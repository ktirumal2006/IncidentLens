package httpapi

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestIngestionStatusDirectAndMethod(t *testing.T) {
	h := NewHandler(stub{}, "")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/ingestion-status", nil))
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"not_applicable"`) {
		t.Fatalf("direct response: %d %s", w.Code, w.Body.String())
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("POST", "/api/v1/ingestion-status", nil))
	if w.Code != 405 {
		t.Fatalf("method: %d", w.Code)
	}
}

func TestIngestionStatusProxyFailClosedAndRecovers(t *testing.T) {
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	var body atomic.Value
	var requests atomic.Int32
	p, err := NewIngestionStatusProxy("http://worker:8083/internal/stream-status")
	if err != nil {
		t.Fatal(err)
	}
	p.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/internal/stream-status" {
			t.Fatalf("wrong worker path: %s", r.URL.Path)
		}
		requests.Add(1)
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body.Load().(string)))}, nil
	})
	p.Now = func() time.Time { return now }
	good := func(at time.Time, observed int, lag any) string {
		v := map[string]any{"mode": "streaming", "state": "ok", "scope": statusScope, "lag_records": lag, "partitions_total": 4, "partitions_observed": observed, "sampled_at": at, "sample_started_at": at, "sample_finished_at": at, "processing_state": "idle"}
		b, _ := json.Marshal(v)
		return string(b)
	}
	body.Store(good(now, 3, 0))
	if s := p.Get(); s.State != "unavailable" || s.LagRecords != nil {
		t.Fatalf("partial healthy accepted: %+v", s)
	}
	if s := p.Get(); s.State != "unavailable" || requests.Load() != 1 {
		t.Fatalf("cache failed: %+v, requests=%d", s, requests.Load())
	}
	now = now.Add(3 * time.Second)
	body.Store(strings.Replace(good(now, 4, 0), `"partitions_total":4`, `"partitions_total":3`, 1))
	if s := p.Get(); s.State != "unavailable" {
		t.Fatalf("three-partition healthy sample accepted: %+v", s)
	}
	now = now.Add(3 * time.Second)
	body.Store(strings.Replace(good(now, 4, 0), `"processing_state":"idle"`, `"processing_state":"failed"`, 1))
	if s := p.Get(); s.State != "unavailable" {
		t.Fatalf("failed worker healthy sample accepted: %+v", s)
	}
	now = now.Add(3 * time.Second)
	body.Store(strings.Replace(good(now, 4, 0), `"processing_state":"idle"`, `"processing_state":"idle","reason_code":"empty_partition_uninitialized"`, 1))
	if s := p.Get(); s.State != "ok" || s.LagRecords == nil || *s.LagRecords != 0 || s.ReasonCode != "empty_partition_uninitialized" {
		t.Fatalf("recovery failed: %+v", s)
	}
	now = now.Add(11 * time.Second)
	if s := p.Get(); s.State != "stale" || s.LagRecords != nil {
		t.Fatalf("stale healthy sample accepted: %+v", s)
	}
	now = now.Add(3 * time.Second)
	body.Store(`{"mode":"streaming","state":"ok","scope":"consumer_group_lag_and_local_worker_progress","lag_records":0,"partitions_total":4,"partitions_observed":4,"sampled_at":"bad","processing_state":"idle"}`)
	if s := p.Get(); s.State != "unavailable" || s.LagRecords != nil {
		t.Fatalf("malformed timestamp accepted: %+v", s)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestIngestionStatusRejectsUnsafeURL(t *testing.T) {
	for _, raw := range []string{"file:///tmp/status", "http://worker:8083/other", "http://worker:8083/internal/stream-status?url=x", "http://user:pass@worker/internal/stream-status"} {
		if _, err := NewIngestionStatusProxy(raw); err == nil {
			t.Fatalf("accepted %q", raw)
		}
	}
}
