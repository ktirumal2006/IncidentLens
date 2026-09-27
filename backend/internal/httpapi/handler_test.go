package httpapi

import (
	"context"
	"errors"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"incidentlens/backend/internal/query"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

type stub struct{}

func (stub) Ping(context.Context) error { return nil }
func (stub) Services(_ context.Context, f query.Filter, n time.Time) (query.Summaries, error) {
	return query.Summaries{ObservedAt: n, Window: f.Window, Services: []query.Summary{}}, nil
}
func (stub) Search(_ context.Context, f query.Filter, n time.Time) (query.SearchResult, error) {
	return query.SearchResult{ObservedAt: n, Window: f.Window, Traces: []query.TraceMatch{}}, nil
}
func (stub) Detail(_ context.Context, w query.Window, id string, n time.Time) (query.DetailResult, error) {
	return query.DetailResult{ObservedAt: n, Window: w, TraceID: id, Spans: []query.Span{}}, nil
}
func TestHTTPValidation(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	h := &Handler{Store: stub{}, Now: func() time.Time { return now }}
	valid := "?from=" + url.QueryEscape(now.Add(-time.Hour).Format(time.RFC3339Nano)) + "&to=" + url.QueryEscape(now.Format(time.RFC3339Nano))
	for _, tc := range []struct {
		path   string
		status int
	}{{"/api/v1/traces" + valid, 200}, {"/api/v1/traces" + valid + "&status=ERROR", 200}, {"/api/v1/traces" + valid + "&status=ERROR&status=OK", 400}, {"/api/v1/traces" + valid + "&min_duration_ns=0", 400}, {"/api/v1/services" + valid + "&unknown=x", 400}, {"/api/v1/traces/bad" + valid, 400}, {"/api/v1/traces/0123456789abcdef0123456789abcdef" + valid, 200}} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", tc.path, nil))
		if w.Code != tc.status {
			t.Errorf("%s: got %d body %s", tc.path, w.Code, w.Body.String())
		}
	}
}
func TestWrongMethod(t *testing.T) {
	h := NewHandler(stub{}, "")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("POST", "/api/v1/traces", strings.NewReader("")))
	if w.Code != 405 {
		t.Fatal(w.Code)
	}
}

type errorStore struct{ err error }

func (s errorStore) Ping(context.Context) error { return s.err }
func (s errorStore) Services(context.Context, query.Filter, time.Time) (query.Summaries, error) {
	return query.Summaries{}, s.err
}
func (s errorStore) Search(context.Context, query.Filter, time.Time) (query.SearchResult, error) {
	return query.SearchResult{}, s.err
}
func (s errorStore) Detail(context.Context, query.Window, string, time.Time) (query.DetailResult, error) {
	return query.DetailResult{}, s.err
}
func TestErrorsAndBounds(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	valid := "?from=" + url.QueryEscape(now.Add(-time.Hour).Format(time.RFC3339Nano)) + "&to=" + url.QueryEscape(now.Format(time.RFC3339Nano))
	for _, tc := range []struct {
		store query.Store
		path  string
		want  int
	}{{stub{}, "/api/v1/traces" + valid + "&bad=%ZZ", 400}, {stub{}, "/api/v1/traces?from=2026-09-18T00%3A00%3A00Z&to=2026-09-18T01%3A00%3A00Z", 400}, {stub{}, "/api/v1/traces?from=2026-09-26T12%3A00%3A00Z&to=2026-09-26T12%3A06%3A00Z", 400}, {errorStore{errors.New("unavailable")}, "/api/v1/traces" + valid, 503}, {errorStore{context.DeadlineExceeded}, "/api/v1/traces" + valid, 504}, {errorStore{&proto.Exception{Code: 159}}, "/api/v1/traces" + valid, 504}, {errorStore{query.ErrNotFound}, "/api/v1/traces/0123456789abcdef0123456789abcdef" + valid, 404}} {
		h := &Handler{Store: tc.store, Now: func() time.Time { return now }}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", tc.path, nil))
		if w.Code != tc.want {
			t.Errorf("%s: got %d want %d body %s", tc.path, w.Code, tc.want, w.Body.String())
		}
	}
}

type hugeStore struct{ stub }

func (hugeStore) Search(_ context.Context, f query.Filter, n time.Time) (query.SearchResult, error) {
	return query.SearchResult{Window: f.Window, ObservedAt: n, Traces: []query.TraceMatch{{TraceID: strings.Repeat("x", query.MaxResponse+1)}}}, nil
}
func TestSearchResponseCap(t *testing.T) {
	now := time.Now().UTC()
	h := &Handler{Store: hugeStore{}, Now: func() time.Time { return now }}
	valid := "?from=" + url.QueryEscape(now.Add(-time.Hour).Format(time.RFC3339Nano)) + "&to=" + url.QueryEscape(now.Format(time.RFC3339Nano))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/traces"+valid, nil))
	if w.Code != 503 {
		t.Fatalf("status %d", w.Code)
	}
}
