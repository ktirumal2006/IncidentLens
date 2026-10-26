package httpapi

import (
	"context"
	"errors"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"incidentlens/backend/internal/detector"
)

type incidentStub struct {
	err           error
	result        detector.Result
	end, observed time.Time
	filter        detector.Filter
	config        detector.Config
}

func (s *incidentStub) Incidents(_ context.Context, end, observed time.Time, cfg detector.Config, f detector.Filter) (detector.Result, error) {
	s.end = end
	s.observed = observed
	s.filter = f
	s.config = cfg
	return s.result, s.err
}
func TestIncidentHTTPValidation(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 123, time.UTC)
	ds := &incidentStub{}
	h := NewHandlerWithDetector(stub{}, ds, detector.DefaultConfig(), "").(*Handler)
	h.Now = func() time.Time { return now }
	valid := "?end=" + url.QueryEscape(now.Add(-time.Minute).Format(time.RFC3339Nano))
	for _, tc := range []struct {
		suffix string
		want   int
	}{{valid, 200}, {valid + "&namespace=", 200}, {"", 400}, {"?end=bad", 400}, {valid + "&end=" + url.QueryEscape(now.Format(time.RFC3339Nano)), 400}, {valid + "&unknown=x", 400}, {valid + "&bad=%ZZ", 400}, {"?end=" + url.QueryEscape(now.Add(time.Nanosecond).Format(time.RFC3339Nano)), 400}, {"?end=" + url.QueryEscape(now.Add(-7*24*time.Hour).Format(time.RFC3339Nano)), 400}} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/incidents"+tc.suffix, nil))
		if w.Code != tc.want {
			t.Errorf("%s got %d want%d body%s", tc.suffix, w.Code, tc.want, w.Body.String())
		}
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/incidents"+valid+"&service=svc&namespace=", nil))
	if w.Code != 200 || ds.filter.Service == nil || *ds.filter.Service != "svc" || ds.filter.Namespace == nil || *ds.filter.Namespace != "" || !ds.end.Equal(now.Add(-time.Minute)) || ds.config != detector.DefaultConfig() {
		t.Fatalf("filter/end/config not passed: %+v", ds)
	}
}
func TestIncidentHTTPFailuresAndCap(t *testing.T) {
	now := time.Now().UTC()
	valid := "?end=" + url.QueryEscape(now.Add(-time.Minute).Format(time.RFC3339Nano))
	for _, tc := range []struct {
		err  error
		want int
	}{{detector.ErrLimit, 503}, {errors.New("database unavailable"), 503}, {context.DeadlineExceeded, 504}, {&proto.Exception{Code: 159}, 504}} {
		ds := &incidentStub{err: tc.err}
		h := NewHandlerWithDetector(stub{}, ds, detector.DefaultConfig(), "")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/incidents"+valid, nil))
		if w.Code != tc.want {
			t.Fatalf("err %v got%d body%s", tc.err, w.Code, w.Body.String())
		}
	}
	ds := &incidentStub{result: detector.Result{Caveats: []string{strings.Repeat("x", 8<<20)}}}
	w := httptest.NewRecorder()
	NewHandlerWithDetector(stub{}, ds, detector.DefaultConfig(), "").ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/incidents"+valid, nil))
	if w.Code != 503 {
		t.Fatalf("response cap status=%d", w.Code)
	}
	w = httptest.NewRecorder()
	NewHandlerWithDetector(stub{}, ds, detector.DefaultConfig(), "").ServeHTTP(w, httptest.NewRequest("POST", "/api/v1/incidents"+valid, nil))
	if w.Code != 405 {
		t.Fatal(w.Code)
	}
}
