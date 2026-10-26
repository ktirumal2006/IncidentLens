package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"incidentlens/backend/internal/detector"
	"incidentlens/backend/internal/query"
)

func TestQueryFailureCausePreservesHTTPContract(t *testing.T) {
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	window := "?from=" + url.QueryEscape(now.Add(-time.Hour).Format(time.RFC3339Nano)) + "&to=" + url.QueryEscape(now.Format(time.RFC3339Nano))
	for _, tc := range []struct {
		name, route, cause string
		err                error
		status             int
		code, message      string
	}{
		{"admission", "services", "admission_busy", query.ErrBusy, 503, "unavailable", "query unavailable or resource cap reached"},
		{"resource", "services", "resource_cap", query.ErrTooLarge, 503, "unavailable", "query unavailable or resource cap reached"},
		{"clickhouse", "detail", "clickhouse_307", &proto.Exception{Code: 307, Message: "private database text"}, 503, "unavailable", "query unavailable or resource cap reached"},
		{"deadline", "traces", "deadline", context.DeadlineExceeded, 504, "deadline_exceeded", "query deadline exceeded"},
		{"clickhouse deadline", "traces", "deadline", &proto.Exception{Code: 159}, 504, "deadline_exceeded", "query deadline exceeded"},
		{"other", "services", "other_query_error", errors.New("private database text"), 503, "unavailable", "query unavailable or resource cap reached"},
		{"non-detail not found", "services", "other_query_error", query.ErrNotFound, 503, "unavailable", "query unavailable or resource cap reached"},
		{"not found", "detail", "", query.ErrNotFound, 404, "not_found", "trace not found"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := "/api/v1/" + tc.route
			if tc.route == "detail" {
				path = "/api/v1/traces/0123456789abcdef0123456789abcdef"
			}
			w := httptest.NewRecorder()
			h := &Handler{Store: errorStore{tc.err}, Now: func() time.Time { return now }}
			h.ServeHTTP(w, httptest.NewRequest("GET", path+window, nil))
			if w.Code != tc.status || w.Header().Get(QueryFailureHeader) != tc.cause {
				t.Fatalf("status=%d cause=%q want %d %q", w.Code, w.Header().Get(QueryFailureHeader), tc.status, tc.cause)
			}
			var body map[string]map[string]string
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || !reflect.DeepEqual(body, map[string]map[string]string{"error": {"code": tc.code, "message": tc.message}}) {
				t.Fatalf("body=%q err=%v", w.Body.String(), err)
			}
			if strings.Contains(w.Body.String()+w.Header().Get(QueryFailureHeader), "private") {
				t.Fatal("private exception text exposed")
			}
		})
	}
	if !errors.Is(query.ErrBusy, query.ErrTooLarge) {
		t.Fatal("admission error no longer matches prior resource cap")
	}
}

func TestIncidentFailureCause(t *testing.T) {
	now := time.Now().UTC()
	path := "/api/v1/incidents?end=" + url.QueryEscape(now.Add(-time.Minute).Format(time.RFC3339Nano))
	for _, tc := range []struct {
		err    error
		cause  string
		status int
	}{
		{detector.ErrLimit, "detector_input_limit", 503},
		{query.ErrBusy, "admission_busy", 503},
		{&proto.Exception{Code: 307}, "clickhouse_307", 503},
		{context.DeadlineExceeded, "deadline", 504},
	} {
		w := httptest.NewRecorder()
		NewHandlerWithDetector(stub{}, &incidentStub{err: tc.err}, detector.DefaultConfig(), "").ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != tc.status || w.Header().Get(QueryFailureHeader) != tc.cause {
			t.Fatalf("error %v: status=%d cause=%q", tc.err, w.Code, w.Header().Get(QueryFailureHeader))
		}
	}
}
