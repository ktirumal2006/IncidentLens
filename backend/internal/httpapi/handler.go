package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"incidentlens/backend/internal/query"
)

type Handler struct {
	Store     query.Store
	StaticDir string
	Now       func() time.Time
}

func NewHandler(store query.Store, staticDir string) http.Handler {
	return &Handler{Store: store, StaticDir: staticDir, Now: func() time.Time { return time.Now().UTC() }}
}
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/livez":
		if r.Method != "GET" {
			method(w)
			return
		}
		w.WriteHeader(200)
		return
	case r.URL.Path == "/readyz":
		if r.Method != "GET" {
			method(w)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if h.Store.Ping(ctx) != nil {
			respondError(w, 503, "unavailable", "storage unavailable")
			return
		}
		w.WriteHeader(200)
		return
	case r.URL.Path == "/api/v1/services":
		h.serveQuery(w, r, "services")
		return
	case r.URL.Path == "/api/v1/traces":
		h.serveQuery(w, r, "traces")
		return
	case strings.HasPrefix(r.URL.Path, "/api/v1/traces/"):
		h.serveQuery(w, r, "detail")
		return
	case strings.HasPrefix(r.URL.Path, "/api/"):
		respondError(w, 404, "not_found", "route not found")
		return
	default:
		if h.StaticDir != "" && r.Method == "GET" {
			http.FileServer(http.Dir(h.StaticDir)).ServeHTTP(w, r)
			return
		}
		respondError(w, 404, "not_found", "route not found")
	}
}
func method(w http.ResponseWriter) {
	w.Header().Set("Allow", "GET")
	respondError(w, 405, "method_not_allowed", "GET required")
}
func respondError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	b, e := json.Marshal(v)
	if e != nil {
		http.Error(w, "serialization error", 500)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(b)
}
func parse(r *http.Request, kind string, now time.Time) (query.Filter, error) {
	var f query.Filter
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return f, query.ErrInvalid
	}
	allowed := map[string]bool{"from": true, "to": true, "service": true, "namespace": true, "operation": true, "limit": true}
	if kind == "traces" {
		for _, k := range []string{"min_duration_ns", "max_duration_ns", "status", "cursor"} {
			allowed[k] = true
		}
	}
	if kind == "detail" {
		allowed = map[string]bool{"from": true, "to": true}
	}
	for k, v := range q {
		if !allowed[k] || len(v) != 1 {
			return f, query.ErrInvalid
		}
	}
	from, ok := one(q, "from")
	if !ok {
		return f, query.ErrInvalid
	}
	to, ok := one(q, "to")
	if !ok {
		return f, query.ErrInvalid
	}
	var e error
	f.From, e = parseTime(from)
	if e != nil {
		return f, query.ErrInvalid
	}
	f.To, e = parseTime(to)
	if e != nil {
		return f, query.ErrInvalid
	}
	f.Limit = 100
	if s, ok := one(q, "limit"); ok {
		f.Limit, e = strconv.Atoi(s)
		if e != nil {
			return f, query.ErrInvalid
		}
	}
	if s, ok := one(q, "service"); ok {
		f.Service = &s
	}
	if s, ok := one(q, "namespace"); ok {
		f.Namespace = &s
	}
	if s, ok := one(q, "operation"); ok {
		f.Operation = &s
	}
	if s, ok := one(q, "min_duration_ns"); ok {
		n, e := strconv.ParseUint(s, 10, 64)
		if e != nil {
			return f, query.ErrInvalid
		}
		f.MinDuration = &n
	}
	if s, ok := one(q, "max_duration_ns"); ok {
		n, e := strconv.ParseUint(s, 10, 64)
		if e != nil {
			return f, query.ErrInvalid
		}
		f.MaxDuration = &n
	}
	if s, ok := one(q, "status"); ok {
		var n uint8
		switch s {
		case "UNSET":
			n = 0
		case "OK":
			n = 1
		case "ERROR":
			n = 2
		default:
			return f, query.ErrInvalid
		}
		f.Status = &n
	}
	if s, ok := one(q, "cursor"); ok {
		f.Cursor = s
	}
	if kind == "detail" {
		return f, query.ValidateWindow(f.Window, now)
	}
	if e := query.ValidateFilter(f, now); e != nil {
		return f, e
	}
	if kind == "traces" && f.Cursor != "" {
		_, _, _, e = query.ParseCursor(f)
		if e != nil {
			return f, e
		}
	}
	return f, nil
}
func one(q url.Values, k string) (string, bool) {
	v, ok := q[k]
	if !ok || len(v) != 1 {
		return "", false
	}
	return v[0], true
}
func parseTime(s string) (time.Time, error) {
	if !strings.Contains(s, "T") || !(strings.HasSuffix(s, "Z") || strings.ContainsAny(s, "+-")) {
		return time.Time{}, query.ErrInvalid
	}
	v, e := time.Parse(time.RFC3339Nano, s)
	if e != nil {
		return time.Time{}, e
	}
	return v.UTC(), nil
}
func (h *Handler) serveQuery(w http.ResponseWriter, r *http.Request, kind string) {
	if r.Method != "GET" {
		method(w)
		return
	}
	now := h.Now().UTC()
	f, e := parse(r, kind, now)
	if e != nil {
		respondError(w, 400, "invalid_request", "invalid query parameters")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	var result any
	switch kind {
	case "services":
		result, e = h.Store.Services(ctx, f, now)
	case "traces":
		result, e = h.Store.Search(ctx, f, now)
	case "detail":
		id := strings.TrimPrefix(r.URL.Path, "/api/v1/traces/")
		if strings.Contains(id, "/") || !query.ValidTraceID(id) {
			respondError(w, 400, "invalid_request", "invalid trace ID")
			return
		}
		result, e = h.Store.Detail(ctx, f.Window, id, now)
	}
	if e != nil {
		var chErr *proto.Exception
		if errors.Is(e, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) || (errors.As(e, &chErr) && chErr.Code == 159) {
			respondError(w, 504, "deadline_exceeded", "query deadline exceeded")
		} else if errors.Is(e, query.ErrInvalid) {
			respondError(w, 400, "invalid_request", "invalid query parameters")
		} else if kind == "detail" && errors.Is(e, query.ErrNotFound) {
			respondError(w, 404, "not_found", "trace not found")
		} else {
			respondError(w, 503, "unavailable", "query unavailable or resource cap reached")
		}
		return
	}
	if kind == "detail" {
		d := result.(query.DetailResult)
		for {
			b, err := json.Marshal(d)
			if err != nil {
				respondError(w, 503, "unavailable", "invalid stored trace data")
				return
			}
			if len(b) <= query.MaxResponse {
				result = d
				break
			}
			if len(d.Spans) == 0 {
				respondError(w, 503, "unavailable", "response cap reached")
				return
			}
			d.Spans = d.Spans[:len(d.Spans)-1]
			d.Truncated = true
			d.TruncationReason = "response_bytes"
			query.Relationships(&d)
		}
	}
	b, e := json.Marshal(result)
	if e != nil {
		respondError(w, 503, "unavailable", "invalid stored data")
		return
	}
	if len(b) > query.MaxResponse {
		respondError(w, 503, "unavailable", "response cap reached")
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(200)
	_, _ = w.Write(b)
}
