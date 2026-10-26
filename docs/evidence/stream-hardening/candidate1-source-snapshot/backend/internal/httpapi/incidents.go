package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"incidentlens/backend/internal/detector"
	"incidentlens/backend/internal/query"
)

func (h *Handler) serveIncidents(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		method(w)
		return
	}
	if h.Incidents == nil {
		respondError(w, 503, "unavailable", "detector unavailable")
		return
	}
	q, e := url.ParseQuery(r.URL.RawQuery)
	if e != nil {
		respondError(w, 400, "invalid_request", "invalid query parameters")
		return
	}
	for k, v := range q {
		if (k != "end" && k != "service" && k != "namespace") || len(v) != 1 {
			respondError(w, 400, "invalid_request", "invalid query parameters")
			return
		}
	}
	s, ok := one(q, "end")
	if !ok {
		respondError(w, 400, "invalid_request", "end required")
		return
	}
	end, e := parseTime(s)
	now := h.Now().UTC()
	if e != nil || detector.ValidateEnd(end, now) != nil {
		respondError(w, 400, "invalid_request", "invalid evaluation end")
		return
	}
	f := detector.Filter{}
	if x, ok := one(q, "service"); ok {
		f.Service = &x
	}
	if x, ok := one(q, "namespace"); ok {
		f.Namespace = &x
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	result, e := h.Incidents.Incidents(ctx, end, now, h.DetectorConfig, f)
	if e != nil {
		var chErr *proto.Exception
		switch {
		case errors.Is(e, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) || (errors.As(e, &chErr) && chErr.Code == 159):
			respondError(w, 504, "deadline_exceeded", "query deadline exceeded")
		case errors.Is(e, query.ErrInvalid):
			respondError(w, 400, "invalid_request", "invalid evaluation request")
		default:
			respondError(w, 503, "unavailable", "query unavailable or resource cap reached")
		}
		return
	}
	b, e := json.Marshal(result)
	if e != nil {
		respondError(w, 503, "unavailable", "invalid evaluation data")
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
