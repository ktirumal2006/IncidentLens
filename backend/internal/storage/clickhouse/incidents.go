package clickhouse

import (
	"context"
	"encoding/json"
	"time"

	"incidentlens/backend/internal/detector"
)

// Incidents reads one deduplicated, receipt-bounded snapshot of the full service
// context. Service filters are applied only when reporting evaluated operations.
func (s *QueryStore) Incidents(ctx context.Context, end, observed time.Time, cfg detector.Config, filter detector.Filter) (detector.Result, error) {
	base, _ := detector.Windows(end)
	if err := detector.ValidateEnd(end, observed); err != nil {
		return detector.Result{}, err
	}
	if err := cfg.Validate(); err != nil {
		return detector.Result{}, err
	}
	if err := s.acquire(ctx); err != nil {
		return detector.Result{}, err
	}
	defer s.release()
	rows, err := s.conn.Query(ctx, `SELECT trace_id,span_id,parent_span_id,service_namespace,service_name,span_name,span_kind,status_code,start_time,duration_ns,dropped_attributes_count,dropped_events_count,dropped_links_count,resource_dropped_attributes_count,scope_dropped_attributes_count,events,links FROM spans FINAL PREWHERE ingested_at <= fromUnixTimestamp64Nano(?) WHERE start_time >= fromUnixTimestamp64Nano(?) AND start_time < fromUnixTimestamp64Nano(?) ORDER BY start_time,trace_id,span_id LIMIT ?`, observed.UnixNano(), base.From.UnixNano(), end.UnixNano(), detector.MaxSpans+1)
	if err != nil {
		return detector.Result{}, err
	}
	defer rows.Close()
	input := make([]detector.Span, 0, 1024)
	bytes := 0
	for rows.Next() {
		var v detector.Span
		var events, links string
		if err = rows.Scan(&v.TraceID, &v.SpanID, &v.ParentSpanID, &v.ServiceNamespace, &v.ServiceName, &v.Operation, &v.Kind, &v.Status, &v.Start, &v.Duration, &v.DroppedAttributesCount, &v.DroppedEventsCount, &v.DroppedLinksCount, &v.ResourceDroppedAttributesCount, &v.ScopeDroppedAttributesCount, &events, &links); err != nil {
			return detector.Result{}, err
		}
		v.Events = json.RawMessage(events)
		v.Links = json.RawMessage(links)
		v.Start = v.Start.UTC()
		bytes += detector.Account(v)
		if bytes > detector.MaxInputBytes || len(input) >= detector.MaxSpans {
			return detector.Result{}, detector.ErrLimit
		}
		input = append(input, v)
	}
	if err = rows.Err(); err != nil {
		return detector.Result{}, err
	}
	return detector.EvaluateContext(ctx, input, end, observed, cfg, filter)
}

var _ detector.Store = (*QueryStore)(nil)
