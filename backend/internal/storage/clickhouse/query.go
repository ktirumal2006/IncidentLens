package clickhouse

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"incidentlens/backend/internal/query"
)

type QueryStore struct {
	conn  driver.Conn
	slots chan struct{}
}

func OpenQuery(address, user, password, database string) (*QueryStore, error) {
	conn, err := ch.Open(&ch.Options{Addr: []string{address}, Auth: ch.Auth{Database: database, Username: user, Password: password}, DialTimeout: 2 * time.Second, ReadTimeout: 5 * time.Second, MaxOpenConns: 4, MaxIdleConns: 4, BlockBufferSize: 1})
	if err != nil {
		return nil, err
	}
	return &QueryStore{conn: conn, slots: make(chan struct{}, 4)}, nil
}
func (s *QueryStore) Close() error                   { return s.conn.Close() }
func (s *QueryStore) Ping(ctx context.Context) error { return s.conn.Ping(ctx) }
func (s *QueryStore) acquire(ctx context.Context) error {
	select {
	case s.slots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		return query.ErrTooLarge
	}
}
func (s *QueryStore) release() { <-s.slots }
func filters(f query.Filter, server bool) (string, []any) {
	p := []string{"start_time >= fromUnixTimestamp64Nano(?)", "start_time < fromUnixTimestamp64Nano(?)"}
	a := []any{f.From.UnixNano(), f.To.UnixNano()}
	if server {
		p = append(p, "span_kind = 2")
	}
	if f.Service != nil {
		p = append(p, "service_name = ?")
		a = append(a, *f.Service)
	}
	if f.Namespace != nil {
		p = append(p, "service_namespace = ?")
		a = append(a, *f.Namespace)
	}
	if f.Operation != nil {
		p = append(p, "span_name = ?")
		a = append(a, *f.Operation)
	}
	if f.MinDuration != nil {
		p = append(p, "duration_ns >= ?")
		a = append(a, *f.MinDuration)
	}
	if f.MaxDuration != nil {
		p = append(p, "duration_ns <= ?")
		a = append(a, *f.MaxDuration)
	}
	if f.Status != nil {
		p = append(p, "status_code = ?")
		a = append(a, *f.Status)
	}
	return strings.Join(p, " AND "), a
}
func (s *QueryStore) Services(ctx context.Context, f query.Filter, observed time.Time) (query.Summaries, error) {
	out := query.Summaries{ObservedAt: observed, Window: f.Window, Services: []query.Summary{}}
	if err := s.acquire(ctx); err != nil {
		return out, err
	}
	defer s.release()
	where, args := filters(f, true)
	sql := `SELECT service_namespace,service_name,span_name,count(),countIf(status_code=2),countIf(status_code=0),quantileExact(0.5)(duration_ns),quantileExact(0.95)(duration_ns) FROM spans FINAL WHERE ` + where + ` GROUP BY service_namespace,service_name,span_name ORDER BY service_namespace,service_name,span_name LIMIT ?`
	args = append(args, f.Limit+1)
	rows, err := s.conn.Query(ctx, sql, args...)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var v query.Summary
		var p50, p95 uint64
		if err = rows.Scan(&v.Namespace, &v.Service, &v.Operation, &v.Count, &v.ErrorCount, &v.UnsetCount, &p50, &p95); err != nil {
			return out, err
		}
		v.P50 = strconv.FormatUint(p50, 10)
		v.P95 = strconv.FormatUint(p95, 10)
		v.ErrorRate = float64(v.ErrorCount) / float64(v.Count)
		v.UnsetRate = float64(v.UnsetCount) / float64(v.Count)
		out.Services = append(out.Services, v)
	}
	if err = rows.Err(); err != nil {
		return out, err
	}
	if len(out.Services) > f.Limit {
		out.Truncated = true
		out.Services = out.Services[:f.Limit]
	}
	return out, nil
}
func (s *QueryStore) Search(ctx context.Context, f query.Filter, observed time.Time) (query.SearchResult, error) {
	out := query.SearchResult{ObservedAt: observed, Window: f.Window, Traces: []query.TraceMatch{}}
	cut, start, id, err := query.ParseCursor(f)
	if err != nil {
		return out, err
	}
	if cut.After(observed) {
		return out, query.ErrInvalid
	}
	if cut.IsZero() {
		cut = observed
	}
	out.IngestionCutoff = cut
	if err = s.acquire(ctx); err != nil {
		return out, err
	}
	defer s.release()
	where, args := filters(f, false)
	sql := `SELECT trace_id,min(start_time) AS matching_start,count(),min(duration_ns),max(duration_ns),countIf(status_code=2) FROM spans FINAL PREWHERE ingested_at <= fromUnixTimestamp64Nano(?) WHERE ` + where + ` GROUP BY trace_id`
	args = append([]any{cut.UnixNano()}, args...)
	if id != "" {
		sql += ` HAVING (matching_start > fromUnixTimestamp64Nano(?) OR (matching_start = fromUnixTimestamp64Nano(?) AND trace_id > ?))`
		args = append(args, start.UnixNano(), start.UnixNano(), id)
	}
	sql += ` ORDER BY matching_start,trace_id LIMIT ?`
	args = append(args, f.Limit+1)
	rows, err := s.conn.Query(ctx, sql, args...)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var v query.TraceMatch
		var min, max uint64
		if err = rows.Scan(&v.TraceID, &v.MatchingStart, &v.Count, &min, &max, &v.ErrorCount); err != nil {
			return out, err
		}
		v.MinDuration = strconv.FormatUint(min, 10)
		v.MaxDuration = strconv.FormatUint(max, 10)
		v.MatchingStart = v.MatchingStart.UTC()
		out.Traces = append(out.Traces, v)
	}
	if err = rows.Err(); err != nil {
		return out, err
	}
	if len(out.Traces) > f.Limit {
		out.Traces = out.Traces[:f.Limit]
		last := out.Traces[len(out.Traces)-1]
		out.NextCursor = query.MakeCursor(f, cut, last.MatchingStart, last.TraceID)
	}
	return out, nil
}

const detailColumns = `span_id,parent_span_id,service_name,service_namespace,span_name,span_kind,status_code,status_message,start_time,end_time,duration_ns,ingested_at,resource_attributes,span_attributes,scope_attributes,events,links,scope_name,scope_version,resource_schema_url,scope_schema_url,trace_state,trace_flags,dropped_attributes_count,dropped_events_count,dropped_links_count,resource_dropped_attributes_count,scope_dropped_attributes_count`

func (s *QueryStore) Detail(ctx context.Context, w query.Window, id string, observed time.Time) (query.DetailResult, error) {
	out := query.DetailResult{ObservedAt: observed, Window: w, TraceID: id, Spans: []query.Span{}, MissingParentIDs: []string{}}
	if !query.ValidTraceID(id) {
		return out, query.ErrInvalid
	}
	if err := s.acquire(ctx); err != nil {
		return out, err
	}
	defer s.release()
	rows, err := s.conn.Query(ctx, `SELECT `+detailColumns+` FROM spans FINAL WHERE trace_id=? AND start_time >= fromUnixTimestamp64Nano(?) AND start_time < fromUnixTimestamp64Nano(?) ORDER BY start_time,span_id LIMIT ?`, id, w.From.UnixNano(), w.To.UnixNano(), query.MaxDetail+1)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	spanBytes := 0
	for rows.Next() {
		var v query.Span
		var duration uint64
		var resources, attrs, scope, events, links string
		err = rows.Scan(&v.SpanID, &v.ParentSpanID, &v.ServiceName, &v.ServiceNamespace, &v.SpanName, &v.SpanKind, &v.StatusCode, &v.StatusMessage, &v.StartTime, &v.EndTime, &duration, &v.IngestedAt, &resources, &attrs, &scope, &events, &links, &v.ScopeName, &v.ScopeVersion, &v.ResourceSchemaURL, &v.ScopeSchemaURL, &v.TraceState, &v.TraceFlags, &v.DroppedAttributesCount, &v.DroppedEventsCount, &v.DroppedLinksCount, &v.ResourceDroppedAttributesCount, &v.ScopeDroppedAttributesCount)
		if err != nil {
			return out, err
		}
		v.DurationNS = strconv.FormatUint(duration, 10)
		v.ResourceAttributes = json.RawMessage(resources)
		v.SpanAttributes = json.RawMessage(attrs)
		v.ScopeAttributes = json.RawMessage(scope)
		v.Events = json.RawMessage(events)
		v.Links = json.RawMessage(links)
		v.StartTime = v.StartTime.UTC()
		v.EndTime = v.EndTime.UTC()
		v.IngestedAt = v.IngestedAt.UTC()
		encoded, encodeErr := json.Marshal(v)
		if encodeErr != nil {
			return out, encodeErr
		}
		if spanBytes+len(encoded)+1 > query.MaxResponse-65536 {
			if len(out.Spans) == 0 {
				return out, query.ErrTooLarge
			}
			out.Truncated = true
			out.TruncationReason = "response_bytes"
			break
		}
		spanBytes += len(encoded) + 1
		out.Spans = append(out.Spans, v)
	}
	if err = rows.Err(); err != nil {
		return out, err
	}
	if len(out.Spans) == 0 {
		return out, query.ErrNotFound
	}
	if len(out.Spans) > query.MaxDetail {
		out.Truncated = true
		out.TruncationReason = "span_limit"
		out.Spans = out.Spans[:query.MaxDetail]
	}
	query.Relationships(&out)
	return out, nil
}

var _ query.Store = (*QueryStore)(nil)
