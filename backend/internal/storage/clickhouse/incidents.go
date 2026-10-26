package clickhouse

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"incidentlens/backend/internal/detector"
)

// Incidents holds one read slot across all phases and uses one receipt cutoff.
func (s *QueryStore) Incidents(ctx context.Context, end, observed time.Time, cfg detector.Config, filter detector.Filter) (detector.Result, error) {
	base, current := detector.Windows(end)
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
	groups, err := s.incidentAggregates(ctx, base.From, current.From, end, observed)
	if err != nil {
		return detector.Result{}, err
	}
	a, err := detector.PrepareAggregates(ctx, groups, end, observed, cfg, filter)
	if err != nil {
		return detector.Result{}, err
	}
	candidates := a.ReportedCandidates()
	if len(candidates) == 0 {
		return a.WithEvidence(ctx, nil, nil)
	}
	seeds, err := s.incidentEvidenceSeeds(ctx, current.From, end, observed, candidates)
	if err != nil {
		return detector.Result{}, err
	}
	ids, seen := make([]string, 0, len(seeds)), map[string]bool{}
	for _, seed := range seeds {
		if !seen[seed.TraceID] {
			seen[seed.TraceID] = true
			ids = append(ids, seed.TraceID)
		}
	}
	contexts, err := s.incidentEvidenceContexts(ctx, base.From, end, observed, ids)
	if err != nil {
		return detector.Result{}, err
	}
	return a.WithEvidence(ctx, seeds, contexts)
}

func (s *QueryStore) incidentAggregates(ctx context.Context, from, split, end, observed time.Time) ([]detector.AggregateGroup, error) {
	// Read 1001 group/window rows so the 501st operation is detected.
	const sql = `SELECT service_namespace,service_name,span_name,start_time>=fromUnixTimestamp64Nano(?) AS current_window,count(),countIf(status_code=2),countIf(status_code=0),quantileExact(0.95)(duration_ns) FROM spans FINAL PREWHERE ingested_at<=fromUnixTimestamp64Nano(?) AND start_time>=fromUnixTimestamp64Nano(?) AND start_time<fromUnixTimestamp64Nano(?) WHERE span_kind=2 GROUP BY service_namespace,service_name,span_name,current_window ORDER BY service_namespace,service_name,span_name,current_window LIMIT 1001`
	rows, err := s.conn.Query(ctx, sql, split.UnixNano(), observed.UnixNano(), from.UnixNano(), end.UnixNano())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	groups := make([]detector.AggregateGroup, 0)
	lookup := map[detector.Identity]int{}
	keyBytes := 0
	for rows.Next() {
		var id detector.Identity
		var current uint8
		var count, errors, unset, p95 uint64
		if err := rows.Scan(&id.Namespace, &id.Service, &id.Operation, &current, &count, &errors, &unset, &p95); err != nil {
			return nil, err
		}
		if current > 1 || count == 0 {
			return nil, detector.ErrLimit
		}
		i, found := lookup[id]
		if !found {
			keyBytes += len(id.Namespace) + len(id.Service) + len(id.Operation) + 96
			if len(groups) == detector.MaxGroups || keyBytes > detector.MaxAggregateKeyBytes {
				return nil, detector.ErrLimit
			}
			i = len(groups)
			lookup[id] = i
			groups = append(groups, detector.AggregateGroup{Identity: id})
		}
		stat := &groups[i].Baseline
		if current == 1 {
			stat = &groups[i].Current
		}
		if stat.Count != 0 {
			return nil, detector.ErrLimit
		}
		stat.Count, stat.ErrorCount, stat.UnsetCount = count, errors, unset
		p := strconv.FormatUint(p95, 10)
		stat.P95 = &p
	}
	return groups, rows.Err()
}

func (s *QueryStore) incidentEvidenceSeeds(ctx context.Context, from, end, observed time.Time, candidates []detector.Identity) ([]detector.Span, error) {
	if len(candidates) > detector.MaxGroups {
		return nil, detector.ErrLimit
	}
	seeds := make([]detector.Span, 0, len(candidates)*5)
	// Narrow each read to one operation instead of sorting all candidate groups
	// together. Reads are sequential under the same slot, cutoff and deadline.
	for _, id := range candidates {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		selected, err := s.incidentOperationSeeds(ctx, from, end, observed, id)
		if err != nil {
			return nil, err
		}
		seeds = append(seeds, selected...)
	}
	return seeds, nil
}

func (s *QueryStore) incidentOperationSeeds(ctx context.Context, from, end, observed time.Time, id detector.Identity) ([]detector.Span, error) {
	// Sorting-key predicates prune before FINAL. Operation and kind remain
	// after replacement. LIMIT BY selects each trace's best span before LIMIT 5.
	const sql = `SELECT trace_id,span_id,start_time,duration_ns,status_code FROM spans FINAL PREWHERE ingested_at<=fromUnixTimestamp64Nano(?) AND start_time>=fromUnixTimestamp64Nano(?) AND start_time<fromUnixTimestamp64Nano(?) AND service_namespace=? AND service_name=? WHERE span_kind=2 AND span_name=? ORDER BY status_code=2 DESC,duration_ns DESC,start_time ASC,trace_id ASC,span_id ASC LIMIT 1 BY trace_id LIMIT 5`
	rows, err := s.conn.Query(ctx, sql, observed.UnixNano(), from.UnixNano(), end.UnixNano(), id.Namespace, id.Service, id.Operation)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	seeds := make([]detector.Span, 0, 5)
	for rows.Next() {
		var v detector.Span
		if err := rows.Scan(&v.TraceID, &v.SpanID, &v.Start, &v.Duration, &v.Status); err != nil {
			return nil, err
		}
		v.ServiceNamespace, v.ServiceName, v.Operation, v.Kind = id.Namespace, id.Service, id.Operation, 2
		seeds = append(seeds, v)
		if len(seeds) > 5 {
			return nil, detector.ErrLimit
		}
	}
	return seeds, rows.Err()
}

func (s *QueryStore) incidentEvidenceContexts(ctx context.Context, from, end, observed time.Time, ids []string) (map[string][]detector.Span, error) {
	contexts := map[string][]detector.Span{}
	if len(ids) == 0 {
		return contexts, nil
	}
	if len(ids) > detector.MaxEvidenceTraces {
		return nil, detector.ErrLimit
	}
	args := []any{observed.UnixNano(), from.UnixNano(), end.UnixNano()}
	marks := make([]string, len(ids))
	for i, id := range ids {
		marks[i] = "?"
		args = append(args, id)
	}
	sql := `SELECT trace_id,span_id,parent_span_id,service_namespace,service_name,span_name,span_kind,status_code,start_time,duration_ns,dropped_attributes_count,dropped_events_count,dropped_links_count,resource_dropped_attributes_count,scope_dropped_attributes_count,events,links FROM spans FINAL PREWHERE ingested_at<=fromUnixTimestamp64Nano(?) AND start_time>=fromUnixTimestamp64Nano(?) AND start_time<fromUnixTimestamp64Nano(?) AND trace_id IN (` + strings.Join(marks, ",") + `) ORDER BY trace_id,start_time,span_id LIMIT 100001`
	rows, err := s.conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	count, bytes := 0, 0
	for rows.Next() {
		var v detector.Span
		var events, links string
		if err := rows.Scan(&v.TraceID, &v.SpanID, &v.ParentSpanID, &v.ServiceNamespace, &v.ServiceName, &v.Operation, &v.Kind, &v.Status, &v.Start, &v.Duration, &v.DroppedAttributesCount, &v.DroppedEventsCount, &v.DroppedLinksCount, &v.ResourceDroppedAttributesCount, &v.ScopeDroppedAttributesCount, &events, &links); err != nil {
			return nil, err
		}
		v.Events, v.Links = json.RawMessage(events), json.RawMessage(links)
		count++
		bytes += detector.Account(v)
		if count > detector.MaxSpans || bytes > detector.MaxInputBytes {
			return nil, detector.ErrLimit
		}
		contexts[v.TraceID] = append(contexts[v.TraceID], v)
	}
	return contexts, rows.Err()
}

var _ detector.Store = (*QueryStore)(nil)
