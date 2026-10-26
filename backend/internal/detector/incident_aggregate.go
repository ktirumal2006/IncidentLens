package detector

import (
	"context"
	"encoding/json"
	"slices"
	"sort"
	"strconv"
	"time"

	"incidentlens/backend/internal/query"
)

// AggregateVersion changes only relationship-caveat scope. Rule statistics and
// candidate ordering retain the exact trace-v1 arithmetic.
const AggregateVersion = "trace-v2"

const MaxEvidenceTraces = MaxGroups * 5
const MaxAggregateKeyBytes = 2 << 20

type AggregateGroup struct {
	Identity
	Baseline Stats
	Current  Stats
}

// AggregateEvaluation holds every candidate identity, including identities
// excluded by the report-only service/namespace filter.
type AggregateEvaluation struct {
	Result     Result
	candidates map[key]bool
}

func validAggregateStats(s Stats) bool {
	if s.ErrorCount > s.Count || s.UnsetCount > s.Count || s.ErrorCount > s.Count-s.UnsetCount {
		return false
	}
	if s.Count == 0 {
		return s.P95 == nil
	}
	if s.P95 == nil {
		return false
	}
	p, err := strconv.ParseUint(*s.P95, 10, 64)
	return err == nil && strconv.FormatUint(p, 10) == *s.P95
}

func normalizeAggregateStats(s Stats) Stats {
	if s.Count > 0 {
		s.ErrorRate = float64(s.ErrorCount) / float64(s.Count)
		s.UnsetRate = float64(s.UnsetCount) / float64(s.Count)
	}
	return s
}

// PrepareAggregates builds the full, unfiltered candidate map before applying
// report filters. It does not infer any missing-parent or cycle claim from
// aggregate rows.
func PrepareAggregates(ctx context.Context, groups []AggregateGroup, end, observed time.Time, cfg Config, filter Filter) (AggregateEvaluation, error) {
	a := AggregateEvaluation{Result: Result{RuleVersion: AggregateVersion, Config: cfg, ObservedAt: observed.UTC(), End: end.UTC(), Operations: []Operation{}, Candidates: []Operation{}, Caveats: []string{"relationship_context_sampled"}}, candidates: map[key]bool{}}
	a.Result.BaselineWindow, a.Result.CurrentWindow = Windows(end)
	if err := ctx.Err(); err != nil {
		return a, err
	}
	if err := cfg.Validate(); err != nil {
		return a, err
	}
	if err := ValidateEnd(end, observed); err != nil {
		return a, err
	}
	if len(groups) > MaxGroups {
		return a, ErrLimit
	}
	keys := make(map[key]bool, len(groups))
	keyBytes := 0
	for _, g := range groups {
		if err := ctx.Err(); err != nil {
			return a, err
		}
		k := key{g.Namespace, g.Service, g.Operation}
		keyBytes += len(k.ns) + len(k.svc) + len(k.op) + 96
		if keyBytes > MaxAggregateKeyBytes || keys[k] || !validAggregateStats(g.Baseline) || !validAggregateStats(g.Current) || (g.Baseline.Count == 0 && g.Current.Count == 0) {
			return a, ErrLimit
		}
		keys[k] = true
		base, cur := normalizeAggregateStats(g.Baseline), normalizeAggregateStats(g.Current)
		caveats := []string{"trace_context_not_evaluated"}
		if base.UnsetCount > 0 || cur.UnsetCount > 0 {
			caveats = append(caveats, "unset_status")
			a.Result.Caveats = addCaveat(a.Result.Caveats, "unset_status")
		}
		op := operationFromStats(k, base, cur, caveats, cfg)
		if op.State == "candidate" {
			a.candidates[k] = true
		}
		if filter.Service != nil && g.Service != *filter.Service || filter.Namespace != nil && g.Namespace != *filter.Namespace {
			continue
		}
		a.Result.Operations = append(a.Result.Operations, op)
	}
	sort.Slice(a.Result.Operations, func(i, j int) bool {
		x, y := a.Result.Operations[i], a.Result.Operations[j]
		return lessKey(key{x.Namespace, x.Service, x.Operation}, key{y.Namespace, y.Service, y.Operation})
	})
	if len(a.Result.Operations) == 0 {
		a.Result.Caveats = addCaveat(a.Result.Caveats, "no_server_coverage")
	}
	sort.Strings(a.Result.Caveats)
	return a, nil
}

func (a AggregateEvaluation) ReportedCandidates() []Identity {
	keys := make([]Identity, 0)
	for _, op := range a.Result.Operations {
		if op.State == "candidate" {
			keys = append(keys, op.Identity)
		}
	}
	return keys
}

func evidenceLess(a, b Span) bool {
	if (a.Status == 2) != (b.Status == 2) {
		return a.Status == 2
	}
	if a.Duration != b.Duration {
		return a.Duration > b.Duration
	}
	if !a.Start.Equal(b.Start) {
		return a.Start.Before(b.Start)
	}
	if a.TraceID != b.TraceID {
		return a.TraceID < b.TraceID
	}
	return a.SpanID < b.SpanID
}

// WithEvidence verifies bounded, complete trace contexts for every selected
// seed; a missing or truncated context fails the whole evaluation.
func (a AggregateEvaluation) WithEvidence(ctx context.Context, seeds []Span, contexts map[string][]Span) (Result, error) {
	out := a.Result
	if err := ctx.Err(); err != nil {
		return out, err
	}
	if len(seeds) > MaxEvidenceTraces || len(contexts) > MaxEvidenceTraces {
		return out, ErrLimit
	}
	rows, bytes := 0, 0
	identities := map[[2]string]bool{}
	for traceID, spans := range contexts {
		for _, s := range spans {
			if rows%256 == 0 {
				if err := ctx.Err(); err != nil {
					return out, err
				}
			}
			if s.TraceID != traceID || s.Start.Before(out.BaselineWindow.From) || !s.Start.Before(out.End) {
				return out, ErrLimit
			}
			id := [2]string{s.TraceID, s.SpanID}
			if identities[id] {
				return out, ErrLimit
			}
			identities[id] = true
			rows++
			bytes += Account(s)
			if rows > MaxSpans || bytes > MaxInputBytes {
				return out, ErrLimit
			}
		}
	}
	selected := map[key][]Span{}
	for _, s := range seeds {
		if s.Kind != 2 || s.Start.Before(out.CurrentWindow.From) || !s.Start.Before(out.End) {
			return out, ErrLimit
		}
		k := spanKey(s)
		if !a.candidates[k] {
			continue
		}
		selected[k] = append(selected[k], s)
		if len(selected[k]) > 5 {
			return out, ErrLimit
		}
	}
	budget := &responseBudget{remaining: query.MaxResponse - 4096}
	for _, op := range out.Operations {
		encoded, err := json.Marshal(op)
		if err != nil {
			return out, err
		}
		copies := 1
		if op.State == "candidate" {
			copies = 2
		}
		if !budget.charge(copies*(len(encoded)+1) + 32) {
			return out, ErrLimit
		}
	}
	for i := range out.Operations {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		op := &out.Operations[i]
		if op.State != "candidate" {
			continue
		}
		k := key{op.Namespace, op.Service, op.Operation}
		ordered := selected[k]
		sort.Slice(ordered, func(i, j int) bool { return evidenceLess(ordered[i], ordered[j]) })
		seen := map[string]bool{}
		for j := range ordered {
			s := &ordered[j]
			if seen[s.TraceID] {
				continue
			}
			seen[s.TraceID] = true
			rows := contexts[s.TraceID]
			if len(rows) == 0 {
				return out, ErrLimit
			}
			ptrs := make([]*Span, len(rows))
			found := false
			for n := range rows {
				ptrs[n] = &rows[n]
				if rows[n].SpanID == s.SpanID {
					if rows[n].Kind != 2 || spanKey(rows[n]) != k || rows[n].Status != s.Status || rows[n].Duration != s.Duration || !rows[n].Start.Equal(s.Start) {
						return out, ErrLimit
					}
					found = true
				}
			}
			if !found {
				return out, ErrLimit
			}
			info, err := makeTraceInfo(ctx, ptrs)
			if err != nil {
				return out, err
			}
			for _, c := range info.caveats {
				if !slices.Contains(op.Caveats, c) && !budget.charge(2*(len(c)+4)) {
					return out, ErrLimit
				}
				op.Caveats = addCaveat(op.Caveats, c)
			}
			ev, err := traceEvidence(ctx, s, info, a.candidates, query.Window{From: out.BaselineWindow.From, To: out.End}, budget)
			if err != nil {
				return out, err
			}
			op.Evidence = append(op.Evidence, ev)
			if len(op.Evidence) == 5 {
				break
			}
		}
		if len(op.Evidence) == 0 {
			return out, ErrLimit
		}
		sort.Strings(op.Caveats)
		out.Candidates = append(out.Candidates, *op)
	}
	rankCandidates(&out)
	return out, ctx.Err()
}
