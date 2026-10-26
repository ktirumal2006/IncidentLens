package detector

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"
)

func aggregateFixture(rows []Span) ([]AggregateGroup, []Span, map[string][]Span) {
	buckets := map[key]*bucket{}
	contexts := map[string][]Span{}
	for _, s := range rows {
		contexts[s.TraceID] = append(contexts[s.TraceID], s)
		if s.Kind != 2 {
			continue
		}
		k := spanKey(s)
		b := buckets[k]
		if b == nil {
			b = &bucket{}
			buckets[k] = b
		}
		v := s
		if s.Start.Before(fixedEnd.Add(-5 * time.Minute)) {
			b.base = append(b.base, &v)
		} else {
			b.cur = append(b.cur, &v)
		}
	}
	groups, seeds := []AggregateGroup{}, []Span{}
	for _, k := range sortedKeys(buckets) {
		b := buckets[k]
		groups = append(groups, AggregateGroup{Identity: identity(k), Baseline: stats(b.base), Current: stats(b.cur)})
		ordered := make([]Span, 0, len(b.cur))
		for _, s := range b.cur {
			ordered = append(ordered, *s)
		}
		sort.Slice(ordered, func(i, j int) bool { return evidenceLess(ordered[i], ordered[j]) })
		seen := map[string]bool{}
		for _, s := range ordered {
			if seen[s.TraceID] {
				continue
			}
			seeds = append(seeds, s)
			seen[s.TraceID] = true
			if len(seen) == 5 {
				break
			}
		}
	}
	return groups, seeds, contexts
}

func TestAggregateRulesAndSelectedEvidenceMatchV1(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MinSamples = 2
	cfg.LatencyDeltaNS = "1"
	rows := append(fixture("", "parent", "op", 2, true, 10, 1), fixture("", "parent", "op", 2, false, 20, 2)...)
	rows = append(rows, fixture("", "child", "op", 2, true, 10, 1)...)
	for i := 0; i < 2; i++ {
		child := Span{TraceID: rows[2+i].TraceID, SpanID: "fffffffffffffff" + string(rune('0'+i)), ParentSpanID: rows[2+i].SpanID, ServiceName: "child", Operation: "op", Kind: 2, Status: 2, Start: rows[2+i].Start.Add(time.Nanosecond), Duration: 20}
		rows = append(rows, child)
	}
	service := "parent"
	old, err := Evaluate(rows, fixedEnd, fixedEnd.Add(time.Minute), cfg, Filter{Service: &service})
	if err != nil {
		t.Fatal(err)
	}
	groups, seeds, contexts := aggregateFixture(rows)
	a, err := PrepareAggregates(context.Background(), groups, fixedEnd, fixedEnd.Add(time.Minute), cfg, Filter{Service: &service})
	if err != nil {
		t.Fatal(err)
	}
	got, err := a.WithEvidence(context.Background(), seeds, contexts)
	if err != nil {
		t.Fatal(err)
	}
	if got.RuleVersion != AggregateVersion || len(got.Operations) != 1 || len(got.Candidates) != 1 || !reflect.DeepEqual(got.Candidates[0].Baseline, old.Candidates[0].Baseline) || !reflect.DeepEqual(got.Candidates[0].Current, old.Candidates[0].Current) || !reflect.DeepEqual(got.Candidates[0].Evidence, old.Candidates[0].Evidence) || !reflect.DeepEqual(got.Candidates[0].TriggeredRules, old.Candidates[0].TriggeredRules) || got.Candidates[0].ServiceRank != old.Candidates[0].ServiceRank {
		t.Fatalf("v2/v1 rule or evidence mismatch: got=%+v old=%+v", got.Candidates, old.Candidates)
	}
	if !contains(got.Caveats, "relationship_context_sampled") || !contains(got.Operations[0].Caveats, "trace_context_not_evaluated") || len(got.Candidates[0].Evidence[0].DownstreamAnomalies) != 1 {
		t.Fatalf("scope or cross-service evidence=%+v", got)
	}
}

func TestAggregateBeyondOldInputCapAndBounds(t *testing.T) {
	now := fixedEnd.Add(time.Minute)
	p1, p2 := "1", "2"
	g := AggregateGroup{Identity: Identity{Service: "svc", Operation: "op"}, Baseline: Stats{Count: 150000, P95: &p1}, Current: Stats{Count: 180000, P95: &p2}}
	a, err := PrepareAggregates(context.Background(), []AggregateGroup{g}, fixedEnd, now, DefaultConfig(), Filter{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := a.WithEvidence(context.Background(), nil, nil)
	if err != nil || got.Operations[0].Baseline.Count != 150000 || got.Operations[0].Current.Count != 180000 {
		t.Fatalf("aggregate larger than old input cap: %+v %v", got, err)
	}
	if _, err := PrepareAggregates(context.Background(), []AggregateGroup{g, g}, fixedEnd, now, DefaultConfig(), Filter{}); !errors.Is(err, ErrLimit) {
		t.Fatalf("duplicate group: %v", err)
	}
	for _, bad := range []string{"", "01", "-1", "18446744073709551616", "not-a-number"} {
		badGroup := g
		badGroup.Current.P95 = &bad
		if _, err := PrepareAggregates(context.Background(), []AggregateGroup{badGroup}, fixedEnd, now, DefaultConfig(), Filter{}); !errors.Is(err, ErrLimit) {
			t.Fatalf("malformed p95 %q: %v", bad, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := PrepareAggregates(ctx, []AggregateGroup{g}, fixedEnd, now, DefaultConfig(), Filter{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled prepare: %v", err)
	}
}

func TestAggregateCandidateMissingContextFailsWholeEvaluation(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MinSamples = 1
	cfg.LatencyDeltaNS = "1"
	rows := append(fixture("", "svc", "op", 1, true, 1, 1), fixture("", "svc", "op", 1, false, 2, 2)...)
	groups, seeds, _ := aggregateFixture(rows)
	a, err := PrepareAggregates(context.Background(), groups, fixedEnd, fixedEnd.Add(time.Minute), cfg, Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.WithEvidence(context.Background(), seeds, nil); !errors.Is(err, ErrLimit) {
		t.Fatalf("missing selected trace must not return partial evidence: %v", err)
	}
	changed := map[string][]Span{seeds[0].TraceID: {{TraceID: seeds[0].TraceID, SpanID: seeds[0].SpanID, Start: seeds[0].Start, Duration: seeds[0].Duration + 1, Kind: 2, Status: seeds[0].Status, ServiceName: seeds[0].ServiceName, Operation: seeds[0].Operation}}}
	if _, err := a.WithEvidence(context.Background(), seeds, changed); !errors.Is(err, ErrLimit) {
		t.Fatalf("seed/context mismatch: %v", err)
	}
	oversized := make([]Span, MaxSpans+1)
	for i := range oversized {
		oversized[i] = rows[1]
		oversized[i].SpanID = fmt.Sprintf("%016x", i+1)
	}
	if _, err := a.WithEvidence(context.Background(), seeds, map[string][]Span{rows[1].TraceID: oversized}); !errors.Is(err, ErrLimit) {
		t.Fatalf("oversized selected context: %v", err)
	}
}
