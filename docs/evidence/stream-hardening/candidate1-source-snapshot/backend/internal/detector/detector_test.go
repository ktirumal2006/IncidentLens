package detector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"incidentlens/backend/internal/query"
	"reflect"
	"strings"
	"testing"
	"time"
)

var fixedEnd = time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

func fixture(ns, svc, op string, n int, baseline bool, duration uint64, status uint8) []Span {
	out := make([]Span, 0, n)
	start := fixedEnd.Add(-4 * time.Minute)
	offset := 0
	if baseline {
		start = fixedEnd.Add(-34 * time.Minute)
		offset = 10000
	}
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(ns + "\x00" + svc + "\x00" + op))
	for i := 0; i < n; i++ {
		out = append(out, Span{TraceID: fmt.Sprintf("%016x%016x", hash.Sum64(), i+1+offset), SpanID: fmt.Sprintf("%016x", i+1), ServiceNamespace: ns, ServiceName: svc, Operation: op, Kind: 2, Status: status, Start: start.Add(time.Duration(i) * time.Millisecond), Duration: duration})
	}
	return out
}
func eval(t *testing.T, rows []Span, cfg Config) Result {
	t.Helper()
	r, e := Evaluate(rows, fixedEnd, fixedEnd.Add(time.Minute), cfg, Filter{})
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func TestHealthyLatencyErrorsAndRecovery(t *testing.T) {
	cfg := DefaultConfig()
	healthy := append(fixture("shop", "svc", "op", 100, true, 100_000_000, 1), fixture("shop", "svc", "op", 100, false, 100_000_000, 1)...)
	r := eval(t, healthy, cfg)
	if len(r.Candidates) != 0 || r.Operations[0].State != "normal" {
		t.Fatalf("healthy=%+v", r)
	}
	slow := append(fixture("shop", "svc", "op", 100, true, 100_000_000, 1), fixture("shop", "svc", "op", 100, false, 200_000_000, 1)...)
	r = eval(t, slow, cfg)
	if len(r.Candidates) != 1 || !reflect.DeepEqual(r.Candidates[0].TriggeredRules, []string{"latency"}) || *r.Candidates[0].P95IncreaseNS != "100000000" {
		t.Fatalf("latency boundary=%+v", r.Candidates)
	}
	fault := append(fixture("shop", "svc", "op", 100, true, 100_000_000, 1), fixture("shop", "svc", "op", 100, false, 100_000_000, 1)...)
	for i := 100; i < 105; i++ {
		fault[i].Status = 2
	}
	r = eval(t, fault, cfg)
	if len(r.Candidates) != 1 || !reflect.DeepEqual(r.Candidates[0].TriggeredRules, []string{"errors"}) {
		t.Fatalf("error boundary=%+v", r.Candidates)
	}
	r = eval(t, healthy, cfg)
	if len(r.Candidates) != 0 {
		t.Fatal("recovery not clear")
	}
}
func TestZeroBaselineLowVolumeUnsetAndExactP95(t *testing.T) {
	cfg := DefaultConfig()
	rows := append(fixture("", "svc", "op", 100, true, 0, 0), fixture("", "svc", "op", 100, false, 100_000_000, 0)...)
	r := eval(t, rows, cfg)
	if len(r.Candidates) != 1 || r.Candidates[0].Current.UnsetCount != 100 || r.Candidates[0].Current.ErrorRate != 0 {
		t.Fatalf("zero baseline/UNSET=%+v", r)
	}
	if !contains(r.Candidates[0].Caveats, "unset_status") {
		t.Fatal("missing unset caveat")
	}
	rows = rows[:199]
	r = eval(t, rows, cfg)
	if r.Operations[0].State != "insufficient_evidence" || len(r.Candidates) != 0 {
		t.Fatalf("low volume=%+v", r)
	}
	spans := fixture("", "svc", "op", 20, true, 1, 1)
	for i := range spans {
		spans[i].Duration = uint64(i + 1)
	}
	st := stats(spanPointers(spans))
	if st.P95 == nil || *st.P95 != "20" {
		t.Fatalf("discrete p95=%+v", st)
	}
}
func TestRankingEvidenceAndRelationships(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MinSamples = 2
	cfg.LatencyDeltaNS = "1"
	rows := append(fixture("", "a", "op", 2, true, 10, 1), fixture("", "a", "op", 2, false, 20, 2)...)
	rows = append(rows, fixture("", "b", "op", 2, true, 10, 1)...)
	rows = append(rows, fixture("", "b", "op", 2, false, 20, 2)...)
	for i := range rows {
		rows[i].TraceID = fmt.Sprintf("%032x", i+1)
	}
	parent := rows[2]
	child := Span{TraceID: parent.TraceID, SpanID: "ffffffffffffffff", ParentSpanID: parent.SpanID, ServiceName: "b", Operation: "op", Kind: 2, Status: 2, Start: parent.Start.Add(time.Nanosecond), Duration: 20, Events: json.RawMessage(`{"events":[{"droppedAttributesCount":1}]}`)}
	rows = append(rows, child)
	r := eval(t, rows, cfg)
	if len(r.Candidates) != 2 || r.Candidates[0].Service != "a" || r.Candidates[0].ServiceRank != 1 || r.Candidates[1].ServiceRank != 2 {
		t.Fatalf("ranking=%+v", r.Candidates)
	}
	ev := r.Candidates[0].Evidence[0]
	if !ev.ErrorPropagation || len(ev.DownstreamAnomalies) != 1 || ev.DownstreamAnomalies[0].Service != "b" || !contains(ev.Caveats, "source_drops") {
		t.Fatalf("relationship evidence=%+v", ev)
	}
	r2 := eval(t, rows, cfg)
	r.ObservedAt = time.Time{}
	r2.ObservedAt = time.Time{}
	if !reflect.DeepEqual(r, r2) {
		t.Fatal("frozen evaluation changed")
	}
}
func TestMissingRelationshipsAndNoCoverage(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MinSamples = 1
	cfg.LatencyDeltaNS = "1"
	rows := append(fixture("", "svc", "op", 1, true, 1, 1), fixture("", "svc", "op", 1, false, 2, 2)...)
	rows[1].ParentSpanID = "missing"
	r := eval(t, rows, cfg)
	if !contains(r.Candidates[0].Evidence[0].Caveats, "missing_parent") || !contains(r.Candidates[0].Evidence[0].Caveats, "missing_root") {
		t.Fatalf("missing relation=%+v", r.Candidates[0].Evidence)
	}
	client := Span{TraceID: "a", SpanID: "b", Kind: 3, Start: fixedEnd.Add(-time.Minute)}
	r = eval(t, []Span{client}, cfg)
	if len(r.Operations) != 0 || !contains(r.Caveats, "no_server_coverage") {
		t.Fatalf("client-only=%+v", r)
	}
}
func TestLimitsAndConfig(t *testing.T) {
	cfg := DefaultConfig()
	if _, e := ConfigFromEnv(func(k string) string {
		if k == "DETECTOR_MIN_SAMPLES" {
			return "0"
		}
		return ""
	}); e == nil {
		t.Fatal("invalid config accepted")
	}
	for _, tc := range []struct{ end, now time.Time }{{fixedEnd, fixedEnd.Add(-8 * 24 * time.Hour)}, {fixedEnd, fixedEnd.Add(8 * 24 * time.Hour)}} {
		if ValidateEnd(tc.end, tc.now) == nil {
			t.Fatal("invalid end accepted")
		}
	}
	rows := make([]Span, MaxSpans+1)
	for i := range rows {
		rows[i] = Span{TraceID: fmt.Sprintf("%032x", i+1), SpanID: "1", Start: fixedEnd.Add(-time.Minute)}
	}
	if _, e := Evaluate(rows, fixedEnd, fixedEnd.Add(time.Minute), cfg, Filter{}); !errors.Is(e, ErrLimit) {
		t.Fatal("span limit not enforced")
	}
	rows = []Span{{Start: fixedEnd.Add(-time.Minute), Events: json.RawMessage(fmt.Sprintf(`{"events":"%s"}`, string(make([]byte, MaxInputBytes))))}}
	if Account(rows[0]) <= MaxInputBytes {
		t.Fatal("byte accounting")
	}
	if _, e := Evaluate(rows, fixedEnd, fixedEnd.Add(time.Minute), cfg, Filter{}); !errors.Is(e, ErrLimit) {
		t.Fatalf("input byte cap=%v", e)
	}
}
func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}
func TestDuplicateOutsideWindowAndServerDenominator(t *testing.T) {
	cfg := DefaultConfig()
	rows := append(fixture("", "svc", "op", 100, true, 100_000_000, 1), fixture("", "svc", "op", 100, false, 200_000_000, 1)...)
	expected := eval(t, rows, cfg)
	replay := append(append([]Span{}, rows...), rows...)
	client := rows[0]
	client.Kind = 3
	client.SpanID = "client"
	client.Duration = ^uint64(0)
	client.Status = 2
	outside := rows[0]
	outside.TraceID = "outside"
	outside.Start = fixedEnd
	replay = append(replay, client, outside)
	got := eval(t, replay, cfg)
	if !reflect.DeepEqual(got.Candidates, expected.Candidates) || got.Operations[0].Baseline.Count != 100 || got.Operations[0].Current.Count != 100 {
		t.Fatalf("duplicate/window/client changed denominator: %+v", got.Operations)
	}
}
func TestErrorBoundaryAndRatioDeltaBothRequired(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MinSamples = 1
	for _, tc := range []struct {
		name            string
		n, errors       int
		baseDur, curDur uint64
		candidate       bool
	}{{"exact error threshold", 20, 1, 100_000_000, 100_000_000, true}, {"below error threshold", 21, 1, 100_000_000, 100_000_000, false}, {"ratio only", 20, 0, 1, 2, false}, {"delta only", 20, 0, 200_000_000, 300_000_000, false}} {
		t.Run(tc.name, func(t *testing.T) {
			rows := append(fixture("", "svc", "op", tc.n, true, tc.baseDur, 1), fixture("", "svc", "op", tc.n, false, tc.curDur, 1)...)
			for i := tc.n; i < tc.n+tc.errors; i++ {
				rows[i].Status = 2
			}
			r := eval(t, rows, cfg)
			if (len(r.Candidates) > 0) != tc.candidate {
				t.Fatalf("candidates=%+v", r.Candidates)
			}
		})
	}
}
func TestFilteredCandidateKeepsDownstreamContextAndFullInterval(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MinSamples = 1
	cfg.LatencyDeltaNS = "1"
	rows := append(fixture("", "parent", "op", 1, true, 1, 1), fixture("", "parent", "op", 1, false, 2, 2)...)
	rows = append(rows, fixture("", "child", "op", 1, true, 1, 1)...)
	child := Span{TraceID: rows[1].TraceID, SpanID: "child", ParentSpanID: rows[1].SpanID, ServiceName: "child", Operation: "op", Kind: 2, Status: 2, Start: rows[1].Start.Add(time.Millisecond), Duration: 2}
	rows = append(rows, child)
	service := "parent"
	r, e := Evaluate(rows, fixedEnd, fixedEnd.Add(time.Minute), cfg, Filter{Service: &service})
	if e != nil {
		t.Fatal(e)
	}
	if len(r.Operations) != 1 || len(r.Candidates) != 1 || len(r.Candidates[0].Evidence[0].DownstreamAnomalies) != 1 {
		t.Fatalf("filtered context=%+v", r)
	}
	ev := r.Candidates[0].Evidence[0]
	if ev.From != fixedEnd.Add(-35*time.Minute) || ev.To != fixedEnd {
		t.Fatalf("evidence interval=%+v", ev)
	}
	if contains(ev.Caveats, "missing_parent") {
		t.Fatal("present cross-service parent reported missing")
	}
}
func TestEvidenceSelectionAndCycle(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MinSamples = 1
	cfg.LatencyDeltaNS = "1"
	rows := append(fixture("", "svc", "op", 8, true, 1, 1), fixture("", "svc", "op", 8, false, 10, 1)...)
	for i := 8; i < 16; i++ {
		rows[i].Duration = uint64(i)
		if i%2 == 0 {
			rows[i].Status = 2
		}
	}
	duplicateTrace := rows[15]
	duplicateTrace.SpanID = "extra"
	duplicateTrace.Duration = 100
	duplicateTrace.Status = 2
	rows = append(rows, duplicateTrace)
	r := eval(t, rows, cfg)
	ev := r.Candidates[0].Evidence
	if len(ev) != 5 || ev[0].SpanID != "extra" {
		t.Fatalf("evidence ordering=%+v", ev)
	}
	seen := map[string]bool{}
	for _, e := range ev {
		if seen[e.TraceID] {
			t.Fatal("duplicate evidence trace")
		}
		seen[e.TraceID] = true
	}
	info, e := makeTraceInfo(context.Background(), spanPointers([]Span{{SpanID: "a", ParentSpanID: "b"}, {SpanID: "b", ParentSpanID: "a"}}))
	if e != nil || !contains(info.caveats, "cycle") || !contains(info.caveats, "missing_root") {
		t.Fatalf("cycle caveats=%v err=%v", info, e)
	}
}
func TestMissingWindowGroupLimitsAndCancellation(t *testing.T) {
	cfg := DefaultConfig()
	rows := fixture("", "only-current", "op", 1, false, 1, 0)
	r := eval(t, rows, cfg)
	if r.Operations[0].Baseline.P95 != nil || r.Operations[0].P95IncreaseNS != nil || r.Operations[0].ErrorRateIncrease != nil || r.Operations[0].State != "insufficient_evidence" {
		t.Fatalf("missing window=%+v", r)
	}
	rows = nil
	for i := 0; i < MaxGroups+1; i++ {
		x := fixture("", "svc", fmt.Sprint(i), 1, false, 1, 1)[0]
		x.TraceID = fmt.Sprintf("%032x", i+1)
		rows = append(rows, x)
	}
	if _, e := Evaluate(rows, fixedEnd, fixedEnd.Add(time.Minute), cfg, Filter{}); !errors.Is(e, ErrLimit) {
		t.Fatalf("group cap=%v", e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e := EvaluateContext(ctx, nil, fixedEnd, fixedEnd.Add(time.Minute), cfg, Filter{}); !errors.Is(e, context.Canceled) {
		t.Fatalf("cancellation=%v", e)
	}
}
func TestConfigurationFields(t *testing.T) {
	for _, env := range []map[string]string{{"DETECTOR_MIN_SAMPLES": "0"}, {"DETECTOR_LATENCY_RATIO_MILLI": "999"}, {"DETECTOR_LATENCY_DELTA_NS": "0"}, {"DETECTOR_ERROR_RATE_BPS": "10001"}, {"DETECTOR_ERROR_INCREASE_BPS": "-1"}, {"DETECTOR_MIN_SAMPLES": "abc"}} {
		if _, e := ConfigFromEnv(func(k string) string { return env[k] }); e == nil {
			t.Fatalf("accepted config=%v", env)
		}
	}
	cfg, e := ConfigFromEnv(func(k string) string {
		if k == "DETECTOR_MIN_SAMPLES" {
			return "2"
		}
		return ""
	})
	if e != nil || cfg.MinSamples != 2 {
		t.Fatalf("valid config=%+v %v", cfg, e)
	}
}
func TestResponseExpansionFailsBeforeUnboundedSerialization(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MinSamples = 1
	cfg.LatencyDeltaNS = "1"
	rows := []Span{}
	for i := 0; i < 100; i++ {
		op := fmt.Sprintf("%03d-", i) + strings.Repeat("x", 4000)
		base := fixture("", "svc", op, 1, true, 1, 1)[0]
		current := fixture("", "svc", op, 1, false, 2, 2)[0]
		current.TraceID = "shared-current-trace"
		current.SpanID = fmt.Sprint(i + 1)
		if i > 0 {
			current.ParentSpanID = fmt.Sprint(i)
		}
		rows = append(rows, base, current)
	}
	if _, e := Evaluate(rows, fixedEnd, fixedEnd.Add(time.Minute), cfg, Filter{}); !errors.Is(e, ErrLimit) {
		t.Fatalf("response expansion cap=%v", e)
	}
}
func TestConfigJSONSafeIntegersAndCanonicalDelta(t *testing.T) {
	for _, cfg := range []Config{{MinSamples: 9007199254740992, LatencyRatioMilli: 2000, LatencyDeltaNS: "1", ErrorRateBPS: 1, ErrorIncreaseBPS: 1}, {MinSamples: 1, LatencyRatioMilli: 9007199254740992, LatencyDeltaNS: "1", ErrorRateBPS: 1, ErrorIncreaseBPS: 1}, {MinSamples: 1, LatencyRatioMilli: 1000, LatencyDeltaNS: "01", ErrorRateBPS: 1, ErrorIncreaseBPS: 1}} {
		if cfg.Validate() == nil {
			t.Fatalf("unsafe/noncanonical config accepted: %+v", cfg)
		}
	}
}

func spanPointers(rows []Span) []*Span {
	out := make([]*Span, len(rows))
	for i := range rows {
		out[i] = &rows[i]
	}
	return out
}
func TestInputPermutationCandidateTiesAndServiceRanks(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MinSamples = 2
	cfg.LatencyDeltaNS = "1"
	rows := []Span{}
	for _, identity := range []Identity{{"", "a", "a"}, {"", "a", "b"}, {"", "b", "a"}} {
		rows = append(rows, fixture(identity.Namespace, identity.Service, identity.Operation, 2, true, 1, 1)...)
		rows = append(rows, fixture(identity.Namespace, identity.Service, identity.Operation, 2, false, 2, 2)...)
	}
	first := eval(t, rows, cfg)
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}
	second := eval(t, rows, cfg)
	if !reflect.DeepEqual(first, second) {
		t.Fatal("input permutation changed deterministic evaluation")
	}
	if len(first.Candidates) != 3 || first.Candidates[0].Service != "a" || first.Candidates[0].Operation != "a" || first.Candidates[1].Operation != "b" || first.Candidates[0].ServiceRank != 1 || first.Candidates[1].ServiceRank != 1 || first.Candidates[2].ServiceRank != 2 {
		t.Fatalf("candidate identity ties/service ranks=%+v", first.Candidates)
	}
}
func TestEvidenceAllOrderingFields(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MinSamples = 1
	cfg.LatencyDeltaNS = "1"
	rows := fixture("", "svc", "op", 7, true, 1, 1)
	for _, tc := range []struct {
		id, span int
		status   uint8
		duration uint64
		second   int
	}{{1, 11, 2, 30, 3}, {1, 10, 2, 30, 3}, {2, 12, 2, 20, 2}, {3, 13, 1, 50, 1}, {4, 14, 1, 40, 2}, {5, 15, 1, 40, 1}, {6, 16, 1, 40, 1}, {7, 17, 1, 40, 1}} {
		rows = append(rows, Span{TraceID: fmt.Sprintf("%032x", tc.id), SpanID: fmt.Sprintf("%016x", tc.span), ServiceName: "svc", Operation: "op", Kind: 2, Status: tc.status, Duration: tc.duration, Start: fixedEnd.Add(-4 * time.Minute).Add(time.Duration(tc.second) * time.Second)})
	}
	r := eval(t, rows, cfg)
	got := r.Candidates[0].Evidence
	wantIDs := []int{1, 2, 3, 5, 6}
	if len(got) != 5 {
		t.Fatal(len(got))
	}
	for i, id := range wantIDs {
		if got[i].TraceID != fmt.Sprintf("%032x", id) {
			t.Fatalf("evidence order=%+v", got)
		}
	}
	if got[0].SpanID != fmt.Sprintf("%016x", 10) {
		t.Fatalf("span-ID tie chose wrong representative: %+v", got[0])
	}
}
func TestExactRationalThresholdsAndWideSignedDurations(t *testing.T) {
	if !rateGE(1, 3, 3333) || rateGE(1, 3, 3334) {
		t.Fatal("fractional error-rate boundary rounded")
	}
	b := Stats{Count: 3, ErrorCount: 1}
	c := Stats{Count: 3, ErrorCount: 2}
	if !deltaGE(b, c, 3333) || deltaGE(b, c, 3334) {
		t.Fatal("fractional rate increase boundary rounded")
	}
	cfg := DefaultConfig()
	cfg.MinSamples = 1
	rows := append(fixture("", "svc", "op", 1, true, ^uint64(0), 1), fixture("", "svc", "op", 1, false, 0, 2)...)
	r := eval(t, rows, cfg)
	if len(r.Candidates) != 1 || *r.Candidates[0].P95IncreaseNS != "-18446744073709551615" {
		t.Fatalf("wide signed duration=%+v", r)
	}
}
func TestNestedEventLinkDropsAndBrokenErrorChain(t *testing.T) {
	for _, s := range []*Span{{Events: json.RawMessage(`{"events":[{"droppedAttributesCount":1}]}`)}, {Links: json.RawMessage(`{"links":[{"droppedAttributesCount":1}]}`)}} {
		if !hasDrops(s) {
			t.Fatal("nested source loss missed")
		}
	}
	parent := Span{TraceID: "trace", SpanID: "parent", ServiceName: "parent", Operation: "op", Kind: 2, Status: 2}
	bridge := Span{TraceID: "trace", SpanID: "bridge", ParentSpanID: "parent", Kind: 3, Status: 1}
	child := Span{TraceID: "trace", SpanID: "child", ParentSpanID: "bridge", ServiceName: "child", Operation: "op", Kind: 2, Status: 2}
	info, e := makeTraceInfo(context.Background(), []*Span{&parent, &bridge, &child})
	if e != nil {
		t.Fatal(e)
	}
	ev, e := traceEvidence(context.Background(), &parent, info, map[key]bool{{svc: "child", op: "op"}: true}, query.Window{}, &responseBudget{remaining: 8 << 20})
	if e != nil || ev.ErrorPropagation || len(ev.DownstreamAnomalies) != 1 {
		t.Fatalf("broken error chain/cooccurrence=%+v err=%v", ev, e)
	}
}
