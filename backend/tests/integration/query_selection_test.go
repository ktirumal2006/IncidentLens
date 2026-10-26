package integration

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"incidentlens/backend/internal/detector"
	"incidentlens/backend/internal/trace"
)

// This exercises the selective evidence SQL through Incidents and real FINAL
// reads. Three candidate operations share a namespace; two share a service,
// while one trace contributes a span to all three operations.
func TestDetectorClickHouseSelectiveEvidenceAcrossOperations(t *testing.T) {
	reader := detectorDatabase(t)
	writer, conn := database(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Exec(ctx, "SYSTEM STOP MERGES incidentlens.spans"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		restore, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := conn.Exec(restore, "SYSTEM START MERGES incidentlens.spans"); err != nil {
			t.Errorf("restore merges: %v", err)
		}
	}()

	end := time.Now().UTC().Add(-5 * time.Hour).Truncate(time.Second)
	observed := time.Now().UTC()
	ns := "selective-seeds-" + queryID(t, 5)
	prefix := queryID(t, 8)
	id := func(n int) string { return prefix + fmt.Sprintf("%016x", n) }
	spanID := func(n int) string { return fmt.Sprintf("%016x", n) }
	baseTime, currentTime := end.Add(-34*time.Minute), end.Add(-4*time.Minute)
	rows := make([]trace.Row, 0, 20)
	add := func(traceNumber, spanNumber int, service, operation string, start time.Time, duration uint64, status uint8) trace.Row {
		r := querySpan(id(traceNumber), spanID(spanNumber), "", service, operation, start, duration, 2, status)
		r.ServiceNamespace = ns
		r.IngestedAt = observed.Add(-time.Second)
		rows = append(rows, r)
		return r
	}
	for i, op := range []struct{ service, name string }{{"shared", "alpha"}, {"shared", "beta"}, {"other", "gamma"}} {
		add(90+i, 90+i, op.service, op.name, baseTime, 10, 1)
	}
	// Alpha has seven distinct current traces. A has two equally ranked spans;
	// span ID breaks that tie. F and the long OK span G must be excluded.
	first := add(1, 1, "shared", "alpha", currentTime, 100, 2)
	add(1, 2, "shared", "alpha", currentTime, 100, 2)
	add(2, 3, "shared", "alpha", currentTime, 100, 2)
	add(3, 4, "shared", "alpha", currentTime.Add(time.Millisecond), 100, 2)
	add(4, 5, "shared", "alpha", currentTime, 90, 2)
	add(5, 6, "shared", "alpha", currentTime, 80, 2)
	add(6, 7, "shared", "alpha", currentTime, 70, 2)
	add(7, 8, "shared", "alpha", currentTime, 1000, 1)
	// Shared trace IDs across operation/service boundaries must not suppress a
	// candidate's own seed or remove its context spans.
	add(1, 9, "shared", "beta", currentTime, 120, 2)
	add(2, 10, "shared", "beta", currentTime, 110, 2)
	add(1, 11, "other", "gamma", currentTime, 130, 2)
	late := querySpan(id(8), spanID(12), "", "shared", "alpha", currentTime, 2000, 2, 2)
	late.ServiceNamespace, late.IngestedAt = ns, observed.Add(time.Second)
	rows = append(rows, late)
	detectorWrite(t, writer, rows)
	detectorWrite(t, writer, []trace.Row{first})
	var physical uint64
	if err := conn.QueryRow(context.Background(), "SELECT count() FROM spans WHERE trace_id=? AND service_namespace=? AND service_name='shared' AND span_name='alpha'", id(1), ns).Scan(&physical); err != nil {
		t.Fatal(err)
	}
	if physical != 3 {
		t.Fatalf("physical replay rows=%d want 3", physical)
	}

	cfg := detector.DefaultConfig()
	cfg.MinSamples = 1
	cfg.LatencyDeltaNS = "1"
	result, err := reader.Incidents(context.Background(), end, observed, cfg, detector.Filter{Namespace: &ns})
	if err != nil {
		t.Fatal(err)
	}
	if result.RuleVersion != detector.AggregateVersion || len(result.Operations) != 3 || len(result.Candidates) != 3 {
		t.Fatalf("candidate operation set=%+v", result)
	}
	byOperation := map[string]detector.Operation{}
	for _, op := range result.Candidates {
		byOperation[op.Service+"/"+op.Operation] = op
	}
	alpha, ok := byOperation["shared/alpha"]
	if !ok || alpha.Baseline.Count != 1 || alpha.Current.Count != 8 || alpha.Current.ErrorCount != 7 {
		t.Fatalf("alpha FINAL counts=%+v", alpha)
	}
	gotIDs := make([]string, 0, len(alpha.Evidence))
	for _, ev := range alpha.Evidence {
		gotIDs = append(gotIDs, ev.TraceID)
	}
	if want := []string{id(1), id(2), id(3), id(4), id(5)}; !reflect.DeepEqual(gotIDs, want) || alpha.Evidence[0].SpanID != spanID(1) {
		t.Fatalf("alpha distinct trace/tie order=%v first_span=%s want=%v", gotIDs, alpha.Evidence[0].SpanID, want)
	}
	for key, want := range map[string][]string{"shared/beta": {id(1), id(2)}, "other/gamma": {id(1)}} {
		op, ok := byOperation[key]
		if !ok {
			t.Fatalf("missing operation %s", key)
		}
		ids := make([]string, 0, len(op.Evidence))
		for _, ev := range op.Evidence {
			ids = append(ids, ev.TraceID)
		}
		if !reflect.DeepEqual(ids, want) {
			t.Fatalf("%s evidence trace IDs=%v want=%v", key, ids, want)
		}
	}
	// The same frozen cutoff excludes the later receipt, while a fresh
	// observation makes its stronger error span the first alpha seed.
	fresh, err := reader.Incidents(context.Background(), end, observed.Add(2*time.Second), cfg, detector.Filter{Namespace: &ns})
	if err != nil {
		t.Fatal(err)
	}
	for _, op := range fresh.Candidates {
		if op.Service == "shared" && op.Operation == "alpha" {
			if op.Current.Count != 9 || len(op.Evidence) != 5 || op.Evidence[0].TraceID != id(8) {
				t.Fatalf("fresh alpha selection=%+v", op)
			}
			return
		}
	}
	t.Fatal("fresh alpha candidate missing")
}
