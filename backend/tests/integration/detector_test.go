package integration

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"incidentlens/backend/internal/detector"
	storage "incidentlens/backend/internal/storage/clickhouse"
	"incidentlens/backend/internal/trace"
)

func detectorDatabase(t *testing.T) *storage.QueryStore {
	t.Helper()
	if os.Getenv("INCIDENTLENS_DETECTOR_INTEGRATION") != "1" {
		t.Skip("set INCIDENTLENS_DETECTOR_INTEGRATION=1 with real local ClickHouse")
	}
	address := os.Getenv("CLICKHOUSE_TEST_ADDRESS")
	if address == "" {
		address = "127.0.0.1:19000"
	}
	s, e := storage.OpenQuery(address, "query", "local-query", "incidentlens")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}
func detectorWrite(t *testing.T, s *storage.Store, rows []trace.Row) {
	t.Helper()
	for len(rows) > 0 {
		n := len(rows)
		if n > 256 {
			n = 256
		}
		if e := s.Write(context.Background(), rows[:n]); e != nil {
			t.Fatal(e)
		}
		rows = rows[n:]
	}
}
func TestDetectorClickHouseReplayCutoffPercentileAndRelationships(t *testing.T) {
	reader := detectorDatabase(t)
	writer, conn := database(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if e := conn.Exec(ctx, "SYSTEM STOP MERGES incidentlens.spans"); e != nil {
		t.Fatal(e)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if e := conn.Exec(ctx, "SYSTEM START MERGES incidentlens.spans"); e != nil {
			t.Errorf("restore merges: %v", e)
		}
	}()
	end := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Second)
	observed := time.Now().UTC()
	ns := "detector-" + queryID(t, 5)
	rows := []trace.Row{}
	replays := []trace.Row{}
	for window := 0; window < 2; window++ {
		start := end.Add(-34 * time.Minute)
		status := uint8(1)
		parentDuration := uint64(100_000_000)
		childDuration := uint64(10_000_000)
		if window == 1 {
			start = end.Add(-4 * time.Minute)
			status = 2
			parentDuration = 400_000_000
			childDuration = 20_000_000
		}
		for i := 0; i < 100; i++ {
			id := queryID(t, 16)
			parentID, clientID, childID := queryID(t, 8), queryID(t, 8), queryID(t, 8)
			parent := querySpan(id, parentID, "", "parent", "Request", start.Add(time.Duration(i)*time.Millisecond), parentDuration+uint64(i)*1_000_000, 2, status)
			client := querySpan(id, clientID, parentID, "parent", "Outbound", parent.StartTime.Add(time.Nanosecond), 9_000_000_000, 3, 2)
			child := querySpan(id, childID, clientID, "child", "Downstream", parent.StartTime.Add(2*time.Nanosecond), childDuration, 2, status)
			for _, p := range []*trace.Row{&parent, &client, &child} {
				p.ServiceNamespace = ns
				p.IngestedAt = observed.Add(-time.Second)
			}
			if window == 1 && i == 0 {
				child.Links = `{"links":[{"droppedAttributesCount":1}]}`
			}
			rows = append(rows, parent, client, child)
			if i < 10 {
				replays = append(replays, parent)
			}
			if window == 1 && i == 99 {
				lateReplay := parent
				lateReplay.IngestedAt = observed.Add(time.Second)
				replays = append(replays, lateReplay)
			}
		}
	}
	detectorWrite(t, writer, rows)
	detectorWrite(t, writer, replays)
	var raw uint64
	if e := conn.QueryRow(context.Background(), "SELECT count() FROM spans WHERE service_namespace=? AND service_name='parent' AND span_kind=2", ns).Scan(&raw); e != nil {
		t.Fatal(e)
	}
	if raw != 221 {
		t.Fatalf("raw parent rows=%d want221", raw)
	}
	service := "parent"
	cfg := detector.DefaultConfig()
	result, e := reader.Incidents(context.Background(), end, observed, cfg, detector.Filter{Namespace: &ns, Service: &service})
	if e != nil {
		t.Fatal(e)
	}
	if len(result.Operations) != 1 || len(result.Candidates) != 1 {
		t.Fatalf("filtered evaluations=%+v", result)
	}
	op := result.Candidates[0]
	if op.Baseline.Count != 100 || op.Current.Count != 100 || op.Current.ErrorCount != 100 || *op.Baseline.P95 != "195000000" || *op.Current.P95 != "495000000" {
		t.Fatalf("dedup SERVER stats=%+v", op)
	}
	var exact uint64
	if e = conn.QueryRow(context.Background(), "SELECT quantileExact(0.95)(duration_ns) FROM spans FINAL WHERE service_namespace=? AND service_name='parent' AND span_kind=2 AND start_time>=fromUnixTimestamp64Nano(?) AND start_time<fromUnixTimestamp64Nano(?)", ns, end.Add(-5*time.Minute).UnixNano(), end.UnixNano()).Scan(&exact); e != nil {
		t.Fatal(e)
	}
	if *op.Current.P95 != strconv.FormatUint(exact, 10) {
		t.Fatalf("Go p95=%s CH=%d", *op.Current.P95, exact)
	}
	if len(op.Evidence) != 5 {
		t.Fatalf("evidence count=%d", len(op.Evidence))
	}
	for _, ev := range op.Evidence {
		if !ev.ErrorPropagation || len(ev.DownstreamAnomalies) != 1 || ev.DownstreamAnomalies[0].Service != "child" || !ev.From.Equal(end.Add(-35*time.Minute)) {
			t.Fatalf("all-service evidence=%+v", ev)
		}
	}
	again, e := reader.Incidents(context.Background(), end, observed, cfg, detector.Filter{Namespace: &ns, Service: &service})
	if e != nil || !reflect.DeepEqual(result, again) {
		t.Fatalf("frozen evaluation changed err=%v", e)
	}
	late := querySpan(queryID(t, 16), queryID(t, 8), "", "parent", "Request", end.Add(-time.Minute), 1_000_000_000, 2, 2)
	late.ServiceNamespace = ns
	late.IngestedAt = observed.Add(time.Second)
	detectorWrite(t, writer, []trace.Row{late})
	frozen, e := reader.Incidents(context.Background(), end, observed, cfg, detector.Filter{Namespace: &ns, Service: &service})
	if e != nil || frozen.Operations[0].Current.Count != 100 {
		t.Fatalf("late data leaked into cutoff: %v %+v", e, frozen)
	}
	fresh, e := reader.Incidents(context.Background(), end, observed.Add(2*time.Second), cfg, detector.Filter{Namespace: &ns, Service: &service})
	if e != nil || fresh.Operations[0].Current.Count != 101 {
		t.Fatalf("late refresh=%v %+v", e, fresh)
	}
	t.Logf("historical detector fixture namespace=%s end=%s raw_parent=%d dedup_base=100 dedup_current=100 p95=%s/%s candidates=%d", ns, end.Format(time.RFC3339Nano), raw, *op.Baseline.P95, *op.Current.P95, len(result.Candidates))
}
func TestDetectorClickHouseGroupCapFailsWholeEvaluation(t *testing.T) {
	reader := detectorDatabase(t)
	writer, _ := database(t)
	end := time.Now().UTC().Add(-4 * time.Hour).Truncate(time.Second)
	observed := time.Now().UTC()
	ns := "detector-cap-" + queryID(t, 5)
	rows := make([]trace.Row, 0, 501)
	for i := 0; i < 501; i++ {
		r := querySpan(queryID(t, 16), queryID(t, 8), "", "group-cap", fmt.Sprintf("op-%03d", i), end.Add(-time.Minute), 1, 2, 1)
		r.ServiceNamespace = ns
		rows = append(rows, r)
	}
	detectorWrite(t, writer, rows)
	_, e := reader.Incidents(context.Background(), end, observed.Add(time.Second), detector.DefaultConfig(), detector.Filter{Namespace: &ns})
	if e != detector.ErrLimit {
		t.Fatalf("group cap expected error, got %v", e)
	}
}
func TestDetectorClickHouseHalfOpenWindows(t *testing.T) {
	reader := detectorDatabase(t)
	writer, _ := database(t)
	end := time.Now().UTC().Add(-6 * time.Hour).Truncate(time.Second)
	ns := "detector-boundary-" + queryID(t, 5)
	rows := []trace.Row{}
	for _, tc := range []struct {
		start    time.Time
		duration uint64
		status   uint8
	}{{end.Add(-35 * time.Minute), 10, 1}, {end.Add(-5*time.Minute - time.Nanosecond), 10, 1}, {end.Add(-5 * time.Minute), 20, 2}, {end.Add(-time.Nanosecond), 20, 2}, {end, 999, 2}, {end.Add(-35*time.Minute - time.Nanosecond), 999, 2}} {
		r := querySpan(queryID(t, 16), queryID(t, 8), "", "boundary", "Request", tc.start, tc.duration, 2, tc.status)
		r.ServiceNamespace = ns
		rows = append(rows, r)
	}
	detectorWrite(t, writer, rows)
	cfg := detector.DefaultConfig()
	cfg.MinSamples = 2
	cfg.LatencyDeltaNS = "1"
	r, e := reader.Incidents(context.Background(), end, time.Now().UTC(), cfg, detector.Filter{Namespace: &ns})
	if e != nil {
		t.Fatal(e)
	}
	if len(r.Operations) != 1 || r.Operations[0].Baseline.Count != 2 || r.Operations[0].Current.Count != 2 || *r.Operations[0].Baseline.P95 != "10" || *r.Operations[0].Current.P95 != "20" || len(r.Candidates) != 1 {
		t.Fatalf("half-open windows=%+v", r)
	}
}
