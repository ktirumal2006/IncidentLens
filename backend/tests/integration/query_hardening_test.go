package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"incidentlens/backend/internal/detector"
	"incidentlens/backend/internal/query"
	storage "incidentlens/backend/internal/storage/clickhouse"
)

// This opt-in fixture deliberately exceeds the old detector input ceilings.
// It is kept out of the ordinary integration suite because it inserts 133,002
// historical rows and measures real ClickHouse behavior under the query profile.
func queryHardeningDatabase(t *testing.T) (driver.Conn, *queryHardeningReader) {
	t.Helper()
	if os.Getenv("INCIDENTLENS_QUERY_HARDENING_INTEGRATION") != "1" {
		t.Skip("set INCIDENTLENS_QUERY_HARDENING_INTEGRATION=1 on an isolated local ClickHouse stack")
	}
	if os.Getenv("INCIDENTLENS_INTEGRATION") != "1" || os.Getenv("INCIDENTLENS_DETECTOR_INTEGRATION") != "1" {
		t.Fatal("query hardening tests require both real ClickHouse integration gates")
	}
	_, admin := database(t)
	return admin, &queryHardeningReader{detectorDatabase(t)}
}

type queryHardeningReader struct{ store detector.Store }

// insertQueryHardeningCorpus uses a single server-side insert rather than
// hundreds of tiny client writes. Every row has an immutable unique trace key,
// a valid event time within the seven-day TTL, and an earlier receipt cutoff.
func insertQueryHardeningCorpus(t *testing.T, admin driver.Conn, namespace string, end, observed time.Time, baselineRows, currentRows, paddingBytes int) {
	t.Helper()
	baseStart := end.Add(-34 * time.Minute).UnixNano()
	currentStart := end.Add(-4 * time.Minute).UnixNano()
	events := `{"events":[]}`
	if paddingBytes > 0 {
		events = fmt.Sprintf(`{"events":[],"padding":"%s"}`, makePadding(paddingBytes))
	}
	const insert = `INSERT INTO spans
		(trace_id,span_id,service_namespace,service_name,span_name,span_kind,status_code,start_time,end_time,duration_ns,ingested_at,events,links,resource_attributes,span_attributes,scope_attributes)
		SELECT concat(lower(leftPad(hex(cityHash64(?)),16,'0')),lower(leftPad(hex(number+1),16,'0'))), '0000000000000001', ?, 'hardening-service', 'Root', 2,
		if(number < ?, 1, 2),
		fromUnixTimestamp64Nano(if(number < ?, toInt64(?) + toInt64(number), toInt64(?) + toInt64(number - ?))),
		fromUnixTimestamp64Nano(if(number < ?, toInt64(?) + toInt64(number) + 100000000, toInt64(?) + toInt64(number - ?) + 400000000)),
		if(number < ?, 100000000, 400000000), fromUnixTimestamp64Nano(?), ?, ?, ?, ?, ?
		FROM numbers(?)`
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := admin.Exec(ctx, insert, namespace, namespace, baselineRows, baselineRows, baseStart, currentStart, baselineRows,
		baselineRows, baseStart, currentStart, baselineRows, baselineRows, observed.Add(-time.Second).UnixNano(), events, `{"links":[]}`, `{}`, `{}`, `{}`,
		baselineRows+currentRows); err != nil {
		t.Fatalf("seed %d rows: %v", baselineRows+currentRows, err)
	}
	var got uint64
	if err := admin.QueryRow(ctx, `SELECT count() FROM spans FINAL WHERE service_namespace=? AND start_time>=fromUnixTimestamp64Nano(?) AND start_time<fromUnixTimestamp64Nano(?)`, namespace, end.Add(-35*time.Minute).UnixNano(), end.UnixNano()).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != uint64(baselineRows+currentRows) {
		t.Fatalf("seeded FINAL rows=%d want=%d", got, baselineRows+currentRows)
	}
}

func makePadding(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = 'x'
	}
	return string(b)
}

// The old in-process evaluator is retained as an oracle for its resource
// ceiling. The synthetic spans match the seeded corpus's accounted field sizes;
// IDs are unique and equal-length but use a different prefix. This establishes
// which fixed ceiling the old path hits.
func assertOldDetectorCeiling(t *testing.T, namespace string, end, observed time.Time, baselineRows, currentRows, paddingBytes int) {
	t.Helper()
	events := `{"events":[]}`
	if paddingBytes > 0 {
		events = fmt.Sprintf(`{"events":[],"padding":"%s"}`, makePadding(paddingBytes))
	}
	spans := make([]detector.Span, 0, baselineRows+currentRows)
	accounted := 0
	for i := 0; i < baselineRows+currentRows; i++ {
		start, duration, status := end.Add(-34*time.Minute).Add(time.Duration(i)*time.Nanosecond), uint64(100000000), uint8(1)
		if i >= baselineRows {
			start, duration, status = end.Add(-4*time.Minute).Add(time.Duration(i-baselineRows)*time.Nanosecond), 400000000, 2
		}
		span := detector.Span{TraceID: fmt.Sprintf("%032x", i+1), SpanID: "0000000000000001", ServiceNamespace: namespace,
			ServiceName: "hardening-service", Operation: "Root", Kind: 2, Status: status, Start: start, Duration: duration,
			Events: json.RawMessage(events), Links: json.RawMessage(`{"links":[]}`)}
		accounted += detector.Account(span)
		spans = append(spans, span)
	}
	if baselineRows+currentRows > detector.MaxSpans {
		if accounted >= detector.MaxInputBytes {
			t.Fatalf("row-ceiling fixture also crosses byte ceiling: %d", accounted)
		}
	} else if accounted <= detector.MaxInputBytes {
		t.Fatalf("byte-ceiling fixture accounts only %d bytes", accounted)
	}
	cfg := detector.DefaultConfig()
	cfg.MinSamples = 2
	_, err := detector.EvaluateContext(context.Background(), spans, end, observed, cfg, detector.Filter{Namespace: &namespace})
	if !errors.Is(err, detector.ErrLimit) {
		t.Fatalf("old raw evaluator on %d spans/%d accounted bytes: got %v, want ErrLimit", len(spans), accounted, err)
	}
	t.Logf("old raw evaluator rejects rows=%d accounted_bytes=%d row_limit=%d byte_limit=%d", len(spans), accounted, detector.MaxSpans, detector.MaxInputBytes)
}

func assertQueryHardeningStats(t *testing.T, reader *queryHardeningReader, namespace string, end, observed time.Time, baselineRows, currentRows int) {
	t.Helper()
	cfg := detector.DefaultConfig()
	cfg.MinSamples = 2
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := reader.store.Incidents(ctx, end, observed, cfg, detector.Filter{Namespace: &namespace})
	if err != nil {
		t.Fatalf("aggregate incidents for %d rows: %v", baselineRows+currentRows, err)
	}
	if len(result.Operations) != 1 || len(result.Candidates) != 1 {
		t.Fatalf("operations=%d candidates=%d want 1 each", len(result.Operations), len(result.Candidates))
	}
	op := result.Operations[0]
	if op.Baseline.Count != uint64(baselineRows) || op.Current.Count != uint64(currentRows) ||
		op.Baseline.ErrorCount != 0 || op.Current.ErrorCount != uint64(currentRows) ||
		op.Baseline.P95 == nil || *op.Baseline.P95 != "100000000" ||
		op.Current.P95 == nil || *op.Current.P95 != "400000000" {
		t.Fatalf("unexpected exact stats: %+v", op)
	}
	t.Logf("exact FINAL aggregate rows=%d baseline=%d current=%d evidence=%d", baselineRows+currentRows, op.Baseline.Count, op.Current.Count, len(op.Evidence))
}

func TestQueryHardeningDetectorRowCeilingAndDetail(t *testing.T) {
	admin, reader := queryHardeningDatabase(t)
	end := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	observed := time.Now().UTC()
	namespace := "query-hardening-rows-" + queryID(t, 5)
	const baselineRows, currentRows = 100000, 2
	insertQueryHardeningCorpus(t, admin, namespace, end, observed, baselineRows, currentRows, 0)
	if baselineRows+currentRows <= detector.MaxSpans {
		t.Fatal("fixture does not cross the old row ceiling")
	}
	assertOldDetectorCeiling(t, namespace, end, observed, baselineRows, currentRows, 0)
	assertQueryHardeningStats(t, reader, namespace, end, observed, baselineRows, currentRows)
	canceled, cancelCanceled := context.WithCancel(context.Background())
	cancelCanceled()
	if _, err := reader.store.Incidents(canceled, end, observed, detector.DefaultConfig(), detector.Filter{Namespace: &namespace}); err == nil {
		t.Fatal("cancelled incident query succeeded")
	}

	// The same trace must be visible with and without an explicit PREWHERE. The
	// query log captures diagnostic read cost; optimizer behavior can make the
	// two equal, so this test does not assert a fabricated speedup.
	var traceID string
	if err := admin.QueryRow(context.Background(), "SELECT trace_id FROM spans FINAL WHERE service_namespace=? AND status_code=2 ORDER BY trace_id LIMIT 1", namespace).Scan(&traceID); err != nil {
		t.Fatal(err)
	}
	window := query.Window{From: end.Add(-5 * time.Minute), To: end}
	detail, err := reader.store.(*storage.QueryStore).Detail(context.Background(), window, traceID, observed)
	if err != nil || len(detail.Spans) != 1 || detail.Spans[0].SpanID != "0000000000000001" {
		t.Fatalf("product detail lookup after large insert: spans=%d err=%v", len(detail.Spans), err)
	}
	address := os.Getenv("CLICKHOUSE_TEST_ADDRESS")
	if address == "" {
		address = "127.0.0.1:19000"
	}
	conn, err := ch.Open(&ch.Options{Addr: []string{address}, Auth: ch.Auth{Database: "incidentlens", Username: "query", Password: "local-query"}})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var maxMemory, maxRows, maxBytes, maxThreads uint64
	if err := conn.QueryRow(context.Background(), `SELECT toUInt64(getSetting('max_memory_usage')),toUInt64(getSetting('max_rows_to_read')),toUInt64(getSetting('max_bytes_to_read')),toUInt64(getSetting('max_threads'))`).Scan(&maxMemory, &maxRows, &maxBytes, &maxThreads); err != nil {
		t.Fatal(err)
	}
	if maxMemory != 268435456 || maxRows != 2000000 || maxBytes != 268435456 || maxThreads != 2 {
		t.Fatalf("query profile limits changed: memory=%d rows=%d bytes=%d threads=%d", maxMemory, maxRows, maxBytes, maxThreads)
	}
	from, to := end.Add(-5*time.Minute), end
	ids := make([]string, 2)
	for i, prewhere := range []bool{false, true} {
		predicate := "WHERE"
		if prewhere {
			predicate = "PREWHERE"
		}
		sql := `SELECT trace_id,span_id,events,links FROM spans FINAL ` + predicate +
			` trace_id=? AND start_time>=fromUnixTimestamp64Nano(?) AND start_time<fromUnixTimestamp64Nano(?) ORDER BY start_time,span_id LIMIT ?`
		queryIDValue := "query-hardening-" + queryID(t, 8)
		ctx, cancel := context.WithTimeout(ch.Context(context.Background(), ch.WithQueryID(queryIDValue)), 5*time.Second)
		rows, err := conn.Query(ctx, sql, traceID, from.UnixNano(), to.UnixNano(), query.MaxDetail+1)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		count := 0
		for rows.Next() {
			var tid, sid, events, links string
			if err := rows.Scan(&tid, &sid, &events, &links); err != nil {
				t.Fatal(err)
			}
			if tid != traceID || sid != "0000000000000001" {
				t.Fatal("unexpected detail identity")
			}
			count++
		}
		err = rows.Err()
		rows.Close()
		cancel()
		if err != nil || count != 1 {
			t.Fatalf("prewhere=%v count=%d err=%v", prewhere, count, err)
		}
		ids[i] = queryIDValue
	}
	if err := admin.Exec(context.Background(), "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatal(err)
	}
	for i, id := range ids {
		var readRows, readBytes, duration uint64
		if err := admin.QueryRow(context.Background(), "SELECT read_rows,read_bytes,query_duration_ms FROM system.query_log WHERE query_id=? AND type='QueryFinish' ORDER BY event_time_microseconds DESC LIMIT 1", id).Scan(&readRows, &readBytes, &duration); err != nil {
			t.Fatal(err)
		}
		t.Logf("detail prewhere=%v read_rows=%d read_bytes=%d duration_ms=%d", i == 1, readRows, readBytes, duration)
	}
}

func TestQueryHardeningDetectorByteCeiling(t *testing.T) {
	admin, reader := queryHardeningDatabase(t)
	end := time.Now().UTC().Add(-3 * time.Hour).Truncate(time.Second)
	observed := time.Now().UTC()
	namespace := "query-hardening-bytes-" + queryID(t, 5)
	const baselineRows, currentRows, payloadBytes = 33000, 2, 1024
	accounted := (baselineRows + currentRows) * (128 + 32 + 16 + len(namespace) + len("hardening-service") + len("Root") + len(`{"events":[],"padding":""}`) + payloadBytes + len(`{"links":[]}`))
	if accounted <= detector.MaxInputBytes || baselineRows+currentRows >= detector.MaxSpans {
		t.Fatalf("fixture bounds: accounted=%d rows=%d", accounted, baselineRows+currentRows)
	}
	insertQueryHardeningCorpus(t, admin, namespace, end, observed, baselineRows, currentRows, payloadBytes)
	assertOldDetectorCeiling(t, namespace, end, observed, baselineRows, currentRows, payloadBytes)
	assertQueryHardeningStats(t, reader, namespace, end, observed, baselineRows, currentRows)
	t.Logf("old detector input accounting=%d bytes exceeds %d; exact aggregate succeeded", accounted, detector.MaxInputBytes)
}
