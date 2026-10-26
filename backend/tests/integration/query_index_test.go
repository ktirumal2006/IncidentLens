package integration

import (
	"context"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// This test uses a private ReplacingMergeTree table with the production sorting
// and partition keys. It exercises the candidate index and FINAL without
// modifying the product spans table or the SELECT-only user's grants.
func queryIndexDatabase(t *testing.T) (driver.Conn, string) {
	t.Helper()
	if os.Getenv("INCIDENTLENS_QUERY_INDEX_INTEGRATION") != "1" {
		t.Skip("set INCIDENTLENS_QUERY_INDEX_INTEGRATION=1 on the local ClickHouse test stack")
	}
	if os.Getenv("INCIDENTLENS_INTEGRATION") != "1" {
		t.Fatal("index test requires the real ClickHouse integration gate")
	}
	_, admin := database(t)
	name := "query_index_" + queryID(t, 5)
	ddl := `CREATE TABLE incidentlens.` + name + ` (
		trace_id FixedString(32), span_id FixedString(16), parent_span_id String,
		service_namespace LowCardinality(String), service_name LowCardinality(String),
		span_name String, span_kind UInt8, status_code UInt8,
		start_time DateTime64(9,'UTC'), duration_ns UInt64,
		ingested_at DateTime64(9,'UTC'), events String, links String
	) ENGINE=ReplacingMergeTree PARTITION BY toDate(start_time)
	ORDER BY (service_namespace,service_name,start_time,trace_id,span_id)`
	if err := admin.Exec(context.Background(), ddl); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := admin.Exec(ctx, `DROP TABLE IF EXISTS incidentlens.`+name+` SYNC`); err != nil {
			t.Errorf("drop isolated index table: %v", err)
		}
	})
	return admin, name
}

const queryIndexSettings = `use_skip_indexes_if_final=1,use_skip_indexes_if_final_exact_mode=1,use_query_condition_cache=0,max_memory_usage=268435456,max_rows_to_read=2000000,max_bytes_to_read=268435456,max_threads=2,max_execution_time=10`

type indexSpan struct {
	TraceID, SpanID, ParentID, Namespace, Service, Operation string
	Kind, Status                                             uint8
	Start                                                    time.Time
	Duration                                                 uint64
	Events, Links                                            string
}

func queryIndexRows(t *testing.T, admin driver.Conn, table, traceID string, from, to, observed time.Time, useIndex bool, contextShape bool) []indexSpan {
	t.Helper()
	selectSQL := `SELECT trace_id,span_id,parent_span_id,service_namespace,service_name,span_name,span_kind,status_code,start_time,duration_ns,events,links FROM incidentlens.` + table + ` FINAL PREWHERE ingested_at<=fromUnixTimestamp64Nano(?) AND start_time>=fromUnixTimestamp64Nano(?) AND start_time<fromUnixTimestamp64Nano(?) AND `
	if contextShape {
		selectSQL += `trace_id IN (?)`
	} else {
		selectSQL += `trace_id=?`
	}
	selectSQL += ` ORDER BY trace_id,start_time,span_id SETTINGS ` + queryIndexSettings + `,use_skip_indexes=?`
	flag := 0
	if useIndex {
		flag = 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rows, err := admin.Query(ctx, selectSQL, observed.UnixNano(), from.UnixNano(), to.UnixNano(), traceID, flag)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := []indexSpan{}
	for rows.Next() {
		var s indexSpan
		if err := rows.Scan(&s.TraceID, &s.SpanID, &s.ParentID, &s.Namespace, &s.Service, &s.Operation, &s.Kind, &s.Status, &s.Start, &s.Duration, &s.Events, &s.Links); err != nil {
			t.Fatal(err)
		}
		s.Start = s.Start.UTC()
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func queryIndexInsertTarget(t *testing.T, admin driver.Conn, table, traceID, spanID, parentID, service string, start, receipt time.Time) {
	t.Helper()
	sql := `INSERT INTO incidentlens.` + table + ` (trace_id,span_id,parent_span_id,service_namespace,service_name,span_name,span_kind,status_code,start_time,duration_ns,ingested_at,events,links) VALUES (?,?,?,?,?,?,?,?,fromUnixTimestamp64Nano(?),?,fromUnixTimestamp64Nano(?),?,?)`
	if err := admin.Exec(context.Background(), sql, traceID, spanID, parentID, "index-test", service, "Root", 2, 2, start.UnixNano(), 100_000_000, receipt.UnixNano(), `{"events":[]}`, `{"links":[]}`); err != nil {
		t.Fatal(err)
	}
}

func TestQueryTraceIndexFinalReplayAndCutoff(t *testing.T) {
	admin, table := queryIndexDatabase(t)
	end := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Second)
	from, start, observed := end.Add(-5*time.Minute), end.Add(-2*time.Minute), time.Now().UTC()
	traceID := queryID(t, 16)
	// A large unrelated part makes the skip-index decision observable while
	// retaining the exact same physical sort/partition layout as spans.
	seed := `INSERT INTO incidentlens.` + table + ` (trace_id,span_id,service_namespace,service_name,span_name,span_kind,status_code,start_time,duration_ns,ingested_at,events,links)
		SELECT concat(lower(leftPad(hex(cityHash64(?)),16,'0')),lower(leftPad(hex(number+1),16,'0'))),
		'0000000000000001','index-test','frontend','Root',2,1,
		fromUnixTimestamp64Nano(toInt64(?)+toInt64(number)*1000),100000000,fromUnixTimestamp64Nano(?),?,?
		FROM numbers(120000)`
	if err := admin.Exec(context.Background(), seed, table, start.Add(-time.Second).UnixNano(), observed.Add(-time.Second).UnixNano(), `{"events":[]}`, `{"links":[]}`); err != nil {
		t.Fatal(err)
	}
	// Exercise the same ADD/MATERIALIZE path needed for a retained volume.
	if err := admin.Exec(context.Background(), `ALTER TABLE incidentlens.`+table+` ADD INDEX idx_trace_id trace_id TYPE bloom_filter(0.001) GRANULARITY 1`); err != nil {
		t.Fatal(err)
	}
	if err := admin.Exec(context.Background(), `ALTER TABLE incidentlens.`+table+` MATERIALIZE INDEX idx_trace_id SETTINGS mutations_sync=1`); err != nil {
		t.Fatal(err)
	}
	if err := admin.Exec(context.Background(), `SYSTEM STOP MERGES incidentlens.`+table); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := admin.Exec(context.Background(), `SYSTEM START MERGES incidentlens.`+table); err != nil {
			t.Logf("start merges after isolated table cleanup: %v", err)
		}
	})
	queryIndexInsertTarget(t, admin, table, traceID, "0000000000000001", "", "frontend", start, observed.Add(-time.Second))
	queryIndexInsertTarget(t, admin, table, traceID, "0000000000000002", "0000000000000001", "backend", start.Add(time.Millisecond), observed.Add(-time.Second))
	queryIndexInsertTarget(t, admin, table, traceID, "0000000000000001", "", "frontend", start, observed.Add(time.Second))
	queryIndexInsertTarget(t, admin, table, traceID, "0000000000000003", "0000000000000002", "backend", start.Add(2*time.Millisecond), observed.Add(2*time.Second))
	var raw uint64
	if err := admin.QueryRow(context.Background(), `SELECT count() FROM incidentlens.`+table+` WHERE trace_id=?`, traceID).Scan(&raw); err != nil || raw != 4 {
		t.Fatalf("unmerged replay rows=%d err=%v want4", raw, err)
	}
	for _, contextShape := range []bool{false, true} {
		without := queryIndexRows(t, admin, table, traceID, from, end, observed, false, contextShape)
		with := queryIndexRows(t, admin, table, traceID, from, end, observed, true, contextShape)
		if !reflect.DeepEqual(without, with) || len(with) != 2 || with[0].SpanID != "0000000000000001" || with[1].SpanID != "0000000000000002" {
			t.Fatalf("index parity context=%v before cutoff: off=%+v on=%+v", contextShape, without, with)
		}
		fresh := observed.Add(3 * time.Second)
		without = queryIndexRows(t, admin, table, traceID, from, end, fresh, false, contextShape)
		with = queryIndexRows(t, admin, table, traceID, from, end, fresh, true, contextShape)
		if !reflect.DeepEqual(without, with) || len(with) != 3 || with[2].SpanID != "0000000000000003" {
			t.Fatalf("index parity context=%v after cutoff: off=%+v on=%+v", contextShape, without, with)
		}
	}
	// This is an index-use assertion, not a latency threshold. Exact FINAL mode
	// may re-add neighboring ranges needed to deduplicate correctly.
	explain := `EXPLAIN indexes=1 SELECT trace_id,span_id FROM incidentlens.` + table + ` FINAL PREWHERE trace_id=? AND start_time>=fromUnixTimestamp64Nano(?) AND start_time<fromUnixTimestamp64Nano(?) SETTINGS ` + queryIndexSettings + `,use_skip_indexes=1`
	planRows, err := admin.Query(context.Background(), explain, traceID, from.UnixNano(), end.UnixNano())
	if err != nil {
		t.Fatal(err)
	}
	defer planRows.Close()
	var planBuilder strings.Builder
	for planRows.Next() {
		var line string
		if err := planRows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		planBuilder.WriteString(line)
		planBuilder.WriteByte('\n')
	}
	if err := planRows.Err(); err != nil {
		t.Fatal(err)
	}
	plan := planBuilder.String()
	granules := regexp.MustCompile(`(?s)Name:\s*idx_trace_id\b.*?Granules:\s*([0-9]+)/([0-9]+)`).FindStringSubmatch(plan)
	if len(granules) != 3 {
		t.Fatalf("index granule counts were not visible in FINAL plan: %s", plan)
	}
	selected, selectedErr := strconv.Atoi(granules[1])
	total, totalErr := strconv.Atoi(granules[2])
	if selectedErr != nil || totalErr != nil || total <= 0 || selected >= total {
		t.Fatalf("index did not prune granules in FINAL plan: %s", plan)
	}
	t.Logf("FINAL indexed plan: %s", strings.ReplaceAll(plan, "\n", " "))
	// The isolated admin connection applies exactly the existing query budgets;
	// the production query user itself remains untouched by this test.
	var memory, rows, bytes, threads uint64
	profileSQL := `SELECT toUInt64(getSetting('max_memory_usage')),toUInt64(getSetting('max_rows_to_read')),toUInt64(getSetting('max_bytes_to_read')),toUInt64(getSetting('max_threads')) SETTINGS ` + queryIndexSettings
	if err := admin.QueryRow(context.Background(), profileSQL).Scan(&memory, &rows, &bytes, &threads); err != nil {
		t.Fatal(err)
	}
	if memory != 268435456 || rows != 2000000 || bytes != 268435456 || threads != 2 {
		t.Fatalf("test profile changed: %d/%d/%d/%d", memory, rows, bytes, threads)
	}
}
