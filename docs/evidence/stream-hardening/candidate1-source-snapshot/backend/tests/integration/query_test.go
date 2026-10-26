package integration

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"incidentlens/backend/internal/trace"
)

// These are black-box acceptance checks for the deployed query API and real
// ClickHouse. They deliberately do not use the API's SQL implementation.
func queryEnabled(t *testing.T) string {
	t.Helper()
	if os.Getenv("INCIDENTLENS_QUERY_INTEGRATION") != "1" {
		t.Skip("set INCIDENTLENS_QUERY_INTEGRATION=1 with the local query API running")
	}
	if os.Getenv("INCIDENTLENS_INTEGRATION") != "1" {
		t.Fatal("INCIDENTLENS_QUERY_INTEGRATION requires INCIDENTLENS_INTEGRATION=1; query tests must use real ClickHouse")
	}
	base := os.Getenv("QUERY_TEST_URL")
	if base == "" {
		base = "http://127.0.0.1:18081"
	}
	return strings.TrimRight(base, "/")
}

func queryID(t *testing.T, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	if b[0] == 0 {
		b[0] = 1
	}
	return hex.EncodeToString(b)
}

func querySpan(traceID, spanID, parentID, service, operation string, start time.Time, duration uint64, kind, status uint8) trace.Row {
	return trace.Row{
		TraceID: traceID, SpanID: spanID, ParentSpanID: parentID,
		ServiceName: service, ServiceNamespace: "query-tests", SpanName: operation,
		SpanKind: kind, StatusCode: status, StartTime: start.UTC(),
		EndTime: start.Add(time.Duration(duration)).UTC(), DurationNS: duration,
		IngestedAt:         time.Now().UTC(),
		ResourceAttributes: `{"attributes":[{"key":"service.name","value":{"stringValue":"` + service + `"}}]}`,
		SpanAttributes:     `{"attributes":[{"key":"test.answer","value":{"intValue":"9007199254740993"}}]}`,
		ScopeAttributes:    `{"attributes":[]}`, ScopeName: "query.acceptance", ScopeVersion: "1",
		Events: `{"events":[]}`, Links: `{"links":[]}`,
	}
}

func queryWindow(base time.Time) url.Values {
	v := url.Values{}
	v.Set("from", base.Add(-time.Second).Format(time.RFC3339Nano))
	v.Set("to", base.Add(time.Minute).Format(time.RFC3339Nano))
	return v
}

func queryGET(t *testing.T, base, path string, v url.Values, want int, result any) {
	t.Helper()
	u := base + path
	if v != nil {
		u += "?" + v.Encode()
	}
	client := &http.Client{Timeout: 12 * time.Second}
	res, err := client.Get(u)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != want {
		var body any
		_ = json.NewDecoder(res.Body).Decode(&body)
		t.Fatalf("GET %s: status=%d want=%d body=%v", u, res.StatusCode, want, body)
	}
	if result != nil {
		if err := json.NewDecoder(res.Body).Decode(result); err != nil {
			t.Fatalf("GET %s decode: %v", u, err)
		}
	}
}

type querySummary struct {
	ServiceName string  `json:"service_name"`
	Operation   string  `json:"operation"`
	SpanCount   int     `json:"span_count"`
	ErrorCount  int     `json:"error_count"`
	UnsetCount  int     `json:"unset_count"`
	ErrorRate   float64 `json:"error_rate"`
	UnsetRate   float64 `json:"unset_rate"`
	P50         string  `json:"p50_duration_ns"`
	P95         string  `json:"p95_duration_ns"`
}
type querySearchItem struct {
	TraceID       string `json:"trace_id"`
	MatchingStart string `json:"matching_start_time"`
	MatchingCount int    `json:"matching_span_count"`
	MinDuration   string `json:"matching_min_duration_ns"`
	MaxDuration   string `json:"matching_max_duration_ns"`
	ErrorCount    int    `json:"matching_error_count"`
}
type querySearch struct {
	IngestionCutoff string            `json:"ingestion_cutoff"`
	Traces          []querySearchItem `json:"traces"`
	NextCursor      string            `json:"next_cursor"`
}

func TestQuerySummaryFiltersReplayAndPercentiles(t *testing.T) {
	baseURL := queryEnabled(t)
	store, conn := database(t)
	service := "query-" + queryID(t, 5)
	base := time.Now().UTC().Add(-4 * time.Minute).Truncate(time.Second)
	rows := make([]trace.Row, 0, 22)
	for i := 0; i < 20; i++ {
		d := uint64((i + 1) * 100)
		status := uint8(1)
		if i == 18 {
			status = 2
		}
		if i == 19 {
			status = 0
		}
		rows = append(rows, querySpan(queryID(t, 16), fmt.Sprintf("%016x", i+1), "", service, "Summary", base.Add(time.Duration(i)*time.Second), d, 2, status))
	}
	// CLIENT spans must not affect the SERVER denominator or percentiles.
	rows = append(rows, querySpan(queryID(t, 16), queryID(t, 8), "", service, "Summary", base, 999999, 3, 2))
	rows = append(rows, querySpan(queryID(t, 16), queryID(t, 8), "", service, "Other", base, 999999, 2, 2))
	if err := store.Write(context.Background(), rows); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(context.Background(), "SYSTEM STOP MERGES incidentlens.spans"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := conn.Exec(context.Background(), "SYSTEM START MERGES incidentlens.spans"); err != nil {
			t.Errorf("restore merges: %v", err)
		}
	}()
	// Skew replay toward the shortest samples: raw quantiles would change.
	if err := store.Write(context.Background(), rows[:2]); err != nil {
		t.Fatal(err)
	}
	var raw uint64
	if err := conn.QueryRow(context.Background(), "SELECT count() FROM spans WHERE service_name = ? AND span_name = 'Summary' AND span_kind = 2", service).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if raw != 22 {
		t.Fatalf("raw replay rows=%d want=22", raw)
	}
	v := queryWindow(base)
	v.Set("service", service)
	v.Set("operation", "Summary")
	var got struct {
		Services  []querySummary `json:"services"`
		Truncated bool           `json:"truncated"`
	}
	queryGET(t, baseURL, "/api/v1/services", v, 200, &got)
	if len(got.Services) != 1 {
		t.Fatalf("services=%+v", got.Services)
	}
	s := got.Services[0]
	if s.SpanCount != 20 || s.ErrorCount != 1 || s.UnsetCount != 1 || math.Abs(s.ErrorRate-.05) > 1e-9 || math.Abs(s.UnsetRate-.05) > 1e-9 || s.P50 != "1100" || s.P95 != "2000" {
		t.Fatalf("replay-safe exact summary=%+v", s)
	}
	v.Set("operation", "Absent")
	queryGET(t, baseURL, "/api/v1/services", v, 200, &got)
	if len(got.Services) != 0 {
		t.Fatalf("absent operation returned %+v", got.Services)
	}
	v.Del("operation")
	v.Set("limit", "1")
	queryGET(t, baseURL, "/api/v1/services", v, 200, &got)
	if len(got.Services) != 1 || !got.Truncated {
		t.Fatalf("summary limit did not disclose truncation: %+v", got)
	}
	v.Set("limit", "2")
	queryGET(t, baseURL, "/api/v1/services", v, 200, &got)
	if len(got.Services) != 2 || got.Truncated {
		t.Fatalf("summary limit 2: %+v", got)
	}
}

func TestQuerySearchSameSpanFiltersAndFrozenPagination(t *testing.T) {
	baseURL := queryEnabled(t)
	store, _ := database(t)
	service := "query-" + queryID(t, 5)
	base := time.Now().UTC().Add(-4 * time.Minute).Truncate(time.Second)
	traceA, traceB, traceC := queryID(t, 16), queryID(t, 16), queryID(t, 16)
	traceSplit := queryID(t, 16)
	ids := []string{traceA, traceB, traceC}
	// All three traces tie on earliest matching start; A also has two matches.
	rows := []trace.Row{}
	for _, id := range ids {
		rows = append(rows, querySpan(id, queryID(t, 8), "", service, "Match", base, 200, 2, 2))
	}
	rows = append(rows,
		querySpan(traceA, queryID(t, 8), "", service, "Match", base.Add(time.Second), 300, 2, 2),
		querySpan(traceB, queryID(t, 8), "", service, "Match", base.Add(2*time.Second), 50, 2, 1),
		querySpan(traceC, queryID(t, 8), "", service, "Other", base.Add(3*time.Second), 999, 2, 2),
		querySpan(traceSplit, queryID(t, 8), "", service, "Match", base.Add(4*time.Second), 100, 2, 2),
		querySpan(traceSplit, queryID(t, 8), "", service, "Match", base.Add(5*time.Second), 500, 2, 1),
	)
	if err := store.Write(context.Background(), rows); err != nil {
		t.Fatal(err)
	}
	v := queryWindow(base)
	v.Set("service", service)
	v.Set("operation", "Match")
	v.Set("status", "ERROR")
	v.Set("min_duration_ns", "200")
	v.Set("limit", "1")
	seen := map[string]querySearchItem{}
	last := ""
	var cursor string
	for page := 0; page < 3; page++ {
		if cursor != "" {
			v.Set("cursor", cursor)
		}
		var out querySearch
		queryGET(t, baseURL, "/api/v1/traces", v, 200, &out)
		if len(out.Traces) != 1 {
			t.Fatalf("page %d: %+v", page, out)
		}
		item := out.Traces[0]
		if last != "" && item.TraceID <= last {
			t.Fatalf("tie order: %s after %s", item.TraceID, last)
		}
		if _, exists := seen[item.TraceID]; exists {
			t.Fatalf("duplicate trace across pages: %s", item.TraceID)
		}
		seen[item.TraceID] = item
		last = item.TraceID
		cursor = out.NextCursor
		if page < 2 && cursor == "" {
			t.Fatal("cursor ended early")
		}
		if page == 2 && cursor != "" {
			t.Fatal("last page retained a next cursor")
		}
	}
	if len(seen) != 3 || seen[traceA].MatchingCount != 2 || seen[traceA].MaxDuration != "300" || seen[traceB].MatchingCount != 1 || seen[traceB].MinDuration != "200" {
		t.Fatalf("matching facts=%+v", seen)
	}
	// Filters must agree on one span, not be satisfied by different spans of a trace.
	v.Del("cursor")
	v.Set("min_duration_ns", "400")
	var none querySearch
	queryGET(t, baseURL, "/api/v1/traces", v, 200, &none)
	if len(none.Traces) != 0 {
		t.Fatalf("cross-span filter matched: %+v", none.Traces)
	}
	// A later write belongs only to a fresh search. Include a tied earlier start.
	v.Set("min_duration_ns", "200")
	var first querySearch
	queryGET(t, baseURL, "/api/v1/traces", v, 200, &first)
	if first.NextCursor == "" {
		t.Fatal("missing pagination cursor")
	}
	lateID := queryID(t, 16)
	if err := store.Write(context.Background(), []trace.Row{querySpan(lateID, queryID(t, 8), "", service, "Match", base, 200, 2, 2)}); err != nil {
		t.Fatal(err)
	}
	v.Set("cursor", first.NextCursor)
	oldSeen := map[string]bool{first.Traces[0].TraceID: true}
	for i := 0; i < 4; i++ {
		var out querySearch
		queryGET(t, baseURL, "/api/v1/traces", v, 200, &out)
		for _, x := range out.Traces {
			oldSeen[x.TraceID] = true
		}
		if out.NextCursor == "" {
			break
		}
		v.Set("cursor", out.NextCursor)
	}
	if oldSeen[lateID] {
		t.Fatal("late arrival leaked into frozen pagination")
	}
	v.Del("cursor")
	v.Set("limit", "10")
	var fresh querySearch
	queryGET(t, baseURL, "/api/v1/traces", v, 200, &fresh)
	found := false
	for _, x := range fresh.Traces {
		if x.TraceID == lateID {
			found = true
		}
	}
	if !found {
		t.Fatal("fresh search did not reveal late arrival")
	}
	// Cursor must bind filters and limits.
	v.Set("limit", "1")
	v.Set("cursor", first.NextCursor)
	v.Set("operation", "Other")
	queryGET(t, baseURL, "/api/v1/traces", v, 400, nil)
}

func TestQueryCursorKeepsEligibleSpanAfterLaterReplay(t *testing.T) {
	baseURL := queryEnabled(t)
	store, conn := database(t)
	if err := conn.Exec(context.Background(), "SYSTEM STOP MERGES incidentlens.spans"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := conn.Exec(context.Background(), "SYSTEM START MERGES incidentlens.spans"); err != nil {
			t.Errorf("restore merges: %v", err)
		}
	}()
	service := "query-" + queryID(t, 5)
	base := time.Now().UTC().Add(-4 * time.Minute).Truncate(time.Second)
	firstID, secondID := queryID(t, 16), queryID(t, 16)
	first := querySpan(firstID, queryID(t, 8), "", service, "Replay", base, 100, 2, 0)
	second := querySpan(secondID, queryID(t, 8), "", service, "Replay", base.Add(time.Second), 200, 2, 2)
	if err := store.Write(context.Background(), []trace.Row{first, second}); err != nil {
		t.Fatal(err)
	}
	v := queryWindow(base)
	v.Set("service", service)
	v.Set("operation", "Replay")
	v.Set("limit", "1")
	var firstPage querySearch
	queryGET(t, baseURL, "/api/v1/traces", v, 200, &firstPage)
	if len(firstPage.Traces) != 1 || firstPage.Traces[0].TraceID != firstID || firstPage.NextCursor == "" {
		t.Fatalf("first page=%+v", firstPage)
	}
	cutoff, err := time.Parse(time.RFC3339Nano, firstPage.IngestionCutoff)
	if err != nil {
		t.Fatal(err)
	}
	second.IngestedAt = cutoff.Add(time.Millisecond)
	if err := store.Write(context.Background(), []trace.Row{second}); err != nil {
		t.Fatal(err)
	}
	var raw uint64
	if err := conn.QueryRow(context.Background(), "SELECT count() FROM spans WHERE trace_id = ?", secondID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if raw != 2 {
		t.Fatalf("raw replay rows=%d want 2", raw)
	}
	v.Set("cursor", firstPage.NextCursor)
	var secondPage querySearch
	queryGET(t, baseURL, "/api/v1/traces", v, 200, &secondPage)
	if len(secondPage.Traces) != 1 || secondPage.Traces[0].TraceID != secondID || secondPage.Traces[0].MatchingCount != 1 {
		t.Fatalf("later receipt hid eligible pre-cutoff span: %+v", secondPage)
	}
}

func TestQueryDetailRelationshipsBoundsAndContext(t *testing.T) {
	baseURL := queryEnabled(t)
	store, _ := database(t)
	service := "query-" + queryID(t, 5)
	base := time.Now().UTC().Add(-4 * time.Minute).Truncate(time.Second)
	id := queryID(t, 16)
	root := queryID(t, 8)
	child := queryID(t, 8)
	missing := queryID(t, 8)
	rows := []trace.Row{
		querySpan(id, child, root, service, "child", base.Add(time.Second), 250, 3, 2),
		querySpan(id, root, "", service, "root", base, 1000, 2, 0),
		querySpan(id, queryID(t, 8), missing, service, "orphan", base.Add(2*time.Second), 100, 2, 1),
	}
	rows[0].StatusMessage = "downstream failed"
	rows[0].DroppedEventsCount = 2
	rows[0].TraceState = "vendor=value"
	rows[0].TraceFlags = 1
	rows[0].ResourceAttributes = `{"attributes":[{"key":"environment","value":{"stringValue":"fixture"}}]}`
	rows[0].ScopeAttributes = `{"attributes":[{"key":"scope.flag","value":{"boolValue":true}}]}`
	rows[0].Events = `{"events":[{"timeUnixNano":"42","name":"checkpoint"}]}`
	rows[0].Links = `{"links":[{"traceId":"AQIDBAUGBwgJCgsMDQ4PEA==","spanId":"AQIDBAUGBwg="}]}`
	rows[0].ResourceSchemaURL = "https://example.test/resource"
	rows[0].ScopeSchemaURL = "https://example.test/scope"
	rows[0].ResourceDroppedAttributesCount = 3
	rows[0].ScopeDroppedAttributesCount = 4
	// The child arrives first. The query must expose the missing parent until a
	// later write makes the earlier-started root visible on refresh.
	if err := store.Write(context.Background(), rows[:1]); err != nil {
		t.Fatal(err)
	}
	v := queryWindow(base)
	var detail struct {
		Spans []struct {
			SpanID         string         `json:"span_id"`
			ParentID       string         `json:"parent_span_id"`
			Service        string         `json:"service_name"`
			Status         uint8          `json:"status_code"`
			Duration       string         `json:"duration_ns"`
			Start          time.Time      `json:"start_time"`
			End            time.Time      `json:"end_time"`
			Attributes     map[string]any `json:"span_attributes"`
			Resources      map[string]any `json:"resource_attributes"`
			Scope          map[string]any `json:"scope_attributes"`
			Events         map[string]any `json:"events"`
			Links          map[string]any `json:"links"`
			ResourceSchema string         `json:"resource_schema_url"`
			ScopeSchema    string         `json:"scope_schema_url"`
			ResourceDrops  uint32         `json:"resource_dropped_attributes_count"`
			ScopeDrops     uint32         `json:"scope_dropped_attributes_count"`
			DroppedEvents  uint32         `json:"dropped_events_count"`
			TraceState     string         `json:"trace_state"`
		} `json:"spans"`
		Missing     []string `json:"missing_parent_ids"`
		Roots       int      `json:"root_count"`
		MissingRoot bool     `json:"has_missing_root"`
		SourceDrops bool     `json:"has_source_drops"`
		Elapsed     string   `json:"observed_elapsed_ns"`
		Truncated   bool     `json:"truncated"`
	}
	queryGET(t, baseURL, "/api/v1/traces/"+id, v, 200, &detail)
	if len(detail.Spans) != 1 || detail.Spans[0].SpanID != child || detail.Roots != 0 || !detail.MissingRoot || len(detail.Missing) != 1 || detail.Missing[0] != root {
		t.Fatalf("child-only observation did not expose missing root: %+v", detail)
	}
	if err := store.Write(context.Background(), rows[1:]); err != nil {
		t.Fatal(err)
	}
	queryGET(t, baseURL, "/api/v1/traces/"+id, v, 200, &detail)
	if len(detail.Spans) != 3 || detail.Spans[0].SpanID != root || detail.Spans[1].SpanID != child || detail.Spans[1].ParentID != root || detail.Spans[1].Status != 2 || detail.Spans[1].Duration != "250" || detail.Spans[1].Service != service || detail.Spans[1].DroppedEvents != 2 || detail.Spans[1].TraceState != "vendor=value" || !detail.SourceDrops || detail.Roots != 1 || detail.Truncated || detail.Elapsed != "2000000100" {
		t.Fatalf("detail=%+v", detail)
	}
	if len(detail.Missing) != 1 || detail.Missing[0] != missing {
		t.Fatalf("missing parents=%v", detail.Missing)
	}
	childSpan := detail.Spans[1]
	if !childSpan.Start.Equal(base.Add(time.Second)) || !childSpan.End.Equal(base.Add(time.Second+250*time.Nanosecond)) || childSpan.ResourceSchema != "https://example.test/resource" || childSpan.ScopeSchema != "https://example.test/scope" || childSpan.ResourceDrops != 3 || childSpan.ScopeDrops != 4 {
		t.Fatalf("detail time/schema/drop context=%+v", childSpan)
	}
	attrList, _ := childSpan.Attributes["attributes"].([]any)
	if len(attrList) != 1 || attrList[0].(map[string]any)["value"].(map[string]any)["intValue"] != "9007199254740993" {
		t.Fatalf("typed attributes=%v", childSpan.Attributes)
	}
	if childSpan.Resources == nil || childSpan.Scope == nil || childSpan.Events == nil || childSpan.Links == nil {
		t.Fatalf("missing full context=%+v", childSpan)
	}
	for name, pair := range map[string]struct {
		value  map[string]any
		needle string
	}{
		"resource": {childSpan.Resources, `"stringValue":"fixture"`},
		"scope":    {childSpan.Scope, `"boolValue":true`},
		"events":   {childSpan.Events, `"name":"checkpoint"`},
		"links":    {childSpan.Links, `"traceId":"AQIDBAUGBwgJCgsMDQ4PEA=="`},
	} {
		encoded, err := json.Marshal(pair.value)
		if err != nil || !strings.Contains(string(encoded), pair.needle) {
			t.Fatalf("%s context mismatch: %s (%v)", name, encoded, err)
		}
	}
	// Narrowing the selected interval makes the formerly present root missing.
	v.Set("from", base.Add(500*time.Millisecond).Format(time.RFC3339Nano))
	queryGET(t, baseURL, "/api/v1/traces/"+id, v, 200, &detail)
	if detail.Roots != 0 || !detail.MissingRoot {
		t.Fatalf("bounded missing root not reported: %+v", detail)
	}
	queryGET(t, baseURL, "/api/v1/traces/"+queryID(t, 16), v, 404, nil)
	// Explicitly reject invalid time windows and unknown parameters.
	v.Set("to", base.Add(500*time.Millisecond).Format(time.RFC3339Nano))
	queryGET(t, baseURL, "/api/v1/traces/"+id, v, 400, nil)
	v = queryWindow(base)
	v.Set("ignored", "x")
	queryGET(t, baseURL, "/api/v1/traces/"+id, v, 400, nil)
}

func TestQueryDetailCycleAndTruncation(t *testing.T) {
	baseURL := queryEnabled(t)
	store, _ := database(t)
	service := "query-" + queryID(t, 5)
	base := time.Now().UTC().Add(-4 * time.Minute).Truncate(time.Second)
	id := queryID(t, 16)
	a := queryID(t, 8)
	b := queryID(t, 8)
	cycle := []trace.Row{querySpan(id, a, b, service, "a", base, 10, 2, 0), querySpan(id, b, a, service, "b", base.Add(time.Nanosecond), 10, 2, 0)}
	if err := store.Write(context.Background(), cycle); err != nil {
		t.Fatal(err)
	}
	v := queryWindow(base)
	var flags struct {
		HasCycles      bool `json:"has_cycles"`
		HasMissingRoot bool `json:"has_missing_root"`
		Truncated      bool `json:"truncated"`
	}
	queryGET(t, baseURL, "/api/v1/traces/"+id, v, 200, &flags)
	if !flags.HasCycles || !flags.HasMissingRoot || flags.Truncated {
		t.Fatalf("cycle flags=%+v", flags)
	}
	// Direct storage writes create more than the detail cap in separate batches.
	bigID := queryID(t, 16)
	for batch := 0; batch < 9; batch++ {
		rows := make([]trace.Row, 0, 256)
		for i := 0; i < 256; i++ {
			n := batch*256 + i
			rows = append(rows, querySpan(bigID, fmt.Sprintf("%016x", n+1), "", service, "large", base.Add(time.Duration(n)*time.Microsecond), 1, 2, 0))
		}
		if err := store.Write(context.Background(), rows); err != nil {
			t.Fatal(err)
		}
	}
	var large struct {
		Spans     []json.RawMessage `json:"spans"`
		Truncated bool              `json:"truncated"`
		Reason    string            `json:"truncation_reason"`
	}
	queryGET(t, baseURL, "/api/v1/traces/"+bigID, v, 200, &large)
	if !large.Truncated || len(large.Spans) > 2048 || large.Reason == "" {
		t.Fatalf("large detail: count=%d truncated=%v reason=%q", len(large.Spans), large.Truncated, large.Reason)
	}
}

func TestQueryDetailResponseByteCap(t *testing.T) {
	baseURL := queryEnabled(t)
	store, _ := database(t)
	service := "query-" + queryID(t, 5)
	base := time.Now().UTC().Add(-4 * time.Minute).Truncate(time.Second)
	id := queryID(t, 16)
	largeValue := strings.Repeat("a", 4000) // below ingestion's single-value bound
	attributes := `{"attributes":[{"key":"large","value":{"stringValue":"` + largeValue + `"}}]}`
	for batch := 0; batch < 8; batch++ {
		rows := make([]trace.Row, 0, 256)
		for i := 0; i < 256; i++ {
			n := batch*256 + i
			row := querySpan(id, fmt.Sprintf("%016x", n+1), "", service, "byte cap", base.Add(time.Duration(n)*time.Microsecond), 1, 2, 0)
			row.SpanAttributes = attributes
			rows = append(rows, row)
		}
		if err := store.Write(context.Background(), rows); err != nil {
			t.Fatal(err)
		}
	}
	var out struct {
		Spans     []json.RawMessage `json:"spans"`
		Truncated bool              `json:"truncated"`
		Reason    string            `json:"truncation_reason"`
	}
	queryGET(t, baseURL, "/api/v1/traces/"+id, queryWindow(base), 200, &out)
	if !out.Truncated || out.Reason != "response_bytes" || len(out.Spans) >= 2048 || len(out.Spans) == 0 {
		t.Fatalf("byte cap count=%d truncated=%v reason=%q", len(out.Spans), out.Truncated, out.Reason)
	}
}

func TestQueryDurationBoundsAndErrors(t *testing.T) {
	baseURL := queryEnabled(t)
	base := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	v := queryWindow(base)
	for _, value := range []string{"0", "-1", "1.5", strconv.FormatUint(math.MaxUint64, 10) + "0"} {
		v.Set("min_duration_ns", value)
		queryGET(t, baseURL, "/api/v1/traces", v, 400, nil)
	}
	v.Del("min_duration_ns")
	v.Set("status", "error")
	queryGET(t, baseURL, "/api/v1/traces", v, 400, nil)
	v.Del("status")
	v.Set("limit", "501")
	queryGET(t, baseURL, "/api/v1/traces", v, 400, nil)
}

func TestQueryHalfOpenTimeDurationAndNamespace(t *testing.T) {
	baseURL := queryEnabled(t)
	store, _ := database(t)
	service := "query-" + queryID(t, 5)
	base := time.Now().UTC().Add(-4 * time.Minute).Truncate(time.Second)
	from, inside, to := queryID(t, 16), queryID(t, 16), queryID(t, 16)
	rows := []trace.Row{
		querySpan(from, queryID(t, 8), "", service, "Boundary", base, 100, 2, 2),
		querySpan(inside, queryID(t, 8), "", service, "Boundary", base.Add(time.Nanosecond), 200, 2, 0),
		querySpan(to, queryID(t, 8), "", service, "Boundary", base.Add(time.Second), 300, 2, 1),
	}
	rows[2].ServiceNamespace = ""
	if err := store.Write(context.Background(), rows); err != nil {
		t.Fatal(err)
	}
	v := url.Values{"from": {base.Format(time.RFC3339Nano)}, "to": {base.Add(time.Second).Format(time.RFC3339Nano)}, "service": {service}, "operation": {"Boundary"}}
	var out querySearch
	queryGET(t, baseURL, "/api/v1/traces", v, 200, &out)
	if len(out.Traces) != 2 {
		t.Fatalf("half-open search got %+v", out.Traces)
	}
	ids := map[string]bool{}
	for _, item := range out.Traces {
		ids[item.TraceID] = true
	}
	if !ids[from] || !ids[inside] || ids[to] {
		t.Fatalf("half-open IDs=%v", ids)
	}
	v.Set("min_duration_ns", "200")
	v.Set("max_duration_ns", "200")
	queryGET(t, baseURL, "/api/v1/traces", v, 200, &out)
	if len(out.Traces) != 1 || out.Traces[0].TraceID != inside {
		t.Fatalf("inclusive duration bounds=%+v", out.Traces)
	}
	v.Del("min_duration_ns")
	v.Del("max_duration_ns")
	v.Set("namespace", "")
	queryGET(t, baseURL, "/api/v1/traces", v, 200, &out)
	if len(out.Traces) != 0 {
		t.Fatalf("explicit empty namespace matched named namespace: %+v", out.Traces)
	}
	v.Set("from", base.Add(time.Second).Format(time.RFC3339Nano))
	v.Set("to", base.Add(2*time.Second).Format(time.RFC3339Nano))
	queryGET(t, baseURL, "/api/v1/traces", v, 200, &out)
	if len(out.Traces) != 1 || out.Traces[0].TraceID != to {
		t.Fatalf("empty namespace exact match=%+v", out.Traces)
	}
}
