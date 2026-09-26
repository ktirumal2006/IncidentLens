// Package integration checks delivery semantics against the real local ClickHouse.
// Run from backend with INCIDENTLENS_INTEGRATION=1 go test ./tests/integration -v.
package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	collectorpb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"incidentlens/backend/internal/ingest"
	storage "incidentlens/backend/internal/storage/clickhouse"
	"incidentlens/backend/internal/trace"
)

func database(t *testing.T) (*storage.Store, driver.Conn) {
	t.Helper()
	if os.Getenv("INCIDENTLENS_INTEGRATION") != "1" {
		t.Skip("requires local Compose ClickHouse; set INCIDENTLENS_INTEGRATION=1")
	}
	address := os.Getenv("CLICKHOUSE_TEST_ADDRESS")
	if address == "" {
		address = "127.0.0.1:19000"
	}
	store, err := storage.Open(address, "default", "local-admin", "incidentlens")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	conn, err := ch.Open(&ch.Options{Addr: []string{address}, Auth: ch.Auth{Database: "incidentlens", Username: "default", Password: "local-admin"}, DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		t.Fatalf("local ClickHouse is required when integration is enabled: %v", err)
	}
	return store, conn
}

func stringValue(s string) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: s}}
}
func attr(k string, v *commonpb.AnyValue) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: v}
}

// Every test owns a random trace identity, so existing demo data is never cleared.
func fixture(t *testing.T) *collectorpb.ExportTraceServiceRequest {
	t.Helper()
	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		t.Fatal(err)
	}
	start := uint64(time.Now().UTC().Add(-time.Minute).UnixNano())
	typed := []*commonpb.KeyValue{
		attr("test.string", stringValue("quoted \"value\" ☃")),
		attr("test.integer", &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 9007199254740993}}),
		attr("test.boolean", &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: true}}),
		attr("test.double", &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: 1.25}}),
		attr("test.bytes", &commonpb.AnyValue{Value: &commonpb.AnyValue_BytesValue{BytesValue: []byte{0, 1, 255}}}),
		attr("test.array", &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{Values: []*commonpb.AnyValue{stringValue("nested"), {Value: &commonpb.AnyValue_IntValue{IntValue: -9}}}}}}),
		attr("test.map", &commonpb.AnyValue{Value: &commonpb.AnyValue_KvlistValue{KvlistValue: &commonpb.KeyValueList{Values: []*commonpb.KeyValue{attr("test.inside", stringValue("value"))}}}}),
	}
	span := &tracepb.Span{TraceId: id, SpanId: bytes.Repeat([]byte{1}, 8), ParentSpanId: bytes.Repeat([]byte{2}, 8), Name: "fixture operation", Kind: tracepb.Span_SPAN_KIND_SERVER, StartTimeUnixNano: start, EndTimeUnixNano: start + 123456789, TraceState: "vendor=value", Flags: 1, Attributes: typed, DroppedAttributesCount: 3, DroppedEventsCount: 4, DroppedLinksCount: 5, Status: &tracepb.Status{Code: tracepb.Status_STATUS_CODE_ERROR, Message: "fixture error"}, Events: []*tracepb.Span_Event{{TimeUnixNano: start + 99, Name: "event", Attributes: typed, DroppedAttributesCount: 6}}, Links: []*tracepb.Span_Link{{TraceId: bytes.Repeat([]byte{3}, 16), SpanId: bytes.Repeat([]byte{4}, 8), TraceState: "link=value", Flags: 1, Attributes: typed, DroppedAttributesCount: 7}}}
	return &collectorpb.ExportTraceServiceRequest{ResourceSpans: []*tracepb.ResourceSpans{{Resource: &resourcepb.Resource{Attributes: append([]*commonpb.KeyValue{attr("service.name", stringValue("integration-fixture")), attr("service.namespace", stringValue("tests"))}, typed...), DroppedAttributesCount: 8}, SchemaUrl: "https://fixture.test/resource-schema", ScopeSpans: []*tracepb.ScopeSpans{{Scope: &commonpb.InstrumentationScope{Name: "fixture.scope", Version: "1.2.3", Attributes: typed, DroppedAttributesCount: 9}, SchemaUrl: "https://fixture.test/scope-schema", Spans: []*tracepb.Span{span}}}}}}
}

func count(t *testing.T, conn driver.Conn, id string, final bool) uint64 {
	t.Helper()
	sql := "SELECT count() FROM spans"
	if final {
		sql += " FINAL"
	}
	sql += " WHERE trace_id = ?"
	var n uint64
	if err := conn.QueryRow(context.Background(), sql, id).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestFullContextRoundTrip(t *testing.T) {
	store, conn := database(t)
	req := fixture(t)
	expected, rejected, err := trace.Normalize(req, time.Now().UTC())
	if err != nil || rejected != 0 || len(expected) != 1 {
		t.Fatalf("normalization: rows=%d rejected=%d error=%v", len(expected), rejected, err)
	}
	if err := store.Write(context.Background(), expected); err != nil {
		t.Fatal(err)
	}
	// Scan in migration column order, then compare the complete stored row.
	var stored trace.Row
	err = conn.QueryRow(context.Background(), "SELECT * FROM spans FINAL WHERE trace_id = ?", expected[0].TraceID).Scan(
		&stored.TraceID, &stored.SpanID, &stored.ParentSpanID,
		&stored.ServiceName, &stored.ServiceNamespace, &stored.SpanName,
		&stored.SpanKind, &stored.StatusCode, &stored.StatusMessage,
		&stored.StartTime, &stored.EndTime, &stored.DurationNS, &stored.IngestedAt,
		&stored.ResourceAttributes, &stored.SpanAttributes, &stored.ScopeAttributes,
		&stored.ScopeName, &stored.ScopeVersion, &stored.ResourceSchemaURL, &stored.ScopeSchemaURL,
		&stored.Events, &stored.Links, &stored.TraceState, &stored.TraceFlags,
		&stored.DroppedAttributesCount, &stored.DroppedEventsCount, &stored.DroppedLinksCount,
		&stored.ResourceDroppedAttributesCount, &stored.ScopeDroppedAttributesCount,
	)
	if err != nil {
		t.Fatal(err)
	}
	// Normalize location pointers without changing the nanosecond instants.
	stored.StartTime = stored.StartTime.UTC()
	stored.EndTime = stored.EndTime.UTC()
	stored.IngestedAt = stored.IngestedAt.UTC()
	if !reflect.DeepEqual(stored, expected[0]) {
		t.Fatalf("stored row differs:\ngot  %#v\nwant %#v", stored, expected[0])
	}
	if n := count(t, conn, expected[0].TraceID, true); n != 1 {
		t.Fatalf("one fixture produced %d logical rows", n)
	}
	original := req.ResourceSpans[0]
	for _, pair := range []struct {
		json       string
		attributes []*commonpb.KeyValue
	}{
		{stored.ResourceAttributes, original.Resource.Attributes},
		{stored.ScopeAttributes, original.ScopeSpans[0].Scope.Attributes},
		{stored.SpanAttributes, original.ScopeSpans[0].Spans[0].Attributes},
	} {
		var decoded resourcepb.Resource
		if err := protojson.Unmarshal([]byte(pair.json), &decoded); err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(&decoded, &resourcepb.Resource{Attributes: pair.attributes}) {
			t.Errorf("typed attributes did not round-trip: %s", pair.json)
		}
	}
	var events, links tracepb.Span
	if err := protojson.Unmarshal([]byte(stored.Events), &events); err != nil {
		t.Fatal(err)
	}
	if err := protojson.Unmarshal([]byte(stored.Links), &links); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(&events, &tracepb.Span{Events: original.ScopeSpans[0].Spans[0].Events}) {
		t.Error("event context did not round-trip")
	}
	if !proto.Equal(&links, &tracepb.Span{Links: original.ScopeSpans[0].Spans[0].Links}) {
		t.Error("link context did not round-trip")
	}

}

type lostAcknowledgment struct{ store *storage.Store }

func (w lostAcknowledgment) Write(ctx context.Context, rows []trace.Row) error {
	if err := w.store.Write(ctx, rows); err != nil {
		return err
	}
	return errors.New("simulated response loss after successful synchronous insert")
}

func TestAmbiguousWriteRetryBeforeMerges(t *testing.T) {
	store, conn := database(t)
	// This opt-in test temporarily stops merges in the local test table only.
	if err := conn.Exec(context.Background(), "SYSTEM STOP MERGES incidentlens.spans"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := conn.Exec(context.Background(), "SYSTEM START MERGES incidentlens.spans"); err != nil {
			t.Errorf("restore merges: %v", err)
		}
	}()
	req := fixture(t)
	_, err := ingest.New(lostAcknowledgment{store}).Export(context.Background(), req)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("ambiguous write returned %v, want Unavailable", err)
	}
	response, err := ingest.New(store).Export(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if response.PartialSuccess != nil && response.PartialSuccess.RejectedSpans != 0 {
		t.Fatal(response)
	}
	id := hex.EncodeToString(req.ResourceSpans[0].ScopeSpans[0].Spans[0].TraceId)
	if n := count(t, conn, id, false); n != 2 {
		t.Fatalf("raw count=%d want two pre-merge copies", n)
	}
	if n := count(t, conn, id, true); n != 1 {
		t.Fatalf("deduplicated count=%d want one logical span", n)
	}
}

func TestMixedBatchPersistsOnlyValidSpans(t *testing.T) {
	store, conn := database(t)
	req := fixture(t)
	valid := req.ResourceSpans[0].ScopeSpans[0].Spans[0]
	invalid := proto.Clone(valid).(*tracepb.Span)
	invalid.SpanId = []byte{1}
	req.ResourceSpans[0].ScopeSpans[0].Spans = append(req.ResourceSpans[0].ScopeSpans[0].Spans, invalid)
	response, err := ingest.New(store).Export(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if response.PartialSuccess == nil || response.PartialSuccess.RejectedSpans != 1 {
		t.Fatalf("partial success=%v, want one rejection", response.PartialSuccess)
	}
	if n := count(t, conn, hex.EncodeToString(valid.TraceId), true); n != 1 {
		t.Fatalf("stored count=%d want one", n)
	}
}

func TestUnavailableStorageNeverAcknowledges(t *testing.T) {
	_, _ = database(t)
	// Port zero is not a reachable server. Use the real driver, not a mock error.
	unavailable, err := storage.Open("127.0.0.1:0", "default", "local-admin", "incidentlens")
	if err != nil {
		t.Fatal(err)
	}
	defer unavailable.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	response, err := ingest.New(unavailable).Export(ctx, fixture(t))
	if err == nil || response != nil {
		t.Fatalf("unavailable storage acknowledged: response=%v error=%v", response, err)
	}
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("code=%v want Unavailable: %v", status.Code(err), err)
	}
}
