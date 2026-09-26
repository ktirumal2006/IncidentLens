package integration

import (
	"bytes"
	"context"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	collectorpb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

func exportTo(t *testing.T, address string, req *collectorpb.ExportTraceServiceRequest) {
	t.Helper()
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	response, err := collectorpb.NewTraceServiceClient(conn).Export(ctx, req)
	if err != nil {
		t.Fatalf("export to %s: %v", address, err)
	}
	if response.PartialSuccess != nil && response.PartialSuccess.RejectedSpans != 0 {
		t.Fatalf("fixture rejected: %v", response)
	}
}

func TestCollectorStoresThreeServiceFixture(t *testing.T) {
	_, conn := database(t)
	req := fixture(t)
	base := req.ResourceSpans[0]
	req.ResourceSpans = nil
	for i, name := range []string{"fixture.frontend", "fixture.recommendation", "fixture.catalog"} {
		resource := proto.Clone(base).(*tracepb.ResourceSpans)
		resource.Resource.Attributes[0].Value = stringValue(name)
		span := resource.ScopeSpans[0].Spans[0]
		span.SpanId = bytes.Repeat([]byte{byte(i + 1)}, 8)
		span.ParentSpanId = nil
		if i > 0 {
			span.ParentSpanId = bytes.Repeat([]byte{byte(i)}, 8)
		}
		req.ResourceSpans = append(req.ResourceSpans, resource)
	}
	address := os.Getenv("COLLECTOR_TEST_ADDRESS")
	if address == "" {
		address = "127.0.0.1:4317"
	}
	exportTo(t, address, req)
	id := hex.EncodeToString(base.ScopeSpans[0].Spans[0].TraceId)
	deadline := time.Now().Add(15 * time.Second)
	for count(t, conn, id, true) != 3 {
		if time.Now().After(deadline) {
			t.Fatal("Collector acknowledged input but three spans did not become visible within 15 seconds")
		}
		time.Sleep(100 * time.Millisecond)
	}
	rows, err := conn.Query(context.Background(), "SELECT service_name, span_id, parent_span_id FROM spans FINAL WHERE trace_id = ? ORDER BY span_id", id)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	i := 0
	for rows.Next() {
		var service, span, parent string
		if err := rows.Scan(&service, &span, &parent); err != nil {
			t.Fatal(err)
		}
		want := req.ResourceSpans[i]
		s := want.ScopeSpans[0].Spans[0]
		if service != want.Resource.Attributes[0].Value.GetStringValue() || span != hex.EncodeToString(s.SpanId) || parent != hex.EncodeToString(s.ParentSpanId) {
			t.Fatalf("service relationship changed: %s %s %s", service, span, parent)
		}
		i++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if i != 3 {
		t.Fatalf("read %d spans, want three", i)
	}
}

func TestAcknowledgedDataSurvivesLocalServiceRestarts(t *testing.T) {
	if os.Getenv("INCIDENTLENS_RESTART_TESTS") != "1" {
		t.Skip("set INCIDENTLENS_RESTART_TESTS=1 to restart local ingest and ClickHouse containers")
	}
	_, conn := database(t)
	req := fixture(t)
	exportTo(t, "127.0.0.1:14317", req)
	id := hex.EncodeToString(req.ResourceSpans[0].ScopeSpans[0].Spans[0].TraceId)
	if n := count(t, conn, id, true); n != 1 {
		t.Fatalf("acknowledged row count=%d", n)
	}
	compose, err := filepath.Abs("../../../deploy/local/compose.yaml")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "docker", "compose", "-f", compose, "restart", "ingest", "clickhouse")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("restart: %v\n%s", err, output)
	}
	// The original client reconnects after restart. No process-memory fixture is re-exported.
	deadline := time.Now().Add(30 * time.Second)
	for {
		var n uint64
		err := conn.QueryRow(ctx, "SELECT count() FROM spans FINAL WHERE trace_id = ?", id).Scan(&n)
		if err == nil && n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("acknowledged data missing after restart: count=%d error=%v", n, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
