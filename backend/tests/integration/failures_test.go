package integration

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	collectorpb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// These tests deliberately stop local services. They must not run concurrently
// with other integration commands or a Demo scenario. No volumes are removed.
func failureStack(t *testing.T) driver.Conn {
	t.Helper()
	if os.Getenv("INCIDENTLENS_FAILURE_TESTS") != "1" {
		t.Skip("set INCIDENTLENS_FAILURE_TESTS=1 to exercise local service outages and Collector queue loss")
	}
	_, conn := database(t)
	t.Cleanup(func() { composeFailure(t, "start", "clickhouse", "ingest", "collector"); waitIngestionReady(t) })
	return conn
}
func composeFailure(t *testing.T, args ...string) string {
	t.Helper()
	path, err := filepath.Abs("../../../deploy/local/compose.yaml")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "docker", append([]string{"compose", "-f", path}, args...)...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("docker compose %v: %v\n%s", args, err, output)
	}
	return string(output)
}
func waitIngestionReady(t *testing.T) {
	t.Helper()
	client := http.Client{Timeout: time.Second}
	deadline := time.Now().Add(40 * time.Second)
	for {
		response, err := client.Get("http://127.0.0.1:18080/readyz")
		if err == nil {
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("ingestion did not become storage-ready within 40 seconds")
		}
		time.Sleep(100 * time.Millisecond)
	}
}
func failureClient(t *testing.T, address string) collectorpb.TraceServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return collectorpb.NewTraceServiceClient(conn)
}
func failureExport(client collectorpb.TraceServiceClient, req *collectorpb.ExportTraceServiceRequest) (*collectorpb.ExportTraceServiceResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	return client.Export(ctx, req)
}
func waitCollectorReady(t *testing.T, client collectorpb.TraceServiceClient) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		if _, err := failureExport(client, &collectorpb.ExportTraceServiceRequest{}); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("Collector did not become ready")
		}
		time.Sleep(100 * time.Millisecond)
	}
}
func fixtureID(req *collectorpb.ExportTraceServiceRequest) string {
	return hex.EncodeToString(req.ResourceSpans[0].ScopeSpans[0].Spans[0].TraceId)
}
func failureCount(t *testing.T, conn driver.Conn, id string) uint64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var n uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM spans FINAL WHERE trace_id = ?", id).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
func waitFixtureCount(t *testing.T, conn driver.Conn, id string, want uint64) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		n := failureCount(t, conn, id)
		if n == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("trace %s count=%d want %d", id, n, want)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestRealStorageOutageAndRetry(t *testing.T) {
	conn := failureStack(t)
	client := failureClient(t, "127.0.0.1:14317")
	baseline := fixture(t)
	exportTo(t, "127.0.0.1:14317", baseline)
	composeFailure(t, "stop", "clickhouse")
	req := fixture(t)
	response, err := failureExport(client, req)
	if status.Code(err) != codes.Unavailable || response != nil {
		t.Fatalf("outage returned response=%v error=%v; want Unavailable and no acknowledgement", response, err)
	}
	composeFailure(t, "start", "clickhouse")
	waitIngestionReady(t)
	for range 2 {
		response, err = failureExport(client, req)
		if err != nil || response.GetPartialSuccess().GetRejectedSpans() != 0 {
			t.Fatalf("recovery export: response=%v error=%v", response, err)
		}
	}
	waitFixtureCount(t, conn, fixtureID(req), 1)
	waitFixtureCount(t, conn, fixtureID(baseline), 1)
	t.Log("Real storage outage returned Unavailable; replay recovered one logical span and previously acknowledged data survived")
}

// startExport leaves the request unacknowledged while downstream storage is down.
func startExport(client collectorpb.TraceServiceClient, req *collectorpb.ExportTraceServiceRequest) <-chan error {
	result := make(chan error, 1)
	go func() {
		response, err := failureExport(client, req)
		if err == nil && response.GetPartialSuccess().GetRejectedSpans() != 0 {
			err = status.Error(codes.InvalidArgument, "unexpected partial success")
		}
		result <- err
	}()
	return result
}
func assertPending(t *testing.T, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		t.Fatalf("Collector returned before downstream recovery: %v", err)
	case <-time.After(1500 * time.Millisecond):
	}
}

func TestCollectorShortOutageAndForcedRestartRecovery(t *testing.T) {
	conn := failureStack(t)
	client := failureClient(t, "127.0.0.1:4317")
	composeFailure(t, "stop", "ingest")
	buffered := fixture(t)
	pending := startExport(client, buffered)
	assertPending(t, pending)
	if n := failureCount(t, conn, fixtureID(buffered)); n != 0 {
		t.Fatalf("stopped ingestion nevertheless stored %d spans", n)
	}
	composeFailure(t, "start", "ingest")
	waitIngestionReady(t)
	if err := <-pending; err != nil {
		t.Fatalf("short-outage request failed: %v", err)
	}
	waitFixtureCount(t, conn, fixtureID(buffered), 1)

	composeFailure(t, "stop", "ingest")
	lost := fixture(t)
	pending = startExport(client, lost)
	assertPending(t, pending)
	composeFailure(t, "kill", "-s", "SIGKILL", "collector")
	if err := <-pending; status.Code(err) != codes.Unavailable {
		t.Fatalf("Collector crash returned %v; want retryable Unavailable", err)
	}
	composeFailure(t, "start", "collector", "ingest")
	waitIngestionReady(t)
	waitCollectorReady(t, client)
	if n := failureCount(t, conn, fixtureID(lost)); n != 0 {
		t.Fatalf("unacknowledged volatile fixture unexpectedly stored: %d", n)
	}
	exportTo(t, "127.0.0.1:4317", lost)
	waitFixtureCount(t, conn, fixtureID(lost), 1)
	waitFixtureCount(t, conn, fixtureID(buffered), 1)
	t.Log("Short outage waited for storage; Collector crash lost only unacknowledged input, and caller replay recovered one logical span")
}

func TestCollectorQueueExhaustionAccounting(t *testing.T) {
	conn := failureStack(t)
	client := failureClient(t, "127.0.0.1:4317")
	composeFailure(t, "restart", "collector")
	waitCollectorReady(t, client)
	composeFailure(t, "stop", "ingest")
	since := time.Now().UTC().Format(time.RFC3339Nano)
	original := fixture(t)
	id := fixtureID(original)
	resource := original.ResourceSpans[0]
	resource.Resource.Attributes = resource.Resource.Attributes[:2]
	resource.ScopeSpans[0].Scope.Attributes = nil
	base := resource.ScopeSpans[0].Spans[0]
	base.Attributes = nil
	base.Events = nil
	base.Links = nil
	base.ParentSpanId = nil
	const batches = 64
	const batchSize = 256
	requests := make([]*collectorpb.ExportTraceServiceRequest, batches)
	type outcome struct {
		batch int
		err   error
	}
	results := make(chan outcome, batches)
	for batch := 0; batch < batches; batch++ {
		req := proto.Clone(original).(*collectorpb.ExportTraceServiceRequest)
		req.ResourceSpans[0].ScopeSpans[0].Spans = nil
		for index := 0; index < batchSize; index++ {
			span := proto.Clone(base).(*tracepb.Span)
			span.SpanId = make([]byte, 8)
			binary.BigEndian.PutUint64(span.SpanId, uint64(batch*batchSize+index+1))
			req.ResourceSpans[0].ScopeSpans[0].Spans = append(req.ResourceSpans[0].ScopeSpans[0].Spans, span)
		}
		requests[batch] = req
		go func(batch int, req *collectorpb.ExportTraceServiceRequest) {
			response, err := failureExport(client, req)
			if err == nil && response.GetPartialSuccess().GetRejectedSpans() != 0 {
				err = status.Error(codes.InvalidArgument, "unexpected partial success")
			}
			results <- outcome{batch, err}
		}(batch, req)
	}
	// Requests fit comfortably in the configured memory limit; the 32-request
	// exporter queue is the deliberately exhausted resource.
	time.Sleep(1500 * time.Millisecond)
	logs := composeFailure(t, "logs", "--since", since, "collector")
	if !strings.Contains(strings.ToLower(logs), "queue is full") {
		t.Fatalf("did not observe explicit exporter queue-full failure; logs:\n%s", logs)
	}
	// Every early response must be retryable, never successful while storage is down.
	outcomes := make([]outcome, 0, batches)
	for len(results) > 0 {
		result := <-results
		if status.Code(result.err) != codes.Unavailable {
			t.Fatalf("queue outage returned %v; want Unavailable before recovery", result.err)
		}
		outcomes = append(outcomes, result)
	}
	if len(outcomes) == 0 {
		t.Fatal("queue exhaustion produced no prompt retryable rejection")
	}
	composeFailure(t, "start", "ingest")
	waitIngestionReady(t)
	for len(outcomes) < batches {
		outcomes = append(outcomes, <-results)
	}
	acknowledged := 0
	var retry []int
	for _, result := range outcomes {
		if result.err == nil {
			acknowledged += batchSize
			continue
		}
		if status.Code(result.err) != codes.Unavailable {
			t.Fatalf("batch %d returned nonretryable failure: %v", result.batch, result.err)
		}
		retry = append(retry, result.batch)
	}
	waitFixtureCount(t, conn, id, uint64(acknowledged))
	for _, batch := range retry {
		response, err := failureExport(client, requests[batch])
		if err != nil || response.GetPartialSuccess().GetRejectedSpans() != 0 {
			t.Fatalf("retry batch %d: response=%v error=%v", batch, response, err)
		}
	}
	waitFixtureCount(t, conn, id, batches*batchSize)
	t.Logf("offered=%d initially-acknowledged=%d retryable-rejected=%d final-unique=%d; no acknowledged input lost", batches*batchSize, acknowledged, len(retry)*batchSize, batches*batchSize)
}
