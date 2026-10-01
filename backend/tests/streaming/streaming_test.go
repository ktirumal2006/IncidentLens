// Package streaming exercises the optional, local Kafka path against real services.
// Tests are deliberately gated and must run serially on a dedicated Compose stack.
package streaming

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	collectorpb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"incidentlens/backend/internal/detector"
	"incidentlens/backend/internal/query"
	storage "incidentlens/backend/internal/storage/clickhouse"
	"incidentlens/backend/internal/stream"
)

const producerAddress = "127.0.0.1:14318"

func requireStack(t *testing.T) driver.Conn {
	t.Helper()
	if os.Getenv("INCIDENTLENS_STREAMING_TESTS") != "1" {
		t.Skip("set INCIDENTLENS_STREAMING_TESTS=1 on the dedicated local streaming stack")
	}
	address := os.Getenv("CLICKHOUSE_TEST_ADDRESS")
	if address == "" {
		address = "127.0.0.1:19000"
	}
	conn, err := ch.Open(&ch.Options{Addr: []string{address}, Auth: ch.Auth{Database: "incidentlens", Username: "default", Password: "local-admin"}, DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		t.Fatalf("enabled streaming test requires ClickHouse: %v", err)
	}
	return conn
}

func fixture(t *testing.T, seed string, number int) (*collectorpb.ExportTraceServiceRequest, string, string) {
	t.Helper()
	trace := sha256.Sum256([]byte(fmt.Sprintf("%s/trace/%d", seed, number)))
	span := sha256.Sum256([]byte(fmt.Sprintf("%s/span/%d", seed, number)))
	id := hex.EncodeToString(trace[:16])
	namespace := "streamtest-" + hex.EncodeToString(trace[16:22])
	start := uint64(time.Now().UTC().Add(-time.Minute).UnixNano())
	request := &collectorpb.ExportTraceServiceRequest{ResourceSpans: []*tracepb.ResourceSpans{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
			{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "stream-fixture"}}},
			{Key: "service.namespace", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: namespace}}},
			{Key: "test.payload", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "retained-payload-" + seed}}},
		}},
		ScopeSpans: []*tracepb.ScopeSpans{{Spans: []*tracepb.Span{{TraceId: trace[:16], SpanId: span[:8], ParentSpanId: trace[24:32], Name: "stream.operation", Kind: tracepb.Span_SPAN_KIND_SERVER, StartTimeUnixNano: start, EndTimeUnixNano: start + 100_000_000}}}},
	}}}
	if bytes.Equal(trace[:16], make([]byte, 16)) || bytes.Equal(span[:8], make([]byte, 8)) {
		t.Fatal("fixture generated zero identity")
	}
	return request, id, namespace
}

func producer(t *testing.T) collectorpb.TraceServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(producerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return collectorpb.NewTraceServiceClient(conn)
}

func export(client collectorpb.TraceServiceClient, req *collectorpb.ExportTraceServiceRequest) (*collectorpb.ExportTraceServiceResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	return client.Export(ctx, req)
}

func seed(t *testing.T) string {
	t.Helper()
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

func compose(t *testing.T, args ...string) {
	t.Helper()
	base, err := filepath.Abs("../../../deploy/local/compose.yaml")
	if err != nil {
		t.Fatal(err)
	}
	overlay, err := filepath.Abs("../../../deploy/streaming/compose.yaml")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", append([]string{"compose", "-f", base, "-f", overlay}, args...)...)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("compose %v: %v: %s", args, err, output)
	}
}

func waitHTTP(t *testing.T, address string) {
	t.Helper()
	deadline := time.Now().Add(35 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		req, _ := http.NewRequestWithContext(ctx, "GET", "http://"+address+"/readyz", nil)
		response, err := http.DefaultClient.Do(req)
		cancel()
		if err == nil {
			response.Body.Close()
			if response.StatusCode == 200 {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("%s not ready within 35 seconds", address)
}

func stored(t *testing.T, conn driver.Conn, id, namespace string) uint64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var count uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM spans FINAL PREWHERE service_namespace=? AND service_name='stream-fixture' WHERE trace_id=? SETTINGS max_threads=2,max_final_threads=2", namespace, id).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func awaitStored(t *testing.T, conn driver.Conn, id, namespace string, want uint64) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		if got := stored(t, conn, id, namespace); got == want {
			return
		} else if time.Now().After(deadline) {
			t.Fatalf("FINAL trace %s has %d spans after 60 seconds; want %d", id, got, want)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func assertPayload(t *testing.T, conn driver.Conn, req *collectorpb.ExportTraceServiceRequest, id, namespace string) {
	t.Helper()
	span := req.ResourceSpans[0].ScopeSpans[0].Spans[0]
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	var spanID, parentID, service, storedNamespace, operation, attributes string
	var kind uint8
	var duration uint64
	var start, end time.Time
	err := conn.QueryRow(ctx, `SELECT span_id,parent_span_id,service_name,service_namespace,span_name,span_kind,duration_ns,start_time,end_time,resource_attributes
		FROM spans FINAL PREWHERE service_namespace=? AND service_name='stream-fixture' WHERE trace_id=? SETTINGS max_threads=2,max_final_threads=2`, namespace, id).
		Scan(&spanID, &parentID, &service, &storedNamespace, &operation, &kind, &duration, &start, &end, &attributes)
	if err != nil {
		t.Fatalf("stored fixture payload: %v", err)
	}
	var resource resourcepb.Resource
	if err := protojson.Unmarshal([]byte(attributes), &resource); err != nil || !proto.Equal(&resource, req.ResourceSpans[0].Resource) {
		t.Fatalf("resource payload changed during delivery/replay: %v", err)
	}
	if spanID != hex.EncodeToString(span.SpanId) || parentID != hex.EncodeToString(span.ParentSpanId) || service != "stream-fixture" || storedNamespace != namespace || operation != span.Name || kind != uint8(span.Kind) || duration != span.EndTimeUnixNano-span.StartTimeUnixNano || start.UnixNano() != int64(span.StartTimeUnixNano) || end.UnixNano() != int64(span.EndTimeUnixNano) {
		t.Fatalf("stored payload differs: span=%s parent=%s service=%s namespace=%s operation=%s kind=%d duration=%d start=%s end=%s attributes=%s", spanID, parentID, service, storedNamespace, operation, kind, duration, start, end, attributes)
	}
}

// A producer success here is a Kafka durability acknowledgment. It is not a
// ClickHouse acknowledgment and cannot be interpreted as query visibility.
func TestBrokerAckBeforeStorageThenWorkerRecovery(t *testing.T) {
	conn := requireStack(t)
	client := producer(t)
	compose(t, "stop", "stream-worker")
	t.Cleanup(func() {
		compose(t, "start", "kafka")
		compose(t, "start", "stream-worker")
	})
	req, id, namespace := fixture(t, seed(t), 1)
	response, err := export(client, req)
	if err != nil || response.GetPartialSuccess().GetRejectedSpans() != 0 {
		t.Fatalf("Kafka acceptance: response=%v error=%v", response, err)
	}
	t.Logf("Kafka ACK before retained broker restart: trace=%s namespace=%s", id, namespace)
	if got := stored(t, conn, id, namespace); got != 0 {
		t.Fatalf("broker ACK incorrectly treated as storage visibility: got %d", got)
	}
	compose(t, "stop", "kafka")
	compose(t, "start", "kafka")
	waitHTTP(t, "127.0.0.1:18082")
	if got := stored(t, conn, id, namespace); got != 0 {
		t.Fatalf("worker stayed stopped, but trace %s became visible after broker restart: %d", id, got)
	}
	compose(t, "start", "stream-worker")
	waitHTTP(t, "127.0.0.1:18083")
	awaitStored(t, conn, id, namespace, 1)
	assertPayload(t, conn, req, id, namespace)
}

func TestBrokerOutageReturnsRetryableFailure(t *testing.T) {
	conn := requireStack(t)
	client := producer(t)
	compose(t, "stop", "kafka")
	t.Cleanup(func() { compose(t, "start", "kafka", "stream-worker") })
	req, id, namespace := fixture(t, seed(t), 1)
	response, err := export(client, req)
	if err == nil || response != nil || (status.Code(err) != codes.Unavailable && status.Code(err) != codes.DeadlineExceeded) {
		t.Fatalf("broker outage: response=%v error=%v; want retryable non-ACK", response, err)
	}
	compose(t, "start", "kafka")
	compose(t, "start", "stream-worker")
	waitHTTP(t, "127.0.0.1:18082")
	waitHTTP(t, "127.0.0.1:18083")
	response, err = export(client, req)
	if err != nil || response.GetPartialSuccess().GetRejectedSpans() != 0 {
		t.Fatalf("caller replay: response=%v error=%v", response, err)
	}
	awaitStored(t, conn, id, namespace, 1)
}

func rawCount(t *testing.T, conn driver.Conn, id, namespace string) uint64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var count uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM spans PREWHERE service_namespace=? AND service_name='stream-fixture' WHERE trace_id=? SETTINGS max_threads=2", namespace, id).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func topicEnd(t *testing.T, c *kgo.Client, topic string) int64 {
	t.Helper()
	req := kmsg.NewPtrListOffsetsRequest()
	partition := kmsg.NewListOffsetsRequestTopicPartition()
	partition.Partition = 0
	partition.Timestamp = -1
	req.Topics = []kmsg.ListOffsetsRequestTopic{{Topic: topic, Partitions: []kmsg.ListOffsetsRequestTopicPartition{partition}}}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	response, err := req.RequestWith(ctx, c)
	if err != nil || len(response.Topics) != 1 || len(response.Topics[0].Partitions) != 1 || response.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("Kafka end offset: %+v error=%v", response, err)
	}
	return response.Topics[0].Partitions[0].Offset
}

func replayGroupUntil(t *testing.T, conn driver.Conn, admin *kgo.Client, topic, group, id, namespace string, rawWant uint64, committedWant int64) {
	t.Helper()
	writer, err := storage.Open("127.0.0.1:19000", "ingest", "local-ingest", "incidentlens")
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	worker, err := stream.NewWorker([]string{"127.0.0.1:19092"}, topic, group, writer)
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	for {
		if rawCount(t, conn, id, namespace) >= rawWant && committedAt(t, admin, topic, group) >= committedWant {
			cancel()
			select {
			case <-done:
			case <-time.After(6 * time.Second):
				t.Fatal("worker did not terminate after replay cancellation")
			}
			return
		}
		select {
		case err := <-done:
			t.Fatalf("worker stopped before replaying trace %s: %v", id, err)
		case <-ctx.Done():
			t.Fatalf("worker failed to replay trace %s within 90 seconds", id)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func TestWriteBeforeCommitCrashAndFreshGroupReplay(t *testing.T) {
	conn := requireStack(t)
	mergeContext, mergeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := conn.Exec(mergeContext, "SYSTEM STOP MERGES incidentlens.spans"); err != nil {
		mergeCancel()
		t.Fatal(err)
	}
	mergeCancel()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := conn.Exec(ctx, "SYSTEM START MERGES incidentlens.spans"); err != nil {
			t.Errorf("restore background merges: %v", err)
		}
	})
	admin, topic := isolatedTopic(t)
	publisher, err := stream.NewProducer([]string{"127.0.0.1:19092"}, topic)
	if err != nil {
		t.Fatal(err)
	}
	defer publisher.Close()
	server := stream.NewServer(publisher)
	runSeed := seed(t)
	req, id, namespace := fixture(t, runSeed, 3)
	startOffset := topicEnd(t, admin, topic)
	if startOffset != 0 {
		t.Fatalf("isolated topic start offset=%d, want 0", startOffset)
	}
	group := "streamtest-crash-" + runSeed
	commitAt(t, admin, topic, group, startOffset)
	publishCtx, publishCancel := context.WithTimeout(context.Background(), 12*time.Second)
	response, err := server.Export(publishCtx, req)
	publishCancel()
	if err != nil || response.GetPartialSuccess().GetRejectedSpans() != 0 {
		t.Fatalf("Kafka ACK: response=%v error=%v", response, err)
	}
	endOffset := topicEnd(t, admin, topic)
	if endOffset != startOffset+1 {
		t.Fatalf("one fixture should publish one broker record: offsets %d -> %d", startOffset, endOffset)
	}
	t.Logf("trace=%s group=%s Kafka record offset=%d next=%d", id, group, startOffset, endOffset)
	if got := stored(t, conn, id, namespace); got != 0 {
		t.Fatalf("isolated topic has no consumer but trace already stored: %d", got)
	}
	base, _ := filepath.Abs("../../../deploy/local/compose.yaml")
	overlay, _ := filepath.Abs("../../../deploy/streaming/compose.yaml")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "docker", "compose", "-f", base, "-f", overlay,
		"run", "--rm", "--no-deps", "-e", "KAFKA_GROUP_ID="+group,
		"-e", "KAFKA_TOPIC="+topic,
		"-e", "STREAM_TEST_EXIT_AFTER_WRITE_TRACE_ID="+id, "stream-worker")
	output, err := command.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("test crash hook timed out: %v: %s", ctx.Err(), output)
	}
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 86 {
		t.Fatalf("worker crash hook exit=%v; want 86: %s", err, output)
	}
	awaitStored(t, conn, id, namespace, 1)
	if got := committedAt(t, admin, topic, group); got != startOffset {
		t.Fatalf("write-before-commit crash advanced group offset %d; want %d", got, startOffset)
	}
	assertPayload(t, conn, req, id, namespace)
	reader, err := storage.OpenQuery("127.0.0.1:19000", "query", "local-query", "incidentlens")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	observed := time.Now().UTC()
	end := observed
	frozenBefore, err := reader.Incidents(context.Background(), end, observed, detector.DefaultConfig(), detector.Filter{Namespace: &namespace})
	if err != nil || len(frozenBefore.Operations) != 1 || frozenBefore.Operations[0].Current.Count != 1 {
		t.Fatalf("before replay detector count: %+v error=%v", frozenBefore, err)
	}
	before := rawCount(t, conn, id, namespace)
	replayGroupUntil(t, conn, admin, topic, group, id, namespace, before+1, endOffset)
	t.Logf("same-group replay committed offset=%d", committedAt(t, admin, topic, group))
	if got := stored(t, conn, id, namespace); got != 1 {
		t.Fatalf("write-before-commit replay inflated FINAL count to %d", got)
	}
	before = rawCount(t, conn, id, namespace)
	freshGroup := "streamtest-fresh-" + runSeed
	replayGroupUntil(t, conn, admin, topic, freshGroup, id, namespace, before+1, endOffset)
	t.Logf("fresh-group replay committed offset=%d", committedAt(t, admin, topic, freshGroup))
	if got := stored(t, conn, id, namespace); got != 1 {
		t.Fatalf("fresh-group replay inflated FINAL count to %d", got)
	}
	summary, err := reader.Services(context.Background(), query.Filter{Window: query.Window{From: end.Add(-5 * time.Minute), To: end}, Namespace: &namespace, Limit: 10}, observed)
	if err != nil || len(summary.Services) != 1 || summary.Services[0].Count != 1 {
		t.Fatalf("duplicate-safe query summary: %+v error=%v", summary, err)
	}
	incident, err := reader.Incidents(context.Background(), end, observed, detector.DefaultConfig(), detector.Filter{Namespace: &namespace})
	if err != nil || len(incident.Operations) != 1 || incident.Operations[0].Current.Count != 1 || !reflect.DeepEqual(incident, frozenBefore) {
		t.Fatalf("duplicate-safe detector count: %+v error=%v", incident, err)
	}
}

func TestInvalidAndOversizedRequestsFailClosed(t *testing.T) {
	conn := requireStack(t)
	client := producer(t)
	req, id, namespace := fixture(t, seed(t), 1)
	invalid := proto.Clone(req).(*collectorpb.ExportTraceServiceRequest)
	invalid.ResourceSpans[0].ScopeSpans[0].Spans[0].SpanId = nil
	response, err := export(client, invalid)
	if err != nil || response.GetPartialSuccess().GetRejectedSpans() != 1 {
		t.Fatalf("invalid-only request: response=%v error=%v", response, err)
	}
	if got := stored(t, conn, id, namespace); got != 0 {
		t.Fatalf("permanently invalid span stored: %d", got)
	}
	mixed, mixedID, mixedNamespace := fixture(t, seed(t), 2)
	mixedBad := proto.Clone(mixed.ResourceSpans[0].ScopeSpans[0].Spans[0]).(*tracepb.Span)
	mixedBad.SpanId = nil
	mixed.ResourceSpans[0].ScopeSpans[0].Spans = append(mixed.ResourceSpans[0].ScopeSpans[0].Spans, mixedBad)
	response, err = export(client, mixed)
	if err != nil || response.GetPartialSuccess().GetRejectedSpans() != 1 {
		t.Fatalf("mixed request: response=%v error=%v", response, err)
	}
	awaitStored(t, conn, mixedID, mixedNamespace, 1)
	oversize := proto.Clone(req).(*collectorpb.ExportTraceServiceRequest)
	oversize.ResourceSpans[0].ScopeSpans[0].Spans[0].Name = string(bytes.Repeat([]byte{'x'}, 4<<20))
	response, err = export(client, oversize)
	if response != nil || status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("oversized whole request: response=%v error=%v", response, err)
	}
	if got := stored(t, conn, id, namespace); got != 0 {
		t.Fatalf("oversized request published to storage: %d", got)
	}
}
