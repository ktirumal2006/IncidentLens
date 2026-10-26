package ingest

import (
	"bytes"
	"context"
	"errors"
	logs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	metrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collector "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	resource "go.opentelemetry.io/proto/otlp/resource/v1"
	oteltrace "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"incidentlens/backend/internal/trace"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type writerFunc func(context.Context, []trace.Row) error

func (f writerFunc) Write(c context.Context, r []trace.Row) error { return f(c, r) }
func request(n int) *collector.ExportTraceServiceRequest {
	sc := &oteltrace.ScopeSpans{}
	for i := 0; i < n; i++ {
		sc.Spans = append(sc.Spans, &oteltrace.Span{TraceId: bytes.Repeat([]byte{1}, 16), SpanId: bytes.Repeat([]byte{2}, 8), StartTimeUnixNano: uint64(time.Now().Add(-time.Second).UnixNano()), EndTimeUnixNano: uint64(time.Now().UnixNano())})
	}
	return &collector.ExportTraceServiceRequest{ResourceSpans: []*oteltrace.ResourceSpans{{Resource: &resource.Resource{Attributes: []*common.KeyValue{{Key: "service.name", Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: "svc"}}}}}, ScopeSpans: []*oteltrace.ScopeSpans{sc}}}}
}
func TestBatchesAndPartialSuccess(t *testing.T) {
	calls := 0
	total := 0
	s := New(writerFunc(func(ctx context.Context, rows []trace.Row) error {
		calls++
		total += len(rows)
		if len(rows) > BatchSize {
			t.Fatal("unbounded batch")
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("no deadline")
		}
		return nil
	}))
	r := request(258)
	r.ResourceSpans[0].ScopeSpans[0].Spans[0].TraceId = nil
	response, err := s.Export(context.Background(), r)
	if err != nil || response.GetPartialSuccess().GetRejectedSpans() != 1 || calls != 2 || total != 257 {
		t.Fatalf("response=%v err=%v calls=%d total=%d", response, err, calls, total)
	}
}
func TestWriteFailureNeverPartialSuccess(t *testing.T) {
	calls := 0
	s := New(writerFunc(func(context.Context, []trace.Row) error {
		calls++
		if calls == 2 {
			return errors.New("secret db details")
		}
		return nil
	}))
	response, err := s.Export(context.Background(), request(257))
	if response != nil || status.Code(err) != codes.Unavailable || strings.Contains(err.Error(), "secret") {
		t.Fatalf("%v %v", response, err)
	}
}
func TestAllInvalidDoesNotWrite(t *testing.T) {
	s := New(writerFunc(func(context.Context, []trace.Row) error { t.Fatal("unexpected write"); return nil }))
	r := request(1)
	r.ResourceSpans[0].Resource = nil
	response, err := s.Export(context.Background(), r)
	if err != nil || response.GetPartialSuccess().GetRejectedSpans() != 1 {
		t.Fatalf("%v %v", response, err)
	}
}
func TestInflightExhaustion(t *testing.T) {
	entered := make(chan struct{}, MaxConcurrentRequests)
	release := make(chan struct{})
	s := New(writerFunc(func(context.Context, []trace.Row) error { entered <- struct{}{}; <-release; return nil }))
	var wg sync.WaitGroup
	for i := 0; i < MaxConcurrentRequests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.Export(context.Background(), request(1)); err != nil {
				t.Error(err)
			}
		}()
	}
	for i := 0; i < MaxConcurrentRequests; i++ {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("writer did not enter")
		}
	}
	if _, err := s.Export(context.Background(), request(1)); status.Code(err) != codes.Unavailable {
		t.Fatal(err)
	}
	close(release)
	wg.Wait()
	if _, err := s.Export(context.Background(), request(0)); err != nil {
		t.Fatal(err)
	}
}
func TestGRPCOutcomes(t *testing.T) {
	lis := bufconn.Listen(1 << 20)
	server := NewGRPCServer(writerFunc(func(context.Context, []trace.Row) error { return nil }))
	go server.Serve(lis)
	defer server.Stop()
	conn, err := grpc.NewClient("passthrough:///buffer", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r := request(1)
	r.ResourceSpans[0].ScopeSpans[0].Spans[0].Name = strings.Repeat("x", trace.MaxRequestBytes)
	if _, err := collector.NewTraceServiceClient(conn).Export(ctx, r); status.Code(err) != codes.ResourceExhausted {
		t.Fatal(err)
	}
	if _, err := metrics.NewMetricsServiceClient(conn).Export(ctx, &metrics.ExportMetricsServiceRequest{}); status.Code(err) != codes.Unimplemented {
		t.Fatal(err)
	}
	if _, err := logs.NewLogsServiceClient(conn).Export(ctx, &logs.ExportLogsServiceRequest{}); status.Code(err) != codes.Unimplemented {
		t.Fatal(err)
	}
}

func TestCanceledWriteCannotAcknowledge(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := New(writerFunc(func(context.Context, []trace.Row) error { cancel(); return nil }))
	response, err := s.Export(ctx, request(1))
	if response != nil || status.Code(err) != codes.Unavailable {
		t.Fatalf("%v %v", response, err)
	}
}

func TestGRPCGzip(t *testing.T) {
	lis := bufconn.Listen(1 << 20)
	var stored atomic.Int64
	server := NewGRPCServer(writerFunc(func(_ context.Context, rows []trace.Row) error {
		stored.Add(int64(len(rows)))
		return nil
	}))
	go server.Serve(lis)
	defer server.Stop()
	conn, err := grpc.NewClient("passthrough:///gzip", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := collector.NewTraceServiceClient(conn)
	response, err := client.Export(ctx, request(1), grpc.UseCompressor("gzip"))
	if err != nil || response.GetPartialSuccess().GetRejectedSpans() != 0 || stored.Load() != 1 {
		t.Fatalf("gzip export: response=%v err=%v stored=%d", response, err, stored.Load())
	}
	// Highly compressible input is small on the wire but exceeds the decompressed
	// receive budget. Reject it before validation or any storage write.
	oversized := request(1)
	oversized.ResourceSpans[0].ScopeSpans[0].Spans[0].Name = strings.Repeat("x", trace.MaxRequestBytes)
	if _, err := client.Export(ctx, oversized, grpc.UseCompressor("gzip")); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("decompressed request limit: %v", err)
	}
	if stored.Load() != 1 {
		t.Fatalf("oversized compressed export reached storage: %d", stored.Load())
	}
}
