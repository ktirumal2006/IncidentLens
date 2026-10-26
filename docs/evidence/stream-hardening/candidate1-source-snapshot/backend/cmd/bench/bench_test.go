package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	collectorpb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func testConfig(t *testing.T) Config {
	t.Helper()
	c, e := parseConfig([]string{"--output-dir", filepath.Join(t.TempDir(), "run"), "--seed", "unit-seed"})
	if e != nil {
		t.Fatal(e)
	}
	return c
}
func TestConfigBounds(t *testing.T) {
	for _, args := range [][]string{{"--query-rate", "NaN"}, {"--query-rate", "Inf"}, {"--rate", "100001"}, {"--rate", "100000", "--duration", "1h"}, {"--batch-traces", "43"}} {
		_, e := parseConfig(append([]string{"--output-dir", "unused", "--seed", "test"}, args...))
		if e == nil {
			t.Fatalf("accepted %v", args)
		}
	}
	c := testConfig(t)
	if c.Services[0] != "bench-frontend" || c.Concurrency != 16 || c.Queue != 64 {
		t.Fatal(c)
	}
}
func TestImmutablePayloadAndTraceChain(t *testing.T) {
	c := testConfig(t)
	at := time.Unix(1700000000, 0)
	a := makeRequest(c, 0, 2, at, false)
	if !proto.Equal(a, makeRequest(c, 0, 2, at, false)) {
		t.Fatal("retry payload changed")
	}
	kinds := map[int32]int{}
	n := 0
	for si, rs := range a.ResourceSpans {
		for _, s := range rs.ScopeSpans[0].Spans {
			n++
			kinds[int32(s.Kind)]++
			if len(s.TraceId) != 16 || len(s.SpanId) != 8 {
				t.Fatal("invalid identity")
			}
			index := int(s.SpanId[7]) - 1
			if index/2 != si {
				t.Fatal("service distribution")
			}
			if index > 0 && int(s.ParentSpanId[7]) != index {
				t.Fatal("broken parent")
			}
			if s.EndTimeUnixNano-s.StartTimeUnixNano != uint64(time.Duration(300-index*20)*time.Millisecond) {
				t.Fatal("duration mismatch")
			}
		}
	}
	if n != 12 || kinds[2] != 6 || kinds[3] != 4 || kinds[1] != 2 {
		t.Fatal(n, kinds)
	}
}
func TestBatchBoundaryAndSetupLedger(t *testing.T) {
	c := testConfig(t)
	c.BaselineTraces = 33
	c.LoadTraces = 65
	c.Warmup = time.Second
	c.Rate = 192
	h := &Harness{config: c}
	for i := 0; i < 2; i++ {
		h.batches = append(h.batches, &Batch{First: i * 32, Count: min(32, 33-i*32), Phase: "setup"})
	}
	start := time.Unix(1700000000, 0)
	tasks := h.buildLoad(start)
	h.batches = append(h.batches, tasks...)
	if len(tasks) != 3 || tasks[2].Count != 1 || tasks[0].Phase != "warmup" || tasks[1].Phase != "measured" {
		t.Fatal(tasks)
	}
	if h.stateForTrace(32) != h.batches[1] || h.stateForTrace(33) != tasks[0] || h.stateForTrace(97) != tasks[2] {
		t.Fatal("ledger indexing")
	}
}

type fakeExporter struct {
	calls    int
	partial  bool
	requests []*collectorpb.ExportTraceServiceRequest
}

func (f *fakeExporter) Export(_ context.Context, r *collectorpb.ExportTraceServiceRequest, _ ...grpc.CallOption) (*collectorpb.ExportTraceServiceResponse, error) {
	f.calls++
	f.requests = append(f.requests, proto.Clone(r).(*collectorpb.ExportTraceServiceRequest))
	if f.partial {
		return &collectorpb.ExportTraceServiceResponse{PartialSuccess: &collectorpb.ExportTracePartialSuccess{RejectedSpans: 1}}, nil
	}
	if f.calls == 1 {
		return nil, status.Error(codes.Unavailable, "test transient")
	}
	return &collectorpb.ExportTraceServiceResponse{}, nil
}
func TestRetryAndPartialSuccess(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(map[bool]string{false: "retry", true: "partial"}[partial], func(t *testing.T) {
			c := testConfig(t)
			c.VisibilityEvery = 0
			rec, e := newRecorder(filepath.Join(t.TempDir(), "events"), 1<<20, func() {})
			if e != nil {
				t.Fatal(e)
			}
			defer rec.close()
			client := &fakeExporter{partial: partial}
			h := &Harness{config: c, rec: rec, client: client, ctx: context.Background()}
			b := &Batch{Count: 1, Scheduled: time.Now(), EventTime: time.Now().Add(-time.Second)}
			h.export(b)
			if partial {
				if client.calls != 1 || b.Acked || b.Rejected != 1 {
					t.Fatal("partial success retried/acked")
				}
			} else {
				if client.calls != 2 || !b.Acked || !proto.Equal(client.requests[0], client.requests[1]) {
					t.Fatal("retry failure")
				}
			}
		})
	}
}
func TestPercentilesAndRecorderLimit(t *testing.T) {
	p := percentiles([]int64{9, 1, 3, 2})
	if p.P50NS != 3 || p.P99NS != 9 || p.Count != 4 {
		t.Fatal(p)
	}
	cancelled := false
	r, e := newRecorder(filepath.Join(t.TempDir(), "events"), 1, func() { cancelled = true })
	if e != nil {
		t.Fatal(e)
	}
	if r.record(Event{Kind: "test"}) == nil || !cancelled {
		t.Fatal("cap not enforced")
	}
	_ = r.close()
}
