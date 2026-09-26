package trace

import (
	"bytes"
	"fmt"
	collector "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	resource "go.opentelemetry.io/proto/otlp/resource/v1"
	oteltrace "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"strings"
	"testing"
	"time"
)

var testNow = time.Date(2026, 9, 24, 12, 0, 0, 123, time.UTC)

func fixture() *collector.ExportTraceServiceRequest {
	return &collector.ExportTraceServiceRequest{ResourceSpans: []*oteltrace.ResourceSpans{{Resource: &resource.Resource{Attributes: []*common.KeyValue{{Key: "service.name", Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: "test-service"}}}}}, ScopeSpans: []*oteltrace.ScopeSpans{{Spans: []*oteltrace.Span{{TraceId: bytes.Repeat([]byte{1}, 16), SpanId: bytes.Repeat([]byte{2}, 8), StartTimeUnixNano: uint64(testNow.Add(-time.Second).UnixNano()), EndTimeUnixNano: uint64(testNow.UnixNano())}}}}}}}
}
func span(r *collector.ExportTraceServiceRequest) *oteltrace.Span {
	return r.ResourceSpans[0].ScopeSpans[0].Spans[0]
}
func TestValidation(t *testing.T) {
	tests := []struct {
		name   string
		change func(*collector.ExportTraceServiceRequest)
		valid  bool
	}{
		{"valid", func(r *collector.ExportTraceServiceRequest) {}, true},
		{"zero trace", func(r *collector.ExportTraceServiceRequest) { span(r).TraceId = make([]byte, 16) }, false},
		{"short span", func(r *collector.ExportTraceServiceRequest) { span(r).SpanId = []byte{1} }, false},
		{"zero parent", func(r *collector.ExportTraceServiceRequest) { span(r).ParentSpanId = make([]byte, 8) }, false},
		{"missing service", func(r *collector.ExportTraceServiceRequest) { r.ResourceSpans[0].Resource = nil }, false},
		{"reverse time", func(r *collector.ExportTraceServiceRequest) { span(r).EndTimeUnixNano = 1 }, false},
		{"retention edge", func(r *collector.ExportTraceServiceRequest) {
			span(r).StartTimeUnixNano = uint64(testNow.Add(-Retention).UnixNano())
		}, true},
		{"expired", func(r *collector.ExportTraceServiceRequest) {
			span(r).StartTimeUnixNano = uint64(testNow.Add(-Retention - time.Nanosecond).UnixNano())
		}, false},
		{"future edge", func(r *collector.ExportTraceServiceRequest) {
			span(r).EndTimeUnixNano = uint64(testNow.Add(FutureTolerance).UnixNano())
		}, true},
		{"future end", func(r *collector.ExportTraceServiceRequest) {
			span(r).EndTimeUnixNano = uint64(testNow.Add(FutureTolerance + time.Nanosecond).UnixNano())
		}, false},
		{"invalid enum", func(r *collector.ExportTraceServiceRequest) { span(r).Kind = 6 }, false},
		{"string edge", func(r *collector.ExportTraceServiceRequest) { span(r).Name = strings.Repeat("x", MaxValueBytes) }, true},
		{"long string", func(r *collector.ExportTraceServiceRequest) { span(r).Name = strings.Repeat("x", MaxValueBytes+1) }, false},
		{"duplicate keys", func(r *collector.ExportTraceServiceRequest) {
			a := r.ResourceSpans[0].Resource.Attributes[0]
			r.ResourceSpans[0].Resource.Attributes = append(r.ResourceSpans[0].Resource.Attributes, a)
		}, false},
		{"invalid link", func(r *collector.ExportTraceServiceRequest) { span(r).Links = []*oteltrace.Span_Link{{}} }, false},
		{"nil event", func(r *collector.ExportTraceServiceRequest) { span(r).Events = []*oteltrace.Span_Event{nil} }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := fixture()
			tt.change(r)
			rows, rejected, err := Normalize(r, testNow)
			if err != nil {
				t.Fatal(err)
			}
			if (len(rows) == 1) != tt.valid || (rejected == 0) != tt.valid {
				t.Fatalf("rows=%d rejected=%d", len(rows), rejected)
			}
		})
	}
}
func TestRequestLimits(t *testing.T) {
	r := fixture()
	sc := r.ResourceSpans[0].ScopeSpans[0]
	s := span(r)
	sc.Spans = nil
	for i := 0; i < MaxSpans; i++ {
		sc.Spans = append(sc.Spans, s)
	}
	if rows, _, err := Normalize(r, testNow); err != nil || len(rows) != MaxSpans {
		t.Fatalf("boundary: %d %v", len(rows), err)
	}
	sc.Spans = append(sc.Spans, s)
	if _, _, err := Normalize(r, testNow); err != ErrRequestLimit {
		t.Fatal(err)
	}
	r = fixture()
	span(r).Name = strings.Repeat("x", MaxRequestBytes)
	if _, _, err := Normalize(r, testNow); err != ErrRequestLimit {
		t.Fatal(err)
	}
}
func TestTypedRoundtripAndFiltering(t *testing.T) {
	r := fixture()
	s := span(r)
	s.Attributes = []*common.KeyValue{
		{Key: "test.int", Value: &common.AnyValue{Value: &common.AnyValue_IntValue{IntValue: 1 << 60}}},
		{Key: "test.bytes", Value: &common.AnyValue{Value: &common.AnyValue_BytesValue{BytesValue: []byte{0, 255}}}},
		{Key: "test.bool", Value: &common.AnyValue{Value: &common.AnyValue_BoolValue{BoolValue: true}}},
		{Key: "test.double", Value: &common.AnyValue{Value: &common.AnyValue_DoubleValue{DoubleValue: 1.5}}},
		{Key: "test.array", Value: &common.AnyValue{Value: &common.AnyValue_ArrayValue{ArrayValue: &common.ArrayValue{Values: []*common.AnyValue{{Value: &common.AnyValue_StringValue{StringValue: "a"}}}}}}},
		{Key: "authorization", Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: "secret"}}},
	}
	rows, _, err := Normalize(r, testNow)
	if err != nil || len(rows) != 1 {
		t.Fatal(err)
	}
	var got resource.Resource
	if err := protojson.Unmarshal([]byte(rows[0].SpanAttributes), &got); err != nil {
		t.Fatal(err)
	}
	want := &resource.Resource{Attributes: s.Attributes[:5]}
	if !proto.Equal(&got, want) {
		t.Fatalf("got %s", rows[0].SpanAttributes)
	}
	if rows[0].StartTime.UnixNano() != int64(s.StartTimeUnixNano) || rows[0].DurationNS != 1e9 {
		t.Fatal("nanoseconds lost")
	}
}
func TestAttributeBounds(t *testing.T) {
	sv := func(s string) *common.AnyValue {
		return &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: s}}
	}
	a := []*common.KeyValue{{Key: "test.value", Value: sv(strings.Repeat("a", MaxValueBytes))}}
	if _, ok := attributes(a, 0); !ok {
		t.Fatal("exact value limit")
	}
	a[0].Value = sv(strings.Repeat("a", MaxValueBytes+1))
	if _, ok := attributes(a, 0); ok {
		t.Fatal("oversized accepted")
	}
	nested := sv("ok")
	for i := 0; i < MaxDepth; i++ {
		nested = &common.AnyValue{Value: &common.AnyValue_ArrayValue{ArrayValue: &common.ArrayValue{Values: []*common.AnyValue{nested}}}}
	}
	if _, ok := value(nested, 0); !ok {
		t.Fatal("depth edge")
	}
	nested = &common.AnyValue{Value: &common.AnyValue_ArrayValue{ArrayValue: &common.ArrayValue{Values: []*common.AnyValue{nested}}}}
	if _, ok := value(nested, 0); ok {
		t.Fatal("depth overflow")
	}
	for _, n := range []int{MaxEvents, MaxEvents + 1} {
		r := fixture()
		for i := 0; i < n; i++ {
			span(r).Events = append(span(r).Events, &oteltrace.Span_Event{TimeUnixNano: uint64(testNow.UnixNano())})
		}
		rows, _, _ := Normalize(r, testNow)
		if (len(rows) == 1) != (n == MaxEvents) {
			t.Fatalf("events %d", n)
		}
	}
}

func TestSharedContextBudgets(t *testing.T) {
	r := fixture()
	for i := 0; i < 20; i++ {
		r.ResourceSpans[0].Resource.Attributes = append(r.ResourceSpans[0].Resource.Attributes, &common.KeyValue{Key: "test." + strings.Repeat("k", i+1), Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: strings.Repeat("x", MaxValueBytes)}}})
	}
	if rows, rejected, err := Normalize(r, testNow); err != nil || len(rows) != 0 || rejected != 1 {
		t.Fatalf("row context bound: %d %d %v", len(rows), rejected, err)
	}
	r = fixture()
	r.ResourceSpans[0].Resource.Attributes = append(r.ResourceSpans[0].Resource.Attributes, &common.KeyValue{Key: "test.shared", Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: strings.Repeat("x", MaxValueBytes)}}})
	s := span(r)
	for i := 1; i < MaxSpans; i++ {
		r.ResourceSpans[0].ScopeSpans[0].Spans = append(r.ResourceSpans[0].ScopeSpans[0].Spans, s)
	}
	if proto.Size(r) > MaxRequestBytes {
		t.Fatal("fixture must fit wire budget")
	}
	if _, _, err := Normalize(r, testNow); err != ErrRequestLimit {
		t.Fatalf("normalized amplification bound: %v", err)
	}
}

func TestCollectionLimits(t *testing.T) {
	for _, n := range []int{MaxAttributes, MaxAttributes + 1} {
		r := fixture()
		for i := 0; i < n; i++ {
			span(r).Attributes = append(span(r).Attributes, &common.KeyValue{Key: fmt.Sprintf("test.%d", i), Value: &common.AnyValue{Value: &common.AnyValue_BoolValue{BoolValue: true}}})
		}
		rows, _, err := Normalize(r, testNow)
		if err != nil || (len(rows) == 1) != (n == MaxAttributes) {
			t.Fatalf("attributes %d: %v", n, err)
		}
	}
	for _, n := range []int{MaxLinks, MaxLinks + 1} {
		r := fixture()
		for i := 0; i < n; i++ {
			span(r).Links = append(span(r).Links, &oteltrace.Span_Link{TraceId: bytes.Repeat([]byte{3}, 16), SpanId: bytes.Repeat([]byte{4}, 8)})
		}
		rows, _, err := Normalize(r, testNow)
		if err != nil || (len(rows) == 1) != (n == MaxLinks) {
			t.Fatalf("links %d: %v", n, err)
		}
	}
}

func TestNestedSensitiveKeysRemoved(t *testing.T) {
	r := fixture()
	span(r).Attributes = []*common.KeyValue{{Key: "test.object", Value: &common.AnyValue{Value: &common.AnyValue_KvlistValue{KvlistValue: &common.KeyValueList{Values: []*common.KeyValue{
		{Key: "password", Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: "sensitive"}}},
		{Key: "test.safe", Value: &common.AnyValue{Value: &common.AnyValue_BoolValue{BoolValue: true}}},
	}}}}}}
	rows, _, err := Normalize(r, testNow)
	if err != nil || len(rows) != 1 || strings.Contains(rows[0].SpanAttributes, "sensitive") || !strings.Contains(rows[0].SpanAttributes, "test.safe") {
		t.Fatalf("%v %v", rows, err)
	}
}
