package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"strings"
	"time"

	collectorpb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

func traceBytes(seed string, index int) []byte {
	h := sha256.Sum256([]byte(seed + ":" + hexIndex(index)))
	return append([]byte{}, h[:16]...)
}
func hexIndex(index int) string {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(index))
	return hex.EncodeToString(b[:])
}
func spanBytes(index int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(index+1))
	return b
}
func traceHex(seed string, index int) string { return hex.EncodeToString(traceBytes(seed, index)) }
func strAttr(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
}
func makeRequest(c Config, first, count int, eventTime time.Time, setup bool) *collectorpb.ExportTraceServiceRequest {
	req := &collectorpb.ExportTraceServiceRequest{}
	for serviceIndex, service := range c.Services {
		rs := &tracepb.ResourceSpans{Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{strAttr("service.name", service), strAttr("service.namespace", c.Namespace), strAttr("service.version", "bench-v1"), strAttr("deployment.environment.name", "local-benchmark")}}, ScopeSpans: []*tracepb.ScopeSpans{{Scope: &commonpb.InstrumentationScope{Name: "incidentlens.bench", Version: "1"}}}}
		for traceIndex := first; traceIndex < first+count; traceIndex++ {
			for local := 0; local < 2; local++ {
				i := serviceIndex*2 + local
				kind := tracepb.Span_SPAN_KIND_SERVER
				duration := time.Duration(300-i*20) * time.Millisecond
				name := "Request"
				if local == 1 {
					kind = tracepb.Span_SPAN_KIND_CLIENT
					name = "CallNext"
				}
				if i == 5 {
					kind = tracepb.Span_SPAN_KIND_INTERNAL
					name = "Work"
				}
				if setup {
					duration = time.Duration(10-i) * time.Millisecond
				}
				status := tracepb.Status_STATUS_CODE_OK
				if !setup && (traceIndex-c.BaselineTraces)%20 == 0 {
					status = tracepb.Status_STATUS_CODE_ERROR
				}
				offset := time.Duration(i) * time.Millisecond
				if setup {
					offset = time.Duration(i) * 100 * time.Microsecond
				}
				parent := []byte(nil)
				if i > 0 {
					parent = spanBytes(i - 1)
				}
				span := &tracepb.Span{TraceId: traceBytes(c.Seed, traceIndex), SpanId: spanBytes(i), ParentSpanId: parent, Name: name, Kind: kind, StartTimeUnixNano: uint64(eventTime.Add(offset).UnixNano()), EndTimeUnixNano: uint64(eventTime.Add(offset + duration).UnixNano()), Flags: 1, Status: &tracepb.Status{Code: status}, Attributes: []*commonpb.KeyValue{strAttr("test.payload", strings.Repeat("x", c.AttributeBytes)), {Key: "test.sequence", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: int64(traceIndex)}}}, {Key: "test.sampled", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: true}}}, {Key: "test.ratio", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: 1.25}}}}}
				rs.ScopeSpans[0].Spans = append(rs.ScopeSpans[0].Spans, span)
			}
		}
		req.ResourceSpans = append(req.ResourceSpans, rs)
	}
	return req
}
