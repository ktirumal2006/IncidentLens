// Package trace validates OTLP input and flattens it into bounded storage rows.
package trace

import (
	"encoding/hex"
	"errors"
	"math"
	"strings"
	"time"

	collector "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	resource "go.opentelemetry.io/proto/otlp/resource/v1"
	oteltrace "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	MaxNormalizedBytes = 8 << 20
	MaxRequestBytes    = 4 << 20
	MaxSpans           = 2048
	MaxSpanBytes       = 64 << 10
	MaxAttributes      = 128
	MaxValueBytes      = 4096
	MaxDepth           = 8
	MaxEvents          = 128
	MaxLinks           = 128
	Retention          = 7 * 24 * time.Hour
	FutureTolerance    = 5 * time.Minute
)

var ErrRequestLimit = errors.New("request exceeds ingestion limit")

type Row struct {
	TraceID, SpanID, ParentSpanID                                 string
	ServiceName, ServiceNamespace, SpanName                       string
	SpanKind, StatusCode                                          uint8
	StatusMessage                                                 string
	StartTime, EndTime, IngestedAt                                time.Time
	DurationNS                                                    uint64
	ResourceAttributes, SpanAttributes, ScopeAttributes           string
	ScopeName, ScopeVersion, ResourceSchemaURL, ScopeSchemaURL    string
	Events, Links, TraceState                                     string
	TraceFlags                                                    uint32
	DroppedAttributesCount, DroppedEventsCount, DroppedLinksCount uint32
	ResourceDroppedAttributesCount, ScopeDroppedAttributesCount   uint32
}

// Normalize rejects whole requests exceeding request bounds. Invalid individual
// spans are counted; valid siblings remain eligible for durable insertion.
func Normalize(req *collector.ExportTraceServiceRequest, now time.Time) ([]Row, int64, error) {
	if proto.Size(req) > MaxRequestBytes {
		return nil, 0, ErrRequestLimit
	}
	count := 0
	for _, r := range req.GetResourceSpans() {
		for _, s := range r.GetScopeSpans() {
			count += len(s.GetSpans())
			if count > MaxSpans {
				return nil, 0, ErrRequestLimit
			}
		}
	}
	rows := make([]Row, 0, count)
	var rejected int64
	normalizedBytes := 0
	for _, r := range req.GetResourceSpans() {
		for _, scope := range r.GetScopeSpans() {
			for _, s := range scope.GetSpans() {
				row, ok := normalizeSpan(r, scope, s, now)
				if !ok {
					rejected++
					continue
				}
				normalizedBytes += row.sizeBytes()
				if normalizedBytes > MaxNormalizedBytes {
					return nil, 0, ErrRequestLimit
				}
				rows = append(rows, row)
			}
		}
	}
	return rows, rejected, nil
}

func normalizeSpan(r *oteltrace.ResourceSpans, sc *oteltrace.ScopeSpans, s *oteltrace.Span, now time.Time) (Row, bool) {
	var row Row
	if s == nil || proto.Size(s)+proto.Size(r.GetResource())+proto.Size(sc.GetScope())+len(r.SchemaUrl)+len(sc.SchemaUrl) > MaxSpanBytes || !validID(s.TraceId, 16) || !validID(s.SpanId, 8) || (len(s.ParentSpanId) > 0 && !validID(s.ParentSpanId, 8)) {
		return row, false
	}
	if s.Kind < 0 || s.Kind > 5 || s.GetStatus().GetCode() < 0 || s.GetStatus().GetCode() > 2 {
		return row, false
	}
	if s.StartTimeUnixNano == 0 || s.EndTimeUnixNano < s.StartTimeUnixNano || s.EndTimeUnixNano > math.MaxInt64 {
		return row, false
	}
	start := time.Unix(0, int64(s.StartTimeUnixNano)).UTC()
	end := time.Unix(0, int64(s.EndTimeUnixNano)).UTC()
	if start.Before(now.Add(-Retention)) || start.After(now.Add(FutureTolerance)) || end.After(now.Add(FutureTolerance)) {
		return row, false
	}
	for _, v := range []string{s.Name, s.TraceState, s.GetStatus().GetMessage(), r.SchemaUrl, sc.SchemaUrl, sc.GetScope().GetName(), sc.GetScope().GetVersion()} {
		if !validString(v) {
			return row, false
		}
	}
	ra, ok := attributes(r.GetResource().GetAttributes(), 0)
	if !ok {
		return row, false
	}
	sa, ok := attributes(s.Attributes, 0)
	if !ok {
		return row, false
	}
	ca, ok := attributes(sc.GetScope().GetAttributes(), 0)
	if !ok {
		return row, false
	}
	service, namespace := "", ""
	for _, a := range ra {
		if a.Key == "service.name" || a.Key == "service.namespace" {
			v, ok := a.Value.Value.(*common.AnyValue_StringValue)
			if !ok || len(v.StringValue) > 256 {
				return row, false
			}
			if a.Key == "service.name" {
				service = v.StringValue
			} else {
				namespace = v.StringValue
			}
		}
	}
	if strings.TrimSpace(service) == "" {
		return row, false
	}
	if len(s.Events) > MaxEvents || len(s.Links) > MaxLinks {
		return row, false
	}
	events := make([]*oteltrace.Span_Event, 0, len(s.Events))
	for _, e := range s.Events {
		if e == nil || e.TimeUnixNano == 0 || e.TimeUnixNano > math.MaxInt64 || !validString(e.Name) {
			return row, false
		}
		a, ok := attributes(e.Attributes, 0)
		if !ok {
			return row, false
		}
		ec := proto.Clone(e).(*oteltrace.Span_Event)
		ec.Attributes = a
		events = append(events, ec)
	}
	links := make([]*oteltrace.Span_Link, 0, len(s.Links))
	for _, l := range s.Links {
		if l == nil || !validID(l.TraceId, 16) || !validID(l.SpanId, 8) || !validString(l.TraceState) {
			return row, false
		}
		a, ok := attributes(l.Attributes, 0)
		if !ok {
			return row, false
		}
		lc := proto.Clone(l).(*oteltrace.Span_Link)
		lc.Attributes = a
		links = append(links, lc)
	}
	// Wrappers retain OTLP JSON types (including int64 strings and bytes base64).
	rj, e1 := protojson.Marshal(&resource.Resource{Attributes: ra})
	sj, e2 := protojson.Marshal(&resource.Resource{Attributes: sa})
	cj, e3 := protojson.Marshal(&resource.Resource{Attributes: ca})
	ej, e4 := protojson.Marshal(&oteltrace.Span{Events: events})
	lj, e5 := protojson.Marshal(&oteltrace.Span{Links: links})
	if e1 != nil || e2 != nil || e3 != nil || e4 != nil || e5 != nil {
		return row, false
	}
	row = Row{
		TraceID:                        hex.EncodeToString(s.TraceId),
		SpanID:                         hex.EncodeToString(s.SpanId),
		ParentSpanID:                   hex.EncodeToString(s.ParentSpanId),
		ServiceName:                    service,
		ServiceNamespace:               namespace,
		SpanName:                       s.Name,
		SpanKind:                       uint8(s.Kind),
		StatusCode:                     uint8(s.GetStatus().GetCode()),
		StatusMessage:                  s.GetStatus().GetMessage(),
		StartTime:                      start,
		EndTime:                        end,
		IngestedAt:                     now.UTC(),
		DurationNS:                     s.EndTimeUnixNano - s.StartTimeUnixNano,
		ResourceAttributes:             string(rj),
		SpanAttributes:                 string(sj),
		ScopeAttributes:                string(cj),
		ScopeName:                      sc.GetScope().GetName(),
		ScopeVersion:                   sc.GetScope().GetVersion(),
		ResourceSchemaURL:              r.SchemaUrl,
		ScopeSchemaURL:                 sc.SchemaUrl,
		Events:                         string(ej),
		Links:                          string(lj),
		TraceState:                     s.TraceState,
		TraceFlags:                     s.Flags,
		DroppedAttributesCount:         s.DroppedAttributesCount,
		DroppedEventsCount:             s.DroppedEventsCount,
		DroppedLinksCount:              s.DroppedLinksCount,
		ResourceDroppedAttributesCount: r.GetResource().GetDroppedAttributesCount(),
		ScopeDroppedAttributesCount:    sc.GetScope().GetDroppedAttributesCount(),
	}
	return row, true
}
func validID(id []byte, n int) bool {
	if len(id) != n {
		return false
	}
	for _, b := range id {
		if b != 0 {
			return true
		}
	}
	return false
}
func validString(s string) bool { return len(s) <= MaxValueBytes }
func allowed(key string) bool {
	switch key {
	case "service.name", "service.namespace", "service.version", "deployment.environment.name", "http.request.method", "http.response.status_code", "rpc.system", "rpc.service", "rpc.method", "server.address", "server.port", "error.type":
		return true
	}
	return strings.HasPrefix(key, "test.")
}
func attributes(in []*common.KeyValue, depth int) ([]*common.KeyValue, bool) {
	if depth > MaxDepth || len(in) > MaxAttributes {
		return nil, false
	}
	out := make([]*common.KeyValue, 0, len(in))
	seen := make(map[string]bool, len(in))
	for _, kv := range in {
		if kv == nil || !validString(kv.Key) || seen[kv.Key] {
			return nil, false
		}
		seen[kv.Key] = true
		v, ok := value(kv.Value, depth)
		if !ok {
			return nil, false
		}
		if allowed(kv.Key) {
			out = append(out, &common.KeyValue{Key: kv.Key, Value: v})
		}
	}
	return out, true
}
func value(v *common.AnyValue, depth int) (*common.AnyValue, bool) {
	if v == nil || v.Value == nil || depth > MaxDepth {
		return nil, false
	}
	switch x := v.Value.(type) {
	case *common.AnyValue_StringValue:
		if !validString(x.StringValue) {
			return nil, false
		}
	case *common.AnyValue_BytesValue:
		if len(x.BytesValue) > MaxValueBytes {
			return nil, false
		}
	case *common.AnyValue_ArrayValue:
		if x.ArrayValue == nil || len(x.ArrayValue.Values) > MaxAttributes {
			return nil, false
		}
		vs := make([]*common.AnyValue, 0, len(x.ArrayValue.Values))
		for _, a := range x.ArrayValue.Values {
			c, ok := value(a, depth+1)
			if !ok {
				return nil, false
			}
			vs = append(vs, c)
		}
		return &common.AnyValue{Value: &common.AnyValue_ArrayValue{ArrayValue: &common.ArrayValue{Values: vs}}}, true
	case *common.AnyValue_KvlistValue:
		if x.KvlistValue == nil {
			return nil, false
		}
		a, ok := attributes(x.KvlistValue.Values, depth+1)
		if !ok {
			return nil, false
		}
		return &common.AnyValue{Value: &common.AnyValue_KvlistValue{KvlistValue: &common.KeyValueList{Values: a}}}, true
	}
	return proto.Clone(v).(*common.AnyValue), true
}

// sizeBytes accounts for persisted string payloads plus 512 bytes of row metadata.
// It is a normalization budget, not an exact Go heap allocation measurement.
func (r Row) sizeBytes() int {
	size := 512
	for _, s := range []string{r.TraceID, r.SpanID, r.ParentSpanID, r.ServiceName,
		r.ServiceNamespace, r.SpanName, r.StatusMessage, r.ResourceAttributes,
		r.SpanAttributes, r.ScopeAttributes, r.ScopeName, r.ScopeVersion,
		r.ResourceSchemaURL, r.ScopeSchemaURL, r.Events, r.Links, r.TraceState} {
		size += len(s)
	}
	return size
}
