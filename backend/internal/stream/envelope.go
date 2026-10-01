package stream

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	resource "go.opentelemetry.io/proto/otlp/resource/v1"
	ottrace "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"incidentlens/backend/internal/trace"
)

const (
	Topic          = "incidentlens-spans-v1"
	Version        = 1
	MaxRecordBytes = 4 << 20
	MaxRows        = 256
)

var (
	ErrInvalidRecord = errors.New("invalid stream record")
	traceIDPattern   = regexp.MustCompile(`^[0-9a-f]{32}$`)
	spanIDPattern    = regexp.MustCompile(`^[0-9a-f]{16}$`)
)

type Envelope struct {
	Version int         `json:"version"`
	Rows    []trace.Row `json:"rows"`
}

// EncodeBatch checks every record's wire size before any batch is published.
func EncodeBatch(rows []trace.Row) ([]byte, error) {
	if len(rows) == 0 || len(rows) > MaxRows {
		return nil, fmt.Errorf("%w: row count", ErrInvalidRecord)
	}
	b, err := json.Marshal(Envelope{Version: Version, Rows: rows})
	if err != nil || len(b) > MaxRecordBytes {
		return nil, fmt.Errorf("%w: record exceeds %d bytes", ErrInvalidRecord, MaxRecordBytes)
	}
	return b, nil
}

// Decode verifies the schema and storage invariants before a consumer writes.
// Replays retain the producer's IngestedAt; a new receipt time would change
// cutoff-based query semantics and undermine duplicate-safe reads.
func Decode(b []byte, now time.Time) ([]trace.Row, error) {
	if len(b) == 0 || len(b) > MaxRecordBytes {
		return nil, fmt.Errorf("%w: size", ErrInvalidRecord)
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	var envelope Envelope
	if err := d.Decode(&envelope); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidRecord, err)
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("%w: trailing data", ErrInvalidRecord)
	}
	if envelope.Version != Version || len(envelope.Rows) == 0 || len(envelope.Rows) > MaxRows {
		return nil, fmt.Errorf("%w: version or row count", ErrInvalidRecord)
	}
	for _, r := range envelope.Rows {
		if !traceIDPattern.MatchString(r.TraceID) || !spanIDPattern.MatchString(r.SpanID) ||
			r.TraceID == strings.Repeat("0", 32) || r.SpanID == strings.Repeat("0", 16) ||
			(r.ParentSpanID != "" && (!spanIDPattern.MatchString(r.ParentSpanID) || r.ParentSpanID == strings.Repeat("0", 16))) ||
			strings.TrimSpace(r.ServiceName) == "" || len(r.ServiceName) > 256 || len(r.ServiceNamespace) > 256 ||
			r.StartTime.IsZero() || r.EndTime.IsZero() || r.EndTime.Before(r.StartTime) ||
			r.StartTime.Before(now.Add(-trace.Retention)) || r.StartTime.After(now.Add(trace.FutureTolerance)) ||
			r.EndTime.After(now.Add(trace.FutureTolerance)) || r.IngestedAt.IsZero() || r.IngestedAt.After(now.Add(trace.FutureTolerance)) ||
			r.DurationNS != uint64(r.EndTime.Sub(r.StartTime)) || r.SpanKind > 5 || r.StatusCode > 2 ||
			len(r.SpanName) > trace.MaxValueBytes || len(r.StatusMessage) > trace.MaxValueBytes ||
			len(r.ScopeName) > trace.MaxValueBytes || len(r.ScopeVersion) > trace.MaxValueBytes ||
			len(r.ResourceSchemaURL) > trace.MaxValueBytes || len(r.ScopeSchemaURL) > trace.MaxValueBytes ||
			len(r.TraceState) > trace.MaxValueBytes ||
			!validStoredJSON(r.ResourceAttributes, &resource.Resource{}) || !validStoredJSON(r.SpanAttributes, &resource.Resource{}) ||
			!validStoredJSON(r.ScopeAttributes, &resource.Resource{}) || !validStoredJSON(r.Events, &ottrace.Span{}) ||
			!validStoredJSON(r.Links, &ottrace.Span{}) {
			return nil, fmt.Errorf("%w: malformed or expired row", ErrInvalidRecord)
		}
	}
	return envelope.Rows, nil
}

func validStoredJSON(s string, message proto.Message) bool {
	if len(s) == 0 || s[0] != '{' {
		return false
	}
	return protojson.Unmarshal([]byte(s), message) == nil
}
