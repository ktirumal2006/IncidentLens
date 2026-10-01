package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	collector "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	resource "go.opentelemetry.io/proto/otlp/resource/v1"
	ottrace "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"incidentlens/backend/internal/trace"
)

func row(now time.Time) trace.Row {
	return trace.Row{
		TraceID: "11111111111111111111111111111111", SpanID: "2222222222222222", ServiceName: "svc", SpanName: "op",
		StartTime: now.Add(-time.Minute), EndTime: now.Add(-time.Minute).Add(time.Nanosecond), IngestedAt: now,
		DurationNS:         1,
		ResourceAttributes: "{}", SpanAttributes: "{}", ScopeAttributes: "{}", Events: "{}", Links: "{}",
	}
}

func TestEnvelopeRoundTripAndPoison(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	b, err := EncodeBatch([]trace.Row{row(now)})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := Decode(b, now)
	if err != nil || len(rows) != 1 || !rows[0].IngestedAt.Equal(now) {
		t.Fatalf("rows=%v err=%v", rows, err)
	}
	for name, payload := range map[string][]byte{
		"version":  []byte(`{"version":2,"rows":[{}]}`),
		"unknown":  []byte(`{"version":1,"rows":[{}],"extra":1}`),
		"trailing": append(append([]byte(nil), b...), []byte(` {}`)...),
		"oversize": make([]byte, MaxRecordBytes+1),
	} {
		if _, err := Decode(payload, now); err == nil {
			t.Fatal(name)
		}
	}
	if _, err := Decode(b, now.Add(trace.Retention+time.Minute)); err == nil {
		t.Fatal("expired row accepted")
	}
	bad := row(now)
	bad.DurationNS = 2
	b, _ = EncodeBatch([]trace.Row{bad})
	if _, err := Decode(b, now); err == nil {
		t.Fatal("duration mismatch accepted")
	}
	for name, change := range map[string]func(*trace.Row){
		"zero trace ID":         func(r *trace.Row) { r.TraceID = strings.Repeat("0", 32) },
		"zero span ID":          func(r *trace.Row) { r.SpanID = strings.Repeat("0", 16) },
		"zero parent ID":        func(r *trace.Row) { r.ParentSpanID = strings.Repeat("0", 16) },
		"invalid attributes":    func(r *trace.Row) { r.SpanAttributes = "{" },
		"wrong attributes type": func(r *trace.Row) { r.SpanAttributes = `{"attributes":"invalid"}` },
		"null attributes":       func(r *trace.Row) { r.ResourceAttributes = "null" },
		"future receipt":        func(r *trace.Row) { r.IngestedAt = now.Add(trace.FutureTolerance + time.Second) },
		"future end":            func(r *trace.Row) { r.EndTime = now.Add(trace.FutureTolerance + time.Second) },
	} {
		invalid := row(now)
		change(&invalid)
		b, _ := EncodeBatch([]trace.Row{invalid})
		if _, err := Decode(b, now); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	if _, err := EncodeBatch(make([]trace.Row, MaxRows+1)); err == nil {
		t.Fatal("too many rows accepted")
	}
	big := row(now)
	big.SpanAttributes = strings.Repeat("x", MaxRecordBytes)
	if _, err := EncodeBatch([]trace.Row{big}); err == nil {
		t.Fatal("oversize accepted")
	}
}

type fakePublisher struct {
	payloads [][]byte
	err      error
}

func (f *fakePublisher) Publish(_ context.Context, b []byte) error {
	f.payloads = append(f.payloads, b)
	return f.err
}

func request(now time.Time, valid, invalid int) *collector.ExportTraceServiceRequest {
	spans := make([]*ottrace.Span, 0, valid+invalid)
	for i := 0; i < valid+invalid; i++ {
		span := &ottrace.Span{TraceId: []byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, SpanId: []byte{2, 2, 2, 2, 2, 2, 2, byte(i + 1)}, Name: "op", StartTimeUnixNano: uint64(now.Add(-time.Minute).UnixNano()), EndTimeUnixNano: uint64(now.Add(-time.Minute).Add(time.Nanosecond).UnixNano())}
		if i >= valid {
			span.SpanId = nil
		}
		spans = append(spans, span)
	}
	return &collector.ExportTraceServiceRequest{ResourceSpans: []*ottrace.ResourceSpans{{Resource: &resource.Resource{Attributes: []*common.KeyValue{{Key: "service.name", Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: "svc"}}}}}, ScopeSpans: []*ottrace.ScopeSpans{{Spans: spans}}}}}
}

func TestExportAcknowledgementAndPartialSuccess(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	pub := &fakePublisher{}
	s := NewServer(pub)
	s.now = func() time.Time { return now }
	response, err := s.Export(context.Background(), request(now, 1, 1))
	if err != nil || response.GetPartialSuccess().GetRejectedSpans() != 1 || len(pub.payloads) != 1 {
		t.Fatalf("response=%v err=%v published=%d", response, err, len(pub.payloads))
	}
	rows, err := Decode(pub.payloads[0], now)
	if err != nil || len(rows) != 1 {
		t.Fatalf("decode=%v rows=%d", err, len(rows))
	}
	pub.err = errors.New("broker down")
	if _, err := s.Export(context.Background(), request(now, 1, 0)); status.Code(err) != codes.Unavailable {
		t.Fatalf("want unavailable, got %v", err)
	}
	if _, err := s.Export(context.Background(), request(now, trace.MaxSpans+1, 0)); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("want resource exhausted, got %v", err)
	}
}

func TestFullRequestRecordPreflight(t *testing.T) {
	now := time.Now().UTC()
	req := request(now, 512, 0)
	all := req.ResourceSpans[0].ScopeSpans[0].Spans
	largeScope := &common.InstrumentationScope{}
	for _, key := range []string{"test.a", "test.b", "test.c", "test.d", "test.e", "test.f"} {
		largeScope.Attributes = append(largeScope.Attributes, &common.KeyValue{Key: key, Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: strings.Repeat("x", 4096)}}})
	}
	req.ResourceSpans[0].ScopeSpans = []*ottrace.ScopeSpans{{Spans: all[:256]}, {Scope: largeScope, Spans: all[256:]}}
	rows, rejected, err := trace.Normalize(req, now)
	if err != nil || rejected != 0 || len(rows) != 512 {
		t.Fatalf("fixture invalid: rows=%d rejected=%d err=%v", len(rows), rejected, err)
	}
	if _, err = EncodeBatch(rows[:256]); err != nil {
		t.Fatal(err)
	}
	if _, err = EncodeBatch(rows[256:]); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("fixture must exceed second record cap: %v", err)
	}
	pub := &fakePublisher{}
	server := NewServer(pub)
	server.now = func() time.Time { return now }
	if _, err = server.Export(context.Background(), req); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("expected permanent limit rejection: %v", err)
	}
	if len(pub.payloads) != 0 {
		t.Fatal("published first chunk before full request preflight")
	}
}

func TestMalformedRowInvariants(t *testing.T) {
	now := time.Now().UTC()
	for name, mutate := range map[string]func(*trace.Row){
		"zero trace":       func(r *trace.Row) { r.TraceID = strings.Repeat("0", 32) },
		"zero span":        func(r *trace.Row) { r.SpanID = strings.Repeat("0", 16) },
		"attribute JSON":   func(r *trace.Row) { r.SpanAttributes = "{" },
		"end before start": func(r *trace.Row) { r.EndTime = r.StartTime.Add(-time.Second) },
	} {
		t.Run(name, func(t *testing.T) {
			r := row(now)
			mutate(&r)
			b, err := EncodeBatch([]trace.Row{r})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = Decode(b, now); err == nil {
				t.Fatal("malformed row accepted")
			}
		})
	}
}

func TestEnvelopeJSONVersionField(t *testing.T) {
	b, err := EncodeBatch([]trace.Row{row(time.Now())})
	if err != nil {
		t.Fatal(err)
	}
	var v map[string]json.RawMessage
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	if string(v["version"]) != "1" {
		t.Fatalf("version=%s", v["version"])
	}
}

func TestRecoverableFetchErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"broker unavailable", fmt.Errorf("fetch: %w", kerr.BrokerNotAvailable), true},
		{"network timeout", &net.DNSError{IsTimeout: true}, true},
		{"poll deadline", context.DeadlineExceeded, true},
		{"group session", &kgo.ErrGroupSession{Err: context.DeadlineExceeded}, true},
		{"unknown member session", fmt.Errorf("fetch: %w", &kgo.ErrGroupSession{Err: kerr.UnknownMemberID}), true},
		{"illegal generation", kerr.IllegalGeneration, true},
		{"rebalance in progress", kerr.RebalanceInProgress, true},
		{"group authorization", &kgo.ErrGroupSession{Err: kerr.GroupAuthorizationFailed}, false},
		{"offset gap", kerr.OffsetOutOfRange, false},
		{"authorization", kerr.TopicAuthorizationFailed, false},
		{"data loss", &kgo.ErrDataLoss{}, false},
		{"decompression cap", &kgo.ErrDecompressTooLarge{Topic: Topic, Offset: 7}, false},
		{"closed", kgo.ErrClientClosed, false},
		{"malformed", errors.New("bad record"), false},
	} {
		if got := recoverableFetch(tc.err); got != tc.want {
			t.Errorf("%s: got %v", tc.name, got)
		}
	}
}

func TestMembershipLossIsFatalDuringCommit(t *testing.T) {
	w := &Worker{}
	for _, membershipErr := range []error{kerr.UnknownMemberID, kerr.IllegalGeneration, kerr.RebalanceInProgress} {
		attempts := 0
		err := w.retry(context.Background(), true, func(context.Context) error {
			attempts++
			return membershipErr
		})
		if !errors.Is(err, membershipErr) || attempts != 1 {
			t.Fatalf("commit err=%v attempts=%d", err, attempts)
		}
	}
}

func TestCommitRetryFailsClosedOnPermanentError(t *testing.T) {
	w := &Worker{}
	attempts := 0
	err := w.retry(context.Background(), true, func(context.Context) error { attempts++; return kerr.GroupAuthorizationFailed })
	if !errors.Is(err, kerr.GroupAuthorizationFailed) || attempts != 1 {
		t.Fatalf("err=%v attempts=%d", err, attempts)
	}
}

func TestWorkerBoundedFetchOptionsAreAccepted(t *testing.T) {
	w, err := NewWorker([]string{"127.0.0.1:1"}, Topic, "stream-unit-fetch-bounds", nil)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()
}
