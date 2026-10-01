package stream

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	collector "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/status"
	"incidentlens/backend/internal/trace"
)

const PublishTimeout = 5 * time.Second

type Publisher interface {
	Publish(context.Context, []byte) error
}

type KafkaPublisher struct {
	Client *kgo.Client
	Topic  string
}

func NewProducer(brokers []string, topic string) (*KafkaPublisher, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...), kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// A caller's finite deadline must also bound an in-flight record.
		// Cancellation can leave an ambiguous broker write; OTLP retries and
		// duplicate-safe FINAL reads handle that outcome.
		kgo.AllowIdempotentProduceCancellation(),
		kgo.MaxBufferedRecords(4), kgo.MaxBufferedBytes(16<<20),
		kgo.ProducerBatchMaxBytes(5<<20), kgo.RecordDeliveryTimeout(PublishTimeout),
	)
	if err != nil {
		return nil, err
	}
	return &KafkaPublisher{Client: client, Topic: topic}, nil
}

func (p *KafkaPublisher) Close()                         { p.Client.Close() }
func (p *KafkaPublisher) Ping(ctx context.Context) error { return CheckTopic(ctx, p.Client, p.Topic) }
func (p *KafkaPublisher) Publish(ctx context.Context, b []byte) error {
	results := p.Client.ProduceSync(ctx, &kgo.Record{Topic: p.Topic, Value: b})
	return results.FirstErr()
}

type Server struct {
	collector.UnimplementedTraceServiceServer
	publisher Publisher
	slots     chan struct{}
	now       func() time.Time
}

func NewServer(p Publisher) *Server {
	return &Server{publisher: p, slots: make(chan struct{}, 4), now: time.Now}
}

func NewGRPCServer(p Publisher) *grpc.Server {
	g := grpc.NewServer(grpc.MaxRecvMsgSize(trace.MaxRequestBytes))
	collector.RegisterTraceServiceServer(g, NewServer(p))
	return g
}

func (s *Server) Export(ctx context.Context, req *collector.ExportTraceServiceRequest) (*collector.ExportTraceServiceResponse, error) {
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		return nil, status.Error(codes.Unavailable, "stream producer capacity exhausted")
	}
	ctx, cancel := context.WithTimeout(ctx, PublishTimeout)
	defer cancel()
	rows, rejected, err := trace.Normalize(req, s.now())
	if err != nil {
		return nil, status.Error(codes.ResourceExhausted, "request exceeds ingestion limit")
	}
	// Preflight the complete request: an oversized later chunk must not leave an
	// earlier accepted chunk behind under a permanent-rejection response.
	records := make([][]byte, 0, (len(rows)+MaxRows-1)/MaxRows)
	for i := 0; i < len(rows); i += MaxRows {
		end := i + MaxRows
		if end > len(rows) {
			end = len(rows)
		}
		b, err := EncodeBatch(rows[i:end])
		if err != nil {
			return nil, status.Error(codes.ResourceExhausted, "normalized stream record exceeds limit")
		}
		records = append(records, b)
	}
	for _, b := range records {
		if err := s.publisher.Publish(ctx, b); err != nil {
			return nil, status.Error(codes.Unavailable, fmt.Sprintf("broker acknowledgement failed; retry may duplicate spans: %v", err))
		}
		if ctx.Err() != nil {
			return nil, status.Error(codes.Unavailable, "broker acknowledgement deadline exceeded")
		}
	}
	response := &collector.ExportTraceServiceResponse{}
	if rejected > 0 {
		response.PartialSuccess = &collector.ExportTracePartialSuccess{RejectedSpans: rejected, ErrorMessage: "spans rejected by validation or payload policy"}
	}
	return response, nil
}
