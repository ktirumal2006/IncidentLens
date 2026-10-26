package stream

import (
	"context"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"incidentlens/backend/internal/benchobs"
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

type Publication struct {
	Partition int32
	Rows      int
	Payload   []byte
}
type BatchPublisher interface {
	PublishBatch(context.Context, []Publication) error
}

type KafkaPublisher struct {
	Client *kgo.Client
	Topic  string
}

type partitionContextKey struct{}

// PartitionForTraceID is the fixed v2 routing function. It hashes the 16
// decoded trace-ID bytes, not the printable hex representation.
func PartitionForTraceID(traceID string) (int32, error) {
	if !traceIDPattern.MatchString(traceID) {
		return 0, fmt.Errorf("invalid trace ID for partition routing")
	}
	b, _ := hex.DecodeString(traceID)
	h := fnv.New32a()
	_, _ = h.Write(b)
	return int32(h.Sum32() % PartitionCount), nil
}

// PublishPartition is metadata for a preflighted record. The interface stays
// small so test publishers can verify routing without a Kafka dependency.
func PublishPartition(ctx context.Context) (int32, bool) {
	p, ok := ctx.Value(partitionContextKey{}).(int32)
	return p, ok
}

func NewProducer(brokers []string, topic string) (*KafkaPublisher, error) {
	if ExpectedPartitions(topic) == 0 {
		return nil, fmt.Errorf("unsupported stream topic %q", topic)
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...), kgo.DefaultProduceTopic(topic),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
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
	started := benchobs.Start()
	partition, ok := PublishPartition(ctx)
	if !ok || partition < 0 || int(partition) >= ExpectedPartitions(p.Topic) {
		return fmt.Errorf("missing or invalid stream partition")
	}
	record := &kgo.Record{Topic: p.Topic, Partition: partition, Value: b}
	results := p.Client.ProduceSync(ctx, record)
	err := results.FirstErr()
	benchobs.Finish(started, benchobs.Event{Kind: "kafka_produce", Rows: benchobs.Rows(ctx), Bytes: len(b), Success: err == nil, Offset: record.Offset, Partition: partition})
	return err
}

// PublishBatch submits all preflighted partition records before waiting for
// broker acknowledgments. A partial failure is ambiguous and the caller must
// replay the entire OTLP export with immutable identities.
func (p *KafkaPublisher) PublishBatch(ctx context.Context, publications []Publication) error {
	records := make([]*kgo.Record, 0, len(publications))
	rows, bytes := 0, 0
	for _, item := range publications {
		if item.Partition < 0 || int(item.Partition) >= ExpectedPartitions(p.Topic) || len(item.Payload) > MaxRecordBytes {
			return fmt.Errorf("invalid partition publication")
		}
		records = append(records, &kgo.Record{Topic: p.Topic, Partition: item.Partition, Value: item.Payload})
		rows += item.Rows
		bytes += len(item.Payload)
	}
	started := benchobs.Start()
	results := p.Client.ProduceSync(ctx, records...)
	err := results.FirstErr()
	offsets := make(map[int32]int64)
	for _, result := range results {
		if result.Err == nil {
			offsets[result.Record.Partition] = result.Record.Offset
		}
	}
	benchobs.Finish(started, benchobs.Event{Kind: "kafka_produce", Rows: rows, Bytes: bytes, Success: err == nil, Offsets: offsets})
	return err
}

type Server struct {
	collector.UnimplementedTraceServiceServer
	publisher  Publisher
	slots      chan struct{}
	now        func() time.Time
	partitions int
}

func NewServer(p Publisher) *Server {
	partitions := PartitionCount
	if k, ok := p.(*KafkaPublisher); ok {
		partitions = ExpectedPartitions(k.Topic)
	}
	return &Server{publisher: p, slots: make(chan struct{}, 4), now: time.Now, partitions: partitions}
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
	records := make([]Publication, 0, (len(rows)+MaxRows-1)/MaxRows+s.partitions)
	groups := make([][]trace.Row, s.partitions)
	for _, row := range rows {
		partition, e := PartitionForTraceID(row.TraceID)
		if e != nil {
			return nil, status.Error(codes.ResourceExhausted, "invalid trace routing identity")
		}
		groups[int(partition)%s.partitions] = append(groups[int(partition)%s.partitions], row)
	}
	for partition, group := range groups {
		for i := 0; i < len(group); i += MaxRows {
			end := min(i+MaxRows, len(group))
			b, e := EncodeBatch(group[i:end])
			if e != nil {
				return nil, status.Error(codes.ResourceExhausted, "normalized stream record exceeds limit")
			}
			records = append(records, Publication{int32(partition), end - i, b})
		}
	}
	if batch, ok := s.publisher.(BatchPublisher); ok && len(records) > 0 {
		if err := batch.PublishBatch(ctx, records); err != nil {
			return nil, status.Error(codes.Unavailable, fmt.Sprintf("broker acknowledgement failed; retry may duplicate spans: %v", err))
		}
		if ctx.Err() != nil {
			return nil, status.Error(codes.Unavailable, "broker acknowledgement deadline exceeded")
		}
		records = nil
	}
	for _, record := range records {
		publishCtx := context.WithValue(benchobs.WithRows(ctx, record.Rows), partitionContextKey{}, record.Partition)
		if err := s.publisher.Publish(publishCtx, record.Payload); err != nil {
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
