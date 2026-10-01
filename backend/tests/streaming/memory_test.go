package streaming

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"incidentlens/backend/internal/stream"
	"incidentlens/backend/internal/trace"
)

func compressedProducer(t *testing.T) *kgo.Client {
	t.Helper()
	client, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:19092"),
		kgo.ProducerBatchMaxBytes(8<<20),
		kgo.ProducerBatchCompression(kgo.GzipCompression()),
		kgo.ProducerLinger(0),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}

func publishCompressed(t *testing.T, producer *kgo.Client, topic string, value []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: value}).FirstErr(); err != nil {
		t.Fatalf("publish compressed batch (%d uncompressed bytes): %v", len(value), err)
	}
}

func TestCompressedBatchAboveDecompressionBoundDoesNotAdvanceOffset(t *testing.T) {
	admin, topic := isolatedTopic(t)
	group := topic + "-memory-bound"
	commitAt(t, admin, topic, group, 0)
	producer := compressedProducer(t)
	publishCompressed(t, producer, topic, bytes.Repeat([]byte{'x'}, 6<<20))
	writer := &forbiddenWriter{}
	worker, err := stream.NewWorker([]string{"127.0.0.1:19092"}, topic, group, writer)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { worker.Client.AllowRebalance(); worker.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err = worker.Run(ctx)
	var tooLarge *kgo.ErrDecompressTooLarge
	if !errors.As(err, &tooLarge) || writer.called {
		t.Fatalf("oversized decompressed batch: error=%v wrote=%v", err, writer.called)
	}
	if tooLarge.Topic != topic || tooLarge.Offset != 0 || tooLarge.NextOffset != 1 {
		t.Fatalf("unexpected oversized batch boundary: %+v", tooLarge)
	}
	if got := committedAt(t, admin, topic, group); got != 0 {
		t.Fatalf("oversized batch advanced committed offset to %d", got)
	}
	t.Logf("uncompressed=%d broker offset=%d next=%d committed=0; worker rejected before storage", 6<<20, tooLarge.Offset, tooLarge.NextOffset)
}

func memoryFixture(now time.Time, batch int) ([]byte, []trace.Row, error) {
	attribute := `{"attributes":[{"key":"test.payload","value":{"stringValue":"` + strings.Repeat("x", 4000) + `"}}]}`
	rows := make([]trace.Row, 128)
	for i := range rows {
		rows[i] = trace.Row{
			TraceID: fmt.Sprintf("%032x", batch+1), SpanID: fmt.Sprintf("%016x", i+1),
			ServiceName: "memory-fixture", ServiceNamespace: "streamtest-memory", SpanName: "memory.operation", SpanKind: 2,
			StartTime: now.Add(-time.Minute), EndTime: now.Add(-time.Minute).Add(time.Nanosecond), IngestedAt: now,
			DurationNS: 1, ResourceAttributes: "{}", SpanAttributes: attribute, ScopeAttributes: "{}", Events: "{}", Links: "{}",
		}
	}
	value, err := stream.EncodeBatch(rows)
	return value, rows, err
}

func TestOneByteFetchBudgetMakesOrderedProgressAcrossCompressedBatches(t *testing.T) {
	admin, topic := isolatedTopic(t)
	group := topic + "-progress"
	commitAt(t, admin, topic, group, 0)
	producer := compressedProducer(t)
	now := time.Now().UTC().Truncate(time.Second)
	expected := make([][]trace.Row, 4)
	for i := range expected {
		value, rows, err := memoryFixture(now, i)
		if err != nil || len(value) >= stream.MaxRecordBytes || len(value) < 500_000 {
			t.Fatalf("fixture %d size=%d rows=%d error=%v", i, len(value), len(rows), err)
		}
		expected[i] = rows
		publishCompressed(t, producer, topic, value) // Sync keeps records in separate producer batches.
	}
	worker, err := stream.NewWorker([]string{"127.0.0.1:19092"}, topic, group, &forbiddenWriter{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { worker.Client.AllowRebalance(); worker.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := range expected {
		fetches := worker.Client.PollFetches(ctx)
		var fetchErr error
		fetches.EachError(func(_ string, _ int32, err error) { fetchErr = err })
		records := fetches.Records()
		if fetchErr != nil || len(records) != 1 || records[0].Offset != int64(i) {
			t.Fatalf("poll %d returned %d records (error=%v); want one ordered batch", i, len(records), fetchErr)
		}
		decoded, err := stream.Decode(records[0].Value, now)
		if err != nil || !reflect.DeepEqual(decoded, expected[i]) {
			t.Fatalf("poll %d payload changed: rows=%d error=%v", i, len(decoded), err)
		}
		if err := worker.Client.CommitRecords(ctx, records[0]); err != nil {
			t.Fatalf("commit offset %d: %v", i+1, err)
		}
		worker.Client.AllowRebalance()
		if got := committedAt(t, admin, topic, group); got != int64(i+1) {
			t.Fatalf("poll %d committed offset=%d want %d", i, got, i+1)
		}
	}
	t.Logf("four separate compressed valid records (%d normalized rows) progressed one fetch each and committed offsets 1..4", 4*128)
}
