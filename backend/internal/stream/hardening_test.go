package stream

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"incidentlens/backend/internal/trace"
)

func TestTraceRoutingStableAcrossPartitions(t *testing.T) {
	for id, want := range map[string]int32{
		"00000000000000000000000000000001": 2,
		"00000000000000000000000000000002": 3,
		"00000000000000000000000000000003": 0,
		"00000000000000000000000000000004": 1,
	} {
		got, err := PartitionForTraceID(id)
		if err != nil || got != want {
			t.Fatalf("stable hash vector %s: got %d, want %d, err %v", id, got, want, err)
		}
	}
	byPartition := map[int32]string{}
	for i := 1; i < 100 && len(byPartition) < PartitionCount; i++ {
		id := fmt.Sprintf("%032x", i)
		partition, err := PartitionForTraceID(id)
		if err != nil || partition < 0 || partition >= PartitionCount {
			t.Fatalf("id=%s partition=%d err=%v", id, partition, err)
		}
		byPartition[partition] = id
		again, err := PartitionForTraceID(id)
		if err != nil || again != partition {
			t.Fatalf("routing unstable for %s", id)
		}
	}
	if len(byPartition) != PartitionCount {
		t.Fatalf("only %d partitions reached", len(byPartition))
	}
	for _, invalid := range []string{"", "g1111111111111111111111111111111", "ABCDEF0123456789ABCDEF0123456789"} {
		if _, err := PartitionForTraceID(invalid); err == nil {
			t.Fatalf("invalid trace ID accepted: %q", invalid)
		}
	}
}

type publicationCapture struct {
	publications []Publication
	calls        int
}

func (p *publicationCapture) Publish(context.Context, []byte) error {
	panic("batch publisher must be used")
}
func (p *publicationCapture) PublishBatch(_ context.Context, batch []Publication) error {
	p.calls++
	p.publications = append(p.publications, batch...)
	return nil
}

func TestExportGroupsRowsByStableTracePartition(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	req := request(now, 20, 0)
	spans := req.ResourceSpans[0].ScopeSpans[0].Spans
	for i, span := range spans {
		b, err := hex.DecodeString(fmt.Sprintf("%032x", i+1))
		if err != nil {
			t.Fatal(err)
		}
		span.TraceId = b
	}
	publisher := &publicationCapture{}
	server := NewServer(publisher)
	server.now = func() time.Time { return now }
	if _, err := server.Export(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if publisher.calls != 1 {
		t.Fatalf("PublishBatch calls=%d", publisher.calls)
	}
	seen := map[int32]bool{}
	count := 0
	for _, publication := range publisher.publications {
		if len(publication.Payload) > MaxRecordBytes || publication.Rows > MaxRows {
			t.Fatalf("unbounded publication %+v", publication)
		}
		rows, err := Decode(publication.Payload, now)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != publication.Rows {
			t.Fatalf("row count mismatch")
		}
		for _, row := range rows {
			want, err := PartitionForTraceID(row.TraceID)
			if err != nil || want != publication.Partition {
				t.Fatalf("trace %s routed to %d, want %d", row.TraceID, publication.Partition, want)
			}
			seen[want] = true
			count++
		}
	}
	if count != 20 || len(seen) != PartitionCount {
		t.Fatalf("rows=%d partitions=%d", count, len(seen))
	}
}

type cohortWriter struct {
	mu      sync.Mutex
	batches [][]trace.Row
	failure error
}

type gatedBatchWriter struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
	rows    int
}

func (w *gatedBatchWriter) Write(ctx context.Context, rows []trace.Row) error {
	return w.WriteBatch(ctx, rows)
}
func (w *gatedBatchWriter) WriteBatch(ctx context.Context, rows []trace.Row) error {
	blocked := false
	w.once.Do(func() { blocked = true; w.rows = len(rows); close(w.started) })
	if blocked {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-w.release:
		}
	}
	return nil
}

func (w *cohortWriter) Write(ctx context.Context, rows []trace.Row) error {
	return w.WriteBatch(ctx, rows)
}
func (w *cohortWriter) WriteBatch(_ context.Context, rows []trace.Row) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failure != nil {
		return w.failure
	}
	w.batches = append(w.batches, append([]trace.Row(nil), rows...))
	return nil
}

func recordFor(t *testing.T, now time.Time, id string, partition int32, offset int64, count int) *kgo.Record {
	t.Helper()
	rows := make([]trace.Row, count)
	for i := range rows {
		rows[i] = row(now)
		rows[i].TraceID = id
		rows[i].SpanID = fmt.Sprintf("%016x", offset*1000+int64(i)+1)
	}
	b, err := EncodeBatch(rows)
	if err != nil {
		t.Fatal(err)
	}
	return &kgo.Record{Topic: Topic, Partition: partition, Offset: offset, Value: b}
}

func TestCohortBatchesAcrossRecordsAndCommitsContiguousHighest(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	ids := map[int32]string{}
	for i := 1; len(ids) < 2; i++ {
		id := fmt.Sprintf("%032x", i)
		p, _ := PartitionForTraceID(id)
		ids[p] = id
	}
	var partitions []int32
	for p := range ids {
		partitions = append(partitions, p)
	}
	writer := &cohortWriter{}
	var committed []*kgo.Record
	worker := &Worker{Topic: Topic, Writer: writer, Now: func() time.Time { return now.Add(time.Second) }, Commit: func(_ context.Context, records ...*kgo.Record) error {
		committed = append(committed, records...)
		return nil
	}}
	records := []*kgo.Record{
		recordFor(t, now, ids[partitions[0]], partitions[0], 0, 40),
		recordFor(t, now, ids[partitions[1]], partitions[1], 0, 40),
		recordFor(t, now, ids[partitions[0]], partitions[0], 1, 40),
	}
	worker.AfterWrite = func(_ *kgo.Record, _ []trace.Row) {
		if got := worker.Progress().LastCompletedProcessingAgeNS; got != int64(time.Second) {
			t.Errorf("receipt-to-write age=%d", got)
		}
	}
	if err := worker.processFetched(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	if len(committed) != 2 {
		t.Fatalf("committed %d partitions", len(committed))
	}
	got := map[int32]int64{}
	for _, record := range committed {
		got[record.Partition] = record.Offset
	}
	if got[partitions[0]] != 1 || got[partitions[1]] != 0 {
		t.Fatalf("non-contiguous commits: %v", got)
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if len(writer.batches) != 1 {
		t.Fatalf("write batches=%d", len(writer.batches))
	}
	for i, want := range records {
		if got := writer.batches[0][i*40].TraceID; got != ids[want.Partition] {
			t.Fatalf("row order at record %d: %s", i, got)
		}
	}
	written := 0
	for _, batch := range writer.batches {
		written += len(batch)
		if len(batch) > CohortMaxRows {
			t.Fatal("row cap exceeded")
		}
	}
	if written != 120 {
		t.Fatalf("written=%d", written)
	}
	progress := worker.Progress()
	if progress.PendingRecords != 0 || progress.LastCommitAt.IsZero() || progress.LastWriteAt.IsZero() {
		t.Fatalf("progress=%+v", progress)
	}
}

func TestCohortWriteFailureRetainsOffsets(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	id := "11111111111111111111111111111111"
	partition, _ := PartitionForTraceID(id)
	writer := &cohortWriter{failure: errors.New("storage unavailable")}
	commits := 0
	worker := &Worker{Topic: Topic, Writer: writer, Now: func() time.Time { return now }, Commit: func(context.Context, ...*kgo.Record) error { commits++; return nil }}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := worker.processFetched(ctx, []*kgo.Record{recordFor(t, now, id, partition, 0, 1)}); err == nil {
		t.Fatal("write failure hidden")
	}
	if commits != 0 {
		t.Fatalf("committed %d times after failed write", commits)
	}
}

func TestRoutingViolationIsPoisonWithoutWrite(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	id := "11111111111111111111111111111111"
	partition, _ := PartitionForTraceID(id)
	writer := &cohortWriter{}
	worker := &Worker{Topic: Topic, Writer: writer, Now: func() time.Time { return now }}
	record := recordFor(t, now, id, (partition+1)%PartitionCount, 0, 1)
	if err := worker.processFetched(context.Background(), []*kgo.Record{record}); err == nil {
		t.Fatal("routing violation accepted")
	}
	if len(writer.batches) != 0 {
		t.Fatal("poison written")
	}
}

func TestWholeCohortWriteCompletesBeforeAnyOffsetCommit(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	ids := map[int32]string{}
	for i := 1; len(ids) < 2; i++ {
		id := fmt.Sprintf("%032x", i)
		p, _ := PartitionForTraceID(id)
		ids[p] = id
	}
	partitions := make([]int32, 0, 2)
	for p := range ids {
		partitions = append(partitions, p)
	}
	writer := &gatedBatchWriter{started: make(chan struct{}), release: make(chan struct{})}
	committed := make(chan struct{}, 1)
	worker := &Worker{Topic: Topic, Writer: writer, Now: func() time.Time { return now }, Commit: func(context.Context, ...*kgo.Record) error { committed <- struct{}{}; return nil }}
	done := make(chan error, 1)
	go func() {
		done <- worker.processFetched(context.Background(), []*kgo.Record{recordFor(t, now, ids[partitions[0]], partitions[0], 0, 1), recordFor(t, now, ids[partitions[1]], partitions[1], 0, 1)})
	}()
	select {
	case <-writer.started:
	case <-time.After(time.Second):
		t.Fatal("write never started")
	}
	if writer.rows != 2 {
		t.Fatalf("whole cohort write rows=%d, want 2", writer.rows)
	}
	select {
	case <-committed:
		t.Fatal("committed before whole cohort write completed")
	case <-time.After(20 * time.Millisecond):
	}
	close(writer.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cohort did not finish")
	}
	select {
	case <-committed:
	default:
		t.Fatal("cohort did not commit after whole write")
	}
}

func TestFetchedResponseSplitsAtHardRecordCap(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	id := "11111111111111111111111111111111"
	partition, _ := PartitionForTraceID(id)
	records := make([]*kgo.Record, CohortMaxRecords+1)
	for i := range records {
		records[i] = recordFor(t, now, id, partition, int64(i), 1)
	}
	writer := &cohortWriter{}
	var offsets []int64
	worker := &Worker{Topic: Topic, Writer: writer, Now: func() time.Time { return now }, Commit: func(_ context.Context, rs ...*kgo.Record) error {
		offsets = append(offsets, rs[0].Offset+1)
		return nil
	}}
	if err := worker.processFetched(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	if len(offsets) != 2 || offsets[0] != CohortMaxRecords || offsets[1] != CohortMaxRecords+1 {
		t.Fatalf("cohort boundaries %v", offsets)
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if len(writer.batches) != 2 || len(writer.batches[0]) != CohortMaxRecords || len(writer.batches[1]) != 1 {
		t.Fatalf("write boundaries %d", len(writer.batches))
	}
}

func TestWholeCohortRespectsTargetAndHardBounds(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	id := "11111111111111111111111111111111"
	partition, _ := PartitionForTraceID(id)
	fullCohortRecords := CohortTargetRows / MaxRows
	records := make([]*kgo.Record, fullCohortRecords+1)
	for i := range records {
		records[i] = recordFor(t, now, id, partition, int64(i), MaxRows)
	}
	writer := &cohortWriter{}
	var offsets []int64
	worker := &Worker{Topic: Topic, Writer: writer, Now: func() time.Time { return now }, Commit: func(_ context.Context, rs ...*kgo.Record) error {
		offsets = append(offsets, rs[0].Offset+1)
		return nil
	}}
	if err := worker.processFetched(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	if len(offsets) != 2 || offsets[0] != int64(fullCohortRecords) || offsets[1] != int64(fullCohortRecords+1) {
		t.Fatalf("committed cohort boundaries %v", offsets)
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if len(writer.batches) != 2 || len(writer.batches[0]) != CohortTargetRows || len(writer.batches[1]) != MaxRows {
		t.Fatalf("whole-write sizes: %d batches", len(writer.batches))
	}
	for _, batch := range writer.batches {
		if len(batch) > CohortMaxRows {
			t.Fatalf("write exceeds hard row cap: %d", len(batch))
		}
	}
}
