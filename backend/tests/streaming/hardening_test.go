package streaming

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	collectorpb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	storage "incidentlens/backend/internal/storage/clickhouse"
	"incidentlens/backend/internal/stream"
	"incidentlens/backend/internal/trace"
)

func isolatedV2Topic(t *testing.T) (*kgo.Client, string) {
	t.Helper()
	requireStack(t)
	admin, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:19092"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	topic := "streamtest-v2-" + seed(t)
	req := kmsg.NewPtrCreateTopicsRequest()
	req.TimeoutMillis = 10000
	v := kmsg.NewCreateTopicsRequestTopic()
	v.Topic = topic
	v.NumPartitions = stream.PartitionCount
	v.ReplicationFactor = 1
	v.Configs = []kmsg.CreateTopicsRequestTopicConfig{{Name: "retention.ms", Value: kmsg.StringPtr("3600000")}, {Name: "retention.bytes", Value: kmsg.StringPtr("16777216")}}
	req.Topics = []kmsg.CreateTopicsRequestTopic{v}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	res, err := req.RequestWith(ctx, admin)
	if err != nil || len(res.Topics) != 1 || res.Topics[0].ErrorCode != 0 {
		t.Fatalf("create v2 test topic: %+v %v", res, err)
	}
	return admin, topic
}

// This exercises the pinned Kafka client and synchronous ClickHouse writer with
// four partitions. The frozen single-partition replay/crash tests remain separate.
func TestFourPartitionRoutingBatchAndFreshGroupReplay(t *testing.T) {
	conn := requireStack(t)
	admin, topic := isolatedV2Topic(t)
	mergeContext, mergeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := conn.Exec(mergeContext, "SYSTEM STOP MERGES incidentlens.spans"); err != nil {
		mergeCancel()
		t.Fatal(err)
	}
	mergeCancel()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := conn.Exec(ctx, "SYSTEM START MERGES incidentlens.spans"); err != nil {
			t.Errorf("restore merges: %v", err)
		}
	})
	producer, err := stream.NewProducer([]string{"127.0.0.1:19092"}, topic)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	ctx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := producer.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	pingCancel()
	server := stream.NewServer(producer)
	runSeed := seed(t)
	namespace := "streamtest-v2-ns-" + runSeed
	identities := make(map[string]bool, 128)
	partitions := map[int32]bool{}
	for exportIndex := 0; exportIndex < 4; exportIndex++ {
		request := &collectorpb.ExportTraceServiceRequest{}
		for i := exportIndex * 32; i < (exportIndex+1)*32; i++ {
			fragment, id, _ := fixture(t, runSeed, i)
			for _, attribute := range fragment.ResourceSpans[0].Resource.Attributes {
				if attribute.Key == "service.namespace" {
					attribute.Value = &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: namespace}}
				}
			}
			partition, routeErr := stream.PartitionForTraceID(id)
			if routeErr != nil {
				t.Fatal(routeErr)
			}
			partitions[partition] = true
			request.ResourceSpans = append(request.ResourceSpans, fragment.ResourceSpans...)
			identities[id] = true
		}
		publishCtx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
		response, publishErr := server.Export(publishCtx, request)
		cancel()
		if publishErr != nil || response.GetPartialSuccess().GetRejectedSpans() != 0 {
			t.Fatalf("publish: %v %+v", publishErr, response)
		}
	}
	if len(partitions) != stream.PartitionCount {
		t.Fatalf("fixture reached only %d partitions", len(partitions))
	}
	ends := map[int32]int64{}
	for p := int32(0); p < stream.PartitionCount; p++ {
		ends[p] = topicEndPartition(t, admin, topic, p)
		if ends[p] < 2 {
			t.Fatalf("partition %d only has %d records; expected cross-record cohort", p, ends[p])
		}
	}
	writeStore, err := storage.Open("127.0.0.1:19000", "ingest", "local-ingest", "incidentlens")
	if err != nil {
		t.Fatal(err)
	}
	defer writeStore.Close()
	group := "streamtest-v2-worker-" + runSeed
	consume := func(group string) {
		worker, err := stream.NewWorker([]string{"127.0.0.1:19092"}, topic, group, writeStore)
		if err != nil {
			t.Fatal(err)
		}
		defer worker.Close()
		runCtx, stop := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- worker.Run(runCtx) }()
		finished := false
		defer func() {
			stop()
			if !finished {
				select {
				case <-done:
				case <-time.After(10 * time.Second):
					t.Errorf("worker did not stop after cancellation")
				}
			}
		}()
		deadline := time.Now().Add(90 * time.Second)
		for time.Now().Before(deadline) {
			all := true
			for partition, end := range ends {
				if committedPartition(t, admin, topic, group, partition) != end {
					all = false
					break
				}
			}
			if all && worker.Progress().PendingRecords == 0 && !worker.Progress().LastCommitAt.IsZero() {
				return
			}
			select {
			case err := <-done:
				finished = true
				t.Fatalf("worker exited early: %v", err)
			case <-time.After(100 * time.Millisecond):
			}
		}
		t.Fatalf("worker %s did not store and commit all 128 traces", group)
	}
	consume(group)
	if got := storedIDs(t, conn, namespace); len(got) != len(identities) {
		t.Fatalf("initial FINAL identities=%d, want %d", len(got), len(identities))
	}
	if got := rawNamespaceRows(t, conn, namespace); got != uint64(len(identities)) {
		t.Fatalf("initial raw rows=%d", got)
	}
	consume(fmt.Sprintf("%s-replay", group))
	if got := rawNamespaceRows(t, conn, namespace); got < uint64(2*len(identities)) {
		t.Fatalf("fresh-group replay did not create physical copies: %d", got)
	}
	got := storedIDs(t, conn, namespace)
	if len(got) != len(identities) {
		t.Fatalf("replay FINAL identities=%d, want %d", len(got), len(identities))
	}
	for id := range identities {
		if !got[id] {
			t.Fatalf("missing replay identity %s", id)
		}
	}
}

type countedBatchStore struct {
	inner *storage.Store
	mu    sync.Mutex
	sizes []int
}

func (c *countedBatchStore) Write(ctx context.Context, rows []trace.Row) error {
	return c.WriteBatch(ctx, rows)
}

func (c *countedBatchStore) WriteBatch(ctx context.Context, rows []trace.Row) error {
	if err := c.inner.WriteBatch(ctx, rows); err != nil {
		return err
	}
	c.mu.Lock()
	c.sizes = append(c.sizes, len(rows))
	c.mu.Unlock()
	return nil
}

// Nine 256-row records in one broker batch cross the 2,048-row target. The
// worker must commit only after both bounded writes and preserve FINAL on replay.
func TestCohortTargetAcrossBrokerBatchAndReplay(t *testing.T) {
	conn := requireStack(t)
	admin, topic := isolatedV2Topic(t)
	namespace := "streamtest-target-" + seed(t)
	id := "00000000000000000000000000000003"
	partition, err := stream.PartitionForTraceID(id)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	const records = 9
	const rowsPerRecord = stream.MaxRows
	const expectedRows = records * rowsPerRecord
	producer, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:19092"), kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.MaxBufferedRecords(records), kgo.ProducerBatchMaxBytes(5<<20))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	batch := make([]*kgo.Record, 0, records)
	for recordIndex := 0; recordIndex < records; recordIndex++ {
		rows := make([]trace.Row, rowsPerRecord)
		for i := range rows {
			rows[i] = trace.Row{
				TraceID: id, SpanID: fmt.Sprintf("%016x", recordIndex*rowsPerRecord+i+1),
				ServiceName: "stream-fixture", ServiceNamespace: namespace, SpanName: "stream.target",
				StartTime: now.Add(-time.Minute), EndTime: now.Add(-time.Minute).Add(time.Nanosecond), IngestedAt: now,
				DurationNS: 1, ResourceAttributes: "{}", SpanAttributes: "{}", ScopeAttributes: "{}", Events: "{}", Links: "{}",
			}
		}
		value, encodeErr := stream.EncodeBatch(rows)
		if encodeErr != nil {
			t.Fatal(encodeErr)
		}
		batch = append(batch, &kgo.Record{Topic: topic, Partition: partition, Value: value})
	}
	produceCtx, cancelProduce := context.WithTimeout(context.Background(), 20*time.Second)
	err = producer.ProduceSync(produceCtx, batch...).FirstErr()
	cancelProduce()
	if err != nil {
		t.Fatal(err)
	}
	if got := topicEndPartition(t, admin, topic, partition); got != records {
		t.Fatalf("broker end=%d, want %d", got, records)
	}
	store, err := storage.Open("127.0.0.1:19000", "ingest", "local-ingest", "incidentlens")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	counted := &countedBatchStore{inner: store}
	consume := func(group string, writer stream.RowWriter) {
		worker, workerErr := stream.NewWorker([]string{"127.0.0.1:19092"}, topic, group, writer)
		if workerErr != nil {
			t.Fatal(workerErr)
		}
		ctx, stop := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- worker.Run(ctx) }()
		doneConsumed := false
		defer func() {
			stop()
			worker.Close()
			if !doneConsumed {
				<-done
			}
		}()
		deadline := time.Now().Add(90 * time.Second)
		for time.Now().Before(deadline) {
			if committedPartition(t, admin, topic, group, partition) == records {
				return
			}
			select {
			case runErr := <-done:
				doneConsumed = true
				t.Fatalf("worker exited before commit: %v", runErr)
			case <-time.After(100 * time.Millisecond):
			}
		}
		t.Fatalf("group %s did not commit all %d records", group, records)
	}
	group := "streamtest-target-" + seed(t)
	consume(group, counted)
	counted.mu.Lock()
	sizes := append([]int(nil), counted.sizes...)
	counted.mu.Unlock()
	if len(sizes) != 2 || sizes[0] != stream.CohortTargetRows || sizes[1] != rowsPerRecord {
		t.Fatalf("completed write sizes=%v, want [%d %d]", sizes, stream.CohortTargetRows, rowsPerRecord)
	}
	if got := rawNamespaceRows(t, conn, namespace); got != expectedRows {
		t.Fatalf("raw rows=%d, want %d", got, expectedRows)
	}
	consume(group+"-replay", store)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var finalRows, uniqueSpans uint64
	if err := conn.QueryRow(ctx, "SELECT count(), uniqExact(span_id) FROM spans FINAL WHERE service_namespace=? AND service_name='stream-fixture'", namespace).Scan(&finalRows, &uniqueSpans); err != nil {
		t.Fatal(err)
	}
	if finalRows != expectedRows || uniqueSpans != expectedRows {
		t.Fatalf("replay FINAL rows=%d unique spans=%d, want %d", finalRows, uniqueSpans, expectedRows)
	}
	if got := rawNamespaceRows(t, conn, namespace); got < 2*expectedRows {
		t.Fatalf("physical replay rows=%d, want at least %d", got, 2*expectedRows)
	}
}

func topicEndPartition(t *testing.T, admin *kgo.Client, topic string, partition int32) int64 {
	t.Helper()
	req := kmsg.NewPtrListOffsetsRequest()
	p := kmsg.NewListOffsetsRequestTopicPartition()
	p.Partition = partition
	p.Timestamp = -1
	req.Topics = []kmsg.ListOffsetsRequestTopic{{Topic: topic, Partitions: []kmsg.ListOffsetsRequestTopicPartition{p}}}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := req.RequestWith(ctx, admin)
	if err != nil || len(res.Topics) != 1 || len(res.Topics[0].Partitions) != 1 || res.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("end offset: %+v %v", res, err)
	}
	return res.Topics[0].Partitions[0].Offset
}
func rawNamespaceRows(t *testing.T, conn driver.Conn, namespace string) uint64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var count uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM spans WHERE service_namespace=? AND service_name='stream-fixture'", namespace).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
func storedIDs(t *testing.T, conn driver.Conn, namespace string) map[string]bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := conn.Query(ctx, "SELECT trace_id FROM spans FINAL PREWHERE service_namespace=? AND service_name='stream-fixture'", namespace)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	result := map[string]bool{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		result[id] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func committedPartition(t *testing.T, admin *kgo.Client, topic, group string, partition int32) int64 {
	t.Helper()
	req := kmsg.NewPtrOffsetFetchRequest()
	req.Group = group
	req.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: topic, Partitions: []int32{partition}}}
	g := kmsg.NewOffsetFetchRequestGroup()
	g.Group = group
	g.Topics = []kmsg.OffsetFetchRequestGroupTopic{{Topic: topic, Partitions: []int32{partition}}}
	req.Groups = []kmsg.OffsetFetchRequestGroup{g}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := req.RequestWith(ctx, admin)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Groups) == 1 {
		parts := res.Groups[0].Topics
		if res.Groups[0].ErrorCode != 0 || len(parts) != 1 || len(parts[0].Partitions) != 1 || parts[0].Partitions[0].ErrorCode != 0 {
			t.Fatalf("offset response %+v", res)
		}
		return parts[0].Partitions[0].Offset
	}
	if res.ErrorCode != 0 || len(res.Topics) != 1 || len(res.Topics[0].Partitions) != 1 || res.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("offset response %+v", res)
	}
	return res.Topics[0].Partitions[0].Offset
}

func groupRebalanceRequested(t *testing.T, admin *kgo.Client, group string) bool {
	t.Helper()
	req := kmsg.NewPtrDescribeGroupsRequest()
	req.Groups = []string{group}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := req.RequestWith(ctx, admin)
	if err != nil {
		t.Fatalf("describe group %s: %v", group, err)
	}
	return len(res.Groups) == 1 && res.Groups[0].ErrorCode == 0 && (len(res.Groups[0].Members) >= 2 || res.Groups[0].State == "PreparingRebalance")
}

func TestFourPartitionWriteBeforeCommitCrashReplays(t *testing.T) {
	conn := requireStack(t)
	admin, topic := isolatedV2Topic(t)
	publisher, err := stream.NewProducer([]string{"127.0.0.1:19092"}, topic)
	if err != nil {
		t.Fatal(err)
	}
	defer publisher.Close()
	request, id, namespace := fixture(t, seed(t), 1)
	partition, err := stream.PartitionForTraceID(id)
	if err != nil {
		t.Fatal(err)
	}
	server := stream.NewServer(publisher)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	_, err = server.Export(ctx, request)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	group := "streamtest-v2-crash-" + seed(t)
	crashCtx, stop := context.WithTimeout(context.Background(), 75*time.Second)
	defer stop()
	cmd := composeCommand(t, crashCtx,
		"run", "--rm", "--no-deps", "-e", "KAFKA_GROUP_ID="+group,
		"-e", "KAFKA_TOPIC="+topic, "-e", "STREAM_TEST_EXIT_AFTER_WRITE_TRACE_ID="+id, "stream-worker")
	output, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 86 {
		t.Fatalf("write-before-commit hook: %v: %s", err, output)
	}
	awaitStored(t, conn, id, namespace, 1)
	if got := committedPartition(t, admin, topic, group, partition); got > 0 {
		t.Fatalf("crash advanced offset to %d", got)
	}
	writeStore, err := storage.Open("127.0.0.1:19000", "ingest", "local-ingest", "incidentlens")
	if err != nil {
		t.Fatal(err)
	}
	defer writeStore.Close()
	worker, err := stream.NewWorker([]string{"127.0.0.1:19092"}, topic, group, writeStore)
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	runCtx, cancelRun := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(runCtx) }()
	joined := false
	defer func() {
		cancelRun()
		if !joined {
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("replay worker did not stop")
			}
		}
	}()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		if committedPartition(t, admin, topic, group, partition) >= 1 {
			cancelRun()
			<-done
			joined = true
			break
		}
		select {
		case runErr := <-done:
			t.Fatalf("replay worker exited: %v", runErr)
		case <-time.After(250 * time.Millisecond):
		}
	}
	if got := committedPartition(t, admin, topic, group, partition); got != 1 {
		cancelRun()
		t.Fatalf("replay committed offset %d, want 1", got)
	}
	if got := stored(t, conn, id, namespace); got != 1 {
		t.Fatalf("duplicate-safe FINAL count=%d", got)
	}
}

// The first insert remains in flight while a second member joins the group.
// Kafka must not observe a commit for that cohort until the write completes.
type gateStore struct {
	inner            *storage.Store
	started          chan struct{}
	release          chan struct{}
	releaseOnce      sync.Once
	blockedPartition int32
	once             sync.Once
}

func (g *gateStore) Write(ctx context.Context, rows []trace.Row) error {
	return g.WriteBatch(ctx, rows)
}
func (g *gateStore) WriteBatch(ctx context.Context, rows []trace.Row) error {
	blocked := false
	g.once.Do(func() {
		blocked = true
		g.blockedPartition, _ = stream.PartitionForTraceID(rows[0].TraceID)
		close(g.started)
	})
	if blocked {
		select {
		case <-g.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return g.inner.WriteBatch(ctx, rows)
}

func TestRebalanceWhileWriteStalledPreservesOffsets(t *testing.T) {
	conn := requireStack(t)
	admin, topic := isolatedV2Topic(t)
	publisher, err := stream.NewProducer([]string{"127.0.0.1:19092"}, topic)
	if err != nil {
		t.Fatal(err)
	}
	defer publisher.Close()
	server := stream.NewServer(publisher)
	namespace := "streamtest-rebalance-" + seed(t)
	ids := map[string]bool{}
	for i := 0; i < 16; i++ {
		req, id, _ := fixture(t, namespace, i)
		for _, attr := range req.ResourceSpans[0].Resource.Attributes {
			if attr.Key == "service.namespace" {
				attr.Value = &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: namespace}}
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
		_, err = server.Export(ctx, req)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		ids[id] = true
	}
	ends := map[int32]int64{}
	for p := int32(0); p < stream.PartitionCount; p++ {
		if end := topicEndPartition(t, admin, topic, p); end > 0 {
			ends[p] = end
		}
	}
	store, err := storage.Open("127.0.0.1:19000", "ingest", "local-ingest", "incidentlens")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	gate := &gateStore{inner: store, started: make(chan struct{}), release: make(chan struct{})}
	group := "streamtest-rebalance-" + seed(t)
	first, err := stream.NewWorker([]string{"127.0.0.1:19092"}, topic, group, gate)
	if err != nil {
		t.Fatal(err)
	}
	ctx1, stop1 := context.WithCancel(context.Background())
	done1 := make(chan error, 1)
	go func() { done1 <- first.Run(ctx1) }()
	var second *stream.Worker
	var stop2 context.CancelFunc
	var done2 chan error
	t.Cleanup(func() {
		gate.releaseOnce.Do(func() { close(gate.release) })
		stop1()
		if stop2 != nil {
			stop2()
		}
		first.Close()
		if second != nil {
			second.Close()
		}
		select {
		case <-done1:
		case <-time.After(10 * time.Second):
			t.Error("first worker failed to stop")
		}
		if done2 != nil {
			select {
			case <-done2:
			case <-time.After(10 * time.Second):
				t.Error("second worker failed to stop")
			}
		}
	})
	select {
	case <-gate.started:
	case <-time.After(20 * time.Second):
		t.Fatal("first worker never began insert")
	}
	second, err = stream.NewWorker([]string{"127.0.0.1:19092"}, topic, group, store)
	if err != nil {
		t.Fatal(err)
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	stop2 = cancel2
	done2 = make(chan error, 1)
	go func() { done2 <- second.Run(ctx2) }()
	// Observe both members in Kafka before releasing the first insert.
	joinedDeadline := time.Now().Add(20 * time.Second)
	for !groupRebalanceRequested(t, admin, group) && time.Now().Before(joinedDeadline) {
		select {
		case err := <-done1:
			t.Fatalf("first worker exited before rebalance: %v", err)
		case err := <-done2:
			t.Fatalf("second worker exited before rebalance: %v", err)
		case <-time.After(100 * time.Millisecond):
		}
	}
	if !groupRebalanceRequested(t, admin, group) {
		t.Fatal("second consumer never requested rebalance while write stalled")
	}
	if got := committedPartition(t, admin, topic, group, gate.blockedPartition); got > 0 {
		t.Fatalf("stalled partition %d committed %d before durable write", gate.blockedPartition, got)
	}
	gate.releaseOnce.Do(func() { close(gate.release) })
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		all := true
		for p, end := range ends {
			if committedPartition(t, admin, topic, group, p) != end {
				all = false
				break
			}
		}
		if all {
			break
		}
		select {
		case err := <-done1:
			t.Fatalf("first worker exited: %v", err)
		case err := <-done2:
			t.Fatalf("second worker exited: %v", err)
		case <-time.After(200 * time.Millisecond):
		}
	}
	for p, end := range ends {
		if got := committedPartition(t, admin, topic, group, p); got != end {
			t.Fatalf("partition %d committed %d, want %d", p, got, end)
		}
	}
	got := storedIDs(t, conn, namespace)
	if len(got) != len(ids) {
		t.Fatalf("stored %d unique spans, want %d", len(got), len(ids))
	}
	for id := range ids {
		if !got[id] {
			t.Fatalf("missing span %s", id)
		}
	}
}

// Simulate an ambiguous partial insert: ClickHouse durably receives the first
// row, but the worker never receives success for the whole two-row cohort.
// The record must remain uncommitted and a full replay must be duplicate-safe.
type ambiguousBatchStore struct {
	inner  *storage.Store
	stored chan struct{}
	once   sync.Once
}

func (a *ambiguousBatchStore) Write(ctx context.Context, rows []trace.Row) error {
	return a.WriteBatch(ctx, rows)
}
func (a *ambiguousBatchStore) WriteBatch(ctx context.Context, rows []trace.Row) error {
	if len(rows) != 2 {
		return fmt.Errorf("whole cohort has %d rows, want 2", len(rows))
	}
	if err := a.inner.WriteBatch(ctx, rows[:1]); err != nil {
		return err
	}
	a.once.Do(func() { close(a.stored) })
	<-ctx.Done()
	return ctx.Err()
}

func TestPartialBatchWriteAmbiguityReplaysWholeCohort(t *testing.T) {
	conn := requireStack(t)
	mergeCtx, mergeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := conn.Exec(mergeCtx, "SYSTEM STOP MERGES incidentlens.spans"); err != nil {
		mergeCancel()
		t.Fatal(err)
	}
	mergeCancel()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := conn.Exec(ctx, "SYSTEM START MERGES incidentlens.spans"); err != nil {
			t.Errorf("restore merges: %v", err)
		}
	})
	admin, topic := isolatedV2Topic(t)
	publisher, err := stream.NewProducer([]string{"127.0.0.1:19092"}, topic)
	if err != nil {
		t.Fatal(err)
	}
	defer publisher.Close()
	namespace := "streamtest-partial-" + seed(t)
	req, id, _ := fixture(t, namespace, 0)
	for _, attr := range req.ResourceSpans[0].Resource.Attributes {
		if attr.Key == "service.namespace" {
			attr.Value = &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: namespace}}
		}
	}
	original := req.ResourceSpans[0].ScopeSpans[0].Spans[0]
	second := *original
	second.SpanId = append([]byte(nil), original.SpanId...)
	second.SpanId[len(second.SpanId)-1] ^= 0xff
	second.Name = "stream.operation.second"
	req.ResourceSpans[0].ScopeSpans[0].Spans = append(req.ResourceSpans[0].ScopeSpans[0].Spans, &second)
	server := stream.NewServer(publisher)
	publishCtx, publishCancel := context.WithTimeout(context.Background(), 12*time.Second)
	_, err = server.Export(publishCtx, req)
	publishCancel()
	if err != nil {
		t.Fatal(err)
	}
	partition, err := stream.PartitionForTraceID(id)
	if err != nil {
		t.Fatal(err)
	}
	if end := topicEndPartition(t, admin, topic, partition); end != 1 {
		t.Fatalf("published offsets=%d, want one two-row record", end)
	}
	store, err := storage.Open("127.0.0.1:19000", "ingest", "local-ingest", "incidentlens")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ambiguous := &ambiguousBatchStore{inner: store, stored: make(chan struct{})}
	group := "streamtest-partial-" + seed(t)
	worker, err := stream.NewWorker([]string{"127.0.0.1:19092"}, topic, group, ambiguous)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(runCtx) }()
	joined := false
	defer func() {
		stop()
		worker.Close()
		if !joined {
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("failed worker did not stop")
			}
		}
	}()
	select {
	case <-ambiguous.stored:
	case <-time.After(20 * time.Second):
		t.Fatal("partial insert not exercised")
	}
	if got := committedPartition(t, admin, topic, group, partition); got > 0 {
		t.Fatalf("offset %d committed before whole write completed", got)
	}
	stop()
	select {
	case <-done:
		joined = true
	case <-time.After(10 * time.Second):
		t.Fatal("failed worker did not stop")
	}
	if got := committedPartition(t, admin, topic, group, partition); got > 0 {
		t.Fatalf("partial insert committed offset %d", got)
	}
	if got := stored(t, conn, id, namespace); got != 1 {
		t.Fatalf("partial insert has %d logical spans, want 1", got)
	}
	replay, err := stream.NewWorker([]string{"127.0.0.1:19092"}, topic, group, store)
	if err != nil {
		t.Fatal(err)
	}
	replayCtx, replayStop := context.WithCancel(context.Background())
	replayDone := make(chan error, 1)
	go func() { replayDone <- replay.Run(replayCtx) }()
	replayJoined := false
	defer func() {
		replayStop()
		replay.Close()
		if !replayJoined {
			select {
			case <-replayDone:
			case <-time.After(10 * time.Second):
				t.Error("replay worker did not stop")
			}
		}
	}()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		if committedPartition(t, admin, topic, group, partition) == 1 {
			replayStop()
			<-replayDone
			replayJoined = true
			break
		}
		select {
		case err := <-replayDone:
			replayJoined = true
			t.Fatalf("replay worker exited: %v", err)
		case <-time.After(200 * time.Millisecond):
		}
	}
	if got := committedPartition(t, admin, topic, group, partition); got != 1 {
		t.Fatalf("replay committed %d, want 1", got)
	}
	if got := stored(t, conn, id, namespace); got != 2 {
		t.Fatalf("replay has %d FINAL spans, want 2", got)
	}
	if got := rawNamespaceRows(t, conn, namespace); got < 3 {
		t.Fatalf("ambiguous first row was not physically replayed: raw rows %d", got)
	}
}
