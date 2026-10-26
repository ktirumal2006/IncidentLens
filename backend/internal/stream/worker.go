package stream

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"incidentlens/backend/internal/benchobs"
	"incidentlens/backend/internal/trace"
)

const (
	CohortWait       = 50 * time.Millisecond
	CohortTargetRows = 2048
	CohortMaxRows    = 4096
	CohortMaxBytes   = 8 << 20
	CohortMaxRecords = 64
)

type RowWriter interface {
	Write(context.Context, []trace.Row) error
}
type BatchWriter interface {
	WriteBatch(context.Context, []trace.Row) error
}

type ProgressSnapshot struct {
	State                        string    `json:"state"`
	ErrorCode                    string    `json:"error_code,omitempty"`
	LastWriteAt                  time.Time `json:"last_write_at,omitempty"`
	LastCommitAt                 time.Time `json:"last_commit_at,omitempty"`
	LastCompletedProcessingAgeNS int64     `json:"last_completed_processing_age_ns"`
	ProcessingAgeUnknownReason   string    `json:"processing_age_unknown_reason,omitempty"`
	OldestPendingReceipt         time.Time `json:"oldest_pending_receipt,omitempty"`
	PendingRecords               int       `json:"pending_records"`
	PendingRows                  int       `json:"pending_rows"`
}

type Worker struct {
	Client     *kgo.Client
	Topic      string
	Writer     RowWriter
	Now        func() time.Time
	AfterWrite func(*kgo.Record, []trace.Row)              // test-only crash boundary
	Commit     func(context.Context, ...*kgo.Record) error // test seam; nil uses Kafka client
	mu         sync.RWMutex
	progress   ProgressSnapshot
}

func NewWorker(brokers []string, topic, group string, writer RowWriter) (*Worker, error) {
	if ExpectedPartitions(topic) == 0 {
		return nil, fmt.Errorf("unsupported stream topic %q", topic)
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topic), kgo.ConsumerGroup(group),
		kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(),
		kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()), kgo.ConsumeResetOffset(kgo.NoResetOffset()),
		// Kafka returns the first batch despite these one-byte requested budgets.
		// A response can include one such <=5 MiB decoded batch per partition;
		// processing remains poll-gated. The fetched response may contain more
		// than 64 records; processFetched splits it into bounded write cohorts.
		kgo.FetchMaxBytes(1), kgo.FetchMaxPartitionBytes(1),
		kgo.MaxDecompressBatchBytes(5<<20), kgo.BrokerMaxReadBytes(8<<20),
		kgo.FetchMaxWait(500*time.Millisecond), kgo.MaxConcurrentFetches(0),
	)
	if err != nil {
		return nil, err
	}
	return &Worker{Client: client, Topic: topic, Writer: writer, Now: time.Now, progress: ProgressSnapshot{State: "idle"}}, nil
}
func (w *Worker) Close()                         { w.Client.Close() }
func (w *Worker) Ping(ctx context.Context) error { return CheckTopic(ctx, w.Client, w.Topic) }
func (w *Worker) Progress() ProgressSnapshot     { w.mu.RLock(); defer w.mu.RUnlock(); return w.progress }
func (w *Worker) setProgress(change func(*ProgressSnapshot)) {
	w.mu.Lock()
	change(&w.progress)
	w.mu.Unlock()
}

type decodedRecord struct {
	record *kgo.Record
	rows   []trace.Row
}

func (w *Worker) Run(ctx context.Context) error {
	fetchBackoff := 250 * time.Millisecond
	for ctx.Err() == nil {
		fetches := w.Client.PollFetches(ctx)
		records := fetches.Records()
		fetchErr := firstFetchError(fetches)
		if len(records) > 0 && fetchErr == nil {
			// Build a short synchronous cohort across polls. No fetch is detached
			// and rebalance stays blocked until every fetched record is resolved.
			deadline := time.Now().Add(CohortWait)
			bytes := 0
			for _, r := range records {
				bytes += len(r.Value)
			}
			for bytes < CohortMaxBytes/2 && len(records) < CohortMaxRecords && time.Now().Before(deadline) {
				pollCtx, cancel := context.WithDeadline(ctx, deadline)
				more := w.Client.PollFetches(pollCtx)
				cancel()
				moreErr := firstFetchError(more)
				additional := more.Records()
				for _, r := range additional {
					bytes += len(r.Value)
				}
				records = append(records, additional...)
				if moreErr != nil && !errors.Is(moreErr, context.DeadlineExceeded) {
					fetchErr = moreErr
					break
				}
				if len(additional) == 0 {
					break
				}
			}
		}
		if len(records) > 0 {
			if err := w.processFetched(ctx, records); err != nil {
				w.Client.AllowRebalance()
				return err
			}
		}
		w.Client.AllowRebalance()
		if fetchErr != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if !recoverableFetch(fetchErr) {
				w.setProgress(func(p *ProgressSnapshot) { p.State = "error"; p.ErrorCode = "fetch_fatal" })
				return fetchErr
			}
			w.setProgress(func(p *ProgressSnapshot) { p.State = "retrying_fetch"; p.ErrorCode = "fetch_unavailable" })
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(fetchBackoff):
			}
			if fetchBackoff < 5*time.Second {
				fetchBackoff *= 2
				if fetchBackoff > 5*time.Second {
					fetchBackoff = 5 * time.Second
				}
			}
			continue
		}
		fetchBackoff = 250 * time.Millisecond
		w.setProgress(func(p *ProgressSnapshot) {
			if p.PendingRecords == 0 {
				p.State = "idle"
				p.ErrorCode = ""
			}
		})
	}
	return ctx.Err()
}

func firstFetchError(fetches kgo.Fetches) error {
	var first error
	fetches.EachError(func(topic string, partition int32, err error) {
		if first == nil {
			first = fmt.Errorf("fetch %s/%d: %w", topic, partition, err)
		}
	})
	return first
}

func (w *Worker) processFetched(ctx context.Context, records []*kgo.Record) error {
	cohort := make([]decodedRecord, 0, min(len(records), CohortMaxRecords))
	rows, bytes := 0, 0
	flush := func() error {
		if len(cohort) == 0 {
			return nil
		}
		err := w.writeAndCommit(ctx, cohort)
		cohort = cohort[:0]
		rows = 0
		bytes = 0
		return err
	}
	for _, record := range records {
		decoded, err := Decode(record.Value, w.Now())
		if err != nil {
			w.setProgress(func(p *ProgressSnapshot) { p.State = "error"; p.ErrorCode = "poison_record" })
			return fmt.Errorf("poison record %s/%d offset %d: %w", record.Topic, record.Partition, record.Offset, err)
		}
		if ExpectedPartitions(w.Topic) == PartitionCount {
			for _, row := range decoded {
				partition, err := PartitionForTraceID(row.TraceID)
				if err != nil || partition != record.Partition {
					w.setProgress(func(p *ProgressSnapshot) { p.State = "error"; p.ErrorCode = "routing_violation" })
					return fmt.Errorf("poison routing %s/%d offset %d", record.Topic, record.Partition, record.Offset)
				}
			}
		}
		if len(cohort) >= CohortMaxRecords || rows+len(decoded) > CohortMaxRows || bytes+len(record.Value) > CohortMaxBytes {
			if err := flush(); err != nil {
				return err
			}
		}
		cohort = append(cohort, decodedRecord{record, decoded})
		rows += len(decoded)
		bytes += len(record.Value)
		if rows >= CohortTargetRows || bytes >= CohortMaxBytes {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

func (w *Worker) writeAndCommit(ctx context.Context, cohort []decodedRecord) error {
	rowCount := 0
	for _, item := range cohort {
		rowCount += len(item.rows)
	}
	batch := make([]trace.Row, 0, rowCount)
	last := make(map[int32]*kgo.Record)
	oldest := time.Time{}
	for _, item := range cohort {
		partition := item.record.Partition
		batch = append(batch, item.rows...)
		last[partition] = item.record
		for _, row := range item.rows {
			if oldest.IsZero() || row.IngestedAt.Before(oldest) {
				oldest = row.IngestedAt
			}
		}
	}
	total := len(batch)
	w.setProgress(func(p *ProgressSnapshot) {
		p.State = "processing"
		p.ErrorCode = ""
		p.PendingRecords = len(cohort)
		p.PendingRows = total
		p.OldestPendingReceipt = oldest
	})
	err := w.retry(ctx, false, func(op context.Context) error {
		if writer, ok := w.Writer.(BatchWriter); ok {
			return writer.WriteBatch(op, batch)
		}
		return w.Writer.Write(op, batch)
	})
	if err != nil {
		w.setProgress(func(p *ProgressSnapshot) { p.State = "error"; p.ErrorCode = "write_failed" })
		return err
	}
	completedWrite := w.Now().UTC()
	age := completedWrite.Sub(oldest).Nanoseconds()
	unknownReason := ""
	if age < 0 {
		age = -1
		unknownReason = "clock_skew"
	}
	w.setProgress(func(p *ProgressSnapshot) {
		p.LastWriteAt = completedWrite
		p.LastCompletedProcessingAgeNS = age
		p.ProcessingAgeUnknownReason = unknownReason
	})
	for _, item := range cohort {
		if w.AfterWrite != nil {
			w.AfterWrite(item.record, item.rows)
		}
	}
	commits := make([]*kgo.Record, 0, len(last))
	offsets := make(map[int32]int64, len(last))
	for partition := int32(0); partition < int32(ExpectedPartitions(w.Topic)); partition++ {
		if record := last[partition]; record != nil {
			commits = append(commits, record)
			offsets[partition] = record.Offset + 1
		}
	}
	err = w.retry(ctx, true, func(op context.Context) error {
		started := benchobs.Start()
		commit := w.Commit
		if commit == nil {
			commit = w.Client.CommitRecords
		}
		result := commit(op, commits...)
		benchobs.Finish(started, benchobs.Event{Kind: "consumer_commit", Rows: total, Success: result == nil, Offsets: offsets})
		return result
	})
	if err != nil {
		w.setProgress(func(p *ProgressSnapshot) { p.State = "error"; p.ErrorCode = "commit_failed" })
		return err
	}
	finished := w.Now().UTC()
	w.setProgress(func(p *ProgressSnapshot) {
		p.State = "idle"
		p.ErrorCode = ""
		p.LastCommitAt = finished
		p.OldestPendingReceipt = time.Time{}
		p.PendingRecords = 0
		p.PendingRows = 0
	})
	return nil
}

func recoverableFetch(err error) bool {
	if err == nil {
		return false
	}
	var dataLoss *kgo.ErrDataLoss
	var tooLarge *kgo.ErrDecompressTooLarge
	if errors.As(err, &dataLoss) || errors.As(err, &tooLarge) || errors.Is(err, kerr.OffsetOutOfRange) || errors.Is(err, kgo.ErrClientClosed) {
		return false
	}
	if membershipChanged(err) {
		return true
	}
	if kerr.IsRetriable(err) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var session *kgo.ErrGroupSession
	if errors.As(err, &session) {
		return recoverableFetch(session.Err)
	}
	var netErr net.Error
	return errors.As(err, &netErr)
}
func membershipChanged(err error) bool {
	return errors.Is(err, kerr.UnknownMemberID) || errors.Is(err, kerr.IllegalGeneration) || errors.Is(err, kerr.RebalanceInProgress)
}
func recoverableCommit(err error) bool { return !membershipChanged(err) && recoverableFetch(err) }
func (w *Worker) retry(ctx context.Context, kafkaCommit bool, op func(context.Context) error) error {
	backoff := 250 * time.Millisecond
	for {
		attempt, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := op(attempt)
		cancel()
		if err == nil {
			return nil
		}
		if kafkaCommit && !recoverableCommit(err) {
			return fmt.Errorf("non-retryable offset commit failure: %w", err)
		}
		if kafkaCommit {
			w.setProgress(func(p *ProgressSnapshot) { p.State = "retrying_commit"; p.ErrorCode = "commit_unavailable" })
		} else {
			w.setProgress(func(p *ProgressSnapshot) { p.State = "retrying_write"; p.ErrorCode = "write_unavailable" })
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("operation canceled after failure: %w", err)
		case <-time.After(backoff):
		}
		if backoff < 5*time.Second {
			backoff *= 2
			if backoff > 5*time.Second {
				backoff = 5 * time.Second
			}
		}
	}
}
