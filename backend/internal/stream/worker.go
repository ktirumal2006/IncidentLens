package stream

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"incidentlens/backend/internal/trace"
)

type RowWriter interface {
	Write(context.Context, []trace.Row) error
}

type Worker struct {
	Client     *kgo.Client
	Topic      string
	Writer     RowWriter
	Now        func() time.Time
	AfterWrite func(*kgo.Record, []trace.Row) // test-only crash boundary; nil in normal operation
}

func NewWorker(brokers []string, topic, group string, writer RowWriter) (*Worker, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topic), kgo.ConsumerGroup(group),
		kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(),
		kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()),
		kgo.ConsumeResetOffset(kgo.NoResetOffset()),
		// Kafka returns the first batch even when it exceeds these one-byte
		// requested budgets. This allows one valid <=5 MiB producer batch to
		// make progress without packing many compressed batches in a fetch.
		kgo.FetchMaxBytes(1), kgo.FetchMaxPartitionBytes(1),
		kgo.MaxDecompressBatchBytes(5<<20), kgo.BrokerMaxReadBytes(8<<20),
		kgo.FetchMaxWait(500*time.Millisecond), kgo.MaxConcurrentFetches(0),
	)
	if err != nil {
		return nil, err
	}
	return &Worker{Client: client, Topic: topic, Writer: writer, Now: time.Now}, nil
}

func (w *Worker) Close()                         { w.Client.Close() }
func (w *Worker) Ping(ctx context.Context) error { return CheckTopic(ctx, w.Client, w.Topic) }

func (w *Worker) Run(ctx context.Context) error {
	fetchBackoff := 250 * time.Millisecond
	for ctx.Err() == nil {
		fetches := w.Client.PollFetches(ctx)
		var fetchErr error
		fetches.EachError(func(topic string, partition int32, err error) {
			if fetchErr == nil {
				fetchErr = fmt.Errorf("fetch %s/%d: %w", topic, partition, err)
			}
		})
		for _, record := range fetches.Records() {
			rows, err := Decode(record.Value, w.Now())
			if err != nil {
				w.Client.AllowRebalance()
				return fmt.Errorf("poison record %s/%d offset %d: %w", record.Topic, record.Partition, record.Offset, err)
			}
			if err = w.retry(ctx, false, func(op context.Context) error { return w.Writer.Write(op, rows) }); err != nil {
				w.Client.AllowRebalance()
				return err
			}
			if w.AfterWrite != nil {
				w.AfterWrite(record, rows)
			}
			if err = w.retry(ctx, true, func(op context.Context) error { return w.Client.CommitRecords(op, record) }); err != nil {
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
				return fetchErr
			}
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
	}
	return ctx.Err()
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
	// PollFetches can report a lost group session after a long idle period.
	// The client can rejoin on the next poll; no record from this fetch is
	// skipped because Run processes records before handling the fetch error.
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

func recoverableCommit(err error) bool {
	// After a write, an ownership/generation error means this member must not
	// keep committing the old record while BlockRebalanceOnPoll holds a rebalance.
	// Exiting leaves the stored record eligible for replay from the last commit.
	return !membershipChanged(err) && recoverableFetch(err)
}

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
