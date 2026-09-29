package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	collectorpb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type Batch struct {
	ID, First, Count                    int
	Phase                               string
	Scheduled, EventTime                time.Time
	Offered, Enqueued, Generated, Acked bool
	Attempts                            int
	Rejected                            int64
	FirstSend, Finished                 time.Time
	AckNS                               atomic.Int64
	DropReason, FinalStatus             string
}
type Known struct {
	TraceID   string
	EventTime time.Time
}
type Harness struct {
	config               Config
	rec                  *Recorder
	client               collectorpb.TraceServiceClient
	ctx                  context.Context
	latest               atomic.Pointer[Known]
	batches              []*Batch
	start, offerFinished time.Time
	visibility           chan Probe
	probeWG              sync.WaitGroup
	probeMu              sync.Mutex
	probes               []ProbeResult
}

func retryable(code codes.Code) bool {
	switch code {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted:
		return true
	}
	return false
}
func retryDelay(attempt int) time.Duration {
	d := 250 * time.Millisecond * time.Duration(1<<uint(attempt-1))
	if d > 2*time.Second {
		d = 2 * time.Second
	}
	return d
}
func waitUntil(ctx context.Context, t time.Time) bool {
	d := time.Until(t)
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return ctx.Err() == nil
	case <-ctx.Done():
		return false
	}
}
func (h *Harness) export(b *Batch) {
	if h.ctx.Err() != nil {
		return
	}
	req := makeRequest(h.config, b.First, b.Count, b.EventTime, b.Phase == "setup")
	b.Generated = true
	size := proto.Size(req)
	if h.rec.record(Event{Kind: "generated", Phase: b.Phase, Batch: b.ID, FirstTrace: b.First, Traces: b.Count, Started: time.Now().UTC(), RequestBytes: size}) != nil {
		return
	}
	for attempt := 1; attempt <= h.config.Attempts; attempt++ {
		if h.ctx.Err() != nil {
			return
		}
		started := time.Now().UTC()
		if b.FirstSend.IsZero() {
			b.FirstSend = started
			h.sample(b)
		}
		b.Attempts++
		ctx, cancel := context.WithTimeout(h.ctx, h.config.ExportTimeout)
		res, err := h.client.Export(ctx, req)
		cancel()
		finished := time.Now().UTC()
		b.Finished = finished
		code := status.Code(err)
		rejected := int64(0)
		if res != nil && res.PartialSuccess != nil {
			rejected = res.PartialSuccess.RejectedSpans
		}
		b.Rejected = rejected
		b.FinalStatus = code.String()
		if rejected > 0 {
			b.FinalStatus = "partial_success"
		}
		willRetry := err != nil && retryable(code) && attempt < h.config.Attempts && h.ctx.Err() == nil
		event := Event{Kind: "export_attempt", Phase: b.Phase, Batch: b.ID, FirstTrace: b.First, Traces: b.Count, Attempt: attempt, Scheduled: b.Scheduled, Started: started, Finished: finished, LatencyNS: finished.Sub(started).Nanoseconds(), SchedulingDelayNS: started.Sub(b.Scheduled).Nanoseconds(), Status: code.String(), RejectedSpans: rejected, Retryable: retryable(code), WillRetry: willRetry, RequestBytes: size}
		if err != nil {
			event.Error = err.Error()
		}
		if rejected > 0 {
			event.Status = "partial_success"
		}
		if err == nil && rejected == 0 {
			b.Acked = true
			b.AckNS.Store(finished.UnixNano())
			h.latest.Store(&Known{traceHex(h.config.Seed, b.First), b.EventTime})
		}
		if h.rec.record(event) != nil {
			return
		}
		if !willRetry || !waitUntil(h.ctx, finished.Add(retryDelay(attempt))) {
			return
		}
	}
}
func (h *Harness) sample(b *Batch) {
	if b.Phase == "setup" || h.config.VisibilityEvery == 0 {
		return
	}
	index := (b.First - h.config.BaselineTraces) / h.config.BatchTraces
	if index%h.config.VisibilityEvery != 0 {
		return
	}
	probe := Probe{Batch: b, TraceID: traceHex(h.config.Seed, b.First), EventTime: b.EventTime, Started: b.FirstSend}
	if index/h.config.VisibilityEvery >= 2048 {
		_ = h.rec.record(Event{Kind: "visibility", Phase: b.Phase, Batch: b.ID, Started: b.FirstSend, Status: "sample_cap", Censored: true})
		return
	}
	select {
	case h.visibility <- probe:
	default:
		_ = h.rec.record(Event{Kind: "visibility", Phase: b.Phase, Batch: b.ID, TraceID: probe.TraceID, Started: b.FirstSend, Status: "probe_queue_drop", Censored: true})
	}
}
func (h *Harness) buildLoad(start time.Time) []*Batch {
	out := []*Batch{}
	setup := batchCount(h.config.BaselineTraces, h.config.BatchTraces)
	for i := 0; i < batchCount(h.config.LoadTraces, h.config.BatchTraces); i++ {
		first := i * h.config.BatchTraces
		count := h.config.BatchTraces
		if count > h.config.LoadTraces-first {
			count = h.config.LoadTraces - first
		}
		scheduled := scheduledAt(start, first, h.config.Rate)
		phase := "measured"
		if scheduled.Before(start.Add(h.config.Warmup)) {
			phase = "warmup"
		}
		out = append(out, &Batch{ID: setup + i, First: h.config.BaselineTraces + first, Count: count, Phase: phase, Scheduled: scheduled, EventTime: scheduled.Add(-time.Second)})
	}
	return out
}
func (h *Harness) schedule(ctx context.Context, tasks []*Batch, queue chan<- *Batch) {
	period := time.Duration(int64(h.config.BatchTraces) * spansPerTrace * int64(time.Second) / int64(h.config.Rate))
	deadline := period
	if deadline < 10*time.Millisecond {
		deadline = 10 * time.Millisecond
	}
	for _, b := range tasks {
		if !waitUntil(ctx, b.Scheduled) {
			return
		}
		now := time.Now().UTC()
		b.Offered = true
		if h.rec.record(Event{Kind: "scheduled", Phase: b.Phase, Batch: b.ID, FirstTrace: b.First, Traces: b.Count, Scheduled: b.Scheduled, Started: now, SchedulingDelayNS: now.Sub(b.Scheduled).Nanoseconds()}) != nil {
			return
		}
		if now.Sub(b.Scheduled) > deadline {
			b.DropReason = "missed_deadline"
			_ = h.rec.record(Event{Kind: "schedule_drop", Phase: b.Phase, Batch: b.ID, FirstTrace: b.First, Traces: b.Count, Scheduled: b.Scheduled, Started: now, Status: "missed_deadline", SchedulingDelayNS: now.Sub(b.Scheduled).Nanoseconds()})
			continue
		}
		select {
		case queue <- b:
			b.Enqueued = true
			_ = h.rec.record(Event{Kind: "enqueued", Phase: b.Phase, Batch: b.ID, FirstTrace: b.First, Traces: b.Count, Scheduled: b.Scheduled, Started: now})
		default:
			b.DropReason = "queue_full"
			_ = h.rec.record(Event{Kind: "schedule_drop", Phase: b.Phase, Batch: b.ID, FirstTrace: b.First, Traces: b.Count, Scheduled: b.Scheduled, Started: now, Status: "queue_full"})
		}
	}
	waitUntil(ctx, h.start.Add(h.config.Warmup+h.config.Duration))
}
func main() {
	c, e := parseConfig(os.Args[1:])
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(2)
	}
	if e = run(c); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run(c Config) error {
	if e := os.Mkdir(c.OutputDir, 0700); e != nil {
		return fmt.Errorf("output directory must be new: %w", e)
	}
	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	offerCtx, cancelOffer := context.WithCancel(signalCtx)
	defer cancelOffer()
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	rec, e := newRecorder(filepath.Join(c.OutputDir, "events.jsonl"), c.MaxOutputBytes, func() { cancelOffer(); cancelRun() })
	if e != nil {
		return e
	}
	h := &Harness{config: c, rec: rec, ctx: runCtx, visibility: make(chan Probe, 64)}
	done := make(chan struct{})
	defer close(done)
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-signalCtx.Done():
				cancelRun()
				cancelOffer()
				_ = rec.flush()
				return
			case <-ticker.C:
				_ = rec.flush()
			case <-done:
				return
			}
		}
	}()
	conn, e := grpc.NewClient(c.Collector, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if e != nil {
		_ = rec.close()
		return e
	}
	defer conn.Close()
	h.client = collectorpb.NewTraceServiceClient(conn)
	c.SerializedBatchBytes = proto.Size(makeRequest(c, c.BaselineTraces, c.BatchTraces, time.Now().UTC().Add(-time.Second), false))
	h.config = c
	if e = writeJSON(filepath.Join(c.OutputDir, "config.json"), c); e != nil {
		_ = rec.close()
		return e
	}
	if e = h.preflight(); e != nil {
		_ = rec.close()
		_ = writeJSON(filepath.Join(c.OutputDir, "summary.json"), map[string]any{"error": e.Error(), "config": c})
		return e
	}
	h.startProbes()
	setupTime := time.Now().UTC().Add(-20 * time.Minute)
	for i := 0; i < batchCount(c.BaselineTraces, c.BatchTraces); i++ {
		count := min(c.BatchTraces, c.BaselineTraces-i*c.BatchTraces)
		h.batches = append(h.batches, &Batch{ID: i, First: i * c.BatchTraces, Count: count, Phase: "setup", EventTime: setupTime})
	}
	for _, b := range h.batches {
		if offerCtx.Err() != nil {
			e = errors.New("setup cancelled; load not started")
			break
		}
		b.Scheduled = time.Now().UTC()
		b.Offered = true
		b.Enqueued = true
		h.export(b)
		if !b.Acked {
			e = errors.New("setup not fully acknowledged; load not started")
			break
		}
	}
	h.start = time.Now().UTC()
	tasks := h.buildLoad(h.start)
	h.batches = append(h.batches, tasks...)
	workers := sync.WaitGroup{}
	queue := make(chan *Batch, c.Queue)
	for i := 0; i < c.Concurrency; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for b := range queue {
				h.export(b)
			}
		}()
	}
	queryDone := make(<-chan struct{})
	if e == nil {
		queryDone = h.startQueries(offerCtx)
	} else {
		closed := make(chan struct{})
		close(closed)
		queryDone = closed
	}
	if e == nil {
		if err := h.rec.record(Event{Kind: "load_start", Started: h.start}); err != nil {
			e = err
		}
		_ = h.rec.flush()
		h.schedule(offerCtx, tasks, queue)
	}
	h.offerFinished = time.Now().UTC()
	cancelOffer()
	close(queue)
	drainExpired := atomic.Bool{}
	drainTimer := time.AfterFunc(c.Drain, func() { drainExpired.Store(true); cancelRun() })
	workers.Wait()
	<-queryDone
	close(h.visibility)
	h.probeWG.Wait()
	drainTimer.Stop()
	settled := false
	if signalCtx.Err() != nil {
		e = errors.New("interrupted: active workload cancelled")
	} else if drainExpired.Load() {
		e = errors.New("drain deadline expired")
	} else if runCtx.Err() != nil {
		e = errors.New("workload cancelled after recording failure")
	} else {
		settled = waitUntil(signalCtx, time.Now().Add(c.Settle))
		if !settled {
			e = errors.New("interrupted during Collector settling")
		}
	}
	h.finishProbes()
	h.finishBatches()
	reconciliation := h.reconcile()
	reconciliation.Settled = settled
	if !settled && reconciliation.Error == "" {
		reconciliation.Verified = false
		reconciliation.Error = "unsettled snapshot: queued Collector writes may remain"
	}
	closeErr := rec.close()
	summary := h.summary(reconciliation, drainExpired.Load(), e, closeErr)
	if writeErr := writeJSON(filepath.Join(c.OutputDir, "summary.json"), summary); writeErr != nil {
		return writeErr
	}
	stdout, _ := json.Marshal(summary)
	fmt.Println(string(stdout))
	if closeErr != nil {
		return closeErr
	}
	if e != nil {
		return e
	}
	if reconciliation.Error != "" {
		return errors.New(reconciliation.Error)
	}
	return nil
}

func (h *Harness) finishBatches() {
	for _, b := range h.batches {
		if !b.Offered {
			continue
		}
		state := "not_emitted"
		if b.Attempts > 0 {
			state = "terminal_error"
		}
		if b.Rejected > 0 {
			state = "partial_success"
		}
		ack := time.Time{}
		if b.Acked {
			state = "acknowledged"
			ack = time.Unix(0, b.AckNS.Load()).UTC()
		}
		latency := int64(0)
		if b.Acked {
			latency = ack.Sub(b.FirstSend).Nanoseconds()
		} else if !b.FirstSend.IsZero() {
			latency = b.Finished.Sub(b.FirstSend).Nanoseconds()
		}
		_ = h.rec.record(Event{Kind: "batch_complete", Phase: b.Phase, Batch: b.ID, FirstTrace: b.First, Traces: b.Count, Attempt: b.Attempts, Scheduled: b.Scheduled, Started: b.FirstSend, Finished: b.Finished, Ack: ack, Status: state, Error: b.DropReason, RejectedSpans: b.Rejected, LatencyNS: latency})
	}
}
