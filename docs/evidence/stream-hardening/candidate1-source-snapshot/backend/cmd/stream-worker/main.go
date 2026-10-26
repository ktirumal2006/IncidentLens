package main

import (
	"context"
	"errors"
	"incidentlens/backend/internal/benchobs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"incidentlens/backend/internal/storage/clickhouse"
	"incidentlens/backend/internal/stream"
	"incidentlens/backend/internal/streamstatus"
	"incidentlens/backend/internal/trace"
)

func env(key, fallback string) string {
	if s := os.Getenv(key); s != "" {
		return s
	}
	return fallback
}
func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
func run() error {
	closeObservations, observationErr := benchobs.FromEnv()
	if observationErr != nil {
		return observationErr
	}
	defer func() {
		if err := closeObservations(); err != nil {
			log.Print(err)
		}
	}()

	store, err := clickhouse.Open(env("CLICKHOUSE_ADDRESS", "127.0.0.1:19000"), env("CLICKHOUSE_USER", "ingest"), env("CLICKHOUSE_PASSWORD", "local-ingest"), "incidentlens")
	if err != nil {
		return err
	}
	defer store.Close()
	worker, err := stream.NewWorker(strings.Split(env("KAFKA_BROKERS", "127.0.0.1:19092"), ","), env("KAFKA_TOPIC", stream.Topic), env("KAFKA_GROUP_ID", "incidentlens-stream-worker-v2"), store)
	if err != nil {
		return err
	}
	defer worker.Close()
	startup, cancelStartup := context.WithTimeout(context.Background(), 5*time.Second)
	err = worker.Ping(startup)
	cancelStartup()
	if err != nil {
		return err
	}
	// Enabled only in an explicitly named integration-test process. Abrupt exit
	// proves that a stored but uncommitted record is replayed on restart.
	if target := os.Getenv("STREAM_TEST_EXIT_AFTER_WRITE_TRACE_ID"); target != "" {
		worker.AfterWrite = func(_ *kgo.Record, rows []trace.Row) {
			for _, row := range rows {
				if row.TraceID == target {
					os.Exit(86)
				}
			}
		}
	}
	return serve(worker, store)
}

type pinger interface{ Ping(context.Context) error }

func serve(worker *stream.Worker, store pinger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	probe, err := streamstatus.NewKafkaProbe(strings.Split(env("KAFKA_BROKERS", "127.0.0.1:19092"), ","), worker.Topic, env("KAFKA_GROUP_ID", "incidentlens-stream-worker-v2"))
	if err != nil {
		return err
	}
	defer probe.Close()
	sampler := streamstatus.New(probe, func() streamstatus.Progress { return statusProgress(worker.Progress()) })
	samplerDone := make(chan struct{})
	go func() { defer close(samplerDone); sampler.Run(ctx) }()
	defer func() { stop(); <-samplerDone }()
	mux := http.NewServeMux()
	mux.Handle("/internal/stream-status", sampler.Handler())
	mux.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		check, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if worker.Ping(check) != nil || store.Ping(check) != nil {
			http.Error(w, "dependency unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	health := &http.Server{Addr: env("STREAM_HEALTH_ADDRESS", "127.0.0.1:18083"), Handler: mux, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8192}
	errs := make(chan error, 2)
	workerDone := make(chan struct{})
	go func() { errs <- health.ListenAndServe() }()
	go func() { defer close(workerDone); errs <- worker.Run(ctx) }()
	// The worker releases blocked partition ownership after its canceled writes
	// settle. Join it before closing its Kafka client or ClickHouse connection.
	defer func() { stop(); <-workerDone }()
	log.Print("stream worker started")
	err = nil
	select {
	case <-ctx.Done():
	case err = <-errs:
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
	}
	stop()
	shutdown, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = health.Shutdown(shutdown)
	return err
}

// Translate local worker state without exposing dependency error text. Zero
// timestamps mean no completed observation, not an epoch-time measurement.
func statusProgress(p stream.ProgressSnapshot) streamstatus.Progress {
	r := streamstatus.Progress{State: "failed", ErrorCode: "worker_failed"}
	switch p.State {
	case "idle":
		r.State, r.ErrorCode = "idle", ""
	case "processing":
		r.State, r.ErrorCode = "running", ""
	case "retrying_write":
		r.State, r.ErrorCode = "retrying", "storage_unavailable"
	case "retrying_commit", "retrying_fetch":
		r.State, r.ErrorCode = "retrying", "broker_unavailable"
	}
	if p.ErrorCode == "poison_record" || p.ErrorCode == "routing_violation" {
		r.ErrorCode = "poison_record"
	}
	if p.ProcessingAgeUnknownReason == "clock_skew" && r.ErrorCode == "" {
		r.ErrorCode = "clock_skew"
	}
	if !p.LastWriteAt.IsZero() {
		r.LastWriteAt = &p.LastWriteAt
		if p.LastCompletedProcessingAgeNS >= 0 {
			r.ProcessingDelayNS = &p.LastCompletedProcessingAgeNS
		}
	}
	if !p.LastCommitAt.IsZero() {
		r.LastCommitAt = &p.LastCommitAt
	}
	if !p.OldestPendingReceipt.IsZero() {
		r.PendingSince = &p.OldestPendingReceipt
	}
	return r
}
