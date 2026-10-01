package main

import (
	"context"
	"errors"
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
	store, err := clickhouse.Open(env("CLICKHOUSE_ADDRESS", "127.0.0.1:19000"), env("CLICKHOUSE_USER", "ingest"), env("CLICKHOUSE_PASSWORD", "local-ingest"), "incidentlens")
	if err != nil {
		return err
	}
	defer store.Close()
	worker, err := stream.NewWorker(strings.Split(env("KAFKA_BROKERS", "127.0.0.1:19092"), ","), env("KAFKA_TOPIC", stream.Topic), env("KAFKA_GROUP_ID", "incidentlens-stream-worker-v1"), store)
	if err != nil {
		return err
	}
	defer worker.Close()
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
	mux := http.NewServeMux()
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
	go func() { errs <- health.ListenAndServe() }()
	go func() { errs <- worker.Run(ctx) }()
	log.Print("stream worker started")
	var err error
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
