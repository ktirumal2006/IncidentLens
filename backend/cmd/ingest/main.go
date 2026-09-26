// ingest accepts trace-only OTLP/gRPC and acknowledges synchronous storage writes.
package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"incidentlens/backend/internal/ingest"
	"incidentlens/backend/internal/storage/clickhouse"
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	store, err := clickhouse.Open(env("CLICKHOUSE_ADDRESS", "127.0.0.1:19000"),
		env("CLICKHOUSE_USER", "ingest"), env("CLICKHOUSE_PASSWORD", "local-ingest"), "incidentlens")
	if err != nil {
		return err
	}
	defer store.Close()
	listener, err := net.Listen("tcp", env("GRPC_ADDRESS", "127.0.0.1:14317"))
	if err != nil {
		return err
	}
	defer listener.Close()
	grpcServer := ingest.NewGRPCServer(store)
	mux := http.NewServeMux()
	mux.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := store.Ping(ctx); err != nil {
			http.Error(w, "storage unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	health := &http.Server{Addr: env("HEALTH_ADDRESS", "127.0.0.1:18080"), Handler: mux,
		ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 3 * time.Second,
		WriteTimeout: 3 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8192}
	errs := make(chan error, 2)
	go func() { errs <- grpcServer.Serve(listener) }()
	go func() { errs <- health.ListenAndServe() }()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log.Printf("trace ingestion listening on %s", listener.Addr())
	select {
	case <-ctx.Done():
	case err = <-errs:
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
	}
	// Drain active writes, but do not hang shutdown on a broken client/dependency.
	done := make(chan struct{})
	go func() { grpcServer.GracefulStop(); close(done) }()
	select {
	case <-done:
	case <-time.After(6 * time.Second):
		grpcServer.Stop()
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = health.Shutdown(shutdown)
	return err
}
