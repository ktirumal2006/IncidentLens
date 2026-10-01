package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"incidentlens/backend/internal/stream"
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
	brokers := strings.Split(env("KAFKA_BROKERS", "127.0.0.1:19092"), ",")
	producer, err := stream.NewProducer(brokers, env("KAFKA_TOPIC", stream.Topic))
	if err != nil {
		return err
	}
	defer producer.Close()
	listener, err := net.Listen("tcp", env("STREAM_GRPC_ADDRESS", "127.0.0.1:14318"))
	if err != nil {
		return err
	}
	defer listener.Close()
	grpcServer := stream.NewGRPCServer(producer)
	mux := http.NewServeMux()
	mux.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := producer.Ping(ctx); err != nil {
			http.Error(w, "broker unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	health := &http.Server{Addr: env("STREAM_HEALTH_ADDRESS", "127.0.0.1:18082"), Handler: mux, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8192}
	errs := make(chan error, 2)
	go func() { errs <- grpcServer.Serve(listener) }()
	go func() { errs <- health.ListenAndServe() }()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log.Printf("stream ingestion listening on %s", listener.Addr())
	select {
	case <-ctx.Done():
	case err = <-errs:
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
	}
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
