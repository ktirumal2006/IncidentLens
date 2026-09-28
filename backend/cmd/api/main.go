package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"incidentlens/backend/internal/detector"
	"incidentlens/backend/internal/httpapi"
	"incidentlens/backend/internal/storage/clickhouse"
)

func env(k, f string) string {
	if s := os.Getenv(k); s != "" {
		return s
	}
	return f
}
func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
func run() error {
	cfg, err := detector.ConfigFromEnv(os.Getenv)
	if err != nil {
		return err
	}
	store, err := clickhouse.OpenQuery(env("CLICKHOUSE_ADDRESS", "127.0.0.1:19000"), env("CLICKHOUSE_USER", "query"), env("CLICKHOUSE_PASSWORD", "local-query"), "incidentlens")
	if err != nil {
		return err
	}
	defer store.Close()
	server := &http.Server{Addr: env("HTTP_ADDRESS", "127.0.0.1:18081"), Handler: httpapi.NewHandlerWithDetector(store, store, cfg, env("FRONTEND_DIR", "")), ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 7 * time.Second, WriteTimeout: 7 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8192}
	errs := make(chan error, 1)
	go func() { errs <- server.ListenAndServe() }()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	select {
	case <-ctx.Done():
	case err = <-errs:
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdown)
	return err
}
