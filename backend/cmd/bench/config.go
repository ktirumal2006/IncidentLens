package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"math"
	"strings"
	"time"
)

const spansPerTrace = 6
const maxPlannedSpans = 2000000

type Config struct {
	OutputDir            string         `json:"output_dir"`
	Seed                 string         `json:"seed"`
	Scenario             string         `json:"scenario"`
	Namespace            string         `json:"namespace"`
	Rate                 int            `json:"rate_spans_per_second"`
	Duration             time.Duration  `json:"duration_ns"`
	Warmup               time.Duration  `json:"warmup_ns"`
	Concurrency          int            `json:"export_concurrency"`
	BatchTraces          int            `json:"batch_traces"`
	Queue                int            `json:"export_queue"`
	Attempts             int            `json:"max_attempts"`
	ExportTimeout        time.Duration  `json:"export_timeout_ns"`
	Drain                time.Duration  `json:"drain_ns"`
	Settle               time.Duration  `json:"settle_ns"`
	EstimatedEventBytes  int64          `json:"estimated_event_bytes_upper_bound"`
	QueryRate            float64        `json:"query_requests_per_second"`
	QueryConcurrency     int            `json:"query_concurrency"`
	QueryMix             string         `json:"query_mix"`
	AttributeBytes       int            `json:"attribute_payload_bytes"`
	BaselineTraces       int            `json:"baseline_traces"`
	Collector            string         `json:"collector"`
	API                  string         `json:"api"`
	ClickHouse           string         `json:"clickhouse"`
	CHUser               string         `json:"clickhouse_user"`
	CHPassword           string         `json:"-"`
	VisibilityEvery      int            `json:"visibility_every_batches"`
	VisibilityTimeout    time.Duration  `json:"visibility_timeout_ns"`
	MaxOutputBytes       int64          `json:"max_event_bytes"`
	LoadTraces           int            `json:"load_traces"`
	LoadSpans            int            `json:"load_spans"`
	SetupSpans           int            `json:"setup_spans"`
	SpanKinds            map[string]int `json:"span_kinds_per_trace"`
	SerializedBatchBytes int            `json:"serialized_full_batch_bytes"`
	Services             []string       `json:"services"`
}

func parseConfig(args []string) (Config, error) {
	c := Config{}
	f := flag.NewFlagSet("bench", flag.ContinueOnError)
	f.StringVar(&c.OutputDir, "output-dir", "", "new output directory (required)")
	f.StringVar(&c.Seed, "seed", "", "unique deterministic run seed (required)")
	f.StringVar(&c.Scenario, "scenario", "steady", "scenario label")
	f.IntVar(&c.Rate, "rate", 500, "offered spans/second")
	f.DurationVar(&c.Duration, "duration", 30*time.Second, "measurement duration")
	f.DurationVar(&c.Warmup, "warmup", 10*time.Second, "warmup duration")
	f.IntVar(&c.Concurrency, "concurrency", 16, "export workers")
	f.IntVar(&c.BatchTraces, "batch-traces", 32, "traces/export, maximum42")
	f.IntVar(&c.Queue, "queue", 64, "bounded pending export batches")
	f.IntVar(&c.Attempts, "attempts", 3, "maximum attempts/batch")
	f.DurationVar(&c.ExportTimeout, "export-timeout", 8*time.Second, "deadline/export attempt")
	f.DurationVar(&c.Drain, "drain", 120*time.Second, "maximum graceful export drain")
	f.DurationVar(&c.Settle, "settle", 35*time.Second, "separate post-attempt Collector retry settling")
	f.Float64Var(&c.QueryRate, "query-rate", 0, "total paced HTTP requests/second,0 disables")
	f.IntVar(&c.QueryConcurrency, "query-concurrency", 4, "HTTP workers")
	f.StringVar(&c.QueryMix, "query-mix", "services,traces,detail,incidents", "round-robin route mix")
	f.IntVar(&c.AttributeBytes, "attribute-bytes", 256, "test.payload string bytes/span")
	f.IntVar(&c.BaselineTraces, "baseline-traces", 0, "separate setup traces atnow-20min")
	f.StringVar(&c.Collector, "collector", "127.0.0.1:4317", "Collector OTLP/gRPC address")
	f.StringVar(&c.API, "api", "http://127.0.0.1:18081", "API base URL")
	f.StringVar(&c.ClickHouse, "clickhouse", "127.0.0.1:19000", "native ClickHouse address")
	f.StringVar(&c.CHUser, "clickhouse-user", "query", "SELECT-only reconciliation user")
	f.StringVar(&c.CHPassword, "clickhouse-password", "local-query", "local reconciliation password(not persisted)")
	f.IntVar(&c.VisibilityEvery, "visibility-every", 10, "sample first trace every N attempted load batches; 0 disables")
	f.DurationVar(&c.VisibilityTimeout, "visibility-timeout", 10*time.Second, "bounded sampled trace polling")
	f.Int64Var(&c.MaxOutputBytes, "max-event-bytes", 256<<20, "hard JSONL disk cap")
	if e := f.Parse(args); e != nil {
		return c, e
	}
	if f.NArg() != 0 {
		return c, errors.New("unexpected positional arguments")
	}
	if e := c.validate(); e != nil {
		return c, e
	}
	h := sha256.Sum256([]byte(c.Seed))
	c.Namespace = "bench-" + hex.EncodeToString(h[:6])
	c.LoadTraces = int(float64(c.Rate) * float64(c.Duration+c.Warmup) / float64(time.Second) / spansPerTrace)
	c.LoadSpans = c.LoadTraces * spansPerTrace
	c.SetupSpans = c.BaselineTraces * spansPerTrace
	if c.LoadSpans+c.SetupSpans > maxPlannedSpans {
		return c, fmt.Errorf("planned spans exceed%d", maxPlannedSpans)
	}
	c.SpanKinds = map[string]int{"SERVER": 3, "CLIENT": 2, "INTERNAL": 1}
	c.Services = []string{"bench-frontend", "bench-api", "bench-store"}
	batches := batchCount(c.LoadTraces+c.BaselineTraces, c.BatchTraces)
	samples := 0
	if c.VisibilityEvery > 0 {
		samples = (batchCount(c.LoadTraces, c.BatchTraces) + c.VisibilityEvery - 1) / c.VisibilityEvery
		if samples > 2048 {
			samples = 2048
		}
	}
	c.EstimatedEventBytes = int64(c.LoadTraces+c.BaselineTraces)*256 + int64(batches)*(int64(c.Attempts)*1200+3200) + int64(math.Ceil(c.QueryRate*(c.Duration+c.Warmup).Seconds()))*1400 + int64(samples)*(int64(c.VisibilityTimeout/(200*time.Millisecond))+2)*1400
	if c.EstimatedEventBytes > c.MaxOutputBytes {
		return c, fmt.Errorf("estimated raw events%d bytes exceed cap%d", c.EstimatedEventBytes, c.MaxOutputBytes)
	}
	return c, nil
}
func (c Config) validate() error {
	if c.OutputDir == "" || c.Seed == "" || len(c.Seed) > 128 || len(c.Scenario) > 128 || c.Rate < 1 || c.Rate > 100000 || c.Duration <= 0 || c.Duration > time.Hour || c.Warmup < 0 || c.Warmup > 10*time.Minute || c.Concurrency < 1 || c.Concurrency > 64 || c.BatchTraces < 1 || c.BatchTraces > 42 || c.Queue < 1 || c.Queue > 1024 || c.Attempts < 1 || c.Attempts > 5 || c.ExportTimeout <= 0 || c.ExportTimeout > time.Minute || c.Drain <= 0 || c.Drain > 3*time.Minute || math.IsNaN(c.QueryRate) || math.IsInf(c.QueryRate, 0) || c.QueryRate < 0 || c.QueryRate > 1000 || c.QueryConcurrency < 1 || c.QueryConcurrency > 64 || c.AttributeBytes < 0 || c.AttributeBytes > 4096 || c.BaselineTraces < 0 || c.BaselineTraces > 10000 || c.Settle < 0 || c.Settle > time.Minute || c.VisibilityEvery < 0 || c.VisibilityTimeout <= 0 || c.VisibilityTimeout > 30*time.Second || c.MaxOutputBytes < 1<<20 || c.MaxOutputBytes > 256<<20 {
		return errors.New("configuration exceeds harness bounds")
	}
	for _, route := range strings.Split(c.QueryMix, ",") {
		switch route {
		case "services", "traces", "detail", "incidents":
		default:
			return fmt.Errorf("unsupported query route%q", route)
		}
	}
	return nil
}
func batchCount(traces, batch int) int { return (traces + batch - 1) / batch }
func scheduledAt(start time.Time, firstLoadTrace, rate int) time.Time {
	return start.Add(time.Duration(int64(firstLoadTrace) * spansPerTrace * int64(time.Second) / int64(rate)))
}
