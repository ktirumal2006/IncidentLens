// Package benchobs records opt-in, bounded benchmark operation timings.
// It is not a product telemetry ingestion pipeline.
package benchobs

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

const MaxBytes = 64 << 20

type Event struct {
	Kind       string          `json:"kind"`
	StartedNS  int64           `json:"started_ns"`
	FinishedNS int64           `json:"finished_ns"`
	DurationNS int64           `json:"duration_ns"`
	Rows       int             `json:"rows"`
	Bytes      int             `json:"bytes,omitempty"`
	Success    bool            `json:"success"`
	PrepareNS  int64           `json:"prepare_ns"`
	AppendNS   int64           `json:"append_ns"`
	SendNS     int64           `json:"send_ns"`
	Offset     int64           `json:"offset"`
	Partition  int32           `json:"partition"`
	Offsets    map[int32]int64 `json:"offsets,omitempty"`
}

type Recorder struct {
	mu      sync.Mutex
	file    *os.File
	bytes   int
	limit   int
	count   int
	dropped int
	failure bool
	closed  bool
}

// active is configured once before any server goroutines start and never changed.
var active *Recorder

type rowsKey struct{}

func WithRows(ctx context.Context, rows int) context.Context {
	if active == nil {
		return ctx
	}
	return context.WithValue(ctx, rowsKey{}, rows)
}
func Rows(ctx context.Context) int { n, _ := ctx.Value(rowsKey{}).(int); return n }

func FromEnv() (func() error, error) {
	path := os.Getenv("BENCH_OPS_FILE")
	if path == "" {
		return func() error { return nil }, nil
	}
	r, err := Open(path, MaxBytes)
	if err != nil {
		return nil, err
	}
	active = r
	return r.Close, nil
}

func Open(path string, limit int) (*Recorder, error) {
	if limit < 2048 || limit > MaxBytes {
		return nil, fmt.Errorf("invalid operation log bound")
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	return &Recorder{file: f, limit: limit}, nil
}

func Start() time.Time {
	if active == nil {
		return time.Time{}
	}
	return time.Now()
}
func Elapsed(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return time.Since(t).Nanoseconds()
}
func Finish(start time.Time, e Event) {
	if active == nil || start.IsZero() {
		return
	}
	end := time.Now()
	e.StartedNS = start.UnixNano()
	e.FinishedNS = end.UnixNano()
	e.DurationNS = end.Sub(start).Nanoseconds()
	active.Record(e)
}

// Disk writing is outside the recorded operation interval. It still adds observer
// overhead to the workload; direct and streaming use the same write recorder.
func (r *Recorder) Record(e Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	b, err := json.Marshal(e)
	if err != nil || r.failure || r.bytes+len(b)+1 > r.limit-1024 {
		r.dropped++
		return
	}
	b = append(b, '\n')
	n, err := r.file.Write(b)
	r.bytes += n
	if err != nil || n != len(b) {
		r.failure = true
		r.dropped++
		return
	}
	r.count++
}

func (r *Recorder) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	// The reserved trailer makes missing observations explicit. A missing trailer
	// (crash or disk failure) also invalidates completeness in the analyzer.
	b, _ := json.Marshal(map[string]any{"kind": "recorder_end", "events": r.count, "dropped": r.dropped, "write_failed": r.failure})
	_, writeErr := r.file.Write(append(b, '\n'))
	closeErr := r.file.Close()
	if r.failure || r.dropped > 0 {
		return fmt.Errorf("operation evidence incomplete: dropped=%d write_failed=%t", r.dropped, r.failure)
	}
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}
