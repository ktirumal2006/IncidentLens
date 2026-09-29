package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"
)

type Event struct {
	Kind              string    `json:"kind"`
	Phase             string    `json:"phase,omitempty"`
	Batch             int       `json:"batch"`
	FirstTrace        int       `json:"first_trace"`
	Traces            int       `json:"traces"`
	Attempt           int       `json:"attempt,omitempty"`
	Scheduled         time.Time `json:"scheduled,omitempty"`
	Started           time.Time `json:"started,omitempty"`
	Finished          time.Time `json:"finished,omitempty"`
	LatencyNS         int64     `json:"latency_ns,omitempty"`
	SchedulingDelayNS int64     `json:"scheduling_delay_ns,omitempty"`
	Status            string    `json:"status,omitempty"`
	HTTPStatus        int       `json:"http_status,omitempty"`
	Route             string    `json:"route,omitempty"`
	Error             string    `json:"error,omitempty"`
	RejectedSpans     int64     `json:"rejected_spans,omitempty"`
	Retryable         bool      `json:"retryable,omitempty"`
	WillRetry         bool      `json:"will_retry,omitempty"`
	RequestBytes      int       `json:"request_bytes,omitempty"`
	ResponseBytes     int64     `json:"response_bytes,omitempty"`
	TraceID           string    `json:"trace_id,omitempty"`
	Ack               time.Time `json:"ack,omitempty"`
	LastMiss          time.Time `json:"last_miss,omitempty"`
	AckToVisibleNS    int64     `json:"ack_to_visible_ns,omitempty"`
	StartToVisibleNS  int64     `json:"start_to_visible_ns,omitempty"`
	Polls             int       `json:"polls,omitempty"`
	Censored          bool      `json:"censored,omitempty"`
}
type Latency struct {
	Count int   `json:"count"`
	P50NS int64 `json:"p50_ns"`
	P95NS int64 `json:"p95_ns"`
	P99NS int64 `json:"p99_ns"`
	MaxNS int64 `json:"max_ns"`
}

func percentiles(values []int64) Latency {
	if len(values) == 0 {
		return Latency{}
	}
	v := append([]int64{}, values...)
	sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
	pick := func(p int) int64 {
		i := len(v) * p / 100
		if i >= len(v) {
			i = len(v) - 1
		}
		return v[i]
	}
	return Latency{len(v), pick(50), pick(95), pick(99), v[len(v)-1]}
}

type Recorder struct {
	mu           sync.Mutex
	file         *os.File
	writer       *bufio.Writer
	limit, bytes int64
	err          error
	cancel       func()
	latency      map[string][]int64
	counts       map[string]int
	status       map[string]int
}

func newRecorder(path string, limit int64, cancel func()) (*Recorder, error) {
	f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return nil, e
	}
	return &Recorder{file: f, writer: bufio.NewWriterSize(f, 64<<10), limit: limit, cancel: cancel, latency: map[string][]int64{}, counts: map[string]int{}, status: map[string]int{}}, nil
}
func (r *Recorder) record(e Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	b, err := json.Marshal(e)
	if err == nil && r.bytes+int64(len(b)+1) > r.limit {
		err = errors.New("JSONL event byte cap reached; workload cancelled")
	}
	if err == nil {
		_, err = r.writer.Write(append(b, '\n'))
	}
	if err != nil {
		r.err = err
		r.cancel()
		return err
	}
	r.bytes += int64(len(b) + 1)
	r.counts[e.Kind+":"+e.Phase]++
	if e.Kind == "export_attempt" || e.Kind == "query" {
		key := e.Kind + ":" + e.Phase + ":" + e.Route
		r.latency[key] = append(r.latency[key], e.LatencyNS)
		r.status[key+":"+e.Status]++
		class := e.Status
		if e.Kind == "query" && e.HTTPStatus > 0 {
			class = fmt.Sprintf("%dxx", e.HTTPStatus/100)
		}
		r.latency[key+":"+class] = append(r.latency[key+":"+class], e.LatencyNS)
	}
	if e.Kind == "export_attempt" {
		r.latency["schedule_delay:"+e.Phase] = append(r.latency["schedule_delay:"+e.Phase], e.SchedulingDelayNS)
	}
	return nil
}
func (r *Recorder) flush() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	if e := r.writer.Flush(); e != nil {
		r.err = e
		r.cancel()
		return e
	}
	return nil
}
func (r *Recorder) close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.writer.Flush()
	if e == nil {
		e = r.file.Sync()
	}
	closeErr := r.file.Close()
	if r.err != nil {
		return r.err
	}
	if e != nil {
		return e
	}
	return closeErr
}
func writeJSON(path string, v any) error {
	b, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		return e
	}
	return os.WriteFile(path, append(b, '\n'), 0600)
}
