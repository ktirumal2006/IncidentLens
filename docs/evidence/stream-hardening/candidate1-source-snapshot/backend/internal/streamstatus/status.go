// Package streamstatus reports bounded, read-only Kafka consumer progress.
package streamstatus

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"sync"
	"time"
)

const (
	ExpectedPartitions = 4
	SampleInterval     = 2 * time.Second
	SampleDeadline     = 2 * time.Second
	StaleAfter         = 10 * time.Second
	MaxResponseBytes   = 4096
)

type Partition struct {
	ID        int32
	LogStart  int64
	End       int64
	Committed int64
}

type Observation struct {
	Partitions []Partition
	// ReasonCode is a safe, fixed identifier, never a broker error string.
	ReasonCode string
}

type Probe interface {
	Observe(context.Context) Observation
}

type Progress struct {
	LastWriteAt       *time.Time
	LastCommitAt      *time.Time
	ProcessingDelayNS *int64
	PendingSince      *time.Time
	State             string // idle, running, retrying, failed
	ErrorCode         string // safe worker-local code only
}

type Response struct {
	Mode               string     `json:"mode"`
	State              string     `json:"state"`
	LagRecords         *int64     `json:"lag_records"`
	PartitionsTotal    int        `json:"partitions_total"`
	PartitionsObserved int        `json:"partitions_observed"`
	SampledAt          *time.Time `json:"sampled_at"`
	SampleStartedAt    *time.Time `json:"sample_started_at"`
	SampleFinishedAt   *time.Time `json:"sample_finished_at"`
	LastWriteAt        *time.Time `json:"last_write_at"`
	LastCommitAt       *time.Time `json:"last_commit_at"`
	ProcessingDelayNS  *string    `json:"processing_delay_ns"`
	PendingAgeNS       *string    `json:"pending_age_ns"`
	ProcessingState    string     `json:"processing_state"`
	ReasonCode         string     `json:"reason_code"`
	Scope              string     `json:"scope"`
}

type Sampler struct {
	Probe    Probe
	Progress func() Progress
	Now      func() time.Time
	mu       sync.RWMutex
	last     Response
}

func New(probe Probe, progress func() Progress) *Sampler {
	return &Sampler{Probe: probe, Progress: progress, Now: time.Now}
}

func unavailable(reason string) Response {
	return Response{Mode: "streaming", State: "unavailable", PartitionsTotal: ExpectedPartitions,
		ProcessingState: "idle", ReasonCode: reason, Scope: "consumer_group_lag_and_local_worker_progress"}
}

func safeState(s string) string {
	switch s {
	case "idle", "running", "retrying", "failed":
		return s
	default:
		return "failed"
	}
}

func safeCode(s string) string {
	switch s {
	case "", "worker_retrying", "worker_failed", "storage_unavailable", "broker_unavailable", "poison_record", "offset_gap", "ownership_lost", "clock_skew":
		return s
	default:
		return "worker_error"
	}
}

func evaluate(parts []Partition) (int64, string, bool) {
	if len(parts) != ExpectedPartitions {
		return 0, "partition_coverage_incomplete", false
	}
	seen := [ExpectedPartitions]bool{}
	var lag int64
	emptyWithoutCommit := false
	for _, p := range parts {
		if p.ID < 0 || p.ID >= ExpectedPartitions || seen[p.ID] {
			return 0, "partition_coverage_inconsistent", false
		}
		seen[p.ID] = true
		if p.LogStart < 0 || p.End < p.LogStart {
			return 0, "offsets_inconsistent", false
		}
		if p.Committed < 0 {
			if p.Committed == -1 && p.LogStart == 0 && p.End == 0 {
				// Zero broker records are certain, but no consumer progress is known.
				emptyWithoutCommit = true
				continue
			}
			return 0, "offset_uninitialized", false
		}
		if p.Committed < p.LogStart {
			return 0, "retained_offset_gap", false
		}
		if p.Committed > p.End {
			return 0, "offsets_inconsistent", false
		}
		delta := p.End - p.Committed
		const maxInt64 = int64(^uint64(0) >> 1)
		if lag > maxInt64-delta {
			return 0, "offsets_inconsistent", false
		}
		lag += delta
	}
	return lag, "", emptyWithoutCommit
}

func (s *Sampler) Sample(ctx context.Context) Response {
	now := s.Now().UTC()
	r := unavailable("sample_unavailable")
	r.SampleStartedAt = &now
	if s.Probe != nil {
		bound, cancel := context.WithTimeout(ctx, SampleDeadline)
		observation := s.Probe.Observe(bound)
		if bound.Err() != nil {
			observation = Observation{ReasonCode: "offsets_unavailable"}
		}
		cancel()
		r.PartitionsObserved = len(observation.Partitions)
		if observation.ReasonCode != "" {
			r.ReasonCode = safeProbeCode(observation.ReasonCode)
		} else if lag, reason, emptyWithoutCommit := evaluate(observation.Partitions); reason != "" {
			r.ReasonCode = reason
		} else {
			r.LagRecords = &lag
			r.ReasonCode = ""
			if emptyWithoutCommit {
				r.ReasonCode = "empty_partition_uninitialized"
			}
			r.State = "ok"
			if lag > 0 {
				r.State = "backlog"
			}
		}
	}
	// Capture progress before closing the observation interval. A write can
	// complete while the sampler reads it; its timestamp is then part of this
	// sample rather than being mistaken for a clock jump.
	var progress Progress
	if s.Progress != nil {
		progress = s.Progress()
	}
	finished := s.Now().UTC()
	r.SampleFinishedAt, r.SampledAt = &finished, &finished
	if finished.Before(now) {
		r.State, r.LagRecords, r.ReasonCode = "unavailable", nil, "clock_skew"
	}
	if s.Progress != nil {
		p := progress
		r.LastWriteAt, r.LastCommitAt = p.LastWriteAt, p.LastCommitAt
		r.ProcessingState = safeState(p.State)
		if p.ProcessingDelayNS != nil && *p.ProcessingDelayNS >= 0 {
			value := strconv.FormatInt(*p.ProcessingDelayNS, 10)
			r.ProcessingDelayNS = &value
		}
		if p.PendingSince != nil && !p.PendingSince.After(finished) {
			value := strconv.FormatInt(finished.Sub(*p.PendingSince).Nanoseconds(), 10)
			r.PendingAgeNS = &value
		}
		if (p.PendingSince != nil && p.PendingSince.After(finished)) ||
			(p.LastWriteAt != nil && p.LastWriteAt.After(finished)) ||
			(p.LastCommitAt != nil && p.LastCommitAt.After(finished)) {
			r.State, r.LagRecords, r.ReasonCode = "unavailable", nil, "clock_skew"
		}
		if code := safeCode(p.ErrorCode); code != "" && r.ReasonCode == "" {
			r.ReasonCode = code
		}
		if r.ProcessingState == "failed" {
			r.State, r.LagRecords = "unavailable", nil
			if r.ReasonCode == "" {
				r.ReasonCode = "worker_failed"
			}
		}
	}
	s.mu.Lock()
	s.last = r
	s.mu.Unlock()
	return r
}

func safeProbeCode(s string) string {
	switch s {
	case "broker_unavailable", "topic_unavailable", "metadata_unavailable", "offsets_unavailable", "partition_coverage_incomplete", "partition_coverage_inconsistent", "offset_uninitialized", "retained_offset_gap", "offsets_inconsistent":
		return s
	default:
		return "sample_unavailable"
	}
}

func (s *Sampler) Current() Response {
	s.mu.RLock()
	r := s.last
	s.mu.RUnlock()
	if r.Mode == "" {
		return unavailable("sample_unavailable")
	}
	age := time.Duration(0)
	if r.SampleFinishedAt != nil {
		age = s.Now().Sub(*r.SampleFinishedAt)
	}
	if age < 0 {
		r.State, r.LagRecords, r.ReasonCode = "unavailable", nil, "clock_skew"
	} else if r.SampleFinishedAt == nil || age > StaleAfter {
		r.State, r.LagRecords, r.ReasonCode = "stale", nil, "sample_stale"
	}
	return r
}

func (s *Sampler) Run(ctx context.Context) {
	s.Sample(ctx)
	ticker := time.NewTicker(SampleInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.Sample(ctx)
		}
	}
}

// Handler exposes only fixed-schema status; it never gates dependency readiness.
func (s *Sampler) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/stream-status" {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		b, err := json.Marshal(s.Current())
		if err != nil || len(b) > MaxResponseBytes {
			http.Error(w, "status unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(b)
	})
}
