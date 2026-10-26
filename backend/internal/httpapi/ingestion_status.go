package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// IngestionStatus is an observation of a consumer group, not a promise that
// every acknowledged span is already queryable.
type IngestionStatus struct {
	Mode               string     `json:"mode"`
	State              string     `json:"state"`
	Scope              string     `json:"scope"`
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
	ReasonCode         string     `json:"reason_code,omitempty"`
}

const statusScope = "consumer_group_lag_and_local_worker_progress"

func directStatus() IngestionStatus {
	return IngestionStatus{Mode: "direct", State: "not_applicable", Scope: statusScope}
}
func unavailableStatus(reason string) IngestionStatus {
	return IngestionStatus{Mode: "streaming", State: "unavailable", Scope: statusScope, ReasonCode: reason}
}

// A fixed, validated internal URL avoids turning the public route into a proxy.
// The mutex also coalesces simultaneous browser requests into one bounded fetch.
type IngestionStatusProxy struct {
	url      string
	client   *http.Client
	mu       sync.Mutex
	cached   IngestionStatus
	cachedAt time.Time
	Now      func() time.Time
}

func NewIngestionStatusProxy(rawURL string) (*IngestionStatusProxy, error) {
	u, err := url.Parse(rawURL)
	if err != nil || u.Scheme != "http" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "/internal/stream-status" {
		return nil, &url.Error{Op: "parse", URL: rawURL, Err: errInvalidStatusURL{}}
	}
	return &IngestionStatusProxy{url: u.String(), client: &http.Client{Timeout: 1500 * time.Millisecond, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, Now: time.Now}, nil
}

type errInvalidStatusURL struct{}

func (errInvalidStatusURL) Error() string {
	return "STREAM_STATUS_URL must be a fixed http URL ending /internal/stream-status"
}

func (p *IngestionStatusProxy) Get() IngestionStatus {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.Now().UTC()
	if !p.cachedAt.IsZero() && now.Sub(p.cachedAt) < 2*time.Second {
		return p.statusAt(now)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, p.url, nil)
	resp, err := p.client.Do(req)
	if err != nil {
		p.cached = unavailableStatus("worker_unreachable")
		p.cachedAt = now
		return p.cached
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		p.cached = unavailableStatus("worker_unavailable")
		p.cachedAt = now
		return p.cached
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024+1))
	if err != nil || len(data) > 64*1024 {
		p.cached = unavailableStatus("invalid_response")
		p.cachedAt = now
		return p.cached
	}
	var status IngestionStatus
	if err := json.Unmarshal(data, &status); err != nil || !validStatus(status, now) {
		p.cached = unavailableStatus("invalid_response")
		p.cachedAt = now
		return p.cached
	}
	status.ReasonCode = safeReasonCode(status.ReasonCode)
	p.cached, p.cachedAt = status, now
	return p.statusAt(now)
}

func (p *IngestionStatusProxy) statusAt(now time.Time) IngestionStatus {
	s := p.cached
	if s.SampledAt != nil && now.Sub(*s.SampledAt) > 10*time.Second {
		s.State, s.LagRecords, s.ReasonCode = "stale", nil, "sample_stale"
	}
	return s
}

func validStatus(s IngestionStatus, now time.Time) bool {
	if s.Mode != "streaming" || s.Scope != statusScope {
		return false
	}
	if s.PartitionsTotal != 4 || s.PartitionsObserved < 0 || s.PartitionsObserved > 4 {
		return false
	}
	if s.ProcessingState != "idle" && s.ProcessingState != "running" && s.ProcessingState != "retrying" && s.ProcessingState != "failed" {
		return false
	}
	if !validDecimal(s.ProcessingDelayNS) || !validDecimal(s.PendingAgeNS) {
		return false
	}
	if s.State == "ok" || s.State == "backlog" {
		if s.ProcessingState == "failed" {
			return false
		}
		if s.LagRecords == nil || *s.LagRecords < 0 || s.SampledAt == nil || s.SampleStartedAt == nil || s.SampleFinishedAt == nil || s.PartitionsObserved != s.PartitionsTotal {
			return false
		}
		if s.SampleStartedAt.After(*s.SampleFinishedAt) || s.SampledAt.Before(*s.SampleStartedAt) || s.SampledAt.After(*s.SampleFinishedAt) {
			return false
		}
		if s.SampledAt.After(now.Add(2 * time.Second)) {
			return false
		}
		return (s.State == "ok") == (*s.LagRecords == 0)
	}
	if s.State != "unavailable" && s.State != "stale" {
		return false
	}
	return s.LagRecords == nil
}

func safeReasonCode(code string) string {
	switch code {
	case "", "broker_unavailable", "topic_unavailable", "metadata_unavailable", "offsets_unavailable", "partition_coverage_incomplete", "partition_coverage_inconsistent", "offset_uninitialized", "empty_partition_uninitialized", "retained_offset_gap", "offset_gap", "offsets_inconsistent", "sample_unavailable", "sample_stale", "worker_retrying", "worker_failed", "storage_unavailable", "poison_record", "ownership_lost", "worker_error", "clock_skew":
		return code
	default:
		return "status_unknown"
	}
}

func validDecimal(value *string) bool {
	if value == nil {
		return true
	}
	if *value == "" || strings.Trim(*value, "0123456789") != "" {
		return false
	}
	_, err := strconv.ParseUint(*value, 10, 64)
	return err == nil
}
