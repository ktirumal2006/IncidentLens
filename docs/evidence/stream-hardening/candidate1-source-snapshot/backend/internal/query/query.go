package query

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	MaxPage     = 500
	MaxDetail   = 2048
	MaxResponse = 8 << 20
)

var ErrInvalid = errors.New("invalid request")
var ErrTooLarge = errors.New("query resource cap")
var ErrNotFound = errors.New("trace not found")

type Window struct {
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
}
type Filter struct {
	Window
	Service     *string `json:"service,omitempty"`
	Namespace   *string `json:"namespace,omitempty"`
	Operation   *string `json:"operation,omitempty"`
	MinDuration *uint64 `json:"min_duration_ns,omitempty"`
	MaxDuration *uint64 `json:"max_duration_ns,omitempty"`
	Status      *uint8  `json:"status,omitempty"`
	Limit       int     `json:"limit"`
	Cursor      string  `json:"-"`
}
type Summary struct {
	Namespace  string  `json:"service_namespace"`
	Service    string  `json:"service_name"`
	Operation  string  `json:"operation"`
	Count      uint64  `json:"span_count"`
	ErrorCount uint64  `json:"error_count"`
	UnsetCount uint64  `json:"unset_count"`
	ErrorRate  float64 `json:"error_rate"`
	UnsetRate  float64 `json:"unset_rate"`
	P50        string  `json:"p50_duration_ns"`
	P95        string  `json:"p95_duration_ns"`
}
type Summaries struct {
	ObservedAt time.Time `json:"observed_at"`
	Window
	Truncated bool      `json:"truncated"`
	Services  []Summary `json:"services"`
}
type TraceMatch struct {
	TraceID       string    `json:"trace_id"`
	MatchingStart time.Time `json:"matching_start_time"`
	Count         uint64    `json:"matching_span_count"`
	MinDuration   string    `json:"matching_min_duration_ns"`
	MaxDuration   string    `json:"matching_max_duration_ns"`
	ErrorCount    uint64    `json:"matching_error_count"`
}
type SearchResult struct {
	ObservedAt      time.Time `json:"observed_at"`
	IngestionCutoff time.Time `json:"ingestion_cutoff"`
	Window
	Traces     []TraceMatch `json:"traces"`
	NextCursor string       `json:"next_cursor"`
}
type Span struct {
	SpanID                         string          `json:"span_id"`
	ParentSpanID                   string          `json:"parent_span_id"`
	ServiceName                    string          `json:"service_name"`
	ServiceNamespace               string          `json:"service_namespace"`
	SpanName                       string          `json:"span_name"`
	SpanKind                       uint8           `json:"span_kind"`
	StatusCode                     uint8           `json:"status_code"`
	StatusMessage                  string          `json:"status_message"`
	StartTime                      time.Time       `json:"start_time"`
	EndTime                        time.Time       `json:"end_time"`
	DurationNS                     string          `json:"duration_ns"`
	IngestedAt                     time.Time       `json:"ingested_at"`
	ResourceAttributes             json.RawMessage `json:"resource_attributes"`
	SpanAttributes                 json.RawMessage `json:"span_attributes"`
	ScopeAttributes                json.RawMessage `json:"scope_attributes"`
	Events                         json.RawMessage `json:"events"`
	Links                          json.RawMessage `json:"links"`
	ScopeName                      string          `json:"scope_name"`
	ScopeVersion                   string          `json:"scope_version"`
	ResourceSchemaURL              string          `json:"resource_schema_url"`
	ScopeSchemaURL                 string          `json:"scope_schema_url"`
	TraceState                     string          `json:"trace_state"`
	TraceFlags                     uint32          `json:"trace_flags"`
	DroppedAttributesCount         uint32          `json:"dropped_attributes_count"`
	DroppedEventsCount             uint32          `json:"dropped_events_count"`
	DroppedLinksCount              uint32          `json:"dropped_links_count"`
	ResourceDroppedAttributesCount uint32          `json:"resource_dropped_attributes_count"`
	ScopeDroppedAttributesCount    uint32          `json:"scope_dropped_attributes_count"`
}
type DetailResult struct {
	ObservedAt time.Time `json:"observed_at"`
	Window
	TraceID           string    `json:"trace_id"`
	Spans             []Span    `json:"spans"`
	Truncated         bool      `json:"truncated"`
	TruncationReason  string    `json:"truncation_reason"`
	MissingParentIDs  []string  `json:"missing_parent_ids"`
	RootCount         int       `json:"root_count"`
	HasMissingRoot    bool      `json:"has_missing_root"`
	HasCycles         bool      `json:"has_cycles"`
	HasSourceDrops    bool      `json:"has_source_drops"`
	ObservedStartTime time.Time `json:"observed_start_time"`
	ObservedEndTime   time.Time `json:"observed_end_time"`
	ObservedElapsedNS string    `json:"observed_elapsed_ns"`
}
type Store interface {
	Ping(context.Context) error
	Services(context.Context, Filter, time.Time) (Summaries, error)
	Search(context.Context, Filter, time.Time) (SearchResult, error)
	Detail(context.Context, Window, string, time.Time) (DetailResult, error)
}

func ValidTraceID(id string) bool {
	if len(id) != 32 {
		return false
	}
	b, e := hex.DecodeString(id)
	if e != nil || len(b) != 16 || strings.ToLower(id) != id {
		return false
	}
	for _, v := range b {
		if v != 0 {
			return true
		}
	}
	return false
}
func ValidateWindow(w Window, now time.Time) error {
	if w.From.IsZero() || w.To.IsZero() || !w.From.Before(w.To) || w.To.Sub(w.From) > 24*time.Hour || w.From.Before(now.Add(-7*24*time.Hour)) || w.To.After(now.Add(5*time.Minute)) {
		return ErrInvalid
	}
	return nil
}
func ValidateFilter(f Filter, now time.Time) error {
	if e := ValidateWindow(f.Window, now); e != nil {
		return e
	}
	if f.Limit < 1 || f.Limit > MaxPage {
		return ErrInvalid
	}
	if f.MinDuration != nil && *f.MinDuration == 0 || f.MaxDuration != nil && *f.MaxDuration == 0 || f.MinDuration != nil && f.MaxDuration != nil && *f.MinDuration > *f.MaxDuration {
		return ErrInvalid
	}
	if f.Status != nil && *f.Status > 2 {
		return ErrInvalid
	}
	return nil
}

type cursor struct {
	Hash    string `json:"h"`
	Cutoff  int64  `json:"c"`
	Start   int64  `json:"s"`
	TraceID string `json:"t"`
}

func filterHash(f Filter) string {
	f.Cursor = ""
	b, _ := json.Marshal(f)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func ParseCursor(f Filter) (time.Time, time.Time, string, error) {
	if f.Cursor == "" {
		return time.Time{}, time.Time{}, "", nil
	}
	var c cursor
	b, e := base64.RawURLEncoding.DecodeString(f.Cursor)
	if e != nil || len(b) > 1024 || json.Unmarshal(b, &c) != nil || c.Hash != filterHash(f) || !ValidTraceID(c.TraceID) || c.Cutoff <= 0 || c.Start <= 0 {
		return time.Time{}, time.Time{}, "", ErrInvalid
	}
	cut := time.Unix(0, c.Cutoff).UTC()
	start := time.Unix(0, c.Start).UTC()
	if start.Before(f.From) || !start.Before(f.To) {
		return time.Time{}, time.Time{}, "", ErrInvalid
	}
	return cut, start, c.TraceID, nil
}
func MakeCursor(f Filter, cutoff, start time.Time, id string) string {
	b, _ := json.Marshal(cursor{filterHash(f), cutoff.UnixNano(), start.UnixNano(), id})
	return base64.RawURLEncoding.EncodeToString(b)
}
func Relationships(d *DetailResult) {
	d.RootCount = 0
	d.MissingParentIDs = []string{}
	ids := make(map[string]Span, len(d.Spans))
	for _, s := range d.Spans {
		ids[s.SpanID] = s
	}
	missing := map[string]bool{}
	d.HasMissingRoot = false
	d.HasCycles = false
	d.HasSourceDrops = false
	for _, s := range d.Spans {
		if s.ParentSpanID == "" {
			d.RootCount++
		} else if _, ok := ids[s.ParentSpanID]; !ok {
			missing[s.ParentSpanID] = true
		}
		if s.DroppedAttributesCount > 0 || s.DroppedEventsCount > 0 || s.DroppedLinksCount > 0 || s.ResourceDroppedAttributesCount > 0 || s.ScopeDroppedAttributesCount > 0 || nestedSourceDrops(s.Events) || nestedSourceDrops(s.Links) {
			d.HasSourceDrops = true
		}
		seen := map[string]bool{s.SpanID: true}
		p := s.ParentSpanID
		for p != "" {
			if seen[p] {
				d.HasCycles = true
				break
			}
			seen[p] = true
			parent, ok := ids[p]
			if !ok {
				break
			}
			p = parent.ParentSpanID
		}
	}
	for id := range missing {
		d.MissingParentIDs = append(d.MissingParentIDs, id)
	}
	sort.Strings(d.MissingParentIDs)
	d.HasMissingRoot = d.RootCount == 0
	if len(d.Spans) > 0 {
		d.ObservedStartTime = d.Spans[0].StartTime
		d.ObservedEndTime = d.Spans[0].EndTime
		for _, s := range d.Spans {
			if s.StartTime.Before(d.ObservedStartTime) {
				d.ObservedStartTime = s.StartTime
			}
			if s.EndTime.After(d.ObservedEndTime) {
				d.ObservedEndTime = s.EndTime
			}
		}
		d.ObservedElapsedNS = fmt.Sprint(d.ObservedEndTime.Sub(d.ObservedStartTime).Nanoseconds())
	} else {
		d.ObservedElapsedNS = "0"
	}
}

// Events and links retain their own OTLP attribute-loss counters in protojson wrappers.
func nestedSourceDrops(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return false
	}
	var v struct {
		Events []struct {
			DroppedAttributesCount uint32 `json:"droppedAttributesCount"`
		} `json:"events"`
		Links []struct {
			DroppedAttributesCount uint32 `json:"droppedAttributesCount"`
		} `json:"links"`
	}
	if json.Unmarshal(raw, &v) != nil {
		return false
	}
	for _, e := range v.Events {
		if e.DroppedAttributesCount > 0 {
			return true
		}
	}
	for _, l := range v.Links {
		if l.DroppedAttributesCount > 0 {
			return true
		}
	}
	return false
}
