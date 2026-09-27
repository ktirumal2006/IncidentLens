package query

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestCursorBindsFilters(t *testing.T) {
	now := time.Now().UTC()
	f := Filter{Window: Window{From: now.Add(-time.Hour), To: now}, Limit: 1}
	id := "0123456789abcdef0123456789abcdef"
	c := MakeCursor(f, now, now.Add(-time.Minute), id)
	f.Cursor = c
	cut, start, got, e := ParseCursor(f)
	if e != nil || !cut.Equal(now) || !start.Equal(now.Add(-time.Minute)) || got != id {
		t.Fatalf("valid cursor: %v", e)
	}
	service := "one"
	f.Service = &service
	if _, _, _, e = ParseCursor(f); !errors.Is(e, ErrInvalid) {
		t.Fatalf("filter mismatch accepted: %v", e)
	}
}
func TestValidateBounds(t *testing.T) {
	now := time.Now().UTC()
	f := Filter{Window: Window{From: now.Add(-time.Hour), To: now}, Limit: 100}
	if e := ValidateFilter(f, now); e != nil {
		t.Fatal(e)
	}
	zero := uint64(0)
	f.MinDuration = &zero
	if e := ValidateFilter(f, now); e == nil {
		t.Fatal("zero duration accepted")
	}
	f.MinDuration = nil
	f.To = f.From
	if e := ValidateFilter(f, now); e == nil {
		t.Fatal("empty window accepted")
	}
}
func TestRelationships(t *testing.T) {
	now := time.Now()
	d := DetailResult{Spans: []Span{{SpanID: "a", ParentSpanID: "b", StartTime: now, EndTime: now.Add(time.Second)}, {SpanID: "b", ParentSpanID: "a", StartTime: now, EndTime: now.Add(2 * time.Second)}}}
	Relationships(&d)
	if !d.HasCycles || !d.HasMissingRoot || d.RootCount != 0 || d.ObservedElapsedNS != "2000000000" {
		t.Fatalf("bad cycle result: %+v", d)
	}
	d = DetailResult{Spans: []Span{{SpanID: "a", ParentSpanID: "missing", StartTime: now, EndTime: now}}}
	Relationships(&d)
	if len(d.MissingParentIDs) != 1 || d.MissingParentIDs[0] != "missing" {
		t.Fatalf("missing parent: %+v", d)
	}
}
func TestCursorWindowAndLimitBinding(t *testing.T) {
	now := time.Now().UTC()
	id := "0123456789abcdef0123456789abcdef"
	f := Filter{Window: Window{From: now.Add(time.Minute), To: now.Add(2 * time.Minute)}, Limit: 1}
	f.Cursor = MakeCursor(f, now, now.Add(90*time.Second), id)
	if _, _, _, e := ParseCursor(f); e != nil {
		t.Fatalf("future event window with earlier receipt: %v", e)
	}
	f.Limit = 2
	if _, _, _, e := ParseCursor(f); !errors.Is(e, ErrInvalid) {
		t.Fatal("limit mismatch accepted")
	}
	f.Limit = 1
	f.To = f.To.Add(time.Second)
	if _, _, _, e := ParseCursor(f); !errors.Is(e, ErrInvalid) {
		t.Fatal("window mismatch accepted")
	}
}
func TestWindowRetentionAndFuture(t *testing.T) {
	now := time.Now().UTC()
	for _, w := range []Window{{now.Add(-8 * 24 * time.Hour), now.Add(-7 * 24 * time.Hour)}, {now.Add(-25 * time.Hour), now}, {now, now.Add(6 * time.Minute)}} {
		if ValidateWindow(w, now) == nil {
			t.Fatalf("accepted window %+v", w)
		}
	}
}
func TestRelationshipsRecomputeAndDropOverflow(t *testing.T) {
	now := time.Now()
	d := DetailResult{Spans: []Span{{SpanID: "a", ParentSpanID: "missing", StartTime: now, EndTime: now, DroppedAttributesCount: ^uint32(0), DroppedEventsCount: 1}}}
	Relationships(&d)
	if !d.HasSourceDrops || len(d.MissingParentIDs) != 1 {
		t.Fatalf("first calculation: %+v", d)
	}
	d.Spans = []Span{{SpanID: "a", StartTime: now, EndTime: now}}
	Relationships(&d)
	if d.RootCount != 1 || len(d.MissingParentIDs) != 0 || d.HasSourceDrops || d.HasMissingRoot {
		t.Fatalf("recompute: %+v", d)
	}
}
func TestRelationshipsNestedSourceDrops(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name          string
		events, links json.RawMessage
	}{{"event", json.RawMessage(`{"events":[{"name":"checkpoint","droppedAttributesCount":2}]}`), json.RawMessage(`{"links":[]}`)}, {"link", json.RawMessage(`{"events":[]}`), json.RawMessage(`{"links":[{"droppedAttributesCount":1}]}`)}} {
		t.Run(tc.name, func(t *testing.T) {
			d := DetailResult{Spans: []Span{{SpanID: "a", StartTime: now, EndTime: now, Events: tc.events, Links: tc.links}}}
			Relationships(&d)
			if !d.HasSourceDrops {
				t.Fatal("nested OTLP dropped attributes not reported")
			}
		})
	}
}
