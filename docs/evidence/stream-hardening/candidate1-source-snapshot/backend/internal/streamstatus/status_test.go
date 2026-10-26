package streamstatus

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type fakeProbe struct{ observation Observation }

func TestKafkaProbeBoundedClientConfiguration(t *testing.T) {
	probe, err := NewKafkaProbe([]string{"127.0.0.1:1"}, "incidentlens-spans-v2", "test-group")
	if err != nil {
		t.Fatalf("admin client configuration: %v", err)
	}
	probe.Close()
}

func (f *fakeProbe) Observe(context.Context) Observation { return f.observation }

func complete(committed, end int64) []Partition {
	parts := make([]Partition, ExpectedPartitions)
	for i := range parts {
		parts[i] = Partition{ID: int32(i), LogStart: 0, End: end, Committed: committed}
	}
	return parts
}

func TestLagRequiresCompleteConsistentInitializedOffsets(t *testing.T) {
	for name, test := range map[string]struct {
		parts []Partition
		want  string
	}{
		"partial":                {complete(0, 1)[:3], "partition_coverage_incomplete"},
		"duplicate":              {[]Partition{{0, 0, 1, 0}, {0, 0, 1, 0}, {2, 0, 1, 0}, {3, 0, 1, 0}}, "partition_coverage_inconsistent"},
		"uninitialized nonempty": {complete(-1, 1), "offset_uninitialized"},
		"retention gap":          {[]Partition{{0, 2, 5, 1}, {1, 0, 0, 0}, {2, 0, 0, 0}, {3, 0, 0, 0}}, "retained_offset_gap"},
		"commit beyond end":      {complete(2, 1), "offsets_inconsistent"},
	} {
		t.Run(name, func(t *testing.T) {
			p := &fakeProbe{Observation{Partitions: test.parts}}
			s := New(p, nil)
			r := s.Sample(context.Background())
			if r.State != "unavailable" || r.LagRecords != nil || r.ReasonCode != test.want {
				t.Fatalf("%+v", r)
			}
		})
	}
	for name, parts := range map[string][]Partition{"verified empty": complete(0, 0), "pristine empty": complete(-1, 0), "backlog": complete(1, 3)} {
		t.Run(name, func(t *testing.T) {
			r := New(&fakeProbe{Observation{Partitions: parts}}, nil).Sample(context.Background())
			want := int64(0)
			if name == "backlog" {
				want = 8
			}
			if r.LagRecords == nil || *r.LagRecords != want || r.State == "unavailable" {
				t.Fatalf("%+v", r)
			}
			if name == "pristine empty" && r.ReasonCode != "empty_partition_uninitialized" {
				t.Fatalf("missing uninitialized caveat: %+v", r)
			}
		})
	}
}

func TestStaleAndDelayedProgressNeverKeepGreenLag(t *testing.T) {
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	pending := now.Add(-3 * time.Second)
	write := now.Add(-time.Second)
	commit := now.Add(-2 * time.Second)
	delay := int64(1_500_000_000)
	s := New(&fakeProbe{Observation{Partitions: complete(0, 0)}}, func() Progress {
		return Progress{LastWriteAt: &write, LastCommitAt: &commit, ProcessingDelayNS: &delay, PendingSince: &pending, State: "retrying", ErrorCode: "storage_unavailable"}
	})
	s.Now = func() time.Time { return now }
	r := s.Sample(context.Background())
	if r.LagRecords == nil || *r.LagRecords != 0 || r.ProcessingState != "retrying" || r.PendingAgeNS == nil || *r.PendingAgeNS != "3000000000" || r.ProcessingDelayNS == nil || *r.ProcessingDelayNS != "1500000000" || r.ReasonCode != "storage_unavailable" {
		t.Fatalf("%+v", r)
	}
	now = now.Add(StaleAfter + time.Nanosecond)
	r = s.Current()
	if r.State != "stale" || r.LagRecords != nil || r.ReasonCode != "sample_stale" {
		t.Fatalf("%+v", r)
	}
}

func TestClockSkewRemovesLagInsteadOfShowingGreen(t *testing.T) {
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	future := now.Add(time.Second)
	s := New(&fakeProbe{Observation{Partitions: complete(0, 0)}}, func() Progress { return Progress{PendingSince: &future, State: "running"} })
	s.Now = func() time.Time { return now }
	r := s.Sample(context.Background())
	if r.State != "unavailable" || r.LagRecords != nil || r.ReasonCode != "clock_skew" {
		t.Fatalf("%+v", r)
	}
	s.Progress = nil
	s.Sample(context.Background())
	now = now.Add(-time.Second)
	r = s.Current()
	if r.State != "unavailable" || r.LagRecords != nil || r.ReasonCode != "clock_skew" {
		t.Fatalf("%+v", r)
	}
}

func TestProgressCompletedDuringSampleIsInsideObservationWindow(t *testing.T) {
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	s := New(&fakeProbe{Observation{Partitions: complete(0, 0)}}, func() Progress {
		now = now.Add(time.Millisecond)
		write := now
		return Progress{LastWriteAt: &write, State: "idle"}
	})
	s.Now = func() time.Time { return now }
	r := s.Sample(context.Background())
	if r.State != "ok" || r.LagRecords == nil || *r.LagRecords != 0 || r.LastWriteAt == nil || !r.SampleFinishedAt.Equal(*r.LastWriteAt) {
		t.Fatalf("normal concurrent progress misclassified: %+v", r)
	}
}

func TestTopicIdentityValidation(t *testing.T) {
	var id [16]byte
	id[0] = 4
	if !sameTopic("", id, "incidentlens-spans-v2", id) || sameTopic("", id, "incidentlens-spans-v2", [16]byte{}) || sameTopic("wrong", [16]byte{}, "incidentlens-spans-v2", id) {
		t.Fatal("topic identity accepted mismatched response")
	}
}

func TestHTTPStatusIsIndependentAndBounded(t *testing.T) {
	s := New(&fakeProbe{Observation{ReasonCode: "secret broker message"}}, nil)
	s.Sample(context.Background())
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/internal/stream-status", nil))
	if w.Code != http.StatusOK || w.Body.Len() > MaxResponseBytes || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("code=%d len=%d", w.Code, w.Body.Len())
	}
	var r Response
	if err := json.Unmarshal(w.Body.Bytes(), &r); err != nil || r.State != "unavailable" || r.LagRecords != nil || r.ReasonCode != "sample_unavailable" {
		t.Fatalf("%+v err=%v", r, err)
	}
	if w.Body.String() == "secret broker message" {
		t.Fatal("raw broker error leaked")
	}
}
