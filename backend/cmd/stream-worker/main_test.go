package main

import (
	"testing"
	"time"

	"incidentlens/backend/internal/stream"
)

func TestStatusProgressPreservesUnknownAndRetrySemantics(t *testing.T) {
	idle := statusProgress(stream.ProgressSnapshot{State: "idle"})
	if idle.State != "idle" || idle.LastWriteAt != nil || idle.ProcessingDelayNS != nil || idle.PendingSince != nil {
		t.Fatalf("invented progress: %+v", idle)
	}
	now := time.Now().UTC()
	r := statusProgress(stream.ProgressSnapshot{State: "retrying_write", LastWriteAt: now, LastCommitAt: now.Add(-time.Second), OldestPendingReceipt: now.Add(-2 * time.Second), LastCompletedProcessingAgeNS: 42})
	if r.State != "retrying" || r.ErrorCode != "storage_unavailable" || r.ProcessingDelayNS == nil || *r.ProcessingDelayNS != 42 || r.PendingSince == nil || !r.PendingSince.Equal(now.Add(-2*time.Second)) {
		t.Fatalf("lost progress: %+v", r)
	}
	failed := statusProgress(stream.ProgressSnapshot{State: "unexpected", ErrorCode: "raw secret error"})
	if failed.State != "failed" || failed.ErrorCode != "worker_failed" {
		t.Fatalf("unsafe state: %+v", failed)
	}
	if r := statusProgress(stream.ProgressSnapshot{State: "error", ErrorCode: "routing_violation"}); r.ErrorCode != "poison_record" {
		t.Fatalf("routing error not fail closed: %+v", r)
	}
}
