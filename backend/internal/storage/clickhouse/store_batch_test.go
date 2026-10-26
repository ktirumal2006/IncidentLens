package clickhouse

import (
	"context"
	"strings"
	"testing"

	"incidentlens/backend/internal/trace"
)

func TestDirectAndStreamBatchCapsAreSeparate(t *testing.T) {
	store := &Store{}
	if err := store.Write(context.Background(), make([]trace.Row, 257)); err == nil || !strings.Contains(err.Error(), "256") {
		t.Fatalf("direct cap: %v", err)
	}
	if err := store.WriteBatch(context.Background(), make([]trace.Row, 4097)); err == nil || !strings.Contains(err.Error(), "4096") {
		t.Fatalf("stream cap: %v", err)
	}
	if err := store.WriteBatch(context.Background(), nil); err != nil {
		t.Fatalf("empty batch: %v", err)
	}
}
