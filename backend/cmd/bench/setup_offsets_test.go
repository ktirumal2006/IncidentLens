package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSetupOffsetsBarrier(t *testing.T) {
	after := time.Unix(10, 0)
	now := time.Unix(13, 0)
	line := func(second, committed, end int64) string {
		return fmt.Sprintf("{\"kind\":\"lag\",\"started_ns\":%d,\"finished_ns\":%d,\"committed\":%d,\"end\":%d,\"lag_records\":%d}\n", second*1e9, second*1e9+1, committed, end, end-committed)
	}
	good := line(11, 16, 16) + line(12, 16, 16)
	for _, tc := range []struct {
		name, data string
		want       bool
	}{
		{"ready", good, true},
		{"one", line(12, 16, 16), false},
		{"before_setup", line(8, 16, 16) + line(9, 16, 16), false},
		{"lagging", line(11, 15, 16) + line(12, 15, 16), false},
		{"uninitialized", line(11, -1, 16) + line(12, -1, 16), false},
		{"empty", line(11, 0, 0) + line(12, 0, 0), false},
		{"new_end", line(11, 16, 16) + line(12, 17, 17), false},
		{"missing_fields", "{\"kind\":\"lag\",\"started_ns\":12000000000}\n", false},
		{"partial", line(11, 16, 16) + line(12, 16, 16)[:20], false},
		{"later_error", good + "{\"kind\":\"lag\",\"started_ns\":12500000000,\"error\":\"unavailable\"}\n", false},
		{"duplicate_time", line(12, 16, 16) + line(12, 16, 16), false},
		{"future", line(14, 16, 16) + line(15, 16, 16), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := setupOffsetsReady([]byte(tc.data), after, now); got != tc.want {
				t.Fatalf("got%v want%v", got, tc.want)
			}
		})
	}
	if setupOffsetsReady([]byte(good), after, now.Add(4*time.Second)) {
		t.Fatal("stale observations accepted")
	}
}

func TestSetupOffsetsCancellationAndBounds(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lag.jsonl")
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if waitSetupOffsets(ctx, path, time.Now()) == nil {
		t.Fatal("cancelled setup proceeded")
	}
	if err := os.WriteFile(path, make([]byte, (1<<20)+1), 0600); err != nil {
		t.Fatal(err)
	}
	if waitSetupOffsets(context.Background(), path, time.Now()) == nil {
		t.Fatal("oversized observer accepted")
	}
}
