package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"
)

// Only complete lines from the bounded read-only observer are considered. Two
// fresh, initialized, fully committed observations establish the setup boundary;
// absent offsets never become a fabricated zero. No broker offsets are changed.
func setupOffsetsReady(data []byte, after, now time.Time) bool {
	var previousEnd, previousTime int64
	ready := false
	for _, line := range bytes.SplitAfter(data, []byte{'\n'}) {
		if len(line) == 0 || line[len(line)-1] != '\n' {
			continue
		}
		var sample struct {
			Kind      string `json:"kind"`
			Started   int64  `json:"started_ns"`
			Finished  int64  `json:"finished_ns"`
			Committed *int64 `json:"committed"`
			End       *int64 `json:"end"`
			Lag       *int64 `json:"lag_records"`
			Error     string `json:"error"`
			Unknown   bool   `json:"uninitialized_or_inconsistent"`
		}
		if json.Unmarshal(line, &sample) != nil {
			previousTime = 0
			ready = false
			continue
		}
		if sample.Started < after.UnixNano() {
			continue
		}
		valid := sample.Kind == "lag" && sample.Error == "" && !sample.Unknown &&
			sample.Committed != nil && sample.End != nil && sample.Lag != nil &&
			*sample.End > 0 && *sample.Committed == *sample.End && *sample.Lag == 0 &&
			sample.Finished >= sample.Started && sample.Finished <= now.UnixNano() &&
			now.UnixNano()-sample.Started <= int64(3*time.Second)
		if !valid {
			previousTime = 0
			ready = false
			continue
		}
		ready = previousTime > 0 && sample.Started > previousTime && *sample.End == previousEnd
		previousTime, previousEnd = sample.Finished, *sample.End
	}
	return ready
}

func waitSetupOffsets(ctx context.Context, path string, after time.Time) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("setup offset wait cancelled: %w", err)
		}
		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("setup lag observer: %w", err)
		}
		data, err := io.ReadAll(io.LimitReader(file, (1<<20)+1))
		file.Close()
		if err != nil {
			return fmt.Errorf("setup lag observer: %w", err)
		}
		if len(data) > 1<<20 {
			return fmt.Errorf("setup lag observer exceeds1MiB")
		}
		if setupOffsetsReady(data, after, time.Now()) {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("setup offsets not observed committed; load not started: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}
