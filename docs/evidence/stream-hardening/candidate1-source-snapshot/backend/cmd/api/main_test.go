package main

import (
	"errors"
	"incidentlens/backend/internal/query"
	"testing"
)

func TestInvalidDetectorConfigFailsStartup(t *testing.T) {
	t.Setenv("DETECTOR_MIN_SAMPLES", "0")
	if e := run(); !errors.Is(e, query.ErrInvalid) {
		t.Fatalf("invalid detector config startup=%v", e)
	}
}
