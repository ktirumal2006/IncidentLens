package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"math/bits"
	"os"
	"path/filepath"
	"strconv"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type Reconciliation struct {
	Settled                     bool   `json:"settled"`
	SnapshotVerified            bool   `json:"snapshot_verified"`
	Verified                    bool   `json:"verified"`
	Error                       string `json:"error,omitempty"`
	PlannedSpans                int64  `json:"planned_spans"`
	EmittedSpans                int64  `json:"emitted_spans"`
	AckedSpans                  int64  `json:"acked_spans"`
	RawRows                     uint64 `json:"raw_rows"`
	FinalUniqueSpans            uint64 `json:"final_unique_spans"`
	ExpectedStored              int64  `json:"expected_stored"`
	PlannedMissing              int64  `json:"planned_missing"`
	EmittedMissing              int64  `json:"emitted_missing"`
	AckedMissing                int64  `json:"acked_missing"`
	NeverAckedStored            int64  `json:"never_acked_stored"`
	Unknown                     uint64 `json:"unknown"`
	PhysicalDuplicatesRemaining uint64 `json:"physical_duplicates_remaining"`
	DuplicateLogicalIdentities  uint64 `json:"duplicate_logical_identities"`
	Ledger                      string `json:"ledger"`
}

func (h *Harness) db() (driver.Conn, error) {
	return ch.Open(&ch.Options{Addr: []string{h.config.ClickHouse}, Auth: ch.Auth{Database: "incidentlens", Username: h.config.CHUser, Password: h.config.CHPassword}, DialTimeout: 2 * time.Second, ReadTimeout: 5 * time.Second, MaxOpenConns: 1, BlockBufferSize: 1})
}
func (h *Harness) preflight() error {
	conn, e := h.db()
	if e != nil {
		return e
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var count uint64
	if e = conn.QueryRow(ctx, "SELECT count() FROM spans PREWHERE service_namespace=?", h.config.Namespace).Scan(&count); e != nil {
		return fmt.Errorf("namespace preflight: %w", e)
	}
	if count != 0 {
		return fmt.Errorf("seed namespace already contains%drows; usefreshseed", count)
	}
	return nil
}
func (h *Harness) stateForTrace(index int) *Batch {
	if index < h.config.BaselineTraces {
		return h.batches[index/h.config.BatchTraces]
	}
	return h.batches[batchCount(h.config.BaselineTraces, h.config.BatchTraces)+(index-h.config.BaselineTraces)/h.config.BatchTraces]
}
func (h *Harness) reconcile() Reconciliation {
	r := Reconciliation{PlannedSpans: int64(h.config.LoadSpans + h.config.SetupSpans), Ledger: "reconciliation.jsonl"}
	conn, e := h.db()
	if e != nil {
		r.Error = e.Error()
		return r
	}
	defer conn.Close()
	total := h.config.LoadTraces + h.config.BaselineTraces
	expected := make(map[string]int, total)
	for i := 0; i < total; i++ {
		expected[traceHex(h.config.Seed, i)] = i
	}
	seen := make([]uint8, total)
	file, e := os.OpenFile(filepath.Join(h.config.OutputDir, r.Ledger), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		r.Error = e.Error()
		return r
	}
	defer file.Close()
	writer := bufio.NewWriterSize(file, 64<<10)
	defer writer.Flush()
	bytes := int64(0)
	emit := func(v any) error {
		b, e := json.Marshal(v)
		if e != nil {
			return e
		}
		if h.rec.bytes+bytes+int64(len(b)+1) > h.config.MaxOutputBytes {
			return fmt.Errorf("combinedraw event/ledger cap reached")
		}
		bytes += int64(len(b) + 1)
		_, e = writer.Write(append(b, '\n'))
		return e
	}
	filters := []struct {
		sql     string
		args    []any
		service string
	}{}
	for _, service := range h.config.Services {
		filters = append(filters, struct {
			sql     string
			args    []any
			service string
		}{"service_name=?", []any{service}, service})
	}
	filters = append(filters, struct {
		sql     string
		args    []any
		service string
	}{"service_name NOT IN (?,?,?)", []any{h.config.Services[0], h.config.Services[1], h.config.Services[2]}, ""})
	for _, filter := range filters {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		args := append([]any{h.config.Namespace}, filter.args...)
		var raw uint64
		e = conn.QueryRow(ctx, "SELECT count() FROM spans PREWHERE service_namespace=? AND "+filter.sql, args...).Scan(&raw)
		cancel()
		if e != nil {
			r.Error = "rawreconciliation: " + e.Error()
			return r
		}
		r.RawRows += raw
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		rows, e := conn.Query(ctx, "SELECT trace_id,span_id,service_name FROM spans FINAL PREWHERE service_namespace=? AND "+filter.sql, args...)
		if e != nil {
			cancel()
			r.Error = "FINALreconciliation: " + e.Error()
			return r
		}
		for rows.Next() {
			var id, span, service string
			if e = rows.Scan(&id, &span, &service); e != nil {
				break
			}
			r.FinalUniqueSpans++
			index, ok := expected[id]
			n, parseErr := strconv.ParseUint(span, 16, 8)
			if !ok || parseErr != nil || n < 1 || n > 6 || service != h.config.Services[(n-1)/2] {
				r.Unknown++
				if e = emit(map[string]any{"kind": "unknown", "trace_id": id, "span_id": span, "service_name": service}); e != nil {
					break
				}
				continue
			}
			mask := uint8(1 << uint(n-1))
			if seen[index]&mask != 0 {
				r.DuplicateLogicalIdentities++
			}
			seen[index] |= mask
		}
		if e == nil {
			e = rows.Err()
		}
		rows.Close()
		cancel()
		if e != nil {
			r.Error = "FINALreconciliation: " + e.Error()
			return r
		}
	}
	if r.RawRows >= r.FinalUniqueSpans {
		r.PhysicalDuplicatesRemaining = r.RawRows - r.FinalUniqueSpans
	}
	for index, mask := range seen {
		b := h.stateForTrace(index)
		stored := bits.OnesCount8(mask)
		r.ExpectedStored += int64(stored)
		r.PlannedMissing += int64(6 - stored)
		if b.Attempts > 0 {
			r.EmittedSpans += 6
			r.EmittedMissing += int64(6 - stored)
		}
		if b.Acked {
			r.AckedSpans += 6
			r.AckedMissing += int64(6 - stored)
		} else {
			r.NeverAckedStored += int64(stored)
		}
		if e = emit(map[string]any{"kind": "expected_trace", "index": index, "trace_id": traceHex(h.config.Seed, index), "expected_mask": 63, "stored_mask": mask, "acked": b.Acked, "emitted": b.Attempts > 0, "enqueued": b.Enqueued, "offered": b.Offered, "phase": b.Phase}); e != nil {
			r.Error = e.Error()
			return r
		}
	}
	if e = writer.Flush(); e == nil {
		e = file.Sync()
	}
	if e != nil {
		r.Error = e.Error()
		return r
	}
	r.Verified = true
	r.SnapshotVerified = true
	return r
}
