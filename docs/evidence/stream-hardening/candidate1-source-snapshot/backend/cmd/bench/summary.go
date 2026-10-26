package main

import (
	"time"
)

type PhaseSummary struct {
	PlannedSpans, OfferedSpans, EnqueuedSpans, GeneratedSpans, EmittedSpans, AckedSpans int64
	ExportedBatches, TerminalFailedBatches, RetriedBatches                              int
	RejectedSpans                                                                       int64
	AckedWithinWindow                                                                   int64
	Ack                                                                                 []int64
	ScheduleAck                                                                         []int64
}

func phaseJSON(p PhaseSummary, seconds float64) map[string]any {
	failure, ackFraction, rate := float64(0), float64(0), float64(0)
	if p.ExportedBatches > 0 {
		failure = float64(p.TerminalFailedBatches) / float64(p.ExportedBatches)
	}
	if p.OfferedSpans > 0 {
		ackFraction = float64(p.AckedWithinWindow) / float64(p.OfferedSpans)
	}
	if seconds > 0 {
		rate = float64(p.AckedWithinWindow) / seconds
	}
	return map[string]any{"planned_spans": p.PlannedSpans, "offered_spans": p.OfferedSpans, "enqueued_spans": p.EnqueuedSpans, "generated_spans": p.GeneratedSpans, "emitted_spans": p.EmittedSpans, "acked_spans": p.AckedSpans, "acked_spans_within_measurement_window": p.AckedWithinWindow, "exported_batches": p.ExportedBatches, "terminal_failed_batches": p.TerminalFailedBatches, "terminal_error_fraction": failure, "unique_ack_fraction": ackFraction, "acked_spans_per_second": rate, "retried_batches": p.RetriedBatches, "rejected_spans": p.RejectedSpans, "ack_latency": percentiles(p.Ack), "schedule_to_ack_latency": percentiles(p.ScheduleAck)}
}
func (h *Harness) summary(r Reconciliation, drainExpired bool, runErr, logErr error) map[string]any {
	measurementStart := h.start.Add(h.config.Warmup)
	measurementEnd := measurementStart.Add(h.config.Duration)
	phases := map[string]*PhaseSummary{"setup": {}, "warmup": {}, "measured": {}}
	dropped, missed := 0, 0
	for _, b := range h.batches {
		p := phases[b.Phase]
		spans := int64(b.Count * 6)
		p.PlannedSpans += spans
		if b.Offered {
			p.OfferedSpans += spans
		}
		if b.Enqueued {
			p.EnqueuedSpans += spans
		}
		if b.Generated {
			p.GeneratedSpans += spans
		}
		if b.Attempts > 0 {
			p.EmittedSpans += spans
			p.ExportedBatches++
			if !b.Acked {
				p.TerminalFailedBatches++
			}
		}
		if b.Attempts > 1 {
			p.RetriedBatches++
		}
		p.RejectedSpans += b.Rejected
		if b.Acked {
			p.AckedSpans += spans
			ack := time.Unix(0, b.AckNS.Load())
			p.Ack = append(p.Ack, ack.Sub(b.FirstSend).Nanoseconds())
			p.ScheduleAck = append(p.ScheduleAck, ack.Sub(b.Scheduled).Nanoseconds())
			if !ack.Before(measurementStart) && ack.Before(measurementEnd) {
				p.AckedWithinWindow += spans
			}
		}
		if b.Offered && !b.Enqueued {
			dropped++
		}
		if b.DropReason == "missed_deadline" {
			missed++
		}
	}
	latencies := map[string]Latency{}
	for key, values := range h.rec.latency {
		latencies[key] = percentiles(values)
	}
	visibilityValues := []int64{}
	visibilityCensored := 0
	for _, probe := range h.probes {
		if probe.censored {
			visibilityCensored++
		} else if probe.probe.Batch.Phase == "measured" {
			visibilityValues = append(visibilityValues, probe.seen.Sub(probe.probe.Started).Nanoseconds())
		}
	}
	summary := map[string]any{"schema_version": 1, "config": h.config, "load_start": h.start, "measurement_start": measurementStart, "measurement_end": measurementEnd, "offering_finished": h.offerFinished, "run_end": time.Now().UTC(), "measured": phaseJSON(*phases["measured"], h.config.Duration.Seconds()), "warmup": phaseJSON(*phases["warmup"], h.config.Warmup.Seconds()), "setup": phaseJSON(*phases["setup"], 0), "driver": map[string]any{"schedule_dropped_batches": dropped, "missed_deadline_batches": missed, "generator_limited": dropped > 0, "logging_error": errorText(logErr)}, "raw_event_bytes": h.rec.bytes, "latencies": latencies, "event_counts": h.rec.counts, "status_counts": h.rec.status, "visibility": map[string]any{"observed_measured": len(visibilityValues), "censored": visibilityCensored, "first_send_to_observation": percentiles(visibilityValues), "poll_interval_ns": int64(200 * time.Millisecond), "method": "sampled expected six-span trace observation; polling upper bounds, not exact storage arrival"}, "reconciliation": r, "drain_deadline_expired": drainExpired, "error": errorText(runErr)}
	return summary
}
func errorText(e error) string {
	if e == nil {
		return ""
	}
	return e.Error()
}
