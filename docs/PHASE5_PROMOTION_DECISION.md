# Phase 5 candidate 3 promotion decision

Decision, October 9, 2026: **do not promote Kafka to the default. Stop tuning.**
Candidate 3 remains frozen. The completed matched diagnostic passed five numerical
gates and failed three. Under the user's bounded decision rule, the full 16-case
campaign and default/cutover rehearsal were not started. A new candidate or Phase 6
requires a separate request. The direct ingestion path remains the default.

## Controlled result

The [campaign](../benchmarks/results/20261009-hardening-candidate3-decision/campaign.json)
used the frozen single-partition Phase 5 baseline and candidate 3's four partitions,
one synchronous writer and 2,048-row target. Both used identical quotas, fixtures,
120-second offered-load windows at 8,000 spans/s, query budgets, bounded recovery,
and the revised Kafka health check. Application images were not rebuilt or changed.
See the [freeze record](evidence/stream-hardening/bounded-decision-freeze-20261009.json),
[exact source/deployment snapshot](evidence/stream-hardening/candidate3-decision-source.json)
and [exact gate calculations](evidence/stream-hardening/candidate3-decision-gates.json).

| Measurement | Baseline | Candidate 3 |
| --- | ---: | ---: |
| Measured-fixture admitted spans, including later ACKs | 439,488 | 920,190 |
| Measured-fixture admitted / 120 s | 3,662.4 spans/s | 7,668.25 spans/s |
| ACKed spans within the measured window / 120 s | 3,536 spans/s | 7,552 spans/s |
| Completed ClickHouse writes during window | 106,368 rows | 533,268 rows |
| Completed persistence throughput | 886.4 rows/s | 4,443.9 rows/s |
| Consumer committed-row throughput | 886.4 rows/s | 4,443.9 rows/s |
| OTLP ACK p95 / p99 | 1,088.845 / 1,247.276 ms | 544.028 / 686.745 ms |
| Peak sampled Kafka lag | 1,890 records | 8,117 records |
| Post-offering drain upper bound | 141.910333 s | 36.942528 s |
| Censored visibility probes / sampled | 229 / 229 | 382 / 476 |
| Observed visibility p95 / p99 | unavailable: no successful observations | 8.939097 / 9.988447 s |
| Non-404 paced query failures / issued | 175 / 465 | 209 / 419 |
| Paced query 404 responses | 84 | 50 |
| Scheduled query drops / scheduled | 15 / 480 | 57 / 480 |
| Full-fixture admitted spans | 490,368 | 1,001,022 |
| Stored unique spans at original snapshot | 239,936 | 1,001,022 |
| Final recovered admitted identities | 490,368 | 1,001,022 |
| Acknowledged identities missing after bounded recovery | 0 | 0 |
| Planned spans never emitted by generator | 550,398 | 39,744 |

Completed write/commit rates count physical rows, including replay if present;
they are not a claim about a new underlying ClickHouse capacity. Final identity
reconciliation is the separate duplicate-safe delivery evidence. The baseline's
250,432 original-deadline misses recovered in supplemental observations; they
are not permanent loss. Candidate 3 had no original-snapshot admitted misses.
The analyzer's `stored_unique_final_spans` field retains the original snapshot
value for the baseline; use the supplemental identity reconciliation for its
actual final recovered total.

Kafka lag is measured in records, whose span counts differ between one- and
four-partition routing; the raw peak counts are not a comparable span-backlog
ratio. Drain bounds include observer polling uncertainty. Visibility percentiles
are conditional on successful observations and must be read alongside censoring.
In particular, candidate 3's 94 visible probes do not represent its 382 misses.

Admission, both ACK tails, completed-write throughput and half-baseline drain
passed their original gates. The failures are:

- **Visibility:** 382/476 probes were censored; the gate permits at most half the
  baseline censored fraction, which here is 1/2.
- **Query errors:** 209/419 versus 175/465 non-404 failures exceeds the allowed
  increase of 1/20. No errors were excluded to obtain a pass.
- **Query scheduling:** 57/480 versus 15/480 drops is an increase of 42/480,
  exceeding 1/20.

## Readiness fix and infrastructure failure

The old Kafka protocol health check had a five-second deadline. An isolated
successful call took 12.394 seconds with the existing 2-CPU/1-GiB broker quota and
512-MiB heap. The fix retains the Kafka API protocol check, uses a 20-second
timeout, a 30-second interval/startup grace period and five retries. A fresh broker
became healthy, while a probe to an unused port failed. The longer interval also
changes health-probe overhead; it was applied equally to both measured variants.
[Verification](evidence/stream-hardening/readiness-fix-20261009/verification.json)
retains timings, health history and unchanged resource settings.

The first decision-run preflight could not stop the existing ClickHouse container:
Docker reported no exit event after trying to kill it. No benchmark case had
started. A Docker Desktop restart restored the container's health and normal
shutdown; all 379 volume names were preserved. The same campaign resumed with
unchanged provenance and fresh cases, preserving the failed attempt in its
journal. [Recovery evidence](evidence/stream-hardening/docker-preflight-recovery-20261009/recovery-result.json)
is separate from the measured result. The campaign retains a stale `restore_error`
from that first preflight; the final restoration command returned zero and the
resumed campaign completed. This historical field is not silently erased.

## Query failures and acceptance

The frozen incident endpoint reads a bounded 35-minute context and rejects more
than 100,000 spans or 32 MiB of detector input. It maps that limit, database errors,
and several other resource failures to HTTP 503. HTTP 504 identifies a deadline
path; other query routes also have admission/storage failure paths.

Successful server SELECTs returning 100,001 rows provide evidence of detector
row-limit pressure. They cannot be joined to individual HTTP responses using the
captured fields. A 503 without that linkage remains unclassified; server transport
or cancellation errors can also be consequences of a client limit or deadline,
so they do not independently prove a storage outage. Faster ingestion can reach
the fixed detector cap sooner, but that does not excuse the failed query gate.
All original status/error/drop counts remain in acceptance. See the
[prior baseline classification](evidence/stream-hardening/query-classification-20261009.json)
and the [final pair classification](evidence/stream-hardening/candidate3-decision-query-classification.json).
Within the exact measured windows, baseline incident requests returned 49×200,
54×503 and 12×504; candidate requests returned 3×200 and 95×503. Successful
SELECTs returning exactly 100,001 rows numbered 19 and 75 respectively. This
supports greater detector-limit exposure, without establishing each HTTP error's
cause. Nonincident routes also returned 503s.

## Reliability and scope limits

Candidate 3 previously passed all 13 real Kafka/ClickHouse streaming race tests,
including the 2,304-row batch-boundary/replay test, crash/rebalance tests and
fail-closed offset-gap handling; its paired smoke reconciled all 888 spans on
each side. [Correctness evidence](evidence/stream-hardening/acceptance3-verification.json)
is not a substitute for failed performance acceptance.

This fresh pair recovered every admitted identity. It does not erase the earlier
interrupted attempt's **7,734 missing acknowledged spans** after an outage longer
than Kafka's one-hour retention. That [loss evidence](VERIFICATION_STREAM_HARDENING.md#interrupted-attempt-and-retention-loss)
remains part of the reliability record. Finite retention, one broker and local
storage do not provide an HA or indefinite-replay guarantee.

The diagnostic is one paired repetition, not full workload acceptance. No default
Kafka entry point was added, no cutover was performed, and no Phase 6 work began.
Further tuning stops with this decision.

The independent [final pair audit](evidence/stream-hardening/candidate3-final-pair-audit.json)
replayed raw analysis, recomputed gates and checked the shared controls, immutable
images, 118-file source snapshot, setup barriers and retained volumes.
