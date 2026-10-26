# Phase 5 downstream query hardening

Status: query candidate 3 passed seven of eight diagnostic gates; scheduled
query drops remain the sole failure. The original Candidate 3 control now directly
attributes detector input-limit failures. Query candidate 4's October 12 smoke
attempt stopped during candidate startup: Docker could not mount the optional
migration file beneath the read-only migration directory. Its candidate load and
diagnostic pair never ran; the index's mixed-workload benefit is unverified.
Kafka remains opt-in. No Phase 6 work.

The [previous Candidate 3 decision](PHASE5_PROMOTION_DECISION.md) passed five
diagnostic gates and failed visibility, non-404 query failures, and scheduled
query drops. The user authorized this separate query-side investigation.
[ADR 0006](adr/0006-bounded-detector-query-plan.md) records the design and its
explicit change in relationship-caveat coverage.

## Findings and implementation

The old detector transfers all normalized spans in a 35-minute interval before
calculating SERVER statistics. Its 100,000-span and 32-MiB application bounds can
therefore reject an otherwise small statistical result. The prior decision's
75 cap-sized SELECT results and 95 incident 503 responses cannot be joined to
attribute every failure. Admission contention and database exceptions also
produce 503s.

The new query computes exact duplicate-safe SERVER counts and p95 in ClickHouse,
then reads bounded supporting trace context. Thresholds, ranking, evidence
selection and report-only filters retain their statistical semantics. The
`trace-v2` contract explicitly marks relationship context as sampled; it does not
claim that unexamined traces have no missing parents, cycles or source drops.
The UI displays this limitation even for normal operation results.

Trace detail uses explicit immutable-key PREWHERE with FINAL. A focused test
measured equal read cost for the old and new predicates: 1,698 rows, 164,722 bytes,
and 6 ms in both queries. This is not evidence of a speedup; existing optimizer
behavior may already provide the same pruning for that fixture.

Bounded HTTP failure headers distinguish admission, detector limits, numeric
ClickHouse exception codes, deadlines, and other resource failures. They contain
no SQL, trace payloads, identifiers, query parameters or exception text. The
benchmark records an allowlisted category without changing statuses, scheduling,
visibility definitions or gates.

## Verified evidence

The [real ClickHouse test run](evidence/query-hardening/aggregate-real-tests3.txt)
passed all six tests with the unchanged query-user profile and a 2-CPU/2-GiB
ClickHouse container:

- 100,002 unique spans account for 25,300,506 bytes. The old evaluator fails its
  row ceiling; the aggregate path returns exact baseline/current counts of
  100,000/2 and two supporting traces.
- 33,002 wider spans account for 42,605,582 bytes. The old evaluator fails its
  byte ceiling; the aggregate path returns exact counts of 33,000/2 and two
  supporting traces.
- Replay before merges, receipt cutoff, exact p95, half-open windows, cross-service
  evidence, distinct-trace ordering, cancellation, the 500-group limit, and
  production detail lookup are exercised. Earlier fixture-only failures remain
  in the evidence directory.

The [detail replay check](evidence/query-hardening/detail-tests.json) returned
three logical spans from six physical rows. Focused Go race tests and all 30
frontend tests passed; see [Go output](evidence/query-hardening/go-unit-tests.txt)
and [frontend output](evidence/query-hardening/frontend-tests.txt).

These checks establish correctness for the exercised cases, not high-load
promotion. Query profile limits remain 256 MiB memory, 256 MiB bytes read,
2,000,000 rows read and two threads. The API retains four slots, its five-second
deadline, and an 8-MiB response cap.

## Pending promotion work

The [predeclared workload](../benchmarks/WORKLOAD_STREAM_HARDENING.md) retains all
original numerical gates. The original Candidate 3 control adds diagnostic
headers only; the new candidate changes the API. Both reuse the exact Candidate
3 producer and worker images. The frozen Phase 5 baseline is retained for each
fresh pair. Differences in API versions are intentional architecture comparisons,
not additional Kafka or ClickHouse capacity claims.

Control attribution is complete (see below). A passing new candidate diagnostic pair,
full promotion campaign, recovery/status regression checks and retained-volume
cutover acceptance remain required before a default change. No gate is waived
because its failure cause is understood.

The October 10 control attempt stopped after its frozen-baseline case: the final
declared supplemental observation still had 106,302 acknowledged spans missing.
Candidate 3's control case was not run. Preserve
[`20261010-candidate3-query-control`](../benchmarks/results/20261010-candidate3-query-control/campaign.json)
as a failed attempt, not a passing pair. A separate retained-volume restart and
[identity reconciliation](evidence/query-hardening/control-baseline-recovery/supplemental-drain.json)
then verified all 1,013,310 acknowledged/emitted spans, with zero unknown or
duplicate logical identities. This establishes eventual recovery for that
attempt; it does not rewrite the failed observation deadline or establish a
promotion result. The 27,456 planned but never emitted spans are generator
shortfall, not acknowledged telemetry loss.

## Query candidate 1 diagnostic result

The fresh [matched 8,000-span/s pair](../benchmarks/results/20261010-candidate3-query1-probe/campaign.json)
completed without changing the workload, quotas or gates. Both cases recovered
all 1,040,574 acknowledged spans; baseline recovery required its already-declared
supplemental observation. The [exact gate evaluation](evidence/query-hardening/query1-probe-gates.json)
passes five gates and fails the same three downstream gates:

| Metric | Frozen baseline | Query candidate 1 |
| --- | ---: | ---: |
| Admitted measured-fixture spans / 120 s | 959,742 / 120 | 959,742 / 120 |
| Completed database rows/s | 3,785.6 | 6,278.45 |
| ACK p95 / p99 | 69.720 / 188.169 ms | 59.386 / 119.321 ms |
| Drain upper observation bound | 81.981044 s | 20.973485 s |
| Censored visibility samples | 491 / 500 | 312 / 500 |
| Non-404 failures / issued queries | 133 / 480 | 132 / 390 |
| Scheduled query drops | 0 / 480 | 83 / 480 |

The [per-request diagnostic categories](evidence/query-hardening/query1-failure-attribution.json)
show no detector input-limit rejection in the new candidate. All 832 measured-phase
visibility-probe 503s were API admission-slot rejections. Scheduled queries started
inside the measured window had 83 admission rejections, 46 incident deadlines and
three other incident failures. These denominators differ deliberately: visibility
traffic is separate from the paced query gate, and late scheduled requests are
reported in scheduling evidence.

The retained ClickHouse query log [separates detector phases](evidence/query-hardening/query1-sql-phase-diagnostics.json):
all 88 statistics queries finished, but only 37 of 88 evidence-selection queries
finished. The other 51 evidence-selection queries recorded processing exceptions;
the recorded HTTP deadlines and phase timing identify this selection step as the
next bottleneck. Its window-function plan orders the large SERVER corpus before
selecting five distinct traces. Selected-trace context reads all finished. The
next experiment compares exact alternative selection plans on the retained corpus,
without changing evidence ordering, sample count, statistics or resource limits.

## Query candidate 2: ordered distinct evidence

The retained-corpus comparison rejected a per-trace aggregate/top-five plan:
all three trials exceeded the unchanged 256-MiB query-memory limit. The ordered
`LIMIT 1 BY` trace/operation followed by ordered `LIMIT 5 BY` operation plan
finished all three trials, returning the same 15 evidence rows as the completed
window-function query. Its timings were 1,039, 961 and 3,228 ms; the original
window plan finished once in 4,342 ms and exceeded five seconds twice.
These are isolated selection trials, not full-load capacity measurements.
The [verified comparison](evidence/query-hardening/seed-selection-verified-comparison.json)
explicitly excludes partial HTTP 200 responses containing an in-band database
exception; the original raw trial flags are retained and superseded.

The new selection plan passed the [six real ClickHouse tests](evidence/query-hardening/query2-real-tests.txt)
and [focused race-enabled Go tests](evidence/query-hardening/query2-unit-tests.txt).
The distinct-trace test verifies the full five-trace order, including multiple
eligible spans from one trace. No ingestion, quotas, timeouts, statistics,
evidence count or promotion gates changed. The [source/image freeze](evidence/query-hardening/query2-freeze.json)
records the API revision and identical Candidate 3 producer/worker image IDs.

A guarded [campaign sequence](evidence/query-hardening/query2-campaign-sequence.json)
runs a fresh paired smoke check and the unchanged 8,000-span/s diagnostic pair.
It starts the full comparison only if provenance, recovery and all eight
diagnostic gates pass. Completion of that sequence alone does not authorize a
default change: full analysis and operational acceptance still apply.


### Query candidate 2 diagnostic outcome

The [completed matched pair](../benchmarks/results/20261010-candidate3-query2-probe/campaign.json)
again passed five gates and failed three: admission, visibility censoring, and
scheduled query drops. Its query-error fraction passed this pair. Full promotion
was not started. The [reproducible summary](evidence/query-hardening/query2-probe-summary.json)
retains exact gate inputs and separate HTTP cause denominators.

| Metric | Frozen baseline | Query candidate 2 |
| --- | ---: | ---: |
| Admitted measured-fixture spans / 120 s | 762,174 / 120 | 561,024 / 120 |
| Completed database rows/s | 1,393.6 | 2,271.55 |
| ACK p95 / p99 | 808.999 / 1,014.914 ms | 901.741 / 972.496 ms |
| Drain upper observation bound | 169.026213 s | 55.936598 s |
| Peak observed Kafka lag, records | 3,344 | 6,422 |
| Censored visibility samples | 398 / 398 | 272 / 289 |
| Non-404 failures / issued queries | 167 / 467 | 115 / 382 |
| Scheduled query drops | 13 / 480 | 91 / 480 |
| Full-run acknowledged and eventually stored spans | 832,062 | 627,072 |

Both cases recovered every acknowledged/emitted identity within their declared
supplemental observations. Planned-but-never-emitted spans were 208,704 and
413,694, respectively. No permanent acknowledged loss was observed. These results
vary substantially from the previous pair; cross-run timing differences are not
attributed solely to a query revision. Repeated/reversed full comparisons remain
required, and this failed pair is not discarded.

There were no detector-input-limit headers. Candidate paced queries recorded
92 admission rejections, 22 deadlines and one other incident failure. Visibility
probes recorded 830 admission rejections. The [retained SQL phase summary](evidence/query-hardening/query2-sql-phase-diagnostics.json)
shows all 85 statistics queries completed (median 605 ms). Evidence selection
completed 69 of 85 calls (successful median 2,002 ms); 16 recorded processing
exceptions. Selected-context reads completed 61 of 69 calls. The next comparison
narrows selection to each exact candidate operation, retaining the same ordering,
evidence count and common evaluation deadline.

## Query candidate 3: selective operation evidence

The new revision uses one sequential evidence-selection query for each reported
candidate operation. Namespace/service/time sorting keys prune the FINAL read;
operation and span kind are filtered after replacement. The exact priority tuple
followed by `LIMIT 1 BY trace_id LIMIT 5` retains the top five distinct traces.
One original query slot, receipt cutoff and five-second context cover the entire
evaluation. Any read failure or cancellation discards already-selected evidence.
The existing 500-group cap bounds fanout; high cardinality may still time out.

[Three retained-corpus trials](evidence/query-hardening/seed-selective-trials3/results.json)
matched the grouped query's 15 evidence rows exactly. Sequential distinct selection
took 1.840, 0.303 and 0.176 seconds versus grouped selection's 3.640, 2.622 and
1.276 seconds, with a lower observed per-query memory peak. The slower first trial
followed database restart. The earlier startup-before-readiness attempt is retained
as unusable performance evidence. No sustained-load benefit is claimed yet.

All [seven real ClickHouse tests](evidence/query-hardening/query3-real-tests.txt)
passed. The added test covers multiple operations in one service, another service,
shared trace IDs, more than five traces, all ordering tie-breakers, replay before
merges and late receipt. [Focused race-enabled tests](evidence/query-hardening/query3-unit-tests-final.txt)
passed, including deterministic cancellation after an operation read: no subsequent
query starts and no partial seeds are returned.

The [freeze](evidence/query-hardening/query3-freeze.json) records unchanged Candidate
3 producer/worker images and the new API. The [guarded sequence](evidence/query-hardening/query3-campaign-sequence.json)
started October 11 with fresh paired smoke, then the original diagnostic workload.
The full campaign runs only if every diagnostic gate passes. Defaults and Phase 6
remain unchanged while performance and operational acceptance are pending.

### Query candidate 3 diagnostic outcome

The [completed diagnostic pair](../benchmarks/results/20261011-candidate3-query3-probe/campaign.json)
passed seven gates. Scheduled query drops remained the sole failure, so the full
campaign did not start. [Exact measurements and cause counts](evidence/query-hardening/query3-probe-summary.json)
retain both cases and unchanged gates.

| Metric | Frozen baseline | Query candidate 3 |
| --- | ---: | ---: |
| Admitted measured-fixture spans / 120 s | 703,998 / 120 | 959,934 / 120 |
| Completed database rows/s | 1,491.2 | 6,219.1 |
| ACK p95 / p99 | 926.559 / 1,106.557 ms | 325.465 / 444.878 ms |
| Drain upper observation bound | 100.958704 s | 22.931508 s |
| Peak observed Kafka lag, records | 2,952 | 4,523 |
| Censored visibility samples | 363 / 363 | 218 / 500 |
| Non-404 failures / issued queries | 169 / 473 | 49 / 427 |
| Scheduled query drops | 7 / 480 | 49 / 480 |
| Full-run acknowledged and recovered spans | 784,446 | 1,040,766 |

The candidate recovered every planned span at its original final snapshot;
the baseline recovered every acknowledged/emitted span within its declared
supplemental observation. Candidate paced failures were exclusively admission
rejections. Its 97 successful incident queries had p95 3.904 seconds; successful
trace-search p95 was 2.304 seconds. Zero incident deadlines does not eliminate
query-worker occupancy or excuse the scheduling failure.

## Direct attribution of the original Candidate 3 failures

A [fresh diagnostic control pair](../benchmarks/results/20261011-candidate3-query-control2/campaign.json)
completed using the archived original Candidate 3 API with diagnostic headers
only and identical producer/worker images. It does not replace either the
historical failed control attempt or any new-query promotion pair. Both cases
recovered all 1,040,766 planned/acknowledged spans. The [summary](evidence/query-hardening/control2-summary.json)
counts actual failure headers, with paced queries restricted to starts inside
the measured window and visibility probes reported separately.

Of 101 original-Candidate-3 incident HTTP 503s, 81 (80.2%) were detector input-limit
rejections and 20 were API admission rejections. Across all four paced routes,
81 of 217 HTTP 503s (37.3%) were detector input-limit failures, 83 were admission
rejections, and 53 were trace-detail ClickHouse code 307 failures. Code 307 is
[`TOO_MANY_BYTES`](https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/ErrorCodes.cpp),
a separate database bound, not the detector's 32-MiB input accounting limit.
All 650 measured-phase visibility-probe 503s were admission rejections.

These are directly measured fractions for this controlled repeat. They cannot
retroactively assign an exact fraction to the earlier uninstrumented 95 incident
503s. The diagnostic header groups the detector's input bounds; the real large
fixtures separately prove rejection by the 100,000-row and 32-MiB ceilings.
New query candidates have not recorded that detector-limit category in their
measured paced requests. All failure categories remain in the original gates.
