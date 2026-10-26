# Phase 5 hardening: declared comparison and promotion gates

Declared October 3, 2026 before implementation and measurements. This compares
the frozen Phase 5 single-partition streaming baseline against the candidate in
[ADR 0005](../docs/adr/0005-hardened-streaming-default.md). Prior retrospective
runs remain historical evidence, not interchangeable with the new paired runs.

## Controls and workload

Use the same host/Docker VM, pinned runtime images, six-span/three-service trace
fixture, 256-byte attribute payload, 32-trace export batches, 16 caller workers,
64-batch queue, eight-second RPC deadline, three attempts, 120-second caller
drain, 35-second original settling and separate 120-second supplemental drain.
Use fresh namespaces, ClickHouse volumes and Kafka volumes for every case. Retain
all raw identities/events, operation timings, offset samples, resource limits,
source/image hashes and failed attempts. Preserve existing product volumes.

Keep Kafka at 2 CPU/1 GiB, producer and worker each 1 CPU/512 MiB, ClickHouse at
2 CPU/2 GiB, API at 1 CPU/512 MiB and Collector at 1 CPU/256 MiB. The candidate
changes partition routing and bounded batching/concurrency, not these limits.
The new status sampler/UI are part of the candidate; record their overhead.
The driver sends one additional bounded status request after each two-second
interval in both variants (the frozen baseline may return 404). Preserve its
response codes, bodies and request time bounds separately from the four query
requests per second. The native benchmark binary is rebuilt from source verified
against the frozen harness after host temporary-file cleanup; the same pinned-Go
binary is used on both sides of every new pair.

| Case | Offered spans/s | Warmup | Measured duration | Query rate |
| --- | ---: | ---: | ---: | ---: |
| Sustained | 2000, 8000 | 10 s | 120 s | 4/s |
| Burst | 32000 | 0 | 10 s | 4/s during offering |
| ClickHouse CPU slowdown | 2000 | 10 s | 120 s | 4/s |

For slowdown, change the owned ClickHouse quota from 2 to 0.25 CPU at load+30s,
hold 30s after verification, then restore 2 CPU. Retain exact command bounds.
Use the original services/traces/detail/incidents route mix and 128 setup traces.
Two repetitions reverse baseline/candidate order in repetition 2: 16 measured
cases total. Run a separate low-rate smoke before the measured campaign. Avoid
unrelated builds/tests/Demo traffic during load. Explicitly release stopped
experiment networks without removing volumes to avoid the previous address-pool
exhaustion; record cleanup and original/final stack restoration.

Retain existing two-million-span and 256-MiB event caps, host free-space floor,
ClickHouse growth, sustained-memory, OOM/restart and readiness stops. Preserve the
2-GiB Kafka actual-volume safety stop and 1-GiB free-space floor. Topic retention
is 1 GiB nominal aggregate for both configurations, distributed across four
candidate partitions. Stop on incomplete cleanup or unsafe conditions.

## Required measurements

- Generated, emitted, admitted spans and exact admitted spans/s; original and
  supplemental unique persisted identities and physical completed write rows/s.
- OTLP first-send-to-ACK p95/p99 and retries; producer and consumer throughput.
- Per-partition and aggregate Kafka lag in records, plus admission/write progress.
  Record counts change with routing, so do not compare raw lag ratios as if record
  sizes were equal. Report exact span identity backlogs at reconciliation too.
- Peak sampled lag, bounded post-offering drain, client write phase timings and
  server INSERT diagnostics where available.
- Query visibility observation bounds/censoring, successful query p95 and error
  statuses, scheduled query drops/skips/late starts and separate probe traffic.
- Never-emitted input, unresolved original-deadline identities and final permanent
  loss evidence. A missing deadline is not automatically permanent loss.

Use integer counts/nanoseconds and exact rational rates. Label wall-clock and
polling uncertainty. Do not fabricate or round measurements.

## Predeclared promotion criteria

Correctness gates are mandatory, independent of performance:

1. Every admitted identity is present after the bounded recovery observations;
   no skipped offsets, poison records or retention gaps. Every emitted identity
   is either stored or explicitly explained by a terminal rejection/failure.
2. Real-dependency tests pass for trace routing, cross-record batches, partial
   write failure, crash after write/before commit, consumer restart, rebalance
   under stalled writes, broker outage/restart, replay and duplicate-safe query/
   detector results. Observe resource bounds and shutdown without false commits.
3. The UI/API show growing lag and delay under a controlled stalled consumer,
   recover after drain, and show unavailable/stale status on sampler/worker failure.
   A zero/unknown backlog cannot be presented as proof of complete telemetry.
4. A clean default Kafka startup and retained-volume cutover/rollback procedure
   pass finite reconciliation before declaring the default promoted.

For both repetitions, at matched 8000 spans/s sustained load:

- Candidate admission is at least 95% of baseline admitted spans; ACK p95/p99 is
  no greater than max(1.25 times its paired baseline value, 250000000 ns).
- Completed-window ClickHouse rows/s is at least 1.25 times baseline, and the
  post-offering drain upper bound is at most half baseline's upper bound. This
  tests reduced processing overhead, not a higher underlying storage ceiling.
- Censored visibility probes are at most half baseline's censored fraction; if
  baseline has zero censoring, candidate must also have zero. Compare conditional
  observation tails alongside this fraction so fast survivors cannot hide misses.

Across both burst and slowdown repetitions, admission is at least 95% of baseline;
all admitted work recovers within the declared bounds. At 2000 sustained spans/s,
require no visibility censoring and observed visibility p95 <=5000000000 ns.
Report all query failures; do not relax detector caps. For each matched case,
non-404 query failure fraction must not increase by more than 1/20 absolute, and
query scheduling drop fraction must not increase by more than 1/20 absolute.
404 rates are evaluated as visibility penalties alongside the visibility probes.

If a criterion fails, retain and explain the result, investigate within this scope,
and predeclare any subsequent changed candidate or protocol before rerunning.
Do not change the default merely because Kafka is the target or because some
benchmarks improved. No Phase 6 work is authorized.

## Setup observation amendment — October 7, 2026 UTC

The first measured attempt stopped after the first pair: candidate admission
began between setup acknowledgment and the consumer's first commit. Its lag
observer correctly reported an uninitialized nonempty group during the first
load sample. All 260766 identities were stored, but the strict evidence check
rejected this unknown observation. The original campaign and volumes remain
unchanged at `results/20261006-hardening-candidate1/`.

Before the next campaign, add the same bounded setup barrier to both variants.
After the existing 128 setup traces are acknowledged, the native harness reads
its case's independent offset observer and waits at most 30 seconds for two
fresh, initialized zero-lag samples with the same positive end offset. Only
then may warmup/offering start. Failure prevents offering; unknown offsets are
never converted to zero. Preserve the barrier event and every raw lag sample.
No additional fixture spans, broker writes, offset changes, resource quotas,
query budgets, or performance thresholds are introduced. All 16 cases are rerun
with the same amended harness binary on both sides; the frozen application
images and candidate 1 application images stay unchanged. Earlier harness
results are retained separately and are not mixed into the new pairs.
