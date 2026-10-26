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

## Candidate 2 declaration — October 7, 2026 UTC

Candidate 1's completed first 8,000-span/s pair in
`results/20261007-hardening-candidate1-retry1/` failed the promotion gates. It
completed 3352.6 write rows/s against baseline 2912 rows/s, but failed the 1.25x
gate, ACK tails, half-drain and half-censoring gates, and query error/drop gates.
This campaign was later interrupted before its burst candidate measured load;
completed results and source/image identities remain evidence, not acceptance.

Candidate 2 tests one change to application behavior: **one synchronous
ClickHouse write per existing cohort**, replacing the two concurrent lane writes.
Keep four trace-ID partitions, 50-ms coalescing, 1024-row target, 4096-row hard
limit, 8-MiB accounted limit and 64-record cap. Keep one-byte fetch budgets and
the existing decompression/read bounds. Keep every CPU/memory quota, producer
limit, query budget, retention setting, workload and numerical gate unchanged.
No offset is committed before the complete cohort write succeeds; ambiguous
partial writes replay the whole uncommitted cohort with duplicate-safe reads.
Partition ownership still permits separate consumer-group members to process
their assigned partitions safely; this candidate removes intra-worker write
parallelism because its benefit was not demonstrated.

Before the full campaign, run the same low-rate paired smoke and an explicitly
diagnostic `--probe-sustained` pair: baseline then candidate, repetition 1 of
the unchanged 8,000-span/s case. A probe cannot establish promotion. If it
warrants continuing, run all 16 declared cases and both repetitions unchanged.
Preserve a failed probe rather than tuning thresholds to accept it.

The runner may resume an interrupted campaign only with identical source,
declaration, binary and image hashes. Retain completed matched pairs; archive
any interrupted/incomplete pair and rerun both sides from fresh volumes and
namespaces in the declared order. Keep attempt history and retained volumes.
Do not reuse a lone baseline across an interruption, mix candidates, or count
archived attempts as completed acceptance cases.

## Candidate 3 declaration — October 8, 2026 UTC

Candidate 2's diagnostic pair completed with all admitted identities recovered,
but failed drain, visibility censoring, and query failure/drop gates. Its 540
completed measured writes contained 418,464 rows. Of these, 280 writes held
fewer than 1,024 rows; operation sequences commonly show a target-sized write
followed by a smaller remainder from the same bounded fetched work. Candidate 2
and its complete diagnostic evidence remain archived, not promoted.

Candidate 3 changes one application setting: **increase the cohort row target
from 1,024 to 2,048** to combine more of those writes. Keep one synchronous writer,
the 4,096-row hard limit, 8-MiB accounted limit, 64-record cap, 50-ms coalescing,
four trace-ID partitions, fetch/decompression bounds, and commit-after-write
semantics unchanged. Keep producer limits, CPU/memory quotas, retention, query
budgets, workload and every numerical gate unchanged. The hypothesis is reduced
insert/commit overhead and contention, not increased ClickHouse capacity.

Verify the changed batch boundary with unit and real-dependency tests, then run
a fresh paired smoke and the same diagnostic `--probe-sustained` pair. Proceed
to the full 16-case campaign only if the diagnostic warrants it. Do not reuse
candidate 2's baseline or reinterpret its failed gates as passing.

Capture additional bounded ClickHouse SELECT diagnostics after load and recovery
for both variants, alongside the existing post-load INSERT diagnostics. Read only
the fresh case's query-user SELECT log, capped at 50,001 rows with a five-second
server query budget; record whether that cap was reached. Retain timestamps,
row/byte counts, duration, memory and exception codes, excluding SQL and exception
text. This may distinguish detector-limit-sized results from server exceptions;
it cannot join every HTTP error to its cause or observe API admission rejections.
It adds no requests during measured traffic, changes no query caps, and is not
a new acceptance gate. Candidate 2's observed correlation between incident 503s
and the 100,000-span detector cap remains a limitation, not a passing result.

## Bounded promotion decision — October 9, 2026 UTC

The user has frozen candidate 3 and prohibited another candidate or Phase 6
without a separate request. Preserve its application source and immutable image
IDs. The previous candidate 3 diagnostic ended before candidate load because
Kafka's Java protocol health check repeatedly exceeded five seconds, despite
the broker reporting startup. An isolated successful probe under the unchanged
2-CPU/1-GiB broker quota took 12.394 seconds.

Apply the readiness-only fix identically to both variants: keep the Kafka API
protocol check, allow a 20-second timeout, use a 30-second interval and startup
grace period, and five retries. Broker heap, CPU/memory quotas, application images,
topic settings and workload remain unchanged. The longer interval changes health
probe overhead equally on both sides; earlier pairs are not substituted for this
new comparison. Verify readiness success and bounded failure before measuring.

Run one fresh matched 8,000-span/s diagnostic pair with the shared fixed readiness
configuration. Use all existing numerical gates unchanged. Proceed to the full
16-case campaign only if every applicable diagnostic numerical gate passes and
its identity/evidence checks are complete. Otherwise publish the failed criteria,
retain the direct default, and stop tuning. An infrastructure failure is not a
performance verdict; preserve it and resolve the blocker to obtain this one valid
pair without changing candidate 3.

Report detector input-limit pressure separately from deadline/transport/storage
evidence and unclassified HTTP errors. The frozen API maps several causes to the
same 503 response; captured SELECT diagnostics do not provide a per-request join.
Do not relabel unknown 503s as detector caps or discard cap-related failures from
the original query-error gate. Default promotion still requires full acceptance
and retained-volume startup/cutover checks; a diagnostic alone cannot promote it.

## Query-hardening extension — October 10, 2026 UTC

The user's separate query-hardening goal supersedes the prior stop instruction
for downstream reads only. Candidate 3's failed promotion and every original
gate remain unchanged. The first query variant implements ADR 0006: exact
ClickHouse detector statistics, bounded supporting traces with explicit sampled
relationship coverage (`trace-v2`), immutable-key detail PREWHERE, and bounded
failure diagnostics. It adds no schema, index, cache, technology, ingestion
tuning, concurrency increase, deadline increase, or runtime quota increase.

Use tag `candidate3-query1` for the variant. Producer and worker tags must alias
the exact frozen Candidate 3 image IDs, not rebuild ingestion from the working
tree. Build only the API image. Record this identity check before smoke and
before the measured diagnostic. Preserve all frozen baseline image IDs. The
common benchmark binary adds only an allowlisted response failure-cause field
to query and visibility-probe events; pacing, windows, deadlines, queues,
visibility criteria, analysis denominators, and numerical gates do not change.
Both variants use this same binary; the frozen baseline has no cause header.

Run focused real-dependency correctness and old-ceiling diagnostics, a fresh
paired smoke, then the same 8,000-span/s diagnostic pair. If its applicable
promotion gates all pass, run the full unchanged 16-case campaign before default
and retained-volume cutover acceptance. Preserve failed attempts and measurements.
Classify any new failure by evidence before changing implementation; never remove
503s, transport failures, scheduled drops, or censored visibility samples from
their gates because their cause is understood.

This compares the frozen Phase 5 architecture with Candidate 3 ingestion plus
the new query architecture. Different API versions are intentional and must be
reported. Improvements cannot be attributed solely to Kafka, and the new caveat
scope must not be described as full `trace-v1` response equivalence. Exact
statistics, rule thresholds, evidence selection, and query status gates remain
the relevant preserved contracts. No measured result is claimed by this
declaration.

For direct failure attribution, first run an additional diagnostic pair with
tag `candidate3-query-control`: frozen Phase 5 baseline versus original Candidate
3 with only failure headers/admission-cause tagging. The control source is
extracted from Candidate 3's archived source; original detector, detail SQL and
frontend stay unchanged. Its patch and per-file hashes are retained under
`docs/evidence/query-hardening/control-source.*`. Producer/worker image IDs remain
exact Candidate 3 aliases. This control does not qualify as the new query
candidate or replace its matched baseline; it exists to count input-cap versus
admission/database/deadline failures under the unchanged offered workload.
