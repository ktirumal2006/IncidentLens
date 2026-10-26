# ADR 0005: Harden Kafka ingestion and gate its promotion to the default

Status: implementation retained; default promotion rejected for frozen candidate 3,
October 9, 2026. The bounded diagnostic failed visibility and query error/drop
gates. Further tuning stops; the direct default remains. See the
[promotion decision](../PHASE5_PROMOTION_DECISION.md). No new capacity claim is made.

Subsequent scope: the user authorized a separate downstream query-hardening goal
on October 9, governed by [ADR 0006](0006-bounded-detector-query-plan.md). It keeps
Candidate 3 ingestion and all promotion gates unchanged. The failed decision
above remains historical; a new result is required for reconsideration.

## Context and objective

The user explicitly selected Kafka as IncidentLens's target architecture after
[the Phase 5 retrospective](../RETROSPECTIVE_PHASE5.md). This supersedes the
previous preference for the direct default once the gates below pass. The
retrospective proved better admission during bursts and storage slowdown, but
also consumer backlog, delayed query visibility and mixed query failures. The
current worker pays for a synchronous ClickHouse write and Kafka commit per
record. It has one partition and no product-visible progress signal.

The existing streaming source and instrumented images are frozen in
[the baseline manifest](../../benchmarks/baselines/phase5-streaming-20261003/manifest.json).
The archive includes uncommitted retrospective instrumentation. Historical
measurements and the reason for the original educational activation remain intact.

## Decision

Keep the existing Go, Kafka, ClickHouse, Collector and React stack. Add bounded
consumer batching and partition parallelism, retain write-before-commit replay,
and expose ingestion progress through the existing API and UI. Kafka becomes the
default only after the declared acceptance tests and matched benchmarks pass.
The direct implementation remains a benchmark/reference, not a second product
mode presented as equally current. No Phase 6 signal ingestion is introduced.

### Partitioning and ordering

Create a new `incidentlens-spans-v2` topic with exactly four partitions and RF1;
never increase v1 partitions in place or reinterpret its offsets. Route normalized
rows by a documented stable hash of trace ID modulo four, grouping all request
rows for each partition before bounded envelope encoding. Keep the existing
256-row/4-MiB envelope limits, preflight every chunk before any publish, and
acknowledge only after every valid chunk receives broker acknowledgment.
The routing function is FNV-1a 32-bit over the 16 decoded trace-ID bytes, then
modulo four. It does not hash the printable hexadecimal string. All preflighted
partition records are submitted together to the bounded producer before waiting
for their acknowledgments; a partially acknowledged export remains retryable
with its original span identities.

All spans of a trace map to one partition. Ordering is broker arrival order in
that partition, not source event-time order or universal order across concurrent
exports. Trace reconstruction already uses immutable IDs, parent links and event
timestamps; it must tolerate out-of-order arrival and partial visibility. A large
trace may span records and remains incomplete until its remaining spans arrive.
No cross-partition transaction or whole-trace atomic visibility is claimed.

Use fixed routing and verify expected partition count at startup. Topic migration
requires stopping new v1 input, draining/reconciling retained v1 work, and then
cutting over to v2 with a new group. Preserve v1 data for bounded replay. Four
partitions share the original nominal retention-byte budget (256 MiB each,
one-hour time retention, 16-MiB segments); a skewed hot partition may exhaust its
own retention earlier. Segment deletion remains asynchronous, not a hard quota.

### Consumer batching and ownership

Candidate 3 uses one process with one synchronous ClickHouse write at a time,
retaining its 1-CPU/512-MiB quota and the existing 2-CPU/2-GiB ClickHouse quota.
Build bounded cohorts of polled records; aggregate small records into up to
4096-row, 8-MiB accounted batches, at most 64 decoded records, with a 50-ms normal
coalescing wait and a 2048-row target. The fetched response may contain more than
64 records and is split into these write cohorts without releasing rebalance
protection. A write can combine assigned partitions while preserving the order
within each partition and one shared write-success boundary. This configuration
is under verification; default promotion remains conditional on acceptance.

Candidate 1 assigned partitions to two serial write lanes, with at most two
concurrent inserts and a 1024-row target, but failed its first complete
high-load pair. Its median insert size was 480 rows and ClickHouse insert
latency rose despite lower worker CPU use. Candidate 2 therefore uses **one
synchronous insert for the complete existing bounded cohort**, with all limits,
partition routing and resource quotas unchanged. It tests whether fewer inserts
and less write contention improve useful throughput. Separate consumer-group
members can still process their assigned partitions; no intra-worker parallel
write benefit is assumed. The original two-lane implementation and failed
measurements remain archived. Candidate 2 still requires all promotion gates.

Candidate 2's October 8 diagnostic pair passed admission, ACK and completed-row
throughput gates, but still failed drain, visibility and query failure/drop
gates. Of 540 measured writes, 280 were smaller than the 1,024-row target, often
following a target-sized write within the fetched work. Candidate 3 therefore
raises only the target to 2,048 rows, preserving the 4,096-row/8-MiB/64-record
hard bounds, 50-ms coalescing, single writer, fetch guards and resource quotas.
Its benefit requires a fresh matched comparison; neither earlier candidate is
accepted as the default.

Keep broker response/decompression and total cohort memory bounded, including
compressed input. Do not rely on row count alone to bound arbitrary attributes.
No detached unbounded queues or speculative storage concurrency are allowed.
Preserve the direct writer's 256-row cap through a separate bounded streaming
batch method. Keep synchronous inserts and FINAL duplicate-safe reads.

Hold `BlockRebalanceOnPoll` across every fetched record in a cohort until all
writes and their contiguous offsets are resolved. Commit the highest contiguous
processed record per partition only after all rows covered by it have been
successfully written. Coalesce commits rather than committing each record.
A failed/ambiguous write is eligible for full replay. Never advance past a poison
record, retained-offset gap or unsuccessful write. On cancellation or generation
loss, stop and release ownership without a false commit; replay may duplicate
physical rows but must not change unique reads or detector results. Do not fetch
more while downstream retry keeps the current cohort outstanding. Tests must
exercise rebalances while writes are stalled and crash after write/before commit.

The pinned franz-go [ownership documentation](https://github.com/twmb/franz-go/blob/v1.22.1/pkg/kgo/config.go)
warns that blocked rebalances can outlast the group timeout; implementation and
tests must handle this failure instead of assuming a lock preserves membership
forever. ClickHouse's [bulk-insert guidance](https://clickhouse.com/docs/optimize/bulk-inserts)
motivates reducing small inserts; measured gains remain a hypothesis, and the
bounded local batch size is a deliberate latency/memory compromise.

### Visible ingestion progress

A worker status endpoint samples end and committed offsets for every expected
partition with a bounded, independent read-only Kafka client. Publish aggregate
lag in **records**, observation times and partition coverage. Missing, inconsistent,
uninitialized or stale observations produce unknown lag, never an invented zero.
The sole uninitialized-offset exception is a verified pristine partition with
log start and end both zero: it contains no broker records, so its broker backlog
is known to be zero. Preserve an explicit `empty_partition_uninitialized` caveat
that the consumer has not committed a starting position; never fabricate a
consumer commit or interpret this as proof of consumer health. A nonempty
partition without a commit still has unknown lag.
Expose worker write/commit progress, retry/error state and processing age with
explicit timestamp scope. No arbitrary broker/storage errors or payloads reach
the browser. Processing age is an observation about pending/completed work, not
a promise that all traces are queryable; wall-clock skew must remain explicit.

The API proxies a fixed internal status URL with a short timeout, bounded response
and bounded caching; the UI polls without overlapping requests and displays
backlog, unavailable/stale status and the observation time. Positive lag can
include written but uncommitted work; zero observed lag does not prove complete
telemetry. An unavailable sampler must not leave stale green status. This is
operational state of trace delivery, not metrics/log ingestion or a new datastore.

## Alternatives

- Batch within one partition: simplest way to reduce write/commit overhead, kept
  as a possible measured alternative if partition parallelism adds no value.
- More consumer processes or four concurrent writers: greater resource contention
  and ownership complexity; defer unless measured evidence requires them.
- ClickHouse async inserts: another buffering/acknowledgment boundary; retain
  synchronous client batching to keep recovery semantics inspectable.
- Collector persistence or direct ingestion: viable reference alternatives, but
  the current user request explicitly chooses Kafka's retained log and admission
  boundary. No Redis, new broker, database, orchestration platform or signal is
  needed for this hardening.

## Acceptance and consequences

The [predeclared hardening workload](../../benchmarks/WORKLOAD_STREAM_HARDENING.md)
contains the quantitative promotion criteria. Require zero unresolved admitted
loss after bounded recovery, duplicate-safe replay/detector checks, safe partition
ownership, bounded memory and truthful product status. Run baseline and candidate
on identical hardware, fixtures, quotas and query budgets with fresh volumes,
repeated cases and reversed order. Report admission and persistence separately,
including query statuses, visibility censoring, lag and drain. Record failed
attempts and never tune quotas or discard failure evidence to obtain a pass.

Only after those gates pass, change local Compose/startup/runbooks and product
architecture to Kafka by default; retain a documented direct reference setup.
Migration and rollback preserve volumes and reconcile in-flight work. A passing
local result still has finite retention, one broker, no disk-loss/HA guarantee,
and no proof that ClickHouse's underlying capacity increased.

The October 7 interrupted comparison demonstrated the retention limit directly:
Docker remained unavailable beyond the one-hour retention, and 7,734 acknowledged
fixture spans were missing after the log expired. The worker failed closed on the
offset gap. This [retained failure evidence](../VERIFICATION_STREAM_HARDENING.md#interrupted-attempt-and-retention-loss)
must accompany any promotion decision; successful bounded recovery tests cannot
be presented as protection against outages longer than the retained log.
