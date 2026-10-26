# ADR 0006: Exact detector statistics with bounded trace evidence

Status: accepted for implementation and verification, October 9, 2026.
Performance acceptance and Kafka default promotion remain pending.

## Context

The user authorized a separate query-hardening goal after the frozen Candidate 3
[decision](../PHASE5_PROMOTION_DECISION.md). That decision remains a failed
promotion, with five diagnostic gates passed and visibility, non-404 query
failures, and scheduled query drops failed. Kafka admission, batching, partition
ownership, recovery, and runtime quotas are frozen for this work.

The detector currently transfers every deduplicated span in a 35-minute interval
to Go. It rejects the entire evaluation above 100,000 spans or 32 MiB of accounted
input. The decision pair contains 75 candidate SELECTs returning 100,001 rows
and 95 incident HTTP 503s. This demonstrates repeated exposure to the row ceiling,
but cannot assign every 503 to it: the four shared query slots, ClickHouse limits,
and other failures use the same HTTP response. Many visibility-probe 503s return
within milliseconds. Probe trace IDs and time windows are valid.

Only SERVER operation/window statistics determine rule firing and ranking.
However, `trace-v1` also computes topology caveats for every observed trace,
including traces never selected as supporting evidence. Retaining that exhaustive
topology scan would retain substantial work unrelated to computing the rules.

## Decision

Introduce the explicitly versioned `trace-v2` evaluation contract:

1. Compute exact SERVER counts, ERROR/UNSET counts, and discrete p95 per operation
   and window in ClickHouse over `FINAL` rows. Keep the same windows, observation
   cutoff, minimum samples, exact threshold arithmetic, candidate ordering, and
   report-only service filters. No approximate percentiles or sampled statistics.
2. Select at most five distinct current-window traces per reported candidate,
   with the existing ERROR/duration/start/trace/span ordering. Fetch full observed
   trace context within the same 35-minute interval and receipt cutoff. Preserve
   cross-service candidate context, relationship checks, source-drop caveats,
   and error-propagation semantics for these selected traces.
3. Explicitly mark relationship context as sampled and operation trace context
   as not exhaustively evaluated. Missing roots/parents, cycles, and source drops
   outside the inspected evidence are not asserted absent. A normal operation
   still means only that no statistical rule fired with sufficient samples.
   This is a documented change in caveat coverage, not full JSON equivalence to
   `trace-v1`; numerical rules and selected trace evidence retain their contract.
4. Bound aggregate groups at 500 and selected trace input at the existing
   100,000-row/32-MiB bounds. Preserve the 8-MiB response budget, four admission
   slots, five-second HTTP deadline, and unchanged ClickHouse query profile.
   Any exceeded bound fails the whole evaluation; no partial statistics or
   silently truncated evidence are returned as success.
5. Move immutable trace-ID and event-time detail predicates before `FINAL`
   using explicit `PREWHERE`. Verify replay and boundary behavior against real
   ClickHouse. Add bounded diagnostic failure reasons that distinguish API
   admission, detector bounds, database exceptions, and deadlines without
   recording trace payloads or changing HTTP error/status semantics.

All reads retain the existing immutable-span-payload assumption. A common
receipt cutoff is not a transactional snapshot across statements: merges can
remove an older receipt copy and newly visible writes can affect later reads.
Evidence inconsistencies must fail explicitly rather than fabricate context.

## Alternatives

- Increase the raw span/byte cap: rejected because it increases application
  memory and transfer cost while leaving the same structural failure.
- Page all trace topology: bounds individual pages but still scans/transfers
  every trace and competes with detail queries. It is unnecessary for the
  statistical rules and selected evidence contract.
- Materialized aggregate tables: deferred. Insert-time aggregates over replayed
  rows can double-count before replacement merges; correct receipt-cutoff and
  duplicate handling would add substantial state and operational complexity.
- Cache evaluations or increase concurrency/timeouts/CPU/memory: deferred.
  Caching introduces freshness semantics and larger resource limits would
  confound the existing promotion workload.
- Trace-ID skipping index: deferred until selective reads are measured; it
  would add storage/write cost to a task that should preserve ingestion.

ClickHouse documents the interaction of [PREWHERE and FINAL](https://clickhouse.com/docs/reference/statements/select/prewhere)
and the memory cost of [exact quantiles](https://clickhouse.com/docs/reference/functions/aggregate-functions/quantileExact).
Only immutable sorting-key fields are newly pushed before replacement; exact
aggregation remains subject to the existing database resource limits.

## Verification and consequences

Compare exact statistics, rules, ranking, and selected evidence with the retained
raw evaluator on finite fixtures. Check replay before merges, late receipts,
window boundaries, ties, filtered cross-service evidence, topology/drop caveats,
empty windows, cancellation, and bounded failures. Exercise a corpus above the
old raw-input ceiling with the real query user and unchanged database quotas.

Rerun the existing Candidate 3 promotion workload and acceptance gates with the
same producer/worker images and resource quotas. Preserve the frozen Phase 5
baseline and historical results; the new candidate changes the query architecture,
so any improvement is an architecture comparison, not evidence of additional
Kafka or ClickHouse capacity. Diagnostic cause attribution supplements the
existing measurements and does not change their definitions.

Kafka remains opt-in until every promotion and operational acceptance gate
passes. ADR 0005 governs the eventual default/cutover decision. No Phase 6 work
is authorized by this ADR.

## Query-plan follow-up, October 10

Query candidate 1 passed five diagnostic gates but still failed visibility,
query errors and scheduling drops. Failure headers and the retained SQL log
identified evidence selection as the remaining detector bottleneck: statistics
queries completed, while 51 of 88 window-function evidence selections recorded
processing exceptions. The original input-limit rejection was no longer observed.

Compare exact selection plans on the retained synthetic corpus under the same
query profile. Reject the per-trace aggregate/top-five plan because it exceeds
256 MiB. Adopt ordered `LIMIT 1 BY` operation/trace followed by ordered
`LIMIT 5 BY` operation. Both orders use the unchanged error/duration/start/trace/span
tuple. This avoids the window function's separate partition ordering and returns
exactly the same evidence rows as the successful original query. Three isolated
selection trials finished in 1,039, 961 and 3,228 ms; the original plan finished
once in 4,342 ms and exceeded five seconds twice. These limited trials justify a
new controlled candidate, not a promotion or sustained-capacity claim.
See [raw and verified comparisons](../evidence/query-hardening/seed-selection-verified-comparison.json).
No runtime limit, rule contract or numerical promotion gate changes.

## Selective operation reads, October 11

The ordered grouped query still failed the next diagnostic: visibility and
scheduled drops persisted, and admission also failed its gate. Statistics
completed in all 85 calls, but 16 of 85 evidence-selection calls recorded
processing exceptions. Successful selection took a median 2,002 ms.

Narrow selection to one exact reported operation per sequential read. Push
namespace, service and time sorting-key predicates before FINAL; keep kind and
operation predicates after replacement. Use the same priority tuple and
`LIMIT 1 BY trace_id LIMIT 5`. Close each result before the next read. All reads
share the original admission slot, receipt cutoff and request deadline; any
failure discards the entire result. The existing 500-group bound caps fanout at
500 selection queries. This can still time out for high operation cardinality;
it is not a promise that every bounded input completes within five seconds.

The [retained-corpus trials](../evidence/query-hardening/seed-selective-trials3/results.json)
returned exactly the same 15 evidence rows in all variants. Sequential distinct
selection took 1.840, 0.303 and 0.176 seconds versus grouped selection's 3.640,
2.622 and 1.276 seconds, using at most 21.9 MB per query versus 82.9 MB. The first
trial after restart was slower; these are isolated comparisons, not a capacity
claim. An iterative top-five/excluded-trace alternative was also exact on this
corpus and smaller in memory, but is deferred because it adds repeated-selection
logic that the simpler distinct query may avoid. A startup-before-readiness
attempt is retained as unusable evidence, not a passing performance trial.

Real SQL tests cover multiple operations within a service, multiple services,
shared trace IDs, exact tie ordering, replay before merges and late receipt.
A deterministic cancellation test verifies that previously selected seeds are
discarded and no subsequent operation read starts. Every performance gate and
operational acceptance requirement still applies before promotion.

## Trace-ID index candidate, October 12

Query revision 3 passed seven of eight diagnostic gates. Scheduled drops were
49/480 versus 7/480 in its matched baseline, exceeding the unchanged permitted
five-percentage-point increase. All admitted spans recovered. The completed
original-Candidate-3 diagnostic control attributes 81 of its 101 incident 503s
to the detector input bound; this is direct attribution for that control repeat,
not a retrospective assignment of causes to older runs.

Adopt a small, optional query-schema candidate for verification: a
`trace_id` `bloom_filter(0.001)` skipping index with granularity 1. It prunes
irrelevant granules for trace detail and selected detector context; it neither
pre-aggregates spans nor changes FINAL deduplication, receipt cutoffs, query
results, statistical rules, or bounds. The pinned 25.8.4.13 server has both
`use_skip_indexes_if_final` and `use_skip_indexes_if_final_exact_mode` enabled.
The latter retains overlapping replacement-key ranges, as described in the
[official 25.5 release notes](https://presentations.clickhouse.com/2025-release-25.5/).

The [isolated retained-corpus comparison](../evidence/query-hardening/trace-index-trials3/results.json)
kept resource limits unchanged and disabled the query-condition cache for both
arms to isolate physical index work. All outputs matched. Context reads fell
from 1,065,342 to 220,674 rows and took 84–103 ms versus 275–308 ms. Detail reads
fell from 997,291 to 98,059 rows, but elapsed time and CPU did not consistently
improve. The index occupied approximately 980 KB compressed for this retained
corpus. An earlier cache-enabled trial is retained separately; warmed condition
cache entries confound its repeated read-cost comparison. These measurements
justify a controlled candidate, not a claim of improved mixed-workload capacity.

The index adds hashing, insert/merge work and disk reads. The full workload must
measure those costs with the same producer, worker and API images as query
revision 3, unchanged quotas, and every existing gate. The candidate overlay
mounts the optional migration; the frozen baseline and ordinary direct setup do
not receive it. Fresh parts build the index on insertion. For retained data,
`ALTER TABLE incidentlens.spans MATERIALIZE INDEX trace_id_bloom SETTINGS mutations_sync=1`
is an explicit, potentially expensive migration step; ordinary startup does not
rebuild every old part. Rerun the migration job explicitly when adding the overlay
to an existing deployment, then materialize once if full coverage is required.
Unindexed parts remain correct and are scanned normally. Removing the overlay
alone does not remove an index from retained volumes; rollback requires
`ALTER TABLE incidentlens.spans DROP INDEX IF EXISTS trace_id_bloom` after removing
the optional migration. This restores the previous read plan without deleting
spans. No table, technology, cache policy or Kafka configuration is added.

Rejected follow-ups are retained under `docs/evidence/query-hardening/`:
early kind filtering showed no consistent benefit; UNION selection increased
peak memory without consistent timing gains; per-trace aggregate selection
used much more CPU and memory. A two-pass trace search adds a full scan and
snapshot-consistency concerns, so search semantics and SQL remain unchanged.
