# ADR 0004: Phase 5 buffering and replay entry decision

Status: accepted for implementation, 2026-09-29. The user explicitly selected
“Build Kafka as an explicit learning exercise despite the Phase 4 deferral.”
Implemented and verified October 1, 2026 UTC; the
[verification report](../VERIFICATION_PHASE5.md) retains failed targets,
unsuccessful attempts and measured operational costs.

## Context

[Phase 5](../MILESTONES.md) is conditional on a specific limitation that the
simpler architecture cannot reasonably address. The request to start the phase
opens that assessment; it does not turn a measured storage/driver limit into
evidence for a broker.

The [Phase 4 campaign](../VERIFICATION_PHASE4.md) found:

- Three 500 spans/s steady repetitions each recovered all 75,000 planned spans.
- The 15-second storage outage recovered all 37,500 planned spans with existing
  Collector behavior and caller retries. Sixteen batches needed 28 retry attempts.
- The highest tested unsaturated ingestion stage was 2,000 spans/s for 45 measured
  seconds. At 8,000 offered spans/s, bounded driver queues dropped planned input;
  all emitted and acknowledged spans were stored. ClickHouse CPU pressure was
  observed, but no causal profile established an exact storage ceiling.
- Two mixed-query HTTP 503s remain unexplained. A broker has no demonstrated
  relationship to fixing those read-path failures.

The existing contract acknowledges successful valid exports after synchronous
ClickHouse storage. [ADR 0002](0002-collector-acknowledgment.md) deliberately
removed early volatile acknowledgment. Existing integration tests distinguish
unacknowledged Collector crash loss from acknowledged stored data; explicit
caller replay recovers the former. Losing volatile, unacknowledged input is a
known limitation, not evidence of a broken acknowledged-data guarantee.

## Explicit learning objective

The user selected Kafka as an educational extension despite the measured Phase 4
deferral. This overrides the milestone's evidence-only activation gate for this
phase; it does not revise the Phase 4 measurements or claim a production need.
The exercise will demonstrate broker acceptance before storage visibility,
consumer progress after storage, retained replay, failure recovery and operational
costs. It remains local, trace-only, opt-in and separate from the direct default.

A successful finite run must preserve every Kafka-acknowledged valid span through
a consumer crash or broker process restart with retained volumes, within declared
retention bounds. A 60-second storage outage at 500 spans/s will measure buffering
and eventual recovery. Zero acknowledged loss is the target, not an assumption.
The producer can acknowledge broker acceptance during the outage; the UI remains
an observation of ClickHouse data and cannot treat that ACK as query visibility.

## Alternatives

| Option | Fit and current evidence | Costs and unresolved behavior |
| --- | --- | --- |
| Keep direct ingestion with bounded Collector and caller retries | Already meets the tested 15-second outage and zero acknowledged-loss contract | Producer must retain ambiguous/failed data for replay; no independent retained stream |
| Evaluate Collector disk-backed queue | Candidate for restart-surviving pending delivery without another service | Local disk lifecycle, capacity, retry expiry, corruption and acknowledgment semantics need real tests; not a historical replay log |
| Introduce a streaming broker | Candidate when retained replay or independent consumers are actually required | Adds producer/consumer protocol, offsets, retention, disk/resource budgets, migration, recovery and operational failure modes |

The pinned Collector exporter helper supports a storage-backed sending queue and
resuming pending exports after restart. It still has finite retry behavior unless
configured otherwise. Persistence alone is not a proof of end-to-end delivery.
See the [v0.137.0 exporter helper documentation](https://github.com/open-telemetry/opentelemetry-collector/blob/v0.137.0/exporter/exporterhelper/README.md).

The pinned file-storage extension exposes per-write fsync and compaction options.
Its documented corruption-recreation behavior can start a fresh database, so
recreation must not be represented as lossless recovery. A candidate must declare
disk bounds and corruption handling before testing. See the
[v0.137.0 file-storage documentation](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/v0.137.0/extension/storage/filestorage/README.md).

Kafka separates retained partition records from consumer positions, enabling
rereading retained data. That introduces a second acknowledgment boundary before
ClickHouse visibility if producers acknowledge broker acceptance. External
ClickHouse writes and consumer offset commits need explicit duplicate-safe crash
handling; Kafka features alone do not make that pair exactly once. See the
[Kafka design documentation](https://kafka.apache.org/41/design/design/).
The accepted design below selects the local Kafka runtime and Go client.
Collector persistence remains an unimplemented alternative, not a verified claim.

## Constraints on any accepted design

- Keep traces as the only ingested signal and keep the external Demo separate.
- Preserve immutable span IDs, event times and payloads across retries/replay.
- State acknowledgment and query visibility separately. Any change to the
  existing success contract must be explicit in this ADR and the ingestion API.
- Validate before any new durable acceptance boundary; define permanent-invalid
  data behavior without falsely acknowledging successful storage.
- Bound queue/log bytes, record size, concurrency, retention and retries. Show
  explicit rejection/backpressure at each full boundary.
- Commit consumer progress only under the declared storage-success condition;
  test crashes between write and commit, with FINAL query/detector deduplication.
- Preserve data volumes during recovery and document migration/rollback before
  replacing the existing path. A local single-node experiment is not HA.

## Verification sequence for the selected learning objective

1. Predeclare finite fixtures, zero acknowledged-loss targets, planned/emitted
   accounting, fault timing, disk/resource limits and recovery deadlines.
2. Verify Kafka admission before storage, consumer write-before-commit crash,
   retained broker restart, fresh-group replay and poison/retention-gap failures
   against real dependencies. Retain unsuccessful attempts and identity evidence.
3. Repeat the Phase 4 workload on the direct and streaming paths with unchanged
   comparable product/hardware limits. Report visibility alongside producer ACK,
   growing corpus, failed targets, safety stops and added operational costs.
4. Run the additional 60-second storage outage and bounded recovery observation.
5. Verify cutover, drain and rollback with retained volumes, then audit milestone
   acceptance against the actual evidence.

The user selected retained Kafka replay as a learning exercise. This sequence does
not claim that a Collector persistence implementation was tested or that Kafka
is required by the default MVP workload.

## Accepted design and consequences

Add an opt-in `deploy/streaming` Compose overlay and two separate Go executables:
`stream-ingest` and `stream-worker`. The existing direct ingestion executable and
configuration remain available for comparison and rollback. The streaming
Collector forwards to stream-ingest; the API continues reading ClickHouse.

`Collector → stream-ingest → Kafka → stream-worker → ClickHouse → query API`

Producer contract:

- Apply the existing bounded normalization/privacy policy before publishing.
  Preflight every record's encoding and size before the first publish in an
  export. Reject permanent invalid/oversized data explicitly; mixed-validity
  exports return partial success only after all valid chunks receive broker ACK.
- Record schema version 1 contains normalized rows with immutable IDs, event
  times, payload and producer-assigned ingestion timestamp. At most 256 rows and
  4 MiB encoded per record. Larger exports use ordered chunks; a partially
  published export failure is ambiguous and callers may replay identical spans.
- Use the pure-Go franz-go client, pinned and tested, with idempotent production,
  all-ISR acknowledgment, bounded buffers and a finite publish deadline. Success
  means Kafka accepted the record, not ClickHouse stored it. RF1 has one ISR;
  no replication, disk-loss, host-power-loss or exactly-once claim follows.
- Producer readiness depends on Kafka/topic availability, independently of
  ClickHouse readiness. Broker failure or exhausted producer capacity returns
  retryable failure without false success.

Consumer and retained-log contract:

- One explicitly created topic `incidentlens-spans-v1`, one partition, RF1 and
  auto-creation disabled. This teaches ordered offset processing with a bounded
  local deployment; it is not a multi-partition scalability claim. Concurrent
  producers establish broker order, not universal event-time order.
- Retain at most one hour or 1 GiB per partition, whichever expires first.
  Segments roll at 64 MiB or five minutes; segment deletion is asynchronous; the configured retention size is not a hard
  filesystem quota. Bound container memory/CPU and stop tests on disk safety
  limits. Consumer lag must remain within retention for the delivery target.
- Fetch/process one broker batch at a time, without prefetch. The one-byte
  requested fetch budgets rely on Kafka's documented first-batch progress rule;
  they do not limit valid records to one byte. Cap decoded batches at 5 MiB and
  broker responses at 8 MiB. The producer's batch cap is 5 MiB uncompressed, with
  individual envelope records still capped at 4 MiB. This prevents a small
  compressed response from expanding into many buffered batches or the client's
  default 1 GiB decoded batch. An oversized decoded batch halts without a commit.
  See the [Kafka 4.1 fetch limits](https://kafka.apache.org/41/generated/consumer_config.html)
  and the pinned franz-go `MaxDecompressBatchBytes`/`MaxConcurrentFetches` contract.
  Disable automatic offset commits.
  Write a full record synchronously to ClickHouse, then commit its next offset.
  Retry storage writes with bounded backoff until cancellation; all write errors,
  including persistent schema/permission failures, retain the offset. Retry only
  recoverable Kafka commit errors; ownership loss stops for safe replay. Do not
  advance past failure. Keep only bounded in-flight work.
- A crash after a successful write but before offset commit replays that record.
  Immutable identities and existing FINAL queries/detector semantics prevent
  duplicate inflation. This is at-least-once processing, not a cross-system
  transaction. Test the ambiguous boundary explicitly.
- Unsupported schema, malformed payload or spans expired beyond ClickHouse's
  visibility retention halt processing visibly without committing the record.
  No silent skip or speculative dead-letter service is introduced.
- Replay uses a separate explicitly named consumer group starting at earliest
  retained offset. It rereads retained records into duplicate-safe storage; it
  cannot reconstruct records already removed by retention. Missing expected
  committed offsets must fail visibly rather than silently reset and claim
  delivery continuity.

Kafka is a pinned official image using single-node KRaft, with 2 CPUs/1 GiB memory
and a 512 MiB JVM heap. Stream producer and worker each have 1 CPU/512 MiB. Existing
ClickHouse/API/Collector quotas remain unchanged. The Go client is an added
implementation dependency because the existing OTLP/ClickHouse clients cannot
speak Kafka; implementing the Kafka protocol ourselves would add unnecessary risk.
The separate binaries keep the direct path's acknowledgment contract intact.

The added costs are a broker volume/JVM, another failure boundary, consumer lag,
retention operations and a worker process. Producer ACK latency must be reported
separately from end-to-end visibility. Passing this exercise will establish only
the tested local semantics, not retroactively justify Kafka for the MVP.

Worker readiness tests dependency connectivity, not insert success or offset
progress. A persistent ClickHouse insert failure can therefore leave readiness
green while the worker retries silently and lag grows. This local learning
implementation requires explicit lag and finite-identity checks; it does not
provide automatic stuck-consumer alerting or permanent storage-error diagnosis.

Before switching back to the direct path, stop new streaming input and drain the
consumer to the captured end offset. Verify FINAL identities before stopping the
worker. Preserve broker data and group offsets for investigation/replay; never
remove volumes as a rollback shortcut. Exact commands, observed rollback and
measured costs are published with the Phase 5 verification report and runbook.
