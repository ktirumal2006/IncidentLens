# Phase 5 learning experiment: predeclared validation

Declared before Phase 5 runtime measurements, 2026-09-29. These are planned
experiments and targets, not results. [ADR 0004](../docs/adr/0004-phase5-buffering-and-replay.md)
records the user's explicit Kafka learning objective despite Phase 4's deferral.

## Controlled comparison

Repeat the [Phase 4 workload](WORKLOAD.md) with its existing payload, rates,
warmups, measurement durations, three steady repetitions, query mix, 15-second
storage outage, resource safety stops and finite identity reconciliation. Preserve
Phase 4's original raw evidence. Run a fresh direct-path campaign and then the
streaming campaign on the same host and Docker VM; record growing corpus and
background changes. Keep original product quotas unchanged and record Kafka and
stream producer/worker quotas separately. Idle unused services must be disclosed.
Do not run Demo load or unrelated tests during either campaign.

Streaming success acknowledges broker admission; direct success acknowledges
ClickHouse storage. Report both producer ACK and first-send-to-visibility latency,
not an unlabeled comparison of incompatible acknowledgment meanings. Final
identity reconciliation must compare planned, emitted, producer-acknowledged and
stored spans. If broker backlog remains after the original settling interval,
retain that snapshot as incomplete delivery at that deadline; record additional
bounded drain observations separately rather than rewriting the original result.
Before measurement: stop streaming ramp escalation if any acknowledged identities
remain missing at the original observation deadline, even if supplemental drain
later succeeds. This is evidence of storage backlog rather than a sustained
throughput result; admission ACK alone must not drive further escalation.
No record missing at an observation deadline is automatically called permanently
lost. Retention expiration and never-emitted driver drops are separate outcomes.

All original 500 spans/s targets remain reported, including zero acknowledged and
planned identity loss at the declared observation deadline. For streaming, also
report the gap between acknowledged and visible spans, consumer lag/end offsets,
Kafka volume usage and added container CPU/memory. The ramp may establish a
producer-admission limit different from sustained storage throughput. Stop on the
same safety boundaries or observed backpressure; retained log growth is not proof
of sustainable throughput. Keep the two-million-span and 256 MiB raw-output caps.

## Finite delivery/failure checks

Use unique recorded fixture identities and isolated topic/group names where
necessary; never overwrite previous evidence. Run serially with explicit cleanup
that restores dependencies while preserving volumes. Do not activate failure
checks concurrently with benchmark campaigns.

- Publish validated traces with the worker stopped. Broker success is allowed
  while ClickHouse lacks the records. Start the worker and require every
  acknowledged identity within 120 seconds, with correct payload and parents.
- Kill the worker after a successful ClickHouse write but before offset commit.
  Restart against retained offsets; require replay and exactly one logical span
  per immutable identity in FINAL queries and detector input.
- Stop the broker and attempt a finite export. Require a bounded retryable
  failure, never false acknowledgment. Restart with retained volume, replay the
  ambiguous input, and reconcile all previously acknowledged identities.
- Replay retained records with a fresh group. Verify duplicate-safe query counts
  and frozen detector outputs, not only physical row counts.
- Inject invalid/unsupported record schema and test size limits. Require explicit
  rejection or fail-closed consumer behavior with no offset advance past the
  offending record. Check known older committed-offset/retention gaps explicitly.
- Additional long-outage scenario: 500 spans/s, 15-second warmup and 120-second
  measurement; stop ClickHouse 30 seconds after load begins for 60 seconds.
  Keep caller retries/batches unchanged. Require zero missing broker-acknowledged
  identities after recovery within a separately recorded 120-second drain bound.
  Report all errors, producer ACK, visibility censoring, pending backlog and
  actual readiness/recovery times. The target is not an HA or disk-loss claim.

## Bounds and acceptance

The learning stack uses one Kafka partition, RF1, one-hour/1GiB log retention,
4 MiB encoded records and at most 256 normalized spans per record. Record topic
settings and image/client versions before runs. Retention deletion is asynchronous;
monitor real volume/free disk rather than treating retention bytes as a hard quota.
Stop or reject the streaming run if Kafka filesystem free space is below 1 GiB,
its actual volume exceeds 2 GiB, or those observations fail. These limits include
asynchronous retention/segment overhead; the original host 10 GiB limit remains.
Tests must finish well within retention; any expiry invalidates a zero-loss claim
for that data and must be reported. Do not silently reset offsets after a gap.

Accept Phase 5 only after the concrete producer/consumer/replay contracts, real
failure checks, comparable measured workloads, operational costs and migration/
rollback steps are implemented and verified. Failed performance targets remain
results. The conclusion must distinguish the educational benefit from an evidence-
based recommendation for the default MVP architecture.


## Consumer bound correction before the final streaming comparison

The first streaming trial was deliberately interrupted during its second steady
repetition after code review found the client default decoded-batch limit was
1 GiB, above the worker's 512 MiB container cap. That trial and its source hashes
remain retained; its completed stages are not substituted for a final campaign.
No worker OOM is inferred from this review finding.

The final worker polls one batch at a time without prefetch, limits decoded
batches to 5 MiB and broker responses to 8 MiB, and fails closed on oversized
decompression. Kafka's first-batch exception permits progress with one-byte
requested fetch budgets. Real compressed-record checks precede the fresh final
streaming campaign. Direct workload/results and all resource quotas stay fixed.
The added fetch round trips are part of the operational cost to be measured.

## Outage scheduler correction and supplemental 15-second run

During the final comparison on October 1, the intended 15-second outage did not
start: Python 3.9 rejected a valid Go timestamp with five fractional digits and
the scheduler kept waiting for load start. The run records `outage_failed` with
`not_started`; it is not outage evidence. Its unchanged steady and ramp stages
remain observations. The corrected scheduler accepts zero through nine fractional
digits and now fails the campaign when a declared fault does not complete.

`plan-phase5-outage15.json` copies the original 15-second outage stage unchanged
into a supplemental campaign after the comparison. This repeats the missing
fault, not its targets or limits. The additional 60-second plan is unchanged.
Both run after timestamp regressions pass; all original artifacts remain retained.
