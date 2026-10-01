# Phase 5 verification

The user explicitly requested Kafka as a learning exercise despite Phase 4's
measured deferral. [ADR 0004](adr/0004-phase5-buffering-and-replay.md) preserves
that distinction. The opt-in learning extension is implemented and verified as
of October 1, 2026 UTC. Real delivery/replay checks and both storage outages passed
finite identity reconciliation; failed performance targets and unsuccessful
attempts remain published below. The direct path remains the MVP recommendation.

## Scope and reproduction

The optional path is Collector → stream-ingest → Kafka → stream-worker →
ClickHouse. The direct path remains the default. Producer success acknowledges
broker admission; API/trace visibility still requires ClickHouse storage.
[The runbook](../deploy/streaming/README.md) specifies cutover, drain, retention
and rollback. [The predeclared workload](../benchmarks/WORKLOAD_PHASE5.md) fixes
the comparison and extra 60-second storage outage before measurement.

```sh
(cd backend && INCIDENTLENS_STREAMING_TESTS=1 go test -race ./tests/streaming -count=1 -timeout=10m -v)
(cd backend && go test -race ./... && go vet ./...)
(cd backend && go build -o /tmp/incidentlens-bench ./cmd/bench)
python3 -m unittest discover -s benchmarks -p 'test_*.py'
python3 benchmarks/run.py --stack direct --plan benchmarks/plan-phase5.json --binary /tmp/incidentlens-bench --output benchmarks/results/<new-direct-run>
python3 benchmarks/run.py --stack streaming --plan benchmarks/plan-phase5.json --binary /tmp/incidentlens-bench --output benchmarks/results/<new-streaming-run>
python3 benchmarks/run.py --stack streaming --plan benchmarks/plan-phase5-outage15.json --binary /tmp/incidentlens-bench --output benchmarks/results/<new-outage15-run>
python3 benchmarks/run.py --stack streaming --plan benchmarks/plan-phase5-outage.json --binary /tmp/incidentlens-bench --output benchmarks/results/<new-outage-run>
```

Use a different output directory for every attempt. Fault suites and benchmarks
must run serially; faults stop local dependencies. Runtime evidence uses the
local Go 1.26.8 toolchain at `/tmp/incidentlens-toolchain/go/bin/go`.

## Retained unsuccessful attempts

- [First streaming fault run](evidence/phase5/streaming-first-run.txt): paused-worker
  admission/recovery and write-before-commit replay passed. Broker recovery did
  not produce visibility within the original 30-second observation deadline;
  another fixture's unscoped FINAL query hit ClickHouse's memory limit. The worker
  now retries transient fetch failures, and fixture queries filter the fixture
  service and use two query threads. Recovery readiness is checked explicitly.
  These are test/implementation corrections, not successful performance results.
- [Initial poison/gap checks](evidence/phase5/poison-first-run.txt): malformed and
  unsupported records left offset zero unchanged; truncating a dedicated fixture
  topic below a committed offset returned OffsetOutOfRange without storage or reset.
- [Second streaming attempt](evidence/phase5/streaming-second-run.txt) could not
  connect to ClickHouse after the host resumed. [Container state](evidence/phase5/clickhouse-resume-failure.json)
  showed timed-out health-check starts despite stale healthy status. A normal
  [dependency restart](evidence/phase5/dependency-restart.txt) failed to receive a
  container exit event. Docker Desktop recovery retained data volumes; this is
  an environment failure, not evidence of stream delivery success.

- [Final fault campaign](evidence/phase5/streaming-final-run.txt) passed poison,
  retained-offset gap, paused-worker recovery, broker outage and invalid/oversized
  request checks. Its new frozen detector comparison used a future evaluation end
  and was correctly rejected. The fixture now freezes the actual observation time;
  [the corrected replay run](evidence/phase5/replay-final-run.txt) passed, including
  explicit write-before-commit offset preservation and both same/fresh-group replay.
- [Direct regression](evidence/phase5/direct-regression.txt) passed with all five
  real-dependency/query/detector/restart/failure gates enabled after Docker recovery.
  The [Go unit/race suite](evidence/phase5/go-unit-final.txt) and
  [benchmark harness suite](evidence/phase5/harness-tests-final.txt) also passed.

- [Post-baseline full fault rerun](evidence/phase5/streaming-acceptance.txt) proved
  that broker-acknowledged input survives broker restart while the worker remains
  stopped. The later replay test's counting query hit ClickHouse's total memory
  limit against the enlarged retained corpus. Fixture queries were narrowed
  to their exact namespace; the failed run remains evidence. No product quota or
  benchmark limit is changed to hide this result.

- The intended 15-second outage in the final bounded-worker comparison
  [did not start](../benchmarks/results/20261001-streaming-bounded/outage-r1/run.json).
  Python 3.9 rejected the valid five-fractional-digit load timestamp
  `2026-10-01T02:51:42.40954Z`; the scheduler waited until the harness ended.
  This stage is not outage evidence, and the campaign remains marked failed.
  Its steady and ramp observations remain available. The corrected parser accepts
  zero through nine fractional digits and explicit timezone offsets. A declared
  fault that does not complete now stops the campaign; the analyzer marks it
  incomplete while preserving metrics. All [29 Python regressions](evidence/phase5/harness-tests-scheduler-final.txt)
  passed after measurement ended and before the supplemental fault campaigns.

## Delivery acceptance

The final [complete race-enabled streaming suite](evidence/phase5/streaming-bounded-acceptance.txt)
passed all tests in 152.732 seconds, including the final compressed-batch bounds.
The [earlier full suite](evidence/phase5/streaming-acceptance-scoped.txt) passed
before that bound correction. Fixture count queries use namespace/service
PREWHERE pruning; crash/replay uses its own finite topic so unrelated retained
benchmark records cannot delay the fixture. Docker Desktop was restarted with
existing volumes before the final suite after a second dependency stall; quotas
were unchanged. [The failed prerequisite check](evidence/phase5/consumer-memory-tests.txt)
and [container state](evidence/phase5/clickhouse-second-resume-failure.json) remain
retained. Neither environment stall establishes a Kafka delivery failure or an
observed container OOM; their underlying cause was not determined.

| Contract | Observed proof |
| --- | --- |
| Broker ACK precedes visibility and survives process restart | With worker stopped, a valid export received Kafka ACK and FINAL count stayed zero. Kafka restarted on the same volume; count remained zero until the worker started, then the exact payload and parent appeared. |
| Write before offset commit | Test worker exited 86 after insert. Unique group's committed offset remained at the record offset; same-group restart then advanced it. |
| Retained replay stays logically duplicate-safe | Same-group and fresh-group replay increased raw copies while FINAL, service count and the frozen detector result remained unchanged. |
| Broker outage | Export returned bounded retryable failure; restart and caller replay recovered the identity. |
| Invalid and oversized data | Invalid/mixed OTLP and whole-request size rejection passed; unit preflight proves an oversized later encoded chunk publishes no earlier chunk. |
| Poison/schema failure | Real malformed and unsupported records caused failure without a storage write or offset advance. |
| Retention gap | Dedicated topic truncation below committed offset produced OffsetOutOfRange, no write and no offset reset. |
| Compressed batch bound | A compressed 6 MiB decoded batch was accepted by the broker, then rejected by the worker before storage; committed offset stayed zero. |
| Bounded fetch progress | Four separate valid compressed records containing 512 normalized rows progressed one fetch at a time, preserved every row and committed offsets 1 through 4. |

Fixture storage recovery uses a 60-second bound; crash/replay allows 90 seconds
for consumer-group session expiry. Both are stricter than the predeclared
120-second maximum. Offset/trace identities are logged. Single-broker retained
volume process restart is the tested failure model; power loss, disk loss and HA
remain outside the guarantee. An observed long-idle membership error also led to
a tested fetch-rejoin fix; commit ownership loss still stops for safe replay.

## Fresh direct comparison

The [fresh direct campaign](../benchmarks/results/20260930T1415Z-direct/campaign.json)
completed mixed, three steady repetitions, the 15-second outage and ramps through
8,000 spans/s. Every emitted and acknowledged identity was stored. The 8,000 ramp
had 138,432 planned spans never emitted by the bounded driver and triggered the
predeclared saturation stop; 32,000 and 100,000 were skipped. The independently
[recomputed analysis](../benchmarks/results/20260930T1415Z-direct/analysis/analysis.json)
retains one mixed trace-detail HTTP 503 and all latency/resource measurements.

The direct campaign ran 14:14:49–14:33:42 UTC on September 30. It began with
815,439 retained physical rows. Host: Intel MacBookPro16,1/i9-9880H, 8 physical/
16 logical CPUs and 64 GiB RAM. Docker Desktop 29.8.0 exposed 16 CPUs and
8,320,958,464 bytes of VM RAM. Quotas were ClickHouse 2 CPUs/2 GiB, API and direct
ingest 1 CPU/512 MiB each, Collector 1 CPU/256 MiB. Kafka and both streaming
processes were stopped during this campaign. Existing data and normal merges
were retained. `caffeinate -i` prevented idle sleep after the mixed stage.

| Direct stage | ACK spans/s inside measured window | Storage ACK p95 / p99 ms | Planned spans | Missing emitted / ACKed |
| --- | ---: | ---: | ---: | ---: |
| Mixed | 498.45 | 161.219 / 213.649 | 65,766 | 0 / 0 |
| Steady 1 | 498.60 | 41.069 / 63.505 | 75,000 | 0 / 0 |
| Steady 2 | 498.60 | 51.796 / 75.546 | 75,000 | 0 / 0 |
| Steady 3 | 498.60 | 28.920 / 34.167 | 75,000 | 0 / 0 |
| Outage 15 s | 497.00 | 18,474.566 / 20,372.472 | 37,500 | 0 / 0 |
| Ramp 500 | 495.73 | 29.379 / 30.688 | 24,996 | 0 / 0 |
| Ramp 2,000 | 1,996.00 | 38.382 / 408.334 | 99,996 | 0 / 0 |
| Ramp 8,000 | 4,757.33 | 898.862 / 1,101.133 | 399,996 | 0 / 0 |

Whole-trace rounding and batch boundaries explain rates slightly below the offered
rate. All steady runs reconciled every planned identity. The mixed run returned
479 HTTP 200s and one trace-detail 503; its cause remains unknown. Successful
query latencies exclude that failure. The direct outage needed 27 retry attempts;
visibility had two censored samples, so its observed successful-sample percentiles
do not establish an unconditional visibility target. Steady visibility p99s were
268.563, 318.428 and 233.431 ms, each from 32 measured samples with 200 ms polling.

## Retained preliminary streaming trial

The [preliminary streaming trial](../benchmarks/results/20260930-streaming/campaign.json)
completed mixed and steady-1, then was deliberately interrupted during steady-2
for a consumer memory-bound correction. Review of pinned franz-go showed a
1 GiB default decoded-batch cap, larger than the worker's 512 MiB container, and
compressed fetch size alone did not bound total decoded payload. This was a code
review finding, not an observed worker OOM. The final worker polls one batch
without prefetch, caps decompression at 5 MiB and response bytes at 8 MiB, and
fails closed on oversized decompression. The [workload amendment](../benchmarks/WORKLOAD_PHASE5.md)
records this correction before the final comparison. No initial result was deleted
or silently replaced; final performance claims require the fresh final campaign.

## Comparison limits and operational costs

These campaigns share the host, workload and original product quotas, but they
are sequential observations, not a randomized causal comparison. The retained
ClickHouse corpus grows between runs; normal merges continue. Docker Desktop
recovery and dependency restarts change caches. The direct campaign precedes the
streaming-only decompression correction; environment manifests preserve distinct
source hashes and image IDs. The generator and direct ingestion behavior did not
change for that correction. Repeating three steady runs exposes some variation
without making the high-percentile estimates robust: each steady run samples only
32 visibility observations, with 200 ms polling, and each mixed query route has
about 120 measured requests.

Replacing the direct ingest process with the producer, worker and Kafka adds
three CPU cores and 1.5 GiB of configured container memory in total. Kafka also
requires a retained volume, a 512 MiB JVM heap, topic/retention administration and
consumer-group recovery. Its Java broker health check runs every five seconds
and remains part of measured operational cost; Java offset/topic inspection runs
outside the measured load windows. One broker and one replica provide no HA,
host/disk-loss guarantee or cross-system exactly-once transaction.

Readiness establishes dependency connectivity, not offset progress. All storage
write errors retain the offset and retry until cancellation, including persistent
schema/permission failures; there are no per-attempt retry diagnostics or automatic
stuck-consumer alerts. Operators must inspect lag and reconcile finite identities.
Retention is finite and deletion asynchronous; a gap halts visibly, and an
independent retained source is necessary to recover data already removed by Kafka.

## Final streaming comparison

The [bounded-worker campaign](../benchmarks/results/20261001-streaming-bounded/campaign.json)
retains its failed overall status because the intended outage did not start.
It ran 02:38:09–02:59:27 UTC on October 1, starting with 1,705,598 retained
physical rows and 40,002,341 active-part bytes, compared with the smaller direct
starting corpus reported above. Both paths used the same host and Docker VM.
Mixed, three steady repetitions, and ramps at 500 and 2,000 spans/s reconciled
every planned identity at the original deadline. The 8,000 ramp acknowledged
395,580 emitted spans out of 399,996 planned; 4,416 were never emitted by the
driver. At its original observation deadline, 35,260 acknowledged spans remained
missing. The predeclared rule stopped escalation, so 32,000 and 100,000 were skipped.

The [comparison tables](evidence/phase5/comparison-tables.md) publish ACK and
visibility percentiles, route latency/errors, three-run variation, measured
CPU/memory, Kafka disk observations and consumer positions. Streaming steady
visibility p99s were 330.693, 385.474 and 331.583 ms; those small samples do not
show a visibility improvement over the direct steady runs. At 2,000 spans/s,
streaming visibility p95/p99 rose to 4.606/5.112 seconds, failing the 1/3-second
targets even though eventual identity reconciliation passed. At 8,000, all 187
sampled probes were censored and no successful visibility quantile exists.

During mixed plus steady windows, Kafka averaged 55.73% of one CPU core across
215 samples and reached 587.30 MiB observed memory; the producer and worker
added 5.56% and 6.66% of one core on the same sample convention. Kafka's sampled
volume grew from 4.63 to 83.60 MiB across the comparison. These measured costs
supplement the configured quotas above; maxima and sample means are not guaranteed
peaks or time-weighted averages. ClickHouse reached its 2 GiB reported memory cap
on both paths. Retained corpus, caches and background merges limit causal claims
about the faster successful mixed-query observations on the streaming run.

The separate [supplemental drain](../benchmarks/results/20261001-streaming-bounded/ramp-8000-r1/supplemental-drain.json)
started at 02:59:17 UTC and completed its first snapshot by 02:59:19, finding all
395,580 emitted/acknowledged spans and no unknown or duplicate logical identities.
This observation meets the supplemental 120-second bound but does not erase the
original deadline failure. The 7,903.2 spans/s measured-window broker ACK rate
is admission throughput, not demonstrated sustained storage throughput.

The mixed run recorded five trace-detail 404s and one service-summary 503 among
480 measured requests. A detail lookup can race storage after a broker ACK;
these observations remain failed query targets, and the exact cause of the 503
was not established. Successful-request percentiles exclude those failures.

## Supplemental storage outages

The [corrected 15-second campaign](../benchmarks/results/20261001-streaming-outage15/campaign.json)
completed the fault and reconciled all 37,500 planned identities. The stop command
started 30.004 seconds after load start; ClickHouse remained stopped for 15.005
seconds after that command completed. API/producer/worker readiness was restored
by 03:02:43.700 UTC, about 20.894 seconds after stop initiation. Broker ACK p95/p99
were 32.788/45.757 ms with no export retries, compared with direct storage ACK
18,474.566/20,372.472 ms and 27 retries. Both eventually stored every emitted span.

This separation does not preserve query availability: six of 16 sampled streaming
visibility probes were censored at their 10-second bound; the ten observed samples
had first-send-to-visibility p95/p99 of 8,531.328 ms. Visibility targets failed.
The direct outage also had censored probes. None of these successful-sample
percentiles estimates unconditional outage visibility.

The [60-second campaign](../benchmarks/results/20261001-streaming-outage60/campaign.json)
also completed the fault and reconciled all 67,500 planned spans, with zero
emitted/acknowledged missing identities at the original settling snapshot. No
supplemental drain was needed. The stop command began 30.006 seconds after load
start, and the measured stopped interval was 60.002 seconds. Dependency readiness
was restored by 03:05:46.976 UTC. Reconciliation finished by 03:07:01.295, about
74.319 seconds after that readiness observation and within the declared 120-second
recovery observation bound. This is a bound from observations, not the exact time
the last span arrived.

Broker ACK p95/p99 were 35.274/49.568 ms with no export retries. Nineteen of 32
visibility probes were censored; the 13 observed samples had p50 260.271 ms and
p95/p99 9,959.659 ms. The visibility targets failed, while finite eventual delivery
passed. This proves the tested retained local buffering behavior, not storage
availability during the outage or protection from broker/disk loss.

## Cutover, rollback and final checks

[Observed cutover](evidence/phase5/stream-cutover-verification.json) verified the
streaming Collector mount, direct ingest stopped, and API/producer/worker ready.
The [observed rollback](evidence/phase5/rollback-verification.json) completed
03:08:21–03:08:48 UTC on October 1. New streaming input stopped first. Captured
topic end and committed group offset both reached 5908 with zero lag. Fresh FINAL
queries reconciled all ten final comparison/outage ledgers with no emitted or
acknowledged identities missing, no unknown identities and no logical duplicates;
the 4,416 never-emitted ramp spans remained explicitly accounted for.

Kafka and both streaming processes then stopped, preserving their named volume
and ClickHouse's named volume. The Collector mounted the original direct config;
direct ingestion and API readiness returned 200. A new 120-span direct fixture
reconciled fully. Previously streamed trace `c4c56e3c337846bb0027de25b7a2d756`
still returned all six spans through the query API. The local stack is left on
the direct path. The runbook's drain/rollback steps are therefore exercised, not
only proposed.

The final [Go race suite](evidence/phase5/go-race-complete.txt) and
[Go vet](evidence/phase5/go-vet-complete.txt) passed. These final commands use
normal unit-test mode; gated integration cases are not claimed as rerun there.
The separate real streaming suite and fully gated direct regression above provide
that evidence. All 29 Python harness/analyzer tests passed. No frontend behavior
changed in Phase 5. Source formatting and source/document whitespace checks
passed; retained raw command logs preserve Docker's trailing whitespace.

The [final secret-pattern scan](evidence/phase5/secrets-final.json) inspected 558
tracked/nonignored files and found no matching credentials or private keys.
Source/configuration and commit scope were also reviewed. The documented local
development passwords remain fixtures; this check is not a general guarantee
that arbitrary telemetry is secret-free. The unrelated user workspace file is
excluded from both the scan and commit.

## Milestone audit and decision

| Required outcome | Evidence and disposition |
| --- | --- |
| Justify an added broker against simpler alternatives | ADR 0004 compares direct ingestion, Collector persistence and Kafka. Explicit user learning authorization activates this extension; Phase 4 did not establish operational necessity. |
| Define producer/consumer contracts before implementation | ADR, ingestion contract and predeclared workload specify broker ACK, ordering, bounded records/fetches, retention, retries, at-least-once writes and duplicate-safe reads. |
| Verify crash/restart, broker outage and replay without inflated queries/detection | Final race-enabled real-dependency suite passes retained broker restart, write-before-commit crash, same/fresh-group replay, frozen detector equality, poison/schema/retention-gap failures and compressed payload limits. |
| Repeat comparable workloads and report benefits/costs | Fresh direct and final streaming measurements, three steady repetitions, mixed query distributions/errors, saturation/backlog accounting and sampled resources are published. The unstarted fault is invalidated and separately rerun; no failed target is hidden. |
| Verify additional 60-second storage outage | All 67,500 planned identities stored within the stated recovery observation bound; 19 censored visibility probes and failed latency targets retained. |
| Exercise migration and rollback without deleting data | Cutover and rollback artifacts verify routing, stopped input, committed end offset, fresh identity reconciliation, retained volumes, restored direct ingestion and old trace visibility. |

Kafka provides a retained replay boundary and decouples producer acknowledgment
from storage outages in this local exercise. It adds CPU, memory, disk/retention
operations and failure states; it does not remove ClickHouse's throughput or
visibility limits. The measured steady visibility tails did not improve, and
the higher-rate backlog illustrates the distinction. Keep direct ingestion as
the default MVP. Collector disk persistence remains an unimplemented alternative,
and no production durability, HA or universal zero-loss claim follows.
