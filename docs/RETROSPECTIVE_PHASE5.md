# Phase 5 controlled retrospective

All 28 measured cases completed across the original attempt and its guarded continuation. Kafka improved admission during the tested bursts and downstream CPU slowdown, at the cost of backlog and delayed query visibility. It did not consistently improve sustained ClickHouse write throughput or query availability. The evidence supports an opt-in buffering/replay exercise, not replacing the direct MVP default. The [predeclared protocol](../benchmarks/WORKLOAD_RETROSPECTIVE.md) is the authority for workload and stop rules. [ADR 0004](adr/0004-phase5-buffering-and-replay.md) records why Kafka was implemented as an **explicit educational exercise** despite the Phase 4 decision to defer it for the default MVP. No Phase 6 work is included.

## What was known before this experiment

The original [Phase 4 campaign](VERIFICATION_PHASE4.md) ran the direct Collector → Go ingestion → ClickHouse path on the same physical host and Docker VM. Three 500 spans/s steady repetitions each reconciled 75,000 planned spans. A 15-second ClickHouse outage recovered all 37,500 planned spans with bounded Collector behavior and caller retries. At the 8,000 spans/s ramp, 399,996 spans were planned, 233,340 were emitted and all emitted spans were acknowledged and found in deduplicated storage; the other 166,656 were never emitted by the bounded driver. The 32,000 and 100,000 stages were skipped. Two mixed-query HTTP 503s failed the zero-error target.

Those observations established a finite recovery result and a high-rate overload/driver limit. They did **not** isolate the storage ceiling or prove that a broker would improve sustained throughput. ClickHouse CPU pressure, queue delay, background merges, VM scheduling and driver drops were not separated by a causal profile. The Phase 4 decision therefore deferred Kafka as an operational necessity. Phase 5 subsequently implemented an opt-in local broker path to learn broker admission, retained replay, offset progress and recovery semantics, while keeping direct ingestion the default. [Phase 5 verification](VERIFICATION_PHASE5.md) records that exercise and its limits; the retrospective below tests the architectural hypothesis rather than retroactively changing the reason for activation.

## Hypotheses and controlled protocol

Compare the original direct path with Collector → stream-ingest → Kafka → stream-worker → ClickHouse, using the same six-span/three-service trace fixture, 256-byte attribute payload, batching, bounded caller concurrency/queue, retries and four-route query mix. Each paired case gets a fresh namespace, new Compose project and empty retained ClickHouse volume; streaming also gets a fresh Kafka volume. The existing product/experiment volumes are preserved. Two repetitions reverse path order to reduce order bias. This differs deliberately from the original Phase 4 growing shared corpus, and limits generalization to established large databases or other hardware.

| Question | Predeclared evidence needed |
| --- | --- |
| Burst absorption | At 2,000, 8,000 and 32,000 offered spans/s for ten seconds, compare generated/emitted/broker-ACKed/stored identities, one-second progress, lag growth and bounded drain after input ends. |
| Admission/storage decoupling | Compare streaming broker publish/OTLP ACK with worker write and commit progress and ClickHouse `FINAL` identities. A broker ACK is not a storage or query-visibility ACK. |
| Backpressure isolation | Compare scheduling drops, failed exports, producer failures, storage write failures, query p95/error fraction and consumer lag at matched sustained rates of 500, 2,000 and 8,000 spans/s. |
| Downstream slowdown recovery | At 2,000 offered spans/s, reduce only the case ClickHouse CPU quota from two to 0.25 cores after 30 seconds, hold 30 seconds and restore two; verify actual quota changes and subsequent write/lag/identity recovery. |

Sustained and slowdown cases have ten seconds warmup and 120 seconds measurement; bursts have no warmup and ten seconds of offering. Every case includes 128 baseline setup traces and four HTTP requests per second during offering. The fault is controlled CPU pressure, not a storage outage. A fault that does not start or restore is invalid evidence. Repeated cases are retained separately rather than averaged into a single trace.

The original harness records scheduled, generated, enqueued, emitted, retried and caller-ACKed batches, exact timestamps, visibility probes and a finite trace/span identity ledger. New opt-in operation logs capture ClickHouse client prepare/append/send/total durations, Kafka ProduceSync duration and consumer commit, with integer nanoseconds, row counts and success status. Their 64 MiB file caps and terminal count/drop trailers must pass before operation metrics are used. Post-run ClickHouse query-log INSERT diagnostics are supplementary; if unavailable, server-side timings remain unknown. Direct OTLP ACK follows completed storage; streaming OTLP ACK follows broker admission. These distinct endpoints must retain separate labels in every comparison.

The streaming lag observer reads committed position then log end once per second. This is a sequential, non-atomic snapshot of **records**, not spans; its request times and errors are retained. Two zero-lag samples at the final stable end offset bound a drain observation, but zero lag alone cannot reconcile identities. Coverage checks flag gaps greater than three seconds through the original run end; any gaps during supplemental drain remain visible in the raw samples. The two zero snapshots bound recovery, not its exact instant. One-second buckets show generator, ACK, broker-publish, writer and consumer-commit progress. The analyzer field `stored_unique_final_spans` means the unique count using ClickHouse `FINAL` at the original post-settling snapshot, not the count after supplemental recovery. Original post-settling identity snapshots remain separate from any supplemental drain; a later recovery cannot turn an earlier unmet deadline into a pass. Never-emitted driver input, unresolved stored identities at a deadline, ambiguous retries and confirmed permanent loss are distinct categories. Missing at a deadline is not automatically permanent loss.

Safety retains the two-million-span and 256 MiB event caps, memory/restart/readiness stops, host 10 GiB free-space floor, ClickHouse 4 GiB growth stop, and Kafka 1 GiB free-space/2 GiB actual-volume bounds. Volumes remain after each case for inspection. The operation recorder and lag polling add measurement overhead; direct and streaming writers use the same operation recorder, while only streaming has broker/worker and offset polling. Client write duration includes protocol and connection work, so it is not pure ClickHouse server execution. Ten-second bursts have few API tail samples. The unchanged detail-query workload selects the most recently acknowledged trace (`backend/cmd/bench/http.go` and `main.go`). With Kafka, that trace may not yet be visible in ClickHouse, so a detail 404 can reflect admission-before-visibility rather than a query-service outage. Keep these responses in the error counts and report their status separately. The query events do not retain the selected trace ID or response body, so exact per-request attribution cannot be reconstructed from those events alone. Host/VM wall-clock checks are retained; cross-process timeline alignment has uncertainty even though local durations use monotonic clocks. Slowdown command initiation/completion and inspected quota bound when pressure took effect. The 30-second monotonic hold begins **after** quota verification and excludes transition-command overhead; the actual throttled interval must not be described as exactly 30 seconds.

## Recorded environment and configured cost

The first pair's retained environment records identify a MacBookPro16,1 with Intel Core i9-9880H, eight physical cores, sixteen logical CPUs and 68,719,476,736 bytes of host memory. Docker reports sixteen VM CPUs and 8,320,962,560 bytes of VM memory, Engine 29.8.0 and Docker Desktop 4.92.0. The host reports macOS 26.6.2; the driver uses Python 3.9.6. These are recorded runtime facts, not hardware reservations or measured consumption. Every case retains its own environment record; see the [final controls audit](evidence/retrospective/final-controls.json).

| Running component | CPU quota in cores | Memory limit in bytes |
| --- | ---: | ---: |
| ClickHouse, both paths | 2 | 2147483648 |
| Query API, both paths | 1 | 536870912 |
| Collector, both paths | 1 | 268435456 |
| Direct ingestion | 1 | 536870912 |
| Streaming producer | 1 | 536870912 |
| Streaming worker | 1 | 536870912 |
| Kafka | 2 | 1073741824 |

The normal direct limits sum to five CPU cores and 3,489,660,928 bytes; streaming limits sum to eight cores and 5,100,273,664 bytes. Thus the Kafka topology adds three cores of configured quota and 1,610,612,736 bytes of memory limit, plus its retained broker volume. These sums are configured bounds, not proof that either topology consumes them. ClickHouse's declared slowdown temporarily changes only its CPU quota. Both paths use ClickHouse 25.8.4.13 and Collector 0.137.0; streaming uses the pinned Kafka 4.1.1 image. Image IDs and source hashes remain authoritative in the per-case provenance.

## Reproduction and retained evidence

The original campaign stopped before the final slowdown pair when Docker reported that all predefined network address pools were fully subnetted. This was a pre-load infrastructure failure, not a measured workload failure. The original failed campaign record remains unchanged. Two completed smoke-test projects were taken down without volume removal; [the recovery record](evidence/retrospective/network-recovery.json) verifies that every preexisting volume remained. The continuation uses new projects and empty volumes, the same workload declaration and native binaries, and only the two previously unmeasured labels. Its first case has identical product image IDs, VM configuration and container quotas to the original streaming cases. The only source-manifest differences are the continuation/analysis Python files and their tests. The delay between attempts adds an environmental timing limitation; The final controls audit covers both continuation cases.


The original campaign root is [benchmarks/results/20261001-retrospective](../benchmarks/results/20261001-retrospective). Each measured `CASE-rN-PATH` directory retains environment/source/image/quota provenance, initial and final storage snapshots, raw harness events/summary/identity ledger, resource samples, operation logs, streaming lag samples, server INSERT diagnostics and an exact case analysis. The campaign record contains ordered commands and cleanup/restoration status. The independent analyzer emits integer nanoseconds/counts and reduced rational rates/error fractions; do not round derived metrics in the findings.

From the repository root, after building the benchmark images for both opt-in case overlays and confirming the stated preflight, the reproducible driver invocation is:

```sh
(cd backend && go build -o /tmp/incidentlens-bench-retro ./cmd/bench)
(cd backend && go build -o /tmp/incidentlens-bench-lag ./cmd/bench-lag)
python3 -m unittest discover -s benchmarks -p 'test_*.py'
RETRO_PROJECT=retro-build BENCH_EVIDENCE_DIR=/tmp/incidentlens-retro-build \
  docker compose -f deploy/local/compose.yaml -f deploy/benchmark/direct.yaml build ingest api
RETRO_PROJECT=retro-build BENCH_EVIDENCE_DIR=/tmp/incidentlens-retro-build \
  docker compose -f deploy/local/compose.yaml -f deploy/streaming/compose.yaml \
  -f deploy/benchmark/streaming.yaml build stream-ingest stream-worker api
python3 benchmarks/retrospective.py \
  --binary /tmp/incidentlens-bench-retro \
  --lag-binary /tmp/incidentlens-bench-lag \
  --output benchmarks/results/<new-retrospective-run>
python3 benchmarks/analyze_retrospective.py \
  benchmarks/results/<new-retrospective-run> \
  --output benchmarks/results/<new-retrospective-run>/analysis
```

The output directory must be new. The driver uses `--no-build` case startup, so the pinned direct and streaming benchmark images must be built before this invocation; the campaign's recorded commands and source/image digests identify the actual execution. It stops the original local stack while experiment ports are in use, stops each isolated case without deleting volumes, and restores direct ingestion at the end. A failed case, incomplete operation trailer, unresolved acknowledged backlog or unsafe cleanup halts progression and remains retained. Do not silently rerun it under the same output directory.

## Evidence coverage and reporting boundaries

| Requested measure | Authoritative evidence and reporting rule |
| --- | --- |
| Generated, emitted and acknowledged spans | Raw harness events and reconciled identity ledger; distinguish the measured cohort from setup/warmup/full-run totals. |
| Persisted spans and loss | Original identity ledger plus separate supplemental observations; report unique stored identities, physical rows, never-emitted input and unresolved identities separately. |
| OTLP ACK p50/p95/p99 | Measured-cohort first-send-to-ACK durations, integer nanoseconds; include sample counts and retain retries. |
| ClickHouse write latency/throughput | Successful client operation durations and rows completed inside the measurement window; phase timings and post-load server INSERT diagnostics are supporting evidence. |
| Query p95/error rate | Per-route successful latency tails and exact failed/total fractions, with HTTP status counts; failed requests are not removed from the error denominator. |
| Producer latency | Streaming `kafka_produce` timings around broker acknowledgment; direct-path admission remains its synchronous OTLP ACK. |
| Consumer throughput | Successful worker write and offset-commit rows per measurement interval; physical work may include replay, and is not a unique-identity count. |
| Kafka lag/backlog | Timestamped committed/end-offset samples and one-second admission/write/commit buckets; offset lag is in records. |
| Burst drain | Last successful write timing plus two zero-lag observations at the final end offset, checked against identity reconciliation; report observation bounds. |
| Sustained overload | Compare offered/admitted/write/commit progress over the same window and show sampled backlog growth while input continues. |

The harness labels a whole batch by its scheduling phase. A batch crossing a warmup/measurement boundary can therefore make measured-cohort counts differ from operation-completion counts in the wall-clock window. Neither series is adjusted to manufacture an exact offered-rate result. Report each denominator and interval explicitly. Successful-query p95 is conditional on success; a low p95 alongside many failures is not a healthy overall query result. The current query analyzer selects requests whose start timestamp falls inside the measurement window. Report scheduled measured tasks, issued measured tasks, queue drops, skips and late starts separately; query error fractions alone exclude tasks dropped before issuance. Visibility probes are additional query traffic outside the four-route request clock, and their observation bounds/censoring must remain distinct from ordinary query latency.

## Measured results and decision

All counts below are exact. Full-run totals include setup and warmup; pulse totals and measured ACK latency use the measured cohort. Rates are exact reduced fractions. The [complete per-case tables](../benchmarks/results/20261002-retrospective-continuation/analysis/tables.md) publish generated/ACKed/stored counts, ACK p50/p95/p99, client write phase latency and throughput, producer/consumer metrics, query p95/error fractions, queue drops, probe censoring, server INSERT diagnostics and drain bounds. The [machine-readable analysis](../benchmarks/results/20261002-retrospective-continuation/analysis/analysis.json) retains one-second progress and every valid lag sample. Both repetitions are reported separately; no pooled average hides variation.

### Sustained input: admission is not storage capacity

At 500 and 2,000 offered spans/s both paths stored all planned identities at the original snapshot in both repetitions. At 8,000 offered spans/s, Kafka admitted substantially more input, but the consumer fell behind. Completed-window write throughput was higher for Kafka in repetition 1 and lower in repetition 2. This does not demonstrate a sustained storage-capacity gain.

| 8,000 spans/s case | Planned full run | Generated/emitted | ACKed | Original unique stored | Eventually stored | Never emitted | Write rows/s in window | ACK p95 ns |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| r1 direct | 1040766 | 450558 | 450558 | 450558 | 450558 | 590208 | 15256/5 | 1801645000 |
| r1 streaming | 1040766 | 1039806 | 1039806 | 689728 | 1039806 | 960 | 17064/5 | 106344000 |
| r2 direct | 1040766 | 500736 | 500736 | 500736 | 500736 | 540030 | 17136/5 | 1396628000 |
| r2 streaming | 1040766 | 1040766 | 1040766 | 642816 | 1040766 | 0 | 2672 | 246304000 |

The following snapshots explicitly show sustained Kafka backlog growth. Offsets are recorded sequentially, so each row is an observation rather than an atomic broker snapshot. Time is the exact sample-start offset from the measured window start, after warmup.

| Repetition | Sample offset ns | Committed offset | End offset | Lag records |
| --- | ---: | ---: | ---: | ---: |
| 1 | 813773000 | 229 | 454 | 225 |
| 1 | 30813927000 | 891 | 1702 | 811 |
| 1 | 60814040000 | 1403 | 2953 | 1550 |
| 1 | 90814952000 | 1826 | 4198 | 2372 |
| 1 | 119814663000 | 2350 | 5384 | 3034 |
| 2 | 87096000 | 266 | 424 | 158 |
| 2 | 30088306000 | 744 | 1673 | 929 |
| 2 | 60087203000 | 1091 | 2924 | 1833 |
| 2 | 90087524000 | 1533 | 4174 | 2641 |
| 2 | 119089124000 | 1923 | 5383 | 3460 |

Streaming had 350078 and 397950 acknowledged spans missing at the original deadline. Supplemental identity reconciliation recovered all of them. Sampled peak lags were 3063 and 3484 records. Two-zero observations bound post-offering drain to 109815030000–111821142000 ns and 114088556000–116096748000 ns respectively. Those later observations do not erase the original deadline misses.

### Burst absorption: improved admission with delayed persistence

At the smallest burst both paths admitted every planned pulse span. At 8,000 spans/s Kafka reduced driver drops in repetition 1; neither path dropped input in repetition 2. At 32,000 spans/s Kafka admitted more in both repetitions, but both designs still dropped substantial input before emission. Kafka did not absorb the entire requested pulse. All emitted identities were stored by the original snapshot in these burst cases.

| Offered pulse spans/s | Rep | Path | Generated/emitted/ACKed pulse spans | Never emitted, full run | ACK p95 ns | Last write after offering, ns | Peak lag records | Kafka drain lower–upper ns |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | --- |
| 2000 | 1 | direct | 19998 | 0 | 32355000 | -5070424 | N/A | N/A |
| 2000 | 1 | streaming | 19998 | 0 | 29648000 | -1278284 | 2 | 0–1977643000 |
| 2000 | 2 | direct | 19998 | 0 | 32606000 | -2215825 | N/A | N/A |
| 2000 | 2 | streaming | 19998 | 0 | 27419000 | 2448313 | 3 | 0–1025207000 |
| 8000 | 1 | direct | 76158 | 3840 | 733722000 | 2329826366 | N/A | N/A |
| 8000 | 1 | streaming | 79614 | 384 | 77157000 | 8153526120 | 203 | 7740495000–9746051000 |
| 8000 | 2 | direct | 79998 | 0 | 151771000 | 304293102 | N/A | N/A |
| 8000 | 2 | streaming | 79998 | 0 | 46250000 | 6343506122 | 163 | 6035492000–8041638000 |
| 32000 | 1 | direct | 85056 | 234942 | 708509000 | 1909997011 | N/A | N/A |
| 32000 | 1 | streaming | 111936 | 208062 | 466840000 | 14173106382 | 354 | 13517244000–15522157000 |
| 32000 | 2 | direct | 93312 | 226686 | 696139000 | 1869536633 | N/A | N/A |
| 32000 | 2 | streaming | 120384 | 199614 | 391241000 | 14763169095 | 381 | 13837221000–15845720000 |

The last-write column is a client completion timestamp relative to offering end, not a sampled query-visibility guarantee. Negative values mean the last write completed before offering ended. Kafka drain also includes offset commit and polling; it is not directly interchangeable with the last-write timestamp.

### Controlled downstream slowdown: admission is isolated, reads are not

All four cases verified the 0.25-core quota and restoration to two cores. Kafka admitted the full planned input in both repetitions, while direct ingestion propagated storage pressure to bounded caller queues. Streaming ACK tails remained much shorter, but the retained work accumulated behind the consumer. This supports admission/storage decoupling and finite backpressure isolation for the tested slowdown. It does not isolate the query API from the same ClickHouse pressure.

| Rep | Path | Planned | Emitted | ACKed | Stored unique | Never emitted | ACK p50/p95/p99 ns | Peak lag records |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | --- | ---: |
| 1 | direct | 260766 | 228702 | 228702 | 228702 | 32064 | 27908000/4998915000/7398135000 | N/A |
| 1 | streaming | 260766 | 260766 | 260766 | 260766 | 0 | 20541000/35432000/51693000 | 271 |
| 2 | direct | 260766 | 207006 | 206622 | 207006 | 53760 | 1510466000/2791304000/15508680000 | N/A |
| 2 | streaming | 260766 | 260766 | 260766 | 260766 | 0 | 32134000/73038000/108440000 | 455 |

In direct repetition 2, 384 emitted spans were stored without a successful caller ACK. This is retry/response ambiguity, not lost stored data. Kafka peak lag was 271/455 records; the second streaming repetition continued draining after offering stopped. The exact quota command bounds, one-second progress and drain observations are retained per case. Faster ACK does not imply faster complete recovery: Kafka also admitted more work that had to be persisted.

### Query behavior, loss and attribution limits

Kafka did not establish better query availability. At sustained 8,000 spans/s, successful streaming detail responses were 0/119 and 0/120, compared with direct 68/110 and 63/118; status counts distinguish 404 visibility misses from 503 failures. Incident responses succeeded 15/119 and 16/120 for streaming, versus 6/109 and 5/118 for direct. The query population differs because storage progress differs. Incident input caps and storage pressure can both matter; response bodies were not retained, so these failures cannot all be assigned a cause.

Query p95 values are conditional on successful issued requests. The full tables report exact error fractions, status counts, scheduled drops and late starts. Visibility-probe traffic is additional load; its censored results remain censored rather than being assigned invented latency. ClickHouse client prepare/append/send timing and server QueryFinish diagnostics help locate observed cost, but cannot separate every contribution from CPU throttling, merges, serialization, connection waiting, queries, VM scheduling and synchronous observer writes. Direct writers and the serial Kafka worker also have different concurrency; this compares the implemented architectures, not a broker-only causal intervention.

Every emitted identity was accounted for after the allowed recovery observations. No acknowledged identity remained missing then, and no permanent loss of an emitted identity was demonstrated. Never-emitted driver input is explicitly counted as unmet offered workload. This finite result is not a retention-exhaustion, disk-loss, broker-HA or indefinite-overload guarantee.

### Decision

Kafka is justified for this explicit learning exercise and as a demonstrated option for decoupling admission from short downstream slowdowns and buffering some bursts. The evidence does not justify treating it as a sustained-throughput optimization or switching the default MVP solely to cure the Phase 4 overload. Keep the direct default; choose streaming when retained replay and acceptance-before-visibility are concrete requirements worth its measured lag, configured resource cost and added failure/retention operations. Collector disk persistence remains an untested simpler alternative for restart-surviving pending delivery, not a result of this comparison.

Limits remain local single-node Kafka with one partition/RF1, finite one-hour/1-GiB retention, two repetitions, fresh storage, short pulses, observer overhead, uncertain cross-process clock alignment, different admitted corpora, and the delay between original and continuation attempts. Neither architecture establishes production capacity or universal latency budgets. No Phase 6 work was started.

## Verification and reproducibility status

The combined analyzer completed with 28 observed cases and no global or case evidence issues. Both original and continuation restoration commands succeeded; the original failed network-start attempt remains in the analysis history. The final Python suite passed 45 tests. Earlier full Go race tests and vet passed for the observer changes; the focused observer check also passed after the zero-offset serialization test. These are distinct from the real 28-case workload evidence. See [test/build evidence](evidence/retrospective), [final controls](evidence/retrospective/final-controls.json), and the [direct-restoration smoke fixture](../benchmarks/results/20261002-retrospective-continuation/restored-direct-smoke/summary.json), which acknowledged and stored all 120 planned spans. API and ingestion readiness both returned 200 after restoration.

The actual continuation and combined-analysis commands were:

```sh
python3 benchmarks/retrospective.py --continue-from benchmarks/results/20261001-retrospective \
  --output benchmarks/results/20261002-retrospective-continuation \
  --binary /tmp/incidentlens-retro-bench --lag-binary /tmp/incidentlens-retro-lag
python3 benchmarks/analyze_retrospective.py benchmarks/results/20261002-retrospective-continuation \
  --output benchmarks/results/20261002-retrospective-continuation/analysis
```

The original campaign file remains failed because of the unmeasured network-start error; the successful continuation is not a rewrite of that history. The guarded runner verifies the completed prefix, unchanged workload/binaries and restored original stack before skipping completed labels. Reproducing the full campaign requires sufficient Docker network address space; stopped isolated projects retain networks until explicitly taken down without volume deletion.
