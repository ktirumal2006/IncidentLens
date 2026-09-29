# Phase 4 verification

The finite campaign completed on 2026-09-28 (America/New_York). Performance
targets and milestone completion are distinct: the measurements expose failed
query and overload targets rather than claiming every budget passed.

## Reproduction and evidence

The [workload declaration](../benchmarks/WORKLOAD.md) and
[ordered plan](../benchmarks/plan.json) were written before measurements.
The [harness instructions](../benchmarks/README.md) describe build and execution.
The completed [raw campaign](../benchmarks/results/20260929T0200Z/campaign.json)
started on 2026-09-29 UTC (2026-09-28 America/New_York). Its directory name is
an identifier; exact observation timestamps are inside the records.

```sh
(cd backend && go test -race ./cmd/bench && go build -o /tmp/incidentlens-bench ./cmd/bench)
python3 -m unittest discover -s benchmarks -p 'test_*.py'
python3 benchmarks/run.py --plan benchmarks/plan.json --binary /tmp/incidentlens-bench --output benchmarks/results/20260929T0200Z
```

The actual Go executable was `/tmp/incidentlens-toolchain/go/bin/go`, version
1.26.8. The benchmark executable SHA-256 is
`ea4c841d4dff0ceca940e5500f024a2a2902ab14479a3e263592fd53966e099b`.
The [environment record](../benchmarks/results/20260929T0200Z/environment.json)
contains image IDs, versions, source hashes, dirty worktree status, exact hardware
and resource limits. Measurements used the Phase 3 product with an uncommitted
Phase 4 harness; the source manifest identifies that harness independently of HEAD.

Host: Intel MacBookPro16,1, i9-9880H, 8 physical/16 logical CPUs, 64 GiB RAM.
Docker Desktop engine 29.8.0 provided 16 CPUs and 8,320,970,752 bytes of VM memory.
Container quotas remained ClickHouse 2 CPUs/2 GiB, API and ingestion each
1 CPU/512 MiB, and Collector 1 CPU/256 MiB. Existing data and normal background
merges were retained. The stack had been stopped and was restarted before this
campaign; no volume was deleted. Before load, the retained table had 106,934
physical rows. This is a shared virtualized development environment, not a
production capacity estimate.

## Completed observations

ACK throughput counts measured-phase spans acknowledged inside the measurement
window. Batch boundaries and whole-trace rounding explain rates slightly below
500 spans/s even when all offered identities are acknowledged.

| Stage | Measured ACK spans/s | ACK p95 / p99 ms | Planned spans including setup/warmup | Missing planned identities |
| --- | ---: | ---: | ---: | ---: |
| Mixed | 498.45 | 124.097 / 336.408 | 65,766 | 0 |
| Steady 1 | 498.60 | 54.563 / 119.898 | 75,000 | 0 |
| Steady 2 | 498.60 | 78.236 / 136.507 | 75,000 | 0 |
| Steady 3 | 498.60 | 43.311 / 108.985 | 75,000 | 0 |

All four completed stages had zero terminal export errors, rejected spans, retries,
or scheduling drops. Their bounded FINAL identity ledgers reconcile every planned
trace/span identity after the separate Collector settling interval. The independent
analyzer found no evidence gaps in these four stages.

The mixed run made 480 measured API requests: 120 per route. Trace search and
trace detail each returned one HTTP 503; the other 478 requests returned 200.
Thus the zero-error API target failed. Successful-request p95/p99 latency budgets
passed on every route. Failure response bodies were not captured and API stdout
contained no corresponding diagnostic entries; the causes of these two responses
remain unknown. Fast failures are excluded from successful latency distributions.

Mixed visibility had 31 measured samples, none censored, with first-send-to-
observation p95 440.414 ms and p99 1,308.742 ms. Steady 1 had 32 samples, none
censored, with p95 361.373 ms and p99 404.530 ms. These are polling upper bounds
with 200 ms polling; small tail sample counts limit p99 interpretation.

## Storage outage

The stop command began 30.004 seconds after the harness load start. After the
stop command completed, ClickHouse remained stopped for 15.003 seconds. Both
API and ingestion readiness had recovered by 2026-09-29T02:26:30.954393Z,
approximately 20.682 seconds after the stop command began. The retained volume
was reused.

All 37,500 planned spans were finally stored and acknowledged, with zero missing
identities, terminal export errors, scheduling drops, or rejected spans.
Sixteen measured batches required 28 retry attempts in total: 27 initial/retry
attempts returned Unavailable and one returned DeadlineExceeded. Measured
acknowledgment p95/p99 increased to 18.674/20.470 seconds. These exceed normal
steady latency budgets; outage errors/retries were explicitly allowed by the
predeclared scenario. Two visibility samples were censored, so observed-only
visibility percentiles cannot establish an unconditional latency target.

The independent raw-event/identity-ledger analysis found no evidence gaps in
this completed outage run. This demonstrates recovery for this finite outage,
rate and caller retry policy; it does not establish durability across Collector
restart or longer storage outages.

## Ramp and tested limit

| Offered spans/s | Measured ACK spans/s | ACK p50 / p95 / p99 ms | Planned / emitted spans, entire run | Never-emitted spans |
| ---: | ---: | ---: | ---: | ---: |
| 500 | 495.73 | 24.903 / 44.206 / 50.451 | 24,996 / 24,996 | 0 |
| 2,000 | 1,996.00 | 26.837 / 202.754 / 447.068 | 99,996 / 99,996 | 0 |
| 8,000 | 4,074.67 | 689.048 / 998.481 / 1104.067 | 399,996 / 233,340 | 166,656 |

At 8,000 offered spans/s, measured acknowledgment throughput fell to 4,074.67
spans/s (50.95% of the scheduled measured identities). ACK p95 rose to 998.481 ms,
providing the predeclared backpressure evidence for the throughput saturation
criterion. There were 868 dropped batches across warmup and measurement: nine
missed scheduling deadlines and 859 full-queue drops. The bounded driver queue
therefore also limited delivered load. Schedule-to-ACK p99 was 4,656.431 ms,
compared with first-send-to-ACK p99 of 1,104.067 ms.

All 233,340 emitted spans in that stage were acknowledged and found in FINAL
storage; the 166,656 missing planned spans were never emitted. This fails the
end-to-end planned-identity target without showing acknowledged-data loss.
There were no terminal export errors or protocol rejections in the ramp.
The 32,000 and 100,000 stages were skipped under the predeclared stop-after-
saturation rule; neither rate was measured.

The highest tested stage without a saturation trigger was 2,000 spans/s for
45 measured seconds after five seconds of warmup. This is a tested lower bound
for this ingestion workload, not an exact capacity estimate or a guarantee for
concurrent incident queries at that rate. Only the 500 spans/s case has three
steady repetitions and a concurrent-query run.

## API and visibility distributions

Successful and failed requests are separate. Each route received 120 measured
requests; successful percentile sample counts are shown below.

| Route | Successes / failures | Successful p50 / p95 / p99 ms | Failed p50 / p95 / p99 ms |
| --- | ---: | ---: | ---: |
| detail | 119 / 1 | 55.129 / 637.054 / 1051.227 | 1.941 / 1.941 / 1.941 |
| incidents | 120 / 0 | 341.887 / 831.691 / 1795.348 | — / — / — |
| services | 120 / 0 | 38.211 / 320.005 / 532.424 | — / — / — |
| traces | 119 / 1 | 25.152 / 201.310 / 488.952 | 2.868 / 2.868 / 2.868 |

| Stage | Visibility observations / censored | First-send-to-observation p50 / p95 / p99 ms |
| --- | ---: | ---: |
| mixed-r1 | 31 / 0 | 261.597 / 440.414 / 1308.742 |
| steady-r1 | 32 / 0 | 240.958 / 361.373 / 404.530 |
| steady-r2 | 32 / 0 | 248.153 / 350.118 / 369.993 |
| steady-r3 | 32 / 0 | 243.564 / 344.627 / 398.959 |
| outage-r1 | 14 / 2 | 241.892 / 1900.206 / 1900.206 |
| ramp-500-r1 | 12 / 0 | 244.332 / 263.291 / 263.291 |
| ramp-2000-r1 | 47 / 0 | 287.463 / 902.511 / 978.026 |
| ramp-8000-r1 | 101 / 0 | 990.282 / 1503.726 / 1900.166 |

Visibility samples apply to attempted batches. Dropped batches have no first-send
clock and are accounted for as never emitted, not fast visibility successes.
The 8,000-stage observed visibility p95 exceeds the 1-second budget. The outage's
censored observations prevent an unconditional percentile pass. Probe counts,
latencies and bytes are retained separately from the four-route query workload.

## Resources, dataset growth and bottlenecks

Two-second requested sampling produced irregular actual intervals because Docker
commands take time; raw command timestamps/durations are retained. CPU means are
unweighted sample means. Docker CPU 100% represents one core; ClickHouse has a
two-core quota. Short observations can exceed a nominal quota due to accounting
windows and rounding. Docker memory uses its reported cache-adjusted convention.
No OOM, unexpected restart, or declared safety stop was recorded. Memory briefly
exceeded 98% in some stages, but not for the declared ten consecutive seconds.

| Stage | ClickHouse mean CPU / peak quota-normalized CPU % | Peak ClickHouse memory MiB | Generator peak CPU % / RSS MiB | Active data growth bytes |
| --- | ---: | ---: | ---: | ---: |
| mixed-r1 | 158.52 / 110.70 | 2045.95 | 1.3 / 28.91 | 1,541,426 |
| steady-r1 | 134.15 / 106.78 | 2038.78 | 1.0 / 29.15 | 1,798,549 |
| steady-r2 | 137.03 / 108.12 | 2045.95 | 1.6 / 28.39 | 1,796,471 |
| steady-r3 | 113.58 / 104.63 | 2033.66 | 1.7 / 26.58 | 1,738,253 |
| outage-r1 | 71.44 / 104.62 | 1939.46 | 5.2 / 59.46 | 962,666 |
| ramp-500-r1 | 91.72 / 89.03 | 2042.88 | 1.3 / 26.30 | 605,081 |
| ramp-2000-r1 | 147.10 / 112.35 | 2032.64 | 5.7 / 34.11 | 2,353,412 |
| ramp-8000-r1 | 213.31 / 110.92 | 1909.76 | 12.7 / 65.52 | 5,556,899 |

At the 8,000-stage, ClickHouse averaged 213.31% one-core CPU, while ingestion
averaged 64.68% and peaked at 96.41%; Collector averaged 20.52%, API 2.55%, and
the driver peaked at 12.7%. ClickHouse CPU pressure is consistent with the rising
export latency and queue backlog. This is a bottleneck hypothesis, not a causal
profile: normal merges, synchronous insert overhead, VM scheduling and ingestion
work may all contribute. The driver drops and limited observations preclude
claiming an exact storage ceiling. Mixed-query ClickHouse memory peaked near
its cap; that may relate to the two 503s, but their cause was not captured.

The retained table grew from 106,934 to 793,532 physical rows. Active parts grew
from 6 to 11, and active data from 2,124,816 to 18,477,573 bytes. These global
snapshots include merging and existing data; per-run FINAL ledgers establish
logical identities. The repeated `x` payload is highly compressible, so disk
growth cannot predict arbitrary telemetry storage cost. Full batches serialized
to 81,822 bytes for load starting at index zero (426.156 bytes/span); the mixed
baseline offset produced 82,014 bytes (427.156 bytes/span). Sequence varints and
tail batches change actual request sizes, retained per attempt.

All four containers' CPU/memory and block I/O, generator RSS/CPU, host free disk,
and ClickHouse filesystem/active-part observations are in the
[analysis JSON](../benchmarks/results/20260929T0200Z/analysis/analysis.json) and
per-stage resources/storage files. Block I/O is cumulative traffic, not occupancy.
The intentional ClickHouse restart reset its I/O counters; its outage delta is
reported unknown instead of a negative byte count. This post-measurement analyzer
correction and its regression test did not change the generator, runner or raw
observations. Their captured source hashes still match the final harness.

## Repeated variation

The three 500 spans/s steady runs each acknowledged 498.6 measured spans/s.
ACK p95 ranged 43.311–78.236 ms (mean 58.703 ms, sample standard deviation
17.827 ms); p99 ranged 108.985–136.507 ms (mean 121.797 ms, sample standard
deviation 13.859 ms). Each reconciled 75,000 spans with no drops or loss.
The corpus grew between runs; these are three ordered local observations, not
independent randomized trials. About three observations support each steady
p99 tail and only one supports most visibility/API p99 tails.

## Phase 5 decision

**Defer Phase 5.** The declared 15-second storage outage recovered every planned
span using existing bounded Collector behavior and caller retries. There is no
measured unmet replay/buffering requirement in this campaign. The 8,000 spans/s
case shows a throughput/backpressure limit with ClickHouse CPU pressure; a broker
would not create storage throughput and could merely postpone backlog failure.

The simpler next investigation, if separately requested, is profiling inserts,
merges and query failures, and testing batching within the existing architecture
against the same loss/retry contracts. This phase does not implement such tuning.
Reconsider streaming only for a separately declared durable replay or longer-
outage requirement that the existing bounded path cannot meet, with an ADR
comparing direct ingestion, Collector buffering and a broker. No Kafka or other
runtime dependency was added.

## Acceptance evidence and limits

| Phase 4 requirement | Evidence |
| --- | --- |
| Repeatable harness and workload | `backend/cmd/bench`, runner/analyzer/tests, WORKLOAD.md and plan.json; committed with this report |
| Targets declared before measurement | Campaign records the declaration/plan hashes; targets remain distinct from failed results |
| Throughput, loss/retries, ACK/visibility/API latency, CPU/memory/disk | Raw events, per-trace FINAL ledgers, resources/storage snapshots and derived analysis |
| Steady, increasing load, concurrent queries, outage/recovery | Eight completed stages, three steady repetitions, declared saturation at 8,000, restored dependency readiness |
| Hardware, versions, commands, dataset, sampling, variation, bottlenecks | Environment/source manifests, exact stage commands, this report and raw JSON |
| Phase 5 decision | Defer, for the evidence and limits above |

The campaign's `completed` status means orchestration finished, not that every
performance target passed. Known failures are the two mixed-query 503s and the
8,000-stage throughput, scheduling, visibility p95 and planned-delivery budgets.
There was zero missing acknowledged or emitted data across all stages. Longer
outages, arbitrary payloads, disk failure, Collector crash durability, long-running
incident queries and production-scale capacity were not measured.

Validation: Go benchmark tests passed with the race detector; Python runner and
analyzer tests passed, including counter resets, finite identity coverage, partial
offering, failure cleanup and missing-evidence handling. The raw analyzer completed
with no evidence gaps in all eight stages. Product runtime code was unchanged;
this campaign is not a rerun of the Phase 3 UI/Demo acceptance suite.
