# Phase 5 measured comparison tables

Sources: [fresh direct raw analysis](../../../benchmarks/results/20260930T1415Z-direct/analysis/analysis.json) and [final bounded streaming raw analysis](../../../benchmarks/results/20261001-streaming-bounded/analysis/analysis.json). Values use the independent analyzer’s half-open measurement windows; warmup is excluded. Direct ACK means ClickHouse write completed; streaming ACK means broker admission. Neither streaming ACK nor a zero lag endpoint proves immediate trace visibility. The streaming `outage-r1` is **invalid as a fault comparison** because its requested outage did not start; its raw trace/ACK metrics remain in the analysis file.

## Ingestion and sampled visibility

| Stage | Path | ACK within window (spans/s) | ACK p95 / p99 (ms) | First send to observed p50 / p95 / p99 (ms) | Visibility observed / censored |
| --- | --- | ---: | ---: | ---: | ---: |
| Mixed #1 | Direct | 498.45 | 161.22 / 213.65 | 282.68 / 573.90 / 688.82 | 31 / 0 |
| Mixed #1 | Streaming | 498.45 | 28.29 / 37.51 | 239.92 / 293.95 / 304.95 | 31 / 0 |
| Steady #1 | Direct | 498.60 | 41.07 / 63.51 | 238.61 / 255.95 / 268.56 | 32 / 0 |
| Steady #1 | Streaming | 498.60 | 27.29 / 34.21 | 238.18 / 281.54 / 330.69 | 32 / 0 |
| Steady #2 | Direct | 498.60 | 51.80 / 75.55 | 238.87 / 275.13 / 318.43 | 32 / 0 |
| Steady #2 | Streaming | 498.60 | 33.02 / 44.63 | 256.19 / 335.37 / 385.47 | 32 / 0 |
| Steady #3 | Direct | 498.60 | 28.92 / 34.17 | 228.83 / 231.88 / 233.43 | 32 / 0 |
| Steady #3 | Streaming | 498.60 | 34.30 / 58.70 | 248.83 / 310.91 / 331.58 | 32 / 0 |
| Outage 15 s #1 | Direct | 497.00 | 18474.57 / 20372.47 | 234.15 / 1695.33 / 1695.33 | 14 / 2 |
| Outage 15 s | Streaming | invalid fault | invalid fault | invalid fault | outage not started |
| Ramp 500 #1 | Direct | 495.73 | 29.38 / 30.69 | 229.41 / 514.13 / 514.13 | 12 / 0 |
| Ramp 500 #1 | Streaming | 495.73 | 35.63 / 54.54 | 247.52 / 461.29 / 461.29 | 12 / 0 |
| Ramp 2000 #1 | Direct | 1996.00 | 38.38 / 408.33 | 266.47 / 368.90 / 1299.76 | 47 / 0 |
| Ramp 2000 #1 | Streaming | 1996.00 | 67.92 / 101.40 | 759.61 / 4606.36 / 5111.89 | 47 / 0 |
| Ramp 8000 #1 | Direct | 4757.33 | 898.86 / 1101.13 | 816.22 / 1473.31 / 1592.44 | 125 / 0 |
| Ramp 8000 #1 | Streaming | 7903.20 | 387.33 / 578.27 | — / — / — | 0 / 187 |

Observation uses 200 ms polling and is an upper bound on actual storage visibility. Censored opportunities make observed-only tails insufficient for an unconditional visibility pass. Direct outage’s 18.47/20.37 s ACK p95/p99 fails the 0.5/2 s targets; it did recover all finite identities. Streaming ramp 2,000 has 4.61/5.11 s observed visibility p95/p99, exceeding the 1/3 s targets. Streaming ramp 8,000 has 187 censored probes and no observed latency quantiles.

## Mixed query route latency and errors

| Route | Path | Successful count | Successful p50 / p95 / p99 (ms) | Failed HTTP statuses |
| --- | --- | ---: | ---: | --- |
| services | Direct | 120 | 63.39 / 241.96 / 426.89 | none |
| services | Streaming | 119 | 22.43 / 91.10 / 165.09 | 503×1 |
| traces | Direct | 120 | 37.38 / 369.61 / 480.42 | none |
| traces | Streaming | 120 | 22.92 / 33.17 / 63.77 | none |
| detail | Direct | 119 | 116.67 / 524.01 / 1156.08 | 503×1 |
| detail | Streaming | 115 | 42.72 / 182.01 / 347.65 | 404×5 |
| incidents | Direct | 120 | 635.31 / 1416.88 / 1894.71 | none |
| incidents | Streaming | 120 | 319.87 / 537.20 / 575.91 | none |

Successful latency quantiles exclude failed HTTP calls. Direct mixed had one detail 503; streaming mixed had one services 503 and five detail 404s. Both fail the declared zero non-2xx target. Direct incidents p95 1.42 s also exceeds its 1 s target.

## Three steady repetitions

| Path | ACK rate mean / sample SD (spans/s) | ACK p95 mean / SD (ms) | ACK p99 mean / SD (ms) |
| --- | ---: | ---: | ---: |
| Direct | 498.60 / 0.00 | 40.59 / 11.45 | 57.74 / 21.28 |
| Streaming | 498.60 / 0.00 | 31.54 / 3.73 | 45.85 / 12.29 |

All six steady stages reconciled 75,000/75,000 planned identities. Their ACK rate was 498.60 spans/s in each repetition. These are three short local runs against a growing retained corpus, not a production capacity estimate.

## Container resources during mixed and steady measurement windows

The four stages per path are Mixed plus Steady 1–3. Maximum quota CPU and memory are the largest *sampled stage maxima* across those windows; mean CPU is weighted by each stage’s sample count, in percent of one core (100% = one core). This is not a time-weighted mean or a peak guarantee.

| Path | Container | Samples | Max CPU / quota (%) | Sample mean CPU (% of one core) | Max observed memory (MiB) |
| --- | --- | ---: | ---: | ---: | ---: |
| Direct | api | 233 | 103.37 | 15.02 | 194.90 |
| Direct | clickhouse | 233 | 111.70 | 115.49 | 2048.00 |
| Direct | collector | 233 | 7.61 | 2.30 | 85.64 |
| Direct | ingest | 233 | 17.24 | 6.28 | 21.84 |
| Streaming | api | 215 | 57.12 | 7.86 | 108.10 |
| Streaming | clickhouse | 215 | 108.20 | 108.56 | 2048.00 |
| Streaming | collector | 215 | 8.71 | 2.05 | 253.60 |
| Streaming | kafka | 215 | 106.25 | 55.73 | 587.30 |
| Streaming | stream-ingest | 215 | 10.72 | 5.56 | 47.69 |
| Streaming | stream-worker | 215 | 23.03 | 6.66 | 19.34 |

Docker CPU may briefly report more than 100% of configured quota at a sampled instant. Memory is Docker’s reported usage/cache convention. Direct had four active measured containers; streaming had six, adding Kafka, producer and worker while direct ingest was stopped.

## Kafka disk and consumer progress: observed endpoints

| Streaming stage | Kafka volume before → after (MiB) | Minimum sampled filesystem free (GiB) | Group committed / end offset at stage end | Lag at stage end |
| --- | ---: | ---: | ---: | ---: |
| mixed #1 | 4.63 → 10.77 | 1891.54 | 1278 / 1278 | 0 |
| steady #1 | 10.77 → 17.75 | 1891.16 | 1669 / 1669 | 0 |
| steady #2 | 17.75 → 24.74 | 1890.85 | 2060 / 2060 | 0 |
| steady #3 | 24.74 → 31.72 | 1890.77 | 2451 / 2451 | 0 |
| ramp-500 #1 | 35.20 → 37.52 | 1890.97 | 2778 / 2778 | 0 |
| ramp-2000 #1 | 37.52 → 46.81 | 1891.00 | 3299 / 3299 | 0 |
| ramp-8000 #1 | 46.81 → 83.60 | 1890.79 | 5312 / 5360 | 48 |

These are beginning/end offsets and sampled disk observations, **not** measured instantaneous peaks. Kafka `df` reports the mounted filesystem’s free space; it is not an independent Docker virtual-disk capacity estimate. The original ramp 8,000 endpoint had lag 48; the later supplemental endpoint was committed/end 5360/5360 with zero lag. [Ramp storage after drain](../../../benchmarks/results/20261001-streaming-bounded/ramp-8000-r1/storage-after-drain.json) contains that later observation.

## Ramp 8,000 original deadline versus supplemental observation

At the original [streaming ramp 8,000 reconciliation](../../../benchmarks/results/20261001-streaming-bounded/ramp-8000-r1/harness/summary.json), 399,996 spans were planned, 360,320 were stored, 35,260 **producer-ACKed and emitted** spans were still missing, and 4,416 planned spans had never been emitted. Thus original-deadline zero ACKed loss/visibility did **not** pass, despite 7,903.20 broker-ACKed spans/s inside the measurement window. The [supplemental drain](../../../benchmarks/results/20261001-streaming-bounded/ramp-8000-r1/supplemental-drain.json) began 2026-10-01 02:59:17.165791 UTC and ended 02:59:19.048107 UTC (within its separate 120 s bound): 395,580 emitted spans were then stored, emitted/ACKed missing were zero, and the same 4,416 never-emitted driver spans remained absent. This later observation does not rewrite the original deadline or demonstrate sustainable 8,000 spans/s storage throughput.

The direct ramp 8,000 had 138,432 planned spans never emitted by its driver, with every emitted/ACKed span stored. Different admission and storage bottlenecks make the two 8,000-stage ACK rates unsuitable as a simple capacity win/loss claim.
