# Controlled retrospective: direct storage versus retained Kafka admission

Declared before this experiment's measurements, October 1, 2026. This is a test
of the user's Kafka hypothesis, not Phase 6 or a throughput improvement claim.
Preserve earlier Phase 4/5 evidence and all failed attempts. Exact integer counts
and nanoseconds are authoritative; derived rates are published as exact fractions
where a decimal cannot represent them without rounding.

## Controls and changes from the original campaign

Use the same physical host, Docker VM, pinned ClickHouse/Collector/Kafka images,
six-span fixture, three services, 256-byte payload, immutable identities and
normalization as Phase 4/5. Preserve 32-trace batches, 16 caller workers, 64-batch
queue, eight-second RPC deadlines, three attempts, 120-second caller drain and
35-second settling. Keep all product CPU/memory limits and Kafka retention fixed.
Do not tune batching, storage settings, consumer concurrency or query caps.

Each case uses a new isolated Compose project and empty ClickHouse volume (and
Kafka volume for streaming); this deliberately replaces the earlier growing
shared corpus to remove cross-case data-size differences. Existing product and
prior experiment volumes are preserved. No Demo or unrelated tests run during
measurement. Alternate path order between repetitions. Fresh storage and startup
caches limit extrapolation to an established large database; record all images,
quotas, hardware, source hashes, and initial/final physical rows and parts.

The original direct stack is stopped only while experiment ports are in use and
restored afterward. Experiment containers stop after each case; volumes remain.
The benchmark operation recorder is enabled identically on both storage writers;
it adds observer work. Capture each prepare/append/send/total client write and
Kafka publish/consumer commit duration in integer nanoseconds, rows, status and
offset. No span payload, SQL text, IDs, credentials or arbitrary errors are logged.
Each operation file is capped at 64 MiB and has an explicit completeness trailer.
A missing trailer, overflow or write failure invalidates operation metrics.

Client write duration includes connection acquisition/protocol/serialization and
server response; it is not pure server execution. Preserve the phase breakdown.
After load, collect ClickHouse query_log insert durations, rows and ProfileEvents
if available; unavailable server diagnostics are explicitly unknown. Exact broker
publish latency is measured around ProduceSync. Consumer throughput uses successful
write/commit row events, with retries/physical copies distinguished from final
unique identities. Direct producer latency is its synchronous OTLP ACK, not a
fictional broker metric.

## Paired cases

Run each case on direct and streaming paths, twice, reversing path order for the
second repetition. Use fresh seeds/namespaces and volumes for every case. Each
path sees the same fixture shape and offered clock; actual driver drops remain
measured differences, never silently compensated with reduced input.

| Case | Offered spans/s | Warmup | Measurement / pulse | Concurrent queries |
| --- | --- | --- | --- | --- |
| Sustained | 500, 2000, 8000 | 10 seconds at the selected rate | 120 seconds | 4 requests/second |
| Burst from idle | 2000, 8000, 32000 | 0 | 10 seconds, then no new input | 4 requests/second during pulse |
| Downstream slowdown | 2000 | 10 seconds | 120 seconds | 4 requests/second |

Use the original services/traces/detail/incidents route mix and 128 baseline
setup traces. Empty storage does not remove the detector's bounded global-input
cap: later incident 503s at high rates must be retained as errors, not discarded.
Short bursts have few query tail samples and cannot establish a robust p95.

For slowdown, change only the experiment ClickHouse CPU quota from two cores to
0.25 cores 30 seconds after load starts, hold for 30 seconds, then restore two.
Record command initiation/completion and inspected quota. Restore even on failure.
This controlled CPU-pressure intervention is not an outage or a storage tuning.

Kafka offsets are polled once per second using the existing Go client, not Java
admin processes. Read committed position then log end, recording both request
times. This sequential snapshot can include concurrent production; record lag
in records, not spans. Observe lag after the pulse until two consecutive zero-lag
samples at a stable end, within a 120-second supplemental drain after the original
settling snapshot. Retain original deadline results separately. Final identity
reconciliation is required: zero lag alone does not prove delivery.

Publish one-second bucketed generated/emitted/ACKed counts, successful storage
rows, broker-produced rows and consumer committed rows alongside offset samples.
Where sustained arrivals exceed observed storage progress, show the time series
and backlog growth rather than quoting admission throughput as storage capacity.
Drain observations are polling bounds; exact event timestamps remain in raw data.

## Safety, loss and decision rules

Preserve the original two-million planned-span and 256 MiB event caps, host 10 GiB
free-space floor, ClickHouse 4 GiB growth stop, 98% memory for ten seconds stop,
unexpected OOM/restart and failed readiness stops. Kafka free space must exceed
1 GiB and actual broker volume stay below 2 GiB. Stop the entire experiment if a
safety boundary or incomplete cleanup makes another load unsafe. Do not delete
data or increase limits to manufacture a pass.

Separate planned, generated, emitted, caller-ACKed, stored unique, physical write
rows and never-emitted driver drops. Missing at a deadline is not automatically
permanent loss; declare it unresolved unless a retained source is exhausted or
expiry proves loss. Preserve retry ambiguity and duplicate-safe FINAL results.

Compare burst absorption, admission/storage decoupling, backpressure isolation and
slowdown recovery separately. Kafka is justified operationally only for a concrete
improvement whose cost and finite retention/durability limits are acceptable.
Faster ACKs alone do not prove faster persistence, query availability or higher
sustained capacity. Explain what Phase 4 demonstrated and did not attribute, the
original educational activation, the retrospective hypothesis and its outcome.
