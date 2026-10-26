# Phase 4 predeclared workload and budgets

Declared before any Phase 4 measurement on 2026-09-28. These are targets and
procedures, not measured results. Preserve failed attempts and amendments.
The existing Collector → ingestion → ClickHouse and read-only API stack is the
system under test. No schema, runtime technology, queue size, resource quota or
query cap is changed to achieve a target.

## Environment and corpus

Use the retained local Docker Desktop volume. Record host model/CPU/RAM/OS,
Docker VM CPU/RAM, engine and image versions, product CPU/memory caps, source
revision and dirty-file hashes, total rows/active parts/bytes before and after
every stage. The initial machine is an Intel MacBookPro16,1 with 8 physical/16
logical CPUs and 64 GiB RAM; Docker has 16 CPUs and approximately 7.75 GiB RAM.
Product caps remain ClickHouse 2 CPUs/2 GiB, ingestion and API 1 CPU/512 MiB each,
and Collector 1 CPU/256 MiB. Runtime collection must verify these values.

Do not clear prior data or suppress normal background merges. Record idle
resource samples before each run. The pinned Demo may remain idle; do not generate
Demo traffic or run unrelated tests during measurements. Results describe this
shared local virtualized environment and growing retained corpus, not native
hardware capacity or production performance.

## Finite synthetic traces

- Full sampling; six spans per trace, equally distributed across `bench-frontend`,
  `bench-api`, and `bench-store`. The chain contains three SERVER, two CLIENT and one INTERNAL
  span. SERVER denominators remain three requests per trace.
- Deterministic IDs from a recorded run seed plus trace/span indices; an isolated
  namespace per run. Replays preserve exact IDs, timestamps and payloads.
- A 256-byte allowlisted `test.payload` string plus bounded typed fixture
  attributes; the final harness records the precise keys and actual serialized
  protobuf request bytes and bytes/span. No compression unless explicitly
  recorded in an amended plan. No sensitive inputs.
- Every twentieth paced trace has ERROR status; other paced traces are OK. A
  six-span duration ladder (300/280/260/240/220/200 ms with small nested offsets)
  represents synthetic completed work; no latency is added to the generator.
  Event starts precede export by one second. These are constructed load traces,
  not measured Demo response durations.
- The mixed-query run first inserts 128 separate baseline traces twenty minutes
  earlier with a 10/9/8/7/6/5 ms duration ladder and OK status. This setup is
  excluded from offered-load/latency statistics, recorded separately, and fully
  included in identity reconciliation. It exercises explained incident evidence
  once current SERVER sample counts exceed 100.
- Target export batch: 32 traces / 192 spans. Tail rounding to whole traces and
  actual planned/emitted totals must be reported. Sixteen export workers, a
  bounded 64-batch queue, eight-second RPC deadline, at most three attempts per
  batch with 250/500 ms retry delays. Never retry OTLP partial success as a
  transient error. Drain/recovery allowance: 120 seconds. After workers finish, allow a separately
  recorded 35-second settling interval for potentially ambiguous Collector
  attempts (its retry budget is 30 seconds) before final reconciliation. This
  settling time is outside measured load and included in total elapsed time.
- Open-loop scheduled arrivals. Record scheduled, generated, enqueued, attempted,
  acknowledged and final stored identities separately. Never quietly slow the
  offered clock when workers are busy. Queue drops and scheduler lag are
  generator failures, not proof of product saturation.

## Ordered campaign

| Stage | Offered spans/s | Warmup | Measurement | Queries / fault |
| --- | ---: | ---: | ---: | --- |
| Mixed | 500 | 10 s | 120 s | 4 HTTP requests/s, four query workers, equal shares of services/search/detail/incidents; 128 baseline setup traces |
| Steady 1, 2, 3 | 500 | 30 s | 120 s each | Query mix off; visibility probes remain measured |
| Outage | 500 | 15 s | 60 s | Stop ClickHouse 30 s after load starts, keep stopped 15 s, then restart retained container; query mix off |
| Ramp 1–4 | 500 / 2,000 / 8,000 / 32,000 | 5 s each | 45 s each | Query mix off; stop ramp after demonstrated product saturation or a safety boundary |
| Ramp ceiling, if needed | 100,000 | 0 s | 20 s | Query mix off; at most two million planned spans |

Each row is a separate finite run with a fresh namespace. Steady repetitions
keep the workload fixed; disclose changing corpus size and background resource
usage rather than silently pooling them. Warmup observations are retained but
excluded from the primary latency/throughput distributions. Also report total
run duration including retries/drain, so fast pacing cannot conceal a long tail.

Run mixed queries first: incident evaluation reads all namespaces in its
35-minute interval and has global 100,000-span/32 MiB/500-operation caps. A
namespace filter does not isolate this input. Record the initial global corpus;
if this mixed workload hits an existing cap, retain and explain its 503 responses
as a failed budget. Do not loosen the cap or relabel fast rejections as good
query latency. This short candidate-producing query workload is not a claim
about unrestricted long-running incident queries.

Query ranges use the actual run interval and namespace. Search and summaries
query the growing run corpus; detail uses a known emitted/acknowledged trace;
incidents use an explicit current end and the baseline setup. Record every route,
status, response size and latency. Report per-route successful and failed request
distributions, not just a pooled average. Keep visibility-probe requests separate.

## Targets and stopping rules

- At 500 spans/s: at least 95% of scheduled measured load acknowledged within the
  measurement interval; zero terminal valid-export errors or scheduling drops.
- Acknowledgment latency from first send: p95 ≤500 ms, p99 ≤2 s. Also report
  schedule-to-ack latency and every attempt's latency/status to expose backlog.
- Per API route: p95 ≤1 s, p99 ≤2 s, zero non-2xx responses during mixed steady
  operation. Report p50 as well, counts, and tail sample limitations.
- Zero missing acknowledged identities after drain. Zero missing emitted and
  planned finite identities after bounded replay/drain is the end-to-end target;
  distinguish never-emitted scheduling drops from delivery loss.
- Visibility: sample the first trace of every tenth batch, poll at 200 ms from
  first send when feasible, using two probe workers and a bounded 64-sample
  queue. Record skipped/censored sample opportunities explicitly; do not declare
  an unconditional visibility target passed from only the successful subset.
  Report p50/p95/p99, sample count and censoring.
  Target first-send-to-observation p95 ≤1 s and p99 ≤3 s. Polling supplies an upper
  bound, not exact storage arrival time. Preserve separate scheduled/send/ack/
  observation clocks and report probe traffic overhead.
- Outage: transient errors, retries and queue overflow are expected observations;
  zero acknowledged loss and zero final finite-identity loss remain targets.
  Report actual stop/restart/readiness times and recovery time, not only the
  requested 15-second outage. Restore ClickHouse in cleanup even on failure.
- Ramp product saturation: terminal export errors exceed 1%, measured acknowledged
  throughput falls below 95% of offered rate with server/backpressure evidence,
  or ack p99 exceeds 2 s. Report the breached stage and last observed sustainable
  stage. Generator-only lag/drops are a driver ceiling; they do not establish
  product capacity. If the highest stage remains sustainable, report only a
  tested lower bound; do not invent an exact capacity.
- Safety boundaries: stop load on unexpected process restart/OOM, persistent
  readiness failure (three consecutive failed samples) outside the declared
  outage, less than 10 GiB host disk free,
  more than 4 GiB additional ClickHouse active data, or a container at ≥98% of
  its memory cap for ten consecutive seconds. Preserve the boundary observation
  as a failed/limited measurement. Never delete volumes as cleanup.
- Per-stage cap: two million planned spans and 256 MiB raw event output. A stage
  that cannot fit these declared bounds is rejected before starting.

## Evidence and Phase 5 decision

Keep machine-readable configuration, per-attempt/query/visibility events,
identity reconciliation, stage summaries, Docker CPU/memory/block-I/O samples,
generator process CPU/RSS, active ClickHouse parts/disk use and orchestration
errors. Docker CPU 100% means one core; show quota-normalized usage separately.
Block I/O is cumulative I/O, not disk occupancy. Source hashes and exact commands
must permit another operator to reproduce the run without attributing dirty
worktree changes to an unrelated commit.

Publish all three steady runs and their variation, the entire attempted ramp,
mixed-query results and outage recovery. Failed targets do not disappear from
the report. Distinguish CPU/storage saturation, application limits, generator
limits and unresolved causes. No claim extends beyond measured inputs/duration.

Recommend Phase 5 only if evidence establishes an unmet buffering/replay or
throughput need that the existing bounded Collector, backpressure and caller
retries cannot meet. A storage bottleneck alone does not justify Kafka; a broker
cannot create storage throughput. Acknowledged loss is a correctness issue to
resolve before claiming an architecture benefit. Phase 5 implementation requires
a separate request and an ADR; this phase only records the decision.
