# Incident evaluation API (Phase 3)

`GET /api/v1/incidents?end=<RFC3339>` evaluates trace rule version `trace-v1`.
`end` is required, accepts an explicit timezone, and is returned in UTC with
nanosecond precision. Optional exact `service` and `namespace` filters select
reported operation groups; absence means all and an empty namespace is literal.
Unknown/repeated parameters are rejected. No mutation, scheduler or incident
lifecycle is introduced. Existing JSON error codes and five-second API query
deadline apply. The UI initially selects one minute before now.

Current window: `[end-5m,end)`; baseline: `[end-35m,end-5m)`. Both must be inside
seven-day retention and `end` must not be in the future. SERVER spans alone form
request denominators, grouped by namespace/service/operation. CLIENT and other
spans supply relationship evidence, never extra requests. Reads use `FINAL` and
one observation/ingestion cutoff. Repeating frozen input, windows and configuration
produces the same evaluations, candidate ordering and evidence; `observed_at`
naturally changes, and late ingestion can change a later evaluation.

## Rule defaults and configuration

The response includes `rule_version`, `config`, `observed_at`, `end`, `baseline`
and `current` (`{from,to}` windows), `operations`, `candidates`, and `caveats`.
The rule's configuration fields (also reflected in matching server environment
variables) are:

| JSON field | Environment variable | Default |
| --- | --- | --- |
| `min_samples` | `DETECTOR_MIN_SAMPLES` | 100 per window |
| `latency_ratio_milli` | `DETECTOR_LATENCY_RATIO_MILLI` | 2000 (2×) |
| `latency_delta_ns` | `DETECTOR_LATENCY_DELTA_NS` | `"100000000"` (100 ms) |
| `error_rate_bps` | `DETECTOR_ERROR_RATE_BPS` | 500 (5%) |
| `error_increase_bps` | `DETECTOR_ERROR_INCREASE_BPS` | 500 (5 percentage points) |

Invalid configuration fails startup. Minimum samples and latency delta are
positive; ratio milli is at least 1000; error thresholds are in 1..10000 bps.
Numeric minimum-sample and ratio fields are at most 9,007,199,254,740,991 so
JSON clients can represent their configured values exactly. Latency delta is a
canonical positive UInt64 decimal string (no sign or leading zeroes). The local
Compose file accepts these environment overrides; recreate the API container
after changing them. Controlled acceptance scenarios use the defaults above.
Duration values and signed duration changes are decimal nanosecond strings.
Rates and rate changes are JSON fractions, not percentages. Boundary comparisons
and ranking use exact arithmetic, not rounded display values.

Each window statistic contains `span_count`, `error_count`, `unset_count`,
`error_rate`, `unset_rate`, and `p95_duration_ns` (null when no samples).
Exact p95 uses the same discrete selection as ClickHouse `quantileExact(0.95)`:
sorted index `floor(0.95*n)`, capped at `n-1`, without interpolation.
Latency fires only when current p95 is at least the configured ratio times
baseline AND the absolute increase reaches the configured delta. A zero baseline
still requires the absolute increase; no division by zero occurs. Errors fire
only when current ERROR share and its increase both reach their thresholds.
UNSET is exposed and never labeled success. Both windows need minimum samples
before either rule can fire.

## Operation results and ranking

Each `operations` entry includes `service_namespace`, `service_name`, `operation`,
`state` (`normal`, `candidate`, `insufficient_evidence`), `baseline` and `current`
statistics, `p95_increase_ns` (null if a window has no samples),
`error_rate_increase` (null if a window has no samples), `triggered_rules`
(`latency`, `errors`), `evidence` and `caveats`. Both changes are returned even
when only one rule fires. Operations are ordered lexicographically by identity.
Groups missing from one window are present with a zero-count statistic.
An empty operation list means no observed SERVER groups, not healthy telemetry.

`candidates` contains the candidate operation entries sorted by ERROR-share
increase descending, p95 increase descending, namespace, service, operation.
Each candidate also has `service_rank`: first occurrence of a distinct
namespace/service assigns ranks 1,2,... . This groups operations for the
top-three-service acceptance check without discarding operation evidence.

At most five distinct supporting current-window trace IDs per candidate are
chosen stably by ERROR first, duration descending, start ascending, trace ID and
span ID. Each evidence entry contains `trace_id`, `span_id`, `start_time`,
`duration_ns`, `status_code`, `from`, `to`, `caveats`,
`downstream_anomalies` (namespace/service/operation identities), and
`error_propagation` (boolean). Evidence links open existing trace detail in the
returned full 35-minute input interval, so visible parents match the detector’s
observed context. Downstream co-occurrence is based on observed parent
chains in the same trace; error propagation means an ERROR descendant is seen
along an ERROR parent chain. Neither proves causation. Child durations are never
summed as elapsed time.

Missing parents/roots, cycles, source-reported drops (including event/link
attribute drops), no SERVER coverage and UNSET status yield explicit caveats
where relevant. Even an observed root does not establish complete delivery.

## Resource limits

Evaluation reads only the selected 35-minute interval and bounded normalized
span fields, with at most 100,000 deduplicated spans and 32 MiB of accounted input,
and at most 500 SERVER operation groups. Crossing any input/group limit fails
with 503 rather than ranking partial statistics. The existing 8 MiB response cap,
four concurrent database-read slots and ClickHouse profile limits apply.
Evaluation accounts for repeated operation and evidence JSON while building the
result, failing the entire evaluation before a response expansion exceeds its
budget. Cancellation checks cover evaluation as well as the database read. All
services are retained as relationship context even when the reported service is
filtered. These are correctness bounds, not a measured capacity claim.
