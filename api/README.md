# Trace query API v1

Phase 2 contract. All routes are read-only, under `/api/v1`. The same Go server
serves the built explorer. Local HTTP port: 18081. No incident endpoint exists.

## Common rules

`from` and `to` are required RFC3339 timestamps with an explicit timezone;
responses use UTC RFC3339 with up to nine fractional digits. Windows are `[from,to)`
on **span start**, at most 24 hours, within seven-day retention. `to` may be up to
five minutes ahead of observation time (ingestion's clock tolerance). Retention
is enforced on every request. Durations are integer nanoseconds, serialized as
decimal **strings** to preserve precision in JavaScript. Counts are JSON numbers.

Search filters are exact, case-sensitive `service`, `namespace`, `operation`,
inclusive positive integer `min_duration_ns` / `max_duration_ns`, and `status`
(`UNSET`, `OK`, `ERROR`). All filters must match the **same span**. Omitted
namespace means any namespace; an explicitly empty namespace means the empty
namespace. `limit` defaults to 100, maximum 500. Unknown, repeated, malformed,
inconsistent parameters and invalid IDs return 400.

Queries have a five-second API context deadline and a seven-second HTTP write
timeout. At most four database reads run concurrently per API process; excess
work fails with 503 rather than entering an unbounded queue. The SELECT-only
ClickHouse user has a 256 MiB memory budget, 2,000,000-row / 256 MiB read budgets,
and two query threads. Exceeding read limits throws rather than returning partial
statistics. These fixed profile limits cannot be overridden. Its read-only mode
allows the driver's timeout setting: the pinned driver adds five seconds to the
remaining context deadline, bounded by a ten-second database fallback. See the
[driver implementation](https://github.com/ClickHouse/clickhouse-go/blob/v2.40.3/context.go)
and [settings constraints](https://clickhouse.com/docs/operations/settings/constraints-on-settings).
JSON responses are capped at 8 MiB. Detail
returns at most 2,048 spans, additionally bounded by the response byte cap.
Overlarge search/summary responses fail explicitly; detail reports truncation.
Detail stops accumulating span data near the byte limit and may return fewer
than 2,048 spans; the final JSON envelope always obeys the cap. API request headers
are limited to 8 KiB (subject to Go's HTTP parser buffering allowance).
Errors have shape `{"error":{"code":"invalid_request","message":"..."}}`.
HTTP codes: 400 invalid request/cursor, 404 absent trace/route, 405 wrong method,
503 dependency unavailable/query resource cap or capacity reached, 504 deadline.
`/livez` and dependency-aware `/readyz` are available.

## GET /services

Accepts `from`, `to`, optional `service`, `namespace`, `operation`, and `limit`.
Groups deduplicated SERVER spans by namespace/service/operation, sorted
lexicographically. Returns `observed_at`, `from`, `to`, `truncated` and `services`:

```json
{"service_namespace":"shop","service_name":"catalog","operation":"GetProduct","span_count":100,"error_count":5,"unset_count":20,"error_rate":0.05,"unset_rate":0.2,"p50_duration_ns":"1000000","p95_duration_ns":"9000000"}
```

Percentiles use ClickHouse `quantileExact`, after deduplication. No SERVER samples
means an empty array, not a healthy-service claim. UNSET is not evidence of success.
The summary cap is explicit via `truncated`; narrow the filters to see more groups.

## GET /traces

Accepts all search filters above plus opaque `cursor`. Returns `observed_at`,
`ingestion_cutoff`, `from`, `to`, `traces` and `next_cursor` (empty when exhausted).
One result per trace, sorted by **earliest matching span start ascending**, then
trace ID ascending. Each item:

```json
{"trace_id":"0123456789abcdef0123456789abcdef","matching_start_time":"2026-09-26T12:00:00Z","matching_span_count":2,"matching_min_duration_ns":"1000000","matching_max_duration_ns":"9000000","matching_error_count":1}
```

These are matching-span facts, not whole-trace elapsed time or counts. The cursor
binds the entire filter/window/limit set, ordering position, and initial ingestion
cutoff; mismatched or malformed cursors fail. Later pages exclude newly ingested
spans. Identical replay must not hide previously eligible spans while their copies
remain stored. Background replacement can discard older receipt metadata: pages
are not a transactional snapshot. Retention expiry invalidates an old window.
Refresh without a cursor establishes a new cutoff and reveals late arrivals.

## GET /traces/{traceId}

Accepts only `from`, `to`. Trace IDs are nonzero 32-character lowercase hex.
Returns `observed_at`, `from`, `to`, `trace_id`, `spans`, `truncated`,
`truncation_reason`, `missing_parent_ids`, `root_count`, `has_missing_root`,
`has_cycles`, `has_source_drops`, `observed_start_time`, `observed_end_time`,
`observed_elapsed_ns`. Elapsed covers only returned spans and is never a delivery
completeness claim. Empty result: 404. Spans sort by start time then span ID;
parent links are explicit and may refer to a parent outside the returned interval.
Missing relationships, cycles, source drops and truncation describe the returned
observation. A root alone never proves complete telemetry. Refresh to see late
spans, or expand the interval within the 24-hour/seven-day bounds.

Each span includes `span_id`, `parent_span_id`, `service_name`, `service_namespace`,
`span_name`, `span_kind` (OTLP numeric enum), `status_code` (OTLP numeric enum),
`status_message`, `start_time`, `end_time`, `duration_ns` (string), `ingested_at`,
`resource_attributes`, `span_attributes`, `scope_attributes`, `events`, `links`
(objects preserving the stored OTLP JSON wrappers), `scope_name`, `scope_version`,
`resource_schema_url`, `scope_schema_url`, `trace_state`, `trace_flags`,
`dropped_attributes_count`, `dropped_events_count`, `dropped_links_count`,
`resource_dropped_attributes_count`, `scope_dropped_attributes_count`.
