# IncidentLens architecture

Status: Phases 1–2 are implemented and verified: trace ingestion, bounded query APIs and the trace explorer. Detection remains proposed. See [ingestion contract](INGESTION.md), [ingestion verification](VERIFICATION.md), [query/explorer verification](VERIFICATION_PHASE2.md), and [supported Demo ADR](adr/0001-demo-trace-only-candidate.md).

## MVP architecture

```text
External OpenTelemetry Demo (supported service subset)
  -> OpenTelemetry Collector (traces pipeline)
  -> IncidentLens Go ingestion service (OTLP/gRPC)
  -> ClickHouse (span storage)
  <- Go query API + deterministic incident detector
  <- React + TypeScript web application (HTTP/JSON)
```

The implementation uses two Go executables in one Go module: ingestion and query API. The query API also serves the built React/TypeScript explorer. The proposed Phase 3 detector will be an internal package invoked by the query API, not a separate service. ClickHouse is the only product datastore. Docker Compose is the local orchestration tool; all product ports bind locally or remain on a private container network.

## Component responsibilities

| Component | Responsibility |
| --- | --- |
| External Demo | Generate realistic cross-service requests and controlled faults using existing upstream instrumentation. |
| Collector | Receive Demo traces, apply bounded batching and attribute filtering, and export OTLP/gRPC to IncidentLens with bounded retries. No metrics/logs pipelines. |
| Go ingestion | Implement OTLP TraceService Export; validate and normalize spans; enforce request/attribute limits; batch inserts; acknowledge completed ClickHouse writes; report protocol errors. |
| ClickHouse | Store spans with retention and support bounded trace retrieval and statistical queries. No application workflow state in the MVP. |
| Go query API | Validate filters, own SQL and query budgets, return stable JSON contracts, and reconstruct trace relationships. |
| Detector package | Evaluate versioned deterministic rules on deduplicated span data and return explained candidates with supporting trace IDs. |
| React/TypeScript UI | Present search, waterfalls, comparisons, and evidence. Own display state; consume the query API without direct database access or duplicated detection logic. |

## Telemetry flow and delivery semantics

1. The Demo exports traces to the Collector. Pin the Demo and Collector versions when implementing the integration; retain the upstream service identity and trace context.
2. The Collector splits requests into bounded batches and exports to the ingestion service using the standard OTLP/gRPC trace contract. Its bounded exporter queue waits for the downstream result and returns retryable overflow errors; see [ADR 0002](adr/0002-collector-acknowledgment.md). OTLP/HTTP and other signals are not initial ingestion endpoints.
3. Ingestion validates IDs, required service identity, timestamp ordering, and payload limits. Preserve resource and instrumentation-scope context when flattening the OTLP hierarchy into span rows. Reject invalid spans explicitly; do not silently invent timestamps or service names.
4. Ingestion writes bounded batches and acknowledges valid spans only after synchronous ClickHouse insert completion. No success response for data held only in ingestion memory. During overload or storage failure, return an appropriate retryable failure; queues and retries remain bounded.
5. Mixed valid/invalid requests use OTLP partial-success semantics only after valid spans have been stored, reporting the rejected count. Permanent request failures and transient failures follow the [OTLP specification](https://opentelemetry.io/docs/specs/otlp/). In particular, partial success must not be used to request a retry of temporary failures.
6. Retries may repeat already-written spans after an ambiguous response or partial write. Query-time deduplication prevents double counting. Collector memory queues can lose unacknowledged queued data on restart or retry exhaustion; there is no claim of durable end-to-end delivery. Persistent buffering requires a separately justified change.
7. Query requests read stored spans. A trace can arrive across multiple batches and appear incomplete; responses include the query's observation time and detected gaps. Refreshing may reveal later spans.

## Initial ClickHouse data model

Phase 1 defines one `spans` table in [migration 001](../migrations/clickhouse/001_spans.sql). The schema follows this design:

| Column(s) | Type | Meaning |
| --- | --- | --- |
| `trace_id`, `span_id` | `FixedString(32)`, `FixedString(16)` | Validated lowercase hexadecimal IDs; never numeric IDs. |
| `parent_span_id` | `String` | Hexadecimal parent ID; empty for an explicit root. |
| `service_name`, `service_namespace` | `LowCardinality(String)` | Resource service identity; namespace may be empty. |
| `span_name` | `String` | Operation name; do not assume bounded cardinality. |
| `span_kind`, `status_code` | `UInt8` | OTLP kind/status values; retain UNSET versus OK versus ERROR. |
| `status_message` | `String` | Bounded status description. |
| `start_time`, `end_time` | `DateTime64(9, 'UTC')` | Source event timestamps. |
| `duration_ns` | `UInt64` | Validated end minus start, not ingestion latency. |
| `ingested_at` | `DateTime64(9, 'UTC')` | Receipt/write metadata, not an event timestamp. |
| `resource_attributes`, `span_attributes`, `scope_attributes` | `String` | Bounded JSON encoding preserving OTLP value types and separate namespaces. |
| `scope_name`, `scope_version`, `resource_schema_url`, `scope_schema_url` | `String` | Instrumentation and schema context. |
| `events`, `links` | `String` | Bounded JSON preserving timestamps, IDs, and typed attributes. Span events are trace data, not a logs pipeline. |
| `trace_state`, `trace_flags` | `String`, `UInt32` | Propagation context. |
| `dropped_attributes_count`, `dropped_events_count`, `dropped_links_count` | `UInt32` | Source-reported span loss indicators. |
| `resource_dropped_attributes_count`, `scope_dropped_attributes_count` | `UInt32` | Source-reported resource and scope loss indicators. |

Storage choices implemented and tested for Phase 1:

- Engine: `ReplacingMergeTree` for immutable spans with identical retry payloads.
- Partition by `toDate(start_time)` in UTC. Order by `(service_namespace, service_name, start_time, trace_id, span_id)` for service/time queries.
- Logical identity is `(trace_id, span_id)` in the single Demo environment. Immutable service identity and start time are assumptions: identical retries must have the same sorting key and partition. Conflicting copies of a span are unsupported producer behavior and must not be silently treated as legitimate updates.
- ReplacingMergeTree replacement happens during merges; it does not ensure immediately unique reads. All MVP query paths, including aggregates and detector inputs, must deduplicate before counting, initially using `FINAL` under the immutable-key assumption. See [ClickHouse's engine documentation](https://clickhouse.com/docs/reference/engines/table-engines/mergetree-family/replacingmergetree). Test replay before merges; do not build aggregate materialized views that count raw duplicate inserts.
- Retention: seven days by event start time, with a query visibility cutoff and background TTL deletion. TTL is not immediate physical removal. Reject spans already outside retention and spans beyond a documented allowed future-clock tolerance.
- Trace-ID lookup may scan across services. Require a bounded time range and measure it before adding indexes, projections, summary tables, or changing sort order.
- No precomputed trace or incident tables initially. Reconstruct from spans and compute summaries on demand. The [ingestion contract](INGESTION.md) specifies bounded protobuf JSON, attribute allowlisting and rejection instead of truncation. Unit and real ClickHouse round-trip tests preserve nanosecond timestamps and typed context.

## API boundaries

The service-summary, trace-search and trace-detail routes are implemented in Phase 2, alongside ingestion and process health. See the [complete query contract](../api/README.md) for exact JSON shapes, nanosecond strings, filters, pagination and errors. Only the incidents route below remains proposed.

| Boundary | Contract |
| --- | --- |
| Collector → ingestion | OTLP/gRPC `opentelemetry.proto.collector.trace.v1.TraceService/Export`. Traces only. |
| Ingestion → storage | Parameterized, bounded batch inserts through the ClickHouse client; write credentials confined to ingestion. |
| UI → query API | Versioned HTTP/JSON `/api/v1`; read-only database credentials confined to query API. |
| `GET /api/v1/services` | Required `from`/`to`; service/operation summaries from deduplicated SERVER spans. |
| `GET /api/v1/traces` | Required `from`/`to`; optional service, operation, span-duration bounds, and span error status; bounded limit and cursor. Returns traces containing matching spans, with matching-span facts clearly distinguished from trace-wide facts. |
| `GET /api/v1/traces/{traceId}` | Required `from`/`to`; spans in that interval plus parent links, missing-parent indicators, and explicit truncation metadata. An empty result is 404; an oversized trace is never silently cut off. |
| `GET /api/v1/incidents` (Phase 3, unimplemented) | Explicit evaluation end time; optional service filter. Uses versioned window/threshold configuration and returns candidates, sample counts, baseline/current values, rule version, observation time, and evidence IDs. |
| Process health | Liveness and dependency readiness endpoints; no telemetry analytics pipeline is implied. |

Time windows are UTC half-open intervals `[from, to)` based on span start time. Initial maximum query range is 24 hours within retention; initial page limit is 100, maximum 500. Validate positive duration bounds, IDs, enum filters, and cursor/filter consistency. Use parameterized values and allowlisted sort fields, a documented query timeout, response size limits, and consistent JSON errors (400 invalid request, 404 absent trace, 503 unavailable dependency, 504 deadline exceeded).

Search sorts by earliest matching span start time ascending with trace ID as tie-breaker, returns each trace once, and carries an ingestion cutoff in the cursor. Explicit receipt-time `PREWHERE` filtering runs before `FINAL` to retain eligible pre-cutoff copies while they remain stored. Background replacement can discard older receipt metadata, so replay plus merging can change later pages. Late arrivals remain visible on a fresh search; cursor pagination is not a transactional database snapshot. Trace detail uses the selected search interval initially; offer a bounded interval expansion for missing parents. A missing parent or root can be detected; complete delivery cannot be proven from spans alone. Do not label a trace complete merely because it has a root.

## Deterministic detection proposal

- Evaluate SERVER spans per service namespace, service name, and operation. CLIENT spans remain available as dependency evidence but are not added to SERVER request denominators. Operations lacking SERVER spans remain explorable but are outside initial rule coverage.
- For evaluation end `T`, compare current `[T-5m, T)` with baseline `[T-35m, T-5m)`. UI defaults `T` to one minute before now to reduce late-arrival effects. This is a grace period, not a completeness guarantee.
- Initial configurable rule defaults: at least 100 spans in each window; latency fires when exact p95 is at least twice baseline and at least 100 ms greater; errors fire when ERROR-status proportion is at least 5% and rises by at least 5 percentage points. UNSET is not evidence of success; expose its share. These are hypotheses to validate on the Demo, not calibrated production thresholds.
- Candidate ordering is deterministic: error-rate increase descending, p95 increase descending, then service namespace/name and operation lexicographically. Group operation candidates by service for the top-three product check. Return both changes even when only one rule fires.
- Attach a bounded, stably ordered set of supporting current-window traces. Mark downstream co-occurring anomalies and error propagation as evidence; do not claim that the highest-ranked service is causally responsible. Concurrent child durations must not be naively summed to explain elapsed time.
- Insufficient samples return an explicit state. Missing relationships and source loss indicators produce evidence caveats. Identical frozen data, windows, and configuration must give identical output; late data can revise a subsequent evaluation.
- No background incident lifecycle, scheduler, notification system, learned model, or persistent detector state is required. An incident here is a window-specific investigation candidate.

## Architectural assumptions and open validation work

- One trusted local environment, synthetic traffic, and a single ClickHouse node with a persistent local volume. Acknowledgment does not imply replication or survival of disk failure.
- Full sampling for the supported demo scenarios, stable `service.name`, propagated IDs, reasonably synchronized clocks, and immutable completed spans. Validate these assumptions in phase 1 and surface violations rather than silently repairing them.
- The [upstream Demo supports Docker deployment and configuration overrides](https://opentelemetry.io/docs/demo/docker-deployment/), but its full service topology is not the MVP dependency list. Phase 1 must pin and validate a reduced, read-only shopping/browsing scenario without excluded technologies. Disable optional services/backends and metrics/logs export using upstream-supported configuration. If the desired scenario requires a forbidden dependency or source changes, record the incompatibility and narrow the scenario; do not silently relax scope or fork the Demo.
- Application stdout for troubleshooting and test-run measurements do not constitute a product logs/metrics ingestion feature. No separate signal backend or collection pipeline is introduced.
- Query bounds, retention, thresholds, payload caps, and hardware limits are planning defaults, not performance guarantees. Final payload/time limits and the Demo service subset remain phase 1 validation decisions.
- Meaningful changes to storage, delivery guarantees, component boundaries, rule semantics, or dependencies require an ADR explaining evidence, alternatives, and consequences.

## Repository structure and later additions

The tree below includes the full target layout. Phases 1–2 implement ingestion/storage, query/httpapi, both executables, the trace explorer, API documentation, local configuration, integration/browser tests and Demo references. Detector, incident UI and benchmarks remain unimplemented. Create later paths only when their phase is requested.

```text
IncidentLens/
├── AGENTS.md
├── README.md
├── docs/
│   ├── PRODUCT.md
│   ├── ARCHITECTURE.md
│   ├── MILESTONES.md
│   └── adr/                       # Numbered decisions with status/consequences
├── backend/                       # One Go module
│   ├── go.mod
│   ├── cmd/
│   │   ├── ingest/                 # Ingestion executable
│   │   └── api/                    # Query API executable
│   └── internal/
│       ├── config/
│       ├── trace/                  # Shared trace types and validation
│       ├── ingest/                 # OTLP handling and batching
│       ├── storage/clickhouse/     # Insert/query implementation
│       ├── query/                  # Search and trace reconstruction
│       ├── detector/               # Phase 3, unimplemented
│       └── httpapi/                # HTTP handlers and transport types
├── frontend/
│   ├── package.json
│   └── src/
│       ├── app/
│       ├── api/                    # Typed HTTP client
│       ├── components/             # Shared UI only when needed
│       └── features/
│           ├── traces/
│           └── incidents/         # Phase 3, unimplemented
├── api/                           # HTTP contract, added in phase 2
├── migrations/clickhouse/          # Ordered schema migrations
├── deploy/local/                  # Product Compose and Collector config
├── integrations/otel-demo/         # Version pin, overrides, scenario instructions
├── tests/
│   ├── fixtures/                  # Small synthetic OTLP datasets
│   ├── integration/               # Real ClickHouse/Collector checks
│   └── e2e/                       # Demo-to-UI workflows
├── benchmarks/                    # Phase 4 harness and measured reports
└── scripts/                       # Small repeatable dev/test helpers
```

Keep unit tests next to Go/TypeScript behavior. Phase 1 cross-component tests live at `backend/tests/integration` within the Go module so they can exercise its internal packages; Phase 2 adds `tests/e2e` for the external Demo-to-explorer workflow. Keep the upstream Demo checkout outside the product repository. Store only IncidentLens-owned integration configuration and the upstream revision here; product packages must never import Demo source. Do not add directories for deferred technologies until their phase is justified.
