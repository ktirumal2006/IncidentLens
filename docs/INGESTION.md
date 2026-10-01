# Phase 1 ingestion contract

Status: Phase 1 implementation and acceptance checks passed. See [verification](VERIFICATION.md).

The only telemetry endpoint is OTLP TraceService Export over gRPC, accepting identity or gzip compression with the same decompressed request limit. No HTTP telemetry endpoint or metrics/logs service is registered. Health HTTP endpoints are `/livez` and `/readyz`; readiness checks ClickHouse connectivity, not schema integrity. Compose waits for the schema migration before starting ingestion.

## Limits and protocol outcomes

These are fixed implementation limits, specified before boundary validation:

| Resource | Limit / behavior |
| --- | --- |
| Decompressed gRPC request | 4 MiB; larger messages: ResourceExhausted, permanent |
| Spans per Export | 2,048; excess rejects whole request |
| Serialized span plus resource/scope context and schema URLs | 64 KiB; excess rejects that span |
| Total normalized Export budget | 8 MiB, measured as string lengths plus 512 bytes per row; excess rejects the whole request before writing |
| Attributes in each resource, scope, span, event, link, nested map | 128 before filtering; duplicates/nil values rejected |
| Array items / value nesting | 128 / 8 |
| String or bytes value, names, schema URLs, status text | 4,096 bytes |
| Service name / namespace | 256 bytes; nonempty service.name required |
| Events / links per span | 128 each |
| Retention | Start time at least receipt time minus 7 days |
| Future clock tolerance | Start and end at most receipt time plus 5 minutes |
| Time validity | Nonzero, representable nanoseconds, end >= start |
| Insert batch | At most 256 spans; sequential batches within one Export |
| Ingestion concurrency / waiting queue | Four active Exports, zero waiting slots; excess: Unavailable |
| Export storage deadline | Five seconds shared by all batches |
| ClickHouse connection pool | Four connections, two-second dial, five-second read timeout |
| Collector batch splitting | 256 spans maximum; minimum 1 span avoids combining requests; one-second flush bound |
| Collector exporter memory queue | 32 requests, two consumers, waits for export result; overflow returns retryable failure; no disk queue |
| Collector retry budget | 30 seconds; interval 1–5 seconds; six-second attempt timeout |
| Collector memory | 192 MiB limiter with 32 MiB spike allowance; 256 MiB container cap |

Empty requests succeed. Invalid per-span IDs, times, enums, identity, attributes, events or links are counted as rejections. Valid spans are written first; only then is partial success returned with the rejected span count and a bounded generic message. All-invalid requests report all spans rejected. A storage error or overload returns Unavailable without partial success; previously written batches may be replayed. Unsupported signals return Unimplemented. No success acknowledges only volatile ingestion memory. See the [OTLP protocol rules](https://opentelemetry.io/docs/specs/otlp/).

## Data representation and filtering

IDs are lowercase hex strings; zero/incorrect-width trace and span IDs are invalid. Empty parent ID denotes an explicit root. All timestamps preserve nanoseconds. Unknown span kinds/status codes are rejected. UNSET remains distinct from OK and ERROR. Resource/scope schema URLs, instrumentation identity, trace flags/state and source-reported drop counts are retained.

Attribute columns contain protobuf JSON Resource wrappers (`attributes`), preserving AnyValue type tags, decimal-string int64 and base64 bytes. Events and links use protobuf JSON Span wrappers (`events` / `links`). Consumers should decode these using OTLP protobuf JSON rather than untyped floating-point JSON conversions. Bounds apply before filtering; overflow rejects the span instead of truncating it.

The ingestion allowlist retains service.name/namespace/version, deployment.environment.name, http.request.method, http.response.status_code, rpc.system/service/method, server.address/port, error.type, and `test.*` fixture attributes. Nested maps use the same allowlist. Other keys are removed from all resource/scope/span/event/link namespaces; source drop counters retain their original meaning and do not include local filtering. Collector additionally filters resource and span attributes before export. This integration accepts synthetic data only: free-text names/status/event text and allowlisted values are bounded, not a general-purpose secret scrubber. Do not send real credentials or personal data, including in `test.*` values.

## Delivery and retention

Synchronous native-protocol inserts use `async_insert=0`. An acknowledged insert resides on the single ClickHouse node; it is not a replication or disk-failure guarantee. There is no ingestion-owned retry queue. The Collector waits for downstream export results using its bounded queue; overflow returns retryable failure. Unacknowledged queued data can be lost on Collector restart or retry exhaustion. Caller deadline expiry may leave an ambiguous result requiring replay. The Collector does not propagate downstream partial-rejection counts as an end-to-end receipt; valid finite fixtures must still be reconciled against storage. See [ADR 0002](adr/0002-collector-acknowledgment.md). An ingestion crash after database commit and before response creates an ambiguous write; replay is expected.

ReplacingMergeTree deduplicates physical rows during merges. Verification reads use `FINAL` immediately, before merges, and apply a seven-day event-time visibility cutoff. TTL rounds deletion eligibility upward by one second so it cannot physically delete a row before the exact nanosecond visibility cutoff; deletion then runs asynchronously. Future query APIs must apply the exact visibility cutoff independently of TTL.

Immutable completed spans and stable service/start-time keys are required. Conflicting copies are unsupported; `FINAL` cannot deduplicate copies whose sorting keys differ. The verification script reports conflicting identities visible in raw storage. It cannot recover conflicts already discarded by merges. Do not interpret them as legitimate updates.


## Optional streaming contract

The [streaming overlay](../deploy/streaming/README.md) uses a separate
`stream-ingest` OTLP/gRPC executable. The existing direct endpoint and its
storage-ACK contract above are unchanged. Collector still waits for downstream
results, with the same partial-rejection caveat.

In this mode a successful valid export means all its records were accepted by
Kafka with all-ISR acknowledgment. It does **not** mean ClickHouse visibility.
Normalization uses the same trace policy, four concurrent export slots and a
4 MiB gRPC request limit. Before publishing anything, the producer encodes every
chunk: schema version 1, at most 256 normalized rows and 4 MiB JSON per record.
An oversized encoded chunk returns ResourceExhausted without publishing any
chunk. Invalid spans are permanently rejected; mixed exports return partial
success after valid records are acknowledged. A publish failure returns retryable
Unavailable; earlier chunks may already exist and caller replay is expected.
Publishing has a finite five-second deadline and bounded client buffers.

`stream-worker` polls one broker batch without prefetch, caps decoded batch bytes
at 5 MiB and response bytes at 8 MiB, and fails closed on oversized decompression.
Its one-byte fetch budgets use Kafka's first-batch progress exception; valid
4 MiB records still progress. It processes partition offsets sequentially, writes a full record
synchronously, then commits its next offset. A crash between write and commit
replays the same normalized payload, including ingestion timestamp. Existing
immutable identities and FINAL query/detector reads prevent logical inflation.
Caller OTLP retries are normalized anew, as on the direct path. No cross-system
transaction or exactly-once guarantee is claimed.

The one-partition RF1 topic retains one hour or 1 GiB, subject to asynchronous
segment deletion. Fresh replay groups begin at the earliest retained offset;
this is not recovery of already deleted data. Unsupported schema, malformed or
expired rows and a committed offset older than retained data halt consumption
without advancing past failure. The local broker volume must survive restarts;
there is no disk-loss, power-loss, replication or HA guarantee. The API/UI always
reflect stored observations, potentially incomplete while the consumer lags.
See [ADR 0004](adr/0004-phase5-buffering-and-replay.md) for bounds and
[the runbook](../deploy/streaming/README.md) for safe drain and rollback steps.
