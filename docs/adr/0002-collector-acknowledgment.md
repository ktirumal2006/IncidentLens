# ADR 0002: wait for downstream results at the Collector

Status: accepted; runtime overflow, outage and restart checks passed on 2026-09-25. See [verification](../VERIFICATION.md).

## Context

The initial Collector used an asynchronous batch processor ahead of a bounded exporter queue. That processor can acknowledge receiver requests before the exporter queue accepts them. On saturation, a request already acknowledged upstream can be discarded. Although the original design disclosed volatile buffering, this behavior conflicts with Phase 1's explicit requirement that queue exhaustion return retryable failure rather than false success.

## Alternatives

1. Keep asynchronous batching and document acknowledged loss: does not satisfy the acceptance criterion.
2. Disable all queues/retries: simpler, but removes the bounded short-outage buffer already in scope.
3. Use Collector 0.137.0's result-waiting bounded exporter queue, without the asynchronous processor: retains bounded buffering and backpressure with the existing component.
4. Add durable infrastructure: outside Phase 1 and unnecessary for this problem.

## Decision

Use option 3. Configure `sending_queue.wait_for_result: true` and `block_on_overflow: false`; keep 32 requests and two consumers. Move span-count splitting into the exporter queue with minimum batch size 1 and maximum 256 spans. Each nonempty request immediately qualifies for export, so small requests are not combined into potentially oversized payloads. A one-second flush bound remains configured. No asynchronous batch processor precedes the queue.

The pinned [exporter helper configuration](https://github.com/open-telemetry/opentelemetry-collector/blob/v0.137.0/exporter/exporterhelper/README.md) and [queue implementation configuration](https://github.com/open-telemetry/opentelemetry-collector/blob/v0.137.0/exporter/exporterhelper/internal/queuebatch/config.go) expose these settings. Ingestion still batches synchronous inserts in groups of at most 256.

## Consequences

Collector calls wait through downstream export/retry rather than acknowledging queue admission. Overflow returns a retryable error. Clients can hit their own deadline before the 30-second Collector retry budget and must treat that as an ambiguous result. A crash can discard unacknowledged in-memory requests; retries remain necessary and duplicate-safe reads remain mandatory. A successful Collector export of valid input follows downstream storage acknowledgment, but downstream OTLP partial rejection is not a per-span end-to-end delivery guarantee. No durable queue, exactly-once claim or disk-failure guarantee is introduced.

This may reduce batching efficiency for small requests. Performance is a later measured gate; preserving correct acknowledgment behavior takes priority. Real overflow, outage recovery, restart loss, and finite-fixture reconciliation tests are required before Phase 1 completes.
