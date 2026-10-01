# Opt-in streaming runbook

This is the local Phase 5 Kafka learning path. The default `deploy/local/compose.yaml` still sends traces directly to Go ingestion and acknowledges them after ClickHouse storage. The streaming overlay sends the same trace-only Collector pipeline to `stream-ingest`, then Kafka, `stream-worker`, and ClickHouse. It requires Docker Compose with `!override` support (v2.24 or newer). See [ADR 0004](../../docs/adr/0004-phase5-buffering-and-replay.md) and the [predeclared Phase 5 workload](../../benchmarks/WORKLOAD_PHASE5.md). These are operating instructions, not a runtime verification claim.

Run commands from the repository root. The overlay uses the official `apache/kafka:4.1.1` linux/amd64 image pinned to digest `sha256:0bc1bb2478f45b6cea78864df86acdc11e8df2c5172477819a4d12942cbe5d40` (Docker Hub tag and pulled image `RepoDigests`, 2026-09-29). It adds the `incidentlens_kafka-data` named volume mounted at `/var/lib/kafka/data`; both Kafka logs and KRaft metadata use that path. Keep this volume and the existing `incidentlens_clickhouse-data` volume during migration, recovery, and rollback.

```sh
docker compose -f deploy/local/compose.yaml -f deploy/streaming/compose.yaml config --services
docker compose -f deploy/local/compose.yaml stop collector ingest
docker compose -f deploy/local/compose.yaml -f deploy/streaming/compose.yaml up -d --build
docker compose -f deploy/local/compose.yaml -f deploy/streaming/compose.yaml ps
curl --fail http://127.0.0.1:18082/readyz
curl --fail http://127.0.0.1:18083/readyz
```

Stop the old direct `ingest` explicitly; the overlay's `direct` profile excludes it from the new service set but does not stop an already-running container. The first command should list ClickHouse, migration, API, Collector, Kafka, topic initialization, stream producer, and worker; it should omit direct ingest. The topic job creates `incidentlens-spans-v1` with one partition, RF1, one-hour or 1 GiB per-partition retention, and a 5 MiB broker/topic message ceiling for records capped at 4 MiB. It reapplies topic limits on later starts. Collector's host OTLP/gRPC port stays `127.0.0.1:4317`; direct `ingest` is on `14317` when enabled, streaming producer on `14318`, Kafka host listener on `19092`, and producer/worker health on `18082`/`18083`. Use the Compose service state and `/readyz` checks before sending input. Do not use `--remove-orphans` or `down -v` for a cutover.

An OTLP success from `stream-ingest` means Kafka accepted valid records, **not** that ClickHouse stored them or the UI can find them. The worker polls one broker batch without prefetch, with a 5 MiB decoded-batch cap and 8 MiB response cap; Kafka's first-batch exception lets valid records progress under one-byte requested fetch budgets. It writes a whole record synchronously, then commits its next offset. Broker admission, consumer offset progress, ClickHouse `FINAL` visibility, and finite identity reconciliation are separate observations. An ambiguous retry may store a duplicate physical row; deduplicated queries should still show one logical span. The Phase 5 workload records each boundary and its observation deadline.

For a controlled rollback, first stop new input while keeping the worker and Kafka running. Capture the retained topic end offset and committed group position, then wait for the worker's `CURRENT-OFFSET` to reach the captured `LOG-END-OFFSET` with zero lag. Use finite fixture IDs or the benchmark reconciliation ledger to verify every expected Kafka-acknowledged immutable span in ClickHouse `FINAL` reads before stopping the worker. A zero lag display alone is insufficient: it does not prove old offsets were retained, that a poison record was handled, or that storage identities match.

```sh
docker compose -f deploy/local/compose.yaml -f deploy/streaming/compose.yaml stop collector stream-ingest
docker compose -f deploy/local/compose.yaml -f deploy/streaming/compose.yaml exec kafka \
  /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic incidentlens-spans-v1 --time -1
docker compose -f deploy/local/compose.yaml -f deploy/streaming/compose.yaml exec kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --group incidentlens-stream-worker-v1 --describe
# Repeat the group inspection until the captured end offset is committed;
# reconcile expected immutable IDs against ClickHouse FINAL before continuing.
docker compose -f deploy/local/compose.yaml -f deploy/streaming/compose.yaml stop stream-worker kafka
docker compose -f deploy/local/compose.yaml up -d --build ingest collector
docker compose -f deploy/local/compose.yaml ps
```

If the broker is unavailable, the producer should return a bounded retryable failure, not a success. Restore Kafka with its existing volume and verify its health, producer readiness, consumer position, and finite identities; resend ambiguous caller input with the same IDs where appropriate. If the worker stops on an unsupported schema, malformed record, expired span, or missing retained committed offset, keep its offset in place and preserve the topic/volume and logs for diagnosis. Kafka records are immutable: an unsupported record cannot be repaired in place. Correct the producer/schema, restore validated data from an independently retained source into an explicitly selected replacement topic/group, and reconcile the affected identities under a documented recovery plan. Preserve the original topic and disclose any unrecoverable range. Do not reset the group to latest, skip a poison record, delete the volume, or report zero loss after a retention gap. A fresh replay group can only reread offsets still retained. Transient fetch/session errors retry automatically. A commit ownership error stops the worker after its storage write; restart the same group with `docker compose -f deploy/local/compose.yaml -f deploy/streaming/compose.yaml start stream-worker` and reconcile the resulting duplicate-safe replay.

This is one local KRaft broker, one partition, and one replica: there is no broker failover or disk-loss guarantee. Kafka has 2 CPUs, 1 GiB container memory, and a 512 MiB JVM heap; producer and worker each have 1 CPU/512 MiB. The topic's one-hour/1 GiB retention and 64 MiB/five-minute segment rolling are not a hard disk quota because deletion is asynchronous. The predeclared experiment stops if Kafka free space falls below 1 GiB, its actual volume exceeds 2 GiB, or disk observations fail; also retain the host's 10 GiB safety floor. Measure real lag and disk usage before retention expiry, since a consumer behind the earliest retained offset cannot recover from this broker alone.

Worker `/readyz` checks Kafka/topic and ClickHouse connectivity; it does not prove
successful inserts or advancing offsets. All ClickHouse write failures retry with
bounded backoff until shutdown, retaining the offset even for persistent schema
or permission errors. These retries currently emit no per-attempt diagnostics.
If lag fails to shrink despite healthy dependencies, inspect the ClickHouse schema,
writer permissions and service state, then reconcile finite identities after
repair. Do not infer successful delivery from a green readiness response. There
is no automatic stuck-consumer alerting in this local learning extension.
