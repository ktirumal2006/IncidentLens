# Phase 5 hardening cutover and rollback procedure

Candidate 3 failed the October 9 [promotion decision](PHASE5_PROMOTION_DECISION.md).
This proposed rehearsal was not executed; no default promotion or cutover occurred.

**Status: proposed, unverified.** Run this only after the correctness and paired-performance gates in [ADR 0005](adr/0005-hardened-streaming-default.md) and [the declared workload](../benchmarks/WORKLOAD_STREAM_HARDENING.md) pass. This document is an acceptance procedure, not evidence that promotion or migration has happened. Use an isolated, finite fixture for rehearsal; keep the existing named volumes when operating the retained local stack.

## Compose models and retained state

The current direct reference is `deploy/local/compose.yaml`. Adding `deploy/streaming/compose.yaml` selects the historical one-partition v1 producer and worker; adding `deploy/streaming/hardening.yaml` last selects the four-partition v2 topic, new group and API progress endpoint. Overlay order matters: the streaming overlay uses `!override` to replace the Collector mount and dependency, and the hardening overlay replaces the topic job. The benchmark drivers use these explicit file lists and must retain them for frozen comparisons.

After promotion, a *proposed* tracked root `compose.yaml` can make v2 the visible default using **one** long-syntax include whose `path` is the ordered list below. The root file has not been added. Three separate include entries would conflict instead of applying the overlays.

```yaml
include:
  - path:
      - deploy/local/compose.yaml
      - deploy/streaming/compose.yaml
      - deploy/streaming/hardening.yaml
```

Before using that entry point, compare its `docker compose config` output with the explicit three-file model. Confirm the Collector mounts `deploy/streaming/collector.yaml` and exports to `stream-ingest:4318`; `ingest` is excluded by its `direct` profile; v2 has four partitions, group `incidentlens-stream-worker-v2`, and the API has `INGESTION_MODE=streaming` with the internal worker status URL. Confirm project/network/volume names remain `incidentlens`, `incidentlens-telemetry`, `incidentlens_clickhouse-data`, and `incidentlens_kafka-data`. An explicit `-f deploy/local/compose.yaml` remains the direct reference. Do not use `down -v`, `--remove-orphans`, topic deletion, group reset, or v1 repartitioning at any transition.

For the commands below, run from the repository root. The existing v1 and v2 models are:

```sh
docker compose -f deploy/local/compose.yaml -f deploy/streaming/compose.yaml config --services
docker compose -f deploy/local/compose.yaml -f deploy/streaming/compose.yaml -f deploy/streaming/hardening.yaml config --services
```

## Audit and drain the original v1 log

Stop external Demo/fixture traffic, then stop the Collector. Let any already dispatched Collector requests finish or fail visibly before stopping `stream-ingest`; an in-memory Collector queue is not a durable ledger. Keep Kafka, ClickHouse, and the v1 worker running. Stop any old direct `ingest` container explicitly; a Compose profile does not stop a container already running from an earlier configuration.

```sh
docker compose -f deploy/local/compose.yaml -f deploy/streaming/compose.yaml stop collector
docker compose -f deploy/local/compose.yaml -f deploy/streaming/compose.yaml stop stream-ingest
docker compose -f deploy/local/compose.yaml -f deploy/streaming/compose.yaml exec -T kafka \
  /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic incidentlens-spans-v1 --time -2
docker compose -f deploy/local/compose.yaml -f deploy/streaming/compose.yaml exec -T kafka \
  /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic incidentlens-spans-v1 --time -1
docker compose -f deploy/local/compose.yaml -f deploy/streaming/compose.yaml exec -T kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --group incidentlens-stream-worker-v1 --describe
```

Save the original partition-0 log start, captured end, and committed offset. Repeat the group inspection until `CURRENT-OFFSET` equals the **captured** end; also require the committed offset to be at least the retained log start. If the group has no committed offset, a poison record, or a retained-offset gap, stop the cutover and preserve the topic, volume, and logs. Do not interpret a current zero-lag display as proof that an earlier lost range was delivered. The global v1 log may include old traffic without a complete producer identity ledger; disclose that interval as unverified even if the current commit reaches the end.

For v1 records with a finite ACK ledger, reconcile every expected immutable `(trace_id, span_id)` against `incidentlens.spans FINAL` before proceeding. Existing benchmark `summary.json` and `reconciliation.jsonl` files record planned, emitted, acknowledged, missing and unexpected identities; use their namespace and exact identity set. The SQL pattern in `scripts/verify-storage.sh` and the prior retained-volume example in `docs/evidence/phase5/rollback-verification.json` show the duplicate-safe query and offset evidence. A raw table count is insufficient. Retain the ledger, CLI outputs, worker logs and a record of any identities that cannot be verified.

The v1 worker can read v1 because `KAFKA_TOPIC` and `KAFKA_GROUP_ID` are set by the historical overlay, and its `/readyz` checks the expected one-partition topic. Its `/internal/stream-status` sampler expects **four** partitions, so use the Kafka offset commands and identity ledger—not that status endpoint—to decide whether v1 has drained.

## Isolated finite v1 → v2 → direct → v2 rehearsal

The unverified `tests/e2e/streaming_cutover.py` first requires absent project
containers and volumes, resolves the proposed root model, and checks exact
equality with the explicit v2 model. It starts that resolved default on isolated
ports, reconciles a finite 600-span fixture and four-partition drain, then uses
the same volumes for the v1 → v2 → direct → v2 sequence below. Each later
transition reconciles every earlier fixture, including the clean-start fixture.
This preparation is not a passing startup or cutover result; execute and retain
its evidence after the performance gates pass.

Use a dedicated Compose project and fresh named volumes for the acceptance rehearsal; keep the retained global volumes untouched. Record the project name, volume IDs, resolved Compose configuration, image IDs, UTC times, and a new seed/output directory for every fixture. The repository's `backend/cmd/bench` finite generator can send through `127.0.0.1:4317` and writes `events.jsonl`, `summary.json`, and `reconciliation.jsonl`; the prior 120-span direct smoke in `docs/evidence/phase5/rollback-verification.json` is a command/ledger example. Do not reuse its seed or output directory. An isolated v1 run should first demonstrate broker ACK, v1 committed end, and exact `FINAL` identities. Stop new input and drain v1 as above before switching that same project to v2; do not switch topic while v1 producer requests remain in flight.

Start the three-file v2 model (or the proposed root default only after its resolved configuration matches), retaining the same rehearsal Kafka and ClickHouse volumes. The v2 topic job must create or verify `incidentlens-spans-v2` with exactly four partitions and RF1 without altering v1. Verify the new group is `incidentlens-stream-worker-v2`; never reuse the v1 offsets. `/readyz` checks dependencies, not completed delivery. A pristine partition with no commit can be displayed as `empty_partition_uninitialized`; it is a caveat, not evidence of consumer progress.

Send a bounded fixture whose trace IDs route across all four partitions. Record each OTLP result, the expected immutable identity set, v2 log starts/ends, and the four group commits. After stopping new input, require each commit to reach its captured partition end and remain at or above its log start. Require all broker-ACKed identities in ClickHouse `FINAL`, zero duplicate logical identities in the reconciliation ledger, and a successful API trace-detail read of a known multi-service trace. Verify the browser/API progress response reports four-partition coverage and an observation time; a zero aggregate lag alone does not prove telemetry completeness. A `backend/cmd/bench-lag` observer with `--partitions 4` can retain timestamped offset samples; its aggregate zero is valid only when each sampled partition has initialized, consistent offsets. Keep v1 topic/group data in the same Kafka volume for later bounded replay.

For rollback, stop external input and the v2 Collector, settle in-flight requests, then stop v2 `stream-ingest`. Capture **all four** v2 end offsets and corresponding log starts. Keep the worker running until each committed offset reaches the captured end; reconcile the v2 ACK ledger against `FINAL` before stopping the worker and Kafka. Preserve both named volumes. Start the explicit direct model from `deploy/local/compose.yaml`, checking that its Collector mounts `deploy/local/collector.yaml` and exports to `ingest:4317`, the direct ingestion/API `/readyz` endpoints respond, and a fresh finite direct fixture reconciles exactly. Previously stored v2 trace IDs must remain readable after the switch. The historical rollback evidence demonstrates the same retention and direct smoke checks, but is not evidence for this new rehearsal.

Finally quiesce direct input, settle its finite ledger, stop direct `collector` and `ingest`, and return to the three-file v2 model with the **same** rehearsal volumes and v2 group. Verify the v2 captured committed offsets did not reset, its old fixture IDs remain queryable, and a new finite v2 fixture again reaches committed ends and exact `FINAL` reconciliation. Stop the direct ingestion container before restarting the streaming Collector; never run both ingestion paths as an accidental duplicate source.

## Failure and acceptance record

The rehearsal passes only with saved resolved configurations, retained-volume identities, original and final topic shapes, per-partition start/end/commit tables at each transition, producer ACK/finite identity ledgers, `FINAL` and API checks, and a direct and v2 return smoke. Record timeout and failed checks rather than moving the boundary. If a broker, worker, or storage failure leaves a poison record, retained-offset gap, uncommitted range or missing acknowledged identity, stop new input, preserve both volumes and service logs, and report the exact unresolved range/identities. Restore a validated source or replay only still-retained offsets under a documented repair; never skip to latest or claim zero loss from a green readiness or lag display.

This local single-broker procedure cannot recover data after Kafka volume loss or retention expiry. It does not prove delivery of historical v1 traffic lacking a complete identity ledger, and no step here establishes a performance improvement; that evidence comes from the frozen paired campaign.
