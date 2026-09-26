# Phase 1 integration checks

These tests use the real ClickHouse native driver and the local Compose stack.
They never clear existing tables or volumes. Every fixture has a random trace ID;
fixture rows expire under the normal seven-day retention policy.

Start the stack and apply migrations using the repository's local run instructions,
then run from `backend/`:

```sh
INCIDENTLENS_INTEGRATION=1 go test ./tests/integration -v -count=1
```

Defaults are ClickHouse `127.0.0.1:19000` (database `incidentlens`, user `default`,
password `local-admin`) and Collector OTLP/gRPC `127.0.0.1:4317`.
`CLICKHOUSE_TEST_ADDRESS` and `COLLECTOR_TEST_ADDRESS` override these endpoints.
An enabled test fails if a required dependency is unavailable; ordinary `go test
./...` skips these dependency checks.

Coverage includes all stored columns, typed OTLP values, events, links, source
loss counts, mixed valid/invalid batches, real connection failure, and replay after
a simulated lost acknowledgment following an actual successful insert. The replay
test temporarily stops merges on the local `incidentlens.spans` table and restores
them afterward, proving raw duplicates exist while `FINAL` returns one logical
span. Run against the dedicated local stack, and do not run this test concurrently
with other merge-control tests.

The Collector test sends a synthetic three-service trace and checks stored service
identities and parent links. It verifies the product pipeline; it does **not**
substitute for the external OpenTelemetry Demo browsing acceptance check.

To additionally restart the local ingestion and ClickHouse containers after a
successfully acknowledged direct OTLP export:

```sh
INCIDENTLENS_INTEGRATION=1 INCIDENTLENS_RESTART_TESTS=1 \
  go test ./tests/integration -v -count=1
```

The restart check uses the repository's `deploy/local/compose.yaml`, the default
local endpoints, and the retained ClickHouse volume. It verifies the acknowledged
fixture is still present without exporting it again. It does not prove durability
against disk loss, nor preservation of unacknowledged Collector memory queues.
The configured Collector waits for downstream export results. Its volatile queue
can still lose unacknowledged requests on a crash; callers must retry ambiguous
results. Acknowledgments do not guarantee complete upstream telemetry.

The additional failure suite deliberately stops ClickHouse/ingestion, force-kills
the Collector, and saturates its 32-request queue with a finite synthetic fixture:

```sh
INCIDENTLENS_INTEGRATION=1 INCIDENTLENS_RESTART_TESTS=1 INCIDENTLENS_FAILURE_TESTS=1 \
  go test -race ./... -v -count=1
```

Run this serially against the dedicated local stack, without concurrent Demo
requests. Cleanup restarts affected services and retains volumes. Tests require
retryable failure during storage outage and queue overflow, check unacknowledged
input loss across a forced Collector restart, and reconcile retried identities
against deduplicated storage. These are correctness tests, not load benchmarks.
See the root verification record for actual execution results.
