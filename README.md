# IncidentLens

IncidentLens is a trace-only investigation project for distributed applications. The planned workflow compares healthy and degraded requests and presents trace evidence for likely responsible services.

**Phase 1 is complete.** Trace-only Go ingestion, ClickHouse storage, and the bounded Collector pipeline have passed unit and real-dependency tests, including replay, outage, overflow, and restart checks. The supported external Demo browsing scenario produces a stored three-service trace. No query API, explorer UI, detector, or performance claims exist yet.

## Local prerequisites

- Docker Engine with Docker Compose v2.39.4 or newer, running Linux containers.
- Go 1.26.8 for host tests. The image build uses that same version.
- Available local ports 4317, 14317, 18080, 18123, and 19000.
- Enough Docker memory for the product container caps (2 GiB ClickHouse, 512 MiB ingestion, 256 MiB Collector), plus Docker overhead and the optional Demo.

If Docker Desktop is running on macOS but `docker` is not on PATH, run `export PATH="/Applications/Docker.app/Contents/Resources/bin:$PATH"` in the terminal used for these commands.

Pinned runtime versions are ClickHouse 25.8.4.13 and Collector Contrib 0.137.0. These versions passed the documented local runtime checks on Linux/amd64 under Docker Desktop. No excluded runtime technologies are part of the product stack.

## Run the product pipeline

From the repository root:

```sh
docker compose -f deploy/local/compose.yaml config --quiet
docker compose -f deploy/local/compose.yaml up --build -d
curl --fail http://127.0.0.1:18080/readyz
```

Compose waits for ClickHouse and runs the ordered SQL migrations before ingestion starts. Migration 001 uses `CREATE IF NOT EXISTS`, so repeating it is safe for its unchanged schema:

```sh
docker compose -f deploy/local/compose.yaml run --rm migrate
```

Apply future schema changes as new numbered migrations; `IF NOT EXISTS` does not repair schema drift. The volume retains ClickHouse data across container restarts and ordinary `down`.

OTLP/gRPC traces enter the Collector on port 4317. Direct ingestion is at port 14317 for tests; it acknowledges only completed database writes. `/livez` and `/readyz` are on port 18080. ClickHouse ports are exposed only on loopback for local verification. The synthetic local passwords in Compose are not production credentials. Ingestion uses an INSERT-only database user; admin access is limited to migration and local verification commands.

## Test and inspect

```sh
./scripts/test.sh
cd backend
INCIDENTLENS_INTEGRATION=1 INCIDENTLENS_RESTART_TESTS=1 INCIDENTLENS_FAILURE_TESTS=1 \
  go test -race ./... -v -count=1
cd ..
./scripts/verify-storage.sh
```

Ordinary Go tests **skip** real-dependency tests unless enabled. Enabled tests fail when dependencies are unavailable. The restart/failure options stop or restart the local services and force-kill the Collector; run them serially without concurrent Demo requests. They retain the data volume and restore services. See [test details](backend/tests/integration/README.md). Synthetic test fixtures do not substitute for the [supported external Demo scenario](integrations/otel-demo/README.md), which has its own build and acceptance requirements.

Storage inspection uses `FINAL` and a bounded time interval. A raw table count can include retries. To inspect parent relationships, use a trace ID from `verify-storage.sh`:

```sh
docker compose -f deploy/local/compose.yaml exec -T clickhouse \
  clickhouse-client --password local-admin --query \
  "SELECT trace_id, span_id, parent_span_id, service_name, start_time, end_time FROM incidentlens.spans FINAL WHERE start_time >= now64(9) - INTERVAL 1 HOUR AND start_time < now64(9) AND trace_id = 'REPLACE_WITH_TRACE_ID' ORDER BY start_time, span_id LIMIT 2048 SETTINGS max_execution_time=5"
```

This manual inspection is capped at 2,048 rows; check the trace's span count before treating it as the entire observed result. A root does not prove complete delivery. Ingestion and the Collector have bounded memory; queued/unacknowledged data may be lost on restart or retry exhaustion. Identical span replay is supported; conflicting copies are not supported producer behavior.

```sh
docker compose -f deploy/local/compose.yaml logs --tail=100 ingest collector
docker compose -f deploy/local/compose.yaml down
```

Do not add `--volumes` unless you intend to delete retained local telemetry.

- [Product scope](docs/PRODUCT.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Exact ingestion limits and delivery semantics](docs/INGESTION.md)
- [Milestones](docs/MILESTONES.md)
- [Verification results and outstanding gates](docs/VERIFICATION.md)
