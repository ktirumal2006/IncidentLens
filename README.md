# IncidentLens

IncidentLens is a trace-only investigation project for distributed applications. It compares service-operation windows and presents ranked investigation candidates with supporting trace evidence and explicit uncertainty.

**Phases 1–4 are complete and verified.** The trace-only pipeline has a bounded HTTP query API and a React/TypeScript explorer for service summaries, trace search, and parent-linked waterfalls. The deterministic detector and incident investigation view pass the controlled Demo fault/recovery scenarios and full acceptance checks. See the [Phase 3 verification record](docs/VERIFICATION_PHASE3.md). The [Phase 4 measurements](docs/VERIFICATION_PHASE4.md) report the tested throughput limit, query failures, loss accounting and decision to defer Phase 5.

## Local prerequisites

- Docker Engine with Docker Compose v2.39.4 or newer, running Linux containers.
- Go 1.26.8 for host tests. The image build uses that same version.
- Node.js 22.12+ and npm for host frontend tests (the container builds the UI with Node 22).
- Available local ports 4317, 14317, 18080, 18081, 18123, and 19000.
- Enough Docker memory for the product container caps (2 GiB ClickHouse, 512 MiB each ingestion and API, 256 MiB Collector), plus Docker overhead and the optional Demo.

If Docker Desktop is running on macOS but `docker` is not on PATH, run `export PATH="/Applications/Docker.app/Contents/Resources/bin:$PATH"` in the terminal used for these commands.

Pinned runtime versions are ClickHouse 25.8.4.13 and Collector Contrib 0.137.0. These versions passed the documented local runtime checks on Linux/amd64 under Docker Desktop. No excluded runtime technologies are part of the product stack.

## Run the product pipeline

From the repository root:

```sh
docker compose -f deploy/local/compose.yaml config --quiet
docker compose -f deploy/local/compose.yaml up --build -d
curl --fail http://127.0.0.1:18080/readyz
curl --fail http://127.0.0.1:18081/readyz
```

Compose waits for ClickHouse and runs the ordered SQL migrations before ingestion starts. Migration 001 uses `CREATE IF NOT EXISTS`, so repeating it is safe for its unchanged schema:

```sh
docker compose -f deploy/local/compose.yaml run --rm migrate
```

Apply future schema changes as new numbered migrations; `IF NOT EXISTS` does not repair schema drift. The volume retains ClickHouse data across container restarts and ordinary `down`.

Open the explorer at **http://127.0.0.1:18081**. Select a UTC interval (up to 24 hours within the last seven days), optionally filter service, operation, matching-span duration or status, then search. Select a trace to inspect its waterfall and span details. Search results describe matching spans; detail elapsed time describes the returned observation. Missing relationships and truncation are visible. Refresh reveals late arrivals; a root is never proof of complete delivery. The supported [external Demo scenario](integrations/otel-demo/README.md) generates real three-service traces.

Select **Incidents** to compare current `[end-5m,end)` with baseline `[end-35m,end-5m)`. The UTC evaluation end initially defaults to one minute before now. Optional exact service/namespace filters narrow the reported operations while retaining cross-service relationship context. The view shows sample counts, p95, ERROR and UNSET shares, rule thresholds and version, service ranks, caveats, and keyboard-accessible evidence traces. Refreshing the same end can reveal late data; identical frozen input and configuration repeat deterministically. A normal result means neither rule fired with enough samples, not proof of health. Low volume or absent SERVER coverage produces insufficient evidence.

Rule `trace-v1` defaults require at least 100 SERVER spans in each window. Latency requires both 2× baseline p95 and a 100 ms increase; errors require both a 5% ERROR share and a five-percentage-point increase. UNSET is not success. The five `DETECTOR_*` startup settings and exact units are documented in the [incident API contract](api/INCIDENTS.md); invalid configuration fails startup. Evaluation is capped at 100,000 deduplicated spans, 32 MiB of accounted input, 500 SERVER operation groups, and an 8 MiB response, with explicit failures rather than partial rankings. These are correctness bounds, not measured capacity. The [predeclared Demo fault/recovery procedure](integrations/otel-demo/PHASE3_SCENARIO.md) uses unchanged pinned images and real source timestamps; its healthy, fault and clean-recovery checks passed.

OTLP/gRPC traces enter the Collector on port 4317. Direct ingestion is at port 14317 for tests; it acknowledges only completed database writes. `/livez` and `/readyz` are on ports 18080 (ingestion) and 18081 (API). ClickHouse ports are exposed only on loopback for local verification. The synthetic local passwords in Compose are not production credentials. Ingestion uses an INSERT-only database user; the API uses a SELECT-only user with server-enforced query budgets. Admin access is limited to migration and local verification commands.

## Test and inspect

```sh
npm --prefix frontend ci
./scripts/test.sh
cd backend
INCIDENTLENS_INTEGRATION=1 INCIDENTLENS_QUERY_INTEGRATION=1 \
  INCIDENTLENS_DETECTOR_INTEGRATION=1 \
  INCIDENTLENS_RESTART_TESTS=1 INCIDENTLENS_FAILURE_TESTS=1 \
  go test -race ./... -v -count=1
cd ..
./scripts/verify-storage.sh
```

The script runs race-enabled Go tests, vet, executable builds, frontend behavior tests, TypeScript checking and the production UI build. See [trace query contracts](api/README.md) and the [incident contract](api/INCIDENTS.md) for exact units, filtering, rules, caps and errors, and [browser end-to-end tests](tests/e2e/README.md) for live Demo workflows. Commands above are verification instructions; Phase 3 passing evidence is recorded in the verification report.

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
- [Query API contract](api/README.md)
- [Incident evaluation contract](api/INCIDENTS.md)
- [Milestones](docs/MILESTONES.md)
- [Verification results and outstanding gates](docs/VERIFICATION.md)
- [Phase 2 acceptance evidence](docs/VERIFICATION_PHASE2.md)
- [Phase 3 acceptance evidence](docs/VERIFICATION_PHASE3.md)
