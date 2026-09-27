# Phase 2 verification record

All Phase 2 acceptance criteria passed on September 27, 2026 (America/New_York).
This is a correctness record, not a benchmark or capacity claim. Phase 3 remains
unimplemented.

## Implementation and reproducibility

Phase 2 adds a Go query executable, SELECT-only ClickHouse access, the three
documented HTTP query routes, and a React/TypeScript explorer served by the same
API process. Storage schema and the trace-only ingestion path are unchanged.
No new runtime service technology or later-milestone component was introduced.
Vite builds static assets; Vitest/Testing Library and Playwright are development
test tools. Their purpose and simpler alternatives are described in the
[frontend README](../frontend/README.md) and [E2E procedure](../tests/e2e/README.md).

The environment is macOS x86_64, Go 1.26.8, Node 22.23.3, Docker Desktop 4.92.0,
ClickHouse 25.8.4.13 and Collector Contrib 0.137.0. The frontend builder's Node
image is digest-pinned in the Dockerfile. The external Demo images and source
reference remain pinned as in Phase 1. Existing local volumes were retained.

From the repository root, with Go, Node, npm, Python 3 and Docker on PATH:

```sh
docker compose -f deploy/local/compose.yaml up --build -d
docker compose -f integrations/otel-demo/compose.yaml up -d
npm --prefix frontend ci
./scripts/test.sh
cd backend
INCIDENTLENS_INTEGRATION=1 INCIDENTLENS_QUERY_INTEGRATION=1 \
  INCIDENTLENS_RESTART_TESTS=1 INCIDENTLENS_FAILURE_TESTS=1 \
  go test -race ./... -v -count=1
cd ..
python3 tests/e2e/demo_trace.py
cd tests/e2e
npm ci
npx playwright install chromium
npm test
```

Run restart/failure/merge-control tests serially. Do not generate Demo requests
during that suite. The query integration gate targets the deployed API, so rebuild
it after backend changes. Disabled integration gates are skips, not passing
dependency verification.

## Executed results

- `./scripts/test.sh` passed: race-enabled Go unit tests, `go vet ./...`, both
  executable builds, all 14 frontend tests, TypeScript checking and the production
  UI build. [Raw output](evidence/phase2-unit-tests.txt). The dependency audit
  reported [zero known vulnerabilities](evidence/phase2-frontend-audit.txt).
- The full `go test -race ./... -v -count=1` command above passed with all four
  dependency/failure gates enabled and no skipped tests. [Raw output](evidence/phase2-full-go-tests.txt).
  This includes all eight query integration tests, ingestion regression tests,
  actual storage outages, process restarts and Collector overflow/recovery.
  The finite queue fixture again reconciled all 16,384 unique spans.
- Product and Demo Compose validation and the production image builds passed.
  The final [API/UI build](evidence/phase2-final-api-build.txt) is deployed locally
  at `http://127.0.0.1:18081`; the retained ClickHouse volume is healthy.
- The final [Chromium E2E run](evidence/phase2-browser-e2e.txt) passed against the
  deployed UI. It submitted search and opened the exact injected trace ID by
  keyboard, then verified all three services in the waterfall.
- A real ClickHouse stop produced API liveness 200, readiness 503 and query 503;
  restarting storage restored readiness to 200. [Outage evidence](evidence/phase2-api-outage.txt).
- Desktop (1440×1000) and mobile (390×844) rendering were visually inspected.
  Service names remain readable, span details expose full identities, and mobile
  had no horizontal overflow or browser page errors. [Check output](evidence/phase2-visual-check.txt),
  [desktop screenshot](evidence/phase2-explorer-desktop.png),
  [mobile screenshot](evidence/phase2-explorer-mobile.png).

The final Demo trace is `b37cf89014eef911c8f627a333181e61`: 14 observed spans
across frontend, recommendationservice and productcatalogservice, with the
expected cross-service parent edges and no ERROR status or source-reported drops.
The only missing parent is the intentionally external injected parent. The UI
correctly shows no explicit root. [Raw API detail](evidence/phase2-demo-detail.json)
preserves the source timestamps and typed context; this observation does not
prove complete delivery for arbitrary traffic.

## Acceptance evidence

| Milestone 2 requirement | Evidence |
| --- | --- |
| Service summaries, search and detail contracts; UTC units, filters, cursors, timeouts, caps and errors | [API contract](../api/README.md), HTTP validation/error tests, deployment configuration and read-only/budget checks. |
| Exact time/service/operation/duration/ERROR filters; duplicate-safe counts and percentiles | Real ClickHouse fixtures check same-span predicates, half-open nanosecond boundaries, inclusive duration bounds, namespaces, SERVER-only summaries and asymmetric replay before merges. Twenty original samples yield p50 1,100 ns and p95 2,000 ns after shorter samples are replayed. |
| Frozen pagination, late arrival refresh, matching versus elapsed duration | Real query tests check tied start times, trace-ID ordering, multiple matching spans per trace, cursor consistency, late writes, fresh searches and pre-cutoff replay. Frontend tests check cached back navigation, refresh and failed-page retry. Search and detail use separately labeled duration facts. |
| Multi-service waterfall, parents, details, status; incomplete/out-of-order/truncated data | Query tests cover children stored before parents, missing roots/parents, cycles, precise typed context, span cap and byte cap. Frontend tests cover nanosecond ordering/position, span inspection, ERROR labels, gaps, source drops and interval expansion. Live Demo/browser evidence is recorded below. |
| Empty/loading/failure/insufficient-data states and keyboard navigation | Frontend behavior tests plus the real browser test. No SERVER samples are labeled insufficient data; empty results do not imply health. |
| Known external Demo ingestion → search → detail; real SQL and user-visible tests | The E2E test injects a known W3C trace ID into the pinned upstream recommendations route, finds that ID through the public API and opens its multi-service waterfall in Chromium. |

## Runtime issues resolved

- The pre-existing Docker VM stopped executing container health checks. Restarting
  Docker Desktop and the retained product/Demo containers restored the runtime.
- The first query profile rejected the driver's automatically transmitted timeout
  setting, so valid queries returned 503. The final SELECT-only user permits
  bounded timeout settings while keeping memory/read/thread limits immutable.
  The API deadline is five seconds; the driver adds a database cancellation grace
  period bounded by a ten-second fallback. [Permission and limit evidence](evidence/phase2-query-permissions.txt)
  verifies failed INSERT, rejected limit overrides and a real read-budget error.
- The Demo honors the injected traceparent. Its external test parent is not
  ingested, so missing-root/parent metadata is expected. The E2E assertion now
  requires that specific missing parent and rejects missing internal links.
- Review corrected sub-millisecond waterfall positioning, first-page cutoff
  preservation, deterministic failed-page retry, source-drop integer overflow,
  nested event/link attribute-loss detection, detail metadata recomputation and
  interval expansion at the 24-hour cap. Visual review also corrected service
  names hidden by long namespaces on narrow screens.

## Remaining limitations

This remains a trusted local, single-environment trace explorer. The supported
historical Demo scenario and Phase 1 delivery/immutable-span assumptions still
apply. No authentication, detection, incident UI or performance envelope is claimed.

Cursor pagination is not a transactional snapshot: late spans appear on refresh,
and background replacement can discard older receipt metadata after replay.
Retention expiry also invalidates older windows. Summary groups and trace detail
are explicitly capped; narrow the selected filters/window when capped. Detail
elapsed time covers returned spans only. Missing links, source-drop indicators,
and even an observed root cannot prove complete telemetry delivery.
