# Phase 2 Demo-to-explorer check

This check sends one request to the supported, pinned external Demo route, then
looks up the resulting trace through the public query API. It uses an injected
W3C `traceparent` to identify the request exactly. The injected parent span is
external to IncidentLens, so the trace detail must report that parent as missing;
other missing parents fail the check. The trace must contain exactly the
frontend, recommendationservice, and productcatalogservice set with the
expected cross-service links, without ERROR spans, source drops, cycles, or
truncation. The browser test searches for that same trace and opens its
multi-service waterfall with the keyboard.

Start both Compose stacks as documented in the root and Demo READMEs. Build the
frontend and deploy the query API at `http://127.0.0.1:18081`. Then run:

```sh
python3 tests/e2e/demo_trace.py
cd tests/e2e
npm ci
npx playwright install chromium
npm test
```

`INCIDENTLENS_API_URL`, `INCIDENTLENS_UI_URL`, and `INCIDENTLENS_DEMO_URL` can
override the local defaults. The first command needs only Python 3's standard
library. The browser check uses the pinned Playwright package and its Chromium
binary. Both commands generate a fresh request and print the observed trace ID,
service set, span count, and correlation method. They do not clear local data.

The check covers the supported recommendations route only. A missing injected
external parent is expected; it does not imply a broken internal service link.
The test does not claim that a root proves complete delivery or that all Demo
traffic is visible.

After the full Phase 3 scenario passes API evaluation, its frozen windows can
also be exercised through the incidents UI without generating new traffic:

```sh
cd tests/e2e
npm run test:incidents -- ../../integrations/otel-demo/evidence/phase3-<run-id>.json
```

This checks healthy/recovery no-candidate states, the declared latency and error
targets, visible baseline/current statistics and rule thresholds, uncertainty,
and keyboard navigation from each candidate's known Demo trace evidence to the
waterfall. The UI uses UTC seconds; the test verifies the same sample window
against the API after dropping the recorded end's fractional seconds.

For the Phase 5 hardening status acceptance, start a dedicated candidate stack
with the four-partition topic, built UI and API, and no other load. Run
`node tests/e2e/streaming_status.mjs` with absolute
`INCIDENTLENS_STREAM_TEST_COMPOSE_FILES` (colon-separated base, streaming and
acceptance overlays), `BENCH_BINARY`, and `STREAM_STATUS_EVIDENCE_DIR`, plus
`STREAM_TEST_PROJECT` beginning `incidentlens-retro-hardening-`. Optional
`INCIDENTLENS_API_URL` and `INCIDENTLENS_UI_URL` override port 18081.
The script requires the existing Playwright installation and Docker CLI. It
retains both finite harness runs, raw status observations, screenshots, command
results and pass/fail JSON in the evidence directory. It pauses ClickHouse to
observe real backlog, pending age and retry during a 600 spans/s, six-second
3,600-span offered load, then verifies recovery. The prime load offers 288
spans. It pauses
the worker for a real unavailable state. The stale screenshot uses a browser
route that freezes a previously successful real status response; this checks
UI stale rendering only. The script unpauses services on failure and does not
start or remove the Compose stack.
