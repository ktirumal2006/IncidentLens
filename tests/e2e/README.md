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
