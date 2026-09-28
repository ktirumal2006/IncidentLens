# Phase 3 verification record

All Phase 3 acceptance criteria passed on September 28, 2026 (America/New_York).
This is a correctness record, not a performance or capacity claim.

## Scope and reproducibility

Phase 3 adds the internal deterministic detector, `GET /api/v1/incidents`, and
an incident investigation view that opens existing trace detail. The rule version
is `trace-v1`; the [API contract](../api/INCIDENTS.md) defines the exact windows,
SERVER denominator, thresholds, ranking, evidence and resource bounds. No schema,
background service, persistent incident lifecycle or later milestone is added.

The controlled Demo scenario uses unchanged pinned external images, real source
timestamps, the default sample minimums and fixed 30-minute/five-minute windows.
The [predeclared plan](../integrations/otel-demo/PHASE3_SCENARIO.md) and
[ADR 0003](adr/0003-demo-detection-faults.md) describe the native catalog error
route and reversible frontend CPU restriction. Each run uses its own namespace.
Run restart/failure tests serially after scenario traffic has finished.

## Reproduce the checks

With the local product and pinned Demo stacks running, from the repository root:

```sh
./scripts/test.sh
python3 integrations/otel-demo/phase3_scenario.py run
python3 integrations/otel-demo/phase3_scenario.py evaluate integrations/otel-demo/evidence/phase3-<run-id>.json
cd tests/e2e
npm ci
npx playwright install chromium
npm run test:incidents -- ../../integrations/otel-demo/evidence/phase3-<run-id>.json
cd ../../backend
INCIDENTLENS_INTEGRATION=1 INCIDENTLENS_QUERY_INTEGRATION=1 \
  INCIDENTLENS_DETECTOR_INTEGRATION=1 INCIDENTLENS_RESTART_TESTS=1 \
  INCIDENTLENS_FAILURE_TESTS=1 go test -race ./... -v -count=1
```

The scenario takes about 26 minutes for actual clean five-minute gaps. Finish it
and its browser checks before running the serial restart/failure suite. All
checks used the existing Go 1.26.8, Node 22.23.3, ClickHouse 25.8.4.13 and
Collector 0.137.0 toolchain on macOS/Docker Desktop. No new runtime dependency,
schema migration, upstream Demo source edit or Phase 4 benchmark was introduced.

## Acceptance audit

| Phase 3 requirement | Required evidence | Status |
| --- | --- | --- |
| Versioned windows, SERVER denominator, exact percentile, samples, thresholds, ranking, bounded evidence | `TestHealthyLatencyErrorsAndRecovery`, exact boundary/zero-baseline/config tests, `TestDetectorClickHouseHalfOpenWindows`, p95 parity and HTTP validation/cap tests | Passed |
| Healthy/latency/errors/boundaries/zero baseline/low volume/UNSET/duplicates/downstream/missing relationship fixtures | Detector fixture suite plus real separate-write replay: 221 raw parent rows yield 100 baseline + 100 current SERVER spans; p95 195/495 ms matches ClickHouse | Passed |
| Frozen determinism including ties/evidence; separate late-data behavior | `TestInputPermutationCandidateTiesAndServiceRanks`, `TestEvidenceAllOrderingFields`, frozen repeated real reads, and receipt-cutoff 100→101 late-refresh test | Passed |
| Explained UI and navigable trace evidence, honest uncertainty | 23 frontend tests, five-window Chromium test, keyboard evidence opening and inspected desktop/mobile screenshots | Passed |
| Predeclared healthy and single-service fault Demo scenarios | Predeclared run below: healthy zero candidates; frontend latency and catalog errors each service rank 1 | Passed |
| Fault removal and full clean current-window recovery; limitations/failures retained | Recorded CPU restoration/request end precede recovery current-window starts; both recoveries have zero candidates; failed/interrupted attempts retained | Passed |

## Executed checks and controlled observations

The race-enabled unit suite, Go vet/build, 23 frontend behavior tests, TypeScript
checking and production UI build passed. [Raw local test output](evidence/phase3-unit-tests.txt).
Ordinary unit runs skip real dependencies. The separate [full race-enabled Go
suite](evidence/phase3-full-go-tests.txt) passed with all five integration,
query, detector, restart and failure gates enabled and no skipped tests. It
verified real outages/restarts and reconciled all 16,384 queue-fixture identities.
The final [API/UI image build](evidence/phase3-final-api-build.txt) is deployed
locally; the data volume was retained.

The six-phase run [raw evidence](../integrations/otel-demo/evidence/phase3-20260928T005118Z-836fef.json)
used namespace `incidentlens-phase3-20260928T005118Z-836fef`, 128 requests per phase
and four concurrent clients. Every phase returned all 128 expected HTTP statuses:
200 for recommendations and 500 for the deliberate unknown-product error. These
are finite correctness observations, not throughput or latency benchmarks.

| Evaluation end (UTC, 2026-09-28) | Phase | Result |
| --- | --- | --- |
| 00:56:47.960243 | Healthy control | Zero candidates. |
| 01:02:10.714595 | Frontend CPU latency | Frontend rank 1; SERVER p95 48,524,288 → 1,294,631,680 ns; baseline 256/current 128 spans; latency rule only. |
| 01:07:26.249516 | Latency recovery | Zero candidates; current window begins after CPU restoration at 01:02:12.422769. |
| 01:12:28.997205 | Native catalog error | Catalog GetProduct rank 1; ERROR 0/2,048 → 128/128; frontend propagated error rank 2. |
| 01:17:32.261452 | Error recovery | Zero candidates after a complete current window without invalid-product requests. |

The baseline includes actual traffic bursts, not continuous load throughout the
30 minutes. All evaluated target windows exceed the default 100 SERVER minimum.
Healthy and latency samples have UNSET status; the UI exposes this instead of
calling them successful spans. The test injects an external parent that is not
ingested, so missing-root/parent caveats are expected. Cleanup restored the base
Demo namespace and verified frontend NanoCpus=0.

[API assertions](evidence/phase3-demo-api-evaluation.txt) passed against all five
evaluation windows with all five default configuration values checked. The
[Chromium incident E2E](evidence/phase3-incidents-browser.txt) passed the same five
windows, found both injected services at rank 1, and opened known emitted Demo
trace IDs by keyboard. It checked the displayed hypotheses/thresholds and found
no browser page errors or horizontal overflow. Both browser suites were rerun
after the final API build and regression outages; the existing [Demo-to-explorer
workflow](evidence/phase3-explorer-regression.txt) also passed with a newly injected
three-service trace.

Desktop (1440×1000) and mobile (390×844) candidate and waterfall screenshots were
visually inspected: comparison values, caveats, evidence controls, service labels
and focus remain readable. [Desktop candidate](evidence/phase3-latency_fault-candidate-desktop.png),
[mobile candidate](evidence/phase3-latency_fault-candidate-mobile.png),
[desktop waterfall](evidence/phase3-latency_fault-waterfall-desktop.png),
[mobile waterfall](evidence/phase3-latency_fault-waterfall-mobile.png).

## Known limitations

Candidates are investigation hypotheses. Source loss, missing relationships,
UNSET status, sampling, late arrivals and shared downstream failures limit
interpretation. The baseline can contain a prior fault; the rules are not a
production-calibrated detector or proof of root cause. Child durations are not
summed as elapsed time.

Identical frozen data, windows and configuration repeat deterministically.
Observation time changes on each request; late data, retention and background
replacement of duplicate receipt metadata can change a later read. The
observation cutoff is not a persistent transactional snapshot.

Input, group, query and response caps fail explicitly instead of producing a
partial ranking. These are local correctness bounds, not measured capacity.
The historical Demo and existing immutable-span and volatile Collector queue
assumptions continue to apply.

## Interrupted work and pilot evidence

The earlier `phase3-20260927T200304Z-6421b8` run was interrupted after its healthy
baseline when work switched to the separate commit/push request. It did not run
a fault or satisfy scenario acceptance. Its raw record is retained; the runner
restored the original Demo namespace and verified zero frontend CPU quota.
The ADR and scenario plan separately disclose the latency pilots, including the
recommendationservice quota that caused request timeouts. Pilot output does not
substitute for the full default-threshold acceptance scenario.

A fresh run `phase3-20260928T004544Z-7173ee` returned 128 healthy HTTP responses
but could not verify storage: ClickHouse readiness was 503 and its container
health checks repeatedly timed out starting. A direct container restart failed
with “tried to kill container, but did not receive an exit event.” The runner was
interrupted before any fault and restored Demo configuration. Docker Desktop
recovery was required; retained data was not intentionally cleared. This run is
not acceptance evidence, and its raw record is preserved.
