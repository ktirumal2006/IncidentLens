# Phase 3 controlled Demo scenario

This plan was written before the full scenario run. It uses the three pinned
upstream images and a run-unique `service.namespace`; no Demo source or image is
changed. The supported recommendations route remains the healthy route. The
catalog's native unknown-product response supplies the error fault through the
upstream frontend product route. The latency fault temporarily limits the
frontend container to 0.1 CPU with Docker; recreating it from the same pinned
Compose configuration removes the limit. See the Phase 3 ADR for the scenario
expansion and tradeoffs.

The runner sends 128 requests per phase with four concurrent clients and an
injected trace ID per request. It requires at least 120 expected HTTP responses
and at least 100 unique frontend SERVER spans for every five-minute current
window. Catalog `GetProduct` must likewise have at least 100 SERVER spans for
the error phase. It waits at least five minutes after the previous traffic ends
before starting the next phase. An explicit evaluation end is recorded after
the phase's spans have had time to export. Actual source span timestamps are
used; no timestamps are shifted and detector defaults are unchanged.

Expected outcomes, fixed before execution:

| Phase | Traffic/fault | Expected detector result |
| --- | --- | --- |
| Healthy baseline | Recommendations, normal CPU | Supplies baseline samples; no evaluation yet. |
| Healthy control | Recommendations, normal CPU | No candidates. |
| Latency fault | Recommendations, frontend at 0.1 CPU | Frontend `HTTP GET` fires latency and ranks in the top three distinct services. All requests should return 200. |
| Latency recovery | Recommendations, restored frontend, full clean current window | No candidates. |
| Error fault | Unknown product through frontend; catalog native NotFound | Catalog `GetProduct` fires errors and ranks in the top three distinct services. Requests should return HTTP 500. |
| Error recovery | Recommendations, full clean current window | No candidates. |

Each evaluation compares current `[T-5m,T)` with baseline `[T-35m,T-5m)`,
requires 100 SERVER spans per window, 2× and +100 ms p95 for latency, and 5%
ERROR share with +5 percentage points for errors. The error route produces a
catalog SERVER ERROR without contacting the excluded feature service because
only product `OLJCESPC7Z` enters that check. It has a two-service request path;
the healthy recommendation path still spans all three services.

Run after both Compose stacks are healthy:

```sh
python3 integrations/otel-demo/phase3_scenario.py run
# This step can wait for the Phase 3 API build/deployment:
python3 integrations/otel-demo/phase3_scenario.py evaluate integrations/otel-demo/evidence/phase3-<run-id>.json
```

The runner prints each phase and writes raw timestamps, response counts, trace
IDs, and real ClickHouse window statistics to its JSON evidence file. Its
`evaluate` mode stores API responses and asserts the predeclared outcomes. A
failed phase remains a failure in the evidence; do not tune detector thresholds
or hide it. The complete run takes about 25 minutes because the clean windows
are real time. Avoid concurrent Demo traffic and product restart tests. If a
run is interrupted, check `docker inspect` for CPU quota and restore the pinned
services with `docker compose -f integrations/otel-demo/compose.yaml up -d
--force-recreate`.

Startup checks require both product readiness endpoints, both Demo gRPC
listeners, and the frontend TCP listener. Docker commands have a 30-second
deadline; dependency failures and evaluation assertions are persisted in the
evidence. Interruption is recorded separately from a failed assertion.

The initial September 27 run was deliberately interrupted after its healthy
baseline when the task was paused. The September 28 00:45 UTC attempt produced
128 HTTP 200 responses but blocked while querying an unhealthy ClickHouse;
both product readiness endpoints returned 503 and Docker health checks failed.
It was deliberately stopped for runtime recovery before any fault phase. These
attempts are incomplete observations, not passing acceptance or detector failures.

The isolated CPU pilot before this full plan found frontend HTTP GET SERVER p95
of 162,033,920 ns at normal CPU (30/30 HTTP 200), and 2,292,310,528 ns at
0.1 CPU (30/30 HTTP 200), each at ten concurrent clients. These are pilot
measurements, not full scenario results or performance claims. A separate 0.01
CPU recommendationservice pilot caused client timeouts and was rejected. Both
services were recreated from the pinned base Compose and their CPU limits were
verified to be unset before the full run.

## Recorded acceptance run

The completed September 28, 2026 run used namespace
`incidentlens-phase3-20260928T005118Z-836fef`. All six bursts returned 128/128
expected responses (HTTP 200 for recommendations, HTTP 500 for unknown product).
The [raw evidence](evidence/phase3-20260928T005118Z-836fef.json) contains actual
traffic times, known trace IDs, ClickHouse statistics and API responses. Final
API evaluation and browser checks passed against the deployed Phase 3 build.

| Evaluation end T (UTC) | Phase | Observed result |
| --- | --- | --- |
| 00:56:47.960243 | Healthy control | No candidates; both sample minimums met. |
| 01:02:10.714595 | Latency | Frontend rank 1; exact p95 48,524,288 → 1,294,631,680 ns, baseline 256/current 128 SERVER spans. |
| 01:07:26.249516 | Latency recovery | No candidates. |
| 01:12:28.997205 | Error | Catalog GetProduct rank 1; ERROR 0/2,048 baseline → 128/128 current. Frontend propagated errors rank 2. |
| 01:17:32.261452 | Error recovery | No candidates. |

The latency quota was removed at 01:02:12.422769 UTC; the recovery current
window began at 01:02:26.249516, after removal. Error requests ended at
01:12:18.992570; the error-recovery current window began at 01:12:32.261452.
Both were full clean five-minute windows. Cleanup recreated the original
three-service Compose configuration and verified frontend NanoCpus = 0.

Traffic was burst-based rather than continuous coverage of all 30 baseline
minutes. Healthy and latency SERVER spans were UNSET, which the API and UI
exposed explicitly. The observations validate these controlled local scenarios,
not production calibration, complete telemetry delivery, causal attribution or
capacity. The browser check opened each target's known trace evidence by
keyboard and showed thresholds, baseline/current samples, uncertainty and the
waterfall at desktop and mobile sizes without page errors or horizontal overflow.
