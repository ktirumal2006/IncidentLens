# Phase 1 verification record

Phase 1 passed on September 25, 2026 (America/New_York; the final Demo request is September 26 in UTC). Phase 2 verification is recorded separately in [its acceptance record](VERIFICATION_PHASE2.md). This records correctness and compatibility, not benchmark or capacity claims.

## Environment and reproducible commands

macOS x86_64; Docker Desktop 4.92.0, Engine 29.8.0, Compose 5.5.1; Docker reported 16 CPUs and 8,320,962,560 bytes of VM memory. Go 1.26.8 was downloaded to `/private/tmp/incidentlens-toolchain` and verified against the published SHA-256. Product pins: ClickHouse 25.8.4.13, Collector Contrib 0.137.0. Demo release and immutable image digests are in [its integration configuration](../integrations/otel-demo/compose.yaml).

The initial product startup created a new ClickHouse volume, ran migration 001 and built ingestion. Subsequent tests reused that volume, including process restarts and repeated migration runs. No existing volume was deleted. `backend/` is restored and all documented paths agree.

From `backend/`, with Go and Docker on PATH:

```sh
INCIDENTLENS_INTEGRATION=1 INCIDENTLENS_RESTART_TESTS=1 INCIDENTLENS_FAILURE_TESTS=1 \
  go test -race ./... -count=1 -v
go vet ./...
go build -o /tmp/incidentlens-ingest ./cmd/ingest
```

The complete final test suite, vet and build passed. All nine real-dependency tests ran; none was skipped. [Raw test output](evidence/phase1-tests.txt) includes the actual finite-fixture accounting. The queue test offered 16,384 spans: 8,192 were initially acknowledged, 8,192 were rejected with retryable failures, and retry produced exactly 16,384 unique stored spans. This is a bounded correctness fixture, not a measured throughput claim.

## Acceptance verification

| Phase 1 criterion | Verified evidence |
| --- | --- |
| Compatible pins and three-service external Demo | Published digest-pinned Demo images ran the documented recommendations request; HTTP 200, four products, and one trace across all three intended services. See ADR 0001 and trace evidence below. |
| No excluded technologies or metrics/logs export/pipelines | Resolved product Compose includes only ClickHouse, migration, ingestion and Collector; Demo includes only frontend, recommendation and catalog. Source/image review found trace-only exporter setup for the selected historical services; exporter selectors disable optional automatic export. Pinned Collector validation passes with only a traces pipeline. The successful request's recent output has no telemetry export failures or feature-service calls. |
| Schema, repeatable migration, exact limits | Fresh-volume migration succeeded; subsequent migration runs succeeded unchanged. `SHOW GRANTS FOR ingest` returned only INSERT on `incidentlens.spans`; selecting table data as that user failed with ACCESS_DENIED as expected. Limits are documented in INGESTION.md. |
| Faithful fixture round trip | `TestFullContextRoundTrip` verified every stored column, exact nanosecond times, typed attributes, scope/resource/schema context, events, links, flags and all source drop counts against real ClickHouse. |
| Invalid/oversized/mixed requests and unsupported signals | Unit and actual gRPC tests passed, including decompressed gzip limits, boundary timestamps, IDs, identity, attribute limits, mixed partial success and metrics/logs Unimplemented. Real mixed-batch test stored only valid spans. |
| Replay and ambiguous-write retry before merges | Test stopped merges, completed a real insert but simulated lost acknowledgment, retried, and asserted two raw rows but one `FINAL` row. Merges were restored. |
| Outage, overload, restart behavior | Real ClickHouse outage returned Unavailable without acknowledgment; retries recovered one logical identity. Previously acknowledged data survived ingestion/ClickHouse restart. Collector waited through a short outage, returned retryable queue overflow, lost only unacknowledged queued input on forced termination, and recovered it after caller replay. |
| Local run and real Demo retrieval | Documented product Compose setup, migrations, health checks and Demo scenario ran successfully. The bounded `FINAL` inspection below verified a real cross-service parent chain. Full relevant suite passed. |

Additional checks passed: native `/livez` 200, dependency-unavailable `/readyz` 503, clean SIGTERM exit; Go vet/build; product and Demo Compose validation; final Collector image `validate --config=/etc/otelcol/config.yaml`; shell syntax and local documentation-link checks. The documented storage-inspection script ran successfully and reported no conflicting copies in its one-hour window. Product and Demo are left running; ClickHouse is healthy.

## Actual Demo evidence

The final documented recommendations request returned HTTP 200 and four products, excluding `OLJCESPC7Z`. Trace `72ee02ce89d6e3361bc88dc1903e2c04` contains 16 unique spans:

- Services: frontend, recommendationservice and productcatalogservice.
- One observed root, all 15 observed child parent IDs found in the same result.
- Root → frontend recommendation client → recommendation server → product-list work → catalog client → catalog server.
- Four frontend product clients link to four catalog server spans; two connection spans occur on this first request after startup. An earlier warm request contained 14 spans.
- Each stored duration exactly equals end minus start using integer nanoseconds; timestamps retain sub-millisecond precision.
- Zero ERROR-status spans and zero source-reported drop counts; status UNSET is preserved and is not interpreted as proof of success.
- No feature-service span. The observed graph does not prove that arbitrary upstream telemetry is complete.

[Raw span evidence](evidence/demo-trace.jsonl) was retrieved with `FINAL`, a one-hour event-time window, a 2,048-row cap and a five-second query limit. An integer-preserving Python check verified identities, parents, durations, services and drop counts. Source inspection, exact closure and runtime configuration are documented in the [Demo integration](../integrations/otel-demo/README.md).

## Failures found and corrected

- Before Docker setup, the enabled storage test failed with connection refused. This was an environment failure, not a passing test.
- Initial Collector delivery failed because ingestion lacked gzip registration. Production gzip support and compressed/decompressed-boundary tests fixed it; final delivery passed.
- The initial asynchronous Collector batch processor could acknowledge before queue acceptance. ADR 0002 moves splitting into a bounded result-waiting exporter queue; the real overflow/restart tests now verify its acknowledgment boundary.
- The historical Demo source build failed when unpinned protobuf tooling required a newer Go compiler. The supported run uses unchanged upstream release images pinned by digest; no upstream service source was modified.
- The recommendation image initially failed on old generated protobuf descriptors. Its supported Python protobuf implementation setting resolved startup. The initial DNS/export errors in [first-start output](../integrations/otel-demo/evidence/demo-startup.txt) precede the fixes and are retained as failure evidence, not successful-run evidence.

## Remaining limitations

Only the documented local synthetic browsing scenario is supported. The historical Demo images have obsolete dependencies and their source correspondence is upstream release attribution, not an independently attested build. No full storefront, fault-detection scenario or production deployment is claimed.

Collector buffering remains volatile. Unacknowledged data can be lost; ambiguous results require caller replay. Downstream partial-rejection counts are not an end-to-end receipt through the Collector. Single-node storage does not promise replication or survival of disk loss. Identical replay relies on immutable span sorting keys; conflicting copies remain unsupported. Background TTL deletion is asynchronous and query visibility must use the exact retention cutoff.

At Phase 1 completion no HTTP query API or UI existed. Phase 2 adds them; detector and measured performance remain later milestones.
