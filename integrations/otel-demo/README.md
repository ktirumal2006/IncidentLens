# External Demo trace-only scenario

This integration uses published upstream Demo `v0.4.0-alpha` images pinned by digest in `compose.yaml`. `VERSION` records the corresponding source tag and commit `860a35de9be667ff36788d3df09ed4a96b9290fd`. Source inspection stays in an external checkout; no Demo application code is copied into IncidentLens.

On September 25, 2026, the three published Linux/amd64 images were pulled and started under Docker Desktop. The documented recommendations request returned HTTP 200 and four products. Storage-level trace verification belongs to the Phase 1 acceptance evidence; HTTP success alone does not prove ingestion. [Recorded image digests](evidence/image-digests.txt) and [initial failed-start output](evidence/demo-startup.txt) are retained. That output contains resolved startup/gzip failures and is not passing-run evidence. The final [Phase 1 verification record](../../docs/VERIFICATION.md) includes a successfully stored 16-span trace across these services.

## Why this historical release

Demo 2.1.3 and 3.1.0 explicitly instantiate frontend metric/log exporters and recommendation log exporters. Environment exporter selectors do not disable exporters constructed directly by applications. The selected historical services create only trace exporters. Their obsolete dependencies limit this setup to local synthetic investigation; no production suitability is claimed.

Recommendation pins Python SDK 1.9.1. Its [configurator](https://github.com/open-telemetry/opentelemetry-python/blob/v1.9.1/opentelemetry-sdk/src/opentelemetry/sdk/_configuration/__init__.py) treats `OTEL_LOGS_EXPORTER=none` as an empty exporter list and has no metric exporter initialization. `OTEL_METRICS_EXPORTER=none` is defensive configuration, not a claim that this old SDK has a metrics toggle. Application stdout stays ordinary Docker output.

The published recommendation image initially failed on old generated protobuf descriptors. The supported `PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python` setting resolves this using the Python implementation, without source changes. It may be slower; no performance claim is made.

## Exact scenario and dependency closure

Use the frontend recommendations API directly, excluding product `OLJCESPC7Z`:

```text
GET /api/recommendations?productIds=OLJCESPC7Z&productIds=OLJCESPC7Z
  frontend → recommendationservice → productcatalogservice.ListProducts
  frontend → productcatalogservice.GetProduct (recommended IDs only)
```

The repeated parameter produces an array in the historical Next.js handler. Recommendation removes input product IDs before selecting products. Only `GetProduct(OLJCESPC7Z)` calls the old feature service, whose upstream dependency is PostgreSQL. Excluding that ID removes this path. The required startup variable `FEATURE_FLAG_GRPC_SERVICE_ADDR` points to a closed local port, which the supported scenario does not contact.

The complete runtime closure is exactly `frontend`, `recommendationservice`, and `productcatalogservice`, plus the independent IncidentLens Collector/ingestion/ClickHouse stack. No upstream Compose file is merged, so cart, Redis, checkout, Kafka, feature service, PostgreSQL, load generator, and other observability backends cannot start implicitly. Other frontend routes and arbitrary product requests are unsupported. Curl does not execute browser instrumentation.

Source evidence:

- [Frontend trace initialization](https://github.com/open-telemetry/opentelemetry-demo/blob/860a35de9be667ff36788d3df09ed4a96b9290fd/src/frontend/utils/telemetry/Instrumentation.js)
- [Recommendations handler](https://github.com/open-telemetry/opentelemetry-demo/blob/860a35de9be667ff36788d3df09ed4a96b9290fd/src/frontend/pages/api/recommendations.ts)
- [Recommendation filtering](https://github.com/open-telemetry/opentelemetry-demo/blob/860a35de9be667ff36788d3df09ed4a96b9290fd/src/recommendationservice/recommendation_server.py)
- [Catalog conditional feature lookup](https://github.com/open-telemetry/opentelemetry-demo/blob/860a35de9be667ff36788d3df09ed4a96b9290fd/src/productcatalogservice/main.go)

## Run

Start IncidentLens using the root run instructions. Its Compose creates `incidentlens-telemetry`. This integration joins that external network and exports directly to `collector:4317`; it does not depend on host-port reachability.

```sh
docker compose -f integrations/otel-demo/compose.yaml config --services
# Exactly frontend, recommendationservice, productcatalogservice.
docker compose -f integrations/otel-demo/compose.yaml pull
docker compose -f integrations/otel-demo/compose.yaml up -d
curl --fail --show-error --max-time 15 'http://127.0.0.1:8080/api/recommendations?productIds=OLJCESPC7Z&productIds=OLJCESPC7Z'
```

Allow services to start before the request. If the initial request fails, inspect `docker compose -f integrations/otel-demo/compose.yaml logs`, resolve any startup issue, then repeat it. A failed request is not healthy-control evidence.

Wait for export, then follow the root storage-verification instructions. Require all three service names within one trace ID with the expected parent chain. Check rejected/drop counts, source times, and absence of feature-service calls; three unrelated service spans do not satisfy the criterion.

```sh
docker compose -f integrations/otel-demo/compose.yaml down
```

## Source inspection and build limitations

To inspect the exact external source, use a new directory outside IncidentLens:

```sh
git clone --branch v0.4.0-alpha --depth 1 https://github.com/open-telemetry/opentelemetry-demo.git /tmp/incidentlens-otel-demo
git -C /tmp/incidentlens-otel-demo rev-parse HEAD
git -C /tmp/incidentlens-otel-demo diff --exit-code HEAD --
```

The upstream source-build attempt failed: the catalog's Go 1.17.7 Dockerfile downloads unversioned `protoc-gen-go`, which now resolves protobuf 1.36.12 requiring Go 1.23. [Raw failure](evidence/catalog-build-failure.txt) is retained. The supported run uses unchanged published release images instead, avoiding this floating build-tool resolution.

An initial source review suspected frontend RPC addresses were compiled in by Next.js. Inspection of the actual release image showed the gateways retain destructured `process.env` access, so runtime Compose variables work; HTTP 200 confirmed it. No `.env.production` preparation or application build is needed. Unused gateways receive `127.0.0.1:1` to avoid empty client targets without introducing services. Published image labels contain no source revision, so tag-to-source correspondence is upstream release attribution rather than independently attested provenance.

## Phase 3 controlled faults

The verified [Phase 3 procedure](PHASE3_SCENARIO.md) adds reversible frontend CPU
latency and the native invalid-product catalog error route, with isolated
namespaces, real fixed windows, predeclared expectations and clean recovery.
[ADR 0003](../../docs/adr/0003-demo-detection-faults.md) documents the narrow route
expansion and unchanged three-service dependency closure.
