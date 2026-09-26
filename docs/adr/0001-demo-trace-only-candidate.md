# ADR 0001: narrow the Demo browsing scenario

Status: accepted, 2026-09-25. See [runtime evidence](../VERIFICATION.md).

## Context

Phase 1 requires an external, pinned Demo scenario spanning at least three services with no excluded runtime dependencies and no metrics/logs export. Source inspection of Demo 2.1.3 and 3.1.0 found explicit frontend metrics/log exporter construction and explicit recommendation log exporter construction. Environment exporter selectors alone do not turn these off. The current full storefront topology also exceeds the product dependency scope.

## Alternatives

1. Run upstream defaults and drop unwanted signals: violates the no-export and runtime dependency requirements.
2. Modify upstream instrumentation: creates a fork and violates the external Demo boundary.
3. Choose another current service subset: preferred if a three-service browsing path meets the requirements; none has yet been validated.
4. Evaluate a historical trace-only subset: preserves the required three-service path, but introduces old build dependencies and compatibility uncertainty.

## Decision

Use published, digest-pinned Demo v0.4.0-alpha images corresponding to source at commit `860a35de9be667ff36788d3df09ed4a96b9290fd`, using only the frontend recommendations endpoint, recommendationservice, and productcatalogservice. Exclude product `OLJCESPC7Z` from recommendations, because its catalog lookup reaches an excluded feature-service dependency. Keep source inspection outside IncidentLens and use unchanged upstream release images with IncidentLens-owned Compose configuration. Set `PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python` to support the historical generated descriptors without modifying services.

The supported recommendations request returned HTTP 200 and produced a stored three-service trace. See [source evidence, dependency closure and run procedure](../../integrations/otel-demo/README.md). The product's Collector → Go → ClickHouse architecture and trace-only scope do not change.

## Consequences

A source-build attempt failed because an unpinned protobuf compiler tool outgrew the historical Go compiler. The published release images avoid that floating build dependency and their digests are recorded. Image labels do not attest the source commit: correspondence is upstream release attribution. Runtime configuration, parent propagation and storage retrieval passed. The full storefront, arbitrary product requests and later fault scenarios remain unsupported or unverified. These obsolete images are limited to the local synthetic scenario; no production suitability is implied.
