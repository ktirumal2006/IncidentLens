# IncidentLens

IncidentLens is a planned observability and production-incident investigation platform for distributed applications. Its initial MVP will use distributed traces to help engineers investigate elevated latency and errors and identify likely responsible services with supporting evidence.

Planned architecture: OpenTelemetry Demo → OpenTelemetry Collector → Go ingestion → ClickHouse → Go query API and deterministic incident detector → React/TypeScript web application.

**Current status: planning only.** No application code, runnable services, UI, or benchmarks have been implemented. The MVP is trace-only; additional signals and infrastructure are deferred.

- [Product plan](docs/PRODUCT.md)
- [Architecture and proposed repository structure](docs/ARCHITECTURE.md)
- [Milestones and acceptance criteria](docs/MILESTONES.md)
- [Instructions for coding agents](AGENTS.md)
