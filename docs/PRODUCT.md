# IncidentLens product plan

Status: Phases 1–2 are complete and verified: trace ingestion, service summaries, trace search and the waterfall explorer. Incident detection and comparison remain proposed. See [milestone status](MILESTONES.md), [ingestion verification](VERIFICATION.md) and [explorer verification](VERIFICATION_PHASE2.md).

## Problem statement

When a distributed application slows down or starts returning errors, the service exposing the symptom may not be the service causing it. Engineers must follow requests across service boundaries, compare healthy and degraded behavior, and inspect evidence before deciding where to investigate.

IncidentLens will turn distributed traces into an investigation workflow: identify a degraded service or operation, compare it with a recent baseline, and inspect representative traces to identify likely responsible services. Its conclusions are evidence-backed hypotheses, not proof of root cause.

## Target user

A software engineer or on-call engineer investigating latency and errors in a distributed application. The initial user runs a local, single-user environment against the external OpenTelemetry Demo e-commerce application. This is a portfolio project, not initially a hosted production service.

## Core user workflow

1. Run the supported OpenTelemetry Demo scenario and send its traces through the Collector to IncidentLens.
2. Select a time range and inspect services, operations, request-span counts, latency, and error proportions derived solely from traces.
3. Open a deterministic incident candidate, or filter traces manually by service, operation, duration, or span error status.
4. Compare the current window with its baseline and inspect the rule, sample counts, and supporting trace IDs.
5. Open a trace waterfall, follow parent/child relationships across services, and examine likely responsible services and uncertainty.
6. Remove the injected fault and evaluate a later window to confirm that the degradation is no longer detected.

## MVP scope

- Trace-only OTLP ingestion through the OpenTelemetry Collector into a Go service and ClickHouse.
- Bounded validation, batching, retry behavior, retention, and duplicate-safe reads.
- Go query API and React/TypeScript interface for time-bounded trace search and trace detail.
- Service/operation latency and error summaries computed from stored spans; these are trace-derived statistics, not an additional telemetry signal.
- Deterministic, versioned rules comparing fixed current and baseline windows, with minimum sample sizes and visible thresholds.
- Ranked investigation candidates with supporting traces and explicit handling of missing or insufficient evidence.
- Repeatable healthy, latency-fault, and error-fault scenarios using a pinned, supported subset of the OpenTelemetry Demo.
- Local development and demonstration with Docker Compose. Phases 1–3 deliver the functional MVP; phase 4 validates its performance envelope without expanding signal scope.

## Explicit non-goals

- No Kafka, Redis, Kubernetes, Terraform, PostgreSQL, AI/LLMs, metrics ingestion, or logs ingestion in the MVP. Upstream Demo defaults are not permission to add excluded technologies to the local setup.
- No full observability suite, arbitrary analytics query language, or replacement for established production platforms.
- No guaranteed root-cause attribution, automated remediation, paging integrations, or persistent incident assignment/acknowledgment workflow.
- No multi-tenancy, public hosting, production authentication system, high availability, or exactly-once delivery guarantee.
- No adaptive/tail sampling, service instrumentation agent, custom broker, or general workflow engine.
- No rewriting, embedding, or maintaining a fork of the OpenTelemetry Demo as product code.

## Success criteria

These are acceptance targets, not achieved results:

- A documented local run produces a trace spanning at least three services in the supported Demo subset and makes it searchable with correct IDs, timestamps, and parent links.
- A known finite fixture corpus is fully accounted for: accepted unique spans match expected stored/queryable spans; rejected spans are explained; replay does not inflate query results or detector statistics.
- A user can find a slow/error trace, inspect its waterfall, and navigate from an incident candidate to its supporting trace evidence.
- Healthy, injected-latency, and injected-error fixtures pass the documented deterministic expectations. In controlled single-fault Demo scenarios, the injected service appears among the top three candidates; ambiguous scenarios display uncertainty rather than a false guarantee.
- Low-volume or incomplete evidence produces an explicit insufficient-evidence state where applicable. Repeating an evaluation over the same frozen data, rule version, and windows returns the same result and ordering.
- The repository contains reproducible tests and scenario instructions. Phase 4 publishes measured capacity and query latency with hardware, workload, methodology, and limitations; no performance claim precedes measurement.

See [architecture](ARCHITECTURE.md) for proposed contracts and [milestones](MILESTONES.md) for delivery gates.
