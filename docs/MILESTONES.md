# IncidentLens milestones

Status: Phases 1 and 2 are complete; their acceptance criteria passed. Phase 3 is the next incomplete milestone and has not started. Phases 3–9 require a separate request.

Phases 1–3 form the functional trace-only MVP. Phase 4 establishes measured performance. Phases 5–8 are conditional extensions: each requires a concrete need and an ADR before implementation; defer or skip a phase when evidence does not justify it. Phase 9 can polish the trace-only product even if extensions are skipped. Redis, PostgreSQL, and AI/LLMs have no planned phase and remain excluded without a separately justified scope change.

## Phase 1: trace ingestion

Deliver the local Collector → Go ingestion → ClickHouse path.

Completed 2026-09-25 (America/New_York). See [criterion-by-criterion verification and raw evidence](VERIFICATION.md).

- Implemented trace-only Go ingestion, faithful bounded normalization, synchronous ClickHouse inserts, repeatable migration, restricted writer credentials, local Collector/Compose configuration and run instructions.
- Passed the complete race-enabled suite with all integration/restart/failure gates enabled, plus Go vet/build and pinned Collector validation.
- Verified exact context storage, invalid/mixed/oversized input, gzip, unsupported signals, duplicate-safe replay before merges, real storage outage, Collector overflow and forced-restart recovery. The finite queue fixture recovered all 16,384 unique spans after retry.
- Retrieved a real 16-span Demo trace across frontend, recommendationservice and productcatalogservice, with every observed parent present, exact stored durations, no ERROR status or source-reported drops, and an HTTP 200 response. This does not prove complete delivery for arbitrary traffic.
- Accepted the digest-pinned historical Demo subset in [ADR 0001](adr/0001-demo-trace-only-candidate.md) and corrected Collector acknowledgment behavior in [ADR 0002](adr/0002-collector-acknowledgment.md).

Remaining limitations: local synthetic use, immutable span keys, no durable Collector queue or disk-failure guarantee, downstream partial-rejection caveats, and an intentionally narrow historical Demo scenario. Phase 2 adds query/explorer behavior separately; detection and benchmarks remain unimplemented.

Acceptance criteria:

- Pin compatible Demo, Collector, and ClickHouse versions. Document and verify a Demo service subset and browsing scenario spanning at least three services without any excluded runtime technologies. Keep the upstream checkout external; store only integration configuration and version references.
- Disable metrics/logs export and pipelines for this integration. Confirm that no excluded backend is started implicitly by Compose dependencies. If a scenario cannot satisfy these constraints, document and select a smaller compatible scenario before implementing around it.
- Implement the proposed span schema and repeatable migration procedure. Document exact request, batch, attribute, queue, future-clock, and timeout limits before testing boundaries.
- A known valid OTLP fixture preserves IDs, parent relationships, nanosecond timestamps, scope/resource context, typed attributes, events, links, and source drop counts in storage.
- Invalid IDs, invalid times, absent service identity, oversized requests, mixed valid/invalid batches, and unsupported signals produce documented protocol outcomes.
- Replaying the same fixture, including an ambiguous-write retry before table merges, leaves one logical span per identity in deduplicated reads.
- Storage unavailability and bounded-queue exhaustion produce retryable failures rather than false success. Restart tests distinguish already-acknowledged stored data from unacknowledged/in-memory data; document where loss is possible.
- A clean local setup can generate and retrieve a multi-service Demo trace through a documented storage verification procedure. All phase 1 behavior tests pass; no explorer UI is required yet.

## Phase 2: trace query/explorer

Deliver bounded HTTP queries and the React/TypeScript trace explorer.

Completed 2026-09-27 (America/New_York). See the [criterion-by-criterion verification and raw evidence](VERIFICATION_PHASE2.md).

- Implemented documented, bounded service summaries, trace search and trace detail
  APIs with SELECT-only access, duplicate-safe reads, stable cursor semantics,
  exact nanosecond fields and explicit incomplete/truncated observations.
- Delivered the React/TypeScript explorer with keyboard search/navigation,
  multi-service waterfalls, span context and visible loading/error/empty/
  insufficient-data states.
- Passed the complete race-enabled Go suite with all real-dependency, query,
  restart and failure gates enabled, plus vet/build, 14 frontend behavior/precision
  tests and a real Demo-to-browser E2E test. Verified desktop/mobile rendering.
- Followed an injected known Demo trace through ingestion, search and detail:
  14 spans across the three supported services; the intentionally external parent
  is reported missing rather than treated as complete telemetry.

Remaining limitations: bounded local queries, nontransactional cursor pagination,
explicit caps, incomplete telemetry and the existing immutable-span/historical
Demo assumptions. No incident detector, incident UI or benchmark is implemented.

Acceptance criteria:

- Document and implement service summaries, trace search, and trace detail contracts, including UTC units, filter semantics, cursor behavior, timeouts, caps, and errors.
- Filter fixtures by time, service, operation, span duration, and ERROR status with exact expected results; replayed spans do not inflate counts or percentiles.
- Verify stable pagination on frozen data and documented refresh behavior with late arrivals. Distinguish matching-span durations from trace elapsed time.
- Display a multi-service waterfall with parent/child relationships, service labels, span details, and error status. Cover missing parents, missing roots, out-of-order arrival, and oversized/truncated responses explicitly.
- Empty, loading, failure, and insufficient-data states are visible. A user can search and open a trace by keyboard.
- An end-to-end test follows a known Demo trace from ingestion to search to detail. Query tests use real ClickHouse where SQL behavior matters; frontend tests cover user-visible behavior.

## Phase 3: deterministic incident detection

Deliver explained, window-specific incident candidates within the query API and UI.

Acceptance criteria:

- Implement and version the documented windows, SERVER-span denominator, exact percentile calculation, minimum samples, thresholds, candidate ordering, and bounded evidence selection.
- Frozen fixtures cover healthy traffic, latency degradation, error degradation, threshold boundaries, zero baseline latency, low volume, UNSET status, duplicate spans, downstream error propagation, and missing relationships.
- Identical data/windows/configuration yield identical results, tie ordering, and evidence selection. Late-data reevaluation is documented separately.
- The UI shows baseline/current values, sample sizes, triggered rules, candidate service/operation, uncertainty, and navigable trace evidence; it never presents a hypothesis as guaranteed root cause.
- Record repeatable healthy, single-service latency, and single-service error Demo scenarios. Healthy control produces no candidate; the injected service ranks in the top three service candidates for each fault scenario, with expected outcomes recorded before execution.
- After fault removal and a full clean current window, reevaluation clears the controlled degradation. Document rule limitations and any failed scenario instead of tuning against undisclosed fixtures.

## Phase 4: load testing

Validate capacity of the existing trace-only architecture; do not add infrastructure to manufacture a target result.

Acceptance criteria:

- Commit a repeatable harness and workload definition: offered span rate, span size/attributes, spans per trace, service distribution, duration, warmup, concurrency, and query mix.
- Before running, declare the target workload, duration, acceptable acknowledged-data loss (zero), acceptable end-to-end loss, and latency/error budgets. Distinguish targets from results.
- Measure ingestion throughput, rejected/retried/lost spans, acknowledgment and visibility latency, API p50/p95/p99, CPU, memory, and disk use using the harness and local runtime tools; no metrics/logs product pipeline is needed.
- Include steady load, increasing load through saturation, concurrent queries, and a storage outage/recovery run. Compare input identities with deduplicated storage for finite runs.
- Publish hardware, versions, commands, dataset size, sampling settings, raw results, repeated-run variation, and bottlenecks. Failed targets and unknowns are reported openly.
- Produce a decision on whether phase 5 is justified by measured buffering/replay or throughput needs. No fabricated benchmark numbers or unmeasured scale claims.

## Phase 5: streaming architecture — conditional

Gate: phase 4 demonstrates a specific ingestion, replay, or outage-buffering limitation that the simpler architecture cannot reasonably address.

Acceptance criteria if activated:

- An ADR compares keeping direct ingestion, bounded Collector buffering, and a streaming broker. Kafka is an option only after this justification; no automatic Redis dependency.
- Define the revised producer/consumer boundary, acknowledgment point, ordering, replay, retention, backpressure, and duplicate semantics before implementation.
- Verify consumer crash/restart, replay, broker outage, and duplicate delivery against known fixtures without inflated query/detection results.
- Repeat the phase 4 workload on comparable hardware and publish both benefits and operational costs, plus a migration/rollback procedure.

## Phase 6: metrics/logs — conditional

Gate: a documented investigation question cannot be answered adequately using traces.

Acceptance criteria if activated:

- An ADR chooses the first additional signal and its user workflow, data model, retention, cost bounds, and correlation contract. Implement signals incrementally.
- Each added signal has validated ingestion, bounded queries, tested malformed/late/duplicate input semantics, and a concrete UI workflow linked to service/time and trace identity where available.
- Define metrics temporality/cardinality handling and logs redaction/size handling before implementing the respective signal.
- A scenario demonstrates added investigative value; the trace-only workflow and tests still pass with new signals disabled.

## Phase 7: Kubernetes — conditional

Gate: there is a deployment/scaling learning objective or demonstrated operational need beyond local Compose.

Acceptance criteria if activated:

- An ADR defines target cluster scope and stateful storage responsibility. Add manifests only for components actually adopted.
- A documented deployment works on a clean local cluster with resource requests/limits, health probes, internal networking, configuration/secrets separation, and persistent storage.
- Pod restart and rollout checks preserve acknowledged data under the stated storage guarantees; the full investigation scenario still works.
- Document setup, teardown, debugging, and differences from the retained Compose workflow. Do not claim high availability from replica counts alone.

## Phase 8: cloud/Terraform — conditional

Gate: a specific cloud deployment objective, provider, budget, and access model are selected. This phase does not require Kubernetes if phase 7 was skipped.

Acceptance criteria if activated:

- An ADR records provider, topology, expected cost assumptions, data handling, and migration/rollback approach.
- Terraform validates and produces a reviewable plan; provisioning and teardown are reproducible. State and credentials are managed outside version control.
- Verify private datastore access, encrypted public transport, appropriate user authentication, retention, and a tested backup/restore path before public use.
- A deployed synthetic scenario passes the investigation workflow; measured operating cost and remaining reliability limits are documented. Verify resource teardown to avoid continuing charges.

## Phase 9: polish

Deliver a clear, credible portfolio presentation of the capabilities actually completed.

Acceptance criteria:

- README and architecture match the implemented system and clearly mark skipped/deferred phases. Remove stale promises and placeholder instructions.
- A clean checkout can follow documented setup and reproduce a healthy-to-fault-to-recovery walkthrough with screenshots or a short recording from the working product.
- Verify keyboard navigation, visible focus, readable contrast, responsive layouts, and useful empty/error/loading states for implemented workflows.
- All relevant automated checks pass; publish real benchmark methodology/results and architectural tradeoffs with links to ADRs.
- Clearly document unsupported cases and known limits. Do not imply that optional streaming, additional signals, Kubernetes, or cloud deployment exists when it does not.
