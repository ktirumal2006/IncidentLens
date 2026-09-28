# ADR 0003: trace-only Demo fault scenarios

Status: accepted; full default-threshold scenarios and clean recovery passed on
2026-09-28. See [verification and raw evidence](../VERIFICATION_PHASE3.md).

## Context

Phase 3 requires healthy, single-service latency and error scenarios, followed by
recovery, using the existing external Demo. The pinned historical recommendations
path has no native latency switch. Modern Demo defaults and its feature-flag
service would introduce excluded dependencies; editing upstream instrumentation
or rewriting stored telemetry would invalidate the validation.

The clean external checkout remains pinned to
`860a35de9be667ff36788d3df09ed4a96b9290fd`. Its existing frontend product endpoint
calls catalog `GetProduct`. The catalog explicitly marks unknown product IDs
ERROR and returns gRPC NotFound. The special feature-service branch is entered
only for `OLJCESPC7Z`, so a fixed unknown ID does not contact it.

## Alternatives

1. Upgrade to the full current Demo and feature flags: excluded runtime
   dependencies and unnecessary scope expansion.
2. Patch services or fabricate slow/error stored spans: violates the external
   application boundary and would not validate detection of real faults.
3. Add a network proxy or traffic-control service: extra dependency/privileges;
   network delay may inflate callers without inflating the intended SERVER span.
4. Use existing application error handling and temporarily constrain one
   container's CPU quota: keeps unchanged upstream images and existing Docker
   tooling, with a clear runtime fault and reversible cleanup.

## Decision

Use option 4. Healthy traffic uses the existing recommendations path. The error
scenario deliberately requests `/api/products/PHASE3-INVALID`, exercising catalog
GetProduct's native unknown-product error. No request uses `OLJCESPC7Z` in the
product endpoint. This narrowly expands ADR 0001's supported routes for testing;
the complete runtime closure remains frontend, recommendationservice and catalog.

The latency scenario constrains only the frontend CPU quota, then restores the
base Compose configuration. The injected service is frontend: the detector must
rank the resulting frontend SERVER latency candidate, not claim that an arbitrary
downstream service was delayed. A pilot at 0.1 CPU and ten concurrent requests
returned 30/30 HTTP 200 and exceeded the default latency thresholds. The full
scenario workload and expected outcomes are declared before execution in its
scenario artifacts. Pilot data is excluded from acceptance with a distinct
service namespace supplied through existing upstream resource configuration.

Keep default detector thresholds, actual source timestamps and the documented
30-minute baseline / five-minute current windows. Generate sufficient actual
requests for both sample minimums; do not shift timestamps or shorten windows.
Allow a full clean current window after removing each fault. Record every
evaluation end time, workload, fault setting, observation and failed assertion.

## Consequences

The error scenario is a deliberate invalid-input failure, not evidence that the
healthy catalog spontaneously malfunctions. CPU quota is a scheduling fault and
its effects depend on hardware and workload. Neither scenario calibrates rules
for production or proves causation. Pilot and acceptance results must remain
separate; failures cannot be hidden by changing undisclosed expectations.

A recommendationservice 0.01-CPU pilot was too severe and caused timeouts; it is
not an accepted scenario. `docker update --cpus 0` did not clear its quota in this
environment. Cleanup therefore recreates affected Demo containers from the base
Compose configuration and verifies restored quotas. Product code, schemas and
upstream source remain unchanged by fault injection. No new runtime technology,
metrics/logs pipeline or benchmark is introduced.
