# Local trace pipeline benchmarks

Phase 4 measures the existing local stack. Read [WORKLOAD.md](WORKLOAD.md) before
running: it fixes the payload, offered load, warmup, repetitions, query mix,
outage, targets and safety stops. Targets are not measured results.

The Go workload generator uses the existing backend module and OTLP/native
ClickHouse dependencies. Python standard-library scripts orchestrate the finite
campaign and collect local Docker/process/disk observations. No runtime service,
metrics/logs ingestion, schema or product resource limit is added.

## Prerequisites

Start the product Compose stack with migrations applied and both readiness
endpoints healthy. Use the documented Go version, Python 3, Docker Compose and
sufficient free disk. Do not generate Demo traffic or run other tests while a
campaign is active. Existing data and normal background merges are retained.
The outage stage temporarily stops the local ClickHouse container and restores
it even on failure; never run against a production or unrelated environment.

From the repository root:

```sh
(cd backend && go test -race ./cmd/bench && go build -o /tmp/incidentlens-bench ./cmd/bench)
python3 -m unittest discover -s benchmarks -p 'test_*.py'
python3 benchmarks/run.py --plan benchmarks/plan.json --binary /tmp/incidentlens-bench --output benchmarks/results/<new-run-id>
```

Choose a new output directory for every campaign. Do not overwrite failed runs.
The runner records the workload declaration hash, exact command, executable hash,
source-file hashes, Git revision/dirty state and environment before starting.
A finite plan and bounded queues/retries prevent the generator from quietly
turning overload into an unlimited backlog. See the generator's `--help` for
flags; the checked-in plan supplies the declared values explicitly.

## Read the evidence

Each campaign retains environment and campaign JSON. Each stage retains the
harness configuration, raw export/query/visibility observations, finite identity
reconciliation, stdout/stderr, resource samples and before/after storage/disk
snapshots. Warmup data remains available but is excluded from measured-period
summaries. Successful and failed API requests must be reported separately.

An acknowledged export is not evidence that every intended span exists: the
harness compares deterministic `(trace_id, span_id)` identities with `FINAL`
storage. Report missing acknowledged, missing emitted, never-emitted and
unexpected identities separately. Retry counts are attempts, not extra logical
spans. Polling visibility is an observed upper bound with censoring, not exact
arrival time. CPU percentages use Docker's one-core convention; block I/O does
not measure disk occupancy.

A failed budget is a result to retain, not permission to tune the stack or
silently change the workload. Generator lag alone is not product saturation.
Unmeasured stages remain unmeasured. Phase 5 depends on the published evidence
and a separate request; this harness does not introduce a broker or durable queue.

## Recorded campaign

See the [Phase 4 report](../docs/VERIFICATION_PHASE4.md) for results and limits.
Recompute derived analysis without contacting the runtime:

```sh
python3 benchmarks/analyze.py benchmarks/results/20260929T0200Z --output /tmp/incidentlens-analysis
```

The campaign's `completed` status describes orchestration, not passing performance
budgets. The report retains query failures and overload drops. Counter resets
produce unknown I/O deltas. Source hashes in environment.json identify the exact
measured generator/runner; the final analyzer adds a tested counter-reset fix.
