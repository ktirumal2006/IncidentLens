#!/usr/bin/env python3
"""Recompute finite benchmark metrics from retained raw observations.

Missing evidence produces explicit unknowns, never a passing measurement.
No runtime calls or telemetry mutations are performed by this analyzer.
"""
import argparse
import calendar
import json
import re
import statistics
import hashlib
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path


def time_ns(value):
    match = re.fullmatch(r"(.+T\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?(Z|[+-]\d\d:\d\d)", value or "")
    if not match:
        raise ValueError(f"invalid explicit-zone timestamp: {value!r}")
    date = datetime.fromisoformat(match[1] + ("+00:00" if match[3] == "Z" else match[3]))
    return calendar.timegm(date.utctimetuple()) * 1_000_000_000 + int((match[2] or "").ljust(9, "0") or "0")


def percentiles(values):
    if not values:
        return {"count": 0, "p50_ns": None, "p95_ns": None, "p99_ns": None, "max_ns": None, "p99_tail_samples": 0}
    ordered = sorted(values)
    result = {"count": len(ordered), "max_ns": ordered[-1], "p99_tail_samples": max(1, len(ordered) // 100)}
    for percentile in (50, 95, 99):
        result[f"p{percentile}_ns"] = ordered[min(len(ordered) - 1, len(ordered) * percentile // 100)]
    return result


def read_json(path, issues):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError) as error:
        issues.append(f"{path.name}: {error}")
        return None


def read_jsonl(path, issues):
    records = []
    try:
        with path.open() as source:
            for number, line in enumerate(source, 1):
                try:
                    record = json.loads(line)
                    if not isinstance(record, dict):
                        raise ValueError("record is not an object")
                    records.append(record)
                except ValueError as error:
                    issues.append(f"{path.name}:{number}: {error}")
    except OSError as error:
        issues.append(f"{path.name}: {error}")
    return records


def event_time(event, field, issues):
    try:
        return time_ns(event.get(field))
    except (ValueError, TypeError) as error:
        issues.append(f"{event.get('kind')} {field}: {error}")
        return None


def analyze_events(events, config, start, end, issues):
    """Use schedule clocks for exports and actual start clocks for HTTP queries."""
    duration = (end - start) / 1e9
    if duration <= 0:
        raise ValueError("measurement interval must be positive")
    spans_per_trace = sum(config.get("span_kinds_per_trace", {}).values())
    if not spans_per_trace:
        issues.append("missing span_kinds_per_trace; identity throughput unknown")
    lifecycle = defaultdict(set)
    scheduled_batches = {}
    for event in events:
        if event.get("kind") == "scheduled":
            scheduled_batches[event.get("batch", 0)] = event_time(event, "scheduled", issues)
    attempts = defaultdict(list)
    completions = {}
    query_values = defaultdict(lambda: {"successful": [], "failed": [], "statuses": Counter(), "bytes": 0, "failed_requests": 0, "successful_requests": 0})
    visibility = []
    probes = []
    kinds = Counter()
    raw_phases = Counter()
    for event in events:
        kind = event.get("kind", "unknown")
        kinds[kind] += 1
        raw_phases[event.get("phase", "unspecified")] += 1
        if kind in {"scheduled", "generated", "enqueued", "schedule_drop"}:
            scheduled = scheduled_batches.get(event.get("batch", 0))
            if scheduled is None:
                scheduled = event_time(event, "scheduled", issues)
            first, count = event.get("first_trace", 0), event.get("traces", 0)
            if not isinstance(first, int) or not isinstance(count, int) or count <= 0:
                issues.append(f"{kind}: invalid trace index/count")
                continue
            if scheduled is not None and start <= scheduled < end:
                lifecycle[kind].update(range(first, first + count))
            if kind == "scheduled":
                scheduled_batches[event.get("batch", 0)] = scheduled
        elif kind == "export_attempt":
            attempts[event.get("batch", 0)].append(event)
        elif kind == "batch_complete":
            batch = event.get("batch", 0)
            if batch in completions:
                issues.append(f"duplicate batch_complete for batch {batch}")
            completions[batch] = event
        elif kind == "query":
            begun = event_time(event, "started", issues)
            if begun is None or not start <= begun < end:
                continue
            route = event.get("route", "unknown")
            status = event.get("http_status", 0)
            outcome = "successful" if 200 <= status < 300 and not event.get("error") else "failed"
            row = query_values[route]
            row["statuses"][str(status)] += 1
            row[outcome + "_requests"] += 1
            row["bytes"] += event.get("response_bytes", 0)
            if isinstance(event.get("latency_ns"), int):
                row[outcome].append(event["latency_ns"])
            else:
                issues.append(f"query {route}: missing latency_ns")
        elif kind == "visibility":
            visibility.append(event)
        elif kind == "visibility_probe":
            probes.append(event)

    ack_latency, schedule_latency, attempt_latency = [], [], []
    acknowledged, acknowledged_within, attempted = set(), set(), set()
    retries = rejected = terminal = 0
    missing_completions = 0
    completion_statuses = Counter()
    measured_batches = set()
    for batch, rows in attempts.items():
        scheduled = scheduled_batches.get(batch)
        if scheduled is None:
            scheduled = event_time(rows[0], "scheduled", issues)
        if scheduled is None or not start <= scheduled < end:
            continue
        measured_batches.add(batch)
        for event in rows:
            attempted.update(range(event.get("first_trace", 0), event.get("first_trace", 0) + event.get("traces", 0)))
            retries += int(event.get("attempt", 0) > 1)
            rejected += max(0, event.get("rejected_spans", 0))
            if isinstance(event.get("latency_ns"), int):
                attempt_latency.append(event["latency_ns"])
        final = completions.get(batch)
        if final is None:
            issues.append(f"batch {batch}: missing final completion; terminal/ACK status unknown")
            missing_completions += 1
            continue
        completion_statuses[final.get("status", "unknown")] += 1
        ack_value = final.get("ack")
        if ack_value and not ack_value.startswith("0001-"):
            ack = event_time(final, "ack", issues)
            first_send = event_time(final, "started", issues)
            if ack is None or first_send is None:
                continue
            first, count = final.get("first_trace", 0), final.get("traces", 0)
            identities = set(range(first, first + count))
            acknowledged.update(identities)
            if start <= ack < end:
                acknowledged_within.update(identities)
            ack_latency.append(ack - first_send)
            schedule_latency.append(ack - scheduled)
        else:
            terminal += 1
    queries = {}
    for route, data in query_values.items():
        queries[route] = {"successful": percentiles(data["successful"]), "failed": percentiles(data["failed"]), "statuses": dict(data["statuses"]), "response_bytes": data["bytes"], "failed_requests": data["failed_requests"], "successful_requests": data["successful_requests"]}
    visible_values, ack_visible_values = [], []
    censored = polls = visibility_samples = 0
    for event in visibility:
        scheduled = scheduled_batches.get(event.get("batch", 0))
        if scheduled is None:
            scheduled = event_time(event, "scheduled", issues)
        if scheduled is None or not start <= scheduled < end:
            continue
        visibility_samples += 1
        polls += event.get("polls", 0)
        if event.get("censored"):
            censored += 1
            continue
        if isinstance(event.get("start_to_visible_ns"), int):
            visible_values.append(event["start_to_visible_ns"])
        else:
            issues.append("visibility: missing start_to_visible_ns")
        if isinstance(event.get("ack_to_visible_ns"), int):
            ack_visible_values.append(event["ack_to_visible_ns"])
    probe_latency = [event["latency_ns"] for event in probes if isinstance(event.get("latency_ns"), int)]
    counts = {kind: len(lifecycle[kind]) * spans_per_trace if (kinds[kind] or kind == "schedule_drop" and kinds["scheduled"]) and spans_per_trace else None for kind in ("scheduled", "generated", "enqueued", "schedule_drop")}
    counts.update(attempted=len(attempted) * spans_per_trace if spans_per_trace else None,
                  acknowledged=len(acknowledged) * spans_per_trace if spans_per_trace else None,
                  acknowledged_within_window=len(acknowledged_within) * spans_per_trace if spans_per_trace else None)
    scheduled_count = counts["scheduled"]
    planned_count = None
    try:
        rate, warmup = config["rate_spans_per_second"], config["warmup_ns"]
        batch, total = config["batch_traces"], config["load_traces"]
        planned_count = sum(min(batch, total-i)*spans_per_trace for i in range(0,total,batch)
                            if warmup <= i*spans_per_trace*1_000_000_000//rate < warmup + config["duration_ns"])
    except (KeyError, ValueError, ZeroDivisionError):
        issues.append("planned schedule unavailable; offered fraction unknown")
    counts["planned"] = planned_count
    return {"measurement_seconds": duration, "raw_event_kinds": dict(kinds), "raw_phase_tags": dict(raw_phases), "measured_counts_spans": counts,
            "offered_spans_per_second": scheduled_count / duration if scheduled_count is not None else None,
            "emitted_spans_per_second": counts["attempted"] / duration if counts["attempted"] is not None else None,
            "acknowledged_within_window_spans_per_second": counts["acknowledged_within_window"] / duration if counts["acknowledged_within_window"] is not None else None,
            "acknowledged_fraction_within_window": counts["acknowledged_within_window"] / planned_count if planned_count else None,
            "acknowledged_final_spans_per_second": counts["acknowledged"] / duration if kinds["scheduled"] and not missing_completions else None,
            "acknowledgment_from_first_send": percentiles(ack_latency), "schedule_to_acknowledgment": percentiles(schedule_latency), "per_attempt_latency": percentiles(attempt_latency),
            "retry_attempts": retries, "rejected_spans": rejected, "terminal_measured_batches": None if missing_completions or not kinds["scheduled"] else terminal, "measured_batches": len(measured_batches), "missing_batch_completions": missing_completions, "batch_completion_statuses": dict(completion_statuses),
            "queries": queries, "visibility": {"sample_count": visibility_samples, "censored_count": censored, "poll_count": polls, "first_send_to_observation": percentiles(visible_values), "ack_to_observation": percentiles(ack_visible_values), "all_phase_probe_request_count": len(probes), "all_phase_probe_latency": percentiles(probe_latency), "note": "Observation latency is an upper bound; probes impose measured read traffic and may observe storage before acknowledgment."}}


def byte_quantity(value):
    match = re.fullmatch(r"\s*([0-9.]+)\s*([A-Za-z]*)\s*", value)
    if not match:
        raise ValueError(f"invalid byte quantity: {value}")
    units = {"B": 1, "": 1, "kB": 1000, "KB": 1000, "MB": 1000**2, "GB": 1000**3, "TB": 1000**4, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3, "TiB": 1024**4}
    return int(float(match[1]) * units[match[2]])


def quota_for(container):
    limits = container.get("limits", {})
    if limits.get("NanoCpus", 0) > 0:
        return limits["NanoCpus"] / 1e9
    if limits.get("CpuQuota", 0) > 0 and limits.get("CpuPeriod", 0) > 0:
        return limits["CpuQuota"] / limits["CpuPeriod"]
    return None


def counter_delta(values):
    # A container restart resets Docker's cumulative I/O counters. A single
    # difference across that boundary is not a measured byte count.
    if len(values) < 2 or any(b < a for a, b in zip(values, values[1:])):
        return None
    return values[-1] - values[0]


def analyze_resources(records, environment, start, end, issues):
    containers = {row.get("name", "").lstrip("/"): row for row in environment.get("containers", [])}
    values = defaultdict(lambda: {"cpu": [], "memory": [], "memory_percent": [], "block_read": [], "block_write": []})
    generator_cpu, generator_rss = [], []
    sample_count = error_count = 0
    for record in records:
        if record.get("kind") != "resource_sample":
            continue
        at = event_time(record, "at", issues)
        if at is None or not start <= at < end:
            continue
        sample_count += 1
        stats = record.get("docker_stats", {})
        if stats.get("returncode") != 0:
            error_count += 1
            issues.append("resource sample: docker_stats command failed")
            continue
        for line in stats.get("stdout", "").splitlines():
            try:
                row = json.loads(line)
                value = values[row["Name"]]
                value["cpu"].append(float(row["CPUPerc"].rstrip("%")))
                value["memory_percent"].append(float(row["MemPerc"].rstrip("%")))
                value["memory"].append(byte_quantity(row["MemUsage"].split("/")[0]))
                read, write = row["BlockIO"].split("/")
                value["block_read"].append(byte_quantity(read))
                value["block_write"].append(byte_quantity(write))
            except (ValueError, KeyError, TypeError) as error:
                error_count += 1
                issues.append(f"resource sample parse: {error}")
        ps = record.get("harness_ps", {})
        try:
            fields = ps["stdout"].split()
            if ps.get("returncode") != 0 or len(fields) < 3:
                raise ValueError("missing/failed ps observation")
            generator_cpu.append(float(fields[1]))
            generator_rss.append(int(fields[2]) * 1024)
        except (KeyError, ValueError) as error:
            issues.append(f"generator resource sample: {error}")
    output = {}
    for name, value in values.items():
        quota = quota_for(containers.get(name, {}))
        output[name] = {"sample_count": len(value["cpu"]), "cpu_quota_cores": quota,
                        "max_cpu_percent_one_core": max(value["cpu"], default=None),
                        "sample_mean_cpu_percent_one_core": statistics.mean(value["cpu"]) if value["cpu"] else None,
                        "max_cpu_percent_quota": max(value["cpu"]) / quota if value["cpu"] and quota else None,
                        "max_memory_bytes_docker": max(value["memory"], default=None), "max_memory_percent": max(value["memory_percent"], default=None),
                        "block_read_delta_bytes": counter_delta(value["block_read"]),
                        "block_write_delta_bytes": counter_delta(value["block_write"])}
    if sample_count == 0:
        issues.append("no resource samples in measurement interval")
    return {"measurement_sample_count": sample_count, "sampling_errors": error_count, "containers": output,
            "generator": {"sample_count": len(generator_cpu), "max_cpu_percent": max(generator_cpu, default=None), "max_rss_bytes": max(generator_rss, default=None)},
            "note": "Docker CPU100% is one core; quota-normalized maxima use inspected limits. Memory is Docker's reported usage/cache convention. Block I/O is cumulative traffic, not disk occupancy; reset counters produce null deltas. Means are unweighted sample means."}


def storage_totals(snapshot, issues):
    try:
        command = snapshot["clickhouse_parts"]
        if command.get("returncode") != 0:
            raise ValueError("storage observation command failed")
        rows = json.loads(command["stdout"])["data"]
        return {key: sum(int(row[key]) for row in rows) for key in ("active_parts", "physical_rows", "disk_bytes")}
    except (ValueError, KeyError, TypeError) as error:
        issues.append(f"storage totals unavailable: {error}")
        return None


def assessment(value, condition):
    return "unknown" if value is None else "pass" if condition(value) else "fail"


def assess_targets(metrics):
    counts = metrics["measured_counts_spans"]
    result = {"acknowledged_within_window_95_percent": assessment(metrics["acknowledged_fraction_within_window"], lambda value: value >= .95),
              "zero_schedule_drops": assessment(counts["schedule_drop"], lambda value: value == 0),
              "zero_terminal_valid_export_errors": assessment(metrics["terminal_measured_batches"], lambda value: value == 0),
              "ack_p95_500_ms": assessment(metrics["acknowledgment_from_first_send"]["p95_ns"], lambda value: value <= 500_000_000),
              "ack_p99_2_seconds": assessment(metrics["acknowledgment_from_first_send"]["p99_ns"], lambda value: value <= 2_000_000_000)}
    result["query_routes"] = {route: {"zero_non_2xx": assessment(row["failed_requests"], lambda value: value == 0),
                                     "p95_1_second": assessment(row["successful"]["p95_ns"], lambda value: value <= 1_000_000_000),
                                     "p99_2_seconds": assessment(row["successful"]["p99_ns"], lambda value: value <= 2_000_000_000)} for route, row in metrics["queries"].items()}
    visible = metrics["visibility"]
    result["visibility"] = {"p95_1_second": assessment(visible["first_send_to_observation"]["p95_ns"], lambda value: value <= 1_000_000_000), "p99_3_seconds": assessment(visible["first_send_to_observation"]["p99_ns"], lambda value: value <= 3_000_000_000), "uncensored": assessment(visible["censored_count"] if visible["sample_count"] else None, lambda value: value == 0)}
    if visible["censored_count"]:
        # Observed-only tails cannot establish the population latency budget.
        for key in ("p95_1_second", "p99_3_seconds"):
            if result["visibility"][key] == "pass":
                result["visibility"][key] = "unknown"
    return result


def analyze_reconciliation(records, config, events, issues):
    planned = config.get("load_traces", 0) + config.get("baseline_traces", 0)
    if not planned or not records:
        issues.append("finite reconciliation ledger/config missing")
        return {"status": "unknown"}
    emitted, acked = set(), set()
    for event in events:
        indices = range(event.get("first_trace", 0), event.get("first_trace", 0) + event.get("traces", 0))
        if event.get("kind") == "export_attempt":
            emitted.update(indices)
        if event.get("kind") == "batch_complete" and event.get("ack") and not event["ack"].startswith("0001-"):
            acked.update(indices)
    seen = set()
    totals = Counter(planned_spans=planned * 6, expected_stored=0, planned_missing=0,
                     emitted_missing=0, acked_missing=0, never_acked_stored=0, unknown=0)
    errors_before = len(issues)
    for row in records:
        if row.get("kind") == "unknown":
            totals["unknown"] += 1
            continue
        index, mask = row.get("index"), row.get("stored_mask")
        if row.get("kind") != "expected_trace" or not isinstance(index, int) or index < 0 or index >= planned or index in seen:
            issues.append("reconciliation: invalid/duplicate expected index")
            continue
        seen.add(index)
        identity = hashlib.sha256(f"{config.get('seed')}:{index:016x}".encode()).hexdigest()[:32]
        if row.get("trace_id") != identity or row.get("expected_mask") != 63 or not isinstance(mask, int) or mask < 0 or mask > 63:
            issues.append(f"reconciliation: invalid identity/mask at index {index}")
            continue
        if row.get("acked") != (index in acked) or row.get("emitted") != (index in emitted):
            issues.append(f"reconciliation: raw delivery classification mismatch at index {index}")
        stored = bin(mask).count("1")
        missing = 6 - stored
        totals["expected_stored"] += stored
        totals["planned_missing"] += missing
        totals["emitted_missing"] += missing if index in emitted else 0
        totals["acked_missing"] += missing if index in acked else 0
        totals["never_acked_stored"] += stored if index not in acked else 0
    if len(seen) != planned:
        issues.append(f"reconciliation: expected {planned} trace records, found {len(seen)}")
    valid = len(issues) == errors_before
    return {"status": "verified_ledger" if valid else "unknown", "counts": dict(totals),
            "zero_identity_loss": "unknown" if not valid else "pass" if totals["planned_missing"] == 0 and totals["unknown"] == 0 else "fail",
            "note": "Stored masks derive from bounded FINAL identity queries; classifications are checked against raw export records. Physical duplicate counts cannot be inferred from masks."}


def analyze_stage(directory, environment):
    issues = []
    run = read_json(directory / "run.json", issues) or {}
    config = read_json(directory / "harness/config.json", issues) or {}
    summary = read_json(directory / "harness/summary.json", issues) or {}
    events = read_jsonl(directory / "harness/events.jsonl", issues)
    records = read_jsonl(directory / "resources.jsonl", issues)
    result = {"directory": str(directory), "name": run.get("name", directory.name), "run_status": run.get("status", "unknown"), "metrics": None, "targets": None, "issues": issues}
    if "outage" in run and (not isinstance(run["outage"], dict) or run["outage"].get("status") != "completed"):
        status = run["outage"].get("status", "unknown") if isinstance(run["outage"], dict) else "unknown"
        issues.append(f"declared storage outage did not complete: {status}")
    try:
        start, end = time_ns(summary.get("measurement_start")), time_ns(summary.get("measurement_end"))
        result["measurement_window"] = {"from": summary["measurement_start"], "to": summary["measurement_end"], "half_open": True}
        result["metrics"] = analyze_events(events, config, start, end, issues)
        result["resources"] = analyze_resources(records, environment, start, end, issues)
        result["targets"] = assess_targets(result["metrics"])
    except (ValueError, TypeError, KeyError) as error:
        issues.append(f"measurement boundaries unavailable: {error}")
    before = read_json(directory / "storage-before.json", issues)
    after = read_json(directory / "storage-after.json", issues)
    initial, final = storage_totals(before or {}, issues), storage_totals(after or {}, issues)
    result["storage"] = {"before": initial, "after": final, "delta": {key: final[key] - initial[key] for key in initial} if initial is not None and final is not None else None, "note": "Global retained-corpus physical rows/parts/bytes include background merges and other retained runs; they are not unique identity reconciliation."}
    ledger = read_jsonl(directory / "harness/reconciliation.jsonl", issues)
    result["reconciliation"] = analyze_reconciliation(ledger, config, events, issues)
    if not summary.get("reconciliation", {}).get("verified") or not summary.get("reconciliation", {}).get("settled"):
        issues.append("reconciliation snapshot incomplete or Collector settling unverified")
        result["reconciliation"]["status"] = "unknown"
        result["reconciliation"]["zero_identity_loss"] = "unknown"
    result["validity"] = "incomplete" if issues else "raw_evidence_available"
    return result


def steady_variation(stages):
    result = {}
    steady = [row for row in stages if str(row["name"]).lower().startswith("steady") and row.get("metrics")]
    for name, getter in {"acknowledged_spans_per_second": lambda row: row["metrics"]["acknowledged_within_window_spans_per_second"], "ack_p95_ns": lambda row: row["metrics"]["acknowledgment_from_first_send"]["p95_ns"], "ack_p99_ns": lambda row: row["metrics"]["acknowledgment_from_first_send"]["p99_ns"]}.items():
        values = [getter(row) for row in steady if getter(row) is not None]
        result[name] = {"count": len(values), "min": min(values, default=None), "max": max(values, default=None), "mean": statistics.mean(values) if values else None, "sample_stddev": statistics.stdev(values) if len(values) >= 2 else None}
    return {"required_repetitions": 3, "observed_stages": len(steady), "metrics": result, "note": "Runs remain separate; differences include the growing retained corpus and background workload."}


def markdown(report):
    lines = ["# Raw benchmark analysis", "", "All rows come from retained observations; unknowns do not pass targets.", "", "| Stage | Validity | Offered spans/s | ACK within window spans/s | ACK p95 / p99 ms | Retries | Terminal batches |", "| --- | --- | ---: | ---: | ---: | ---: | ---: |"]
    def number(value):
        return "unknown" if value is None else f"{value:.2f}"
    for row in report["stages"]:
        metrics = row.get("metrics") or {}
        latency = metrics.get("acknowledgment_from_first_send", {})
        ns = lambda key: latency.get(key) / 1e6 if latency.get(key) is not None else None
        lines.append(f"| {row['name']} | {row['validity']} | {number(metrics.get('offered_spans_per_second'))} | {number(metrics.get('acknowledged_within_window_spans_per_second'))} | {number(ns('p95_ns'))} / {number(ns('p99_ns'))} | {metrics.get('retry_attempts', 'unknown')} | {metrics.get('terminal_measured_batches', 'unknown')} |")
    lines += ["", "Latency percentiles use sorted index floor(n×p/100), capped at n−1. Successful and failed API distributions, sample counts, censored visibility and resource observations remain in analysis.json. A small p99 sample is not a robust capacity claim.", ""]
    for row in report["stages"]:
        if row["issues"]:
            lines += [f"## {row['name']} evidence gaps", ""] + [f"- {issue}" for issue in sorted(set(row["issues"]))] + [""]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("campaign", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    issues = []
    campaign = read_json(arguments.campaign / "campaign.json", issues) or {}
    environment = read_json(arguments.campaign / "environment.json", issues) or {}
    directories = sorted(path.parent for path in arguments.campaign.glob("*/run.json"))
    stages = [analyze_stage(path, environment) for path in directories]
    if not directories:
        issues.append("no stage run.json files found")
    report = {"schema_version": 1, "campaign_status": campaign.get("status", "unknown"), "source_campaign": str(arguments.campaign.resolve()), "issues": issues, "stages": stages, "steady_variation": steady_variation(stages), "note": "Targets are the predeclared WORKLOAD.md targets. No status from a harness summary is treated as proof of measured success."}
    arguments.output.mkdir(parents=True, exist_ok=True)
    (arguments.output / "analysis.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    (arguments.output / "tables.md").write_text(markdown(report))
    return int(bool(issues) or any(row["validity"] == "incomplete" for row in stages))


if __name__ == "__main__":
    raise SystemExit(main())
