#!/usr/bin/env python3
"""Finite, local benchmark orchestration. Plans and raw observations are retained."""
import argparse
import datetime
import json
import os
import re
import signal
import shutil
import subprocess
import threading
import time
import urllib.request
import uuid
from pathlib import Path

import environment

FLAGS = {"rate", "duration", "warmup", "concurrency", "batch-traces", "queue", "attempts", "export-timeout", "query-concurrency", "query-rate", "query-mix", "attribute-bytes", "baseline-traces", "drain", "settle", "collector", "api", "clickhouse", "visibility-every", "visibility-timeout", "max-event-bytes"}


def seconds(value):
    match = re.fullmatch(r"(\d+(?:\.\d+)?)(ms|s|m|h)", str(value))
    if not match:
        raise ValueError("duration must use one positive Go unit ms/s/m/h")
    return float(match[1]) * {"ms": .001, "s": 1, "m": 60, "h": 3600}[match[2]]


def save(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def validate_plan(plan):
    if plan.get("version") != 1 or not isinstance(plan.get("declaration"), str):
        raise ValueError("plan requires version=1 and declaration path to predeclared workload")
    stages = plan.get("stages")
    if not isinstance(stages, list) or not 1 <= len(stages) <= 30:
        raise ValueError("plan must contain 1..30 bounded stages")
    names = set()
    for stage in stages:
        name = stage.get("name", "")
        if not name or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for c in name) or name in names:
            raise ValueError("stage names must be unique ASCII letters/digits/dash/underscore")
        names.add(name)
        if not 1 <= stage.get("repeat", 1) <= 3:
            raise ValueError("repeat must be 1..3")
        if not 1 <= stage.get("timeout_seconds", 0) <= 3600:
            raise ValueError("each stage requires timeout_seconds in 1..3600")
        workload = stage.get("workload", {})
        if not isinstance(workload, dict) or not {"rate", "duration", "warmup"} <= workload.keys() or workload.keys() - FLAGS:
            raise ValueError("stage workload requires rate/duration/warmup and only supported bench flags")
        if any(not isinstance(v, (str, int, float)) or isinstance(v, bool) for v in workload.values()):
            raise ValueError("workload flag values must be strings or numbers")
        rate = float(workload["rate"])
        planned = rate * (seconds(workload["duration"]) + seconds(workload["warmup"])) + int(workload.get("baseline-traces", 0)) * 6
        if rate <= 0 or seconds(workload["duration"]) <= 0 or planned > 2_000_000:
            raise ValueError("stage exceeds positive rate/duration or two-million planned-span cap")
        outage = stage.get("outage")
        if outage is not None:
            if not isinstance(outage, dict) or set(outage) != {"after_seconds", "duration_seconds"}:
                raise ValueError("outage requires after_seconds and duration_seconds")
            if not 0 <= outage["after_seconds"] < stage["timeout_seconds"] or not 1 <= outage["duration_seconds"] <= 60:
                raise ValueError("outage schedule exceeds bounds")
    return plan


def bench_command(binary, stage, run_id, output):
    args = [str(binary), "--output-dir", str(output), "--seed", run_id, "--scenario", stage["name"]]
    for key, value in sorted(stage["workload"].items()):
        args += ["--" + key, str(value)]
    return args


class Records:
    def __init__(self, path):
        self.file = path.open("w")
        self.lock = threading.Lock()

    def write(self, value):
        with self.lock:
            self.file.write(json.dumps(value, sort_keys=True) + "\n")
            self.file.flush()

    def close(self):
        self.file.close()


def memory_percentages(observation):
    result = {}
    for line in observation.get("stdout", "").splitlines():
        try:
            value = json.loads(line)
            result[value["Name"]] = float(value["MemPerc"].rstrip("%"))
        except (ValueError, KeyError, TypeError):
            continue
    return result


def disk_bytes(snapshot):
    try:
        return sum(int(row["disk_bytes"]) for row in json.loads(snapshot["clickhouse_parts"]["stdout"])["data"])
    except (ValueError, KeyError, TypeError):
        return None


def restart_baseline(observation):
    counts = {}
    for line in observation.get("stdout", "").splitlines():
        name, restarts, _ = line.split(" ", 2)
        counts[name] = int(restarts)
    return counts


def classify_ramp(summary, rate):
    measured, driver = summary.get("measured", {}), summary.get("driver", {})
    required = {"terminal_error_fraction", "acked_spans_per_second", "ack_latency", "terminal_failed_batches"}
    if not required <= measured.keys():
        return {"status": "unavailable", "error": "required measured summary fields missing"}
    reasons = []
    latency = measured["ack_latency"]
    if measured["terminal_error_fraction"] > .01:
        reasons.append("terminal_export_error_fraction_above_1_percent")
    if latency.get("count", 0) and latency.get("p99_ns", 0) > 2_000_000_000:
        reasons.append("ack_p99_above_2_seconds")
    server_evidence = measured["terminal_failed_batches"] > 0 or measured.get("retried_batches", 0) > 0 or latency.get("p95_ns", 0) > 500_000_000
    if measured["acked_spans_per_second"] < .95 * rate and server_evidence:
        reasons.append("ack_rate_below_95_percent_with_export_or_backpressure_evidence")
    driver_limit = bool(driver.get("schedule_dropped_batches", 0) or driver.get("missed_deadline_batches", 0) or driver.get("logging_error"))
    return {"status": "observed", "product_saturated": bool(reasons), "product_reasons": reasons,
            "driver_limit": driver_limit, "low_ack_rate": measured["acked_spans_per_second"] < .95 * rate}


def sampler(pid, ids, stop, records, interval, safety, outage_active, initial_disk, directory, baseline_restarts):
    next_sample = time.monotonic()
    high_memory_since = {}
    last_disk = time.monotonic()
    restart_counts = dict(baseline_restarts)
    readiness_failures = []
    while not stop.is_set():
        observed = environment.sample(pid, ids)
        records.write(observed)
        now = time.monotonic()
        # Docker omits stopped containers from stats during the declared outage.
        names = set(memory_percentages(observed["docker_stats"]))
        expected = {name.strip('"').lstrip('/') for name in baseline_restarts}
        missing = expected - names
        allowed_missing = {"incidentlens-clickhouse-1"} if outage_active.is_set() else set()
        if observed["docker_stats"].get("returncode") != 0 or missing - allowed_missing or names - expected or observed["container_health"].get("returncode") != 0:
            safety.update(reason="safety_observation_failed", observation=observed)
        for name, percentage in memory_percentages(observed["docker_stats"]).items():
            if percentage >= 98:
                high_memory_since.setdefault(name, now)
                if now - high_memory_since[name] >= 10:
                    safety.update(reason="container_memory_98_percent_for_10_seconds", container=name)
            else:
                high_memory_since.pop(name, None)
        health = observed.get("container_health", {}).get("stdout", "")
        for line in health.splitlines():
            try:
                name, restarts, state = line.split(" ", 2)
                state = json.loads(state)
                restart_counts.setdefault(name, int(restarts))
                if state.get("OOMKilled") or int(restarts) > restart_counts[name]:
                    safety.update(reason="container_oom_or_restart", container=name, state=state, restart_count=int(restarts))
            except (ValueError, TypeError):
                records.write({"kind": "sampling_parse_error", "at": environment.utc(), "raw": line})
                safety.update(reason="invalid_container_health_observation")
        free = shutil.disk_usage(environment.ROOT).free
        records.write({"kind": "host_disk", "at": environment.utc(), "free_bytes": free})
        if free < 10 * (1 << 30):
            safety.update(reason="host_free_disk_below_10_GiB", free_bytes=free)
        event_size = sum(p.stat().st_size for p in directory.rglob("*.jsonl") if p.is_file())
        if event_size >= 256 * (1 << 20):
            safety.update(reason="raw_event_output_256_MiB_cap", bytes=event_size)
        if not outage_active.is_set():
            ready = readiness(timeout=2)
            records.write({"kind": "readiness", "at": environment.utc(), "observations": ready})
            if ready != {"api": 200, "ingestion": 200}:
                readiness_failures.append({"at": environment.utc(), "observations": ready})
                if len(readiness_failures) >= 3:
                    safety.update(reason="three_consecutive_readiness_failures_outside_outage", failed_samples=list(readiness_failures))
            else:
                readiness_failures = []
            if now - last_disk >= 10:
                snapshot = environment.storage_snapshot()
                records.write({"kind": "storage_sample", **snapshot})
                size = disk_bytes(snapshot)
                if size is not None and initial_disk is not None and size - initial_disk > 4 * (1 << 30):
                    safety.update(reason="clickhouse_disk_growth_above_4_GiB", growth_bytes=size-initial_disk)
                last_disk = time.monotonic()
        else:
            readiness_failures = []
        if safety:
            records.write({"kind": "safety_stop", "at": environment.utc(), **safety})
            break
        next_sample += interval
        stop.wait(max(0, next_sample - time.monotonic()))
        if next_sample < time.monotonic() - interval:
            next_sample = time.monotonic()


def read_load_start(path):
    try:
        with path.open() as source:
            for line in source:
                try:
                    event = json.loads(line)
                    if event.get("kind") == "load_start":
                        value = event["started"]
                        return value, datetime.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
                except (ValueError, KeyError, TypeError):
                    continue
    except OSError:
        pass
    return None


def outage_worker(schedule, stop, records, result, active, events_path=None):
    if events_path is not None:
        while not stop.is_set():
            load = read_load_start(events_path)
            if load is not None:
                result["load_start"] = load[0]
                delay = max(0, load[1] + schedule["after_seconds"] - time.time())
                break
            stop.wait(.1)
        else:
            delay = 0
    else:
        delay = schedule["after_seconds"]
    if stop.is_set() or stop.wait(delay):
        result.update(status="not_started", reason="harness ended before scheduled outage")
        records.write({"kind": "outage", "at": environment.utc(), **result})
        return
    result.update(status="running", requested=schedule, started_at=environment.utc())
    if events_path is not None:
        result["actual_stop_command_offset_seconds"] = time.time() - load[1]
        result["offset_reference"] = "harness load_start"
    active.set()
    try:
        observed = environment.command(["docker", "compose", "-f", str(environment.COMPOSE), "stop", "-t", "1", "clickhouse"], timeout=20)
        result["stop_command"] = observed
        records.write({"kind": "outage_stop", **observed})
        if observed.get("returncode") != 0:
            raise RuntimeError("ClickHouse stop failed or timed out; restore still required")
        down = time.monotonic()
        ended_early = stop.wait(schedule["duration_seconds"])
        result.update(observed_stop_seconds=time.monotonic() - down, harness_ended_during_outage=ended_early)
        result["status"] = "shortened" if ended_early else "completed"
    except Exception as exc:
        result.update(status="failed", error=repr(exc))
    finally:
        restored = environment.command(["docker", "compose", "-f", str(environment.COMPOSE), "start", "clickhouse"], timeout=30)
        result["restore_command"] = restored
        result["restored_at"] = environment.utc()
        if restored.get("returncode") != 0:
            result["status"] = "restore_failed"
        elif restored.get("returncode") == 0:
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                result["recovery_readiness"] = readiness(timeout=2)
                if result["recovery_readiness"] == {"api": 200, "ingestion": 200}:
                    break
                time.sleep(1)
            if result["recovery_readiness"] != {"api": 200, "ingestion": 200}:
                result["status"] = "recovery_failed"
        active.clear()
        records.write({"kind": "outage_restore", "at": environment.utc(), **result})


def terminate(process, grace_seconds=100):
    result = {"requested_at": environment.utc(), "grace_seconds": grace_seconds, "forced_kill": False}
    if process.poll() is not None:
        return {**result, "already_terminal": True}
    os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=grace_seconds)
    except subprocess.TimeoutExpired:
        result["forced_kill"] = True
        result["reconciliation_unverified"] = True
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=5)
    result.update(ended_at=environment.utc(), terminal=process.poll() is not None, returncode=process.returncode)
    return result


def run_stage(binary, stage, seed, directory, interval, campaign_initial_disk=None):
    directory.mkdir()
    result = {"name": stage["name"], "seed": seed, "started_at": environment.utc(), "status": "running"}
    initial_snapshot = environment.storage_snapshot()
    save(directory / "storage-before.json", initial_snapshot)
    ids, inventory = environment.container_ids()
    save(directory / "containers-before.json", inventory)
    if disk_bytes(initial_snapshot) is None or inventory.get("returncode") != 0 or len(ids) != 4 or initial_snapshot.get("host_disk", {}).get("free_bytes", 0) < 10 * (1 << 30):
        result.update(status="preflight_failed", error="storage snapshot or four product container IDs unavailable; no load started", ended_at=environment.utc())
        save(directory / "run.json", result)
        return result
    args = bench_command(binary, stage, seed, directory / "harness")
    result["command"] = args
    save(directory / "run.json", result)
    records = Records(directory / "resources.jsonl")
    idle = environment.sample(os.getpid(), ids)
    records.write({"kind": "idle_sample", "observation": idle})
    if idle["docker_stats"].get("returncode") != 0 or len(memory_percentages(idle["docker_stats"])) != 4 or idle["container_health"].get("returncode") != 0:
        result.update(status="preflight_failed", error="idle safety observations unavailable; no load started", ended_at=environment.utc())
        records.close()
        save(directory / "run.json", result)
        return result
    stop = threading.Event()
    outage_active = threading.Event()
    safety = {}
    process = None
    workers = []
    started = time.monotonic()
    try:
        with (directory / "stdout.jsonl").open("w") as stdout, (directory / "stderr.txt").open("w") as stderr:
            process = subprocess.Popen(args, cwd=environment.ROOT, stdout=stdout, stderr=stderr, start_new_session=True)
            result["pid"] = process.pid
            safety_disk = disk_bytes(initial_snapshot) if campaign_initial_disk is None else campaign_initial_disk
            workers.append(threading.Thread(target=sampler, args=(process.pid, ids, stop, records, interval, safety, outage_active, safety_disk, directory, restart_baseline(idle["container_health"]))))
            if "outage" in stage:
                result["outage"] = {}
                workers.append(threading.Thread(target=outage_worker, args=(stage["outage"], stop, records, result["outage"], outage_active, directory / "harness/events.jsonl")))
            for worker in workers:
                worker.start()
            try:
                deadline = started + stage["timeout_seconds"]
                while process.poll() is None:
                    if safety:
                        result["safety_stop"] = dict(safety)
                        stop.set()
                        result["termination"] = terminate(process)
                        break
                    stop.wait(min(.5, max(.01, deadline-time.monotonic())))
                    if time.monotonic() >= deadline and process.poll() is None:
                        raise subprocess.TimeoutExpired(args, stage["timeout_seconds"])
                result["returncode"] = process.returncode
                result["status"] = "completed" if result["returncode"] == 0 else "harness_failed"
                if safety:
                    result["status"] = "safety_stopped"
            except subprocess.TimeoutExpired:
                result["status"] = "timed_out"
                stop.set()
                result["termination"] = terminate(process)
                result["returncode"] = process.returncode
    except BaseException as exc:
        result.update(status="interrupted" if isinstance(exc, KeyboardInterrupt) else "runner_failed", error=repr(exc))
        if process is not None:
            stop.set()
            result["termination"] = terminate(process)
        if isinstance(exc, KeyboardInterrupt):
            result["interrupted"] = True
    finally:
        stop.set()
        for worker in workers:
            # Stop/start commands plus bounded dependency recovery can consume
            # up to 85 seconds. Do not close its raw log while it restores.
            worker.join(timeout=100)
            if worker.is_alive():
                result["status"] = "cleanup_failed"
                result["cleanup_error"] = "resource/outage thread failed to terminate within bounded wait"
        records.close()
        result.update(ended_at=environment.utc(), elapsed_seconds=time.monotonic() - started)
        result["harness_terminal"] = process is None or process.poll() is not None
        if not result["harness_terminal"]:
            result["status"] = "cleanup_failed"
        summary = directory / "harness/summary.json"
        try:
            result["summary"] = json.loads(summary.read_text())
        except (OSError, ValueError) as exc:
            result["summary_error"] = repr(exc)
            if result["status"] == "completed":
                result["status"] = "missing_summary"
        if result.get("outage", {}).get("status") not in (None, "completed"):
            result["status"] = "outage_failed"
        save(directory / "storage-after.json", environment.storage_snapshot())
        save(directory / "run.json", result)
    return result


def readiness(timeout=5):
    observations = {}
    for name, address in (("api", "http://127.0.0.1:18081/readyz"), ("ingestion", "http://127.0.0.1:18080/readyz")):
        try:
            with urllib.request.urlopen(address, timeout=timeout) as response:
                observations[name] = response.status
        except Exception as exc:
            observations[name] = repr(exc)
    return observations


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--sample-interval", type=float, default=2)
    args = parser.parse_args()
    if not 1 <= args.sample_interval <= 30:
        parser.error("sample interval must be 1..30 seconds")
    plan = validate_plan(json.loads(args.plan.read_text()))
    declaration = (environment.ROOT / plan["declaration"]).resolve()
    if not declaration.is_file():
        parser.error("predeclared workload document must exist")
    binary = args.binary.resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        parser.error("binary must be an existing executable; build before measurement")
    args.output.mkdir(parents=True, exist_ok=False)
    campaign = {"started_at": environment.utc(), "status": "running", "plan": plan,
                "declaration": {"path": str(declaration), "sha256": environment.digest(declaration)},
                "plan_sha256": environment.digest(args.plan), "stages": []}
    save(args.output / "campaign.json", campaign)
    try:
        provenance = environment.environment(binary)
        save(args.output / "environment.json", provenance)
        if len(provenance["containers"]) != 4 or any("image_id" not in c for c in provenance["containers"]) or provenance["source_manifest"]["git_inventory_status"] != [0, 0] or "error" in provenance["docker_vm"]:
            raise RuntimeError("source/runtime provenance unavailable; no workload started")
        campaign["preflight_readiness"] = readiness()
        if campaign["preflight_readiness"] != {"api": 200, "ingestion": 200}:
            raise RuntimeError("product not ready; no workload started")
        campaign["initial_storage"] = environment.storage_snapshot()
        initial_disk = disk_bytes(campaign["initial_storage"])
        if initial_disk is None:
            raise RuntimeError("initial storage safety baseline unavailable; no workload started")
        ramp_saturated = False
        for stage in plan["stages"]:
            if stage.get("group") == "ramp" and ramp_saturated:
                campaign["stages"].append({"name": stage["name"], "status": "skipped_after_saturation"})
                save(args.output / "campaign.json", campaign)
                continue
            for repetition in range(stage.get("repeat", 1)):
                label = stage["name"] + f"-r{repetition + 1}"
                seed = uuid.uuid4().hex + "-" + label
                print(f"START {label} {environment.utc()}", flush=True)
                result = run_stage(binary, stage, seed, args.output / label, args.sample_interval, initial_disk)
                result["repetition"] = repetition + 1
                campaign["stages"].append(result)
                save(args.output / "campaign.json", campaign)
                print(f"END {label} {result['status']}", flush=True)
                if result.get("interrupted"):
                    raise KeyboardInterrupt()
                if result.get("safety_stop") or result.get("outage", {}).get("status") in ("restore_failed", "recovery_failed"):
                    raise RuntimeError("safety boundary or failed storage recovery; campaign stopped")
                if result["status"] in ("preflight_failed", "cleanup_failed"):
                    raise RuntimeError("measurement safety/provenance unavailable; campaign stopped")
                if stage.get("group") == "ramp":
                    result["ramp_assessment"] = classify_ramp(result.get("summary", {}), float(stage["workload"]["rate"]))
                    save(args.output / label / "run.json", result)
                    save(args.output / "campaign.json", campaign)
                    if result["ramp_assessment"]["status"] != "observed":
                        raise RuntimeError("ramp summary schema unavailable; capacity assessment not possible")
                    ramp_saturated = result["ramp_assessment"]["product_saturated"] or result["status"] != "completed"
        campaign["status"] = "completed" if all(s["status"] in ("completed", "skipped_after_saturation") for s in campaign["stages"]) else "failed"
    except BaseException as exc:
        campaign.update(status="interrupted" if isinstance(exc, KeyboardInterrupt) else "failed", error=repr(exc))
    finally:
        campaign["ended_at"] = environment.utc()
        campaign["final_readiness"] = readiness()
        if campaign["final_readiness"] != {"api": 200, "ingestion": 200}:
            campaign["status"] = "failed"
        save(args.output / "campaign.json", campaign)
    return 0 if campaign["status"] == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
