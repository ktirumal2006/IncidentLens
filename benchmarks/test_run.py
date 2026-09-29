import json
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

import environment
import run


class Buffer:
    def __init__(self):
        self.values = []

    def write(self, value):
        self.values.append(value)


class ImmediateEvent:
    def wait(self, _):
        return False

    def is_set(self):
        return False


class PlanTests(unittest.TestCase):
    def plan(self):
        return {"version": 1, "declaration": "benchmarks/WORKLOAD.md", "stages": [
            {"name": "steady", "timeout_seconds": 180, "workload": {"rate": 500, "warmup": "30s", "duration": "120s"}}]}

    def test_committed_plan_matches_bounded_workload(self):
        plan = run.validate_plan(json.loads((environment.ROOT / "benchmarks/plan.json").read_text()))
        self.assertEqual([s["name"] for s in plan["stages"]], ["mixed", "steady", "outage", "ramp-500", "ramp-2000", "ramp-8000", "ramp-32000", "ramp-100000"])
        self.assertEqual(plan["stages"][1]["repeat"], 3)
        self.assertEqual(plan["stages"][0]["workload"]["baseline-traces"], 128)
        self.assertEqual(plan["stages"][2]["outage"], {"after_seconds": 30, "duration_seconds": 15})

    def test_unsafe_plans_rejected(self):
        for mutation in (
            lambda p: p.update(version=2),
            lambda p: p["stages"][0].update(name="../escape"),
            lambda p: p["stages"][0].update(repeat=4),
            lambda p: p["stages"][0].update(timeout_seconds=0),
            lambda p: p["stages"][0]["workload"].update(rate=100000),
            lambda p: p["stages"][0]["workload"].update(**{"output-dir": "escape"}),
            lambda p: p["stages"][0].update(outage={"after_seconds": 200, "duration_seconds": 15}),
        ):
            plan = self.plan()
            mutation(plan)
            with self.subTest(plan=plan), self.assertRaises(ValueError):
                run.validate_plan(plan)

    def test_command_does_not_use_shell_or_split_values(self):
        stage = self.plan()["stages"][0]
        stage["workload"]["query-mix"] = "services,traces,detail,incidents"
        command = run.bench_command("/tmp/bench binary", stage, "seed", Path("/tmp/result space"))
        self.assertEqual(command[0], "/tmp/bench binary")
        self.assertIn("/tmp/result space", command)
        self.assertIn("services,traces,detail,incidents", command)


class MeasurementTests(unittest.TestCase):
    def summary(self):
        return {"measured": {"terminal_error_fraction": 0, "terminal_failed_batches": 0,
                "acked_spans_per_second": 500, "ack_latency": {"count": 100, "p95_ns": 100, "p99_ns": 200}}, "driver": {}}

    def test_driver_drops_do_not_prove_product_saturation(self):
        summary = self.summary()
        summary["driver"]["schedule_dropped_batches"] = 100
        summary["measured"]["acked_spans_per_second"] = 100
        observed = run.classify_ramp(summary, 500)
        self.assertTrue(observed["driver_limit"])
        self.assertFalse(observed["product_saturated"])

    def test_product_breach_requires_exact_boundary_or_evidence(self):
        summary = self.summary()
        summary["measured"]["terminal_error_fraction"] = .01
        summary["measured"]["ack_latency"]["p99_ns"] = 2_000_000_000
        self.assertFalse(run.classify_ramp(summary, 500)["product_saturated"])
        summary["measured"]["terminal_error_fraction"] = .01001
        self.assertTrue(run.classify_ramp(summary, 500)["product_saturated"])
        summary = self.summary()
        summary["measured"]["acked_spans_per_second"] = 474
        summary["measured"]["terminal_failed_batches"] = 1
        self.assertTrue(run.classify_ramp(summary, 500)["product_saturated"])

    def test_missing_summary_is_not_sustainable_result(self):
        self.assertEqual(run.classify_ramp({}, 500)["status"], "unavailable")

    def test_raw_stats_and_disk_parse_failures_remain_visible(self):
        self.assertEqual(run.memory_percentages({"stdout": '{"Name":"api","MemPerc":"98.01%"}\nnot-json'}), {"api": 98.01})
        self.assertIsNone(run.disk_bytes({"clickhouse_parts": {"stdout": "bad"}}))
        self.assertEqual(run.disk_bytes({"clickhouse_parts": {"stdout": '{"data":[{"disk_bytes":"42"}]}'}}), 42)

    def test_failed_storage_preflight_never_starts_process(self):
        with tempfile.TemporaryDirectory() as temporary, patch.object(environment, "storage_snapshot", return_value={}), patch.object(environment, "container_ids", return_value=([], {"returncode": 1})), patch.object(subprocess, "Popen") as popen:
            result = run.run_stage("unused", {"name": "x"}, "seed", Path(temporary)/"stage", 2)
            self.assertEqual(result["status"], "preflight_failed")
            popen.assert_not_called()
            self.assertTrue((Path(temporary)/"stage/run.json").is_file())

    def test_three_readiness_failures_are_required(self):
        observed = {"docker_stats": {"returncode": 0, "stdout": '{"Name":"api","MemPerc":"20%"}'},
                    "container_health": {"returncode": 0, "stdout": '"api" 2 {"OOMKilled":false}'}}
        safety, records = {}, Buffer()
        with tempfile.TemporaryDirectory() as temporary, patch.object(environment, "sample", return_value=observed), patch.object(run, "readiness", return_value={"api": "503", "ingestion": 200}), patch.object(run.shutil, "disk_usage", return_value=type("Disk", (), {"free": 20*(1<<30)})()):
            run.sampler(1, ["id"], ImmediateEvent(), records, 2, safety, threading.Event(), 0, Path(temporary), {'"api"': 2})
        self.assertEqual(safety["reason"], "three_consecutive_readiness_failures_outside_outage")
        self.assertEqual(len(safety["failed_samples"]), 3)
        self.assertEqual(len([v for v in records.values if v.get("kind") == "readiness"]), 3)

    def test_missing_resource_observation_fails_closed(self):
        observed = {"docker_stats": {"returncode": 1, "stderr": "failed"}, "container_health": {"returncode": 1}}
        safety = {}
        with tempfile.TemporaryDirectory() as temporary, patch.object(environment, "sample", return_value=observed), patch.object(run, "readiness", return_value={"api": 200, "ingestion": 200}), patch.object(run.shutil, "disk_usage", return_value=type("Disk", (), {"free": 20*(1<<30)})()):
            run.sampler(1, ["id"], ImmediateEvent(), Buffer(), 2, safety, threading.Event(), 0, Path(temporary), {})
        self.assertEqual(safety["reason"], "safety_observation_failed")


class CleanupTests(unittest.TestCase):
    def test_outage_restores_after_failed_stop(self):
        records, result, active = Buffer(), {}, threading.Event()
        with patch.object(environment, "command", side_effect=[{"returncode": 1}, {"returncode": 0}]) as command, patch.object(run, "readiness", return_value={"api": 200, "ingestion": 200}):
            run.outage_worker({"after_seconds": 0, "duration_seconds": 15}, ImmediateEvent(), records, result, active)
        self.assertEqual(result["status"], "failed")
        self.assertIn("start", command.call_args_list[-1].args[0])
        self.assertFalse(active.is_set())
        self.assertEqual(records.values[-1]["kind"], "outage_restore")

    def test_outage_cancelled_before_start_does_not_mutate(self):
        stop, records, result = threading.Event(), Buffer(), {}
        stop.set()
        with patch.object(environment, "command") as command:
            run.outage_worker({"after_seconds": 1, "duration_seconds": 15}, stop, records, result, threading.Event())
        command.assert_not_called()
        self.assertEqual(result["status"], "not_started")

    def test_process_group_termination_leaves_no_live_harness(self):
        process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"], start_new_session=True)
        try:
            run.terminate(process)
            self.assertIsNotNone(process.poll())
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()

    def test_load_start_reader_skips_partial_lines(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary)/"events.jsonl"
            path.write_text('bad\n'+json.dumps({"kind":"load_start", "started":"2026-09-28T00:00:00Z"})+'\n{')
            value = run.read_load_start(path)
            self.assertEqual(value[0], "2026-09-28T00:00:00Z")
            self.assertEqual(value[1], 1790553600)

    def test_forced_termination_is_explicitly_unverified(self):
        from unittest.mock import Mock
        process = Mock(pid=123, returncode=-9)
        process.poll.side_effect = [None, -9]
        process.wait.side_effect = [subprocess.TimeoutExpired("bench", 100), -9]
        with patch.object(run.os, "killpg") as kill:
            value = run.terminate(process)
        self.assertTrue(value["forced_kill"])
        self.assertTrue(value["reconciliation_unverified"])
        self.assertEqual(process.wait.call_args_list[0].kwargs["timeout"], 100)
        self.assertEqual(kill.call_count, 2)

    def test_command_timeout_keeps_partial_raw_output(self):
        with patch.object(subprocess, "run", side_effect=subprocess.TimeoutExpired(["x"], 1, output=b"partial", stderr=b"failure")):
            value = environment.command(["x"], timeout=1)
        self.assertIsNone(value["returncode"])
        self.assertEqual(value["stdout"], "partial")
        self.assertEqual(value["stderr"], "failure")


if __name__ == "__main__":
    unittest.main()
