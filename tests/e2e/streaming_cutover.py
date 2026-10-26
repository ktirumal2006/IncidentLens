#!/usr/bin/env python3
"""UNVERIFIED, clean default startup and isolated v1 -> v2 -> direct -> v2 rehearsal.

Requires prebuilt Go bench/bench-lag binaries, explicit existing service images,
Docker Compose and a promoted root compose.yaml. Never uses the incidentlens
project or deletes volumes. See
docs/STREAMING_CUTOVER.md. This script does not audit the original global v1 log.
"""

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[2]
BASE = ROOT / "deploy/local/compose.yaml"
STREAM = ROOT / "deploy/streaming/compose.yaml"
HARDEN = ROOT / "deploy/streaming/hardening.yaml"
DEFAULT = ROOT / "compose.yaml"
PORTS = {"collector": 24317, "direct_grpc": 24318, "direct_health": 28080,
         "api": 28081, "ch_http": 28123, "ch_native": 29000,
         "producer_grpc": 24319, "producer_health": 28082,
         "worker_health": 28083, "kafka": 29092}
TOPICS = {"v1": ("incidentlens-spans-v1", "incidentlens-stream-worker-v1", 1),
          "v2": ("incidentlens-spans-v2", "incidentlens-stream-worker-v2", 4)}
PROJECT_RE = re.compile(r"^incidentlens-cutover-[a-z0-9]{8,24}$")


def utc():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def digest(path):
    sha = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1 << 20), b""):
            sha.update(block)
    return sha.hexdigest()


class Rehearsal:
    def __init__(self, project, output, bench, lag, images):
        self.project, self.output, self.bench, self.lag = project, output, bench, lag
        self.images = images
        self.image_ids = {}
        self.commands = []
        self.record = {"status": "running", "started_at": utc(), "project": project,
                       "ports": PORTS, "stages": [], "commands": self.commands,
                       "binaries": {"bench": {"path": str(bench), "sha256": digest(bench)},
                                    "bench_lag": {"path": str(lag), "sha256": digest(lag)}},
                       "compose_sources": {str(path.relative_to(ROOT)): digest(path)
                                           for path in (BASE, STREAM, HARDEN, DEFAULT)},
                       "selected_images": images}
        self.stream_overlay = output / "isolated-stream.yaml"
        self.direct_overlay = output / "isolated-direct.yaml"
        self.root_model = output / "resolved-root.compose.json"
        self.models = {
            "v1": [BASE, STREAM, self.stream_overlay],
            "v2": [BASE, STREAM, HARDEN, self.stream_overlay],
            "direct": [BASE, self.direct_overlay],
            "default": [self.root_model, self.stream_overlay],
        }
        self.active = None
        self.volumes = None
        self.seen = {}
        self.ledgers = {}
        self.flush()

    def flush(self):
        save(self.output / "acceptance.json", self.record)

    def command(self, args, label, timeout=30, check=True):
        started = utc()
        try:
            run = subprocess.run([str(a) for a in args], cwd=ROOT, capture_output=True,
                                 text=True, timeout=timeout)
            row = {"label": label, "command": [str(a) for a in args],
                   "started_at": started, "finished_at": utc(),
                   "returncode": run.returncode, "stdout": run.stdout, "stderr": run.stderr}
        except (OSError, subprocess.TimeoutExpired) as exc:
            row = {"label": label, "command": [str(a) for a in args],
                   "started_at": started, "finished_at": utc(), "error": repr(exc)}
        self.commands.append(row)
        self.flush()
        if check and row.get("returncode") != 0:
            raise RuntimeError(f"{label}: command failed; raw output retained")
        return row

    def compose(self, mode, args, label, timeout=60, check=True):
        files = self.models[mode]
        cmd = ["docker", "compose", "-p", self.project]
        for file in files:
            cmd += ["-f", file]
        return self.command(cmd + args, label, timeout, check)

    def inventory(self, label):
        row = self.command(["docker", "volume", "ls", "--filter",
                            f"label=com.docker.compose.project={self.project}",
                            "--format", "{{.Name}}"], label)
        names = sorted(row["stdout"].split())
        if names:
            self.command(["docker", "volume", "inspect", *names], label + "-inspect")
        return names

    def assert_volumes(self, label):
        names = self.inventory(label)
        expected = sorted([f"{self.project}_clickhouse-data", f"{self.project}_kafka-data"])
        if names != expected:
            raise RuntimeError(f"{label}: volume identity changed: {names}")
        if self.volumes is None:
            self.volumes = names
        elif names != self.volumes:
            raise RuntimeError(f"{label}: retained volumes differ")

    def write_overlay(self):
        # Separate direct and streaming overrides: an override must not define
        # services absent from its base model. Neither contains bind paths.
        common = f"""networks:
  telemetry:
    name: {self.project}-telemetry
services:
  clickhouse:
    ports: !override [\"127.0.0.1:{PORTS['ch_http']}:8123\", \"127.0.0.1:{PORTS['ch_native']}:9000\"]
  api:
    image: {self.images['api']}
    ports: !override [\"127.0.0.1:{PORTS['api']}:8081\"]
  collector:
    ports: !override [\"127.0.0.1:{PORTS['collector']}:4317\"]
"""
        self.direct_overlay.write_text(common + f"""  ingest:
    image: {self.images['ingest']}
    ports: !override [\"127.0.0.1:{PORTS['direct_grpc']}:4317\", \"127.0.0.1:{PORTS['direct_health']}:8080\"]
""")
        self.stream_overlay.write_text(common + f"""  ingest:
    image: {self.images['ingest']}
    ports: !override [\"127.0.0.1:{PORTS['direct_grpc']}:4317\", \"127.0.0.1:{PORTS['direct_health']}:8080\"]
  kafka:
    environment:
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:9092,HOST://localhost:{PORTS['kafka']}
    ports: !override [\"127.0.0.1:{PORTS['kafka']}:19092\"]
  stream-ingest:
    image: {self.images['stream_ingest']}
    ports: !override [\"127.0.0.1:{PORTS['producer_grpc']}:4318\", \"127.0.0.1:{PORTS['producer_health']}:8082\"]
  stream-worker:
    image: {self.images['stream_worker']}
    ports: !override [\"127.0.0.1:{PORTS['worker_health']}:8083\"]
""")

    def check_config(self):
        # Compare plain root include to the explicit model before introducing
        # isolation. Compose include imports a completed application model;
        # applying an override beside that include can conflict on service names.
        def plain(files, label):
            args = ["docker", "compose", "-p", self.project]
            for file in files:
                args += ["-f", file]
            return json.loads(self.command(args + ["config", "--format", "json"], label)["stdout"])
        plain_explicit = plain([BASE, STREAM, HARDEN], "config-plain-explicit-v2")
        plain_root = plain([DEFAULT], "config-plain-root-default")
        if plain_explicit != plain_root:
            raise RuntimeError("plain root default differs from explicit v2 model")
        save(self.root_model, plain_root)
        canonical = json.loads(self.compose("v2", ["config", "--format", "json"],
                                            "config-isolated-explicit-v2")["stdout"])
        proposed = json.loads(self.compose("default", ["config", "--format", "json"],
                                           "config-isolated-resolved-default")["stdout"])
        if canonical != proposed:
            raise RuntimeError("isolated resolved root differs from explicit v2 model")
        save(self.output / "resolved-v2.json", canonical)
        other = {}
        for mode in ("v1", "direct"):
            resolved = json.loads(self.compose(mode, ["config", "--format", "json"],
                                               f"config-{mode}")["stdout"])
            save(self.output / f"resolved-{mode}.json", resolved)
            other[mode] = resolved
        services = canonical["services"]
        if services["stream-ingest"]["environment"].get("KAFKA_TOPIC") != TOPICS["v2"][0] or \
           services["stream-worker"]["environment"].get("KAFKA_GROUP_ID") != TOPICS["v2"][1] or \
           services["api"]["environment"].get("INGESTION_MODE") != "streaming" or \
           "direct" not in services["ingest"].get("profiles", []):
            raise RuntimeError("resolved default does not select isolated v2")
        for mode, expected_mount in (("v1", "deploy/streaming/collector.yaml"),
                                     ("direct", "deploy/local/collector.yaml"),
                                     ("v2", "deploy/streaming/collector.yaml")):
            model = canonical if mode == "v2" else other[mode]
            mounts = model["services"]["collector"].get("volumes", [])
            if len(mounts) != 1 or not str(mounts[0].get("source", "")).endswith(expected_mount):
                raise RuntimeError(f"{mode}: Collector mount does not select expected exporter")
            if model["networks"]["telemetry"].get("name") != f"{self.project}-telemetry":
                raise RuntimeError(f"{mode}: network is not isolated")
        if other["v1"]["services"]["stream-worker"]["environment"].get("KAFKA_GROUP_ID") != TOPICS["v1"][1] or \
           other["direct"]["services"]["api"]["environment"].get("INGESTION_MODE") == "streaming":
            raise RuntimeError("historical v1 or direct reference configuration changed")

    def inspect_selected_images(self):
        for service, reference in self.images.items():
            row = self.command(["docker", "image", "inspect", reference, "--format", "{{.Id}}"],
                               f"prebuilt-image-{service}")
            image_id = row["stdout"].strip()
            if not image_id.startswith("sha256:"):
                raise RuntimeError(f"{service}: selected image lacks immutable local ID")
            self.image_ids[service] = image_id
        self.record["selected_image_ids"] = self.image_ids
        self.flush()

    def readiness(self, mode):
        ports = [PORTS["api"], PORTS["direct_health"] if mode == "direct" else PORTS["producer_health"],
                 *([] if mode == "direct" else [PORTS["worker_health"]])]
        deadline = time.monotonic() + 90
        for port in ports:
            while True:
                try:
                    with urllib.request.urlopen(f"http://127.0.0.1:{port}/readyz", timeout=2) as response:
                        if response.status == 200:
                            break
                except (OSError, urllib.error.HTTPError):
                    pass
                if time.monotonic() >= deadline:
                    raise RuntimeError(f"{mode} readiness timed out on {port}")
                time.sleep(1)
        self.record["stages"].append({"kind": "readiness", "mode": mode, "at": utc(), "ports": ports})
        self.flush()

    def start(self, mode):
        self.active = mode  # also quiesce a partially started owned project on failure
        self.compose(mode, ["up", "-d", "--no-build"], f"start-{mode}", 180)
        self.readiness(mode)
        self.assert_volumes(f"volumes-after-{mode}")
        self.compose(mode, ["ps", "--format", "json"], f"containers-{mode}")
        active = set(self.compose(mode, ["ps", "--services", "--status", "running"],
                                  f"running-services-{mode}")["stdout"].split())
        expected = ({"clickhouse", "api", "collector", "ingest"} if mode == "direct" else
                    {"clickhouse", "api", "collector", "kafka", "stream-ingest", "stream-worker"})
        if active != expected:
            raise RuntimeError(f"{mode}: unexpected running services {sorted(active)}")
        ids = self.compose(mode, ["ps", "-q"], f"container-ids-{mode}")["stdout"].split()
        if ids:
            row = self.command(["docker", "inspect", *ids], f"container-images-mounts-{mode}")
            observed = json.loads(row["stdout"])
            expected_images = {"api": "api", "ingest": "ingest", "stream-ingest": "stream_ingest",
                               "stream-worker": "stream_worker"}
            for container in observed:
                service = container.get("Config", {}).get("Labels", {}).get("com.docker.compose.service")
                if service in expected_images and container.get("Image") != self.image_ids[expected_images[service]]:
                    raise RuntimeError(f"{mode}: running {service} image differs from selected image ID")

    def fixture(self, name):
        seed = f"cutover-{name}-{uuid.uuid4().hex}"
        path = self.output / name
        cmd = [self.bench, "--output-dir", path, "--seed", seed, "--rate", "120",
               "--duration", "5s", "--warmup", "0s", "--batch-traces", "4",
               "--concurrency", "2", "--query-rate", "0", "--visibility-every", "1",
               "--settle", "3s", "--drain", "20s", "--collector",
               f"127.0.0.1:{PORTS['collector']}", "--api",
               f"http://127.0.0.1:{PORTS['api']}", "--clickhouse",
               f"127.0.0.1:{PORTS['ch_native']}"]
        self.command(cmd, f"fixture-{name}", 60)
        summary = json.loads((path / "summary.json").read_text())
        recon = summary.get("reconciliation") or {}
        measured = summary.get("measured") or {}
        if summary.get("error") or not recon.get("verified") or not recon.get("snapshot_verified") or \
           recon.get("acked_missing") != 0 or recon.get("duplicate_logical_identities") != 0 or \
           recon.get("unknown") != 0 or measured.get("acked_spans") != 600 or \
           recon.get("expected_stored") != recon.get("final_unique_spans"):
            raise RuntimeError(f"fixture {name} failed finite ACK/FINAL identity reconciliation")
        if not (path / "reconciliation.jsonl").is_file():
            raise RuntimeError(f"fixture {name} identity ledger missing")
        self.record["stages"].append({"kind": "fixture", "name": name, "seed": seed,
                                      "namespace": summary["config"]["namespace"],
                                      "acked": measured["acked_spans"], "at": utc()})
        for line in (path / "reconciliation.jsonl").read_text().splitlines():
            item = json.loads(line)
            if item.get("kind") == "expected_trace" and item.get("acked"):
                self.seen[name] = item["trace_id"]
                break
        if name not in self.seen:
            raise RuntimeError(f"fixture {name} lacks an acknowledged trace ID")
        expected = {}
        services = ("bench-frontend", "bench-api", "bench-store")
        for line in (path / "reconciliation.jsonl").read_text().splitlines():
            item = json.loads(line)
            if item.get("kind") != "expected_trace":
                raise RuntimeError(f"fixture {name} ledger has unexpected entry")
            if not item.get("acked") or item.get("expected_mask") != 63 or item.get("stored_mask") != 63:
                raise RuntimeError(f"fixture {name} ledger contains unresolved identity")
            trace_id = item.get("trace_id")
            if not isinstance(trace_id, str) or not re.fullmatch(r"[0-9a-f]{32}", trace_id):
                raise RuntimeError(f"fixture {name} ledger contains malformed trace ID")
            for n in range(1, 7):
                identity = (trace_id, f"{n:016x}")
                if identity in expected:
                    raise RuntimeError(f"fixture {name} ledger repeats an identity")
                expected[identity] = services[(n - 1) // 2]
        if len(expected) != 600:
            raise RuntimeError(f"fixture {name} ledger has {len(expected)} expected spans, not 600")
        self.ledgers[name] = {"namespace": summary["config"]["namespace"], "expected": expected}
        self.flush()
        return summary

    def reconcile_prior(self, mode, label):
        """Check every prior ACKed span identity and service in FINAL."""
        for fixture_name, ledger in self.ledgers.items():
            namespace = ledger["namespace"]
            if not re.fullmatch(r"bench-[0-9a-f]{12}", namespace):
                raise RuntimeError(f"{fixture_name}: malformed ledger namespace")
            sql = ("SELECT trace_id,span_id,service_name FROM incidentlens.spans FINAL "
                   f"WHERE service_namespace='{namespace}' ORDER BY trace_id,span_id "
                   "LIMIT 2001 SETTINGS max_execution_time=5 FORMAT JSONEachRow")
            row = self.compose(mode, ["exec", "-T", "clickhouse", "clickhouse-client",
                                      "--password", "local-admin", "--query", sql],
                               f"final-{label}-{fixture_name}", 15)
            actual_rows = [json.loads(line) for line in row["stdout"].splitlines()]
            actual = {}
            duplicate = []
            for item in actual_rows:
                identity = (item["trace_id"], item["span_id"])
                if identity in actual:
                    duplicate.append(identity)
                actual[identity] = item["service_name"]
            expected = ledger["expected"]
            missing = sorted(set(expected) - set(actual))
            unexpected = sorted(set(actual) - set(expected))
            wrong_service = sorted(identity for identity in set(expected) & set(actual)
                                   if expected[identity] != actual[identity])
            evidence = {"mode": mode, "transition": label, "fixture": fixture_name,
                        "namespace": namespace, "expected_count": len(expected),
                        "actual_rows": actual_rows, "missing": missing,
                        "unexpected": unexpected, "wrong_service": wrong_service,
                        "duplicate_logical_identities": sorted(duplicate),
                        "capped": len(actual_rows) >= 2001, "at": utc()}
            save(self.output / f"final-{label}-{fixture_name}.json", evidence)
            if missing or unexpected or wrong_service or duplicate or len(actual_rows) >= 2001:
                raise RuntimeError(f"{label}: {fixture_name} ACKed identity set changed")
        self.record["stages"].append({"kind": "final_identity_reconciliation",
                                      "mode": mode, "transition": label,
                                      "fixtures": sorted(self.ledgers), "at": utc()})
        self.flush()

    def check_trace(self, fixture_name, label):
        now = dt.datetime.now(dt.timezone.utc)
        query = urllib.parse.urlencode({"from": (now - dt.timedelta(hours=1)).isoformat(),
                                        "to": (now + dt.timedelta(minutes=1)).isoformat()})
        trace_id = self.seen[fixture_name]
        url = f"http://127.0.0.1:{PORTS['api']}/api/v1/traces/{trace_id}?{query}"
        with urllib.request.urlopen(url, timeout=5) as response:
            detail = json.load(response)
        save(self.output / f"{label}.json", detail)
        if detail.get("trace_id") != trace_id or len(detail.get("spans", [])) != 6 or detail.get("truncated"):
            raise RuntimeError(f"{label}: previously ACKed trace is not fully queryable")

    def topic_positions(self, mode, version, label):
        topic, group, count = TOPICS[version]
        shape = self.compose(mode, ["exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh",
                                    "--bootstrap-server", "kafka:9092", "--describe", "--topic", topic],
                             label + "-shape", 25)["stdout"]
        if not re.search(rf"PartitionCount:\s*{count}(?:\s|$)", shape) or \
           not re.search(r"ReplicationFactor:\s*1(?:\s|$)", shape):
            raise RuntimeError(f"{label}: unexpected topic partition count or replication factor")
        starts = self.compose(mode, ["exec", "-T", "kafka", "/opt/kafka/bin/kafka-get-offsets.sh",
                                     "--bootstrap-server", "kafka:9092", "--topic", topic, "--time", "-2"],
                              label + "-starts", 25)["stdout"]
        ends = self.compose(mode, ["exec", "-T", "kafka", "/opt/kafka/bin/kafka-get-offsets.sh",
                                   "--bootstrap-server", "kafka:9092", "--topic", topic, "--time", "-1"],
                            label + "-ends", 25)["stdout"]
        def parse(raw):
            found = {}
            for line in raw.splitlines():
                fields = line.split(":")
                if len(fields) != 3 or fields[0] != topic:
                    raise RuntimeError(f"{label}: malformed topic offset {line!r}")
                partition, offset = int(fields[1]), int(fields[2])
                if partition in found:
                    raise RuntimeError(f"{label}: duplicate partition")
                found[partition] = offset
            if set(found) != set(range(count)):
                raise RuntimeError(f"{label}: incomplete partition coverage")
            return found
        first, last = parse(starts), parse(ends)
        if any(first[p] < 0 or last[p] < first[p] for p in first):
            raise RuntimeError(f"{label}: retained offset gap or invalid log bounds")
        self.record["stages"].append({"kind": "offset_capture", "label": label,
                                      "topic": topic, "starts": first, "ends": last, "at": utc()})
        self.flush()
        return first, last

    def committed(self, mode, version, label):
        topic, group, count = TOPICS[version]
        row = self.compose(mode, ["exec", "-T", "kafka", "/opt/kafka/bin/kafka-consumer-groups.sh",
                                  "--bootstrap-server", "kafka:9092", "--group", group, "--describe"],
                           label, 25)
        found = {}
        for line in row["stdout"].splitlines():
            fields = line.split()
            if len(fields) >= 6 and fields[0] == group and fields[1] == topic:
                p = int(fields[2])
                if p in found or fields[3] == "-":
                    raise RuntimeError(f"{label}: duplicate or uninitialized commit")
                found[p] = int(fields[3])
        if set(found) != set(range(count)):
            raise RuntimeError(f"{label}: incomplete committed partitions")
        return found

    def drain(self, mode, version, label, require_nonempty=False):
        first, last = self.topic_positions(mode, version, label)
        if require_nonempty and any(last[p] <= first[p] for p in first):
            raise RuntimeError(f"{label}: fixture did not cover all partitions")
        deadline = time.monotonic() + 90
        while True:
            try:
                committed = self.committed(mode, version, label + "-commits")
                if all(first[p] <= committed[p] == last[p] for p in first):
                    break
                if any(committed[p] < first[p] or committed[p] > last[p] for p in first):
                    raise RuntimeError(f"{label}: commit outside retained captured bounds")
            except RuntimeError as exc:
                if "uninitialized" not in str(exc) and "incomplete committed" not in str(exc):
                    raise
            if time.monotonic() >= deadline:
                raise RuntimeError(f"{label}: failed to drain captured ends within 90s")
            time.sleep(2)
        self.record["stages"].append({"kind": "drained", "label": label,
                                      "topic": TOPICS[version][0], "starts": first,
                                      "ends": last, "committed": committed, "at": utc()})
        topic, group, count = TOPICS[version]
        lag_path = self.output / f"{label}-lag.jsonl"
        self.command([self.lag, "--output", lag_path, "--brokers",
                      f"127.0.0.1:{PORTS['kafka']}", "--topic", topic,
                      "--group", group, "--partitions", str(count), "--duration", "3s"],
                     label + "-lag-observer", 12)
        samples = [json.loads(line) for line in lag_path.read_text().splitlines()]
        if not any(s.get("kind") == "lag" and s.get("lag_records") == 0 and
                   len(s.get("partitions", [])) == count and
                   all(p.get("committed") == committed[p.get("partition")] and
                       p.get("end") == last[p.get("partition")] for p in s["partitions"])
                   for s in samples):
            raise RuntimeError(f"{label}: independent lag observer did not confirm captured drain")
        self.flush()
        return committed

    def quiesce(self, mode):
        self.compose(mode, ["stop", "collector"], f"stop-{mode}-collector", 40)
        if mode != "direct":
            self.compose(mode, ["stop", "stream-ingest"], f"stop-{mode}-producer", 40)
        self.record["stages"].append({"kind": "quiesced", "mode": mode, "at": utc()})
        self.flush()

    def api_status(self, label="v2-status"):
        url = f"http://127.0.0.1:{PORTS['api']}/api/v1/ingestion-status"
        started = dt.datetime.now(dt.timezone.utc)
        deadline = time.monotonic() + 15
        observations = []
        while True:
            try:
                with urllib.request.urlopen(url, timeout=2) as response:
                    status = json.load(response)
                observations.append({"at": utc(), "http_status": response.status, "body": status})
                sampled = status.get("sampled_at")
                sample_time = dt.datetime.fromisoformat(sampled.replace("Z", "+00:00")) if isinstance(sampled, str) else None
                if sample_time is not None and sample_time >= started and \
                   status.get("state") == "ok" and status.get("lag_records") == 0 and \
                   status.get("partitions_total") == 4 and status.get("partitions_observed") == 4 and \
                   status.get("processing_state") == "idle" and not status.get("reason_code"):
                    save(self.output / f"{label}.json", {"observations": observations, "accepted": status})
                    return
            except (OSError, ValueError, TypeError) as exc:
                observations.append({"at": utc(), "error": repr(exc)})
            if time.monotonic() >= deadline:
                save(self.output / f"{label}.json", {"observations": observations, "error": "healthy fresh zero-lag sample not observed"})
                raise RuntimeError("v2 product status did not become fresh and healthy within 15s")
            time.sleep(1)

    def run(self):
        # check_config proved this resolved model equals the root default.
        # main required absent project volumes/containers before this first start.
        self.start("default")
        self.fixture("clean-default-fixture")
        self.reconcile_prior("default", "clean-default")
        self.check_trace("clean-default-fixture", "clean-default-trace")
        self.quiesce("default")
        self.drain("default", "v2", "clean-default-drain", True)
        self.api_status("clean-default-status")
        self.compose("default", ["stop", "stream-worker"], "stop-clean-default-worker")
        self.start("v1")
        self.fixture("v1-fixture")
        self.reconcile_prior("v1", "v1-initial")
        self.check_trace("v1-fixture", "v1-trace-before-cutover")
        self.quiesce("v1")
        self.drain("v1", "v1", "v1-before-v2", True)
        v1_positions = self.topic_positions("v1", "v1", "v1-frozen-position")
        self.compose("v1", ["stop", "stream-worker"], "stop-v1-worker")
        self.start("v2")
        self.reconcile_prior("v2", "v1-after-v2")
        self.check_trace("v1-fixture", "v1-trace-after-v2-cutover")
        self.fixture("v2-fixture")
        self.reconcile_prior("v2", "v2-initial")
        self.check_trace("v2-fixture", "v2-trace-before-direct")
        self.quiesce("v2")
        v2_before = self.drain("v2", "v2", "v2-before-direct", True)
        self.api_status()
        self.drain("v2", "v1", "retained-v1-after-v2", True)
        if self.topic_positions("v2", "v1", "v1-after-v2-position") != v1_positions:
            raise RuntimeError("v1 retained log bounds changed during v2 cutover")
        v2_positions = self.topic_positions("v2", "v2", "v2-frozen-position")
        self.compose("v2", ["stop", "stream-worker", "kafka"], "stop-v2-before-direct")
        self.start("direct")
        self.reconcile_prior("direct", "after-direct-rollback")
        self.check_trace("v1-fixture", "v1-trace-in-direct")
        self.check_trace("v2-fixture", "v2-trace-in-direct")
        self.fixture("direct-fixture")
        self.reconcile_prior("direct", "direct-initial")
        self.check_trace("direct-fixture", "direct-trace")
        self.quiesce("direct")
        self.compose("direct", ["stop", "ingest"], "stop-direct-ingest")
        self.start("default")
        self.reconcile_prior("default", "after-v2-return")
        if self.topic_positions("default", "v2", "v2-after-direct-position") != v2_positions:
            raise RuntimeError("v2 retained log bounds changed during direct rollback")
        self.check_trace("v1-fixture", "v1-trace-after-return")
        self.check_trace("v2-fixture", "v2-trace-after-return")
        self.check_trace("direct-fixture", "direct-trace-after-return")
        if self.committed("default", "v2", "v2-commit-after-return") != v2_before:
            raise RuntimeError("v2 group commit changed during direct rollback")
        self.fixture("v2-return-fixture")
        self.reconcile_prior("default", "v2-return-final")
        self.check_trace("v2-return-fixture", "v2-return-trace")
        self.quiesce("default")
        self.drain("default", "v2", "v2-return", True)
        self.drain("default", "v1", "retained-v1-final", True)
        if self.topic_positions("default", "v1", "v1-final-position") != v1_positions:
            raise RuntimeError("v1 retained log bounds changed during rehearsal")
        self.assert_volumes("volumes-final")


def ensure_unused_ports():
    for port in sorted(set(PORTS.values())):
        with socket.socket() as sock:
            try:
                sock.bind(("127.0.0.1", port))
            except OSError as exc:
                raise RuntimeError(f"isolated host port {port} unavailable: {exc}") from exc


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True, help="new incidentlens-cutover-<8..24 lowercase/digit> project")
    parser.add_argument("--output", type=Path, required=True, help="new evidence directory")
    parser.add_argument("--bench-binary", type=Path, required=True, help="prebuilt native backend/cmd/bench")
    parser.add_argument("--lag-binary", type=Path, required=True, help="prebuilt native backend/cmd/bench-lag")
    parser.add_argument("--ingest-image", required=True, help="existing direct ingest image reference")
    parser.add_argument("--api-image", required=True, help="existing query API image reference")
    parser.add_argument("--stream-ingest-image", required=True, help="existing v1/v2 producer image reference")
    parser.add_argument("--stream-worker-image", required=True, help="existing v1/v2 worker image reference")
    args = parser.parse_args()
    if not PROJECT_RE.fullmatch(args.project) or args.project == "incidentlens":
        parser.error("invalid isolated project name")
    if not all(p.is_file() for p in (BASE, STREAM, HARDEN, DEFAULT)):
        parser.error("promoted root/default or explicit Compose model missing")
    bench = args.bench_binary.resolve()
    lag = args.lag_binary.resolve()
    if any(not path.is_file() or not os.access(path, os.X_OK) for path in (bench, lag)):
        parser.error("prebuilt bench or bench-lag binary unavailable")
    images = {"ingest": args.ingest_image, "api": args.api_image,
              "stream_ingest": args.stream_ingest_image, "stream_worker": args.stream_worker_image}
    if any(not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/:@-]{0,255}", ref) for ref in images.values()):
        parser.error("image references must be plain local names, tags, or digests")
    ensure_unused_ports()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False, mode=0o700)
    task = Rehearsal(args.project, output, bench, lag, images)
    try:
        if task.inventory("initial-volume-inventory"):
            raise RuntimeError("project already owns volumes; refusing retained-data reuse")
        containers = task.command(["docker", "ps", "-aq", "--filter",
                                   f"label=com.docker.compose.project={args.project}"],
                                  "initial-container-inventory")["stdout"].split()
        if containers:
            raise RuntimeError("project already owns containers; refusing existing stack")
        task.inspect_selected_images()
        task.write_overlay()
        task.check_config()
        task.run()
        task.record.update(status="passed", ended_at=utc())
    except BaseException as exc:
        task.record.update(status="failed", error=repr(exc), ended_at=utc())
    finally:
        # Preserve all containers, logs and volumes for inspection. Quiesce only
        # this owned project; never call down, remove-orphans or reset offsets.
        if task.active is not None:
            quiesce = task.compose(task.active, ["stop", "collector"], "final-quiesce-collector", 40, False)
            task.compose(task.active, ["logs", "--no-color", "--tail", "500"], "final-service-logs", 40, False)
            stopped = task.compose(task.active, ["stop"], "final-stop-owned-project", 90, False)
            if quiesce.get("returncode") != 0 or stopped.get("returncode") != 0:
                task.record["status"] = "failed"
                task.record["cleanup_error"] = "owned project did not stop cleanly"
        try:
            task.inventory("final-volume-inventory")
        except Exception as exc:
            task.record["final_inventory_error"] = repr(exc)
            task.record["status"] = "failed"
        task.flush()
    print(json.dumps({"status": task.record["status"], "evidence": str(output)}))
    return 0 if task.record["status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
