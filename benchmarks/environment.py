"""Bounded, read-only environment observations for the local benchmark runner."""
import hashlib
import json
import os
import platform
import shutil
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
COMPOSE = ROOT / "deploy/local/compose.yaml"


def utc():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def command(args, timeout=15):
    started = time.monotonic()
    record = {"at": utc(), "command": [str(a) for a in args]}
    try:
        result = subprocess.run(args, cwd=ROOT, capture_output=True, text=True, timeout=timeout)
        record.update(returncode=result.returncode, stdout=result.stdout, stderr=result.stderr)
    except (OSError, subprocess.TimeoutExpired) as exc:
        record.update(returncode=None, error=repr(exc))
        if isinstance(exc, subprocess.TimeoutExpired):
            for key, value in (("stdout", exc.stdout), ("stderr", exc.stderr)):
                if value is not None:
                    record[key] = value.decode("utf-8", errors="replace") if isinstance(value, bytes) else value
    record["elapsed_seconds"] = time.monotonic() - started
    return record


def digest(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(1 << 20), b""):
            value.update(block)
    return value.hexdigest()


def manifest():
    tracked = command(["git", "ls-files", "-z"])
    others = command(["git", "ls-files", "--others", "--exclude-standard", "-z"])
    paths = set((tracked.get("stdout", "") + others.get("stdout", "")).split("\0"))
    selected = {}
    for name in sorted(paths):
        path = ROOT / name
        if not name or not path.is_file():
            continue
        parts = Path(name).parts
        relevant = parts[0] in {"backend", "frontend", "deploy", "migrations", "api", "scripts"}
        relevant |= parts[0] == "benchmarks" and len(parts) == 2 and path.suffix in {".py", ".md", ".json"}
        if relevant:
            selected[name] = {"bytes": path.stat().st_size, "sha256": digest(path)}
    return {"files": selected, "git_inventory_status": [tracked.get("returncode"), others.get("returncode")]}


def container_ids():
    probe = command(["docker", "compose", "-f", str(COMPOSE), "ps", "-q", "clickhouse", "ingest", "api", "collector"])
    return probe.get("stdout", "").split(), probe


def environment(binary):
    info = command(["docker", "info", "--format", "{{json .}}"])
    try:
        parsed = json.loads(info.get("stdout", ""))
        docker_vm = {key: parsed.get(key) for key in ("ServerVersion", "OperatingSystem", "OSType", "Architecture", "NCPU", "MemTotal", "DockerRootDir", "Driver")}
    except ValueError:
        docker_vm = {"error": info}
    ids, inventory = container_ids()
    inspect = command(["docker", "inspect", *ids]) if ids else inventory
    containers = []
    try:
        for value in json.loads(inspect.get("stdout", "[]")):
            host = value["HostConfig"]
            containers.append({"id": value["Id"], "name": value["Name"], "image_id": value["Image"],
                               "image_reference": value["Config"]["Image"], "state": value["State"],
                               "limits": {k: host.get(k) for k in ("Memory", "MemorySwap", "NanoCpus", "CpuQuota", "CpuPeriod", "CpusetCpus", "PidsLimit")}})
    except (ValueError, KeyError) as exc:
        containers = [{"error": repr(exc), "observation": inspect}]
    images = []
    for image_id in sorted({c["image_id"] for c in containers if "image_id" in c}):
        raw = command(["docker", "image", "inspect", image_id])
        try:
            value = json.loads(raw.get("stdout", ""))[0]
            images.append({k: value.get(k) for k in ("Id", "RepoTags", "RepoDigests", "Created", "Architecture", "Os")})
        except (ValueError, IndexError):
            images.append({"error": raw})
    hardware = {}
    if platform.system() == "Darwin":
        for key in ("hw.model", "machdep.cpu.brand_string", "hw.memsize", "hw.physicalcpu", "hw.logicalcpu"):
            hardware[key] = command(["sysctl", "-n", key])
    else:
        hardware["lscpu"] = command(["lscpu"])
        try:
            hardware["meminfo"] = Path("/proc/meminfo").read_text()
        except OSError as exc:
            hardware["meminfo"] = repr(exc)
    disk = shutil.disk_usage(ROOT)
    go = os.environ.get("GO_BINARY") or shutil.which("go")
    if go is None and Path("/tmp/incidentlens-toolchain/go/bin/go").is_file():
        go = "/tmp/incidentlens-toolchain/go/bin/go"
    return {"at": utc(), "platform": platform.platform(), "python": platform.python_version(), "cpu_count": os.cpu_count(),
            "hardware": hardware, "docker_vm": docker_vm, "docker_version": command(["docker", "version", "--format", "{{json .}}"]),
            "go_binary_build": command([go or "go", "version", "-m", str(Path(binary).resolve())]),
            "clickhouse_version": command(["docker", "compose", "-f", str(COMPOSE), "exec", "-T", "clickhouse", "clickhouse-client", "--password", "local-admin", "--query", "SELECT version()"]),
            "containers": containers, "images": images, "git_head": command(["git", "rev-parse", "HEAD"]),
            "git_dirty": command(["git", "status", "--porcelain=v1"]), "source_manifest": manifest(),
            "binary": {"path": str(Path(binary).resolve()), "sha256": digest(binary), "bytes": Path(binary).stat().st_size},
            "host_disk": {"total_bytes": disk.total, "used_bytes": disk.used, "free_bytes": disk.free}}


def storage_snapshot():
    sql = "SELECT table, count() AS active_parts, sum(rows) AS physical_rows, sum(bytes_on_disk) AS disk_bytes, sum(data_compressed_bytes) AS compressed_bytes, sum(data_uncompressed_bytes) AS uncompressed_bytes FROM system.parts WHERE database='incidentlens' AND active GROUP BY table FORMAT JSON"
    probe = command(["docker", "compose", "-f", str(COMPOSE), "exec", "-T", "clickhouse", "clickhouse-client", "--password", "local-admin", "--query", sql])
    disk = shutil.disk_usage(ROOT)
    return {"at": utc(), "clickhouse_parts": probe,
            "clickhouse_disk": command(["docker", "compose", "-f", str(COMPOSE), "exec", "-T", "clickhouse", "df", "-k", "/var/lib/clickhouse"]),
            "host_disk": {"total_bytes": disk.total, "used_bytes": disk.used, "free_bytes": disk.free}}


def sample(pid, ids):
    return {"at": utc(), "kind": "resource_sample", "harness_pid": pid,
            "docker_stats": command(["docker", "stats", "--no-stream", "--format", "{{json .}}", *ids], timeout=5) if ids else {"error": "no product containers discovered"},
            "harness_ps": command(["ps", "-p", str(pid), "-o", "pid=,pcpu=,rss=,etime=,stat="], timeout=3),
            "container_health": command(["docker", "inspect", "--format", '{{json .Name}} {{json .RestartCount}} {{json .State}}', *ids], timeout=5) if ids else {"error": "no containers"},
            "ps_units": {"pcpu": "percent", "rss": "KiB"}}
