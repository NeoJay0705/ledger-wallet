#!/usr/bin/env python3
"""Qualify the pinned PostgreSQL 18 container and shared-device cgroup caps.

This is an infrastructure-only run. It starts one temporary PostgreSQL
container on a newly created internal bridge network, verifies the runtime
limits and PostgreSQL statistics/settings, writes 32 MiB through SQL, runs a
checkpoint, records cgroup I/O deltas, and removes only resources carrying
this run's unique ownership labels.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import shutil
import stat
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[3]
QUAL_ROOT = Path(__file__).resolve().parent
CAPS_PATH = QUAL_ROOT / "io_caps.json"
IMAGE_TAG = "postgres:18.6-bookworm"
IMAGE_REF = (
    "docker.io/library/postgres@sha256:"
    "afc7e2d441324c0388fa80c3d24f733b4194a4eb7f47dd8ee2b08eb1a24a647c"
)
IMAGE_ID = (
    "sha256:2e182ed2ff8f5d0835353864264437a26dffc542a9a6c677ec495aa54e491627"
)
IMAGE_VERSION = "18.6"
IMAGE_ARCH = "amd64"
OWNER = "wallet-db-compare-qualification"
MEMORY_BYTES = 10 * 1024**3
EXPECTED_SETTINGS = {
    "shared_buffers": "2GB",
    "work_mem": "4MB",
    "maintenance_work_mem": "256MB",
    "autovacuum_work_mem": "64MB",
    "max_connections": "100",
    "fsync": "on",
    "synchronous_commit": "on",
    "full_page_writes": "on",
    "wal_sync_method": "fdatasync",
    "commit_delay": "0",
    "wal_level": "replica",
    "checkpoint_timeout": "5min",
    "max_wal_size": "1GB",
    "autovacuum": "on",
    "track_io_timing": "on",
    "track_wal_io_timing": "on",
}

SOURCE_RECORD = {
    "checked_at_utc": "2026-10-13",
    "postgresql_versioning_policy": "https://www.postgresql.org/support/versioning/",
    "postgresql_release_notes": "https://www.postgresql.org/docs/current/release.html",
    "postgresql_18_release_notes": "https://www.postgresql.org/docs/18/release-18.html",
    "docker_official_postgres_tags": "https://hub.docker.com/_/postgres/tags?name=bookworm&page=1",
    "docker_official_postgres_18_data_path": "https://github.com/docker-library/docs/blob/master/postgres/content.md#pgdata",
    "postgresql_18_stats_docs": "https://www.postgresql.org/docs/18/monitoring-stats.html",
    "postgresql_18_stats_settings_docs": "https://www.postgresql.org/docs/18/runtime-config-statistics.html",
}


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds")


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def command(
    run_dir: Path,
    name: str,
    argv: list[str],
    timeout: float = 120,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    log_root = run_dir / "commands"
    log_root.mkdir(parents=True, exist_ok=True)
    try:
        result = subprocess.run(
            argv,
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            check=False,
        )
    except Exception as exc:
        (log_root / f"{name}.error.txt").write_text(f"{type(exc).__name__}: {exc}\n")
        raise
    (log_root / f"{name}.stdout.txt").write_text(result.stdout)
    (log_root / f"{name}.stderr.txt").write_text(result.stderr)
    write_json(
        log_root / f"{name}.command.json",
        {"argv": argv, "returncode": result.returncode, "completed_at_utc": utc_now()},
    )
    if check and result.returncode != 0:
        raise RuntimeError(
            f"command {name} exited {result.returncode}; see {log_root / (name + '.stderr.txt')}"
        )
    return result


def parse_json_stdout(result: subprocess.CompletedProcess[str], label: str) -> Any:
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{label} did not return JSON: {exc}") from exc


def docker_inspect(run_dir: Path, name: str, object_id: str) -> dict[str, Any]:
    result = command(run_dir, name, ["docker", "inspect", object_id])
    value = parse_json_stdout(result, name)
    if not isinstance(value, list) or len(value) != 1:
        raise RuntimeError(f"expected one inspect result for {object_id}")
    return value[0]


def read_cgroup_file(cgroup_dir: Path, name: str) -> str:
    path = cgroup_dir / name
    if not path.is_file():
        raise RuntimeError(f"missing required cgroup file: {path}")
    return path.read_text().strip()


def parse_io_stats(text: str) -> dict[str, dict[str, int]]:
    parsed: dict[str, dict[str, int]] = {}
    for line in text.splitlines():
        columns = line.split()
        if not columns:
            continue
        device = columns[0]
        counters: dict[str, int] = {}
        for field in columns[1:]:
            key, value = field.split("=", 1)
            counters[key] = int(value)
        parsed[device] = counters
    return parsed


def parse_io_max(text: str) -> dict[str, dict[str, str]]:
    parsed: dict[str, dict[str, str]] = {}
    for line in text.splitlines():
        columns = line.split()
        if not columns:
            continue
        device = columns[0]
        parsed[device] = dict(field.split("=", 1) for field in columns[1:])
    return parsed


def read_diskstats() -> dict[str, list[int]]:
    devices: dict[str, list[int]] = {}
    for line in Path("/proc/diskstats").read_text().splitlines():
        columns = line.split()
        if len(columns) < 14:
            continue
        major, minor, name = columns[:3]
        key = f"{major}:{minor}"
        if key in {"8:16", "8:18"}:
            devices[key] = [int(value) for value in columns[3:]]
            devices[key].insert(0, name)  # Keep the kernel device name with raw counters.
    return devices


def memory_available_bytes() -> int:
    for line in Path("/proc/meminfo").read_text().splitlines():
        if line.startswith("MemAvailable:"):
            return int(line.split()[1]) * 1024
    raise RuntimeError("MemAvailable missing from /proc/meminfo")


def directory_manifest(root: Path) -> list[dict[str, Any]]:
    """Describe a fresh qualification bind mount without following symlinks."""
    manifest: list[dict[str, Any]] = []
    root = root.resolve(strict=True)
    for current, dirs, files in os.walk(root, topdown=True, followlinks=False):
        current_path = Path(current)
        for name in sorted(dirs + files):
            path = current_path / name
            info = path.lstat()
            mode = info.st_mode
            if stat.S_ISLNK(mode):
                raise RuntimeError(f"refusing cleanup because bind manifest contains symlink: {path}")
            if not (stat.S_ISDIR(mode) or stat.S_ISREG(mode)):
                raise RuntimeError(f"refusing cleanup because bind manifest contains special file: {path}")
            manifest.append(
                {
                    "path": path.relative_to(root).as_posix(),
                    "type": "directory" if stat.S_ISDIR(mode) else "file",
                    "mode": oct(stat.S_IMODE(mode)),
                    "uid": info.st_uid,
                    "gid": info.st_gid,
                    "size": info.st_size,
                }
            )
    return manifest


def cleanup_owned_data(
    run_dir: Path,
    pgdata: Path,
    run_id: str,
    nonce: str,
    caps: dict[str, Any],
    mounted_for_run: bool,
) -> dict[str, Any]:
    """Remove only this run's empty bind directory or verified PG18 data tree."""
    result: dict[str, Any] = {"path": str(pgdata), "removed": False}
    if pgdata.is_symlink() or not pgdata.exists() or pgdata.resolve().parent != run_dir.resolve():
        raise RuntimeError(f"refusing to clean unexpected bind path: {pgdata}")
    contents = sorted(path.name for path in pgdata.iterdir())
    if not contents:
        pgdata.rmdir()
        result["removed"] = True
        result["method"] = "host rmdir of the run-created empty directory"
        result["manifest"] = []
        return result
    if not mounted_for_run:
        raise RuntimeError("refusing Docker cleanup helper because no owned container bind mount was verified")
    if contents != ["18"]:
        raise RuntimeError(f"refusing Docker cleanup helper; PG18 bind root has unexpected top-level entries: {contents}")
    pgdata_child = pgdata / "18"
    if not pgdata_child.is_dir() or pgdata_child.is_symlink():
        raise RuntimeError(f"refusing Docker cleanup helper; expected owned PG18 directory missing: {pgdata_child}")
    version_entries = sorted(path.name for path in pgdata_child.iterdir())
    if version_entries not in ([], ["docker"]):
        raise RuntimeError(f"refusing Docker cleanup helper; PG18 directory has unexpected entries: {version_entries}")
    if "docker" in version_entries and (pgdata_child / "docker").is_symlink():
        raise RuntimeError("refusing Docker cleanup helper; PGDATA path is a symlink")
    manifest = directory_manifest(pgdata)
    manifest_bytes = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    manifest_path = run_dir / "data-directory-manifest-before-cleanup.json"
    write_json(
        manifest_path,
        {
            "run_id": run_id,
            "nonce": nonce,
            "bind_root": str(pgdata),
            "container_mount_destination": "/var/lib/postgresql",
            "pgdata": "/var/lib/postgresql/18/docker",
            "top_level_entries": contents,
            "entry_count": len(manifest),
            "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
            "entries": manifest,
        },
    )
    helper_name = f"wallet-db-pgqual-cleanup-{nonce}"
    helper = command(
        run_dir,
        "owned-data-cleanup-helper",
        [
            "docker",
            "run",
            "--rm",
            "--name",
            helper_name,
            "--label",
            f"owner={OWNER}",
            "--label",
            f"qualification-nonce={nonce}",
            "--label",
            f"qualification-run-id={run_id}",
            "--network",
            "none",
            "--user",
            "0:0",
            "--cpuset-cpus",
            "0,1",
            "--memory",
            "256m",
            "--memory-swap",
            "256m",
            "--device-read-bps",
            f"{caps['device_path']}:{caps['read_bps']}",
            "--device-write-bps",
            f"{caps['device_path']}:{caps['write_bps']}",
            "--device-read-iops",
            f"{caps['device_path']}:{caps['read_iops']}",
            "--device-write-iops",
            f"{caps['device_path']}:{caps['write_iops']}",
            "--mount",
            f"type=bind,src={pgdata},dst=/var/lib/postgresql",
            "--entrypoint",
            "sh",
            IMAGE_REF,
            "-ec",
            "rm -rf -- /var/lib/postgresql/18",
        ],
        timeout=120,
    )
    if helper.returncode != 0:
        raise RuntimeError(f"owned data cleanup helper exited {helper.returncode}")
    if list(pgdata.iterdir()):
        raise RuntimeError(f"owned data cleanup helper left files in bind root: {list(pgdata.iterdir())}")
    pgdata.rmdir()
    result.update(
        {
            "removed": True,
            "method": "pinned-image Docker root helper after ownership/path/manifest validation",
            "helper_name": helper_name,
            "manifest_path": str(manifest_path),
            "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        }
    )
    return result


def run_psql(
    run_dir: Path,
    container_id: str,
    name: str,
    sql: str,
    timeout: float = 120,
) -> str:
    result = command(
        run_dir,
        name,
        [
            "docker",
            "exec",
            "--user",
            "postgres",
            container_id,
            "psql",
            "-X",
            "-v",
            "ON_ERROR_STOP=1",
            "-U",
            "postgres",
            "-d",
            "walletbench",
            "-At",
            "-c",
            sql,
        ],
        timeout=timeout,
    )
    return result.stdout.strip()


def main() -> int:
    nonce = uuid.uuid4().hex[:12]
    run_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + nonce
    run_dir = QUAL_ROOT / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    container_name = f"wallet-db-pgqual-{nonce}"
    network_name = f"wallet-db-pgqual-net-{nonce}"
    pgdata = run_dir / "pgdata"
    # The official image entrypoint starts as root and initializes PGDATA
    # before switching to the postgres account. The bind parent is traversable
    # (0755); PGDATA itself is tightened to 0700 inside the image.
    pgdata.mkdir(mode=0o755)
    pgdata.chmod(0o755)
    container_id: str | None = None
    network_id: str | None = None
    summary: dict[str, Any] = {
        "run_id": run_id,
        "nonce": nonce,
        "owner_label": OWNER,
        "started_at_utc": utc_now(),
        "status": "running",
        "image_tag": IMAGE_TAG,
        "image_ref": IMAGE_REF,
        "expected_image_id": IMAGE_ID,
        "expected_server_version": IMAGE_VERSION,
        "container_name": container_name,
        "network_name": network_name,
        "data_mount_host_path": str(pgdata),
        "qualification_scope": "infrastructure only; no wallet workload",
    }
    caps: dict[str, Any] = {}
    mounted_for_run = False
    write_json(run_dir / "qualification.json", summary)
    write_json(run_dir / "sources.json", SOURCE_RECORD)
    write_json(run_dir / "io_caps.json", json.loads(CAPS_PATH.read_text()))

    try:
        if not CAPS_PATH.is_file():
            raise RuntimeError(f"shared I/O cap file missing: {CAPS_PATH}")
        caps = json.loads(CAPS_PATH.read_text())
        expected_caps = {
            "device_path": "/dev/sdb",
            "major_minor": "8:16",
            "read_bps": 403631184,
            "write_bps": 152226096,
            "read_iops": 24497,
            "write_iops": 23500,
        }
        if caps != expected_caps:
            raise RuntimeError(f"I/O cap file differs from approved profile: {caps!r}")
        summary["io_caps_sha256"] = sha256_file(CAPS_PATH)
        summary["qualification_script_sha256"] = sha256_file(Path(__file__))

        docker_version = command(run_dir, "docker-version", ["docker", "version", "--format", "{{.Server.Version}}"])
        docker_info = command(
            run_dir,
            "docker-info",
            ["docker", "info", "--format", "{{json .}}"],
        )
        info = parse_json_stdout(docker_info, "docker info")
        if info.get("CgroupVersion") != "2":
            raise RuntimeError(f"Docker cgroup v2 required; got {info.get('CgroupVersion')!r}")
        summary["docker_server_version"] = docker_version.stdout.strip()
        summary["docker_info"] = {
            "CgroupVersion": info.get("CgroupVersion"),
            "CgroupDriver": info.get("CgroupDriver"),
            "DockerRootDir": info.get("DockerRootDir"),
            "OperatingSystem": info.get("OperatingSystem"),
            "Architecture": info.get("Architecture"),
        }
        st = os.stat("/dev/sdb")
        host_disk = {
            "device_path": os.path.realpath("/dev/sdb"),
            "device_major_minor": f"{os.major(st.st_rdev)}:{os.minor(st.st_rdev)}",
            "filesystem_source": subprocess.run(
                ["findmnt", "-no", "SOURCE,FSTYPE", "-T", str(ROOT)],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
            ).stdout.strip(),
            "filesystem_stat_device_major_minor": f"{os.major(os.stat(ROOT).st_dev)}:{os.minor(os.stat(ROOT).st_dev)}",
            "mem_available_before_start_bytes": memory_available_bytes(),
            "diskstats_before_start": read_diskstats(),
        }
        if host_disk["device_major_minor"] != caps["major_minor"]:
            raise RuntimeError(f"approved cap device no longer matches host: {host_disk}")
        if host_disk["filesystem_stat_device_major_minor"] != "8:18":
            raise RuntimeError(f"repository data is not on expected /dev/sdb2 (8:18): {host_disk}")
        summary["host_storage"] = host_disk

        command(run_dir, "pull-official-tag", ["docker", "pull", IMAGE_TAG], timeout=600)
        image_result = command(run_dir, "image-inspect", ["docker", "image", "inspect", IMAGE_TAG])
        image = parse_json_stdout(image_result, "docker image inspect")[0]
        write_json(run_dir / "image-inspect.json", image)
        repo_digests = image.get("RepoDigests") or []
        pinned_digest = IMAGE_REF.rsplit("@", 1)[1]
        observed_digests = {digest.rsplit("@", 1)[-1] for digest in repo_digests}
        if pinned_digest not in observed_digests:
            raise RuntimeError(f"pulled tag does not resolve to pinned OCI ref: {repo_digests!r}")
        if image.get("Id") != IMAGE_ID:
            raise RuntimeError(f"pinned image ID changed: {image.get('Id')!r}")
        if image.get("Architecture") != IMAGE_ARCH or image.get("Os") != "linux":
            raise RuntimeError(f"unexpected image platform: {image.get('Os')}/{image.get('Architecture')}")
        summary["image"] = {
            "tag": IMAGE_TAG,
            "repo_digest": IMAGE_REF,
            "repo_digests": repo_digests,
            "image_id": image.get("Id"),
            "os": image.get("Os"),
            "architecture": image.get("Architecture"),
            "created": image.get("Created"),
        }
        write_json(run_dir / "qualification.json", summary)

        network = command(
            run_dir,
            "network-create",
            [
                "docker",
                "network",
                "create",
                "--driver",
                "bridge",
                "--internal",
                "--label",
                f"owner={OWNER}",
                "--label",
                f"qualification-nonce={nonce}",
                "--label",
                f"qualification-run-id={run_id}",
                network_name,
            ],
        )
        network_id = network.stdout.strip()
        summary["network_id"] = network_id
        write_json(run_dir / "qualification.json", summary)
        network_info = docker_inspect(run_dir, "network-inspect", network_id)
        if network_info.get("Driver") != "bridge" or not network_info.get("Internal"):
            raise RuntimeError(f"network is not an isolated internal bridge: {network_info}")
        network_labels = network_info.get("Labels") or {}
        if network_labels.get("owner") != OWNER or network_labels.get("qualification-nonce") != nonce:
            raise RuntimeError("created network ownership labels do not match this run")
        write_json(run_dir / "network-inspect.json", network_info)

        postgres_args = ["postgres"]
        for setting, value in EXPECTED_SETTINGS.items():
            postgres_args.extend(["-c", f"{setting}={value}"])
        run_args = [
            "docker",
            "run",
            "--detach",
            "--name",
            container_name,
            "--label",
            f"owner={OWNER}",
            "--label",
            f"qualification-nonce={nonce}",
            "--label",
            f"qualification-run-id={run_id}",
            "--network",
            network_name,
            "--cpuset-cpus",
            "0,1",
            "--memory",
            "10g",
            "--memory-swap",
            "10g",
            "--device-read-bps",
            f"{caps['device_path']}:{caps['read_bps']}",
            "--device-write-bps",
            f"{caps['device_path']}:{caps['write_bps']}",
            "--device-read-iops",
            f"{caps['device_path']}:{caps['read_iops']}",
            "--device-write-iops",
            f"{caps['device_path']}:{caps['write_iops']}",
            "--mount",
            f"type=bind,src={pgdata},dst=/var/lib/postgresql",
            "--env",
            "POSTGRES_HOST_AUTH_METHOD=trust",
            "--env",
            "POSTGRES_USER=postgres",
            "--env",
            "POSTGRES_DB=walletbench",
            "--env",
            "PGDATA=/var/lib/postgresql/18/docker",
            IMAGE_REF,
            *postgres_args,
        ]
        launched = command(run_dir, "container-run", run_args)
        container_id = launched.stdout.strip()
        summary["container_id"] = container_id
        write_json(run_dir / "qualification.json", summary)
        inspect = docker_inspect(run_dir, "container-inspect-before", container_id)
        write_json(run_dir / "container-inspect-before.json", inspect)
        labels = inspect.get("Config", {}).get("Labels") or {}
        if labels.get("owner") != OWNER or labels.get("qualification-nonce") != nonce:
            raise RuntimeError("container ownership labels do not match this run")
        host_config = inspect.get("HostConfig") or {}
        if host_config.get("CpusetCpus") != "0,1":
            raise RuntimeError(f"container CPU set differs from requested 0,1: {host_config.get('CpusetCpus')!r}")
        if host_config.get("Memory") != MEMORY_BYTES or host_config.get("MemorySwap") != MEMORY_BYTES:
            raise RuntimeError(
                f"container memory/swap limits differ from 10 GiB/no extra swap: "
                f"{host_config.get('Memory')}/{host_config.get('MemorySwap')}"
            )
        if host_config.get("CpuQuota") not in (0, None) or host_config.get("NanoCpus") not in (0, None):
            raise RuntimeError("container unexpectedly has a CPU quota")
        if host_config.get("PortBindings") not in (None, {}):
            raise RuntimeError(f"host ports are unexpectedly published: {host_config.get('PortBindings')}")
        if host_config.get("NetworkMode") != network_name:
            raise RuntimeError(f"container not attached to its dedicated network: {host_config.get('NetworkMode')}")
        if inspect.get("Image") != IMAGE_ID:
            raise RuntimeError(f"running container image ID differs from the pinned image: {inspect.get('Image')}")
        configured_devices = {
            field: host_config.get(field)
            for field in ("BlkioDeviceReadBps", "BlkioDeviceWriteBps", "BlkioDeviceReadIOps", "BlkioDeviceWriteIOps")
        }
        write_json(run_dir / "docker-hostconfig-device-limits.json", configured_devices)
        mounts = inspect.get("Mounts") or []
        bind_mount = next(
            (mount for mount in mounts if mount.get("Destination") == "/var/lib/postgresql"),
            None,
        )
        if not bind_mount or bind_mount.get("Type") != "bind" or Path(bind_mount.get("Source", "")).resolve() != pgdata.resolve():
            raise RuntimeError(f"PostgreSQL data directory is not the intended bind mount: {bind_mount}")
        mounted_for_run = True
        if inspect.get("NetworkSettings", {}).get("Ports", {}).get("5432/tcp") is not None:
            raise RuntimeError("PostgreSQL port is unexpectedly published to the host")
        env = inspect.get("Config", {}).get("Env") or []
        env_map = dict(item.split("=", 1) for item in env if "=" in item)
        for key, expected in {
            "POSTGRES_HOST_AUTH_METHOD": "trust",
            "POSTGRES_USER": "postgres",
            "POSTGRES_DB": "walletbench",
            "PGDATA": "/var/lib/postgresql/18/docker",
        }.items():
            if env_map.get(key) != expected:
                raise RuntimeError(f"container environment {key} did not match {expected!r}")
        summary["container_id"] = container_id
        summary["network_id"] = network_id
        summary["docker_hostconfig_device_limits"] = configured_devices

        state = inspect.get("State") or {}
        pid = int(state.get("Pid") or 0)
        if pid <= 0:
            raise RuntimeError(f"container has no live host PID: {state}")
        cgroup_text = Path(f"/proc/{pid}/cgroup").read_text()
        unified = next((line.split("::", 1)[1] for line in cgroup_text.splitlines() if "::" in line), None)
        if unified is None:
            raise RuntimeError("container host PID has no unified cgroup-v2 path")
        cgroup_root = Path("/sys/fs/cgroup").resolve()
        cgroup_dir = (cgroup_root / unified.lstrip("/")).resolve()
        if cgroup_root not in cgroup_dir.parents and cgroup_dir != cgroup_root:
            raise RuntimeError(f"resolved cgroup path escaped cgroup root: {cgroup_dir}")
        cgroup_snapshot = {
            "host_pid": pid,
            "proc_cgroup": cgroup_text,
            "unified_path": unified,
            "resolved_directory": str(cgroup_dir),
            "cpuset_cpus_effective": read_cgroup_file(cgroup_dir, "cpuset.cpus.effective"),
            "memory_max": read_cgroup_file(cgroup_dir, "memory.max"),
            "memory_swap_max": read_cgroup_file(cgroup_dir, "memory.swap.max"),
            "cpu_max": read_cgroup_file(cgroup_dir, "cpu.max"),
            "io_max": read_cgroup_file(cgroup_dir, "io.max"),
            "io_stat_before_sql": read_cgroup_file(cgroup_dir, "io.stat"),
        }
        if cgroup_snapshot["cpuset_cpus_effective"] not in {"0-1", "0,1"}:
            raise RuntimeError(f"actual cgroup CPU affinity differs from CPU 0,1: {cgroup_snapshot['cpuset_cpus_effective']}")
        if cgroup_snapshot["memory_max"] != str(MEMORY_BYTES):
            raise RuntimeError(f"actual cgroup memory.max differs from 10 GiB: {cgroup_snapshot['memory_max']}")
        if cgroup_snapshot["memory_swap_max"] != "0":
            raise RuntimeError(f"actual cgroup memory.swap.max is not zero: {cgroup_snapshot['memory_swap_max']}")
        if cgroup_snapshot["cpu_max"].split()[0] != "max":
            raise RuntimeError(f"actual cgroup CPU quota is set: {cgroup_snapshot['cpu_max']}")
        io_limits = parse_io_max(cgroup_snapshot["io_max"])
        expected_device_limit = {
            "rbps": str(caps["read_bps"]),
            "wbps": str(caps["write_bps"]),
            "riops": str(caps["read_iops"]),
            "wiops": str(caps["write_iops"]),
        }
        if io_limits.get(caps["major_minor"]) != expected_device_limit:
            raise RuntimeError(
                f"actual cgroup io.max for {caps['major_minor']} differs from approved caps: "
                f"{io_limits.get(caps['major_minor'])!r}"
            )
        write_json(run_dir / "cgroup-before-sql.json", cgroup_snapshot)

        ready = False
        for attempt in range(120):
            result = command(
                run_dir,
                f"pg-isready-{attempt:03d}",
                [
                    "docker",
                    "exec",
                    "--user",
                    "postgres",
                    container_id,
                    "pg_isready",
                    "-h",
                    "127.0.0.1",
                    "-U",
                    "postgres",
                    "-d",
                    "walletbench",
                ],
                check=False,
            )
            if result.returncode == 0:
                ready = True
                break
            lifecycle = command(
                run_dir,
                f"container-state-before-ready-{attempt:03d}",
                ["docker", "inspect", "--format", "{{.State.Status}}:{{.State.ExitCode}}", container_id],
                check=False,
            )
            lifecycle_value = lifecycle.stdout.strip()
            if lifecycle.returncode != 0 or lifecycle_value.startswith("exited:") or lifecycle_value.startswith("dead:"):
                raise RuntimeError(
                    f"PostgreSQL container exited before readiness ({lifecycle_value}); logs and stopped inspect retained"
                )
            time.sleep(1)
        if not ready:
            raise RuntimeError("PostgreSQL did not become ready within 120 seconds")
        summary["ready_at_utc"] = utc_now()

        settings_sql = (
            "SELECT json_build_object('server_version', current_setting('server_version'), "
            "'server_version_num', current_setting('server_version_num')::integer, "
            "'in_recovery', pg_is_in_recovery(), 'settings', "
            "(SELECT json_object_agg(name, current_setting(name) ORDER BY name) "
            "FROM unnest(ARRAY["
            + ",".join("'" + name + "'" for name in EXPECTED_SETTINGS)
            + "]) AS names(name)))::text"
        )
        settings_raw = run_psql(run_dir, container_id, "postgres-settings", settings_sql)
        settings = json.loads(settings_raw)
        write_json(run_dir / "postgres-settings.json", settings)
        actual_server_version = str(settings.get("server_version", "")).split()[0]
        if actual_server_version != IMAGE_VERSION or settings.get("server_version_num") != 180006:
            raise RuntimeError(f"running PostgreSQL version is not 18.6: {settings}")
        if settings.get("in_recovery") is not False:
            raise RuntimeError(f"PostgreSQL is in recovery/standby mode: {settings.get('in_recovery')}")
        if settings.get("settings") != EXPECTED_SETTINGS:
            raise RuntimeError(f"PostgreSQL SHOW settings differ from approved profile: {settings.get('settings')}")

        stats_sql = (
            "SELECT jsonb_build_object('wal_io_rows', "
            "(SELECT count(*) FROM pg_stat_io WHERE object = 'wal'), "
            "'wal_io_rows_sample', (SELECT jsonb_agg(to_jsonb(s) ORDER BY backend_type, context) "
            "FROM pg_stat_io AS s WHERE object = 'wal'), "
            "'wal_stats', (SELECT to_jsonb(w) FROM pg_stat_wal AS w), "
            "'database_stats', (SELECT to_jsonb(d) FROM pg_stat_database AS d WHERE datname = current_database()), "
            "'checkpointer_stats', (SELECT to_jsonb(c) FROM pg_stat_checkpointer AS c), "
            "'activity_wait_counts', (SELECT jsonb_object_agg(wait_group, n) FROM ("
            "SELECT coalesce(wait_event_type, 'none') AS wait_group, count(*) AS n "
            "FROM pg_stat_activity GROUP BY coalesce(wait_event_type, 'none')) AS waits), "
            "'stat_io_columns', (SELECT jsonb_agg(column_name ORDER BY ordinal_position) "
            "FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_stat_io'))::text"
        )
        stats_raw = run_psql(run_dir, container_id, "postgres-stats-before-write", stats_sql)
        stats = json.loads(stats_raw)
        write_json(run_dir / "postgres-stats-before-write.json", stats)
        if not stats.get("wal_io_rows"):
            raise RuntimeError("pg_stat_io has no rows for object=wal")
        wal_rows = stats.get("wal_io_rows_sample") or []
        required_wal_fields = {"writes", "write_time", "fsyncs", "fsync_time"}
        if not wal_rows or not required_wal_fields.issubset(wal_rows[0]):
            raise RuntimeError(f"pg_stat_io WAL rows lack required timing fields: {wal_rows[:1]}")
        wal_stats = stats.get("wal_stats") or {}
        if not {"wal_bytes", "wal_fpi"}.issubset(wal_stats):
            raise RuntimeError(f"pg_stat_wal lacks WAL bytes/FPI fields: {wal_stats}")
        if not {"xact_commit", "xact_rollback"}.issubset(stats.get("database_stats") or {}):
            raise RuntimeError(f"pg_stat_database lacks transaction counters: {stats.get('database_stats')}")
        if not {"num_timed", "num_requested", "write_time", "sync_time"}.issubset(stats.get("checkpointer_stats") or {}):
            raise RuntimeError(f"pg_stat_checkpointer lacks expected checkpoint fields: {stats.get('checkpointer_stats')}")

        pgdata_stat = os.stat(pgdata)
        data_file_device = f"{os.major(pgdata_stat.st_dev)}:{os.minor(pgdata_stat.st_dev)}"
        data_file_identity = {
            "host_bind_mount_path": str(pgdata),
            "host_st_dev_major_minor": data_file_device,
            "container_data_root": "/var/lib/postgresql/18/docker",
            "host_cgroup_pid": pid,
            "cgroup_path": str(cgroup_dir),
            "approved_io_max_device": caps["major_minor"],
        }
        if data_file_device != "8:18":
            raise RuntimeError(f"PostgreSQL bind directory is not on /dev/sdb2 (8:18): {data_file_identity}")
        write_json(run_dir / "data-device-and-cgroup.json", data_file_identity)

        write_sql = (
            "CREATE TABLE qualification_io (id integer PRIMARY KEY, payload bytea NOT NULL); "
            "INSERT INTO qualification_io (id, payload) "
            "SELECT i, repeat('x', 1048576)::bytea FROM generate_series(1, 32) AS i; "
            "CHECKPOINT;"
        )
        io_before = parse_io_stats(cgroup_snapshot["io_stat_before_sql"])
        write_json(
            run_dir / "buffered-write-before.json",
            {
                "captured_at_utc": utc_now(),
                "cgroup_io_stat": cgroup_snapshot["io_stat_before_sql"],
                "host_diskstats": read_diskstats(),
                "expected_rows": 32,
                "expected_payload_bytes": 32 * 1048576,
            },
        )
        run_psql(run_dir, container_id, "bounded-sql-write-and-checkpoint", write_sql, timeout=300)
        validation_sql = (
            "SELECT jsonb_build_object('rows', count(*), 'payload_bytes', sum(octet_length(payload)))::text "
            "FROM qualification_io"
        )
        data_validation = json.loads(run_psql(run_dir, container_id, "bounded-write-validation", validation_sql))
        write_json(run_dir / "bounded-write-validation.json", data_validation)
        if data_validation != {"rows": 32, "payload_bytes": 32 * 1048576}:
            raise RuntimeError(f"bounded SQL write result did not match expected 32 MiB: {data_validation}")
        time.sleep(2)
        cgroup_after_text = read_cgroup_file(cgroup_dir, "io.stat")
        io_after = parse_io_stats(cgroup_after_text)
        diskstats_after = read_diskstats()
        target_devices = {data_file_device, caps["major_minor"]}
        attributed_device_deltas = {}
        for device in sorted(target_devices):
            before_counts = io_before.get(device, {})
            after_counts = io_after.get(device, {})
            attributed_device_deltas[device] = {
                key: after_counts.get(key, 0) - before_counts.get(key, 0)
                for key in sorted(set(before_counts) | set(after_counts))
            }
        writes_attributed = any(
            delta.get("wbytes", 0) > 0 and delta.get("wios", 0) > 0
            for delta in attributed_device_deltas.values()
        )
        write_evidence = {
            "captured_at_utc": utc_now(),
            "cgroup_io_stat_after_checkpoint": cgroup_after_text,
            "cgroup_device_deltas": attributed_device_deltas,
            "host_diskstats_after": diskstats_after,
            "bounded_write_rows": data_validation["rows"],
            "bounded_write_payload_bytes": data_validation["payload_bytes"],
            "checkpoint_statement_completed": True,
            "writes_attributed_to_data_or_approved_device": writes_attributed,
            "file_device_major_minor": data_file_device,
            "approved_io_limit_device_major_minor": caps["major_minor"],
        }
        write_json(run_dir / "buffered-write-after-checkpoint.json", write_evidence)
        if not writes_attributed:
            raise RuntimeError(
                "buffered SQL writes/checkpoint produced no cgroup wbytes+wios delta on the data/approved device"
            )

        stats_after_raw = run_psql(run_dir, container_id, "postgres-stats-after-checkpoint", stats_sql)
        stats_after = json.loads(stats_after_raw)
        write_json(run_dir / "postgres-stats-after-checkpoint.json", stats_after)

        logs = command(run_dir, "container-logs-before-stop", ["docker", "logs", container_id], check=False)
        summary["qualification_checks"] = {
            "pinned_image_digest_and_id": True,
            "official_postgres_version_18_6": True,
            "not_in_recovery": True,
            "approved_show_settings": True,
            "pg_stat_io_wal_and_timing": True,
            "pg_stat_wal_bytes_fpi": True,
            "pg_stat_database_and_checkpointer": True,
            "container_hostconfig_caps": True,
            "cgroup_io_max_8_16": True,
            "file_device_8_18": True,
            "bounded_buffered_sql_write_checkpoint_attributed": writes_attributed,
            "no_host_ports_dedicated_internal_network": True,
        }
        summary["postgres_settings"] = settings
        summary["bounded_write_attribution"] = write_evidence
        summary["finished_qualification_at_utc"] = utc_now()
        summary["status"] = "passed"
        (run_dir / "container-logs.txt").write_text(logs.stdout + logs.stderr)

    except Exception as exc:
        summary["status"] = "failed"
        summary["failure"] = f"{type(exc).__name__}: {exc}"
        summary["failed_at_utc"] = utc_now()
        if container_id:
            inspect_result = subprocess.run(
                ["docker", "inspect", container_id],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
            (run_dir / "failure-container-inspect.stdout.json").write_text(inspect_result.stdout)
            (run_dir / "failure-container-inspect.stderr.txt").write_text(inspect_result.stderr)
            logs = subprocess.run(
                ["docker", "logs", container_id],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
            (run_dir / "failure-container-logs.txt").write_text(logs.stdout + logs.stderr)
        print(summary["failure"], file=sys.stderr)
    finally:
        cleanup: dict[str, Any] = {"started_at_utc": utc_now(), "actions": []}
        cleanup_errors: list[str] = []
        container_removed = container_id is None
        if container_id:
            try:
                inspect_result = subprocess.run(
                    ["docker", "inspect", container_id],
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    check=False,
                )
                if inspect_result.returncode == 0:
                    inspected = json.loads(inspect_result.stdout)[0]
                    write_json(run_dir / "container-inspect-before-cleanup.json", inspected)
                    labels = inspected.get("Config", {}).get("Labels") or {}
                    owned = labels.get("owner") == OWNER and labels.get("qualification-nonce") == nonce
                    mounts = inspected.get("Mounts") or []
                    bind_mount = next((m for m in mounts if m.get("Destination") == "/var/lib/postgresql"), None)
                    mounted_for_run = bool(
                        owned
                        and bind_mount
                        and bind_mount.get("Type") == "bind"
                        and Path(bind_mount.get("Source", "")).resolve() == pgdata.resolve()
                    )
                    if owned:
                        state = inspected.get("State", {}).get("Status")
                        if state == "running":
                            stopped = command(
                                run_dir,
                                "container-stop-cleanup",
                                ["docker", "stop", "--time", "30", container_id],
                                check=False,
                            )
                            cleanup["actions"].append({"container_id": container_id, "stop_returncode": stopped.returncode})
                        post_stop = subprocess.run(
                            ["docker", "inspect", container_id],
                            text=True,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            check=False,
                        )
                        if post_stop.returncode == 0:
                            write_json(
                                run_dir / "container-inspect-stopped.json",
                                json.loads(post_stop.stdout)[0],
                            )
                            remove_args = ["docker", "rm"]
                            if state == "running":
                                remove_args.append("--force")
                            remove_args.append(container_id)
                            removed = command(run_dir, "container-remove-cleanup", remove_args, check=False)
                            container_removed = removed.returncode == 0
                            cleanup["actions"].append(
                                {"container_id": container_id, "owned": True, "remove_returncode": removed.returncode}
                            )
                            if not container_removed:
                                cleanup_errors.append(f"owned container removal failed for {container_id}")
                        else:
                            container_removed = True
                            cleanup["actions"].append({"container_id": container_id, "already_absent": True})
                    else:
                        cleanup["actions"].append(
                            {"container_id": container_id, "owned": False, "removed": False, "reason": "ownership labels could not be verified"}
                        )
                        cleanup_errors.append(f"container ownership could not be verified for {container_id}")
                else:
                    container_removed = True
                    cleanup["actions"].append({"container_id": container_id, "already_absent": True})
            except Exception as cleanup_exc:
                cleanup_errors.append(f"container cleanup: {type(cleanup_exc).__name__}: {cleanup_exc}")
                cleanup["actions"].append({"container_id": container_id, "error": cleanup_errors[-1]})
        if network_id:
            try:
                inspect_result = subprocess.run(
                    ["docker", "network", "inspect", network_id],
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    check=False,
                )
                if inspect_result.returncode == 0:
                    inspected_network = json.loads(inspect_result.stdout)[0]
                    labels = inspected_network.get("Labels") or {}
                    owned = labels.get("owner") == OWNER and labels.get("qualification-nonce") == nonce
                    if owned:
                        removed = command(
                            run_dir,
                            "network-remove-cleanup",
                            ["docker", "network", "rm", network_id],
                            check=False,
                        )
                        cleanup["actions"].append(
                            {"network_id": network_id, "owned": True, "remove_returncode": removed.returncode}
                        )
                        if removed.returncode != 0:
                            cleanup_errors.append(f"owned network removal failed for {network_id}")
                    else:
                        cleanup["actions"].append(
                            {"network_id": network_id, "owned": False, "removed": False, "reason": "ownership labels could not be verified"}
                        )
                        cleanup_errors.append(f"network ownership could not be verified for {network_id}")
                else:
                    cleanup["actions"].append({"network_id": network_id, "already_absent": True})
            except Exception as cleanup_exc:
                cleanup_errors.append(f"network cleanup: {type(cleanup_exc).__name__}: {cleanup_exc}")
                cleanup["actions"].append({"network_id": network_id, "error": cleanup_errors[-1]})
        if pgdata.exists() and not pgdata.is_symlink() and pgdata.resolve().parent == run_dir.resolve():
            try:
                if not caps:
                    caps = json.loads(CAPS_PATH.read_text())
                data_cleanup = cleanup_owned_data(
                    run_dir,
                    pgdata,
                    run_id,
                    nonce,
                    caps,
                    mounted_for_run and container_removed,
                )
                cleanup["actions"].append({"data_directory": data_cleanup})
            except Exception as cleanup_exc:
                cleanup_errors.append(f"data cleanup: {type(cleanup_exc).__name__}: {cleanup_exc}")
                cleanup["actions"].append(
                    {
                        "data_directory": str(pgdata),
                        "removed": False,
                        "retained_for_safety": True,
                        "error": cleanup_errors[-1],
                    }
                )
        elif pgdata.is_symlink():
            cleanup_errors.append(f"refused symlink data path: {pgdata}")
            cleanup["actions"].append({"data_directory": str(pgdata), "removed": False, "reason": "refused to follow symlink"})
        cleanup["finished_at_utc"] = utc_now()
        cleanup["status"] = "passed" if not cleanup_errors else "failed"
        cleanup["errors"] = cleanup_errors
        write_json(run_dir / "cleanup.json", cleanup)
        summary["cleanup"] = cleanup
        if cleanup_errors:
            summary["status"] = "failed"
            summary["cleanup_status"] = "failed; any unreconciled owned path is retained and recorded"
            prior_failure = summary.get("failure")
            cleanup_failure = "; ".join(cleanup_errors)
            summary["failure"] = f"{prior_failure}; cleanup: {cleanup_failure}" if prior_failure else cleanup_failure
        elif summary.get("status") == "running":
            summary["status"] = "failed"
            summary["failure"] = "qualification ended without a final status"
        summary["finished_at_utc"] = utc_now()
        write_json(run_dir / "qualification.json", summary)

    print(json.dumps({"run_id": run_id, "status": summary["status"], "run_dir": str(run_dir)}, sort_keys=True))
    return 0 if summary["status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
