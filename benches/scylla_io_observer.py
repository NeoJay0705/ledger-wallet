#!/usr/bin/env python3
"""Read-only diskstats collector and post-run Scylla I/O window extractor."""
from __future__ import annotations

import argparse
import datetime as dt
import gzip
import hashlib
import json
import math
import os
import pathlib
import re
import sys
import time
from typing import Any, Iterable, Sequence

try:
    from . import scylla_wallet as wallet_runner
except ImportError:  # direct `python3 benches/scylla_io_observer.py` invocation
    import scylla_wallet as wallet_runner


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
SERVICE_CPUS = (6, 7, 14, 15)
SAMPLE_INTERVAL_NS = 1_000_000_000
MAX_SAMPLE_GAP_SECONDS = 2.5
DISKSTATS_FIELDS = (
    "reads_completed", "reads_merged", "sectors_read", "read_ms",
    "writes_completed", "writes_merged", "sectors_written", "write_ms",
    "ios_in_progress", "io_ms", "weighted_io_ms", "discards_completed",
    "discards_merged", "sectors_discarded", "discard_ms",
    "flush_requests", "flush_ms",
)
COUNTER_FIELDS = tuple(
    name for name in DISKSTATS_FIELDS if name != "ios_in_progress"
)
QUEUE_METRICS = (
    "scylla_io_queue_total_operations",
    "scylla_io_queue_total_delay_sec",
    "scylla_io_queue_total_exec_sec",
)
QUEUE_CLASSES = ("main", "commitlog")
QUEUE_SHARDS = ("0", "1")
DATA_MOUNTPOINT = "/var/lib/scylla"
EXPECTED_DATA_DEVICE = {"name": "sdb2", "major_minor": "8:18"}
EXPECTED_WHOLE_DEVICE = {"name": "sdb", "major_minor": "8:16"}
EXPECTED_PROFILE = {
    "node_count": 1,
    "shards": 2,
    "per_shard_pool": 3,
    "data_connections": 6,
    "users": 1024,
    "requests_per_user": 1000,
    "replication_factor": 1,
    "read_cl": "ONE",
    "write_cl": "ONE",
    "wallet_profile": "wallet-memory-lwt-v1",
    "account_state_source": "memory",
    "serial_cl": "LOCAL_SERIAL",
}
EXPECTED_WORKER_COUNTS = (256, 512)


class ObserverError(RuntimeError):
    pass


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="microseconds")


def write_json(path: pathlib.Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + f".tmp-{os.getpid()}")
    raw = (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n").encode()
    with temporary.open("xb") as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def set_observer_affinity() -> dict[str, Any]:
    expected = set(SERVICE_CPUS)
    if not hasattr(os, "sched_setaffinity") or not hasattr(os, "sched_getaffinity"):
        raise ObserverError("host cannot set and inspect the observer CPU affinity")
    try:
        os.sched_setaffinity(0, expected)
        actual = set(os.sched_getaffinity(0))
    except OSError as exc:
        raise ObserverError(f"cannot set observer affinity to CPUs {sorted(expected)}: {exc}") from exc
    if actual != expected:
        raise ObserverError(f"observer affinity {sorted(actual)} != expected {sorted(expected)}")
    return {"requested_cpus": sorted(expected), "actual_cpus": sorted(actual), "valid": True}


def _read_dev_file(path: pathlib.Path) -> tuple[int, int]:
    try:
        value = path.read_text(encoding="ascii").strip()
        major_text, minor_text = value.split(":", 1)
        major, minor = int(major_text), int(minor_text)
    except (OSError, ValueError) as exc:
        raise ObserverError(f"cannot read block-device identity from {path}: {exc}") from exc
    if major < 0 or minor < 0:
        raise ObserverError(f"invalid block-device major/minor in {path}: {value!r}")
    return major, minor


def _reject_composite_sysfs_device(path: pathlib.Path, role: str) -> None:
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise ObserverError(f"{role} sysfs block device is unresolved: {exc}") from exc
    if "/virtual/" in resolved.as_posix():
        raise ObserverError(f"{role} maps to a virtual/composite block device: {resolved}")
    slaves = resolved / "slaves"
    try:
        slave_names = sorted(item.name for item in slaves.iterdir()) if slaves.exists() else []
    except OSError as exc:
        raise ObserverError(f"cannot inspect {role} block-device slaves: {exc}") from exc
    if slave_names:
        raise ObserverError(f"{role} maps through composite/slave devices: {slave_names}")


def resolve_block_identity(host_st_dev: int,
                           sys_root: pathlib.Path = pathlib.Path("/sys")) -> dict[str, Any]:
    """Resolve a stat st_dev to one block device/partition and its whole disk."""
    data_major, data_minor = os.major(host_st_dev), os.minor(host_st_dev)
    sys_root = sys_root.resolve()
    data_link = sys_root / "dev" / "block" / f"{data_major}:{data_minor}"
    try:
        data_sysfs = data_link.resolve(strict=True)
    except OSError as exc:
        raise ObserverError(
            f"data st_dev {data_major}:{data_minor} has no /sys/dev/block mapping: {exc}") from exc
    try:
        data_sysfs.relative_to(sys_root)
    except ValueError as exc:
        raise ObserverError(f"data block-device sysfs mapping escapes {sys_root}: {data_sysfs}") from exc
    _reject_composite_sysfs_device(data_sysfs, "data device")
    if _read_dev_file(data_sysfs / "dev") != (data_major, data_minor):
        raise ObserverError("data-device st_dev does not match its sysfs major/minor")

    partition_number: int | None = None
    partition_file = data_sysfs / "partition"
    if partition_file.exists():
        try:
            partition_number = int(partition_file.read_text(encoding="ascii").strip())
        except (OSError, ValueError) as exc:
            raise ObserverError(f"invalid partition number in {partition_file}: {exc}") from exc
        whole_sysfs = data_sysfs.parent
        if (whole_sysfs / "partition").exists():
            raise ObserverError("nested partition mappings are not supported")
        if whole_sysfs.parent.name != "block":
            raise ObserverError(f"partition parent is not a whole block device: {whole_sysfs}")
    else:
        whole_sysfs = data_sysfs
    _reject_composite_sysfs_device(whole_sysfs, "whole device")
    whole_major, whole_minor = _read_dev_file(whole_sysfs / "dev")
    whole_link = sys_root / "dev" / "block" / f"{whole_major}:{whole_minor}"
    try:
        canonical_whole = whole_link.resolve(strict=True)
    except OSError as exc:
        raise ObserverError(f"whole-device {whole_major}:{whole_minor} has no sysfs mapping: {exc}") from exc
    if canonical_whole != whole_sysfs.resolve(strict=True):
        raise ObserverError("whole-device sysfs mapping is not canonical")

    return {
        "host_st_dev": host_st_dev,
        "data_device": {
            "name": data_sysfs.name,
            "major": data_major,
            "minor": data_minor,
            "major_minor": f"{data_major}:{data_minor}",
            "sysfs_path": str(data_sysfs),
            "partition_number": partition_number,
        },
        "whole_device": {
            "name": whole_sysfs.name,
            "major": whole_major,
            "minor": whole_minor,
            "major_minor": f"{whole_major}:{whole_minor}",
            "sysfs_path": str(whole_sysfs.resolve(strict=True)),
        },
        "whole_and_data_are_same_device": (whole_major, whole_minor) == (data_major, data_minor),
        "physical_disk_count": 1,
        "multi_or_composite_device_rejected": True,
    }


def resolve_device_profile(data_path: pathlib.Path,
                           sys_root: pathlib.Path = pathlib.Path("/sys")) -> dict[str, Any]:
    """Resolve the runner data path's st_dev to a whole disk and partition."""
    try:
        resolved_data_path = data_path.expanduser().resolve(strict=True)
        host_st_dev = resolved_data_path.stat().st_dev
    except OSError as exc:
        raise ObserverError(f"cannot stat data path {data_path}: {exc}") from exc
    identity = resolve_block_identity(host_st_dev, sys_root)
    return {
        "schema_version": 1,
        "captured_utc": utc_now(),
        "data_path": str(resolved_data_path),
        **identity,
    }


def validate_calibrated_device_profile(profile: dict[str, Any]) -> dict[str, Any]:
    """Require the exact single-disk identity used by the completed calibration."""
    data = profile.get("data_device")
    whole = profile.get("whole_device")
    observed = {
        "data_device": ({"name": data.get("name"), "major_minor": data.get("major_minor")}
                        if isinstance(data, dict) else None),
        "whole_device": ({"name": whole.get("name"), "major_minor": whole.get("major_minor")}
                         if isinstance(whole, dict) else None),
    }
    expected = {"data_device": EXPECTED_DATA_DEVICE, "whole_device": EXPECTED_WHOLE_DEVICE}
    if observed != expected:
        raise ObserverError(
            f"resolved data filesystem device differs from calibrated identity: "
            f"expected={expected}, observed={observed}")
    return {"valid": True, "expected": expected, "observed": observed}


def parse_diskstats(payload: str, profile: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Parse exactly the selected whole disk and data partition, never summing them."""
    selected: dict[tuple[int, int], dict[str, Any]] = {}
    expected: dict[tuple[int, int], str] = {}
    role_identity: dict[str, tuple[int, int]] = {}
    for role in ("whole_device", "data_device"):
        item = profile.get(role)
        if not isinstance(item, dict):
            if role == "data_device" and profile.get("whole_and_data_are_same_device") is False:
                raise ObserverError(f"device profile is missing {role}")
            continue
        identity = (int(item["major"]), int(item["minor"]))
        name = str(item["name"])
        if identity in expected and expected[identity] != name:
            raise ObserverError(f"device profile assigns conflicting names to {identity}")
        expected[identity] = name
        role_identity[role] = identity

    for line_number, line in enumerate(payload.splitlines(), 1):
        parts = line.split()
        if len(parts) < 3:
            continue
        try:
            identity = (int(parts[0]), int(parts[1]))
        except ValueError:
            continue
        if identity not in expected:
            continue
        if len(parts) != 3 + len(DISKSTATS_FIELDS):
            raise ObserverError(
                f"/proc/diskstats line {line_number} for selected device has {len(parts)-3} fields; "
                f"expected {len(DISKSTATS_FIELDS)}")
        name = parts[2]
        if name != expected[identity]:
            raise ObserverError(f"diskstats identity {identity} is named {name!r}, expected {expected[identity]!r}")
        if identity in selected:
            raise ObserverError(f"/proc/diskstats repeats device identity {identity}")
        try:
            values = [int(value) for value in parts[3:]]
        except ValueError as exc:
            raise ObserverError(f"/proc/diskstats line {line_number} has a noninteger field") from exc
        if any(value < 0 for value in values):
            raise ObserverError(f"/proc/diskstats line {line_number} has a negative field")
        selected[identity] = {
            "major": identity[0], "minor": identity[1], "name": name,
            "major_minor": f"{identity[0]}:{identity[1]}",
            "fields": dict(zip(DISKSTATS_FIELDS, values)),
        }

    missing = [f"{identity[0]}:{identity[1]} ({name})"
               for identity, name in expected.items() if identity not in selected]
    if missing:
        raise ObserverError(f"selected device(s) are missing from /proc/diskstats: {missing}")
    by_role: dict[str, dict[str, Any]] = {}
    if "whole_device" not in role_identity:
        raise ObserverError("device profile has no whole_device entry")
    by_role["whole_device"] = selected[role_identity["whole_device"]]
    by_role["data_device"] = selected[role_identity.get("data_device", role_identity["whole_device"])]
    return by_role


def read_diskstats_sample(profile: dict[str, Any],
                          diskstats_path: pathlib.Path = pathlib.Path("/proc/diskstats")) -> dict[str, Any]:
    try:
        payload = diskstats_path.read_text(encoding="ascii")
    except OSError as exc:
        raise ObserverError(f"cannot read {diskstats_path}: {exc}") from exc
    return parse_diskstats(payload, profile)


def _write_json_line(handle: Any, value: dict[str, Any]) -> None:
    handle.write(json.dumps(value, sort_keys=True, allow_nan=False) + "\n")
    handle.flush()


def _current_data_st_dev(data_path: pathlib.Path) -> int:
    try:
        return data_path.expanduser().resolve(strict=True).stat().st_dev
    except OSError as exc:
        raise ObserverError(f"cannot stat data path during collection {data_path}: {exc}") from exc


def _validate_adjacent_diskstats(previous: dict[str, Any], current: dict[str, Any]) -> None:
    for role in ("whole_device", "data_device"):
        previous_device = previous["diskstats"][role]
        current_device = current["diskstats"][role]
        if (previous_device.get("major_minor") != current_device.get("major_minor")
                or previous_device.get("name") != current_device.get("name")):
            raise ObserverError(f"{role} identity changed between collector samples")
        for field_name in COUNTER_FIELDS:
            before = previous_device["fields"][field_name]
            after = current_device["fields"][field_name]
            if after < before:
                raise ObserverError(
                    f"{role}.{field_name} counter reset/wrapped between collector samples")


def collect_diskstats(output_dir: pathlib.Path, data_path: pathlib.Path,
                      stop_file: pathlib.Path, *, interval_seconds: float = 1.0,
                      sys_root: pathlib.Path = pathlib.Path("/sys"),
                      diskstats_path: pathlib.Path = pathlib.Path("/proc/diskstats")) -> dict[str, Any]:
    output_dir = output_dir.expanduser().resolve()
    stop_file = stop_file.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    result_path = output_dir / "collector-result.json"
    samples_path = output_dir / "samples.jsonl"
    profile_path = output_dir / "device-profile.json"
    ready_path = output_dir / "ready.json"
    if any(path.exists() for path in (result_path, samples_path, profile_path, ready_path)):
        raise ObserverError(f"observer output already contains artifacts: {output_dir}")
    if stop_file.exists():
        raise ObserverError(f"stop-file already exists before collector start: {stop_file}")

    affinity = set_observer_affinity()
    profile = resolve_device_profile(data_path, sys_root)
    calibrated_identity = validate_calibrated_device_profile(profile)
    profile["observer_affinity"] = affinity
    profile["calibrated_identity_validation"] = calibrated_identity
    profile_bytes = (json.dumps(profile, indent=2, sort_keys=True, allow_nan=False) + "\n").encode()
    profile_sha = hashlib.sha256(profile_bytes).hexdigest()
    write_json(profile_path, profile)
    errors: list[str] = []
    samples: list[dict[str, Any]] = []
    gaps: list[float] = []
    started_utc = utc_now()
    started_wall_ns = time.time_ns()
    started_mono_ns = time.monotonic_ns()
    sample_index = 0
    collector_failed = False
    try:
        with samples_path.open("x", encoding="utf-8", buffering=1) as handle:
            due_ns = time.monotonic_ns()
            while True:
                if sample_index > 0:
                    while not stop_file.exists():
                        remaining_ns = due_ns - time.monotonic_ns()
                        if remaining_ns <= 0:
                            break
                        time.sleep(min(remaining_ns / 1_000_000_000, 0.1))
                    if stop_file.exists():
                        break
                diskstats = read_diskstats_sample(profile, diskstats_path)
                observed_st_dev = _current_data_st_dev(data_path)
                sample = {
                    "sample_index": sample_index,
                    "sampled_utc": utc_now(),
                    "time_unix_ns": time.time_ns(),
                    "time_monotonic_ns": time.monotonic_ns(),
                    "observer_pid": os.getpid(),
                    "host_st_dev": observed_st_dev,
                    "data_device_major_minor": f"{os.major(observed_st_dev)}:{os.minor(observed_st_dev)}",
                    "whole_device_major_minor": profile["whole_device"]["major_minor"],
                    "diskstats": diskstats,
                }
                violation: str | None = None
                if samples:
                    gap_seconds = (sample["time_monotonic_ns"] - samples[-1]["time_monotonic_ns"]) / 1e9
                    gaps.append(gap_seconds)
                    if gap_seconds > MAX_SAMPLE_GAP_SECONDS:
                        violation = (f"sampling gap {gap_seconds:.6f}s exceeded "
                                     f"{MAX_SAMPLE_GAP_SECONDS}s")
                _write_json_line(handle, sample)
                samples.append(sample)
                sample_index += 1
                try:
                    if violation is not None:
                        raise ObserverError(violation)
                    _validate_disk_sample_identity(sample, profile)
                    if len(samples) > 1:
                        previous_sample = samples[-2]
                        previous_wall = previous_sample["time_unix_ns"]
                        previous_mono = previous_sample["time_monotonic_ns"]
                        if (sample["time_unix_ns"] <= previous_wall
                                or sample["time_monotonic_ns"] <= previous_mono):
                            raise ObserverError("collector wall or monotonic timestamps did not increase")
                        _validate_adjacent_diskstats(previous_sample, sample)
                except ObserverError as exc:
                    errors.append(str(exc))
                    collector_failed = True
                    break
                if sample_index == 1:
                    write_json(ready_path, {
                        "ready": True, "created_utc": utc_now(),
                        "first_sample": sample, "sample_path": str(samples_path.resolve()),
                        "profile_path": str(profile_path.resolve()),
                        "device_profile_sha256": profile_sha,
                        "observer_affinity": affinity,
                    })
                due_ns += int(interval_seconds * 1e9)
            handle.flush()
    except Exception as exc:
        errors.append(f"{type(exc).__name__}: {exc}")
        collector_failed = True

    max_gap = max(gaps) if gaps else None
    if len(samples) < 2:
        errors.append(f"only {len(samples)} diskstats sample(s); at least 2 are required")
    stopped_utc = utc_now()
    result = {
        "valid": not errors,
        "started_utc": started_utc,
        "stopped_utc": stopped_utc,
        "started_time_unix_ns": started_wall_ns,
        "started_time_monotonic_ns": started_mono_ns,
        "stop_file": str(stop_file),
        "stop_observed": stop_file.exists(),
        "stop_reason": ("stop_file" if stop_file.exists() and not collector_failed
                        else "collector_error_or_interruption"),
        "observer_pid": os.getpid(),
        "sample_file": str(samples_path.resolve()),
        "device_profile_file": str(profile_path.resolve()),
        "device_profile_sha256": profile_sha,
        "device_identity": {
            "host_st_dev": profile["host_st_dev"],
            "data_device": profile["data_device"],
            "whole_device": profile["whole_device"],
        },
        "observer_affinity": affinity,
        "sample_count": len(samples),
        "max_gap_seconds": max_gap,
        "max_gap_valid": max_gap is None or max_gap <= MAX_SAMPLE_GAP_SECONDS,
        "sample_interval_seconds": interval_seconds,
        "errors": errors,
    }
    if result["max_gap_valid"] is False:
        result["valid"] = False
    try:
        write_json(result_path, result)
    except Exception as exc:
        errors.append(f"cannot write collector-result.json: {type(exc).__name__}: {exc}")
        result["valid"] = False
        result["errors"] = errors
    return result


def read_observer_samples(path: pathlib.Path) -> list[dict[str, Any]]:
    samples: list[dict[str, Any]] = []
    try:
        handle = path.open("r", encoding="utf-8")
    except OSError as exc:
        raise ObserverError(f"cannot open observer samples {path}: {exc}") from exc
    with handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ObserverError(f"invalid observer sample JSON at line {line_number}: {exc}") from exc
            if not isinstance(value, dict):
                raise ObserverError(f"observer sample at line {line_number} is not an object")
            samples.append(value)
    return samples


def _finite_number(value: Any, context: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ObserverError(f"{context} is not a finite number: {value!r}")
    return float(value)


def _valid_ns(value: Any, context: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ObserverError(f"{context} must be a positive integer nanosecond timestamp")
    return value


def _profile_identity(profile: dict[str, Any]) -> dict[str, Any]:
    data = profile.get("data_device")
    whole = profile.get("whole_device")
    if not isinstance(data, dict) or not isinstance(whole, dict):
        raise ObserverError("device profile is missing data_device or whole_device")
    return {
        "host_st_dev": profile.get("host_st_dev"),
        "data_device_major_minor": data.get("major_minor"),
        "data_device_name": data.get("name"),
        "whole_device_major_minor": whole.get("major_minor"),
        "whole_device_name": whole.get("name"),
    }


def verify_trial_device_identity(profile: dict[str, Any], trial: dict[str, Any]) -> dict[str, Any]:
    expected_st_dev = profile.get("host_st_dev")
    io_validation = trial.get("io_properties_runtime_validation")
    if not isinstance(io_validation, dict) or io_validation.get("valid") is not True:
        raise ObserverError("runner I/O properties runtime validation is missing or invalid")
    nodes = io_validation.get("nodes")
    if not isinstance(nodes, dict) or set(nodes) != {"nodea"}:
        raise ObserverError("runner I/O validation must cover exactly nodea")
    device = nodes["nodea"].get("device_stats")
    if not isinstance(device, dict) or device.get("valid") is not True:
        raise ObserverError("runner nodea data-device stat evidence is invalid")
    observed_st_dev = device.get("host_owned_root_device_id")
    if observed_st_dev != expected_st_dev:
        raise ObserverError(
            f"observer/runner host st_dev mismatch: observer={expected_st_dev!r}, runner={observed_st_dev!r}")
    if (device.get("container_paths_share_device") is not True
            or device.get("container_device_matches_host_bind_source") is not True):
        raise ObserverError("runner container data/commitlog paths do not match the owned host bind device")
    observed_paths = device.get("observed_device_ids")
    expected_paths = ("/var/lib/scylla", "/var/lib/scylla/data", "/var/lib/scylla/commitlog")
    if not isinstance(observed_paths, dict) or any(
            observed_paths.get(path) != expected_st_dev for path in expected_paths):
        raise ObserverError("runner container data/commitlog st_dev values do not match observer identity")
    return {"valid": True, "observer_host_st_dev": expected_st_dev,
            "runner_host_st_dev": observed_st_dev, "node": "nodea"}


def verify_expected_trial_profile(result: dict[str, Any]) -> dict[str, Any]:
    config = result.get("config")
    if not isinstance(config, dict):
        raise ObserverError("service result is missing config")
    observed = {
        "node_count": config.get("node_count"),
        "shards": config.get("S", config.get("shards")),
        "per_shard_pool": config.get("P", config.get("per_shard_pool")),
        "data_connections": config.get("data_connection_target"),
        "workers": config.get("C", config.get("db_workers")),
        "users": config.get("users"),
        "requests_per_user": config.get("requests_per_user"),
        "replication_factor": config.get("replication_factor"),
        "read_cl": config.get("read_cl"),
        "write_cl": config.get("write_cl"),
        "wallet_profile": config.get("wallet_profile"),
        "account_state_source": config.get("account_state_source"),
        "serial_cl": config.get("serial_cl"),
    }
    mismatches = {name: {"expected": expected, "observed": observed[name]}
                  for name, expected in EXPECTED_PROFILE.items()
                  if observed[name] != expected}
    if observed["workers"] not in EXPECTED_WORKER_COUNTS:
        mismatches["workers"] = {
            "expected_one_of": list(EXPECTED_WORKER_COUNTS),
            "observed": observed["workers"],
        }
    endpoints = config.get("nodes")
    if not isinstance(endpoints, list) or len(endpoints) != 1 or not isinstance(endpoints[0], str):
        mismatches["nodes"] = {"expected": "one nodea endpoint", "observed": endpoints}
    if mismatches:
        raise ObserverError(f"service result does not match observer target profile: {mismatches}")
    return {"valid": True, "expected": {**EXPECTED_PROFILE,
                                           "workers_one_of": list(EXPECTED_WORKER_COUNTS)},
            "observed": observed,
            "nodes": endpoints}


def _validate_disk_sample_identity(sample: dict[str, Any], profile: dict[str, Any]) -> None:
    expected = _profile_identity(profile)
    observed = {
        "host_st_dev": sample.get("host_st_dev"),
        "data_device_major_minor": sample.get("data_device_major_minor"),
        "whole_device_major_minor": sample.get("whole_device_major_minor"),
    }
    for name in ("host_st_dev", "data_device_major_minor", "whole_device_major_minor"):
        if observed[name] != expected[name]:
            raise ObserverError(f"disk identity changed in observer sample: {name}: "
                                f"expected {expected[name]!r}, observed {observed[name]!r}")
    diskstats = sample.get("diskstats")
    if not isinstance(diskstats, dict):
        raise ObserverError("observer sample is missing diskstats mapping")
    for role in ("whole_device", "data_device"):
        item = diskstats.get(role)
        expected_identity = profile["whole_device"] if role == "whole_device" else profile["data_device"]
        if item is None and role == "data_device" and profile.get("whole_and_data_are_same_device") is True:
            item = diskstats.get("whole_device")
        if not isinstance(item, dict):
            raise ObserverError(f"observer sample is missing {role}")
        if (item.get("name") != expected_identity.get("name")
                or item.get("major_minor") != expected_identity.get("major_minor")):
            raise ObserverError(f"observer sample {role} identity differs from device profile")
        fields = item.get("fields")
        if not isinstance(fields, dict) or set(fields) != set(DISKSTATS_FIELDS):
            raise ObserverError(f"observer sample {role} does not have all 17 diskstats fields")
        for name, value in fields.items():
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ObserverError(f"observer sample {role}.{name} is not a nonnegative integer")


def _disk_device_window(role: str, samples: Sequence[dict[str, Any]],
                        profile: dict[str, Any], start_unix_ns: int,
                        end_unix_ns: int) -> dict[str, Any]:
    in_window = [sample for sample in samples
                 if start_unix_ns <= sample["time_unix_ns"] <= end_unix_ns]
    if len(in_window) < 2:
        raise ObserverError(f"{role} needs at least two observer samples inside measurement window")
    for sample in in_window:
        _validate_disk_sample_identity(sample, profile)
    first, last = in_window[0], in_window[-1]
    pair_interval_ns = last["time_monotonic_ns"] - first["time_monotonic_ns"]
    if pair_interval_ns <= 0:
        raise ObserverError(f"{role} sample monotonic timestamps are not increasing")
    gaps = [(right["time_monotonic_ns"] - left["time_monotonic_ns"]) / 1e9
            for left, right in zip(in_window, in_window[1:])]
    if gaps and max(gaps) > MAX_SAMPLE_GAP_SECONDS:
        raise ObserverError(f"{role} measurement-window sample gap exceeds {MAX_SAMPLE_GAP_SECONDS}s")

    first_device = first["diskstats"][role]
    last_device = last["diskstats"][role]
    deltas: dict[str, int] = {}
    for field_name in COUNTER_FIELDS:
        previous = None
        for sample in in_window:
            value = sample["diskstats"][role]["fields"][field_name]
            if previous is not None and value < previous:
                raise ObserverError(f"{role}.{field_name} counter reset/wrapped inside measurement window")
            previous = value
        delta = (last_device["fields"][field_name] - first_device["fields"][field_name])
        if delta < 0:
            raise ObserverError(f"{role}.{field_name} counter reset/wrapped across window")
        deltas[field_name] = delta

    elapsed_seconds = pair_interval_ns / 1e9
    elapsed_ms = elapsed_seconds * 1000
    reads = deltas["reads_completed"]
    writes = deltas["writes_completed"]
    flushes = deltas["flush_requests"]
    sectors_read = deltas["sectors_read"]
    sectors_written = deltas["sectors_written"]
    read_bytes = sectors_read * 512
    write_bytes = sectors_written * 512
    read_ms = deltas["read_ms"]
    write_ms = deltas["write_ms"]
    flush_ms = deltas["flush_ms"]
    return {
        "device": {"name": first_device["name"],
                   "major_minor": first_device["major_minor"]},
        "sample_count_in_window": len(in_window),
        "first_sample_unix_ns": first["time_unix_ns"],
        "last_sample_unix_ns": last["time_unix_ns"],
        "first_sample_offset_from_window_start_ns": first["time_unix_ns"] - start_unix_ns,
        "last_sample_offset_from_window_end_ns": last["time_unix_ns"] - end_unix_ns,
        "pair_interval_ns": pair_interval_ns,
        "pair_interval_seconds": elapsed_seconds,
        "delta": deltas,
        "read_bytes": read_bytes,
        "write_bytes": write_bytes,
        "read_mib_per_second": read_bytes / (1024**2) / elapsed_seconds,
        "write_mib_per_second": write_bytes / (1024**2) / elapsed_seconds,
        "read_iops": reads / elapsed_seconds,
        "write_iops": writes / elapsed_seconds,
        "total_iops": (reads + writes) / elapsed_seconds,
        "weighted_read_await_ms_per_op": read_ms / reads if reads else None,
        "weighted_write_await_ms_per_op": write_ms / writes if writes else None,
        "busy_percent": deltas["io_ms"] / elapsed_ms * 100 if elapsed_ms else None,
        "average_queue_estimate": deltas["weighted_io_ms"] / elapsed_ms if elapsed_ms else None,
        "flush_requests": flushes,
        "flush_ms": flush_ms,
        "average_flush_ms_per_request": flush_ms / flushes if flushes else None,
        "flush_average_status": "observed" if flushes else "N/A_no_flush_requests",
        "units_note": "diskstats sectors use 512-byte units; whole-device and partition views are not additive",
    }


_MARKER = re.compile(r"^# runner_sample_unix_ns (\d+)\b")


def read_runner_prometheus_scrapes(path: pathlib.Path) -> list[dict[str, Any]]:
    try:
        handle = gzip.open(path, "rt", encoding="utf-8", errors="strict")
    except OSError as exc:
        raise ObserverError(f"cannot open runner Prometheus gzip {path}: {exc}") from exc
    scrapes: list[dict[str, Any]] = []
    marker_ts: int | None = None
    lines: list[str] = []
    try:
        with handle:
            for line_number, line in enumerate(handle, 1):
                match = _MARKER.match(line.strip())
                if match:
                    if marker_ts is not None:
                        samples = wallet_runner.parse_prometheus_samples("".join(lines))
                        scrapes.append({"time_unix_ns": marker_ts, "samples": samples})
                    marker_ts = int(match.group(1))
                    lines = []
                elif marker_ts is not None:
                    lines.append(line)
        if marker_ts is not None:
            scrapes.append({"time_unix_ns": marker_ts,
                            "samples": wallet_runner.parse_prometheus_samples("".join(lines))})
    except (OSError, EOFError) as exc:
        raise ObserverError(f"cannot read runner Prometheus gzip {path}: {exc}") from exc
    if not scrapes:
        raise ObserverError(f"runner Prometheus file has no runner_sample_unix_ns markers: {path}")
    timestamps = [scrape["time_unix_ns"] for scrape in scrapes]
    if any(right <= left for left, right in zip(timestamps, timestamps[1:])):
        raise ObserverError("runner Prometheus scrape marker timestamps are not strictly increasing")
    return scrapes


def _queue_sample_map(scrape: dict[str, Any], metric_name: str,
                      mountpoint: str) -> dict[tuple[tuple[str, str], ...], float]:
    rows: dict[tuple[tuple[str, str], ...], float] = {}
    for sample in scrape["samples"]:
        if sample.get("name") != metric_name:
            continue
        labels = sample.get("labels") or {}
        if labels.get("class") not in QUEUE_CLASSES or labels.get("shard") not in QUEUE_SHARDS:
            continue
        if labels.get("mountpoint") != mountpoint:
            continue
        key = tuple(sorted((str(name), str(value)) for name, value in labels.items()))
        if key in rows:
            raise ObserverError(f"duplicate Prometheus {metric_name} full-label series: {dict(key)}")
        value = _finite_number(sample.get("value"), f"Prometheus {metric_name}{dict(key)}")
        if value < 0:
            raise ObserverError(f"Prometheus {metric_name}{dict(key)} is negative")
        rows[key] = value
    return rows


def extract_io_queue_window(prom_path: pathlib.Path, start_unix_ns: int,
                            end_unix_ns: int,
                            expected_mountpoint: str = DATA_MOUNTPOINT) -> dict[str, Any]:
    scrapes = read_runner_prometheus_scrapes(prom_path)
    in_window = [scrape for scrape in scrapes
                 if start_unix_ns <= scrape["time_unix_ns"] <= end_unix_ns]
    if len(in_window) < 2:
        raise ObserverError("Prometheus marker window has fewer than two in-window scrapes")
    window_gap_seconds = [(right["time_unix_ns"] - left["time_unix_ns"]) / 1e9
                          for left, right in zip(in_window, in_window[1:])]
    if window_gap_seconds and max(window_gap_seconds) > MAX_SAMPLE_GAP_SECONDS:
        raise ObserverError("Prometheus marker window has a gap above 2.5 seconds")

    excluded_mountpoints: dict[str, int] = {}
    expected_pairs = {(category, shard) for category in QUEUE_CLASSES for shard in QUEUE_SHARDS}
    by_pair: dict[tuple[str, str], dict[str, Any]] = {}
    reset_errors: list[str] = []
    for scrape in in_window:
        for sample in scrape["samples"]:
            if sample.get("name") not in QUEUE_METRICS:
                continue
            labels = sample.get("labels") or {}
            if labels.get("class") not in QUEUE_CLASSES or labels.get("shard") not in QUEUE_SHARDS:
                continue
            if labels.get("mountpoint") != expected_mountpoint:
                mountpoint_key = str(labels.get("mountpoint", "<missing>"))
                excluded_mountpoints[mountpoint_key] = excluded_mountpoints.get(mountpoint_key, 0) + 1

    endpoint_maps: dict[str, list[dict[tuple[tuple[str, str], ...], float]]] = {
        metric: [_queue_sample_map(scrape, metric, expected_mountpoint) for scrape in in_window]
        for metric in QUEUE_METRICS
    }
    for metric, maps in endpoint_maps.items():
        expected_keys = set(maps[0])
        for sample_index, current_map in enumerate(maps[1:], 1):
            if set(current_map) != expected_keys:
                raise ObserverError(
                    f"{metric} data-device full-label series are missing or changed at "
                    f"in-window scrape {sample_index}")
    for pair in sorted(expected_pairs):
        pair_records: list[dict[str, Any]] = []
        first_scrape, last_scrape = in_window[0], in_window[-1]
        metric_keys: dict[str, set[tuple[tuple[str, str], ...]]] = {}
        for metric in QUEUE_METRICS:
            first_map = endpoint_maps[metric][0]
            last_map = endpoint_maps[metric][-1]
            keys = {key for key in first_map if dict(key).get("class") == pair[0]
                    and dict(key).get("shard") == pair[1]}
            final_keys = {key for key in last_map if dict(key).get("class") == pair[0]
                          and dict(key).get("shard") == pair[1]}
            if not keys or keys != final_keys:
                raise ObserverError(
                    f"missing or changed {metric} data-device series for {pair[0]}/shard-{pair[1]} "
                    f"between window endpoints")
            metric_keys[metric] = keys
        if not (metric_keys[QUEUE_METRICS[0]] == metric_keys[QUEUE_METRICS[1]]
                == metric_keys[QUEUE_METRICS[2]]):
            raise ObserverError(f"queue metric label sets differ for {pair[0]}/shard-{pair[1]}")

        for key in sorted(metric_keys[QUEUE_METRICS[0]]):
            values_by_metric: dict[str, tuple[float, float]] = {}
            for metric in QUEUE_METRICS:
                series = []
                for scrape, current_map in zip(in_window, endpoint_maps[metric]):
                    if key in current_map:
                        series.append((scrape["time_unix_ns"], current_map[key]))
                if len(series) < 2:
                    raise ObserverError(f"insufficient {metric} points for full-label series {dict(key)}")
                for (_, previous), (_, current) in zip(series, series[1:]):
                    if current < previous:
                        reset_errors.append(
                            f"{metric}{dict(key)} reset/wrapped within the measurement window")
                first_value = endpoint_maps[metric][0].get(key)
                last_value = endpoint_maps[metric][-1].get(key)
                if first_value is None or last_value is None:
                    raise ObserverError(f"missing {metric} endpoint value for {dict(key)}")
                values_by_metric[metric] = (first_value, last_value)
            if reset_errors:
                raise ObserverError(reset_errors[0])
            delta_ops = values_by_metric[QUEUE_METRICS[0]][1] - values_by_metric[QUEUE_METRICS[0]][0]
            delta_delay = values_by_metric[QUEUE_METRICS[1]][1] - values_by_metric[QUEUE_METRICS[1]][0]
            delta_exec = values_by_metric[QUEUE_METRICS[2]][1] - values_by_metric[QUEUE_METRICS[2]][0]
            if min(delta_ops, delta_delay, delta_exec) < 0:
                raise ObserverError(f"queue counter reset/wrapped for full-label series {dict(key)}")
            pair_records.append({
                "labels": dict(key),
                "delta_operations": delta_ops,
                "delta_delay_sec": delta_delay,
                "delta_exec_sec": delta_exec,
                "mean_queue_delay_ms_per_operation": (delta_delay * 1000 / delta_ops
                                                       if delta_ops else None),
                "mean_queue_exec_ms_per_operation": (delta_exec * 1000 / delta_ops
                                                      if delta_ops else None),
                "zero_operations_status": "N/A_no_operations" if delta_ops == 0 else None,
            })
        total_ops = sum(row["delta_operations"] for row in pair_records)
        total_delay = sum(row["delta_delay_sec"] for row in pair_records)
        total_exec = sum(row["delta_exec_sec"] for row in pair_records)
        by_pair[f"{pair[0]}/shard-{pair[1]}"] = {
            "series_count": len(pair_records), "full_label_series": pair_records,
            "delta_operations": total_ops, "delta_delay_sec": total_delay,
            "delta_exec_sec": total_exec,
            "mean_queue_delay_ms_per_operation": total_delay * 1000 / total_ops if total_ops else None,
            "mean_queue_exec_ms_per_operation": total_exec * 1000 / total_ops if total_ops else None,
            "zero_operations_status": "N/A_no_operations" if total_ops == 0 else None,
        }
    if set(by_pair) != {f"{category}/shard-{shard}" for category, shard in expected_pairs}:
        raise ObserverError("one or more required main/commitlog shard queue series are missing")
    return {
        "valid": True,
        "source": str(prom_path.resolve()),
        "source_sha256": hashlib.sha256(prom_path.read_bytes()).hexdigest(),
        "metric_names": list(QUEUE_METRICS),
        "expected_mountpoint": expected_mountpoint,
        "excluded_other_mountpoint_series_counts": excluded_mountpoints,
        "in_window_scrape_count": len(in_window),
        "window_max_gap_seconds": max(window_gap_seconds) if window_gap_seconds else None,
        "measurement_start_unix_ns": start_unix_ns,
        "measurement_end_unix_ns": end_unix_ns,
        "first_scrape_unix_ns": first_scrape["time_unix_ns"],
        "last_scrape_unix_ns": last_scrape["time_unix_ns"],
        "first_scrape_offset_from_window_start_ns": first_scrape["time_unix_ns"] - start_unix_ns,
        "last_scrape_offset_from_window_end_ns": last_scrape["time_unix_ns"] - end_unix_ns,
        "series_by_class_shard": by_pair,
        "limitation": "runner Prometheus snapshots are approximately 1 Hz; short intervals between snapshots can be missed",
    }


def analyze_trial(trial_dir: pathlib.Path, observer_dir: pathlib.Path) -> dict[str, Any]:
    trial_dir = trial_dir.expanduser().resolve(strict=True)
    observer_dir = observer_dir.expanduser().resolve(strict=True)
    service_path = trial_dir / "service" / "result.json"
    runner_path = trial_dir / "runner-result.json"
    profile_path = observer_dir / "device-profile.json"
    collector_path = observer_dir / "collector-result.json"
    samples_path = observer_dir / "samples.jsonl"
    prom_path = trial_dir / "prometheus" / "nodea.prom.gz"
    required_files = (service_path, runner_path, profile_path, collector_path,
                      samples_path, prom_path)
    missing = [str(path) for path in required_files if not path.is_file()]
    if missing:
        raise ObserverError(f"required observer/trial artifact(s) missing: {missing}")

    service_result = wallet_runner.read_json_object(service_path)
    runner_result = wallet_runner.read_json_object(runner_path)
    profile = wallet_runner.read_json_object(profile_path)
    collector_result = wallet_runner.read_json_object(collector_path)
    collector_errors = collector_result.get("errors")
    if collector_result.get("valid") is not True or collector_errors != []:
        raise ObserverError(f"collector result is not valid and clean: {collector_result}")
    if (collector_result.get("stop_observed") is not True
            or collector_result.get("stop_reason") != "stop_file"):
        raise ObserverError("collector did not finish through the requested stop-file gate")
    if collector_result.get("max_gap_valid") is not True:
        raise ObserverError("collector maximum sample gap exceeded its 2.5 second limit")
    reported_affinity = collector_result.get("observer_affinity")
    if (not isinstance(reported_affinity, dict)
            or reported_affinity.get("valid") is not True
            or reported_affinity.get("requested_cpus") != list(SERVICE_CPUS)
            or reported_affinity.get("actual_cpus") != list(SERVICE_CPUS)):
        raise ObserverError("collector observer affinity evidence is invalid")
    profile_bytes = profile_path.read_bytes()
    profile_sha = hashlib.sha256(profile_bytes).hexdigest()
    if collector_result.get("device_profile_sha256") != profile_sha:
        raise ObserverError("collector result device-profile SHA-256 does not match artifact")
    expected_identity = _profile_identity(profile)
    reported_identity = collector_result.get("device_identity")
    if (not isinstance(reported_identity, dict)
            or reported_identity.get("host_st_dev") != expected_identity["host_st_dev"]
            or reported_identity.get("data_device", {}).get("major_minor") != expected_identity["data_device_major_minor"]
            or reported_identity.get("data_device", {}).get("name") != expected_identity["data_device_name"]
            or reported_identity.get("whole_device", {}).get("major_minor") != expected_identity["whole_device_major_minor"]
            or reported_identity.get("whole_device", {}).get("name") != expected_identity["whole_device_name"]):
        raise ObserverError("collector-result device identity differs from device-profile.json")
    samples = read_observer_samples(samples_path)
    if len(samples) != collector_result.get("sample_count"):
        raise ObserverError("collector sample_count does not match samples.jsonl line count")
    if len(samples) < 2:
        raise ObserverError("observer has fewer than two diskstats samples")
    sample_timestamps = []
    for sample in samples:
        _validate_disk_sample_identity(sample, profile)
        time_unix_ns = _valid_ns(sample.get("time_unix_ns"), "observer time_unix_ns")
        time_monotonic_ns = _valid_ns(sample.get("time_monotonic_ns"), "observer time_monotonic_ns")
        sample_timestamps.append((time_unix_ns, time_monotonic_ns))
    for before, after in zip(sample_timestamps, sample_timestamps[1:]):
        if after[0] <= before[0] or after[1] <= before[1]:
            raise ObserverError("observer wall or monotonic sample timestamps are not strictly increasing")
    full_max_gap = max((after[1] - before[1]) / 1e9
                       for before, after in zip(sample_timestamps, sample_timestamps[1:]))
    if full_max_gap > MAX_SAMPLE_GAP_SECONDS:
        raise ObserverError("observer samples contain a gap above 2.5 seconds")
    reported_max_gap = collector_result.get("max_gap_seconds")
    if (isinstance(reported_max_gap, bool) or not isinstance(reported_max_gap, (int, float))
            or not math.isfinite(reported_max_gap)
            or abs(float(reported_max_gap) - full_max_gap) > 1e-6):
        raise ObserverError("collector-result max_gap_seconds differs from samples.jsonl")

    profile_validation = verify_expected_trial_profile(service_result)
    identity_validation = verify_trial_device_identity(profile, runner_result)
    measurement = service_result.get("measurement")
    if not isinstance(measurement, dict) or measurement.get("started") is not True:
        raise ObserverError("service result does not contain a started measurement window")
    start_ns = _valid_ns(measurement.get("start_unix_ns"), "measurement.start_unix_ns")
    end_ns = _valid_ns(measurement.get("end_unix_ns"), "measurement.end_unix_ns")
    if end_ns <= start_ns:
        raise ObserverError("service measurement window end is not after start")

    in_window = [sample for sample in samples if start_ns <= sample["time_unix_ns"] <= end_ns]
    if len(in_window) < 2:
        raise ObserverError("diskstats observer has fewer than two samples inside service window")
    whole = _disk_device_window("whole_device", samples, profile, start_ns, end_ns)
    data = _disk_device_window("data_device", samples, profile, start_ns, end_ns)
    queue_metrics = extract_io_queue_window(prom_path, start_ns, end_ns)
    return {
        "schema_version": 1,
        "valid": True,
        "created_utc": utc_now(),
        "trial_dir": str(trial_dir),
        "observer_dir": str(observer_dir),
        "service_result_file": str(service_path),
        "runner_result_file": str(runner_path),
        "measurement_window": {
            "start_unix_ns": start_ns, "end_unix_ns": end_ns,
            "duration_ns": end_ns - start_ns,
            "diskstats_sample_count": len(in_window),
            "observer_max_gap_seconds": full_max_gap,
        },
        "profile_validation": profile_validation,
        "disk_identity_validation": identity_validation,
        "device_profile": profile,
        "diskstats": {
            "whole_device": whole,
            "data_partition": data,
            "views_are_not_additive": True,
            "primary_physical_device_for_flush": whole["device"],
        },
        "scylla_queue_metrics": queue_metrics,
        "service_result_valid": service_result.get("valid"),
        "service_request_summary": service_result.get("requests"),
        "interpretation_note": (
            "This artifact reports sampled disk and Scylla queue counters only; it does not assign a bottleneck cause."),
    }


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--collect", action="store_true", help="collect host /proc/diskstats until stop-file exists")
    mode.add_argument("--analyze", action="store_true", help="extract disk and Scylla queue deltas for a completed trial")
    parser.add_argument("--output-dir", type=pathlib.Path,
                        help="collector artifact directory (required with --collect)")
    parser.add_argument("--data-path", type=pathlib.Path,
                        help="runner data filesystem path used to resolve host st_dev (required with --collect)")
    parser.add_argument("--stop-file", type=pathlib.Path,
                        help="collector stops normally when this file exists (required with --collect)")
    parser.add_argument("--trial-dir", type=pathlib.Path,
                        help="completed runner trial directory (required with --analyze)")
    parser.add_argument("--observer-dir", type=pathlib.Path,
                        help="collector artifact directory (required with --analyze)")
    parser.add_argument("--output", type=pathlib.Path,
                        help="analysis JSON output path (required with --analyze)")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    if args.collect:
        missing = [name for name, value in (("--output-dir", args.output_dir),
                                            ("--data-path", args.data_path),
                                            ("--stop-file", args.stop_file)) if value is None]
        if missing:
            parser.error("--collect requires " + ", ".join(missing))
        try:
            result = collect_diskstats(args.output_dir, args.data_path, args.stop_file)
            print(json.dumps(result, indent=2, sort_keys=True, allow_nan=False))
            return 0 if result.get("valid") is True else 1
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            try:
                args.output_dir.mkdir(parents=True, exist_ok=True)
                write_json(args.output_dir / "collector-result.json", {
                    "valid": False, "errors": [error], "sample_count": 0,
                    "max_gap_seconds": None, "finished_utc": utc_now(),
                })
            except Exception:
                pass
            print(error, file=sys.stderr)
            return 1
    missing = [name for name, value in (("--trial-dir", args.trial_dir),
                                        ("--observer-dir", args.observer_dir),
                                        ("--output", args.output)) if value is None]
    if missing:
        parser.error("--analyze requires " + ", ".join(missing))
    try:
        result = analyze_trial(args.trial_dir, args.observer_dir)
    except Exception as exc:
        result = {"schema_version": 1, "valid": False,
                  "created_utc": utc_now(),
                  "trial_dir": str(args.trial_dir),
                  "observer_dir": str(args.observer_dir),
                  "errors": [f"{type(exc).__name__}: {exc}"]}
    try:
        write_json(args.output, result)
    except Exception as exc:
        print(f"cannot write analysis output {args.output}: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, sort_keys=True, allow_nan=False))
    return 0 if result.get("valid") is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
