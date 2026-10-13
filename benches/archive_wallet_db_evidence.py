#!/usr/bin/env python3
"""Build and verify a compact, reproducible wallet DB benchmark evidence bundle."""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
import os
from pathlib import Path, PurePosixPath
import shutil
import sys
import tarfile
import tempfile
from typing import Any, Mapping, Sequence

RUN_ID = "run-20261013T124123Z-3216cf07f3"
RAW_PREFIX = "wallet-db-compare"
EXPECTED_REQUESTS = 1_024_000
EXPECTED_USERS = 1_024
CPU_DENOMINATOR = 2
MIB = 1024 * 1024
DISK_FIELDS = (
    "reads_completed", "reads_merged", "sectors_read", "read_ms",
    "writes_completed", "writes_merged", "sectors_written", "write_ms",
    "ios_in_progress", "io_ms", "weighted_io_ms", "discards_completed",
    "discards_merged", "sectors_discarded", "discard_ms", "flush_requests",
    "flush_ms",
)
STAGES = (
    "account_wait", "admission_wait", "compute", "db_queue_wait", "db_write",
    "reply", "cql_write_lwt", "pool_wait", "sql_transaction",
)
PERCENTILES = ("p50", "p95", "p99", "p999", "max")
CASE_FIELDS = [
    "order", "case_id", "case_kind", "repeat", "backend", "connections",
    "window_seconds", "rps", "requests_expected", "requests_attempted",
    "requests_accepted", "requests_applied", "replies_received", "workload_errors",
    "unexpected_business_results", "driver_queries", "driver_errors",
    "driver_request_timeouts", "driver_connection_timeouts", "driver_retries",
    "driver_counters_valid", "measured_select_requests", "driver_query_delta",
    "internal_write_operations", "measured_sql_transaction_count",
    "measured_cql_request_count", "pg_function_calls", "pg_ledger_insert_attempts",
    "pg_account_updates", "pg_duplicate_conflicts", "cql_write_lwt_count",
    "backend_in_flight_peak", "accounts_expected", "accounts_verified",
    "ledger_rows_expected", "ledger_rows_seen", "verification_valid",
    "case_status", "service_status", "service_valid", "ready_valid", "events_valid",
    "result_valid", "metrics_valid", "monitor_window_valid", "cleanup_status",
    "cleanup_complete", "cleanup_errors", "artifact_source_path", "artifact_source_sha256",
    "service_result_source_path", "service_result_source_sha256",
    "service_events_source_path", "service_events_source_sha256",
]
for _stage in ("end_to_end", *STAGES):
    for _metric in ("count", *PERCENTILES):
        CASE_FIELDS.append(f"latency_{_stage}_{_metric}_ns")

RESOURCE_FIELDS = [
    "case_id", "backend", "source_kind", "source_path", "source_sha256",
    "sample_index", "sample_unix_ns", "window_start_unix_ns", "window_end_unix_ns",
    "offset_from_window_start_ns", "offset_from_window_end_ns", "window_relation",
    "boundary_role", "selection_roles", "cgroup_cpu_usage_usec", "cgroup_cpu_user_usec",
    "cgroup_cpu_system_usec", "memory_current_bytes", "memory_peak_bytes",
    "memory_swap_current_bytes", "memory_oom", "memory_oom_kill", "postgres_pss_kb",
    "cgroup_io_device", "cgroup_io_rbytes", "cgroup_io_wbytes", "cgroup_io_rios",
    "cgroup_io_wios", "postgres_wal_bytes", "postgres_wal_fsyncs",
    "postgres_wal_fsync_time_ms", "whole_device_major_minor", "data_device_major_minor",
]
for _role in ("whole_device", "data_device"):
    for _field in DISK_FIELDS:
        RESOURCE_FIELDS.append(f"{_role}_{_field}")


class EvidenceError(RuntimeError):
    pass


def canonical_json(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2,
                       allow_nan=False) + "\n").encode("utf-8")


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def file_digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def relpath(value: str) -> str:
    p = PurePosixPath(value)
    if p.is_absolute() or not p.parts or any(part in ("", ".", "..") for part in p.parts):
        raise EvidenceError(f"unsafe evidence path: {value!r}")
    return p.as_posix()


class Source:
    """Read sources relative to the root of target/wallet-db-compare."""

    def __init__(self, path: Path):
        root = path.expanduser().resolve(strict=True)
        if not root.is_dir():
            raise EvidenceError(f"source root is not a directory: {root}")
        if not (root / "runs").is_dir() and (root / RAW_PREFIX / "runs").is_dir():
            root = root / RAW_PREFIX
        if not (root / "runs").is_dir():
            raise EvidenceError(f"source root has no runs directory: {root}")
        self.root = root

    def read(self, name: str) -> bytes:
        name = relpath(name)
        path = self.root.joinpath(*PurePosixPath(name).parts)
        if path.is_symlink():
            raise EvidenceError(f"source is a symlink: {name}")
        try:
            resolved = path.resolve(strict=True)
        except FileNotFoundError as exc:
            raise EvidenceError(f"missing source file: {name}") from exc
        if not resolved.is_relative_to(self.root) or not resolved.is_file():
            raise EvidenceError(f"source is not a regular file below source root: {name}")
        return resolved.read_bytes()

    def ref(self, name: str, payload: bytes | None = None) -> dict[str, Any]:
        data = self.read(name) if payload is None else payload
        return {"path": f"{RAW_PREFIX}/{relpath(name)}", "bytes": len(data), "sha256": digest(data)}


def read_json(source: Source, name: str) -> tuple[dict[str, Any], bytes]:
    data = source.read(name)
    try:
        value = json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EvidenceError(f"invalid JSON in {name}: {exc}") from exc
    if not isinstance(value, dict):
        raise EvidenceError(f"expected JSON object in {name}")
    return value, data


def read_jsonl_bytes(data: bytes, name: str) -> list[dict[str, Any]]:
    rows = []
    for line_no, line in enumerate(data.splitlines(), 1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise EvidenceError(f"invalid JSONL at {name}:{line_no}: {exc}") from exc
        if not isinstance(value, dict):
            raise EvidenceError(f"expected JSON object at {name}:{line_no}")
        rows.append(value)
    if not rows:
        raise EvidenceError(f"empty JSONL source: {name}")
    return rows


def read_jsonl(source: Source, name: str) -> tuple[list[dict[str, Any]], bytes]:
    data = source.read(name)
    return read_jsonl_bytes(data, name), data


def require_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise EvidenceError(f"{label} is not an integer")
    return value


def require_true(value: Any, label: str) -> None:
    if value is not True:
        raise EvidenceError(f"{label} did not pass")


def file_manifest(root: Path) -> tuple[list[dict[str, Any]], int]:
    root = root.expanduser().resolve(strict=True)
    if (root / RAW_PREFIX / "runs").is_dir() and not (root / "runs").is_dir():
        root = root / RAW_PREFIX
    rows = []
    for current, dirs, files in os.walk(root, topdown=True, followlinks=False):
        base = Path(current)
        for name in dirs + files:
            path = base / name
            if path.is_symlink():
                raise EvidenceError(f"raw evidence contains a symlink: {path}")
        for name in files:
            path = base / name
            if not path.is_file():
                raise EvidenceError(f"raw evidence is not a regular file: {path}")
            rows.append({"path": f"{RAW_PREFIX}/{path.relative_to(root).as_posix()}",
                         "bytes": path.stat().st_size, "sha256": file_digest(path)})
    rows.sort(key=lambda row: row["path"])
    return rows, sum(row["bytes"] for row in rows)


def render_manifest(rows: Sequence[Mapping[str, Any]]) -> bytes:
    out = io.StringIO(newline="")
    writer = csv.DictWriter(out, fieldnames=("path", "bytes", "SHA-256"), lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow({"path": row["path"], "bytes": row["bytes"], "SHA-256": row["sha256"]})
    return out.getvalue().encode("utf-8")


def parse_manifest(data: bytes) -> list[dict[str, Any]]:
    try:
        reader = csv.DictReader(io.StringIO(data.decode("utf-8"), newline=""))
        if reader.fieldnames != ["path", "bytes", "SHA-256"]:
            raise EvidenceError("original-file-manifest.csv has an unexpected header")
        rows, seen = [], set()
        for row in reader:
            name = relpath(row["path"])
            value, sha = int(row["bytes"]), row["SHA-256"]
            if not name.startswith(RAW_PREFIX + "/") or name in seen:
                raise EvidenceError(f"invalid or duplicate manifest path: {name}")
            if value < 0 or len(sha) != 64 or any(c not in "0123456789abcdef" for c in sha):
                raise EvidenceError(f"invalid size or sha256 for {name}")
            seen.add(name)
            rows.append({"path": name, "bytes": value, "sha256": sha})
        if not rows or [r["path"] for r in rows] != sorted(r["path"] for r in rows):
            raise EvidenceError("manifest is empty or paths are not sorted")
        return rows
    except (UnicodeDecodeError, KeyError, ValueError, csv.Error) as exc:
        raise EvidenceError(f"cannot parse original-file-manifest.csv: {exc}") from exc


def verify_archive(path: Path, manifest: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    path = path.expanduser().resolve(strict=True)
    expected = {row["path"]: (int(row["bytes"]), row["sha256"]) for row in manifest}
    seen, total = set(), 0
    try:
        with tarfile.open(path, "r:gz") as tar:
            for member in tar:
                name = member.name.rstrip("/")
                parts = PurePosixPath(name)
                if parts.is_absolute() or any(part in ("", ".", "..") for part in parts.parts):
                    raise EvidenceError(f"unsafe raw archive member: {member.name!r}")
                if name == RAW_PREFIX and member.isdir():
                    continue
                if not name.startswith(RAW_PREFIX + "/"):
                    raise EvidenceError(f"unexpected member outside {RAW_PREFIX}/: {name}")
                if member.isdir():
                    continue
                if not member.isfile():
                    raise EvidenceError(f"raw archive has a non-regular member: {name}")
                if name in seen:
                    raise EvidenceError(f"duplicate archive member: {name}")
                seen.add(name)
                wanted = expected.get(name)
                if wanted is None:
                    raise EvidenceError(f"unmanifested archive member: {name}")
                stream = tar.extractfile(member)
                if stream is None:
                    raise EvidenceError(f"cannot read archive member: {name}")
                h, size = hashlib.sha256(), 0
                with stream:
                    for block in iter(lambda: stream.read(1024 * 1024), b""):
                        size += len(block)
                        h.update(block)
                if size != wanted[0] or h.hexdigest() != wanted[1]:
                    raise EvidenceError(f"archive member differs from original manifest: {name}")
                total += size
    except (OSError, tarfile.TarError) as exc:
        raise EvidenceError(f"cannot read raw archive {path}: {exc}") from exc
    missing = sorted(set(expected) - seen)
    if missing:
        raise EvidenceError(f"archive is missing {len(missing)} file(s), first: {missing[0]}")
    return {"path": str(path), "size_bytes": path.stat().st_size, "sha256": file_digest(path),
            "file_count": len(seen), "uncompressed_bytes": total}


def parse_key_values(value: Any, label: str) -> dict[str, int]:
    if not isinstance(value, str):
        raise EvidenceError(f"{label} is not text")
    result = {}
    for line in value.splitlines():
        parts = line.split()
        if len(parts) != 2 or parts[0] in result:
            raise EvidenceError(f"invalid {label} line: {line!r}")
        result[parts[0]] = int(parts[1])
    return result


def parse_io_stat(value: Any, label: str) -> dict[str, dict[str, int]]:
    if not isinstance(value, str):
        raise EvidenceError(f"{label} is not text")
    result = {}
    for line in value.splitlines():
        parts = line.split()
        if not parts or parts[0] in result:
            raise EvidenceError(f"invalid {label} line: {line!r}")
        counters = {}
        for item in parts[1:]:
            key, sep, raw = item.partition("=")
            if not sep or not raw.isdigit() or key in counters:
                raise EvidenceError(f"invalid {label} counter: {item!r}")
            counters[key] = int(raw)
        result[parts[0]] = counters
    if not result:
        raise EvidenceError(f"{label} has no device counters")
    return result


def parse_disk_line(line: str) -> tuple[str, str, dict[str, int]]:
    parts = line.split()
    if len(parts) != 3 + len(DISK_FIELDS):
        raise EvidenceError(f"diskstats row has {len(parts) - 3} counters, expected {len(DISK_FIELDS)}")
    return (f"{parts[0]}:{parts[1]}", parts[2],
            dict(zip(DISK_FIELDS, (int(v) for v in parts[3:]))))


def parse_host_disk(sample: Mapping[str, Any], kind: str) -> dict[str, Any]:
    result = {}
    if kind == "host_diskstats":
        raw = sample.get("diskstats")
        if not isinstance(raw, dict):
            raise EvidenceError("host diskstats sample has no parsed diskstats object")
        for role in ("whole_device", "data_device"):
            item = raw.get(role)
            if not isinstance(item, dict) or set(item.get("fields", {})) != set(DISK_FIELDS):
                raise EvidenceError(f"host diskstats sample has incomplete {role} fields")
            result[role] = {"major_minor": item.get("major_minor"), "name": item.get("name"),
                            "fields": {key: require_int(item["fields"][key], f"{role}.{key}")
                                       for key in DISK_FIELDS}}
    elif kind == "scylla_proc_host":
        raw = (sample.get("host") or {}).get("diskstats")
        if not isinstance(raw, str):
            raise EvidenceError("Scylla proc sample lacks host diskstats")
        parsed = {}
        for line in raw.splitlines():
            major_minor, name, fields = parse_disk_line(line)
            parsed[major_minor] = {"major_minor": major_minor, "name": name, "fields": fields}
        for role, key in (("whole_device", "8:16"), ("data_device", "8:18")):
            if key not in parsed:
                raise EvidenceError(f"Scylla proc host samples lack device {key}")
            result[role] = parsed[key]
    else:
        raise EvidenceError(f"unknown host disk sample kind: {kind}")
    if result["whole_device"]["major_minor"] != "8:16" or result["data_device"]["major_minor"] != "8:18":
        raise EvidenceError("host disk identity differs from calibrated 8:16 / 8:18 devices")
    return result


def event_counts(data: bytes, name: str) -> dict[str, int]:
    counts = {}
    for event in read_jsonl_bytes(data, name):
        phase = event.get("phase")
        if not isinstance(phase, str):
            raise EvidenceError(f"service event is missing phase in {name}")
        counts[phase] = counts.get(phase, 0) + 1
    return dict(sorted(counts.items()))


def summarize_latency(service: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    e2e = service.get("end_to_end_latency")
    stages = (service.get("stage_metrics") or {}).get("stages")
    if not isinstance(e2e, dict) or not isinstance(stages, dict):
        raise EvidenceError("service result lacks latency histograms")
    ns = {"end_to_end": {k: e2e.get(k) for k in ("count", *PERCENTILES)}}
    for stage in STAGES:
        value = stages.get(stage)
        if isinstance(value, dict):
            ns[stage] = {k: value.get(k) for k in ("count", *PERCENTILES)}
    ms = {}
    for stage, values in ns.items():
        ms[stage] = {k: (v / 1_000_000 if isinstance(v, (int, float)) else None)
                     for k, v in values.items() if k != "count"}
        ms[stage]["count"] = values.get("count")
    return ns, ms


def validate_case(backend: str, artifact: Mapping[str, Any],
                  service: Mapping[str, Any]) -> dict[str, Any]:
    cleanup = artifact.get("cleanup")
    if not isinstance(cleanup, dict) or not isinstance(cleanup.get("errors", []), list):
        raise EvidenceError("runner artifact has invalid cleanup evidence")
    errors = cleanup.get("errors") or []
    if backend == "postgres":
        ready = (artifact.get("ready_validation") or {}).get("valid") is True
        events = (artifact.get("events_validation") or {}).get("valid") is True
        result = (artifact.get("result_validation") or {}).get("valid") is True
        monitor = artifact.get("postgres_monitor") or {}
        window = monitor.get("measured_window") or {}
        metrics = (monitor.get("valid") is True and window.get("valid") is True
                   and (window.get("cgroup_io_validation") or {}).get("valid") is True)
        host = artifact.get("host_diskstats_measured_window") or {}
        disk = (host.get("valid") is True
                and (host.get("collector") or {}).get("valid") is True
                and (host.get("collector") or {}).get("max_gap_valid") is True)
        cleanup_ok = (artifact.get("cleanup_status") == "passed" and cleanup.get("status") == "passed"
                      and cleanup.get("container_removed") is True and cleanup.get("network_removed") is True
                      and cleanup.get("data_removed") is True and not errors)
        exit_code = (artifact.get("cli_process") or {}).get("returncode")
        out = {"case_status": artifact.get("status"), "service_status": service.get("status"),
               "service_valid": service.get("valid") is True, "cli_exit_code": exit_code,
               "ready_valid": ready, "events_valid": events, "result_valid": result,
               "metrics_valid": metrics, "host_diskstats_valid": disk,
               "monitor_window_valid": window.get("valid") is True,
               "cleanup_status": artifact.get("cleanup_status"), "cleanup_complete": cleanup_ok,
               "cleanup_errors": errors}
        if (artifact.get("status") != "passed" or exit_code != 0 or not ready or not events
                or not result or not metrics or not disk or not cleanup_ok):
            raise EvidenceError(f"PostgreSQL case failed one or more validations: {artifact.get('case_id')}")
    elif backend == "scylla":
        events = (artifact.get("event_profile_validation") or {}).get("valid") is True
        metrics = artifact.get("required_metrics_valid") is True
        monitor = artifact.get("monitor") or {}
        process = (monitor.get("process_window") or {}).get("valid") is True
        sockets = monitor.get("socket_window_stable") is True
        counter_errors = (monitor.get("counter_window_deltas") or {}).get("errors") or []
        cleanup_ok = (artifact.get("cleanup_status") == "clean" and cleanup.get("status") == "clean"
                      and cleanup.get("containers_stopped") is True and cleanup.get("network_removed") is True
                      and not cleanup.get("retained_data") and not errors)
        exit_code = artifact.get("cli_exit_code")
        ready = (artifact.get("ready_profile_validation") or {}).get("pool_ready_connections", 0) > 0
        out = {"case_status": artifact.get("status"), "service_status": service.get("status"),
               "service_valid": service.get("valid") is True, "cli_exit_code": exit_code,
               "ready_valid": ready, "events_valid": events, "result_valid": service.get("valid") is True,
               "metrics_valid": metrics, "monitor_window_valid": process and sockets,
               "counter_window_errors": counter_errors,
               "cleanup_status": artifact.get("cleanup_status"), "cleanup_complete": cleanup_ok,
               "cleanup_errors": errors}
        if (artifact.get("status") != "passed" or exit_code != 0 or not events or not metrics
                or not process or not sockets or counter_errors or not cleanup_ok):
            raise EvidenceError(f"Scylla case failed one or more validations: {artifact.get('case_id')}")
    else:
        raise EvidenceError(f"unknown backend: {backend}")
    require_true(service.get("valid"), "service result validity")
    if service.get("status") != "verified":
        raise EvidenceError("service result status is not verified")
    return out


def make_case(source: Source, item: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, bytes]]:
    backend, case_id = item.get("backend"), item.get("case_id")
    if backend not in ("postgres", "scylla") or not isinstance(case_id, str):
        raise EvidenceError("campaign row has unsupported backend/case id")
    if backend == "postgres":
        base = f"runs/{RUN_ID}/postgres-cases/{relpath(case_id)}"
        artifact_path = f"{base}/case-result.json"
    else:
        trial_id = item.get("source_trial_id")
        if not isinstance(trial_id, str):
            raise EvidenceError(f"{case_id} is missing source_trial_id")
        base = f"runs/{RUN_ID}/trials/{relpath(trial_id)}"
        artifact_path = f"{base}/runner-result.json"
    result_path, events_path = f"{base}/service/result.json", f"{base}/service/events.jsonl"
    artifact, artifact_bytes = read_json(source, artifact_path)
    service, service_bytes = read_json(source, result_path)
    events_bytes = source.read(events_path)
    validation = validate_case(backend, artifact, service)
    phases = event_counts(events_bytes, events_path)
    required_phases = ((artifact.get("events_validation") or {}).get("required_phases")
                       if backend == "postgres" else
                       (artifact.get("event_profile_validation") or {}).get("phases"))
    if not isinstance(required_phases, list) or any(phases.get(p, 0) == 0 for p in required_phases):
        raise EvidenceError(f"{case_id} service events lack required phases")
    requests, verification = service.get("requests"), service.get("verification")
    if not isinstance(requests, dict) or not isinstance(verification, dict):
        raise EvidenceError(f"{case_id} lacks request/verification evidence")
    for key in ("expected", "attempted", "accepted", "applied", "replies_received"):
        if require_int(requests.get(key), f"{case_id}.{key}") != EXPECTED_REQUESTS:
            raise EvidenceError(f"{case_id} is missing expected workload requests")
    for key in ("errors", "unexpected_business_results"):
        if require_int(requests.get(key), f"{case_id}.{key}") != 0:
            raise EvidenceError(f"{case_id} reports workload errors or business result mismatches")
    operations = requests.get("operation_counts") or {}
    if (operations.get("credit") != EXPECTED_REQUESTS // 2
            or operations.get("debit") != EXPECTED_REQUESTS // 2):
        raise EvidenceError(f"{case_id} does not have 512,000 credit and debit operations")
    for key, expected in (("accounts_expected", EXPECTED_USERS), ("accounts_verified", EXPECTED_USERS),
                          ("ledger_rows_expected", EXPECTED_REQUESTS), ("ledger_rows_seen", EXPECTED_REQUESTS)):
        if require_int(verification.get(key), f"{case_id}.verification.{key}") != expected:
            raise EvidenceError(f"{case_id} full verification count differs: {key}")
    require_true(verification.get("valid"), f"{case_id} verification")
    require_true(verification.get("complete"), f"{case_id} verification completeness")
    if require_int(verification.get("failures"), f"{case_id}.verification.failures") != 0:
        raise EvidenceError(f"{case_id} verification has failures")
    driver = service.get("driver_counters") or {}
    delta = driver.get("delta") or {}
    for key in ("errors", "request_timeouts", "connection_timeouts", "retries"):
        if require_int(delta.get(key), f"{case_id}.driver.{key}") != 0:
            raise EvidenceError(f"{case_id} driver {key} counter is nonzero")
    require_true(driver.get("valid"), f"{case_id} driver counter validity")
    measurement = service.get("measurement") or {}
    duration_ns = require_int(measurement.get("wall_duration_ns"), f"{case_id}.duration")
    start_ns = require_int(measurement.get("start_unix_ns"), f"{case_id}.start")
    end_ns = require_int(measurement.get("end_unix_ns"), f"{case_id}.end")
    if duration_ns <= 0 or end_ns <= start_ns or end_ns - start_ns != duration_ns:
        raise EvidenceError(f"{case_id} has invalid window timestamps")
    rps = measurement.get("rps")
    if not isinstance(rps, (int, float)) or isinstance(rps, bool) or not math.isfinite(rps):
        raise EvidenceError(f"{case_id} has invalid RPS")
    artifact_rps = (((artifact.get("cli_result") or {}).get("measurement") or {}).get("rps")
                    if backend == "scylla" else artifact.get("rps"))
    for label, value in (("runner", artifact_rps), ("campaign", item.get("rps"))):
        if not isinstance(value, (int, float)) or not math.isclose(float(value), float(rps), rel_tol=1e-10):
            raise EvidenceError(f"{case_id} {label} RPS differs from service result")
    latency_ns, latency_ms = summarize_latency(service)
    refs = [source.ref(artifact_path, artifact_bytes), source.ref(result_path, service_bytes),
            source.ref(events_path, events_bytes)]
    query = service.get("query_accounting") or requests.get("query_accounting") or {}
    stage_counters = (service.get("stage_metrics") or {}).get("counters") or {}
    db_accounting = service.get("postgres_accounting") or {} if backend == "postgres" else query
    row = {
        "order": int(item.get("order")), "case_id": case_id,
        "case_kind": "calibration" if case_id.startswith("pg-calibration-") else "formal",
        "repeat": item.get("repeat"), "backend": backend,
        "connections": artifact.get("pg_connections") if backend == "postgres" else artifact.get("data_connections"),
        "source_base": base, "window_seconds": duration_ns / 1e9,
        "window_start_unix_ns": start_ns, "window_end_unix_ns": end_ns, "rps": float(rps),
        "requests": {k: requests.get(k) for k in
                     ("expected", "attempted", "accepted", "applied", "replies_received",
                      "errors", "unexpected_business_results", "operation_counts", "outcome_counts")},
        "driver_counters": {**{k: delta.get(k) for k in
                              ("queries", "errors", "request_timeouts", "connection_timeouts", "retries")},
                            "valid": driver.get("valid")},
        "query_accounting": query,
        "database_counters": {
            "stage_metrics": stage_counters,
            "postgres_accounting": db_accounting if backend == "postgres" else None,
            "measured_cql_request_count": query.get("measured_cql_request_count") if backend == "scylla" else None,
            "measured_lwt_count": stage_counters.get("cql_write_lwt"),
            "backend_in_flight_peak": ((service.get("stage_metrics") or {}).get("database_in_flight_peak")
                                       if backend == "postgres" else
                                       (service.get("stage_metrics") or {}).get("cql_in_flight_peak")),
        },
        "latencies_ns": latency_ns, "latencies_ms": latency_ms,
        "verification": {k: verification.get(k) for k in
                         ("account_state_source", "accounts_expected", "accounts_verified",
                          "ledger_rows_expected", "ledger_rows_seen", "memory_cache_accounts_compared",
                          "complete", "valid", "failures")},
        "validation": validation,
        "service_events": {"phase_counts": phases, "event_count": sum(phases.values()),
                           "required_phases": required_phases, "phases_present": True},
        "source_inputs": refs,
    }
    copies = {f"cases/{case_id}/service/result.json": service_bytes,
              f"cases/{case_id}/service/events.jsonl": events_bytes}
    return row, copies


def parse_wal_io(stats: Mapping[str, Any]) -> dict[str, float]:
    rows = stats.get("io")
    if not isinstance(rows, list):
        raise EvidenceError("PostgreSQL stats.io is not a list")
    syncs, sync_ms = 0, 0.0
    for row in rows:
        if isinstance(row, dict) and row.get("object") == "wal":
            syncs += int(row.get("fsyncs") or 0)
            sync_ms += float(row.get("fsync_time") or 0)
    wal = stats.get("wal") or {}
    return {"bytes": float(wal.get("wal_bytes", 0)),
            "fsyncs": float(syncs), "fsync_time_ms": sync_ms}


def choose_samples(samples: Sequence[Mapping[str, Any]], timestamps: Sequence[int],
                   start: int, end: int, label: str) -> tuple[int, int, list[int]]:
    before = [i for i, ts in enumerate(timestamps) if ts <= start]
    after = [i for i, ts in enumerate(timestamps) if ts >= end]
    inside = [i for i, ts in enumerate(timestamps) if start <= ts <= end]
    if not before or not after or not inside:
        raise EvidenceError(f"{label} does not bracket and cover the request window")
    left, right = before[-1], after[0]
    if timestamps[right] <= timestamps[left]:
        raise EvidenceError(f"{label} has an empty sample bracket")
    if start - timestamps[left] > 2_500_000_000 or timestamps[right] - end > 2_500_000_000:
        raise EvidenceError(f"{label} boundary sample is more than 2.5s from request window")
    return left, right, inside


def row_base(case: Mapping[str, Any], kind: str, ref: Mapping[str, Any], index: Any,
             timestamp: int, start: int, end: int, left: int, right: int) -> dict[str, Any]:
    relation = "before" if timestamp < start else "after" if timestamp > end else "inside"
    boundary = "before" if timestamp == left else "after" if timestamp == right else ""
    return {
        "case_id": case["case_id"], "backend": case["backend"], "source_kind": kind,
        "source_path": ref["path"], "source_sha256": ref["sha256"], "sample_index": index,
        "sample_unix_ns": timestamp, "window_start_unix_ns": start, "window_end_unix_ns": end,
        "offset_from_window_start_ns": timestamp - start, "offset_from_window_end_ns": timestamp - end,
        "window_relation": relation, "boundary_role": boundary, "selection_roles": "",
    }


def fill_disk(row: dict[str, Any], disks: Mapping[str, Any]) -> None:
    row["whole_device_major_minor"] = disks["whole_device"]["major_minor"]
    row["data_device_major_minor"] = disks["data_device"]["major_minor"]
    for role in ("whole_device", "data_device"):
        for name in DISK_FIELDS:
            row[f"{role}_{name}"] = disks[role]["fields"][name]


def calc_disk_delta(left: Mapping[str, Any], right: Mapping[str, Any]) -> dict[str, Any]:
    result = {}
    for role in ("whole_device", "data_device"):
        a, b = left["disk"][role], right["disk"][role]
        if a["major_minor"] != b["major_minor"]:
            raise EvidenceError(f"host {role} identity changed across window")
        counters = {}
        for name in DISK_FIELDS:
            delta = b["fields"][name] - a["fields"][name]
            if delta < 0:
                raise EvidenceError(f"host diskstats {role}.{name} moved backwards")
            counters[name] = delta
        result[role] = {"major_minor": b["major_minor"], "name": b["name"], "counters": counters}
    return result


def host_metrics(rows: Sequence[Mapping[str, Any]], left_ns: int, right_ns: int) -> dict[str, Any]:
    by_time = {int(row["timestamp_unix_ns"]): row for row in rows}
    if right_ns <= left_ns or left_ns not in by_time or right_ns not in by_time:
        raise EvidenceError("host diskstats selected boundary samples are missing or invalid")
    delta = calc_disk_delta(by_time[left_ns], by_time[right_ns])
    elapsed = (right_ns - left_ns) / 1e9
    out = {}
    for role in ("whole_device", "data_device"):
        c = delta[role]["counters"]
        out[role] = {
            "major_minor": delta[role]["major_minor"], "name": delta[role]["name"],
            "counter_deltas": c, "write_bytes": c["sectors_written"] * 512,
            "write_mib_per_second": c["sectors_written"] * 512 / MIB / elapsed,
            "write_iops": c["writes_completed"] / elapsed,
            "device_busy_percent": c["io_ms"] / (elapsed * 1000) * 100,
            "flush_requests": c["flush_requests"],
        }
    return {
        "source_window": {"start_sample_unix_ns": left_ns, "end_sample_unix_ns": right_ns,
                          "sample_interval_seconds": elapsed},
        "whole_device": out["whole_device"], "data_device_partition": out["data_device"],
        "additive_rule": "whole device 8:16 and partition 8:18 are alternate views; never add counters",
        "formula": "counter deltas / sample interval; sectors use 512 bytes; MiB uses 1024^2",
    }


def cpu_metrics(scope: str, ref: Mapping[str, Any], left: int, right: int,
                delta_usec: int, start: int, end: int) -> dict[str, Any]:
    bracket = right - left
    if bracket <= 0 or delta_usec < 0:
        raise EvidenceError("CPU sample bracket or counter delta is invalid")
    return {
        "scope": scope, "source": ref["path"], "source_sha256": ref["sha256"],
        "start_sample_unix_ns": left, "end_sample_unix_ns": right,
        "start_sample_offset_from_window_ns": left - start,
        "end_sample_offset_from_window_ns": right - end,
        "counter_delta_usec": delta_usec, "bracket_duration_ns": bracket,
        "bracket_duration_seconds": bracket / 1e9, "cpu_count_denominator": CPU_DENOMINATOR,
        "formula": "usage_usec delta / bracket elapsed usec / 2 database CPUs * 100",
        "cpu_percent": (delta_usec * 1000 / bracket) / CPU_DENOMINATOR * 100,
    }


def sum_scylla_counter(metric: Any, case_id: str) -> float:
    if not isinstance(metric, dict) or not isinstance(metric.get("series"), dict):
        raise EvidenceError(f"{case_id} is missing Scylla counter series")
    total = 0.0
    for shard, value in metric["series"].items():
        if value.get("status") != "observed" or value.get("delta") is None:
            raise EvidenceError(f"{case_id} Scylla counter shard {shard} is unobserved")
        total += float(value["delta"])
    return total


def derive_resources(source: Source, case: Mapping[str, Any], artifact: Mapping[str, Any]
                     ) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    case_id, backend, base = case["case_id"], case["backend"], case["source_base"]
    rows: list[dict[str, Any]] = []
    if backend == "postgres":
        monitor_path, disk_path = f"{base}/postgres-monitor/samples.jsonl", f"{base}/disk-observer/samples.jsonl"
        pg_samples, pg_bytes = read_jsonl(source, monitor_path)
        disk_samples, disk_bytes = read_jsonl(source, disk_path)
        pg_ref, disk_ref = source.ref(monitor_path, pg_bytes), source.ref(disk_path, disk_bytes)
        window = (artifact.get("postgres_monitor") or {}).get("measured_window") or {}
        start = require_int(window.get("window_start_unix_ns"), f"{case_id} PG window start")
        end = require_int(window.get("window_end_unix_ns"), f"{case_id} PG window end")
        if end <= start:
            raise EvidenceError(f"{case_id} PostgreSQL window is empty")
        ts = [require_int(s.get("sampled_unix_ns"), f"{case_id} PG sample time") for s in pg_samples]
        left, right, inside = choose_samples(pg_samples, ts, start, end, f"{case_id} PostgreSQL monitor")
        if ts[left] != window.get("before_sample_unix_ns") or ts[right] != window.get("after_sample_unix_ns"):
            raise EvidenceError(f"{case_id} raw PostgreSQL boundary samples differ from runner summary")
        memory_rows = [pg_samples[i] for i in inside]
        mem_peak_index = max(inside, key=lambda i: pg_samples[i]["cgroup"]["memory"]["memory.current"])
        pss_peak_index = max(inside, key=lambda i: pg_samples[i]["postgres_pss"]["pss_kb"])
        # Keep every in-window sample in Git so memory and PSS peaks can be
        # independently recalculated from the compact CSV. Include the nearest
        # CPU/I/O/WAL boundary samples even when they fall outside the window.
        for index in sorted(set([left, right, *inside])):
            sample, stamp = pg_samples[index], ts[index]
            cg, mem = sample.get("cgroup") or {}, (sample.get("cgroup") or {}).get("memory") or {}
            cpu, io = cg.get("cpu_stat") or {}, parse_io_stat(cg.get("io_stat"), f"{case_id} PG io.stat")
            dev = io.get("8:16")
            if dev is None:
                raise EvidenceError(f"{case_id} PG sample lacks cgroup device 8:16")
            wal = parse_wal_io(sample.get("stats") or {})
            row = row_base(case, "pg_monitor", pg_ref, sample.get("sample_index"), stamp,
                           start, end, ts[left], ts[right])
            roles = []
            if index == left:
                roles.append("left_cpu_io_wal_boundary")
            if index == right:
                roles.append("right_cpu_io_wal_boundary")
            if index in inside:
                roles.append("in_window_sample")
            if index == mem_peak_index:
                roles.append("in_window_peak_memory")
            if index == pss_peak_index:
                roles.append("in_window_peak_pss")
            row["selection_roles"] = ";".join(roles)
            row.update({
                "cgroup_cpu_usage_usec": require_int(cpu.get("usage_usec"), "PG cpu usage"),
                "cgroup_cpu_user_usec": require_int(cpu.get("user_usec"), "PG cpu user"),
                "cgroup_cpu_system_usec": require_int(cpu.get("system_usec"), "PG cpu system"),
                "memory_current_bytes": require_int(mem.get("memory.current"), "PG memory current"),
                "memory_peak_bytes": require_int(mem.get("memory.peak"), "PG memory peak"),
                "memory_swap_current_bytes": require_int(mem.get("memory.swap.current"), "PG swap current"),
                "memory_oom": require_int((mem.get("memory.events") or {}).get("oom"), "PG OOM"),
                "memory_oom_kill": require_int((mem.get("memory.events") or {}).get("oom_kill"), "PG OOM kill"),
                "postgres_pss_kb": require_int((sample.get("postgres_pss") or {}).get("pss_kb"), "PG PSS"),
                "cgroup_io_device": "8:16", "cgroup_io_rbytes": dev.get("rbytes"),
                "cgroup_io_wbytes": dev.get("wbytes"), "cgroup_io_rios": dev.get("rios"),
                "cgroup_io_wios": dev.get("wios"),
                "postgres_wal_bytes": int(wal["bytes"]), "postgres_wal_fsyncs": int(wal["fsyncs"]),
                "postgres_wal_fsync_time_ms": wal["fsync_time_ms"],
            })
            rows.append(row)
        disk_ts = [require_int(s.get("time_unix_ns"), "host diskstats time") for s in disk_samples]
        dleft, dright, _ = choose_samples(disk_samples, disk_ts, start, end, f"{case_id} host diskstats")
        host_rows = []
        for index in sorted({dleft, dright}):
            sample, stamp = disk_samples[index], disk_ts[index]
            parsed = parse_host_disk(sample, "host_diskstats")
            row = row_base(case, "host_diskstats", disk_ref, sample.get("sample_index"), stamp,
                           start, end, disk_ts[dleft], disk_ts[dright])
            row["selection_roles"] = row["boundary_role"]
            fill_disk(row, parsed)
            rows.append(row)
            host_rows.append({"timestamp_unix_ns": stamp, "disk": parsed})
        left_sample, right_sample = pg_samples[left], pg_samples[right]
        left_cpu = (left_sample.get("cgroup") or {}).get("cpu_stat") or {}
        right_cpu = (right_sample.get("cgroup") or {}).get("cpu_stat") or {}
        cpu_delta = require_int(right_cpu.get("usage_usec"), "PG right usage") - require_int(
            left_cpu.get("usage_usec"), "PG left usage")
        max_mem, max_pss = pg_samples[mem_peak_index], pg_samples[pss_peak_index]
        monitor_window = window.get("cgroup") or {}
        io_windows = monitor_window.get("io_stat_deltas") or {}
        cgroup_delta = ((io_windows.get("8:16") or {}).get("counters"))
        if (not isinstance(cgroup_delta, dict)
                or (io_windows.get("8:16") or {}).get("status") != "valid"):
            raise EvidenceError(f"{case_id} PG cgroup I/O summary is invalid")
        left_wal = parse_wal_io(left_sample.get("stats") or {})
        right_wal = parse_wal_io(right_sample.get("stats") or {})
        wal_bytes = int(right_wal["bytes"] - left_wal["bytes"])
        wal_fsyncs = int(right_wal["fsyncs"] - left_wal["fsyncs"])
        wal_fsync_ms = right_wal["fsync_time_ms"] - left_wal["fsync_time_ms"]
        db_deltas = ((window.get("postgresql_statistics") or {}).get("deltas") or {})
        expected_wal = db_deltas.get("wal") or {}
        if expected_wal.get("status") != "valid" or expected_wal.get("counters", {}).get("wal_bytes") != wal_bytes:
            raise EvidenceError(f"{case_id} raw WAL bytes differ from case summary")
        expected_io = db_deltas.get("io") or {}
        expected_fsyncs = sum(int((v.get("counters") or {}).get("fsyncs") or 0)
                              for k, v in expected_io.items() if "|wal|" in k)
        expected_fsync_ms = sum(float((v.get("counters") or {}).get("fsync_time") or 0)
                                for k, v in expected_io.items() if "|wal|" in k)
        if wal_fsyncs != expected_fsyncs or not math.isclose(wal_fsync_ms, expected_fsync_ms,
                                                               rel_tol=1e-9, abs_tol=1e-6):
            raise EvidenceError(f"{case_id} raw WAL fsync counters differ from case summary")
        post = artifact.get("post_window_checkpoint_and_drain") or {}
        post_io = post.get("cgroup_io_validation") or {}
        checkpoint = post.get("checkpoint_command") or {}
        if post_io.get("valid") is not True or checkpoint.get("returncode") != 0:
            raise EvidenceError(f"{case_id} post-window checkpoint validation failed")
        resource = {
            "case_id": case_id, "case_kind": case["case_kind"], "backend": backend,
            "measurement_window": {"start_unix_ns": start, "end_unix_ns": end,
                                   "duration_seconds": (end-start)/1e9},
            "database_cpu": cpu_metrics("PostgreSQL cgroup v2 cpu.stat", pg_ref, ts[left], ts[right],
                                        cpu_delta, start, end),
            "database_memory": {
                "source": pg_ref["path"], "source_sha256": pg_ref["sha256"],
                "sample_count_in_measurement_window": len(memory_rows),
                "maximum_memory_current_bytes": int(max_mem["cgroup"]["memory"]["memory.current"]),
                "maximum_memory_current_mib": int(max_mem["cgroup"]["memory"]["memory.current"]) / MIB,
                "maximum_memory_sample_unix_ns": max_mem["sampled_unix_ns"],
                "maximum_memory_peak_counter_bytes": max(int(s["cgroup"]["memory"]["memory.peak"])
                                                         for s in memory_rows),
                "maximum_swap_current_bytes": max(int(s["cgroup"]["memory"]["memory.swap.current"])
                                                  for s in memory_rows),
                "oom_events_at_after_boundary": (right_sample["cgroup"]["memory"]["memory.events"] or {}).get("oom"),
                "oom_kill_events_at_after_boundary": (right_sample["cgroup"]["memory"]["memory.events"] or {}).get("oom_kill"),
            },
            "postgres_pss": {
                "source": pg_ref["path"], "source_sha256": pg_ref["sha256"],
                "sample_count_in_measurement_window": len(memory_rows),
                "maximum_pss_kb": max_pss["postgres_pss"]["pss_kb"],
                "maximum_pss_mib": max_pss["postgres_pss"]["pss_kb"] / 1024,
                "maximum_sample_unix_ns": max_pss["sampled_unix_ns"],
                "rss_sum_not_used": True,
            },
            "cgroup_io_window": {"device": "8:16", "counters": cgroup_delta,
                                 "write_bytes": cgroup_delta.get("wbytes"),
                                 "source": pg_ref["path"], "source_sha256": pg_ref["sha256"]},
            "host_diskstats": host_metrics(host_rows, disk_ts[dleft], disk_ts[dright]),
            "durability_counters": {
                "kind": "PostgreSQL WAL statistics", "source": pg_ref["path"],
                "source_sha256": pg_ref["sha256"], "wal_bytes": wal_bytes,
                "wal_fsyncs": wal_fsyncs, "wal_fsync_time_ms": wal_fsync_ms,
                "note": "Asynchronously published PostgreSQL WAL/io counters; not equivalent to Scylla commitlog counters.",
            },
            "post_window_checkpoint": {
                "scope": post.get("scope"), "start_unix_ns": post.get("checkpoint_started_unix_ns"),
                "finish_unix_ns": post.get("checkpoint_finished_unix_ns"),
                "elapsed_seconds": (post.get("checkpoint_finished_unix_ns", 0)
                                    - post.get("checkpoint_started_unix_ns", 0)) / 1e9,
                "returncode": checkpoint.get("returncode"),
                "cgroup_write_bytes": (post_io.get("counters") or {}).get("wbytes"),
                "cgroup_write_ios": (post_io.get("counters") or {}).get("wios"),
                "source": source.ref(f"{base}/case-result.json"),
            },
            "source_inputs": [pg_ref, disk_ref, source.ref(f"{base}/case-result.json")],
        }
    elif backend == "scylla":
        proc_path = f"{base}/proc/samples.jsonl"
        host_path = f"runs/{RUN_ID}/scylla-host-observers/{case_id}/samples.jsonl"
        proc_samples, proc_bytes = read_jsonl(source, proc_path)
        host_samples, host_bytes = read_jsonl(source, host_path)
        proc_ref, host_ref = source.ref(proc_path, proc_bytes), source.ref(host_path, host_bytes)
        window = (artifact.get("monitor") or {}).get("measurement_window") or {}
        start, end = require_int(window.get("start_unix_ns"), "Scylla window start"), require_int(
            window.get("end_unix_ns"), "Scylla window end")
        if end <= start:
            raise EvidenceError(f"{case_id} Scylla window is empty")
        proc_ts = [require_int(s.get("timestamp_unix_ns"), "Scylla sample timestamp") for s in proc_samples]
        left, right, inside = choose_samples(proc_samples, proc_ts, start, end, f"{case_id} Scylla proc monitor")
        mem_peak_index = max(inside, key=lambda i: int(
            proc_samples[i]["scylla_processes"]["nodea"]["cgroup"]["memory.current"]))
        # Preserve the full in-window series so the reported memory peak is
        # reproducible from the compact evidence bundle.
        for index in sorted(set([left, right, *inside])):
            sample, stamp = proc_samples[index], proc_ts[index]
            node = (sample.get("scylla_processes") or {}).get("nodea")
            if not isinstance(node, dict):
                raise EvidenceError(f"{case_id} Scylla sample lacks nodea")
            cg = node.get("cgroup") or {}
            cpu = parse_key_values(cg.get("cpu.stat"), f"{case_id} Scylla cpu.stat")
            io = parse_io_stat(cg.get("io.stat"), f"{case_id} Scylla io.stat").get("8:16")
            if io is None:
                raise EvidenceError(f"{case_id} Scylla sample lacks cgroup device 8:16")
            row = row_base(case, "scylla_proc", proc_ref, index, stamp, start, end,
                           proc_ts[left], proc_ts[right])
            roles = []
            if index == left:
                roles.append("left_cpu_io_boundary")
            if index == right:
                roles.append("right_cpu_io_boundary")
            if index in inside:
                roles.append("in_window_sample")
            if index == mem_peak_index:
                roles.append("in_window_peak_memory")
            row["selection_roles"] = ";".join(roles)
            row.update({
                "cgroup_cpu_usage_usec": cpu.get("usage_usec"),
                "cgroup_cpu_user_usec": cpu.get("user_usec"), "cgroup_cpu_system_usec": cpu.get("system_usec"),
                "memory_current_bytes": int(cg["memory.current"]), "memory_peak_bytes": int(cg["memory.peak"]),
                "cgroup_io_device": "8:16", "cgroup_io_rbytes": io.get("rbytes"),
                "cgroup_io_wbytes": io.get("wbytes"), "cgroup_io_rios": io.get("rios"),
                "cgroup_io_wios": io.get("wios"),
            })
            rows.append(row)
        disk_ts = [require_int(s.get("time_unix_ns"), "Scylla host diskstats timestamp") for s in host_samples]
        dleft, dright, _ = choose_samples(host_samples, disk_ts, start, end, f"{case_id} host diskstats")
        host_rows = []
        for index in sorted({dleft, dright}):
            sample, stamp = host_samples[index], disk_ts[index]
            disks = parse_host_disk(sample, "host_diskstats")
            row = row_base(case, "host_diskstats", host_ref, sample.get("sample_index"), stamp,
                           start, end, disk_ts[dleft], disk_ts[dright])
            row["selection_roles"] = row["boundary_role"]
            fill_disk(row, disks)
            rows.append(row)
            host_rows.append({"timestamp_unix_ns": stamp, "disk": disks})
        left_node = (proc_samples[left].get("scylla_processes") or {}).get("nodea") or {}
        right_node = (proc_samples[right].get("scylla_processes") or {}).get("nodea") or {}
        left_cg, right_cg = left_node.get("cgroup") or {}, right_node.get("cgroup") or {}
        left_cpu = parse_key_values(left_cg.get("cpu.stat"), "Scylla left cpu.stat")
        right_cpu = parse_key_values(right_cg.get("cpu.stat"), "Scylla right cpu.stat")
        cpu_delta = right_cpu["usage_usec"] - left_cpu["usage_usec"]
        memory_rows = [proc_samples[i] for i in inside]
        max_mem = proc_samples[mem_peak_index]
        left_io = parse_io_stat(left_cg.get("io.stat"), "Scylla left io.stat")["8:16"]
        right_io = parse_io_stat(right_cg.get("io.stat"), "Scylla right io.stat")["8:16"]
        io_delta = {}
        for key in set(left_io) & set(right_io):
            value = right_io[key] - left_io[key]
            if value < 0:
                raise EvidenceError(f"{case_id} Scylla cgroup I/O counter {key} moved backwards")
            io_delta[key] = value
        counter_node = (((artifact.get("monitor") or {}).get("counter_window_deltas") or {}).get("nodes") or {}).get("nodea") or {}
        commitlog_bytes = sum_scylla_counter(counter_node.get("scylla_commitlog_bytes_written"), case_id)
        commitlog_flushes = sum_scylla_counter(counter_node.get("scylla_commitlog_flush"), case_id)
        runner_ref = source.ref(f"{base}/runner-result.json")
        resource = {
            "case_id": case_id, "case_kind": case["case_kind"], "backend": backend,
            "measurement_window": {"start_unix_ns": start, "end_unix_ns": end,
                                   "duration_seconds": (end-start)/1e9},
            "database_cpu": cpu_metrics("Scylla cgroup v2 cpu.stat", proc_ref, proc_ts[left], proc_ts[right],
                                        cpu_delta, start, end),
            "database_memory": {
                "source": proc_ref["path"], "source_sha256": proc_ref["sha256"],
                "sample_count_in_measurement_window": len(memory_rows),
                "maximum_memory_current_bytes": int(max_mem["scylla_processes"]["nodea"]["cgroup"]["memory.current"]),
                "maximum_memory_current_mib": int(max_mem["scylla_processes"]["nodea"]["cgroup"]["memory.current"]) / MIB,
                "maximum_memory_sample_unix_ns": max_mem["timestamp_unix_ns"],
                "maximum_memory_peak_counter_bytes": max(int(s["scylla_processes"]["nodea"]["cgroup"]["memory.peak"])
                                                         for s in memory_rows),
                "maximum_swap_current_bytes": None, "oom_events_at_after_boundary": None,
                "oom_kill_events_at_after_boundary": None,
            },
            "postgres_pss": None,
            "cgroup_io_window": {"device": "8:16", "counters": io_delta,
                                 "write_bytes": io_delta.get("wbytes"),
                                 "source": proc_ref["path"], "source_sha256": proc_ref["sha256"]},
            "host_diskstats": host_metrics(host_rows, disk_ts[dleft], disk_ts[dright]),
            "durability_counters": {
                "kind": "Scylla commitlog metric deltas", "source": runner_ref["path"],
                "source_sha256": runner_ref["sha256"], "commitlog_bytes_written": commitlog_bytes,
                "commitlog_flush_count": commitlog_flushes,
                "note": "Scylla commitlog counters are not request counts and are not equivalent to PostgreSQL WAL/fsync counters.",
            },
            "post_window_checkpoint": None, "source_inputs": [proc_ref, host_ref, runner_ref],
        }
    else:
        raise EvidenceError(f"unknown backend in resource summary: {backend}")
    return resource, rows, resource["source_inputs"]


def render_csv(rows: Sequence[Mapping[str, Any]], fields: Sequence[str]) -> bytes:
    out = io.StringIO(newline="")
    writer = csv.DictWriter(out, fieldnames=fields, extrasaction="raise", lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return out.getvalue().encode("utf-8")


def build_summaries(source: Source) -> tuple[dict[str, Any], bytes, bytes, dict[str, Any], dict[str, bytes]]:
    run = f"runs/{RUN_ID}"
    campaign, campaign_bytes = read_json(source, f"{run}/campaign.json")
    cases = campaign.get("cases")
    if not isinstance(cases, list) or len(cases) != 9:
        raise EvidenceError("formal run campaign must have exactly nine cases")
    run_manifest, run_bytes = read_json(source, f"{run}/run-manifest.json")
    plan, plan_bytes = read_json(source, f"{run}/approved-plan.json")
    plan_hash = plan.get("contract_sha256")
    if not isinstance(plan_hash, str) or len(plan_hash) != 64:
        raise EvidenceError("approved plan is missing a valid contract_sha256")
    if (campaign.get("approved_contract_sha256") != plan_hash
            or run_manifest.get("approved_contract_sha256") != plan_hash):
        raise EvidenceError("campaign, run manifest, and approved plan contract hashes differ")
    if run_manifest.get("approved_plan_sha256") != digest(plan_bytes):
        raise EvidenceError("run manifest approved_plan_sha256 differs from approved-plan.json")
    summary_cases, case_csv_rows, resources, sample_rows = [], [], [], []
    case_provenance: dict[str, dict[str, Any]] = {}
    resource_provenance: dict[str, dict[str, Any]] = {}
    copied: dict[str, bytes] = {}
    for item in sorted(cases, key=lambda x: int(x.get("order", 0))):
        case, copies = make_case(source, item)
        copied.update(copies)
        summary_cases.append(case)
        case_csv_rows.append(case_csv_row(case))
        for ref in case["source_inputs"]:
            case_provenance[ref["path"]] = ref
        artifact = json.loads(source.read(f"{case['source_base']}/case-result.json") if case["backend"] == "postgres"
                              else source.read(f"{case['source_base']}/runner-result.json"))
        resource, current_rows, inputs = derive_resources(source, case, artifact)
        resources.append(resource)
        sample_rows.extend(current_rows)
        for ref in inputs:
            resource_provenance[ref["path"]] = ref
    copied["run-manifest.json"], copied["approved-plan.json"] = run_bytes, plan_bytes
    campaign_ref = source.ref(f"{run}/campaign.json", campaign_bytes)
    run_ref = source.ref(f"{run}/run-manifest.json", run_bytes)
    plan_ref = source.ref(f"{run}/approved-plan.json", plan_bytes)
    for ref in (campaign_ref, run_ref, plan_ref):
        case_provenance[ref["path"]] = ref
        resource_provenance[ref["path"]] = ref
    summary = {
        "schema_version": 1, "run_id": RUN_ID, "campaign_status": campaign.get("status"),
        "completed_case_count": campaign.get("completed_case_count"),
        "planned_case_count": campaign.get("planned_case_count"),
        "automatic_retries": campaign.get("automatic_retries"),
        "run_manifest": {"source": run_ref, "status": run_manifest.get("status"),
                         "binary_sha256": run_manifest.get("binary_sha256"),
                         "approved_contract_sha256": run_manifest.get("approved_contract_sha256"),
                         "approved_plan_sha256": run_manifest.get("approved_plan_sha256")},
        "approved_plan": {"source": plan_ref, "contract_sha256": plan_hash},
        "cases": summary_cases, "source_inputs": [case_provenance[k] for k in sorted(case_provenance)],
        "derivation": "normalized from original campaign, runner/case result, service result and event files; full sources remain in raw archive",
    }
    case_csv = render_csv(case_csv_rows, CASE_FIELDS)
    resource_csv = render_csv(sample_rows, RESOURCE_FIELDS)
    resource_summary = {
        "schema_version": 1, "run_id": RUN_ID, "case_resources": resources,
        "formulas": {
            "window": "request window uses exact client start/end Unix-ns timestamps; samples carry their offset and inside/before/after relation",
            "database_cpu": "cgroup usage_usec delta between nearest sample at/before window start and nearest sample at/after window end, divided by bracket duration and two assigned database CPUs, multiplied by 100",
            "database_memory": "maximum cgroup memory.current at samples inside measured window; MiB = bytes / 1024^2",
            "postgres_pss": "maximum summed PostgreSQL process PSS in-window; MiB = KB / 1024; backend RSS is not summed",
            "postgres_swap_oom": "maximum sampled memory.swap.current and ending memory.events counters",
            "cgroup_io": "8:16 io.stat boundary counter delta; wbytes is container write bytes",
            "host_diskstats": "nearest host samples bracketing request window; rates use host-sample interval and diskstats sectors * 512; MiB = bytes / 1024^2",
            "whole_vs_partition": "whole device 8:16 and data partition 8:18 are alternate views; never add their counters",
            "wal_vs_commitlog": "PostgreSQL WAL/fsync and Scylla commitlog counters have different semantics and are not compared one-to-one or treated as request counts",
            "post_window_checkpoint": "PostgreSQL checkpoint and cgroup write bytes are after the measured request window, outside RPS and in-window resource metrics",
        },
        "source_inputs": [resource_provenance[k] for k in sorted(resource_provenance)],
        "derivation": "rebuilt from original PostgreSQL monitor samples, Scylla proc samples, host disk observer samples and case/runner result files",
    }
    copied.update({
        "campaign-summary.json": canonical_json(summary), "cases.csv": case_csv,
        "resource-samples.csv": resource_csv, "resource-summary.json": canonical_json(resource_summary),
    })
    return summary, case_csv, resource_csv, resource_summary, copied


def case_csv_row(case: Mapping[str, Any]) -> dict[str, Any]:
    req, driver = case["requests"], case["driver_counters"]
    query, db = case["query_accounting"], case["database_counters"]
    pg, stage_counters = db.get("postgres_accounting") or {}, db.get("stage_metrics") or {}
    ver, valid, refs = case["verification"], case["validation"], case["source_inputs"]
    service = next(x for x in refs if x["path"].endswith("/service/result.json"))
    events = next(x for x in refs if x["path"].endswith("/service/events.jsonl"))
    row = {
        "order": case["order"], "case_id": case["case_id"], "case_kind": case["case_kind"],
        "repeat": case["repeat"], "backend": case["backend"], "connections": case["connections"],
        "window_seconds": case["window_seconds"], "rps": case["rps"],
        "requests_expected": req["expected"], "requests_attempted": req["attempted"],
        "requests_accepted": req["accepted"], "requests_applied": req["applied"],
        "replies_received": req["replies_received"], "workload_errors": req["errors"],
        "unexpected_business_results": req["unexpected_business_results"],
        "driver_queries": driver["queries"], "driver_errors": driver["errors"],
        "driver_request_timeouts": driver["request_timeouts"],
        "driver_connection_timeouts": driver["connection_timeouts"], "driver_retries": driver["retries"],
        "driver_counters_valid": driver["valid"],
        "measured_select_requests": query.get("measured_select_requests"),
        "driver_query_delta": query.get("driver_query_delta"),
        "internal_write_operations": query.get("internal_write_operations"),
        "measured_sql_transaction_count": query.get("measured_sql_transaction_count"),
        "measured_cql_request_count": query.get("measured_cql_request_count"),
        "pg_function_calls": pg.get("function_calls"), "pg_ledger_insert_attempts": pg.get("ledger_insert_attempts"),
        "pg_account_updates": pg.get("account_updates"), "pg_duplicate_conflicts": pg.get("duplicate_conflicts"),
        "cql_write_lwt_count": stage_counters.get("cql_write_lwt"),
        "backend_in_flight_peak": db.get("backend_in_flight_peak"),
        "accounts_expected": ver["accounts_expected"], "accounts_verified": ver["accounts_verified"],
        "ledger_rows_expected": ver["ledger_rows_expected"], "ledger_rows_seen": ver["ledger_rows_seen"],
        "verification_valid": ver["valid"], "case_status": valid["case_status"],
        "service_status": valid["service_status"], "service_valid": valid["service_valid"],
        "ready_valid": valid["ready_valid"], "events_valid": valid["events_valid"],
        "result_valid": valid["result_valid"], "metrics_valid": valid["metrics_valid"],
        "monitor_window_valid": valid["monitor_window_valid"], "cleanup_status": valid["cleanup_status"],
        "cleanup_complete": valid["cleanup_complete"],
        "cleanup_errors": json.dumps(valid.get("cleanup_errors") or [], separators=(",", ":")),
        "artifact_source_path": refs[0]["path"], "artifact_source_sha256": refs[0]["sha256"],
        "service_result_source_path": service["path"], "service_result_source_sha256": service["sha256"],
        "service_events_source_path": events["path"], "service_events_source_sha256": events["sha256"],
    }
    for stage in ("end_to_end", *STAGES):
        for metric in ("count", *PERCENTILES):
            row[f"latency_{stage}_{metric}_ns"] = (case["latencies_ns"].get(stage) or {}).get(metric)
    return row


def read_campaign_sources(source: Source) -> list[dict[str, Any]]:
    campaign, _ = read_json(source, f"runs/{RUN_ID}/campaign.json")
    return campaign["cases"]


def warnings_doc(source: Source) -> tuple[bytes, list[dict[str, Any]]]:
    paths = [
        "runs/run-20261013T121329Z-2834e7a9e5/campaign.json",
        "runs/run-20261013T122913Z-072162c586/campaign.json",
        f"runs/{RUN_ID}/campaign.json",
        "runs/run-20261013T121329Z-2834e7a9e5/postgres-cases/pg-calibration-D64/case-result.json",
        "runs/run-20261013T122913Z-072162c586/postgres-cases/pg-calibration-D64/case-result.json",
        f"runs/{RUN_ID}/trials/trial-004-S2-C512-P3-D6-round-robin-RF1-R1-W1-N1-wallet-memory-lwt-v1-rep1/logs/nodea.container.log",
        f"runs/{RUN_ID}/trials/trial-007-S2-C512-P3-D6-round-robin-RF1-R1-W1-N1-wallet-memory-lwt-v1-rep2/logs/nodea.container.log",
        f"runs/{RUN_ID}/trials/trial-008-S2-C512-P3-D6-round-robin-RF1-R1-W1-N1-wallet-memory-lwt-v1-rep3/logs/nodea.container.log",
    ]
    refs = [source.ref(path) for path in paths]
    lines = [
        "# 原始資料警示與執行事件", "",
        "本文整理正式 campaign 前的停止原因與 Scylla 啟動日誌訊息。整理資料來自封存原始檔；不改寫 service workload counters，也不量化環境訊息的效能影響。", "",
        "## 正式 run 前的兩次停止", "",
        "| Run | 狀況 | 處置與有效性 |", "|---|---|---|",
        "| run-20261013T121329Z-2834e7a9e5 | 完成 2/9 後停止。D64 workload 尚未開始，readiness gate 的 pg_isready 沒有指定 TCP host，誤把暫時的 Unix-socket init server 當成 TCP server。 | 不列入正式結果。後續強制 TCP readiness，並檢查 container logs 與 observer/collector readiness。 |",
        "| run-20261013T122913Z-072162c586 | 完成 2/9 個有效案例後停止。D64 執行 1,024,000 requests、workload errors=0 並通過完整 verification，但 monitor interval 記錄 2.673s、2.642s，超過 2.5s 門檻。 | 不列入正式結果。後續依 capture 開始時間排程 +1s；若 capture 超時則結束後立即開始；保留 2.5s 門檻且不做 catch-up burst。 |",
        "",
        "後續另執行經 root 批准的全新九案例 campaign。正式 campaign 與 run manifest 報告 9/9 完成及 Supervisor exit code 0。九個 service result 都報告 1,024,000 replies、workload errors=0，並通過全量驗證與 owned cleanup。前兩個 run 的案例未併入正式結果。", "",
        "## Scylla container 啟動訊息", "",
        "三次正式 Scylla 測試都記錄以下環境訊息：mbind shard memory 操作因 Operation not permitted 失敗；perf_event stall detector 無法建立並降級為 POSIX timer；/run/udev/data 無法開啟，diskstats collector 停用 udev device properties；RF=1 低於建議閾值 3。RF=1 是本次比較條件。", "",
        "Service 結果仍記錄 1,024,000/1,024,000 applied，workload errors=0，driver errors、retries、request timeouts、connection timeouts 均為 0。啟動日誌 warnings/errors 與 workload counters 是不同觀測面；本次沒有量化上述環境訊息對吞吐量或延遲的影響。", "",
        "## 原始來源與 SHA-256", "", "| 原始封存內路徑 | Bytes | SHA-256 |", "|---|---:|---|",
    ]
    for ref in sorted(refs, key=lambda x: x["path"]):
        lines.append(f"| {ref['path']} | {ref['bytes']} | {ref['sha256']} |")
    return ("\n".join(lines) + "\n").encode("utf-8"), refs


def readme(info: Mapping[str, Any], manifest_ref: Mapping[str, Any], bundle_name: str) -> bytes:
    bundle = f"benches/data/wallet_db_compare/{bundle_name}"
    text = f"""# Wallet DB 比較證據索引

此 Git 證據包收錄九個案例的原始 service result/events、run manifest、approved plan，以及從封存原始資料產生的案例與資源摘要。`campaign-summary.json`、`cases.csv`、`resource-samples.csv`、`resource-summary.json` 和 `warnings/incidents.md` 是摘要或衍生資料，不是完整 runner/case JSON。完整來源、qualification、先前停止的 runs、plans、launch records、logs 和其他原始檔保存在下列本機封存。

<a id="raw-archive"></a>
## 原始封存

- 本機路徑：`{info['path']}`
- 壓縮檔大小：{info['size_bytes']} bytes；SHA-256：`{info['sha256']}`
- 內容：{info['file_count']} 個一般檔案，解壓後 {info['uncompressed_bytes']} bytes，根目錄為 `wallet-db-compare/`
- Git 內原始檔清單：`original-file-manifest.csv`（{manifest_ref['bytes']} bytes；SHA-256：`{manifest_ref['sha256']}`）
- 可用位置：僅此主機，沒有共享 URL。

逐檔核對封存內容與 Git 內的路徑、大小及 SHA-256 清單：

~~~sh
python3 benches/archive_wallet_db_evidence.py verify --bundle {bundle} --archive {info['path']}
~~~

解壓後重新產生摘要，並逐 byte 比對 Git 內輸出：

~~~sh
mkdir -p /tmp/ledger-wallet-wallet-db-compare-restore && tar -xzf {info['path']} -C /tmp/ledger-wallet-wallet-db-compare-restore
python3 benches/archive_wallet_db_evidence.py verify --bundle {bundle} --source-root /tmp/ledger-wallet-wallet-db-compare-restore/wallet-db-compare
~~~

`build` 不會覆寫已存在的輸出目錄。`verify --source-root` 會核對解壓來源、原始檔清單、精簡包內的 byte-preserved 檔案，以及衍生摘要是否可重建。

<a id="original-evidence-lookup"></a>
## 原始證據索引

封存保留 `target/wallet-db-compare/` 的原始相對路徑，並置於 `wallet-db-compare/` 根目錄下。常用來源：

- Qualification scripts/results：`wallet-db-compare/qualification/` 與 `wallet-db-compare/qualification/scylla-profile-qualification-20261013T115554Z/`。
- 先前停止的 runs：`wallet-db-compare/runs/run-20261013T121329Z-2834e7a9e5/` 與 `wallet-db-compare/runs/run-20261013T122913Z-072162c586/`。
- 核准的原始 plan：`wallet-db-compare/plans/plan-20261013T123805Z-eff4ebb234/plan.json`。
- 正式原始案例、runner results 和 monitors：`wallet-db-compare/runs/{RUN_ID}/`。
- Supervisor launch records：`wallet-db-compare/campaign-launches/`。

封存中的 JSON/JSONL 原檔逐 byte 保留；其中歷史 absolute paths 不會改寫。Git 內的 `run-manifest.json`、`approved-plan.json` 和九個案例的 service result/events 也與封存來源逐 byte 相同。
"""
    return text.replace("{RUN_ID}", RUN_ID).encode("utf-8")


def bundle_files(root: Path) -> list[dict[str, Any]]:
    result = []
    for current, dirs, files in os.walk(root, topdown=True, followlinks=False):
        base = Path(current)
        for name in dirs + files:
            if (base / name).is_symlink():
                raise EvidenceError(f"bundle contains symlink: {base / name}")
        for name in files:
            p = base / name
            result.append({"path": p.relative_to(root).as_posix(), "bytes": p.stat().st_size,
                           "sha256": file_digest(p)})
    return sorted(result, key=lambda x: x["path"])


def write_new(root: Path, name: str, data: bytes) -> None:
    name = relpath(name)
    path = root.joinpath(*PurePosixPath(name).parts)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    with os.fdopen(fd, "wb") as stream:
        stream.write(data)


def compact_copy_check(bundle: Path, source: Source) -> None:
    campaign = read_campaign_sources(source)
    for rel in (f"runs/{RUN_ID}/run-manifest.json", f"runs/{RUN_ID}/approved-plan.json"):
        if source.read(rel) != (bundle / Path(rel).name).read_bytes():
            raise EvidenceError(f"compact copy differs from original: {rel}")
    for item in campaign:
        if item["backend"] == "postgres":
            base = f"runs/{RUN_ID}/postgres-cases/{item['case_id']}"
        else:
            base = f"runs/{RUN_ID}/trials/{item['source_trial_id']}"
        for name in ("result.json", "events.jsonl"):
            original = f"{base}/service/{name}"
            copied = bundle / "cases" / item["case_id"] / "service" / name
            if source.read(original) != copied.read_bytes():
                raise EvidenceError(f"compact copy differs from original: {original}")


def verify_bundle(bundle: Path, archive: Path | None = None,
                  source_root: Path | None = None) -> dict[str, Any]:
    bundle = bundle.expanduser().resolve(strict=True)
    manifest_path = bundle / "bundle-manifest.json"
    if not bundle.is_dir() or not manifest_path.is_file() or manifest_path.is_symlink():
        raise EvidenceError("bundle directory or bundle-manifest.json is missing")
    try:
        manifest = json.loads(manifest_path.read_bytes())
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EvidenceError(f"cannot parse bundle manifest: {exc}") from exc
    expected = manifest.get("files")
    actual = [row for row in bundle_files(bundle) if row["path"] != "bundle-manifest.json"]
    if not isinstance(expected, list) or actual != expected:
        raise EvidenceError("bundle file set, byte count, or SHA-256 differs from bundle manifest")
    raw_manifest = parse_manifest((bundle / "original-file-manifest.csv").read_bytes())
    raw_meta = manifest.get("raw_archive") or {}
    if archive is not None:
        observed = verify_archive(archive, raw_manifest)
        for key in ("size_bytes", "sha256", "file_count", "uncompressed_bytes"):
            if observed[key] != raw_meta.get(key):
                raise EvidenceError(f"raw archive {key} differs from bundle manifest")
    rebuilt = False
    if source_root is not None:
        source = Source(source_root)
        rows, _ = file_manifest(source.root)
        if rows != raw_manifest:
            raise EvidenceError("unpacked source does not match original-file-manifest.csv")
        compact_copy_check(bundle, source)
        _, _, _, _, generated = build_summaries(source)
        for name in ("campaign-summary.json", "cases.csv", "resource-samples.csv", "resource-summary.json"):
            if generated[name] != (bundle / name).read_bytes():
                raise EvidenceError(f"derived output drifted: {name}")
        expected_warnings, _ = warnings_doc(source)
        if expected_warnings != (bundle / "warnings/incidents.md").read_bytes():
            raise EvidenceError("warnings/incidents.md does not reproduce from original source")
        rebuilt = True
    return {
        "valid": True, "bundle_file_count": len(actual) + 1,
        "bundle_bytes": sum(x["bytes"] for x in actual) + manifest_path.stat().st_size,
        "original_file_count": len(raw_manifest),
        "original_file_bytes": sum(x["bytes"] for x in raw_manifest),
        "raw_archive_checked": archive is not None,
        "source_tree_checked": source_root is not None,
        "derived_outputs_rebuilt": rebuilt,
    }


def build_bundle(source_root: Path, output: Path, archive: Path) -> dict[str, Any]:
    source = Source(source_root)
    output = output.expanduser().absolute()
    if output.exists():
        raise EvidenceError(f"refusing to overwrite existing bundle: {output}")
    raw_rows, raw_bytes = file_manifest(source.root)
    archive_meta = verify_archive(archive, raw_rows)
    original_csv = render_manifest(raw_rows)
    manifest_ref = {"bytes": len(original_csv), "sha256": digest(original_csv)}
    _, _, _, _, generated = build_summaries(source)
    warnings, _ = warnings_doc(source)
    generated["original-file-manifest.csv"] = original_csv
    generated["warnings/incidents.md"] = warnings
    generated["README.md"] = readme(archive_meta, manifest_ref, output.name)
    output.parent.mkdir(parents=True, exist_ok=True)
    temp = Path(tempfile.mkdtemp(prefix=f".{output.name}.", dir=output.parent))
    try:
        for name, payload in sorted(generated.items()):
            write_new(temp, name, payload)
        file_list = bundle_files(temp)
        bundle_manifest = {
            "schema_version": 1, "run_id": RUN_ID, "files": file_list,
            "raw_archive": {"path": archive_meta["path"], "size_bytes": archive_meta["size_bytes"],
                            "sha256": archive_meta["sha256"], "file_count": archive_meta["file_count"],
                            "uncompressed_bytes": archive_meta["uncompressed_bytes"], "shared_url": None},
            "original_file_manifest": {"path": "original-file-manifest.csv",
                                       "bytes": manifest_ref["bytes"], "sha256": manifest_ref["sha256"]},
            "note": "file hashes exclude bundle-manifest.json itself",
        }
        write_new(temp, "bundle-manifest.json", canonical_json(bundle_manifest))
        verify_bundle(temp, archive=archive, source_root=source.root)
        if output.exists():
            raise EvidenceError(f"refusing to overwrite concurrently created bundle: {output}")
        os.rename(temp, output)
    except BaseException:
        shutil.rmtree(temp, ignore_errors=True)
        raise
    bundle_total = sum(p.stat().st_size for p in output.rglob("*") if p.is_file())
    return {"bundle": str(output), "bundle_file_count": len(file_list) + 1, "bundle_bytes": bundle_total,
            "original_file_count": len(raw_rows), "original_file_bytes": raw_bytes,
            "raw_archive": archive_meta}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subs = parser.add_subparsers(dest="command", required=True)
    build = subs.add_parser("build", help="create a new bundle from original sources")
    build.add_argument("--source-root", type=Path, required=True)
    build.add_argument("--bundle", type=Path, required=True)
    build.add_argument("--archive", type=Path, required=True)
    verify = subs.add_parser("verify", help="verify compact bundle, raw archive, and/or unpacked source")
    verify.add_argument("--bundle", type=Path, required=True)
    verify.add_argument("--archive", type=Path)
    verify.add_argument("--source-root", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "build":
            result = build_bundle(args.source_root, args.bundle, args.archive)
        else:
            result = verify_bundle(args.bundle, args.archive, args.source_root)
        print(json.dumps(result, sort_keys=True, indent=2))
        return 0
    except (EvidenceError, OSError, tarfile.TarError) as exc:
        print(f"evidence verification failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
