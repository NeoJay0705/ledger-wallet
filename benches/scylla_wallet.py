#!/usr/bin/env python3
"""Owned, repeatable ScyllaDB wallet benchmark runner (Python stdlib only)."""
from __future__ import annotations

import argparse
import csv
import datetime
import gzip
import hashlib
import ipaddress
import json
import os
import pathlib
import platform
import re
import shutil
import socket
import stat
import statistics
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any, Iterable, Sequence

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
CRATE_DIR = REPO_ROOT / "benchmarks" / "scylla-wallet"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "target" / "scylla-wallet" / "runs"
IMAGE_TAG = "scylladb/scylla:2026.1.14"
PINNED_REPO_DIGEST = "scylladb/scylla@sha256:333b9ab3a3214c7f98af100711c51df7f58964e8227e6d26050e889ebb216df1"
RUST_PACKAGE = "ledger-wallet-scylla-bench"
WALLET_PROFILE = "wallet-memory-lwt-v1"
ACCOUNT_STATE_SOURCE = "memory"
LEDGER_KEY = ("account_id", "kind", "tx_id")
SERIAL_CL = "LOCAL_SERIAL"
CQL_REQUESTS_PER_TRANSACTION = 1
CQL_STATEMENTS_PER_TRANSACTION = 1
SCYLLA_SOURCE_REF = "scylla-2026.1.14"
SCYLLA_SOURCE_COMMIT = "0dbea6201d676548993bc5889d8f4a34dd6ef54c"
PERIODIC_SYNC_LWT_SOURCE_COMMIT = "0dbea6201d676548993bc5889d8f4a34dd6ef54c"
PERIODIC_SYNC_DEFAULT_PERIOD_MS = 10_000
PERIODIC_IO_QUEUE_READINESS_TIMEOUT_SECONDS = 30
PERIODIC_IO_QUEUE_READINESS_POLL_SECONDS = 1
PERIODIC_SYNC_LWT_SOURCE_FILES = {
    "service/paxos/paxos_state.cc": {
        "sha256": "ce604cec3fb307cd1f9702bd736542c3dd06a4d79157c133268d15480d117edf",
        "evidence": {
            "prepare_promise_write": "save_paxos_promise(*schema",
            "accept_proposal_write": "save_paxos_proposal(*schema",
            "learn_forces_sync": "db::commitlog::force_sync::yes",
            "non_tablet_profile_uses_system_paxos": "if (!s.table().uses_tablets())",
            "non_tablet_paxos_schema": "co_return db::system_keyspace::paxos()",
        },
    },
    "service/storage_proxy.cc": {
        "sha256": "01e21de8e8958ff4541a95317b9391147340d0de399642aede3fc99c57992034",
        "evidence": {
            "prepare_path": "paxos::paxos_state::prepare(",
            "accept_path": "paxos::paxos_state::accept(",
            "learn_path": "paxos::paxos_state::learn(",
        },
    },
    "db/system_keyspace.cc": {
        "sha256": "e5ce0e5beee75b6b1c078234aab4171d127f173d080ed9fc768cfbee80f9bd09",
        "evidence": {
            "paxos_table_sync_property": "builder.set_wait_for_sync_to_commitlog(true)",
            "property_is_for_paxos_table": "system_keyspace::PAXOS",
        },
    },
    "replica/database.cc": {
        "sha256": "fd94c1bb730ff18237260c3286aa0efd45fff985cfb9176ed9851a035c17787f",
        "evidence": {
            "schema_property_enables_force_sync": "sync = sync || db::commitlog::force_sync(s->wait_for_sync_to_commitlog())",
            "writer_receives_sync": "commitlog_entry_writer cew(s, m, sync)",
            "writer_submitted_to_commitlog": "cf.commitlog()->add_entry(uuid, cew, timeout)",
        },
    },
    "db/commitlog/commitlog.cc": {
        "sha256": "8e5bde0fa0228db89f1a342b3a242154a0edea2301fa2726ff9716e13c2736d6",
        "evidence": {
            "forced_writer_uses_sync_path_in_periodic_mode": "_segment_manager->cfg.mode == sync_mode::BATCH || writer.sync",
            "sync_path_waits_for_batch_cycle": "co_await s->batch_cycle(timeout)",
            "batch_cycle_doc": "For batch mode we force a write",
            "sync_calls_cycle": "return cycle(true)",
        },
    },
    "db/config.cc": {
        "sha256": "194cea8734ec311c5163ea2a5a34d71072ee63251469ce2247b5f89a83f90e2f",
        "evidence": {
            "periodic_mode_ack_semantics": "Periodic syncs are acknowledged immediately.",
            "period_default_10000": '"commitlog_sync_period_in_ms", value_status::Used, 10000',
        },
    },
}
PERIODIC_SYNC_LWT_DOC_URL = (
    "https://docs.scylladb.com/manual/branch-2026.1/kb/lwt-differences.html")
PERIODIC_SYNC_LWT_DOC_PHRASE = "This is done in all commitlog_sync modes."
PERIODIC_SYNC_LWT_DOC_DEVICE_PHRASE = "has hit the device"
SCYLLA_SEASTAR_COMMIT = "bf882627c3f813c8cfb88cb595ceed34d6462300"
SCYLLA_IO_PROPERTIES_SOURCE_EVIDENCE = {
    "repository": "scylladb/scylla-seastar",
    "commit": SCYLLA_SEASTAR_COMMIT,
    "disk_params_source_sha256": "03e52b910cbb7694740fd2388f87dd14c748ebf7ee4dd8c24b1a93ef2d1b30de",
    "disk_params_source_url": (
        f"https://raw.githubusercontent.com/scylladb/scylla-seastar/"
        f"{SCYLLA_SEASTAR_COMMIT}/src/core/disk_params.cc"),
    "disk_params_source_locations": "src/core/disk_params.cc:98 YAMLLoadFile/st_dev; generate_config:181 mountpoint labels",
    "disk_params_behavior": (
        "YAMLLoadFile reads the configured file, stats mountpoints, and maps queue devices; "
        "runtime io_queue_activations labels expose per-shard device mapping, including empty-mountpoint fallback queues"),
    "reactor_source_url": (
        f"https://raw.githubusercontent.com/scylladb/scylla-seastar/"
        f"{SCYLLA_SEASTAR_COMMIT}/src/core/reactor.cc"),
    "registered_option_location": "src/core/reactor.cc:3683",
    "runtime_metric": "scylla_io_queue_activations",
    "runtime_metric_classes": ["main", "commitlog"],
    "expected_container_mountpoint": "/var/lib/scylla",
}
DRIVER_SOURCE_REF = "v1.9.0"
SCYLLA_SOURCE_FILES = (
    "db/config.cc", "db/config.hh", "db/commitlog/commitlog.cc",
    "db/commitlog/commitlog.hh", "service/storage_proxy.cc", "main.cc",
)
DRIVER_SOURCE_FILE = "scylla/src/network/connection.rs"
SEASTAR_METRICS_SOURCE_FILE = "src/core/metrics.cc"
SEASTAR_AIO_SOURCE_FILES = (
    "src/core/metrics.cc", "src/core/reactor.cc", "include/seastar/core/reactor.hh",
    "src/core/disk_params.cc",
)
EXPECTED_SOURCE_SHA256 = {
    "db/config.cc": "194cea8734ec311c5163ea2a5a34d71072ee63251469ce2247b5f89a83f90e2f",
    "db/config.hh": "811d86268d30fdd93a1554c4436479db53c50fff4ea18dc58d37972f2de7fa8f",
    "db/commitlog/commitlog.cc": "8e5bde0fa0228db89f1a342b3a242154a0edea2301fa2726ff9716e13c2736d6",
    "db/commitlog/commitlog.hh": "b935d23374880689d6d1981f1318fa1ebdd183e8a31fbda2188507fe75bd8ad1",
    "service/storage_proxy.cc": "01e21de8e8958ff4541a95317b9391147340d0de399642aede3fc99c57992034",
    "main.cc": "e33d5b2e1a42a0e9334ad51f06f2c52d321ebe889cdb6a0a1229a325039ae728",
    DRIVER_SOURCE_FILE: "dd185b37f57bd7e6bfdeb2d41af48283e070e65ee9e4bae91843949dd2ddfdf8",
}
EXPECTED_SEASTAR_METRICS_SHA256 = "41fa65e506ff588df5360e0ec60767cb4c6e215933db0b48fcfb39dcdf728a98"
EXPECTED_SEASTAR_AIO_SHA256 = {
    "src/core/metrics.cc": EXPECTED_SEASTAR_METRICS_SHA256,
    "src/core/reactor.cc": "a544b859a44781c301cd0e9e0b628fcc490c20fb5d567ccf491a440b5ebb9b57",
    "include/seastar/core/reactor.hh": "22c9238983d26daeec8ff8d12e8d09ca520309af913ec851e6066d236efffee2",
    "src/core/disk_params.cc": "03e52b910cbb7694740fd2388f87dd14c748ebf7ee4dd8c24b1a93ef2d1b30de",
}
SCYLLA_AIO_PROFILE = {
    "reactor_backend": "linux-aio",
    "max_networking_io_control_blocks": 1024,
    "reserve_io_control_blocks": 1024,
    "startup_flags": {
        "--reactor-backend": "linux-aio",
        "--max-networking-io-control-blocks": 1024,
        "--reserve-io-control-blocks": 1024,
    },
    "scope": "fixed local benchmark profile; bounded in-flight networking AIO, not production-optimal",
}
AIO_CONTROL_BLOCK_HEADROOM = 1024
STORAGE_IO_CONTROL_BLOCKS_PER_SHARD = 1024
PREEMPT_IO_CONTROL_BLOCKS_PER_SHARD = 2
SCYLLA_AIO_SOURCE_EVIDENCE = {
    "scylla_release_ref": SCYLLA_SOURCE_REF,
    "scylla_release_commit": SCYLLA_SOURCE_COMMIT,
    "scylladb_main": {
        "url": "https://raw.githubusercontent.com/scylladb/scylladb/scylla-2026.1.14/main.cc",
        "sha256": EXPECTED_SOURCE_SHA256["main.cc"],
        "location": "main.cc:716",
        "behavior": "release default max_networking_io_control_blocks=50000; runner overrides it explicitly",
    },
    "seastar_submodule": {
        "repository": "https://github.com/scylladb/scylla-seastar",
        "commit": SCYLLA_SEASTAR_COMMIT,
        "disk_params_source": {
            "url": f"https://raw.githubusercontent.com/scylladb/scylla-seastar/{SCYLLA_SEASTAR_COMMIT}/src/core/disk_params.cc",
            "sha256": EXPECTED_SEASTAR_AIO_SHA256["src/core/disk_params.cc"],
            "locations": "YAMLLoadFile at line 98; generate_config mountpoint labeling at line 181",
        },
        "reactor_source": {
            "url": f"https://raw.githubusercontent.com/scylladb/scylla-seastar/{SCYLLA_SEASTAR_COMMIT}/src/core/reactor.cc",
            "sha256": EXPECTED_SEASTAR_AIO_SHA256["src/core/reactor.cc"],
            "location": "src/core/reactor.cc:4200,4423",
            "behavior": "adjust_max_networking_aio_io_control_blocks may lower networking IOCBs against available AIO budget",
        },
        "reactor_header": {
            "url": f"https://raw.githubusercontent.com/scylladb/scylla-seastar/{SCYLLA_SEASTAR_COMMIT}/include/seastar/core/reactor.hh",
            "sha256": EXPECTED_SEASTAR_AIO_SHA256["include/seastar/core/reactor.hh"],
            "location": "include/seastar/core/reactor.hh:230-232",
            "behavior": "max_aio_per_queue=128 and max_aio=1024; preemption budget is 2",
        },
    },
}
METRICS_RELABEL_CONTAINER_PATH = "/etc/scylla/ledger-wallet-metrics.yaml"
IO_PROPERTIES_CONTAINER_PATH = "/etc/scylla/ledger-wallet-io-properties.yaml"
METRICS_RELABEL_ROOT_KEY = "relabel_configs"
METRICS_RELABEL_CONFIG = """relabel_configs:
  - source_labels: [__name__]
    regex: '.*reactor_utilization'
    target_label: __level
    replacement: basic
  - source_labels: [__name__]
    regex: '.*storage_proxy_coordinator_(read_timeouts|write_timeouts|write_errors.*|read_errors.*)'
    action: report_when_empty
metric_family_configs: []
"""
METRICS_RELABEL_SOURCE_EVIDENCE = {
    "scylla_release_ref": SCYLLA_SOURCE_REF,
    "scylla_release_commit": SCYLLA_SOURCE_COMMIT,
    "release_sources": {
        "service/storage_proxy.cc": {
            "url": "https://raw.githubusercontent.com/scylladb/scylladb/scylla-2026.1.14/service/storage_proxy.cc",
            "sha256": EXPECTED_SOURCE_SHA256["service/storage_proxy.cc"],
            "behavior": "coordinator read/write timeout and error metric families may skip empty values",
        },
        "db/config.cc": {
            "url": "https://raw.githubusercontent.com/scylladb/scylladb/scylla-2026.1.14/db/config.cc",
            "sha256": EXPECTED_SOURCE_SHA256["db/config.cc"],
            "behavior": "pinned release reads YAML key relabel_configs",
            "parser_location": "db/config.cc:1991",
            "parser_expression": "const YAML::Node& relabel_configs = yaml[\"relabel_configs\"];",
        },
        "main.cc": {
            "url": "https://raw.githubusercontent.com/scylladb/scylladb/scylla-2026.1.14/main.cc",
            "sha256": EXPECTED_SOURCE_SHA256["main.cc"],
            "behavior": "pinned server exposes --relabel-config-file startup option",
        },
    },
    "seastar_submodule": {
        "repository": "https://github.com/scylladb/scylla-seastar",
        "commit": SCYLLA_SEASTAR_COMMIT,
        "source": {
            "url": f"https://raw.githubusercontent.com/scylladb/scylla-seastar/{SCYLLA_SEASTAR_COMMIT}/{SEASTAR_METRICS_SOURCE_FILE}",
            "sha256": EXPECTED_SEASTAR_METRICS_SHA256,
            "locations": "report_when_empty relabel action at lines 179-182 and 630-635",
        },
    },
}
KEYSPACE_TABLETS_SOURCE_EVIDENCE = {
    "release_ref": SCYLLA_SOURCE_REF,
    "release_commit": SCYLLA_SOURCE_COMMIT,
    "get_initial_tablets": {
        "url": "https://raw.githubusercontent.com/scylladb/scylladb/scylla-2026.1.14/cql3/statements/ks_prop_defs.cc",
        "sha256": "b27ea7c75761c6acef9a4fa013e6de7e01c04b01c3f445dbe33edc5183297047",
        "behavior": "tablets.enabled=false returns nullopt; default-enabled NTS can carry initial_tablets=0",
    },
    "persist_scylla_keyspaces": {
        "url": "https://raw.githubusercontent.com/scylladb/scylladb/scylla-2026.1.14/db/schema_tables.cc",
        "sha256": "dd6761f88d40906a84dcfd888f5fb75c71cbcc2eabedb470b4694e68baa239e0",
        "behavior": "initial_tablets is written only when the optional value is present",
    },
}
CPU_ASSIGNMENTS = {
    1: {"nodea": (0,), "nodeb": (2,), "nodec": (4,)},
    2: {"nodea": (0, 1), "nodeb": (2, 3), "nodec": (4, 5)},
    4: {"nodea": (0, 1, 8, 9), "nodeb": (2, 3, 10, 11), "nodec": (4, 5, 12, 13)},
}
SERVICE_CPUS = (6, 7, 14, 15)
NODE_NAMES = ("nodea", "nodeb", "nodec")
SUPPORTED_NODE_COUNTS = (1, 3)
DEFAULT_SHARDS = (1, 2, 4)
DEFAULT_WORKERS = (12, 24, 48)
CONSISTENCY_LEVELS = {"ONE": 1, "TWO": 2, "THREE": 3}
CONSISTENCY_NAMES = {value: name for name, value in CONSISTENCY_LEVELS.items()}
WALLET_COLUMN_SCHEMA = {
    "account_id": ("bigint", "partition_key", 0),
    "kind": ("tinyint", "clustering", 0),
    "tx_id": ("bigint", "clustering", 1),
    "balance": ("blob", "static", None),
    "version": ("bigint", "static", None),
    "write_clock": ("bigint", "static", None),
    "op": ("tinyint", "regular", None),
    "amount": ("blob", "regular", None),
    "outcome": ("tinyint", "regular", None),
    "balance_after": ("blob", "regular", None),
    "ledger_version": ("bigint", "regular", None),
    "ledger_write_clock": ("bigint", "regular", None),
}
FORMAL_USERS, FORMAL_REQUESTS_PER_USER, FORMAL_REPEATS = 50_000, 200, 3
SMOKE_USERS, SMOKE_REQUESTS_PER_USER = 100, 20
PILOT_USERS, PILOT_REQUESTS_PER_USER = 1_000, 20
FORMAL_CASE_COUNT = 27
QUEUE_CAPACITY = 50_000
NODE_MEMORY_BYTES, NODE_CGROUP_BYTES = 8 * 1024**3, 10 * 1024**3
FORMAL_MIN_AVAILABLE_DISK_BYTES = 32 * 1024**3
FORMAL_MIN_AVAILABLE_MEMORY_BYTES = 32 * 1024**3
FORMAL_DISK_RESERVE_BYTES = 16 * 1024**3
STARTUP_WAIT_SECONDS, SEED_READY_WAIT_SECONDS = 360, 900
CLI_GATE_WAIT_SECONDS = 180
SHUTDOWN_WAIT_SECONDS = 60
EXPECTED_HOST_IO_LIMITS = {
    "device_path": "/dev/sdb",
    "major_minor": "8:16",
    "read_bps": 403_631_184,
    "write_bps": 152_226_096,
    "read_iops": 24_497,
    "write_iops": 23_500,
}


class RunnerError(RuntimeError):
    """Configuration, validation, or owned-resource failure."""


def validate_host_io_limits_document(value: Any) -> dict[str, Any]:
    """Validate the common, pre-calibrated block-I/O cap profile."""
    if not isinstance(value, dict) or set(value) != set(EXPECTED_HOST_IO_LIMITS):
        raise RunnerError(
            "host I/O limits JSON must contain exactly "
            + ", ".join(sorted(EXPECTED_HOST_IO_LIMITS)))
    normalized: dict[str, Any] = {}
    for name, expected in EXPECTED_HOST_IO_LIMITS.items():
        actual = value[name]
        if isinstance(expected, int):
            if isinstance(actual, bool) or not isinstance(actual, int) or actual != expected:
                raise RunnerError(f"host I/O limit {name}={actual!r}; expected {expected}")
        elif actual != expected:
            raise RunnerError(f"host I/O limit {name}={actual!r}; expected {expected!r}")
        normalized[name] = actual
    return normalized


def load_host_io_limits(path: pathlib.Path | None) -> dict[str, Any] | None:
    """Read the optional common caps file and confirm its device node identity."""
    if path is None:
        return None
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RunnerError(f"cannot read host I/O limits JSON {path}: {exc}") from exc
    limits = validate_host_io_limits_document(document)
    try:
        info = pathlib.Path(limits["device_path"]).stat()
    except OSError as exc:
        raise RunnerError(f"cannot stat capped block device {limits['device_path']}: {exc}") from exc
    if not stat.S_ISBLK(info.st_mode):
        raise RunnerError(f"capped I/O path is not a block device: {limits['device_path']}")
    actual = f"{os.major(info.st_rdev)}:{os.minor(info.st_rdev)}"
    if actual != limits["major_minor"]:
        raise RunnerError(
            f"capped block device {limits['device_path']} is {actual}, expected {limits['major_minor']}")
    limits["source_json"] = str(path.resolve())
    limits["source_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    return limits


def parse_cgroup_io_max(payload: str, limits: dict[str, Any]) -> dict[str, Any]:
    """Extract and verify the target device row from cgroup v2 io.max."""
    matches: list[dict[str, Any]] = []
    for line in payload.splitlines():
        fields = line.split()
        if fields and fields[0] == limits["major_minor"]:
            values: dict[str, Any] = {"major_minor": fields[0]}
            for field in fields[1:]:
                name, sep, raw = field.partition("=")
                if not sep or name in values:
                    raise RunnerError(f"invalid cgroup io.max entry: {line!r}")
                if raw == "max":
                    values[name] = "max"
                else:
                    try:
                        values[name] = int(raw)
                    except ValueError as exc:
                        raise RunnerError(f"invalid cgroup io.max value: {field!r}") from exc
            matches.append(values)
    if len(matches) != 1:
        raise RunnerError(
            f"cgroup io.max must contain exactly one {limits['major_minor']} row; found {len(matches)}")
    observed = matches[0]
    expected = {"major_minor": limits["major_minor"], "rbps": limits["read_bps"],
                "wbps": limits["write_bps"], "riops": limits["read_iops"],
                "wiops": limits["write_iops"]}
    if observed != expected:
        raise RunnerError(f"cgroup io.max limits differ: expected={expected}, observed={observed}")
    return observed


def validate_docker_io_limit_host_config(host_config: dict[str, Any],
                                         limits: dict[str, Any]) -> dict[str, Any]:
    """Require Docker inspect to report all four requested per-device caps."""
    names = ("BlkioDeviceReadBps", "BlkioDeviceWriteBps",
             "BlkioDeviceReadIOps", "BlkioDeviceWriteIOps")
    values = {name: host_config.get(name) for name in names}
    expected = {
        "BlkioDeviceReadBps": limits["read_bps"],
        "BlkioDeviceWriteBps": limits["write_bps"],
        "BlkioDeviceReadIOps": limits["read_iops"],
        "BlkioDeviceWriteIOps": limits["write_iops"],
    }
    for name, expected_rate in expected.items():
        entries = values[name]
        if not isinstance(entries, list) or len(entries) != 1:
            raise RunnerError(f"Docker HostConfig.{name} must contain one exact device limit")
        entry = entries[0]
        if (not isinstance(entry, dict) or entry.get("Path") != limits["device_path"]
                or entry.get("Rate") != expected_rate):
            raise RunnerError(
                f"Docker HostConfig.{name} differs from the common caps: {entries!r}")
    return {"valid": True, "limits": values}


def process_cgroup_io_max(pid: int) -> str:
    """Read the cgroup v2 io.max file owning a host PID."""
    try:
        entries = pathlib.Path(f"/proc/{pid}/cgroup").read_text().splitlines()
    except OSError as exc:
        raise RunnerError(f"cannot read cgroup identity for pid {pid}: {exc}") from exc
    relative = next((line.split(":", 2)[2].lstrip("/") for line in entries
                     if line.startswith("0::")), None)
    if relative is None:
        raise RunnerError(f"pid {pid} has no cgroup v2 identity")
    path = pathlib.Path("/sys/fs/cgroup") / relative / "io.max"
    try:
        return path.read_text(encoding="ascii")
    except OSError as exc:
        raise RunnerError(f"cannot read cgroup io.max for pid {pid}: {exc}") from exc


def validate_container_io_limits(host_config: dict[str, Any], pid: int,
                                 limits: dict[str, Any],
                                 io_max_payload: str | None = None) -> dict[str, Any]:
    """Verify both Docker's saved request and the live cgroup's effective caps."""
    host = validate_docker_io_limit_host_config(host_config, limits)
    payload = process_cgroup_io_max(pid) if io_max_payload is None else io_max_payload
    cgroup = parse_cgroup_io_max(payload, limits)
    return {"valid": True, "host_config": host,
            "host_pid": pid, "cgroup_io_max_raw": payload.strip(),
            "effective_limits": cgroup}


def docker_io_limit_flags(limits: dict[str, Any] | None) -> list[str]:
    if limits is None:
        return []
    return [item for flag, key in (("--device-read-bps", "read_bps"),
                                   ("--device-write-bps", "write_bps"),
                                   ("--device-read-iops", "read_iops"),
                                   ("--device-write-iops", "write_iops"))
            for item in (flag, f"{limits['device_path']}:{limits[key]}")]


@dataclass(frozen=True)
class TrialCase:
    shards: int
    workers: int
    per_shard_pool: int
    repeat: int
    order: int
    worker_routing: str = "round-robin"
    per_shard_pool_overridden: bool = False
    requested_per_shard_pool: int | None = None
    replication_factor: int = 3
    read_cl: str = "THREE"
    write_cl: str = "THREE"
    wallet_profile: str = WALLET_PROFILE
    node_count: int = 3

    @property
    def data_connection_target(self) -> int:
        return self.node_count * self.shards * self.per_shard_pool

    @property
    def expected_connections_per_node(self) -> int:
        return self.shards * self.per_shard_pool

    @property
    def setup_concurrency(self) -> int:
        return min(self.workers, self.data_connection_target)

    @property
    def per_worker_queue_capacity(self) -> int:
        return 1 if self.worker_routing == "per-user" else QUEUE_CAPACITY

    @property
    def case_id(self) -> str:
        durability = ("" if self.replication_factor == 3 and self.read_cl == "THREE"
                      and self.write_cl == "THREE" else
                      f"-RF{self.replication_factor}-R{CONSISTENCY_LEVELS[self.read_cl]}"
                      f"-W{CONSISTENCY_LEVELS[self.write_cl]}")
        if self.per_shard_pool_overridden or self.worker_routing != "round-robin":
            return (f"S{self.shards}-C{self.workers}-P{self.per_shard_pool}"
                    f"-D{self.data_connection_target}-{self.worker_routing}{durability}"
                    f"{'' if self.node_count == 3 else f'-N{self.node_count}'}"
                    f"-{self.wallet_profile}"
                    f"-rep{self.repeat}")
        return (f"S{self.shards}-C{self.workers}{durability}"
                f"{'' if self.node_count == 3 else f'-N{self.node_count}'}-"
                f"{self.wallet_profile}-rep{self.repeat}")


@dataclass
class ContainerRecord:
    name: str
    node: str
    container_id: str
    ip: str
    cpuset: str
    host_pid: int | None = None
    host_process_cmd: str | None = None
    state: str = "created"


@dataclass
class CleanupResult:
    status: str = "not_started"
    containers_stopped: bool = False
    post_stop_inspection_files: list[str] = field(default_factory=list)
    containers_removed: list[str] = field(default_factory=list)
    network_removed: bool = False
    data_removed: list[str] = field(default_factory=list)
    retained_data: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def parse_int_csv(value: str, label: str) -> tuple[int, ...]:
    try:
        values = tuple(int(p.strip()) for p in value.split(",") if p.strip())
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"{label} must be comma-separated integers") from exc
    if not values or any(v <= 0 for v in values):
        raise argparse.ArgumentTypeError(f"{label} values must be positive integers")
    if len(set(values)) != len(values):
        raise argparse.ArgumentTypeError(f"{label} values must not repeat")
    return values


def node_names_for(node_count: int) -> tuple[str, ...]:
    if isinstance(node_count, bool) or node_count not in SUPPORTED_NODE_COUNTS:
        raise RunnerError(f"node-count must be one of {','.join(map(str, SUPPORTED_NODE_COUNTS))}")
    return NODE_NAMES[:node_count]


def _canonical_consistency(value: int | str, label: str) -> str:
    if isinstance(value, bool):
        raise RunnerError(f"{label} must be one of 1, 2, or 3")
    if isinstance(value, int):
        name = CONSISTENCY_NAMES.get(value)
    elif isinstance(value, str):
        name = value.upper()
        if name not in CONSISTENCY_LEVELS:
            name = None
    else:
        name = None
    if name is None:
        raise RunnerError(f"{label} must be one of 1, 2, or 3")
    return name


def case_id_for_profile(case: TrialCase, commitlog_sync: str = "batch",
                        commitlog_sync_period_in_ms: int | None = None) -> str:
    if commitlog_sync == "batch":
        return case.case_id
    return f"{case.case_id}-commitlog-periodic-{commitlog_sync_period_in_ms}ms"


def build_matrix(shards: Sequence[int], workers: Sequence[int], repeats: int,
                 per_shard_pool: int | None = None,
                 worker_routing: str = "round-robin",
                 replication_factor: int = 3,
                 read_cl: int | str = 3,
                 write_cl: int | str = 3,
                 node_count: int = 3) -> list[TrialCase]:
    node_names_for(node_count)
    if repeats < 1:
        raise RunnerError("repeats must be positive")
    if len(set(shards)) != len(shards) or any(s not in CPU_ASSIGNMENTS for s in shards):
        raise RunnerError("shards must be a unique subset of 1,2,4")
    if len(set(workers)) != len(workers) or any(c <= 0 for c in workers):
        raise RunnerError("workers must be unique positive integers")
    if per_shard_pool is not None and per_shard_pool <= 0:
        raise RunnerError("per-shard-pool must be a positive integer")
    if worker_routing not in ("round-robin", "per-user"):
        raise RunnerError("worker-routing must be round-robin or per-user")
    if (isinstance(replication_factor, bool) or not isinstance(replication_factor, int)
            or replication_factor not in (1, 2, 3)):
        raise RunnerError("replication-factor must be 1, 2, or 3")
    if replication_factor > node_count:
        raise RunnerError(f"replication-factor {replication_factor} exceeds node-count {node_count}")
    read_name = _canonical_consistency(read_cl, "read-cl")
    write_name = _canonical_consistency(write_cl, "write-cl")
    if CONSISTENCY_LEVELS[read_name] > replication_factor:
        raise RunnerError(f"read-cl {read_name} exceeds replication-factor {replication_factor}")
    if CONSISTENCY_LEVELS[write_name] > replication_factor:
        raise RunnerError(f"write-cl {write_name} exceeds replication-factor {replication_factor}")
    pairs = [(s, c) for s in shards for c in workers]
    for s, c in pairs:
        if per_shard_pool is None and c % (node_count * s):
            raise RunnerError(f"C={c} is not divisible by node-count*S={node_count*s} for S={s}")
    result: list[TrialCase] = []
    order = 0
    for repeat in range(1, repeats + 1):
        offset = (repeat - 1) % len(pairs)
        for s, c in pairs[offset:] + pairs[:offset]:
            order += 1
            pool = per_shard_pool if per_shard_pool is not None else c // (node_count * s)
            result.append(TrialCase(
                s, c, pool, repeat, order, worker_routing,
                per_shard_pool is not None, per_shard_pool,
                replication_factor, read_name, write_name, WALLET_PROFILE, node_count))
    return result


def cpuset_for(shards: int, node_count: int = 3) -> dict[str, str]:
    if shards not in CPU_ASSIGNMENTS:
        raise RunnerError(f"unsupported shard count S={shards}")
    node_names = node_names_for(node_count)
    return {**{n: ",".join(map(str, CPU_ASSIGNMENTS[shards][n])) for n in node_names},
            "service": ",".join(map(str, SERVICE_CPUS))}


def _run(args: Sequence[str], *, check: bool = True, timeout: float | None = None,
         capture: bool = True, env: dict[str, str] | None = None,
         cwd: pathlib.Path | None = None) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(list(args), check=False, text=True,
            stdout=subprocess.PIPE if capture else None,
            stderr=subprocess.PIPE if capture else None, timeout=timeout, env=env, cwd=cwd)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RunnerError(f"command failed to run: {args[0]}: {exc}") from exc
    if check and result.returncode:
        raise RunnerError(f"command exited {result.returncode}: {' '.join(args)}\n"
                          f"{(result.stdout or '')[-4000:]}{(result.stderr or '')[-4000:]}")
    return result


def _utc_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")


def write_json(path: pathlib.Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def write_metrics_relabel_profile(trial_dir: pathlib.Path) -> dict[str, Any]:
    """Write and hash the fixed, non-dropping Prometheus relabel profile."""
    config_path = trial_dir / "metrics-relabel.yaml"
    config_path.write_text(METRICS_RELABEL_CONFIG, encoding="utf-8")
    digest = hashlib.sha256(config_path.read_bytes()).hexdigest()
    profile = {
        "config_path": str(config_path.resolve()),
        "container_path": METRICS_RELABEL_CONTAINER_PATH,
        "sha256": digest,
        "rules": [
            {"source_labels": ["__name__"], "regex": ".*reactor_utilization",
             "target_label": "__level", "replacement": "basic"},
            {"source_labels": ["__name__"],
             "regex": ".*storage_proxy_coordinator_(read_timeouts|write_timeouts|write_errors.*|read_errors.*)",
             "action": "report_when_empty"},
        ],
        "metric_family_configs": [],
        "source_evidence": METRICS_RELABEL_SOURCE_EVIDENCE,
        "semantics": "report empty required storage-proxy metric families as server-emitted zero; unobserved window metrics remain N/A",
    }
    write_json(trial_dir / "metrics-relabel-profile.json", profile)
    return profile


def inspect_io_properties_source(source_path: pathlib.Path | None) -> dict[str, Any]:
    """Read one stable I/O properties input and describe its provenance."""
    if source_path is None:
        return {"enabled": False, "requested_path": None, "resolved_source_path": None,
                "source_sha256": None, "source_size_bytes": 0, "content": None,
                "provenance": {"kind": "no --io-properties-file supplied"}}
    requested = source_path.expanduser()
    try:
        resolved = requested.resolve(strict=True)
        before = resolved.stat()
        if not resolved.is_file():
            raise RunnerError(f"I/O properties input is not a regular file: {resolved}")
        raw = resolved.read_bytes()
        after = resolved.stat()
        stable_fields = ("st_dev", "st_ino", "st_size", "st_mtime_ns")
        if any(getattr(before, key) != getattr(after, key) for key in stable_fields):
            raise RunnerError(f"I/O properties input changed while being read: {resolved}")
        content = raw.decode("utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise RunnerError(f"cannot read UTF-8 I/O properties input {requested}: {exc}") from exc
    if not raw or not content.strip():
        raise RunnerError(f"I/O properties input is empty: {resolved}")
    digest = hashlib.sha256(raw).hexdigest()
    return {
        "enabled": True, "requested_path": str(requested),
        "resolved_source_path": str(resolved), "source_sha256": digest,
        "source_size_bytes": len(raw), "content": content,
        "provenance": {
            "kind": "user-supplied local file snapshot",
            "requested_path": str(requested), "resolved_path": str(resolved),
            "device": before.st_dev, "inode": before.st_ino,
            "size_bytes": before.st_size, "mtime_ns": before.st_mtime_ns,
            "captured_utc": _utc_now(),
        },
        "source_evidence": SCYLLA_IO_PROPERTIES_SOURCE_EVIDENCE,
    }


def write_io_properties_profile(trial_dir: pathlib.Path,
                                source_path: pathlib.Path | None,
                                captured_profile: dict[str, Any] | None = None) -> dict[str, Any]:
    """Persist exact input content and hash for a read-only container mount."""
    profile = (dict(captured_profile) if captured_profile is not None
               else inspect_io_properties_source(source_path))
    if profile["enabled"]:
        config_path = trial_dir / "io-properties.yaml"
        content_bytes = str(profile["content"]).encode("utf-8")
        config_path.write_bytes(content_bytes)
        config_path.chmod(0o444)
        copied_hash = hashlib.sha256(config_path.read_bytes()).hexdigest()
        if copied_hash != profile["source_sha256"]:
            raise RunnerError("I/O properties snapshot hash does not match the supplied file")
        profile.update({"config_path": str(config_path.resolve()),
                        "container_path": IO_PROPERTIES_CONTAINER_PATH,
                        "copied_sha256": copied_hash,
                        "read_only_container_mount": True,
                        "load_evidence": None})
    else:
        profile.update({"config_path": None, "container_path": None,
                        "copied_sha256": None, "read_only_container_mount": False,
                        "load_evidence": {"status": "not_requested"}})
    write_json(trial_dir / "io-properties-profile.json", profile)
    return profile


def validate_io_properties_load(profile: dict[str, Any],
                                launch_config: dict[str, dict[str, Any]],
                                startup_logs: dict[str, str],
                                node_names: Sequence[str],
                                runtime_nodes: dict[str, dict[str, Any]] | None = None,
                                shards: int = 1) -> dict[str, Any]:
    """Validate the readonly file, running argv, and Scylla's loaded queue mapping."""
    nodes: dict[str, Any] = {}
    problems: list[str] = []
    runtime_nodes = runtime_nodes or {}
    enabled = profile.get("enabled") is True
    expected_nodes = set(node_names)
    if (set(launch_config) != expected_nodes or set(startup_logs) != expected_nodes
            or (enabled and set(runtime_nodes) != expected_nodes)):
        problems.append("I/O properties evidence does not cover the exact owned node inventory")
    expected_path = profile.get("container_path") if enabled else None
    expected_source = profile.get("config_path") if enabled else None
    expected_hash = profile.get("source_sha256") if enabled else None
    for node in node_names:
        config = launch_config.get(node, {})
        flags = config.get("parsed_launch_flags", {})
        host = config.get("host_config", {})
        io_mounts = [mount for mount in (host.get("mounts") or [])
                     if mount.get("Destination") == IO_PROPERTIES_CONTAINER_PATH]
        actual_flag = flags.get("io_properties_file")
        actual_hash = config.get("observed_io_properties_sha256")
        runtime = runtime_nodes.get(node, {})
        log = startup_logs.get(node, "")
        missing_reason = "none of --io-properties and --io-properties-file are set."
        missing_warning = missing_reason in log
        if enabled:
            flag_valid = actual_flag == expected_path
            runtime_argv = runtime.get("scylla_argv")
            runtime_flag = argv_option_value(runtime_argv, "--io-properties-file")
            runtime_flag_valid = runtime_flag == expected_path
            mount_valid = (len(io_mounts) == 1
                           and io_mounts[0].get("Source") == expected_source
                           and io_mounts[0].get("RW") is False)
            hash_valid = (actual_hash == expected_hash
                          and runtime.get("container_file_sha256") == expected_hash)
            startup_valid = bool(log.strip()) and not missing_warning
            device_stats = runtime.get("device_stats", {})
            device_stats_valid = device_stats.get("valid") is True
            queue_evidence = runtime.get("io_queue_mountpoints", {})
            queue_mountpoints_valid = queue_evidence.get("valid") is True
            effective_load_valid = (flag_valid and runtime_flag_valid and mount_valid
                                    and hash_valid and startup_valid and device_stats_valid
                                    and queue_mountpoints_valid)
            if not flag_valid:
                problems.append(f"{node}: Scylla launch does not pass the expected io-properties-file")
            if not runtime_flag_valid:
                problems.append(f"{node}: live Scylla argv does not identify the expected io-properties-file")
            if not mount_valid:
                problems.append(f"{node}: I/O properties file is not the exact read-only snapshot mount")
            if not hash_valid:
                problems.append(f"{node}: mounted I/O properties content hash is missing or mismatched")
            if not startup_valid:
                problems.append(f"{node}: startup log reports missing I/O properties or is unavailable")
            if not device_stats_valid:
                problems.append(f"{node}: /var/lib/scylla, data, and commitlog device IDs do not agree")
            if not queue_mountpoints_valid:
                problems.append(f"{node}: Scylla main/commitlog queue mountpoint metrics do not identify /var/lib/scylla")
            nodes[node] = {"launch_flag": actual_flag, "launch_flag_valid": flag_valid,
                           "running_scylla_flag": runtime_flag,
                           "running_scylla_flag_valid": runtime_flag_valid,
                           "mount_count": len(io_mounts), "readonly_mount_valid": mount_valid,
                           "expected_sha256": expected_hash,
                           "observed_container_sha256": actual_hash,
                           "content_hash_valid": hash_valid,
                           "startup_log_nonempty": bool(log.strip()),
                           "missing_properties_warning": missing_warning,
                           "startup_acceptance_valid": startup_valid,
                           "device_stats": device_stats,
                           "device_stats_valid": device_stats_valid,
                           "io_queue_mountpoints": queue_evidence,
                           "queue_mountpoints_valid": queue_mountpoints_valid,
                           "effective_load_evidence_valid": effective_load_valid}
        else:
            disabled_valid = actual_flag is None and not io_mounts
            if not disabled_valid:
                problems.append(f"{node}: unspecified I/O properties profile has a launch flag or mount")
            nodes[node] = {"launch_flag": actual_flag, "mount_count": len(io_mounts),
                           "disabled_profile_valid": disabled_valid,
                           "profile_status": "legacy_no_file_profile"}
    return {"valid": not problems, "enabled": enabled,
            "expected_nodes": list(node_names), "nodes": nodes,
            "shards": shards,
            "source_evidence": SCYLLA_IO_PROPERTIES_SOURCE_EVIDENCE,
            "effective_load_claim": (
                "file hash, read-only mount, live Scylla argv, matching data/commitlog st_dev, "
                "and per-shard main/commitlog io_queue_activations labels include the data mountpoint; "
            "empty-mountpoint fallback and other mountpoint queues are retained separately. This verifies "
            "the data-device mapping, not calibration quality or whether other device I/O is controlled"
                if enabled else "no I/O properties file was requested; retained legacy profile"),
            "problems": problems}


def argv_option_value(argv: Any, option: str) -> str | None:
    if not isinstance(argv, (list, tuple)):
        return None
    for index, raw in enumerate(argv):
        value = str(raw)
        if value.startswith(option + "="):
            return value.partition("=")[2]
        if value == option and index + 1 < len(argv):
            return str(argv[index + 1])
    return None


def validate_io_queue_mountpoints(prometheus_by_node: dict[str, str],
                                  node_names: Sequence[str], shards: int,
                                  expected_mountpoint: str = "/var/lib/scylla") -> dict[str, Any]:
    """Require actual per-shard scheduler queues to identify the mounted data device."""
    problems: list[str] = []
    expected_nodes = set(node_names)
    if set(prometheus_by_node) != expected_nodes:
        problems.append("I/O queue metrics must cover the exact owned node inventory")
    nodes: dict[str, Any] = {}
    for node in node_names:
        payload = prometheus_by_node.get(node, "")
        parsed = (payload if isinstance(payload, list)
                  else parse_prometheus_samples(str(payload)))
        metrics = [sample for sample in parsed
                   if sample.get("name") == "scylla_io_queue_activations"
                   and sample.get("labels", {}).get("class") in ("main", "commitlog")]
        by_class_shard: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for sample in metrics:
            labels = sample.get("labels", {})
            key = (str(labels.get("class", "")), str(labels.get("shard", "")))
            by_class_shard.setdefault(key, []).append(sample)
        expected_pairs = {(category, str(shard))
                          for category in ("main", "commitlog") for shard in range(shards)}
        observed_pairs = set(by_class_shard)
        missing = sorted(expected_pairs - observed_pairs)
        unexpected = sorted(observed_pairs - expected_pairs)
        mismatches = []
        mountpoint_evidence: dict[str, list[dict[str, Any]]] = {}
        for pair, rows in by_class_shard.items():
            observed_mountpoints = sorted({str(row.get("labels", {}).get("mountpoint", ""))
                                           for row in rows})
            mountpoint_evidence[f"{pair[0]}/shard-{pair[1]}"] = [
                {"mountpoint": str(row.get("labels", {}).get("mountpoint", "")),
                 "value": row.get("value"), "labels": row.get("labels", {})}
                for row in rows]
            if expected_mountpoint not in observed_mountpoints:
                mismatches.append({"class": pair[0], "shard": pair[1],
                                   "mountpoints": observed_mountpoints,
                                   "missing_expected_mapping": True})
        node_valid = not missing and not unexpected and not mismatches
        if missing:
            problems.append(f"{node}: missing main/commitlog queue series {missing}")
        if unexpected:
            problems.append(f"{node}: unexpected I/O queue class/shard series {unexpected}")
        if mismatches:
            problems.append(f"{node}: scheduler queues are missing data mountpoint {expected_mountpoint}: {mismatches}")
        nodes[node] = {"metric_name": "scylla_io_queue_activations",
                       "expected_mountpoint": expected_mountpoint,
                       "expected_shards": list(range(shards)),
                       "series_count": len(metrics),
                       "observed_pairs": [list(pair) for pair in sorted(observed_pairs)],
                       "mountpoints_by_class_shard": {
                           f"{category}/shard-{shard}": sorted({
                               str(row.get("labels", {}).get("mountpoint", "")) for row in rows})
                           for (category, shard), rows in sorted(by_class_shard.items())},
                       "activation_evidence_by_class_shard": mountpoint_evidence,
                       "missing_series": [list(pair) for pair in missing],
                       "unexpected_series": [list(pair) for pair in unexpected],
                       "mismatches": mismatches, "valid": node_valid}
    return {"valid": not problems and set(nodes) == expected_nodes,
            "expected_nodes": list(node_names), "shards": shards,
            "source_evidence": SCYLLA_IO_PROPERTIES_SOURCE_EVIDENCE,
            "nodes": nodes, "problems": problems}


def attach_io_queue_validation(runtime_nodes: dict[str, dict[str, Any]],
                               queue_validation: dict[str, Any],
                               node_names: Sequence[str]) -> dict[str, dict[str, Any]]:
    """Attach each node's effective queue result in the shape consumed by the load gate."""
    validation_nodes = queue_validation.get("nodes", {})
    for node in node_names:
        queue_evidence = dict(runtime_nodes.get(node, {}).get("io_queue_mountpoints", {}))
        node_validation = validation_nodes.get(node, {})
        queue_evidence["valid"] = node_validation.get("valid") is True
        queue_evidence["validation"] = node_validation
        runtime_nodes.setdefault(node, {})["io_queue_mountpoints"] = queue_evidence
    return runtime_nodes


def missing_io_queue_series_only(evidence: dict[str, Any],
                                 node_names: Sequence[str], shards: int) -> bool:
    """Return true only when expected queue series are absent and all other gates pass."""
    queue = evidence.get("queue_mountpoint_validation", {})
    queue_nodes = queue.get("nodes", {})
    runtime_nodes = evidence.get("nodes", {})
    load_nodes = evidence.get("runtime_validation", {}).get("nodes", {})
    profile = evidence.get("profile", {})
    if (profile.get("enabled") is not True
            or queue.get("shards") != shards
            or queue.get("expected_nodes") != list(node_names)
            or set(queue_nodes) != set(node_names)
            or set(runtime_nodes) != set(node_names)
            or set(load_nodes) != set(node_names)):
        return False
    if any(queue_nodes[node].get("metric_name") != "scylla_io_queue_activations"
           or queue_nodes[node].get("expected_mountpoint") != "/var/lib/scylla"
           for node in node_names):
        return False

    allowed_top_problem = re.compile(
        r"^[^:]+: (?:runtime probe contains no main/commitlog I/O queue series|"
        r"missing main/commitlog queue series \[.*\]|"
        r"Scylla main/commitlog queue mountpoint metrics do not identify /var/lib/scylla)$")
    top_problems = evidence.get("problems", [])
    if any(not allowed_top_problem.fullmatch(str(problem)) for problem in top_problems):
        return False

    allowed_queue_problem = re.compile(r"^[^:]+: missing main/commitlog queue series \[.*\]$")
    if any(not allowed_queue_problem.fullmatch(str(problem))
           for problem in queue.get("problems", [])):
        return False
    allowed_load_problem = re.compile(
        r"^[^:]+: Scylla main/commitlog queue mountpoint metrics do not identify /var/lib/scylla$")
    load_problems = evidence.get("runtime_validation", {}).get("problems", [])
    if any(not allowed_load_problem.fullmatch(str(problem)) for problem in load_problems):
        return False

    missing_any = False
    expected_pairs = {(category, str(shard))
                      for category in ("main", "commitlog") for shard in range(shards)}
    for node in node_names:
        queue_node = queue_nodes[node]
        observed_pairs = {tuple(str(part) for part in pair)
                          for pair in queue_node.get("observed_pairs", [])}
        missing_pairs = {tuple(str(part) for part in pair)
                         for pair in queue_node.get("missing_series", [])}
        if (not missing_pairs and observed_pairs != expected_pairs
                or not missing_pairs <= expected_pairs
                or observed_pairs - expected_pairs
                or observed_pairs | missing_pairs != expected_pairs
                or queue_node.get("unexpected_series")
                or queue_node.get("mismatches")):
            return False
        if missing_pairs:
            missing_any = True

        runtime = runtime_nodes[node]
        runtime_errors = runtime.get("errors", [])
        if any(error != "runtime probe contains no main/commitlog I/O queue series"
               for error in runtime_errors):
            return False
        if runtime.get("prometheus_probe_error") is not None:
            return False
        raw_probe = runtime.get("io_queue_mountpoints", {}).get("raw_probe_file", {})
        if (not raw_probe.get("path") or not raw_probe.get("sha256_uncompressed")
                or raw_probe.get("bytes_uncompressed", 0) <= 0):
            return False

        load = load_nodes[node]
        for field_name in ("launch_flag_valid", "running_scylla_flag_valid",
                           "readonly_mount_valid", "content_hash_valid",
                           "startup_acceptance_valid", "device_stats_valid"):
            if load.get(field_name) is not True:
                return False
        if load.get("queue_mountpoints_valid") is not (not missing_pairs):
            return False
    return missing_any


def parse_device_stat_evidence(output: str, expected_paths: Sequence[str],
                               host_device_id: int | None) -> dict[str, Any]:
    observed: dict[str, int] = {}
    for line in output.splitlines():
        path, separator, value = line.rpartition(" ")
        if not separator:
            continue
        try:
            observed[path] = int(value)
        except ValueError:
            continue
    missing = [path for path in expected_paths if path not in observed]
    values = {observed[path] for path in expected_paths if path in observed}
    same_container_device = not missing and len(values) == 1
    host_match = (host_device_id is not None and same_container_device
                  and next(iter(values)) == host_device_id)
    return {"expected_paths": list(expected_paths), "observed_device_ids": observed,
            "missing_paths": missing, "host_owned_root_device_id": host_device_id,
            "container_paths_share_device": same_container_device,
            "container_device_matches_host_bind_source": host_match,
            "valid": host_match}


def scylla_relabel_yaml_key(source_text: str) -> str:
    """Extract the fixed release's relabel YAML root key from db/config.cc."""
    matches = re.findall(
        r'const\s+YAML::Node&\s+relabel_configs\s*=\s*yaml\["([a-z_]+)"\]\s*;',
        source_text)
    if len(matches) != 1:
        raise RunnerError(f"expected one pinned relabel YAML parser key, observed {len(matches)}")
    return matches[0]


def read_json_object(path: pathlib.Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RunnerError(f"cannot read JSON file {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise RunnerError(f"expected JSON object in {path}")
    return value


def write_ownership_marker(owned_root: pathlib.Path, nonce: str, trial_id: str,
                           node_count: int = 3) -> pathlib.Path:
    node_names = node_names_for(node_count)
    owned_root.mkdir(parents=True, exist_ok=True)
    if owned_root.is_symlink():
        raise RunnerError(f"owned-data path must not be a symlink: {owned_root}")
    marker = owned_root / ".ledger-wallet-owner.json"
    if marker.exists():
        raise RunnerError(f"refusing to reuse marker: {marker}")
    write_json(marker, {"format": 1, "nonce": nonce, "trial_id": trial_id,
                        "created_utc": _utc_now(), "owned_children": list(node_names),
                        "node_count": node_count})
    return marker


def validate_ownership_marker(trial_dir: pathlib.Path, owned_root: pathlib.Path,
                              nonce: str, trial_id: str,
                              node_count: int = 3) -> pathlib.Path:
    node_names = node_names_for(node_count)
    trial_real = trial_dir.resolve(strict=True)
    if owned_root.is_symlink():
        raise RunnerError(f"refusing symlinked owned-data root: {owned_root}")
    owned_real = owned_root.resolve(strict=True)
    if owned_real.parent != trial_real or owned_real.name != "owned-data":
        raise RunnerError(f"owned-data escaped its trial directory: {owned_real}")
    marker = owned_real / ".ledger-wallet-owner.json"
    content = read_json_object(marker)
    if content.get("nonce") != nonce or content.get("trial_id") != trial_id:
        raise RunnerError(f"ownership marker mismatch: {marker}")
    if content.get("owned_children") != list(node_names) or content.get("node_count", node_count) != node_count:
        raise RunnerError(f"ownership marker child list mismatch: {marker}")
    for node in node_names:
        child = owned_real / node
        if not child.is_dir() or child.is_symlink() or child.resolve(strict=True).parent != owned_real:
            raise RunnerError(f"owned data directory is missing or unsafe: {child}")
    return marker


_PROM_SAMPLE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{.*\})?\s+[-+0-9.eEInfNa]+(?:\s+\d+)?$")
_PROM_VALUE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})?\s+"
                         r"([-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?|NaN|[-+]?Inf)"
                         r"(?:\s+(\d+))?$")
_PROM_LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:\\.|[^"])*)"')
COMMITLOG_COUNTERS = (
    "scylla_commitlog_flush", "scylla_commitlog_bytes_written",
    "scylla_commitlog_cycle", "scylla_commitlog_alloc",
)
COMMITLOG_GAUGES = ("scylla_commitlog_pending_flushes",)


def parse_prometheus_metric_names(payload: str) -> set[str]:
    names = set()
    for line in payload.splitlines():
        if line and not line.startswith("#"):
            match = _PROM_SAMPLE.match(line.strip())
            if match:
                names.add(match.group(1))
    return names


def parse_prometheus_samples(payload: str) -> list[dict[str, Any]]:
    samples = []
    for line in payload.splitlines():
        if not line or line.startswith("#"):
            continue
        match = _PROM_VALUE.match(line.strip())
        if not match:
            continue
        name, raw_labels, raw_value, timestamp = match.groups()
        labels = {}
        for label in _PROM_LABEL.finditer(raw_labels or ""):
            labels[label.group(1)] = label.group(2).replace('\\"', '"').replace('\\\\', '\\')
        try:
            value = float(raw_value)
        except ValueError:
            continue
        samples.append({"name": name, "labels": labels, "value": value,
                        "exposition_timestamp": int(timestamp) if timestamp else None})
    return samples


def prometheus_window_metric_evidence(prom_dir: pathlib.Path, start_ns: int,
                                      end_ns: int,
                                      node_names: Sequence[str] = NODE_NAMES) -> dict[str, dict[str, Any]]:
    evidence: dict[str, dict[str, Any]] = {}
    marker = re.compile(r"^# runner_sample_unix_ns (\d+)\b")
    for node in node_names:
        path = prom_dir / f"{node}.prom.gz"
        names: set[str] = set()
        timestamps: list[int] = []
        current_timestamp: int | None = None
        current_names: set[str] = set()

        def flush_sample() -> None:
            if current_timestamp is not None and start_ns <= current_timestamp <= end_ns:
                timestamps.append(current_timestamp)
                names.update(current_names)

        try:
            with gzip.open(path, "rt", encoding="utf-8", errors="replace") as handle:
                for line in handle:
                    match = marker.match(line)
                    if match:
                        flush_sample()
                        current_timestamp = int(match.group(1))
                        current_names = set()
                    elif current_timestamp is not None and not line.startswith("#"):
                        current_names.update(parse_prometheus_metric_names(line))
            flush_sample()
            evidence[node] = {"sample_count": len(timestamps),
                              "sample_timestamps_unix_ns": timestamps,
                              "metric_names": sorted(names),
                              "raw_file": str(path)}
        except (OSError, EOFError, gzip.BadGzipFile) as exc:
            evidence[node] = {"sample_count": 0, "sample_timestamps_unix_ns": [],
                              "metric_names": [], "raw_file": str(path),
                              "error": str(exc)}
    return evidence


def metric_evidence(payload: str) -> dict[str, Any]:
    return metric_evidence_from_names(parse_prometheus_metric_names(payload))


def metric_evidence_from_names(metric_names: Iterable[str]) -> dict[str, Any]:
    names = sorted(set(metric_names))
    categories = {
        "reactor_cpu": ("reactor_utilization", "reactor_cpu"),
        "latency": ("coordinator_write_latency", "write_latency", "request_latency"),
        "cache": ("cache_memory", "cache_hit", "cache_requests"),
        "flush": ("memtable_flush", "flushes"),
        "compaction": ("compaction",),
        "commitlog_sync": ("commitlog_sync", "scylla_commitlog_flush"),
        "backlog": ("queue_length", "backlog", "pending_tasks", "pending_flushes"),
        "errors": ("write_errors", "read_errors", "request_errors", "dropped_mutations"),
        "timeouts": ("write_timeouts", "read_timeouts", "request_timeouts"),
        "network": ("transport_server", "network_bytes", "network_packets"),
    }
    found = {group: [name for name in names if any(p in name.lower() for p in patterns)]
             for group, patterns in categories.items()}
    return {"metric_names": names, "categories": found}


def _json_objects_in_text(text: str) -> list[dict[str, Any]]:
    decoder, objects = json.JSONDecoder(), []
    for index, char in enumerate(text):
        if char == "{":
            try:
                value, _ = decoder.raw_decode(text[index:])
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                objects.append(value)
    return objects


def parse_cql_json_rows(text: str) -> list[dict[str, Any]]:
    """Parse top-level JSON rows, skipping nested JSON objects such as replication maps."""
    decoder = json.JSONDecoder()
    rows: list[dict[str, Any]] = []
    cursor = 0
    while cursor < len(text):
        opening = text.find("{", cursor)
        if opening < 0:
            break
        try:
            value, end = decoder.raw_decode(text, opening)
        except json.JSONDecodeError:
            cursor = opening + 1
            continue
        if isinstance(value, dict):
            rows.append(value)
        cursor = max(end, opening + 1)
    return rows


def cqlsh_reports_zero_rows(text: str) -> bool:
    return any(line.strip().lower() == "(0 rows)" for line in text.splitlines())


def cql_readiness_version(exit_code: int, output: str) -> str | None:
    """Return the CQL-reported compatibility version for a successful JSON probe."""
    if exit_code != 0:
        return None
    rows = _json_objects_in_text(output)
    if len(rows) != 1:
        return None
    version = rows[0].get("release_version")
    if not isinstance(version, str) or not version.strip():
        return None
    return version.strip()


def parse_cqlsh_client_rows(output: str) -> list[dict[str, Any]]:
    rows = []
    # Each system.clients JSON object is one physical connection row. Its
    # client_options map is nested JSON and must not be counted as another row.
    for row in parse_cql_json_rows(output):
        options = row.get("client_options", row.get("CLIENT_OPTIONS", row))
        if isinstance(options, str):
            try:
                options = json.loads(options)
            except json.JSONDecodeError:
                options = None
        rows.append({"row": row, "client_options": options})
    return rows


def _option(options: Any, key: str) -> Any:
    if isinstance(options, dict):
        for candidate, value in options.items():
            if str(candidate).upper() == key.upper():
                return value
    return None


def audit_session_connections(rows_by_node: dict[str, Sequence[dict[str, Any]]],
                              session_id: str, workers: int | None = None, *,
                              expected_data_per_node: int | None = None,
                              target_data_connections: int | None = None,
                              shards: int | None = None,
                              per_shard_pool: int | None = None,
                              node_count: int = 3) -> dict[str, Any]:
    node_names = node_names_for(node_count)
    # Preserve the legacy helper call shape for existing callers/tests. New
    # runner paths provide the physical connection target independently from C.
    if expected_data_per_node is None or target_data_connections is None:
        if workers is None or workers <= 0 or workers % node_count:
            raise RunnerError("legacy connection audit requires workers divisible by node count")
        expected_data_per_node = workers // node_count
        target_data_connections = workers
    if (expected_data_per_node <= 0
            or target_data_connections != node_count * expected_data_per_node):
        raise RunnerError("connection audit requires per-node target and total target to agree with node count")
    nodes, problems = {}, []
    if set(rows_by_node) != set(node_names):
        problems.append(f"connection audit node inventory must be exactly {list(node_names)}, "
                        f"observed {sorted(rows_by_node)}")
    total_data = total_control = 0
    for node in node_names:
        data_rows, controls, unknown = [], 0, 0
        for item in rows_by_node.get(node, ()):
            options = item.get("client_options")
            if _option(options, "SESSION_ID") != session_id:
                continue
            if _option(options, "DRIVER_CONFIG") is not None:
                controls += 1
            elif isinstance(options, dict):
                data_rows.append(item.get("row", {}))
            else:
                unknown += 1
        total_data += len(data_rows)
        total_control += controls
        if len(data_rows) != expected_data_per_node:
            problems.append(f"{node}: expected {expected_data_per_node} data connections, observed {len(data_rows)}")
        if unknown:
            problems.append(f"{node}: {unknown} connections could not be classified")
        nodes[node] = {"expected_data": expected_data_per_node, "actual_data": len(data_rows),
                       "control_connections": controls, "unclassified_connections": unknown,
                       "data_rows": data_rows}
    if total_data != target_data_connections:
        problems.append(f"expected {target_data_connections} data connections total, observed {total_data}")
    return {"session_id": session_id, "workers": workers,
            "node_count": node_count,
            "shards": shards, "per_shard_pool": per_shard_pool,
            "expected_data_connections_per_node": expected_data_per_node,
            "target_data_connections": target_data_connections,
            "actual_data_connections": total_data, "control_connections": total_control,
            "nodes": nodes, "valid": not problems, "problems": problems}


def estimate_disk_bytes(users: int, requests_per_user: int,
                        replication_factor: int = 3,
                        node_count: int = 3) -> dict[str, int]:
    node_names_for(node_count)
    if isinstance(replication_factor, bool) or replication_factor not in (1, 2, 3):
        raise RunnerError("replication-factor must be 1, 2, or 3")
    if replication_factor > node_count:
        raise RunnerError(f"replication-factor {replication_factor} exceeds node-count {node_count}")
    total = users * requests_per_user
    logical = total * 1024
    replicated = logical * replication_factor
    commitlog = node_count * (1024 + 64) * 1024**2
    return {"total_requests": total, "estimated_logical_bytes": logical,
            "replication_factor": replication_factor,
            "node_count": node_count,
            "estimated_replicated_bytes": replicated, "estimated_compaction_bytes": replicated,
            "estimated_commitlog_bytes": commitlog,
            "required_with_reserve_bytes": replicated * 2 + commitlog + FORMAL_DISK_RESERVE_BYTES}


def is_formal_matrix(shards: Sequence[int], workers: Sequence[int], repeats: int,
                     users: int, requests_per_user: int,
                     per_shard_pool: int | None = None,
                     worker_routing: str = "round-robin",
                     replication_factor: int = 3,
                     read_cl: int | str = 3,
                     write_cl: int | str = 3,
                     node_count: int = 3) -> bool:
    try:
        read_name = _canonical_consistency(read_cl, "read-cl")
        write_name = _canonical_consistency(write_cl, "write-cl")
    except RunnerError:
        return False
    return (node_count == 3 and tuple(shards) == DEFAULT_SHARDS and tuple(workers) == DEFAULT_WORKERS
            and repeats == FORMAL_REPEATS and users == FORMAL_USERS
            and requests_per_user == FORMAL_REQUESTS_PER_USER
            and per_shard_pool is None and worker_routing == "round-robin"
            and replication_factor == 3 and read_name == "THREE" and write_name == "THREE")


def read_host_cpu_busy_pct() -> float | None:
    def read() -> tuple[int, int] | None:
        try:
            values = [int(v) for v in pathlib.Path("/proc/stat").read_text().splitlines()[0].split()[1:]]
            return sum(values), values[3] + (values[4] if len(values) > 4 else 0)
        except (OSError, IndexError, ValueError):
            return None
    before = read()
    time.sleep(0.2)
    after = read()
    if before is None or after is None or after[0] <= before[0]:
        return None
    return 100.0 * ((after[0] - before[0]) - (after[1] - before[1])) / (after[0] - before[0])


def _nearest_existing_path(path: pathlib.Path) -> pathlib.Path:
    current = path.expanduser().resolve()
    while not current.exists() and current != current.parent:
        current = current.parent
    return current


def backing_device_name(path: pathlib.Path = REPO_ROOT) -> str | None:
    target = _nearest_existing_path(path)
    result = _run(["findmnt", "-n", "-o", "SOURCE", "-T", str(target)], check=False)
    if result.returncode:
        return None
    source = (result.stdout or "").strip().splitlines()[0].split("[", 1)[0]
    return pathlib.Path(source).name if source.startswith("/dev/") else None


def read_device_busy_pct(device: str | None = None,
                         data_path: pathlib.Path = REPO_ROOT) -> float | None:
    target = device or backing_device_name(data_path)
    if not target:
        return None
    def read() -> int | None:
        try:
            for row in pathlib.Path("/proc/diskstats").read_text().splitlines():
                fields = row.split()
                if len(fields) >= 14 and fields[2] == target:
                    return int(fields[12])
        except (OSError, ValueError):
            pass
        return None
    before = read()
    time.sleep(0.2)
    after = read()
    if before is None or after is None:
        return None
    return max(0.0, min(100.0, (after - before) / 2.0))


def read_host_aio_counters(proc_root: pathlib.Path = pathlib.Path("/proc")) -> dict[str, Any]:
    values: dict[str, int | None] = {"aio_max_nr": None, "aio_nr": None}
    errors = {}
    for name, relative in (("aio_max_nr", "sys/fs/aio-max-nr"),
                           ("aio_nr", "sys/fs/aio-nr")):
        try:
            values[name] = int((proc_root / relative).read_text().strip())
        except (OSError, ValueError) as exc:
            errors[name] = f"{type(exc).__name__}: {exc}"
    available = (values["aio_max_nr"] - values["aio_nr"]
                 if values["aio_max_nr"] is not None and values["aio_nr"] is not None
                 else None)
    return {**values, "aio_available": available, "errors": errors}


def scylla_aio_profile_evidence() -> dict[str, Any]:
    return {"settings": SCYLLA_AIO_PROFILE,
            "source_evidence": SCYLLA_AIO_SOURCE_EVIDENCE}


def compute_aio_budget(shards: int, aio_max_nr: int | None,
                       aio_nr: int | None, node_count: int = 3) -> dict[str, Any]:
    per_shard_allocated = (
        SCYLLA_AIO_PROFILE["max_networking_io_control_blocks"]
        + STORAGE_IO_CONTROL_BLOCKS_PER_SHARD
        + PREEMPT_IO_CONTROL_BLOCKS_PER_SHARD)
    reserve_per_node = SCYLLA_AIO_PROFILE["reserve_io_control_blocks"]
    required_final_headroom = max(AIO_CONTROL_BLOCK_HEADROOM, reserve_per_node)
    try:
        node_names_for(node_count)
        topology_error = None
    except RunnerError as exc:
        topology_error = str(exc)
    required = (node_count * shards * per_shard_allocated + required_final_headroom
                if shards > 0 and topology_error is None else None)
    available = aio_max_nr - aio_nr if aio_max_nr is not None and aio_nr is not None else None
    if topology_error:
        error = topology_error
    elif aio_max_nr is None or aio_nr is None:
        error = "host aio-max-nr or aio-nr is unavailable"
    elif aio_max_nr < 0 or aio_nr < 0 or aio_nr > aio_max_nr:
        error = f"invalid host AIO counters: aio-max-nr={aio_max_nr}, aio-nr={aio_nr}"
    elif shards <= 0:
        error = f"invalid Scylla shard count: {shards}"
    elif available < required:
        error = (f"host AIO headroom {available} is below required {required} "
                 f"for {node_count} nodes at S={shards}")
    else:
        error = None
    return {
        "valid": error is None,
        "aio_max_nr": aio_max_nr, "aio_nr": aio_nr,
        "aio_available": available, "shards_per_node": shards,
        "node_count": node_count,
        "nodes": node_count,
        "max_networking_io_control_blocks_per_shard": SCYLLA_AIO_PROFILE[
            "max_networking_io_control_blocks"],
        "storage_io_control_blocks_per_shard": STORAGE_IO_CONTROL_BLOCKS_PER_SHARD,
        "preempt_io_control_blocks_per_shard": PREEMPT_IO_CONTROL_BLOCKS_PER_SHARD,
        "reserve_io_control_blocks_per_node": reserve_per_node,
        "per_shard_allocated_requirement": per_shard_allocated,
        "fixed_headroom": AIO_CONTROL_BLOCK_HEADROOM,
        "required_final_headroom": required_final_headroom,
        "required_available": required,
        "formula": (
            f"node_count*S*(max_networking_io_control_blocks_per_shard="
            f"{SCYLLA_AIO_PROFILE['max_networking_io_control_blocks']}+"
            f"storage_io_control_blocks_per_shard={STORAGE_IO_CONTROL_BLOCKS_PER_SHARD}+"
            f"preempt_io_control_blocks_per_shard={PREEMPT_IO_CONTROL_BLOCKS_PER_SHARD})+"
            f"max(fixed_headroom={AIO_CONTROL_BLOCK_HEADROOM},"
            f"reserve_io_control_blocks_per_node={reserve_per_node}); "
            "the final global headroom remains at least one process reserve, "
            "not reserve multiplied by S or node count"
        ),
        "error": error,
        "profile": scylla_aio_profile_evidence(),
    }


def parse_mode_defaults(mode: str) -> tuple[int, int]:
    return {"smoke": (SMOKE_USERS, SMOKE_REQUESTS_PER_USER),
            "pilot": (PILOT_USERS, PILOT_REQUESTS_PER_USER),
            "formal": (FORMAL_USERS, FORMAL_REQUESTS_PER_USER)}[mode]


def host_inventory(data_path: pathlib.Path = REPO_ROOT) -> dict[str, Any]:
    lscpu = _run(["lscpu", "-J"], check=False)
    try:
        lscpu_data: Any = json.loads(lscpu.stdout or "{}") if lscpu.returncode == 0 else None
    except json.JSONDecodeError:
        lscpu_data = {"raw": lscpu.stdout}
    memory: dict[str, int] = {}
    try:
        for line in pathlib.Path("/proc/meminfo").read_text().splitlines():
            key, _, value = line.partition(":")
            if key in ("MemTotal", "MemAvailable"):
                memory[key] = int(value.split()[0]) * 1024
    except (OSError, ValueError, IndexError):
        pass
    sibling_map = {}
    for cpu in range(os.cpu_count() or 0):
        path = pathlib.Path(f"/sys/devices/system/cpu/cpu{cpu}/topology/thread_siblings_list")
        try:
            sibling_map[str(cpu)] = path.read_text().strip()
        except OSError:
            sibling_map[str(cpu)] = "unavailable"
    try:
        measured_data_path = _nearest_existing_path(data_path)
        disk_available = shutil.disk_usage(measured_data_path).free
    except OSError:
        disk_available = None
    docker = _run(["docker", "version", "--format", "{{.Server.Version}}"], check=False)
    aio = read_host_aio_counters()
    return {
        "captured_utc": _utc_now(),
        "hostname": socket.gethostname(),
        "platform": platform.platform(),
        "python": sys.version,
        "lscpu_json": lscpu_data,
        "cpu_count_logical": os.cpu_count(),
        "cpu_sibling_map": sibling_map,
        "memory_total_bytes": memory.get("MemTotal"),
        "memory_available_bytes": memory.get("MemAvailable"),
        "disk_available_bytes": disk_available,
        "aio_max_nr": aio["aio_max_nr"],
        "aio_nr": aio["aio_nr"],
        "aio_available": aio["aio_available"],
        "aio_counter_errors": aio["errors"],
        "data_path": str(data_path.resolve()),
        "measured_data_path": str(measured_data_path) if "measured_data_path" in locals() else None,
        "backing_device": backing_device_name(data_path),
        "docker_version": (docker.stdout or "").strip(),
    }


def strict_formal_preflight(path: pathlib.Path, required_disk_bytes: int,
                            wait_seconds: int = 60,
                            data_path: pathlib.Path = REPO_ROOT) -> dict[str, Any]:
    started = time.monotonic()
    samples: list[dict[str, Any]] = []
    last_error: str | None = None
    consecutive_passes = 0
    while time.monotonic() - started <= wait_seconds:
        cpu_busy = read_host_cpu_busy_pct()
        disk_busy = read_device_busy_pct(data_path=data_path)
        inventory = host_inventory(data_path)
        sample = {
            "elapsed_seconds": round(time.monotonic() - started, 3),
            "host_cpu_busy_pct": cpu_busy,
            "device_busy_pct": disk_busy,
            "disk_available_bytes": inventory["disk_available_bytes"],
            "memory_available_bytes": inventory["memory_available_bytes"],
            "aio_max_nr": inventory["aio_max_nr"],
            "aio_nr": inventory["aio_nr"],
            "aio_available": inventory["aio_available"],
            "data_path": inventory["data_path"],
            "measured_data_path": inventory["measured_data_path"],
            "backing_device": inventory["backing_device"],
            "required_disk_bytes": required_disk_bytes,
        }
        samples.append(sample)
        if cpu_busy is None or disk_busy is None:
            last_error = "host CPU or single backing device counters unavailable"
        elif inventory["disk_available_bytes"] is None or inventory["disk_available_bytes"] < FORMAL_MIN_AVAILABLE_DISK_BYTES:
            last_error = "available disk is below 32 GiB"
        elif inventory["disk_available_bytes"] < required_disk_bytes:
            last_error = (f"available disk {inventory['disk_available_bytes']} is below estimated "
                          f"required capacity {required_disk_bytes} including reserve")
        elif inventory["memory_available_bytes"] is None or inventory["memory_available_bytes"] < FORMAL_MIN_AVAILABLE_MEMORY_BYTES:
            last_error = "available memory is below 32 GiB"
        elif cpu_busy > 10:
            last_error = f"host CPU busy is above 10% ({cpu_busy:.2f}%)"
        elif disk_busy > 5:
            last_error = f"backing device busy is above 5% ({disk_busy:.2f}%)"
        else:
            last_error = None
        if last_error and ("below estimated required capacity" in last_error
                           or "below 32 GiB" in last_error):
            result = {"valid": False, "error": last_error,
                      "required_disk_bytes": required_disk_bytes,
                      "consecutive_passing_samples": 0, "samples": samples,
                      "waited_seconds": round(time.monotonic() - started, 3)}
            write_json(path, result)
            raise RunnerError(result["error"])
        consecutive_passes = consecutive_passes + 1 if last_error is None else 0
        if consecutive_passes >= 3:
            result = {"valid": True, "required_disk_bytes": required_disk_bytes,
                      "consecutive_passing_samples": consecutive_passes, "samples": samples,
                      "waited_seconds": round(time.monotonic() - started, 3)}
            write_json(path, result)
            return result
        time.sleep(1)
    result = {"valid": False, "error": last_error or "preflight did not stabilize",
              "required_disk_bytes": required_disk_bytes,
              "consecutive_passing_samples": consecutive_passes,
              "samples": samples, "waited_seconds": round(time.monotonic() - started, 3)}
    write_json(path, result)
    raise RunnerError(result["error"])


def source_evidence(output_dir: pathlib.Path) -> dict[str, Any]:
    root = output_dir / "source-evidence"
    manifest: dict[str, Any] = {}
    metrics_relabel_parser: dict[str, Any] | None = None
    files = [(SCYLLA_SOURCE_REF, f) for f in SCYLLA_SOURCE_FILES]
    files.append((DRIVER_SOURCE_REF, DRIVER_SOURCE_FILE))
    for ref, rel in files:
        project = "scylladb/scylladb" if ref.startswith("scylla-") else "scylladb/scylla-rust-driver"
        url = f"https://raw.githubusercontent.com/{project}/{ref}/{rel}"
        destination = root / ref / rel
        destination.parent.mkdir(parents=True, exist_ok=True)
        try:
            with urllib.request.urlopen(url, timeout=30) as response:
                data = response.read()
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise RunnerError(f"cannot fetch pinned source evidence {url}: {exc}") from exc
        digest = hashlib.sha256(data).hexdigest()
        expected = EXPECTED_SOURCE_SHA256[rel]
        if digest != expected:
            raise RunnerError(f"pinned source hash mismatch for {rel}: expected {expected}, got {digest}")
        destination.write_bytes(data)
        manifest[f"{ref}/{rel}"] = {"url": url, "path": str(destination),
                                    "sha256": digest, "bytes": len(data)}
        if rel == "db/config.cc":
            parser_key = scylla_relabel_yaml_key(data.decode("utf-8", errors="replace"))
            if parser_key != METRICS_RELABEL_ROOT_KEY:
                raise RunnerError("metrics relabel YAML root key differs from pinned parser: "
                                  f"expected {METRICS_RELABEL_ROOT_KEY}, got {parser_key}")
            metrics_relabel_parser = {
                "source": f"{ref}/{rel}", "line": 1991,
                "expression": f'const YAML::Node& relabel_configs = yaml["{parser_key}"];',
                "parsed_yaml_key": parser_key,
            }
    for rel in SEASTAR_AIO_SOURCE_FILES:
        seastar_url = (f"https://raw.githubusercontent.com/scylladb/scylla-seastar/"
                       f"{SCYLLA_SEASTAR_COMMIT}/{rel}")
        seastar_destination = root / "scylla-seastar" / SCYLLA_SEASTAR_COMMIT / rel
        seastar_destination.parent.mkdir(parents=True, exist_ok=True)
        try:
            with urllib.request.urlopen(seastar_url, timeout=30) as response:
                seastar_data = response.read()
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise RunnerError(f"cannot fetch pinned Seastar source evidence {seastar_url}: {exc}") from exc
        seastar_digest = hashlib.sha256(seastar_data).hexdigest()
        expected_seastar_digest = EXPECTED_SEASTAR_AIO_SHA256[rel]
        if seastar_digest != expected_seastar_digest:
            raise RunnerError("pinned Seastar source hash mismatch for "
                              f"{rel}: expected {expected_seastar_digest}, got {seastar_digest}")
        seastar_destination.write_bytes(seastar_data)
        manifest[f"scylla-seastar/{SCYLLA_SEASTAR_COMMIT}/{rel}"] = {
            "url": seastar_url, "path": str(seastar_destination),
            "sha256": seastar_digest, "bytes": len(seastar_data),
            "submodule_commit": SCYLLA_SEASTAR_COMMIT,
        }
    result = {
        "captured_utc": _utc_now(),
        "files": manifest,
        "scylla_seastar_submodule_commit": SCYLLA_SEASTAR_COMMIT,
        "metrics_relabel_parser": metrics_relabel_parser,
        "aio_profile_source_evidence": SCYLLA_AIO_SOURCE_EVIDENCE,
        "scylla_source_commit": _git_remote_tag_commit(
            "https://github.com/scylladb/scylladb.git", SCYLLA_SOURCE_REF),
        "driver_source_commit": _git_remote_tag_commit(
            "https://github.com/scylladb/scylla-rust-driver.git", DRIVER_SOURCE_REF),
    }
    result["expected_scylla_source_commit"] = SCYLLA_SOURCE_COMMIT
    result["scylla_tag_commit_matches_expected"] = (
        result["scylla_source_commit"] == SCYLLA_SOURCE_COMMIT)
    write_json(root / "manifest.json", result)
    return result


def _source_fragment_evidence(text: str, fragments: dict[str, str]) -> dict[str, Any]:
    lines = text.splitlines()
    evidence: dict[str, Any] = {}
    for label, fragment in fragments.items():
        matching_lines = [index for index, line in enumerate(lines, 1) if fragment in line]
        evidence[label] = {"fragment": fragment, "matched": bool(matching_lines),
                           "line_numbers": matching_lines}
    return evidence


def validate_periodic_lwt_source_evidence(manifest: dict[str, Any],
                                          evidence_root: pathlib.Path) -> dict[str, Any]:
    """Recheck immutable source bytes and the force-sync evidence chain from disk."""
    evidence_root = evidence_root.resolve()
    errors: list[str] = []
    if manifest.get("source_commit") != PERIODIC_SYNC_LWT_SOURCE_COMMIT:
        errors.append("periodic LWT evidence does not name the immutable pinned Scylla commit")
    expected_commit_url = (f"https://github.com/scylladb/scylladb/commit/"
                           f"{PERIODIC_SYNC_LWT_SOURCE_COMMIT}")
    if manifest.get("source_commit_url") != expected_commit_url:
        errors.append("periodic LWT evidence source_commit_url is not the immutable commit URL")
    observed_files: dict[str, Any] = {}
    manifest_files = manifest.get("files")
    if not isinstance(manifest_files, dict):
        manifest_files = {}
        errors.append("periodic LWT evidence file manifest is missing")
    for relative_path, expected in PERIODIC_SYNC_LWT_SOURCE_FILES.items():
        entry = manifest_files.get(relative_path)
        if not isinstance(entry, dict):
            errors.append(f"periodic LWT evidence is missing {relative_path}")
            continue
        expected_url = (f"https://raw.githubusercontent.com/scylladb/scylladb/"
                        f"{PERIODIC_SYNC_LWT_SOURCE_COMMIT}/{relative_path}")
        if entry.get("url") != expected_url:
            errors.append(f"{relative_path} must use its immutable commit URL")
        try:
            path = pathlib.Path(str(entry.get("path", ""))).resolve(strict=True)
            path.relative_to(evidence_root)
            data = path.read_bytes()
            text = data.decode("utf-8")
        except (OSError, ValueError, UnicodeDecodeError) as exc:
            errors.append(f"cannot read immutable evidence file {relative_path}: {exc}")
            continue
        digest = hashlib.sha256(data).hexdigest()
        if entry.get("sha256") != digest or entry.get("bytes") != len(data):
            errors.append(f"{relative_path} recorded byte count or digest differs from saved content")
        fragments = _source_fragment_evidence(text, expected["evidence"])
        expected_fragment_valid = all(value["matched"] for value in fragments.values())
        observed_files[relative_path] = {
            "path": str(path), "url": entry.get("url"), "sha256": digest,
            "expected_sha256": expected["sha256"], "bytes": len(data),
            "evidence": fragments, "valid": digest == expected["sha256"] and expected_fragment_valid,
        }
        if digest != expected["sha256"]:
            errors.append(f"immutable source SHA-256 mismatch for {relative_path}")
        if not expected_fragment_valid:
            missing = [name for name, value in fragments.items() if not value["matched"]]
            errors.append(f"{relative_path} is missing required force-sync evidence: {missing}")
    docs = manifest.get("official_lwt_documentation")
    docs_valid = False
    if isinstance(docs, dict):
        expected_docs_path = evidence_root / "official-lwt-differences.html"
        try:
            docs_path = pathlib.Path(str(docs.get("path", ""))).resolve(strict=True)
            docs_path.relative_to(evidence_root)
            docs_data = docs_path.read_bytes()
            docs_text = docs_data.decode("utf-8")
            docs_digest = hashlib.sha256(docs_data).hexdigest()
            docs_valid = (docs.get("url") == PERIODIC_SYNC_LWT_DOC_URL
                          and docs_path == expected_docs_path.resolve()
                          and PERIODIC_SYNC_LWT_DOC_PHRASE in docs_text
                          and PERIODIC_SYNC_LWT_DOC_DEVICE_PHRASE in docs_text
                          and docs_digest == docs.get("sha256"))
            if not docs_valid:
                errors.append("official LWT documentation URL, content, or recorded hash is invalid")
        except (OSError, ValueError, UnicodeDecodeError) as exc:
            errors.append(f"cannot read official LWT documentation evidence: {exc}")
            docs_digest = None
    else:
        docs_digest = None
        errors.append("official LWT documentation evidence is missing")
    valid = (not errors and set(manifest_files) == set(PERIODIC_SYNC_LWT_SOURCE_FILES)
             and set(observed_files) == set(PERIODIC_SYNC_LWT_SOURCE_FILES)
             and docs_valid)
    return {"valid": valid, "source_commit": manifest.get("source_commit"),
            "expected_source_commit": PERIODIC_SYNC_LWT_SOURCE_COMMIT,
            "files": observed_files,
            "official_lwt_documentation": {
                "url": PERIODIC_SYNC_LWT_DOC_URL, "sha256": docs_digest,
                "required_phrases": [PERIODIC_SYNC_LWT_DOC_DEVICE_PHRASE,
                                     PERIODIC_SYNC_LWT_DOC_PHRASE],
                "valid": docs_valid},
            "interpretation": manifest.get("interpretation"), "errors": errors}


def capture_periodic_lwt_source_evidence(run_dir: pathlib.Path) -> dict[str, Any]:
    """Download and validate the immutable Scylla LWT force-sync path evidence."""
    root = run_dir / "periodic-lwt-source-evidence"
    source_root = root / PERIODIC_SYNC_LWT_SOURCE_COMMIT
    source_root.mkdir(parents=True, exist_ok=True)
    files: dict[str, Any] = {}
    for relative_path, expected in PERIODIC_SYNC_LWT_SOURCE_FILES.items():
        url = (f"https://raw.githubusercontent.com/scylladb/scylladb/"
               f"{PERIODIC_SYNC_LWT_SOURCE_COMMIT}/{relative_path}")
        try:
            with urllib.request.urlopen(url, timeout=30) as response:
                data = response.read()
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise RunnerError(f"cannot fetch immutable periodic LWT evidence {url}: {exc}") from exc
        digest = hashlib.sha256(data).hexdigest()
        destination = source_root / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(data)
        text = data.decode("utf-8", errors="replace")
        evidence = _source_fragment_evidence(text, expected["evidence"])
        files[relative_path] = {"url": url, "path": str(destination.resolve()),
                                "sha256": digest, "bytes": len(data), "evidence": evidence}

    docs_url = PERIODIC_SYNC_LWT_DOC_URL
    try:
        with urllib.request.urlopen(docs_url, timeout=30) as response:
            docs_data = response.read()
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise RunnerError(f"cannot fetch official LWT durability documentation {docs_url}: {exc}") from exc
    docs_path = root / "official-lwt-differences.html"
    docs_path.write_bytes(docs_data)
    docs_entry = {"url": docs_url, "path": str(docs_path.resolve()),
                  "sha256": hashlib.sha256(docs_data).hexdigest(),
                  "bytes": len(docs_data)}
    manifest: dict[str, Any] = {
        "captured_utc": _utc_now(),
        "source_commit": PERIODIC_SYNC_LWT_SOURCE_COMMIT,
        "source_commit_url": f"https://github.com/scylladb/scylladb/commit/{PERIODIC_SYNC_LWT_SOURCE_COMMIT}",
        "files": files,
        "official_lwt_documentation": docs_entry,
        "interpretation": {
            "periodic_ordinary_writes": (
                "Pinned config documentation says periodic syncs are acknowledged immediately; "
                "this runner does not interpret ordinary periodic writes as synchronous acknowledgements."),
            "lwt_force_sync_path": (
                "Pinned Scylla code marks system.paxos for commitlog sync, routes prepare/accept/learn "
                "through that path, and learn requests force_sync::yes for the committed mutation. "
                "The commitlog writer routes writer.sync through batch_cycle/sync even when periodic "
                "mode is configured; this does not establish one physical fsync per transaction or "
                "exclude coalescing. This benchmark disables tablets, so the evidence applies to "
                "the system.paxos route and does not claim behavior for tablet-backed LWT paths."),
            "official_documentation": (
                "The pinned 2026.1 manual states that all LWT writes use synchronous commitlog mode "
                "and reach the device before LWT completion, in all commitlog_sync modes."),
        },
    }
    validation = validate_periodic_lwt_source_evidence(manifest, root)
    manifest["validation"] = validation
    manifest["valid"] = validation["valid"]
    manifest_path = root / "manifest.json"
    write_json(manifest_path, manifest)
    if not validation["valid"]:
        raise RunnerError("periodic LWT source evidence failed closed: " + "; ".join(validation["errors"]))
    return {"valid": True, "manifest_path": str(manifest_path.resolve()),
            "manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
            "source_commit": PERIODIC_SYNC_LWT_SOURCE_COMMIT,
            "validation": validation}


def validate_periodic_lwt_evidence_reference(reference: dict[str, Any]) -> dict[str, Any]:
    manifest_path = pathlib.Path(str(reference.get("manifest_path", "")))
    try:
        manifest_bytes = manifest_path.read_bytes()
        manifest = json.loads(manifest_bytes)
    except (OSError, json.JSONDecodeError) as exc:
        return {"valid": False, "errors": [f"cannot reload periodic LWT source manifest: {exc}"]}
    digest = hashlib.sha256(manifest_bytes).hexdigest()
    if digest != reference.get("manifest_sha256"):
        return {"valid": False, "errors": ["periodic LWT source manifest SHA-256 changed"]}
    if not isinstance(manifest, dict) or manifest.get("valid") is not True:
        return {"valid": False, "errors": ["periodic LWT source manifest is not marked valid"]}
    saved_validation = manifest.get("validation")
    if not isinstance(saved_validation, dict) or saved_validation.get("valid") is not True:
        return {"valid": False, "errors": ["periodic LWT source manifest validation is not marked valid"]}
    validation = validate_periodic_lwt_source_evidence(manifest, manifest_path.parent)
    validation["manifest_sha256"] = digest
    if reference.get("source_commit") != PERIODIC_SYNC_LWT_SOURCE_COMMIT:
        validation["valid"] = False
        validation.setdefault("errors", []).append("periodic LWT evidence reference commit changed")
    return validation


def source_hashes() -> dict[str, str]:
    paths = [pathlib.Path(__file__), REPO_ROOT / "Cargo.toml", CRATE_DIR / "Cargo.toml",
             CRATE_DIR / "Cargo.lock", REPO_ROOT / "Cargo.lock",
             REPO_ROOT / "tests" / "test_scylla_wallet_runner.py"]
    paths.extend(path for path in (
        REPO_ROOT / "docs" / "01-31.raw-requirement-scylladb-wallet-benchmark.md",
        REPO_ROOT / "docs" / "01-32.development-design-scylladb-wallet-benchmark.md",
        REPO_ROOT / "docs" / "01-36.raw-requirement-scylladb-per-user-worker-benchmark.md",
        REPO_ROOT / "docs" / "01-37.development-design-scylladb-per-user-worker-benchmark.md",
        REPO_ROOT / "docs" / "01-39.raw-requirement-scylladb-rf1-cl1-benchmark.md",
        REPO_ROOT / "docs" / "01-40.development-design-scylladb-rf1-cl1-benchmark.md",
        REPO_ROOT / "docs" / "01-51.raw-requirement-scylladb-calibrated-single-node-shard-benchmark.md",
        REPO_ROOT / "docs" / "01-52.development-design-scylladb-calibrated-single-node-shard-benchmark.md",
        CRATE_DIR / "README.md") if path.is_file())
    for prefix in ("01-42.*", "01-43.*", "01-44.*"):
        paths.extend(sorted((REPO_ROOT / "docs").glob(prefix)))
    paths.extend(sorted((CRATE_DIR / "src").glob("*.rs")))
    paths.extend(sorted((CRATE_DIR / "tests").glob("*.rs")))
    paths.extend(sorted((CRATE_DIR / "sql").glob("*.sql")))
    hashes = {str(path.relative_to(REPO_ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
              for path in paths if path.is_file()}
    git = _run(["git", "status", "--porcelain=v1"], check=False, cwd=REPO_ROOT)
    hashes["git_worktree_dirty"] = "true" if (git.stdout or "").strip() else "false"
    hashes["git_status_porcelain"] = (git.stdout or "").strip()
    head = _run(["git", "rev-parse", "HEAD"], check=False, cwd=REPO_ROOT)
    hashes["git_head"] = (head.stdout or "").strip() or "unavailable"
    return hashes


def fetch_prometheus(ip: str, timeout: float = 5.0) -> str:
    with urllib.request.urlopen(f"http://{ip}:9180/metrics", timeout=timeout) as response:
        return response.read().decode("utf-8", errors="replace")


def _git_remote_tag_commit(repository: str, tag: str) -> str | None:
    result = _run(["git", "ls-remote", "--tags", repository,
                   f"refs/tags/{tag}", f"refs/tags/{tag}^{{}}"],
                  check=False, timeout=30)
    if result.returncode:
        return None
    refs = {}
    for line in (result.stdout or "").splitlines():
        parts = line.split()
        if len(parts) == 2:
            refs[parts[1]] = parts[0]
    return refs.get(f"refs/tags/{tag}^{{}}", refs.get(f"refs/tags/{tag}"))


def _scylla_tag_commit() -> str | None:
    return _git_remote_tag_commit("https://github.com/scylladb/scylladb.git", SCYLLA_SOURCE_REF)


class OwnedCluster:
    """One nonce-owned one- or three-node cluster and its exact storage paths."""

    def __init__(self, trial_dir: pathlib.Path, owned_root: pathlib.Path, nonce: str,
                 trial_id: str, shards: int, run_id: str,
                 memory_limit_bytes: int = NODE_CGROUP_BYTES,
                 node_count: int = 3,
                 io_properties_profile: dict[str, Any] | None = None,
                 commitlog_sync: str = "batch",
                 commitlog_sync_period_in_ms: int | None = None,
                 host_io_limits: dict[str, Any] | None = None) -> None:
        self.node_names = node_names_for(node_count)
        self.node_count = node_count
        self.trial_dir, self.owned_root = trial_dir, owned_root
        self.metrics_relabel_profile = write_metrics_relabel_profile(trial_dir)
        self.metrics_relabel_path = pathlib.Path(self.metrics_relabel_profile["config_path"])
        self.nonce, self.trial_id, self.shards, self.run_id = nonce, trial_id, shards, run_id
        self.memory_limit_bytes = memory_limit_bytes
        self.commitlog_sync = commitlog_sync
        self.commitlog_sync_period_in_ms = commitlog_sync_period_in_ms
        self.host_io_limits = host_io_limits
        self.io_properties_profile = io_properties_profile or {
            "enabled": False, "config_path": None, "container_path": None,
            "source_sha256": None, "copied_sha256": None,
        }
        self.network_name = f"lw-scylla-{nonce[:10]}-{trial_id[:24]}"
        self.cluster_name = f"ledger-wallet-{nonce[:8]}-{trial_id[:24]}"
        self.records: list[ContainerRecord] = []
        self.network_id: str | None = None
        self.network_subnet: ipaddress.IPv4Network | None = None
        self.cleanup_result = CleanupResult()
        self.recovered_containers: list[dict[str, Any]] = []
        self.resource_state_uncertain = False
        self.api_config_cache: dict[str, dict[str, Any]] = {}
        self.aio_budget: dict[str, Any] | None = None
        self.labels = {"ledger-wallet.owner": "scylla-wallet-runner",
                       "ledger-wallet.run-id": run_id, "ledger-wallet.trial-id": trial_id,
                       "ledger-wallet.nonce": nonce}

    def start(self, startup_timeout: int = STARTUP_WAIT_SECONDS) -> dict[str, Any]:
        self.trial_dir.mkdir(parents=True, exist_ok=True)
        self.owned_root.mkdir(parents=True, exist_ok=False)
        marker = write_ownership_marker(self.owned_root, self.nonce, self.trial_id,
                                       self.node_count)
        for node in self.node_names:
            (self.owned_root / node).mkdir()
        self._write_ownership_manifest()
        self._create_network()
        try:
            for node in self.node_names:
                record = self._create_node(node)
                _run(["docker", "start", record.container_id], timeout=120)
                record.state = "running"
                self._write_ownership_manifest()
                if node == self.node_names[0]:
                    self._wait_cql(record, startup_timeout)
            for record in self.records[1:]:
                self._wait_cql(record, startup_timeout)
            status = self._wait_cluster_un(startup_timeout)
            for record in self.records:
                resolve_scylla_host_pid(record)
            self._write_ownership_manifest()
            nodes = [asdict(self._refresh_record(record)) for record in self.records]
            evidence = {
                "run_id": self.run_id, "trial_id": self.trial_id, "nonce": self.nonce,
                "image": PINNED_REPO_DIGEST, "network": self.network_name,
                "network_id": self.network_id, "network_subnet": str(self.network_subnet),
                "cluster_name": self.cluster_name, "shards": self.shards,
                "node_count": self.node_count, "node_names": list(self.node_names),
                "cpuset": cpuset_for(self.shards, self.node_count),
                "memory_limit_bytes": self.memory_limit_bytes,
                "scylla_memory_bytes": NODE_MEMORY_BYTES, "marker": str(marker),
                "commitlog_sync": self.commitlog_sync,
                "commitlog_sync_period_in_ms": self.commitlog_sync_period_in_ms,
                "commitlog_use_o_dsync": False,
                "commitlog_total_space_in_mb": 1024,
                "commitlog_segment_size_in_mb": 64,
                "metrics_relabel_profile": self.metrics_relabel_profile,
                "io_properties_profile": self.io_properties_profile,
                "host_io_limits": self.host_io_limits,
                "commitlog_sync": self.commitlog_sync,
                "commitlog_sync_period_in_ms": self.commitlog_sync_period_in_ms,
                "aio_profile": scylla_aio_profile_evidence(), "aio_budget": self.aio_budget,
                "nodes": nodes, "nodetool_status": status,
            }
            write_json(self.trial_dir / "cluster.json", evidence)
            return evidence
        except BaseException:
            self.capture_all_logs()
            raise

    def endpoints(self) -> list[str]:
        if len(self.records) != self.node_count or tuple(r.node for r in self.records) != self.node_names:
            raise RunnerError(f"expected owned nodes {self.node_names}, have {[r.node for r in self.records]}")
        return [f"{record.ip}:9042" for record in self.records]

    def _write_ownership_manifest(self) -> None:
        write_json(self.trial_dir / "ownership-manifest.json", {
            "run_id": self.run_id, "trial_id": self.trial_id, "nonce": self.nonce,
            "owned_root": str(self.owned_root), "network_name": self.network_name,
            "node_count": self.node_count, "owned_children": list(self.node_names),
            "network_id": self.network_id,
            "labels": self.labels,
            "nodes": [asdict(record) for record in self.records],
            "recovered_containers": self.recovered_containers,
        })

    def _create_network(self) -> None:
        command = ["docker", "network", "create", "--driver", "bridge"]
        for key, value in self.labels.items():
            command.extend(["--label", f"{key}={value}"])
        command.append(self.network_name)
        try:
            self.network_id = (_run(command, timeout=60).stdout or "").strip()
        except RunnerError:
            # The daemon may have completed create after the client timed out.
            # Cleanup later resolves networks by the unique nonce labels.
            self.resource_state_uncertain = True
            raise
        self._write_ownership_manifest()
        inspect = _run(["docker", "network", "inspect", self.network_id])
        try:
            raw = json.loads(inspect.stdout)[0]["IPAM"]["Config"][0]["Subnet"]
            self.network_subnet = ipaddress.ip_network(raw, strict=False)
        except (KeyError, IndexError, TypeError, json.JSONDecodeError, ValueError) as exc:
            raise RunnerError(f"could not inspect owned network subnet: {exc}") from exc
        if not isinstance(self.network_subnet, ipaddress.IPv4Network) or self.network_subnet.num_addresses < 16:
            raise RunnerError(f"unusable owned network subnet: {self.network_subnet}")

    def _node_ip(self, index: int) -> str:
        assert self.network_subnet is not None
        address = self.network_subnet.network_address + 10 + index
        if address not in self.network_subnet or address == self.network_subnet.broadcast_address:
            raise RunnerError(f"no fixed node address available in {self.network_subnet}")
        return str(address)

    def _create_node(self, node: str) -> ContainerRecord:
        node_ip = self._node_ip(self.node_names.index(node))
        cpus = cpuset_for(self.shards, self.node_count)[node]
        name = f"lw-scylla-{self.nonce[:8]}-{self.trial_id[:12]}-{node}"
        command = [
            "docker", "create", "--name", name, "--pull=never",
            "--network", self.network_name, "--ip", node_ip,
            "--cpuset-cpus", cpus, "--memory", str(self.memory_limit_bytes),
            "--memory-swap", str(self.memory_limit_bytes),
            "--mount", f"type=bind,src={(self.owned_root / node).resolve()},dst=/var/lib/scylla",
            "--mount", (f"type=bind,src={self.metrics_relabel_path.resolve()},"
                        f"dst={METRICS_RELABEL_CONTAINER_PATH},readonly"),
            "--tmpfs", "/run:rw,nosuid,nodev,size=128m",
        ]
        command.extend(docker_io_limit_flags(self.host_io_limits))
        if self.io_properties_profile.get("enabled") is True:
            config_path = pathlib.Path(str(self.io_properties_profile.get("config_path", "")))
            expected_hash = self.io_properties_profile.get("source_sha256")
            if (not config_path.is_file() or config_path.is_symlink()
                    or hashlib.sha256(config_path.read_bytes()).hexdigest() != expected_hash):
                raise RunnerError("I/O properties snapshot is missing, symlinked, or hash-mismatched")
            command.extend(["--mount", (f"type=bind,src={config_path.resolve()},"
                                         f"dst={IO_PROPERTIES_CONTAINER_PATH},readonly")])
        for key, value in self.labels.items():
            command.extend(["--label", f"{key}={value}"])
        command.extend(["--label", f"ledger-wallet.node={node}", PINNED_REPO_DIGEST,
            "--developer-mode", "1", "--io-setup", "0", "--overprovisioned", "1",
            "--smp", str(self.shards), "--cpuset", cpus, "--thread-affinity", "1",
            "--memory", "8G",
            "--cluster-name", self.cluster_name, "--seeds", self._node_ip(0),
            "--listen-address", node_ip, "--rpc-address", node_ip,
            "--broadcast-address", node_ip, "--broadcast-rpc-address", node_ip,
            "--prometheus-address", "0.0.0.0", "--prometheus-port", "9180",
            "--commitlog-sync", self.commitlog_sync, "--commitlog-use-o-dsync", "false",
            "--commitlog-total-space-in-mb", "1024", "--commitlog-segment-size-in-mb", "64",
            "--relabel-config-file", METRICS_RELABEL_CONTAINER_PATH,
            "--max-networking-io-control-blocks",
            str(SCYLLA_AIO_PROFILE["max_networking_io_control_blocks"]),
            "--reserve-io-control-blocks",
            str(SCYLLA_AIO_PROFILE["reserve_io_control_blocks"]),
            "--reactor-backend", SCYLLA_AIO_PROFILE["reactor_backend"]])
        if self.commitlog_sync == "periodic":
            if self.commitlog_sync_period_in_ms != PERIODIC_SYNC_DEFAULT_PERIOD_MS:
                raise RunnerError("periodic commitlog launch requires explicit 10000 ms period")
            command.extend(["--commitlog-sync-period-in-ms",
                            str(self.commitlog_sync_period_in_ms)])
        elif self.commitlog_sync_period_in_ms is not None:
            raise RunnerError("commitlog sync period may only be set for periodic mode")
        if self.io_properties_profile.get("enabled") is True:
            command.extend(["--io-properties-file", IO_PROPERTIES_CONTAINER_PATH])
        try:
            container_id = (_run(command, timeout=120).stdout or "").strip()
        except RunnerError:
            # A daemon/client timeout can happen after Docker has created the
            # object. Recover it by its unpredictable exact name and verify all
            # ownership labels before adding it to the cleanup manifest.
            probe = _run(["docker", "inspect", name], check=False, timeout=10)
            if probe.returncode:
                self.resource_state_uncertain = True
                raise
            info = _decode_inspect(probe.stdout)
            labels = info.get("Config", {}).get("Labels") or {}
            expected_source = str((self.owned_root / node).resolve(strict=False))
            mounts = info.get("Mounts") or []
            exact_mount = any(str(pathlib.Path(mount.get("Source", "")).resolve(strict=False))
                              == expected_source
                              and mount.get("Destination") == "/var/lib/scylla"
                              for mount in mounts)
            if (not self._labels_match(labels) or labels.get("ledger-wallet.node") != node
                    or not exact_mount):
                raise RunnerError(f"created name {name} has mismatched ownership labels or data mount")
            container_id = str(info.get("Id", ""))
        if not container_id:
            raise RunnerError(f"docker create returned no ID for {node}")
        record = ContainerRecord(name, node, container_id, node_ip, cpus, state="created")
        self.records.append(record)
        self._write_ownership_manifest()
        return record

    def _refresh_record(self, record: ContainerRecord) -> ContainerRecord:
        info = _decode_inspect(_run(["docker", "inspect", record.container_id]).stdout)
        labels = info.get("Config", {}).get("Labels") or {}
        if any(labels.get(k) != v for k, v in self.labels.items()):
            raise RunnerError(f"container {record.name} ownership labels do not match")
        record.state = str(info.get("State", {}).get("Status", "unknown"))
        actual_ip = info.get("NetworkSettings", {}).get("Networks", {}).get(self.network_name, {}).get("IPAddress")
        if actual_ip != record.ip or record.state != "running":
            raise RunnerError(f"container {record.name} has unexpected IP or is not running")
        return record

    def _wait_cql(self, record: ContainerRecord, timeout: int) -> None:
        deadline, last = time.monotonic() + timeout, "no response"
        while time.monotonic() < deadline:
            info = _decode_inspect(_run(["docker", "inspect", record.container_id]).stdout)
            state = info.get("State", {})
            if not state.get("Running"):
                raise RunnerError(f"{record.node} stopped during startup: {state}")
            result = _run(["docker", "exec", record.container_id, "cqlsh", record.ip, "9042",
                           "--execute", "SELECT JSON release_version FROM system.local"],
                          check=False, timeout=10)
            version = cql_readiness_version(result.returncode, result.stdout or "")
            if version is not None:
                return
            last = ((result.stderr or "") + (result.stdout or ""))[-1000:]
            time.sleep(1)
        raise RunnerError(f"{record.node} did not become CQL-ready: {last}")

    def _wait_cluster_un(self, timeout: int) -> str:
        deadline, last = time.monotonic() + timeout, ""
        while time.monotonic() < deadline:
            result = _run(["docker", "exec", self.records[0].container_id, "nodetool", "status"],
                          check=False, timeout=15)
            last = (result.stdout or "") + (result.stderr or "")
            if result.returncode == 0 and len(re.findall(r"(?m)^\s*UN\s+\S+", last)) == self.node_count:
                return last
            time.sleep(1)
        raise RunnerError(f"cluster did not reach exactly {self.node_count} UN nodes: {last[-2500:]}")

    def cqlsh(self, record: ContainerRecord, query: str, consistency: str | None = "ONE") -> str:
        command = ["docker", "exec", record.container_id, "cqlsh", record.ip, "9042"]
        if consistency:
            query = f"CONSISTENCY {consistency}; {query}"
        command.extend(["--execute", query])
        return _run(command, timeout=30).stdout or ""

    def query_system_clients(self, record: ContainerRecord) -> list[dict[str, Any]]:
        return parse_cqlsh_client_rows(self.cqlsh(
            record, "SELECT JSON client_options FROM system.clients", consistency="ONE"))

    def verify_server_config(self) -> dict[str, Any]:
        nodes: dict[str, Any] = {}
        for record in self.records:
            version_output = self.cqlsh(record, "SELECT JSON release_version FROM system.local")
            version_objects = _json_objects_in_text(version_output)
            version = next((x.get("release_version") for x in version_objects if x.get("release_version")), None)
            prom = fetch_prometheus(record.ip)
            nodes[record.node] = {
                "ip": record.ip, "release_version": version,
                "release_version_output": version_output,
                "api_commitlog_config": self._api_config(record),
                "prometheus_metric_evidence": metric_evidence(prom),
                "prometheus_bytes": len(prom.encode()),
                "prometheus_raw_probe": prom,
            }
        versions = {node["release_version"] for node in nodes.values()}
        if len(versions) != 1 or None in versions:
            raise RunnerError(f"inconsistent/missing node release versions: {versions}")
        return {"image_repo_digest": PINNED_REPO_DIGEST,
                "image_inspect": self.image_inspect_evidence(),
                "nodes": nodes, "version_consistent": True}

    def verify_schema_agreement(self) -> dict[str, Any]:
        versions = {}
        outputs = {}
        for record in self.records:
            output = self.cqlsh(record, "SELECT JSON schema_version FROM system.local",
                                consistency="ONE")
            outputs[record.node] = output
            rows = _json_objects_in_text(output)
            versions[record.node] = next((row.get("schema_version") for row in rows
                                          if row.get("schema_version")), None)
        valid = len(set(versions.values())) == 1 and None not in versions.values()
        evidence = {"valid": valid, "consistency": "ONE", "schema_versions": versions,
                    "raw_outputs": outputs}
        write_json(self.trial_dir / "schema-agreement.json", evidence)
        if not valid:
            raise RunnerError(f"Scylla schema agreement is not established: {versions}")
        return evidence

    def image_inspect_evidence(self) -> dict[str, Any]:
        result = _run(["docker", "image", "inspect", PINNED_REPO_DIGEST])
        try:
            image = json.loads(result.stdout)[0]
        except (json.JSONDecodeError, IndexError, TypeError) as exc:
            raise RunnerError(f"cannot inspect pinned Scylla image: {exc}") from exc
        digests = image.get("RepoDigests") or []
        if PINNED_REPO_DIGEST not in digests:
            raise RunnerError(f"local image does not report pinned RepoDigest: {digests}")
        return {"reference": PINNED_REPO_DIGEST, "id": image.get("Id"),
                "repo_digests": digests, "repo_tags": image.get("RepoTags"),
                "architecture": image.get("Architecture"), "os": image.get("Os"),
                "created": image.get("Created"),
                "labels": image.get("Config", {}).get("Labels") or {}}

    def full_server_build_evidence(self) -> dict[str, Any]:
        nodes = {}
        for record in self.records:
            result = _run(["docker", "exec", record.container_id, "scylla", "--version"],
                          check=False, timeout=10)
            nodes[record.node] = {
                "scylla_version_exit_code": result.returncode,
                "scylla_version_stdout": result.stdout,
                "scylla_version_stderr": result.stderr,
                "version_has_tag_commit_prefix": (
                    "2026.1.14" in ((result.stdout or "") + (result.stderr or ""))
                    and SCYLLA_SOURCE_COMMIT[:12] in
                    ((result.stdout or "") + (result.stderr or ""))),
                "container_cmd": _decode_inspect(_run(["docker", "inspect", record.container_id]).stdout)
                    .get("Config", {}).get("Cmd"),
            }
        image = self.image_inspect_evidence()
        revision = image["labels"].get("org.opencontainers.image.revision")
        source_revision = _scylla_tag_commit()
        source_outputs_match = bool(nodes) and all(
            node["version_has_tag_commit_prefix"] and node["scylla_version_exit_code"] == 0
            for node in nodes.values())
        correspondence = ("verified_by_build_version" if source_outputs_match and
                           source_revision == SCYLLA_SOURCE_COMMIT else "unverified")
        return {"image": image, "nodes": nodes, "image_revision_label": revision,
                "source_tag_commit": source_revision,
                "expected_source_tag_commit": SCYLLA_SOURCE_COMMIT,
                "image_revision_label_matches_source": revision == SCYLLA_SOURCE_COMMIT,
                "source_version_correspondence": correspondence}

    def _api_config(self, record: ContainerRecord) -> dict[str, Any]:
        if record.container_id in self.api_config_cache:
            return self.api_config_cache[record.container_id]
        names = ("commitlog_sync", "commitlog_sync_period_in_ms", "commitlog_use_o_dsync",
                 "commitlog_sync_batch_window_in_ms", "commitlog_total_space_in_mb",
                 "commitlog_segment_size_in_mb")
        probes = []
        for name in names:
            url = f"http://127.0.0.1:10000/v2/config/{name}"
            result = _run(["docker", "exec", record.container_id, "curl", "--silent",
                           "--show-error", "--write-out", "\\n%{http_code}", url],
                          check=False, timeout=8)
            text = result.stdout or ""
            body, _, status_text = text.rpartition("\n")
            try:
                status = int(status_text.strip())
            except ValueError:
                status = None
            probes.append({"config_name": name, "url": url, "status": status,
                           "exit_code": result.returncode, "body": body,
                           "stderr": result.stderr})
        evidence = {"api_scope": "127.0.0.1 inside owned container",
                    "host_api_access": "not used; Admin REST binds to container loopback",
                    "probes": probes, "launch_config": {
            "commitlog_sync": self.commitlog_sync,
            "commitlog_sync_period_in_ms": self.commitlog_sync_period_in_ms,
            "commitlog_use_o_dsync": False,
            "commitlog_total_space_in_mb": 1024, "commitlog_segment_size_in_mb": 64,
            "commitlog_sync_batch_window_in_ms": "release default 10000; not overridden",
            "periodic_lwt_semantics": (
                "LWT force_sync path is source-verified; this does not mean one fsync per transaction"
                if self.commitlog_sync == "periodic" else None),
        }}
        self.api_config_cache[record.container_id] = evidence
        return evidence

    def capture_all_logs(self) -> None:
        log_dir = self.trial_dir / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        for record in self.records:
            result = _run(["docker", "logs", record.container_id], check=False, timeout=10)
            (log_dir / f"{record.node}.container.log").write_text(
                (result.stdout or "") + (result.stderr or ""), encoding="utf-8")

    def capture_and_validate_runtime_aio(self) -> dict[str, Any]:
        log_dir = self.trial_dir / "logs" / "runtime-aio"
        log_dir.mkdir(parents=True, exist_ok=True)
        records_by_node: dict[str, list[ContainerRecord]] = {}
        for record in self.records:
            records_by_node.setdefault(record.node, []).append(record)
        collection_errors: list[str] = []
        captured_logs: dict[str, str] = {}
        node_evidence: dict[str, Any] = {}
        for node in self.node_names:
            matches = records_by_node.get(node, [])
            if len(matches) != 1:
                message = f"expected one owned {node} container to collect startup log, found {len(matches)}"
                collection_errors.append(message)
                node_evidence[node] = {"valid": False, "fetch_error": message,
                                       "stdout_file": None, "stderr_file": None}
                continue
            record = matches[0]
            fetch_error = None
            exit_code = None
            stdout = ""
            stderr = ""
            try:
                result = _run(["docker", "logs", record.container_id],
                              check=False, timeout=10)
                exit_code = result.returncode
                stdout = result.stdout or ""
                stderr = result.stderr or ""
                if exit_code != 0:
                    fetch_error = f"docker logs exited with status {exit_code}"
            except RunnerError as exc:
                fetch_error = str(exc)
            stdout_bytes = stdout.encode("utf-8")
            stderr_bytes = stderr.encode("utf-8")
            stdout_path = log_dir / f"{node}.stdout.log"
            stderr_path = log_dir / f"{node}.stderr.log"
            stdout_path.write_bytes(stdout_bytes)
            stderr_path.write_bytes(stderr_bytes)
            captured_logs[node] = stdout + stderr
            if fetch_error:
                collection_errors.append(f"{node}: {fetch_error}")
            node_evidence[node] = {
                "container_id": record.container_id,
                "docker_logs_exit_code": exit_code,
                "fetch_error": fetch_error,
                "stdout_file": str(stdout_path.resolve()),
                "stdout_sha256": hashlib.sha256(stdout_bytes).hexdigest(),
                "stdout_bytes": len(stdout_bytes),
                "stderr_file": str(stderr_path.resolve()),
                "stderr_sha256": hashlib.sha256(stderr_bytes).hexdigest(),
                "stderr_bytes": len(stderr_bytes),
                "combined_sha256": hashlib.sha256(stdout_bytes + stderr_bytes).hexdigest(),
                "valid": fetch_error is None,
            }
        validation = validate_runtime_aio_startup_logs(
            captured_logs, self.node_names,
            io_properties_enabled=self.io_properties_profile.get("enabled") is True)
        problems = [*validation["problems"], *collection_errors]
        evidence = {
            "valid": validation["valid"] and not collection_errors,
            "collected_utc": _utc_now(),
            "container_log_command": "docker logs <exact owned container id>",
            "logs_directory": str(log_dir.resolve()),
            "nodes": node_evidence,
            "startup_validation": validation,
            "collection_errors": collection_errors,
            "problems": problems,
        }
        write_json(self.trial_dir / "runtime-aio-validation.json", evidence)
        return evidence

    def capture_io_properties_runtime_evidence(
            self, launch_config: dict[str, dict[str, Any]],
            runtime_aio_evidence: dict[str, Any], *,
            defer_missing_series_until_ready: bool = False) -> dict[str, Any]:
        profile_enabled = self.io_properties_profile.get("enabled") is True
        if defer_missing_series_until_ready and (
                not profile_enabled or self.commitlog_sync != "periodic"):
            raise RunnerError(
                "deferred I/O queue readiness is only allowed for periodic mode with an enabled properties profile")
        deferred = defer_missing_series_until_ready and profile_enabled
        started = time.monotonic()
        deadline = started + (PERIODIC_IO_QUEUE_READINESS_TIMEOUT_SECONDS if deferred else 0)
        attempts: list[dict[str, Any]] = []
        attempt_number = 0
        final: dict[str, Any] = {}
        readiness_status = "single_capture"
        while True:
            attempt_number += 1
            final = self._capture_io_properties_runtime_evidence_once(
                launch_config, runtime_aio_evidence, attempt_number)
            retryable = missing_io_queue_series_only(final, self.node_names, self.shards)
            node_evidence = final.get("nodes", {})
            queue_validation = final.get("queue_mountpoint_validation", {}).get("nodes", {})
            attempts.append({
                "attempt": attempt_number,
                "captured_utc": _utc_now(),
                "valid": final.get("valid") is True,
                "retryable_missing_series_only": retryable,
                "nodes": {
                    node: {
                        "missing_series": queue_validation.get(node, {}).get("missing_series", []),
                        "unexpected_series": queue_validation.get(node, {}).get("unexpected_series", []),
                        "mismatches": queue_validation.get(node, {}).get("mismatches", []),
                        "raw_probe_file": node_evidence.get(node, {}).get(
                            "io_queue_mountpoints", {}).get("raw_probe_file"),
                    }
                    for node in self.node_names
                },
                "problems": list(final.get("problems", [])),
            })
            progress_status = ("ready" if final.get("valid") is True else
                               "single_capture_invalid" if not deferred else
                               "polling" if retryable else "immediate_validation_failure")
            progress_readiness = {
                "enabled": deferred, "status": progress_status,
                "after_cli_phase": "ready_to_measure" if deferred else None,
                "max_wait_seconds": (PERIODIC_IO_QUEUE_READINESS_TIMEOUT_SECONDS if deferred else 0),
                "poll_interval_seconds": (PERIODIC_IO_QUEUE_READINESS_POLL_SECONDS if deferred else None),
                "attempt_count": attempt_number,
                "elapsed_seconds": round(max(0.0, time.monotonic() - started), 6),
                "valid": final.get("valid") is True,
                "attempts": list(attempts),
            }
            write_json(self.trial_dir / "io-properties-runtime-readiness-progress.json",
                       {**final, "series_readiness": progress_readiness})
            if final.get("valid") is True:
                readiness_status = "ready" if deferred else "single_capture"
                break
            if not deferred:
                readiness_status = "single_capture_invalid"
                break
            if not retryable:
                readiness_status = "immediate_validation_failure"
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                readiness_status = "timeout"
                break
            time.sleep(min(PERIODIC_IO_QUEUE_READINESS_POLL_SECONDS, remaining))

        elapsed = max(0.0, time.monotonic() - started)
        readiness = {
            "enabled": deferred,
            "status": readiness_status,
            "after_cli_phase": "ready_to_measure" if deferred else None,
            "max_wait_seconds": (PERIODIC_IO_QUEUE_READINESS_TIMEOUT_SECONDS if deferred else 0),
            "poll_interval_seconds": (PERIODIC_IO_QUEUE_READINESS_POLL_SECONDS if deferred else None),
            "attempt_count": attempt_number,
            "elapsed_seconds": round(elapsed, 6),
            "valid": final.get("valid") is True,
            "attempts": attempts,
        }
        final["series_readiness"] = readiness
        if readiness_status == "timeout":
            final.setdefault("problems", []).append(
                "expected Scylla I/O queue series did not appear before the ready gate deadline")
        if readiness_status == "immediate_validation_failure":
            final.setdefault("problems", []).append(
                "I/O properties validation failed on a non-series condition; readiness polling stopped")
        final["valid"] = final.get("valid") is True and readiness["valid"] is True
        write_json(self.trial_dir / "io-properties-runtime-validation.json", final)
        return final

    def _capture_io_properties_runtime_evidence_once(
            self, launch_config: dict[str, dict[str, Any]],
            runtime_aio_evidence: dict[str, Any], attempt_number: int) -> dict[str, Any]:
        profile = self.io_properties_profile
        if profile.get("enabled") is not True:
            evidence = {
                "valid": True, "status": "not_requested_legacy_profile",
                "node_count": self.node_count, "expected_nodes": list(self.node_names),
                "source_evidence": SCYLLA_IO_PROPERTIES_SOURCE_EVIDENCE,
                "runtime_validation": {"status": "not_requested"},
            }
            write_json(self.trial_dir / "io-properties-runtime-validation.json", evidence)
            return evidence

        output_dir = self.trial_dir / "io-properties-runtime"
        if attempt_number > 1:
            output_dir = output_dir / f"attempt-{attempt_number:03d}"
        output_dir.mkdir(parents=True, exist_ok=True)
        runtime_nodes: dict[str, dict[str, Any]] = {}
        problems: list[str] = []
        startup_logs: dict[str, str] = {}
        for node in self.node_names:
            record_matches = [record for record in self.records if record.node == node]
            node_problems: list[str] = []
            if len(record_matches) != 1:
                message = f"expected one owned {node} container, observed {len(record_matches)}"
                problems.append(message)
                runtime_nodes[node] = {"valid": False, "errors": [message]}
                startup_logs[node] = ""
                continue
            record = record_matches[0]
            argv = None
            argv_error = None
            try:
                if record.host_pid is None:
                    resolve_scylla_host_pid(record)
                argv = read_process_argv(record.host_pid)
            except RunnerError as exc:
                argv_error = str(exc)
                node_problems.append(argv_error)

            sha_result = _run(["docker", "exec", record.container_id,
                               "sha256sum", IO_PROPERTIES_CONTAINER_PATH],
                              check=False, timeout=10)
            sha_match = re.match(r"\s*([0-9a-fA-F]{64})\s+",
                                 sha_result.stdout or "") if sha_result.returncode == 0 else None
            mounted_sha = sha_match.group(1).lower() if sha_match else None
            if mounted_sha != profile.get("source_sha256"):
                node_problems.append("mounted file SHA-256 differs from input snapshot")

            stat_paths = ("/var/lib/scylla", "/var/lib/scylla/data",
                          "/var/lib/scylla/commitlog")
            stat_result = _run(["docker", "exec", record.container_id, "stat", "-c", "%n %d",
                                *stat_paths], check=False, timeout=10)
            try:
                host_device_id = os.stat(self.owned_root / node).st_dev
            except OSError:
                host_device_id = None
            device_stats = parse_device_stat_evidence(
                stat_result.stdout or "" if stat_result.returncode == 0 else "",
                stat_paths, host_device_id)
            device_stats.update({"exit_code": stat_result.returncode,
                                 "stderr": stat_result.stderr or ""})
            if not device_stats["valid"]:
                node_problems.append("container data/commitlog device mapping is not verified")

            raw_prometheus = ""
            prom_error = None
            try:
                raw_prometheus = fetch_prometheus(record.ip, timeout=5.0)
                raw_bytes = raw_prometheus.encode("utf-8")
                prom_path = output_dir / f"{node}.prom.gz"
                with gzip.open(prom_path, "wb") as handle:
                    handle.write(raw_bytes)
                prom_file = {"path": str(prom_path.resolve()),
                             "sha256_uncompressed": hashlib.sha256(raw_bytes).hexdigest(),
                             "bytes_uncompressed": len(raw_bytes)}
            except Exception as exc:
                prom_error = f"{type(exc).__name__}: {exc}"
                prom_file = {"path": None, "sha256_uncompressed": None,
                             "bytes_uncompressed": 0}
                node_problems.append(f"runtime Prometheus probe failed: {prom_error}")
            queue_metrics = [sample for sample in parse_prometheus_samples(raw_prometheus)
                             if sample.get("name") == "scylla_io_queue_activations"
                             and sample.get("labels", {}).get("class") in ("main", "commitlog")]
            queue_evidence = {
                "metric_name": "scylla_io_queue_activations",
                "raw_probe_file": prom_file,
                "series": queue_metrics,
            }
            if not queue_metrics:
                node_problems.append("runtime probe contains no main/commitlog I/O queue series")

            aio_node = runtime_aio_evidence.get("nodes", {}).get(node, {})
            stdout_path, stderr_path = aio_node.get("stdout_file"), aio_node.get("stderr_file")
            try:
                startup_stdout = pathlib.Path(stdout_path).read_text(encoding="utf-8") if stdout_path else ""
                startup_stderr = pathlib.Path(stderr_path).read_text(encoding="utf-8") if stderr_path else ""
            except OSError as exc:
                startup_stdout = startup_stderr = ""
                node_problems.append(f"cannot load captured startup logs: {exc}")
            startup_logs[node] = startup_stdout + startup_stderr
            runtime_nodes[node] = {
                "valid": not node_problems,
                "scylla_host_pid": record.host_pid,
                "scylla_argv": argv,
                "argv_error": argv_error,
                "container_sha256_exit_code": sha_result.returncode,
                "container_sha256_stdout": sha_result.stdout or "",
                "container_sha256_stderr": sha_result.stderr or "",
                "container_file_sha256": mounted_sha,
                "device_stats": device_stats,
                "io_queue_mountpoints": queue_evidence,
                "prometheus_probe_error": prom_error,
                "startup_log_files": {"stdout": stdout_path, "stderr": stderr_path},
                "errors": node_problems,
            }
            problems.extend(f"{node}: {problem}" for problem in node_problems)

        queue_validation = validate_io_queue_mountpoints(
            {node: runtime_nodes.get(node, {}).get("io_queue_mountpoints", {}).get("series", [])
             for node in self.node_names}, self.node_names, self.shards)
        attach_io_queue_validation(runtime_nodes, queue_validation, self.node_names)
        validation = validate_io_properties_load(
            profile, launch_config, startup_logs, self.node_names,
            runtime_nodes=runtime_nodes, shards=self.shards)
        problems.extend(validation["problems"])
        evidence = {
            "valid": not problems and queue_validation["valid"] and validation["valid"],
            "status": "effective_properties_verified" if not problems else "invalid",
            "node_count": self.node_count, "expected_nodes": list(self.node_names),
            "profile": {key: value for key, value in profile.items() if key != "content"},
            "source_evidence": SCYLLA_IO_PROPERTIES_SOURCE_EVIDENCE,
            "queue_mountpoint_validation": queue_validation,
            "runtime_validation": validation,
            "nodes": runtime_nodes,
            "config_surface_note": (
                "Effective device mapping is checked from Scylla Prometheus queue labels; "
                "the runner does not infer loaded properties from warning absence."),
            "problems": problems,
        }
        return evidence

    def stop_and_cleanup(self) -> CleanupResult:
        result = self.cleanup_result
        result.status = "running"
        self.capture_all_logs()
        network_inventory_safe, network_error = self._discover_owned_network()
        if network_error:
            result.errors.append(network_error)
        owned_ids, safe_inventory, inventory_error = self._discover_owned_containers()
        if inventory_error:
            result.errors.append(inventory_error)
        stopped: set[str] = set()
        for container_id, info in reversed(list(owned_ids.items())):
            try:
                labels = info.get("Config", {}).get("Labels") or {}
                if not self._labels_match(labels):
                    raise RunnerError(f"refusing to stop container with mismatched ownership labels: {container_id}")
                _run(["docker", "stop", "--time", str(SHUTDOWN_WAIT_SECONDS), container_id],
                     check=False, timeout=SHUTDOWN_WAIT_SECONDS + 10)
                info = _decode_inspect(_run(["docker", "inspect", container_id]).stdout)
                labels = info.get("Config", {}).get("Labels") or {}
                if not self._labels_match(labels):
                    raise RunnerError(f"container labels changed while stopping: {container_id}")
                state = info.get("State")
                if not isinstance(state, dict) or state.get("Running") is not False:
                    raise RunnerError(
                        f"container is not confirmed stopped after stop: {container_id}; "
                        f"State.Running={state.get('Running') if isinstance(state, dict) else None!r}")
                node = labels.get("ledger-wallet.node")
                if node not in self.node_names:
                    raise RunnerError(f"stopped owned container has invalid node label: {node!r}")
                exit_code = state.get("ExitCode")
                evidence_path = self.trial_dir / "cleanup-post-stop" / f"{node}.inspect.json"
                evidence_path.parent.mkdir(parents=True, exist_ok=True)
                write_json(evidence_path, {
                    "container_id": container_id, "node": node,
                    "exit_code": exit_code, "finished_at": state.get("FinishedAt"),
                    "oom_killed": state.get("OOMKilled"),
                    "exit_code_class": ("exit_137" if exit_code == 137 else
                                        "exit_0" if exit_code == 0 else "other_or_unavailable"),
                    "labels": labels, "mounts": info.get("Mounts"),
                    "post_stop_inspect": info,
                })
                result.post_stop_inspection_files.append(str(evidence_path))
                stopped.add(container_id)
            except Exception as exc:
                result.errors.append(f"stop/inspect {container_id}: {exc}")
        result.containers_stopped = (
            safe_inventory and network_inventory_safe and not inventory_error and not network_error
            and len(stopped) == len(owned_ids)
        )
        if not result.containers_stopped:
            result.status = "retained_data_container_stop_unconfirmed"
            result.retained_data = [str(self.owned_root / node) for node in self.node_names]
            result.errors.append("data retained because every container using owned mounts was not proven stopped")
            self._write_cleanup_audit()
            return result
        for container_id in reversed(list(owned_ids)):
            try:
                info = _decode_inspect(_run(["docker", "inspect", container_id]).stdout)
                labels = info.get("Config", {}).get("Labels") or {}
                if not self._labels_match(labels):
                    raise RunnerError(f"refusing to remove foreign container {container_id}")
                _run(["docker", "rm", container_id])
                result.containers_removed.append(str(labels.get("ledger-wallet.node", container_id)))
            except Exception as exc:
                result.errors.append(f"remove container {container_id}: {exc}")
        if len(result.containers_removed) != len(owned_ids):
            result.status = "partial_container_removal_data_retained"
            result.retained_data = [str(self.owned_root / node) for node in self.node_names]
            self._write_cleanup_audit()
            return result
        remaining, remaining_safe, remaining_error = self._discover_owned_containers()
        if remaining or not remaining_safe or remaining_error:
            result.status = "retained_data_container_remains"
            result.retained_data = [str(self.owned_root / node) for node in self.node_names]
            result.errors.append(remaining_error or f"containers still reference owned mounts: {sorted(remaining)}")
            self._write_cleanup_audit()
            return result
        if self.network_id:
            try:
                network = json.loads(_run(["docker", "network", "inspect", self.network_id]).stdout)[0]
                labels = network.get("Labels") or {}
                if any(labels.get(k) != v for k, v in self.labels.items()):
                    raise RunnerError("network ownership labels do not match")
                _run(["docker", "network", "rm", self.network_id])
                result.network_removed = True
            except Exception as exc:
                result.errors.append(f"remove network: {exc}")
                result.retained_data = [str(self.owned_root / node) for node in self.node_names]
                result.status = "cleanup_error_data_retained"
                self._write_cleanup_audit()
                return result
        if self.owned_root.exists():
            try:
                validate_ownership_marker(self.trial_dir, self.owned_root, self.nonce,
                                          self.trial_id, self.node_count)
                self._remove_owned_data()
            except Exception as exc:
                result.errors.append(f"remove owned data: {exc}")
                result.retained_data = [str(self.owned_root)]
        result.status = ("clean" if not result.errors and
                         (self.network_id is None or result.network_removed) else "cleanup_error")
        self._write_cleanup_audit()
        return result

    def _discover_owned_network(self) -> tuple[bool, str | None]:
        listed = _run(["docker", "network", "ls", "-q"], check=False, timeout=30)
        if listed.returncode:
            return False, f"cannot inventory Docker networks: {listed.stderr}"
        found = []
        for network_id in (listed.stdout or "").split():
            inspected = _run(["docker", "network", "inspect", network_id],
                             check=False, timeout=15)
            if inspected.returncode:
                return False, f"cannot inspect Docker network {network_id}: {inspected.stderr}"
            try:
                network = json.loads(inspected.stdout)[0]
            except (json.JSONDecodeError, IndexError, TypeError) as exc:
                return False, f"invalid network inspection for {network_id}: {exc}"
            canonical_id = network.get("Id")
            if not isinstance(canonical_id, str) or not canonical_id.strip():
                return False, f"network inspection for {network_id} has no canonical Id"
            canonical_id = canonical_id.strip()
            listed_id = network_id.strip()
            if canonical_id != listed_id and not canonical_id.startswith(listed_id):
                return False, f"inspected network Id does not resolve listed id {network_id}"
            labels = network.get("Labels") or {}
            if labels.get("ledger-wallet.nonce") == self.nonce:
                if any(labels.get(key) != value for key, value in self.labels.items()):
                    return False, f"nonce-matched network {network_id} has mismatched ownership labels"
                found.append(canonical_id)
        if len(found) > 1:
            return False, f"multiple nonce-owned networks found: {found}"
        if found:
            if (self.network_id and self.network_id != found[0]
                    and not found[0].startswith(self.network_id)):
                return False, "manifest network id differs from nonce-owned network"
            self.network_id = found[0]
        self._write_ownership_manifest()
        return True, None

    def _labels_match(self, labels: dict[str, Any]) -> bool:
        return all(labels.get(key) == value for key, value in self.labels.items())

    def _discover_owned_containers(self) -> tuple[dict[str, dict[str, Any]], bool, str | None]:
        """Inventory every container, including creates interrupted before start."""
        ids_result = _run(["docker", "ps", "-aq"], check=False, timeout=30)
        if ids_result.returncode:
            return {}, False, f"cannot inventory Docker containers: {ids_result.stderr}"
        expected_mounts = {
            str((self.owned_root / node).resolve(strict=False)): node for node in self.node_names
        }
        owned: dict[str, dict[str, Any]] = {}
        unsafe_mount_users: list[str] = []
        inventory_safe = True
        for container_id in (ids_result.stdout or "").split():
            inspected = _run(["docker", "inspect", container_id], check=False, timeout=15)
            if inspected.returncode:
                inventory_safe = False
                unsafe_mount_users.append(f"inspect failed for {container_id}")
                continue
            info = _decode_inspect(inspected.stdout)
            mounts = info.get("Mounts") or []
            used_nodes = [expected_mounts.get(str(pathlib.Path(m.get("Source", "")).resolve(strict=False)))
                          for m in mounts]
            used_nodes = [node for node in used_nodes if node]
            labels = info.get("Config", {}).get("Labels") or {}
            has_trial_nonce = labels.get("ledger-wallet.nonce") == self.nonce
            if has_trial_nonce:
                if not self._labels_match(labels) or len(used_nodes) != 1:
                    inventory_safe = False
                    unsafe_mount_users.append(f"nonce-labelled container {container_id} has invalid labels or mounts")
                    continue
                label_node = labels.get("ledger-wallet.node")
                if label_node not in self.node_names or label_node != used_nodes[0]:
                    inventory_safe = False
                    unsafe_mount_users.append(f"nonce-labelled container {container_id} has wrong node/mount")
                    continue
                owned[container_id] = info
            elif used_nodes:
                inventory_safe = False
                unsafe_mount_users.append(f"foreign container {container_id} uses owned mount(s) {used_nodes}")
        expected_nodes = {str(info.get("Config", {}).get("Labels", {}).get("ledger-wallet.node"))
                          for info in owned.values()}
        if len(expected_nodes) != len(owned):
            inventory_safe = False
            unsafe_mount_users.append("duplicate owned node labels detected")
        if unsafe_mount_users:
            return owned, False, "; ".join(unsafe_mount_users)
        self.recovered_containers = [
            {"container_id": container_id,
             "name": str(info.get("Name", "")).lstrip("/"),
             "node": str((info.get("Config", {}).get("Labels") or {}).get("ledger-wallet.node")),
             "state": info.get("State", {}).get("Status"),
             "mounts": info.get("Mounts", [])}
            for container_id, info in owned.items()]
        self._write_ownership_manifest()
        return owned, inventory_safe, None

    def _remove_owned_data(self) -> None:
        for node in self.node_names:
            path = self.owned_root / node
            if path.is_symlink() or path.resolve(strict=True).parent != self.owned_root.resolve(strict=True):
                raise RunnerError(f"refusing unsafe data path: {path}")
            helper = f"lw-scylla-clean-{self.nonce[:8]}-{node}"
            _run(["docker", "run", "--rm", "--user", "0:0", "--name", helper,
                  "--label", "ledger-wallet.owner=scylla-wallet-runner-cleaner",
                  "--mount", f"type=bind,src={path.resolve()},dst=/owned",
                  "--entrypoint", "/bin/sh", PINNED_REPO_DIGEST,
                  "-c", "find /owned -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +"], timeout=120)
            path.rmdir()
            self.cleanup_result.data_removed.append(str(path))
        (self.owned_root / ".ledger-wallet-owner.json").unlink()
        self.owned_root.rmdir()

    def _write_cleanup_audit(self) -> None:
        write_json(self.trial_dir / "cleanup.json", asdict(self.cleanup_result))


def _decode_inspect(text: str) -> dict[str, Any]:
    try:
        value = json.loads(text)
    except json.JSONDecodeError as exc:
        raise RunnerError(f"invalid docker inspect JSON: {exc}") from exc
    if not isinstance(value, list) or not value or not isinstance(value[0], dict):
        raise RunnerError("docker inspect returned no object")
    return value[0]


def _read_process_sample(pid: int) -> dict[str, Any]:
    result: dict[str, Any] = {"pid": pid}
    try:
        raw = pathlib.Path(f"/proc/{pid}/stat").read_text()
        end = raw.rfind(")")
        fields = raw[end + 2:].split()
        result["stat_raw"] = raw.strip()
        if len(fields) > 21:
            result["utime_ticks"] = int(fields[11])
            result["stime_ticks"] = int(fields[12])
            result["rss_pages"] = int(fields[21])
    except (OSError, ValueError, IndexError):
        result["stat_error"] = "unavailable"
    try:
        status = pathlib.Path(f"/proc/{pid}/status").read_text()
        selected = {}
        for line in status.splitlines():
            key, sep, value = line.partition(":")
            if sep and key in ("Name", "VmRSS", "VmHWM", "Threads", "voluntary_ctxt_switches",
                               "nonvoluntary_ctxt_switches", "Cpus_allowed_list"):
                selected[key] = value.strip()
        result["status"] = selected
    except OSError:
        result["status_error"] = "unavailable"
    try:
        io = {}
        for line in pathlib.Path(f"/proc/{pid}/io").read_text().splitlines():
            key, sep, value = line.partition(":")
            if sep:
                try:
                    io[key] = int(value.strip())
                except ValueError:
                    continue
        result["io"] = io
    except OSError:
        result["io_error"] = "unavailable"
    result["cgroup"] = _read_process_cgroup(pid)
    return result


def read_process_argv(pid: int) -> list[str]:
    try:
        raw = pathlib.Path(f"/proc/{pid}/cmdline").read_bytes()
    except OSError as exc:
        raise RunnerError(f"cannot read Scylla process argv from /proc/{pid}/cmdline: {exc}") from exc
    argv = [part.decode("utf-8", errors="replace") for part in raw.split(b"\0") if part]
    if not argv:
        raise RunnerError(f"Scylla process argv is empty for pid {pid}")
    return argv


def _read_process_cgroup(pid: int) -> dict[str, Any]:
    try:
        lines = pathlib.Path(f"/proc/{pid}/cgroup").read_text().splitlines()
    except OSError as exc:
        return {"error": str(exc)}
    relative = None
    for line in lines:
        parts = line.split(":", 2)
        if len(parts) == 3 and parts[0] == "0" and parts[1] == "":
            relative = parts[2].lstrip("/")
            break
    if relative is None:
        return {"error": "no cgroup v2 path"}
    root = pathlib.Path("/sys/fs/cgroup") / relative
    result: dict[str, Any] = {"scope": str(root)}
    for name in ("io.stat", "cpu.stat", "memory.current", "memory.peak"):
        try:
            result[name] = (root / name).read_text().strip()
        except OSError as exc:
            result[name + "_error"] = str(exc)
    return result


def _host_proc_snapshot(device: str | None) -> dict[str, Any]:
    result: dict[str, Any] = {"timestamp_utc": _utc_now()}
    for name, path in (("stat", "/proc/stat"), ("diskstats", "/proc/diskstats"),
                       ("net_dev", "/proc/net/dev")):
        try:
            text = pathlib.Path(path).read_text()
            if name == "diskstats" and device:
                text = "\n".join(row for row in text.splitlines()
                                 if len(row.split()) >= 3 and row.split()[2] == device)
            result[name] = text
        except OSError as exc:
            result[name + "_error"] = str(exc)
    return result


def resolve_scylla_host_pid(record: ContainerRecord) -> int:
    result = _run(["docker", "top", record.container_id, "-eo", "pid,args"],
                  check=False, timeout=5)
    if result.returncode:
        raise RunnerError(f"cannot list processes in {record.node}: {result.stderr}")
    choices: list[tuple[int, str]] = []
    for line in (result.stdout or "").splitlines()[1:]:
        match = re.match(r"\s*(\d+)\s+(.*)", line)
        if match:
            command = match.group(2).strip()
            executable = command.split(maxsplit=1)[0] if command else ""
            if pathlib.PurePosixPath(executable).name == "scylla":
                choices.append((int(match.group(1)), command))
    if len(choices) != 1:
        raise RunnerError(f"expected one Scylla process in {record.node}, found {choices}")
    record.host_pid, record.host_process_cmd = choices[0]
    return record.host_pid


def _tcp_socket_rows(pid: int) -> set[tuple[str, str, str]]:
    try:
        socket_inodes = set()
        for fd in pathlib.Path(f"/proc/{pid}/fd").iterdir():
            try:
                target = os.readlink(fd)
            except OSError:
                continue
            match = re.fullmatch(r"socket:\[(\d+)\]", target)
            if match:
                socket_inodes.add(match.group(1))
    except OSError:
        return set()
    rows: set[tuple[str, str, str]] = set()
    for table in ("/proc/net/tcp", "/proc/net/tcp6"):
        try:
            lines = pathlib.Path(table).read_text().splitlines()[1:]
        except OSError:
            continue
        for line in lines:
            fields = line.split()
            if len(fields) < 10 or fields[9] not in socket_inodes:
                continue
            local, remote, state, inode = fields[1], fields[2], fields[3], fields[9]
            try:
                remote_port = int(remote.rsplit(":", 1)[1], 16)
            except (IndexError, ValueError):
                continue
            if remote_port in (9042, 9142, 19042, 19142):
                rows.add((inode, local, remote))
    return rows


class TrialMonitor:
    """One-second raw node scrape and host process/device sampler."""

    def __init__(self, trial_dir: pathlib.Path, cluster: OwnedCluster,
                 cli_process: subprocess.Popen[Any] | None = None) -> None:
        self.trial_dir, self.cluster, self.cli_process = trial_dir, cluster, cli_process
        self.node_names = cluster.node_names
        self.prom_dir = trial_dir / "prometheus"
        self.proc_dir = trial_dir / "proc"
        self.prom_dir.mkdir(parents=True, exist_ok=True)
        self.proc_dir.mkdir(parents=True, exist_ok=True)
        self.device = backing_device_name(cluster.owned_root)
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        self.sample_lock = threading.Lock()
        self.started = time.monotonic()
        self.sample_count = 0
        self.errors: list[str] = []
        self.measurement_start_unix_ns: int | None = None
        self.measurement_end_unix_ns: int | None = None
        self.prom_sample_count: dict[str, int] = {node: 0 for node in self.node_names}
        self.counter_samples: dict[str, dict[str, dict[str, dict[str, list[tuple[int, float]]]]]] = {
            node: {} for node in self.node_names}
        self.socket_samples: list[dict[str, Any]] = []
        self.process_samples: list[dict[str, Any]] = []
        self._proc_file = (self.proc_dir / "samples.jsonl").open("a", encoding="utf-8")
        self._python_cpu_start = time.process_time()

    def sample_once(self) -> None:
        with self.sample_lock:
            elapsed = round(time.monotonic() - self.started, 6)
            sample_unix_ns = time.time_ns()
            stamp = _utc_now()
            sample: dict[str, Any] = {
                "timestamp_utc": stamp, "timestamp_unix_ns": sample_unix_ns,
                "elapsed_seconds": elapsed, "host": _host_proc_snapshot(self.device),
                "device": self.device,
                "python_process_cpu_seconds": time.process_time() - self._python_cpu_start,
                "scylla_processes": {},
            }
            for record in self.cluster.records:
                try:
                    pid = resolve_scylla_host_pid(record)
                    sample["scylla_processes"][record.node] = _read_process_sample(pid)
                except Exception as exc:
                    self.errors.append(f"{record.node} process sample: {exc}")
                    sample["scylla_processes"][record.node] = {"error": str(exc)}
                prom_unix_ns = time.time_ns()
                try:
                    raw = fetch_prometheus(record.ip, timeout=3.0)
                    parsed_samples = parse_prometheus_samples(raw)
                    destination = self.prom_dir / f"{record.node}.prom.gz"
                    with gzip.open(destination, "ab") as handle:
                        handle.write((f"# runner_sample_unix_ns {prom_unix_ns} "
                                      f"elapsed_seconds {elapsed}\n").encode())
                        handle.write(raw.encode())
                        if not raw.endswith("\n"):
                            handle.write(b"\n")
                    self.prom_sample_count[record.node] += 1
                    for metric in parsed_samples:
                        if metric["name"] not in COMMITLOG_COUNTERS and metric["name"] not in COMMITLOG_GAUGES and metric["name"] not in (
                                "scylla_network_bytes_sent", "scylla_network_bytes_received"):
                            continue
                        labels = metric["labels"]
                        shard = str(labels.get("shard", "unlabeled"))
                        group_labels = {key: value for key, value in labels.items() if key != "shard"}
                        group_key = json.dumps(group_labels, sort_keys=True)
                        series = self.counter_samples[record.node].setdefault(
                            metric["name"], {}).setdefault(shard, {}).setdefault(group_key, [])
                        series.append((prom_unix_ns, metric["value"]))
                except Exception as exc:
                    self.errors.append(f"{record.node} Prometheus sample: {exc}")
                    sample.setdefault("prometheus_errors", {})[record.node] = str(exc)
            if self.cli_process is not None:
                try:
                    sample["client_process"] = _read_process_sample(self.cli_process.pid)
                    sample["client_native_sockets"] = sorted(_tcp_socket_rows(self.cli_process.pid))
                    self.socket_samples.append({
                        "timestamp_unix_ns": time.time_ns(),
                        "sockets": sample["client_native_sockets"],
                    })
                except Exception as exc:
                    self.errors.append(f"client process/socket sample: {exc}")
                    sample["client_process_error"] = str(exc)
            self.process_samples.append(sample)
            self._proc_file.write(json.dumps(sample, sort_keys=True) + "\n")
            self._proc_file.flush()
            self.sample_count += 1

    def _loop(self) -> None:
        while not self.stop_event.is_set():
            started = time.monotonic()
            self.sample_once()
            delay = max(0.0, 1.0 - (time.monotonic() - started))
            self.stop_event.wait(delay)

    def start(self) -> None:
        self.sample_once()
        self.thread = threading.Thread(target=self._loop, name="scylla-wallet-monitor", daemon=True)
        self.thread.start()

    def set_measurement_start(self, timestamp_unix_ns: int) -> None:
        with self.sample_lock:
            self.measurement_start_unix_ns = timestamp_unix_ns

    def set_measurement_end(self, timestamp_unix_ns: int) -> None:
        with self.sample_lock:
            self.measurement_end_unix_ns = timestamp_unix_ns

    def stop(self, window_start_unix_ns: int | None = None,
             window_end_unix_ns: int | None = None, *,
             logical_requests_expected: int | None = None,
             logical_replies_received: int | None = None) -> dict[str, Any]:
        if window_start_unix_ns is not None:
            self.set_measurement_start(window_start_unix_ns)
        if window_end_unix_ns is not None:
            self.set_measurement_end(window_end_unix_ns)
        self.stop_event.set()
        if self.thread is not None:
            # Each Docker and Prometheus operation in sample_once has a timeout.
            # Join fully before closing the file so there is no concurrent write.
            self.thread.join()
        try:
            self.sample_once()  # capture a post-window socket/process sample
        except Exception as exc:
            self.errors.append(f"final monitor sample: {exc}")
        finally:
            self._proc_file.close()
        if window_start_unix_ns is None or window_end_unix_ns is None:
            start_ns, end_ns = 0, (1 << 127)
            window_valid = False
        else:
            start_ns, end_ns = window_start_unix_ns, window_end_unix_ns
            window_valid = start_ns > 0 and end_ns >= start_ns
        prom_window_evidence = prometheus_window_metric_evidence(
            self.prom_dir, start_ns, end_ns, self.node_names)
        categories_by_node = {node: metric_evidence_from_names(
            evidence["metric_names"]) for node, evidence in prom_window_evidence.items()}
        socket_result = validate_socket_window(self.socket_samples, start_ns, end_ns)
        proc_result = validate_process_samples(
            self.process_samples, start_ns, end_ns, self.node_names)
        counter_evidence = summarize_counter_window(
            self.counter_samples, start_ns, end_ns, self.cluster.shards,
            self.node_names)
        counter_evidence["application_work_contract"] = application_work_contract(
            logical_requests_expected, logical_replies_received)
        summary = {
            "sample_count": self.sample_count,
            "expected_node_names": list(self.node_names),
            "device_counted_once": self.device,
            "measurement_window": {"start_unix_ns": window_start_unix_ns,
                                   "end_unix_ns": window_end_unix_ns,
                                   "timestamps_valid": window_valid},
            "prometheus_total_samples": self.prom_sample_count,
            "prometheus_window_samples": {node: evidence["sample_count"]
                                          for node, evidence in prom_window_evidence.items()},
            "prometheus_window_sample_timestamps_unix_ns": {
                node: evidence["sample_timestamps_unix_ns"]
                for node, evidence in prom_window_evidence.items()},
            "prometheus_window_metric_names": {node: evidence["metric_names"]
                                               for node, evidence in prom_window_evidence.items()},
            "prometheus_window_parse_errors": {node: evidence.get("error")
                                                for node, evidence in prom_window_evidence.items()
                                                if evidence.get("error")},
            "required_metric_evidence": categories_by_node,
            "counter_window_deltas": counter_evidence,
            "socket_window": socket_result,
            "socket_window_stable": socket_result["valid"],
            "process_window": proc_result,
            "errors": self.errors,
        }
        write_json(self.proc_dir / "monitor-summary.json", summary)
        return summary


def validate_socket_window(samples: Sequence[dict[str, Any]], start_ns: int,
                           end_ns: int) -> dict[str, Any]:
    before = [sample for sample in samples if sample.get("timestamp_unix_ns", 0) < start_ns]
    inside_after = [sample for sample in samples if sample.get("timestamp_unix_ns", 0) >= start_ns]
    baseline_sample = before[-1] if before else None
    baseline = set(tuple(row) for row in (baseline_sample or {}).get("sockets", []))
    relevant = [sample for sample in inside_after
                if sample.get("timestamp_unix_ns", 0) <= end_ns]
    after = [sample for sample in samples if sample.get("timestamp_unix_ns", 0) > end_ns]
    if after:
        relevant.append(after[0])
    changes = []
    if baseline_sample is None:
        changes.append({"error": "no pre-window socket baseline"})
    for sample in relevant:
        observed = set(tuple(row) for row in sample.get("sockets", []))
        if observed != baseline:
            changes.append({"timestamp_unix_ns": sample.get("timestamp_unix_ns"),
                            "removed": sorted(baseline - observed),
                            "added": sorted(observed - baseline)})
    return {"valid": baseline_sample is not None and not changes,
            "baseline_unix_ns": (baseline_sample or {}).get("timestamp_unix_ns"),
            "baseline_sockets": sorted(baseline),
            "observed_sample_count": len(relevant), "changes": changes,
            "limitation": "one-second endpoint snapshots can miss a shorter transient reconnect"}


def validate_process_samples(samples: Sequence[dict[str, Any]], start_ns: int,
                             end_ns: int,
                             node_names: Sequence[str] = NODE_NAMES) -> dict[str, Any]:
    selected = [sample for sample in samples
                if start_ns <= sample.get("timestamp_unix_ns", 0) <= end_ns]
    before = [sample for sample in samples if sample.get("timestamp_unix_ns", 0) < start_ns]
    after = [sample for sample in samples if sample.get("timestamp_unix_ns", 0) > end_ns]
    tracked = ([before[-1]] if before else []) + selected + ([after[0]] if after else [])
    nodes: dict[str, Any] = {}
    valid = bool(selected)
    for node in node_names:
        rows = [sample.get("scylla_processes", {}).get(node, {}) for sample in selected]
        observed_pids = sorted({int(sample["scylla_processes"][node]["pid"])
                                for sample in tracked
                                if isinstance(sample.get("scylla_processes", {}).get(node), dict)
                                and sample["scylla_processes"][node].get("pid") is not None})
        cpu_rss = [row for row in rows if "error" not in row
                   and "stat_error" not in row
                   and "utime_ticks" in row and "stime_ticks" in row
                   and "rss_pages" in row
                   and "VmRSS" in row.get("status", {})]
        io = [row for row in rows if row.get("io")
              or row.get("cgroup", {}).get("io.stat")]
        pid_stable = len(observed_pids) == 1
        node_valid = bool(cpu_rss and io and pid_stable)
        nodes[node] = {"window_samples": len(rows), "cpu_rss_samples": len(cpu_rss),
                       "io_or_cgroup_samples": len(io), "valid": node_valid,
                       "observed_host_pids": observed_pids,
                       "host_pid_stable_across_window_and_boundaries": pid_stable,
                       "io_source": ("proc_pid_io" if any(row.get("io") for row in io)
                                     else "cgroup_v2_io.stat" if io else "unavailable")}
        valid = valid and node_valid
    return {"valid": valid, "nodes": nodes,
            "window_sample_count": len(selected),
            "io_scope_note": "cgroup io.stat is container-scope and is an explicit fallback for root-owned Scylla PIDs",
            "pid_sampling_limitation": "one-second snapshots can miss a shorter stop/restart between samples"}


def application_work_contract(logical_requests_expected: int | None = None,
                              logical_replies_received: int | None = None) -> dict[str, Any]:
    expected = logical_requests_expected
    expected_statements = (expected * CQL_STATEMENTS_PER_TRANSACTION
                           if expected is not None else None)
    expected_selects = (expected * 0 if expected is not None else None)
    return {
        "wallet_profile": WALLET_PROFILE,
        "account_state_source": ACCOUNT_STATE_SOURCE,
        "ledger_key": list(LEDGER_KEY),
        "serial_cl": SERIAL_CL,
        "logical_requests_expected": expected,
        "logical_replies_received": logical_replies_received,
        "partition_mutations_per_request": 1,
        "partition_mutations_expected": expected,
        "cql_requests_per_transaction": CQL_REQUESTS_PER_TRANSACTION,
        "application_cql_statements_per_request": CQL_STATEMENTS_PER_TRANSACTION,
        "application_cql_statements_expected": expected_statements,
        "measured_select_requests_per_transaction": 0,
        "measured_select_requests_expected": expected_selects,
        "cql_shape": "one conditional INSERT ... IF NOT EXISTS (LWT); no measured SELECT statements",
    }


def summarize_counter_window(
        samples: dict[str, dict[str, dict[str, dict[str, list[tuple[int, float]]]]]],
        start_ns: int, end_ns: int, shards: int,
        node_names: Sequence[str] = NODE_NAMES) -> dict[str, Any]:
    counter_names = (*COMMITLOG_COUNTERS,
                     "scylla_network_bytes_sent", "scylla_network_bytes_received")
    node_evidence: dict[str, Any] = {}
    required_valid = start_ns > 0 and end_ns >= start_ns
    for node in node_names:
        metric_results: dict[str, Any] = {}
        for name in (*counter_names, *COMMITLOG_GAUGES):
            by_shard = samples.get(node, {}).get(name, {})
            shard_results: dict[str, Any] = {}
            expected_shards = [str(shard) for shard in range(shards)]
            actual_shards = sorted(by_shard, key=lambda value: (not value.isdigit(), value))
            target_shards = expected_shards if any(key != "unlabeled" for key in by_shard) else ["unlabeled"]
            for shard in target_shards:
                group_series = by_shard.get(shard, {})
                values_by_timestamp: dict[int, dict[str, float]] = {}
                for group_key, samples_for_group in group_series.items():
                    for timestamp, value in samples_for_group:
                        values_by_timestamp.setdefault(timestamp, {})[group_key] = value
                expected_groups = set(group_series)
                series = sorted(((stamp, sum(group_values.values()))
                                 for stamp, group_values in values_by_timestamp.items()
                                 if set(group_values) == expected_groups),
                                key=lambda pair: pair[0])
                before = [(stamp, value) for stamp, value in series if stamp <= start_ns]
                inside = [(stamp, value) for stamp, value in series if start_ns <= stamp <= end_ns]
                through_end = [(stamp, value) for stamp, value in series if stamp <= end_ns]
                start_point = before[-1] if before else None
                end_point = through_end[-1] if through_end else None
                monotonic_window = ([point for point in series
                                     if start_point is not None
                                     and start_point[0] <= point[0] <= end_ns])
                reset_transitions = [
                    {"previous_sample_unix_ns": previous[0],
                     "previous_value": previous[1], "current_sample_unix_ns": current[0],
                     "current_value": current[1]}
                    for previous, current in zip(monotonic_window, monotonic_window[1:])
                    if current[1] < previous[1]
                ]
                common = {
                    "shard": shard,
                    "observed_shards": actual_shards,
                    "window_sample_count": len(inside),
                    "start_value": start_point[1] if start_point else None,
                    "start_sample_unix_ns": start_point[0] if start_point else None,
                    "start_sampling_offset_ns": start_point[0] - start_ns if start_point else None,
                    "end_value": end_point[1] if end_point else None,
                    "end_sample_unix_ns": end_point[0] if end_point else None,
                    "end_sampling_offset_ns": end_point[0] - end_ns if end_point else None,
                    "reset_transitions": reset_transitions,
                }
                if name in COMMITLOG_GAUGES:
                    values = [value for _, value in inside]
                    common.update({"status": "observed" if values else "unavailable",
                                   "first": values[0] if values else None,
                                   "min": min(values) if values else None,
                                   "max": max(values) if values else None,
                                   "last": values[-1] if values else None})
                elif start_point is None or end_point is None or not inside:
                    common.update({"status": "unavailable", "delta": None})
                    if name in COMMITLOG_COUNTERS:
                        required_valid = False
                elif reset_transitions or end_point[1] < start_point[1]:
                    common.update({"status": "counter_reset_or_wrap", "delta": None})
                    if name in COMMITLOG_COUNTERS:
                        required_valid = False
                else:
                    common.update({"status": "observed", "delta": end_point[1] - start_point[1]})
                shard_results[shard] = common
            if name in COMMITLOG_COUNTERS:
                present_expected = all(shard in shard_results for shard in expected_shards)
                if not present_expected:
                    required_valid = False
                if target_shards == ["unlabeled"] and shards > 1:
                    required_valid = False
            metric_results[name] = {"series": shard_results,
                                    "source_metric": name,
                                    "semantics_note": ("Scylla commitlog counter; not a request count"
                                                       if name in COMMITLOG_COUNTERS else None)}
        node_evidence[node] = metric_results
    return {"valid": required_valid, "required_commitlog_metrics": list(COMMITLOG_COUNTERS),
            "nodes": node_evidence,
            "application_work_contract": application_work_contract(),
            "limitations": ["commitlog flush counters are not request counts",
                            "counter deltas use nearest raw scrape at/before both window boundaries",
                            "a metric for DB write await or fsync duration is not inferred from request latency"]}


def required_metric_validation(monitor_summary: dict[str, Any]) -> dict[str, Any]:
    required = ("reactor_cpu", "latency", "cache", "flush", "compaction",
                "commitlog_sync", "backlog", "errors", "timeouts", "network")
    nodes: dict[str, Any] = {}
    window_samples = monitor_summary.get("prometheus_window_samples", {})
    valid = monitor_summary.get("measurement_window", {}).get("timestamps_valid") is True
    for node, evidence in monitor_summary.get("required_metric_evidence", {}).items():
        categories = evidence.get("categories", {})
        missing = [key for key in required if not categories.get(key)]
        count = window_samples.get(node, 0)
        nodes[node] = {"missing_required_groups": missing,
                       "missing_groups_reported_as_na": missing,
                       "window_samples": count,
                       "metric_names": evidence.get("metric_names", [])}
        valid = valid and not missing and count > 0
    expected_nodes = set(monitor_summary.get("expected_node_names", NODE_NAMES))
    if set(nodes) != expected_nodes:
        valid = False
    process = monitor_summary.get("process_window", {})
    sockets = monitor_summary.get("socket_window", {})
    counters = monitor_summary.get("counter_window_deltas", {})
    valid = (valid and process.get("valid") is True and sockets.get("valid") is True
             and counters.get("valid") is True)
    return {"valid": valid, "required_groups": list(required),
            "expected_nodes": sorted(expected_nodes), "nodes": nodes,
            "process_evidence_valid": process.get("valid") is True,
            "socket_window_valid": sockets.get("valid") is True,
            "counter_window_deltas_valid": counters.get("valid") is True}


def docker_config_evidence(cluster: OwnedCluster) -> dict[str, Any]:
    result: dict[str, Any] = {}
    current_relabel_hash = hashlib.sha256(cluster.metrics_relabel_path.read_bytes()).hexdigest()
    for record in cluster.records:
        info = _decode_inspect(_run(["docker", "inspect", record.container_id]).stdout)
        cmd = info.get("Config", {}).get("Cmd") or []
        env = info.get("Config", {}).get("Env") or []
        actual_flags: dict[str, str | bool] = {}
        for index, arg in enumerate(cmd):
            if arg.startswith("--") and "=" in arg:
                key, value = arg[2:].split("=", 1)
                actual_flags[key.replace("-", "_")] = value
            elif arg.startswith("--") and index + 1 < len(cmd) and not cmd[index + 1].startswith("--"):
                actual_flags[arg[2:].replace("-", "_")] = cmd[index + 1]
        scylla_argv = (read_process_argv(record.host_pid)
                       if record.host_pid is not None else None)
        host_io_limit_validation = None
        if cluster.host_io_limits is not None:
            if record.host_pid is None:
                raise RunnerError(f"cannot verify cgroup I/O caps for {record.node}: host PID is missing")
            host_config = info.get("HostConfig") or {}
            host_io_limit_validation = validate_container_io_limits(
                host_config, record.host_pid, cluster.host_io_limits)
        io_sha = None
        if cluster.io_properties_profile.get("enabled") is True:
            sha_result = _run(["docker", "exec", record.container_id,
                               "sha256sum", IO_PROPERTIES_CONTAINER_PATH],
                              check=False, timeout=10)
            if sha_result.returncode == 0:
                match = re.match(r"\s*([0-9a-fA-F]{64})\s+", sha_result.stdout or "")
                io_sha = match.group(1).lower() if match else None
        result[record.node] = {
            "container_config_cmd": cmd, "container_env": env,
            "parsed_launch_flags": actual_flags,
            "scylla_process_argv": scylla_argv,
            "observed_io_properties_sha256": io_sha,
            "expected_io_properties_source": cluster.io_properties_profile.get("config_path"),
            "expected_io_properties_sha256": cluster.io_properties_profile.get("source_sha256"),
            "expected_owned_data_source": str(cluster.owned_root / record.node),
            "expected_metrics_relabel_source": str(cluster.metrics_relabel_path),
            "expected_metrics_relabel_sha256": cluster.metrics_relabel_profile["sha256"],
            "observed_metrics_relabel_sha256": current_relabel_hash,
            "host_config": {
                "memory_bytes": info.get("HostConfig", {}).get("Memory"),
                "memory_swap_bytes": info.get("HostConfig", {}).get("MemorySwap"),
                "cpu_quota": info.get("HostConfig", {}).get("CpuQuota"),
                "cpu_period": info.get("HostConfig", {}).get("CpuPeriod"),
                "nano_cpus": info.get("HostConfig", {}).get("NanoCpus"),
                "cpuset_cpus": info.get("HostConfig", {}).get("CpusetCpus"),
                "network_mode": info.get("HostConfig", {}).get("NetworkMode"),
                "port_bindings": info.get("HostConfig", {}).get("PortBindings"),
                "blkio_device_read_bps": info.get("HostConfig", {}).get("BlkioDeviceReadBps"),
                "blkio_device_write_bps": info.get("HostConfig", {}).get("BlkioDeviceWriteBps"),
                "blkio_device_read_iops": info.get("HostConfig", {}).get("BlkioDeviceReadIOps"),
                "blkio_device_write_iops": info.get("HostConfig", {}).get("BlkioDeviceWriteIOps"),
                "mounts": info.get("Mounts"),
                "metrics_relabel_mounts": [
                    mount for mount in (info.get("Mounts") or [])
                    if mount.get("Destination") == METRICS_RELABEL_CONTAINER_PATH],
                "io_properties_mounts": [
                    mount for mount in (info.get("Mounts") or [])
                    if mount.get("Destination") == IO_PROPERTIES_CONTAINER_PATH],
            },
            "host_io_limits_validation": host_io_limit_validation,
            "effective_config_api": cluster._api_config(record),
            "version_cql": cluster.cqlsh(record, "SELECT JSON release_version FROM system.local"),
        }
    return result


def parse_cli_events(path: pathlib.Path) -> list[dict[str, Any]]:
    events = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise RunnerError(f"cannot read CLI event file {path}: {exc}") from exc
    for line_number, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError as exc:
            raise RunnerError(f"invalid event JSON at {path}:{line_number}: {exc}") from exc
        if isinstance(event, dict):
            events.append(event)
    return events


def find_event(events: Sequence[dict[str, Any]], phase: str) -> dict[str, Any] | None:
    return next((event for event in events if event.get("phase") == phase), None)


def event_session_id(event: dict[str, Any]) -> str | None:
    value = event.get("session_id")
    if value is None and isinstance(event.get("metadata"), dict):
        value = event["metadata"].get("session_id")
    return str(value) if value else None


def wait_for_event(path: pathlib.Path, process: subprocess.Popen[Any], phase: str,
                   timeout: int) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last_error: str | None = None
    while time.monotonic() < deadline:
        if path.exists():
            try:
                event = find_event(parse_cli_events(path), phase)
            except RunnerError as exc:
                last_error = str(exc)
                event = None
            if event:
                return event
        if process.poll() is not None:
            if path.exists():
                event = find_event(parse_cli_events(path), phase)
                if event:
                    return event
            raise RunnerError(f"CLI exited {process.returncode} before phase {phase}")
        time.sleep(0.1)
    suffix = f"; last parse error: {last_error}" if last_error else ""
    raise RunnerError(f"CLI did not emit phase {phase} within {timeout}s{suffix}")


def terminate_process(process: subprocess.Popen[Any], timeout: int = 10) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=timeout)


def _query_all_session_clients(cluster: OwnedCluster) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}
    for record in cluster.records:
        output = cluster.cqlsh(record, "SELECT JSON client_options FROM system.clients",
                               consistency="ONE")
        result[record.node] = parse_cqlsh_client_rows(output)
    return result


def connection_audit(cluster: OwnedCluster, session_id: str, case: TrialCase,
                     output_path: pathlib.Path, phase: str) -> dict[str, Any]:
    rows = _query_all_session_clients(cluster)
    audit = audit_session_connections(
        rows, session_id, workers=case.workers,
        expected_data_per_node=case.expected_connections_per_node,
        target_data_connections=case.data_connection_target,
        shards=case.shards, per_shard_pool=case.per_shard_pool,
        node_count=case.node_count)
    audit.update({
        "C": case.workers, "S": case.shards, "P": case.per_shard_pool,
        "D": case.data_connection_target, "worker_routing": case.worker_routing,
        "per_shard_pool_overridden": case.per_shard_pool_overridden,
        "requested_per_shard_pool": case.requested_per_shard_pool,
        "setup_concurrency": case.setup_concurrency,
        "per_worker_queue_capacity": case.per_worker_queue_capacity,
    })
    audit["phase"] = phase
    audit["raw_client_rows"] = rows
    audit["captured_utc"] = _utc_now()
    write_json(output_path, audit)
    return audit


def _run_cql_json_query(record: ContainerRecord, query: str) -> dict[str, Any]:
    statement = f"CONSISTENCY ONE; {query}"
    command = ["docker", "exec", record.container_id, "cqlsh", record.ip, "9042",
               "--execute", statement]
    try:
        process = _run(command, check=False, timeout=30)
        output = process.stdout or ""
        return {"query": query, "consistency": "ONE", "return_code": process.returncode,
                "raw_output": output, "stderr": process.stderr or "",
                "rows": parse_cql_json_rows(output),
                "zero_rows_footer": cqlsh_reports_zero_rows(output)}
    except RunnerError as exc:
        return {"query": query, "consistency": "ONE", "return_code": None,
                "raw_output": "", "stderr": str(exc), "runner_error": str(exc), "rows": []}


def _numeric_replication_factor(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and re.fullmatch(r"[0-9]+", value.strip()):
        return int(value.strip())
    return None


def validate_keyspace_schema(keyspace: str, keyspace_query: dict[str, Any],
                             tablets_query: dict[str, Any],
                             replication_factor: int = 3) -> dict[str, Any]:
    problems: list[str] = []
    if isinstance(replication_factor, bool) or replication_factor not in (1, 2, 3):
        raise RunnerError("replication-factor must be 1, 2, or 3")
    keyspace_rows = parse_cql_json_rows(str(keyspace_query.get("raw_output", "")))
    tablets_rows = parse_cql_json_rows(str(tablets_query.get("raw_output", "")))
    keyspace_query["rows"] = keyspace_rows
    tablets_query["rows"] = tablets_rows
    interpretation: dict[str, Any] = {
        "keyspace_row_count": len(keyspace_rows),
        "durable_writes": None, "replication_class": None,
        "replication_factor_expected": replication_factor,
        "replication_factors": {}, "tablets_metadata_row_count": len(tablets_rows),
        "initial_tablets": "absent", "tablets_disabled": None,
        "tablets_interpretation": None,
    }

    if keyspace_query.get("return_code") != 0:
        problems.append(f"keyspace metadata query failed: {keyspace_query.get('stderr') or keyspace_query.get('return_code')}")
    if len(keyspace_rows) != 1:
        problems.append(f"expected exactly one keyspace metadata row, observed {len(keyspace_rows)}")
    elif not isinstance(keyspace_rows[0], dict):
        problems.append("keyspace metadata row is not an object")
    else:
        row = keyspace_rows[0]
        missing = [name for name in ("durable_writes", "replication") if name not in row]
        if missing:
            problems.append(f"keyspace metadata row is missing fields: {missing}")
        durable_writes = row.get("durable_writes")
        interpretation["durable_writes"] = durable_writes
        if durable_writes is not True:
            problems.append(f"durable_writes must be true, observed {durable_writes!r}")

        replication = row.get("replication")
        if not isinstance(replication, dict):
            problems.append(f"replication metadata must be a map, observed {replication!r}")
        else:
            strategy = replication.get("class")
            interpretation["replication_class"] = strategy
            accepted_strategies = {
                "NetworkTopologyStrategy",
                "org.apache.cassandra.locator.NetworkTopologyStrategy",
            }
            if not isinstance(strategy, str) or strategy not in accepted_strategies:
                problems.append(f"replication class is not NetworkTopologyStrategy: {strategy!r}")
            factors = {name: value for name, value in replication.items() if name != "class"}
            interpretation["replication_factors"] = factors
            if len(factors) != 1:
                problems.append(f"replication must contain exactly one DC factor, observed {factors}")
            for dc, raw_factor in factors.items():
                if not isinstance(dc, str) or not dc.strip():
                    problems.append(f"replication DC name is invalid: {dc!r}")
                factor = _numeric_replication_factor(raw_factor)
                if factor is None:
                    problems.append(f"replication factor for {dc!r} is not numeric: {raw_factor!r}")
                elif factor != replication_factor:
                    problems.append(f"replication factor for {dc!r} must be {replication_factor}, observed {factor}")

    if tablets_query.get("return_code") != 0:
        problems.append("system_schema.scylla_keyspaces metadata query failed: "
                        f"{tablets_query.get('stderr') or tablets_query.get('return_code')}")
    raw_tablets = str(tablets_query.get("raw_output", ""))
    zero_rows_footer = cqlsh_reports_zero_rows(raw_tablets)
    tablets_query["zero_rows_footer"] = zero_rows_footer
    if not tablets_rows and not zero_rows_footer:
        if not raw_tablets.strip():
            problems.append("tablets metadata query succeeded with blank output; disabled state is unconfirmed")
        else:
            problems.append("tablets metadata query returned non-JSON output without rows")
    if len(tablets_rows) == 0:
        no_rows_confirmed = tablets_query.get("return_code") == 0 and zero_rows_footer
        interpretation["tablets_disabled"] = no_rows_confirmed
        interpretation["tablets_interpretation"] = (
            "disabled: no scylla_keyspaces row; optional initial_tablets is absent"
            if no_rows_confirmed else "unknown: no successful zero-row result")
    elif len(tablets_rows) == 1 and isinstance(tablets_rows[0], dict):
        tablet_row = tablets_rows[0]
        if "initial_tablets" not in tablet_row:
            problems.append("scylla_keyspaces row is missing initial_tablets")
            interpretation["tablets_interpretation"] = "invalid: missing initial_tablets field"
        elif tablet_row["initial_tablets"] is None:
            interpretation["initial_tablets"] = None
            interpretation["tablets_disabled"] = True
            interpretation["tablets_interpretation"] = (
                "disabled: initial_tablets is null (optional value absent)")
        else:
            interpretation["initial_tablets"] = tablet_row["initial_tablets"]
            interpretation["tablets_disabled"] = False
            interpretation["tablets_interpretation"] = (
                "enabled/present: initial_tablets has a value")
            problems.append("tablets must be disabled; system_schema.scylla_keyspaces "
                            f"initial_tablets is present: {tablet_row['initial_tablets']!r}")
    else:
        interpretation["tablets_interpretation"] = "invalid: expected zero rows or one null row"
        problems.append(f"expected zero or one scylla_keyspaces row, observed {len(tablets_rows)}")

    return {
        "keyspace": keyspace,
        "queries": {"keyspace_options": keyspace_query, "tablet_metadata": tablets_query},
        "source_evidence": KEYSPACE_TABLETS_SOURCE_EVIDENCE,
        "interpretation": interpretation,
        "valid": not problems, "problems": problems,
    }


def validate_runtime_aio_startup_logs(logs_by_node: dict[str, str],
                                     node_names: Sequence[str] = NODE_NAMES,
                                     io_properties_enabled: bool = False) -> dict[str, Any]:
    expected_nodes = set(node_names)
    observed_nodes = set(logs_by_node)
    problems: list[str] = []
    if observed_nodes != expected_nodes:
        problems.append(
            f"runtime AIO logs must cover exactly {sorted(expected_nodes)}, "
            f"observed {sorted(observed_nodes)}")
    reduction_pattern = re.compile(
        r"\breducing\b[^\n]{0,240}\bmax-networking-io-control-blocks\b", re.IGNORECASE)
    capacity_pattern = re.compile(r"not enough AIO capacity", re.IGNORECASE)
    missing_io_properties_pattern = re.compile(
        r"none of --io-properties and --io-properties-file are set", re.IGNORECASE)
    minimum_pattern = re.compile(
        r"\bminimum\b[^\n]{0,160}\bAIO\b|\bAIO\b[^\n]{0,160}\bminimum\b",
        re.IGNORECASE)
    nodes: dict[str, Any] = {}
    for node in node_names:
        raw = logs_by_node.get(node)
        node_problems: list[str] = []
        if not isinstance(raw, str) or not raw.strip():
            node_problems.append("runtime startup log is missing or blank")
            raw = ""
        backends = re.findall(r"\bReactor backend:\s*([^\s,;]+)", raw, re.IGNORECASE)
        if not backends:
            node_problems.append("actual Reactor backend marker is missing")
        elif len(backends) != 1 or {value.lower() for value in backends} != {"linux-aio"}:
            node_problems.append(f"actual Reactor backend must be exactly linux-aio, observed {backends}")
        failure_matches = []
        for line in raw.splitlines():
            if reduction_pattern.search(line):
                failure_matches.append({"kind": "network_iocb_reduction", "line": line.strip()})
            if capacity_pattern.search(line):
                failure_matches.append({"kind": "insufficient_aio_capacity", "line": line.strip()})
            if minimum_pattern.search(line):
                failure_matches.append({"kind": "minimum_aio_failure", "line": line.strip()})
        if failure_matches:
            node_problems.append("startup log reports AIO reduction or insufficient AIO capacity")
        missing_io_properties = bool(missing_io_properties_pattern.search(raw))
        if io_properties_enabled and missing_io_properties:
            node_problems.append("startup log reports that no I/O properties file was configured")
        nodes[node] = {
            "log_present": bool(raw.strip()),
            "reactor_backend_markers": backends,
            "backend_valid": (len(backends) == 1 and backends[0].lower() == "linux-aio"),
            "failure_markers": failure_matches,
            "missing_io_properties_warning": missing_io_properties,
            "valid": not node_problems,
            "problems": node_problems,
        }
        problems.extend(f"{node}: {problem}" for problem in node_problems)
    return {"valid": not problems, "expected_nodes": list(node_names),
            "observed_nodes": sorted(observed_nodes), "nodes": nodes,
            "problems": problems,
            "tool_fallback_note": (
                "io_uring-to-epoll tool fallback notices are not reactor backend markers; "
                "the actual Reactor backend marker must be linux-aio")}


def verify_keyspace(cluster: OwnedCluster, keyspace: str,
                    replication_factor: int = 3) -> dict[str, Any]:
    # The workload crate creates the schema. The runner checks the effective
    # keyspace options before releasing the measurement gate.
    escaped = keyspace.replace("'", "''")
    keyspace_query = _run_cql_json_query(
        cluster.records[0],
        "SELECT JSON durable_writes, replication FROM system_schema.keyspaces "
        f"WHERE keyspace_name = '{escaped}'")
    tablets_query = _run_cql_json_query(
        cluster.records[0],
        "SELECT JSON initial_tablets FROM system_schema.scylla_keyspaces "
        f"WHERE keyspace_name = '{escaped}'")
    result = validate_keyspace_schema(keyspace, keyspace_query, tablets_query,
                                       replication_factor=replication_factor)
    write_json(cluster.trial_dir / "keyspace-validation.json", result)
    if not result["valid"]:
        raise RunnerError("; ".join(result["problems"]))
    return result


def validate_wallet_schema(keyspace: str, query: dict[str, Any]) -> dict[str, Any]:
    """Verify the wallet partition/clustering key and memory-LWT ledger columns."""
    rows = parse_cql_json_rows(str(query.get("raw_output", "")))
    query["rows"] = rows
    problems: list[str] = []
    if query.get("return_code") != 0:
        problems.append(f"wallet column metadata query failed: {query.get('stderr') or query.get('return_code')}")
    by_name: dict[str, dict[str, Any]] = {}
    for row in rows:
        name = row.get("column_name") if isinstance(row, dict) else None
        if not isinstance(name, str) or not name:
            problems.append(f"wallet column metadata row has invalid column_name: {row!r}")
            continue
        if name in by_name:
            problems.append(f"wallet column metadata repeats {name!r}")
        else:
            by_name[name] = row
    expected_names = set(WALLET_COLUMN_SCHEMA)
    observed_names = set(by_name)
    if "transaction_at" in observed_names:
        problems.append("wallet schema must not contain transaction_at")
    if observed_names != expected_names:
        problems.append(
            f"wallet columns must be exactly {sorted(expected_names)}, observed {sorted(observed_names)}")
    columns: dict[str, Any] = {}
    for name, (expected_type, expected_kind, expected_position) in WALLET_COLUMN_SCHEMA.items():
        row = by_name.get(name)
        if row is None:
            continue
        observed_type = row.get("type")
        observed_kind = row.get("kind")
        position = row.get("position")
        columns[name] = {"type": observed_type, "kind": observed_kind,
                         "position": position}
        if not isinstance(observed_type, str) or observed_type.lower() != expected_type:
            problems.append(f"wallet column {name} type must be {expected_type}, observed {observed_type!r}")
        if observed_kind != expected_kind:
            problems.append(f"wallet column {name} kind must be {expected_kind}, observed {observed_kind!r}")
        if expected_position is not None:
            numeric_position = _numeric_replication_factor(position)
            if numeric_position != expected_position:
                problems.append(
                    f"wallet column {name} position must be {expected_position}, observed {position!r}")
    result = {
        "keyspace": keyspace,
        "table": "wallet",
        "query": query.get("query"),
        "consistency": query.get("consistency"),
        "return_code": query.get("return_code"),
        "raw_output": query.get("raw_output", ""),
        "stderr": query.get("stderr", ""),
        "expected_primary_key": "((account_id),kind,tx_id)",
        "expected_columns": {name: {"type": shape[0], "kind": shape[1],
                                     "position": shape[2]}
                              for name, shape in WALLET_COLUMN_SCHEMA.items()},
        "observed_columns": columns,
        "transaction_at_absent": "transaction_at" not in observed_names,
        "valid": not problems,
        "problems": problems,
    }
    return result


def verify_wallet_schema(cluster: OwnedCluster, keyspace: str) -> dict[str, Any]:
    escaped = keyspace.replace("'", "''")
    query = _run_cql_json_query(
        cluster.records[0],
        "SELECT JSON column_name, kind, position, type FROM system_schema.columns "
        f"WHERE keyspace_name = '{escaped}' AND table_name = 'wallet'")
    result = validate_wallet_schema(keyspace, query)
    write_json(cluster.trial_dir / "wallet-schema-validation.json", result)
    if not result["valid"]:
        raise RunnerError("wallet table schema validation failed: " + "; ".join(result["problems"]))
    return result


def verify_image_is_pulled() -> dict[str, Any]:
    result = _run(["docker", "image", "inspect", PINNED_REPO_DIGEST], check=False)
    if result.returncode:
        raise RunnerError(f"pinned Scylla image is not present locally: {PINNED_REPO_DIGEST}")
    try:
        image = json.loads(result.stdout)[0]
    except (json.JSONDecodeError, IndexError, TypeError) as exc:
        raise RunnerError(f"invalid image inspection response: {exc}") from exc
    digests = image.get("RepoDigests") or []
    if PINNED_REPO_DIGEST not in digests:
        raise RunnerError(f"Scylla image does not have the pinned RepoDigest: {digests}")
    return {"reference": PINNED_REPO_DIGEST, "id": image.get("Id"),
            "repo_digests": digests, "labels": image.get("Config", {}).get("Labels") or {},
            "architecture": image.get("Architecture"), "os": image.get("Os"),
            "created": image.get("Created")}


def validate_launch_config(config: dict[str, Any], node_count: int = 3,
                           io_properties_profile: dict[str, Any] | None = None,
                           commitlog_sync: str = "batch",
                           commitlog_sync_period_in_ms: int | None = None) -> dict[str, Any]:
    required = {
        "developer_mode": "1",
        "io_setup": "0",
        "overprovisioned": "1",
        "thread_affinity": "1",
        "commitlog_use_o_dsync": "false",
        "commitlog_total_space_in_mb": "1024",
        "commitlog_segment_size_in_mb": "64",
        "prometheus_address": "0.0.0.0",
        "prometheus_port": "9180",
        "relabel_config_file": METRICS_RELABEL_CONTAINER_PATH,
        "max_networking_io_control_blocks": "1024",
        "reserve_io_control_blocks": "1024",
        "reactor_backend": "linux-aio",
    }
    required["commitlog_sync"] = commitlog_sync
    if commitlog_sync == "periodic":
        required["commitlog_sync_period_in_ms"] = str(commitlog_sync_period_in_ms)
    nodes, errors = config, []
    try:
        node_names = node_names_for(node_count)
    except RunnerError as exc:
        node_names = ()
        errors.append(str(exc))
    if set(nodes) != set(node_names):
        errors.append(f"launch config node inventory must be exactly {list(node_names)}, observed {sorted(nodes)}")
    io_enabled = (io_properties_profile or {}).get("enabled") is True
    cpus_by_node = CPU_ASSIGNMENTS
    shard_count = None
    for node in node_names:
        item = nodes.get(node, {})
        flags = item.get("parsed_launch_flags", {})
        for name, expected in required.items():
            actual = flags.get(name)
            if str(actual).lower() != expected:
                errors.append(f"{node}: {name} expected {expected!r}, got {actual!r}")
        if commitlog_sync == "batch" and flags.get("commitlog_sync_period_in_ms") is not None:
            errors.append(f"{node}: batch launch must not override commitlog_sync_period_in_ms")
        if (commitlog_sync == "periodic"
                and commitlog_sync_period_in_ms != PERIODIC_SYNC_DEFAULT_PERIOD_MS):
            errors.append("periodic launch profile requires commitlog_sync_period_in_ms=10000")
        io_mounts = [mount for mount in (item.get("host_config", {}).get("mounts") or [])
                     if mount.get("Destination") == IO_PROPERTIES_CONTAINER_PATH]
        if io_enabled:
            expected_path = io_properties_profile.get("container_path")
            expected_source = io_properties_profile.get("config_path")
            expected_hash = io_properties_profile.get("source_sha256")
            if flags.get("io_properties_file") != expected_path:
                errors.append(f"{node}: --io-properties-file must equal {expected_path!r}")
            if (len(io_mounts) != 1 or io_mounts[0].get("Source") != expected_source
                    or io_mounts[0].get("RW") is not False):
                errors.append(f"{node}: I/O properties must be the exact readonly profile mount")
            if (item.get("expected_io_properties_sha256") != expected_hash
                    or item.get("observed_io_properties_sha256") != expected_hash):
                errors.append(f"{node}: mounted I/O properties SHA-256 is missing or mismatched")
        elif io_mounts or flags.get("io_properties_file") is not None:
            errors.append(f"{node}: unexpected I/O properties mount/flag in legacy profile")
        host = item.get("host_config", {})
        try:
            current_shards = int(flags.get("smp", "0"))
            expected_cpus = set(cpus_by_node[current_shards][node])
            if shard_count is not None and shard_count != current_shards:
                errors.append(f"{node}: Scylla shard count differs from other nodes")
            shard_count = current_shards
            observed_cpus = {int(part) for part in str(host.get("cpuset_cpus", "")).split(",") if part}
            server_cpus = {int(part) for part in str(flags.get("cpuset", "")).split(",") if part}
        except (KeyError, TypeError, ValueError):
            expected_cpus, observed_cpus, server_cpus = set(), set(), set()
        if observed_cpus != expected_cpus:
            errors.append(f"{node}: Docker cpuset {observed_cpus} != expected {expected_cpus}")
        if server_cpus != expected_cpus:
            errors.append(f"{node}: Scylla --cpuset {server_cpus} != expected {expected_cpus}")
        if host.get("memory_bytes") != NODE_CGROUP_BYTES or host.get("memory_swap_bytes") != NODE_CGROUP_BYTES:
            errors.append(f"{node}: cgroup memory/swap limits do not match the 10 GiB profile")
        if any(host.get(key) not in (None, 0) for key in ("cpu_quota", "cpu_period", "nano_cpus")):
            errors.append(f"{node}: Docker CPU quota is enabled")
        if host.get("port_bindings") not in (None, {}):
            errors.append(f"{node}: container has published host ports")
        if not str(host.get("network_mode", "")).startswith("lw-scylla-"):
            errors.append(f"{node}: container is not on its isolated owned network")
        mounts = host.get("mounts") or []
        expected_source = str(item.get("expected_owned_data_source", ""))
        data_mounts = [mount for mount in mounts
                       if mount.get("Destination") == "/var/lib/scylla"]
        if (len(data_mounts) != 1 or data_mounts[0].get("Source") != expected_source
                or data_mounts[0].get("RW") is not True):
            errors.append(f"{node}: Scylla data mount is not the exact writable owned-data directory")
        expected_relabel_source = str(item.get("expected_metrics_relabel_source", ""))
        expected_relabel_hash = item.get("expected_metrics_relabel_sha256")
        observed_relabel_hash = item.get("observed_metrics_relabel_sha256")
        if (not expected_relabel_hash or observed_relabel_hash != expected_relabel_hash
                or expected_relabel_hash != hashlib.sha256(METRICS_RELABEL_CONFIG.encode()).hexdigest()):
            errors.append(f"{node}: metrics relabel config hash is missing or changed")
        relabel_mounts = [mount for mount in mounts
                          if mount.get("Destination") == METRICS_RELABEL_CONTAINER_PATH]
        if (len(relabel_mounts) != 1
                or relabel_mounts[0].get("Source") != expected_relabel_source
                or relabel_mounts[0].get("RW") is not False):
            errors.append(f"{node}: metrics relabel config must be the exact read-only trial bind mount")
    if shard_count is None:
        shard_count = 0
    return {"valid": not errors, "required_launch_flags": required, "errors": errors,
            "node_count": node_count, "expected_nodes": list(node_names),
            "shards": shard_count, "io_properties_enabled": io_enabled,
            "commitlog_sync": commitlog_sync,
            "commitlog_sync_period_in_ms": commitlog_sync_period_in_ms,
            "nodes": config}


def thread_affinity_evidence(pid: int, expected_cpus: Sequence[int]) -> dict[str, Any]:
    task_root = pathlib.Path(f"/proc/{pid}/task")
    try:
        tids = sorted(task_root.iterdir(), key=lambda path: int(path.name))
    except (OSError, ValueError) as exc:
        return {"valid": False, "error": str(exc), "tasks": []}
    rows = []
    errors = []
    for task in tids:
        try:
            tid = int(task.name)
            comm = (task / "comm").read_text().strip()
            status = (task / "status").read_text()
            affinity_text = next((line.split(":", 1)[1].strip()
                                  for line in status.splitlines()
                                  if line.startswith("Cpus_allowed_list:")), "")
            rows.append({"tid": tid, "comm": comm, "affinity_text": affinity_text})
        except (OSError, ValueError) as exc:
            errors.append(f"cannot read task {task}: {exc}")
    evidence = validate_thread_affinity_rows(rows, pid, expected_cpus)
    evidence["errors"] = errors + evidence["errors"]
    evidence["valid"] = not evidence["errors"]
    return evidence


def validate_thread_affinity_rows(rows: Sequence[dict[str, Any]], pid: int,
                                  expected_cpus: Sequence[int]) -> dict[str, Any]:
    tasks: list[dict[str, Any]] = []
    errors = []
    expected = set(expected_cpus)
    for row in rows:
        try:
            tid = int(row["tid"])
            comm = str(row["comm"])
            affinity_text = str(row["affinity_text"])
            affinity = parse_cpu_list(affinity_text)
            is_reactor = tid == pid or "reactor" in comm.lower()
            tasks.append({"tid": tid, "comm": comm, "cpus_allowed_list": affinity_text,
                          "cpus": sorted(affinity), "reactor_candidate": is_reactor,
                          "reactor_identity": ("main-reactor-0" if tid == pid else
                                                "named-reactor" if is_reactor else None)})
            if not affinity or not affinity.issubset(expected):
                errors.append(f"TID {tid} affinity {affinity} escapes expected cpuset {expected}")
        except (KeyError, TypeError, ValueError) as exc:
            errors.append(f"invalid thread affinity row {row}: {exc}")
    reactors = [task for task in tasks if task["reactor_candidate"]]
    reactor_cpus = [task["cpus"] for task in reactors]
    if len(reactors) != len(expected_cpus):
        errors.append(f"found {len(reactors)} reactor threads for {len(expected_cpus)} shards")
    if any(len(cpus) != 1 for cpus in reactor_cpus):
        errors.append("each reactor thread must have a single-CPU affinity mask")
    observed_reactor_cpus = {cpu for cpus in reactor_cpus for cpu in cpus}
    if observed_reactor_cpus != expected:
        errors.append(f"reactor CPU coverage {observed_reactor_cpus} != expected {expected}")
    return {"valid": not errors, "expected_cpus": sorted(expected), "thread_count": len(tasks),
            "reactor_thread_count": len(reactors),
            "reactor_cpu_coverage": sorted(observed_reactor_cpus),
            "tasks": tasks, "errors": errors}


def parse_cpu_list(value: str) -> set[int]:
    cpus: set[int] = set()
    for item in value.split(","):
        if not item:
            continue
        if "-" in item:
            lo_text, hi_text = item.split("-", 1)
            lo, hi = int(lo_text), int(hi_text)
            cpus.update(range(lo, hi + 1))
        else:
            cpus.add(int(item))
    return cpus


def _query_system_config(cluster: OwnedCluster, record: ContainerRecord) -> dict[str, Any]:
    names = ("commitlog_sync", "commitlog_sync_period_in_ms",
             "commitlog_sync_batch_window_in_ms", "commitlog_use_o_dsync",
             "commitlog_total_space_in_mb", "commitlog_segment_size_in_mb")
    evidence = {}
    for name in names:
        query = f"SELECT JSON name, value FROM system.config WHERE name = '{name}'"
        result = _run(["docker", "exec", record.container_id, "cqlsh", record.ip, "9042",
                       "--execute", f"CONSISTENCY ONE; {query}"], check=False, timeout=15)
        output = result.stdout or ""
        objects = _json_objects_in_text(output)
        item = {"exit_code": result.returncode, "output": output,
                "rows": objects, "error": result.stderr}
        if name == "commitlog_sync":
            item["json_parser_gap"] = bool(
                result.returncode == 0 and not objects
                and re.search(r"\(\s*1\s+rows?\s*\)", output, re.IGNORECASE))
            item["json_parse_status"] = (
                "query_error" if result.returncode != 0 else
                "parsed_rows" if objects else
                "parser_gap" if item["json_parser_gap"] else "no_parsed_rows")
            plain_query = "SELECT name, value FROM system.config WHERE name = 'commitlog_sync'"
            plain_execute = f"CONSISTENCY ONE; {plain_query}"
            plain_result = _run(["docker", "exec", record.container_id, "cqlsh", record.ip, "9042",
                                 "--execute", plain_execute],
                                check=False, timeout=15)
            item["plain_probe"] = parse_commitlog_sync_plain_probe(
                plain_result.returncode, plain_result.stdout or "", plain_result.stderr or "",
                query=plain_execute)
        evidence[name] = item
    return evidence


def parse_commitlog_sync_plain_probe(exit_code: int, output: str, stderr: str = "",
                                     query: str | None = None) -> dict[str, Any]:
    """Parse one exact cqlsh name/value table row for commitlog_sync."""
    result: dict[str, Any] = {
        "query": query or "SELECT name, value FROM system.config WHERE name = 'commitlog_sync'",
        "consistency_level": "ONE",
        "exit_code": exit_code,
        "output": output,
        "stderr": stderr,
        "header": None,
        "rows": [],
        "name": None,
        "raw_value": None,
        "normalized_value": None,
        "valid": False,
        "errors": [],
    }
    errors: list[str] = result["errors"]
    if exit_code != 0:
        errors.append(f"plain CQL query exited with status {exit_code}")
    lines = output.splitlines()
    header_indices = [index for index, line in enumerate(lines)
                      if [part.strip() for part in line.split("|")] == ["name", "value"]]
    if len(header_indices) != 1:
        errors.append("plain CQL output must contain exactly one `name | value` header")
    else:
        header_index = header_indices[0]
        result["header"] = ["name", "value"]
        rows: list[dict[str, str]] = []
        footers: list[int] = []
        for line in lines[header_index + 1:]:
            stripped = line.strip()
            if not stripped:
                continue
            if re.fullmatch(r"[-+]+", stripped):
                continue
            footer = re.fullmatch(r"\(\s*(\d+)\s+rows?\s*\)", stripped, re.IGNORECASE)
            if footer:
                footers.append(int(footer.group(1)))
                continue
            if "|" not in line:
                errors.append(f"unexpected text after plain CQL header: {stripped!r}")
                continue
            columns = [part.strip() for part in line.split("|")]
            if len(columns) != 2:
                errors.append(f"plain CQL row must have exactly two columns: {stripped!r}")
                continue
            rows.append({"name": columns[0], "raw_value": columns[1]})
        result["rows"] = rows
        if len(rows) != 1:
            errors.append(f"plain CQL output must contain exactly one data row; got {len(rows)}")
        if footers != [1]:
            errors.append(f"plain CQL output must end with an exact one-row footer; got {footers!r}")
        if len(rows) == 1:
            row = rows[0]
            result["name"] = row["name"]
            result["raw_value"] = row["raw_value"]
            if row["name"] != "commitlog_sync":
                errors.append(f"plain CQL row name must be commitlog_sync; got {row['name']!r}")
            try:
                value = json.loads(row["raw_value"])
            except json.JSONDecodeError as exc:
                errors.append(f"plain CQL value is not one JSON scalar: {exc.msg}")
            else:
                if not isinstance(value, str) or value not in ("batch", "periodic"):
                    errors.append(f"plain CQL value must be batch or periodic; got {value!r}")
                else:
                    result["normalized_value"] = value
    if exit_code == 0 and result["normalized_value"] is None and not errors:
        errors.append("plain CQL output did not provide a recognized commitlog_sync value")
    result["valid"] = not errors and result["normalized_value"] in ("batch", "periodic")
    return result


def _runtime_config_evidence(cluster: OwnedCluster) -> dict[str, Any]:
    nodes = {}
    for record in cluster.records:
        nodes[record.node] = {
            "api10000": cluster._api_config(record),
            "system_config_virtual_table": _query_system_config(cluster, record),
        }
    return {"nodes": nodes}


def _json_lookup(value: Any, name: str) -> Any:
    if isinstance(value, dict):
        for key, nested in value.items():
            if str(key).replace("-", "_").lower() == name.replace("-", "_").lower():
                return nested
        for nested in value.values():
            found = _json_lookup(nested, name)
            if found is not None:
                return found
    elif isinstance(value, list):
        for nested in value:
            found = _json_lookup(nested, name)
            if found is not None:
                return found
    return None


def _system_config_value(evidence: dict[str, Any], name: str) -> Any:
    for row in evidence.get("rows", []):
        if str(row.get("name", row.get("NAME", ""))).lower() == name.lower():
            return row.get("value", row.get("VALUE"))
    return None


def _normalize_system_commitlog_sync(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    candidate = value.strip()
    if candidate in ("batch", "periodic"):
        return candidate
    try:
        decoded = json.loads(candidate)
    except json.JSONDecodeError:
        return None
    return decoded if isinstance(decoded, str) and decoded in ("batch", "periodic") else None


def validate_runtime_commitlog_config(runtime: dict[str, Any],
                                      node_names: Sequence[str] = NODE_NAMES,
                                      commitlog_sync: str = "batch",
                                      commitlog_sync_period_in_ms: int | None = None) -> dict[str, Any]:
    expected: dict[str, str] = {
        "commitlog_sync": commitlog_sync,
        "commitlog_use_o_dsync": "false",
        "commitlog_total_space_in_mb": "1024",
        "commitlog_segment_size_in_mb": "64",
        "commitlog_sync_batch_window_in_ms": "10000",
        "commitlog_sync_period_in_ms": str(commitlog_sync_period_in_ms or PERIODIC_SYNC_DEFAULT_PERIOD_MS),
    }
    result: dict[str, Any] = {"expected": expected, "commitlog_sync": commitlog_sync,
                              "commitlog_sync_period_in_ms": commitlog_sync_period_in_ms,
                              "nodes": {}, "valid": True}
    problems = []
    for node in node_names:
        item = runtime.get("nodes", {}).get(node, {})
        virtual = item.get("system_config_virtual_table", {})
        api_probes = item.get("api10000", {}).get("probes", [])
        api_values: dict[str, Any] = {}
        for probe in api_probes:
            if probe.get("status") != 200 or not probe.get("config_name"):
                continue
            try:
                api_values[probe["config_name"]] = json.loads(probe.get("body", ""))
            except json.JSONDecodeError:
                api_values[probe["config_name"]] = probe.get("body", "").strip().strip('"')
        observed = {}
        evidence_by_name = {}
        for name, expected_value in expected.items():
            query = virtual.get(name, {})
            api_value = api_values.get(name)
            json_system_value = _system_config_value(query, name)
            plain_probe = query.get("plain_probe") if name == "commitlog_sync" else None
            plain_valid = isinstance(plain_probe, dict) and plain_probe.get("valid") is True
            system_value = (plain_probe.get("normalized_value") if plain_valid
                            else json_system_value)
            value = api_value if api_value is not None else system_value
            source = ("api10000:/v2/config" if api_value is not None else
                      "system.config:plain-cql" if name == "commitlog_sync" and plain_valid else
                      "system.config:json" if system_value is not None else None)
            normalized = str(value).strip().lower() if value is not None else None
            api_normalized = str(api_value).strip().lower() if api_value is not None else None
            system_normalized = (_normalize_system_commitlog_sync(system_value)
                                 if name == "commitlog_sync" else
                                 str(system_value).strip().lower()
                                 if system_value is not None else None)
            json_system_normalized = (
                _normalize_system_commitlog_sync(json_system_value)
                if name == "commitlog_sync" else
                str(json_system_value).strip().lower() if json_system_value is not None else None)
            observed[name] = value
            evidence_by_name[name] = {"value": value, "normalized": normalized,
                                      "source": source,
                                      "api_value": api_value,
                                      "system_config_value": system_value,
                                      "system_config_json_value": json_system_value,
                                      "system_config_json_parser_gap": query.get("json_parser_gap", False),
                                      "system_config_json_parse_status": query.get("json_parse_status"),
                                      "system_config_plain_probe": plain_probe,
                                      "exit_code": query.get("exit_code"),
                                      "raw_output": query.get("output")}
            if normalized != expected_value:
                problems.append(f"{node}: {name} expected {expected_value}, got {value!r}")
            if (name == "commitlog_sync" and plain_valid and json_system_value is not None
                    and json_system_normalized != system_normalized):
                problems.append(
                    f"{node}: plain and JSON system.config commitlog_sync probes disagree "
                    f"({system_normalized!r} != {json_system_normalized!r})")
            if name in ("commitlog_sync", "commitlog_use_o_dsync") and name not in api_values:
                problems.append(f"{node}: {name} was not verified through Admin REST API v2")
            if (commitlog_sync == "periodic"
                    and name in ("commitlog_sync", "commitlog_sync_period_in_ms",
                                 "commitlog_use_o_dsync")):
                if api_normalized != expected_value:
                    problems.append(f"{node}: {name} periodic profile was not verified through Admin REST API v2")
                if system_normalized != expected_value:
                    problems.append(f"{node}: {name} periodic profile was not verified through system.config")
                if name == "commitlog_sync" and not plain_valid:
                    errors = (plain_probe.get("errors", []) if isinstance(plain_probe, dict)
                              else ["plain CQL probe is missing"])
                    problems.append(
                        f"{node}: commitlog_sync plain system.config probe failed: "
                        + "; ".join(str(error) for error in errors))
        result["nodes"][node] = {"observed": observed, "evidence": evidence_by_name}
    result["problems"] = problems
    result["valid"] = not problems and set(result["nodes"]) == set(node_names)
    return result


def _cli_binary_path(args: argparse.Namespace, run_dir: pathlib.Path) -> pathlib.Path:
    if args.binary:
        binary = args.binary.expanduser().resolve(strict=True)
        if not binary.is_file() or not os.access(binary, os.X_OK):
            raise RunnerError(f"binary is not executable: {binary}")
        return binary
    target_dir = REPO_ROOT / "target" / "scylla-wallet"
    candidate = target_dir / "release" / RUST_PACKAGE
    if os.name == "nt":
        candidate = candidate.with_suffix(".exe")
    if args.skip_build:
        if not candidate.is_file():
            raise RunnerError(f"--skip-build requested but binary is missing: {candidate}")
        return candidate.resolve()
    build_log = run_dir / "build.log"
    env = dict(os.environ)
    env["CARGO_TARGET_DIR"] = str(target_dir)
    with build_log.open("w", encoding="utf-8") as output:
        result = subprocess.run(
            ["cargo", "build", "--locked", "--release", "--manifest-path",
             str(CRATE_DIR / "Cargo.toml")],
            cwd=REPO_ROOT, env=env, text=True, stdout=output, stderr=subprocess.STDOUT, check=False)
    if result.returncode:
        raise RunnerError(f"cargo build failed; see {build_log}")
    if not candidate.is_file():
        raise RunnerError(f"build succeeded but expected binary is missing: {candidate}")
    return candidate.resolve()


def _keyspace_name() -> str:
    return "wallet_bench_" + uuid.uuid4().hex[:12]


def _cli_command(binary: pathlib.Path, endpoints: Sequence[str], keyspace: str,
                 case: TrialCase, users: int, requests_per_user: int, queue_capacity: int,
                 mode: str, output_dir: pathlib.Path, gate: pathlib.Path,
                 finish_gate: pathlib.Path) -> list[str]:
    command = [str(binary), "--nodes", ",".join(endpoints), "--keyspace", keyspace,
            "--shards", str(case.shards), "--db-workers", str(case.workers),
            "--users", str(users), "--requests-per-user", str(requests_per_user),
            "--queue-capacity", str(queue_capacity), "--output-dir", str(output_dir),
            "--mode", mode, "--start-gate", str(gate),
            "--finish-gate", str(finish_gate)]
    # Omit the new default flags for compatibility with the reviewed legacy CLI.
    if case.per_shard_pool_overridden:
        command += ["--per-shard-pool", str(case.per_shard_pool)]
    if case.worker_routing != "round-robin":
        command += ["--worker-routing", case.worker_routing]
    if (case.replication_factor, case.read_cl, case.write_cl) != (3, "THREE", "THREE"):
        command += ["--replication-factor", str(case.replication_factor),
                    "--read-cl", str(CONSISTENCY_LEVELS[case.read_cl]),
                    "--write-cl", str(CONSISTENCY_LEVELS[case.write_cl])]
    return command


def _event_value(event: dict[str, Any], name: str) -> Any:
    if name in event:
        return event[name]
    metadata = event.get("metadata")
    return metadata.get(name) if isinstance(metadata, dict) else None


def _event_int(event: dict[str, Any], name: str) -> int:
    value = _event_value(event, name)
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise RunnerError(f"CLI event {event.get('phase')} is missing integer {name}") from exc


def _validate_profile_fields(document: dict[str, Any], case: TrialCase, context: str,
                             *, required: bool,
                             fields: Sequence[str] | None = None) -> None:
    """Check the CLI's worker and physical-pool contract against this trial."""
    expected: dict[str, Any] = {
        "C": case.workers,
        "S": case.shards,
        "P": case.per_shard_pool,
        "data_connection_target": case.data_connection_target,
        "worker_routing": case.worker_routing,
        "per_shard_pool_overridden": case.per_shard_pool_overridden,
        "requested_per_shard_pool": case.requested_per_shard_pool,
        "setup_concurrency": case.setup_concurrency,
        "per_worker_queue_capacity": case.per_worker_queue_capacity,
    }
    for name, want in expected.items():
        if fields is not None and name not in fields:
            continue
        if name not in document:
            if required:
                raise RunnerError(f"{context} is missing required profile field {name}")
            continue
        actual = document[name]
        if actual != want:
            raise RunnerError(f"{context} {name}={actual!r} does not match {want!r}")


def _case_has_nondefault_durability(case: TrialCase) -> bool:
    return (case.replication_factor, case.read_cl, case.write_cl) != (3, "THREE", "THREE")


def _validate_durability_fields(document: dict[str, Any], case: TrialCase,
                                context: str, *, required: bool) -> dict[str, Any]:
    expected = {"replication_factor": case.replication_factor,
                "read_cl": case.read_cl, "write_cl": case.write_cl}
    observed: dict[str, Any] = {}
    for name, want in expected.items():
        if name not in document:
            if required:
                raise RunnerError(f"{context} is missing required durability field {name}")
            continue
        actual = document[name]
        observed[name] = actual
        if actual != want:
            raise RunnerError(f"{context} {name}={actual!r} does not match {want!r}")
    return observed


def _document_field(document: dict[str, Any], name: str) -> Any:
    return _event_value(document, name)


def _contains_json_key(value: Any, key: str) -> bool:
    if isinstance(value, dict):
        return key in value or any(_contains_json_key(item, key) for item in value.values())
    if isinstance(value, list):
        return any(_contains_json_key(item, key) for item in value)
    return False


def _validate_no_transaction_at(value: Any, context: str) -> None:
    if _contains_json_key(value, "transaction_at"):
        raise RunnerError(f"{context} contains forbidden transaction_at metadata")


def _validate_wallet_profile_fields(document: dict[str, Any], context: str,
                                    *, account_state_source: str | None = None) -> dict[str, Any]:
    expected = {
        "wallet_profile": WALLET_PROFILE,
        "ledger_key": list(LEDGER_KEY),
        "serial_cl": SERIAL_CL,
        "cql_requests_per_transaction": CQL_REQUESTS_PER_TRANSACTION,
        "cql_statement_count": CQL_STATEMENTS_PER_TRANSACTION,
    }
    if account_state_source is not None:
        expected["account_state_source"] = account_state_source
    observed: dict[str, Any] = {}
    for name, want in expected.items():
        actual = _document_field(document, name)
        if actual is None:
            raise RunnerError(f"{context} is missing required wallet profile field {name}")
        observed[name] = actual
        if actual != want or (isinstance(want, int) and
                              (isinstance(actual, bool) or not isinstance(actual, int))):
            raise RunnerError(f"{context} {name}={actual!r} does not match {want!r}")
    if account_state_source is None:
        source = _document_field(document, "account_state_source")
        if source not in ("db", "memory"):
            raise RunnerError(f"{context} account_state_source must be db or memory, observed {source!r}")
        observed["account_state_source"] = source
    return observed


def validate_preloading_contract(events: Sequence[dict[str, Any]], case: TrialCase,
                                 users: int) -> dict[str, Any]:
    """Require bounded DB preload completion before the measurement-ready gate."""
    preloading = [(index, event) for index, event in enumerate(events)
                  if event.get("phase") == "preloading"]
    started = [(index, event) for index, event in preloading
               if event.get("status") == "started"]
    completed = [(index, event) for index, event in preloading
                 if event.get("status") == "complete"]
    if len(started) != 1 or len(completed) != 1:
        raise RunnerError("CLI must emit exactly one started and one complete preloading event")
    started_index, started_event = started[0]
    completed_index, complete_event = completed[0]
    if started_index >= completed_index:
        raise RunnerError("preloading complete event must follow its started event")
    for label, event in (("preloading started", started_event),
                         ("preloading complete", complete_event)):
        _validate_wallet_profile_fields(event, label, account_state_source="db")
        _validate_no_transaction_at(event, label)
        if _event_value(event, "state_preload_concurrency") != case.setup_concurrency:
            raise RunnerError(
                f"{label} state_preload_concurrency must be {case.setup_concurrency}")
    count = _event_value(complete_event, "account_states_preloaded")
    if isinstance(count, bool) or count != users:
        raise RunnerError(f"preloading completed {count!r} account states; expected {users}")
    ready = [(index, event) for index, event in enumerate(events)
             if event.get("phase") == "ready_to_measure"]
    if len(ready) != 1:
        raise RunnerError("CLI must emit exactly one ready_to_measure event")
    ready_index, ready_event = ready[0]
    if ready_index <= completed_index:
        raise RunnerError("all account states must be preloaded before ready_to_measure")
    _validate_wallet_profile_fields(ready_event, "ready_to_measure event",
                                    account_state_source="memory")
    return {"valid": True, "account_states_preloaded": count,
            "state_preload_concurrency": case.setup_concurrency,
            "wallet_profile": WALLET_PROFILE,
            "preloading_started_event_index": started_index,
            "preloading_complete_event_index": completed_index,
            "ready_event_index": ready_index}


def validate_lwt_event_contract(events: Sequence[dict[str, Any]], case: TrialCase,
                                users: int, requests_per_user: int) -> dict[str, Any]:
    if not events:
        raise RunnerError("CLI emitted no events")
    _validate_no_transaction_at(events, "CLI events")
    preload_evidence = validate_preloading_contract(events, case, users)
    for index, event in enumerate(events):
        phase = event.get("phase", "unknown")
        _validate_wallet_profile_fields(
            event, f"CLI {phase} event",
            account_state_source="db" if phase in ("seeding", "preloading") else None)
        if phase not in ("seeding", "preloading") and index >= preload_evidence["preloading_complete_event_index"]:
            if _event_value(event, "account_state_source") != ACCOUNT_STATE_SOURCE:
                raise RunnerError(f"CLI {phase} event must use account_state_source=memory")
    return {**preload_evidence, "events_checked": len(events),
            "total_requests_expected": users * requests_per_user,
            "wallet_profile": WALLET_PROFILE}


def validate_lwt_result_contract(result: dict[str, Any], case: TrialCase,
                                 users: int | None = None,
                                 requests_per_user: int | None = None) -> dict[str, Any]:
    """Verify one measured LWT CQL request per acceptance and memory-cache final checking."""
    _validate_no_transaction_at(result, "CLI result")
    config = result.get("config")
    if not isinstance(config, dict):
        raise RunnerError("CLI result.json is missing its config object")
    profile = _validate_wallet_profile_fields(
        config, "CLI result config", account_state_source=ACCOUNT_STATE_SOURCE)
    account_count = config.get("account_states_preloaded")
    setup_concurrency = config.get("state_preload_concurrency")
    config_users = config.get("users")
    if isinstance(account_count, bool) or not isinstance(account_count, int) or account_count <= 0:
        raise RunnerError("CLI result config account_states_preloaded is not a positive integer")
    expected_users = users if users is not None else config_users
    if isinstance(expected_users, bool) or not isinstance(expected_users, int) or expected_users <= 0:
        raise RunnerError("expected users is not a positive integer")
    if account_count != expected_users or config_users != expected_users:
        raise RunnerError(
            f"CLI preloaded {account_count} account states for users={config_users}; expected {expected_users}")
    if setup_concurrency != case.setup_concurrency:
        raise RunnerError("CLI result state_preload_concurrency does not match min(C,D)")
    for name, expected in (("measured_select_requests", 0),
                           ("measured_cql_requests_per_transaction", 1)):
        if config.get(name) != expected:
            raise RunnerError(f"CLI result config {name}={config.get(name)!r}, expected {expected}")

    requests = result.get("requests")
    if not isinstance(requests, dict):
        raise RunnerError("CLI result is missing request counts")
    accepted = requests.get("accepted")
    if isinstance(accepted, bool) or not isinstance(accepted, int) or accepted < 0:
        raise RunnerError("CLI result requests.accepted is not a nonnegative integer")
    expected_request_count = requests.get("expected")
    if (isinstance(expected_request_count, bool)
            or not isinstance(expected_request_count, int)
            or expected_request_count != accepted):
        raise RunnerError("valid LWT workload must accept every expected request")
    expected_total = (expected_users * requests_per_user
                      if requests_per_user is not None else config.get("total_requests_expected"))
    if isinstance(expected_total, bool) or not isinstance(expected_total, int) or expected_total <= 0:
        raise RunnerError("CLI result expected transaction count is missing or invalid")
    if accepted != expected_total:
        raise RunnerError(f"CLI accepted {accepted} requests; expected {expected_total}")

    query_accounting = result.get("query_accounting", requests.get("query_accounting"))
    if not isinstance(query_accounting, dict):
        raise RunnerError("CLI result is missing query_accounting evidence")
    expected_accounting = {
        "measured_select_requests": 0,
        "measured_cql_request_count": accepted,
        "cql_requests_per_transaction": CQL_REQUESTS_PER_TRANSACTION,
        "cql_statement_count": CQL_STATEMENTS_PER_TRANSACTION,
    }
    for name, expected in expected_accounting.items():
        if query_accounting.get(name) != expected:
            raise RunnerError(
                f"CLI query_accounting.{name}={query_accounting.get(name)!r}, expected {expected}")

    driver_delta = ((result.get("driver_counters") or {}).get("delta") or {})
    if driver_delta.get("queries") != accepted:
        raise RunnerError(
            f"driver query delta={driver_delta.get('queries')!r}; expected one LWT query per accepted request ({accepted})")
    if driver_delta.get("errors") != 0:
        raise RunnerError(f"driver errors during a valid workload: {driver_delta.get('errors')!r}")

    stage_metrics = result.get("stage_metrics")
    counters = stage_metrics.get("counters") if isinstance(stage_metrics, dict) else None
    if not isinstance(counters, dict):
        raise RunnerError("CLI result is missing measured CQL stage counters")
    if counters.get("cql_write_lwt") != accepted:
        raise RunnerError(
            f"measured cql_write_lwt={counters.get('cql_write_lwt')!r}; expected {accepted}")
    for name, value in counters.items():
        if name.startswith(("cql_read_", "cql_preload_", "cql_verify_")):
            if isinstance(value, bool) or not isinstance(value, int) or value != 0:
                raise RunnerError(
                    f"setup/final SELECT counter {name} leaked into measurement: {value!r}")
    for name in ("cql_read_metadata", "cql_read_ledger"):
        value = counters.get(name, 0)
        if isinstance(value, bool) or not isinstance(value, int) or value != 0:
            raise RunnerError(f"measured {name}={value!r}; expected 0")
    if counters.get("cql_write_batch", 0) != 0:
        raise RunnerError("memory-LWT profile must not issue ordinary CQL batches")
    if counters.get("accepted_requests") != accepted:
        raise RunnerError("measured accepted_requests counter does not match request result")

    verification = result.get("verification")
    if not isinstance(verification, dict):
        raise RunnerError("CLI result is missing final account verification evidence")
    expected_verification = {
        "complete": True, "valid": True,
        "account_state_source": "memory_cache_vs_scylla",
        "memory_cache_accounts_compared": expected_users,
        "accounts_expected": expected_users,
        "accounts_verified": expected_users,
        "ledger_rows_expected": expected_total,
        "ledger_rows_seen": expected_total,
        "failures": 0,
    }
    for name, expected in expected_verification.items():
        if verification.get(name) != expected:
            raise RunnerError(
                f"CLI verification.{name}={verification.get(name)!r}, expected {expected!r}")
    return {"valid": True, **profile,
            "account_states_preloaded": account_count,
            "state_preload_concurrency": setup_concurrency,
            "accepted_requests": accepted,
            "measured_lwt_requests": counters["cql_write_lwt"],
            "measured_select_requests": 0,
            "driver_query_delta": driver_delta["queries"],
            "memory_cache_accounts_compared": expected_users,
            "ledger_rows_verified": expected_total}


def validate_ready_event_profile(ready: dict[str, Any], case: TrialCase) -> dict[str, Any]:
    """Reject a ready event that reports a different worker/pool contract."""
    _validate_no_transaction_at(ready, "ready_to_measure event")
    wallet_profile = _validate_wallet_profile_fields(
        ready, "ready_to_measure event", account_state_source=ACCOUNT_STATE_SOURCE)
    pool_ready = _event_value(ready, "pool_ready_connections")
    if pool_ready is None or int(pool_ready) != case.data_connection_target:
        raise RunnerError(
            f"CLI driver pool reports {pool_ready!r} ready data connections; "
            f"expected D={case.data_connection_target}")
    explicit_profile = (case.per_shard_pool_overridden or
                        case.worker_routing != "round-robin")
    names = ("C", "S", "P", "data_connection_target", "worker_routing",
             "per_shard_pool_overridden", "requested_per_shard_pool",
             "setup_concurrency", "per_worker_queue_capacity",
             "replication_factor", "read_cl", "write_cl")
    profile = {}
    for name in names:
        value = _event_value(ready, name)
        if value is not None or (name == "requested_per_shard_pool" and
                                 (name in ready or isinstance(ready.get("metadata"), dict) and
                                  name in ready["metadata"])):
            profile[name] = value
    _validate_profile_fields(profile, case, "ready_to_measure event",
                             required=explicit_profile)
    _validate_durability_fields(
        profile, case, "ready_to_measure event",
        required=_case_has_nondefault_durability(case))
    # Legacy clients still have to expose their pool target and P; these fields
    # are part of the existing runner/CLI handshake rather than optional hints.
    if profile.get("P") != case.per_shard_pool:
        raise RunnerError(f"CLI P value does not match requested per-shard pool={case.per_shard_pool}")
    if profile.get("data_connection_target") != case.data_connection_target:
        raise RunnerError(f"CLI data_connection_target does not match D={case.node_count}*S*P")
    return {"C": case.workers, "S": case.shards, "P": case.per_shard_pool,
            "D": case.data_connection_target, "worker_routing": case.worker_routing,
            "replication_factor": case.replication_factor,
            "read_cl": case.read_cl, "write_cl": case.write_cl,
            "pool_ready_connections": pool_ready, **wallet_profile}


def validate_cli_event_profiles(events: Sequence[dict[str, Any]],
                                case: TrialCase,
                                users: int | None = None,
                                requests_per_user: int | None = None) -> dict[str, Any]:
    """Check profile fields on each flushed event, not just the ready event."""
    explicit_profile = (case.per_shard_pool_overridden or
                        case.worker_routing != "round-robin")
    names = ("C", "S", "P", "data_connection_target", "worker_routing",
             "per_shard_pool_overridden", "requested_per_shard_pool",
             "setup_concurrency", "per_worker_queue_capacity",
             "replication_factor", "read_cl", "write_cl")
    checked_phases = []
    for event in events:
        profile = {}
        metadata = event.get("metadata")
        phase = event.get("phase", "unknown")
        for name in names:
            if name in event:
                profile[name] = event[name]
            elif isinstance(metadata, dict) and name in metadata:
                profile[name] = metadata[name]
        state_source = "db" if phase in ("seeding", "preloading") else None
        _validate_wallet_profile_fields(event, f"CLI {phase} event",
                                        account_state_source=state_source)
        _validate_no_transaction_at(event, f"CLI {phase} event")
        _validate_profile_fields(profile, case,
                                 f"CLI {phase} event",
                                 required=explicit_profile)
        _validate_durability_fields(
            profile, case, f"CLI {phase} event",
            required=_case_has_nondefault_durability(case))
        checked_phases.append(phase)
    preload_evidence = None
    if users is not None and requests_per_user is not None:
        preload_evidence = validate_lwt_event_contract(
            events, case, users, requests_per_user)
    elif users is not None:
        preload_evidence = validate_preloading_contract(events, case, users)
    return {"valid": True, "events_checked": len(checked_phases),
            "phases": checked_phases, "C": case.workers, "S": case.shards,
            "P": case.per_shard_pool, "D": case.data_connection_target,
            "worker_routing": case.worker_routing,
            "replication_factor": case.replication_factor,
            "read_cl": case.read_cl, "write_cl": case.write_cl,
            "wallet_profile": WALLET_PROFILE,
            "preloading": preload_evidence}


def validate_cli_result_profile(result: dict[str, Any], case: TrialCase,
                                users: int | None = None,
                                requests_per_user: int | None = None,
                                expected_nodes: Sequence[str] | None = None) -> dict[str, Any]:
    """Validate the final service config and physical connection pool evidence."""
    lwt_contract = validate_lwt_result_contract(
        result, case, users=users, requests_per_user=requests_per_user)
    explicit_profile = (case.per_shard_pool_overridden or
                        case.worker_routing != "round-robin")
    config = result.get("config")
    if not isinstance(config, dict):
        raise RunnerError("CLI result.json is missing its config object")
    observed_nodes = config.get("nodes")
    observed_node_count = config.get("node_count")
    if (isinstance(observed_node_count, bool) or observed_node_count != case.node_count
            or not isinstance(observed_nodes, list)
            or len(observed_nodes) != case.node_count
            or any(not isinstance(node, str) or not node for node in observed_nodes)):
        raise RunnerError(
            f"CLI result config node inventory/count must contain exactly {case.node_count} nodes")
    if expected_nodes is not None and observed_nodes != list(expected_nodes):
        raise RunnerError(
            f"CLI result config nodes {observed_nodes!r} do not match cluster endpoints {list(expected_nodes)!r}")
    _validate_profile_fields(config, case, "CLI result config", required=explicit_profile)
    durability = _validate_durability_fields(
        config, case, "CLI result config", required=_case_has_nondefault_durability(case))
    for name in ("queue_capacity", "global_admission_capacity"):
        if name not in config:
            if explicit_profile:
                raise RunnerError(f"CLI result config is missing required field {name}")
        elif config[name] != QUEUE_CAPACITY:
            raise RunnerError(f"CLI result config {name}={config[name]!r} is not {QUEUE_CAPACITY}")
    pool = result.get("connection_pool")
    if not isinstance(pool, dict):
        raise RunnerError("CLI result.json is missing its connection_pool object")
    pool_profile = {
        "C": pool.get("C"), "S": pool.get("S"), "P": pool.get("P"),
        "data_connection_target": pool.get("target_total_data_connections"),
        "worker_routing": pool.get("worker_routing"),
        "per_shard_pool_overridden": pool.get("per_shard_pool_overridden"),
        "requested_per_shard_pool": pool.get("requested_per_shard_pool"),
        "setup_concurrency": pool.get("setup_concurrency"),
    }
    _validate_profile_fields(pool_profile, case, "CLI connection_pool report",
                             required=explicit_profile,
                             fields=("C", "S", "P", "data_connection_target",
                                     "worker_routing", "per_shard_pool_overridden",
                                     "requested_per_shard_pool", "setup_concurrency"))
    if pool.get("target_total_data_connections") != case.data_connection_target:
        raise RunnerError(f"CLI connection_pool target is not D={case.node_count}*S*P")
    for name in ("pool_ready_connections", "pool_after_last_reply"):
        if pool.get(name) != case.data_connection_target:
            raise RunnerError(
                f"CLI connection_pool {name}={pool.get(name)!r} "
                f"does not match D={case.data_connection_target}")
    if pool.get("valid") is not True:
        raise RunnerError("CLI connection_pool validation is not valid")
    if pool.get("mismatch_samples") != 0:
        raise RunnerError("CLI connection_pool observed mismatched data-connection samples")
    for name in ("minimum_sampled_connections", "maximum_sampled_connections"):
        sampled = pool.get(name)
        if sampled is not None and sampled != case.data_connection_target:
            raise RunnerError(f"CLI connection_pool {name}={sampled!r} is not D")
    stage_metrics = result.get("stage_metrics")
    cql_in_flight_peak = (stage_metrics.get("cql_in_flight_peak")
                          if isinstance(stage_metrics, dict) else None)
    if (not isinstance(cql_in_flight_peak, int) or isinstance(cql_in_flight_peak, bool)
            or cql_in_flight_peak < 0):
        raise RunnerError("CLI result is missing a valid cql_in_flight_peak")
    if cql_in_flight_peak > case.workers:
        raise RunnerError(
            f"CLI cql_in_flight_peak={cql_in_flight_peak} exceeds worker limit C={case.workers}")
    return {"valid": True, "C": case.workers, "S": case.shards,
            "P": case.per_shard_pool, "D": case.data_connection_target,
            "node_count": case.node_count, "nodes": observed_nodes,
            "replication_factor": case.replication_factor,
            "read_cl": case.read_cl, "write_cl": case.write_cl,
            "worker_routing": case.worker_routing,
            "wallet_profile": WALLET_PROFILE,
            "lwt_contract": lwt_contract,
            "pool_ready_connections": pool["pool_ready_connections"],
            "pool_after_last_reply": pool["pool_after_last_reply"],
            "cql_in_flight_peak": cql_in_flight_peak}


def apply_runner_affinity() -> dict[str, Any]:
    expected = set(SERVICE_CPUS)
    if not hasattr(os, "sched_setaffinity") or not hasattr(os, "sched_getaffinity"):
        raise RunnerError("this host cannot set/inspect process CPU affinity")
    os.sched_setaffinity(0, expected)
    actual = set(os.sched_getaffinity(0))
    if actual != expected:
        raise RunnerError(f"runner/monitor CPU affinity {actual} != expected {expected}")
    return {"requested": sorted(expected), "actual": sorted(actual)}


def run_trial(run_id: str, run_dir: pathlib.Path, case: TrialCase, args: argparse.Namespace,
              users: int, requests_per_user: int, binary: pathlib.Path) -> dict[str, Any]:
    commitlog_sync = getattr(args, "resolved_commitlog_sync", args.commitlog_sync)
    commitlog_sync_period = getattr(args, "resolved_commitlog_sync_period_in_ms",
                                    args.commitlog_sync_period_in_ms)
    trial_id = (f"trial-{case.order:03d}-"
                f"{case_id_for_profile(case, commitlog_sync, commitlog_sync_period)}")
    trial_dir = run_dir / "trials" / trial_id
    service_dir = trial_dir / "service"
    owned_root = trial_dir / "owned-data"
    trial_dir.mkdir(parents=True, exist_ok=False)
    service_dir.mkdir()
    io_properties_profile = write_io_properties_profile(
        trial_dir, getattr(args, "io_properties_file", None),
        captured_profile=getattr(args, "resolved_io_properties_profile", None))
    nonce = uuid.uuid4().hex
    cluster = OwnedCluster(trial_dir, owned_root, nonce, trial_id, case.shards, run_id,
                           node_count=case.node_count,
                           io_properties_profile=io_properties_profile,
                           commitlog_sync=commitlog_sync,
                           commitlog_sync_period_in_ms=commitlog_sync_period,
                           host_io_limits=getattr(args, "resolved_host_io_limits", None))
    cli_process: subprocess.Popen[Any] | None = None
    monitor: TrialMonitor | None = None
    stdout_handle = stderr_handle = None
    gate = trial_dir / "start.gate"
    finish_gate = trial_dir / "finish.gate"
    keyspace = _keyspace_name()
    hashes = source_hashes()
    record: dict[str, Any] = {
        "order": case.order, "repeat": case.repeat, "trial_id": trial_id, "mode": args.mode,
        "node_count": case.node_count, "node_names": list(node_names_for(case.node_count)),
        "shards": case.shards, "workers": case.workers, "C": case.workers,
        "per_shard_pool": case.per_shard_pool, "P": case.per_shard_pool,
        "target_data_connections": case.data_connection_target,
        "D": case.data_connection_target, "worker_routing": case.worker_routing,
        "wallet_profile": case.wallet_profile,
        "account_state_source": ACCOUNT_STATE_SOURCE,
        "setup_state_source": "db",
        "ledger_key": list(LEDGER_KEY), "serial_cl": SERIAL_CL,
        "cql_requests_per_transaction": CQL_REQUESTS_PER_TRANSACTION,
        "cql_statement_count": CQL_STATEMENTS_PER_TRANSACTION,
        "logical_mutations_per_successful_request": 1,
        "paxos_internal_storage_activity_note": (
            "LWT may produce additional Paxos/internal storage and commitlog activity; "
            "applied=false does not imply zero fsync"),
        "replication_factor": case.replication_factor,
        "read_cl": case.read_cl, "write_cl": case.write_cl,
        "commitlog_sync": commitlog_sync,
        "commitlog_sync_period_in_ms": commitlog_sync_period,
        "commitlog_use_o_dsync": False,
        "commitlog_total_space_in_mb": 1024,
        "commitlog_segment_size_in_mb": 64,
        "commitlog_sync_profile": ("periodic_lwt_force_sync_10000ms"
                                   if commitlog_sync == "periodic" else "batch"),
        "per_shard_pool_overridden": case.per_shard_pool_overridden,
        "requested_per_shard_pool": case.requested_per_shard_pool,
        "expected_data_connections_per_node": case.expected_connections_per_node,
        "setup_concurrency": case.setup_concurrency,
        "per_worker_queue_capacity": case.per_worker_queue_capacity,
        "queue_capacity": QUEUE_CAPACITY,
        "global_admission_capacity": QUEUE_CAPACITY,
        "users": users, "requests_per_user": requests_per_user,
        "total_requests": users * requests_per_user, "keyspace": keyspace,
        "logical_wallet_mutations_expected": users * requests_per_user,
        "expected_application_cql_operations": users * requests_per_user,
        "measured_select_requests": 0,
        "status": "running", "validation": {}, "data_connections": None,
        "required_metrics_valid": False, "cleanup_status": "not_started",
        "image_digest": PINNED_REPO_DIGEST,
        "io_properties_enabled": io_properties_profile.get("enabled") is True,
        "io_properties_sha256": io_properties_profile.get("source_sha256"),
        "io_properties_profile": {key: value for key, value in io_properties_profile.items()
                                  if key != "content"},
        "host_io_limits": getattr(args, "resolved_host_io_limits", None),
        "source_dirty": hashes.get("git_worktree_dirty", "unknown"),
        "source_hashes": hashes,
        "started_utc": _utc_now(),
    }
    periodic_evidence = getattr(args, "periodic_lwt_sync_source_evidence", None)
    if commitlog_sync == "periodic":
        periodic_validation = (validate_periodic_lwt_evidence_reference(periodic_evidence)
                               if isinstance(periodic_evidence, dict) else
                               {"valid": False, "errors": ["periodic LWT source evidence reference is missing"]})
        record["periodic_lwt_sync_evidence"] = {
            **(periodic_evidence if isinstance(periodic_evidence, dict) else {}),
            "validation": periodic_validation,
            "valid": periodic_validation.get("valid") is True,
        }
        record["validation"]["periodic_lwt_sync_evidence"] = periodic_validation.get("valid") is True
        if record["validation"]["periodic_lwt_sync_evidence"] is not True:
            raise RunnerError("periodic commitlog requires validated immutable LWT source evidence")
    host_aio = read_host_aio_counters()
    aio_budget = compute_aio_budget(case.shards, host_aio["aio_max_nr"], host_aio["aio_nr"],
                                    case.node_count)
    cluster.aio_budget = aio_budget
    record["aio_profile"] = scylla_aio_profile_evidence()
    record["aio_budget"] = aio_budget
    record["validation"]["aio_budget"] = aio_budget["valid"]
    write_json(trial_dir / "aio-budget.json", aio_budget)
    write_json(trial_dir / "trial-config.json", {
        **record, "cpuset": cpuset_for(case.shards, case.node_count),
        "service_cpuset": cpuset_for(case.shards, case.node_count)["service"],
        "cgroup_memory_limit_bytes": NODE_CGROUP_BYTES, "scylla_memory_bytes": NODE_MEMORY_BYTES,
        "commitlog_sync": commitlog_sync, "commitlog_use_o_dsync": False,
        "commitlog_sync_period_in_ms": commitlog_sync_period,
        "commitlog_sync_batch_window_in_ms": "release default 10000; not overridden",
        "commitlog_sync_behavior_note": (
            "Pinned periodic configuration acknowledges ordinary writes immediately; LWT writes "
            "use the source-verified force_sync path in all sync modes. This does not establish "
            "one fsync per transaction or rule out coalescing."
            if commitlog_sync == "periodic" else
            "Batch acknowledgements follow the release batch sync path; a standalone per-write fsync profile is unsupported."),
        "commitlog_sync_batch_window_effective_runtime_behavior": (
            "not used by selected periodic mode" if commitlog_sync == "periodic"
            else "not established"),
        "durable_writes": True, "replication_factor": case.replication_factor,
        "wallet_profile": case.wallet_profile,
        "account_state_source": ACCOUNT_STATE_SOURCE,
        "setup_state_source": "db",
        "ledger_key": list(LEDGER_KEY), "serial_cl": SERIAL_CL,
        "cql_requests_per_transaction": CQL_REQUESTS_PER_TRANSACTION,
        "cql_statement_count": CQL_STATEMENTS_PER_TRANSACTION,
        "logical_mutations_per_successful_request": 1,
        "paxos_internal_storage_activity_note": (
            "LWT may produce additional Paxos/internal storage and commitlog activity; "
            "applied=false does not imply zero fsync"),
        "measured_select_requests": 0,
        "measured_cql_requests_per_transaction": CQL_REQUESTS_PER_TRANSACTION,
        "account_states_preloaded_expected": users,
        "state_preload_concurrency_expected": case.setup_concurrency,
        "read_cl": case.read_cl, "write_cl": case.write_cl,
        "target_connections_per_node": case.expected_connections_per_node,
        "target_data_connections_total_D": case.data_connection_target,
        "worker_routing": case.worker_routing,
        "per_shard_pool_overridden": case.per_shard_pool_overridden,
        "requested_per_shard_pool": case.requested_per_shard_pool,
        "setup_concurrency": case.setup_concurrency,
        "per_worker_queue_capacity": case.per_worker_queue_capacity,
        "queue_capacity": QUEUE_CAPACITY,
        "global_admission_capacity": QUEUE_CAPACITY,
        "metrics_relabel_profile": cluster.metrics_relabel_profile,
        "io_properties_profile": {key: value for key, value in io_properties_profile.items()
                                  if key != "content"},
        "host_io_limits": getattr(args, "resolved_host_io_limits", None),
        "io_properties_runtime_validation_expected": io_properties_profile.get("enabled") is True,
        "aio_profile": scylla_aio_profile_evidence(), "aio_budget": aio_budget,
        "no_cpu_quota": True, "container_ports_published": False,
    })
    try:
        if not aio_budget["valid"]:
            raise RunnerError(aio_budget["error"] or "host AIO budget is not sufficient")
        cluster.start(startup_timeout=args.startup_timeout)
        record["cluster"] = safe_json_read(trial_dir / "cluster.json")
        record["server_build"] = cluster.full_server_build_evidence()
        source_version_match = (
            record["server_build"].get("source_version_correspondence") == "verified_by_build_version"
            and record["server_build"].get("source_tag_commit") == SCYLLA_SOURCE_COMMIT
            and all(item.get("version_has_tag_commit_prefix") is True
                    for item in record["server_build"].get("nodes", {}).values()))
        record["validation"]["source_build_correspondence"] = source_version_match
        if not source_version_match:
            raise RunnerError("Scylla image full build version does not match the pinned release tag commit")
        source_evidence_path = run_dir / "source-evidence" / "manifest.json"
        record["source_evidence"] = safe_json_read(source_evidence_path) if source_evidence_path.exists() else None
        record["runtime_config"] = _runtime_config_evidence(cluster)
        record["runtime_commitlog_validation"] = validate_runtime_commitlog_config(
            record["runtime_config"], cluster.node_names, commitlog_sync,
            commitlog_sync_period)
        record["validation"]["runtime_commitlog"] = record["runtime_commitlog_validation"]["valid"]
        write_json(trial_dir / "runtime-commitlog-validation.json", record["runtime_commitlog_validation"])
        if not record["runtime_commitlog_validation"]["valid"]:
            raise RunnerError("effective Scylla commitlog config could not be validated: " +
                              "; ".join(record["runtime_commitlog_validation"]["problems"]))
        record["launch_config"] = docker_config_evidence(cluster)
        record["launch_config_validation"] = validate_launch_config(
            record["launch_config"], case.node_count, io_properties_profile,
            commitlog_sync, commitlog_sync_period)
        record["validation"]["launch_config"] = record["launch_config_validation"]["valid"]
        if not record["launch_config_validation"]["valid"]:
            raise RunnerError("; ".join(record["launch_config_validation"]["errors"]))
        record["runtime_aio_validation"] = cluster.capture_and_validate_runtime_aio()
        record["validation"]["runtime_aio"] = record["runtime_aio_validation"]["valid"]
        if not record["runtime_aio_validation"]["valid"]:
            raise RunnerError("runtime Scylla AIO startup evidence is invalid: " +
                              "; ".join(record["runtime_aio_validation"]["problems"]))
        defer_io_validation_until_ready = (
            commitlog_sync == "periodic" and io_properties_profile.get("enabled") is True)
        record["io_properties_runtime_gate"] = {
            "deferred_until_ready_to_measure": defer_io_validation_until_ready,
            "reason": ("periodic commitlog queues are registered lazily after seed I/O"
                       if defer_io_validation_until_ready else "existing pre-CLI validation timing"),
            "measurement_gate_remains_closed_until_valid": True,
        }
        if not defer_io_validation_until_ready:
            record["io_properties_runtime_validation"] = cluster.capture_io_properties_runtime_evidence(
                record["launch_config"], record["runtime_aio_validation"])
            record["validation"]["io_properties_runtime"] = record[
                "io_properties_runtime_validation"]["valid"]
            if not record["io_properties_runtime_validation"]["valid"]:
                raise RunnerError("runtime Scylla I/O properties evidence is invalid: " +
                                  "; ".join(record["io_properties_runtime_validation"]["problems"]))
        record["server_version_validation"] = cluster.verify_server_config()
        record["validation"]["server_version"] = record["server_version_validation"]["version_consistent"]
        record["reactor_affinity"] = {}
        for node_record in cluster.records:
            scylla_pid = resolve_scylla_host_pid(node_record)
            expected_cpus = CPU_ASSIGNMENTS[case.shards][node_record.node]
            record["reactor_affinity"][node_record.node] = thread_affinity_evidence(scylla_pid, expected_cpus)
        affinity_valid = all(value["valid"] for value in record["reactor_affinity"].values())
        record["validation"]["reactor_affinity"] = affinity_valid
        write_json(trial_dir / "reactor-affinity.json", record["reactor_affinity"])
        if not affinity_valid:
            raise RunnerError("Scylla thread affinity did not match the configured shard cpus")

        command = _cli_command(binary, cluster.endpoints(), keyspace, case,
                               users, requests_per_user, QUEUE_CAPACITY,
                               args.mode, service_dir, gate, finish_gate)
        taskset = shutil.which("taskset")
        if not taskset:
            raise RunnerError("taskset is required to pin the workload and monitor to CPUs 6,7,14,15")
        full_command = [taskset, "-c", cpuset_for(case.shards, case.node_count)["service"], *command]
        record["cli_command"] = full_command
        record["cli_binary_sha256"] = hashlib.sha256(binary.read_bytes()).hexdigest()
        (trial_dir / "logs").mkdir(parents=True, exist_ok=True)
        stdout_handle = (trial_dir / "logs" / "service.stdout.log").open("wb")
        stderr_handle = (trial_dir / "logs" / "service.stderr.log").open("wb")
        cli_process = subprocess.Popen(full_command, cwd=REPO_ROOT, stdout=stdout_handle,
                                       stderr=stderr_handle, start_new_session=True)
        events_path = service_dir / "events.jsonl"
        ready = wait_for_event(events_path, cli_process, "ready_to_measure", SEED_READY_WAIT_SECONDS)
        session_id = event_session_id(ready)
        if not session_id:
            raise RunnerError(f"ready_to_measure event is missing session_id: {ready}")
        record["session_id"] = session_id
        record["ready_event"] = ready
        events_before_gate = parse_cli_events(events_path)
        record["preloading_validation"] = validate_lwt_event_contract(
            events_before_gate, case, users, requests_per_user)
        record["validation"]["wallet_preload"] = True
        record["validation"]["wallet_profile"] = True
        record["ready_profile_validation"] = validate_ready_event_profile(ready, case)
        if defer_io_validation_until_ready:
            record["io_properties_runtime_validation"] = cluster.capture_io_properties_runtime_evidence(
                record["launch_config"], record["runtime_aio_validation"],
                defer_missing_series_until_ready=True)
            record["validation"]["io_properties_runtime"] = record[
                "io_properties_runtime_validation"]["valid"]
            if not record["io_properties_runtime_validation"]["valid"]:
                raise RunnerError("runtime Scylla I/O properties evidence is invalid: " +
                                  "; ".join(record["io_properties_runtime_validation"]["problems"]))
        keyspace_check = verify_keyspace(cluster, keyspace, case.replication_factor)
        record["validation"]["keyspace"] = keyspace_check["valid"]
        if not keyspace_check["valid"]:
            raise RunnerError(f"keyspace validation failed: {keyspace_check['problems']}")
        wallet_schema = verify_wallet_schema(cluster, keyspace)
        record["wallet_schema_validation"] = wallet_schema
        record["validation"]["wallet_schema"] = wallet_schema["valid"]
        schema_agreement = cluster.verify_schema_agreement()
        record["schema_agreement"] = schema_agreement
        record["validation"]["schema_agreement"] = schema_agreement["valid"]
        before = connection_audit(cluster, session_id, case,
                                  trial_dir / "connection-audit-before.json", "before_gate")
        record["connection_audit_before"] = before
        record["validation"]["connections_before_gate"] = before["valid"]
        if not before["valid"]:
            raise RunnerError("SESSION_ID connection audit failed before measurement gate")

        monitor = TrialMonitor(trial_dir, cluster, cli_process)
        monitor.start()
        with gate.open("x", encoding="utf-8") as handle:
            handle.write(f"{nonce}\n")
            handle.flush()
            os.fsync(handle.fileno())
        measuring = wait_for_event(events_path, cli_process, "measuring", CLI_GATE_WAIT_SECONDS)
        record["measuring_event"] = measuring
        measuring_start_ns = _event_int(measuring, "measurement_start_unix_ns")
        monitor.set_measurement_start(measuring_start_ns)
        client_end = wait_for_event(events_path, cli_process, "client_end", 12 * 60 * 60)
        record["client_end_event"] = client_end
        window_start_ns = _event_int(client_end, "measurement_start_unix_ns")
        window_end_ns = _event_int(client_end, "measurement_end_unix_ns")
        if measuring_start_ns != window_start_ns:
            raise RunnerError("measuring and client_end report different measurement starts")
        monitor.set_measurement_end(window_end_ns)
        monitor_summary = monitor.stop(
            window_start_ns, window_end_ns,
            logical_requests_expected=users * requests_per_user,
            logical_replies_received=_event_int(client_end, "replies_received"))
        monitor = None
        record["monitor"] = monitor_summary
        metrics_validation = required_metric_validation(monitor_summary)
        record["required_metrics_valid"] = metrics_validation["valid"]
        record["validation"]["required_metrics"] = metrics_validation["valid"]
        write_json(trial_dir / "required-metrics-validation.json", metrics_validation)
        # The finish gate keeps the Scylla session alive until post-window audit.
        # Wait for full durable verification first; `verifying` is too early.
        verified = wait_for_event(events_path, cli_process, "verified", 12 * 60 * 60)
        record["verified_event"] = verified
        awaiting = wait_for_event(events_path, cli_process, "awaiting_runner_audit", 30)
        record["awaiting_runner_audit_event"] = awaiting
        if cli_process.poll() is not None:
            raise RunnerError(f"CLI exited before post-window audit (status {cli_process.returncode})")
        after = connection_audit(cluster, session_id, case,
                                 trial_dir / "connection-audit-after.json", "after_verified_before_finish_gate")
        record["connection_audit_after"] = after
        record["validation"]["connections_after_window"] = after["valid"]
        record["validation"]["connection_sockets_stable"] = monitor_summary["socket_window_stable"]
        if not after["valid"]:
            raise RunnerError("SESSION_ID connection audit failed after verification")
        if not monitor_summary["socket_window_stable"]:
            raise RunnerError("CLI native transport sockets changed during measurement window")
        if "session_id" in awaiting and event_session_id(awaiting) != session_id:
            raise RunnerError("CLI session id changed before post-window audit")
        with finish_gate.open("x", encoding="utf-8") as handle:
            handle.write(f"{nonce}\n")
            handle.flush()
            os.fsync(handle.fileno())
        released = wait_for_event(events_path, cli_process, "runner_audit_released", CLI_GATE_WAIT_SECONDS)
        record["runner_audit_released_event"] = released
        timeout = 12 * 60 * 60 if args.mode == "formal" else 2 * 60 * 60
        try:
            exit_code = cli_process.wait(timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            terminate_process(cli_process)
            raise RunnerError(f"CLI did not exit within {timeout}s after finish-gate release") from exc
        record["cli_exit_code"] = exit_code
        events = parse_cli_events(events_path)
        record["events"] = events
        record["event_profile_validation"] = validate_cli_event_profiles(
            events, case, users, requests_per_user)
        if exit_code != 0:
            raise RunnerError(f"CLI exited with status {exit_code}")
        if find_event(events, "verified") is None:
            raise RunnerError("CLI exited 0 without a verified event")
        result_path = service_dir / "result.json"
        result = safe_json_read(result_path) if result_path.exists() else {}
        record["cli_result"] = result
        if result.get("valid") is not True:
            raise RunnerError("CLI result.json does not contain valid=true")
        record["cli_profile_validation"] = validate_cli_result_profile(
            result, case, users, requests_per_user, expected_nodes=cluster.endpoints())
        record["validation"]["cli"] = True
        non_metric_validations = [value for key, value in record["validation"].items()
                                  if key != "required_metrics"]
        if not all(non_metric_validations):
            record["status"] = "invalid"
        elif record["required_metrics_valid"]:
            record["status"] = "passed"
        elif args.mode == "formal":
            record["status"] = "invalid"
        else:
            record["status"] = "passed_relaxed"
            record["notes"] = ["diagnostic mode: missing required window metrics or process/socket evidence"]
    except KeyboardInterrupt:
        record["status"] = "interrupted"
        record["error"] = "KeyboardInterrupt: run interrupted by SIGINT"
        if cli_process is not None:
            terminate_process(cli_process)
            record["cli_exit_code"] = cli_process.poll()
    except Exception as exc:
        record["error"] = f"{type(exc).__name__}: {exc}"
        if cli_process is not None:
            terminate_process(cli_process)
            record["cli_exit_code"] = cli_process.poll()
        record["status"] = "failed"
    finally:
        if cli_process is not None and cli_process.poll() is None:
            terminate_process(cli_process)
        if monitor is not None:
            try:
                record["monitor"] = monitor.stop()
                monitor = None
            except Exception as monitor_exc:
                record.setdefault("errors", []).append(f"monitor stop: {monitor_exc}")
        if stdout_handle is not None:
            stdout_handle.close()
        if stderr_handle is not None:
            stderr_handle.close()
        pre_cleanup_outcome = {
            **record,
            "cleanup_status": "pending",
            "captured_before_cleanup_utc": _utc_now(),
        }
        try:
            write_json(trial_dir / "trial-outcome-before-cleanup.json", pre_cleanup_outcome)
        except Exception as snapshot_exc:
            record.setdefault("errors", []).append(
                f"pre-cleanup outcome snapshot: {type(snapshot_exc).__name__}: {snapshot_exc}")
        try:
            cleanup = cluster.stop_and_cleanup()
        except Exception as cleanup_exc:
            cleanup = cluster.cleanup_result
            cleanup.status = "cleanup_error"
            cleanup.errors.append(f"cleanup exception: {cleanup_exc}")
            cluster._write_cleanup_audit()
        record["cleanup_status"] = cleanup.status
        record["cleanup"] = asdict(cleanup)
        if cleanup.status != "clean" and record.get("status") != "interrupted":
            record["status"] = "infrastructure_failure"
        record["finished_utc"] = _utc_now()
        write_json(trial_dir / "runner-result.json", record)
    return record


def run_trial_matrix(run_id: str, run_dir: pathlib.Path, cases: Sequence[TrialCase],
                     args: argparse.Namespace, users: int, requests_per_user: int,
                     binary: pathlib.Path, trials: list[dict[str, Any]]) -> str | None:
    for case in cases:
        trial = run_trial(run_id, run_dir, case, args, users, requests_per_user, binary)
        trials.append(trial)
        if trial.get("cleanup_status") != "clean":
            error = trial.get("error") or "no trial error detail supplied"
            return (f"trial {trial.get('trial_id')} cleanup was "
                    f"{trial.get('cleanup_status')}; status={trial.get('status')}; error={error}")
        status = trial.get("status")
        if status == "interrupted":
            detail = f"trial {trial.get('trial_id')} was interrupted"
            if trial.get("error"):
                detail += f"; error={trial['error']}"
            return detail
        if status not in ("passed", "passed_relaxed"):
            error = trial.get("error") or "no trial error detail supplied"
            return (f"trial {trial.get('trial_id')} ended status={status}; error={error}")
    return None


def safe_json_read(path: pathlib.Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RunnerError(f"cannot read JSON result {path}: {exc}") from exc
    return value if isinstance(value, dict) else {"raw_result": value}


def create_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run nonce-owned, isolated one- or three-node ScyllaDB wallet benchmark trials.")
    parser.add_argument("--mode", choices=("smoke", "pilot", "formal"), default="formal",
                        help="formal is the default; smoke/pilot are explicitly diagnostic")
    parser.add_argument("--shards", type=lambda value: parse_int_csv(value, "shards"),
                        help="comma-separated shard counts (default depends on mode)")
    parser.add_argument("--workers", type=lambda value: parse_int_csv(value, "workers"),
                        help="comma-separated DB worker / in-flight upper-bound C values")
    parser.add_argument("--per-shard-pool", type=int,
                        help="explicit per-shard data-connection pool P; if omitted, retain legacy C/(N*S) rule")
    parser.add_argument("--worker-routing", choices=("round-robin", "per-user"),
                        default="round-robin",
                        help="assign accepted accounts to DB workers (default: round-robin)")
    parser.add_argument("--replication-factor", type=int, default=3,
        help="keyspace RF, 1..node-count (default: 3; must not exceed node count)")
    parser.add_argument("--read-cl", type=int, default=3,
                        help="wallet read consistency level, numeric 1..3 (default: 3/THREE)")
    parser.add_argument("--write-cl", type=int, default=3,
                        help="wallet write consistency level, numeric 1..3 (default: 3/THREE)")
    parser.add_argument("--commitlog-sync", choices=("batch", "periodic"), default="batch",
                        help="Scylla commitlog sync mode (default: batch)")
    parser.add_argument("--commitlog-sync-period-in-ms", type=int,
                        help="periodic sync interval; periodic pilot profile requires explicit 10000")
    parser.add_argument("--repeats", type=int, help="repeat count (default depends on mode)")
    parser.add_argument("--users", type=int, help="accounts per trial (default depends on mode)")
    parser.add_argument("--requests-per-user", type=int,
                        help="sequential requests per account (default depends on mode)")
    parser.add_argument("--node-count", type=int, choices=SUPPORTED_NODE_COUNTS, default=3,
                        help="owned Scylla node count, 1 or 3 (default: 3)")
    parser.add_argument("--io-properties-file", type=pathlib.Path,
                        help="optional calibrated Scylla I/O properties YAML, copied and mounted read-only")
    parser.add_argument("--host-io-limits-json", type=pathlib.Path,
                        help="optional verified common host-device cgroup I/O caps JSON; disabled by default")
    parser.add_argument("--output-root", type=pathlib.Path, default=DEFAULT_OUTPUT_ROOT,
                        help=f"run artifact root (default: {DEFAULT_OUTPUT_ROOT})")
    parser.add_argument("--skip-build", action="store_true", help="require and reuse built release binary")
    parser.add_argument("--binary", type=pathlib.Path, help="explicit executable benchmark CLI")
    parser.add_argument("--startup-timeout", type=int, default=STARTUP_WAIT_SECONDS,
                        help=f"startup/cluster-ready deadline (default: {STARTUP_WAIT_SECONDS}s)")
    parser.add_argument("--preflight-wait", type=int, default=60,
                        help="formal host stability preflight wait, at most 60 seconds")
    parser.add_argument("--dry-run", action="store_true",
                        help="print the exact matrix, capacity estimate and CLI commands; create no containers")
    return parser


def resolve_run_parameters(args: argparse.Namespace) -> dict[str, Any]:
    mode_defaults = {
        "smoke": ((1,), (48,), 1),
        "pilot": (DEFAULT_SHARDS, DEFAULT_WORKERS, 1),
        "formal": (DEFAULT_SHARDS, DEFAULT_WORKERS, FORMAL_REPEATS),
    }
    default_shards, default_workers, default_repeats = mode_defaults[args.mode]
    shards = args.shards or default_shards
    workers = args.workers or default_workers
    repeats = default_repeats if args.repeats is None else args.repeats
    default_users, default_requests = parse_mode_defaults(args.mode)
    users = default_users if args.users is None else args.users
    requests_per_user = default_requests if args.requests_per_user is None else args.requests_per_user
    node_count = getattr(args, "node_count", 3)
    node_names = node_names_for(node_count)
    io_properties_profile = inspect_io_properties_source(
        getattr(args, "io_properties_file", None))
    host_io_limits = load_host_io_limits(getattr(args, "host_io_limits_json", None))
    if args.mode == "formal" and io_properties_profile["enabled"]:
        raise RunnerError(
            "--io-properties-file selects a calibrated custom I/O profile; use pilot mode, not formal mode")
    if users <= 0:
        raise RunnerError("users must be a positive integer")
    if requests_per_user <= 0 or requests_per_user % 2 or requests_per_user > 1000:
        raise RunnerError("requests-per-user must be positive, even, and at most 1000")
    replication_factor = args.replication_factor
    if isinstance(replication_factor, bool) or replication_factor not in (1, 2, 3):
        raise RunnerError("replication-factor must be 1, 2, or 3")
    read_cl = _canonical_consistency(args.read_cl, "read-cl")
    write_cl = _canonical_consistency(args.write_cl, "write-cl")
    if CONSISTENCY_LEVELS[read_cl] > replication_factor:
        raise RunnerError(f"read-cl {read_cl} exceeds replication-factor {replication_factor}")
    if CONSISTENCY_LEVELS[write_cl] > replication_factor:
        raise RunnerError(f"write-cl {write_cl} exceeds replication-factor {replication_factor}")
    commitlog_sync = args.commitlog_sync
    commitlog_sync_period = args.commitlog_sync_period_in_ms
    if commitlog_sync == "batch":
        if commitlog_sync_period is not None:
            raise RunnerError("--commitlog-sync-period-in-ms is only valid with --commitlog-sync periodic")
    elif commitlog_sync == "periodic":
        if args.mode != "pilot":
            raise RunnerError("periodic commitlog profile is allowed only in pilot mode")
        if commitlog_sync_period != PERIODIC_SYNC_DEFAULT_PERIOD_MS:
            raise RunnerError("periodic commitlog pilot requires explicit --commitlog-sync-period-in-ms 10000")
        if WALLET_PROFILE != "wallet-memory-lwt-v1":
            raise RunnerError("periodic commitlog profile requires wallet-memory-lwt-v1")
    if args.startup_timeout <= 0 or args.preflight_wait < 0 or args.preflight_wait > 60:
        raise RunnerError("startup timeout must be positive and preflight wait must be 0..60 seconds")
    if args.worker_routing == "per-user" and tuple(workers) != (users,):
        raise RunnerError("worker-routing=per-user requires exactly one workers value equal to users")
    cases = build_matrix(shards, workers, repeats,
                         per_shard_pool=args.per_shard_pool,
                         worker_routing=args.worker_routing,
                         replication_factor=replication_factor,
                         read_cl=read_cl, write_cl=write_cl,
                         node_count=node_count)
    exact_formal = args.mode == "formal" and is_formal_matrix(
        shards, workers, repeats, users, requests_per_user,
        per_shard_pool=args.per_shard_pool,
        worker_routing=args.worker_routing,
        replication_factor=replication_factor,
        read_cl=read_cl, write_cl=write_cl, node_count=node_count)
    explicit_custom = (tuple(shards) != DEFAULT_SHARDS or tuple(workers) != DEFAULT_WORKERS
                       or repeats != FORMAL_REPEATS or users != FORMAL_USERS
                       or requests_per_user != FORMAL_REQUESTS_PER_USER
                       or args.per_shard_pool is not None
                       or args.worker_routing != "round-robin"
                       or replication_factor != 3 or read_cl != "THREE" or write_cl != "THREE"
                       or node_count != 3 or io_properties_profile["enabled"]
                       or commitlog_sync != "batch")
    workload_class = ("formal_candidate" if exact_formal else
                      "custom_workload" if explicit_custom and args.mode == "formal" else
                      f"diagnostic_{args.mode}")
    return {"shards": tuple(shards), "workers": tuple(workers), "repeats": repeats,
            "users": users, "requests_per_user": requests_per_user,
            "cases": cases, "exact_formal_candidate": exact_formal,
            "node_count": node_count, "node_names": node_names,
            "custom_workload": explicit_custom, "workload_class": workload_class,
            "wallet_profile": WALLET_PROFILE,
            "io_properties_profile": io_properties_profile,
            "host_io_limits": host_io_limits,
            "per_shard_pool_override": args.per_shard_pool,
            "worker_routing": args.worker_routing,
            "replication_factor": replication_factor,
            "read_cl": read_cl, "write_cl": write_cl,
            "commitlog_sync": commitlog_sync,
            "commitlog_sync_period_in_ms": commitlog_sync_period,
            "commitlog_sync_profile": ("periodic_lwt_force_sync_10000ms"
                                       if commitlog_sync == "periodic" else "batch"),
            "queue_capacity": QUEUE_CAPACITY}


def dry_run_report(args: argparse.Namespace, parameters: dict[str, Any]) -> dict[str, Any]:
    estimate = estimate_disk_bytes(parameters["users"], parameters["requests_per_user"],
                                    parameters["replication_factor"], parameters["node_count"])
    io_profile = parameters["io_properties_profile"]
    io_profile_summary = {key: value for key, value in io_profile.items()
                          if key != "content"}
    binary = args.binary.resolve() if args.binary else (
        REPO_ROOT / "target" / "scylla-wallet" / "release" / RUST_PACKAGE)
    commands = []
    for case in parameters["cases"]:
        commands.append({
            "case_id": case_id_for_profile(case, parameters["commitlog_sync"],
                                           parameters["commitlog_sync_period_in_ms"]),
            "order": case.order,
            "wallet_profile": case.wallet_profile,
            "account_state_source": ACCOUNT_STATE_SOURCE,
            "ledger_key": list(LEDGER_KEY), "serial_cl": SERIAL_CL,
            "cql_requests_per_transaction": CQL_REQUESTS_PER_TRANSACTION,
            "cql_statement_count": CQL_STATEMENTS_PER_TRANSACTION,
            "measured_select_requests": 0,
            "taskset_cpus": ",".join(map(str, SERVICE_CPUS)),
            "command": ["taskset", "-c", ",".join(map(str, SERVICE_CPUS)), str(binary),
                        "--nodes", ",".join(f"{node.upper()}:9042"
                                            for node in parameters["node_names"]),
                        "--keyspace", "wallet_bench_<nonce>", "--shards", str(case.shards),
                        "--db-workers", str(case.workers), "--users", str(parameters["users"]),
                        "--requests-per-user", str(parameters["requests_per_user"]),
                        "--queue-capacity", str(QUEUE_CAPACITY), "--output-dir", "<trial>/service",
                        "--mode", args.mode, "--start-gate", "<trial>/start.gate",
                        "--finish-gate", "<trial>/finish.gate"],
            "replication_factor": case.replication_factor,
            "read_cl": case.read_cl, "write_cl": case.write_cl,
            "per_shard_pool_P": case.per_shard_pool,
            "per_shard_pool_overridden": case.per_shard_pool_overridden,
            "requested_per_shard_pool": case.requested_per_shard_pool,
            "worker_routing": case.worker_routing,
            "worker_routing_explicit": case.worker_routing != "round-robin",
            "expected_data_connections_per_node": case.expected_connections_per_node,
            "target_data_connections_total_D": case.data_connection_target,
            "worker_inflight_limit_C": case.workers,
            "setup_concurrency": case.setup_concurrency,
            "per_worker_queue_capacity": case.per_worker_queue_capacity,
            "global_admission_capacity": QUEUE_CAPACITY,
            "node_count": case.node_count,
            "node_names": list(node_names_for(case.node_count)),
            "commitlog_sync": parameters["commitlog_sync"],
            "commitlog_sync_period_in_ms": parameters["commitlog_sync_period_in_ms"],
            "commitlog_use_o_dsync": False,
            "commitlog_total_space_in_mb": 1024,
            "commitlog_segment_size_in_mb": 64,
            "scylla_server_flags": ["--commitlog-sync", parameters["commitlog_sync"],
                                    "--commitlog-use-o-dsync", "false",
                                    *(["--commitlog-sync-period-in-ms",
                                       str(parameters["commitlog_sync_period_in_ms"])]
                                      if parameters["commitlog_sync"] == "periodic" else [])],
            "node_cpus": {node: CPU_ASSIGNMENTS[case.shards][node]
                          for node in node_names_for(case.node_count)},
        })
        if case.per_shard_pool_overridden:
            commands[-1]["command"] += ["--per-shard-pool", str(case.per_shard_pool)]
        if case.worker_routing != "round-robin":
            commands[-1]["command"] += ["--worker-routing", case.worker_routing]
        host_io_limits = parameters["host_io_limits"]
        if host_io_limits is not None:
            commands[-1]["docker_io_flags"] = docker_io_limit_flags(host_io_limits)
        if _case_has_nondefault_durability(case):
            commands[-1]["command"] += ["--replication-factor", str(case.replication_factor),
                                         "--read-cl", str(CONSISTENCY_LEVELS[case.read_cl]),
                                         "--write-cl", str(CONSISTENCY_LEVELS[case.write_cl])]
    inventory = host_inventory(args.output_root)
    aio_budget_by_shards = {
        str(shards): compute_aio_budget(
            shards, inventory.get("aio_max_nr"), inventory.get("aio_nr"),
            parameters["node_count"])
        for shards in parameters["shards"]
    }
    report = {
        "mode": args.mode, "workload_class": parameters["workload_class"],
        "commitlog_sync": parameters["commitlog_sync"],
        "commitlog_sync_period_in_ms": parameters["commitlog_sync_period_in_ms"],
        "commitlog_use_o_dsync": False,
        "commitlog_total_space_in_mb": 1024,
        "commitlog_segment_size_in_mb": 64,
        "commitlog_sync_profile": parameters["commitlog_sync_profile"],
        "periodic_lwt_sync_source_evidence": (
            {"required": True, "source_commit": PERIODIC_SYNC_LWT_SOURCE_COMMIT,
             "manifest_path": "<run>/periodic-lwt-source-evidence/manifest.json",
             "status": "required before a trial; not fetched by dry-run"}
            if parameters["commitlog_sync"] == "periodic" else {"required": False}),
        "node_count": parameters["node_count"],
        "node_names": list(parameters["node_names"]),
        "io_properties_profile": io_profile_summary,
        "wallet_profile": WALLET_PROFILE,
        "account_state_source": ACCOUNT_STATE_SOURCE,
        "ledger_key": list(LEDGER_KEY), "serial_cl": SERIAL_CL,
        "cql_requests_per_transaction": CQL_REQUESTS_PER_TRANSACTION,
        "cql_statement_count": CQL_STATEMENTS_PER_TRANSACTION,
        "measured_select_requests": 0,
        "formal_complete": False, "node_count": parameters["node_count"],
        "node_names": list(parameters["node_names"]), "shards": parameters["shards"],
        "workers_C": parameters["workers"], "repeats": parameters["repeats"],
        "replication_factor": parameters["replication_factor"],
        "read_cl": parameters["read_cl"], "write_cl": parameters["write_cl"],
        "per_shard_pool_P_override": parameters["per_shard_pool_override"],
        "worker_routing": parameters["worker_routing"],
        "connection_contract": "D=node_count*S*P data connections; C is the independent worker/in-flight upper bound",
        "queue_capacity": QUEUE_CAPACITY,
        "global_admission_capacity": QUEUE_CAPACITY,
        "users": parameters["users"], "requests_per_user": parameters["requests_per_user"],
        "total_requests_per_trial": parameters["users"] * parameters["requests_per_user"],
        "matrix_case_count": len(parameters["cases"]),
        "rotation_order": [asdict(case) for case in parameters["cases"]],
        "capacity_estimate": estimate,
        "disk_available_bytes": inventory.get("disk_available_bytes"),
        "disk_estimate_fits": (inventory.get("disk_available_bytes") is not None and
                                inventory["disk_available_bytes"] >= estimate["required_with_reserve_bytes"]),
        "aio_budget_by_shards": aio_budget_by_shards,
        "cpus_by_shards": {str(shards): cpuset_for(shards, parameters["node_count"])
                           for shards in parameters["shards"]},
        "python_monitor_cpu_affinity": list(SERVICE_CPUS),
        "host_lscpu_and_sibling_evidence": inventory,
        "pinned_image": PINNED_REPO_DIGEST,
        "server_local_profile": {"developer_mode": 1, "io_setup": 0,
                                 "overprovisioned": 1,
                                 "commitlog_sync": parameters["commitlog_sync"],
                                 "commitlog_sync_period_in_ms": parameters["commitlog_sync_period_in_ms"],
                                 "commitlog_use_o_dsync": False,
                                 "commitlog_total_space_in_mb": 1024,
                                 "commitlog_segment_size_in_mb": 64,
                                 "aio": scylla_aio_profile_evidence(),
                                 "io_properties_profile": io_profile_summary,
                                 "io_properties_launch": (
                                     {"flag": "--io-properties-file",
                                      "container_path": IO_PROPERTIES_CONTAINER_PATH,
                                      "read_only_bind_mount": True}
                                     if io_profile["enabled"] else {"status": "not_requested"}),
                                 "limitation": "same-host/shared-device local profile; not production tuning"},
        "commands": commands,
        "docker_actions": "none; dry-run does not create or start containers",
    }
    if parameters["host_io_limits"] is not None:
        report["host_io_limits"] = parameters["host_io_limits"]
    return report


def _measurement_summary(trial: dict[str, Any]) -> dict[str, Any]:
    result = trial.get("cli_result") or {}
    latency = result.get("end_to_end_latency") or {}
    measurement = result.get("measurement") or {}
    return {"rps": measurement.get("rps"), "p50_ns": latency.get("p50_ns"),
            "p95_ns": latency.get("p95_ns"), "p99_ns": latency.get("p99_ns"),
            "p999_ns": latency.get("p999_ns"), "max_ns": latency.get("max_ns"),
            "duration_seconds": measurement.get("wall_seconds")}


def _counter_metric_total(trial: dict[str, Any], metric: str) -> float | None:
    counter = (trial.get("monitor") or {}).get("counter_window_deltas") or {}
    nodes = counter.get("nodes", {})
    deltas = []
    expected_shards = {str(shard) for shard in range(int(trial.get("shards", 0)))}
    node_count = int(trial.get("node_count", 3))
    for node in node_names_for(node_count):
        series = nodes.get(node, {}).get(metric, {}).get("series", {})
        if not expected_shards.issubset(series):
            return None
        for shard in expected_shards:
            item = series[shard]
            if item.get("status") != "observed" or item.get("delta") is None:
                return None
            deltas.append(float(item["delta"]))
    return sum(deltas) if deltas else None


def _counter_node_total(trial: dict[str, Any], metric: str, node: str) -> float | None:
    counter = (trial.get("monitor") or {}).get("counter_window_deltas") or {}
    series = counter.get("nodes", {}).get(node, {}).get(metric, {}).get("series", {})
    expected_shards = {str(shard) for shard in range(int(trial.get("shards", 0)))}
    if not expected_shards or not expected_shards.issubset(series):
        return None
    deltas = []
    for shard in expected_shards:
        item = series[shard]
        if item.get("status") != "observed" or item.get("delta") is None:
            return None
        deltas.append(float(item["delta"]))
    return sum(deltas)


def _trial_resource_summary(trial: dict[str, Any]) -> dict[str, Any]:
    result = trial.get("cli_result") or {}
    requests = result.get("requests") or {}
    expected = requests.get("expected")
    cpu_used = (result.get("process_cpu") or {}).get("cpu_used_ns")
    connections = result.get("connection_pool") or {}
    stage_metrics = result.get("stage_metrics") or {}
    driver_counters = (result.get("driver_counters") or {}).get("delta") or {}
    heartbeat = result.get("heartbeat") or {}
    wallet_profile = trial.get("wallet_profile", "unknown")
    query_accounting = result.get("query_accounting") or requests.get("query_accounting") or {}
    measured_counters = stage_metrics.get("counters") or {}
    workload_expected = trial.get("total_requests")
    if isinstance(workload_expected, bool) or not isinstance(workload_expected, int):
        workload_expected = expected if isinstance(expected, int) and not isinstance(expected, bool) else None
    expected_cql_operations = (workload_expected if wallet_profile == WALLET_PROFILE else None)
    measured_selects = query_accounting.get("measured_select_requests")
    return {
        "logical_requests_expected": (expected if expected is not None else workload_expected),
        "logical_replies_received": requests.get("replies_received"),
        "wallet_profile": wallet_profile,
        "account_state_source": trial.get("account_state_source", "unknown"),
        "serial_cl": trial.get("serial_cl"),
        "cql_requests_per_transaction": trial.get("cql_requests_per_transaction"),
        "cql_statement_count": trial.get("cql_statement_count"),
        "measured_select_requests": measured_selects,
        "logical_wallet_mutations_expected": (workload_expected
                                               if wallet_profile == WALLET_PROFILE else None),
        "expected_application_cql_operations": expected_cql_operations,
        "measured_lwt_requests": measured_counters.get("cql_write_lwt"),
        "driver_cql_queries_delta": driver_counters.get("queries"),
        "process_cpu_used_ns": cpu_used,
        "process_cpu_ns_per_request": (cpu_used / expected if isinstance(cpu_used, (int, float))
                                       and isinstance(expected, int) and expected > 0 else None),
        "heartbeat_max_lag_ns": heartbeat.get("maximum_lag_ns"),
        "data_connections_ready": connections.get("pool_ready_connections"),
        "data_connections_after_last_reply": connections.get("pool_after_last_reply"),
        "data_connections_min": connections.get("minimum_sampled_connections"),
        "data_connections_max": connections.get("maximum_sampled_connections"),
        "workload_cql_in_flight_peak": stage_metrics.get("cql_in_flight_peak"),
    }


def write_run_reports(run_dir: pathlib.Path, args: argparse.Namespace,
                      parameters: dict[str, Any], trials: list[dict[str, Any]],
                      preflight: dict[str, Any] | None,
                      blocked_error: str | None = None) -> dict[str, Any]:
    parameters = dict(parameters)
    parameters.setdefault("commitlog_sync", "batch")
    parameters.setdefault("commitlog_sync_period_in_ms", None)
    parameters.setdefault("commitlog_sync_profile", "batch")
    expected_ids = [f"trial-{case.order:03d}-"
                    f"{case_id_for_profile(case, parameters['commitlog_sync'], parameters['commitlog_sync_period_in_ms'])}"
                    for case in parameters["cases"]]
    sync_period_label = (str(parameters["commitlog_sync_period_in_ms"])
                         if parameters["commitlog_sync"] == "periodic" else "unused; release default 10000")
    passed_ids = [trial.get("trial_id") for trial in trials
                  if trial.get("status") == "passed" and trial.get("cleanup_status") == "clean"
                  and trial.get("required_metrics_valid") is True]
    all_trials_complete = len(trials) == len(parameters["cases"])
    expected_orders = {case.order for case in parameters["cases"]}
    actual_orders = {trial.get("order") for trial in trials}
    exact_formal = parameters["exact_formal_candidate"]
    formal_complete = (exact_formal and all_trials_complete and actual_orders == expected_orders
                       and not blocked_error
                       and len(passed_ids) == FORMAL_CASE_COUNT
                       and set(passed_ids) == set(expected_ids)
                       and all(trial.get("wallet_profile") == WALLET_PROFILE for trial in trials)
                       and all(trial.get("cleanup_status") == "clean" for trial in trials)
                       and all(trial.get("status") == "passed" for trial in trials)
                       and all(trial.get("required_metrics_valid") is True for trial in trials))
    if formal_complete:
        formal_status = "formal_complete"
    elif args.mode == "formal":
        formal_status = "formal_partial"
    else:
        formal_status = "diagnostic_only"
    summary = {
        "run_id": run_dir.name, "mode": args.mode,
        "workload_class": parameters["workload_class"],
        "commitlog_sync": parameters["commitlog_sync"],
        "commitlog_sync_period_in_ms": parameters["commitlog_sync_period_in_ms"],
            "commitlog_use_o_dsync": False,
            "commitlog_total_space_in_mb": 1024,
            "commitlog_segment_size_in_mb": 64,
        "commitlog_sync_profile": parameters["commitlog_sync_profile"],
        "periodic_lwt_sync_evidence": parameters.get("periodic_lwt_sync_evidence"),
        "node_count": parameters.get("node_count", 3),
        "node_names": list(parameters.get("node_names", NODE_NAMES)),
        "wallet_profile": WALLET_PROFILE,
        "account_state_source": ACCOUNT_STATE_SOURCE,
        "ledger_key": list(LEDGER_KEY), "serial_cl": SERIAL_CL,
        "cql_requests_per_transaction": CQL_REQUESTS_PER_TRANSACTION,
        "cql_statement_count": CQL_STATEMENTS_PER_TRANSACTION,
        "measured_select_requests": 0,
        "shards_S": parameters["shards"], "workers_C": parameters["workers"],
        "replication_factor": parameters["replication_factor"],
        "read_cl": parameters["read_cl"], "write_cl": parameters["write_cl"],
        "per_shard_pool_P_override": parameters["per_shard_pool_override"],
        "worker_routing": parameters["worker_routing"],
        "connection_contract": "D=node_count*S*P data connections; C is the independent worker/in-flight upper bound",
        "io_properties_profile": {key: value for key, value in
                                  parameters.get("io_properties_profile", {"enabled": False}).items()
                                  if key != "content"},
        "queue_capacity": QUEUE_CAPACITY,
        "global_admission_capacity": QUEUE_CAPACITY,
        "worker_connection_profiles": [
            {"case_id": case_id_for_profile(case, parameters["commitlog_sync"],
                                             parameters["commitlog_sync_period_in_ms"]),
             "node_count": case.node_count,
             "node_names": list(node_names_for(case.node_count)),
             "S": case.shards, "C": case.workers,
             "P": case.per_shard_pool, "D": case.data_connection_target,
             "wallet_profile": case.wallet_profile,
             "account_state_source": ACCOUNT_STATE_SOURCE,
             "ledger_key": list(LEDGER_KEY), "serial_cl": SERIAL_CL,
             "cql_requests_per_transaction": CQL_REQUESTS_PER_TRANSACTION,
             "cql_statement_count": CQL_STATEMENTS_PER_TRANSACTION,
             "measured_select_requests": 0,
             "logical_mutations_per_successful_request": 1,
             "replication_factor": case.replication_factor,
             "read_cl": case.read_cl, "write_cl": case.write_cl,
             "commitlog_sync": parameters["commitlog_sync"],
             "commitlog_sync_period_in_ms": parameters["commitlog_sync_period_in_ms"],
        "commitlog_use_o_dsync": False,
        "commitlog_total_space_in_mb": 1024,
        "commitlog_segment_size_in_mb": 64,
             "commitlog_sync_profile": parameters["commitlog_sync_profile"],
             "worker_routing": case.worker_routing,
             "per_shard_pool_overridden": case.per_shard_pool_overridden,
             "requested_per_shard_pool": case.requested_per_shard_pool,
             "expected_connections_per_node": case.expected_connections_per_node,
             "setup_concurrency": case.setup_concurrency,
             "per_worker_queue_capacity": case.per_worker_queue_capacity}
            for case in parameters["cases"]],
        "formal_status": formal_status, "formal_complete": formal_complete,
        "formal_completion_rule": "wallet-memory-lwt-v1 batch profile only; exact 50,000x200, RF=3/read=THREE/write=THREE, S={1,2,4}, C={12,24,48}, repeats=3; 27 valid trials with required metrics and clean cleanup",
        "expected_case_count": len(parameters["cases"]), "actual_trial_count": len(trials),
        "blocked_error": blocked_error, "preflight": preflight,
        "trials": trials,
        "missing_trial_orders": sorted(set(case.order for case in parameters["cases"])
                                        - {trial.get("order") for trial in trials}),
        "created_utc": _utc_now(),
    }
    write_json(run_dir / "run-result.json", summary)
    fields = ["order", "trial_id", "status", "mode", "node_count", "node_names",
              "io_properties_sha256", "shards", "workers", "C", "P", "D",
              "commitlog_sync", "commitlog_sync_period_in_ms", "commitlog_sync_profile",
              "commitlog_use_o_dsync", "commitlog_total_space_in_mb",
              "commitlog_segment_size_in_mb",
              "wallet_profile", "account_state_source", "serial_cl",
              "cql_requests_per_transaction", "cql_statement_count", "measured_select_requests",
              "logical_wallet_mutations_expected", "measured_lwt_requests",
              "replication_factor", "read_cl", "write_cl",
              "worker_routing", "per_shard_pool_overridden", "requested_per_shard_pool",
              "expected_data_connections_per_node", "setup_concurrency",
              "per_worker_queue_capacity", "per_shard_pool",
              "repeat", "users", "requests_per_user", "total_requests", "rps", "p50_ns",
              "p95_ns", "p99_ns", "p999_ns", "max_ns", "duration_seconds",
              "logical_requests_expected", "logical_replies_received",
              "expected_application_cql_operations", "driver_cql_queries_delta",
              "process_cpu_used_ns", "process_cpu_ns_per_request", "heartbeat_max_lag_ns",
              "data_connections_ready", "data_connections_after_last_reply",
              "data_connections_min", "data_connections_max", "workload_cql_in_flight_peak",
              "commitlog_flush_delta_total", "commitlog_bytes_written_delta_total",
              "commitlog_cycle_delta_total", "commitlog_alloc_delta_total",
              "commitlog_flush_nodea_delta", "commitlog_flush_nodeb_delta",
              "commitlog_flush_nodec_delta",
              "required_metrics_valid", "cleanup_status", "image_digest", "source_dirty", "error"]
    with (run_dir / "summary.csv").open("w", encoding="utf-8", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        for trial in trials:
            writer.writerow({**{key: trial.get(key) for key in fields},
                             **_measurement_summary(trial),
                             **_trial_resource_summary(trial),
                             "commitlog_flush_delta_total": _counter_metric_total(
                                 trial, "scylla_commitlog_flush"),
                             "commitlog_bytes_written_delta_total": _counter_metric_total(
                                 trial, "scylla_commitlog_bytes_written"),
                             "commitlog_cycle_delta_total": _counter_metric_total(
                                 trial, "scylla_commitlog_cycle"),
                             "commitlog_alloc_delta_total": _counter_metric_total(
                                 trial, "scylla_commitlog_alloc"),
                             "commitlog_flush_nodea_delta": _counter_node_total(
                                 trial, "scylla_commitlog_flush", "nodea"),
                             "commitlog_flush_nodeb_delta": _counter_node_total(
                                 trial, "scylla_commitlog_flush", "nodeb"),
                             "commitlog_flush_nodec_delta": _counter_node_total(
                                 trial, "scylla_commitlog_flush", "nodec")})

    counter_fields = ["order", "trial_id", "node", "shard", "metric", "status",
                      "start_value", "start_sample_unix_ns", "start_sampling_offset_ns",
                      "end_value", "end_sample_unix_ns", "end_sampling_offset_ns",
                      "window_sample_count", "delta"]
    with (run_dir / "counter-deltas.csv").open("w", encoding="utf-8", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=counter_fields)
        writer.writeheader()
        for trial in trials:
            counter = (trial.get("monitor") or {}).get("counter_window_deltas") or {}
            for node in node_names_for(int(trial.get("node_count", 3))):
                metrics = counter.get("nodes", {}).get(node, {})
                for metric in (*COMMITLOG_COUNTERS, *COMMITLOG_GAUGES,
                               "scylla_network_bytes_sent", "scylla_network_bytes_received"):
                    series = metrics.get(metric, {}).get("series", {})
                    for shard, evidence in series.items():
                        writer.writerow({"order": trial.get("order"),
                                         "trial_id": trial.get("trial_id"),
                                         "node": node, "shard": shard, "metric": metric,
                                         **{key: evidence.get(key) for key in counter_fields
                                            if key in evidence}})
    case_groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for trial in trials:
        case_groups.setdefault((trial["shards"], trial["workers"], trial["per_shard_pool"],
                                trial.get("worker_routing", "round-robin"),
                                bool(trial.get("per_shard_pool_overridden", False)),
                                trial.get("replication_factor", 3),
                                trial.get("read_cl", "THREE"),
                                trial.get("write_cl", "THREE"),
                                trial.get("wallet_profile", "unknown"),
                                int(trial.get("node_count", parameters.get("node_count", 3))),
                                trial.get("io_properties_sha256") or "",
                                trial.get("commitlog_sync", "batch"),
                                trial.get("commitlog_sync_period_in_ms")), []).append(trial)
    aggregate_fields = ["node_count", "shards_S", "workers_C", "per_shard_pool_P",
                        "data_connections_D", "io_properties_sha256", "commitlog_sync",
                        "commitlog_sync_period_in_ms", "commitlog_use_o_dsync",
                        "wallet_profile", "replication_factor", "read_cl", "write_cl",
                        "worker_routing", "per_shard_pool_overridden", "expected_connections_per_node",
                        "repeats", "rps_median",
                        "rps_min", "rps_max", "p50_ns_median", "p50_ns_min", "p50_ns_max",
                        "p95_ns_median", "p95_ns_min", "p95_ns_max",
                        "p99_ns_median", "p99_ns_min", "p99_ns_max", "all_trials_valid"]
    with (run_dir / "case-summary.csv").open("w", encoding="utf-8", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=aggregate_fields)
        writer.writeheader()
        for (shards, workers, per_shard_pool, worker_routing, pool_overridden,
             replication_factor, read_cl, write_cl, wallet_profile, node_count,
             io_properties_sha256, commitlog_sync, commitlog_sync_period), group in sorted(case_groups.items()):
            values = {name: [float(_measurement_summary(item)[name]) for item in group
                             if _measurement_summary(item).get(name) is not None]
                      for name in ("rps", "p50_ns", "p95_ns", "p99_ns")}
            row: dict[str, Any] = {"node_count": node_count,
                                   "shards_S": shards, "workers_C": workers,
                                   "per_shard_pool_P": per_shard_pool,
                                   "data_connections_D": node_count * shards * per_shard_pool,
                                   "io_properties_sha256": io_properties_sha256,
                                   "wallet_profile": wallet_profile,
                                   "commitlog_sync": commitlog_sync,
                                   "commitlog_sync_period_in_ms": commitlog_sync_period,
                                   "commitlog_use_o_dsync": False,
                                   "replication_factor": replication_factor,
                                   "read_cl": read_cl, "write_cl": write_cl,
                                   "worker_routing": worker_routing,
                                   "per_shard_pool_overridden": pool_overridden,
                                   "expected_connections_per_node": shards * per_shard_pool,
                                   "repeats": len(group),
                                   "all_trials_valid": all(item.get("status") == "passed"
                                                           and item.get("required_metrics_valid") is True
                                                           and item.get("cleanup_status") == "clean"
                                                           for item in group)}
            for name, samples in values.items():
                row[f"{name}_median"] = statistics.median(samples) if samples else None
                row[f"{name}_min"] = min(samples) if samples else None
                row[f"{name}_max"] = max(samples) if samples else None
            writer.writerow(row)
    report_lines = [
        "# ScyllaDB Ledger Wallet Benchmark 執行報告",
        "",
        f"- Run：`{run_dir.name}`",
        f"- 模式：`{args.mode}`；工作負載分類：`{parameters['workload_class']}`",
        f"- 正式完整性：`{formal_status}`（formal_complete={str(formal_complete).lower()}）",
        f"- Nodes：{parameters.get('node_count', 3)} ({', '.join(parameters.get('node_names', NODE_NAMES))})；Trial：{len(trials)}/{len(parameters['cases'])}；正式完整必須 27/27 通過必要 metrics 與 cleanup。",
        f"- Image：`{PINNED_REPO_DIGEST}`",
        f"- Wallet implementation：`{WALLET_PROFILE}`；state source=`memory`，ledger key=`{','.join(LEDGER_KEY)}`，serial CL=`{SERIAL_CL}`。",
        f"- Durability profile：RF={parameters['replication_factor']}，wallet read CL={parameters['read_cl']}，write CL={parameters['write_cl']}；commitlog_sync={parameters['commitlog_sync']}，period={sync_period_label} ms，commitlog_use_o_dsync=false，total=1024 MiB，segment=64 MiB。",
        f"- 預估磁碟需求按 RF={parameters['replication_factor']} replica，另含 compaction、{parameters.get('node_count', 3)}-node commitlog 與 16 GiB reserve：{estimate_disk_bytes(parameters['users'], parameters['requests_per_user'], parameters['replication_factor'], parameters.get('node_count', 3))['required_with_reserve_bytes']} bytes。",
        "- 本機 profile 固定 developer-mode=1、io-setup=0、overprovisioned=1；共用 host/device 的結果不代表 production tuning。",
        f"- schema/system metadata bootstrap 與 system.clients audit 使用 CL=ONE；這些操作在量測窗外。Wallet seed/read/write/replay/verify 使用本 trial 設定的 read={parameters['read_cl']}、write={parameters['write_cl']}。",
        ("- 同一 host 三節點不代表獨立故障域；" if parameters.get("node_count", 3) == 3
         else "- 單節點配置沒有副本冗餘；") +
        "closed-loop 有限負載不證明 steady-state capacity、hot-account 或長期 partition growth。",
        f"- I/O properties profile：{'enabled' if parameters.get('io_properties_profile', {}).get('enabled') else 'not requested'}；SHA-256={parameters.get('io_properties_profile', {}).get('source_sha256')}。啟用時需通過唯讀掛載、執行中 argv、data/commitlog device 與每 shard queue mountpoint 驗證；空 mountpoint fallback 另存，不代表其他 device I/O 受控。",
        ("- Periodic 10000 ms 為獨立 pilot profile；immutable pinned source 與 Scylla 2026.1 LWT 說明已核對：LWT force_sync 路徑在所有 sync mode 都會同步到裝置。這不表示每筆交易恰好一次物理 fsync，也不排除 coalescing；普通 periodic writes 仍不以同步 ack 解讀。"
         if parameters["commitlog_sync"] == "periodic" else
         "- 逐筆 fsync profile：unsupported；沒有以 periodic 或 batch-window sweep 代替。"),
        "- Commitlog counters 是 Scylla shard counters，不是 request 數；flush/bytes_written/cycle/alloc 的邊界 delta 與取樣偏移見 `counter-deltas.csv`。每個 accepted transaction 在量測窗使用 1 個 LWT CQL request/statement，沒有量測窗內 SELECT；成功的新交易對應 1 個 wallet partition mutation。Paxos 可能產生額外內部儲存與 commitlog 活動；`applied=false` 不代表沒有 fsync。",
    ]
    if blocked_error:
        report_lines += ["", f"Formal preflight/執行未啟動或中止：{blocked_error}"]
    report_lines += ["", "| Trial | Nodes | Wallet profile | Sync | Period ms | S | C | P | D | RF | Read CL | Write CL | Routing | 狀態 | 必要 metrics | Cleanup | RPS | p99 (ns) |",
                     "|---|---:|---|---|---:|---:|---:|---:|---:|---:|---|---|---|---|---|---|---:|---:|"]
    for trial in trials:
        metric = "pass" if trial.get("required_metrics_valid") else "missing/invalid"
        measure = _measurement_summary(trial)
        report_lines.append(
            f"| {trial.get('trial_id')} | {trial.get('node_count', 3)} | {trial.get('wallet_profile', 'unknown')} | "
            f"{trial.get('commitlog_sync', 'batch')} | "
            f"{trial.get('commitlog_sync_period_in_ms') if trial.get('commitlog_sync') == 'periodic' else 'unused'} | "
            f"{trial.get('shards')} | {trial.get('workers')} | "
            f"{trial.get('per_shard_pool')} | {trial.get('D')} | {trial.get('replication_factor', 3)} | "
            f"{trial.get('read_cl', 'THREE')} | {trial.get('write_cl', 'THREE')} | {trial.get('worker_routing')} | "
            f"{trial.get('status')} | {metric} | "
            f"{trial.get('cleanup_status')} | {measure.get('rps')} | {measure.get('p99_ns')} |")
    report_lines += ["", "## Commitlog counter window deltas",
                     "",
                     "以下數值按 Scylla node/shard 保留。起止值使用窗口邊界前最近 scrape；sampling offset 同時保存。缺值或 counter reset 保持 N/A。",
                     "",
                     "| Trial | Node | Shard | Flush | Bytes written | Cycle | Alloc | Start offset (ns) | End offset (ns) |",
                     "|---|---|---:|---:|---:|---:|---:|---:|---:|"]
    for trial in trials:
        counter = (trial.get("monitor") or {}).get("counter_window_deltas") or {}
        for node in node_names_for(int(trial.get("node_count", 3))):
            metrics = counter.get("nodes", {}).get(node, {})
            shards_in_data = sorted(metrics.get("scylla_commitlog_flush", {}).get("series", {}),
                                    key=lambda value: (not value.isdigit(), value))
            for shard in shards_in_data:
                items = [metrics.get(name, {}).get("series", {}).get(shard, {})
                         for name in COMMITLOG_COUNTERS]
                values = [item.get("delta") if item.get("status") == "observed" else "N/A"
                          for item in items]
                start_offsets = [item.get("start_sampling_offset_ns") for item in items]
                end_offsets = [item.get("end_sampling_offset_ns") for item in items]
                report_lines.append(
                    f"| {trial.get('trial_id')} | {node} | {shard} | "
                    f"{values[0]} | {values[1]} | {values[2]} | {values[3]} | "
                    f"{start_offsets[0]} | {end_offsets[0]} |")
    report_lines += ["", "詳細 raw Prometheus、process/host counters、連線 audit、版本、source hash、container logs 與每 trial JSON 均保留在本 run 目錄。",
                     "只要 trial 缺失、required metric 缺漏、validation 失敗或 cleanup 不乾淨，就不會標記 formal_complete。"]
    (run_dir / "report.zh-TW.md").write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    return summary


def runner_exit_code(mode: str, expected_trial_count: int,
                     trials: Sequence[dict[str, Any]], blocked_error: str | None,
                     formal_complete: bool) -> int:
    if mode == "formal":
        return 0 if formal_complete else 1
    diagnostic_ok = (blocked_error is None and len(trials) == expected_trial_count
                     and all(trial.get("status") in ("passed", "passed_relaxed")
                             and trial.get("cleanup_status") == "clean"
                             for trial in trials))
    return 0 if diagnostic_ok else 1


def recover_preflight_artifact(run_dir: pathlib.Path,
                              preflight: dict[str, Any] | None) -> dict[str, Any] | None:
    if preflight is not None:
        return preflight
    path = run_dir / "formal-preflight.json"
    if not path.is_file():
        return None
    try:
        return read_json_object(path)
    except RunnerError:
        return {"valid": False, "error": f"could not reload preflight artifact: {path}"}


def main(argv: Sequence[str] | None = None) -> int:
    parser = create_argument_parser()
    args = parser.parse_args(argv)
    try:
        parameters = resolve_run_parameters(args)
    except (RunnerError, argparse.ArgumentTypeError) as exc:
        parser.error(str(exc))
    args.resolved_io_properties_profile = parameters["io_properties_profile"]
    args.resolved_host_io_limits = parameters["host_io_limits"]
    args.resolved_commitlog_sync = parameters["commitlog_sync"]
    args.resolved_commitlog_sync_period_in_ms = parameters["commitlog_sync_period_in_ms"]
    if args.dry_run:
        print(json.dumps(dry_run_report(args, parameters), indent=2, sort_keys=True))
        return 0

    output_root = args.output_root.expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    run_id = datetime.datetime.now(datetime.timezone.utc).strftime("run-%Y%m%dT%H%M%SZ-") + uuid.uuid4().hex[:8]
    run_dir = output_root / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    estimate = estimate_disk_bytes(parameters["users"], parameters["requests_per_user"],
                                    parameters["replication_factor"], parameters["node_count"])
    initial_host_inventory = host_inventory(run_dir)
    write_json(run_dir / "run-config.json", {
        "run_id": run_id, "mode": args.mode, "workload_class": parameters["workload_class"],
        "commitlog_sync": parameters["commitlog_sync"],
        "commitlog_sync_period_in_ms": parameters["commitlog_sync_period_in_ms"],
        "commitlog_use_o_dsync": False,
        "commitlog_total_space_in_mb": 1024,
        "commitlog_segment_size_in_mb": 64,
        "commitlog_sync_profile": parameters["commitlog_sync_profile"],
        "periodic_lwt_sync_evidence": (
            {"required": True, "source_commit": PERIODIC_SYNC_LWT_SOURCE_COMMIT,
             "manifest_path": str((run_dir / "periodic-lwt-source-evidence" / "manifest.json").resolve()),
             "status": "must validate before starting any trial"}
            if parameters["commitlog_sync"] == "periodic" else {"required": False}),
        "wallet_profile": WALLET_PROFILE,
        "account_state_source": ACCOUNT_STATE_SOURCE,
        "setup_state_source": "db",
        "ledger_key": list(LEDGER_KEY), "serial_cl": SERIAL_CL,
        "cql_requests_per_transaction": CQL_REQUESTS_PER_TRANSACTION,
        "cql_statement_count": CQL_STATEMENTS_PER_TRANSACTION,
        "measured_select_requests": 0,
        "logical_mutations_per_successful_request": 1,
        "paxos_internal_storage_activity_note": (
            "LWT may produce additional Paxos/internal storage and commitlog activity; "
            "applied=false does not imply zero fsync"),
        "node_count": parameters["node_count"],
        "node_names": list(parameters["node_names"]),
        "shards": parameters["shards"], "workers_C": parameters["workers"],
        "replication_factor": parameters["replication_factor"],
        "read_cl": parameters["read_cl"], "write_cl": parameters["write_cl"],
        "per_shard_pool_P_override": parameters["per_shard_pool_override"],
        "worker_routing": parameters["worker_routing"],
        "connection_contract": "D=node_count*S*P data connections; C is the independent worker/in-flight upper bound",
        "repeats": parameters["repeats"], "users": parameters["users"],
        "requests_per_user": parameters["requests_per_user"],
        "queue_capacity": QUEUE_CAPACITY, "global_admission_capacity": QUEUE_CAPACITY,
        "rotation_matrix": [asdict(case) for case in parameters["cases"]],
        "capacity_estimate": estimate, "pinned_image": PINNED_REPO_DIGEST,
        "io_properties_profile": {key: value for key, value in
                                  parameters["io_properties_profile"].items()
                                  if key != "content"},
        "host_io_limits": parameters["host_io_limits"],
        "host_inventory_before": initial_host_inventory,
        "aio_budget_by_shards": {
            str(shards): compute_aio_budget(
                shards, initial_host_inventory.get("aio_max_nr"),
                initial_host_inventory.get("aio_nr"), parameters["node_count"])
            for shards in parameters["shards"]},
        "python_monitor_cpu_affinity": list(SERVICE_CPUS),
        "server_local_profile": {"developer_mode": 1, "io_setup": 0, "overprovisioned": 1,
                                 "aio": scylla_aio_profile_evidence(),
                                 "io_properties_profile": {key: value for key, value in
                                                           parameters["io_properties_profile"].items()
                                                           if key != "content"},
                                 "io_properties_launch": (
                                     {"flag": "--io-properties-file",
                                      "container_path": IO_PROPERTIES_CONTAINER_PATH,
                                      "read_only_bind_mount": True}
                                     if parameters["io_properties_profile"]["enabled"]
                                     else {"status": "not_requested"})},
    })
    trials: list[dict[str, Any]] = []
    preflight: dict[str, Any] | None = None
    blocked_error: str | None = None
    try:
        if args.mode == "formal":
            preflight = strict_formal_preflight(run_dir / "formal-preflight.json",
                                                estimate["required_with_reserve_bytes"],
                                                args.preflight_wait, data_path=run_dir)
        else:
            preflight = {"valid": True, "mode": "relaxed_diagnostic",
                         "host_inventory": host_inventory(run_dir),
                         "note": "smoke/pilot does not use formal host-utilization admission"}
            write_json(run_dir / "formal-preflight.json", preflight)
        source_evidence(run_dir)
        periodic_evidence = None
        if parameters["commitlog_sync"] == "periodic":
            periodic_evidence = capture_periodic_lwt_source_evidence(run_dir)
        parameters["periodic_lwt_sync_evidence"] = periodic_evidence
        run_config_path = run_dir / "run-config.json"
        run_config = read_json_object(run_config_path)
        run_config["periodic_lwt_sync_evidence"] = (
            periodic_evidence if periodic_evidence is not None else {"required": False})
        write_json(run_config_path, run_config)
        args.periodic_lwt_sync_source_evidence = periodic_evidence
        verify_image_is_pulled()
        runner_affinity = apply_runner_affinity()
        write_json(run_dir / "runner-affinity.json", runner_affinity)
        binary = _cli_binary_path(args, run_dir)
        blocked_error = run_trial_matrix(
            run_id, run_dir, parameters["cases"], args,
            parameters["users"], parameters["requests_per_user"], binary, trials)
    except Exception as exc:
        blocked_error = f"{type(exc).__name__}: {exc}"
        if not trials:
            write_json(run_dir / "pre-run-error.json", {"error": blocked_error})
    preflight = recover_preflight_artifact(run_dir, preflight)
    summary = write_run_reports(run_dir, args, parameters, trials, preflight, blocked_error)
    print(json.dumps({"run_dir": str(run_dir), "formal_status": summary["formal_status"],
                      "formal_complete": summary["formal_complete"],
                      "trial_count": len(trials), "blocked_error": blocked_error},
                     indent=2, sort_keys=True))
    return runner_exit_code(args.mode, len(parameters["cases"]), trials,
                            blocked_error, summary["formal_complete"])


if __name__ == "__main__":
    raise SystemExit(main())
