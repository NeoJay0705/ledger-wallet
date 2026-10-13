#!/usr/bin/env python3
"""Pinned, owned-container comparison of synchronous Scylla and PostgreSQL wallets."""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import pathlib
import re
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from dataclasses import asdict, dataclass
from typing import Any, Sequence
from urllib.parse import quote

try:
    from . import scylla_io_observer as disk_observer
    from . import scylla_wallet as wallet_runner
except ImportError:  # direct `python3 benches/wallet_db_compare.py` invocation
    import scylla_io_observer as disk_observer
    import scylla_wallet as wallet_runner


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
CRATE_DIR = REPO_ROOT / "benchmarks" / "scylla-wallet"
PROFILE_DIR = CRATE_DIR / "profiles" / "equal-resource-20261013"
SQL_PATH = CRATE_DIR / "sql" / "postgres_wallet.sql"
SCYLLA_IO_PROPERTIES_PATH = PROFILE_DIR / "io_properties.yaml"
SCYLLA_IO_PROPERTIES_RELATIVE_PATH = SCYLLA_IO_PROPERTIES_PATH.relative_to(REPO_ROOT).as_posix()
SCYLLA_IO_PROPERTIES_SHA256 = "12bf0b79c4520ca6d00183ca14b87d321b9221b687cc6e889619876f5b87b2bf"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "target" / "wallet-db-compare" / "runs"
DEFAULT_CAPS_PATH = PROFILE_DIR / "io_caps.json"
PG_IMAGE_REF = "docker.io/library/postgres@sha256:afc7e2d441324c0388fa80c3d24f733b4194a4eb7f47dd8ee2b08eb1a24a647c"
PG_IMAGE_ID = "sha256:2e182ed2ff8f5d0835353864264437a26dffc542a9a6c677ec495aa54e491627"
PG_VERSION = "18.6"
PG_RUNTIME_VERSION = "18.6"
PG_RUNTIME_VERSION_NUM = 180006
PG_DATABASE = "walletbench"
PG_SERVICE_APPNAME = "ledger-wallet-postgres-wallet-bench"
PG_MONITOR_APPNAME = "ledger-wallet-pg-monitor"
PG_TABLE_MONITOR_APPNAME = "ledger-wallet-pg-table-monitor"
POSTGRES_SCHEMA_SQL_SHA256 = "7a26158b91785a0a8eb8da2950608d662f530eeecab5fe584535506da010e5f1"
PG_CGROUP_MEMORY_BYTES = 10 * 1024**3
PG_SHARED_SWAP_BYTES = PG_CGROUP_MEMORY_BYTES
PG_DATABASE_CPUS = (0, 1)
SERVICE_CPUS = wallet_runner.SERVICE_CPUS
USERS = 1_024
REQUESTS_PER_USER = 1_000
TOTAL_REQUESTS = USERS * REQUESTS_PER_USER
WORKERS_C = 512
QUEUE_CAPACITY = 50_000
SETUP_CONCURRENCY = 6
PG_CALIBRATION_CONNECTIONS = (16, 32, 64)
SCYLLA_CONNECTIONS = 6
SCYLLA_SHARDS = 2
SCYLLA_POOL_PER_SHARD = 3
SCYLLA_CASE_REPEATS = 3
PG_CASE_REPEATS = 3
MIN_DISK_BYTES = 64 * 1024**3
MIN_AVAILABLE_MEMORY_BYTES = 32 * 1024**3
EXPECTED_RELEASE_BINARY_SHA256 = "02ddf0f843ab8a7a15d2f20cc3ed35f6d7661fcbd1d4e4141250672fcba7e555"
PREFLIGHT_SECONDS = 60
PG_STARTUP_SECONDS = 600
PG_READY_SECONDS = 900
CLI_READY_SECONDS = 900
CLI_GATE_SECONDS = 180
CLI_MEASURE_SECONDS = 12 * 60 * 60
CLI_FINISH_SECONDS = 180
CONTAINER_STOP_SECONDS = 60
SAMPLE_INTERVAL_SECONDS = 1.0
SAMPLE_MAX_GAP_SECONDS = 2.5
PG_WAL_COUNTERS = ("wal_records", "wal_fpi", "wal_bytes", "wal_buffers_full")
PG_DATABASE_COUNTERS = (
    "xact_commit", "xact_rollback", "tup_inserted", "tup_updated", "deadlocks",
)
PG_IO_COUNTERS = ("writes", "write_time", "fsyncs", "fsync_time")
PG_SETTINGS_EXPECTED = {
    "shared_buffers": (2 * 1024**3, "bytes"),
    "work_mem": (4 * 1024**2, "bytes"),
    "maintenance_work_mem": (256 * 1024**2, "bytes"),
    "autovacuum_work_mem": (64 * 1024**2, "bytes"),
    "max_connections": (100, "integer"),
    "fsync": ("on", "text"),
    "synchronous_commit": ("on", "text"),
    "full_page_writes": ("on", "text"),
    "wal_sync_method": ("fdatasync", "text"),
    "commit_delay": (0, "integer"),
    "wal_level": ("replica", "text"),
    "checkpoint_timeout": (300, "seconds"),
    "max_wal_size": (1024**3, "bytes"),
    "autovacuum": ("on", "text"),
    "track_io_timing": ("on", "text"),
    "track_wal_io_timing": ("on", "text"),
}


class CompareError(RuntimeError):
    """A comparison configuration, evidence, workload, or ownership failure."""


@dataclass(frozen=True)
class CompareCase:
    order: int
    backend: str
    repeat: int
    pg_connections: int | None = None

    @property
    def case_id(self) -> str:
        if self.backend == "postgres" and self.order <= len(PG_CALIBRATION_CONNECTIONS):
            return f"pg-calibration-D{self.pg_connections}"
        return f"{self.backend}-rep{self.repeat}"


def campaign_cases(selected_pg_connections: int | None = None) -> list[CompareCase]:
    """Return three PG calibration trials and, once selected, the six fixed comparisons."""
    cases = [CompareCase(index, "postgres", index, connections)
             for index, connections in enumerate(PG_CALIBRATION_CONNECTIONS, 1)]
    if selected_pg_connections is None:
        return cases
    if selected_pg_connections not in PG_CALIBRATION_CONNECTIONS:
        raise CompareError("selected PG connection count must be one of 16, 32, or 64")
    schedule = ("scylla", "postgres", "postgres", "scylla", "scylla", "postgres")
    repeat_by_backend = {"scylla": 0, "postgres": 0}
    for backend in schedule:
        repeat_by_backend[backend] += 1
        cases.append(CompareCase(len(cases) + 1, backend, repeat_by_backend[backend],
                                 selected_pg_connections if backend == "postgres" else None))
    return cases


def select_postgres_connections(calibration_trials: Sequence[dict[str, Any]],
                                tolerance: float = 0.95) -> dict[str, Any]:
    """Choose the smallest fully valid D within tolerance of the best calibration RPS."""
    if len(calibration_trials) != len(PG_CALIBRATION_CONNECTIONS):
        raise CompareError("connection calibration must contain exactly the D=16,32,64 trials")
    by_d: dict[int, dict[str, Any]] = {}
    for trial in calibration_trials:
        connections = trial.get("pg_connections")
        if isinstance(connections, bool) or not isinstance(connections, int) or connections not in PG_CALIBRATION_CONNECTIONS:
            raise CompareError(f"calibration has invalid PG connection count: {connections!r}")
        if connections in by_d:
            raise CompareError(f"calibration repeats D={connections}")
        if trial.get("valid") is not True:
            raise CompareError(f"D={connections} calibration trial is invalid; stopping selection")
        if trial.get("requests") != TOTAL_REQUESTS or trial.get("full_verification") is not True:
            raise CompareError(f"D={connections} calibration lacks the full correct workload and verification")
        if trial.get("cleanup_status") != "passed":
            raise CompareError(f"D={connections} calibration cleanup did not pass")
        rps = trial.get("rps")
        if isinstance(rps, bool) or not isinstance(rps, (int, float)) or rps <= 0:
            raise CompareError(f"D={connections} calibration RPS is missing or nonpositive")
        by_d[connections] = trial
    if set(by_d) != set(PG_CALIBRATION_CONNECTIONS):
        raise CompareError("calibration must have exactly one valid trial for each D=16,32,64")
    best = max(float(trial["rps"]) for trial in by_d.values())
    threshold = best * tolerance
    selected = min(d for d, trial in by_d.items() if float(trial["rps"]) >= threshold)
    return {"valid": True, "selection_rule": "smallest D within 95% of best valid RPS",
            "tolerance": tolerance, "best_rps": best, "threshold_rps": threshold,
            "selected_pg_connections": selected,
            "calibration": [{"D": d, "rps": float(by_d[d]["rps"]),
                             "within_threshold": float(by_d[d]["rps"]) >= threshold}
                            for d in PG_CALIBRATION_CONNECTIONS]}


def canonical_json_bytes(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"),
                        ensure_ascii=False, allow_nan=False) + "\n").encode()


def sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def load_host_io_limits(path: pathlib.Path) -> dict[str, Any]:
    limits = wallet_runner.load_host_io_limits(path)
    if limits is None:
        raise CompareError("common host I/O caps are required for every comparison case")
    return limits


def _expected_events(events: Sequence[dict[str, Any]], schema: str,
                     pg_connections: int, sql_sha256: str) -> dict[str, Any]:
    if not events:
        raise CompareError("CLI emitted no PostgreSQL events")
    phases = ["startup", "seeding", "preloading", "ready_to_measure", "measuring",
              "client_end", "drained", "verifying", "verified",
              "awaiting_runner_audit", "runner_audit_released"]
    by_phase: dict[str, list[dict[str, Any]]] = {}
    for event in events:
        if event.get("status") == "failed" or event.get("phase") == "finish_gate_timeout":
            raise CompareError(f"PostgreSQL CLI emitted a failed lifecycle event: {event.get('phase')}")
        if not isinstance(event.get("timestamp_unix_ns"), int) or not isinstance(event.get("elapsed_ns"), int):
            raise CompareError("PostgreSQL event is missing monotonic lifecycle timestamps")
        if event.get("backend") != "postgres":
            raise CompareError(f"CLI event has unexpected backend: {event.get('backend')!r}")
        if event.get("wallet_profile") != "postgres-memory-tx-v1":
            raise CompareError("CLI event is missing wallet_profile=postgres-memory-tx-v1")
        if event.get("pg_schema") != schema:
            raise CompareError("CLI event PostgreSQL schema differs from this owned case")
        if event.get("mode") != "pilot":
            raise CompareError("CLI PostgreSQL event mode differs from approved pilot profile")
        for name, expected in (("C", WORKERS_C), ("database_name", PG_DATABASE),
                               ("pg_connections", pg_connections),
                               ("db_cpu_count", len(PG_DATABASE_CPUS)),
                               ("schema_sql_file", "benchmarks/scylla-wallet/sql/postgres_wallet.sql"),
                               ("schema_sql_sha256", sql_sha256),
                               ("application_name", PG_SERVICE_APPNAME),
                               ("ledger_key", ["account_id", "tx_id"]),
                               ("accounts_table", "accounts"),
                               ("accounts_primary_key_constraint", "accounts_pkey"),
                               ("ledger_table", "ledger"),
                               ("ledger_primary_key_constraint", "ledger_pkey"),
                               ("ledger_primary_key", ["account_id", "tx_id"]),
                               ("sql_function_calls_per_transaction", 1),
                               ("client_query_timeout_ms", 30_000),
                               ("data_connection_target", pg_connections),
                               ("worker_routing", "round-robin"),
                               ("setup_concurrency", SETUP_CONCURRENCY),
                               ("state_preload_concurrency", SETUP_CONCURRENCY),
                               ("per_worker_queue_capacity", QUEUE_CAPACITY),
                               ("global_admission_capacity", QUEUE_CAPACITY),
                               ("users", USERS), ("requests_per_user", REQUESTS_PER_USER),
                               ("total_requests", TOTAL_REQUESTS)):
            if event.get(name) != expected:
                raise CompareError(f"CLI {event.get('phase')} event {name}={event.get(name)!r}; expected {expected!r}")
        expected_source = ("memory" if event.get("phase") in
                           {"ready_to_measure", "measuring", "client_end", "drained",
                            "verifying", "verified", "awaiting_runner_audit",
                            "runner_audit_released"} else "db")
        if event.get("account_state_source") != expected_source:
            raise CompareError(f"CLI {event.get('phase')} event account_state_source is not {expected_source}")
        forbidden = {"keyspace", "nodes", "S", "P", "replication_factor",
                     "read_cl", "write_cl", "serial_cl", "cql_statement_count",
                     "cql_requests_per_transaction", "paxos"}
        if forbidden.intersection(event):
            raise CompareError("PostgreSQL event contains Scylla/CQL/Paxos-only workload fields")
        by_phase.setdefault(str(event.get("phase", "unknown")), []).append(event)
    observed = {}
    unknown_phases = sorted(set(by_phase) - set(phases))
    if unknown_phases:
        raise CompareError(f"PostgreSQL CLI emitted unexpected lifecycle phases: {unknown_phases}")
    for phase in phases:
        candidates = by_phase.get(phase, [])
        expected_count = 2 if phase in {"seeding", "preloading"} else 1
        if len(candidates) != expected_count:
            raise CompareError(f"CLI must emit exactly {expected_count} {phase} event(s); found {len(candidates)}")
        observed[phase] = candidates[-1]
    ready = observed["ready_to_measure"]
    for name, expected in (("C", WORKERS_C), ("users", USERS),
                           ("requests_per_user", REQUESTS_PER_USER),
                           ("global_admission_capacity", QUEUE_CAPACITY),
                           ("pg_connections", pg_connections),
                           ("db_cpu_count", len(PG_DATABASE_CPUS)),
                           ("setup_concurrency", SETUP_CONCURRENCY),
                           ("state_preload_concurrency", SETUP_CONCURRENCY),
                           ("data_connection_target", pg_connections),
                           ("account_states_preloaded", USERS),
                           ("all_clients_ready", True),
                           ("database_name", PG_DATABASE),
                           ("schema_sql_file", "benchmarks/scylla-wallet/sql/postgres_wallet.sql"),
                           ("schema_sql_sha256", sql_sha256),
                           ("application_name", PG_SERVICE_APPNAME),
                           ("ledger_key", ["account_id", "tx_id"]),
                           ("accounts_table", "accounts"),
                           ("accounts_primary_key_constraint", "accounts_pkey"),
                           ("ledger_table", "ledger"),
                           ("ledger_primary_key_constraint", "ledger_pkey"),
                           ("ledger_primary_key", ["account_id", "tx_id"]),
                           ("sql_function_calls_per_transaction", 1)):
        actual = ready.get(name)
        if actual != expected:
            raise CompareError(f"ready_to_measure {name}={actual!r}; expected {expected}")
    if ready.get("pool_ready_connections") != pg_connections:
        raise CompareError("ready_to_measure pool_ready_connections does not match D")
    ready_pids = ready.get("postgres_backend_pids")
    if (not isinstance(ready_pids, list) or len(ready_pids) != pg_connections
            or any(isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0 for pid in ready_pids)
            or len(set(ready_pids)) != pg_connections):
        raise CompareError("ready_to_measure postgres_backend_pids does not identify all D pool connections")
    for name, expected in (("backend", "postgres"), ("wallet_profile", "postgres-memory-tx-v1"),
                           ("account_state_source", "memory"),
                           ("data_connection_target", pg_connections),
                           ("worker_routing", "round-robin"),
                           ("per_worker_queue_capacity", QUEUE_CAPACITY),
                           ("total_requests", TOTAL_REQUESTS),
                           ("client_query_timeout_ms", 30_000)):
        if ready.get(name) != expected:
            raise CompareError(f"ready_to_measure {name}={ready.get(name)!r}; expected {expected!r}")
    for phase in ("seeding", "preloading"):
        rows = by_phase[phase]
        if rows[0].get("status") != "started" or rows[-1].get("status") != "complete":
            raise CompareError(f"PG {phase} must emit one started event before one complete event")
        if events.index(rows[0]) >= events.index(rows[-1]):
            raise CompareError(f"PG {phase} completion precedes its start")
    if by_phase["seeding"][-1].get("accounts_seeded") != USERS:
        raise CompareError("PG seeding did not complete all 1024 accounts")
    if by_phase["preloading"][-1].get("account_states_preloaded") != USERS:
        raise CompareError("PG preloading did not complete all 1024 account states")
    measuring = observed["measuring"]
    client_end = observed["client_end"]
    start_ns = measuring.get("measurement_start_unix_ns")
    end_ns = client_end.get("measurement_end_unix_ns")
    if not isinstance(start_ns, int) or not isinstance(end_ns, int) or end_ns <= start_ns:
        raise CompareError("CLI measured-window event timestamps are missing or invalid")
    if client_end.get("measurement_start_unix_ns") != start_ns:
        raise CompareError("measuring and client_end events report different measurement starts")
    if client_end.get("started") is not True or client_end.get("replies_received") != TOTAL_REQUESTS:
        raise CompareError("CLI client_end does not report the complete measured workload")
    if observed["verified"].get("valid") is not True:
        raise CompareError("CLI verified event does not report valid full verification")
    if observed["verified"].get("accounts_verified") != USERS or observed["verified"].get("failures") != 0:
        raise CompareError("CLI verified event does not report all accounts and zero failures")
    if observed["drained"].get("service_failed") is not False:
        raise CompareError("CLI drained event reports a failed service")
    session_id = ready.get("session_id")
    for event in events:
        if event.get("phase") != "startup" and event.get("session_id") != session_id:
            raise CompareError("PostgreSQL CLI lifecycle changed session identity after connect")
    phase_order = [events.index(observed[phase]) for phase in
                   ("startup", "seeding", "preloading", "ready_to_measure", "measuring", "client_end", "drained",
                    "verifying", "verified", "awaiting_runner_audit", "runner_audit_released")]
    if phase_order != sorted(phase_order):
        raise CompareError("PostgreSQL CLI lifecycle phases are out of order")
    return {"valid": True, "phase_counts": {phase: len(rows) for phase, rows in by_phase.items()},
            "required_phases": phases, "pool_ready_connections": pg_connections,
            "postgres_backend_pids": ready_pids,
            "setup_concurrency": SETUP_CONCURRENCY,
            "measurement_start_unix_ns": start_ns, "measurement_end_unix_ns": end_ns}


def validate_pg_connection_pool(pool: Any, connections: int) -> dict[str, Any]:
    if not isinstance(pool, dict):
        raise CompareError("CLI result is missing PostgreSQL connection_pool evidence")
    expected = {"target_total_data_connections": connections,
                "pool_ready_connections": connections,
                "pool_after_last_reply": connections,
                "minimum_sampled_connections": connections,
                "maximum_sampled_connections": connections,
                "mismatch_samples": 0, "valid": True}
    mismatches = {name: {"expected": value, "observed": pool.get(name)}
                  for name, value in expected.items() if pool.get(name) != value}
    if mismatches:
        raise CompareError(f"PostgreSQL pool report differs from D={connections}: {mismatches}")
    sampling_period = pool.get("sampling_period_ns")
    if (isinstance(sampling_period, bool) or not isinstance(sampling_period, int)
            or sampling_period <= 0):
        raise CompareError("PostgreSQL pool sampling_period_ns is missing or invalid")
    if not isinstance(pool.get("metric_source"), str) or not pool["metric_source"]:
        raise CompareError("PostgreSQL pool metric_source is missing")
    sample_count = _integer(pool.get("samples_during_measurement"),
                            "connection_pool.samples_during_measurement", minimum=1)
    if pool.get("formal_sample_required") is not False:
        raise CompareError("pilot connection-pool report must not claim a formal-only sample requirement")
    return {"valid": True, "connections": connections,
            "sampling_period_ns": sampling_period, "metric_source": pool["metric_source"],
            "sample_count": sample_count, "observed": pool}


def _integer(document: Any, context: str, *, minimum: int = 0) -> int:
    if isinstance(document, bool) or not isinstance(document, int) or document < minimum:
        raise CompareError(f"{context} must be an integer >= {minimum}: {document!r}")
    return document


def validate_pg_result(result: dict[str, Any], *, pg_schema: str,
                       pg_connections: int, sql_sha256: str) -> dict[str, Any]:
    if (result.get("valid") is not True or result.get("status") != "verified"
            or result.get("backend") != "postgres"):
        raise CompareError("CLI result is not a valid PostgreSQL run")
    if result.get("wallet_profile") != "postgres-memory-tx-v1":
        raise CompareError("CLI result wallet profile is not postgres-memory-tx-v1")
    config = result.get("config")
    if not isinstance(config, dict):
        raise CompareError("CLI result is missing its config object")
    expected_config = {"backend": "postgres", "wallet_profile": "postgres-memory-tx-v1",
                       "database_name": PG_DATABASE, "pg_schema": pg_schema,
                       "pg_connections": pg_connections, "C": WORKERS_C,
                       "db_cpu_count": len(PG_DATABASE_CPUS),
                       "data_connection_target": pg_connections,
                       "worker_routing": "round-robin",
                       "users": USERS, "requests_per_user": REQUESTS_PER_USER,
                       "total_requests_expected": TOTAL_REQUESTS,
                       "per_worker_queue_capacity": QUEUE_CAPACITY,
                       "queue_capacity": QUEUE_CAPACITY,
                       "global_admission_capacity": QUEUE_CAPACITY,
                       "setup_concurrency": SETUP_CONCURRENCY,
                       "state_preload_concurrency": SETUP_CONCURRENCY,
                       "account_states_preloaded": USERS,
                       "account_state_source": "memory"}
    for name, expected in expected_config.items():
        if config.get(name) != expected:
            raise CompareError(f"CLI result config.{name}={config.get(name)!r}; expected {expected!r}")
    postgres = result.get("postgres")
    if not isinstance(postgres, dict):
        raise CompareError("CLI result is missing postgres metadata")
    if postgres.get("database_name") != PG_DATABASE or postgres.get("schema_name") != pg_schema:
        raise CompareError("CLI PostgreSQL database/schema metadata differs from the owned case")
    if postgres.get("schema_sql_file") != "benchmarks/scylla-wallet/sql/postgres_wallet.sql":
        raise CompareError("CLI PostgreSQL SQL source path differs from the frozen source")
    if postgres.get("schema_sql_sha256") != sql_sha256:
        raise CompareError("CLI PostgreSQL SQL source hash does not match the frozen SQL file")
    if postgres.get("function_name") != "wallet_commit":
        raise CompareError("CLI PostgreSQL metadata does not report wallet_commit")
    if postgres.get("schema_sql_sha256") != POSTGRES_SCHEMA_SQL_SHA256:
        raise CompareError("CLI PostgreSQL metadata does not match the fixed comparison SQL hash")
    if postgres.get("application_name") != PG_SERVICE_APPNAME:
        raise CompareError("CLI PostgreSQL application name differs from the frozen service identity")
    if (postgres.get("client_query_timeout_ms") != 30_000
            or postgres.get("pool_wait_excluded_from_client_timeout") is not True
            or postgres.get("implicit_transaction_ack") !=
            "query_one completion after PostgreSQL ReadyForQuery"):
        raise CompareError("CLI PostgreSQL timeout or transaction-acknowledgement contract is invalid")
    per_connection = postgres.get("connection_settings")
    if not isinstance(per_connection, list) or len(per_connection) != pg_connections:
        raise CompareError(f"CLI PostgreSQL metadata does not describe all D={pg_connections} connections")
    backend_pids = []
    for index, settings in enumerate(per_connection):
        if not isinstance(settings, dict):
            raise CompareError(f"PostgreSQL connection settings row {index} is malformed")
        pid = _integer(settings.get("backend_pid"), f"postgres.connection_settings[{index}].backend_pid", minimum=1)
        backend_pids.append(pid)
        expected_session = {"application_name": PG_SERVICE_APPNAME,
                            "synchronous_commit": "on", "fsync": "on",
                            "full_page_writes": "on", "commit_delay": "0",
                            "is_standby": False, "synchronous_standby_names": ""}
        for name, expected in expected_session.items():
            if settings.get(name) != expected:
                raise CompareError(f"PostgreSQL connection {pid} has {name}={settings.get(name)!r}, expected {expected!r}")
    if len(set(backend_pids)) != pg_connections:
        raise CompareError("PostgreSQL pool connection backend PIDs are not unique")
    for table_key, constraint_key, table, constraint in (
            ("accounts_table", "accounts_primary_key_constraint", "accounts", "accounts_pkey"),
            ("ledger_table", "ledger_primary_key_constraint", "ledger", "ledger_pkey")):
        if postgres.get(table_key) != table or postgres.get(constraint_key) != constraint:
            raise CompareError(f"PostgreSQL catalog identity metadata differs for {table}")
    if postgres.get("ledger_primary_key") != ["account_id", "tx_id"]:
        raise CompareError("PostgreSQL ledger primary key columns differ from (account_id, tx_id)")
    pool = validate_pg_connection_pool(result.get("connection_pool"), pg_connections)
    pool_fields = result["connection_pool"]
    expected_pool_fields = {
        "backend": "postgres", "database_name": PG_DATABASE, "pg_schema": pg_schema,
        "pg_connections": pg_connections, "target_total_data_connections": pg_connections,
        "C": WORKERS_C, "db_cpu_count": len(PG_DATABASE_CPUS),
        "setup_concurrency": SETUP_CONCURRENCY, "pool_ready_connections": pg_connections,
        "pool_after_last_reply": pg_connections,
        "metric_source": "tokio-postgres persistent connection pool liveness",
    }
    for name, expected in expected_pool_fields.items():
        if pool_fields.get(name) != expected:
            raise CompareError(f"CLI PostgreSQL connection_pool.{name}={pool_fields.get(name)!r}; expected {expected!r}")
    if pool.get("sample_count", 0) < 1:
        raise CompareError("PostgreSQL connection sampler captured no measurement-window sample")
    requests = result.get("requests")
    if not isinstance(requests, dict):
        raise CompareError("CLI PostgreSQL result is missing request counts")
    request_counts = {name: _integer(requests.get(name), f"requests.{name}")
                      for name in ("expected", "attempted", "accepted", "replies_received", "applied")}
    if any(value != TOTAL_REQUESTS for value in request_counts.values()):
        raise CompareError(f"PostgreSQL request counts differ from {TOTAL_REQUESTS}: {request_counts}")
    if _integer(requests.get("errors"), "requests.errors") != 0:
        raise CompareError("PostgreSQL workload reported request errors")
    if _integer(requests.get("unexpected_business_results"), "requests.unexpected_business_results") != 0:
        raise CompareError("PostgreSQL workload reported an unexpected wallet outcome")
    if (requests.get("operation_counts") != {"credit": TOTAL_REQUESTS // 2,
                                               "debit": TOTAL_REQUESTS // 2}
            or requests.get("outcome_counts") != {"applied": TOTAL_REQUESTS,
                                                    "insufficient_funds": 0,
                                                    "credit_overflow": 0,
                                                    "invalid_amount": 0}):
        raise CompareError("PostgreSQL credit/debit or business-outcome counts differ from the fixed workload")
    counters = result.get("driver_counters")
    delta = counters.get("delta") if isinstance(counters, dict) else None
    if not isinstance(delta, dict):
        raise CompareError("CLI PostgreSQL result is missing driver counter deltas")
    for name, expected in (("queries", TOTAL_REQUESTS), ("errors", 0), ("retries", 0),
                           ("connection_timeouts", 0), ("request_timeouts", 0)):
        if _integer(delta.get(name), f"driver_counters.delta.{name}") != expected:
            raise CompareError(f"driver counter {name} does not match {expected}")
    accounting = result.get("postgres_accounting")
    if not isinstance(accounting, dict):
        raise CompareError("CLI PostgreSQL result is missing postgres_accounting")
    expected_accounting = {"function_calls": TOTAL_REQUESTS,
                           "measured_sql_transaction_count": TOTAL_REQUESTS,
                           "sql_transactions_per_request": 1,
                           "ledger_insert_attempts": TOTAL_REQUESTS,
                           "account_updates": TOTAL_REQUESTS,
                           "internal_write_operations": 2 * TOTAL_REQUESTS,
                           "duplicate_conflicts": 0,
                           "accepted_requests": TOTAL_REQUESTS,
                           "driver_query_delta": TOTAL_REQUESTS,
                           "driver_query_calls_per_request": 1}
    for name, expected in expected_accounting.items():
        if _integer(accounting.get(name), f"postgres_accounting.{name}") != expected:
            raise CompareError(f"postgres_accounting.{name} does not match {expected}")
    if accounting.get("query_completion") != "ReadyForQuery after implicit transaction completion":
        raise CompareError("PostgreSQL SQL acknowledgement does not report completion after ReadyForQuery")
    query_accounting = result.get("query_accounting")
    expected_query_accounting = {"backend": "postgres", "account_state_source": "memory",
                                 "measured_select_requests": 0,
                                 "measured_sql_transaction_count": TOTAL_REQUESTS,
                                 "accepted_requests": TOTAL_REQUESTS,
                                 "driver_query_delta": TOTAL_REQUESTS,
                                 "internal_write_operations": 2 * TOTAL_REQUESTS}
    if not isinstance(query_accounting, dict) or any(
            query_accounting.get(name) != expected for name, expected in expected_query_accounting.items()):
        raise CompareError("PostgreSQL query_accounting does not match the exact one-call/two-write workload")
    request_query_accounting = requests.get("query_accounting")
    if request_query_accounting != query_accounting:
        raise CompareError("requests.query_accounting differs from top-level query_accounting")
    verification = result.get("verification")
    if not isinstance(verification, dict):
        raise CompareError("CLI PostgreSQL result is missing full verification evidence")
    expected_verification = {"complete": True, "valid": True,
                             "account_state_source": "memory_cache_vs_postgres",
                             "accounts_expected": USERS,
                             "memory_cache_accounts_compared": USERS,
                             "accounts_verified": USERS,
                             "ledger_rows_expected": TOTAL_REQUESTS,
                             "ledger_rows_seen": TOTAL_REQUESTS, "failures": 0}
    for name, expected in expected_verification.items():
        if verification.get(name) != expected:
            raise CompareError(f"PostgreSQL verification.{name} does not match {expected!r}")
    stages = result.get("stage_metrics")
    if not isinstance(stages, dict):
        raise CompareError("CLI PostgreSQL result is missing stage metrics")
    stage_rows = stages.get("stages")
    if not isinstance(stage_rows, dict):
        raise CompareError("CLI PostgreSQL stage metrics are missing the named stages map")
    for name in ("pool_wait", "sql_transaction"):
        row = stage_rows.get(name)
        if not isinstance(row, dict):
            raise CompareError(f"CLI PostgreSQL result is missing {name} stage metrics")
        if _integer(row.get("count"), f"stage_metrics.stages.{name}.count") != TOTAL_REQUESTS:
            raise CompareError(f"CLI PostgreSQL {name} stage count differs from accepted requests")
        if _integer(row.get("overflow_count"), f"stage_metrics.stages.{name}.overflow_count") != 0:
            raise CompareError(f"CLI PostgreSQL {name} stage histogram overflowed")
    expected_pg_counters = {"pg_function_calls": TOTAL_REQUESTS,
                            "pg_ledger_insert_attempts": TOTAL_REQUESTS,
                            "pg_account_updates": TOTAL_REQUESTS,
                            "pg_duplicate_conflicts": 0,
                            "accepted_requests": TOTAL_REQUESTS}
    metric_counters = stages.get("counters")
    if not isinstance(metric_counters, dict):
        raise CompareError("CLI PostgreSQL stage metrics are missing counters")
    for name, expected in expected_pg_counters.items():
        if _integer(metric_counters.get(name), f"stage_metrics.counters.{name}") != expected:
            raise CompareError(f"CLI PostgreSQL stage counter {name} differs from {expected}")
    if "cql_in_flight_current" in stages or "cql_in_flight_peak" in stages:
        raise CompareError("PostgreSQL stage metrics contain Scylla-named in-flight counters")
    activity = {name: _integer(stages.get(name), f"stage_metrics.{name}")
                for name in ("database_in_flight_current", "database_in_flight_peak",
                             "workers_total", "workers_busy_current", "workers_busy_peak",
                             "workers_idle_current", "workers_idle_peak")}
    if (activity["database_in_flight_current"] != 0
            or activity["database_in_flight_peak"] < 1
            or activity["database_in_flight_peak"] > pg_connections
            or activity["workers_total"] != WORKERS_C
            or activity["workers_busy_current"] != 0
            or activity["workers_busy_peak"] > WORKERS_C
            or activity["workers_idle_current"] != WORKERS_C
            or activity["workers_idle_peak"] > WORKERS_C):
        raise CompareError("PostgreSQL database in-flight peak is outside 1..D or final worker state differs from C=512")
    measurement = result.get("measurement")
    latency = result.get("end_to_end_latency")
    if not isinstance(measurement, dict) or measurement.get("started") is not True:
        raise CompareError("CLI PostgreSQL measurement evidence is missing")
    for name in ("request_count", "attempted_count", "accepted_requests"):
        if _integer(measurement.get(name), f"measurement.{name}") != TOTAL_REQUESTS:
            raise CompareError(f"CLI measurement.{name} differs from fixed request count")
    measurement_start = _integer(measurement.get("start_unix_ns"), "measurement.start_unix_ns", minimum=1)
    measurement_end = _integer(measurement.get("end_unix_ns"), "measurement.end_unix_ns", minimum=1)
    if measurement_end <= measurement_start:
        raise CompareError("CLI measurement timestamps do not form a non-empty measured window")
    duration_ns = measurement_end - measurement_start
    if (measurement.get("wall_duration_ns") != duration_ns
            or measurement.get("end_offset_ns") != duration_ns
            or measurement.get("start_offset_ns") != 0):
        raise CompareError("CLI measurement duration/offset fields do not match its UTC timestamps")
    rps = measurement.get("rps")
    if isinstance(rps, bool) or not isinstance(rps, (int, float)) or rps <= 0:
        raise CompareError("CLI PostgreSQL measurement RPS is missing or invalid")
    expected_rps = TOTAL_REQUESTS * 1_000_000_000 / duration_ns
    if abs(float(rps) - expected_rps) > max(1e-9, expected_rps * 1e-9):
        raise CompareError("CLI RPS differs from full replies divided by the measured wall window")
    if not isinstance(latency, dict):
        raise CompareError("CLI PostgreSQL result is missing end-to-end latency percentiles")
    if _integer(latency.get("count"), "end_to_end_latency.count") != TOTAL_REQUESTS:
        raise CompareError("end-to-end latency sample count differs from accepted requests")
    if _integer(latency.get("overflow_count"), "end_to_end_latency.overflow_count") != 0:
        raise CompareError("end-to-end latency histogram overflowed")
    percentiles = {name: _integer(latency.get(name), f"end_to_end_latency.{name}")
                   for name in ("p50_ns", "p95_ns", "p99_ns", "p999_ns", "max_ns")}
    if not (percentiles["p50_ns"] <= percentiles["p95_ns"] <= percentiles["p99_ns"]
            <= percentiles["p999_ns"] <= percentiles["max_ns"]):
        raise CompareError("CLI end-to-end latency percentiles are not monotonically ordered")
    if result.get("errors") != []:
        raise CompareError("PostgreSQL CLI result contains errors")
    forbidden = []
    def scan_forbidden(value: Any, path: str = "result") -> None:
        if isinstance(value, dict):
            for key, nested in value.items():
                normalized = str(key).lower()
                if "cql" in normalized or "paxos" in normalized:
                    forbidden.append(f"{path}.{key}")
                scan_forbidden(nested, f"{path}.{key}")
        elif isinstance(value, list):
            for index, nested in enumerate(value):
                scan_forbidden(nested, f"{path}[{index}]")
    scan_forbidden(result)
    if forbidden:
        raise CompareError(f"PostgreSQL result contains Scylla/CQL/Paxos-only fields: {forbidden[:5]}")
    serialized = json.dumps(result, sort_keys=True)
    if "postgresql://" in serialized.lower() or re.search(r'"[^" ]*dsn[^" ]*"\s*:', serialized, re.I):
        raise CompareError("PostgreSQL result appears to expose a DSN or credential field")
    return {"valid": True, "backend": "postgres", "pg_schema": pg_schema,
            "pg_connections": pg_connections, "requests": request_counts,
            "driver_counters": delta, "postgres_accounting": accounting,
            "verification": verification, "pool": pool,
            "rps": float(rps), "end_to_end_latency": latency,
            "stage_metrics": stages, "connection_backend_pids": sorted(backend_pids)}


def validate_pg_settings(settings: Any) -> dict[str, Any]:
    if isinstance(settings, list):
        by_name = {row.get("name"): row for row in settings if isinstance(row, dict)}
    elif isinstance(settings, dict):
        by_name = settings
    else:
        raise CompareError("PostgreSQL runtime settings evidence is not an object or list")
    missing = sorted(set(PG_SETTINGS_EXPECTED) - set(by_name))
    if missing:
        raise CompareError(f"PostgreSQL runtime settings are missing: {missing}")
    observed: dict[str, Any] = {}
    for name, (expected, kind) in PG_SETTINGS_EXPECTED.items():
        row = by_name[name]
        if isinstance(row, dict):
            setting, unit = row.get("setting"), row.get("unit")
            source = row.get("source")
            pending_restart = row.get("pending_restart")
        else:
            setting, unit, source, pending_restart = row, None, None, False
        if pending_restart is True:
            raise CompareError(f"PostgreSQL setting {name} has pending_restart=true")
        if kind == "text":
            if setting != expected:
                raise CompareError(f"PostgreSQL setting {name}={setting!r}; expected {expected!r}")
            normalized: Any = setting
        elif kind == "integer":
            try:
                normalized = int(setting)
            except (TypeError, ValueError) as exc:
                raise CompareError(f"PostgreSQL setting {name} is not an integer") from exc
            if normalized != expected:
                raise CompareError(f"PostgreSQL setting {name}={normalized}; expected {expected}")
        else:
            try:
                numeric = int(setting)
            except (TypeError, ValueError) as exc:
                raise CompareError(f"PostgreSQL setting {name} is not numeric") from exc
            factors = {"B": 1, "kB": 1024, "8kB": 8192, "MB": 1024**2,
                       "GB": 1024**3, "s": 1, "min": 60, "h": 3600}
            if unit not in factors:
                raise CompareError(f"PostgreSQL setting {name} has unsupported unit {unit!r}")
            normalized = numeric * factors[unit]
            if normalized != expected:
                raise CompareError(
                    f"PostgreSQL setting {name}={normalized} {unit}; expected {expected}")
        observed[name] = {"setting": setting, "unit": unit, "source": source,
                          "pending_restart": pending_restart, "normalized": normalized}
    return {"valid": True, "settings": observed}


def normalize_sql_body(value: str) -> str:
    return "\n".join(line.rstrip() for line in value.strip().splitlines())


def source_function_body(sql_source: str) -> str:
    match = re.search(
        r"CREATE\s+FUNCTION\s+wallet_commit\b[\s\S]*?\bAS\s+\$\$([\s\S]*?)\$\$\s*;",
        sql_source, flags=re.IGNORECASE)
    if not match:
        raise CompareError("frozen PostgreSQL SQL source has no wallet_commit dollar-quoted body")
    return normalize_sql_body(match.group(1))


PG_EXPECTED_COLUMNS = {
    "accounts": [("account_id", "bigint"), ("balance", "bytea"),
                 ("version", "bigint"), ("write_clock", "bigint")],
    "ledger": [("account_id", "bigint"), ("tx_id", "bigint"),
               ("operation", "smallint"), ("amount", "bytea"),
               ("outcome", "smallint"), ("balance_after", "bytea"),
               ("version", "bigint"), ("write_clock", "bigint")],
}
PG_EXPECTED_CHECKS = {
    "accounts": ("checkaccount_id>=0", "checkoctet_lengthbalance=8",
                 "checkversion>=0", "checkwrite_clock>=0"),
    "ledger": ("checkaccount_id>=0", "checktx_id>=0", "checkoperationin0,1",
               "checkoctet_lengthamount=8", "checkoutcomebetween0and3",
               "checkoctet_lengthbalance_after=8", "checkversion>=0",
               "checkwrite_clock>=0"),
}


def _compact_constraint(value: str) -> str:
    compact = re.sub(r"[\s()]", "", value.lower())
    # PostgreSQL 18 deparses these equivalent source expressions using
    # array-any and expanded comparison forms. Keep this allowlist exact.
    equivalents = {
        "checkoperation=anyarray[0,1]": "checkoperationin0,1",
        "checkoutcome>=0andoutcome<=3": "checkoutcomebetween0and3",
    }
    return equivalents.get(compact, compact)


def validate_pg_schema_evidence(evidence: Any, sql_source: str) -> dict[str, Any]:
    if not isinstance(evidence, dict):
        raise CompareError("PostgreSQL catalog evidence is missing")
    tables = evidence.get("tables")
    if not isinstance(tables, list):
        raise CompareError("PostgreSQL catalog evidence is missing tables")
    by_name = {row.get("name"): row for row in tables if isinstance(row, dict)}
    if set(by_name) != set(PG_EXPECTED_COLUMNS):
        raise CompareError(f"PostgreSQL schema tables are not exactly accounts and ledger: {sorted(by_name)}")
    primary_keys = {"accounts": ["account_id"], "ledger": ["account_id", "tx_id"]}
    primary_key_constraints = {"accounts": "accounts_pkey", "ledger": "ledger_pkey"}
    for table, expected_columns in PG_EXPECTED_COLUMNS.items():
        row = by_name[table]
        columns = row.get("columns")
        if not isinstance(columns, list):
            raise CompareError(f"PostgreSQL table {table} has no column metadata")
        observed_columns = [(column.get("name"), column.get("type"))
                            for column in columns if isinstance(column, dict)]
        if observed_columns != expected_columns:
            raise CompareError(f"PostgreSQL {table} columns differ: {observed_columns}")
        if any(column.get("not_null") is not True for column in columns):
            raise CompareError(f"PostgreSQL table {table} has a nullable wallet column")
        if row.get("primary_key") != primary_keys[table]:
            raise CompareError(f"PostgreSQL table {table} primary key is invalid")
        if row.get("primary_key_constraint") != primary_key_constraints[table]:
            raise CompareError(f"PostgreSQL table {table} primary key constraint name is invalid")
        checks = row.get("checks")
        if not isinstance(checks, list):
            raise CompareError(f"PostgreSQL table {table} check constraints are missing")
        check_definitions = []
        for item in checks:
            if isinstance(item, str):
                check_definitions.append(item)
            elif isinstance(item, dict) and item.get("validated") is True:
                check_definitions.append(item.get("definition", ""))
            else:
                raise CompareError(f"PostgreSQL table {table} has an unvalidated or malformed check")
        compact = [_compact_constraint(item) for item in check_definitions]
        if len(compact) != len(PG_EXPECTED_CHECKS[table]) or set(compact) != set(PG_EXPECTED_CHECKS[table]):
            raise CompareError(f"PostgreSQL table {table} check constraints differ from the frozen SQL: {compact}")
    indexes = evidence.get("indexes")
    expected_indexes = {
        ("accounts_pkey", "accounts", True, True, True, True, "btree", ("account_id",)),
        ("ledger_pkey", "ledger", True, True, True, True, "btree", ("account_id", "tx_id")),
    }
    if not isinstance(indexes, list):
        raise CompareError("PostgreSQL catalog evidence is missing index metadata")
    observed_indexes = set()
    for row in indexes:
        if not isinstance(row, dict):
            raise CompareError("PostgreSQL catalog index row is malformed")
        observed_indexes.add((row.get("name"), row.get("table"), row.get("unique"),
                              row.get("primary"), row.get("valid"), row.get("ready"),
                              row.get("method"), tuple(row.get("columns") or [])))
    if observed_indexes != expected_indexes:
        raise CompareError("PostgreSQL schema indexes differ from the two frozen primary-key indexes")
    functions = evidence.get("functions")
    if not isinstance(functions, list) or len(functions) != 1:
        raise CompareError("PostgreSQL schema must contain exactly one wallet_commit function")
    function = functions[0]
    if not isinstance(function, dict) or function.get("name") != "wallet_commit":
        raise CompareError("PostgreSQL wallet_commit catalog definition is missing")
    if (function.get("language") != "plpgsql"
            or function.get("security_definer") is not False
            or function.get("strict") is not False):
        raise CompareError("PostgreSQL wallet_commit language or execution attributes differ from the frozen SQL")
    expected_arguments = ("p_account_id bigint, p_tx_id bigint, p_operation smallint, p_amount bytea, "
                          "p_outcome smallint, p_balance_after bytea, p_version bigint, p_write_clock bigint")
    if re.sub(r"\s+", " ", str(function.get("identity_arguments", "")).strip().lower()) != expected_arguments:
        raise CompareError("PostgreSQL wallet_commit argument signature differs from the frozen SQL")
    expected_result = ("table(did_insert boolean, current_balance bytea, current_version bigint, "
                       "current_write_clock bigint, record_operation smallint, record_amount bytea, "
                       "record_outcome smallint, record_balance_after bytea, record_version bigint, "
                       "record_write_clock bigint)")
    if re.sub(r"\s+", " ", str(function.get("result", "")).strip().lower()) != expected_result:
        raise CompareError("PostgreSQL wallet_commit result signature differs from the frozen SQL")
    source_body = source_function_body(sql_source)
    catalog_body = function.get("source_body")
    if not isinstance(catalog_body, str) or normalize_sql_body(catalog_body) != source_body:
        raise CompareError("runtime wallet_commit source body differs from frozen SQL source")
    definition = function.get("definition")
    if (not isinstance(definition, str)
            or "FUNCTION" not in definition.upper()
            or "WALLET_COMMIT" not in definition.upper()):
        raise CompareError("pg_get_functiondef did not return the wallet_commit definition")
    runtime_body_sha = sha256_bytes(source_body.encode())
    if function.get("source_body_sha256") != runtime_body_sha:
        raise CompareError("reported runtime function body hash does not match the observed body")
    return {"valid": True, "tables": sorted(by_name), "primary_keys": primary_keys,
            "primary_key_constraints": primary_key_constraints,
            "wallet_commit_source_body_sha256": runtime_body_sha,
            "pg_get_functiondef_sha256": sha256_bytes(definition.encode()),
            "schema_sql_sha256": sha256_bytes(sql_source.encode())}


def validate_container_profile(host_config: dict[str, Any], inspect: dict[str, Any],
                                pid: int, cgroup_dir: pathlib.Path,
                                limits: dict[str, Any], network_name: str,
                                bind_root: pathlib.Path) -> dict[str, Any]:
    """Fail closed unless the PG container has the exact approved runtime limits."""
    if not isinstance(host_config, dict):
        raise CompareError("Docker inspection omitted HostConfig")
    if host_config.get("CpusetCpus") != "0,1":
        raise CompareError(f"PostgreSQL CpusetCpus={host_config.get('CpusetCpus')!r}, expected 0,1")
    if host_config.get("Memory") != PG_CGROUP_MEMORY_BYTES:
        raise CompareError("PostgreSQL Docker memory limit is not exactly 10 GiB")
    if host_config.get("MemorySwap") != PG_CGROUP_MEMORY_BYTES:
        raise CompareError("PostgreSQL Docker memory+swap is not exactly 10 GiB (extra swap is forbidden)")
    if host_config.get("CpuQuota") not in (None, 0) or host_config.get("NanoCpus") not in (None, 0):
        raise CompareError("PostgreSQL Docker container unexpectedly has a CPU quota")
    if host_config.get("CpuPeriod") not in (None, 0):
        raise CompareError("PostgreSQL Docker container unexpectedly has a CPU period")
    if host_config.get("NetworkMode") != network_name:
        raise CompareError("PostgreSQL container is not on this case's owned network")
    if inspect.get("Image") != PG_IMAGE_ID:
        raise CompareError("PostgreSQL container is not running the qualified image ID")
    actual_mount = next((mount for mount in inspect.get("Mounts", [])
                         if mount.get("Destination") == "/var/lib/postgresql"), None)
    if (not isinstance(actual_mount, dict) or actual_mount.get("Type") != "bind"
            or pathlib.Path(actual_mount.get("Source", "")).resolve() != bind_root.resolve()):
        raise CompareError("PostgreSQL data bind mount differs from the exact owned case path")
    if pid <= 0 or not pathlib.Path(f"/proc/{pid}").exists():
        raise CompareError("PostgreSQL container has no live host PID")
    try:
        relative = next(line.split("::", 1)[1] for line in
                        pathlib.Path(f"/proc/{pid}/cgroup").read_text().splitlines()
                        if line.startswith("0::"))
    except (OSError, StopIteration) as exc:
        raise CompareError(f"cannot resolve PostgreSQL container cgroup v2 path: {exc}") from exc
    root = pathlib.Path("/sys/fs/cgroup").resolve()
    actual_cgroup = (root / relative.lstrip("/")).resolve()
    if actual_cgroup != cgroup_dir.resolve() or (root not in actual_cgroup.parents and actual_cgroup != root):
        raise CompareError("PostgreSQL container cgroup path changed or escaped cgroup v2 root")
    cgroup = {}
    for filename in ("cpuset.cpus.effective", "memory.max", "memory.swap.max",
                     "cpu.max", "io.max", "io.stat"):
        path = actual_cgroup / filename
        try:
            cgroup[filename] = path.read_text(encoding="ascii").strip()
        except OSError as exc:
            raise CompareError(f"cannot read critical PostgreSQL cgroup file {filename}: {exc}") from exc
    if set(wallet_runner.parse_cpu_list(cgroup["cpuset.cpus.effective"])) != set(PG_DATABASE_CPUS):
        raise CompareError("effective PostgreSQL cgroup CPU set differs from CPUs 0,1")
    if cgroup["memory.max"] != str(PG_CGROUP_MEMORY_BYTES):
        raise CompareError("effective PostgreSQL cgroup memory.max differs from 10 GiB")
    if cgroup["memory.swap.max"] != "0":
        raise CompareError("effective PostgreSQL cgroup memory.swap.max is not zero")
    if cgroup["cpu.max"].split()[0] != "max":
        raise CompareError("effective PostgreSQL cgroup CPU quota is not unlimited")
    io_validation = wallet_runner.validate_container_io_limits(host_config, pid, limits,
                                                               cgroup["io.max"])
    return {"valid": True, "host_pid": pid, "cgroup_path": str(actual_cgroup),
            "host_config": {"cpuset_cpus": host_config.get("CpusetCpus"),
                            "memory_bytes": host_config.get("Memory"),
                            "memory_swap_bytes": host_config.get("MemorySwap"),
                            "cpu_quota": host_config.get("CpuQuota"),
                            "cpu_period": host_config.get("CpuPeriod"),
                            "nano_cpus": host_config.get("NanoCpus"),
                            "network_mode": host_config.get("NetworkMode"),
                            "port_bindings": host_config.get("PortBindings"),
                            "io_limits": io_validation["host_config"]},
            "cgroup": cgroup, "io_limits_validation": io_validation}


def cgroup_snapshot(cgroup_dir: pathlib.Path) -> dict[str, Any]:
    """Collect required cgroup-v2 counters, preserving absent fields as errors."""
    def read(name: str) -> str:
        path = cgroup_dir / name
        try:
            return path.read_text(encoding="ascii").strip()
        except OSError as exc:
            raise CompareError(f"cannot read PostgreSQL cgroup {name}: {exc}") from exc

    cpu_stat = {}
    for line in read("cpu.stat").splitlines():
        columns = line.split()
        if len(columns) == 2:
            try:
                cpu_stat[columns[0]] = int(columns[1])
            except ValueError as exc:
                raise CompareError(f"invalid PostgreSQL cpu.stat line: {line!r}") from exc
    if "usage_usec" not in cpu_stat:
        raise CompareError("PostgreSQL cgroup cpu.stat does not contain usage_usec")
    memory = {}
    for field in ("memory.current", "memory.peak", "memory.events", "memory.swap.current"):
        value = read(field)
        if field == "memory.events":
            parsed = {}
            for line in value.splitlines():
                key, _, raw = line.partition(" ")
                if key:
                    parsed[key] = int(raw)
            memory[field] = parsed
        else:
            if not value.isdigit():
                raise CompareError(f"PostgreSQL cgroup {field} is not an integer")
            memory[field] = int(value)
    return {"sampled_unix_ns": time.time_ns(), "sampled_monotonic_ns": time.monotonic_ns(),
            "cpu_stat": cpu_stat, "memory": memory, "io_stat": read("io.stat")}


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def postgres_settings_sql() -> str:
    names = ",".join(_sql_string(name) for name in PG_SETTINGS_EXPECTED)
    return ("SELECT jsonb_build_object('server_version', current_setting('server_version'), "
            "'server_version_num', current_setting('server_version_num')::integer, "
            "'in_recovery', pg_is_in_recovery(), 'settings', "
            f"(SELECT jsonb_agg(to_jsonb(s) ORDER BY name) FROM pg_settings s WHERE name IN ({names})))::text")


def postgres_stats_sql() -> str:
    return """SET stats_fetch_consistency = 'none';
SELECT jsonb_build_object(
 'captured_at', clock_timestamp(),
 'server_version', current_setting('server_version'),
 'wal', (SELECT to_jsonb(w) FROM pg_stat_wal w),
 'io', (SELECT coalesce(jsonb_agg(to_jsonb(i) ORDER BY backend_type, object, context), '[]'::jsonb) FROM pg_stat_io i),
 'database', (SELECT to_jsonb(d) FROM pg_stat_database d WHERE datname = 'walletbench'),
 'checkpointer', (SELECT to_jsonb(c) FROM pg_stat_checkpointer c),
 'bgwriter', (SELECT to_jsonb(b) FROM pg_stat_bgwriter b),
 'activity', (SELECT jsonb_build_object(
    'connections', (SELECT count(*) FROM pg_stat_activity WHERE datname = 'walletbench' AND application_name = 'ledger-wallet-postgres-wallet-bench'),
    'service_backend_pids', (SELECT coalesce(jsonb_agg(pid ORDER BY pid), '[]'::jsonb)
       FROM pg_stat_activity WHERE datname = 'walletbench' AND application_name = 'ledger-wallet-postgres-wallet-bench'),
    'waits', (SELECT coalesce(jsonb_agg(to_jsonb(w) ORDER BY application_name, backend_type, state, wait_event_type, wait_event), '[]'::jsonb)
              FROM (SELECT application_name, backend_type, state, wait_event_type, wait_event, count(*) AS count
                    FROM pg_stat_activity
                    WHERE application_name NOT IN ('ledger-wallet-pg-monitor', 'ledger-wallet-pg-table-monitor')
                    GROUP BY application_name, backend_type, state, wait_event_type, wait_event) w)))
)::text;"""


def postgres_table_stats_sql(schema: str) -> str:
    _validate_schema_name(schema)
    return ("SET stats_fetch_consistency = 'none';\n"
            "SELECT jsonb_build_object('captured_at', clock_timestamp(), 'stats', "
            "(SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY relname), '[]'::jsonb) "
            "FROM pg_stat_user_tables t WHERE schemaname = " + _sql_string(schema) +
            " AND relname IN ('accounts','ledger')))::text;")


def _validate_schema_name(schema: str) -> str:
    if (not isinstance(schema, str) or not schema.startswith("wallet_bench_")
            or len(schema) > 63 or not re.fullmatch(r"[a-z][a-z0-9_]*", schema)):
        raise CompareError(f"unsafe/non-owned PostgreSQL schema name: {schema!r}")
    return schema


def postgres_schema_sql(schema: str) -> str:
    schema = _validate_schema_name(schema)
    return f"""SELECT jsonb_build_object(
 'tables', (SELECT coalesce(jsonb_agg(jsonb_build_object(
   'name', t.relname,
   'columns', (SELECT coalesce(jsonb_agg(jsonb_build_object('name', a.attname,
       'type', format_type(a.atttypid,a.atttypmod), 'not_null', a.attnotnull) ORDER BY a.attnum), '[]'::jsonb)
       FROM pg_attribute a WHERE a.attrelid=t.oid AND a.attnum>0 AND NOT a.attisdropped),
   'primary_key', (SELECT to_jsonb(array_agg(a.attname ORDER BY k.ordinality))
       FROM pg_constraint c CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS k(attnum,ordinality)
       JOIN pg_attribute a ON a.attrelid=t.oid AND a.attnum=k.attnum WHERE c.conrelid=t.oid AND c.contype='p'),
   'primary_key_constraint', (SELECT c.conname FROM pg_constraint c
       WHERE c.conrelid=t.oid AND c.contype='p'),
   'checks', (SELECT coalesce(jsonb_agg(jsonb_build_object('definition', pg_get_constraintdef(c.oid),
       'validated', c.convalidated) ORDER BY c.conname), '[]'::jsonb)
       FROM pg_constraint c WHERE c.conrelid=t.oid AND c.contype='c')) ORDER BY t.relname), '[]'::jsonb)
   FROM pg_class t JOIN pg_namespace n ON n.oid=t.relnamespace
   WHERE n.nspname={_sql_string(schema)} AND t.relkind IN ('r','p')),
 'indexes', (SELECT coalesce(jsonb_agg(jsonb_build_object(
   'name', idx.relname, 'table', tbl.relname, 'unique', i.indisunique,
   'primary', i.indisprimary, 'valid', i.indisvalid, 'ready', i.indisready,
   'method', am.amname,
   'columns', (SELECT coalesce(jsonb_agg(a.attname ORDER BY k.ordinality), '[]'::jsonb)
       FROM unnest(i.indkey) WITH ORDINALITY AS k(attnum,ordinality)
       JOIN pg_attribute a ON a.attrelid=tbl.oid AND a.attnum=k.attnum
       WHERE k.ordinality <= i.indnkeyatts))), '[]'::jsonb)
   FROM pg_index i JOIN pg_class tbl ON tbl.oid=i.indrelid
   JOIN pg_class idx ON idx.oid=i.indexrelid
   JOIN pg_namespace n ON n.oid=tbl.relnamespace
   JOIN pg_am am ON am.oid=idx.relam
   WHERE n.nspname={_sql_string(schema)}),
 'functions', (SELECT coalesce(jsonb_agg(jsonb_build_object(
   'name', p.proname, 'definition', pg_get_functiondef(p.oid), 'source_body', p.prosrc,
   'language', l.lanname, 'identity_arguments', pg_get_function_identity_arguments(p.oid),
   'result', pg_get_function_result(p.oid), 'security_definer', p.prosecdef,
   'strict', p.proisstrict) ORDER BY p.proname), '[]'::jsonb)
   FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace JOIN pg_language l ON l.oid=p.prolang
   WHERE n.nspname={_sql_string(schema)} AND p.prokind='f')
)::text;"""


def validate_pg_runtime_settings(document: Any) -> dict[str, Any]:
    if not isinstance(document, dict):
        raise CompareError("PostgreSQL settings query did not return an object")
    server_version = document.get("server_version")
    version_num = document.get("server_version_num")
    if (not isinstance(server_version, str)
            or not server_version.split()
            or server_version.split()[0] != PG_RUNTIME_VERSION
            or isinstance(version_num, bool)
            or not isinstance(version_num, int)
            or version_num != PG_RUNTIME_VERSION_NUM):
        raise CompareError(f"PostgreSQL runtime version differs from pinned 18.6: {document.get('server_version')!r}")
    if document.get("in_recovery") is not False:
        raise CompareError("PostgreSQL server is in recovery/standby mode")
    settings_result = validate_pg_settings(document.get("settings"))
    return {"valid": True, "server_version": document["server_version"],
            "server_version_num": document["server_version_num"],
            "in_recovery": False, **settings_result}


def validate_pg_stat_capabilities(stats: Any) -> dict[str, Any]:
    if not isinstance(stats, dict):
        raise CompareError("PostgreSQL baseline stats snapshot is missing")
    wal = stats.get("wal")
    database = stats.get("database")
    checkpoint = stats.get("checkpointer")
    io_rows = stats.get("io")
    if not isinstance(wal, dict) or not {"wal_bytes", "wal_fpi", "wal_records", "stats_reset"}.issubset(wal):
        raise CompareError("PostgreSQL pg_stat_wal is missing required WAL counters or stats_reset")
    if not isinstance(database, dict) or not {"xact_commit", "xact_rollback", "stats_reset"}.issubset(database):
        raise CompareError("PostgreSQL pg_stat_database is missing transaction counters or stats_reset")
    if not isinstance(checkpoint, dict) or "stats_reset" not in checkpoint:
        raise CompareError("PostgreSQL pg_stat_checkpointer is missing stats_reset")
    if not isinstance(io_rows, list) or not io_rows:
        raise CompareError("PostgreSQL pg_stat_io returned no rows")
    wal_io_rows = [row for row in io_rows if isinstance(row, dict) and row.get("object") == "wal"]
    if not wal_io_rows:
        raise CompareError("PostgreSQL pg_stat_io has no object=wal rows")
    required_io = {"writes", "write_time", "fsyncs", "fsync_time"}
    if any(not required_io.issubset(row) or "stats_reset" not in row for row in wal_io_rows):
        raise CompareError("PostgreSQL pg_stat_io WAL rows lack write/fsync timing or reset metadata")
    activity = stats.get("activity")
    if not isinstance(activity, dict) or not isinstance(activity.get("connections"), int):
        raise CompareError("PostgreSQL pg_stat_activity connection snapshot is missing")
    if activity.get("connections") <= 0:
        raise CompareError("PostgreSQL pg_stat_activity did not observe wallet data connections")
    if not isinstance(activity.get("waits"), list):
        raise CompareError("PostgreSQL pg_stat_activity wait snapshot is missing")
    pids = activity.get("service_backend_pids")
    if (not isinstance(pids, list) or len(pids) != activity["connections"]
            or any(isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0 for pid in pids)
            or len(set(pids)) != len(pids)):
        raise CompareError("PostgreSQL pg_stat_activity service backend PID list is missing or inconsistent")
    return {"valid": True, "pg_stat_io_rows": len(io_rows),
            "pg_stat_io_wal_rows": len(wal_io_rows), "wal_timing_fields": sorted(required_io),
            "wal_counters": sorted(wal), "database_counters": sorted(database),
            "checkpointer_fields": sorted(checkpoint),
            "bgwriter_status": "available" if isinstance(stats.get("bgwriter"), dict)
            else "not_available"}


def validate_pg_monitor_samples(samples: Sequence[dict[str, Any]], backend_pids: Sequence[int],
                                pg_connections: int) -> dict[str, Any]:
    if len(samples) < 2:
        raise CompareError("PostgreSQL external monitor has fewer than two samples")
    wanted = sorted(backend_pids)
    if len(wanted) != pg_connections or len(set(wanted)) != pg_connections:
        raise CompareError("expected service PID set does not match PG pool size D")
    max_gap_ns = int(SAMPLE_MAX_GAP_SECONDS * 1e9)
    for prior, current in zip(samples, samples[1:]):
        before_ns, after_ns = prior.get("sampled_unix_ns"), current.get("sampled_unix_ns")
        if (not isinstance(before_ns, int) or not isinstance(after_ns, int)
                or after_ns <= before_ns or after_ns - before_ns > max_gap_ns):
            raise CompareError("PostgreSQL monitor sample timestamps are nonmonotonic or >2.5s apart")
    for index, sample in enumerate(samples):
        stats = sample.get("stats")
        if not isinstance(stats, dict):
            raise CompareError(f"PostgreSQL monitor sample {index} has no stats object")
        capability = validate_pg_stat_capabilities(stats)
        activity = stats["activity"]
        pids = sorted(activity["service_backend_pids"])
        if activity["connections"] != pg_connections or pids != wanted:
            raise CompareError(f"PostgreSQL sample {index} sees service pool {pids}, expected {wanted}")
        tables = sample.get("table_stats")
        table_rows = tables.get("stats") if isinstance(tables, dict) else None
        if not isinstance(table_rows, list):
            raise CompareError(f"PostgreSQL monitor sample {index} has no table-stat rows")
        table_names = sorted(row.get("relname") for row in table_rows if isinstance(row, dict))
        if table_names != ["accounts", "ledger"]:
            raise CompareError(f"PostgreSQL monitor sample {index} does not see accounts and ledger table stats")
        for row in table_rows:
            for name in ("n_dead_tup", "n_live_tup", "autovacuum_count", "last_autovacuum"):
                if name not in row:
                    raise CompareError(f"PostgreSQL table-stat sample is missing {name}; preserve as unavailable")
        cgroup = sample.get("cgroup")
        if not isinstance(cgroup, dict) or not isinstance(cgroup.get("cpu_stat"), dict):
            raise CompareError(f"PostgreSQL monitor sample {index} has no cgroup CPU evidence")
        memory = cgroup.get("memory")
        if not isinstance(memory, dict) or any(name not in memory for name in
                                               ("memory.current", "memory.peak", "memory.events", "memory.swap.current")):
            raise CompareError(f"PostgreSQL monitor sample {index} has incomplete cgroup memory evidence")
        if not isinstance(cgroup.get("io_stat"), str):
            raise CompareError(f"PostgreSQL monitor sample {index} has no cgroup io.stat evidence")
        pss = sample.get("postgres_pss")
        if (not isinstance(pss, dict)
                or _integer(pss.get("postgres_process_count"), "postgres_process_count", minimum=1) < 1
                or _integer(pss.get("pss_kb"), "postgres_pss_kb") <= 0
                or _integer(pss.get("unreadable_process_count"), "postgres_unreadable_process_count") != 0):
            raise CompareError(f"PostgreSQL monitor sample {index} has invalid PSS evidence")
        if capability.get("valid") is not True:
            raise CompareError(f"PostgreSQL monitor sample {index} has invalid stats capabilities")
    return {"valid": True, "sample_count": len(samples),
            "pool_connections": pg_connections, "backend_pids": wanted,
            "monitor_connections_excluded_by_application_name": True,
            "max_gap_seconds": max((b["sampled_unix_ns"] - a["sampled_unix_ns"])
                                    for a, b in zip(samples, samples[1:])) / 1e9}


def pg_stat_snapshot_delta(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    """Compute raw cumulative counter deltas with offsets; PG stats publish asynchronously."""
    result: dict[str, Any] = {"valid": True, "publication_is_asynchronous": True,
                              "before_sample_unix_ns": before.get("sampled_unix_ns"),
                              "after_sample_unix_ns": after.get("sampled_unix_ns"),
                              "before_snapshot": before.get("stats"),
                              "after_snapshot": after.get("stats"), "deltas": {}}
    for group in ("wal", "database", "checkpointer", "bgwriter"):
        left = (before.get("stats") or {}).get(group)
        right = (after.get("stats") or {}).get(group)
        if not isinstance(left, dict) or not isinstance(right, dict):
            result["deltas"][group] = {"status": "not_available", "reason": "view row missing"}
            continue
        l_reset, r_reset = left.get("stats_reset"), right.get("stats_reset")
        counters = {}
        for key in sorted(set(left) & set(right)):
            if key.endswith("_time") or key.startswith("wal_") or key.startswith("xact_") or key in {
                "wal_records", "wal_fpi", "wal_bytes", "wal_buffers_full", "xact_commit",
                "xact_rollback", "tup_inserted", "tup_updated", "deadlocks", "num_timed",
                "num_requested", "num_done", "buffers_written", "buffers_clean",
                "buffers_alloc", "writes", "fsyncs", "checkpoints_timed", "checkpoints_req",
            }:
                a, b = left.get(key), right.get(key)
                if isinstance(a, (int, float)) and not isinstance(a, bool) and isinstance(b, (int, float)) and not isinstance(b, bool):
                    counters[key] = (b - a) if b >= a else None
        result["deltas"][group] = {"status": "valid" if l_reset == r_reset else "reset_during_window",
                                   "stats_reset_before": l_reset, "stats_reset_after": r_reset,
                                   "counters": counters}
        if l_reset != r_reset or any(value is None for value in counters.values()):
            result["valid"] = False
    for label, sample in (("before", before), ("after", after)):
        rows = ((sample.get("stats") or {}).get("io"))
        if not isinstance(rows, list):
            result["deltas"][f"io_{label}"] = {"status": "not_available"}
    before_io = {tuple(row.get(key) for key in ("backend_type", "object", "context")): row
                 for row in ((before.get("stats") or {}).get("io") or []) if isinstance(row, dict)}
    after_io = {tuple(row.get(key) for key in ("backend_type", "object", "context")): row
                for row in ((after.get("stats") or {}).get("io") or []) if isinstance(row, dict)}
    io_deltas = {}
    for key in sorted(set(before_io) | set(after_io), key=str):
        lrow, rrow = before_io.get(key), after_io.get(key)
        label = "|".join(str(part) for part in key)
        if lrow is None or rrow is None:
            io_deltas[label] = {"status": "not_available", "reason": "row absent in one sample"}
            continue
        reset_equal = lrow.get("stats_reset") == rrow.get("stats_reset")
        counter_delta = {}
        for name in ("reads", "read_time", "writes", "write_time", "writebacks",
                     "writeback_time", "extends", "extend_time", "hits", "evictions",
                     "reuses", "fsyncs", "fsync_time"):
            a, b = lrow.get(name), rrow.get(name)
            if isinstance(a, (int, float)) and not isinstance(a, bool) and isinstance(b, (int, float)) and not isinstance(b, bool):
                counter_delta[name] = b - a if b >= a else None
        io_deltas[label] = {"status": "valid" if reset_equal and None not in counter_delta.values() else "reset_or_counter_decrease",
                            "stats_reset_before": lrow.get("stats_reset"),
                            "stats_reset_after": rrow.get("stats_reset"), "counters": counter_delta}
        if io_deltas[label]["status"] != "valid":
            result["valid"] = False
    result["deltas"]["io"] = io_deltas
    return result


def parse_cgroup_io_stat(value: Any) -> dict[str, dict[str, int]]:
    if not isinstance(value, str):
        raise CompareError("cgroup io.stat payload is not text")
    result: dict[str, dict[str, int]] = {}
    for line in value.splitlines():
        fields = line.split()
        if not fields or not re.fullmatch(r"\d+:\d+", fields[0]) or fields[0] in result:
            raise CompareError(f"invalid or duplicate cgroup io.stat line: {line!r}")
        row: dict[str, int] = {}
        for item in fields[1:]:
            key, separator, raw = item.partition("=")
            if not separator or not key or not raw.isdigit() or key in row:
                raise CompareError(f"invalid cgroup io.stat counter: {item!r}")
            row[key] = int(raw)
        result[fields[0]] = row
    if not result:
        raise CompareError("cgroup io.stat has no device counters")
    return result


def analyze_host_diskstats(observer_dir: pathlib.Path, start_unix_ns: int,
                           end_unix_ns: int) -> dict[str, Any]:
    try:
        samples = disk_observer.read_observer_samples(observer_dir / "samples.jsonl")
        profile = wallet_runner.read_json_object(observer_dir / "device-profile.json")
        collector = wallet_runner.read_json_object(observer_dir / "collector-result.json")
    except Exception as exc:
        raise CompareError(f"cannot load shared host diskstats evidence: {exc}") from exc
    if collector.get("valid") is not True or len(samples) < 3:
        raise CompareError("shared host diskstats collector did not produce a valid >=3-sample series")
    if disk_observer.validate_calibrated_device_profile(profile).get("valid") is not True:
        raise CompareError("shared host diskstats device profile does not match calibrated /dev/sdb2")
    before = [sample for sample in samples if sample.get("time_unix_ns", 0) <= start_unix_ns]
    after = [sample for sample in samples if sample.get("time_unix_ns", 0) >= end_unix_ns]
    if not before or not after:
        raise CompareError("host diskstats samples do not bracket the PostgreSQL measured window")
    left, right = before[-1], after[0]
    if (start_unix_ns - left["time_unix_ns"] > int(SAMPLE_MAX_GAP_SECONDS * 1e9)
            or right["time_unix_ns"] - end_unix_ns > int(SAMPLE_MAX_GAP_SECONDS * 1e9)):
        raise CompareError("nearest host diskstats boundary sample is >2.5 seconds from measured window")
    deltas = {}
    for role in ("whole_device", "data_device"):
        lrow = (left.get("diskstats") or {}).get(role)
        rrow = (right.get("diskstats") or {}).get(role)
        if not isinstance(lrow, dict) or not isinstance(rrow, dict):
            raise CompareError(f"host diskstats is missing {role}")
        if (lrow.get("major_minor") != rrow.get("major_minor")
                or lrow.get("major_minor") not in {"8:16", "8:18"}):
            raise CompareError(f"host diskstats {role} identity changed during the measurement")
        counters = {}
        left_fields = lrow.get("fields") or {}
        right_fields = rrow.get("fields") or {}
        for name in disk_observer.COUNTER_FIELDS:
            a, b = left_fields.get(name), right_fields.get(name)
            if not isinstance(a, int) or not isinstance(b, int) or b < a:
                raise CompareError(f"host diskstats {role}.{name} is missing or moved backwards")
            counters[name] = b - a
        deltas[role] = {"major_minor": lrow["major_minor"], "name": lrow.get("name"),
                        "counters": counters}
    return {"valid": True, "scope": "host device counters; shared by concurrent host activity",
            "window_start_unix_ns": start_unix_ns, "window_end_unix_ns": end_unix_ns,
            "window_duration_ns": end_unix_ns - start_unix_ns,
            "before_sample_unix_ns": left["time_unix_ns"],
            "after_sample_unix_ns": right["time_unix_ns"],
            "before_offset_ns": left["time_unix_ns"] - start_unix_ns,
            "after_offset_ns": right["time_unix_ns"] - end_unix_ns,
            "observer_sample_count": len(samples), "collector": collector,
            "device_profile": profile, "deltas": deltas}


def _redact_text(value: str, secrets_to_redact: Sequence[str]) -> str:
    for secret in secrets_to_redact:
        if secret:
            value = value.replace(secret, "<redacted>")
    return value


def _safe_argv(argv: Sequence[str], secrets_to_redact: Sequence[str]) -> list[str]:
    output = []
    redact_next = False
    for item in argv:
        if redact_next:
            output.append("<redacted>")
            redact_next = False
            continue
        if item == "--pg-dsn":
            output.append(item)
            redact_next = True
        elif item.startswith("--pg-dsn="):
            output.append("--pg-dsn=<redacted>")
        else:
            output.append(_redact_text(str(item), secrets_to_redact))
    return output


def _write_json(path: pathlib.Path, value: Any) -> None:
    wallet_runner.write_json(path, value)


def run_recorded_command(case_dir: pathlib.Path, name: str, argv: Sequence[str], *,
                         timeout: float = 120.0, check: bool = True,
                         secrets_to_redact: Sequence[str] = ()) -> subprocess.CompletedProcess[str]:
    command_dir = case_dir / "commands"
    command_dir.mkdir(parents=True, exist_ok=True)
    safe_name = re.sub(r"[^A-Za-z0-9_.-]", "_", name)
    try:
        result = subprocess.run(list(argv), cwd=REPO_ROOT, text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                timeout=timeout, check=False)
    except Exception as exc:
        _write_json(command_dir / f"{safe_name}.command.json",
                    {"argv": _safe_argv(argv, secrets_to_redact),
                     "error": f"{type(exc).__name__}: {_redact_text(str(exc), secrets_to_redact)}"})
        raise
    stdout = _redact_text(result.stdout, secrets_to_redact)
    stderr = _redact_text(result.stderr, secrets_to_redact)
    (command_dir / f"{safe_name}.stdout.txt").write_text(stdout, encoding="utf-8")
    (command_dir / f"{safe_name}.stderr.txt").write_text(stderr, encoding="utf-8")
    _write_json(command_dir / f"{safe_name}.command.json",
                {"argv": _safe_argv(argv, secrets_to_redact),
                 "returncode": result.returncode, "completed_unix_ns": time.time_ns()})
    safe_result = subprocess.CompletedProcess(result.args, result.returncode, stdout, stderr)
    if check and result.returncode != 0:
        raise CompareError(f"command {name} exited {result.returncode}; inspect {command_dir}")
    return safe_result


def _inspect_container(case_dir: pathlib.Path, identifier: str,
                       secrets_to_redact: Sequence[str]) -> dict[str, Any]:
    result = run_recorded_command(case_dir, f"inspect-{identifier[:12]}",
                                  ["docker", "inspect", identifier],
                                  secrets_to_redact=secrets_to_redact)
    try:
        value = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise CompareError(f"docker inspect {identifier} did not return JSON") from exc
    if not isinstance(value, list) or len(value) != 1 or not isinstance(value[0], dict):
        raise CompareError(f"docker inspect {identifier} returned an unexpected result")
    inspected = value[0]
    # Keep safe launch configuration and remove credentials before evidence is persisted.
    config = inspected.get("Config")
    if isinstance(config, dict) and isinstance(config.get("Env"), list):
        config["Env"] = [(_redact_text(item, secrets_to_redact)
                          if "POSTGRES_PASSWORD=" in item else item)
                         for item in config["Env"]]
    _write_json(case_dir / f"container-inspect-{identifier[:12]}.json", inspected)
    return inspected


def _validate_owned_labels(document: dict[str, Any], *, nonce: str,
                           run_id: str, case_id: str, resource: str) -> None:
    labels = (document.get("Config", {}).get("Labels") or {}) if resource == "container" else (document.get("Labels") or {})
    expected = {"owner": "ledger-wallet-db-compare", "compare-nonce": nonce,
                "compare-run-id": run_id, "compare-case-id": case_id}
    if any(labels.get(key) != value for key, value in expected.items()):
        raise CompareError(f"{resource} ownership labels do not match the exact comparison case")


def _parse_single_json(text: str, label: str) -> dict[str, Any]:
    nonempty = [line for line in text.splitlines() if line.strip()]
    if len(nonempty) != 1:
        raise CompareError(f"{label} returned {len(nonempty)} JSON result lines; expected exactly one")
    try:
        value = json.loads(nonempty[0])
    except json.JSONDecodeError as exc:
        raise CompareError(f"{label} output is not valid JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise CompareError(f"{label} JSON result is not an object")
    return value


def _docker_psql(case_dir: pathlib.Path, container_id: str, name: str,
                 sql: str, *, database: str = "postgres", app_name: str,
                 timeout: float = 60.0, capture: bool = True,
                 secrets_to_redact: Sequence[str] = ()) -> dict[str, Any]:
    argv = ["docker", "exec", "--user", "postgres", "--env", f"PGAPPNAME={app_name}",
            container_id, "psql", "-X", "-q", "-A", "-t", "-v", "ON_ERROR_STOP=1",
            "-U", "postgres", "-d", database, "-c", sql]
    if capture:
        result = run_recorded_command(case_dir, name, argv, timeout=timeout,
                                      secrets_to_redact=secrets_to_redact)
    else:
        result = subprocess.run(argv, cwd=REPO_ROOT, text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, timeout=timeout, check=False)
        if result.returncode != 0:
            raise CompareError(f"PostgreSQL monitor query {name} failed: "
                               f"{_redact_text(result.stderr, secrets_to_redact)}")
    return _parse_single_json(result.stdout, name)


def _pg_process_pss(case_dir: pathlib.Path, container_id: str) -> dict[str, int]:
    script = r'''sum=0; seen=0; missing=0; pids=""; for proc in /proc/[0-9]*; do
  [ -r "$proc/comm" ] || continue
  name=$(cat "$proc/comm" 2>/dev/null || true)
  [ "$name" = postgres ] || continue
  pss=$(awk '$1 == "Pss:" { print $2; found=1 } END { if (!found) exit 3 }' "$proc/smaps_rollup" 2>/dev/null) || { missing=$((missing+1)); continue; }
  case "$pss" in ''|*[!0-9]*) missing=$((missing+1)); continue;; esac
  sum=$((sum+pss)); seen=$((seen+1)); pids="$pids ${proc##*/}"
done
printf '{"postgres_process_count":%s,"pss_kb":%s,"unreadable_process_count":%s,"container_pids":"%s"}\n' "$seen" "$sum" "$missing" "${pids# }"'''
    result = subprocess.run(["docker", "exec", "--user", "postgres", container_id,
                             "sh", "-ec", script], text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            timeout=20, check=False)
    if result.returncode != 0:
        raise CompareError(f"cannot capture PostgreSQL process PSS: {result.stderr.strip()}")
    value = _parse_single_json(result.stdout, "PostgreSQL process PSS")
    count = _integer(value.get("postgres_process_count"), "postgres_process_count", minimum=1)
    _integer(value.get("pss_kb"), "postgres_process_pss_kb")
    missing = _integer(value.get("unreadable_process_count"), "unreadable_process_count")
    if missing:
        raise CompareError(f"could not read PSS for {missing} PostgreSQL backend processes")
    return value


class PostgresMonitor:
    """1 Hz raw PG statistics and container cgroup sampling, separate from D."""

    def __init__(self, case_dir: pathlib.Path, container_id: str,
                 cgroup_dir: pathlib.Path, schema: str,
                 secrets_to_redact: Sequence[str] = ()) -> None:
        self.case_dir = case_dir
        self.container_id = container_id
        self.cgroup_dir = cgroup_dir
        self.schema = schema
        self.secrets_to_redact = tuple(secrets_to_redact)
        self.samples_path = case_dir / "postgres-monitor" / "samples.jsonl"
        self.samples_path.parent.mkdir(parents=True, exist_ok=False)
        self._handle = self.samples_path.open("x", encoding="utf-8", buffering=1)
        self.samples: list[dict[str, Any]] = []
        self.errors: list[str] = []
        self.stop_event = threading.Event()
        self.lock = threading.Lock()
        self.thread: threading.Thread | None = None

    def capture(self) -> dict[str, Any]:
        with self.lock:
            before_unix = time.time_ns()
            before_mono = time.monotonic_ns()
            started = time.monotonic()
            stats = _docker_psql(self.case_dir, self.container_id, "postgres-monitor-stats",
                                 postgres_stats_sql(), app_name=PG_MONITOR_APPNAME,
                                 capture=False, secrets_to_redact=self.secrets_to_redact)
            tables = _docker_psql(self.case_dir, self.container_id, "postgres-monitor-tables",
                                  postgres_table_stats_sql(self.schema), database=PG_DATABASE,
                                  app_name=PG_TABLE_MONITOR_APPNAME, capture=False,
                                  secrets_to_redact=self.secrets_to_redact)
            cg = cgroup_snapshot(self.cgroup_dir)
            pss = _pg_process_pss(self.case_dir, self.container_id)
            ended = time.monotonic()
            sample = {"sample_index": len(self.samples),
                      "sampled_unix_ns": before_unix,
                      "sampled_monotonic_ns": before_mono,
                      "snapshot_complete_unix_ns": time.time_ns(),
                      "snapshot_complete_monotonic_ns": time.monotonic_ns(),
                      "query_started_unix_ns": before_unix,
                      "query_started_monotonic_ns": before_mono,
                      "query_duration_seconds": ended - started,
                      "query_end_unix_ns": time.time_ns(),
                      "admin_database": "postgres", "admin_application_name": PG_MONITOR_APPNAME,
                      "table_database": PG_DATABASE,
                      "table_application_name": PG_TABLE_MONITOR_APPNAME,
                      "stats": stats, "table_stats": tables, "cgroup": cg, "postgres_pss": pss}
            self._handle.write(json.dumps(sample, sort_keys=True, allow_nan=False) + "\n")
            self._handle.flush()
            self.samples.append(sample)
            return sample

    def start(self) -> dict[str, Any]:
        baseline = self.capture()
        self.thread = threading.Thread(target=self._loop,
                                       args=(baseline["sampled_monotonic_ns"],),
                                       name="pg-monitor-1hz", daemon=True)
        self.thread.start()
        return baseline

    def _loop(self, previous_capture_start_ns: int) -> None:
        interval_ns = int(SAMPLE_INTERVAL_SECONDS * 1_000_000_000)
        due_ns = previous_capture_start_ns + interval_ns
        while not self.stop_event.is_set():
            remaining = (due_ns - time.monotonic_ns()) / 1_000_000_000
            if remaining > 0 and self.stop_event.wait(remaining):
                break
            try:
                sample = self.capture()
            except Exception as exc:
                self.errors.append(f"{type(exc).__name__}: {exc}")
                self.stop_event.set()
                break
            capture_start_ns = sample.get("sampled_monotonic_ns")
            if not isinstance(capture_start_ns, int):
                self.errors.append("PostgreSQL monitor sample has no monotonic capture-start timestamp")
                self.stop_event.set()
                break
            # Anchor every due time to the actual prior capture start. If capture
            # ran past its next due time, take the next snapshot immediately;
            # recomputing from that actual start prevents catch-up bursts.
            due_ns = capture_start_ns + interval_ns

    def stop(self) -> dict[str, Any]:
        self.stop_event.set()
        if self.thread is not None:
            self.thread.join(timeout=30)
            if self.thread.is_alive():
                self.errors.append("PostgreSQL monitor failed to stop within 30 seconds")
        self._handle.flush()
        self._handle.close()
        if self.errors:
            raise CompareError("PostgreSQL monitor failed: " + "; ".join(self.errors))
        if len(self.samples) < 2:
            raise CompareError("PostgreSQL monitor captured fewer than two samples")
        return {"valid": True, "sample_count": len(self.samples), "sample_period_seconds": SAMPLE_INTERVAL_SECONDS,
                "stats_fetch_consistency": "none (fresh one-shot transactions)",
                "admin_connection_database": "postgres", "admin_application_name": PG_MONITOR_APPNAME,
                "table_connection_database": PG_DATABASE,
                "table_application_name": PG_TABLE_MONITOR_APPNAME,
                "extra_monitor_connections_excluded_from_data_pool_D": True,
                "samples_path": str(self.samples_path), "samples": self.samples}


def analyze_pg_monitor_window(samples: Sequence[dict[str, Any]], start_unix_ns: int,
                              end_unix_ns: int) -> dict[str, Any]:
    if end_unix_ns <= start_unix_ns or len(samples) < 2:
        raise CompareError("cannot analyze an empty PostgreSQL monitor window")
    left_candidates = [sample for sample in samples if sample.get("sampled_unix_ns", 0) <= start_unix_ns]
    right_candidates = [sample for sample in samples if sample.get("sampled_unix_ns", 0) >= end_unix_ns]
    if not left_candidates or not right_candidates:
        raise CompareError("PostgreSQL monitor samples do not bracket the measured window")
    left, right = left_candidates[-1], right_candidates[0]
    if (start_unix_ns - left["sampled_unix_ns"] > int(SAMPLE_MAX_GAP_SECONDS * 1e9)
            or right["sampled_unix_ns"] - end_unix_ns > int(SAMPLE_MAX_GAP_SECONDS * 1e9)):
        raise CompareError("nearest PostgreSQL monitor boundary sample is >2.5 seconds from the measured window")
    async_delta = pg_stat_snapshot_delta(left, right)
    if async_delta.get("valid") is not True:
        raise CompareError("PostgreSQL statistics reset or cumulative counter decreased during the measured window")
    left_cg = left.get("cgroup") or {}
    right_cg = right.get("cgroup") or {}
    cpu_delta = (right_cg.get("cpu_stat") or {}).get("usage_usec", 0) - (left_cg.get("cpu_stat") or {}).get("usage_usec", 0)
    if cpu_delta < 0:
        raise CompareError("PostgreSQL cgroup CPU usage counter moved backwards")
    memory_samples = [sample.get("cgroup", {}).get("memory", {}) for sample in samples
                      if start_unix_ns <= sample.get("sampled_unix_ns", 0) <= end_unix_ns]
    pss_samples = [sample.get("postgres_pss", {}).get("pss_kb") for sample in samples
                   if start_unix_ns <= sample.get("sampled_unix_ns", 0) <= end_unix_ns]
    table_left = left.get("table_stats", {}).get("stats")
    table_right = right.get("table_stats", {}).get("stats")
    left_io = parse_cgroup_io_stat(left_cg.get("io_stat"))
    right_io = parse_cgroup_io_stat(right_cg.get("io_stat"))
    io_deltas = {}
    for device in sorted(set(left_io) | set(right_io)):
        arow, brow = left_io.get(device), right_io.get(device)
        if arow is None or brow is None:
            io_deltas[device] = {"status": "not_available", "reason": "device absent at one boundary"}
            continue
        counters = {}
        for name in sorted(set(arow) & set(brow)):
            if brow[name] < arow[name]:
                counters[name] = None
            else:
                counters[name] = brow[name] - arow[name]
        io_deltas[device] = {"status": "valid" if None not in counters.values() else "counter_decrease",
                             "counters": counters}
    memory_currents = [row.get("memory.current") for row in memory_samples
                       if isinstance(row.get("memory.current"), int)]
    swap_currents = [row.get("memory.swap.current") for row in memory_samples
                     if isinstance(row.get("memory.swap.current"), int)]
    return {"valid": True, "window_start_unix_ns": start_unix_ns,
            "window_end_unix_ns": end_unix_ns, "window_duration_ns": end_unix_ns - start_unix_ns,
            "before_sample_unix_ns": left["sampled_unix_ns"],
            "after_sample_unix_ns": right["sampled_unix_ns"],
            "before_query_start_offset_ns": left.get("query_started_unix_ns", left["sampled_unix_ns"]) - start_unix_ns,
            "before_query_completion_offset_ns": left.get("snapshot_complete_unix_ns", left["sampled_unix_ns"]) - start_unix_ns,
            "after_query_start_offset_ns": right.get("query_started_unix_ns", right["sampled_unix_ns"]) - end_unix_ns,
            "after_query_completion_offset_ns": right.get("snapshot_complete_unix_ns", right["sampled_unix_ns"]) - end_unix_ns,
            "postgresql_statistics": async_delta,
            "stats_are_asynchronously_published": True,
            "cgroup": {"cpu_usage_usec_delta": cpu_delta,
                       "memory_current_start": (left_cg.get("memory") or {}).get("memory.current"),
                       "memory_current_end": (right_cg.get("memory") or {}).get("memory.current"),
                       "maximum_sampled_memory_current_during_window": max(memory_currents) if memory_currents else None,
                       "container_lifetime_memory_peak_at_boundary": (right_cg.get("memory") or {}).get("memory.peak"),
                       "memory_swap_current_start": (left_cg.get("memory") or {}).get("memory.swap.current"),
                       "memory_swap_current_end": (right_cg.get("memory") or {}).get("memory.swap.current"),
                       "maximum_sampled_memory_swap_current_during_window": max(swap_currents) if swap_currents else None,
                       "memory_events_end": (right_cg.get("memory") or {}).get("memory.events"),
                       "io_stat_start": left_cg.get("io_stat"), "io_stat_end": right_cg.get("io_stat"),
                       "io_stat_deltas": io_deltas},
            "postgres_process_pss_kb": {"sample_count": len(pss_samples),
                                        "minimum": min(pss_samples) if pss_samples else None,
                                        "maximum": max(pss_samples) if pss_samples else None,
                                        "sum_rss_is_not_used": True},
            "table_autovacuum_dead_tuple_snapshots": {"before": table_left, "after": table_right},
            "activity_wait_snapshots": {"before": (left.get("stats") or {}).get("activity"),
                                        "after": (right.get("stats") or {}).get("activity")}}


def validate_pg_cgroup_io_window(window: Any, *, label: str) -> dict[str, Any]:
    if not isinstance(window, dict) or window.get("valid") is not True:
        raise CompareError(f"PostgreSQL {label} cgroup I/O window is invalid")
    cgroup = window.get("cgroup")
    deltas = cgroup.get("io_stat_deltas") if isinstance(cgroup, dict) else None
    device = deltas.get("8:16") if isinstance(deltas, dict) else None
    if not isinstance(device, dict) or device.get("status") != "valid":
        raise CompareError(f"PostgreSQL {label} cgroup I/O counters lack a valid capped-device 8:16 delta")
    counters = device.get("counters")
    if not isinstance(counters, dict) or not {"rbytes", "wbytes", "rios", "wios"}.issubset(counters):
        raise CompareError(f"PostgreSQL {label} cgroup I/O counters are missing bytes or operation fields")
    return {"valid": True, "device": "8:16", "counters": counters}


def _directory_manifest(root: pathlib.Path) -> list[dict[str, Any]]:
    manifest: list[dict[str, Any]] = []
    root_real = root.resolve(strict=True)
    for current, dirs, files in os.walk(root_real, topdown=True, followlinks=False):
        current_path = pathlib.Path(current)
        for name in sorted(dirs + files):
            path = current_path / name
            info = path.lstat()
            mode = info.st_mode
            if stat.S_ISLNK(mode) or not (stat.S_ISDIR(mode) or stat.S_ISREG(mode)):
                raise CompareError(f"refusing to remove owned PostgreSQL data containing special path: {path}")
            manifest.append({"path": path.relative_to(root_real).as_posix(),
                             "type": "directory" if stat.S_ISDIR(mode) else "file",
                             "mode": oct(stat.S_IMODE(mode)), "uid": info.st_uid,
                             "gid": info.st_gid, "size": info.st_size})
    return manifest


class OwnedPostgresCase:
    """One PostgreSQL instance, fresh internal network, owned bind mount and caps."""

    def __init__(self, run_dir: pathlib.Path, case_dir: pathlib.Path, run_id: str,
                 case: CompareCase, limits: dict[str, Any]) -> None:
        self.run_dir = run_dir
        self.case_dir = case_dir
        self.run_id = run_id
        self.case = case
        self.case_id = case.case_id
        self.limits = limits
        self.nonce = uuid.uuid4().hex
        self.network_name = f"wallet-db-pg-{self.nonce[:12]}"
        self.container_name = f"wallet-db-pg-{self.nonce[:12]}"
        self.bind_root = (case_dir / "owned-data" / "postgres").resolve()
        self.schema = f"wallet_bench_{self.nonce[:20]}"
        self.network_id: str | None = None
        self.container_id: str | None = None
        self.host_pid: int | None = None
        self.cgroup_dir: pathlib.Path | None = None
        self.container_ip: str | None = None
        self.readiness_probe_path = case_dir / "postgres-readiness-probes.jsonl"
        self.readiness_probe_count = 0
        self.mounted_for_run = False
        self.container_removed = False
        self.network_removed = False
        self.data_removed = False
        self.cleanup_errors: list[str] = []

    def _labels(self) -> list[str]:
        result = []
        for key, value in (("owner", "ledger-wallet-db-compare"),
                           ("compare-nonce", self.nonce),
                           ("compare-run-id", self.run_id),
                           ("compare-case-id", self.case_id)):
            result.extend(["--label", f"{key}={value}"])
        return result

    def create(self) -> dict[str, Any]:
        _validate_schema_name(self.schema)
        self.case_dir.mkdir(parents=True, exist_ok=False)
        owner_manifest = {"format": 1, "owner": "ledger-wallet-db-compare",
                          "run_id": self.run_id, "case_id": self.case_id,
                          "nonce": self.nonce, "schema": self.schema,
                          "pgdata_mount": str(self.bind_root),
                          "container_mount": "/var/lib/postgresql",
                          "pgdata_container_path": "/var/lib/postgresql/18/docker"}
        _write_json(self.case_dir / "ownership.json", owner_manifest)
        self.bind_root.mkdir(parents=True, exist_ok=False, mode=0o755)
        self.bind_root.chmod(0o755)
        bind_info = disk_observer.resolve_device_profile(self.bind_root)
        disk_observer.validate_calibrated_device_profile(bind_info)
        if bind_info.get("data_device", {}).get("major_minor") != "8:18":
            raise CompareError("PostgreSQL data bind is not on qualified /dev/sdb2 (8:18)")
        _write_json(self.case_dir / "data-device-profile.json", bind_info)
        net = run_recorded_command(self.case_dir, "network-create",
            ["docker", "network", "create", "--driver", "bridge", "--internal",
             *self._labels(), self.network_name])
        self.network_id = net.stdout.strip()
        if not self.network_id:
            raise CompareError("Docker did not return the owned PostgreSQL network ID")
        network = self._inspect_network()
        self._validate_network(network)
        _write_json(self.case_dir / "network-inspect.json", network)

        postgres_argv: list[str] = ["postgres"]
        for name, (value, _) in PG_SETTINGS_EXPECTED.items():
            if name in {"shared_buffers", "work_mem", "maintenance_work_mem",
                        "autovacuum_work_mem", "max_connections", "fsync",
                        "synchronous_commit", "full_page_writes", "wal_sync_method",
                        "commit_delay", "wal_level", "checkpoint_timeout", "max_wal_size",
                        "autovacuum", "track_io_timing", "track_wal_io_timing"}:
                if name.endswith("_mem") or name in {"shared_buffers", "max_wal_size"}:
                    unit_text = {"shared_buffers": "2GB", "work_mem": "4MB",
                                 "maintenance_work_mem": "256MB", "autovacuum_work_mem": "64MB",
                                 "max_wal_size": "1GB"}[name]
                elif name == "checkpoint_timeout":
                    unit_text = "5min"
                else:
                    unit_text = str(value)
                postgres_argv.extend(["-c", f"{name}={unit_text}"])
        argv = ["docker", "run", "--detach", "--name", self.container_name,
                *self._labels(), "--network", self.network_name,
                "--cpuset-cpus", "0,1", "--memory", str(PG_CGROUP_MEMORY_BYTES),
                "--memory-swap", str(PG_CGROUP_MEMORY_BYTES),
                *wallet_runner.docker_io_limit_flags(self.limits),
                "--mount", f"type=bind,src={self.bind_root},dst=/var/lib/postgresql",
                "--env", "POSTGRES_HOST_AUTH_METHOD=trust",
                "--env", "POSTGRES_USER=postgres", "--env", f"POSTGRES_DB={PG_DATABASE}",
                "--env", "PGDATA=/var/lib/postgresql/18/docker",
                PG_IMAGE_REF, *postgres_argv]
        run_recorded_command(self.case_dir, "container-run", argv)
        # The name is generated by us, but ownership still comes only from verified labels.
        self.container_id = self.container_name
        inspect = self._inspect_container()
        self._validate_container_ownership(inspect)
        self.mounted_for_run = True
        state = inspect.get("State") or {}
        self.host_pid = int(state.get("Pid") or 0)
        if self.host_pid <= 0:
            self._capture_failure_state("container-has-no-pid")
            raise CompareError("PostgreSQL container has no host PID immediately after launch")
        pid_cgroup = pathlib.Path(f"/proc/{self.host_pid}/cgroup").read_text(encoding="ascii")
        relative = next((line.split("::", 1)[1] for line in pid_cgroup.splitlines()
                         if line.startswith("0::")), None)
        if relative is None:
            raise CompareError("PostgreSQL host PID does not have a cgroup-v2 path")
        self.cgroup_dir = (pathlib.Path("/sys/fs/cgroup").resolve() / relative.lstrip("/")).resolve()
        profile = validate_container_profile(inspect.get("HostConfig") or {}, inspect,
                                             self.host_pid, self.cgroup_dir, self.limits,
                                             self.network_name, self.bind_root)
        ports = (inspect.get("NetworkSettings") or {}).get("Ports") or {}
        if any(value not in (None, []) for value in ports.values()):
            raise CompareError("PostgreSQL container unexpectedly publishes a host port")
        env_items = inspect.get("Config", {}).get("Env") or []
        env = dict(item.split("=", 1) for item in env_items if "=" in item)
        for key, expected in (("POSTGRES_HOST_AUTH_METHOD", "trust"),
                              ("POSTGRES_USER", "postgres"), ("POSTGRES_DB", PG_DATABASE),
                              ("PGDATA", "/var/lib/postgresql/18/docker")):
            if env.get(key) != expected:
                raise CompareError(f"PostgreSQL container environment {key} differs from expected")
        network_info = (inspect.get("NetworkSettings", {}).get("Networks", {}) or {}).get(self.network_name)
        if not isinstance(network_info, dict):
            raise CompareError("PostgreSQL container is not attached to its owned network")
        ip = network_info.get("IPAddress")
        try:
            import ipaddress
            parsed_ip = ipaddress.ip_address(ip)
        except ValueError as exc:
            raise CompareError(f"PostgreSQL container network IP is invalid: {ip!r}") from exc
        if parsed_ip.is_unspecified or parsed_ip.is_loopback or parsed_ip.is_multicast:
            raise CompareError("PostgreSQL container has an unsafe network IP")
        self.container_ip = str(parsed_ip)
        _write_json(self.case_dir / "container-runtime-profile.json", profile)
        self._wait_ready()
        return {"valid": True, "container_id": self.container_id,
                "container_ip": self.container_ip, "network_id": self.network_id,
                "host_pid": self.host_pid, "cgroup_dir": str(self.cgroup_dir),
                "schema": self.schema, "data_device": bind_info,
                "readiness_probe_attempts_file": str(self.readiness_probe_path),
                "readiness_probe_attempt_count": self.readiness_probe_count,
                "runtime_limits": profile}

    def _inspect_container(self) -> dict[str, Any]:
        identifier = self.container_id or self.container_name
        return _inspect_container(self.case_dir, identifier, ())

    def _inspect_network(self) -> dict[str, Any]:
        identifier = self.network_id or self.network_name
        result = run_recorded_command(self.case_dir, "network-inspect",
                                      ["docker", "network", "inspect", identifier], check=False)
        if result.returncode != 0:
            raise CompareError(f"owned PostgreSQL network {identifier} cannot be inspected")
        value = json.loads(result.stdout)
        if not isinstance(value, list) or len(value) != 1 or not isinstance(value[0], dict):
            raise CompareError("Docker network inspect returned an unexpected result")
        return value[0]

    def _inspect_named_container_optional(self) -> dict[str, Any] | None:
        """Return None only for Docker's exact not-found response, never for daemon errors."""
        result = run_recorded_command(self.case_dir, "inspect-owned-name-cleanup",
                                     ["docker", "inspect", self.container_name], check=False)
        if result.returncode == 0:
            try:
                value = json.loads(result.stdout)
            except json.JSONDecodeError as exc:
                raise CompareError("Docker named-container inspect returned invalid JSON") from exc
            if not isinstance(value, list) or len(value) != 1 or not isinstance(value[0], dict):
                raise CompareError("Docker named-container inspect returned an unexpected result")
            return value[0]
        if (f"No such object: {self.container_name}" in result.stderr
                or f"No such object: {self.container_name}" in result.stdout):
            return None
        raise CompareError("Docker could not inspect the exact owned container name: "
                           + (result.stderr or result.stdout).strip())

    def _inspect_named_network_optional(self) -> dict[str, Any] | None:
        """Return None only when the exact generated network name is absent."""
        result = run_recorded_command(self.case_dir, "network-inspect-owned-name-cleanup",
                                     ["docker", "network", "inspect", self.network_name], check=False)
        if result.returncode == 0:
            try:
                value = json.loads(result.stdout)
            except json.JSONDecodeError as exc:
                raise CompareError("Docker named-network inspect returned invalid JSON") from exc
            if not isinstance(value, list) or len(value) != 1 or not isinstance(value[0], dict):
                raise CompareError("Docker named-network inspect returned an unexpected result")
            return value[0]
        missing = f"Error: No such network: {self.network_name}"
        if missing in result.stderr or missing in result.stdout:
            return None
        raise CompareError("Docker could not inspect the exact owned network name: "
                           + (result.stderr or result.stdout).strip())

    def _validate_network(self, network: dict[str, Any]) -> None:
        _validate_owned_labels(network, nonce=self.nonce, run_id=self.run_id,
                               case_id=self.case_id, resource="network")
        if network.get("Driver") != "bridge" or network.get("Internal") is not True:
            raise CompareError("PostgreSQL case network is not a dedicated internal bridge")

    def _validate_container_ownership(self, inspect: dict[str, Any]) -> None:
        _validate_owned_labels(inspect, nonce=self.nonce, run_id=self.run_id,
                               case_id=self.case_id, resource="container")
        if inspect.get("Name", "").lstrip("/") != self.container_name:
            raise CompareError("PostgreSQL container name differs from exact owned case name")

    def _capture_failure_state(self, suffix: str) -> None:
        if not self.container_id:
            return
        inspected = subprocess.run(["docker", "inspect", self.container_id], text=True,
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    timeout=20, check=False)
        (self.case_dir / f"failure-container-inspect-{suffix}.stdout.json").write_text(
            inspected.stdout, encoding="utf-8")
        (self.case_dir / f"failure-container-inspect-{suffix}.stderr.txt").write_text(
            inspected.stderr, encoding="utf-8")
        logs = subprocess.run(["docker", "logs", self.container_id], text=True,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              timeout=20, check=False)
        (self.case_dir / f"failure-container-logs-{suffix}.txt").write_text(
            logs.stdout + logs.stderr, encoding="utf-8")

    def _wait_ready(self) -> None:
        deadline = time.monotonic() + PG_READY_SECONDS
        attempt = 0
        self.readiness_probe_path.parent.mkdir(parents=True, exist_ok=True)
        argv = ["docker", "exec", "--user", "postgres", self.container_id or "",
                "pg_isready", "-h", "127.0.0.1", "-p", "5432",
                "-U", "postgres", "-d", PG_DATABASE]
        if self.readiness_probe_path.exists():
            raise CompareError("PostgreSQL readiness evidence path already exists")
        with self.readiness_probe_path.open("x", encoding="utf-8", buffering=1) as evidence:
            while time.monotonic() < deadline:
                attempt += 1
                row: dict[str, Any] = {"attempt": attempt, "timestamp_unix_ns": time.time_ns(),
                                       "argv": argv}
                if not self.container_id:
                    row["error"] = "container_id is missing"
                    evidence.write(json.dumps(row, sort_keys=True) + "\n")
                    evidence.flush()
                    os.fsync(evidence.fileno())
                    raise CompareError("PostgreSQL container has no ID during TCP readiness check")
                try:
                    probe = subprocess.run(argv, text=True, stdout=subprocess.PIPE,
                                           stderr=subprocess.PIPE, timeout=20, check=False)
                except (OSError, subprocess.SubprocessError) as exc:
                    row.update({"error": f"{type(exc).__name__}: {exc}"})
                    evidence.write(json.dumps(row, sort_keys=True) + "\n")
                    evidence.flush()
                    os.fsync(evidence.fileno())
                    self._capture_failure_state(f"readiness-probe-error-{attempt:03d}")
                    raise CompareError(f"PostgreSQL TCP readiness probe command failed: {exc}") from exc
                row.update({"returncode": probe.returncode, "stdout": probe.stdout,
                            "stderr": probe.stderr})
                if probe.returncode == 0:
                    evidence.write(json.dumps(row, sort_keys=True) + "\n")
                    evidence.flush()
                    os.fsync(evidence.fileno())
                    self.readiness_probe_count = attempt
                    return
                try:
                    inspect = self._inspect_container()
                except Exception as exc:
                    row["container_inspect_error"] = f"{type(exc).__name__}: {exc}"
                    evidence.write(json.dumps(row, sort_keys=True) + "\n")
                    evidence.flush()
                    os.fsync(evidence.fileno())
                    self._capture_failure_state(f"readiness-inspect-error-{attempt:03d}")
                    raise CompareError(f"cannot inspect PostgreSQL after failed TCP readiness probe: {exc}") from exc
                state = inspect.get("State") or {}
                row["container_state"] = {key: state.get(key) for key in
                                           ("Status", "Running", "ExitCode", "Error")}
                evidence.write(json.dumps(row, sort_keys=True) + "\n")
                evidence.flush()
                if state.get("Status") not in {"running", "restarting"} or state.get("Running") is not True:
                    os.fsync(evidence.fileno())
                    self.readiness_probe_count = attempt
                    self._capture_failure_state(f"early-exit-{attempt:03d}")
                    raise CompareError(f"PostgreSQL container exited before TCP readiness: {state}")
                time.sleep(min(1.0, max(0.0, deadline - time.monotonic())))
            evidence.flush()
            os.fsync(evidence.fileno())
            self.readiness_probe_count = attempt
        self._capture_failure_state("readiness-timeout")
        raise CompareError(f"PostgreSQL TCP readiness timed out after {PG_READY_SECONDS}s")

    def admin(self, name: str, sql: str, *, database: str = "postgres") -> dict[str, Any]:
        return _docker_psql(self.case_dir, self.container_id or "", name, sql,
                            database=database, app_name=PG_MONITOR_APPNAME,
                            secrets_to_redact=())

    def capture_runtime(self, sql_source: str) -> dict[str, Any]:
        settings = self.admin("postgres-runtime-settings", postgres_settings_sql())
        settings_validation = validate_pg_runtime_settings(settings)
        stats = self.admin("postgres-runtime-stats-capability", postgres_stats_sql())
        stat_validation = validate_pg_stat_capabilities(stats)
        schema_evidence = self.admin("postgres-runtime-schema-catalog",
                                     postgres_schema_sql(self.schema), database=PG_DATABASE)
        functions = schema_evidence.get("functions")
        if isinstance(functions, list):
            for function in functions:
                if isinstance(function, dict) and isinstance(function.get("source_body"), str):
                    function["source_body_sha256"] = sha256_bytes(
                        normalize_sql_body(function["source_body"]).encode())
        schema_validation = validate_pg_schema_evidence(schema_evidence, sql_source)
        runtime = {"valid": True, "captured_before_measurement_gate": True,
                   "settings": settings, "settings_validation": settings_validation,
                   "baseline_stats": stats, "stats_capability_validation": stat_validation,
                   "catalog": schema_evidence, "catalog_validation": schema_validation,
                   "postgres_data_device": "8:18", "docker_image_ref": PG_IMAGE_REF,
                   "docker_image_id": PG_IMAGE_ID, "postgres_version": PG_VERSION,
                   "host_io_caps": self.limits}
        _write_json(self.case_dir / "postgres-runtime-evidence.json", runtime)
        return runtime

    def host_dsn(self) -> str:
        if not self.container_ip:
            raise CompareError("PostgreSQL container IP is not known")
        # The per-case bridge is internal and has no published port; trust auth is scoped to it.
        dsn = (f"postgresql://postgres@{self.container_ip}:5432/{PG_DATABASE}"
               f"?application_name={PG_SERVICE_APPNAME}&connect_timeout=10")
        try:
            with socket.create_connection((self.container_ip, 5432), timeout=3):
                pass
        except OSError as exc:
            failure_path = self.case_dir / "host-network-reachability-failure.json"
            failure = {"timestamp_unix_ns": time.time_ns(), "container_ip": self.container_ip,
                       "port": 5432, "timeout_seconds": 3,
                       "error_type": type(exc).__name__, "errno": exc.errno,
                       "error": str(exc)}
            try:
                _write_json(failure_path, failure)
                self._capture_failure_state("host-dsn-connect-failure")
            except Exception as capture_error:
                failure["failure_evidence_capture_error"] = (
                    f"{type(capture_error).__name__}: {capture_error}")
                _write_json(failure_path, failure)
            raise CompareError("host service cannot reach PostgreSQL on its private internal-bridge IP") from exc
        return dsn

    def cleanup(self) -> dict[str, Any]:
        actions: list[dict[str, Any]] = []
        if self.container_id is None:
            try:
                candidate = self._inspect_named_container_optional()
                if candidate is None:
                    self.container_removed = True
                    actions.append({"container_absent_by_exact_generated_name": True})
                else:
                    self._validate_container_ownership(candidate)
                    self.container_id = str(candidate.get("Id") or self.container_name)
                    actions.append({"recovered_container_by_exact_name_and_labels": True})
            except Exception as exc:
                self.cleanup_errors.append(f"container discovery: {type(exc).__name__}: {exc}")
                actions.append({"container_removed": False, "error": self.cleanup_errors[-1]})
        if self.container_id:
            try:
                inspect = self._inspect_container()
                self._validate_container_ownership(inspect)
                mounts = inspect.get("Mounts") or []
                bind = next((item for item in mounts if item.get("Destination") == "/var/lib/postgresql"), None)
                exact_mount = bool(bind and bind.get("Type") == "bind"
                                   and pathlib.Path(bind.get("Source", "")).resolve() == self.bind_root.resolve())
                if not exact_mount:
                    raise CompareError("owned PostgreSQL container bind source changed before cleanup")
                state = inspect.get("State", {}).get("Status")
                if state == "running":
                    stopped = run_recorded_command(self.case_dir, "container-stop-cleanup",
                        ["docker", "stop", "--time", str(CONTAINER_STOP_SECONDS), self.container_id],
                        check=False, timeout=CONTAINER_STOP_SECONDS + 20)
                    actions.append({"container_stop_returncode": stopped.returncode})
                stopped_inspect = self._inspect_container()
                _write_json(self.case_dir / "container-inspect-stopped.json", stopped_inspect)
                self._validate_container_ownership(stopped_inspect)
                removed = run_recorded_command(self.case_dir, "container-remove-cleanup",
                    ["docker", "rm", self.container_id], check=False)
                if removed.returncode != 0:
                    raise CompareError("owned PostgreSQL container removal failed")
                self.container_removed = True
                actions.append({"container_removed": True, "exact_bind_mount_verified": True})
            except Exception as exc:
                self.cleanup_errors.append(f"container: {type(exc).__name__}: {exc}")
                actions.append({"container_removed": False, "error": self.cleanup_errors[-1]})
        elif not self.container_removed:
            # Discovery failed. Retain data and report the failure; absence is never inferred
            # from a Docker daemon/permission/inspection error.
            actions.append({"container_removed": False, "reason": "exact ownership could not be verified"})
        if self.network_id is None:
            try:
                network = self._inspect_named_network_optional()
                if network is None:
                    self.network_removed = True
                    actions.append({"network_absent_by_exact_generated_name": True})
                else:
                    self._validate_network(network)
                    self.network_id = str(network.get("Id") or self.network_name)
            except Exception as exc:
                self.cleanup_errors.append(f"network discovery: {type(exc).__name__}: {exc}")
                actions.append({"network_removed": False, "error": self.cleanup_errors[-1]})
        if self.network_id:
            try:
                network = self._inspect_network()
                self._validate_network(network)
                containers = network.get("Containers") or {}
                if containers:
                    raise CompareError(f"owned PostgreSQL network still has attached endpoints: {sorted(containers)}")
                removed = run_recorded_command(self.case_dir, "network-remove-cleanup",
                    ["docker", "network", "rm", self.network_id], check=False)
                if removed.returncode != 0:
                    raise CompareError("owned PostgreSQL network removal failed")
                self.network_removed = True
                actions.append({"network_removed": True})
            except Exception as exc:
                self.cleanup_errors.append(f"network: {type(exc).__name__}: {exc}")
                actions.append({"network_removed": False, "error": self.cleanup_errors[-1]})
        try:
            self._cleanup_data(actions)
        except Exception as exc:
            self.cleanup_errors.append(f"data: {type(exc).__name__}: {exc}")
            actions.append({"data_removed": False, "retained_for_safety": True,
                            "error": self.cleanup_errors[-1]})
        result = {"status": "passed" if not self.cleanup_errors else "failed",
                  "actions": actions, "errors": self.cleanup_errors,
                  "container_removed": self.container_removed,
                  "network_removed": self.network_removed, "data_removed": self.data_removed}
        _write_json(self.case_dir / "cleanup.json", result)
        return result

    def _cleanup_data(self, actions: list[dict[str, Any]]) -> None:
        owner_path = self.case_dir / "ownership.json"
        try:
            owner = json.loads(owner_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise CompareError(f"cannot validate exact PostgreSQL case ownership manifest: {exc}") from exc
        expected_owner = {"owner": "ledger-wallet-db-compare", "run_id": self.run_id,
                          "case_id": self.case_id, "nonce": self.nonce,
                          "pgdata_mount": str(self.bind_root),
                          "container_mount": "/var/lib/postgresql",
                          "pgdata_container_path": "/var/lib/postgresql/18/docker"}
        if not isinstance(owner, dict) or any(owner.get(key) != value for key, value in expected_owner.items()):
            raise CompareError("PostgreSQL data ownership manifest does not match this exact run/case/nonce")
        root = self.bind_root
        if not root.exists() and not root.is_symlink():
            owned_parent = self.case_dir / "owned-data"
            if owned_parent.exists():
                if owned_parent.is_symlink() or any(owned_parent.iterdir()):
                    raise CompareError("refusing to remove unexpected PostgreSQL owned-data entries")
                owned_parent.rmdir()
            self.data_removed = True
            actions.append({"data_removed": True, "method": "no data bind was created"})
            return
        if root.is_symlink() or not root.exists():
            raise CompareError("owned PostgreSQL bind root is missing or symlinked")
        case_real = self.case_dir.resolve(strict=True)
        if root.resolve(strict=True).parent != (case_real / "owned-data").resolve(strict=True):
            raise CompareError("owned PostgreSQL data path escaped exact comparison case")
        if not self.container_removed:
            raise CompareError("retaining PostgreSQL data because its owned container was not removed")
        if not self.mounted_for_run:
            if any(root.iterdir()):
                raise CompareError("retaining PostgreSQL data because no exact owned bind mount was verified")
            root.rmdir()
            (root.parent).rmdir()
            self.data_removed = True
            actions.append({"data_removed": True, "method": "host rmdir of empty run-created directories"})
            return
        names = sorted(item.name for item in root.iterdir())
        if not names:
            root.rmdir()
            root.parent.rmdir()
            self.data_removed = True
            actions.append({"data_removed": True, "method": "host rmdir of empty run-created directories"})
            return
        if names != ["18"]:
            raise CompareError(f"refusing PG data cleanup; owned bind root has unexpected entries: {names}")
        version_root = root / "18"
        if not version_root.is_dir() or version_root.is_symlink():
            raise CompareError("refusing PG cleanup; /18 is not a plain run-owned directory")
        version_names = sorted(item.name for item in version_root.iterdir())
        if version_names not in ([], ["docker"]):
            raise CompareError(f"refusing PG cleanup; unexpected PG18 entries: {version_names}")
        if "docker" in version_names and ((version_root / "docker").is_symlink()
                                           or not (version_root / "docker").is_dir()):
            raise CompareError("refusing PG cleanup; PGDATA child is unsafe")
        manifest = _directory_manifest(root)
        manifest_raw = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
        manifest_record = {"run_id": self.run_id, "case_id": self.case_id,
                           "nonce": self.nonce, "bind_root": str(root),
                           "mount_destination": "/var/lib/postgresql",
                           "pgdata": "/var/lib/postgresql/18/docker",
                           "manifest_sha256": sha256_bytes(manifest_raw), "entries": manifest}
        _write_json(self.case_dir / "data-directory-manifest-before-cleanup.json", manifest_record)
        helper_name = f"wallet-db-pg-cleanup-{self.nonce[:12]}"
        helper_argv = ["docker", "run", "--rm", "--name", helper_name,
                       *self._labels(), "--network", "none", "--user", "0:0",
                       "--cpuset-cpus", "0,1", "--memory", "268435456",
                       "--memory-swap", "268435456", *wallet_runner.docker_io_limit_flags(self.limits),
                       "--mount", f"type=bind,src={root},dst=/var/lib/postgresql",
                       "--entrypoint", "sh", PG_IMAGE_REF, "-ec",
                       "rm -rf -- /var/lib/postgresql/18"]
        run_recorded_command(self.case_dir, "owned-data-cleanup-helper", helper_argv,
                             timeout=180, secrets_to_redact=())
        if list(root.iterdir()):
            raise CompareError("owned PostgreSQL cleanup helper left entries in data bind")
        root.rmdir()
        root.parent.rmdir()
        self.data_removed = True
        actions.append({"data_removed": True,
                        "method": "pinned-image root helper after exact ownership/path manifest check",
                        "helper_name": helper_name,
                        "manifest_sha256": manifest_record["manifest_sha256"]})


def _utc_stamp() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _comparison_source_paths() -> list[pathlib.Path]:
    evidence_fixtures = [REPO_ROOT / "tests" / "fixtures" / name for name in (
        "postgres18_schema_catalog_raw.json",
        "postgres18_settings_raw.json",
        "postgres18_stats_raw.json",
        "postgres18_pss_direct_postgres.json",
        "scylla_periodic_missing_commitlog_series.json",
        "README.md",
    )]
    paths = [REPO_ROOT / "benches" / "wallet_db_compare.py",
             REPO_ROOT / "benches" / "scylla_wallet.py",
             REPO_ROOT / "benches" / "scylla_io_observer.py",
             REPO_ROOT / "tests" / "test_wallet_db_compare.py",
             REPO_ROOT / "tests" / "test_scylla_wallet_runner.py",
             REPO_ROOT / "tests" / "test_scylla_io_observer.py",
             REPO_ROOT / "docs" / "01-63.raw-requirement-wallet-db-equal-resource-benchmark.md",
             REPO_ROOT / "docs" / "01-64.development-design-wallet-db-equal-resource-benchmark.md",
             REPO_ROOT / "docs" / "01-65.development-validation-wallet-db-equal-resource-benchmark.md",
             *evidence_fixtures,
             SCYLLA_IO_PROPERTIES_PATH,
             REPO_ROOT / "Cargo.toml", REPO_ROOT / "Cargo.lock",
             CRATE_DIR / "Cargo.toml", CRATE_DIR / "Cargo.lock"]
    for folder in (CRATE_DIR / "src", CRATE_DIR / "tests", CRATE_DIR / "sql"):
        paths.extend(sorted(folder.rglob("*.rs" if folder.name != "sql" else "*.sql")))
    required = [path for path in paths if path.parent in {
        REPO_ROOT / "benches", REPO_ROOT / "tests", REPO_ROOT / "docs"}
        or path in evidence_fixtures or path == SCYLLA_IO_PROPERTIES_PATH]
    missing = [str(path) for path in required if not path.is_file()]
    if missing:
        raise CompareError(f"comparison source snapshot is missing required inputs: {missing}")
    return sorted({path.resolve() for path in paths if path.is_file()})


def comparison_source_hashes() -> dict[str, str]:
    return {str(path.relative_to(REPO_ROOT)): sha256_bytes(path.read_bytes())
            for path in _comparison_source_paths()}


def _contract_body(source_hashes: dict[str, str], binary_sha256: str,
                   caps_sha256: str, caps: dict[str, Any]) -> dict[str, Any]:
    caps_values = {key: caps[key] for key in
                   ("device_path", "major_minor", "read_bps", "write_bps", "read_iops", "write_iops")}
    return {
        "contract_version": 1,
        "purpose": "equal-resource synchronous wallet database comparison",
        "measurement_claim": "full correctness-verified request RPS and latency; no steady-state claim",
        "input_hashes": {"comparison_sources": source_hashes,
                         "release_binary_sha256": binary_sha256,
                         "postgres_sql_sha256": POSTGRES_SCHEMA_SQL_SHA256,
                         "scylla_io_properties_sha256": SCYLLA_IO_PROPERTIES_SHA256,
                         "host_io_caps_sha256": caps_sha256},
        "binary": {"release": "target/scylla-wallet/release/ledger-wallet-scylla-bench",
                   "sha256": binary_sha256,
                   "expected_final_agent_build_sha256": EXPECTED_RELEASE_BINARY_SHA256},
        "postgres": {
            "image": PG_IMAGE_REF, "image_id": PG_IMAGE_ID,
            "runtime_version": PG_RUNTIME_VERSION, "database": PG_DATABASE,
            "data_mount": "/var/lib/postgresql",
            "pgdata": "/var/lib/postgresql/18/docker",
            "cpus": list(PG_DATABASE_CPUS), "cpuset": "0,1",
            "container_memory_bytes": PG_CGROUP_MEMORY_BYTES,
            "container_memory_swap_bytes": PG_SHARED_SWAP_BYTES,
            "cpu_quota": "unlimited",
            "settings": {name: value for name, (value, _) in PG_SETTINGS_EXPECTED.items()},
            "synchronous_durability": {"fsync": "on", "synchronous_commit": "on",
                                        "full_page_writes": "on", "wal_sync_method": "fdatasync",
                                        "commit_delay": 0, "wal_level": "replica"},
            "connection_calibration": list(PG_CALIBRATION_CONNECTIONS),
            "connection_selection": "smallest D within 95% of the best valid full-workload RPS",
            "wallet_profile": "postgres-memory-tx-v1",
            "sql_function": "wallet_commit",
            "sql_acknowledgement": "ReadyForQuery after implicit transaction completion",
            "schema_scope": "unique owned namespace in fresh walletbench database",
            "network_route": {
                "mode": "host-native service reaches container IP on this case's dedicated internal Docker bridge",
                "published_host_ports": False,
                "authentication": "POSTGRES_HOST_AUTH_METHOD=trust scoped to the internal bridge",
                "qualification_status": "bounded qualification 20261013T115555Z-5fba6c262ab4 passed host_ip_test, Wallet integration, and 32x32 CLI smoke; each campaign case still requires the fail-closed runtime reachability probe",
            },
        },
        "scylla": {
            "image": wallet_runner.PINNED_REPO_DIGEST,
            "shards": SCYLLA_SHARDS, "per_shard_pool": SCYLLA_POOL_PER_SHARD,
            "data_connections": SCYLLA_CONNECTIONS,
            "replication_factor": 1, "read_consistency": "ONE",
            "write_consistency": "ONE", "serial_consistency": "LOCAL_SERIAL",
            "commitlog_sync": "batch", "commitlog_use_o_dsync": False,
            "process_memory_bytes": 8 * 1024**3,
            "container_memory_bytes": wallet_runner.NODE_CGROUP_BYTES,
            "io_properties_profile": {
                "source_file": SCYLLA_IO_PROPERTIES_RELATIVE_PATH,
                "source_sha256": SCYLLA_IO_PROPERTIES_SHA256,
                "runtime_validation_required": True,
                "mount_read_only": True,
                "load_validation": "source hash + container mount + launch argv + runtime file hash + loaded device/queue mapping",
            },
            "host_diskstats_observer": {
                "implementation": "benches/scylla_io_observer.py",
                "collection": "read-only host /proc/diskstats at 1 Hz from before trial through after cleanup",
                "required_devices": {"whole_device": "8:16 (sdb)",
                                     "data_partition": "8:18 (sdb2)"},
                "required_measurement_views": ["whole_device", "data_partition"],
                "maximum_sampling_gap_seconds": disk_observer.MAX_SAMPLE_GAP_SECONDS,
                "required_analysis": "existing analyze_trial plus comparator host-delta schema",
            },
            "host_io_limits": "same as PostgreSQL",
        },
        "shared_host_io_limits": caps_values,
        "shared_resources": {
            "database_cpus": [0, 1],
            "database_container_memory_bytes": PG_CGROUP_MEMORY_BYTES,
            "memory_swap_limit_equals_memory_limit": True,
            "service_cpus": list(SERVICE_CPUS),
            "service_runner_includes_all_application_and_pool_tasks": True,
            "one_database_container_at_a_time": True,
            "host_global_io_or_sysfs_changes": False,
        },
        "workload": {
            "mode": "pilot", "users": USERS, "requests_per_user": REQUESTS_PER_USER,
            "total_requests": TOTAL_REQUESTS, "C": WORKERS_C,
            "amount": 1, "operations": "alternating credit/debit per account",
            "expected_credit_requests": TOTAL_REQUESTS // 2,
            "expected_debit_requests": TOTAL_REQUESTS // 2,
            "per_worker_queue_capacity": QUEUE_CAPACITY,
            "global_queue_capacity": QUEUE_CAPACITY,
            "setup_concurrency": SETUP_CONCURRENCY,
            "state_preload_concurrency": SETUP_CONCURRENCY,
            "worker_routing": "round-robin",
            "service_cpu_set": list(SERVICE_CPUS),
            "verification": "all account state and ledger rows; no retry on error",
        },
        "campaign": {
            "postgres_calibration": [
                {"backend": "postgres", "pg_connections": value, "repeats": 1,
                 "case_id": f"pg-calibration-D{value}"}
                for value in PG_CALIBRATION_CONNECTIONS],
            "comparison_order": ["scylla-rep1", "postgres-rep1", "postgres-rep2",
                                 "scylla-rep2", "scylla-rep3", "postgres-rep3"],
            "repeats_per_backend": 3,
            "run_one_database_at_a_time": True,
            "stop_on_first_invalid_case": True,
            "automatic_workload_retries": False,
        },
        "preflight": {"routine": "wallet_runner.strict_formal_preflight",
                      "wait_seconds": PREFLIGHT_SECONDS,
                      "required_available_disk_bytes": MIN_DISK_BYTES,
                      "required_available_memory_bytes": MIN_AVAILABLE_MEMORY_BYTES,
                      "max_host_cpu_busy_pct": 10,
                      "max_backing_device_busy_pct": 5,
                      "consecutive_samples": 3,
                      "apply_scylla_aio_gate_to_postgres": False},
        "monitoring": {
            "postgres_stats_interval_seconds": SAMPLE_INTERVAL_SECONDS,
            "postgres_monitor_application_names": [PG_MONITOR_APPNAME, PG_TABLE_MONITOR_APPNAME],
            "service_pool_excluded_from_monitor_count": True,
            "stats_fetch_consistency": "none; fresh snapshots; cumulative PG counters are asynchronously published",
            "measured_window": "first request through last reply; E2E includes pool wait",
            "pool_wait_and_sql_stage_separate": True,
            "host_diskstats": "read-only shared observer; report whole-disk and partition deltas",
            "checkpoint_and_drain_io": "captured and reported separately after measured request window",
            "io_limit_verification": "Docker HostConfig plus owned container cgroup io.max must match all four 8:16 caps",
            "container_cgroup_cpu_memory_pss": True,
            "no_sum_of_shared_rss": True,
        },
        "ownership": {"fresh_container_network_data_per_case": True,
                      "exact_nonce_labels_required_for_cleanup": True,
                      "stopped_container_inspected_before_remove": True,
                      "unknown_resources_are_never_removed": True,
                      "failure_evidence_preserved": True},
        "artifacts": {"run_root": str(DEFAULT_OUTPUT_ROOT),
                      "plan_requires_explicit_sha256_approval_before_execution": True,
                      "captured_source_and_binary_snapshots": True},
    }


def _binary_path(value: pathlib.Path | None) -> pathlib.Path:
    path = value or (REPO_ROOT / "target" / "scylla-wallet" / "release" /
                     wallet_runner.RUST_PACKAGE)
    try:
        resolved = path.expanduser().resolve(strict=True)
    except OSError as exc:
        raise CompareError(f"release CLI binary is missing: {path}: {exc}") from exc
    if not resolved.is_file() or not os.access(resolved, os.X_OK):
        raise CompareError(f"release CLI binary is not an executable regular file: {resolved}")
    return resolved


def create_frozen_plan(plan_root: pathlib.Path, binary: pathlib.Path,
                       caps_path: pathlib.Path) -> dict[str, Any]:
    caps_raw = caps_path.read_bytes()
    caps_sha = sha256_bytes(caps_raw)
    if caps_sha != "c95f1a8ba5204051fe48e61bd05333ef10122f1b9e0ad78c8a41ad1bbf9b87fc":
        raise CompareError(f"host I/O cap file hash differs from qualified bytes: {caps_sha}")
    caps = load_host_io_limits(caps_path)
    if caps is None:
        raise CompareError("comparison plan requires the shared host I/O caps file")
    sql_sha = sha256_bytes(SQL_PATH.read_bytes())
    if sql_sha != POSTGRES_SCHEMA_SQL_SHA256:
        raise CompareError(f"PostgreSQL SQL file changed from the reviewed source: {sql_sha}")
    scylla_io_sha = sha256_bytes(SCYLLA_IO_PROPERTIES_PATH.read_bytes())
    if scylla_io_sha != SCYLLA_IO_PROPERTIES_SHA256:
        raise CompareError("calibrated Scylla I/O properties differ from the approved source: "
                           f"{scylla_io_sha}")
    source_hashes = comparison_source_hashes()
    binary_sha = sha256_bytes(binary.read_bytes())
    if binary_sha != EXPECTED_RELEASE_BINARY_SHA256:
        raise CompareError("release CLI binary does not match the finalized Rust agent build: "
                           f"expected {EXPECTED_RELEASE_BINARY_SHA256}, observed {binary_sha}")
    contract = _contract_body(source_hashes, binary_sha, caps_sha, caps)
    contract_sha = sha256_bytes(canonical_json_bytes(contract))
    plan_root.mkdir(parents=True, exist_ok=False)
    snapshot = plan_root / "source-snapshot"
    snapshot.mkdir()
    copied: list[dict[str, Any]] = []
    for source in _comparison_source_paths():
        relative = source.relative_to(REPO_ROOT)
        destination = snapshot / "repo" / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
        destination.chmod(stat.S_IMODE(destination.stat().st_mode) & ~0o222)
        copied.append({"path": relative.as_posix(),
                       "sha256": sha256_bytes(destination.read_bytes()),
                       "size_bytes": destination.stat().st_size})
    binary_snapshot = snapshot / "binary" / wallet_runner.RUST_PACKAGE
    binary_snapshot.parent.mkdir(parents=True)
    shutil.copy2(binary, binary_snapshot)
    binary_snapshot.chmod(0o555)
    caps_snapshot = snapshot / "io_caps.json"
    shutil.copy2(caps_path, caps_snapshot)
    caps_snapshot.chmod(0o444)
    artifact = {"created_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
                "plan_id": plan_root.name,
                "binary_snapshot_relative_path": str(binary_snapshot.relative_to(plan_root)),
                "caps_snapshot_relative_path": str(caps_snapshot.relative_to(plan_root)),
                "source_snapshot_files": copied}
    plan = {"contract": contract, "contract_sha256": contract_sha,
            "artifact": artifact}
    _write_json(plan_root / "plan.json", plan)
    _write_json(plan_root / "source-snapshot" / "manifest.json",
                {"contract_sha256": contract_sha, "source_files": copied,
                 "binary_sha256": binary_sha, "caps_sha256": caps_sha})
    return {"plan_dir": str(plan_root), "plan_file": str(plan_root / "plan.json"),
            "contract_sha256": contract_sha, "binary_sha256": binary_sha,
            "source_file_count": len(copied), "status": "review_required_no_containers_started"}


def validate_frozen_plan(plan_file: pathlib.Path, approved_sha256: str) -> tuple[dict[str, Any], pathlib.Path, dict[str, Any]]:
    try:
        plan = json.loads(plan_file.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CompareError(f"cannot read comparison plan {plan_file}: {exc}") from exc
    if not isinstance(plan, dict) or not isinstance(plan.get("contract"), dict):
        raise CompareError("comparison plan does not contain a contract")
    contract = plan["contract"]
    actual_sha = sha256_bytes(canonical_json_bytes(contract))
    if plan.get("contract_sha256") != actual_sha:
        raise CompareError("comparison plan contract checksum is invalid")
    if not re.fullmatch(r"[0-9a-f]{64}", approved_sha256 or "") or approved_sha256 != actual_sha:
        raise CompareError("execution requires --approved-contract-sha256 equal to the frozen contract hash")
    expected_binary_sha = contract.get("binary", {}).get("sha256")
    expected_sources = contract.get("input_hashes", {}).get("comparison_sources")
    if expected_sources != comparison_source_hashes():
        raise CompareError("comparison source files changed since the plan was frozen; create and review a new plan")
    sql_sha = sha256_bytes(SQL_PATH.read_bytes())
    if sql_sha != POSTGRES_SCHEMA_SQL_SHA256 or contract.get("input_hashes", {}).get("postgres_sql_sha256") != sql_sha:
        raise CompareError("PostgreSQL SQL source hash differs from the frozen plan")
    scylla_io_sha = sha256_bytes(SCYLLA_IO_PROPERTIES_PATH.read_bytes())
    if (scylla_io_sha != SCYLLA_IO_PROPERTIES_SHA256
            or contract.get("input_hashes", {}).get("scylla_io_properties_sha256") != scylla_io_sha):
        raise CompareError("calibrated Scylla I/O properties hash differs from the frozen plan")
    artifact = plan.get("artifact")
    if not isinstance(artifact, dict):
        raise CompareError("comparison plan snapshot metadata is missing")
    root = plan_file.resolve(strict=True).parent
    frozen_files = artifact.get("source_snapshot_files")
    if not isinstance(frozen_files, list):
        raise CompareError("comparison plan source snapshot manifest is missing")
    contract_sources = contract.get("input_hashes", {}).get("comparison_sources")
    if not isinstance(contract_sources, dict):
        raise CompareError("comparison plan source hash map is missing")
    observed_paths: set[str] = set()
    for item in frozen_files:
        if not isinstance(item, dict) or not isinstance(item.get("path"), str):
            raise CompareError("comparison plan source snapshot entry is malformed")
        relative = pathlib.PurePosixPath(item["path"])
        if relative.is_absolute() or ".." in relative.parts:
            raise CompareError("comparison plan source snapshot path escapes its repository root")
        key = relative.as_posix()
        if key in observed_paths or key not in contract_sources:
            raise CompareError(f"comparison plan source snapshot has duplicate or unexpected file {key}")
        observed_paths.add(key)
        snapshot_path = root / "source-snapshot" / "repo" / pathlib.Path(*relative.parts)
        if snapshot_path.is_symlink() or not snapshot_path.is_file():
            raise CompareError(f"comparison plan source snapshot is missing or symlinked: {key}")
        if sha256_bytes(snapshot_path.read_bytes()) != contract_sources[key]:
            raise CompareError(f"comparison plan source snapshot hash differs from contract: {key}")
    if observed_paths != set(contract_sources):
        raise CompareError("comparison plan source snapshot inventory differs from frozen source hashes")
    binary_relative = pathlib.PurePosixPath(str(artifact.get("binary_snapshot_relative_path", "")))
    if binary_relative.is_absolute() or ".." in binary_relative.parts:
        raise CompareError("comparison plan binary snapshot path escapes the plan directory")
    binary = root.joinpath(*binary_relative.parts)
    if binary.is_symlink() or not binary.is_file():
        raise CompareError("frozen release binary snapshot is missing or symlinked")
    observed_binary_sha = sha256_bytes(binary.read_bytes())
    if observed_binary_sha != expected_binary_sha:
        raise CompareError("frozen release binary does not match the approved contract hash")
    cap_relative = pathlib.PurePosixPath(str(artifact.get("caps_snapshot_relative_path", "")))
    if cap_relative.is_absolute() or ".." in cap_relative.parts:
        raise CompareError("comparison plan caps snapshot path escapes the plan directory")
    caps_snapshot = root.joinpath(*cap_relative.parts)
    if caps_snapshot.is_symlink() or not caps_snapshot.is_file():
        raise CompareError("frozen host I/O cap snapshot is missing or symlinked")
    caps_sha = sha256_bytes(caps_snapshot.read_bytes())
    if caps_sha != contract.get("input_hashes", {}).get("host_io_caps_sha256"):
        raise CompareError("frozen host I/O caps do not match the approved contract hash")
    caps = load_host_io_limits(caps_snapshot)
    if caps is None:
        raise CompareError("frozen plan has no shared host I/O caps")
    return plan, binary, caps


def validate_pg_ready_event(event: Any, *, schema: str, pg_connections: int,
                            sql_sha256: str) -> dict[str, Any]:
    if not isinstance(event, dict) or event.get("phase") != "ready_to_measure":
        raise CompareError("PostgreSQL CLI did not reach its ready_to_measure gate")
    for name, expected in (("backend", "postgres"),
                           ("wallet_profile", "postgres-memory-tx-v1"),
                           ("mode", "pilot"), ("database_name", PG_DATABASE),
                           ("pg_schema", schema), ("pg_connections", pg_connections),
                           ("db_cpu_count", 2), ("C", WORKERS_C),
                           ("data_connection_target", pg_connections),
                           ("schema_sql_sha256", sql_sha256),
                           ("application_name", PG_SERVICE_APPNAME),
                           ("accounts_table", "accounts"),
                           ("accounts_primary_key_constraint", "accounts_pkey"),
                           ("ledger_table", "ledger"),
                           ("ledger_primary_key_constraint", "ledger_pkey"),
                           ("ledger_primary_key", ["account_id", "tx_id"]),
                           ("account_state_source", "memory"),
                           ("pool_ready_connections", pg_connections),
                           ("account_states_preloaded", USERS),
                           ("all_clients_ready", True),
                           ("setup_concurrency", SETUP_CONCURRENCY),
                           ("state_preload_concurrency", SETUP_CONCURRENCY),
                           ("users", USERS), ("requests_per_user", REQUESTS_PER_USER),
                           ("total_requests", TOTAL_REQUESTS)):
        if event.get(name) != expected:
            raise CompareError(f"ready_to_measure {name}={event.get(name)!r}; expected {expected!r}")
    pids = event.get("postgres_backend_pids")
    if (not isinstance(pids, list) or len(pids) != pg_connections
            or any(isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0 for pid in pids)
            or len(set(pids)) != pg_connections):
        raise CompareError("ready_to_measure does not identify exactly D PostgreSQL pool backend PIDs")
    session_id = event.get("session_id")
    if not isinstance(session_id, str) or not session_id.startswith(f"postgres:{schema}:"):
        raise CompareError("ready_to_measure session_id does not identify the owned PostgreSQL schema")
    forbidden = {"nodes", "keyspace", "S", "P", "replication_factor", "read_cl",
                 "write_cl", "serial_cl", "cql_statement_count", "paxos"}
    if forbidden.intersection(event):
        raise CompareError("PostgreSQL ready event contains Scylla-only workload fields")
    if re.search(r"postgres(?:ql)?://|\bdsn\b", json.dumps(event, sort_keys=True), re.I):
        raise CompareError("PostgreSQL ready event appears to expose a DSN")
    return {"valid": True, "pool_ready_connections": pg_connections,
            "postgres_backend_pids": sorted(pids), "session_id": session_id}


class _CapturedChild:
    """Drain subprocess pipes into memory and persist only redacted output."""

    def __init__(self, argv: Sequence[str], *, cwd: pathlib.Path,
                 secrets_to_redact: Sequence[str] = ()) -> None:
        self.argv = list(argv)
        self.cwd = cwd
        self.secrets = tuple(secrets_to_redact)
        self.process = subprocess.Popen(self.argv, cwd=cwd, stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE, start_new_session=True)
        self._stdout: list[bytes] = []
        self._stderr: list[bytes] = []
        self._threads = [threading.Thread(target=self._drain, args=(self.process.stdout, self._stdout),
                                          daemon=True),
                         threading.Thread(target=self._drain, args=(self.process.stderr, self._stderr),
                                          daemon=True)]
        for thread in self._threads:
            thread.start()

    @staticmethod
    def _drain(stream: Any, target: list[bytes]) -> None:
        if stream is None:
            return
        while True:
            chunk = stream.read(65_536)
            if not chunk:
                return
            target.append(chunk)

    def terminate(self, timeout: float = 15.0) -> None:
        if self.process.poll() is None:
            try:
                os.killpg(self.process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                self.process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(self.process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                self.process.wait(timeout=10)

    def finish(self, case_dir: pathlib.Path, name: str, timeout: float = 30.0) -> dict[str, Any]:
        try:
            return_code = self.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            self.terminate()
            raise CompareError(f"{name} did not exit within {timeout}s") from exc
        for thread in self._threads:
            thread.join(timeout=10)
            if thread.is_alive():
                raise CompareError(f"{name} output reader did not stop")
        stdout = b"".join(self._stdout).decode("utf-8", errors="replace")
        stderr = b"".join(self._stderr).decode("utf-8", errors="replace")
        logs = case_dir / "logs"
        logs.mkdir(parents=True, exist_ok=True)
        (logs / f"{name}.stdout.log").write_text(_redact_text(stdout, self.secrets), encoding="utf-8")
        (logs / f"{name}.stderr.log").write_text(_redact_text(stderr, self.secrets), encoding="utf-8")
        _write_json(logs / f"{name}.command.json",
                    {"argv": _safe_argv(self.argv, self.secrets),
                     "returncode": return_code, "completed_unix_ns": time.time_ns()})
        return {"returncode": return_code,
                "stdout_sha256_redacted": sha256_bytes(_redact_text(stdout, self.secrets).encode()),
                "stderr_sha256_redacted": sha256_bytes(_redact_text(stderr, self.secrets).encode())}


def _read_complete_events(path: pathlib.Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    raw = path.read_bytes()
    lines = raw.splitlines(keepends=True)
    events = []
    for index, line in enumerate(lines):
        if not line.endswith(b"\n"):
            if index == len(lines) - 1:
                break
            raise CompareError("PostgreSQL CLI event stream contains an incomplete middle line")
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise CompareError(f"PostgreSQL CLI event line {index + 1} is invalid JSON: {exc}") from exc
        if not isinstance(value, dict):
            raise CompareError(f"PostgreSQL CLI event line {index + 1} is not an object")
        if value.get("status") == "failed" or value.get("phase") in {"failed", "finish_gate_timeout"}:
            raise CompareError(f"PostgreSQL CLI emitted failure event {value.get('phase')}: {value.get('error')}")
        events.append(value)
    return events


def _wait_pg_event(path: pathlib.Path, child: _CapturedChild, phase: str,
                   timeout_seconds: float) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        events = _read_complete_events(path)
        found = [event for event in events if event.get("phase") == phase]
        if found:
            return found[-1]
        code = child.process.poll()
        if code is not None:
            raise CompareError(f"PostgreSQL CLI exited {code} before {phase}")
        time.sleep(0.2)
    raise CompareError(f"PostgreSQL CLI did not emit {phase} within {timeout_seconds}s")


def _create_fsync_gate(path: pathlib.Path, nonce: str) -> None:
    with path.open("x", encoding="ascii") as handle:
        handle.write(nonce + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def _directory_free_bytes(path: pathlib.Path) -> int:
    return shutil.disk_usage(path).free


def _verify_preflight_artifact(document: dict[str, Any]) -> dict[str, Any]:
    if document.get("valid") is not True:
        raise CompareError("strict host resource preflight did not pass")
    samples = document.get("samples")
    if not isinstance(samples, list) or len(samples) < 3:
        raise CompareError("strict host resource preflight lacks three consecutive samples")
    recent = samples[-3:]
    for index, sample in enumerate(recent):
        if (sample.get("disk_available_bytes", 0) < MIN_DISK_BYTES
                or sample.get("memory_available_bytes", 0) < MIN_AVAILABLE_MEMORY_BYTES
                or sample.get("host_cpu_busy_pct") is None
                or sample.get("host_cpu_busy_pct") > 10
                or sample.get("device_busy_pct") is None
                or sample.get("device_busy_pct") > 5):
            raise CompareError(f"strict preflight sample {index} violates the frozen host resource thresholds")
    return {"valid": True, "sample_count": len(samples),
            "verified_consecutive_samples": 3, "thresholds": {
                "disk_available_bytes_minimum": MIN_DISK_BYTES,
                "memory_available_bytes_minimum": MIN_AVAILABLE_MEMORY_BYTES,
                "host_cpu_busy_pct_maximum": 10,
                "device_busy_pct_maximum": 5}}


def _check_pg_image(run_dir: pathlib.Path) -> dict[str, Any]:
    result = subprocess.run(["docker", "image", "inspect", PG_IMAGE_REF], cwd=REPO_ROOT,
                            text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            timeout=30, check=False)
    if result.returncode != 0:
        raise CompareError("pinned PostgreSQL image is not already present; execute will not pull it")
    try:
        items = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise CompareError("pinned PostgreSQL image inspect returned invalid JSON") from exc
    if not isinstance(items, list) or len(items) != 1 or not isinstance(items[0], dict):
        raise CompareError("pinned PostgreSQL image inspect returned an unexpected result")
    image = items[0]
    if image.get("Id") != PG_IMAGE_ID:
        raise CompareError(f"pinned PostgreSQL image ID differs: {image.get('Id')!r}")
    pinned_digest = PG_IMAGE_REF.split("@", 1)[1]
    if not any(item.endswith("@" + pinned_digest) for item in (image.get("RepoDigests") or [])):
        raise CompareError("local PostgreSQL image does not report the pinned immutable digest")
    if image.get("Architecture") != "amd64" or image.get("Os") != "linux":
        raise CompareError("local PostgreSQL image platform differs from qualified linux/amd64")
    evidence = {"valid": True, "reference": PG_IMAGE_REF, "image_id": image["Id"],
                "repo_digests": image.get("RepoDigests") or [],
                "architecture": image.get("Architecture"), "os": image.get("Os"),
                "created": image.get("Created"),
                "runtime_version_expected": PG_RUNTIME_VERSION}
    wallet_runner.write_json(run_dir / "postgres-image-preflight.json", evidence)
    return evidence


def _wait_observer(child: subprocess.Popen[Any], stop_file: pathlib.Path,
                   timeout_seconds: float = 40.0) -> dict[str, Any]:
    if not stop_file.exists():
        stop_file.parent.mkdir(parents=True, exist_ok=True)
        stop_file.write_text("stop\n", encoding="ascii")
    try:
        code = child.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired as exc:
        try:
            os.killpg(child.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            child.wait(timeout=5)
        raise CompareError("shared diskstats observer did not exit after its stop-file gate") from exc
    if code != 0:
        raise CompareError(f"shared diskstats observer exited with status {code}")
    return {"valid": True, "returncode": code, "stop_file": str(stop_file)}


def _wait_diskstats_sample_after(observer_dir: pathlib.Path, boundary_unix_ns: int,
                                 child: subprocess.Popen[Any], timeout_seconds: float = 4.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    samples_path = observer_dir / "samples.jsonl"
    while time.monotonic() < deadline:
        if child.poll() is not None:
            raise CompareError(f"shared diskstats observer exited {child.returncode} before the post-window boundary sample")
        if samples_path.is_file():
            samples = disk_observer.read_observer_samples(samples_path)
            after = [sample for sample in samples
                     if isinstance(sample.get("time_unix_ns"), int)
                     and sample["time_unix_ns"] >= boundary_unix_ns]
            if after:
                return after[0]
        time.sleep(0.1)
    raise CompareError("shared diskstats observer did not sample after the post-window checkpoint")


def _pg_cli_argv(binary: pathlib.Path, case: OwnedPostgresCase, dsn: str,
                 service_dir: pathlib.Path, start_gate: pathlib.Path,
                 finish_gate: pathlib.Path) -> list[str]:
    return [str(binary), "--backend", "postgres", "--pg-dsn", dsn,
            "--pg-schema", case.schema, "--pg-connections", str(case.case.pg_connections),
            "--db-cpus", str(len(PG_DATABASE_CPUS)), "--db-workers", str(WORKERS_C),
            "--users", str(USERS), "--requests-per-user", str(REQUESTS_PER_USER),
            "--queue-capacity", str(QUEUE_CAPACITY), "--output-dir", str(service_dir),
            "--mode", "pilot", "--start-gate", str(start_gate),
            "--finish-gate", str(finish_gate)]


def _pg_checkpoint_command(case: OwnedPostgresCase, name: str) -> dict[str, Any]:
    argv = ["docker", "exec", "--user", "postgres", "--env",
            f"PGAPPNAME={PG_MONITOR_APPNAME}", case.container_id or "",
            "psql", "-X", "-q", "-A", "-t", "-v", "ON_ERROR_STOP=1",
            "-U", "postgres", "-d", PG_DATABASE, "-c", "CHECKPOINT"]
    result = run_recorded_command(case.case_dir, name, argv, timeout=600)
    return {"returncode": result.returncode, "stdout": result.stdout.strip(),
            "completed_unix_ns": time.time_ns()}


def _record_pg_failure(case_dir: pathlib.Path, record: dict[str, Any],
                       exc: BaseException) -> None:
    record["status"] = "failed"
    record["error"] = f"{type(exc).__name__}: {exc}"
    record["failed_unix_ns"] = time.time_ns()
    try:
        _write_json(case_dir / "case-result-before-cleanup.json", record)
    except Exception:
        pass


def run_postgres_case(run_id: str, run_dir: pathlib.Path, case: CompareCase,
                      binary: pathlib.Path, limits: dict[str, Any]) -> dict[str, Any]:
    if case.backend != "postgres" or case.pg_connections not in (*PG_CALIBRATION_CONNECTIONS, 16, 32, 64):
        raise CompareError("invalid PostgreSQL campaign case")
    case_root = run_dir / "postgres-cases"
    case_root.mkdir(parents=True, exist_ok=True)
    case_dir = case_root / case.case_id
    sql_source = SQL_PATH.read_text(encoding="utf-8")
    sql_sha = sha256_bytes(sql_source.encode())
    if sql_sha != POSTGRES_SCHEMA_SQL_SHA256:
        raise CompareError("PostgreSQL SQL source changed before case launch")
    owned = OwnedPostgresCase(run_dir, case_dir, run_id, case, limits)
    cli: _CapturedChild | None = None
    monitor: PostgresMonitor | None = None
    observer: subprocess.Popen[Any] | None = None
    observer_stop = case_dir / "disk-observer.stop"
    observer_stdout = observer_stderr = None
    observer_stdout_handle = observer_stderr_handle = None
    service_dir = case_dir / "service"
    start_gate = case_dir / "start.gate"
    finish_gate = case_dir / "finish.gate"
    record: dict[str, Any] = {
        "order": case.order, "case_id": case.case_id, "backend": "postgres",
        "repeat": case.repeat, "pg_connections": case.pg_connections,
        "mode": "pilot", "workload": {"users": USERS,
            "requests_per_user": REQUESTS_PER_USER, "total_requests": TOTAL_REQUESTS,
            "C": WORKERS_C, "queue_capacity": QUEUE_CAPACITY},
        "schema": owned.schema, "schema_sql_sha256": sql_sha,
        "postgres_image": PG_IMAGE_REF, "host_io_limits": limits,
        "binary_sha256": sha256_bytes(binary.read_bytes()),
        "status": "running", "started_unix_ns": time.time_ns(),
    }
    error: BaseException | None = None
    process_finished = False
    monitor_stopped = False
    observer_stopped = False
    try:
        record["container"] = owned.create()
        record["ownership"] = json.loads((case_dir / "ownership.json").read_text(encoding="utf-8"))
        service_dir.mkdir()
        observer_dir = case_dir / "disk-observer"
        observer_stop = observer_dir / "stop.gate"
        observer_stdout = (case_dir / "logs" / "disk-observer.stdout.log")
        observer_stderr = (case_dir / "logs" / "disk-observer.stderr.log")
        observer_stdout.parent.mkdir(parents=True, exist_ok=True)
        observer_stdout_handle = observer_stdout.open("xb")
        observer_stderr_handle = observer_stderr.open("xb")
        observer_started_ns = time.time_ns()
        observer = subprocess.Popen(
            [sys.executable, str(REPO_ROOT / "benches" / "scylla_io_observer.py"),
             "--collect", "--output-dir", str(observer_dir), "--data-path", str(case_dir),
             "--stop-file", str(observer_stop)], cwd=REPO_ROOT,
            stdout=observer_stdout_handle, stderr=observer_stderr_handle,
            start_new_session=True)
        record["disk_observer"] = {"pid": observer.pid, "output_dir": str(observer_dir),
                                   "stop_file": str(observer_stop), "read_only_host_diskstats": True}
        record["disk_observer_first_sample_before_host_probe"] = _wait_diskstats_sample_after(
            observer_dir, observer_started_ns, observer)
        dsn = owned.host_dsn()
        safe_service_command = ["taskset", "-c", ",".join(map(str, SERVICE_CPUS)),
                                *_safe_argv(_pg_cli_argv(binary, owned, dsn, service_dir,
                                                        start_gate, finish_gate), (dsn,))]
        _write_json(case_dir / "service-command.json",
                    {"argv": safe_service_command,
                     "dsn_redacted": True, "application_name": PG_SERVICE_APPNAME,
                     "service_cpus": list(SERVICE_CPUS)})
        taskset = shutil.which("taskset")
        if not taskset:
            raise CompareError("taskset is required to pin PostgreSQL load generator to CPUs 6,7,14,15")
        command = [taskset, "-c", ",".join(map(str, SERVICE_CPUS)),
                   *_pg_cli_argv(binary, owned, dsn, service_dir, start_gate, finish_gate)]
        cli = _CapturedChild(command, cwd=REPO_ROOT, secrets_to_redact=(dsn,))
        events_path = service_dir / "events.jsonl"
        ready = _wait_pg_event(events_path, cli, "ready_to_measure", CLI_READY_SECONDS)
        ready_validation = validate_pg_ready_event(ready, schema=owned.schema,
                                                   pg_connections=int(case.pg_connections),
                                                   sql_sha256=sql_sha)
        if dsn in json.dumps(ready, sort_keys=True):
            raise CompareError("PostgreSQL CLI ready event exposed the private DSN")
        if ready.get("start_gate") != str(start_gate) or ready.get("finish_gate") != str(finish_gate):
            raise CompareError("PostgreSQL ready event gate paths differ from this fresh owned case")
        record["ready_event"] = ready
        record["ready_validation"] = ready_validation

        runtime = owned.capture_runtime(sql_source)
        service_pids = ready_validation["postgres_backend_pids"]
        runtime_activity = runtime["baseline_stats"].get("activity")
        if (not isinstance(runtime_activity, dict)
                or sorted(runtime_activity.get("service_backend_pids", [])) != service_pids
                or runtime_activity.get("connections") != case.pg_connections):
            raise CompareError("pre-gate pg_stat_activity backend PIDs differ from the CLI-ready pool")
        record["postgres_runtime_evidence"] = runtime

        monitor = PostgresMonitor(case_dir, owned.container_id or "", owned.cgroup_dir,
                                  owned.schema, secrets_to_redact=(dsn,))
        baseline = monitor.start()
        baseline_activity = baseline["stats"].get("activity")
        if (not isinstance(baseline_activity, dict)
                or sorted(baseline_activity.get("service_backend_pids", [])) != service_pids
                or baseline_activity.get("connections") != case.pg_connections):
            raise CompareError("external monitor baseline does not match the exact CLI pool PIDs")
        _create_fsync_gate(start_gate, owned.nonce)
        measuring = _wait_pg_event(events_path, cli, "measuring", CLI_GATE_SECONDS)
        record["measuring_event"] = measuring
        client_end = _wait_pg_event(events_path, cli, "client_end", CLI_MEASURE_SECONDS)
        record["client_end_event"] = client_end
        start_ns = _integer(measuring.get("measurement_start_unix_ns"),
                            "measuring.measurement_start_unix_ns", minimum=1)
        end_ns = _integer(client_end.get("measurement_end_unix_ns"),
                          "client_end.measurement_end_unix_ns", minimum=1)
        if end_ns <= start_ns or client_end.get("measurement_start_unix_ns") != start_ns:
            raise CompareError("PostgreSQL CLI measured-window timestamps are inconsistent")

        # Capture the first post-reply boundary immediately; this diagnostic query and
        # its disk I/O are outside the CLI's first-request-to-last-reply interval.
        post_reply_sample = monitor.capture()
        if post_reply_sample["sampled_unix_ns"] < end_ns:
            raise CompareError("external PostgreSQL monitor wall clock preceded the last-reply boundary")
        drained = _wait_pg_event(events_path, cli, "drained", CLI_FINISH_SECONDS)
        verified = _wait_pg_event(events_path, cli, "verified", CLI_FINISH_SECONDS)
        awaiting = _wait_pg_event(events_path, cli, "awaiting_runner_audit", CLI_FINISH_SECONDS)
        record["drained_event"] = drained
        record["verified_event"] = verified
        record["awaiting_runner_audit_event"] = awaiting
        if cli.process.poll() is not None:
            raise CompareError(f"PostgreSQL CLI exited before the finish-gate audit: {cli.process.returncode}")

        before_checkpoint = monitor.capture()
        checkpoint_started_ns = time.time_ns()
        checkpoint = _pg_checkpoint_command(owned, "post-window-checkpoint")
        checkpoint_finished_ns = time.time_ns()
        after_checkpoint = monitor.capture()
        checkpoint_window = analyze_pg_monitor_window(
            [before_checkpoint, after_checkpoint],
            before_checkpoint["sampled_unix_ns"], after_checkpoint["sampled_unix_ns"])
        checkpoint_io_validation = validate_pg_cgroup_io_window(
            checkpoint_window, label="post-window checkpoint")
        record["post_window_checkpoint_and_drain"] = {
            "scope": "after the CLI's last-reply measurement boundary; excluded from RPS",
            "checkpoint_started_unix_ns": checkpoint_started_ns,
            "checkpoint_finished_unix_ns": checkpoint_finished_ns,
            "checkpoint_command": checkpoint,
            "cgroup_and_stats_before": before_checkpoint,
            "cgroup_and_stats_after": after_checkpoint,
            "postgresql_cumulative_stats_delta": pg_stat_snapshot_delta(before_checkpoint, after_checkpoint),
            "cgroup_io_delta": checkpoint_window,
            "cgroup_io_validation": checkpoint_io_validation,
        }

        record["host_diskstats_post_window_boundary_sample"] = _wait_diskstats_sample_after(
            observer_dir, checkpoint_finished_ns, observer)
        sample_summary = monitor.stop()
        monitor_stopped = True
        monitor = None
        samples = sample_summary["samples"]
        pg_pool_validation = validate_pg_monitor_samples(samples, service_pids,
                                                        int(case.pg_connections))
        window = analyze_pg_monitor_window(samples, start_ns, end_ns)
        window["cgroup_io_validation"] = validate_pg_cgroup_io_window(
            window, label="measured workload")
        record["postgres_monitor"] = {
            "valid": True, "sample_count": sample_summary["sample_count"],
            "sample_period_seconds": SAMPLE_INTERVAL_SECONDS,
            "samples_path": sample_summary["samples_path"],
            "application_names": [PG_MONITOR_APPNAME, PG_TABLE_MONITOR_APPNAME],
            "stats_fetch_consistency": sample_summary["stats_fetch_consistency"],
            "pool_pids_validation": pg_pool_validation,
            "measured_window": window,
        }

        _wait_observer(observer, observer_stop)
        observer_stopped = True
        observer_stdout_handle.close()
        observer_stderr_handle.close()
        record["host_diskstats_measured_window"] = analyze_host_diskstats(
            observer_dir, start_ns, end_ns)
        record["host_diskstats_post_window_checkpoint"] = analyze_host_diskstats(
            observer_dir, checkpoint_started_ns, checkpoint_finished_ns)

        _create_fsync_gate(finish_gate, owned.nonce)
        released = _wait_pg_event(events_path, cli, "runner_audit_released", CLI_FINISH_SECONDS)
        record["runner_audit_released_event"] = released
        cli_result_code = cli.finish(case_dir, "postgres-cli")
        process_finished = True
        record["cli_process"] = cli_result_code
        if cli_result_code["returncode"] != 0:
            raise CompareError(f"PostgreSQL wallet CLI exited with status {cli_result_code['returncode']}")
        if (cli_result_code.get("stdout_sha256_redacted") is None
                or cli_result_code.get("stderr_sha256_redacted") is None):
            raise CompareError("PostgreSQL CLI output capture omitted redacted stream hashes")
        events = _read_complete_events(events_path)
        event_validation = _expected_events(events, owned.schema,
                                             int(case.pg_connections), sql_sha)
        if any(event.get("finish_gate") != str(finish_gate) for event in events):
            raise CompareError("a PostgreSQL lifecycle event changed or omitted the owned finish gate")
        result_path = service_dir / "result.json"
        try:
            result = json.loads(result_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise CompareError(f"PostgreSQL wallet CLI result.json is missing or invalid: {exc}") from exc
        if not isinstance(result, dict):
            raise CompareError("PostgreSQL wallet CLI result is not a JSON object")
        result_validation = validate_pg_result(result, pg_schema=owned.schema,
                                               pg_connections=int(case.pg_connections),
                                               sql_sha256=sql_sha)
        if (result["measurement"].get("start_unix_ns") != event_validation["measurement_start_unix_ns"]
                or result["measurement"].get("end_unix_ns") != event_validation["measurement_end_unix_ns"]):
            raise CompareError("PostgreSQL result measurement timestamps differ from lifecycle events")
        if sorted(result_validation["connection_backend_pids"]) != service_pids:
            raise CompareError("final PostgreSQL report connection PIDs differ from the pre-gate pool")
        record["events_validation"] = event_validation
        record["result_validation"] = result_validation
        record["cli_result"] = result
        record["result_sha256"] = sha256_bytes(canonical_json_bytes(result))
        record["rps"] = result_validation["rps"]
        record["measurement"] = result.get("measurement")
        record["status"] = "passed"
    except BaseException as exc:
        error = exc
        _record_pg_failure(case_dir, record, exc)
    finally:
        if cli is not None and not process_finished:
            try:
                cli.terminate()
                record["cli_process"] = cli.finish(case_dir, "postgres-cli-failed", timeout=30)
                process_finished = True
            except Exception as exc:
                record.setdefault("cleanup_warnings", []).append(
                    f"CLI termination/output capture: {type(exc).__name__}: {exc}")
        if monitor is not None and not monitor_stopped:
            try:
                monitor_summary = monitor.stop()
                record["postgres_monitor_partial"] = {
                    "sample_count": monitor_summary["sample_count"],
                    "samples_path": monitor_summary["samples_path"]}
            except Exception as exc:
                record.setdefault("cleanup_warnings", []).append(
                    f"PostgreSQL monitor stop: {type(exc).__name__}: {exc}")
        if observer is not None and not observer_stopped:
            try:
                observer_stop_path = case_dir / "disk-observer" / "stop.gate"
                _wait_observer(observer, observer_stop_path)
                observer_stopped = True
                if observer_stdout_handle is not None:
                    observer_stdout_handle.close()
                if observer_stderr_handle is not None:
                    observer_stderr_handle.close()
            except Exception as exc:
                record.setdefault("cleanup_warnings", []).append(
                    f"diskstats observer stop: {type(exc).__name__}: {exc}")
        try:
            record["cleanup"] = owned.cleanup()
            record["cleanup_status"] = record["cleanup"].get("status")
        except Exception as exc:
            record["cleanup_status"] = "failed"
            record.setdefault("cleanup_warnings", []).append(
                f"PostgreSQL owned cleanup: {type(exc).__name__}: {exc}")
        if record.get("cleanup_status") != "passed":
            record["status"] = "failed"
            record.setdefault("error", "owned PostgreSQL cleanup did not pass")
        record["finished_unix_ns"] = time.time_ns()
        try:
            _write_json(case_dir / "case-result.json", record)
        except Exception as exc:
            record.setdefault("cleanup_warnings", []).append(
                f"final case result write: {type(exc).__name__}: {exc}")
    if error is not None:
        raise CompareError(f"{case.case_id} failed: {type(error).__name__}: {error}") from error
    if record.get("status") != "passed" or record.get("cleanup_status") != "passed":
        raise CompareError(f"{case.case_id} did not complete a valid, clean trial")
    return record


def run_scylla_case(run_id: str, run_dir: pathlib.Path, case: CompareCase,
                    binary: pathlib.Path, limits: dict[str, Any]) -> dict[str, Any]:
    if case.backend != "scylla":
        raise CompareError("invalid Scylla campaign case")
    trial = wallet_runner.TrialCase(
        shards=SCYLLA_SHARDS, workers=WORKERS_C, per_shard_pool=SCYLLA_POOL_PER_SHARD,
        repeat=case.repeat, order=case.order, worker_routing="round-robin",
        per_shard_pool_overridden=True, requested_per_shard_pool=SCYLLA_POOL_PER_SHARD,
        replication_factor=1, read_cl="ONE", write_cl="ONE",
        wallet_profile=wallet_runner.WALLET_PROFILE, node_count=1)
    io_properties_profile = wallet_runner.inspect_io_properties_source(SCYLLA_IO_PROPERTIES_PATH)
    if (io_properties_profile.get("enabled") is not True
            or io_properties_profile.get("source_sha256") != SCYLLA_IO_PROPERTIES_SHA256):
        raise CompareError("calibrated Scylla I/O properties are missing or differ from the frozen profile")
    args = argparse.Namespace(
        mode="pilot", startup_timeout=wallet_runner.STARTUP_WAIT_SECONDS,
        commitlog_sync="batch", commitlog_sync_period_in_ms=None,
        resolved_commitlog_sync="batch", resolved_commitlog_sync_period_in_ms=None,
        io_properties_file=SCYLLA_IO_PROPERTIES_PATH,
        resolved_io_properties_profile=io_properties_profile,
        resolved_host_io_limits=limits, periodic_lwt_sync_source_evidence=None)
    expected_trial_id = (f"trial-{case.order:03d}-"
                         f"{wallet_runner.case_id_for_profile(trial, "batch", None)}")
    case_dir = run_dir / "trials" / expected_trial_id
    observer_dir = run_dir / "scylla-host-observers" / case.case_id
    observer_stop = observer_dir / "stop.gate"
    observer_stdout_path = run_dir / "logs" / f"{case.case_id}-disk-observer.stdout.log"
    observer_stderr_path = run_dir / "logs" / f"{case.case_id}-disk-observer.stderr.log"
    observer_stdout_path.parent.mkdir(parents=True, exist_ok=True)
    observer_stdout_handle = observer_stdout_path.open("xb")
    observer_stderr_handle = observer_stderr_path.open("xb")
    observer: subprocess.Popen[Any] | None = None
    observer_join: dict[str, Any] | None = None
    observer_join_error: str | None = None
    observer_first_sample: dict[str, Any] | None = None
    observer_analysis: dict[str, Any] | None = None
    host_window: dict[str, Any] | None = None
    analysis_error: str | None = None
    runner_error: BaseException | None = None
    result: dict[str, Any] | None = None
    observer_started_ns = time.time_ns()
    try:
        observer = subprocess.Popen(
            [sys.executable, str(REPO_ROOT / "benches" / "scylla_io_observer.py"),
             "--collect", "--output-dir", str(observer_dir), "--data-path", str(run_dir),
             "--stop-file", str(observer_stop)], cwd=REPO_ROOT,
            stdout=observer_stdout_handle, stderr=observer_stderr_handle,
            start_new_session=True)
        observer_first_sample = _wait_diskstats_sample_after(
            observer_dir, observer_started_ns, observer)
        result = wallet_runner.run_trial(run_id, run_dir, trial, args, USERS,
                                         REQUESTS_PER_USER, binary)
    except BaseException as exc:
        runner_error = exc
    finally:
        if observer is not None:
            try:
                observer_join = _wait_observer(observer, observer_stop)
                observer_join["joined_after_runner_return"] = True
                observer_join["runner_return_unix_ns"] = time.time_ns()
            except Exception as exc:
                observer_join_error = f"{type(exc).__name__}: {exc}"
        observer_stdout_handle.close()
        observer_stderr_handle.close()
        if observer_join is not None and case_dir.is_dir():
            try:
                observer_analysis = disk_observer.analyze_trial(case_dir, observer_dir)
                measurement = ((result or {}).get("cli_result") or {}).get("measurement")
                if not isinstance(measurement, dict):
                    raise CompareError("Scylla trial has no measurement window for host diskstats analysis")
                host_window = analyze_host_diskstats(
                    observer_dir, int(measurement["start_unix_ns"]), int(measurement["end_unix_ns"]))
                _validate_scylla_observer_analysis(observer_analysis, host_window, measurement)
                _write_json(case_dir / "host-io-observer-analysis.json", {
                    "valid": True, "observer_analysis": observer_analysis,
                    "host_diskstats_measured_window": host_window,
                    "collector_artifacts": _observer_artifact_references(observer_dir),
                })
            except Exception as exc:
                analysis_error = f"{type(exc).__name__}: {exc}"
                try:
                    _write_json(case_dir / "host-io-observer-analysis.json", {
                        "valid": False, "error": analysis_error,
                        "collector_artifacts": _observer_artifact_references(observer_dir),
                    })
                except Exception:
                    pass
    if result is None:
        result = {}
    if result.get("trial_id") not in (None, expected_trial_id):
        runner_error = runner_error or CompareError(
            f"Scylla runner trial ID {result.get('trial_id')!r} differs from expected {expected_trial_id!r}")
    case_dir = run_dir / "trials" / str(result.get("trial_id") or expected_trial_id)
    launch_rows = [row for row in (result.get("launch_config") or {}).values()
                   if isinstance(row, dict)]
    cap_verified = (len(launch_rows) == 1 and
                    launch_rows[0].get("host_io_limits_validation", {}).get("valid") is True)
    io_runtime = result.get("io_properties_runtime_validation")
    io_profile = result.get("io_properties_profile")
    io_profile_verified = (
        result.get("io_properties_enabled") is True
        and result.get("io_properties_sha256") == SCYLLA_IO_PROPERTIES_SHA256
        and isinstance(io_profile, dict)
        and io_profile.get("source_sha256") == SCYLLA_IO_PROPERTIES_SHA256
        and io_profile.get("copied_sha256") == SCYLLA_IO_PROPERTIES_SHA256
        and isinstance(io_runtime, dict) and io_runtime.get("valid") is True
        and result.get("validation", {}).get("io_properties_runtime") is True)
    case_record = {
        "campaign_order": case.order, "campaign_case_id": case.case_id,
        "backend": "scylla", "status": result.get("status"),
        "cleanup_status": result.get("cleanup_status"),
        "host_io_limits": limits, "binary_sha256": sha256_bytes(binary.read_bytes()),
        "cli_result": result.get("cli_result"),
        "measurement": (result.get("cli_result") or {}).get("measurement"),
        "source_trial_result": "runner-result.json",
        "host_io_limits_verified": cap_verified,
        "io_properties": {"source_file": SCYLLA_IO_PROPERTIES_RELATIVE_PATH,
                          "expected_sha256": SCYLLA_IO_PROPERTIES_SHA256,
                          "verified": io_profile_verified,
                          "runner_profile": io_profile,
                          "runtime_validation": io_runtime},
        "host_diskstats_observer": {
            "valid": (observer_join is not None and observer_analysis is not None
                      and host_window is not None and analysis_error is None),
            "process_pid": observer.pid if observer is not None else None,
            "first_sample": observer_first_sample,
            "join": observer_join,
            "join_error": observer_join_error,
            "analysis_error": analysis_error,
            "analysis_file": str(case_dir / "host-io-observer-analysis.json"),
            "collector_artifacts": _observer_artifact_references(observer_dir),
            "observer_analysis_valid": (observer_analysis or {}).get("valid") is True,
            "host_window_valid": (host_window or {}).get("valid") is True,
            "whole_device": ((observer_analysis or {}).get("diskstats") or {}).get("whole_device"),
            "data_partition": ((observer_analysis or {}).get("diskstats") or {}).get("data_partition"),
            "sampling_max_gap_seconds": (((observer_analysis or {}).get("measurement_window") or {})
                                           .get("observer_max_gap_seconds")),
        },
    }
    if case_dir.is_dir():
        _write_json(case_dir / "wallet-db-compare-case.json", case_record)
    else:
        _write_json(observer_dir / "wallet-db-compare-case.json", case_record)
    if runner_error is not None:
        raise CompareError(f"Scylla case {case.case_id} runner failed: {runner_error}") from runner_error
    if observer_join_error is not None:
        raise CompareError(f"Scylla case {case.case_id} diskstats observer could not stop cleanly: {observer_join_error}")
    if analysis_error is not None:
        raise CompareError(f"Scylla case {case.case_id} host I/O evidence failed: {analysis_error}")
    if not io_profile_verified:
        raise CompareError(f"Scylla case {case.case_id} did not validate the frozen I/O properties profile")
    if result.get("status") != "passed" or result.get("cleanup_status") != "clean":
        raise CompareError(f"Scylla case {case.case_id} is not fully valid/clean: "
                           f"status={result.get('status')}, cleanup={result.get('cleanup_status')}, "
                           f"error={result.get('error')}")
    if not cap_verified:
        raise CompareError(f"Scylla case {case.case_id} did not verify all shared host I/O caps")
    cli_result = result.get("cli_result")
    if not isinstance(cli_result, dict) or cli_result.get("valid") is not True:
        raise CompareError(f"Scylla case {case.case_id} lacks valid full-wallet CLI verification")
    measurement = cli_result.get("measurement")
    if (not isinstance(measurement, dict)
            or measurement.get("request_count") != TOTAL_REQUESTS
            or measurement.get("accepted_requests") != TOTAL_REQUESTS):
        raise CompareError(f"Scylla case {case.case_id} did not complete all {TOTAL_REQUESTS} requests")
    return {"order": case.order, "case_id": case.case_id, "backend": "scylla",
            "repeat": case.repeat, "status": result["status"],
            "cleanup_status": result["cleanup_status"],
            "rps": measurement.get("rps"), "measurement": measurement,
            "source_trial_id": result.get("trial_id"),
            "artifact": str(case_dir / "runner-result.json"),
            "host_io_limits_verified": cap_verified,
            "io_properties_verified": io_profile_verified,
            "host_diskstats_valid": True,
            "host_diskstats_analysis": str(case_dir / "host-io-observer-analysis.json")}


def _observer_artifact_references(observer_dir: pathlib.Path) -> dict[str, str | None]:
    return {name: str(observer_dir / filename) if (observer_dir / filename).is_file() else None
            for name, filename in (("collector_result", "collector-result.json"),
                                   ("device_profile", "device-profile.json"),
                                   ("samples", "samples.jsonl"))}


def _validate_scylla_observer_analysis(analysis: dict[str, Any],
                                      host_window: dict[str, Any],
                                      measurement: dict[str, Any]) -> None:
    if analysis.get("valid") is not True:
        raise CompareError("Scylla host diskstats observer analysis is invalid")
    if ((analysis.get("profile_validation") or {}).get("valid") is not True
            or (analysis.get("disk_identity_validation") or {}).get("valid") is not True
            or (analysis.get("scylla_queue_metrics") or {}).get("valid") is not True):
        raise CompareError("Scylla observer workload, data-device, or queue-metric validation is invalid")
    if host_window.get("valid") is not True:
        raise CompareError("Scylla raw host diskstats window is invalid")
    window = analysis.get("measurement_window")
    if not isinstance(window, dict) or window.get("diskstats_sample_count", 0) < 2:
        raise CompareError("Scylla diskstats observer needs at least two in-window samples")
    max_gap = window.get("observer_max_gap_seconds")
    if (isinstance(max_gap, bool) or not isinstance(max_gap, (int, float))
            or max_gap > disk_observer.MAX_SAMPLE_GAP_SECONDS):
        raise CompareError("Scylla host diskstats observer sampling gap exceeds the approved maximum")
    if (window.get("start_unix_ns") != measurement.get("start_unix_ns")
            or window.get("end_unix_ns") != measurement.get("end_unix_ns")):
        raise CompareError("Scylla observer and wallet measurement boundaries differ")
    views = ((analysis.get("diskstats") or {}).get("whole_device"),
             (analysis.get("diskstats") or {}).get("data_partition"))
    expected = (("8:16", "sdb"), ("8:18", "sdb2"))
    for view, (major_minor, name) in zip(views, expected):
        if (not isinstance(view, dict)
                or (view.get("device") or {}).get("major_minor") != major_minor
                or (view.get("device") or {}).get("name") != name
                or view.get("sample_count_in_window", 0) < 2
                or not isinstance(view.get("delta"), dict)
                or set(view["delta"]) != set(disk_observer.COUNTER_FIELDS)):
            raise CompareError(f"Scylla observer is missing valid raw counters for {name} {major_minor}")
    host_deltas = host_window.get("deltas")
    if (not isinstance(host_deltas, dict)
            or host_deltas.get("whole_device", {}).get("major_minor") != "8:16"
            or host_deltas.get("data_device", {}).get("major_minor") != "8:18"
            or set(host_deltas.get("whole_device", {}).get("counters", {}))
            != set(disk_observer.COUNTER_FIELDS)
            or set(host_deltas.get("data_device", {}).get("counters", {}))
            != set(disk_observer.COUNTER_FIELDS)):
        raise CompareError("Scylla PG-equivalent raw host diskstats window is incomplete")


def run_campaign(plan: dict[str, Any], binary: pathlib.Path,
                 limits: dict[str, Any], plan_file: pathlib.Path) -> dict[str, Any]:
    contract_sha = plan["contract_sha256"]
    runs_root = DEFAULT_OUTPUT_ROOT
    runs_root.mkdir(parents=True, exist_ok=True)
    run_id = f"run-{_utc_stamp()}-{uuid.uuid4().hex[:10]}"
    run_dir = runs_root / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    (run_dir / "qualification").mkdir()
    approved_plan = run_dir / "approved-plan.json"
    shutil.copy2(plan_file, approved_plan)
    caps_path = pathlib.Path(str(limits.get("source_json", DEFAULT_CAPS_PATH)))
    caps_target = run_dir / "qualification" / "io_caps.json"
    shutil.copy2(caps_path, caps_target)
    if sha256_bytes(caps_target.read_bytes()) != plan["contract"]["input_hashes"]["host_io_caps_sha256"]:
        raise CompareError("run-local host I/O cap copy differs from the approved contract")
    run_manifest = {
        "run_id": run_id, "status": "running", "started_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "approved_contract_sha256": contract_sha,
        "plan_id": plan["artifact"]["plan_id"], "approved_plan_file": str(approved_plan),
        "approved_plan_sha256": sha256_bytes(approved_plan.read_bytes()),
        "binary_sha256": sha256_bytes(binary.read_bytes()),
        "source_hashes": plan["contract"]["input_hashes"]["comparison_sources"],
        "postgres_sql_sha256": POSTGRES_SCHEMA_SQL_SHA256,
        "scylla_io_properties_file": SCYLLA_IO_PROPERTIES_RELATIVE_PATH,
        "scylla_io_properties_sha256": SCYLLA_IO_PROPERTIES_SHA256,
        "host_io_caps": {key: limits[key] for key in
                         ("device_path", "major_minor", "read_bps", "write_bps", "read_iops", "write_iops")},
        "host_io_caps_sha256": plan["contract"]["input_hashes"]["host_io_caps_sha256"],
        "one_database_at_a_time": True,
    }
    _write_json(run_dir / "run-manifest.json", run_manifest)
    campaign = {"run_id": run_id, "approved_contract_sha256": contract_sha,
                "status": "running", "cases": [], "completed_case_count": 0,
                "planned_case_count": 9, "automatic_retries": 0,
                "stop_on_first_invalid_case": True}
    _write_json(run_dir / "campaign.json", campaign)
    failure: str | None = None
    try:
        preflight = wallet_runner.strict_formal_preflight(
            run_dir / "strict-preflight.json", MIN_DISK_BYTES,
            PREFLIGHT_SECONDS, data_path=run_dir)
        campaign["preflight"] = _verify_preflight_artifact(preflight)
        _write_json(run_dir / "campaign.json", campaign)
        pg_image = _check_pg_image(run_dir)
        scylla_image = wallet_runner.verify_image_is_pulled()
        if not scylla_image.get("id"):
            raise CompareError("pinned Scylla image inspection returned no image ID")
        campaign["image_preflight"] = {"postgres": pg_image, "scylla": scylla_image}
        affinity = wallet_runner.apply_runner_affinity()
        campaign["runner_affinity"] = affinity
        _write_json(run_dir / "campaign.json", campaign)

        calibration_cases = campaign_cases()[:len(PG_CALIBRATION_CONNECTIONS)]
        calibration: list[dict[str, Any]] = []
        for case in calibration_cases:
            item = run_postgres_case(run_id, run_dir, case, binary, limits)
            calibration.append({"pg_connections": case.pg_connections,
                                "valid": item.get("status") == "passed"
                                and item.get("cleanup_status") == "passed",
                                "rps": item.get("rps"),
                                "requests": ((item.get("cli_result") or {}).get("requests") or {}).get("accepted"),
                                "full_verification": ((item.get("cli_result") or {}).get("verification") or {}).get("valid"),
                                "cleanup_status": item.get("cleanup_status"),
                                "case_id": case.case_id,
                                "artifact": str(run_dir / "postgres-cases" / case.case_id / "case-result.json")})
            campaign["cases"].append(item)
            campaign["completed_case_count"] += 1
            campaign["calibration"] = calibration
            _write_json(run_dir / "campaign.json", campaign)

        selection = select_postgres_connections(calibration)
        campaign["postgres_connection_selection"] = selection
        _write_json(run_dir / "campaign.json", campaign)
        full_cases = campaign_cases(selection["selected_pg_connections"])[
            len(PG_CALIBRATION_CONNECTIONS):]
        for case in full_cases:
            if case.backend == "postgres":
                item = run_postgres_case(run_id, run_dir, case, binary, limits)
            else:
                item = run_scylla_case(run_id, run_dir, case, binary, limits)
            campaign["cases"].append(item)
            campaign["completed_case_count"] += 1
            _write_json(run_dir / "campaign.json", campaign)
        if campaign["completed_case_count"] != 9:
            raise CompareError("campaign did not complete exactly three calibration and six comparison cases")
        final_cases = campaign["cases"][3:]
        scylla_rps = [float(row["rps"]) for row in final_cases if row.get("backend") == "scylla"]
        postgres_rps = [float(row["rps"]) for row in final_cases if row.get("backend") == "postgres"]
        if len(scylla_rps) != 3 or len(postgres_rps) != 3:
            raise CompareError("campaign comparison phase has an incomplete 3+3 repeat set")
        campaign["comparison_summary"] = {
            "status": "valid_all_six_cases",
            "scylla_rps_each": scylla_rps,
            "postgres_rps_each": postgres_rps,
            "scylla_median_rps": sorted(scylla_rps)[1],
            "postgres_median_rps": sorted(postgres_rps)[1],
            "postgres_to_scylla_median_rps_ratio": sorted(postgres_rps)[1] / sorted(scylla_rps)[1],
            "performance_acceptance_threshold": None,
            "claim_scope": "full verified request window; post-window checkpoint excluded; no steady-state claim",
        }
        campaign["status"] = "complete_valid"
    except BaseException as exc:
        failure = f"{type(exc).__name__}: {exc}"
        campaign["status"] = "stopped_on_first_failure"
        campaign["failure"] = failure
        campaign["failure_unix_ns"] = time.time_ns()
    campaign["finished_utc"] = dt.datetime.now(dt.timezone.utc).isoformat()
    run_manifest["status"] = campaign["status"]
    run_manifest["finished_utc"] = campaign["finished_utc"]
    if failure:
        run_manifest["failure"] = failure
    _write_json(run_dir / "campaign.json", campaign)
    _write_json(run_dir / "run-manifest.json", run_manifest)
    if failure:
        raise CompareError(f"campaign stopped; evidence preserved at {run_dir}: {failure}")
    return {"run_dir": str(run_dir), "status": campaign["status"],
            "contract_sha256": contract_sha,
            "postgres_connection_selection": campaign["postgres_connection_selection"],
            "comparison_summary": campaign["comparison_summary"],
            "case_count": campaign["completed_case_count"]}


def create_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Freeze, review, and run the equal-resource synchronous Scylla/PostgreSQL wallet comparison.")
    actions = parser.add_mutually_exclusive_group(required=True)
    actions.add_argument("--dry-run", action="store_true",
                         help="print the immutable launch contract preview; start no containers")
    actions.add_argument("--plan", action="store_true",
                         help="freeze source and binary snapshots and emit a reviewable contract hash")
    actions.add_argument("--execute", type=pathlib.Path,
                         help="run only the contract in this plan file, after exact-hash approval")
    parser.add_argument("--approved-contract-sha256",
                        help="required for --execute; exact reviewed plan contract SHA-256")
    parser.add_argument("--binary", type=pathlib.Path,
                        help="release CLI path for --dry-run or --plan; defaults to the finalized release build")
    parser.add_argument("--caps-json", type=pathlib.Path,
                        help="shared I/O caps JSON for --dry-run or --plan")
    return parser


def _preview_contract(binary: pathlib.Path, caps_path: pathlib.Path) -> dict[str, Any]:
    caps_sha = sha256_bytes(caps_path.read_bytes())
    if caps_sha != "c95f1a8ba5204051fe48e61bd05333ef10122f1b9e0ad78c8a41ad1bbf9b87fc":
        raise CompareError(f"host I/O cap file hash differs from qualified bytes: {caps_sha}")
    limits = load_host_io_limits(caps_path)
    if limits is None:
        raise CompareError("shared host I/O caps are required")
    sql_sha = sha256_bytes(SQL_PATH.read_bytes())
    if sql_sha != POSTGRES_SCHEMA_SQL_SHA256:
        raise CompareError("PostgreSQL SQL source does not match the reviewed SQL hash")
    binary_sha = sha256_bytes(binary.read_bytes())
    if binary_sha != EXPECTED_RELEASE_BINARY_SHA256:
        raise CompareError("release CLI binary does not match the finalized Rust build hash")
    contract = _contract_body(comparison_source_hashes(), binary_sha, caps_sha, limits)
    return {"contract": contract, "contract_sha256": sha256_bytes(canonical_json_bytes(contract)),
            "status": "preview_only_no_containers_started"}


def main(argv: Sequence[str] | None = None) -> int:
    parser = create_argument_parser()
    args = parser.parse_args(argv)
    if args.approved_contract_sha256 and not args.execute:
        parser.error("--approved-contract-sha256 is only valid with --execute")
    if args.execute and not args.approved_contract_sha256:
        parser.error("--execute requires --approved-contract-sha256")
    if args.execute and (args.binary is not None or args.caps_json is not None):
        parser.error("--execute uses the already-frozen binary and caps snapshots from its plan")
    try:
        if args.execute:
            plan, binary, limits = validate_frozen_plan(
                args.execute.expanduser().resolve(strict=True), args.approved_contract_sha256)
            output = run_campaign(plan, binary, limits,
                                  args.execute.expanduser().resolve(strict=True))
        else:
            binary = _binary_path(args.binary)
            caps_path = (args.caps_json or DEFAULT_CAPS_PATH).expanduser().resolve(strict=True)
            if args.dry_run:
                output = _preview_contract(binary, caps_path)
            else:
                plans_root = REPO_ROOT / "target" / "wallet-db-compare" / "plans"
                plans_root.mkdir(parents=True, exist_ok=True)
                plan_id = f"plan-{_utc_stamp()}-{uuid.uuid4().hex[:10]}"
                output = create_frozen_plan(plans_root / plan_id, binary, caps_path)
        print(json.dumps(output, indent=2, sort_keys=True, allow_nan=False))
        return 0
    except (CompareError, wallet_runner.RunnerError, disk_observer.ObserverError,
            OSError, subprocess.SubprocessError, ValueError) as exc:
        print(json.dumps({"status": "failed", "error": f"{type(exc).__name__}: {exc}"},
                         indent=2, sort_keys=True), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
