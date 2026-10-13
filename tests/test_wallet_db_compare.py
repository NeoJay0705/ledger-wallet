from __future__ import annotations

import copy
import errno
import hashlib
import json
import pathlib
import tempfile
import threading
import time
import unittest
from unittest import mock

from benches import wallet_db_compare as compare


def pg_settings_fixture() -> dict[str, dict[str, object]]:
    return {
        "shared_buffers": {"setting": "262144", "unit": "8kB", "source": "configuration file", "pending_restart": False},
        "work_mem": {"setting": "4096", "unit": "kB", "source": "configuration file", "pending_restart": False},
        "maintenance_work_mem": {"setting": "262144", "unit": "kB", "source": "configuration file", "pending_restart": False},
        "autovacuum_work_mem": {"setting": "65536", "unit": "kB", "source": "configuration file", "pending_restart": False},
        "max_connections": {"setting": "100", "unit": None, "source": "configuration file", "pending_restart": False},
        "fsync": {"setting": "on", "unit": None, "source": "configuration file", "pending_restart": False},
        "synchronous_commit": {"setting": "on", "unit": None, "source": "configuration file", "pending_restart": False},
        "full_page_writes": {"setting": "on", "unit": None, "source": "configuration file", "pending_restart": False},
        "wal_sync_method": {"setting": "fdatasync", "unit": None, "source": "configuration file", "pending_restart": False},
        "commit_delay": {"setting": "0", "unit": "us", "source": "configuration file", "pending_restart": False},
        "wal_level": {"setting": "replica", "unit": None, "source": "configuration file", "pending_restart": False},
        "checkpoint_timeout": {"setting": "5", "unit": "min", "source": "configuration file", "pending_restart": False},
        "max_wal_size": {"setting": "1048576", "unit": "kB", "source": "configuration file", "pending_restart": False},
        "autovacuum": {"setting": "on", "unit": None, "source": "configuration file", "pending_restart": False},
        "track_io_timing": {"setting": "on", "unit": None, "source": "configuration file", "pending_restart": False},
        "track_wal_io_timing": {"setting": "on", "unit": None, "source": "configuration file", "pending_restart": False},
    }


def pg_stats_fixture(pids: list[int]) -> dict[str, object]:
    return {
        "wal": {"wal_bytes": 10, "wal_fpi": 2, "wal_records": 3, "wal_buffers_full": 0,
                "stats_reset": "2026-01-01T00:00:00+00:00"},
        "database": {"xact_commit": 5, "xact_rollback": 0, "tup_inserted": 2,
                     "tup_updated": 3, "deadlocks": 0, "stats_reset": "2026-01-01T00:00:00+00:00"},
        "checkpointer": {"num_timed": 0, "num_requested": 0, "num_done": 0,
                         "stats_reset": "2026-01-01T00:00:00+00:00"},
        "bgwriter": {"buffers_written": 0, "buffers_clean": 0, "buffers_alloc": 0,
                     "stats_reset": "2026-01-01T00:00:00+00:00"},
        "io": [{"backend_type": "checkpointer", "object": "wal", "context": "normal",
                "writes": 2, "write_time": 0.2, "fsyncs": 1, "fsync_time": 0.1,
                "stats_reset": "2026-01-01T00:00:00+00:00"}],
        "activity": {"connections": len(pids), "service_backend_pids": pids, "waits": []},
    }


def valid_pg_ready_event(schema: str, connections: int = 16) -> dict[str, object]:
    return {
        "phase": "ready_to_measure", "timestamp_unix_ns": 100, "elapsed_ns": 10,
        "session_id": f"postgres:{schema}:" + ",".join(str(pid) for pid in range(100, 100 + connections)),
        "backend": "postgres", "wallet_profile": "postgres-memory-tx-v1", "mode": "pilot",
        "C": compare.WORKERS_C, "database_name": compare.PG_DATABASE, "pg_schema": schema,
        "pg_connections": connections, "db_cpu_count": 2,
        "schema_sql_file": "benchmarks/scylla-wallet/sql/postgres_wallet.sql",
        "schema_sql_sha256": compare.POSTGRES_SCHEMA_SQL_SHA256,
        "application_name": compare.PG_SERVICE_APPNAME,
        "ledger_key": ["account_id", "tx_id"], "accounts_table": "accounts",
        "accounts_primary_key_constraint": "accounts_pkey", "ledger_table": "ledger",
        "ledger_primary_key_constraint": "ledger_pkey", "ledger_primary_key": ["account_id", "tx_id"],
        "sql_function_calls_per_transaction": 1, "client_query_timeout_ms": 30000,
        "account_state_source": "memory", "data_connection_target": connections,
        "worker_routing": "round-robin", "setup_concurrency": 6,
        "state_preload_concurrency": 6, "per_worker_queue_capacity": compare.QUEUE_CAPACITY,
        "global_admission_capacity": compare.QUEUE_CAPACITY, "users": compare.USERS,
        "requests_per_user": compare.REQUESTS_PER_USER, "total_requests": compare.TOTAL_REQUESTS,
        "pool_ready_connections": connections, "account_states_preloaded": compare.USERS,
        "all_clients_ready": True, "postgres_backend_pids": list(range(100, 100 + connections)),
    }


def valid_pg_events(schema: str, connections: int = 16) -> list[dict[str, object]]:
    events: list[dict[str, object]] = []
    phases = ["startup", "seeding", "seeding", "preloading", "preloading",
              "ready_to_measure", "measuring", "client_end", "drained",
              "verifying", "verified", "awaiting_runner_audit", "runner_audit_released"]
    for index, phase in enumerate(phases):
        event = valid_pg_ready_event(schema, connections)
        event.update({"phase": phase, "timestamp_unix_ns": 100 + index,
                      "elapsed_ns": index * 10,
                      "account_state_source": "memory" if phase in {
                          "ready_to_measure", "measuring", "client_end", "drained", "verifying",
                          "verified", "awaiting_runner_audit", "runner_audit_released"} else "db"})
        if phase == "seeding":
            event["status"] = "started" if len([row for row in events if row["phase"] == phase]) == 0 else "complete"
            if event["status"] == "complete":
                event["accounts_seeded"] = compare.USERS
        elif phase == "preloading":
            event["status"] = "started" if len([row for row in events if row["phase"] == phase]) == 0 else "complete"
            if event["status"] == "complete":
                event["account_states_preloaded"] = compare.USERS
        elif phase == "ready_to_measure":
            event["start_gate"] = "/tmp/start.gate"
        elif phase == "measuring":
            event["measurement_start_unix_ns"] = 10000
        elif phase == "client_end":
            event.update({"started": True, "measurement_start_unix_ns": 10000,
                          "measurement_end_unix_ns": 20000, "replies_received": compare.TOTAL_REQUESTS})
        elif phase == "verified":
            event.update({"valid": True, "accounts_verified": compare.USERS, "failures": 0})
        elif phase == "drained":
            event["service_failed"] = False
        events.append(event)
    return events


def valid_pg_result(schema: str, connections: int = 16) -> dict[str, object]:
    pids = list(range(100, 100 + connections))
    query_accounting = {"backend": "postgres", "account_state_source": "memory",
                        "measured_select_requests": 0,
                        "measured_sql_transaction_count": compare.TOTAL_REQUESTS,
                        "accepted_requests": compare.TOTAL_REQUESTS,
                        "driver_query_delta": compare.TOTAL_REQUESTS,
                        "internal_write_operations": 2 * compare.TOTAL_REQUESTS}
    stage = {"count": compare.TOTAL_REQUESTS, "min_ns": 1, "mean_ns": 2,
             "p50_ns": 2, "p95_ns": 3, "p99_ns": 3, "p999_ns": 3,
             "max_ns": 3, "overflow_count": 0}
    return {
        "schema_version": 1, "backend": "postgres", "valid": True, "status": "verified",
        "wallet_profile": "postgres-memory-tx-v1", "mode": "pilot",
        "config": {"backend": "postgres", "wallet_profile": "postgres-memory-tx-v1",
                   "database_name": compare.PG_DATABASE, "pg_schema": schema,
                   "pg_connections": connections, "C": compare.WORKERS_C,
                   "db_cpu_count": 2, "data_connection_target": connections,
                   "worker_routing": "round-robin", "users": compare.USERS,
                   "requests_per_user": compare.REQUESTS_PER_USER,
                   "total_requests_expected": compare.TOTAL_REQUESTS,
                   "per_worker_queue_capacity": compare.QUEUE_CAPACITY,
                   "queue_capacity": compare.QUEUE_CAPACITY,
                   "global_admission_capacity": compare.QUEUE_CAPACITY,
                   "setup_concurrency": 6, "state_preload_concurrency": 6,
                   "account_states_preloaded": compare.USERS, "account_state_source": "memory"},
        "postgres": {"database_name": compare.PG_DATABASE, "schema_name": schema,
                     "schema_sql_file": "benchmarks/scylla-wallet/sql/postgres_wallet.sql",
                     "schema_sql_sha256": compare.POSTGRES_SCHEMA_SQL_SHA256,
                     "function_name": "wallet_commit", "application_name": compare.PG_SERVICE_APPNAME,
                     "accounts_table": "accounts", "accounts_primary_key_constraint": "accounts_pkey",
                     "ledger_table": "ledger", "ledger_primary_key_constraint": "ledger_pkey",
                     "ledger_primary_key": ["account_id", "tx_id"],
                     "client_query_timeout_ms": 30000,
                     "pool_wait_excluded_from_client_timeout": True,
                     "implicit_transaction_ack": "query_one completion after PostgreSQL ReadyForQuery",
                     "connection_settings": [{"backend_pid": pid,
                         "application_name": compare.PG_SERVICE_APPNAME,
                         "synchronous_commit": "on", "fsync": "on", "full_page_writes": "on",
                         "commit_delay": "0", "is_standby": False,
                         "synchronous_standby_names": ""} for pid in pids]},
        "connection_pool": {"backend": "postgres", "database_name": compare.PG_DATABASE,
            "pg_schema": schema, "pg_connections": connections,
            "target_total_data_connections": connections, "C": compare.WORKERS_C,
            "db_cpu_count": 2, "setup_concurrency": 6,
            "pool_ready_connections": connections, "pool_after_last_reply": connections,
            "samples_during_measurement": 20, "minimum_sampled_connections": connections,
            "maximum_sampled_connections": connections, "mismatch_samples": 0,
            "sampling_period_ns": 100000000, "metric_source": "tokio-postgres persistent connection pool liveness",
            "valid": True, "formal_sample_required": False},
        "requests": {"expected": compare.TOTAL_REQUESTS, "attempted": compare.TOTAL_REQUESTS,
            "accepted": compare.TOTAL_REQUESTS, "replies_received": compare.TOTAL_REQUESTS,
            "applied": compare.TOTAL_REQUESTS, "errors": 0, "unexpected_business_results": 0,
            "operation_counts": {"credit": compare.TOTAL_REQUESTS // 2,
                                 "debit": compare.TOTAL_REQUESTS // 2},
            "outcome_counts": {"applied": compare.TOTAL_REQUESTS, "insufficient_funds": 0,
                               "credit_overflow": 0, "invalid_amount": 0},
            "query_accounting": query_accounting},
        "driver_counters": {"delta": {"queries": compare.TOTAL_REQUESTS, "errors": 0,
            "retries": 0, "connection_timeouts": 0, "request_timeouts": 0}},
        "postgres_accounting": {"function_calls": compare.TOTAL_REQUESTS,
            "measured_sql_transaction_count": compare.TOTAL_REQUESTS,
            "sql_transactions_per_request": 1, "ledger_insert_attempts": compare.TOTAL_REQUESTS,
            "account_updates": compare.TOTAL_REQUESTS,
            "internal_write_operations": 2 * compare.TOTAL_REQUESTS,
            "duplicate_conflicts": 0, "accepted_requests": compare.TOTAL_REQUESTS,
            "driver_query_delta": compare.TOTAL_REQUESTS,
            "driver_query_calls_per_request": 1,
            "query_completion": "ReadyForQuery after implicit transaction completion"},
        "query_accounting": query_accounting,
        "verification": {"complete": True, "valid": True,
            "account_state_source": "memory_cache_vs_postgres", "accounts_expected": compare.USERS,
            "memory_cache_accounts_compared": compare.USERS, "accounts_verified": compare.USERS,
            "ledger_rows_expected": compare.TOTAL_REQUESTS,
            "ledger_rows_seen": compare.TOTAL_REQUESTS, "failures": 0},
        "stage_metrics": {"stages": {"pool_wait": copy.deepcopy(stage),
                                       "sql_transaction": copy.deepcopy(stage)},
            "counters": {"pg_function_calls": compare.TOTAL_REQUESTS,
                         "pg_ledger_insert_attempts": compare.TOTAL_REQUESTS,
                         "pg_account_updates": compare.TOTAL_REQUESTS,
                         "pg_duplicate_conflicts": 0,
                         "accepted_requests": compare.TOTAL_REQUESTS},
            "database_in_flight_current": 0, "database_in_flight_peak": 16,
            "workers_total": compare.WORKERS_C, "workers_busy_current": 0,
            "workers_busy_peak": 512, "workers_idle_current": 512,
            "workers_idle_peak": 512},
        "measurement": {"started": True, "request_count": compare.TOTAL_REQUESTS,
            "attempted_count": compare.TOTAL_REQUESTS, "accepted_requests": compare.TOTAL_REQUESTS,
            "start_unix_ns": 10000, "end_unix_ns": 20000,
            "start_offset_ns": 0, "end_offset_ns": 10000, "wall_duration_ns": 10000,
            "rps": compare.TOTAL_REQUESTS * 1000000000 / 10000},
        "end_to_end_latency": {"count": compare.TOTAL_REQUESTS, "overflow_count": 0,
                               "p50_ns": 2, "p95_ns": 3, "p99_ns": 3, "p999_ns": 3, "max_ns": 3},
        "errors": [],
    }


class CampaignContractTests(unittest.TestCase):
    def test_campaign_schedule_has_exact_repeat_order_and_calibration(self) -> None:
        calibration = compare.campaign_cases()
        self.assertEqual([(case.backend, case.pg_connections) for case in calibration],
                         [("postgres", 16), ("postgres", 32), ("postgres", 64)])
        cases = compare.campaign_cases(32)[3:]
        self.assertEqual([case.backend for case in cases],
                         ["scylla", "postgres", "postgres", "scylla", "scylla", "postgres"])
        self.assertEqual([case.repeat for case in cases], [1, 1, 2, 2, 3, 3])
        self.assertEqual([case.pg_connections for case in cases], [None, 32, 32, None, None, 32])

    def test_connection_selector_uses_smallest_within_95_percent(self) -> None:
        selected = compare.select_postgres_connections([
            {"pg_connections": 16, "valid": True, "rps": 900.0,
             "requests": compare.TOTAL_REQUESTS, "full_verification": True, "cleanup_status": "passed"},
            {"pg_connections": 32, "valid": True, "rps": 1000.0,
             "requests": compare.TOTAL_REQUESTS, "full_verification": True, "cleanup_status": "passed"},
            {"pg_connections": 64, "valid": True, "rps": 940.0,
             "requests": compare.TOTAL_REQUESTS, "full_verification": True, "cleanup_status": "passed"},
        ])
        self.assertEqual(selected["selected_pg_connections"], 32)
        with self.assertRaises(compare.CompareError):
            compare.select_postgres_connections([
                {"pg_connections": 16, "valid": True, "rps": 900.0,
                 "requests": compare.TOTAL_REQUESTS, "full_verification": True, "cleanup_status": "passed"},
                {"pg_connections": 32, "valid": False, "rps": 1000.0,
                 "requests": compare.TOTAL_REQUESTS, "full_verification": True, "cleanup_status": "passed"},
                {"pg_connections": 64, "valid": True, "rps": 940.0,
                 "requests": compare.TOTAL_REQUESTS, "full_verification": True, "cleanup_status": "passed"},
            ])

    def test_ready_event_binds_real_pool_and_schema_metadata(self) -> None:
        schema = "wallet_bench_test123"
        event = valid_pg_ready_event(schema)
        self.assertTrue(compare.validate_pg_ready_event(
            event, schema=schema, pg_connections=16,
            sql_sha256=compare.POSTGRES_SCHEMA_SQL_SHA256)["valid"])
        event["postgres_backend_pids"][-1] = event["postgres_backend_pids"][0]
        with self.assertRaisesRegex(compare.CompareError, "backend PIDs"):
            compare.validate_pg_ready_event(event, schema=schema, pg_connections=16,
                                            sql_sha256=compare.POSTGRES_SCHEMA_SQL_SHA256)

    def test_events_and_result_match_postgres_cli_lifecycle_contract(self) -> None:
        schema = "wallet_bench_fixture"
        events = valid_pg_events(schema)
        lifecycle = compare._expected_events(events, schema, 16,
                                              compare.POSTGRES_SCHEMA_SQL_SHA256)
        self.assertEqual(lifecycle["measurement_end_unix_ns"], 20000)
        report = compare.validate_pg_result(valid_pg_result(schema), pg_schema=schema,
                                            pg_connections=16,
                                            sql_sha256=compare.POSTGRES_SCHEMA_SQL_SHA256)
        self.assertEqual(report["rps"], compare.TOTAL_REQUESTS * 100000)

    def test_result_rejects_errors_duplicates_and_wrong_ack(self) -> None:
        schema = "wallet_bench_fixture"
        for mutate in (
            lambda value: value["requests"].update(errors=1),
            lambda value: value["driver_counters"]["delta"].update(retries=1),
            lambda value: value["verification"].update(ledger_rows_seen=compare.TOTAL_REQUESTS - 1),
            lambda value: value["postgres"].update(implicit_transaction_ack="request write completed"),
            lambda value: value["stage_metrics"].update(database_in_flight_peak=17),
            lambda value: value["stage_metrics"].update(database_in_flight_peak=0),
        ):
            result = valid_pg_result(schema)
            mutate(result)
            with self.assertRaises(compare.CompareError):
                compare.validate_pg_result(result, pg_schema=schema, pg_connections=16,
                                           sql_sha256=compare.POSTGRES_SCHEMA_SQL_SHA256)

    def test_result_rejects_dsn_and_backend_specific_fields(self) -> None:
        schema = "wallet_bench_fixture"
        result = valid_pg_result(schema)
        result["postgres"]["dsn"] = "postgresql://secret"
        with self.assertRaises(compare.CompareError):
            compare.validate_pg_result(result, pg_schema=schema, pg_connections=16,
                                       sql_sha256=compare.POSTGRES_SCHEMA_SQL_SHA256)
        result = valid_pg_result(schema)
        result["stage_metrics"]["cql_in_flight_peak"] = 0
        with self.assertRaises(compare.CompareError):
            compare.validate_pg_result(result, pg_schema=schema, pg_connections=16,
                                       sql_sha256=compare.POSTGRES_SCHEMA_SQL_SHA256)


class PostgresEvidenceTests(unittest.TestCase):
    def test_runtime_settings_are_exact_and_pending_restart_fails(self) -> None:
        valid = {"server_version": "18.6 (Debian 18.6-1.pgdg12+2)", "server_version_num": 180006,
                 "in_recovery": False, "settings": pg_settings_fixture()}
        self.assertTrue(compare.validate_pg_runtime_settings(valid)["valid"])
        valid["settings"]["fsync"]["pending_restart"] = True
        with self.assertRaisesRegex(compare.CompareError, "pending_restart"):
            compare.validate_pg_runtime_settings(valid)

    def test_actual_pg18_settings_fixture_matches_exact_runtime_patch(self) -> None:
        fixture = pathlib.Path(__file__).parent / "fixtures" / "postgres18_settings_raw.json"
        observed = json.loads(fixture.read_text(encoding="utf-8"))
        validation = compare.validate_pg_runtime_settings(observed)
        self.assertEqual(validation["server_version"], "18.6 (Debian 18.6-1.pgdg12+2)")
        self.assertEqual(validation["server_version_num"], 180006)
        for wrong_version, wrong_num in (("18.5 (Debian)", 180005), ("18.7 (Debian)", 180007),
                                         ("18.6 (Debian)", 180005), ("19.0 (Debian)", 190000)):
            invalid = copy.deepcopy(observed)
            invalid["server_version"] = wrong_version
            invalid["server_version_num"] = wrong_num
            with self.assertRaisesRegex(compare.CompareError, "runtime version"):
                compare.validate_pg_runtime_settings(invalid)

    def test_stats_capability_and_async_raw_deltas(self) -> None:
        before = {"sampled_unix_ns": 100, "stats": pg_stats_fixture([101, 102])}
        after = copy.deepcopy(before)
        after["sampled_unix_ns"] = 200
        after["stats"]["wal"]["wal_bytes"] += 99
        after["stats"]["database"]["xact_commit"] += 2
        self.assertTrue(compare.validate_pg_stat_capabilities(before["stats"])["valid"])
        delta = compare.pg_stat_snapshot_delta(before, after)
        self.assertTrue(delta["valid"])
        self.assertEqual(delta["deltas"]["wal"]["counters"]["wal_bytes"], 99)
        del after["stats"]["wal"]["wal_fpi"]
        delta = compare.pg_stat_snapshot_delta(before, after)
        self.assertNotIn("wal_fpi", delta["deltas"]["wal"]["counters"])

    def test_actual_pg18_stats_fixture_has_required_wal_and_activity_capabilities(self) -> None:
        fixture = pathlib.Path(__file__).parent / "fixtures" / "postgres18_stats_raw.json"
        actual = json.loads(fixture.read_text(encoding="utf-8"))
        validated = compare.validate_pg_stat_capabilities(actual)
        self.assertTrue(validated["valid"])
        self.assertGreater(validated["pg_stat_io_wal_rows"], 0)
        self.assertEqual(actual["activity"]["connections"], 16)
        self.assertEqual(len(actual["activity"]["service_backend_pids"]), 16)

    def test_schema_catalog_matches_frozen_sql_function_and_constraints(self) -> None:
        sql_source = compare.SQL_PATH.read_text(encoding="utf-8")
        body = compare.source_function_body(sql_source)
        checks = {
            "accounts": ["CHECK (account_id >= 0)", "CHECK (octet_length(balance) = 8)",
                         "CHECK (version >= 0)", "CHECK (write_clock >= 0)"],
            "ledger": ["CHECK (account_id >= 0)", "CHECK (tx_id >= 0)",
                       "CHECK (operation IN (0, 1))", "CHECK (octet_length(amount) = 8)",
                       "CHECK (outcome BETWEEN 0 AND 3)",
                       "CHECK (octet_length(balance_after) = 8)",
                       "CHECK (version >= 0)", "CHECK (write_clock >= 0)"],
        }
        evidence = {"tables": [
            {"name": "accounts", "columns": [
                {"name": name, "type": kind, "not_null": True}
                for name, kind in compare.PG_EXPECTED_COLUMNS["accounts"]],
             "primary_key": ["account_id"], "primary_key_constraint": "accounts_pkey",
             "checks": [{"definition": value, "validated": True} for value in checks["accounts"]]},
            {"name": "ledger", "columns": [
                {"name": name, "type": kind, "not_null": True}
                for name, kind in compare.PG_EXPECTED_COLUMNS["ledger"]],
             "primary_key": ["account_id", "tx_id"], "primary_key_constraint": "ledger_pkey",
             "checks": [{"definition": value, "validated": True} for value in checks["ledger"]]},
        ], "indexes": [
            {"name": "accounts_pkey", "table": "accounts", "unique": True, "primary": True,
             "valid": True, "ready": True, "method": "btree", "columns": ["account_id"]},
            {"name": "ledger_pkey", "table": "ledger", "unique": True, "primary": True,
             "valid": True, "ready": True, "method": "btree", "columns": ["account_id", "tx_id"]},
        ], "functions": [{
            "name": "wallet_commit", "language": "plpgsql", "security_definer": False,
            "strict": False,
            "identity_arguments": "p_account_id bigint, p_tx_id bigint, p_operation smallint, p_amount bytea, p_outcome smallint, p_balance_after bytea, p_version bigint, p_write_clock bigint",
            "result": "TABLE(did_insert boolean, current_balance bytea, current_version bigint, current_write_clock bigint, record_operation smallint, record_amount bytea, record_outcome smallint, record_balance_after bytea, record_version bigint, record_write_clock bigint)",
            "source_body": body,
            "source_body_sha256": hashlib.sha256(body.encode()).hexdigest(),
            "definition": "CREATE FUNCTION wallet_commit() RETURNS TABLE ...",
        }]}
        self.assertTrue(compare.validate_pg_schema_evidence(evidence, sql_source)["valid"])
        evidence["tables"][1]["primary_key"] = ["account_id"]
        with self.assertRaisesRegex(compare.CompareError, "primary key"):
            compare.validate_pg_schema_evidence(evidence, sql_source)

    def test_actual_pg18_catalog_fixture_accepts_only_observed_deparser_equivalences(self) -> None:
        fixture = pathlib.Path(__file__).parent / "fixtures" / "postgres18_schema_catalog_raw.json"
        evidence = json.loads(fixture.read_text(encoding="utf-8"))
        function = evidence["functions"][0]
        normalized_body = compare.normalize_sql_body(function["source_body"])
        function["source_body_sha256"] = hashlib.sha256(normalized_body.encode()).hexdigest()
        ledger_checks = {row["definition"] for row in evidence["tables"]
                         if row["name"] == "ledger" for row in row["checks"]}
        self.assertIn("CHECK ((operation = ANY (ARRAY[0, 1])))", ledger_checks)
        self.assertIn("CHECK (((outcome >= 0) AND (outcome <= 3)))", ledger_checks)
        self.assertTrue(compare.validate_pg_schema_evidence(
            evidence, compare.SQL_PATH.read_text(encoding="utf-8"))["valid"])

        for replacement in (
            ("operation = ANY (ARRAY[0, 1])", "operation = ANY (ARRAY[0, 2])"),
            ("operation = ANY (ARRAY[0, 1])", "operation = ANY (ARRAY[0, 1, 2])"),
            ("outcome <= 3", "outcome <= 4"),
        ):
            invalid = copy.deepcopy(evidence)
            checks = next(row["checks"] for row in invalid["tables"] if row["name"] == "ledger")
            target = next(row for row in checks if replacement[0] in row["definition"])
            target["definition"] = target["definition"].replace(*replacement)
            with self.assertRaisesRegex(compare.CompareError, "check constraints"):
                compare.validate_pg_schema_evidence(invalid,
                                                    compare.SQL_PATH.read_text(encoding="utf-8"))

        invalid = copy.deepcopy(evidence)
        operation_check = next(row for row in next(t["checks"] for t in invalid["tables"]
                                                    if t["name"] == "ledger")
                               if "operation = ANY" in row["definition"])
        operation_check["validated"] = False
        with self.assertRaisesRegex(compare.CompareError, "unvalidated"):
            compare.validate_pg_schema_evidence(invalid,
                                                compare.SQL_PATH.read_text(encoding="utf-8"))

    def test_actual_pg18_pss_fixture_and_same_uid_command_are_validated(self) -> None:
        fixture = pathlib.Path(__file__).parent / "fixtures" / "postgres18_pss_direct_postgres.json"
        observed = json.loads(fixture.read_text(encoding="utf-8"))
        self.assertEqual(observed["user"], "postgres")
        self.assertEqual(observed["returncode"], 0)
        with tempfile.TemporaryDirectory() as temp_dir:
            with mock.patch.object(compare.subprocess, "run",
                                    return_value=mock.Mock(returncode=0, stdout=observed["stdout"], stderr="")) as run:
                result = compare._pg_process_pss(pathlib.Path(temp_dir), "owned-container")
            argv = run.call_args.args[0]
            self.assertEqual(argv[1:4], ["exec", "--user", "postgres"])
            self.assertNotIn("--cap-add", argv)
        self.assertEqual(result["postgres_process_count"], 25)
        self.assertEqual(result["pss_kb"], 128395)
        self.assertEqual(result["unreadable_process_count"], 0)

        unreadable = json.loads(observed["stdout"])
        unreadable["unreadable_process_count"] = 1
        with tempfile.TemporaryDirectory() as temp_dir:
            with mock.patch.object(compare.subprocess, "run",
                                    return_value=mock.Mock(returncode=0,
                                                           stdout=json.dumps(unreadable), stderr="")):
                with self.assertRaisesRegex(compare.CompareError, "could not read PSS"):
                    compare._pg_process_pss(pathlib.Path(temp_dir), "owned-container")

    def test_exact_shared_host_io_caps_and_cgroup_readback(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            fake_root = pathlib.Path(temp_dir) / "repo"
            caps_path = (fake_root / "benchmarks" / "scylla-wallet" / "profiles" /
                         "equal-resource-20261013" / "io_caps.json")
            caps_path.parent.mkdir(parents=True)
            caps_path.write_bytes(compare.DEFAULT_CAPS_PATH.read_bytes())
            document = json.loads(caps_path.read_text(encoding="utf-8"))
            limits = compare.wallet_runner.validate_host_io_limits_document(document)
            self.assertEqual(limits["major_minor"], "8:16")
            cgroup = "8:16 rbps=403631184 wbps=152226096 riops=24497 wiops=23500\n"
            self.assertEqual(compare.wallet_runner.parse_cgroup_io_max(cgroup, limits)["wiops"], 23500)
            with self.assertRaises(compare.wallet_runner.RunnerError):
                compare.wallet_runner.parse_cgroup_io_max(
                    cgroup.replace("wiops=23500", "wiops=max"), limits)

    def test_measured_cgroup_io_requires_the_capped_device_counters(self) -> None:
        window = {"valid": True, "cgroup": {"io_stat_deltas": {
            "8:16": {"status": "valid", "counters": {"rbytes": 0, "wbytes": 4096,
                                                            "rios": 0, "wios": 1}}}}}
        self.assertTrue(compare.validate_pg_cgroup_io_window(window, label="fixture")["valid"])
        with self.assertRaisesRegex(compare.CompareError, "capped-device"):
            compare.validate_pg_cgroup_io_window({"valid": True, "cgroup": {"io_stat_deltas": {}}},
                                                 label="fixture")

    def test_cleanup_discovery_only_treats_exact_not_found_as_absent(self) -> None:
        case = compare.CompareCase(1, "postgres", 1, 16)
        pg = compare.OwnedPostgresCase(pathlib.Path("/tmp/run"), pathlib.Path("/tmp/case"),
                                       "run-id", case, {"device_path": "/dev/sdb"})
        missing = mock.Mock(returncode=1, stdout="", stderr=f"Error: No such object: {pg.container_name}")
        with mock.patch.object(compare, "run_recorded_command", return_value=missing):
            self.assertIsNone(pg._inspect_named_container_optional())
        daemon_error = mock.Mock(returncode=1, stdout="", stderr="Cannot connect to the Docker daemon")
        with mock.patch.object(compare, "run_recorded_command", return_value=daemon_error):
            with self.assertRaisesRegex(compare.CompareError, "could not inspect"):
                pg._inspect_named_container_optional()

    def test_owner_labels_must_match_exact_nonce_and_run(self) -> None:
        valid = {"Config": {"Labels": {"owner": "ledger-wallet-db-compare",
            "compare-nonce": "nonce", "compare-run-id": "run", "compare-case-id": "case"}}}
        compare._validate_owned_labels(valid, nonce="nonce", run_id="run", case_id="case",
                                       resource="container")
        valid["Config"]["Labels"]["compare-nonce"] = "other"
        with self.assertRaisesRegex(compare.CompareError, "ownership labels"):
            compare._validate_owned_labels(valid, nonce="nonce", run_id="run", case_id="case",
                                           resource="container")

    def test_process_command_redaction_keeps_dsn_out_of_evidence(self) -> None:
        dsn = "postgresql://postgres:secret@172.18.0.2:5432/walletbench"
        argv = ["binary", "--pg-dsn", dsn, "--pg-schema", "wallet_bench_x"]
        safe = compare._safe_argv(argv, (dsn,))
        self.assertEqual(safe, ["binary", "--pg-dsn", "<redacted>", "--pg-schema", "wallet_bench_x"])
        self.assertNotIn("secret", json.dumps(safe))

    def test_preflight_rejects_insufficient_disk_memory_or_busy_host(self) -> None:
        sample = {"disk_available_bytes": compare.MIN_DISK_BYTES,
                  "memory_available_bytes": compare.MIN_AVAILABLE_MEMORY_BYTES,
                  "host_cpu_busy_pct": 10.0, "device_busy_pct": 5.0}
        self.assertTrue(compare._verify_preflight_artifact(
            {"valid": True, "samples": [sample, sample, sample]})["valid"])
        bad = dict(sample, disk_available_bytes=compare.MIN_DISK_BYTES - 1)
        with self.assertRaises(compare.CompareError):
            compare._verify_preflight_artifact({"valid": True, "samples": [sample, sample, bad]})


class ScyllaCampaignEvidenceTests(unittest.TestCase):
    @staticmethod
    def _valid_observer_analysis(measurement: dict[str, object]) -> dict[str, object]:
        counters = {name: 0 for name in compare.disk_observer.COUNTER_FIELDS}
        return {
            "valid": True,
            "profile_validation": {"valid": True},
            "disk_identity_validation": {"valid": True},
            "scylla_queue_metrics": {"valid": True},
            "measurement_window": {
                "start_unix_ns": measurement["start_unix_ns"],
                "end_unix_ns": measurement["end_unix_ns"],
                "diskstats_sample_count": 4,
                "observer_max_gap_seconds": 1.0,
            },
            "diskstats": {
                "whole_device": {"device": {"major_minor": "8:16", "name": "sdb"},
                                 "sample_count_in_window": 4, "delta": dict(counters)},
                "data_partition": {"device": {"major_minor": "8:18", "name": "sdb2"},
                                   "sample_count_in_window": 4, "delta": dict(counters)},
            },
        }

    @staticmethod
    def _valid_host_window() -> dict[str, object]:
        counters = {name: 0 for name in compare.disk_observer.COUNTER_FIELDS}
        return {"valid": True, "deltas": {
            "whole_device": {"major_minor": "8:16", "counters": dict(counters)},
            "data_device": {"major_minor": "8:18", "counters": dict(counters)},
        }}

    def test_calibrated_io_profile_and_observer_are_bound_into_plan_inputs(self) -> None:
        self.assertEqual(compare.sha256_bytes(compare.SCYLLA_IO_PROPERTIES_PATH.read_bytes()),
                         compare.SCYLLA_IO_PROPERTIES_SHA256)
        self.assertEqual(compare.SCYLLA_IO_PROPERTIES_RELATIVE_PATH,
                         "benchmarks/scylla-wallet/profiles/equal-resource-20261013/io_properties.yaml")
        self.assertIn(compare.SCYLLA_IO_PROPERTIES_RELATIVE_PATH,
                      compare.comparison_source_hashes())
        sources = compare.comparison_source_hashes()
        for path in ("docs/01-63.raw-requirement-wallet-db-equal-resource-benchmark.md",
                     "docs/01-64.development-design-wallet-db-equal-resource-benchmark.md",
                     "docs/01-65.development-validation-wallet-db-equal-resource-benchmark.md",
                     "tests/test_scylla_wallet_runner.py", "tests/test_scylla_io_observer.py",
                     "tests/fixtures/postgres18_schema_catalog_raw.json",
                     "tests/fixtures/postgres18_settings_raw.json",
                     "tests/fixtures/postgres18_stats_raw.json",
                     "tests/fixtures/postgres18_pss_direct_postgres.json",
                     "tests/fixtures/scylla_periodic_missing_commitlog_series.json",
                     "tests/fixtures/README.md"):
            self.assertIn(path, sources)
        contract = compare._contract_body(sources, "b" * 64, "c" * 64,
                                          {key: value for key, value in json.loads(
                                              compare.DEFAULT_CAPS_PATH.read_text()).items()})
        scylla = contract["scylla"]
        self.assertEqual(scylla["io_properties_profile"]["source_sha256"],
                         compare.SCYLLA_IO_PROPERTIES_SHA256)
        self.assertTrue(scylla["io_properties_profile"]["runtime_validation_required"])
        self.assertEqual(scylla["host_diskstats_observer"]["required_measurement_views"],
                         ["whole_device", "data_partition"])

    def test_observer_analysis_requires_whole_partition_and_full_sampling_evidence(self) -> None:
        measurement = {"start_unix_ns": 10, "end_unix_ns": 20}
        analysis = self._valid_observer_analysis(measurement)
        host_window = self._valid_host_window()
        compare._validate_scylla_observer_analysis(analysis, host_window, measurement)
        analysis["diskstats"].pop("whole_device")
        with self.assertRaisesRegex(compare.CompareError, "raw counters"):
            compare._validate_scylla_observer_analysis(analysis, host_window, measurement)
        analysis = self._valid_observer_analysis(measurement)
        analysis["measurement_window"]["observer_max_gap_seconds"] = 3.0
        with self.assertRaisesRegex(compare.CompareError, "sampling gap"):
            compare._validate_scylla_observer_analysis(analysis, host_window, measurement)
        analysis = self._valid_observer_analysis(measurement)
        host_window["deltas"]["data_device"]["major_minor"] = "8:17"
        with self.assertRaisesRegex(compare.CompareError, "PG-equivalent"):
            compare._validate_scylla_observer_analysis(analysis, host_window, measurement)

    def test_scylla_trial_uses_calibrated_profile_and_joins_observer_after_cleanup(self) -> None:
        case = compare.CompareCase(4, "scylla", 2)
        run_id = "run-test"
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = pathlib.Path(tmp)
            binary = run_dir / "ledger-wallet-scylla-bench"
            binary.write_bytes(b"fixture binary")
            limits = {"device_path": "/dev/sdb", "major_minor": "8:16",
                      "read_bps": 403631184, "write_bps": 152226096,
                      "read_iops": 24497, "write_iops": 23500}
            measurement = {"start_unix_ns": 10, "end_unix_ns": 20,
                           "request_count": compare.TOTAL_REQUESTS,
                           "accepted_requests": compare.TOTAL_REQUESTS,
                           "rps": 1234.5}
            trial_case = compare.wallet_runner.TrialCase(
                shards=compare.SCYLLA_SHARDS, workers=compare.WORKERS_C,
                per_shard_pool=compare.SCYLLA_POOL_PER_SHARD, repeat=case.repeat,
                order=case.order, worker_routing="round-robin",
                per_shard_pool_overridden=True,
                requested_per_shard_pool=compare.SCYLLA_POOL_PER_SHARD,
                replication_factor=1, read_cl="ONE", write_cl="ONE",
                wallet_profile=compare.wallet_runner.WALLET_PROFILE, node_count=1)
            trial_id = (f"trial-{case.order:03d}-"
                        f"{compare.wallet_runner.case_id_for_profile(trial_case, 'batch', None)}")
            result = {
                "trial_id": trial_id, "status": "passed", "cleanup_status": "clean",
                "launch_config": {"nodea": {"host_io_limits_validation": {"valid": True}}},
                "io_properties_enabled": True,
                "io_properties_sha256": compare.SCYLLA_IO_PROPERTIES_SHA256,
                "io_properties_profile": {"source_sha256": compare.SCYLLA_IO_PROPERTIES_SHA256,
                                           "copied_sha256": compare.SCYLLA_IO_PROPERTIES_SHA256},
                "io_properties_runtime_validation": {"valid": True},
                "validation": {"io_properties_runtime": True},
                "cli_result": {"valid": True, "measurement": measurement},
            }
            lifecycle: list[str] = []

            class Child:
                pid = 9876
                returncode = 0

                def poll(self) -> int | None:
                    return None

                def wait(self, timeout: float | None = None) -> int:
                    return 0

            child = Child()

            def fake_run_trial(_run_id: str, _run_dir: pathlib.Path, _trial: object,
                               args: object, _users: int, _requests: int,
                               _binary: pathlib.Path) -> dict[str, object]:
                self.assertEqual(args.io_properties_file, compare.SCYLLA_IO_PROPERTIES_PATH)
                self.assertEqual(args.resolved_io_properties_profile["source_sha256"],
                                 compare.SCYLLA_IO_PROPERTIES_SHA256)
                (run_dir / "trials" / trial_id).mkdir(parents=True)
                lifecycle.append("runner_cleanup_complete")
                return result

            def fake_join(_child: object, stop_file: pathlib.Path) -> dict[str, object]:
                stop_file.parent.mkdir(parents=True, exist_ok=True)
                stop_file.write_text("stop\n")
                self.assertTrue(stop_file.parent.is_dir())
                lifecycle.append("observer_joined")
                return {"valid": True, "returncode": 0, "stop_file": str(stop_file)}

            analysis = self._valid_observer_analysis(measurement)
            with (mock.patch.object(compare.subprocess, "Popen", return_value=child),
                  mock.patch.object(compare, "_wait_diskstats_sample_after",
                                    return_value={"time_unix_ns": 1}),
                  mock.patch.object(compare.wallet_runner, "run_trial", side_effect=fake_run_trial),
                  mock.patch.object(compare, "_wait_observer", side_effect=fake_join),
                  mock.patch.object(compare.disk_observer, "analyze_trial",
                                    side_effect=lambda *_args: (lifecycle.append("analysis_after_join") or analysis)),
                  mock.patch.object(compare, "analyze_host_diskstats",
                                    return_value=self._valid_host_window())):
                item = compare.run_scylla_case(run_id, run_dir, case, binary, limits)

            self.assertLess(lifecycle.index("runner_cleanup_complete"), lifecycle.index("observer_joined"))
            self.assertLess(lifecycle.index("observer_joined"), lifecycle.index("analysis_after_join"))
            self.assertTrue(item["io_properties_verified"])
            self.assertTrue(item["host_diskstats_valid"])
            case_artifact = json.loads((run_dir / "trials" / trial_id /
                                        "wallet-db-compare-case.json").read_text())
            self.assertEqual(case_artifact["io_properties"]["expected_sha256"],
                             compare.SCYLLA_IO_PROPERTIES_SHA256)
            self.assertTrue(case_artifact["host_diskstats_observer"]["valid"])

    def test_scylla_observer_is_stopped_even_when_runner_raises(self) -> None:
        case = compare.CompareCase(4, "scylla", 2)
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = pathlib.Path(tmp)
            binary = run_dir / "bin"
            binary.write_bytes(b"binary")
            child = mock.Mock(pid=999, returncode=0)
            child.poll.return_value = None
            lifecycle: list[str] = []

            def fake_wait(_observer_dir: pathlib.Path, _boundary: int,
                          _child: object) -> dict[str, int]:
                lifecycle.append("first_sample")
                return {"time_unix_ns": 1}

            def fake_join(_child: object, _stop: pathlib.Path) -> dict[str, object]:
                lifecycle.append("observer_joined")
                return {"valid": True, "returncode": 0}

            with (mock.patch.object(compare.subprocess, "Popen", return_value=child),
                  mock.patch.object(compare, "_wait_diskstats_sample_after", side_effect=fake_wait),
                  mock.patch.object(compare.wallet_runner, "run_trial",
                                    side_effect=RuntimeError("fixture runner failure")),
                  mock.patch.object(compare, "_wait_observer", side_effect=fake_join)):
                with self.assertRaisesRegex(compare.CompareError, "runner failed"):
                    compare.run_scylla_case("run", run_dir, case, binary, {})
            self.assertIn("observer_joined", lifecycle)
            evidence_path = run_dir / "scylla-host-observers" / case.case_id / "wallet-db-compare-case.json"
            self.assertTrue(evidence_path.is_file())


class PostgresStartupEvidenceTests(unittest.TestCase):
    @staticmethod
    def _owned_case(case_dir: pathlib.Path) -> compare.OwnedPostgresCase:
        case = compare.CompareCase(1, "postgres", 1, 16)
        owned = compare.OwnedPostgresCase(case_dir.parent, case_dir, "run-test", case, {})
        owned.container_id = "owned-pg-container"
        return owned

    def test_readiness_requires_tcp_and_records_socket_only_startup_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            case_dir = pathlib.Path(temp_dir) / "case"
            case_dir.mkdir()
            owned = self._owned_case(case_dir)
            probe_results = [
                mock.Mock(returncode=1, stdout="no response", stderr=""),
                mock.Mock(returncode=0, stdout="accepting connections", stderr=""),
            ]
            with (mock.patch.object(compare.subprocess, "run", side_effect=probe_results) as run,
                  mock.patch.object(owned, "_inspect_container",
                                    return_value={"State": {"Status": "running", "Running": True}}),
                  mock.patch.object(compare.time, "monotonic", return_value=10.0),
                  mock.patch.object(compare.time, "sleep")):
                owned._wait_ready()

            self.assertEqual(run.call_count, 2)
            for call in run.call_args_list:
                argv = call.args[0]
                self.assertIn("-h", argv)
                self.assertEqual(argv[argv.index("-h") + 1], "127.0.0.1")
                self.assertEqual(argv[argv.index("-p") + 1], "5432")
            rows = [json.loads(line) for line in owned.readiness_probe_path.read_text().splitlines()]
            self.assertEqual([row["returncode"] for row in rows], [1, 0])
            self.assertEqual(rows[0]["stdout"], "no response")
            self.assertEqual(owned.readiness_probe_count, 2)

    def test_early_container_exit_retains_probe_inspect_and_logs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            case_dir = pathlib.Path(temp_dir) / "case"
            case_dir.mkdir()
            owned = self._owned_case(case_dir)
            inspect_payload = json.dumps([{"State": {"Status": "exited", "Running": False,
                                                        "ExitCode": 1}}])
            command_results = [
                mock.Mock(returncode=1, stdout="no response", stderr=""),
                mock.Mock(returncode=0, stdout=inspect_payload, stderr=""),
                mock.Mock(returncode=0, stdout="init server exited", stderr="docker logs stderr"),
            ]
            with (mock.patch.object(compare.subprocess, "run", side_effect=command_results) as run,
                  mock.patch.object(owned, "_inspect_container",
                                    return_value={"State": {"Status": "exited", "Running": False,
                                                             "ExitCode": 1}}),
                  mock.patch.object(compare.time, "monotonic", return_value=10.0)):
                with self.assertRaisesRegex(compare.CompareError, "exited before TCP readiness"):
                    owned._wait_ready()

            rows = [json.loads(line) for line in owned.readiness_probe_path.read_text().splitlines()]
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["container_state"]["Status"], "exited")
            self.assertEqual(owned.readiness_probe_count, 1)
            self.assertTrue((case_dir / "failure-container-inspect-early-exit-001.stdout.json").is_file())
            logs = (case_dir / "failure-container-logs-early-exit-001.txt").read_text()
            self.assertIn("init server exited", logs)
            self.assertIn("docker logs stderr", logs)
            self.assertEqual(run.call_count, 3)

    def test_tcp_readiness_timeout_retains_last_probe_and_container_diagnostics(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            case_dir = pathlib.Path(temp_dir) / "case"
            case_dir.mkdir()
            owned = self._owned_case(case_dir)
            command_results = [
                mock.Mock(returncode=1, stdout="no response", stderr=""),
                mock.Mock(returncode=0, stdout='[{"State":{"Status":"running"}}]', stderr=""),
                mock.Mock(returncode=0, stdout="server did not become TCP-ready", stderr=""),
            ]
            with (mock.patch.object(compare.subprocess, "run", side_effect=command_results) as run,
                  mock.patch.object(owned, "_inspect_container",
                                    return_value={"State": {"Status": "running", "Running": True}}),
                  mock.patch.object(compare, "PG_READY_SECONDS", 0.01),
                  mock.patch.object(compare.time, "monotonic",
                                    side_effect=[100.0, 100.0, 100.0, 100.02]),
                  mock.patch.object(compare.time, "sleep")):
                with self.assertRaisesRegex(compare.CompareError, "TCP readiness timed out"):
                    owned._wait_ready()

            rows = [json.loads(line) for line in owned.readiness_probe_path.read_text().splitlines()]
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["returncode"], 1)
            self.assertEqual(rows[0]["argv"][rows[0]["argv"].index("-h") + 1], "127.0.0.1")
            self.assertEqual(owned.readiness_probe_count, 1)
            self.assertTrue((case_dir / "failure-container-inspect-readiness-timeout.stdout.json").is_file())
            logs = (case_dir / "failure-container-logs-readiness-timeout.txt").read_text()
            self.assertIn("server did not become TCP-ready", logs)
            self.assertEqual(run.call_count, 3)

    def test_host_route_failure_saves_error_and_container_diagnostics(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            case_dir = pathlib.Path(temp_dir) / "case"
            case_dir.mkdir()
            owned = self._owned_case(case_dir)
            owned.container_ip = "172.24.0.2"
            command_results = [
                mock.Mock(returncode=0, stdout='[{"Id":"owned-pg-container"}]', stderr=""),
                mock.Mock(returncode=0, stdout="postgres exited", stderr=""),
            ]
            with (mock.patch.object(compare.socket, "create_connection",
                                    side_effect=OSError(errno.EHOSTUNREACH, "No route to host")),
                  mock.patch.object(compare.subprocess, "run", side_effect=command_results)):
                with self.assertRaisesRegex(compare.CompareError, "cannot reach PostgreSQL"):
                    owned.host_dsn()

            failure = json.loads((case_dir / "host-network-reachability-failure.json").read_text())
            self.assertEqual(failure["container_ip"], "172.24.0.2")
            self.assertEqual(failure["port"], 5432)
            self.assertEqual(failure["errno"], errno.EHOSTUNREACH)
            self.assertTrue((case_dir / "failure-container-inspect-host-dsn-connect-failure.stdout.json").is_file())
            logs = (case_dir / "failure-container-logs-host-dsn-connect-failure.txt").read_text()
            self.assertIn("postgres exited", logs)


class PostgresMonitorCadenceTests(unittest.TestCase):
    class FakeClock:
        def __init__(self, now_ns: int) -> None:
            self.now_ns = now_ns

        def monotonic_ns(self) -> int:
            return self.now_ns

        def advance(self, duration_seconds: float) -> None:
            self.now_ns += round(duration_seconds * 1_000_000_000)

    class FakeEvent:
        def __init__(self, clock: "PostgresMonitorCadenceTests.FakeClock") -> None:
            self.clock = clock
            self.stopped = False
            self.waits: list[float] = []

        def is_set(self) -> bool:
            return self.stopped

        def set(self) -> None:
            self.stopped = True

        def wait(self, timeout: float | None = None) -> bool:
            assert timeout is not None
            self.waits.append(timeout)
            self.clock.advance(timeout)
            return self.stopped

    def _run_fake_schedule(self, *, baseline_duration: float,
                           capture_durations: list[float]) -> tuple[list[float], list[float]]:
        with tempfile.TemporaryDirectory() as temp_dir:
            monitor = compare.PostgresMonitor(pathlib.Path(temp_dir), "container",
                                              pathlib.Path("/sys/fs/cgroup"), "schema")
            clock = self.FakeClock(round(baseline_duration * 1_000_000_000))
            stop_event = self.FakeEvent(clock)
            monitor.stop_event = stop_event  # type: ignore[assignment]
            starts: list[float] = []
            durations = iter(capture_durations)

            def capture() -> dict[str, int]:
                start_ns = clock.monotonic_ns()
                starts.append(start_ns / 1_000_000_000)
                clock.advance(next(durations))
                if len(starts) == len(capture_durations):
                    stop_event.set()
                return {"sampled_monotonic_ns": start_ns}

            monitor.capture = capture  # type: ignore[method-assign]
            try:
                with mock.patch.object(compare.time, "monotonic_ns", side_effect=clock.monotonic_ns):
                    monitor._loop(0)
            finally:
                monitor._handle.close()
            return starts, stop_event.waits

    def test_long_capture_is_followed_immediately_without_extra_interval_or_burst(self) -> None:
        starts, waits = self._run_fake_schedule(baseline_duration=1.67,
                                                capture_durations=[1.67, 1.67, 1.67])
        self.assertEqual(len(waits), 0)
        self.assertEqual(len(starts), 3)
        self.assertAlmostEqual(starts[0], 1.67, delta=0.001)
        for previous, current in zip(starts, starts[1:]):
            self.assertAlmostEqual(current - previous, 1.67, delta=0.001)

    def test_short_capture_keeps_one_second_capture_start_cadence(self) -> None:
        starts, waits = self._run_fake_schedule(baseline_duration=0.4,
                                                capture_durations=[0.4, 0.4, 0.4])
        self.assertEqual(len(starts), 3)
        self.assertAlmostEqual(starts[0], 1.0, delta=0.001)
        for previous, current in zip(starts, starts[1:]):
            self.assertAlmostEqual(current - previous, 1.0, delta=0.001)
        self.assertEqual(len(waits), 3)
        for wait in waits:
            self.assertAlmostEqual(wait, 0.6, delta=0.001)

    def test_stop_event_wakes_wait_and_capture_error_sets_stop(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            monitor = compare.PostgresMonitor(pathlib.Path(temp_dir), "container",
                                              pathlib.Path("/sys/fs/cgroup"), "schema")
            waiting = threading.Event()

            class ObservableStopEvent(threading.Event):
                def wait(self, timeout: float | None = None) -> bool:
                    waiting.set()
                    return super().wait(timeout)

            stop_event = ObservableStopEvent()
            monitor.stop_event = stop_event
            calls = 0

            def capture() -> dict[str, int]:
                nonlocal calls
                calls += 1
                return {"sampled_monotonic_ns": time.monotonic_ns()}

            monitor.capture = capture  # type: ignore[method-assign]
            thread = threading.Thread(target=monitor._loop,
                                      args=(time.monotonic_ns(),), daemon=True)
            try:
                thread.start()
                self.assertTrue(waiting.wait(timeout=1.0), "monitor did not enter its timed wait")
                stop_event.set()
                thread.join(timeout=1.0)
                self.assertFalse(thread.is_alive(), "stop did not wake the monitor wait")
                self.assertEqual(calls, 0)

                monitor.stop_event = threading.Event()
                monitor.capture = mock.Mock(side_effect=RuntimeError("fixture capture failure"))  # type: ignore[method-assign]
                monitor._loop(time.monotonic_ns() - 2_000_000_000)
                self.assertTrue(monitor.stop_event.is_set())
                self.assertEqual(monitor.errors, ["RuntimeError: fixture capture failure"])
            finally:
                monitor._handle.close()


if __name__ == "__main__":
    unittest.main()
