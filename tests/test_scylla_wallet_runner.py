"""Boundary tests for the stdlib Scylla wallet runner."""
from __future__ import annotations

import gzip
import csv
import hashlib
import io
import json
import pathlib
import subprocess
import tempfile
import unittest
from contextlib import ExitStack, redirect_stdout
from unittest import mock

from benches import scylla_wallet as runner


class HostIOLimitTests(unittest.TestCase):
    def test_common_limit_document_and_docker_flags_are_exact(self) -> None:
        limits = dict(runner.EXPECTED_HOST_IO_LIMITS)
        self.assertEqual(runner.validate_host_io_limits_document(limits), limits)
        self.assertEqual(runner.docker_io_limit_flags(limits), [
            "--device-read-bps", "/dev/sdb:403631184",
            "--device-write-bps", "/dev/sdb:152226096",
            "--device-read-iops", "/dev/sdb:24497",
            "--device-write-iops", "/dev/sdb:23500",
        ])
        self.assertEqual(runner.docker_io_limit_flags(None), [])
        changed = {**limits, "read_iops": 1}
        with self.assertRaisesRegex(runner.RunnerError, "read_iops"):
            runner.validate_host_io_limits_document(changed)
        with self.assertRaisesRegex(runner.RunnerError, "exactly"):
            runner.validate_host_io_limits_document({**limits, "extra": 1})

    def test_container_verification_requires_docker_and_effective_cgroup_caps(self) -> None:
        limits = dict(runner.EXPECTED_HOST_IO_LIMITS)
        host_config = {
            "BlkioDeviceReadBps": [{"Path": "/dev/sdb", "Rate": 403631184}],
            "BlkioDeviceWriteBps": [{"Path": "/dev/sdb", "Rate": 152226096}],
            "BlkioDeviceReadIOps": [{"Path": "/dev/sdb", "Rate": 24497}],
            "BlkioDeviceWriteIOps": [{"Path": "/dev/sdb", "Rate": 23500}],
        }
        io_max = "8:16 rbps=403631184 wbps=152226096 riops=24497 wiops=23500\n"
        evidence = runner.validate_container_io_limits(host_config, 71, limits, io_max)
        self.assertTrue(evidence["valid"])
        self.assertEqual(evidence["effective_limits"]["major_minor"], "8:16")
        with self.assertRaisesRegex(runner.RunnerError, "HostConfig"):
            runner.validate_container_io_limits({**host_config, "BlkioDeviceWriteIOps": None},
                                                71, limits, io_max)
        with self.assertRaisesRegex(runner.RunnerError, "io.max limits differ"):
            runner.validate_container_io_limits(host_config, 71, limits,
                                                "8:16 rbps=max wbps=152226096 riops=24497 wiops=23500\n")

    def test_caps_are_opt_in_and_legacy_docker_launch_flags_stay_empty(self) -> None:
        args = runner.create_argument_parser().parse_args(["--mode", "smoke"])
        parameters = runner.resolve_run_parameters(args)
        self.assertIsNone(parameters["host_io_limits"])
        self.assertEqual(runner.docker_io_limit_flags(parameters["host_io_limits"]), [])


class MatrixTests(unittest.TestCase):
    def test_formal_matrix_uses_deterministic_rotation_and_pool_math(self) -> None:
        cases = runner.build_matrix((1, 2, 4), (12, 24, 48), 3)
        self.assertEqual(len(cases), 27)
        self.assertEqual([(case.shards, case.workers) for case in cases[:9]], [
            (1, 12), (1, 24), (1, 48), (2, 12), (2, 24), (2, 48),
            (4, 12), (4, 24), (4, 48),
        ])
        self.assertEqual([(case.shards, case.workers) for case in cases[9:12]], [
            (1, 24), (1, 48), (2, 12),
        ])
        self.assertEqual([case.per_shard_pool for case in cases if case.shards == 4][:3],
                         [1, 2, 4])

    def test_worker_count_must_be_divisible_by_three_times_shards(self) -> None:
        with self.assertRaisesRegex(runner.RunnerError, "not divisible"):
            runner.build_matrix((2,), (10,), 1)

    def test_formal_completeness_requires_exact_workload(self) -> None:
        self.assertTrue(runner.is_formal_matrix((1, 2, 4), (12, 24, 48), 3, 50_000, 200))
        self.assertFalse(runner.is_formal_matrix((1, 2, 4), (12, 24, 48), 3, 100, 20))

    def test_original_five_positional_case_arguments_keep_constructor_compatibility(self) -> None:
        case = runner.TrialCase(1, 12, 4, 1, 1)
        self.assertEqual(case.case_id, "S1-C12-wallet-memory-lwt-v1-rep1")
        self.assertEqual(case.data_connection_target, 12)
        self.assertEqual(case.expected_connections_per_node, 4)
        self.assertEqual(case.worker_routing, "round-robin")
        self.assertFalse(case.per_shard_pool_overridden)

    def test_explicit_per_user_pool_decouples_workers_and_connections(self) -> None:
        case, = runner.build_matrix((4,), (50_000,), 1, per_shard_pool=4,
                                    worker_routing="per-user")
        self.assertEqual((case.shards, case.workers, case.per_shard_pool), (4, 50_000, 4))
        self.assertEqual(case.data_connection_target, 48)
        self.assertEqual(case.expected_connections_per_node, 16)
        self.assertEqual(case.setup_concurrency, 48)
        self.assertEqual(case.per_worker_queue_capacity, 1)
        self.assertTrue(case.per_shard_pool_overridden)
        self.assertEqual(case.requested_per_shard_pool, 4)
        self.assertEqual(case.case_id,
                         "S4-C50000-P4-D48-per-user-wallet-memory-lwt-v1-rep1")

    def test_explicit_pool_and_routing_never_qualify_as_full_formal_matrix(self) -> None:
        self.assertTrue(runner.is_formal_matrix(
            (1, 2, 4), (12, 24, 48), 3, 50_000, 200))
        self.assertFalse(runner.is_formal_matrix(
            (1, 2, 4), (12, 24, 48), 3, 50_000, 200, per_shard_pool=4))
        self.assertFalse(runner.is_formal_matrix(
            (1, 2, 4), (12, 24, 48), 3, 50_000, 200, worker_routing="per-user"))

    def test_nondefault_durability_has_distinct_case_id_and_is_custom_formal(self) -> None:
        case, = runner.build_matrix((4,), (50_000,), 1, per_shard_pool=4,
                                    worker_routing="per-user", replication_factor=1,
                                    read_cl=1, write_cl=1)
        self.assertEqual(case.case_id,
                         "S4-C50000-P4-D48-per-user-RF1-R1-W1-wallet-memory-lwt-v1-rep1")
        self.assertEqual((case.replication_factor, case.read_cl, case.write_cl),
                         (1, "ONE", "ONE"))
        self.assertFalse(runner.is_formal_matrix(
            (1, 2, 4), (12, 24, 48), 3, 50_000, 200,
            replication_factor=1, read_cl=1, write_cl=1))

    def test_periodic_sync_profile_is_explicit_custom_pilot_and_has_distinct_case_id(self) -> None:
        parser = runner.create_argument_parser()
        args = parser.parse_args([
            "--mode", "pilot", "--node-count", "1", "--shards", "2", "--workers", "512",
            "--per-shard-pool", "3", "--replication-factor", "1", "--read-cl", "1",
            "--write-cl", "1", "--commitlog-sync", "periodic",
            "--commitlog-sync-period-in-ms", "10000", "--users", "1024",
            "--requests-per-user", "1000", "--repeats", "1",
        ])
        parameters = runner.resolve_run_parameters(args)
        case, = parameters["cases"]
        self.assertEqual((case.shards, case.workers, case.per_shard_pool,
                          case.replication_factor, case.read_cl, case.write_cl,
                          case.data_connection_target), (2, 512, 3, 1, "ONE", "ONE", 6))
        self.assertEqual(parameters["commitlog_sync"], "periodic")
        self.assertEqual(parameters["commitlog_sync_period_in_ms"], 10_000)
        self.assertFalse(parameters["exact_formal_candidate"])
        self.assertEqual(parameters["workload_class"], "diagnostic_pilot")
        self.assertIn("commitlog-periodic-10000ms",
                      runner.case_id_for_profile(case, "periodic", 10_000))
        self.assertEqual(runner.resolve_run_parameters(parser.parse_args([]))["commitlog_sync"],
                         "batch")

    def test_periodic_sync_rejects_missing_wrong_period_and_non_pilot_modes(self) -> None:
        parser = runner.create_argument_parser()
        base = ["--mode", "pilot", "--node-count", "1", "--shards", "2", "--workers", "512",
                "--per-shard-pool", "3", "--replication-factor", "1", "--read-cl", "1",
                "--write-cl", "1"]
        invalid = (
            base + ["--commitlog-sync-period-in-ms", "10000"],
            base + ["--commitlog-sync", "periodic"],
            base + ["--commitlog-sync", "periodic", "--commitlog-sync-period-in-ms", "5000"],
            ["--mode", "formal", "--commitlog-sync", "periodic",
             "--commitlog-sync-period-in-ms", "10000"],
            ["--mode", "smoke", "--commitlog-sync", "periodic",
             "--commitlog-sync-period-in-ms", "10000"],
        )
        for argv in invalid:
            with self.subTest(argv=argv), self.assertRaises(runner.RunnerError):
                runner.resolve_run_parameters(parser.parse_args(argv))

    def test_consistency_cannot_exceed_replication_factor(self) -> None:
        for args, error in (((1, 3, 1), "read-cl THREE"),
                            ((1, 1, 2), "write-cl TWO"),
                            ((0, 1, 1), "replication-factor"),
                            ((4, 1, 1), "replication-factor")):
            with self.subTest(args=args), self.assertRaisesRegex(runner.RunnerError, error):
                runner.build_matrix((4,), (50_000,), 1, per_shard_pool=4,
                                    worker_routing="per-user", replication_factor=args[0],
                                    read_cl=args[1], write_cl=args[2])


class NodeTopologyTests(unittest.TestCase):
    def test_legacy_three_node_defaults_and_formal_candidate_are_unchanged(self) -> None:
        args = runner.create_argument_parser().parse_args([])
        params = runner.resolve_run_parameters(args)
        self.assertEqual(args.node_count, 3)
        self.assertEqual(args.replication_factor, 3)
        self.assertEqual(params["node_names"], ("nodea", "nodeb", "nodec"))
        self.assertTrue(params["exact_formal_candidate"])
        self.assertEqual(len(params["cases"]), 27)
        first = params["cases"][0]
        self.assertEqual((first.node_count, first.per_shard_pool,
                          first.expected_connections_per_node,
                          first.data_connection_target), (3, 4, 4, 12))
        self.assertEqual(first.case_id, "S1-C12-wallet-memory-lwt-v1-rep1")
        self.assertEqual(runner.cpuset_for(1), {
            "nodea": "0", "nodeb": "2", "nodec": "4", "service": "6,7,14,15"})

    def test_single_node_rf1_cl1_s1_s2_c128_p3_resolves_and_dry_runs(self) -> None:
        args = runner.create_argument_parser().parse_args([
            "--mode", "formal", "--node-count", "1", "--replication-factor", "1",
            "--read-cl", "1", "--write-cl", "1", "--shards", "1,2",
            "--workers", "128", "--per-shard-pool", "3", "--worker-routing", "round-robin",
            "--users", "1024", "--requests-per-user", "1000", "--repeats", "1",
            "--skip-build", "--dry-run",
        ])
        params = runner.resolve_run_parameters(args)
        self.assertEqual(params["node_names"], ("nodea",))
        self.assertFalse(params["exact_formal_candidate"])
        self.assertEqual(params["workload_class"], "custom_workload")
        cases = params["cases"]
        self.assertEqual([(case.shards, case.workers, case.per_shard_pool,
                           case.expected_connections_per_node, case.data_connection_target)
                          for case in cases], [(1, 128, 3, 3, 3), (2, 128, 3, 6, 6)])
        self.assertTrue(all("-N1" in case.case_id and "RF1-R1-W1" in case.case_id
                            for case in cases))
        self.assertEqual(runner.cpuset_for(1, 1), {"nodea": "0", "service": "6,7,14,15"})
        self.assertEqual(runner.cpuset_for(2, 1), {"nodea": "0,1", "service": "6,7,14,15"})

        with mock.patch.object(runner, "host_inventory", return_value={
                "aio_max_nr": 1_000_000, "aio_nr": 0, "disk_available_bytes": 200_000_000_000,
                "memory_available_bytes": 40_000_000_000, "lscpu_json": {},
                "cpu_sibling_map": {}, "data_path": "/tmp/output", "backing_device": "sdb",
                "docker_version": "test"}):
            report = runner.dry_run_report(args, params)
        self.assertEqual(report["node_count"], 1)
        self.assertEqual(report["node_names"], ["nodea"])
        self.assertEqual(report["matrix_case_count"], 2)
        self.assertFalse(report["formal_complete"])
        self.assertEqual(report["capacity_estimate"]["node_count"], 1)
        self.assertEqual(report["aio_budget_by_shards"]["1"]["required_available"],
                         1 * 1 * (1024 + 1024 + 2) + 1024)
        self.assertEqual(report["aio_budget_by_shards"]["2"]["required_available"],
                         1 * 2 * (1024 + 1024 + 2) + 1024)
        for command, case in zip(report["commands"], cases):
            argv = command["command"]
            self.assertEqual(argv[argv.index("--nodes") + 1], "NODEA:9042")
            self.assertEqual(argv[argv.index("--per-shard-pool") + 1], "3")
            self.assertEqual(argv[argv.index("--replication-factor") + 1], "1")
            self.assertEqual(argv[argv.index("--read-cl") + 1], "1")
            self.assertEqual(argv[argv.index("--write-cl") + 1], "1")
            self.assertEqual(command["target_data_connections_total_D"], case.shards * 3)
            self.assertEqual(command["expected_data_connections_per_node"], case.shards * 3)

        full_rf3 = runner.estimate_disk_bytes(1024, 1000, replication_factor=1, node_count=3)
        one_rf1 = report["capacity_estimate"]
        self.assertEqual(one_rf1["estimated_replicated_bytes"], full_rf3["estimated_replicated_bytes"])
        self.assertEqual(one_rf1["estimated_commitlog_bytes"] * 3,
                         full_rf3["estimated_commitlog_bytes"])

    def test_unsupported_topology_and_rf_above_nodes_fail_before_cluster(self) -> None:
        with self.assertRaisesRegex(runner.RunnerError, "node-count must be one of 1,3"):
            runner.node_names_for(2)
        args = runner.create_argument_parser().parse_args([
            "--mode", "pilot", "--node-count", "1", "--replication-factor", "3",
            "--read-cl", "3", "--write-cl", "3",
        ])
        with self.assertRaisesRegex(runner.RunnerError, "replication-factor 3 exceeds node-count 1"):
            runner.resolve_run_parameters(args)

    def test_single_node_session_connection_audit_uses_s_times_p(self) -> None:
        session = "one-node-session"
        rows = {"nodea": [{
            "row": {"port": 9200 + index},
            "client_options": {"SESSION_ID": session,
                               "APPLICATION_NAME": "ledger-wallet-scylla-bench"},
        } for index in range(6)]}
        audit = runner.audit_session_connections(
            rows, session, workers=128, expected_data_per_node=6,
            target_data_connections=6, shards=2, per_shard_pool=3, node_count=1)
        self.assertTrue(audit["valid"], audit["problems"])
        self.assertEqual(audit["node_count"], 1)
        self.assertEqual(audit["actual_data_connections"], 6)
        self.assertEqual(audit["nodes"]["nodea"]["actual_data"], 6)

        missing_connection = {"nodea": rows["nodea"][:-1]}
        invalid = runner.audit_session_connections(
            missing_connection, session, workers=128, expected_data_per_node=6,
            target_data_connections=6, shards=2, per_shard_pool=3, node_count=1)
        self.assertFalse(invalid["valid"])

    def test_single_node_aio_and_metrics_validators_use_exact_inventory(self) -> None:
        one_node_budget = runner.compute_aio_budget(2, 100_000, 0, node_count=1)
        three_node_budget = runner.compute_aio_budget(2, 100_000, 0, node_count=3)
        per_shard = 1024 + 1024 + 2
        self.assertEqual(one_node_budget["required_available"], 2 * per_shard + 1024)
        self.assertEqual(three_node_budget["required_available"], 3 * 2 * per_shard + 1024)

        names = set(runner.COMMITLOG_COUNTERS)
        categories = runner.metric_evidence_from_names(names | {
            "scylla_reactor_utilization", "scylla_coordinator_write_latency",
            "scylla_cache_hits", "scylla_flushes", "scylla_compaction_tasks",
            "scylla_backlog",
            "scylla_storage_proxy_coordinator_write_errors",
            "scylla_storage_proxy_coordinator_read_timeouts",
            "scylla_network_bytes_sent", "scylla_network_bytes_received",
        })
        summary = {
            "expected_node_names": ["nodea"],
            "measurement_window": {"timestamps_valid": True},
            "prometheus_window_samples": {"nodea": 3},
            "required_metric_evidence": {"nodea": categories},
            "process_window": {"valid": True}, "socket_window": {"valid": True},
            "counter_window_deltas": {"valid": True},
        }
        self.assertTrue(runner.required_metric_validation(summary)["valid"])
        summary["required_metric_evidence"]["nodeb"] = categories
        self.assertFalse(runner.required_metric_validation(summary)["valid"])

    def test_single_node_run_reports_only_its_observed_node_metrics(self) -> None:
        args = runner.create_argument_parser().parse_args([
            "--mode", "pilot", "--node-count", "1", "--replication-factor", "1",
            "--read-cl", "1", "--write-cl", "1", "--shards", "1",
            "--workers", "128", "--per-shard-pool", "3", "--users", "1024",
            "--requests-per-user", "1000", "--repeats", "1",
        ])
        params = runner.resolve_run_parameters(args)
        case, = params["cases"]
        trial_id = f"trial-{case.order:03d}-{case.case_id}"
        node_metrics = {
            metric: {"series": {"0": {"status": "observed", "delta": 4,
                                       "start_value": 1, "start_sample_unix_ns": 10,
                                       "start_sampling_offset_ns": -1,
                                       "end_value": 5, "end_sample_unix_ns": 20,
                                       "end_sampling_offset_ns": 1,
                                       "window_sample_count": 2}}}
            for metric in runner.COMMITLOG_COUNTERS
        }
        trial = {
            "order": case.order, "trial_id": trial_id, "mode": "pilot",
            "status": "passed_relaxed", "cleanup_status": "clean",
            "required_metrics_valid": False, "node_count": 1, "node_names": ["nodea"],
            "wallet_profile": runner.WALLET_PROFILE, "account_state_source": "memory",
            "serial_cl": "LOCAL_SERIAL", "cql_requests_per_transaction": 1,
            "cql_statement_count": 1, "measured_select_requests": 0,
            "logical_wallet_mutations_expected": 1_024_000,
            "replication_factor": 1, "read_cl": "ONE", "write_cl": "ONE",
            "shards": 1, "workers": 128, "per_shard_pool": 3,
            "per_shard_pool_overridden": True, "requested_per_shard_pool": 3,
            "worker_routing": "round-robin", "expected_data_connections_per_node": 3,
            "setup_concurrency": 3, "per_worker_queue_capacity": runner.QUEUE_CAPACITY,
            "D": 3, "users": 1024, "requests_per_user": 1000,
            "total_requests": 1_024_000, "image_digest": runner.PINNED_REPO_DIGEST,
            "monitor": {"counter_window_deltas": {"nodes": {"nodea": node_metrics}}},
            "cli_result": {
                "valid": True, "requests": {"expected": 1_024_000,
                                              "replies_received": 1_024_000},
                "measurement": {"rps": 1000, "wall_seconds": 1024},
                "end_to_end_latency": {"p50_ns": 100, "p95_ns": 200,
                                       "p99_ns": 300, "p999_ns": 400, "max_ns": 500},
                "connection_pool": {"pool_ready_connections": 3,
                                    "pool_after_last_reply": 3,
                                    "minimum_sampled_connections": 3,
                                    "maximum_sampled_connections": 3},
                "stage_metrics": {"counters": {"cql_write_lwt": 1_024_000},
                                  "cql_in_flight_peak": 128},
                "driver_counters": {"delta": {"queries": 1_024_000}},
                "process_cpu": {"cpu_used_ns": 1_024_000_000},
                "heartbeat": {"maximum_lag_ns": 1000},
            },
        }
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = pathlib.Path(tmp) / "run-one"
            run_dir.mkdir()
            summary = runner.write_run_reports(run_dir, args, params, [trial],
                                               {"valid": True})
            self.assertEqual(summary["node_count"], 1)
            self.assertEqual(summary["node_names"], ["nodea"])
            self.assertFalse(summary["formal_complete"])
            with (run_dir / "summary.csv").open(newline="", encoding="utf-8") as handle:
                row = next(csv.DictReader(handle))
            self.assertEqual(row["node_count"], "1")
            self.assertIn("nodea", row["node_names"])
            self.assertEqual(row["commitlog_flush_delta_total"], "4.0")
            self.assertEqual(row["commitlog_flush_nodea_delta"], "4.0")
            self.assertEqual(row["commitlog_flush_nodeb_delta"], "")
            with (run_dir / "counter-deltas.csv").open(newline="", encoding="utf-8") as handle:
                counter_rows = list(csv.DictReader(handle))
            self.assertTrue(counter_rows)
            self.assertEqual({row["node"] for row in counter_rows}, {"nodea"})
            report_text = (run_dir / "report.zh-TW.md").read_text(encoding="utf-8")
            self.assertIn("Nodes：1 (nodea)", report_text)
            self.assertIn("單節點配置沒有副本冗餘", report_text)

    def test_one_node_ownership_marker_and_data_removal_are_narrow(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            trial = pathlib.Path(tmp) / "trial"
            owned = trial / "owned-data"
            trial.mkdir()
            owned.mkdir()
            (owned / "nodea").mkdir()
            runner.write_ownership_marker(owned, "one-node-nonce", "one-node-trial", 1)
            cluster = runner.OwnedCluster(
                trial, owned, "one-node-nonce", "one-node-trial", 1, "run-one", node_count=1)
            marker = runner.validate_ownership_marker(
                trial, owned, "one-node-nonce", "one-node-trial", 1)
            self.assertTrue(marker.is_file())
            self.assertEqual(cluster.node_names, ("nodea",))
            commands = []

            def clean_exact_mount(argv, **kwargs):
                commands.append(list(argv))
                return subprocess.CompletedProcess(list(argv), 0, "", "")

            with mock.patch.object(runner, "_run", side_effect=clean_exact_mount):
                cluster._remove_owned_data()
            self.assertEqual(len(commands), 1)
            self.assertTrue(any(
                argument == f"type=bind,src={(owned / 'nodea').resolve()},dst=/owned"
                for argument in commands[0]))
            self.assertFalse(owned.exists())

    def test_one_node_final_result_requires_exact_endpoint_inventory(self) -> None:
        case, = runner.build_matrix((2,), (128,), 1, per_shard_pool=3,
                                    replication_factor=1, read_cl=1, write_cl=1,
                                    node_count=1)
        result = _valid_memory_lwt_result(case, users=64, requests_per_user=20)
        result["config"]["nodes"] = ["172.24.0.2:9042"]
        evidence = runner.validate_cli_result_profile(
            result, case, expected_nodes=["172.24.0.2:9042"])
        self.assertTrue(evidence["valid"])
        self.assertEqual(evidence["node_count"], 1)
        self.assertEqual(evidence["D"], 6)
        with self.assertRaisesRegex(runner.RunnerError, "do not match cluster endpoints"):
            runner.validate_cli_result_profile(
                result, case, expected_nodes=["172.24.0.3:9042"])

    def test_optional_io_properties_profile_is_hash_captured_and_formal_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            source = root / "io.conf"
            source.write_text("[commitlog]\nread_ahead = 4096\n", encoding="utf-8")
            source_profile = runner.inspect_io_properties_source(source)
            trial = root / "trial"
            trial.mkdir()
            copied = runner.write_io_properties_profile(trial, source, source_profile)
            config_path = pathlib.Path(copied["config_path"])
            self.assertEqual(copied["source_sha256"], hashlib.sha256(source.read_bytes()).hexdigest())
            self.assertEqual(copied["copied_sha256"], copied["source_sha256"])
            self.assertEqual(config_path.read_bytes(), source.read_bytes())
            self.assertEqual(config_path.stat().st_mode & 0o222, 0)
            self.assertEqual(copied["container_path"], runner.IO_PROPERTIES_CONTAINER_PATH)
            self.assertIn("provenance", copied)

            formal = runner.create_argument_parser().parse_args([
                "--mode", "formal", "--io-properties-file", str(source)])
            with self.assertRaisesRegex(runner.RunnerError, "use pilot mode"):
                runner.resolve_run_parameters(formal)

            pilot = runner.create_argument_parser().parse_args([
                "--mode", "pilot", "--node-count", "1", "--replication-factor", "1",
                "--read-cl", "1", "--write-cl", "1", "--io-properties-file", str(source),
                "--shards", "1", "--workers", "128", "--per-shard-pool", "3",
            ])
            resolved = runner.resolve_run_parameters(pilot)
            self.assertTrue(resolved["io_properties_profile"]["enabled"])
            self.assertEqual(resolved["io_properties_profile"]["source_sha256"], copied["source_sha256"])
            self.assertEqual(resolved["node_names"], ("nodea",))


def _valid_memory_lwt_result(case: runner.TrialCase, users: int = 64,
                             requests_per_user: int = 20,
                             *, explicit_read_zeros: bool = False) -> dict[str, object]:
    total = users * requests_per_user
    profile = {
        "wallet_profile": runner.WALLET_PROFILE,
        "account_state_source": "memory",
        "ledger_key": list(runner.LEDGER_KEY),
        "serial_cl": runner.SERIAL_CL,
        "cql_requests_per_transaction": 1,
        "cql_statement_count": 1,
    }
    counts = {"cql_write_lwt": total, "accepted_requests": total}
    if explicit_read_zeros:
        counts.update({"cql_read_metadata": 0, "cql_read_ledger": 0})
    config = {
        **profile, "users": users,
        "node_count": case.node_count,
        "nodes": [f"{node}.example:9042"
                  for node in runner.node_names_for(case.node_count)],
        "account_states_preloaded": users,
        "state_preload_concurrency": case.setup_concurrency,
        "measured_select_requests": 0,
        "measured_cql_requests_per_transaction": 1,
        "total_requests_expected": total,
        "queue_capacity": runner.QUEUE_CAPACITY,
        "global_admission_capacity": runner.QUEUE_CAPACITY,
        "C": case.workers, "S": case.shards, "P": case.per_shard_pool,
        "data_connection_target": case.data_connection_target,
        "worker_routing": case.worker_routing,
        "per_shard_pool_overridden": case.per_shard_pool_overridden,
        "requested_per_shard_pool": case.requested_per_shard_pool,
        "setup_concurrency": case.setup_concurrency,
        "per_worker_queue_capacity": case.per_worker_queue_capacity,
        "replication_factor": case.replication_factor,
        "read_cl": case.read_cl, "write_cl": case.write_cl,
    }
    pool = {
        "C": case.workers, "S": case.shards, "P": case.per_shard_pool,
        "target_total_data_connections": case.data_connection_target,
        "worker_routing": case.worker_routing,
        "per_shard_pool_overridden": case.per_shard_pool_overridden,
        "requested_per_shard_pool": case.requested_per_shard_pool,
        "setup_concurrency": case.setup_concurrency,
        "pool_ready_connections": case.data_connection_target,
        "pool_after_last_reply": case.data_connection_target,
        "minimum_sampled_connections": case.data_connection_target,
        "maximum_sampled_connections": case.data_connection_target,
        "valid": True, "mismatch_samples": 0,
    }
    return {
        "valid": True, "config": config,
        "requests": {"accepted": total, "expected": total,
                     "replies_received": total, "attempted": total, "applied": total,
                     "query_accounting": {
            "measured_select_requests": 0,
            "measured_cql_request_count": total,
            "cql_requests_per_transaction": 1,
            "cql_statement_count": 1,
        }},
        "driver_counters": {"delta": {"queries": total, "errors": 0}},
        "stage_metrics": {"counters": counts,
                          "cql_in_flight_peak": min(case.workers, total)},
        "connection_pool": pool,
        "verification": {
            "complete": True, "valid": True,
            "account_state_source": "memory_cache_vs_scylla",
            "memory_cache_accounts_compared": users,
            "accounts_expected": users, "accounts_verified": users,
            "ledger_rows_expected": total, "ledger_rows_seen": total,
            "failures": 0,
        },
    }


class WorkerPoolProfileTests(unittest.TestCase):
    def setUp(self) -> None:
        self.case = runner.build_matrix((4,), (50_000,), 1, per_shard_pool=4,
                                        worker_routing="per-user")[0]

    def test_per_user_arguments_and_workload_are_validated_and_forwarded(self) -> None:
        parser = runner.create_argument_parser()
        args = parser.parse_args([
            "--mode", "formal", "--shards", "4", "--workers", "50000",
            "--per-shard-pool", "4", "--worker-routing", "per-user",
            "--users", "50000", "--requests-per-user", "200", "--repeats", "1",
            "--skip-build",
        ])
        parameters = runner.resolve_run_parameters(args)
        self.assertEqual(parameters["workload_class"], "custom_workload")
        self.assertFalse(parameters["exact_formal_candidate"])
        case, = parameters["cases"]
        self.assertEqual((case.workers, case.per_shard_pool, case.data_connection_target),
                         (50_000, 4, 48))
        command = runner._cli_command(
            pathlib.Path("bench"), ("a:9042", "b:9042", "c:9042"),
            "wallet_bench_case", case, 50_000, 200, runner.QUEUE_CAPACITY,
            "formal", pathlib.Path("service"), pathlib.Path("start.gate"),
            pathlib.Path("finish.gate"))
        self.assertIn(["--per-shard-pool", "4"],
                      [command[index:index + 2] for index, token in enumerate(command)
                       if token == "--per-shard-pool"])
        self.assertIn(["--worker-routing", "per-user"],
                      [command[index:index + 2] for index, token in enumerate(command)
                       if token == "--worker-routing"])
        self.assertEqual(command[command.index("--db-workers") + 1], "50000")
        self.assertEqual(command[command.index("--queue-capacity") + 1], "50000")

    def test_legacy_profile_omits_new_cli_flags(self) -> None:
        case = runner.build_matrix((4,), (48,), 1)[0]
        command = runner._cli_command(
            pathlib.Path("bench"), ("a:9042", "b:9042", "c:9042"),
            "wallet_bench_case", case, 50_000, 200, runner.QUEUE_CAPACITY,
            "pilot", pathlib.Path("service"), pathlib.Path("start.gate"),
            pathlib.Path("finish.gate"))
        self.assertNotIn("--per-shard-pool", command)
        self.assertNotIn("--worker-routing", command)
        self.assertNotIn("--replication-factor", command)
        self.assertNotIn("--read-cl", command)
        self.assertNotIn("--write-cl", command)

    def test_rf1_cl1_profile_forwards_numeric_cli_values(self) -> None:
        case, = runner.build_matrix((4,), (50_000,), 1, 4, "per-user",
                                    replication_factor=1, read_cl=1, write_cl=1)
        command = runner._cli_command(
            pathlib.Path("bench"), ("a:9042", "b:9042", "c:9042"),
            "wallet_bench_case", case, 50_000, 200, runner.QUEUE_CAPACITY,
            "formal", pathlib.Path("service"), pathlib.Path("start.gate"),
            pathlib.Path("finish.gate"))
        self.assertEqual(command[command.index("--replication-factor") + 1], "1")
        self.assertEqual(command[command.index("--read-cl") + 1], "1")
        self.assertEqual(command[command.index("--write-cl") + 1], "1")
        self.assertEqual(command[command.index("--db-workers") + 1], "50000")

    def test_per_user_requires_one_worker_count_equal_to_user_count(self) -> None:
        args = runner.create_argument_parser().parse_args([
            "--mode", "formal", "--shards", "4", "--workers", "49999",
            "--per-shard-pool", "4", "--worker-routing", "per-user",
            "--users", "50000", "--requests-per-user", "200", "--repeats", "1",
        ])
        with self.assertRaisesRegex(runner.RunnerError, "requires exactly one workers value equal to users"):
            runner.resolve_run_parameters(args)

    def test_ready_event_requires_exact_explicit_connection_profile(self) -> None:
        event = {
            "wallet_profile": runner.WALLET_PROFILE,
            "account_state_source": "memory",
            "ledger_key": list(runner.LEDGER_KEY), "serial_cl": runner.SERIAL_CL,
            "cql_requests_per_transaction": 1, "cql_statement_count": 1,
            "pool_ready_connections": 48, "C": 50_000, "S": 4, "P": 4,
            "data_connection_target": 48, "worker_routing": "per-user",
            "per_shard_pool_overridden": True, "requested_per_shard_pool": 4,
            "setup_concurrency": 48, "per_worker_queue_capacity": 1,
        }
        evidence = runner.validate_ready_event_profile(event, self.case)
        self.assertEqual(evidence["D"], 48)
        self.assertEqual(evidence["pool_ready_connections"], 48)
        event["data_connection_target"] = 50_000
        with self.assertRaisesRegex(runner.RunnerError, "data_connection_target"):
            runner.validate_ready_event_profile(event, self.case)

    def test_every_flushed_event_must_keep_the_same_explicit_profile(self) -> None:
        profile = {
            "wallet_profile": runner.WALLET_PROFILE,
            "account_state_source": "memory",
            "ledger_key": list(runner.LEDGER_KEY), "serial_cl": runner.SERIAL_CL,
            "cql_requests_per_transaction": 1, "cql_statement_count": 1,
            "C": 50_000, "S": 4, "P": 4, "data_connection_target": 48,
            "worker_routing": "per-user", "per_shard_pool_overridden": True,
            "requested_per_shard_pool": 4, "setup_concurrency": 48,
            "per_worker_queue_capacity": 1,
        }
        evidence = runner.validate_cli_event_profiles(
            [{"phase": "ready_to_measure", **profile},
             {"phase": "client_end", "metadata": profile}], self.case)
        self.assertEqual(evidence["events_checked"], 2)
        with self.assertRaisesRegex(runner.RunnerError, "client_end event P"):
            runner.validate_cli_event_profiles(
                [{"phase": "ready_to_measure", **profile},
                 {"phase": "client_end", **{**profile, "P": 8}}], self.case)
        missing = dict(profile)
        del missing["S"]
        with self.assertRaisesRegex(runner.RunnerError, "missing required profile field S"):
            runner.validate_cli_event_profiles([{"phase": "measuring", **missing}], self.case)

    def test_final_result_validates_pool_D_and_worker_peak_C(self) -> None:
        result = _valid_memory_lwt_result(self.case, users=64, requests_per_user=20)
        result["stage_metrics"]["cql_in_flight_peak"] = 49_999
        evidence = runner.validate_cli_result_profile(result, self.case)
        self.assertTrue(evidence["valid"])
        result["connection_pool"]["pool_after_last_reply"] = 50_000
        with self.assertRaisesRegex(runner.RunnerError, "pool_after_last_reply"):
            runner.validate_cli_result_profile(result, self.case)
        result["connection_pool"]["pool_after_last_reply"] = 48
        result["stage_metrics"]["cql_in_flight_peak"] = 50_001
        with self.assertRaisesRegex(runner.RunnerError, "exceeds worker limit C"):
            runner.validate_cli_result_profile(result, self.case)

    def test_rf1_cl1_requires_matching_durability_metadata_in_ready_events_and_result(self) -> None:
        case, = runner.build_matrix((4,), (50_000,), 1, 4, "per-user",
                                    replication_factor=1, read_cl=1, write_cl=1)
        profile = {
            "wallet_profile": runner.WALLET_PROFILE,
            "account_state_source": "memory",
            "ledger_key": list(runner.LEDGER_KEY), "serial_cl": runner.SERIAL_CL,
            "cql_requests_per_transaction": 1, "cql_statement_count": 1,
            "C": 50_000, "S": 4, "P": 4, "data_connection_target": 48,
            "worker_routing": "per-user", "per_shard_pool_overridden": True,
            "requested_per_shard_pool": 4, "setup_concurrency": 48,
            "per_worker_queue_capacity": 1, "replication_factor": 1,
            "read_cl": "ONE", "write_cl": "ONE",
        }
        ready = {"pool_ready_connections": 48, **profile}
        self.assertEqual(runner.validate_ready_event_profile(ready, case)["read_cl"], "ONE")
        with self.assertRaisesRegex(runner.RunnerError, "missing required durability field read_cl"):
            runner.validate_ready_event_profile(
                {key: value for key, value in ready.items() if key != "read_cl"}, case)
        bad_write = {**ready, "write_cl": "THREE"}
        with self.assertRaisesRegex(runner.RunnerError, "write_cl='THREE'"):
            runner.validate_ready_event_profile(bad_write, case)
        self.assertTrue(runner.validate_cli_event_profiles(
            [{"phase": "ready_to_measure", **profile},
             {"phase": "verified", **profile}], case)["valid"])
        with self.assertRaisesRegex(runner.RunnerError, "verified event replication_factor"):
            runner.validate_cli_event_profiles(
                [{"phase": "verified", **{**profile, "replication_factor": 3}}], case)

        result = _valid_memory_lwt_result(case, users=50_000, requests_per_user=200)
        self.assertEqual(runner.validate_cli_result_profile(result, case)["replication_factor"], 1)
        del result["config"]["replication_factor"]
        with self.assertRaisesRegex(runner.RunnerError,
                                   "missing required durability field replication_factor"):
            runner.validate_cli_result_profile(result, case)

    def test_dry_run_and_case_identity_include_memory_lwt_contract(self) -> None:
        args = runner.create_argument_parser().parse_args([
            "--mode", "smoke", "--shards", "1", "--workers", "3",
            "--users", "64", "--requests-per-user", "20", "--skip-build", "--dry-run",
        ])
        parameters = runner.resolve_run_parameters(args)
        with mock.patch.object(runner, "host_inventory", return_value={
                "aio_max_nr": 1_000_000, "aio_nr": 0, "disk_available_bytes": 100_000}):
            report = runner.dry_run_report(args, parameters)
        case, = parameters["cases"]
        command, = report["commands"]
        self.assertIn(runner.WALLET_PROFILE, case.case_id)
        self.assertEqual(report["wallet_profile"], "wallet-memory-lwt-v1")
        self.assertEqual(command["wallet_profile"], "wallet-memory-lwt-v1")
        self.assertEqual(command["serial_cl"], "LOCAL_SERIAL")
        self.assertEqual(command["ledger_key"], ["account_id", "kind", "tx_id"])
        self.assertEqual(command["cql_requests_per_transaction"], 1)
        self.assertEqual(command["cql_statement_count"], 1)


class MemoryLwtContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.case, = runner.build_matrix((1,), (3,), 1)

    def _profile(self, source: str) -> dict[str, object]:
        return {
            "wallet_profile": runner.WALLET_PROFILE,
            "account_state_source": source,
            "ledger_key": ["account_id", "kind", "tx_id"],
            "serial_cl": "LOCAL_SERIAL",
            "cql_requests_per_transaction": 1,
            "cql_statement_count": 1,
        }

    def test_valid_result_accepts_omitted_or_explicit_zero_read_counters(self) -> None:
        for explicit in (False, True):
            with self.subTest(explicit_read_zeros=explicit):
                result = _valid_memory_lwt_result(
                    self.case, users=64, requests_per_user=20,
                    explicit_read_zeros=explicit)
                evidence = runner.validate_cli_result_profile(result, self.case)
                self.assertTrue(evidence["valid"])
                self.assertEqual(evidence["lwt_contract"]["measured_lwt_requests"], 1280)
                self.assertEqual(evidence["lwt_contract"]["measured_select_requests"], 0)

    def test_measured_selects_or_ordinary_batches_invalidate_lwt_result(self) -> None:
        for counter_name in ("cql_read_ledger", "cql_preload_state",
                             "cql_verify_metadata", "cql_verify_ledger"):
            with self.subTest(counter=counter_name):
                result = _valid_memory_lwt_result(self.case)
                result["stage_metrics"]["counters"][counter_name] = 1
                with self.assertRaisesRegex(runner.RunnerError, counter_name):
                    runner.validate_lwt_result_contract(result, self.case, 64, 20)
        result = _valid_memory_lwt_result(self.case)
        result["stage_metrics"]["counters"]["cql_write_batch"] = 1
        with self.assertRaisesRegex(runner.RunnerError, "ordinary CQL batches"):
            runner.validate_lwt_result_contract(result, self.case, 64, 20)

    def test_lwt_result_requires_one_query_per_accepted_request_and_cache_verify(self) -> None:
        result = _valid_memory_lwt_result(self.case)
        result["driver_counters"]["delta"]["queries"] -= 1
        with self.assertRaisesRegex(runner.RunnerError, "one LWT query per accepted request"):
            runner.validate_lwt_result_contract(result, self.case, 64, 20)
        result = _valid_memory_lwt_result(self.case)
        result["verification"]["memory_cache_accounts_compared"] = 63
        with self.assertRaisesRegex(runner.RunnerError, "memory_cache_accounts_compared"):
            runner.validate_lwt_result_contract(result, self.case, 64, 20)

    def test_preload_is_complete_before_ready_and_events_switch_to_memory_state(self) -> None:
        db_profile = self._profile("db")
        memory_profile = self._profile("memory")
        events = [
            {"phase": "startup", **db_profile},
            {"phase": "seeding", **db_profile},
            {"phase": "preloading", "status": "started",
             "state_preload_concurrency": 3, **db_profile},
            {"phase": "preloading", "status": "complete",
             "state_preload_concurrency": 3, "account_states_preloaded": 64,
             **db_profile},
            {"phase": "ready_to_measure", **memory_profile},
            {"phase": "measuring", **memory_profile},
            {"phase": "verified", **memory_profile},
        ]
        evidence = runner.validate_cli_event_profiles(events, self.case, 64, 20)
        self.assertTrue(evidence["preloading"]["valid"])
        self.assertEqual(evidence["preloading"]["account_states_preloaded"], 64)
        invalid = [dict(event) for event in events]
        invalid[3]["account_states_preloaded"] = 63
        with self.assertRaisesRegex(runner.RunnerError, "expected 64"):
            runner.validate_cli_event_profiles(invalid, self.case, 64, 20)
        invalid = [dict(event) for event in events]
        invalid[-1]["account_state_source"] = "db"
        with self.assertRaisesRegex(runner.RunnerError, "must use account_state_source=memory"):
            runner.validate_cli_event_profiles(invalid, self.case, 64, 20)

    def test_wallet_schema_requires_the_memory_lwt_primary_key_and_no_transaction_time(self) -> None:
        rows = [{"column_name": name, "type": shape[0], "kind": shape[1],
                 "position": shape[2]}
                for name, shape in runner.WALLET_COLUMN_SCHEMA.items()]
        query = {"query": "SELECT JSON wallet columns", "consistency": "ONE",
                 "return_code": 0, "raw_output": "\n".join(json.dumps(row) for row in rows),
                 "stderr": ""}
        result = runner.validate_wallet_schema("wallet_bench_test", query)
        self.assertTrue(result["valid"], result["problems"])
        self.assertEqual(result["expected_primary_key"], "((account_id),kind,tx_id)")
        rows.append({"column_name": "transaction_at", "type": "bigint",
                     "kind": "regular", "position": -1})
        query["raw_output"] = "\n".join(json.dumps(row) for row in rows)
        result = runner.validate_wallet_schema("wallet_bench_test", query)
        self.assertFalse(result["valid"])
        self.assertTrue(any("transaction_at" in problem for problem in result["problems"]))


class WorkerPoolParameterTests(unittest.TestCase):
    def test_dry_run_keeps_C_P_D_and_routing_separate(self) -> None:
        args = runner.create_argument_parser().parse_args([
            "--mode", "formal", "--shards", "4", "--workers", "50000",
            "--per-shard-pool", "4", "--worker-routing", "per-user",
            "--users", "50000", "--requests-per-user", "200", "--repeats", "1",
            "--skip-build",
        ])
        parameters = runner.resolve_run_parameters(args)
        with mock.patch.object(runner, "host_inventory", return_value={
                "aio_max_nr": 1_000_000, "aio_nr": 0, "disk_available_bytes": 100_000}):
            report = runner.dry_run_report(args, parameters)
        command, = report["commands"]
        self.assertEqual((command["worker_inflight_limit_C"],
                          command["per_shard_pool_P"],
                          command["target_data_connections_total_D"],
                          command["expected_data_connections_per_node"],
                          command["worker_routing"],
                          command["per_worker_queue_capacity"],
                          command["global_admission_capacity"]),
                         (50_000, 4, 48, 16, "per-user", 1, 50_000))
        self.assertIn(["--per-shard-pool", "4"],
                      [command["command"][index:index + 2]
                       for index, token in enumerate(command["command"])
                       if token == "--per-shard-pool"])
        self.assertIn(["--worker-routing", "per-user"],
                      [command["command"][index:index + 2]
                       for index, token in enumerate(command["command"])
                       if token == "--worker-routing"])
        self.assertEqual(report["workload_class"], "custom_workload")

    def test_rf1_cl1_dry_run_is_one_custom_case_and_adjusts_disk_estimate(self) -> None:
        args = runner.create_argument_parser().parse_args([
            "--mode", "formal", "--shards", "4", "--workers", "50000",
            "--per-shard-pool", "4", "--worker-routing", "per-user",
            "--users", "50000", "--requests-per-user", "200", "--repeats", "1",
            "--replication-factor", "1", "--read-cl", "1", "--write-cl", "1",
            "--skip-build",
        ])
        parameters = runner.resolve_run_parameters(args)
        with mock.patch.object(runner, "host_inventory", return_value={
                "aio_max_nr": 1_000_000, "aio_nr": 0, "disk_available_bytes": 100_000}):
            report = runner.dry_run_report(args, parameters)
        command, = report["commands"]
        for flag, expected in (("--replication-factor", "1"), ("--read-cl", "1"),
                               ("--write-cl", "1")):
            self.assertEqual(command["command"][command["command"].index(flag) + 1], expected)
        self.assertEqual((command["replication_factor"], command["read_cl"], command["write_cl"]),
                         (1, "ONE", "ONE"))
        self.assertEqual((command["target_data_connections_total_D"],
                          command["expected_data_connections_per_node"]), (48, 16))
        self.assertEqual(report["matrix_case_count"], 1)
        self.assertEqual(report["workload_class"], "custom_workload")
        self.assertFalse(report["formal_complete"])
        rf1 = report["capacity_estimate"]
        rf3 = runner.estimate_disk_bytes(50_000, 200, 3)
        self.assertEqual(rf1["replication_factor"], 1)
        self.assertEqual(rf1["estimated_replicated_bytes"], rf1["estimated_logical_bytes"])
        self.assertEqual(rf3["estimated_replicated_bytes"], 3 * rf1["estimated_logical_bytes"])
        self.assertEqual(rf1["estimated_commitlog_bytes"], rf3["estimated_commitlog_bytes"])
        self.assertLess(rf1["required_with_reserve_bytes"], rf3["required_with_reserve_bytes"])

    def test_invalid_rf_cl_arguments_are_rejected_during_parameter_resolution(self) -> None:
        invalid_profiles = (
            ("0", "1", "1", "replication-factor"),
            ("4", "1", "1", "replication-factor"),
            ("1", "2", "1", "read-cl TWO exceeds replication-factor 1"),
            ("1", "1", "3", "write-cl THREE exceeds replication-factor 1"),
        )
        for rf, read_cl, write_cl, message in invalid_profiles:
            with self.subTest(rf=rf, read_cl=read_cl, write_cl=write_cl):
                args = runner.create_argument_parser().parse_args([
                    "--replication-factor", rf, "--read-cl", read_cl, "--write-cl", write_cl,
                    "--mode", "smoke",
                ])
                with self.assertRaisesRegex(runner.RunnerError, message):
                    runner.resolve_run_parameters(args)


class ProfileReportTests(unittest.TestCase):
    def test_formal_completion_requires_current_wallet_profile_on_all_27_trials(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = pathlib.Path(tmp) / "run"
            run_dir.mkdir()
            args = runner.create_argument_parser().parse_args(["--mode", "formal"])
            parameters = runner.resolve_run_parameters(args)
            trials = [{
                "order": case.order,
                "trial_id": f"trial-{case.order:03d}-{case.case_id}",
                "status": "passed", "cleanup_status": "clean",
                "required_metrics_valid": True,
                "wallet_profile": runner.WALLET_PROFILE,
                "shards": case.shards, "workers": case.workers,
                "per_shard_pool": case.per_shard_pool,
                "replication_factor": case.replication_factor,
                "read_cl": case.read_cl, "write_cl": case.write_cl,
            } for case in parameters["cases"]]
            complete = runner.write_run_reports(run_dir, args, parameters, trials, {"valid": True})
            self.assertTrue(complete["formal_complete"])
            trials[0]["wallet_profile"] = "legacy-3-cql-v0"
            mixed = runner.write_run_reports(run_dir, args, parameters, trials, {"valid": True})
            self.assertFalse(mixed["formal_complete"])

    def test_case_summary_keeps_distinct_pool_profiles_separate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = pathlib.Path(tmp) / "run"
            run_dir.mkdir()
            cases = [
                runner.build_matrix((4,), (50_000,), 1, 4, "per-user")[0],
                runner.build_matrix((4,), (50_000,), 1, 5, "per-user")[0],
            ]
            trials = [{
                "order": index, "trial_id": case.case_id, "status": "failed",
                "mode": "formal", "shards": case.shards, "workers": case.workers,
                "C": case.workers, "per_shard_pool": case.per_shard_pool,
                "P": case.per_shard_pool, "D": case.data_connection_target,
                "replication_factor": case.replication_factor,
                "read_cl": case.read_cl, "write_cl": case.write_cl,
                "worker_routing": case.worker_routing,
                "per_shard_pool_overridden": case.per_shard_pool_overridden,
                "requested_per_shard_pool": case.requested_per_shard_pool,
                "expected_data_connections_per_node": case.expected_connections_per_node,
                "setup_concurrency": case.setup_concurrency,
                "per_worker_queue_capacity": 1,
                "repeat": case.repeat, "users": 50_000, "requests_per_user": 200,
                "total_requests": 10_000_000, "cleanup_status": "clean",
                "required_metrics_valid": False,
            } for index, case in enumerate(cases, 1)]
            args = runner.create_argument_parser().parse_args(["--mode", "formal"])
            parameters = {
                "cases": cases, "exact_formal_candidate": False,
                "workload_class": "custom_workload", "shards": (4,),
                "workers": (50_000,), "per_shard_pool_override": 4,
                "worker_routing": "per-user", "users": 50_000,
                "replication_factor": 3, "read_cl": "THREE", "write_cl": "THREE",
                "requests_per_user": 200,
            }
            summary = runner.write_run_reports(run_dir, args, parameters, trials, None)
            with (run_dir / "case-summary.csv").open(encoding="utf-8", newline="") as source:
                rows = list(csv.DictReader(source))
            self.assertEqual(len(rows), 2)
            self.assertEqual([row["data_connections_D"] for row in rows], ["48", "60"])
            self.assertEqual([row["per_shard_pool_P"] for row in rows], ["4", "5"])
            self.assertEqual([row["worker_routing"] for row in rows], ["per-user", "per-user"])
            self.assertFalse(summary["formal_complete"])
            self.assertEqual(summary["worker_connection_profiles"][0]["D"], 48)

    def test_case_summary_keeps_rf_and_consistency_profiles_separate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = pathlib.Path(tmp) / "run"
            run_dir.mkdir()
            default, = runner.build_matrix((4,), (50_000,), 1, 4, "per-user")
            rf1, = runner.build_matrix((4,), (50_000,), 1, 4, "per-user",
                                       replication_factor=1, read_cl=1, write_cl=1)
            cases = [default, rf1]
            trials = [{
                "order": index, "trial_id": case.case_id, "status": "failed",
                "mode": "formal", "shards": case.shards, "workers": case.workers,
                "C": case.workers, "per_shard_pool": case.per_shard_pool,
                "P": case.per_shard_pool, "D": case.data_connection_target,
                "replication_factor": case.replication_factor,
                "read_cl": case.read_cl, "write_cl": case.write_cl,
                "worker_routing": case.worker_routing,
                "per_shard_pool_overridden": case.per_shard_pool_overridden,
                "requested_per_shard_pool": case.requested_per_shard_pool,
                "repeat": case.repeat, "cleanup_status": "clean",
                "required_metrics_valid": False,
            } for index, case in enumerate(cases, 1)]
            parameters = {
                "cases": cases, "exact_formal_candidate": False,
                "workload_class": "custom_workload", "shards": (4,),
                "workers": (50_000,), "per_shard_pool_override": 4,
                "worker_routing": "per-user", "users": 50_000,
                "requests_per_user": 200, "replication_factor": 3,
                "read_cl": "THREE", "write_cl": "THREE",
            }
            runner.write_run_reports(run_dir, runner.create_argument_parser().parse_args(
                ["--mode", "formal"]), parameters, trials, None)
            with (run_dir / "case-summary.csv").open(encoding="utf-8", newline="") as source:
                rows = list(csv.DictReader(source))
            self.assertEqual(len(rows), 2)
            self.assertEqual({row["replication_factor"] for row in rows}, {"1", "3"})
            by_rf = {row["replication_factor"]: row for row in rows}
            self.assertEqual((by_rf["1"]["read_cl"], by_rf["1"]["write_cl"]), ("ONE", "ONE"))
            self.assertEqual((by_rf["3"]["read_cl"], by_rf["3"]["write_cl"]),
                             ("THREE", "THREE"))

    def test_case_summary_and_cql_cost_never_mix_wallet_profiles(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = pathlib.Path(tmp) / "run"
            run_dir.mkdir()
            cases = runner.build_matrix((1,), (3,), 2)
            trial_common = {
                "mode": "pilot", "shards": 1, "workers": 3, "C": 3,
                "per_shard_pool": 1, "P": 1, "D": 3,
                "replication_factor": 3, "read_cl": "THREE", "write_cl": "THREE",
                "worker_routing": "round-robin", "per_shard_pool_overridden": False,
                "requested_per_shard_pool": None, "expected_data_connections_per_node": 1,
                "setup_concurrency": 3, "per_worker_queue_capacity": runner.QUEUE_CAPACITY,
                "repeat": 1, "users": 64, "requests_per_user": 20,
                "total_requests": 1280, "cleanup_status": "clean",
                "status": "failed", "required_metrics_valid": False,
                "cli_result": {"requests": {"expected": 1280, "replies_received": 0}},
            }
            trials = [
                {**trial_common, "order": 1, "trial_id": cases[0].case_id,
                 "wallet_profile": runner.WALLET_PROFILE,
                 "account_state_source": "memory", "serial_cl": "LOCAL_SERIAL",
                 "cql_requests_per_transaction": 1, "cql_statement_count": 1},
                {**trial_common, "order": 2, "trial_id": cases[1].case_id,
                 "wallet_profile": "legacy-3-cql-v0"},
            ]
            parameters = {
                "cases": cases, "exact_formal_candidate": False,
                "workload_class": "diagnostic_pilot", "shards": (1,), "workers": (3,),
                "per_shard_pool_override": None, "worker_routing": "round-robin",
                "users": 64, "requests_per_user": 20, "replication_factor": 3,
                "read_cl": "THREE", "write_cl": "THREE",
            }
            runner.write_run_reports(
                run_dir, runner.create_argument_parser().parse_args(["--mode", "pilot"]),
                parameters, trials, None)
            with (run_dir / "case-summary.csv").open(encoding="utf-8", newline="") as source:
                grouped = list(csv.DictReader(source))
            self.assertEqual({row["wallet_profile"] for row in grouped},
                             {runner.WALLET_PROFILE, "legacy-3-cql-v0"})
            with (run_dir / "summary.csv").open(encoding="utf-8", newline="") as source:
                summary_rows = list(csv.DictReader(source))
            by_profile = {row["wallet_profile"]: row for row in summary_rows}
            self.assertEqual(by_profile[runner.WALLET_PROFILE][
                "expected_application_cql_operations"], "1280")
            self.assertEqual(by_profile["legacy-3-cql-v0"][
                "expected_application_cql_operations"], "")


class BuildTests(unittest.TestCase):
    def test_default_build_uses_run_dir_log_and_locked_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp) / "repo"
            run_dir = pathlib.Path(tmp) / "runs" / "run-test"
            crate_dir = root / "benchmarks" / "scylla-wallet"
            crate_dir.mkdir(parents=True)
            run_dir.mkdir(parents=True)
            manifest = crate_dir / "Cargo.toml"
            manifest.write_text("[package]\nname = 'fixture'\n", encoding="utf-8")
            args = runner.create_argument_parser().parse_args(["--mode", "smoke"])
            observed: dict[str, object] = {}

            def fake_cargo(argv, **kwargs):
                observed["argv"] = list(argv)
                observed["env"] = kwargs["env"]
                kwargs["stdout"].write("mock locked build\n")
                target = pathlib.Path(kwargs["env"]["CARGO_TARGET_DIR"])
                binary = target / "release" / runner.RUST_PACKAGE
                binary.parent.mkdir(parents=True)
                binary.write_text("fixture binary", encoding="utf-8")
                binary.chmod(0o755)
                return subprocess.CompletedProcess(argv, 0, "", "")

            with mock.patch.object(runner, "REPO_ROOT", root), \
                    mock.patch.object(runner, "CRATE_DIR", crate_dir), \
                    mock.patch.object(runner.subprocess, "run", side_effect=fake_cargo):
                binary_path = runner._cli_binary_path(args, run_dir)

            self.assertEqual(binary_path, (root / "target" / "scylla-wallet" / "release"
                                           / runner.RUST_PACKAGE).resolve())
            self.assertEqual(observed["argv"], [
                "cargo", "build", "--locked", "--release", "--manifest-path", str(manifest),
            ])
            self.assertEqual(observed["env"]["CARGO_TARGET_DIR"],
                             str(root / "target" / "scylla-wallet"))
            self.assertEqual((run_dir / "build.log").read_text(encoding="utf-8"),
                             "mock locked build\n")

    def test_launch_input_hashes_include_per_user_requirement_and_design(self) -> None:
        hashes = runner.source_hashes()
        for path in ("docs/01-36.raw-requirement-scylladb-per-user-worker-benchmark.md",
                     "docs/01-37.development-design-scylladb-per-user-worker-benchmark.md",
                     "docs/01-39.raw-requirement-scylladb-rf1-cl1-benchmark.md",
                     "docs/01-40.development-design-scylladb-rf1-cl1-benchmark.md",
                     "docs/01-42.raw-requirement-scylladb-memory-lwt-benchmark.md",
                     "docs/01-43.development-design-scylladb-memory-lwt-benchmark.md",
                     "docs/01-44.development-validation-scylladb-memory-lwt-benchmark.md",
                     "benches/scylla_wallet.py", "tests/test_scylla_wallet_runner.py",
                     "Cargo.toml", "Cargo.lock",
                     "benchmarks/scylla-wallet/Cargo.lock"):
            self.assertIn(path, hashes)
            self.assertEqual(hashes[path], hashlib.sha256(
                (runner.REPO_ROOT / path).read_bytes()).hexdigest())

class AffinityTests(unittest.TestCase):
    def test_s1_main_scylla_thread_is_reactor_zero(self) -> None:
        rows = [
            {"tid": 700, "comm": "scylla", "affinity_text": "0"},
            {"tid": 701, "comm": "alien-rpc-dict", "affinity_text": "0"},
            {"tid": 702, "comm": "syscall-0", "affinity_text": "0"},
        ]
        evidence = runner.validate_thread_affinity_rows(rows, 700, (0,))
        self.assertTrue(evidence["valid"], evidence["errors"])
        self.assertEqual(evidence["reactor_thread_count"], 1)

    def test_s2_main_and_named_reactor_cover_distinct_cpus(self) -> None:
        rows = [
            {"tid": 700, "comm": "scylla", "affinity_text": "0"},
            {"tid": 701, "comm": "reactor-1", "affinity_text": "1"},
            {"tid": 702, "comm": "alien-rpc-dict", "affinity_text": "0-1"},
        ]
        self.assertTrue(runner.validate_thread_affinity_rows(rows, 700, (0, 1))["valid"])
        rows[1]["affinity_text"] = "0-1"
        invalid = runner.validate_thread_affinity_rows(rows, 700, (0, 1))
        self.assertFalse(invalid["valid"])
        self.assertTrue(any("single-CPU" in error for error in invalid["errors"]))


class ScyllaProcessResolutionTests(unittest.TestCase):
    def _record(self) -> runner.ContainerRecord:
        return runner.ContainerRecord("owned-nodea", "nodea", "container-id",
                                      "172.24.0.2", "0")

    def test_selects_exact_scylla_executable_among_housekeeping_and_exporter(self) -> None:
        output = (
            "PID ARGS\n"
            "17208 /usr/bin/scylla --options-file /etc/scylla/scylla.yaml\n"
            "17209 /bin/bash /scylla-housekeeping-service.sh\n"
            "17211 /opt/scylladb/node_exporter/node_exporter --web.listen-address=:9100\n")
        with mock.patch.object(runner, "_run", return_value=subprocess.CompletedProcess(
                ["docker", "top"], 0, output, "")):
            record = self._record()
            self.assertEqual(runner.resolve_scylla_host_pid(record), 17208)
        self.assertEqual(record.host_process_cmd,
                         "/usr/bin/scylla --options-file /etc/scylla/scylla.yaml")

    def test_no_exact_scylla_executable_is_an_error(self) -> None:
        output = ("PID ARGS\n"
                  "17209 /bin/bash /scylla-housekeeping-service.sh\n"
                  "17211 /opt/scylladb/node_exporter/node_exporter\n")
        with mock.patch.object(runner, "_run", return_value=subprocess.CompletedProcess(
                ["docker", "top"], 0, output, "")):
            with self.assertRaisesRegex(runner.RunnerError, "expected one Scylla process"):
                runner.resolve_scylla_host_pid(self._record())

    def test_multiple_exact_scylla_executables_are_an_error(self) -> None:
        output = ("PID ARGS\n"
                  "17208 /usr/bin/scylla --options-file /etc/scylla/scylla.yaml\n"
                  "17210 /opt/scylla/bin/scylla --version\n")
        with mock.patch.object(runner, "_run", return_value=subprocess.CompletedProcess(
                ["docker", "top"], 0, output, "")):
            with self.assertRaisesRegex(runner.RunnerError, "expected one Scylla process"):
                runner.resolve_scylla_host_pid(self._record())


class KeyspaceValidationTests(unittest.TestCase):
    keyspace = "wallet_bench_fixture"

    def _query(self, raw: str, return_code: int = 0, stderr: str = "") -> dict[str, object]:
        return {"query": "fixture query", "consistency": "ONE", "return_code": return_code,
                "raw_output": raw, "stderr": stderr}

    def _keyspace_row(self, replication: object | None = None,
                      durable_writes: object = True) -> str:
        if replication is None:
            replication = {"class": "NetworkTopologyStrategy", "datacenter1": "3"}
        return json.dumps({"durable_writes": durable_writes, "replication": replication})

    def _validate(self, tablet_output: str | None = None, replication: object | None = None,
                  keyspace_output: str | None = None, tablet_exit: int = 0,
                  tablet_stderr: str = "", keyspace_exit: int = 0,
                  durable_writes: object = True, replication_factor: int = 3):
        if tablet_output is None:
            tablet_output = ("Consistency level set to ONE.\n\n"
                             " [json]\n--------\n\n(0 rows)\n")
        main = self._query(keyspace_output if keyspace_output is not None else
                           self._keyspace_row(replication, durable_writes))
        main["return_code"] = keyspace_exit
        tablets = self._query(tablet_output, tablet_exit, tablet_stderr)
        return runner.validate_keyspace_schema(self.keyspace, main, tablets,
                                               replication_factor=replication_factor)

    def test_nested_replication_json_is_one_top_level_keyspace_row(self) -> None:
        raw = self._keyspace_row({"class": "NetworkTopologyStrategy", "datacenter1": "3"})
        rows = runner.parse_cql_json_rows(raw)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["replication"]["datacenter1"], "3")
        self.assertTrue(self._validate(keyspace_output=raw)["valid"])

    def test_absent_or_null_initial_tablets_means_disabled(self) -> None:
        absent = self._validate(tablet_output=("Consistency level set to ONE.\n\n"
                                               " [json]\n--------\n\n(0 rows)\n"))
        null_row = self._validate(tablet_output='{"initial_tablets": null}\n')
        self.assertTrue(absent["valid"], absent["problems"])
        self.assertTrue(null_row["valid"], null_row["problems"])
        self.assertEqual(absent["interpretation"]["tablets_interpretation"],
                         "disabled: no scylla_keyspaces row; optional initial_tablets is absent")
        self.assertTrue(null_row["interpretation"]["tablets_disabled"])

    def test_blank_successful_tablets_query_cannot_confirm_disabled(self) -> None:
        result = self._validate(tablet_output="   \n\t")
        self.assertFalse(result["valid"])
        self.assertFalse(result["interpretation"]["tablets_disabled"])
        self.assertTrue(any("blank output" in problem for problem in result["problems"]))

    def test_real_cqlsh_zero_rows_footer_confirms_absent_tablets_metadata(self) -> None:
        raw = ("Consistency level set to ONE.\n\n"
               " initial_tablets\n"
               "-----------------\n\n"
               "(0 rows)\n")
        result = self._validate(tablet_output=raw)
        self.assertTrue(result["valid"], result["problems"])
        self.assertTrue(result["queries"]["tablet_metadata"]["zero_rows_footer"])
        self.assertTrue(result["interpretation"]["tablets_disabled"])

    def test_non_json_tablet_output_without_zero_rows_footer_is_invalid(self) -> None:
        result = self._validate(tablet_output="cqlsh returned no table metadata\n")
        self.assertFalse(result["valid"])
        self.assertTrue(any("non-JSON output without rows" in problem
                            for problem in result["problems"]))

    def test_zero_enabled_value_and_other_nonnull_values_are_rejected(self) -> None:
        for output in ('{"initial_tablets": 0}', '{"initial_tablets": 16}',
                       '{"initial_tablets": "0"}', '{"initial_tablets": false}', '{}',
                       '{"initial_tablets": null}\n{"initial_tablets": null}'):
            with self.subTest(output=output):
                result = self._validate(tablet_output=output)
                self.assertFalse(result["valid"])
                self.assertIsNotNone(result["interpretation"]["tablets_interpretation"])

    def test_unknown_scylla_keyspaces_schema_or_failed_query_is_invalid(self) -> None:
        result = self._validate(tablet_output="Unrecognized name initial_tablets", tablet_exit=1,
                                tablet_stderr="InvalidRequest: Unrecognized name initial_tablets")
        self.assertFalse(result["valid"])
        self.assertTrue(any("metadata query failed" in problem for problem in result["problems"]))
        self.assertIn("Unrecognized name", result["queries"]["tablet_metadata"]["raw_output"])

    def test_keyspace_query_requires_one_row_durable_writes_and_exact_rf3_dc(self) -> None:
        for raw in ("", self._keyspace_row() + "\n" + self._keyspace_row()):
            with self.subTest(raw=raw):
                self.assertFalse(self._validate(keyspace_output=raw)["valid"])
        self.assertFalse(self._validate(durable_writes=False)["valid"])
        self.assertFalse(self._validate(keyspace_exit=1)["valid"])
        self.assertFalse(self._validate(replication={
            "class": "NetworkTopologyStrategy", "datacenter1": "3", "datacenter2": "0"})["valid"])
        self.assertFalse(self._validate(replication={
            "class": "NetworkTopologyStrategy", "datacenter1": "three"})["valid"])

    def test_keyspace_schema_accepts_expected_rf1_and_rejects_rf3_mismatch(self) -> None:
        rf1 = self._validate(replication={"class": "NetworkTopologyStrategy", "datacenter1": "1"},
                             replication_factor=1)
        self.assertTrue(rf1["valid"], rf1["problems"])
        self.assertEqual(rf1["interpretation"]["replication_factor_expected"], 1)
        self.assertFalse(self._validate(
            replication={"class": "NetworkTopologyStrategy", "datacenter1": "3"},
            replication_factor=1)["valid"])
        self.assertFalse(self._validate(
            replication={"class": "NetworkTopologyStrategy", "datacenter1": "1"},
            replication_factor=3)["valid"])

    def test_only_network_topology_strategy_names_are_accepted(self) -> None:
        for strategy in ("NetworkTopologyStrategy",
                         "org.apache.cassandra.locator.NetworkTopologyStrategy"):
            with self.subTest(strategy=strategy):
                self.assertTrue(self._validate(replication={
                    "class": strategy, "dc1": "3"})["valid"])
        self.assertFalse(self._validate(replication={
            "class": "SimpleStrategy", "replication_factor": "3"})["valid"])

    def test_verify_keyspace_persists_expected_rf_and_both_queries(self) -> None:
        main_output = self._keyspace_row(
            {"class": "NetworkTopologyStrategy", "datacenter1": "1"})
        tablets_output = '{"initial_tablets": null}\n'
        with tempfile.TemporaryDirectory() as tmp:
            trial_dir = pathlib.Path(tmp)
            record = runner.ContainerRecord("nodea", "nodea", "cid", "172.24.0.2", "0")
            cluster = mock.Mock()
            cluster.trial_dir = trial_dir
            cluster.records = [record]

            def fake_run(argv, **kwargs):
                statement = argv[-1]
                self.assertTrue(statement.startswith("CONSISTENCY ONE; SELECT JSON "))
                if "FROM system_schema.keyspaces" in statement:
                    output = main_output
                elif "FROM system_schema.scylla_keyspaces" in statement:
                    output = tablets_output
                else:
                    raise AssertionError(statement)
                return subprocess.CompletedProcess(argv, 0, output, "")

            with mock.patch.object(runner, "_run", side_effect=fake_run):
                result = runner.verify_keyspace(cluster, self.keyspace, replication_factor=1)
            self.assertTrue(result["valid"])
            saved = json.loads((trial_dir / "keyspace-validation.json").read_text())
            self.assertEqual(saved["interpretation"]["replication_factor_expected"], 1)
            queries = saved["queries"]
            self.assertEqual(queries["keyspace_options"]["raw_output"], main_output)
            self.assertEqual(queries["tablet_metadata"]["raw_output"], tablets_output)
            self.assertIn("durable_writes, replication", queries["keyspace_options"]["query"])
            self.assertNotIn("tablets", queries["keyspace_options"]["query"])
            self.assertIn("initial_tablets", queries["tablet_metadata"]["query"])
            self.assertTrue(saved["source_evidence"]["get_initial_tablets"]["url"].endswith(
                "/cql3/statements/ks_prop_defs.cc"))
            self.assertTrue(saved["source_evidence"]["persist_scylla_keyspaces"]["url"].endswith(
                "/db/schema_tables.cc"))


class RuntimeAioLogTests(unittest.TestCase):
    @staticmethod
    def _logs() -> dict[str, str]:
        return {node: f"INFO seastar - Reactor backend: linux-aio\n"
                      f"WARN init - tool io_uring is unavailable; tool falls back to epoll\n"
                for node in runner.NODE_NAMES}

    def test_runtime_aio_requires_all_three_nodes_and_actual_backend_marker(self) -> None:
        missing = self._logs()
        missing.pop("nodec")
        self.assertFalse(runner.validate_runtime_aio_startup_logs(missing)["valid"])

        blank_backend = self._logs()
        blank_backend["nodeb"] = "INFO startup completed\n"
        result = runner.validate_runtime_aio_startup_logs(blank_backend)
        self.assertFalse(result["valid"])
        self.assertTrue(any("marker is missing" in problem for problem in result["problems"]))

    def test_wrong_backend_reduction_and_minimum_aio_messages_fail(self) -> None:
        wrong = self._logs()
        wrong["nodea"] = "INFO seastar - Reactor backend: epoll\n"
        self.assertFalse(runner.validate_runtime_aio_startup_logs(wrong)["valid"])

        for message in (
                "WARN seastar - Your system does not have enough AIO capacity; "
                "reducing `max-networking-io-control-blocks'.",
                "Could not initialize seastar: system does not satisfy minimum AIO requirements."):
            with self.subTest(message=message):
                reduced = self._logs()
                reduced["nodec"] += message + "\n"
                result = runner.validate_runtime_aio_startup_logs(reduced)
                self.assertFalse(result["valid"])
                self.assertTrue(result["nodes"]["nodec"]["failure_markers"])

    def test_tool_io_uring_fallback_is_accepted_when_reactor_is_linux_aio(self) -> None:
        result = runner.validate_runtime_aio_startup_logs(self._logs())
        self.assertTrue(result["valid"], result["problems"])

    @staticmethod
    def _cluster(trial_dir: pathlib.Path) -> runner.OwnedCluster:
        cluster = runner.OwnedCluster(trial_dir, trial_dir / "owned-data", "nonce-aio",
                                      "trial-aio", 1, "run-aio")
        cluster.records = [runner.ContainerRecord(f"owned-{node}", node,
                           f"container-{node}", f"172.24.0.{i + 2}", str(i))
                           for i, node in enumerate(runner.NODE_NAMES)]
        return cluster

    def test_collector_saves_raw_streams_hashes_and_validation_before_workload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            trial_dir = pathlib.Path(tmp) / "trial"
            trial_dir.mkdir()
            cluster = self._cluster(trial_dir)
            runtime_logs = self._logs()

            def fake_run(argv, **kwargs):
                node = argv[-1].removeprefix("container-")
                return subprocess.CompletedProcess(argv, 0, runtime_logs[node], "daemon stderr\n")

            with mock.patch.object(runner, "_run", side_effect=fake_run):
                evidence = cluster.capture_and_validate_runtime_aio()

            self.assertTrue(evidence["valid"], evidence["problems"])
            self.assertTrue((trial_dir / "runtime-aio-validation.json").is_file())
            for node in runner.NODE_NAMES:
                item = evidence["nodes"][node]
                stdout_path = pathlib.Path(item["stdout_file"])
                stderr_path = pathlib.Path(item["stderr_file"])
                self.assertNotIn("owned-data", str(stdout_path))
                self.assertEqual(hashlib.sha256(stdout_path.read_bytes()).hexdigest(),
                                 item["stdout_sha256"])
                self.assertEqual(hashlib.sha256(stderr_path.read_bytes()).hexdigest(),
                                 item["stderr_sha256"])
                self.assertEqual(stderr_path.read_text(), "daemon stderr\n")

    def test_collector_fetch_failure_is_persisted_and_invalid(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            trial_dir = pathlib.Path(tmp) / "trial"
            trial_dir.mkdir()
            cluster = self._cluster(trial_dir)

            def failed_node_c(argv, **kwargs):
                if argv[-1] == "container-nodec":
                    return subprocess.CompletedProcess(argv, 1, "", "docker logs unavailable\n")
                return subprocess.CompletedProcess(argv, 0,
                    "INFO seastar - Reactor backend: linux-aio\n", "")

            with mock.patch.object(runner, "_run", side_effect=failed_node_c):
                evidence = cluster.capture_and_validate_runtime_aio()

            self.assertFalse(evidence["valid"])
            self.assertTrue(any("nodec: docker logs exited" in error
                                for error in evidence["collection_errors"]))
            self.assertTrue((trial_dir / "runtime-aio-validation.json").is_file())


class ConnectionAuditTests(unittest.TestCase):
    def test_nested_client_options_counts_each_physical_connection_once(self) -> None:
        session_id = "driver-session-abc123"

        def data_connection(port: int) -> dict[str, object]:
            return {"client_options": {
                "SESSION_ID": session_id,
                "APPLICATION_NAME": "ledger-wallet-scylla-bench",
            }, "address": "192.0.2.1", "port": port}

        def cqlsh_output(rows: list[dict[str, object]]) -> str:
            return (" client_options | address | port\n"
                    "----------------+---------+-----\n" +
                    "\n".join(json.dumps(row) for row in rows) +
                    f"\n({len(rows)} rows)\n")

        physical_rows: dict[str, list[dict[str, object]]] = {
            node: [data_connection(9100 + index) for index in range(4)]
            for node in runner.NODE_NAMES
        }
        physical_rows["nodea"].append({
            "client_options": {"SESSION_ID": session_id,
                               "DRIVER_CONFIG": {"heartbeat_interval": "30 seconds"}},
            "address": "192.0.2.1", "port": 9199,
        })
        physical_rows["nodeb"].append({
            "client_options": {"SESSION_ID": "other-session", "APPLICATION_NAME": "other"},
            "address": "192.0.2.2", "port": 9299,
        })

        by_node = {node: runner.parse_cqlsh_client_rows(cqlsh_output(rows))
                   for node, rows in physical_rows.items()}
        self.assertEqual([len(by_node[node]) for node in runner.NODE_NAMES], [5, 5, 4])
        audit = runner.audit_session_connections(by_node, session_id, workers=12)

        self.assertTrue(audit["valid"], audit["problems"])
        self.assertEqual(audit["actual_data_connections"], 12)
        self.assertEqual(audit["control_connections"], 1)
        self.assertEqual([audit["nodes"][node]["actual_data"] for node in runner.NODE_NAMES],
                         [4, 4, 4])
        self.assertEqual(audit["nodes"]["nodea"]["control_connections"], 1)
        self.assertEqual(audit["nodes"]["nodeb"]["actual_data"], 4)

    def test_explicit_s4_pool_audits_D48_independently_from_C50000(self) -> None:
        session_id = "per-user-s4-session"
        by_node = {
            node: [{
                "row": {"port": 9000 + index},
                "client_options": {"SESSION_ID": session_id,
                                   "APPLICATION_NAME": "ledger-wallet-scylla-bench"},
            } for index in range(16)]
            for node in runner.NODE_NAMES
        }
        audit = runner.audit_session_connections(
            by_node, session_id, workers=50_000,
            expected_data_per_node=16, target_data_connections=48,
            shards=4, per_shard_pool=4)
        self.assertTrue(audit["valid"], audit["problems"])
        self.assertEqual(audit["workers"], 50_000)
        self.assertEqual(audit["expected_data_connections_per_node"], 16)
        self.assertEqual(audit["target_data_connections"], 48)
        self.assertEqual(audit["actual_data_connections"], 48)


class MetricsRelabelTests(unittest.TestCase):
    def test_profile_records_yaml_hash_and_pinned_source_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            profile = runner.write_metrics_relabel_profile(pathlib.Path(tmp))
            content = pathlib.Path(profile["config_path"]).read_bytes()
            self.assertEqual(profile["sha256"], hashlib.sha256(content).hexdigest())
            self.assertEqual(profile["container_path"], runner.METRICS_RELABEL_CONTAINER_PATH)
            self.assertEqual(profile["metric_family_configs"], [])
            self.assertIn("action: report_when_empty", content.decode())
            self.assertIn("replacement: basic", content.decode())
            self.assertEqual(profile["source_evidence"]["scylla_release_commit"],
                             runner.SCYLLA_SOURCE_COMMIT)
            seastar = profile["source_evidence"]["seastar_submodule"]
            self.assertEqual(seastar["commit"], "bf882627c3f813c8cfb88cb595ceed34d6462300")
            self.assertTrue(seastar["source"]["url"].endswith("/src/core/metrics.cc"))
            self.assertEqual(seastar["source"]["sha256"],
                             runner.EXPECTED_SEASTAR_METRICS_SHA256)
            pinned_parser_fixture = (
                'const YAML::Node& relabel_configs = yaml["relabel_configs"];')
            parser_key = runner.scylla_relabel_yaml_key(pinned_parser_fixture)
            yaml_top_level_keys = {
                line.split(":", 1)[0]
                for line in content.decode().splitlines()
                if line and not line[0].isspace() and ":" in line
            }
            self.assertEqual(parser_key, "relabel_configs")
            self.assertIn(parser_key, yaml_top_level_keys)
            self.assertNotIn("relabel_rules", yaml_top_level_keys)
            self.assertEqual(
                profile["source_evidence"]["release_sources"]["db/config.cc"]["parser_expression"],
                pinned_parser_fixture)
            aio_sources = runner.scylla_aio_profile_evidence()["source_evidence"]
            self.assertEqual(aio_sources["seastar_submodule"]["commit"],
                             runner.SCYLLA_SEASTAR_COMMIT)
            self.assertTrue(aio_sources["seastar_submodule"]["reactor_source"]["url"].endswith(
                "/src/core/reactor.cc"))
            self.assertTrue(aio_sources["seastar_submodule"]["reactor_header"]["url"].endswith(
                "/include/seastar/core/reactor.hh"))
            self.assertEqual(aio_sources["scylladb_main"]["sha256"],
                             runner.EXPECTED_SOURCE_SHA256["main.cc"])

    def test_node_create_uses_trial_config_readonly_bind_and_server_flag(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            trial = pathlib.Path(tmp) / "trial"
            owned = trial / "owned-data"
            trial.mkdir()
            owned.mkdir()
            for node in runner.NODE_NAMES:
                (owned / node).mkdir()
            cluster = runner.OwnedCluster(trial, owned, "nonce-relabel", "trial-relabel",
                                          1, "run-relabel")
            cluster.network_subnet = runner.ipaddress.ip_network("172.24.0.0/16")
            calls: list[list[str]] = []

            def create_only(argv, **kwargs):
                command = list(argv)
                calls.append(command)
                return subprocess.CompletedProcess(command, 0, "owned-created-id\n", "")

            with mock.patch.object(runner, "_run", side_effect=create_only):
                cluster._create_node("nodea")
            command = calls[0]
            mounts = [command[index + 1] for index, arg in enumerate(command[:-1])
                      if arg == "--mount"]
            relabel_mount = next(value for value in mounts
                                 if runner.METRICS_RELABEL_CONTAINER_PATH in value)
            self.assertTrue(relabel_mount.endswith(
                f"dst={runner.METRICS_RELABEL_CONTAINER_PATH},readonly"))
            self.assertIn("src=" + str((trial / "metrics-relabel.yaml").resolve()), relabel_mount)
            flag_index = command.index("--relabel-config-file")
            self.assertEqual(command[flag_index + 1], runner.METRICS_RELABEL_CONTAINER_PATH)
            self.assertEqual(command[command.index("--max-networking-io-control-blocks") + 1],
                             "1024")
            self.assertEqual(command[command.index("--reserve-io-control-blocks") + 1], "1024")
            self.assertEqual(command[command.index("--reactor-backend") + 1], "linux-aio")
            self.assertEqual(command[command.index("--commitlog-sync") + 1], "batch")
            self.assertNotIn("--commitlog-sync-period-in-ms", command)

            periodic_trial = pathlib.Path(tmp) / "periodic-trial"
            periodic_trial.mkdir()
            periodic_owned = periodic_trial / "owned-data"
            periodic_owned.mkdir()
            (periodic_owned / "nodea").mkdir()
            periodic = runner.OwnedCluster(
                periodic_trial, periodic_owned, "nonce-periodic", "trial-periodic", 2,
                "run-periodic", node_count=1, commitlog_sync="periodic",
                commitlog_sync_period_in_ms=10_000)
            periodic.network_subnet = runner.ipaddress.ip_network("172.25.0.0/16")
            periodic_calls: list[list[str]] = []

            def periodic_create(argv, **kwargs):
                command = list(argv)
                periodic_calls.append(command)
                return subprocess.CompletedProcess(command, 0, "owned-periodic-id\n", "")

            with mock.patch.object(runner, "_run", side_effect=periodic_create):
                periodic._create_node("nodea")
            periodic_command = periodic_calls[0]
            self.assertEqual(periodic_command[periodic_command.index("--commitlog-sync") + 1],
                             "periodic")
            self.assertEqual(
                periodic_command[periodic_command.index("--commitlog-sync-period-in-ms") + 1],
                "10000")

    def test_launch_evidence_requires_hash_readonly_mount_and_relabel_flag(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            relabel_source = pathlib.Path(tmp) / "metrics-relabel.yaml"
            relabel_source.write_text(runner.METRICS_RELABEL_CONFIG, encoding="utf-8")
            relabel_hash = hashlib.sha256(relabel_source.read_bytes()).hexdigest()
            configs = {}
            for node, cpus in runner.CPU_ASSIGNMENTS[1].items():
                cpuset = ",".join(str(cpu) for cpu in cpus)
                configs[node] = {
                    "parsed_launch_flags": {
                        "developer_mode": "1", "io_setup": "0", "overprovisioned": "1",
                        "thread_affinity": "1", "commitlog_sync": "batch",
                        "commitlog_use_o_dsync": "false",
                        "commitlog_total_space_in_mb": "1024",
                        "commitlog_segment_size_in_mb": "64",
                        "prometheus_address": "0.0.0.0", "prometheus_port": "9180",
                        "relabel_config_file": runner.METRICS_RELABEL_CONTAINER_PATH,
                        "max_networking_io_control_blocks": "1024",
                        "reserve_io_control_blocks": "1024", "reactor_backend": "linux-aio",
                        "smp": "1", "cpuset": cpuset,
                    },
                    "expected_owned_data_source": f"/tmp/{node}-data",
                    "expected_metrics_relabel_source": str(relabel_source.resolve()),
                    "expected_metrics_relabel_sha256": relabel_hash,
                    "observed_metrics_relabel_sha256": relabel_hash,
                    "host_config": {
                        "memory_bytes": runner.NODE_CGROUP_BYTES,
                        "memory_swap_bytes": runner.NODE_CGROUP_BYTES,
                        "cpu_quota": 0, "cpu_period": 0, "nano_cpus": 0,
                        "cpuset_cpus": cpuset, "network_mode": "lw-scylla-owned",
                        "port_bindings": None,
                        "mounts": [
                            {"Source": f"/tmp/{node}-data", "Destination": "/var/lib/scylla",
                             "RW": True},
                            {"Source": str(relabel_source.resolve()),
                             "Destination": runner.METRICS_RELABEL_CONTAINER_PATH, "RW": False},
                        ],
                    },
                }
            valid = runner.validate_launch_config(configs)
            self.assertTrue(valid["valid"], valid["errors"])
            periodic_configs = json.loads(json.dumps(configs))
            for item in periodic_configs.values():
                item["parsed_launch_flags"]["commitlog_sync"] = "periodic"
                item["parsed_launch_flags"]["commitlog_sync_period_in_ms"] = "10000"
            periodic_valid = runner.validate_launch_config(
                periodic_configs, commitlog_sync="periodic",
                commitlog_sync_period_in_ms=10_000)
            self.assertTrue(periodic_valid["valid"], periodic_valid["errors"])
            missing_period = json.loads(json.dumps(periodic_configs))
            del missing_period["nodeb"]["parsed_launch_flags"]["commitlog_sync_period_in_ms"]
            self.assertFalse(runner.validate_launch_config(
                missing_period, commitlog_sync="periodic",
                commitlog_sync_period_in_ms=10_000)["valid"])
            batch_with_period = json.loads(json.dumps(configs))
            batch_with_period["nodeb"]["parsed_launch_flags"]["commitlog_sync_period_in_ms"] = "10000"
            self.assertFalse(runner.validate_launch_config(batch_with_period)["valid"])
            for flag, wrong_value in (
                    ("max_networking_io_control_blocks", "50000"),
                    ("reserve_io_control_blocks", "0"),
                    ("reactor_backend", "epoll")):
                with self.subTest(flag=flag):
                    configs["nodeb"]["parsed_launch_flags"][flag] = wrong_value
                    invalid_aio = runner.validate_launch_config(configs)
                    self.assertFalse(invalid_aio["valid"])
                    self.assertTrue(any(flag in error for error in invalid_aio["errors"]))
                    configs["nodeb"]["parsed_launch_flags"][flag] = {
                        "max_networking_io_control_blocks": "1024",
                        "reserve_io_control_blocks": "1024",
                        "reactor_backend": "linux-aio",
                    }[flag]
            configs["nodeb"]["host_config"]["mounts"][1]["RW"] = True
            writable = runner.validate_launch_config(configs)
            self.assertFalse(writable["valid"])
            self.assertTrue(any("read-only trial bind mount" in error
                                for error in writable["errors"]))

    def test_server_reported_zero_error_and_timeout_families_satisfy_required_groups(self) -> None:
        zero_metrics = (
            "scylla_storage_proxy_coordinator_read_timeouts 0\n"
            "scylla_storage_proxy_coordinator_write_timeouts 0\n"
            "scylla_storage_proxy_coordinator_write_errors 0\n"
            "scylla_storage_proxy_coordinator_read_errors 0\n")
        names = runner.parse_prometheus_metric_names(zero_metrics)
        evidence = runner.metric_evidence_from_names(names)
        self.assertEqual(len(evidence["categories"]["errors"]), 2)
        self.assertEqual(len(evidence["categories"]["timeouts"]), 2)

        all_names = names | {
            "scylla_reactor_utilization", "scylla_coordinator_write_latency",
            "scylla_cache_hit", "scylla_memtable_flush", "scylla_compaction",
            "scylla_commitlog_flush", "scylla_commitlog_pending_flushes",
            "scylla_network_bytes_sent",
        }
        summary = {
            "measurement_window": {"timestamps_valid": True},
            "prometheus_window_samples": {node: 1 for node in runner.NODE_NAMES},
            "required_metric_evidence": {
                node: runner.metric_evidence_from_names(all_names) for node in runner.NODE_NAMES},
            "process_window": {"valid": True}, "socket_window": {"valid": True},
            "counter_window_deltas": {"valid": True},
        }
        self.assertTrue(runner.required_metric_validation(summary)["valid"])
        summary["required_metric_evidence"]["nodea"] = runner.metric_evidence_from_names(
            all_names - names)
        invalid = runner.required_metric_validation(summary)
        self.assertFalse(invalid["valid"])
        self.assertIn("errors", invalid["nodes"]["nodea"]["missing_required_groups"])
        self.assertIn("timeouts", invalid["nodes"]["nodea"]["missing_required_groups"])


class AioBudgetTests(unittest.TestCase):
    def test_host_aio_reader_captures_both_proc_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            sys_fs = root / "sys" / "fs"
            sys_fs.mkdir(parents=True)
            (sys_fs / "aio-max-nr").write_text("4096\n")
            (sys_fs / "aio-nr").write_text("1024\n")
            evidence = runner.read_host_aio_counters(root)
        self.assertEqual(evidence["aio_max_nr"], 4096)
        self.assertEqual(evidence["aio_nr"], 1024)
        self.assertEqual(evidence["aio_available"], 3072)

    def test_aio_budget_accepts_exact_boundary_and_rejects_one_below(self) -> None:
        per_shard = (runner.SCYLLA_AIO_PROFILE["max_networking_io_control_blocks"]
                     + runner.STORAGE_IO_CONTROL_BLOCKS_PER_SHARD
                     + runner.PREEMPT_IO_CONTROL_BLOCKS_PER_SHARD)
        required = 3 * 4 * per_shard + runner.AIO_CONTROL_BLOCK_HEADROOM
        at_boundary = runner.compute_aio_budget(4, 262_144, 262_144 - required)
        below_boundary = runner.compute_aio_budget(4, 262_144, 262_144 - required + 1)
        self.assertEqual(at_boundary["required_available"], required)
        self.assertEqual(at_boundary["aio_available"], required)
        self.assertEqual(at_boundary["per_shard_allocated_requirement"], per_shard)
        self.assertEqual(at_boundary["storage_io_control_blocks_per_shard"], 1024)
        self.assertEqual(at_boundary["preempt_io_control_blocks_per_shard"], 2)
        self.assertEqual(at_boundary["reserve_io_control_blocks_per_node"], 1024)
        self.assertEqual(at_boundary["required_final_headroom"], 1024)
        self.assertTrue(at_boundary["valid"])
        self.assertFalse(below_boundary["valid"])
        self.assertIn("below required", below_boundary["error"])

    def test_node_reserve_is_reported_but_not_multiplied_as_per_shard_storage(self) -> None:
        with mock.patch.dict(runner.SCYLLA_AIO_PROFILE,
                             {"reserve_io_control_blocks": 4096}):
            budget = runner.compute_aio_budget(2, 100_000, 0)
        self.assertEqual(budget["per_shard_allocated_requirement"], 1024 + 1024 + 2)
        self.assertEqual(budget["required_final_headroom"], 4096)
        self.assertEqual(budget["required_available"], 3 * 2 * (1024 + 1024 + 2) + 4096)
        self.assertEqual(budget["reserve_io_control_blocks_per_node"], 4096)

    def test_unknown_or_insufficient_aio_fails_before_any_cluster_start(self) -> None:
        cases = (
            {"aio_max_nr": None, "aio_nr": 0},
            {"aio_max_nr": 262_144,
             "aio_nr": 262_144 - (3 * 4 * (1024 + 1024 + 2) + 1024) + 1},
        )
        for aio_counters in cases:
            with self.subTest(aio_counters=aio_counters), tempfile.TemporaryDirectory() as tmp:
                run_dir = pathlib.Path(tmp) / "run"
                run_dir.mkdir()
                args = runner.create_argument_parser().parse_args(["--mode", "smoke"])
                case = runner.build_matrix((4,), (12,), 1)[0]
                cleanup = runner.CleanupResult(status="clean")
                with mock.patch.object(runner, "source_hashes", return_value={}), \
                        mock.patch.object(runner, "read_host_aio_counters",
                                          return_value=aio_counters), \
                        mock.patch.object(runner.OwnedCluster, "start") as start, \
                        mock.patch.object(runner.OwnedCluster, "stop_and_cleanup",
                                          return_value=cleanup):
                    result = runner.run_trial(
                        "aio-test", run_dir, case, args, 2, 2, pathlib.Path("unused-binary"))
                start.assert_not_called()
                trial_dir = run_dir / "trials" / result["trial_id"]
                saved = runner.read_json_object(trial_dir / "aio-budget.json")
                self.assertFalse(saved["valid"])
                self.assertFalse(result["validation"]["aio_budget"])
                self.assertEqual(result["status"], "failed")
                self.assertTrue((trial_dir / "trial-config.json").is_file())


class OwnershipTests(unittest.TestCase):
    def test_marker_binds_exact_trial_and_children(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            trial = pathlib.Path(tmp) / "trial"
            owned = trial / "owned-data"
            trial.mkdir()
            for node in runner.NODE_NAMES:
                (owned / node).mkdir(parents=True)
            runner.write_ownership_marker(owned, "nonce-a", "trial-a")
            marker = runner.validate_ownership_marker(trial, owned, "nonce-a", "trial-a")
            self.assertTrue(marker.is_file())
            with self.assertRaisesRegex(runner.RunnerError, "mismatch"):
                runner.validate_ownership_marker(trial, owned, "nonce-b", "trial-a")

    def test_cleanup_keeps_data_when_docker_inventory_is_uncertain(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            trial = pathlib.Path(tmp) / "trial"
            owned = trial / "owned-data"
            trial.mkdir()
            owned.mkdir()
            for node in runner.NODE_NAMES:
                (owned / node).mkdir()
            runner.write_ownership_marker(owned, "nonce-a", "trial-a")
            cluster = runner.OwnedCluster(trial, owned, "nonce-a", "trial-a", 1, "run-a")
            calls: list[list[str]] = []

            def fake_run(argv, **kwargs):
                command = list(argv)
                calls.append(command)
                if command[:3] == ["docker", "network", "ls"]:
                    return subprocess.CompletedProcess(command, 1, "", "inventory denied")
                if command[:2] == ["docker", "ps"]:
                    return subprocess.CompletedProcess(command, 0, "", "")
                raise AssertionError(f"unexpected cleanup command: {command}")

            with mock.patch.object(runner, "_run", side_effect=fake_run):
                result = cluster.stop_and_cleanup()
            self.assertNotEqual(result.status, "clean")
            self.assertTrue(all((owned / node).is_dir() for node in runner.NODE_NAMES))
            self.assertTrue((owned / ".ledger-wallet-owner.json").is_file())
            self.assertFalse(any("stop" in command or command[:2] == ["docker", "rm"]
                                 for command in calls))
            self.assertFalse(any(command[:2] == ["docker", "run"] for command in calls))

    def test_network_inventory_normalizes_docker_short_id_to_inspected_canonical_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            trial = pathlib.Path(tmp) / "trial"
            owned = trial / "owned-data"
            trial.mkdir()
            owned.mkdir()
            for node in runner.NODE_NAMES:
                (owned / node).mkdir()
            runner.write_ownership_marker(owned, "nonce-net", "trial-net")
            cluster = runner.OwnedCluster(trial, owned, "nonce-net", "trial-net", 1, "run-net")
            canonical_id = "a" * 64
            short_id = canonical_id[:12]
            cluster.network_id = canonical_id  # docker network create returns the full id
            inspected = {"Id": canonical_id, "Labels": cluster.labels}

            def inspect_network(argv, **kwargs):
                command = list(argv)
                if command[:4] == ["docker", "network", "ls", "-q"]:
                    return subprocess.CompletedProcess(command, 0, short_id + "\n", "")
                if command[:3] == ["docker", "network", "inspect"]:
                    self.assertEqual(command[3], short_id)
                    return subprocess.CompletedProcess(command, 0, json.dumps([inspected]), "")
                raise AssertionError(f"unexpected network inventory command: {command}")

            with mock.patch.object(runner, "_run", side_effect=inspect_network):
                safe, error = cluster._discover_owned_network()
                self.assertTrue(safe, error)
                self.assertEqual(cluster.network_id, canonical_id)
                # A short id in a recovered manifest is normalized only after inspecting it.
                cluster.network_id = short_id
                safe, error = cluster._discover_owned_network()
            self.assertTrue(safe, error)
            self.assertEqual(cluster.network_id, canonical_id)

    def test_different_inspected_network_id_fails_closed_and_retains_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            trial = pathlib.Path(tmp) / "trial"
            owned = trial / "owned-data"
            trial.mkdir()
            owned.mkdir()
            for node in runner.NODE_NAMES:
                (owned / node).mkdir()
            runner.write_ownership_marker(owned, "nonce-netbad", "trial-netbad")
            cluster = runner.OwnedCluster(
                trial, owned, "nonce-netbad", "trial-netbad", 1, "run-netbad")
            cluster.network_id = "b" * 64
            inspected = {"Id": "a" * 64, "Labels": cluster.labels}
            calls: list[list[str]] = []

            def inspect_network(argv, **kwargs):
                command = list(argv)
                calls.append(command)
                if command[:4] == ["docker", "network", "ls", "-q"]:
                    return subprocess.CompletedProcess(command, 0, "a" * 12 + "\n", "")
                if command[:3] == ["docker", "network", "inspect"]:
                    return subprocess.CompletedProcess(command, 0, json.dumps([inspected]), "")
                if command[:3] == ["docker", "ps", "-aq"]:
                    return subprocess.CompletedProcess(command, 0, "", "")
                raise AssertionError(f"unexpected cleanup command: {command}")

            with mock.patch.object(runner, "_run", side_effect=inspect_network):
                result = cluster.stop_and_cleanup()
            self.assertEqual(result.status, "retained_data_container_stop_unconfirmed")
            self.assertTrue(any("manifest network id differs" in error for error in result.errors))
            self.assertTrue(all((owned / node).is_dir() for node in runner.NODE_NAMES))
            self.assertTrue((owned / ".ledger-wallet-owner.json").is_file())
            self.assertFalse(any(command[:3] == ["docker", "network", "rm"] for command in calls))

    def test_foreign_container_using_owned_mount_is_never_stopped_or_removed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            trial = pathlib.Path(tmp) / "trial"
            owned = trial / "owned-data"
            trial.mkdir()
            owned.mkdir()
            for node in runner.NODE_NAMES:
                (owned / node).mkdir()
            runner.write_ownership_marker(owned, "nonce-b", "trial-b")
            cluster = runner.OwnedCluster(trial, owned, "nonce-b", "trial-b", 1, "run-b")
            foreign = {
                "Id": "foreign-id", "Name": "/unrelated-service",
                "Config": {"Labels": {"owner": "someone-else"}},
                "Mounts": [{"Source": str((owned / "nodea").resolve()),
                            "Destination": "/var/lib/scylla"}],
                "State": {"Status": "running", "Running": True},
            }
            calls: list[list[str]] = []

            def fake_run(argv, **kwargs):
                command = list(argv)
                calls.append(command)
                if command[:3] == ["docker", "network", "ls"]:
                    return subprocess.CompletedProcess(command, 0, "", "")
                if command[:2] == ["docker", "ps"]:
                    return subprocess.CompletedProcess(command, 0, "foreign-id\n", "")
                if command[:2] == ["docker", "inspect"]:
                    return subprocess.CompletedProcess(command, 0, json.dumps([foreign]), "")
                raise AssertionError(f"unexpected cleanup command: {command}")

            with mock.patch.object(runner, "_run", side_effect=fake_run):
                result = cluster.stop_and_cleanup()
            self.assertEqual(result.status, "retained_data_container_stop_unconfirmed")
            self.assertTrue(any("foreign container" in error for error in result.errors))
            self.assertTrue((owned / "nodea").is_dir())
            self.assertFalse(any(command[:2] in (["docker", "stop"], ["docker", "rm"],
                                                 ["docker", "run"])
                                 for command in calls))

    def _cleanup_fixture(self, tmp: str, post_stop_state: dict[str, object]):
        trial = pathlib.Path(tmp) / "trial"
        owned = trial / "owned-data"
        trial.mkdir()
        owned.mkdir()
        for node in runner.NODE_NAMES:
            (owned / node).mkdir()
        runner.write_ownership_marker(owned, "nonce-stop", "trial-stop")
        cluster = runner.OwnedCluster(trial, owned, "nonce-stop", "trial-stop", 1, "run-stop")
        info = {
            "Id": "owned-id", "Name": "/owned-nodea",
            "Config": {"Labels": {**cluster.labels, "ledger-wallet.node": "nodea"}},
            "Mounts": [{"Source": str((owned / "nodea").resolve()),
                        "Destination": "/var/lib/scylla", "RW": True}],
            "State": {"Status": "running", "Running": True},
        }
        stopped = False
        removed = False
        commands: list[list[str]] = []

        def fake_run(argv, **kwargs):
            nonlocal stopped, removed
            command = list(argv)
            commands.append(command)
            if command[:3] == ["docker", "network", "ls"]:
                return subprocess.CompletedProcess(command, 0, "", "")
            if command[:3] == ["docker", "ps", "-aq"]:
                return subprocess.CompletedProcess(command, 0,
                                                   "" if removed else "owned-id\n", "")
            if command[:2] == ["docker", "stop"]:
                stopped = True
                return subprocess.CompletedProcess(command, 0, "owned-id\n", "")
            if command[:2] == ["docker", "inspect"]:
                snapshot = dict(info)
                snapshot["Config"] = dict(info["Config"])
                snapshot["Mounts"] = list(info["Mounts"])
                if stopped:
                    snapshot["State"] = post_stop_state
                return subprocess.CompletedProcess(command, 0, json.dumps([snapshot]), "")
            if command[:2] == ["docker", "rm"]:
                removed = True
                return subprocess.CompletedProcess(command, 0, "owned-id\n", "")
            raise AssertionError(f"unexpected cleanup command: {command}")

        return cluster, owned, info, commands, fake_run

    def test_cleanup_saves_full_post_stop_inspect_before_remove(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state = {"Status": "exited", "Running": False, "ExitCode": 137,
                     "FinishedAt": "2026-10-11T11:00:00Z", "OOMKilled": True}
            cluster, owned, info, commands, fake_run = self._cleanup_fixture(tmp, state)

            def remove_owned_data():
                evidence = json.loads((cluster.trial_dir / "cleanup-post-stop" /
                                       "nodea.inspect.json").read_text())
                self.assertEqual(evidence["exit_code"], 137)
                self.assertTrue(evidence["oom_killed"])
                self.assertEqual(evidence["finished_at"], state["FinishedAt"])
                self.assertEqual(evidence["labels"], info["Config"]["Labels"])
                self.assertEqual(evidence["mounts"], info["Mounts"])
                self.assertEqual(evidence["post_stop_inspect"]["State"], state)
                for node in runner.NODE_NAMES:
                    (owned / node).rmdir()
                    cluster.cleanup_result.data_removed.append(str(owned / node))
                (owned / ".ledger-wallet-owner.json").unlink()
                owned.rmdir()

            def assert_evidence_precedes_remove(argv, **kwargs):
                command = list(argv)
                if command[:2] == ["docker", "rm"]:
                    evidence_path = cluster.trial_dir / "cleanup-post-stop" / "nodea.inspect.json"
                    self.assertTrue(evidence_path.is_file())
                    self.assertEqual(json.loads(evidence_path.read_text())["exit_code"], 137)
                return fake_run(argv, **kwargs)

            with mock.patch.object(runner, "_run", side_effect=assert_evidence_precedes_remove), \
                    mock.patch.object(cluster, "_remove_owned_data", side_effect=remove_owned_data):
                result = cluster.stop_and_cleanup()
            self.assertEqual(result.status, "clean")
            self.assertTrue(result.post_stop_inspection_files)

    def test_cleanup_retains_data_when_post_stop_running_field_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cluster, owned, _, commands, fake_run = self._cleanup_fixture(
                tmp, {"Status": "exited", "ExitCode": 0, "FinishedAt": "2026-10-11T11:00:00Z"})
            with mock.patch.object(runner, "_run", side_effect=fake_run), \
                    mock.patch.object(cluster, "_remove_owned_data") as remove_owned_data:
                result = cluster.stop_and_cleanup()
            self.assertEqual(result.status, "retained_data_container_stop_unconfirmed")
            self.assertFalse(result.containers_stopped)
            self.assertTrue(any("not confirmed stopped" in error for error in result.errors))
            self.assertTrue(all((owned / node).is_dir() for node in runner.NODE_NAMES))
            self.assertTrue((owned / ".ledger-wallet-owner.json").is_file())
            self.assertFalse(any(command[:2] == ["docker", "rm"] for command in commands))
            remove_owned_data.assert_not_called()

    def test_interrupted_create_recovery_requires_exact_labels_and_mount(self) -> None:
        def fixture(tmp: str, labels_ok: bool):
            trial = pathlib.Path(tmp) / "trial"
            owned = trial / "owned-data"
            trial.mkdir()
            owned.mkdir()
            for node in runner.NODE_NAMES:
                (owned / node).mkdir()
            cluster = runner.OwnedCluster(trial, owned, "nonce-c", "trial-c", 1, "run-c")
            cluster.network_subnet = runner.ipaddress.ip_network("172.24.0.0/16")
            labels = {**cluster.labels, "ledger-wallet.node": "nodea"}
            if not labels_ok:
                labels["ledger-wallet.nonce"] = "foreign-nonce"
            inspected = {"Id": "recovered-id", "Name": "/recovered-nodea",
                         "Config": {"Labels": labels},
                         "Mounts": [{"Source": str((owned / "nodea").resolve()),
                                     "Destination": "/var/lib/scylla"}],
                         "State": {"Status": "created", "Running": False}}
            return cluster, inspected

        with tempfile.TemporaryDirectory() as tmp:
            cluster, inspected = fixture(tmp, True)
            commands: list[list[str]] = []

            def recover(argv, **kwargs):
                command = list(argv)
                commands.append(command)
                if command[1] == "create":
                    raise runner.RunnerError("docker create response timed out")
                if command[:2] == ["docker", "inspect"]:
                    return subprocess.CompletedProcess(command, 0, json.dumps([inspected]), "")
                raise AssertionError(f"unexpected recovery command: {command}")

            with mock.patch.object(runner, "_run", side_effect=recover):
                record = cluster._create_node("nodea")
            manifest = json.loads((cluster.trial_dir / "ownership-manifest.json").read_text())
            self.assertEqual(record.state, "created")
            self.assertEqual(manifest["nodes"][0]["container_id"], "recovered-id")
            self.assertEqual(manifest["nodes"][0]["node"], "nodea")
            self.assertEqual([command[1] for command in commands], ["create", "inspect"])

        with tempfile.TemporaryDirectory() as tmp:
            cluster, inspected = fixture(tmp, False)

            def reject_wrong_owner(argv, **kwargs):
                command = list(argv)
                if command[1] == "create":
                    raise runner.RunnerError("docker create response timed out")
                return subprocess.CompletedProcess(command, 0, json.dumps([inspected]), "")

            with mock.patch.object(runner, "_run", side_effect=reject_wrong_owner):
                with self.assertRaisesRegex(runner.RunnerError, "mismatched ownership"):
                    cluster._create_node("nodea")
            self.assertEqual(cluster.records, [])

    def test_partial_create_scan_records_only_nonce_and_mount_match(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            trial = pathlib.Path(tmp) / "trial"
            owned = trial / "owned-data"
            trial.mkdir()
            owned.mkdir()
            for node in runner.NODE_NAMES:
                (owned / node).mkdir()
            runner.write_ownership_marker(owned, "nonce-d", "trial-d")
            cluster = runner.OwnedCluster(trial, owned, "nonce-d", "trial-d", 1, "run-d")
            recovered = {
                "Id": "created-id", "Name": "/created-nodea",
                "Config": {"Labels": {**cluster.labels, "ledger-wallet.node": "nodea"}},
                "Mounts": [{"Source": str((owned / "nodea").resolve()),
                            "Destination": "/var/lib/scylla"}],
                "State": {"Status": "created", "Running": False},
            }
            calls: list[list[str]] = []

            def fake_run(argv, **kwargs):
                command = list(argv)
                calls.append(command)
                if command[:3] == ["docker", "ps", "-aq"]:
                    return subprocess.CompletedProcess(command, 0, "created-id\n", "")
                if command[:2] == ["docker", "inspect"]:
                    return subprocess.CompletedProcess(command, 0, json.dumps([recovered]), "")
                raise AssertionError(f"unexpected inventory command: {command}")

            with mock.patch.object(runner, "_run", side_effect=fake_run):
                found, safe, error = cluster._discover_owned_containers()
            self.assertTrue(safe, error)
            self.assertEqual(set(found), {"created-id"})
            manifest = json.loads((trial / "ownership-manifest.json").read_text())
            self.assertEqual(manifest["recovered_containers"][0]["state"], "created")
            self.assertEqual(manifest["recovered_containers"][0]["node"], "nodea")

            recovered["Config"]["Labels"]["ledger-wallet.owner"] = "foreign-owner"
            with mock.patch.object(runner, "_run", side_effect=fake_run):
                found, safe, error = cluster._discover_owned_containers()
            self.assertFalse(safe)
            self.assertEqual(found, {})
            self.assertIn("invalid labels or mounts", error)
            self.assertTrue(owned.is_dir())


class MetricTests(unittest.TestCase):
    @staticmethod
    def _counter_samples() -> dict[str, dict[str, dict[str, dict[str, list[tuple[int, float]]]]]]:
        samples = {node: {} for node in runner.NODE_NAMES}
        for node in runner.NODE_NAMES:
            for metric in runner.COMMITLOG_COUNTERS:
                samples[node][metric] = {"0": {"{}": [(90, 10), (150, 18)]}}
            samples[node]["scylla_network_bytes_sent"] = {
                "0": {"{}": [(90, 100), (150, 120)]}}
            samples[node]["scylla_network_bytes_received"] = {
                "0": {"{}": [(90, 200), (150, 230)]}}
        return samples

    def test_counter_contract_uses_current_lwt_profile_and_keeps_unknown_counts(self) -> None:
        evidence = runner.summarize_counter_window(
            self._counter_samples(), 100, 200, 1)
        contract = evidence["application_work_contract"]
        self.assertEqual(contract["wallet_profile"], "wallet-memory-lwt-v1")
        self.assertEqual(contract["account_state_source"], "memory")
        self.assertEqual(contract["ledger_key"], ["account_id", "kind", "tx_id"])
        self.assertEqual(contract["serial_cl"], "LOCAL_SERIAL")
        self.assertEqual(contract["logical_requests_expected"], None)
        self.assertEqual(contract["application_cql_statements_per_request"], 1)
        self.assertEqual(contract["application_cql_statements_expected"], None)
        self.assertEqual(contract["measured_select_requests_per_transaction"], 0)
        self.assertEqual(contract["measured_select_requests_expected"], None)
        self.assertIn("conditional INSERT", contract["cql_shape"])
        self.assertNotIn("UNLOGGED BATCH", contract["cql_shape"])
        self.assertNotIn("2 reads", contract["cql_shape"])
        # Reporting changes must not alter commitlog boundary-delta accounting.
        flush = evidence["nodes"]["nodea"]["scylla_commitlog_flush"]["series"]["0"]
        self.assertEqual(flush["delta"], 8)

    def test_monitor_stop_annotates_lwt_work_contract_with_one_statement_per_request(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            proc_dir = root / "proc"
            proc_dir.mkdir()
            monitor = object.__new__(runner.TrialMonitor)
            monitor.trial_dir = root
            monitor.prom_dir = root / "prometheus"
            monitor.proc_dir = proc_dir
            monitor.device = "sdb"
            monitor.cluster = mock.Mock(shards=1)
            monitor.node_names = runner.NODE_NAMES
            monitor.stop_event = runner.threading.Event()
            monitor.thread = None
            monitor.sample_lock = runner.threading.Lock()
            monitor.measurement_start_unix_ns = None
            monitor.measurement_end_unix_ns = None
            monitor._proc_file = io.StringIO()
            monitor.sample_count = 3
            monitor.errors = []
            monitor.prom_sample_count = {node: 2 for node in runner.NODE_NAMES}
            monitor.counter_samples = self._counter_samples()
            monitor.socket_samples = []
            monitor.process_samples = []

            prom_evidence = {
                node: {"sample_count": 1, "sample_timestamps_unix_ns": [150],
                       "metric_names": []}
                for node in runner.NODE_NAMES
            }
            with (mock.patch.object(monitor, "sample_once"),
                  mock.patch.object(runner, "prometheus_window_metric_evidence",
                                    return_value=prom_evidence)):
                summary = monitor.stop(
                    100, 200, logical_requests_expected=12_800,
                    logical_replies_received=12_800)

            contract = summary["counter_window_deltas"]["application_work_contract"]
            self.assertEqual(contract["logical_requests_expected"], 12_800)
            self.assertEqual(contract["logical_replies_received"], 12_800)
            self.assertEqual(contract["application_cql_statements_per_request"], 1)
            self.assertEqual(contract["application_cql_statements_expected"], 12_800)
            self.assertEqual(contract["measured_select_requests_per_transaction"], 0)
            self.assertEqual(contract["measured_select_requests_expected"], 0)
            self.assertEqual(contract["cql_requests_per_transaction"], 1)
            self.assertIn("conditional INSERT", contract["cql_shape"])
            self.assertNotIn("UNLOGGED BATCH", contract["cql_shape"])
            self.assertNotIn("2 reads", contract["cql_shape"])
            persisted = json.loads((proc_dir / "monitor-summary.json").read_text())
            self.assertEqual(
                persisted["counter_window_deltas"]["application_work_contract"],
                contract)

    def test_gzip_evidence_excludes_samples_outside_measurement_window(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            payload = "\n".join((
                "# runner_sample_unix_ns 9 elapsed_seconds 0",
                "outside_before 1",
                "# runner_sample_unix_ns 10 elapsed_seconds 1",
                "inside_first 2",
                "# runner_sample_unix_ns 20 elapsed_seconds 2",
                "inside_last 3",
                "# runner_sample_unix_ns 21 elapsed_seconds 3",
                "outside_after 4",
                "",
            ))
            for node in runner.NODE_NAMES:
                with gzip.open(root / f"{node}.prom.gz", "wt", encoding="utf-8") as stream:
                    stream.write(payload)
            evidence = runner.prometheus_window_metric_evidence(root, 10, 20)
            self.assertEqual(evidence["nodea"]["sample_count"], 2)
            self.assertEqual(evidence["nodea"]["metric_names"], ["inside_first", "inside_last"])
            self.assertNotIn("outside_before", evidence["nodea"]["metric_names"])
            self.assertNotIn("outside_after", evidence["nodea"]["metric_names"])

    def test_parser_preserves_group_labels_for_counter_aggregation(self) -> None:
        parsed = runner.parse_prometheus_samples(
            'scylla_network_bytes_sent{shard="0",protocol="native"} 12\n'
            'scylla_network_bytes_sent{shard="0",protocol="gossip"} 4\n')
        self.assertEqual(len(parsed), 2)
        self.assertEqual(parsed[0]["labels"]["protocol"], "native")

    def test_counter_delta_sums_group_labels_per_shard_and_timestamp(self) -> None:
        samples = {node: {} for node in runner.NODE_NAMES}
        for node in runner.NODE_NAMES:
            for metric in runner.COMMITLOG_COUNTERS:
                samples[node][metric] = {"0": {"{}": [(90, 10), (150, 18)]}}
            samples[node]["scylla_network_bytes_sent"] = {"0": {
                '{"protocol":"native"}': [(90, 10), (150, 15)],
                '{"protocol":"gossip"}': [(90, 100), (150, 108)],
            }}
            samples[node]["scylla_network_bytes_received"] = {"0": {
                "{}": [(90, 20), (150, 30)]}}
        evidence = runner.summarize_counter_window(samples, 100, 200, 1)
        self.assertTrue(evidence["valid"])
        network = evidence["nodes"]["nodea"]["scylla_network_bytes_sent"]["series"]["0"]
        self.assertEqual(network["delta"], 13)
        flush = evidence["nodes"]["nodea"]["scylla_commitlog_flush"]["series"]["0"]
        self.assertEqual(flush["delta"], 8)

    def test_missing_commitlog_sample_or_counter_reset_is_invalid(self) -> None:
        samples = {node: {} for node in runner.NODE_NAMES}
        for node in runner.NODE_NAMES:
            for metric in runner.COMMITLOG_COUNTERS:
                samples[node][metric] = {"0": {"{}": [(90, 10), (150, 18)]}}
            samples[node]["scylla_commitlog_flush"]["0"]["{}"] = [
                (90, 10), (120, 2), (150, 18)]
        evidence = runner.summarize_counter_window(samples, 100, 200, 1)
        self.assertFalse(evidence["valid"])
        flush = evidence["nodes"]["nodea"]["scylla_commitlog_flush"]["series"]["0"]
        self.assertEqual(flush["status"], "counter_reset_or_wrap")
        self.assertEqual(flush["delta"], None)
        self.assertEqual(flush["reset_transitions"][0]["current_value"], 2)


class IOPropertiesRuntimeTests(unittest.TestCase):
    @staticmethod
    def _captured_periodic_series_failure() -> dict[str, object]:
        captured = (pathlib.Path(__file__).parent / "fixtures" /
                    "scylla_periodic_missing_commitlog_series.json")
        return json.loads(captured.read_text(encoding="utf-8"))

    @staticmethod
    def _sample(category: str, mountpoint: str, value: float) -> dict[str, object]:
        return {
            "name": "scylla_io_queue_activations",
            "labels": {"class": category, "shard": "0", "mountpoint": mountpoint,
                       "stream": "rw", "iogroup": "0"},
            "value": value,
        }

    def _mapped_and_fallback_series(self) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        for category in ("main", "commitlog"):
            rows.append(self._sample(category, "/var/lib/scylla", 11.0))
            # disk_params.cc may register q0 as an empty-mountpoint fallback.
            rows.append(self._sample(category, "", 0.0))
        return rows

    def test_empty_mountpoint_fallback_is_retained_with_data_mapping(self) -> None:
        result = runner.validate_io_queue_mountpoints(
            {"nodea": self._mapped_and_fallback_series()}, ["nodea"], 1)

        self.assertTrue(result["valid"], result["problems"])
        node = result["nodes"]["nodea"]
        self.assertEqual(node["mountpoints_by_class_shard"]["main/shard-0"],
                         ["", "/var/lib/scylla"])
        activations = node["activation_evidence_by_class_shard"]
        self.assertEqual(
            [(row["mountpoint"], row["value"]) for row in activations["commitlog/shard-0"]],
            [("/var/lib/scylla", 11.0), ("", 0.0)])

    def test_other_mountpoint_series_are_preserved_with_data_mapping(self) -> None:
        rows = self._mapped_and_fallback_series()
        rows.append(self._sample("main", "/mnt/other", 7.0))
        result = runner.validate_io_queue_mountpoints({"nodea": rows}, ["nodea"], 1)

        self.assertTrue(result["valid"], result["problems"])
        node = result["nodes"]["nodea"]
        self.assertEqual(node["mountpoints_by_class_shard"]["main/shard-0"],
                         ["", "/mnt/other", "/var/lib/scylla"])
        self.assertIn(
            {"mountpoint": "/mnt/other", "value": 7.0,
             "labels": rows[-1]["labels"]},
            node["activation_evidence_by_class_shard"]["main/shard-0"])

    def test_empty_fallback_without_data_device_mapping_is_invalid(self) -> None:
        rows = [self._sample("main", "", 0.0),
                self._sample("commitlog", "/var/lib/scylla", 3.0)]
        result = runner.validate_io_queue_mountpoints({"nodea": rows}, ["nodea"], 1)
        self.assertFalse(result["valid"])
        self.assertTrue(any(item["class"] == "main" and item["missing_expected_mapping"]
                            for item in result["nodes"]["nodea"]["mismatches"]))

    def test_runtime_composition_passes_queue_validity_to_effective_load_gate(self) -> None:
        node = "nodea"
        source_path = "/owned/trial/io-properties.yaml"
        digest = "a" * 64
        series = self._mapped_and_fallback_series()
        queue_validation = runner.validate_io_queue_mountpoints(
            {node: series}, [node], 1)
        runtime_nodes = {node: {"io_queue_mountpoints": {
            "metric_name": "scylla_io_queue_activations", "series": series,
        }}}

        # capture_io_properties_runtime_evidence uses this composition step before
        # validate_io_properties_load consumes io_queue_mountpoints.valid.
        runner.attach_io_queue_validation(runtime_nodes, queue_validation, [node])
        profile = {"enabled": True, "config_path": source_path,
                   "container_path": runner.IO_PROPERTIES_CONTAINER_PATH,
                   "source_sha256": digest}
        launch_config = {node: {
            "parsed_launch_flags": {
                "io_properties_file": runner.IO_PROPERTIES_CONTAINER_PATH},
            "host_config": {"mounts": [{
                "Destination": runner.IO_PROPERTIES_CONTAINER_PATH,
                "Source": source_path, "RW": False,
            }]},
            "observed_io_properties_sha256": digest,
        }}
        runtime_nodes[node].update({
            "scylla_argv": ["/usr/bin/scylla", "--io-properties-file",
                            runner.IO_PROPERTIES_CONTAINER_PATH],
            "container_file_sha256": digest,
            "device_stats": {"valid": True},
        })
        result = runner.validate_io_properties_load(
            profile, launch_config, {node: "Starting Scylla with I/O properties"},
            [node], runtime_nodes=runtime_nodes, shards=1)

        self.assertTrue(runtime_nodes[node]["io_queue_mountpoints"]["valid"])
        self.assertEqual(runtime_nodes[node]["io_queue_mountpoints"]["validation"],
                         queue_validation["nodes"][node])
        self.assertTrue(result["valid"], result["problems"])

        without_consumed_valid = {node: dict(runtime_nodes[node])}
        without_consumed_valid[node]["io_queue_mountpoints"] = {
            key: value for key, value in runtime_nodes[node]["io_queue_mountpoints"].items()
            if key != "valid"
        }
        rejected = runner.validate_io_properties_load(
            profile, launch_config, {node: "Starting Scylla with I/O properties"},
            [node], runtime_nodes=without_consumed_valid, shards=1)
        self.assertFalse(rejected["valid"])

    def test_second_periodic_failure_is_retryable_missing_commitlog_series_only(self) -> None:
        evidence = self._captured_periodic_series_failure()

        self.assertFalse(evidence["valid"])
        self.assertTrue(runner.missing_io_queue_series_only(evidence, ["nodea"], 2))
        queue = evidence["queue_mountpoint_validation"]["nodes"]["nodea"]
        self.assertEqual(queue["missing_series"], [["commitlog", "0"], ["commitlog", "1"]])
        self.assertEqual(queue["mismatches"], [])
        self.assertEqual(queue["unexpected_series"], [])
        for shard in range(2):
            self.assertIn("/var/lib/scylla", queue["mountpoints_by_class_shard"][
                f"main/shard-{shard}"])
        self.assertTrue(evidence["runtime_validation"]["nodes"]["nodea"][
            "device_stats_valid"])

    def test_wrong_mapping_is_not_classified_as_retryable_missing_series(self) -> None:
        evidence = self._captured_periodic_series_failure()
        node = evidence["queue_mountpoint_validation"]["nodes"]["nodea"]
        node["mismatches"] = [{"class": "main", "shard": "1",
                               "mountpoints": [""], "missing_expected_mapping": True}]
        self.assertFalse(runner.missing_io_queue_series_only(evidence, ["nodea"], 2))

    def test_hash_argv_and_device_failures_are_not_retryable(self) -> None:
        for field_name in ("content_hash_valid", "running_scylla_flag_valid",
                           "device_stats_valid"):
            with self.subTest(field=field_name):
                evidence = self._captured_periodic_series_failure()
                evidence["runtime_validation"]["nodes"]["nodea"][field_name] = False
                self.assertFalse(runner.missing_io_queue_series_only(evidence, ["nodea"], 2))

    def test_periodic_series_readiness_repolls_then_records_every_probe(self) -> None:
        missing = self._captured_periodic_series_failure()
        ready = json.loads(json.dumps(missing))
        ready["valid"] = True
        ready["problems"] = []
        ready["queue_mountpoint_validation"]["valid"] = True
        ready["queue_mountpoint_validation"]["problems"] = []
        qnode = ready["queue_mountpoint_validation"]["nodes"]["nodea"]
        qnode["missing_series"] = []
        qnode["observed_pairs"] = [["commitlog", "0"], ["commitlog", "1"],
                                   ["main", "0"], ["main", "1"]]
        qnode["valid"] = True
        ready["runtime_validation"]["valid"] = True
        ready["runtime_validation"]["problems"] = []
        load_node = ready["runtime_validation"]["nodes"]["nodea"]
        load_node["queue_mountpoints_valid"] = True
        load_node["effective_load_evidence_valid"] = True
        ready["nodes"]["nodea"]["io_queue_mountpoints"]["valid"] = True
        ready["nodes"]["nodea"]["errors"] = []

        with tempfile.TemporaryDirectory() as tmp:
            cluster = object.__new__(runner.OwnedCluster)
            cluster.trial_dir = pathlib.Path(tmp)
            cluster.io_properties_profile = {"enabled": True}
            cluster.commitlog_sync = "periodic"
            cluster.node_names = ["nodea"]
            cluster.shards = 2
            clock = [0.0]
            capture = mock.Mock(side_effect=[missing, ready])
            with mock.patch.object(cluster, "_capture_io_properties_runtime_evidence_once",
                                   capture), \
                    mock.patch.object(runner.time, "monotonic", side_effect=lambda: clock[0]), \
                    mock.patch.object(runner.time, "sleep", side_effect=lambda seconds: clock.__setitem__(
                        0, clock[0] + seconds)):
                result = cluster.capture_io_properties_runtime_evidence(
                    {}, {}, defer_missing_series_until_ready=True)

            self.assertTrue(result["valid"], result["problems"])
            readiness = result["series_readiness"]
            self.assertEqual(readiness["status"], "ready")
            self.assertEqual(readiness["after_cli_phase"], "ready_to_measure")
            self.assertEqual(readiness["attempt_count"], 2)
            self.assertEqual([attempt["retryable_missing_series_only"]
                              for attempt in readiness["attempts"]], [True, False])
            self.assertTrue(readiness["valid"])
            self.assertTrue((pathlib.Path(tmp) / "io-properties-runtime-validation.json").is_file())

    def test_periodic_series_readiness_times_out_with_missing_series_evidence(self) -> None:
        missing = self._captured_periodic_series_failure()
        with tempfile.TemporaryDirectory() as tmp:
            cluster = object.__new__(runner.OwnedCluster)
            cluster.trial_dir = pathlib.Path(tmp)
            cluster.io_properties_profile = {"enabled": True}
            cluster.commitlog_sync = "periodic"
            cluster.node_names = ["nodea"]
            cluster.shards = 2
            clock = [0.0]
            with mock.patch.object(cluster, "_capture_io_properties_runtime_evidence_once",
                                   return_value=missing) as capture, \
                    mock.patch.object(runner, "PERIODIC_IO_QUEUE_READINESS_TIMEOUT_SECONDS", 2), \
                    mock.patch.object(runner.time, "monotonic", side_effect=lambda: clock[0]), \
                    mock.patch.object(runner.time, "sleep", side_effect=lambda seconds: clock.__setitem__(
                        0, clock[0] + seconds)):
                result = cluster.capture_io_properties_runtime_evidence(
                    {}, {}, defer_missing_series_until_ready=True)

            self.assertFalse(result["valid"])
            self.assertEqual(result["series_readiness"]["status"], "timeout")
            self.assertEqual(result["series_readiness"]["attempt_count"], 3)
            self.assertEqual(capture.call_count, 3)
            self.assertTrue(any("did not appear" in problem for problem in result["problems"]))

    def test_periodic_series_readiness_stops_on_wrong_mapping_without_retry(self) -> None:
        wrong_mapping = self._captured_periodic_series_failure()
        wrong_mapping["queue_mountpoint_validation"]["nodes"]["nodea"]["mismatches"] = [
            {"class": "main", "shard": "1", "mountpoints": [""],
             "missing_expected_mapping": True}]
        with tempfile.TemporaryDirectory() as tmp:
            cluster = object.__new__(runner.OwnedCluster)
            cluster.trial_dir = pathlib.Path(tmp)
            cluster.io_properties_profile = {"enabled": True}
            cluster.commitlog_sync = "periodic"
            cluster.node_names = ["nodea"]
            cluster.shards = 2
            with mock.patch.object(cluster, "_capture_io_properties_runtime_evidence_once",
                                   return_value=wrong_mapping) as capture, \
                    mock.patch.object(runner.time, "sleep") as sleep:
                result = cluster.capture_io_properties_runtime_evidence(
                    {}, {}, defer_missing_series_until_ready=True)
            self.assertFalse(result["valid"])
            self.assertEqual(result["series_readiness"]["status"],
                             "immediate_validation_failure")
            self.assertEqual(result["series_readiness"]["attempt_count"], 1)
            capture.assert_called_once()
            sleep.assert_not_called()

    def test_deferred_readiness_is_refused_outside_periodic_io_profile(self) -> None:
        for sync, enabled in (("batch", True), ("periodic", False)):
            with self.subTest(sync=sync, enabled=enabled):
                cluster = object.__new__(runner.OwnedCluster)
                cluster.io_properties_profile = {"enabled": enabled}
                cluster.commitlog_sync = sync
                with mock.patch.object(cluster, "_capture_io_properties_runtime_evidence_once") as capture:
                    with self.assertRaisesRegex(runner.RunnerError, "only allowed for periodic"):
                        cluster.capture_io_properties_runtime_evidence(
                            {}, {}, defer_missing_series_until_ready=True)
                capture.assert_not_called()

    def test_periodic_runner_keeps_measurement_gate_closed_until_io_validation(self) -> None:
        order: list[str] = []
        defer_flags: list[bool] = []
        ready = {"phase": "ready_to_measure", "session_id": "session-test"}
        profile = {"enabled": True, "config_path": "/owned/io-properties.yaml",
                   "container_path": runner.IO_PROPERTIES_CONTAINER_PATH,
                   "source_sha256": "a" * 64}
        failure = {"valid": False, "problems": ["missing commitlog queue series"],
                   "series_readiness": {"enabled": True, "status": "timeout",
                                        "after_cli_phase": "ready_to_measure",
                                        "valid": False, "attempt_count": 31,
                                        "attempts": []}}

        class FakeProcess:
            returncode = None

            def poll(self):
                return self.returncode

            def terminate(self):
                self.returncode = -15

            def wait(self, timeout=None):
                return self.returncode

        class FakeCluster:
            def __init__(self, *args, **kwargs):
                self.trial_dir = args[0]
                self.node_count = 1
                self.node_names = ["nodea"]
                self.shards = 2
                self.records = [mock.Mock(node="nodea")]
                self.io_properties_profile = profile
                self.metrics_relabel_profile = {"config_path": "/owned/metrics-relabel.yaml"}
                self.cleanup_result = runner.CleanupResult(status="clean")

            def start(self, startup_timeout=None):
                runner.write_json(self.trial_dir / "cluster.json", {"node_count": 1})
                return {}

            def full_server_build_evidence(self):
                return {"source_version_correspondence": "verified_by_build_version",
                        "source_tag_commit": runner.SCYLLA_SOURCE_COMMIT,
                        "nodes": {"nodea": {"version_has_tag_commit_prefix": True}}}

            def endpoints(self):
                return ["172.24.0.10:9042"]

            def capture_and_validate_runtime_aio(self):
                return {"valid": True, "nodes": {"nodea": {}}}

            def capture_io_properties_runtime_evidence(self, launch_config, aio, *,
                                                       defer_missing_series_until_ready=False):
                self.assert_ready_order()
                order.append("io_properties_capture")
                defer_flags.append(defer_missing_series_until_ready)
                return failure

            @staticmethod
            def assert_ready_order():
                if not order or order[-1] != "preloading_validated":
                    raise AssertionError("I/O properties validation must follow validated seed/preload completion")

            def verify_server_config(self):
                return {"version_consistent": True}

            def stop_and_cleanup(self):
                return self.cleanup_result

        case = runner.TrialCase(
            2, 4, 3, 1, 1, per_shard_pool_overridden=True,
            requested_per_shard_pool=3, replication_factor=1,
            read_cl="ONE", write_cl="ONE", node_count=1)
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = pathlib.Path(tmp) / "run"
            run_dir.mkdir()
            args = runner.create_argument_parser().parse_args([
                "--mode", "pilot", "--node-count", "1", "--replication-factor", "1",
                "--read-cl", "1", "--write-cl", "1", "--commitlog-sync", "periodic",
                "--commitlog-sync-period-in-ms", "10000"])
            args.periodic_lwt_sync_source_evidence = {"valid": True}

            def fake_wait(_path, _process, phase, _timeout):
                order.append(f"wait_{phase}")
                self.assertEqual(phase, "ready_to_measure")
                return ready

            def fake_preload_validation(*_args):
                order.append("preloading_validated")
                return {"valid": True, "account_states_preloaded": 1024}

            with ExitStack() as stack:
                for patcher in (
                    mock.patch.object(runner, "source_hashes", return_value={}),
                    mock.patch.object(runner, "write_io_properties_profile", return_value=profile),
                    mock.patch.object(runner, "validate_periodic_lwt_evidence_reference",
                                      return_value={"valid": True}),
                    mock.patch.object(runner, "read_host_aio_counters",
                                      return_value={"aio_max_nr": 1_000_000, "aio_nr": 0}),
                    mock.patch.object(runner, "compute_aio_budget",
                                      return_value={"valid": True, "error": None}),
                    mock.patch.object(runner, "OwnedCluster", FakeCluster),
                    mock.patch.object(runner, "_runtime_config_evidence",
                                      return_value={"nodes": {"nodea": {}}}),
                    mock.patch.object(runner, "validate_runtime_commitlog_config",
                                      return_value={"valid": True}),
                    mock.patch.object(runner, "docker_config_evidence",
                                      return_value={"nodea": {}}),
                    mock.patch.object(runner, "validate_launch_config",
                                      return_value={"valid": True, "errors": []}),
                    mock.patch.object(runner, "resolve_scylla_host_pid", return_value=123),
                    mock.patch.object(runner, "thread_affinity_evidence",
                                      return_value={"valid": True}),
                    mock.patch.object(runner, "_cli_command", return_value=["fake-cli"]),
                    mock.patch.object(runner.shutil, "which", return_value="/usr/bin/taskset"),
                    mock.patch.object(runner.subprocess, "Popen", return_value=FakeProcess()),
                    mock.patch.object(runner, "wait_for_event", side_effect=fake_wait),
                    mock.patch.object(runner, "parse_cli_events", return_value=[ready]),
                    mock.patch.object(runner, "validate_lwt_event_contract",
                                      side_effect=fake_preload_validation),
                    mock.patch.object(runner, "validate_ready_event_profile",
                                      return_value={"valid": True}),
                ):
                    stack.enter_context(patcher)
                monitor_start = stack.enter_context(
                    mock.patch.object(runner.TrialMonitor, "start"))
                result = runner.run_trial("ready-gate-test", run_dir, case, args, 1024, 1000,
                                          pathlib.Path(runner.__file__))

            self.assertEqual(result["status"], "failed")
            self.assertEqual(order, ["wait_ready_to_measure", "preloading_validated",
                                     "io_properties_capture"], result)
            self.assertEqual(defer_flags, [True])
            self.assertFalse((run_dir / "trials" / result["trial_id"] / "start.gate").exists())
            self.assertFalse(any("measuring" in item for item in order))
            monitor_start.assert_not_called()

    def test_single_node_launch_profile_checks_flags_mount_hash_and_topology(self) -> None:
        node = "nodea"
        data_path = "/owned/trial/owned-data/nodea"
        metrics_path = "/owned/trial/metrics-relabel.yaml"
        io_path = "/owned/trial/io-properties.yaml"
        digest = "b" * 64
        metrics_digest = hashlib.sha256(runner.METRICS_RELABEL_CONFIG.encode()).hexdigest()
        flags = {
            "developer_mode": "1", "io_setup": "0", "overprovisioned": "1",
            "thread_affinity": "1", "commitlog_sync": "batch",
            "commitlog_use_o_dsync": "false", "commitlog_total_space_in_mb": "1024",
            "commitlog_segment_size_in_mb": "64", "prometheus_address": "0.0.0.0",
            "prometheus_port": "9180", "relabel_config_file": runner.METRICS_RELABEL_CONTAINER_PATH,
            "max_networking_io_control_blocks": "1024", "reserve_io_control_blocks": "1024",
            "reactor_backend": "linux-aio", "smp": "2", "cpuset": "0,1",
            "io_properties_file": runner.IO_PROPERTIES_CONTAINER_PATH,
        }
        profile = {"enabled": True, "config_path": io_path,
                   "container_path": runner.IO_PROPERTIES_CONTAINER_PATH,
                   "source_sha256": digest}
        item = {
            "parsed_launch_flags": flags,
            "host_config": {
                "cpuset_cpus": "0,1", "memory_bytes": runner.NODE_CGROUP_BYTES,
                "memory_swap_bytes": runner.NODE_CGROUP_BYTES, "cpu_quota": None,
                "cpu_period": None, "nano_cpus": None, "port_bindings": {},
                "network_mode": "lw-scylla-test-network",
                "mounts": [
                    {"Source": data_path, "Destination": "/var/lib/scylla", "RW": True},
                    {"Source": metrics_path, "Destination": runner.METRICS_RELABEL_CONTAINER_PATH,
                     "RW": False},
                    {"Source": io_path, "Destination": runner.IO_PROPERTIES_CONTAINER_PATH,
                     "RW": False},
                ],
            },
            "expected_owned_data_source": data_path,
            "expected_metrics_relabel_source": metrics_path,
            "expected_metrics_relabel_sha256": metrics_digest,
            "observed_metrics_relabel_sha256": metrics_digest,
            "expected_io_properties_sha256": digest,
            "observed_io_properties_sha256": digest,
        }
        valid = runner.validate_launch_config({node: item}, 1, profile)
        self.assertTrue(valid["valid"], valid["errors"])
        self.assertEqual(valid["expected_nodes"], [node])
        self.assertEqual(valid["shards"], 2)

        bad_mount = json.loads(json.dumps(item))
        bad_mount["host_config"]["mounts"][2]["RW"] = True
        invalid = runner.validate_launch_config({node: bad_mount}, 1, profile)
        self.assertFalse(invalid["valid"])
        self.assertTrue(any("exact readonly profile mount" in error
                            for error in invalid["errors"]))
        self.assertFalse(runner.validate_launch_config({node: item}, 3, profile)["valid"])

    def test_io_properties_snapshot_preserves_content_hash_and_readonly_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            source = root / "io.conf"
            source.write_text("[commitlog]\nread_ahead = 4096\n", encoding="utf-8")
            captured = runner.inspect_io_properties_source(source)
            trial = root / "trial"
            trial.mkdir()
            saved = runner.write_io_properties_profile(trial, source, captured)
            snapshot = pathlib.Path(saved["config_path"])
            self.assertEqual(saved["source_sha256"], hashlib.sha256(source.read_bytes()).hexdigest())
            self.assertEqual(saved["copied_sha256"], saved["source_sha256"])
            self.assertEqual(snapshot.read_bytes(), source.read_bytes())
            self.assertEqual(snapshot.stat().st_mode & 0o222, 0)
            self.assertEqual(saved["container_path"], runner.IO_PROPERTIES_CONTAINER_PATH)
            self.assertTrue(saved["read_only_container_mount"])


class ValidationTests(unittest.TestCase):
    def test_preflight_report_recovers_artifact_written_before_strict_failure(self) -> None:
        expected = {"valid": False, "error": "insufficient capacity",
                    "required_with_reserve_bytes": 82_042_421_248}
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = pathlib.Path(tmp)
            (run_dir / "formal-preflight.json").write_text(
                json.dumps(expected), encoding="utf-8")
            self.assertEqual(runner.recover_preflight_artifact(run_dir, None), expected)

    def test_cql_readiness_accepts_compatibility_version_not_binary_version(self) -> None:
        self.assertEqual(runner.cql_readiness_version(
            0, 'Connected to localhost.\n{"release_version": "3.0.8"}\n'), "3.0.8")

    def test_cql_readiness_rejects_failed_empty_or_ambiguous_probe(self) -> None:
        self.assertIsNone(runner.cql_readiness_version(
            1, '{"release_version": "3.0.8"}'))
        self.assertIsNone(runner.cql_readiness_version(
            0, '{"release_version": "  "}'))
        self.assertIsNone(runner.cql_readiness_version(
            0, "not JSON and no query result"))
        self.assertIsNone(runner.cql_readiness_version(
            0, '{"release_version":"3.0.8"}\n{"release_version":"3.0.8"}'))

    def test_periodic_does_not_satisfy_batch_profile(self) -> None:
        runtime = {"nodes": {node: {
            "api10000": {"probes": [
                {"config_name": "commitlog_sync", "status": 200, "body": '"periodic"'},
                {"config_name": "commitlog_use_o_dsync", "status": 200, "body": "false"},
            ]},
            "system_config_virtual_table": {},
        } for node in runner.NODE_NAMES}}
        result = runner.validate_runtime_commitlog_config(runtime)
        self.assertFalse(result["valid"])
        self.assertTrue(any("commitlog_sync expected batch" in problem
                            for problem in result["problems"]))

    def test_periodic_runtime_requires_rest_and_system_config_to_match(self) -> None:
        expected = {
            "commitlog_sync": "periodic", "commitlog_sync_period_in_ms": "10000",
            "commitlog_use_o_dsync": "false", "commitlog_sync_batch_window_in_ms": "10000",
            "commitlog_total_space_in_mb": "1024", "commitlog_segment_size_in_mb": "64",
        }

        def runtime(api_values=None, system_values=None, plain_sync_value="periodic"):
            api_values = dict(expected if api_values is None else api_values)
            system_values = dict(expected if system_values is None else system_values)
            nodes = {node: {
                "api10000": {"probes": [
                    {"config_name": name, "status": 200, "body": json.dumps(value)}
                    for name, value in api_values.items()]},
                "system_config_virtual_table": {
                    name: {"exit_code": 0, "rows": [{"name": name, "value": value}],
                           "output": json.dumps({"name": name, "value": value})}
                    for name, value in system_values.items()},
            } for node in runner.NODE_NAMES}
            plain = runner.parse_commitlog_sync_plain_probe(
                0, "Consistency level set to ONE.\n\nname | value\n-----+---------\n"
                f"commitlog_sync | {json.dumps(plain_sync_value)}\n\n(1 rows)\n")
            for node in runner.NODE_NAMES:
                nodes[node]["system_config_virtual_table"]["commitlog_sync"]["plain_probe"] = plain
            return {"nodes": nodes}

        valid = runner.validate_runtime_commitlog_config(
            runtime(), commitlog_sync="periodic", commitlog_sync_period_in_ms=10_000)
        self.assertTrue(valid["valid"], valid["problems"])
        self.assertEqual(valid["commitlog_sync"], "periodic")
        self.assertEqual(valid["nodes"]["nodea"]["observed"]["commitlog_sync_period_in_ms"],
                         "10000")
        wrong_period = dict(expected)
        wrong_period["commitlog_sync_period_in_ms"] = "5000"
        invalid_rest = runner.validate_runtime_commitlog_config(
            runtime(api_values=wrong_period), commitlog_sync="periodic",
            commitlog_sync_period_in_ms=10_000)
        self.assertFalse(invalid_rest["valid"])
        self.assertTrue(any("commitlog_sync_period_in_ms expected 10000" in p
                            for p in invalid_rest["problems"]))
        invalid_system = runner.validate_runtime_commitlog_config(
            runtime(system_values={k: v for k, v in expected.items()
                                   if k != "commitlog_sync_period_in_ms"}),
            commitlog_sync="periodic", commitlog_sync_period_in_ms=10_000)
        self.assertFalse(invalid_system["valid"])
        self.assertTrue(any("not verified through system.config" in p
                            for p in invalid_system["problems"]))
        no_plain_probe = runtime()
        del no_plain_probe["nodes"][runner.NODE_NAMES[0]]["system_config_virtual_table"][
            "commitlog_sync"]["plain_probe"]
        invalid_plain = runner.validate_runtime_commitlog_config(
            no_plain_probe, commitlog_sync="periodic", commitlog_sync_period_in_ms=10_000)
        self.assertFalse(invalid_plain["valid"])
        self.assertTrue(any("plain system.config probe failed" in p
                            for p in invalid_plain["problems"]))
        disagreeing = runner.validate_runtime_commitlog_config(
            runtime(system_values={**expected, "commitlog_sync": "batch"}),
            commitlog_sync="periodic", commitlog_sync_period_in_ms=10_000)
        self.assertFalse(disagreeing["valid"])
        self.assertTrue(any("plain and JSON system.config" in p
                            for p in disagreeing["problems"]))
        json_parser_gap = runtime()
        captured_json = CommitlogSyncPlainProbeTests.CAPTURED_FIRST_ATTEMPT_JSON
        for node in runner.NODE_NAMES:
            query = json_parser_gap["nodes"][node]["system_config_virtual_table"]["commitlog_sync"]
            query.update({"rows": [], "output": captured_json, "json_parser_gap": True,
                          "json_parse_status": "parser_gap"})
        valid_plain_despite_json_gap = runner.validate_runtime_commitlog_config(
            json_parser_gap, commitlog_sync="periodic", commitlog_sync_period_in_ms=10_000)
        self.assertTrue(valid_plain_despite_json_gap["valid"],
                        valid_plain_despite_json_gap["problems"])
        sync_evidence = valid_plain_despite_json_gap["nodes"][runner.NODE_NAMES[0]]["evidence"][
            "commitlog_sync"]
        self.assertEqual(sync_evidence["system_config_value"], "periodic")
        self.assertTrue(sync_evidence["system_config_json_parser_gap"])
        self.assertEqual(sync_evidence["system_config_json_parse_status"], "parser_gap")


class CommitlogSyncPlainProbeTests(unittest.TestCase):
    CAPTURED_FIRST_ATTEMPT_JSON = (
        'Consistency level set to ONE.\n\n [json]\n'
        '-------------------------------------------------------\n'
        ' {"name": "commitlog_sync", "value": "\\\\"periodic\\\\""}\n\n'
        '(1 rows)\n')
    VALID_PERIODIC_TABLE = (
        "Consistency level set to ONE.\n\n"
        " name           | value\n"
        "----------------+------------\n"
        ' commitlog_sync | "periodic"\n\n'
        "(1 rows)\n")

    def test_captured_first_attempt_json_escape_is_marked_parser_gap_and_plain_fixture_works(self) -> None:
        # Exact raw JSON-query output from run-20261013T015038Z-0010535e.
        captured_json = self.CAPTURED_FIRST_ATTEMPT_JSON
        self.assertEqual(runner._json_objects_in_text(captured_json), [])
        self.assertIn('(1 rows)', captured_json)
        parsed = runner.parse_commitlog_sync_plain_probe(0, self.VALID_PERIODIC_TABLE)
        self.assertTrue(parsed["valid"], parsed["errors"])
        self.assertEqual(parsed["name"], "commitlog_sync")
        self.assertEqual(parsed["normalized_value"], "periodic")
        # The cell text is the output of the pinned image's cqlshlib
        # format_value_text for a CQL text value containing the quotes.
        self.assertEqual(parsed["raw_value"], '"periodic"')
        batch = runner.parse_commitlog_sync_plain_probe(
            0, self.VALID_PERIODIC_TABLE.replace('"periodic"', '"batch"'))
        self.assertTrue(batch["valid"], batch["errors"])
        self.assertEqual(batch["normalized_value"], "batch")

    def test_query_system_config_preserves_json_parser_gap_and_plain_probe(self) -> None:
        captured_json = self.CAPTURED_FIRST_ATTEMPT_JSON
        record = runner.ContainerRecord("owned-nodea", "nodea", "owned-id", "172.24.0.2", "0")

        class Cluster:
            records = [record]

        executed_statements = []

        def fake_run(argv, **kwargs):
            statement = argv[-1]
            executed_statements.append(statement)
            if "SELECT name, value FROM system.config WHERE name = 'commitlog_sync'" in statement:
                return subprocess.CompletedProcess(argv, 0, self.VALID_PERIODIC_TABLE, "")
            if "SELECT JSON name, value FROM system.config WHERE name = 'commitlog_sync'" in statement:
                return subprocess.CompletedProcess(argv, 0, captured_json, "")
            value = "false" if "commitlog_use_o_dsync" in statement else "10000"
            name = statement.rsplit("'", 2)[1]
            output = f'{{"name": "{name}", "value": "{value}"}}\n(1 rows)\n'
            return subprocess.CompletedProcess(argv, 0, output, "")

        with mock.patch.object(runner, "_run", side_effect=fake_run):
            evidence = runner._query_system_config(Cluster(), record)
        self.assertEqual(executed_statements[0],
                         "CONSISTENCY ONE; SELECT JSON name, value FROM system.config "
                         "WHERE name = 'commitlog_sync'")
        self.assertEqual(executed_statements[1],
                         "CONSISTENCY ONE; SELECT name, value FROM system.config "
                         "WHERE name = 'commitlog_sync'")
        sync = evidence["commitlog_sync"]
        self.assertTrue(sync["json_parser_gap"])
        self.assertEqual(sync["json_parse_status"], "parser_gap")
        self.assertEqual(sync["rows"], [])
        self.assertEqual(sync["plain_probe"]["normalized_value"], "periodic")
        self.assertEqual(sync["plain_probe"]["query"],
                         "CONSISTENCY ONE; SELECT name, value FROM system.config "
                         "WHERE name = 'commitlog_sync'")
        self.assertEqual(sync["plain_probe"]["consistency_level"], "ONE")
        self.assertIn('\\\\"periodic', sync["output"])

    def test_plain_probe_rejects_query_error_wrong_header_or_name_extra_rows_and_unknown_value(self) -> None:
        malformed = (
            (1, self.VALID_PERIODIC_TABLE, "plain CQL query exited"),
            (0, self.VALID_PERIODIC_TABLE.replace("name           | value", "field | value"),
             "exactly one `name | value` header"),
            (0, self.VALID_PERIODIC_TABLE.replace("commitlog_sync |", "some_other_key |"),
             "row name must be commitlog_sync"),
            (0, self.VALID_PERIODIC_TABLE.replace('(1 rows)', '(2 rows)').replace(
                'commitlog_sync | "periodic"',
                'commitlog_sync | "periodic"\nsecond_row | "periodic"'),
             "exactly one data row"),
            (0, self.VALID_PERIODIC_TABLE.replace('"periodic"', '"unknown"'),
             "must be batch or periodic"),
            (0, self.VALID_PERIODIC_TABLE.replace('"periodic"', 'periodic'),
             "not one JSON scalar"),
        )
        for exit_code, output, expected_error in malformed:
            with self.subTest(expected_error=expected_error):
                parsed = runner.parse_commitlog_sync_plain_probe(exit_code, output)
                self.assertFalse(parsed["valid"])
                self.assertTrue(any(expected_error in error for error in parsed["errors"]))

    def test_periodic_launch_and_parameters_are_closed_against_wrong_profile(self) -> None:
        parser = runner.create_argument_parser()
        args = parser.parse_args([
            "--mode", "pilot", "--node-count", "1", "--shards", "2", "--workers", "512",
            "--per-shard-pool", "3", "--replication-factor", "1", "--read-cl", "1",
            "--write-cl", "1", "--commitlog-sync", "periodic",
            "--commitlog-sync-period-in-ms", "10000",
        ])
        params = runner.resolve_run_parameters(args)
        self.assertEqual(params["commitlog_sync_profile"], "periodic_lwt_force_sync_10000ms")
        report = runner.dry_run_report(args, params)
        self.assertEqual(report["commands"][0]["scylla_server_flags"], [
            "--commitlog-sync", "periodic", "--commitlog-use-o-dsync", "false",
            "--commitlog-sync-period-in-ms", "10000"])
        self.assertTrue(report["periodic_lwt_sync_source_evidence"]["required"])
        self.assertFalse(runner.resolve_run_parameters(parser.parse_args([]))["custom_workload"])


class PeriodicLwtEvidenceTests(unittest.TestCase):
    def _fixture_evidence(self, root: pathlib.Path, expected_files: dict[str, dict[str, object]]):
        source_root = root / runner.PERIODIC_SYNC_LWT_SOURCE_COMMIT
        files = {}
        for relative, definition in expected_files.items():
            content = ("\n".join(definition["evidence"].values()) + "\n").encode()
            path = source_root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)
            url = (f"https://raw.githubusercontent.com/scylladb/scylladb/"
                   f"{runner.PERIODIC_SYNC_LWT_SOURCE_COMMIT}/{relative}")
            files[relative] = {"url": url, "path": str(path),
                               "sha256": hashlib.sha256(content).hexdigest(),
                               "bytes": len(content)}
            definition["sha256"] = hashlib.sha256(content).hexdigest()
        docs = root / "official-lwt-differences.html"
        docs_bytes = ("<p>Scylla LWT ensures data has hit the device. "
                      + runner.PERIODIC_SYNC_LWT_DOC_PHRASE + "</p>").encode()
        docs.write_bytes(docs_bytes)
        manifest = {
            "source_commit": runner.PERIODIC_SYNC_LWT_SOURCE_COMMIT,
            "source_commit_url": (f"https://github.com/scylladb/scylladb/commit/"
                                  f"{runner.PERIODIC_SYNC_LWT_SOURCE_COMMIT}"),
            "files": files,
            "official_lwt_documentation": {
                "url": runner.PERIODIC_SYNC_LWT_DOC_URL, "path": str(docs),
                "sha256": hashlib.sha256(docs_bytes).hexdigest(), "bytes": len(docs_bytes)},
            "interpretation": {"lwt_force_sync_path": "fixture evidence"},
        }
        return manifest

    def test_periodic_source_evidence_checks_immutable_commit_hashes_and_force_sync_chain(self) -> None:
        fixture_files = {
            "service/paxos/paxos_state.cc": {"sha256": "", "evidence": {
                "prepare": "save_paxos_promise", "accept": "save_paxos_proposal",
                "learn": "force_sync::yes", "tablet_route": "system_keyspace::paxos"}},
            "service/storage_proxy.cc": {"sha256": "", "evidence": {
                "prepare": "paxos_state::prepare(", "accept": "paxos_state::accept(",
                "learn": "paxos_state::learn("}},
        }
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            with mock.patch.object(runner, "PERIODIC_SYNC_LWT_SOURCE_FILES", fixture_files):
                manifest = self._fixture_evidence(root, fixture_files)
                result = runner.validate_periodic_lwt_source_evidence(manifest, root)
                self.assertTrue(result["valid"], result["errors"])
                self.assertTrue(result["files"]["service/paxos/paxos_state.cc"]["valid"])
                path = pathlib.Path(manifest["files"]["service/paxos/paxos_state.cc"]["path"])
                path.write_text("changed force_sync::no\n", encoding="utf-8")
                changed = runner.validate_periodic_lwt_source_evidence(manifest, root)
                self.assertFalse(changed["valid"])
                self.assertTrue(any("SHA-256 mismatch" in error for error in changed["errors"]))

    def test_periodic_source_evidence_rejects_tag_url_and_missing_force_sync_marker(self) -> None:
        fixture_files = {
            "service/paxos/paxos_state.cc": {"sha256": "", "evidence": {
                "learn": "force_sync::yes"}},
        }
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            with mock.patch.object(runner, "PERIODIC_SYNC_LWT_SOURCE_FILES", fixture_files):
                manifest = self._fixture_evidence(root, fixture_files)
                entry = manifest["files"]["service/paxos/paxos_state.cc"]
                entry["url"] = entry["url"].replace(
                    runner.PERIODIC_SYNC_LWT_SOURCE_COMMIT, runner.SCYLLA_SOURCE_REF)
                invalid_url = runner.validate_periodic_lwt_source_evidence(manifest, root)
                self.assertFalse(invalid_url["valid"])
                self.assertTrue(any("immutable commit URL" in error
                                    for error in invalid_url["errors"]))
                entry["url"] = (f"https://raw.githubusercontent.com/scylladb/scylladb/"
                                f"{runner.PERIODIC_SYNC_LWT_SOURCE_COMMIT}/service/paxos/paxos_state.cc")
                pathlib.Path(entry["path"]).write_text("no force sync marker", encoding="utf-8")
                entry["sha256"] = hashlib.sha256(b"no force sync marker").hexdigest()
                entry["bytes"] = len(b"no force sync marker")
                missing = runner.validate_periodic_lwt_source_evidence(manifest, root)
                self.assertFalse(missing["valid"])
                self.assertTrue(any("required force-sync evidence" in error
                                    for error in missing["errors"]))

    def test_periodic_evidence_reference_rechecks_manifest_hash_and_source_files(self) -> None:
        fixture_files = {
            "service/paxos/paxos_state.cc": {"sha256": "", "evidence": {
                "learn": "force_sync::yes", "tablet_profile": "system_keyspace::paxos"}},
        }
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            with mock.patch.object(runner, "PERIODIC_SYNC_LWT_SOURCE_FILES", fixture_files):
                manifest = self._fixture_evidence(root, fixture_files)
                validation = runner.validate_periodic_lwt_source_evidence(manifest, root)
                self.assertTrue(validation["valid"], validation["errors"])
                manifest["validation"] = validation
                manifest["valid"] = True
                manifest_path = root / "manifest.json"
                runner.write_json(manifest_path, manifest)
                reference = {"valid": True, "source_commit": runner.PERIODIC_SYNC_LWT_SOURCE_COMMIT,
                             "manifest_path": str(manifest_path),
                             "manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest()}
                checked = runner.validate_periodic_lwt_evidence_reference(reference)
                self.assertTrue(checked["valid"], checked["errors"])
                pathlib.Path(manifest["files"]["service/paxos/paxos_state.cc"]["path"]).write_text(
                    "replacement source", encoding="utf-8")
                changed = runner.validate_periodic_lwt_evidence_reference(reference)
                self.assertFalse(changed["valid"])

    def test_failed_diagnostic_trial_cannot_exit_successfully(self) -> None:
        failed = [{"status": "failed", "cleanup_status": "clean"}]
        invalid = [{"status": "invalid", "cleanup_status": "clean"}]
        self.assertEqual(runner.runner_exit_code("smoke", 1, failed, None, False), 1)
        self.assertEqual(runner.runner_exit_code("pilot", 1, invalid, None, False), 1)
        passed = [{"status": "passed_relaxed", "cleanup_status": "clean"}]
        self.assertEqual(runner.runner_exit_code("smoke", 1, passed, None, False), 0)

    def test_inventory_measures_the_requested_output_filesystem(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = pathlib.Path(tmp)
            requested = base / "another-filesystem" / "runs"
            measured: list[pathlib.Path] = []

            def fake_disk_usage(path):
                measured.append(pathlib.Path(path))
                return mock.Mock(free=12345)

            with mock.patch.object(runner.shutil, "disk_usage", side_effect=fake_disk_usage), \
                    mock.patch.object(runner, "backing_device_name", return_value="sdb"):
                inventory = runner.host_inventory(requested)
            self.assertEqual(measured, [base])
            self.assertEqual(inventory["disk_available_bytes"], 12345)
            self.assertEqual(inventory["backing_device"], "sdb")
            self.assertEqual(inventory["data_path"], str(requested.resolve()))

    def test_process_evidence_invalidates_pid_change_across_window(self) -> None:
        samples = []
        for timestamp, pid in ((90, 100), (120, 100), (150, 101), (210, 101)):
            samples.append({
                "timestamp_unix_ns": timestamp,
                "scylla_processes": {
                    node: {"pid": pid if node == "nodea" else 200 + runner.NODE_NAMES.index(node),
                           "utime_ticks": 1, "stime_ticks": 1, "rss_pages": 10,
                           "status": {"VmRSS": "100 kB"}, "io": {"read_bytes": 1}}
                    for node in runner.NODE_NAMES
                },
            })
        result = runner.validate_process_samples(samples, 100, 200)
        self.assertFalse(result["valid"])
        self.assertFalse(result["nodes"]["nodea"]["host_pid_stable_across_window_and_boundaries"])
        self.assertEqual(result["nodes"]["nodea"]["observed_host_pids"], [100, 101])


class InterruptTests(unittest.TestCase):
    def test_run_trial_records_sigint_and_still_runs_final_cleanup(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = pathlib.Path(tmp) / "run"
            run_dir.mkdir()
            args = runner.create_argument_parser().parse_args(["--mode", "smoke"])
            case = runner.build_matrix((1,), (3,), 1)[0]
            cleanup = runner.CleanupResult(
                status="retained_data_container_stop_unconfirmed",
                retained_data=[str(run_dir / "owned-data")])
            with mock.patch.object(runner, "source_hashes", return_value={}), \
                    mock.patch.object(runner.OwnedCluster, "start", side_effect=KeyboardInterrupt), \
                    mock.patch.object(runner.OwnedCluster, "stop_and_cleanup", return_value=cleanup) as stop:
                result = runner.run_trial("run-test", run_dir, case, args, 2, 2,
                                          pathlib.Path("unused-binary"))
            self.assertEqual(result["status"], "interrupted")
            self.assertEqual(result["cleanup_status"], "retained_data_container_stop_unconfirmed")
            self.assertIn("SIGINT", result["error"])
            self.assertEqual(stop.call_count, 1)
            saved = json.loads((run_dir / "trials" / result["trial_id"] /
                                "runner-result.json").read_text())
            self.assertEqual(saved["status"], "interrupted")

    def test_matrix_stops_after_interruption_and_cannot_report_success(self) -> None:
        args = runner.create_argument_parser().parse_args(["--mode", "smoke"])
        cases = runner.build_matrix((1,), (3, 6), 1)
        trials: list[dict[str, object]] = []
        interrupted = {"trial_id": "first", "status": "interrupted",
                       "cleanup_status": "clean"}
        with mock.patch.object(runner, "run_trial", return_value=interrupted) as run:
            blocked = runner.run_trial_matrix(
                "run", pathlib.Path("unused-run-dir"), cases, args, 2, 2,
                pathlib.Path("unused-binary"), trials)
        self.assertEqual(run.call_count, 1)
        self.assertEqual(trials, [interrupted])
        self.assertIn("interrupted", blocked or "")
        self.assertEqual(runner.runner_exit_code("smoke", len(cases), trials, blocked, False), 1)

    def test_matrix_stops_after_failed_or_invalid_trial_with_clean_cleanup(self) -> None:
        args = runner.create_argument_parser().parse_args(["--mode", "formal"])
        cases = runner.build_matrix((1,), (12,), 3)
        for status in ("failed", "invalid"):
            with self.subTest(status=status):
                trials: list[dict[str, object]] = []
                failed = {"trial_id": "trial-001-S1-C12-rep1", "status": status,
                          "cleanup_status": "clean", "error": "storage validation failed"}
                with mock.patch.object(runner, "run_trial", side_effect=[failed, {}]) as run:
                    blocked = runner.run_trial_matrix(
                        "run", pathlib.Path("unused-run-dir"), cases, args, 50_000, 200,
                        pathlib.Path("unused-binary"), trials)
                self.assertEqual(run.call_count, 1)
                self.assertEqual(trials, [failed])
                self.assertIn("trial-001-S1-C12-rep1", blocked or "")
                self.assertIn(f"status={status}", blocked or "")
                self.assertIn("storage validation failed", blocked or "")

    def test_matrix_continues_passed_statuses_then_stops_at_failed_trial(self) -> None:
        args = runner.create_argument_parser().parse_args(["--mode", "smoke"])
        cases = runner.build_matrix((1,), (3, 6, 9), 1)
        trials: list[dict[str, object]] = []
        outcomes = [
            {"trial_id": "passed", "status": "passed", "cleanup_status": "clean"},
            {"trial_id": "passed-relaxed", "status": "passed_relaxed", "cleanup_status": "clean"},
            {"trial_id": "failed-third", "status": "failed", "cleanup_status": "clean",
             "error": "AIO validation failed"},
        ]
        with mock.patch.object(runner, "run_trial", side_effect=outcomes) as run:
            blocked = runner.run_trial_matrix(
                "run", pathlib.Path("unused-run-dir"), cases, args, 2, 2,
                pathlib.Path("unused-binary"), trials)
        self.assertEqual(run.call_count, 3)
        self.assertEqual(trials, outcomes)
        self.assertIn("failed-third", blocked or "")
        self.assertIn("AIO validation failed", blocked or "")

    def test_unclean_cleanup_takes_priority_and_stops_after_trial(self) -> None:
        args = runner.create_argument_parser().parse_args(["--mode", "smoke"])
        cases = runner.build_matrix((1,), (3, 6), 1)
        trials: list[dict[str, object]] = []
        broken = {"trial_id": "trial-001", "status": "failed",
                  "cleanup_status": "retained_data_container_stop_unconfirmed",
                  "error": "measurement failed"}
        with mock.patch.object(runner, "run_trial", side_effect=[broken, {}]) as run:
            blocked = runner.run_trial_matrix(
                "run", pathlib.Path("unused-run-dir"), cases, args, 2, 2,
                pathlib.Path("unused-binary"), trials)
        self.assertEqual(run.call_count, 1)
        self.assertEqual(trials, [broken])
        self.assertRegex(blocked or "", r"trial-001 cleanup was retained_data_container_stop_unconfirmed")
        self.assertIn("status=failed", blocked or "")
        self.assertIn("measurement failed", blocked or "")

    def test_main_writes_partial_run_report_and_returns_nonzero_after_interrupt(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_root = pathlib.Path(tmp) / "runs"
            interrupted = {
                "order": 1, "repeat": 1, "trial_id": "trial-001-S1-C48-rep1",
                "mode": "smoke", "shards": 1, "workers": 48,
                "per_shard_pool": 16, "users": 2, "requests_per_user": 2,
                "total_requests": 4, "status": "interrupted", "cleanup_status": "clean",
                "required_metrics_valid": False,
            }
            with mock.patch.object(runner, "host_inventory", return_value={}), \
                    mock.patch.object(runner, "source_evidence"), \
                    mock.patch.object(runner, "verify_image_is_pulled"), \
                    mock.patch.object(runner, "apply_runner_affinity", return_value={}), \
                    mock.patch.object(runner, "_cli_binary_path", return_value=pathlib.Path("unused")), \
                    mock.patch.object(runner, "run_trial", return_value=interrupted) as run, \
                    redirect_stdout(io.StringIO()):
                code = runner.main(["--mode", "smoke", "--users", "2",
                                    "--requests-per-user", "2", "--output-root", str(output_root)])
            self.assertEqual(code, 1)
            self.assertEqual(run.call_count, 1)
            run_dirs = list(output_root.glob("run-*"))
            self.assertEqual(len(run_dirs), 1)
            summary = json.loads((run_dirs[0] / "run-result.json").read_text())
            self.assertEqual(summary["actual_trial_count"], 1)
            self.assertIn("interrupted", summary["blocked_error"])
            self.assertTrue((run_dirs[0] / "report.zh-TW.md").is_file())


if __name__ == "__main__":
    unittest.main()
