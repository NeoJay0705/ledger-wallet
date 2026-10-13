"""Offline boundaries for the one-node Scylla I/O observer."""
from __future__ import annotations

import gzip
import json
import os
import pathlib
import tempfile
import unittest
from unittest import mock

from benches import scylla_io_observer as observer


class SysfsIdentityTests(unittest.TestCase):
    def _sysfs_partition(self, root: pathlib.Path, *, composite: bool = False) -> pathlib.Path:
        sys_root = root / "sys"
        whole = sys_root / "devices" / "mock0" / "block" / "sdb"
        partition = whole / "sdb2"
        whole.mkdir(parents=True)
        partition.mkdir()
        (whole / "dev").write_text("8:16\n")
        (partition / "dev").write_text("8:18\n")
        (partition / "partition").write_text("2\n")
        if composite:
            slaves = partition / "slaves"
            slaves.mkdir()
            (slaves / "dm-0").symlink_to(root / "fake-slave")
        dev_block = sys_root / "dev" / "block"
        dev_block.mkdir(parents=True)
        (dev_block / "8:16").symlink_to(whole)
        (dev_block / "8:18").symlink_to(partition)
        return sys_root

    def test_partition_st_dev_maps_to_sdb2_and_parent_whole_sdb(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sys_root = self._sysfs_partition(pathlib.Path(tmp))
            identity = observer.resolve_block_identity(os.makedev(8, 18), sys_root)
        self.assertEqual(identity["data_device"]["name"], "sdb2")
        self.assertEqual(identity["data_device"]["major_minor"], "8:18")
        self.assertEqual(identity["data_device"]["partition_number"], 2)
        self.assertEqual(identity["whole_device"]["name"], "sdb")
        self.assertEqual(identity["whole_device"]["major_minor"], "8:16")
        self.assertFalse(identity["whole_and_data_are_same_device"])
        self.assertEqual(identity["physical_disk_count"], 1)

    def test_calibrated_collector_requires_exact_sdb2_partition_and_sdb_disk(self) -> None:
        profile = {
            "data_device": {"name": "sdb2", "major_minor": "8:18"},
            "whole_device": {"name": "sdb", "major_minor": "8:16"},
        }
        validation = observer.validate_calibrated_device_profile(profile)
        self.assertTrue(validation["valid"])
        self.assertEqual(validation["observed"]["data_device"],
                         {"name": "sdb2", "major_minor": "8:18"})
        profile["whole_device"]["name"] = "sdc"
        with self.assertRaisesRegex(observer.ObserverError, "differs from calibrated identity"):
            observer.validate_calibrated_device_profile(profile)

    def test_unmapped_and_composite_device_identities_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sys_root = self._sysfs_partition(pathlib.Path(tmp))
            with self.assertRaisesRegex(observer.ObserverError, "no /sys/dev/block mapping"):
                observer.resolve_block_identity(os.makedev(8, 19), sys_root)
        with tempfile.TemporaryDirectory() as tmp:
            sys_root = self._sysfs_partition(pathlib.Path(tmp), composite=True)
            with self.assertRaisesRegex(observer.ObserverError, "composite/slave"):
                observer.resolve_block_identity(os.makedev(8, 18), sys_root)


class DiskstatsTests(unittest.TestCase):
    @staticmethod
    def _profile() -> dict[str, object]:
        return {
            "host_st_dev": os.makedev(8, 18),
            "data_device": {"name": "sdb2", "major": 8, "minor": 18,
                            "major_minor": "8:18", "partition_number": 2},
            "whole_device": {"name": "sdb", "major": 8, "minor": 16,
                             "major_minor": "8:16"},
            "whole_and_data_are_same_device": False,
        }

    @staticmethod
    def _diskstat_line(major: int, minor: int, name: str,
                       updates: dict[str, int] | None = None) -> str:
        values = dict.fromkeys(observer.DISKSTATS_FIELDS, 0)
        values.update(updates or {})
        return f"{major} {minor} {name} " + " ".join(str(values[key]) for key in observer.DISKSTATS_FIELDS)

    @classmethod
    def _sample(cls, unix_ns: int, monotonic_ns: int,
                whole: dict[str, int], part: dict[str, int] | None = None) -> dict[str, object]:
        profile = cls._profile()
        text = "\n".join((
            cls._diskstat_line(8, 16, "sdb", whole),
            cls._diskstat_line(8, 18, "sdb2", part or whole),
        ))
        return {
            "time_unix_ns": unix_ns, "time_monotonic_ns": monotonic_ns,
            "host_st_dev": profile["host_st_dev"],
            "data_device_major_minor": "8:18", "whole_device_major_minor": "8:16",
            "diskstats": observer.parse_diskstats(text, profile),
        }

    def test_selected_diskstats_requires_all_seventeen_fields(self) -> None:
        profile = self._profile()
        complete = self._diskstat_line(8, 16, "sdb") + "\n" + self._diskstat_line(8, 18, "sdb2")
        parsed = observer.parse_diskstats(complete, profile)
        self.assertEqual(set(parsed), {"whole_device", "data_device"})
        self.assertEqual(len(parsed["whole_device"]["fields"]), 17)
        short = "8 16 sdb 0 0 0 0 0 0 0 0 0 0 0\n" + self._diskstat_line(8, 18, "sdb2")
        with self.assertRaisesRegex(observer.ObserverError, "expected 17"):
            observer.parse_diskstats(short, profile)

    def test_window_rates_flush_zero_is_na_and_views_are_separate(self) -> None:
        first = self._sample(1_000_000_000, 1_000_000_000, {})
        end_updates = {
            "reads_completed": 100, "sectors_read": 200, "read_ms": 400,
            "writes_completed": 50, "sectors_written": 300, "write_ms": 250,
            "io_ms": 1000, "weighted_io_ms": 1200,
            "flush_requests": 0, "flush_ms": 0,
        }
        last = self._sample(3_000_000_000, 3_000_000_000, end_updates,
                            {"reads_completed": 5, "sectors_read": 10})
        result = observer._disk_device_window("whole_device", [first, last], self._profile(),
                                              1_000_000_000, 3_000_000_000)
        self.assertEqual(result["pair_interval_seconds"], 2.0)
        self.assertAlmostEqual(result["read_iops"], 50.0)
        self.assertAlmostEqual(result["write_iops"], 25.0)
        self.assertAlmostEqual(result["weighted_read_await_ms_per_op"], 4.0)
        self.assertAlmostEqual(result["weighted_write_await_ms_per_op"], 5.0)
        self.assertAlmostEqual(result["busy_percent"], 50.0)
        self.assertAlmostEqual(result["average_queue_estimate"], 0.6)
        self.assertEqual(result["flush_requests"], 0)
        self.assertIsNone(result["average_flush_ms_per_request"])
        self.assertEqual(result["flush_average_status"], "N/A_no_flush_requests")
        self.assertEqual(result["device"]["name"], "sdb")

    def test_any_observed_counter_reset_invalidates_even_if_final_value_recovers(self) -> None:
        values = [90, 10, 120]
        samples = [self._sample(index * 1_000_000_000, index * 1_000_000_000,
                                {"reads_completed": value})
                   for index, value in enumerate(values, 1)]
        with self.assertRaisesRegex(observer.ObserverError, "reads_completed counter reset"):
            observer._disk_device_window("whole_device", samples, self._profile(),
                                         1_000_000_000, 3_000_000_000)

    def test_runner_host_device_identity_mismatch_is_rejected(self) -> None:
        profile = self._profile()
        trial = {"io_properties_runtime_validation": {
            "valid": True,
            "nodes": {"nodea": {"device_stats": {
                "valid": True, "host_owned_root_device_id": os.makedev(8, 99)}}}}}
        with self.assertRaisesRegex(observer.ObserverError, "st_dev mismatch"):
            observer.verify_trial_device_identity(profile, trial)


class ProfileValidationTests(unittest.TestCase):
    @staticmethod
    def _service_result(workers: int) -> dict[str, object]:
        return {"config": {
            "node_count": 1, "S": 2, "P": 3, "data_connection_target": 6,
            "C": workers, "users": 1024, "requests_per_user": 1000,
            "replication_factor": 1, "read_cl": "ONE", "write_cl": "ONE",
            "wallet_profile": "wallet-memory-lwt-v1", "account_state_source": "memory",
            "serial_cl": "LOCAL_SERIAL", "nodes": ["172.24.0.2:9042"],
        }}

    def test_expected_profile_accepts_and_records_c256_or_conditional_c512(self) -> None:
        for workers in (256, 512):
            with self.subTest(workers=workers):
                evidence = observer.verify_expected_trial_profile(self._service_result(workers))
                self.assertTrue(evidence["valid"])
                self.assertEqual(evidence["observed"]["workers"], workers)
                self.assertEqual(evidence["expected"]["workers_one_of"], [256, 512])

    def test_expected_profile_rejects_unapproved_worker_count(self) -> None:
        with self.assertRaisesRegex(observer.ObserverError, "workers"):
            observer.verify_expected_trial_profile(self._service_result(128))


class PrometheusWindowTests(unittest.TestCase):
    @staticmethod
    def _scrape(timestamp: int, values: dict[tuple[str, str], float],
                fallback_addition: float = 0.0) -> str:
        lines = [f"# runner_sample_unix_ns {timestamp} elapsed_seconds 1"]
        for category in observer.QUEUE_CLASSES:
            for shard in observer.QUEUE_SHARDS:
                ops = values[(category, shard)]
                delay = ops * 0.00001
                execution = ops * 0.00002
                for metric, value in zip(observer.QUEUE_METRICS, (ops, delay, execution)):
                    labels = (f'class="{category}",iogroup="0",mountpoint="/var/lib/scylla",'
                              f'shard="{shard}",stream="rw"')
                    lines.append(f"{metric}{{{labels}}} {value}")
                    fallback_labels = (f'class="{category}",iogroup="0",mountpoint="",'
                                       f'shard="{shard}",stream="rw"')
                    lines.append(f"{metric}{{{fallback_labels}}} {value + fallback_addition}")
        return "\n".join(lines) + "\n"

    @staticmethod
    def _write_scrapes(path: pathlib.Path, scrapes: list[tuple[int, str]]) -> None:
        with gzip.open(path, "wt", encoding="utf-8") as handle:
            for _, text in scrapes:
                handle.write(text)

    def test_marker_window_uses_only_in_window_data_mount_series_and_zero_ops_is_na(self) -> None:
        base = {(category, shard): 100.0 for category in observer.QUEUE_CLASSES
                for shard in observer.QUEUE_SHARDS}
        end = dict(base)
        end[("main", "0")] = 100.0
        end[("main", "1")] = 127.0
        end[("commitlog", "0")] = 125.0
        end[("commitlog", "1")] = 130.0
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "nodea.prom.gz"
            self._write_scrapes(path, [
                (900, self._scrape(900, base, fallback_addition=100_000)),
                (1000, self._scrape(1000, base, fallback_addition=100_000)),
                (2000, self._scrape(2000, end, fallback_addition=100_000)),
                (2100, self._scrape(2100, {(k): value + 999_999 for k, value in end.items()})),
            ])
            result = observer.extract_io_queue_window(path, 1000, 2000)
        self.assertEqual(result["in_window_scrape_count"], 2)
        self.assertEqual(result["first_scrape_offset_from_window_start_ns"], 0)
        self.assertEqual(result["last_scrape_offset_from_window_end_ns"], 0)
        self.assertEqual(result["series_by_class_shard"]["commitlog/shard-0"]["delta_operations"], 25)
        main_zero = result["series_by_class_shard"]["main/shard-0"]
        self.assertEqual(main_zero["delta_operations"], 0)
        self.assertIsNone(main_zero["mean_queue_delay_ms_per_operation"])
        self.assertEqual(main_zero["zero_operations_status"], "N/A_no_operations")
        self.assertIn("", result["excluded_other_mountpoint_series_counts"])

    def test_window_queue_counter_reset_is_rejected(self) -> None:
        values = {(category, shard): 10.0 for category in observer.QUEUE_CLASSES
                  for shard in observer.QUEUE_SHARDS}
        lower = dict(values)
        lower[("commitlog", "0")] = 2.0
        higher = dict(values)
        higher[("commitlog", "0")] = 15.0
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "nodea.prom.gz"
            self._write_scrapes(path, [
                (100, self._scrape(100, values)),
                (200, self._scrape(200, lower)),
                (300, self._scrape(300, higher)),
            ])
            with self.assertRaisesRegex(observer.ObserverError, "reset/wrapped"):
                observer.extract_io_queue_window(path, 100, 300)

    def test_missing_required_series_at_intermediate_scrape_is_rejected(self) -> None:
        values = {(category, shard): 10.0 for category in observer.QUEUE_CLASSES
                  for shard in observer.QUEUE_SHARDS}
        before = self._scrape(100, values)
        missing_middle = self._scrape(200, values)
        missing_middle = "\n".join(
            line for line in missing_middle.splitlines()
            if not (line.startswith("scylla_io_queue_total_operations{")
                    and 'class="commitlog"' in line and 'shard="0"' in line
                    and 'mountpoint="/var/lib/scylla"' in line)) + "\n"
        after = self._scrape(300, values)
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "nodea.prom.gz"
            self._write_scrapes(path, [(100, before), (200, missing_middle), (300, after)])
            with self.assertRaisesRegex(observer.ObserverError, "missing or changed at in-window scrape"):
                observer.extract_io_queue_window(path, 100, 300)


class CollectorTests(unittest.TestCase):
    def test_ready_is_written_after_first_sample_and_stop_file_finishes_cleanly(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            output = root / "io-observer"
            stop = output / "stop"
            profile = {
                "schema_version": 1, "captured_utc": "test", "data_path": str(root),
                "host_st_dev": os.makedev(8, 18),
                "data_device": {"name": "sdb2", "major": 8, "minor": 18,
                                 "major_minor": "8:18", "partition_number": 2},
                "whole_device": {"name": "sdb", "major": 8, "minor": 16,
                                 "major_minor": "8:16"},
                "whole_and_data_are_same_device": False,
                "physical_disk_count": 1,
            }
            device = {"name": "sdb", "major": 8, "minor": 16,
                      "major_minor": "8:16", "fields": dict.fromkeys(observer.DISKSTATS_FIELDS, 0)}
            part = {"name": "sdb2", "major": 8, "minor": 18,
                    "major_minor": "8:18", "fields": dict.fromkeys(observer.DISKSTATS_FIELDS, 0)}
            calls = 0

            def read_sample(_profile: dict[str, object], _path: pathlib.Path) -> dict[str, object]:
                nonlocal calls
                calls += 1
                if calls == 2:
                    stop.touch()
                return {"whole_device": device, "data_device": part}

            with mock.patch.object(observer, "set_observer_affinity", return_value={"valid": True}), \
                    mock.patch.object(observer, "resolve_device_profile", return_value=profile), \
                    mock.patch.object(observer, "_current_data_st_dev", return_value=os.makedev(8, 18)), \
                    mock.patch.object(observer, "read_diskstats_sample", side_effect=read_sample):
                result = observer.collect_diskstats(output, root, stop, interval_seconds=0)

            ready = json.loads((output / "ready.json").read_text())
            self.assertTrue(ready["ready"])
            self.assertEqual(ready["first_sample"]["sample_index"], 0)
            self.assertTrue(result["valid"], result["errors"])
            self.assertTrue(result["stop_observed"])
            self.assertEqual(result["stop_reason"], "stop_file")
            self.assertGreater(result["observer_pid"], 0)
            self.assertGreater(ready["first_sample"]["observer_pid"], 0)
            self.assertEqual(result["sample_count"], 2)
            self.assertIsNotNone(result["max_gap_seconds"])

    @staticmethod
    def _collector_sample(reads_completed: int) -> dict[str, object]:
        whole_fields = dict.fromkeys(observer.DISKSTATS_FIELDS, 0)
        whole_fields["reads_completed"] = reads_completed
        whole = {"name": "sdb", "major": 8, "minor": 16, "major_minor": "8:16",
                 "fields": whole_fields}
        part = {"name": "sdb2", "major": 8, "minor": 18, "major_minor": "8:18",
                "fields": dict(whole_fields)}
        return {"whole_device": whole, "data_device": part}

    def _profile(self, root: pathlib.Path) -> dict[str, object]:
        return {
            "schema_version": 1, "captured_utc": "test", "data_path": str(root),
            "host_st_dev": os.makedev(8, 18),
            "data_device": {"name": "sdb2", "major": 8, "minor": 18,
                            "major_minor": "8:18", "partition_number": 2},
            "whole_device": {"name": "sdb", "major": 8, "minor": 16,
                             "major_minor": "8:16"},
            "whole_and_data_are_same_device": False,
            "physical_disk_count": 1,
        }

    def _collect_with_mocked_samples(self, root: pathlib.Path, output: pathlib.Path,
                                     stop: pathlib.Path, read_side_effect: object,
                                     monotonic_values: list[int] | None = None) -> dict[str, object]:
        patches = [
            mock.patch.object(observer, "set_observer_affinity", return_value={"valid": True}),
            mock.patch.object(observer, "resolve_device_profile", return_value=self._profile(root)),
            mock.patch.object(observer, "_current_data_st_dev", return_value=os.makedev(8, 18)),
            mock.patch.object(observer, "read_diskstats_sample", side_effect=read_side_effect),
        ]
        if monotonic_values is not None:
            patches.append(mock.patch.object(observer.time, "monotonic_ns", side_effect=monotonic_values))
        with patches[0], patches[1], patches[2], patches[3]:
            if monotonic_values is not None:
                with patches[4]:
                    return observer.collect_diskstats(output, root, stop, interval_seconds=0)
            return observer.collect_diskstats(output, root, stop, interval_seconds=0)

    def test_counter_reset_stops_collection_after_first_bad_adjacent_sample(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            output = root / "io-observer"
            stop = output / "stop"
            calls = 0

            def read_sample(*_args: object) -> dict[str, object]:
                nonlocal calls
                calls += 1
                if calls == 3:
                    stop.touch()
                return self._collector_sample(10 if calls == 1 else 2)

            result = self._collect_with_mocked_samples(root, output, stop, read_sample)
            self.assertFalse(result["valid"])
            self.assertEqual(result["sample_count"], 2)
            self.assertEqual(calls, 2)
            self.assertEqual(result["stop_reason"], "collector_error_or_interruption")
            self.assertIn("reads_completed counter reset/wrapped", result["errors"][0])

    def test_data_path_device_change_stops_collection_immediately(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            output = root / "io-observer"
            stop = output / "stop"
            calls = 0

            def read_sample(*_args: object) -> dict[str, object]:
                nonlocal calls
                calls += 1
                if calls == 3:
                    stop.touch()
                return self._collector_sample(10 + calls)

            actual_devices = [os.makedev(8, 18), os.makedev(8, 19)]
            with mock.patch.object(observer, "set_observer_affinity", return_value={"valid": True}), \
                    mock.patch.object(observer, "resolve_device_profile", return_value=self._profile(root)), \
                    mock.patch.object(observer, "_current_data_st_dev", side_effect=actual_devices), \
                    mock.patch.object(observer, "read_diskstats_sample", side_effect=read_sample):
                result = observer.collect_diskstats(output, root, stop, interval_seconds=0)
            self.assertFalse(result["valid"])
            self.assertEqual(result["sample_count"], 2)
            self.assertEqual(calls, 2)
            self.assertIn("disk identity changed", result["errors"][0])

    def test_gap_over_limit_stops_collection_immediately(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            output = root / "io-observer"
            stop = output / "stop"
            calls = 0

            def read_sample(*_args: object) -> dict[str, object]:
                nonlocal calls
                calls += 1
                if calls == 3:
                    stop.touch()
                return self._collector_sample(10 + calls)

            monotonic_values = [1_000_000, 2_000_000, 3_000_000, 4_000_000, 3_000_000_000]
            result = self._collect_with_mocked_samples(
                root, output, stop, read_sample, monotonic_values=monotonic_values)
            self.assertFalse(result["valid"])
            self.assertEqual(result["sample_count"], 2)
            self.assertEqual(calls, 2)
            self.assertGreater(result["max_gap_seconds"], observer.MAX_SAMPLE_GAP_SECONDS)
            self.assertFalse(result["max_gap_valid"])
            self.assertIn("sampling gap", result["errors"][0])

    def test_main_records_early_collector_failure_and_returns_nonzero(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output = pathlib.Path(tmp) / "io-observer"
            with mock.patch.object(observer, "collect_diskstats",
                                   side_effect=observer.ObserverError("unmapped device")):
                code = observer.main(["--collect", "--output-dir", str(output),
                                      "--data-path", tmp, "--stop-file", str(output / "stop")])
            result = json.loads((output / "collector-result.json").read_text())
        self.assertEqual(code, 1)
        self.assertFalse(result["valid"])
        self.assertIn("unmapped device", result["errors"][0])


if __name__ == "__main__":
    unittest.main()
