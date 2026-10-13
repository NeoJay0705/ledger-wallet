"""Offline integrity and reproducibility checks for compact wallet DB evidence."""

from __future__ import annotations

import csv
import io
import json
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "benches"))

import archive_wallet_db_evidence as evidence  # noqa: E402


REPO = Path(__file__).resolve().parents[1]
SOURCE_ROOT = REPO / "target" / "wallet-db-compare"
BUNDLE = REPO / "benches/data/wallet_db_compare/run-20261013T124123Z-3216cf07f3"
ARCHIVE = Path(
    "/home/neojhou/benchmark-archives/ledger-wallet/wallet-db-compare/"
    "wallet-db-compare-20261013T124123Z-3216cf07f3.tar.gz"
)


class WalletEvidenceIntegrityTests(unittest.TestCase):
    def test_raw_archive_rejects_member_data_tampering(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw = root / "wallet-db-compare"
            (raw / "runs").mkdir(parents=True)
            (raw / "runs" / "evidence.json").write_bytes(b'{"status":"passed"}\n')
            manifest, _ = evidence.file_manifest(raw)
            archive = root / "raw.tar.gz"
            with tarfile.open(archive, "w:gz") as tar:
                tar.add(raw, arcname=evidence.RAW_PREFIX)
            self.assertEqual(evidence.verify_archive(archive, manifest)["file_count"], 1)

            tampered = root / "tampered.tar.gz"
            with tarfile.open(tampered, "w:gz") as tar:
                directory = tarfile.TarInfo(evidence.RAW_PREFIX)
                directory.type = tarfile.DIRTYPE
                tar.addfile(directory)
                payload = b'{"status":"failed"}\n'
                member = tarfile.TarInfo(f"{evidence.RAW_PREFIX}/runs/evidence.json")
                member.size = len(payload)
                tar.addfile(member, io.BytesIO(payload))
            with self.assertRaises(evidence.EvidenceError):
                evidence.verify_archive(tampered, manifest)

    def test_resource_window_selection_fails_closed_without_brackets(self) -> None:
        with self.assertRaisesRegex(evidence.EvidenceError, "bracket and cover"):
            evidence.choose_samples([{}, {}], [1_000, 2_000], 0, 3_000, "fixture")
        with self.assertRaisesRegex(evidence.EvidenceError, "more than 2.5s"):
            evidence.choose_samples(
                [{}, {}, {}], [0, 3_000_000_000, 4_000_000_000],
                2_900_000_000, 3_500_000_000, "fixture",
            )

    def test_bundle_verification_rejects_derived_data_tampering(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            bundle = Path(temporary)
            (bundle / "cases.csv").write_text("case_id,rps\nfixture,1\n", encoding="utf-8")
            (bundle / "original-file-manifest.csv").write_text(
                "path,bytes,SHA-256\nwallet-db-compare/runs/x.json,1," + "0" * 64 + "\n",
                encoding="utf-8",
            )
            files = evidence.bundle_files(bundle)
            (bundle / "bundle-manifest.json").write_bytes(
                evidence.canonical_json({"files": files})
            )
            self.assertTrue(evidence.verify_bundle(bundle)["valid"])
            (bundle / "cases.csv").write_text("case_id,rps\nfixture,2\n", encoding="utf-8")
            with self.assertRaisesRegex(evidence.EvidenceError, "bundle file set"):
                evidence.verify_bundle(bundle)

    @unittest.skipUnless(BUNDLE.is_dir(), "generated compact bundle is not present")
    def test_git_bundle_recalculates_resources_and_checks_original_copies(self) -> None:
        self.assertTrue(evidence.verify_bundle(BUNDLE)["valid"])
        readme = (BUNDLE / "README.md").read_text(encoding="utf-8")
        self.assertIn('<a id="raw-archive"></a>', readme)
        self.assertIn('<a id="original-evidence-lookup"></a>', readme)
        summary = json.loads((BUNDLE / "campaign-summary.json").read_bytes())
        resources = json.loads((BUNDLE / "resource-summary.json").read_bytes())
        rows = list(csv.DictReader(
            io.StringIO((BUNDLE / "resource-samples.csv").read_text(encoding="utf-8"))
        ))
        original_rows = evidence.parse_manifest((BUNDLE / "original-file-manifest.csv").read_bytes())
        original_by_path = {row["path"]: row for row in original_rows}

        for case in summary["cases"]:
            for name in ("result.json", "events.jsonl"):
                ref = next(
                    item for item in case["source_inputs"]
                    if item["path"].endswith(f"/service/{name}")
                )
                original = original_by_path[ref["path"]]
                copied = (BUNDLE / "cases" / case["case_id"] / "service" / name).read_bytes()
                self.assertEqual(len(copied), ref["bytes"])
                self.assertEqual(evidence.digest(copied), ref["sha256"])
                self.assertEqual(original["bytes"], ref["bytes"])
                self.assertEqual(original["sha256"], ref["sha256"])

        run_manifest_ref = summary["run_manifest"]["source"]
        plan_ref = summary["approved_plan"]["source"]
        for name, ref in (("run-manifest.json", run_manifest_ref),
                          ("approved-plan.json", plan_ref)):
            original = original_by_path[ref["path"]]
            copied = (BUNDLE / name).read_bytes()
            self.assertEqual(evidence.digest(copied), ref["sha256"])
            self.assertEqual(original["sha256"], ref["sha256"])

        resources_by_case = {item["case_id"]: item for item in resources["case_resources"]}
        for case_id, resource in resources_by_case.items():
            kind = "pg_monitor" if resource["backend"] == "postgres" else "scylla_proc"
            case_rows = [row for row in rows if row["case_id"] == case_id and row["source_kind"] == kind]
            in_window = [row for row in case_rows if row["window_relation"] == "inside"]
            self.assertEqual(
                len(in_window),
                resource["database_memory"]["sample_count_in_measurement_window"],
                case_id,
            )
            max_memory = max(int(row["memory_current_bytes"]) for row in in_window) / evidence.MIB
            self.assertEqual(max_memory, resource["database_memory"]["maximum_memory_current_mib"])
            if resource["postgres_pss"] is not None:
                max_pss = max(int(row["postgres_pss_kb"]) for row in in_window) / 1024
                self.assertEqual(max_pss, resource["postgres_pss"]["maximum_pss_mib"])

            left = next(row for row in case_rows if row["selection_roles"].startswith("left_"))
            right = next(row for row in case_rows if row["selection_roles"].startswith("right_"))
            elapsed_ns = int(right["sample_unix_ns"]) - int(left["sample_unix_ns"])
            cpu_delta_usec = int(right["cgroup_cpu_usage_usec"]) - int(left["cgroup_cpu_usage_usec"])
            recalculated_cpu = (cpu_delta_usec * 1000 / elapsed_ns) / 2 * 100
            self.assertAlmostEqual(
                recalculated_cpu,
                resource["database_cpu"]["cpu_percent"],
                places=12,
                msg=case_id,
            )

    @unittest.skipUnless(
        (SOURCE_ROOT / "runs" / evidence.RUN_ID).is_dir(),
        "formal raw benchmark run is not present",
    )
    def test_mutated_service_counters_are_rejected(self) -> None:
        source = evidence.Source(SOURCE_ROOT)
        cases = evidence.read_campaign_sources(source)
        item = next(row for row in cases if row["backend"] == "postgres")
        base = f"runs/{evidence.RUN_ID}/postgres-cases/{item['case_id']}"
        result_path = f"{base}/service/result.json"
        original_read = source.read

        def changed_read(name: str) -> bytes:
            payload = original_read(name)
            if name == result_path:
                value = json.loads(payload)
                value["requests"]["errors"] = 1
                return evidence.canonical_json(value)
            return payload

        source.read = changed_read  # type: ignore[method-assign]
        with self.assertRaisesRegex(evidence.EvidenceError, "workload errors"):
            evidence.make_case(source, item)

    @unittest.skipUnless(
        (SOURCE_ROOT / "runs" / evidence.RUN_ID).is_dir(),
        "formal raw benchmark run is not present",
    )
    def test_approved_plan_and_run_manifest_hash_mismatches_are_rejected(self) -> None:
        mutations = (
            (f"runs/{evidence.RUN_ID}/approved-plan.json", "contract_sha256", "0" * 64,
             "contract hashes differ"),
            (f"runs/{evidence.RUN_ID}/run-manifest.json", "approved_plan_sha256", "0" * 64,
             "approved_plan_sha256 differs"),
        )
        for path, field, value, message in mutations:
            with self.subTest(field=field):
                source = evidence.Source(SOURCE_ROOT)
                original_read = source.read

                def changed_read(name: str, *, target: str = path,
                                 key: str = field, replacement: str = value) -> bytes:
                    payload = original_read(name)
                    if name == target:
                        obj = json.loads(payload)
                        obj[key] = replacement
                        return evidence.canonical_json(obj)
                    return payload

                source.read = changed_read  # type: ignore[method-assign]
                with self.assertRaisesRegex(evidence.EvidenceError, message):
                    evidence.build_summaries(source)

    @unittest.skipUnless(
        (SOURCE_ROOT / "runs" / evidence.RUN_ID).is_dir() and BUNDLE.is_dir() and ARCHIVE.is_file(),
        "raw benchmark run, archive, and generated compact bundle are required",
    )
    def test_original_sources_rebuild_without_drift_and_match_report_metrics(self) -> None:
        result = evidence.verify_bundle(BUNDLE, archive=ARCHIVE, source_root=SOURCE_ROOT)
        self.assertTrue(result["derived_outputs_rebuilt"])
        self.assertEqual(result["original_file_count"], 2033)
        self.assertEqual(result["original_file_bytes"], 176_228_422)

        source = evidence.Source(SOURCE_ROOT)
        summary, _, samples_csv, resources, _ = evidence.build_summaries(source)
        self.assertEqual(
            summary["approved_plan"]["contract_sha256"],
            "07eccbb58d92c389be5325104e39f7e591f5423c8546a2edf4e3615614587cc7",
        )
        self.assertEqual(
            resources["source_inputs"],
            sorted(resources["source_inputs"], key=lambda item: item["path"]),
        )
        resource_by_case = {row["case_id"]: row for row in resources["case_resources"]}
        expected_formal = {
            "scylla-rep1": (93.45438899548262, 2916.60546875, None),
            "postgres-rep1": (78.42630075423111, 828.3515625, 256.591796875),
            "postgres-rep2": (78.29560716891338, 828.72265625, 256.6005859375),
            "scylla-rep2": (92.99153538932904, 2900.2734375, None),
            "scylla-rep3": (93.31071192687718, 2875.484375, None),
            "postgres-rep3": (78.18978627028632, 828.77734375, 256.6875),
        }
        for case_id, (cpu, memory_mib, pss_mib) in expected_formal.items():
            current = resource_by_case[case_id]
            self.assertEqual(current["database_cpu"]["cpu_percent"], cpu)
            self.assertEqual(current["database_memory"]["maximum_memory_current_mib"], memory_mib)
            observed_pss = current["postgres_pss"]
            self.assertEqual(
                observed_pss["maximum_pss_mib"] if observed_pss else None,
                pss_mib,
            )

        expected_calibration = {
            "pg-calibration-D16": (78.52606656654095, 830.0),
            "pg-calibration-D32": (89.9438373745036, 872.78125),
            "pg-calibration-D64": (96.18376119624169, 949.703125),
        }
        for case_id, (cpu, memory_mib) in expected_calibration.items():
            current = resource_by_case[case_id]
            self.assertEqual(current["database_cpu"]["cpu_percent"], cpu)
            self.assertEqual(current["database_memory"]["maximum_memory_current_mib"], memory_mib)

        rows = list(csv.DictReader(io.StringIO(samples_csv.decode("utf-8"))))
        for case_id, resource in resource_by_case.items():
            selected = [row for row in rows if row["case_id"] == case_id and row["source_kind"] in {
                "pg_monitor", "scylla_proc"
            }]
            in_window = [row for row in selected if row["window_relation"] == "inside"]
            self.assertEqual(
                len(in_window),
                resource["database_memory"]["sample_count_in_measurement_window"],
                case_id,
            )
            self.assertTrue(any("left_" in row["selection_roles"] for row in selected), case_id)
            self.assertTrue(any("right_" in row["selection_roles"] for row in selected), case_id)

    def test_archive_manifest_and_tar_are_consistent(self) -> None:
        if not BUNDLE.is_dir() or not ARCHIVE.is_file():
            self.skipTest("generated compact bundle or raw archive is not present")
        manifest = evidence.parse_manifest((BUNDLE / "original-file-manifest.csv").read_bytes())
        result = evidence.verify_archive(ARCHIVE, manifest)
        self.assertEqual(result["file_count"], 2033)
        self.assertEqual(result["uncompressed_bytes"], 176_228_422)
        self.assertEqual(result["sha256"], "d0977b40c7b1f75dadc57ede978538669941aff76bc2b446f29324b6970c3c19")


if __name__ == "__main__":
    unittest.main()
