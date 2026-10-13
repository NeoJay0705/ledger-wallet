"""Offline tests for Scylla evidence archive integrity and compact summaries."""

from __future__ import annotations

import io
import csv
import hashlib
import json
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "benches"))

import archive_scylla_evidence as evidence  # noqa: E402

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SCYLLA_PACKAGE = REPOSITORY_ROOT / "benches" / "data" / "scylla_wallet"


class EvidenceArchiveTests(unittest.TestCase):
    def test_packaged_sources_match_manifests_without_target_tree(self) -> None:
        manifest_path = SCYLLA_PACKAGE / "original-files.csv"
        with manifest_path.open(newline="", encoding="utf-8") as manifest_file:
            manifest_rows = list(csv.DictReader(manifest_file))
        original_by_path = {row["source_path"]: row for row in manifest_rows}
        self.assertEqual(len(original_by_path), len(manifest_rows))

        copied_files = sorted(
            path
            for path in (SCYLLA_PACKAGE / "artifacts" / "target" / "scylla-wallet").rglob("*")
            if path.is_file()
        )
        self.assertTrue(copied_files)
        for copied_file in copied_files:
            source_path = copied_file.relative_to(SCYLLA_PACKAGE / "artifacts").as_posix()
            with self.subTest(source_path=source_path):
                original = original_by_path[source_path]
                contents = copied_file.read_bytes()
                self.assertEqual(len(contents), int(original["size_bytes"]))
                self.assertEqual(
                    hashlib.sha256(contents).hexdigest(), original["sha256"]
                )

        summaries = sorted((SCYLLA_PACKAGE / "summaries").rglob("*.summary.json"))
        self.assertTrue(summaries)
        for summary_path in summaries:
            with self.subTest(summary=summary_path.relative_to(SCYLLA_PACKAGE).as_posix()):
                summary = json.loads(summary_path.read_text(encoding="utf-8"))
                source = summary["source"]
                original = original_by_path[source["path"]]
                self.assertEqual(source["size_bytes"], int(original["size_bytes"]))
                self.assertEqual(source["sha256"], original["sha256"])

        source_manifest_path = SCYLLA_PACKAGE / "qualification" / "source-manifest.csv"
        with source_manifest_path.open(newline="", encoding="utf-8") as manifest_file:
            qualification_rows = list(csv.DictReader(manifest_file))
        self.assertEqual(len(qualification_rows), 1)
        qualification = qualification_rows[0]
        qualification_copy = REPOSITORY_ROOT / qualification["package_path"]
        qualification_contents = qualification_copy.read_bytes()
        self.assertEqual(
            len(qualification_contents), int(qualification["source_size_bytes"])
        )
        self.assertEqual(
            hashlib.sha256(qualification_contents).hexdigest(),
            qualification["source_sha256"],
        )
        self.assertNotIn(qualification["source_archive_member"], original_by_path)

    def test_compiler_directories_are_excluded_but_build_validation_is_kept(self) -> None:
        for path in (
            "debug/deps/bench",
            "release/bench",
            "build/dependency/out",
            "incremental/cache",
        ):
            with self.subTest(path=path):
                self.assertFalse(evidence.is_evidence_path(path))
        self.assertTrue(
            evidence.is_evidence_path("formal-jobs/job/input-snapshot/build/output")
        )
        self.assertFalse(evidence.is_evidence_path(".rustc_info.json"))
        self.assertTrue(evidence.is_evidence_path("build-validation/final.stdout"))
        self.assertTrue(evidence.is_evidence_path("runs/run-1/summary.csv"))

    def test_archive_manifest_hashes_and_safe_extract_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            temp_root = Path(temporary)
            source = temp_root / "target" / "scylla-wallet"
            (source / "runs" / "run-1").mkdir(parents=True)
            (source / "build-validation").mkdir()
            (source / "debug" / "deps").mkdir(parents=True)
            (source / "runs" / "run-1" / "summary.csv").write_text(
                "case,rps\nS1-C1,123.5\n", encoding="utf-8"
            )
            (source / "build-validation" / "final.stdout").write_text(
                "offline validation output\n", encoding="utf-8"
            )
            (source / "debug" / "deps" / "compiled-binary").write_bytes(b"ignored")

            archive_dir = temp_root / "external-archive"
            metadata = evidence.pack_evidence_archive(source, archive_dir)
            self.assertEqual(metadata["file_count"], 2)
            self.assertTrue(evidence.verify_archive_metadata(archive_dir)["verified"])

            extracted = temp_root / "restored"
            result = evidence.extract_evidence_archive(
                archive_dir / evidence.ARCHIVE_BASENAME,
                archive_dir / evidence.MANIFEST_BASENAME,
                extracted,
            )
            self.assertEqual(result["file_count"], 2)
            self.assertEqual(
                (extracted / "scylla-wallet/runs/run-1/summary.csv").read_text(
                    encoding="utf-8"
                ),
                "case,rps\nS1-C1,123.5\n",
            )
            self.assertFalse(
                (extracted / "scylla-wallet/debug/deps/compiled-binary").exists()
            )

    def test_verification_rejects_manifest_hash_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            temp_root = Path(temporary)
            source = temp_root / "source"
            source.mkdir()
            (source / "result.json").write_text('{"status":"failed"}\n', encoding="utf-8")
            archive_dir = temp_root / "archive"
            evidence.pack_evidence_archive(source, archive_dir)

            manifest_path = archive_dir / evidence.MANIFEST_BASENAME
            rows = manifest_path.read_text(encoding="utf-8")
            original_hash = evidence.read_manifest(manifest_path)[0].sha256
            manifest_path.write_text(rows.replace(original_hash, "0" * 64), encoding="utf-8")
            with self.assertRaises(evidence.EvidenceArchiveError):
                evidence.verify_archive_metadata(archive_dir)

    def test_archive_verification_rejects_duplicate_or_outside_members(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            temp_root = Path(temporary)
            source = temp_root / "source"
            source.mkdir()
            source_file = source / "result.json"
            source_file.write_text('{"status":"passed"}\n', encoding="utf-8")
            archive_dir = temp_root / "archive"
            evidence.pack_evidence_archive(source, archive_dir)
            archive_path = archive_dir / evidence.ARCHIVE_BASENAME
            manifest_path = archive_dir / evidence.MANIFEST_BASENAME
            entry = evidence.read_manifest(manifest_path)[0]
            contents = source_file.read_bytes()

            with tarfile.open(archive_path, "w:gz") as archive:
                info = tarfile.TarInfo(entry.archive_member)
                info.size = len(contents)
                archive.addfile(info, io.BytesIO(contents))
                duplicate = tarfile.TarInfo(entry.archive_member)
                duplicate.size = len(contents)
                archive.addfile(duplicate, io.BytesIO(contents))
            with self.assertRaisesRegex(evidence.EvidenceArchiveError, "duplicate tar member"):
                evidence.verify_evidence_archive(archive_path, manifest_path)

            with tarfile.open(archive_path, "w:gz") as archive:
                info = tarfile.TarInfo("outside/result.json")
                info.size = len(contents)
                archive.addfile(info, io.BytesIO(contents))
            with self.assertRaisesRegex(evidence.EvidenceArchiveError, "unsafe tar member path"):
                evidence.verify_evidence_archive(archive_path, manifest_path)

    def test_pack_refuses_to_overwrite_existing_archive(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            temp_root = Path(temporary)
            source = temp_root / "source"
            source.mkdir()
            (source / "result.json").write_text('{"status":"failed"}\n', encoding="utf-8")
            archive_dir = temp_root / "archive"
            evidence.pack_evidence_archive(source, archive_dir)
            original_hash = evidence.sha256_file(archive_dir / evidence.ARCHIVE_BASENAME)
            with self.assertRaisesRegex(evidence.EvidenceArchiveError, "refusing to overwrite"):
                evidence.pack_evidence_archive(source, archive_dir)
            self.assertEqual(
                evidence.sha256_file(archive_dir / evidence.ARCHIVE_BASENAME), original_hash
            )

    def test_extract_refuses_existing_tree_or_symlink_destination(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            temp_root = Path(temporary)
            source = temp_root / "source"
            source.mkdir()
            (source / "result.json").write_text('{"status":"passed"}\n', encoding="utf-8")
            archive_dir = temp_root / "archive"
            evidence.pack_evidence_archive(source, archive_dir)

            destination = temp_root / "restored"
            destination.mkdir()
            existing_tree = destination / "scylla-wallet"
            existing_tree.mkdir()
            marker = existing_tree / "marker"
            marker.write_text("keep", encoding="utf-8")
            with self.assertRaisesRegex(evidence.EvidenceArchiveError, "refusing to overwrite"):
                evidence.extract_evidence_archive(
                    archive_dir / evidence.ARCHIVE_BASENAME,
                    archive_dir / evidence.MANIFEST_BASENAME,
                    destination,
                )
            self.assertEqual(marker.read_text(encoding="utf-8"), "keep")

            symlink_destination = temp_root / "symlink-destination"
            symlink_destination.symlink_to(destination, target_is_directory=True)
            with self.assertRaisesRegex(evidence.EvidenceArchiveError, "symlink destination"):
                evidence.extract_evidence_archive(
                    archive_dir / evidence.ARCHIVE_BASENAME,
                    archive_dir / evidence.MANIFEST_BASENAME,
                    symlink_destination,
                )

    def test_extracted_tree_rejects_symlink_entries(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            temp_root = Path(temporary)
            source = temp_root / "source"
            source.mkdir()
            (source / "result.json").write_text('{"status":"passed"}\n', encoding="utf-8")
            archive_dir = temp_root / "archive"
            evidence.pack_evidence_archive(source, archive_dir)
            extracted = temp_root / "restored"
            evidence.extract_evidence_archive(
                archive_dir / evidence.ARCHIVE_BASENAME,
                archive_dir / evidence.MANIFEST_BASENAME,
                extracted,
            )
            root = extracted / "scylla-wallet"
            (root / "extra-link").symlink_to(root / "result.json")
            with self.assertRaisesRegex(evidence.EvidenceArchiveError, "symlink in extracted"):
                evidence.verify_extracted_tree(root, archive_dir / evidence.MANIFEST_BASENAME)

    def test_large_result_summary_keeps_performance_and_correctness_scalars(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "runner-result.json"
            source.write_text(
                json.dumps(
                    {
                        "status": "failed_setup",
                        "cases": [
                            {
                                "case_id": "S2-C512",
                                "accepted_requests": 0,
                                "response_rps": None,
                                "verification": {"valid": False, "errors": 1},
                            }
                        ],
                        "opaque": "omit-me",
                    }
                ),
                encoding="utf-8",
            )
            summary = evidence.summarize_large_result_json(
                source,
                source_relative_path="runs/run-1/runner-result.json",
                source_sha256="a" * 64,
            )
            self.assertEqual(summary["source"]["sha256"], "a" * 64)
            self.assertEqual(summary["top_level"]["status"], "failed_setup")
            record = summary["result_records"]["records"][0]
            self.assertEqual(record["accepted_requests"], 0)
            self.assertEqual(record["response_rps"], None)
            self.assertEqual(record["verification"]["errors"], 1)
            self.assertNotIn("opaque", record)


if __name__ == "__main__":
    unittest.main()
