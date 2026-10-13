#!/usr/bin/env python3
"""Package and verify retained Scylla benchmark evidence without build output."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
import sys
import tarfile
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Iterable


ARCHIVE_BASENAME = "scylla-wallet-evidence.tar.gz"
MANIFEST_BASENAME = "original-files.csv"
METADATA_BASENAME = "archive-metadata.json"
ARCHIVE_ROOT = "scylla-wallet"
EXCLUDED_COMPONENTS = {"debug", "release", "build", "incremental"}
EXCLUDED_ROOT_FILES = {".rustc_info.json"}
SUMMARY_SCALAR_KEYS = {
    "account_state_source",
    "accounts_expected",
    "accounts_verified",
    "accepted",
    "accepted_requests",
    "actual_trial_count",
    "applied",
    "attempted",
    "attempted_count",
    "blocked_error",
    "cql_requests_per_transaction",
    "cql_statement_count",
    "cleanup_status",
    "cli_exit_code",
    "complete",
    "count",
    "d",
    "duration_seconds",
    "elapsed_ns",
    "end_to_end_rps",
    "error",
    "errors",
    "expected",
    "expected_case_count",
    "expected_trial_count",
    "failed",
    "failures",
    "formal_complete",
    "formal_status",
    "ledger_rows_expected",
    "ledger_rows_seen",
    "mean_ns",
    "measured_cql_request_count",
    "measured_select_requests",
    "max_ns",
    "min_ns",
    "mode",
    "order",
    "p",
    "p50_ns",
    "p95_ns",
    "p99_ns",
    "p999_ns",
    "per_shard_pool",
    "r",
    "read_cl",
    "replies_received",
    "replication_factor",
    "request_count",
    "requests_per_user",
    "response_rps",
    "rps",
    "run_id",
    "s",
    "serial_cl",
    "shards",
    "shards_per_node",
    "status",
    "successful_applied_rps",
    "total_requests",
    "total_requests_expected",
    "trial_id",
    "users",
    "valid",
    "unit",
    "verification_failures",
    "wall_duration_ns",
    "wall_seconds",
    "wallet_profile",
    "window_duration_ns",
    "workers",
    "worker_routing",
    "write_cl",
    "write_consistency",
    "cleanup",
    "required_metrics_valid",
    "required_metrics",
    "workload_class",
    "cli_binary_sha256",
}
SUMMARY_GROUP_KEYS = {
    "cases",
    "cleanup",
    "client_end_event",
    "cli_result",
    "driver_counters",
    "end_to_end_latency",
    "measurement",
    "query_accounting",
    "requests",
    "trials",
    "validation",
    "verification",
    "verified_event",
}


class EvidenceArchiveError(RuntimeError):
    """Raised when an evidence archive or its manifest fails validation."""


@dataclass(frozen=True)
class ManifestEntry:
    source_path: str
    archive_member: str
    size_bytes: int
    sha256: str


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def is_evidence_path(relative_path: str | Path) -> bool:
    """Exclude only target/scylla-wallet's top-level compiler output directories."""
    parts = PurePosixPath(str(relative_path).replace(os.sep, "/")).parts
    if not parts:
        return True
    return parts[0] not in EXCLUDED_COMPONENTS and not (
        len(parts) == 1 and parts[0] in EXCLUDED_ROOT_FILES
    )


def scan_evidence_files(source_root: Path) -> list[Path]:
    if not source_root.is_dir():
        raise EvidenceArchiveError(f"source directory does not exist: {source_root}")
    files: list[Path] = []
    for current, directory_names, file_names in os.walk(source_root, topdown=True, followlinks=False):
        current_path = Path(current)
        relative_dir = current_path.relative_to(source_root)
        if not relative_dir.parts:
            directory_names[:] = [
                name
                for name in directory_names
                if is_evidence_path((relative_dir / name).as_posix())
            ]
        for name in list(directory_names):
            path = current_path / name
            if path.is_symlink():
                raise EvidenceArchiveError(f"symlink is not permitted in evidence source: {path}")
        for name in file_names:
            path = current_path / name
            if not relative_dir.parts and name in EXCLUDED_ROOT_FILES:
                continue
            if path.is_symlink():
                raise EvidenceArchiveError(f"symlink is not permitted in evidence source: {path}")
            files.append(path)
    return sorted(files, key=lambda item: item.relative_to(source_root).as_posix())


def create_original_manifest(source_root: Path) -> list[ManifestEntry]:
    """Hash each retained source file and record its exact tar member name."""
    entries: list[ManifestEntry] = []
    for path in scan_evidence_files(source_root):
        relative = path.relative_to(source_root).as_posix()
        entries.append(
            ManifestEntry(
                source_path=f"target/scylla-wallet/{relative}",
                archive_member=f"{ARCHIVE_ROOT}/{relative}",
                size_bytes=path.stat().st_size,
                sha256=sha256_file(path),
            )
        )
    return entries


def write_manifest(entries: Iterable[ManifestEntry], manifest_path: Path) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with manifest_path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=("source_path", "archive_member", "size_bytes", "sha256"),
        )
        writer.writeheader()
        for entry in entries:
            writer.writerow(asdict(entry))


def read_manifest(manifest_path: Path) -> list[ManifestEntry]:
    entries: list[ManifestEntry] = []
    seen_members: set[str] = set()
    with manifest_path.open(newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream)
        expected = ["source_path", "archive_member", "size_bytes", "sha256"]
        if reader.fieldnames != expected:
            raise EvidenceArchiveError(
                f"unexpected manifest columns: {reader.fieldnames!r}"
            )
        for row in reader:
            member = row["archive_member"]
            pure_member = PurePosixPath(member)
            if (
                pure_member.is_absolute()
                or ".." in pure_member.parts
                or not pure_member.parts
                or pure_member.parts[0] != ARCHIVE_ROOT
            ):
                raise EvidenceArchiveError(f"unsafe archive member path: {member}")
            if not row["source_path"].startswith("target/scylla-wallet/"):
                raise EvidenceArchiveError(f"unexpected source path: {row['source_path']}")
            source_parts = PurePosixPath(row["source_path"]).parts[2:]
            archive_relative_parts = pure_member.parts[1:]
            if source_parts != archive_relative_parts:
                raise EvidenceArchiveError(
                    f"source/archive path mismatch for {row['source_path']}"
                )
            if member in seen_members:
                raise EvidenceArchiveError(f"duplicate archive member: {member}")
            seen_members.add(member)
            try:
                size = int(row["size_bytes"])
            except (TypeError, ValueError) as error:
                raise EvidenceArchiveError(f"invalid size for {member}") from error
            checksum = row["sha256"]
            if len(checksum) != 64 or any(c not in "0123456789abcdef" for c in checksum):
                raise EvidenceArchiveError(f"invalid SHA-256 for {member}")
            entries.append(
                ManifestEntry(
                    source_path=row["source_path"],
                    archive_member=member,
                    size_bytes=size,
                    sha256=checksum,
                )
            )
    return entries


def verify_evidence_archive(archive_path: Path, manifest_path: Path) -> dict[str, int]:
    """Check every tar member against the original file count, size, and hash."""
    entries = read_manifest(manifest_path)
    expected = {entry.archive_member: entry for entry in entries}
    seen: set[str] = set()
    seen_members: set[str] = set()
    with tarfile.open(archive_path, "r:gz") as archive:
        for member in archive.getmembers():
            pure_member = PurePosixPath(member.name)
            if (
                pure_member.is_absolute()
                or ".." in pure_member.parts
                or not pure_member.parts
                or pure_member.parts[0] != ARCHIVE_ROOT
            ):
                raise EvidenceArchiveError(f"unsafe tar member path: {member.name}")
            if member.name in seen_members:
                raise EvidenceArchiveError(f"duplicate tar member: {member.name}")
            seen_members.add(member.name)
            if not member.isfile() and not member.isdir():
                raise EvidenceArchiveError(f"unsupported tar member type: {member.name}")
            if member.isdir():
                continue
            if member.name not in expected:
                raise EvidenceArchiveError(f"unmanifested tar member: {member.name}")
            entry = expected[member.name]
            if member.size != entry.size_bytes:
                raise EvidenceArchiveError(
                    f"size mismatch for {member.name}: tar={member.size}, manifest={entry.size_bytes}"
                )
            stream = archive.extractfile(member)
            if stream is None:
                raise EvidenceArchiveError(f"cannot read tar member: {member.name}")
            digest = hashlib.sha256()
            size = 0
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                size += len(block)
                digest.update(block)
            if size != entry.size_bytes or digest.hexdigest() != entry.sha256:
                raise EvidenceArchiveError(f"content hash mismatch for {member.name}")
            seen.add(member.name)
    missing = sorted(set(expected) - seen)
    if missing:
        raise EvidenceArchiveError(f"archive is missing {len(missing)} manifest members")
    return {
        "file_count": len(entries),
        "total_source_bytes": sum(entry.size_bytes for entry in entries),
        "archive_bytes": archive_path.stat().st_size,
    }


def pack_evidence_archive(source_root: Path, archive_dir: Path) -> dict[str, Any]:
    """Create the compressed raw archive, complete original manifest, and metadata."""
    source_root = source_root.resolve()
    archive_dir.mkdir(parents=True, exist_ok=True)
    archive_path = archive_dir / ARCHIVE_BASENAME
    manifest_path = archive_dir / MANIFEST_BASENAME
    metadata_path = archive_dir / METADATA_BASENAME
    lock_path = archive_dir / ".pack.lock"
    outputs = (archive_path, manifest_path, metadata_path)
    try:
        lock_fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError as error:
        raise EvidenceArchiveError(f"another pack operation holds {lock_path}") from error
    os.close(lock_fd)
    created_outputs: list[Path] = []
    try:
        existing = [path.name for path in outputs if path.exists() or path.is_symlink()]
        if existing:
            raise EvidenceArchiveError(
                "refusing to overwrite existing archive output: " + ", ".join(existing)
            )
        return _pack_evidence_archive_locked(source_root, archive_dir, outputs, created_outputs)
    except BaseException:
        for path in created_outputs:
            try:
                path.unlink()
            except FileNotFoundError:
                pass
        raise
    finally:
        try:
            lock_path.unlink()
        except FileNotFoundError:
            pass


def _pack_evidence_archive_locked(
    source_root: Path,
    archive_dir: Path,
    outputs: tuple[Path, Path, Path],
    created_outputs: list[Path],
) -> dict[str, Any]:
    archive_path, manifest_path, metadata_path = outputs
    entries = create_original_manifest(source_root)
    if not entries:
        raise EvidenceArchiveError("no eligible evidence files found")

    with tempfile.TemporaryDirectory(prefix="scylla-evidence-", dir=archive_dir) as scratch:
        scratch_dir = Path(scratch)
        scratch_manifest = scratch_dir / MANIFEST_BASENAME
        scratch_archive = scratch_dir / ARCHIVE_BASENAME
        write_manifest(entries, scratch_manifest)
        with tarfile.open(scratch_archive, "w:gz", compresslevel=1) as archive:
            for entry in entries:
                relative = entry.archive_member.removeprefix(f"{ARCHIVE_ROOT}/")
                source = source_root / relative
                archive.add(source, arcname=entry.archive_member, recursive=False)
        verify_result = verify_evidence_archive(scratch_archive, scratch_manifest)
        archive_sha = sha256_file(scratch_archive)
        manifest_sha = sha256_file(scratch_manifest)
        try:
            os.link(scratch_manifest, manifest_path)
            created_outputs.append(manifest_path)
            os.link(scratch_archive, archive_path)
            created_outputs.append(archive_path)
        except FileExistsError as error:
            raise EvidenceArchiveError("archive output appeared during pack; refusing overwrite") from error

    metadata = {
        **verify_result,
        "archive": ARCHIVE_BASENAME,
        "archive_sha256": archive_sha,
        "manifest": MANIFEST_BASENAME,
        "manifest_sha256": manifest_sha,
        "source_root": str(source_root),
        "archive_root": ARCHIVE_ROOT,
        "excluded_components": sorted(EXCLUDED_COMPONENTS),
        "excluded_root_files": sorted(EXCLUDED_ROOT_FILES),
    }
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=archive_dir, prefix=".metadata-", delete=False
    ) as stream:
        metadata_temp = Path(stream.name)
        stream.write(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n")
    try:
        os.link(metadata_temp, metadata_path)
        created_outputs.append(metadata_path)
    except FileExistsError as error:
        raise EvidenceArchiveError("archive metadata appeared during pack; refusing overwrite") from error
    finally:
        metadata_temp.unlink(missing_ok=True)
    return metadata


def verify_archive_metadata(archive_dir: Path) -> dict[str, Any]:
    metadata_path = archive_dir / METADATA_BASENAME
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    archive_path = archive_dir / metadata["archive"]
    manifest_path = archive_dir / metadata["manifest"]
    if sha256_file(archive_path) != metadata["archive_sha256"]:
        raise EvidenceArchiveError("archive SHA-256 does not match archive-metadata.json")
    if sha256_file(manifest_path) != metadata["manifest_sha256"]:
        raise EvidenceArchiveError("manifest SHA-256 does not match archive-metadata.json")
    verified = verify_evidence_archive(archive_path, manifest_path)
    for key in ("file_count", "total_source_bytes", "archive_bytes"):
        if verified[key] != metadata[key]:
            raise EvidenceArchiveError(f"metadata mismatch for {key}")
    return {**metadata, "verified": True}


def verify_extracted_tree(extracted_root: Path, manifest_path: Path) -> dict[str, int]:
    """Validate an extracted scylla-wallet directory against the original manifest."""
    entries = read_manifest(manifest_path)
    expected: dict[str, ManifestEntry] = {}
    for entry in entries:
        member_path = PurePosixPath(entry.archive_member)
        if not member_path.parts or member_path.parts[0] != ARCHIVE_ROOT:
            raise EvidenceArchiveError(f"unexpected archive root: {entry.archive_member}")
        relative = PurePosixPath(*member_path.parts[1:]).as_posix()
        expected[relative] = entry
    files = sorted(
        path for path in extracted_root.rglob("*") if path.is_file()
    )
    for path in extracted_root.rglob("*"):
        if path.is_symlink():
            raise EvidenceArchiveError(f"symlink in extracted evidence tree: {path}")
    actual = {path.relative_to(extracted_root).as_posix(): path for path in files}
    if set(actual) != set(expected):
        missing = sorted(set(expected) - set(actual))
        extra = sorted(set(actual) - set(expected))
        raise EvidenceArchiveError(
            f"extracted tree paths differ (missing={len(missing)}, extra={len(extra)})"
        )
    for relative, path in actual.items():
        entry = expected[relative]
        if path.stat().st_size != entry.size_bytes or sha256_file(path) != entry.sha256:
            raise EvidenceArchiveError(f"extracted file hash mismatch: {relative}")
    return {
        "file_count": len(entries),
        "total_source_bytes": sum(entry.size_bytes for entry in entries),
    }


def extract_evidence_archive(
    archive_path: Path, manifest_path: Path, destination: Path
) -> dict[str, int]:
    """Safely extract only manifest-listed regular files, then verify every hash."""
    verify_evidence_archive(archive_path, manifest_path)
    if destination.is_symlink():
        raise EvidenceArchiveError(f"refusing symlink destination: {destination}")
    archive_root = destination / ARCHIVE_ROOT
    if archive_root.exists() or archive_root.is_symlink():
        raise EvidenceArchiveError(f"refusing to overwrite extracted tree: {archive_root}")
    destination.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".scylla-wallet-extract-", dir=destination) as scratch:
        scratch_root = Path(scratch) / ARCHIVE_ROOT
        scratch_root.mkdir()
        with tarfile.open(archive_path, "r:gz") as archive:
            members = archive.getmembers()
            for member in members:
                if member.isdir():
                    continue
                pure_member = PurePosixPath(member.name)
                # verify_evidence_archive checks the complete member list and hashes first.
                relative = PurePosixPath(*pure_member.parts[1:])
                source = archive.extractfile(member)
                if source is None:
                    raise EvidenceArchiveError(f"cannot read tar member: {member.name}")
                output = scratch_root.joinpath(*relative.parts)
                output.parent.mkdir(parents=True, exist_ok=True)
                with output.open("xb") as stream:
                    shutil.copyfileobj(source, stream)
        result = verify_extracted_tree(scratch_root, manifest_path)
        os.rename(scratch_root, archive_root)
    return {**result, "destination": str(archive_root)}


def _small_scalar(value: Any) -> Any:
    if isinstance(value, str) and len(value) > 512:
        return {"truncated": True, "length": len(value), "prefix": value[:256]}
    return value


def project_result_record(value: Any, *, depth: int = 0, max_records: int = 128) -> Any:
    """Keep a readable allowlist of result fields and metric groups."""
    if isinstance(value, list):
        items = [project_result_record(item, depth=depth + 1, max_records=max_records) for item in value[:max_records]]
        result: dict[str, Any] = {"count": len(value), "records": items}
        if len(value) > max_records:
            result["records_truncated"] = True
        return result
    if not isinstance(value, dict):
        return value

    projected: dict[str, Any] = {}
    for key, child in value.items():
        normalized = key.lower()
        if normalized in SUMMARY_SCALAR_KEYS and not isinstance(child, (dict, list)):
            projected[key] = _small_scalar(child)
            continue
        if normalized == "validation" and isinstance(child, dict):
            projected[key] = {
                field: _small_scalar(result)
                for field, result in child.items()
                if isinstance(result, (str, int, float, bool)) or result is None
            }
            continue
        if normalized in SUMMARY_GROUP_KEYS and isinstance(child, (dict, list)):
            projected[key] = project_result_record(
                child, depth=depth + 1, max_records=max_records
            )
            continue
        if normalized in ("error_samples", "failures") and isinstance(child, list):
            projected[key] = {"count": len(child)}
        elif depth == 0 and isinstance(child, list) and normalized in ("trials", "cases"):
            projected[key] = project_result_record(child, depth=depth + 1)
    return projected


def summarize_large_result_json(
    source_path: Path,
    *,
    source_relative_path: str,
    source_sha256: str | None = None,
) -> dict[str, Any]:
    """Create a compact scalar summary while preserving a pointer to the raw JSON."""
    payload = json.loads(source_path.read_text(encoding="utf-8"))
    projection = project_result_record(payload)
    source_keys = sorted(payload) if isinstance(payload, dict) else None
    records = (
        projection.get("trials", projection.get("cases"))
        if isinstance(projection, dict)
        else None
    )
    return {
        "summary_format": "scylla-wallet-selected-json-scalars-v1",
        "source": {
            "path": source_relative_path,
            "size_bytes": source_path.stat().st_size,
            "sha256": source_sha256 or sha256_file(source_path),
        },
        "source_json_top_level_keys": source_keys,
        "top_level": {
            key: value
            for key, value in projection.items()
            if key not in ("trials", "cases")
        }
        if isinstance(projection, dict)
        else projection,
        "result_records": records,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    pack = subparsers.add_parser("pack", help="write archive, full source manifest, metadata")
    pack.add_argument("--source", type=Path, default=Path("target/scylla-wallet"))
    pack.add_argument(
        "--archive-dir",
        type=Path,
        default=Path("/home/neojhou/benchmark-archives/ledger-wallet/scylla-wallet"),
    )

    verify = subparsers.add_parser("verify", help="verify archive and manifest hashes")
    verify.add_argument(
        "--archive-dir",
        type=Path,
        default=Path("/home/neojhou/benchmark-archives/ledger-wallet/scylla-wallet"),
    )

    extract = subparsers.add_parser("extract", help="safely extract and verify archived evidence")
    extract.add_argument("--archive", type=Path)
    extract.add_argument("--manifest", type=Path)
    extract.add_argument("--destination", required=True, type=Path)
    extract.add_argument(
        "--archive-dir",
        type=Path,
        default=Path("/home/neojhou/benchmark-archives/ledger-wallet/scylla-wallet"),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    try:
        if arguments.command == "pack":
            result = pack_evidence_archive(arguments.source, arguments.archive_dir)
        elif arguments.command == "verify":
            result = verify_archive_metadata(arguments.archive_dir)
        else:
            archive_path = arguments.archive or arguments.archive_dir / ARCHIVE_BASENAME
            manifest_path = arguments.manifest or arguments.archive_dir / MANIFEST_BASENAME
            result = extract_evidence_archive(
                archive_path, manifest_path, arguments.destination
            )
    except (EvidenceArchiveError, OSError, json.JSONDecodeError, tarfile.TarError) as error:
        print(f"evidence archive error: {error}", file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
