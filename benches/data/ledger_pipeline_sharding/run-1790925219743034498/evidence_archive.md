# Run 1790925219743034498: archived evidence

The complete original run directory was preserved before repository edits as a gzip tar archive. The archive has one top-level directory, `run-1790925219743034498/`, and contains the original `report.md`, `run.log`, all trial data, and all run metadata. The archive predates this index and its companion `evidence_manifest.csv`; those two files index the original snapshot.

- Archive: `/home/neojhou/benchmark-archives/ledger-wallet/ledger_pipeline_sharding/run-1790925219743034498.tar.gz`
- Compressed size: 362945492 bytes
- Compressed SHA-256: `0d92dcd3d54b5fb69e16c4314b0cff5f36111311ddfb2fd55688545dec990d61`
- Original files: 394
- Original uncompressed file bytes: 1491289639
- Original file inventory: [`evidence_manifest.csv`](evidence_manifest.csv), with a SHA-256 and byte count for every original file

## Availability and extraction

The archive is preserved locally outside Git at `/home/neojhou/benchmark-archives/ledger-wallet/ledger_pipeline_sharding/`. It has not been uploaded, and there is no shared download URL. Standalone Git clones contain the compact summaries and this index, but not the original raw samples or complete logs; retrieve this archive separately for raw percentile and tail analysis. The local backup Git branch `backup/ledger-pipeline-sharding-full-evidence-20261002` is also local only.

Extract and verify the archive from a directory that does not already contain a `run-1790925219743034498/` directory:

```sh
mkdir -p /path/to/evidence
sha256sum /home/neojhou/benchmark-archives/ledger-wallet/ledger_pipeline_sharding/run-1790925219743034498.tar.gz
tar -xzf /home/neojhou/benchmark-archives/ledger-wallet/ledger_pipeline_sharding/run-1790925219743034498.tar.gz -C /path/to/evidence
```

The archive predates `evidence_manifest.csv`, so use the tracked inventory from a Git checkout. From the checkout root, set `root` to the extracted run directory and verify every original file:

```sh
python3 - <<'PY'
import csv
import hashlib
from pathlib import Path

root = Path("/path/to/evidence/run-1790925219743034498")
manifest = Path("benches/data/ledger_pipeline_sharding/run-1790925219743034498/evidence_manifest.csv")
with manifest.open(newline="", encoding="utf-8") as source:
    rows = list(csv.DictReader(source))
for row in rows:
    path = root / row["path"]
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    assert path.stat().st_size == int(row["bytes"]), row["path"]
    assert digest.hexdigest() == row["sha256"], row["path"]
print(f"verified {len(rows)} original files")
PY
```

## Provenance and capture limits

The original `run_manifest.txt` records `git_base_HEAD=21ac904d7015b4de673c42a66593d27d98eddccd`. Its `source_hash_audit.txt` records 10/10 source hashes matched. The original run data and provenance files were left untouched. `parent_run_status.txt` records that the parent `run.log` does not contain all initial build warnings and that one poll response was lost; this archive preserves that original log as captured and does not fill those gaps. Per-trial stdout/stderr, CSVs, statuses, and RocksDB options are preserved as recorded.

The report describes the completed 24-trial formal matrix: 240,000,000 requests total. The archive inventory allows checking every original file independently.
