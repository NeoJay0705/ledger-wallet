#!/usr/bin/env python3
"""Render a Markdown summary from the curated ledger sweep evidence."""
import argparse
import csv
import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))
import batch_sweep
import conn_sweep
import fixconn_report
import fixconn_sweep
import tikv6_report


def number(value):
    return fixconn_report.number(value)


def compact(value):
    value = number(value)
    return f"{value / 1000:.1f}k" if value is not None else "沒量"


def fmt(value, digits=3):
    value = number(value)
    return f"{value:.{digits}f}" if value is not None else "沒量"


def read_csv(path):
    if not path.is_file():
        return []
    with path.open(newline="", encoding="utf-8") as stream:
        return list(csv.DictReader(stream))


def effective_rows(rows):
    return fixconn_sweep.effective_primary(rows)


def r1_summary_value(row, db, root=ROOT):
    """Read R1 throughput from its published JSON evidence via batch helper."""
    if not row or not row.get("attempt_dir"):
        return None
    path = pathlib.Path(root) / row["attempt_dir"]
    candidates = [path] if path.suffix == ".json" else [path / "summary.json"]
    if path.name == "result.json":
        candidates.append(path.parent.parent / "summary.json")
    for candidate in candidates:
        try:
            summary = json.loads(candidate.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        extracted = batch_sweep.extract_row(db, summary)
        if number(extracted.get("rps_request")) is not None:
            return number(extracted["rps_request"])
    return None


def measurement_tables(root=ROOT):
    """Build tables while reusing the report/driver selection helpers."""
    global ROOT
    old = ROOT
    ROOT = pathlib.Path(root)
    try:
        lines = ["## 固定連線 batch sweep（16 connections）", "",
                 "飽和點採來源 driver 定義：最小 batch，其 RPS 達成功點最大值的 95%；若曲線只到達上限或提前終止，標示觀測範圍。", "",
                 "| 曲線 | batch | RPS | p99 (s) | DB CPU (cores) | TiDB-server CPU (% cpuset) | flush await (ms) |", "|---|---:|---:|---:|---:|---:|---:|"]
        for series in fixconn_sweep.ORIGINAL_SERIES:
            meta = fixconn_sweep.SERIES[series]
            suffix = fixconn_sweep.series_meta(series)["suffix"]
            rows = read_csv(ROOT / "artifacts" / f"fixconn-{meta[0]}-{suffix}-n{meta[2]}.csv")
            chosen = effective_rows(rows)
            reason = fixconn_sweep.completion(rows, series)
            sat, label = fixconn_sweep.saturation(rows, reason)
            db, cpus, _n, _extended = fixconn_sweep.SERIES[series]
            for batch in fixconn_sweep.grid_for(series):
                row = chosen.get(batch, {})
                summary = fixconn_report.summary(row) if row else {}
                resources = summary.get("resource_summary") or {}
                means = resources.get("means") or {}
                db_cpu = (means.get("db_cpu_core_equivalents") if db == "pg"
                          else resources.get("tikv_cpu_cores_mean"))
                tidb_pct = "沒量"
                if db == "tidb":
                    cpu = number(row.get("tidb_cpu_cores"))
                    if cpu is None:
                        cpu = number(means.get("tidb_cpu_cores_mean"))
                    cpuset = 6 if "c8x" in series else cpus
                    tidb_pct = fmt(cpu / cpuset * 100 if cpu is not None else None, 1)
                marker = " ← 飽和" if batch == sat else ""
                lines.append(f"| {series} | {batch} | {compact(row.get('rps_request'))} | {fmt(row.get('p99_s'))} | {fmt(db_cpu, 2)} | {tidb_pct} | {fmt(row.get('flush_await_ms'), 2)}{marker} |")
            lines.append(f"| **{series} 飽和** | — | batch {sat if sat is not None else '沒量'} / {label or '沒量'} | — | — | — | — |")

        lines += ["", "## TiKV 配置補測", "", "| 配置 | batch | RPS | p99 (s) | TiKV CPU (cores) | TiDB-server CPU (cores / % cpuset) | flush await (ms) | 飽和點 |", "|---|---:|---:|---:|---:|---:|---:|---|"]
        for series, config in (("tidb-k4t4-n16", "k4t4"), ("tidb-k6t8-n16", "k6t8")):
            meta = fixconn_sweep.SERIES[series]
            suffix = fixconn_sweep.series_meta(series)["suffix"]
            rows = read_csv(ROOT / "artifacts" / f"fixconn-{meta[0]}-{suffix}-n{meta[2]}.csv")
            chosen = effective_rows(rows)
            sat, _label = fixconn_sweep.saturation(rows, fixconn_sweep.completion(rows, series))
            cpuset = fixconn_sweep.series_meta(series)["tidb_cpuset_size"]
            for batch in tikv6_report.BATCHES:
                row = chosen.get(batch, {})
                rsrc = fixconn_report.summary(row).get("resource_summary") or {} if row else {}
                tidb_cpu = number(row.get("tidb_cpu_cores")) or number(rsrc.get("tidb_cpu_cores_mean"))
                tidb_cell = (f"{fmt(tidb_cpu, 2)} / {fmt(tidb_cpu / cpuset * 100, 1)}%"
                             if tidb_cpu is not None else "沒量")
                marker = "是" if batch == sat else ""
                lines.append(f"| {config} | {batch} | {compact(row.get('rps_request'))} | {fmt(row.get('p99_s'))} | {fmt(rsrc.get('tikv_cpu_cores_mean'), 2)} | {tidb_cell} | {fmt(row.get('flush_await_ms'), 2)} | {marker} |")

        lines += ["", "## Batch sweep（256 connections）", "", "| DB / CPU | 峰值 batch | 峰值 RPS | 飽和 batch |", "|---|---:|---:|---:|"]
        for db in ("pg", "tidb"):
            for cpus in (4, 8):
                rows = batch_sweep.read_rows(db, cpus)
                # batch_sweep owns the effective rerun selection and 95% saturation rule.
                successful = [r for r in rows if r.get("role") in ("primary", "flush_rerun") and r.get("status") == "ok" and number(r.get("rps_request")) is not None]
                for batch in {r.get("batch") for r in successful}:
                    rerun = [r for r in successful if r.get("batch") == batch and r.get("role") == "flush_rerun"]
                    if rerun:
                        successful = [r for r in successful if not (r.get("batch") == batch and r.get("role") == "primary")]
                sat = batch_sweep.saturation_point([dict(r, role="primary") for r in successful])
                peak = max(successful, key=lambda r: float(r["rps_request"])) if successful else {}
                lines.append(f"| {db.upper()} CPU{cpus} | {peak.get('batch', '沒量')} | {compact(peak.get('rps_request'))} | {sat.get('batch') if sat else '沒量'} |")

        lines += ["", "## Connection sweep（batch 50 / 100）", "", "| DB / CPU / batch | 飽和 connections | RPS |", "|---|---:|---:|"]
        for db in ("pg", "tidb"):
            for cpus in (4, 8):
                for batch in conn_sweep.BATCHES:
                    rows = conn_sweep.rows_for(db, cpus, batch)
                    sat = conn_sweep.saturation(rows, conn_sweep.GRIDS[cpus])
                    chosen = conn_sweep.effective_primary(rows)
                    n = sat.get("conns")
                    row = chosen.get(n, {}) if n is not None else {}
                    lines.append(f"| {db.upper()} CPU{cpus} / {batch} | {n if n is not None else '沒量'} | {compact(row.get('rps_request'))} |")

        lines += ["", "## R1 對照（summary.json 發布值）", "", "| DB / CPU / batch | RPS | 資料狀態 |", "|---|---:|---|"]
        for db in ("pg", "tidb"):
            for cpus in (4, 8):
                for batch in (4000, 8000):
                    rows = batch_sweep.read_rows(db, cpus)
                    row = next((r for r in rows if int(r.get("batch") or 0) == batch and r.get("status") == "ok"), None)
                    value = r1_summary_value(row, db, ROOT)
                    lines.append(f"| {db.upper()} CPU{cpus} / {batch} | {compact(value)} | {'已量測' if value is not None else '沒量'} |")
        return "\n".join(lines) + "\n"
    finally:
        ROOT = old


def build(root=ROOT):
    return "# Sweep 結果摘要\n\n" + measurement_tables(root)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=pathlib.Path, help="選擇性寫入 Markdown 檔案；預設只輸出 stdout")
    args = parser.parse_args(argv)
    text = build()
    if args.out:
        output = args.out if args.out.is_absolute() else pathlib.Path.cwd() / args.out
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(text, encoding="utf-8")
    print(text, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
