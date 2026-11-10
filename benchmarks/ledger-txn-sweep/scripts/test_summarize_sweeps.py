import csv
import pathlib
import re
import unittest

from scripts import summarize_sweeps

ROOT = pathlib.Path(__file__).resolve().parents[1]
ARTIFACTS = ROOT / "artifacts"


@unittest.skipUnless(ARTIFACTS.is_dir(), "published artifacts/ evidence is absent")
class SummarizeSweepsTests(unittest.TestCase):
    def csv_rows(self, relative):
        with (ARTIFACTS / relative).open(newline="", encoding="utf-8") as stream:
            return list(csv.DictReader(stream))

    def test_fixconn_saturation_values_match_report_evidence(self):
        expected = {
            "fixconn-pg-c4-n16.csv": (8000, 245300),
            "fixconn-tidb-c4-n16.csv": (2000, 128300),
            "fixconn-pg-c8-n16.csv": (8000, 357200),
            "fixconn-tidb-c8x-n16.csv": (4000, 152800),
            "fixconn-tidb-c8x-n32.csv": (2000, 147300),
        }
        text = summarize_sweeps.build()
        for name, (batch, expected_rps) in expected.items():
            rows = [r for r in self.csv_rows(name) if r["status"] == "ok" and r["role"] in ("primary", "flush_rerun")]
            effective = summarize_sweeps.fixconn_sweep.effective_primary(rows)
            sat, _label = summarize_sweeps.fixconn_sweep.saturation(rows, summarize_sweeps.fixconn_sweep.completion(rows, name))
            self.assertEqual(sat, batch, name)
            rps = float(effective[batch]["rps_request"])
            self.assertEqual(round(rps / 1000, 1), round(expected_rps / 1000, 1), name)
            series = name.removesuffix(".csv").replace("fixconn-", "")
            self.assertIn(f"| {series} | {batch} | {rps / 1000:.1f}k |", text)

    def test_tikv6_reported_rps_and_table_values_are_csv_derived(self):
        expected = {("fixconn-tidb-k4t4-n16.csv", 2000): 127600,
                    ("fixconn-tidb-k6t8-n16.csv", 4000): 152300,
                    ("fixconn-tidb-k6t8-n16.csv", 8000): 151500}
        text = summarize_sweeps.build()
        for (name, batch), expected_rps in expected.items():
            row = next(r for r in self.csv_rows(name) if int(r["batch"]) == batch and r["status"] == "ok")
            actual = float(row["rps_request"])
            self.assertEqual(round(actual / 1000, 1), round(expected_rps / 1000, 1))
            config = "k4t4" if "k4t4" in name else "k6t8"
            self.assertIn(f"| {config} | {batch} | {actual / 1000:.1f}k |", text)

    def test_r1_comparison_uses_published_summary_json_and_marks_missing(self):
        text = summarize_sweeps.build()
        for db in ("pg", "tidb"):
            for cpus in (4, 8):
                for batch in (4000, 8000):
                    row = next((r for r in summarize_sweeps.batch_sweep.read_rows(db, cpus)
                                if int(r.get("batch") or 0) == batch and r.get("status") == "ok"), None)
                    value = summarize_sweeps.r1_summary_value(row, db)
                    cell = f"| {db.upper()} CPU{cpus} / {batch} | {summarize_sweeps.compact(value)} |"
                    self.assertIn(cell, text)
        self.assertIn("| TIDB CPU4 / 8000 | 沒量 | 沒量 |", text)

    def test_summary_every_numeric_rps_cell_is_a_source_csv_value(self):
        text = summarize_sweeps.measurement_tables()
        # Verify rendered fixed-sweep per-batch RPS cells against their exact selected CSV rows.
        for series in summarize_sweeps.fixconn_sweep.ORIGINAL_SERIES:
            meta = summarize_sweeps.fixconn_sweep.SERIES[series]
            suffix = summarize_sweeps.fixconn_sweep.series_meta(series)["suffix"]
            rows = summarize_sweeps.read_csv(ARTIFACTS / f"fixconn-{meta[0]}-{suffix}-n{meta[2]}.csv")
            effective = summarize_sweeps.fixconn_sweep.effective_primary(rows)
            meta = summarize_sweeps.fixconn_sweep.SERIES[series]
            db, cpus, _connections, _extended = meta
            for batch, row in effective.items():
                value = float(row["rps_request"])
                summary = summarize_sweeps.fixconn_report.summary(row)
                resources = summary.get("resource_summary") or {}
                db_cpu = ((resources.get("means") or {}).get("db_cpu_core_equivalents")
                          if db == "pg" else resources.get("tikv_cpu_cores_mean"))
                tidb_cpu = summarize_sweeps.number(row.get("tidb_cpu_cores"))
                if tidb_cpu is None:
                    tidb_cpu = summarize_sweeps.number(resources.get("tidb_cpu_cores_mean"))
                tidb_pct = (summarize_sweeps.fmt(tidb_cpu / (6 if "c8x" in series else cpus) * 100, 1)
                            if db == "tidb" and tidb_cpu is not None else "沒量")
                expected = (f"| {series} | {batch} | {value / 1000:.1f}k | "
                            f"{summarize_sweeps.fmt(row.get('p99_s'))} | "
                            f"{summarize_sweeps.fmt(db_cpu, 2)} | {tidb_pct} | "
                            f"{summarize_sweeps.fmt(row.get('flush_await_ms'), 2)}")
                self.assertIn(expected, text)
        self.assertIn("沒量", text)


if __name__ == "__main__":
    unittest.main()
