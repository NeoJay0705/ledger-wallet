import csv
import pathlib
import re
import tempfile
import unittest
import json

from scripts import tikv6_report as gen

ROOT = pathlib.Path(__file__).resolve().parents[1]
COLUMNS = ['batch','role','attempt_dir','status','exit','go_reached','cleanup','wall_s','duration_s','rps_request','p50_s','p95_s','p99_s','max_s','windows','db_cpu_cores','tidb_cpu_cores','flush_await_ms','client_cpu_cores','imported','cause']


class Tikv6ReportTests(unittest.TestCase):
    def fixture(self, root, include=True):
        art = root / 'artifacts'
        art.mkdir(parents=True, exist_ok=True)
        names = ('tidb-k4t4-n16', 'tidb-k6t8-n16', 'tidb-c4-n16', 'tidb-c8x-n16')
        if not include:
            return
        for series in names:
            rows = []
            batches = (2000, 4000, 8000)
            for i, batch in enumerate(batches):
                attempt = f'artifacts/{series}-bm{batch}'
                cause = 'tidb cpu recomputed over full window' if series in ('tidb-k4t4-n16','tidb-k6t8-n16') else ''
                rows.append(dict(batch=batch, role='primary', attempt_dir=attempt, status='ok',
                                 rps_request=100000 + i*10000 + (20000 if 'k6t8' in series else 0),
                                 p50_s='.1', p95_s='.2', p99_s='.3', max_s='.4', windows=gen.driver.expected_windows(batch),
                                 db_cpu_cores='2', tidb_cpu_cores='3.4' if 'k4t4' in series else '4.0',
                                 flush_await_ms='1.2', client_cpu_cores='.5', cause=cause))
                d = root / attempt
                d.mkdir(parents=True)
                summary = {'resource_summary': {'means': {'tidb_cpu_cores_mean': 1.0, 'tikv_cpu_cores_mean': 2.0,
                            'tikv_cpu_pct_of_quota': 50, 'client_cpu_cores_mean': .5},
                            'memory_current_bytes_max': {'tidb': 1024**3},
                            'net': {'tidb': {'rx_mib_s_avg': 1, 'tx_mib_s_avg': 2}},
                            'device_means': {'device_write_mib_s': 1, 'device_write_iops': 2, 'device_util_pct': 3,
                                             'device_flush_await_ms': 1.2, 'device_write_await_ms': 1.5}}}
                (d / 'summary.json').write_text(json.dumps(summary))
            with (art / f'fixconn-{series}.csv').open('w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=COLUMNS)
                writer.writeheader(); writer.writerows(rows)

    def test_denominators_markers_ratios_and_sanitization(self):
        with tempfile.TemporaryDirectory() as td:
            root = pathlib.Path(td); self.fixture(root)
            text = gen.build(root)
            table = text.split('## 2. New-configuration measurements',1)[1].split('## 3.',1)[0]
            self.assertIn('k4t4 / 2000', table)
            self.assertIn('3.40 / 85.0%*†', table)
            self.assertIn('4.00 / 50.0%†', table)
            comp = text.split('## 3. Equal-batch comparison',1)[1].split('## 4.',1)[0]
            self.assertIn('1.20', comp)  # k6t8 / k4t4
            self.assertIn('20.0%', comp) # k6t8 vs c8x reference
            self.assertNotRegex(text.lower(), r'nan|none')

    def test_empty_artifacts_produce_no_data_without_crashing(self):
        with tempfile.TemporaryDirectory() as td:
            root = pathlib.Path(td); self.fixture(root, include=False)
            text = gen.build(root)
            self.assertIn('no data', text)
            section = text.split('## 2. New-configuration measurements',1)[1].split('## 3.',1)[0]
            self.assertEqual(len(re.findall(r'^\| k4t4 / (?:2000|4000|8000) \|', section, re.M)), 3)
            self.assertEqual(len(re.findall(r'^\| k6t8 / (?:2000|4000|8000) \|', section, re.M)), 3)
            self.assertNotRegex(text.lower(), r'nan|none')

    def test_real_artifacts_have_three_new_rows_per_config(self):
        text = gen.build(ROOT)
        section = text.split('## 2. New-configuration measurements',1)[1].split('## 3.',1)[0]
        self.assertEqual(len(re.findall(r'^\| k4t4 / (?:2000|4000|8000) \|', section, re.M)), 3)
        self.assertEqual(len(re.findall(r'^\| k6t8 / (?:2000|4000|8000) \|', section, re.M)), 3)
        self.assertNotRegex(text.lower(), r'nan|none')


if __name__ == '__main__':
    unittest.main()
