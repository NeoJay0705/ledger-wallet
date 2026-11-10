import csv
import pathlib
import tempfile
import unittest
from unittest import mock
import sys
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import conn_sweep as cs


def good_summary(db='pg', **changes):
    resource = ({'means': {'device_flush_await_ms': 0.4}} if db == 'pg'
                else {'device_means': {'device_flush_await_ms': 0.4}, 'resource_window_formal': True})
    summary = {
        'verification': {'valid': True},
        'effective': {'users': 50000, 'requests_per_user': 40, 'queues': 256,
                      'connections': 4, 'batch_max': 50},
        'requests': 2_000_000,
        'resource_summary': resource,
    }
    if db == 'pg':
        summary['resource_window_formal'] = True
    else:
        summary['durability'] = {'valid': True}
    summary.update(changes)
    return summary


class ConnSweepTests(unittest.TestCase):
    def test_argv_contract_both_databases_every_flag(self):
        self.assertEqual(cs.wrapper_argv('pg', 4, 50, 8, 'D'), [
            'python3', 'scripts/batch_queue_run.py', 'D', '--mode', 'txn',
            '--cpus', '4', '--users', '50000', '--requests-per-user', '40',
            '--queues', '256', '--connections', '8', '--batch-max', '50',
            '--sample-interval-ms', '250', '--pg-conf', 'wal_buffers=256MB',
            '--pg-conf', 'max_wal_size=8GB', '--pg-conf', 'track_wal_io_timing=on'])
        self.assertEqual(cs.wrapper_argv('tidb', 8, 100, 16, 'D'), [
            'python3', 'scripts/txn_tidb_run.py', 'D', '--cpus', '8',
            '--users', '50000', '--requests-per-user', '40', '--queues', '256',
            '--connections', '16', '--batch-max', '100', '--sample-interval-ms', '250'])

    def test_dir_naming_and_next_index_from_dirs_and_csv(self):
        self.assertEqual(cs.attempt_name('pg', 4, 50, 8, 2),
                         'artifacts/connsweep-pg-c4-bm50-n8-a2')
        with tempfile.TemporaryDirectory() as td:
            cs.ART = pathlib.Path(td)
            (cs.ART / 'connsweep-pg-c4-bm50-n8-a3-idle.json').touch()
            p = cs.csv_path('pg', 4, 50)
            with p.open('w', newline='') as f:
                w = csv.DictWriter(f, fieldnames=cs.COLUMNS); w.writeheader()
                w.writerow({'conns': 8, 'attempt_dir': 'artifacts/connsweep-pg-c4-bm50-n8-a7'})
            self.assertEqual(cs.next_index('pg', 4, 50, 8), 8)

    def test_ok_predicate_rejects_all_invalid_requirements(self):
        def ok(db='pg', summary=None, cleanup=None, exit_code=0):
            return cs.is_ok(db, summary or good_summary(db), cleanup or {'status': 'success'},
                            exit_code, connections=4, batch=50)
        self.assertTrue(ok())
        self.assertTrue(ok(db='tidb'))
        s = good_summary('tidb'); s['resource_summary']['resource_window_formal'] = False
        self.assertFalse(ok(db='tidb', summary=s))
        for mutate in (
            lambda s: s.update(requests=1),
            lambda s: s.update(resource_window_formal=False),
            lambda s: s.update(resource_summary={'means': {}}),
            lambda s: s.update(verification={'valid': False}),
            lambda s: s['effective'].update(connections=2),
        ):
            s = good_summary(); mutate(s); self.assertFalse(ok(summary=s))
        self.assertFalse(ok(cleanup={'status': 'failed'}))
        self.assertFalse(ok(exit_code=1))
        self.assertFalse(ok(db='tidb', summary=good_summary('tidb', durability={'valid': False})))

    def _nonformal_tidb(self):
        s = good_summary('tidb')
        s['resource_summary']['resource_window_formal'] = False
        s['resource_summary']['container_resource_summaries'] = {
            'res-tikv1': {'formal': True, 'window_start_unix_ns': 1_000_000_000,
                          'window_end_unix_ns': 3_000_000_000},
            'res-pd': {'formal': True},
            'res-tidb': {'formal': False, 'missing_intervals': [
                {'reason': 'required cumulative counter/memory sample missing'}]},
        }
        return s

    def _resources(self, directory, blank_cpu=False):
        d = directory / 'res-tidb'; d.mkdir(parents=True, exist_ok=True)
        with (d / 'resources.csv').open('w', newline='') as f:
            w = csv.DictWriter(f, fieldnames=['wall_utc', 'monotonic', 'db_cpu_usage_usec',
                                               'memory_current_bytes', 'allocated_cpu_capacity_cores'])
            w.writeheader()
            w.writerows([
                {'wall_utc': '1970-01-01T00:00:00Z', 'monotonic': '0', 'db_cpu_usage_usec': '1000000', 'memory_current_bytes': '10', 'allocated_cpu_capacity_cores': '4'},
                {'wall_utc': '1970-01-01T00:00:01Z', 'monotonic': '1', 'db_cpu_usage_usec': '2000000', 'memory_current_bytes': '11', 'allocated_cpu_capacity_cores': '4'},
                {'wall_utc': '1970-01-01T00:00:02Z', 'monotonic': '2', 'db_cpu_usage_usec': '' if blank_cpu else '4000000', 'memory_current_bytes': '12', 'allocated_cpu_capacity_cores': '4'},
                {'wall_utc': '1970-01-01T00:00:03Z', 'monotonic': '3', 'db_cpu_usage_usec': '5000000', 'memory_current_bytes': '13', 'allocated_cpu_capacity_cores': '4'},
            ])

    def test_tidb_full_window_cpu_baseline_and_blank_cpu(self):
        with tempfile.TemporaryDirectory() as td:
            d = pathlib.Path(td); s = self._nonformal_tidb(); self._resources(d)
            cores, pct = cs.tidb_full_window_cpu(d, s)
            self.assertAlmostEqual(cores, 1.5); self.assertAlmostEqual(pct, 37.5)
            self._resources(d, blank_cpu=True)
            self.assertIsNone(cs.tidb_full_window_cpu(d, s))

    def test_tidb_acceptance_reclassification_paths(self):
        s = self._nonformal_tidb()
        with tempfile.TemporaryDirectory() as td:
            d = pathlib.Path(td); self._resources(d)
            args = ('tidb', s, {'status': 'success'}, 0)
            self.assertTrue(cs.is_ok(*args, connections=4, batch=50, attempt_dir=d))
            self.assertFalse(cs.is_ok(*args, connections=4, batch=50))
            s['resource_summary']['container_resource_summaries']['res-tikv1']['formal'] = False
            self.assertFalse(cs.is_ok(*args, connections=4, batch=50, attempt_dir=d))
            s['resource_summary']['container_resource_summaries']['res-tikv1']['formal'] = True
            self._resources(d, blank_cpu=True)
            self.assertFalse(cs.is_ok(*args, connections=4, batch=50, attempt_dir=d))
        self.assertTrue(cs.is_ok('tidb', good_summary('tidb'), {'status': 'success'}, 0,
                                 connections=4, batch=50))

    def test_reclassify_appends_once_and_duplicate_primary_prefers_ok(self):
        with tempfile.TemporaryDirectory() as td:
            oldroot, oldart = cs.ROOT, cs.ART
            try:
                cs.ROOT = pathlib.Path(td); cs.ART = cs.ROOT / 'artifacts'
                rel = 'artifacts/connsweep-tidb-c4-bm50-n4-a1'; d = cs.ROOT / rel
                d.mkdir(parents=True); self._resources(d)
                s = self._nonformal_tidb(); s['effective'].update(connections=4, batch_max=50)
                (d / 'summary.json').write_text(__import__('json').dumps(s))
                (d / 'cleanup.json').write_text('{"status":"success"}')
                (d / 'client').mkdir(); (d / 'client' / 'result.json').write_text('{}')
                (cs.csv_path('tidb', 4, 50)).parent.mkdir(parents=True, exist_ok=True)
                cs.append('tidb', 4, 50, {'conns': 4, 'role': 'primary', 'attempt_dir': rel,
                                         'status': 'incomplete'})
                added = cs.reclassify('tidb', 4, 50)
                self.assertEqual(len(added), 1)
                self.assertEqual(cs.reclassify('tidb', 4, 50), [])
                rows = cs.rows_for('tidb', 4, 50)
                self.assertEqual(len(rows), 2)
                self.assertEqual(cs.effective_primary(rows)[4]['status'], 'ok')
                self.assertEqual(cs.effective_primary(rows)[4]['cause'], cs.TIDB_RECLASSIFY_NOTE)
            finally:
                cs.ROOT, cs.ART = oldroot, oldart

    def test_effective_primary_flush_replacement_failure_flag_replicate_exclusion(self):
        rows = [
            {'conns': '4', 'role': 'primary', 'status': 'ok', 'rps_request': '90'},
            {'conns': '4', 'role': 'flush_rerun', 'status': 'ok', 'rps_request': '80'},
            {'conns': '8', 'role': 'primary', 'status': 'ok', 'rps_request': '100'},
            {'conns': '8', 'role': 'flush_rerun', 'status': 'incomplete'},
            {'conns': '4', 'role': 'replicate', 'status': 'ok', 'rps_request': '999'},
        ]
        eff = cs.effective_primary(rows)
        self.assertEqual(eff[4]['rps_request'], '80')
        self.assertEqual(eff[8]['cause'], 'flush>2ms, rerun failed')
        self.assertNotEqual(eff[4]['rps_request'], '999')

    def test_saturation_uses_replacement_incomplete_and_256_label(self):
        rows = [{'conns': str(n), 'role': 'primary', 'status': 'ok', 'rps_request': str(r)}
                for n, r in ((4, 90), (8, 100), (16, 95))]
        rows.append({'conns': '8', 'role': 'flush_rerun', 'status': 'ok', 'rps_request': '80'})
        self.assertEqual(cs.saturation(rows, [4, 8, 16])['conns'], 16)
        self.assertEqual(cs.saturation(rows, [4, 8, 16, 32])['status'], 'incomplete')
        full = [{'conns': str(n), 'role': 'primary', 'status': 'ok', 'rps_request': str(n)}
                for n in (4, 8, 16, 32, 64, 128, 256)]
        self.assertEqual(cs.saturation(full, [4, 8, 16, 32, 64, 128, 256])['label'],
                         'observed-grid saturation')

    def test_replicate_points_normal_first_and_incomplete(self):
        rows = [{'conns': str(n), 'role': 'primary', 'status': 'ok', 'rps_request': str(r)}
                for n, r in ((4, 100), (8, 90), (16, 50))]
        self.assertEqual(cs.replicate_points(rows, [4, 8, 16]), [4])
        rows[0]['rps_request'] = '70'; rows[1]['rps_request'] = '100'
        self.assertEqual(cs.replicate_points(rows, [4, 8, 16]), [8, 4])
        self.assertEqual(cs.replicate_points(rows[:2], [4, 8, 16]), [])

    def test_resume_skips_ok_finishes_flush_and_skips_replicate(self):
        rows = [{'conns': str(n), 'role': 'primary', 'status': 'ok', 'flush_await_ms': '0.2'}
                for n in cs.GRIDS[4]]
        rows[1]['flush_await_ms'] = '3'
        rows.append({'conns': '8', 'role': 'replicate', 'status': 'ok'})
        with mock.patch.object(cs, 'rows_for', return_value=rows), mock.patch.object(cs, 'attempt') as attempt:
            cs.run('pg', 4, 50)
            self.assertEqual(attempt.call_args_list, [mock.call('pg', 4, 50, 8, 'flush_rerun', False)])
        with mock.patch.object(cs, 'rows_for', return_value=[
            {'conns': str(n), 'role': 'primary', 'status': 'ok', 'rps_request': str(100-n)}
            for n in cs.GRIDS[4]] + [{'conns': '4', 'role': 'replicate'}]), mock.patch.object(cs, 'attempt') as attempt:
            cs.replicate('pg', 4, 50)
            self.assertNotIn(mock.call('pg', 4, 50, 4, 'replicate', False), attempt.call_args_list)

    def test_sampler_retry_bounded_at_two_fresh_attempts(self):
        rows = []
        returns = [{'status': 'incomplete', 'cause': 'sampler'}] * 3
        with mock.patch.object(cs, 'GRIDS', {4: [4]}), mock.patch.object(cs, 'rows_for', return_value=[]), mock.patch.object(cs, 'attempt', side_effect=returns) as attempt:
            cs.run('pg', 4, 50)
            self.assertEqual(attempt.call_count, 3)

    def test_cleanup_failure_stops_campaign(self):
        with mock.patch.object(cs, 'next_index', return_value=1), mock.patch.object(cs.bs, 'idle_gate', return_value={'status': 'ok'}), \
             mock.patch.object(cs.subprocess, 'run', return_value=mock.Mock(returncode=0)), \
             mock.patch.object(cs, 'csv_path', return_value=pathlib.Path(tempfile.gettempdir()) / 'no-write.csv'):
            with tempfile.TemporaryDirectory() as td:
                oldroot, oldart = cs.ROOT, cs.ART
                try:
                    cs.ROOT = pathlib.Path(td); cs.ART = cs.ROOT / 'artifacts'
                    (cs.ROOT / 'artifacts/connsweep-pg-c4-bm50-n4-a1').mkdir(parents=True)
                    (cs.ROOT / 'artifacts/connsweep-pg-c4-bm50-n4-a1/cleanup.json').write_text('{"status":"failed"}')
                    with self.assertRaises(RuntimeError): cs.attempt('pg', 4, 50, 4, 'primary')
                finally:
                    cs.ROOT, cs.ART = oldroot, oldart

    def test_other_failure_recorded_and_sweep_continues(self):
        outcomes = iter([{'status': 'incomplete', 'cause': 'other'}])
        with mock.patch.object(cs, 'GRIDS', {4: [4, 8]}), mock.patch.object(cs, 'rows_for', return_value=[]), mock.patch.object(cs, 'attempt', side_effect=lambda *a: next(outcomes, None)) as attempt:
            cs.run('pg', 4, 50)
            self.assertEqual(attempt.call_count, 2)

    def test_idle_gate_failure_recorded(self):
        with tempfile.TemporaryDirectory() as td:
            oldroot, oldart = cs.ROOT, cs.ART
            try:
                cs.ROOT = pathlib.Path(td); cs.ART = cs.ROOT / 'artifacts'
                with mock.patch.object(cs, 'next_index', return_value=1), \
                     mock.patch.object(cs.bs, 'idle_gate', return_value={'status': 'gate_failed'}), \
                     mock.patch.object(cs, 'append') as append, mock.patch.object(cs.subprocess, 'run') as run:
                    row = cs.attempt('pg', 4, 50, 4, 'primary')
                    self.assertEqual(row['status'], 'gate_failed')
                    self.assertEqual(row['cause'], 'idle gate')
                    self.assertTrue((cs.ROOT / (row['attempt_dir'] + '-idle.json')).exists())
                    run.assert_not_called(); append.assert_called_once()
            finally:
                cs.ROOT, cs.ART = oldroot, oldart


if __name__ == '__main__':
    unittest.main()
