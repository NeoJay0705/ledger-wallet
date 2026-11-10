import csv
import json
import pathlib
import tempfile
import unittest

import batch_sweep as bs

# Minimal fixtures use the exact keys and nesting present in the real wrapper summaries.
def pg_summary(flush=0.37):
    return {'duration_s': 23.5, 'rps_request': 170000.0,
            'latency_us': {'p50': 1000000, 'p95': 2000000, 'p99': 3000000, 'max': 4000000},
            'outcomes': {'windows': 512}, 'verification': {'valid': True},
            'resource_summary': {'means': {'db_cpu_core_equivalents': 3.9,
                'device_flush_await_ms': flush}}}

def tidb_summary():
    return {'duration_s': 64.8, 'rps_request': 61600.0,
            'latency_us': {'p50': 10_000_000, 'p95': 18_000_000, 'p99': 24_000_000, 'max': 27_000_000},
            'outcomes': {'windows': 1024}, 'verification': {'valid': True},
            'resource_summary': {'tikv_cpu_cores_mean': 2.4, 'tidb_cpu_cores_mean': 2.29,
                'device_means': {'device_flush_await_ms': 1.33}}}

class BatchSweepTests(unittest.TestCase):
    def test_idle_boundaries_each_cpu_and_proc(self):
        self.assertEqual(bs.evaluate({0:90,1:90},{'p':30}), (True,[]))
        self.assertFalse(bs.evaluate({0:89.9,1:90},{})[0])
        self.assertFalse(bs.evaluate({0:90,1:89.9},{})[0])
        self.assertFalse(bs.evaluate({0:90},{"p":30.1})[0])

    def test_extract_pg_tidb_composite_and_null_flush(self):
        p=bs.extract_row('pg',pg_summary())
        self.assertEqual((p['duration_s'],p['rps_request'],p['p50_s'],p['windows'],p['db_cpu_cores']), (23.5,170000,1,512,3.9))
        self.assertFalse(p['flush_missing'])
        t=bs.extract_row('tidb',tidb_summary())
        self.assertEqual((t['p95_s'],t['db_cpu_cores'],t['tidb_cpu_cores'],t['flush_await_ms']), (18,2.4,2.29,1.33))
        resource={'means':{'db_cpu_core_equivalents':3.948889543144514,'device_flush_await_ms':0.37277486910994767}}
        composite=bs.extract_row('pg',{'summary':pg_summary(),'resource':resource})
        self.assertEqual(composite['db_cpu_cores'],3.948889543144514)
        self.assertEqual(composite['flush_await_ms'],0.37277486910994767)
        missing=bs.extract_row('pg',pg_summary(None))
        self.assertTrue(missing['flush_missing']);self.assertIsNone(missing['flush_await_ms'])
        self.assertIsNone(bs.extract_row('pg',{'duration_s':1})['db_cpu_cores'])

    def test_saturation_primary_only_failure_excluded_and_label(self):
        rows=[{'batch':50,'role':'primary','status':'ok','rps_request':90},
              {'batch':100,'role':'primary','status':'ok','rps_request':95},
              {'batch':250,'role':'primary','status':'incomplete','rps_request':1000},
              {'batch':500,'role':'replicate','status':'ok','rps_request':1000}]
        self.assertEqual(bs.saturation_point(rows),{'batch':100,'label':'saturation point'})
        self.assertEqual(bs.saturation_point([{'batch':50,'status':'ok','rps_request':100}]),
                         {'batch':50,'label':'observed-grid saturation'})
        self.assertIsNone(bs.saturation_point([{'batch':4,'status':'incomplete','rps_request':1}]))

    def test_failure_without_summary_has_no_metrics(self):
        # The pure driver-row contract: failure rows carry facts but never zero-valued metrics.
        row={'batch':50,'status':'incomplete','exit':1,'go_reached':'false','cause':'other'}
        self.assertNotIn('rps_request',row);self.assertNotIn('duration_s',row)
        self.assertEqual(bs.classify_cause('OOM 8176'), 'memory (8176)')
        self.assertEqual(bs.classify_cause('request timeout'), 'timeout')
        self.assertEqual(bs.classify_cause('sampler failed'), 'sampler')
        self.assertEqual(bs.classify_cause('',cleanup='failed'), 'cleanup residue')

    def test_sampler_failure_is_not_terminal(self):
        rows=[{'batch':'1000','role':'primary','status':'incomplete','go_reached':'true','cause':'sampler'}]
        self.assertFalse(bs.terminal_tidb_failure(rows))
        rows[0]['cause']='memory (8176)'
        self.assertTrue(bs.terminal_tidb_failure(rows))

    def test_tidb_terminal_failure_resume_and_8000_gate(self):
        rows=[{'batch':'6000','role':'primary','status':'incomplete','go_reached':'true'}]
        # CSV round trip is the persistence mechanism used by resume.
        with tempfile.TemporaryDirectory() as td:
            f=pathlib.Path(td)/'state.csv'
            with f.open('w',newline='') as h:
                w=csv.DictWriter(h,fieldnames=['batch','role','status','go_reached']);w.writeheader();w.writerows(rows)
            with f.open(newline='') as h:restored=list(csv.DictReader(h))
            self.assertTrue(bs.terminal_tidb_failure(restored))
            self.assertFalse(bs.allows_batch('tidb',8000,restored))
        valid=[{'batch':'6000','role':'primary','status':'ok'}]
        self.assertTrue(bs.allows_batch('tidb',8000,valid))
        self.assertTrue(bs.allows_batch('pg',8000,[]))
        self.assertFalse(bs.terminal_tidb_failure([{'role':'primary','status':'incomplete','go_reached':'false'}]))

    def test_flush_once_and_attempt_names_are_fresh(self):
        r={'flush_await_ms':'2.01'}
        self.assertTrue(bs.should_flush_rerun(r,0));self.assertFalse(bs.should_flush_rerun(r,1))
        self.assertFalse(bs.should_flush_rerun({'flush_await_ms':'2'},0))
        self.assertEqual(bs.attempt_name('pg',4,100,1),'artifacts/sweep-pg-c4-bm100-a1')
        self.assertNotEqual(bs.attempt_name('pg',4,100,1),bs.attempt_name('pg',4,100,2))

    def test_replicate_selection(self):
        rows=[{'batch':50,'status':'ok','rps_request':60}, {'batch':100,'status':'ok','rps_request':99},
              {'batch':250,'status':'ok','rps_request':100}]
        self.assertEqual(bs.replicate_points(rows,[50,100,250]),[100,50])
        self.assertEqual(bs.replicate_points([{'batch':50,'status':'ok','rps_request':100}], [50,100]),[50,100])

    def test_wrapper_argv_fixed_contract(self):
        pg=bs.wrapper_argv('pg',4,250,'artifacts/x')
        self.assertEqual(pg,['python3','scripts/batch_queue_run.py','artifacts/x','--mode','txn','--cpus','4','--users','100000','--requests-per-user','40','--queues','256','--batch-max','250','--sample-interval-ms','250','--pg-conf','wal_buffers=256MB','--pg-conf','max_wal_size=8GB','--pg-conf','track_wal_io_timing=on'])
        self.assertEqual(bs.wrapper_argv('tidb',8,250,'artifacts/x'),['python3','scripts/txn_tidb_run.py','artifacts/x','--cpus','8','--users','100000','--requests-per-user','40','--queues','256','--batch-max','250','--sample-interval-ms','250'])

if __name__=='__main__':unittest.main()
