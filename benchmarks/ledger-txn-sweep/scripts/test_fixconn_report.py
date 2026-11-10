import csv, json, tempfile, unittest
from pathlib import Path
from unittest.mock import patch
import sys
sys.path.insert(0,str(Path(__file__).resolve().parent))
import fixconn_report as report
import fixconn_sweep as driver

class FixconnReportTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.root=Path(self.tmp.name); self.art=self.root/'artifacts';self.art.mkdir()
        self.patches=[patch.object(report,'ROOT',self.root),patch.object(report,'OUT',self.root/'report.md'),patch.object(driver,'ROOT',self.root),patch.object(driver,'ART',self.art)]
        for p in self.patches:p.start()
        self.fixture()
    def tearDown(self):
        for p in reversed(self.patches):p.stop()
        self.tmp.cleanup()
    def fixture(self):
        for s,(db,c,n,x) in driver.SERIES.items():
            rows=[]
            if s in ('pg-c4-n16','tidb-c4-n16','tidb-c8x-n16'):
                for batch,role in [(50,'primary'),(100,'primary')]:
                    # For test saturation: 50 is the peak, 100 is the 2nd low point.
                    rows.append(self.add_attempt(s,batch,role,10000 if batch==50 else 7000, marked=(s=='tidb-c4-n16' and batch==100)))
                if s=='pg-c4-n16':rows.append(self.add_attempt(s,100,'flush_rerun',8000))
                rows.append(self.add_attempt(s,250,'primary',6000,failed=True))
                rows.append({'batch':'250','role':'terminated','status':'terminated','cause':'failure-limit'})
                if s=='pg-c4-n16': rows.append(self.add_attempt(s,50,'replicate',12000))
            path=driver.series_file(s);path.parent.mkdir(parents=True,exist_ok=True)
            with path.open('w',newline='') as f:
                w=csv.DictWriter(f,fieldnames=driver.COLUMNS);w.writeheader();w.writerows(rows)
    def add_attempt(self,s,b,role,rps,marked=False,failed=False):
        db,c,n,x=driver.SERIES[s];relative=driver.attempt_name(s,b,1)+( '-rep' if role=='replicate' else ('-rerun' if role=='flush_rerun' else ''));d=self.root/relative;d.mkdir(parents=True)
        tidb=db=='tidb';cpu=3.4 if c==4 else 5.1
        resource={'means':{'db_cpu_core_equivalents':2.0,'client_cpu_core_equivalents':.3,'device_write_mib_s':2,'device_write_iops':3,'device_util_pct':4,'device_flush_await_ms':.5,'device_write_await_ms':.6},'tikv_cpu_cores_mean':2.8,'tikv_cpu_pct_of_quota':70,'tidb_cpu_cores_mean':cpu,'client_cpu_cores_mean':.3,'memory_current_bytes_max':{'tidb':1000000000},'device_means':{'device_write_mib_s':2,'device_write_iops':3,'device_util_pct':4,'device_flush_await_ms':.5,'device_write_await_ms':.6},'net':{'tidb':{'rx_mib_s_avg':1,'tx_mib_s_avg':2}},'container_resource_summaries':{'res-pd':{'means':{'client_cpu_core_equivalents':.3,'db_cpu_core_equivalents':.4,'sampler_cpu_core_equivalents':.2}}}}
        (d/'summary.json').write_text(json.dumps({'measurement_start_unix_ns':1704067200000000000,'measurement_end_unix_ns':1704067210000000000,'resource_summary':resource}))
        if db=='pg':
            (d/'net.csv').write_text('wall_utc,rx_bytes,tx_bytes\n2024-01-01T00:00:00Z,0,0\n2024-01-01T00:00:10Z,10485760,20971520\n')
        if tidb:
            (d/'durability.json').write_text(json.dumps({'stores':[{'store':1,'counters':{'raft_engine_write_duration_seconds_count':{'delta':10},'raft_engine_sync_log_duration_seconds_count':{'delta':9}}}]}))
            (d/'client').mkdir();(d/'client/topology.json').write_text(json.dumps({'store_status':[{'store_id':1,'region_count':256,'leader_count':256}]}))
        if failed:(d/'failure.txt').write_text('synthetic window timeout')
        return {'batch':b,'role':role,'attempt_dir':relative,'status':'incomplete' if failed else 'ok','rps_request':rps,'p50_s':.01,'p95_s':.02,'p99_s':.03,'max_s':.04,'windows':driver.expected_windows(b),'db_cpu_cores':2,'tidb_cpu_cores':cpu,'client_cpu_cores':.3,'flush_await_ms':.5,'imported':'false','cause':'reclassification full-window CPU' if marked else ('timeout' if failed else '')}
    def test_complete_synthetic_report(self):
        text=report.build()
        self.assertIn('3.40 / 85.0%*†',text);self.assertIn('5.10 / 85.0%*',text)
        self.assertIn('failure-limit',text);self.assertIn('synthetic window timeout',text)
        self.assertIn('Flush reruns',text);self.assertIn('pg-c4-n16 | 100 | 7.0k / 0.030 / 0.50 ms | 8.0k / 0.030 / 0.50 ms | rerun',text)
        self.assertIn('flush-rerun replaced',text);self.assertIn('Dip: batch 100',text)
        self.assertIn('network rx/tx MiB/s | role markers | replicate RPS |',text)
        self.assertNotIn('completion / saturation |',text);self.assertIn('**sat**',text)
        self.assertNotIn('n/a / n/a% |',text)
        pg=next(r for r in driver.rows('pg-c4-n16') if r.get('role')=='primary')
        _,_,_,_,_,_,z=report.resource(pg,'pg',4)
        self.assertEqual(report.disk(z),[2,3,4,.5,.6]);self.assertEqual(report.network(pg,'pg',z),'1.00 / 2.00')
        self.assertIn(r'FLAG \|diff\|>10%',text) # replicate diff checks below via injected replicate
        self.assertNotIn('nan',text.lower());self.assertNotIn('None',text)
        self.assertIn('TiKV per-thread CPU: not measured',text)
    def test_shared_cpu_single_source_and_smoke(self):
        row=next(r for r in driver.rows('tidb-c8x-n16') if r.get('role')=='primary')
        self.assertEqual(driver.shared_cpu(self.root/row['attempt_dir']),(.3,.4,.2,.45))
        # Read-only real smoke evidence verifies the production shape and reader.
        smoke=Path(__file__).resolve().parents[1]/'artifacts/smoke-fixconn-c8x-a1'
        if (smoke/'summary.json').exists():self.assertIsNotNone(driver.shared_cpu(smoke))
    def test_empty_artifacts_produces_no_data(self):
        with tempfile.TemporaryDirectory() as d:
            oldart=driver.ART;driver.ART=Path(d)
            try:
                text=report.build();self.assertIn('no data',text.lower());self.assertNotIn('nan',text.lower());self.assertNotIn('None',text)
            finally:driver.ART=oldart
    def test_tidb_cpuset_denominators_normal_and_reclassified(self):
        r4=next(r for r in driver.rows('tidb-c4-n16') if str(r['batch'])=='100')
        r8=next(r for r in driver.rows('tidb-c8x-n16') if str(r['batch'])=='50')
        self.assertEqual(report.resource(r4,'tidb',4)[3],85.0)
        self.assertEqual(report.resource(r8,'tidb',8)[3],85.0)
        marked4=dict(r4,tidb_cpu_cores='3.4')
        marked8=dict(r8,attempt_dir='artifacts/tidb-c8x-n16-bm50-a1',tidb_cpu_cores='5.1',cause='full-window reclassification')
        self.assertEqual(report.resource(marked4,'tidb',4)[3],85.0)
        self.assertEqual(report.resource(marked8,'tidb',8)[3],85.0)
        self.assertIn('85.0%*†',report.build())

if __name__=='__main__':unittest.main()
