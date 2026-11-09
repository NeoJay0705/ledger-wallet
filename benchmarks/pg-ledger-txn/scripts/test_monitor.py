import unittest, tempfile, pathlib, json, csv, datetime
from unittest.mock import patch
import monitor
from single_run import filter_resources, docker_resource_assignment
from campaign import load_run
from monitor import cpu_core_equivalents, parse_cpu_stat, parse_metrics, owned_resource, disk_interval_metrics

class MonitorTests(unittest.TestCase):
    @staticmethod
    def sample_row(t, cpu):
        return {'monotonic':t,'wall_utc':datetime.datetime.fromtimestamp(t,datetime.timezone.utc).isoformat(),'db_cpu_usage_usec':cpu,'client_cpu_ticks':int((t+1000)*100),'client_rss_bytes':1000,'memory_current_bytes':2000,'allocated_cpu_capacity_cores':6,'client_cpu_capacity_cores':3,'client_threads':4,'sampler_cpu_ticks':int(t*100),'client_worker_cpu_ticks_json':'{}','client_worker_cpu_json':'{}','client_io_read_bytes':int(t*1048576),'client_io_write_bytes':int(t*524288),'io_rbytes':int(t*2097152),'io_wbytes':int(t*1048576),'io_rios':int(t*2),'io_wios':int(t),'client_worker_cpu_json':'{}','device_read_mib_s':1,'device_write_mib_s':.5,'device_read_iops':2,'device_write_iops':1,'device_read_await_ms':2,'device_write_await_ms':3,'device_read_ops':2,'device_write_ops':1,'device_flush_ops':0,'device_util_pct':10,'device_avg_queue_depth':.2,'device_flush_iops':'','device_flush_await_ms':''}
    def test_sampler_proc_exit_race_only_suppresses_confirmed_exits(self):
        with tempfile.TemporaryDirectory() as d:
            proc_root=pathlib.Path(d)
            sampler=monitor.Sampler(d,'pg','unused',123,[])
            exit_check=monitor.proc_exit_confirmed
            for pid,state in ((123,'Z'),(124,'X'),(125,'R'),(126,'S')):
                task=proc_root/str(pid); task.mkdir()
                (task/'stat').write_text(f'{pid} (client ) name) {state} 0 0 0\\n')
            with patch.object(monitor,'proc_info',side_effect=PermissionError(13,'Permission denied')):
                for pid in (123,124):
                    with patch.object(monitor,'proc_exit_confirmed',side_effect=lambda p: exit_check(p,proc_root)):
                        self.assertIsNone(sampler.read_client_info(pid))
                for pid in (125,126):
                    with patch.object(monitor,'proc_exit_confirmed',side_effect=lambda p: exit_check(p,proc_root)):
                        with self.assertRaises(PermissionError): sampler.read_client_info(pid)
                (proc_root/'127').mkdir()
                with patch.object(monitor,'proc_exit_confirmed',side_effect=lambda p: exit_check(p,proc_root)):
                    self.assertIsNone(sampler.read_client_info(127))
            valid={'cpu_ticks':10,'rss_bytes':20,'threads':1,'io':{'rchar':3},'tasks':[]}
            with patch.object(monitor,'proc_info',return_value=valid):
                self.assertEqual(sampler.read_client_info(128),valid)

    def test_cpu_core_equivalents(self):
        self.assertEqual(cpu_core_equivalents(2_500_000, 2), 1.25)
        with self.assertRaises(ValueError): cpu_core_equivalents(1, 0)
    def test_cgroup_cpu_parser(self):
        self.assertEqual(parse_cpu_stat('usage_usec 112\nnr_periods 3\nnr_throttled 1\nthrottled_usec 9\n'), {'usage_usec':112,'nr_periods':3,'nr_throttled':1,'throttled_usec':9})
    def test_ownership_fails_closed(self):
        self.assertTrue(owned_resource('mine','mine'))
        self.assertFalse(owned_resource('other','mine'))
        self.assertFalse(owned_resource('','mine'))
    def test_resource_filter_excludes_verification_tail(self):
        with tempfile.TemporaryDirectory() as d:
            out=pathlib.Path(d); start=1000.0; end=1002.2
            with (out/'resources.csv').open('w',newline='') as f:
                sample=self.sample_row(1000,10_000_000)
                w=csv.DictWriter(f,fieldnames=list(sample)); w.writeheader()
                for t,c in [(999,0),(1000,10_000_000),(1001,11_000_000),(1002,12_000_000),(1003,99_000_000)]: w.writerow(self.sample_row(t,c))
            result={'measurement_start_unix_ns':int(start*1e9),'measurement_end_unix_ns':int(end*1e9)}
            self.assertTrue(filter_resources(out,result))
            with (out/'resources-measured.csv').open() as f: rows=list(csv.DictReader(f))
            self.assertEqual(len(rows),2)
            self.assertTrue(all(float(r['end_unix_seconds']) <= end for r in rows))
            summary=json.loads((out/'resource-summary.json').read_text())
            self.assertFalse(summary['verification_tail_included'])
            self.assertEqual(summary['scope'],'measured business window only')
            self.assertEqual(summary['allocated_cpu_capacity_cores'],6)
            self.assertAlmostEqual(summary['means']['db_normalized_cpu_pct_of_allocated_capacity'],100/6)
            self.assertEqual(summary['cpu_core_equivalents_mean'],summary['means']['db_cpu_core_equivalents'])
            self.assertEqual(summary['client_cpu_core_equivalents_mean'],summary['means']['client_cpu_core_equivalents'])

    def test_filter_resources_accepts_large_scylla_metrics_field(self):
        with tempfile.TemporaryDirectory() as d:
            out=pathlib.Path(d)
            metrics=json.dumps({'padding':'x'*140_000})
            self.assertGreater(len(metrics),128*1024)
            rows=[self.sample_row(1000,10_000_000),self.sample_row(1001,11_000_000)]
            for row in rows: row['scylla_metrics_json']=metrics
            with (out/'resources.csv').open('w',newline='') as f:
                w=csv.DictWriter(f,fieldnames=list(rows[0])); w.writeheader(); w.writerows(rows)
            result={'measurement_start_unix_ns':int(1000e9),'measurement_end_unix_ns':int(1001e9)}
            self.assertTrue(filter_resources(out,result))
            with (out/'resources-measured.csv').open() as f: measured=list(csv.DictReader(f))
            self.assertEqual(len(measured),1)
            self.assertEqual(float(measured[0]['start_unix_seconds']),1000)

    def test_jitter_intervals_are_retained_and_weighted(self):
        with tempfile.TemporaryDirectory() as d:
            out=pathlib.Path(d); start=1000.0; end=1002.0
            samples=[(999.5,99.5,0),(1000.0,100.0,0),(1000.99,100.99,990_000),(1002.0,102.0,2_000_000),(1002.01,102.01,99_000_000)]
            with (out/'resources.csv').open('w',newline='') as f:
                sample=self.sample_row(1000,0); w=csv.DictWriter(f,fieldnames=list(sample)); w.writeheader()
                for wall,mono,cpu in samples:
                    r=self.sample_row(mono,cpu); r['wall_utc']=datetime.datetime.fromtimestamp(wall,datetime.timezone.utc).isoformat(); w.writerow(r)
            result={'measurement_start_unix_ns':int(start*1e9),'measurement_end_unix_ns':int(end*1e9)}
            self.assertTrue(filter_resources(out,result))
            with (out/'resources-measured.csv').open() as f: rows=list(csv.DictReader(f))
            self.assertEqual(len(rows),2)
            self.assertAlmostEqual(float(rows[0]['elapsed_seconds']),.99)
            summary=json.loads((out/'resource-summary.json').read_text())
            self.assertAlmostEqual(summary['cpu_interval_coverage_seconds'],2.0)
            self.assertAlmostEqual(summary['means']['db_cpu_core_equivalents'],1.0)
            self.assertEqual(summary['allocated_cpu_capacity_cores'],6)
            self.assertEqual(summary['means']['client_read_mib_s'],1.0)
            self.assertFalse(summary['verification_tail_included'])

    def test_resource_filter_excludes_reset_and_missing_data_explicitly(self):
        with tempfile.TemporaryDirectory() as d:
            out=pathlib.Path(d); rows=[self.sample_row(1000,10_000_000),self.sample_row(1001,9_000_000)]
            with (out/'resources.csv').open('w',newline='') as f:
                w=csv.DictWriter(f,fieldnames=list(rows[0])); w.writeheader(); w.writerows(rows)
            result={'measurement_start_unix_ns':int(1000e9),'measurement_end_unix_ns':int(1001e9)}
            self.assertFalse(filter_resources(out,result))
            summary=json.loads((out/'resource-summary.json').read_text())
            self.assertFalse(summary['formal']); self.assertEqual(summary['missing_intervals'][0]['reason'],'counter reset')
            sampler_reset=[self.sample_row(1000,10_000_000),self.sample_row(1001,11_000_000)]
            sampler_reset[1]['sampler_cpu_ticks']=sampler_reset[0]['sampler_cpu_ticks']-1
            with (out/'resources.csv').open('w',newline='') as f:
                w=csv.DictWriter(f,fieldnames=list(sampler_reset[0])); w.writeheader(); w.writerows(sampler_reset)
            self.assertFalse(filter_resources(out,result))
            summary=json.loads((out/'resource-summary.json').read_text())
            self.assertEqual(summary['missing_intervals'][0]['reason'],'counter reset')
            rows[1]['client_io_read_bytes']=''
            with (out/'resources.csv').open('w',newline='') as f:
                w=csv.DictWriter(f,fieldnames=list(rows[0])); w.writeheader(); w.writerows(rows)
            self.assertFalse(filter_resources(out,result))
            summary=json.loads((out/'resource-summary.json').read_text())
            self.assertIn('required cumulative counter/memory sample missing',summary['missing_intervals'][0]['reason'])

    def test_campaign_counts_pg_sockets_on_current_endpoint(self):
        with tempfile.TemporaryDirectory() as d:
            p=pathlib.Path(d)
            (p/'run.json').write_text('{"backend":"pg"}')
            (p/'socket-snapshots.json').write_text('{"expected_data":96,"expected_control":0,"expected_total":96,"stable_samples":3,"observations":[{"counts_by_remote_port":{"15432":96}}]}')
            got=load_run(p)
            self.assertEqual(got['sockets']['observed_data'],96)
            self.assertEqual(got['sockets']['observed_total'],96)

    def test_sampler_errors_real_json_list_and_missing_file_fail_closed(self):
        with tempfile.TemporaryDirectory() as d:
            p=pathlib.Path(d); (p/'sampler-errors.json').write_text('["scrape failed"]\n')
            got=load_run(p)
            self.assertEqual(got['sampler_errors']['errors'], ['scrape failed'])
            (p/'sampler-errors.json').unlink()
            self.assertTrue(load_run(p)['sampler_errors']['errors'])
    def test_disk_zero_operations_have_undefined_await_and_resets_are_missing(self):
        before={'sectors_read':100,'sectors_written':200,'reads_completed':4,'writes_completed':0,'read_ms':20,'write_ms':0,'io_ms':30,'weighted_io_ms':40,'flushes_completed':0,'flush_ms':0}
        after={'sectors_read':100,'sectors_written':200,'reads_completed':4,'writes_completed':0,'read_ms':20,'write_ms':0,'io_ms':30,'weighted_io_ms':40,'flushes_completed':0,'flush_ms':0}
        got=disk_interval_metrics(after,before,1)
        self.assertEqual(got['read_iops'],0); self.assertEqual(got['write_iops'],0)
        self.assertIsNone(got['read_await_ms']); self.assertIsNone(got['write_await_ms'])
        reset=dict(after,sectors_read=2)
        self.assertIsNone(disk_interval_metrics(reset,before,1)['read_mib_s'])
        missing=dict(after,sectors_read=None)
        self.assertIsNone(disk_interval_metrics(missing,before,1)['read_mib_s'])
    @unittest.skipUnless((pathlib.Path(__file__).resolve().parents[1]/'artifacts/duplicate-zero-probe-20261103/inspect-ready.json').exists(), 'fixture from the original campaign artifacts is not part of this repository')
    def test_docker_cpus_inspect_nanocpus_fixture(self):
        fixture=pathlib.Path(__file__).resolve().parents[1]/'artifacts/duplicate-zero-probe-20261103/inspect-ready.json'
        host=json.loads(fixture.read_text())[0]['HostConfig']
        self.assertEqual(host['CpuQuota'],0); self.assertEqual(host['CpuPeriod'],0)
        actual=docker_resource_assignment(host,3,'0-2',8*1024**3)
        self.assertEqual(actual['docker_nano_cpus'],3_000_000_000)
        self.assertEqual(actual['expected_quota_cores'],3)

    def test_disk_helper_sampled_positive_rates_and_missing_await(self):
        before={'sectors_read':0,'sectors_written':0,'reads_completed':0,'writes_completed':0,'read_ms':0,'write_ms':0,'io_ms':0,'weighted_io_ms':0,'flushes_completed':0,'flush_ms':0}
        after=dict(before,sectors_read=2048,sectors_written=1024,reads_completed=4,writes_completed=2,read_ms=8,write_ms=6,io_ms=50,weighted_io_ms=100,flushes_completed=1,flush_ms=4)
        got=disk_interval_metrics(after,before,2)
        self.assertEqual(got['read_iops'],2); self.assertEqual(got['write_iops'],1)
        self.assertEqual(got['read_await_ms'],2); self.assertEqual(got['write_await_ms'],3)
        self.assertEqual(got['util_pct'],2.5)
        missing=dict(after,read_ms=None)
        self.assertIsNone(disk_interval_metrics(missing,before,2)['read_await_ms'])
        with self.assertRaisesRegex(ValueError,'required disk counters missing'):
            disk_interval_metrics({k:v for k,v in after.items() if k!='write_ms'},before,2)

    def test_summary_await_is_operation_weighted_and_ignores_idle(self):
        with tempfile.TemporaryDirectory() as d:
            out=pathlib.Path(d)
            samples=[]
            for i,(ops,await_ms) in enumerate(((0,''),(100,1),(1,100),(0,''))):
                row=self.sample_row(1000+i,10_000_000+i*1_000_000)
                row['wall_utc']=datetime.datetime.fromtimestamp(1000+i,datetime.timezone.utc).isoformat()
                row.update(device_read_ops=ops,device_read_await_ms=await_ms,device_write_ops=0,device_write_await_ms='',device_flush_ops=0)
                samples.append(row)
            with (out/'resources.csv').open('w',newline='') as f:
                w=csv.DictWriter(f,fieldnames=list(samples[0])); w.writeheader(); w.writerows(samples)
            result={'measurement_start_unix_ns':int(1000e9),'measurement_end_unix_ns':int(1003e9)}
            self.assertTrue(filter_resources(out,result))
            summary=json.loads((out/'resource-summary.json').read_text())
            self.assertAlmostEqual(summary['means']['device_read_await_ms'],200/101)
            self.assertIsNone(summary['means']['device_write_await_ms'])
            self.assertIn('cpu_core_equivalents_mean',summary['means'])
            self.assertIn('client_cpu_core_equivalents_mean',summary['means'])
            self.assertEqual(summary['cpu_core_equivalents_mean'],summary['means']['cpu_core_equivalents_mean'])
            self.assertEqual(summary['client_cpu_core_equivalents_mean'],summary['means']['client_cpu_core_equivalents_mean'])

    def test_prometheus_parser(self):
        got=parse_metrics('# HELP x hi\nx{shard="2"} 3.5\ny 9\n')
        self.assertEqual(got, {'x{shard="2"}':3.5,'y':9.0})

if __name__=='__main__': unittest.main()
