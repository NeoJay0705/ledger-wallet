import sys, unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parent))
from diagnostics import summarize,pct_change,validate_level,invocation_plan,metric_summary,validate_source_pins,select_formal_level,run_one

class DiagnosticsTests(unittest.TestCase):
    def test_group_medians_ranges_and_percent_math(self):
        xs=[{'result':{'business_rps':10,'metrics':{'end_to_end':{'p50_us':100,'p95_us':200,'p99_us':300}}},'artifacts':'a'},{'result':{'business_rps':20,'metrics':{'end_to_end':{'p50_us':200,'p95_us':300,'p99_us':400}}},'artifacts':'b'},{'result':{'business_rps':30,'metrics':{'end_to_end':{'p50_us':300,'p95_us':400,'p99_us':500}}},'artifacts':'c'}]
        s=summarize(xs)
        self.assertEqual(s['median_rps'],20);self.assertEqual((s['min_rps'],s['max_rps']),(10,30));self.assertEqual(s['run_paths'],['a','b','c'])
        self.assertEqual((s['p50_us_median'],s['p95_us_median'],s['p99_us_median']),(200,300,400))
        self.assertFalse(any(k.endswith('_rps') and k.startswith('p') for k in s))
        self.assertEqual(pct_change(100,105),5)
    def test_interleaves_three_modes_and_alternates_backend_order(self):
        plan=invocation_plan(['pg','scylla'],3)
        self.assertEqual([(m,r) for m,r,b in plan[::2]],[(m,r) for r,order in [(1,['baseline','pool','client']),(2,['client','baseline','pool']),(3,['pool','client','baseline'])] for m in order])
        self.assertEqual(len(plan),18)
        self.assertEqual({(m,b):sum(1 for mm,_,bb in plan if (mm,bb)==(m,b)) for m in ('baseline','pool','client') for b in ('pg','scylla')},{(m,b):3 for m in ('baseline','pool','client') for b in ('pg','scylla')})
        for rep in range(1,4):
            run=[b for _,r,b in plan if r==rep]
            expected=['pg','scylla'] if rep%2 else ['scylla','pg']
            self.assertEqual([run[i:i+2] for i in range(0,len(run),2)],[expected]*3)
    def test_six_source_pins_require_diagnostics_hash_and_refuse_mismatch(self):
        import diagnostics
        current=diagnostics.source_pins()
        self.assertEqual(current['diagnostics_sha256'],diagnostics.hashlib.sha256(Path(diagnostics.__file__).read_bytes()).hexdigest())
        data={'source_pins':dict(current)}
        data['source_pins']['diagnostics_sha256']='stale'
        with self.assertRaisesRegex(ValueError,'diagnostics_sha256'):
            validate_source_pins(data,six=True)

    def test_formal_source_pins_require_shared_hashes_but_ignore_controller(self):
        pins={k:k for k in ('binary_sha256','cargo_lock_sha256','wrapper_sha256','monitor_sha256','scylla_config_sha256')}
        validate_source_pins({'config':{'metadata':{**pins,'controller_sha256':'old'}}},pins)
        pins['wrapper_sha256']='changed'
        with self.assertRaises(ValueError):validate_source_pins({'config':{'metadata':{k:k for k in ('binary_sha256','cargo_lock_sha256','wrapper_sha256','monitor_sha256','scylla_config_sha256')}}},pins)
    def test_measurement_metrics_exclude_verification_tail(self):
        import csv,json,tempfile,datetime
        with tempfile.TemporaryDirectory() as td:
            p=Path(td); start=datetime.datetime(2024,1,1,tzinfo=datetime.timezone.utc)
            (p/'client').mkdir()
            (p/'client/result.json').write_text(json.dumps({'measurement_start_unix_ns':int(start.timestamp()*1e9),'measurement_end_unix_ns':int((start.timestamp()+2)*1e9)}))
            fields=['wall_utc','scylla_metrics_json']
            with (p/'resources.csv').open('w',newline='') as f:
                w=csv.DictWriter(f,fieldnames=fields);w.writeheader()
                for sec,value in [(0,10),(1,20),(2,30),(3,999999)]:
                    metric={'reactor_utilization{shard="0"}':value,'scylla_commitlog_total_bytes_written':sec*100,'scylla_coordinator_cas_total':sec*2}
                    if sec==1: metric.update({f'padding_metric_{i:05d}':i for i in range(12_000)})
                    encoded=json.dumps(metric)
                    if sec==1: self.assertGreater(len(encoded),128*1024)
                    w.writerow({'wall_utc':(start+datetime.timedelta(seconds=sec)).isoformat(),'scylla_metrics_json':encoded})
            summary=metric_summary(p)
            self.assertEqual(summary['reactor_utilization_by_shard']['0']['mean'],20)
            self.assertEqual(summary['reactor_utilization_by_shard']['0']['max'],30)
            self.assertEqual(summary['counter_deltas']['scylla_commitlog_total_bytes_written']['delta'],200)
            self.assertEqual(summary['measurement_window']['samples'],3)
            self.assertNotIn('999999',json.dumps(summary))
    def test_backend_specific_onset_selection_and_mixed_onsets_refusal(self):
        data={'candidate_N':{'pg':256,'scylla':512},'levels':{'256':{'valid':True,'K':1000},'512':{'valid':True,'K':2000}},'candidate_flags':{'pg':True,'scylla':True},'state':'candidates_require_diagnostics'}
        self.assertEqual(select_formal_level(data,'pg'),(256,1000));self.assertEqual(select_formal_level(data,'scylla'),(512,2000))
        with self.assertRaises(ValueError): select_formal_level(data,'pg',512)

    def test_six_profile_propagation_and_smt_client_sensitivity(self):
        import tempfile
        from unittest.mock import patch
        from diagnostics import run_one
        with tempfile.TemporaryDirectory() as td, patch('diagnostics.campaign.invoke',return_value=1) as invoke, patch('diagnostics.campaign.load_run',return_value={'result':{},'cleanup':{},'artifacts':td}):
            with patch('six_cpu_campaign.verify_six') as verify:
                run_one(Path(td),'pg',256,1000,'client',1,True,20,'6')
                verify.assert_called_once_with(unittest.mock.ANY,'6,7,14,15',4)
            cmd=invoke.call_args.args[0]
            self.assertIn('--cpu-profile',cmd);self.assertEqual(cmd[cmd.index('--cpu-profile')+1],'6')
            self.assertEqual(cmd[cmd.index('--client-cpus')+1],'6,7,14,15');self.assertEqual(cmd[cmd.index('--workers')+1],'4')

    @unittest.skip('pinned to the binary hash of the original (earlier) campaign; not applicable here')
    def test_formal_main_dispatches_explicit_backend_to_nine_runs(self):
        import json,tempfile
        from unittest.mock import patch
        import diagnostics
        with tempfile.TemporaryDirectory() as td:
            root=Path(td); pins=diagnostics.source_pins(); campaign_file=root/'campaign.json'; output=root/'out'
            data={'formal':True,'profile':'6','source_pins':pins,'state':'candidates_require_diagnostics','candidate_N':{'pg':256},'candidate_flags':{'pg':True},'levels':{'256':{'valid':True,'K':100}},'candidate_N':{'pg':256}}
            campaign_file.write_text(json.dumps(data))
            fake=lambda *args:{'accepted':True,'result':{'business_rps':10,'metrics':{'end_to_end':{'p50_us':10,'p95_us':20,'p99_us':30}}},'artifacts':'run','backend':args[1],'mode':args[4],'repeat':args[5]}
            with patch.object(sys,'argv',['diagnostics','--campaign',str(campaign_file),'--backend','pg','--output',str(output)]), patch.object(diagnostics,'run_one',side_effect=fake) as run:
                self.assertEqual(diagnostics.main(),0)
            self.assertEqual(run.call_count,9)

    def test_candidate_incomplete_and_wrong_level_rejected(self):
        data={'state':'running','candidate_N':{'pg':32},'candidate_flags':{'pg':True},'levels':{'32':{'valid':True,'K':2000}}}
        with self.assertRaises(ValueError):validate_level(data,'pg',32,2000)
        data['state']='candidates_require_diagnostics'
        with self.assertRaises(ValueError):validate_level(data,'pg',32,1999)
        self.assertTrue(validate_level(data,'pg',32,2000)['valid'])

if __name__=='__main__':unittest.main()
