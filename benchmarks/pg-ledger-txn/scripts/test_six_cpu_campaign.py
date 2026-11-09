import unittest, math

LAT={'end_to_end':{'count':3,'p50_us':11,'p95_us':22,'p99_us':33}}
import six_cpu_campaign as s

class SixCpuSchedulingTests(unittest.TestCase):
    def test_initial_calibration_uses_validated_rates_not_500(self):
        self.assertEqual(s.initial_calibration_k('pg',256),math.ceil(20000*12*1.15/256))
        self.assertEqual(s.initial_calibration_k('scylla',512),math.ceil(17000*12*1.15/512))

    def test_controller_always_passes_profile_six(self):
        from unittest.mock import patch
        with patch('six_cpu_campaign.campaign.invoke',return_value=1) as invoke, patch('six_cpu_campaign.campaign.load_run',return_value={'result':{},'cleanup':{},'artifacts':'x'}):
            s.invoke_run(__import__('pathlib').Path('/tmp/out'),'pg',256,1000,1,20,False)
            cmd=invoke.call_args.args[0]
            self.assertIn('--cpu-profile',cmd);self.assertEqual(cmd[cmd.index('--cpu-profile')+1],'6')

    def test_independent_starts_and_other_backend_continues(self):
        self.assertEqual(s.START,{'pg':256,'scylla':512})
        levels={'256':{'valid':True,'medians_rps':{'pg':100}},'512':{'valid':True,'medians_rps':{'pg':102,'scylla':80}},'1024':{'valid':True,'medians_rps':{'pg':103,'scylla':90}},'2048':{'valid':True,'medians_rps':{'scylla':95}}}
        self.assertEqual(s.onset(levels,'pg')['onset_N'],256)
        self.assertIsNone(s.onset(levels,'scylla'))
        levels['1024']['medians_rps']['scylla']=81
        levels['2048']['medians_rps']['scylla']=82
        self.assertEqual(s.onset(levels,'scylla')['onset_N'],512)

    def test_negative_and_flat_gains_are_onset_candidates(self):
        levels={'256':{'valid':True,'medians_rps':{'pg':100}},'512':{'valid':True,'medians_rps':{'pg':94}},'1024':{'valid':True,'medians_rps':{'pg':94}}}
        x=s.onset(levels,'pg')
        self.assertEqual((x['onset_N'],x['confirmation_N']),(256,1024))
        self.assertEqual(x['doubling_gains_pct'],[-6.0,0.0]);self.assertTrue(x['overload'])

    def test_onset_is_first_point_and_confirmation_peak_are_distinct(self):
        x=s.onset({'256':{'valid':True,'medians_rps':{'pg':100}},'512':{'valid':True,'medians_rps':{'pg':102}},'1024':{'valid':True,'medians_rps':{'pg':103}}},'pg')
        self.assertEqual((x['onset_N'],x['confirmation_N'],x['peak_N']),(256,1024,1024))

    def test_matched_common_max_missing_backend_and_complete_reuse(self):
        pins={'x':'x'}
        base={'source_pins':pins,'formal':True,'candidates':{'pg':{'onset_N':256},'scylla':{'onset_N':512}},'levels':{'512':{'valid':True,'K':1000,'profile':'6','repetitions':[]}}}
        plan=s.matched_plan(base,pins)
        self.assertEqual((plan['N'],plan['run_backends']),(512,['pg','scylla']))
        reps=[{'backend':b,'accepted':True,'K':1000,'profile':'6','artifacts':b+str(i)} for b in ('pg','scylla') for i in range(3)]
        base['levels']['512']['repetitions']=reps
        from unittest.mock import patch
        with patch.object(s,'accept',return_value=(True,[])):
            plan=s.matched_plan(base,pins)
        self.assertEqual(plan['run_backends'],[])
        self.assertEqual(set(plan['reuse']),{'pg','scylla'})

    def test_mismatched_k_never_mixes_and_pin_mismatch_refuses(self):
        pins={'x':'x'}; reps=[{'backend':b,'accepted':True,'K':999,'profile':'6','artifacts':str(i)} for b in ('pg','scylla') for i in range(3)]
        data={'source_pins':pins,'formal':True,'candidates':{'pg':{'onset_N':256},'scylla':{'onset_N':256}},'levels':{'256':{'valid':True,'K':1000,'profile':'6','repetitions':reps}}}
        self.assertEqual(s.matched_plan(data,pins)['run_backends'],['pg','scylla'])
        with self.assertRaises(ValueError):s.matched_plan(data,{'x':'changed'})

    def test_verify_six_fails_closed_on_actual_shard_and_capacity_evidence(self):
        import json,tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as td:
            p=Path(td)
            (p/'run.json').write_text(json.dumps({'cpu_profile':'6','backend':'scylla','db_cpu_quota':6,'db_cpus':'0-5','sampler_cpus':'14-15','client_cpus':'6-7','workers':2,'expected_shards':6}))
            (p/'resource-placement.json').write_text(json.dumps({'cpu_profile':'6','expected_shards':6,'docker_nano_cpus':6000000000,'docker_cpuset_cpus':'0-5','docker_memory_bytes':8*1024**3,'sampler_actual_affinity_cpus':[14,15],'client_actual_affinity_cpus':[6,7]}))
            (p/'resource-summary.json').write_text(json.dumps({'allocated_cpu_capacity_cores':6}))
            (p/'scylla-ready-connections.json').write_text(json.dumps({'expected_shards':6,'observed_shards':6}))
            (p/'client-ready.json').write_text(json.dumps({'diagnostics':{'driver_shards':'6'}}))
            record={'artifacts':str(p),'resource':{'allocated_cpu_capacity_cores':6}}
            self.assertEqual(s.verify_six(record)['cpu_profile'],'6')
            (p/'client-ready.json').write_text(json.dumps({'diagnostics':{'driver_shards':'5'}}))
            with self.assertRaises(ValueError):s.verify_six(record)
            (p/'client-ready.json').write_text(json.dumps({'diagnostics':{'driver_shards':'6'}}))
            (p/'resource-summary.json').write_text(json.dumps({'allocated_cpu_capacity_cores':5}))
            with self.assertRaises(ValueError):s.verify_six(record)

    def test_latency_percentiles_are_medians_of_latency_metrics_not_rps(self):
        rows=[{'result':{'business_rps':r,'metrics':{'end_to_end':{'p50_us':a,'p95_us':b,'p99_us':c}}}} for r,a,b,c in ((1,10,20,30),(100,40,50,60),(10000,70,80,90))]
        summary=s.rps_summary(rows)
        self.assertEqual((summary['median_rps'],summary['min_rps'],summary['max_rps']),(100,1,10000))
        self.assertEqual((summary['p50_us_median'],summary['p95_us_median'],summary['p99_us_median']),(40,50,60))
        self.assertFalse(any(k.startswith('p') and k.endswith('_rps') for k in summary))

    def test_rps_summary_reads_db_cpu_from_real_resource_schema(self):
        rows=[{'result':{'business_rps':100,'metrics':{'end_to_end':{'p50_us':11,'p95_us':22,'p99_us':33}}},'resource':{'cpu_core_equivalents_mean':4.5,'client_cpu_core_equivalents_mean':0.8,'client_rss_bytes_max':1234,'db_memory_current_bytes_max':5678,'means':{'db_cpu_core_equivalents':4.5}}}]
        summary=s.rps_summary(rows)
        self.assertEqual(summary['db_cpu_core_equivalents_mean'],4.5)
        self.assertNotEqual(summary['db_cpu_core_equivalents_mean'],0)
        self.assertEqual(summary['client_cpu_core_equivalents_mean'],0.8)
        self.assertEqual(summary['client_rss_bytes_max'],1234)

    def test_short_run_retry_restarts_pass_and_structural_failure_stops_immediately(self):
        from unittest.mock import patch
        calls=[]
        def fake(b,n,k,rep):
            calls.append((b,rep,k)); return {'artifacts':'/missing','result':{'elapsed_ms':10000}}
        with patch.object(s,'accept',side_effect=lambda r,n,k,m,sm:(k>100,['too_short'] if k==100 else [])):
            attempts,final_rows,ok,k=s.run_reps(None,256,100,['pg'],fake,30,False)
        self.assertTrue(ok);self.assertGreater(k,100);self.assertEqual(len(calls),4)
        self.assertEqual(len(attempts),4);self.assertEqual(len(final_rows),3)
        self.assertEqual(calls[0][2],100);self.assertEqual([x[2] for x in calls[1:]],[k]*3)
        calls.clear()
        with patch.object(s,'accept',return_value=(False,['too_short','resource_window'])):
            result=s.run_reps(None,256,100,['pg'],fake,30,False)
        self.assertEqual(len(result),4);self.assertEqual(result[2:],(False,100));self.assertEqual(len(calls),1)

    def test_second_short_pass_returns_executed_k(self):
        from unittest.mock import patch
        calls=[]
        def fake(b,n,k,rep):
            calls.append(k);return {'result':{'elapsed_ms':10000},'artifacts':'x'}
        with patch.object(s,'accept',return_value=(False,['too_short'])):
            attempts,final,ok,k=s.run_reps(None,256,100,['pg'],fake,30,False)
        self.assertFalse(ok);self.assertEqual(k,calls[-1]);self.assertEqual(len(calls),2)
        self.assertEqual(len(final),1);self.assertEqual(len(attempts),2)

    def test_main_matched_complete_reuse_is_zero_calls_and_pin_mismatch_refuses(self):
        import json, tempfile
        from pathlib import Path
        from unittest.mock import patch
        pins=s.source_pins()
        with tempfile.TemporaryDirectory() as td:
            root=Path(td); records=[{'backend':b,'accepted':True,'K':1000,'profile':'6','N':256,'artifacts':f'{b}-{i}','result':{'business_rps':100,'metrics':LAT}} for b in ('pg','scylla') for i in range(3)]
            valid={'source_pins':pins,'formal':True,'candidates':{'pg':{'onset_N':256},'scylla':{'onset_N':256}},'levels':{'256':{'valid':True,'K':1000,'profile':'6','repetitions':records}}}
            camp=root/'campaign.json';camp.write_text(json.dumps(valid));out=root/'out'
            with patch.object(s,'accept',return_value=(True,[])),patch.object(s,'invoke_run',side_effect=AssertionError('complete reuse must not invoke DB')),patch('sys.argv',['six_cpu_campaign.py','matched','--campaign',str(camp),'--output',str(out)]):
                self.assertEqual(s.main(),0)
            result=json.loads((out/'matched.json').read_text())
            self.assertTrue(result['valid']);self.assertEqual(result['runs'],[]);self.assertEqual(result['reused'].keys(),{'pg','scylla'})
            bad=dict(valid);bad['source_pins']={'wrong':'pin'};camp2=root/'bad.json';camp2.write_text(json.dumps(bad))
            with patch('sys.argv',['six_cpu_campaign.py','matched','--campaign',str(camp2),'--output',str(root/'bad-out')]):
                with self.assertRaises(SystemExit):s.main()

    def test_matched_reuse_does_not_hide_structural_retry_failure(self):
        import json,tempfile
        from pathlib import Path
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as td:
            root=Path(td); camp=root/'campaign.json'; out=root/'out'
            camp.write_text(json.dumps({'formal':True,'source_pins':s.source_pins()}))
            reused=[{'backend':'scylla','accepted':True,'K':100,'profile':'6','N':256,'artifacts':f'reuse-{i}','result':{'business_rps':100,'metrics':LAT}} for i in range(3)]
            plan={'N':256,'K':100,'reuse':{'scylla':reused},'run_backends':['pg']}
            calls=[]
            def invoke(directory,b,n,k,rep,warmup,smoke):
                if rep==0:return {'artifacts':'calibration','result':{'business_rps':100,'elapsed_ms':35000,'metrics':LAT}}
                calls.append((b,k,rep)); idx=len(calls)
                return {'artifacts':f'new-{idx}','result':{'business_rps':100,'elapsed_ms':10000,'metrics':LAT}}
            def accepted(r,n,k,*args):
                return (False,['too_short']) if r['artifacts']=='new-1' else (False,['client_counts'])
            with patch.object(s,'matched_plan',return_value=plan),patch.object(s,'initial_calibration_k',return_value=100),patch.object(s,'invoke_run',side_effect=invoke),patch.object(s,'accept',side_effect=accepted),patch.object(s,'accept_calibration',return_value=(True,[])),patch.object(s.campaign,'next_k',return_value=100),patch('sys.argv',['six_cpu_campaign.py','matched','--campaign',str(camp),'--output',str(out)]):
                rc=s.main()
            result=json.loads((out/'matched.json').read_text())
            self.assertNotEqual(rc,0);self.assertFalse(result['valid'])
            self.assertEqual(len(calls),2)
            self.assertEqual(result['reused'].keys(),{'scylla'})
            self.assertEqual(result['state'],'stopped_invalid')

    def test_main_matched_discards_reuse_when_common_k_changes(self):
        import json, tempfile
        from pathlib import Path
        from unittest.mock import patch
        pins=s.source_pins()
        with tempfile.TemporaryDirectory() as td:
            root=Path(td); out=root/'out'; camp=root/'campaign.json'
            reused=[{'backend':'scylla','accepted':True,'K':1000,'profile':'6','N':512,'artifacts':f'reuse-{i}','result':{'business_rps':100,'metrics':LAT}} for i in range(3)]
            camp.write_text(json.dumps({'source_pins':pins,'formal':True,'candidates':{'pg':{'onset_N':256},'scylla':{'onset_N':512}},'levels':{'512':{'valid':True,'K':1000,'profile':'6','repetitions':reused}}}))
            calls=[]
            def invoke(directory,b,n,k,rep,warmup,smoke):
                calls.append((b,n,k,rep))
                return {'artifacts':f'new-{b}-{k}-{rep}-{len(calls)}','result':{'business_rps':100,'elapsed_ms':35000,'metrics':LAT}}
            accepted_count=[0]
            def accepted(r,n,k,*args):
                if r.get('artifacts','').startswith('new-'):
                    accepted_count[0]+=1
                    if accepted_count[0]==1:return False,['too_short']
                return True,[]
            with patch.object(s,'invoke_run',side_effect=invoke),patch.object(s,'accept',side_effect=accepted),patch.object(s,'accept_calibration',return_value=(True,[])),patch('sys.argv',['six_cpu_campaign.py','matched','--campaign',str(camp),'--output',str(out)]):
                rc=s.main()
            result=json.loads((out/'matched.json').read_text())
            self.assertEqual(rc,0);self.assertTrue(result['valid']);self.assertEqual(result['reused'],{})
            self.assertEqual({b:sum(x['backend']==b and x['accepted'] and x['K']==result['K'] for x in result['runs']) for b in ('pg','scylla')},{'pg':3,'scylla':3})
            self.assertTrue(all(x['N']==512 and x['K']==result['K'] for x in result['runs']))
            self.assertTrue(any(x['K']!=result['K'] for x in result['attempts']))
            self.assertTrue(all(b in ('pg','scylla') for b,_,_,_ in calls))

    def test_main_explore_independent_onset_controller_and_saturation(self):
        import json, tempfile
        from pathlib import Path
        from unittest.mock import patch
        for saturation in (False,True):
            with tempfile.TemporaryDirectory() as td:
                out=Path(td)/'out'; calls=[]
                def fake_run(directory,b,n,k,rep,warmup,smoke):
                    calls.append((b,n,rep))
                    artifact=Path(td)/'artifact';artifact.mkdir(exist_ok=True)
                    (artifact/'resource-summary.json').write_text('{}')
                    return {'artifacts':str(artifact),'result':{'business_rps':100,'elapsed_ms':35000,'metrics':LAT}}
                def accepted(r,n,k,*args): return True,[]
                with patch.object(s,'invoke_run',side_effect=fake_run),patch.object(s,'accept',side_effect=accepted),patch.object(s,'accept_calibration',return_value=(True,[])),patch.object(s.campaign,'guard',return_value=[]),patch.object(s,'initial_calibration_k',return_value=10),patch.object(s,'median_rate',side_effect=lambda rows,b: 100 if b=='pg' else (100 if max(x['N'] for x in rows if x['backend']==b)<=512 else 90)),patch('sys.argv',['six_cpu_campaign.py','explore','--output',str(out),'--max-users','512' if saturation else '4096']):
                    rc=s.main()
                journal=json.loads((out/'journal.json').read_text())
                if saturation:
                    campaign_json=json.loads((out/'campaign.json').read_text())
                    self.assertEqual(rc,2);self.assertEqual(journal['state'],'stopped_saturation_unconfirmed')
                    self.assertNotEqual(journal['state'],'candidates_require_diagnostics')
                    self.assertFalse(any(campaign_json['candidate_flags'].values()))
                else:
                    self.assertEqual(rc,0)
                    calls_by_backend={b:sorted(set(n for bb,n,r in calls if bb==b and r>0)) for b in ('pg','scylla')}
                    self.assertEqual(calls_by_backend,{'pg':[256,512,1024],'scylla':[512,1024,2048]})
                    self.assertEqual(journal['candidates']['pg']['confirmation_N'],1024)
                    self.assertEqual(journal['candidates']['scylla']['confirmation_N'],2048)

if __name__=='__main__':unittest.main()
