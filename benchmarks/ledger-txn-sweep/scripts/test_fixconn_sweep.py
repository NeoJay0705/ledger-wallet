import csv,hashlib,json,pathlib,sys,tempfile,unittest
from unittest import mock
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parent))
import fixconn_sweep as f
_REAL_ROOT=pathlib.Path(__file__).resolve().parents[1]
_REAL_ART=_REAL_ROOT/'artifacts'

def _csv_checksums():
 return {p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in _REAL_ART.glob('fixconn-*.csv')}

def setUpModule():
 global _before_csv
 _before_csv=_csv_checksums()


def tearDownModule():
 assert _csv_checksums()==_before_csv, 'real fixconn CSVs changed during tests'

_original_series_file=f.series_file
def _guarded_series_file(s):
 p=_original_series_file(s).resolve()
 try:p.relative_to(_REAL_ART.resolve())
 except ValueError:return p
 raise AssertionError(f'test attempted to access real campaign CSV: {p}')
f.series_file=_guarded_series_file

class IsolatedFileTests(unittest.TestCase):
 def setUp(self):
  self.tmp=pathlib.Path(tempfile.mkdtemp());self.old=(f.ART,f.ROOT,f.cs.ART,f.cs.ROOT,f.bs.ART,f.bs.ROOT)
  f.ART=f.cs.ART=f.bs.ART=self.tmp/'artifacts'
  f.ROOT=f.cs.ROOT=f.bs.ROOT=self.tmp
  f.ART.mkdir()
 def tearDown(self):
  f.ART,f.ROOT,f.cs.ART,f.cs.ROOT,f.bs.ART,f.bs.ROOT=self.old
 def test_real_csv_path_guard_rejects_write_target(self):
  original=f.ART
  f.ART=_REAL_ART
  try:
   with self.assertRaisesRegex(AssertionError,'real campaign CSV'):f.append('tidb-c4-n16',{'batch':100,'role':'primary','rps_request':100})
  finally:f.ART=original

class FixconnTests(IsolatedFileTests):
 def test_argv_all_series_and_names(self):
  for sid,(db,c,n,x) in f.SERIES.items():
   a=f.wrapper_argv(sid,250,'D');self.assertIn('--connections',a);self.assertIn(str(n),a);self.assertIn('--batch-max',a);self.assertIn('250',a);self.assertEqual('--tidb-extended' in a,x)
   if db=='pg':self.assertEqual(a.count('--pg-conf'),3);self.assertIn('--mode',a)
   self.assertEqual(f.attempt_name(sid,250,2).endswith('-a2'),True)
 def test_index_next_and_paths(self):
  s='pg-c4-n16';(f.ART/pathlib.Path(f.attempt_name(s,50,3)).name).touch()
  self.assertEqual(f._index(s,50,[]),4)
 def test_effective_flush_note_replicate_excluded(self):
  rs=[{'batch':'50','role':'primary','status':'ok','rps_request':'10','cause':'† note'}, {'batch':'50','role':'flush_rerun','status':'incomplete'}, {'batch':'50','role':'replicate','status':'ok','rps_request':'999'}]
  e=f.effective_primary(rs)[50];self.assertEqual(e['rps_request'],'10');self.assertIn('† note',e['cause']);self.assertIn('flush>2ms',e['cause'])
  rs.append({'batch':'50','role':'flush_rerun','status':'ok','rps_request':'12'});self.assertEqual(f.effective_primary(rs)[50]['rps_request'],'12')
 def test_stop_boundary_reset_record_and_import_seed(self):
  def rows(vals):return [{'batch':str(b),'role':'primary','status':'ok','rps_request':str(v),'imported':'true' if b<250 else ''} for b,v in vals]
  self.assertIsNone(f.completion(rows([(50,100),(100,105)])))
  self.assertEqual(f.completion(rows([(50,100),(100,90),(250,95),(500,90),(1000,90)])),'stop-rule')
  self.assertIsNone(f.completion(rows([(50,100),(100,90),(250,110),(500,90)])))
  self.assertEqual(f.completion(rows([(50,100),(100,110),(250,100),(500,100)])),'stop-rule')
 def test_flush_finalized_before_comparison(self):
  rs=[{'batch':str(b),'role':'primary','status':'ok','rps_request':str(v)} for b,v in [(50,100),(100,80)]]
  rs.append({'batch':'100','role':'flush_rerun','status':'ok','rps_request':'110'})
  self.assertIsNone(f.completion(rs))
 def test_stop_cap_and_failure_helpers(self):
  rs=[{'batch':str(b),'role':'primary','status':'ok','rps_request':str(b)} for b in f.GRID]
  self.assertEqual(f.completion(rs),'cap');self.assertEqual(f.saturation(rs,'cap')[1],'observed-range')
  x=[{'batch':'50','role':'primary','status':'ok','rps_request':'1'}, {'batch':'100','role':'primary','status':'incomplete','cause':'timeout','cleanup':'success'}]
  self.assertEqual(f.completion(x),'failure-limit')
 def test_saturation_and_replicate_sets(self):
  for target,want in [(50,[50]),(4000,[4000,2000]),(8000,[8000,4000])]:
   vals={b:1 for b in f.GRID};vals[target]=10
   rs=[{'batch':str(b),'role':'primary','status':'ok','rps_request':str(v)} for b,v in vals.items()]
   self.assertEqual(f.saturation(rs,'stop-rule')[0],target);self.assertEqual(f.replicate_set(rs),want)
  self.assertEqual(f.saturation([{'batch':'50','role':'primary','status':'ok','rps_request':'1'}],'cap')[1],'observed-range')
 def test_import_exact_six_idempotent_and_refuses_missing_idle(self):
  import conn_sweep as cs
  groups={('pg',4,50):'correct',('pg',4,100):'correct',('pg',8,50):'correct',('pg',8,100):'correct',('tidb',4,50):'correct',('tidb',4,100):'† corrected'}
  def source(db,c,b):
   d=f'original-{db}-{c}-{b}';(f.ROOT/(d+'-idle.json')).write_text('{}')
   return [{'conns':'16','role':'primary','status':'ok','attempt_dir':d,'rps_request':str(b),'cause':'† historical' if 'tidb'==db and b==100 else '', 'tidb_cpu_cores':'3.2387625' if db=='tidb' and b==100 else ''}]
  with mock.patch.object(cs,'rows_for',side_effect=lambda db,c,b:source(db,c,b)):
   for s in ('pg-c4-n16','pg-c8-n16','tidb-c4-n16'):
    f.ensure_imports(s);f.ensure_imports(s);self.assertEqual(len(f.rows(s)),2)
   self.assertEqual(sum(len(f.rows(s)) for s in ('pg-c4-n16','pg-c8-n16','tidb-c4-n16')),6)
   t=f.rows('tidb-c4-n16')[1];self.assertEqual(t['tidb_cpu_cores'],'3.2387625');self.assertIn('†',t['cause'])
  with mock.patch.object(cs,'rows_for',return_value=[{'conns':'16','role':'primary','status':'ok','attempt_dir':'missing'}]),self.assertRaises(RuntimeError):f.imported_rows('pg-c4-n16')
 def test_import_rows_match_conn_sweep_using_temp_paths(self):
  import conn_sweep as cs
  def source(db,c,b):
   rel=f'artifacts/connsweep-{db}-c{c}-bm{b}-n16-a1';(f.ROOT/(rel+'-idle.json')).write_text('{}')
   return [{'conns':'16','role':'primary','status':'ok','attempt_dir':rel,'rps_request':str(b),'tidb_cpu_cores':'3.2388' if db=='tidb' and b==100 else ''}]
  with mock.patch.object(cs,'rows_for',side_effect=lambda db,c,b:source(db,c,b)):
   for sid,db,c in [('pg-c4-n16','pg',4),('pg-c8-n16','pg',8),('tidb-c4-n16','tidb',4)]:
    got=f.imported_rows(sid)
    self.assertEqual(len(got),2)
    for row in got:self.assertEqual(row['rps_request'],str(row['batch']))
   row=next(r for r in f.imported_rows('tidb-c4-n16') if int(r['batch'])==100)
   self.assertEqual(row['tidb_cpu_cores'],'3.2388')
 def test_attempt_gate_and_fake_runner_retry_cleanup(self):
  s='pg-c8-n16';calls=[]
  def fake(argv,cwd):
   calls.append(argv);d=f.ROOT/argv[2];d.mkdir(parents=True);(d/'cleanup.json').write_text('{"status":"success"}');(d/'failure.txt').write_text('sampler race');return type('P',(),{'returncode':1})()
  with mock.patch.object(f.bs,'idle_gate',return_value={'status':'ok'}),mock.patch.object(f,'ensure_imports'),mock.patch.object(f,'imported_rows',return_value=[]):
   with self.assertRaisesRegex(RuntimeError,'sampler retries exhausted'):f.run_series(s,runner=fake)
  self.assertEqual(len(calls),3)
 def test_idle_gate_failure_stops_and_records(self):
  with mock.patch.object(f.bs,'idle_gate',return_value={'status':'gate_failed'}),self.assertRaisesRegex(RuntimeError,'idle gate'):
   f.attempt('pg-c4-n16',50,'primary',[])
  self.assertEqual(f.rows('pg-c4-n16')[0]['cause'],'idle gate')
 def test_cleanup_failure_stops(self):
  def fake(argv,cwd):
   d=f.ROOT/argv[2];d.mkdir(parents=True);(d/'cleanup.json').write_text('{"status":"failed"}');return type('P',(),{'returncode':1})()
  with mock.patch.object(f.bs,'idle_gate',return_value={'status':'ok'}),self.assertRaisesRegex(RuntimeError,'cleanup failure'):f.attempt('pg-c4-n16',50,'primary',[],fake)
 def test_resume_terminated_no_launch_and_dry_writes_nothing(self):
  s='tidb-c8x-n16';f.append(s,{'batch':100,'role':'terminated','status':'terminated','cause':'stop-rule'})
  before=sorted(p.name for p in f.ART.iterdir());f.run_series(s,True);self.assertEqual(sorted(p.name for p in f.ART.iterdir()),before)
  with mock.patch.object(f,'attempt',side_effect=AssertionError('launch')):f.run_series(s,runner=lambda *a,**k:None)
 def test_dry_run_output_and_no_writes(self):
  before=sorted(f.ART.iterdir())
  with mock.patch('builtins.print') as p:f.run_series('tidb-c8x-n16',True)
  self.assertEqual(sorted(f.ART.iterdir()),before);self.assertTrue(any('note: later points depend' in c.args[0] for c in p.call_args_list))
 def test_audit_flags_windows_missing_replicate_and_after_termination(self):
  s='pg-c4-n16'
  for b in f.GRID[:3]:f.append(s,{'batch':b,'role':'primary','status':'ok','rps_request':100,'windows':1})
  f.append(s,{'batch':250,'role':'terminated','status':'terminated','cause':'stop-rule'})
  f.append(s,{'batch':8000,'role':'primary','status':'incomplete','attempt_dir':'after'})
  with mock.patch('builtins.print') as p:code=f.audit()
  text=' '.join(str(c.args[0]) for c in p.call_args_list);self.assertEqual(code,1);self.assertIn('windows mismatch',text);self.assertIn('attempt after termination',text);self.assertIn('missing required replicate',text)
 def test_attempt_calls_runner_without_timeout(self):
  calls=[]
  def runner(argv,cwd):
   calls.append((argv,cwd));d=f.ROOT/argv[2];d.mkdir(parents=True);(d/'cleanup.json').write_text('{"status":"success"}');(d/'failure.txt').write_text('database failure')
   return type('P',(),{'returncode':1})()
  with mock.patch.object(f.bs,'idle_gate',return_value={'status':'ok'}):f.attempt('pg-c4-n16',50,'primary',[],runner)
  self.assertEqual(len(calls),1);self.assertEqual(calls[0][1],f.ROOT)
 def test_completion_ignores_non_curve_failures_and_waits_for_flush(self):
  for row in (
   {'status':'gate_failed','cause':'idle gate','cleanup':'success'},
   {'status':'incomplete','cause':'other','cleanup':'failed'},
   {'status':'incomplete','cause':'sampler','cleanup':'success'}):
   rs=[{'batch':'50','role':'primary','status':'ok','rps_request':'1'},dict(batch='100',role='primary',**row)]
   self.assertIsNone(f.completion(rs))
  rs=[{'batch':'50','role':'primary','status':'ok','rps_request':'1','flush_await_ms':'3'}]
  self.assertIsNone(f.completion(rs))
 def test_audit_import_missing_idle_evidence(self):
  f.append('pg-c4-n16',{'batch':50,'role':'primary','status':'ok','imported':'true','attempt_dir':'absent'})
  with mock.patch('builtins.print') as p:code=f.audit('pg-c4-n16')
  text=' '.join(str(c.args[0]) for c in p.call_args_list)
  self.assertEqual(code,1);self.assertIn('VIOLATION imported row missing idle evidence',text)
 def test_tidb_full_window_cpu_uses_attempt_capacity_six(self):
  import datetime
  d=self.tmp/'cpu-attempt';(d/'res-tidb').mkdir(parents=True)
  start=datetime.datetime(2024,1,1,tzinfo=datetime.timezone.utc);end=start+datetime.timedelta(seconds=1)
  summary={'resource_summary':{'container_resource_summaries':{'res-tikv1':{'window_start_unix_ns':int(start.timestamp()*1e9),'window_end_unix_ns':int(end.timestamp()*1e9)}}}}
  with (d/'res-tidb'/'resources.csv').open('w',newline='') as out:
   w=csv.DictWriter(out,fieldnames=['wall_utc','monotonic','db_cpu_usage_usec','memory_current_bytes','allocated_cpu_capacity_cores']);w.writeheader()
   for t,mono,cpu in ((start,0,0),(start+datetime.timedelta(milliseconds=500),.5,2875000),(end,1,5750000)):
    w.writerow({'wall_utc':t.isoformat(),'monotonic':mono,'db_cpu_usage_usec':cpu,'memory_current_bytes':1,'allocated_cpu_capacity_cores':6})
  cores,percent=f.cs.tidb_full_window_cpu(d,summary)
  self.assertAlmostEqual(cores,5.75);self.assertAlmostEqual(percent,5.75/6*100)
 def test_tidb_attempt_reclassifies_full_window_cpu(self):
  s='tidb-c8x-n16';directory=None
  def runner(argv,cwd):
   nonlocal directory
   directory=f.ROOT/argv[2];directory.mkdir(parents=True)
   (directory/'cleanup.json').write_text('{"status":"success"}')
   (directory/'summary.json').write_text('{"resource_summary":{"resource_window_formal":false}}')
   return type('P',(),{'returncode':0})()
  with mock.patch.object(f.bs,'idle_gate',return_value={'status':'ok'}),mock.patch.object(f.cs,'is_ok',return_value=True),mock.patch.object(f.cs,'tidb_full_window_cpu',return_value=(5.75,95.8)),mock.patch.object(f.bs,'extract_row',return_value={'rps_request':1}):
   row=f.attempt(s,50,'primary',[],runner)
  self.assertEqual(row['tidb_cpu_cores'],5.75);self.assertEqual(row['cause'],f.cs.TIDB_RECLASSIFY_NOTE)
 def _exercise_run_series_failure(self,s,flush_failure):
  calls=[];seen={}
  def runner(argv,cwd):
   rel=argv[2];directory=f.ROOT/rel;directory.mkdir(parents=True);batch=int(rel.split('-bm')[1].split('-')[0]);index=int(rel.rsplit('-a',1)[1]);calls.append((batch,index));seen[batch]=seen.get(batch,0)+1
   good=not ((flush_failure and batch==50 and seen[batch]==2) or (not flush_failure and batch==100))
   (directory/'cleanup.json').write_text('{"status":"success"}')
   summary={'resource_summary':{'resource_window_formal':True},'marker':good}
   (directory/'summary.json').write_text(json.dumps(summary))
   (directory/'failure.txt').write_text('database failure' if not good else '')
   return type('P',(),{'returncode':0 if good else 1})()
  def extract(db,summ):return {'rps_request':100,'flush_await_ms':3 if flush_failure else 0}
  with mock.patch.object(f.bs,'idle_gate',return_value={'status':'ok'}),mock.patch.object(f,'ensure_imports'),mock.patch.object(f,'imported_rows',return_value=[]),mock.patch.object(f.cs,'is_ok',side_effect=lambda db,su,cl,ex,**kw:ex==0),mock.patch.object(f.bs,'extract_row',side_effect=extract):
   f.run_series(s,runner=runner)
  rs=f.rows(s)
  self.assertEqual(next(x for x in rs if x['role']=='terminated')['cause'],'failure-limit')
  self.assertFalse(any(b> (50 if flush_failure else 100) for b,i in calls))
  self.assertTrue(any(x['role']=='replicate' for x in rs))
  if flush_failure:self.assertEqual(calls[:2][0][0],50);self.assertEqual(calls[:2][1][0],50)
  else:self.assertIn(100,seen)
 def test_run_series_failure_limit_primary_pg_and_tidb(self):
  for s in ('pg-c4-n16','tidb-c4-n16'):
   with self.subTest(series=s):self._exercise_run_series_failure(s,False)
 def test_run_series_failure_limit_flush_rerun_pg_and_tidb(self):
  for s in ('pg-c4-n16','tidb-c4-n16'):
   with self.subTest(series=s):self._exercise_run_series_failure(s,True)
 def test_shared_cpu_one_res_pd_source(self):
  d=self.tmp/'attempt';(d/'summary.json').parent.mkdir(parents=True)
  data={'resource_summary':{'container_resource_summaries':{'res-pd':{'means':{'client_cpu_core_equivalents':.5,'db_cpu_core_equivalents':.6,'sampler_cpu_core_equivalents':.7}},'res-tikv1':{'means':{'sampler_cpu_core_equivalents':99}}}}}
  (d/'summary.json').write_text(json.dumps(data));self.assertEqual(f.shared_cpu(d)[:3],(.5,.6,.7));self.assertAlmostEqual(f.shared_cpu(d)[3],.9)
class RevisedSeriesTests(IsolatedFileTests):
 def test_new_series_argv_metadata_and_grid(self):
  for sid,cpus,extended,tikv,suffix,ref,size,quota in (('tidb-k4t4-n16',4,False,False,'k4t4','tidb-c4-n16',4,4),('tidb-k6t8-n16',8,True,True,'k6t8','tidb-c8x-n16',8,6)):
   a=f.wrapper_argv(sid,2000,'D')
   self.assertEqual(f.grid_for(sid),[2000,4000,8000]);self.assertEqual(a[a.index('--connections')+1],'16')
   self.assertEqual(a[a.index('--cpus')+1],str(cpus));self.assertEqual('--tidb-extended' in a,extended);self.assertEqual('--tikv-cpus' in a,tikv)
   self.assertIn(suffix,f.attempt_name(sid,2000,1));self.assertEqual(f.series_meta(sid),{'db':'tidb','wrapper_cpus':cpus,'tidb_cpuset_size':size,'tikv_quota_total':quota,'reference_series':ref,'grid':[2000,4000,8000],'suffix':suffix})
 def test_new_completion_grid_complete_and_failure_limit(self):
  s='tidb-k4t4-n16';rs=[{'batch':str(b),'role':'primary','status':'ok','rps_request':'10'} for b in (2000,4000,8000)]
  self.assertEqual(f.completion(rs,s),'grid-complete')
  self.assertEqual(f.completion(rs[:1]+[{'batch':'4000','role':'primary','status':'incomplete','cleanup':'success','cause':'database'}],s),'failure-limit')

 def _new_runner(self,s,fail_batch=None,flush_fail=False,rate=100):
  calls=[];counts={}
  def runner(argv,cwd):
   rel=argv[2];d=f.ROOT/rel;d.mkdir(parents=True);b=int(rel.split('-bm')[1].split('-')[0]);idx=int(rel.rsplit('-a',1)[1]);calls.append((b,idx));counts[b]=counts.get(b,0)+1
   good=not (fail_batch==b and ((flush_fail and counts[b]==2) or (not flush_fail)))
   (d/'cleanup.json').write_text('{"status":"success"}')
   (d/'summary.json').write_text(json.dumps({'resource_summary':{'resource_window_formal':True},'outcomes':{'windows':f.expected_windows(b)},'test_flush':flush_fail and b==fail_batch}))
   (d/'failure.txt').write_text('database failure' if not good else '')
   return type('P',(),{'returncode':0 if good else 1})()
  patches=(mock.patch.object(f.bs,'idle_gate',return_value={'status':'ok'}),mock.patch.object(f,'ensure_imports'),mock.patch.object(f,'imported_rows',return_value=[]),mock.patch.object(f.cs,'is_ok',side_effect=lambda db,su,cl,ex,**kw:ex==0),mock.patch.object(f.bs,'extract_row',side_effect=lambda db,su:{'rps_request':rate,'flush_await_ms':3 if su.get('test_flush') else 0,'windows':su['outcomes']['windows']}))
  return runner,calls,patches
 def test_new_series_run_grid_termination_and_resume(self):
  for s in ('tidb-k4t4-n16','tidb-k6t8-n16'):
   with self.subTest(series=s):
    runner,calls,patches=self._new_runner(s)
    with patches[0],patches[1],patches[2],patches[3],patches[4]:f.run_series(s,runner=runner)
    self.assertEqual([x[0] for x in calls],[2000,4000,8000])
    rs=f.rows(s);self.assertEqual(next(x['cause'] for x in rs if x['role']=='terminated'),'grid-complete')
    n=len(calls)
    with patches[0],patches[1],patches[2],patches[3],patches[4]:f.run_series(s,runner=runner)
    self.assertEqual(len(calls),n)
 def test_new_series_failure_limit_primary_and_flush(self):
  for s in ('tidb-k4t4-n16','tidb-k6t8-n16'):
   for flush in (False,True):
    with self.subTest(series=s,flush=flush):
     for item in f.ART.iterdir():
      if item.is_dir():__import__('shutil').rmtree(item)
      else:item.unlink()
     runner,calls,patches=self._new_runner(s,4000,flush)
     with patches[0],patches[1],patches[2],patches[3],patches[4]:f.run_series(s,runner=runner)
     rs=f.rows(s);self.assertEqual(next(x['cause'] for x in rs if x['role']=='terminated'),'failure-limit')
     self.assertFalse(any(b>4000 for b,_ in calls))
     if flush:self.assertTrue(any(x['role']=='flush_rerun' for x in rs),repr(rs))
 def test_new_series_replicate_threshold_and_idempotence(self):
  for s in ('tidb-k4t4-n16','tidb-k6t8-n16'):
   for multiplier,count in ((1.15,0),(.85,0),(1.151,1),(.849,1)):
    with self.subTest(series=s,multiplier=multiplier):
     for item in f.ART.iterdir():
      if item.is_dir():__import__('shutil').rmtree(item)
      else:item.unlink()
     ref=f._options(s)['reference_series']
     for b in (2000,4000,8000):f.append(ref,{'batch':b,'role':'primary','status':'ok','rps_request':'100'})
     rate={1.15:115.0,.85:85.0,1.151:115.1,.849:84.9}[multiplier]
     runner,calls,patches=self._new_runner(s,rate=rate)
     with patches[0],patches[1],patches[2],patches[3],patches[4]:f.run_series(s,runner=runner)
     rs=f.rows(s);reps=[r for r in rs if r['role']=='replicate']
     self.assertEqual(len(reps),count*3)
     for b in (2000,4000,8000):self.assertEqual(f.effective_primary(rs)[b]['rps_request'],str(rate))
     n=len(calls)
     with patches[0],patches[1],patches[2],patches[3],patches[4]:f.run_series(s,runner=runner)
     self.assertEqual(len(calls),n)
 def test_new_series_reference_missing_warns_without_violation_and_empty_audit(self):
  for s in ('tidb-k4t4-n16','tidb-k6t8-n16'):
   with mock.patch('builtins.print') as p:code=f.audit(s)
   text=' '.join(str(x.args[0]) for x in p.call_args_list)
   self.assertEqual(code,0);self.assertIn('completion=running',text);self.assertNotIn('VIOLATION',text)
   # Run with successful primaries but no matching reference: no replicate is launched.
   runner,calls,patches=self._new_runner(s)
   with patches[0],patches[1],patches[2],patches[3],patches[4]:f.run_series(s,runner=runner)
   self.assertFalse(any(x['role']=='replicate' for x in f.rows(s)))
   with mock.patch.object(f,'shared_cpu',return_value=(1,1,1,1)),mock.patch('builtins.print') as p:code=f.audit(s)
   text=' '.join(str(x.args[0]) for x in p.call_args_list)
   self.assertEqual(code,0);self.assertIn('WARNING reference series',text)
 def test_new_series_dry_run_and_all_scope(self):
  for s in ('tidb-k4t4-n16','tidb-k6t8-n16'):
   before=sorted(x.name for x in f.ART.iterdir())
   with mock.patch('builtins.print') as p:f.run_series(s,True)
   self.assertEqual(sorted(x.name for x in f.ART.iterdir()),before)
   lines=[x.args[0] for x in p.call_args_list];self.assertEqual(sum(x.startswith('argv: ') for x in lines),3);self.assertEqual(lines[-1],'note: three-point grid; extra runs depend on reference deviation')
  with mock.patch.object(f,'run_series') as run:f.main(['run','--series','all']);self.assertEqual([x.args[0] for x in run.call_args_list],list(f.ORIGINAL_SERIES))
  with mock.patch('builtins.print') as p:f.audit()
  self.assertEqual([x.args[0].split(':')[0] for x in p.call_args_list if ': completion=' in x.args[0]],list(f.ORIGINAL_SERIES))
 def test_new_series_audit_required_evidence_violations(self):
  for s in ('tidb-k4t4-n16','tidb-k6t8-n16'):
   f.append(s,{'batch':2000,'role':'primary','status':'ok','rps_request':'200','windows':1,'attempt_dir':'gone'})
   f.append(s,{'batch':2000,'role':'terminated','status':'terminated','cause':'grid-complete'})
   f.append(s,{'batch':2000,'role':'primary','status':'ok','rps_request':'200','windows':1,'attempt_dir':'after'})
   with mock.patch('builtins.print') as p:code=f.audit(s)
   text=' '.join(x.args[0] for x in p.call_args_list)
   self.assertEqual(code,1);self.assertIn('windows mismatch',text);self.assertIn('idle evidence missing',text);self.assertIn('attempt after termination',text)
   if s=='tidb-k6t8-n16':self.assertIn('shared-CPU evidence missing',text)
   else:self.assertNotIn('shared-CPU evidence missing',text)
   f.ART.joinpath(f.series_file(s).name).unlink()
  s='tidb-k6t8-n16';f.append(s,{'batch':2000,'role':'primary','status':'ok','rps_request':'200','windows':f.expected_windows(2000),'attempt_dir':'okdir'})
  # A terminated grid-complete state without required replicate must be flagged.
  f.append(s,{'batch':2000,'role':'terminated','status':'terminated','cause':'grid-complete'})
  ref=f._options(s)['reference_series'];f.append(ref,{'batch':2000,'role':'primary','status':'ok','rps_request':'100'})
  with mock.patch('builtins.print') as p:code=f.audit(s)
  self.assertEqual(code,1);self.assertIn('missing required replicate', ' '.join(x.args[0] for x in p.call_args_list))
if __name__=='__main__':unittest.main()
