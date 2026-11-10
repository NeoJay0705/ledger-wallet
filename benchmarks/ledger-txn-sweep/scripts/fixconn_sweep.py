#!/usr/bin/env python3
"""Fixed-connection batch sweep. Serial, resumable, and stdlib-only."""
import argparse,csv,json,math,pathlib,subprocess,sys,time
from decimal import Decimal
ROOT=pathlib.Path(__file__).resolve().parents[1]; ART=ROOT/'artifacts';sys.path.insert(0,str(pathlib.Path(__file__).resolve().parent))
import conn_sweep as cs
import batch_sweep as bs
GRID=[50,100,250,500,1000,2000,4000,8000];USERS=50000;REQS=40;QUEUES=256
SERIES={'pg-c4-n16':('pg',4,16,False),'tidb-c4-n16':('tidb',4,16,False),'pg-c8-n16':('pg',8,16,False),'tidb-c8x-n16':('tidb',8,16,True),'tidb-c8x-n32':('tidb',8,32,True),'tidb-k4t4-n16':('tidb',4,16,False),'tidb-k6t8-n16':('tidb',8,16,True)}
ORIGINAL_SERIES=('pg-c4-n16','tidb-c4-n16','pg-c8-n16','tidb-c8x-n16','tidb-c8x-n32')
SERIES_OPTIONS={'tidb-k4t4-n16':{'suffix':'k4t4','grid':[2000,4000,8000],'wrapper_cpus':4,'tidb_cpuset_size':4,'tikv_quota_total':4,'reference_series':'tidb-c4-n16'},'tidb-k6t8-n16':{'suffix':'k6t8','grid':[2000,4000,8000],'wrapper_cpus':8,'tidb_cpuset_size':8,'tikv_quota_total':6,'tikv_cpus':6,'extended':True,'shared_cpu':True,'reference_series':'tidb-c8x-n16'}}
def _options(s):return SERIES_OPTIONS.get(s,{})
def grid_for(s):return _options(s).get('grid',GRID)
def _suffix(s):
 if s in SERIES_OPTIONS:return _options(s)['suffix']
 db,c,n,x=SERIES[s];return f'c{c}{"x" if x else ""}'
def _is_new(s):return s in SERIES_OPTIONS
def series_meta(s):
 db,c,n,x=SERIES[s];o=_options(s)
 return {'db':db,'wrapper_cpus':o.get('wrapper_cpus',c),'tidb_cpuset_size':o.get('tidb_cpuset_size',6 if x and db=='tidb' else c if db=='tidb' else None),'tikv_quota_total':o.get('tikv_quota_total',8 if x and db=='tidb' else 4 if db=='tidb' else None),'reference_series':o.get('reference_series'),'grid':grid_for(s),'suffix':_suffix(s)}
COLUMNS=['batch','role','attempt_dir','status','exit','go_reached','cleanup','wall_s','duration_s','rps_request','p50_s','p95_s','p99_s','max_s','windows','db_cpu_cores','tidb_cpu_cores','flush_await_ms','client_cpu_cores','imported','cause']
def series_file(s):
 db,c,n,x=SERIES[s];return ART/f'fixconn-{db}-{_suffix(s)}-n{n}.csv'
def wrapper_argv(s,b,d):
 db,c,n,x=SERIES[s];a=['python3','scripts/batch_queue_run.py' if db=='pg' else 'scripts/txn_tidb_run.py',d]
 if db=='pg':a+=['--mode','txn']
 a+=['--cpus',str(c),'--users',str(USERS),'--requests-per-user',str(REQS),'--queues',str(QUEUES),'--connections',str(n),'--batch-max',str(b),'--sample-interval-ms','250']
 if db=='pg':a+=['--pg-conf','wal_buffers=256MB','--pg-conf','max_wal_size=8GB','--pg-conf','track_wal_io_timing=on']
 if x or _options(s).get('extended'):a+=['--tidb-extended']
 if _options(s).get('tikv_cpus'):a+=['--tikv-cpus',str(_options(s)['tikv_cpus'])]
 return a
def attempt_name(s,b,k):
 db,c,n,x=SERIES[s];return f'artifacts/fixconn-{db}-{_suffix(s)}-n{n}-bm{b}-a{k}'
def rows(s):
 p=series_file(s)
 if not p.exists():return []
 with p.open(newline='') as f:return list(csv.DictReader(f))
def append(s,row):
 p=series_file(s);p.parent.mkdir(parents=True,exist_ok=True)
 with p.open('a',newline='') as f:
  w=csv.DictWriter(f,fieldnames=COLUMNS)
  if p.stat().st_size==0:w.writeheader()
  w.writerow({k:row.get(k,'') for k in COLUMNS})
def append_note(old,note):return old if note in old else (old+'; ' if old else '')+note
def effective_primary(rs):
 out={}
 for b in GRID:
  group=[r for r in rs if str(r.get('batch'))==str(b)]
  p=next((r.copy() for r in reversed(group) if r.get('role')=='primary' and r.get('status')=='ok'),None)
  if not p:continue
  rer=next((r for r in reversed(group) if r.get('role')=='flush_rerun'),None)
  if rer:
   if rer.get('status')=='ok':p=rer.copy()
   else:p['cause']=append_note(p.get('cause',''),'flush>2ms, rerun failed')
  out[b]=p
 return out
def expected_windows(b):return 80*math.ceil(7840/b)+176*math.ceil(7800/b)
def imported_rows(s,write=False):
 db,c,n,x=SERIES[s]
 if n!=16 or x:return []
 out=[]
 for b in (50,100):
  eff=cs.effective_primary(cs.rows_for(db,c,b)).get(16)
  if not eff:raise RuntimeError(f'missing effective connsweep import: {db} c{c} batch {b} conns 16')
  rel=eff.get('attempt_dir','');idle=ROOT/(rel+'-idle.json')
  if not rel or not idle.is_file():raise RuntimeError(f'missing import idle evidence: {idle}')
  row={k:eff.get(k,'') for k in COLUMNS};row.update(batch=b,role='primary',status='ok',imported='true',attempt_dir=rel)
  if write:append(s,row)
  out.append(row)
 return out
def ensure_imports(s):
 present=rows(s)
 for r in imported_rows(s):
  if not any(str(x.get('batch'))==str(r['batch']) and x.get('imported')=='true' for x in present):append(s,r);present.append(r)
def completion(rs,series=None):
 term=next((r.get('cause') for r in rs if r.get('role')=='terminated'),None)
 if term:return term
 eff=effective_primary(rs);grid=grid_for(series) if series else GRID
 for b in grid:
  if _needs_flush(_latest_primary(rs,b),rs,b):return None
 best=None;streak=0
 for b in grid:
  if b not in eff:
   failed=any(str(x.get('batch'))==str(b) and x.get('role') in ('primary','flush_rerun') and x.get('status') not in ('ok','','gate_failed') and x.get('cause')!='sampler' and x.get('cleanup')=='success' for x in rs)
   if failed:return 'failure-limit'
   return None
  if _is_new(series):continue
  try:v=float(eff[b]['rps_request'])
  except (ValueError,TypeError,KeyError):return None
  if best is not None:streak=streak+1 if v<1.05*best else 0
  best=v if best is None else max(best,v)
  if streak==2:return 'stop-rule'
  if b==8000:return 'cap'
 return 'grid-complete' if _is_new(series) and all(b in eff for b in grid) else None
def saturation(rs,reason=None):
 vals=[(b,float(r['rps_request'])) for b,r in effective_primary(rs).items() if r.get('rps_request') not in ('',None)]
 if not vals:return None,None
 top=max(v for _,v in vals);b=min(b for b,v in vals if v>=.95*top)
 return b,'observed-range' if reason in ('failure-limit','cap') else 'saturation point'
def replicate_set(rs,series=None):
 reason=completion(rs,series);b,_=saturation(rs,reason)
 if _is_new(series):return []
 if not reason or b is None:return []
 i=GRID.index(b);return [b] if i==0 else [b,GRID[i-1]]
def _index(s,b,rs):
 prefix=pathlib.Path(attempt_name(s,b,0)).name.rsplit('-a',1)[0]+'-a';nums=[]
 for p in ART.glob(prefix+'*'):
  try:nums.append(int(p.name.rsplit('-a',1)[1].split('-idle',1)[0]))
  except ValueError:pass
 for r in rs:
  try:nums.append(int(pathlib.Path(r['attempt_dir']).name.rsplit('-a',1)[1]))
  except (KeyError,ValueError,IndexError):pass
 return max(nums,default=0)+1
def shared_cpu(attempt_dir):
 """Return (client, PD, wrapper/sampler, combined/2 CPUs) from res-pd only.
 means.client_cpu_core_equivalents is the client field; res-pd's
 db_cpu_core_equivalents is PD and wrapper_cpu_core_equivalents is the wrapper.
 All res-* records observe identical wrapper ticks, so summing them double-counts.
 """
 try:
  s=json.loads((pathlib.Path(attempt_dir)/'summary.json').read_text());r=s['resource_summary']['container_resource_summaries']['res-pd']
  means=r.get('means') or {};client=means.get('client_cpu_core_equivalents');pd=means.get('db_cpu_core_equivalents');wrapper=means.get('sampler_cpu_core_equivalents')
  vals=tuple(float(v) for v in (client,pd,wrapper));return (*vals,sum(vals)/2)
 except (OSError,ValueError,KeyError,TypeError):return None
def _cause(directory,cleanup):
 fail=''
 for p in (directory/'failure.txt',directory/'client'/'invalid.json'):
  if p.exists():fail+=' '+p.read_text(errors='replace')
 return bs.classify_cause(fail,cleanup=cleanup.get('status',''))
def attempt(s,b,role,rs,runner=None):
 rel=attempt_name(s,b,_index(s,b,rs));argv=wrapper_argv(s,b,rel);directory=ROOT/rel
 if directory.exists():raise RuntimeError(f'attempt exists: {rel}')
 ART.mkdir(parents=True,exist_ok=True);gate=bs.idle_gate();(ROOT/(rel+'-idle.json')).write_text(json.dumps(gate,indent=2)+'\n')
 row={'batch':b,'role':role,'attempt_dir':rel,'status':'gate_failed','go_reached':'false','cause':'idle gate','imported':'false'}
 if gate.get('status')=='ok':
  start=time.monotonic()
  p=(runner or subprocess.run)(argv,cwd=ROOT)
  row['wall_s']=time.monotonic()-start
  try:cleanup=json.loads((directory/'cleanup.json').read_text())
  except (OSError,ValueError):cleanup={}
  try:summ=json.loads((directory/'summary.json').read_text())
  except (OSError,ValueError):summ={}
  row.update(exit=p.returncode,cleanup=cleanup.get('status','missing'),go_reached=str((directory/'GO').exists() or (directory/'client/result.json').exists()).lower())
  db,c,n,x=SERIES[s];good=cs.is_ok(db,summ,cleanup,p.returncode,connections=n,batch=b,attempt_dir=directory)
  row['status']='ok' if good else 'incomplete'
  if summ:
   row.update(bs.extract_row(db,summ));row['client_cpu_cores']=cs._summary_client(db,summ)
  cause=_cause(directory,cleanup)
  if not good:row['cause']=cause
  if good:
   row['cause']=''
   db,c,n,x=SERIES[s]
   if db=='tidb' and not (summ.get('resource_summary') or {}).get('resource_window_formal'):
    cpu=cs.tidb_full_window_cpu(directory,summ)
    if cpu is not None:
     row['tidb_cpu_cores']=cpu[0];row['cause']=append_note(row['cause'],cs.TIDB_RECLASSIFY_NOTE)
  if cleanup.get('status')!='success':append(s,row);raise RuntimeError('cleanup failure stops campaign')
 append(s,row)
 if row['cause']=='idle gate':raise RuntimeError('idle gate failed; campaign stopped')
 return row
def _latest_primary(rs,b):return next((r for r in reversed(rs) if str(r.get('batch'))==str(b) and r.get('role')=='primary' and r.get('status')=='ok'),None)
def _needs_flush(primary,rs,b):
 rer=[r for r in rs if str(r.get('batch'))==str(b) and r.get('role')=='flush_rerun']
 return primary is not None and not rer and bs.should_flush_rerun(primary)
def _flush(s,b,rs,runner):
 primary=_latest_primary(rs,b)
 if not _needs_flush(primary,rs,b):return
 r=attempt(s,b,'flush_rerun',rs,runner)
 if r.get('cause')=='sampler':
  for _ in range(2):
   r=attempt(s,b,'flush_rerun',rows(s),runner)
   if r.get('cause')!='sampler':break
 if r.get('cause')=='sampler':raise RuntimeError('sampler retries exhausted; curve incomplete')
 if r.get('status')!='ok':append(s,{'batch':b,'role':'terminated','status':'terminated','cause':'failure-limit'} )
def _primary(s,b,rs,runner):
 r=attempt(s,b,'primary',rs,runner)
 for _ in range(2):
  if r.get('status')=='ok' or r.get('cause')!='sampler':break
  r=attempt(s,b,'primary',rows(s),runner)
 if r.get('cause')=='sampler':raise RuntimeError('sampler retries exhausted; curve incomplete')
 if r.get('status')!='ok':append(s,{'batch':b,'role':'terminated','status':'terminated','cause':'failure-limit'});return
 _flush(s,b,rows(s),runner)
def run_series(s,dry=False,runner=None):
 if dry:
  rs=rows(s);imps={int(r['batch']) for r in imported_rows(s)} if not _is_new(s) and SERIES[s][2]==16 and not SERIES[s][3] else set()
  eff=effective_primary(rs)
  for b in grid_for(s):
   if b not in imps and b not in eff:print('argv: '+json.dumps(wrapper_argv(s,b,attempt_name(s,b,_index(s,b,rs)))))
  if not _is_new(s):print('note: later points depend on the stop rule')
  else:print('note: three-point grid; extra runs depend on reference deviation')
  return
 ensure_imports(s);rs=rows(s)
 if completion(rs,s):
  if not _is_new(s):_run_replicates(s,rs,runner)
  return
 for b in grid_for(s):
  rs=rows(s)
  if completion(rs,s):break
  p=_latest_primary(rs,b)
  if p is None:_primary(s,b,rs,runner)
  else:_flush(s,b,rs,runner)
  rs=rows(s)
  if _is_new(s) and _latest_primary(rs,b) and not any(x.get('role')=='terminated' for x in rs):_run_series_replicate(s,b,rs,runner)
  rs=rows(s)
  if completion(rs,s):
   if not any(x.get('role')=='terminated' for x in rs):append(s,{'batch':b,'role':'terminated','status':'terminated','cause':completion(rs,s)})
   break
 rs=rows(s);reason=completion(rs,s)
 if reason and not any(x.get('role')=='terminated' for x in rs):append(s,{'role':'terminated','status':'terminated','cause':reason});rs=rows(s)
 if reason and not _is_new(s):_run_replicates(s,rs,runner)
def _replicate_needed(s,b,rs):
 ref=effective_primary(rows(_options(s)['reference_series'])).get(b)
 primary=effective_primary(rs).get(b)
 if not ref or not primary:return False
 try:return abs(Decimal(str(primary['rps_request']))/Decimal(str(ref['rps_request']))-Decimal(1))>Decimal('0.15')
 except (ValueError,TypeError,ZeroDivisionError,KeyError):return False
def _run_series_replicate(s,b,rs,runner=None):
 if not _replicate_needed(s,b,rs) or any(str(x.get('batch'))==str(b) and x.get('role')=='replicate' for x in rs):return
 r=attempt(s,b,'replicate',rs,runner)
 for _ in range(2):
  if r.get('cause')!='sampler':break
  r=attempt(s,b,'replicate',rows(s),runner)
 if r.get('cause')=='sampler':raise RuntimeError('replicate sampler retries exhausted')
def _run_replicates(s,rs,runner=None):
 for b in replicate_set(rs,s):
  rs=rows(s)
  if any(str(x.get('batch'))==str(b) and x.get('role')=='replicate' for x in rs):continue
  r=attempt(s,b,'replicate',rs,runner)
  for _ in range(2):
   if r.get('cause')!='sampler':break
   r=attempt(s,b,'replicate',rows(s),runner)
  if r.get('cause')=='sampler':raise RuntimeError('replicate sampler retries exhausted')
def audit(only=None):
 bad=False
 for s in ([only] if only else ORIGINAL_SERIES):
  rs=rows(s);reason=completion(rs,s) or 'running';sat,label=saturation(rs,reason if reason!='running' else None)
  print(f'{s}: completion={reason} saturation={sat} label={label}')
  eff=effective_primary(rs);print('  batches: '+', '.join(f'{b}={"flush-flag" if b in eff and "flush>2ms" in eff[b].get("cause","") else "ok" if b in eff else "failed" if any(str(x.get("batch"))==str(b) for x in rs) else "missing"}' for b in grid_for(s)))
  print('  imported: '+(', '.join(str(x.get('batch')) for x in rs if x.get('imported')=='true') or 'none'))
  done=[int(x['batch']) for x in rs if x.get('role')=='replicate'];print(f'  replicates required={replicate_set(rs,s) if not _is_new(s) else [b for b in grid_for(s) if _replicate_needed(s,b,rs)]} done={done}')
  for x in rs:
   if x.get('imported')=='true' and not (ROOT/(x.get('attempt_dir','')+'-idle.json')).is_file():print('  VIOLATION imported row missing idle evidence');bad=True
   if x.get('status')=='ok':
    try:
     if int(float(x.get('windows',''))) != expected_windows(int(x['batch'])):print(f'  VIOLATION windows mismatch batch={x["batch"]}');bad=True
    except (ValueError,TypeError,KeyError):print(f'  VIOLATION windows missing batch={x.get("batch")}');bad=True
   if (s.startswith('tidb-c8x') or _options(s).get('shared_cpu',False)) and x.get('status')=='ok' and shared_cpu(ROOT/x.get('attempt_dir','')) is None:print('  VIOLATION shared-CPU evidence missing');bad=True
  term=next((i for i,x in enumerate(rs) if x.get('role')=='terminated'),None)
  if term is not None and any(x.get('role') in ('primary','flush_rerun') and x.get('attempt_dir') for x in rs[term+1:]):print('  VIOLATION attempt after termination');bad=True
  if _is_new(s):
   if reason!='running' and reason not in ('grid-complete','failure-limit'):print(f'  VIOLATION completion={reason}');bad=True
   if reason=='grid-complete' and any(b not in eff for b in grid_for(s)):print('  VIOLATION grid missing effective primary');bad=True
   refpath=series_file(_options(s)['reference_series'])
   if not refpath.is_file():print(f'  WARNING reference series {_options(s)["reference_series"]} is missing')
   for x in rs:
    if x.get('attempt_dir') and not (ROOT/(x['attempt_dir']+'-idle.json')).is_file():print(f'  VIOLATION idle evidence missing for {x["attempt_dir"]}');bad=True
   for b in grid_for(s):
    if b in eff and _replicate_needed(s,b,rs) and not any(str(y.get('batch'))==str(b) and y.get('role')=='replicate' for y in rs):print(f'  VIOLATION missing required replicate [{b}]');bad=True
  if reason!='running':
   required=replicate_set(rs,s) if not _is_new(s) else [b for b in grid_for(s) if _replicate_needed(s,b,rs)]
   missing=[b for b in required if b not in done]
   if missing:print(f'  VIOLATION missing required replicate {missing}');bad=True
 return int(bad)
def main(argv=None):
 p=argparse.ArgumentParser();sp=p.add_subparsers(dest='cmd',required=True);r=sp.add_parser('run');r.add_argument('--series',required=True);r.add_argument('--dry-run',action='store_true');a=sp.add_parser('audit');a.add_argument('--series',choices=list(SERIES));args=p.parse_args(argv)
 if args.cmd=='audit':
  if args.series:return audit(args.series)
  return audit()
 names=list(ORIGINAL_SERIES) if args.series=='all' else [args.series]
 if any(x not in SERIES for x in names):p.error('unknown series')
 for s in names:run_series(s,args.dry_run)
 return 0
def audit_series(s):return audit(s)
if __name__=='__main__':sys.exit(main())
