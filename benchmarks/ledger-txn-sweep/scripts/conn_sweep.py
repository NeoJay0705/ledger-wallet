#!/usr/bin/env python3
"""Serial resumable connection-count sweep; stdlib only."""
import argparse,csv,json,pathlib,subprocess,sys,time,datetime
ROOT=pathlib.Path(__file__).resolve().parents[1]; ART=ROOT/'artifacts'
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parent))
import batch_sweep as bs
USERS=50000;REQS=40;QUEUES=256;BATCHES=(50,100)
GRIDS={4:[4,8,16,32,64,128,256],8:[8,16,32,64,128,256]}
COLUMNS=['conns','role','attempt_dir','status','exit','go_reached','cleanup','wall_s','duration_s','rps_request','p50_s','p95_s','p99_s','max_s','windows','db_cpu_cores','tidb_cpu_cores','flush_await_ms','client_cpu_cores','cause']
def wrapper_argv(db,cpus,batch,conns,directory):
 c=['python3','scripts/batch_queue_run.py' if db=='pg' else 'scripts/txn_tidb_run.py',directory]
 if db=='pg':c+=['--mode','txn']
 c+=['--cpus',str(cpus),'--users',str(USERS),'--requests-per-user',str(REQS),'--queues',str(QUEUES),'--connections',str(conns),'--batch-max',str(batch),'--sample-interval-ms','250']
 if db=='pg':c+=['--pg-conf','wal_buffers=256MB','--pg-conf','max_wal_size=8GB','--pg-conf','track_wal_io_timing=on']
 return c
def attempt_name(db,c,b,n,k):return f'artifacts/connsweep-{db}-c{c}-bm{b}-n{n}-a{k}'
def csv_path(db,c,b):return ART/f'connsweep-{db}-c{c}-bm{b}.csv'
def rows_for(db,c,b):
 p=csv_path(db,c,b)
 if not p.exists():return []
 with p.open(newline='') as f:return list(csv.DictReader(f))
def next_index(db,c,b,n):
 prefix=f'connsweep-{db}-c{c}-bm{b}-n{n}-a'; seen=[]
 for p in ART.glob(prefix+'*'):
  try:seen.append(int(p.name.rsplit('-a',1)[1].split('-idle',1)[0]))
  except ValueError:pass
 for r in rows_for(db,c,b):
  try:seen.append(int(pathlib.Path(r['attempt_dir']).name.rsplit('-a',1)[1]))
  except (ValueError,KeyError):pass
 return max(seen,default=0)+1
def effective_primary(rows):
 out={}
 for n in sorted({int(r['conns']) for r in rows if r.get('conns')}):
  group=[r for r in rows if int(r.get('conns',0) or 0)==n]
  primary=next((r for r in reversed(group) if r.get('role')=='primary' and r.get('status')=='ok'),None)
  rerun=next((r for r in reversed(group) if r.get('role')=='flush_rerun'),None)
  if primary:
   selected=primary.copy()
   if rerun:
    if rerun.get('status')=='ok':selected=rerun.copy()
    else:selected['cause']='flush>2ms, rerun failed'
   out[n]=selected
 return out
def saturation(rows,grid):
 effective=effective_primary(rows)
 if any(n not in effective for n in grid):return {'status':'incomplete','conns':None,'label':None}
 top=max(float(effective[n]['rps_request']) for n in grid)
 n=next(x for x in grid if float(effective[x]['rps_request'])>=.95*top)
 return {'status':'ok','conns':n,'label':'observed-grid saturation' if n==256 else 'saturation point'}
def replicate_points(rows,grid):
 sat=saturation(rows,grid)
 if sat['status']!='ok':return []
 i=grid.index(sat['conns']);return [sat['conns']] if i==0 else [sat['conns'],grid[i-1]]
def _summary_client(db,s):
 r=s.get('resource_summary') or {}
 return ((r.get('means') or {}).get('client_cpu_core_equivalents') if db=='pg' else r.get('client_cpu_cores_mean'))
TIDB_RECLASSIFY_NOTE='tidb cgroup io counters blank at start; tidb cpu recomputed over full window'

def _utc(value):
 try:return datetime.datetime.fromisoformat(value.replace('Z','+00:00')).astimezone(datetime.timezone.utc)
 except (ValueError,TypeError,AttributeError):return None

def tidb_full_window_cpu(attempt_dir,summary):
 try:
  resource=summary['resource_summary'];window=resource['container_resource_summaries']['res-tikv1']
  start=datetime.datetime.fromtimestamp(int(window['window_start_unix_ns'])/1e9,datetime.timezone.utc)
  end=datetime.datetime.fromtimestamp(int(window['window_end_unix_ns'])/1e9,datetime.timezone.utc)
  with (pathlib.Path(attempt_dir)/'res-tidb'/'resources.csv').open(newline='') as f:rows=list(csv.DictReader(f))
  timed=[(t,r) for r in rows if (t:=_utc(r.get('wall_utc'))) is not None]
  inside=[(t,r) for t,r in timed if start<=t<=end]
  baselines=[(t,r) for t,r in timed if t<=start]
  if not inside or not baselines:return None
  selected=[max(baselines,key=lambda x:x[0])[1]]+[r for _,r in inside]
  if any(not r.get('db_cpu_usage_usec') or not r.get('memory_current_bytes') for _,r in inside):return None
  first,last=selected[0],selected[-1]
  elapsed=float(last['monotonic'])-float(first['monotonic'])
  if elapsed<=0:return None
  cores=(float(last['db_cpu_usage_usec'])-float(first['db_cpu_usage_usec']))/1e6/elapsed
  capacity=float(last['allocated_cpu_capacity_cores'])
  if capacity<=0:return None
  return cores,cores/capacity*100
 except (OSError,KeyError,ValueError,TypeError,OverflowError):return None

def _tidb_reclassifiable(summary,attempt_dir):
 resource=summary.get('resource_summary') or {};containers=resource.get('container_resource_summaries') or {}
 tidb=containers.get('res-tidb') or {};stores=[v for k,v in containers.items() if k.startswith('res-tikv')]
 pd=containers.get('res-pd')
 missing=tidb.get('missing_intervals')
 only_missing=bool(missing) and all(isinstance(x,dict) and x.get('reason')=='required cumulative counter/memory sample missing' for x in missing)
 return (bool(stores) and all(x.get('formal') is True for x in stores) and pd is not None and pd.get('formal') is True
         and tidb.get('formal') is False and only_missing and tidb_full_window_cpu(attempt_dir,summary) is not None)

def is_ok(db,summary,cleanup,exit_code,users=USERS,requests_per_user=REQS,queues=QUEUES,connections=None,batch=None,attempt_dir=None):
 if not summary or exit_code!=0 or cleanup.get('status')!='success' or summary.get('verification',{}).get('valid') is not True:return False
 e=summary.get('effective') or {}; expected={'users':users,'requests_per_user':requests_per_user,'queues':queues,'batch_max':batch}
 if connections is not None:expected['connections']=connections
 if any(e.get(k)!=v for k,v in expected.items()) or summary.get('requests')!=users*requests_per_user:return False
 m=bs.extract_row(db,summary)
 if m.get('flush_missing'):return False
 formal=(summary.get('resource_window_formal') if db=='pg' else (summary.get('resource_summary') or {}).get('resource_window_formal'))
 if formal is not True and not (db=='tidb' and attempt_dir is not None and _tidb_reclassifiable(summary,attempt_dir)):return False
 if db=='tidb' and summary.get('durability',{}).get('valid') is not True:return False
 return True
def append(db,c,b,row):
 p=csv_path(db,c,b);p.parent.mkdir(parents=True,exist_ok=True)
 with p.open('a',newline='') as f:
  w=csv.DictWriter(f,fieldnames=COLUMNS)
  if p.stat().st_size==0:w.writeheader()
  w.writerow({k:row.get(k,'') for k in COLUMNS})
def attempt(db,c,b,n,role,dry=False):
 k=next_index(db,c,b,n);rel=attempt_name(db,c,b,n,k);argv=wrapper_argv(db,c,b,n,rel)
 if dry:print('argv: '+json.dumps(argv));return None
 directory=ROOT/rel
 if directory.exists():raise RuntimeError(f'attempt directory exists: {rel}')
 ART.mkdir(parents=True,exist_ok=True)
 gate=bs.idle_gate();(ROOT/(rel+'-idle.json')).write_text(json.dumps(gate,indent=2)+'\n')
 if gate.get('status')!='ok':
  row={'conns':n,'role':role,'attempt_dir':rel,'status':'gate_failed','go_reached':'false','cause':'idle gate'};append(db,c,b,row);return row
 start=time.monotonic();proc=subprocess.run(argv,cwd=ROOT);wall=time.monotonic()-start
 cleanup=json.loads((directory/'cleanup.json').read_text()) if (directory/'cleanup.json').exists() else {}
 try:s=json.loads((directory/'summary.json').read_text())
 except (OSError,ValueError):s={}
 good=is_ok(db,s,cleanup,proc.returncode,connections=n,batch=b,attempt_dir=directory)
 row={'conns':n,'role':role,'attempt_dir':rel,'status':'ok' if good else 'incomplete','exit':proc.returncode,'go_reached':str((directory/'GO').exists() or (directory/'client/result.json').exists()).lower(),'cleanup':cleanup.get('status','missing'),'wall_s':wall,'cause':'' if good else bs.classify_cause((directory/'failure.txt').read_text(errors='replace') if (directory/'failure.txt').exists() else '',cleanup=cleanup.get('status',''))}
 if s:
  row.update(bs.extract_row(db,s));row['client_cpu_cores']=_summary_client(db,s)
  if db=='tidb' and not (s.get('resource_summary') or {}).get('resource_window_formal') and good:
   row['tidb_cpu_cores']=tidb_full_window_cpu(directory,s)[0];row['cause']=TIDB_RECLASSIFY_NOTE
 append(db,c,b,row)
 if cleanup.get('status')!='success':raise RuntimeError('cleanup residue/failure; campaign stopped')
 return row
def run(db,c,b,dry=False):
 grid=GRIDS[c];rs=rows_for(db,c,b)
 for n in grid:
  prim=[r for r in rs if int(r.get('conns',0) or 0)==n and r.get('role')=='primary' and r.get('status')=='ok']
  rer=[r for r in rs if int(r.get('conns',0) or 0)==n and r.get('role')=='flush_rerun']
  if prim:
   if bs.should_flush_rerun(prim[-1],len(rer)):
    result=attempt(db,c,b,n,'flush_rerun',dry)
    if result:rs.append(result)
   continue
  result=attempt(db,c,b,n,'primary',dry)
  for retry in range(2):
   if result and result.get('cause')=='sampler' and result.get('status')!='ok':result=attempt(db,c,b,n,'primary',dry)
   else:break
  if result and result.get('status')=='ok' and bs.should_flush_rerun(result):attempt(db,c,b,n,'flush_rerun',dry)
  if result:rs.append(result)
def replicate(db,c,b,dry=False):
 rs=rows_for(db,c,b); points=replicate_points(rs,GRIDS[c])
 for n in points:
  if any(int(r.get('conns',0) or 0)==n and r.get('role')=='replicate' for r in rs):continue
  attempt(db,c,b,n,'replicate',dry)
def reclassify(db,c,b):
 rows=rows_for(db,c,b);existing_ok={r.get('attempt_dir') for r in rows if r.get('status')=='ok'};added=[]
 for old in rows:
  rel=old.get('attempt_dir','');directory=ROOT/rel
  if old.get('status')=='ok' or rel in existing_ok or not directory.is_dir():continue
  sp=directory/'summary.json';cp=directory/'cleanup.json';rp=directory/'client'/'result.json'
  if not (sp.is_file() and cp.is_file() and rp.is_file()):continue
  try:summary=json.loads(sp.read_text());cleanup=json.loads(cp.read_text())
  except (OSError,ValueError):continue
  if cleanup.get('status')!='success':continue
  try:connections=int(old['conns'])
  except (KeyError,ValueError):continue
  if not is_ok(db,summary,cleanup,0,connections=connections,batch=b,attempt_dir=directory):continue
  row={'conns':connections,'role':old.get('role',''),'attempt_dir':rel,'status':'ok','exit':0,'go_reached':'true','cleanup':'success','cause':TIDB_RECLASSIFY_NOTE}
  row.update(bs.extract_row(db,summary));row['client_cpu_cores']=_summary_client(db,summary)
  cpu=tidb_full_window_cpu(directory,summary)
  if cpu is None:continue
  row['tidb_cpu_cores']=cpu[0]
  append(db,c,b,row);added.append(row);existing_ok.add(rel)
 print(f'reclassified {db} c{c} batch {b}: appended {len(added)} ok row(s)')
 for row in added:print(f"  conns={row['conns']} role={row['role']} attempt_dir={row['attempt_dir']} tidb_cpu_cores={row['tidb_cpu_cores']} rps_request={row.get('rps_request')}")
 return added

def audit():
 print('db cpus batch conns status')
 totals={'ok':0,'failed':0,'flush-flag':0,'missing':0}
 for db in ('pg','tidb'):
  for c in (4,8):
   for b in BATCHES:
    rows=rows_for(db,c,b);eff=effective_primary(rows)
    for n in GRIDS[c]:
     if n not in eff: status='failed' if any(int(r.get('conns',0) or 0)==n for r in rows) else 'missing'
     elif eff[n].get('cause')=='flush>2ms, rerun failed': status='flush-flag'
     else: status='ok'
     totals[status]+=1
     print(db,c,b,n,status)
 print('totals: '+' '.join(f'{k}={v}' for k,v in totals.items())+f" configurations={sum(totals.values())}")
def summarize(db,c,b):
 rows=rows_for(db,c,b);print(db,c,b,saturation(rows,GRIDS[c]));print(json.dumps(effective_primary(rows),indent=2))
def main(argv=None):
 p=argparse.ArgumentParser();s=p.add_subparsers(dest='cmd',required=True)
 for name in ('run','replicate','summarize','reclassify'):
  x=s.add_parser(name);x.add_argument('--db',choices=('pg','tidb'),required=True);x.add_argument('--cpus',type=int,choices=(4,8),required=True);x.add_argument('--batch',type=int,choices=BATCHES,required=True)
  if name!='summarize':x.add_argument('--dry-run',action='store_true')
 x=s.add_parser('all');x.add_argument('--db',choices=('pg','tidb'),required=True);x.add_argument('--cpus',type=int,choices=(4,8),required=True);x.add_argument('--dry-run',action='store_true')
 s.add_parser('audit');a=p.parse_args(argv)
 if a.cmd=='audit':audit();return 0
 if a.cmd=='summarize':summarize(a.db,a.cpus,a.batch)
 elif a.cmd=='reclassify':reclassify(a.db,a.cpus,a.batch)
 elif a.cmd=='run':run(a.db,a.cpus,a.batch,a.dry_run)
 elif a.cmd=='replicate':replicate(a.db,a.cpus,a.batch,a.dry_run)
 else:
  for b in BATCHES:
   run(a.db,a.cpus,b,a.dry_run)
   if not a.dry_run:replicate(a.db,a.cpus,b,False)
 if getattr(a,'dry_run',False):print('replicates are decided after the full grid')
 return 0
if __name__=='__main__':sys.exit(main())
