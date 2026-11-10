#!/usr/bin/env python3
"""Serial, resumable batch sweep driver. Standard-library only."""
import argparse, csv, json, os, pathlib, subprocess, sys, time
ROOT=pathlib.Path(__file__).resolve().parents[1]; ART=ROOT/'artifacts'
GRID=[50,100,250,500,1000,2000,4000,8000]
COLUMNS=['batch','role','attempt_dir','status','exit','go_reached','cleanup','wall_s','duration_s','rps_request','p50_s','p95_s','p99_s','max_s','windows','db_cpu_cores','tidb_cpu_cores','flush_await_ms','imported','cause']

def evaluate(cpu_idle, proc_cpu, min_idle=90, max_proc=30):
    reasons=[]
    for cpu,v in cpu_idle.items():
        if v is None or v<min_idle: reasons.append(f'cpu{cpu} idle {v} < {min_idle}')
    for proc,v in proc_cpu.items():
        if v is not None and v>max_proc: reasons.append(f'{proc} CPU {v} > {max_proc}')
    return not reasons,reasons

def _path(obj,*keys):
    for key in keys:
        if not isinstance(obj,dict) or key not in obj:return None
        obj=obj[key]
    return obj

def extract_row(db, summary_or_composite):
    if 'summary' in summary_or_composite and 'resource' in summary_or_composite:
        s=summary_or_composite['summary']; r=summary_or_composite['resource']
    else:s=summary_or_composite;r=s.get('resource_summary') or {}
    lat=s.get('latency_us') or {}; out={'duration_s':s.get('duration_s'),'rps_request':s.get('rps_request'),
      'p50_s':_sec(lat.get('p50')),'p95_s':_sec(lat.get('p95')),'p99_s':_sec(lat.get('p99')),'max_s':_sec(lat.get('max')),
      'windows':_path(s,'outcomes','windows'),'db_cpu_cores':None,'tidb_cpu_cores':None,'flush_await_ms':None,'flush_missing':False}
    if db=='pg':out['db_cpu_cores']=_path(r,'means','db_cpu_core_equivalents'); flush=_path(r,'means','device_flush_await_ms')
    elif db=='tidb':out['db_cpu_cores']=r.get('tikv_cpu_cores_mean');out['tidb_cpu_cores']=r.get('tidb_cpu_cores_mean');flush=_path(r,'device_means','device_flush_await_ms')
    else:raise ValueError('db must be pg or tidb')
    out['flush_await_ms']=flush;out['flush_missing']=flush is None
    return out

def _sec(v):return None if v is None else v/1e6
def _num(v):
    try:return float(v) if v not in (None,'') else None
    except (ValueError,TypeError):return None

def saturation_point(primary_rows,frac=0.95):
    rows=[r for r in primary_rows if r.get('role','primary')=='primary' and r.get('status','ok')=='ok' and _num(r.get('rps_request')) is not None]
    if not rows:return None
    top=max(float(r['rps_request']) for r in rows); largest=max(int(r['batch']) for r in primary_rows if r.get('role','primary')=='primary')
    for r in sorted(rows,key=lambda x:int(x['batch'])):
        if float(r['rps_request'])>=frac*top:
            b=int(r['batch']);return {'batch':b,'label':'observed-grid saturation' if b==largest else 'saturation point'}
    return None

def terminal_tidb_failure(rows):return any(r.get('role')=='primary' and r.get('status') not in ('ok','') and str(r.get('go_reached')).lower()=='true' and r.get('cause')!='sampler' for r in rows)
def allows_batch(db,batch,rows):return db!='tidb' or batch!=8000 or any(int(r.get('batch',0) or 0)==6000 and r.get('role')=='primary' and r.get('status')=='ok' for r in rows)
def should_flush_rerun(row,prior_reruns=0):return prior_reruns<1 and _num(row.get('flush_await_ms')) is not None and float(row['flush_await_ms'])>2

def replicate_points(primary_rows,grid=None):
    grid=sorted(grid or GRID);sat=saturation_point(primary_rows)
    if not sat:return []
    b=sat['batch'];i=grid.index(b);return [b,grid[i-1] if i else grid[i+1]] if len(grid)>1 else [b]
def attempt_name(db,cpus,batch,index):return f'artifacts/sweep-{db}-c{cpus}-bm{batch}-a{index}'
def classify_cause(failure='',invalid='',cleanup=''):
    text=(failure+' '+invalid).lower()
    if cleanup and cleanup!='success':return 'cleanup residue'
    if '8176' in text or 'memory' in text or 'oom' in text:return 'memory (8176)'
    if 'timeout' in text:return 'timeout'
    if 'sampler' in text:return 'sampler'
    return 'other'
def wrapper_argv(db,cpus,batch,directory):
    cmd=['python3','scripts/batch_queue_run.py' if db=='pg' else 'scripts/txn_tidb_run.py',directory]
    cmd+=['--mode','txn'] if db=='pg' else []
    cmd+=['--cpus',str(cpus),'--users','100000','--requests-per-user','40','--queues','256','--batch-max',str(batch),'--sample-interval-ms','250']
    if db=='pg':cmd+=['--pg-conf','wal_buffers=256MB','--pg-conf','max_wal_size=8GB','--pg-conf','track_wal_io_timing=on']
    return cmd

def _proc_stat(pid):
    try:
        raw=pathlib.Path(f'/proc/{pid}/stat').read_text(); tail=raw[raw.rfind(')')+2:].split()
        comm=raw[raw.find('(')+1:raw.rfind(')')]
        return int(tail[11])+int(tail[12]),int(tail[1]),comm
    except (OSError,ValueError,IndexError):return None

def _cpu_stat():
    out={}
    for line in pathlib.Path('/proc/stat').read_text().splitlines():
        f=line.split()
        if f and f[0].startswith('cpu') and f[0]!='cpu' and f[0][3:].isdigit():
            nums=list(map(int,f[1:]));out[int(f[0][3:])]=(sum(nums),nums[3]+(nums[4] if len(nums)>4 else 0))
    return out

def _descendants():
    parents={};
    for p in pathlib.Path('/proc').iterdir():
        if p.name.isdigit():
            s=_proc_stat(p.name)
            if s:parents[int(p.name)]=s[1]
    excluded={os.getpid()};changed=True
    while changed:
        changed=False
        for pid,ppid in parents.items():
            if ppid in excluded and pid not in excluded:excluded.add(pid);changed=True
    return excluded

def _sample():
    cpus=_cpu_stat();procs={};excluded=_descendants()
    for p in pathlib.Path('/proc').iterdir():
        if p.name.isdigit() and int(p.name) not in excluded:
            s=_proc_stat(p.name)
            if s:procs[int(p.name)]=(s[0],s[2])
    return cpus,procs

def idle_gate(interval=5,timeout=60):
    started=time.monotonic();evidence={'thresholds':{'min_idle':90,'max_proc':30},'attempts':[],'status':'gate_failed'}
    while time.monotonic()-started<=timeout:
        a,pa=_sample();time.sleep(interval);b,pb=_sample();elapsed=interval
        idle={}
        for c,(tot,idle0) in a.items():
            if c in b:
                dt=b[c][0]-tot;di=b[c][1]-idle0;idle[str(c)]=100*di/dt if dt>0 else None
        ticks=os.sysconf('SC_CLK_TCK');procs={}
        for pid,(ticks0,name) in pa.items():
            if pid in pb:
                delta=pb[pid][0]-ticks0
                if delta>=0:procs[f'{pid}:{name}']=100*delta/ticks/elapsed
        ok,reasons=evaluate(idle,procs);top=sorted(procs.items(),key=lambda x:x[1],reverse=True)[:5]
        item={'cpu_idle_pct':idle,'top_process_cpu_pct':[{ 'process':k,'cpu_pct':v} for k,v in top],'ok':ok,'reasons':reasons,'interval_s':elapsed}
        evidence['attempts'].append(item)
        if ok:evidence['status']='ok';break
    evidence.update(evidence['attempts'][-1]);evidence['status']='ok' if evidence['attempts'][-1]['ok'] else 'gate_failed'
    return evidence

def _csv_path(db,c):return ART/f'sweep-{db}-c{c}.csv'
def read_rows(db,c):
    p=_csv_path(db,c)
    if not p.exists():return []
    with p.open(newline='') as f:return list(csv.DictReader(f))
def append_row(db,c,row):
    p=_csv_path(db,c);p.parent.mkdir(exist_ok=True)
    with p.open('a',newline='') as f:
        w=csv.DictWriter(f,fieldnames=COLUMNS); 
        if p.stat().st_size==0:w.writeheader()
        w.writerow({k:row.get(k) for k in COLUMNS})
def _imports():return json.loads((ART/'sweep-imports.json').read_text())
def _import_rows(db,c,rows,write=True):
    existing={int(r['batch']) for r in rows}
    for item in _imports()['imports']:
        if item['db']!=db or int(item['cpus'])!=c or int(item['batch']) in existing:continue
        s=json.loads((ROOT/item['source']).read_text())
        if 'resource_source' in item:r=json.loads((ROOT/item['resource_source']).read_text());metrics=extract_row(db,{'summary':s,'resource':r})
        else:metrics=extract_row(db,s)
        row=dict(batch=item['batch'],role='primary',attempt_dir=item['source'],status='ok' if not metrics['flush_missing'] and s.get('verification',{}).get('valid') else 'incomplete',exit=0,go_reached='true',cleanup='unavailable (historical, approved)',wall_s='',imported='true',cause='',**metrics)
        row['idle_evidence']='unavailable (historical, approved)'
        if write:append_row(db,c,row)
        rows.append({k:str(v) for k,v in row.items()});existing.add(int(item['batch']))
        print(f"import {db} c{c} batch {item['batch']}: {item['source']}")

def _next_index(db,c,batch):
    prefix=f'sweep-{db}-c{c}-bm{batch}-a'; indices=[]
    for p in ART.glob(prefix+'*'):
        try:indices.append(int(p.name.rsplit('-a',1)[1].split('-idle',1)[0]))
        except (ValueError,IndexError):pass
    for row in read_rows(db,c):
        if int(row.get('batch',0) or 0)==batch:
            name=pathlib.Path(row.get('attempt_dir','')).name
            if name.startswith(prefix):
                try:indices.append(int(name.rsplit('-a',1)[1]))
                except (ValueError,IndexError):pass
    return max(indices,default=0)+1

def _cause(path):
    failure=(path/'failure.txt').read_text(errors='replace').lower() if (path/'failure.txt').exists() else ''
    inv=path/'client/invalid.json';failure += (' '+inv.read_text(errors='replace').lower()) if inv.exists() else ''
    if '8176' in failure or 'memory' in failure or 'oom' in failure:return 'memory (8176)'
    if 'timeout' in failure:return 'timeout'
    if 'sampler' in failure:return 'sampler'
    return 'other'

def _run_one(db,c,batch,role,rows,dry):
    idx=_next_index(db,c,batch);rel=attempt_name(db,c,batch,idx);argv=wrapper_argv(db,c,batch,rel)
    if dry:print('argv: '+json.dumps(argv));return
    if pathlib.Path(ROOT/rel).exists():raise RuntimeError(f'attempt directory already exists: {rel}')
    gate=idle_gate();(ROOT/(rel+'-idle.json')).write_text(json.dumps(gate,indent=2)+'\n')
    if gate['status']!='ok':
        row={'batch':batch,'role':role,'attempt_dir':rel,'status':'gate_failed','exit':'','go_reached':'false','cleanup':'','wall_s':'','imported':'false','cause':'idle gate'};append_row(db,c,row);return
    t=time.monotonic();proc=subprocess.run(argv,cwd=ROOT);wall=time.monotonic()-t;p=ROOT/rel
    cleanup=json.loads((p/'cleanup.json').read_text()) if (p/'cleanup.json').exists() else {};summary=None
    try:summary=json.loads((p/'summary.json').read_text())
    except (OSError,ValueError):pass
    valid=bool(summary and summary.get('verification',{}).get('valid') is True)
    metrics=extract_row(db,summary) if summary else {}
    good=proc.returncode==0 and cleanup.get('status')=='success' and valid and not metrics.get('flush_missing')
    go=(p/'GO').exists() or (p/'client/result.json').exists()
    row=dict(batch=batch,role=role,attempt_dir=rel,status='ok' if good else 'incomplete',exit=proc.returncode,go_reached=str(go).lower(),cleanup=cleanup.get('status','missing'),wall_s=wall,imported='false',cause='' if good else classify_cause((p/'failure.txt').read_text(errors='replace') if (p/'failure.txt').exists() else '', (p/'client/invalid.json').read_text(errors='replace') if (p/'client/invalid.json').exists() else '', cleanup.get('status','')))
    if good:row.update(metrics)
    append_row(db,c,row);rows.append({k:str(v) for k,v in row.items()})
    if cleanup.get('status')!='success':raise RuntimeError('cleanup residue/failure; campaign stopped')
    return row

def run_series(db,c,batches,dry):
    rows=read_rows(db,c)
    _import_rows(db,c,rows,write=not dry)
    terminal=db=='tidb' and terminal_tidb_failure(rows)
    for b in sorted(set(batches)):
        if terminal:print(f'skip batch {b}: TiDB terminal post-GO failure persisted');continue
        if not allows_batch(db,b,rows):print(f'skip batch {b}: TiDB requires valid 6000');continue
        prior=[r for r in rows if int(r.get('batch',0) or 0)==b and r.get('role')=='primary' and r.get('status')=='ok']
        reruns=[r for r in rows if int(r.get('batch',0) or 0)==b and r.get('role')=='flush_rerun']
        if prior:
            if should_flush_rerun(prior[0],len(reruns)):
                print(f'flush rerun batch {b}');_run_one(db,c,b,'flush_rerun',rows,dry)
            else:print(f'skip batch {b}: OK primary exists')
            continue
        print(f'primary batch {b}');row=_run_one(db,c,b,'primary',rows,dry)
        # Known tooling race (monitor.Sampler reading /proc/<client>/io at client exit): not a benchmark result; retry in a fresh attempt dir, at most 2 extra attempts.
        for _ in range(2):
            if row and row.get('status')!='ok' and row.get('cause')=='sampler':
                print(f'sampler race on batch {b}; fresh attempt');row=_run_one(db,c,b,'primary',rows,dry)
        if row and row.get('status')=='ok' and should_flush_rerun(row):
            print(f'flush rerun batch {b}');rr=_run_one(db,c,b,'flush_rerun',rows,dry)
            if rr and rr.get('status')=='ok':
                # First flagged run remains in CSV; rerun has role for primary selection.
                pass
        if db=='tidb' and row and row.get('status')!='ok' and row.get('go_reached')=='true':terminal=True

def summarize(db=None,c=None):
    for d in ('pg','tidb'):
      if db and d!=db:continue
      for cp in (4,8):
        if c and cp!=c:continue
        rows=read_rows(d,cp)
        if not rows:continue
        primary=[r for r in rows if r.get('role') in ('primary','flush_rerun') and r.get('status')=='ok']
        # flush rerun is authoritative for that batch where available.
        for b in {r['batch'] for r in primary}:
            rer=[r for r in primary if r['batch']==b and r['role']=='flush_rerun']
            if rer:primary=[r for r in primary if not (r['batch']==b and r['role']=='primary')]
        sat_rows=[dict(r,role='primary') for r in primary]
        sat_rows += [r for r in rows if r.get('role')=='primary' and r.get('status')!='ok']
        sat=saturation_point(sat_rows);print(f'\n## {d.upper()} c{cp} — saturation: {sat}')
        print('| batch | windows | rps | duration s | p50/p95/p99/max s | DB cores | TiDB cores | flush ms | flags | replicate RPS |')
        print('|---:|---:|---:|---:|---|---:|---:|---:|---|---:|')
        for r in sorted(rows,key=lambda x:(int(x['batch']),x.get('role',''))):
            reps=[x['rps_request'] for x in rows if x.get('batch')==r['batch'] and x.get('role')=='replicate' and x.get('status')=='ok']
            flags=','.join(x for x,yes in [('imported',r.get('imported')=='true'),('flush_rerun',r.get('role')=='flush_rerun'),('incomplete',r.get('status')!='ok')] if yes)
            print(f"| {r['batch']} ({r.get('role')}) | {r.get('windows','')} | {r.get('rps_request','')} | {r.get('duration_s','')} | {r.get('p50_s','')}/{r.get('p95_s','')}/{r.get('p99_s','')}/{r.get('max_s','')} | {r.get('db_cpu_cores','')} | {r.get('tidb_cpu_cores','')} | {r.get('flush_await_ms','')} | {flags} | {','.join(reps)} |")
        for rep in (r for r in rows if r.get('role')=='replicate' and r.get('status')=='ok'):
            base=next((x for x in primary if x.get('batch')==rep.get('batch')),None)
            if base and _num(base.get('rps_request')):
                delta=(float(rep['rps_request'])/float(base['rps_request'])-1)*100
                print(f"Stability note batch {rep['batch']}: replicate RPS {delta:+.2f}% vs primary.")

def main(argv=None):
    ap=argparse.ArgumentParser();sub=ap.add_subparsers(dest='cmd',required=True)
    for name in ('run','replicate'):
        p=sub.add_parser(name);p.add_argument('--db',choices=('pg','tidb'),required=True);p.add_argument('--cpus',type=int,choices=(4,8),required=True);p.add_argument('--dry-run',action='store_true')
        if name=='run':p.add_argument('--batches',default=None)
        else:p.add_argument('--batches',required=True)
    p=sub.add_parser('summarize');p.add_argument('--db',choices=('pg','tidb'));p.add_argument('--cpus',type=int,choices=(4,8));p.add_argument('--prefix')
    p=sub.add_parser('idle-probe')
    a=ap.parse_args(argv)
    if a.cmd=='idle-probe':print(json.dumps(idle_gate(),indent=2));return 0
    if a.cmd=='summarize':summarize(a.db,a.cpus);return 0
    batches=([int(x) for x in a.batches.split(',')] if a.batches else (GRID if a.db=='pg' else GRID[:-1]+[6000,8000]))
    if a.cmd=='replicate':
        rows=read_rows(a.db,a.cpus)
        _import_rows(a.db,a.cpus,rows,write=not a.dry_run)
        for b in batches:
            print(f'replicate batch {b}')
            row=_run_one(a.db,a.cpus,b,'replicate',rows,a.dry_run)
            for _ in range(2):
                if row and row.get('status')!='ok' and row.get('cause')=='sampler':
                    print(f'sampler race on replicate {b}; fresh attempt');row=_run_one(a.db,a.cpus,b,'replicate',rows,a.dry_run)
    else:run_series(a.db,a.cpus,batches,a.dry_run)
    return 0
if __name__=='__main__':sys.exit(main())
