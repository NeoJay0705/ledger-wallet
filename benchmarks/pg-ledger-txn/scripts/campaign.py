#!/usr/bin/env python3
"""One-shot paired benchmark campaign controller. No scores without run-level evidence."""
import argparse, csv, hashlib, json, math, os, pathlib, shutil, statistics, subprocess, sys, time, signal
ROOT=pathlib.Path(__file__).resolve().parents[1]
from endpoints import PG_PORT, SCYLLA_PORT
BACKENDS=('pg','scylla')
_CHILD=None

def next_k(rps,target,users,safety=1.15):
    if rps<=0 or target<=0 or users<1 or safety<1: raise ValueError('positive RPS/target/users and safety >= 1 required')
    return max(1,math.ceil(rps*target*safety/users))
def median_improvement(a,b): return (b-a)/a
def candidate_n(medians,threshold=.05):
    ns=sorted(medians)
    if len(ns)<3:return None
    a,b,c=ns[-3:]
    return c if b==2*a and c==2*b and median_improvement(medians[a],medians[b])<threshold and median_improvement(medians[b],medians[c])<threshold else None
def plateau_candidate(medians,threshold=.05): return candidate_n(medians,threshold) is not None

def run_accepted(r,min_seconds,n,k,resource_coverage=.8):
    why=[]; result=r.get('result') or {}; total=n*k
    if r.get('returncode')!=0: why.append('wrapper_failed')
    if float(result.get('elapsed_ms',0))/1000<min_seconds: why.append('too_short')
    if result.get('completed')!=total or result.get('expected_total')!=total: why.append('client_counts')
    if (result.get('metrics') or {}).get('end_to_end',{}).get('count')!=result.get('completed'): why.append('histogram_population')
    v=result.get('validation') or {}
    if any(v.get(a)!=z for a,z in [('accounts',n),('expected_accounts',n),('ledger_rows',total),('expected_rows',total),('amount_sum',total),('expected_sum',total)]): why.append('client_validation')
    q=r.get('verification') or {}
    if not q.get('valid') or any(q.get(a)!=z for a,z in [('N',n),('K',k),('expected_total',total)]): why.append('independent_verification')
    resource=r.get('resource') or {}
    if not resource.get('formal') or resource.get('coverage',0)<resource_coverage: why.append('resource_window')
    if (r.get('sampler_errors') or {}).get('errors')!=[]: why.append('sampler_errors')
    s=r.get('sockets') or {}
    if s.get('stable_samples',0)<3 or any(s.get(a)!=s.get(b) for a,b in [('expected_data','observed_data'),('expected_control','observed_control'),('expected_total','observed_total')]): why.append('socket_counts')
    if (r.get('cleanup') or {}).get('status')!='success': why.append('cleanup')
    return not why,why

def run_accepted_calibration(r,n,k):
    # Structural evidence only; calibration duration/coverage have relaxed thresholds.
    c=dict(r); c['resource']={**(r.get('resource') or {}),'formal':True,'coverage':1}
    ok,why=run_accepted(c,0,n,k)
    result=r.get('result') or {}; res=r.get('resource') or {}
    if float(result.get('elapsed_ms',0))/1000<3: why.append('calibration_too_short')
    if not res.get('window_duration_seconds') or res.get('coverage',0)<.5: why.append('calibration_resource_coverage')
    return not why,why

def write_json(p,obj):
    p.parent.mkdir(parents=True,exist_ok=True); t=p.with_suffix(p.suffix+'.tmp'); t.write_text(json.dumps(obj,indent=2,sort_keys=True)+'\n'); t.replace(p)
def load_run(path):
    def j(name,default=None):
        try:return json.loads((path/name).read_text())
        except Exception:return default
    result=j('client/result.json',{}) or {}; res=j('resource-summary.json',{}) or {}; sockets=j('socket-snapshots.json',{}) or {}; obs=sockets.get('observations',[]); last=obs[-1].get('counts_by_remote_port',{}) if obs else {}; backend=(j('run.json',{}) or {}).get('backend')
    data=last.get(str(PG_PORT),0) if backend=='pg' else last.get(str(SCYLLA_PORT),0)-sockets.get('expected_control',0)
    sampler=j('sampler-errors.json',None)
    if isinstance(sampler,dict): sampler=sampler.get('errors',['invalid_sampler_errors_format'])
    if not isinstance(sampler,list): sampler=['missing_or_invalid_sampler_errors_file']
    control=last.get(str(SCYLLA_PORT),0)-data if backend=='scylla' else 0
    return {'result':result,'verification':j('verification-status.json',{}),'resource':{**res,'formal':bool(res.get('formal')),'coverage':res.get('cpu_interval_coverage_seconds',0)/res.get('window_duration_seconds',1) if res.get('window_duration_seconds') else 0},'sockets':{**sockets,'observed_data':data,'observed_control':control,'observed_total':last.get(str(PG_PORT),0) if backend=='pg' else last.get(str(SCYLLA_PORT),0)},'sampler_errors':{'errors':sampler},'cleanup':j('cleanup.json',{}),'artifacts':str(path)}
def metadata():
    binary=ROOT/'target/release/ledger-bench'
    sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
    return {'binary_sha256':sha(binary) if binary.exists() else None,'cargo_lock_sha256':sha(ROOT/'Cargo.lock'),'controller_sha256':sha(pathlib.Path(__file__)),'wrapper_sha256':sha(ROOT/'scripts/single_run.py'),'monitor_sha256':sha(ROOT/'scripts/monitor.py'),'scylla_config_sha256':sha(ROOT/'config/scylla.yaml'),'platform':sys.platform,'cpu_count':os.cpu_count(),'hostname':os.uname().nodename}
def validated_rps(levels,backend):
    valid=sorted((int(n),l) for n,l in levels.items() if l.get('valid') and backend in l.get('medians_rps',{}))
    return float(valid[-1][1]['medians_rps'][backend]) if valid else 500.
def guard(a,out,started):
    reasons=[]; usage=shutil.disk_usage(out)
    try: mem=int(next(x.split()[1] for x in pathlib.Path('/proc/meminfo').read_text().splitlines() if x.startswith('MemAvailable:')))*1024
    except Exception:mem=0
    if mem and mem<a.min_free_memory_gb*1024**3: reasons.append('free_host_memory_below_guard')
    if usage.free<a.min_free_disk_gb*1024**3: reasons.append('free_disk_below_guard')
    if a.max_wall_seconds and time.monotonic()-started>=a.max_wall_seconds: reasons.append('wall_timeout')
    return reasons

def invoke(cmd):
    global _CHILD
    _CHILD=subprocess.Popen(cmd,cwd=ROOT)
    try:return _CHILD.wait()
    finally:_CHILD=None

def signal_handler(signum,frame):
    child=_CHILD
    if child and child.poll() is None:
        try: child.send_signal(signum)
        except ProcessLookupError: pass
        child.wait()
    raise SystemExit(128+signum)

def run_level(n,k,reps,run_fn,accept_fn,min_seconds=30):
    """Run paired order; one short result invalidates/retries the complete level once."""
    attempts=[]
    def pass_at(shared_k):
        rows=[]
        for rep in range(1,reps+1):
            order=BACKENDS if rep%2 else tuple(reversed(BACKENDS))
            for b in order:
                r=run_fn(b,n,shared_k,rep); r.update(backend=b,N=n,K=shared_k,repeat=rep)
                ok,why=accept_fn(r,n,shared_k); r.update(accepted=ok,rejection_reasons=why); rows.append(r)
                if not ok: return rows
        return rows
    rows=pass_at(k); attempts.extend(rows)
    if any('too_short' in x['rejection_reasons'] for x in rows):
        durations=[float(x.get('result',{}).get('elapsed_ms',0))/1000 for x in rows if float(x.get('result',{}).get('elapsed_ms',0))>0]
        shortest=min(durations,default=0); larger=max(k+1,math.ceil(k*(min_seconds/max(shortest,.1))*1.2))
        retry=pass_at(larger); attempts.extend(retry)
        return {'N':n,'K':larger,'retry_of_K':k,'repetitions':retry,'valid':len(retry)==reps*2 and all(x['accepted'] for x in retry),'attempts':attempts},attempts
    level={'N':n,'K':k,'repetitions':rows,'valid':len(rows)==reps*2 and all(x['accepted'] for x in rows)}
    return level,attempts

def export(out,journal):
    rows=[]
    for n,l in journal.get('levels',{}).items():
        if not l.get('valid'):continue
        for b in BACKENDS:
            xs=[x for x in l.get('repetitions',[]) if x.get('backend')==b and x.get('accepted')]
            if len(xs)!=3:continue
            rs=[x['result'] for x in xs]; lat=[r.get('metrics',{}).get('end_to_end',{}) for r in rs]
            rows.append({'N':n,'K':l['K'],'backend':b,'valid':True,'median_rps':statistics.median(r['business_rps'] for r in rs),'min_rps':min(r['business_rps'] for r in rs),'max_rps':max(r['business_rps'] for r in rs),'p50_us_median':statistics.median(x['p50_us'] for x in lat),'p95_us_median':statistics.median(x['p95_us'] for x in lat),'p99_us_median':statistics.median(x['p99_us'] for x in lat),'histogram_population_min':min(x['count'] for x in lat),'db_cpu_core_equivalents_mean':statistics.median(x.get('resource',{}).get('cpu_core_equivalents_mean') or 0 for x in xs),'db_memory_current_bytes_max':max(x.get('resource',{}).get('db_memory_current_bytes_max') or 0 for x in xs),'client_cpu_core_equivalents_mean':statistics.median(x.get('resource',{}).get('client_cpu_core_equivalents_mean') or 0 for x in xs),'client_rss_bytes_max':max(x.get('resource',{}).get('client_rss_bytes_max') or 0 for x in xs),'resource_coverage_min':min(x.get('resource',{}).get('coverage') or 0 for x in xs),'run_artifacts':';'.join(x.get('artifacts','') for x in xs)})
    fields=['N','K','backend','valid','median_rps','min_rps','max_rps','p50_us_median','p95_us_median','p99_us_median','histogram_population_min','db_cpu_core_equivalents_mean','db_memory_current_bytes_max','client_cpu_core_equivalents_mean','client_rss_bytes_max','resource_coverage_min','run_artifacts']
    with (out/'levels.csv').open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=fields); w.writeheader(); w.writerows(rows)
    med={b:{int(x['N']):float(x['median_rps']) for x in rows if x['backend']==b} for b in BACKENDS}
    journal['candidate_N']={b:candidate_n(med[b]) for b in BACKENDS}; journal['candidate_flags']={b:journal['candidate_N'][b] is not None for b in BACKENDS}; journal['diagnostics_required']=True; journal['saturation_confirmed']=False
    write_json(out/'campaign.json',{k:v for k,v in journal.items() if k!='_config'}); write_json(out/'journal.json',journal)

def main():
    ap=argparse.ArgumentParser(description=__doc__); ap.add_argument('--output',type=pathlib.Path,default=ROOT/'artifacts/gate3c-campaign'); ap.add_argument('--target-seconds',type=float,default=35); ap.add_argument('--min-seconds',type=float,default=30); ap.add_argument('--repetitions',type=int,default=3); ap.add_argument('--max-users',type=int,default=4096); ap.add_argument('--max-wall-seconds',type=float); ap.add_argument('--min-free-memory-gb',type=float,default=2); ap.add_argument('--min-free-disk-gb',type=float,default=10); ap.add_argument('--start-users',type=int,default=1); ap.add_argument('--warmup-requests-per-user',type=int,default=20); ap.add_argument('--smoke',action='store_true',help='explicitly mark this reduced-duration campaign non-formal'); a=ap.parse_args()
    if min(a.target_seconds,a.min_seconds,a.repetitions,a.max_users,a.start_users,a.warmup_requests_per_user)<=0 or a.start_users&(a.start_users-1) or a.start_users>a.max_users or a.repetitions!=3: ap.error('positive settings required; repetitions must equal 3 and start-users be a power of two <= max-users')
    if (a.min_seconds<30 or a.target_seconds<35) and not a.smoke: ap.error('reduced target/minimum duration requires explicit --smoke')
    out=a.output.resolve(); out.mkdir(parents=True,exist_ok=True)
    if any((out/x).exists() for x in ('journal.json','campaign.json','levels.csv')): ap.error('output already contains journal/previous results; choose a new directory')
    config={'target_seconds':a.target_seconds,'min_seconds':a.min_seconds,'repetitions':a.repetitions,'max_users':a.max_users,'start_users':a.start_users,'warmup_requests_per_user':a.warmup_requests_per_user,'formal':not a.smoke and a.min_seconds>=30 and a.target_seconds>=35,'smoke':a.smoke,'metadata':metadata()}
    j={'config':config,'attempts':[],'levels':{},'calibrations':{},'state':'running'}; started=time.monotonic(); write_json(out/'journal.json',j)
    def run_fn(b,n,k,rep):
        d=out/f'level-N{n}-K{k}'/f'{b}-r{rep}-a{len(j["attempts"])+1}'; rc=invoke([sys.executable,str(ROOT/'scripts/single_run.py'),b,str(n),str(k),str(d),'--warmup-requests-per-user',str(a.warmup_requests_per_user)])
        r=load_run(d) if d.exists() else {'result':{},'cleanup':{},'artifacts':str(d)}; r['returncode']=rc; return r
    try:
        n=a.start_users; prior={b:validated_rps({},b) for b in BACKENDS}
        while n<=a.max_users:
            why=guard(a,out,started)
            if why:j.update(state='stopped_saturation_unconfirmed',stop_reasons=why);break
            calibration={}
            for b in BACKENDS:
                ck=next_k(prior[b],12,n); good=False
                for attempt in range(3):
                    d=out/'calibration'/f'{b}-N{n}-K{ck}-a{attempt+1}'; rc=invoke([sys.executable,str(ROOT/'scripts/single_run.py'),b,str(n),str(ck),str(d),'--warmup-requests-per-user',str(a.warmup_requests_per_user)])
                    r=load_run(d) if d.exists() else {'result':{},'cleanup':{},'artifacts':str(d)}; r['returncode']=rc
                    ok,rs=run_accepted_calibration(r,n,ck); rate=float((r.get('result') or {}).get('business_rps') or 0)
                    calibration.setdefault(b,[]).append({'K':ck,'accepted':ok,'rejection_reasons':rs,'business_rps':rate,'returncode':rc,'directory':str(d)})
                    j['calibrations'][str(n)]=calibration; write_json(out/'journal.json',j)
                    if ok and rate>0:prior[b]=rate;good=True;break
                    if not (rs and all(x in ('calibration_too_short','calibration_resource_coverage') for x in rs)):break
                    ck=max(ck+1,math.ceil(ck*1.5))
                if not good:j.update(state='stopped_calibration_invalid',stop_reasons=[f'{b}_calibration_invalid']);break
            if j['state']!='running':break
            k=next_k(max(prior.values()),a.target_seconds,n)
            level,attempts=run_level(n,k,3,run_fn,lambda r,nn,kk:run_accepted(r,a.min_seconds,nn,kk),a.min_seconds); j['attempts'].extend(attempts); level['calibration']=calibration; j['levels'][str(n)]=level; write_json(out/'journal.json',j)
            if not level['valid']:j.update(state='stopped_invalid_level');break
            med={b:statistics.median(x['result']['business_rps'] for x in level['repetitions'] if x['backend']==b) for b in BACKENDS};level['medians_rps']=med
            current={b:{int(nn):ll['medians_rps'][b] for nn,ll in j['levels'].items() if ll.get('valid') and b in ll.get('medians_rps',{})} for b in BACKENDS}
            if all(candidate_n(current[b]) for b in BACKENDS):j.update(state='candidates_require_diagnostics');break
            if n==a.max_users:j.update(state='stopped_saturation_unconfirmed',stop_reasons=['max_users_limit']);break
            n*=2
    except BaseException as e:
        if isinstance(e,SystemExit) and e.code==130: j.update(state='interrupted',stop_reasons=['SIGINT'])
        elif isinstance(e,SystemExit) and e.code==143:j.update(state='interrupted',stop_reasons=['SIGTERM'])
        else:j.update(state='failed',stop_reasons=[f'{type(e).__name__}: {e}'])
        export(out,j)
        if isinstance(e,SystemExit):raise
        return 2
    export(out,j)
    return 2 if j['state'] in ('stopped_invalid_level','stopped_calibration_invalid','stopped_saturation_unconfirmed','interrupted','failed') else 0

if __name__=='__main__':
    signal.signal(signal.SIGINT,signal_handler); signal.signal(signal.SIGTERM,signal_handler)
    sys.exit(main())
