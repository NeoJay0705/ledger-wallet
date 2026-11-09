#!/usr/bin/env python3
"""Paired pool/client sensitivity diagnostics; never accepts saturation."""
import argparse, csv, datetime, hashlib, json, pathlib, statistics, subprocess, sys, signal
import campaign
ROOT=pathlib.Path(__file__).resolve().parents[1]
MODES=('baseline','pool','client')
REPETITION_MODES=(MODES,('client','baseline','pool'),('pool','client','baseline'))
PIN_FIELDS=('binary_sha256','cargo_lock_sha256','wrapper_sha256','monitor_sha256','scylla_config_sha256')
SIX_PIN_FIELDS=PIN_FIELDS+('campaign_helper_sha256','resource_profiles_sha256','six_cpu_controller_sha256','diagnostics_sha256')


def median(xs): return statistics.median(xs)
def summarize(rows):
    vals=[float(x['result']['business_rps']) for x in rows]
    summary={'median_rps':median(vals),'min_rps':min(vals),'max_rps':max(vals),'runs':len(vals),'run_paths':[x['artifacts'] for x in rows]}
    for key in ('p50_us','p95_us','p99_us'):
        summary[key+'_median']=median([float(x['result']['metrics']['end_to_end'][key]) for x in rows])
    return summary
def pct_change(base, value): return (value-base)/base*100 if base else None

def source_pins():
    def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
    from six_cpu_campaign import source_pins as six_pins
    return {**six_pins(),'diagnostics_sha256':sha(pathlib.Path(__file__)),'pg_image':__import__('single_run').PG_IMAGE,'scylla_image':__import__('single_run').SC_IMAGE}

def select_formal_level(campaign_data, backend, level=None):
    candidates=campaign_data.get('candidate_N') or {}
    n=level if level is not None else candidates.get(backend)
    if n is None: raise ValueError(f'campaign has no candidate onset for {backend}')
    row=campaign_data.get('levels',{}).get(str(n),{})
    k=row.get('K')
    if k is None: raise ValueError('selected campaign level missing K')
    validate_level(campaign_data,backend,n,k)
    return n,k


def validate_level(campaign_data, backend, n, k):
    if campaign_data.get('state') not in ('candidates_require_diagnostics','completed','accepted'):
        raise ValueError('campaign is incomplete or has no diagnostic-ready state')
    if campaign_data.get('candidate_N',{}).get(backend)!=n or not campaign_data.get('candidate_flags',{}).get(backend):
        raise ValueError(f'campaign has no valid candidate_N for {backend} at N={n}')
    level=campaign_data.get('levels',{}).get(str(n),{})
    if not level.get('valid') or level.get('K')!=k: raise ValueError('selected campaign level is incomplete or N/K does not match')
    return level

def metric_summary(path):
    """Summarize Scylla samples strictly inside client/result.json's measurement window."""
    try:
        result=json.loads((path/'client/result.json').read_text())
        start=result['measurement_start_unix_ns']/1e9
        end=result['measurement_end_unix_ns']/1e9
        # Scylla's per-shard metrics JSON can exceed csv's 128 KiB default.
        csv.field_size_limit(16 * 1024 * 1024)
        with (path/'resources.csv').open(newline='') as f: rows=list(csv.DictReader(f))
    except (OSError,ValueError,KeyError,TypeError): return {}
    from monitor import parse_metrics
    points=[]
    for row in rows:
        try:
            stamp=datetime.datetime.fromisoformat(row['wall_utc']).timestamp()
            if start <= stamp <= end:
                points.append((stamp,{k:float(v) for k,v in json.loads(row.get('scylla_metrics_json') or '{}').items()}))
        except (ValueError,TypeError): continue
    if not points: return {'measurement_window':{'start_unix_ns':result['measurement_start_unix_ns'],'end_unix_ns':result['measurement_end_unix_ns'],'samples':0}}
    points.sort(key=lambda x:x[0]); first,last=points[0],points[-1]
    out={'measurement_window':{'start_unix_ns':result['measurement_start_unix_ns'],'end_unix_ns':result['measurement_end_unix_ns'],'samples':len(points),'first_sample_utc':datetime.datetime.fromtimestamp(first[0],datetime.timezone.utc).isoformat(),'last_sample_utc':datetime.datetime.fromtimestamp(last[0],datetime.timezone.utc).isoformat()}}
    reactors={}
    for _,metrics in points:
        for key,value in metrics.items():
            if 'reactor_utilization' not in key.lower(): continue
            import re
            shard=re.search(r'shard[=\" ]+([^,}\"]+)',key.lower())
            reactors.setdefault(shard.group(1) if shard else key,[]).append(value)
    if reactors: out['reactor_utilization_by_shard']={k:{'mean':statistics.mean(v),'min':min(v),'max':max(v),'samples':len(v)} for k,v in reactors.items()}
    # Counter deltas use first/last in-window observations; decreases indicate resets, never negative work.
    counters={}
    keys=set(first[1])|set(last[1])
    for key in sorted(keys):
        low=key.lower()
        if not any(x in low for x in ('commitlog','coordinator_cas','paxos')) or not any(x in low for x in ('total','count','write','read','cas')): continue
        if key not in first[1] or key not in last[1]: continue
        delta=last[1][key]-first[1][key]
        counters[key]={'delta':delta if delta>=0 else None,'counter_reset':delta<0,'first':first[1][key],'last':last[1][key]}
    if counters: out['counter_deltas']=counters
    return out

def validate_source_pins(campaign_data, current=None, six=False):
    expected=campaign_data.get('source_pins') or (campaign_data.get('config') or {}).get('metadata') or {}
    current=current or source_pins()
    fields=SIX_PIN_FIELDS if six else PIN_FIELDS
    mismatches=[key for key in fields if not expected.get(key) or expected.get(key)!=current.get(key)]
    if mismatches: raise ValueError('campaign source pins mismatch or missing: '+', '.join(mismatches))

def invocation_plan(backends, reps):
    plan=[]
    for rep in range(1,reps+1):
        order=backends if rep%2 else list(reversed(backends))
        for mode in REPETITION_MODES[(rep-1)%len(REPETITION_MODES)]:
            plan.extend((mode,rep,b) for b in order)
    return plan

def run_one(out, backend, n, k, mode, rep, smoke, warmup, cpu_profile='3'):
    d=out/'runs'/mode/f'{backend}-r{rep}'
    six=cpu_profile=='6'
    opts={'baseline':[],'pool':['--pg-data-pool-size','192'] if backend=='pg' else ['--diagnostic-multiplier','2'],
          'client':['--client-cpus','6,7,14,15','--workers','4'] if six else ['--client-cpus','3-7','--workers','5']}[mode]
    cmd=[sys.executable,str(ROOT/'scripts/single_run.py'),backend,str(n),str(k),str(d),'--cpu-profile',cpu_profile,'--warmup-requests-per-user',str(warmup),*opts]
    rc=campaign.invoke(cmd)
    r=campaign.load_run(d) if d.exists() else {'artifacts':str(d),'result':{},'cleanup':{}}
    r.update(returncode=rc,backend=backend,N=n,K=k,mode=mode,repeat=rep)
    if six:
        from six_cpu_campaign import verify_six
        expected_cpus='6-7' if mode!='client' else '6,7,14,15'
        expected_workers=2 if mode!='client' else 4
        try: verify_six(r,expected_cpus,expected_workers)
        except (OSError,ValueError,KeyError,TypeError,AttributeError,json.JSONDecodeError):
            r.update(accepted=False,rejection_reasons=['cpu_profile6'],scylla_metrics=metric_summary(d) if backend=='scylla' else {})
            return r
    ok,why=campaign.run_accepted(r,2 if smoke else 30,n,k,resource_coverage=.5 if smoke else .8)
    # Formal diagnostic measurements use identical N/K and need >=30s; smoke remains explicitly nonformal.
    if not smoke and float((r.get('result') or {}).get('elapsed_ms',0))/1000<30:
        ok=False
        if 'too_short' not in why:why.append('too_short')
    r.update(accepted=ok,rejection_reasons=why,scylla_metrics=metric_summary(d) if backend=='scylla' else {})
    return r

def main():
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--campaign',type=pathlib.Path); ap.add_argument('--level',type=int)
    ap.add_argument('--backend',choices=(*campaign.BACKENDS,'both')); ap.add_argument('--users',type=int); ap.add_argument('--requests-per-user',type=int); ap.add_argument('--cpu-profile',choices=('3','6'),default='3')
    ap.add_argument('--output',type=pathlib.Path,required=True); ap.add_argument('--smoke',action='store_true')
    ap.add_argument('--warmup-requests-per-user',type=int,default=20); a=ap.parse_args()
    if a.smoke:
        if a.campaign or not all((a.backend,a.users,a.requests_per_user)):ap.error('--smoke requires manual --backend/--users/--requests-per-user and no campaign')
        n,k=a.users,a.requests_per_user; backends=list(campaign.BACKENDS) if a.backend=='both' else [a.backend]; reps=1
    else:
        if not a.campaign or a.users is not None or a.requests_per_user is not None:ap.error('formal diagnostics require --campaign; manual users/requests are smoke-only')
        if a.backend is None: ap.error('formal diagnostics require explicit --backend pg|scylla|both')
        data=json.loads(a.campaign.read_text()); reps=3
        backends=list(campaign.BACKENDS) if a.backend=='both' else [a.backend]
        six=data.get('profile')=='6' or data.get('source_pins',{}).get('resource_profiles_sha256') is not None
        if six: a.cpu_profile='6'
        elif a.cpu_profile!='3': ap.error('historical campaign only supports CPU profile 3')
        candidates=data.get('candidate_N') or {}
        try: n,k=select_formal_level(data,backends[0],a.level)
        except ValueError as e: ap.error(str(e))
        try:
            for b in backends:
                validate_level(data,b,n,k)
                if six and candidates.get(b)!=n: raise ValueError('backend onset Ns differ; run diagnostics separately')
            validate_source_pins(data,six=six)
        except ValueError as e:ap.error(str(e))
        formal=data.get('formal',data.get('config',{}).get('formal'))
        if formal is not True:ap.error('campaign must be formal for formal diagnostics')
    if min(n,k,a.warmup_requests_per_user)<1:ap.error('N, K and warmup must be positive')
    out=a.output.resolve()
    if out.exists() and any(out.iterdir()):ap.error('output exists and is not empty; refusing overwrite')
    out.mkdir(parents=True,exist_ok=True)
    runs=[]
    # Interleave sensitivity kinds within each repetition and alternate backend order.
    try:
        for mode,rep,backend in invocation_plan(backends,reps):
            runs.append(run_one(out,backend,n,k,mode,rep,a.smoke,a.warmup_requests_per_user,a.cpu_profile))
            campaign.write_json(out/'diagnostics.partial.json',{'formal':not a.smoke,'N':n,'K':k,'backend':backend,'runs':runs,'source_pins':source_pins()})
            if not runs[-1]['accepted']: break
    except (KeyboardInterrupt,SystemExit):
        campaign.write_json(out/'diagnostics.partial.json',{'formal':not a.smoke,'N':n,'K':k,'backends':backends,'runs':runs,'valid':False,'source_pins':source_pins()})
        raise
    except BaseException as e:
        campaign.write_json(out/'diagnostics.partial.json',{'formal':not a.smoke,'N':n,'K':k,'backends':backends,'runs':runs,'valid':False,'error':f'{type(e).__name__}: {e}','source_pins':source_pins()})
        raise
    if any(not r['accepted'] for r in runs):
        campaign.write_json(out/'diagnostics.json',{'formal':not a.smoke,'N':n,'K':k,'backends':backends,'runs':runs,'valid':False,'source_pins':source_pins(),'campaign':str(a.campaign.resolve()) if a.campaign else None})
        return 2
    grouped={}
    for mode in MODES:
        grouped[mode]={}
        for backend in backends:
            rows=[r for r in runs if r['mode']==mode and r['backend']==backend]
            grouped[mode][backend]=summarize(rows) if rows else None
    changes={mode:{} for mode in ('pool','client')}
    for mode in changes:
        for b in backends:
            base=grouped['baseline'][b]['median_rps']; value=grouped[mode][b]['median_rps']
            changes[mode][b]={'percent_vs_baseline':pct_change(base,value),'baseline_median_rps':base,'variant_median_rps':value}
    def material(mode):return any(x['percent_vs_baseline'] is not None and x['percent_vs_baseline']>=5 for x in changes[mode].values())
    accepted=all(r['accepted'] for r in runs) and not a.smoke
    doc={'formal':not a.smoke,'smoke':a.smoke,'N':n,'K':k,'repetitions':reps,'backends':backends,'modes':grouped,'changes':changes,'pool_material_improvement':material('pool'),'client_material_improvement':material('client'),'pool_material_improvement_by_backend':{b:changes['pool'][b]['percent_vs_baseline'] is not None and changes['pool'][b]['percent_vs_baseline']>=5 for b in backends},'client_material_improvement_by_backend':{b:changes['client'][b]['percent_vs_baseline'] is not None and changes['client'][b]['percent_vs_baseline']>=5 for b in backends},'pool_gain_by_backend':{b:changes['pool'][b]['percent_vs_baseline'] is not None and changes['pool'][b]['percent_vs_baseline']>=5 for b in backends},'client_gain_by_backend':{b:changes['client'][b]['percent_vs_baseline'] is not None and changes['client'][b]['percent_vs_baseline']>=5 for b in backends},'sensitivity_passed':accepted and not material('pool') and not material('client'),'FINAL_saturation_accepted':False,'candidate_accepted':False,'acceptance_note':'Sensitivity result is not FINAL saturation acceptance; final acceptance requires main campaign DB-resource evidence review.','runs':runs,'source_pins':source_pins(),'campaign':str(a.campaign.resolve()) if a.campaign else None}
    campaign.write_json(out/'diagnostics.json',doc)
    return 0 if all(r['accepted'] for r in runs) else 2

if __name__=='__main__':
    signal.signal(signal.SIGINT,campaign.signal_handler);signal.signal(signal.SIGTERM,campaign.signal_handler)
    sys.exit(main())
