#!/usr/bin/env python3
"""Independent six-CPU curve exploration and source-pinned matched supplement."""
import argparse, csv, hashlib, json, math, pathlib, signal, statistics, subprocess, sys, time
import campaign
ROOT=pathlib.Path(__file__).resolve().parents[1]
START={'pg':256,'scylla':512}


def initial_calibration_k(backend,n,target_seconds=12):
    seed={'pg':20000,'scylla':17000}[backend]
    return campaign.next_k(seed,target_seconds,n)


def onset(levels, backend, threshold=.05):
    valid=sorted((int(n), x) for n,x in levels.items() if x.get('valid') and backend in x.get('medians_rps',{}))
    for i in range(len(valid)-2):
        (a,la),(b,lb),(c,lc)=valid[i:i+3]
        gain_ab=(lb['medians_rps'][backend]-la['medians_rps'][backend])/la['medians_rps'][backend]
        gain_bc=(lc['medians_rps'][backend]-lb['medians_rps'][backend])/lb['medians_rps'][backend]
        if b==2*a and c==2*b and gain_ab < threshold and gain_bc < threshold:
            peak=max(valid,key=lambda p:p[1]['medians_rps'][backend])
            return {'onset_N':a,'confirmation_N':c,'peak_N':peak[0],'peak_rps':peak[1]['medians_rps'][backend],'doubling_gains_pct':[gain_ab*100,gain_bc*100],'overload':gain_ab<0 or gain_bc<0}
    return None


def source_pins():
    def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
    return {**campaign.metadata(), 'campaign_helper_sha256':sha(ROOT/'scripts/campaign.py'), 'resource_profiles_sha256':sha(ROOT/'scripts/resource_profiles.py'), 'six_cpu_controller_sha256':sha(pathlib.Path(__file__)), 'diagnostics_sha256':sha(ROOT/'scripts/diagnostics.py')}


def verify_six(r, expected_client_cpus=None, expected_workers=None):
    run=json.loads((pathlib.Path(r['artifacts'])/'run.json').read_text())
    placement=json.loads((pathlib.Path(r['artifacts'])/'resource-placement.json').read_text())
    client_cpus=run.get('client_cpus'); workers=run.get('workers')
    if run.get('cpu_profile')!='6' or placement.get('cpu_profile')!='6' or run.get('db_cpu_quota')!=6 or run.get('db_cpus')!='0-5' or run.get('sampler_cpus')!='14-15' or client_cpus not in ('6-7','6,7,14,15') or workers not in (2,4) or (client_cpus=='6-7' and workers!=2) or (client_cpus=='6,7,14,15' and workers!=4) or run.get('expected_shards')!=6 or placement.get('expected_shards')!=6 or placement.get('docker_nano_cpus')!=6000000000 or placement.get('docker_cpuset_cpus')!='0-5' or placement.get('docker_memory_bytes')!=8*1024**3 or placement.get('sampler_actual_affinity_cpus')!=[14,15] or placement.get('client_actual_affinity_cpus')!=([6,7] if client_cpus=='6-7' else [6,7,14,15]) :
        raise ValueError('actual run does not prove approved six-CPU placement')
    if expected_client_cpus is not None and (client_cpus!=expected_client_cpus or workers!=expected_workers): raise ValueError('client placement does not match requested diagnostics mode')
    resource=json.loads((pathlib.Path(r['artifacts'])/'resource-summary.json').read_text())
    if resource.get('allocated_cpu_capacity_cores')!=6 or (r.get('resource') or {}).get('allocated_cpu_capacity_cores',6)!=6: raise ValueError('resource does not prove six allocated CPU cores')
    if run.get('backend')=='scylla':
        ready=json.loads((pathlib.Path(r['artifacts'])/'scylla-ready-connections.json').read_text())
        client_ready=json.loads((pathlib.Path(r['artifacts'])/'client-ready.json').read_text())
        if ready.get('observed_shards')!=6 or ready.get('expected_shards')!=6: raise ValueError('Scylla readiness does not prove six shards')
        if (client_ready.get('diagnostics') or {}).get('driver_shards') not in (6,'6'): raise ValueError('Scylla client does not prove six driver shards')
    return run


def accept(r,n,k,minimum,smoke):
    try: verify_six(r)
    except (OSError,ValueError,KeyError,TypeError,AttributeError,json.JSONDecodeError): return False,['cpu_profile6']
    return campaign.run_accepted(r,minimum,n,k,resource_coverage=.5 if smoke else .8)


def accept_calibration(r,n,k):
    try: verify_six(r)
    except (OSError,ValueError,KeyError,TypeError,AttributeError,json.JSONDecodeError): return False,['cpu_profile6']
    resource=r.get('resource') or {}
    if not resource.get('formal') or not resource.get('window_duration_seconds') or resource.get('coverage',0)<.5: return False,['resource_window']
    return campaign.run_accepted_calibration(r,n,k)


def median_rate(rows,b):
    return statistics.median(x['result']['business_rps'] for x in rows if x['backend']==b and x.get('accepted'))


def run_reps(out,n,k,backends,run_fn,minimum,smoke):
    all_attempts=[]
    for retry in range(2):
        rows=[]
        failed=False
        for rep in range(1,4):
            order=backends if rep%2 else tuple(reversed(backends))
            for b in order:
                r=run_fn(b,n,k,rep); r.update(backend=b,N=n,K=k,repeat=rep)
                ok,why=accept(r,n,k,minimum,smoke); r.update(profile='6',accepted=ok,rejection_reasons=why); rows.append(r)
                if not ok:
                    failed=True
                    # Only a pure duration rejection is eligible for a whole-pass retry.
                    if set(why)!={'too_short'}:
                        all_attempts.extend(rows)
                        return all_attempts,rows,False,k
                    break
            if failed: break
        all_attempts.extend(rows)
        if not failed and len(rows)==3*len(backends): return all_attempts,rows,True,k
        if retry == 1: return all_attempts,rows,False,k
        durations=[float(x.get('result',{}).get('elapsed_ms',0))/1000 for x in rows]
        k=max(k+1,math.ceil(k*minimum/max(min(durations,default=.1),.1)*1.2))
    return all_attempts,rows,False,k


def compatible_runs(data,n,k,pins):
    if data.get('source_pins')!=pins: raise ValueError('exploration source pins mismatch')
    x=data.get('levels',{}).get(str(n),{})
    if not x.get('valid') or x.get('K')!=k or x.get('profile')!='6' or not data.get('formal'): return {}
    return {b:[r for r in x.get('repetitions',[]) if r.get('backend')==b and r.get('K')==k and r.get('profile')=='6' and accept(r,n,k,30,False)[0]] for b in campaign.BACKENDS}


def matched_plan(data,pins):
    if data.get('source_pins')!=pins: raise ValueError('exploration source pins mismatch')
    candidates=data.get('candidates',{})
    if any(not candidates.get(b) for b in campaign.BACKENDS): raise ValueError('both complete onset candidates are required')
    n=max(candidates[b]['onset_N'] for b in campaign.BACKENDS)
    level=data.get('levels',{}).get(str(n),{})
    k=level.get('K') if level.get('valid') and level.get('profile')=='6' and data.get('formal') else None
    reusable=compatible_runs(data,n,k,pins) if k else {}
    reusable={b:rs for b,rs in reusable.items() if len(rs)==3}
    return {'N':n,'K':k,'reuse':reusable,'run_backends':[b for b in campaign.BACKENDS if b not in reusable]}


def main():
    ap=argparse.ArgumentParser(description=__doc__); sub=ap.add_subparsers(dest='phase',required=True)
    e=sub.add_parser('explore'); e.add_argument('--output',type=pathlib.Path,required=True); e.add_argument('--target-seconds',type=float,default=35);e.add_argument('--min-seconds',type=float,default=30);e.add_argument('--max-users',type=int,default=8192);e.add_argument('--start-pg',type=int,default=256);e.add_argument('--start-scylla',type=int,default=512);e.add_argument('--smoke',action='store_true');e.add_argument('--warmup-requests-per-user',type=int,default=20);e.add_argument('--max-wall-seconds',type=float);e.add_argument('--min-free-memory-gb',type=float,default=2);e.add_argument('--min-free-disk-gb',type=float,default=10)
    m=sub.add_parser('matched');m.add_argument('--campaign',type=pathlib.Path,required=True);m.add_argument('--output',type=pathlib.Path,required=True);m.add_argument('--smoke',action='store_true')
    a=ap.parse_args(); smoke=a.smoke
    if a.output.exists() and any(a.output.iterdir()): ap.error('output is nonempty; refusing overwrite')
    a.output.mkdir(parents=True,exist_ok=True); pins=source_pins()
    if a.phase=='matched':
        try: data=json.loads(a.campaign.read_text()); plan=matched_plan(data,pins)
        except (OSError,ValueError,json.JSONDecodeError) as ex: ap.error(str(ex))
        if smoke: ap.error('matched phase cannot be smoke/nonformal')
        n=plan['N']; k=plan['K']; minimum=30
        # Missing/incompatible backend requires fresh common calibration; never mix K.
        calibrations={}
        if plan['run_backends']:
            rates={}
            def matched_run(b,nn,kk,rep): return invoke_run(a.output,b,nn,kk,rep,20,False)
            for b in campaign.BACKENDS:
                if b in plan['reuse']:
                    rates[b]=statistics.median(float(r['result']['business_rps']) for r in plan['reuse'][b])
                    continue
                ck=initial_calibration_k(b,n)
                r=matched_run(b,n,ck,0); good,why=accept_calibration(r,n,ck)
                calibrations[b]={'K':ck,'accepted':good,'rps':(r.get('result') or {}).get('business_rps'),'rejection_reasons':why,'artifacts':r.get('artifacts')}
                campaign.write_json(a.output/'journal.json',{'phase':'matched','N':n,'source_pins':pins,'calibrations':calibrations,'main_final_acceptance_pending':True})
                if not good: ap.error(f'common-point calibration failed for {b}: {why}')
                rates[b]=float(r['result']['business_rps'])
            k=max(campaign.next_k(r,35,n) for r in rates.values())
            plan['reuse']={b:rs for b,rs in plan['reuse'].items() if all(row.get('K')==k for row in rs)}
            plan['run_backends']=[b for b in campaign.BACKENDS if b not in plan['reuse']]
        j={'phase':'matched','N':n,'K':k,'formal':True,'reused':{b:[r['artifacts'] for r in rs] for b,rs in plan['reuse'].items()},'run_backends':plan['run_backends'],'calibrations':calibrations,'main_final_acceptance_pending':True,'source_pins':pins}
        campaign.write_json(a.output/'journal.json',j)
        def run(b,nn,kk,rep): return invoke_run(a.output,b,nn,kk,rep,a.warmup_requests_per_user if hasattr(a,'warmup_requests_per_user') else 20,False)
        initial_k=k
        rows,final_rows,ok,k=run_reps(a.output,n,k,plan['run_backends'],run,minimum,False) if plan['run_backends'] else ([],[],True,k)
        if ok and k!=initial_k and plan['reuse']:
            plan['reuse']={}; plan['run_backends']=list(campaign.BACKENDS)
            more,final_rows,ok,k=run_reps(a.output,n,k,plan['run_backends'],run,minimum,False); rows.extend(more)
        j.update(K=k,reused={b:[r['artifacts'] for r in rs] for b,rs in plan['reuse'].items()},run_backends=plan['run_backends'],runs=final_rows,attempts=rows,valid=ok and all(len(plan['reuse'].get(b,[]))==3 or len([x for x in final_rows if x['backend']==b and x.get('accepted') and x.get('K')==k and x.get('N')==n])==3 for b in campaign.BACKENDS),state='complete' if ok else 'stopped_invalid', main_final_acceptance_pending=True)
        j['summaries']={b:{**rps_summary(plan['reuse'].get(b,[]) or [x for x in final_rows if x['backend']==b and x.get('accepted')]),'run_paths':[x['artifacts'] for x in plan['reuse'].get(b,[]) or [x for x in final_rows if x['backend']==b and x.get('accepted')]]} for b in campaign.BACKENDS if plan['reuse'].get(b) or any(x['backend']==b and x.get('accepted') for x in final_rows)}
        campaign.write_json(a.output/'matched.json',j)
        with (a.output/'matched.csv').open('w',newline='') as f:
            w=csv.DictWriter(f,fieldnames=['N','K','backend','source','valid','median_rps','min_rps','max_rps','p50_us_median','p95_us_median','p99_us_median','client_cpu_core_equivalents_mean','db_cpu_core_equivalents_mean','client_rss_bytes_max','db_memory_current_bytes_max','client_read_mib_s_mean','client_write_mib_s_mean','db_cgroup_read_mib_s_mean','db_cgroup_write_mib_s_mean','device_util_pct_mean','device_avg_queue_depth_mean','run_artifacts']);w.writeheader()
            for b in campaign.BACKENDS:
                reused=plan['reuse'].get(b,[]); fresh=[x for x in final_rows if x['backend']==b]
                metrics=rps_summary(reused or [x for x in fresh if x.get('accepted')]) if reused or any(x.get('accepted') for x in fresh) else {}
                w.writerow({'N':n,'K':k,'backend':b,'source':'reused' if reused else 'new','valid':len(reused)==3 or len([x for x in fresh if x.get('accepted')])==3,**metrics,'run_artifacts':';'.join(x['artifacts'] for x in reused+fresh)})
        return 0 if j['valid'] else 2
    if a.max_users>8192 or a.max_users<1 or a.max_users&(a.max_users-1) or any(x<1 or x&(x-1) for x in (a.start_pg,a.start_scylla)) or max(a.start_pg,a.start_scylla)>a.max_users or a.target_seconds<=0 or a.min_seconds<=0: ap.error('starts/max must be positive powers of two within safety limit 8192')
    if ((a.min_seconds<30 or a.target_seconds<35) and not smoke) or (smoke and (a.min_seconds>=30 and a.target_seconds>=35)): ap.error('smoke requires reduced durations; formal requires >=30/35 seconds')
    mins={'pg':a.start_pg,'scylla':a.start_scylla}; active=set(campaign.BACKENDS); j={'phase':'explore','formal':not smoke,'smoke':smoke,'source_pins':pins,'profile':'6','config':{'target_seconds':a.target_seconds,'min_seconds':a.min_seconds,'max_users':a.max_users,'start_pg':a.start_pg,'start_scylla':a.start_scylla,'smoke':smoke,'warmup_requests_per_user':a.warmup_requests_per_user,'output':str(a.output)},'levels':{},'attempts':[],'calibrations':{},'candidates':{},'state':'running'}; campaign.write_json(a.output/'journal.json',j); began=time.monotonic()
    def run(b,n,k,rep): return invoke_run(a.output,b,n,k,rep,a.warmup_requests_per_user,smoke)
    try:
        while active:
            n=min(mins[b] for b in active); backends=[b for b in campaign.BACKENDS if b in active and mins[b]==n]
            reasons=campaign.guard(argparse.Namespace(min_free_memory_gb=a.min_free_memory_gb,min_free_disk_gb=a.min_free_disk_gb,max_wall_seconds=a.max_wall_seconds),a.output,began)
            if reasons: j.update(state='stopped_saturation_unconfirmed',stop_reasons=reasons);break
            rates={}; calibration={}
            for b in backends:
                ck=initial_calibration_k(b,n,12)
                r=run(b,n,ck,0); ok,why=accept_calibration(r,n,ck); calibration[b]={'K':ck,'accepted':ok,'rps':(r.get('result') or {}).get('business_rps'),'rejection_reasons':why,'artifacts':r.get('artifacts')}; j.setdefault('calibrations',{}).setdefault(str(n),{})[b]=calibration[b]; campaign.write_json(a.output/'journal.json',j)
                if not ok: j.update(state='stopped_calibration_invalid',stop_reasons=[b+':'+','.join(why)]);break
                rates[b]=float(r['result']['business_rps'])
            if j['state']!='running':break
            if len(backends)==2:
                k=max(campaign.next_k(rates[b],a.target_seconds,n) for b in backends)
            else:k=campaign.next_k(rates[backends[0]],a.target_seconds,n)
            attempts,rows,ok,k=run_reps(a.output,n,k,backends,run,a.min_seconds,smoke); j['attempts'].extend(attempts)
            level={'N':n,'K':k,'profile':'6','valid':ok and len(rows)==3*len(backends) and all(x.get('K')==k and x.get('accepted') for x in rows),'repetitions':rows,'attempts':attempts,'calibration':calibration,'medians_rps':{b:median_rate(rows,b) for b in backends if ok}}
            j['levels'][str(n)]=level; j['candidates']={b:onset(j['levels'],b) or j['candidates'].get(b) for b in campaign.BACKENDS}; campaign.write_json(a.output/'journal.json',j)
            if not level['valid']:j.update(state='stopped_invalid_level');break
            for b in backends:
                if j['candidates'].get(b): active.discard(b)
                else:
                    mins[b]=n*2
                    if mins[b]>a.max_users: active.discard(b); j['state']='stopped_saturation_unconfirmed'
            if not active and all(j['candidates'].get(b) is not None for b in campaign.BACKENDS):j['state']='candidates_require_diagnostics'
            if not active:break
    except BaseException as ex:
        j.update(state='interrupted' if isinstance(ex,SystemExit) else 'failed',stop_reasons=[str(ex)]);campaign.write_json(a.output/'journal.json',j);raise
    j['state']=j.get('state','stopped_saturation_unconfirmed');campaign.write_json(a.output/'journal.json',j); export_six(a.output,j);return 0 if j['state']=='candidates_require_diagnostics' else 2


def invoke_run(out,b,n,k,rep,warmup,smoke):
    d=out/f'raw-N{n}-K{k}'/f'{b}-r{rep}-{time.time_ns()}'
    cmd=[sys.executable,str(ROOT/'scripts/single_run.py'),b,str(n),str(k),str(d),'--cpu-profile','6','--warmup-requests-per-user',str(warmup)]
    rc=campaign.invoke(cmd);r=campaign.load_run(d) if d.exists() else {'artifacts':str(d),'result':{},'cleanup':{}};r.update(returncode=rc);return r


def rps_summary(rows):
    values=[float(x['result']['business_rps']) for x in rows]
    result={'median_rps':statistics.median(values),'min_rps':min(values),'max_rps':max(values)}
    latency={key:statistics.median(float(x['result']['metrics']['end_to_end'][key]) for x in rows) for key in ('p50_us','p95_us','p99_us')}
    result.update({key+'_median':value for key,value in latency.items()})
    resources=[x.get('resource') or {} for x in rows]
    means=[r.get('means') or {} for r in resources]
    result['client_cpu_core_equivalents_mean']=statistics.median((r.get('client_cpu_core_equivalents_mean') or 0) for r in resources)
    result['db_cpu_core_equivalents_mean']=statistics.median((r.get('cpu_core_equivalents_mean') or (r.get('means') or {}).get('db_cpu_core_equivalents') or 0) for r in resources)
    for key in ('client_rss_bytes_max','db_memory_current_bytes_max'):
        result[key]=max((r.get(key) or 0) for r in resources)
    for key in ('client_read_mib_s','client_write_mib_s','db_cgroup_read_mib_s','db_cgroup_write_mib_s','device_util_pct','device_avg_queue_depth'):
        vals=[m.get(key) for m in means]
        result[key+'_mean']=statistics.median(vals) if vals and all(v is not None for v in vals) else None
    return result


def med_optional(rows,key):
    values=[x.get(key) for x in rows]
    return statistics.median(values) if values and all(v is not None for v in values) else None


def export_six(out,j):
    rows=[]
    for n,l in j['levels'].items():
        if not l.get('valid'): continue
        for b in campaign.BACKENDS:
            xs=[x for x in l['repetitions'] if x['backend']==b and x.get('accepted')]
            if not xs:continue
            resources=[(json.loads((pathlib.Path(x['artifacts'])/'resource-summary.json').read_text())) for x in xs]
            means=[x.get('means',{}) for x in resources]
            rows.append({'N':n,'K':l['K'],'backend':b,'valid':l['valid'],**rps_summary(xs),'cpu_profile':'6','client_cpu_core_equivalents_mean':statistics.median(x.get('client_cpu_core_equivalents_mean') or 0 for x in resources),'db_cpu_core_equivalents_mean':statistics.median(x.get('cpu_core_equivalents_mean') or 0 for x in resources),'db_memory_current_bytes_max':max(x.get('db_memory_current_bytes_max') or 0 for x in resources),'client_rss_bytes_max':max(x.get('client_rss_bytes_max') or 0 for x in resources),'client_read_mib_s_mean':med_optional(means,'client_read_mib_s'),'client_write_mib_s_mean':med_optional(means,'client_write_mib_s'),'db_cgroup_read_mib_s_mean':med_optional(means,'db_cgroup_read_mib_s'),'db_cgroup_write_mib_s_mean':med_optional(means,'db_cgroup_write_mib_s'),'device_util_pct_mean':med_optional(means,'device_util_pct'),'device_avg_queue_depth_mean':med_optional(means,'device_avg_queue_depth'),'run_artifacts':';'.join(x['artifacts'] for x in xs)})
    with (out/'curve.csv').open('w',newline='') as f:
        fields=['N','K','backend','valid','median_rps','min_rps','max_rps','p50_us_median','p95_us_median','p99_us_median','cpu_profile','client_cpu_core_equivalents_mean','db_cpu_core_equivalents_mean','db_memory_current_bytes_max','client_rss_bytes_max','client_read_mib_s_mean','client_write_mib_s_mean','db_cgroup_read_mib_s_mean','db_cgroup_write_mib_s_mean','device_util_pct_mean','device_avg_queue_depth_mean','run_artifacts'];w=csv.DictWriter(f,fieldnames=fields);w.writeheader();w.writerows(rows)
    j['candidate_N']={b:(j['candidates'].get(b) or {}).get('onset_N') for b in campaign.BACKENDS};j['candidate_flags']={b:j['candidate_N'][b] is not None for b in campaign.BACKENDS};j['candidate_confirmation_N']={b:(j['candidates'].get(b) or {}).get('confirmation_N') for b in campaign.BACKENDS};j['saturation_confirmed']=False
    campaign.write_json(out/'campaign.json',j)

if __name__=='__main__':
    signal.signal(signal.SIGINT,campaign.signal_handler);signal.signal(signal.SIGTERM,campaign.signal_handler);sys.exit(main())
