#!/usr/bin/env python3
"""Generate the fixed-connection batch-sweep report from artifact CSVs."""
import csv, datetime, json, math, pathlib, sys
ROOT=pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'scripts'))
import fixconn_sweep as driver
OUT=ROOT/'docs/ledger-fixconn-report.md'

def number(v):
    try:
        x=float(v)
        return x if math.isfinite(x) else None
    except (TypeError,ValueError): return None

def f(v,d=2):
    x=number(v); return f'{x:.{d}f}' if x is not None else 'n/a'
def k(v):
    x=number(v); return f'{x/1000:.1f}k' if x is not None else 'n/a'
def jload(p):
    try:return json.loads(pathlib.Path(p).read_text())
    except (OSError,ValueError,TypeError):return {}
def rows(s):return driver.rows(s)
def valid(rs):return [r for r in rs if r.get('status')=='ok' and r.get('role') in ('primary','flush_rerun')]
def effective(rs):return driver.effective_primary(rs)
def attempt(r):return ROOT/r['attempt_dir'] if r.get('attempt_dir') else None
def summary(r):
    a=attempt(r);return jload(a/'summary.json') if a else {}
def summary_resources(r):return summary(r).get('resource_summary') or {}
def reclassified(r):
    cause=(r.get('cause') or '').lower()
    return any(token in cause for token in ('reclass','non-formal','full-window','recomputed over full window'))
def resource(r,db,c):
    s=summary(r); z=s.get('resource_summary') or {}; means=z.get('means') or {}; marked=reclassified(r)
    if db=='pg':
        cpu=number(means.get('db_cpu_core_equivalents')); quota=c
        return cpu, cpu/quota*100 if cpu is not None else None, None,None,number(means.get('client_cpu_core_equivalents')),number(z.get('db_memory_current_bytes_max')),z
    tidb=z.get('tidb_cpu_cores_mean')
    if marked:
        tidb=number(r.get('tidb_cpu_cores'))
        if tidb is None:
            try:
                # fixed-sweep's established full-window reclassification helper
                import conn_sweep as cs
                x=cs.tidb_full_window_cpu(attempt(r),s);tidb=x[0] if x else None
            except (OSError,ValueError,TypeError):pass
    else:tidb=number(tidb)
    quota=c; tcap=6 if 'c8x' in r.get('attempt_dir','') else 4
    tikv=number(z.get('tikv_cpu_cores_mean')); tpct=(tidb/tcap*100 if marked else number(z.get('tidb_cpu_pct_of_cpuset')))
    if tpct is None and tidb is not None:tpct=tidb/tcap*100
    mem=z.get('memory_current_bytes_max') or {}
    return tikv,number(z.get('tikv_cpu_pct_of_quota')) or (tikv/quota*100 if tikv is not None else None),tidb,tpct,number(z.get('client_cpu_cores_mean')),number(mem.get('tidb')),z

def network(r,db,z):
    if db=='tidb':
        n=(z.get('net') or {}).get('tidb') or {};return f"{f(n.get('rx_mib_s_avg'))} / {f(n.get('tx_mib_s_avg'))}"
    a=attempt(r); sm=summary(r); start=(number(sm.get('measurement_start_unix_ns')) or 0)/1e9; end=(number(sm.get('measurement_end_unix_ns')) or 0)/1e9
    try:
        with (a/'net.csv').open(newline='') as stream:samples=list(csv.DictReader(stream))
        points=[]
        for row in samples:
            t=datetime.datetime.fromisoformat(row['wall_utc'].replace('Z','+00:00')).timestamp()
            points.append((t,int(row['rx_bytes']),int(row['tx_bytes'])))
        rates=[]
        for p,q in zip(points,points[1:]):
            dt=q[0]-p[0]
            if dt>0 and p[0]>=start and q[0]<=end:rates.append(((q[1]-p[1])/dt/1024**2,(q[2]-p[2])/dt/1024**2))
        if rates:return f'{f(sum(x[0] for x in rates)/len(rates))} / {f(sum(x[1] for x in rates)/len(rates))}'
    except (OSError,ValueError,KeyError,TypeError,AttributeError):pass
    return 'n/a / n/a'
def disk(z):
    d=z.get('device_means') or z.get('means') or {};d=d.get('means') or d
    keys=('device_write_mib_s','device_write_iops','device_util_pct','device_flush_await_ms','device_write_await_ms')
    return [d.get(x) for x in keys]
def status_replicates(rs,b):return [r for r in rs if str(r.get('batch'))==str(b) and r.get('role')=='replicate' and r.get('status')=='ok']
def attempt_failure(r):
    a=attempt(r); text=''
    if a:
        for p in (a/'failure.txt',a/'client'/'stderr.txt',a/'client'/'invalid.json'):
            if p.is_file():text+=' '.join(p.read_text(errors='replace').split())
    return text[:240] or r.get('cause') or 'no failure text recorded'
def durability(r):
    a=attempt(r); s=summary(r);d=jload(a/'durability.json') if a else {}
    stores=d.get('stores') or (s.get('durability') or {}).get('stores') or []
    out=[]
    for i,x in enumerate(stores):
        c=x.get('counters') or {};w=(c.get('raft_engine_write_duration_seconds_count') or {}).get('delta');sy=(c.get('raft_engine_sync_log_duration_seconds_count') or {}).get('delta')
        out.append(f"store {x.get('store',i)} write={f(w,0)} sync={f(sy,0)}")
    return '; '.join(out) or 'n/a'
def topology(r):
    a=attempt(r);t=jload(a/'client'/'topology.json') if a else {};sts=t.get('store_status') or []
    return '; '.join(f"store {x.get('store_id',x.get('store','?'))}: {x.get('region_count','n/a')} regions/{x.get('leader_count','n/a')} leaders" for x in sts) or 'n/a'
def build():
    allrows={s:rows(s) for s in driver.SERIES}; eff={s:effective(rs) for s,rs in allrows.items()}
    L=['# Fixed-connection batch sweep report','','Generated entirely from fixed-connection CSVs and attempt artifacts. Missing measurements are shown as `n/a`.','', '## 1. Scope and environment','','Five fixed-connection curves compare PostgreSQL and TiDB under the placements below. CPU placement reflects the recorded campaign contract; versions/settings are read from each attempt artifact when present.','', '| Series | CPU placement |','|---|---|']
    placements={'pg-c4-n16':'PG cpuset 0-3, quota 4; client 4-5; 16 connections','tidb-c4-n16':'TiKV 0-3, quota 4; tidb-server 6-7,12-13 (4 logical CPUs); client 4-5; PD/sampler placement per attempt','pg-c8-n16':'PG cpuset 0-7, quota 8; client 8-9; 16 connections','tidb-c8x-n16':'TiKV 0-7, total quota 8; tidb-server 10-15 (6 logical CPUs); client + PD + sampler 8-9; 16 connections','tidb-c8x-n32':'TiKV 0-7, total quota 8; tidb-server 10-15 (6 logical CPUs); client + PD + sampler 8-9; 32 connections'}
    for s,p in placements.items():L.append(f'| {s} | {p} |')
    versions=set();settings=[]
    for s,rs in allrows.items():
      for r in rs:
       a=attempt(r)
       if not a:continue
       for fn in ('settings.txt','cluster_config.tsv','variables.json'):
        if (a/fn).exists(): settings.append(f'`{a.relative_to(ROOT)}/{fn}`')
       sm=summary(r); versions.update(str(x) for x in [sm.get('version'),sm.get('database_version')] if x)
    L += ['',f"Recorded version fields: {', '.join(sorted(versions)) if versions else 'n/a (not present in available summaries)'}.",'Recorded settings evidence: '+(', '.join(dict.fromkeys(settings)) if settings else 'n/a (no settings files present in available attempts)')+'.','CPU8+ uses SMT sibling logical CPUs; client/PD/sampler share logical CPUs 8-9, and sampler measurement may be perturbed. Client and PD each have a 2-CPU ceiling; they do not reserve separate CPUs.','', '## 2. Workload','','- 2,000,000 requests: 50,000 accounts × 40; 256 queues. TiDB uses 256 regions per table, no replication factor; one queue maps to a region.','- Fixed connections per series (16 except CPU8+ 32-connection curve); ascending batch grid: 50, 100, 250, 500, 1,000, 2,000, 4,000, 8,000.','- Stop after two consecutive points each below 105% of the best earlier RPS; a non-sampler failure ends the curve. 8,000 is the cap. Saturation is the smallest successful primary batch at or above 95% of curve maximum; failure-limit/cap curves are labeled observed-range.','- Replicates are the saturation point and its next-lower grid point (only saturation point if batch 50); replicates do not affect curve selection. Idle gate precedes each attempt. Flush-await >2 ms triggers one rerun; successful rerun replaces the primary. Imported connsweep points are marked imported.','','## 3. Per-series curves','','Latency values are seconds. DB CPU percent uses PostgreSQL quota (4/8 cores) or total TiKV quota (4/8 cores). TiDB-server percentage uses cpuset size 4 except CPU8+ size 6; † denotes full-window reclassification CPU. `*` means at least 85%.']
    for s,meta in driver.SERIES.items():
      db,c,n,x=meta; rs=allrows[s]; e=eff[s];reason=driver.completion(rs);sat,label=driver.saturation(rs,reason)
      L += ['',f'### {s}',f'Completion: `{reason or "incomplete/running"}`; saturation: batch {sat if sat is not None else "n/a"} ({label or "n/a"}).','', '| Batch | RPS | p50 | p95 | p99 | max | windows (expected) | DB CPU cores / % quota | TiDB-server cores / % cpuset | client CPU | memory GiB | sdb write MiB/s / IOPS / util % / flush await / write await | network rx/tx MiB/s | role markers | replicate RPS |','|---:|---:|---:|---:|---:|---:|---|---|---|---:|---:|---|---|---|']
      for b in driver.GRID:
       r=e.get(b,{})
       if not r:
        L.append(f'| {b} | n/a | n/a | n/a | n/a | n/a | n/a ({driver.expected_windows(b)}) | n/a | n/a | n/a | n/a | n/a | n/a | no successful primary | n/a |');continue
       dbcpu,dbpct,tcpu,tpct,client,mem,z=resource(r,db,c); dv=disk(z); reps=status_replicates(rs,b);markers=[]
       if r.get('imported')=='true':markers.append('imported')
       if any(str(q.get('batch'))==str(b) and q.get('role')=='flush_rerun' for q in rs):markers.append('flush-rerun replaced')
       if b==sat:markers.append('**sat**')
       mark='†' if reclassified(r) else '';tidbcell='—'
       if db=='tidb':tidbcell=f'{f(tcpu)} / {f(tpct,1)}%{"*" if tpct is not None and tpct>=85 else ""}{mark}'
       rep=', '.join(k(q.get('rps_request')) for q in reps) or 'n/a';expected=driver.expected_windows(b)
       L.append(f"| {b} | {k(r.get('rps_request'))} | {f(r.get('p50_s'),3)} | {f(r.get('p95_s'),3)} | {f(r.get('p99_s'),3)} | {f(r.get('max_s'),3)} | {r.get('windows') or 'n/a'} ({expected}) | {f(dbcpu)} / {f(dbpct,1)}% | {tidbcell} | {f(client)} | {f(number(mem)/1024**3 if number(mem) is not None else None)} | {' / '.join(f(v,2) for v in dv)} | {network(r,db,z)} | {', '.join(markers) or 'primary'} | {rep} |")
    L += ['', '### Flush reruns','','Each successful rerun replaces its original primary in the effective curve. RPS is requests/s; p99 and flush await are seconds and milliseconds respectively.','','| Series | Batch | Original primary RPS / p99 / flush await | Rerun RPS / p99 / flush await | Used |','|---|---:|---|---|---|']
    rerun_count=0
    for s,rs in allrows.items():
      for rer in (q for q in rs if q.get('role')=='flush_rerun'):
        primary=next((q for q in reversed(rs) if str(q.get('batch'))==str(rer.get('batch')) and q.get('role')=='primary' and q.get('status')=='ok'),{})
        triple=lambda q:f"{k(q.get('rps_request'))} / {f(q.get('p99_s'),3)} / {f(q.get('flush_await_ms'))} ms"
        L.append(f"| {s} | {rer.get('batch')} | {triple(primary)} | {triple(rer)} | {'rerun' if rer.get('status')=='ok' else 'primary (rerun failed)'} |")
        rerun_count+=1
    if not rerun_count:L.append('| n/a | n/a | no flush reruns recorded | n/a | n/a |')
    L += ['', '## 4. Equal-batch comparison','','| Batch | PG4 RPS / p99 | TiDB4 RPS / p99 | PG8 RPS / p99 | TiDB8+ N16 RPS / p99 | TiDB8+ N32 RPS / p99 |','|---:|---|---|---|---|---|']
    names=list(driver.SERIES)
    for b in driver.GRID:
      vals=[]
      for s in names:
       r=eff[s].get(b,{});vals.append(f"{k(r.get('rps_request'))} / {f(r.get('p99_s'),3)}" if r else 'n/a / n/a')
      L.append(f'| {b} | '+' | '.join(vals)+' |')
    L += ['', 'CPU8+ versus other series, equal batch: RPS ratios are computed only when both measured values exist.','','| Batch | 8+ N16 / PG8 | 8+ N16 / TiDB4 | 8+ N16 / PG4 | 8+ N32 / PG8 |','|---:|---:|---:|---:|---:|']
    for b in driver.GRID:
      rr=[]
      for a,z in [('tidb-c8x-n16','pg-c8-n16'),('tidb-c8x-n16','tidb-c4-n16'),('tidb-c8x-n16','pg-c4-n16'),('tidb-c8x-n32','pg-c8-n16')]:
       x=number(eff[a].get(b,{}).get('rps_request'));y=number(eff[z].get(b,{}).get('rps_request'));rr.append(f(x/y,2) if x is not None and y else 'n/a')
      L.append(f'| {b} | '+' | '.join(rr)+' |')
    L += ['', '## 5. CPU8+ shared-CPU accounting','','One source only: `fixconn_sweep.shared_cpu` reads client, PD and wrapper/sampler once from `res-pd`; sampler observations across other res-* records are not summed. Total percentage is of two shared logical CPUs.','','| Series / batch / role | Client cores | PD cores | wrapper/sampler cores | total / % of 2 CPUs |','|---|---:|---:|---:|---:|']
    for s in ('tidb-c8x-n16','tidb-c8x-n32'):
      for r in valid(allrows[s]):
       x=driver.shared_cpu(ROOT/r['attempt_dir']); L.append(f"| {s} / {r.get('batch')} / {r.get('role')} | "+('n/a | n/a | n/a | n/a' if x is None else f'{f(x[0])} | {f(x[1])} | {f(x[2])} | {f(sum(x[:3]))} / {f(x[3]*100,1)}%{"*" if x[3]>=.85 else ""}')+' |')
    L += ['', '## 6. Durability and region distribution (each TiDB attempt)','','| Series / batch / role | TiKV raft-engine write/sync deltas | client/topology.json region distribution |','|---|---|---|']
    for s in ('tidb-c4-n16','tidb-c8x-n16','tidb-c8x-n32'):
     for r in valid(allrows[s]): L.append(f"| {s} / {r.get('batch')} / {r.get('role')} | {durability(r)} | {topology(r)} |")
    L += ['', '## 7. Replicate comparison','','| Series / batch | Primary RPS | Replicate RPS | Difference % | Flag |','|---|---:|---:|---:|---|']
    for s,rs in allrows.items():
     for r in rs:
      if r.get('role')!='replicate':continue
      p=eff[s].get(int(r.get('batch') or 0),{});pv=number(p.get('rps_request'));rv=number(r.get('rps_request'));delta=(rv/pv-1)*100 if pv else None
      replaced=any(str(q.get('batch'))==str(r.get('batch')) and q.get('role')=='flush_rerun' and q.get('status')=='ok' for q in rs)
      flags=[]
      if delta is not None and abs(delta)>10:flags.append(r'FLAG \|diff\|>10%')
      if replaced:flags.append('FLAG primary is flush-rerun replacement')
      L.append(f"| {s} / {r.get('batch')} | {k(pv)} | {k(rv)} | {f(delta,1)} | {'; '.join(flags) or '—'} |")
    L += ['', '## 8. Failures and limits','','| Series | Batch ending curve | Role | Cause | Failure text excerpt |','|---|---:|---|---|---|']
    failures=0
    for s,rs in allrows.items():
     for r in rs:
      if r.get('role') in ('primary','flush_rerun') and r.get('status') not in ('ok',''):
       failures+=1;L.append(f"| {s} | {r.get('batch','n/a')} | {r.get('role')} | {r.get('cause') or 'n/a'} | {attempt_failure(r)} |")
    if not failures:L.append('| n/a | n/a | n/a | no recorded failed attempts | n/a |')
    L += ['', '## 9. Data-derived observations','','Facts below are calculated from available successful primaries; no causal inference is intended.']
    for s,rs in allrows.items():
      e=eff[s]
      if not e:L.append(f'- {s}: no data; peak, saturation, and CPU-at-saturation not measured.');continue
      peak=max(e,key=lambda b:number(e[b].get('rps_request')) or -1);sat,label=driver.saturation(rs,driver.completion(rs));L.append(f'- {s}: maximum observed RPS {k(e[peak].get("rps_request"))} at batch {peak}; saturation {sat if sat is not None else "n/a"} ({label or "n/a"}).')
      if sat in e and s.startswith('tidb'):
       _,_,tc,tp,_,_,_=resource(e[sat], 'tidb',driver.SERIES[s][1]);L.append(f'  - TiDB-server at saturation: {f(tc)} cores, {f(tp,1)}% of cpuset'+(' (>=85%)' if tp is not None and tp>=85 else '')+'.')
      ordered=sorted(e)
      for previous,current in zip(ordered,ordered[1:]):
       before=number(e[previous].get('rps_request'));now=number(e[current].get('rps_request'))
       if before is None or now is None or now>=before*.9:continue
       point=e[current];flush=number(point.get('flush_await_ms'));rerun=any(str(q.get('batch'))==str(current) and q.get('role')=='flush_rerun' for q in rs)
       rerun_text='done' if rerun else ('no rerun (flush await <=2 ms)' if flush is not None and flush<=2 else 'not recorded')
       L.append(f"  - Dip: batch {current} effective RPS {k(now)}, down from {k(before)} at batch {previous}; p99={f(point.get('p99_s'),3)} s, flush await={f(flush)} ms; flush rerun {rerun_text}.")
    flushes=[];ratios=[]
    for s,e in eff.items():
     for r in e.values():
      _,_,_,_,_,_,z=resource(r,driver.SERIES[s][0],driver.SERIES[s][1]);v=number(disk(z)[3]);
      if v is not None:flushes.append(v)
    for other in ('pg-c4-n16','tidb-c4-n16','pg-c8-n16'):
      for b,r in eff['tidb-c8x-n16'].items():
       y=number(eff[other].get(b,{}).get('rps_request'));x=number(r.get('rps_request'))
       if x is not None and y:ratios.append(x/y)
    L.append(f"- Measured flush-await range: {f(min(flushes))}–{f(max(flushes))} ms." if flushes else '- Flush await: not measured.')
    L.append(f"- CPU8+ N16 / other-series equal-batch RPS ratio range: {f(min(ratios))}–{f(max(ratios))}." if ratios else '- CPU8+ comparison ratio: not measured.')
    L += ['- TiKV per-thread CPU: not measured. Run-end region distribution: not measured (topology is the captured client topology evidence).','', '## 10. Limitations and file index','','- Single-run points and selected replicates do not characterize variance comprehensively; observed-range labels do not imply a measured saturation point.','- CPU8+ shared placement may perturb sampler measurements; SMT siblings are shared. TiKV per-thread CPU and run-end region distribution were not measured.','- PostgreSQL network is computed from timestamped wrapper `net.csv` samples inside the summary measurement window; `n/a / n/a` means the network file is absent or has no usable in-window sample pairs.','- File index: `artifacts/fixconn-*.csv`; each row’s attempt directory contains `summary.json`, resource summaries, optional `durability.json` and `client/topology.json`, failure evidence, cleanup, and idle-gate evidence. Imported rows refer to original connsweep attempt paths.','- Empty or partial campaigns are reported with `n/a` and “no data”; absent artifacts are not inferred.','']
    text='\n'.join(L)
    if 'nan' in text.lower() or 'None' in text: text=text.replace('nan','n/a').replace('NaN','n/a').replace('None','n/a')
    return text

def main(argv=None):
    global OUT
    args=sys.argv[1:] if argv is None else argv
    if args:OUT=pathlib.Path(args[0]);OUT=OUT if OUT.is_absolute() else pathlib.Path.cwd()/OUT
    text=build();OUT.parent.mkdir(parents=True,exist_ok=True);OUT.write_text(text,encoding='utf-8');print(f'generated {OUT}');return 0
if __name__=='__main__':raise SystemExit(main())
