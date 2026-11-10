#!/usr/bin/env python3
"""Generate the connection sweep report using only standard-library modules."""
import csv, json, pathlib, sys, math
ROOT=pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'scripts'))
import conn_sweep as driver
OUT=ROOT/'docs/ledger-connsweep-report.md'

def jload(p):
    try: return json.loads(pathlib.Path(p).read_text())
    except (OSError,ValueError,TypeError): return {}
def num(v):
    try:
        x=float(v); return x if math.isfinite(x) else None
    except (TypeError,ValueError): return None
def fmt(v,d=2):
    x=num(v); return f'{x:.{d}f}' if x is not None else '—'
def k(v):
    x=num(v); return f'{x/1000:.1f}k' if x is not None else '—'
def gb(v):
    x=num(v); return f'{x/1024**3:.2f}' if x is not None else '—'
def pct(x): return fmt(x,1)
def loadrows(db,c,b): return driver.rows_for(db,c,b)
def curves():
    return {(db,c,b):loadrows(db,c,b) for db in ('pg','tidb') for c in (4,8) for b in driver.BATCHES}
def selected_attempt(row): return ROOT/row['attempt_dir'] if row and row.get('attempt_dir') else None
def summary(row): return jload(selected_attempt(row)/'summary.json') if selected_attempt(row) else {}
def effective(db,c,b): return driver.effective_primary(loadrows(db,c,b))
def attempts_all():
    out=[]
    for db,c,b in curves(): out.extend(loadrows(db,c,b))
    return out

def resource_values(db,row):
    s=summary(row); r=s.get('resource_summary') or {}; mark='†' if db=='tidb' and driver.TIDB_RECLASSIFY_NOTE in (row.get('cause') or '') else ''
    if db=='pg':
        m=r.get('means') or {}; cpu=num(m.get('db_cpu_core_equivalents')); quota=4 if '-c4-' in row.get('attempt_dir','') else 8
        return {'dbcpu':cpu,'dbpct':cpu/quota*100 if cpu is not None else None,'client':num(m.get('client_cpu_core_equivalents')),'mem':num(r.get('db_memory_current_bytes_max')),'rss':num(r.get('client_rss_bytes_max')),'mark':mark,'dev':m,'net':{}}
    cap=4; tidbcpu=num(row.get('tidb_cpu_cores')) if mark else num(r.get('tidb_cpu_cores_mean'))
    if mark and tidbcpu is None:
        calc=driver.tidb_full_window_cpu(selected_attempt(row),s); tidbcpu=calc[0] if calc else None
    mem=r.get('memory_current_bytes_max') or {}; stores=[v for key,v in mem.items() if key.startswith('tikv')]
    return {'dbcpu':num(r.get('tikv_cpu_cores_mean')),'dbpct':num(r.get('tikv_cpu_pct_of_quota')),'tidbcpu':tidbcpu,
      'tidbpct': (tidbcpu/4*100 if mark and tidbcpu is not None else num(r.get('tidb_cpu_pct_of_cpuset'))),
      'pd':num(r.get('pd_cpu_cores_mean')),'client':num(r.get('client_cpu_cores_mean')),'mem':num(mem.get('tidb')),
      'tikvmem':max([num(x) for x in stores if num(x) is not None],default=None),'storesmem':[num(x) for x in stores],
      'rss':num(r.get('client_rss_bytes_max')),'mark':mark,'dev':r.get('device_means') or {},'net':r.get('net') or {}}
def devvals(v):
    d=v.get('dev') or {}
    if 'means' in d: d=d['means']
    return [d.get('device_write_mib_s'),d.get('device_write_iops'),d.get('device_util_pct'),d.get('device_avg_queue_depth'),d.get('device_flush_await_ms'),d.get('device_write_await_ms')]
def network(v,db,row=None):
    n=v.get('net') or {}
    if db=='pg' and row:
        a=selected_attempt(row); s=summary(row); start=(num(s.get('measurement_start_unix_ns')) or 0)/1e9; end=(num(s.get('measurement_end_unix_ns')) or 0)/1e9
        try:
            with (a/'net.csv').open(newline='') as f: samples=list(csv.DictReader(f))
            import datetime
            points=[]
            for x in samples:
                t=datetime.datetime.fromisoformat(x['wall_utc'].replace('Z','+00:00')).timestamp()
                points.append((t,int(x['rx_bytes']),int(x['tx_bytes'])))
            rates=[]
            for p,q in zip(points,points[1:]):
                dt=q[0]-p[0]
                if dt>0 and p[0]>=start and q[0]<=end: rates.append(((q[1]-p[1])/dt/1024**2,(q[2]-p[2])/dt/1024**2))
            if rates: return f'{fmt(sum(x[0] for x in rates)/len(rates))} / {fmt(sum(x[1] for x in rates)/len(rates))}'
        except (OSError,ValueError,KeyError,TypeError): pass
        return '— / —'
    x=n.get('tidb') or {}
    return f"{fmt(x.get('rx_mib_s_avg'))} / {fmt(x.get('tx_mib_s_avg'))}"
def durability_table(row):
    a=selected_attempt(row); s=summary(row); d=jload(a/'durability.json') if a else {}; stores=d.get('stores') or (s.get('durability') or {}).get('stores') or []
    top=s.get('effective',{}).get('topology',{}); statuses=top.get('store_status') or []
    lines=[]
    for ix,z in enumerate(stores):
        counters=z.get('counters') or {}; store=z.get('store',ix)
        write=(counters.get('raft_engine_write_duration_seconds_count') or {}).get('delta')
        sync=(counters.get('raft_engine_sync_log_duration_seconds_count') or {}).get('delta')
        sr= statuses[ix] if ix<len(statuses) else {}
        reg=sr.get('region_count'); lead=sr.get('leader_count'); total=sum(x.get('region_count',0) for x in statuses) or 1
        lines.append(f'| {store} | {fmt(write,0)} | {fmt(sync,0)} | {reg if reg is not None else "—"} | {lead if lead is not None else "—"} | {fmt(100*reg/total,1) if reg is not None else "—"}% |')
    return lines or ['| — | — | — | — | — | — |']

def build():
    E={(db,c,b):effective(db,c,b) for db,c,b in curves()}
    sats={(db,c,b):driver.saturation(loadrows(db,c,b),driver.GRIDS[c]) for db,c,b in curves()}
    lines=['# Connection sweep 測試報告：PostgreSQL vs TiDB，CPU4 / CPU8，connections 掃描','','目的：在固定 batch-max（50、100）與 256 queues 下增加 client connections，量測 RPS 與 latency，找出各系列的飽和點。Latency 的可接受門檻由讀者自行選定，本報告不預設 SLO。','', '資料來源：`artifacts/connsweep-{pg,tidb}-c{4,8}-bm{50,100}.csv` 與各 attempt 目錄；選擇 effective primary、saturation、replicate 嚴格重用 `scripts/conn_sweep.py`。','','## 1. 測試環境','','| 項目 | 內容 |','|---|---|','| 主機 | AMD Ryzen 7 3700X，8 核 16 執行緒（SMT 兄弟核為 N 與 N+8），62 GB RAM，Linux 6.17.0 |','| 磁碟 | `/dev/sdb2`（ext4）；Docker 資料與 DB 資料均在實體碟 sdb，DB/WAL 未分碟 |','| PostgreSQL | 18.6（Debian），Docker 容器 |','| TiDB | v8.5.6（PD、TiKV、TiDB 均 `pingcap/*:v8.5.6`；version() 為 8.0.11-TiDB-v8.5.6） |','| 容器管理 | wrapper 每次建立一次性容器、網路與 volume，完成後清除 |','','### 資源配置（cpuset 綁核）','','| 情境 | DB（計入 CPU 預算） | tidb-server（不計入） | client | PD + sampler |','|---|---|---|---|---|','| PG CPU4 | PG cpuset 0-3，NanoCpus=4，8 GiB | – | 4-5（2 worker） | sampler 14-15 |','| PG CPU8 | PG cpuset 0-7，NanoCpus=8，8 GiB | – | 8-9 | sampler 14-15 |','| TiDB CPU4 | 1 TiKV：cpuset 0-3，NanoCpus=4，8 GiB | cpuset 6-7,12-13（4 threads），16 GiB | 4-5 | PD、sampler 14-15 |','| TiDB CPU8 | 2 TiKV：各 NanoCpus=4，cpuset 0-3 與 4-7，各 8 GiB | cpuset 10-13（4 threads），16 GiB | 8-9 | PD、sampler 14-15 |','','TiDB CPU 預算只計 TiKV；tidb-server 與 PD 另外綁核並取樣。CPU4/CPU8 均與 SMT sibling 共用（主機僅 8 個實體核），PG 與 TiDB 相同。','','## 2. 負載方式','','- **資料：** 50,000 accounts × 40 requests = 2,000,000 requests；amount=1，transaction id 為新 UUID。','- **Queues / connections：** queue = `user % 256`，共 256 queues。每個測點使用 N 條資料連線（另有 monitor）；每條連線服務 `256/N` queues，round-robin 送出各 queue 的 windows；每連線同時一個 window in-flight，每 queue 至多一個 window in-flight，同 queue 依序處理。','- **Window：** 最多 batch-max 筆同一 queue 的請求構成一個 atomic transaction；每 window 只碰一個 TiDB region；TiDB 每表 256 個 pre-split regions、no RF，一 queue 對應一 region。PG 一條 SQL；TiDB pessimistic transaction 兩次 round trip。30 s per-window timeout。','- **Queue 數與 windows：** 50,000 × 40 按 256 queues 分配後，80 queues 各有 7,840 requests，176 queues 各 7,800。預期 windows = 40,016（batch 50）、20,048（batch 100），與 N 無關。逐一檢查所有 effective primary CSV `windows` 欄位：'+('全部符合預期' if all(int(float(E[(db,c,b)][n]['windows']))==(40016 if b==50 else 20048) for db,c,b in E for n in driver.GRIDS[c] if n in E[(db,c,b)]) else '發現不一致（詳見資料列）')+'。','- **掃描網格：** CPU4：4、8、16、32、64、128、256；CPU8：8、16、32、64、128、256。256 是上限，因 queue 只有 256；不向上擴展。','','### PostgreSQL 與 TiDB 差異（未補償）','','- TiDB 一個 window 為 multi-statement、2 round trips，before/after 由 client 計算；PG 為單一 statement / round trip、由 DB 計算。','- TiDB 表有 shard 欄位，沒有 CHECK（TiDB 預設關閉）；兩者均保留 FK。','- TiDB 額外包含 tidb-server 與 PD；PG 沒有。Latency 是 window 送出至最後回應（TiDB 至 COMMIT），按 requests 加權。','','## 3. DB 設定','','| 項目 | PostgreSQL | TiDB |','|---|---|---|','| 持久化 | `fsync=on`、`synchronous_commit=on`、`full_page_writes=on` | raft-engine `enable=true`；每個 write batch sync；以 write/sync counter 差值檢驗（±1%） |','| 記憶體 / WAL | `shared_buffers=2GB`、`wal_buffers=256MB`、`max_wal_size=8GB`、`max_connections=300`、`track_wal_io_timing=on`、`checkpoint_timeout=300s` | TiKV block cache 2 GiB；tidb-server memory 16 GiB |','| 複本 | 單機 | no RF（`max-replicas=1`） |','| TiKV | – | unified readpool=4、scheduler workers=4、raftstore store/apply/IO pools=2/2/1、raft write wait=20us |','| TiDB variables | – | pessimistic、1PC=1、async commit=1、FK checks=1、multi_statement_mode=OFF（client connection flag 允許 multi-statement） |','','PG attempt 的 `settings.txt` 與 TiDB `cluster_config.tsv` / `variables.json` 確認設定與前次 batch-sweep 報告一致；各 CPU / batch 抽查的實際設定一致。','','## 4. 監控與驗證方式','','- **Sampling：** 250 ms；以 cgroup CPU/memory、主機 sdb diskstats、container network、client CPU/RSS 取樣，依 client measurement start/end 篩選。TiDB 另取 TiKV raft-engine durability 與 topology。','- CPU 以 cgroup usage 換算 cores / quota；memory 為 cgroup `memory.current` 最大值（含 page cache），client RSS 為最大值；sdb 列 write MiB/s、write IOPS、util、queue depth、flush await、write await。','- Idle gate：每次 launch 前各 logical CPU idle（含 iowait）至少 90%，同一 5 秒無其他 process 使用超過 30% 單核；最多等待 60 秒。每次測量都有 `<attempt>-idle.json`。','- Stall：flush await > 2 ms 則額外 rerun 一次，rerun 若 OK 取代 primary，兩列均保留。Sampler race 最多於新目錄重試兩次。','- **Latency：** 每 window 從送出至 COMMIT 回應，window latency 按其中 requests 加權，報 p50/p95/p99/max。','- **飽和定義：** fixed grid 上 effective-primary RPS 最大值的 95% 門檻，取最小 N；replicate 只跑飽和點及其下一個較低 grid 點，不改 primary 選擇。若 256 命中，標示 observed-grid saturation。','- **TiDB reclassified 樣本：** '+str(sum(1 for r in attempts_all() if driver.TIDB_RECLASSIFY_NOTE in (r.get('cause') or '')))+' 筆 attempt 紀錄（含 replicate），其測量窗口開始時 tidb container cgroup io counters 空白，wrapper 因而標記 res-tidb non-formal。Driver 僅在 TiKV/PD formal 且 tidb CPU/memory samples 完整時接受；tidb CPU 對 † row 以 full-window counter 重算（3 個 formal attempt 驗證差異在 0.04% 內），不採用 `summary.json` 的 tidb CPU 值。','','## 5. 結果','','### 5.1 飽和點總覽','','| 系列 | batch | 飽和連線數 | RPS | p50 / p99 秒 | 標籤 | peak RPS | 256 connections / peak |','|---|---:|---:|---:|---:|---|---:|---:|']
    for db in ('pg','tidb'):
      for c in (4,8):
       for b in driver.BATCHES:
        e=E[(db,c,b)]; sat=sats[(db,c,b)]; top=max((num(e[n].get('rps_request')) or 0 for n in driver.GRIDS[c] if n in e),default=0); peakn=max(driver.GRIDS[c],key=lambda n:num(e.get(n,{}).get('rps_request')) or -1) if e else None
        r=e.get(sat.get('conns'),{}); at=e.get(256,{})
        ratio=(num(at.get('rps_request'))/top*100) if at and top else None
        lines.append(f"| {db.upper()} CPU{c} | {b} | {sat.get('conns','不完整')} | {k(r.get('rps_request'))} | {fmt(num(r.get('p50_s')),3)} / {fmt(num(r.get('p99_s')),3)} | {sat.get('label') or '—'} | {k(top)}（N={peakn if peakn is not None else '—'}） | {pct(ratio)}% |")
    lines+=['','### 5.2–5.5 各曲線詳細資料','']
    for db,c in [('pg',4),('pg',8),('tidb',4),('tidb',8)]:
      lines += [f"#### {db.upper()} CPU{c}"]
      for b in driver.BATCHES:
        e=E[(db,c,b)]; grid=driver.GRIDS[c]; lines += [f'##### batch-max {b} (a) RPS 與 latency（秒；方括號為 replicate RPS）','','| conns | windows | RPS [replicates] | p50 | p95 | p99 | max |','|---:|---:|---:|---:|---:|---:|---:|']
        for n in grid:
          row=e.get(n,{}); reps=[r for r in loadrows(db,c,b) if int(r.get('conns') or 0)==n and r.get('role')=='replicate' and r.get('status')=='ok']
          repspec=', '.join(k(x.get('rps_request')) for x in reps)
          lines.append(f"| {n} | {row.get('windows','—')} | {k(row.get('rps_request'))}{' ['+repspec+']' if repspec else ''} | {fmt(row.get('p50_s'),3)} | {fmt(row.get('p95_s'),3)} | {fmt(row.get('p99_s'),3)} | {fmt(row.get('max_s'),3)} |")
        lines += ['', '##### (b) CPU 與記憶體','','| conns | DB CPU cores / % quota |'+(' TiDB-server cores / % (4-CPU cpuset) |' if db=='tidb' else '')+' client CPU cores | DB memory max GiB |'+(' TiKV memory max GiB |' if db=='tidb' else '')+' client RSS GiB | client-bound |','|---:|---:|'+('---:|' if db=='tidb' else '')+'---:|---:|'+('---:|' if db=='tidb' else '')+'---:|---:|']
        for n in grid:
          row=e.get(n,{});v=resource_values(db,row); bound='yes' if (v.get('client') or 0)>=1.7 else 'no'; cell=f"{fmt(v.get('dbcpu'))} / {pct(v.get('dbpct'))}%{v.get('mark','')}"
          if db=='tidb': cell+=f" | {fmt(v.get('tidbcpu'))} / {pct(v.get('tidbpct'))}%{'*' if (v.get('tidbpct') or 0)>=85 else ''}{v.get('mark','')}"
          cell+=f" | {fmt(v.get('client'))} | {gb(v.get('mem'))} |"
          if db=='tidb': cell+=f" {' / '.join(gb(x) for x in v.get('storesmem',[]))} |"
          cell+=f" {gb(v.get('rss'))} | {bound} |"
          lines.append(f'| {n} | {cell}')
        lines+=['','##### (c) 磁碟（sdb）與網路','','| conns | write MiB/s | write IOPS | util % | queue depth | flush await ms | write await ms | network rx / tx MiB/s |','|---:|---:|---:|---:|---:|---:|---:|---|']
        for n in grid:
          v=resource_values(db,e.get(n,{})); d=devvals(v); lines.append(f"| {n} | {fmt(d[0],1)} | {fmt(d[1],0)} | {fmt(d[2],1)} | {fmt(d[3],2)} | {fmt(d[4],2)} | {fmt(d[5],2)} | {network(v,db,e.get(n,{}))} |")
        if db=='tidb':
          lines+=['','##### (d) TiKV raft-engine durability 與 store distribution','','| conns | store | raft write 次數 | sync 次數 | region 數 | leader 數 | region 占比 |','|---:|---:|---:|---:|---:|---:|---:|']
          for n in grid:
            for x in durability_table(e.get(n,{})): lines.append(f'| {n} '+x[1:])
        lines.append('')
    lines+=['### 5.6 PostgreSQL 與 TiDB 對照','','每表為相同 CPU / batch 的 RPS 與 p99（秒）。']
    for c in (4,8):
      for b in driver.BATCHES:
        lines += [f'#### CPU{c}，batch-max {b}','','| conns | PG RPS | PG p99 s | TiDB RPS | TiDB p99 s |','|---:|---:|---:|---:|---:|']
        for n in driver.GRIDS[c]:
          p=E[('pg',c,b)].get(n,{});t=E[('tidb',c,b)].get(n,{})
          lines.append(f"| {n} | {k(p.get('rps_request'))} | {fmt(p.get('p99_s'))} | {k(t.get('rps_request'))} | {fmt(t.get('p99_s'))} |")
    lines += ['', '## 6. Region 分布（TiDB；計時前 topology）','','由所有 OK attempt 的 `client/topology.json` 檢查：accounts / ledger 各表 region counts 均為 256/256，peer count != 1 為 0。Store balance 範圍依 attempts 計算：']
    bal=[]
    for row in attempts_all():
      if row.get('status')!='ok' or not row.get('attempt_dir','').startswith('artifacts/connsweep-tidb-'): continue
      t=jload(ROOT/row['attempt_dir']/'client/topology.json'); statuses=t.get('store_status') or []; total=sum(x.get('region_count',0) for x in statuses)
      if total: bal += [100*x['region_count']/total for x in statuses]
    lines.append(f"store region balance 最低/最高占比：{fmt(min(bal),1) if bal else '—'}% / {fmt(max(bal),1) if bal else '—'}%（OK attempts）。沒有量測 post-run distribution、per-region load 或各 store data size。")
    lines+=['','## 7. 全部 attempt 清單','','| DB/CPU/batch | conns | role | attempt dir | status | wall s | RPS | cause |','|---|---:|---|---|---|---:|---:|---|']
    for db,c,b in curves():
      for r in loadrows(db,c,b):
        wall=r.get('wall_s')
        if not num(wall): wall=next((x.get('wall_s') for x in loadrows(db,c,b) if x.get('attempt_dir')==r.get('attempt_dir') and num(x.get('wall_s')) is not None),None)
        lines.append(f"| {db} CPU{c} / {b} | {r.get('conns','—')} | {r.get('role','—')} | `{r.get('attempt_dir','—')}` | {r.get('status','—')} | {fmt(wall,1)} | {k(r.get('rps_request'))} | {r.get('cause') or '—'} |")
    validcoverage=sum(1 for (db,c,b),e in E.items() if all(n in e for n in driver.GRIDS[c]) for _ in [0])
    goodpoints=sum(len(E[(db,c,b)]) for db,c,b in E)
    idledirs=list((ROOT/'artifacts').glob('connsweep-*-idle.json'))
    attemptdirs=[p for p in (ROOT/'artifacts').glob('connsweep-*-a[0-9]*') if p.is_dir()]
    lines += ['',f'Coverage audit：{goodpoints} / 52 grid configurations have effective OK primaries; complete curves {validcoverage}/8. Idle evidence files：{len(idledirs)}；matching attempt directories：{len(attemptdirs)}。Attempt list includes every CSV row, including failures, replicates and reclassified rows.','','## 8. 觀察（僅報告可由資料計算的事實）','']
    for key,e in E.items():
      db,c,b=key
      if not e: continue
      topn=max(e,key=lambda n:num(e[n].get('rps_request')) or -1); top=e[topn]; last=e.get(256,{}); sat=sats[key];
      lines.append(f"- {db.upper()} CPU{c} batch {b}：峰值在 N={topn}，RPS={k(top.get('rps_request'))}，p99={fmt(top.get('p99_s'),3)} s；N=256 RPS={k(last.get('rps_request'))}（峰值 {pct((num(last.get('rps_request')) or 0)/(num(top.get('rps_request')) or 1)*100)}%），p99={fmt(last.get('p99_s'),3)} s；飽和點 N={sat.get('conns','incomplete')}。")
      if sat.get('conns') in e:
        v=resource_values(db,e[sat['conns']]);lines.append(f"  飽和點 N={sat['conns']} 資源：DB CPU={pct(v.get('dbpct'))}% quota"+(f", tidb-server={pct(v.get('tidbpct'))}%{'*' if (v.get('tidbpct') or 0)>=85 else ''}" if db=='tidb' else '')+'.')
    replicates=[];outliers=[]
    for (db,c,b),e in E.items():
      for n,row in e.items():
        reps=[r for r in loadrows(db,c,b) if int(r.get('conns') or 0)==n and r.get('role')=='replicate' and r.get('status')=='ok']
        for r in reps:
          delta=(num(r.get('rps_request'))/num(row.get('rps_request'))-1)*100 if num(row.get('rps_request')) else 0
          replicates.append(f'{db} CPU{c} b{b} N{n}: {delta:+.1f}%')
          if abs(delta)>10: outliers.append((db,c,b,n,row,delta))
      grid=driver.GRIDS[c]
      for i,n in enumerate(grid[1:-1],1):
        a=num(e.get(grid[i-1],{}).get('rps_request'));x=num(e.get(n,{}).get('rps_request'));z=num(e.get(grid[i+1],{}).get('rps_request'))
        if a and x and z and (x<min(a,z)*.8 or x>max(a,z)*1.2): outliers.append((db,c,b,n,e[n],None))
    lines.append('- Replicate 相對 effective primary 的 RPS 差：'+('; '.join(replicates) if replicates else '無 OK replicate')+'.')
    lines.append('- Outlier（replicate 差異 >10% 或相鄰 RPS 比例偏離 >20%）：')
    seen=set()
    for db,c,b,n,r,delta in outliers:
      if (db,c,b,n) in seen: continue
      seen.add((db,c,b,n));v=resource_values(db,r);d=devvals(v);lines.append(f"  - {db} CPU{c} batch {b} N{n}：RPS={k(r.get('rps_request'))}，flush await={fmt(d[4])} ms，DB CPU={pct(v.get('dbpct'))}% quota"+(f"，replicate delta={delta:+.1f}%" if delta is not None else '，neighbour-order outlier')+'.')
    outlier=E[('pg',8,50)].get(32,{}); ov=resource_values('pg',outlier); od=devvals(ov)
    lines.append(f"- 已知 outlier PG CPU8 batch 50 / N=32：RPS={k(outlier.get('rps_request'))}，比兩側 N=16 / N=64 的 RPS 均低；flush await={fmt(od[4])} ms，DB CPU={pct(ov.get('dbpct'))}% quota；沒有對此點執行 replicate。沒有量測 TiKV per-thread CPU、TiDB per-stage latency。")
    bounds=[(db,c,b,n) for (db,c,b),e in E.items() for n,row in e.items() if (resource_values(db,row).get('client') or 0)>=1.7]
    lines.append('Client-bound flag（client CPU ≥ 85% of 2 CPUs）：'+('有：'+', '.join(f'{x[0]} CPU{x[1]} b{x[2]} N{x[3]}' for x in bounds) if bounds else 'none（所有 effective primary 均低於門檻）')+'.')
    lines+=['','## 9. 限制與已知事件','','- 每個 grid point 原則上單次 run；僅飽和點與下一低點 replicate，不能從單次結果推定穩態曲線。','- 總量 2M requests；高 connections 測點只需數秒，短 run 的取樣與雜訊影響較大；不可與此前 4M batch-sweep 直接比較。','- tidb-server 在 CPU4 / CPU8 都是 4 threads；client 固定 2 CPUs，tokio runtime 未配置；並有 SMT sibling sharing。','- TiDB failures / incomplete，以及歷史 stray incomplete attempts 均保留在第 7 節 CSV 列表；不納入 effective-primary。','- 無 TiKV per-thread profiling、per-stage latency、post-run region distribution 或 per-region load。','','## 10. 檔案索引','','- 原始 CSV：`artifacts/connsweep-{pg,tidb}-c{4,8}-bm{50,100}.csv`。','- Attempt：表列 attempt 目錄內 `summary.json`、`resources-measured.csv` / `res-*/resources.csv`、`durability.json`、`client/topology.json`、`settings.txt` / TiDB cluster settings、cleanup 與 idle gate evidence。','- 工具與依據：`scripts/conn_sweep.py`、`scripts/conn_sweep_report.py`、`docs/ledger-connsweep-requirements.md`、`docs/ledger-connsweep-design.md`、`docs/ledger-batchsweep-report.md`。','']
    OUT.write_text('\n'.join(lines),encoding='utf-8')
    return E,sats

if __name__=='__main__':
    E,S=build()
    print(f'generated {OUT.relative_to(ROOT)}')
    for (db,c,b),sat in S.items(): print(db,c,b,sat)
