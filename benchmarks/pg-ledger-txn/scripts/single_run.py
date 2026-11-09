#!/usr/bin/env python3
"""One disposable, pinned-database run of the actual ledger-bench release client."""
import argparse, csv, datetime, json, math, os, pathlib, signal, socket, subprocess, sys, time, uuid, urllib.request
from monitor import Sampler, parse_metrics, owned_resource, filesystem_device
from endpoints import PG_HOST, PG_PORT, PG_CONTAINER_PORT, PG_URL, SCYLLA_HOST, SCYLLA_PORT, SCYLLA_NATIVE_PORT
from resource_profiles import get_profile, validate_client_cpus, cpus_from_list

PG_IMAGE = 'postgres@sha256:afc7e2d441324c0388fa80c3d24f733b4194a4eb7f47dd8ee2b08eb1a24a647c'
SC_IMAGE = 'scylladb/scylla@sha256:333b9ab3a3214c7f98af100711c51df7f58964e8227e6d26050e889ebb216df1'
ROOT = pathlib.Path(__file__).resolve().parents[1]

def run(cmd, **kw):
    check=kw.pop('check',True)
    result=subprocess.run(cmd, check=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kw)
    if check and result.returncode:
        raise subprocess.CalledProcessError(result.returncode, cmd, output=result.stdout, stderr=result.stderr)
    if not check: return result
    return result.stdout + result.stderr


def listener_diagnostics(port):
    """Read-only listener census; never signals or modifies any process."""
    commands={
        'ss': ['ss','-H','-ltnp'],
        'docker_publishers': ['docker','ps','-a','--filter',f'publish={port}','--format','{{.ID}} {{.Names}} {{.Status}} {{.Ports}}'],
    }
    diagnostics={}
    for name,cmd in commands.items():
        try:
            r=subprocess.run(cmd,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,check=False)
            diagnostics[name]={'returncode':r.returncode,'stdout':r.stdout,'stderr':r.stderr}
        except OSError as e: diagnostics[name]={'error':str(e)}
    return diagnostics


def preflight_endpoint(out, port):
    census=listener_diagnostics(port)
    result={'host':PG_HOST,'port':port,'ephemeral_range':'32768-60999','outside_ephemeral_range':not 32768 <= port <= 60999,'listeners_before_bind':census}
    try:
        with socket.socket(socket.AF_INET,socket.SOCK_STREAM) as probe: probe.bind((PG_HOST,port))
        result.update(available=True,bind_error=None)
    except OSError as e:
        result.update(available=False,bind_error=f'{type(e).__name__}: {e}',listeners_on_bind_failure=listener_diagnostics(port))
    save(out/'endpoint-preflight.json',result)
    if not result['available']:
        raise RuntimeError(f'endpoint preflight bind failed on {PG_HOST}:{port}; see endpoint-preflight.json')

def stamp(): return datetime.datetime.now(datetime.timezone.utc).isoformat()
def save(path, data): path.write_text(data if isinstance(data, str) else json.dumps(data, indent=2) + '\n')
def docker(*args, **kw): return run(['docker', *map(str,args)], **kw)
def account_uuid(i): return str(uuid.UUID(int=(0x4c454447455242454e43480000000000 | (i+1))))
def docker_resource_assignment(host_config, quota, cpuset, memory_bytes):
    """Record and validate Docker's effective --cpus representation (NanoCpus)."""
    actual={'docker_cpuset_cpus':host_config.get('CpusetCpus'),'docker_nano_cpus':host_config.get('NanoCpus'),'docker_cpu_quota':host_config.get('CpuQuota'),'docker_cpu_period':host_config.get('CpuPeriod'),'docker_memory_bytes':host_config.get('Memory'),'expected_quota_cores':quota,'expected_nano_cpus':quota*1_000_000_000}
    if actual['docker_cpuset_cpus']!=cpuset or actual['docker_nano_cpus']!=actual['expected_nano_cpus'] or actual['docker_memory_bytes']!=memory_bytes:
        raise RuntimeError(f'effective Docker resource configuration mismatch: {actual}')
    return actual

def socket_counts(pid, target_port):
    """Count this client's established TCP sockets by remote DB port via fd inode mapping."""
    try:
        inodes = set()
        for fd in pathlib.Path(f'/proc/{pid}/fd').iterdir():
            try:
                link = os.readlink(fd)
                if link.startswith('socket:['): inodes.add(link[8:-1])
            except OSError: pass
        counts = {}
        for table in ('tcp','tcp6'):
            try: lines = pathlib.Path(f'/proc/{pid}/net/{table}').read_text().splitlines()[1:]
            except OSError: continue
            for line in lines:
                f = line.split()
                if len(f) < 10 or f[3] != '01' or f[9] not in inodes: continue
                port = int(f[2].rsplit(':',1)[1],16)
                counts[port] = counts.get(port,0)+1
        return {str(p): counts.get(p,0) for p in target_port}
    except (FileNotFoundError, PermissionError): return {str(p):0 for p in target_port}

def filter_resources(out, result):
    """Summarize only valid adjacent samples wholly inside the engine business window."""
    start=result['measurement_start_unix_ns']/1e9; end=result['measurement_end_unix_ns']/1e9
    # Scylla's per-shard metrics JSON can exceed csv's 128 KiB default.
    csv.field_size_limit(16 * 1024 * 1024)
    with (out/'resources.csv').open(newline='') as f: rows=list(csv.DictReader(f))
    points=[]
    for r in rows:
        try: points.append((datetime.datetime.fromisoformat(r['wall_utc']).timestamp(),float(r['monotonic']),r))
        except (ValueError,KeyError,TypeError): pass
    intervals=[]; missing=[]; ticks=os.sysconf('SC_CLK_TCK')
    for (t0,m0,a),(t1,m1,b) in zip(points,points[1:]):
        dt=m1-m0
        if not (t0>=start and t1<=end and dt>0 and abs((t1-t0)-dt)<=max(.05,dt*.05)): continue
        required=('db_cpu_usage_usec','client_cpu_ticks','client_rss_bytes','client_threads','client_io_read_bytes','client_io_write_bytes','sampler_cpu_ticks','client_worker_cpu_ticks_json','memory_current_bytes','client_cpu_capacity_cores','io_rbytes','io_wbytes','io_rios','io_wios')
        device_required=('device_read_mib_s','device_write_mib_s','device_read_iops','device_write_iops','device_util_pct','device_avg_queue_depth')
        if any(a.get(k) in ('',None) or b.get(k) in ('',None) for k in required) or b.get('client_worker_cpu_json') in ('',None) or any(b.get(k) in ('',None) for k in device_required):
            missing.append({'start':t0,'end':t1,'reason':'required cumulative counter/memory sample missing'}); continue
        def delta(key):
            d=int(b[key])-int(a[key]); return d if d>=0 else None
        du=delta('db_cpu_usage_usec'); ct=delta('client_cpu_ticks'); read=delta('client_io_read_bytes'); write=delta('client_io_write_bytes')
        dbread=delta('io_rbytes'); dbwrite=delta('io_wbytes'); dbrios=delta('io_rios'); dbwios=delta('io_wios')
        sampler_ticks=delta('sampler_cpu_ticks')
        if None in (du,ct,read,write,dbread,dbwrite,dbrios,dbwios,sampler_ticks): missing.append({'start':t0,'end':t1,'reason':'counter reset'}); continue
        cpu=du/1e6/dt; cap=float(b.get('allocated_cpu_capacity_cores') or 0)
        client_cap=float(b.get('client_cpu_capacity_cores') or 0)
        if cap<=0 or client_cap<=0: missing.append({'start':t0,'end':t1,'reason':'allocated DB/client CPU capacity missing'}); continue
        entry={'start_unix_seconds':t0,'end_unix_seconds':t1,'elapsed_seconds':dt,'allocated_cpu_capacity_cores':cap,'db_cpu_core_equivalents':cpu,'db_normalized_cpu_pct_of_allocated_capacity':cpu/cap*100,'db_memory_current_bytes':int(b['memory_current_bytes']),'db_cgroup_read_mib_s':dbread/1048576/dt,'db_cgroup_write_mib_s':dbwrite/1048576/dt,'db_cgroup_read_iops':dbrios/dt,'db_cgroup_write_iops':dbwios/dt,'client_cpu_core_equivalents':ct/ticks/dt,'client_cpu_normalized_pct_of_allocated_capacity':ct/ticks/dt/client_cap*100,'client_cpu_thread_core_equivalents_json':b.get('client_worker_cpu_json','{}'),'client_rss_bytes':int(b['client_rss_bytes']) if b.get('client_rss_bytes') else None,'client_physical_read_bytes':read,'client_physical_write_bytes':write,'client_read_mib_s':read/1048576/dt,'client_write_mib_s':write/1048576/dt,'sampler_cpu_core_equivalents':sampler_ticks/ticks/dt}
        for ops,await_field in (('device_read_ops','device_read_await_ms'),('device_write_ops','device_write_await_ms')):
            if b.get(ops) not in ('',None) and float(b[ops])>0 and b.get(await_field) in ('',None):
                missing.append({'start':t0,'end':t1,'reason':f'{await_field} missing/reset despite positive operations'}); entry=None; break
        if entry is None: continue
        entry['device_read_ops']=b.get('device_read_ops',''); entry['device_write_ops']=b.get('device_write_ops',''); entry['device_flush_ops']=b.get('device_flush_ops','')
        for field in ('device_read_mib_s','device_write_mib_s','device_read_iops','device_write_iops','device_read_await_ms','device_write_await_ms','device_util_pct','device_avg_queue_depth','device_flush_iops','device_flush_await_ms'):
            entry[field]=b.get(field,'')
        intervals.append(entry)
    fields=list(dict.fromkeys(k for x in intervals for k in x)) or ['start_unix_seconds','end_unix_seconds','elapsed_seconds']
    with (out/'resources-measured.csv').open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=fields); w.writeheader(); w.writerows(intervals)
    duration=sum(x['elapsed_seconds'] for x in intervals)
    means={k:(sum(float(x[k])*x['elapsed_seconds'] for x in intervals)/duration if duration and all(x.get(k) not in ('',None) for x in intervals) else None) for k in ('db_cpu_core_equivalents','db_normalized_cpu_pct_of_allocated_capacity','client_cpu_core_equivalents','client_read_mib_s','client_write_mib_s','db_cgroup_read_mib_s','db_cgroup_write_mib_s','db_cgroup_read_iops','db_cgroup_write_iops','device_read_mib_s','device_write_mib_s','device_read_iops','device_write_iops','device_read_await_ms','device_write_await_ms','device_util_pct','device_avg_queue_depth','sampler_cpu_core_equivalents')}
    for prefix in ('read','write','flush'):
        ops_key=f'device_{prefix}_ops'; await_key=f'device_{prefix}_await_ms'
        weighted=sum(float(x[ops_key])*float(x[await_key]) for x in intervals if x.get(ops_key) not in ('',None) and float(x[ops_key])>0 and x.get(await_key) not in ('',None))
        total_ops=sum(float(x[ops_key]) for x in intervals if x.get(ops_key) not in ('',None) and float(x[ops_key])>0 and x.get(await_key) not in ('',None))
        means[await_key]=weighted/total_ops if total_ops else None
    means['cpu_core_equivalents_mean']=means.get('db_cpu_core_equivalents')
    means['client_cpu_core_equivalents_mean']=means.get('client_cpu_core_equivalents')
    memory=[int(r['memory_current_bytes']) for t,_,r in points if start<=t<=end and r.get('memory_current_bytes')]
    formal=bool(intervals and memory and not missing)
    save(out/'resource-summary.json',{'schema_version':2,'units':{'cpu':'core-equivalents','cpu_normalized':'percent of allocated CPU capacity','io_rate':'MiB/s','io_ops':'operations/s','await':'ms; null when no operations','device_util':'percent','queue_depth':'average requests'},'formal':formal,'scope':'measured business window only','window_start_unix_ns':result['measurement_start_unix_ns'],'window_end_unix_ns':result['measurement_end_unix_ns'],'window_duration_seconds':end-start,'cpu_interval_coverage_seconds':duration,'cpu_intervals':len(intervals),'missing_intervals':missing,'verification_tail_included':False,'allocated_cpu_capacity_cores':float(intervals[0]['allocated_cpu_capacity_cores']) if intervals else None,'cpu_core_equivalents_mean':means.get('cpu_core_equivalents_mean'),'client_cpu_core_equivalents_mean':means.get('client_cpu_core_equivalents_mean'),'means':means,'db_memory_current_bytes_max':max(memory,default=None),'client_rss_bytes_max':max((int(r['client_rss_bytes']) for t,_,r in points if start<=t<=end and r.get('client_rss_bytes')),default=None),'claim':'valid measured resource window' if formal else 'nonformal: missing or reset required measured-window data'})
    return formal

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('backend',choices=['pg','scylla']); ap.add_argument('N',type=int); ap.add_argument('K',type=int); ap.add_argument('output',type=pathlib.Path)
    ap.add_argument('--cpu-profile',choices=['3','6'],default='3')
    ap.add_argument('--diagnostic-multiplier',type=int,default=1); ap.add_argument('--client-cpus'); ap.add_argument('--workers',type=int)
    ap.add_argument('--pg-data-pool-size',type=int,default=96)
    ap.add_argument('--warmup-requests-per-user',type=int,default=20)
    ap.add_argument('--startup-timeout',type=int,default=600); ap.add_argument('--socket-timeout',type=int,default=30)
    a=ap.parse_args()
    if a.N<1 or a.K<1 or a.diagnostic_multiplier<1 or (a.workers is not None and a.workers<1) or a.pg_data_pool_size<1 or a.warmup_requests_per_user<1: ap.error('N,K,multiplier,workers,pool size,warmup requests must be positive')
    profile=get_profile(a.cpu_profile)
    a.client_cpus=a.client_cpus or profile.client_cpus; a.workers=a.workers or profile.workers
    try:
        validate_client_cpus(profile,a.client_cpus)
        sampler_cpu_set=cpus_from_list(profile.sampler_cpus)
        os.sched_setaffinity(0,sampler_cpu_set)
    except (AttributeError,OSError,ValueError) as e: ap.error(f'invalid/unsupported resource placement: {e}')
    out=a.output.resolve(); out.mkdir(parents=True,exist_ok=False)
    token=uuid.uuid4().hex; prefix='ledger-g2d-'+token[:12]; names={k:prefix+'-'+k for k in ('db','vol','net')}
    for kind,name in names.items():
        # Randomized names plus explicit refusal prevent collisions; never clean an unowned resource.
        cmd = {'db':['inspect'], 'vol':['volume','inspect'], 'net':['network','inspect']}[kind]
        probe = subprocess.run(['docker', *cmd, name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if probe.returncode == 0: raise RuntimeError(f'resource collision refused: {name}')
    owner=token; labels=['--label','ledger.gate=2d','--label',f'ledger.gate.owner={owner}']
    container=None; proc=None; sampler=None; status=1; running=False; signal_received=None
    def owned(resource,kind):
        try:
            key={'container':'{{ index .Config.Labels "ledger.gate.owner" }}','volume':'{{ index .Labels "ledger.gate.owner" }}','network':'{{ index .Labels "ledger.gate.owner" }}'}[kind]
            return owned_resource(docker('inspect','-f',key,resource).strip(),owner)
        except Exception:return False
    def on_signal(sig,frame):
        nonlocal signal_received
        signal_received=sig
        if proc and proc.poll() is None: proc.send_signal(sig)
        raise KeyboardInterrupt(f'signal {sig}')
    old={s:signal.signal(s,on_signal) for s in (signal.SIGINT,signal.SIGTERM)}
    try:
        (out/'run.json').write_text(json.dumps({'backend':a.backend,'N':a.N,'K':a.K,'owner':owner,'started_at':stamp(),'command_args':sys.argv,'measurement_scope':'GO through client exit; includes verification-phase tail pending engine interval timestamps','formal_cpu_claim':'not clean formal interval; verification filtering pending','cpu_profile':a.cpu_profile,'db_cpus':profile.db_cores,'db_cpu_quota':profile.quota,'expected_shards':profile.shards,'sampler_cpus':profile.sampler_cpus,'wrapper_cpus':profile.sampler_cpus,'client_cpus':a.client_cpus,'client_workers':a.workers,'pg_data_pool_size':a.pg_data_pool_size},indent=2)+'\n')
        save(out/'machine.json',{'hostname':socket.gethostname(),'platform':sys.platform,'cpu_affinity':sorted(os.sched_getaffinity(0)),'cpuinfo':pathlib.Path('/proc/cpuinfo').read_text(),'cgroup_mount':str(pathlib.Path('/sys/fs/cgroup'))})
        docker('network','create',*labels,names['net'])
        docker('volume','create',*labels,names['vol'])
        container=prefix+'-db'
        if a.backend=='pg':
            image=PG_IMAGE; port=PG_PORT
            preflight_endpoint(out,port)
            docker('run','-d','--name',prefix+'-db',*labels,'--network',names['net'],'--cpuset-cpus',profile.db_cores,'--cpus',str(profile.quota),'--memory','8g','-p',f'{PG_HOST}:{PG_PORT}:{PG_CONTAINER_PORT}','-e','POSTGRES_PASSWORD=gate2d','-e','POSTGRES_DB=ledger','-v',names['vol']+':/var/lib/postgresql',image,'postgres','-c','shared_buffers=2GB','-c','fsync=on','-c','synchronous_commit=on','-c','full_page_writes=on','-c','commit_delay=1000','-c','commit_siblings=0','-c','max_connections=220','-c','work_mem=4MB')
        else:
            image=SC_IMAGE; port=SCYLLA_PORT
            docker('run','-d','--name',prefix+'-db',*labels,'--network',names['net'],'--cpuset-cpus',profile.db_cores,'--cpus',str(profile.quota),'--memory','8g','-p',f'{SCYLLA_HOST}:{SCYLLA_NATIVE_PORT}:{SCYLLA_NATIVE_PORT}','-p',f'{SCYLLA_HOST}:{SCYLLA_PORT}:{SCYLLA_PORT}','-p','127.0.0.1:19181:9180','-v',names['vol']+':/var/lib/scylla','-v',str(ROOT/'config/scylla.yaml')+':/etc/scylla/scylla.yaml:ro',image,'--smp',str(profile.shards),'--memory','6G','--overprovisioned','0','--io-setup','0','--listen-address','0.0.0.0','--rpc-address','0.0.0.0')
        deadline=time.monotonic()+a.startup_timeout; ready=False
        while time.monotonic()<deadline:
            try:
                cmd=['docker','exec',container,'pg_isready','-h','127.0.0.1','-U','postgres','-d','ledger'] if a.backend=='pg' else ['docker','exec',container,'cqlsh','-e','DESCRIBE CLUSTER']
                if subprocess.run(cmd,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=max(.001,min(10,deadline-time.monotonic()))).returncode==0: ready=True; break
            except (OSError,subprocess.TimeoutExpired): pass
            time.sleep(min(2,max(0,deadline-time.monotonic())))
        if not ready: raise RuntimeError(f'{a.backend} readiness timed out ({a.startup_timeout}s)')
        inspect=json.loads(docker('inspect',container)); save(out/'inspect-start.json',inspect)
        c=inspect[0]['HostConfig']; actual={'cpu_profile':a.cpu_profile,'expected_shards':profile.shards,**docker_resource_assignment(c,profile.quota,profile.db_cores,8589934592),'sampler_cpus':profile.sampler_cpus,'client_cpus':a.client_cpus,'client_workers':a.workers,'data_filesystem':filesystem_device('/var/lib/docker'),'docker_filesystem':filesystem_device('/var/lib/docker'),'expected_physical_device':'sdb'}
        save(out/'resource-placement.json',actual)
        if actual['data_filesystem'].get('physical_device')!='sdb' or actual['docker_filesystem'].get('physical_device')!='sdb': raise RuntimeError(f'backing physical-device mapping differs from required sdb: {actual}')
        save(out/'server-start.log',docker('logs',container))
        if a.backend=='pg':
            settings="SELECT name,setting,unit,source FROM pg_settings WHERE name IN ('shared_buffers','fsync','synchronous_commit','full_page_writes','commit_delay','commit_siblings','max_connections','work_mem') ORDER BY name;"
            save(out/'settings.txt',docker('exec',container,'psql','-X','-At','-U','postgres','-d','ledger','-c',settings))
        else:
            settings="SELECT name,source,value FROM system.config WHERE name IN ('commitlog_sync','commitlog_sync_batch_window_in_ms','broadcast_rpc_address','rpc_address','native_transport_port','native_shard_aware_transport_port');"
            save(out/'settings.txt',docker('exec',container,'cqlsh','-e',settings)+'\n'+docker('exec',container,'cqlsh','-e',"SELECT keyspace_name,durable_writes,replication FROM system_schema.keyspaces WHERE keyspace_name='ledger_bench';"))
        binary=ROOT/'target/release/ledger-bench'
        run(['cargo','build','--manifest-path',str(ROOT/'Cargo.toml'),'--locked','--offline','--release','--target-dir',str(ROOT/'target')],cwd=ROOT)
        readyfile=out/'client-ready.json'; gofile=out/'GO'
        host=PG_URL if a.backend=='pg' else None
        cmd=[str(binary),'--backend',a.backend,'--users',str(a.N),'--requests-per-user',str(a.K),'--output-dir',str(out/'client'),'--client-workers',str(a.workers),'--expected-shards',str(profile.shards),'--ready-file',str(readyfile),'--go-file',str(gofile),'--handshake-timeout-ms','600000','--setup-timeout-ms','300000']
        cmd += ['--pg-data-pool-size',str(a.pg_data_pool_size),'--warmup-requests-per-user',str(a.warmup_requests_per_user)]
        if host: cmd += ['--pg-url',host]
        else: cmd += ['--scylla-url',f'{SCYLLA_HOST}:{SCYLLA_PORT}','--diagnostic-multiplier',str(a.diagnostic_multiplier)]
        proc=subprocess.Popen(['taskset','-c',a.client_cpus,*cmd],cwd=ROOT,stdout=open(out/'client.stdout','w'),stderr=subprocess.STDOUT)
        deadline=time.monotonic()+600
        while not readyfile.exists() and proc.poll() is None and time.monotonic()<deadline: time.sleep(.1)
        if not readyfile.exists(): raise RuntimeError('client failed to initialize/pool before ready handshake; see client.stdout and invalid.json')
        placement=json.loads((out/'resource-placement.json').read_text()); placement.update(sampler_actual_affinity_cpus=sorted(os.sched_getaffinity(0)),client_actual_affinity_cpus=sorted(os.sched_getaffinity(proc.pid))); save(out/'resource-placement.json',placement)
        if a.backend=='scylla':
            runtime='=== initialized workload keyspace durability ===\n'+docker('exec',container,'cqlsh','-e',"SELECT keyspace_name,durable_writes,replication FROM system_schema.keyspaces WHERE keyspace_name='ledger_bench';")
            runtime+='\n=== client consistency/retry policy (fixed source configuration) ===\nread/write consistency=LOCAL_QUORUM; serial consistency=LOCAL_SERIAL; speculative execution=disabled; automatic retry policy=disabled\n'
            save(out/'settings-runtime.txt',runtime)
            # Scrape the native Scylla Prometheus endpoint while the client pool is live; preserve raw exposition.
            with urllib.request.urlopen('http://127.0.0.1:19181/metrics',timeout=5) as response: metrics=response.read().decode('utf-8')
            save(out/'server-metrics.raw.txt',metrics)
            save(out/'server-metrics-connections.txt','\n'.join(line for line in metrics.splitlines() if 'connection' in line.lower() and ('shard' in line.lower() or 'cql' in line.lower()))+'\n')
            parsed=parse_metrics(metrics)
            shard={k:v for k,v in parsed.items() if k.startswith('scylla_transport_current_connections{') and 'shard=' in k}
            if len(shard)<profile.shards: raise RuntimeError(f'required Scylla per-shard current-connection metrics missing: expected {profile.shards}, observed {len(shard)}')
            ready=json.loads(readyfile.read_text()); observed=int(ready.get('diagnostics',{}).get('driver_shards','0'))
            if observed != profile.shards: raise RuntimeError(f'driver shard mismatch: expected {profile.shards}, observed {observed}')
            save(out/'scylla-ready-connections.json',{'at':stamp(),'expected_shards':profile.shards,'observed_shards':observed,'connections_per_shard':max(1,math.ceil(a.N/(10*profile.shards)))*a.diagnostic_multiplier,'per_shard_current_connections':shard})
        expected_data=a.pg_data_pool_size if a.backend=='pg' else profile.shards*a.diagnostic_multiplier*max(1,math.ceil(a.N/(10*profile.shards)))
        # Scylla 0.15 creates a separate control pool from the initial known-node contact point;
        # because that point is explicitly 127.0.0.1:19042, port alone cannot classify its role.
        # Source evidence is captured with each run; target count is data pool plus that control connection.
        expected_control=0 if a.backend=='pg' else 1
        save(out/'client-sockets.json',{'expected_data':expected_data,'expected_control':expected_control,'expected_total':expected_data+expected_control,'observations':[]})
        target=[PG_PORT] if a.backend=='pg' else [SCYLLA_PORT,SCYLLA_NATIVE_PORT]
        observations=[]; previous=None; stable=0; deadline=time.monotonic()+a.socket_timeout
        while time.monotonic()<deadline:
            counts=socket_counts(proc.pid,target); observations.append({'at':stamp(),'counts_by_remote_port':counts})
            expected=expected_data+expected_control
            good=(counts.get(str(port),0)==expected if a.backend=='pg' else counts.get(str(SCYLLA_PORT),0)==expected and counts.get(str(SCYLLA_NATIVE_PORT),0)==0)
            stable=stable+1 if good and counts==previous else (1 if good else 0); previous=counts
            if stable>=3: break
            time.sleep(.5)
        save(out/'socket-snapshots.json',{'expected_data':expected_data,'expected_control':expected_control,'expected_total':expected,'port_roles':{str(SCYLLA_PORT):'shard-aware data pool plus Scylla 0.15 control connection (same endpoint; role counts inferred from source and total corroborated by OS census)',str(SCYLLA_NATIVE_PORT):'native port; socket presence is not itself proof of control role',str(PG_PORT):'PostgreSQL data'},'observations':observations,'stable_samples':stable})
        if stable<3:
            if a.backend=='scylla':
                raise RuntimeError(f'Scylla socket readiness blocker: require {expected_data} configured shard-aware data sockets plus 1 source-proven control connection (total {expected}) on 19042; observed {observations[-1]["counts_by_remote_port"] if observations else {}}')
            raise RuntimeError(f'socket readiness failed: expected {expected} sockets on {port}, observed {observations[-1]["counts_by_remote_port"] if observations else {}}')
        save(out/'run.json',{'backend':a.backend,'N':a.N,'K':a.K,'owner':owner,'ready_at':stamp(),'expected_total':a.N*a.K,'command':cmd,'cpu_profile':a.cpu_profile,'db_cpus':profile.db_cores,'db_cpu_quota':profile.quota,'expected_shards':profile.shards,'client_cpus':a.client_cpus,'client_cpu_capacity_cores':len(validate_client_cpus(profile,a.client_cpus)),'sampler_cpus':profile.sampler_cpus,'workers':a.workers,'diagnostic_multiplier':a.diagnostic_multiplier,'connections_per_shard':max(1,math.ceil(a.N/(10*profile.shards)))*a.diagnostic_multiplier})
        save(out/'started_at.json',{'started_at':stamp()})
        sampler=Sampler(out,a.backend,docker('inspect','-f','{{.Id}}',container).strip(),proc.pid,target,allocated_cpus=profile.quota,client_cpus=len(validate_client_cpus(profile,a.client_cpus)),device='sdb')
        go_wall=stamp(); go_mono=time.monotonic(); gofile.write_text(go_wall+'\n')
        save(out/'phase-timestamps.json',{'GO':{'wall_utc':go_wall,'monotonic':go_mono},'measurement_scope':'sampler begins at GO and ends at client exit; verification interval filtering pending'})
        sampler.start()
        status=proc.wait(); exit_wall=stamp(); exit_mono=time.monotonic(); sampler.stop()
        save(out/'phase-timestamps.json',{'GO':{'wall_utc':go_wall,'monotonic':go_mono},'client_exit':{'wall_utc':exit_wall,'monotonic':exit_mono},'measurement_scope':'sampler begins at GO and ends at client exit; verification interval filtering pending'})
        save(out/'completed_at.json',{'completed_at':exit_wall,'client_exit_status':status})
        if status: raise RuntimeError(f'client exited {status}; see client.stdout')
        filter_resources(out, json.loads((out/'client/result.json').read_text()))
        # Independent server-side checks before deleting the disposable DB.
        if a.backend=='pg':
            sql="SELECT a.account_id,a.balance,count(l.transaction_id)::bigint,coalesce(sum(l.amount),0)::bigint FROM accounts a LEFT JOIN ledger l USING(account_id) GROUP BY a.account_id,a.balance ORDER BY a.account_id;"
            raw=docker('exec',container,'psql','-X','-At','-F','|','-U','postgres','-d','ledger','-c',sql)
            save(out/'independent-query.sql',sql+'\n'); save(out/'independent-accounts.txt',raw)
            totals_sql='SELECT count(*),coalesce(sum(balance),0),coalesce(sum(x.cnt),0),coalesce(sum(x.amt),0) FROM (SELECT a.balance,count(l.transaction_id) cnt,coalesce(sum(l.amount),0) amt FROM accounts a LEFT JOIN ledger l USING(account_id) GROUP BY a.account_id,a.balance) x;'
            totals=docker('exec',container,'psql','-X','-At','-F','|','-U','postgres','-d','ledger','-c',totals_sql)
            save(out/'independent-totals-query.sql',totals_sql+'\\n')
            # account rows above are canonical per-account proof; validate every expected record
            lines=[x.split('|') for x in raw.splitlines() if x]
            expected=[(account_uuid(i),a.K,a.K,a.K) for i in range(a.N)]
            actual=[(r[0],int(r[1]),int(r[2]),int(r[3])) for r in lines]
            if sorted(actual)!=sorted(expected): raise RuntimeError(f'independent PostgreSQL verification mismatch: expected {expected}, actual {actual}')
            save(out/'independent-totals.txt',totals)
        else:
            cql="PAGING OFF;\nSELECT DISTINCT account_id FROM ledger;"
            ids=docker('exec',container,'cqlsh','--keyspace','ledger_bench','-e',cql)
            save(out/'independent-query.cql',cql+'\n')
            save(out/'independent-account-set.raw.txt',ids)
            queries=[f"SELECT account_id,balance,count(transaction_id),sum(amount) FROM ledger WHERE account_id={account_uuid(i)} AND transaction_id>00000000-0000-0000-0000-000000000000;" for i in range(a.N)]
            script='PAGING OFF;\n'+'\n'.join(queries)+'\n'
            save(out/'independent-query.cql',script)
            result=subprocess.run(['docker','exec','-i',container,'cqlsh','--keyspace','ledger_bench','--request-timeout=60','--connect-timeout=10'],input=script,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
            raw=result.stdout; save(out/'independent-accounts.raw.txt',raw)
            if result.returncode: raise RuntimeError(f'batched Scylla cqlsh failed ({result.returncode}): {raw}')
            expected={account_uuid(i):(a.K,a.K,a.K) for i in range(a.N)}; actual={}
            for line in raw.splitlines():
                if '|' not in line: continue
                fields=[part.strip() for part in line.split('|')]
                if len(fields)==4:
                    try:
                        aid=str(uuid.UUID(fields[0])); actual.setdefault(aid,[]).append(tuple(int(v) for v in fields[1:]))
                    except ValueError: pass
            rows=[{'account_id':aid,'query':queries[i],'expected':list(values),'actual':actual.get(aid,[])} for i,(aid,values) in enumerate(expected.items())]
            save(out/'independent-accounts.json',rows)
            if set(actual)!=set(expected) or any(actual.get(k)!=[v] for k,v in expected.items()):
                raise RuntimeError(f'independent Scylla aggregate/account-set mismatch: expected {expected}, actual {actual}; raw saved')
            expected_ids=set(expected)
            actual_ids=set()
            for line in ids.splitlines():
                for token in line.replace('|',' ').split():
                    try: actual_ids.add(str(uuid.UUID(token)))
                    except ValueError: pass
            if actual_ids != expected_ids:
                raise RuntimeError(f'independent Scylla account-set mismatch: expected {sorted(expected_ids)}, actual {sorted(actual_ids)}; raw saved')
        save(out/'verification-status.json',{'valid':True,'N':a.N,'K':a.K,'expected_total':a.N*a.K,'independent_account_count':a.N})
    except BaseException as e:
        save(out/'failure.txt',f'{type(e).__name__}: {e}\n');
        if isinstance(e,subprocess.CalledProcessError):
            save(out/'failure-command.json',{'returncode':e.returncode,'cmd':e.cmd,'stdout':e.stdout or e.output or '', 'stderr':e.stderr or ''})
            if a.backend=='pg' and 'docker' in str(e.cmd) and 'run' in e.cmd:
                save(out/'bind-failure-diagnostics.json',{'port':PG_PORT,'listeners':listener_diagnostics(PG_PORT)})
        status=1
        if isinstance(e,KeyboardInterrupt): pass
    finally:
        try:
            if sampler and sampler.thread and sampler.thread.is_alive(): sampler.stop()
        except Exception as e: save(out/'sampler-stop-error.txt',str(e)+'\n'); status=1
        try:
            if proc and proc.poll() is None:
                if signal_received is not None:
                    try: proc.wait(timeout=30)
                    except subprocess.TimeoutExpired: proc.terminate(); proc.wait(timeout=10)
                else:
                    proc.terminate(); proc.wait(timeout=10)
        except Exception:
            if proc: proc.kill(); proc.wait()
        cleanup_errors=[]
        if container and owned(container,'container'):
            try: save(out/'server-final.log',docker('logs',container))
            except Exception as e: cleanup_errors.append(f'capture container logs: {e}')
            try: save(out/'inspect-final.json',json.loads(docker('inspect',container)))
            except Exception as e: cleanup_errors.append(f'inspect container: {e}')
            try: docker('rm','-f',container)
            except Exception as e: cleanup_errors.append(f'remove container: {e}')
        for kind in ('vol','net'):
            name=names[kind]
            resource_kind='volume' if kind=='vol' else 'network'
            try:
                if owned(name,resource_kind): docker(resource_kind,'rm',name)
            except Exception as e: cleanup_errors.append(f'remove {resource_kind} {name}: {e}')
        residue=[]
        for label, name, kind in [('container',prefix+'-db','container'),('volume',names['vol'],'volume'),('network',names['net'],'network')]:
            residue.append({'kind':label,'name':name,'exists':owned(name,kind)})
        save(out/'cleanup.json',{'at':stamp(),'residue':residue,'errors':cleanup_errors,'signal':signal_received,'status':'failed' if cleanup_errors or any(item['exists'] for item in residue) else 'success'})
        if cleanup_errors or any(item['exists'] for item in residue): status=1
        for s,h in old.items(): signal.signal(s,h)
    if status: return 1
    return 0
if __name__=='__main__': sys.exit(main())
