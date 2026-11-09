"""Per-second, dependency-free host/cgroup/process sampler with explicit raw counters and units."""
import csv, json, os, pathlib, re, threading, time, urllib.request
from datetime import datetime, timezone

MIB=1024*1024

def owned_resource(label_owner, expected_owner): return bool(label_owner) and label_owner == expected_owner
def cpu_core_equivalents(usage_usec_delta, elapsed_seconds):
    if elapsed_seconds <= 0: raise ValueError('elapsed_seconds must be positive')
    return usage_usec_delta / 1_000_000 / elapsed_seconds
def parse_cpu_stat(text): return {k:int(v) for k,v in (line.split() for line in text.splitlines()) if k in ('usage_usec','nr_periods','nr_throttled','throttled_usec')}
def cgroup_dir(container_id):
    roots=(pathlib.Path('/sys/fs/cgroup/docker')/container_id,pathlib.Path('/sys/fs/cgroup/system.slice')/f'docker-{container_id}.scope',pathlib.Path('/sys/fs/cgroup')/container_id)
    for p in roots:
        if (p/'cpu.stat').exists(): return p
    for root in (pathlib.Path('/sys/fs/cgroup/docker'),pathlib.Path('/sys/fs/cgroup/system.slice')):
        if root.exists():
            for p in root.glob(f'**/*{container_id}*'):
                if (p/'cpu.stat').exists(): return p
    raise FileNotFoundError(f'cgroup-v2 directory for {container_id} not found')
def proc_exit_confirmed(pid, proc_root='/proc'):
    """Return True only when /proc confirms the PID is gone or a zombie/dead task."""
    try:
        s=(pathlib.Path(proc_root)/str(pid)/'stat').read_text()
    except FileNotFoundError:
        return True
    except OSError:
        return False
    # comm is parenthesized and may itself contain spaces or ')'. State is the
    # first field after its final closing parenthesis; malformed/racing reads
    # are not evidence that the process exited.
    tail=s[s.rfind(')')+2:].split()
    return bool(tail) and tail[0] in ('Z','X')
def proc_cpu_ticks(pid):
    s=pathlib.Path(f'/proc/{pid}/stat').read_text(); tail=s[s.rfind(')')+2:].split(); return int(tail[11])+int(tail[12])
def proc_info(pid):
    s=pathlib.Path(f'/proc/{pid}/stat').read_text(); tail=s[s.rfind(')')+2:].split()
    stat=pathlib.Path(f'/proc/{pid}/status').read_text(); threads=int(re.search(r'^Threads:\s+(\d+)',stat,re.M).group(1))
    io={k:int(v) for k,v in (line.split(':') for line in pathlib.Path(f'/proc/{pid}/io').read_text().splitlines())}
    task=[]
    for tid in pathlib.Path(f'/proc/{pid}/task').iterdir():
        try:
            t=pathlib.Path(tid/'stat').read_text(); x=t[t.rfind(')')+2:].split(); task.append((tid.name,int(x[11])+int(x[12])))
        except (OSError,ValueError): pass
    return {'cpu_ticks':int(tail[11])+int(tail[12]),'rss_bytes':int(tail[21])*os.sysconf('SC_PAGE_SIZE'),'threads':threads,'io':io,'tasks':task}
def established_sockets(pid, ports):
    inodes=set()
    for fd in pathlib.Path(f'/proc/{pid}/fd').iterdir():
        try:
            link=os.readlink(fd)
            if link.startswith('socket:['): inodes.add(link[8:-1])
        except OSError: pass
    counts={str(p):0 for p in ports}
    for table in ('tcp','tcp6'):
        try: lines=pathlib.Path(f'/proc/{pid}/net/{table}').read_text().splitlines()[1:]
        except OSError: continue
        for line in lines:
            f=line.split()
            if len(f)>9 and f[3]=='01' and f[9] in inodes:
                port=int(f[2].rsplit(':',1)[1],16)
                if str(port) in counts: counts[str(port)]+=1
    return counts
def parse_metrics(text):
    out={}
    for line in text.splitlines():
        if not line or line.startswith('#'): continue
        m=re.match(r'([^ {]+)(\{[^}]*\})?\s+([-+0-9.eE]+)',line)
        if m:
            try: out[m.group(1)+(m.group(2) or '')]=float(m.group(3))
            except ValueError: pass
    return out
def diskstats():
    result={}
    for line in pathlib.Path('/proc/diskstats').read_text().splitlines():
        x=line.split(); name=x[2]
        if len(x)<14: continue
        vals=list(map(int,x[3:])); result[name]={'reads_completed':vals[0],'sectors_read':vals[2],'read_ms':vals[3],'writes_completed':vals[4],'sectors_written':vals[6],'write_ms':vals[7],'io_ms':vals[9],'weighted_io_ms':vals[10], 'flushes_completed':vals[15] if len(vals)>15 else None,'flush_ms':vals[16] if len(vals)>16 else None}
    return result
def filesystem_device(path):
    """Return mount source, filesystem and physical block ancestry from proc/sysfs."""
    mounts=pathlib.Path('/proc/self/mounts').read_text().splitlines(); best=None
    target=os.path.realpath(path)
    for line in mounts:
        f=line.split()
        if len(f)<3: continue
        mount=f[1].replace('\\040',' ')
        if target==mount or target.startswith(mount.rstrip('/')+'/'):
            if best is None or len(mount)>len(best[1]): best=(f[0],mount,f[2])
    if not best: return {'error':f'no mount found for {path}'}
    source,mount,fs=best; dev=pathlib.Path('/sys/class/block')/pathlib.Path(source).name
    if not dev.exists():
        dev=pathlib.Path('/sys/dev/block')
        try: dev=dev/str(os.stat(source).st_rdev)
        except OSError: return {'source':source,'mountpoint':mount,'filesystem':fs,'error':'block device mapping unavailable'}
    try:
        real=dev.resolve(); block=real.name
        if (real/'partition').exists(): block=real.parent.name
        if (real/'slaves').exists() and list((real/'slaves').iterdir()): block=sorted((real/'slaves').iterdir())[0].name
        return {'source':source,'mountpoint':mount,'filesystem':fs,'partition_device':pathlib.Path(source).name,'physical_device':block}
    except OSError as e: return {'source':source,'mountpoint':mount,'filesystem':fs,'error':str(e)}
def counter_delta(new,old):
    if new is None or old is None: return None
    d=new-old
    return d if d>=0 else None
def disk_interval_metrics(current,previous,elapsed):
    """Return physical-device rates; missing/reset counters are None and zero-op await is undefined."""
    if elapsed<=0: raise ValueError('elapsed must be positive')
    required=('sectors_read','sectors_written','reads_completed','writes_completed','read_ms','write_ms','io_ms','weighted_io_ms')
    missing=[k for k in required if k not in current or k not in previous]
    if missing: raise ValueError('required disk counters missing: '+','.join(missing))
    d={k:counter_delta(current.get(k),previous.get(k)) for k in ('sectors_read','sectors_written','reads_completed','writes_completed','read_ms','write_ms','io_ms','weighted_io_ms','flushes_completed','flush_ms')}
    read_ops=d['reads_completed']; write_ops=d['writes_completed']; flush_ops=d['flushes_completed']
    out={'read_mib_s':None if d['sectors_read'] is None else d['sectors_read']*512/MIB/elapsed,'write_mib_s':None if d['sectors_written'] is None else d['sectors_written']*512/MIB/elapsed,'read_iops':None if read_ops is None else read_ops/elapsed,'write_iops':None if write_ops is None else write_ops/elapsed,'read_ops':read_ops,'write_ops':write_ops,'read_await_ms':None if not read_ops else (None if d['read_ms'] is None else d['read_ms']/read_ops),'write_await_ms':None if not write_ops else (None if d['write_ms'] is None else d['write_ms']/write_ops),'util_pct':None if d['io_ms'] is None else min(100,d['io_ms']/(elapsed*10)),'avg_queue_depth':None if d['weighted_io_ms'] is None else d['weighted_io_ms']/(elapsed*1000)}
    if flush_ops is not None: out['flush_iops']=flush_ops/elapsed; out['flush_ops']=flush_ops
    if flush_ops and d['flush_ms'] is not None: out['flush_await_ms']=d['flush_ms']/flush_ops
    return out

class Sampler:
    def __init__(self,out,backend,container_id,pid,ports,allocated_cpus=3,client_cpus=3,interval=1.0,device='sdb'):
        self.out=pathlib.Path(out); self.backend=backend; self.container_id=container_id; self.pid=pid; self.ports=ports
        self.allocated_cpus=allocated_cpus; self.client_cpus=client_cpus; self.interval=interval; self.device=device
        self.stop_event=threading.Event(); self.errors=[]; self.thread=None; self.cg=None; self.rows=[]; self.prev=None
        self.metric_url='http://127.0.0.1:19181/metrics'; self.clock_ticks_rate=os.sysconf('SC_CLK_TCK')
    def scrape(self):
        with urllib.request.urlopen(self.metric_url,timeout=2) as r: return r.read().decode()
    def read_client_info(self,pid=None):
        pid=self.pid if pid is None else pid
        try: return proc_info(pid)
        except Exception:
            if proc_exit_confirmed(pid): return None
            raise
    def run(self):
        self.out.mkdir(parents=True,exist_ok=True)
        if self.backend=='scylla':
            try: (self.out/'server-metrics-baseline.raw.txt').write_text(self.scrape())
            except Exception as e: self.errors.append(f'baseline scrape: {e}')
        try: self.cg=cgroup_dir(self.container_id)
        except Exception as e: self.errors.append(f'cgroup lookup: {e}')
        fields=['monotonic','wall_utc','phase','allocated_cpu_capacity_cores','client_cpu_capacity_cores','db_cpu_usage_usec','db_nr_periods','db_nr_throttled','db_throttled_usec','memory_current_bytes','memory_stat_cache_bytes','memory_stat_anon_bytes','io_rbytes','io_wbytes','io_rios','io_wios','client_cpu_ticks','client_cpu_core_equivalents','client_rss_bytes','client_threads','client_io_rchar_bytes','client_io_wchar_bytes','client_io_read_bytes','client_io_write_bytes','client_worker_cpu_ticks_json','client_worker_cpu_json','client_sockets_json','host_diskstats_json','device_read_mib_s','device_write_mib_s','device_read_iops','device_write_iops','device_read_await_ms','device_write_await_ms','device_read_ops','device_write_ops','device_flush_ops','device_util_pct','device_avg_queue_depth','device_flush_iops','device_flush_await_ms','host_cpu_busy_pct','sampler_cpu_ticks','scylla_metrics_json']
        with (self.out/'resources.csv').open('w',newline='') as f:
            w=csv.DictWriter(f,fieldnames=fields); w.writeheader(); deadline=time.monotonic()
            while not self.stop_event.is_set():
                now=time.monotonic(); wall=datetime.now(timezone.utc).isoformat(); row={k:'' for k in fields}; row.update(monotonic=now,wall_utc=wall,phase='GO_to_client_exit_includes_verification_pending_interval_filter',allocated_cpu_capacity_cores=self.allocated_cpus,client_cpu_capacity_cores=self.client_cpus)
                try:
                    if not self.cg: raise RuntimeError('cgroup unavailable')
                    cs=parse_cpu_stat((self.cg/'cpu.stat').read_text()); mem={k:int(v) for k,v in (x.split() for x in (self.cg/'memory.stat').read_text().splitlines())}; io={}
                    for line in (self.cg/'io.stat').read_text().splitlines():
                        for item in line.split()[1:]: k,v=item.split('='); io[k]=io.get(k,0)+int(v)
                    row.update(db_cpu_usage_usec=cs['usage_usec'],db_nr_periods=cs.get('nr_periods',''),db_nr_throttled=cs.get('nr_throttled',''),db_throttled_usec=cs.get('throttled_usec',''),memory_current_bytes=int((self.cg/'memory.current').read_text()),memory_stat_cache_bytes=mem.get('file',''),memory_stat_anon_bytes=mem.get('anon',''),io_rbytes=io.get('rbytes',''),io_wbytes=io.get('wbytes',''),io_rios=io.get('rios',''),io_wios=io.get('wios',''))
                except Exception as e: self.errors.append(f'{wall} cgroup: {e}')
                p=None
                try:
                    p=self.read_client_info()
                    if p is not None: row.update(client_cpu_ticks=p['cpu_ticks'],client_rss_bytes=p['rss_bytes'],client_threads=p['threads'],client_io_rchar_bytes=p['io'].get('rchar'),client_io_wchar_bytes=p['io'].get('wchar'),client_io_read_bytes=p['io'].get('read_bytes'),client_io_write_bytes=p['io'].get('write_bytes'),client_worker_cpu_ticks_json=json.dumps(p['tasks'],sort_keys=True),client_worker_cpu_json='',client_sockets_json=json.dumps(established_sockets(self.pid,self.ports),sort_keys=True))
                except Exception as e: self.errors.append(f'{wall} client proc: {e}')
                try:
                    row['sampler_cpu_ticks']=proc_cpu_ticks(os.getpid())
                except Exception as e: self.errors.append(f'{wall} sampler proc: {e}')
                try:
                    ds=diskstats(); row['host_diskstats_json']=json.dumps(ds,sort_keys=True); dev=ds.get(self.device)
                    if dev is None: raise RuntimeError(f'required physical device {self.device} absent from /proc/diskstats')
                    stat=pathlib.Path('/proc/stat').read_text().splitlines()[0].split()[1:]; vals=list(map(int,stat)); host=(vals[3]+vals[4],sum(vals))
                    old=self.prev
                    if old:
                        dt=now-old['time']; pd=old['disk'].get(self.device)
                        if pd and dt>0:
                            metrics=disk_interval_metrics(dev,pd,dt)
                            for key,source in [('device_read_mib_s','read_mib_s'),('device_write_mib_s','write_mib_s'),('device_read_iops','read_iops'),('device_write_iops','write_iops'),('device_read_await_ms','read_await_ms'),('device_write_await_ms','write_await_ms'),('device_util_pct','util_pct'),('device_avg_queue_depth','avg_queue_depth'),('device_flush_iops','flush_iops'),('device_flush_await_ms','flush_await_ms')]: row[key]='' if metrics.get(source) is None else metrics[source]
                            row['device_read_ops']=metrics.get('read_ops',''); row['device_write_ops']=metrics.get('write_ops',''); row['device_flush_ops']=metrics.get('flush_ops','')
                        if old.get('client') and row.get('client_cpu_ticks') not in ('',None):
                            row['client_cpu_core_equivalents']=max(0,int(row['client_cpu_ticks'])-old['client']['cpu_ticks'])/self.clock_ticks_rate/(now-old['time'])
                            tasks={tid:ticks for tid,ticks in json.loads(row['client_worker_cpu_ticks_json'])}; prior=dict(old['client']['tasks']); row['client_worker_cpu_json']=json.dumps({tid:max(0,ticks-prior.get(tid,ticks))/self.clock_ticks_rate/(now-old['time']) for tid,ticks in tasks.items()},sort_keys=True)
                        if old.get('host'):
                            di=host[0]-old['host'][0]; total=host[1]-old['host'][1]; row['host_cpu_busy_pct']=100*(1-di/total) if total>0 and di>=0 else ''
                    self.prev={'time':now,'disk':ds,'host':host,'client':p,'sampler_ticks':row.get('sampler_cpu_ticks')}
                except Exception as e: self.errors.append(f'{wall} host/disk: {e}')
                if self.backend=='scylla':
                    try:
                        raw=self.scrape(); metrics=parse_metrics(raw)
                        row['scylla_metrics_json']=json.dumps({k:v for k,v in metrics.items() if any(x in k.lower() for x in ('reactor','commitlog','paxos','coordinator_cas','compaction','transport_cql_connections','transport_current_connections'))},sort_keys=True)
                        if len(self.rows)%10==0: (self.out/f'scylla-metrics-{len(self.rows):04d}.raw.txt').write_text(raw)
                    except Exception as e: self.errors.append(f'{wall} metrics scrape: {e}')
                w.writerow(row); f.flush(); self.rows.append(row); deadline+=self.interval; self.stop_event.wait(max(0,deadline-time.monotonic()))
        if self.backend=='scylla':
            try:
                end=self.scrape(); (self.out/'server-metrics-end.raw.txt').write_text(end); keys=parse_metrics(end)
                groups={'reactor_utilization':('reactor_utilization',),'commitlog':('commitlog',),'paxos':('coordinator_cas',),'compaction':('compaction',),'transport_connections':('transport_cql_connections','transport_current_connections')}
                missing=[name for name,terms in groups.items() if not any(any(term in key.lower() for term in terms) for key in keys)]
                if missing: self.errors.append('required Scylla metric groups missing: '+','.join(missing))
            except Exception as e: self.errors.append(f'end scrape: {e}')
        (self.out/'sampler-errors.json').write_text(json.dumps(self.errors,indent=2)+'\n')
    def start(self): self.thread=threading.Thread(target=self.run,daemon=True); self.thread.start()
    def stop(self):
        self.stop_event.set()
        if self.thread: self.thread.join(timeout=10)
        if self.thread and self.thread.is_alive(): self.errors.append('sampler thread did not stop')
        (self.out/'sampler-errors.json').write_text(json.dumps(self.errors,indent=2)+'\n')
        required=('cpu.stat','memory.current','memory.stat','io.stat')
        if self.cg and any(not (self.cg/x).exists() for x in required): raise RuntimeError('required cgroup metrics missing')
        if not self.rows: self.errors.append('no resource samples recorded')
        if self.rows and not any(r.get('client_rss_bytes') not in ('',None) for r in self.rows): self.errors.append('no valid client process RSS sample')
        if self.errors: raise RuntimeError('sampler errors: '+'; '.join(self.errors))
