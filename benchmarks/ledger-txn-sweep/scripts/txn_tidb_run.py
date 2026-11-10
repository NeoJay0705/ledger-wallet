#!/usr/bin/env python3
"""Disposable, resource-pinned TiDB runner for txn_batch_tidb."""
import argparse
import csv
import datetime
import json
import math
import os
import pathlib
import signal
import socket
import subprocess
import sys
import time
import uuid
import urllib.request

import monitor
import single_run
from batch_queue_run import NetSampler, net_rate_summary, parse_net_dev
from resource_profiles import cpus_from_list

ROOT = single_run.ROOT
IMAGE_PD = 'pingcap/pd:v8.5.6'
IMAGE_TIKV = 'pingcap/tikv:v8.5.6'
IMAGE_TIDB = 'pingcap/tidb:v8.5.6'
PROFILES = {
    4: {'tikv': ['0-3'], 'tidb': '6-7,12-13', 'client': '4-5', 'sampler': '14-15'},
    8: {'tikv': ['0-3', '4-7'], 'tidb': '10-13', 'client': '8-9', 'sampler': '14-15'},
}
PORTS = (14000, 20180, 20181)


def select_profile(cpus, extended=False, tikv_cpus=None):
    if tikv_cpus == 6 and (cpus != 8 or not extended):
        raise ValueError('--tikv-cpus 6 requires --cpus 8 and --tidb-extended')
    if extended and cpus != 8:
        raise ValueError('--tidb-extended is valid only with --cpus 8')
    if tikv_cpus == 6:
        return {'tikv': ['0-2', '4-6'], 'tidb': '3,7,10-15', 'client': '8-9', 'sampler': '8-9'}
    if extended:
        return {'tikv': ['0-3', '4-7'], 'tidb': '10-15', 'client': '8-9', 'sampler': '8-9'}
    return PROFILES[cpus]


def tikv_quota_per_store(tikv_cpus, number_of_stores):
    # --tikv-cpus 6 is a TOTAL split across stores; the default (None/4) keeps the historical 4 CPUs per store.
    return tikv_cpus // number_of_stores if tikv_cpus == 6 else 4


def tikv_run_args(cpuset, quota):
    return ['--cpuset-cpus', cpuset, '--cpus', str(quota)]


def tikv_cpu_pct_of_quota(cpu_cores, tikv_cpus_total):
    return cpu_cores / tikv_cpus_total * 100
METRICS = ('raft_engine_sync_log_duration_seconds_count', 'raft_engine_write_duration_seconds_count',
           'raft_engine_write_size_sum', 'raft_engine_sync_log_duration_seconds_sum')


def resolve_connections(connections, queues):
    connections = queues if connections is None else connections
    if not 1 <= connections <= queues:
        raise ValueError('--connections must be between 1 and --queues')
    return connections


def expected_socket_count(connections):
    return connections + 1


def tidb_client_command(binary, users, requests_per_user, queues, connections, batch_max,
                        sample_interval_ms, clientdir, ready, go, stores, retry_pct, inject_rejects=False):
    cmd = [str(binary), '--mysql-url', 'mysql://root@127.0.0.1:14000',
           '--users', str(users), '--requests-per-user', str(requests_per_user),
           '--queues', str(queues), '--connections', str(connections), '--batch-max', str(batch_max),
           '--request-timeout-ms', '30000', '--sample-interval-ms', str(sample_interval_ms),
           '--output-dir', str(clientdir), '--ready-file', str(ready), '--go-file', str(go),
           '--handshake-timeout-ms', '600000', '--stores', str(stores), '--retry-pct', str(retry_pct)]
    if inject_rejects:
        cmd.append('--inject-rejects')
    return cmd


def partition_client_exit_errors(samplers, client_pid, exit_time):
    """Remove only narrowly-scoped sampler /proc/io permission races after client exit."""
    if isinstance(exit_time, str):
        exit_time = datetime.datetime.fromisoformat(exit_time.replace('Z', '+00:00'))
    tolerated = []
    cutoff = exit_time - datetime.timedelta(seconds=2)
    for sampler in samplers:
        remaining = []
        for error in sampler.errors:
            timestamp = error.split(' ', 1)[0]
            try:
                at = datetime.datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            except ValueError:
                at = None
            matches = (f' client proc: ' in error and 'Permission denied' in error
                       and f'/proc/{client_pid}/io' in error and at is not None
                       and at >= cutoff)
            if matches:
                tolerated.append({'sampler dir': str(sampler.out), 'error': error})
            else:
                remaining.append(error)
        sampler.errors[:] = remaining
    return tolerated


def metric_value(text, name):
    """Sum samples whose Prometheus metric name exactly matches name."""
    values = []
    invalid = False
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        fields = line.split()
        if len(fields) < 2:
            continue
        sample = fields[0].split('{', 1)[0]
        if sample != name:
            continue
        try:
            value = float(fields[1])
        except ValueError:
            invalid = True
            continue
        if math.isfinite(value):
            values.append(value)
        else:
            invalid = True
    return None if invalid or not values else sum(values)


def durability_delta(before_texts, after_texts):
    """Calculate and validate per-store raft-engine deltas and aggregate sync/write counts."""
    if len(before_texts) != len(after_texts) or not before_texts:
        raise ValueError('before/after TiKV store count mismatch or empty')
    stores = []
    for i, (before, after) in enumerate(zip(before_texts, after_texts)):
        item = {'store': i, 'counters': {}}
        for name in METRICS:
            b, a = metric_value(before, name), metric_value(after, name)
            if b is None or a is None:
                raise ValueError(f'store {i}: missing durability counter {name}')
            if not math.isfinite(b) or not math.isfinite(a) or a < b:
                raise ValueError(f'store {i}: invalid/reset durability counter {name}: {b} -> {a}')
            item['counters'][name] = {'before': b, 'after': a, 'delta': a - b}
        write = item['counters'][METRICS[1]]['delta']
        sync = item['counters'][METRICS[0]]['delta']
        if write <= 0 or sync <= 0:
            raise ValueError(f'store {i}: write and sync count deltas must be positive')
        stores.append(item)
    write_total = sum(s['counters'][METRICS[1]]['delta'] for s in stores)
    sync_total = sum(s['counters'][METRICS[0]]['delta'] for s in stores)
    if abs(sync_total - write_total) > 0.01 * write_total:
        raise ValueError(f'aggregate sync/write count differs by more than 1%: {sync_total} vs {write_total}')
    return {'stores': stores, 'write_count_delta_total': write_total,
            'sync_count_delta_total': sync_total, 'sync_write_count_delta_difference': sync_total-write_total,
            'valid': True}


def preflight_endpoint(out, port):
    """Check a host endpoint while tolerating TIME_WAIT from the previous run."""
    host = '127.0.0.1'
    census = single_run.listener_diagnostics(port)
    result = {'host': host, 'port': port, 'listeners_before_bind': census}
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            probe.bind((host, port))
        result.update(available=True, bind_error=None)
    except OSError as e:
        result.update(available=False, bind_error=f'{type(e).__name__}: {e}',
                      listeners_on_bind_failure=single_run.listener_diagnostics(port))
    single_run.save(out / f'endpoint-preflight-{port}.json', result)
    if not result['available']:
        raise RuntimeError(f'endpoint preflight bind failed on {host}:{port}; see endpoint-preflight-{port}.json')


def _resource_summary(out, result, count, cpuset_sizes, tikv_cpus_total=None):
    summaries = [json.loads(d.read_text()) for d in out.glob('res-*/resource-summary.json')]
    bydir = {d.parent.name: json.loads(d.read_text()) for d in out.glob('res-*/resource-summary.json')}
    tikv = [bydir[f'res-tikv{i+1}'] for i in range(count)]
    tidb, pd = bydir['res-tidb'], bydir['res-pd']
    tikv_cpu = sum((x.get('cpu_core_equivalents_mean') or 0) for x in tikv)
    tidb_cpu = tidb.get('cpu_core_equivalents_mean')
    intervals = min((x.get('cpu_intervals', 0) for x in summaries), default=0)
    formal = all(x.get('formal') is True for x in summaries) and intervals >= 4
    first = tikv[0]
    net = {}
    for name in ['tidb'] + [f'tikv{i+1}' for i in range(count)]:
        with (out / f'res-{name}' / 'net.csv').open(newline='') as f:
            net[name] = net_rate_summary(list(csv.DictReader(f)), result['measurement_start_unix_ns'], result['measurement_end_unix_ns'])
    return {
        'tikv_cpu_cores_mean': tikv_cpu, 'tikv_cpu_pct_of_quota': tikv_cpu_pct_of_quota(tikv_cpu, tikv_cpus_total if tikv_cpus_total is not None else 4*count),
        'tidb_cpu_cores_mean': tidb_cpu,
        'tidb_cpu_pct_of_cpuset': (tidb_cpu / cpuset_sizes['tidb'] * 100) if tidb_cpu is not None else None,
        'tidb_saturated': tidb_cpu is not None and tidb_cpu >= .85 * cpuset_sizes['tidb'],
        'pd_cpu_cores_mean': pd.get('cpu_core_equivalents_mean'),
        'memory_current_bytes_max': {name: bydir[f'res-{name}'].get('db_memory_current_bytes_max')
                                     for name in ['tidb', 'pd'] + [f'tikv{i+1}' for i in range(count)]},
        'device_means': {k: v for k, v in first.get('means', {}).items() if k.startswith('device_')},
        'client_cpu_cores_mean': first.get('client_cpu_core_equivalents_mean'),
        'client_rss_bytes_max': first.get('client_rss_bytes_max'), 'net': net,
        'container_resource_summaries': bydir, 'full_resource_intervals': intervals,
        'resource_window_formal': formal,
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('output_dir', type=pathlib.Path)
    ap.add_argument('--cpus', type=int, choices=(4, 8), default=4)
    ap.add_argument('--tidb-extended', action='store_true')
    ap.add_argument('--tikv-cpus', type=int, choices=(4, 6), default=None)
    ap.add_argument('--users', type=int, default=100000)
    ap.add_argument('--requests-per-user', type=int, default=40)
    ap.add_argument('--queues', type=int, default=256)
    ap.add_argument('--connections', type=int)
    ap.add_argument('--batch-max', type=int, required=True)
    ap.add_argument('--sample-interval-ms', type=int, default=250)
    ap.add_argument('--retry-pct', type=int, default=0)
    ap.add_argument('--inject-rejects', action='store_true')
    a = ap.parse_args()
    if min(a.users, a.requests_per_user, a.queues, a.batch_max, a.sample_interval_ms) <= 0 or a.retry_pct < 0:
        ap.error('workload values must be positive and retry percentage nonnegative')
    try:
        a.connections = resolve_connections(a.connections, a.queues)
    except ValueError as e:
        ap.error(str(e))
    out = a.output_dir.resolve()
    if out.exists():
        ap.error(f'output directory already exists: {out}')
    try:
        profile = select_profile(a.cpus, a.tidb_extended, a.tikv_cpus)
    except ValueError as e:
        ap.error(str(e))
    try:
        os.sched_setaffinity(0, cpus_from_list(profile['sampler']))
    except (AttributeError, OSError, ValueError) as e:
        ap.error(f'invalid/unsupported resource placement: {e}')
    out.mkdir(parents=True, exist_ok=False)
    owner = uuid.uuid4().hex
    prefix = 'ledger-tt-' + owner[:12]
    names = {'network': prefix+'-net', 'pd': prefix+'-pd', 'tidb': prefix+'-tidb'}
    tikvs = [prefix+f'-tikv{i}' for i in range(len(profile['tikv']))]
    volumes = [prefix+f'-tikv{i}-vol' for i in range(len(tikvs))]
    tikv_quota = tikv_quota_per_store(a.tikv_cpus, len(tikvs))
    tikv_cpus_total = tikv_quota * len(tikvs)
    resources = [('network', names['network'])] + [('container', n) for n in [names['pd'], *tikvs, names['tidb']]] + [('volume', v) for v in volumes]
    inspect_cmd = {'container': ['inspect'], 'volume': ['volume', 'inspect'], 'network': ['network', 'inspect']}
    for kind, name in resources:
        if subprocess.run(['docker', *inspect_cmd[kind], name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0:
            raise RuntimeError(f'resource collision refused: {name}')
    labels = ['--label', 'ledger.gate=2d', '--label', f'ledger.gate.owner={owner}']
    processes, samplers, nets = [], [], []
    status, signal_received = 1, None

    def owned(name, kind):
        template = {'container': '{{ index .Config.Labels "ledger.gate.owner" }}',
                    'volume': '{{ index .Labels "ledger.gate.owner" }}',
                    'network': '{{ index .Labels "ledger.gate.owner" }}'}[kind]
        try:
            return monitor.owned_resource(single_run.docker('inspect', '-f', template, name).strip(), owner)
        except Exception:
            return False

    def on_signal(sig, frame):
        nonlocal signal_received
        signal_received = sig
        for proc in processes:
            if proc.poll() is None:
                proc.send_signal(sig)
        raise KeyboardInterrupt(f'signal {sig}')

    old_handlers = {sig: signal.signal(sig, on_signal) for sig in (signal.SIGINT, signal.SIGTERM)}
    container_names = [names['pd'], *tikvs, names['tidb']]
    try:
        for port in PORTS:
            preflight_endpoint(out, port)
        (out/'pd.toml').write_text('[replication]\nmax-replicas = 1\n\n[schedule]\nmax-merge-region-size = 0\nmax-merge-region-keys = 0\n')
        (out/'tikv.toml').write_text('[storage.block-cache]\ncapacity = "2GB"\n')
        single_run.docker('network', 'create', *labels, names['network'])
        for volume in volumes:
            single_run.docker('volume', 'create', *labels, volume)
        single_run.docker('run', '-d', '--name', names['pd'], *labels, '--network', names['network'],
                          '--cpuset-cpus', profile['sampler'], '--network-alias', 'pd', '-v', f'{out}/pd.toml:/pd.toml:ro', IMAGE_PD,
                          '--name=pd', '--data-dir=/data', '--client-urls=http://0.0.0.0:2379',
                          '--advertise-client-urls=http://pd:2379', '--peer-urls=http://0.0.0.0:2380',
                          '--advertise-peer-urls=http://pd:2380', '--config=/pd.toml')
        for i, (name, volume, cpuset) in enumerate(zip(tikvs, volumes, profile['tikv'])):
            port = 20180+i
            single_run.docker('run', '-d', '--name', name, *labels, '--network', names['network'],
                              *tikv_run_args(cpuset, tikv_quota), '--memory', '8g', '--network-alias', name,
                              '-p', f'127.0.0.1:{port}:20180', '-v', f'{volume}:/data',
                              '-v', f'{out}/tikv.toml:/tikv.toml:ro', IMAGE_TIKV,
                              '--addr=0.0.0.0:20160', f'--advertise-addr={name}:20160',
                              '--status-addr=0.0.0.0:20180', f'--advertise-status-addr={name}:20180',
                              '--pd=pd:2379', '--data-dir=/data', '--config=/tikv.toml')
        single_run.docker('run', '-d', '--name', names['tidb'], *labels, '--network', names['network'],
                          '--cpuset-cpus', profile['tidb'], '--memory', '16g',
                          '-p', '127.0.0.1:14000:4000', IMAGE_TIDB, '--store=tikv', '--path=pd:2379',
                          '--advertise-address=tidb')
        inspect_by_name = {}
        placement = {'cpu_profile': a.cpus, 'tidb_extended': a.tidb_extended, 'tikv': [], 'tidb_cpuset': profile['tidb'],
                     'client_cpus': profile['client'], 'sampler_cpus': profile['sampler'],
                     'tikv_cpus_total': tikv_cpus_total, 'tikv_quota_per_store': tikv_quota,
                     'data_filesystem': monitor.filesystem_device('/var/lib/docker'),
                     'docker_filesystem': monitor.filesystem_device('/var/lib/docker'), 'expected_physical_device': 'sdb'}
        for name, cpuset in zip(tikvs, profile['tikv']):
            ins = json.loads(single_run.docker('inspect', name)); inspect_by_name[name] = ins
            single_run.save(out/f'inspect-start-{name}.json', ins)
            actual = single_run.docker_resource_assignment(ins[0]['HostConfig'], tikv_quota, cpuset, 8589934592)
            placement['tikv'].append({'name': name, **actual})
        for key, name in [('pd', names['pd']), ('tidb', names['tidb'])]:
            ins = json.loads(single_run.docker('inspect', name)); inspect_by_name[name] = ins
            single_run.save(out/f'inspect-start-{name}.json', ins)
            hc = ins[0]['HostConfig']
            if key == 'tidb':
                if hc.get('CpusetCpus') != profile['tidb'] or hc.get('Memory') != 17179869184:
                    raise RuntimeError(f'TiDB resource placement mismatch: {hc}')
            elif hc.get('CpusetCpus') != profile['sampler']:
                raise RuntimeError(f'PD cpuset mismatch: {hc}')
        single_run.save(out/'resource-placement.json', placement)
        if placement['data_filesystem'].get('physical_device') != 'sdb' or placement['docker_filesystem'].get('physical_device') != 'sdb':
            raise RuntimeError(f"backing physical-device mapping differs from required sdb: {placement}")
        single_run.run(['cargo', 'build', '--manifest-path', str(ROOT/'Cargo.toml'), '--locked', '--offline', '--release'], cwd=ROOT)
        if a.cpus == 8:
            # Let PD's region balancer finish its startup grace period before the client pre-splits tables.
            time.sleep(125)
        clientdir = out/'client'; ready = out/'client-ready'; go = out/'GO'
        cmd = tidb_client_command(ROOT/'target/release/txn_batch_tidb', a.users, a.requests_per_user,
                                  a.queues, a.connections, a.batch_max, a.sample_interval_ms,
                                  clientdir, ready, go, len(tikvs), a.retry_pct, a.inject_rejects)
        client = subprocess.Popen(['taskset', '-c', profile['client'], *cmd], cwd=ROOT,
                                  stdout=(out/'client.stdout').open('w'), stderr=subprocess.STDOUT)
        processes.append(client)
        deadline = time.monotonic()+600
        while not ready.exists() and client.poll() is None and time.monotonic() < deadline:
            time.sleep(.1)
        if not ready.exists():
            raise RuntimeError('client did not reach ready handshake within 600s; see client.stdout')
        expected = expected_socket_count(a.connections); observations=[]; previous=None; stable=0
        deadline = time.monotonic()+30
        while time.monotonic() < deadline:
            current = single_run.socket_counts(client.pid, [14000]).get('14000', 0)
            observations.append({'at': single_run.stamp(), 'established_to_14000': current})
            stable = stable+1 if current == expected and current == previous else (1 if current == expected else 0)
            previous = current
            if stable >= 3: break
            time.sleep(.5)
        single_run.save(out/'socket-snapshots.json', {'expected': expected, 'observations': observations, 'stable_samples': stable})
        if stable < 3: raise RuntimeError(f'socket census mismatch: expected {expected}, observations={observations}')
        before=[]
        for i in range(len(tikvs)):
            with urllib.request.urlopen(f'http://127.0.0.1:{20180+i}/metrics', timeout=10) as r: before.append(r.read().decode())
            single_run.save(out/f'tikv{i+1}-metrics-before.raw.txt', before[-1])
        cpusizes = {'tidb': len(cpus_from_list(profile['tidb']))}
        for i, name in enumerate(container_names):
            sub = out/('res-'+('pd' if name == names['pd'] else 'tidb' if name == names['tidb'] else f'tikv{tikvs.index(name)+1}'))
            sub.mkdir(parents=True, exist_ok=True)
            container_id = inspect_by_name[name][0]['Id']; pid = client.pid
            allocated = 2 if name == names['pd'] else cpusizes['tidb'] if name == names['tidb'] else tikv_quota
            ports = [14000] if name == names['tidb'] else [20180+tikvs.index(name)] if name in tikvs else []
            sampler = monitor.Sampler(sub, 'pg', container_id, pid, ports, allocated_cpus=allocated,
                                      client_cpus=2, interval=a.sample_interval_ms/1000, device='sdb')
            samplers.append(sampler)
            sampler.start()
            if name != names['pd']:
                net = NetSampler(sub, int(inspect_by_name[name][0]['State']['Pid']), a.sample_interval_ms/1000)
                nets.append(net); net.start()
        go.write_text(single_run.stamp()+'\n')
        client_status = client.wait()
        client_exit_time = datetime.datetime.now(datetime.timezone.utc).isoformat()
        tolerated = partition_client_exit_errors(samplers, client.pid, client_exit_time)
        single_run.save(out/'sampler-tolerated-errors.json', tolerated)
        errors=[]
        for sampler in samplers:
            try: sampler.stop()
            except Exception as e: errors.append(str(e))
        for net in nets:
            try: net.stop()
            except Exception as e: errors.append(str(e))
        if errors: raise RuntimeError('sampler error: '+'; '.join(errors))
        if client_status: raise RuntimeError(f'client exited {client_status}; see client.stdout')
        result = json.loads((clientdir/'result.json').read_text())
        if not result.get('verification', {}).get('valid', False): raise RuntimeError('client result reports invalid verification')
        after=[]
        for i in range(len(tikvs)):
            with urllib.request.urlopen(f'http://127.0.0.1:{20180+i}/metrics', timeout=10) as r: after.append(r.read().decode())
            single_run.save(out/f'tikv{i+1}-metrics-after.raw.txt', after[-1])
        try:
            durability = durability_delta(before, after)
        except Exception as e:
            single_run.save(out/'durability.json', {'valid': False, 'error': str(e)})
            raise
        single_run.save(out/'durability.json', durability)
        for d in [out/f'res-tikv{i+1}' for i in range(len(tikvs))] + [out/'res-tidb', out/'res-pd']:
            single_run.filter_resources(d, result)
        summary = dict(result)
        summary['durability'] = durability
        summary['resource_summary'] = _resource_summary(out, result, len(tikvs), cpusizes, tikv_cpus_total)
        single_run.save(out/'summary.json', summary)
        status = 0
    except BaseException as e:
        single_run.save(out/'failure.txt', f'{type(e).__name__}: {e}\n')
        status = 1
    finally:
        for sampler in samplers:
            if sampler.thread and sampler.thread.is_alive():
                try: sampler.stop()
                except Exception: status = 1
        for net in nets:
            if net.thread and net.thread.is_alive():
                try: net.stop()
                except Exception: status = 1
        for proc in processes:
            if proc.poll() is None:
                try: proc.wait(timeout=30 if signal_received else 0)
                except subprocess.TimeoutExpired:
                    proc.terminate()
                    try: proc.wait(timeout=10)
                    except subprocess.TimeoutExpired: proc.kill(); proc.wait()
        errors=[]
        for name in container_names:
            if owned(name, 'container'):
                try: single_run.save(out/f'docker-logs-{name}.txt', single_run.docker('logs', name))
                except Exception as e: errors.append(f'logs {name}: {e}')
                try: single_run.save(out/f'inspect-final-{name}.json', json.loads(single_run.docker('inspect', name)))
                except Exception as e: errors.append(f'inspect {name}: {e}')
                try: single_run.docker('rm', '-f', name)
                except Exception as e: errors.append(f'remove {name}: {e}')
        for volume in volumes:
            try:
                if owned(volume, 'volume'): single_run.docker('volume', 'rm', volume)
            except Exception as e: errors.append(f'remove volume {volume}: {e}')
        try:
            if owned(names['network'], 'network'): single_run.docker('network', 'rm', names['network'])
        except Exception as e: errors.append(f'remove network: {e}')
        residue = [{'kind': kind, 'name': name, 'exists': owned(name, kind)} for kind, name in resources]
        cleanup = {'at': single_run.stamp(), 'owner': owner, 'residue': residue, 'errors': errors,
                   'signal': signal_received, 'status': 'failed' if errors or any(x['exists'] for x in residue) else 'success'}
        single_run.save(out/'cleanup.json', cleanup)
        if errors or any(x['exists'] for x in residue): status = 1
        for sig, handler in old_handlers.items(): signal.signal(sig, handler)
    return status


if __name__ == '__main__':
    sys.exit(main())
