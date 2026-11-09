#!/usr/bin/env python3
"""Run the PostgreSQL batch-queue benchmark with owned disposable resources."""
import argparse
import csv
import datetime
import json
import os
import pathlib
import signal
import subprocess
import sys
import threading
import time
import uuid

import monitor
import single_run
from endpoints import PG_HOST, PG_PORT, PG_CONTAINER_PORT, PG_URL
from resource_profiles import Profile, cpus_from_list, validate_client_cpus

ROOT = single_run.ROOT
PG_IMAGE = single_run.PG_IMAGE
PROFILES = {
    4: Profile(4, '0-3', 4, 4, '4-5', 2, '14-15', frozenset(range(0, 4)) | frozenset(range(8, 12))),
    # 8 DB cores = all physical cores; the client necessarily shares SMT siblings with DB cores (disclosed).
    8: Profile(8, '0-7', 8, 8, '8-9', 2, '14-15', frozenset(range(0, 8))),
}
PROFILE = PROFILES[4]


def parse_net_dev(text):
    """Parse /proc/net/dev into {interface: (rx_bytes, tx_bytes)}."""
    result = {}
    for line in text.splitlines():
        if ':' not in line:
            continue
        interface, counters = line.split(':', 1)
        interface = interface.strip()
        values = counters.split()
        if len(values) < 9:
            continue
        try:
            result[interface] = (int(values[0]), int(values[8]))
        except ValueError:
            continue
    return result


def _wall_seconds(value):
    return datetime.datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()


def net_rate_summary(rows, start_unix_ns, end_unix_ns):
    """Summarize rates from adjacent cumulative samples wholly within the window."""
    start, end = start_unix_ns / 1e9, end_unix_ns / 1e9
    points = []
    for row in rows:
        try:
            points.append((_wall_seconds(row['wall_utc']), int(row['rx_bytes']), int(row['tx_bytes'])))
        except (KeyError, ValueError, TypeError):
            continue
    rates = []
    for a, b in zip(points, points[1:]):
        t0, rx0, tx0 = a
        t1, rx1, tx1 = b
        dt = t1 - t0
        if t0 < start or t1 > end or dt <= 0 or rx1 < rx0 or tx1 < tx0:
            continue
        rates.append({'start_unix_seconds': t0, 'end_unix_seconds': t1,
                      'elapsed_seconds': dt, 'rx_mib_s': (rx1-rx0)/1048576/dt,
                      'tx_mib_s': (tx1-tx0)/1048576/dt})
    duration = sum(row['elapsed_seconds'] for row in rates)
    return {
        'intervals': len(rates), 'covered_seconds': duration,
        'rx_mib_s_avg': sum(r['rx_mib_s']*r['elapsed_seconds'] for r in rates)/duration if duration else None,
        'rx_mib_s_max': max((r['rx_mib_s'] for r in rates), default=None),
        'tx_mib_s_avg': sum(r['tx_mib_s']*r['elapsed_seconds'] for r in rates)/duration if duration else None,
        'tx_mib_s_max': max((r['tx_mib_s'] for r in rates), default=None),
        'rate_intervals': rates,
    }


class NetSampler:
    def __init__(self, out, pid, interval):
        self.out = pathlib.Path(out)
        self.pid = pid
        self.interval = interval
        self.stop_event = threading.Event()
        self.thread = None
        self.errors = []

    def run(self):
        deadline = time.monotonic()
        try:
            with (self.out / 'net.csv').open('w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=['wall_utc', 'rx_bytes', 'tx_bytes'])
                writer.writeheader()
                while not self.stop_event.is_set():
                    wall = single_run.stamp()
                    try:
                        data = pathlib.Path(f'/proc/{self.pid}/net/dev').read_text()
                        counters = parse_net_dev(data)
                        if 'eth0' not in counters:
                            raise RuntimeError('eth0 missing from container network namespace')
                        rx, tx = counters['eth0']
                        writer.writerow({'wall_utc': wall, 'rx_bytes': rx, 'tx_bytes': tx})
                        f.flush()
                    except Exception as e:
                        self.errors.append(f'{wall}: {e}')
                    deadline += self.interval
                    self.stop_event.wait(max(0, deadline-time.monotonic()))
        except Exception as e:
            self.errors.append(f'net sampler: {e}')

    def start(self):
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=10)
        if self.thread and self.thread.is_alive():
            self.errors.append('net sampler thread did not stop')
        if self.errors:
            raise RuntimeError('net sampler errors: ' + '; '.join(self.errors))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('output_dir', type=pathlib.Path)
    ap.add_argument('--users', type=int, default=100000)
    ap.add_argument('--requests-per-user', type=int, default=10)
    ap.add_argument('--queues', type=int, default=256)
    ap.add_argument('--batch-max', type=int, default=1000)
    ap.add_argument('--sample-interval-ms', type=int, default=1000)
    ap.add_argument('--cpus', type=int, choices=sorted(PROFILES), default=4)
    ap.add_argument('--mode', choices=('batch', 'single', 'txn'), default='batch')
    ap.add_argument('--retry-pct', type=int, default=0)
    ap.add_argument('--inject-rejects', action='store_true')
    ap.add_argument('--pg-conf', action='append', default=[])
    a = ap.parse_args()
    fixed_pg_settings = {'shared_buffers', 'fsync', 'synchronous_commit', 'full_page_writes', 'max_connections'}
    for setting in a.pg_conf:
        if '=' not in setting or not setting.split('=', 1)[0]:
            ap.error(f'--pg-conf must be key=value with a non-empty key: {setting!r}')
        if setting.split('=', 1)[0] in fixed_pg_settings:
            ap.error(f'--pg-conf cannot override wrapper-fixed setting: {setting.split("=", 1)[0]}')
    pg_conf_args = [part for setting in a.pg_conf for part in ('-c', setting)]
    global PROFILE
    PROFILE = PROFILES[a.cpus]
    if min(a.users, a.requests_per_user, a.queues, a.batch_max, a.sample_interval_ms) <= 0:
        ap.error('all workload and interval values must be positive')
    out = a.output_dir.resolve()
    if out.exists():
        ap.error(f'output directory already exists: {out}')
    try:
        validate_client_cpus(PROFILE, PROFILE.client_cpus)
        sampler_cpus = cpus_from_list(PROFILE.sampler_cpus)
        os.sched_setaffinity(0, sampler_cpus)
    except (AttributeError, OSError, ValueError) as e:
        ap.error(f'invalid/unsupported resource placement: {e}')
    out.mkdir(parents=True, exist_ok=False)
    owner = uuid.uuid4().hex
    prefix = 'ledger-bq-' + owner[:12]
    container, volume, network = prefix+'-db', prefix+'-vol', prefix+'-net'
    resources = [('container', container), ('volume', volume), ('network', network)]
    for kind, name in resources:
        cmd = {'container': ['inspect'], 'volume': ['volume', 'inspect'], 'network': ['network', 'inspect']}[kind]
        if subprocess.run(['docker', *cmd, name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0:
            raise RuntimeError(f'resource collision refused: {name}')
    labels = ['--label', 'ledger.gate=2d', '--label', f'ledger.gate.owner={owner}']
    proc = sampler = net_sampler = None
    status = 1
    signal_received = None

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
        if proc and proc.poll() is None:
            proc.send_signal(sig)
        raise KeyboardInterrupt(f'signal {sig}')

    old_handlers = {sig: signal.signal(sig, on_signal) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        single_run.preflight_endpoint(out, PG_PORT)
        single_run.docker('network', 'create', *labels, network)
        single_run.docker('volume', 'create', *labels, volume)
        single_run.docker('run', '-d', '--name', container, *labels, '--network', network,
                          '--cpuset-cpus', PROFILE.db_cores, '--cpus', str(PROFILE.quota), '--memory', '8g',
                          '-p', f'{PG_HOST}:{PG_PORT}:{PG_CONTAINER_PORT}', '-e', 'POSTGRES_PASSWORD=gate2d',
                          '-e', 'POSTGRES_DB=ledger', '-v', volume+':/var/lib/postgresql', PG_IMAGE,
                          'postgres', '-c', 'shared_buffers=2GB', '-c', 'fsync=on',
                          '-c', 'synchronous_commit=on', '-c', 'full_page_writes=on',
                          '-c', f'max_connections={max(300, a.queues + 44)}', *pg_conf_args)
        deadline = time.monotonic() + 600
        while time.monotonic() < deadline:
            try:
                ready = subprocess.run(['docker', 'exec', container, 'pg_isready', '-h', '127.0.0.1', '-U', 'postgres', '-d', 'ledger'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5)
                if ready.returncode == 0:
                    break
            except (OSError, subprocess.TimeoutExpired):
                pass
            time.sleep(.5)
        else:
            raise RuntimeError('PostgreSQL readiness timed out')
        inspect = json.loads(single_run.docker('inspect', container))
        single_run.save(out/'inspect-start.json', inspect)
        host_config = inspect[0]['HostConfig']
        placement = {
            'cpu_profile': PROFILE.cpus, 'db_cores': PROFILE.db_cores, 'db_cpu_quota': PROFILE.quota,
            **single_run.docker_resource_assignment(host_config, PROFILE.quota, PROFILE.db_cores, 8589934592),
            'client_cpus': PROFILE.client_cpus, 'client_workers': PROFILE.workers,
            'sampler_cpus': PROFILE.sampler_cpus,
            'data_filesystem': monitor.filesystem_device('/var/lib/docker'),
            'docker_filesystem': monitor.filesystem_device('/var/lib/docker'),
            'expected_physical_device': 'sdb',
        }
        single_run.save(out/'resource-placement.json', placement)
        if placement['data_filesystem'].get('physical_device') != 'sdb' or placement['docker_filesystem'].get('physical_device') != 'sdb':
            raise RuntimeError(f"backing physical-device mapping differs from required sdb: {placement}")
        single_run.save(out/'server-start.log', single_run.docker('logs', container))
        settings = "SELECT 'server_version',current_setting('server_version'),'',''; SELECT name,setting,unit,source FROM pg_settings WHERE name IN ('shared_buffers','fsync','synchronous_commit','full_page_writes','commit_delay','commit_siblings','max_connections','max_wal_size','wal_buffers','track_wal_io_timing','checkpoint_completion_target','checkpoint_timeout') ORDER BY name;"
        single_run.save(out/'settings.txt', single_run.docker('exec', container, 'psql', '-X', '-At', '-U', 'postgres', '-d', 'ledger', '-c', settings))
        single_run.run(['cargo', 'build', '--manifest-path', str(ROOT/'Cargo.toml'), '--locked', '--offline', '--release'], cwd=ROOT)
        client_dir = out/'client'
        ready_file, go_file = out/'client-ready', out/'GO'
        if a.mode == 'batch':
            binary = ROOT/'target/release/batch_queue_pg'
            cmd = [str(binary), '--pg-url', PG_URL, '--users', str(a.users), '--requests-per-user', str(a.requests_per_user),
                   '--queues', str(a.queues), '--batch-max', str(a.batch_max), '--request-timeout-ms', '30000',
                   '--sample-interval-ms', str(a.sample_interval_ms), '--output-dir', str(client_dir),
                   '--ready-file', str(ready_file), '--go-file', str(go_file), '--handshake-timeout-ms', '600000']
        elif a.mode == 'txn':
            binary = ROOT/'target/release/txn_batch_pg'
            cmd = [str(binary), '--pg-url', PG_URL, '--users', str(a.users), '--requests-per-user', str(a.requests_per_user),
                   '--queues', str(a.queues), '--batch-max', str(a.batch_max), '--request-timeout-ms', '30000',
                   '--sample-interval-ms', str(a.sample_interval_ms), '--output-dir', str(client_dir),
                   '--ready-file', str(ready_file), '--go-file', str(go_file), '--handshake-timeout-ms', '600000',
                   '--retry-pct', str(a.retry_pct)]
            if a.inject_rejects:
                cmd.append('--inject-rejects')
        else:
            binary = ROOT/'target/release/nobatch_pg'
            cmd = [str(binary), '--pg-url', PG_URL, '--users', str(a.users), '--requests-per-user', str(a.requests_per_user),
                   '--connections', str(a.queues), '--request-timeout-ms', '30000',
                   '--sample-interval-ms', str(a.sample_interval_ms), '--output-dir', str(client_dir),
                   '--ready-file', str(ready_file), '--go-file', str(go_file), '--handshake-timeout-ms', '600000']
        proc = subprocess.Popen(['taskset', '-c', PROFILE.client_cpus, *cmd], cwd=ROOT,
                                stdout=(out/'client.stdout').open('w'), stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 600
        while not ready_file.exists() and proc.poll() is None and time.monotonic() < deadline:
            time.sleep(.1)
        if not ready_file.exists():
            raise RuntimeError('client did not reach ready handshake; see client.stdout')
        save = single_run.save
        save(out/'client-ready.json', {'ready_file_text': ready_file.read_text().strip()})
        pid = int(inspect[0]['State']['Pid'])
        observations, previous, stable = [], None, 0
        deadline = time.monotonic() + 30
        expected = a.queues + 1
        while time.monotonic() < deadline:
            counts = single_run.socket_counts(proc.pid, [PG_PORT])
            current = counts.get(str(PG_PORT), 0)
            observations.append({'at': single_run.stamp(), 'counts_by_remote_port': counts})
            stable = stable + 1 if current == expected and current == previous else (1 if current == expected else 0)
            previous = current
            if stable >= 3:
                break
            time.sleep(.5)
        single_run.save(out/'socket-snapshots.json', {'expected': expected, 'observations': observations, 'stable_samples': stable})
        if stable < 3:
            raise RuntimeError(f'socket readiness failed: expected {expected} established sockets, observed {observations[-1] if observations else {}}')
        container_id = inspect[0]['Id']
        interval = a.sample_interval_ms / 1000
        sampler = monitor.Sampler(out, 'pg', container_id, proc.pid, [PG_PORT], allocated_cpus=PROFILE.quota,
                                  client_cpus=2, interval=interval, device='sdb')
        net_sampler = NetSampler(out, pid, interval)
        sampler.start()
        net_sampler.start()
        go_file.write_text(single_run.stamp()+'\n')
        client_status = proc.wait()
        sampler_error = net_error = None
        try:
            sampler.stop()
        except Exception as e:
            sampler_error = str(e)
        try:
            net_sampler.stop()
        except Exception as e:
            net_error = str(e)
        if sampler_error or net_error:
            raise RuntimeError('; '.join(x for x in (sampler_error, net_error) if x))
        if client_status != 0:
            raise RuntimeError(f'client exited {client_status}; see client.stdout')
        result_path = client_dir/'result.json'
        result = json.loads(result_path.read_text())
        if not result.get('verification', {}).get('valid', False):
            raise RuntimeError('client result reports invalid verification')
        single_run.filter_resources(out, result)
        resource_summary = json.loads((out/'resource-summary.json').read_text())
        with (out/'net.csv').open(newline='') as f:
            net_summary = net_rate_summary(list(csv.DictReader(f)), result['measurement_start_unix_ns'], result['measurement_end_unix_ns'])
        intervals = int(resource_summary.get('cpu_intervals', 0))
        formal = intervals >= 4
        if not formal:
            print(f'WARNING: only {intervals} full resource intervals; resource_window_formal=false', file=sys.stderr)
        summary = dict(result)
        summary['resource_summary'] = {k: resource_summary.get(k) for k in ('cpu_intervals', 'cpu_interval_coverage_seconds', 'db_cpu_core_equivalents_mean', 'client_cpu_core_equivalents_mean', 'db_memory_current_bytes_max', 'client_rss_bytes_max', 'means', 'formal')}
        summary['net_summary'] = {k: v for k, v in net_summary.items() if k != 'rate_intervals'}
        summary['full_resource_intervals'] = intervals
        summary['resource_window_formal'] = formal
        single_run.save(out/'summary.json', summary)
        status = 0
    except BaseException as e:
        single_run.save(out/'failure.txt', f'{type(e).__name__}: {e}\n')
        status = 1
    finally:
        if sampler and sampler.thread and sampler.thread.is_alive():
            try:
                sampler.stop()
            except Exception as e:
                single_run.save(out/'sampler-stop-error.txt', str(e)+'\n')
                status = 1
        if net_sampler and net_sampler.thread and net_sampler.thread.is_alive():
            try:
                net_sampler.stop()
            except Exception as e:
                single_run.save(out/'net-sampler-stop-error.txt', str(e)+'\n')
                status = 1
        if proc and proc.poll() is None:
            try:
                proc.wait(timeout=30 if signal_received is not None else 0)
            except subprocess.TimeoutExpired:
                proc.terminate()
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill(); proc.wait()
        errors = []
        if owned(container, 'container'):
            try:
                single_run.save(out/'server-final.log', single_run.docker('logs', container))
            except Exception as e:
                errors.append(f'capture container logs: {e}')
            try:
                single_run.save(out/'inspect-final.json', json.loads(single_run.docker('inspect', container)))
            except Exception as e:
                errors.append(f'inspect container: {e}')
            try:
                single_run.docker('rm', '-f', container)
            except Exception as e:
                errors.append(f'remove container: {e}')
        for kind, name in [('volume', volume), ('network', network)]:
            try:
                if owned(name, kind):
                    single_run.docker(kind, 'rm', name)
            except Exception as e:
                errors.append(f'remove {kind} {name}: {e}')
        residue = [{'kind': kind, 'name': name, 'exists': owned(name, kind)} for kind, name in resources]
        cleanup = {'at': single_run.stamp(), 'owner': owner, 'residue': residue, 'errors': errors,
                   'signal': signal_received, 'status': 'failed' if errors or any(r['exists'] for r in residue) else 'success'}
        single_run.save(out/'cleanup.json', cleanup)
        if errors or any(r['exists'] for r in residue):
            status = 1
        for sig, handler in old_handlers.items():
            signal.signal(sig, handler)
    return status


if __name__ == '__main__':
    sys.exit(main())
