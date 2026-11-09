#!/usr/bin/env python3
"""Summarize archived runs under results/ into a Markdown table (stdout) and results/summary.csv.

Reads, per run directory: result.json, resource-summary.json, settings.txt.
Usage: python3 scripts/summarize_results.py [results_dir]
"""
import csv, json, pathlib, sys

def load(path):
    p = pathlib.Path(path)
    return json.loads(p.read_text()) if p.exists() else {}

def rnd(v, d=0):
    return None if v is None else round(v, d) if d else round(v)

def summarize(run):
    r, rs = load(run / 'result.json'), load(run / 'resource-summary.json')
    means = rs.get('means', {})
    o, lat, w = r.get('outcomes', {}), r.get('latency_us', {}), r.get('wal_stats', {})
    mb = lambda v: None if v is None else round(v / 1048576, 1)
    return {
        'run': run.name,
        'window_batch_max': r.get('effective', {}).get('batch_max'),
        'requests_sent': r.get('requests'),
        'applied': o.get('applied'), 'existing': o.get('existing'),
        'duration_s': round(r.get('duration_s', 0), 2),
        'rps_total': round(r.get('rps_request', 0)),
        'rps_applied_unique': round(r.get('rps_applied_unique', 0)),
        'latency_p50_s': round(lat.get('p50', 0) / 1e6, 2), 'latency_p95_s': round(lat.get('p95', 0) / 1e6, 2),
        'latency_p99_s': round(lat.get('p99', 0) / 1e6, 2),
        'db_cpu_cores': rnd(means.get('db_cpu_core_equivalents'), 2),
        'db_cpu_pct': rnd(means.get('db_normalized_cpu_pct_of_allocated_capacity')),
        'client_cpu_cores': rnd(means.get('client_cpu_core_equivalents'), 2),
        'db_mem_max_mib': mb(rs.get('db_memory_current_bytes_max')),
        'client_rss_max_mib': mb(rs.get('client_rss_bytes_max')),
        'dev_write_mib_s': rnd(means.get('device_write_mib_s')),
        'dev_write_iops': rnd(means.get('device_write_iops')),
        'dev_write_await_ms': rnd(means.get('device_write_await_ms'), 2),
        'dev_flush_await_ms': rnd(means.get('device_flush_await_ms'), 2),
        'dev_util_pct': rnd(means.get('device_util_pct')),
        'dev_queue_depth': rnd(means.get('device_avg_queue_depth'), 2),
        'wal_mb': round(w.get('lsn_delta_bytes', 0) / 1e6),
        'wal_settled': w.get('settled'),
        'verification_valid': r.get('verification', {}).get('valid'),
        'stalled_flush_await_gt_2ms': (means.get('device_flush_await_ms') or 0) > 2,
    }

def main():
    root = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else pathlib.Path(__file__).resolve().parent.parent / 'results')
    rows = [summarize(d) for d in sorted(root.iterdir()) if (d / 'result.json').exists()]
    with open(root / 'summary.csv', 'w', newline='') as f:
        wr = csv.DictWriter(f, fieldnames=list(rows[0])); wr.writeheader(); wr.writerows(rows)
    keys = list(rows[0])
    print('| ' + ' | '.join(keys) + ' |'); print('|' + '---|' * len(keys))
    for r in rows: print('| ' + ' | '.join(str(r[k]) for k in keys) + ' |')

if __name__ == '__main__':
    main()
