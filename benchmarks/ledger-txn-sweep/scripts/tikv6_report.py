#!/usr/bin/env python3
"""Generate the TiKV 6-CPU placement comparison report from fixed-sweep artifacts."""
import math
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
import fixconn_report as report
import fixconn_sweep as driver

OUT = ROOT / 'docs/ledger-tikv6-report.md'
NEW = ('tidb-k4t4-n16', 'tidb-k6t8-n16')
REFS = {'tidb-k4t4-n16': ('tidb-c4-n16', 'c4'), 'tidb-k6t8-n16': ('tidb-c8x-n16', 'c8x')}
BATCHES = (2000, 4000, 8000)


def num(v):
    return report.number(v)


def fmt(v, d=2):
    return report.f(v, d)


def compact(v):
    x = num(v)
    return f'{x/1000:.1f}k' if x is not None else 'n/a'


def safe_ratio(a, b):
    x, y = num(a), num(b)
    return x / y if x is not None and y not in (None, 0) else None


def pct_diff(a, b):
    ratio = safe_ratio(a, b)
    return (ratio - 1) * 100 if ratio is not None else None


def eff(rs, series):
    return driver.effective_primary(rs)


def successful(r):
    return bool(r and r.get('status') == 'ok')


def item(e, batch):
    return e.get(batch, e.get(str(batch), {})) or {}


def build(root=None):
    """Build report; root is injectable so tests can use an isolated temp tree."""
    global ROOT
    old_root, old_art, old_report_root = ROOT, driver.ART, report.ROOT
    if root is not None:
        ROOT = pathlib.Path(root).resolve()
        driver.ART = ROOT / 'artifacts'
        report.ROOT = ROOT
    try:
        def read_series(s):
            # Resolve the fixed artifact names directly; avoids sweep helper state
            # leaking from callers/tests that temporarily guard series_file().
            db, _cpus, conns, extended = driver.SERIES[s]
            suffix = driver.series_meta(s)['suffix']
            path = driver.ART / f'fixconn-{db}-{suffix}-n{conns}.csv'
            if not path.is_file():
                return []
            import csv
            with path.open(newline='') as stream:
                return list(csv.DictReader(stream))
        raw = {s: read_series(s) for s in (*NEW, 'tidb-c4-n16', 'tidb-c8x-n16')}
        effective = {s: eff(raw[s], s) for s in raw}
        L = ['# TiKV placement report: ledger-tikv6', '', 'Generated from the recorded CSV rows and their attempt artifacts. Missing or invalid measurements are `n/a`.', '',
             '## 1. Scope and placement', '',
             '| Configuration | TiKV placement / quota | tidb-server placement | Client / PD / sampler |', '|---|---|---|---|',
             '| k4t4 (new) | 1 store × 4 logical CPUs; quota 4 | 4 logical CPUs: 6-7,12-13 | client 4-5; PD + sampler 14-15 |',
             '| k6t8 (new) | 2 stores × 3 CPUs; cpusets 0-2 / 4-6; quota 3 per store (6 total) | 8 logical CPUs: 3,7,10-15 | client + PD + sampler 8-9 |',
             '| c4 reference (earlier) | Same placement as k4t4 | Same placement as k4t4 | Same placement as k4t4 |',
             '| c8x reference (earlier) | 2 stores × 4 CPU quota (8 total) | 6 logical CPUs: 10-15 | client + PD + sampler 8-9 |', '',
             'Workload: 2,000,000 requests (50,000 × 40), 256 queues, 256 regions per table, no replication factor, 16 connections; batches 2,000 / 4,000 / 8,000, one run each. An extra run is required only when RPS differs by more than ±15% from the matching reference; flush-await >2 ms invokes the established flush rerun rule. Runs were single observations; no repeatability is implied.', '',
             'Placement disclosures: in k6t8 logical CPUs N and N+8 share a physical core. From the listed cpusets, TiKV/client pairs are 0/8 and 1/9; TiDB-server/TiKV pairs are 4/12, 5/13, and 6/14; TiDB-server pairs are 3/11 and 7/15. Client, PD, and sampler share logical CPUs 8-9. The placements alone do not establish actual contention. TiKV has a quota of 3 per store. Each batch/configuration is a single primary run (extra runs, if any, are supplemental).', '',
             '## 2. New-configuration measurements', '',
             'Latency is seconds; windows are observed (expected). TiKV CPU is cores / percent of TiKV quota (4 for k4t4, 6 for k6t8). TiDB-server CPU is cores / percent of its cpuset (4 or 8); `*` means ≥85%, and `†` marks CSV full-window CPU reclassification (using CSV `tidb_cpu_cores` and the configuration denominator). Disk columns are write MiB/s / IOPS / util % / flush-await ms / write-await ms. Network is TiDB rx / tx MiB/s.', '',
             '| Config / batch | RPS | p50 | p95 | p99 | max | windows (expected) | TiKV cores / % quota | tidb-server cores / % cpuset | client CPU | tidb memory GiB | sdb write / IOPS / util / flush / write await | network rx / tx MiB/s | PD cores | shared CPU client / PD / wrapper-sampler / total % of 2 CPUs | extra-run / flush info |',
             '|---|---:|---:|---:|---:|---:|---|---|---|---:|---:|---|---|---:|---|---|']
        for s in NEW:
            meta = driver.series_meta(s)
            for b in BATCHES:
                r = item(effective[s], b)
                if not successful(r):
                    L.append(f'| {"k4t4" if s == NEW[0] else "k6t8"} / {b} | n/a | n/a | n/a | n/a | n/a | n/a ({driver.expected_windows(b)}) | n/a | n/a | n/a | n/a | n/a | n/a / n/a | n/a | n/a | {"no data" if not raw[s] else "no successful primary"} |')
                    continue
                tikv, tikvpct, tcpu, tpct, client, mem, z = report.resource(r, 'tidb', meta['tikv_quota_total'])
                shared = driver.shared_cpu(ROOT / r['attempt_dir']) if r.get('attempt_dir') else None
                pd = shared[1] if shared else None
                client_cpu = num(client)
                shared_cell = (f'{fmt(shared[0])} / {fmt(shared[1])} / {fmt(shared[2])} / {fmt(shared[3]*100,1)}%' if shared else
                               (f'{fmt(client_cpu)} / {fmt(pd)} / n/a / n/a' if s == NEW[0] else 'n/a'))
                reclass = report.reclassified(r)
                # fixconn_report's validated resource helper knows legacy cpuset names;
                # these new series require explicit 4/8-CPU denominators.
                tp = tcpu / meta['tidb_cpuset_size'] * 100 if tcpu is not None else None
                tidb_cell = f'{fmt(tcpu)} / {fmt(tp,1)}%'
                tidb_cell += ('*' if tp is not None and tp >= 85 else '') + ('†' if reclass else '')
                dv = report.disk(z)
                disk_cell = ' / '.join(fmt(x) for x in dv)
                mem_gib = num(mem) / 1024**3 if num(mem) is not None else None
                extra = [f'flush rerun {"recorded" if any(str(x.get("batch"))==str(b) and x.get("role")=="flush_rerun" for x in raw[s]) else "not recorded"}']
                reps = [x for x in raw[s] if str(x.get('batch')) == str(b) and x.get('role') == 'replicate']
                if reps: extra.append('extra run(s): ' + ', '.join(f'{x.get("status","n/a")} RPS {compact(x.get("rps_request"))}' for x in reps))
                else: extra.append('no reference-triggered extra run recorded')
                L.append(f'| {"k4t4" if s == NEW[0] else "k6t8"} / {b} | {compact(r.get("rps_request"))} | {fmt(r.get("p50_s"),3)} | {fmt(r.get("p95_s"),3)} | {fmt(r.get("p99_s"),3)} | {fmt(r.get("max_s"),3)} | {r.get("windows") or "n/a"} ({driver.expected_windows(b)}) | {fmt(tikv)} / {fmt(tikvpct,1)}% | {tidb_cell} | {fmt(client_cpu)} | {fmt(mem_gib)} | {disk_cell} | {report.network(r,"tidb",z)} | {fmt(pd)} | {shared_cell} | {"; ".join(extra)} |')
        L += ['', '## 3. Equal-batch comparison', '', '| Batch | k4t4 RPS / p99 | k6t8 RPS / p99 | c4 reference RPS / p99 | c8x reference RPS / p99 | k6t8 / k4t4 RPS | k6t8 / c8x RPS | k4t4 vs c4 | k6t8 vs c8x |', '|---:|---|---|---|---|---:|---:|---:|---:|']
        for b in BATCHES:
            vals = {s: item(effective[s], b) for s in raw}
            def pair(s):
                r = vals[s]
                return f'{compact(r.get("rps_request"))} / {fmt(r.get("p99_s"),3)}' if successful(r) else 'n/a / n/a'
            k4, k6, c4, c8 = (vals[s] for s in ('tidb-k4t4-n16','tidb-k6t8-n16','tidb-c4-n16','tidb-c8x-n16'))
            L.append(f'| {b} | {pair("tidb-k4t4-n16")} | {pair("tidb-k6t8-n16")} | {pair("tidb-c4-n16")} | {pair("tidb-c8x-n16")} | {fmt(safe_ratio(k6.get("rps_request"),k4.get("rps_request")))} | {fmt(safe_ratio(k6.get("rps_request"),c8.get("rps_request")))} | {fmt(pct_diff(k4.get("rps_request"),c4.get("rps_request")),1)}% | {fmt(pct_diff(k6.get("rps_request"),c8.get("rps_request")),1)}% |')
        L += ['', '## 4. TiKV durability and region distribution', '', 'Raft-engine write/sync values are per-store counter deltas from the attempt evidence; topology is the captured client `topology.json` distribution.', '', '| Config / batch | raft-engine write / sync deltas per store | Region distribution |', '|---|---|---|']
        for s in NEW:
            for b in BATCHES:
                r = item(effective[s], b)
                if successful(r): L.append(f'| {"k4t4" if s == NEW[0] else "k6t8"} / {b} | {report.durability(r)} | {report.topology(r)} |')
                else: L.append(f'| {"k4t4" if s == NEW[0] else "k6t8"} / {b} | n/a | n/a |')
        L += ['', '## 5. Data-derived facts', '']
        all_flush = []
        for s in NEW:
            name = 'k4t4' if s == NEW[0] else 'k6t8'
            e = {b: item(effective[s], b) for b in BATCHES}
            good = {b:r for b,r in e.items() if successful(r)}
            if not good:
                L.append(f'- {name}: no data; maximum RPS, reference ratio, TiDB-server threshold, TiKV CPU percentage, and flush-await range are not measured.')
                continue
            peak = max(good, key=lambda b: num(good[b].get('rps_request')) or -1)
            L.append(f'- {name}: maximum observed RPS is {compact(good[peak].get("rps_request"))} at batch {peak}.')
            for b,r in good.items():
                meta=driver.series_meta(s); _,_,tcpu,tpct,_,_,z=report.resource(r,'tidb',meta['tikv_quota_total'])
                tp=tcpu/meta['tidb_cpuset_size']*100 if report.reclassified(r) and tcpu is not None else tpct
                tikv_pct=(num(z.get('tikv_cpu_pct_of_quota')) or (num(z.get('tikv_cpu_cores_mean'))/meta['tikv_quota_total']*100 if num(z.get('tikv_cpu_cores_mean')) is not None else None))
                all_flush.append(num(report.disk(z)[3]))
                L.append(f'  - Batch {b}: tidb-server {fmt(tp,1)}% of cpuset {"(≥85%)" if tp is not None and tp>=85 else "(<85%)" if tp is not None else "(n/a)"}; TiKV {fmt(tikv_pct,1)}% of quota; flush await {fmt(report.disk(z)[3])} ms.')
                ref=effective[REFS[s][0]]
                rr=item(ref,b)
                L.append(f'    - vs {REFS[s][1]} reference RPS: ratio {fmt(safe_ratio(r.get("rps_request"),rr.get("rps_request")))}; difference {fmt(pct_diff(r.get("rps_request"),rr.get("rps_request")),1)}%.')
        measured_flush=[v for v in all_flush if v is not None]
        L.append(f'- Flush-await range across available new-run data: {fmt(min(measured_flush))}–{fmt(max(measured_flush))} ms.' if measured_flush else '- Flush-await range: not measured.')
        L += ['- TiKV per-thread CPU: not measured.', '- Why RPS differs or does not differ: not measured; these tables do not establish causation.', '', '## 6. Limitations and file index', '', '- The three scheduled batches have one primary run per new configuration. A single run does not estimate run-to-run variance; any extra run is reported separately and never silently substituted, except the established successful flush-rerun replacement rule.', '- CPU placement and quota are controlled descriptions, not proof of CPU contention or an explanation for throughput differences. The comparison references are earlier measurements.', '- File index: `artifacts/fixconn-tidb-k4t4-n16.csv`, `artifacts/fixconn-tidb-k6t8-n16.csv`; references `artifacts/fixconn-tidb-c4-n16.csv` and `artifacts/fixconn-tidb-c8x-n16.csv`. Attempt evidence is under the `attempt_dir` in each CSV, including `summary.json`, optional `durability.json`, and `client/topology.json`.', '- Missing/invalid measurements are shown as `n/a`; no data is not interpreted as zero.', '']
        text='\n'.join(L)
        return text.replace('nan','n/a').replace('None','n/a')
    finally:
        ROOT, driver.ART, report.ROOT = old_root, old_art, old_report_root


def main():
    text = build()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(text, encoding='utf-8')
    print(f'generated {OUT}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
