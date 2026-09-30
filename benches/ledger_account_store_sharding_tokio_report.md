# Tokio account store sharding benchmark results

This report records the full five-case topology matrix for the standalone
sharding benchmark described in
[`01-21`](../docs/01-21.development-design-ledger-account-store-sharding-tokio-benchmark.md).
The full run completed successfully. A reduced topology smoke remains below as
a preliminary wiring check.

## Full 10M transaction matrix

Exact command:

```sh
BINDGEN_EXTRA_CLANG_ARGS='-I/usr/lib/gcc/x86_64-linux-gnu/13/include' \
  cargo bench --bench ledger_account_store_sharding_tokio
```

The run processed five cases in the fixed order S1, S2 shared, S2 dedicated,
S4 shared, S4 dedicated. Every case used 50,000 users × 200 requests, for
10,000,000 durable transactions split evenly between 5,000,000 credits and
5,000,000 debits. Batch size was 4,096, mode was checkpoint, and the local
checkpoint interval was 100,000 sequences. The Tokio runtime used four worker
threads. RocksDB budgets were 128 MiB total write buffers, 128 MiB total block
cache, and four background jobs across the layout, with at most two write
buffers per DB. The complete matrix took 608.113 seconds.

All five strict idle preflights passed on the first attempt on `/dev/sdb2`
(`8:18`). The observed CPU busy range was 0.918–1.377%, device busy was
0–0.100%, available memory was 42.91–43.48 GiB, and free space was
90.37–90.49 GiB. All cases passed their recovery and namespace-bounded
integrity checks. Each aggregate result row reports 10 million transactions,
5 million credits, 5 million debits, 100 checkpoints enqueued, and 100
checkpoints completed. Local final sequence and checkpoint counts also matched
the expected shard split: 10 million / 100 for S1, 5 million / 50 per shard for
S2, and 2.5 million / 25 per shard for S4.

### Aggregate throughput, latency, CPU, recovery, and database size

Foreground RPS covers transaction processing. Settled RPS includes checkpoint
drain. CPU cores are process CPU time divided by foreground or settled wall
time; these values include RocksDB background threads. Handler mean latency is
computed across all transactions. Percentiles use nearest-rank selection over
the stable 1-in-1,024 transaction sample; the aggregate sample count was 9,646
per case.

| Case | Database layout | Foreground / settled RPS | Mean / p50 / p95 / p99 latency (ms) [samples] | CPU cores, foreground / settled | Local final sequence | Recovery / integrity (s) | DB / sampled peak RSS (MiB) |
| --- | --- | ---: | ---: | ---: | --- | ---: | ---: |
| S1 control | One shared DB | 72,752.2 / 72,749.2 | 55.351 / 55.520 / 86.777 / 99.173 [9,646] | 1.159 / 1.159 | 10.0M | 1.910 / 41.905 | 610.1 / 370.2 |
| S2 shared | One shared DB | 117,312.3 / 117,306.3 | 67.356 / 60.005 / 107.695 / 318.781 [9,646] | 1.893 / 1.893 | 5.0M per shard | 3.523 / 35.378 | 592.5 / 526.7 |
| S2 dedicated | Two DBs | 153,462.4 / 153,453.7 | 52.094 / 49.420 / 81.254 / 174.378 [9,646] | 2.449 / 2.449 | 5.0M per shard | 9.288 / 38.014 | 564.8 / 628.0 |
| S4 shared | One shared DB | 196,024.2 / 196,012.3 | 79.270 / 72.957 / 118.820 / 232.611 [9,646] | 3.651 / 3.651 | 2.5M per shard | 2.594 / 38.198 | 571.7 / 678.0 |
| S4 dedicated | Four DBs | 258,256.2 / 258,236.6 | 59.057 / 55.768 / 88.239 / 226.257 [9,646] | 4.369 / 4.368 | 2.5M per shard | 0.314 / 33.957 | 554.1 / 750.2 |

### RocksDB counters

WAL, flush, and compaction byte counts are decimal GB. Stall time was zero in
all five cases. RocksDB counters were sampled once per database: a shared DB
contributed one ticker delta, and dedicated DB counters were summed.

| Case | WAL syncs | WAL bytes (GB) | Flush write (GB) | Compaction read / write (GB) | Stall time (ms) |
| --- | ---: | ---: | ---: | ---: | ---: |
| S1 control | 2,800 | 1.600 | 0.649 | 1.112 / 1.418 | 0 |
| S2 shared | 2,985 | 1.560 | 0.618 | 0.991 / 1.268 | 0 |
| S2 dedicated | 3,000 | 1.560 | 0.626 | 1.813 / 2.093 | 0 |
| S4 shared | 3,240 | 1.540 | 0.605 | 0.977 / 1.261 | 0 |
| S4 dedicated | 3,400 | 1.540 | 0.609 | 1.754 / 2.049 | 0 |

### Checkpoint timings

Drain is elapsed wall time. The other checkpoint timings are accumulated metric
durations summed across shards, so they can include work that ran concurrently.
The aggregate checkpoint count stayed at 100 for each topology; each shard
produced 100, 50, or 25 checkpoints in S1, S2, or S4 respectively.

| Case | Checkpoints (per shard) | Drain wall (ms) | Snapshot / queue wait / chunk sync / manifest sync / summed duration (ms) |
| --- | ---: | ---: | ---: |
| S1 control | 100 (100) | 5.080 | 117.134 / 0.142 / 376.712 / 93.818 / 591.552 |
| S2 shared | 100 (50 each) | 3.899 | 59.358 / 0.149 / 1,178.412 / 689.143 / 1,932.911 |
| S2 dedicated | 100 (50 each) | 3.180 | 59.678 / 0.142 / 241.011 / 144.490 / 451.052 |
| S4 shared | 100 (25 each) | 2.591 | 30.599 / 0.147 / 537.289 / 279.822 / 861.387 |
| S4 dedicated | 100 (25 each) | 2.429 | 29.630 / 0.153 / 212.593 / 119.706 / 375.383 |

Each local checkpoint serializes its shard namespace. The full DB therefore
checkpoints a 50,000-account namespace 100 times in S1; S2 checkpoints two
25,000-account namespaces 50 times each; S4 checkpoints four 12,500-account
namespaces 25 times each. This changes payload size per checkpoint while the
aggregate checkpoint count stays fixed.

### Process and target-device I/O

The table shows foreground values followed by values after checkpoint drain.
Process `rchar/wchar` are logical bytes passed through read/write calls;
process `read/write` are `/proc/self/io` storage counters. Device values are
the target block device counters. Values are GiB (`2^30` bytes), rounded to
three decimals; the retained CSV has exact byte counts. The target device was
`sdb2` for every case.

| Case | Process rchar / wchar, foreground → settled (GiB) | Process read / write, foreground → settled (GiB) | Device read / write, foreground → settled (GiB) | Device busy, foreground → settled (ms) |
| --- | ---: | ---: | ---: | ---: |
| S1 control | 20.423 / 3.415 → 20.423 / 3.416 | 0.000 / 3.426 → 0.000 / 3.427 | 0.000 / 3.516 → 0.000 / 3.517 | 10,829 → 10,832 |
| S2 shared | 20.568 / 3.209 → 20.568 / 3.210 | 0.000 / 3.221 → 0.000 / 3.221 | 0.000 / 3.303 → 0.000 / 3.303 | 21,357 → 21,360 |
| S2 dedicated | 15.885 / 3.986 → 15.885 / 3.986 | 0.000 / 3.998 → 0.000 / 3.998 | 0.000 / 4.063 → 0.000 / 4.063 | 11,989 → 11,991 |
| S4 shared | 21.081 / 3.173 → 21.082 / 3.173 | 0.000 / 3.186 → 0.000 / 3.186 | 0.000 / 3.237 → 0.000 / 3.237 | 14,473 → 14,475 |
| S4 dedicated | 15.888 / 3.911 → 15.888 / 3.911 | 0.000 / 3.925 → 0.000 / 3.926 | 0.000 / 3.992 → 0.000 / 3.992 | 12,614 → 12,616 |

### Per-shard results

Per-shard RPS uses that shard's own foreground wall time. Shard latency
percentiles use its portion of the same stable sample.

| Case | Shard | Accounts | Transactions / final seq | Checkpoints | RPS | p50 / p95 / p99 latency (ms) | Samples |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| S1 control | 0 | 50,000 | 10,000,000 / 10,000,000 | 100 | 72,752.3 | 55.520 / 86.777 / 99.173 | 9,646 |
| S2 shared | 0 | 25,000 | 5,000,000 / 5,000,000 | 50 | 58,785.2 | 59.557 / 103.726 / 316.710 | 4,779 |
| S2 shared | 1 | 25,000 | 5,000,000 / 5,000,000 | 50 | 58,656.2 | 60.405 / 109.067 / 319.851 | 4,867 |
| S2 dedicated | 0 | 25,000 | 5,000,000 / 5,000,000 | 50 | 76,731.3 | 49.264 / 81.265 / 164.399 | 4,779 |
| S2 dedicated | 1 | 25,000 | 5,000,000 / 5,000,000 | 50 | 76,798.2 | 49.695 / 81.254 / 181.247 | 4,867 |
| S4 shared | 0 | 12,500 | 2,500,000 / 2,500,000 | 25 | 49,375.1 | 74.656 / 115.486 / 224.513 | 2,451 |
| S4 shared | 1 | 12,500 | 2,500,000 / 2,500,000 | 25 | 49,573.6 | 72.850 / 114.831 / 404.019 | 2,428 |
| S4 shared | 2 | 12,500 | 2,500,000 / 2,500,000 | 25 | 49,023.7 | 72.740 / 119.173 / 212.589 | 2,328 |
| S4 shared | 3 | 12,500 | 2,500,000 / 2,500,000 | 25 | 49,006.2 | 71.996 / 121.709 / 217.757 | 2,439 |
| S4 dedicated | 0 | 12,500 | 2,500,000 / 2,500,000 | 25 | 67,823.8 | 54.961 / 85.178 / 212.516 | 2,451 |
| S4 dedicated | 1 | 12,500 | 2,500,000 / 2,500,000 | 25 | 67,578.8 | 56.174 / 85.732 / 309.653 | 2,428 |
| S4 dedicated | 2 | 12,500 | 2,500,000 / 2,500,000 | 25 | 67,162.6 | 55.610 / 90.087 / 218.352 | 2,328 |
| S4 dedicated | 3 | 12,500 | 2,500,000 / 2,500,000 | 25 | 64,564.3 | 56.354 / 89.778 / 203.715 | 2,439 |

### Shared versus dedicated DBs

Ratios compare dedicated DBs with the shared DB at the same shard count. The
latency values are reductions in the dedicated case from the shared case.

| Shards | Foreground / settled throughput ratio | Mean latency reduction | p50 reduction | p95 reduction | p99 reduction |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 2 | 1.308× / 1.308× | 22.7% | 17.6% | 24.6% | 45.3% |
| 4 | 1.317× / 1.317× | 25.5% | 23.6% | 25.7% | 2.7% |

### Run limits and retained data

This matrix is one run in a fixed order, with one measurement per case. It does
not estimate run-to-run variation or remove possible order and cache effects.
The aggregate percentile sample is about 0.1% of transactions; the per-shard
tail samples are smaller, so p95 and p99 should be treated as observations
from this run rather than stable service-level estimates. Checkpoint payload
size also changes with the per-shard account namespace size, as described
above.

The process exited successfully and logged `run_complete=true`. CSV validation
confirmed five result rows with 68 fields each and 13 shard rows with 14
fields each. All transaction, credit/debit, local-sequence, and checkpoint
totals matched the workload. All preflight, recovery, and integrity checks
passed, and no case database directories remain. Committed raw artifacts:
[results CSV](data/ledger_account_store/sharding_results.csv),
[per-shard CSV](data/ledger_account_store/sharding_shards.csv), and
[run log](data/ledger_account_store/sharding_run.log). Original target run
path (provenance):
`target/ledger-account-store-sharding-trials/run-1790704815887432455-21452/`.
No benchmark workload or harness corrections were needed.

## Preliminary topology smoke

The smoke run checked the five layouts, strict preflight, recovery, and
namespace-bounded integrity checks with a reduced workload. It is a wiring
check rather than a topology performance comparison.

Exact command:

```sh
BINDGEN_EXTRA_CLANG_ARGS='-I/usr/lib/gcc/x86_64-linux-gnu/13/include' \
  cargo bench --bench ledger_account_store_sharding_tokio -- \
  --smoke-users 2000 --smoke-waves 20
```

All five cases processed 40,000 durable transactions: 20,000 credits and
20,000 debits, with 2,000 users, 20 requests per user, batch size 4,096, and
four Tokio worker threads. Each per-case strict 3-second idle preflight passed
on its first attempt. CPU busy ranged from 0.792% to 1.356% (10% limit), device
busy from 0% to 0.067% (5% limit), available memory from 46.58 GB to 46.68 GB
(128 MiB minimum), and free space from 97.03 GB to 97.05 GB (configured
reserve plus estimated workload). All five cases closed and reopened their
databases, recovered each shard's expected local sequence, and passed the
namespace-bounded index and ledger integrity scan. The target device was
`/dev/sdb2` (`8:18`). Total run time was 16.975 seconds.

| Case | Layout | Local final seq | Completed RPS | Settled RPS | Handler p50 / p95 / p99 (ms) | CPU cores | Recovery (s) | Integrity scan (s) | DB bytes | Sampled peak RSS (MiB) |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| S1 control | One shared DB | 40,000 | 261,514 | 260,571 | 7.743 / 8.717 / 8.717 | 0.803 | 0.180 | 0.073 | 2,675,957 | 27.5 |
| S2 shared | One shared DB | 20,000 / shard | 352,799 | 351,189 | 5.389 / 8.015 / 8.015 | 1.105 | 0.172 | 0.074 | 2,594,248 | 30.6 |
| S2 dedicated | Two DBs | 20,000 / shard | 463,514 | 460,732 | 4.190 / 5.429 / 5.429 | 1.342 | 0.179 | 0.072 | 2,680,236 | 31.5 |
| S4 shared | One shared DB | 10,000 / shard | 359,653 | 357,938 | 4.500 / 22.038 / 23.404 | 1.252 | 0.176 | 0.072 | 2,530,096 | 36.8 |
| S4 dedicated | Four DBs | 10,000 / shard | 505,095 | 501,717 | 3.600 / 5.493 / 5.493 | 1.657 | 0.201 | 0.071 | 2,781,263 | 41.3 |

The equal configured RocksDB budgets were 128 MiB total write buffers, 128 MiB
total block cache, and four maximum background jobs across the layout. Shared
DB RocksDB counters were sampled once; dedicated DB counters were summed
across distinct DBs. There were zero flush/compaction bytes and zero stall
time in these short runs. Local sequences stayed below the 100,000 checkpoint
threshold, so no checkpoint generations were produced. This smoke therefore
does not measure the default checkpoint work or its per-shard snapshot-size
scaling.

Raw smoke CSV and log files are retained at
`target/ledger-account-store-sharding-trials/run-1790704513496722285-17467/`.
The CSV contains detailed foreground and settled process/device I/O, RocksDB
ticker deltas, checkpoint timings, aggregate latency, and preflight
measurements; `shards.csv` contains per-shard RPS, sequence, and latency data.
Both CSV files have matching header and row widths (68 fields in `results.csv`,
14 in `shards.csv`). The per-case database directories were removed after the
recovery and integrity checks completed.
