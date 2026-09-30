# Tokio Account Store benchmark results

This report covers the standalone direct-batch account-store benchmark described in [the stage 2 design](../docs/01-20.development-design-ledger-account-store-tokio-benchmark.md). The default full matrix completed successfully with one repetition for each of the four combinations. Workload planning and fresh database initialization preceded each case's default strict idle preflight; the foreground window began immediately after preflight passed.

## Full 10M-request matrix

Exact command:

```sh
BINDGEN_EXTRA_CLANG_ARGS='-I/usr/lib/gcc/x86_64-linux-gnu/13/include' \
  cargo bench --bench ledger_account_store_tokio
```

Committed raw artifacts: [results CSV](data/ledger_account_store/baseline_results.csv) and [run log](data/ledger_account_store/baseline_run.log). Original target run path (provenance): `target/ledger-account-store-trials/run-1790694629351298127-13488/`.

The retained CSV has 79 aligned columns and four passed rows. Every case processed 10,000,000 logical requests and committed 9,000,000 transactions: 4,000,000 credits, 4,000,000 debits, 1,000,000 successful refunds, and 1,000,000 balance queries. All transaction replies were applied, every queried and recovered balance matched the expected final balance of 20 for all 50,000 users, and latest sequence was 9,000,000. Checkpoint mode completed and enqueued exactly 90 checkpoints with zero lag. All cases passed close/reopen recovery and the separate integrity scan. Each trial DB directory was removed after measuring its on-disk size; CSV and log were retained.

All four default strict preflights passed. Across cases CPU busy ranged 1.128–1.190% (10% maximum), target device busy 0.000–0.400% (5% maximum), available memory 46.66–46.88 GB (128 MiB minimum), and free space 97.62–97.63 GB (at least 1 GiB reserve plus the configured request estimate). The target was `/dev/sdb2` (`8:18`).

### Throughput and durability

| Case | Balance mode | Batch | Foreground wall (s) | Foreground all RPS | Foreground tx RPS | Settled wall (s) | Settled all RPS | Settled tx RPS | Checkpoints / lag | Recovery (s) | Integrity scan (s) | DB bytes | Status |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| case-01 | per-batch | 2,048 | 144.698 | 69,109.32 | 62,198.39 | 144.699 | 69,109.04 | 62,198.14 | 0 / 0 | 1.915 | 28.021 | 610,773,975 | passed |
| case-02 | checkpoint | 2,048 | 139.242 | 71,817.18 | 64,635.46 | 139.248 | 71,814.10 | 64,632.69 | 90 / 0 | 1.679 | 26.903 | 616,258,709 | passed |
| case-03 | per-batch | 4,096 | 140.268 | 71,291.90 | 64,162.71 | 140.269 | 71,291.60 | 64,162.44 | 0 / 0 | 1.581 | 30.101 | 610,739,357 | passed |
| case-04 | checkpoint | 4,096 | 147.272 | 67,901.53 | 61,111.38 | 147.277 | 67,899.28 | 61,109.35 | 90 / 0 | 0.733 | 21.746 | 617,065,758 | passed |

Settled wall/CPU starts at foreground start and ends when the checkpoint-drain barrier completes. The interval includes the foreground I/O-counter sampling gap before drain starts, so settled wall is not exactly foreground wall plus `checkpoint_drain_wall_s`. Settled RPS uses that measured interval. Recovery and integrity timings are outside both request windows.

### Request-path latency

Transaction handler mean/count include every transaction request; percentiles use the deterministic sampled requests (8,732 transaction samples per case). Balance query mean/count include every query; each case has 966 deterministic query samples. Handler spans represent batch-shared wait through commit, memory publish, and checkpoint enqueue backpressure. Balance query timings are the direct memory path. The table reports transaction percentiles in milliseconds and query percentiles in nanoseconds.

| Case | Tx mean (ms) | Tx p50 (ms) | Tx p95 (ms) | Tx p99 (ms) | Query mean (ns) | Query p50 (ns) | Query p95 (ns) | Query p99 (ns) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| case-01 | 32.640 | 32.364 | 49.133 | 60.232 | 106.59 | 121 | 160 | 421 |
| case-02 | 31.388 | 30.246 | 47.268 | 127.621 | 106.01 | 130 | 151 | 371 |
| case-03 | 63.153 | 63.225 | 94.501 | 120.023 | 97.06 | 70 | 150 | 291 |
| case-04 | 66.218 | 61.285 | 119.311 | 201.610 | 99.08 | 120 | 151 | 350 |

### CPU and I/O

CPU is `ProcessTime`. Process `rchar/wchar` are bytes passed through read/write system calls; process read/write bytes and target-device read/write bytes are storage I/O counters. Values below are foreground counters; the CSV also preserves settled counters after checkpoint drain.

| Case | Foreground CPU (s) | Foreground cores | Settled CPU (s) | Settled cores | Process rchar (GB) | Process wchar (GB) | Process storage read/write (GB) | Target read/write (GB) | Target busy (ms) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| case-01 | 166.486 | 1.151 | 166.487 | 1.151 | 24.376 | 3.839 | 0.000 / 3.858 | 0.000 / 3.929 | 13,109 |
| case-02 | 146.900 | 1.055 | 146.903 | 1.055 | 22.630 | 3.198 | 0.000 / 3.218 | 0.000 / 3.334 | 17,932 |
| case-03 | 165.427 | 1.179 | 165.428 | 1.179 | 24.319 | 3.787 | 0.000 / 3.797 | 0.000 / 3.841 | 11,300 |
| case-04 | 146.864 | 0.997 | 146.866 | 0.997 | 22.645 | 3.206 | 0.000 / 3.216 | 0.000 / 3.306 | 27,211 |

### Per-batch timing phases

These means use the reported batch count as denominator. Read/build includes index and refund lookups plus ledger/index construction, but excludes final latest-sequence/balance-key staging. The synchronous write phase includes that staging and `DB::write_opt` with `sync=true`; it is not fsync-only. Memory publish ends before checkpoint snapshot capture.

| Case | Batches | Read/build mean (ms) | Synced batch-write mean (ms) | Memory publish mean (ms) |
| --- | ---: | ---: | ---: | ---: |
| case-01 | 4,460 | 24.385 | 7.393 | 0.216 |
| case-02 | 4,460 | 23.656 | 6.850 | 0.219 |
| case-03 | 2,260 | 47.775 | 13.124 | 0.386 |
| case-04 | 2,260 | 46.533 | 17.330 | 0.401 |

### Checkpoint and RocksDB counters

| Case | Snapshot capture (s) | Queue wait (s) | Chunk sync (s) | Manifest sync (s) | Checkpoint total (s) | WAL sync count | WAL bytes | Writes with WAL | Flush write bytes | Compaction read/write bytes | Stall (µs) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| case-01 | 0 | 0 | 0 | 0 | 0 | 4,460 | 1,559,156,100 | 4,460 | 623,733,287 | 1,557,975,567 / 1,655,917,446 | 0 |
| case-02 | 0.110 | 0.000484 | 0.328 | 0.097 | 0.545 | 4,637 | 1,451,326,959 | 4,640 | 605,318,132 | 1,096,809,974 / 1,142,094,952 | 0 |
| case-03 | 0 | 0 | 0 | 0 | 0 | 2,260 | 1,559,079,100 | 2,260 | 626,640,385 | 1,495,487,156 / 1,600,964,116 | 0 |
| case-04 | 0.112 | 0.000486 | 0.949 | 1.001 | 2.070 | 2,439 | 1,451,249,983 | 2,440 | 607,511,771 | 1,097,327,090 / 1,147,103,271 | 0 |

Chunk and manifest sync times are observed per checkpoint writer; checkpoint totals can overlap foreground work. Phase values are not additive parts of foreground latency. Per-batch WAL counters include synchronous transaction-batch writes and RocksDB internal activity during the measured request-plus-drain interval.

## Smoke validation

The release smoke used 10 users × 20 waves, batch size 32, both balance modes, checkpoint quantity 10, and relaxed 100% CPU/disk preflight thresholds. Both cases passed preflight, recovered successfully, and passed integrity validation. Its output is retained at `target/ledger-account-store-smoke/run-1790694473073209167-11253/`.

## Limitations

Each full-matrix case ran once, in a fixed order, on one shared host and target block device. The p95/p99 values are descriptive for the deterministic sample in that run; they do not establish causal effects or statistical significance between modes or batch sizes. In particular, b4096 checkpoint was associated with a 201.610 ms handler p99 and 2.070 s checkpoint total versus 127.621 ms and 0.545 s for b2048 checkpoint; these single-run observations do not show that batch size caused the difference. Workload plan generation (0.051295 s) and DB initialization are excluded from request RPS; transaction structs are assembled from the precomputed plan inside the foreground window. Checkpoint recovery was separately timed, and the full ledger/index integrity scan is reported separately rather than charged to recovery.

## Credit/debit 50/50 full matrix

The `credit-debit-50-50` profile runs 50,000 users × 200 requests per case. Each user's deterministic 10-op pattern repeats 20 times and contains 5 amount-1 credits and 5 amount-1 debits interleaved in a deterministic order that starts with a credit, never overdrafts, and returns to zero; all replies are `Applied`. Account IDs use varying valid patterns, so each interior pattern wave includes both operation types. The first wave is necessarily all credit and the last necessarily all debit given initial balance zero, credit-first patterns, and final balance zero. There are no refunds or balance queries.

Exact command:

```sh
BINDGEN_EXTRA_CLANG_ARGS='-I/usr/lib/gcc/x86_64-linux-gnu/13/include' \
  cargo bench --bench ledger_account_store_tokio -- \
  --workload credit-debit-50-50
```

The default strict per-case preflight passed for all four cases. The target was `/dev/sdb2` (`8:18`); observed CPU busy was 0.918–1.086% against the 10% limit, device busy was 0.000–0.100% against the 5% limit, available memory was at least 46.61 GB against 128 MiB, and free space was at least 97.52 GB against the configured reserve plus request estimate. Each case processed 10,000,000 logical requests and committed exactly 10,000,000 new transactions: 5,000,000 credits, 5,000,000 debits, zero refunds, and zero balance queries. Every case recovered latest sequence 10,000,000 and zero balance for all 50,000 users, passed the full integrity scan, measured DB size, and removed its trial DB directory. The two checkpoint cases completed and enqueued exactly 100 snapshots with zero lag.

Committed raw artifacts: [results CSV](data/ledger_account_store/credit_debit_50_50_results.csv) and [run log](data/ledger_account_store/credit_debit_50_50_run.log). Original target run path (provenance): `target/ledger-account-store-trials/run-1790699020493837436-5993/`. The CSV contains 81 aligned columns and four passed rows. Query latency has `not_applicable` status, blank latency values, and sample count 0 in every row; no query latency was measured.

### Throughput and transaction latency

RPS uses all 10,000,000 requests, which are all transactions in this profile. Transaction means include every handler request; percentiles use 9,646 deterministic samples per case.

| Case | Balance mode | Batch | Foreground wall (s) | Foreground RPS | Settled wall (s) | Settled RPS | Tx mean (ms) | Tx p50 (ms) | Tx p95 (ms) | Tx p99 (ms) | Tx samples | Query latency |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| case-01 | per-batch | 2,048 | 148.510 | 67,335.37 | 148.511 | 67,335.12 | 29.999 | 29.757 | 43.166 | 55.106 | 9,646 | not applicable; count 0 |
| case-02 | checkpoint | 2,048 | 137.236 | 72,867.13 | 137.241 | 72,864.27 | 27.715 | 27.416 | 42.038 | 45.409 | 9,646 | not applicable; count 0 |
| case-03 | per-batch | 4,096 | 142.741 | 70,056.80 | 142.742 | 70,056.55 | 57.384 | 56.857 | 84.154 | 103.010 | 9,646 | not applicable; count 0 |
| case-04 | checkpoint | 4,096 | 133.228 | 75,059.44 | 133.233 | 75,056.45 | 53.545 | 52.724 | 81.461 | 104.985 | 9,646 | not applicable; count 0 |

### CPU and I/O

CPU is `ProcessTime`. Process `rchar/wchar` count bytes passed through read/write system calls; process read/write and target read/write count storage I/O. Values below are foreground counters; the CSV also retains settled counters.

| Case | Foreground CPU (s) | Foreground cores | Settled CPU (s) | Settled cores | Process rchar/wchar (GB) | Process storage read/write (GB) | Target read/write (GB) | Target busy (ms) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| case-01 | 169.580 | 1.142 | 169.581 | 1.142 | 25.278 / 3.987 | 0.000 / 4.008 | 0.000 / 4.139 | 14,628 |
| case-02 | 154.760 | 1.128 | 154.768 | 1.128 | 23.965 / 3.573 | 0.000 / 3.595 | 0.000 / 3.684 | 12,632 |
| case-03 | 167.780 | 1.175 | 167.780 | 1.175 | 25.293 / 3.987 | 0.000 / 3.999 | 0.000 / 4.083 | 11,332 |
| case-04 | 153.649 | 1.153 | 153.657 | 1.153 | 24.007 / 3.587 | 0.000 / 3.599 | 0.000 / 3.644 | 10,502 |

### Checkpoints and recovery

| Case | Checkpoints completed / enqueued / lag | Snapshot capture (s) | Queue wait (s) | Chunk sync (s) | Manifest sync (s) | Checkpoint total (s) | Recovery (s) | Integrity scan (s) | DB bytes |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| case-01 | 0 / 0 / 0 | 0 | 0 | 0 | 0 | 0 | 2.995 | 35.352 | 621,066,212 |
| case-02 | 100 / 100 / 0 | 0.118 | 0.001 | 0.412 | 0.105 | 0.637 | 2.765 | 24.121 | 627,572,615 |
| case-03 | 0 / 0 / 0 | 0 | 0 | 0 | 0 | 0 | 0.756 | 34.286 | 621,081,932 |
| case-04 | 100 / 100 / 0 | 0.121 | 0.001 | 0.441 | 0.106 | 0.667 | 2.628 | 25.059 | 627,538,097 |

### Profile smoke validation

Release smokes ran both the original baseline and the new profile with 10 users × 20 waves, batch size 32, both modes, checkpoint quantity 10, and relaxed CPU/device-busy preflight thresholds. All four cases passed preflight, recovery, and integrity validation. Baseline cases committed 180 transactions (80 credits, 80 debits, 20 refunds, 20 queries) and ended at balance 2 per user; checkpoint mode completed 18 snapshots. The new profile committed 200 transactions (100 credits, 100 debits, no refunds/queries) and ended at balance 0 per user; checkpoint mode completed 20 snapshots. The new profile CSV leaves query latency blank with `not_applicable` and count 0.

- Baseline smoke: `target/ledger-account-store-smoke-baseline/run-1790698921777212830-4397/`
- 50/50 smoke: `target/ledger-account-store-smoke-credit-debit-50-50/run-1790698941009803536-4702/`

The four 50/50 results are a single fixed-order run on the same host and device as the retained baseline results. Do not treat cross-profile throughput differences as causal: each baseline case commits 9,000,000 transactions plus 1,000,000 in-memory balance queries, while each new profile case commits 10,000,000 durable transactions and has no queries. The equal logical request totals therefore have different durable-write counts. Results within and across these one-run matrices are descriptive, not statistically significant comparisons.
