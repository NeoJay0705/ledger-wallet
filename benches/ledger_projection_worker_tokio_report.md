# Tokio Ledger Projection Worker Benchmark Results

## Run

Full workload: 50,000 users × 200 requests, 10,000,000 durable records per case, one shard, 4,096-record foreground batches, checkpoint balance mode, four Tokio workers, and 256-record projection batches. Synthetic destination delay was 0 ms.

Exact command:

```sh
BINDGEN_EXTRA_CLANG_ARGS='-I/usr/lib/gcc/x86_64-linux-gnu/13/include' cargo bench --bench ledger_projection_worker_tokio -- --output-root target/ledger-projection-worker-tokio-full-20260930-final
```

The bindgen include path is host-specific. Without it, this host's RocksDB binding build failed because clang could not find stdbool.h.

The run completed all three cases. The retained results.csv is the raw 68-column output copied without transformation; run.log is the exact case summary log. Both are under [benches/data/ledger_projection_worker/](data/ledger_projection_worker/). The executable wrote its originals to target/ledger-projection-worker-tokio-full-20260930-final/.

## Results

| Case | Foreground window | Foreground tx/s | Handler p50 / p95 / p99 (samples) | Measured window | Projected records | Projected records/s | Lag at foreground end | Peak sampled lag | Process CPU / cores | Peak RSS | DB bytes before cleanup |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Live writes, projection disabled | 148.129 s | 67,508.530 | 55,555,838 / 95,796,930 / 191,754,762 ns (9,646) | 148.135 s | 0 | 0 | 10,000,000 | 10,000,000 | 160.507 s / 1.084 | 379,482,112 B | 640,444,161 B |
| Prefill, then catch up | Prefill excluded | 0 | N/A | 22.964 s | 10,000,000 | 435,470.581 | 10,000,000 initial backlog | 10,000,000 | 23.108 s / 1.006 | 2,070,892,544 B | 640,444,583 B |
| Live writes with projection | 142.596 s | 70,128.230 | 58,026,060 / 87,762,360 / 97,923,127 ns (9,646) | 142.602 s | 10,000,000 | 70,125.215 | 848 | 4,608 | 182.405 s / 1.279 | 2,135,478,272 B | 640,446,603 B |

The concurrent foreground rate was 3.881% higher than control in this run (70,128.230 vs. 67,508.530 transactions/s). This is one run per case with no repetitions or confidence interval; the difference is descriptive and does not establish a repeatable performance effect.

Handler latency is a stable deterministic sample: the existing account-store hash filter selects transaction IDs whose mixed account and transaction ID has its low 10 bits clear, about one transaction in 1,024. For each selected transaction, the sample is the full handle_batch wall duration of its containing batch, from API entry until batch processing and checkpoint enqueue finish. It is batch-level time assigned to each sampled transaction, not isolated per-transaction service time. Catch-up has no foreground write handler calls in its measured window, so its value is N/A.

The catch-up prefill happened before the measured window; its foreground transaction rate is therefore zero in that window. Its lag-at-foreground-end field records the initial 10,000,000-record backlog. In the control, peak lag equals the exact final durable sequence because projection is disabled. In the concurrent case, lag was sampled every 5 ms, with the measured foreground-end lag included in the peak.

## Projection batch latency

Values are p50 / p95 / p99 in nanoseconds, followed by the number of measured projection batches. Control values are N/A because that case has no projector; its CSV zero placeholders do not represent zero latency.

| Case | Ledger read and decode | Mock apply | Actual async delay | Dispatch/wait residual | Total batch time |
|---|---:|---:|---:|---:|---:|
| Live writes, projection disabled | N/A | N/A | N/A | N/A | N/A |
| Prefill, then catch up | 436,538 / 492,844 / 545,473 (39,063) | 115,096 / 227,927 / 442,931 (39,063) | 0 / 0 / 0 (39,063) | 19,025 / 23,165 / 27,252 (39,063) | 573,215 / 710,392 / 925,174 (39,063) |
| Live writes with projection | 266,991 / 488,696 / 702,297 (39,200) | 128,241 / 246,272 / 480,140 (39,200) | 0 / 0 / 0 (39,200) | 21,160 / 29,606 / 36,117 (39,200) | 427,702 / 716,233 / 915,696 (39,200) |

Catch-up reads full 256-record batches except for the final partial batch, yielding ceil(10,000,000 / 256) = 39,063 batches. The live worker reads each currently committed prefix as the writer publishes batches of up to 4,096. Because each 50,000-user wave ends with an 848-record foreground batch, the worker can apply additional partial projection batches at those source-head boundaries; this run recorded 39,200 batches.

Ledger-read time is measured inside the spawn_blocking closure and covers RocksDB reads plus decoding. Total time starts before async range dispatch and ends when mock apply finishes. Actual delay measures elapsed time around tokio::time::sleep; dispatch/wait is the nonnegative residual after subtracting ledger read, actual delay, and mock apply from total. Percentiles are calculated independently per stage over all batches.

## I/O and RocksDB counters

| Case | Process read / write bytes | Target device read / write bytes | RocksDB ticker read / write bytes | WAL syncs / bytes | Flush write bytes | Compaction read / write bytes | Stall µs |
|---|---:|---:|---:|---:|---:|---:|---:|
| Live writes, projection disabled | 0 / 3,709,173,760 | 94,208 / 3,725,885,440 | 0 / 1,600,343,208 | 2,799 / 1,600,343,208 | 648,964,139 | 1,137,451,135 / 1,447,264,258 | 0 |
| Prefill, then catch up | 0 / 0 | 0 / 8,564,736 | 312,504 / 0 | 0 / 0 | 0 | 0 / 0 | 0 |
| Live writes with projection | 0 / 3,687,157,760 | 65,536 / 3,719,610,368 | 313,600 / 1,600,343,220 | 2,800 / 1,600,343,220 | 648,964,285 | 1,173,020,581 / 1,425,244,162 | 0 |

Process rchar / wchar deltas were 21,983,453,016 / 3,697,305,630 bytes for control, 293,201,987 / 22,080 bytes for catch-up, and 21,920,701,415 / 3,675,436,852 bytes for concurrent. Device counters are for sdb2 (8:18). RocksDB ticker deltas are not equivalent to returned ledger payload or device bytes; process and device I/O use /proc and /proc/diskstats deltas.

## Preflight and cleanup

The Linux idle preflight used the configured 10% maximum CPU busy, 5% maximum target-device busy, and a free-space gate of 8,753,741,824 bytes (10,000,000 × 768 bytes per record plus a 1 GiB reserve). The memory estimate was 256 bytes per projected record plus 768 MiB headroom. All gates passed without relaxing thresholds:

| Measured case window | Gate attempts | CPU busy | Target-device busy | Available memory | Free space |
|---|---:|---:|---:|---:|---:|
| Control | 2 | 1.502347% | 0.000000% | 43,232,800,768 B | 95,402,377,216 B |
| Catch-up, after prefill and checkpoint drain | 20 | 1.818182% | 0.000000% | 42,754,359,296 B | 94,703,755,264 B |
| Concurrent | 1 | 1.157335% | 0.000000% | 42,794,176,512 B | 95,398,625,280 B |

Catch-up performs a second strict idle preflight after prefill and checkpoint drain. It passed on its 20th observation; this second result is the one reported for the measured catch-up window. All temporary per-case RocksDB directories were removed after store shutdown and handle drop. The final output directory contains only results.csv and run.log.

## Interpretation and limits

The destination is an in-memory mock with atomic batch validation, exact-replay idempotency, and contiguous sequence progress. Its sequence is not durable production projection progress. The mock reserves capacity for all expected projected keys before measured windows, so that capacity reservation is excluded; per-batch staging allocations, hashing, inserts, and payload handling are included. Results therefore do not represent destination database allocation or persistence costs.

The benchmark does not measure a network database, external serialization, retries across process restarts, retention, or garbage collection. The catch-up case is a warm local RocksDB read path after full prefill; its result should not be treated as cold-cache throughput. Concurrent process CPU is process-wide and includes both writer and projector work. RSS is sampled at 10 ms intervals. RocksDB ticker, process I/O, and device I/O counters measure different layers. These are single-run measurements, not a statistical comparison.
