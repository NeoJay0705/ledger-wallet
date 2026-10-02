# Tokio runtime thread scaling benchmark

Run kind: **Complete full matrix**. Completed trials: 36/36.

Workload for this run: **full**, 50,000 users/coroutines x 200 requests (10,000,000 requests per trial), sample stride 64, with 3,000ms resource observations.

## Throughput by scenario and worker count

| Scenario | Workers | RPS median | RPS min | RPS max | Repetition observations (1, 2, 3) |
|---|---:|---:|---:|---:|---|
| foreground_persistence | 3 | 42314.210 | 40215.625 | 42716.961 | 42314.21, 40215.625, 42716.961 |
| foreground_persistence | 4 | 42194.714 | 41885.300 | 42646.548 | 41885.3, 42646.548, 42194.714 |
| foreground_persistence | 6 | 41457.480 | 38790.943 | 41763.357 | 38790.943, 41457.48, 41763.357 |
| foreground_persistence | 8 | 41485.161 | 40654.659 | 42028.490 | 40654.659, 41485.161, 42028.49 |
| integrated_pipeline | 3 | 36864.652 | 33232.778 | 37020.535 | 36864.652, 33232.778, 37020.535 |
| integrated_pipeline | 4 | 36895.045 | 36893.326 | 37016.302 | 37016.302, 36893.326, 36895.045 |
| integrated_pipeline | 6 | 36880.325 | 36737.183 | 36972.846 | 36880.325, 36737.183, 36972.846 |
| integrated_pipeline | 8 | 36625.235 | 36138.720 | 36867.891 | 36138.72, 36625.235, 36867.891 |
| queue_echo | 3 | 2695292.706 | 2601478.042 | 2700354.064 | 2601478.042, 2700354.064, 2695292.706 |
| queue_echo | 4 | 3005474.298 | 2858120.606 | 3027091.413 | 3005474.298, 2858120.606, 3027091.413 |
| queue_echo | 6 | 2827001.015 | 2777494.880 | 2930234.032 | 2777494.88, 2827001.015, 2930234.032 |
| queue_echo | 8 | 2713550.581 | 2644224.357 | 2739649.365 | 2644224.357, 2739649.365, 2713550.581 |

Percentiles are trial-level nearest-rank samples. This report lists trial percentiles below and does not treat their median as a pooled percentile.

## Request latency observations

| Scenario | Workers | p50 observations (ns) | p95 observations (ns) | p99 observations (ns) |
|---|---:|---|---|---|
| foreground_persistence | 3 | 1173622653, 1184836666, 1163356797 | 1777669317, 1818251513, 1723368585 | 2061058428, 4039871566, 2141336509 |
| foreground_persistence | 4 | 1178753219, 1157739454, 1165313140 | 1777848193, 1752832126, 1772846336 | 2120311365, 2130229115, 2098389878 |
| foreground_persistence | 6 | 1197880578, 1191515457, 1164156202 | 1866773546, 1799049218, 1788854883 | 4565029899, 2043404860, 2104517400 |
| foreground_persistence | 8 | 1201544474, 1167677536, 1166798017 | 1813757797, 1800054482, 1771721450 | 2208492546, 2152723106, 2114516717 |
| integrated_pipeline | 3 | 1333134295, 1314342741, 1325952191 | 2095206308, 2105486083, 2068038319 | 2434177440, 11747531383, 2411307390 |
| integrated_pipeline | 4 | 1322460449, 1333585420, 1326752264 | 2060255285, 2084993514, 2075881670 | 2503881716, 2483038051, 2479852499 |
| integrated_pipeline | 6 | 1333219392, 1337250244, 1329838753 | 2070139718, 2079467825, 2065287403 | 2485035382, 2452847466, 2506809059 |
| integrated_pipeline | 8 | 1359082985, 1341153165, 1322778294 | 2111703677, 2099938160, 2063326645 | 2474607624, 2471331140, 2476742473 |
| queue_echo | 3 | 18288040, 18037080, 18110110 | 24001998, 20679023, 20549602 | 25508070, 23131592, 22302036 |
| queue_echo | 4 | 15850349, 16566288, 15713054 | 19951176, 21438692, 19867522 | 21353755, 22700046, 20774543 |
| queue_echo | 6 | 17758138, 17273134, 17159943 | 20241676, 19399531, 18735667 | 21433662, 20448837, 19911310 |
| queue_echo | 8 | 18867727, 17932158, 18323812 | 21244094, 20826615, 20458784 | 22662933, 22619778, 21264455 |

## Per-trial observations

| Trial | Scenario | Workers | Rep | Requests | RPS | Process CPU cores | CPU ns/request | p50 ns | p95 ns | p99 ns |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | queue_echo | 3 | 1 | 10000000 | 2601478.042 | 2.673548 | 1027.708 | 18288040 | 24001998 | 25508070 |
| 2 | foreground_persistence | 3 | 1 | 10000000 | 42314.21 | 1.154924 | 27294.008 | 1173622653 | 1777669317 | 2061058428 |
| 3 | integrated_pipeline | 3 | 1 | 10000000 | 36864.652 | 1.254515 | 34030.288 | 1333134295 | 2095206308 | 2434177440 |
| 4 | queue_echo | 4 | 1 | 10000000 | 3005474.298 | 3.238747 | 1077.621 | 15850349 | 19951176 | 21353755 |
| 5 | foreground_persistence | 4 | 1 | 10000000 | 41885.3 | 1.179225 | 28153.666 | 1178753219 | 1777848193 | 2120311365 |
| 6 | integrated_pipeline | 4 | 1 | 10000000 | 37016.302 | 1.273023 | 34390.871 | 1322460449 | 2060255285 | 2503881716 |
| 7 | queue_echo | 6 | 1 | 10000000 | 2777494.88 | 4.437047 | 1597.508 | 17758138 | 20241676 | 21433662 |
| 8 | foreground_persistence | 6 | 1 | 10000000 | 38790.943 | 1.179164 | 30397.909 | 1197880578 | 1866773546 | 4565029899 |
| 9 | integrated_pipeline | 6 | 1 | 10000000 | 36880.325 | 1.350168 | 36609.437 | 1333219392 | 2070139718 | 2485035382 |
| 10 | queue_echo | 8 | 1 | 10000000 | 2644224.357 | 5.052052 | 1910.606 | 18867727 | 21244094 | 22662933 |
| 11 | foreground_persistence | 8 | 1 | 10000000 | 40654.659 | 1.306764 | 32143.026 | 1201544474 | 1813757797 | 2208492546 |
| 12 | integrated_pipeline | 8 | 1 | 10000000 | 36138.72 | 1.420106 | 39295.971 | 1359082985 | 2111703677 | 2474607624 |
| 13 | queue_echo | 4 | 2 | 10000000 | 2858120.606 | 3.243539 | 1134.855 | 16566288 | 21438692 | 22700046 |
| 14 | foreground_persistence | 4 | 2 | 10000000 | 42646.548 | 1.175288 | 27558.818 | 1157739454 | 1752832126 | 2130229115 |
| 15 | integrated_pipeline | 4 | 2 | 10000000 | 36893.326 | 1.271931 | 34475.901 | 1333585420 | 2084993514 | 2483038051 |
| 16 | queue_echo | 6 | 2 | 10000000 | 2827001.015 | 4.503577 | 1593.064 | 17273134 | 19399531 | 20448837 |
| 17 | foreground_persistence | 6 | 2 | 10000000 | 41457.48 | 1.262121 | 30443.745 | 1191515457 | 1799049218 | 2043404860 |
| 18 | integrated_pipeline | 6 | 2 | 10000000 | 36737.183 | 1.351464 | 36787.372 | 1337250244 | 2079467825 | 2452847466 |
| 19 | queue_echo | 8 | 2 | 10000000 | 2739649.365 | 5.072301 | 1851.448 | 17932158 | 20826615 | 22619778 |
| 20 | foreground_persistence | 8 | 2 | 10000000 | 41485.161 | 1.332415 | 32117.864 | 1167677536 | 1800054482 | 2152723106 |
| 21 | integrated_pipeline | 8 | 2 | 10000000 | 36625.235 | 1.420207 | 38776.743 | 1341153165 | 2099938160 | 2471331140 |
| 22 | queue_echo | 3 | 2 | 10000000 | 2700354.064 | 2.661106 | 985.469 | 18037080 | 20679023 | 23131592 |
| 23 | foreground_persistence | 3 | 2 | 10000000 | 40215.625 | 1.09808 | 27304.814 | 1184836666 | 1818251513 | 4039871566 |
| 24 | integrated_pipeline | 3 | 2 | 10000000 | 33232.778 | 1.120517 | 33717.226 | 1314342741 | 2105486083 | 11747531383 |
| 25 | queue_echo | 6 | 3 | 10000000 | 2930234.032 | 4.403403 | 1502.754 | 17159943 | 18735667 | 19911310 |
| 26 | foreground_persistence | 6 | 3 | 10000000 | 41763.357 | 1.251516 | 29966.846 | 1164156202 | 1788854883 | 2104517400 |
| 27 | integrated_pipeline | 6 | 3 | 10000000 | 36972.846 | 1.359364 | 36766.537 | 1329838753 | 2065287403 | 2506809059 |
| 28 | queue_echo | 8 | 3 | 10000000 | 2713550.581 | 5.309293 | 1956.593 | 18323812 | 20458784 | 21264455 |
| 29 | foreground_persistence | 8 | 3 | 10000000 | 42028.49 | 1.338278 | 31842.172 | 1166798017 | 1771721450 | 2114516717 |
| 30 | integrated_pipeline | 8 | 3 | 10000000 | 36867.891 | 1.436526 | 38964.150 | 1322778294 | 2063326645 | 2476742473 |
| 31 | queue_echo | 3 | 3 | 10000000 | 2695292.706 | 2.649479 | 983.007 | 18110110 | 20549602 | 22302036 |
| 32 | foreground_persistence | 3 | 3 | 10000000 | 42716.961 | 1.15329 | 26998.407 | 1163356797 | 1723368585 | 2141336509 |
| 33 | integrated_pipeline | 3 | 3 | 10000000 | 37020.535 | 1.252041 | 33820.184 | 1325952191 | 2068038319 | 2411307390 |
| 34 | queue_echo | 4 | 3 | 10000000 | 3027091.413 | 3.237921 | 1069.653 | 15713054 | 19867522 | 20774543 |
| 35 | foreground_persistence | 4 | 3 | 10000000 | 42194.714 | 1.164784 | 27604.976 | 1165313140 | 1772846336 | 2098389878 |
| 36 | integrated_pipeline | 4 | 3 | 10000000 | 36895.045 | 1.270961 | 34448.012 | 1326752264 | 2075881670 | 2479852499 |

## Method and limits

The full profile uses 50,000 request coroutines/accounts and exactly 10,000,000 requests per trial, with one outstanding request per coroutine, a 50,000-entry bounded queue, batch size 2,048, a 5 ms first-dequeue timeout, and deterministic 50/50 amount-1 credits and debits for storage trials. Foreground persistence uses PerBatch balances and seeds 150,000 history records at balance 100; it leaves projector, watermark manager, and GC unspawned during measurement and does not perform final projection catch-up. Its unprojected record backlog after client completion is expected by design. Integrated pipeline uses projection batches of 256, GC batches of 256, 100 ms GC/watermark intervals, 500 ms retention, and a bounded GC worker that yields during catch-up and sleeps while blocked. GC backlog includes records newer than or otherwise ineligible for its durable retention prefix.

Each trial runs in a fresh child process and Tokio runtime. Thread order rotates by repetition; actual order is in `trial_manifest.csv`. All children inherit the same CPU affinity. Full preflights require CPU busy at or below 10%, target-device busy at or below 5%, plus workload-specific memory and free-space reserves. The runtime worker count controls Tokio async workers; Tokio blocking-pool and RocksDB internal threads keep their existing defaults.

Process CPU is measured to the latest reply endpoint in each scenario. Queue echo samples process CPU immediately after a coroutine's final reply and records `cpu_sample_after_reply_ns`; storage trials retain the source benchmark's recorded CPU sample offset. This adds one process CPU clock sample after each coroutine's last reply. RPS interval boundaries differ slightly: queue echo spans the earliest individual request start through latest response observation, while storage spans the pre-release measurement start through latest route reply. Request latency is sampled at stride 64 in full trials and 1 in smoke runs. Queue echo process and device I/O counters are sampled after worker join and their delay after the latest reply is recorded in `io_sample_offset_us`; pipeline snapshots retain the original `io_sample_offset_us` and storage snapshot offsets. Process and device counters for these scopes include any tail work in the recorded delay. RocksDB read timing includes range-read decoding; GC sync-write timing covers synchronous WAL batch write and is not an fsync-only measure. Per-file I/O attribution is unsupported.

Setup, seed preparation, preflight, settlement, final projection/GC work, recovery, and integrity validation are excluded from client throughput. The aggregate CSV preserves every trial's p50/p95/p99; medians across repetitions are descriptive and are not pooled percentiles or significance claims.

## Files

`ledger_thread_scaling_summary.csv` contains one row per completed trial; `ledger_thread_scaling_stages.csv` contains request and background stage percentile summaries; `ledger_thread_scaling_background_summary.csv` contains projector/GC progress and RocksDB background counters; `trial_manifest.csv`, `run_metadata.txt`, and each trial's stdout/stderr retain execution evidence. Full per-event pipeline diagnostics are removed after extracting these summaries.

## Reproduction

```sh
BINDGEN_EXTRA_CLANG_ARGS='-I/usr/lib/gcc/x86_64-linux-gnu/13/include' cargo bench --locked --bench ledger_thread_scaling_tokio
```

Smoke validation runs the same 36 settings with 200 users/coroutines x 200 requests and 100ms resource observations:

```sh
BINDGEN_EXTRA_CLANG_ARGS='-I/usr/lib/gcc/x86_64-linux-gnu/13/include' cargo bench --locked --bench ledger_thread_scaling_tokio -- --smoke
```

