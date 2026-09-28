# Tokio 並行 RocksDB ledger benchmark 實測報告

**執行日期：** 2026-09-27。完整矩陣成功完成 24/24 trials；每個 trial 寫入 10,000,000 筆交易，關閉 writer DB 並重開後，完整驗證 10,000,000 次 shuffled ledger lookup。

## 執行結果

在 batch size 2,048 時，4 個在途 query batches 得到本輪最高 median throughput：733,227 RPS，約為 sequential baseline 的 3.18 倍。把上限提高到 8 後，median RPS 降至 712,442，CPU 使用升至 7.56 core equivalents，batch submission-to-completion p99 從 14.394 ms 升至 29.175 ms。

在 batch size 4,096 時，8 個在途 batches 比 4 個多 2.3% median RPS（694,035 對 678,194），但 CPU core equivalents 從 3.919 升至 7.642，p99 batch latency 從 31.921 ms 升至 50.931 ms。兩種 batch size 都顯示並行可提升 aggregate throughput；提高到 8 個在途 batches 後，延遲與 CPU 成本都更高。

下表每個情境有 3 個 trials。RPS、CPU 與各 latency percentile 都是先在每個 trial 計算，再取三次 trial 的中位數。Sequential speedup 使用既有 sequential CSV 中相同 batch size 的三次 query RPS 中位數，僅作方向參考，並非同一時間配對執行。

| Batch | 在途 query batches 上限 | Query RPS 中位數 | Sequential RPS 比率 | CPU core equivalents | Batch submission-to-completion p50 / p95 / p99 (ms) | Native multiget p50 / p95 / p99 (ms) | Blocking-pool wait p99 (ms) |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 2,048 | 1 | 234,484 | 1.02× | 1.004 | 8.594 / 9.749 / 10.767 | 8.376 / 9.500 / 10.418 | 0.0133 |
| 2,048 | 2 | 441,048 | 1.91× | 1.996 | 9.018 / 10.316 / 11.535 | 8.729 / 9.998 / 11.213 | 0.0144 |
| 2,048 | 4 | 733,227 | 3.18× | 3.937 | 10.717 / 13.440 / 14.394 | 10.356 / 13.036 / 13.976 | 0.0232 |
| 2,048 | 8 | 712,442 | 3.09× | 7.560 | 22.192 / 27.129 / 29.175 | 21.689 / 26.595 / 28.599 | 0.0387 |
| 4,096 | 1 | 225,767 | 0.99× | 1.004 | 17.644 / 20.279 / 21.221 | 17.099 / 19.697 / 20.610 | 0.0205 |
| 4,096 | 2 | 435,914 | 1.92× | 1.995 | 18.417 / 20.540 / 22.463 | 17.786 / 19.871 / 21.781 | 0.0159 |
| 4,096 | 4 | 678,194 | 2.98× | 3.919 | 23.532 / 27.763 / 31.921 | 22.592 / 26.942 / 31.060 | 0.0182 |
| 4,096 | 8 | 694,035 | 3.05× | 7.642 | 47.169 / 49.512 / 50.931 | 45.216 / 47.573 / 49.524 | 0.0761 |

`query_rps` 是完成驗證的 lookup 數除以整個 query phase wall time。CPU core equivalents 是 process CPU seconds 除以同一 phase 的 wall seconds，涵蓋 Tokio runtime、blocking workers 和 process 內其他工作。

`query_batch_submission_to_completion` 每批記錄一次，從提交 `spawn_blocking` 到主 runtime 收到 JoinSet 結果，包含 blocking-pool wait、native lookup、解碼驗證與完成訊息返回。此表中的 p50/p95/p99 是每個 trial 的逐批分布，再取三次 trial 的中位數；它不是每筆交易各自量得的延遲。交易加權 batch latency 另保留在 `query_tx_latency_*` 欄位。`query_batched_multi_get_call` 只計時 RocksDB `batched_multi_get_cf` native call。Blocking-pool wait 的 p99 在 0.0133–0.0761 ms，遠低於 native multiget p99；這些 run 中的大部分批次尾端時間落在 RocksDB 呼叫內。

## 與既有 sequential baseline 比較

既有 [sequential report](rocksdb_tokio_sequential_report.md) 在 batch size 2,048 的 median query RPS 為 230,614，在 4,096 為 227,342。新 benchmark 的 concurrency 1 結果分別是 234,484 和 225,767，接近同一 throughput 範圍；提高並行上限後，RPS 約隨 concurrency 1.9 倍、3 倍成長至 2 或 4 個在途 batches。這是兩次獨立執行的比較：它們沒有共用時間、trial 順序或 cache 狀態，因此比率不能視為嚴格配對的因果效果。兩個 benchmark 都先寫入，再關閉並重開 DB；重開不會清除 RocksDB 或 OS page cache。

## 完整性與 strict preflight

24 個 rows 的每一列都記錄 10M writes 和 10M 完整驗證的 queries。Batch size 2,048 的 write/query 各有 4,883 batches；batch size 4,096 各有 2,442 batches。寫入階段仍依序處理同一份 workload；只有 query phase 的在途 batch 上限改變。每個 query result 帶回原始 batch index，逐 ID 配對驗證完整 ledger entry，並檢查 batch index 沒有越界或重複、所有 batches 都完成且總 lookup 數正確。

每個 write/query phase 開始前都使用 3 秒 idle observation、CPU busy 上限 10%、目標裝置 `/dev/sdb2` busy 上限 5%；timeout 為 60 秒。48 個 phase gates 共留下 115 筆 observation：48 ready、67 wait。全部 write phases 在第 1 次 observation 通過；query phases 都在 timeout 內通過。67 次 wait 全由 disk busy 超過 5% 觸發，沒有 CPU、可用空間或記憶體門檻造成的 wait。通過的 samples 中 CPU busy 最高 7.891%、disk busy 最高 4.766%；沒有放寬任何門檻。每個 trial 完成後都關閉 DB handles 並移除唯一 DB directory；執行後 output directory 沒有殘留的 `trial-*` 目錄。

Query preflight 各情境的 observation 次數（rep1/rep2/rep3）如下；所有 write preflight 均為 1/1/1：

| Batch | 在途 query batches 上限 | Query preflight attempts (rep1 / rep2 / rep3) |
|---:|---:|---:|
| 2,048 | 1 | 4 / 1 / 7 |
| 2,048 | 2 | 2 / 1 / 8 |
| 2,048 | 4 | 3 / 2 / 7 |
| 2,048 | 8 | 3 / 1 / 9 |
| 4,096 | 1 | 3 / 2 / 7 |
| 4,096 | 2 | 2 / 2 / 7 |
| 4,096 | 4 | 2 / 2 / 7 |
| 4,096 | 8 | 2 / 2 / 5 |

RocksDB WAL sync ticker delta 在每一列都等於 write batch 數：4,883 或 2,442；query phase 的 WAL sync delta 全為 0。Query `flush_write_bytes` 與 background error delta 全為 0；非同步 compaction ticker 中位數為 0，最大觀察值是 73,516,648 bytes read 和 70,807,490 bytes written。這些 phase counters 可包含 RocksDB background work。

`/proc/self/io` 的 query `read_bytes` 在 24 次中有 22 次為 0，另 2 次各為 4 KiB；這符合 query 大多沒有程序歸帳的實體讀取，但無法區分 RocksDB block cache、OS page cache 或其他快取層。Query I/O counters、裝置 busy counter、RocksDB ticker、histogram、properties 和 heartbeat 都保留在原始 CSV。裝置 busy 是整個 block device 的活動，不是本 process 獨佔的 I/O。

## 執行環境、驗證與原始資料

本次在同一 host 執行；硬體及 OS 資訊見 [sequential report](rocksdb_tokio_sequential_report.md)。Cargo/RocksDB build 需要額外指定 GCC 13 的標準 header 搜尋路徑，因為此 host 沒有 `/usr/include/stdbool.h`：

```sh
BINDGEN_EXTRA_CLANG_ARGS='-I/usr/lib/gcc/x86_64-linux-gnu/13/include' \
  cargo check --bench rocksdb_tokio_parallel_query
```

Compile check 成功且沒有 compiler warnings。之後以 50,000 筆交易、兩種 batch size、四種 concurrency、每組一次的 smoke workload 實際執行，輸出 8 筆完整資料列；16 個 write/query preflight gates 全部 ready，所有 rows 都驗證 50,000 writes 和 50,000 queries。

完整矩陣命令使用預設 10M workload、3 repetitions、兩種 batch size 和四種 concurrency；strict preflight 參數明確固定為 3 秒、60 秒 timeout、10% CPU、5% disk：

```sh
BINDGEN_EXTRA_CLANG_ARGS='-I/usr/lib/gcc/x86_64-linux-gnu/13/include' \
  cargo bench --bench rocksdb_tokio_parallel_query -- \
  --iterations 10000000 --repetitions 3 \
  --batch-sizes 2048,4096 --max-concurrent-batches 1,2,4,8 \
  --output-dir target/rocksdb-tokio-parallel-query-full \
  --preflight-observation-ms 3000 --preflight-timeout-ms 60000 \
  --preflight-max-cpu-pct 10 --preflight-max-disk-busy-pct 5
```

| Artifact | Contents |
|---|---|
| [Full results CSV](rocksdb_tokio_parallel_query_results.csv) | 215 columns; 24 complete trial rows |
| [Full run log](rocksdb_tokio_parallel_query_run.log) | command output and all 115 strict-preflight observations |
| [Smoke CSV](rocksdb_tokio_parallel_query_smoke_results.csv) | 8 correctness/smoke rows |
| [Smoke log](rocksdb_tokio_parallel_query_smoke.log) | smoke build output and preflight observations |
| [Design document](../docs/01-13.development-design-rocksdb-parallel-query-benchmark.md) | workload, metrics, preflight, CLI and cleanup design |

The sequential benchmark source and its prior CSV/report remain unchanged.
