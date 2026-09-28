# RocksDB 大批次尾延遲歸因報告

**範圍：** batch size 2,048、4,096；baseline、diagnostic 與 CPU diagnostic 各三輪。報告以 baseline 的 throughput/latency 作性能數字，以 CPU diagnostic trace 作機制歸因。diagnostic 會啟用 RocksDB PerfContext、記錄逐批 trace，並增加量測成本；其 RPS 不視為一般模式的性能結果。

## 方法與執行方式

三組量測每輪都寫入並查詢 10,000,000 筆交易，batch size 為 2,048 和 4,096，各重複三次。查詢以固定 seed 洗牌既有 transaction IDs，使用 RocksDB batched multiget。baseline 不帶 `--diagnostic`；兩組逐批診斷帶 `--diagnostic`，CPU 詳細組另保存 worker thread CPU 與 `RUSAGE_THREAD` counter。實際執行沿用預設 10M iterations；以下是明列 `--iterations 10000000` 的等價重現命令。複跑時請將輸出目錄改成未使用的新路徑。

```sh
BINDGEN_EXTRA_CLANG_ARGS='-I/usr/lib/gcc/x86_64-linux-gnu/13/include' \
  cargo bench --bench rocksdb_tokio_sequential -- \
  --iterations 10000000 --repetitions 3 --batch-sizes 2048,4096 \
  --output-dir target/rocksdb-tail-attribution-repro
```

診斷兩組在上面命令加入 `--diagnostic`。三組都沿用 strict preflight 預設值：CPU busy 上限 10%、目標磁碟 busy 上限 5%；沒有放寬門檻。只有 preflight 回報 `ready` 才開始該 phase。

## 完整性檢查

三套結果 CSV 各有六列、每列記錄 10M write 和 10M query；三套共 18 個 trials 皆 exit 0 並完成驗證。每組六個 trial 都通過 strict preflight，所有 12 個 write/query phase 最後均達 `ready`。CPU 詳細組的 12 個 phase preflight 共有 14 次 wait，之後全部恢復 ready。

每份 2,048 trace 有 9,766 列（4,883 write batches 與 4,883 query batches）；每份 4,096 trace 有 4,884 列（各 2,442 batches）。CPU 詳細組六份 trace 共 43,950 列，每列 47 欄；`residual_ns` 全為 0，且 `native_call_thread_cpu_ns` 均不超過 `native_call_ns` 1 μs 以上。較早的 diagnostic trace 使用較早的 36 欄格式，列數與 residual 檢查也完整。run log 記錄每次 trial DB cleanup 完成；沒有留下 `trial-*` 目錄。

## Baseline 性能數字

下表為各 batch size 三輪的中位數。延遲分位數以交易數加權；CPU cores 是整個 benchmark process 的 CPU time 除以該 phase wall time。延遲單位為毫秒。

| Batch | Write / query RPS | Write / query CPU cores | Write p50 / p95 / p99 | Query p50 / p95 / p99 |
|---:|---:|---:|---:|---:|
| 2,048 | 535,303 / 231,992 | 1.262 / 1.035 | 3.598 / 4.182 / 7.327 | 8.419 / 10.014 / 19.280 |
| 4,096 | 622,500 / 233,345 | 1.431 / 1.004 | 6.152 / 7.307 / 24.273 | 17.230 / 19.302 / 20.495 |

CPU diagnostic 三輪中位數另列如下，供讀者看出診斷模式與 baseline 的差異，不作一般性能比較。其 query RPS 約低於 baseline 7.7%（2,048）及 8.8%（4,096）；差異包含 instrumentation 與執行環境變動。

| Batch | Write / query RPS | Write / query CPU cores | Write / query p99 (ms) |
|---:|---:|---:|---:|
| 2,048 | 531,393 / 214,086 | 1.262 / 1.032 | 9.684 / 17.972 |
| 4,096 | 622,439 / 212,750 | 1.439 / 1.032 | 24.358 / 36.129 |

## 尾端計算方式

逐批比較在**同一 trial、同一 phase**內依 `batch_total_ns` 排序，以批次列數取最慢 top 1% 和其餘 lower 99%；每輪 2,048 取 49 個 top batches，4,096 取 25 個。尾端增量是兩組的平均 batch latency 之差；native call 的歸因比例則是同一組之 native call 平均值差除以該尾端增量。此算法不會拿 write/query 兩個 phase 的 p99 相減。

CSV 的交易 p99 與以下 trace p99.9 都是按 `item_count` 加權的 nearest-rank 分位數。每個 batch 完成時間由該批所有交易共享，並非逐筆啟動計時器所得的獨立交易延遲。

## 寫入尾延遲

CPU 詳細組六輪中，top 1% slow batches 相對 lower 99% 的額外 batch latency，有 99.96%–100.02% 落在 `native_call_ns`。`write_wal_time` 的相同增量約為額外 native 時間的 98.73%–99.62%；`write_delay_time` 在所有 batch 都是 0。每批恰有一次 WAL sync：2,048 每輪 4,883 次、4,096 每輪 2,442 次。

每輪 RocksDB WAL sync histogram 的最大值，與逐批 trace 的最大 `write_wal_time` 僅差 0.196–0.380 ms。下表也列出同輪最慢 write batch 的 native wall 與 worker thread CPU；長達約 141–169 ms 的 native call 中，thread CPU 僅約 2.4–4.3 ms。

| Batch | Rep | WAL histogram max (ms) | Trace max `write_wal_time` (ms) | 最慢 batch native wall (ms) | 同批 thread CPU (ms) |
|---:|---:|---:|---:|---:|---:|
| 2,048 | 1 | 139.129 | 139.339 | 141.535 | 2.471 |
| 2,048 | 2 | 138.955 | 139.151 | 141.323 | 2.437 |
| 2,048 | 3 | 141.697 | 141.896 | 144.041 | 2.422 |
| 4,096 | 1 | 164.597 | 164.915 | 168.458 | 3.908 |
| 4,096 | 2 | 165.077 | 165.457 | 168.605 | 3.593 |
| 4,096 | 3 | 162.410 | 162.790 | 166.658 | 4.329 |

RocksDB v11.8.1 原始碼將 `write_wal_time` 的計時範圍放在 `WriteGroupToWAL` 周圍，該路徑包含 WAL write 與 sync：[計時位置](https://github.com/facebook/rocksdb/blob/v11.8.1/db/db_impl/db_impl_write.cc#L1366-L1369)、[WAL 寫入與 Sync 路徑](https://github.com/facebook/rocksdb/blob/v11.8.1/db/db_impl/db_impl_write.cc#L2350-L2394)。結合 WAL histogram 與逐批指標，可高度確定這些極端 write tail 主要是同步 WAL file `Sync` 等待。這些指標無法再區分儲存裝置 firmware、kernel 或其他 I/O 競爭。

## 查詢尾延遲

CPU 詳細組 top 1% slow query batches 相對 lower 99% 的額外 batch latency，有 94.89%–99.76% 落在 native multiget call。六輪逐批 `native_call_thread_cpu_ns / native_call_ns` 中位數約 99.98%–99.99%；top 1% 平均比值約 99.4%–99.97%。六輪合計 21,975 個 query batches 的 major page faults 均為 0，只有 132 個 batch 有 voluntary context switch。CPU 幾乎用滿 native call wall time，證據支持查詢 tail 主要是 RocksDB lookup 的 CPU 工作，而非 OS wait 或 I/O 阻塞；context switch 計數本身不作阻塞原因證明。

在 2,048 rep1/rep2、4,096 rep1/rep2 四個 query phase，query phase 期間同時觀察到 compaction reads 約 68–72 MiB、writes 約 65–69 MiB。開頭較慢區段的 109、74、38、53 batches 是依逐批 `user_key_comparison_count` 階躍作事後描述性分段：下一批的 count 從約 181k（2,048）或 364k（4,096）降到約 26k 或 52k，並伴隨 batch latency 下降。這不是預先設定的統計閾值，也不是 compaction 完成 timestamp：

| Batch / rep | 開頭較慢 batches | 開頭區段耗時 (s) | 開頭平均 batch latency (ms) | 後續平均 batch latency (ms) | Query compaction read / write (MiB) |
|---|---:|---:|---:|---:|---:|
| 2,048 / 1 | 109 | 1.986 | 18.2 | 約 9 | 71.76 / 68.53 |
| 2,048 / 2 | 74 | 1.623 | 21.9 | 約 9 | 68.17 / 65.53 |
| 4,096 / 1 | 38 | 1.646 | 43.3 | 約 18–19 | 68.28 / 65.53 |
| 4,096 / 2 | 53 | 1.948 | 36.8 | 約 18–19 | 68.26 / 65.53 |

依前述同 trial/phase 的 top 1% 對 lower 99% 分組，這四個 phase 中 slow 與 normal batches 的 `rocksdb_user_key_comparison_count` 約為 2,048 的 181k 對 26–28k、4,096 的 364k 對 54–56k；`rocksdb_block_seek_ns` 約為 2,048 的 8–10 ms 對 1.6–1.7 ms、4,096 的 16–19 ms 對 3.1 ms。slow/normal 的 block read count 幾乎相同；query `/proc/self/io` `read_bytes` 增量為 0 至 4 KiB。另兩輪（rep3）沒有 query compaction，也沒有開頭高延遲；query p99 分別為 2,048 的 11.297 ms、4,096 的 22.268 ms，低於有 compaction 的輪次（2,048：17.972/21.916 ms；4,096：43.520/36.129 ms）。

這些 trace 可直接將較慢 query batch 歸因於當時較高的 key comparison 與 block seek CPU 工作；背景 compaction 與延遲下降有強時間關聯。compaction 完成後 lookup amplification 下降是合理推測，但沒有逐批 SST file count 或 compaction completion timestamp，不能宣稱已直接證明 LSM 檔案層級變化。PerfContext 的 `block_read_time` 在 trace 全為 0；這不表示沒有讀取，因 `block_read_count` 非 0。CPU time、block read count 與 `/proc/self/io` counter 一併作為判讀依據。

後續[平行查詢吞吐實驗](rocksdb_tokio_parallel_query_report.md)量測不同在途 batch 上限下的 aggregate throughput 與 batch 完成時間；該測試沒有隔離 key comparison 或 LSM 狀態，因此不能確認這裡觀察到的 key-comparison/LSM 尾延遲成因。

## CPU diagnostic 逐輪延遲分位數

以下列出每輪交易加權 p99（summary CSV）及 trace `item_count` 加權 nearest-rank p99.9，單位為毫秒。p99.9 是 trace 依 batch latency 排序並累加 `item_count` 到 `ceil(10,000,000 × 0.999)` 所得。

| Batch | Rep | Write p99 | Query p99 | Write p99.9 | Query p99.9 |
|---:|---:|---:|---:|---:|---:|
| 2,048 | 1 | 10.440 | 17.972 | 115.892 | 19.590 |
| 2,048 | 2 | 9.684 | 21.916 | 90.312 | 22.788 |
| 2,048 | 3 | 7.539 | 11.297 | 120.058 | 12.441 |
| 4,096 | 1 | 23.085 | 43.520 | 147.879 | 44.293 |
| 4,096 | 2 | 26.943 | 36.129 | 147.502 | 41.232 |
| 4,096 | 3 | 24.358 | 22.268 | 138.465 | 23.268 |

## 限制與證據邊界

- 性能數字取 baseline 三輪中位數。CPU diagnostic 啟用額外計時與 PerfContext 收集，且 query RPS 低於 baseline；診斷數據用來定位延遲組成，不代表無診斷模式的吞吐量。
- 寫入證據指向同步 WAL `Sync` 等待，但無法由現有計數區分 firmware、kernel、其他 I/O 競爭等更下層來源。
- 查詢證據支持 key comparison/block seek 的 CPU 工作；compaction 和開頭延遲段落具有時間關聯。缺少逐批 SST 數量及 compaction 完成時間，因此 LSM 檔案層級的變化仍屬推測。
- p99/p99.9 是 batch 完成時間依交易數加權的分位數；同批交易共享該時間。不同 phase 的 p99 不能相減；尾端增量只按同一 trial/phase 的 top 1% 與 lower 99% 平均值比較。
- `block_read_time=0` 不等於沒有 block read；trace 中 `block_read_count` 非 0，且另有 CPU 及 process I/O counter 可對照。

## 原始資料與設計文件

| 類別 | 原始資料 |
|---|---|
| Baseline | [results CSV](rocksdb_tail_baseline_results.csv) · [run log](rocksdb_tail_baseline_run.log) |
| Diagnostic | [results CSV](rocksdb_tail_diagnostic_results.csv) · [run log](rocksdb_tail_diagnostic_run.log) |
| CPU diagnostic | [results CSV](rocksdb_tail_cpu_diagnostic_results.csv) · [run log](rocksdb_tail_cpu_diagnostic_run.log) |
| 設計與背景 | [尾延遲歸因設計](../docs/01-12.development-design-rocksdb-tail-latency-attribution.md) · [既有 RocksDB benchmark 報告](rocksdb_tokio_sequential_report.md) |

較早一套 diagnostic trace：

- 2,048：[rep1](rocksdb_tail_attribution_data/diagnostic/rocksdb-tokio-sequential-b2048-rep1-1790444822262864034.trace.csv)、[rep2](rocksdb_tail_attribution_data/diagnostic/rocksdb-tokio-sequential-b2048-rep2-1790444895358045332.trace.csv)、[rep3](rocksdb_tail_attribution_data/diagnostic/rocksdb-tokio-sequential-b2048-rep3-1790444966433239714.trace.csv)
- 4,096：[rep1](rocksdb_tail_attribution_data/diagnostic/rocksdb-tokio-sequential-b4096-rep1-1790445058196933451.trace.csv)、[rep2](rocksdb_tail_attribution_data/diagnostic/rocksdb-tokio-sequential-b4096-rep2-1790445132900969753.trace.csv)、[rep3](rocksdb_tail_attribution_data/diagnostic/rocksdb-tokio-sequential-b4096-rep3-1790445204148525130.trace.csv)

CPU 詳細 trace：

- 2,048：[rep1](rocksdb_tail_attribution_data/diagnostic_cpu/rocksdb-tokio-sequential-b2048-rep1-1790446085422075717.trace.csv)、[rep2](rocksdb_tail_attribution_data/diagnostic_cpu/rocksdb-tokio-sequential-b2048-rep2-1790446157517388176.trace.csv)、[rep3](rocksdb_tail_attribution_data/diagnostic_cpu/rocksdb-tokio-sequential-b2048-rep3-1790446229644866564.trace.csv)
- 4,096：[rep1](rocksdb_tail_attribution_data/diagnostic_cpu/rocksdb-tokio-sequential-b4096-rep1-1790446327668450652.trace.csv)、[rep2](rocksdb_tail_attribution_data/diagnostic_cpu/rocksdb-tokio-sequential-b4096-rep2-1790446397061381719.trace.csv)、[rep3](rocksdb_tail_attribution_data/diagnostic_cpu/rocksdb-tokio-sequential-b4096-rep3-1790446467339272272.trace.csv)
