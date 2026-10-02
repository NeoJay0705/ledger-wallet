# Ledger Pipeline 分片基準報告

本報告來自 `run-1790925219743034498`，profile 為 **正式矩陣**。8 cases × 3 trials 已完成，共 24 child、240,000,000 requests，parent 與 24 個 child 均 exit 0。每 trial 有 155,808 個 request samples 與 36 個 stage identities。DB reopen、餘額／sequence／投影／GC 安全性驗證通過，owned DB scratch 全部刪除；10/10 source hashes matched。報告表由驗證過的 trial 資料衍生。完整原始 trial CSV、child logs/status、run manifest 與 RocksDB options 保存在 [archive index](data/ledger_pipeline_sharding/run-1790925219743034498/evidence_archive.md) 所述 Git 外本機 gzip archive。

## 固定工作負載

正式比較只包含 S2/S4、shared/dedicated RocksDB、Chunked group 256 的 P4/P8，共 8 cases；每 case 3 次旋轉順序的全新 trial。每 trial 為 50,000 個全域 account、每 account 200 個循序請求，共 10,000,000 個請求；每 account 先在計時窗外寫入 3 筆 seed。請求各半 credit/debit、金額 1，期末餘額 100。account ID 以 `account_id % shard_count` 路由，同一 account 的請求保持順序。

固定參數：4 個 Tokio async workers；每 trial 有 50000 個總 queue slots，按 shard 平分；batch 2048、首次 dequeue 等待 5 ms；PerBatch 原子同步帳本寫入；projector/GC batch 256、watermark tick 100 ms、retention 500 ms、GC 閒置 tick 100 ms。4 workers 不代表 CPU 配額。每個 DB 的 write buffer 與 cache 按 DB 數切分，總設定各 128 MiB；每 DB 的 `max_background_jobs` 合計 8。child 共用 RocksDB default Env，LOW/HIGH thread pool 分別設為 6/2；pool 與 jobs 是設定值，不是硬體或活躍執行緒上限。

## 正式矩陣結果

每列彙整同一 case 的 3 個 trial：RPS、CPU、有限 drain RPS 顯示 median [min–max]；request p50/p95/p99 顯示各 trial percentile 的 median，p99 另列 trial 間範圍。延遲單位為 ms。

| Case | client RPS | CPU cores | CPU µs/request | request p50 / p95 / p99 ms | p99 trial 範圍 ms | finite drain RPS | client-end projection / destination / GC backlog records | trial summaries |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| s2_shared_chunked256_c4 | 97411.933 [84116.623–105138.897] | 3.627 [3.119–3.876] | 37.077 [36.862–37.234] | 409.676 / 628.481 / 3878.714 | 756.440–8400.170 | 26576.790 [25794.057–28002.248] | 2734464 / 2733952 / 8897904 | [r1](data/ledger_pipeline_sharding/run-1790925219743034498/trials/trial-01-s2_shared_chunked256_c4-p01/summary.csv) [r2](data/ledger_pipeline_sharding/run-1790925219743034498/trials/trial-02-s2_shared_chunked256_c4-p06/summary.csv) [r3](data/ledger_pipeline_sharding/run-1790925219743034498/trials/trial-03-s2_shared_chunked256_c4-p03/summary.csv) |
| s2_shared_chunked256_c8 | 126816.837 [126687.636–127472.515] | 4.975 [4.941–4.979] | 39.027 [39.001–39.260] | 378.101 / 573.644 / 639.004 | 633.049–665.460 | 29477.602 [28187.359–29873.903] | 4129408 / 4129152 / 8897392 | [r1](data/ledger_pipeline_sharding/run-1790925219743034498/trials/trial-01-s2_shared_chunked256_c8-p02/summary.csv) [r2](data/ledger_pipeline_sharding/run-1790925219743034498/trials/trial-02-s2_shared_chunked256_c8-p07/summary.csv) [r3](data/ledger_pipeline_sharding/run-1790925219743034498/trials/trial-03-s2_shared_chunked256_c8-p04/summary.csv) |
| s2_dedicated_chunked256_c4 | 134942.042 [91398.445–135171.283] | 4.840 [3.238–4.855] | 35.803 [35.427–35.979] | 349.800 / 678.842 / 729.769 | 722.399–4827.803 | 27957.987 [27805.994–29463.342] | 1809536 / 1809024 / 8891760 | [r1](data/ledger_pipeline_sharding/run-1790925219743034498/trials/trial-01-s2_dedicated_chunked256_c4-p03/summary.csv) [r2](data/ledger_pipeline_sharding/run-1790925219743034498/trials/trial-02-s2_dedicated_chunked256_c4-p08/summary.csv) [r3](data/ledger_pipeline_sharding/run-1790925219743034498/trials/trial-03-s2_dedicated_chunked256_c4-p05/summary.csv) |
| s2_dedicated_chunked256_c8 | 135557.296 [123829.782–147130.348] | 5.018 [4.584–5.412] | 37.016 [36.783–37.020] | 320.246 / 646.338 / 703.898 | 681.381–743.979 | 28153.969 [28141.480–28888.941] | 3371392 / 3371136 / 8894832 | [r1](data/ledger_pipeline_sharding/run-1790925219743034498/trials/trial-01-s2_dedicated_chunked256_c8-p04/summary.csv) [r2](data/ledger_pipeline_sharding/run-1790925219743034498/trials/trial-02-s2_dedicated_chunked256_c8-p01/summary.csv) [r3](data/ledger_pipeline_sharding/run-1790925219743034498/trials/trial-03-s2_dedicated_chunked256_c8-p06/summary.csv) |
| s4_shared_chunked256_c4 | 175708.139 [143376.182–175848.389] | 7.189 [5.889–7.209] | 40.998 [40.912–41.073] | 265.494 / 449.072 / 491.831 | 490.999–533.961 | 37057.229 [36893.531–43999.192] | 4367488 / 4366720 / 8887408 | [r1](data/ledger_pipeline_sharding/run-1790925219743034498/trials/trial-01-s4_shared_chunked256_c4-p05/summary.csv) [r2](data/ledger_pipeline_sharding/run-1790925219743034498/trials/trial-02-s4_shared_chunked256_c4-p02/summary.csv) [r3](data/ledger_pipeline_sharding/run-1790925219743034498/trials/trial-03-s4_shared_chunked256_c4-p07/summary.csv) |
| s4_shared_chunked256_c8 | 165201.802 [144767.070–180568.303] | 7.058 [6.212–7.822] | 42.914 [42.724–43.317] | 257.283 / 448.395 / 550.234 | 497.699–4144.427 | 37340.380 [36957.222–37487.497] | 5210496 / 5209984 / 8892784 | [r1](data/ledger_pipeline_sharding/run-1790925219743034498/trials/trial-01-s4_shared_chunked256_c8-p06/summary.csv) [r2](data/ledger_pipeline_sharding/run-1790925219743034498/trials/trial-02-s4_shared_chunked256_c8-p03/summary.csv) [r3](data/ledger_pipeline_sharding/run-1790925219743034498/trials/trial-03-s4_shared_chunked256_c8-p08/summary.csv) |
| s4_dedicated_chunked256_c4 | 125832.617 [122609.239–126380.653] | 4.551 [4.423–4.577] | 36.167 [36.077–36.217] | 196.244 / 556.154 / 7230.121 | 6237.390–7251.140 | 37267.331 [31310.173–37756.019] | 3012224 / 3011712 / 8889968 | [r1](data/ledger_pipeline_sharding/run-1790925219743034498/trials/trial-01-s4_dedicated_chunked256_c4-p07/summary.csv) [r2](data/ledger_pipeline_sharding/run-1790925219743034498/trials/trial-02-s4_dedicated_chunked256_c4-p04/summary.csv) [r3](data/ledger_pipeline_sharding/run-1790925219743034498/trials/trial-03-s4_dedicated_chunked256_c4-p01/summary.csv) |
| s4_dedicated_chunked256_c8 | 228511.276 [141657.941–228877.765] | 8.449 [5.271–8.458] | 37.011 [36.914–37.209] | 186.861 / 407.732 / 818.792 | 817.401–7492.739 | 38156.310 [34455.816–38310.298] | 4015744 / 4015232 / 8890480 | [r1](data/ledger_pipeline_sharding/run-1790925219743034498/trials/trial-01-s4_dedicated_chunked256_c8-p08/summary.csv) [r2](data/ledger_pipeline_sharding/run-1790925219743034498/trials/trial-02-s4_dedicated_chunked256_c8-p05/summary.csv) [r3](data/ledger_pipeline_sharding/run-1790925219743034498/trials/trial-03-s4_dedicated_chunked256_c8-p02/summary.csv) |

儲存與資源量測使用每 case 三次 trial 的 median；bytes/syncs 為 RocksDB/process/device counter delta，非每 shard 歸因。

| Case | WAL syncs | WAL bytes | flush write bytes | compaction read/write bytes | stall µs | process read/write bytes | device read/write bytes | VmHWM at client end MiB | final DB MiB | preflight CPU/device max % |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| s2_shared_chunked256_c4 | 28967 | 1831626075 | 666700558 | 1198811744 / 1583510007 | 0 | 0 / 4202778624 | 0 / 4600635392 | 1944.7 | 96.4 | 2.43 / 3.70 |
| s2_shared_chunked256_c8 | 23501 | 1831406683 | 656359562 | 1213594755 / 1590689885 | 0 | 0 / 4176101376 | 4096 / 4464058368 | 1985.2 | 134.0 | 2.13 / 0.33 |
| s2_dedicated_chunked256_c4 | 41797 | 1832182818 | 660259895 | 2152165977 / 2524791516 | 0 | 0 / 5189619712 | 0 / 5632176128 | 1948.2 | 24.8 | 2.26 / 0.37 |
| s2_dedicated_chunked256_c8 | 35678 | 1831781233 | 660287350 | 2143054976 / 2508251402 | 0 | 0 / 5149102080 | 0 / 5537488896 | 1973.6 | 26.0 | 2.62 / 4.33 |
| s4_shared_chunked256_c4 | 12498 | 1831753217 | 662194880 | 1124249401 / 1475313862 | 0 | 0 / 4025020416 | 4096 / 4214202368 | 2031.4 | 109.0 | 2.20 / 0.13 |
| s4_shared_chunked256_c8 | 10484 | 1831355629 | 659296082 | 1115033222 / 1451725621 | 0 | 0 / 3988021248 | 61440 / 4090187776 | 2121.5 | 112.3 | 2.24 / 4.80 |
| s4_dedicated_chunked256_c4 | 37123 | 1832093158 | 656482286 | 2285022268 / 2671529299 | 0 | 0 / 5315563520 | 0 / 5697900544 | 2028.1 | 67.4 | 2.36 / 0.17 |
| s4_dedicated_chunked256_c8 | 33196 | 1831873409 | 657183645 | 2267122675 / 2633061708 | 0 | 0 / 5263380480 | 0 / 5380960256 | 2066.5 | 67.6 | 2.20 / 0.20 |

Per-shard common-window RPS、client-end 與 final sequence/progress、GC prefix、final watermark timestamp/target 序號、每 batch 查詢峰值見各 trial 的 `shards.csv`；36 個固定 stage 在 `stages.csv`，raw samples 分別在 `raw_stage_samples.csv` 與 `request_samples.csv`。

## 矩陣內描述性差異

下表使用各組 trial median 計算百分比差異；只描述本矩陣的觀測，不作因果或飽和推論。

| 配對 | client RPS 差異 | CPU cores 差異 |
|---|---:|---:|
| S2 P4: dedicated 相對 shared | +38.53% | +33.43% |
| S2 P8: dedicated 相對 shared | +6.89% | +0.87% |
| S2 shared: P8 相對 P4 | +30.19% | +37.16% |
| S2 dedicated: P8 相對 P4 | +0.46% | +3.69% |
| S4 P4: dedicated 相對 shared | -28.39% | -36.69% |
| S4 P8: dedicated 相對 shared | +38.32% | +19.70% |
| S4 shared: P8 相對 P4 | -5.98% | -1.81% |
| S4 dedicated: P8 相對 P4 | +81.60% | +85.65% |
| shared P4: S4 相對 S2 | +80.38% | +98.20% |
| shared P8: S4 相對 S2 | +30.27% | +41.87% |
| dedicated P4: S4 相對 S2 | -6.75% | -5.96% |
| dedicated P8: S4 相對 S2 | +68.57% | +68.36% |

歷史 S1 基線只作背景描述：P4 為 56,982.273 RPS、2.093051 CPU cores、request p50/p95/p99 為 733.594762/1,131.694630/5,850.762527 ms；P8 為 75,076.856 RPS、3.177564 CPU cores、652.001851/957.783005/1,124.482976 ms。該基線使用不同時點、RocksDB 預設資源配置與舊 namespace，屬未配對比較，不能用來主張因果速度提升。詳見[舊報告](ledger_pipeline_index_lookup_tokio_report.md)。

## 結果解讀

- 本次最高前景 RPS 中位數為 S4/dedicated/P8 的 **228,511.276**，CPU 為 **8.448744 core equivalents**。三輪 RPS 為 141,657.941–228,877.765，p99 為 817.401–7,492.739 ms；兩輪較快、一輪明顯較慢，不能只用中位數承諾 tail SLA。
- S4/shared/P4 的 request p99 中位數最低，為 **491.831 ms**；三輪為 490.999–533.961 ms。RPS 中位數為 175,708.139，範圍 143,376.182–175,848.389。尾延遲較一致，吞吐仍有波動。S2/shared/P8 的吞吐最接近，三輪為 126,687.636–127,472.515，p99 為 633.049–665.460 ms；CPU 中位數為 4.974933。
- DB 拓樸沒有全面勝者：S2/P4 的 dedicated 相對 shared RPS +38.53%，S4/P4 則 -28.39%。需同時看 S、P、tail 與背景進度。
- S2/dedicated P4→P8 的前景 RPS 中位數 +0.46%、CPU +3.69%；S4/shared P4→P8 的 RPS -5.98%。這只說明 P8 在這些配置未帶來吞吐改善，不證明已找到飽和點或最優 P。
- S4/dedicated P4→P8 的前景 RPS +81.60%、CPU +85.65%，finite drain RPS 僅 +2.39%（37,267.331→38,156.310）。前景提高沒有等比例提高本次有限工作集的結算速度。
- Stage 觀測：S4/dedicated/P4 的 query-wall p99 約 20.758 ms、sync WriteBatch p99 570.358 ms、batch-gate p99 552.122 ms、GC sync-write p99 545.621 ms。長尾與 queue、gate 及較長同步寫入等待同時出現。程式中前景 handler 與 safe GC 共用同 shard 的 batch gate，會互斥；gate 等待也包含喚醒排程。這不能指定某次 fsync、compaction 或 native lock 為根因；不同 stage 的 p99 不可相加。
- 在本次 client-window counters 中，dedicated 配置的 compaction write bytes 中位數比同 S/P 的 shared 高約 57.7%–81.4%，process write bytes 高約 23.3%–32.1%；WAL bytes 各組約 1.83 GB。較高前景 RPS 不等於較低儲存寫入成本。這些是各自 client window 的觀測，background 進度與 wall 長度不同，不能當成完成整個 pipeline 後的總 write amplification，也不能單憑 counter 指定 tail 原因。
- 本次每 trial 的 process physical read 為 0–4,096 bytes，目標 partition read 為 0–1,556,480 bytes。資料經過 seed 並可由 cache 服務，這不是 cold-read benchmark。24 個 trial 的 RocksDB stall counter 都是 0；同步寫入與 GC 仍有長尾，不能據此稱所有寫入都沒有等待。

## 逐段 latency 對照

前景表：**p50 / p95 / p99，ms**；每個值是各 trial percentile 的三次 median，不跨 trial 合併。每個前景 sample 是一次 batch（最多 2,048 筆）。

| Case | `index.batch_gate_wait` | `index.query_wall` | `index.sync_write_batch` |
|---|---:|---:|---:|
| s2_shared_chunked256_c4 | 8.917 / 13.980 / 143.332 | 9.576 / 16.535 / 19.036 | 10.365 / 17.559 / 151.527 |
| s2_shared_chunked256_c8 | 9.122 / 18.877 / 40.719 | 5.838 / 10.321 / 12.568 | 11.078 / 16.536 / 36.658 |
| s2_dedicated_chunked256_c4 | 6.083 / 10.880 / 30.023 | 8.162 / 12.908 / 14.828 | 9.519 / 12.756 / 28.402 |
| s2_dedicated_chunked256_c8 | 6.233 / 11.859 / 38.564 | 5.100 / 9.534 / 11.708 | 9.697 / 12.999 / 31.348 |
| s4_shared_chunked256_c4 | 12.166 / 21.669 / 85.915 | 12.009 / 23.554 / 27.949 | 12.743 / 22.523 / 96.296 |
| s4_shared_chunked256_c8 | 12.412 / 27.406 / 141.868 | 8.634 / 19.043 / 23.958 | 14.005 / 22.426 / 142.967 |
| s4_dedicated_chunked256_c4 | 7.193 / 16.678 / 552.122 | 8.673 / 17.252 / 20.758 | 10.177 / 19.757 / 570.358 |
| s4_dedicated_chunked256_c8 | 7.658 / 17.362 / 54.317 | 6.230 / 14.224 / 18.501 | 10.474 / 18.241 / 33.733 |

背景表：**p50 / p95 / p99，ms**；每個值是各 trial percentile 的三次 median，不跨 trial 合併。Projector 與 GC sample 最多涵蓋 256 筆；watermark sample 是一次成功 publication。GC samples 包含 no-op。只納入 client window 內完成的背景事件；跨過 last reply 才完成的事件不在表內，因此不代表全部背景 event 的 tail latency。

| Case | `projection.read` | `projection.progress_sync` | `watermark.persist` | `gc.scan` | `gc.sync_write` |
|---|---:|---:|---:|---:|---:|
| s2_shared_chunked256_c4 | 1.275 / 2.161 / 2.578 | 1.793 / 10.976 / 17.400 | 2.546 / 11.178 / 44.647 | 6.663 / 11.611 / 13.746 | 2.531 / 8.459 / 135.255 |
| s2_shared_chunked256_c8 | 1.344 / 2.279 / 2.728 | 2.301 / 10.993 / 15.769 | 7.143 / 12.718 / 12.718 | 7.039 / 12.779 / 15.089 | 2.437 / 9.382 / 34.957 |
| s2_dedicated_chunked256_c4 | 1.159 / 2.026 / 2.404 | 1.221 / 10.755 / 14.197 | 1.448 / 10.890 / 17.896 | 5.592 / 9.204 / 10.571 | 2.030 / 3.811 / 25.703 |
| s2_dedicated_chunked256_c8 | 1.223 / 2.124 / 2.606 | 1.464 / 11.231 / 17.142 | 1.648 / 526.879 / 526.879 | 5.716 / 9.687 / 11.342 | 1.963 / 4.125 / 28.839 |
| s4_shared_chunked256_c4 | 1.676 / 2.716 / 3.432 | 8.112 / 15.966 / 32.108 | 8.436 / 15.333 / 18.728 | 8.264 / 14.984 / 18.117 | 5.974 / 13.890 / 50.877 |
| s4_shared_chunked256_c8 | 1.758 / 3.047 / 4.533 | 9.008 / 16.299 / 47.899 | 11.061 / 357.787 / 357.788 | 9.153 / 18.001 / 23.000 | 5.488 / 13.374 / 131.640 |
| s4_dedicated_chunked256_c4 | 1.291 / 2.344 / 3.557 | 2.219 / 12.424 / 30.753 | 2.224 / 14.722 / 18.144 | 5.698 / 10.441 / 12.654 | 2.803 / 10.568 / 545.621 |
| s4_dedicated_chunked256_c8 | 1.341 / 2.549 / 4.309 | 2.574 / 13.229 / 22.705 | 2.451 / 12.813 / 13.320 | 6.006 / 11.618 / 14.791 | 2.799 / 9.452 / 48.463 |

兩表只列指定 metrics。sample_count 與完整 36 個 stage identities 見 [matrix_stages.csv](data/ledger_pipeline_sharding/run-1790925219743034498/matrix_stages.csv)；不要將 batch p99 除以 batch size 當成每筆 request 的 p99。

## 測量口徑與限制

每個 reply 在前景原子同步 WriteBatch 成功、memory publish 完成且結果被 client 觀測後返回；client 不會逐筆等待 projection 或 GC。Client RPS 使用全域 start barrier 到最後一個 reply 的時間。50,000 個 coroutine 各最多一筆 outstanding request，queue slots 合計也是 50,000；此閉迴路工作負載主要觀察 buffered backlog 與 pipeline 行為，不能宣稱量到持續超出 queue capacity 時的 enqueue backpressure 性能。

CPU 與 CPU µs/request 是 client window 內整個 process 的 CPU 值，不包含 settlement CPU，也沒有 per-shard 歸因。計入 Tokio、blocking、RocksDB native 與 background threads；4 個 async workers 不是 4 個 CPU quota，host 有 8 個 physical cores／16 個 logical processors。Client window 結束於最後一個 reply，background workers 當時仍在工作。

Settlement 包括 background catch-up 與 drain；background workers 收集後，finalizer 依 shard 順序執行最後 watermark 與 safe GC。finite drain RPS 受此有限 settlement/finalizer 政策影響，不能當成 concurrent GC worker 容量或 steady-state throughput。Background CSV/stage 只納入不晚於 last reply 完成的 event。Settlement 只報總耗時與最終 progress，沒有 post-window 逐段 CPU、IO 或 latency。

VmHWM/RSS 在 client end 附近按 `storage_sample_offset` 讀取；涵蓋 startup 與 seed，為截至讀取時的 process lifetime peak，不包含後續 settlement、recovery 或 artifact-writing peak。DB bytes 則是在 recovery/integrity 驗證及關閉 unique DB 後、刪除 scratch 前量得。

CPU 與 IO 的計時邊界不同。Process/device/RocksDB IO 起點在 seed 和 measurement preflight 後、queue/background setup 與 client task spawn 前；終點在 last reply 後。各 counter offset 保留於 trial metadata；這些 IO delta 與 CPU/barrier window 不完全相同。Target 是 logical mounted partition `sdb2`（8:18），包含主機其他 IO，不是 per-file/per-shard 或全部 physical disk 的精準歸因。Process/device counters 各取一次；RocksDB stats 依 unique DB 只取一次。Native operation durations 可重疊，不應相加為總延遲。

每 shard 固定 P 時，增加 S 也會提高理論全域 query-group cap `S × P`，不能把差異拆成純 shard 效益；global observed peaks 為 NA。RocksDB shared stats 只計一次，dedicated stats 加總唯一 DB；WAL sync 是觀測 counter，RocksDB 可能協調同 DB 的同步寫入，不能假設 sync 數等於 foreground batch 數或各 shard commits 總和。

前置檢查要求 CPU busy ≤10%、目標裝置 busy ≤5%，最多等待 60 秒。正式試次 setup 與 measurement preflight 各觀測 3 秒；smoke child 與 reduced test 觀測 100 ms，並使用縮小資源預留。兩種 DB 配置的總 write-buffer/cache 各 128 MiB、每 DB background-job 設定合計 8；每個 child 共用 RocksDB default Env LOW/HIGH pools 6/2。這些是配置值，不是 RSS、active-thread 或 CPU 硬限制。

`MockDB` successful apply 依 benchmark contract 視為 durable，只用同一個 in-memory instance 驗證 source DB close/reopen；不代表 external DB process-crash 或斷電 durability。S1 是不同時點、RocksDB options 與 key layout 的 unpaired history。兩個 S、兩個 P 與三次 repetition 不足以證明 physical CPU saturation 或 steady-state capacity。

Parent `run.log` 有 capture gap：初始 build warnings 未完整保存，另有一次 poll output 遺失。這是 parent log capture 限制；24 個 child 的 raw metrics、stdout/stderr 與 status 完整保留。詳情見 [parent run status](data/ledger_pipeline_sharding/run-1790925219743034498/parent_run_status.txt) 與 [completion audit](data/ledger_pipeline_sharding/run-1790925219743034498/run_completion_audit.txt)；沒有補造遺失的 parent log。

### Archive 與原始資料

Git 保留完整試次索引：[trial_manifest.csv](data/ledger_pipeline_sharding/run-1790925219743034498/trial_manifest.csv)、[matrix_trials.csv](data/ledger_pipeline_sharding/run-1790925219743034498/matrix_trials.csv)、[matrix_shards.csv](data/ledger_pipeline_sharding/run-1790925219743034498/matrix_shards.csv)、[matrix_stages.csv](data/ledger_pipeline_sharding/run-1790925219743034498/matrix_stages.csv)、[run_manifest.txt](data/ledger_pipeline_sharding/run-1790925219743034498/run_manifest.txt)、[parent_run_status.txt](data/ledger_pipeline_sharding/run-1790925219743034498/parent_run_status.txt)、[run_completion_audit.txt](data/ledger_pipeline_sharding/run-1790925219743034498/run_completion_audit.txt)、[source_hash_audit.txt](data/ledger_pipeline_sharding/run-1790925219743034498/source_hash_audit.txt)、[evidence manifest](data/ledger_pipeline_sharding/run-1790925219743034498/evidence_manifest.csv)。Git 中也有逐 trial summary、shards 與 stages CSV；上方各結果列連到這些 summaries。

Git 保存緊湊摘要與本次稽核資料；完整原始 samples、逐 trial 詳細 logs、RocksDB options、原始 `report.md` 與 `run.log` 位於 [archive index](data/ledger_pipeline_sharding/run-1790925219743034498/evidence_archive.md) 說明的 Git 外本機 gzip archive。Standalone clone 不含 raw samples 或完整 logs，必須另行取得該 archive 才能重做 raw percentile/tail 分析。Archive 尚未上傳，沒有共用下載 URL。Parent log 的初始 build warnings 未完整保存，且有一次 poll output capture gap；原始檔與該缺口均依記錄保留。
