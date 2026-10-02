# Tokio queue 與 ledger pipeline 執行緒擴展結果

Run ID：1790834093174466926 · **完整 full matrix** · 2026-10-01 · 36 個子程序 trial、共 360,000,000 requests。

情境為 queue_echo、foreground_persistence、integrated_pipeline，各比較 Tokio runtime worker 3、4、6、8。每 trial 固定 50,000 users/coroutines、每個 coroutine 200 requests、共 10,000,000 requests、每個 coroutine 同時最多一筆 in-flight request、容量 50,000 bounded queue、foreground batch 2,048、first-dequeue timeout 5 ms。Full run 的逐 request latency 以 deterministic hash sampling 約 1/64 取樣（stride 64；分位數採 nearest-rank），依 hash 條件選取，不是依 ID 順序固定抽每第 64 個 request。

固定設定：ledger storage profiles 使用 single-shard 與 `PerBatch` balance mode；量測交易為 Credit/Debit 各 50%、amount 1、history 0%。`integrated_pipeline` 的 projection 與 GC batch 均為 256，retention 為 500 ms，watermark checks 每 100 ms；GC 有進度時 yield 並繼續，blocked 或無進度時 sleep 100 ms。各角色共用設定的 Tokio worker pool，沒有 per-role pinned thread。Projection destination 是 benchmark 的 durable mock destination：apply 成功依 benchmark contract 視為 durable，close/reopen check 沿用同一個 in-memory destination；這不代表外部 DB 或 process-crash durability，因此這裡量到的 destination cost 不應泛化為實際 external DB latency。

原始證據：[summary CSV, 36 trials](ledger_thread_scaling_summary.csv) · [stage percentiles, 672 rows](ledger_thread_scaling_stages.csv) · [background summary, 36 rows](ledger_thread_scaling_background_summary.csv) · [trial manifest](trial_manifest.csv) · [run metadata](run_metadata.txt) · [設計文件](../../../../docs/01-26.development-design-ledger-thread-scaling-tokio-benchmark.md) · [原始產生報告](ledger_thread_scaling_report.md)

## 12 個 runtime 設定：throughput 與 CPU

RPS 是完成 requests 除以該情境所記錄的 request wall interval。CPU cores 是 process CPU seconds 除以對應 CPU wall；CPU µs/request 是每筆完成請求的 CPU 成本。此表的 median 是三輪描述統計；同時列出每輪 RPS 與 min–max。

| Scenario | Tokio workers | RPS median [min–max] | RPS rep 1 / 2 / 3 | CPU cores median | CPU µs/request median |
| --- | --- | --- | --- | --- | --- |
| queue_echo | 3 | 2695292.706 [2601478.042–2700354.064] | 2601478.042 / 2700354.064 / 2695292.706 | 2.661 | 0.985469 |
| queue_echo | 4 | 3005474.298 [2858120.606–3027091.413] | 3005474.298 / 2858120.606 / 3027091.413 | 3.239 | 1.077621 |
| queue_echo | 6 | 2827001.015 [2777494.880–2930234.032] | 2777494.880 / 2827001.015 / 2930234.032 | 4.437 | 1.593064 |
| queue_echo | 8 | 2713550.581 [2644224.357–2739649.365] | 2644224.357 / 2739649.365 / 2713550.581 | 5.072 | 1.910606 |
| foreground_persistence | 3 | 42314.210 [40215.625–42716.961] | 42314.210 / 40215.625 / 42716.961 | 1.153 | 27.294008 |
| foreground_persistence | 4 | 42194.714 [41885.300–42646.548] | 41885.300 / 42646.548 / 42194.714 | 1.175 | 27.604976 |
| foreground_persistence | 6 | 41457.480 [38790.943–41763.357] | 38790.943 / 41457.480 / 41763.357 | 1.252 | 30.397909 |
| foreground_persistence | 8 | 41485.161 [40654.659–42028.490] | 40654.659 / 41485.161 / 42028.490 | 1.332 | 32.117864 |
| integrated_pipeline | 3 | 36864.652 [33232.778–37020.535] | 36864.652 / 33232.778 / 37020.535 | 1.252 | 33.820184 |
| integrated_pipeline | 4 | 36895.045 [36893.326–37016.302] | 37016.302 / 36893.326 / 36895.045 | 1.272 | 34.448012 |
| integrated_pipeline | 6 | 36880.325 [36737.183–36972.846] | 36880.325 / 36737.183 / 36972.846 | 1.351 | 36.766537 |
| integrated_pipeline | 8 | 36625.235 [36138.720–36867.891] | 36138.720 / 36625.235 / 36867.891 | 1.420 | 38.964150 |

## Request latency：保留每輪分位數

每格為 repetition 1 / 2 / 3，單位毫秒。這些是各 trial 實測 nearest-rank 分位數，沒有把分位數跨輪平均或合併成 pooled percentile。樣本數見 summary CSV。

| Scenario | Workers | p50 ms, rep 1 / 2 / 3 | p95 ms, rep 1 / 2 / 3 | p99 ms, rep 1 / 2 / 3 |
| --- | --- | --- | --- | --- |
| queue_echo | 3 | 18.288 / 18.037 / 18.110 | 24.002 / 20.679 / 20.550 | 25.508 / 23.132 / 22.302 |
| queue_echo | 4 | 15.850 / 16.566 / 15.713 | 19.951 / 21.439 / 19.868 | 21.354 / 22.700 / 20.775 |
| queue_echo | 6 | 17.758 / 17.273 / 17.160 | 20.242 / 19.400 / 18.736 | 21.434 / 20.449 / 19.911 |
| queue_echo | 8 | 18.868 / 17.932 / 18.324 | 21.244 / 20.827 / 20.459 | 22.663 / 22.620 / 21.264 |
| foreground_persistence | 3 | 1173.623 / 1184.837 / 1163.357 | 1777.669 / 1818.252 / 1723.369 | 2061.058 / 4039.872 / 2141.337 |
| foreground_persistence | 4 | 1178.753 / 1157.739 / 1165.313 | 1777.848 / 1752.832 / 1772.846 | 2120.311 / 2130.229 / 2098.390 |
| foreground_persistence | 6 | 1197.881 / 1191.515 / 1164.156 | 1866.774 / 1799.049 / 1788.855 | 4565.030 / 2043.405 / 2104.517 |
| foreground_persistence | 8 | 1201.544 / 1167.678 / 1166.798 | 1813.758 / 1800.054 / 1771.721 | 2208.493 / 2152.723 / 2114.517 |
| integrated_pipeline | 3 | 1333.134 / 1314.343 / 1325.952 | 2095.206 / 2105.486 / 2068.038 | 2434.177 / 11747.531 / 2411.307 |
| integrated_pipeline | 4 | 1322.460 / 1333.585 / 1326.752 | 2060.255 / 2084.994 / 2075.882 | 2503.882 / 2483.038 / 2479.852 |
| integrated_pipeline | 6 | 1333.219 / 1337.250 / 1329.839 | 2070.140 / 2079.468 / 2065.287 | 2485.035 / 2452.847 / 2506.809 |
| integrated_pipeline | 8 | 1359.083 / 1341.153 / 1322.778 | 2111.704 / 2099.938 / 2063.327 | 2474.608 / 2471.331 / 2476.742 |

## 結果解讀

**Queue echo。** 4 workers 的 median RPS 為 3005474.298，比 3 workers 的 2695292.706 高 11.5%；median CPU cost 則由 0.985469 升至 1.077621 µs/request（增加 9.4%）。6、8 workers 的 median RPS 分別為 2827001.015、2713550.581，都低於 4 workers，CPU 使用較高。此 instrumented queue workload 以 4 workers 得到最高觀察 throughput，3 workers 有最低 per-request CPU 成本。

**Storage profiles。** Foreground persistence 在 3 / 4 / 6 / 8 workers 的 median RPS 為 42314.210, 42194.714, 41457.480, 41485.161；integrated pipeline 為 36864.652, 36895.045, 36880.325, 36625.235。Integrated 的 3、4 workers 接近，6、8 沒有帶來 throughput 增益。Integrated CPU cost 隨 workers 增加為 33.820184, 34.448012, 36.766537, 38.964150 µs/request。4 workers 是 shared pool 的合理第一候選：有 queue-only throughput 增益，而它三輪 integrated p99 落在 2.480–2.504 s；3 workers 的 CPU 成本最低。Foreground writes 和 GC 共用 AccountStore serialized batch gate；foreground batch 為 2,048 records，projection/GC batch 為 256 records，增加 Tokio workers 沒有移除這個序列化點。這組結果沒有證明普遍最佳值；每個設定只有三輪描述性觀察，也不能證明某個 thread count 導致或避免 I/O outlier。

**保留的 outlier。** Trial 24（integrated_pipeline、3 workers、rep 2）的 request.total p99 為 11.747531 s、request.queue p99 為 11.408620 s、request.handler p99 為 488.226 ms、GC.sync_write p99 為 228.454 ms、GC.total p99 為 490.577 ms。其 target-device busy 為 89.556 s，其他 integrated trial 約 53–54 s；RocksDB stall_us 為 0。結果與 synchronous-I/O slowdown 加上 serialized batch gate/queue 的放大相符，但沒有 time-aligned syscall、per-file I/O 或 event trace，無法確定根因或歸因於 worker count。參見 [trial 24 storage summary](trials/trial-24-integrated_pipeline_t3_r2/ledger_pipeline_summary.csv) 與 [trial 24 stage summary](trials/trial-24-integrated_pipeline_t3_r2/ledger_pipeline_stages.csv)。Foreground outlier 也全數保留：trial 23（3 workers、rep 2）p99 4.039872 s，trial 8（6 workers、rep 1）p99 4.565030 s。

## 4-worker request stage 參考

涵蓋三個 4-worker repetition 的所有有樣本 request stages。Admission、enqueue 用 µs 避免小值被四捨五入成 0；其他 stages 用 ms。Request.handler 是每個 request 觀察到的整批處理時間，不是純 worker service time。Full stage CSV 保留所有 worker 設定；sample count 為 0 表示該 stage 未執行，參考表省略零樣本列但 raw CSV 保留。

| Scenario | Rep | Stage | Samples | p50 | p95 | p99 |
| --- | --- | --- | --- | --- | --- | --- |
| queue_echo | 1 | request.total (ms) | 155,809 | 15.850 | 19.951 | 21.354 |
| queue_echo | 1 | request.enqueue (µs) | 155,809 | 0.060 | 0.140 | 0.190 |
| queue_echo | 1 | request.queue (ms) | 155,809 | 15.492 | 19.466 | 20.864 |
| queue_echo | 1 | request.batch (ms) | 155,809 | 0.104 | 0.264 | 0.359 |
| queue_echo | 1 | request.handler (ms) | 155,809 | 0.001 | 0.001 | 0.001 |
| queue_echo | 1 | request.response (ms) | 155,809 | 0.260 | 0.506 | 0.657 |
| foreground_persistence | 1 | request.total (ms) | 155,808 | 1178.753 | 1777.848 | 2120.311 |
| foreground_persistence | 1 | request.admission (µs) | 155,808 | 0.481 | 1.683 | 5.500 |
| foreground_persistence | 1 | request.enqueue (µs) | 155,808 | 0.060 | 0.150 | 0.250 |
| foreground_persistence | 1 | request.queue (ms) | 155,808 | 1131.555 | 1706.413 | 2037.126 |
| foreground_persistence | 1 | request.batch (ms) | 155,808 | 0.135 | 1.027 | 1.545 |
| foreground_persistence | 1 | request.handler (ms) | 155,808 | 46.171 | 71.628 | 84.691 |
| foreground_persistence | 1 | request.response (ms) | 155,808 | 0.712 | 1.671 | 2.030 |
| integrated_pipeline | 1 | request.total (ms) | 155,808 | 1322.460 | 2060.255 | 2503.882 |
| integrated_pipeline | 1 | request.admission (µs) | 155,808 | 0.471 | 1.703 | 5.180 |
| integrated_pipeline | 1 | request.enqueue (µs) | 155,808 | 0.070 | 0.151 | 0.240 |
| integrated_pipeline | 1 | request.queue (ms) | 155,808 | 1269.020 | 1978.509 | 2408.530 |
| integrated_pipeline | 1 | request.batch (ms) | 155,808 | 0.141 | 1.214 | 1.766 |
| integrated_pipeline | 1 | request.handler (ms) | 155,808 | 51.858 | 82.481 | 100.525 |
| integrated_pipeline | 1 | request.response (ms) | 155,808 | 0.816 | 1.883 | 2.284 |
| queue_echo | 2 | request.total (ms) | 155,809 | 16.566 | 21.439 | 22.700 |
| queue_echo | 2 | request.enqueue (µs) | 155,809 | 0.060 | 0.150 | 0.230 |
| queue_echo | 2 | request.queue (ms) | 155,809 | 16.159 | 20.942 | 22.106 |
| queue_echo | 2 | request.batch (ms) | 155,809 | 0.107 | 0.277 | 0.398 |
| queue_echo | 2 | request.handler (ms) | 155,809 | 0.001 | 0.001 | 0.001 |
| queue_echo | 2 | request.response (ms) | 155,809 | 0.272 | 0.585 | 0.711 |
| foreground_persistence | 2 | request.total (ms) | 155,808 | 1157.739 | 1752.832 | 2130.229 |
| foreground_persistence | 2 | request.admission (µs) | 155,808 | 0.461 | 1.453 | 3.777 |
| foreground_persistence | 2 | request.enqueue (µs) | 155,808 | 0.060 | 0.141 | 0.231 |
| foreground_persistence | 2 | request.queue (ms) | 155,808 | 1110.437 | 1682.891 | 2042.238 |
| foreground_persistence | 2 | request.batch (ms) | 155,808 | 0.134 | 0.775 | 1.434 |
| foreground_persistence | 2 | request.handler (ms) | 155,808 | 45.518 | 70.896 | 84.586 |
| foreground_persistence | 2 | request.response (ms) | 155,808 | 0.670 | 1.433 | 1.915 |
| integrated_pipeline | 2 | request.total (ms) | 155,808 | 1333.585 | 2084.994 | 2483.038 |
| integrated_pipeline | 2 | request.admission (µs) | 155,808 | 0.481 | 1.744 | 5.500 |
| integrated_pipeline | 2 | request.enqueue (µs) | 155,808 | 0.070 | 0.160 | 0.240 |
| integrated_pipeline | 2 | request.queue (ms) | 155,808 | 1279.680 | 2000.076 | 2392.665 |
| integrated_pipeline | 2 | request.batch (ms) | 155,808 | 0.142 | 1.277 | 1.804 |
| integrated_pipeline | 2 | request.handler (ms) | 155,808 | 52.196 | 82.827 | 100.751 |
| integrated_pipeline | 2 | request.response (ms) | 155,808 | 0.822 | 1.933 | 2.314 |
| queue_echo | 3 | request.total (ms) | 155,809 | 15.713 | 19.868 | 20.775 |
| queue_echo | 3 | request.enqueue (µs) | 155,809 | 0.060 | 0.140 | 0.190 |
| queue_echo | 3 | request.queue (ms) | 155,809 | 15.344 | 19.433 | 20.308 |
| queue_echo | 3 | request.batch (ms) | 155,809 | 0.104 | 0.259 | 0.355 |
| queue_echo | 3 | request.handler (ms) | 155,809 | 0.001 | 0.001 | 0.001 |
| queue_echo | 3 | request.response (ms) | 155,809 | 0.257 | 0.498 | 0.648 |
| foreground_persistence | 3 | request.total (ms) | 155,808 | 1165.313 | 1772.846 | 2098.390 |
| foreground_persistence | 3 | request.admission (µs) | 155,808 | 0.441 | 1.362 | 3.206 |
| foreground_persistence | 3 | request.enqueue (µs) | 155,808 | 0.060 | 0.141 | 0.231 |
| foreground_persistence | 3 | request.queue (ms) | 155,808 | 1117.654 | 1703.740 | 2021.816 |
| foreground_persistence | 3 | request.batch (ms) | 155,808 | 0.133 | 0.734 | 1.371 |
| foreground_persistence | 3 | request.handler (ms) | 155,808 | 45.915 | 72.064 | 88.670 |
| foreground_persistence | 3 | request.response (ms) | 155,808 | 0.660 | 1.378 | 1.855 |
| integrated_pipeline | 3 | request.total (ms) | 155,808 | 1326.752 | 2075.882 | 2479.852 |
| integrated_pipeline | 3 | request.admission (µs) | 155,808 | 0.481 | 1.734 | 5.410 |
| integrated_pipeline | 3 | request.enqueue (µs) | 155,808 | 0.070 | 0.151 | 0.240 |
| integrated_pipeline | 3 | request.queue (ms) | 155,808 | 1273.024 | 1993.640 | 2373.289 |
| integrated_pipeline | 3 | request.batch (ms) | 155,808 | 0.142 | 1.265 | 1.790 |
| integrated_pipeline | 3 | request.handler (ms) | 155,808 | 51.984 | 83.071 | 102.110 |
| integrated_pipeline | 3 | request.response (ms) | 155,808 | 0.817 | 1.905 | 2.280 |

## 4-worker integrated background stage 參考

Projector、GC stage 是 client end 前完成的 per-batch background events。Projection.read 包含 ordered range read 與 decode。每筆 eligible Credit/Debit 在 GC.scan 會做兩次 point get（ledger 與 transaction-index），並 decode、做 equality/safety validation；metadata proof reads 和 destination verification 在 scan timer 之外、GC.total 之內。GC.total 也包含 batch-gate wait、delete dispatch 等工作。GC.sync_write 是同步 WAL batch write 時間，不是 fsync-only。

| Rep | Stage | Samples | p50 ms | p95 ms | p99 ms |
| --- | --- | --- | --- | --- | --- |
| 1 | projection.read (ms) | 39,056 | 0.289 | 0.502 | 0.625 |
| 1 | projection.apply (ms) | 39,056 | 0.142 | 0.232 | 0.394 |
| 1 | projection.progress_sync (ms) | 39,056 | 0.961 | 2.135 | 3.154 |
| 1 | projection.total (ms) | 39,056 | 1.450 | 2.721 | 4.140 |
| 1 | gc.scan (ms) | 4,900 | 5.897 | 10.150 | 12.436 |
| 1 | gc.delete_build (ms) | 4,900 | 0.052 | 0.090 | 0.177 |
| 1 | gc.sync_write (ms) | 4,900 | 1.719 | 2.408 | 3.003 |
| 1 | gc.total (ms) | 4,900 | 53.143 | 83.772 | 102.276 |
| 1 | watermark.fence_wait (ms) | 325 | 767.355 | 1557.875 | 1797.948 |
| 1 | watermark.projection_wait (ms) | 325 | 12.178 | 14.839 | 722.597 |
| 1 | watermark.persist (ms) | 325 | 0.952 | 1.065 | 2.606 |
| 1 | watermark.total (ms) | 325 | 792.080 | 1571.283 | 1812.724 |
| 2 | projection.read (ms) | 39,056 | 0.293 | 0.501 | 0.623 |
| 2 | projection.apply (ms) | 39,056 | 0.142 | 0.236 | 0.406 |
| 2 | projection.progress_sync (ms) | 39,056 | 0.960 | 2.131 | 3.164 |
| 2 | projection.total (ms) | 39,056 | 1.454 | 2.720 | 4.302 |
| 2 | gc.scan (ms) | 4,886 | 5.919 | 10.174 | 12.437 |
| 2 | gc.delete_build (ms) | 4,886 | 0.050 | 0.102 | 0.185 |
| 2 | gc.sync_write (ms) | 4,886 | 1.718 | 2.421 | 3.322 |
| 2 | gc.total (ms) | 4,886 | 53.716 | 84.177 | 102.898 |
| 2 | watermark.fence_wait (ms) | 324 | 767.482 | 1564.849 | 1742.250 |
| 2 | watermark.projection_wait (ms) | 324 | 12.169 | 14.213 | 644.717 |
| 2 | watermark.persist (ms) | 324 | 0.955 | 1.066 | 2.544 |
| 2 | watermark.total (ms) | 324 | 789.412 | 1577.913 | 1754.154 |
| 3 | projection.read (ms) | 39,056 | 0.288 | 0.509 | 0.638 |
| 3 | projection.apply (ms) | 39,056 | 0.142 | 0.237 | 0.394 |
| 3 | projection.progress_sync (ms) | 39,056 | 0.957 | 2.155 | 3.728 |
| 3 | projection.total (ms) | 39,056 | 1.444 | 2.751 | 4.386 |
| 3 | gc.scan (ms) | 4,886 | 5.875 | 10.089 | 12.339 |
| 3 | gc.delete_build (ms) | 4,886 | 0.052 | 0.085 | 0.178 |
| 3 | gc.sync_write (ms) | 4,886 | 1.727 | 2.458 | 3.755 |
| 3 | gc.total (ms) | 4,886 | 53.380 | 84.368 | 103.313 |
| 3 | watermark.fence_wait (ms) | 324 | 768.687 | 1559.130 | 1779.466 |
| 3 | watermark.projection_wait (ms) | 324 | 12.094 | 16.481 | 677.240 |
| 3 | watermark.persist (ms) | 324 | 0.955 | 1.064 | 2.126 |
| 3 | watermark.total (ms) | 324 | 788.599 | 1572.918 | 1798.640 |

## All-worker background progress 與 settlement

表中 24 個有 storage 的 trial 都連到原始 per-trial summary；queue_echo 沒有 storage background stage。Foreground 的 projection/GC 在 measurement 與 settlement 均關閉，相關 worker work 與 watermark updates 為 0。Disabled prefix 的 backlog counter 表示序號差距，不代表有背景 worker 正在跑。

| Trial summary | Scenario | Workers | Rep | Projection/GC | Projection batches | Projection records during clients | Projection backlog at client end | GC steps | GC scanned during clients | GC deleted during clients | GC backlog at client end | GC prefix after settle | Watermark updates |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| [trial 02](trials/trial-02-foreground_persistence_t3_r1/ledger_pipeline_summary.csv) | foreground_persistence | 3 | 1 | false/false | 0 | 0 | 10,000,000 | 0 | 0 | 0 | 10,150,000 | 0 | 0 |
| [trial 03](trials/trial-03-integrated_pipeline_t3_r1/ledger_pipeline_summary.csv) | integrated_pipeline | 3 | 1 | true/true | 39,056 | 9,998,336 | 1,664 | 4,901 | 1,254,656 | 1,254,656 | 8,895,344 | 10,150,000 | 322 |
| [trial 05](trials/trial-05-foreground_persistence_t4_r1/ledger_pipeline_summary.csv) | foreground_persistence | 4 | 1 | false/false | 0 | 0 | 10,000,000 | 0 | 0 | 0 | 10,150,000 | 0 | 0 |
| [trial 06](trials/trial-06-integrated_pipeline_t4_r1/ledger_pipeline_summary.csv) | integrated_pipeline | 4 | 1 | true/true | 39,056 | 9,998,336 | 1,664 | 4,900 | 1,254,400 | 1,254,400 | 8,895,600 | 10,150,000 | 325 |
| [trial 08](trials/trial-08-foreground_persistence_t6_r1/ledger_pipeline_summary.csv) | foreground_persistence | 6 | 1 | false/false | 0 | 0 | 10,000,000 | 0 | 0 | 0 | 10,150,000 | 0 | 0 |
| [trial 09](trials/trial-09-integrated_pipeline_t6_r1/ledger_pipeline_summary.csv) | integrated_pipeline | 6 | 1 | true/true | 39,056 | 9,998,336 | 1,664 | 4,913 | 1,257,728 | 1,257,728 | 8,892,272 | 10,150,000 | 323 |
| [trial 11](trials/trial-11-foreground_persistence_t8_r1/ledger_pipeline_summary.csv) | foreground_persistence | 8 | 1 | false/false | 0 | 0 | 10,000,000 | 0 | 0 | 0 | 10,150,000 | 0 | 0 |
| [trial 12](trials/trial-12-integrated_pipeline_t8_r1/ledger_pipeline_summary.csv) | integrated_pipeline | 8 | 1 | true/true | 39,056 | 9,998,336 | 1,664 | 4,935 | 1,263,360 | 1,263,360 | 8,886,640 | 10,150,000 | 320 |
| [trial 14](trials/trial-14-foreground_persistence_t4_r2/ledger_pipeline_summary.csv) | foreground_persistence | 4 | 2 | false/false | 0 | 0 | 10,000,000 | 0 | 0 | 0 | 10,150,000 | 0 | 0 |
| [trial 15](trials/trial-15-integrated_pipeline_t4_r2/ledger_pipeline_summary.csv) | integrated_pipeline | 4 | 2 | true/true | 39,056 | 9,998,336 | 1,408 | 4,886 | 1,250,816 | 1,250,816 | 8,899,184 | 10,150,000 | 324 |
| [trial 17](trials/trial-17-foreground_persistence_t6_r2/ledger_pipeline_summary.csv) | foreground_persistence | 6 | 2 | false/false | 0 | 0 | 10,000,000 | 0 | 0 | 0 | 10,150,000 | 0 | 0 |
| [trial 18](trials/trial-18-integrated_pipeline_t6_r2/ledger_pipeline_summary.csv) | integrated_pipeline | 6 | 2 | true/true | 39,056 | 9,998,336 | 1,664 | 4,908 | 1,256,448 | 1,256,448 | 8,893,552 | 10,150,000 | 324 |
| [trial 20](trials/trial-20-foreground_persistence_t8_r2/ledger_pipeline_summary.csv) | foreground_persistence | 8 | 2 | false/false | 0 | 0 | 10,000,000 | 0 | 0 | 0 | 10,150,000 | 0 | 0 |
| [trial 21](trials/trial-21-integrated_pipeline_t8_r2/ledger_pipeline_summary.csv) | integrated_pipeline | 8 | 2 | true/true | 39,056 | 9,998,336 | 1,664 | 4,899 | 1,254,144 | 1,254,144 | 8,895,856 | 10,150,000 | 322 |
| [trial 23](trials/trial-23-foreground_persistence_t3_r2/ledger_pipeline_summary.csv) | foreground_persistence | 3 | 2 | false/false | 0 | 0 | 10,000,000 | 0 | 0 | 0 | 10,150,000 | 0 | 0 |
| [trial 24](trials/trial-24-integrated_pipeline_t3_r2/ledger_pipeline_summary.csv) | integrated_pipeline | 3 | 2 | true/true | 39,056 | 9,998,336 | 1,664 | 4,887 | 1,251,072 | 1,251,072 | 8,898,928 | 10,150,000 | 322 |
| [trial 26](trials/trial-26-foreground_persistence_t6_r3/ledger_pipeline_summary.csv) | foreground_persistence | 6 | 3 | false/false | 0 | 0 | 10,000,000 | 0 | 0 | 0 | 10,150,000 | 0 | 0 |
| [trial 27](trials/trial-27-integrated_pipeline_t6_r3/ledger_pipeline_summary.csv) | integrated_pipeline | 6 | 3 | true/true | 39,057 | 9,998,592 | 1,408 | 4,922 | 1,260,032 | 1,260,032 | 8,889,968 | 10,150,000 | 325 |
| [trial 29](trials/trial-29-foreground_persistence_t8_r3/ledger_pipeline_summary.csv) | foreground_persistence | 8 | 3 | false/false | 0 | 0 | 10,000,000 | 0 | 0 | 0 | 10,150,000 | 0 | 0 |
| [trial 30](trials/trial-30-integrated_pipeline_t8_r3/ledger_pipeline_summary.csv) | integrated_pipeline | 8 | 3 | true/true | 39,056 | 9,998,336 | 1,664 | 4,937 | 1,263,872 | 1,263,872 | 8,886,128 | 10,150,000 | 323 |
| [trial 32](trials/trial-32-foreground_persistence_t3_r3/ledger_pipeline_summary.csv) | foreground_persistence | 3 | 3 | false/false | 0 | 0 | 10,000,000 | 0 | 0 | 0 | 10,150,000 | 0 | 0 |
| [trial 33](trials/trial-33-integrated_pipeline_t3_r3/ledger_pipeline_summary.csv) | integrated_pipeline | 3 | 3 | true/true | 39,056 | 9,998,336 | 1,664 | 4,891 | 1,252,096 | 1,252,096 | 8,897,904 | 10,150,000 | 322 |
| [trial 35](trials/trial-35-foreground_persistence_t4_r3/ledger_pipeline_summary.csv) | foreground_persistence | 4 | 3 | false/false | 0 | 0 | 10,000,000 | 0 | 0 | 0 | 10,150,000 | 0 | 0 |
| [trial 36](trials/trial-36-integrated_pipeline_t4_r3/ledger_pipeline_summary.csv) | integrated_pipeline | 4 | 3 | true/true | 39,056 | 9,998,336 | 1,664 | 4,886 | 1,250,816 | 1,250,816 | 8,899,184 | 10,150,000 | 324 |

Integrated client-end projection backlog 為 1,408–1,664 records，GC backlog 為 8,886,128–8,899,184；client 結束後 settlement 為 259.374–308.073 s。Projector progress excludes 150,000 seed records。GC 起始 prefix 為 0，所以 client period deletion 可包含 seed；GC backlog 是 latest sequence 減 durable prefix，不是 10M 減 deleted count。

| Trial summary | Scenario | Workers | Rep | Client wall s | Settlement s | Recovery s | Integrity s |
| --- | --- | --- | --- | --- | --- | --- | --- |
| [trial 02](trials/trial-02-foreground_persistence_t3_r1/ledger_pipeline_summary.csv) | foreground_persistence | 3 | 1 | 236.327 | 0.000781 | 0.460711 | 60.226796 |
| [trial 03](trials/trial-03-integrated_pipeline_t3_r1/ledger_pipeline_summary.csv) | integrated_pipeline | 3 | 1 | 271.263 | 308.072759 | 1.496117 | 0.318096 |
| [trial 05](trials/trial-05-foreground_persistence_t4_r1/ledger_pipeline_summary.csv) | foreground_persistence | 4 | 1 | 238.747 | 0.000869 | 0.470540 | 60.308294 |
| [trial 06](trials/trial-06-integrated_pipeline_t4_r1/ledger_pipeline_summary.csv) | integrated_pipeline | 4 | 1 | 270.151 | 303.867200 | 3.834360 | 0.343877 |
| [trial 08](trials/trial-08-foreground_persistence_t6_r1/ledger_pipeline_summary.csv) | foreground_persistence | 6 | 1 | 257.792 | 0.002320 | 0.412122 | 66.447507 |
| [trial 09](trials/trial-09-integrated_pipeline_t6_r1/ledger_pipeline_summary.csv) | integrated_pipeline | 6 | 1 | 271.147 | 262.382398 | 3.117511 | 0.354879 |
| [trial 11](trials/trial-11-foreground_persistence_t8_r1/ledger_pipeline_summary.csv) | foreground_persistence | 8 | 1 | 245.974 | 0.000825 | 0.396525 | 62.263757 |
| [trial 12](trials/trial-12-integrated_pipeline_t8_r1/ledger_pipeline_summary.csv) | integrated_pipeline | 8 | 1 | 276.712 | 297.649805 | 1.574323 | 0.352104 |
| [trial 14](trials/trial-14-foreground_persistence_t4_r2/ledger_pipeline_summary.csv) | foreground_persistence | 4 | 2 | 234.486 | 0.002903 | 0.456162 | 58.895762 |
| [trial 15](trials/trial-15-integrated_pipeline_t4_r2/ledger_pipeline_summary.csv) | integrated_pipeline | 4 | 2 | 271.052 | 305.822900 | 1.580598 | 0.341649 |
| [trial 17](trials/trial-17-foreground_persistence_t6_r2/ledger_pipeline_summary.csv) | foreground_persistence | 6 | 2 | 241.211 | 0.001166 | 0.389557 | 59.483465 |
| [trial 18](trials/trial-18-integrated_pipeline_t6_r2/ledger_pipeline_summary.csv) | integrated_pipeline | 6 | 2 | 272.204 | 301.611837 | 1.614029 | 0.351867 |
| [trial 20](trials/trial-20-foreground_persistence_t8_r2/ledger_pipeline_summary.csv) | foreground_persistence | 8 | 2 | 241.050 | 0.000937 | 0.423358 | 60.758531 |
| [trial 21](trials/trial-21-integrated_pipeline_t8_r2/ledger_pipeline_summary.csv) | integrated_pipeline | 8 | 2 | 273.036 | 302.675110 | 3.464334 | 0.344981 |
| [trial 23](trials/trial-23-foreground_persistence_t3_r2/ledger_pipeline_summary.csv) | foreground_persistence | 3 | 2 | 248.660 | 0.000833 | 0.405736 | 59.739981 |
| [trial 24](trials/trial-24-integrated_pipeline_t3_r2/ledger_pipeline_summary.csv) | integrated_pipeline | 3 | 2 | 300.908 | 261.808194 | 3.219104 | 0.343096 |
| [trial 26](trials/trial-26-foreground_persistence_t6_r3/ledger_pipeline_summary.csv) | foreground_persistence | 6 | 3 | 239.444 | 0.000862 | 0.433214 | 59.712510 |
| [trial 27](trials/trial-27-integrated_pipeline_t6_r3/ledger_pipeline_summary.csv) | integrated_pipeline | 6 | 3 | 270.469 | 293.780542 | 1.591530 | 0.348793 |
| [trial 29](trials/trial-29-foreground_persistence_t8_r3/ledger_pipeline_summary.csv) | foreground_persistence | 8 | 3 | 237.934 | 0.000980 | 0.422216 | 58.827859 |
| [trial 30](trials/trial-30-integrated_pipeline_t8_r3/ledger_pipeline_summary.csv) | integrated_pipeline | 8 | 3 | 271.239 | 297.080714 | 1.644574 | 0.341708 |
| [trial 32](trials/trial-32-foreground_persistence_t3_r3/ledger_pipeline_summary.csv) | foreground_persistence | 3 | 3 | 234.099 | 0.002927 | 0.493477 | 59.681975 |
| [trial 33](trials/trial-33-integrated_pipeline_t3_r3/ledger_pipeline_summary.csv) | integrated_pipeline | 3 | 3 | 270.120 | 292.389198 | 3.278907 | 0.344200 |
| [trial 35](trials/trial-35-foreground_persistence_t4_r3/ledger_pipeline_summary.csv) | foreground_persistence | 4 | 3 | 236.997 | 0.000791 | 0.410246 | 59.131999 |
| [trial 36](trials/trial-36-integrated_pipeline_t4_r3/ledger_pipeline_summary.csv) | integrated_pipeline | 4 | 3 | 271.039 | 259.374206 | 3.427292 | 0.342226 |

## Process/device I/O 與 RocksDB counters

第一張表是每設定三輪的 descriptive median。Process rchar/wchar、read/write bytes 與 target-device counters 沿用既有 adapter scope；MiB 使用 1,048,576 bytes。Process/device snapshot endpoint 在不同情境有差異，個別試次資料見連結 summary。

| Scenario | Workers | Process rchar MiB | Process wchar MiB | Process read MiB | Process write MiB | Device read MiB | Device write MiB | Device busy s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| queue_echo | 3 | 0.011 | 0.000 | 0.000 | 0.000 | 0.000 | 0.785 | 0.006 |
| queue_echo | 4 | 0.011 | 0.000 | 0.000 | 0.000 | 0.000 | 0.070 | 0.005 |
| queue_echo | 6 | 0.011 | 0.000 | 0.000 | 0.000 | 0.000 | 0.191 | 0.071 |
| queue_echo | 8 | 0.011 | 0.000 | 0.000 | 0.000 | 0.000 | 0.422 | 0.016 |
| foreground_persistence | 3 | 45459.600 | 3631.297 | 0.000 | 3650.637 | 0.016 | 3799.824 | 13.720 |
| foreground_persistence | 4 | 45552.929 | 3626.160 | 0.000 | 3645.559 | 0.000 | 3777.203 | 13.776 |
| foreground_persistence | 6 | 45625.890 | 3640.267 | 0.000 | 3659.699 | 0.074 | 3794.258 | 16.524 |
| foreground_persistence | 8 | 45595.316 | 3643.268 | 0.000 | 3662.738 | 0.000 | 3801.895 | 14.317 |
| integrated_pipeline | 3 | 49064.603 | 3870.923 | 0.000 | 4062.996 | 0.062 | 4767.695 | 53.900 |
| integrated_pipeline | 4 | 49031.817 | 3865.906 | 0.000 | 4058.145 | 0.000 | 4723.094 | 54.021 |
| integrated_pipeline | 6 | 49044.126 | 3873.019 | 0.000 | 4065.445 | 0.000 | 4734.535 | 53.507 |
| integrated_pipeline | 8 | 49150.363 | 3875.551 | 0.000 | 4068.129 | 0.000 | 4740.141 | 53.841 |

RocksDB counters 是 storage settings 內三輪的 descriptive median。WAL sync、flush/compaction 與 stalls 保留現有快照語意；I/O 無 per-file attribution。

| Scenario | Workers | WAL syncs | WAL MiB | Writes with WAL | Flush write MiB | Compaction read MiB | Compaction write MiB | Stall ms |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| foreground_persistence | 3 | 4883 | 1545.115 | 4883 | 602.062 | 1388.274 | 1483.314 | 0.000 |
| foreground_persistence | 4 | 4883 | 1545.115 | 4883 | 600.746 | 1383.780 | 1479.562 | 0.000 |
| foreground_persistence | 6 | 4883 | 1545.115 | 4883 | 604.862 | 1391.832 | 1489.589 | 0.000 |
| foreground_persistence | 8 | 4883 | 1545.115 | 4883 | 605.525 | 1394.438 | 1491.798 | 0.000 |
| integrated_pipeline | 3 | 49144 | 1592.115 | 49154 | 622.708 | 1608.528 | 1654.799 | 0.000 |
| integrated_pipeline | 4 | 49148 | 1592.060 | 49151 | 621.491 | 1604.344 | 1651.183 | 0.000 |
| integrated_pipeline | 6 | 49167 | 1592.311 | 49175 | 623.890 | 1610.024 | 1655.793 | 0.000 |
| integrated_pipeline | 8 | 49186 | 1592.516 | 49195 | 624.764 | 1610.851 | 1657.115 | 0.000 |

Per-trial endpoint offsets 保留在連結的 queue/pipeline source summary。CPU offset 以最後 reply 後 µs 表示（queue 原值為 ns，storage 原值為 µs）；pipeline 另有 progress、process/device I/O 及 RocksDB snapshot offsets。Queue I/O 在 worker join 後取樣，會包含其 offset 內的 tail work。

| Trial summary | Scenario | Workers | Rep | CPU endpoint offset µs | Progress offset µs | Process/device I/O offset µs | RocksDB snapshot offset µs |
| --- | --- | --- | --- | --- | --- | --- | --- |
| [trial 01](trials/trial-01-queue_echo_t3_r1/queue_echo_summary.csv) | queue_echo | 3 | 1 | 0.671 | — | 9448 | — |
| [trial 02](trials/trial-02-foreground_persistence_t3_r1/ledger_pipeline_summary.csv) | foreground_persistence | 3 | 1 | 0.000 | 3 | 11 | 551 |
| [trial 03](trials/trial-03-integrated_pipeline_t3_r1/ledger_pipeline_summary.csv) | integrated_pipeline | 3 | 1 | 0.000 | 4 | 2173 | 2835 |
| [trial 04](trials/trial-04-queue_echo_t4_r1/queue_echo_summary.csv) | queue_echo | 4 | 1 | 0.891 | — | 11583 | — |
| [trial 05](trials/trial-05-foreground_persistence_t4_r1/ledger_pipeline_summary.csv) | foreground_persistence | 4 | 1 | 1.000 | 14 | 23 | 624 |
| [trial 06](trials/trial-06-integrated_pipeline_t4_r1/ledger_pipeline_summary.csv) | integrated_pipeline | 4 | 1 | 0.000 | 2 | 12 | 573 |
| [trial 07](trials/trial-07-queue_echo_t6_r1/queue_echo_summary.csv) | queue_echo | 6 | 1 | 1.212 | — | 20848 | — |
| [trial 08](trials/trial-08-foreground_persistence_t6_r1/ledger_pipeline_summary.csv) | foreground_persistence | 6 | 1 | 0.000 | 6 | 1563 | 2076 |
| [trial 09](trials/trial-09-integrated_pipeline_t6_r1/ledger_pipeline_summary.csv) | integrated_pipeline | 6 | 1 | 1.000 | 7 | 133 | 666 |
| [trial 10](trials/trial-10-queue_echo_t8_r1/queue_echo_summary.csv) | queue_echo | 8 | 1 | 1.103 | — | 19823 | — |
| [trial 11](trials/trial-11-foreground_persistence_t8_r1/ledger_pipeline_summary.csv) | foreground_persistence | 8 | 1 | 0.000 | 578 | 587 | 1119 |
| [trial 12](trials/trial-12-integrated_pipeline_t8_r1/ledger_pipeline_summary.csv) | integrated_pipeline | 8 | 1 | 1.000 | 8 | 1695 | 2254 |
| [trial 13](trials/trial-13-queue_echo_t4_r2/queue_echo_summary.csv) | queue_echo | 4 | 2 | 1.062 | — | 14421 | — |
| [trial 14](trials/trial-14-foreground_persistence_t4_r2/ledger_pipeline_summary.csv) | foreground_persistence | 4 | 2 | 0.000 | 18 | 2095 | 2646 |
| [trial 15](trials/trial-15-integrated_pipeline_t4_r2/ledger_pipeline_summary.csv) | integrated_pipeline | 4 | 2 | 0.000 | 15 | 2225 | 2833 |
| [trial 16](trials/trial-16-queue_echo_t6_r2/queue_echo_summary.csv) | queue_echo | 6 | 2 | 0.862 | — | 15694 | — |
| [trial 17](trials/trial-17-foreground_persistence_t6_r2/ledger_pipeline_summary.csv) | foreground_persistence | 6 | 2 | 1.000 | 65 | 73 | 802 |
| [trial 18](trials/trial-18-integrated_pipeline_t6_r2/ledger_pipeline_summary.csv) | integrated_pipeline | 6 | 2 | 0.000 | 6 | 1607 | 2177 |
| [trial 19](trials/trial-19-queue_echo_t8_r2/queue_echo_summary.csv) | queue_echo | 8 | 2 | 0.902 | — | 22454 | — |
| [trial 20](trials/trial-20-foreground_persistence_t8_r2/ledger_pipeline_summary.csv) | foreground_persistence | 8 | 2 | 1.000 | 679 | 688 | 1265 |
| [trial 21](trials/trial-21-integrated_pipeline_t8_r2/ledger_pipeline_summary.csv) | integrated_pipeline | 8 | 2 | 1.000 | 188 | 197 | 961 |
| [trial 22](trials/trial-22-queue_echo_t3_r2/queue_echo_summary.csv) | queue_echo | 3 | 2 | 0.651 | — | 13489 | — |
| [trial 23](trials/trial-23-foreground_persistence_t3_r2/ledger_pipeline_summary.csv) | foreground_persistence | 3 | 2 | 0.000 | 564 | 575 | 1167 |
| [trial 24](trials/trial-24-integrated_pipeline_t3_r2/ledger_pipeline_summary.csv) | integrated_pipeline | 3 | 2 | 0.000 | 7 | 16 | 648 |
| [trial 25](trials/trial-25-queue_echo_t6_r3/queue_echo_summary.csv) | queue_echo | 6 | 3 | 0.772 | — | 21772 | — |
| [trial 26](trials/trial-26-foreground_persistence_t6_r3/ledger_pipeline_summary.csv) | foreground_persistence | 6 | 3 | 0.000 | 11 | 20 | 616 |
| [trial 27](trials/trial-27-integrated_pipeline_t6_r3/ledger_pipeline_summary.csv) | integrated_pipeline | 6 | 3 | 1.000 | 10 | 19 | 621 |
| [trial 28](trials/trial-28-queue_echo_t8_r3/queue_echo_summary.csv) | queue_echo | 8 | 3 | 0.752 | — | 16467 | — |
| [trial 29](trials/trial-29-foreground_persistence_t8_r3/ledger_pipeline_summary.csv) | foreground_persistence | 8 | 3 | 1.000 | 471 | 482 | 1109 |
| [trial 30](trials/trial-30-integrated_pipeline_t8_r3/ledger_pipeline_summary.csv) | integrated_pipeline | 8 | 3 | 1.000 | 95 | 106 | 667 |
| [trial 31](trials/trial-31-queue_echo_t3_r3/queue_echo_summary.csv) | queue_echo | 3 | 3 | 0.822 | — | 10956 | — |
| [trial 32](trials/trial-32-foreground_persistence_t3_r3/ledger_pipeline_summary.csv) | foreground_persistence | 3 | 3 | 0.000 | 10 | 2132 | 2686 |
| [trial 33](trials/trial-33-integrated_pipeline_t3_r3/ledger_pipeline_summary.csv) | integrated_pipeline | 3 | 3 | 1.000 | 4 | 2889 | 3438 |
| [trial 34](trials/trial-34-queue_echo_t4_r3/queue_echo_summary.csv) | queue_echo | 4 | 3 | 0.682 | — | 18993 | — |
| [trial 35](trials/trial-35-foreground_persistence_t4_r3/ledger_pipeline_summary.csv) | foreground_persistence | 4 | 3 | 0.000 | 11 | 20 | 560 |
| [trial 36](trials/trial-36-integrated_pipeline_t4_r3/ledger_pipeline_summary.csv) | integrated_pipeline | 4 | 3 | 0.000 | 190 | 2412 | 3044 |

## 驗證、host 與限制

36 個 fresh child processes 全數成功，每 trial 恰完成 10,000,000 replies。24 個 storage trials 均恰為 5,000,000 credits + 5,000,000 debits、amount 1，餘額、request sequence、durable progress constraints、close/reopen recovery 和 integrity 驗證通過。Foreground 各輪 projection/GC flags 為 false，projector/GC work 及 watermark updates 為 0；source sequence 為 10,150,000，而 projected/destination sequence 保持 seed 的 150,000。Foreground 沒有執行 final projection catch-up。Full archive 沒有保留 trial DB/scratch directory 或 per-event background CSV。

60 個 strict full-run resource observations 全部通過；觀察到的 CPU busy 最大 2.9061%、target-device busy 最大 4.966%，低於 10%/5% gate。所有子程序 inherited CPU affinity 0-15。Host 是 AMD Ryzen 7 3700X、8 physical cores / 16 logical CPUs、SMT enabled，device sdb2 (8:18)。Rust compiler：rustc 1.98.1 (48a229cea 2026-09-01)；Cargo.lock versions：Tokio 1.53.1、rocksdb 0.25.0、librocksdb-sys 0.19.0+11.8.1。Runtime worker_threads 只控制 Tokio async workers；blocking pool 和 RocksDB internal background/compaction threads 沿用 defaults。

每個 trial 都是新 process/runtime/owned DB。Setup、seed preparation、seed-history projection、preflight、settlement、recovery 與 integrity 不計入 client RPS。Queue RPS 從最早 request start 到最後 reply；storage RPS 從 client release 前的 measurement start 到最後 route reply，兩者 wall interval 邊界略不同。Process CPU 使用對應的 release/start 至 latest-reply endpoint：queue CPU 從 client release 起算，雖然 queue RPS 從最早 individual request start 起算；storage CPU 與 RPS 都是 release-to-reply interval。Queue 在每個 coroutine final reply 後取 process CPU sample並記錄 offset，storage 保留原 benchmark sample offset。約 50,000 個 final-reply CPU clock samples 是各 trial 共同 instrumentation overhead；結果不代表未 instrumented queue capacity。I/O 與 RocksDB counter 保留各自 sample endpoint offsets；counter scope 內可包含 latest reply 後的 tail work。

Projection.read 包含 ordered range read/decode。每筆 eligible Credit/Debit 的 GC.scan 會做兩次 point get（ledger 與 transaction-index）並 decode、驗證 equality/safety；GC.total 另含 batch gate wait、metadata proof、destination verification/dispatch，GC.sync_write 是 sync WAL batch-write timing 而非 fsync-only。Foreground writes 和 GC 共用 batch gate；foreground batch 2,048、projection/GC batch 256。Request.handler 是 per-request whole-batch observation，projection/GC 是 client end 前完成的 per-batch samples；不能以 percentiles 相減估 exact gate wait。無 time-aligned syscall/event trace 或 per-file attribution。

此前已通過 cargo check --locked --benches、36-case short smoke、原 request-batch CLI smoke 與六-case ledger-pipeline smoke。Full run 使用預設 3,000 ms preflight observation。Percentiles 使用 deterministic stride-64 sample 的 nearest-rank。三輪數據採描述性解讀，無 significance claim，也不單獨作 thread-count causal proof。

## 重現

Full 36-trial command:

    BINDGEN_EXTRA_CLANG_ARGS='-I/usr/lib/gcc/x86_64-linux-gnu/13/include' cargo bench --locked --bench ledger_thread_scaling_tokio

Reviewed short smoke 使用每 trial 200 users/coroutines × 200 requests 與 100 ms resource observations:

    BINDGEN_EXTRA_CLANG_ARGS='-I/usr/lib/gcc/x86_64-linux-gnu/13/include' cargo bench --locked --bench ledger_thread_scaling_tokio -- --smoke --output-root target/ledger-thread-scaling-smoke
