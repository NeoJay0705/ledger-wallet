# Tokio Request Ring Buffer HashMap 基準測試報告

執行日期：2026-09-29。完整預設矩陣 128/128 個情境成功完成。每列處理 10,000,000 個請求，共 1,280,000,000 個 case-request。每個情境執行一輪。

## 執行環境與資源 preflight

- Ubuntu 24.04.2 LTS、Linux 6.17.0-35-generic、x86_64
- AMD Ryzen 7 3700X、8 個實體核心與 16 個邏輯 CPU
- Rust 1.98.1、Cargo 1.98.1、Tokio 1.53.1；Cargo bench optimized profile
- 完整執行時間：2026-09-29 06:05:21–07:10:48 UTC，65 分 27 秒；wrapper exit code 0。

完整 benchmark 前的資源與閒置觀察記錄於 2026-09-29 06:04:04 UTC，preflight 結果為 PASS。1 秒 CPU sample 為 1.814%；16 個邏輯 CPU；load average（1/5/15 分鐘）為 0.485 / 4.964 / 5.607；可用記憶體 47,820,947,456 bytes；可用磁碟 100,492,402,688 bytes。當時採用 CPU <=10%、可用記憶體 >=4 GiB、可用磁碟 >=4 GiB 作為執行門檻。逐項程序摘要及原始 preflight 數值見 [run log](bounded_queue_request_ring_tokio_run.log)。

完整矩陣命令：

```sh
BINDGEN_EXTRA_CLANG_ARGS='-I/usr/lib/gcc/x86_64-linux-gnu/13/include' \
  cargo bench --bench bounded_queue_request_ring_tokio
```

run log 記錄命令、scenario 順序、每列開始／完成狀態、退出碼、git HEAD `26371c9def93ad1cea4aca50265aefa0b8440010`，以及 2026-09-29 06:05:01 UTC 的 benchmark source SHA-256。SHA-256 清單包含 Cargo.toml、README、benchmark、ring support module、測試與設計文件。

## Workload 與矩陣

固定 50,000 個 ring Request slots、一個 FIFO batch worker，三個 Tokio runtime worker threads。每個 coroutine 同時最多一筆未完成請求；收到 oneshot response 的 ID/count 後才送下一筆。Workload 使用 10,000,000 個連續 ID、100,000 個 rotating keys，map 狀態為 empty 或 prefilled。C=[100000, 50000, 25000, 12500, 6250, 3125, 1562, 781]、B=[2048, 4096]、T=[1, 5, 10, 20] ms；T 從各 active batch 第一筆 admitted request 開始計算。達 B 時 seal；timer 可 seal partial batch。成功的全 N closed-loop workload 不因 ID 全部 submitted 而提前 flush。

## 完整矩陣驗證

- CSV 有 128 列、128 個不重複矩陣 key；key 集合精確符合 8 個 C × 2 種 map × 2 個 B × 4 個 T，每個 repetition=1。
- 每列 completed、response verifier count 與 final map verifier 都通過；每列處理 10,000,000 個 ID，final map 長度為 100,000。
- 每列 5 個 stage 的 count 都是 10,000,000，五段 total 精確加總為 completion total。
- 預設 deterministic request-ID sample stride=1,024；每列 completion、ring-full wait 與五個 stage 都有 9,892 個樣本。平均值使用全部 N 筆請求；p50/p95/p99 使用此樣本。
- 所有列 ring capacity=50,000、peak occupancy 未超過容量、wrap validation 通過。713,680 個 batches 都執行 wrap check；24,161 個 batch 跨過實體陣列末端。
- 128 列沒有 partial-success row；matrix、request/response/map、stage accounting、sample count、capacity、flush/batch 與 wrap 檢查都通過。

全矩陣共 713,680 個 batches：279,983 個 full、433,697 個 timeout、0 個 shutdown tail。`ring_full_wait_events` 在每個 case 中對曾至少一次遇到 ring 滿載的 request 加一次；128 列合計 110,381,028 個 request indicators，單列不超過 N。此欄不是等待迴圈次數。

## 依 coroutine 數量彙整

RPS 單位為百萬請求／秒。每列 median、範圍、延遲與 CPU 值先依相同 C 的 16 個 map/B/T 設定彙整。Ring/Pool matched ratio 是 16 個相同矩陣 key 的逐列 RPS 比率中位數；它不等於兩組 RPS 中位數相除。Pool 數據取自既有 batch-pool CSV。兩個 benchmark 都記錄於 2026-09-29、分別於不同時間執行、各一輪；兩者 backpressure 機制不同。

| C | Ring completed RPS median（範圍） | Pool completed RPS median | Ring/Pool matched RPS ratio | Ring completion mean (ms) | Ring completion p50 / p95 / p99 (ms) | Pool completion p50 / p95 / p99 (ms) | Ring / pool CPU cores median |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 100000 | 0.998 (0.944–1.062) | 1.226 | 0.831 | 92.634 | 49.501 / 236.505 / 389.708 | 79.720 / 92.686 / 102.567 | 2.60 / 2.75 |
| 50000 | 1.533 (1.341–1.583) | 1.641 | 0.932 | 32.463 | 31.733 / 38.261 / 46.003 | 29.156 / 35.381 / 46.166 | 2.51 / 2.64 |
| 25000 | 1.771 (1.651–1.884) | 1.672 | 1.085 | 14.077 | 13.699 / 17.628 / 21.395 | 14.369 / 19.039 / 24.040 | 2.46 / 2.63 |
| 12500 | 1.879 (1.804–1.976) | 1.729 | 1.083 | 6.596 | 6.479 / 8.823 / 11.260 | 6.820 / 10.442 / 12.775 | 2.39 / 2.61 |
| 6250 | 2.188 (2.013–2.455) | 2.025 | 1.098 | 2.845 | 2.763 / 3.713 / 5.260 | 2.931 / 4.253 / 6.111 | 2.32 / 2.45 |
| 3125 | 2.127 (0.147–2.442) | 2.219 | 1.002 | 1.432 | 1.364 / 1.892 / 3.036 | 1.273 / 2.017 / 3.368 | 2.28 / 2.45 |
| 1562 | 0.197 (0.074–0.774) | 0.196 | 1.002 | 8.659 | 8.419 / 9.434 / 9.513 | 8.444 / 9.462 / 9.567 | 0.25 / 0.26 |
| 781 | 0.099 (0.037–0.369) | 0.099 | 0.999 | 8.634 | 8.946 / 9.105 / 9.280 | 8.945 / 9.130 / 9.291 | 0.13 / 0.14 |

Ring 在 C=100000 的 completion p95/p99 中位數為 236.505 / 389.708 ms；配對 pool 為 92.686 / 102.567 ms。該 C 的 stage0（t0-to-admit）p95/p99 中位數為 186.318 / 343.494 ms，ring-full wait p95/p99 中位數為 184.981 / 342.349 ms。每列有 3,216,074–3,669,134 個 request indicators 顯示曾遇到 ring 滿載；最高 occupancy 為 50,000。這些量測在同一組 C 設定中同時出現，報告不以此單獨歸因 completion tail 的來源。

## 與 per-request Tokio bounded queue 比較

依 `(repetition, configured_coroutines, map_state, batch_size, batch_timeout_ms)` 配對兩份 CSV，共 128 個完全相同的設定 key；每個 C 各有 16 組 map／batch／timeout 設定。兩份資料每列都完成 10,000,000 個 request，並使用 deterministic request-ID sample stride=1,024、每列 9,892 個 completion latency samples。表中 RPS、completion latency 與 CPU 核心等效數是各 C 的 16 個設定中位數；Ring/queue RPS ratio 則是 16 個逐 key ratio 的中位數，不是兩個 RPS 中位數的比值。

| C | Ring 完成 RPS 中位數 (M/s) | Queue 完成 RPS 中位數 (M/s) | 16 組配對 Ring/queue RPS ratio 中位數 | Ring completion p50 / p95 / p99 (ms) | Queue completion p50 / p95 / p99 (ms) | Ring CPU 核心等效數中位數 | Queue CPU 核心等效數中位數 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 100000 | 0.998 | 1.489 | 0.673 | 49.501 / 236.505 / 389.708 | 65.733 / 73.036 / 80.388 | 2.602 | 2.719 |
| 50000 | 1.533 | 2.121 | 0.720 | 31.733 / 38.261 / 46.003 | 23.031 / 25.895 / 28.306 | 2.511 | 2.379 |
| 25000 | 1.771 | 2.164 | 0.805 | 13.699 / 17.628 / 21.395 | 11.196 / 13.425 / 14.665 | 2.458 | 2.356 |
| 12500 | 1.879 | 2.489 | 0.750 | 6.479 / 8.823 / 11.260 | 4.826 / 5.926 / 7.046 | 2.391 | 2.321 |
| 6250 | 2.188 | 3.179 | 0.687 | 2.763 / 3.713 / 5.260 | 1.889 / 2.559 / 3.366 | 2.318 | 2.258 |
| 3125 | 2.127 | 2.261 | 0.869 | 1.364 / 1.892 / 3.036 | 1.474 / 2.214 / 2.461 | 2.282 | 1.528 |
| 1562 | 0.197 | 0.194 | 1.015 | 8.419 / 9.434 / 9.513 | 9.104 / 9.311 / 9.506 | 0.245 | 0.161 |
| 781 | 0.099 | 0.098 | 1.005 | 8.946 / 9.105 / 9.280 | 8.871 / 8.998 / 9.100 | 0.134 | 0.086 |

配對 RPS ratio 在 C=100000 至 C=3125 均小於 1；C=1562 與 C=781 時則接近 1。C=100000 時，Ring completion p95/p99 中位數為 236.505 / 389.708 ms，queue 為 73.036 / 80.388 ms；Ring 的 stage0 與 ring-full wait 百分位數見下節「Admission 與 ring occupancy」。兩種 benchmark 只有端到端 completed RPS、completion latency 與 CPU 核心等效數可以對齊比較。Queue 的 per-request dequeue/t2 stage（`t2-t0`）在 Ring 中沒有相同對應 stage，因此不直接比較 stage latency。

Queue run 於 2026-09-26 記錄，Ring run 於 2026-09-29 記錄。兩者都依固定情境順序執行，每個情境各一輪。這些分別執行的結果僅提供描述性比較，不能據此推論因果關係。

## Admission 與 ring occupancy

`stage0` 是 t0-to-admit。`ring_full_wait` 記錄每筆 request 在 ring full 時等待的累計時間；未遇到滿載的 request 以零值納入 mean 與 sample。它是 stage0 內的子指標，不能再與 stage0 相加。`ring_full_wait_events` 每筆 request 最多計一次，表示該 request 在 admission 過程中至少遇到一次滿載。下表的 request indicator 數列出每 C 的 16 個 case 中位數與範圍；wrapped batches 是 16 列加總。

| C | stage0 mean median (ms) | stage0 p50 / p95 / p99 median (ms) | ring-full wait mean median (ms) | ring-full wait p50 / p95 / p99 median (ms) | 每列遇到滿載的 request indicators：median（範圍） | peak occupancy | wrapped batches 加總 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 100000 | 44.935 | 0.979 / 186.318 / 343.494 | 44.269 | 0.000 / 184.981 / 342.349 | 3,587,166 (3,216,074–3,669,134) | 50,000 | 3,099 |
| 50000 | 0.954 | 0.955 / 1.951 / 2.399 | 0.350 | 0.000 / 1.455 / 1.743 | 3,350,909 (3,180,258–3,540,320) | 50,000 | 3,178 |
| 25000 | 0.594 | 0.736 / 1.077 / 1.754 | 0.000 | 0.000 / 0.000 / 0.000 | 0 (0–0) | 28,577 | 3,178 |
| 12500 | 0.619 | 0.697 / 0.991 / 1.680 | 0.000 | 0.000 / 0.000 / 0.000 | 0 (0–0) | 15,356 | 3,177 |
| 6250 | 0.556 | 0.565 / 1.003 / 1.327 | 0.000 | 0.000 / 0.000 / 0.000 | 0 (0–0) | 10,338 | 3,177 |
| 3125 | 0.259 | 0.263 / 0.511 / 0.788 | 0.000 | 0.000 / 0.000 / 0.000 | 0 (0–0) | 6,165 | 1,984 |
| 1562 | 0.170 | 0.182 / 0.318 / 0.424 | 0.000 | 0.000 / 0.000 / 0.000 | 0 (0–0) | 3,123 | 3,184 |
| 781 | 0.100 | 0.106 / 0.197 / 0.257 | 0.000 | 0.000 / 0.000 / 0.000 | 0 (0–0) | 1,561 | 3,184 |

## 五段 latency 依 C 彙整

下表每個百分位值是各 C 下 16 個 map/B/T case 的 per-case statistic 中位數。Mean 的每列來源為全部 10,000,000 筆 request；p50/p95/p99 來自 deterministic sampled IDs。

| C | Stage | Mean median (ms) | p50 / p95 / p99 median (ms) |
|---:|---|---:|---:|
| 100000 | `t0_to_admit` | 44.935 | 0.979 / 186.318 / 343.494 |
| 100000 | `admit_to_seal` | 0.934 | 0.736 / 2.202 / 2.947 |
| 100000 | `seal_to_start` | 44.554 | 45.040 / 52.144 / 65.144 |
| 100000 | `start_to_done_map_update` | 0.156 | 0.148 / 0.209 / 0.256 |
| 100000 | `done_to_t6_response_delivery` | 0.575 | 0.591 / 0.991 / 1.284 |
| 50000 | `t0_to_admit` | 0.954 | 0.955 / 1.951 / 2.399 |
| 50000 | `admit_to_seal` | 0.634 | 0.569 / 1.587 / 1.932 |
| 50000 | `seal_to_start` | 30.411 | 29.724 / 35.227 / 43.286 |
| 50000 | `start_to_done_map_update` | 0.143 | 0.139 / 0.192 / 0.243 |
| 50000 | `done_to_t6_response_delivery` | 0.536 | 0.520 / 0.889 / 1.286 |
| 25000 | `t0_to_admit` | 0.594 | 0.736 / 1.077 / 1.754 |
| 25000 | `admit_to_seal` | 0.589 | 0.568 / 1.489 / 1.758 |
| 25000 | `seal_to_start` | 11.791 | 11.556 / 13.876 / 18.492 |
| 25000 | `start_to_done_map_update` | 0.156 | 0.154 / 0.232 / 0.280 |
| 25000 | `done_to_t6_response_delivery` | 0.508 | 0.473 / 0.922 / 1.822 |
| 12500 | `t0_to_admit` | 0.619 | 0.697 / 0.991 / 1.680 |
| 12500 | `admit_to_seal` | 0.535 | 0.540 / 1.333 / 1.632 |
| 12500 | `seal_to_start` | 4.762 | 4.585 / 5.772 / 8.269 |
| 12500 | `start_to_done_map_update` | 0.144 | 0.143 / 0.205 / 0.254 |
| 12500 | `done_to_t6_response_delivery` | 0.446 | 0.420 / 0.815 / 1.169 |
| 6250 | `t0_to_admit` | 0.556 | 0.565 / 1.003 / 1.327 |
| 6250 | `admit_to_seal` | 0.496 | 0.466 / 1.051 / 1.319 |
| 6250 | `seal_to_start` | 1.381 | 1.294 / 2.029 / 2.910 |
| 6250 | `start_to_done_map_update` | 0.103 | 0.099 / 0.141 / 0.196 |
| 6250 | `done_to_t6_response_delivery` | 0.349 | 0.335 / 0.653 / 0.995 |
| 3125 | `t0_to_admit` | 0.259 | 0.263 / 0.511 / 0.788 |
| 3125 | `admit_to_seal` | 0.508 | 0.500 / 0.951 / 1.308 |
| 3125 | `seal_to_start` | 0.251 | 0.263 / 0.365 / 0.510 |
| 3125 | `start_to_done_map_update` | 0.067 | 0.061 / 0.106 / 0.150 |
| 3125 | `done_to_t6_response_delivery` | 0.322 | 0.316 / 0.569 / 0.910 |
| 1562 | `t0_to_admit` | 0.170 | 0.182 / 0.318 / 0.424 |
| 1562 | `admit_to_seal` | 8.146 | 7.999 / 9.092 / 9.247 |
| 1562 | `seal_to_start` | 0.003 | 0.002 / 0.004 / 0.007 |
| 1562 | `start_to_done_map_update` | 0.091 | 0.085 / 0.138 / 0.197 |
| 1562 | `done_to_t6_response_delivery` | 0.247 | 0.238 / 0.450 / 0.673 |
| 781 | `t0_to_admit` | 0.100 | 0.106 / 0.197 / 0.257 |
| 781 | `admit_to_seal` | 8.337 | 8.603 / 8.902 / 8.983 |
| 781 | `seal_to_start` | 0.002 | 0.002 / 0.004 / 0.007 |
| 781 | `start_to_done_map_update` | 0.055 | 0.051 / 0.081 / 0.117 |
| 781 | `done_to_t6_response_delivery` | 0.137 | 0.133 / 0.240 / 0.394 |

## Timeout flush 與批次觀察

所有 C<B 的 case 都以 timeout seal partial batches，沒有 full flush 或 shutdown tail。低於 B 的矩陣切片如下：

| C | B | case 數 | full batches | timeout batches | tail batches |
|---:|---:|---:|---:|---:|---:|
| 3125 | 4096 | 8 | 0 | 28,155 | 0 |
| 1562 | 2048 | 8 | 0 | 51,696 | 0 |
| 1562 | 4096 | 8 | 0 | 51,540 | 0 |
| 781 | 2048 | 8 | 0 | 102,442 | 0 |
| 781 | 4096 | 8 | 0 | 102,442 | 0 |

每個 coroutine 等待目前 response 後才送下一個 ID；當 C<B 時，active batch 由第一筆 admission 起算 deadline。所有設定都照該 timeout 規則結束，沒有因 N 已全部 submitted 而提前 flush。全矩陣 tail flush 次數為 0；explicit shutdown tail path 未在成功 closed-loop run 中觸發。

## 指標定義、對照與限制

`t0` 在 admission wait 前；`t_admit` 在 Request slot publish 後；`t_seal`、`t_start`、`t_done` 分別記錄 batch seal、worker start、map update/count staging 完成；`t6` 在 client 收到 oneshot response 後。五段為 t0-to-admit、admit-to-seal、seal-to-start、start-to-done-map-update、done-to-t6-response-delivery。`t_done` 在 response send attempts 之前；dispatch/dropped receiver 的處理時間落在最後一段。

Completed RPS 使用 N 除以最早 t0 至最晚 t6 的時間窗；admission RPS 使用最早 t0 至最晚 t_admit。Ring backpressure 由 tail-head occupancy 與 bounded Notify wait 控制；既有 buffer-pool benchmark 使用 semaphore permits 與 buffer availability wait。不同 backpressure 機制、分開時間執行及各一輪的結果提供描述性對照，沒有隔離單一機制的實驗設計，因此不以數據宣稱因果關係。

## 驗證命令與檔案

`cargo fmt --all -- --check`、`BINDGEN_EXTRA_CLANG_ARGS='-I/usr/lib/gcc/x86_64-linux-gnu/13/include' cargo check --locked --all-targets`、相同環境變數的 `cargo test --locked` 均通過；test suite 共 24 個測試，包含 7 個 request-ring correctness tests。Release smoke 包含 N>50,000 的 wrap 測試、N>100,000 的 capacity/full-wait 測試，以及 C=1、N=1 timeout partial batch；兩種 map state 的 verifier、stage sums 與 ring metrics 都通過。Transient smoke CSV/log 已移除。

- 原始結果：[128-case CSV](bounded_queue_request_ring_tokio_results.csv)
- 完整命令、進度、preflight 與 source hash：[run log](bounded_queue_request_ring_tokio_run.log)
- 設計文件：[Tokio request ring HashMap benchmark](../docs/01-17.development-design-tokio-request-ring-hashmap-benchmark.md)
- per-request queue 對照原始結果：[128-case CSV](bounded_queue_hashmap_tokio_results.csv)
- per-request queue 對照報告：[Tokio bounded queue HashMap benchmark](bounded_queue_hashmap_tokio_report.md)
- 對照原始結果：[batch-pool CSV](bounded_queue_batch_pool_tokio_results.csv)
