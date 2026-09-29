# Tokio bounded queue batch-pool HashMap 基準測試報告

執行日期：2026-09-29。預設矩陣完整完成 128/128 個情境。每個情境處理 10,000,000 筆請求並執行一輪，總計 12.8 億筆請求。所有情境都通過程式內驗證。

## 執行環境與完整測試

- Ubuntu 24.04.2 LTS、x86_64、Linux 6.17.0-35-generic
- AMD Ryzen 7 3700X，8 個實體核心／16 個邏輯 CPU
- Rust 1.98.1、Cargo 1.98.1、Tokio 1.53.1
- `cargo bench` optimized profile

完整測試前有一筆非正式資源快照。操作者當時觀察到 16 個邏輯 CPU、約 44 GiB 可用記憶體、約 94 GB 可用磁碟，以及 load average 1.75 / 5.82 / 4.97。程序清單中可見編輯器和系統服務，benchmark 尚未啟動。快照時間與原始命令輸出沒有保存；也沒有設定正式的閒置門檻或 preflight gate，因此這些內容不是可稽核的 preflight 紀錄。

完整矩陣實際執行的命令如下。stdout 寫入原始 CSV，stderr 寫入 Cargo 和各情境進度。測試結束後，`_full_` 檔名改成 benchmark 慣用的標準檔名，內容未變更。

```sh
BINDGEN_EXTRA_CLANG_ARGS='-I/usr/lib/gcc/x86_64-linux-gnu/13/include' \
  cargo bench --bench bounded_queue_batch_pool_tokio \
  > benches/bounded_queue_batch_pool_tokio_full_results.csv \
  2> benches/bounded_queue_batch_pool_tokio_full_run.log
```

情境依固定順序執行：C=100000、50000、25000、12500、6250、3125、1562、781；接著依序測試 empty/prefilled map、B=2048/4096、T=1/5/10/20 ms。每個設定執行一次，沒有隨機排序。與既有 Tokio per-request queue benchmark 的比較採用其 2026-09-26 的結果，因此兩次執行日期不同，而且各自都只有一輪。

## 依 coroutine 數量彙整

RPS 單位為百萬請求／秒（M requests/s）。新 benchmark 的 median、範圍、完成延遲和 CPU 數值，均彙整該 C 下 16 個 map／batch／timeout 設定。完成平均延遲使用全部 10,000,000 筆請求；p95 來自抽樣資料。`new/old matched ratio` 是對應相同 map／batch／timeout 設定後，16 個新舊 RPS 比率的中位數；它不等於新舊 RPS 中位數的比值。

| C | 新版 completed RPS median | 新版 completed RPS 範圍 | 舊版 completed RPS median | new/old matched ratio | 完成平均延遲 median (ms) | 完成延遲 p50 / p95 / p99 median (ms) | CPU cores median |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 100000 | 1.226 | 1.095–1.286 | 1.489 | 0.821 | 80.814 | 79.720 / 92.686 / 102.567 | 2.754 |
| 50000 | 1.641 | 1.445–1.717 | 2.121 | 0.776 | 30.278 | 29.156 / 35.381 / 46.166 | 2.639 |
| 25000 | 1.672 | 1.507–1.751 | 2.164 | 0.766 | 14.876 | 14.369 / 19.039 / 24.040 | 2.629 |
| 12500 | 1.729 | 1.613–1.859 | 2.489 | 0.683 | 7.174 | 6.820 / 10.442 / 12.775 | 2.609 |
| 6250 | 2.025 | 1.837–2.146 | 3.179 | 0.638 | 3.049 | 2.931 / 4.253 / 6.111 | 2.450 |
| 3125 | 2.219 | 0.147–2.335 | 2.261 | 0.866 | 1.359 | 1.273 / 2.017 / 3.368 | 2.449 |
| 1562 | 0.196 | 0.074–0.754 | 0.194 | 1.013 | 8.675 | 8.444 / 9.462 / 9.567 | 0.260 |
| 781 | 0.099 | 0.037–0.370 | 0.098 | 1.006 | 8.623 | 8.945 / 9.130 / 9.291 | 0.141 |

C=3125 以上的 matched cases 中，batch-pool benchmark 的 RPS 中位數比率低於 1；C=1562 和 781 時的配對中位數接近 1。這次量測中，移除 per-request dequeue 並未讓大部分 matched 設定取得較高 completed throughput。Batch-pool 路徑同時包含 active-batch mutex 協調、semaphore permit 取得、timeout sealing 和 buffer-pool handoff；這些額外路徑可能帶來成本，但這次比較沒有隔離或證明任何因果關係。兩次 benchmark 在不同日期執行，且各只有一輪。

低 C 時，每個 coroutine 會等待目前請求的回覆後才繼續送下一筆，因此 batch 常在達到 B 前先到 deadline。例如 C=781 時所有設定都由 timeout flush；20 ms 設定的 completed RPS 較低、完成延遲較高。各設定的數據見原始 CSV。

## Buffer pool 與 batch 觀察

128 個情境中有 10 個發生 buffer expansion 和 availability wait。這 10 個情境全是 T=1 ms，C 為 25000、50000 或 100000；其餘 118 個情境都沒有 expansion 或 buffer wait event。

| C | 有 pool 活動的設定 | 每個受影響設定的 pool expansions | Peak buffers | Buffer wait events |
|---:|---|---:|---:|---:|
| 100000 | 兩種 map state、兩種 B | 19–37 | 46–56 | 12,566–36,608 |
| 50000 | 兩種 map state、兩種 B | 9–30 | 36–45 | 4,506–39,057 |
| 25000 | 兩種 map state，B=4096 | 7–9 | 22–24 | 3,896–6,934 |

最多的 buffer wait events 是 39,057，出現在 C=50000、empty map、B=4096、T=1 ms；該設定擴充 28 次，peak 為 43 個 buffers。最多的 expansions 是 37 次，出現在 C=100000、B=4096 的兩種 map state。最高的 `buffer_availability_wait_mean_ns` 約為每筆 438 微秒，出現在 C=50000、prefilled map、B=4096、T=1 ms。這個 mean 以全部 10,000,000 筆請求為分母，包含沒有等待的請求；它不等於實際發生 wait 的請求之平均值，CSV 沒有另列後者。這些數據描述 pool activity 發生的位置，不能證明其成因或對 throughput 的影響。

128 個情境合計有 710,287 個 batches：282,007 個 full batches、428,280 個 timeout batches。成功的 closed-loop 情境都沒有使用明確 shutdown tail flush；tail path 是經程式碼檢視的 cleanup 路徑，未由這次成功 workload 實際觸發。

## 指標定義與限制

Completed RPS 是 N 除以最早 t0 至最晚 t6 的時間窗。Admission RPS 使用最早 t0 至最晚 t_admit。Capacity-acquire wait 與 reserved-but-unadmitted wait 共同切分 t0-to-admit；持有 permit 並等待 Vec 的 request 尚未到達 t_admit。Buffer-pool wait 獨立記錄。CSV 中的 mean 使用全部 N 筆請求；p50/p95/p99 依 deterministic request-ID hash 抽樣。完整矩陣在預設 stride 1,024 下，每個情境的 latency sample count 為 9,892。

五個可加總的逐筆階段是 t0-to-admit、admit-to-seal、seal-to-start、start-to-done、done-to-t6。t_start-to-t_done 只包含 map updates 和 count staging；oneshot response dispatch 計入 t_done-to-t6。每列都通過 request ID 和 per-key count permutation、final map、N completions、有限數值、stage total、capacity 與 permit accounting、batch／flush accounting，以及 buffer recycling 驗證。CSV 有 128 個不重複矩陣 key，每列完成 10,000,000 筆請求，沒有部分列。

舊 benchmark 的 per-request dequeue latency 在新版沒有直接對應階段。舊版 `batch_ns_per_item` 包含 map update 和 `StagedResponse` vector 填入；新版 `handler_ns_per_item` 是原地更新 `Request.mapped_count`，response dispatch 則發生在 t_done 之後。兩項 per-item 指標的工作範圍不同，不應視為相同成本直接比較。

## 驗證與短測試

`cargo fmt --check`、`BINDGEN_EXTRA_CLANG_ARGS='-I/usr/lib/gcc/x86_64-linux-gnu/13/include' cargo check --locked --all-targets`，以及使用相同環境變數的 `cargo test --locked` 均通過；test suite 有 17 個 integration tests 通過。

短測試以 N=20,001、C=8,192、B=2048、T=20 ms 涵蓋兩種 map state（各 9 個 full batches 和 1 個 timeout batch）；以 N=257、C=5、B=2048、T=1 ms 涵蓋兩種 map state（各 52 個 timeout batches）；另測試 N=1、C=1、empty map，輸出數值皆有限。C<B 測試都完成，沒有 deadlock。

完整矩陣前另執行一個代表性單案例：C=6250、empty map、B=2048、T=5 ms，N=10M；完成時間 5.418 秒，completed RPS 為 1.846M。該案例有 4,890 個 batches（4,881 full、9 timeout），沒有 pool expansions 或 buffer waits。這個案例與上方的固定順序矩陣及非正式資源快照分開。

## 檔案

- [完整 128 案例原始 CSV](bounded_queue_batch_pool_tokio_results.csv)
- [完整執行進度與 Cargo log](bounded_queue_batch_pool_tokio_run.log)
- [Benchmark 設計文件](../docs/01-16.development-design-bounded-queue-batch-pool-tokio-benchmark.md)
