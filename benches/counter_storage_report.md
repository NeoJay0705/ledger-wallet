# Stack、Heap 與 HashMap 計數效能基準測試報告

## 目標與測試組別

比較三種儲存方式在單執行緒反覆計數時的效能：

- **Stack**：`u64` 計數值。
- **Heap**：以 `Box<u64>` 儲存的計數值。
- **HashMap**：單一固定 key 的 `HashMap<u64, u64>`，每次透過 `get_mut` 取得並更新值。

## 測試方法

以 release 模式執行 benchmark。每組每輪執行 10,000,000 次，共 3 輪，使用單一執行緒。`Box<u64>` 與 HashMap 的初始化不計入計時。Stack 與 Box 組每輪透過 `read_volatile` 讀值、再以 `write_volatile` 寫回；HashMap 組使用 `black_box`，降低查找與更新被最佳化掉的可能。每輪結束後驗證最終值為 10,000,000。

每項指標記錄三輪實測值的中位數；下表只列中位數，不列逐輪原始數據。三組每輪的最終值皆為 10,000,000，且驗證通過。

### 指標

- **RPS** = iterations / wall_seconds
- **平均 latency** = wall_ns / iterations
- **One-core CPU%** = 100 × ProcessTime / wall

輸出也包含 normalized CPU%；本報告列出的是 one-core CPU%。

## 測試環境與執行方式

- Ubuntu 24.04.2 LTS，x86_64
- AMD Ryzen 7 3700X 8-Core Processor（8 核、16 執行緒）
- `rustc` / `cargo` 1.98.1
- 執行命令：`cargo bench --bench counter_storage`

## 結果

| 組別 | RPS（ops/s） | 平均 latency（ns/op） | One-core CPU% |
| --- | ---: | ---: | ---: |
| Stack (`u64`) | 3,895,749,737.04 | 0.257 | 100.03% |
| Heap (`Box<u64>`) | 3,924,609,815.29 | 0.255 | 100.03% |
| HashMap（單一固定 key） | 71,114,364.29 | 14.062 | 99.99% |

## 解讀限制

- 此處的 RPS 是每次計數加一操作的每秒次數（ops/s），不是網路 request 數。
- 平均 latency 是由 throughput 換算的平均 ns/op，不是獨立測得的 memory load latency，也不是 p50 或 p99 延遲。
- Stack 與 Box 的值很可能留在 cache；兩者數值接近，不能據此主張差異具統計顯著性，也不能推論一般 DRAM 存取速度的差異。
- HashMap 組的每次操作包含雜湊查找，因此結果反映查找與更新的整體成本。
- CPU% 是程序 CPU time 除以 wall time；約 100% 表示約用滿一個核心。100.03% 可能來自計時雜訊。

## 驗證紀錄

已執行 `cargo fmt --check`、`cargo check --benches --locked`、短輪次實測與預設實測。
