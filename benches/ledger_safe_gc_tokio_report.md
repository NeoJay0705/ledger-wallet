# Tokio Ledger 安全 GC 基準測試報告

## 執行

正式基準測試在 Linux 上完成全部四個預設 case，並通過設定的嚴格閒置與資源預檢。每個 case 使用 50,000 個使用者、50,000 個 Tokio coroutine、10,000,000 個 request：9,000,000 筆新 commit、500,000 次歷史命中與 500,000 次歷史未命中，另有 150,000 筆 seed transaction。GC 開關比較使用完全相同的命中／未命中分布與 seed key。

已封存的原始資料：

- [摘要 CSV](data/ledger_safe_gc/safe_gc_summary.csv)
- [延遲 CSV](data/ledger_safe_gc/safe_gc_latency.csv)
- [背景事件 CSV](data/ledger_safe_gc/safe_gc_background.csv)
- [執行紀錄](data/ledger_safe_gc/safe_gc_run.log)

原始輸出來源路徑：`target/ledger-safe-gc-tokio-trials/ledger-safe-gc-1790770252490052716/`。CSV 列寬檢查通過：摘要 CSV 有 4 列資料、每列 54 欄；延遲 CSV 有 88 列資料、每列 7 欄；背景事件 CSV 有 149,424 列資料、每列 16 欄。

執行命令：

```sh
BINDGEN_EXTRA_CLANG_ARGS='-I/usr/lib/gcc/x86_64-linux-gnu/13/include' \
  cargo bench --bench ledger_safe_gc_tokio
```

整體 setup 預檢第一次即通過：CPU busy 2.40%、disk busy 0.00%、可用記憶體 45,526,339,584 bytes，`sdb2` 可用空間 93,825,175,552 bytes。四組 setup 與測量預檢也全部通過。建立封存前，benchmark 已逐一驗證每組 case 的 request 總數、歷史命中／未命中結果、餘額、投影進度、重啟 metadata 與保留 ledger 完整性。

## 結果

| 測試組合 | 用戶端耗時 (s) | RPS | CPU 核心數 | 接近結束時 DB 大小 (bytes) | 接近結束時 GC prefix | 接近結束時 backlog | 用戶端期間 GC 刪除數 | 最終 sweep 後 prefix | 用戶端結束後等待時間 (s) | 恢復時間 (s) | 完整性檢查 (s) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| PerBatch, GC off | 224.965 | 44,451.4 | 1.262 | 601,775,646 | 0 | 9,150,000 | 0 | 0 | 0.002 | 1.044 | 53.840 |
| PerBatch, GC on | 246.557 | 40,558.5 | 1.270 | 552,636,691 | 1,125,888 | 8,024,112 | 1,125,888 | 9,150,000 | 298.281 | 3.885 | 3.593 |
| Checkpoint, GC off | 207.524 | 48,187.1 | 1.283 | 595,393,758 | 0 | 9,150,000 | 0 | 0 | 0.005 | 0.342 | 64.300 |
| Checkpoint, GC on | 236.760 | 42,236.9 | 1.256 | 582,332,364 | 1,125,888 | 8,024,112 | 1,125,888 | 9,101,808 | 286.370 | 1.493 | 1.459 |

RPS 只使用 10M request 的 client wall 作為分母；seed/setup、最終追趕、最終 GC、close/reopen、recovery、integrity 與餘額驗證都不計入。這次執行中，GC 開啟時 PerBatch RPS 比 GC 關閉時低 8.8%，Checkpoint RPS 低 12.3%。每個 case 只執行一次，這些是在此主機上的單次量測，並非重複執行的信賴區間。

兩個 GC 開啟的 case 都在 client 執行期間刪除 1,125,888 筆記錄。接近結束時的 backlog 是 `latest_seq - gc_prefix_seq`，其中包括仍受 durable watermark 或 balance-coverage guard 阻擋的記錄，因此不等於已符合刪除條件的記錄數。PerBatch 最終 sweep 到達 sequence 9,150,000。Checkpoint 最終 sweep 停在 sequence 9,101,808，因為已發布的 checkpoint manifest 尚未涵蓋後續 48,192 筆記錄。最終順序是發布最終 durable watermark、執行 GC loop 直到受阻或無進度、排空 checkpoints，然後 close/reopen 並驗證。checkpoint drain 後沒有再執行第二次 GC，因此後續 drain 可能讓保留尾端記錄符合之後 GC poll 的條件。`gc_prefix_after_settle` 記錄最終 sweep 觀察到的 prefix，不表示 checkpoint drain 後已達到最大清理量。聚焦測試 `checkpoint_lag_blocks_gc_until_manifest_covers_the_prefix` 驗證 GC 會等待 manifest 涵蓋該 prefix，且 coverage 前進後可刪除記錄。

當交易 timestamp 以非 sequence 順序到達時，安全 prefix 採保守策略：一筆尚不符合條件的記錄會阻擋後續記錄，直到該筆符合條件。GC worker 每次 sync write 處理一個有界 batch；prefix 前進時會在 batch 之間 yield，受阻或沒有進度時則等待設定的 poll interval。

## 延遲與 I/O

`safe_gc_latency.csv` 記錄 request stages、projector read/apply/progress-sync/total、GC scan/delete-build/sync-write/total，以及 watermark fence-wait/projection-wait/persist/total 的 p50/p95/p99（單位為 nanoseconds）與 sample count。sample count 為 0 代表該 case 沒有該 stage 的操作。GC 關閉的 case 因而沒有 GC samples；Checkpoint+GC 則有非零 GC calls，但當 balance coverage 阻擋 prefix 時，scan/delete/write 的時間皆為零。

下表列出 request total、queue 與 handler 的 p50/p95/p99。Total 與 queue 以秒顯示，handler 以毫秒顯示；每個 case 的三個 stage 各有 155,808 個 samples。

| 測試組合 | 每個 stage 的 samples | request.total p50 / p95 / p99 (s) | request.queue p50 / p95 / p99 (s) | request.handler p50 / p95 / p99 (ms) |
|---|---:|---:|---:|---:|
| PerBatch, GC off | 155,808 | 1.198 / 1.837 / 2.011 | 1.149 / 1.762 / 1.927 | 47.053 / 73.748 / 84.360 |
| PerBatch, GC on | 155,808 | 1.280 / 2.115 / 2.420 | 1.228 / 2.028 / 2.326 | 50.433 / 84.806 / 100.882 |
| Checkpoint, GC off | 155,808 | 1.084 / 1.803 / 2.039 | 1.040 / 1.730 / 1.965 | 42.352 / 71.277 / 80.226 |
| Checkpoint, GC on | 155,808 | 1.239 / 2.066 / 2.247 | 1.188 / 1.983 / 2.161 | 48.594 / 82.583 / 92.416 |

Client 執行期間，PerBatch+GC 記錄 4,398 次 GC call。其 GC scan/delete-build/sync-write 的 p50/p95/p99 分別為：

- Scan：6.02 / 10.43 / 12.29 ms。
- Delete batch build：0.048 / 0.091 / 0.181 ms。
- Sync write：1.76 / 2.49 / 3.71 ms。
- End-to-end GC call：54.48 / 86.90 / 101.95 ms。

Checkpoint+GC 記錄相同次數的 calls；對應 p50/p95/p99 分別為 6.22 / 10.55 / 11.80 ms、0.047 / 0.061 / 0.089 ms、1.74 / 2.46 / 3.89 ms，以及 53.27 / 84.80 / 94.78 ms。End-to-end call time 包含 blocking-worker 排程與其他操作 overhead，因此可能大於各元件時間的總和。目前 instrumentation 無法直接歸因 GC total 減去已量測 scan/build/sync 後的差額，不能據此斷言其確切成因。

下表列出 I/O measured window 內累積 counters 的 delta，於 client window 結束附近取樣。I/O 與 storage sample 都在 `client_end` 之後取得；offset 分別是相對 `client_end` 的取樣時間（µs），來源為 `safe_gc_summary.csv` 的 `io_sample_offset_us` 與 `storage_sample_offset_us`。Process 與 device I/O、RocksDB WAL、flush、compaction 及 stall counters 均以測量窗口開始時的 counters 為基準。表中 stall 單位為 microseconds，bytes 為 bytes。

| 測試組合 | I/O offset (µs) | Storage offset (µs) | WAL syncs | WAL bytes | Flush write bytes | Compaction write bytes | Stall (µs) |
|---|---:|---:|---:|---:|---:|---:|---:|
| PerBatch, GC off | 3 | 515 | 39,853 | 1,459,512,251 | 557,585,106 | 1,158,809,089 | 0 |
| PerBatch, GC on | 4 | 3,376 | 44,231 | 1,502,462,073 | 597,847,634 | 1,355,589,598 | 0 |
| Checkpoint, GC off | 3 | 3,081 | 40,032 | 1,351,685,109 | 562,714,248 | 1,150,664,617 | 0 |
| Checkpoint, GC on | 4 | 553 | 44,347 | 1,394,633,369 | 578,921,824 | 1,004,022,716 | 0 |

封存資料也保留 process 與 device I/O 其他 counters。client endpoint 的 CPU sample 立即取得。Prefix/backlog 是接近結束時讀取的 atomic snapshot，與同時進行的 GC completion events 並非同一個原子快照。DB bytes 是稍後 storage sample 取得的 logical directory size；RocksDB logical delete 不保證實體檔案立即縮小，harness 也不會強制執行 compaction。這些 counters 是觀察值，instrumentation 未直接量測單一 I/O counter 與延遲或 GC 行為之間的因果歸屬。

## 驗證與限制

聚焦測試通過：

- `ledger_safe_gc`：28 passed。
- `ledger_account_store`：11 passed。
- `ledger_time_boundary`：31 passed。

四 case 的 `--smoke` 執行也通過，每 case 使用 200 個使用者與 40,000 個 request。它驗證了報告階段，並在較小的 client window 觀察到 PerBatch GC 刪除 23,029 筆記錄；此結果不能取代完整執行。

目的端是在來源 RocksDB close/reopen 期間仍保留同一個 instance 的 in-memory mock。測試依據明確的外部資料庫契約假設，將 mock apply 成功視為已耐久。這驗證來源端 recovery，以及 source progress-sync 失敗後的冪等重播；它沒有測試 process crash，也沒有驗證真實外部資料庫的 durability。RocksDB delete 是邏輯刪除；實際空間回收取決於後續 compaction。
