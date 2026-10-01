# Tokio 單分片 Credit／Debit Ledger Pipeline 基準報告

## 執行摘要

2026-10-01 以 canonical strict 預設完成六個 case，每個 case 有 50,000 個 coroutine、每個 coroutine 200 筆 request，共 10,000,000 筆 request。六案均通過 reply、操作比例、餘額、projection、GC、安全 checkpoint、integrity、RocksDB close/reopen recovery 驗證；完成後各 case 的臨時 DB 均已清除。

下列數值來自一次固定順序的完整跑次。它們描述這次執行，不代表重複試驗的均值或統計差異；GC on 的 0% case 本次 RPS 高於 GC off，不能由這次固定順序結果推論 GC 提升吞吐。5% 歷史流量 case 每案只有 9,500,000 fresh commits，因此同時列出總 request RPS 與 fresh commit/s。

## 工作負載與執行環境

- Tokio multi-thread runtime 3 workers；單 shard；bounded queue 50,000；batch 最多 2,048 筆或 5 ms。
- 每位使用者量測前寫入 Credit 100、Credit 1、Debit 1，seed balance 為 100；seed 共 150,000 筆，計時前完成。
- 每個 measured request 金額為 1；Credit 和 Debit 各占 50%。5% 歷史 case 含 250,000 個 seed exact hit 與 250,000 個 miss，fresh request 為 9,500,000，Credit／Debit 各 4,750,000。
- 5% case 的歷史 lookup 使用 10 ms async mock delay。GC retention 500 ms，watermark tick 100 ms，GC batch 256。Checkpoint 門檻沿用正式預設 100,000。
- 儲存裝置為 `/dev/sdb2`（ext4）。完整跑次命令：

```sh
BINDGEN_EXTRA_CLANG_ARGS='-I/usr/lib/gcc/x86_64-linux-gnu/13/include' \
  cargo bench --locked --bench ledger_pipeline_tokio
```

## 吞吐與 CPU

| Case | Client wall (s) | 總 request RPS | Fresh commit/s | CPU cores |
|---|---:|---:|---:|---:|
| PerBatch · GC off · 0% | 293.806 | 34,036.0 | 34,036.0 | 1.031 |
| PerBatch · GC on · 0% | 271.959 | 36,770.3 | 36,770.3 | 1.251 |
| PerBatch · GC on · 5% | 285.125 | 35,072.4 | 33,318.8 | 1.109 |
| Checkpoint · GC off · 0% | 261.222 | 38,281.6 | 38,281.6 | 1.101 |
| Checkpoint · GC on · 0% | 257.560 | 38,825.9 | 38,825.9 | 1.236 |
| Checkpoint · GC on · 5% | 243.331 | 41,096.3 | 39,041.5 | 1.248 |

總 request RPS 為 request count 除以最後 coroutine 收到最後 reply 的 client window。Fresh commit/s 使用同一 client window，分子只計 fresh commit；歷史 hit/miss 不計入 fresh commit。

## Client 與背景 stage latency

下列 latency 為毫秒，欄位順序是 p50 / p95 / p99。Request stages 使用 deterministic hash sample stride 64；本次 request.total 與其他 request stages 各有約 155,808 個樣本。各 stage 分位數不可相加。完整 25 個 stage 的 p50/p95/p99 與樣本數列於 stages CSV。

| Case | Request total | Request queue | Request handler | Historical lookup |
|---|---:|---:|---:|---:|
| PerBatch · GC off · 0% | 1209.38 / 1860.28 / 6869.53 | 1159.79 / 1785.30 / 6591.58 | 47.38 / 74.41 / 260.40 | — |
| PerBatch · GC on · 0% | 1343.09 / 2076.11 / 2488.75 | 1288.27 / 1992.61 / 2371.74 | 52.64 / 83.26 / 101.13 | — |
| PerBatch · GC on · 5% | 1296.45 / 2107.00 / 11674.81 | 1242.95 / 2022.85 / 11407.68 | 50.97 / 83.21 / 481.52 | 11.10 / 11.77 / 11.94 |
| Checkpoint · GC off · 0% | 1141.87 / 1898.61 / 6926.69 | 1095.65 / 1824.99 / 6671.09 | 44.78 / 74.08 / 275.33 | — |
| Checkpoint · GC on · 0% | 1281.82 / 2005.32 / 2283.87 | 1229.87 / 1925.12 / 2196.11 | 50.26 / 79.97 / 98.25 | — |
| Checkpoint · GC on · 5% | 1233.71 / 2024.55 / 2324.70 | 1183.39 / 1944.52 / 2225.95 | 48.05 / 80.73 / 98.86 | 11.10 / 11.76 / 11.94 |

5% case 的 sampled historical lookup p50/p95/p99 約為 11.10 / 11.76 / 11.94 ms；0% case 沒有歷史 lookup 樣本。三案觀察到較高的 request.total／request.queue p99：PerBatch GC off 0% 為 6.870／6.592 s、PerBatch GC on 5% 為 11.675／11.408 s、Checkpoint GC off 0% 為 6.927／6.671 s。這些 tail 出現在 queue wait 樣本中；目前 instrumentation 無法判定其原因。5% mixed case 的 queue 和 handler 分位數涵蓋所有 sampled request，歷史 route 對其繞過的 stages（包含 commit-only stages）貢獻零值；history lookup 另列，因此 queue／handler 數值不是 fresh-only 條件分位數。

Background stage p95（毫秒；無事件樣本時以 `—` 表示）：

| Case | Projection total | GC total | Watermark total | Checkpoint total |
|---|---:|---:|---:|---:|
| PerBatch · GC off · 0% | 2.40 | — | 1313.57 | — |
| PerBatch · GC on · 0% | 2.73 | 84.76 | 1573.09 | — |
| PerBatch · GC on · 5% | 2.73 | 84.97 | 1577.20 | — |
| Checkpoint · GC off · 0% | 2.37 | — | 1338.16 | 9.06 |
| Checkpoint · GC on · 0% | 2.72 | 81.48 | 1489.93 | 10.09 |
| Checkpoint · GC on · 5% | 2.68 | 82.35 | 1541.23 | 8.38 |

## I/O、GC 與 checkpoint

I/O 和 RocksDB ticker 樣本在 JoinSet 收集後讀取；device I/O offset 本次為 16–183 µs，storage/RocksDB/DB-size offset 為 538–726 µs。這些 delta 可能包含 client window 結束後的短尾端工作。CPU sample 在最後 reply 後立即取得，CPU offset 為 0 µs。Target device counters 是整個 `/dev/sdb2` 的 delta，不是 process-exclusive；另列 process write counters 供區分。

| Case | Peak RSS (GiB) | Process writes (GB) | Target-device writes (GB) | Device busy (ms) | WAL GB / syncs | Compaction writes (GB) | RocksDB stall (µs) |
|---|---:|---:|---:|---:|---:|---:|---:|
| PerBatch · GC off · 0% | 1.74 | 3.99 | 4.64 | 102,330 | 1.62 / 44,274 | 1.56 | 0 |
| PerBatch · GC on · 0% | 1.79 | 4.26 | 4.94 | 54,284 | 1.67 / 49,144 | 1.74 | 0 |
| PerBatch · GC on · 5% | 1.85 | 3.78 | 4.43 | 86,037 | 1.59 / 46,685 | 1.35 | 0 |
| Checkpoint · GC off · 0% | 1.86 | 3.69 | 4.33 | 79,373 | 1.50 / 44,455 | 1.38 | 0 |
| Checkpoint · GC on · 0% | 1.95 | 3.64 | 4.34 | 52,302 | 1.55 / 49,265 | 1.24 | 0 |
| Checkpoint · GC on · 5% | 1.95 | 3.56 | 4.21 | 49,577 | 1.47 / 46,810 | 1.26 | 0 |

近 client window end 的進度與清理量：

| Case | Durable projection backlog | GC prefix | GC backlog | Checkpoint lag | GC prefix after settle / final seq |
|---|---:|---:|---:|---:|---:|
| PerBatch · GC off · 0% | 1,664 | 0 | 10,150,000 | 0 | 0 / 10,150,000 |
| PerBatch · GC on · 0% | 1,408 | 1,251,072 | 8,898,928 | 0 | 10,150,000 / 10,150,000 |
| PerBatch · GC on · 5% | 0 | 1,188,096 | 8,461,904 | 0 | 9,650,000 / 9,650,000 |
| Checkpoint · GC off · 0% | 1,664 | 0 | 10,150,000 | 48,768 | 0 / 10,150,000 |
| Checkpoint · GC on · 0% | 1,664 | 1,250,048 | 8,899,952 | 48,768 | 10,101,232 / 10,150,000 |
| Checkpoint · GC on · 5% | 0 | 1,191,168 | 8,458,832 | 48,480 | 9,601,520 / 9,650,000 |

GC backlog 欄位是 `latest_seq - gc_prefix`，表示 prefix 後方仍保留的全部 sequence，不代表當下都已符合 watermark、projection 和 checkpoint 條件、可以刪除。

GC-off cases retained prefix 0 and recorded no GC deletes. PerBatch GC-on cases caught up to final sequence after settlement. Checkpoint GC-on cases stopped at the latest balance checkpoint: 10,101,232 of 10,150,000 records for the 0% case, and 9,601,520 of 9,650,000 for the 5% case. The remaining 48,768 and 48,480 records were retained because they were beyond manifest coverage; no unsafe deletion was performed.

GC-on settlement after client completion took 297.625 s, 256.410 s, 306.312 s, and 307.044 s for the four GC-on cases. These final projection/watermark/GC sweeps are outside client RPS and request latency. PerBatch GC reached the full eligible prefix; Checkpoint GC remained bounded by the published checkpoint manifest.

## Strict preflight 與資料驗證

Canonical preflight 觀察 3 s、最長等待 60 s，CPU busy 上限 10%、磁碟 busy 上限 5%；保留額外記憶體 768 MiB 與額外磁碟 1 GiB，並計入每筆 record 256 bytes 記憶體及 768 bytes 磁碟估算。全域 setup preflight 以 1 次嘗試通過：CPU 1.694%、磁碟 0.200%、可用記憶體 45.13 GB、可用磁碟 93.33 GB。每個 case 建 DB 前與測量開始前都各自通過 strict preflight：

| Case | Setup CPU / disk busy (%) | Measurement CPU / disk busy (%) |
|---|---:|---:|
| PerBatch · GC off · 0% | 1.946 / 0.067 | 1.798 / 0.067 |
| PerBatch · GC on · 0% | 2.110 / 0.100 | 1.630 / 0.067 |
| PerBatch · GC on · 5% | 1.902 / 0.067 | 1.632 / 0.000 |
| Checkpoint · GC off · 0% | 1.989 / 1.766 | 1.841 / 4.599 |
| Checkpoint · GC on · 0% | 2.508 / 0.000 | 1.819 / 0.000 |
| Checkpoint · GC on · 5% | 1.944 / 0.367 | 1.693 / 0.067 |

歸檔 CSV 已以標準 CSV parser 讀取並檢查每列欄位數：summary 為 6 rows × 76 columns；stages 為 150 rows（每案 25 stages）；background 為 251,774 rows。每案 request 總數恰為 10,000,000，Credit／Debit 各 5,000,000；5% case 的 fresh、hit、miss 分別為 9,500,000、250,000、250,000。六案成功後沒有殘留 trial DB 目錄。

## 原始結果與限制

- [summary CSV](data/ledger_pipeline/run-1790787222900941094/ledger_pipeline_summary.csv)
- [stage percentile CSV](data/ledger_pipeline/run-1790787222900941094/ledger_pipeline_stages.csv)
- [background event CSV](data/ledger_pipeline/run-1790787222900941094/ledger_pipeline_background.csv)
- [run metadata and case log](data/ledger_pipeline/run-1790787222900941094/ledger_pipeline_run.log)
- [canonical command stdout capture](data/ledger_pipeline/run-1790787222900941094/ledger_pipeline_stdout.log)

這是一次固定順序跑次，沒有重複執行；機器狀態、page cache 和 RocksDB compaction 會影響結果。表格描述本次觀測，不能據此宣稱 GC 或 checkpoint 對吞吐的因果效果。Destination 是同程序內的 mock；source RocksDB 關閉後重開時保留同一份 mock destination。測試不模擬程序 crash，也不宣稱外部 projection database 的 crash durability。GC 使用同步 WAL delete batch，沒有強制 RocksDB compaction。

終端輸出中 `RUN` 行曾將 `expected_seed_records=10150000` 寫成 seed count；該值其實是總 durable record 數（150,000 seeds + 10,000,000 measured requests）。seed-only count 為 150,000；歸檔 metadata 的 `seed_records=150000` 正確。歸檔的 stdout capture 與原始 command output byte-for-byte 相同；其餘 raw archive 也維持原樣。benchmark 程式已修正未來 `RUN` 行的欄位名稱為 `expected_total_records`。
