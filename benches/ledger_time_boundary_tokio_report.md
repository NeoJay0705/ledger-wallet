# Tokio 單 shard 交易時間分界基準測試報告

## 執行摘要

修正後的 per-user 歷史請求排列完成四個預設情境，每個情境實際量測 10,000,000 requests。四個 case 各自通過建立 DB 前與 measured window 前的嚴格 preflight；每個 case 完成後逐帳戶驗證 50,000 個使用者餘額，並移除及確認 trial RocksDB 目錄不存在。原始 CSV 與執行記錄為 [`full-10m-results.csv`](data/ledger_time_boundary/full-10m-results.csv) 及 [`full-10m-run.log`](data/ledger_time_boundary/full-10m-run.log)。本次 run ID 是 `1790758172820073645`。

每個情境的 `fenced_admissions` 都是 0，實測只涵蓋沒有 admission 等待 fence 的 watermark 發布。已接受請求 drain、enqueue 後 caller 取消仍保留 guard、projection lag 阻止發布、持久化錯誤維持舊分界等 contended/error path 由 focused tests 驗證；不能把這組 RPS 當成 fence contention 的效能結果。

## 環境與命令

- 日期：2026-09-30（Asia/Taipei）
- OS/kernel：Linux 6.17.0-35-generic，x86_64
- CPU：AMD Ryzen 7 3700X，8 cores / 16 logical CPUs
- 記憶體：62 GiB；preflight 前約 44 GiB available
- DB 裝置：`sdb2`（`/dev/sdb2`）；檔案系統 915 GB，preflight 前約 89 GB 可用
- Rust/Cargo：1.98.1

```sh
BINDGEN_EXTRA_CLANG_ARGS='-I/usr/lib/gcc/x86_64-linux-gnu/13/include' \
  cargo bench --bench ledger_time_boundary_tokio
```

Runtime 使用 4 個 Tokio worker threads、50,000 個 user 及 50,000 個 coroutine，一個 coroutine 固定服務一位 user，且同一時間最多有一筆未回覆的 request。每個 measured case 是 200 requests/user，共 10,000,000 requests。Commit queue 容量 50,000、batch size 2,048、batch timeout 5 ms；projector batch size 256。Watermark manager 每秒提出候選，retention 為 60 秒。歷史 lookup 使用 10 ms 非同步合成延遲；projector 無額外合成延遲。

每個 case 在開啟 AccountStore、配置 mock projection、寫入 50,000 筆真實投影 seed 前先做一次 idle/resource preflight；measured window 前再做第二次。門檻為 CPU busy ≤ 10%、目標裝置 busy ≤ 5%、available memory 至少 3,378,106,368 bytes、free space 至少 8,753,741,824 bytes；每次觀察 3 秒，最多等 60 秒。八次 preflight 全通過；各 phase 的 attempts 為 setup `[1, 14, 5, 1]`、measurement `[1, 1, 3, 1]`。實際 busy、記憶體與空間數值可由 run log 查看。

## 實測結果

RPS 分母為最後一筆 client reply 被觀察到的 `client_wall`。CPU core equivalents 為 process CPU seconds 除以 `settled_wall`。Latency 使用固定 `splitmix64(logical_id) % 64 == 0` 的 deterministic sample，nearest-rank percentile；old latency 表顯示 lookup stage 的 p50/p95/p99，單位為 ms。

| 舊時間比例 | Fresh / old 完成數 | 舊查詢 hit / miss | Fresh / old / total RPS | Client wall | CPU core equivalents | Peak RSS | hit / miss latency samples |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0% | 10,000,000 / 0 | 0 / 0 | 56,601.2 / 0 / 56,601.2 | 176.675 s | 1.356 | 1.72 GiB | 0 / 0 |
| 1% | 9,900,000 / 100,000 | 50,000 / 50,000 | 53,885.0 / 544.3 / 54,429.3 | 183.725 s | 1.332 | 1.79 GiB | 769 / 810 |
| 5% | 9,500,000 / 500,000 | 250,000 / 250,000 | 47,874.5 / 2,519.7 / 50,394.2 | 198.435 s | 1.150 | 1.82 GiB | 3,954 / 3,839 |
| 10% | 9,000,000 / 1,000,000 | 500,000 / 500,000 | 53,893.2 / 5,988.1 / 59,881.3 | 166.997 s | 1.337 | 1.83 GiB | 7,793 / 7,867 |

不同 mix 的結果各只執行一次，因此這些數字是描述性單次量測；沒有足夠重複次數或信賴區間來宣稱 mix 造成 RPS 差異。Process CPU 與裝置/process I/O 量測的 settled window 包含 client window 及其後處理，和 client-wall RPS 分母不同。四個 case 的 RocksDB stall ticker delta 都是 0；目標 device 為 `sdb2`。完整 process I/O、device counters、WAL/flush/compaction、projector/watermark batches 與 stages 都保存在 CSV。

| 舊時間比例 | Fresh total p50 / p95 / p99 | Old hit lookup p50 / p95 / p99 | Old miss lookup p50 / p95 / p99 |
| ---: | ---: | ---: | ---: |
| 0% | 866.8 / 1,383.8 / 1,635.4 | 無樣本 | 無樣本 |
| 1% | 930.4 / 1,431.5 / 1,854.1 | 11.014 / 11.750 / 11.873 | 11.055 / 11.710 / 11.950 |
| 5% | 909.4 / 1,507.9 / 5,302.8 | 11.055 / 11.762 / 11.956 | 11.073 / 11.774 / 11.956 |
| 10% | 924.2 / 1,465.6 / 1,792.5 | 11.104 / 11.814 / 11.996 | 11.091 / 11.840 / 12.006 |

Fresh 和 old 各 stage 的 percentile 及 request sample counts 都在 CSV。每個 stage 的 percentile 分開計算，不可彼此相加；程式沒有記錄 per-request residual/scheduler-staging span，因此不能用 stage percentile 重建 end-to-end percentile。

| 舊時間比例 | Fresh queue wait p50 / p95 / p99 (ms) | Fresh handler p50 / p95 / p99 (ms) | Watermark persist p50 / p95 / p99 (ms) | WAL syncs / WAL bytes (GB) / target device writes (GB) |
| ---: | ---: | ---: | ---: | ---: |
| 0% | 832.0 / 1,328.4 / 1,575.6 | 33.5 / 55.3 / 70.2 | 1.1 / 6.0 / 82.4 | 5,258 / 1.500 / 3.836 |
| 1% | 892.5 / 1,373.1 / 1,783.4 | 35.5 / 57.4 / 71.9 | 1.1 / 6.3 / 139.7 | 5,215 / 1.485 / 3.783 |
| 5% | 872.5 / 1,446.5 / 5,072.5 | 35.2 / 59.3 / 196.0 | 1.1 / 249.3 / 984.5 | 5,024 / 1.425 / 3.587 |
| 10% | 886.6 / 1,408.4 / 1,725.2 | 35.8 / 58.7 / 72.8 | 1.1 / 5.8 / 12.0 | 4,743 / 1.350 / 3.290 |

5% case 的 fresh total p99 約 5.30 s，同一 case fresh queue p99 約 5.07 s；這是同次 run 中 queue tail 較長的線索。兩組 percentile 是各自分布摘要，並非逐 request 配對，不能據此做精確的 end-to-end latency 拆解。I/O 欄位是全 measured/settled window 的累計 delta；`target device writes` 是裝置 aggregate counter，不代表只有此 process 寫入。

## 負載及邊界解讀

- 每個 user 的 200 request slots 依 `(request_index * 37 + splitmix64(user) % 200) % 200` 排列；37 與 200 互質。`old_pct` 的前段 slots 為 hit，接續同數 slots 為 miss，所以每位 user 在 1%、5%、10% case 分別恰有 1、5、10 個 hit 及 miss，並分散在 request positions。其餘 requests 是 fresh credit transaction。
- 每位 user 只有一筆 projector 實際產生的 seed record（`tx_id=0`）。Old hits 重複使用 50,000 個 hot keys，最多各半 case 會送出 500,000 筆 hit；misses 使用唯一 absent keys。這不是大量歷史 keys 的一般化隨機查詢模型。所有 hit/miss latency sample strata 在三種舊時間 case 都有實際 samples。
- 每個 case 完成全部 replies 後、`client_wall` 結束後，檢查全部 50,000 個記憶體餘額。預期為 seed balance 1 加上該 user 所有 fresh credits：0%、1%、5%、10% case 分別是 201、199、191、181；四次全數通過。
- Fresh measured transactions 全是 amount 1 的 credit；本 workload 不包含 debit、refund 或 balance-query traffic。
- 歷史目的端是 process 內的 `MockProjectionStore`。RocksDB 只同步持久化 watermark metadata；projection 本身 volatile。因此此資料不保證 restart 後可安全恢復歷史查詢，不涵蓋 GC、外部 DB 或 production durability。
- `watermark_target_sequence_final` 是最後一次已發布 watermark 在 drain/fence 後捕獲並等待 projection 到達的 sequence。`projection_final_sequence` 是所有 measured commits 完成且 projector 最終追上後的 sequence，包含 50,000 筆 seed；發布後 `transaction_at >= candidate` 的新請求仍可 commit，所以這兩欄通常不同。本次四個 case 的值依序為 `10,023,760 / 10,050,000`、`9,917,264 / 9,950,000`、`9,544,528 / 9,550,000`、`9,050,000 / 9,050,000`。
- `fenced_admissions=0` 表示 measured requests 沒有在候選區間等待 fence；不表示測過有競爭時的等待成本。

## 驗證

- `cargo test --test ledger_time_boundary`：31 passed。
- `cargo bench --bench ledger_time_boundary_tokio --no-run`：passed。
- `cargo bench --bench ledger_time_boundary_tokio -- --smoke`：四個 case 各 4,000 requests passed；核對 CSV 欄位、hit/miss 分布、全部帳戶餘額及資料目錄 cleanup。
- 上述預設完整命令：四個 case 各完成 10,000,000 requests，八次 preflight 及四次全帳戶餘額檢查通過。Canonical CSV/log 是本節所連的 run ID `1790758172820073645` 原始執行結果。
