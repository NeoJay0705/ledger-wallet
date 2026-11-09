# pg-ledger-txn — PostgreSQL 冪等帳本交易（before/after）批次吞吐測試

在固定資源的單機 Docker 環境量測 PostgreSQL 18.6 處理「冪等 ledger 交易」的吞吐：一個 statement 處理一個視窗（混合多帳號）的請求，每筆回傳 `applied` / `existing` / `rejected:*` 與 before/after 餘額。Client 是 Rust（tokio + tokio-postgres），256 個 queue、256 條連線。

設計與分析文件在上層 `docs/`：
[需求](../../docs/03-01.raw-requirement-pg-ledger-txn-batch-benchmark.md) ·
[設計與 SQL](../../docs/03-02.development-design-pg-ledger-txn-batch-benchmark.md) ·
[結果分析](../../docs/03-03.development-analysis-pg-ledger-txn-batch-benchmark.md)

## 目錄
| 路徑 | 內容 |
|---|---|
| `src/bin/txn_batch_pg.rs` | 本次測試的 client（R1 語意：冪等、before/after、拒絕狀態、重送路徑、DB 端驗證） |
| `src/bin/batch_queue_pg.rs`, `src/bin/nobatch_pg.rs` | 較早的基準 client（舊 SQL：3 欄 ledger、`ON CONFLICT DO NOTHING`、不回傳記錄）；本次結果不使用 |
| `src/main.rs`, `src/backend.rs`, `src/config.rs`, `config/scylla.yaml` | 更早的 PG／ScyllaDB 比較專案遺留，本次結果不使用 |
| `sql/schema.sql`, `sql/txn_batch.sql` | schema 與每視窗執行的 statement（由 client 的 `BATCH_SQL` 取出） |
| `scripts/batch_queue_run.py` | wrapper：建立資源受限的 PG 容器、綁核、取樣資源、執行 client、驗收、清理 |
| `scripts/sql_functional_test.py` | SQL 功能測試（拋棄式容器） |
| `scripts/summarize_results.py` | 由 `results/` 產生 `results/summary.csv` 與 Markdown 表 |
| `results/<run>/` | 各 run 的原始量測（見下） |

## 環境需求（本機結果的條件）
- 16 個邏輯 CPU（8 核 SMT）。`--cpus 8` 的配置：DB 容器 CPU 0–7、client CPU 8–9、sampler CPU 14–15。client 與 DB 共用 SMT 兄弟核，干擾沒有量測。
- Docker、Python 3、Rust 1.98.1（上層 `rust-toolchain.toml`）。首次建置需下載 crates（使用 `--offline` 需先有 cargo cache）。
- wrapper 會檢查 DB 資料目錄與 Docker 目錄位於實體裝置 `sdb`（`scripts/batch_queue_run.py` 內寫死，約第 226–230、286 行）；在其他主機需改成自己的裝置名稱。
- 埠 `127.0.0.1:15432` 必須空閒；run 必須循序執行。
- **安全：** 腳本與 client 預設內含**僅限本機的拋棄式密碼**（`gate2d`／`postgres`），容器只綁 `127.0.0.1`，run 結束即刪除。不要拿去連任何真實資料庫，也不要把這些值當作憑證使用。`results/` 內已把 client 記錄的連線字串遮蔽為 `USER:PASSWORD`。

## 測試方式
```sh
cd benchmarks/pg-ledger-txn
# 1. Rust 單元測試（9 + 8 + 4 + 5 + 11 項；txn_batch_pg 的 11 項涵蓋重送／拒絕注入計數、去重、穩定排序、結果對應）
cargo test --locked --offline
# 2. Python 單元測試（54 項，其中 2 項因依賴原專案 artifact/binary pin 而 skip）
python3 -m unittest discover -s scripts
# 3. SQL 功能測試（需 Docker；拋棄式容器，不發布埠，結束自動移除）
python3 scripts/sql_functional_test.py
# 4. 端到端 smoke（約 1.5 分鐘；含重送與拒絕路徑，用 8 CPU profile）
python3 scripts/batch_queue_run.py artifacts/smoke --mode txn --cpus 8 --queues 256 \
  --users 2000 --requests-per-user 20 --batch-max 80 --retry-pct 5 --inject-rejects \
  --sample-interval-ms 250 --pg-conf wal_buffers=256MB --pg-conf max_wal_size=8GB --pg-conf track_wal_io_timing=on
```
Smoke 驗收（`artifacts/smoke/client/result.json`）：`windows=512`、`retries_sent=existing=2000`、`injected_unknown=injected_invalid=256` 且兩種 rejected 各 256、`missing/unexpected/replay_mismatch=0`、`verification.valid=true`。

## 重現正式 cell（每格約 1–2 分鐘，4,000,000 個新請求）
```sh
COMMON="--mode txn --cpus 8 --queues 256 --users 100000 --requests-per-user 40 --sample-interval-ms 250 \
  --pg-conf wal_buffers=256MB --pg-conf max_wal_size=8GB --pg-conf track_wal_io_timing=on"
python3 scripts/batch_queue_run.py artifacts/bm4000 $COMMON --batch-max 4000
python3 scripts/batch_queue_run.py artifacts/bm8000 $COMMON --batch-max 8000
python3 scripts/batch_queue_run.py artifacts/bm8000-retry5 $COMMON --batch-max 8000 --retry-pct 5   # 另加 200,000 筆重送，共 4.2 M
```
每個 run 前請確認：無殘留 `ledger-bq*` 容器、埠空閒、磁碟閒置（sdb 寫入 < 1 MiB/s、`Dirty` < 50 MB）。**驗收**：`client/result.json` 的 `verification.valid=true`、`outcomes.applied=4000000`、`missing/unexpected/replay_mismatch=0`、`windows=1024`（4000）／`512`（8000）、`existing=retries_sent`；`settings.txt` 的 `wal_buffers=32768`（8 kB 頁）、`max_wal_size=8192 MB`、`track_wal_io_timing=on` 且來源為 `command line`；`wal_stats.settled=true`；停頓判定：device flush await ≤ 2 ms（超過視為磁碟停頓，該 run 不採用，最多重跑一次）。

## 結果（每格 1 次，PostgreSQL 18.6，8 CPU）
詳細解讀與限制見分析文件。`rps_total` = 回覆的請求總數／秒；`rps_applied_unique` = 新套用的交易／秒。延遲是**每個請求的 statement 執行時間**（含 SQL 重試；不含 queue 等待與組陣列／解析時間），會隨視窗大小增加，只能在相同視窗內比較。

| run | 視窗 | 總請求 | rps_total | rps_applied_unique | p50 / p95 / p99 延遲 (s) | DB CPU 核（%） | client CPU 核 | 裝置寫入 MiB/s | write await ms | flush await ms | 磁碟 util | WAL MB | 狀態 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| window4000-run1 | 4000 | 4.0 M | 195,047 | 195,047 | 4.59 / 6.98 / 7.92 | 7.43（93%） | 0.26 | 108 | 3.48 | 0.45 | 26% | 1048 | ok |
| window4000-run2 | 4000 | 4.0 M | 204,926 | 204,926 | 4.39 / 6.59 / 7.95 | 7.43（93%） | 0.27 | 105 | 4.20 | 0.40 | 26% | 1048 | ok |
| window8000-run1 | 8000 | 4.0 M | 262,134 | 262,134 | 6.30 / 9.44 / 10.17 | 7.27（91%） | 0.38 | 127 | 5.34 | 0.37 | 28% | 990 | ok |
| window8000-run2-STALLED | 8000 | 4.0 M | 126,183 | 126,183 | 14.93 / 16.57 / 17.89 | 3.88（49%） | 0.20 | 76 | 62.83 | 5.81 | 75% | 990 | 磁碟停頓，不採用 |
| window8000-retry5-run1-STALLED | 8000 + ≤422 重送 | 4.2 M | 157,716 | 150,206 | 10.57 / 19.53 / 20.41 | 4.48（56%） | 0.24 | 84 | 25.88 | 4.50 | 62% | 991 | 磁碟停頓，不採用 |
| window8000-retry5-run2 | 8000 + ≤422 重送 | 4.2 M | 259,833 | 247,460 | 6.76 / 10.23 / 10.94 | 7.28（91%） | 0.39 | 137 | 6.41 | 0.50 | 31% | 988 | ok |

## 原始量測資料（`results/<run>/`）
| 檔案 | 內容 |
|---|---|
| `result.json` | RPS、延遲百分位、視窗大小分佈、`outcomes`（applied／existing／rejected／missing…）、DB 端驗證、`wal_stats`（`pg_stat_wal`、`pg_stat_io` 前後快照、LSN 差） |
| `per_second.csv` | 每 250 ms 取樣：完成請求／statement 增量、in-flight 請求、忙碌連線、queue 剩餘、server active |
| `resources-measured.csv` | 量測視窗內每 250 ms：DB／client／sampler CPU（core-equivalents）、DB cgroup 記憶體與 IO、client RSS、裝置讀寫 MiB/s／IOPS／await／util／queue depth／flush |
| `resource-summary.json` | 上述的平均與最大值（DB 記憶體為 cgroup `memory.current`，**含 page cache**，會逼近容器 8 GiB 上限，不等於 PG 行程記憶體） |
| `settings.txt`, `resource-placement.json`, `latency.hdr` | 生效的 PG 參數、綁核與裝置配置、HDR 延遲直方圖 |
| `summary.csv` | `python3 scripts/summarize_results.py` 產生的彙整表 |

`smoke/` 是 2,000 帳號的小型驗證 run，資源視窗不足一個完整區間，只用於功能驗證。

## 限制（摘要）
每格只有 1–2 個有效樣本；磁碟為共用的消費級 SATA SSD，6 次中 2 次停頓；視窗 4000 比先前舊 SQL 慢約 20% 的原因**未查明**；沒有等待事件與執行計畫的取樣；未量 SMT 干擾；沒有發生 23505／40P01，SQL 重試路徑只在單元測試／設計層驗證；每個帳號只有單一 executor，所以沒有量到跨連線的鎖競爭。完整清單見分析文件。
