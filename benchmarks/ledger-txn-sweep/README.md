# ledger-txn-sweep — PostgreSQL／TiDB 帳本交易掃描

本 crate 整理五組 ledger transaction 測試案例、執行程式、測試與詳細結果報告（`docs/04`–`08`、[`RESULTS.md`](RESULTS.md)）。原始的逐次量測輸出（`artifacts/`）不納入版控；結果只代表列明硬體與配置下的觀測，並非一般化效能保證。既有 [`benchmarks/pg-ledger-txn`](../pg-ledger-txn/README.md) 是較早的獨立 harness，本次未修改。

## 測試案例與文件

- **04 TiDB R1**：CPU4／CPU8、before/after、no RF、預切 region，並對照 PG R1：[需求](../../docs/04-01.raw-requirement-ledger-txn-tidb.md) · [設計與任務](../../docs/04-02.development-design-ledger-txn-tidb.md) · [分析](../../docs/04-03.development-analysis-ledger-txn-tidb.md)
- **05 Batch sweep**：256 queues、256 connections，批次大小掃描：[需求](../../docs/05-01.raw-requirement-ledger-txn-batch-sweep.md) · [設計](../../docs/05-02.development-design-ledger-txn-batch-sweep.md) · [分析](../../docs/05-03.development-analysis-ledger-txn-batch-sweep.md)
- **06 Connection sweep**：batch 50／100，連線數 4–256：[需求](../../docs/06-01.raw-requirement-ledger-txn-connection-sweep.md) · [設計與任務](../../docs/06-02.development-design-ledger-txn-connection-sweep.md) · [分析](../../docs/06-03.development-analysis-ledger-txn-connection-sweep.md)
- **07 Fixed-connection sweep**：PG CPU4／CPU8、TiDB CPU4、TiDB CPU8+，16 或 32 connections：[需求](../../docs/07-01.raw-requirement-ledger-txn-fixed-connection-sweep.md) · [設計與任務](../../docs/07-02.development-design-ledger-txn-fixed-connection-sweep.md) · [分析](../../docs/07-03.development-analysis-ledger-txn-fixed-connection-sweep.md)
- **08 TiKV6/TiDB8**：TiKV 4／tidb-server 4 與 TiKV 6／tidb-server 8 補測：[需求](../../docs/08-01.raw-requirement-ledger-txn-tikv6-tidb8.md) · [設計](../../docs/08-02.development-design-ledger-txn-tikv6-tidb8.md) · [分析](../../docs/08-03.development-analysis-ledger-txn-tikv6-tidb8.md)

## 目錄與重現

| 路徑 | 說明 |
|---|---|
| `src/bin/` | PostgreSQL 與 TiDB Rust clients |
| `scripts/` | runners、sweep drivers、reports、audit 與測試 |
| `RESULTS.md` | 各曲線結果摘要表（由 `summarize_sweeps.py` 從本機 `artifacts/` 產生） |
| `artifacts/` | 本機執行輸出，**不入版控**（`.gitignore`），重跑後才會出現 |
| `SECURITY-NOTES.txt` | 開發用拋棄式測試憑證說明 |

`artifacts/` 與 `scripts/` 路徑均相對於本 crate 根目錄。

### 1. 不需 Docker、不需 artifacts 的檢查

```sh
cd benchmarks/ledger-txn-sweep
python3 -m unittest discover -s scripts -p 'test_*.py'      # 依賴本機 artifacts 的測試在缺資料時會 skip
CARGO_TARGET_DIR="$PWD/target" cargo test --locked --offline
python3 scripts/conn_sweep.py all --db tidb --cpus 4 --dry-run   # 只列命令，不執行
python3 scripts/fixconn_sweep.py run --series tidb-k6t8-n16 --dry-run
```

### 2. 完整重現實驗（需 Docker 與權限；全部輸出寫入本機 `artifacts/`，不入版控）

照順序執行，後面的步驟會讀前面的結果：

```sh
cd benchmarks/ledger-txn-sweep
cargo build --release --locked   # 加 --offline 需要本機 Cargo cache 已有相依套件
# (a) R1 基準：PG CPU4 與 TiDB CPU4/CPU8，batch 4000/8000（指令見 docs/04-02 的 T4）
python3 scripts/batch_queue_run.py artifacts/txnpg4-bm4000 --mode txn --cpus 4 --requests-per-user 40 --queues 256 \
  --batch-max 4000 --sample-interval-ms 250 --pg-conf wal_buffers=256MB --pg-conf max_wal_size=8GB --pg-conf track_wal_io_timing=on
python3 scripts/txn_tidb_run.py artifacts/txntidb-c4-bm4000 --cpus 4 --requests-per-user 40 --queues 256 --batch-max 4000 --sample-interval-ms 250
# 其餘格子同理（--batch-max 8000、--cpus 8）
# (b) batch sweep（256 queue、256 連線）；先把 sweep-imports.example.json 複製為 artifacts/sweep-imports.json，
#     並把其中的 source 路徑改成 (a) 實際產生的目錄
cp sweep-imports.example.json artifacts/sweep-imports.json
for db in pg tidb; do for c in 4 8; do python3 scripts/batch_sweep.py run --db $db --cpus $c; done; done
#     預設 grid 到 8000；PG 在 8000 仍上升時，本次另以 --batches 延伸到 12000、16000（例：--db pg --cpus 4 --batches 12000,16000）
#     飽和點附近的 replicate：先看 summarize 的建議，再執行 batch_sweep.py replicate（參數見 --help）
# (c) 連線數掃描（batch 50/100）
for db in pg tidb; do for c in 4 8; do python3 scripts/conn_sweep.py all --db $db --cpus $c; done; done
# (d) 固定連線 batch 掃描（需要 (c) 的 batch 50/100 結果）與 TiKV6 補測
python3 scripts/fixconn_sweep.py run --series all
python3 scripts/fixconn_sweep.py run --series tidb-k4t4-n16
python3 scripts/fixconn_sweep.py run --series tidb-k6t8-n16
# (e) 稽核與報告
python3 scripts/fixconn_sweep.py audit
python3 scripts/conn_sweep.py audit
python3 scripts/fixconn_report.py /tmp/fixconn-report.md     # 不給路徑會寫到本 crate 的 docs/（已 gitignore）
python3 scripts/tikv6_report.py                              # 寫到 docs/ledger-tikv6-report.md
python3 scripts/conn_sweep_report.py                         # 寫到 docs/ledger-connsweep-report.md
python3 scripts/summarize_sweeps.py --out RESULTS.md         # 結果摘要表
```

注意：每個點都會先過 idle gate（CPU 不閒置會重試，最多 60 秒）；TiDB 兩個 store 的組態每次固定等 125 秒讓 PD 平衡。TiDB batch sweep 只有在 batch 6000 成功時才會嘗試 8000，且 8000 在 256 連線下於本機未能完成（見 `docs/05-03`）。

### 3. 環境需求

只支援 `--mode txn`（`batch_queue_run.py`）與 `txn_tidb_run.py` 這兩條路徑；`batch_queue_run.py` 的其他 mode、`single_run.py` 與 `campaign.py` 為從原專案帶入的輔助程式，其對應的 client／設定檔（`batch_queue_pg`、`nobatch_pg`、`config/scylla.yaml`）不在本 crate，單獨執行會失敗。


真正執行需要 Docker 與足夠權限，以及 8-core／16-thread host 的 CPU layout。量測硬體：Ryzen 7 3700X（8C/16T）、62 GB RAM、單一 SATA disk `sdb`。host 上必須可用的埠：14000（tidb）、20180／20181（TiKV status）、15432（PG）。PD 的 2379 只在 Docker 內部網路使用，不綁定 host 埠（host 上 2379 被其他服務占用也不影響）。勿與其他工作負載同時量測。

量測規則：idle gate 要求每顆 CPU idle 至少 90%，且依 gate 檢查其他程序 CPU 使用；flush await >2 ms 依規則觸發一次 rerun；replicate 相差超過 ±15% 時依案例規則補測；TiDB durability gate 必須通過。細節見各案例文件與 runner。腳本中的 `postgres`／`gate2d` 是**僅限本機拋棄式容器**的開發測試密碼，不可連接真實資料庫。

## 結果摘要

tidb-server CPU 為其 cpuset 的使用比例，≥85% 視為接近飽和（tidb-c4-n16 的 87.9%、k4t4 的 85.4–86.5% 屬此類，該配置下 tidb-server 為可能的限制）。

飽和定義：最小 batch，其 RPS 達該曲線已量測最大值的至少 95%；若只量到 grid 上限或提前終止，標示「觀測範圍」。以下表格數字取自 [`RESULTS.md`](RESULTS.md)（完整各 batch 的數字見該檔與 `docs/07-03`、`docs/08-03`），可由 `scripts/summarize_sweeps.py` 在本機 artifacts 上重生；缺值標「沒量」。

### Fixed-connection 五條曲線：飽和點

| 曲線 | batch | RPS | p99 (s) | DB CPU (cores) | TiDB-server CPU (% cpuset) | flush await (ms) | 飽和標籤 |
|---|---:|---:|---:|---:|---:|---:|---|
| pg-c4-n16 | 8000 | 245.3k | 0.632 | 3.89 | 沒量 | 0.37 | observed-range |
| tidb-c4-n16 | 2000 | 128.3k | 0.366 | 3.17 | 87.9 | 0.42 | saturation point |
| pg-c8-n16 | 8000 | 357.2k | 0.430 | 6.53 | 沒量 | 0.36 | observed-range |
| tidb-c8x-n16 | 4000 | 152.8k | 0.583 | 4.90 | 81.6 | 0.46 | observed-range |
| tidb-c8x-n32 | 2000 | 147.3k | 0.633 | 4.89 | 83.9 | 0.45 | saturation point |

### TiKV 補測（所有量測點）

| 配置 | batch | RPS | p99 (s) | TiKV CPU (cores) | TiDB-server CPU (cores / % cpuset) | flush await (ms) | 飽和點 |
|---|---:|---:|---:|---:|---:|---:|---|
| k4t4 | 2000 | 127.6k | 0.355 | 3.15 | 3.46 / 86.5% | 0.43 | 是 |
| k4t4 | 4000 | 129.2k | 0.668 | 3.22 | 3.42 / 85.4% | 0.43 | |
| k4t4 | 8000 | 127.8k | 1.563 | 3.36 | 3.45 / 86.3% | 0.45 | |
| k6t8 | 2000 | 139.2k | 0.328 | 4.07 | 4.82 / 60.2% | 0.46 | |
| k6t8 | 4000 | 152.3k | 0.552 | 4.74 | 5.17 / 64.7% | 0.46 | 是 |
| k6t8 | 8000 | 151.5k | 1.075 | 4.81 | 5.43 / 67.9% | 0.47 | |

TiDB scaling 的觀測限於 16 connections：TiKV 4／tidb-server 4 → TiKV 8／tidb-server 6，RPS +9–19%；PG CPU4→CPU8，RPS +44–46%。這是測得的差異，不能視為因果結論。混雜因素：單一主機、SMT sibling 共用、共用單一磁碟。未量：TiKV per-thread CPU、window 內 per-stage latency、physical-core isolation。

## 限制與數字追溯

- 無 replication factor（no RF）；TiDB batch 8000 在 256 connections 下因記憶體限制不可行。
- 原始量測輸出（逐 250 ms samples、server logs、每次 run 的 summary）不納入版控；結果以 `docs/04`–`08` 的分析報告與 `RESULTS.md` 為準，重現需依上節步驟重跑。
- 開發用 throwaway credentials 列於 `SECURITY-NOTES.txt`，僅適用於本機短生命週期測試容器。
- 數字追溯：fixed sweep 在本機為 `artifacts/fixconn-*.csv` 加各列 `attempt_dir` 下的 `summary.json`；TiKV 補測為 `artifacts/fixconn-tidb-{k4t4,k6t8}-n16.csv`；batch／connection sweep 為 `artifacts/sweep-*.csv`、`artifacts/connsweep-*.csv`。這些檔案不在版控內；未量到的項目在報告中標「沒量」，不可推作零。
