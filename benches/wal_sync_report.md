# 同步 WAL benchmark 實測報告

## 執行摘要

本次在 2026-09-26 執行 `cargo bench --bench wal_sync`，使用 optimized bench profile。每輪寫入 10,000,000 筆預先產生的 128-byte 記錄，測試 6 種 batch／rotation 組態，每種重複 3 輪，共 18 輪。量測涵蓋單一同步 writer 將完整 batch 寫入 WAL segment、呼叫 `File::sync_all`，以及新 segment 首次使用前的 directory sync。

在 1 GiB rotation threshold 下，batch 從 64 增至 4,096 時，durable RPS 的輪次中位數由 0.052 M records/s 增至 1.214 M records/s（約 23 倍）；每筆平均 durable latency 的輪次中位數由 1.227 ms 增至 3.373 ms（約 2.75 倍），CPU core equivalents 由 0.053 增至 0.409。每輪 payload 固定約 1.28 GB；每輪 file sync 次數由 156,250 降至 2,442。多個組態有明顯的輪次差異，詳見下方逐輪結果與限制。

## 環境與執行方式

- 日期：2026-09-26。
- 作業系統與核心：Ubuntu 24.04.2，Linux `6.17.0-35-generic`。
- Rust／Cargo：`1.98.1`。
- WAL 目標裝置與檔案系統：`/dev/sdb2`，ext4；主機有 16 個 logical CPUs。測試前可用磁碟空間約 90 GiB，`MemAvailable` 約 40 GiB。
- 命令：`cargo bench --bench wal_sync`，使用 optimized bench profile 與程式的預設完整矩陣。

CSV 的 `preflight_fs_type` 欄位由 `stat -f` 取得，標示為 `ext2/ext3`；掛載資訊 `findmnt` 識別的檔案系統是 ext4。檔案系統描述以掛載資訊為準。

每輪開始前，preflight 觀察 3 秒，CPU busy 上限為 10%、目標磁碟 busy 上限為 5%，最多等候 60 秒。可用空間需大於預估 WAL bytes 加上 `max(25%, 1 GiB)` reserve；預先配置 payload 後的 `MemAvailable` 門檻為 512 MiB。本次共有 21 筆 preflight observation：18 筆為 ready、3 筆為 wait。3 筆 wait 分屬兩個 trial：b64 rep3 一次、b256 rep1 兩次；這兩個 trial 後續都取得 ready，沒有 timeout。Preflight 僅在每輪開始前取樣，不能代表量測期間的系統狀態。

## 完整矩陣

下表的 RPS 中位數與範圍以三輪各自的 durable RPS 計算；其他欄位是三輪該項統計值的中位數。Latency p95/p99 是先分別取每輪的 sampled nearest-rank quantile，再取三輪 quantile 的中位數，並非將所有樣本合併後重算。表內時間單位皆為 ms。

| Batch | Rotation | Durable RPS 中位數（M records/s）[範圍] | 每筆平均 latency 中位數 | 每筆 p95 中位數 | 每筆 p99 中位數 | File fsync 平均值中位數 | File fsync p99 中位數 | Batch write 平均值中位數 | CPU core equivalents 中位數 | Segments |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 64 | 1 GiB | 0.052 [0.051–0.061] | 1.227 | 1.123 | 1.227 | 1.195 | 1.200 | 0.012 | 0.053 | 2 |
| 256 | 1 GiB | 0.167 [0.135–0.205] | 1.531 | 1.331 | 1.999 | 1.435 | 1.914 | 0.023 | 0.087 | 2 |
| 1,024 | 1 GiB | 0.603 [0.218–0.624] | 1.698 | 1.709 | 2.814 | 1.357 | 1.622 | 0.054 | 0.227 | 2 |
| 2,048 | 1 GiB | 0.940 [0.443–0.944] | 2.179 | 2.241 | 4.057 | 1.519 | 2.653 | 0.084 | 0.327 | 2 |
| 4,096 | 1 GiB | 1.214 [0.542–1.216] | 3.373 | 3.393 | 10.646 | 2.060 | 9.449 | 0.152 | 0.409 | 2 |
| 4,096 | 256 MiB | 1.218 [0.265–1.219] | 3.361 | 3.407 | 10.637 | 2.052 | 9.352 | 0.150 | 0.409 | 5 |

### 每輪 durable RPS

單位為 M records/s。這些值保留輪次差異，供對照上表的中位數與範圍。

| Batch | Rotation | Rep 1 | Rep 2 | Rep 3 |
|---:|---:|---:|---:|---:|
| 64 | 1 GiB | 0.060743 | 0.050619 | 0.052166 |
| 256 | 1 GiB | 0.135140 | 0.205386 | 0.167155 |
| 1,024 | 1 GiB | 0.602894 | 0.218000 | 0.623507 |
| 2,048 | 1 GiB | 0.944452 | 0.443214 | 0.939588 |
| 4,096 | 1 GiB | 1.216356 | 0.541917 | 1.214231 |
| 4,096 | 256 MiB | 1.218640 | 1.218260 | 0.265103 |

Batch 1,024 rep 2、batch 2,048 rep 2、batch 4,096／1 GiB rep 2，以及 batch 4,096／256 MiB rep 3 的 wall RPS 明顯低於同組另外兩輪。這四輪的 file fsync 平均值與量測期間目標裝置 busy 比例如下：

| 組態與輪次 | Durable RPS（M records/s） | 每筆 sampled p99（ms） | File fsync 平均值 | 量測期間 `/dev/sdb2` busy |
|---|---:|---:|---:|---:|
| Batch 1,024、rep 2 | 0.218000 | 119.93 | 4.35 ms | 91.9% |
| Batch 2,048、rep 2 | 0.443214 | 65.91 | 3.95 ms | 85.2% |
| Batch 4,096、1 GiB、rep 2 | 0.541917 | 87.01 | 6.21 ms | 81.8% |
| Batch 4,096、256 MiB、rep 3 | 0.265103 | 186.95 | 14.08 ms | 91.1% |

Batch 4,096 的其他輪次約為 59.5–59.6% device busy、約 2.05 ms 平均 file fsync。Batch 64 的三輪約為 93.5–94.6% busy；該組自身 workload 已使裝置 busy 比例偏高。由於 preflight 只在各輪之前取樣，無法僅由這些資料把輪次差異歸因於背景 I/O 或單一原因。

Batch 4,096 下，1 GiB threshold 中位數為 1.214 M records/s、2 個 segments；256 MiB threshold 中位數為 1.218 M records/s、5 個 segments。兩組都有大幅輪次差異，因此這組測量不能判定 rotation 成本可忽略。

## 工作量、驗證與清理

每輪寫入 10,000,000 筆、每筆 128 bytes 的預先產生記錄；同一份 payload 在各情境重用，不計入 WAL latency。每個 batch 完整寫入單一 segment 後呼叫一次 file `sync_all`，每個新 segment 在第一個 durable batch 前同步目錄。各 batch size 每輪的 batch／file sync 次數為：64 筆 156,250 次、256 筆 39,063 次、1,024 筆 9,766 次、2,048 筆 4,883 次、4,096 筆 2,442 次。新 segment 數也列於結果表。

18 輪合計處理 180,000,000 筆記錄、寫入 23,076,094,128 frame bytes，執行 644,538 次 file fsync。每輪 recovery 都驗證出完整預期記錄與 frame bytes；18 輪的 `truncated_tail_bytes` 皆為 0。每輪完成 recovery 後都刪除該輪 WAL 目錄；`target/wal-sync-benchmark/` 最後為空。

CSV 中各輪的 WAL measured wall time 合計 947.724 秒，recovery time 合計 53.745 秒。完整 artifact timestamp interval 為 17 分 49.389 秒；此區間不是精確的 benchmark process runtime。

## 指標定義與限制

- Durable RPS 是 records 除以從第一個完整預先產生 batch handoff 到最後一筆 durability acknowledgement 的 wall time。上游收集與組成 batch 的時間不包含在內。
- 每筆記錄 latency 從完整 batch handoff 開始；同一 batch 內所有 records 共用該 batch 的 latency。平均值涵蓋該輪 records；p50/p95/p99 使用每輪 9,893 筆 deterministic sample，依 nearest-rank 計算。
- CPU 使用 `ProcessTime` CPU time，並以另外量測且對齊的 CPU wall time 算 core equivalents；該計量包含 benchmark bookkeeping。它不等同主機整體 CPU busy。
- Batch write、file sync、directory sync、segment open 與 rotation 的計時事件會依各自定義重疊；各階段 quantile 不應相加成總 latency。Recovery 與清理均在 WAL throughput／latency 量測之外。
- 這是單一同步 writer 的獨立 benchmark，並非正式 ledger 寫入路徑；不包含 Tokio、queue、gRPC 或餘額更新。Recovery 邏輯有模擬 torn-tail 與 checksum corruption 的測試，但本次沒有實際斷電或 power-cycle，也未驗證特定硬體在斷電時對 `sync_all` 的實際保留行為。

## 原始資料與設計

- [逐輪原始 CSV](wal_sync_results.csv)
- [Preflight 執行記錄](wal_sync_preflight.log)
- [同步 WAL benchmark 設計文件](../docs/01-09.development-design-sync-wal-benchmark.md)
