# Tokio sequential WAL benchmark 實測報告

## 執行摘要

2026-09-26 執行 `cargo bench --bench wal_tokio_sequential` 的完整預設矩陣：6 種 batch／rotation 情境，各 3 輪，共 18 輪；每輪寫入 10,000,000 筆預先產生的 128-byte deterministic records。各輪都完成 frame、checksum、LSN／record 順序及內容 recovery 驗證，並清除自己的 WAL 目錄。

各組 durable RPS 中位數從 batch 64 的 54.0k records/s 上升至 batch 4,096 的約 1.19M records/s。durable latency 以每筆記錄所屬 batch 從 handoff 到 `sync_all` 成功的時間計算；batch 增大時吞吐提高，平均每筆所分配的 batch durability latency 也提高。量測中的 `tokio::fs` 操作仍依序等待，每次只有一個 batch 在途，因此這個 benchmark 沒有測試重疊寫入或並行 durability。與先前分開執行的同步 WAL benchmark 相比，各組 Tokio／sync 中位數比值落在約 0.98–1.04，沒有一致的 sequential throughput 優勢；不同執行時的磁碟負載和輪次離散度使此比較只能作描述，不能視為配對 A/B 結果。

## 環境與執行方式

- 日期：2026-09-26；optimized Cargo bench profile。
- 作業系統與核心：Ubuntu 24.04.2，Linux `6.17.0-35-generic`，x86_64。
- CPU：AMD Ryzen 7 3700X，8 個實體核心、16 個 logical CPUs。
- 記憶體：62 GiB；執行前 `MemAvailable` 約 40 GiB。
- WAL 裝置：`/dev/sdb2`，掛載於 `/`，`findmnt` 顯示 ext4。preflight CSV 的 `preflight_fs_type=ext2/ext3` 是 `stat -f` 回報值；掛載資訊顯示的 ext4 是此處採用的檔案系統描述。
- Rust：`rustc 1.98.1`。
- 執行命令：`cargo bench --bench wal_tokio_sequential`。沒有附加參數，保留預設 10M 筆、3 repetitions、全部 batch／rotation 組態及預設 preflight 門檻。

預設矩陣為 batch 64、256、1,024、2,048、4,096 搭配 1 GiB rotation；另加 batch 4,096 搭配 256 MiB rotation。每個 batch 順序完成 `write_all().await`、`flush().await`、`sync_all().await` 才開始下一個 batch。throughput 的量測窗從第一個 batch handoff 到最後一個 durable acknowledgement。

## Preflight

每輪執行前觀察 3 秒；預設 CPU busy 上限 10%、目標裝置 busy 上限 5%、`MemAvailable` 門檻 512 MiB，最多等待 60 秒。可用空間需足以容納估計 WAL bytes 加上 `max(25%, 1 GiB)` reserve。本次 20 次 preflight observation 中，18 次為 ready、2 次為 wait；等待發生於 batch 256／1 GiB／rep3（第一次裝置 busy 8.699%）和 batch 1,024／1 GiB／rep2（第一次 10.099%，重試 4.433%）。所有等待輪次都在後續 observation 通過預設門檻，沒有 timeout；沒有放寬門檻。

Preflight 只描述每輪開始前的 3 秒觀察。實際 WAL 量測期間，`/dev/sdb2` busy 約 58.7–92.7%；此值包含整個 block device 的活動，不能分辨 benchmark 寫入與其他 I/O 的個別貢獻。

## 完整矩陣

下表的 RPS、wall time、每筆平均 durable latency、CPU core equivalents 與量測期間 device busy，均先逐輪計算，再列出三輪中位數及最小–最大範圍。CPU core equivalents 是 benchmark process CPU seconds 除以相同 CPU 計量窗的 wall seconds，表示平均使用的核心數，不是主機 CPU 百分比。

| Batch / rotation | Durable RPS，M records/s | Wall，s | 每筆平均 durable latency，ms | CPU core equivalents | Device busy | Segments / 每輪 file syncs |
|---|---:|---:|---:|---:|---:|---:|
| 64 / 1 GiB | 0.054 [0.050–0.055] | 185.02 [181.13–199.02] | 1.184 [1.159–1.274] | 0.078 [0.074–0.080] | 92.1 [91.9–92.7]% | 2 / 156,250 |
| 256 / 1 GiB | 0.173 [0.125–0.201] | 57.95 [49.77–79.98] | 1.484 [1.274–2.047] | 0.112 [0.084–0.131] | 88.7 [87.0–91.8]% | 2 / 39,063 |
| 1,024 / 1 GiB | 0.613 [0.221–0.613] | 16.32 [16.32–45.18] | 1.671 [1.671–4.626] | 0.254 [0.095–0.254] | 75.3 [75.0–90.9]% | 2 / 9,766 |
| 2,048 / 1 GiB | 0.925 [0.456–0.927] | 10.81 [10.79–21.92] | 2.213 [2.209–4.490] | 0.344 [0.173–0.344] | 66.9 [66.7–83.5]% | 2 / 4,883 |
| 4,096 / 1 GiB | 1.194 [0.547–1.199] | 8.37 [8.34–18.28] | 3.429 [3.416–7.488] | 0.421 [0.196–0.422] | 59.0 [58.7–81.0]% | 2 / 2,442 |
| 4,096 / 256 MiB | 1.189 [0.261–1.191] | 8.41 [8.40–38.35] | 3.444 [3.438–15.708] | 0.422 [0.096–0.422] | 58.8 [58.7–91.0]% | 5 / 2,442 |

### 每筆 durable latency 分位數

`record_latency_mean_ns` 將每個 batch 從 handoff 到 durable acknowledgement 的時間，按該 batch 的 record 數加權；p50／p95／p99 則來自預先建立的 deterministic hash sample（預設 stride 1,024，nearest-rank）。因此這些數字表示「該筆所在 batch 的完成時間」，不是每筆寫入各自的獨立 I/O 延遲。表中的每個數字同樣是每輪分位數的 median [min–max]，單位為 ms。

| Batch / rotation | p50 | p95 | p99 |
|---|---:|---:|---:|
| 64 / 1 GiB | 1.055 [1.054–1.056] | 1.149 [1.146–1.151] | 1.264 [1.248–1.586] |
| 256 / 1 GiB | 1.257 [1.257–1.257] | 1.356 [1.353–1.359] | 1.517 [1.462–5.153] |
| 1,024 / 1 GiB | 1.626 [1.625–1.626] | 1.737 [1.731–1.772] | 2.099 [1.890–120.784] |
| 2,048 / 1 GiB | 2.144 [2.143–2.148] | 2.264 [2.264–3.708] | 3.328 [3.276–65.053] |
| 4,096 / 1 GiB | 3.302 [3.299–3.309] | 3.439 [3.434–56.511] | 10.647 [10.519–87.887] |
| 4,096 / 256 MiB | 3.309 [3.308–3.314] | 3.479 [3.467–129.213] | 10.863 [10.600–201.636] |

### 每輪 durable RPS

單位為 M records/s，保留各輪結果以呈現裝置活動與輪次差異。

| Batch / rotation | Rep 1 | Rep 2 | Rep 3 | Median [range] |
|---|---:|---:|---:|---:|
| 64 / 1 GiB | 0.054048 | 0.055208 | 0.050246 | 0.054 [0.050–0.055] |
| 256 / 1 GiB | 0.125026 | 0.200942 | 0.172550 | 0.173 [0.125–0.201] |
| 1,024 / 1 GiB | 0.612765 | 0.221343 | 0.612881 | 0.613 [0.221–0.613] |
| 2,048 / 1 GiB | 0.925370 | 0.456134 | 0.927009 | 0.925 [0.456–0.927] |
| 4,096 / 1 GiB | 1.194180 | 0.546931 | 1.198735 | 1.194 [0.547–1.199] |
| 4,096 / 256 MiB | 1.191022 | 1.189136 | 0.260751 | 1.189 [0.261–1.191] |

## I/O 階段耗時

下表列出每輪各階段「每次操作平均耗時」的 median [min–max]，單位為 ms。`write_all().await` 是該 awaited write_all 呼叫的耗時；呼叫完成本身不代表資料已持久化。接著的 `flush().await` 與 `sync_all().await` 分開計時；只有整個 batch 的 `sync_all` 成功後，writer 才回報 durable acknowledgement 並進入下一批。階段是各自計量的呼叫，不保證其平均值加總等於總 batch latency；frame encoding、rotation、目錄操作及 writer/runtime bookkeeping 也在端到端路徑中。

| Batch / rotation | Frame encode | `write_all().await` | `flush().await` | File `sync_all()` | Directory sync | Segment open | Rotation |
|---|---:|---:|---:|---:|---:|---:|---:|
| 64 / 1 GiB | 0.018 [0.018–0.018] | 0.002 [0.002–0.002] | 0.024 [0.024–0.024] | 1.140 [1.115–1.229] | 30.662 [1.022–60.747] | 0.057 [0.053–0.060] | 1.039 [1.021–1.057] |
| 256 / 1 GiB | 0.072 [0.072–0.072] | 0.004 [0.004–0.004] | 0.034 [0.034–0.034] | 1.372 [1.164–1.935] | 20.630 [1.206–45.381] | 0.073 [0.066–0.077] | 1.128 [1.047–1.136] |
| 1,024 / 1 GiB | 0.285 [0.285–0.285] | 0.010 [0.010–0.010] | 0.063 [0.063–0.063] | 1.313 [1.312–4.255] | 1.026 [1.024–57.486] | 0.060 [0.058–0.070] | 1.078 [1.063–1.112] |
| 2,048 / 1 GiB | 0.570 [0.569–0.571] | 0.020 [0.020–0.020] | 0.098 [0.098–0.100] | 1.524 [1.522–3.787] | 0.899 [0.850–26.610] | 0.054 [0.054–0.057] | 0.958 [0.897–0.966] |
| 4,096 / 1 GiB | 1.146 [1.144–1.149] | 0.038 [0.038–0.039] | 0.168 [0.167–0.171] | 2.075 [2.065–6.105] | 0.973 [0.961–27.845] | 0.056 [0.053–0.064] | 1.041 [1.034–1.073] |
| 4,096 / 256 MiB | 1.148 [1.147–1.152] | 0.038 [0.038–0.039] | 0.172 [0.170–0.172] | 2.084 [2.080–14.291] | 0.907 [0.895–24.514] | 0.059 [0.058–0.064] | 0.968 [0.941–0.985] |

File `sync_all` 每次操作 p99 的 median [min–max]：

| Batch / rotation | File sync p99，ms |
|---|---:|
| 64 / 1 GiB | 1.218 [1.216–1.513] |
| 256 / 1 GiB | 1.399 [1.355–2.292] |
| 1,024 / 1 GiB | 1.585 [1.508–120.133] |
| 2,048 / 1 GiB | 2.715 [2.656–60.663] |
| 4,096 / 1 GiB | 9.316 [9.247–83.483] |
| 4,096 / 256 MiB | 9.489 [9.392–196.921] |

階段 p50／p95／p99 的逐輪原始值均保留在 CSV。上表中 directory sync、segment open 與 rotation 的操作次數較少，特別是 directory sync 每輪只對新 segment 執行；其跨輪範圍不代表大量獨立樣本的穩定分布。

## 1 ms heartbeat

Heartbeat 與 writer 在 current-thread Tokio runtime 上執行。表中的 p50／p95／p99／max 是相對預定 tick 時間的 lateness，先在每輪對實際 poll 樣本取 nearest-rank，再列出三輪分位數的 median [min–max]；單位為 ms。`missed intervals` 是程式對每個觀察到的 tick，將 `floor(lateness / 1 ms)` 累加的數量；它表示量測窗內因遲到而跳過的完整 1 ms 時段，不是錯誤數。

| Batch / rotation | Poll ticks | Missed intervals | p50 | p95 | p99 | Max |
|---|---:|---:|---:|---:|---:|---:|
| 64 / 1 GiB | 151974 [132339–174801] | 29160 [24217–52682] | 0.724 [0.658–0.921] | 1.212 [1.126–1.481] | 1.287 [1.197–1.547] | 2.602 [2.364–3.686] |
| 256 / 1 GiB | 39836 [36016–74760] | 9929 [5223–21938] | 0.750 [0.531–1.084] | 1.211 [1.026–1.701] | 1.278 [1.066–1.759] | 2.572 [2.497–3.226] |
| 1,024 / 1 GiB | 15466 [15347–29250] | 969 [853–15928] | 0.423 [0.415–1.035] | 1.019 [1.008–1.517] | 1.081 [1.069–1.554] | 2.187 [2.166–3.029] |
| 2,048 / 1 GiB | 8254 [6172–15951] | 4615 [2552–5972] | 0.873 [0.825–1.296] | 1.399 [1.393–1.838] | 1.461 [1.450–1.888] | 3.105 [2.980–3.252] |
| 4,096 / 1 GiB | 4792 [4426–7961] | 3947 [3550–10322] | 1.208 [1.167–1.641] | 2.498 [2.317–2.933] | 2.598 [2.453–3.095] | 2.724 [2.644–3.238] |
| 4,096 / 256 MiB | 5209 [4706–21233] | 3690 [3200–17117] | 1.185 [1.107–1.260] | 2.340 [1.984–2.378] | 2.495 [2.091–2.730] | 2.997 [2.755–3.657] |

Heartbeat lateness includes Tokio timer granularity, runtime poll timing, and operating-system scheduling jitter. Tokio filesystem operations run on its blocking pool, which lets the runtime thread continue polling other tasks while those operations await completion; these samples show heartbeat activity during this workload, but they do not prove that event-loop behavior is independent of storage under all devices, loads, or external conditions. Lateness is not a direct measure of storage latency and cannot by itself identify the source of a delay. Larger batches had higher heartbeat p99 in this run, but the data do not attribute that increase to one cause.

## 與同步 WAL 結果對照

下表將本次 median durable RPS 與先前獨立執行的 [`wal_sync` 報告](wal_sync_report.md) 比較。比值是兩次 run 中位數的比，不是同一時段交錯或配對測試。

| Batch / rotation | Tokio median，M/s | Sync median，M/s | Tokio / sync |
|---|---:|---:|---:|
| 64 / 1 GiB | 0.054 | 0.052 | 1.036× |
| 256 / 1 GiB | 0.173 | 0.167 | 1.032× |
| 1,024 / 1 GiB | 0.613 | 0.603 | 1.016× |
| 2,048 / 1 GiB | 0.925 | 0.940 | 0.985× |
| 4,096 / 1 GiB | 1.194 | 1.214 | 0.983× |
| 4,096 / 256 MiB | 1.189 | 1.218 | 0.976× |

這些輪次在同一主機與目標裝置上分開執行，兩次測量之間的磁碟與主機負載會變化；本次量測期間裝置 busy 從約 58.7% 到 92.7%，也出現明顯 fsync 尾延遲。因此 1–4% 的 median 差異不能解讀為 Tokio 的因果效能增益或退化。對這個一批接一批等待 durability 的 sequential workload，Tokio filesystem API 讓 event loop 可在 blocking pool 處理檔案工作時繼續排程，但沒有帶來可從這兩次測量確認的穩定 throughput 提升。

## 離散輪次與儲存負載

四個低吞吐輪次伴隨較高的量測期間 device busy 及較長 file sync 尾延遲：

| 組態與輪次 | Durable RPS，M/s | Device busy | File sync 平均，ms | File sync p99，ms |
|---|---:|---:|---:|---:|
| Batch 1,024、1 GiB、rep2 | 0.221 | 90.9% | 4.255 | 120.133 |
| Batch 2,048、1 GiB、rep2 | 0.456 | 83.5% | 3.787 | 60.663 |
| Batch 4,096、1 GiB、rep2 | 0.547 | 81.0% | 6.105 | 83.483 |
| Batch 4,096、256 MiB、rep3 | 0.261 | 91.0% | 14.291 | 196.921 |

這些指標顯示 trial 之間的裝置活動和 sync 延遲不同，但不能單獨判斷是外部 I/O、裝置內部狀態或其他因素造成。Preflight 只有在每輪之前取樣，不能用來推定整個量測窗的背景負載。

## Recovery、清理與完整性

- 18 輪共處理 180,000,000 records、23,076,094,128 frame bytes、644,538 次 file `sync_all`，合計 45 個 segments。
- 每輪 recovery 的 frame bytes 都等於 writer 記錄的 `actual_frame_bytes`；record count 均為 10,000,000，所有 `truncated_tail_bytes` 均為 0。
- 每輪 `file_syncs == batches`；1 GiB threshold 每輪 2 個 segments，256 MiB threshold 每輪 5 個 segments；`directory_syncs == segments`。
- Recovery 後已刪除所有逐輪 WAL 目錄；`target/wal-tokio-benchmark/` 下沒有剩餘 trial directory。
- CSV 有 18 筆資料列、118 欄，所有資料列欄位數一致。

Recovery 在 throughput 計時窗外執行；這些 recovery 結果驗證正常結束後的資料讀回，不是實際斷電測試。

## 限制

- 此矩陣量測單一 process、單一順序 writer、同時最多一批在途的本機 WAL；不代表多 writer、服務併發、外部到達率或使用者 SLO。
- `tokio::fs` 的一般檔案工作由 blocking pool 執行。Heartbeat 可與該 pool 的工作排程，但其 lateness 仍受計時器粒度和作業系統排程影響；本測量無法證明所有外部條件下都不受 storage 影響。
- 完成 `sync_all` 並在同一主機上 recovery 成功，沒有實際拔電或 power-cycle；因此不能單靠本 benchmark 證明裝置在真實斷電情況下的持久性。
- 同步 WAL 與 Tokio WAL 是不同時間的 runs；device busy、fsync 尾端及每輪差異都會影響結果。比值僅作背景參考。

## 原始資料與設計

- [逐輪結果 CSV](wal_tokio_sequential_results.csv)
- [Preflight 與執行記錄（stderr）](wal_tokio_sequential_preflight.log)
- [Tokio sequential WAL benchmark 設計](../docs/01-10.development-design-wal-tokio-sequential-benchmark.md)
- [同步 WAL 實測結果與限制](wal_sync_report.md)
