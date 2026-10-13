# ScyllaDB Ledger Wallet Benchmark 執行報告

- Run：`run-20261012T071303Z-a7f4b0a6`
- 模式：`formal`；工作負載分類：`custom_workload`
- 正式完整性：`formal_partial`（formal_complete=false）
- Trial：1/1；正式完整必須 27/27 通過必要 metrics 與 cleanup。
- Image：`scylladb/scylla@sha256:333b9ab3a3214c7f98af100711c51df7f58964e8227e6d26050e889ebb216df1`
- 預估磁碟需求含 replica、compaction、commitlog 與 16 GiB reserve：82042421248 bytes。
- 本機 profile 固定 developer-mode=1、io-setup=0、overprovisioned=1；共用 host/device 的結果不代表 production tuning。
- schema/system metadata bootstrap 與 system.clients audit 使用 CL=ONE；這些操作在量測窗外。Wallet seed/read/write/replay/verify 維持 CL=THREE。
- 同一 host 三節點不代表獨立故障域；closed-loop 有限負載不證明 steady-state capacity、hot-account 或長期 partition growth。
- 逐筆 fsync profile：unsupported；沒有以 periodic 或 batch-window sweep 代替。
- Commitlog counters 是 Scylla shard counters，不是 request 數；flush/bytes_written/cycle/alloc 的邊界 delta 與取樣偏移見 `counter-deltas.csv`。每 request 對應 1 partition mutation、3 application CQL statements（2 reads + 1 batch）。

Formal preflight/執行未啟動或中止：trial trial-001-S4-C50000-P4-D48-per-user-rep1 ended status=failed; error=RunnerError: CLI exited with status 1

| Trial | S | C | P | D | Routing | 狀態 | 必要 metrics | Cleanup | RPS | p99 (ns) |
|---|---:|---:|---:|---:|---|---|---|---|---:|---:|
| trial-001-S4-C50000-P4-D48-per-user-rep1 | 4 | 50000 | 4 | 48 | per-user | failed | pass | clean | None | None |

## Commitlog counter window deltas

以下數值按 Scylla node/shard 保留。起止值使用窗口邊界前最近 scrape；sampling offset 同時保存。缺值或 counter reset 保持 N/A。

| Trial | Node | Shard | Flush | Bytes written | Cycle | Alloc | Start offset (ns) | End offset (ns) |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| trial-001-S4-C50000-P4-D48-per-user-rep1 | nodea | 0 | 9012.0 | 331415552.0 | 9067.0 | 480582.0 | -181050444 | -206815202 |
| trial-001-S4-C50000-P4-D48-per-user-rep1 | nodea | 1 | 8786.0 | 322408448.0 | 8842.0 | 466998.0 | -181050444 | -206815202 |
| trial-001-S4-C50000-P4-D48-per-user-rep1 | nodea | 2 | 9148.0 | 322473984.0 | 9202.0 | 465464.0 | -181050444 | -206815202 |
| trial-001-S4-C50000-P4-D48-per-user-rep1 | nodea | 3 | 8673.0 | 326201344.0 | 8730.0 | 473591.0 | -181050444 | -206815202 |
| trial-001-S4-C50000-P4-D48-per-user-rep1 | nodeb | 0 | 8418.0 | 329920512.0 | 8468.0 | 480584.0 | -124347434 | -152776825 |
| trial-001-S4-C50000-P4-D48-per-user-rep1 | nodeb | 1 | 9542.0 | 324362240.0 | 9601.0 | 466997.0 | -124347434 | -152776825 |
| trial-001-S4-C50000-P4-D48-per-user-rep1 | nodeb | 2 | 8314.0 | 320069632.0 | 8372.0 | 465462.0 | -124347434 | -152776825 |
| trial-001-S4-C50000-P4-D48-per-user-rep1 | nodeb | 3 | 9086.0 | 327307264.0 | 9131.0 | 473596.0 | -124347434 | -152776825 |
| trial-001-S4-C50000-P4-D48-per-user-rep1 | nodec | 0 | 8835.0 | 330858496.0 | 8881.0 | 480585.0 | -68883415 | -96686747 |
| trial-001-S4-C50000-P4-D48-per-user-rep1 | nodec | 1 | 9534.0 | 324182016.0 | 9593.0 | 466996.0 | -68883415 | -96686747 |
| trial-001-S4-C50000-P4-D48-per-user-rep1 | nodec | 2 | 10304.0 | 325304320.0 | 10363.0 | 465463.0 | -68883415 | -96686747 |
| trial-001-S4-C50000-P4-D48-per-user-rep1 | nodec | 3 | 9416.0 | 328110080.0 | 9465.0 | 473593.0 | -68883415 | -96686747 |

詳細 raw Prometheus、process/host counters、連線 audit、版本、source hash、container logs 與每 trial JSON 均保留在本 run 目錄。
只要 trial 缺失、required metric 缺漏、validation 失敗或 cleanup 不乾淨，就不會標記 formal_complete。
