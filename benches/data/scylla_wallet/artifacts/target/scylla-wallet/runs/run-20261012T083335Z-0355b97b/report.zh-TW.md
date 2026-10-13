# ScyllaDB Ledger Wallet Benchmark 執行報告

- Run：`run-20261012T083335Z-0355b97b`
- 模式：`formal`；工作負載分類：`custom_workload`
- 正式完整性：`formal_partial`（formal_complete=false）
- Trial：1/1；正式完整必須 27/27 通過必要 metrics 與 cleanup。
- Image：`scylladb/scylla@sha256:333b9ab3a3214c7f98af100711c51df7f58964e8227e6d26050e889ebb216df1`
- Durability profile：RF=1，wallet read CL=ONE，write CL=ONE。
- 預估磁碟需求按 RF=1 replica，另含 compaction、3-node commitlog 與 16 GiB reserve：41082421248 bytes。
- 本機 profile 固定 developer-mode=1、io-setup=0、overprovisioned=1；共用 host/device 的結果不代表 production tuning。
- schema/system metadata bootstrap 與 system.clients audit 使用 CL=ONE；這些操作在量測窗外。Wallet seed/read/write/replay/verify 使用本 trial 設定的 read=ONE、write=ONE。
- 同一 host 三節點不代表獨立故障域；closed-loop 有限負載不證明 steady-state capacity、hot-account 或長期 partition growth。
- 逐筆 fsync profile：unsupported；沒有以 periodic 或 batch-window sweep 代替。
- Commitlog counters 是 Scylla shard counters，不是 request 數；flush/bytes_written/cycle/alloc 的邊界 delta 與取樣偏移見 `counter-deltas.csv`。每 request 對應 1 partition mutation、3 application CQL statements（2 reads + 1 batch）。

Formal preflight/執行未啟動或中止：trial trial-001-S4-C50000-P4-D48-per-user-RF1-R1-W1-rep1 ended status=failed; error=RunnerError: CLI exited with status 1

| Trial | S | C | P | D | RF | Read CL | Write CL | Routing | 狀態 | 必要 metrics | Cleanup | RPS | p99 (ns) |
|---|---:|---:|---:|---:|---:|---|---|---|---|---|---|---:|---:|
| trial-001-S4-C50000-P4-D48-per-user-RF1-R1-W1-rep1 | 4 | 50000 | 4 | 48 | 1 | ONE | ONE | per-user | failed | pass | clean | None | None |

## Commitlog counter window deltas

以下數值按 Scylla node/shard 保留。起止值使用窗口邊界前最近 scrape；sampling offset 同時保存。缺值或 counter reset 保持 N/A。

| Trial | Node | Shard | Flush | Bytes written | Cycle | Alloc | Start offset (ns) | End offset (ns) |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| trial-001-S4-C50000-P4-D48-per-user-RF1-R1-W1-rep1 | nodea | 0 | 39.0 | 4001792.0 | 40.0 | 6156.0 | -170202696 | -971260102 |
| trial-001-S4-C50000-P4-D48-per-user-RF1-R1-W1-rep1 | nodea | 1 | 40.0 | 4050944.0 | 40.0 | 6225.0 | -170202696 | -971260102 |
| trial-001-S4-C50000-P4-D48-per-user-RF1-R1-W1-rep1 | nodea | 2 | 38.0 | 4153344.0 | 38.0 | 6396.0 | -170202696 | -971260102 |
| trial-001-S4-C50000-P4-D48-per-user-RF1-R1-W1-rep1 | nodea | 3 | 39.0 | 4141056.0 | 39.0 | 6381.0 | -170202696 | -971260102 |
| trial-001-S4-C50000-P4-D48-per-user-RF1-R1-W1-rep1 | nodeb | 0 | 38.0 | 4124672.0 | 40.0 | 6345.0 | -113612866 | -919277950 |
| trial-001-S4-C50000-P4-D48-per-user-RF1-R1-W1-rep1 | nodeb | 1 | 41.0 | 4116480.0 | 43.0 | 6315.0 | -113612866 | -919277950 |
| trial-001-S4-C50000-P4-D48-per-user-RF1-R1-W1-rep1 | nodeb | 2 | 40.0 | 4284416.0 | 42.0 | 6589.0 | -113612866 | -919277950 |
| trial-001-S4-C50000-P4-D48-per-user-RF1-R1-W1-rep1 | nodeb | 3 | 34.0 | 4395008.0 | 39.0 | 6779.0 | -113612866 | -919277950 |
| trial-001-S4-C50000-P4-D48-per-user-RF1-R1-W1-rep1 | nodec | 0 | 39.0 | 4308992.0 | 41.0 | 6640.0 | -56596753 | -865003444 |
| trial-001-S4-C50000-P4-D48-per-user-RF1-R1-W1-rep1 | nodec | 1 | 40.0 | 4349952.0 | 42.0 | 6694.0 | -56596753 | -865003444 |
| trial-001-S4-C50000-P4-D48-per-user-RF1-R1-W1-rep1 | nodec | 2 | 41.0 | 4419584.0 | 43.0 | 6801.0 | -56596753 | -865003444 |
| trial-001-S4-C50000-P4-D48-per-user-RF1-R1-W1-rep1 | nodec | 3 | 43.0 | 4464640.0 | 44.0 | 6867.0 | -56596753 | -865003444 |

詳細 raw Prometheus、process/host counters、連線 audit、版本、source hash、container logs 與每 trial JSON 均保留在本 run 目錄。
只要 trial 缺失、required metric 缺漏、validation 失敗或 cleanup 不乾淨，就不會標記 formal_complete。
