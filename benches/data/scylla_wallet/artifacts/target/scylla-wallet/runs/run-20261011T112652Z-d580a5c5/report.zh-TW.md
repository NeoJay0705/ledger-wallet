# ScyllaDB Ledger Wallet Benchmark 執行報告

- Run：`run-20261011T112652Z-d580a5c5`
- 模式：`smoke`；工作負載分類：`diagnostic_smoke`
- 正式完整性：`diagnostic_only`（formal_complete=false）
- Trial：1/1；正式完整必須 27/27 通過必要 metrics 與 cleanup。
- Image：`scylladb/scylla@sha256:333b9ab3a3214c7f98af100711c51df7f58964e8227e6d26050e889ebb216df1`
- 預估磁碟需求含 replica、compaction、commitlog 與 16 GiB reserve：20614709248 bytes。
- 本機 profile 固定 developer-mode=1、io-setup=0、overprovisioned=1；共用 host/device 的結果不代表 production tuning。
- schema/system metadata bootstrap 與 system.clients audit 使用 CL=ONE；這些操作在量測窗外。Wallet seed/read/write/replay/verify 維持 CL=THREE。
- 同一 host 三節點不代表獨立故障域；closed-loop 有限負載不證明 steady-state capacity、hot-account 或長期 partition growth。
- 逐筆 fsync profile：unsupported；沒有以 periodic 或 batch-window sweep 代替。
- Commitlog counters 是 Scylla shard counters，不是 request 數；flush/bytes_written/cycle/alloc 的邊界 delta 與取樣偏移見 `counter-deltas.csv`。每 request 對應 1 partition mutation、3 application CQL statements（2 reads + 1 batch）。

| Trial | S | C | P=C/(3S) | 狀態 | 必要 metrics | Cleanup | RPS | p99 (ns) |
|---|---:|---:|---:|---|---|---|---:|---:|
| trial-001-S1-C12-rep1 | 1 | 12 | 4 | passed_relaxed | missing/invalid | clean | 2921.5586553406506 | 79101951 |

## Commitlog counter window deltas

以下數值按 Scylla node/shard 保留。起止值使用窗口邊界前最近 scrape；sampling offset 同時保存。缺值或 counter reset 保持 N/A。

| Trial | Node | Shard | Flush | Bytes written | Cycle | Alloc | Start offset (ns) | End offset (ns) |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| trial-001-S1-C12-rep1 | nodea | 0 | N/A | N/A | N/A | N/A | -2806805 | -687372900 |
| trial-001-S1-C12-rep1 | nodeb | 0 | 1.0 | 12288.0 | 2.0 | 12.0 | -94502182 | -651877397 |
| trial-001-S1-C12-rep1 | nodec | 0 | 11.0 | 69632.0 | 12.0 | 62.0 | -58481616 | -615321850 |

詳細 raw Prometheus、process/host counters、連線 audit、版本、source hash、container logs 與每 trial JSON 均保留在本 run 目錄。
只要 trial 缺失、required metric 缺漏、validation 失敗或 cleanup 不乾淨，就不會標記 formal_complete。
