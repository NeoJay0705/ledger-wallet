# ScyllaDB Ledger Wallet Benchmark 執行報告

- Run：`run-20261012T110929Z-8db6ac1b`
- 模式：`smoke`；工作負載分類：`diagnostic_smoke`
- 正式完整性：`diagnostic_only`（formal_complete=false）
- Trial：1/1；正式完整必須 27/27 通過必要 metrics 與 cleanup。
- Image：`scylladb/scylla@sha256:333b9ab3a3214c7f98af100711c51df7f58964e8227e6d26050e889ebb216df1`
- Wallet implementation：`wallet-memory-lwt-v1`；state source=`memory`，ledger key=`account_id,kind,tx_id`，serial CL=`LOCAL_SERIAL`。
- Durability profile：RF=1，wallet read CL=ONE，write CL=ONE。
- 預估磁碟需求按 RF=1 replica，另含 compaction、3-node commitlog 與 16 GiB reserve：20628635648 bytes。
- 本機 profile 固定 developer-mode=1、io-setup=0、overprovisioned=1；共用 host/device 的結果不代表 production tuning。
- schema/system metadata bootstrap 與 system.clients audit 使用 CL=ONE；這些操作在量測窗外。Wallet seed/read/write/replay/verify 使用本 trial 設定的 read=ONE、write=ONE。
- 同一 host 三節點不代表獨立故障域；closed-loop 有限負載不證明 steady-state capacity、hot-account 或長期 partition growth。
- 逐筆 fsync profile：unsupported；沒有以 periodic 或 batch-window sweep 代替。
- Commitlog counters 是 Scylla shard counters，不是 request 數；flush/bytes_written/cycle/alloc 的邊界 delta 與取樣偏移見 `counter-deltas.csv`。每個 accepted transaction 在量測窗使用 1 個 LWT CQL request/statement，沒有量測窗內 SELECT；成功的新交易對應 1 個 wallet partition mutation。Paxos 可能產生額外內部儲存與 commitlog 活動；`applied=false` 不代表沒有 fsync。

| Trial | Wallet profile | S | C | P | D | RF | Read CL | Write CL | Routing | 狀態 | 必要 metrics | Cleanup | RPS | p99 (ns) |
|---|---|---:|---:|---:|---:|---:|---|---|---|---|---|---|---:|---:|
| trial-001-S4-C64-P4-D48-per-user-RF1-R1-W1-wallet-memory-lwt-v1-rep1 | wallet-memory-lwt-v1 | 4 | 64 | 4 | 48 | 1 | ONE | ONE | per-user | passed | pass | clean | 2850.067650719856 | 39911423 |

## Commitlog counter window deltas

以下數值按 Scylla node/shard 保留。起止值使用窗口邊界前最近 scrape；sampling offset 同時保存。缺值或 counter reset 保持 N/A。

| Trial | Node | Shard | Flush | Bytes written | Cycle | Alloc | Start offset (ns) | End offset (ns) |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| trial-001-S4-C64-P4-D48-per-user-RF1-R1-W1-wallet-memory-lwt-v1-rep1 | nodea | 0 | 1417.0 | 5808128.0 | 1418.0 | 3545.0 | -176889263 | -479915205 |
| trial-001-S4-C64-P4-D48-per-user-RF1-R1-W1-wallet-memory-lwt-v1-rep1 | nodea | 1 | 1419.0 | 7270400.0 | 1420.0 | 5323.0 | -176889263 | -479915205 |
| trial-001-S4-C64-P4-D48-per-user-RF1-R1-W1-wallet-memory-lwt-v1-rep1 | nodea | 2 | 1415.0 | 5799936.0 | 1416.0 | 3538.0 | -176889263 | -479915205 |
| trial-001-S4-C64-P4-D48-per-user-RF1-R1-W1-wallet-memory-lwt-v1-rep1 | nodea | 3 | 1419.0 | 7995392.0 | 1420.0 | 6210.0 | -176889263 | -479915205 |
| trial-001-S4-C64-P4-D48-per-user-RF1-R1-W1-wallet-memory-lwt-v1-rep1 | nodeb | 0 | 1441.0 | 7376896.0 | 1441.0 | 5407.0 | -121764764 | -421334534 |
| trial-001-S4-C64-P4-D48-per-user-RF1-R1-W1-wallet-memory-lwt-v1-rep1 | nodeb | 1 | 1442.0 | 5910528.0 | 1443.0 | 2706.0 | -121764764 | -421334534 |
| trial-001-S4-C64-P4-D48-per-user-RF1-R1-W1-wallet-memory-lwt-v1-rep1 | nodeb | 2 | 1441.0 | 7380992.0 | 1442.0 | 4506.0 | -121764764 | -421334534 |
| trial-001-S4-C64-P4-D48-per-user-RF1-R1-W1-wallet-memory-lwt-v1-rep1 | nodeb | 3 | 1441.0 | 8118272.0 | 1442.0 | 6308.0 | -121764764 | -421334534 |
| trial-001-S4-C64-P4-D48-per-user-RF1-R1-W1-wallet-memory-lwt-v1-rep1 | nodec | 0 | 1464.0 | 6000640.0 | 1465.0 | 3664.0 | -66110186 | -357566865 |
| trial-001-S4-C64-P4-D48-per-user-RF1-R1-W1-wallet-memory-lwt-v1-rep1 | nodec | 1 | 1461.0 | 10473472.0 | 1462.0 | 8218.0 | -66110186 | -357566865 |
| trial-001-S4-C64-P4-D48-per-user-RF1-R1-W1-wallet-memory-lwt-v1-rep1 | nodec | 2 | 1464.0 | 5996544.0 | 1464.0 | 3663.0 | -66110186 | -357566865 |
| trial-001-S4-C64-P4-D48-per-user-RF1-R1-W1-wallet-memory-lwt-v1-rep1 | nodec | 3 | 1461.0 | 7483392.0 | 1462.0 | 4566.0 | -66110186 | -357566865 |

詳細 raw Prometheus、process/host counters、連線 audit、版本、source hash、container logs 與每 trial JSON 均保留在本 run 目錄。
只要 trial 缺失、required metric 缺漏、validation 失敗或 cleanup 不乾淨，就不會標記 formal_complete。
