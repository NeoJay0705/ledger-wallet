# ScyllaDB Ledger Wallet Benchmark 執行報告

- Run：`run-20261013T015038Z-0010535e`
- 模式：`pilot`；工作負載分類：`diagnostic_pilot`
- 正式完整性：`diagnostic_only`（formal_complete=false）
- Nodes：1 (nodea)；Trial：1/1；正式完整必須 27/27 通過必要 metrics 與 cleanup。
- Image：`scylladb/scylla@sha256:333b9ab3a3214c7f98af100711c51df7f58964e8227e6d26050e889ebb216df1`
- Wallet implementation：`wallet-memory-lwt-v1`；state source=`memory`，ledger key=`account_id,kind,tx_id`，serial CL=`LOCAL_SERIAL`。
- Durability profile：RF=1，wallet read CL=ONE，write CL=ONE；commitlog_sync=periodic，period=10000 ms，commitlog_use_o_dsync=false，total=1024 MiB，segment=64 MiB。
- 預估磁碟需求按 RF=1 replica，另含 compaction、1-node commitlog 與 16 GiB reserve：20417871872 bytes。
- 本機 profile 固定 developer-mode=1、io-setup=0、overprovisioned=1；共用 host/device 的結果不代表 production tuning。
- schema/system metadata bootstrap 與 system.clients audit 使用 CL=ONE；這些操作在量測窗外。Wallet seed/read/write/replay/verify 使用本 trial 設定的 read=ONE、write=ONE。
- 單節點配置沒有副本冗餘；closed-loop 有限負載不證明 steady-state capacity、hot-account 或長期 partition growth。
- I/O properties profile：enabled；SHA-256=12bf0b79c4520ca6d00183ca14b87d321b9221b687cc6e889619876f5b87b2bf。啟用時需通過唯讀掛載、執行中 argv、data/commitlog device 與每 shard queue mountpoint 驗證；空 mountpoint fallback 另存，不代表其他 device I/O 受控。
- Periodic 10000 ms 為獨立 pilot profile；immutable pinned source 與 Scylla 2026.1 LWT 說明已核對：LWT force_sync 路徑在所有 sync mode 都會同步到裝置。這不表示每筆交易恰好一次物理 fsync，也不排除 coalescing；普通 periodic writes 仍不以同步 ack 解讀。
- Commitlog counters 是 Scylla shard counters，不是 request 數；flush/bytes_written/cycle/alloc 的邊界 delta 與取樣偏移見 `counter-deltas.csv`。每個 accepted transaction 在量測窗使用 1 個 LWT CQL request/statement，沒有量測窗內 SELECT；成功的新交易對應 1 個 wallet partition mutation。Paxos 可能產生額外內部儲存與 commitlog 活動；`applied=false` 不代表沒有 fsync。

Formal preflight/執行未啟動或中止：trial trial-001-S2-C512-P3-D6-round-robin-RF1-R1-W1-N1-wallet-memory-lwt-v1-rep1-commitlog-periodic-10000ms ended status=failed; error=RunnerError: effective Scylla commitlog config could not be validated: nodea: commitlog_sync periodic profile was not verified through system.config

| Trial | Nodes | Wallet profile | Sync | Period ms | S | C | P | D | RF | Read CL | Write CL | Routing | 狀態 | 必要 metrics | Cleanup | RPS | p99 (ns) |
|---|---:|---|---|---:|---:|---:|---:|---:|---:|---|---|---|---|---|---|---:|---:|
| trial-001-S2-C512-P3-D6-round-robin-RF1-R1-W1-N1-wallet-memory-lwt-v1-rep1-commitlog-periodic-10000ms | 1 | wallet-memory-lwt-v1 | periodic | 10000 | 2 | 512 | 3 | 6 | 1 | ONE | ONE | round-robin | failed | missing/invalid | clean | None | None |

## Commitlog counter window deltas

以下數值按 Scylla node/shard 保留。起止值使用窗口邊界前最近 scrape；sampling offset 同時保存。缺值或 counter reset 保持 N/A。

| Trial | Node | Shard | Flush | Bytes written | Cycle | Alloc | Start offset (ns) | End offset (ns) |
|---|---|---:|---:|---:|---:|---:|---:|---:|

詳細 raw Prometheus、process/host counters、連線 audit、版本、source hash、container logs 與每 trial JSON 均保留在本 run 目錄。
只要 trial 缺失、required metric 缺漏、validation 失敗或 cleanup 不乾淨，就不會標記 formal_complete。
