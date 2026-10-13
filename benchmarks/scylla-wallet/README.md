# ScyllaDB Ledger Wallet Bench

本 crate 是獨立的 benchmark 程式，不依賴 RocksDB 專案的執行路徑。服務與 load clients 共用 Rust process；Python monitors 在另一個 process 記錄 CPU、device 與 Scylla metrics。Tokio runtime 使用四個 worker threads。服務沒有 HTTP endpoint。

## 目前文件與狀態

- [記憶體狀態與單 CQL LWT 需求](../../docs/01-42.raw-requirement-scylladb-memory-lwt-benchmark.md)
- [記憶體狀態與單 CQL LWT 設計](../../docs/01-43.development-design-scylladb-memory-lwt-benchmark.md)
- [驗證紀錄](../../docs/01-44.development-validation-scylladb-memory-lwt-benchmark.md)

ScyllaDB profile 是 `wallet-memory-lwt-v1`；PostgreSQL profile 是 `postgres-memory-tx-v1`。兩者共用同一個 `Service`、cache 計算、load clients 與完整驗證流程，分別由 `ScyllaStore` 和 `PostgresStore` 實作 `WalletStore`。本 README 不把歷史三 CQL 結果當成新 profile 的驗證或性能結果。

## 等資源同步持久化比較

ScyllaDB 與 PostgreSQL 比較由本 crate 提供共同 Wallet workload；執行條件與 qualification 見 [01-63 至 01-65](../../docs/01-63.raw-requirement-wallet-db-equal-resource-benchmark.md)、[正式比較結果 01-66](../../docs/01-66.development-analysis-wallet-db-equal-resource-benchmark.md)。Scylla 歷史 raw evidence、Git 小檔和摘要的查找入口為 [證據包索引](../../benches/data/scylla_wallet/README.md)；文件包裝與還原規則見 [01-67](../../docs/01-67.development-design-benchmark-evidence-packaging.md)。

## PostgreSQL comparison profile

PostgreSQL profile 固定 `C=512` service workers，`--pg-connections D` 設定總資料連線數；seed、preload 與完整驗證固定最多六個平行工作。`--db-cpus` 記錄 DB CPU 數，預設為 2。指定新建的 `wallet_bench_*` schema；CLI 只接受 `walletbench` database，拒絕既有 schema，並為每條 pool connection 設定相同的 `application_name=ledger-wallet-postgres-wallet-bench`。

每條連線都必須回報 `synchronous_commit=on`、`fsync=on`、`full_page_writes=on`、`commit_delay=0`、primary role 與空的 `synchronous_standby_names`，否則不啟動 workload。Result 不包含 DSN。Owned runner 使用本機 trust 連線，並另外驗證 PostgreSQL server image 與其餘 durability 設定。

資料表與 `wallet_commit` function 的完整 SQL 位於 [`sql/postgres_wallet.sql`](sql/postgres_wallet.sql)，其 SHA-256 會同時寫入 ready event 與 result metadata。`balance`、`amount` 和 `balance_after` 都保留 8-byte big-endian `BYTEA`；account ID、transaction ID、version 和 logical clock 使用非負 `BIGINT` 範圍。每筆新 request 呼叫一次 prepared `SELECT wallet_commit(...)`，由 PostgreSQL implicit transaction 原子執行 ledger INSERT 與 account UPDATE。Account UPDATE 必須正好影響一列，否則 function raise error 並回滾同一交易。Duplicate request 讀取目前 account state 與原始完整 ledger record，不更新 account；`Service` 依 request body 區分 replay 與 conflict。

本機離線測試不連線到資料庫。Owned PostgreSQL integration test 預設 ignored，會建立並在結束時刪除唯一的測試 schema：

~~~bash
LEDGER_WALLET_PG_DSN='postgresql:///walletbench?host=/var/run/postgresql' \
  CARGO_TARGET_DIR=target/scylla-wallet \
  cargo test --locked --manifest-path benchmarks/scylla-wallet/Cargo.toml --test postgres_integration -- --ignored
~~~

正式比較時由 runner 啟動新編譯的 bench binary，傳入 `--backend postgres --pg-dsn "$LEDGER_WALLET_PG_DSN" --pg-schema wallet_bench_<case> --pg-connections D --db-cpus 2`。PG result 會分開列出 pool wait、SQL transaction duration、client query delta、function/ledger/account operation counters、實際 pool liveness 與完整 account/ledger verification；不輸出 Scylla-only CQL 或 Paxos counters。

## 目前服務與資料模型

ScyllaDB 保存 durable balance、ledger 與 dedupe 結果。Service 在 seed 後、measurement gate 前預載帳戶狀態，measurement 路徑使用記憶體 AccountState 計算交易。只有收到 LWT acknowledgement 後，Service 才發布新的 cache state。

Request 欄位為 `account_id`、`tx_id`、`operation` 與 `amount`；不含 `transaction_at`。Ledger key 是 `['account_id','kind','tx_id']`；`kind=0` 是帳戶 metadata sentinel，`kind=1` 是交易。`balance`、`version`、`write_clock` 是 static 欄位。Seed clock 為 0；每筆新交易（包括業務拒絕）以 checked arithmetic 將 clock 加 1。Logical clock 獨立於 wall clock 和資料庫時間，不是 CQL timestamp。

結果 metadata 固定包含 `wallet_profile=wallet-memory-lwt-v1`、`account_state_source=memory`、`ledger_key=["account_id","kind","tx_id"]`、`serial_cl=LOCAL_SERIAL`、`cql_requests_per_transaction=1` 及 `cql_statement_count=1`。新 profile 的 reports 與 case IDs 和歷史三 CQL profile 分組。

每個帳戶只有一個 owner。它從 cache 計算交易開始，持續到 conditional write 結果處理、cache publish 與 reply。相同 key/body 的 replay 回傳原 ledger 結果，不重寫 wallet row，也不前進或回退 cache；Paxos 內部仍可能寫入或 fsync。相同 key 配不同 body 回 Conflict。不同帳戶可並行。

每個新交易只送一個 `INSERT ... IF NOT EXISTS`，在同一 partition 寫入 static state 與 ledger row。Seed 也使用 LWT。設定 `LOCAL_SERIAL`；一般 read/write CL 分開設定。False LWT response 必須帶回符合目前 cache 的 static state，再用既有 ledger body 判斷 replay 或 conflict。LWT 條件檢查目標 ledger row；Paxos 在 partition 範圍協調，不驗證另一個 `tx_id` 所用 cache 是否最新。因此仍須維持單一 service、帳戶 owner 與禁止外部 writer。

LWT 包含 Paxos 內部工作。`cql_requests_per_transaction=1` 表示一個 client CQL request，不代表一次資料庫內部操作或一次 fsync。此 profile 不使用一般 CQL BATCH；`commitlog_sync=batch` 與 fsync 後 ack 設定維持不變。

## Worker 與連線池

DB workers 共用一個 `Arc<Session>`。每個 worker 同時最多等待一個 workload CQL request；`C` 是 worker 數及 workload CQL in-flight 上限。Driver pool 的目標 data connections `D` 可獨立設定。Driver 依 token 與 shard 選擇 connection；connection 可 multiplex requests，因此 worker 數不等於實體連線數。

預設 pool 目標沿用 `P=C/(3*S)`。指定 `--per-shard-pool P` 後，worker 與 pool 解耦：

~~~text
legacy: P = C / (3 * S), D = 3 * S * P = C
explicit pool: D = 3 * S * P
~~~

`--worker-routing per-user` 將 `account_id` 固定送到同一 worker，並要求 `C=users`。每個 worker queue 容量為 1；全域 admission 容量由 `--queue-capacity` 限制。預設 `round-robin` 維持舊 routing。

Seed、preload 與 verification 在 measurement window 外，使用有界併發 `min(C,D)`。Measurement 路徑不執行 SELECT。`cql_write_lwt` 計數應與送出 LWT 的交易嘗試數一致；測量期間的 metadata/ledger read counters 預期為零。CLI 在 verification 前擷取 measurement-window driver-counter delta；對成功 enqueue 的 request，預期每筆一個 client CQL request。Preload 與 post-window verification 不在 CLI delta 中。獨立 integration smoke 的 driver snapshot 若涵蓋 post-window verification，需把 verification queries 一併計入該測試的預期數。

| 每 node shards `S` | `C=12` 的 `P` | `C=24` 的 `P` | `C=48` 的 `P` |
| ---: | ---: | ---: | ---: |
| 1 | 4 | 8 | 16 |
| 2 | 2 | 4 | 8 |
| 4 | 1 | 2 | 4 |

Write timeout、storage error 或回應解碼失敗在送出後都代表 CommitOutcomeUnknown。Service 凍結帳戶並停止全域 admission，不自動重試。Caller cancellation 不取消已接受的 worker 工作。Shutdown drain accepted work，再驗證 DB ledger 與 shutdown 後的 cache snapshot；未知結果時 cache 不得前進。完成驗證後才清理 owned resources。

## ScyllaDB 與 CPU profile

Runner 使用三個同機隔離的 ScyllaDB nodes。
Nodes 共用 host 與 storage device。
這個 profile 用於比較，不代表 production tuning。

| 設定 | 固定值 |
| --- | --- |
| ScyllaDB | `2026.1.14` |
| Container image | `scylladb/scylla@sha256:333b9ab3a3214c7f98af100711c51df7f58964e8227e6d26050e889ebb216df1` |
| Rust driver | `scylla 1.9.0` |
| Keyspace | 預設 RF=3；CLI 可設 RF=1..3，`durable_writes=true` |
| Wallet read/write consistency | 預設 read/write `THREE`；CLI 可分別設 1..RF。LWT serial CL 固定 `LOCAL_SERIAL` |
| Commitlog | `commitlog_sync=batch` |
| Node profile | `developer-mode=1`、`io-setup=0`、`overprovisioned=1` |

`Cargo.lock` 固定 `scylla=1.9.0` 與其依賴 `scylla-cql=2.0.0`。
Scylla CQL `client_options` 的 `DRIVER_VERSION=2.0.0` 取自 `scylla-cql` 的 `options::DEFAULT_DRIVER_VERSION`，其值是該 crate 的 `CARGO_PKG_VERSION`。
這個 client metadata 不表示 top-level Rust driver 變成 2.0.0。

每個 Scylla process 設定 `--memory 8G`。
Docker `Memory=10 GiB`、`MemorySwap=10 GiB` 表示 10 GiB memory limit 外沒有額外 swap allowance。
Default RF=3 / write CL=THREE 的 wallet write acknowledgement 要求三個 replicas 回覆。LWT 另使用 `LOCAL_SERIAL`；讀取 CL 與一般 write CL 分開記錄。
Runner 會驗證 node 的 effective commitlog 設定。
Schema metadata、DDL 與 `system.clients` audit 使用 CL `ONE`。
這些操作在 workload window 以外。
Readiness 查詢 `system.local.release_version` 可回報 CQL compatibility version `3.0.8`；它不是 Scylla binary release。
Runner 以 pinned image digest、image metadata 與 `scylla --version` 核對實際 release。

固定 release 沒有已驗證的獨立 per-write fsync profile。
Commitlog group commit 可能讓多筆交易共用一次 fsync。
本 benchmark 不宣稱每筆 logical transaction 對應一次 fsync；單一 LWT client request 也不代表單一 Paxos operation 或單次 fsync。
`commitlog_sync_batch_window_in_ms` 保留 release default `10000`，但 runtime batching effect 尚未建立。
01-32 保存固定 release [`db/config.cc`](https://raw.githubusercontent.com/scylladb/scylladb/scylla-2026.1.14/db/config.cc)、[`db/commitlog/commitlog.cc`](https://raw.githubusercontent.com/scylladb/scylladb/scylla-2026.1.14/db/commitlog/commitlog.cc) 與 [`db/commitlog/commitlog.hh`](https://raw.githubusercontent.com/scylladb/scylladb/scylla-2026.1.14/db/commitlog/commitlog.hh) source evidence：設定有註冊，檢查的 commitlog execution path 未讀取此 window。這不是固定等待 10 秒的證據。
Periodic profile 不符合 fsync 後 ack 的要求。

Runner 將三個 Scylla nodes 固定在以下 CPU sets。
Service、load 與 monitors 固定使用 `6,7,14,15`。
Service 和 load 在同一個 process。
Rust process CPU 數值包括 service、load clients 與程式內 monitors。
Python monitor 的 CPU 由 runner 另外記錄。
Runner 將 Python monitor CPU samples 寫入每 trial 的 `proc/samples.jsonl`；此值不在 `summary.csv`。
Scylla host PID 由 `docker top <container> -eo pid,args` 找出；只接受第一個 executable token 的 basename 恰為 `scylla` 的 row。

| `S` | Node A | Node B | Node C | Service、load、internal monitors |
| ---: | --- | --- | --- | --- |
| 1 | `0` | `2` | `4` | `6,7,14,15` |
| 2 | `0,1` | `2,3` | `4,5` | `6,7,14,15` |
| 4 | `0,1,8,9` | `2,3,10,11` | `4,5,12,13` | `6,7,14,15` |

`S=4` 使用每個 node 的兩個 physical cores 及其 SMT threads。
這些測試也改變 DB CPU 數量。
結果不能稱為固定 CPU 下的純軟體效益。
本結果也不是受控的 RocksDB 對 ScyllaDB 比較。

## 目前 profile 的執行流程

~~~text
seed and bounded preload (before the measurement gate)
  -> preload_state / preload_account_state
  -> AccountState cache

client input
  -> Service::submit
  -> account owner lock
  -> worker_loop
  -> process_job
  -> compute from cached AccountState
  -> ScyllaStore::commit
  -> one kind=1 INSERT ... IF NOT EXISTS
       LOCAL_SERIAL + configured regular write CL
  -> acknowledged Applied / Existing result
  -> publish AccountState cache only for Applied
     Existing keeps latest cache and returns stored ledger result
  -> reply

unknown write result / response decode failure
  -> freeze account and fail global admission closed
  -> drain accepted work
  -> compare full DB ledger/state with shutdown cache snapshot
  -> retain unknown-outcome evidence
  -> cleanup owned resources and report

Runner
  -> preflight
  -> run_trial
  -> verify DB ledger and cache snapshot
  -> stop_and_cleanup owned resources
  -> write run reports
~~~

Measurement window 從 clients 一起開始，到最後 client 收到 reply 為止。
Seed、drain、verification 與 cleanup 不計入 window。
CLI 先等待 `ready_to_measure`。
Runner 可用 start gate 對齊外部 monitors。
CLI 發出 `verified` 後可等待 finish gate。
Runner 先 audit `system.clients`，再釋放 finish gate。
`events.jsonl` 記錄 `startup`、`seeding`、`ready_to_measure`、`measuring`、`client_end`、`drained`、`verifying` 與 `verified`。
`measuring` 含 `measurement_start_unix_ns`；`client_end` 同時含 `measurement_start_unix_ns` 與 `measurement_end_unix_ns`。
E2E latency 從每次 `submit` 前量到 caller 收到 reply 後。
CLI 以 16 個 HDR stripes 合併 latency，不為每個 user 配置完整 histogram；原始樣本每 64 筆取一筆。
Stage histograms 在最後 reply 後立即停止；seed、drain、verification 與 finish-gate wait 不納入。
結果包含 request count、RPS、E2E 與 stage latency、process CPU、`/proc/self/io`、heartbeat lag、pool connection samples、driver counters 與完整 account/ledger validation。`cache_snapshot()` 在 service shutdown 後擷取；驗證會記錄實際完成比較的 cache account 數量。若寫入結果 unknown，不把可能已套用的 row 假定為未提交。

每次 trial 都使用新的 owned node data。
Runner 先驗證結果，再停止並檢查 resource ownership。
Runner 只刪除本輪明確擁有的 data、commitlog、hints、cache 與 volume。
Runner 不刪除 foreign resources。
Network ownership 以 `docker network inspect` 回傳的 canonical ID 核對，不把 `docker network ls -q` 的 short ID 當完整 ID。
Logs、metrics、設定、source hashes、correctness 與 cleanup audit 會保留。

Artifacts 放在 `target/scylla-wallet/runs/<run-id>/`。
目錄包含 trial results、summary、raw metrics、process logs、configuration、source hashes 與 cleanup records。
CLI 的 stdout、stderr、events、sample CSV 與 invalid result 也會保留。
缺少必要 metrics 會輸出 `N/A` 並使 formal trial invalid。
Diagnostic mode 可有 `passed_relaxed` 狀態。
此狀態不代表 formal pass。
Formal run 只有完整 27 trials、所有必要 metrics、validation 與 cleanup 都通過時才是 `formal_complete`。
自訂較小的 formal workload 不是完整 formal matrix。

## Workload 與容量門檻

| Mode | 預設 workload | 預設執行 |
| --- | --- | --- |
| `smoke` | 100 accounts × 20 requests | 每個指定 case 一次 |
| `pilot` | 1,000 accounts × 20 requests | 每個指定 case 一次 |
| `formal` | 50,000 accounts × 200 requests | 27 trials；9 個 S/C cases 各跑 3 次 |

Runner 預設 `smoke` case 為 `S=1,C=48`；完整 smoke 命令明確指定 9 個 S/C cases。
`pilot` 預設使用全部 9 個 cases，`formal` 使用相同 cases 並各重複三次。
預設 formal cases 使用 `S={1,2,4}` 與 `C={12,24,48}`。
每個 account 順序執行 200 requests。
Credit 與 Debit 數量相同。
正式矩陣在開始前會進行 disk、memory、CPU 與 device preflight。

CLI 必須指定 `--nodes`、`--keyspace` 與 `--output-dir`。
`--nodes` 接受三個以逗號分隔的 distinct endpoints；`--keyspace` 使用唯一的 `wallet_bench_*` 名稱。
其他參數可用 `--shards`、`--db-workers`、`--per-shard-pool`、`--worker-routing`、`--replication-factor`、`--read-cl`、`--write-cl`、`--users`、`--requests-per-user`、`--queue-capacity` 與 `--mode` 覆寫。
RF 與 CL 參數接受數值 1、2、3；read/write CL 不可高於 RF。Run metadata 另以 `ONE`、`TWO`、`THREE` 記錄一致性名稱，並記錄 write CL 所要求的 fsync acknowledgement replica 數。
預設值依序為 `S=1`、`C=12`、50,000 users、每 user 200 requests、queue capacity 50,000 與 `smoke`。
Request 數必須為偶數，且不得超過每帳戶 1,000 rows 的 verify 上限。
每個 client 先準備就緒，再一起開始；它依序送出 requests。
全部回覆完成後，CLI 停止 stage metrics、drain service，並驗證每個 ledger row。
`--start-gate` 可讓 runner 對齊外部 monitors；`--finish-gate` 讓 runner 在 CLI 關閉 session 前完成 `system.clients` audit。
Gate 最多等待 180 秒，等待不計入 workload window。
Result、events、sampled latency CSV 與 invalid result 寫入 `--output-dir`。

Formal disk estimate 使用每 request 1 KiB。
它計算 RF=3 replicas、一份 compaction copy、三個 1,024 MiB commitlogs、三個 64 MiB commitlog segments 與 16 GiB reserve。
50,000 × 200 workload 的估算需求為 82,042,421,248 bytes，約 76.4 GiB。
Formal preflight 至少要求 32 GiB available disk 與 32 GiB available memory。
Host CPU busy 必須不高於 10%。
Storage device busy 必須不高於 5%。
這些條件須連續三次取樣通過。
Runner 不會為了通過 gate 而刪除 foreign data 或降低門檻。

## 常用命令

從 repository root 執行下列命令。

跑 crate tests：

~~~bash
CARGO_TARGET_DIR=target/scylla-wallet cargo test --locked --manifest-path benchmarks/scylla-wallet/Cargo.toml
~~~

跑 runner tests：

~~~bash
python3 -m unittest discover -s tests -p test_scylla_wallet_runner.py -v
~~~

列出完整 formal matrix，不啟動 containers：

~~~bash
python3 benches/scylla_wallet.py --mode formal --dry-run
~~~

跑 smoke matrix：

~~~bash
python3 benches/scylla_wallet.py --mode smoke --shards 1,2,4 --workers 12,24,48
~~~

跑 `S=2` pilot：

~~~bash
python3 benches/scylla_wallet.py --mode pilot --shards 2 --workers 12,24,48
~~~

執行完整 formal matrix：

~~~bash
python3 benches/scylla_wallet.py --mode formal
~~~

Formal run 須先通過完整 preflight。
不要在未授權的 cluster 執行 integration tests。
不要把 diagnostic output 標記為 formal result。

## ScyllaDB integration tests

Integration tests 預設為 ignored。
它們需要一個使用者明確擁有的三節點 cluster，並建立唯一的 `wallet_bench_*` keyspace。
Contract test 驗證同帳戶順序、不同帳戶相同 `tx_id`、單 statement LWT count、replay/conflict、拒絕結果、shutdown cache snapshot 與 DB ledger/state。
Test 不會刪除 keyspace 或 foreign resources；Runner 負責 owned-resource cleanup。

~~~bash
SCYLLA_NODES=node-a:9042,node-b:9042,node-c:9042 SCYLLA_SHARDS=1 SCYLLA_WORKERS=3 CARGO_TARGET_DIR=target/scylla-wallet cargo test --locked --manifest-path benchmarks/scylla-wallet/Cargo.toml --test scylla_integration -- --ignored --test-threads=1
~~~

請將 endpoint 換成明確擁有的三個測試 nodes。
`SCYLLA_WORKERS` 必須符合 `workers % (3 * SCYLLA_SHARDS) = 0`。
Fault test 另外要求 `SCYLLA_FAULT_CONTAINER` 與 `SCYLLA_FAULT_OWNER`。
Container 必須同時有 `ledger-wallet.owner=scylla-wallet-runner` 與 `ledger-wallet.nonce` labels；nonce 必須與 `SCYLLA_FAULT_OWNER` 完全相同。
只可對 runner-owned test container 執行 fault test。
若單 node fault 後 LWT 沒有回覆成功，服務 fail-closed。Node restart 後完整驗證可見原狀態或多一筆已 coherent 套用、但 acknowledgement 未知的交易；cache 不得對未知交易前進，也不得自動重試。這項驗證不表示支援 Tokio 服務程序重啟後恢復，也不表示 host crash 或 power loss。
不可連到 production cluster。

SIGKILL fault 驗證單一 owned node 暫時不可用後的 wallet 資料一致性。
它不代表整個 cluster process crash、host crash 或 power loss。
本機 host 的 `ptrace_scope=1` 拒絕 `strace` attach；denial evidence 已保留，trace evidence 標示 unavailable。
Runner 不會變更 host policy。
Unavailable evidence 不證明或否定 fsync-per-write。

## 歷史結果：舊三 CQL profile

以下 pilot、formal preflight 與 smoke artifacts 均來自記憶體狀態 LWT profile 啟用前的三 CQL 實作。舊 workload 每筆交易執行兩個 SELECT 和一個 two-statement `UNLOGGED BATCH`。這些數字不驗證 `wallet-memory-lwt-v1`，也不與新 profile 合併比較。

Fixed-profile pilot `run-20261011T145542Z-8bf4d8f3` 完成全部九個 S/C cases。每個 case 使用 1,000 users × 20 requests，執行一次。合計 180,000 requests。每個 case 的 20,000 requests 都 accepted、applied 並收到 reply；1,000 accounts 與 20,000 ledger rows 驗證成功。每個 case 有 60,000 driver queries，errors、retries 與 timeouts 為零，pool min/max 都等於 `C`。九個 cases 的 metrics 與 cleanup validation 全部通過。

| Shards/node `S` | Workers/data connections `C` | Per-shard target `P` | RPS | p99 (ms) |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 12 | 4 | 2,971.7 | 385.09 |
| 1 | 24 | 8 | 5,137.7 | 242.22 |
| 1 | 48 | 16 | 8,225.3 | 152.44 |
| 2 | 12 | 2 | 2,839.0 | 406.06 |
| 2 | 24 | 4 | 5,166.4 | 225.18 |
| 2 | 48 | 8 | 9,626.4 | 129.04 |
| 4 | 12 | 1 | 2,700.8 | 435.42 |
| 4 | 24 | 2 | 4,871.3 | 252.18 |
| 4 | 48 | 4 | 8,748.8 | 169.08 |

每個 S/C case 只有一次短時間 diagnostic run。這些數字不能證明哪個資料庫引擎較快、SMT 帶來效能提升，或 steady-state capacity。Pilot 使用固定 Linux AIO profile。27 份 node logs 都顯示沒有 IOCB autoreduction。較早的 adaptive-IOCB run 保持獨立，不與本次結果合併。

- [Pilot report](../../benches/data/scylla_wallet/artifacts/target/scylla-wallet/runs/run-20261011T145542Z-8bf4d8f3/report.zh-TW.md)
- [Summary CSV](../../benches/data/scylla_wallet/artifacts/target/scylla-wallet/runs/run-20261011T145542Z-8bf4d8f3/summary.csv)
- [Case summary CSV](../../benches/data/scylla_wallet/artifacts/target/scylla-wallet/runs/run-20261011T145542Z-8bf4d8f3/case-summary.csv)
- [Counter deltas CSV](../../benches/data/scylla_wallet/archive-index.md#source-3e5c936269804998)
- [Fixed-AIO local audit](../../benches/data/scylla_wallet/archive-index.md#source-ef9af7c1a9fe12aa)
- [Latest startup-evidence replay](../../benches/data/scylla_wallet/artifacts/target/scylla-wallet/runner-validation/final-startup-evidence-replay.json)

Startup-evidence replay 使用較嚴格的 final validators 檢查九個保存的 outputs。Replay 沒有重跑 pilot，也沒有修改原始 source hashes 或 raw artifacts。Tablets query 的空白輸出無效；必須明確證明零筆，或包含一筆 null JSON row。Runner 在 workload 啟動前保存 Docker logs 與 hashes，並要求使用 Linux AIO 且沒有 IOCB reduction。不相關的 `io_uring` 到 `epoll` warning 可接受。

Formal attempt `run-20261011T152947Z-0f450816` 在建立 cluster 前停止。目標 filesystem `sdb2` 有 41,649,401,856 bytes（38.789 GiB）可用，低於包含 16 GiB reserve 的 82,042,421,248 bytes（76.408 GiB）估算。0.43 秒內只取得一筆 sample，未達三次連續通過的要求。Run 記錄零個 trials 與 `formal_complete=false`；沒有留下 owned resources 或 processes。

- [Formal preflight](../../benches/data/scylla_wallet/artifacts/target/scylla-wallet/runs/run-20261011T152947Z-0f450816/formal-preflight.json)
- [Formal attempt report](../../benches/data/scylla_wallet/artifacts/target/scylla-wallet/runs/run-20261011T152947Z-0f450816/report.zh-TW.md)
- [Formal run result](../../benches/data/scylla_wallet/artifacts/target/scylla-wallet/runs/run-20261011T152947Z-0f450816/run-result.json)
- [Formal attempt stdout](../../benches/data/scylla_wallet/artifacts/target/scylla-wallet/runner-validation/formal-capacity-attempt.stdout.log)

將 output root 移到容量足夠的 persistent filesystem，並通過所有 strict gates 後，使用：

~~~bash
python3 benches/scylla_wallet.py --mode formal \
  --output-root /path/to/persistent-disk/scylla-runs --skip-build
~~~

此路徑是 placeholder。維持容量估算與 gates，不使用 tmpfs 或刪除 foreign data 來通過 preflight。

歷史 `target/scylla-wallet` artifacts 的完整 raw tree 保存在本機外部 archive；小型原始檔、derived JSON summaries 和每個來源路徑的 archive 對照見 [Scylla evidence package](../../benches/data/scylla_wallet/README.md)。Package 中的 tracked evidence 可在 fresh checkout 閱讀；raw archive 僅能由保存該本機 archive 的環境取回。精簡結果也記錄在 [01-33](../../docs/01-33.development-validation-scylladb-wallet-benchmark.md)。

歷史驗證紀錄：在舊三 CQL implementation snapshot 上，Rust formatting、20 個預設 Rust tests、56 個 Python runner tests 及 `py_compile` 通過。當時預設 Rust tests 略過兩個需要真實 Scylla 的 tests；舊 live contract 與 fault evidence 連結見 01-33。這些數字不是目前 memory-LWT implementation 的 validation。

## Historical first-valid smoke

首個有效 diagnostic smoke 使用 `S=1,C=12,P=4`，100 accounts × 20 requests，共 2,000 requests。
Measurement window 為 0.684566095 秒；2,000 requests 全部 accepted、attempted、replied 並 applied，100 accounts 與 2,000 ledger rows 都驗證成功。
Pool min/max 都是 12；driver query delta 為 6,000，errors、retries、connection timeouts 與 request timeouts 都是 0。
除 required metrics 外，其餘 trial validation 都通過；status 是 `passed_relaxed`。
短 window 缺少足夠 Prometheus samples：node A 在 window 內沒有 sample，node B/C 各有一筆；預設值為零的 errors/timeouts series 沒有輸出，因此 required metrics 無法驗證。
Runner 後續加入 relabel profile，並在 fixed-profile pilot 中完成九 case。這筆歷史 smoke 的 `passed_relaxed` 狀態不代表正式通過，也不代表目前 pilot 失敗。

- [Run report](../../benches/data/scylla_wallet/artifacts/target/scylla-wallet/runs/run-20261011T112652Z-d580a5c5/report.zh-TW.md)
- [Trial configuration](../../benches/data/scylla_wallet/artifacts/target/scylla-wallet/runs/run-20261011T112652Z-d580a5c5/trials/trial-001-S1-C12-rep1/trial-config.json)
- [CLI result](../../benches/data/scylla_wallet/artifacts/target/scylla-wallet/runs/run-20261011T112652Z-d580a5c5/trials/trial-001-S1-C12-rep1/service/result.json)
- [Required metrics validation](../../benches/data/scylla_wallet/archive-index.md#source-020ce0ff688a0099)
- [Trial cleanup](../../benches/data/scylla_wallet/artifacts/target/scylla-wallet/runs/run-20261011T112652Z-d580a5c5/trials/trial-001-S1-C12-rep1/cleanup.json)

較早的 4-request C=12 lifecycle smoke 只驗證 CLI gates 與 database contract，不是性能結果。

- [CLI result](../../benches/data/scylla_wallet/artifacts/target/scylla-wallet/cli-c12-smoke/result.json)：4 requests、2 accounts，ledger verification 通過，driver query delta 為 12，retries/errors/timeouts 為零。
- [CLI events](../../benches/data/scylla_wallet/artifacts/target/scylla-wallet/cli-c12-smoke/events.jsonl)：包含 ready、measurement window、verified 與 runner audit gate。
- [Node A `system.clients` audit](../../benches/data/scylla_wallet/artifacts/target/scylla-wallet/cli-c12-smoke/nodea-system-clients-cl-one.txt)
- [Node B `system.clients` audit](../../benches/data/scylla_wallet/artifacts/target/scylla-wallet/cli-c12-smoke/nodeb-system-clients-cl-one.txt)
- [Node C `system.clients` audit](../../benches/data/scylla_wallet/artifacts/target/scylla-wallet/cli-c12-smoke/nodec-system-clients-cl-one.txt)
- [Final Rust test output](../../benches/data/scylla_wallet/artifacts/target/scylla-wallet/build-validation/final-cargo-test.stdout)
- [Final Rust format check](../../benches/data/scylla_wallet/archive-index.md#source-9e67ae619ee4d74d)（原始檔為 0 bytes；外部 archive 保留）
- [Final Python test stderr](../../benches/data/scylla_wallet/artifacts/target/scylla-wallet/runner-validation/final-python-tests.stderr)：56 tests passed。
- [Final Python compile output](../../benches/data/scylla_wallet/archive-index.md#source-e3561a10293fa869)（原始檔為 0 bytes；外部 archive 保留）
- [Failure-path cleanup recovery audit](../../benches/data/scylla_wallet/artifacts/target/scylla-wallet/runs/run-20261011T110026Z-634aa1b8/trials/trial-001-S1-C12-rep1/recovery-audit.json)：先前 infrastructure failure 後，owned resources recovery cleanup 通過；此為 cleanup 證據，不是性能 trial。

此 smoke 設定 `C=12`、`S=1`、`P=4`。
每個 node 有四條 data connections。
Node C 另有一條 driver control connection。
