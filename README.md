# ledger-wallet

以 Rust 進行帳本系統相關的基準測試實驗，涵蓋 bounded queue、WAL、RocksDB 與同主機 gRPC。正式 ledger 核心與儲存設計仍在規劃中，尚未實作為可執行服務。

## 開發環境

需要 rustup。專案的 `rust-toolchain.toml` 固定 Rust 1.98.1；Cargo 會使用此版本。

```sh
cargo fmt --check
cargo check --locked
cargo test --locked
cargo bench --bench counter_storage -- --iterations 10000000 --repetitions 3
cargo bench --bench bounded_queue_hashmap
cargo bench --bench bounded_queue_hashmap -- --iterations 10000 --repetitions 1
cargo bench --bench wal_tokio_sequential -- --iterations 10000 --repetitions 1 --batch-sizes 1024 --rotation-bytes 104857600 --preflight-observation-ms 50 --preflight-timeout-ms 5000 --preflight-max-cpu-pct 100 --preflight-max-disk-busy-pct 100 --preflight-min-mem-bytes 16777216 --preflight-free-reserve-bytes 1048576
BINDGEN_EXTRA_CLANG_ARGS='-I/usr/lib/gcc/x86_64-linux-gnu/13/include' cargo bench --bench rocksdb_tokio_sequential -- --iterations 512 --repetitions 1 --batch-sizes 64 --output-dir target/rocksdb-tokio-smoke --preflight-observation-ms 50 --preflight-timeout-ms 5000 --preflight-max-cpu-pct 100 --preflight-max-disk-busy-pct 100 --preflight-min-mem-bytes 16777216 --preflight-free-reserve-bytes 1048576
```

RocksDB benchmark builds also need libclang and a C/C++ toolchain. On the Ubuntu host used for development, `BINDGEN_EXTRA_CLANG_ARGS` adds GCC 13's standard include directory because bindgen's Clang header search does not find it by default. This path is specific to that host; use the system's matching GCC include directory elsewhere. To run the full default 10M-by-15-trial matrix, use `BINDGEN_EXTRA_CLANG_ARGS='-I/usr/lib/gcc/x86_64-linux-gnu/13/include' cargo bench --bench rocksdb_tokio_sequential`.

`counter_storage` 以單執行緒分別遞增 stack 上的 `u64`、`Box<u64>` 及只有一個預先建立固定 key 的 `HashMap<u64, u64>`。每一輪每種儲存方式都從 0 遞增相同次數，並確認最終值等於 `--iterations`；Box 與 HashMap 的建立在計時外。預設每輪 10,000,000 次、重複 3 輪，可用 `--iterations N` 和 `--repetitions N` 傳入正整數縮短或調整執行。輸出每輪的 wall time、RPS、平均 latency（wall nanoseconds 除以操作數）、ProcessTime CPU 時間差、CPU core equivalents、單核心 CPU 百分比，以及依 `available_parallelism` 正規化的 CPU 百分比；接著列出各指標的輪次中位數。

這是單執行緒、單一固定熱 key 的微基準。Stack 與 Box 中的同一個 `u64` 都可能常駐 CPU cache；數據反映熱點更新、指標間接存取及 volatile 指令成本，不能直接視為一般 DRAM stack/heap 速度差。HashMap 數據另包含固定 key 的雜湊與查找成本。平均 latency 不是逐次量測的 p50/p99；CPU 百分比是程序 CPU time 與 wall time 的比值，不是主機即時用量，小樣本可能有明顯計時雜訊。

`bounded_queue_hashmap` 是獨立同步 pipeline benchmark，固定使用 main thread collector、單一 producer thread、單一 worker thread，不使用 Tokio。Producer 以 ID `0..N-1` 依序送出 `key=id%100000` 的請求，不等待個別回覆；容量 50,000 的 request 與 response `sync_channel` 在滿載時提供 blocking backpressure。Worker 依 FIFO 收集 2048 或 4096 筆 batch，第一筆進 batch 後開始 1、5、10 或 20 ms deadline，達到 batch size 或 deadline 即處理，並在 producer 關閉時送出不足一批的尾批。每筆 response 都檢查 ID 與累計 count；每輪也驗證 map 長度與所有 key 的最終 count。Map 初始狀態可為空，或在計時前預先放入 100,000 個 value=0 的 key。預設每情境 10,000,000 筆、全 16 組、每組一輪；以 `--iterations`、`--repetitions`、`--map-state all|empty|prefilled`、`--batch-size all|2048|4096`、`--batch-timeout-ms all|1|5|10|20` 篩選。`--latency-sample-stride` 預設 1024；起跑前依每筆邏輯序號以 `splitmix64(index) % stride == 0` 建立 deterministic hash sample 計畫，stride=1 時每筆都取樣，輸出各 latency 分位數的實際樣本數。短輪次可用上面的 10,000 筆命令涵蓋矩陣。

該 benchmark 保留 producer 每次 blocking `send` 呼叫耗時作為 `enqueue_latency`；worker 成功取得 request 的 `recv` / `recv_timeout` 呼叫耗時仍作為 dequeue 呼叫指標，包含呼叫內阻塞等待。Timeout 與 Disconnected 呼叫不納入 dequeue latency。兩者都不是以下四段 latency 的替代值。因 `sync_channel` 沒有提供精確的成功入列時間，而且 receiver 可能在 `send` 返回前取得訊息，enqueue 呼叫耗時不能當作無重疊的 queue latency t1，也不會加進四段。

新增的逐筆 latency breakdown 以同一 request ID 的時間點串接：t0 是 producer 開始嘗試 send（`Request.started_at`）；t2 是 worker 成功 `recv` / `recv_timeout` 返回；t3 是該 batch 開始 map 更新；t4 是 batch map 更新與本地 response staging 完成、開始送 response 前；t6 是 collector 收到 response。四段定義為 `submit_to_dequeue=t2-t0`（包含 blocking send 與 request queue 內等待）、`batch_wait=t3-t2`、`batch_service=t4-t3`（該筆等到整個 batch 完成）、`response_delivery=t6-t4`（包含 response send 阻塞、response queue 等待及 collector 取出）。每段 mean 累計全部 N 筆實測時間；四段 count 都必須等於 N，四段 total nanoseconds 之和也必須逐輪精確等於 pipeline latency total。四段 p50/p95/p99 使用同一預先計算的 deterministic hash sample plan 和 nearest-rank，但不同階段的分位數不能直接相加；只有平均值可因逐筆分段而相加為完整 pipeline 平均值。Batch active duration / `map_update_avg_ns_per_item` 保留作原 batch 工作區間指標，並不等同於每筆都計入等待整批時間的 `batch_service`。

Enqueue RPS 使用 producer 首筆嘗試至末筆成功送入的時間。`dequeue_observed_rps` 是 N 除以首次成功 recv 呼叫開始至最後一次成功 recv 結果返回的 wall span，包含兩次 recv 間的 map 更新與 response send；`dequeue_active_rps` 是 N 除以 N 次成功 recv 呼叫耗時總和，呼叫耗時包含等待。Completed RPS 使用 pipeline 首筆嘗試至末筆 response 到達的時間。各階段 active RPS 不能相加或視為完整 pipeline throughput。ProcessTime 是程序 CPU time（Producer、Worker、Collector）；CPU 計量 window 從 main thread barrier 返回後至 collector 收到最後一筆 response，輸出的 `cpu_measurement_wall_s` 是同一 window 的 wall time；core equivalents = CPU seconds / `cpu_measurement_wall_s`，one-core CPU% = 100 × core equivalents。重複輪次逐輪輸出，並逐欄取輪次中位數。此 harness 描述固定三角色的本機同步通道 pipeline，不代表網路服務、磁碟持久性或外部到達率。

實測報告：[bounded_queue_hashmap benchmark](benches/bounded_queue_hashmap_report.md)；設計文件：[bounded queue HashMap benchmark](docs/01-06.development-design-bounded-queue-hashmap-benchmark.md) 與 [latency breakdown subdevelopment](docs/01-07.development-design-bounded-queue-latency-breakdown.md)。

`bounded_queue_hashmap_tokio` 是另外一個 Tokio pipeline benchmark，保留上述同步 benchmark 作獨立比較。它使用 3 個 Tokio runtime worker threads、一個容量 50,000 的 bounded request queue、一個 async batch worker task，以及 C 個依序送出並逐筆等待 oneshot response 的 request coroutine。預設 C 為 100000、50000、25000、12500、6250、3125、1562、781；每個 C 測試空 map/預填 map、batch size 2048/4096、timeout 1/5/10/20 ms 共 16 種變體，每情境 N=10,000,000、一輪。可用 `--coroutines all|N[,N...]`、`--iterations`、`--repetitions`、`--map-state`、`--batch-size`、`--batch-timeout-ms` 與 `--latency-sample-stride` 縮小工作量；程式以 CSV 列出 throughput、逐筆 latency、四段可加總 latency、batch、CPU 與驗證資料。Completed RPS 與 latency 結束於最後 t6；CPU wall/core 指標以 ProcessTime sample 附近的 `cpu_measured_at` 為終點，包含最後 coroutine 的少量 latency/count 收尾記錄。預設命令為 `cargo bench --bench bounded_queue_hashmap_tokio`。低 C 時每個 coroutine 只有一筆未完成 request，批次可能以 timeout flush。實測報告：[Tokio bounded queue HashMap benchmark](benches/bounded_queue_hashmap_tokio_report.md)；設計文件：[Tokio benchmark 設計](docs/01-08.development-design-bounded-queue-hashmap-tokio-benchmark.md)。

`bounded_queue_batch_pool_tokio` 是另外一個批次作為佇列單位的 Tokio HashMap benchmark。Producer 在短 async mutex 區段 append 到 shared active `Vec<Request>`；達 B=2048/4096 或第一筆 admitted request 的 T=1/5/10/20 ms timeout 時，整個 Vec ownership 進入 FIFO ready queue，由一個 worker 更新 map 並逐筆回覆 oneshot。預設沿用 N=10,000,000、100,000 keys、8 個 C 值、empty/prefilled map 的 128 種矩陣，每種一輪。50,000 個 semaphore permits 限制 reserved 與 admitted request 的總數；permit 等待、reserved-but-unadmitted、buffer-pool availability wait 及 admitted outstanding 分別記錄。Pool 預配置 `ceil(50000/B)+2` 個 Vec，必要時可擴充至 50,000 個，CSV 記錄 expansions、peak buffers、batch 及五個可加總 latency stages。t_start 到 t_done 只含 map updates 和 count staging；oneshot dispatch 由 t_done 到 t6 計時。舊 benchmark 的 per-request dequeue latency 不可直接和本測試的 stages 比較。可用 `--iterations`、`--repetitions`、`--coroutines`、`--map-state`、`--batch-size`、`--batch-timeout-ms` 和 `--latency-sample-stride` 篩選；預設命令為 `cargo bench --bench bounded_queue_batch_pool_tokio`。實測報告：[Tokio batch-pool HashMap benchmark](benches/bounded_queue_batch_pool_tokio_report.md)、[完整矩陣原始 CSV](benches/bounded_queue_batch_pool_tokio_results.csv)、[執行進度記錄](benches/bounded_queue_batch_pool_tokio_run.log)；設計文件：[batch-pool Tokio benchmark](docs/01-16.development-design-bounded-queue-batch-pool-tokio-benchmark.md)。

`bounded_queue_request_ring_tokio` 是另一個獨立 Tokio HashMap benchmark，使用固定 50,000 個 Request slot 的 ring。Producer 在 async metadata mutex 下將 Request 寫入 tail slot 並發佈單調遞增的邏輯索引；一個 FIFO worker 取得 sealed range 後直接在原 slot 更新 HashMap count 並送 oneshot response。Ring head 在整批 map 更新及所有 response send 嘗試完成後才前進，因此正在處理的 slot 仍計入 tail-head 容量。跨越實體陣列末端的批次最多以兩段連續 slot 範圍依序處理，不搬移或複製 Request。達 B=2048/4096 或首筆 admitted request 的 T=1/5/10/20 ms deadline 時 seal；即使所有 ID 已送出，也不提前 flush partial batch。工作量沿用 N=10,000,000、100,000 keys、8 個 C 值、empty/prefilled map 的 128 種矩陣，每種一輪；可用既有 CLI flags 縮小情境。CSV 記錄 full-ring wait、peak occupancy、wrapped batches、flush reason、CPU、batch 統計、response/count verifier，以及五段可精確相加的 latency。Latency mean 涵蓋全部 N 筆；p50/p95/p99 使用預設 stride 1024 的 deterministic ID sample。與 batch-pool 結果對照時，兩份結果均記錄於 2026-09-29，屬不同時間執行且各一輪；backpressure 分別由 ring 索引與 semaphore/pool buffer 控制。差異僅作描述，不據此單獨歸因。報告亦加入與 [Tokio per-request bounded queue benchmark](benches/bounded_queue_hashmap_tokio_report.md) 的 128-case 對照。預設命令為 `cargo bench --bench bounded_queue_request_ring_tokio`；完整矩陣原始 CSV、報告與設計見 [request-ring Tokio benchmark 報告](benches/bounded_queue_request_ring_tokio_report.md)、[完整矩陣原始 CSV](benches/bounded_queue_request_ring_tokio_results.csv)、[執行進度記錄](benches/bounded_queue_request_ring_tokio_run.log) 與 [request-ring 設計文件](docs/01-17.development-design-tokio-request-ring-hashmap-benchmark.md)。

`wal_sync` 是獨立的同步 crash-recoverable WAL benchmark，不使用 Tokio。預設每輪 10,000,000 筆 128-byte deterministic records，batch size 64、256、1,024、2,048、4,096 各 3 輪，rotation threshold 1 GiB；另測 batch size 4,096、256 MiB threshold 3 輪。每個 batch 完整寫入一個 segment 後呼叫一次 `File::sync_all`；新 segment 檔名會先做 directory sync 才回覆第一個 durable batch。Recovery 檢查全部 frame、checksum、record 順序與數量；結果以 CSV 輸出，驗證後清除該輪專屬 WAL 目錄。每輪開始前會檢查 filesystem、磁碟空間、記憶體、CPU 與 target disk busy；tmpfs 會被拒絕，預設 CPU 上限 10%、disk busy 上限 5%，最多等候閒置 60 秒。可用 `--iterations`、`--repetitions`、`--batch-sizes`、`--rotation-bytes` 與 `--preflight-*` 參數做短輪次或調整門檻。不要以完整預設矩陣作 smoke run；可用設計文件中的 10,000 筆命令。實測報告：[同步 WAL benchmark 實測報告](benches/wal_sync_report.md)；設計文件：[同步 WAL benchmark](docs/01-09.development-design-sync-wal-benchmark.md)。

`wal_tokio_sequential` 是與 `wal_sync` 對應的 sequential Tokio WAL benchmark。預設同樣每輪 10,000,000 筆、六種 batch/rotation 情境、每種 3 輪，並沿用 preflight、recovery 驗證及逐輪目錄清理。它使用帶 time driver 的 current-thread runtime 和 `tokio::fs`；一批必須完成 `write_all().await`、`flush().await`、`sync_all().await` 才會開始下一批。CSV 分開列出這三個 I/O stage，並在第一批 handoff 到最後 durable acknowledgement 的相同量測窗，以 1 ms heartbeat 記錄 tick 數、missed ticks 及 lateness p50/p95/p99/max。Heartbeat lateness 描述 event loop 排程延遲，不會單獨歸因於 WAL I/O。短 smoke command 已列在上方；完整預設矩陣有 18 輪，實測報告見 [Tokio sequential WAL benchmark 實測報告](benches/wal_tokio_sequential_report.md)，設計文件見 [Tokio sequential WAL benchmark](docs/01-10.development-design-wal-tokio-sequential-benchmark.md)。

`rocksdb_tokio_sequential` 是獨立 RocksDB ledger benchmark。預設每個 trial 寫入 10,000,000 筆交易，batch size 64、256、1,024、2,048、4,096 各跑 3 次；每個批次以單一 `WriteBatch` 原子寫入 ledger、受影響 users 的最新餘額及全域 sequence，開啟 WAL 並使用 `sync=true`，順序 await blocking-pool write。Workload 依序輪流走訪 100,000 users，每個 user 100 rounds 交替 credit/debit，每筆固定 100 cents。每筆 ledger value 包含固定 `Applied` outcome；以 tx_id 查詢時會回傳並驗證 outcome 和其餘交易欄位。寫入完成後關閉並重開各 trial DB，以固定 seed 洗牌 10M 個既有 tx IDs，再用 RocksDB batched multiget 逐筆驗證完整 ledger values。CSV 分開列出 write/query RPS、以交易數加權的 phase latency、build/write/prep/read/decode stages、CPU、process I/O、裝置 busy、1 ms event-loop heartbeat、RocksDB WAL/flush/compaction/stall stats 及兩階段 idle preflight。CSV row 寫到 stdout，preflight attempts 寫到 `<output-dir>/preflight.log`；每輪結束或遇到錯誤都會刪除該輪唯一 DB directory。預設 15 個 trial；完整 10M 矩陣已於 2026-09-26 完成。實測結果見 [Tokio sequential RocksDB benchmark 報告](benches/rocksdb_tokio_sequential_report.md)、[原始 CSV](benches/rocksdb_tokio_sequential_results.csv) 與 [preflight/stderr log](benches/rocksdb_tokio_sequential_preflight.log)。詳細指標語意與 limitations 見 [Tokio sequential RocksDB benchmark 設計](docs/01-11.development-design-rocksdb-tokio-sequential-benchmark.md)。

需要排查 2,048 或 4,096 大批次尾延遲時，可加 `--diagnostic` 產生 write/query per-batch trace CSV；trace 保存在記憶體中，直到讀回驗證通過並清除 trial DB 才一次寫檔。每列包含可加總重建 batch latency 的階段、worker thread/CPU、RocksDB write WAL/memtable/delay 與 read block cache/read/decompression PerfContext 指標。新版 trace 也記錄 native-call thread CPU time，以及同區間 context switches 和 page faults，用來對照 CPU 執行時間與等待。可依 `item_count` 對 `batch_total_ns` 加權計算 p99.9。實測結果見 [RocksDB 大批次尾延遲歸因報告](benches/rocksdb_tail_attribution_report.md)；短 smoke 命令如下，欄位、算法與限制見 [RocksDB 大批次尾延遲歸因設計](docs/01-12.development-design-rocksdb-tail-latency-attribution.md)：

```sh
BINDGEN_EXTRA_CLANG_ARGS='-I/usr/lib/gcc/x86_64-linux-gnu/13/include' \
  cargo bench --bench rocksdb_tokio_sequential -- \
  --iterations 4096 --repetitions 1 --batch-sizes 2048 --diagnostic \
  --output-dir target/rocksdb-tokio-diagnostic-smoke \
  --preflight-observation-ms 50 --preflight-timeout-ms 5000 \
  --preflight-max-cpu-pct 100 --preflight-max-disk-busy-pct 100 \
  --preflight-min-mem-bytes 16777216 --preflight-free-reserve-bytes 1048576
```

## 文件

- [歷史原始需求（待重新設計）](docs/01-01.raw-requirement-ledger-wallet.md)
- [專案初始化設計](docs/01-02.development-design-project-initialization.md)

## Tokio 同主機 gRPC Echo 基準測試

`grpc_echo_loopback` 以兩個獨立 Tokio 程序，在明文 127.0.0.1 loopback 上執行 unary echo。預設矩陣使用 128/4096-byte payload、固定 100 RPS baseline、closed-loop concurrency 1 至 256、一個共用 channel、5 秒 warmup、20 秒量測及 3 次重複。Client/server 綁定到主機實體核心 0..7 的 SMT sibling 配對；執行需要 `taskset` 和 `ss`。Protobuf 來源與已生成的 tonic/prost Rust 模組保存在 bench 目錄，所以其他 package build 不需要 protoc。

可用以下命令執行單一短案例 smoke check：

```sh
cargo bench --bench grpc_echo_loopback -- --payload-sizes 128 --concurrencies 1 --modes closed --channels 1 --warmup-seconds 0.2 --seconds 1 --repetitions 1 --output-dir target/grpc-echo-loopback-smoke
```

每個 trial 前都會執行嚴格閒置前置檢查，並在全新輸出目錄寫入 CSV、嘗試紀錄及逐 trial 診斷資料。多 channel 與替代 CPU 配置需明確指定；先觀察單 channel 結果，再依據量測證據決定是否比較。延遲、CPU 時間範圍、loopback 限制及各項指標的注意事項，請見 [gRPC Echo 基準測試設計](docs/01-14.development-design-grpc-echo-loopback-benchmark.md)。
