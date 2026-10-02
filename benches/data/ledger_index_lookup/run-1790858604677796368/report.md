# Tokio 單分片交易批次並行 transaction index 查詢與前景持久化效能對照報告

> **正式矩陣完成：18/18 trials 均以 exit code 0 結束。** 正式 run 包含 18 個 unique mode/repetition pairs、414 stage rows（23 per trial）及 180,000,000 筆交易；正式結果表與驗收狀態見下文。

## 固定工作負載

每個全尺寸 trial 由 50,000 個使用者各送出 200 筆請求，共 10,000,000 筆；每位使用者同時最多有一筆 outstanding request，全部請求進入單一 shard。Credit/debit 各占一半，金額為 1，history 為 0，採 `PerBatch` 餘額計算；效能負載不含 refund 或 balance query。佇列容量為 50,000，batch size 上限為 2,048；批次收集 timer 從第一筆 dequeue 並進入批次時開始，最多再等 5 ms。

每位使用者先由未修改的預設 handler 執行三筆 seed transaction：credit 100、credit 1、debit 1。每位使用者的初始及預期最終餘額都是 100；seed 後 sequence 為 150,000，完整負載後為 10,150,000。

全程不啟動 projector、watermark manager 或 GC worker。初始 projection/destination sequence 邊界維持 150,000，GC prefix 為 0，完整負載保持 10,000,000 筆未投影 backlog。Projection destination 是記憶體 `MockDB`；成功 apply 依其契約視為 durable，recovery 使用相同 instance。這不構成真實外部資料庫或程序崩潰持久性的證據。

## 策略矩陣與隔離

完整矩陣包含六種策略、每種 repetition 1–3，共 18 個 fresh child-process trials，每個 trial 使用新的 Tokio runtime 與自有 RocksDB 目錄：

| Mode | transaction index lookup |
| --- | --- |
| `point_get` | 每筆交易各做一次 Get，查詢後立即按原順序套用；不建立 lookup group。 |
| `whole_batch_multiget` | 每批至多 2,048 個 key，一次 MultiGet。 |
| `chunked_256_p1` | 每組 256 個 key，最多 1 個已提交且尚未收集的 group。 |
| `chunked_256_p2` | 每組 256 個 key，最多 2 個已提交且尚未收集的 groups。 |
| `chunked_256_p4` | 每組 256 個 key，最多 4 個已提交且尚未收集的 groups。 |
| `chunked_256_p8` | 每組 256 個 key，最多 8 個已提交且尚未收集的 groups。 |

MultiGet 查詢的是交易冪等性用的 transaction index。新負載的 10,000,000 個 key 預期全為 unique miss；這與先前正向的 LEDGER HIT lookup benchmark 範圍不同，後者不能證明 transaction-index misses 或 256-key groups 會加速。

每個 trial 共用一個 `worker_threads(4)` Tokio runtime，沿用繼承的 CPU affinity；不另行 pin 角色 thread，Tokio blocking pool 與 RocksDB internal pools 使用既有預設。已提交但尚未收集的 group 數，與當下實際執行中的 blocking query closure 數分開記錄。兩者都不等同於 CPU core 數。每個成功 child 關閉並重開資料庫、執行 integrity/recovery 檢查後才移除該 trial DB。

## 主機與 preflight

正式 run `1790858604677796368` 使用 AMD Ryzen 7 3700X：1 socket、8 cores、每 core 2 threads、16 logical processors、SMT enabled。程序的 inherited `cpus_allowed_list` 為 `0-15`；benchmark 沒有改寫 affinity。工具鏈為 rustc 1.98.1、Tokio 1.53.1、rocksdb 0.25.0。

資料位於 `/dev/sdb2`（device `sdb2`, major:minor `8:18`），檔案系統為 ext4；preflight path 是 `benches/data`。正式 preflight 觀察 3,000 ms，門檻為 CPU ≤10%、target device busy ≤5%，最多等待 60 秒。此次 setup observation 為 CPU 2.362%、device busy 0.100%；metadata 記錄 available memory 44,244,291,584 bytes、free filesystem space 90,794,991,616 bytes。Smoke 使用相同百分比門檻、100 ms observation，輸出寫在 `target/`。

## 計時範圍與讀值

Client measurement window 從釋放 client 前開始，到最慢 coroutine 收到 reply 為止；seed、preflight、settlement、recovery 與 integrity 不在窗口內。RPS 使用此窗口內完成的請求數除以 client wall time。CPU 是 benchmark endpoint process time，在最後 reply 時取樣；程序 IO 與目標裝置 IO 使用帶 offset 的前後 delta，並保留最後 reply 後的 tail 註記。

Request latency 以 ns 記錄，對 admission、enqueue、queue、batch、handler、response、total 等 stage 依 deterministic hash 選取約 1/64 請求；這是 hash 抽樣，不是每 64 筆固定取一筆。保存樣本數，p50/p95/p99 用 nearest-rank。Trial summary 另記錄 client wall seconds、RPS、CPU seconds、CPU core equivalent、CPU ns/request、RSS、process/device IO、WAL syncs/bytes、flush、compaction 與 stall。RSS 取 storage endpoint 的 `getrusage` `peak_rss_bytes`，涵蓋 process start 到該 endpoint，包含 seed 與 startup，並非僅測量窗口內的 RSS。CPU core equivalent 是整個 child process 的 CPU seconds 除以 client wall seconds，包含 load coroutines、runtime、blocking closures 與 RocksDB internal threads；此值不是按角色拆分的 CPU 使用量。Stage CSV 以 `unit`、`scope`、`sample_count` 及 p50/p95/p99 ns 註明分位數的單位和母體。 正式 trial 的 CPU sample offset 為 0–1 µs、IO offset 為 112–2,695 µs、storage offset 為 663–3,260 µs。Raw run directory 在複製正式報告前的 logical size 為 10.776 MiB。

Instrumentation 粒度有一項固定限制：`point_get` 的 native Get 及時鐘呼叫按 key 計數；MultiGet 的 native 呼叫及時鐘按 query group 計數。因此兩者的 call count 與 group-level durations 不能直接解讀成相同粒度的單次 lookup 成本。PointGet 的 in-flight group 數是 0/N/A。並行 group 的耗時互相重疊，group duration 加總不等於 query phase wall；不得把重疊時間相加作為前景 wall time。

## 正確性閘門與範圍限制

正式 full-matrix root gate **PASS**：manifest 有 18 個 unique mode/repetition pairs，18/18 rows 為 ok 且 exit code 0；stage CSV 共 414 rows（每 trial 23 stages）。跨 trial correctness/data checks 均通過：累計 180,000,000 筆交易（90,000,000 credits、90,000,000 debits），各 trial 均為 10,000,000 MISS、0 HIT、latest sequence 10,150,000、projected/destination sequence 150,000、GC prefix 0、seed-time boundary 未變、4 async workers 且背景 worker 未啟動；trial DB 均已清理。原始 12 個 unique correctness gates（public 3、private 8、budgeted 1）通過。Post-handshake-fix 後，`concurrent_groups_reconstruct_results_in_original_request_order` focused rerun 通過 1/1；這是既有 private case 的補跑，不是第 13 個 unique gate。

`FailSyncWrite` 的注入點在原生 `WriteBatch` 呼叫之前，因此只證明這個 prewrite fault 不會呼叫原生寫入或發布記憶體。真實 RocksDB write error 的 durable outcome 可能不確定；基準將任何寫入錯誤視為整個 trial fatal 並 teardown，不再執行一般寫入。Production 重試相同 key 前必須 close/reopen 並 reconcile durable state；不得宣稱同一 in-memory store 可安全續跑，也不得宣稱已驗證 physical power-cut recovery。

三次 repetition 僅提供描述性比較。下表保留每個 trial 的 RPS、request p50/p95/p99、CPU、recovery 與 integrity；各 mode 的 median 逐欄計算，request/stage 分位數是三個 trial percentile 的 median，絕不合併原始樣本或 pooled。三次重複不支持因果或統計顯著性結論，也不代表完整 projection、外部資料庫或完整 pipeline 的效能。

## Smoke 歷程

前兩次 smoke 未完成驗收，修正均屬 runner/metadata harness，沒有更動 workload 或 lookup 演算法：

1. `1790858358019393184` 在 `point_get_r1` 報錯：`index-lookup metrics cover 200 batches, account store recorded 0`。Adapter 把捕獲的 200 筆 batch records 與 legacy `measured_account_metrics.batches` 的 0 比較；legacy helper 只填背景欄位，前景欄位為 0。修正後 adapter 對捕獲的 `IndexLookupBatchMetrics.transaction_count` 求和，要求總和等於 requests，並保留非空且 per-batch 的記錄。
2. `1790858406612839850` 的 child trial 1 成功、manifest 狀態為 `ok`，但 parent metadata validator 因 writer 格式字串加上 `writeln` 產生的尾端空分隔行而拒絕 metadata；因此此 parent validation attempt 中止，metadata 留在 `running`。修正 reader 忽略空白分隔行，同時仍驗證所有 required fields。這不是 child transaction 或 lookup trial 失敗。

修正後完整 smoke `1790858458331307204` 完成 18 個 mode/repetition cases；每 case 200 users × 200 requests，共 40,000 requests，stride 1。Smoke artifacts 位於 `target/ledger-index-lookup-smoke/run-1790858458331307204/`；這些資料只確認小型 smoke 流程，不作全尺寸效能結論。

## Commands 與 raw artifacts

全尺寸正式命令：

```sh
BINDGEN_EXTRA_CLANG_ARGS='-I/usr/lib/gcc/x86_64-linux-gnu/13/include' cargo bench --locked --bench ledger_index_lookup_tokio
```

Smoke 命令：

```sh
BINDGEN_EXTRA_CLANG_ARGS='-I/usr/lib/gcc/x86_64-linux-gnu/13/include' cargo bench --locked --bench ledger_index_lookup_tokio -- --smoke --output-root target/ledger-index-lookup-smoke
```

CLI 可用 `--modes` 指定逗號分隔的六個精確 mode 名稱；`--repetitions` 接受逗號分隔且不重複的 `1,2,3` 值。部分篩選結果標記為 partial，不覆寫 canonical report。`--validate-only` 只列出矩陣，不建立資料庫。

正式 run 的原始資料：[`run_metadata.txt`](run_metadata.txt)、[`trial_manifest.csv`](trial_manifest.csv)、[`18-trial summary CSV`](ledger_index_lookup_summary.csv)、[`23-stage raw CSV`](ledger_index_lookup_stages.csv)。正式 run metadata 狀態為 complete；正式原始資料連結與數值表見下文。

成功 smoke 的本機產物路徑（位於 ignored `target/`，只作本機參照）：`target/ledger-index-lookup-smoke/run-1790858458331307204/run_metadata.txt`、`target/ledger-index-lookup-smoke/run-1790858458331307204/trial_manifest.csv`、`target/ledger-index-lookup-smoke/run-1790858458331307204/ledger_index_lookup_summary.csv`、`target/ledger-index-lookup-smoke/run-1790858458331307204/ledger_index_lookup_stages.csv`。

## 正式矩陣結果

Run `1790858604677796368` 已完成；各 mode 數值來自三個完整 trial，median 逐欄計算。Request/stage percentiles 為 trial percentile 的 median，未 pooled。

### 六種 mode 摘要

| Mode | RPS median | RPS min–max | CPU cores median | CPU µs/request median | Request p50 ms median | p95 ms median | p99 ms median |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `point_get` | 41,910.817 | 41,736.865–42,206.646 | 1.172649 | 27.922183 | 1,178.464 | 1,778.674 | 2,124.428 |
| `whole_batch_multiget` | 50,133.656 | 49,633.634–50,307.796 | 1.206294 | 24.241747 | 981.759 | 1,463.711 | 1,704.684 |
| `chunked_256_p1` | 47,050.242 | 46,992.582–48,174.993 | 1.195032 | 25.399057 | 1,048.824 | 1,556.591 | 1,861.117 |
| `chunked_256_p2` | 66,035.867 | 65,925.121–66,351.717 | 1.826680 | 27.692995 | 744.309 | 1,088.272 | 1,329.172 |
| `chunked_256_p4` | 84,817.870 | 84,713.769–85,179.121 | 2.589715 | 30.559671 | 573.774 | 848.637 | 1,001.405 |
| `chunked_256_p8` | 98,578.103 | 98,508.356–99,194.604 | 3.495190 | 35.456052 | 490.564 | 714.682 | 846.428 |

### 18 個 per-trial 結果

每列保留同一 trial 的 RPS、CPU、latency、recovery 與 integrity pairing。CPU µs/request 與 latency ms 均依 raw summary 轉換。

| Trial | Mode / rep | RPS | CPU cores | CPU µs/request | p50 ms | p95 ms | p99 ms | Recovery s | Integrity s |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | `point_get` / 1 | 41,736.865 | 1.172649 | 28.096246 | 1,185.063 | 1,793.757 | 2,124.428 | 0.449966 | 60.146496 |
| 2 | `whole_batch_multiget` / 1 | 49,633.634 | 1.206294 | 24.303956 | 1,001.152 | 1,483.231 | 1,730.991 | 0.405601 | 61.247668 |
| 3 | `chunked_256_p1` / 1 | 47,050.242 | 1.195032 | 25.399057 | 1,048.824 | 1,573.012 | 1,808.420 | 0.420952 | 60.992734 |
| 4 | `chunked_256_p2` / 1 | 66,035.867 | 1.828731 | 27.692995 | 739.895 | 1,104.870 | 1,329.172 | 0.401410 | 60.082285 |
| 5 | `chunked_256_p4` / 1 | 84,817.870 | 2.592006 | 30.559671 | 570.471 | 861.268 | 1,004.343 | 0.426254 | 60.283850 |
| 6 | `chunked_256_p8` / 1 | 98,508.356 | 3.513623 | 35.668273 | 490.564 | 716.775 | 846.428 | 0.420852 | 59.702479 |
| 7 | `whole_batch_multiget` / 2 | 50,133.656 | 1.215327 | 24.241747 | 981.759 | 1,463.711 | 1,699.895 | 0.442142 | 65.258583 |
| 8 | `chunked_256_p1` / 2 | 46,992.582 | 1.195335 | 25.436675 | 1,052.484 | 1,556.591 | 1,861.117 | 0.433251 | 60.935799 |
| 9 | `chunked_256_p2` / 2 | 65,925.121 | 1.826680 | 27.708406 | 744.309 | 1,088.272 | 1,378.934 | 0.452912 | 63.834373 |
| 10 | `chunked_256_p4` / 2 | 84,713.769 | 2.589715 | 30.570178 | 579.401 | 847.956 | 980.542 | 0.464323 | 59.281914 |
| 11 | `chunked_256_p8` / 2 | 98,578.103 | 3.495190 | 35.456052 | 490.603 | 714.682 | 853.799 | 0.391763 | 64.042508 |
| 12 | `point_get` / 2 | 42,206.646 | 1.176887 | 27.883928 | 1,165.885 | 1,778.674 | 2,092.622 | 0.402915 | 60.041768 |
| 13 | `chunked_256_p1` / 3 | 48,174.993 | 1.194335 | 24.791593 | 1,024.122 | 1,523.005 | 1,883.946 | 0.433662 | 58.942141 |
| 14 | `chunked_256_p2` / 3 | 66,351.717 | 1.794472 | 27.044841 | 749.456 | 1,059.737 | 1,323.259 | 0.457039 | 59.352230 |
| 15 | `chunked_256_p4` / 3 | 85,179.121 | 2.586577 | 30.366328 | 573.774 | 848.637 | 1,001.405 | 0.413328 | 60.710965 |
| 16 | `chunked_256_p8` / 3 | 99,194.604 | 3.490808 | 35.191509 | 489.088 | 713.670 | 836.058 | 0.424497 | 58.672149 |
| 17 | `point_get` / 3 | 41,910.817 | 1.170241 | 27.922183 | 1,178.464 | 1,767.767 | 2,150.419 | 0.490496 | 60.050026 |
| 18 | `whole_batch_multiget` / 3 | 50,307.796 | 1.205977 | 23.971977 | 981.555 | 1,446.174 | 1,704.684 | 0.412546 | 59.973183 |

### 23 stages × 6 modes

Scope 逐字取自 stage CSV。Samples 為每 mode 三個 trial 的 min–max。Stage sample count 為 0 時顯示 `N/A`，不顯示為 0；p50/p95/p99 是非空 trial percentiles 的 median。Admission/enqueue、dispatch wait、key prep、blocking-pool wait 與 memory publish 依 methods section 的清單以 µs 顯示，其他 stages 以 ms 顯示；三位小數。

| Mode | Stage | Scope | Display unit | Samples min–max | p50 median | p95 median | p99 median |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: |
| `point_get` | `request.total` | per_sample_request | ms | 155808–155808 | 1,178.464 | 1,778.674 | 2,124.428 |
| `point_get` | `request.admission` | per_sample_request | µs | 155808–155808 | 0.461 | 1.453 | 3.507 |
| `point_get` | `request.enqueue` | per_sample_request | µs | 155808–155808 | 0.060 | 0.150 | 0.241 |
| `point_get` | `request.queue` | per_sample_request | ms | 155808–155808 | 1,130.886 | 1,707.568 | 2,037.478 |
| `point_get` | `request.batch` | per_sample_request | ms | 155808–155808 | 0.148 | 0.821 | 1.418 |
| `point_get` | `request.handler` | per_sample_request | ms | 155808–155808 | 46.349 | 71.687 | 84.980 |
| `point_get` | `request.response` | per_sample_request | ms | 155808–155808 | 0.670 | 1.504 | 1.902 |
| `point_get` | `dispatch.wait` | per_batch_wall | µs | 4883–4883 | 2.414 | 3.446 | 4.459 |
| `point_get` | `keyprep` | per_batch_sum_of_key_encoding | µs | 4883–4883 | 88.213 | 104.061 | 116.169 |
| `point_get` | `query.wall` | per_batch_wall_from_keyprep_start_to_all_groups_validated | ms | N/A (0) | N/A | N/A | N/A |
| `point_get` | `lookup.blocking_pool_wait` | per_batch_sum_of_group_waits_overlapping | µs | N/A (0) | N/A | N/A | N/A |
| `point_get` | `lookup.native_get` | per_batch_sum_of_get_or_multiget_call_durations | ms | 4883–4883 | 34.694 | 59.367 | 66.825 |
| `point_get` | `lookup.decode_validate` | per_batch_sum_of_record_decode_and_key_validation | ms | 4883–4883 | 0.000 | 0.000 | 0.000 |
| `point_get` | `lookup.submit_to_collection` | per_batch_sum_of_overlapping_group_durations | ms | N/A (0) | N/A | N/A | N/A |
| `point_get` | `apply.blocking_pool_wait` | per_batch_legacy_worker_wait_before_interleaved_loop | µs | 4883–4883 | 10.390 | 14.076 | 17.613 |
| `point_get` | `apply.submit_to_collection` | per_batch_worker_wall_including_pool_wait | ms | 4883–4883 | 45.685 | 71.080 | 84.252 |
| `point_get` | `sequential_apply_build` | per_batch_legacy_interleaved_loop_including_point_reads_and_decode | ms | 4883–4883 | 36.379 | 61.188 | 68.774 |
| `point_get` | `write.sync_write_batch` | per_batch_sync_write_call | ms | 4883–4883 | 8.817 | 10.603 | 25.500 |
| `point_get` | `memory.publish` | per_batch_state_publish_wall | µs | 4883–4883 | 244.939 | 283.782 | 322.855 |
| `point_get` | `lookup_group.blocking_pool_wait` | per_group_worker_start_minus_submit | µs | N/A (0) | N/A | N/A | N/A |
| `point_get` | `lookup_group.native_get` | per_group_multiget_call_wall | ms | N/A (0) | N/A | N/A | N/A |
| `point_get` | `lookup_group.decode_validate` | per_group_record_decode_and_key_validation_sum | ms | N/A (0) | N/A | N/A | N/A |
| `point_get` | `lookup_group.submit_to_collection` | per_group_submit_to_join_completion_wall | ms | N/A (0) | N/A | N/A | N/A |
| `whole_batch_multiget` | `request.total` | per_sample_request | ms | 155808–155808 | 981.759 | 1,463.711 | 1,704.684 |
| `whole_batch_multiget` | `request.admission` | per_sample_request | µs | 155808–155808 | 0.471 | 1.413 | 3.186 |
| `whole_batch_multiget` | `request.enqueue` | per_sample_request | µs | 155808–155808 | 0.060 | 0.141 | 0.230 |
| `whole_batch_multiget` | `request.queue` | per_sample_request | ms | 155808–155808 | 942.423 | 1,404.425 | 1,641.271 |
| `whole_batch_multiget` | `request.batch` | per_sample_request | ms | 155808–155808 | 0.146 | 0.908 | 1.334 |
| `whole_batch_multiget` | `request.handler` | per_sample_request | ms | 155808–155808 | 38.373 | 58.590 | 67.053 |
| `whole_batch_multiget` | `request.response` | per_sample_request | ms | 155808–155808 | 0.687 | 1.513 | 1.805 |
| `whole_batch_multiget` | `dispatch.wait` | per_batch_wall | µs | 4883–4883 | 2.324 | 3.327 | 4.238 |
| `whole_batch_multiget` | `keyprep` | per_batch_wall | µs | 4883–4883 | 84.458 | 142.016 | 237.015 |
| `whole_batch_multiget` | `query.wall` | per_batch_wall_from_keyprep_start_to_all_groups_validated | ms | 4883–4883 | 27.409 | 47.633 | 50.648 |
| `whole_batch_multiget` | `lookup.blocking_pool_wait` | per_batch_sum_of_group_waits_overlapping | µs | 4883–4883 | 10.570 | 14.256 | 17.582 |
| `whole_batch_multiget` | `lookup.native_get` | per_batch_sum_of_get_or_multiget_call_durations | ms | 4883–4883 | 27.068 | 47.232 | 50.186 |
| `whole_batch_multiget` | `lookup.decode_validate` | per_batch_sum_of_record_decode_and_key_validation | ms | 4883–4883 | 0.000 | 0.000 | 0.000 |
| `whole_batch_multiget` | `lookup.submit_to_collection` | per_batch_sum_of_overlapping_group_durations | ms | 4883–4883 | 27.153 | 47.362 | 50.287 |
| `whole_batch_multiget` | `apply.blocking_pool_wait` | per_batch_sequential_apply_worker_wait | µs | 4883–4883 | 11.121 | 13.616 | 17.633 |
| `whole_batch_multiget` | `apply.submit_to_collection` | per_batch_worker_wall_including_pool_wait | ms | 4883–4883 | 10.028 | 11.818 | 25.483 |
| `whole_batch_multiget` | `sequential_apply_build` | per_batch_sequential_apply_and_build_after_prefetch | ms | 4883–4883 | 0.939 | 1.092 | 1.226 |
| `whole_batch_multiget` | `write.sync_write_batch` | per_batch_sync_write_call | ms | 4883–4883 | 8.986 | 10.752 | 24.400 |
| `whole_batch_multiget` | `memory.publish` | per_batch_state_publish_wall | µs | 4883–4883 | 244.739 | 281.057 | 317.064 |
| `whole_batch_multiget` | `lookup_group.blocking_pool_wait` | per_group_worker_start_minus_submit | µs | 4883–4883 | 10.570 | 14.256 | 17.582 |
| `whole_batch_multiget` | `lookup_group.native_get` | per_group_multiget_call_wall | ms | 4883–4883 | 27.068 | 47.232 | 50.186 |
| `whole_batch_multiget` | `lookup_group.decode_validate` | per_group_record_decode_and_key_validation_sum | ms | 4883–4883 | 0.000 | 0.000 | 0.000 |
| `whole_batch_multiget` | `lookup_group.submit_to_collection` | per_group_submit_to_join_completion_wall | ms | 4883–4883 | 27.153 | 47.362 | 50.287 |
| `chunked_256_p1` | `request.total` | per_sample_request | ms | 155808–155808 | 1,048.824 | 1,556.591 | 1,861.117 |
| `chunked_256_p1` | `request.admission` | per_sample_request | µs | 155808–155808 | 0.460 | 1.313 | 2.715 |
| `chunked_256_p1` | `request.enqueue` | per_sample_request | µs | 155808–155808 | 0.060 | 0.140 | 0.230 |
| `chunked_256_p1` | `request.queue` | per_sample_request | ms | 155808–155808 | 1,005.846 | 1,494.729 | 1,792.107 |
| `chunked_256_p1` | `request.batch` | per_sample_request | ms | 155808–155808 | 0.147 | 0.786 | 1.300 |
| `chunked_256_p1` | `request.handler` | per_sample_request | ms | 155808–155808 | 41.101 | 62.738 | 72.411 |
| `chunked_256_p1` | `request.response` | per_sample_request | ms | 155808–155808 | 0.673 | 1.429 | 1.790 |
| `chunked_256_p1` | `dispatch.wait` | per_batch_wall | µs | 4883–4883 | 2.375 | 3.396 | 4.499 |
| `chunked_256_p1` | `keyprep` | per_batch_wall | µs | 4883–4883 | 82.344 | 138.199 | 229.650 |
| `chunked_256_p1` | `query.wall` | per_batch_wall_from_keyprep_start_to_all_groups_validated | ms | 4883–4883 | 30.034 | 50.790 | 56.094 |
| `chunked_256_p1` | `lookup.blocking_pool_wait` | per_batch_sum_of_group_waits_overlapping | µs | 4883–4883 | 73.840 | 96.962 | 107.711 |
| `chunked_256_p1` | `lookup.native_get` | per_batch_sum_of_get_or_multiget_call_durations | ms | 4883–4883 | 29.331 | 50.013 | 55.262 |
| `chunked_256_p1` | `lookup.decode_validate` | per_batch_sum_of_record_decode_and_key_validation | ms | 4883–4883 | 0.000 | 0.000 | 0.000 |
| `chunked_256_p1` | `lookup.submit_to_collection` | per_batch_sum_of_overlapping_group_durations | ms | 4883–4883 | 29.587 | 50.295 | 55.570 |
| `chunked_256_p1` | `apply.blocking_pool_wait` | per_batch_sequential_apply_worker_wait | µs | 4883–4883 | 9.377 | 14.467 | 17.824 |
| `chunked_256_p1` | `apply.submit_to_collection` | per_batch_worker_wall_including_pool_wait | ms | 4883–4883 | 10.054 | 11.960 | 26.298 |
| `chunked_256_p1` | `sequential_apply_build` | per_batch_sequential_apply_and_build_after_prefetch | ms | 4883–4883 | 0.966 | 1.129 | 1.265 |
| `chunked_256_p1` | `write.sync_write_batch` | per_batch_sync_write_call | ms | 4883–4883 | 8.991 | 10.784 | 25.201 |
| `chunked_256_p1` | `memory.publish` | per_batch_state_publish_wall | µs | 4883–4883 | 245.240 | 284.874 | 319.619 |
| `chunked_256_p1` | `lookup_group.blocking_pool_wait` | per_group_worker_start_minus_submit | µs | 39063–39063 | 8.967 | 13.947 | 16.942 |
| `chunked_256_p1` | `lookup_group.native_get` | per_group_multiget_call_wall | ms | 39063–39063 | 3.650 | 6.366 | 7.130 |
| `chunked_256_p1` | `lookup_group.decode_validate` | per_group_record_decode_and_key_validation_sum | ms | 39063–39063 | 0.000 | 0.000 | 0.000 |
| `chunked_256_p1` | `lookup_group.submit_to_collection` | per_group_submit_to_join_completion_wall | ms | 39063–39063 | 3.679 | 6.405 | 7.192 |
| `chunked_256_p2` | `request.total` | per_sample_request | ms | 155808–155808 | 744.309 | 1,088.272 | 1,329.172 |
| `chunked_256_p2` | `request.admission` | per_sample_request | µs | 155808–155808 | 0.481 | 1.452 | 3.276 |
| `chunked_256_p2` | `request.enqueue` | per_sample_request | µs | 155808–155808 | 0.061 | 0.141 | 0.230 |
| `chunked_256_p2` | `request.queue` | per_sample_request | ms | 155808–155808 | 714.188 | 1,046.468 | 1,280.179 |
| `chunked_256_p2` | `request.batch` | per_sample_request | ms | 155808–155808 | 0.149 | 0.960 | 1.381 |
| `chunked_256_p2` | `request.handler` | per_sample_request | ms | 155808–155808 | 28.431 | 42.063 | 51.137 |
| `chunked_256_p2` | `request.response` | per_sample_request | ms | 155808–155808 | 0.706 | 1.565 | 1.854 |
| `chunked_256_p2` | `dispatch.wait` | per_batch_wall | µs | 4883–4883 | 2.475 | 3.446 | 4.348 |
| `chunked_256_p2` | `keyprep` | per_batch_wall | µs | 4883–4883 | 77.255 | 189.224 | 244.358 |
| `chunked_256_p2` | `query.wall` | per_batch_wall_from_keyprep_start_to_all_groups_validated | ms | 4883–4883 | 17.543 | 29.925 | 33.837 |
| `chunked_256_p2` | `lookup.blocking_pool_wait` | per_batch_sum_of_group_waits_overlapping | µs | 4883–4883 | 82.956 | 101.952 | 115.314 |
| `chunked_256_p2` | `lookup.native_get` | per_batch_sum_of_get_or_multiget_call_durations | ms | 4883–4883 | 33.709 | 57.766 | 65.931 |
| `chunked_256_p2` | `lookup.decode_validate` | per_batch_sum_of_record_decode_and_key_validation | ms | 4883–4883 | 0.000 | 0.000 | 0.000 |
| `chunked_256_p2` | `lookup.submit_to_collection` | per_batch_sum_of_overlapping_group_durations | ms | 4883–4883 | 34.054 | 58.107 | 66.315 |
| `chunked_256_p2` | `apply.blocking_pool_wait` | per_batch_sequential_apply_worker_wait | µs | 4883–4883 | 11.682 | 16.361 | 19.827 |
| `chunked_256_p2` | `apply.submit_to_collection` | per_batch_worker_wall_including_pool_wait | ms | 4883–4883 | 10.318 | 12.106 | 28.888 |
| `chunked_256_p2` | `sequential_apply_build` | per_batch_sequential_apply_and_build_after_prefetch | ms | 4883–4883 | 0.991 | 1.181 | 1.371 |
| `chunked_256_p2` | `write.sync_write_batch` | per_batch_sync_write_call | ms | 4883–4883 | 9.184 | 10.858 | 27.746 |
| `chunked_256_p2` | `memory.publish` | per_batch_state_publish_wall | µs | 4883–4883 | 247.403 | 284.494 | 319.179 |
| `chunked_256_p2` | `lookup_group.blocking_pool_wait` | per_group_worker_start_minus_submit | µs | 39063–39063 | 9.939 | 15.899 | 19.386 |
| `chunked_256_p2` | `lookup_group.native_get` | per_group_multiget_call_wall | ms | 39063–39063 | 4.214 | 7.320 | 8.471 |
| `chunked_256_p2` | `lookup_group.decode_validate` | per_group_record_decode_and_key_validation_sum | ms | 39063–39063 | 0.000 | 0.000 | 0.000 |
| `chunked_256_p2` | `lookup_group.submit_to_collection` | per_group_submit_to_join_completion_wall | ms | 39063–39063 | 4.257 | 7.364 | 8.530 |
| `chunked_256_p4` | `request.total` | per_sample_request | ms | 155808–155808 | 573.774 | 848.637 | 1,001.405 |
| `chunked_256_p4` | `request.admission` | per_sample_request | µs | 155808–155808 | 0.481 | 1.473 | 3.486 |
| `chunked_256_p4` | `request.enqueue` | per_sample_request | µs | 155808–155808 | 0.061 | 0.150 | 0.231 |
| `chunked_256_p4` | `request.queue` | per_sample_request | ms | 155808–155808 | 550.526 | 818.886 | 967.129 |
| `chunked_256_p4` | `request.batch` | per_sample_request | ms | 155808–155808 | 0.149 | 0.989 | 1.388 |
| `chunked_256_p4` | `request.handler` | per_sample_request | ms | 155808–155808 | 21.616 | 30.731 | 41.779 |
| `chunked_256_p4` | `request.response` | per_sample_request | ms | 155808–155808 | 0.713 | 1.596 | 1.879 |
| `chunked_256_p4` | `dispatch.wait` | per_batch_wall | µs | 4883–4883 | 2.454 | 3.456 | 4.479 |
| `chunked_256_p4` | `keyprep` | per_batch_wall | µs | 4883–4883 | 76.544 | 203.451 | 256.130 |
| `chunked_256_p4` | `query.wall` | per_batch_wall_from_keyprep_start_to_all_groups_validated | ms | 4883–4883 | 10.716 | 18.552 | 20.907 |
| `chunked_256_p4` | `lookup.blocking_pool_wait` | per_batch_sum_of_group_waits_overlapping | µs | 4883–4883 | 95.488 | 116.978 | 155.981 |
| `chunked_256_p4` | `lookup.native_get` | per_batch_sum_of_get_or_multiget_call_durations | ms | 4883–4883 | 39.690 | 69.405 | 76.757 |
| `chunked_256_p4` | `lookup.decode_validate` | per_batch_sum_of_record_decode_and_key_validation | ms | 4883–4883 | 0.000 | 0.000 | 0.000 |
| `chunked_256_p4` | `lookup.submit_to_collection` | per_batch_sum_of_overlapping_group_durations | ms | 4883–4883 | 40.072 | 69.774 | 77.318 |
| `chunked_256_p4` | `apply.blocking_pool_wait` | per_batch_sequential_apply_worker_wait | µs | 4883–4883 | 12.444 | 17.704 | 21.551 |
| `chunked_256_p4` | `apply.submit_to_collection` | per_batch_worker_wall_including_pool_wait | ms | 4883–4883 | 10.442 | 12.143 | 32.352 |
| `chunked_256_p4` | `sequential_apply_build` | per_batch_sequential_apply_and_build_after_prefetch | ms | 4883–4883 | 0.992 | 1.167 | 1.410 |
| `chunked_256_p4` | `write.sync_write_batch` | per_batch_sync_write_call | ms | 4883–4883 | 9.317 | 10.895 | 31.304 |
| `chunked_256_p4` | `memory.publish` | per_batch_state_publish_wall | µs | 4883–4883 | 247.545 | 287.128 | 319.439 |
| `chunked_256_p4` | `lookup_group.blocking_pool_wait` | per_group_worker_start_minus_submit | µs | 39063–39063 | 11.571 | 17.973 | 22.512 |
| `chunked_256_p4` | `lookup_group.native_get` | per_group_multiget_call_wall | ms | 39063–39063 | 4.903 | 8.675 | 10.123 |
| `chunked_256_p4` | `lookup_group.decode_validate` | per_group_record_decode_and_key_validation_sum | ms | 39063–39063 | 0.000 | 0.000 | 0.000 |
| `chunked_256_p4` | `lookup_group.submit_to_collection` | per_group_submit_to_join_completion_wall | ms | 39063–39063 | 4.951 | 8.726 | 10.182 |
| `chunked_256_p8` | `request.total` | per_sample_request | ms | 155808–155808 | 490.564 | 714.682 | 846.428 |
| `chunked_256_p8` | `request.admission` | per_sample_request | µs | 155808–155808 | 0.561 | 1.663 | 4.529 |
| `chunked_256_p8` | `request.enqueue` | per_sample_request | µs | 155808–155808 | 0.070 | 0.151 | 0.241 |
| `chunked_256_p8` | `request.queue` | per_sample_request | ms | 155808–155808 | 471.001 | 689.212 | 807.838 |
| `chunked_256_p8` | `request.batch` | per_sample_request | ms | 155808–155808 | 0.150 | 1.202 | 1.456 |
| `chunked_256_p8` | `request.handler` | per_sample_request | ms | 155808–155808 | 18.121 | 24.507 | 42.140 |
| `chunked_256_p8` | `request.response` | per_sample_request | ms | 155808–155808 | 0.774 | 1.728 | 1.945 |
| `chunked_256_p8` | `dispatch.wait` | per_batch_wall | µs | 4883–4883 | 2.445 | 3.527 | 5.029 |
| `chunked_256_p8` | `keyprep` | per_batch_wall | µs | 4883–4883 | 78.697 | 217.998 | 273.873 |
| `chunked_256_p8` | `query.wall` | per_batch_wall_from_keyprep_start_to_all_groups_validated | ms | 4883–4883 | 7.142 | 12.446 | 14.333 |
| `chunked_256_p8` | `lookup.blocking_pool_wait` | per_batch_sum_of_group_waits_overlapping | µs | 4883–4883 | 101.230 | 611.366 | 2,545.524 |
| `chunked_256_p8` | `lookup.native_get` | per_batch_sum_of_get_or_multiget_call_durations | ms | 4883–4883 | 48.984 | 86.958 | 98.979 |
| `chunked_256_p8` | `lookup.decode_validate` | per_batch_sum_of_record_decode_and_key_validation | ms | 4883–4883 | 0.000 | 0.000 | 0.000 |
| `chunked_256_p8` | `lookup.submit_to_collection` | per_batch_sum_of_overlapping_group_durations | ms | 4883–4883 | 49.421 | 87.326 | 99.359 |
| `chunked_256_p8` | `apply.blocking_pool_wait` | per_batch_sequential_apply_worker_wait | µs | 4883–4883 | 11.592 | 15.559 | 21.190 |
| `chunked_256_p8` | `apply.submit_to_collection` | per_batch_worker_wall_including_pool_wait | ms | 4883–4883 | 10.377 | 12.135 | 34.431 |
| `chunked_256_p8` | `sequential_apply_build` | per_batch_sequential_apply_and_build_after_prefetch | ms | 4883–4883 | 0.986 | 1.188 | 1.487 |
| `chunked_256_p8` | `write.sync_write_batch` | per_batch_sync_write_call | ms | 4883–4883 | 9.271 | 10.943 | 33.213 |
| `chunked_256_p8` | `memory.publish` | per_batch_state_publish_wall | µs | 4883–4883 | 246.643 | 287.800 | 328.496 |
| `chunked_256_p8` | `lookup_group.blocking_pool_wait` | per_group_worker_start_minus_submit | µs | 39063–39063 | 12.383 | 20.388 | 308.549 |
| `chunked_256_p8` | `lookup_group.native_get` | per_group_multiget_call_wall | ms | 39063–39063 | 6.064 | 11.024 | 12.660 |
| `chunked_256_p8` | `lookup_group.decode_validate` | per_group_record_decode_and_key_validation_sum | ms | 39063–39063 | 0.000 | 0.000 | 0.000 |
| `chunked_256_p8` | `lookup_group.submit_to_collection` | per_group_submit_to_join_completion_wall | ms | 39063–39063 | 6.114 | 11.079 | 12.724 |

### IO 與持久化 medians

Byte 欄位換算 GiB/MiB；device busy 按 `busy_ms / (client_wall_s × 10)` 計算。

| Mode | Process read GiB | Process write GiB | Device read GiB | Device write GiB | Peak RSS MiB | Device busy % |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `point_get` | 0.000 | 3.574 | 0.000 | 3.702 | 769.414 | 5.732 |
| `whole_batch_multiget` | 0.000 | 3.560 | 0.000 | 3.677 | 770.395 | 6.808 |
| `chunked_256_p1` | 0.000 | 3.559 | 0.000 | 3.688 | 772.824 | 6.436 |
| `chunked_256_p2` | 0.000 | 3.560 | 0.000 | 3.677 | 807.426 | 9.128 |
| `chunked_256_p4` | 0.000 | 3.574 | 0.000 | 3.661 | 809.402 | 11.171 |
| `chunked_256_p8` | 0.000 | 3.563 | 0.000 | 3.652 | 802.949 | 13.005 |

WAL/flush/compaction/stall：

| Mode | WAL syncs | Writes with WAL | WAL bytes | Flush write GiB | Compaction read GiB | Compaction write GiB | Stall µs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `point_get` | 4,883 | 4,883 | 1,620,170,905 | 0.587 | 1.364 | 1.459 | 0 |
| `whole_batch_multiget` | 4,883 | 4,883 | 1,620,170,905 | 0.587 | 1.352 | 1.445 | 0 |
| `chunked_256_p1` | 4,883 | 4,883 | 1,620,170,905 | 0.586 | 1.351 | 1.444 | 0 |
| `chunked_256_p2` | 4,883 | 4,883 | 1,620,170,905 | 0.587 | 1.350 | 1.445 | 0 |
| `chunked_256_p4` | 4,883 | 4,883 | 1,620,170,905 | 0.586 | 1.367 | 1.459 | 0 |
| `chunked_256_p8` | 4,883 | 4,883 | 1,620,170,905 | 0.587 | 1.356 | 1.448 | 0 |

### Lookup coverage 與 concurrency

| Mode | Native calls / trial | Keys / trial | MISS / trial | HIT / trial | Peak submitted / not collected | Peak blocking closures running |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `point_get` | 10,000,000 | 10,000,000 | 10,000,000 | 0 | 0 | 0 |
| `whole_batch_multiget` | 4,883 | 10,000,000 | 10,000,000 | 0 | 1 | 1 |
| `chunked_256_p1` | 39,063 | 10,000,000 | 10,000,000 | 0 | 1 | 1 |
| `chunked_256_p2` | 39,063 | 10,000,000 | 10,000,000 | 0 | 2 | 2 |
| `chunked_256_p4` | 39,063 | 10,000,000 | 10,000,000 | 0 | 4 | 4 |
| `chunked_256_p8` | 39,063 | 10,000,000 | 10,000,000 | 0 | 8 | 8 |

Native Get calls 以 key 計數，MultiGet calls 以 group 計數。已提交且尚未收集的 group 峰值與實際同時執行的 blocking closure 峰值分別列出；兩者均不等於 CPU core 數。

## 結果解讀

### Throughput 與 CPU 成本

在本次 transaction-index MISS profile 中，PointGet median 為 41,910.817 RPS。WholeBatch 為 50,133.656 RPS（相對 PointGet +19.62%）；P1 為 47,050.242、P2 為 66,035.867、P4 為 84,817.870（2.024× PointGet）、P8 為 98,578.103 RPS（2.352× PointGet）。P8 是本次測試最高的吞吐量設定。

P8 相對 P4 的 RPS 增加 16.22%，平均 CPU cores 增加 34.96%，CPU µs/request 增加 16.02%。CPU core medians：PointGet 1.172649、WholeBatch 1.206294、P1 1.195032、P2 1.826680、P4 2.589715、P8 3.495190。WholeBatch 的 CPU/request 最低，為 24.241747 µs；PointGet 27.922183、P1 25.399057、P2 27.692995、P4 30.559671、P8 35.456052 µs。P8 相對 PointGet 的 CPU/request 高 26.98%。

### Query phase、持久化與端到端 latency

Query phase wall p50：WholeBatch 27.409 ms、P1 30.034 ms、P2 17.543 ms、P4 10.716 ms、P8 7.142 ms；PointGet 的讀取與套用逐筆交錯，沒有獨立 query phase，故為 N/A。Sync WriteBatch call p50 落在 8.817–9.317 ms；P8 的此段 p50 大於其 query phase p50。這是整段 serial write call，包含 RocksDB CPU、WAL、memtable 與 sync 工作，不可標作純 fsync。並行群組耗時互相重疊，不可加總或以分位數相減歸因。

50,000-user closed-loop 負載的 request tails 多數等待在 queue。Queue p50：PointGet 1,130.886 ms、P8 471.001 ms；handler p50：PointGet 46.349 ms、P8 18.121 ms。各 mode request p99 median：PointGet 2,124.428 ms、WholeBatch 1,704.684 ms、P1 1,861.117 ms、P2 1,329.172 ms、P4 1,001.405 ms、P8 846.428 ms。這些是端到端 request percentiles，不是單一 RocksDB key read latency。

### Query concurrency 與裝置讀值

實際 blocking-query-closure peak 依 PointGet、WholeBatch、P1、P2、P4、P8 順序為 0/1/1/2/4/8。CPU cores 是整個 child process 的平均 CPU seconds/client wall seconds，不是 peak 值，也不能證明 4 個 CPU 已飽和。較早的 LEDGER HIT 優勢不能預測此 transaction-index MISS profile。

每個 trial 均有 4,883 batches、4,883 WAL syncs 及 1,620,170,905 WAL bytes。Native calls：PointGet 10,000,000、WholeBatch 4,883、每個 chunked mode 39,063。各 mode 寫入工作量相同，且都維持 WAL enabled 與 sync=true。

Measured device busy 為 5.659–13.026%，此值不足以證明裝置飽和或不飽和。Process physical-read counters 為 0–8,192 bytes/trial，device read counters 為 0–593,920 bytes；在這個 profile 中 native Get 成本主要反映 cached path。Device counters 是整顆裝置的非 exclusive counters。沒有 per-thread perf、syscall 或 per-file tracing，故無法精確歸因 CPU、lock 或 native algorithm 成本；也不能宣稱 fsync 不重要或 physical disk latency 為零。

### Recovery、範圍與限制

Recovery 時間範圍為 0.391763–0.490496 s，integrity 範圍為 58.672149–65.258583 s；18 個 trial 的 owned DB 均在驗證後清理，沒有殘留 trial DB。Run directory 在正式報告複製前 logical size 為 10.776 MiB。

結果僅描述本次固定單 shard、10M unique transaction-index misses/trial、PerBatch、四個 Tokio async workers、既有 blocking/RocksDB pools、同步 WAL 寫入與當前主機的效能。Projection、watermark、GC 均未啟動，destination 是同 instance reopen 的記憶體 MockDB；不能外推到外部 projection DB、程序/實體斷電 durability 或完整 pipeline。三次 repetition 是描述性資料，不支持因果或統計顯著性。正式結果本身沒有更改任何 default。

## 驗收狀態

Formal rootgate 與數據 cross-checks：**PASS**。18/18 trials exit 0、18 unique pairs、414 stage rows、全部 lookup/sequence/projection/GC/worker/boundary checks 通過。原始 12 個 unique correctness gates（public 3 + private 8 + budgeted 1）通過。Post-handshake-fix focused rerun `concurrent_groups_reconstruct_results_in_original_request_order`：**PASS 1/1**；它是既有 private case 的補跑，不是第 13 個 unique gate。

FailSyncWrite 在原生 WriteBatch 前注入，只證明該 prewrite failure 不呼叫原生寫入且不發布 memory。真實 RocksDB write error 的 durable outcome 可能不確定；benchmark 將其視為整個 trial fatal 並 teardown，不繼續一般寫入。Production 重試相同 key 前必須 close/reopen 並 reconcile durable state；不宣稱同一 in-memory store 可安全續跑，也不宣稱 physical power-cut validation。

## Raw artifacts

Run-level: [summary](ledger_index_lookup_summary.csv) · [stages](ledger_index_lookup_stages.csv) · [manifest](trial_manifest.csv) · [metadata](run_metadata.txt). Per-trial links include summary, batch trace, stages, metadata, stdout and stderr.

| Trial | Mode / rep | Per-trial raw files |
| ---: | --- | --- |
| 1 | `point_get` / 1 | [summary](trials/trial-01-point_get_r1/ledger_index_lookup_trial_summary.csv) · [batches](trials/trial-01-point_get_r1/ledger_index_lookup_trial_batches.csv) · [stages](trials/trial-01-point_get_r1/ledger_index_lookup_trial_stages.csv) · [metadata](trials/trial-01-point_get_r1/ledger_index_lookup_trial_metadata.txt) · [stdout](trials/trial-01-point_get_r1/trial-01.stdout.log) · [stderr](trials/trial-01-point_get_r1/trial-01.stderr.log) |
| 2 | `whole_batch_multiget` / 1 | [summary](trials/trial-02-whole_batch_multiget_r1/ledger_index_lookup_trial_summary.csv) · [batches](trials/trial-02-whole_batch_multiget_r1/ledger_index_lookup_trial_batches.csv) · [stages](trials/trial-02-whole_batch_multiget_r1/ledger_index_lookup_trial_stages.csv) · [metadata](trials/trial-02-whole_batch_multiget_r1/ledger_index_lookup_trial_metadata.txt) · [stdout](trials/trial-02-whole_batch_multiget_r1/trial-02.stdout.log) · [stderr](trials/trial-02-whole_batch_multiget_r1/trial-02.stderr.log) |
| 3 | `chunked_256_p1` / 1 | [summary](trials/trial-03-chunked_256_p1_r1/ledger_index_lookup_trial_summary.csv) · [batches](trials/trial-03-chunked_256_p1_r1/ledger_index_lookup_trial_batches.csv) · [stages](trials/trial-03-chunked_256_p1_r1/ledger_index_lookup_trial_stages.csv) · [metadata](trials/trial-03-chunked_256_p1_r1/ledger_index_lookup_trial_metadata.txt) · [stdout](trials/trial-03-chunked_256_p1_r1/trial-03.stdout.log) · [stderr](trials/trial-03-chunked_256_p1_r1/trial-03.stderr.log) |
| 4 | `chunked_256_p2` / 1 | [summary](trials/trial-04-chunked_256_p2_r1/ledger_index_lookup_trial_summary.csv) · [batches](trials/trial-04-chunked_256_p2_r1/ledger_index_lookup_trial_batches.csv) · [stages](trials/trial-04-chunked_256_p2_r1/ledger_index_lookup_trial_stages.csv) · [metadata](trials/trial-04-chunked_256_p2_r1/ledger_index_lookup_trial_metadata.txt) · [stdout](trials/trial-04-chunked_256_p2_r1/trial-04.stdout.log) · [stderr](trials/trial-04-chunked_256_p2_r1/trial-04.stderr.log) |
| 5 | `chunked_256_p4` / 1 | [summary](trials/trial-05-chunked_256_p4_r1/ledger_index_lookup_trial_summary.csv) · [batches](trials/trial-05-chunked_256_p4_r1/ledger_index_lookup_trial_batches.csv) · [stages](trials/trial-05-chunked_256_p4_r1/ledger_index_lookup_trial_stages.csv) · [metadata](trials/trial-05-chunked_256_p4_r1/ledger_index_lookup_trial_metadata.txt) · [stdout](trials/trial-05-chunked_256_p4_r1/trial-05.stdout.log) · [stderr](trials/trial-05-chunked_256_p4_r1/trial-05.stderr.log) |
| 6 | `chunked_256_p8` / 1 | [summary](trials/trial-06-chunked_256_p8_r1/ledger_index_lookup_trial_summary.csv) · [batches](trials/trial-06-chunked_256_p8_r1/ledger_index_lookup_trial_batches.csv) · [stages](trials/trial-06-chunked_256_p8_r1/ledger_index_lookup_trial_stages.csv) · [metadata](trials/trial-06-chunked_256_p8_r1/ledger_index_lookup_trial_metadata.txt) · [stdout](trials/trial-06-chunked_256_p8_r1/trial-06.stdout.log) · [stderr](trials/trial-06-chunked_256_p8_r1/trial-06.stderr.log) |
| 7 | `whole_batch_multiget` / 2 | [summary](trials/trial-07-whole_batch_multiget_r2/ledger_index_lookup_trial_summary.csv) · [batches](trials/trial-07-whole_batch_multiget_r2/ledger_index_lookup_trial_batches.csv) · [stages](trials/trial-07-whole_batch_multiget_r2/ledger_index_lookup_trial_stages.csv) · [metadata](trials/trial-07-whole_batch_multiget_r2/ledger_index_lookup_trial_metadata.txt) · [stdout](trials/trial-07-whole_batch_multiget_r2/trial-07.stdout.log) · [stderr](trials/trial-07-whole_batch_multiget_r2/trial-07.stderr.log) |
| 8 | `chunked_256_p1` / 2 | [summary](trials/trial-08-chunked_256_p1_r2/ledger_index_lookup_trial_summary.csv) · [batches](trials/trial-08-chunked_256_p1_r2/ledger_index_lookup_trial_batches.csv) · [stages](trials/trial-08-chunked_256_p1_r2/ledger_index_lookup_trial_stages.csv) · [metadata](trials/trial-08-chunked_256_p1_r2/ledger_index_lookup_trial_metadata.txt) · [stdout](trials/trial-08-chunked_256_p1_r2/trial-08.stdout.log) · [stderr](trials/trial-08-chunked_256_p1_r2/trial-08.stderr.log) |
| 9 | `chunked_256_p2` / 2 | [summary](trials/trial-09-chunked_256_p2_r2/ledger_index_lookup_trial_summary.csv) · [batches](trials/trial-09-chunked_256_p2_r2/ledger_index_lookup_trial_batches.csv) · [stages](trials/trial-09-chunked_256_p2_r2/ledger_index_lookup_trial_stages.csv) · [metadata](trials/trial-09-chunked_256_p2_r2/ledger_index_lookup_trial_metadata.txt) · [stdout](trials/trial-09-chunked_256_p2_r2/trial-09.stdout.log) · [stderr](trials/trial-09-chunked_256_p2_r2/trial-09.stderr.log) |
| 10 | `chunked_256_p4` / 2 | [summary](trials/trial-10-chunked_256_p4_r2/ledger_index_lookup_trial_summary.csv) · [batches](trials/trial-10-chunked_256_p4_r2/ledger_index_lookup_trial_batches.csv) · [stages](trials/trial-10-chunked_256_p4_r2/ledger_index_lookup_trial_stages.csv) · [metadata](trials/trial-10-chunked_256_p4_r2/ledger_index_lookup_trial_metadata.txt) · [stdout](trials/trial-10-chunked_256_p4_r2/trial-10.stdout.log) · [stderr](trials/trial-10-chunked_256_p4_r2/trial-10.stderr.log) |
| 11 | `chunked_256_p8` / 2 | [summary](trials/trial-11-chunked_256_p8_r2/ledger_index_lookup_trial_summary.csv) · [batches](trials/trial-11-chunked_256_p8_r2/ledger_index_lookup_trial_batches.csv) · [stages](trials/trial-11-chunked_256_p8_r2/ledger_index_lookup_trial_stages.csv) · [metadata](trials/trial-11-chunked_256_p8_r2/ledger_index_lookup_trial_metadata.txt) · [stdout](trials/trial-11-chunked_256_p8_r2/trial-11.stdout.log) · [stderr](trials/trial-11-chunked_256_p8_r2/trial-11.stderr.log) |
| 12 | `point_get` / 2 | [summary](trials/trial-12-point_get_r2/ledger_index_lookup_trial_summary.csv) · [batches](trials/trial-12-point_get_r2/ledger_index_lookup_trial_batches.csv) · [stages](trials/trial-12-point_get_r2/ledger_index_lookup_trial_stages.csv) · [metadata](trials/trial-12-point_get_r2/ledger_index_lookup_trial_metadata.txt) · [stdout](trials/trial-12-point_get_r2/trial-12.stdout.log) · [stderr](trials/trial-12-point_get_r2/trial-12.stderr.log) |
| 13 | `chunked_256_p1` / 3 | [summary](trials/trial-13-chunked_256_p1_r3/ledger_index_lookup_trial_summary.csv) · [batches](trials/trial-13-chunked_256_p1_r3/ledger_index_lookup_trial_batches.csv) · [stages](trials/trial-13-chunked_256_p1_r3/ledger_index_lookup_trial_stages.csv) · [metadata](trials/trial-13-chunked_256_p1_r3/ledger_index_lookup_trial_metadata.txt) · [stdout](trials/trial-13-chunked_256_p1_r3/trial-13.stdout.log) · [stderr](trials/trial-13-chunked_256_p1_r3/trial-13.stderr.log) |
| 14 | `chunked_256_p2` / 3 | [summary](trials/trial-14-chunked_256_p2_r3/ledger_index_lookup_trial_summary.csv) · [batches](trials/trial-14-chunked_256_p2_r3/ledger_index_lookup_trial_batches.csv) · [stages](trials/trial-14-chunked_256_p2_r3/ledger_index_lookup_trial_stages.csv) · [metadata](trials/trial-14-chunked_256_p2_r3/ledger_index_lookup_trial_metadata.txt) · [stdout](trials/trial-14-chunked_256_p2_r3/trial-14.stdout.log) · [stderr](trials/trial-14-chunked_256_p2_r3/trial-14.stderr.log) |
| 15 | `chunked_256_p4` / 3 | [summary](trials/trial-15-chunked_256_p4_r3/ledger_index_lookup_trial_summary.csv) · [batches](trials/trial-15-chunked_256_p4_r3/ledger_index_lookup_trial_batches.csv) · [stages](trials/trial-15-chunked_256_p4_r3/ledger_index_lookup_trial_stages.csv) · [metadata](trials/trial-15-chunked_256_p4_r3/ledger_index_lookup_trial_metadata.txt) · [stdout](trials/trial-15-chunked_256_p4_r3/trial-15.stdout.log) · [stderr](trials/trial-15-chunked_256_p4_r3/trial-15.stderr.log) |
| 16 | `chunked_256_p8` / 3 | [summary](trials/trial-16-chunked_256_p8_r3/ledger_index_lookup_trial_summary.csv) · [batches](trials/trial-16-chunked_256_p8_r3/ledger_index_lookup_trial_batches.csv) · [stages](trials/trial-16-chunked_256_p8_r3/ledger_index_lookup_trial_stages.csv) · [metadata](trials/trial-16-chunked_256_p8_r3/ledger_index_lookup_trial_metadata.txt) · [stdout](trials/trial-16-chunked_256_p8_r3/trial-16.stdout.log) · [stderr](trials/trial-16-chunked_256_p8_r3/trial-16.stderr.log) |
| 17 | `point_get` / 3 | [summary](trials/trial-17-point_get_r3/ledger_index_lookup_trial_summary.csv) · [batches](trials/trial-17-point_get_r3/ledger_index_lookup_trial_batches.csv) · [stages](trials/trial-17-point_get_r3/ledger_index_lookup_trial_stages.csv) · [metadata](trials/trial-17-point_get_r3/ledger_index_lookup_trial_metadata.txt) · [stdout](trials/trial-17-point_get_r3/trial-17.stdout.log) · [stderr](trials/trial-17-point_get_r3/trial-17.stderr.log) |
| 18 | `whole_batch_multiget` / 3 | [summary](trials/trial-18-whole_batch_multiget_r3/ledger_index_lookup_trial_summary.csv) · [batches](trials/trial-18-whole_batch_multiget_r3/ledger_index_lookup_trial_batches.csv) · [stages](trials/trial-18-whole_batch_multiget_r3/ledger_index_lookup_trial_stages.csv) · [metadata](trials/trial-18-whole_batch_multiget_r3/ledger_index_lookup_trial_metadata.txt) · [stdout](trials/trial-18-whole_batch_multiget_r3/trial-18.stdout.log) · [stderr](trials/trial-18-whole_batch_multiget_r3/trial-18.stderr.log) |
