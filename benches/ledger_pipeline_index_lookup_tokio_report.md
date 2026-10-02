# Ledger Pipeline Index Lookup：完整 pipeline 整合效能報告

> 本報告涵蓋 `integrated_pipeline` 九個完整 trial。九個 child trials 均完成且通過逐 trial 與整體資料驗證；原始 runner 在報告輸出階段失敗，之後以受限的 report-only recovery 重新驗證 artifacts 並產生報告。原始 run metadata 的 `run_status=failed` 保留，不將原始命令記為成功。

## Run 身分與矩陣

沿用 `ledger_index_lookup_tokio` target。`--profile foreground_persistence` 是預設，保留六種 foreground lookup mode × repetition 1、2、3；本報告使用 `--profile integrated_pipeline`，僅接受 `point_get`、`chunked_256_p4`、`chunked_256_p8`。每個 repetition 輪替 mode 執行順序；repetition 是唯一 trial 身分，不是重複次數。完整矩陣為 9 個 fresh child-process、Tokio runtime 與 RocksDB trial。正式命令：

```sh
BINDGEN_EXTRA_CLANG_ARGS='-I/usr/lib/gcc/x86_64-linux-gnu/13/include' cargo bench --locked --bench ledger_index_lookup_tokio -- --profile integrated_pipeline
```

完整 trial 固定 50,000 users/coroutines × 每位 200 筆 sequential requests，共 10,000,000 requests/trial；每位使用者最多一筆 outstanding request，credit/debit 各 50%，amount 1，history 0。使用單 shard、`PerBatch` balances、queue capacity 50,000、batch 上限 2,048、first-dequeue timeout 5 ms 及 4 個共享 Tokio async workers。Projector、watermark manager 與 safe GC worker 都啟用；projection/GC batch 256、watermark tick 100 ms、retention 500 ms。GC 有進展時連續 catch up 並 yield；受阻或無工作時 sleep 100 ms。Blocking pool、RocksDB internal pools 與 CPU affinity 沿用既有預設／繼承值。

Pipeline CLI 的獨立預設為 `--index-lookup chunked --index-group-size 256 --index-concurrency 4`，runtime workers 預設 4。Group size 範圍 1..=2048、concurrency 範圍 1..=8，由 `IndexLookupConfig` 驗證。明確指定 group size 或 concurrency 僅可搭配 `--index-lookup chunked`；在其他 mode 下明確傳入會被拒絕。這項 pipeline 設定與 formal runner 的固定 profile 分開。

## Preflight 與 client window

正式 run root、每個 trial setup，以及 seed 完成後的 premeasurement 都執行 3 秒 strict preflight：CPU busy ≤10%、目標裝置 busy ≤5%，最多等待 60 秒，並檢查預估負載與 reserve 所需 memory/free-space；runner 不提供放寬門檻的參數。Smoke profile 使用 200 users × 200 requests、stride 1、100 ms observation；輸出限於 `target/`。Smoke 的小型批次通常只有一個 lookup group，無法驗證正式負載下的多 group parallelism。Smoke 與 partial matrix 不更新 canonical report。

Client wall 從 start barrier 釋放前，到最慢 coroutine 收到最後 reply 為止。Seed、preflight、settlement、recovery、integrity 不納入 RPS／request latency。Seed 為每人 credit 100、credit 1、debit 1，共 150,000 seed sequence；預期 request window 後 latest sequence 10,150,000、每人餘額 100。

## 計時、scope 與彙總規則

- Request latency 依 deterministic hash stride 64 抽樣；每 trial 使用 nearest-rank 計算 p50/p95/p99 並保留 sample count。跨 trial 只彙整各 trial percentile 的 median/min/max，不 pool 原始樣本，也不平均不同 trial 的 percentile。
- CPU 量測在 start barrier 前重設 process CPU clock，於最慢 coroutine 收到 reply 時讀取；CPU time 是整個 client window 內 child process 所有 threads 的增量，不含 setup 與 seed。CPU core equivalents 以此 CPU 增量除以 client wall，CPU µs/request 以此增量除以完成 requests。這是 child process 總量，不是 Tokio worker、blocking worker 或 RocksDB thread 分項。CPU sample offset 相對 client end 記錄。
- RSS 使用 process peak RSS，涵蓋程序啟動與 seed 階段。Progress 在 client end、背景 workers 仍執行時讀取。程序 IO、裝置 IO 與 RocksDB/storage 的起始 snapshot 在 seed/preflight 完成後、queue/background/client task setup 之前取得；結束 snapshot 在 JoinSet 收集後取得，報告各自相對 client end 的 offset。這些 IO/storage delta 的起始點不同於 client window，也包含其後執行期間的活動，不應稱作精確的 client-window-only IO。
- 每 trial 輸出 24 個 stage summaries。Captured foreground batch metrics 的 `transaction_count` 總和須為 10,000,000。`dispatch.wait` 與 exclusive AccountStore `batch_gate.wait` 分開報告。
- Lookup `query.wall` 涵蓋 key preparation 到所有 query groups 收集、驗證完成；群組 blocking-pool wait、native call、decode/validate 與 submit-to-collection 另列。並行群組的 clock duration 互相重疊，不可加總或與 batch wall 相減作歸因。所有 stage duration 都是診斷指標，不可加總成 request end-to-end latency。
- RocksDB/WAL counters 以整個 DB 為 scope，包含前景與 background projection、watermark、GC writes。數量可能隨背景進度與 mode 改變；不假定 4,883 次 WAL sync 或固定 WAL bytes，也不把 foreground-only 的 sync 數套用至整合測試。程序 IO 與 block-device IO 各保留其 scope 和 offset，不宣稱 per-file、syscall、per-thread CPU 或物理 IO 歸因。

## Background progress 與 settlement

Client-end 在 workers 還活著時分開讀取 latest sequence、source durable projected sequence、destination sequence、GC prefix、watermark 與相關進度。有效關係為：

```text
seed_seq <= source_projected_seq <= destination_seq <= latest_seq
gc_prefix_seq <= source_projected_seq
gc_prefix_seq <= latest_seq
```

不要求 client-end source projected sequence 與 destination sequence 相等。分開報告 `latest_seq - source_projected_seq` projection backlog，以及 `latest_seq - gc_prefix_seq` GC backlog。

Settlement 必須先完成 source durable projection，並核對 `durable_projected_seq = destination_seq = latest_seq`；接著推進 final boundary/watermark，再由 safe GC 僅刪除已投影、帳戶餘額具 durable coverage 且符合 retention 的連續舊 prefix。本 profile 使用 PerBatch balance coverage。Retained rows 可因 500 ms retention 而留存，不要求 GC 刪除全部資料。Close/reopen source RocksDB 後核對所有餘額 100、sequence 10,150,000、boundary target ≤ durable projected sequence，且 projection 覆蓋 GC prefix。

Destination 是 benchmark `MockDB`：successful in-memory apply 依測試契約視為 durable；reopen 檢查沿用同一 instance。此項不代表外部資料庫或 process-crash/power-loss durability。

## Correctness gate status

Focused suite 已執行：

```sh
BINDGEN_EXTRA_CLANG_ARGS='-I/usr/lib/gcc/x86_64-linux-gnu/13/include' cargo test --locked --test ledger_pipeline_index_lookup pipeline_parallel_index_
```

結果：5 passed、0 failed、28 filtered；native build 4m01s、測試執行 0.73s，fixtures 已清理。五個 focused flows 涵蓋 point_get/P4/P8 的順序、duplicate/conflict 與 history/projection/watermark/GC 路徑；awaiter cancellation 下 gate/head/fence；Checkpoint + GC + 同一 MockDB instance 的兩次 reopen；lookup failure 的 queued-request drain；以及 pre-write failure 不發布 sequence/boundary。這些 correctness tests 不替代完整 formal profile。Checkpoint/snapshot 與 history 是相容性正確性測試，不增加為 formal 效能矩陣案例。

Smoke 矩陣：第一個 smoke attempt 在 parent validator 階段失敗；後續完整 smoke 的 9/9 cases 通過。Smoke 使用 200 users × 200 requests（40,000 requests/trial），其批次通常只有一個 lookup group，不能用來證明完整負載的多群組平行度，也不作正式效能結果。

正式九個 child trials 的逐 trial artifacts 與整體 matrix 驗證皆通過，90,000,000 requests 中 credits/debits 各 45,000,000；settlement、recovery、integrity 與 GC final prefix 檢查全部完成。所有 trial owned DB 目錄已清理。

## Formal results：`integrated_pipeline`

本次 metadata 記錄 AMD Ryzen 7 3700X（8 physical cores／16 logical processors、SMT enabled）、允許 CPU 0–15 且沿用 inherited affinity（未 pin threads）、ext4 `/dev/sdb2`、rustc 1.98.1、Tokio 1.53.1 與 RocksDB 0.25.0。這些結果描述此主機與 runtime 組合。

Run `1790871680151272835` 的 9/9 子程序各完成 10,000,000 requests，共 90,000,000 requests、45,000,000 credits 與 45,000,000 debits。逐 trial artifacts、aggregate matrix 與 manifest 均經 report-only recovery 驗證；每 trial 的 captured batch transaction count 為 10,000,000，合計 43,947 batches、1,402,272 sampled requests 與 216 lookup-stage rows。原始完整命令因 canonical report 輸出路徑重複 `benches/` 而以 exit code 1 結束；當時九個 child trials 與 aggregate validation 已成功。`run_metadata.txt` 的 `run_status=failed` 保留。之後以 `--report-only` 重新驗證完整矩陣與各 trial artifacts 後重產報告，沒有重跑 runtime benchmark，且不改寫原始 run status。詳見 [原始事件紀錄](data/ledger_pipeline_index_lookup/run-1790871680151272835/report_incident.txt) 與 [report-only recovery 記錄](data/ledger_pipeline_index_lookup/run-1790871680151272835/report_recovery.txt)。

此 workload 為閉迴路 50,000 coroutine × 200 requests，每位使用者一次僅有一筆 outstanding request；Client RPS 是此有限 request window 的速率，不代表低到達率 SLA。

### 三種 mode 摘要

RPS 列 median 與三個 trial 的 min–max。Request p50/p95/p99 列各 trial nearest-rank percentile 的 median；另列 request p99 的 trial min–max。CPU core equivalents、CPU µs/request、process peak RSS、whole-DB WAL、client-end projection backlog 與 GC backlog 均按 mode 彙整。

| Mode | Client RPS median (min–max) | Request p50/p95/p99 trial-percentile median (ms) | Request p99 min–max (ms) | CPU cores median | CPU µs/request median | RSS peak median (bytes) | Whole-DB WAL sync median | Whole-DB WAL bytes median (min–max) | Projection backlog median (min–max) | GC backlog median (min–max) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `point_get` | 36506.174 (36220.777–36719.915) | 1343.964902 / 2093.681900 / 2491.550427 | 2476.115494–2513.713343 | 1.276268 | 35.014281 | 1,861,894,144 | 49,148 | 1,669,473,607 (1,669,424,603–1,669,522,339) | 1,408 (1,408–1,664) | 8,897,136 (8,895,856–8,898,416) |
| `chunked_256_p4` | 56982.273 (53855.320–67098.727) | 733.594762 / 1131.694630 / 5850.762527 | 1297.396932–11150.254106 | 2.093051 | 36.731618 | 1,834,176,512 | 49,203 | 1,669,390,821 (1,669,372,429–1,669,578,201) | 1,664 (1,664–1,664) | 8,899,440 (8,894,576–8,899,952) |
| `chunked_256_p8` | 75076.856 (69256.317–75223.618) | 652.001851 / 957.783005 / 1124.482976 | 1101.310151–1217.676725 | 3.177564 | 42.324146 | 1,846,804,480 | 48,859 | 1,669,559,515 (1,669,364,739–1,669,705,929) | 1,664 (1,664–1,664) | 8,894,320 (8,890,480–8,899,440) |

以三次 repetition 的 mode median 比較，P4 的 client RPS 比 point_get 高 56.09%，P8 比 point_get 高 105.66%。P8 相對 P4 的 RPS 高 31.75%，同時平均 CPU core equivalents 高 51.81%、CPU µs/request 高 15.23%。這是三個 repetitions 的描述性比較，不足以單獨證明 CPU 因果或硬體飽和。P4 仍作為可調整的起始預設，以保留背景工作所需 CPU 空間；這些結果不證明 P4 最優或尾延遲最安全。此矩陣中 P8 的吞吐量與 request p99 較佳，但 CPU 使用較高。

### 九個 trial 明細

CPU 是 start barrier 前重設、直到 client end 的 whole-process CPU time 增量，只涵蓋 client window，不含 setup/seed；RSS peak 則涵蓋 startup/seed。程序 IO 與目標裝置 IO 的取樣 offset 以最慢 reply 為基準；WAL 計數是整個 DB 的觀測值。

| Trial | Mode | Rep | Client wall (s) | Client RPS | Request p50/p95/p99 (ms) | CPU cores | CPU µs/request | RSS peak (bytes) | Process read/write (bytes) | Device read/write; busy time | Whole-DB WAL syncs/bytes | CPU/progress/process-IO/storage offsets from client end |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | `point_get` | 1 | 272.331784 | 36719.915 | 1336.260717 / 2093.681900 / 2476.115494 | 1.276268 | 34.756831 | 1,853,513,728 | 4,096 / 4,254,781,440 | sdb2 352,256 / 4,959,916,032; 53,944 ms | 49,148 / 1,669,473,607 | CPU 0; progress 16; proc IO 171; storage 781 µs |
| 2 | `chunked_256_p4` | 1 | 149.034123 | 67098.727 | 731.068782 / 1074.212703 / 1297.396932 | 2.464503 | 36.729509 | 1,834,176,512 | 4,096 / 4,140,892,160 | sdb2 110,592 / 4,794,318,848; 53,366 ms | 49,245 / 1,669,578,201 | CPU 0; progress 11; proc IO 3667; storage 4285 µs |
| 3 | `chunked_256_p8` | 1 | 132.936972 | 75223.618 | 648.551278 / 956.952310 / 1124.482976 | 3.185607 | 42.348491 | 1,849,835,520 | 4,096 / 4,085,985,280 | sdb2 212,992 / 4,705,210,368; 52,726 ms | 48,859 / 1,669,559,515 | CPU 0; progress 115; proc IO 1243; storage 1876 µs |
| 4 | `chunked_256_p4` | 2 | 175.493174 | 56982.273 | 733.594762 / 1132.438232 / 5850.762527 | 2.093051 | 36.731618 | 1,823,846,400 | 0 / 4,146,667,520 | sdb2 200,704 / 4,825,976,832; 79,801 ms | 49,203 / 1,669,372,429 | CPU 1; progress 163; proc IO 3629; storage 4237 µs |
| 5 | `chunked_256_p8` | 2 | 133.196840 | 75076.856 | 652.001851 / 957.783005 / 1101.310151 | 3.177564 | 42.324146 | 1,846,575,104 | 0 / 4,087,513,088 | sdb2 163,840 / 4,723,724,288; 53,207 ms | 48,878 / 1,669,705,929 | CPU 1; progress 7; proc IO 890; storage 1452 µs |
| 6 | `point_get` | 2 | 273.926265 | 36506.174 | 1343.964902 / 2088.070185 / 2491.550427 | 1.278237 | 35.014281 | 1,861,894,144 | 0 / 4,255,088,640 | sdb2 217,088 / 4,971,401,216; 53,726 ms | 49,150 / 1,669,522,339 | CPU 0; progress 5; proc IO 134; storage 782 µs |
| 7 | `chunked_256_p8` | 3 | 144.391161 | 69256.317 | 652.416279 / 979.872203 / 1217.676725 | 2.926951 | 42.262583 | 1,846,804,480 | 0 / 4,080,312,320 | sdb2 221,184 / 4,696,473,600; 64,592 ms | 48,843 / 1,669,364,739 | CPU 1; progress 8; proc IO 1486; storage 2084 µs |
| 8 | `point_get` | 3 | 276.084638 | 36220.777 | 1354.641480 / 2102.392579 / 2513.713343 | 1.275874 | 35.224923 | 1,872,994,304 | 0 / 4,256,157,696 | sdb2 1,388,544 / 4,957,552,640; 53,899 ms | 49,144 / 1,669,424,603 | CPU 0; progress 32; proc IO 174; storage 781 µs |
| 9 | `chunked_256_p4` | 3 | 185.682678 | 53855.320 | 742.266904 / 1131.694630 / 11150.254106 | 1.994212 | 37.029055 | 1,839,894,528 | 0 / 4,152,299,520 | sdb2 0 / 4,809,207,808; 89,793 ms | 49,203 / 1,669,390,821 | CPU 0; progress 9; proc IO 850; storage 2175 µs |

### Lookup and batch-gate stage summaries

下表列每個 mode 的 24 個 stage。時間為每 trial stage p50/p95/p99 的 across-trial median，並列該 mode 三 trial 的 sample-count min–max 及 raw CSV literal scope。PointGet 沒有 group/query-wall samples，列為 N/A。Stage 不可相加成端到端延遲；平行 lookup groups 的 clock duration 互相重疊。

| Stage | point_get p50/p95/p99 median (ms) | point_get samples | point_get literal scope | P4 p50/p95/p99 median (ms) | P4 samples | P4 literal scope | P8 p50/p95/p99 median (ms) | P8 samples | P8 literal scope |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `request.total` | 1343.964902 / 2093.681900 / 2491.550427 | 155808–155808 | `per_sample_request` | 733.594762 / 1131.694630 / 5850.762527 | 155808–155808 | `per_sample_request` | 652.001851 / 957.783005 / 1124.482976 | 155808–155808 | `per_sample_request` |
| `request.admission` | 0.000501 / 0.001803 / 0.006282 | 155808–155808 | `per_sample_request` | 0.000541 / 0.001974 / 0.008085 | 155808–155808 | `per_sample_request` | 0.000601 / 0.002245 / 0.009648 | 155808–155808 | `per_sample_request` |
| `request.enqueue` | 0.000070 / 0.000151 / 0.000231 | 155808–155808 | `per_sample_request` | 0.000070 / 0.000160 / 0.000250 | 155808–155808 | `per_sample_request` | 0.000070 / 0.000161 / 0.000261 | 155808–155808 | `per_sample_request` |
| `request.queue` | 1288.724498 / 2008.912772 / 2389.032564 | 155808–155808 | `per_sample_request` | 703.774458 / 1088.723591 / 5554.051179 | 155808–155808 | `per_sample_request` | 625.778960 / 921.744643 / 1079.961055 | 155808–155808 | `per_sample_request` |
| `request.batch` | 0.141015 / 1.357133 / 1.813769 | 155808–155808 | `per_sample_request` | 0.144851 / 1.429719 / 1.829191 | 155808–155808 | `per_sample_request` | 0.147897 / 1.439027 / 1.755781 | 155808–155808 | `per_sample_request` |
| `request.handler` | 52.699330 / 83.340372 / 102.353510 | 155808–155808 | `per_sample_request` | 27.607295 / 42.245246 / 176.989162 | 155808–155808 | `per_sample_request` | 24.343249 / 35.304219 / 55.429483 | 155808–155808 | `per_sample_request` |
| `request.response` | 0.837249 / 1.947479 / 2.289600 | 155808–155808 | `per_sample_request` | 0.846707 / 1.991642 / 2.343876 | 155808–155808 | `per_sample_request` | 0.845815 / 1.953040 / 2.237433 | 155808–155808 | `per_sample_request` |
| `dispatch.wait` | 0.001823 / 0.002755 / 0.003437 | 4883–4883 | `per_batch_wall` | 0.001773 / 0.002715 / 0.003516 | 4883–4883 | `per_batch_wall` | 0.001764 / 0.002795 / 0.003607 | 4883–4883 | `per_batch_wall` |
| `batch_gate.wait` | 6.103964 / 10.800439 / 13.340237 | 4883–4883 | `per_batch_exclusive_batch_gate_lock_wait` | 6.394934 / 11.758435 / 142.119347 | 4883–4883 | `per_batch_exclusive_batch_gate_lock_wait` | 6.513967 / 11.586864 / 18.883667 | 4883–4883 | `per_batch_exclusive_batch_gate_lock_wait` |
| `keyprep` | 0.090042 / 0.102489 / 0.110287 | 4883–4883 | `per_batch_sum_of_key_encoding` | 0.082305 / 0.231604 / 0.279424 | 4883–4883 | `per_batch_wall` | 0.083978 / 0.243056 / 0.288480 | 4883–4883 | `per_batch_wall` |
| `query.wall` | N/A | 0–0 | `per_batch_wall_from_keyprep_start_to_all_groups_validated` | 10.313056 / 17.960036 / 20.652616 | 4883–4883 | `per_batch_wall_from_keyprep_start_to_all_groups_validated` | 6.751985 / 11.849087 / 13.139298 | 4883–4883 | `per_batch_wall_from_keyprep_start_to_all_groups_validated` |
| `lookup.blocking_pool_wait` | N/A | 0–0 | `per_batch_sum_of_group_waits_overlapping` | 0.080690 / 0.102774 / 0.128752 | 4883–4883 | `per_batch_sum_of_group_waits_overlapping` | 0.091711 / 0.165370 / 1.104621 | 4883–4883 | `per_batch_sum_of_group_waits_overlapping` |
| `lookup.native_get` | 34.885061 / 60.623878 / 73.202560 | 4883–4883 | `per_batch_sum_of_get_or_multiget_call_durations` | 38.072566 / 66.878958 / 75.341835 | 4883–4883 | `per_batch_sum_of_get_or_multiget_call_durations` | 46.346119 / 83.194840 / 90.861379 | 4883–4883 | `per_batch_sum_of_get_or_multiget_call_durations` |
| `lookup.decode_validate` | 0.000000 / 0.000000 / 0.000000 | 4883–4883 | `per_batch_sum_of_record_decode_and_key_validation` | 0.000000 / 0.000000 / 0.000000 | 4883–4883 | `per_batch_sum_of_record_decode_and_key_validation` | 0.000000 / 0.000000 / 0.000000 | 4883–4883 | `per_batch_sum_of_record_decode_and_key_validation` |
| `lookup.submit_to_collection` | N/A | 0–0 | `per_batch_sum_of_overlapping_group_durations` | 38.346849 / 67.415323 / 75.676033 | 4883–4883 | `per_batch_sum_of_overlapping_group_durations` | 46.694848 / 83.544710 / 91.170607 | 4883–4883 | `per_batch_sum_of_overlapping_group_durations` |
| `apply.blocking_pool_wait` | 0.009387 / 0.012814 / 0.016151 | 4883–4883 | `per_batch_legacy_worker_wait_before_interleaved_loop` | 0.011452 / 0.017222 / 0.021179 | 4883–4883 | `per_batch_sequential_apply_worker_wait` | 0.011241 / 0.016090 / 0.023745 | 4883–4883 | `per_batch_sequential_apply_worker_wait` |
| `apply.submit_to_collection` | 45.986364 / 71.845431 / 88.253870 | 4883–4883 | `per_batch_worker_wall_including_pool_wait` | 10.395471 / 12.077623 / 145.370560 | 4883–4883 | `per_batch_worker_wall_including_pool_wait` | 10.567573 / 12.314547 / 18.260870 | 4883–4883 | `per_batch_worker_wall_including_pool_wait` |
| `sequential_apply_build` | 36.568275 / 62.444906 / 75.182454 | 4883–4883 | `per_batch_legacy_interleaved_loop_including_point_reads_and_decode` | 0.989534 / 1.170035 / 1.432635 | 4883–4883 | `per_batch_sequential_apply_and_build_after_prefetch` | 0.995094 / 1.235711 / 1.493989 | 4883–4883 | `per_batch_sequential_apply_and_build_after_prefetch` |
| `write.sync_write_batch` | 9.028277 / 10.599918 / 20.060918 | 4883–4883 | `per_batch_sync_write_call` | 9.277485 / 10.891070 / 144.321113 | 4883–4883 | `per_batch_sync_write_call` | 9.436855 / 11.080154 / 17.096388 | 4883–4883 | `per_batch_sync_write_call` |
| `memory.publish` | 0.246292 / 0.284123 / 0.319428 | 4883–4883 | `per_batch_state_publish_wall` | 0.247564 / 0.287930 / 0.320160 | 4883–4883 | `per_batch_state_publish_wall` | 0.249828 / 0.294913 / 0.340439 | 4883–4883 | `per_batch_state_publish_wall` |
| `lookup_group.blocking_pool_wait` | N/A | 0–0 | `per_group_worker_start_minus_submit` | 0.009808 / 0.015449 / 0.020288 | 39063–39063 | `per_group_worker_start_minus_submit` | 0.011231 / 0.017864 / 0.037750 | 39063–39063 | `per_group_worker_start_minus_submit` |
| `lookup_group.native_get` | N/A | 0–0 | `per_group_multiget_call_wall` | 4.752977 / 8.388003 / 9.808457 | 39063–39063 | `per_group_multiget_call_wall` | 5.829344 / 10.276197 / 11.859995 | 39063–39063 | `per_group_multiget_call_wall` |
| `lookup_group.decode_validate` | N/A | 0–0 | `per_group_record_decode_and_key_validation_sum` | 0.000000 / 0.000000 / 0.000000 | 39063–39063 | `per_group_record_decode_and_key_validation_sum` | 0.000000 / 0.000000 / 0.000000 | 39063–39063 | `per_group_record_decode_and_key_validation_sum` |
| `lookup_group.submit_to_collection` | N/A | 0–0 | `per_group_submit_to_join_completion_wall` | 4.794275 / 8.441683 / 9.865134 | 39063–39063 | `per_group_submit_to_join_completion_wall` | 5.879799 / 10.325179 / 11.918565 | 39063–39063 | `per_group_submit_to_join_completion_wall` |

### Pipeline background stage summaries

標準 pipeline stage CSV 含 25 個 stage：7 個一般 request stages、3 個 history request stages、12 個 active projection/GC/watermark stages、3 個 checkpoint stages。Integrated profile 使用 history 0 與 PerBatch balances；history/checkpoint stages 無樣本，列 N/A。Projection、GC 與 watermark quantiles 只納入 completion time ≤ `client_end` 的事件；settlement 另行計時。

| Raw stage | point_get p50/p95/p99 median (ms) | samples (trial min–max) | P4 p50/p95/p99 median (ms) | samples (trial min–max) | P8 p50/p95/p99 median (ms) | samples (trial min–max) | Profile status |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `projection.read` | 0.312928 / 0.504205 / 0.620879 | 39056–39056 | 0.372198 / 0.726763 / 0.925676 | 39056–39056 | 0.745998 / 1.211701 / 1.422075 | 39056–39056 | Active events completed ≤ `client_end` |
| `projection.apply` | 0.142417 / 0.232057 / 0.387125 | 39056–39056 | 0.155251 / 0.256361 / 0.409848 | 39056–39056 | 0.163176 / 0.278382 / 0.412123 | 39056–39056 | Active events completed ≤ `client_end` |
| `projection.progress_sync` | 0.965188 / 2.124320 / 3.680285 | 39056–39056 | 0.990847 / 3.071346 / 11.009512 | 39056–39056 | 1.021163 / 10.246055 / 11.670621 | 39056–39056 | Active events completed ≤ `client_end` |
| `projection.total` | 1.482888 / 2.738762 / 4.411547 | 39056–39056 | 1.632078 / 3.920828 / 11.668797 | 39056–39056 | 2.105005 / 11.101333 / 12.522647 | 39056–39056 | Active events completed ≤ `client_end` |
| `gc.scan` | 5.967203 / 10.272530 / 12.506588 | 4889–4899 | 6.357084 / 11.015849 / 13.035051 | 4883–4904 | 6.451369 / 11.262020 / 12.766189 | 4885–4920 | Active events completed ≤ `client_end` |
| `gc.delete_build` | 0.048455 / 0.071215 / 0.158472 | 4889–4899 | 0.047100 / 0.059634 / 0.081204 | 4883–4904 | 0.045265 / 0.058129 / 0.067762 | 4885–4920 | Active events completed ≤ `client_end` |
| `gc.sync_write` | 1.716611 / 2.410292 / 3.545580 | 4889–4899 | 1.707320 / 2.445723 / 135.797540 | 4883–4904 | 1.625717 / 2.430452 / 10.838912 | 4885–4920 | Active events completed ≤ `client_end` |
| `gc.total` | 54.111352 / 84.537746 / 104.568958 | 4889–4899 | 29.009283 / 43.853230 / 181.317203 | 4883–4904 | 25.704149 / 36.635764 / 55.878334 | 4885–4920 | Active events completed ≤ `client_end` |
| `watermark.fence_wait` | 779.797394 / 1568.525003 / 1938.523938 | 319–322 | 225.815214 / 580.797768 / 809.254185 | 392–417 | 279.828348 / 691.871379 / 751.791829 | 31–41 | Active events completed ≤ `client_end` |
| `watermark.projection_wait` | 12.283283 / 14.934005 / 722.487762 | 319–322 | 12.607216 / 71.857829 / 3882.753488 | 392–417 | 1020.845684 / 12266.417310 / 12431.136807 | 31–41 | Active events completed ≤ `client_end` |
| `watermark.persist` | 0.953099 / 1.054699 / 2.143336 | 319–322 | 0.982363 / 2.431467 / 11.999858 | 392–417 | 1.028762 / 10.885489 / 17.479536 | 31–41 | Active events completed ≤ `client_end` |
| `watermark.total` | 795.059870 / 1582.165126 / 1951.789956 | 319–322 | 254.035067 / 705.984031 / 4302.042741 | 392–417 | 1102.425629 / 12473.773060 / 12651.102344 | 31–41 | Active events completed ≤ `client_end` |
| `checkpoint.total` | N/A | 0 | N/A | 0 | N/A | 0 | N/A；PerBatch profile 無 checkpoint samples |
| `checkpoint.chunk_sync` | N/A | 0 | N/A | 0 | N/A | 0 | N/A；PerBatch profile 無 checkpoint samples |
| `checkpoint.manifest_sync` | N/A | 0 | N/A | 0 | N/A | 0 | N/A；PerBatch profile 無 checkpoint samples |
| `request.historical_lookup` | N/A | 0 | N/A | 0 | N/A | 0 | N/A；history disabled |
| `request.historical_hit_lookup` | N/A | 0 | N/A | 0 | N/A | 0 | N/A；history disabled |
| `request.historical_miss_lookup` | N/A | 0 | N/A | 0 | N/A | 0 | N/A；history disabled |

### Client-end progress and settlement

Client-end progress 在 workers 仍執行時取樣；下表保留 source projected 與 destination 分開的值，不要求此時相等。九個 trial 的 client-end projection backlog 為 1,408–1,664；GC backlog 為 8,890,480–8,899,952，約 8.89M。Foreground RPS 量的是 request window，不能當作整個 pipeline 能在持續背景工作中維持相同速度的證據。 Projection backlog 是 client-end 的單一端點快照；這些數值不能證明整段負載期間 backlog 都維持在此範圍，也沒有量出 peak backlog。Watermark 的 `projection_wait` 顯示 P8 每 trial p50 median 為 1,020.845684 ms、p99 median 為 12,431.136807 ms，P8 每 trial 有 31–41 個 watermark events；P4 為 392–417 個、point_get 為 319–322 個。這些觀測顯示 durable boundary publication 在負載期間等待 projection；資料不足以歸因到特定 CPU 或裝置原因。

Settlement 要求 source durable projected sequence、destination sequence 與 latest sequence 最終都為 10,150,000；九個 trial 的 final GC prefix 也都到達 10,150,000。Close/reopen 後均驗證餘額 100、sequence 10,150,000、boundary target 不高於 durable projected sequence，且 projection 覆蓋 GC prefix。因 retention 為 500 ms，達成最終安全 prefix 不要求刪掉所有 rows。每 trial 的 `settle_after_client_s` 在 close/reopen 與 integrity verification 之前停止；兩個後續檢查另列。

| Trial | Mode | Client-end latest | Source projected | Destination | Projection backlog | GC prefix at client end | GC backlog | Final GC prefix | Settle after client (s) | Reopen recovery (s) | Integrity (s) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | `point_get` | 10150000 | 10148592 | 10148592 | 1,408 | 1252864 | 8,897,136 | 10150000 | 296.756907 | 3.078376 | 0.337587 |
| 2 | `chunked_256_p4` | 10150000 | 10148336 | 10148592 | 1,664 | 1255424 | 8,894,576 | 10150000 | 308.182828 | 1.632515 | 0.347988 |
| 3 | `chunked_256_p8` | 10150000 | 10148336 | 10148592 | 1,664 | 1255680 | 8,894,320 | 10150000 | 309.884336 | 4.241287 | 0.354991 |
| 4 | `chunked_256_p4` | 10150000 | 10148336 | 10148592 | 1,664 | 1250048 | 8,899,952 | 10150000 | 309.707725 | 1.528783 | 0.335405 |
| 5 | `chunked_256_p8` | 10150000 | 10148336 | 10148592 | 1,664 | 1259520 | 8,890,480 | 10150000 | 300.224864 | 1.581375 | 0.355938 |
| 6 | `point_get` | 10150000 | 10148336 | 10148592 | 1,664 | 1254144 | 8,895,856 | 10150000 | 301.149457 | 4.312730 | 0.337788 |
| 7 | `chunked_256_p8` | 10150000 | 10148336 | 10148592 | 1,664 | 1250560 | 8,899,440 | 10150000 | 301.839715 | 1.659829 | 0.362205 |
| 8 | `point_get` | 10150000 | 10148592 | 10148592 | 1,408 | 1251584 | 8,898,416 | 10150000 | 300.939338 | 1.495095 | 0.324307 |
| 9 | `chunked_256_p4` | 10150000 | 10148336 | 10148592 | 1,664 | 1250560 | 8,899,440 | 10150000 | 274.888766 | 4.058971 | 0.357515 |

**有限負載 drain 指標。** 依 trial 計算 `10,000,000 / (client_wall_s + settle_after_client_s)`，再取 mode 的 median。這是涵蓋有限 10M load 和其 settlement 的 finite-run drain throughput；它不是 steady-state capacity，且不含 setup 與後續 recovery/integrity/reopen 驗證。

| Mode | Median settlement after client (s) | Finite-run drain RPS median |
| --- | --- | --- |
| `point_get` | 300.939338 | 17389.014381 |
| `chunked_256_p4` | 308.182828 | 21712.158082 |
| `chunked_256_p8` | 301.839715 | 22582.472477 |

### Lookup 執行量與尾端變異

每 trial 共 4,883 batches、155,808 個 request latency samples。PointGet 每 trial 執行 10,000,000 native Get calls；Chunked P4/P8 每 trial 各提交 39,063 個 MultiGet groups，查詢 10,000,000 keys。三種 mode 均為 10,000,000 misses、0 hits；P4/P8 各 repetition 的最大 in-flight groups 與 running query jobs 分別為 4 和 8。

| Mode | Native Get/MultiGet calls per trial | Keys looked up per trial | Hits per trial | Misses per trial | Peak in-flight groups | Peak running query jobs |
| --- | --- | --- | --- | --- | --- | --- |
| `point_get` | 10,000,000 | 10,000,000 | 0 | 10,000,000 | 0 | 0 |
| `chunked_256_p4` | 39,063 | 10,000,000 | 0 | 10,000,000 | 4 | 4 |
| `chunked_256_p8` | 39,063 | 10,000,000 | 0 | 10,000,000 | 8 | 8 |

下表由全部九個 `ledger_index_lookup_trial_batches.csv` 逐 batch 計算，列最大值及超過門檻的 batch 數。P4 r2、r3 顯示較長的 `sync_write_batch` 與 `batch_gate.wait`；這兩 trial 的 query.wall maxima 約 23 ms，沒有同步升到相同幅度。這些是可觀察的等待差異；缺少 syscall、per-file、per-thread 與精細裝置追蹤，不能據此指定 fsync、compaction 或裝置原因。

| Trial | Mode | Rep | Max `sync_write_batch` (ms) | WriteBatch >100 ms | WriteBatch >1,000 ms | Max `batch_gate.wait` (ms) | Gate >100 ms | Max `query.wall` (ms) | Query wall >100 ms |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | `point_get` | 1 | 233.490319 | 15 | 0 | 197.175918 | 3 | N/A | N/A |
| 2 | `chunked_256_p4` | 1 | 252.937261 | 12 | 0 | 214.838615 | 9 | 27.180274 | 0 |
| 3 | `chunked_256_p8` | 1 | 139.526197 | 3 | 0 | 268.717867 | 17 | 16.418247 | 0 |
| 4 | `chunked_256_p4` | 2 | 1287.675620 | 57 | 1 | 549.997516 | 55 | 23.301346 | 0 |
| 5 | `chunked_256_p8` | 2 | 142.582346 | 3 | 0 | 218.750923 | 17 | 17.404355 | 0 |
| 6 | `point_get` | 2 | 237.471521 | 11 | 0 | 142.019893 | 5 | N/A | N/A |
| 7 | `chunked_256_p8` | 3 | 473.231709 | 27 | 0 | 368.039912 | 33 | 16.994102 | 0 |
| 8 | `point_get` | 3 | 223.377671 | 8 | 0 | 158.878925 | 8 | N/A | N/A |
| 9 | `chunked_256_p4` | 3 | 617.761206 | 67 | 0 | 1262.750889 | 73 | 23.726641 | 0 |

P4 request p99 median 為 5,850.762527 ms，比 point_get 高 134.82%；P4 三次 trial 的 p99 範圍為 1,297.396932–11,150.254106 ms，因此不能只以 median 掩蓋尾端變異。P8 在本矩陣的 p99 median 為 1,124.482976 ms（範圍 1,101.310151–1,217.676725 ms），同時耗用較多 CPU。三 repetitions 僅描述本次測量，不代表尾延遲的普遍分布。

### Whole-DB IO 與量測限制

下列程序／裝置 IO 與 RocksDB flush、compaction、stall 欄位均為各 trial 的 mode median；IO/storage 起始 snapshot 取自 seed/preflight 後且 queue、background、client task 尚未設置時，結束 snapshot 則在 JoinSet 收集後。這些 delta 不等同狹義 client window 的 IO。

| Mode | Process read median (bytes) | Process write median (bytes) | Device read median (bytes) | Device write median (bytes) | Device busy median (ms) | RocksDB flush write median (bytes) | Compaction read median (bytes) | Compaction write median (bytes) | `stall_us` median |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `point_get` | 0 | 4,255,088,640 | 352,256 | 4,959,916,032 | 53,899 | 651,799,668 | 1,685,011,121 | 1,730,932,162 | 0 |
| `chunked_256_p4` | 0 | 4,146,667,520 | 110,592 | 4,809,207,808 | 79,801 | 652,141,878 | 1,566,976,595 | 1,622,129,591 | 0 |
| `chunked_256_p8` | 0 | 4,085,985,280 | 212,992 | 4,705,210,368 | 53,207 | 652,316,779 | 1,510,814,811 | 1,562,274,369 | 0 |

RocksDB/WAL 數字涵蓋共享 DB 的前景與 background writes。Whole-DB WAL sync median 為 point_get 49,148、P4 49,203、P8 48,859 次；WAL bytes median 與每 trial 範圍見 mode 摘要。九個 trial 的 `stall_us` 均為 0；device read 範圍為 0–1,388,544 bytes，並非 cold-read benchmark。DB/WAL、程序 IO 與目標裝置 IO 的 scope 及 offsets 不同；量測不提供 per-file/syscall/per-thread CPU 證據，也不支持將等待精確歸因到 fsync、compaction、特定裝置或 CPU 飽和。

## Raw artifact links

Raw run root: `benches/data/ledger_pipeline_index_lookup/run-1790871680151272835/`. The archive contains `report.md`; the canonical report is `benches/ledger_pipeline_index_lookup_tokio_report.md`. Archive-report links are relative to this run root; canonical report links are prefixed with `data/ledger_pipeline_index_lookup/run-1790871680151272835/`.

- [Run summary](data/ledger_pipeline_index_lookup/run-1790871680151272835/ledger_index_lookup_summary.csv), [stage percentiles](data/ledger_pipeline_index_lookup/run-1790871680151272835/ledger_index_lookup_stages.csv), [manifest](data/ledger_pipeline_index_lookup/run-1790871680151272835/trial_manifest.csv), [original run metadata](data/ledger_pipeline_index_lookup/run-1790871680151272835/run_metadata.txt), [original runner report incident](data/ledger_pipeline_index_lookup/run-1790871680151272835/report_incident.txt), [report-only recovery record](data/ledger_pipeline_index_lookup/run-1790871680151272835/report_recovery.txt)
- Pipeline artifacts per trial: `ledger_pipeline_summary.csv`, `ledger_pipeline_stages.csv`, `ledger_pipeline_background.csv`, `ledger_pipeline_index_lookup.csv`, `ledger_pipeline_run.log`

Per-trial raw artifact links:

- Trial 1 `point_get` / r1: [index summary](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-01-point_get_r1/ledger_index_lookup_trial_summary.csv), [batch timings](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-01-point_get_r1/ledger_index_lookup_trial_batches.csv), [lookup stages](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-01-point_get_r1/ledger_index_lookup_trial_stages.csv), [pipeline summary](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-01-point_get_r1/ledger_pipeline_summary.csv), [pipeline stages](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-01-point_get_r1/ledger_pipeline_stages.csv), [background events](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-01-point_get_r1/ledger_pipeline_background.csv), [pipeline run log](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-01-point_get_r1/ledger_pipeline_run.log), [lookup parameters](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-01-point_get_r1/ledger_pipeline_index_lookup.csv), [metadata](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-01-point_get_r1/ledger_index_lookup_trial_metadata.txt), [stdout](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-01-point_get_r1/trial-01.stdout.log), [stderr](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-01-point_get_r1/trial-01.stderr.log)
- Trial 2 `chunked_256_p4` / r1: [index summary](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-02-chunked_256_p4_r1/ledger_index_lookup_trial_summary.csv), [batch timings](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-02-chunked_256_p4_r1/ledger_index_lookup_trial_batches.csv), [lookup stages](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-02-chunked_256_p4_r1/ledger_index_lookup_trial_stages.csv), [pipeline summary](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-02-chunked_256_p4_r1/ledger_pipeline_summary.csv), [pipeline stages](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-02-chunked_256_p4_r1/ledger_pipeline_stages.csv), [background events](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-02-chunked_256_p4_r1/ledger_pipeline_background.csv), [pipeline run log](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-02-chunked_256_p4_r1/ledger_pipeline_run.log), [lookup parameters](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-02-chunked_256_p4_r1/ledger_pipeline_index_lookup.csv), [metadata](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-02-chunked_256_p4_r1/ledger_index_lookup_trial_metadata.txt), [stdout](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-02-chunked_256_p4_r1/trial-02.stdout.log), [stderr](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-02-chunked_256_p4_r1/trial-02.stderr.log)
- Trial 3 `chunked_256_p8` / r1: [index summary](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-03-chunked_256_p8_r1/ledger_index_lookup_trial_summary.csv), [batch timings](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-03-chunked_256_p8_r1/ledger_index_lookup_trial_batches.csv), [lookup stages](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-03-chunked_256_p8_r1/ledger_index_lookup_trial_stages.csv), [pipeline summary](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-03-chunked_256_p8_r1/ledger_pipeline_summary.csv), [pipeline stages](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-03-chunked_256_p8_r1/ledger_pipeline_stages.csv), [background events](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-03-chunked_256_p8_r1/ledger_pipeline_background.csv), [pipeline run log](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-03-chunked_256_p8_r1/ledger_pipeline_run.log), [lookup parameters](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-03-chunked_256_p8_r1/ledger_pipeline_index_lookup.csv), [metadata](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-03-chunked_256_p8_r1/ledger_index_lookup_trial_metadata.txt), [stdout](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-03-chunked_256_p8_r1/trial-03.stdout.log), [stderr](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-03-chunked_256_p8_r1/trial-03.stderr.log)
- Trial 4 `chunked_256_p4` / r2: [index summary](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-04-chunked_256_p4_r2/ledger_index_lookup_trial_summary.csv), [batch timings](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-04-chunked_256_p4_r2/ledger_index_lookup_trial_batches.csv), [lookup stages](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-04-chunked_256_p4_r2/ledger_index_lookup_trial_stages.csv), [pipeline summary](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-04-chunked_256_p4_r2/ledger_pipeline_summary.csv), [pipeline stages](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-04-chunked_256_p4_r2/ledger_pipeline_stages.csv), [background events](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-04-chunked_256_p4_r2/ledger_pipeline_background.csv), [pipeline run log](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-04-chunked_256_p4_r2/ledger_pipeline_run.log), [lookup parameters](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-04-chunked_256_p4_r2/ledger_pipeline_index_lookup.csv), [metadata](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-04-chunked_256_p4_r2/ledger_index_lookup_trial_metadata.txt), [stdout](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-04-chunked_256_p4_r2/trial-04.stdout.log), [stderr](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-04-chunked_256_p4_r2/trial-04.stderr.log)
- Trial 5 `chunked_256_p8` / r2: [index summary](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-05-chunked_256_p8_r2/ledger_index_lookup_trial_summary.csv), [batch timings](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-05-chunked_256_p8_r2/ledger_index_lookup_trial_batches.csv), [lookup stages](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-05-chunked_256_p8_r2/ledger_index_lookup_trial_stages.csv), [pipeline summary](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-05-chunked_256_p8_r2/ledger_pipeline_summary.csv), [pipeline stages](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-05-chunked_256_p8_r2/ledger_pipeline_stages.csv), [background events](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-05-chunked_256_p8_r2/ledger_pipeline_background.csv), [pipeline run log](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-05-chunked_256_p8_r2/ledger_pipeline_run.log), [lookup parameters](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-05-chunked_256_p8_r2/ledger_pipeline_index_lookup.csv), [metadata](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-05-chunked_256_p8_r2/ledger_index_lookup_trial_metadata.txt), [stdout](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-05-chunked_256_p8_r2/trial-05.stdout.log), [stderr](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-05-chunked_256_p8_r2/trial-05.stderr.log)
- Trial 6 `point_get` / r2: [index summary](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-06-point_get_r2/ledger_index_lookup_trial_summary.csv), [batch timings](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-06-point_get_r2/ledger_index_lookup_trial_batches.csv), [lookup stages](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-06-point_get_r2/ledger_index_lookup_trial_stages.csv), [pipeline summary](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-06-point_get_r2/ledger_pipeline_summary.csv), [pipeline stages](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-06-point_get_r2/ledger_pipeline_stages.csv), [background events](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-06-point_get_r2/ledger_pipeline_background.csv), [pipeline run log](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-06-point_get_r2/ledger_pipeline_run.log), [lookup parameters](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-06-point_get_r2/ledger_pipeline_index_lookup.csv), [metadata](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-06-point_get_r2/ledger_index_lookup_trial_metadata.txt), [stdout](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-06-point_get_r2/trial-06.stdout.log), [stderr](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-06-point_get_r2/trial-06.stderr.log)
- Trial 7 `chunked_256_p8` / r3: [index summary](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-07-chunked_256_p8_r3/ledger_index_lookup_trial_summary.csv), [batch timings](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-07-chunked_256_p8_r3/ledger_index_lookup_trial_batches.csv), [lookup stages](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-07-chunked_256_p8_r3/ledger_index_lookup_trial_stages.csv), [pipeline summary](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-07-chunked_256_p8_r3/ledger_pipeline_summary.csv), [pipeline stages](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-07-chunked_256_p8_r3/ledger_pipeline_stages.csv), [background events](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-07-chunked_256_p8_r3/ledger_pipeline_background.csv), [pipeline run log](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-07-chunked_256_p8_r3/ledger_pipeline_run.log), [lookup parameters](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-07-chunked_256_p8_r3/ledger_pipeline_index_lookup.csv), [metadata](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-07-chunked_256_p8_r3/ledger_index_lookup_trial_metadata.txt), [stdout](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-07-chunked_256_p8_r3/trial-07.stdout.log), [stderr](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-07-chunked_256_p8_r3/trial-07.stderr.log)
- Trial 8 `point_get` / r3: [index summary](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-08-point_get_r3/ledger_index_lookup_trial_summary.csv), [batch timings](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-08-point_get_r3/ledger_index_lookup_trial_batches.csv), [lookup stages](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-08-point_get_r3/ledger_index_lookup_trial_stages.csv), [pipeline summary](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-08-point_get_r3/ledger_pipeline_summary.csv), [pipeline stages](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-08-point_get_r3/ledger_pipeline_stages.csv), [background events](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-08-point_get_r3/ledger_pipeline_background.csv), [pipeline run log](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-08-point_get_r3/ledger_pipeline_run.log), [lookup parameters](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-08-point_get_r3/ledger_pipeline_index_lookup.csv), [metadata](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-08-point_get_r3/ledger_index_lookup_trial_metadata.txt), [stdout](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-08-point_get_r3/trial-08.stdout.log), [stderr](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-08-point_get_r3/trial-08.stderr.log)
- Trial 9 `chunked_256_p4` / r3: [index summary](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-09-chunked_256_p4_r3/ledger_index_lookup_trial_summary.csv), [batch timings](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-09-chunked_256_p4_r3/ledger_index_lookup_trial_batches.csv), [lookup stages](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-09-chunked_256_p4_r3/ledger_index_lookup_trial_stages.csv), [pipeline summary](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-09-chunked_256_p4_r3/ledger_pipeline_summary.csv), [pipeline stages](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-09-chunked_256_p4_r3/ledger_pipeline_stages.csv), [background events](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-09-chunked_256_p4_r3/ledger_pipeline_background.csv), [pipeline run log](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-09-chunked_256_p4_r3/ledger_pipeline_run.log), [lookup parameters](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-09-chunked_256_p4_r3/ledger_pipeline_index_lookup.csv), [metadata](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-09-chunked_256_p4_r3/ledger_index_lookup_trial_metadata.txt), [stdout](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-09-chunked_256_p4_r3/trial-09.stdout.log), [stderr](data/ledger_pipeline_index_lookup/run-1790871680151272835/trials/trial-09-chunked_256_p4_r3/trial-09.stderr.log)


