# Tokio 同機 gRPC Echo：Kernel 層量測與瓶頸歸因

## 結論摘要

此測試只涵蓋同一台主機上兩個 Tokio 程序以明文 IPv4 loopback `127.0.0.1` 傳送 unary gRPC echo。`lo` 是 `noqueue`，資料不經實體 NIC 或實體鏈路；本報告不把 loopback bytes/packets 換算成 NIC line rate，也不把 TCP/kernel 時間分攤成每個 RPC 的 latency。

低負載 100 RPS 時，兩端 CPU 約 0.018/0.007 cores（128 B）及 0.021/0.008 cores（4096 B），E2E p99 約 0.27–0.30 ms。C=256 時增加獨立 TCP channel 明顯提高完成速率：128 B 從 ch1 的 78.0k RPS 到 ch8 的 148.4k；4096 B 從 44.3k 到 ch8 的 86.4k。兩端核心等效值同時升至約 2.85–2.95 cores，表示多通道改善了整體吞吐，但沒有證據能把改善歸因於單一 kernel、HTTP/2 或網路子系統。

TCP snapshot 顯示兩個方向都曾有非零 Send-Q/Recv-Q；4096 B、ch1 的單 flow queue maxima 明顯大於 ch8 每條 flow 的 maxima，並且 client `rwnd_limited` 在三次中有 769 ms、0 ms、1,213 ms 的 phase-window delta。這表示接收窗口背壓偶爾出現，但不是每次持續發生，也不足單獨解釋 ch1 的吞吐上限。`perf stat` 量到 4096 B 約一半 cycles 在 kernel mode；同時每 RPC context-switch 數由 ch1 到 ch8 明顯下降。這些是程序/事件總量，不是逐 RPC 成本，也不能指明 syscall 或 scheduler 的具體原因。

沒有觀察到單一 CPU 的 softirq 接近滿載：C=256 ch8 時，每 trial 最忙核心的 softirq 百分比中位數為 8.4%（128 B）與 13.3%（4096 B），最大觀察值約 13.5%。但程序 CPU 約 3 cores、loopback 封包與 queue 都隨負載增加。此結果支持「瓶頸在同機 application/runtime/TCP/loopback/排程鏈路中，沒有實體 NIC bottleneck 證據」；不能據此判定鏈路內的唯一瓶頸。

Perf stack 顯示 128 B ch1 的直接 samples 中 `Mutex::lock_contended` 約占 client 7.32%、server 7.53%，ch8 降至 1.79%、0.95%；ch8 的 samples 則較多落在 `memmove`、allocator 和 HTTP/2 符號。這與多通道降低部分 user-space contention 相符，但不識別該 mutex 的實際呼叫來源，也不證明因果。Kernel symbols 受 `/proc/kallsyms` 限制而顯示 `[unknown]`；CAP_PERFMON 也無法讀取 tracefs syscall/sched events，因此 syscall、`sched_switch` 和精確 H2-vs-kernel 成本仍未歸因。

## 執行與有效性

- 24 次 kernel diagnostic：128/4096 B 固定 100 RPS baseline 各 3 次；C=256、ch1/ch4/ch8 各 3 次，共 18 次飽和測試。
- 18 次同設定 kernel-off C=256 對照；診斷前後另各 4 次 ch1/ch8 bracket，用來觀察時間漂移。
- Perf stat：128/4096 B、C=256、ch1/ch8 各 3 次，先有一批一般輸出，再以獨立 batch 重跑 `stat -v` 收集 time-enabled/time-running；兩批都保留且不混成六次重複。Perf record 對四種 payload/channel 組合各跑一個代表 trial。
- 所有正式 case 維持原本 3 秒 strict idle preflight、CPU <=10%、disk busy <=5%、MemAvailable >=512 MiB、loopback 背景門檻與 port 閒置條件，沒有放寬。正式 socket/proc 與 perf trial 全部通過；smoke 使用短 warmup/measurement，但沒有縮短 strict preflight。
- Client completion timestamp、CPU endpoint、`PHASE_END` 均在 latency merge/sort 前固定。Parent 收到 `PHASE_END` 後先發 SIGINT 停止 perf，再取 `/proc` 與 TCP/ss boundary，最後才 wait/report perf 並送 ACK。Perf finalize 不會延長 client completion endpoint 或 RPS/latency window。Kernel on/off smoke 均正常退出。

### 主要結果

以下是三次 trial 各自指標的中位數。Latency 為 E2E p50/p95/p99，單位 ms；CPU 是 client/server process core equivalent。

| Payload | 負載 | Channels | RPS | E2E p50 / p95 / p99 (ms) | Client / server CPU (cores) |
|---:|---|---:|---:|---:|---:|
| 128 B | fixed 100 RPS | 1 | 100.0 | 0.118 / 0.169 / 0.302 | 0.018 / 0.007 |
| 4096 B | fixed 100 RPS | 1 | 100.0 | 0.132 / 0.189 / 0.268 | 0.021 / 0.008 |
| 128 B | C=256 | 1 | 78,033.9 | 3.068 / 5.374 / 6.200 | 1.830 / 1.848 |
| 128 B | C=256 | 4 | 138,148.7 | 1.773 / 2.997 / 3.814 | 2.656 / 2.778 |
| 128 B | C=256 | 8 | 148,382.4 | 1.689 / 2.838 / 3.443 | 2.847 / 2.912 |
| 4096 B | C=256 | 1 | 44,263.7 | 5.804 / 8.131 / 9.409 | 1.736 / 1.653 |
| 4096 B | C=256 | 4 | 84,739.5 | 2.901 / 4.847 / 5.965 | 2.814 / 2.892 |
| 4096 B | C=256 | 8 | 86,391.7 | 2.900 / 5.060 / 6.007 | 2.893 / 2.947 |

### Kernel-on 與 kernel-off 對照

以下列出 dense `ss -tinmH` diagnostic 與 `--kernel-sample-ms 0` 對照的 RPS 中位數。控制組分區連續執行，前後 bracket 顯示主機/時間漂移，因此這些差值只作同設定參考，不作監控成本的精確因果估計。RPS 差異方向混合，沒有一致的 sampler slowdown 訊號。舊式 `results.csv` queue maxima 使用約每秒一次 ss；dense group 使用 100 ms 採樣，兩者的 queue maxima 不可互相比較。

| Payload | Channels | Dense diagnostic RPS | Kernel-off RPS | Dense diagnostic p99 (ms) | Kernel-off p99 (ms) |
|---:|---:|---:|---:|---:|---:|
| 128 B | 1 | 78,033.9 | 77,070.3 | 6.200 | 6.231 |
| 128 B | 4 | 138,148.7 | 136,553.9 | 3.814 | 3.809 |
| 128 B | 8 | 148,382.4 | 144,841.8 | 3.443 | 3.575 |
| 4096 B | 1 | 44,263.7 | 44,401.5 | 9.409 | 9.477 |
| 4096 B | 4 | 84,739.5 | 85,379.2 | 5.965 | 6.029 |
| 4096 B | 8 | 86,391.7 | 89,308.4 | 6.007 | 5.866 |

Kernel-off brackets的RPS前後變化為：128 B ch1 77,171.5→82,552.5、ch8 148,570.0→141,874.0；4096 B ch1 43,328.4→45,181.8、ch8 90,264.1→85,499.4。這些變化達約 4–7%，大於多數 dense-vs-off 差異，故本報告不把兩個 run block 間的細小差值歸因給 sampler。

## Dense socket 與 TCP_INFO

每次 diagnostic trial 在 phase 內約每 100 ms 執行一次 `taskset -c 6,7,14,15 ss -tinmH`，200 個 phase 內樣本、0 次 sample error；另有 phase-end boundary 樣本。每條 TCP flow 由 server port 分出 client 與 server endpoint，兩端分別記錄 Send-Q、Recv-Q、`skmem`、raw TCP_INFO 與累積 counter delta。`tcp_snapshot_group_medians.csv` 的 queue maxima 是每次 trial 先跨該端所有 channels 取最大，再對 3 repeats 取中位數；它是取樣 maxima，不是全程最大 queue。

| Payload | Channels | Client Send-Q / Recv-Q (B) | Server Send-Q / Recv-Q (B) |
|---:|---:|---:|---:|
| 128 B | 1 | 22,498 / 32,809 | 33,809 / 39,800 |
| 128 B | 4 | 12,864 / 12,096 | 12,160 / 12,864 |
| 128 B | 8 | 6,432 / 6,080 | 6,080 / 6,432 |
| 4096 B | 1 | 422,337 / 299,440 | 231,560 / 419,666 |
| 4096 B | 4 | 266,752 / 249,540 | 161,265 / 266,765 |
| 4096 B | 8 | 133,389 / 133,024 | 133,037 / 133,408 |

Queue 非零並非罕見：4096 B ch1 的 client Send-Q/Recv-Q 分別在約 50–53%/60–65% phase 內 endpoint snapshots 非零；server 約 43.5–49%/65–72% 非零。Ch8 聚合 8 條 flow 後，client send/recv 非零比例約 56–61%/31–35%，server 約 28–32%/72–77%。多通道降低了部分每 flow maxima，但總 flow 數增加；不能由任一快照最大值推論採樣間從未積壓。

4096 B ch1/ch8 的 `skmem` snapshot maxima（每欄 bytes、每 trial 跨該端 flows 取最大後再取三次中位數）：

| Channels | Endpoint | r | w | rb | tb |
|---:|---|---:|---:|---:|---:|
| 1 | client | 337,022 | 427,137 | 1,335,972 | 2,626,560 |
| 1 | server | 436,946 | 285,320 | 534,093 | 2,626,560 |
| 8 | client | 166,637 | 196,736 | 729,614 | 2,626,560 |
| 8 | server | 196,736 | 165,677 | 626,048 | 2,626,560 |

`rwnd_limited_ms` 由 first in-phase 到 phase-end boundary 的 TCP_INFO 毫秒累積差值；百分比以該 socket 約 20.04 秒的觀察 interval 為分母，不採用 `ss` 的 connection-lifetime 括號百分比。4096 B ch1 client 三次分別為 **769 ms (3.84%)、0 ms、1,213 ms (6.05%)**。Ch4/8 可用 endpoint 欄位的差值為 0；部分 server row 未提供此欄，空值代表未觀察到欄位，不代表零。Client endpoint 的 `rwnd_limited` 表示 client 作為 sender 時受遠端 server advertised receive window 限制，對應 **client→server request send** 方向；它和偶發背壓相符，但 repeat 間變化且 phase 佔比有限，不能單獨解釋 throughput 差距。`sndbuf_limited` 欄在正式 ss 輸出未提供，未能量化。Raw first/end、delta 與每條 flow 的 window/offset 在 [TCP duration derived CSV](grpc_echo_kernel_data/analysis/tcp_duration_deltas.csv) 和原始時間序列都有保留。

TCP_INFO `busy_ms` 是 socket 的累積 busy 狀態時間，不是 CPU busy time；例如低負載 server baseline 的差值也會近似隨 wall interval 增長。它不可與 `/proc/stat` 的 CPU busy 或 process `task-clock` 混用，也不作 CPU 瓶頸判斷。

TCP byte counters 同樣是每 socket 累積 delta，分 client/server 方向，不是 RPC 數。4096 B client sender（client→server）三次 trial 的 bytes_sent 中位數與 bytes_retrans 中位數如下：

| Channels | Median bytes sent | Median bytes retrans | Retrans / sent | Retrans delta range |
|---:|---:|---:|---:|---:|
| 1 | 3,690,192,434 | 1,662,754 | 0.0451% | 0–2,365,790 B |
| 4 | 7,060,036,746 | 153 | 0.0000022% | 135–1,341 B |
| 8 | 7,198,695,073 | 45 | 0.0000006% | 18–126 B |

Server→client 的對應 bytes_retrans delta 為零；128 B ch4/ch8 的 client bytes_retrans 也有小的非零 delta（中位數 1,741/1,034 B）。因此報告不把 retransmission 描述為全程為零。這些 counter 由 TCP stack 對整個 flow 累計，不是 gRPC retry 或每 RPC loss。

## Perf stat 與 CPU profile

Perf 專用 ELF 為 `/home/neojhou/repos/ledger-wallet/target/grpc-echo-perf`，`getcap` 顯示 `cap_perfmon=ep`；`/usr/bin/perf` 是 shell wrapper，沒有對 wrapper 設 capability。使用此 ELF 的 perf permission preflight 對七個指定 event 都取得有效數值，stderr 無 permission/access denied。原先無能力時的失敗 preflight 與後續通過紀錄均保留。Verbose stat 每個 event 的 time-running 都是 **100%**；沒有 `<not counted>`、`<not supported>` 或 `n/a`。Cycles user/kernel 百分比以下是 cycles:u 與 cycles:k 的觀測值比例，不是 CPU time 比例。

| Payload | Channels | Process | task-clock cores | context switches / s | context switches / completed RPC | kernel cycles / (user+kernel) |
|---:|---:|---|---:|---:|---:|---:|
| 128 B | 1 | client | 1.834 | 25,440 | 0.320 | 25.3% |
| 128 B | 1 | server | 2.018 | 32,019 | 0.404 | 30.3% |
| 128 B | 8 | client | 2.829 | 13,106 | 0.094 | 34.3% |
| 128 B | 8 | server | 2.887 | 10,826 | 0.077 | 30.6% |
| 4096 B | 1 | client | 1.781 | 35,431 | 0.830 | 50.1% |
| 4096 B | 1 | server | 1.718 | 26,357 | 0.623 | 50.9% |
| 4096 B | 8 | client | 2.884 | 10,485 | 0.113 | 52.7% |
| 4096 B | 8 | server | 2.922 | 8,289 | 0.088 | 50.0% |

各列為 3 repeats 的中位數。從 ch1 到 ch8，RPS 增加時 context switches/s 與 context switches/RPC 都下降；kernel-cycle fraction 則沒有一致的方向（例如 128 B client 增加、4096 B 兩端近似持平）。這可支持「每 RPC scheduling/context-switch 次數下降」的描述；不能把 kernel cycles 占比直接當成 network bottleneck 或 RPC latency。

Perf monitor attach/start offset 相對 PHASE_START 約 +0.25 至 +0.65 ms，SIGINT/end offset 相對 PHASE_END 約 +0.052 至 +0.156 ms；每角色每 trial 的 status、實際命令、counter、enabled/running ns 與 stderr 都在 perf artifacts。`task-clock` 是目標 process 各執行緒 CPU time 總和。`cycles:k` 是程序被計數的 kernel-mode cycles；兩者都不是逐 RPC latency。

49 Hz DWARF CPU profiles 每端有約 1,600–2,800 samples，perf report status 0 且沒有 lost samples。128 B ch1 的 `Mutex::lock_contended` direct sample 為 client 7.32%、server 7.53%；ch8 對應 1.79%、0.95%。其餘高位符號包含 `__memmove_avx_unaligned_erms`、`_int_malloc`/`malloc`、HTTP/2/Hpack 函式。這是四個 representative trial 的 sample profile，record 本身可能增加負擔，不用它比較 RPS。Perf stderr 警告 `/proc/kallsyms` 受限，所以 `[unknown]` kernel samples 不作函式歸因，也不要求 CAP_SYSLOG。

## Loopback、softirq 與瓶頸鏈

`/proc/stat` per-core 最大 softirq 百分比（每次 trial 先取最忙核心，再對 3 次取中位數）為：

| Payload | Channels | Hottest-core softirq median | 三次中最大觀察值 | Hottest-core total busy median |
|---:|---:|---:|---:|---:|
| 128 B | 1 | 2.06% | 2.11% | 46.6% |
| 128 B | 4 | 4.76% | 4.86% | 74.6% |
| 128 B | 8 | 8.37% | 8.65% | 87.6% |
| 4096 B | 1 | 4.13% | 4.33% | 46.5% |
| 4096 B | 4 | 10.84% | 11.17% | 86.0% |
| 4096 B | 8 | 13.28% | 13.47% | 91.6% |

Hottest-core total busy 包含 user/kernel/softirq 工作，不能把它等同 softirq。Ch8 時 worker process 合計已接近配置的 3 cores，但沒有單一核心顯示 softirq 接近 100%。Loopback interface counters 的 RX 與 TX 在此拓樸中相等；表列出各自單邊的 median bytes/packets，不把 RX+TX 加總解讀成物理鏈路流量：

| Payload | Channels | Loopback RX bytes | Loopback TX bytes | RX packets | TX packets |
|---:|---:|---:|---:|---:|---:|
| 128 B | 1 | 643,852,941 | 643,852,941 | 712,421 | 712,421 |
| 128 B | 8 | 1,349,118,052 | 1,349,118,052 | 3,674,336 | 3,674,336 |
| 4096 B | 1 | 7,486,726,337 | 7,486,726,337 | 2,184,687 | 2,184,687 |
| 4096 B | 8 | 14,695,266,674 | 14,695,266,674 | 5,938,452 | 5,938,452 |

依 `tmp_net_test.md` 的鏈路模型，這些資料可見到 process CPU、TCP send/receive queue、TCP flow-control 計時、loopback packet/byte 與 per-core softirq。此主機 `lo` 使用 noqueue，測試沒有物理 NIC TX ring、qdisc backlog、實體 link 或 receiver NIC 指標。Queue 與 `rwnd_limited` 在部分單 flow trial 提供背壓證據；loopback packet 增長和 softirq 上升說明 host networking 工作隨流量增加，但 softirq 沒有單核飽和證據。Perf cycles 顯示相當比例的 process kernel-mode 執行，卻未提供 syscall/softirq 函式符號。因此不能在 sender application、HTTP/2/runtime、TCP/loopback kernel 路徑之間做唯一 bottleneck 判定，也不能以 loopback counters 宣稱 physical NIC bottleneck。

既有 `results.csv` 仍保留 client/server `/proc/PID/io` byte counters 與 preflight 的目標 block-device busy 監控；本次 78 個 trial 的兩端 `read_bytes`、`write_bytes` start/end 均為 0。這與 Echo 業務路徑無磁碟 I/O 一致；socket 資料面由上列 loopback 與 TCP 指標觀察。

## 採樣擾動與限制

100 ms `ss` sampler 每次 diagnostic trial 約執行 200 次，child `ss`/`taskset` process 約使用 0.21–0.26 CPU cores；parent sampler CPU 約 0.004 cores 以下，monitor wall duty 約 21.6–26.4%。`ss` 被固定於原 benchmark worker 未分配的 CPU 6/7 與 SMT sibling 14/15，仍會消耗 host CPU/記憶體頻寬並可能改變排程。故 kernel-off 控制與前後 bracket 必須一起看；本次 bracket 顯示約 4–7% 時間漂移，dense-vs-off RPS 差值方向又混合，不能宣稱精確 overhead。

Perf record 比 stat 更重；各自只跑四個 representative trial，所有 records 都保留，但該 batch 不用來比較 kernel-off RPS。Perf tracepoint 事件雖由 `perf list` 列出，CAP_PERFMON 下 `perf stat -p` 仍因 `No permissions to read /sys/kernel/tracing/events/syscalls/sys_enter_sendmsg` 失敗（status 129）。沒有 mount/remount tracefs、修改全機 sysctl、執行 bpftrace 或申請 CAP_SYSLOG；失敗命令與完整 stderr 在 [tracepoint probe log](grpc_echo_kernel_data/tracepoint_probe.log)。

`t0-t1` 與 `t2-t3` 仍包含 tonic/HTTP/2 framework、本機 queue、排程與 loopback transport。程序的 perf CPU counters 不可分派到單一 RPC；t1-t2 handler duration 也不代表 server process 全部 CPU 成本。要進一步分辨 H2/runtime 與具體 kernel syscall/scheduler 熱點，需要一個可讀 tracefs/perf tracepoint 的安全配置，或可解析 kernel symbols 的 profile；目前兩項都未具備。

## 可追溯資料與分析

- [設計文件](../docs/01-15.development-design-grpc-echo-kernel-attribution.md)；[所有測試命令](grpc_echo_kernel_data/commands.md)；[run manifest](grpc_echo_kernel_data/analysis/manifest.txt)。
- [78-trial 彙整](grpc_echo_kernel_data/analysis/trial_summaries.csv)、[每組中位數](grpc_echo_kernel_data/analysis/group_medians.csv)、[per-core CPU raw summary](grpc_echo_kernel_data/analysis/per_core_cpu.csv)、[hottest core summary](grpc_echo_kernel_data/analysis/per_core_hottest.csv)。
- [ss monitor CPU/wall cost](grpc_echo_kernel_data/analysis/ss_monitor_cost.csv)、[TCP queue snapshot maxima](grpc_echo_kernel_data/analysis/tcp_snapshot_group_medians.csv)、[queue nonzero snapshot share](grpc_echo_kernel_data/analysis/tcp_queue_sample_occupancy.csv)、[skmem endpoint summary](grpc_echo_kernel_data/analysis/tcp_snapshot_endpoint_summary.csv)。
- [TCP duration deltas](grpc_echo_kernel_data/analysis/tcp_duration_deltas.csv)、[TCP counter deltas](grpc_echo_kernel_data/analysis/tcp_counter_deltas.csv)、[counter group medians](grpc_echo_kernel_data/analysis/tcp_counter_group_medians.csv)。正式 dense raw series 位於 [diagnostic baseline](grpc_echo_kernel_data/diagnostic_baseline/run-1790522417558086197-48161-0/) 及 [diagnostic saturation](grpc_echo_kernel_data/diagnostic_saturation/run-1790522592015262494-52216-0/) 的每 trial `*.kernel_tcp.csv`。
- [verbose perf events 與 time-running](grpc_echo_kernel_data/analysis/perf_stat_events.csv)、[perf stat group medians](grpc_echo_kernel_data/analysis/perf_stat_group_medians.csv)。Verbose batch run 為 `run-1790525579635643380-33277-0`；第一批非 verbose perf stat `run-1790523407189597516-2399-0` 也另行保留。代表性 `.perf.data`、stderr 與 report 在 `perf_record_targeted/run-1790523828994978554-8267-0/`。
- Kernel-off 飽和 control 為 `kernel_off_saturation/run-1790524070414027150-11677-0/`；診斷前後 bracket 分別為 `kernel_off_bracket_before/run-1790522295855558216-46450-0/` 與 `kernel_off_bracket_after/run-1790524594159299566-19046-0/`。每個 run 目錄均保留 `results.csv`、`run.txt`、`preflight.log`、`perf_preflight.log`（perf off 時記為 skipped）與 trial diagnostics。
- 分析重算程式為 [analyze_grpc_echo_kernel.py](analyze_grpc_echo_kernel.py)。它不改寫原始試驗 CSV；新增 `analysis/` CSV 可從指定 run 目錄重建。舊的 [grpc_echo_loopback_report.md](grpc_echo_loopback_report.md) 與既有基準資料未覆寫。
