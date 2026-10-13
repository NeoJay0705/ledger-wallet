# 原始資料警示與執行事件

本文整理正式 campaign 前的停止原因與 Scylla 啟動日誌訊息。整理資料來自封存原始檔；不改寫 service workload counters，也不量化環境訊息的效能影響。

## 正式 run 前的兩次停止

| Run | 狀況 | 處置與有效性 |
|---|---|---|
| run-20261013T121329Z-2834e7a9e5 | 完成 2/9 後停止。D64 workload 尚未開始，readiness gate 的 pg_isready 沒有指定 TCP host，誤把暫時的 Unix-socket init server 當成 TCP server。 | 不列入正式結果。後續強制 TCP readiness，並檢查 container logs 與 observer/collector readiness。 |
| run-20261013T122913Z-072162c586 | 完成 2/9 個有效案例後停止。D64 執行 1,024,000 requests、workload errors=0 並通過完整 verification，但 monitor interval 記錄 2.673s、2.642s，超過 2.5s 門檻。 | 不列入正式結果。後續依 capture 開始時間排程 +1s；若 capture 超時則結束後立即開始；保留 2.5s 門檻且不做 catch-up burst。 |

後續另執行經 root 批准的全新九案例 campaign。正式 campaign 與 run manifest 報告 9/9 完成及 Supervisor exit code 0。九個 service result 都報告 1,024,000 replies、workload errors=0，並通過全量驗證與 owned cleanup。前兩個 run 的案例未併入正式結果。

## Scylla container 啟動訊息

三次正式 Scylla 測試都記錄以下環境訊息：mbind shard memory 操作因 Operation not permitted 失敗；perf_event stall detector 無法建立並降級為 POSIX timer；/run/udev/data 無法開啟，diskstats collector 停用 udev device properties；RF=1 低於建議閾值 3。RF=1 是本次比較條件。

Service 結果仍記錄 1,024,000/1,024,000 applied，workload errors=0，driver errors、retries、request timeouts、connection timeouts 均為 0。啟動日誌 warnings/errors 與 workload counters 是不同觀測面；本次沒有量化上述環境訊息對吞吐量或延遲的影響。

## 原始來源與 SHA-256

| 原始封存內路徑 | Bytes | SHA-256 |
|---|---:|---|
| wallet-db-compare/runs/run-20261013T121329Z-2834e7a9e5/campaign.json | 1755565 | fc68ff04f9bae2187d9500afe56432b9e337405610dce22be0f6221b7a1eb686 |
| wallet-db-compare/runs/run-20261013T121329Z-2834e7a9e5/postgres-cases/pg-calibration-D64/case-result.json | 6990 | c374b75aea15242578f6a3fc2fa307d10a318158abd2a9e398e60b806b80fa7c |
| wallet-db-compare/runs/run-20261013T122913Z-072162c586/campaign.json | 1760987 | 5399dd778d876ecc6b2304f1b33481c2bcf10df89362ac12c886daa3b491c2a7 |
| wallet-db-compare/runs/run-20261013T122913Z-072162c586/postgres-cases/pg-calibration-D64/case-result.json | 567127 | 0197102d78ae6399cd61b95a827021f76e510e620a6f05239c47aa01b22a39aa |
| wallet-db-compare/runs/run-20261013T124123Z-3216cf07f3/campaign.json | 5287920 | 243bd6d4494841732594e401369cbc59d3947bd65d75118cd20211fd9d608c02 |
| wallet-db-compare/runs/run-20261013T124123Z-3216cf07f3/trials/trial-004-S2-C512-P3-D6-round-robin-RF1-R1-W1-N1-wallet-memory-lwt-v1-rep1/logs/nodea.container.log | 118175 | 8c724eefa369b9fe759eeef4de6e0301db5c9fe7ff2f55bfe3baca02b0dc6dc2 |
| wallet-db-compare/runs/run-20261013T124123Z-3216cf07f3/trials/trial-007-S2-C512-P3-D6-round-robin-RF1-R1-W1-N1-wallet-memory-lwt-v1-rep2/logs/nodea.container.log | 118184 | 446d32874f74917ba42cc1b8cbad3ad3c8d9705349b5a91fc815a6ef25fc215f |
| wallet-db-compare/runs/run-20261013T124123Z-3216cf07f3/trials/trial-008-S2-C512-P3-D6-round-robin-RF1-R1-W1-N1-wallet-memory-lwt-v1-rep3/logs/nodea.container.log | 118185 | 46a4fb881500e0752ede0eaebdc20dcc096cd5c1f4265191ea332b0e1f8327a2 |
