# 離線測試證據 fixtures

以下 PostgreSQL 18 fixtures 逐 byte 取自凍結計畫 `target/wallet-db-compare/plans/plan-20261013T120948Z-296254efc2/source-snapshot/repo/tests/fixtures/`：

- `postgres18_schema_catalog_raw.json` — SHA-256 `ee66f94ec51e39fc9838d72671e4c23af500a2fb3b7925c044fe2074f89ad8a4`。
- `postgres18_settings_raw.json` — SHA-256 `975bbed9399054175aacb9ad6c0a7f5a7267da91cb0aac28a5f9d15a03c11d86`。
- `postgres18_stats_raw.json` — SHA-256 `ffedf20c2710cf3cbe2c232df8f0b29c8a1cac3c8cd3ced53345a477d6f58932`。
- `postgres18_pss_direct_postgres.json` — SHA-256 `7d233bd386b7a6c956350f72bed8c0bea99a8bfa3a13424c3e3a906bc5f1ff0b`。

`scylla_periodic_missing_commitlog_series.json` 是 Scylla periodic C512 setup-fix trial 的 runtime validation 證據，逐 byte 取自 `target/scylla-wallet/pilot-jobs/calibrated-single-node-rf1-s2-periodic-c512-setup-fix-20261013T015444Z/cases/full-S2-C512-P3-D6-N1-RF1-R1-W1-periodic10000/runs/run-20261013T020501Z-9ef70a76/trials/trial-001-S2-C512-P3-D6-round-robin-RF1-R1-W1-N1-wallet-memory-lwt-v1-rep1-commitlog-periodic-10000ms/io-properties-runtime-validation.json`，SHA-256 `325243fa7d18c098723760d6badb37e8b42205931bda9bc6e6876c622ffc6a3d`。
