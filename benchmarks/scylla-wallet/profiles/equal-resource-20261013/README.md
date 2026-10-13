# 等資源比較主機 I/O profile

本 profile 的檔案逐 byte 複製自既有校準證據：

- `io_caps.json`：`target/wallet-db-compare/qualification/io_caps.json`，SHA-256 `c95f1a8ba5204051fe48e61bd05333ef10122f1b9e0ad78c8a41ad1bbf9b87fc`。
- `io_properties.yaml`：`target/scylla-wallet/iotune-calibration-jobs/20261012T133244Z-6180f531/budget/io_properties.yaml`，SHA-256 `12bf0b79c4520ca6d00183ca14b87d321b9221b687cc6e889619876f5b87b2bf`。

這組設定對應校準主機的 `/dev/sdb`（8:16）及其 ext4 資料分割區 `/dev/sdb2`（8:18）。其他主機或磁碟須重新校準 I/O 上限與 Scylla `io_properties.yaml`，不可直接沿用。

封存的歷史計畫與來源 hash 維持原樣。將相同 profile bytes 納入 repo 後，新的 packaging source hash 會不同；這項封裝變更不要求重跑 benchmark。
