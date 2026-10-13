# Wallet DB 比較證據索引

此 Git 證據包收錄九個案例的原始 service result/events、run manifest、approved plan，以及從封存原始資料產生的案例與資源摘要。`campaign-summary.json`、`cases.csv`、`resource-samples.csv`、`resource-summary.json` 和 `warnings/incidents.md` 是摘要或衍生資料，不是完整 runner/case JSON。完整來源、qualification、先前停止的 runs、plans、launch records、logs 和其他原始檔保存在下列本機封存。

<a id="raw-archive"></a>
## 原始封存

- 本機路徑：`/home/neojhou/benchmark-archives/ledger-wallet/wallet-db-compare/wallet-db-compare-20261013T124123Z-3216cf07f3.tar.gz`
- 壓縮檔大小：45614492 bytes；SHA-256：`d0977b40c7b1f75dadc57ede978538669941aff76bc2b446f29324b6970c3c19`
- 內容：2033 個一般檔案，解壓後 176228422 bytes，根目錄為 `wallet-db-compare/`
- Git 內原始檔清單：`original-file-manifest.csv`（376286 bytes；SHA-256：`1a119472ce6c01f9566e649d6c731561a80b55a9626a929aa5d7fddcd1419a2f`）
- 可用位置：僅此主機，沒有共享 URL。

逐檔核對封存內容與 Git 內的路徑、大小及 SHA-256 清單：

~~~sh
python3 benches/archive_wallet_db_evidence.py verify --bundle benches/data/wallet_db_compare/run-20261013T124123Z-3216cf07f3 --archive /home/neojhou/benchmark-archives/ledger-wallet/wallet-db-compare/wallet-db-compare-20261013T124123Z-3216cf07f3.tar.gz
~~~

解壓後重新產生摘要，並逐 byte 比對 Git 內輸出：

~~~sh
mkdir -p /tmp/ledger-wallet-wallet-db-compare-restore && tar -xzf /home/neojhou/benchmark-archives/ledger-wallet/wallet-db-compare/wallet-db-compare-20261013T124123Z-3216cf07f3.tar.gz -C /tmp/ledger-wallet-wallet-db-compare-restore
python3 benches/archive_wallet_db_evidence.py verify --bundle benches/data/wallet_db_compare/run-20261013T124123Z-3216cf07f3 --source-root /tmp/ledger-wallet-wallet-db-compare-restore/wallet-db-compare
~~~

`build` 不會覆寫已存在的輸出目錄。`verify --source-root` 會核對解壓來源、原始檔清單、精簡包內的 byte-preserved 檔案，以及衍生摘要是否可重建。

<a id="original-evidence-lookup"></a>
## 原始證據索引

封存保留 `target/wallet-db-compare/` 的原始相對路徑，並置於 `wallet-db-compare/` 根目錄下。常用來源：

- Qualification scripts/results：`wallet-db-compare/qualification/` 與 `wallet-db-compare/qualification/scylla-profile-qualification-20261013T115554Z/`。
- 先前停止的 runs：`wallet-db-compare/runs/run-20261013T121329Z-2834e7a9e5/` 與 `wallet-db-compare/runs/run-20261013T122913Z-072162c586/`。
- 核准的原始 plan：`wallet-db-compare/plans/plan-20261013T123805Z-eff4ebb234/plan.json`。
- 正式原始案例、runner results 和 monitors：`wallet-db-compare/runs/run-20261013T124123Z-3216cf07f3/`。
- Supervisor launch records：`wallet-db-compare/campaign-launches/`。

封存中的 JSON/JSONL 原檔逐 byte 保留；其中歷史 absolute paths 不會改寫。Git 內的 `run-manifest.json`、`approved-plan.json` 和九個案例的 service result/events 也與封存來源逐 byte 相同。
