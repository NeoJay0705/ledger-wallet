# Scylla Wallet benchmark 證據包

此 Git 證據包讓 fresh checkout 在沒有 `target/scylla-wallet` 時也能查閱主要歷史證據。Package 收錄已引用且需要檢閱的小型原檔逐位元組副本（service result、summary/configuration CSV 與 JSON、非空診斷或驗證 log）、大型 `result.json`、`run-result.json`、`runner-result.json` 的選定欄位摘要，以及完整原始檔案清單與 archive metadata。空 stdout/stderr/log 不複製到 Git；索引保留其原始大小、SHA-256 與封存 member。每個衍生摘要記錄原始路徑、大小和 SHA-256；完整原始 JSON 保留在外部封存。

完整 raw evidence 位於本機 `/home/neojhou/benchmark-archives/ledger-wallet/scylla-wallet`，沒有共用下載網址。封存含 4,061 個檔案、原始大小合計 5,407,757,600 bytes；壓縮後為 4,367,723,502 bytes，SHA-256 為 `0d4f5a0aa109ea4add3fdcbb7fa7c061fe0845bf258e820cf50677e03297aa4b`。完整原始檔案清單收錄為 [`original-files.csv`](original-files.csv)，SHA-256 為 `92f36e42f0c069667845e5ed848278a9d2a020063689ca60394bb434a1a8c3b4`；原始 [`archive-metadata.json`](archive-metadata.json) SHA-256 為 `0b63c7cae447d1723ab183d26865b59483886ece04b750e96755c0fc0fbcd037`，記錄封存與清單的 hash、檔案數和位元組數。

[原始來源索引](archive-index.md#original-source-lookup) 對照每個歷史 `target/scylla-wallet/...` 路徑與 Git 檔或 archive member。索引區分完整原檔副本、衍生 JSON 摘要、只存於 archive 的原始檔、目錄前綴和排除的編譯產物。僅排除 `target/scylla-wallet/release/` 等頂層編譯輸出；jobs 的 `input-snapshot/` 內凍結 binary 屬於歷史測試證據，已保留於 raw archive。歷史三 CQL profile 與 memory-LWT profile 的結果分開存放；失敗案例不併入有效 RPS 結果。

```bash
# 驗證 archive SHA、完整來源清單、檔案數、原始 bytes 與每個 member hash。
python3 benches/archive_scylla_evidence.py verify

# 還原到新目錄；工具拒絕覆蓋既有 tree 或 symlink destination。
python3 benches/archive_scylla_evidence.py extract --destination /tmp/scylla-evidence-restored
```

這兩個命令需要上述本機外部封存。Git 小檔與摘要不需要 `target/` 或 archive 就能閱讀。歷史 PostgreSQL qualification script source 副本位於 [`qualification/source/qualify_postgres18.py`](qualification/source/qualify_postgres18.py)，SHA-256 為 `067b66ad21782d1b37a1b525cbda9bc9503ba5a03baa17b70038711f6d86c9d9`；來源是另一個 wallet-db-compare archive 的 `wallet-db-compare/qualification/qualify_postgres18.py`，不是 Scylla 原始檔案清單成員。它只作歷史 source review，不能當成 fresh checkout 的可直接執行命令；來源路徑與 hash 另列於 [`qualification/source-manifest.csv`](qualification/source-manifest.csv)。

逐位元組副本保留原有內容；歷史 report 內的相對連結可能仍指向原 `target/` 路徑，不保證在新複本可直接開啟。Git 內的 01-40 source snapshot 有兩個相對連結指向未複製的歷史同目錄文件；兩個來源仍在 raw archive，索引錨點為 `#source-03ef4e3dfa6afaf4` 與 `#source-95d61aecd5faeba7`。請以本 package 的索引作為 canonical 來源對照。原始 manifest 描述歷史 input snapshot。文件曾在測量後增補證據連結與索引，因此 input snapshot 記錄的舊文件 SHA 不表示目前包裝後文件仍有相同 SHA。
