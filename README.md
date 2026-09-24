# ledger-wallet

以 Rust 建立的帳本專案，目前僅完成專案初始化，尚未提供交易、帳戶或儲存功能。

## 開發環境

需要 rustup。專案的 `rust-toolchain.toml` 固定 Rust 1.98.1；Cargo 會使用此版本。

```sh
cargo fmt --check
cargo check --locked
```

目前 `src/main.rs` 是空入口，執行程式不會產生輸出。

## 文件

- [原始需求](docs/01-01.raw-requirement-ledger-wallet.md)
- [專案初始化設計](docs/01-02.development-design-project-initialization.md)
