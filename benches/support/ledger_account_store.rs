//! Compatibility shim for the historical benchmark module name.
//!
//! Benchmark binaries use the library implementation. Integration tests that
//! need the storage fault hooks compile that same source file with `cfg(test)`.

#[cfg(test)]
#[path = "../../src/storage/account_store.rs"]
mod account_store;

#[cfg(test)]
pub use self::account_store::*;

#[cfg(not(test))]
pub use ledger_wallet::storage::account_store::*;
