//! Compatibility shim for the historical benchmark module name.

#[cfg(test)]
#[path = "../../src/batch_queue.rs"]
mod batch_queue;

#[cfg(test)]
pub use self::batch_queue::*;

#[cfg(not(test))]
pub use ledger_wallet::batch_queue::*;
