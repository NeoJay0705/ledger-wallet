mod postgres_store;
mod service;
mod store;

pub mod metrics;

pub use metrics::Metrics;
pub use postgres_store::{PostgresOperationCounters, PostgresStore};
pub use service::{Service, WorkerRouting};
pub use store::{ScyllaStore, WalletStore};

use serde::{Deserialize, Serialize};
use std::error::Error as StdError;
use std::fmt;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Transaction {
    pub account_id: u64,
    pub tx_id: u64,
    pub operation: Operation,
    pub amount: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Operation {
    Credit,
    Debit,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Outcome {
    Applied,
    InsufficientFunds,
    CreditOverflow,
    InvalidAmount,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TransactionResult {
    pub outcome: Outcome,
    pub balance: u64,
    pub version: u64,
    pub replayed: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LedgerRecord {
    pub request: Transaction,
    pub result: TransactionResult,
    /// Logical per-account clock persisted with the immutable ledger row.
    pub write_clock: i64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AccountState {
    pub balance: u64,
    pub version: u64,
    /// Latest logical per-account clock persisted in the static account state.
    pub write_clock: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommitResolution {
    Applied,
    Existing {
        current: AccountState,
        record: LedgerRecord,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DriverCounters {
    pub queries: u64,
    pub errors: u64,
    pub retries: u64,
    pub connection_timeouts: u64,
    pub request_timeouts: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Error {
    InvalidConfiguration(String),
    NotAccepted(String),
    Conflict(String),
    Storage(String),
    CommitConfirmedRollback(String),
    CommitOutcomeUnknown(String),
    Corruption(String),
    Frozen(String),
    Closed,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration(message) => write!(f, "invalid configuration: {message}"),
            Self::NotAccepted(message) => write!(f, "request not accepted: {message}"),
            Self::Conflict(message) => write!(f, "transaction conflict: {message}"),
            Self::Storage(message) => write!(f, "storage error: {message}"),
            Self::CommitConfirmedRollback(message) => {
                write!(f, "commit was confirmed rolled back: {message}")
            }
            Self::CommitOutcomeUnknown(message) => {
                write!(f, "commit outcome unknown: {message}")
            }
            Self::Corruption(message) => write!(f, "wallet data corruption: {message}"),
            Self::Frozen(message) => write!(f, "account frozen: {message}"),
            Self::Closed => f.write_str("service is closed"),
        }
    }
}

impl StdError for Error {}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConsistencyLevel {
    One,
    Two,
    Three,
}

impl ConsistencyLevel {
    pub const fn replica_count(self) -> u8 {
        match self {
            Self::One => 1,
            Self::Two => 2,
            Self::Three => 3,
        }
    }

    pub const fn canonical_name(self) -> &'static str {
        match self {
            Self::One => "ONE",
            Self::Two => "TWO",
            Self::Three => "THREE",
        }
    }

    pub const fn from_replica_count(count: u8) -> Option<Self> {
        match count {
            1 => Some(Self::One),
            2 => Some(Self::Two),
            3 => Some(Self::Three),
            _ => None,
        }
    }
}
