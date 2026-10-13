//! Historical transaction projection contracts.
//!
//! A durable implementation must make `apply_batch` atomic across transaction
//! rows, refund state, and contiguous progress. Its returned progress must be
//! durable. Reads used for old transaction replay and refund-once validation
//! must read that same committed state, rather than a lagging replica.

use crate::{LedgerRecord, Transaction, TransactionKey, TransactionResult};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;

pub type HistoryFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, HistoryError>> + Send + 'a>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistoryDurability {
    /// Process-local state. It can be rebuilt only while every source ledger
    /// record remains retained.
    Volatile,
    /// `apply_batch` and the returned progress survive process restart.
    Durable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistoryErrorKind {
    Unavailable,
    Conflict,
    Corruption,
    IdentityMismatch,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryError {
    pub kind: HistoryErrorKind,
    pub message: String,
}

impl HistoryError {
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self {
            kind: HistoryErrorKind::Unavailable,
            message: message.into(),
        }
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self {
            kind: HistoryErrorKind::Conflict,
            message: message.into(),
        }
    }

    pub fn corruption(message: impl Into<String>) -> Self {
        Self {
            kind: HistoryErrorKind::Corruption,
            message: message.into(),
        }
    }

    pub fn identity_mismatch(message: impl Into<String>) -> Self {
        Self {
            kind: HistoryErrorKind::IdentityMismatch,
            message: message.into(),
        }
    }

    pub fn is_temporary(&self) -> bool {
        self.kind == HistoryErrorKind::Unavailable
    }
}

impl std::fmt::Display for HistoryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.kind, self.message)
    }
}

impl std::fmt::Display for HistoryErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Unavailable => "unavailable",
            Self::Conflict => "conflict",
            Self::Corruption => "corruption",
            Self::IdentityMismatch => "identity mismatch",
        })
    }
}

impl std::error::Error for HistoryError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryBinding {
    /// Randomly generated once and stored beside the source database.
    pub service_identity: String,
    /// Stable per-shard identity presented to the destination.
    pub source_identity: String,
    pub shard_id: u32,
    pub shard_count: u32,
    pub schema_version: u32,
    pub routing_version: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistoricalLookup {
    ExactReplay(TransactionResult),
    Conflict,
    NotFound,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HistoricalDebit {
    pub amount: u64,
    pub already_refunded: bool,
}

/// Object-safe async history store, instantiated once per source shard.
///
/// `verify_binding` must durably bind a new destination namespace to the full
/// source identity and reject any later mismatch. In particular, a coincidentally
/// high progress value is not evidence that it belongs to this source.
pub trait HistoryStore: Send + Sync + 'static {
    fn durability(&self) -> HistoryDurability;

    fn destination_identity(&self) -> String;

    fn verify_binding<'a>(&'a self, binding: &'a HistoryBinding) -> HistoryFuture<'a, ()>;

    fn apply_batch<'a>(&'a self, records: &'a [LedgerRecord]) -> HistoryFuture<'a, u64>;

    fn lookup_transaction<'a>(
        &'a self,
        request: &'a Transaction,
    ) -> HistoryFuture<'a, HistoricalLookup>;

    fn progress(&self) -> HistoryFuture<'_, u64>;

    fn lookup_debit<'a>(
        &'a self,
        key: TransactionKey,
    ) -> HistoryFuture<'a, Option<HistoricalDebit>>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ProjectedTransaction {
    operation: crate::Operation,
    amount: u64,
    refund_of: Option<TransactionKey>,
    result: TransactionResult,
}

impl ProjectedTransaction {
    fn from_record(record: &LedgerRecord) -> Self {
        Self {
            operation: record.request.operation,
            amount: record.request.amount,
            refund_of: record.request.refund_of,
            result: record.result,
        }
    }

    fn matches_request(&self, request: &Transaction) -> bool {
        self.operation == request.operation
            && self.amount == request.amount
            && self.refund_of == request.refund_of
    }
}

#[derive(Default)]
struct MockState {
    latest_sequence: u64,
    by_key: HashMap<TransactionKey, ProjectedTransaction>,
    by_sequence: HashMap<u64, TransactionKey>,
    refunded_targets: HashSet<TransactionKey>,
}

/// In-memory history store for local CLI use and projector exercises.
///
/// This is deliberately `Volatile`. The service will replay retained source
/// records from sequence one on each open and will never publish a watermark or
/// run source GC while this store is selected.
#[derive(Default)]
pub struct MockHistory {
    destination_identity: String,
    state: Mutex<MockState>,
}

impl MockHistory {
    pub fn new(destination_identity: impl Into<String>) -> Self {
        Self {
            destination_identity: destination_identity.into(),
            state: Mutex::new(MockState::default()),
        }
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, MockState>, HistoryError> {
        self.state
            .lock()
            .map_err(|_| HistoryError::corruption("mock history mutex poisoned"))
    }
}

impl HistoryStore for MockHistory {
    fn durability(&self) -> HistoryDurability {
        HistoryDurability::Volatile
    }

    fn destination_identity(&self) -> String {
        if self.destination_identity.is_empty() {
            "mock-volatile-history".to_owned()
        } else {
            self.destination_identity.clone()
        }
    }

    fn verify_binding<'a>(&'a self, _binding: &'a HistoryBinding) -> HistoryFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn apply_batch<'a>(&'a self, records: &'a [LedgerRecord]) -> HistoryFuture<'a, u64> {
        Box::pin(async move {
            if records.is_empty() {
                return self.lock().map(|state| state.latest_sequence);
            }
            let mut prior = None;
            for record in records {
                let sequence = record.result.seq;
                if sequence == 0 {
                    return Err(HistoryError::corruption(
                        "projection sequence numbers begin at one",
                    ));
                }
                if prior.is_some_and(|previous: u64| sequence != previous.saturating_add(1)) {
                    return Err(HistoryError::corruption(format!(
                        "projection batch is not contiguous before sequence {sequence}"
                    )));
                }
                prior = Some(sequence);
            }

            let mut state = self.lock()?;
            let mut expected_new = state.latest_sequence.saturating_add(1);
            let mut staged_by_key = HashMap::<TransactionKey, ProjectedTransaction>::new();
            let mut staged_by_sequence = Vec::<(u64, TransactionKey)>::new();
            let mut staged_refunds = Vec::new();
            for record in records {
                let sequence = record.result.seq;
                let key = record.request.key;
                let projected = ProjectedTransaction::from_record(record);
                if let Some(existing) = state.by_key.get(&key) {
                    if existing != &projected || state.by_sequence.get(&sequence) != Some(&key) {
                        return Err(HistoryError::conflict(format!(
                            "history key {key:?} conflicts with its projected record"
                        )));
                    }
                    continue;
                }
                if let Some(existing) = staged_by_key.get(&key) {
                    if existing != &projected || existing.result.seq == sequence {
                        return Err(HistoryError::conflict(format!(
                            "projection batch repeats key {key:?}"
                        )));
                    }
                    return Err(HistoryError::corruption(format!(
                        "projection key {key:?} appears at more than one sequence"
                    )));
                }
                if sequence != expected_new {
                    return Err(HistoryError::corruption(format!(
                        "projection sequence gap: expected {expected_new}, got {sequence}"
                    )));
                }
                if let Some(existing_key) = state.by_sequence.get(&sequence) {
                    if *existing_key != key {
                        return Err(HistoryError::conflict(format!(
                            "projection sequence {sequence} already belongs to {existing_key:?}"
                        )));
                    }
                }
                if record.request.operation == crate::Operation::Refund
                    && record.result.status == crate::TransactionStatus::Applied
                {
                    let target = record.request.refund_of.ok_or_else(|| {
                        HistoryError::corruption("applied refund has no target key")
                    })?;
                    let debit = staged_by_key
                        .get(&target)
                        .or_else(|| state.by_key.get(&target))
                        .ok_or_else(|| {
                            HistoryError::corruption(format!(
                                "applied refund target {target:?} is absent from history"
                            ))
                        })?;
                    if debit.operation != crate::Operation::Debit
                        || debit.result.status != crate::TransactionStatus::Applied
                        || debit.amount != record.request.amount
                        || target.account_id != record.request.key.account_id
                    {
                        return Err(HistoryError::corruption(format!(
                            "applied refund target {target:?} does not match its debit"
                        )));
                    }
                    if state.refunded_targets.contains(&target) || staged_refunds.contains(&target)
                    {
                        return Err(HistoryError::corruption(format!(
                            "history contains a second applied refund for {target:?}"
                        )));
                    }
                    staged_refunds.push(target);
                }
                expected_new = expected_new
                    .checked_add(1)
                    .ok_or_else(|| HistoryError::corruption("projection sequence overflow"))?;
                staged_by_sequence.push((sequence, key));
                staged_by_key.insert(key, projected);
            }

            let latest = expected_new.saturating_sub(1).max(state.latest_sequence);
            for (sequence, key) in staged_by_sequence {
                state.by_sequence.insert(sequence, key);
            }
            state.by_key.extend(staged_by_key);
            state.refunded_targets.extend(staged_refunds);
            state.latest_sequence = latest;
            Ok(latest)
        })
    }

    fn lookup_transaction<'a>(
        &'a self,
        request: &'a Transaction,
    ) -> HistoryFuture<'a, HistoricalLookup> {
        Box::pin(async move {
            let state = self.lock()?;
            let Some(projected) = state.by_key.get(&request.key) else {
                return Ok(HistoricalLookup::NotFound);
            };
            if projected.matches_request(request) {
                Ok(HistoricalLookup::ExactReplay(projected.result))
            } else {
                Ok(HistoricalLookup::Conflict)
            }
        })
    }

    fn progress(&self) -> HistoryFuture<'_, u64> {
        Box::pin(async { self.lock().map(|state| state.latest_sequence) })
    }

    fn lookup_debit<'a>(
        &'a self,
        key: TransactionKey,
    ) -> HistoryFuture<'a, Option<HistoricalDebit>> {
        Box::pin(async move {
            let state = self.lock()?;
            let Some(projected) = state.by_key.get(&key) else {
                return Ok(None);
            };
            if projected.operation != crate::Operation::Debit
                || projected.result.status != crate::TransactionStatus::Applied
            {
                return Ok(None);
            }
            Ok(Some(HistoricalDebit {
                amount: projected.amount,
                already_refunded: state.refunded_targets.contains(&key),
            }))
        })
    }
}
