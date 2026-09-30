//! In-memory destination used to measure ledger catch-up without a second
//! storage engine obscuring the source-read and projector costs.

use crate::ledger_account_store::{LedgerRecord, TransactionKey};
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Clone, Debug, Eq, PartialEq)]
struct ProjectedTransaction {
    operation: crate::ledger_account_store::Operation,
    amount: u64,
    refund_of: Option<TransactionKey>,
    status: crate::ledger_account_store::TransactionStatus,
    balance: u64,
    sequence: u64,
}

impl ProjectedTransaction {
    fn from_record(record: &LedgerRecord) -> Self {
        Self {
            operation: record.request.operation,
            amount: record.request.amount,
            refund_of: record.request.refund_of,
            status: record.result.status,
            balance: record.result.balance,
            sequence: record.result.seq,
        }
    }

    fn matches(&self, record: &LedgerRecord) -> bool {
        self == &Self::from_record(record)
    }
}

#[derive(Default)]
struct State {
    latest_sequence: u64,
    by_key: HashMap<TransactionKey, ProjectedTransaction>,
}

/// Atomic, idempotent mock projection destination.
///
/// This deliberately keeps every projected key and payload in memory. It has
/// no persistence, garbage collection, or restart-recovery semantics.
#[derive(Default)]
pub struct MockProjectionStore {
    state: Mutex<State>,
}

impl MockProjectionStore {
    pub fn with_capacity(capacity: usize) -> Result<Self, String> {
        let mut by_key = HashMap::new();
        by_key
            .try_reserve(capacity)
            .map_err(|error| format!("cannot reserve mock projection capacity: {error}"))?;
        Ok(Self {
            state: Mutex::new(State {
                latest_sequence: 0,
                by_key,
            }),
        })
    }

    /// Apply one contiguous source batch atomically.
    ///
    /// An exact replay is accepted. Conflicting records and sequence gaps fail
    /// without changing any key or advancing the projection watermark.
    pub fn apply_batch(&self, records: &[LedgerRecord]) -> Result<u64, String> {
        if records.is_empty() {
            return Ok(self.progress());
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| "mock projection mutex poisoned".to_owned())?;

        let mut prior_sequence: Option<u64> = None;
        for record in records {
            let sequence = record.result.seq;
            if sequence == 0 {
                return Err("projection sequence numbers begin at one".to_owned());
            }
            if let Some(previous) = prior_sequence
                && sequence != previous.saturating_add(1)
            {
                return Err(format!(
                    "projection batch is not contiguous at sequence {} (got {sequence})",
                    previous.saturating_add(1)
                ));
            }
            prior_sequence = Some(sequence);
        }

        let mut next_new_sequence = state
            .latest_sequence
            .checked_add(1)
            .ok_or_else(|| "projection sequence overflow".to_owned())?;
        let mut staged = HashMap::<TransactionKey, ProjectedTransaction>::new();
        for record in records {
            let projected = ProjectedTransaction::from_record(record);
            let key = record.request.key;
            if let Some(existing) = state.by_key.get(&key) {
                if !existing.matches(record) {
                    return Err(format!(
                        "conflicting projected transaction key {:?}",
                        record.request.key
                    ));
                }
                continue;
            }
            if let Some(existing) = staged.get(&key) {
                if existing != &projected {
                    return Err(format!(
                        "projection batch repeats key {:?} with conflicting payload",
                        record.request.key
                    ));
                }
                return Err(format!(
                    "projection batch repeats key {:?} at multiple sequences",
                    record.request.key
                ));
            }
            if record.result.seq != next_new_sequence {
                return Err(format!(
                    "projection sequence gap: expected {next_new_sequence}, got {}",
                    record.result.seq
                ));
            }
            next_new_sequence = next_new_sequence
                .checked_add(1)
                .ok_or_else(|| "projection sequence overflow".to_owned())?;
            staged.insert(key, projected);
        }

        for (key, value) in staged {
            state.by_key.insert(key, value);
        }
        state.latest_sequence = state.latest_sequence.max(
            next_new_sequence
                .checked_sub(1)
                .ok_or_else(|| "projection sequence underflow".to_owned())?,
        );
        Ok(state.latest_sequence)
    }

    pub fn progress(&self) -> u64 {
        self.state
            .lock()
            .expect("mock projection mutex poisoned")
            .latest_sequence
    }

    pub fn contains(&self, key: TransactionKey) -> bool {
        self.state
            .lock()
            .expect("mock projection mutex poisoned")
            .by_key
            .contains_key(&key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ledger_account_store::{
        Operation, Transaction, TransactionResult, TransactionStatus,
    };

    fn record(sequence: u64, tx_id: u64, amount: u64) -> LedgerRecord {
        LedgerRecord {
            request: Transaction {
                key: TransactionKey {
                    account_id: 7,
                    tx_id,
                    transaction_at: tx_id + 100,
                },
                operation: Operation::Credit,
                amount,
                refund_of: None,
            },
            result: TransactionResult {
                status: TransactionStatus::Applied,
                balance: amount,
                seq: sequence,
            },
        }
    }

    #[test]
    fn exact_duplicate_batch_replay_is_idempotent() {
        let store = MockProjectionStore::default();
        let batch = vec![record(1, 1, 3), record(2, 2, 5)];
        assert_eq!(store.apply_batch(&batch).unwrap(), 2);
        assert_eq!(store.apply_batch(&batch).unwrap(), 2);
        assert!(store.contains(batch[0].request.key));
        assert!(store.contains(batch[1].request.key));
    }

    #[test]
    fn conflicting_batch_fails_without_partial_inserts_or_progress() {
        let store = MockProjectionStore::default();
        let first = record(1, 1, 3);
        store.apply_batch(std::slice::from_ref(&first)).unwrap();

        let new_before_conflict = record(2, 2, 5);
        let mut conflict = record(3, 1, 99);
        conflict.result.balance = 99;
        let error = store
            .apply_batch(&[new_before_conflict.clone(), conflict])
            .unwrap_err();

        assert!(error.contains("conflicting projected transaction"));
        assert_eq!(store.progress(), 1);
        assert!(!store.contains(new_before_conflict.request.key));
    }

    #[test]
    fn sequence_gap_does_not_advance_projection() {
        let store = MockProjectionStore::default();
        let error = store.apply_batch(&[record(2, 2, 5)]).unwrap_err();
        assert!(error.contains("sequence gap"));
        assert_eq!(store.progress(), 0);
        assert!(!store.contains(record(2, 2, 5).request.key));
    }
}
