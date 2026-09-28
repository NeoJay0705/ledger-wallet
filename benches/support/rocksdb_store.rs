//! Benchmark-only data model and key/value encoding for the RocksDB ledger trial.

use rocksdb::statistics::StatsLevel;
use rocksdb::{DB, Options, WriteBatch};
use std::collections::BTreeMap;
use std::path::Path;

pub const USER_COUNT: usize = 100_000;
pub const TRANSACTIONS_PER_USER: u64 = 100;
pub const AMOUNT_CENTS: u64 = 100;
pub const MAX_TRANSACTIONS: u64 = USER_COUNT as u64 * TRANSACTIONS_PER_USER;
const LEDGER_PREFIX: u8 = b'L';
const BALANCE_PREFIX: u8 = b'B';
const GLOBAL_SEQUENCE_KEY: &[u8] = b"G:latest-ledger-seq";
const LEDGER_VALUE_BYTES: usize = 8 + 8 + 1 + 1 + 8 + 8 + 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Credit,
    Debit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Applied,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LedgerEntry {
    pub tx_id: u64,
    pub user_id: u64,
    pub direction: Direction,
    pub outcome: Outcome,
    pub amount_cents: u64,
    pub post_balance_cents: u64,
    pub seq: u64,
}

impl LedgerEntry {
    /// Produce the fixed-cent workload entry for a one-based global sequence.
    pub fn for_seq(seq: u64) -> Result<Self, String> {
        if seq == 0 || seq > MAX_TRANSACTIONS {
            return Err(format!(
                "sequence {seq} is outside the workload range 1..={MAX_TRANSACTIONS}"
            ));
        }
        let zero_based = seq - 1;
        let user_id = zero_based % USER_COUNT as u64;
        let user_round = zero_based / USER_COUNT as u64;
        let direction = if user_round % 2 == 0 {
            Direction::Credit
        } else {
            Direction::Debit
        };
        let post_balance_cents = match direction {
            Direction::Credit => AMOUNT_CENTS,
            Direction::Debit => 0,
        };
        Ok(Self {
            tx_id: seq,
            user_id,
            direction,
            outcome: Outcome::Applied,
            amount_cents: AMOUNT_CENTS,
            post_balance_cents,
            seq,
        })
    }
}

pub fn rocksdb_options() -> Options {
    let mut options = Options::default();
    options.create_if_missing(true);
    options.enable_statistics();
    options.set_statistics_level(StatsLevel::ExceptDetailedTimers);
    options
}

/// Open the default column family explicitly so its handle is available to
/// RocksDB's column-family multiget API.
pub fn open_db<P: AsRef<Path>>(options: &Options, path: P) -> Result<DB, rocksdb::Error> {
    DB::open_cf(options, path, [rocksdb::DEFAULT_COLUMN_FAMILY_NAME])
}

pub fn ledger_key(tx_id: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(9);
    key.push(LEDGER_PREFIX);
    key.extend_from_slice(&tx_id.to_be_bytes());
    key
}

pub fn balance_key(user_id: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(9);
    key.push(BALANCE_PREFIX);
    key.extend_from_slice(&user_id.to_be_bytes());
    key
}

pub fn encode_ledger_entry(entry: LedgerEntry) -> [u8; LEDGER_VALUE_BYTES] {
    let mut encoded = [0; LEDGER_VALUE_BYTES];
    encoded[0..8].copy_from_slice(&entry.tx_id.to_le_bytes());
    encoded[8..16].copy_from_slice(&entry.user_id.to_le_bytes());
    encoded[16] = match entry.direction {
        Direction::Credit => 0,
        Direction::Debit => 1,
    };
    encoded[17] = match entry.outcome {
        Outcome::Applied => 0,
    };
    encoded[18..26].copy_from_slice(&entry.amount_cents.to_le_bytes());
    encoded[26..34].copy_from_slice(&entry.post_balance_cents.to_le_bytes());
    encoded[34..42].copy_from_slice(&entry.seq.to_le_bytes());
    encoded
}

pub fn decode_ledger_entry(bytes: &[u8]) -> Result<LedgerEntry, String> {
    if bytes.len() != LEDGER_VALUE_BYTES {
        return Err(format!(
            "ledger value has {} bytes, expected {LEDGER_VALUE_BYTES}",
            bytes.len()
        ));
    }
    let read_u64 = |range: std::ops::Range<usize>| {
        u64::from_le_bytes(
            bytes[range]
                .try_into()
                .expect("checked ledger value length"),
        )
    };
    let direction = match bytes[16] {
        0 => Direction::Credit,
        1 => Direction::Debit,
        value => return Err(format!("unknown ledger direction code {value}")),
    };
    let outcome = match bytes[17] {
        0 => Outcome::Applied,
        value => return Err(format!("unknown ledger outcome code {value}")),
    };
    Ok(LedgerEntry {
        tx_id: read_u64(0..8),
        user_id: read_u64(8..16),
        direction,
        outcome,
        amount_cents: read_u64(18..26),
        post_balance_cents: read_u64(26..34),
        seq: read_u64(34..42),
    })
}

/// Build ledger, affected-account, and global-sequence writes without changing
/// the caller's committed balance vector. The returned updates are applied only
/// after RocksDB acknowledges the complete WriteBatch.
pub fn build_write_batch(
    entries: &[LedgerEntry],
    committed_balances: &[u64],
    previous_global_seq: u64,
) -> Result<(WriteBatch, Vec<(usize, u64)>), String> {
    if entries.is_empty() {
        return Err("cannot write an empty ledger batch".to_owned());
    }

    let mut final_balances = BTreeMap::<usize, u64>::new();
    let mut batch = WriteBatch::default();
    for (offset, entry) in entries.iter().copied().enumerate() {
        let expected_seq = previous_global_seq
            .checked_add(offset as u64 + 1)
            .ok_or_else(|| "global sequence overflow".to_owned())?;
        if entry.seq != expected_seq || entry.tx_id != expected_seq {
            return Err(format!(
                "ledger sequence/tx_id mismatch: got seq={} tx_id={}, expected {expected_seq}",
                entry.seq, entry.tx_id
            ));
        }
        let user_index = usize::try_from(entry.user_id)
            .map_err(|_| format!("user id {} does not fit usize", entry.user_id))?;
        let committed = committed_balances
            .get(user_index)
            .copied()
            .ok_or_else(|| format!("user id {} has no balance slot", entry.user_id))?;
        let balance = final_balances.entry(user_index).or_insert(committed);
        *balance = match entry.direction {
            Direction::Credit => balance
                .checked_add(entry.amount_cents)
                .ok_or_else(|| format!("balance overflow for user {}", entry.user_id))?,
            Direction::Debit if *balance >= entry.amount_cents => *balance - entry.amount_cents,
            Direction::Debit => {
                return Err(format!(
                    "debit of {} cents would make user {} balance negative",
                    entry.amount_cents, entry.user_id
                ));
            }
        };
        if *balance != entry.post_balance_cents {
            return Err(format!(
                "post-balance mismatch for tx {}: computed {}, entry says {}",
                entry.tx_id, balance, entry.post_balance_cents
            ));
        }
        batch.put(ledger_key(entry.tx_id), encode_ledger_entry(entry));
    }

    let updates: Vec<_> = final_balances.into_iter().collect();
    for (user_id, balance) in &updates {
        batch.put(balance_key(*user_id as u64), balance.to_le_bytes());
    }
    let final_seq = entries.last().expect("non-empty batch checked").seq;
    batch.put(GLOBAL_SEQUENCE_KEY, final_seq.to_le_bytes());
    Ok((batch, updates))
}

pub fn apply_balance_updates(balances: &mut [u64], updates: &[(usize, u64)]) {
    for &(user_id, balance) in updates {
        balances[user_id] = balance;
    }
}

/// Read encoded values with RocksDB's block-table batched multiget path.
pub fn read_values(db: &DB, keys: &[Vec<u8>], sorted_input: bool) -> Result<Vec<Vec<u8>>, String> {
    let cf = db
        .cf_handle("default")
        .ok_or_else(|| "RocksDB default column family is unavailable".to_owned())?;
    let values = db.batched_multi_get_cf(cf, keys.iter(), sorted_input);
    values
        .into_iter()
        .enumerate()
        .map(|(index, value)| {
            value
                .map_err(|error| format!("RocksDB multiget failed at result {index}: {error}"))?
                .map(|value| value.as_ref().to_vec())
                .ok_or_else(|| format!("RocksDB key at result {index} is missing"))
        })
        .collect()
}

pub fn validate_stored_state(
    db: &DB,
    balances: &[u64],
    expected_seq: u64,
    touched_users: usize,
) -> Result<(), String> {
    let raw_seq = db
        .get(GLOBAL_SEQUENCE_KEY)
        .map_err(|error| format!("could not read latest global ledger sequence: {error}"))?
        .ok_or_else(|| "latest global ledger sequence is missing".to_owned())?;
    let expected_seq_bytes = expected_seq.to_le_bytes();
    if raw_seq.as_slice() != expected_seq_bytes.as_slice() {
        return Err(format!(
            "latest global ledger sequence mismatch: found {:?}, expected {expected_seq}",
            raw_seq.as_slice()
        ));
    }

    if touched_users > balances.len() {
        return Err(format!(
            "touched user count {touched_users} exceeds balance slots {}",
            balances.len()
        ));
    }
    for (chunk_start, chunk) in balances[..touched_users].chunks(4096).enumerate() {
        let first_user = chunk_start * 4096;
        let keys: Vec<_> = (first_user..first_user + chunk.len())
            .map(|user_id| balance_key(user_id as u64))
            .collect();
        let values = read_values(db, &keys, true)?;
        for (offset, (raw, expected)) in values.iter().zip(chunk).enumerate() {
            let actual = u64::from_le_bytes(raw.as_slice().try_into().map_err(|_| {
                format!(
                    "balance value has invalid width for user {}",
                    first_user + offset
                )
            })?);
            if actual != *expected {
                return Err(format!(
                    "persisted balance mismatch for user {}: found {actual}, expected {expected}",
                    first_user + offset
                ));
            }
        }
    }
    Ok(())
}
