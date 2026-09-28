#[path = "../benches/support/rocksdb_store.rs"]
mod store;

use rocksdb::{DB, WriteOptions};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

struct TempTree(PathBuf);

impl TempTree {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let nonce = NEXT.fetch_add(1, Ordering::Relaxed);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "ledger-wallet-rocksdb-test-{}-{timestamp}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn db_path(&self) -> PathBuf {
        self.0.join("db")
    }
}

impl Drop for TempTree {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn open_db(path: &Path) -> DB {
    let options = store::rocksdb_options();
    store::open_db(&options, path).unwrap()
}

fn write_sync_batch(db: &DB, batch: rocksdb::WriteBatch) {
    let mut write_options = WriteOptions::default();
    write_options.set_sync(true);
    db.write_opt(batch, &write_options).unwrap();
}

#[test]
fn one_batch_persists_duplicate_user_ledger_balance_and_global_sequence() {
    let temp = TempTree::new();
    let db = open_db(&temp.db_path());
    let entries = [
        store::LedgerEntry {
            tx_id: 1,
            user_id: 0,
            direction: store::Direction::Credit,
            outcome: store::Outcome::Applied,
            amount_cents: 100,
            post_balance_cents: 100,
            seq: 1,
        },
        store::LedgerEntry {
            tx_id: 2,
            user_id: 1,
            direction: store::Direction::Credit,
            outcome: store::Outcome::Applied,
            amount_cents: 100,
            post_balance_cents: 100,
            seq: 2,
        },
        store::LedgerEntry {
            tx_id: 3,
            user_id: 0,
            direction: store::Direction::Debit,
            outcome: store::Outcome::Applied,
            amount_cents: 100,
            post_balance_cents: 0,
            seq: 3,
        },
    ];
    let mut committed_balances = vec![0; store::USER_COUNT];
    let (batch, updates) = store::build_write_batch(&entries, &committed_balances, 0).unwrap();

    assert_eq!(updates, vec![(0, 0), (1, 100)]);
    write_sync_batch(&db, batch);
    store::apply_balance_updates(&mut committed_balances, &updates);
    store::validate_stored_state(&db, &committed_balances, 3, 2).unwrap();

    let keys: Vec<_> = entries
        .iter()
        .map(|entry| store::ledger_key(entry.tx_id))
        .collect();
    let encoded = store::read_values(&db, &keys, false).unwrap();
    for (raw, expected) in encoded.iter().zip(entries) {
        assert_eq!(store::decode_ledger_entry(raw).unwrap(), expected);
    }
}

#[test]
fn ledger_lookup_multiget_survives_close_and_reopen() {
    let temp = TempTree::new();
    let path = temp.db_path();
    let entries: Vec<_> = (1..=120)
        .map(|seq| store::LedgerEntry::for_seq(seq).unwrap())
        .collect();
    let mut committed_balances = vec![0; store::USER_COUNT];
    {
        let db = open_db(&path);
        let (batch, updates) = store::build_write_batch(&entries, &committed_balances, 0).unwrap();
        write_sync_batch(&db, batch);
        store::apply_balance_updates(&mut committed_balances, &updates);
        store::validate_stored_state(&db, &committed_balances, 120, 120).unwrap();
    }

    let reopened = open_db(&path);
    store::validate_stored_state(&reopened, &committed_balances, 120, 120).unwrap();
    let query_ids = [120, 1, 65, 100, 101];
    let keys: Vec<_> = query_ids
        .iter()
        .map(|tx_id| store::ledger_key(*tx_id))
        .collect();
    let decoded: Vec<_> = store::read_values(&reopened, &keys, false)
        .unwrap()
        .iter()
        .map(|bytes| store::decode_ledger_entry(bytes).unwrap())
        .collect();
    let expected: Vec<_> = query_ids
        .iter()
        .map(|tx_id| store::LedgerEntry::for_seq(*tx_id).unwrap())
        .collect();
    assert_eq!(decoded, expected);
}

#[test]
fn batch_builder_rejects_a_debit_that_would_make_balance_negative() {
    let debit_first = store::LedgerEntry {
        tx_id: 1,
        user_id: 0,
        direction: store::Direction::Debit,
        outcome: store::Outcome::Applied,
        amount_cents: 100,
        post_balance_cents: 0,
        seq: 1,
    };
    let error = match store::build_write_batch(&[debit_first], &vec![0; store::USER_COUNT], 0) {
        Ok(_) => panic!("debit-first batch unexpectedly succeeded"),
        Err(error) => error,
    };
    assert!(error.contains("negative"));
}
