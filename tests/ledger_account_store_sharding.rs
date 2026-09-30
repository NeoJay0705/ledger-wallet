#[path = "../benches/support/ledger_account_store.rs"]
mod ledger_account_store;

use ledger_account_store::{
    AccountStore, BalanceMode, Operation, Reply, RocksDbBudget, Transaction, TransactionKey,
    TransactionStatus,
};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

struct TempDb(PathBuf);

impl TempDb {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let nonce = NEXT.fetch_add(1, Ordering::Relaxed);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after Unix epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "ledger-wallet-sharded-account-store-test-{}-{timestamp}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&path).expect("create unique test directory");
        Self(path)
    }

    fn database(&self) -> PathBuf {
        self.0.join("shared-db")
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn budget() -> RocksDbBudget {
    RocksDbBudget {
        write_buffer_size: 1024 * 1024,
        max_write_buffer_number: 2,
        block_cache_bytes: 1024 * 1024,
        max_background_jobs: 1,
    }
}

fn same_identity_credit(amount: u64) -> Transaction {
    Transaction {
        key: TransactionKey {
            account_id: 42,
            tx_id: 9,
            transaction_at: 77,
        },
        operation: Operation::Credit,
        amount,
        refund_of: None,
    }
}

fn assert_applied(reply: &Reply, amount: u64) {
    assert_eq!(
        reply,
        &Reply::Transaction {
            status: TransactionStatus::Applied,
            balance: amount,
            seq: 1,
            replayed: false,
        }
    );
}

#[tokio::test]
async fn shared_database_namespaces_isolate_same_request_keys_checkpoint_and_recovery() {
    let temp = TempDb::new();
    let path = temp.database();
    let (db, options) = AccountStore::open_database_with_budget(&path, budget())
        .await
        .unwrap();
    let store_one = AccountStore::open_on_database(
        db.clone(),
        options.clone(),
        vec![42],
        1,
        BalanceMode::Checkpoint,
        1,
    )
    .await
    .unwrap();
    let store_two = AccountStore::open_on_database(
        db.clone(),
        options.clone(),
        vec![42],
        2,
        BalanceMode::Checkpoint,
        1,
    )
    .await
    .unwrap();

    let first = same_identity_credit(7);
    let second = same_identity_credit(11);
    assert_applied(
        &store_one.handle_batch(vec![first.clone()]).await.unwrap()[0],
        7,
    );
    assert_applied(
        &store_two.handle_batch(vec![second.clone()]).await.unwrap()[0],
        11,
    );
    store_one.drain_checkpoints().await.unwrap();
    store_two.drain_checkpoints().await.unwrap();
    assert_eq!(store_one.metrics().checkpoint_count, 1);
    assert_eq!(store_two.metrics().checkpoint_count, 1);
    assert_eq!(store_one.latest_seq(), 1);
    assert_eq!(store_two.latest_seq(), 1);
    store_one.shutdown().await.unwrap();
    store_two.shutdown().await.unwrap();
    drop(db);
    drop(options);

    let (db, options) = AccountStore::open_database_with_budget(&path, budget())
        .await
        .unwrap();
    let recovered_one = AccountStore::open_on_database(
        db.clone(),
        options.clone(),
        vec![42],
        1,
        BalanceMode::Checkpoint,
        1,
    )
    .await
    .unwrap();
    let recovered_two = AccountStore::open_on_database(
        db.clone(),
        options.clone(),
        vec![42],
        2,
        BalanceMode::Checkpoint,
        1,
    )
    .await
    .unwrap();
    assert_eq!(recovered_one.namespace_id(), Some(1));
    assert_eq!(recovered_two.namespace_id(), Some(2));
    assert_eq!(recovered_one.latest_seq(), 1);
    assert_eq!(recovered_two.latest_seq(), 1);
    assert_eq!(recovered_one.all_balances(), vec![(42, 7)]);
    assert_eq!(recovered_two.all_balances(), vec![(42, 11)]);
    assert_eq!(
        recovered_one.handle_batch(vec![first]).await.unwrap()[0],
        Reply::Transaction {
            status: TransactionStatus::Applied,
            balance: 7,
            seq: 1,
            replayed: true,
        }
    );
    assert_eq!(
        recovered_two.handle_batch(vec![second]).await.unwrap()[0],
        Reply::Transaction {
            status: TransactionStatus::Applied,
            balance: 11,
            seq: 1,
            replayed: true,
        }
    );
    recovered_one.validate_integrity().await.unwrap();
    recovered_two.validate_integrity().await.unwrap();
    recovered_one.shutdown().await.unwrap();
    recovered_two.shutdown().await.unwrap();
}
