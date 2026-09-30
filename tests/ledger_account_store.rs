#[path = "../benches/support/ledger_account_store.rs"]
mod ledger_account_store;

use ledger_account_store::{
    AccountStore, BalanceMode, Operation, Reply, Transaction, TransactionKey, TransactionStatus,
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
            .expect("system clock is after the Unix epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "ledger-wallet-account-store-test-{}-{timestamp}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&root).expect("create unique test directory");
        Self(root)
    }

    fn path(&self) -> PathBuf {
        self.0.join("db")
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn txn(
    account_id: u64,
    tx_id: u64,
    transaction_at: u64,
    operation: Operation,
    amount: u64,
) -> Transaction {
    Transaction {
        key: TransactionKey {
            account_id,
            tx_id,
            transaction_at,
        },
        operation,
        amount,
        refund_of: None,
    }
}

fn assert_transaction(
    reply: &Reply,
    status: TransactionStatus,
    balance: u64,
    seq: u64,
    replayed: bool,
) {
    assert_eq!(
        reply,
        &Reply::Transaction {
            status,
            balance,
            seq,
            replayed,
        }
    );
}

#[tokio::test]
async fn per_batch_recovery_preserves_rejections_and_composite_request_identity() {
    let directory = TempDb::new();
    let path = directory.path();
    let original = txn(0, 7, 700, Operation::Credit, 9);
    let same_tx_id_new_timestamp = txn(0, 7, 701, Operation::Credit, 1);
    let insufficient = txn(0, 8, 800, Operation::Debit, 11);
    let invalid_amount = txn(0, 9, 900, Operation::Credit, 0);

    {
        let store = AccountStore::open(&path, 1, BalanceMode::PerBatch, 10)
            .await
            .unwrap();
        let replies = store
            .handle_batch(vec![
                original.clone(),
                same_tx_id_new_timestamp.clone(),
                insufficient.clone(),
                invalid_amount.clone(),
            ])
            .await
            .unwrap();
        assert_transaction(&replies[0], TransactionStatus::Applied, 9, 1, false);
        assert_transaction(&replies[1], TransactionStatus::Applied, 10, 2, false);
        assert_transaction(
            &replies[2],
            TransactionStatus::InsufficientFunds,
            10,
            3,
            false,
        );
        assert_transaction(&replies[3], TransactionStatus::InvalidAmount, 10, 4, false);
        store.shutdown().await.unwrap();
    }

    let store = AccountStore::open(&path, 1, BalanceMode::PerBatch, 10)
        .await
        .unwrap();
    assert_eq!(store.latest_seq(), 4);
    assert_eq!(store.balance(0).unwrap(), 10);

    let replies = store
        .handle_batch(vec![
            original.clone(),
            txn(0, 7, 700, Operation::Credit, 10),
            insufficient.clone(),
            invalid_amount.clone(),
        ])
        .await
        .unwrap();
    assert_transaction(&replies[0], TransactionStatus::Applied, 9, 1, true);
    assert_eq!(replies[1], Reply::Conflict);
    assert_transaction(
        &replies[2],
        TransactionStatus::InsufficientFunds,
        10,
        3,
        true,
    );
    assert_transaction(&replies[3], TransactionStatus::InvalidAmount, 10, 4, true);

    let applied_debit = txn(0, 10, 1_000, Operation::Debit, 10);
    let reply = store.handle_batch(vec![applied_debit]).await.unwrap();
    assert_transaction(&reply[0], TransactionStatus::Applied, 0, 5, false);
    assert_eq!(store.latest_seq(), 5);
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn checkpoint_crossing_between_batches_recovers_checkpoint_and_later_ledger() {
    let directory = TempDb::new();
    let path = directory.path();
    let first = txn(0, 1, 10, Operation::Credit, 7);
    let second = txn(0, 2, 20, Operation::Debit, 3);
    let after_checkpoint = txn(1, 1, 30, Operation::Credit, 5);

    {
        let store = AccountStore::open(&path, 2, BalanceMode::Checkpoint, 2)
            .await
            .unwrap();
        let first_reply = store.handle_batch(vec![first.clone()]).await.unwrap();
        assert_transaction(&first_reply[0], TransactionStatus::Applied, 7, 1, false);

        let second_reply = store.handle_batch(vec![second.clone()]).await.unwrap();
        assert_transaction(&second_reply[0], TransactionStatus::Applied, 4, 2, false);
        store.drain_checkpoints().await.unwrap();
        let metrics = store.metrics();
        assert_eq!(metrics.checkpoint_count, 1);
        assert_eq!(metrics.checkpoint_latest_seq, 2);

        let later_reply = store
            .handle_batch(vec![after_checkpoint.clone()])
            .await
            .unwrap();
        assert_transaction(&later_reply[0], TransactionStatus::Applied, 5, 3, false);
        store.shutdown().await.unwrap();
    }

    let recovered = AccountStore::open(&path, 2, BalanceMode::Checkpoint, 2)
        .await
        .unwrap();
    assert_eq!(recovered.latest_seq(), 3);
    assert_eq!(recovered.all_balances(), [(0, 4), (1, 5)]);

    let replay = recovered.handle_batch(vec![first]).await.unwrap();
    assert_transaction(&replay[0], TransactionStatus::Applied, 7, 1, true);
    assert_eq!(recovered.latest_seq(), 3);
    assert_eq!(recovered.all_balances(), [(0, 4), (1, 5)]);
    recovered.validate_integrity().await.unwrap();
    recovered.shutdown().await.unwrap();
}
