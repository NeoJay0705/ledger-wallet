use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use ledger_wallet::storage::account_store::{
    AccountStore, HistoricalDebit, Operation, RefundHistory, Reply, RocksDbBudget, Transaction,
    TransactionKey, TransactionStatus,
};
use rocksdb::{DB, Options};

const SHARD_ID: u32 = 17;

struct DurableHistory;

impl RefundHistory for DurableHistory {
    fn lookup_debit(&self, _: TransactionKey) -> Result<Option<HistoricalDebit>, String> {
        Ok(None)
    }

    fn projection_progress(&self) -> Result<u64, String> {
        Ok(1)
    }
}

fn fresh_path(label: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after Unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "ledger-wallet-formal-{label}-{}-{nonce}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}

async fn open_dynamic(path: &Path) -> Result<(Arc<DB>, Arc<Options>, AccountStore), String> {
    let budget = RocksDbBudget {
        write_buffer_size: 4 * 1024 * 1024,
        max_write_buffer_number: 2,
        block_cache_bytes: 1024 * 1024,
        max_background_jobs: 1,
    };
    let (db, options) = AccountStore::open_database_with_budget(path, budget).await?;
    let store =
        AccountStore::open_dynamic_on_database(Arc::clone(&db), Arc::clone(&options), SHARD_ID)
            .await?;
    Ok((db, options, store))
}

fn transaction(
    account_id: u64,
    tx_id: u64,
    transaction_at: u64,
    operation: Operation,
    amount: u64,
    refund_of: Option<TransactionKey>,
) -> Transaction {
    Transaction {
        key: TransactionKey {
            account_id,
            tx_id,
            transaction_at,
        },
        operation,
        amount,
        refund_of,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dynamic_unknown_balance_query_does_not_create_persistent_state() {
    let path = fresh_path("unknown-query");
    let (db, options, store) = open_dynamic(&path).await.unwrap();
    assert_eq!(store.balance(4_444).unwrap(), 0);
    assert_eq!(store.try_balance_snapshot(4_444).unwrap(), (0, 0));
    assert!(store.all_balances().is_empty());
    store.shutdown().await.unwrap();
    drop(db);
    drop(options);

    let (db, options, reopened) = open_dynamic(&path).await.unwrap();
    assert_eq!(reopened.balance(4_444).unwrap(), 0);
    assert!(reopened.all_balances().is_empty());
    reopened.shutdown().await.unwrap();
    drop(db);
    drop(options);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dynamic_mode_persists_zero_balances_for_new_rejected_transactions() {
    let path = fresh_path("rejected");
    let (db, options, store) = open_dynamic(&path).await.unwrap();

    assert_eq!(store.balance(9_001).unwrap(), 0);
    assert!(store.all_balances().is_empty());

    let rejected = store
        .handle_batch(vec![
            transaction(9_001, 1, 1, Operation::Debit, 5, None),
            transaction(9_002, 2, 2, Operation::Credit, 0, None),
            transaction(9_003, 3, 3, Operation::Refund, 4, None),
        ])
        .await
        .unwrap();
    assert_eq!(
        rejected,
        vec![
            Reply::Transaction {
                status: TransactionStatus::InsufficientFunds,
                balance: 0,
                seq: 1,
                replayed: false,
            },
            Reply::Transaction {
                status: TransactionStatus::InvalidAmount,
                balance: 0,
                seq: 2,
                replayed: false,
            },
            Reply::Transaction {
                status: TransactionStatus::InvalidRefund,
                balance: 0,
                seq: 3,
                replayed: false,
            },
        ]
    );
    assert_eq!(
        store.all_balances(),
        vec![(9_001, 0), (9_002, 0), (9_003, 0)]
    );
    store.shutdown().await.unwrap();
    drop(db);
    drop(options);

    let (db, options, reopened) = open_dynamic(&path).await.unwrap();
    assert_eq!(reopened.latest_seq(), 3);
    assert_eq!(
        reopened.all_balances(),
        vec![(9_001, 0), (9_002, 0), (9_003, 0)]
    );
    reopened.validate_integrity().await.unwrap();
    reopened.shutdown().await.unwrap();
    drop(db);
    drop(options);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dynamic_refund_replay_and_conflict_preserve_the_committed_balance() {
    let path = fresh_path("refund");
    let (db, options, store) = open_dynamic(&path).await.unwrap();
    let debit_key = TransactionKey {
        account_id: 8_100,
        tx_id: 2,
        transaction_at: 20,
    };
    let refund = transaction(8_100, 3, 30, Operation::Refund, 60, Some(debit_key));

    let applied = store
        .handle_batch(vec![
            transaction(8_100, 1, 10, Operation::Credit, 100, None),
            transaction(8_100, 2, 20, Operation::Debit, 60, None),
            refund.clone(),
        ])
        .await
        .unwrap();
    assert!(applied.iter().all(|reply| matches!(
        reply,
        Reply::Transaction {
            status: TransactionStatus::Applied,
            ..
        }
    )));
    assert_eq!(store.balance(8_100).unwrap(), 100);

    let replay = store.handle_batch(vec![refund.clone()]).await.unwrap();
    assert!(matches!(
        replay.as_slice(),
        [Reply::Transaction {
            status: TransactionStatus::Applied,
            balance: 100,
            seq: 3,
            replayed: true,
        }]
    ));
    let conflict = store
        .handle_batch(vec![transaction(
            8_100,
            3,
            30,
            Operation::Refund,
            50,
            Some(debit_key),
        )])
        .await
        .unwrap();
    assert_eq!(conflict, vec![Reply::Conflict]);
    assert_eq!(store.latest_seq(), 3);
    assert_eq!(store.balance(8_100).unwrap(), 100);
    store.shutdown().await.unwrap();
    drop(db);
    drop(options);

    let (db, options, reopened) = open_dynamic(&path).await.unwrap();
    assert_eq!(reopened.balance(8_100).unwrap(), 100);
    assert_eq!(reopened.latest_seq(), 3);
    reopened.shutdown().await.unwrap();
    drop(db);
    drop(options);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dynamic_per_batch_gc_uses_persisted_balance_coverage_without_account_count() {
    let path = fresh_path("dynamic-gc");
    let (db, options, store) = open_dynamic(&path).await.unwrap();
    store
        .bind_history_destination("formal-storage-gc-history")
        .await
        .unwrap();
    store
        .handle_batch(vec![transaction(5_555, 1, 1, Operation::Credit, 7, None)])
        .await
        .unwrap();
    store.persist_projection_progress(1).await.unwrap();
    store.persist_projected_before_durable(2, 1).await.unwrap();
    store.set_refund_history(Arc::new(DurableHistory)).unwrap();

    let gc = store.collect_garbage(1).await.unwrap();
    assert_eq!(gc.deleted, 1);
    assert_eq!(gc.gc_prefix_seq, 1);
    assert_eq!(store.balance(5_555).unwrap(), 7);
    assert!(!store.has_ledger_sequence(1).await.unwrap());
    assert!(
        !store
            .has_transaction_index(TransactionKey {
                account_id: 5_555,
                tx_id: 1,
                transaction_at: 1,
            })
            .await
            .unwrap()
    );
    store.validate_integrity().await.unwrap();
    store.shutdown().await.unwrap();
    drop(db);
    drop(options);

    let (db, options, reopened) = open_dynamic(&path).await.unwrap();
    assert_eq!(reopened.balance(5_555).unwrap(), 7);
    assert_eq!(reopened.gc_prefix_seq(), 1);
    reopened.validate_integrity().await.unwrap();
    reopened.shutdown().await.unwrap();
    drop(db);
    drop(options);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dynamic_history_destination_identity_persists_and_cannot_change() {
    let path = fresh_path("history-identity");
    let (db, options, store) = open_dynamic(&path).await.unwrap();
    assert_eq!(store.persisted_history_destination().await.unwrap(), None);
    store
        .bind_history_destination("history-service:primary")
        .await
        .unwrap();
    store
        .bind_history_destination("history-service:primary")
        .await
        .unwrap();
    assert_eq!(
        store.persisted_history_destination().await.unwrap(),
        Some("history-service:primary".to_owned())
    );
    assert!(
        store
            .bind_history_destination("bad\nidentity")
            .await
            .is_err()
    );
    store.shutdown().await.unwrap();
    drop(db);
    drop(options);

    let (db, options, reopened) = open_dynamic(&path).await.unwrap();
    assert_eq!(
        reopened.persisted_history_destination().await.unwrap(),
        Some("history-service:primary".to_owned())
    );
    assert!(
        reopened
            .bind_history_destination("history-service:replacement")
            .await
            .is_err()
    );
    assert_eq!(
        reopened.persisted_history_destination().await.unwrap(),
        Some("history-service:primary".to_owned())
    );
    reopened.shutdown().await.unwrap();
    drop(db);
    drop(options);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dynamic_recovery_fails_closed_if_projection_identity_is_missing() {
    let path = fresh_path("missing-history-identity");
    let (db, options, store) = open_dynamic(&path).await.unwrap();
    store
        .handle_batch(vec![transaction(5_556, 1, 1, Operation::Credit, 7, None)])
        .await
        .unwrap();
    store
        .bind_history_destination("history-service:primary")
        .await
        .unwrap();
    store.persist_projection_progress(1).await.unwrap();
    store.shutdown().await.unwrap();

    let mut identity_key = vec![b'S'];
    identity_key.extend_from_slice(&SHARD_ID.to_be_bytes());
    identity_key.extend_from_slice(b"M:history-destination");
    db.delete(identity_key).unwrap();
    drop(db);
    drop(options);

    let budget = RocksDbBudget {
        write_buffer_size: 4 * 1024 * 1024,
        max_write_buffer_number: 2,
        block_cache_bytes: 1024 * 1024,
        max_background_jobs: 1,
    };
    let (db, options) = AccountStore::open_database_with_budget(&path, budget)
        .await
        .unwrap();
    let error = AccountStore::open_dynamic_on_database(db, options, SHARD_ID)
        .await
        .err()
        .expect("projection progress without a source identity should fail recovery");
    assert!(
        error.contains("no bound history destination identity"),
        "{error}"
    );
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dynamic_recovery_fails_closed_when_a_retained_account_balance_is_missing() {
    let path = fresh_path("missing-balance");
    let (db, options, store) = open_dynamic(&path).await.unwrap();
    store
        .handle_batch(vec![transaction(7_007, 1, 1, Operation::Credit, 12, None)])
        .await
        .unwrap();
    store.shutdown().await.unwrap();

    let mut balance_key = vec![b'S'];
    balance_key.extend_from_slice(&SHARD_ID.to_be_bytes());
    balance_key.push(b'B');
    balance_key.extend_from_slice(&7_007_u64.to_be_bytes());
    db.delete(balance_key).unwrap();
    drop(db);
    drop(options);

    let budget = RocksDbBudget {
        write_buffer_size: 4 * 1024 * 1024,
        max_write_buffer_number: 2,
        block_cache_bytes: 1024 * 1024,
        max_background_jobs: 1,
    };
    let (db, options) = AccountStore::open_database_with_budget(&path, budget)
        .await
        .unwrap();
    let error = AccountStore::open_dynamic_on_database(db, options, SHARD_ID)
        .await
        .err()
        .expect("missing balance recovery should fail");
    assert!(error.contains("without a durable balance key"), "{error}");
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dynamic_recovery_fails_closed_on_well_formed_but_mismatched_balance() {
    let path = fresh_path("mismatched-balance");
    let (db, options, store) = open_dynamic(&path).await.unwrap();
    store
        .handle_batch(vec![transaction(7_008, 1, 1, Operation::Credit, 12, None)])
        .await
        .unwrap();
    store.shutdown().await.unwrap();

    let mut balance_key = vec![b'S'];
    balance_key.extend_from_slice(&SHARD_ID.to_be_bytes());
    balance_key.push(b'B');
    balance_key.extend_from_slice(&7_008_u64.to_be_bytes());
    db.put(balance_key, 13_u64.to_be_bytes()).unwrap();
    drop(db);
    drop(options);

    let budget = RocksDbBudget {
        write_buffer_size: 4 * 1024 * 1024,
        max_write_buffer_number: 2,
        block_cache_bytes: 1024 * 1024,
        max_background_jobs: 1,
    };
    let (db, options) = AccountStore::open_database_with_budget(&path, budget)
        .await
        .unwrap();
    let error = AccountStore::open_dynamic_on_database(db, options, SHARD_ID)
        .await
        .err()
        .expect("mismatched durable balance recovery should fail");
    assert!(
        error.contains("latest retained ledger result is 12"),
        "{error}"
    );
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dynamic_recovery_rejects_huge_latest_sequence_without_sequence_sized_allocation() {
    let path = fresh_path("huge-sequence");
    let (db, options, store) = open_dynamic(&path).await.unwrap();
    store
        .handle_batch(vec![transaction(6_006, 1, 1, Operation::Credit, 12, None)])
        .await
        .unwrap();
    store.shutdown().await.unwrap();

    let mut sequence_key = vec![b'S'];
    sequence_key.extend_from_slice(&SHARD_ID.to_be_bytes());
    sequence_key.extend_from_slice(b"M:latest-seq");
    db.put(sequence_key, u64::MAX.to_be_bytes()).unwrap();
    drop(db);
    drop(options);

    let budget = RocksDbBudget {
        write_buffer_size: 4 * 1024 * 1024,
        max_write_buffer_number: 2,
        block_cache_bytes: 1024 * 1024,
        max_background_jobs: 1,
    };
    let (db, options) = AccountStore::open_database_with_budget(&path, budget)
        .await
        .unwrap();
    let error = AccountStore::open_dynamic_on_database(db, options, SHARD_ID)
        .await
        .err()
        .expect("invalid huge sequence recovery should fail");
    assert!(
        error.contains("transaction index has 1 retained records"),
        "{error}"
    );
    std::fs::remove_dir_all(path).unwrap();
}
