#[allow(dead_code)]
#[path = "../benches/support/ledger_account_store.rs"]
mod ledger_account_store;
#[allow(dead_code)]
#[path = "../benches/support/ledger_projection_worker.rs"]
mod ledger_projection_worker;
#[allow(dead_code)]
#[path = "../benches/support/ledger_time_boundary.rs"]
mod ledger_time_boundary;
#[allow(dead_code)]
#[path = "../benches/support/request_batch_queue.rs"]
mod request_batch_queue;

use ledger_account_store::{
    AccountStore, BalanceMode, Operation, RefundHistory, Reply, RocksDbBudget, Transaction,
    TransactionKey, TransactionStatus,
};
use ledger_projection_worker::{HistoricalLookup, MockProjectionStore};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

struct TempDb(PathBuf);

impl TempDb {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let nonce = NEXT.fetch_add(1, Ordering::Relaxed);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock is after Unix epoch")
            .as_nanos();
        Self(std::env::temp_dir().join(format!(
            "ledger-safe-gc-test-{}-{timestamp}-{nonce}",
            std::process::id()
        )))
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn tx(
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

async fn store(path: &TempDb, mode: BalanceMode, checkpoint_quantity: u64) -> AccountStore {
    AccountStore::open(&path.0, 1, mode, checkpoint_quantity)
        .await
        .unwrap()
}

async fn attach_history(source: &AccountStore, history: &Arc<MockProjectionStore>) {
    let history: Arc<dyn RefundHistory> = history.clone();
    source
        .set_refund_history(history)
        .expect("install mock historical destination");
}

async fn project_until(source: &AccountStore, history: &Arc<MockProjectionStore>, target: u64) {
    let mut next = source.durable_projection_progress().await.unwrap() + 1;
    while next <= target {
        let read = source.read_ledger_range(next, 64).await.unwrap();
        assert!(!read.records.is_empty(), "projector made progress");
        let projected = history.apply_batch(&read.records).unwrap();
        source.persist_projection_progress(projected).await.unwrap();
        next = projected + 1;
    }
}

async fn publish(source: &AccountStore, timestamp: u64, sequence: u64) {
    assert!(source.durable_projection_progress().await.unwrap() >= sequence);
    source
        .persist_projected_before_durable(timestamp, sequence)
        .await
        .unwrap();
}

async fn seed_refundable(source: &AccountStore, timestamp: u64) -> (Transaction, Transaction) {
    let credit = tx(0, 0, timestamp, Operation::Credit, 2, None);
    let debit = tx(0, 1, timestamp, Operation::Debit, 1, None);
    let refund = tx(0, 2, timestamp, Operation::Refund, 1, Some(debit.key));
    let replies = source
        .handle_batch(vec![credit, debit.clone(), refund.clone()])
        .await
        .unwrap();
    assert_eq!(replies.len(), 3);
    for reply in replies {
        let Reply::Transaction { status, .. } = reply else {
            panic!("seed transaction should not conflict");
        };
        assert_eq!(status, TransactionStatus::Applied);
    }
    (debit, refund)
}

#[tokio::test]
async fn failed_progress_sync_replays_idempotently_after_source_restart() {
    let path = TempDb::new();
    let history = Arc::new(MockProjectionStore::default());
    let source = store(&path, BalanceMode::PerBatch, 10).await;
    attach_history(&source, &history).await;
    let credit = tx(0, 1, 10, Operation::Credit, 7, None);
    source.handle_batch(vec![credit.clone()]).await.unwrap();

    let read = source.read_ledger_range(1, 1).await.unwrap();
    assert_eq!(history.apply_batch(&read.records).unwrap(), 1);
    source.fail_next_projection_progress_sync_for_test();
    assert!(source.persist_projection_progress(1).await.is_err());
    assert_eq!(source.durable_projection_progress().await.unwrap(), 0);
    source.shutdown().await.unwrap();

    let reopened = store(&path, BalanceMode::PerBatch, 10).await;
    attach_history(&reopened, &history).await;
    assert_eq!(reopened.verify_projection_destination().await.unwrap(), 1);
    project_until(&reopened, &history, 1).await;
    assert_eq!(reopened.durable_projection_progress().await.unwrap(), 1);
    assert_eq!(
        history.lookup_transaction(&credit).unwrap(),
        HistoricalLookup::ExactReplay(ledger_account_store::TransactionResult {
            status: TransactionStatus::Applied,
            balance: 7,
            seq: 1,
        })
    );
    reopened.validate_integrity().await.unwrap();
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn gc_sync_failure_is_atomic_and_restart_resumes_from_durable_prefix() {
    let path = TempDb::new();
    let history = Arc::new(MockProjectionStore::default());
    let source = store(&path, BalanceMode::PerBatch, 10).await;
    attach_history(&source, &history).await;
    let credit = tx(0, 1, 10, Operation::Credit, 4, None);
    source.handle_batch(vec![credit]).await.unwrap();
    project_until(&source, &history, 1).await;
    publish(&source, 100, 1).await;

    source.fail_next_gc_sync_for_test();
    assert!(source.collect_garbage(8).await.is_err());
    assert_eq!(source.gc_prefix_seq(), 0);
    assert!(source.has_ledger_sequence(1).await.unwrap());
    assert!(
        source
            .has_transaction_index(TransactionKey {
                account_id: 0,
                tx_id: 1,
                transaction_at: 10,
            })
            .await
            .unwrap()
    );
    source.shutdown().await.unwrap();

    let reopened = store(&path, BalanceMode::PerBatch, 10).await;
    attach_history(&reopened, &history).await;
    assert_eq!(
        reopened.restored_projected_before().await.unwrap(),
        Some((100, 1))
    );
    let outcome = reopened.collect_garbage(8).await.unwrap();
    assert_eq!(outcome.deleted, 1);
    assert_eq!(outcome.gc_prefix_seq, 1);
    assert!(!reopened.has_ledger_sequence(1).await.unwrap());
    assert!(
        !reopened
            .has_transaction_index(TransactionKey {
                account_id: 0,
                tx_id: 1,
                transaction_at: 10,
            })
            .await
            .unwrap()
    );
    reopened.shutdown().await.unwrap();

    let after_sync = store(&path, BalanceMode::PerBatch, 10).await;
    attach_history(&after_sync, &history).await;
    assert_eq!(after_sync.gc_prefix_seq(), 1);
    assert_eq!(
        after_sync.restored_projected_before().await.unwrap(),
        Some((100, 1))
    );
    after_sync.validate_integrity().await.unwrap();
    after_sync.shutdown().await.unwrap();
}

#[tokio::test]
async fn strict_boundary_and_out_of_order_timestamps_only_delete_a_safe_prefix() {
    let path = TempDb::new();
    let history = Arc::new(MockProjectionStore::default());
    let source = store(&path, BalanceMode::PerBatch, 10).await;
    attach_history(&source, &history).await;
    let records = vec![
        tx(0, 1, 99, Operation::Credit, 1, None),
        tx(0, 2, 100, Operation::Credit, 1, None),
        tx(0, 3, 20, Operation::Credit, 1, None),
    ];
    source.handle_batch(records).await.unwrap();
    project_until(&source, &history, 3).await;
    publish(&source, 100, 3).await;

    let first = source.collect_garbage(8).await.unwrap();
    assert_eq!(first.deleted, 1);
    assert_eq!(first.blocked_at_seq, Some(2));
    assert_eq!(source.gc_prefix_seq(), 1);
    assert!(source.has_ledger_sequence(2).await.unwrap());
    assert!(source.has_ledger_sequence(3).await.unwrap());

    publish(&source, 101, 3).await;
    let second = source.collect_garbage(8).await.unwrap();
    assert_eq!(second.deleted, 2);
    assert_eq!(second.gc_prefix_seq, 3);
    assert!(!source.has_ledger_sequence(2).await.unwrap());
    assert!(!source.has_ledger_sequence(3).await.unwrap());
    source.validate_integrity().await.unwrap();
    source.shutdown().await.unwrap();
}

#[tokio::test]
async fn checkpoint_lag_blocks_gc_until_manifest_covers_the_prefix() {
    let path = TempDb::new();
    let history = Arc::new(MockProjectionStore::default());
    let source = store(&path, BalanceMode::Checkpoint, 2).await;
    attach_history(&source, &history).await;
    source
        .handle_batch(vec![tx(0, 1, 10, Operation::Credit, 2, None)])
        .await
        .unwrap();
    project_until(&source, &history, 1).await;
    publish(&source, 100, 1).await;
    let blocked = source.collect_garbage(8).await.unwrap();
    assert_eq!(blocked.deleted, 0);
    assert_eq!(source.gc_prefix_seq(), 0);
    assert!(source.has_ledger_sequence(1).await.unwrap());

    source
        .handle_batch(vec![tx(0, 2, 200, Operation::Credit, 3, None)])
        .await
        .unwrap();
    source.drain_checkpoints().await.unwrap();
    project_until(&source, &history, 2).await;
    let eligible = source.collect_garbage(8).await.unwrap();
    assert_eq!(eligible.deleted, 1);
    assert_eq!(eligible.gc_prefix_seq, 1);
    assert!(!source.has_ledger_sequence(1).await.unwrap());
    assert!(source.has_ledger_sequence(2).await.unwrap());
    source.shutdown().await.unwrap();

    let recovered = store(&path, BalanceMode::Checkpoint, 2).await;
    attach_history(&recovered, &history).await;
    assert_eq!(recovered.balance(0).unwrap(), 5);
    recovered.validate_integrity().await.unwrap();
    recovered.shutdown().await.unwrap();
}

#[tokio::test]
async fn refund_marker_survives_until_refund_gc_and_history_prevents_second_refund() {
    let path = TempDb::new();
    let history = Arc::new(MockProjectionStore::default());
    let source = store(&path, BalanceMode::PerBatch, 10).await;
    attach_history(&source, &history).await;
    let (debit, refund) = seed_refundable(&source, 10).await;
    project_until(&source, &history, 3).await;
    publish(&source, 100, 2).await;

    let partial = source.collect_garbage(8).await.unwrap();
    assert_eq!(partial.gc_prefix_seq, 2);
    assert_eq!(partial.deleted, 2);
    assert!(source.has_ledger_sequence(3).await.unwrap());
    assert!(source.refund_marker_exists(debit.key).await.unwrap());
    assert_eq!(
        history.lookup_transaction(&refund).unwrap(),
        HistoricalLookup::ExactReplay(ledger_account_store::TransactionResult {
            status: TransactionStatus::Applied,
            balance: 2,
            seq: 3,
        })
    );

    let replay_refund = tx(0, 99, 200, Operation::Refund, 1, Some(debit.key));
    let reply = source
        .handle_batch(vec![replay_refund.clone()])
        .await
        .unwrap();
    let Reply::Transaction {
        status, balance, ..
    } = reply[0]
    else {
        panic!("refund request should not conflict");
    };
    assert_eq!(status, TransactionStatus::RefundAlreadyUsed);
    assert_eq!(balance, 2);
    source.shutdown().await.unwrap();

    let reopened = store(&path, BalanceMode::PerBatch, 10).await;
    attach_history(&reopened, &history).await;
    assert_eq!(
        reopened.restored_projected_before().await.unwrap(),
        Some((100, 2))
    );
    publish(&reopened, 101, 3).await;
    let final_gc = reopened.collect_garbage(8).await.unwrap();
    assert_eq!(final_gc.gc_prefix_seq, 3);
    assert!(!reopened.refund_marker_exists(debit.key).await.unwrap());
    assert!(!reopened.has_ledger_sequence(3).await.unwrap());
    assert!(reopened.has_ledger_sequence(4).await.unwrap());
    reopened.shutdown().await.unwrap();

    let after_refund_gc = store(&path, BalanceMode::PerBatch, 10).await;
    attach_history(&after_refund_gc, &history).await;
    let retry_after_restart = tx(0, 100, 200, Operation::Refund, 1, Some(debit.key));
    let reply = after_refund_gc
        .handle_batch(vec![retry_after_restart])
        .await
        .unwrap();
    let Reply::Transaction {
        status, balance, ..
    } = reply[0]
    else {
        panic!("refund request should not conflict");
    };
    assert_eq!(status, TransactionStatus::RefundAlreadyUsed);
    assert_eq!(balance, 2);
    after_refund_gc.validate_integrity().await.unwrap();
    after_refund_gc.shutdown().await.unwrap();
}

#[tokio::test]
async fn empty_or_behind_history_destination_cannot_restore_boundary_or_run_gc() {
    let path = TempDb::new();
    let history = Arc::new(MockProjectionStore::default());
    let source = store(&path, BalanceMode::PerBatch, 10).await;
    attach_history(&source, &history).await;
    source
        .handle_batch(vec![tx(0, 1, 10, Operation::Credit, 1, None)])
        .await
        .unwrap();
    project_until(&source, &history, 1).await;
    publish(&source, 100, 1).await;
    source.shutdown().await.unwrap();

    let reopened = store(&path, BalanceMode::PerBatch, 10).await;
    assert!(reopened.restored_projected_before().await.is_err());
    assert!(reopened.collect_garbage(8).await.is_err());
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn integrity_still_rejects_missing_records_above_gc_prefix() {
    let path = TempDb::new();
    let history = Arc::new(MockProjectionStore::default());
    let source = store(&path, BalanceMode::PerBatch, 10).await;
    attach_history(&source, &history).await;
    source
        .handle_batch(vec![
            tx(0, 1, 10, Operation::Credit, 1, None),
            tx(0, 2, 200, Operation::Credit, 1, None),
        ])
        .await
        .unwrap();
    project_until(&source, &history, 2).await;
    publish(&source, 100, 1).await;
    let gc = source.collect_garbage(8).await.unwrap();
    assert_eq!(gc.gc_prefix_seq, 1);
    source.delete_ledger_sequence_for_test(2).await.unwrap();
    let error = source.validate_integrity().await.unwrap_err();
    assert!(error.contains("missing ledger sequence 2"), "{error}");
    source.shutdown().await.unwrap();
}

#[tokio::test]
async fn gc_metadata_is_isolated_between_rocksdb_namespaces() {
    let path = TempDb::new();
    let budget = RocksDbBudget {
        write_buffer_size: 4 * 1024 * 1024,
        max_write_buffer_number: 3,
        block_cache_bytes: 4 * 1024 * 1024,
        max_background_jobs: 2,
    };
    let (db, options) = AccountStore::open_database_with_budget(&path.0, budget.clone())
        .await
        .unwrap();
    let mode = BalanceMode::PerBatch;
    let first =
        AccountStore::open_on_database(Arc::clone(&db), Arc::clone(&options), vec![1], 7, mode, 10)
            .await
            .unwrap();
    let second =
        AccountStore::open_on_database(Arc::clone(&db), Arc::clone(&options), vec![2], 8, mode, 10)
            .await
            .unwrap();
    let first_history = Arc::new(MockProjectionStore::default());
    let second_history = Arc::new(MockProjectionStore::default());
    attach_history(&first, &first_history).await;
    attach_history(&second, &second_history).await;
    first
        .handle_batch(vec![tx(1, 1, 10, Operation::Credit, 1, None)])
        .await
        .unwrap();
    second
        .handle_batch(vec![tx(2, 1, 10, Operation::Credit, 2, None)])
        .await
        .unwrap();
    project_until(&first, &first_history, 1).await;
    project_until(&second, &second_history, 1).await;
    publish(&first, 100, 1).await;
    publish(&second, 100, 1).await;

    assert_eq!(first.collect_garbage(8).await.unwrap().gc_prefix_seq, 1);
    assert_eq!(first.gc_prefix_seq(), 1);
    assert_eq!(second.gc_prefix_seq(), 0);
    assert!(second.has_ledger_sequence(1).await.unwrap());
    first.shutdown().await.unwrap();
    second.shutdown().await.unwrap();
    drop(db);
    let restored_db = AccountStore::open_database_with_budget(&path.0, budget)
        .await
        .unwrap();
    let recovered_second = AccountStore::open_on_database(
        Arc::clone(&restored_db.0),
        Arc::clone(&restored_db.1),
        vec![2],
        8,
        mode,
        10,
    )
    .await
    .unwrap();
    assert_eq!(recovered_second.gc_prefix_seq(), 0);
    assert!(recovered_second.has_ledger_sequence(1).await.unwrap());
    recovered_second.shutdown().await.unwrap();
}
