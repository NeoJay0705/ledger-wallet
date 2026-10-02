#[path = "../benches/support/ledger_account_store.rs"]
mod ledger_account_store;

use ledger_account_store::{
    AccountStore, BalanceMode, IndexLookupConfig, IndexLookupMode, Operation, Reply, Transaction,
    TransactionKey, TransactionStatus,
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
            "ledger-wallet-index-lookup-test-{}-{timestamp}-{nonce}",
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

fn refund_txn(tx_id: u64, transaction_at: u64, debit: TransactionKey) -> Transaction {
    Transaction {
        key: TransactionKey {
            account_id: 0,
            tx_id,
            transaction_at,
        },
        operation: Operation::Refund,
        amount: 3,
        refund_of: Some(debit),
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

fn assert_store_state(store: &AccountStore, seq: u64, balances: &[(u64, u64)]) {
    assert_eq!(store.latest_seq(), seq);
    assert_eq!(store.all_balances().as_slice(), balances);
}

fn lookup_modes() -> [(IndexLookupMode, &'static str); 6] {
    [
        (IndexLookupMode::PointGet, "point-get"),
        (IndexLookupMode::WholeBatchMultiGet, "whole-batch-multiget"),
        (
            IndexLookupMode::Chunked {
                group_size: 256,
                max_in_flight: 1,
            },
            "chunked-256-1",
        ),
        (
            IndexLookupMode::Chunked {
                group_size: 256,
                max_in_flight: 2,
            },
            "chunked-256-2",
        ),
        (
            IndexLookupMode::Chunked {
                group_size: 256,
                max_in_flight: 4,
            },
            "chunked-256-4",
        ),
        (
            IndexLookupMode::Chunked {
                group_size: 256,
                max_in_flight: 8,
            },
            "chunked-256-8",
        ),
    ]
}

fn measured_batch(seed_debit: &Transaction) -> Vec<Transaction> {
    let mut transactions = Vec::with_capacity(261);
    transactions.push(txn(0, 3, 1_002, Operation::Credit, 2));
    for id in 1..=255 {
        transactions.push(txn(1, id, 2_000 + id, Operation::Credit, 1));
    }

    transactions.push(transactions[0].clone());
    transactions.push(txn(0, 3, 1_002, Operation::Credit, 3));
    transactions.push(seed_debit.clone());
    transactions.push(txn(0, 2, 1_001, Operation::Debit, 4));
    transactions.push(txn(0, 4, 1_003, Operation::Debit, 5));
    assert_eq!(transactions.len(), 261);
    transactions
}

async fn assert_mode_matches_legacy(mode: IndexLookupMode, label: &str) {
    let baseline_directory = TempDb::new();
    let indexed_directory = TempDb::new();
    let baseline_path = baseline_directory.path();
    let indexed_path = indexed_directory.path();
    let config = IndexLookupConfig::new(mode).unwrap();

    let baseline = AccountStore::open(&baseline_path, 2, BalanceMode::PerBatch, 100)
        .await
        .unwrap_or_else(|error| panic!("open legacy baseline for {label}: {error}"));
    let indexed = AccountStore::open(&indexed_path, 2, BalanceMode::PerBatch, 100)
        .await
        .unwrap_or_else(|error| panic!("open {label} store: {error}"));

    let seed_credit = txn(0, 1, 1_000, Operation::Credit, 100);
    let seed_debit = txn(0, 2, 1_001, Operation::Debit, 3);
    let seed = vec![seed_credit, seed_debit.clone()];
    let baseline_seed = baseline.handle_batch(seed.clone()).await.unwrap();
    let indexed_seed = indexed.handle_batch(seed).await.unwrap();
    assert_eq!(indexed_seed, baseline_seed, "seed replies for {label}");
    assert_transaction(&baseline_seed[0], TransactionStatus::Applied, 100, 1, false);
    assert_transaction(&baseline_seed[1], TransactionStatus::Applied, 97, 2, false);
    assert_store_state(&baseline, 2, &[(0, 97), (1, 0)]);
    assert_store_state(&indexed, 2, &[(0, 97), (1, 0)]);

    let measured = measured_batch(&seed_debit);
    let baseline_replies = baseline.handle_batch(measured.clone()).await.unwrap();
    let indexed_result = indexed
        .handle_batch_with_index_lookup(measured, config)
        .await
        .unwrap_or_else(|error| panic!("run {label} lookup batch: {error}"));
    assert_eq!(
        indexed_result.metrics.mode, mode,
        "selected mode for {label}"
    );
    assert_eq!(
        indexed_result.metrics.transaction_count, 261,
        "reported transaction count for {label}"
    );
    assert_eq!(indexed_result.replies.len(), 261, "reply count for {label}");
    assert_eq!(
        indexed_result.replies, baseline_replies,
        "full replies for {label}"
    );
    assert_transaction(
        &indexed_result.replies[0],
        TransactionStatus::Applied,
        99,
        3,
        false,
    );
    assert_transaction(
        &indexed_result.replies[255],
        TransactionStatus::Applied,
        255,
        258,
        false,
    );
    assert_transaction(
        &indexed_result.replies[256],
        TransactionStatus::Applied,
        99,
        3,
        true,
    );
    assert_eq!(indexed_result.replies[257], Reply::Conflict);
    assert_transaction(
        &indexed_result.replies[258],
        TransactionStatus::Applied,
        97,
        2,
        true,
    );
    assert_eq!(indexed_result.replies[259], Reply::Conflict);
    assert_transaction(
        &indexed_result.replies[260],
        TransactionStatus::Applied,
        94,
        259,
        false,
    );
    assert_store_state(&baseline, 259, &[(0, 94), (1, 255)]);
    assert_store_state(&indexed, 259, &[(0, 94), (1, 255)]);

    let refunds = vec![
        refund_txn(5, 1_004, seed_debit.key),
        refund_txn(6, 1_005, seed_debit.key),
    ];
    let baseline_refunds = baseline.handle_batch(refunds.clone()).await.unwrap();
    let indexed_refunds = indexed
        .handle_batch_with_index_lookup(refunds, config)
        .await
        .unwrap_or_else(|error| panic!("run {label} refund batch: {error}"));
    assert_eq!(
        indexed_refunds.metrics.mode, mode,
        "refund mode for {label}"
    );
    assert_eq!(
        indexed_refunds.replies, baseline_refunds,
        "refund replies for {label}"
    );
    assert_transaction(
        &indexed_refunds.replies[0],
        TransactionStatus::Applied,
        97,
        260,
        false,
    );
    assert_transaction(
        &indexed_refunds.replies[1],
        TransactionStatus::RefundAlreadyUsed,
        97,
        261,
        false,
    );
    assert_store_state(&baseline, 261, &[(0, 97), (1, 255)]);
    assert_store_state(&indexed, 261, &[(0, 97), (1, 255)]);

    let persisted_retry = txn(0, 3, 1_002, Operation::Credit, 2);
    let baseline_retry = baseline
        .handle_batch(vec![persisted_retry.clone()])
        .await
        .unwrap();
    let indexed_retry = indexed
        .handle_batch_with_index_lookup(vec![persisted_retry], config)
        .await
        .unwrap_or_else(|error| panic!("run {label} persisted retry: {error}"));
    assert_eq!(indexed_retry.metrics.mode, mode, "retry mode for {label}");
    assert_eq!(
        indexed_retry.replies, baseline_retry,
        "retry replies for {label}"
    );
    assert_transaction(
        &indexed_retry.replies[0],
        TransactionStatus::Applied,
        99,
        3,
        true,
    );
    assert_store_state(&baseline, 261, &[(0, 97), (1, 255)]);
    assert_store_state(&indexed, 261, &[(0, 97), (1, 255)]);

    baseline.validate_integrity().await.unwrap();
    indexed.validate_integrity().await.unwrap();
    baseline.shutdown().await.unwrap();
    indexed.shutdown().await.unwrap();

    let recovered_baseline = AccountStore::open(&baseline_path, 2, BalanceMode::PerBatch, 100)
        .await
        .unwrap_or_else(|error| panic!("reopen legacy baseline for {label}: {error}"));
    let recovered_indexed = AccountStore::open(&indexed_path, 2, BalanceMode::PerBatch, 100)
        .await
        .unwrap_or_else(|error| panic!("reopen {label} store: {error}"));
    assert_store_state(&recovered_baseline, 261, &[(0, 97), (1, 255)]);
    assert_store_state(&recovered_indexed, 261, &[(0, 97), (1, 255)]);
    recovered_baseline.validate_integrity().await.unwrap();
    recovered_indexed.validate_integrity().await.unwrap();
    recovered_baseline.shutdown().await.unwrap();
    recovered_indexed.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn index_lookup_strategies_match_legacy_batch_semantics() {
    for (mode, label) in lookup_modes() {
        assert_mode_matches_legacy(mode, label).await;
    }
}

#[test]
fn chunked_lookup_configuration_rejects_out_of_bounds_limits() {
    for mode in [
        IndexLookupMode::Chunked {
            group_size: 0,
            max_in_flight: 1,
        },
        IndexLookupMode::Chunked {
            group_size: 2_049,
            max_in_flight: 1,
        },
        IndexLookupMode::Chunked {
            group_size: 1,
            max_in_flight: 0,
        },
        IndexLookupMode::Chunked {
            group_size: 1,
            max_in_flight: 9,
        },
    ] {
        assert!(
            IndexLookupConfig::new(mode).is_err(),
            "expected invalid config to fail: {mode:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn empty_index_lookup_batches_return_empty_replies_without_advancing_sequence() {
    let directory = TempDb::new();
    let path = directory.path();
    let store = AccountStore::open(&path, 1, BalanceMode::PerBatch, 100)
        .await
        .unwrap();

    for (mode, label) in lookup_modes() {
        let config = IndexLookupConfig::new(mode).unwrap();
        let result = store
            .handle_batch_with_index_lookup(Vec::new(), config)
            .await
            .unwrap_or_else(|error| panic!("empty {label} batch failed: {error}"));
        assert!(result.replies.is_empty(), "empty replies for {label}");
        assert_eq!(result.metrics.transaction_count, 0, "count for {label}");
        assert_eq!(result.metrics.mode, mode, "mode for {label}");
        let expected_query_wall = match mode {
            IndexLookupMode::PointGet => None,
            IndexLookupMode::WholeBatchMultiGet | IndexLookupMode::Chunked { .. } => Some(0),
        };
        assert_eq!(
            result.metrics.query_wall_ns, expected_query_wall,
            "query wall time for {label}"
        );
        assert_store_state(&store, 0, &[(0, 0)]);
    }

    store.validate_integrity().await.unwrap();
    store.shutdown().await.unwrap();
}
