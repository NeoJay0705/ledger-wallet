use super::*;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex as StdMutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
            "ledger-wallet-index-lookup-fault-test-{}-{timestamp}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&root).expect("create unique fault-test directory");
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

struct ReleaseBlockedQuery(Arc<(StdMutex<bool>, Condvar)>);

impl ReleaseBlockedQuery {
    fn new() -> Self {
        Self(Arc::new((StdMutex::new(false), Condvar::new())))
    }

    fn release(&self) {
        let (lock, changed) = &*self.0;
        if let Ok(mut released) = lock.lock() {
            *released = true;
            changed.notify_all();
        }
    }
}

impl Drop for ReleaseBlockedQuery {
    fn drop(&mut self) {
        self.release();
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

fn credits(count: usize, first_id: u64) -> Vec<Transaction> {
    (0..count)
        .map(|offset| {
            let offset = u64::try_from(offset).expect("small test offset fits u64");
            txn(
                0,
                first_id + offset,
                10_000 + first_id + offset,
                Operation::Credit,
                1,
            )
        })
        .collect()
}

fn prefetch_modes() -> [(IndexLookupMode, &'static str); 5] {
    [
        (IndexLookupMode::WholeBatchMultiGet, "whole-batch"),
        (
            IndexLookupMode::Chunked {
                group_size: 256,
                max_in_flight: 1,
            },
            "chunked-p1",
        ),
        (
            IndexLookupMode::Chunked {
                group_size: 256,
                max_in_flight: 2,
            },
            "chunked-p2",
        ),
        (
            IndexLookupMode::Chunked {
                group_size: 256,
                max_in_flight: 4,
            },
            "chunked-p4",
        ),
        (
            IndexLookupMode::Chunked {
                group_size: 256,
                max_in_flight: 8,
            },
            "chunked-p8",
        ),
    ]
}

fn all_modes() -> [(IndexLookupMode, &'static str); 6] {
    [
        (IndexLookupMode::PointGet, "point-get"),
        (IndexLookupMode::WholeBatchMultiGet, "whole-batch"),
        (
            IndexLookupMode::Chunked {
                group_size: 256,
                max_in_flight: 1,
            },
            "chunked-p1",
        ),
        (
            IndexLookupMode::Chunked {
                group_size: 256,
                max_in_flight: 2,
            },
            "chunked-p2",
        ),
        (
            IndexLookupMode::Chunked {
                group_size: 256,
                max_in_flight: 4,
            },
            "chunked-p4",
        ),
        (
            IndexLookupMode::Chunked {
                group_size: 256,
                max_in_flight: 8,
            },
            "chunked-p8",
        ),
    ]
}

async fn open_store(path: &Path, users: usize) -> AccountStore {
    AccountStore::open(path, users, BalanceMode::PerBatch, 100)
        .await
        .expect("open temporary account store")
}

fn durable_latest_seq(store: &AccountStore) -> Option<u64> {
    store
        .inner
        .db
        .get(store.inner.keyspace.latest_seq())
        .expect("read durable latest sequence")
        .map(|bytes| decode_u64(&bytes, "latest sequence").expect("decode durable sequence"))
}

fn assert_gate_free(store: &AccountStore) {
    let guard = store
        .inner
        .batch_gate
        .try_lock()
        .expect("batch gate released");
    drop(guard);
}

async fn wait_for_coordinator_handles_to_drop(store: &AccountStore) -> bool {
    tokio::time::timeout(Duration::from_secs(5), async {
        while Arc::strong_count(&store.inner) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok()
}

async fn assert_no_new_commit(
    store: &AccountStore,
    expected_seq: u64,
    expected_balance: u64,
    first_new_key: TransactionKey,
) {
    assert_eq!(store.latest_seq(), expected_seq);
    assert_eq!(store.balance(0).unwrap(), expected_balance);
    assert_eq!(durable_latest_seq(store), Some(expected_seq));
    assert!(
        !store.has_ledger_sequence(expected_seq + 1).await.unwrap(),
        "unexpected ledger sequence {}",
        expected_seq + 1
    );
    assert!(
        !store.has_transaction_index(first_new_key).await.unwrap(),
        "unexpected transaction index for {first_new_key:?}"
    );
    assert_gate_free(store);
}

async fn wait_for_signal(receiver: mpsc::Receiver<()>) -> bool {
    tokio::time::timeout(
        Duration::from_secs(4),
        tokio::task::spawn_blocking(move || receiver.recv_timeout(Duration::from_secs(3))),
    )
    .await
    .is_ok_and(|joined| joined.is_ok_and(|received| received.is_ok()))
}

async fn assert_prefetch_fault_is_atomic(
    mode: IndexLookupMode,
    fault: IndexLookupTestFault,
    label: &str,
) {
    let directory = TempDb::new();
    let path = directory.path();
    let store = open_store(&path, 1).await;
    let transactions = credits(257, 1);
    store.push_index_lookup_test_fault(fault);

    let result = store
        .handle_batch_with_index_lookup(transactions.clone(), IndexLookupConfig::new(mode).unwrap())
        .await;
    assert!(result.is_err(), "{label} should reject the batch");
    assert_no_new_commit(&store, 0, 0, transactions[0].key).await;
    store.validate_integrity().await.unwrap();
    store.shutdown().await.unwrap();

    let reopened = open_store(&path, 1).await;
    assert_no_new_commit(&reopened, 0, 0, transactions[0].key).await;
    reopened.validate_integrity().await.unwrap();
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prefetch_read_panics_and_failures_are_atomic_for_all_multiget_modes() {
    for (mode, mode_label) in prefetch_modes() {
        assert_prefetch_fault_is_atomic(
            mode,
            IndexLookupTestFault::FailReadGroup(0),
            &format!("{mode_label} read failure"),
        )
        .await;
        assert_prefetch_fault_is_atomic(
            mode,
            IndexLookupTestFault::PanicReadGroup(0),
            &format!("{mode_label} read panic"),
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn malformed_group_results_are_rejected_before_commit() {
    let mode = IndexLookupMode::Chunked {
        group_size: 256,
        max_in_flight: 2,
    };
    for (fault, label) in [
        (
            IndexLookupTestFault::CorruptGroupBounds(0),
            "corrupted result bounds",
        ),
        (
            IndexLookupTestFault::DropGroupResult(0),
            "dropped group result",
        ),
        (
            IndexLookupTestFault::DuplicateGroupResult(0),
            "duplicate group result",
        ),
    ] {
        assert_prefetch_fault_is_atomic(mode, fault, label).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn corrupt_or_mismatched_persisted_indexes_fail_before_new_commit() {
    let modes = [
        (IndexLookupMode::WholeBatchMultiGet, "whole-batch"),
        (
            IndexLookupMode::Chunked {
                group_size: 256,
                max_in_flight: 4,
            },
            "chunked-p4",
        ),
    ];

    for (mode, mode_label) in modes {
        for (corruption, corruption_label) in [
            (PersistedIndexCorruption::MalformedBytes, "malformed-bytes"),
            (
                PersistedIndexCorruption::MismatchedDecodedKey,
                "mismatched-decoded-key",
            ),
        ] {
            let directory = TempDb::new();
            let path = directory.path();
            let store = open_store(&path, 1).await;
            let persisted = txn(0, 10_000, 1, Operation::Credit, 5);
            let seed_replies = store.handle_batch(vec![persisted.clone()]).await.unwrap();
            assert_eq!(seed_replies.len(), 1);
            let next = txn(0, 10_001, 2, Operation::Credit, 2);
            let key = store.inner.keyspace.transaction(persisted.key);

            let encoded = match corruption {
                PersistedIndexCorruption::MalformedBytes => vec![0],
                PersistedIndexCorruption::MismatchedDecodedKey => {
                    let mismatched = StoredTransaction {
                        request: txn(0, 99_999, 99, Operation::Credit, 5),
                        result: TransactionResult {
                            status: TransactionStatus::Applied,
                            balance: 5,
                            seq: 1,
                        },
                    };
                    encode_transaction(&mismatched)
                }
            };
            if matches!(corruption, PersistedIndexCorruption::MismatchedDecodedKey) {
                assert!(decode_transaction(&encoded).is_ok());
            }
            store
                .inner
                .db
                .put(key, encoded)
                .expect("inject persisted index corruption");

            let result = store
                .handle_batch_with_index_lookup(
                    vec![persisted, next.clone()],
                    IndexLookupConfig::new(mode).unwrap(),
                )
                .await;
            assert!(
                result.is_err(),
                "{mode_label} should reject {corruption_label}"
            );
            assert_eq!(store.latest_seq(), 1);
            assert_eq!(store.balance(0).unwrap(), 5);
            assert_eq!(durable_latest_seq(&store), Some(1));
            assert!(!store.has_ledger_sequence(2).await.unwrap());
            assert!(!store.has_transaction_index(next.key).await.unwrap());
            assert_gate_free(&store);
            store.shutdown().await.unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_early_query_drains_running_groups_and_keeps_batch_gate_held() {
    let directory = TempDb::new();
    let path = directory.path();
    let store = open_store(&path, 1).await;
    let transactions = credits(513, 1);
    let release = ReleaseBlockedQuery::new();
    let (started_tx, started_rx) = mpsc::channel();
    store.push_index_lookup_test_fault(IndexLookupTestFault::FailReadGroup(0));
    store.push_index_lookup_test_fault(IndexLookupTestFault::BlockReadGroup(
        1,
        started_tx,
        Arc::clone(&release.0),
    ));

    let caller_store = store.clone();
    let caller = tokio::spawn(async move {
        caller_store
            .handle_batch_with_index_lookup(
                transactions,
                IndexLookupConfig::new(IndexLookupMode::Chunked {
                    group_size: 256,
                    max_in_flight: 2,
                })
                .unwrap(),
            )
            .await
    });

    let second_group_started = wait_for_signal(started_rx).await;
    let caller_still_waiting = !caller.is_finished();
    let gate_still_held = store.inner.batch_gate.try_lock().is_err();
    release.release();
    let caller_result = tokio::time::timeout(Duration::from_secs(5), caller)
        .await
        .expect("draining failed query jobs should finish")
        .expect("batch caller task should join");
    assert!(
        second_group_started,
        "second query group should have started"
    );
    assert!(
        caller_still_waiting,
        "caller must await all submitted groups"
    );
    assert!(
        gate_still_held,
        "batch gate must remain held while a group runs"
    );
    assert!(
        caller_result.is_err(),
        "injected query failure should return Err"
    );
    assert_no_new_commit(&store, 0, 0, credits(1, 1)[0].key).await;
    assert!(
        wait_for_coordinator_handles_to_drop(&store).await,
        "query coordinator should release its store handle"
    );

    let next = txn(0, 50_000, 50_000, Operation::Credit, 1);
    let replies = store.handle_batch(vec![next.clone()]).await.unwrap();
    assert_transaction(&replies[0], TransactionStatus::Applied, 1, 1, false);
    assert_eq!(store.latest_seq(), 1);
    assert_eq!(store.balance(0).unwrap(), 1);
    assert_eq!(durable_latest_seq(&store), Some(1));
    store.validate_integrity().await.unwrap();
    store.shutdown().await.unwrap();

    let reopened = open_store(&path, 1).await;
    assert_eq!(reopened.latest_seq(), 1);
    assert_eq!(reopened.balance(0).unwrap(), 1);
    assert!(reopened.has_ledger_sequence(1).await.unwrap());
    reopened.validate_integrity().await.unwrap();
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelling_batch_awaiter_does_not_cancel_coordinator_commit() {
    let directory = TempDb::new();
    let path = directory.path();
    let store = open_store(&path, 1).await;
    let transactions = credits(2, 1);
    let retry = transactions.clone();
    let release = ReleaseBlockedQuery::new();
    let (started_tx, started_rx) = mpsc::channel();
    store.push_index_lookup_test_fault(IndexLookupTestFault::BlockReadGroup(
        0,
        started_tx,
        Arc::clone(&release.0),
    ));

    let caller_store = store.clone();
    let caller = tokio::spawn(async move {
        caller_store
            .handle_batch_with_index_lookup(
                transactions,
                IndexLookupConfig::new(IndexLookupMode::Chunked {
                    group_size: 256,
                    max_in_flight: 1,
                })
                .unwrap(),
            )
            .await
    });
    let query_started = wait_for_signal(started_rx).await;
    caller.abort();
    let caller_cancelled = caller
        .await
        .expect_err("outer awaiter should be aborted")
        .is_cancelled();
    let gate_held_after_cancel = store.inner.batch_gate.try_lock().is_err();
    release.release();
    let gate_acquired_after_completion =
        tokio::time::timeout(Duration::from_secs(5), store.inner.batch_gate.lock())
            .await
            .is_ok();
    assert!(query_started, "blocked query worker should have started");
    assert!(caller_cancelled, "caller task should report cancellation");
    assert!(
        gate_held_after_cancel,
        "coordinator should retain the batch gate after caller cancellation"
    );
    assert!(
        gate_acquired_after_completion,
        "coordinator should release the batch gate after committing"
    );
    assert!(
        wait_for_coordinator_handles_to_drop(&store).await,
        "cancelled caller's coordinator should release its store handle"
    );
    assert_eq!(store.latest_seq(), 2);
    assert_eq!(store.balance(0).unwrap(), 2);
    assert_eq!(durable_latest_seq(&store), Some(2));
    assert!(store.has_ledger_sequence(1).await.unwrap());
    assert!(store.has_ledger_sequence(2).await.unwrap());
    for transaction in &retry {
        assert!(store.has_transaction_index(transaction.key).await.unwrap());
    }
    store.validate_integrity().await.unwrap();
    store.shutdown().await.unwrap();

    let reopened = open_store(&path, 1).await;
    assert_eq!(reopened.latest_seq(), 2);
    assert_eq!(reopened.balance(0).unwrap(), 2);
    reopened.validate_integrity().await.unwrap();
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn injected_sync_write_failure_is_atomic_and_retryable_for_every_mode() {
    for (mode, label) in all_modes() {
        let directory = TempDb::new();
        let path = directory.path();
        let store = open_store(&path, 1).await;
        let transactions = credits(3, 1);
        store.push_index_lookup_test_fault(IndexLookupTestFault::FailSyncWrite);

        let result = store
            .handle_batch_with_index_lookup(
                transactions.clone(),
                IndexLookupConfig::new(mode).unwrap(),
            )
            .await;
        assert!(
            result.is_err(),
            "{label} sync-write fault should return Err"
        );
        assert_no_new_commit(&store, 0, 0, transactions[0].key).await;

        let retry = store
            .handle_batch_with_index_lookup(
                transactions.clone(),
                IndexLookupConfig::new(mode).unwrap(),
            )
            .await
            .unwrap_or_else(|error| panic!("{label} retry failed: {error}"));
        assert_eq!(retry.replies.len(), 3, "reply count for {label}");
        for (index, reply) in retry.replies.iter().enumerate() {
            let amount = u64::try_from(index + 1).unwrap();
            assert_transaction(reply, TransactionStatus::Applied, amount, amount, false);
        }
        assert_eq!(store.latest_seq(), 3);
        assert_eq!(store.balance(0).unwrap(), 3);
        assert_eq!(durable_latest_seq(&store), Some(3));
        store.validate_integrity().await.unwrap();
        store.shutdown().await.unwrap();

        let reopened = open_store(&path, 1).await;
        assert_eq!(reopened.latest_seq(), 3);
        assert_eq!(reopened.balance(0).unwrap(), 3);
        reopened.validate_integrity().await.unwrap();
        reopened.shutdown().await.unwrap();
    }
}

#[derive(Clone, Copy)]
enum PersistedIndexCorruption {
    MalformedBytes,
    MismatchedDecodedKey,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_groups_reconstruct_results_in_original_request_order() {
    let directory = TempDb::new();
    let path = directory.path();
    let store = open_store(&path, 1).await;
    let persisted = txn(0, 10_000, 1, Operation::Credit, 1);
    let seeded = store.handle_batch(vec![persisted.clone()]).await.unwrap();
    assert_transaction(&seeded[0], TransactionStatus::Applied, 1, 1, false);

    let mut transactions = credits(300, 1);
    transactions[256] = persisted;
    let release_group_zero = ReleaseBlockedQuery::new();
    let release_group_one = ReleaseBlockedQuery::new();
    let (group_zero_started_tx, group_zero_started_rx) = mpsc::channel();
    let (group_one_started_tx, group_one_started_rx) = mpsc::channel();
    let (group_collected_tx, group_collected_rx) = mpsc::channel();
    store.push_index_lookup_test_fault(IndexLookupTestFault::BlockReadGroup(
        0,
        group_zero_started_tx,
        Arc::clone(&release_group_zero.0),
    ));
    store.push_index_lookup_test_fault(IndexLookupTestFault::BlockReadGroup(
        1,
        group_one_started_tx,
        Arc::clone(&release_group_one.0),
    ));
    store.push_index_lookup_test_fault(IndexLookupTestFault::GroupCollectedSignal(
        1,
        group_collected_tx,
    ));

    let caller_store = store.clone();
    let caller = tokio::spawn(async move {
        caller_store
            .handle_batch_with_index_lookup(
                transactions,
                IndexLookupConfig::new(IndexLookupMode::Chunked {
                    group_size: 256,
                    max_in_flight: 2,
                })
                .unwrap(),
            )
            .await
    });

    let group_zero_started = wait_for_signal(group_zero_started_rx).await;
    let group_one_started = wait_for_signal(group_one_started_rx).await;
    release_group_one.release();
    let group_one_collected = tokio::time::timeout(
        Duration::from_secs(4),
        tokio::task::spawn_blocking(move || {
            group_collected_rx.recv_timeout(Duration::from_secs(3))
        }),
    )
    .await
    .is_ok_and(|joined| joined.is_ok_and(|received| received.is_ok_and(|group| group == 1)));
    let caller_waited_for_group_zero = !caller.is_finished();
    release_group_zero.release();
    let result = tokio::time::timeout(Duration::from_secs(5), caller)
        .await
        .expect("batch should complete after releasing group zero")
        .expect("batch caller task should join")
        .expect("batch should commit after every lookup group is collected");

    assert!(group_zero_started, "group zero should have blocked");
    assert!(group_one_started, "group one should have blocked");
    assert!(
        group_one_collected,
        "released group one should be collected while group zero remains blocked"
    );
    assert!(
        caller_waited_for_group_zero,
        "the batch should remain pending while group zero is blocked"
    );
    assert!(
        wait_for_coordinator_handles_to_drop(&store).await,
        "query coordinator should release its store handle"
    );
    assert_eq!(result.metrics.groups.len(), 2);
    assert_eq!(result.metrics.groups[0].group_index, 1);
    assert_eq!(result.metrics.groups[1].group_index, 0);
    assert_eq!(result.metrics.groups[0].key_count, 44);
    assert_eq!(result.metrics.groups[1].key_count, 256);
    assert_eq!(result.metrics.max_observed_running_query_jobs, 2);
    assert_eq!(result.replies.len(), 300);
    for (position, reply) in result.replies.iter().enumerate() {
        if position == 256 {
            assert_transaction(reply, TransactionStatus::Applied, 1, 1, true);
        } else {
            let (balance, seq) = if position < 256 {
                ((position + 2) as u64, (position + 2) as u64)
            } else {
                ((position + 1) as u64, (position + 1) as u64)
            };
            assert_transaction(reply, TransactionStatus::Applied, balance, seq, false);
        }
    }
    assert_eq!(store.latest_seq(), 300);
    assert_eq!(store.balance(0).unwrap(), 300);
    assert_eq!(durable_latest_seq(&store), Some(300));
    store.validate_integrity().await.unwrap();
    store.shutdown().await.unwrap();

    let reopened = open_store(&path, 1).await;
    assert_eq!(reopened.latest_seq(), 300);
    assert_eq!(reopened.balance(0).unwrap(), 300);
    reopened.validate_integrity().await.unwrap();
    reopened.shutdown().await.unwrap();
}

struct FakeRefundHistory(HistoricalDebit);

impl RefundHistory for FakeRefundHistory {
    fn lookup_debit(&self, _key: TransactionKey) -> Result<Option<HistoricalDebit>, String> {
        Ok(Some(self.0))
    }

    fn projection_progress(&self) -> Result<u64, String> {
        Ok(0)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn historical_refund_resolution_checks_amount_before_staged_used_marker() {
    let directory = TempDb::new();
    let path = directory.path();
    let store = open_store(&path, 1).await;
    let target = TransactionKey {
        account_id: 0,
        tx_id: 40,
        transaction_at: 4_000,
    };
    let staged_refunds = HashSet::from([target]);
    let staged = HashMap::new();
    let history = FakeRefundHistory(HistoricalDebit {
        amount: 3,
        already_refunded: false,
    });
    let refund = Transaction {
        key: TransactionKey {
            account_id: 0,
            tx_id: 41,
            transaction_at: 4_001,
        },
        operation: Operation::Refund,
        amount: 3,
        refund_of: Some(target),
    };
    let valid_amount = resolve_refund_target(
        &store.inner.db,
        &store.inner.keyspace,
        &staged,
        &refund,
        &staged_refunds,
        Some(&history),
        1,
    )
    .unwrap();
    assert!(matches!(valid_amount, RefundTarget::Used));

    let invalid_amount = resolve_refund_target(
        &store.inner.db,
        &store.inner.keyspace,
        &staged,
        &Transaction {
            amount: 2,
            ..refund
        },
        &staged_refunds,
        Some(&history),
        1,
    )
    .unwrap();
    assert!(matches!(invalid_amount, RefundTarget::Invalid));
    store.shutdown().await.unwrap();
}
