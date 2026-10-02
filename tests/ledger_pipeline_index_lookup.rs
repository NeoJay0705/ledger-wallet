#![allow(dead_code)]

#[path = "../benches/support/ledger_account_store.rs"]
mod ledger_account_store;
#[path = "../benches/support/ledger_projection_worker.rs"]
mod ledger_projection_worker;
#[path = "../benches/support/ledger_time_boundary.rs"]
mod ledger_time_boundary;
#[path = "../benches/support/request_batch_queue.rs"]
mod request_batch_queue;

use ledger_account_store::{
    AccountStore, BalanceMode, IndexLookupBatchMetrics, IndexLookupConfig, IndexLookupMode,
    Operation, RefundHistory, Reply, Transaction, TransactionKey, TransactionResult,
    TransactionStatus,
};
use ledger_projection_worker::{HistoricalLookup, MockProjectionStore};
use ledger_time_boundary::{
    AdmissionGate, AdmittedTransaction, GateDecision, GuardedReply, ProjectionProgress,
    RoutedReply, WatermarkManager,
};
use request_batch_queue::{BatchQueue, BatchWorker, RequestHandle};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::watch;

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
            "ledger-pipeline-index-lookup-test-{}-{timestamp}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&root).expect("create unique test directory");
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

struct Pipeline {
    source: AccountStore,
    history: Arc<MockProjectionStore>,
    gate: Arc<AdmissionGate>,
    progress: Arc<ProjectionProgress>,
    manager: WatermarkManager,
    queue: BatchQueue<AdmittedTransaction, GuardedReply>,
    worker: Option<BatchWorker>,
    head_tx: watch::Sender<u64>,
    head_rx: watch::Receiver<u64>,
    lookup: IndexLookupConfig,
    batch_metrics: Arc<Mutex<Vec<IndexLookupBatchMetrics>>>,
}

impl Pipeline {
    async fn open(
        path: &Path,
        mode: IndexLookupMode,
        balance_mode: BalanceMode,
        checkpoint_quantity: u64,
        initial_watermark: u64,
        max_batch_size: usize,
        batch_timeout: Duration,
    ) -> Self {
        let source = AccountStore::open(path, 1, balance_mode, checkpoint_quantity)
            .await
            .expect("open pipeline source");
        let history = Arc::new(MockProjectionStore::default());
        let history_trait: Arc<dyn RefundHistory> = history.clone();
        source
            .set_refund_history(history_trait)
            .expect("install the historical destination");
        Self::from_store(
            source,
            history,
            mode,
            initial_watermark,
            max_batch_size,
            batch_timeout,
        )
        .await
    }

    async fn from_store(
        source: AccountStore,
        history: Arc<MockProjectionStore>,
        mode: IndexLookupMode,
        initial_watermark: u64,
        max_batch_size: usize,
        batch_timeout: Duration,
    ) -> Self {
        let durable_progress = source
            .durable_projection_progress()
            .await
            .expect("read initial durable projection progress");
        let progress = ProjectionProgress::new(durable_progress);
        let gate = AdmissionGate::new(initial_watermark);
        let manager = WatermarkManager::new(Arc::clone(&gate), Arc::clone(&progress));
        let (head_tx, head_rx) = watch::channel(source.latest_seq());
        let batch_metrics = Arc::new(Mutex::new(Vec::new()));
        let lookup = IndexLookupConfig::new(mode).expect("valid pipeline lookup mode");
        let (queue, worker) = ledger_time_boundary::spawn_commit_queue_with_index_lookup(
            source.clone(),
            head_tx.clone(),
            64,
            max_batch_size,
            batch_timeout,
            lookup,
            Arc::clone(&batch_metrics),
        )
        .expect("start indexed commit queue");
        Self {
            source,
            history,
            gate,
            progress,
            manager,
            queue,
            worker: Some(worker),
            head_tx,
            head_rx,
            lookup,
            batch_metrics,
        }
    }

    async fn close(self) {
        let Self {
            source,
            queue,
            worker,
            ..
        } = self;
        drop(queue);
        if let Some(worker) = worker {
            worker.join().await.expect("join the commit queue");
        }
        source.shutdown().await.expect("close pipeline source");
    }
}

struct ReleaseOnDrop(Arc<(Mutex<bool>, Condvar)>);

impl ReleaseOnDrop {
    fn release(&self) {
        let (released, changed) = &*self.0;
        *released.lock().expect("query-release mutex is healthy") = true;
        changed.notify_all();
    }
}

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.release();
    }
}

fn txn(account_id: u64, tx_id: u64, at: u64, operation: Operation, amount: u64) -> Transaction {
    Transaction {
        key: TransactionKey {
            account_id,
            tx_id,
            transaction_at: at,
        },
        operation,
        amount,
        refund_of: None,
    }
}

async fn enqueue_current(
    pipeline: &Pipeline,
    transaction: Transaction,
) -> RequestHandle<GuardedReply> {
    let guard = match pipeline
        .gate
        .admit(transaction.key.transaction_at)
        .await
        .expect("admit current transaction")
    {
        GateDecision::Commit(guard) => guard,
        GateDecision::Historical => panic!("test transaction should use the commit path"),
    };
    pipeline
        .queue
        .submit(AdmittedTransaction::with_guard_for_test(transaction, guard))
        .await
        .expect("enqueue accepted transaction")
}

async fn commit_batch(pipeline: &Pipeline, transactions: Vec<Transaction>) -> Vec<RoutedReply> {
    let mut handles = Vec::with_capacity(transactions.len());
    for transaction in transactions {
        handles.push(enqueue_current(pipeline, transaction).await);
    }
    let mut replies = Vec::with_capacity(handles.len());
    for handle in handles {
        let completed = handle.wait().await.expect("accepted commit completes");
        replies.push(completed.reply.observe());
    }
    replies
}

async fn route(pipeline: &Pipeline, transaction: Transaction) -> RoutedReply {
    ledger_time_boundary::route_request(
        &pipeline.gate,
        &pipeline.queue,
        &pipeline.history,
        Duration::ZERO,
        transaction,
        Instant::now(),
    )
    .await
    .expect("route request")
    .reply
}

async fn project_to(pipeline: &Pipeline, target: u64) {
    let mut next = pipeline
        .source
        .durable_projection_progress()
        .await
        .expect("read durable source projection progress")
        + 1;
    while next <= target {
        let read = pipeline
            .source
            .read_ledger_range(next, 64)
            .await
            .expect("read source rows for projection");
        assert!(
            !read.records.is_empty(),
            "projection has a contiguous source batch"
        );
        let acknowledged = pipeline
            .history
            .apply_batch(&read.records)
            .expect("apply records to the retained destination");
        pipeline
            .source
            .persist_projection_progress(acknowledged)
            .await
            .expect("sync source progress after destination apply");
        pipeline
            .progress
            .acknowledge(acknowledged)
            .expect("publish synced projection progress to waiters");
        next = acknowledged + 1;
    }
}

fn parallel_modes() -> [(IndexLookupMode, &'static str); 3] {
    [
        (IndexLookupMode::PointGet, "point_get"),
        (
            IndexLookupMode::Chunked {
                group_size: 2,
                max_in_flight: 4,
            },
            "p4",
        ),
        (
            IndexLookupMode::Chunked {
                group_size: 2,
                max_in_flight: 8,
            },
            "p8",
        ),
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pipeline_parallel_index_modes_preserve_order_and_history_gc_routing() {
    for (mode, label) in parallel_modes() {
        let directory = TempDb::new();
        let pipeline = Pipeline::open(
            &directory.path(),
            mode,
            BalanceMode::PerBatch,
            100,
            0,
            6,
            Duration::from_millis(100),
        )
        .await;
        let original = txn(0, 1, 100, Operation::Credit, 10);
        let debit = txn(0, 2, 101, Operation::Debit, 3);
        let second_credit = txn(0, 3, 102, Operation::Credit, 4);
        let conflicting_duplicate = txn(0, 1, 100, Operation::Credit, 11);
        let final_debit = txn(0, 4, 104, Operation::Debit, 2);

        // The exact duplicate and conflict fall in later lookup groups. The
        // queue still applies and replies in the accepted request order.
        let replies = commit_batch(
            &pipeline,
            vec![
                original.clone(),
                debit,
                second_credit,
                original.clone(),
                conflicting_duplicate.clone(),
                final_debit,
            ],
        )
        .await;
        assert_eq!(
            replies,
            [
                RoutedReply::Commit(Reply::Transaction {
                    status: TransactionStatus::Applied,
                    balance: 10,
                    seq: 1,
                    replayed: false,
                }),
                RoutedReply::Commit(Reply::Transaction {
                    status: TransactionStatus::Applied,
                    balance: 7,
                    seq: 2,
                    replayed: false,
                }),
                RoutedReply::Commit(Reply::Transaction {
                    status: TransactionStatus::Applied,
                    balance: 11,
                    seq: 3,
                    replayed: false,
                }),
                RoutedReply::Commit(Reply::Transaction {
                    status: TransactionStatus::Applied,
                    balance: 10,
                    seq: 1,
                    replayed: true,
                }),
                RoutedReply::Commit(Reply::Conflict),
                RoutedReply::Commit(Reply::Transaction {
                    status: TransactionStatus::Applied,
                    balance: 9,
                    seq: 4,
                    replayed: false,
                }),
            ],
            "ordered queue replies for {label}"
        );
        assert_eq!(
            pipeline.source.latest_seq(),
            4,
            "latest sequence for {label}"
        );
        assert_eq!(
            pipeline.source.balance(0).unwrap(),
            9,
            "balance for {label}"
        );
        let foreground_batches = pipeline.batch_metrics.lock().unwrap().clone();
        let foreground_batch = foreground_batches
            .first()
            .expect("the accepted foreground batch has lookup metrics");
        assert_eq!(
            foreground_batch.transaction_count, 6,
            "the cross-group duplicate and conflict share one queue batch for {label}"
        );
        match mode {
            IndexLookupMode::Chunked { .. } => {
                assert_eq!(
                    foreground_batch.groups_submitted, 3,
                    "lookup groups for {label}"
                );
                let mut first_positions: Vec<_> = foreground_batch
                    .groups
                    .iter()
                    .map(|group| group.first_position)
                    .collect();
                first_positions.sort_unstable();
                assert_eq!(first_positions, [0, 2, 4], "group boundaries for {label}");
            }
            IndexLookupMode::PointGet => {
                assert_eq!(foreground_batch.groups_submitted, 0);
            }
            IndexLookupMode::WholeBatchMultiGet => unreachable!(),
        }

        project_to(&pipeline, 4).await;
        assert_eq!(
            pipeline.history.progress(),
            4,
            "destination progress for {label}"
        );
        assert_eq!(
            pipeline.source.durable_projection_progress().await.unwrap(),
            4,
            "durable source projection for {label}"
        );
        assert!(
            pipeline
                .manager
                .advance_once_durable(200, &pipeline.source)
                .await
                .unwrap()
        );
        assert_eq!(
            pipeline.source.restored_projected_before().await.unwrap(),
            Some((200, 4)),
            "durable watermark target for {label}"
        );

        let gc = pipeline.source.collect_garbage(16).await.unwrap();
        assert_eq!(
            gc.deleted, 4,
            "safe GC deletes the covered old prefix for {label}"
        );
        assert_eq!(gc.gc_prefix_seq, 4, "safe GC prefix for {label}");
        assert_eq!(
            pipeline.source.gc_prefix_seq(),
            4,
            "published GC prefix for {label}"
        );
        assert!(!pipeline.source.has_ledger_sequence(1).await.unwrap());
        assert!(
            !pipeline
                .source
                .has_transaction_index(original.key)
                .await
                .unwrap()
        );

        assert_eq!(
            route(&pipeline, original.clone()).await,
            RoutedReply::HistoricalHit(TransactionResult {
                status: TransactionStatus::Applied,
                balance: 10,
                seq: 1,
            }),
            "old exact replay routes to the retained destination for {label}"
        );
        assert_eq!(
            route(&pipeline, conflicting_duplicate).await,
            RoutedReply::Conflict,
            "old conflict routes to the retained destination for {label}"
        );
        assert_eq!(
            route(&pipeline, txn(0, 99, 150, Operation::Credit, 1)).await,
            RoutedReply::HistoricalMiss,
            "missing expired key is rejected as a historical miss for {label}"
        );

        let retained = txn(0, 5, 210, Operation::Credit, 7);
        assert_eq!(
            route(&pipeline, retained.clone()).await,
            RoutedReply::Commit(Reply::Transaction {
                status: TransactionStatus::Applied,
                balance: 16,
                seq: 5,
                replayed: false,
            }),
            "post-watermark request uses the selected source lookup for {label}"
        );
        assert_eq!(
            route(&pipeline, retained.clone()).await,
            RoutedReply::Commit(Reply::Transaction {
                status: TransactionStatus::Applied,
                balance: 16,
                seq: 5,
                replayed: true,
            }),
            "retained current key still replays through the source index for {label}"
        );
        let second_gc = pipeline.source.collect_garbage(16).await.unwrap();
        assert_eq!(
            second_gc.deleted, 0,
            "GC cannot pass the published target for {label}"
        );
        assert_eq!(
            pipeline.source.gc_prefix_seq(),
            4,
            "GC remains at target for {label}"
        );
        assert!(pipeline.source.has_ledger_sequence(5).await.unwrap());
        assert_eq!(
            pipeline.source.durable_projection_progress().await.unwrap(),
            4,
            "GC remains bounded by durable projection for {label}"
        );
        assert_eq!(
            pipeline
                .source
                .verify_projection_destination()
                .await
                .unwrap(),
            4,
            "source is attached to the destination that was projected for {label}"
        );
        pipeline.source.validate_integrity().await.unwrap();

        let metrics = pipeline.batch_metrics.lock().unwrap().clone();
        assert!(
            !metrics.is_empty(),
            "queue records selected lookup batches for {label}"
        );
        assert!(metrics.iter().all(|sample| sample.mode == mode));
        assert!(metrics.iter().all(|sample| {
            sample.max_observed_in_flight_groups
                <= match mode {
                    IndexLookupMode::Chunked { max_in_flight, .. } => max_in_flight,
                    IndexLookupMode::PointGet | IndexLookupMode::WholeBatchMultiGet => 1,
                }
        }));

        pipeline.close().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pipeline_parallel_index_canceled_queue_waiter_keeps_guard_through_projection() {
    let directory = TempDb::new();
    let mut pipeline = Pipeline::open(
        &directory.path(),
        IndexLookupMode::Chunked {
            group_size: 2,
            max_in_flight: 4,
        },
        BalanceMode::PerBatch,
        100,
        10,
        2_048,
        Duration::from_millis(5),
    )
    .await;
    let (started, release) = pipeline.source.block_index_lookup_group_for_test(0);
    let release = ReleaseOnDrop(release);
    let transaction = txn(0, 1, 15, Operation::Credit, 6);
    let guard = match pipeline.gate.admit(15).await.unwrap() {
        GateDecision::Commit(guard) => guard,
        GateDecision::Historical => panic!("timestamp is above the starting watermark"),
    };
    let requests = vec![AdmittedTransaction::with_guard_for_test(
        transaction.clone(),
        guard,
    )];
    let outer_waiter = tokio::spawn(ledger_time_boundary::run_index_lookup_queue_coordinator(
        pipeline.source.clone(),
        pipeline.head_tx.clone(),
        requests,
        pipeline.lookup,
        Arc::clone(&pipeline.batch_metrics),
    ));
    tokio::task::spawn_blocking(move || {
        started
            .recv_timeout(Duration::from_secs(5))
            .expect("lookup group starts")
    })
    .await
    .expect("wait for blocked query worker");

    // This is the same queue-handler waiter used by the bounded queue. Its
    // owned child must retain the admission guard and commit-head sender.
    outer_waiter.abort();
    match outer_waiter.await {
        Err(error) => assert!(error.is_cancelled()),
        Ok(_) => panic!("queue handler waiter should have been cancelled"),
    }

    let manager = Arc::new(WatermarkManager::new(
        Arc::clone(&pipeline.gate),
        Arc::clone(&pipeline.progress),
    ));
    let source_for_watermark = pipeline.source.clone();
    let watermark_task = tokio::spawn(async move {
        manager
            .advance_once_durable(20, &source_for_watermark)
            .await
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if pipeline.gate.active_fence().unwrap() == Some(20) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("watermark fence installs while the accepted lookup is blocked");
    assert_eq!(pipeline.gate.watermark().unwrap(), 10);
    assert_eq!(pipeline.source.latest_seq(), 0);
    assert_eq!(
        pipeline.source.persisted_projected_before().await.unwrap(),
        None
    );

    release.release();
    tokio::time::timeout(Duration::from_secs(3), pipeline.head_rx.changed())
        .await
        .expect("accepted queue batch publishes its committed head")
        .expect("commit-head sender remains alive");
    assert_eq!(*pipeline.head_rx.borrow(), 1);
    assert_eq!(pipeline.source.latest_seq(), 1);
    assert_eq!(pipeline.gate.active_fence().unwrap(), Some(20));
    assert_eq!(pipeline.gate.watermark().unwrap(), 10);
    assert_eq!(pipeline.progress.sequence().unwrap(), 0);
    assert_eq!(
        pipeline.source.persisted_projected_before().await.unwrap(),
        None
    );

    project_to(&pipeline, 1).await;
    assert!(watermark_task.await.unwrap().unwrap());
    assert_eq!(pipeline.gate.watermark().unwrap(), 20);
    assert_eq!(
        pipeline.source.restored_projected_before().await.unwrap(),
        Some((20, 1))
    );
    assert_eq!(
        pipeline.history.lookup_transaction(&transaction).unwrap(),
        HistoricalLookup::ExactReplay(TransactionResult {
            status: TransactionStatus::Applied,
            balance: 6,
            seq: 1,
        })
    );
    pipeline.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pipeline_parallel_index_checkpoint_gc_reopen_and_new_handler_checkpoint() {
    let directory = TempDb::new();
    let first_mode = IndexLookupMode::Chunked {
        group_size: 2,
        max_in_flight: 4,
    };
    let pipeline = Pipeline::open(
        &directory.path(),
        first_mode,
        BalanceMode::Checkpoint,
        2,
        0,
        2_048,
        Duration::from_millis(5),
    )
    .await;
    let original = txn(0, 1, 100, Operation::Credit, 10);
    let debit = txn(0, 2, 101, Operation::Debit, 3);
    assert_eq!(
        commit_batch(&pipeline, vec![original.clone(), debit]).await,
        [
            RoutedReply::Commit(Reply::Transaction {
                status: TransactionStatus::Applied,
                balance: 10,
                seq: 1,
                replayed: false,
            }),
            RoutedReply::Commit(Reply::Transaction {
                status: TransactionStatus::Applied,
                balance: 7,
                seq: 2,
                replayed: false,
            }),
        ]
    );
    pipeline.source.drain_checkpoints().await.unwrap();
    assert_eq!(pipeline.source.metrics().checkpoint_latest_seq, 2);
    project_to(&pipeline, 2).await;
    assert!(
        pipeline
            .manager
            .advance_once_durable(200, &pipeline.source)
            .await
            .unwrap()
    );
    let first_gc = pipeline.source.collect_garbage(8).await.unwrap();
    assert_eq!(first_gc.deleted, 2);
    assert_eq!(first_gc.gc_prefix_seq, 2);
    assert_eq!(pipeline.source.gc_prefix_seq(), 2);
    let history = Arc::clone(&pipeline.history);
    pipeline.close().await;

    let recovered = AccountStore::open(&directory.path(), 1, BalanceMode::Checkpoint, 2)
        .await
        .expect("reopen checkpoint-mode source");
    let history_trait: Arc<dyn RefundHistory> = history.clone();
    recovered.set_refund_history(history_trait).unwrap();
    assert_eq!(recovered.latest_seq(), 2);
    assert_eq!(recovered.balance(0).unwrap(), 7);
    assert_eq!(recovered.gc_prefix_seq(), 2);
    assert_eq!(
        recovered.restored_projected_before().await.unwrap(),
        Some((200, 2))
    );
    assert_eq!(recovered.verify_projection_destination().await.unwrap(), 2);
    assert_eq!(
        history.progress(),
        2,
        "recovery keeps the same destination instance"
    );

    let mode_after_reopen = IndexLookupMode::Chunked {
        group_size: 2,
        max_in_flight: 8,
    };
    let pipeline = Pipeline::from_store(
        recovered,
        Arc::clone(&history),
        mode_after_reopen,
        200,
        2_048,
        Duration::from_millis(5),
    )
    .await;
    assert_eq!(
        route(&pipeline, original.clone()).await,
        RoutedReply::HistoricalHit(TransactionResult {
            status: TransactionStatus::Applied,
            balance: 10,
            seq: 1,
        })
    );
    assert_eq!(
        route(&pipeline, txn(0, 99, 150, Operation::Credit, 1)).await,
        RoutedReply::HistoricalMiss
    );

    let retained_credit = txn(0, 3, 210, Operation::Credit, 4);
    let retained_debit = txn(0, 4, 211, Operation::Debit, 1);
    assert_eq!(
        commit_batch(&pipeline, vec![retained_credit.clone(), retained_debit]).await,
        [
            RoutedReply::Commit(Reply::Transaction {
                status: TransactionStatus::Applied,
                balance: 11,
                seq: 3,
                replayed: false,
            }),
            RoutedReply::Commit(Reply::Transaction {
                status: TransactionStatus::Applied,
                balance: 10,
                seq: 4,
                replayed: false,
            }),
        ]
    );
    pipeline.source.drain_checkpoints().await.unwrap();
    assert_eq!(pipeline.source.metrics().checkpoint_latest_seq, 4);
    let reopened_handler_batches = pipeline.batch_metrics.lock().unwrap().clone();
    assert!(!reopened_handler_batches.is_empty());
    assert!(
        reopened_handler_batches
            .iter()
            .all(|sample| sample.mode == mode_after_reopen)
    );
    assert_eq!(
        route(&pipeline, retained_credit.clone()).await,
        RoutedReply::Commit(Reply::Transaction {
            status: TransactionStatus::Applied,
            balance: 11,
            seq: 3,
            replayed: true,
        })
    );
    let bounded_gc = pipeline.source.collect_garbage(8).await.unwrap();
    assert_eq!(bounded_gc.deleted, 0);
    assert_eq!(bounded_gc.gc_prefix_seq, 2);
    assert!(pipeline.source.has_ledger_sequence(3).await.unwrap());
    assert!(pipeline.source.has_ledger_sequence(4).await.unwrap());
    assert_eq!(
        pipeline.source.durable_projection_progress().await.unwrap(),
        2
    );
    assert_eq!(
        pipeline.source.restored_projected_before().await.unwrap(),
        Some((200, 2))
    );
    assert_eq!(pipeline.history.progress(), 2);
    pipeline.source.validate_integrity().await.unwrap();
    pipeline.close().await;

    let final_reopen = AccountStore::open(&directory.path(), 1, BalanceMode::Checkpoint, 2)
        .await
        .expect("reopen after the new indexed handler checkpoint");
    let history_trait: Arc<dyn RefundHistory> = history.clone();
    final_reopen.set_refund_history(history_trait).unwrap();
    assert_eq!(final_reopen.latest_seq(), 4);
    assert_eq!(final_reopen.balance(0).unwrap(), 10);
    assert_eq!(final_reopen.gc_prefix_seq(), 2);
    assert_eq!(
        final_reopen.verify_projection_destination().await.unwrap(),
        2
    );
    assert_eq!(history.progress(), 2);
    final_reopen.validate_integrity().await.unwrap();
    final_reopen.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pipeline_parallel_index_lookup_failure_errors_queued_requests_without_false_progress() {
    let directory = TempDb::new();
    let mut pipeline = Pipeline::open(
        &directory.path(),
        IndexLookupMode::Chunked {
            group_size: 2,
            max_in_flight: 4,
        },
        BalanceMode::PerBatch,
        100,
        0,
        1,
        Duration::from_millis(5),
    )
    .await;
    let (started, release) = pipeline.source.block_index_lookup_group_for_test(0);
    let release = ReleaseOnDrop(release);
    let first = enqueue_current(&pipeline, txn(0, 1, 10, Operation::Credit, 5)).await;
    tokio::task::spawn_blocking(move || {
        started
            .recv_timeout(Duration::from_secs(5))
            .expect("first lookup group starts")
    })
    .await
    .expect("wait for first query worker");
    drop(first); // Its caller is gone, while the accepted queue batch remains active.

    let second = enqueue_current(&pipeline, txn(0, 2, 11, Operation::Credit, 7)).await;
    let third = enqueue_current(&pipeline, txn(0, 3, 12, Operation::Credit, 9)).await;
    pipeline.source.fail_index_lookup_group_for_test(0);
    release.release();

    let second_error = match second.wait().await {
        Err(error) => error,
        Ok(_) => panic!("failed lookup unexpectedly replied to the second request"),
    };
    let third_error = match third.wait().await {
        Err(error) => error,
        Ok(_) => panic!("failed lookup unexpectedly replied to the third request"),
    };
    assert!(second_error.contains("injected MultiGet read failure"));
    assert!(third_error.contains("injected MultiGet read failure"));
    let worker_error = pipeline
        .worker
        .take()
        .expect("commit worker is present")
        .join()
        .await
        .expect_err("lookup failure stops the queue worker");
    assert!(worker_error.contains("injected MultiGet read failure"));

    assert_eq!(pipeline.source.latest_seq(), 1);
    assert_eq!(pipeline.source.balance(0).unwrap(), 5);
    assert_eq!(*pipeline.head_rx.borrow(), 1);
    assert_eq!(pipeline.gate.watermark().unwrap(), 0);
    assert_eq!(pipeline.source.gc_prefix_seq(), 0);
    assert_eq!(
        pipeline.source.durable_projection_progress().await.unwrap(),
        0
    );
    assert_eq!(
        pipeline.source.persisted_projected_before().await.unwrap(),
        None
    );
    assert!(pipeline.source.has_ledger_sequence(1).await.unwrap());
    assert!(!pipeline.source.has_ledger_sequence(2).await.unwrap());
    assert!(!pipeline.source.has_ledger_sequence(3).await.unwrap());
    pipeline.source.validate_integrity().await.unwrap();

    pipeline.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pipeline_parallel_index_prewrite_failure_does_not_publish_sequence_or_boundary() {
    let directory = TempDb::new();
    let mut pipeline = Pipeline::open(
        &directory.path(),
        IndexLookupMode::Chunked {
            group_size: 2,
            max_in_flight: 4,
        },
        BalanceMode::PerBatch,
        100,
        0,
        1,
        Duration::from_millis(5),
    )
    .await;
    pipeline.source.fail_next_index_lookup_write_for_test();
    let request = enqueue_current(&pipeline, txn(0, 1, 10, Operation::Credit, 5)).await;
    let request_error = match request.wait().await {
        Err(error) => error,
        Ok(_) => panic!("pre-write fault unexpectedly produced a reply"),
    };
    assert!(request_error.contains("injected synchronous account batch write failure"));
    let worker_error = pipeline
        .worker
        .take()
        .expect("commit worker is present")
        .join()
        .await
        .expect_err("pre-write failure stops the queue worker");
    assert!(worker_error.contains("injected synchronous account batch write failure"));

    assert_eq!(pipeline.source.latest_seq(), 0);
    assert_eq!(pipeline.source.balance(0).unwrap(), 0);
    assert_eq!(*pipeline.head_rx.borrow(), 0);
    assert_eq!(pipeline.gate.watermark().unwrap(), 0);
    assert_eq!(pipeline.source.gc_prefix_seq(), 0);
    assert_eq!(
        pipeline.source.durable_projection_progress().await.unwrap(),
        0
    );
    assert_eq!(
        pipeline.source.persisted_projected_before().await.unwrap(),
        None
    );
    assert!(!pipeline.source.has_ledger_sequence(1).await.unwrap());
    pipeline.source.validate_integrity().await.unwrap();
    pipeline.close().await;
}
