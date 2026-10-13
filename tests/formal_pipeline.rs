use ledger_wallet::history::{
    HistoricalDebit, HistoricalLookup, HistoryBinding, HistoryDurability, HistoryError,
    HistoryFuture, HistoryStore, MockHistory,
};
use ledger_wallet::{
    DatabaseLayout, LedgerRecord, Operation, Pipeline, PipelineConfig, PipelineError, Transaction,
    TransactionKey, TransactionResult, TransactionStatus,
};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

struct OwnedTestDirectory(PathBuf);

impl OwnedTestDirectory {
    fn new(label: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target/formal-pipeline-tests")
            .join(format!(
                "{label}-{}-{nonce:x}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
        fs::create_dir_all(&path).expect("create test-owned directory");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for OwnedTestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn test_config(path: impl Into<PathBuf>, shards: u32, layout: DatabaseLayout) -> PipelineConfig {
    let mut config = PipelineConfig::new(path);
    config.shard_count = shards;
    config.database_layout = layout;
    config.global_queue_capacity = 64;
    config.global_in_flight_limit = 64;
    config.batch_size = 16;
    config.batch_flush_timeout = Duration::from_millis(2);
    config.admission_timeout = Duration::from_secs(3);
    config.response_timeout = Duration::from_secs(8);
    config.history_io_concurrency = 4;
    config.history_operation_timeout = Duration::from_secs(2);
    config.global_storage_batches = 4;
    config.projector_batch_size = 16;
    config.gc_batch_size = 16;
    config.shutdown_timeout = Duration::from_secs(10);
    config.startup_min_free_disk_bytes = 0;
    config.startup_min_available_memory_bytes = 0;
    config
}

fn volatile_histories(shards: u32) -> Vec<Arc<dyn HistoryStore>> {
    (0..shards)
        .map(|shard| {
            Arc::new(MockHistory::new(format!("formal-volatile-shard-{shard}")))
                as Arc<dyn HistoryStore>
        })
        .collect()
}

fn account_for_shard(base: u64, shard: u32) -> u64 {
    base + u64::from(shard)
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

async fn wait_for_projection(pipeline: &Pipeline, timeout: Duration) {
    tokio::time::timeout(timeout, async {
        loop {
            let status = pipeline.status().await.expect("pipeline status");
            if status
                .shards
                .iter()
                .all(|shard| shard.projected_sequence >= shard.source_sequence)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("projector catches up");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_and_dedicated_shards_replay_refusals_refunds_and_reopen_with_closed_handles() {
    for layout in [DatabaseLayout::Shared, DatabaseLayout::Dedicated] {
        for shards in [1, 2, 4] {
            let owned = OwnedTestDirectory::new(&format!("{}-s{shards}", layout.as_str()));
            let config = test_config(owned.path().join("data"), shards, layout);
            let pipeline = Pipeline::open(config.clone(), volatile_histories(shards))
                .await
                .expect("open formal pipeline");

            for shard in 0..shards {
                let account = account_for_shard(5_000, shard);
                let credit = pipeline
                    .credit(account, 1, 100, 10)
                    .await
                    .expect("credit succeeds");
                assert_eq!(credit.shard_id, shard);
                assert_eq!(credit.sequence, 1);
                assert_eq!(credit.status, TransactionStatus::Applied);
                assert!(!credit.replayed);

                let refused = pipeline
                    .debit(account, 2, 200, 11)
                    .await
                    .expect("business refusal is a durable receipt");
                assert_eq!(refused.sequence, 2);
                assert_eq!(refused.status, TransactionStatus::InsufficientFunds);
                assert_eq!(refused.balance, 10);
                let refusal_replay = pipeline
                    .debit(account, 2, 200, 11)
                    .await
                    .expect("same refused request replays its receipt");
                assert_eq!(refusal_replay.sequence, refused.sequence);
                assert_eq!(refusal_replay.status, refused.status);
                assert_eq!(refusal_replay.balance, refused.balance);
                assert!(refusal_replay.replayed);
                assert!(matches!(
                    pipeline.debit(account, 2, 200, 10).await,
                    Err(PipelineError::Conflict)
                ));

                let refund_account = account_for_shard(10_000, shard);
                let fund = pipeline
                    .credit(refund_account, 3, 300, 100)
                    .await
                    .expect("fund refund account");
                assert_eq!(fund.sequence, 3);
                let debit = pipeline
                    .debit(refund_account, 4, 400, 40)
                    .await
                    .expect("original debit");
                assert_eq!(debit.sequence, 4);
                let first_refund = pipeline
                    .refund(refund_account, 5, 500, 40, 4, 400)
                    .await
                    .expect("first refund succeeds");
                assert_eq!(first_refund.sequence, 5);
                assert_eq!(first_refund.balance, 100);
                let exact_refund = pipeline
                    .refund(refund_account, 5, 500, 40, 4, 400)
                    .await
                    .expect("exact refund key replays");
                assert_eq!(exact_refund.sequence, first_refund.sequence);
                assert_eq!(exact_refund.status, first_refund.status);
                assert!(exact_refund.replayed);
                let second_refund = pipeline
                    .refund(refund_account, 6, 600, 40, 4, 400)
                    .await
                    .expect("refund-once refusal is a receipt");
                assert_eq!(second_refund.sequence, 6);
                assert_eq!(second_refund.status, TransactionStatus::RefundAlreadyUsed);
                assert_eq!(second_refund.balance, 100);

                let duplicate_key = transaction(account, 7, 700, Operation::Credit, 5, None);
                let first = pipeline.submit(duplicate_key.clone());
                let second = pipeline.submit(duplicate_key);
                let (first, second) = tokio::join!(first, second);
                let first = first.expect("one concurrent idempotent request");
                let second = second.expect("second concurrent idempotent request");
                assert_eq!(first.sequence, 7);
                assert_eq!(second.sequence, 7);
                assert_eq!(first.balance, 15);
                assert_eq!(second.balance, 15);
                assert_ne!(first.replayed, second.replayed);

                let unknown = account_for_shard(20_000, shard);
                let zero = pipeline
                    .balance(unknown)
                    .await
                    .expect("implicit zero balance");
                assert_eq!(zero.shard_id, shard);
                assert_eq!(zero.account_id, unknown);
                assert_eq!(zero.balance, 0);
                assert_eq!(zero.sequence, 7);
                let current = pipeline.balance(account).await.expect("current balance");
                assert_eq!(current.balance, 15);
                let refund_balance = pipeline
                    .balance(refund_account)
                    .await
                    .expect("refund balance");
                assert_eq!(refund_balance.balance, 100);
            }

            wait_for_projection(&pipeline, Duration::from_secs(5)).await;
            let old_handle = pipeline.clone();
            pipeline.shutdown().await.expect("graceful shutdown");
            old_handle
                .shutdown()
                .await
                .expect("repeat shutdown returns its cached result");
            assert!(matches!(
                old_handle.credit(5_000, 99, 999, 1).await,
                Err(PipelineError::Closed)
            ));
            // This open succeeds while the old public handles remain live,
            // proving that closed facades no longer retain RocksDB handles.
            let reopened = Pipeline::open(config.clone(), volatile_histories(shards))
                .await
                .expect("reopen source while closed handle remains in scope");
            wait_for_projection(&reopened, Duration::from_secs(5)).await;
            for shard in 0..shards {
                let balance = reopened
                    .balance(account_for_shard(5_000, shard))
                    .await
                    .expect("balance after reopen");
                assert_eq!(balance.balance, 15);
                let refund_balance = reopened
                    .balance(account_for_shard(10_000, shard))
                    .await
                    .expect("refund balance after reopen");
                assert_eq!(refund_balance.balance, 100);
            }
            let reopened_status = reopened.status().await.expect("reopened status");
            assert!(
                reopened_status
                    .shards
                    .iter()
                    .all(|shard| { shard.watermark == 0 && shard.gc_prefix_sequence == 0 })
            );
            reopened.shutdown().await.expect("close reopened service");
            drop(old_handle);
            drop(pipeline);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn schema_routing_and_missing_manifest_fail_closed_without_deleting_source() {
    let owned = OwnedTestDirectory::new("manifest");
    let config = test_config(owned.path().join("data"), 1, DatabaseLayout::Shared);
    let pipeline = Pipeline::open(config.clone(), volatile_histories(1))
        .await
        .expect("initial service open");
    pipeline
        .credit(7, 1, 100, 10)
        .await
        .expect("durable source write");
    pipeline.shutdown().await.expect("close source");

    let mut wrong_shards = config.clone();
    wrong_shards.shard_count = 2;
    assert!(
        Pipeline::open(wrong_shards, volatile_histories(2))
            .await
            .is_err()
    );

    let metadata_path = config.data_dir.join("service.meta");
    let original = fs::read_to_string(&metadata_path).expect("read service manifest");
    for (old, new) in [
        ("schema_version=1", "schema_version=99"),
        ("routing_version=1", "routing_version=99"),
    ] {
        fs::write(&metadata_path, original.replace(old, new)).expect("mutate test manifest");
        assert!(
            Pipeline::open(config.clone(), volatile_histories(1))
                .await
                .is_err()
        );
        assert!(config.data_dir.join("database").exists());
    }
    fs::remove_file(&metadata_path).expect("remove owned test manifest");
    assert!(
        Pipeline::open(config.clone(), volatile_histories(1))
            .await
            .is_err()
    );
    assert!(config.data_dir.join("database").exists());
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DurableRow {
    request: Transaction,
    result: TransactionResult,
}

#[derive(Clone, Default)]
struct DurableState {
    binding: Option<HistoryBinding>,
    progress: u64,
    rows_by_key: HashMap<TransactionKey, DurableRow>,
    rows_by_sequence: BTreeMap<u64, DurableRow>,
    refunded_debits: HashSet<TransactionKey>,
}

struct TestDurableHistory {
    identity: String,
    state: Mutex<DurableState>,
    temporary_failures: AtomicUsize,
    always_fail: AtomicBool,
}

impl TestDurableHistory {
    fn new(identity: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            identity: identity.into(),
            state: Mutex::new(DurableState::default()),
            temporary_failures: AtomicUsize::new(0),
            always_fail: AtomicBool::new(false),
        })
    }

    fn clone_state_from(&self, source: &Self) {
        *self.state.lock().expect("durable state lock") = source
            .state
            .lock()
            .expect("source durable state lock")
            .clone();
    }
}

impl HistoryStore for TestDurableHistory {
    fn durability(&self) -> HistoryDurability {
        HistoryDurability::Durable
    }

    fn destination_identity(&self) -> String {
        self.identity.clone()
    }

    fn verify_binding<'a>(&'a self, binding: &'a HistoryBinding) -> HistoryFuture<'a, ()> {
        Box::pin(async move {
            let mut state = self
                .state
                .lock()
                .map_err(|_| HistoryError::corruption("durable fake state mutex poisoned"))?;
            match &state.binding {
                Some(existing) if existing != binding => Err(HistoryError::identity_mismatch(
                    "destination is already bound to a different source",
                )),
                Some(_) => Ok(()),
                None if state.progress > 0 => Err(HistoryError::identity_mismatch(
                    "durable rows exist without a source binding",
                )),
                None => {
                    state.binding = Some(binding.clone());
                    Ok(())
                }
            }
        })
    }

    fn apply_batch<'a>(&'a self, records: &'a [LedgerRecord]) -> HistoryFuture<'a, u64> {
        Box::pin(async move {
            if self.always_fail.load(Ordering::Acquire) {
                return Err(HistoryError::conflict("injected permanent apply conflict"));
            }
            if self
                .temporary_failures
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Err(HistoryError::unavailable(
                    "injected temporary apply failure",
                ));
            }
            let mut guard = self
                .state
                .lock()
                .map_err(|_| HistoryError::corruption("durable fake state mutex poisoned"))?;
            let mut staged = guard.clone();
            for record in records {
                let row = DurableRow {
                    request: record.request.clone(),
                    result: record.result,
                };
                let sequence = record.result.seq;
                if sequence <= staged.progress {
                    if staged.rows_by_sequence.get(&sequence) != Some(&row) {
                        return Err(HistoryError::conflict(format!(
                            "sequence {sequence} replay differs from durable history"
                        )));
                    }
                    continue;
                }
                let expected = staged
                    .progress
                    .checked_add(1)
                    .ok_or_else(|| HistoryError::corruption("history sequence overflow"))?;
                if sequence != expected {
                    return Err(HistoryError::corruption(format!(
                        "expected contiguous sequence {expected}, got {sequence}"
                    )));
                }
                if staged
                    .rows_by_key
                    .get(&record.request.key)
                    .is_some_and(|existing| existing != &row)
                {
                    return Err(HistoryError::conflict(
                        "transaction key has a different durable body",
                    ));
                }
                if record.request.operation == Operation::Refund
                    && record.result.status == TransactionStatus::Applied
                {
                    let debit = record.request.refund_of.ok_or_else(|| {
                        HistoryError::corruption("applied refund has no debit key")
                    })?;
                    let target = staged.rows_by_key.get(&debit).ok_or_else(|| {
                        HistoryError::corruption("applied refund has no debit row")
                    })?;
                    if target.request.operation != Operation::Debit
                        || !staged.refunded_debits.insert(debit)
                    {
                        return Err(HistoryError::corruption(
                            "refund target is invalid or was already refunded",
                        ));
                    }
                }
                staged.rows_by_key.insert(record.request.key, row.clone());
                staged.rows_by_sequence.insert(sequence, row);
                staged.progress = sequence;
            }
            let progress = staged.progress;
            *guard = staged;
            Ok(progress)
        })
    }

    fn lookup_transaction<'a>(
        &'a self,
        request: &'a Transaction,
    ) -> HistoryFuture<'a, HistoricalLookup> {
        Box::pin(async move {
            let state = self
                .state
                .lock()
                .map_err(|_| HistoryError::corruption("durable fake state mutex poisoned"))?;
            match state.rows_by_key.get(&request.key) {
                Some(row) if row.request == *request => {
                    Ok(HistoricalLookup::ExactReplay(row.result))
                }
                Some(_) => Ok(HistoricalLookup::Conflict),
                None => Ok(HistoricalLookup::NotFound),
            }
        })
    }

    fn progress(&self) -> HistoryFuture<'_, u64> {
        Box::pin(async move {
            self.state
                .lock()
                .map(|state| state.progress)
                .map_err(|_| HistoryError::corruption("durable fake state mutex poisoned"))
        })
    }

    fn lookup_debit<'a>(
        &'a self,
        key: TransactionKey,
    ) -> HistoryFuture<'a, Option<HistoricalDebit>> {
        Box::pin(async move {
            let state = self
                .state
                .lock()
                .map_err(|_| HistoryError::corruption("durable fake state mutex poisoned"))?;
            Ok(state.rows_by_key.get(&key).and_then(|row| {
                (row.request.operation == Operation::Debit).then_some(HistoricalDebit {
                    amount: row.request.amount,
                    already_refunded: state.refunded_debits.contains(&key),
                })
            }))
        })
    }
}

fn durable_histories(history: Arc<TestDurableHistory>) -> Vec<Arc<dyn HistoryStore>> {
    vec![history]
}

fn unix_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

async fn wait_for_shard(
    pipeline: &Pipeline,
    timeout: Duration,
    predicate: impl Fn(&ledger_wallet::ShardStatus) -> bool,
) -> ledger_wallet::ShardStatus {
    tokio::time::timeout(timeout, async {
        loop {
            let status = pipeline.status().await.expect("pipeline status");
            let shard = status.shards.first().expect("single shard status");
            if predicate(shard) {
                return shard.clone();
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("shard status predicate")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_history_watermark_gc_restart_preserve_exact_replay_and_refund_once() {
    let owned = OwnedTestDirectory::new("durable-history");
    let mut config = test_config(owned.path().join("data"), 1, DatabaseLayout::Shared);
    config.retention = Duration::from_millis(20);
    config.watermark_interval = Duration::from_millis(5);
    config.blocked_gc_sleep = Duration::from_millis(5);
    let history = TestDurableHistory::new("formal-destination-stable");
    let pipeline = Pipeline::open(config.clone(), durable_histories(Arc::clone(&history)))
        .await
        .expect("open durable history pipeline");

    let account = 7;
    let base_time = unix_micros().saturating_add(1_000);
    let credit = transaction(account, 1, base_time, Operation::Credit, 100, None);
    let debit_key = TransactionKey {
        account_id: account,
        tx_id: 2,
        transaction_at: base_time + 1,
    };
    let debit = transaction(account, 2, base_time + 1, Operation::Debit, 40, None);
    let refund = transaction(
        account,
        3,
        base_time + 2,
        Operation::Refund,
        40,
        Some(debit_key),
    );
    assert_eq!(pipeline.submit(credit.clone()).await.unwrap().sequence, 1);
    assert_eq!(pipeline.submit(debit).await.unwrap().sequence, 2);
    assert_eq!(pipeline.submit(refund.clone()).await.unwrap().sequence, 3);

    let gc_status = wait_for_shard(&pipeline, Duration::from_secs(5), |shard| {
        shard.projected_sequence >= 3 && shard.gc_prefix_sequence >= 3
    })
    .await;
    assert!(gc_status.watermark > base_time + 2);
    assert_eq!(history.progress().await.unwrap(), 3);

    let second_refund_at = unix_micros().saturating_add(30_000_000);
    let refused_refund = pipeline
        .refund(account, 4, second_refund_at, 40, 2, base_time + 1)
        .await
        .expect("historical debit state prevents second refund after GC");
    assert_eq!(refused_refund.sequence, 4);
    assert_eq!(refused_refund.status, TransactionStatus::RefundAlreadyUsed);
    assert_eq!(refused_refund.balance, 100);
    wait_for_shard(&pipeline, Duration::from_secs(5), |shard| {
        shard.projected_sequence >= 4
    })
    .await;
    pipeline.shutdown().await.expect("close durable source");

    let wrong_destination = TestDurableHistory::new("formal-destination-wrong");
    wrong_destination.clone_state_from(&history);
    assert!(
        Pipeline::open(config.clone(), durable_histories(wrong_destination))
            .await
            .is_err()
    );
    assert!(
        Pipeline::open(config.clone(), volatile_histories(1))
            .await
            .is_err()
    );

    let reopened = Pipeline::open(config.clone(), durable_histories(Arc::clone(&history)))
        .await
        .expect("restore matching durable history destination");
    let restored = reopened.status().await.expect("restored status");
    assert!(restored.shards[0].gc_prefix_sequence >= 3);
    assert!(restored.shards[0].watermark >= gc_status.watermark);

    let replay = reopened
        .submit(credit.clone())
        .await
        .expect("historical exact replay survives GC");
    assert_eq!(replay.sequence, 1);
    assert_eq!(replay.status, TransactionStatus::Applied);
    assert_eq!(replay.balance, 100);
    assert!(replay.historical_replay);
    assert!(replay.replayed);
    assert!(matches!(
        reopened
            .submit(transaction(
                account,
                99,
                base_time + 3,
                Operation::Credit,
                1,
                None
            ))
            .await,
        Err(PipelineError::ExpiredTime { .. })
    ));
    let second_refund_replay = reopened
        .refund(account, 4, second_refund_at, 40, 2, base_time + 1)
        .await
        .expect("source idempotency index survives restart");
    assert_eq!(second_refund_replay.sequence, 4);
    assert_eq!(
        second_refund_replay.status,
        TransactionStatus::RefundAlreadyUsed
    );
    assert!(second_refund_replay.replayed);
    reopened
        .shutdown()
        .await
        .expect("close reopened durable source");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_request_cancellation_and_shutdown_drain_keep_committed_source_reopenable() {
    let owned = OwnedTestDirectory::new("cancel-shutdown");
    let mut config = test_config(owned.path().join("data"), 1, DatabaseLayout::Shared);
    config.global_queue_capacity = 8;
    config.global_in_flight_limit = 16;
    config.batch_size = 8;
    config.batch_flush_timeout = Duration::from_millis(500);
    config.response_timeout = Duration::from_secs(10);
    let pipeline = Pipeline::open(config.clone(), volatile_histories(1))
        .await
        .expect("open cancellation pipeline");

    let cancelled_tx = transaction(3, 1, 1_001, Operation::Credit, 10, None);
    let cancel_service = pipeline.clone();
    let cancelled = tokio::spawn(async move { cancel_service.submit(cancelled_tx).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let enqueue = pipeline
                .metrics()
                .stages
                .into_iter()
                .find(|stage| stage.stage == "enqueue")
                .map(|stage| stage.count)
                .unwrap_or_default();
            if enqueue > 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("request reaches accepted queue state");
    cancelled.abort();
    let _ = cancelled.await;

    let drain_service = pipeline.clone();
    let drained = tokio::spawn(async move { drain_service.credit(4, 1, 1_002, 20).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let enqueue_count = pipeline
                .metrics()
                .stages
                .into_iter()
                .find(|stage| stage.stage == "enqueue")
                .map(|stage| stage.count)
                .unwrap_or_default();
            if enqueue_count >= 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("second request reaches accepted queue state before shutdown");
    pipeline
        .shutdown()
        .await
        .expect("shutdown drains accepted work");
    let drain_receipt = drained
        .await
        .expect("transaction caller joins")
        .expect("accepted transaction receives result");
    assert_eq!(drain_receipt.sequence, 2);
    assert_eq!(drain_receipt.balance, 20);
    assert!(
        pipeline
            .metrics()
            .stages
            .into_iter()
            .find(|stage| stage.stage == "batch_wait")
            .is_some_and(|stage| stage.count >= 1)
    );

    let reopened = Pipeline::open(config.clone(), volatile_histories(1))
        .await
        .expect("reopen after accepted caller cancellation and shutdown");
    let balance = reopened
        .balance(3)
        .await
        .expect("cancelled caller result committed");
    assert_eq!(balance.balance, 10);
    let other = reopened
        .balance(4)
        .await
        .expect("drained shutdown result committed");
    assert_eq!(other.balance, 20);
    assert_eq!(other.sequence, 2);
    reopened.shutdown().await.expect("close reopened source");
}
