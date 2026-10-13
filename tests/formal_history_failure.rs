use ledger_wallet::history::{
    HistoricalLookup, HistoryBinding, HistoryDurability, HistoryError, HistoryFuture, HistoryStore,
};
use ledger_wallet::{
    DatabaseLayout, Lifecycle, Operation, Pipeline, PipelineConfig, Transaction, TransactionKey,
    TransactionStatus,
};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy)]
enum FailureMode {
    TemporaryUnavailable,
    PermanentConflict,
}

impl FailureMode {
    fn error(self) -> HistoryError {
        match self {
            Self::TemporaryUnavailable => {
                HistoryError::unavailable("test history destination is temporarily unavailable")
            }
            Self::PermanentConflict => {
                HistoryError::conflict("test history destination has a permanent conflict")
            }
        }
    }

    fn expected_attempts(self) -> usize {
        match self {
            Self::TemporaryUnavailable => 5,
            Self::PermanentConflict => 1,
        }
    }
}

/// Process-local fixture for exercising durable-source error handling only.
/// It deliberately declares the destination contract as Durable while retaining
/// only a stable binding and progress value in memory; it is not a DB connector
/// and makes no process-crash durability claim.
struct FailingDurableHistory {
    destination_identity: String,
    mode: FailureMode,
    binding: Mutex<Option<HistoryBinding>>,
    apply_attempts: AtomicUsize,
}

impl FailingDurableHistory {
    fn new(mode: FailureMode) -> Self {
        let destination_identity = match mode {
            FailureMode::TemporaryUnavailable => "failure-fixture-unavailable",
            FailureMode::PermanentConflict => "failure-fixture-conflict",
        };
        Self {
            destination_identity: destination_identity.to_owned(),
            mode,
            binding: Mutex::new(None),
            apply_attempts: AtomicUsize::new(0),
        }
    }

    fn apply_attempts(&self) -> usize {
        self.apply_attempts.load(Ordering::SeqCst)
    }
}

impl HistoryStore for FailingDurableHistory {
    fn durability(&self) -> HistoryDurability {
        HistoryDurability::Durable
    }

    fn destination_identity(&self) -> String {
        self.destination_identity.clone()
    }

    fn verify_binding<'a>(&'a self, binding: &'a HistoryBinding) -> HistoryFuture<'a, ()> {
        Box::pin(async move {
            let mut existing = self
                .binding
                .lock()
                .map_err(|_| HistoryError::corruption("test fixture binding lock poisoned"))?;
            match existing.as_ref() {
                Some(existing) if existing != binding => Err(HistoryError::identity_mismatch(
                    "test fixture is already bound to another source identity",
                )),
                Some(_) => Ok(()),
                None => {
                    *existing = Some(binding.clone());
                    Ok(())
                }
            }
        })
    }

    fn apply_batch<'a>(
        &'a self,
        _records: &'a [ledger_wallet::LedgerRecord],
    ) -> HistoryFuture<'a, u64> {
        Box::pin(async move {
            self.apply_attempts.fetch_add(1, Ordering::SeqCst);
            Err(self.mode.error())
        })
    }

    fn lookup_transaction<'a>(
        &'a self,
        _request: &'a Transaction,
    ) -> HistoryFuture<'a, HistoricalLookup> {
        Box::pin(async { Ok(HistoricalLookup::NotFound) })
    }

    fn progress(&self) -> HistoryFuture<'_, u64> {
        Box::pin(async { Ok(0) })
    }

    fn lookup_debit<'a>(
        &'a self,
        _key: TransactionKey,
    ) -> HistoryFuture<'a, Option<ledger_wallet::HistoricalDebit>> {
        Box::pin(async { Ok(None) })
    }
}

struct OwnedSourceDir(PathBuf);

impl OwnedSourceDir {
    fn new() -> std::io::Result<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let target = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target");
        std::fs::create_dir_all(&target)?;
        let path = target.join(format!(
            "ledger-wallet-formal-history-failure-{}-{nonce}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path)?;
        Ok(Self(path))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for OwnedSourceDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn config(data_dir: impl Into<PathBuf>) -> PipelineConfig {
    let mut config = PipelineConfig::new(data_dir);
    config.database_layout = DatabaseLayout::Shared;
    config.global_queue_capacity = 16;
    config.global_in_flight_limit = 16;
    config.batch_size = 8;
    config.batch_flush_timeout = Duration::from_millis(1);
    config.admission_timeout = Duration::from_secs(5);
    config.response_timeout = Duration::from_secs(5);
    config.history_io_concurrency = 2;
    config.history_operation_timeout = Duration::from_millis(50);
    config.global_storage_batches = 1;
    config.projector_batch_size = 8;
    config.gc_batch_size = 8;
    config.retention = Duration::from_millis(100);
    config.watermark_interval = Duration::from_millis(10);
    config.blocked_gc_sleep = Duration::from_millis(10);
    config.shutdown_timeout = Duration::from_secs(10);
    config.rocksdb_write_buffer_budget_bytes = 8 * 1024 * 1024;
    config.rocksdb_block_cache_budget_bytes = 8 * 1024 * 1024;
    config.rocksdb_background_jobs_budget = 1;
    config.startup_min_free_disk_bytes = 0;
    config.startup_min_available_memory_bytes = 0;
    config
}

fn unix_time_micros() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after Unix epoch")
            .as_micros(),
    )
    .expect("Unix microseconds fit in u64")
}

async fn wait_for_degraded_projector(
    pipeline: &Pipeline,
    timeout: Duration,
) -> Result<ledger_wallet::PipelineStatus, String> {
    let deadline = Instant::now() + timeout;
    loop {
        let status = pipeline
            .status()
            .await
            .map_err(|error| format!("could not read pipeline status: {error}"))?;
        let projector_failed = status
            .shards
            .iter()
            .flat_map(|shard| &shard.health)
            .any(|health| health.contains("projector"));
        if status.lifecycle == Lifecycle::Degraded && projector_failed {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "pipeline did not report degraded projector within {timeout:?}; last status: {status:?}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn exercise_failure_mode(
    mode: FailureMode,
    owned_dir: &OwnedSourceDir,
) -> Result<(), String> {
    let pipeline_config = config(owned_dir.path());
    let history = Arc::new(FailingDurableHistory::new(mode));
    let histories = vec![Arc::clone(&history) as Arc<dyn HistoryStore>];
    let pipeline = Pipeline::open(pipeline_config, histories)
        .await
        .map_err(|error| format!("could not open source with test fixture: {error}"))?;

    let transaction_at = unix_time_micros();
    let request = Transaction {
        key: TransactionKey {
            account_id: 7,
            tx_id: 1,
            transaction_at,
        },
        operation: Operation::Credit,
        amount: 10,
        refund_of: None,
    };
    let receipt = pipeline.submit(request.clone()).await;
    let degraded_status = wait_for_degraded_projector(&pipeline, Duration::from_secs(10)).await;
    let source_progress = history.progress().await;
    let history_lookup = history.lookup_transaction(&request).await;
    let history_debit = history.lookup_debit(request.key).await;
    let apply_attempts = history.apply_attempts();

    let shutdown = pipeline.shutdown().await;
    let waited_closed = pipeline.wait_closed().await;
    let closed_status = pipeline.status().await;
    drop(pipeline);

    let mut failures = Vec::new();
    match receipt {
        Ok(receipt) => {
            if receipt.sequence != 1
                || receipt.status != TransactionStatus::Applied
                || receipt.balance != 10
                || receipt.replayed
            {
                failures.push(format!("unexpected first commit receipt: {receipt:?}"));
            }
        }
        Err(error) => failures.push(format!("first credit was not committed: {error}")),
    }
    match degraded_status {
        Ok(status) => {
            if status.lifecycle != Lifecycle::Degraded {
                failures.push(format!(
                    "expected degraded lifecycle, got {:?}",
                    status.lifecycle
                ));
            }
            if let Some(shard) = status.shards.first() {
                if shard.source_sequence != 1 {
                    failures.push(format!(
                        "source sequence is {}, expected 1",
                        shard.source_sequence
                    ));
                }
                if shard.projected_sequence != 0 {
                    failures.push(format!(
                        "projected sequence advanced to {}",
                        shard.projected_sequence
                    ));
                }
                if shard.history_sequence != 0 {
                    failures.push(format!(
                        "reported history sequence advanced to {}",
                        shard.history_sequence
                    ));
                }
                if shard.gc_prefix_sequence != 0 {
                    failures.push(format!(
                        "GC prefix advanced to {}",
                        shard.gc_prefix_sequence
                    ));
                }
                if shard.watermark >= transaction_at {
                    failures.push(format!(
                        "watermark {} reached/passed the accepted transaction time {transaction_at}",
                        shard.watermark
                    ));
                }
                if !shard
                    .health
                    .iter()
                    .any(|health| health.contains("projector"))
                {
                    failures.push(format!(
                        "projector health error is absent: {:?}",
                        shard.health
                    ));
                }
            } else {
                failures.push("pipeline status has no shard".to_owned());
            }
        }
        Err(error) => failures.push(error),
    }
    match source_progress {
        Ok(progress) if progress == 0 => {}
        Ok(progress) => failures.push(format!("history progress is {progress}, expected 0")),
        Err(error) => failures.push(format!("could not read history progress: {error}")),
    }
    match history_lookup {
        Ok(HistoricalLookup::NotFound) => {}
        Ok(other) => failures.push(format!("failed apply became visible as {other:?}")),
        Err(error) => failures.push(format!("history lookup failed: {error}")),
    }
    match history_debit {
        Ok(None) => {}
        Ok(Some(debit)) => failures.push(format!("unexpected projected debit state: {debit:?}")),
        Err(error) => failures.push(format!("history debit lookup failed: {error}")),
    }
    if apply_attempts != mode.expected_attempts() {
        failures.push(format!(
            "history apply attempts were {apply_attempts}, expected {}",
            mode.expected_attempts()
        ));
    }
    match &shutdown {
        Err(error) if error.to_string().contains("projector") => {}
        Err(error) => failures.push(format!(
            "shutdown error omitted projector diagnostic: {error}"
        )),
        Ok(()) => failures
            .push("shutdown unexpectedly reported success after projector failure".to_owned()),
    }
    match &waited_closed {
        Err(error) if error.to_string().contains("projector") => {}
        Err(error) => failures.push(format!(
            "closed result omitted projector diagnostic: {error}"
        )),
        Ok(()) => failures
            .push("wait_closed unexpectedly reported success after projector failure".to_owned()),
    }
    match closed_status {
        Ok(status) if status.lifecycle == Lifecycle::Closed => {}
        Ok(status) => failures.push(format!(
            "lifecycle after shutdown is {:?}, not Closed",
            status.lifecycle
        )),
        Err(error) => failures.push(format!("could not observe closed lifecycle: {error}")),
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; "))
    }
}

fn run_failure_mode(mode: FailureMode) -> Result<(), String> {
    let owned_dir = OwnedSourceDir::new()
        .map_err(|error| format!("could not create test-owned source directory: {error}"))?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(16)
        .enable_all()
        .build()
        .map_err(|error| format!("could not build bounded test runtime: {error}"))?;
    runtime.block_on(exercise_failure_mode(mode, &owned_dir))
}

#[test]
fn temporary_history_unavailability_exhaustion_freezes_projection_and_gc() {
    run_failure_mode(FailureMode::TemporaryUnavailable)
        .unwrap_or_else(|error| panic!("temporary history failure contract failed: {error}"));
}

#[test]
fn permanent_history_conflict_stops_projection_without_fake_progress() {
    run_failure_mode(FailureMode::PermanentConflict)
        .unwrap_or_else(|error| panic!("permanent history failure contract failed: {error}"));
}
