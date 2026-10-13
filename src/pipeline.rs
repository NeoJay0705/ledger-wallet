//! Public asynchronous ledger pipeline.

use crate::admission::{AdmissionGate, CommitGuard, GateDecision, ProjectionProgress};
use crate::batch_queue::{self, BatchQueue, BatchWorker, Completed};
use crate::config::{DatabaseLayout, PipelineConfig};
use crate::error::PipelineError;
use crate::history::{
    HistoricalDebit, HistoricalLookup, HistoryBinding, HistoryDurability, HistoryError,
    HistoryErrorKind, HistoryStore,
};
use crate::metrics::{Metrics, MetricsSnapshot, Stage};
use crate::storage::account_store::{
    AccountStore, HistoricalDebit as StorageHistoricalDebit, IndexLookupBatchMetrics,
    IndexLookupConfig, IndexLookupMode, RefundHistory, RocksDbBudget,
};
use crate::{Operation, Reply, Transaction, TransactionKey, TransactionResult, TransactionStatus};
use rocksdb::{DB, Options};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::runtime::Handle;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, oneshot, watch};
use tokio::task::{JoinHandle, JoinSet};

const SERVICE_SCHEMA_VERSION: u32 = 1;
const ROUTING_VERSION: u32 = 1;
const ROUTING_NAME: &str = "account_id_mod_shard_count";
const BALANCE_MODE_NAME: &str = "per_batch";
const LIFE_RUNNING: u8 = 0;
const LIFE_DEGRADED: u8 = 1;
const LIFE_CLOSING: u8 = 2;
const LIFE_CLOSED: u8 = 3;
const PROJECTOR_READY: u8 = 0;
const PROJECTOR_TEMPORARY: u8 = 1;
const PROJECTOR_FAILED: u8 = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Lifecycle {
    Running,
    Degraded,
    Closing,
    Closed,
}

impl Lifecycle {
    fn from_byte(value: u8) -> Self {
        match value {
            LIFE_DEGRADED => Self::Degraded,
            LIFE_CLOSING => Self::Closing,
            LIFE_CLOSED => Self::Closed,
            _ => Self::Running,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Degraded => "degraded",
            Self::Closing => "closing",
            Self::Closed => "closed",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionReceipt {
    pub shard_id: u32,
    pub sequence: u64,
    pub status: TransactionStatus,
    pub balance: u64,
    /// True when this idempotency key was already present in the source store.
    pub replayed: bool,
    /// True when the original result came from the retained history projection.
    pub historical_replay: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BalanceReceipt {
    pub shard_id: u32,
    pub account_id: u64,
    pub balance: u64,
    pub sequence: u64,
}

#[derive(Clone, Debug)]
pub struct ShardStatus {
    pub shard_id: u32,
    pub source_identity: String,
    pub source_sequence: u64,
    /// In durable mode, this is source-side synced projector progress. In
    /// volatile mode, it is process-local replay progress and is never a GC
    /// safety mark.
    pub projected_sequence: u64,
    pub history_sequence: u64,
    pub watermark: u64,
    pub gc_prefix_sequence: u64,
    pub queued_requests: usize,
    pub queue_capacity: usize,
    pub history_durability: HistoryDurability,
    pub history_identity: String,
    pub health: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct PipelineStatus {
    pub lifecycle: Lifecycle,
    pub service_identity: String,
    pub data_dir: PathBuf,
    pub shard_count: u32,
    pub database_layout: DatabaseLayout,
    pub in_flight: usize,
    pub in_flight_limit: usize,
    pub global_queue_capacity: usize,
    pub shards: Vec<ShardStatus>,
    pub metrics: MetricsSnapshot,
}

struct AcceptedTransaction {
    transaction: Transaction,
    guard: CommitGuard,
    in_flight: OwnedSemaphorePermit,
}

struct GuardedReply {
    reply: Reply,
    guard: Option<CommitGuard>,
    in_flight: Option<OwnedSemaphorePermit>,
}

impl GuardedReply {
    fn into_receipt(mut self, shard_id: u32) -> Result<TransactionReceipt, PipelineError> {
        self.guard.take();
        self.in_flight.take();
        match self.reply {
            Reply::Conflict => Err(PipelineError::Conflict),
            Reply::Transaction {
                status,
                balance,
                seq,
                replayed,
            } => Ok(TransactionReceipt {
                shard_id,
                sequence: seq,
                status,
                balance,
                replayed,
                historical_replay: false,
            }),
        }
    }
}

struct ShardContext {
    shard_id: u32,
    source_identity: String,
    store: Mutex<Option<AccountStore>>,
    queue: Mutex<Option<BatchQueue<AcceptedTransaction, GuardedReply>>>,
    gate: Arc<AdmissionGate>,
    progress: Arc<ProjectionProgress>,
    history: Arc<dyn HistoryStore>,
    history_durability: HistoryDurability,
    history_identity: String,
    history_io: Arc<Semaphore>,
    history_operation_timeout: Duration,
    storage_batches: Arc<Semaphore>,
    committed_head: watch::Sender<u64>,
    committed_head_receiver: watch::Receiver<u64>,
    background_stop: watch::Sender<bool>,
    projector_state: AtomicU8,
    history_sequence: AtomicU64,
    gc_prefix_sequence: AtomicU64,
    health: Mutex<BTreeMap<&'static str, String>>,
}

impl ShardContext {
    fn store(&self) -> Result<AccountStore, PipelineError> {
        self.store
            .lock()
            .map_err(|_| PipelineError::infrastructure("source state", "store lock poisoned"))?
            .as_ref()
            .cloned()
            .ok_or(PipelineError::Closed)
    }

    fn set_health(&self, component: &'static str, message: impl Into<String>) {
        if let Ok(mut health) = self.health.lock() {
            health.insert(component, message.into());
        }
    }

    fn clear_health(&self, component: &'static str) {
        if let Ok(mut health) = self.health.lock() {
            health.remove(component);
        }
    }

    fn health(&self) -> Vec<String> {
        self.health
            .lock()
            .map(|health| {
                health
                    .iter()
                    .map(|(component, message)| format!("{component}: {message}"))
                    .collect()
            })
            .unwrap_or_else(|_| vec!["health: health mutex poisoned".to_owned()])
    }
}

struct AdmissionState {
    closing: bool,
    active_attempts: usize,
}

struct AdmissionTracker {
    state: Mutex<AdmissionState>,
    changed: Notify,
}

impl AdmissionTracker {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(AdmissionState {
                closing: false,
                active_attempts: 0,
            }),
            changed: Notify::new(),
        })
    }

    fn begin(self: &Arc<Self>) -> Result<AdmissionAttempt, PipelineError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| PipelineError::infrastructure("admission", "state lock poisoned"))?;
        if state.closing {
            return Err(PipelineError::Closed);
        }
        state.active_attempts = state
            .active_attempts
            .checked_add(1)
            .ok_or_else(|| PipelineError::infrastructure("admission", "attempt count overflow"))?;
        Ok(AdmissionAttempt {
            tracker: Arc::clone(self),
            active: true,
        })
    }

    fn close(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.closing = true;
        }
        self.changed.notify_waiters();
    }

    async fn wait_idle(&self) {
        loop {
            let notified = self.changed.notified();
            let idle = self
                .state
                .lock()
                .map(|state| state.active_attempts == 0)
                .unwrap_or(true);
            if idle {
                return;
            }
            notified.await;
        }
    }
}

struct AdmissionAttempt {
    tracker: Arc<AdmissionTracker>,
    active: bool,
}

impl Drop for AdmissionAttempt {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        if let Ok(mut state) = self.tracker.state.lock() {
            state.active_attempts = state.active_attempts.saturating_sub(1);
        }
        self.tracker.changed.notify_waiters();
    }
}

struct Shared {
    config: PipelineConfig,
    service_identity: String,
    shards: Vec<Arc<ShardContext>>,
    in_flight: Arc<Semaphore>,
    admission: Arc<AdmissionTracker>,
    lifecycle: Arc<AtomicU8>,
    shutdown: watch::Sender<bool>,
    shutdown_done: watch::Receiver<Option<Result<(), String>>>,
    metrics: Arc<Metrics>,
    public_handles: AtomicUsize,
}

/// Cloneable public service handle. Dropping the last handle requests graceful
/// shutdown; the owned coordinator keeps all accepted work and source resources
/// alive until it has joined every worker.
pub struct Pipeline {
    shared: Arc<Shared>,
}

impl Clone for Pipeline {
    fn clone(&self) -> Self {
        self.shared.public_handles.fetch_add(1, Ordering::Relaxed);
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl Drop for Pipeline {
    fn drop(&mut self) {
        if self.shared.public_handles.fetch_sub(1, Ordering::AcqRel) == 1 {
            request_shutdown(
                &self.shared.admission,
                &self.shared.lifecycle,
                &self.shared.shutdown,
            );
        }
    }
}

impl Pipeline {
    /// Open/recover source shards and start their owned queue, projection,
    /// watermark, GC, and shutdown coordinators.
    pub async fn open(
        config: PipelineConfig,
        histories: Vec<Arc<dyn HistoryStore>>,
    ) -> Result<Self, PipelineError> {
        if Handle::try_current().is_err() {
            return Err(PipelineError::InvalidConfiguration(
                "Pipeline::open must run inside a Tokio runtime".to_owned(),
            ));
        }
        // Startup is owned independently from this caller future. If a caller
        // cancels while RocksDB or a history backend is opening, the task
        // keeps the source handles until it can finish recovery or clean up.
        let (opened_sender, opened_receiver) = oneshot::channel();
        tokio::spawn(async move {
            let result = Self::open_owned(config, histories).await;
            let _ = opened_sender.send(result);
        });
        opened_receiver.await.map_err(|_| {
            PipelineError::infrastructure("startup", "owned startup task stopped unexpectedly")
        })?
    }

    async fn open_owned(
        config: PipelineConfig,
        histories: Vec<Arc<dyn HistoryStore>>,
    ) -> Result<Self, PipelineError> {
        config
            .validate()
            .map_err(PipelineError::InvalidConfiguration)?;
        if histories.len() != config.shard_count as usize {
            return Err(PipelineError::InvalidConfiguration(format!(
                "one history store is required per shard: expected {}, got {}",
                config.shard_count,
                histories.len()
            )));
        }
        let resource_config = config.clone();
        tokio::task::spawn_blocking(move || check_startup_resources(&resource_config))
            .await
            .map_err(|error| {
                PipelineError::infrastructure("startup resource check", error.to_string())
            })?
            .map_err(|error| PipelineError::InvalidConfiguration(error))?;
        let metadata_config = config.clone();
        let data_dir = config.data_dir.clone();
        let metadata = tokio::task::spawn_blocking(move || {
            load_or_create_service_metadata(&data_dir, &metadata_config)
        })
        .await
        .map_err(|error| PipelineError::infrastructure("service metadata", error.to_string()))?
        .map_err(|error| PipelineError::infrastructure("service metadata", error))?;

        let stores = open_source_stores(&config).await?;
        let history_io = Arc::new(Semaphore::new(config.history_io_concurrency));
        let storage_batches = Arc::new(Semaphore::new(config.global_storage_batches));
        let recovered = match recover_shards(
            stores,
            histories,
            &config,
            &metadata,
            Arc::clone(&history_io),
        )
        .await
        {
            Ok(recovered) => recovered,
            Err(error) => return Err(error),
        };

        let metrics = Arc::new(Metrics::new());
        let in_flight = Arc::new(Semaphore::new(config.global_in_flight_limit));
        let admission = AdmissionTracker::new();
        let lifecycle = Arc::new(AtomicU8::new(LIFE_RUNNING));
        let mut contexts = Vec::with_capacity(recovered.len());
        let index_lookup = IndexLookupConfig::new(IndexLookupMode::Chunked {
            group_size: config.index_lookup_group_size,
            max_in_flight: config.index_lookup_max_in_flight,
        })
        .map_err(PipelineError::InvalidConfiguration)?;
        for recovered_shard in recovered {
            let RecoveredShard {
                shard_id,
                source_identity,
                store,
                history,
                history_identity,
                durability,
                latest,
                durable_source_progress,
                history_sequence,
                watermark,
                gc_prefix,
            } = recovered_shard;
            let (committed_head, committed_head_receiver) = watch::channel(latest);
            let (background_stop, _) = watch::channel(false);
            contexts.push(Arc::new(ShardContext {
                shard_id,
                source_identity,
                store: Mutex::new(Some(store)),
                queue: Mutex::new(None),
                gate: AdmissionGate::new(watermark),
                progress: ProjectionProgress::new(durable_source_progress),
                history,
                history_durability: durability,
                history_identity,
                history_io: Arc::clone(&history_io),
                history_operation_timeout: config.history_operation_timeout,
                storage_batches: Arc::clone(&storage_batches),
                committed_head: committed_head.clone(),
                committed_head_receiver,
                background_stop,
                projector_state: AtomicU8::new(PROJECTOR_READY),
                history_sequence: AtomicU64::new(history_sequence),
                gc_prefix_sequence: AtomicU64::new(gc_prefix),
                health: Mutex::new(BTreeMap::new()),
            }));
        }
        // Clone every source store before starting any worker. This leaves all
        // fallible startup reads ahead of task creation and gives partial
        // queue startup one cleanup path with explicit ownership of clones.
        let stores = contexts
            .iter()
            .map(|context| context.store())
            .collect::<Result<Vec<_>, _>>()?;
        let starting_sequences = contexts
            .iter()
            .map(|context| context.progress.sequence())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| PipelineError::infrastructure("projector recovery", error))?;
        let mut queue_workers = Vec::with_capacity(contexts.len());
        let mut projector_tasks = Vec::with_capacity(contexts.len());
        let mut watermark_tasks = Vec::new();
        let mut gc_tasks = Vec::new();
        for shard_index in 0..contexts.len() {
            let context = &contexts[shard_index];
            let queue_result = spawn_shard_queue(
                Arc::clone(&context),
                stores[shard_index].clone(),
                config.queue_capacity_for_shard(context.shard_id),
                config.batch_size,
                config.batch_flush_timeout,
                index_lookup,
                Arc::clone(&metrics),
            );
            let (queue, worker) = match queue_result {
                Ok(queue) => queue,
                Err(error) => {
                    let cleanup_errors =
                        cleanup_partial_start(&contexts, queue_workers, stores).await;
                    let detail = if cleanup_errors.is_empty() {
                        error
                    } else {
                        format!("{error}; startup cleanup: {}", cleanup_errors.join("; "))
                    };
                    return Err(PipelineError::infrastructure("start shard queue", detail));
                }
            };
            queue_workers.push((context.shard_id, worker));
            if let Err(error) = install_queue(context, queue) {
                let cleanup_errors = cleanup_partial_start(&contexts, queue_workers, stores).await;
                let detail = if cleanup_errors.is_empty() {
                    error
                } else {
                    format!("{error}; cleanup: {}", cleanup_errors.join("; "))
                };
                return Err(PipelineError::infrastructure("start shard queue", detail));
            }
        }
        for ((context, store), starting_sequence) in contexts
            .iter()
            .zip(stores.into_iter())
            .zip(starting_sequences)
        {
            let projector = spawn_projector_task(
                Arc::clone(&context),
                store.clone(),
                starting_sequence,
                *context.committed_head_receiver.borrow(),
                config.projector_batch_size,
                context.committed_head_receiver.clone(),
                context.background_stop.subscribe(),
                Arc::clone(&metrics),
            );
            projector_tasks.push((context.shard_id, projector));
            if context.history_durability == HistoryDurability::Durable {
                let watermark = spawn_watermark_task(
                    Arc::clone(&context),
                    store.clone(),
                    config.retention,
                    config.watermark_interval,
                    context.background_stop.subscribe(),
                    Arc::clone(&metrics),
                );
                watermark_tasks.push((context.shard_id, watermark));
                let gc = spawn_gc_task(
                    Arc::clone(&context),
                    store,
                    config.gc_batch_size,
                    config.blocked_gc_sleep,
                    context.background_stop.subscribe(),
                    Arc::clone(&metrics),
                );
                gc_tasks.push((context.shard_id, gc));
            }
        }

        let (shutdown, shutdown_receiver) = watch::channel(false);
        let (shutdown_done_tx, shutdown_done) = watch::channel(None);
        let service_identity = metadata.service_identity;
        tokio::spawn(run_shutdown_coordinator(
            contexts.clone(),
            admission.clone(),
            Arc::clone(&lifecycle),
            shutdown_receiver,
            shutdown.clone(),
            shutdown_done_tx,
            queue_workers,
            projector_tasks,
            watermark_tasks,
            gc_tasks,
            Arc::clone(&storage_batches),
        ));
        Ok(Self {
            shared: Arc::new(Shared {
                config,
                service_identity,
                shards: contexts,
                in_flight,
                admission,
                lifecycle,
                shutdown,
                shutdown_done,
                metrics,
                public_handles: AtomicUsize::new(1),
            }),
        })
    }

    pub async fn credit(
        &self,
        account_id: u64,
        tx_id: u64,
        transaction_at: u64,
        amount: u64,
    ) -> Result<TransactionReceipt, PipelineError> {
        self.submit(Transaction {
            key: TransactionKey {
                account_id,
                tx_id,
                transaction_at,
            },
            operation: Operation::Credit,
            amount,
            refund_of: None,
        })
        .await
    }

    pub async fn debit(
        &self,
        account_id: u64,
        tx_id: u64,
        transaction_at: u64,
        amount: u64,
    ) -> Result<TransactionReceipt, PipelineError> {
        self.submit(Transaction {
            key: TransactionKey {
                account_id,
                tx_id,
                transaction_at,
            },
            operation: Operation::Debit,
            amount,
            refund_of: None,
        })
        .await
    }

    pub async fn refund(
        &self,
        account_id: u64,
        tx_id: u64,
        transaction_at: u64,
        amount: u64,
        debit_tx_id: u64,
        debit_transaction_at: u64,
    ) -> Result<TransactionReceipt, PipelineError> {
        self.submit(Transaction {
            key: TransactionKey {
                account_id,
                tx_id,
                transaction_at,
            },
            operation: Operation::Refund,
            amount,
            refund_of: Some(TransactionKey {
                account_id,
                tx_id: debit_tx_id,
                transaction_at: debit_transaction_at,
            }),
        })
        .await
    }

    pub async fn submit(
        &self,
        transaction: Transaction,
    ) -> Result<TransactionReceipt, PipelineError> {
        let started = Instant::now();
        let result = self.submit_inner(transaction).await;
        self.shared.metrics.record_request();
        self.shared
            .metrics
            .record(Stage::Request, elapsed_ns(started.elapsed()));
        result
    }

    async fn submit_inner(
        &self,
        transaction: Transaction,
    ) -> Result<TransactionReceipt, PipelineError> {
        let shard_id = (transaction.key.account_id % self.shared.config.shard_count as u64) as u32;
        let context = Arc::clone(&self.shared.shards[shard_id as usize]);
        let _attempt = self.shared.admission.begin()?;
        let in_flight = self
            .shared
            .in_flight
            .clone()
            .try_acquire_owned()
            .map_err(|_| PipelineError::Overloaded)?;
        let gate_started = Instant::now();
        let decision = tokio::time::timeout(
            self.shared.config.admission_timeout,
            context.gate.admit(transaction.key.transaction_at),
        )
        .await
        .map_err(|_| PipelineError::NotAccepted("time-boundary admission timed out".to_owned()))?
        .map_err(|error| PipelineError::infrastructure("time-boundary admission", error))?;
        self.shared
            .metrics
            .record(Stage::Admission, elapsed_ns(gate_started.elapsed()));
        match decision {
            (GateDecision::Historical, None) => {
                let lookup_started = Instant::now();
                let permit = acquire_history(&context, context.history_operation_timeout).await?;
                let lookup = match tokio::time::timeout(
                    context.history_operation_timeout,
                    context.history.lookup_transaction(&transaction),
                )
                .await
                {
                    Ok(Ok(lookup)) => {
                        context.clear_health("history_lookup");
                        if context.projector_state.load(Ordering::Acquire) == PROJECTOR_TEMPORARY {
                            context
                                .projector_state
                                .store(PROJECTOR_READY, Ordering::Release);
                            context.clear_health("projector_temporary");
                        }
                        Ok(lookup)
                    }
                    Ok(Err(error)) => {
                        update_history_failure(&context, "history_lookup", &error);
                        Err(history_pipeline_error(
                            error,
                            "lookup historical transaction",
                        ))
                    }
                    Err(_) => {
                        let error = HistoryError::unavailable(format!(
                            "historical lookup exceeded {:?}",
                            context.history_operation_timeout
                        ));
                        update_history_failure(&context, "history_lookup", &error);
                        Err(history_pipeline_error(
                            error,
                            "lookup historical transaction",
                        ))
                    }
                };
                drop(permit);
                self.shared
                    .metrics
                    .record(Stage::HistoryLookup, elapsed_ns(lookup_started.elapsed()));
                match lookup? {
                    HistoricalLookup::ExactReplay(result) => {
                        Ok(receipt_from_history(shard_id, result))
                    }
                    HistoricalLookup::Conflict => Err(PipelineError::Conflict),
                    HistoricalLookup::NotFound => Err(PipelineError::ExpiredTime {
                        transaction_at: transaction.key.transaction_at,
                        watermark: context.gate.watermark().map_err(|error| {
                            PipelineError::infrastructure("read watermark", error)
                        })?,
                    }),
                }
            }
            (GateDecision::Commit, Some(guard)) => {
                let queue = context
                    .queue
                    .lock()
                    .map_err(|_| PipelineError::infrastructure("queue", "queue lock poisoned"))?
                    .as_ref()
                    .cloned()
                    .ok_or(PipelineError::Closed)?;
                let enqueue_started = Instant::now();
                let payload = AcceptedTransaction {
                    transaction,
                    guard,
                    in_flight,
                };
                let request = tokio::time::timeout(
                    self.shared.config.admission_timeout,
                    queue.submit(payload),
                )
                .await
                .map_err(|_| {
                    PipelineError::NotAccepted("bounded queue admission timed out".to_owned())
                })?
                .map_err(|error| PipelineError::NotAccepted(error))?;
                self.shared
                    .metrics
                    .record(Stage::Enqueue, elapsed_ns(enqueue_started.elapsed()));
                let completed = match tokio::time::timeout(
                    self.shared.config.response_timeout,
                    request.wait(),
                )
                .await
                {
                    Ok(Ok(completed)) => completed,
                    Ok(Err(error)) => return Err(PipelineError::accepted_failure(error)),
                    Err(_) => {
                        return Err(PipelineError::accepted_failure(format!(
                            "response deadline {:?} elapsed; retry with the same transaction key and tx_at",
                            self.shared.config.response_timeout
                        )));
                    }
                };
                self.record_queue_stages(&completed);
                completed.reply.into_receipt(shard_id)
            }
            _ => Err(PipelineError::infrastructure(
                "time-boundary admission",
                "gate returned an invalid decision/guard pair",
            )),
        }
    }

    pub async fn balance(&self, account_id: u64) -> Result<BalanceReceipt, PipelineError> {
        let started = Instant::now();
        let result = async {
            let _attempt = self.shared.admission.begin()?;
            let _permit = self
                .shared
                .in_flight
                .clone()
                .try_acquire_owned()
                .map_err(|_| PipelineError::Overloaded)?;
            let shard_id = (account_id % self.shared.config.shard_count as u64) as u32;
            let store = self.shared.shards[shard_id as usize].store()?;
            let (balance, sequence) = store
                .try_balance_snapshot(account_id)
                .map_err(|error| PipelineError::infrastructure("balance", error))?;
            Ok(BalanceReceipt {
                shard_id,
                account_id,
                balance,
                sequence,
            })
        }
        .await;
        self.shared.metrics.record_request();
        self.shared
            .metrics
            .record(Stage::Balance, elapsed_ns(started.elapsed()));
        result
    }

    pub async fn status(&self) -> Result<PipelineStatus, PipelineError> {
        let mut shards = Vec::with_capacity(self.shared.shards.len());
        for context in &self.shared.shards {
            let store = context.store.lock().map_err(|_| {
                PipelineError::infrastructure("status", "source store lock poisoned")
            })?;
            let source_sequence = match store.as_ref() {
                Some(store) => store
                    .try_latest_seq()
                    .map_err(|error| PipelineError::infrastructure("status", error))?,
                None => *context.committed_head_receiver.borrow(),
            };
            let (queued_requests, queue_capacity) = context
                .queue
                .lock()
                .map_err(|_| PipelineError::infrastructure("status", "queue lock poisoned"))?
                .as_ref()
                .map(|queue| {
                    (
                        queue.max_capacity().saturating_sub(queue.capacity()),
                        queue.max_capacity(),
                    )
                })
                .unwrap_or((0, 0));
            shards.push(ShardStatus {
                shard_id: context.shard_id,
                source_identity: context.source_identity.clone(),
                source_sequence,
                projected_sequence: context
                    .progress
                    .sequence()
                    .map_err(|error| PipelineError::infrastructure("status", error))?,
                history_sequence: context.history_sequence.load(Ordering::Acquire),
                watermark: context
                    .gate
                    .watermark()
                    .map_err(|error| PipelineError::infrastructure("status", error))?,
                gc_prefix_sequence: context.gc_prefix_sequence.load(Ordering::Acquire),
                queued_requests,
                queue_capacity,
                history_durability: context.history_durability,
                history_identity: context.history_identity.clone(),
                health: context.health(),
            });
        }
        let maximum = self.shared.config.global_in_flight_limit;
        let available = self.shared.in_flight.available_permits();
        let active = maximum.saturating_sub(available);
        Ok(PipelineStatus {
            lifecycle: Lifecycle::from_byte(self.shared.lifecycle.load(Ordering::Acquire)),
            service_identity: self.shared.service_identity.clone(),
            data_dir: self.shared.config.data_dir.clone(),
            shard_count: self.shared.config.shard_count,
            database_layout: self.shared.config.database_layout,
            in_flight: active,
            in_flight_limit: maximum,
            global_queue_capacity: self.shared.config.global_queue_capacity,
            shards,
            metrics: self.shared.metrics.snapshot(),
        })
    }

    /// Request graceful shutdown. On timeout, the owned coordinator continues;
    /// call `wait_closed` if the caller must keep the Tokio runtime alive until
    /// all accepted work, fences, workers, and RocksDB handles are released.
    pub async fn shutdown(&self) -> Result<(), PipelineError> {
        request_shutdown(
            &self.shared.admission,
            &self.shared.lifecycle,
            &self.shared.shutdown,
        );
        match tokio::time::timeout(self.shared.config.shutdown_timeout, self.wait_closed()).await {
            Ok(result) => result,
            Err(_) => Err(PipelineError::ShutdownTimeout),
        }
    }

    pub async fn wait_closed(&self) -> Result<(), PipelineError> {
        let mut changed = self.shared.shutdown_done.clone();
        loop {
            if let Some(result) = changed.borrow_and_update().clone() {
                return result.map_err(|error| PipelineError::infrastructure("shutdown", error));
            }
            changed.changed().await.map_err(|_| {
                PipelineError::infrastructure("shutdown", "completion channel closed")
            })?;
        }
    }

    pub fn metrics(&self) -> MetricsSnapshot {
        self.shared.metrics.snapshot()
    }

    fn record_queue_stages(&self, completed: &Completed<GuardedReply>) {
        self.shared
            .metrics
            .record(Stage::Queue, elapsed_ns(completed.queue_wait));
        self.shared
            .metrics
            .record(Stage::BatchWait, elapsed_ns(completed.batch_wait));
        self.shared
            .metrics
            .record(Stage::Response, elapsed_ns(completed.response_wait));
    }
}

struct RecoveredShard {
    shard_id: u32,
    source_identity: String,
    store: AccountStore,
    history: Arc<dyn HistoryStore>,
    history_identity: String,
    durability: HistoryDurability,
    latest: u64,
    durable_source_progress: u64,
    history_sequence: u64,
    watermark: u64,
    gc_prefix: u64,
}

async fn recover_shards(
    stores: Vec<AccountStore>,
    histories: Vec<Arc<dyn HistoryStore>>,
    config: &PipelineConfig,
    metadata: &ServiceMetadata,
    history_io: Arc<Semaphore>,
) -> Result<Vec<RecoveredShard>, PipelineError> {
    let mut remaining_stores = stores.into_iter();
    let mut recovered = Vec::new();
    for shard_id in 0..config.shard_count {
        let store = remaining_stores.next().ok_or_else(|| {
            PipelineError::infrastructure("source recovery", "store count mismatch")
        })?;
        let history = Arc::clone(&histories[shard_id as usize]);
        match recover_one_shard(
            shard_id,
            store,
            history,
            config,
            metadata,
            Arc::clone(&history_io),
        )
        .await
        {
            Ok(shard) => recovered.push(shard),
            Err((error, failed_store)) => {
                let mut cleanup = vec![failed_store];
                cleanup.extend(
                    recovered
                        .into_iter()
                        .map(|shard: RecoveredShard| shard.store),
                );
                cleanup.extend(remaining_stores);
                let cleanup_errors = shutdown_unstarted_stores(cleanup).await;
                return Err(if cleanup_errors.is_empty() {
                    error
                } else {
                    PipelineError::infrastructure(
                        "source recovery cleanup",
                        format!("{error}; cleanup: {}", cleanup_errors.join("; ")),
                    )
                });
            }
        }
    }
    Ok(recovered)
}

async fn recover_one_shard(
    shard_id: u32,
    store: AccountStore,
    history: Arc<dyn HistoryStore>,
    config: &PipelineConfig,
    metadata: &ServiceMetadata,
    history_io: Arc<Semaphore>,
) -> Result<RecoveredShard, (PipelineError, AccountStore)> {
    let history_identity = history.destination_identity();
    if history_identity.trim().is_empty() {
        return Err((
            PipelineError::InvalidConfiguration(format!(
                "history destination identity for shard {shard_id} is empty"
            )),
            store,
        ));
    }
    let source_identity = format!("{}:shard:{shard_id}", metadata.service_identity);
    let binding = HistoryBinding {
        service_identity: metadata.service_identity.clone(),
        source_identity: source_identity.clone(),
        shard_id,
        shard_count: config.shard_count,
        schema_version: SERVICE_SCHEMA_VERSION,
        routing_version: ROUTING_VERSION,
    };
    let latest = match store.try_latest_seq() {
        Ok(sequence) => sequence,
        Err(error) => {
            return Err((
                PipelineError::infrastructure("source recovery", error),
                store,
            ));
        }
    };
    let durable_source_progress = match store.durable_projection_progress().await {
        Ok(sequence) => sequence,
        Err(error) => {
            return Err((
                PipelineError::infrastructure("projection recovery", error),
                store,
            ));
        }
    };
    let gc_prefix = store.gc_prefix_seq();
    let persisted_boundary = match store.persisted_projected_before().await {
        Ok(boundary) => boundary,
        Err(error) => {
            return Err((
                PipelineError::infrastructure("watermark recovery", error),
                store,
            ));
        }
    };
    let durability = history.durability();
    let persisted_destination = match store.persisted_history_destination().await {
        Ok(destination) => destination,
        Err(error) => {
            return Err((
                PipelineError::infrastructure("history destination recovery", error),
                store,
            ));
        }
    };
    if durability == HistoryDurability::Volatile {
        if durable_source_progress > 0
            || gc_prefix > 0
            || persisted_boundary.is_some()
            || persisted_destination.is_some()
        {
            return Err((
                PipelineError::InvalidConfiguration(format!(
                    "shard {shard_id} has durable history metadata and cannot open with volatile history"
                )),
                store,
            ));
        }
    } else {
        let result = tokio::time::timeout(
            config.history_operation_timeout,
            history.verify_binding(&binding),
        )
        .await;
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                return Err((
                    history_pipeline_error(error, "bind history destination"),
                    store,
                ));
            }
            Err(_) => {
                return Err((
                    PipelineError::HistoryUnavailable(
                        "history binding validation timed out".to_owned(),
                    ),
                    store,
                ));
            }
        }
    }
    let history_sequence =
        match tokio::time::timeout(config.history_operation_timeout, history.progress()).await {
            Ok(Ok(sequence)) => sequence,
            Ok(Err(error)) => {
                return Err((
                    history_pipeline_error(error, "read history progress"),
                    store,
                ));
            }
            Err(_) => {
                return Err((
                    PipelineError::HistoryUnavailable("history progress read timed out".to_owned()),
                    store,
                ));
            }
        };
    if history_sequence > latest {
        return Err((
            PipelineError::HistoryConflict(format!(
                "destination {} progress {history_sequence} exceeds shard {shard_id} source sequence {latest}",
                history_identity
            )),
            store,
        ));
    }
    if durability == HistoryDurability::Durable {
        if history_sequence < durable_source_progress {
            return Err((
                PipelineError::HistoryConflict(format!(
                    "destination {} covers {history_sequence}, below source durable progress {durable_source_progress} on shard {shard_id}",
                    history_identity
                )),
                store,
            ));
        }
        if let Some(existing) = &persisted_destination {
            if existing != &history_identity {
                return Err((
                    PipelineError::HistoryConflict(format!(
                        "shard {shard_id} is bound to history destination {existing:?}, not {history_identity:?}"
                    )),
                    store,
                ));
            }
        } else if durable_source_progress > 0 || gc_prefix > 0 || persisted_boundary.is_some() {
            return Err((
                PipelineError::Corruption(format!(
                    "shard {shard_id} has durable history progress without a source-side destination binding"
                )),
                store,
            ));
        }
        match tokio::time::timeout(
            config.history_operation_timeout,
            store.bind_history_destination(&history_identity),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                return Err((
                    PipelineError::HistoryConflict(format!(
                        "cannot bind shard {shard_id} to history destination {history_identity:?}: {error}"
                    )),
                    store,
                ));
            }
            Err(_) => {
                return Err((
                    PipelineError::HistoryUnavailable(format!(
                        "binding source shard {shard_id} to history destination timed out"
                    )),
                    store,
                ));
            }
        }
        let bridge: Arc<dyn RefundHistory> = Arc::new(HistoryRefundBridge {
            history: Arc::clone(&history),
            runtime: Handle::current(),
            history_io,
            operation_timeout: config.history_operation_timeout,
        });
        if let Err(error) = store.set_refund_history(bridge) {
            return Err((
                PipelineError::infrastructure("install refund history", error),
                store,
            ));
        }
        let verified_progress = match store.verify_projection_destination().await {
            Ok(progress) => progress,
            Err(error) => {
                return Err((
                    PipelineError::HistoryConflict(format!(
                        "destination validation failed on shard {shard_id}: {error}"
                    )),
                    store,
                ));
            }
        };
        if verified_progress != history_sequence {
            return Err((
                PipelineError::HistoryConflict(format!(
                    "destination progress changed during shard {shard_id} recovery"
                )),
                store,
            ));
        }
    }
    let restored_boundary = if durability == HistoryDurability::Durable {
        match store.restored_projected_before().await {
            Ok(boundary) => boundary,
            Err(error) => {
                return Err((
                    PipelineError::Corruption(format!(
                        "cannot restore shard {shard_id} watermark: {error}"
                    )),
                    store,
                ));
            }
        }
    } else {
        None
    };
    if gc_prefix > 0 && restored_boundary.is_none() {
        return Err((
            PipelineError::Corruption(format!(
                "shard {shard_id} has GC progress without a restorable paired watermark"
            )),
            store,
        ));
    }
    Ok(RecoveredShard {
        shard_id,
        source_identity,
        store,
        history,
        history_identity,
        durability,
        latest,
        durable_source_progress,
        history_sequence,
        watermark: restored_boundary
            .map(|(watermark, _)| watermark)
            .unwrap_or(0),
        gc_prefix,
    })
}

struct HistoryRefundBridge {
    history: Arc<dyn HistoryStore>,
    runtime: Handle,
    history_io: Arc<Semaphore>,
    operation_timeout: Duration,
}

impl RefundHistory for HistoryRefundBridge {
    fn lookup_debit(&self, key: TransactionKey) -> Result<Option<StorageHistoricalDebit>, String> {
        let history = Arc::clone(&self.history);
        let semaphore = Arc::clone(&self.history_io);
        let timeout = self.operation_timeout;
        self.runtime.block_on(async move {
            retry_sync_history(timeout, || {
                let history = Arc::clone(&history);
                let semaphore = Arc::clone(&semaphore);
                async move {
                    let _permit = semaphore
                        .acquire_owned()
                        .await
                        .map_err(|_| HistoryError::unavailable("history IO semaphore closed"))?;
                    history.lookup_debit(key).await
                }
            })
            .await
            .map(|debit| debit.map(to_storage_historical_debit))
            .map_err(|error| error.to_string())
        })
    }

    fn projection_progress(&self) -> Result<u64, String> {
        let history = Arc::clone(&self.history);
        let semaphore = Arc::clone(&self.history_io);
        let timeout = self.operation_timeout;
        self.runtime.block_on(async move {
            retry_sync_history(timeout, || {
                let history = Arc::clone(&history);
                let semaphore = Arc::clone(&semaphore);
                async move {
                    let _permit = semaphore
                        .acquire_owned()
                        .await
                        .map_err(|_| HistoryError::unavailable("history IO semaphore closed"))?;
                    history.progress().await
                }
            })
            .await
            .map_err(|error| error.to_string())
        })
    }
}

async fn retry_sync_history<T, F, Fut>(
    operation_timeout: Duration,
    mut operation: F,
) -> Result<T, HistoryError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, HistoryError>>,
{
    let mut delay = Duration::from_millis(100);
    let mut last_error = None;
    for attempt in 0..5 {
        match tokio::time::timeout(operation_timeout, operation()).await {
            Ok(Ok(value)) => return Ok(value),
            Ok(Err(error)) if error.is_temporary() => last_error = Some(error),
            Ok(Err(error)) => return Err(error),
            Err(_) => last_error = Some(HistoryError::unavailable("history operation timed out")),
        }
        if attempt < 4 {
            tokio::time::sleep(delay).await;
            delay = delay.saturating_mul(2).min(Duration::from_secs(1));
        }
    }
    Err(last_error.unwrap_or_else(|| HistoryError::unavailable("history operation failed")))
}

fn to_storage_historical_debit(debit: HistoricalDebit) -> StorageHistoricalDebit {
    StorageHistoricalDebit {
        amount: debit.amount,
        already_refunded: debit.already_refunded,
    }
}

fn history_pipeline_error(error: HistoryError, operation: &'static str) -> PipelineError {
    match error.kind {
        HistoryErrorKind::Unavailable => {
            PipelineError::HistoryUnavailable(format!("{operation}: {}", error.message))
        }
        HistoryErrorKind::Conflict | HistoryErrorKind::IdentityMismatch => {
            PipelineError::HistoryConflict(format!("{operation}: {}", error.message))
        }
        HistoryErrorKind::Corruption => {
            PipelineError::Corruption(format!("{operation}: {}", error.message))
        }
    }
}

async fn acquire_history(
    context: &ShardContext,
    timeout: Duration,
) -> Result<OwnedSemaphorePermit, PipelineError> {
    tokio::time::timeout(timeout, Arc::clone(&context.history_io).acquire_owned())
        .await
        .map_err(|_| PipelineError::HistoryUnavailable("history IO capacity timed out".to_owned()))?
        .map_err(|_| PipelineError::HistoryUnavailable("history IO gate is closed".to_owned()))
}

fn receipt_from_history(shard_id: u32, result: TransactionResult) -> TransactionReceipt {
    TransactionReceipt {
        shard_id,
        sequence: result.seq,
        status: result.status,
        balance: result.balance,
        replayed: true,
        historical_replay: true,
    }
}

fn spawn_shard_queue(
    context: Arc<ShardContext>,
    store: AccountStore,
    capacity: usize,
    max_batch_size: usize,
    timeout: Duration,
    lookup: IndexLookupConfig,
    metrics: Arc<Metrics>,
) -> Result<(BatchQueue<AcceptedTransaction, GuardedReply>, BatchWorker), String> {
    batch_queue::spawn(
        batch_queue::Config {
            capacity,
            max_batch_size,
            timeout,
        },
        move |requests: Vec<AcceptedTransaction>| {
            let context = Arc::clone(&context);
            let store = store.clone();
            let metrics = Arc::clone(&metrics);
            async move {
                let storage_permit = Arc::clone(&context.storage_batches)
                    .acquire_owned()
                    .await
                    .map_err(|_| "global storage batch gate is closed".to_owned())?;
                let batch_started = Instant::now();
                let mut guards = Vec::with_capacity(requests.len());
                let mut in_flight = Vec::with_capacity(requests.len());
                let transactions = requests
                    .into_iter()
                    .map(|request| {
                        guards.push(request.guard);
                        in_flight.push(request.in_flight);
                        request.transaction
                    })
                    .collect();
                let result = store
                    .handle_batch_with_index_lookup(transactions, lookup)
                    .await;
                let batch_wall_ns = elapsed_ns(batch_started.elapsed());
                drop(storage_permit);
                let result = result?;
                record_index_metrics(&metrics, &result.metrics);
                metrics.record(Stage::Batch, batch_wall_ns);
                let latest = store.try_latest_seq()?;
                context.committed_head.send_replace(latest);
                Ok(result
                    .replies
                    .into_iter()
                    .zip(guards)
                    .zip(in_flight)
                    .map(|((reply, guard), in_flight)| GuardedReply {
                        reply,
                        guard: Some(guard),
                        in_flight: Some(in_flight),
                    })
                    .collect())
            }
        },
    )
}

fn record_index_metrics(metrics: &Metrics, batch: &IndexLookupBatchMetrics) {
    if let Some(query) = batch.query_wall_ns {
        metrics.record(Stage::IndexRead, query);
    } else if batch.native_get_ns > 0 {
        metrics.record(Stage::IndexRead, batch.native_get_ns);
    }
    metrics.record(Stage::Apply, batch.sequential_apply_build_ns);
    metrics.record(
        Stage::SyncPublish,
        batch
            .sync_write_batch_ns
            .saturating_add(batch.memory_publish_ns),
    );
}

fn spawn_projector_task(
    context: Arc<ShardContext>,
    source: AccountStore,
    starting_sequence: u64,
    source_head: u64,
    batch_size: usize,
    committed_head: watch::Receiver<u64>,
    shutdown: watch::Receiver<bool>,
    metrics: Arc<Metrics>,
) -> JoinHandle<Result<(), String>> {
    tokio::spawn(async move {
        let result = run_projector(
            Arc::clone(&context),
            source,
            starting_sequence,
            source_head,
            batch_size,
            committed_head,
            shutdown,
            metrics,
        )
        .await;
        if let Err(error) = &result {
            context
                .projector_state
                .store(PROJECTOR_FAILED, Ordering::Release);
            context.progress.fail(error.clone());
            context.set_health("projector", error.clone());
        }
        result
    })
}

async fn run_projector(
    context: Arc<ShardContext>,
    source: AccountStore,
    starting_sequence: u64,
    source_head: u64,
    batch_size: usize,
    mut committed_head: watch::Receiver<u64>,
    mut shutdown: watch::Receiver<bool>,
    metrics: Arc<Metrics>,
) -> Result<(), String> {
    if batch_size == 0 {
        return Err("projector batch size must be positive".to_owned());
    }
    let mut next_sequence = starting_sequence
        .checked_add(1)
        .ok_or_else(|| "projection sequence overflow".to_owned())?;
    let mut latest = source_head;
    let mut consecutive_history_failures = 0_u32;
    loop {
        if context.projector_state.load(Ordering::Acquire) == PROJECTOR_FAILED {
            return Err("history destination was marked permanently failed".to_owned());
        }
        if next_sequence <= latest {
            let started = Instant::now();
            let source_permit = Arc::clone(&context.storage_batches)
                .acquire_owned()
                .await
                .map_err(|_| "global storage batch gate is closed".to_owned())?;
            let read = source.read_ledger_range(next_sequence, batch_size).await?;
            drop(source_permit);
            if read.records.is_empty() {
                let source_latest = source.try_latest_seq()?;
                if next_sequence <= source_latest {
                    return Err(format!(
                        "projector found no retained source record at sequence {next_sequence}"
                    ));
                }
                latest = source_latest;
                continue;
            }
            let history_permit = Arc::clone(&context.history_io)
                .acquire_owned()
                .await
                .map_err(|_| "history IO gate is closed".to_owned())?;
            let applied = tokio::time::timeout(
                context.history_operation_timeout,
                context.history.apply_batch(&read.records),
            )
            .await
            .unwrap_or_else(|_| {
                Err(HistoryError::unavailable(format!(
                    "history apply exceeded {:?}",
                    context.history_operation_timeout
                )))
            });
            drop(history_permit);
            let acknowledged = match applied {
                Ok(sequence) => {
                    consecutive_history_failures = 0;
                    if context.projector_state.load(Ordering::Acquire) != PROJECTOR_FAILED {
                        context
                            .projector_state
                            .store(PROJECTOR_READY, Ordering::Release);
                        context.clear_health("projector_temporary");
                    }
                    sequence
                }
                Err(error) if error.is_temporary() => {
                    consecutive_history_failures = consecutive_history_failures.saturating_add(1);
                    if context.projector_state.load(Ordering::Acquire) != PROJECTOR_FAILED {
                        context
                            .projector_state
                            .store(PROJECTOR_TEMPORARY, Ordering::Release);
                    }
                    context.set_health("projector_temporary", error.to_string());
                    if consecutive_history_failures >= 5 {
                        return Err(format!(
                            "history destination remained unavailable for {} apply attempts: {error}",
                            consecutive_history_failures
                        ));
                    }
                    let backoff_ms = 100_u64
                        .saturating_mul(1_u64 << (consecutive_history_failures - 1))
                        .min(1_000);
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    continue;
                }
                Err(error) => return Err(error.to_string()),
            };
            let last_read = read
                .records
                .last()
                .map(|record| record.result.seq)
                .ok_or_else(|| "projector read returned no final sequence".to_owned())?;
            if acknowledged < last_read {
                return Err(format!(
                    "history acknowledged sequence {acknowledged} below applied batch end {last_read}"
                ));
            }
            let source_latest = source.try_latest_seq()?;
            if acknowledged > source_latest {
                return Err(format!(
                    "history progress {acknowledged} exceeds source head {source_latest}"
                ));
            }
            if context.history_durability == HistoryDurability::Durable {
                let storage_permit = Arc::clone(&context.storage_batches)
                    .acquire_owned()
                    .await
                    .map_err(|_| "global storage batch gate is closed".to_owned())?;
                let persisted = source.persist_projection_progress(acknowledged).await;
                drop(storage_permit);
                persisted?;
            }
            context
                .history_sequence
                .store(acknowledged, Ordering::Release);
            context.progress.acknowledge(acknowledged)?;
            metrics.record(Stage::Projector, elapsed_ns(started.elapsed()));
            next_sequence = acknowledged
                .checked_add(1)
                .ok_or_else(|| "projection sequence overflow".to_owned())?;
            latest = source_latest;
            continue;
        }
        if *shutdown.borrow() {
            return Ok(());
        }
        tokio::select! {
            changed = committed_head.changed() => {
                if changed.is_err() { latest = source.try_latest_seq()?; }
                else { latest = *committed_head.borrow_and_update(); }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    latest = source.try_latest_seq()?;
                    if next_sequence > latest { return Ok(()); }
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(100)) => {
                latest = source.try_latest_seq()?;
            }
        }
    }
}

fn update_history_failure(context: &ShardContext, component: &'static str, error: &HistoryError) {
    context.set_health(component, error.to_string());
    if error.is_temporary() {
        if context.projector_state.load(Ordering::Acquire) != PROJECTOR_FAILED {
            context
                .projector_state
                .store(PROJECTOR_TEMPORARY, Ordering::Release);
        }
    } else {
        context
            .projector_state
            .store(PROJECTOR_FAILED, Ordering::Release);
        context.progress.fail(error.to_string());
    }
}

fn spawn_watermark_task(
    context: Arc<ShardContext>,
    source: AccountStore,
    retention: Duration,
    interval: Duration,
    shutdown: watch::Receiver<bool>,
    metrics: Arc<Metrics>,
) -> JoinHandle<Result<(), String>> {
    tokio::spawn(async move {
        let result = run_watermarker(
            Arc::clone(&context),
            source,
            retention,
            interval,
            shutdown,
            metrics,
        )
        .await;
        if let Err(error) = &result {
            context.set_health("watermark", error.clone());
        }
        result
    })
}

async fn run_watermarker(
    context: Arc<ShardContext>,
    source: AccountStore,
    retention: Duration,
    interval: Duration,
    mut shutdown: watch::Receiver<bool>,
    metrics: Arc<Metrics>,
) -> Result<(), String> {
    if interval.is_zero() {
        return Err("watermark interval must be positive".to_owned());
    }
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        tokio::select! {
            _ = ticker.tick() => {},
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { return Ok(()); }
            }
        }
        if context.projector_state.load(Ordering::Acquire) != PROJECTOR_READY {
            continue;
        }
        let candidate = unix_time_micros()
            .saturating_sub(u64::try_from(retention.as_micros()).unwrap_or(u64::MAX));
        let Some(fence) = context.gate.begin_fence(candidate)? else {
            continue;
        };
        let started = Instant::now();
        fence.wait_for_admitted_commits().await?;
        let target = source.try_latest_seq()?;
        context.progress.wait_for(target).await?;
        // Acquire the process storage permit only around this concrete sync;
        // never keep it while waiting for guards or projection progress.
        let storage_permit = Arc::clone(&context.storage_batches)
            .acquire_owned()
            .await
            .map_err(|_| "global storage batch gate is closed".to_owned())?;
        let persist = source
            .persist_projected_before_durable(candidate, target)
            .await;
        drop(storage_permit);
        persist?;
        fence.publish()?;
        metrics.record(Stage::Watermark, elapsed_ns(started.elapsed()));
    }
}

fn spawn_gc_task(
    context: Arc<ShardContext>,
    source: AccountStore,
    batch_size: usize,
    blocked_sleep: Duration,
    mut shutdown: watch::Receiver<bool>,
    metrics: Arc<Metrics>,
) -> JoinHandle<Result<(), String>> {
    tokio::spawn(async move {
        if batch_size == 0 || blocked_sleep.is_zero() {
            return Err("GC batch size and blocked sleep must be positive".to_owned());
        }
        let mut delay = Duration::ZERO;
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            if !delay.is_zero() {
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {},
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() { return Ok(()); }
                    }
                }
            } else {
                tokio::task::yield_now().await;
            }
            if context.projector_state.load(Ordering::Acquire) != PROJECTOR_READY {
                delay = blocked_sleep;
                continue;
            }
            let started = Instant::now();
            let storage_permit = Arc::clone(&context.storage_batches)
                .acquire_owned()
                .await
                .map_err(|_| "global storage batch gate is closed".to_owned())?;
            let outcome = source.collect_garbage(batch_size).await;
            drop(storage_permit);
            match outcome {
                Ok(outcome) => {
                    context
                        .gc_prefix_sequence
                        .store(outcome.gc_prefix_seq, Ordering::Release);
                    context.clear_health("gc");
                    metrics.record(Stage::GarbageCollection, elapsed_ns(started.elapsed()));
                    if outcome.deleted == 0 || outcome.blocked_at_seq.is_some() {
                        delay = blocked_sleep;
                    } else {
                        delay = Duration::ZERO;
                    }
                }
                Err(error) => {
                    context.set_health("gc", error);
                    delay = blocked_sleep;
                }
            }
        }
    })
}

#[derive(Clone, Copy)]
enum TaskKind {
    Queue(u32),
    Projector(u32),
    Watermark(u32),
    Gc(u32),
}

struct TaskExit {
    kind: TaskKind,
    result: Result<(), String>,
}

async fn run_shutdown_coordinator(
    contexts: Vec<Arc<ShardContext>>,
    admission: Arc<AdmissionTracker>,
    lifecycle: Arc<AtomicU8>,
    mut shutdown: watch::Receiver<bool>,
    shutdown_sender: watch::Sender<bool>,
    shutdown_done: watch::Sender<Option<Result<(), String>>>,
    queue_workers: Vec<(u32, BatchWorker)>,
    projector_tasks: Vec<(u32, JoinHandle<Result<(), String>>)>,
    watermark_tasks: Vec<(u32, JoinHandle<Result<(), String>>)>,
    gc_tasks: Vec<(u32, JoinHandle<Result<(), String>>)>,
    storage_batches: Arc<Semaphore>,
) {
    let mut tasks = JoinSet::<TaskExit>::new();
    for (shard, worker) in queue_workers {
        tasks.spawn(async move {
            TaskExit {
                kind: TaskKind::Queue(shard),
                result: worker.join().await,
            }
        });
    }
    for (shard, task) in projector_tasks {
        tasks.spawn(async move {
            TaskExit {
                kind: TaskKind::Projector(shard),
                result: join_named(task, "projector").await,
            }
        });
    }
    for (shard, task) in watermark_tasks {
        tasks.spawn(async move {
            TaskExit {
                kind: TaskKind::Watermark(shard),
                result: join_named(task, "watermark").await,
            }
        });
    }
    for (shard, task) in gc_tasks {
        tasks.spawn(async move {
            TaskExit {
                kind: TaskKind::Gc(shard),
                result: join_named(task, "GC").await,
            }
        });
    }
    let mut queue_remaining = contexts.len();
    let mut background_remaining = contexts.len()
        + contexts
            .iter()
            .filter(|context| context.history_durability == HistoryDurability::Durable)
            .count()
            * 2;
    let mut shutdown_started = *shutdown.borrow();
    let mut observed_errors = Vec::new();
    while !shutdown_started {
        tokio::select! {
            changed = shutdown.changed() => {
                shutdown_started = changed.is_err() || *shutdown.borrow();
            }
            joined = tasks.join_next(), if !tasks.is_empty() => {
                match joined {
                    Some(Ok(exit)) => {
                        let is_queue = matches!(exit.kind, TaskKind::Queue(_));
                        if let Some(error) = handle_task_exit(exit, &contexts, &admission, &lifecycle, &mut shutdown_started, &shutdown_sender) {
                            observed_errors.push(error);
                        }
                        if is_queue {
                            queue_remaining = queue_remaining.saturating_sub(1);
                        } else {
                            background_remaining = background_remaining.saturating_sub(1);
                        }
                    }
                    Some(Err(error)) => {
                        set_lifecycle_degraded(&lifecycle);
                        for context in &contexts { context.set_health("supervisor", error.to_string()); }
                        observed_errors.push(format!("supervised task join failed: {error}"));
                    }
                    None => { shutdown_started = true; }
                }
            }
        }
    }
    request_shutdown(&admission, &lifecycle, &shutdown_sender);
    for context in &contexts {
        if let Ok(mut queue) = context.queue.lock() {
            queue.take();
        }
    }
    admission.wait_idle().await;
    while queue_remaining > 0 {
        match tasks.join_next().await {
            Some(Ok(exit)) => {
                let is_queue = matches!(exit.kind, TaskKind::Queue(_));
                if let Some(error) = handle_task_exit(
                    exit,
                    &contexts,
                    &admission,
                    &lifecycle,
                    &mut shutdown_started,
                    &shutdown_sender,
                ) {
                    observed_errors.push(error);
                }
                if is_queue {
                    queue_remaining = queue_remaining.saturating_sub(1);
                } else {
                    background_remaining = background_remaining.saturating_sub(1);
                }
            }
            Some(Err(error)) => {
                set_lifecycle_degraded(&lifecycle);
                for context in &contexts {
                    context.set_health("supervisor", error.to_string());
                }
                observed_errors.push(format!("supervised task join failed: {error}"));
            }
            None => break,
        }
    }
    for context in &contexts {
        context.background_stop.send_replace(true);
    }
    while background_remaining > 0 {
        match tasks.join_next().await {
            Some(Ok(exit)) => {
                let is_queue = matches!(exit.kind, TaskKind::Queue(_));
                if let Some(error) = handle_task_exit(
                    exit,
                    &contexts,
                    &admission,
                    &lifecycle,
                    &mut shutdown_started,
                    &shutdown_sender,
                ) {
                    observed_errors.push(error);
                }
                if !is_queue {
                    background_remaining = background_remaining.saturating_sub(1);
                }
            }
            Some(Err(error)) => {
                set_lifecycle_degraded(&lifecycle);
                for context in &contexts {
                    context.set_health("supervisor", error.to_string());
                }
                observed_errors.push(format!("supervised task join failed: {error}"));
                background_remaining = background_remaining.saturating_sub(1);
            }
            None => break,
        }
    }
    let mut close_errors = Vec::new();
    for context in &contexts {
        let store = context.store.lock().ok().and_then(|mut store| store.take());
        if let Some(store) = store {
            match store.shutdown_on_blocking().await {
                Ok(()) => {}
                Err(error) => {
                    close_errors.push(format!("shard {} close: {error}", context.shard_id))
                }
            }
        }
    }
    // Dropping the service-owned budget gate happens after every storage task
    // has joined. AccountStore shutdown consumes its last service handle.
    drop(storage_batches);
    for context in &contexts {
        for message in context.health() {
            observed_errors.push(format!("shard {} {message}", context.shard_id));
        }
    }
    lifecycle.store(LIFE_CLOSED, Ordering::Release);
    observed_errors.extend(close_errors);
    let result = if observed_errors.is_empty() {
        Ok(())
    } else {
        Err(observed_errors.join("; "))
    };
    shutdown_done.send_replace(Some(result));
}

async fn join_named(
    task: JoinHandle<Result<(), String>>,
    name: &'static str,
) -> Result<(), String> {
    task.await
        .map_err(|error| format!("{name} task did not join cleanly: {error}"))?
}

fn handle_task_exit(
    exit: TaskExit,
    contexts: &[Arc<ShardContext>],
    admission: &AdmissionTracker,
    lifecycle: &AtomicU8,
    shutdown_started: &mut bool,
    shutdown: &watch::Sender<bool>,
) -> Option<String> {
    let (shard_id, component, queue_worker) = match exit.kind {
        TaskKind::Queue(shard) => (shard, "queue", true),
        TaskKind::Projector(shard) => (shard, "projector", false),
        TaskKind::Watermark(shard) => (shard, "watermark", false),
        TaskKind::Gc(shard) => (shard, "gc", false),
    };
    let context = contexts.get(shard_id as usize);
    let mut observed_error = None;
    if let Err(error) = exit.result {
        if let Some(context) = context {
            context.set_health(component, error.clone());
            if component == "projector" {
                context
                    .projector_state
                    .store(PROJECTOR_FAILED, Ordering::Release);
                context.progress.fail(error.clone());
            }
        }
        set_lifecycle_degraded(lifecycle);
        observed_error = Some(format!("shard {shard_id} {component} failed: {error}"));
    } else if !*shutdown_started {
        if let Some(context) = context {
            context.set_health(component, "worker exited before service shutdown");
        }
        set_lifecycle_degraded(lifecycle);
        observed_error = Some(format!(
            "shard {shard_id} {component} exited before shutdown"
        ));
    }
    if queue_worker && !*shutdown_started {
        request_shutdown(admission, lifecycle, shutdown);
        *shutdown_started = true;
    }
    observed_error
}

fn set_lifecycle_degraded(lifecycle: &AtomicU8) {
    let _ = lifecycle.compare_exchange(
        LIFE_RUNNING,
        LIFE_DEGRADED,
        Ordering::AcqRel,
        Ordering::Acquire,
    );
}

fn request_shutdown(
    admission: &AdmissionTracker,
    lifecycle: &AtomicU8,
    shutdown: &watch::Sender<bool>,
) {
    admission.close();
    loop {
        let current = lifecycle.load(Ordering::Acquire);
        if current == LIFE_CLOSED || current == LIFE_CLOSING {
            break;
        }
        if lifecycle
            .compare_exchange(current, LIFE_CLOSING, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            break;
        }
    }
    if lifecycle.load(Ordering::Acquire) != LIFE_CLOSED {
        shutdown.send_replace(true);
    }
}

async fn open_source_stores(config: &PipelineConfig) -> Result<Vec<AccountStore>, PipelineError> {
    let mut stores = Vec::with_capacity(config.shard_count as usize);
    match config.database_layout {
        DatabaseLayout::Shared => {
            let path = config.data_dir.join("database");
            let budget = rocksdb_budget(config, 1)?;
            ensure_durable_directory(&path).await?;
            let (db, options) = AccountStore::open_database_with_budget(&path, budget)
                .await
                .map_err(|error| PipelineError::infrastructure("open shared RocksDB", error))?;
            for shard_id in 0..config.shard_count {
                match AccountStore::open_dynamic_on_database(
                    Arc::clone(&db),
                    Arc::clone(&options),
                    shard_id,
                )
                .await
                {
                    Ok(store) => stores.push(store),
                    Err(error) => {
                        let mut errors =
                            shutdown_unstarted_stores(std::mem::take(&mut stores)).await;
                        if let Err(release_error) = drop_rocksdb_handles(db, options).await {
                            errors.push(release_error.to_string());
                        }
                        let detail = if errors.is_empty() {
                            error
                        } else {
                            format!("{error}; cleanup: {}", errors.join("; "))
                        };
                        return Err(PipelineError::infrastructure(
                            "recover shared shard",
                            detail,
                        ));
                    }
                }
            }
            drop_rocksdb_handles(db, options).await?;
        }
        DatabaseLayout::Dedicated => {
            let budget = rocksdb_budget(config, config.shard_count as usize)?;
            for shard_id in 0..config.shard_count {
                let path = config
                    .data_dir
                    .join(format!("shard-{shard_id}"))
                    .join("database");
                if let Err(error) = ensure_durable_directory(&path).await {
                    let cleanup_errors =
                        shutdown_unstarted_stores(std::mem::take(&mut stores)).await;
                    return Err(if cleanup_errors.is_empty() {
                        error
                    } else {
                        PipelineError::infrastructure(
                            "prepare dedicated RocksDB directory",
                            format!("{error}; cleanup: {}", cleanup_errors.join("; ")),
                        )
                    });
                }
                let (db, options) =
                    match AccountStore::open_database_with_budget(&path, budget.clone()).await {
                        Ok(opened) => opened,
                        Err(error) => {
                            let cleanup_errors =
                                shutdown_unstarted_stores(std::mem::take(&mut stores)).await;
                            let detail = if cleanup_errors.is_empty() {
                                error
                            } else {
                                format!("{error}; cleanup: {}", cleanup_errors.join("; "))
                            };
                            return Err(PipelineError::infrastructure(
                                "open dedicated RocksDB",
                                detail,
                            ));
                        }
                    };
                let result = AccountStore::open_dynamic_on_database(
                    Arc::clone(&db),
                    Arc::clone(&options),
                    shard_id,
                )
                .await;
                if let Err(release_error) = drop_rocksdb_handles(db, options).await {
                    let mut errors = Vec::new();
                    match result {
                        Ok(store) => {
                            if let Err(error) = store.shutdown_on_blocking().await {
                                errors.push(format!("close current shard {shard_id}: {error}"));
                            }
                        }
                        Err(error) => {
                            errors.push(format!("recover current shard {shard_id}: {error}"))
                        }
                    }
                    errors.extend(shutdown_unstarted_stores(std::mem::take(&mut stores)).await);
                    errors.push(release_error.to_string());
                    return Err(PipelineError::infrastructure(
                        "release RocksDB",
                        errors.join("; "),
                    ));
                }
                match result {
                    Ok(store) => stores.push(store),
                    Err(error) => {
                        let cleanup_errors =
                            shutdown_unstarted_stores(std::mem::take(&mut stores)).await;
                        let detail = if cleanup_errors.is_empty() {
                            error
                        } else {
                            format!("{error}; cleanup: {}", cleanup_errors.join("; "))
                        };
                        return Err(PipelineError::infrastructure(
                            "recover dedicated shard",
                            detail,
                        ));
                    }
                }
            }
        }
    }
    Ok(stores)
}

fn rocksdb_budget(
    config: &PipelineConfig,
    databases: usize,
) -> Result<RocksDbBudget, PipelineError> {
    let write_buffer_size = config.rocksdb_write_buffer_budget_bytes / databases / 2;
    let block_cache_bytes = config.rocksdb_block_cache_budget_bytes / databases;
    let max_background_jobs = config.rocksdb_background_jobs_budget / databases as i32;
    if write_buffer_size == 0 || block_cache_bytes == 0 || max_background_jobs < 1 {
        return Err(PipelineError::InvalidConfiguration(
            "RocksDB process budget is too small for configured DB count".to_owned(),
        ));
    }
    Ok(RocksDbBudget {
        write_buffer_size,
        max_write_buffer_number: 2,
        block_cache_bytes,
        max_background_jobs,
    })
}

async fn shutdown_unstarted_stores(stores: Vec<AccountStore>) -> Vec<String> {
    let mut errors = Vec::new();
    for store in stores {
        if let Err(error) = store.shutdown_on_blocking().await {
            errors.push(error);
        }
    }
    errors
}

async fn cleanup_partial_start(
    contexts: &[Arc<ShardContext>],
    queue_workers: Vec<(u32, BatchWorker)>,
    store_clones: Vec<AccountStore>,
) -> Vec<String> {
    for context in contexts {
        if let Ok(mut queue) = context.queue.lock() {
            queue.take();
        }
    }
    let mut errors = Vec::new();
    for (shard_id, worker) in queue_workers {
        if let Err(error) = worker.join().await {
            errors.push(format!("shard {shard_id} queue join: {error}"));
        }
    }
    if let Err(error) = tokio::task::spawn_blocking(move || drop(store_clones)).await {
        errors.push(format!("release startup store clones: {error}"));
    }
    for context in contexts {
        let store = context.store.lock().ok().and_then(|mut store| store.take());
        if let Some(store) = store {
            if let Err(error) = store.shutdown_on_blocking().await {
                errors.push(format!("shard {} close: {error}", context.shard_id));
            }
        }
    }
    errors
}

fn install_queue(
    context: &ShardContext,
    queue: BatchQueue<AcceptedTransaction, GuardedReply>,
) -> Result<(), String> {
    match context.queue.lock() {
        Ok(mut slot) => {
            *slot = Some(queue);
            Ok(())
        }
        Err(poisoned) => {
            drop(poisoned);
            Err("queue state lock poisoned during startup".to_owned())
        }
    }
}

async fn ensure_durable_directory(path: &Path) -> Result<(), PipelineError> {
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || create_and_sync_directory_tree(&path))
        .await
        .map_err(|error| {
            PipelineError::infrastructure("prepare source directory", error.to_string())
        })?
        .map_err(|error| PipelineError::infrastructure("prepare source directory", error))
}

async fn drop_rocksdb_handles(db: Arc<DB>, options: Arc<Options>) -> Result<(), PipelineError> {
    tokio::task::spawn_blocking(move || drop((db, options)))
        .await
        .map_err(|error| {
            PipelineError::infrastructure("release RocksDB handles", error.to_string())
        })
}

fn create_and_sync_directory_tree(path: &Path) -> Result<(), String> {
    let absolute_path = if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir()
            .map_err(|error| format!("cannot resolve current directory: {error}"))?
            .join(path)
    };
    fs::create_dir_all(&absolute_path)
        .map_err(|error| format!("cannot create {}: {error}", absolute_path.display()))?;
    let mut directory = Some(absolute_path.as_path());
    while let Some(path) = directory {
        File::open(path)
            .and_then(|file| file.sync_all())
            .map_err(|error| format!("cannot sync directory {}: {error}", path.display()))?;
        directory = path.parent();
    }
    Ok(())
}

fn check_startup_resources(config: &PipelineConfig) -> Result<(), String> {
    let absolute_path = if config.data_dir.is_absolute() {
        config.data_dir.clone()
    } else {
        std::env::current_dir()
            .map_err(|error| format!("cannot resolve current directory: {error}"))?
            .join(&config.data_dir)
    };
    let mut candidate = absolute_path.as_path();
    while !candidate.exists() {
        candidate = candidate
            .parent()
            .ok_or_else(|| "data directory has no existing parent".to_owned())?;
    }
    let metadata = fs::metadata(candidate)
        .map_err(|error| format!("cannot inspect {}: {error}", candidate.display()))?;
    if !metadata.is_dir() {
        return Err(format!(
            "data directory parent {} is not a directory",
            candidate.display()
        ));
    }
    #[cfg(target_os = "linux")]
    {
        check_linux_filesystem(candidate, config.startup_min_free_disk_bytes)?;
        check_linux_available_memory(config.startup_min_available_memory_bytes)?;
        Ok(())
    }
    #[cfg(not(target_os = "linux"))]
    {
        if config.startup_min_free_disk_bytes > 0 || config.startup_min_available_memory_bytes > 0 {
            return Err(
                "startup disk/memory admission checks are implemented only on Linux; set both thresholds to zero to disable"
                    .to_owned(),
            );
        }
        Ok(())
    }
}

#[cfg(target_os = "linux")]
fn check_linux_filesystem(path: &Path, minimum_free_bytes: usize) -> Result<(), String> {
    use std::os::unix::ffi::OsStrExt;

    let path_string = std::ffi::CString::new(path.as_os_str().as_bytes())
        .map_err(|_| "data directory path contains a NUL byte".to_owned())?;
    // SAFETY: `path_string` is a NUL-terminated path and `stats` is a valid
    // writable struct for the duration of the syscall.
    let mut filesystem = unsafe { std::mem::zeroed::<libc::statfs>() };
    if unsafe { libc::statfs(path_string.as_ptr(), &mut filesystem) } != 0 {
        return Err(format!(
            "cannot inspect filesystem for {}: {}",
            path.display(),
            std::io::Error::last_os_error()
        ));
    }
    let fs_type = filesystem.f_type as u64;
    const TMPFS_MAGIC: u64 = 0x0102_1994;
    const RAMFS_MAGIC: u64 = 0x8584_58f6;
    if fs_type == TMPFS_MAGIC || fs_type == RAMFS_MAGIC {
        return Err(format!(
            "source database path {} is on a volatile tmpfs/ramfs filesystem",
            path.display()
        ));
    }

    let mut stats = unsafe { std::mem::zeroed::<libc::statvfs>() };
    if unsafe { libc::statvfs(path_string.as_ptr(), &mut stats) } != 0 {
        return Err(format!(
            "cannot read free disk space for {}: {}",
            path.display(),
            std::io::Error::last_os_error()
        ));
    }
    let available = u128::from(stats.f_bavail).saturating_mul(u128::from(stats.f_frsize));
    if available < minimum_free_bytes as u128 {
        return Err(format!(
            "source filesystem has {available} free bytes; configured minimum is {minimum_free_bytes}"
        ));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn check_linux_available_memory(minimum_available_bytes: usize) -> Result<(), String> {
    if minimum_available_bytes == 0 {
        return Ok(());
    }
    let contents = fs::read_to_string("/proc/meminfo")
        .map_err(|error| format!("cannot read /proc/meminfo: {error}"))?;
    let available_kib = contents
        .lines()
        .find_map(|line| {
            let (key, values) = line.split_once(':')?;
            if key != "MemAvailable" {
                return None;
            }
            values.split_whitespace().next()?.parse::<u64>().ok()
        })
        .ok_or_else(|| "MemAvailable is missing from /proc/meminfo".to_owned())?;
    let available_bytes = u128::from(available_kib).saturating_mul(1024);
    if available_bytes < minimum_available_bytes as u128 {
        return Err(format!(
            "Linux reports {available_bytes} MemAvailable bytes; configured minimum is {minimum_available_bytes}"
        ));
    }
    Ok(())
}

#[derive(Clone)]
struct ServiceMetadata {
    service_identity: String,
}

fn load_or_create_service_metadata(
    data_dir: &Path,
    config: &PipelineConfig,
) -> Result<ServiceMetadata, String> {
    create_and_sync_directory_tree(data_dir)?;
    let metadata_path = data_dir.join("service.meta");
    if metadata_path.exists() {
        return read_and_parse_service_metadata(&metadata_path, config);
    }
    let mut directory_has_entries = false;
    for entry in
        fs::read_dir(data_dir).map_err(|error| format!("cannot inspect data directory: {error}"))?
    {
        let entry =
            entry.map_err(|error| format!("cannot inspect data directory entry: {error}"))?;
        if !entry
            .file_name()
            .to_string_lossy()
            .starts_with("service.meta.tmp-")
        {
            directory_has_entries = true;
            break;
        }
    }
    if directory_has_entries {
        if metadata_path.exists() {
            return read_and_parse_service_metadata(&metadata_path, config);
        }
        return Err(format!(
            "service metadata is missing from existing data directory {}; refusing unversioned source data",
            data_dir.display()
        ));
    }
    let identity = new_service_identity()?;
    let contents = format!(
        "format=ledger-wallet-service\nschema_version={SERVICE_SCHEMA_VERSION}\nrouting_version={ROUTING_VERSION}\nrouting={ROUTING_NAME}\nshard_count={}\ndatabase_layout={}\nbalance_mode={BALANCE_MODE_NAME}\nservice_identity={identity}\n",
        config.shard_count,
        config.database_layout.as_str(),
    );
    let temp_path = data_dir.join(format!("service.meta.tmp-{identity}"));
    let mut temp = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp_path)
        .map_err(|error| format!("cannot create temporary service metadata: {error}"))?;
    temp.write_all(contents.as_bytes())
        .and_then(|_| temp.sync_all())
        .map_err(|error| format!("cannot sync service metadata: {error}"))?;
    drop(temp);
    match fs::hard_link(&temp_path, &metadata_path) {
        Ok(()) => {
            fs::remove_file(&temp_path)
                .map_err(|error| format!("cannot remove published metadata temporary: {error}"))?;
            sync_directory(data_dir)?;
            parse_service_metadata(&contents, config)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            // Another opener atomically published the immutable manifest.
            // The temporary file is ours, so remove only that entry and use
            // the winner's synced contents and identity.
            fs::remove_file(&temp_path).map_err(|remove_error| {
                format!("manifest already exists, but temporary cleanup failed: {remove_error}")
            })?;
            sync_directory(data_dir)?;
            read_and_parse_service_metadata(&metadata_path, config)
        }
        Err(error) => {
            let _ = fs::remove_file(&temp_path);
            Err(format!(
                "cannot publish service metadata without replacement: {error}"
            ))
        }
    }
}

fn read_and_parse_service_metadata(
    metadata_path: &Path,
    config: &PipelineConfig,
) -> Result<ServiceMetadata, String> {
    let mut contents = String::new();
    File::open(metadata_path)
        .and_then(|mut file| file.read_to_string(&mut contents))
        .map_err(|error| format!("cannot read service metadata: {error}"))?;
    parse_service_metadata(&contents, config)
}

fn sync_directory(path: &Path) -> Result<(), String> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| format!("cannot sync directory {}: {error}", path.display()))
}

fn parse_service_metadata(
    contents: &str,
    config: &PipelineConfig,
) -> Result<ServiceMetadata, String> {
    let mut fields = BTreeMap::new();
    for line in contents.lines() {
        let (key, value) = line
            .split_once('=')
            .ok_or_else(|| "malformed service metadata line".to_owned())?;
        if fields.insert(key, value).is_some() {
            return Err(format!("duplicate service metadata field {key}"));
        }
    }
    let expected = [
        ("format", "ledger-wallet-service".to_owned()),
        ("schema_version", SERVICE_SCHEMA_VERSION.to_string()),
        ("routing_version", ROUTING_VERSION.to_string()),
        ("routing", ROUTING_NAME.to_owned()),
        ("shard_count", config.shard_count.to_string()),
        (
            "database_layout",
            config.database_layout.as_str().to_owned(),
        ),
        ("balance_mode", BALANCE_MODE_NAME.to_owned()),
    ];
    for (key, expected_value) in &expected {
        if fields.get(*key).copied() != Some(expected_value.as_str()) {
            return Err(format!(
                "service metadata mismatch for {key}: expected {expected_value:?}, got {:?}",
                fields.get(key)
            ));
        }
    }
    let identity = fields
        .get("service_identity")
        .copied()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "service metadata has no service identity".to_owned())?;
    if fields.len() != expected.len() + 1 {
        return Err("service metadata contains unsupported fields".to_owned());
    }
    Ok(ServiceMetadata {
        service_identity: identity.to_owned(),
    })
}

fn new_service_identity() -> Result<String, String> {
    let mut bytes = [0_u8; 16];
    File::open("/dev/urandom")
        .and_then(|mut source| source.read_exact(&mut bytes))
        .map_err(|error| {
            format!("cannot read operating-system random service identity: {error}")
        })?;
    let mut identity = String::with_capacity(36);
    identity.push_str("svc-");
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut identity, "{byte:02x}")
            .map_err(|error| format!("cannot encode service identity: {error}"))?;
    }
    Ok(identity)
}

fn elapsed_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn unix_time_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_micros()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}
