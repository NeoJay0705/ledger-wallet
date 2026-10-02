//! Standalone benchmark storage handler for Tokio account transactions.
//!
//! Foreground requests are submitted as awaited batches. The only concurrent
//! work is the bounded, sequential balance-checkpoint writer in checkpoint
//! mode.

use rocksdb::statistics::{StatsLevel, Ticker};
use rocksdb::{
    BlockBasedOptions, Cache, Direction as DbDirection, IteratorMode, Options, WriteBatch,
    WriteOptions, DB, DEFAULT_COLUMN_FAMILY_NAME,
};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex as AsyncMutex};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TransactionKey {
    pub account_id: u64,
    pub tx_id: u64,
    pub transaction_at: u64,
}

#[cfg(test)]
#[path = "ledger_index_lookup_fault_tests.rs"]
mod index_lookup_fault_tests;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Operation {
    Credit,
    Debit,
    Refund,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Transaction {
    pub key: TransactionKey,
    pub operation: Operation,
    pub amount: u64,
    pub refund_of: Option<TransactionKey>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionStatus {
    Applied,
    InsufficientFunds,
    CreditOverflow,
    InvalidAmount,
    InvalidRefund,
    RefundAlreadyUsed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransactionResult {
    pub status: TransactionStatus,
    pub balance: u64,
    pub seq: u64,
}

/// A decoded, committed ledger record returned in sequence order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LedgerRecord {
    pub request: Transaction,
    pub result: TransactionResult,
}

/// Records read by one contiguous ledger-range request.
#[derive(Clone, Debug)]
pub struct LedgerRangeRead {
    pub records: Vec<LedgerRecord>,
    /// Time spent in RocksDB reads and record decoding, excluding blocking-pool
    /// queue and async dispatch time.
    pub db_read_ns: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Reply {
    Transaction {
        status: TransactionStatus,
        balance: u64,
        seq: u64,
        replayed: bool,
    },
    Conflict,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BalanceMode {
    PerBatch,
    Checkpoint,
}

const MAX_INDEX_LOOKUP_GROUP_SIZE: usize = 2_048;
const MAX_INDEX_LOOKUP_IN_FLIGHT: usize = 8;

/// Immutable strategy selection for one account-store invocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexLookupMode {
    PointGet,
    WholeBatchMultiGet,
    Chunked {
        group_size: usize,
        max_in_flight: usize,
    },
}

/// Validated transaction-index lookup strategy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IndexLookupConfig {
    mode: IndexLookupMode,
}

impl IndexLookupConfig {
    pub fn new(mode: IndexLookupMode) -> Result<Self, String> {
        if let IndexLookupMode::Chunked {
            group_size,
            max_in_flight,
        } = mode
        {
            if group_size == 0 || group_size > MAX_INDEX_LOOKUP_GROUP_SIZE {
                return Err(format!(
                    "index lookup group size must be in 1..={MAX_INDEX_LOOKUP_GROUP_SIZE}"
                ));
            }
            if max_in_flight == 0 || max_in_flight > MAX_INDEX_LOOKUP_IN_FLIGHT {
                return Err(format!(
                    "index lookup max in-flight groups must be in 1..={MAX_INDEX_LOOKUP_IN_FLIGHT}"
                ));
            }
        }
        Ok(Self { mode })
    }

    pub fn mode(self) -> IndexLookupMode {
        self.mode
    }
}

/// Per-group read timings. Durations are cumulative within each group; groups
/// can overlap, so these values must not be added to the batch wall time.
#[derive(Clone, Debug, Default)]
pub struct IndexLookupGroupMetrics {
    pub group_index: usize,
    pub first_position: usize,
    pub key_count: usize,
    pub blocking_pool_wait_ns: u64,
    pub native_get_ns: u64,
    pub decode_ns: u64,
    pub submit_to_collection_ns: u64,
}

/// Opt-in per-batch metrics returned by the selected-strategy handler.
#[derive(Clone, Debug)]
pub struct IndexLookupBatchMetrics {
    pub mode: IndexLookupMode,
    pub transaction_count: usize,
    pub dispatch_wait_ns: u64,
    /// Time from immediately before acquisition of the exclusive account
    /// batch gate until it is acquired. This includes prior batches and GC.
    pub batch_gate_wait_ns: u64,
    pub key_prep_ns: u64,
    /// `None` for the legacy interleaved point-get loop.
    pub query_wall_ns: Option<u64>,
    /// Lookup worker wait; `None` for PointGet because reads and apply share
    /// the legacy process worker.
    pub blocking_pool_wait_ns: Option<u64>,
    pub native_get_ns: u64,
    pub decode_ns: u64,
    /// Sum of group submit-to-collection durations. Parallel group values
    /// overlap; `None` for PointGet, which has no lookup groups.
    pub submit_to_collection_ns: Option<u64>,
    /// Wait for the sequential apply/build worker to be collected.
    pub apply_submit_to_collection_ns: u64,
    /// Apply worker wait; for PointGet this is the wait for the legacy worker
    /// that interleaves reads with apply.
    pub apply_blocking_pool_wait_ns: u64,
    /// Includes the in-loop point reads and decode for PointGet, matching its
    /// original interleaved lookup/apply order.
    pub sequential_apply_build_ns: u64,
    pub sync_write_batch_ns: u64,
    pub memory_publish_ns: u64,
    /// Native RocksDB `get` calls or batched `MultiGet` calls, as selected.
    pub get_calls: u64,
    /// Transaction-index keys returned by the native read operation.
    pub keys_looked_up: u64,
    pub hits: u64,
    pub misses: u64,
    pub groups_submitted: usize,
    pub max_observed_in_flight_groups: usize,
    /// Maximum blocking MultiGet closures simultaneously running. PointGet
    /// has no group jobs and reports zero here.
    pub max_observed_running_query_jobs: usize,
    /// Completion order, which may differ from group order.
    pub groups: Vec<IndexLookupGroupMetrics>,
}

#[derive(Clone, Debug)]
pub struct IndexLookupBatchResult {
    pub replies: Vec<Reply>,
    pub metrics: IndexLookupBatchMetrics,
}

#[derive(Clone, Copy, Debug)]
pub struct CheckpointSample {
    pub sequence: u64,
    pub duration_ns: u64,
    pub chunk_sync_ns: u64,
    pub manifest_sync_ns: u64,
    pub completed_at: Instant,
}

#[derive(Clone, Debug, Default)]
pub struct MetricsSnapshot {
    pub transactions: u64,
    pub transaction_latency_ns: u64,
    pub transaction_samples_ns: Vec<u64>,
    pub balance_queries: u64,
    pub balance_latency_ns: u64,
    pub balance_samples_ns: Vec<u64>,
    pub batches: u64,
    pub read_build_ns: u64,
    pub wal_sync_ns: u64,
    pub publish_ns: u64,
    pub checkpoint_count: u64,
    pub checkpoint_snapshot_ns: u64,
    pub checkpoint_queue_wait_ns: u64,
    pub checkpoint_chunk_sync_ns: u64,
    pub checkpoint_manifest_sync_ns: u64,
    pub checkpoint_duration_ns: u64,
    pub checkpoint_latest_seq: u64,
    pub checkpoint_snapshots_enqueued: u64,
    pub checkpoint_samples: Vec<CheckpointSample>,
    pub projection_progress_sync_ns: u64,
    pub gc_scan_ns: u64,
    pub gc_delete_ns: u64,
    pub gc_write_ns: u64,
    pub gc_records_scanned: u64,
    pub gc_records_deleted: u64,
    pub gc_bytes_deleted: u64,
}

/// Minimal projected debit state needed to validate refunds after source GC.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HistoricalDebit {
    pub amount: u64,
    pub already_refunded: bool,
}

/// Read-only history interface used when an old debit or refund marker has
/// been removed from the source ledger. Implementations must reflect the
/// historical destination's durable-apply contract.
pub trait RefundHistory: Send + Sync {
    fn lookup_debit(&self, key: TransactionKey) -> Result<Option<HistoricalDebit>, String>;

    /// Sequence reported by the external destination under the benchmark's
    /// successful-apply-is-durable contract.
    fn projection_progress(&self) -> Result<u64, String>;
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GcStepOutcome {
    pub scanned: u64,
    pub deleted: u64,
    pub gc_prefix_seq: u64,
    pub blocked_at_seq: Option<u64>,
    pub scan_ns: u64,
    pub delete_ns: u64,
    pub write_ns: u64,
    pub bytes_deleted: u64,
}

struct State {
    balances: HashMap<u64, u64>,
    seq: u64,
}

struct Inner {
    db: Arc<DB>,
    options: Arc<Options>,
    keyspace: Keyspace,
    mode: BalanceMode,
    account_ids: Vec<u64>,
    checkpoint_quantity: u64,
    state: Mutex<State>,
    batch_gate: AsyncMutex<()>,
    checkpoint_tx: Mutex<Option<mpsc::Sender<CheckpointMessage>>>,
    checkpoint_worker: Mutex<Option<tokio::task::JoinHandle<Result<(), String>>>>,
    checkpoint_failure: Arc<Mutex<Option<String>>>,
    metrics: Arc<Mutex<MetricsSnapshot>>,
    refund_history: RwLock<Option<Arc<dyn RefundHistory>>>,
    projected_seq: std::sync::atomic::AtomicU64,
    gc_prefix_seq: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    index_lookup_test_faults: Mutex<Vec<IndexLookupTestFault>>,
    #[cfg(test)]
    fail_next_progress_sync: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    fail_next_gc_sync: std::sync::atomic::AtomicBool,
}

#[cfg(test)]
#[derive(Clone)]
enum IndexLookupTestFault {
    FailReadGroup(usize),
    PanicReadGroup(usize),
    DelayReadGroup(usize, Duration),
    BlockReadGroup(
        usize,
        std::sync::mpsc::Sender<()>,
        Arc<(Mutex<bool>, std::sync::Condvar)>,
    ),
    CorruptGroupBounds(usize),
    DropGroupResult(usize),
    DuplicateGroupResult(usize),
    GroupCollectedSignal(usize, std::sync::mpsc::Sender<usize>),
    FailSyncWrite,
}

#[cfg(not(test))]
#[derive(Clone)]
enum IndexLookupTestFault {}

enum CheckpointMessage {
    Snapshot(CheckpointSnapshot),
    Barrier(tokio::sync::oneshot::Sender<Result<(), String>>),
}

struct CheckpointSnapshot {
    seq: u64,
    balances: Vec<(u64, u64)>,
}

#[derive(Clone, Copy, Debug)]
struct Manifest {
    generation: u64,
    seq: u64,
    users: u64,
    chunks: u32,
    checksum: u64,
}

struct Recovered {
    balances: Vec<u64>,
    latest_seq: u64,
    manifest: Option<Manifest>,
    projected_seq: u64,
    gc_prefix_seq: u64,
}

struct BatchOutcome {
    replies: Vec<Reply>,
    balances: Vec<(u64, u64)>,
    latest_seq: u64,
    new_transactions: u64,
    read_build_ns: u64,
    wal_sync_ns: u64,
    index_metrics: IndexApplyMetrics,
}

#[derive(Clone, Debug, Default)]
struct IndexApplyMetrics {
    blocking_pool_wait_ns: u64,
    key_prep_ns: u64,
    native_get_ns: u64,
    decode_ns: u64,
    get_calls: u64,
    keys_looked_up: u64,
    hits: u64,
    misses: u64,
    sequential_apply_build_ns: u64,
    sync_write_batch_ns: u64,
}

#[derive(Default)]
struct IndexPrefetchMetrics {
    key_prep_ns: u64,
    query_wall_ns: u64,
    blocking_pool_wait_ns: u64,
    native_get_ns: u64,
    decode_ns: u64,
    submit_to_collection_ns: u64,
    groups_submitted: usize,
    max_observed_in_flight_groups: usize,
    max_observed_running_query_jobs: usize,
    groups: Vec<IndexLookupGroupMetrics>,
    get_calls: u64,
    keys_looked_up: u64,
    hits: u64,
    misses: u64,
}

#[derive(Clone, Debug)]
struct IndexLookupGroupCompletion {
    group_index: usize,
    first_position: usize,
    end_position: usize,
    entries: Vec<(usize, Option<StoredTransaction>)>,
    metrics: IndexLookupGroupMetrics,
}

#[derive(Default)]
struct QueryJobActivity {
    active: AtomicUsize,
    peak: AtomicUsize,
}

struct QueryJobActivityGuard(Arc<QueryJobActivity>);

impl QueryJobActivity {
    fn enter(self: &Arc<Self>) -> QueryJobActivityGuard {
        let active = self.active.fetch_add(1, Ordering::AcqRel) + 1;
        self.peak.fetch_max(active, Ordering::AcqRel);
        QueryJobActivityGuard(Arc::clone(self))
    }
}

impl Drop for QueryJobActivityGuard {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::AcqRel);
    }
}

const LEDGER_PREFIX: u8 = b'L';
const TRANSACTION_PREFIX: u8 = b'T';
const BALANCE_PREFIX: u8 = b'B';
const REFUND_PREFIX: u8 = b'R';
const CHECKPOINT_PREFIX: u8 = b'C';
const KEY_LATEST_SEQ: &[u8] = b"M:latest-seq";
const KEY_PROJECTED_BEFORE: &[u8] = b"M:projected-before";
const KEY_PROJECTED_SEQ: &[u8] = b"M:projected-seq";
const KEY_PROJECTED_BEFORE_SEQ: &[u8] = b"M:projected-before-seq";
const KEY_GC_PREFIX_SEQ: &[u8] = b"M:gc-prefix-seq";
const KEY_CHECKPOINT_MANIFEST: &[u8] = b"M:account-balance-checkpoint";
const CHECKPOINT_CHUNK_ACCOUNTS: usize = 1_024;
const CHECKPOINT_QUEUE_CAPACITY: usize = 2;
const LATENCY_SAMPLE_MASK: u64 = 0x3ff;

#[derive(Clone, Debug)]
pub struct RocksDbBudget {
    pub write_buffer_size: usize,
    pub max_write_buffer_number: i32,
    pub block_cache_bytes: usize,
    pub max_background_jobs: i32,
}

#[derive(Clone, Debug)]
struct Keyspace {
    prefix: Vec<u8>,
}

impl Keyspace {
    fn legacy() -> Self {
        Self { prefix: Vec::new() }
    }

    fn shard(shard_id: u32) -> Self {
        let mut prefix = Vec::with_capacity(5);
        prefix.push(b'S');
        prefix.extend_from_slice(&shard_id.to_be_bytes());
        Self { prefix }
    }

    fn key(&self, key: &[u8]) -> Vec<u8> {
        let mut scoped = Vec::with_capacity(self.prefix.len() + key.len());
        scoped.extend_from_slice(&self.prefix);
        scoped.extend_from_slice(key);
        scoped
    }

    fn starts_with(&self, key: &[u8], base_prefix: u8) -> bool {
        key.starts_with(&self.prefix) && key.get(self.prefix.len()) == Some(&base_prefix)
    }

    fn strip<'a>(&self, key: &'a [u8]) -> Option<&'a [u8]> {
        key.strip_prefix(self.prefix.as_slice())
    }

    fn latest_seq(&self) -> Vec<u8> {
        self.key(KEY_LATEST_SEQ)
    }

    fn checkpoint_manifest(&self) -> Vec<u8> {
        self.key(KEY_CHECKPOINT_MANIFEST)
    }

    fn transaction(&self, key: TransactionKey) -> Vec<u8> {
        self.key(&transaction_key(key))
    }

    fn ledger(&self, seq: u64) -> Vec<u8> {
        self.key(&ledger_key(seq))
    }

    fn balance(&self, account: u64) -> Vec<u8> {
        self.key(&balance_key(account))
    }

    fn refund(&self, key: TransactionKey) -> Vec<u8> {
        self.key(&refund_key(key))
    }

    fn checkpoint_chunk(&self, generation: u64, chunk: u32) -> Vec<u8> {
        self.key(&checkpoint_chunk_key(generation, chunk))
    }
}

/// Standalone handler/storage object. A caller should await each batch before
/// submitting its next batch, matching the benchmark's single-flight design.
#[derive(Clone)]
pub struct AccountStore {
    inner: Arc<Inner>,
}

impl AccountStore {
    pub async fn open(
        path: &Path,
        users: usize,
        mode: BalanceMode,
        checkpoint_quantity: u64,
    ) -> Result<Self, String> {
        if users == 0 {
            return Err("account store requires at least one user".to_owned());
        }
        let path = path.to_path_buf();
        let (db, options) = tokio::task::spawn_blocking(move || open_database_default(&path))
            .await
            .map_err(|error| format!("account-store open worker failed: {error}"))??;
        Self::open_with_keyspace(
            db,
            options,
            (0..users as u64).collect(),
            Keyspace::legacy(),
            mode,
            checkpoint_quantity,
        )
        .await
    }

    pub async fn open_on_database(
        db: Arc<DB>,
        options: Arc<Options>,
        account_ids: Vec<u64>,
        shard_id: u32,
        mode: BalanceMode,
        checkpoint_quantity: u64,
    ) -> Result<Self, String> {
        Self::open_with_keyspace(
            db,
            options,
            account_ids,
            Keyspace::shard(shard_id),
            mode,
            checkpoint_quantity,
        )
        .await
    }

    async fn open_with_keyspace(
        db: Arc<DB>,
        options: Arc<Options>,
        mut account_ids: Vec<u64>,
        keyspace: Keyspace,
        mode: BalanceMode,
        checkpoint_quantity: u64,
    ) -> Result<Self, String> {
        if account_ids.is_empty() {
            return Err("account store requires at least one account".to_owned());
        }
        if checkpoint_quantity == 0 {
            return Err("checkpoint quantity must be positive".to_owned());
        }
        account_ids.sort_unstable();
        if account_ids.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err("account IDs must be unique within a store".to_owned());
        }
        let recovery_db = Arc::clone(&db);
        let recovery_accounts = account_ids.clone();
        let recovery_keyspace = keyspace.clone();
        let recovered = tokio::task::spawn_blocking(move || {
            recover_database(&recovery_db, &recovery_accounts, &recovery_keyspace, mode)
        })
        .await
        .map_err(|error| format!("account-store recovery worker failed: {error}"))??;

        let failure = Arc::new(Mutex::new(None));
        let metrics = Arc::new(Mutex::new(MetricsSnapshot::default()));
        let (sender, worker) = if mode == BalanceMode::Checkpoint {
            let (sender, receiver) = mpsc::channel(CHECKPOINT_QUEUE_CAPACITY);
            let worker_db = Arc::clone(&db);
            let worker_failure = Arc::clone(&failure);
            let worker_metrics = Arc::clone(&metrics);
            let worker_keyspace = keyspace.clone();
            let previous = recovered.manifest;
            let worker = tokio::spawn(async move {
                checkpoint_worker(
                    worker_db,
                    worker_keyspace,
                    receiver,
                    previous,
                    worker_failure,
                    worker_metrics,
                )
                .await
            });
            (Some(sender), Some(worker))
        } else {
            (None, None)
        };
        let in_memory_balances = account_ids
            .iter()
            .copied()
            .zip(recovered.balances)
            .collect();
        Ok(Self {
            inner: Arc::new(Inner {
                db,
                options,
                keyspace,
                mode,
                account_ids,
                checkpoint_quantity,
                state: Mutex::new(State {
                    balances: in_memory_balances,
                    seq: recovered.latest_seq,
                }),
                batch_gate: AsyncMutex::new(()),
                checkpoint_tx: Mutex::new(sender),
                checkpoint_worker: Mutex::new(worker),
                checkpoint_failure: failure,
                metrics,
                refund_history: RwLock::new(None),
                projected_seq: std::sync::atomic::AtomicU64::new(recovered.projected_seq),
                gc_prefix_seq: std::sync::atomic::AtomicU64::new(recovered.gc_prefix_seq),
                #[cfg(test)]
                fail_next_progress_sync: std::sync::atomic::AtomicBool::new(false),
                #[cfg(test)]
                fail_next_gc_sync: std::sync::atomic::AtomicBool::new(false),
                #[cfg(test)]
                index_lookup_test_faults: Mutex::new(Vec::new()),
            }),
        })
    }

    pub async fn open_database_with_budget(
        path: &Path,
        budget: RocksDbBudget,
    ) -> Result<(Arc<DB>, Arc<Options>), String> {
        let path = path.to_path_buf();
        tokio::task::spawn_blocking(move || open_database_budgeted(&path, budget))
            .await
            .map_err(|error| format!("RocksDB open worker failed: {error}"))?
    }

    pub fn account_ids(&self) -> &[u64] {
        &self.inner.account_ids
    }

    pub fn namespace_id(&self) -> Option<u32> {
        if self.inner.keyspace.prefix.is_empty() {
            None
        } else {
            Some(u32::from_be_bytes(
                self.inner.keyspace.prefix[1..5]
                    .try_into()
                    .expect("shard namespace is five bytes"),
            ))
        }
    }

    pub async fn handle_batch(&self, transactions: Vec<Transaction>) -> Result<Vec<Reply>, String> {
        let handler_started = Instant::now();
        if transactions.is_empty() {
            return Ok(Vec::new());
        }
        let _batch_guard = self.inner.batch_gate.lock().await;
        self.check_checkpoint_failure()?;
        let tx_count = transactions.len() as u64;
        let sample_ids: Vec<_> = transactions
            .iter()
            .map(|transaction| (transaction.key.account_id, transaction.key.tx_id))
            .collect();
        let (starting_seq, starting_balances) = {
            let state = self
                .inner
                .state
                .lock()
                .map_err(|_| "account state mutex poisoned")?;
            let mut balances = HashMap::new();
            for transaction in &transactions {
                let account = transaction.key.account_id;
                if !state.balances.contains_key(&account) {
                    return Err(format!(
                        "account {account} is outside this store's configured account set"
                    ));
                }
                balances.entry(account).or_insert_with(|| {
                    *state
                        .balances
                        .get(&account)
                        .expect("all configured account balances are initialized")
                });
            }
            (state.seq, balances)
        };
        let db = Arc::clone(&self.inner.db);
        let mode = self.inner.mode;
        let keyspace = self.inner.keyspace.clone();
        let refund_history = self
            .inner
            .refund_history
            .read()
            .map_err(|_| "refund-history lock poisoned")?
            .clone();
        let gc_prefix_seq = self.gc_prefix_seq();
        let outcome = tokio::task::spawn_blocking(move || {
            process_batch(
                db,
                keyspace,
                transactions,
                starting_balances,
                starting_seq,
                mode,
                refund_history,
                gc_prefix_seq,
            )
        })
        .await
        .map_err(|error| format!("account batch worker failed: {error}"))??;

        let publish_started = Instant::now();
        let mut snapshots = Vec::new();
        let publish_ns;
        let mut checkpoint_snapshot_ns = 0_u64;
        {
            let mut state = self
                .inner
                .state
                .lock()
                .map_err(|_| "account state mutex poisoned")?;
            for (account, balance) in outcome.balances.iter().copied() {
                state.balances.insert(account, balance);
            }
            state.seq = outcome.latest_seq;
            publish_ns = nanos(publish_started.elapsed());
            if self.inner.mode == BalanceMode::Checkpoint && outcome.new_transactions > 0 {
                let before = starting_seq / self.inner.checkpoint_quantity;
                let after = state.seq / self.inner.checkpoint_quantity;
                if before < after {
                    let snapshot_started = Instant::now();
                    let mut balances: Vec<_> = state
                        .balances
                        .iter()
                        .map(|(account, balance)| (*account, *balance))
                        .collect();
                    balances.sort_unstable_by_key(|(account, _)| *account);
                    for _ in before..after {
                        snapshots.push(CheckpointSnapshot {
                            seq: state.seq,
                            balances: balances.clone(),
                        });
                    }
                    checkpoint_snapshot_ns += nanos(snapshot_started.elapsed());
                }
            }
        }
        {
            let mut metrics = self
                .inner
                .metrics
                .lock()
                .map_err(|_| "account metrics mutex poisoned")?;
            metrics.transactions += tx_count;
            metrics.batches += 1;
            metrics.read_build_ns += outcome.read_build_ns;
            metrics.wal_sync_ns += outcome.wal_sync_ns;
            metrics.publish_ns += publish_ns;
            metrics.checkpoint_snapshot_ns += checkpoint_snapshot_ns;
        }

        for snapshot in snapshots {
            let queued_at = Instant::now();
            let sender = self
                .inner
                .checkpoint_tx
                .lock()
                .map_err(|_| "checkpoint sender mutex poisoned")?
                .as_ref()
                .cloned()
                .ok_or_else(|| "checkpoint writer is not available".to_owned())?;
            sender
                .send(CheckpointMessage::Snapshot(snapshot))
                .await
                .map_err(|_| self.checkpoint_failure_message("checkpoint writer stopped"))?;
            let queue_wait_ns = nanos(queued_at.elapsed());
            let mut metrics = self
                .inner
                .metrics
                .lock()
                .map_err(|_| "account metrics mutex poisoned")?;
            metrics.checkpoint_queue_wait_ns += queue_wait_ns;
            metrics.checkpoint_snapshots_enqueued += 1;
        }
        self.check_checkpoint_failure()?;
        let handler_latency_ns = nanos(handler_started.elapsed());
        let mut metrics = self
            .inner
            .metrics
            .lock()
            .map_err(|_| "account metrics mutex poisoned")?;
        metrics.transaction_latency_ns = metrics
            .transaction_latency_ns
            .saturating_add(handler_latency_ns.saturating_mul(tx_count));
        metrics
            .transaction_samples_ns
            .extend(sample_ids.into_iter().filter_map(|(account, tx_id)| {
                stable_sample(account, tx_id).then_some(handler_latency_ns)
            }));
        Ok(outcome.replies)
    }

    /// Run a batch with the explicitly selected transaction-index strategy.
    /// The coordinator owns the store and batch after it is spawned, so
    /// dropping this caller future cannot release the batch gate while a
    /// blocking RocksDB query or sync write is still running.
    pub async fn handle_batch_with_index_lookup(
        &self,
        transactions: Vec<Transaction>,
        config: IndexLookupConfig,
    ) -> Result<IndexLookupBatchResult, String> {
        let store = self.clone();
        let submitted_at = Instant::now();
        tokio::spawn(async move {
            store
                .handle_batch_with_index_lookup_coordinator(transactions, config, submitted_at)
                .await
        })
        .await
        .map_err(|error| format!("index-lookup batch coordinator failed: {error}"))?
    }

    async fn handle_batch_with_index_lookup_coordinator(
        &self,
        transactions: Vec<Transaction>,
        config: IndexLookupConfig,
        submitted_at: Instant,
    ) -> Result<IndexLookupBatchResult, String> {
        let dispatch_wait_ns = nanos(submitted_at.elapsed());
        if transactions.is_empty() {
            return Ok(IndexLookupBatchResult {
                replies: Vec::new(),
                metrics: IndexLookupBatchMetrics {
                    mode: config.mode,
                    transaction_count: 0,
                    dispatch_wait_ns,
                    batch_gate_wait_ns: 0,
                    key_prep_ns: 0,
                    query_wall_ns: match config.mode {
                        IndexLookupMode::PointGet => None,
                        IndexLookupMode::WholeBatchMultiGet | IndexLookupMode::Chunked { .. } => {
                            Some(0)
                        }
                    },
                    blocking_pool_wait_ns: None,
                    native_get_ns: 0,
                    decode_ns: 0,
                    sequential_apply_build_ns: 0,
                    sync_write_batch_ns: 0,
                    memory_publish_ns: 0,
                    get_calls: 0,
                    keys_looked_up: 0,
                    hits: 0,
                    misses: 0,
                    submit_to_collection_ns: None,
                    apply_submit_to_collection_ns: 0,
                    apply_blocking_pool_wait_ns: 0,
                    groups_submitted: 0,
                    max_observed_in_flight_groups: 0,
                    max_observed_running_query_jobs: 0,
                    groups: Vec::new(),
                },
            });
        }

        let batch_gate_wait_started = Instant::now();
        let _batch_guard = self.inner.batch_gate.lock().await;
        let batch_gate_wait_ns = nanos(batch_gate_wait_started.elapsed());
        self.check_checkpoint_failure()?;
        let tx_count = transactions.len() as u64;
        let sample_ids: Vec<_> = transactions
            .iter()
            .map(|transaction| (transaction.key.account_id, transaction.key.tx_id))
            .collect();
        let (starting_seq, starting_balances) = {
            let state = self
                .inner
                .state
                .lock()
                .map_err(|_| "account state mutex poisoned")?;
            let mut balances = HashMap::new();
            for transaction in &transactions {
                let account = transaction.key.account_id;
                if !state.balances.contains_key(&account) {
                    return Err(format!(
                        "account {account} is outside this store's configured account set"
                    ));
                }
                balances.entry(account).or_insert_with(|| {
                    *state
                        .balances
                        .get(&account)
                        .expect("all configured account balances are initialized")
                });
            }
            (state.seq, balances)
        };

        #[cfg(test)]
        let test_faults = self.take_index_lookup_test_faults()?;
        #[cfg(not(test))]
        let test_faults = Vec::new();

        let db = Arc::clone(&self.inner.db);
        let mode = self.inner.mode;
        let keyspace = self.inner.keyspace.clone();
        let refund_history = self
            .inner
            .refund_history
            .read()
            .map_err(|_| "refund-history lock poisoned")?
            .clone();
        let gc_prefix_seq = self.gc_prefix_seq();
        let (prefetched, mut prefetch_metrics, query_wall_ns) = match config.mode {
            IndexLookupMode::PointGet => (None, IndexPrefetchMetrics::default(), None),
            IndexLookupMode::WholeBatchMultiGet | IndexLookupMode::Chunked { .. } => {
                let query_started = Instant::now();
                let (records, metrics) = prefetch_transaction_indexes(
                    Arc::clone(&db),
                    keyspace.clone(),
                    &transactions,
                    config,
                    &test_faults,
                )
                .await?;
                let wall_ns = nanos(query_started.elapsed());
                let mut metrics = metrics;
                metrics.query_wall_ns = wall_ns;
                (Some(records), metrics, Some(wall_ns))
            }
        };

        let process_submitted_at = Instant::now();
        let apply_started = Instant::now();
        let outcome = tokio::task::spawn_blocking(move || {
            process_batch_with_index_lookup(
                db,
                keyspace,
                transactions,
                starting_balances,
                starting_seq,
                mode,
                refund_history,
                gc_prefix_seq,
                prefetched,
                process_submitted_at,
                test_faults,
            )
        })
        .await
        .map_err(|error| format!("index-lookup account batch worker failed: {error}"))??;
        let apply_submit_to_collection_ns = nanos(apply_started.elapsed());
        if config.mode == IndexLookupMode::PointGet {
            prefetch_metrics.blocking_pool_wait_ns = outcome.index_metrics.blocking_pool_wait_ns;
            prefetch_metrics.key_prep_ns = outcome.index_metrics.key_prep_ns;
            prefetch_metrics.native_get_ns = outcome.index_metrics.native_get_ns;
            prefetch_metrics.decode_ns = outcome.index_metrics.decode_ns;
            prefetch_metrics.get_calls = outcome.index_metrics.get_calls;
            prefetch_metrics.keys_looked_up = outcome.index_metrics.keys_looked_up;
            prefetch_metrics.hits = outcome.index_metrics.hits;
            prefetch_metrics.misses = outcome.index_metrics.misses;
        }

        let publish_started = Instant::now();
        let mut snapshots = Vec::new();
        let mut checkpoint_snapshot_ns = 0_u64;
        {
            let mut state = self
                .inner
                .state
                .lock()
                .map_err(|_| "account state mutex poisoned")?;
            for (account, balance) in outcome.balances.iter().copied() {
                state.balances.insert(account, balance);
            }
            state.seq = outcome.latest_seq;
            if self.inner.mode == BalanceMode::Checkpoint && outcome.new_transactions > 0 {
                let before = starting_seq / self.inner.checkpoint_quantity;
                let after = state.seq / self.inner.checkpoint_quantity;
                if before < after {
                    let snapshot_started = Instant::now();
                    let mut balances: Vec<_> = state
                        .balances
                        .iter()
                        .map(|(account, balance)| (*account, *balance))
                        .collect();
                    balances.sort_unstable_by_key(|(account, _)| *account);
                    for _ in before..after {
                        snapshots.push(CheckpointSnapshot {
                            seq: state.seq,
                            balances: balances.clone(),
                        });
                    }
                    checkpoint_snapshot_ns += nanos(snapshot_started.elapsed());
                }
            }
        }
        let memory_publish_ns = nanos(publish_started.elapsed());
        {
            let mut metrics = self
                .inner
                .metrics
                .lock()
                .map_err(|_| "account metrics mutex poisoned")?;
            metrics.transactions += tx_count;
            metrics.batches += 1;
            metrics.read_build_ns += outcome.read_build_ns;
            metrics.wal_sync_ns += outcome.wal_sync_ns;
            metrics.publish_ns += memory_publish_ns;
            metrics.checkpoint_snapshot_ns += checkpoint_snapshot_ns;
        }

        for snapshot in snapshots {
            let queued_at = Instant::now();
            let sender = self
                .inner
                .checkpoint_tx
                .lock()
                .map_err(|_| "checkpoint sender mutex poisoned")?
                .as_ref()
                .cloned()
                .ok_or_else(|| "checkpoint writer is not available".to_owned())?;
            sender
                .send(CheckpointMessage::Snapshot(snapshot))
                .await
                .map_err(|_| self.checkpoint_failure_message("checkpoint writer stopped"))?;
            let queue_wait_ns = nanos(queued_at.elapsed());
            let mut metrics = self
                .inner
                .metrics
                .lock()
                .map_err(|_| "account metrics mutex poisoned")?;
            metrics.checkpoint_queue_wait_ns += queue_wait_ns;
            metrics.checkpoint_snapshots_enqueued += 1;
        }
        self.check_checkpoint_failure()?;
        let handler_latency_ns = nanos(submitted_at.elapsed());
        let mut metrics = self
            .inner
            .metrics
            .lock()
            .map_err(|_| "account metrics mutex poisoned")?;
        metrics.transaction_latency_ns = metrics
            .transaction_latency_ns
            .saturating_add(handler_latency_ns.saturating_mul(tx_count));
        metrics
            .transaction_samples_ns
            .extend(sample_ids.into_iter().filter_map(|(account, tx_id)| {
                stable_sample(account, tx_id).then_some(handler_latency_ns)
            }));

        let index_metrics = outcome.index_metrics;
        Ok(IndexLookupBatchResult {
            replies: outcome.replies,
            metrics: IndexLookupBatchMetrics {
                mode: config.mode,
                transaction_count: tx_count as usize,
                dispatch_wait_ns,
                batch_gate_wait_ns,
                key_prep_ns: prefetch_metrics.key_prep_ns,
                query_wall_ns,
                blocking_pool_wait_ns: (config.mode != IndexLookupMode::PointGet)
                    .then_some(prefetch_metrics.blocking_pool_wait_ns),
                native_get_ns: prefetch_metrics.native_get_ns,
                decode_ns: prefetch_metrics.decode_ns,
                submit_to_collection_ns: (config.mode != IndexLookupMode::PointGet)
                    .then_some(prefetch_metrics.submit_to_collection_ns),
                apply_submit_to_collection_ns,
                apply_blocking_pool_wait_ns: index_metrics.blocking_pool_wait_ns,
                sequential_apply_build_ns: index_metrics.sequential_apply_build_ns,
                sync_write_batch_ns: index_metrics.sync_write_batch_ns,
                memory_publish_ns,
                get_calls: prefetch_metrics.get_calls,
                keys_looked_up: prefetch_metrics.keys_looked_up,
                hits: prefetch_metrics.hits,
                misses: prefetch_metrics.misses,
                groups_submitted: prefetch_metrics.groups_submitted,
                max_observed_in_flight_groups: prefetch_metrics.max_observed_in_flight_groups,
                max_observed_running_query_jobs: prefetch_metrics.max_observed_running_query_jobs,
                groups: prefetch_metrics.groups,
            },
        })
    }

    pub fn balance(&self, account_id: u64) -> Result<u64, String> {
        self.balance_for_request(account_id, account_id)
    }

    pub fn balance_for_request(&self, account_id: u64, request_id: u64) -> Result<u64, String> {
        let started = Instant::now();
        let balance = self
            .inner
            .state
            .lock()
            .map_err(|_| "account state mutex poisoned")?
            .balances
            .get(&account_id)
            .copied()
            .ok_or_else(|| format!("unknown account {account_id}"))?;
        let elapsed = nanos(started.elapsed());
        let mut metrics = self
            .inner
            .metrics
            .lock()
            .map_err(|_| "account metrics mutex poisoned")?;
        metrics.balance_queries += 1;
        metrics.balance_latency_ns += elapsed;
        if stable_sample(account_id, request_id) {
            metrics.balance_samples_ns.push(elapsed);
        }
        Ok(balance)
    }

    pub fn latest_seq(&self) -> u64 {
        self.inner.state.lock().expect("state mutex poisoned").seq
    }

    pub fn gc_prefix_seq(&self) -> u64 {
        self.inner
            .gc_prefix_seq
            .load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn set_refund_history(&self, history: Arc<dyn RefundHistory>) -> Result<(), String> {
        *self
            .inner
            .refund_history
            .write()
            .map_err(|_| "refund-history lock poisoned".to_owned())? = Some(history);
        Ok(())
    }

    /// Persist successful destination application before publishing progress
    /// to another worker. The destination apply must be idempotent because a
    /// process can stop after apply but before this source-side WAL sync.
    pub async fn persist_projection_progress(&self, sequence: u64) -> Result<(), String> {
        let db = Arc::clone(&self.inner.db);
        let keyspace = self.inner.keyspace.clone();
        #[cfg(test)]
        let fail = self
            .inner
            .fail_next_progress_sync
            .swap(false, std::sync::atomic::Ordering::AcqRel);
        #[cfg(not(test))]
        let fail = false;
        let started = Instant::now();
        tokio::task::spawn_blocking(move || {
            if fail {
                return Err("injected projected-progress sync failure".to_owned());
            }
            let latest = db
                .get(keyspace.latest_seq())
                .map_err(db_error("read latest sequence for projection progress"))?
                .ok_or_else(|| "latest sequence metadata is missing".to_owned())
                .and_then(|bytes| decode_u64(&bytes, "latest sequence"))?;
            if sequence > latest {
                return Err(format!(
                    "projected sequence {sequence} is ahead of latest sequence {latest}"
                ));
            }
            let key = keyspace.key(KEY_PROJECTED_SEQ);
            let previous = db
                .get(&key)
                .map_err(db_error("read durable projected sequence"))?
                .map(|bytes| decode_u64(&bytes, "projected sequence"))
                .transpose()?
                .unwrap_or(0);
            if sequence < previous {
                return Err(format!(
                    "projected sequence cannot move backwards from {previous} to {sequence}"
                ));
            }
            if sequence == previous {
                return Ok(());
            }
            let mut batch = WriteBatch::default();
            batch.put(key, sequence.to_be_bytes());
            db.write_opt(batch, &sync_write_options())
                .map_err(db_error("synchronously persist projected sequence"))
        })
        .await
        .map_err(|error| format!("projected-progress worker failed: {error}"))??;
        self.inner
            .projected_seq
            .store(sequence, std::sync::atomic::Ordering::Release);
        self.inner
            .metrics
            .lock()
            .map_err(|_| "account metrics mutex poisoned")?
            .projection_progress_sync_ns += nanos(started.elapsed());
        Ok(())
    }

    pub async fn durable_projection_progress(&self) -> Result<u64, String> {
        let db = Arc::clone(&self.inner.db);
        let key = self.inner.keyspace.key(KEY_PROJECTED_SEQ);
        tokio::task::spawn_blocking(move || {
            db.get(key)
                .map_err(db_error("read durable projected sequence"))?
                .map(|bytes| decode_u64(&bytes, "projected sequence"))
                .transpose()
                .map(|sequence| sequence.unwrap_or(0))
        })
        .await
        .map_err(|error| format!("projected-progress read worker failed: {error}"))?
    }

    pub fn projected_seq(&self) -> u64 {
        self.inner
            .projected_seq
            .load(std::sync::atomic::Ordering::Acquire)
    }

    #[cfg(test)]
    pub fn fail_next_projection_progress_sync_for_test(&self) {
        self.inner
            .fail_next_progress_sync
            .store(true, std::sync::atomic::Ordering::Release);
    }

    #[cfg(test)]
    pub fn fail_next_gc_sync_for_test(&self) {
        self.inner
            .fail_next_gc_sync
            .store(true, std::sync::atomic::Ordering::Release);
    }

    #[cfg(test)]
    fn push_index_lookup_test_fault(&self, fault: IndexLookupTestFault) {
        self.inner
            .index_lookup_test_faults
            .lock()
            .expect("index-lookup test-fault mutex poisoned")
            .push(fault);
    }

    #[cfg(test)]
    pub fn block_index_lookup_group_for_test(
        &self,
        group_index: usize,
    ) -> (
        std::sync::mpsc::Receiver<()>,
        Arc<(Mutex<bool>, std::sync::Condvar)>,
    ) {
        let (started, started_receiver) = std::sync::mpsc::channel();
        let release = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        self.push_index_lookup_test_fault(IndexLookupTestFault::BlockReadGroup(
            group_index,
            started,
            Arc::clone(&release),
        ));
        (started_receiver, release)
    }

    #[cfg(test)]
    pub fn fail_index_lookup_group_for_test(&self, group_index: usize) {
        self.push_index_lookup_test_fault(IndexLookupTestFault::FailReadGroup(group_index));
    }

    #[cfg(test)]
    pub fn panic_index_lookup_group_for_test(&self, group_index: usize) {
        self.push_index_lookup_test_fault(IndexLookupTestFault::PanicReadGroup(group_index));
    }

    #[cfg(test)]
    pub fn fail_next_index_lookup_write_for_test(&self) {
        self.push_index_lookup_test_fault(IndexLookupTestFault::FailSyncWrite);
    }

    #[cfg(test)]
    fn take_index_lookup_test_faults(&self) -> Result<Vec<IndexLookupTestFault>, String> {
        let mut faults = self
            .inner
            .index_lookup_test_faults
            .lock()
            .map_err(|_| "index-lookup test-fault mutex poisoned")?;
        Ok(std::mem::take(&mut *faults))
    }

    #[cfg(test)]
    pub async fn delete_ledger_sequence_for_test(&self, sequence: u64) -> Result<(), String> {
        let db = Arc::clone(&self.inner.db);
        let key = self.inner.keyspace.ledger(sequence);
        tokio::task::spawn_blocking(move || {
            db.delete(key)
                .map_err(db_error("inject missing ledger record"))
        })
        .await
        .map_err(|error| format!("test corruption worker failed: {error}"))?
    }

    /// Atomically persist the routing boundary and the sequence drained before
    /// it. The durable projection watermark must already cover that sequence.
    pub async fn persist_projected_before_durable(
        &self,
        timestamp: u64,
        target_sequence: u64,
    ) -> Result<(), String> {
        let db = Arc::clone(&self.inner.db);
        let keyspace = self.inner.keyspace.clone();
        tokio::task::spawn_blocking(move || {
            let progress = db
                .get(keyspace.key(KEY_PROJECTED_SEQ))
                .map_err(db_error("read projection progress before publishing boundary"))?
                .map(|bytes| decode_u64(&bytes, "projected sequence"))
                .transpose()?
                .unwrap_or(0);
            if target_sequence > progress {
                return Err(format!(
                    "cannot publish boundary through sequence {target_sequence}; durable projection is {progress}"
                ));
            }
            let boundary_key = keyspace.key(KEY_PROJECTED_BEFORE);
            let sequence_key = keyspace.key(KEY_PROJECTED_BEFORE_SEQ);
            let previous_boundary = db
                .get(&boundary_key)
                .map_err(db_error("read previous projected-before boundary"))?
                .map(|bytes| decode_u64(&bytes, "projected-before boundary"))
                .transpose()?;
            let previous_sequence = db
                .get(&sequence_key)
                .map_err(db_error("read previous boundary sequence"))?
                .map(|bytes| decode_u64(&bytes, "projected-before sequence"))
                .transpose()?;
            match (previous_boundary, previous_sequence) {
                (Some(previous_boundary), Some(previous_sequence)) => {
                    if timestamp < previous_boundary || target_sequence < previous_sequence {
                        return Err(format!(
                            "projected-before metadata cannot move backwards from ({previous_boundary}, {previous_sequence}) to ({timestamp}, {target_sequence})"
                        ));
                    }
                }
                (Some(previous_boundary), None) if timestamp < previous_boundary => {
                    return Err(format!(
                        "projected-before boundary cannot move backwards from {previous_boundary} to {timestamp}"
                    ));
                }
                (None, Some(_)) => {
                    return Err("projected-before boundary metadata is incomplete".to_owned());
                }
                _ => {}
            }
            let mut batch = WriteBatch::default();
            batch.put(boundary_key, timestamp.to_be_bytes());
            batch.put(sequence_key, target_sequence.to_be_bytes());
            db.write_opt(batch, &sync_write_options())
                .map_err(db_error("synchronously publish projected-before boundary"))
        })
        .await
        .map_err(|error| format!("projected-before metadata worker failed: {error}"))?
    }

    /// Restore a boundary only when its atomic publication metadata is intact
    /// and durable projection progress covers the sequence recorded with it.
    pub async fn restored_projected_before(&self) -> Result<Option<(u64, u64)>, String> {
        self.verify_projection_destination().await?;
        let db = Arc::clone(&self.inner.db);
        let keyspace = self.inner.keyspace.clone();
        tokio::task::spawn_blocking(move || {
            let boundary = db
                .get(keyspace.key(KEY_PROJECTED_BEFORE))
                .map_err(db_error("read persisted projected-before boundary"))?
                .map(|bytes| decode_u64(&bytes, "projected-before boundary"))
                .transpose()?;
            let sequence = db
                .get(keyspace.key(KEY_PROJECTED_BEFORE_SEQ))
                .map_err(db_error("read persisted boundary sequence"))?
                .map(|bytes| decode_u64(&bytes, "projected-before sequence"))
                .transpose()?;
            let (Some(boundary), Some(sequence)) = (boundary, sequence) else {
                // A legacy timestamp-only boundary cannot prove the projection
                // state needed for routing or deletion after restart.
                return Ok(None);
            };
            let progress = db
                .get(keyspace.key(KEY_PROJECTED_SEQ))
                .map_err(db_error("read projected progress for restored boundary"))?
                .map(|bytes| decode_u64(&bytes, "projected sequence"))
                .transpose()?
                .unwrap_or(0);
            if progress < sequence {
                return Err(format!(
                    "persisted boundary sequence {sequence} exceeds durable projection progress {progress}"
                ));
            }
            Ok(Some((boundary, sequence)))
        })
        .await
        .map_err(|error| format!("projected-before recovery worker failed: {error}"))?
    }

    /// Confirm the retained historical destination covers source-side durable
    /// progress before a caller resumes routing or GC after source reopen.
    pub async fn verify_projection_destination(&self) -> Result<u64, String> {
        let source_progress = self.durable_projection_progress().await?;
        let has_boundary = self.restored_boundary_metadata_exists().await?;
        let history = self
            .inner
            .refund_history
            .read()
            .map_err(|_| "refund-history lock poisoned")?
            .clone();
        let Some(history) = history else {
            if source_progress > 0 || has_boundary {
                return Err(format!(
                    "source has durable projection progress {source_progress}, but historical destination is not restored"
                ));
            }
            return Ok(0);
        };
        let destination_progress = history.projection_progress()?;
        if destination_progress < source_progress {
            return Err(format!(
                "historical destination progress {destination_progress} is behind durable source projection {source_progress}"
            ));
        }
        Ok(destination_progress)
    }

    async fn restored_boundary_metadata_exists(&self) -> Result<bool, String> {
        let db = Arc::clone(&self.inner.db);
        let key = self.inner.keyspace.key(KEY_PROJECTED_BEFORE);
        tokio::task::spawn_blocking(move || {
            db.get(key)
                .map(|value| value.is_some())
                .map_err(db_error("check persisted boundary metadata"))
        })
        .await
        .map_err(|error| format!("boundary metadata check worker failed: {error}"))?
    }

    /// Delete a bounded contiguous ledger prefix whose every record is older
    /// than the durable boundary and covered by both durable progress marks.
    /// Deletes and the new prefix watermark share one synchronous WriteBatch.
    pub async fn collect_garbage(&self, max_records: usize) -> Result<GcStepOutcome, String> {
        if max_records == 0 {
            return Err("GC batch size must be positive".to_owned());
        }
        self.drain_checkpoints().await?;
        let _gate = self.inner.batch_gate.lock().await;
        self.check_checkpoint_failure()?;
        if self
            .inner
            .refund_history
            .read()
            .map_err(|_| "refund-history lock poisoned")?
            .is_none()
        {
            return Err("safe GC requires an installed historical refund lookup".to_owned());
        }
        self.verify_projection_destination().await?;
        let db = Arc::clone(&self.inner.db);
        let keyspace = self.inner.keyspace.clone();
        let account_count = self.inner.account_ids.len();
        let mode = self.inner.mode;
        #[cfg(test)]
        let fail = self
            .inner
            .fail_next_gc_sync
            .swap(false, std::sync::atomic::Ordering::AcqRel);
        #[cfg(not(test))]
        let fail = false;
        let outcome = tokio::task::spawn_blocking(move || {
            collect_garbage_sync(db, keyspace, account_count, mode, max_records, fail)
        })
        .await
        .map_err(|error| format!("GC worker failed: {error}"))??;
        self.inner
            .gc_prefix_seq
            .store(outcome.gc_prefix_seq, std::sync::atomic::Ordering::Release);
        let mut metrics = self
            .inner
            .metrics
            .lock()
            .map_err(|_| "account metrics mutex poisoned")?;
        metrics.gc_scan_ns += outcome.scan_ns;
        metrics.gc_delete_ns += outcome.delete_ns;
        metrics.gc_write_ns += outcome.write_ns;
        metrics.gc_records_scanned += outcome.scanned;
        metrics.gc_records_deleted += outcome.deleted;
        metrics.gc_bytes_deleted += outcome.bytes_deleted;
        Ok(outcome)
    }

    pub async fn has_ledger_sequence(&self, sequence: u64) -> Result<bool, String> {
        let db = Arc::clone(&self.inner.db);
        let key = self.inner.keyspace.ledger(sequence);
        tokio::task::spawn_blocking(move || {
            db.get(key)
                .map(|value| value.is_some())
                .map_err(db_error("check ledger sequence"))
        })
        .await
        .map_err(|error| format!("ledger existence worker failed: {error}"))?
    }

    pub async fn has_transaction_index(&self, key: TransactionKey) -> Result<bool, String> {
        let db = Arc::clone(&self.inner.db);
        let key = self.inner.keyspace.transaction(key);
        tokio::task::spawn_blocking(move || {
            db.get(key)
                .map(|value| value.is_some())
                .map_err(db_error("check transaction index"))
        })
        .await
        .map_err(|error| format!("transaction-index existence worker failed: {error}"))?
    }

    pub async fn refund_marker_exists(&self, key: TransactionKey) -> Result<bool, String> {
        let db = Arc::clone(&self.inner.db);
        let key = self.inner.keyspace.refund(key);
        tokio::task::spawn_blocking(move || {
            db.get(key)
                .map(|value| value.is_some())
                .map_err(db_error("check refund marker"))
        })
        .await
        .map_err(|error| format!("refund-marker existence worker failed: {error}"))?
    }

    /// Persist the transaction-time boundary only after the caller has
    /// confirmed that every committed sequence through its target is
    /// projected. The key uses the same single-shard namespace as the ledger,
    /// and RocksDB's WAL is synchronously written before this method returns.
    ///
    /// This benchmark's historical destination is in memory, so callers must
    /// not restore this value as an active boundary after a process restart.
    pub async fn persist_projected_before(&self, timestamp: u64) -> Result<(), String> {
        let db = Arc::clone(&self.inner.db);
        let key = self.inner.keyspace.key(KEY_PROJECTED_BEFORE);
        tokio::task::spawn_blocking(move || {
            let mut batch = WriteBatch::default();
            batch.put(key, timestamp.to_be_bytes());
            db.write_opt(batch, &sync_write_options())
                .map_err(db_error("synchronously persist projected-before boundary"))
        })
        .await
        .map_err(|error| format!("projected-before metadata worker failed: {error}"))?
    }

    /// Read the persisted boundary for verification and reporting. It is not
    /// used to initialize request routing because projection data is volatile.
    pub async fn persisted_projected_before(&self) -> Result<Option<u64>, String> {
        let db = Arc::clone(&self.inner.db);
        let key = self.inner.keyspace.key(KEY_PROJECTED_BEFORE);
        tokio::task::spawn_blocking(move || {
            db.get(key)
                .map_err(db_error("read persisted projected-before boundary"))?
                .map(|bytes| decode_u64(&bytes, "projected-before boundary"))
                .transpose()
        })
        .await
        .map_err(|error| format!("projected-before metadata read worker failed: {error}"))?
    }

    /// Read up to `max_records` committed records beginning at `first_seq`.
    ///
    /// The returned records are always contiguous and ordered. A missing,
    /// malformed, or sequence-mismatched record is an error. RocksDB calls and
    /// decoding run on Tokio's blocking pool so this method does not block a
    /// runtime worker thread.
    pub async fn read_ledger_range(
        &self,
        first_seq: u64,
        max_records: usize,
    ) -> Result<LedgerRangeRead, String> {
        if first_seq == 0 {
            return Err("ledger sequence numbers begin at one".to_owned());
        }
        if max_records == 0 {
            return Ok(LedgerRangeRead {
                records: Vec::new(),
                db_read_ns: 0,
            });
        }
        let db = Arc::clone(&self.inner.db);
        let keyspace = self.inner.keyspace.clone();
        tokio::task::spawn_blocking(move || {
            let started = Instant::now();
            let latest_seq = db
                .get(keyspace.latest_seq())
                .map_err(db_error("read latest sequence for ledger range"))?
                .ok_or_else(|| "latest sequence metadata is missing".to_owned())
                .and_then(|bytes| decode_u64(&bytes, "latest sequence"))?;
            let record_count = if first_seq > latest_seq {
                0
            } else {
                latest_seq
                    .checked_sub(first_seq)
                    .and_then(|distance| distance.checked_add(1))
                    .ok_or_else(|| "ledger range length overflow".to_owned())?
                    .min(u64::try_from(max_records).unwrap_or(u64::MAX))
            };
            let count = usize::try_from(record_count)
                .map_err(|_| "ledger range length does not fit memory address space".to_owned())?;
            let sequences = (0..count)
                .map(|offset| {
                    first_seq
                        .checked_add(offset as u64)
                        .ok_or_else(|| "ledger range sequence overflow".to_owned())
                })
                .collect::<Result<Vec<_>, _>>()?;
            let keys: Vec<_> = sequences
                .iter()
                .copied()
                .map(|seq| keyspace.ledger(seq))
                .collect();
            let values = db.multi_get(keys.iter());
            let mut records = Vec::with_capacity(count);
            for (seq, value) in sequences.into_iter().zip(values) {
                let bytes = value
                    .map_err(db_error("read ledger range"))?
                    .ok_or_else(|| format!("ledger sequence {seq} is missing"))?;
                let stored = decode_transaction(&bytes)
                    .map_err(|error| format!("ledger sequence {seq} is corrupt: {error}"))?;
                if stored.result.seq != seq {
                    return Err(format!(
                        "ledger key sequence {seq} contains sequence {}",
                        stored.result.seq
                    ));
                }
                records.push(LedgerRecord {
                    request: stored.request,
                    result: stored.result,
                });
            }
            Ok(LedgerRangeRead {
                records,
                db_read_ns: nanos(started.elapsed()),
            })
        })
        .await
        .map_err(|error| format!("ledger range worker failed: {error}"))?
    }

    pub fn all_balances(&self) -> Vec<(u64, u64)> {
        let state = self.inner.state.lock().expect("state mutex poisoned");
        let mut balances: Vec<_> = state.balances.iter().map(|(a, b)| (*a, *b)).collect();
        balances.sort_unstable_by_key(|(account, _)| *account);
        balances
    }

    pub fn metrics(&self) -> MetricsSnapshot {
        self.inner
            .metrics
            .lock()
            .expect("account metrics mutex poisoned")
            .clone()
    }

    pub fn rocksdb_stats(&self) -> (u64, u64, u64, u64, u64, u64, u64) {
        (
            self.inner.options.get_ticker_count(Ticker::WalFileSynced),
            self.inner.options.get_ticker_count(Ticker::WalFileBytes),
            self.inner.options.get_ticker_count(Ticker::WriteWithWal),
            self.inner.options.get_ticker_count(Ticker::FlushWriteBytes),
            self.inner
                .options
                .get_ticker_count(Ticker::CompactReadBytes),
            self.inner
                .options
                .get_ticker_count(Ticker::CompactWriteBytes),
            self.inner.options.get_ticker_count(Ticker::StallMicros),
        )
    }

    /// Scan the complete durable transaction index and ledger. The benchmark
    /// runs this after measuring close/reopen recovery so startup timings stay
    /// separate from post-run integrity validation.
    pub async fn validate_integrity(&self) -> Result<(), String> {
        let db = Arc::clone(&self.inner.db);
        let seq = self.latest_seq();
        let gc_prefix_seq = self.gc_prefix_seq();
        let keyspace = self.inner.keyspace.clone();
        tokio::task::spawn_blocking(move || {
            validate_index_and_ledger(&db, seq, gc_prefix_seq, &keyspace)
        })
        .await
        .map_err(|error| format!("integrity scan worker failed: {error}"))?
    }

    pub async fn shutdown(self) -> Result<(), String> {
        let drain_result = self.drain_checkpoints().await;
        if let Some(sender) = self
            .inner
            .checkpoint_tx
            .lock()
            .map_err(|_| "checkpoint sender mutex poisoned")?
            .take()
        {
            drop(sender);
        }
        let worker = self
            .inner
            .checkpoint_worker
            .lock()
            .map_err(|_| "checkpoint worker mutex poisoned")?
            .take();
        let worker_result = if let Some(worker) = worker {
            worker
                .await
                .map_err(|error| format!("checkpoint worker join failed: {error}"))?
        } else {
            Ok(())
        };
        drain_result?;
        worker_result
    }

    fn check_checkpoint_failure(&self) -> Result<(), String> {
        match self.inner.checkpoint_failure.lock() {
            Ok(failure) => failure.clone().map_or(Ok(()), Err),
            Err(_) => Err("checkpoint failure mutex poisoned".to_owned()),
        }
    }

    fn checkpoint_failure_message(&self, fallback: &str) -> String {
        self.inner
            .checkpoint_failure
            .lock()
            .ok()
            .and_then(|failure| failure.clone())
            .unwrap_or_else(|| fallback.to_owned())
    }
}

impl AccountStore {
    pub async fn drain_checkpoints(&self) -> Result<(), String> {
        let sender = self
            .inner
            .checkpoint_tx
            .lock()
            .map_err(|_| "checkpoint sender mutex poisoned")?
            .as_ref()
            .cloned();
        let Some(sender) = sender else {
            return self.check_checkpoint_failure();
        };
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        sender
            .send(CheckpointMessage::Barrier(reply_tx))
            .await
            .map_err(|_| self.checkpoint_failure_message("checkpoint writer stopped"))?;
        reply_rx
            .await
            .map_err(|_| self.checkpoint_failure_message("checkpoint barrier was dropped"))??;
        self.check_checkpoint_failure()
    }
}

fn open_database_default(path: &Path) -> Result<(Arc<DB>, Arc<Options>), String> {
    let mut options = Options::default();
    options.create_if_missing(true);
    options.enable_statistics();
    options.set_statistics_level(StatsLevel::ExceptDetailedTimers);
    let db = Arc::new(
        DB::open_cf_with_opts(
            &options,
            path,
            [(DEFAULT_COLUMN_FAMILY_NAME, options.clone())],
        )
        .map_err(|error| format!("cannot open RocksDB {}: {error}", path.display()))?,
    );
    Ok((db, Arc::new(options)))
}

fn open_database_budgeted(
    path: &Path,
    budget: RocksDbBudget,
) -> Result<(Arc<DB>, Arc<Options>), String> {
    if budget.write_buffer_size == 0
        || budget.max_write_buffer_number < 2
        || budget.block_cache_bytes == 0
        || budget.max_background_jobs < 1
    {
        return Err(
            "RocksDB budget values must be positive and include at least two write buffers"
                .to_owned(),
        );
    }
    let mut options = Options::default();
    options.create_if_missing(true);
    options.enable_statistics();
    options.set_statistics_level(StatsLevel::ExceptDetailedTimers);
    options.set_write_buffer_size(budget.write_buffer_size);
    options.set_max_write_buffer_number(budget.max_write_buffer_number);
    options.set_max_background_jobs(budget.max_background_jobs);
    let cache = Cache::new_lru_cache(budget.block_cache_bytes);
    let mut table_options = BlockBasedOptions::default();
    table_options.set_block_cache(&cache);
    options.set_block_based_table_factory(&table_options);
    let db = Arc::new(
        DB::open_cf_with_opts(
            &options,
            path,
            [(DEFAULT_COLUMN_FAMILY_NAME, options.clone())],
        )
        .map_err(|error| format!("cannot open RocksDB {}: {error}", path.display()))?,
    );
    Ok((db, Arc::new(options)))
}

fn recover_database(
    db: &DB,
    account_ids: &[u64],
    keyspace: &Keyspace,
    mode: BalanceMode,
) -> Result<Recovered, String> {
    let account_positions: HashMap<_, _> = account_ids
        .iter()
        .copied()
        .enumerate()
        .map(|(position, id)| (id, position))
        .collect();
    let latest_seq = match db
        .get(keyspace.latest_seq())
        .map_err(db_error("read latest sequence"))?
    {
        Some(bytes) => decode_u64(&bytes, "latest sequence")?,
        None => {
            let mut batch = WriteBatch::default();
            batch.put(keyspace.latest_seq(), 0_u64.to_be_bytes());
            db.write_opt(batch, &sync_write_options())
                .map_err(db_error("initialize latest sequence"))?;
            0
        }
    };
    let projected_seq = db
        .get(keyspace.key(KEY_PROJECTED_SEQ))
        .map_err(db_error("read durable projected sequence during recovery"))?
        .map(|bytes| decode_u64(&bytes, "projected sequence"))
        .transpose()?
        .unwrap_or(0);
    if projected_seq > latest_seq {
        return Err(format!(
            "durable projected sequence {projected_seq} is ahead of latest sequence {latest_seq}"
        ));
    }
    let gc_prefix_seq = db
        .get(keyspace.key(KEY_GC_PREFIX_SEQ))
        .map_err(db_error("read GC prefix during recovery"))?
        .map(|bytes| decode_u64(&bytes, "GC prefix sequence"))
        .transpose()?
        .unwrap_or(0);
    if gc_prefix_seq > latest_seq || gc_prefix_seq > projected_seq {
        return Err(format!(
            "GC prefix {gc_prefix_seq} exceeds latest sequence {latest_seq} or durable projection {projected_seq}"
        ));
    }
    let persisted_boundary = db
        .get(keyspace.key(KEY_PROJECTED_BEFORE))
        .map_err(db_error("read projected-before boundary during recovery"))?
        .map(|bytes| decode_u64(&bytes, "projected-before boundary"))
        .transpose()?;
    let boundary_sequence = db
        .get(keyspace.key(KEY_PROJECTED_BEFORE_SEQ))
        .map_err(db_error("read projected-before sequence during recovery"))?
        .map(|bytes| decode_u64(&bytes, "projected-before sequence"))
        .transpose()?;
    if boundary_sequence.is_some() && persisted_boundary.is_none() {
        return Err("projected-before sequence exists without its boundary".to_owned());
    }
    if let Some(sequence) = boundary_sequence {
        if sequence > projected_seq {
            return Err(format!(
                "published boundary sequence {sequence} exceeds durable projection {projected_seq}"
            ));
        }
        if gc_prefix_seq > sequence {
            return Err(format!(
                "GC prefix {gc_prefix_seq} exceeds published boundary sequence {sequence}"
            ));
        }
    } else if gc_prefix_seq > 0 {
        return Err("GC prefix exists without verified projected-before metadata".to_owned());
    }
    let mut balances = vec![0_u64; account_ids.len()];
    let manifest = if mode == BalanceMode::Checkpoint {
        load_checkpoint(db, account_ids, keyspace)?
    } else {
        None
    };
    let balance_coverage = match (mode, manifest) {
        (BalanceMode::PerBatch, _) => latest_seq,
        (BalanceMode::Checkpoint, Some(manifest)) => manifest.seq,
        (BalanceMode::Checkpoint, None) => 0,
    };
    if gc_prefix_seq > balance_coverage {
        return Err(format!(
            "GC prefix {gc_prefix_seq} exceeds durable balance coverage {balance_coverage}"
        ));
    }
    match mode {
        BalanceMode::PerBatch => {
            for (position, account) in account_ids.iter().copied().enumerate() {
                if let Some(bytes) = db
                    .get(keyspace.balance(account))
                    .map_err(db_error("read persisted account balance"))?
                {
                    balances[position] = decode_u64(&bytes, "account balance")?;
                }
            }
        }
        BalanceMode::Checkpoint => {
            if let Some(manifest) = manifest {
                if manifest.seq > latest_seq {
                    return Err(format!(
                        "checkpoint sequence {} is ahead of latest sequence {latest_seq}",
                        manifest.seq
                    ));
                }
                balances = read_checkpoint(db, manifest, account_ids, keyspace)?;
            }
        }
    }
    if mode == BalanceMode::Checkpoint {
        let start = manifest.map_or(0, |checkpoint| checkpoint.seq);
        if start < gc_prefix_seq {
            return Err(format!(
                "checkpoint sequence {start} is behind GC prefix {gc_prefix_seq}"
            ));
        }
        for seq in start.saturating_add(1)..=latest_seq {
            let bytes = db
                .get(keyspace.ledger(seq))
                .map_err(db_error("read ledger for checkpoint replay"))?
                .ok_or_else(|| format!("ledger sequence {seq} is missing during replay"))?;
            let stored = decode_transaction(&bytes)?;
            if stored.result.seq != seq {
                return Err(format!(
                    "ledger key sequence {seq} contains sequence {}",
                    stored.result.seq
                ));
            }
            let account = account_positions
                .get(&stored.request.key.account_id)
                .copied()
                .ok_or_else(|| {
                    format!(
                        "ledger references account {} outside this namespace",
                        stored.request.key.account_id
                    )
                })?;
            let balance = balances
                .get_mut(account)
                .ok_or_else(|| format!("ledger references unknown account index {account}"))?;
            *balance = stored.result.balance;
        }
    }
    cleanup_checkpoint_orphans(db, keyspace, manifest)?;
    Ok(Recovered {
        balances,
        latest_seq,
        manifest,
        projected_seq,
        gc_prefix_seq,
    })
}

fn collect_garbage_sync(
    db: Arc<DB>,
    keyspace: Keyspace,
    account_count: usize,
    mode: BalanceMode,
    max_records: usize,
    fail_before_sync: bool,
) -> Result<GcStepOutcome, String> {
    let latest_seq = db
        .get(keyspace.latest_seq())
        .map_err(db_error("read latest sequence for GC"))?
        .ok_or_else(|| "latest sequence metadata is missing".to_owned())
        .and_then(|bytes| decode_u64(&bytes, "latest sequence"))?;
    let projected_seq = db
        .get(keyspace.key(KEY_PROJECTED_SEQ))
        .map_err(db_error("read durable projected sequence for GC"))?
        .map(|bytes| decode_u64(&bytes, "projected sequence"))
        .transpose()?
        .unwrap_or(0);
    if projected_seq > latest_seq {
        return Err(format!(
            "durable projected sequence {projected_seq} is ahead of latest sequence {latest_seq}"
        ));
    }
    let prefix_key = keyspace.key(KEY_GC_PREFIX_SEQ);
    let gc_prefix_seq = db
        .get(&prefix_key)
        .map_err(db_error("read GC prefix"))?
        .map(|bytes| decode_u64(&bytes, "GC prefix sequence"))
        .transpose()?
        .unwrap_or(0);
    let Some(boundary_bytes) = db
        .get(keyspace.key(KEY_PROJECTED_BEFORE))
        .map_err(db_error("read published boundary for GC"))?
    else {
        if gc_prefix_seq > 0 {
            return Err("GC prefix exists without a published boundary".to_owned());
        }
        return Ok(GcStepOutcome::default());
    };
    let boundary = decode_u64(&boundary_bytes, "projected-before boundary")?;
    let boundary_sequence = db
        .get(keyspace.key(KEY_PROJECTED_BEFORE_SEQ))
        .map_err(db_error("read published boundary sequence for GC"))?
        .map(|bytes| decode_u64(&bytes, "projected-before sequence"))
        .transpose()?
        .ok_or_else(|| "published boundary is missing its sequence proof".to_owned())?;
    if boundary_sequence > projected_seq {
        return Err(format!(
            "published boundary sequence {boundary_sequence} exceeds durable projection {projected_seq}"
        ));
    }
    if gc_prefix_seq > boundary_sequence {
        return Err(format!(
            "GC prefix {gc_prefix_seq} exceeds published boundary sequence {boundary_sequence}"
        ));
    }
    let balance_coverage = match mode {
        BalanceMode::PerBatch => latest_seq,
        BalanceMode::Checkpoint => {
            let Some(bytes) = db
                .get(keyspace.checkpoint_manifest())
                .map_err(db_error("read checkpoint manifest for GC"))?
            else {
                if gc_prefix_seq > 0 {
                    return Err("GC prefix exists without a published checkpoint".to_owned());
                }
                return Ok(GcStepOutcome {
                    gc_prefix_seq,
                    ..GcStepOutcome::default()
                });
            };
            let manifest = decode_manifest(&bytes)
                .map_err(|error| format!("published checkpoint manifest is corrupt: {error}"))?;
            let expected_chunks = account_count.div_ceil(CHECKPOINT_CHUNK_ACCOUNTS);
            if manifest.users != account_count as u64
                || manifest.chunks as usize != expected_chunks
                || manifest.seq > latest_seq
            {
                return Err("published checkpoint manifest is invalid for GC".to_owned());
            }
            manifest.seq
        }
    };
    if gc_prefix_seq > latest_seq {
        return Err(format!(
            "GC prefix {gc_prefix_seq} exceeds latest sequence {latest_seq}"
        ));
    }
    if balance_coverage > latest_seq {
        return Err(format!(
            "durable balance coverage {balance_coverage} exceeds latest sequence {latest_seq}"
        ));
    }
    if gc_prefix_seq > balance_coverage || gc_prefix_seq > projected_seq {
        return Err(format!(
            "GC prefix {gc_prefix_seq} exceeds durable projected/balance coverage ({projected_seq}, {balance_coverage})"
        ));
    }
    let safe_sequence = latest_seq
        .min(projected_seq)
        .min(balance_coverage)
        .min(boundary_sequence);
    if gc_prefix_seq > safe_sequence {
        return Err(format!(
            "GC prefix {gc_prefix_seq} exceeds current safe sequence {safe_sequence}"
        ));
    }
    if gc_prefix_seq == safe_sequence {
        return Ok(GcStepOutcome {
            gc_prefix_seq,
            ..GcStepOutcome::default()
        });
    }

    let scan_started = Instant::now();
    let scan_end = safe_sequence.min(
        gc_prefix_seq
            .checked_add(u64::try_from(max_records).unwrap_or(u64::MAX))
            .ok_or_else(|| "GC sequence range overflow".to_owned())?,
    );
    let mut batch = WriteBatch::default();
    let mut scanned = 0_u64;
    let mut deleted = 0_u64;
    let mut bytes_deleted = 0_u64;
    let mut next_prefix = gc_prefix_seq;
    let mut blocked_at_seq = None;
    let mut delete_ns = 0_u64;
    for sequence in gc_prefix_seq.saturating_add(1)..=scan_end {
        let ledger_key = keyspace.ledger(sequence);
        let encoded = db
            .get(&ledger_key)
            .map_err(db_error("read candidate ledger record for GC"))?
            .ok_or_else(|| format!("ledger sequence {sequence} is missing before GC prefix"))?;
        let record = decode_transaction(&encoded)
            .map_err(|error| format!("ledger sequence {sequence} is corrupt during GC: {error}"))?;
        if record.result.seq != sequence {
            return Err(format!(
                "ledger key sequence {sequence} contains sequence {} during GC",
                record.result.seq
            ));
        }
        scanned += 1;
        if sequence > projected_seq
            || sequence > balance_coverage
            || sequence > boundary_sequence
            || record.request.key.transaction_at >= boundary
        {
            blocked_at_seq = Some(sequence);
            break;
        }
        let transaction_key = keyspace.transaction(record.request.key);
        let indexed = db
            .get(&transaction_key)
            .map_err(db_error("read transaction index record for GC"))?
            .ok_or_else(|| {
                format!("transaction index for ledger sequence {sequence} is missing")
            })?;
        if indexed.as_slice() != encoded.as_slice() {
            return Err(format!(
                "transaction index differs from ledger sequence {sequence} during GC"
            ));
        }
        let refund_marker = if record.request.operation == Operation::Refund
            && record.result.status == TransactionStatus::Applied
        {
            let target = record
                .request
                .refund_of
                .ok_or_else(|| format!("applied refund at sequence {sequence} has no target"))?;
            let marker_key = keyspace.refund(target);
            let marker_value = db
                .get(&marker_key)
                .map_err(db_error("read applied refund marker for GC"))?
                .ok_or_else(|| {
                    format!("applied refund at sequence {sequence} has no refund marker")
                })?;
            if marker_value.as_slice() != transaction_key.as_slice() {
                return Err(format!(
                    "applied refund marker does not point to sequence {sequence}"
                ));
            }
            Some((marker_key, marker_value.len()))
        } else {
            None
        };
        let delete_started = Instant::now();
        if let Some((marker_key, marker_value_len)) = refund_marker {
            batch.delete(marker_key);
            bytes_deleted =
                bytes_deleted.saturating_add(u64::try_from(marker_value_len).unwrap_or(u64::MAX));
        }
        batch.delete(&ledger_key);
        batch.delete(&transaction_key);
        bytes_deleted = bytes_deleted
            .saturating_add(u64::try_from(ledger_key.len()).unwrap_or(u64::MAX))
            .saturating_add(u64::try_from(transaction_key.len()).unwrap_or(u64::MAX))
            .saturating_add(
                u64::try_from(encoded.len())
                    .unwrap_or(u64::MAX)
                    .saturating_mul(2),
            );
        deleted += 1;
        next_prefix = sequence;
        delete_ns = delete_ns.saturating_add(nanos(delete_started.elapsed()));
    }
    let scan_ns = nanos(scan_started.elapsed()).saturating_sub(delete_ns);
    let mut write_ns = 0;
    if deleted > 0 {
        batch.put(&prefix_key, next_prefix.to_be_bytes());
        if fail_before_sync {
            return Err("injected GC sync failure before atomic batch write".to_owned());
        }
        let write_started = Instant::now();
        db.write_opt(batch, &sync_write_options())
            .map_err(db_error("synchronously commit ledger GC batch"))?;
        write_ns = nanos(write_started.elapsed());
    }
    Ok(GcStepOutcome {
        scanned,
        deleted,
        gc_prefix_seq: next_prefix,
        blocked_at_seq,
        scan_ns,
        delete_ns,
        write_ns,
        bytes_deleted,
    })
}

fn validate_index_and_ledger(
    db: &DB,
    latest_seq: u64,
    gc_prefix_seq: u64,
    keyspace: &Keyspace,
) -> Result<(), String> {
    if gc_prefix_seq > latest_seq {
        return Err(format!(
            "GC prefix {gc_prefix_seq} exceeds latest sequence {latest_seq}"
        ));
    }
    let retained_count = latest_seq - gc_prefix_seq;
    let seq_count = usize::try_from(retained_count)
        .map_err(|_| "latest sequence does not fit memory address space".to_owned())?;
    let mut indexed_seq = vec![false; seq_count];
    let mut index_count = 0_u64;
    let mut ledger_keys = Vec::<Vec<u8>>::with_capacity(1_024);
    let mut indexed_values = Vec::<Vec<u8>>::with_capacity(1_024);
    let start_index_key = keyspace.key(&[TRANSACTION_PREFIX]);
    for entry in db.iterator(IteratorMode::From(&start_index_key, DbDirection::Forward)) {
        let (key, value) = entry.map_err(db_error("scan transaction index"))?;
        if !keyspace.starts_with(&key, TRANSACTION_PREFIX) {
            break;
        }
        let record = decode_transaction(&value)?;
        if key.as_ref() != keyspace.transaction(record.request.key).as_slice() {
            return Err("transaction index key does not match its stored request".to_owned());
        }
        if record.result.seq <= gc_prefix_seq {
            return Err(format!(
                "transaction index retains GC'd sequence {} at or below prefix {gc_prefix_seq}",
                record.result.seq
            ));
        }
        let seq_index = record
            .result
            .seq
            .checked_sub(gc_prefix_seq + 1)
            .ok_or_else(|| "transaction index contains sequence zero".to_owned())?;
        let seq_index = usize::try_from(seq_index)
            .map_err(|_| "transaction sequence does not fit memory address space".to_owned())?;
        let seen = indexed_seq
            .get_mut(seq_index)
            .ok_or_else(|| "transaction index sequence exceeds latest sequence".to_owned())?;
        if std::mem::replace(seen, true) {
            return Err(format!(
                "transaction index repeats sequence {}",
                seq_index + 1
            ));
        }
        ledger_keys.push(keyspace.ledger(record.result.seq));
        indexed_values.push(value.to_vec());
        index_count += 1;
        if ledger_keys.len() == 1_024 {
            validate_index_ledger_chunk(db, &ledger_keys, &indexed_values, keyspace)?;
            ledger_keys.clear();
            indexed_values.clear();
        }
    }
    if !ledger_keys.is_empty() {
        validate_index_ledger_chunk(db, &ledger_keys, &indexed_values, keyspace)?;
    }
    if index_count != retained_count || indexed_seq.iter().any(|present| !present) {
        return Err(format!(
            "transaction index has {index_count} retained records for latest sequence {latest_seq} and GC prefix {gc_prefix_seq}"
        ));
    }
    let mut expected_seq = gc_prefix_seq.saturating_add(1);
    let start_ledger_key = keyspace.key(&[LEDGER_PREFIX]);
    for entry in db.iterator(IteratorMode::From(&start_ledger_key, DbDirection::Forward)) {
        let (key, value) = entry.map_err(db_error("scan ledger"))?;
        if !keyspace.starts_with(&key, LEDGER_PREFIX) {
            break;
        }
        let base_key = keyspace
            .strip(&key)
            .ok_or_else(|| "ledger key has wrong namespace".to_owned())?;
        let key_seq = decode_prefixed_u64(base_key, LEDGER_PREFIX, "ledger key")?;
        let record = decode_transaction(&value)?;
        if key_seq != expected_seq || record.result.seq != expected_seq {
            return Err(format!(
                "ledger is not contiguous at sequence {expected_seq} (key {key_seq}, value {})",
                record.result.seq
            ));
        }
        expected_seq += 1;
    }
    if expected_seq.saturating_sub(1) != latest_seq {
        return Err(format!(
            "ledger has {} retained records for latest sequence {latest_seq} and GC prefix {gc_prefix_seq}",
            expected_seq.saturating_sub(1)
        ));
    }
    Ok(())
}

fn validate_index_ledger_chunk(
    db: &DB,
    ledger_keys: &[Vec<u8>],
    indexed_values: &[Vec<u8>],
    keyspace: &Keyspace,
) -> Result<(), String> {
    let ledger_values = db.multi_get(ledger_keys.iter());
    for ((key, ledger_value), indexed_value) in
        ledger_keys.iter().zip(ledger_values).zip(indexed_values)
    {
        let ledger_value = ledger_value
            .map_err(db_error("read ledger records for index validation"))?
            .ok_or_else(|| {
                let seq = keyspace
                    .strip(key)
                    .and_then(|base| decode_prefixed_u64(base, LEDGER_PREFIX, "ledger key").ok())
                    .unwrap_or(0);
                format!("transaction index references missing ledger sequence {seq}")
            })?;
        if ledger_value.as_slice() != indexed_value.as_slice() {
            let base_key = keyspace
                .strip(key)
                .ok_or_else(|| "ledger key has wrong namespace".to_owned())?;
            let seq = decode_prefixed_u64(base_key, LEDGER_PREFIX, "ledger key")?;
            return Err(format!(
                "transaction index record differs from ledger sequence {seq}"
            ));
        }
    }
    Ok(())
}

fn process_batch(
    db: Arc<DB>,
    keyspace: Keyspace,
    transactions: Vec<Transaction>,
    balances: HashMap<u64, u64>,
    starting_seq: u64,
    mode: BalanceMode,
    refund_history: Option<Arc<dyn RefundHistory>>,
    gc_prefix_seq: u64,
) -> Result<BatchOutcome, String> {
    process_batch_selected(
        db,
        keyspace,
        transactions,
        balances,
        starting_seq,
        mode,
        refund_history,
        gc_prefix_seq,
        None,
        None,
        Vec::new(),
    )
}

fn process_batch_with_index_lookup(
    db: Arc<DB>,
    keyspace: Keyspace,
    transactions: Vec<Transaction>,
    balances: HashMap<u64, u64>,
    starting_seq: u64,
    mode: BalanceMode,
    refund_history: Option<Arc<dyn RefundHistory>>,
    gc_prefix_seq: u64,
    prefetched: Option<Vec<Option<StoredTransaction>>>,
    submitted_at: Instant,
    test_faults: Vec<IndexLookupTestFault>,
) -> Result<BatchOutcome, String> {
    process_batch_selected(
        db,
        keyspace,
        transactions,
        balances,
        starting_seq,
        mode,
        refund_history,
        gc_prefix_seq,
        prefetched,
        Some(submitted_at),
        test_faults,
    )
}

fn process_batch_selected(
    db: Arc<DB>,
    keyspace: Keyspace,
    transactions: Vec<Transaction>,
    mut balances: HashMap<u64, u64>,
    starting_seq: u64,
    mode: BalanceMode,
    refund_history: Option<Arc<dyn RefundHistory>>,
    gc_prefix_seq: u64,
    prefetched: Option<Vec<Option<StoredTransaction>>>,
    submitted_at: Option<Instant>,
    test_faults: Vec<IndexLookupTestFault>,
) -> Result<BatchOutcome, String> {
    let build_started = Instant::now();
    let mut index_metrics = IndexApplyMetrics::default();
    if let Some(submitted_at) = submitted_at {
        index_metrics.blocking_pool_wait_ns = nanos(Instant::now().duration_since(submitted_at));
    }
    let mut final_seq = starting_seq;
    let mut batch = WriteBatch::default();
    let mut staged = HashMap::<TransactionKey, StoredTransaction>::new();
    let mut staged_refunds = HashSet::<TransactionKey>::new();
    let mut touched_accounts = HashSet::<u64>::new();
    let mut replies = Vec::with_capacity(transactions.len());
    let mut new_transactions = 0_u64;

    let apply_started = Instant::now();
    for (position, transaction) in transactions.into_iter().enumerate() {
        let current = *balances.get(&transaction.key.account_id).ok_or_else(|| {
            format!(
                "starting balance for account {} is missing",
                transaction.key.account_id
            )
        })?;
        if let Some(prior) = staged.get(&transaction.key) {
            let reply = if prior.request == transaction {
                Reply::Transaction {
                    status: prior.result.status,
                    balance: prior.result.balance,
                    seq: prior.result.seq,
                    replayed: true,
                }
            } else {
                Reply::Conflict
            };
            replies.push(reply);
            continue;
        }

        let stored_prior = if let Some(prefetched) = prefetched.as_ref() {
            index_metrics.keys_looked_up = index_metrics.keys_looked_up.saturating_add(1);
            let prior = prefetched.get(position).ok_or_else(|| {
                format!("prefetched transaction-index results omit position {position}")
            })?;
            if prior.is_some() {
                index_metrics.hits = index_metrics.hits.saturating_add(1);
            } else {
                index_metrics.misses = index_metrics.misses.saturating_add(1);
            }
            prior.clone()
        } else {
            let key_started = submitted_at.map(|_| Instant::now());
            let encoded_key = keyspace.transaction(transaction.key);
            if let Some(key_started) = key_started {
                index_metrics.key_prep_ns = index_metrics
                    .key_prep_ns
                    .saturating_add(nanos(key_started.elapsed()));
            }
            let get_started = submitted_at.map(|_| Instant::now());
            let bytes = db
                .get(&encoded_key)
                .map_err(db_error("check transaction index"))?;
            if let Some(get_started) = get_started {
                index_metrics.get_calls = index_metrics.get_calls.saturating_add(1);
                index_metrics.keys_looked_up = index_metrics.keys_looked_up.saturating_add(1);
                index_metrics.native_get_ns = index_metrics
                    .native_get_ns
                    .saturating_add(nanos(get_started.elapsed()));
            }
            match bytes {
                Some(bytes) => {
                    let decode_started = submitted_at.map(|_| Instant::now());
                    let decoded = decode_transaction(&bytes)?;
                    if let Some(decode_started) = decode_started {
                        index_metrics.decode_ns = index_metrics
                            .decode_ns
                            .saturating_add(nanos(decode_started.elapsed()));
                        index_metrics.hits = index_metrics.hits.saturating_add(1);
                    }
                    Some(decoded)
                }
                None => {
                    if submitted_at.is_some() {
                        index_metrics.misses = index_metrics.misses.saturating_add(1);
                    }
                    None
                }
            }
        };

        if let Some(prior) = stored_prior {
            if prior.request.key != transaction.key {
                return Err("transaction index value has a mismatched key".to_owned());
            }
            let reply = if prior.request == transaction {
                Reply::Transaction {
                    status: prior.result.status,
                    balance: prior.result.balance,
                    seq: prior.result.seq,
                    replayed: true,
                }
            } else {
                Reply::Conflict
            };
            replies.push(reply);
            continue;
        }

        let mut next_balance = current;
        let status = if transaction.amount == 0 {
            TransactionStatus::InvalidAmount
        } else {
            match transaction.operation {
                Operation::Credit => match current.checked_add(transaction.amount) {
                    Some(value) => {
                        next_balance = value;
                        TransactionStatus::Applied
                    }
                    None => TransactionStatus::CreditOverflow,
                },
                Operation::Debit if current >= transaction.amount => {
                    next_balance = current - transaction.amount;
                    TransactionStatus::Applied
                }
                Operation::Debit => TransactionStatus::InsufficientFunds,
                Operation::Refund => {
                    match resolve_refund_target(
                        &db,
                        &keyspace,
                        &staged,
                        &transaction,
                        &staged_refunds,
                        refund_history.as_deref(),
                        gc_prefix_seq,
                    )? {
                        RefundTarget::Invalid => TransactionStatus::InvalidRefund,
                        RefundTarget::Used => TransactionStatus::RefundAlreadyUsed,
                        RefundTarget::Valid(amount) => match current.checked_add(amount) {
                            Some(value) => {
                                next_balance = value;
                                TransactionStatus::Applied
                            }
                            None => TransactionStatus::CreditOverflow,
                        },
                    }
                }
            }
        };
        final_seq = final_seq
            .checked_add(1)
            .ok_or_else(|| "ledger sequence overflow".to_owned())?;
        let stored = StoredTransaction {
            request: transaction.clone(),
            result: TransactionResult {
                status,
                balance: next_balance,
                seq: final_seq,
            },
        };
        let encoded = encode_transaction(&stored);
        batch.put(keyspace.ledger(final_seq), &encoded);
        batch.put(keyspace.transaction(transaction.key), &encoded);
        if status == TransactionStatus::Applied {
            balances.insert(transaction.key.account_id, next_balance);
            touched_accounts.insert(transaction.key.account_id);
            if transaction.operation == Operation::Refund {
                let target = transaction
                    .refund_of
                    .expect("a successful refund has a validated target");
                batch.put(
                    keyspace.refund(target),
                    keyspace.transaction(transaction.key),
                );
                staged_refunds.insert(target);
            }
        }
        staged.insert(transaction.key, stored.clone());
        replies.push(Reply::Transaction {
            status,
            balance: next_balance,
            seq: final_seq,
            replayed: false,
        });
        new_transactions += 1;
    }

    index_metrics.sequential_apply_build_ns = nanos(apply_started.elapsed());
    let read_build_ns = nanos(build_started.elapsed());
    let wal_started = Instant::now();
    if new_transactions > 0 {
        batch.put(keyspace.latest_seq(), final_seq.to_be_bytes());
        if mode == BalanceMode::PerBatch {
            for account in touched_accounts.iter().copied() {
                batch.put(keyspace.balance(account), balances[&account].to_be_bytes());
            }
        }
        if should_fail_sync_write(&test_faults) {
            return Err("injected synchronous account batch write failure".to_owned());
        }
        db.write_opt(batch, &sync_write_options())
            .map_err(db_error("synchronously commit account batch"))?;
    }
    let wal_sync_ns = nanos(wal_started.elapsed());
    index_metrics.sync_write_batch_ns = wal_sync_ns;
    let final_balances = touched_accounts
        .into_iter()
        .map(|account| (account, balances[&account]))
        .collect();
    Ok(BatchOutcome {
        replies,
        balances: final_balances,
        latest_seq: final_seq,
        new_transactions,
        read_build_ns,
        wal_sync_ns,
        index_metrics,
    })
}

#[derive(Clone)]
struct PreparedIndexKey {
    position: usize,
    expected_key: TransactionKey,
    encoded_key: Vec<u8>,
}

async fn prefetch_transaction_indexes(
    db: Arc<DB>,
    keyspace: Keyspace,
    transactions: &[Transaction],
    config: IndexLookupConfig,
    test_faults: &[IndexLookupTestFault],
) -> Result<(Vec<Option<StoredTransaction>>, IndexPrefetchMetrics), String> {
    let query_prepare_started = Instant::now();
    let group_size = match config.mode {
        IndexLookupMode::PointGet => {
            return Err("PointGet does not use the MultiGet prefetch path".to_owned());
        }
        IndexLookupMode::WholeBatchMultiGet => transactions.len(),
        IndexLookupMode::Chunked { group_size, .. } => group_size,
    };
    let max_in_flight = match config.mode {
        IndexLookupMode::WholeBatchMultiGet => 1,
        IndexLookupMode::Chunked { max_in_flight, .. } => max_in_flight,
        IndexLookupMode::PointGet => unreachable!(),
    };
    let prepared: Vec<_> = transactions
        .iter()
        .enumerate()
        .map(|(position, transaction)| PreparedIndexKey {
            position,
            expected_key: transaction.key,
            encoded_key: keyspace.transaction(transaction.key),
        })
        .collect();
    let key_prep_ns = nanos(query_prepare_started.elapsed());
    let group_count = prepared.len().div_ceil(group_size);
    let mut slots = (0..transactions.len())
        .map(|_| None::<Option<StoredTransaction>>)
        .collect::<Vec<_>>();
    let mut seen_groups = vec![false; group_count];
    let mut submitted_at = vec![None; group_count];
    let mut tasks = tokio::task::JoinSet::new();
    let activity = Arc::new(QueryJobActivity::default());
    let mut next_group = 0_usize;
    let mut metrics = IndexPrefetchMetrics {
        key_prep_ns,
        ..Default::default()
    };
    let mut first_error = None::<String>;

    while !tasks.is_empty() || (first_error.is_none() && next_group < group_count) {
        while first_error.is_none() && next_group < group_count && tasks.len() < max_in_flight {
            let group_index = next_group;
            let first_position = group_index * group_size;
            let end_position = first_position
                .saturating_add(group_size)
                .min(prepared.len());
            let group = prepared[first_position..end_position].to_vec();
            let group_db = Arc::clone(&db);
            let group_activity = Arc::clone(&activity);
            let group_faults = test_faults
                .iter()
                .filter(|fault| test_fault_applies_to_group(fault, group_index))
                .cloned()
                .collect::<Vec<_>>();
            let submitted = Instant::now();
            submitted_at[group_index] = Some(submitted);
            tasks.spawn_blocking(move || {
                let _running = group_activity.enter();
                run_query_group(
                    group_db,
                    group_index,
                    first_position,
                    end_position,
                    group,
                    submitted,
                    group_faults,
                )
            });
            next_group += 1;
            metrics.groups_submitted += 1;
            metrics.max_observed_in_flight_groups =
                metrics.max_observed_in_flight_groups.max(tasks.len());
        }

        let Some(joined) = tasks.join_next().await else {
            break;
        };
        match joined {
            Ok(Ok(mut completion)) => {
                let group_index = completion.group_index;
                let Some(Some(group_submitted)) = submitted_at.get(group_index).copied() else {
                    first_error.get_or_insert_with(|| {
                        format!("MultiGet returned out-of-range group {group_index}")
                    });
                    continue;
                };
                completion.metrics.submit_to_collection_ns =
                    nanos(Instant::now().duration_since(group_submitted));
                let expected_first = group_index.saturating_mul(group_size);
                let expected_end = expected_first
                    .saturating_add(group_size)
                    .min(prepared.len());
                if group_index >= group_count {
                    first_error.get_or_insert_with(|| {
                        format!("MultiGet returned out-of-range group {group_index}")
                    });
                    continue;
                }
                if seen_groups[group_index] {
                    first_error.get_or_insert_with(|| {
                        format!("MultiGet returned duplicate group {group_index}")
                    });
                    continue;
                }
                if completion.first_position != expected_first
                    || completion.end_position != expected_end
                {
                    first_error.get_or_insert_with(|| {
                        format!(
                            "MultiGet group {group_index} returned bounds {}..{}, expected {expected_first}..{expected_end}",
                            completion.first_position, completion.end_position
                        )
                    });
                    continue;
                }
                seen_groups[group_index] = true;
                let expected_len = expected_end.saturating_sub(expected_first);
                if completion.entries.len() != expected_len {
                    first_error.get_or_insert_with(|| {
                        format!(
                            "MultiGet group {group_index} returned {} positions, expected {expected_len}",
                            completion.entries.len()
                        )
                    });
                    continue;
                }
                for (position, record) in &completion.entries {
                    if *position < expected_first || *position >= expected_end {
                        first_error.get_or_insert_with(|| {
                            format!(
                                "MultiGet group {group_index} returned out-of-range position {position}"
                            )
                        });
                        break;
                    }
                    if slots[*position].is_some() {
                        first_error.get_or_insert_with(|| {
                            format!("MultiGet returned duplicate position {position}")
                        });
                        break;
                    }
                    slots[*position] = Some(record.clone());
                }
                metrics.blocking_pool_wait_ns = metrics
                    .blocking_pool_wait_ns
                    .saturating_add(completion.metrics.blocking_pool_wait_ns);
                metrics.native_get_ns = metrics
                    .native_get_ns
                    .saturating_add(completion.metrics.native_get_ns);
                metrics.decode_ns = metrics
                    .decode_ns
                    .saturating_add(completion.metrics.decode_ns);
                metrics.submit_to_collection_ns = metrics
                    .submit_to_collection_ns
                    .saturating_add(completion.metrics.submit_to_collection_ns);
                metrics.groups.push(completion.metrics);
                signal_group_collected_for_test(group_index, &test_faults);
            }
            Ok(Err(error)) => {
                first_error.get_or_insert(error);
            }
            Err(error) => {
                first_error
                    .get_or_insert_with(|| format!("MultiGet blocking task failed: {error}"));
            }
        }
    }

    // join_next above drains every submitted blocking closure, including jobs
    // still running after the first query, decode, or result-validation error.
    if let Some(error) = first_error {
        return Err(error);
    }
    if metrics.groups_submitted != group_count || seen_groups.iter().any(|seen| !seen) {
        return Err(format!(
            "MultiGet returned {} of {group_count} expected groups",
            seen_groups.iter().filter(|seen| **seen).count()
        ));
    }
    if slots.iter().any(Option::is_none) {
        return Err("MultiGet result omitted one or more original positions".to_owned());
    }
    let records = slots
        .into_iter()
        .map(|slot| slot.expect("all MultiGet result positions were validated"))
        .collect::<Vec<_>>();
    metrics.keys_looked_up = transactions.len() as u64;
    metrics.get_calls = metrics.groups_submitted as u64;
    metrics.hits = records.iter().filter(|record| record.is_some()).count() as u64;
    metrics.misses = records.iter().filter(|record| record.is_none()).count() as u64;
    metrics.max_observed_running_query_jobs = activity.peak.load(Ordering::Acquire);
    Ok((records, metrics))
}

fn run_query_group(
    db: Arc<DB>,
    group_index: usize,
    first_position: usize,
    end_position: usize,
    group: Vec<PreparedIndexKey>,
    submitted_at: Instant,
    test_faults: Vec<IndexLookupTestFault>,
) -> Result<IndexLookupGroupCompletion, String> {
    let blocking_pool_wait_ns = nanos(submitted_at.elapsed());
    apply_query_test_faults(group_index, &test_faults)?;
    let cf = db
        .cf_handle("default")
        .ok_or_else(|| "RocksDB default column family is unavailable".to_owned())?;
    let native_started = Instant::now();
    let values = db.batched_multi_get_cf(
        &cf,
        group.iter().map(|key| key.encoded_key.as_slice()),
        false,
    );
    let native_get_ns = nanos(native_started.elapsed());
    if values.len() != group.len() {
        return Err(format!(
            "MultiGet group {group_index} returned {} values for {} keys",
            values.len(),
            group.len()
        ));
    }
    let mut entries = Vec::with_capacity(group.len());
    let mut decode_ns = 0_u64;
    for (prepared, value) in group.iter().zip(values) {
        let value = value.map_err(db_error("read transaction index with MultiGet"))?;
        let record = if let Some(value) = value {
            let decode_started = Instant::now();
            let record = decode_transaction(&value)?;
            if record.request.key != prepared.expected_key {
                return Err(format!(
                    "transaction index value at position {} has a mismatched key",
                    prepared.position
                ));
            }
            decode_ns = decode_ns.saturating_add(nanos(decode_started.elapsed()));
            Some(record)
        } else {
            None
        };
        entries.push((prepared.position, record));
    }
    let mut completion = IndexLookupGroupCompletion {
        group_index,
        first_position,
        end_position,
        entries,
        metrics: IndexLookupGroupMetrics {
            group_index,
            first_position,
            key_count: group.len(),
            blocking_pool_wait_ns,
            native_get_ns,
            decode_ns,
            submit_to_collection_ns: 0,
        },
    };
    mutate_group_result_for_test(group_index, &mut completion, &test_faults);
    Ok(completion)
}

#[cfg(test)]
fn test_fault_applies_to_group(fault: &IndexLookupTestFault, group_index: usize) -> bool {
    match fault {
        IndexLookupTestFault::FailReadGroup(index)
        | IndexLookupTestFault::PanicReadGroup(index)
        | IndexLookupTestFault::DelayReadGroup(index, _)
        | IndexLookupTestFault::BlockReadGroup(index, _, _)
        | IndexLookupTestFault::CorruptGroupBounds(index)
        | IndexLookupTestFault::DropGroupResult(index)
        | IndexLookupTestFault::DuplicateGroupResult(index)
        | IndexLookupTestFault::GroupCollectedSignal(index, _) => *index == group_index,
        IndexLookupTestFault::FailSyncWrite => false,
    }
}

#[cfg(not(test))]
fn test_fault_applies_to_group(_: &IndexLookupTestFault, _: usize) -> bool {
    false
}

#[cfg(test)]
fn signal_group_collected_for_test(group_index: usize, faults: &[IndexLookupTestFault]) {
    for fault in faults {
        if let IndexLookupTestFault::GroupCollectedSignal(index, sender) = fault {
            if *index == group_index {
                let _ = sender.send(group_index);
            }
        }
    }
}

#[cfg(not(test))]
fn signal_group_collected_for_test(_: usize, _: &[IndexLookupTestFault]) {}

#[cfg(test)]
fn apply_query_test_faults(
    group_index: usize,
    faults: &[IndexLookupTestFault],
) -> Result<(), String> {
    for fault in faults {
        match fault {
            IndexLookupTestFault::FailReadGroup(index) if *index == group_index => {
                return Err(format!(
                    "injected MultiGet read failure in group {group_index}"
                ));
            }
            IndexLookupTestFault::PanicReadGroup(index) if *index == group_index => {
                panic!("injected MultiGet task panic in group {group_index}");
            }
            IndexLookupTestFault::DelayReadGroup(index, delay) if *index == group_index => {
                std::thread::sleep(*delay);
            }
            IndexLookupTestFault::BlockReadGroup(index, started, release)
                if *index == group_index =>
            {
                let _ = started.send(());
                let (lock, changed) = &**release;
                let mut released = lock
                    .lock()
                    .map_err(|_| "test query-release mutex poisoned".to_owned())?;
                while !*released {
                    released = changed
                        .wait(released)
                        .map_err(|_| "test query-release mutex poisoned".to_owned())?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

#[cfg(not(test))]
fn apply_query_test_faults(_: usize, _: &[IndexLookupTestFault]) -> Result<(), String> {
    Ok(())
}

#[cfg(test)]
fn mutate_group_result_for_test(
    group_index: usize,
    completion: &mut IndexLookupGroupCompletion,
    faults: &[IndexLookupTestFault],
) {
    for fault in faults {
        match fault {
            IndexLookupTestFault::CorruptGroupBounds(index) if *index == group_index => {
                completion.first_position = completion.first_position.saturating_add(1);
            }
            IndexLookupTestFault::DropGroupResult(index) if *index == group_index => {
                completion.entries.pop();
            }
            IndexLookupTestFault::DuplicateGroupResult(index) if *index == group_index => {
                if let Some(entry) = completion.entries.first().cloned() {
                    completion.entries.push(entry);
                }
            }
            _ => {}
        }
    }
}

#[cfg(not(test))]
fn mutate_group_result_for_test(
    _: usize,
    _: &mut IndexLookupGroupCompletion,
    _: &[IndexLookupTestFault],
) {
}

#[cfg(test)]
fn should_fail_sync_write(faults: &[IndexLookupTestFault]) -> bool {
    faults
        .iter()
        .any(|fault| matches!(fault, IndexLookupTestFault::FailSyncWrite))
}

#[cfg(not(test))]
fn should_fail_sync_write(_: &[IndexLookupTestFault]) -> bool {
    false
}

enum RefundTarget {
    Invalid,
    Used,
    Valid(u64),
}

fn resolve_refund_target(
    db: &DB,
    keyspace: &Keyspace,
    staged: &HashMap<TransactionKey, StoredTransaction>,
    transaction: &Transaction,
    staged_refunds: &HashSet<TransactionKey>,
    refund_history: Option<&dyn RefundHistory>,
    gc_prefix_seq: u64,
) -> Result<RefundTarget, String> {
    let Some(refund_key_value) = transaction.refund_of else {
        return Ok(RefundTarget::Invalid);
    };
    if refund_key_value.account_id != transaction.key.account_id {
        return Ok(RefundTarget::Invalid);
    }
    if let Some(prior) = staged.get(&refund_key_value) {
        if prior.request.operation != Operation::Debit
            || prior.result.status != TransactionStatus::Applied
            || prior.request.amount != transaction.amount
        {
            return Ok(RefundTarget::Invalid);
        }
        if staged_refunds.contains(&refund_key_value) {
            return Ok(RefundTarget::Used);
        }
        return Ok(RefundTarget::Valid(prior.request.amount));
    }
    let local_record = db
        .get(keyspace.transaction(refund_key_value))
        .map_err(db_error("read refund target index"))?
        .map(|bytes| decode_transaction(&bytes))
        .transpose()?;
    if let Some(prior) = local_record.as_ref() {
        if prior.request.operation != Operation::Debit
            || prior.result.status != TransactionStatus::Applied
            || prior.request.key.account_id != transaction.key.account_id
            || prior.request.amount != transaction.amount
        {
            return Ok(RefundTarget::Invalid);
        }
        if staged_refunds.contains(&refund_key_value)
            || db
                .get(keyspace.refund(refund_key_value))
                .map_err(db_error("read refund marker"))?
                .is_some()
        {
            return Ok(RefundTarget::Used);
        }
        return Ok(RefundTarget::Valid(prior.request.amount));
    }
    if gc_prefix_seq == 0 {
        return Ok(RefundTarget::Invalid);
    }
    let history = refund_history.ok_or_else(|| {
        format!(
            "refund target {refund_key_value:?} may be GC'd, but historical lookup is unavailable"
        )
    })?;
    let Some(historical) = history.lookup_debit(refund_key_value)? else {
        return Ok(RefundTarget::Invalid);
    };
    if historical.amount != transaction.amount {
        return Ok(RefundTarget::Invalid);
    }
    if staged_refunds.contains(&refund_key_value)
        || db
            .get(keyspace.refund(refund_key_value))
            .map_err(db_error("read refund marker"))?
            .is_some()
        || historical.already_refunded
    {
        return Ok(RefundTarget::Used);
    }
    Ok(RefundTarget::Valid(historical.amount))
}

async fn checkpoint_worker(
    db: Arc<DB>,
    keyspace: Keyspace,
    mut receiver: mpsc::Receiver<CheckpointMessage>,
    mut previous: Option<Manifest>,
    failure: Arc<Mutex<Option<String>>>,
    metrics: Arc<Mutex<MetricsSnapshot>>,
) -> Result<(), String> {
    while let Some(message) = receiver.recv().await {
        match message {
            CheckpointMessage::Barrier(reply) => {
                let result = failure
                    .lock()
                    .map_err(|_| "checkpoint failure mutex poisoned".to_owned())?
                    .clone()
                    .map_or(Ok(()), Err);
                let _ = reply.send(result.clone());
                result?;
            }
            CheckpointMessage::Snapshot(snapshot) => {
                let next_generation = previous.map_or(1, |value| value.generation + 1);
                let write_started = Instant::now();
                let db_for_write = Arc::clone(&db);
                let keyspace_for_write = keyspace.clone();
                let old = previous;
                let result = tokio::task::spawn_blocking(move || {
                    write_checkpoint(
                        &db_for_write,
                        &keyspace_for_write,
                        next_generation,
                        old,
                        snapshot,
                    )
                })
                .await;
                let result = match result {
                    Ok(result) => result,
                    Err(error) => {
                        let message = format!("checkpoint blocking task failed: {error}");
                        if let Ok(mut slot) = failure.lock() {
                            *slot = Some(message.clone());
                        }
                        return Err(message);
                    }
                };
                match result {
                    Ok((manifest, chunk_ns, manifest_ns)) => {
                        previous = Some(manifest);
                        let duration_ns = nanos(write_started.elapsed());
                        let completed_at = Instant::now();
                        let mut metrics = metrics
                            .lock()
                            .map_err(|_| "account metrics mutex poisoned".to_owned())?;
                        metrics.checkpoint_count += 1;
                        metrics.checkpoint_chunk_sync_ns += chunk_ns;
                        metrics.checkpoint_manifest_sync_ns += manifest_ns;
                        metrics.checkpoint_duration_ns += duration_ns;
                        metrics.checkpoint_latest_seq = manifest.seq;
                        metrics.checkpoint_samples.push(CheckpointSample {
                            sequence: manifest.seq,
                            duration_ns,
                            chunk_sync_ns: chunk_ns,
                            manifest_sync_ns: manifest_ns,
                            completed_at,
                        });
                    }
                    Err(error) => {
                        if let Ok(mut slot) = failure.lock() {
                            *slot = Some(error.clone());
                        }
                        return Err(error);
                    }
                }
            }
        }
    }
    Ok(())
}

fn write_checkpoint(
    db: &DB,
    keyspace: &Keyspace,
    generation: u64,
    previous: Option<Manifest>,
    snapshot: CheckpointSnapshot,
) -> Result<(Manifest, u64, u64), String> {
    let chunks = snapshot.balances.len().div_ceil(CHECKPOINT_CHUNK_ACCOUNTS);
    let chunks_u32 = u32::try_from(chunks).map_err(|_| "too many checkpoint chunks".to_owned())?;
    let checksum = checkpoint_checksum(&snapshot.balances);
    let mut chunk_batch = WriteBatch::default();
    for (chunk_no, balances) in snapshot
        .balances
        .chunks(CHECKPOINT_CHUNK_ACCOUNTS)
        .enumerate()
    {
        let mut encoded = Vec::with_capacity(4 + balances.len() * 16);
        encoded.extend_from_slice(&(balances.len() as u32).to_be_bytes());
        for (account, balance) in balances {
            encoded.extend_from_slice(&account.to_be_bytes());
            encoded.extend_from_slice(&balance.to_be_bytes());
        }
        chunk_batch.put(
            keyspace.checkpoint_chunk(generation, chunk_no as u32),
            encoded,
        );
    }
    let chunk_started = Instant::now();
    db.write_opt(chunk_batch, &sync_write_options())
        .map_err(db_error("sync checkpoint chunks"))?;
    let chunk_ns = nanos(chunk_started.elapsed());

    let manifest = Manifest {
        generation,
        seq: snapshot.seq,
        users: snapshot.balances.len() as u64,
        chunks: chunks_u32,
        checksum,
    };
    let mut manifest_batch = WriteBatch::default();
    manifest_batch.put(keyspace.checkpoint_manifest(), encode_manifest(manifest));
    if let Some(old) = previous {
        for chunk in 0..old.chunks {
            manifest_batch.delete(keyspace.checkpoint_chunk(old.generation, chunk));
        }
    }
    let manifest_started = Instant::now();
    db.write_opt(manifest_batch, &sync_write_options())
        .map_err(db_error("sync checkpoint manifest"))?;
    Ok((manifest, chunk_ns, nanos(manifest_started.elapsed())))
}

fn load_checkpoint(
    db: &DB,
    account_ids: &[u64],
    keyspace: &Keyspace,
) -> Result<Option<Manifest>, String> {
    let users = account_ids.len();
    let Some(bytes) = db
        .get(keyspace.checkpoint_manifest())
        .map_err(db_error("read checkpoint manifest"))?
    else {
        return Ok(None);
    };
    let manifest = decode_manifest(&bytes)
        .map_err(|error| format!("published checkpoint manifest is corrupt: {error}"))?;
    if manifest.users != users as u64
        || manifest.chunks as usize != users.div_ceil(CHECKPOINT_CHUNK_ACCOUNTS)
    {
        return Err("published checkpoint manifest has invalid user/chunk counts".to_owned());
    }
    let balances = read_checkpoint(db, manifest, account_ids, keyspace)
        .map_err(|error| format!("published checkpoint is corrupt: {error}"))?;
    let ordered: Vec<_> = balances
        .into_iter()
        .zip(account_ids.iter().copied())
        .map(|(balance, account)| (account, balance))
        .collect();
    if checkpoint_checksum(&ordered) != manifest.checksum {
        return Err("published checkpoint checksum mismatch".to_owned());
    }
    Ok(Some(manifest))
}

fn read_checkpoint(
    db: &DB,
    manifest: Manifest,
    account_ids: &[u64],
    keyspace: &Keyspace,
) -> Result<Vec<u64>, String> {
    let users = account_ids.len();
    if manifest.users != users as u64 {
        return Err("checkpoint account count does not match configured users".to_owned());
    }
    let expected_chunks = users.div_ceil(CHECKPOINT_CHUNK_ACCOUNTS);
    if manifest.chunks as usize != expected_chunks {
        return Err("checkpoint chunk count is invalid".to_owned());
    }
    let mut balances = vec![0_u64; users];
    let mut covered = 0_usize;
    for chunk in 0..manifest.chunks {
        let bytes = db
            .get(keyspace.checkpoint_chunk(manifest.generation, chunk))
            .map_err(db_error("read checkpoint chunk"))?
            .ok_or_else(|| format!("checkpoint chunk {chunk} is missing"))?;
        if bytes.len() < 4 {
            return Err(format!("checkpoint chunk {chunk} is truncated"));
        }
        let count = u32::from_be_bytes(bytes[..4].try_into().expect("4-byte slice")) as usize;
        if count == 0 || count > CHECKPOINT_CHUNK_ACCOUNTS || bytes.len() != 4 + count * 16 {
            return Err(format!("checkpoint chunk {chunk} has invalid width/count"));
        }
        for slot in 0..count {
            let offset = 4 + slot * 16;
            let account =
                u64::from_be_bytes(bytes[offset..offset + 8].try_into().expect("8 bytes"));
            let balance =
                u64::from_be_bytes(bytes[offset + 8..offset + 16].try_into().expect("8 bytes"));
            if account_ids.get(covered).copied() != Some(account) || covered >= users {
                return Err(format!("checkpoint account order breaks at {covered}"));
            }
            balances[covered] = balance;
            covered += 1;
        }
    }
    if covered != users {
        return Err(format!("checkpoint covers {covered} of {users} users"));
    }
    let ordered: Vec<_> = balances
        .iter()
        .enumerate()
        .map(|(position, balance)| (account_ids[position], *balance))
        .collect();
    if checkpoint_checksum(&ordered) != manifest.checksum {
        return Err("checkpoint checksum mismatch".to_owned());
    }
    Ok(balances)
}

fn cleanup_checkpoint_orphans(
    db: &DB,
    keyspace: &Keyspace,
    keep: Option<Manifest>,
) -> Result<(), String> {
    let mut orphan_keys = Vec::new();
    let start_key = keyspace.key(&[CHECKPOINT_PREFIX]);
    for entry in db.iterator(IteratorMode::From(&start_key, DbDirection::Forward)) {
        let (key, _) = entry.map_err(db_error("scan checkpoint chunks"))?;
        if !keyspace.starts_with(&key, CHECKPOINT_PREFIX) {
            break;
        }
        let base_key = keyspace
            .strip(&key)
            .ok_or_else(|| "checkpoint key has wrong namespace".to_owned())?;
        let valid_key = base_key.len() == 13;
        let generation = if valid_key {
            u64::from_be_bytes(base_key[1..9].try_into().expect("8-byte generation"))
        } else {
            0
        };
        let chunk_index = if valid_key {
            u32::from_be_bytes(base_key[9..13].try_into().expect("4-byte chunk index"))
        } else {
            0
        };
        let valid_kept_chunk = keep.is_some_and(|manifest| {
            manifest.generation == generation && chunk_index < manifest.chunks
        });
        if !valid_key || !valid_kept_chunk {
            orphan_keys.push(key.to_vec());
        }
    }
    if !orphan_keys.is_empty() {
        let mut batch = WriteBatch::default();
        for key in orphan_keys {
            batch.delete(key);
        }
        db.write_opt(batch, &sync_write_options())
            .map_err(db_error("delete orphan checkpoint chunks"))?;
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StoredTransaction {
    request: Transaction,
    result: TransactionResult,
}

fn encode_transaction(stored: &StoredTransaction) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(70);
    put_key(&mut bytes, stored.request.key);
    bytes.push(operation_code(stored.request.operation));
    bytes.extend_from_slice(&stored.request.amount.to_be_bytes());
    if let Some(refund_of) = stored.request.refund_of {
        bytes.push(1);
        put_key(&mut bytes, refund_of);
    } else {
        bytes.push(0);
    }
    bytes.push(status_code(stored.result.status));
    bytes.extend_from_slice(&stored.result.balance.to_be_bytes());
    bytes.extend_from_slice(&stored.result.seq.to_be_bytes());
    bytes
}

fn decode_transaction(bytes: &[u8]) -> Result<StoredTransaction, String> {
    let mut cursor = 0_usize;
    let key = read_key(bytes, &mut cursor)?;
    let operation = match read_byte(bytes, &mut cursor)? {
        0 => Operation::Credit,
        1 => Operation::Debit,
        2 => Operation::Refund,
        value => return Err(format!("unknown operation code {value}")),
    };
    let amount = read_u64_cursor(bytes, &mut cursor)?;
    let refund_of = match read_byte(bytes, &mut cursor)? {
        0 => None,
        1 => Some(read_key(bytes, &mut cursor)?),
        value => return Err(format!("unknown refund flag {value}")),
    };
    let status = match read_byte(bytes, &mut cursor)? {
        0 => TransactionStatus::Applied,
        1 => TransactionStatus::InsufficientFunds,
        2 => TransactionStatus::CreditOverflow,
        3 => TransactionStatus::InvalidAmount,
        4 => TransactionStatus::InvalidRefund,
        5 => TransactionStatus::RefundAlreadyUsed,
        value => return Err(format!("unknown transaction status code {value}")),
    };
    let balance = read_u64_cursor(bytes, &mut cursor)?;
    let seq = read_u64_cursor(bytes, &mut cursor)?;
    if cursor != bytes.len() {
        return Err(format!(
            "transaction record has {} trailing bytes",
            bytes.len() - cursor
        ));
    }
    Ok(StoredTransaction {
        request: Transaction {
            key,
            operation,
            amount,
            refund_of,
        },
        result: TransactionResult {
            status,
            balance,
            seq,
        },
    })
}

fn transaction_key(key: TransactionKey) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(25);
    bytes.push(TRANSACTION_PREFIX);
    put_key(&mut bytes, key);
    bytes
}

fn ledger_key(seq: u64) -> Vec<u8> {
    prefixed_u64(LEDGER_PREFIX, seq)
}

fn balance_key(account: u64) -> Vec<u8> {
    prefixed_u64(BALANCE_PREFIX, account)
}

fn refund_key(key: TransactionKey) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(25);
    bytes.push(REFUND_PREFIX);
    put_key(&mut bytes, key);
    bytes
}

fn checkpoint_chunk_key(generation: u64, chunk: u32) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(13);
    bytes.push(CHECKPOINT_PREFIX);
    bytes.extend_from_slice(&generation.to_be_bytes());
    bytes.extend_from_slice(&chunk.to_be_bytes());
    bytes
}

fn prefixed_u64(prefix: u8, value: u64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(9);
    bytes.push(prefix);
    bytes.extend_from_slice(&value.to_be_bytes());
    bytes
}

fn encode_manifest(manifest: Manifest) -> [u8; 40] {
    let mut bytes = [0_u8; 40];
    bytes[..4].copy_from_slice(b"ACCP");
    bytes[4..12].copy_from_slice(&manifest.generation.to_be_bytes());
    bytes[12..20].copy_from_slice(&manifest.seq.to_be_bytes());
    bytes[20..28].copy_from_slice(&manifest.users.to_be_bytes());
    bytes[28..32].copy_from_slice(&manifest.chunks.to_be_bytes());
    bytes[32..40].copy_from_slice(&manifest.checksum.to_be_bytes());
    bytes
}

fn decode_manifest(bytes: &[u8]) -> Result<Manifest, String> {
    if bytes.len() != 40 || &bytes[..4] != b"ACCP" {
        return Err("invalid checkpoint manifest format".to_owned());
    }
    Ok(Manifest {
        generation: u64::from_be_bytes(bytes[4..12].try_into().expect("8 bytes")),
        seq: u64::from_be_bytes(bytes[12..20].try_into().expect("8 bytes")),
        users: u64::from_be_bytes(bytes[20..28].try_into().expect("8 bytes")),
        chunks: u32::from_be_bytes(bytes[28..32].try_into().expect("4 bytes")),
        checksum: u64::from_be_bytes(bytes[32..40].try_into().expect("8 bytes")),
    })
}

fn checkpoint_checksum(balances: &[(u64, u64)]) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for (account, balance) in balances {
        for byte in account
            .to_be_bytes()
            .into_iter()
            .chain(balance.to_be_bytes())
        {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
    }
    hash
}

fn sync_write_options() -> WriteOptions {
    let mut options = WriteOptions::default();
    options.disable_wal(false);
    options.set_sync(true);
    options
}

fn read_key(bytes: &[u8], cursor: &mut usize) -> Result<TransactionKey, String> {
    Ok(TransactionKey {
        account_id: read_u64_cursor(bytes, cursor)?,
        tx_id: read_u64_cursor(bytes, cursor)?,
        transaction_at: read_u64_cursor(bytes, cursor)?,
    })
}

fn put_key(bytes: &mut Vec<u8>, key: TransactionKey) {
    bytes.extend_from_slice(&key.account_id.to_be_bytes());
    bytes.extend_from_slice(&key.tx_id.to_be_bytes());
    bytes.extend_from_slice(&key.transaction_at.to_be_bytes());
}

fn read_byte(bytes: &[u8], cursor: &mut usize) -> Result<u8, String> {
    let byte = *bytes
        .get(*cursor)
        .ok_or_else(|| "truncated transaction record".to_owned())?;
    *cursor += 1;
    Ok(byte)
}

fn read_u64_cursor(bytes: &[u8], cursor: &mut usize) -> Result<u64, String> {
    let end = cursor
        .checked_add(8)
        .ok_or_else(|| "transaction cursor overflow".to_owned())?;
    let value = bytes
        .get(*cursor..end)
        .ok_or_else(|| "truncated transaction record".to_owned())?;
    *cursor = end;
    Ok(u64::from_be_bytes(value.try_into().expect("8-byte slice")))
}

fn operation_code(operation: Operation) -> u8 {
    match operation {
        Operation::Credit => 0,
        Operation::Debit => 1,
        Operation::Refund => 2,
    }
}

fn status_code(status: TransactionStatus) -> u8 {
    match status {
        TransactionStatus::Applied => 0,
        TransactionStatus::InsufficientFunds => 1,
        TransactionStatus::CreditOverflow => 2,
        TransactionStatus::InvalidAmount => 3,
        TransactionStatus::InvalidRefund => 4,
        TransactionStatus::RefundAlreadyUsed => 5,
    }
}

fn decode_u64(bytes: &[u8], label: &str) -> Result<u64, String> {
    let value: [u8; 8] = bytes
        .try_into()
        .map_err(|_| format!("persisted {label} has invalid width"))?;
    Ok(u64::from_be_bytes(value))
}

fn decode_prefixed_u64(bytes: &[u8], prefix: u8, label: &str) -> Result<u64, String> {
    if bytes.len() != 9 || bytes[0] != prefix {
        return Err(format!("persisted {label} has invalid key width/prefix"));
    }
    Ok(u64::from_be_bytes(
        bytes[1..9].try_into().expect("8-byte sequence"),
    ))
}

fn db_error(context: &'static str) -> impl FnOnce(rocksdb::Error) -> String {
    move |error| format!("RocksDB {context} failed: {error}")
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn stable_sample(account_id: u64, request_id: u64) -> bool {
    mix64(account_id.rotate_left(17) ^ request_id) & LATENCY_SAMPLE_MASK == 0
}

fn mix64(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct TestDb(PathBuf);

    impl TestDb {
        fn new(label: &str) -> Self {
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "ledger-account-store-{label}-{}-{nonce}",
                std::process::id()
            ));
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestDb {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn txn(
        account: u64,
        tx_id: u64,
        at: u64,
        operation: Operation,
        amount: u64,
        refund_of: Option<TransactionKey>,
    ) -> Transaction {
        Transaction {
            key: TransactionKey {
                account_id: account,
                tx_id,
                transaction_at: at,
            },
            operation,
            amount,
            refund_of,
        }
    }

    fn status(reply: &Reply) -> (TransactionStatus, u64, u64, bool) {
        match reply {
            Reply::Transaction {
                status,
                balance,
                seq,
                replayed,
            } => (*status, *balance, *seq, *replayed),
            Reply::Conflict => panic!("expected transaction reply, got conflict"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn one_synced_batch_atomically_publishes_ledger_index_sequence_and_balance() {
        let directory = TestDb::new("atomic");
        let store = AccountStore::open(directory.path(), 2, BalanceMode::PerBatch, 100)
            .await
            .unwrap();
        let credit = txn(0, 1, 101, Operation::Credit, 5, None);
        let debit = txn(0, 2, 102, Operation::Debit, 2, None);
        let replies = store
            .handle_batch(vec![credit.clone(), debit.clone()])
            .await
            .unwrap();
        assert_eq!(
            status(&replies[0]),
            (TransactionStatus::Applied, 5, 1, false)
        );
        assert_eq!(
            status(&replies[1]),
            (TransactionStatus::Applied, 3, 2, false)
        );
        assert_eq!(store.latest_seq(), 2);
        assert_eq!(store.balance(0).unwrap(), 3);
        assert_eq!(
            store
                .inner
                .db
                .get(KEY_LATEST_SEQ)
                .unwrap()
                .unwrap()
                .as_ref(),
            2_u64.to_be_bytes()
        );
        assert_eq!(
            store
                .inner
                .db
                .get(balance_key(0))
                .unwrap()
                .unwrap()
                .as_ref(),
            3_u64.to_be_bytes()
        );
        for expected in [&credit, &debit] {
            let index = store
                .inner
                .db
                .get(transaction_key(expected.key))
                .unwrap()
                .unwrap();
            let seq = if expected.key.tx_id == 1 { 1 } else { 2 };
            let ledger = store.inner.db.get(ledger_key(seq)).unwrap().unwrap();
            assert_eq!(index.as_slice(), ledger.as_slice());
        }
        store.shutdown().await.unwrap();

        let reopened = AccountStore::open(directory.path(), 2, BalanceMode::PerBatch, 100)
            .await
            .unwrap();
        assert_eq!(reopened.latest_seq(), 2);
        assert_eq!(reopened.all_balances(), [(0, 3), (1, 0)]);
        reopened.validate_integrity().await.unwrap();
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn exact_duplicate_replays_without_sequence_and_changed_body_conflicts() {
        let directory = TestDb::new("dedupe");
        let store = AccountStore::open(directory.path(), 1, BalanceMode::PerBatch, 100)
            .await
            .unwrap();
        let original = txn(0, 77, 700, Operation::Credit, 3, None);
        let duplicate = original.clone();
        let conflict = txn(0, 77, 700, Operation::Credit, 4, None);
        let replies = store
            .handle_batch(vec![original.clone(), duplicate, conflict.clone()])
            .await
            .unwrap();
        assert_eq!(
            status(&replies[0]),
            (TransactionStatus::Applied, 3, 1, false)
        );
        assert_eq!(
            status(&replies[1]),
            (TransactionStatus::Applied, 3, 1, true)
        );
        assert_eq!(replies[2], Reply::Conflict);
        let persisted_duplicate = store.handle_batch(vec![original]).await.unwrap();
        assert_eq!(
            status(&persisted_duplicate[0]),
            (TransactionStatus::Applied, 3, 1, true)
        );
        let persisted_conflict = store.handle_batch(vec![conflict]).await.unwrap();
        assert_eq!(persisted_conflict, [Reply::Conflict]);
        assert_eq!(store.latest_seq(), 1);
        assert_eq!(store.balance(0).unwrap(), 3);
        store.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn same_batch_refund_is_full_same_account_and_one_time() {
        let directory = TestDb::new("refund");
        let store = AccountStore::open(directory.path(), 2, BalanceMode::PerBatch, 100)
            .await
            .unwrap();
        let credit = txn(0, 1, 1, Operation::Credit, 10, None);
        let debit = txn(0, 2, 2, Operation::Debit, 4, None);
        let refund = txn(0, 3, 3, Operation::Refund, 4, Some(debit.key));
        let second_refund = txn(0, 4, 4, Operation::Refund, 4, Some(debit.key));
        let replies = store
            .handle_batch(vec![credit, debit.clone(), refund, second_refund])
            .await
            .unwrap();
        assert_eq!(status(&replies[0]).0, TransactionStatus::Applied);
        assert_eq!(status(&replies[1]).0, TransactionStatus::Applied);
        assert_eq!(
            status(&replies[2]),
            (TransactionStatus::Applied, 10, 3, false)
        );
        assert_eq!(
            status(&replies[3]),
            (TransactionStatus::RefundAlreadyUsed, 10, 4, false)
        );
        assert!(store.inner.db.get(refund_key(debit.key)).unwrap().is_some());
        assert_eq!(store.balance(0).unwrap(), 10);
        assert_eq!(store.latest_seq(), 4);
        store.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn rejection_results_receive_sequences_and_overflow_is_explicit() {
        let directory = TestDb::new("reject");
        let store = AccountStore::open(directory.path(), 2, BalanceMode::PerBatch, 100)
            .await
            .unwrap();
        let credit_on_one = txn(1, 1, 11, Operation::Credit, 1, None);
        let debit = txn(0, 1, 1, Operation::Debit, 1, None);
        let max_credit = txn(0, 2, 2, Operation::Credit, u64::MAX, None);
        let overflow = txn(0, 3, 3, Operation::Credit, 1, None);
        let no_target = txn(0, 4, 4, Operation::Refund, 1, None);
        let cross_account = txn(0, 5, 5, Operation::Refund, 1, Some(credit_on_one.key));
        let zero = txn(0, 6, 6, Operation::Credit, 0, None);
        let refund_credit = txn(1, 2, 12, Operation::Refund, 1, Some(credit_on_one.key));
        let replies = store
            .handle_batch(vec![
                debit,
                max_credit,
                overflow,
                no_target,
                cross_account,
                zero,
                credit_on_one,
                refund_credit,
            ])
            .await
            .unwrap();
        let statuses: Vec<_> = replies.iter().map(|reply| status(reply).0).collect();
        assert_eq!(
            statuses,
            [
                TransactionStatus::InsufficientFunds,
                TransactionStatus::Applied,
                TransactionStatus::CreditOverflow,
                TransactionStatus::InvalidRefund,
                TransactionStatus::InvalidRefund,
                TransactionStatus::InvalidAmount,
                TransactionStatus::Applied,
                TransactionStatus::InvalidRefund,
            ]
        );
        assert_eq!(
            replies
                .iter()
                .map(|reply| status(reply).2)
                .collect::<Vec<_>>(),
            (1..=8).collect::<Vec<_>>()
        );
        assert_eq!(store.balance(0).unwrap(), u64::MAX);
        assert_eq!(store.balance(1).unwrap(), 1);
        assert_eq!(store.latest_seq(), 8);
        assert!(store
            .handle_batch(vec![txn(2, 99, 99, Operation::Credit, 1, None)])
            .await
            .is_err());
        assert_eq!(store.latest_seq(), 8);
        store.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn malformed_persisted_index_fails_request_without_publishing_state() {
        let directory = TestDb::new("index-error");
        let store = AccountStore::open(directory.path(), 1, BalanceMode::PerBatch, 100)
            .await
            .unwrap();
        let transaction = txn(0, 1, 1, Operation::Credit, 7, None);
        store.handle_batch(vec![transaction.clone()]).await.unwrap();
        store
            .inner
            .db
            .put(transaction_key(transaction.key), [0_u8])
            .unwrap();
        assert!(store.handle_batch(vec![transaction]).await.is_err());
        assert_eq!(store.latest_seq(), 1);
        assert_eq!(store.balance(0).unwrap(), 7);
        store.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn checkpoint_recovery_ignores_orphan_chunks_and_replays_without_manifest() {
        let directory = TestDb::new("checkpoint-orphan");
        let store = AccountStore::open(directory.path(), 3, BalanceMode::Checkpoint, 100)
            .await
            .unwrap();
        let credit = txn(2, 1, 1, Operation::Credit, 5, None);
        store.handle_batch(vec![credit]).await.unwrap();
        store
            .inner
            .db
            .put(checkpoint_chunk_key(99, 0), [9_u8, 8, 7])
            .unwrap();
        store.shutdown().await.unwrap();

        let recovered = AccountStore::open(directory.path(), 3, BalanceMode::Checkpoint, 100)
            .await
            .unwrap();
        assert_eq!(recovered.latest_seq(), 1);
        assert_eq!(recovered.all_balances(), [(0, 0), (1, 0), (2, 5)]);
        assert!(recovered
            .inner
            .db
            .get(checkpoint_chunk_key(99, 0))
            .unwrap()
            .is_none());
        recovered.validate_integrity().await.unwrap();
        recovered.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn published_checkpoint_manifest_recovers_and_corruption_fails_closed() {
        let directory = TestDb::new("checkpoint-manifest");
        let store = AccountStore::open(directory.path(), 3, BalanceMode::Checkpoint, 1)
            .await
            .unwrap();
        store
            .handle_batch(vec![txn(1, 1, 1, Operation::Credit, 4, None)])
            .await
            .unwrap();
        store.drain_checkpoints().await.unwrap();
        let manifest = decode_manifest(
            &store
                .inner
                .db
                .get(KEY_CHECKPOINT_MANIFEST)
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(manifest.seq, 1);
        assert_eq!(manifest.generation, 1);
        store.shutdown().await.unwrap();

        let recovered = AccountStore::open(directory.path(), 3, BalanceMode::Checkpoint, 1)
            .await
            .unwrap();
        assert_eq!(recovered.all_balances(), [(0, 0), (1, 4), (2, 0)]);
        recovered.validate_integrity().await.unwrap();
        recovered.shutdown().await.unwrap();

        let corrupt = AccountStore::open(directory.path(), 3, BalanceMode::Checkpoint, 1)
            .await
            .unwrap();
        corrupt
            .inner
            .db
            .put(checkpoint_chunk_key(manifest.generation, 0), [0_u8])
            .unwrap();
        corrupt.shutdown().await.unwrap();
        assert!(
            AccountStore::open(directory.path(), 3, BalanceMode::Checkpoint, 1)
                .await
                .is_err()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn ledger_range_is_contiguous_and_respects_shard_namespace() {
        let directory = TestDb::new("ledger-range");
        let store = AccountStore::open(directory.path(), 1, BalanceMode::Checkpoint, 100)
            .await
            .unwrap();
        store
            .handle_batch(vec![
                txn(0, 1, 1, Operation::Credit, 1, None),
                txn(0, 2, 2, Operation::Credit, 2, None),
                txn(0, 3, 3, Operation::Credit, 3, None),
            ])
            .await
            .unwrap();
        let middle = store.read_ledger_range(2, 8).await.unwrap();
        assert_eq!(
            middle
                .records
                .iter()
                .map(|record| record.result.seq)
                .collect::<Vec<_>>(),
            [2, 3]
        );
        assert!(store
            .read_ledger_range(4, 2)
            .await
            .unwrap()
            .records
            .is_empty());
        assert!(store
            .read_ledger_range(1, 0)
            .await
            .unwrap()
            .records
            .is_empty());
        assert!(store.read_ledger_range(0, 1).await.is_err());

        let shared_db = Arc::clone(&store.inner.db);
        let options = Arc::clone(&store.inner.options);
        let shard = AccountStore::open_on_database(
            shared_db,
            options,
            vec![0],
            23,
            BalanceMode::Checkpoint,
            100,
        )
        .await
        .unwrap();
        shard
            .handle_batch(vec![txn(0, 1, 1, Operation::Credit, 17, None)])
            .await
            .unwrap();
        let namespaced = shard.read_ledger_range(1, 4).await.unwrap();
        assert_eq!(namespaced.records.len(), 1);
        assert_eq!(namespaced.records[0].request.amount, 17);
        assert_eq!(shard.namespace_id(), Some(23));
        shard.shutdown().await.unwrap();
        store.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn ledger_range_fails_on_missing_corrupt_and_mismatched_records() {
        let missing_directory = TestDb::new("ledger-range-missing");
        let missing = AccountStore::open(missing_directory.path(), 1, BalanceMode::PerBatch, 100)
            .await
            .unwrap();
        missing
            .handle_batch(vec![
                txn(0, 1, 1, Operation::Credit, 1, None),
                txn(0, 2, 2, Operation::Credit, 2, None),
            ])
            .await
            .unwrap();
        missing
            .inner
            .db
            .delete(missing.inner.keyspace.ledger(2))
            .unwrap();
        assert!(missing
            .read_ledger_range(1, 2)
            .await
            .unwrap_err()
            .contains("ledger sequence 2 is missing"));
        missing.shutdown().await.unwrap();

        let corrupt_directory = TestDb::new("ledger-range-corrupt");
        let corrupt = AccountStore::open(corrupt_directory.path(), 1, BalanceMode::PerBatch, 100)
            .await
            .unwrap();
        corrupt
            .handle_batch(vec![txn(0, 1, 1, Operation::Credit, 1, None)])
            .await
            .unwrap();
        let original = corrupt
            .inner
            .db
            .get(corrupt.inner.keyspace.ledger(1))
            .unwrap()
            .unwrap();
        corrupt
            .inner
            .db
            .put(corrupt.inner.keyspace.ledger(1), [0_u8])
            .unwrap();
        assert!(corrupt
            .read_ledger_range(1, 1)
            .await
            .unwrap_err()
            .contains("ledger sequence 1 is corrupt"));

        let mut decoded = decode_transaction(&original).unwrap();
        decoded.result.seq = 2;
        corrupt
            .inner
            .db
            .put(
                corrupt.inner.keyspace.ledger(1),
                encode_transaction(&decoded),
            )
            .unwrap();
        assert!(corrupt
            .read_ledger_range(1, 1)
            .await
            .unwrap_err()
            .contains("ledger key sequence 1 contains sequence 2"));
        corrupt.shutdown().await.unwrap();
    }
}
