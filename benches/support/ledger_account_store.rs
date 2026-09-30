//! Standalone benchmark storage handler for Tokio account transactions.
//!
//! Foreground requests are submitted as awaited batches. The only concurrent
//! work is the bounded, sequential balance-checkpoint writer in checkpoint
//! mode.

use rocksdb::statistics::{StatsLevel, Ticker};
use rocksdb::{
    BlockBasedOptions, Cache, DB, Direction as DbDirection, IteratorMode, Options, WriteBatch,
    WriteOptions,
};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex as AsyncMutex, mpsc};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TransactionKey {
    pub account_id: u64,
    pub tx_id: u64,
    pub transaction_at: u64,
}

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
}

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
}

struct BatchOutcome {
    replies: Vec<Reply>,
    balances: Vec<(u64, u64)>,
    latest_seq: u64,
    new_transactions: u64,
    read_build_ns: u64,
    wal_sync_ns: u64,
}

const LEDGER_PREFIX: u8 = b'L';
const TRANSACTION_PREFIX: u8 = b'T';
const BALANCE_PREFIX: u8 = b'B';
const REFUND_PREFIX: u8 = b'R';
const CHECKPOINT_PREFIX: u8 = b'C';
const KEY_LATEST_SEQ: &[u8] = b"M:latest-seq";
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
        let outcome = tokio::task::spawn_blocking(move || {
            process_batch(
                db,
                keyspace,
                transactions,
                starting_balances,
                starting_seq,
                mode,
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
        let keyspace = self.inner.keyspace.clone();
        tokio::task::spawn_blocking(move || validate_index_and_ledger(&db, seq, &keyspace))
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
        DB::open(&options, path)
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
        DB::open(&options, path)
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
    let mut balances = vec![0_u64; account_ids.len()];
    let manifest = if mode == BalanceMode::Checkpoint {
        load_checkpoint(db, account_ids, keyspace)?
    } else {
        None
    };
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
    })
}

fn validate_index_and_ledger(db: &DB, latest_seq: u64, keyspace: &Keyspace) -> Result<(), String> {
    let seq_count = usize::try_from(latest_seq)
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
        let seq_index = record
            .result
            .seq
            .checked_sub(1)
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
    if index_count != latest_seq || indexed_seq.iter().any(|present| !present) {
        return Err(format!(
            "transaction index has {index_count} records for latest sequence {latest_seq}"
        ));
    }
    let mut expected_seq = 1_u64;
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
            "ledger has {} records for latest sequence {latest_seq}",
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
    mut balances: HashMap<u64, u64>,
    starting_seq: u64,
    mode: BalanceMode,
) -> Result<BatchOutcome, String> {
    let build_started = Instant::now();
    let mut final_seq = starting_seq;
    let mut batch = WriteBatch::default();
    let mut staged = HashMap::<TransactionKey, StoredTransaction>::new();
    let mut staged_refunds = HashSet::<TransactionKey>::new();
    let mut touched_accounts = HashSet::<u64>::new();
    let mut replies = Vec::with_capacity(transactions.len());
    let mut new_transactions = 0_u64;

    for transaction in transactions {
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

        if let Some(bytes) = db
            .get(keyspace.transaction(transaction.key))
            .map_err(db_error("check transaction index"))?
        {
            let prior = decode_transaction(&bytes)?;
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

    let read_build_ns = nanos(build_started.elapsed());
    let wal_started = Instant::now();
    if new_transactions > 0 {
        batch.put(keyspace.latest_seq(), final_seq.to_be_bytes());
        if mode == BalanceMode::PerBatch {
            for account in touched_accounts.iter().copied() {
                batch.put(keyspace.balance(account), balances[&account].to_be_bytes());
            }
        }
        db.write_opt(batch, &sync_write_options())
            .map_err(db_error("synchronously commit account batch"))?;
    }
    let wal_sync_ns = nanos(wal_started.elapsed());
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
    })
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
) -> Result<RefundTarget, String> {
    let Some(refund_key_value) = transaction.refund_of else {
        return Ok(RefundTarget::Invalid);
    };
    if refund_key_value.account_id != transaction.key.account_id {
        return Ok(RefundTarget::Invalid);
    }
    let prior = if let Some(record) = staged.get(&refund_key_value) {
        Some(record.clone())
    } else {
        db.get(keyspace.transaction(refund_key_value))
            .map_err(db_error("read refund target index"))?
            .map(|bytes| decode_transaction(&bytes))
            .transpose()?
    };
    let Some(prior) = prior else {
        return Ok(RefundTarget::Invalid);
    };
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
    Ok(RefundTarget::Valid(prior.request.amount))
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
                        let mut metrics = metrics
                            .lock()
                            .map_err(|_| "account metrics mutex poisoned".to_owned())?;
                        metrics.checkpoint_count += 1;
                        metrics.checkpoint_chunk_sync_ns += chunk_ns;
                        metrics.checkpoint_manifest_sync_ns += manifest_ns;
                        metrics.checkpoint_duration_ns += nanos(write_started.elapsed());
                        metrics.checkpoint_latest_seq = manifest.seq;
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
        assert!(
            store
                .handle_batch(vec![txn(2, 99, 99, Operation::Credit, 1, None)])
                .await
                .is_err()
        );
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
        assert!(
            recovered
                .inner
                .db
                .get(checkpoint_chunk_key(99, 0))
                .unwrap()
                .is_none()
        );
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
        assert!(
            store
                .read_ledger_range(4, 2)
                .await
                .unwrap()
                .records
                .is_empty()
        );
        assert!(
            store
                .read_ledger_range(1, 0)
                .await
                .unwrap()
                .records
                .is_empty()
        );
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
        assert!(
            missing
                .read_ledger_range(1, 2)
                .await
                .unwrap_err()
                .contains("ledger sequence 2 is missing")
        );
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
        assert!(
            corrupt
                .read_ledger_range(1, 1)
                .await
                .unwrap_err()
                .contains("ledger sequence 1 is corrupt")
        );

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
        assert!(
            corrupt
                .read_ledger_range(1, 1)
                .await
                .unwrap_err()
                .contains("ledger key sequence 1 contains sequence 2")
        );
        corrupt.shutdown().await.unwrap();
    }
}
