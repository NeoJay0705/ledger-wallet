//! Full closed-loop Tokio benchmark for a safe transaction-time boundary.

use crate::ledger_account_store::{
    AccountStore, BalanceMode, Operation, Reply, Transaction, TransactionKey, TransactionResult,
    TransactionStatus,
};
use crate::ledger_preflight::{self, IoSample, PreflightConfig, PreflightReport};
use crate::ledger_projection_worker::MockProjectionStore;
use crate::ledger_time_boundary::{
    self, AdmissionGate, GateContentionSnapshot, ProjectionBatchSample, ProjectionMetrics,
    ProjectionProgress, RequestStages, RoutedReply, WatermarkManager, WatermarkSample,
};
use crate::request_batch_queue::{BatchQueue, BatchWorker};
use cpu_time::ProcessTime;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::runtime::Builder;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::task::JoinSet;

const DEFAULT_USERS: usize = 50_000;
const DEFAULT_COROUTINES: usize = 50_000;
const DEFAULT_REQUESTS_PER_USER: usize = 200;
const DEFAULT_SAMPLE_STRIDE: u64 = 64;
const DEFAULT_QUEUE_CAPACITY: usize = 50_000;
const DEFAULT_BATCH_SIZE: usize = 2_048;
const DEFAULT_BATCH_TIMEOUT_MS: u64 = 5;
const DEFAULT_PROJECTION_BATCH_SIZE: usize = 256;
const DEFAULT_RETENTION_MS: u64 = 60_000;
const DEFAULT_WATERMARK_INTERVAL_MS: u64 = 1_000;
const DEFAULT_HISTORY_DELAY_MS: u64 = 10;
const DEFAULT_PROJECTION_DELAY_MS: u64 = 0;
const DEFAULT_OUTPUT_ROOT: &str = "target/ledger-time-boundary-tokio-trials";
const DEFAULT_PREFLIGHT_OBSERVATION_MS: u64 = 3_000;
const DEFAULT_PREFLIGHT_TIMEOUT_MS: u64 = 60_000;
const DEFAULT_MAX_CPU_BUSY_PCT: f64 = 10.0;
const DEFAULT_MAX_DISK_BUSY_PCT: f64 = 5.0;
const DEFAULT_MEMORY_RESERVE_BYTES: u64 = 768 * 1024 * 1024;
const DEFAULT_FREE_SPACE_RESERVE_BYTES: u64 = 1024 * 1024 * 1024;
const ESTIMATED_DISK_BYTES_PER_TRANSACTION: u64 = 768;
const ESTIMATED_PROJECTION_BYTES_PER_TRANSACTION: u64 = 256;
const PROJECTION_TIMEOUT: Duration = Duration::from_secs(300);
const CASES: [u8; 4] = [0, 1, 5, 10];
#[derive(Clone, Debug)]
struct Config {
    users: usize,
    coroutines: usize,
    requests_per_user: usize,
    sample_stride: u64,
    queue_capacity: usize,
    batch_size: usize,
    batch_timeout: Duration,
    projection_batch_size: usize,
    projection_delay: Duration,
    history_delay: Duration,
    retention: Duration,
    watermark_interval: Duration,
    output_root: PathBuf,
    preflight: PreflightConfig,
    memory_reserve_bytes: u64,
    free_space_reserve_bytes: u64,
    smoke: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            users: DEFAULT_USERS,
            coroutines: DEFAULT_COROUTINES,
            requests_per_user: DEFAULT_REQUESTS_PER_USER,
            sample_stride: DEFAULT_SAMPLE_STRIDE,
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
            batch_size: DEFAULT_BATCH_SIZE,
            batch_timeout: Duration::from_millis(DEFAULT_BATCH_TIMEOUT_MS),
            projection_batch_size: DEFAULT_PROJECTION_BATCH_SIZE,
            projection_delay: Duration::from_millis(DEFAULT_PROJECTION_DELAY_MS),
            history_delay: Duration::from_millis(DEFAULT_HISTORY_DELAY_MS),
            retention: Duration::from_millis(DEFAULT_RETENTION_MS),
            watermark_interval: Duration::from_millis(DEFAULT_WATERMARK_INTERVAL_MS),
            output_root: PathBuf::from(DEFAULT_OUTPUT_ROOT),
            preflight: PreflightConfig {
                observation: Duration::from_millis(DEFAULT_PREFLIGHT_OBSERVATION_MS),
                timeout: Duration::from_millis(DEFAULT_PREFLIGHT_TIMEOUT_MS),
                max_cpu_busy_pct: DEFAULT_MAX_CPU_BUSY_PCT,
                max_disk_busy_pct: DEFAULT_MAX_DISK_BUSY_PCT,
                min_available_mem_bytes: 0,
                min_free_bytes: 0,
            },
            memory_reserve_bytes: DEFAULT_MEMORY_RESERVE_BYTES,
            free_space_reserve_bytes: DEFAULT_FREE_SPACE_RESERVE_BYTES,
            smoke: false,
        }
    }
}

impl Config {
    fn parse() -> Result<Self, String> {
        let mut config = Self::default();
        let args: Vec<_> = std::env::args().skip(1).collect();
        let mut index = 0;
        while index < args.len() {
            let flag = args[index].as_str();
            if flag == "--help" || flag == "-h" {
                print_help();
                std::process::exit(0);
            }
            if flag == "--smoke" {
                config.smoke = true;
                config.users = 20;
                config.coroutines = 20;
                config.requests_per_user = DEFAULT_REQUESTS_PER_USER;
                config.sample_stride = 1;
                index += 1;
                continue;
            }
            // Cargo passes this through for `harness = false` benches.
            if flag == "--bench" {
                index += 1;
                continue;
            }
            index += 1;
            let value = args
                .get(index)
                .ok_or_else(|| format!("missing value for {flag}"))?;
            match flag {
                "--users" => config.users = parse_value(flag, value)?,
                "--coroutines" => config.coroutines = parse_value(flag, value)?,
                "--requests-per-user" => config.requests_per_user = parse_value(flag, value)?,
                "--sample-stride" => config.sample_stride = parse_value(flag, value)?,
                "--queue-capacity" => config.queue_capacity = parse_value(flag, value)?,
                "--batch-size" => config.batch_size = parse_value(flag, value)?,
                "--batch-timeout-ms" => {
                    config.batch_timeout = Duration::from_millis(parse_value(flag, value)?)
                }
                "--projection-batch-size" => {
                    config.projection_batch_size = parse_value(flag, value)?
                }
                "--projection-delay-ms" => {
                    config.projection_delay = Duration::from_millis(parse_value(flag, value)?)
                }
                "--history-delay-ms" => {
                    config.history_delay = Duration::from_millis(parse_value(flag, value)?)
                }
                "--retention-ms" => {
                    config.retention = Duration::from_millis(parse_value(flag, value)?)
                }
                "--watermark-interval-ms" => {
                    config.watermark_interval = Duration::from_millis(parse_value(flag, value)?)
                }
                "--output-root" => config.output_root = PathBuf::from(value),
                "--preflight-observation-ms" => {
                    config.preflight.observation = Duration::from_millis(parse_value(flag, value)?)
                }
                "--preflight-timeout-ms" => {
                    config.preflight.timeout = Duration::from_millis(parse_value(flag, value)?)
                }
                "--max-cpu-busy-pct" => {
                    config.preflight.max_cpu_busy_pct = parse_value(flag, value)?
                }
                "--max-disk-busy-pct" => {
                    config.preflight.max_disk_busy_pct = parse_value(flag, value)?
                }
                "--memory-reserve-mib" => {
                    config.memory_reserve_bytes = parse_value::<u64>(flag, value)?
                        .checked_mul(1024 * 1024)
                        .ok_or_else(|| format!("{flag} is too large"))?
                }
                "--free-space-reserve-mib" => {
                    config.free_space_reserve_bytes = parse_value::<u64>(flag, value)?
                        .checked_mul(1024 * 1024)
                        .ok_or_else(|| format!("{flag} is too large"))?
                }
                _ => return Err(format!("unknown option {flag}")),
            }
            index += 1;
        }
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), String> {
        if self.users == 0 || self.coroutines == 0 || self.requests_per_user == 0 {
            return Err("users, coroutines, and requests per user must be positive".to_owned());
        }
        if self.requests_per_user % DEFAULT_REQUESTS_PER_USER != 0 {
            return Err(format!(
                "requests per user must be a multiple of {DEFAULT_REQUESTS_PER_USER} for an exact per-user mix"
            ));
        }
        let total = self
            .users
            .checked_mul(self.requests_per_user)
            .ok_or_else(|| "total request count overflows usize".to_owned())?;
        if total % 200 != 0 {
            return Err(
                "users * requests per user must be a multiple of 200 for exact old hit/miss mixes"
                    .to_owned(),
            );
        }
        if self.coroutines != self.users {
            return Err(
                "the benchmark runs one coroutine per user; coroutines must equal users".to_owned(),
            );
        }
        if self.coroutines > total {
            return Err("coroutines must not exceed the request count".to_owned());
        }
        if self.sample_stride == 0
            || self.queue_capacity == 0
            || self.batch_size == 0
            || self.batch_timeout.is_zero()
            || self.projection_batch_size == 0
            || self.watermark_interval.is_zero()
        {
            return Err(
                "sample stride, queue, batch, timeout, and intervals must be positive".to_owned(),
            );
        }
        if self.preflight.observation.is_zero() || self.preflight.timeout.is_zero() {
            return Err("preflight observation and timeout must be positive".to_owned());
        }
        if !(0.0..=100.0).contains(&self.preflight.max_cpu_busy_pct)
            || !(0.0..=100.0).contains(&self.preflight.max_disk_busy_pct)
        {
            return Err("preflight CPU and disk percentages must be in 0..=100".to_owned());
        }
        Ok(())
    }

    fn total_requests(&self) -> u64 {
        (self.users * self.requests_per_user) as u64
    }

    fn expected_disk_bytes(&self) -> Result<u64, String> {
        self.total_requests()
            .checked_mul(ESTIMATED_DISK_BYTES_PER_TRANSACTION)
            .and_then(|bytes| bytes.checked_add(self.free_space_reserve_bytes))
            .ok_or_else(|| "disk requirement estimate overflowed".to_owned())
    }

    fn expected_memory_bytes(&self) -> Result<u64, String> {
        let records = self
            .total_requests()
            .checked_add(self.users as u64)
            .ok_or_else(|| "projection record estimate overflowed".to_owned())?;
        records
            .checked_mul(ESTIMATED_PROJECTION_BYTES_PER_TRANSACTION)
            .and_then(|bytes| bytes.checked_add(self.memory_reserve_bytes))
            .ok_or_else(|| "memory requirement estimate overflowed".to_owned())
    }
}

fn print_help() {
    eprintln!(
        "Tokio single-shard transaction-time boundary benchmark\n\
         Defaults: 50,000 users x 200 requests, 50,000 coroutines, four cases\n\
         Cases: 0%, 1%, 5%, and 10% historical requests; old requests are split\n\
         evenly between projected hits and misses.\n\
         Options: --smoke --users N --coroutines N --requests-per-user N\n\
         --sample-stride N --queue-capacity N --batch-size N\n\
         --batch-timeout-ms N --projection-batch-size N --projection-delay-ms N\n\
         --history-delay-ms N --retention-ms N --watermark-interval-ms N\n\
         --output-root PATH --preflight-observation-ms N --preflight-timeout-ms N\n\
         --max-cpu-busy-pct N --max-disk-busy-pct N\n\
         --memory-reserve-mib N --free-space-reserve-mib N"
    );
}

fn parse_value<T: std::str::FromStr>(flag: &str, value: &str) -> Result<T, String> {
    value
        .parse()
        .map_err(|_| format!("invalid value for {flag}: {value}"))
}

struct DbDirectory {
    path: PathBuf,
    removed: bool,
}

impl DbDirectory {
    fn create(root: &Path, case_name: &str, nonce: u128) -> Result<Self, String> {
        let path = root.join(format!("db-{case_name}-{nonce}"));
        fs::create_dir(&path)
            .map_err(|error| format!("cannot create case database {}: {error}", path.display()))?;
        Ok(Self {
            path,
            removed: false,
        })
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn cleanup(&mut self) -> Result<(), String> {
        fs::remove_dir_all(&self.path).map_err(|error| {
            format!(
                "cannot remove case database directory {}: {error}",
                self.path.display()
            )
        })?;
        self.removed = true;
        if self.path.exists() {
            return Err(format!(
                "case database directory still exists after removal: {}",
                self.path.display()
            ));
        }
        Ok(())
    }
}

impl Drop for DbDirectory {
    fn drop(&mut self) {
        if self.removed {
            return;
        }
        if let Err(error) = fs::remove_dir_all(&self.path) {
            if error.kind() != std::io::ErrorKind::NotFound {
                eprintln!(
                    "CASE_DB_CLEANUP_WARNING path={} error={error}",
                    self.path.display()
                );
            }
        }
    }
}

struct CaseRuntime {
    store: AccountStore,
    gate: Arc<AdmissionGate>,
    projection: Arc<MockProjectionStore>,
    progress: Arc<ProjectionProgress>,
    projection_metrics: Arc<ProjectionMetrics>,
    manager: Arc<WatermarkManager>,
    background_failure: watch::Sender<Option<String>>,
    background_failure_rx: watch::Receiver<Option<String>>,
    seed_results: Arc<Vec<TransactionResult>>,
    committed_head: watch::Sender<u64>,
    projector_shutdown: watch::Sender<bool>,
    projector_task: Option<JoinHandle<Result<(), String>>>,
    queue: Option<
        BatchQueue<ledger_time_boundary::AdmittedTransaction, ledger_time_boundary::GuardedReply>,
    >,
    worker: Option<BatchWorker>,
    manager_shutdown: Option<watch::Sender<bool>>,
    manager_task: Option<JoinHandle<Result<(), String>>>,
    store_shutdown: bool,
}

impl CaseRuntime {
    async fn open(config: &Config, path: &Path) -> Result<Self, String> {
        let store =
            AccountStore::open(path, config.users, BalanceMode::Checkpoint, 100_000).await?;
        let projection_capacity = usize::try_from(config.total_requests())
            .ok()
            .and_then(|count| count.checked_add(config.users))
            .ok_or_else(|| "projection capacity does not fit memory address space".to_owned())?;
        let projection = match MockProjectionStore::with_capacity(projection_capacity) {
            Ok(projection) => Arc::new(projection),
            Err(error) => {
                let _ = store.clone().shutdown().await;
                return Err(error);
            }
        };
        let gate = AdmissionGate::new(0);
        let progress = ProjectionProgress::new(0);
        let projection_metrics = Arc::new(ProjectionMetrics::default());
        let manager = Arc::new(WatermarkManager::new(
            Arc::clone(&gate),
            Arc::clone(&progress),
        ));
        let (committed_head, committed_rx) = watch::channel(0_u64);
        let (background_failure, background_failure_rx) = watch::channel(None);
        let (projector_shutdown, projector_shutdown_rx) = watch::channel(false);
        let projector_task = ledger_time_boundary::spawn_projector(
            store.clone(),
            Arc::clone(&projection),
            Arc::clone(&progress),
            Arc::clone(&projection_metrics),
            background_failure.clone(),
            committed_rx,
            projector_shutdown_rx,
            config.projection_batch_size,
            config.projection_delay,
        );
        Ok(Self {
            store,
            gate,
            projection,
            progress,
            projection_metrics,
            manager,
            background_failure,
            background_failure_rx,
            seed_results: Arc::new(Vec::new()),
            committed_head,
            projector_shutdown,
            projector_task: Some(projector_task),
            queue: None,
            worker: None,
            manager_shutdown: None,
            manager_task: None,
            store_shutdown: false,
        })
    }

    async fn prefill_and_publish_initial(
        &mut self,
        seed_timestamp: u64,
        config: &Config,
    ) -> Result<u64, String> {
        let mut transactions = Vec::new();
        transactions
            .try_reserve_exact(config.users)
            .map_err(|error| format!("cannot reserve history seed requests: {error}"))?;
        for user in 0..config.users as u64 {
            transactions.push(Transaction {
                key: TransactionKey {
                    account_id: user,
                    tx_id: 0,
                    transaction_at: seed_timestamp,
                },
                operation: Operation::Credit,
                amount: 1,
                refund_of: None,
            });
        }
        for batch in transactions.chunks(DEFAULT_BATCH_SIZE) {
            let replies = self.store.handle_batch(batch.to_vec()).await?;
            if replies.len() != batch.len() {
                return Err("seed transaction response count did not match the batch".to_owned());
            }
            for (request, reply) in batch.iter().zip(replies) {
                let Reply::Transaction {
                    status,
                    balance,
                    seq,
                    replayed,
                } = reply
                else {
                    return Err(format!(
                        "history seed for account {} unexpectedly conflicted",
                        request.key.account_id
                    ));
                };
                if status != TransactionStatus::Applied || replayed {
                    return Err(format!(
                        "history seed for account {} returned {status:?}, replayed={replayed}",
                        request.key.account_id
                    ));
                }
                let expected_account =
                    u64::try_from(Arc::make_mut(&mut self.seed_results).len())
                        .map_err(|_| "seed result count overflowed account id".to_owned())?;
                if request.key.account_id != expected_account {
                    return Err(format!(
                        "history seed order changed: expected account {expected_account}, received {}",
                        request.key.account_id
                    ));
                }
                Arc::make_mut(&mut self.seed_results).push(TransactionResult {
                    status,
                    balance,
                    seq,
                });
            }
            self.committed_head.send_replace(self.store.latest_seq());
        }
        let target = self.store.latest_seq();
        ledger_time_boundary::wait_for_projection_with_timeout(
            &self.progress,
            target,
            PROJECTION_TIMEOUT,
        )
        .await?;
        if self.projection.progress() != target {
            return Err(format!(
                "projection destination reached {}, expected {target}",
                self.projection.progress()
            ));
        }

        let candidate = ledger_time_boundary::unix_time_micros()
            .saturating_sub(u64::try_from(config.retention.as_micros()).unwrap_or(u64::MAX));
        if candidate <= seed_timestamp {
            return Err(format!(
                "initial watermark candidate {candidate} does not cover seed timestamp {seed_timestamp}"
            ));
        }
        if !self.manager.advance_once(candidate, &self.store).await? {
            return Err("initial watermark did not advance".to_owned());
        }
        if self.store.persisted_projected_before().await? != Some(candidate) {
            return Err("persisted watermark did not match the published boundary".to_owned());
        }
        if self.gate.watermark()? != candidate {
            return Err("in-memory watermark did not match the persisted boundary".to_owned());
        }
        Ok(candidate)
    }

    fn start_queue(&mut self, config: &Config) -> Result<(), String> {
        let (queue, worker) = ledger_time_boundary::spawn_commit_queue(
            self.store.clone(),
            self.committed_head.clone(),
            config.queue_capacity,
            config.batch_size,
            config.batch_timeout,
        )?;
        self.queue = Some(queue);
        self.worker = Some(worker);
        Ok(())
    }

    fn start_manager(&mut self, config: &Config) {
        let (shutdown, shutdown_rx) = watch::channel(false);
        self.manager_shutdown = Some(shutdown);
        let manager = Arc::clone(&self.manager);
        let store = self.store.clone();
        let retention = config.retention;
        let interval = config.watermark_interval;
        let background_failure = self.background_failure.clone();
        self.manager_task = Some(tokio::spawn(async move {
            let result = manager
                .run_periodic(store, retention, interval, shutdown_rx)
                .await;
            if let Err(error) = &result {
                background_failure.send_replace(Some(format!("watermark manager failed: {error}")));
            }
            result
        }));
    }

    async fn shutdown(&mut self) -> Result<(), String> {
        let mut first_error = None;
        if let Some(shutdown) = self.manager_shutdown.take() {
            shutdown.send_replace(true);
        }
        if let Some(task) = self.manager_task.take() {
            match task.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    first_error.get_or_insert(error);
                }
                Err(error) => {
                    first_error
                        .get_or_insert_with(|| format!("watermark manager task failed: {error}"));
                }
            }
        }
        if let Err(error) = self.check_watermark_consistency().await {
            first_error.get_or_insert(error);
        }
        self.queue.take();
        if let Some(worker) = self.worker.take() {
            if let Err(error) = worker.join().await {
                first_error.get_or_insert(error);
            }
        }
        self.projector_shutdown.send_replace(true);
        if let Some(task) = self.projector_task.take() {
            match task.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    first_error.get_or_insert(error);
                }
                Err(error) => {
                    first_error.get_or_insert_with(|| format!("projector task failed: {error}"));
                }
            }
        }
        if !self.store_shutdown {
            self.store_shutdown = true;
            if let Err(error) = self.store.clone().shutdown().await {
                first_error.get_or_insert(error);
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn check_watermark_consistency(&self) -> Result<(), String> {
        if let Some(persisted) = self.store.persisted_projected_before().await? {
            let published = self.gate.watermark()?;
            if persisted != published {
                return Err(format!(
                    "persisted watermark {persisted} differs from published watermark {published} after manager stop"
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct LatencySample {
    stages: RequestStages,
}

#[derive(Default)]
struct ClientStats {
    completed: u64,
    fresh: u64,
    historical: u64,
    status_counts: BTreeMap<String, u64>,
    fresh_samples: Vec<LatencySample>,
    historical_samples: Vec<LatencySample>,
    historical_hit_samples: Vec<LatencySample>,
    historical_miss_samples: Vec<LatencySample>,
}

impl ClientStats {
    fn record(
        &mut self,
        outcome: ledger_time_boundary::RequestOutcome,
        historical_request: bool,
        sampled: bool,
        logical_id: u64,
        expected_seed_result: Option<TransactionResult>,
    ) -> Result<(), String> {
        let stages = outcome.stages;
        let reply = outcome.reply;
        self.completed = self
            .completed
            .checked_add(1)
            .ok_or_else(|| "completed request count overflow".to_owned())?;
        let status = match (historical_request, reply) {
            (true, RoutedReply::HistoricalHit(result)) if expected_seed_result == Some(result) => {
                self.historical += 1;
                "old_hit"
            }
            (true, RoutedReply::HistoricalHit(result)) => {
                return Err(format!(
                    "historical hit for request {logical_id} returned {result:?}, expected {expected_seed_result:?}"
                ));
            }
            (true, RoutedReply::HistoricalMiss) => {
                self.historical += 1;
                "old_miss"
            }
            (true, RoutedReply::Conflict) => {
                self.historical += 1;
                "old_conflict"
            }
            (
                false,
                RoutedReply::Commit(Reply::Transaction {
                    status, replayed, ..
                }),
            ) => {
                self.fresh += 1;
                match (status, replayed) {
                    (TransactionStatus::Applied, true) => "fresh_duplicate_replay",
                    (TransactionStatus::Applied, false) => "fresh_applied",
                    (TransactionStatus::InsufficientFunds, _) => "fresh_insufficient_funds",
                    (TransactionStatus::CreditOverflow, _) => "fresh_credit_overflow",
                    (TransactionStatus::InvalidAmount, _) => "fresh_invalid_amount",
                    (TransactionStatus::InvalidRefund, _) => "fresh_invalid_refund",
                    (TransactionStatus::RefundAlreadyUsed, _) => "fresh_refund_already_used",
                }
            }
            (false, RoutedReply::Commit(Reply::Conflict)) => {
                self.fresh += 1;
                "fresh_conflict"
            }
            (false, RoutedReply::Conflict) => {
                return Err(format!(
                    "fresh request {logical_id} was routed to historical conflict handling"
                ));
            }
            (true, RoutedReply::Commit(_)) => {
                return Err(format!(
                    "historical request {logical_id} was routed to the commit worker"
                ));
            }
            (false, RoutedReply::HistoricalHit(_) | RoutedReply::HistoricalMiss) => {
                return Err(format!(
                    "fresh request {logical_id} was routed to historical storage"
                ));
            }
        };
        *self.status_counts.entry(status.to_owned()).or_default() += 1;
        if sampled {
            let sample = LatencySample { stages };
            if historical_request {
                self.historical_samples.push(sample);
                match status {
                    "old_hit" => self.historical_hit_samples.push(sample),
                    "old_miss" => self.historical_miss_samples.push(sample),
                    _ => {}
                }
            } else {
                self.fresh_samples.push(sample);
            }
        }
        Ok(())
    }

    fn merge(&mut self, mut other: ClientStats) -> Result<(), String> {
        self.completed = self
            .completed
            .checked_add(other.completed)
            .ok_or_else(|| "completed request count overflow".to_owned())?;
        self.fresh = self
            .fresh
            .checked_add(other.fresh)
            .ok_or_else(|| "fresh request count overflow".to_owned())?;
        self.historical = self
            .historical
            .checked_add(other.historical)
            .ok_or_else(|| "historical request count overflow".to_owned())?;
        for (status, count) in other.status_counts {
            *self.status_counts.entry(status).or_default() += count;
        }
        self.fresh_samples.append(&mut other.fresh_samples);
        self.historical_samples
            .append(&mut other.historical_samples);
        self.historical_hit_samples
            .append(&mut other.historical_hit_samples);
        self.historical_miss_samples
            .append(&mut other.historical_miss_samples);
        Ok(())
    }
}

async fn run_clients(
    config: &Config,
    old_pct: u8,
    seed_timestamp: u64,
    gate: Arc<AdmissionGate>,
    queue: BatchQueue<
        ledger_time_boundary::AdmittedTransaction,
        ledger_time_boundary::GuardedReply,
    >,
    projection: Arc<MockProjectionStore>,
    background_failure: watch::Receiver<Option<String>>,
    seed_results: Arc<Vec<TransactionResult>>,
) -> Result<ClientStats, String> {
    let requests_per_user = config.requests_per_user as u64;
    let mut tasks = JoinSet::new();
    for user in 0..config.users as u64 {
        let gate = Arc::clone(&gate);
        let queue = queue.clone();
        let projection = Arc::clone(&projection);
        let background_failure = background_failure.clone();
        let seed_results = Arc::clone(&seed_results);
        let delay = config.history_delay;
        let sample_stride = config.sample_stride;
        tasks.spawn(async move {
            let mut report = ClientStats::default();
            let mut background_failure = background_failure;
            for request_index in 0..requests_per_user {
                let logical_id = user * requests_per_user + request_index;
                let (historical, hit) = classify_request(user, request_index, old_pct);
                let transaction = if historical && hit {
                    Transaction {
                        key: TransactionKey {
                            account_id: user,
                            tx_id: 0,
                            transaction_at: seed_timestamp,
                        },
                        operation: Operation::Credit,
                        amount: 1,
                        refund_of: None,
                    }
                } else if historical {
                    Transaction {
                        key: TransactionKey {
                            account_id: user,
                            tx_id: requests_per_user + 1 + request_index,
                            transaction_at: seed_timestamp,
                        },
                        operation: Operation::Credit,
                        amount: 1,
                        refund_of: None,
                    }
                } else {
                    Transaction {
                        key: TransactionKey {
                            account_id: user,
                            tx_id: request_index + 1,
                            transaction_at: ledger_time_boundary::unix_time_micros(),
                        },
                        operation: Operation::Credit,
                        amount: 1,
                        refund_of: None,
                    }
                };
                let started = Instant::now();
                let outcome = tokio::select! {
                    biased;
                    error = wait_for_background_failure(&mut background_failure) => {
                        return Err(error);
                    }
                    outcome = ledger_time_boundary::route_request(
                        &gate,
                        &queue,
                        &projection,
                        delay,
                        transaction,
                        started,
                    ) => outcome?,
                };
                let sampled = splitmix64(logical_id) % sample_stride == 0;
                report.record(
                    outcome,
                    historical,
                    sampled,
                    logical_id,
                    if historical && hit {
                        Some(seed_results[user as usize])
                    } else {
                        None
                    },
                )?;
            }
            Ok::<_, String>(report)
        });
    }

    let mut combined = ClientStats::default();
    while let Some(task) = tasks.join_next().await {
        match task {
            Ok(Ok(report)) => combined.merge(report)?,
            Ok(Err(error)) => {
                tasks.abort_all();
                while tasks.join_next().await.is_some() {}
                return Err(error);
            }
            Err(error) => {
                tasks.abort_all();
                while tasks.join_next().await.is_some() {}
                return Err(format!("client coroutine failed: {error}"));
            }
        }
    }
    if combined.completed != config.total_requests() {
        return Err(format!(
            "completed {} requests, expected {}",
            combined.completed,
            config.total_requests(),
        ));
    }
    Ok(combined)
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn classify_request(user: u64, request_index: u64, old_pct: u8) -> (bool, bool) {
    let cycle = DEFAULT_REQUESTS_PER_USER as u64;
    let phase = splitmix64(user) % cycle;
    let slot = ((request_index % cycle) * 37 + phase) % cycle;
    let hit_slots = u64::from(old_pct);
    let old_slots = hit_slots * 2;
    (slot < old_slots, slot < hit_slots)
}

async fn wait_for_background_failure(receiver: &mut watch::Receiver<Option<String>>) -> String {
    loop {
        if let Some(error) = receiver.borrow().clone() {
            return error;
        }
        if receiver.changed().await.is_err() {
            return "background worker failure channel closed".to_owned();
        }
    }
}

struct PeakRssSampler {
    stop: watch::Sender<bool>,
    task: JoinHandle<u64>,
}

impl PeakRssSampler {
    fn start() -> Self {
        let (stop, mut stop_rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            let mut peak = read_rss_bytes().unwrap_or(0);
            let mut ticker = tokio::time::interval(Duration::from_millis(10));
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        if let Some(current) = read_rss_bytes() {
                            peak = peak.max(current);
                        }
                    }
                    changed = stop_rx.changed() => {
                        if changed.is_err() || *stop_rx.borrow() {
                            break;
                        }
                    }
                }
            }
            peak
        });
        Self { stop, task }
    }

    async fn finish(self) -> Result<u64, String> {
        self.stop.send_replace(true);
        self.task
            .await
            .map_err(|error| format!("RSS sampler task failed: {error}"))
    }
}

fn read_rss_bytes() -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|line| line.starts_with("VmRSS:"))?;
    let kib = line.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    kib.checked_mul(1024)
}

#[derive(Default)]
struct IoDelta {
    process_rchar_bytes: u64,
    process_wchar_bytes: u64,
    process_read_bytes: u64,
    process_write_bytes: u64,
    target_device: String,
    target_major_minor: String,
    target_read_bytes: u64,
    target_write_bytes: u64,
    target_busy_ms: u64,
}

impl IoDelta {
    fn from_samples(before: IoSample, after: IoSample) -> Result<Self, String> {
        if before.target_major_minor != after.target_major_minor {
            return Err("target storage device changed during measured window".to_owned());
        }
        Ok(Self {
            process_rchar_bytes: delta(after.process_rchar_bytes, before.process_rchar_bytes),
            process_wchar_bytes: delta(after.process_wchar_bytes, before.process_wchar_bytes),
            process_read_bytes: delta(after.process_read_bytes, before.process_read_bytes),
            process_write_bytes: delta(after.process_write_bytes, before.process_write_bytes),
            target_device: after.target_device,
            target_major_minor: after.target_major_minor,
            target_read_bytes: delta(after.target_read_bytes, before.target_read_bytes),
            target_write_bytes: delta(after.target_write_bytes, before.target_write_bytes),
            target_busy_ms: delta(after.target_busy_ms, before.target_busy_ms),
        })
    }
}

fn delta(after: u64, before: u64) -> u64 {
    after.saturating_sub(before)
}

#[derive(Default)]
struct RocksDelta {
    wal_sync_count: u64,
    wal_bytes: u64,
    writes_with_wal: u64,
    flush_write_bytes: u64,
    compaction_read_bytes: u64,
    compaction_write_bytes: u64,
    stall_micros: u64,
}

fn rocksd_delta_full(
    after: (u64, u64, u64, u64, u64, u64, u64),
    before: (u64, u64, u64, u64, u64, u64, u64),
) -> RocksDelta {
    RocksDelta {
        wal_sync_count: delta(after.0, before.0),
        wal_bytes: delta(after.1, before.1),
        writes_with_wal: delta(after.2, before.2),
        flush_write_bytes: delta(after.3, before.3),
        compaction_read_bytes: delta(after.4, before.4),
        compaction_write_bytes: delta(after.5, before.5),
        stall_micros: delta(after.6, before.6),
    }
}

fn contention_delta(
    after: GateContentionSnapshot,
    before: GateContentionSnapshot,
) -> (u64, u64, u64) {
    (
        delta(after.blocked_admissions, before.blocked_admissions),
        delta(after.blocked_wait_ns, before.blocked_wait_ns),
        if after.blocked_admissions > before.blocked_admissions {
            after.max_blocked_wait_ns
        } else {
            0
        },
    )
}

fn metrics_delta(
    after: crate::ledger_account_store::MetricsSnapshot,
    before: crate::ledger_account_store::MetricsSnapshot,
) -> crate::ledger_account_store::MetricsSnapshot {
    let transaction_sample_start = before
        .transaction_samples_ns
        .len()
        .min(after.transaction_samples_ns.len());
    let balance_sample_start = before
        .balance_samples_ns
        .len()
        .min(after.balance_samples_ns.len());
    crate::ledger_account_store::MetricsSnapshot {
        transactions: delta(after.transactions, before.transactions),
        transaction_latency_ns: delta(after.transaction_latency_ns, before.transaction_latency_ns),
        transaction_samples_ns: after
            .transaction_samples_ns
            .into_iter()
            .skip(transaction_sample_start)
            .collect(),
        balance_queries: delta(after.balance_queries, before.balance_queries),
        balance_latency_ns: delta(after.balance_latency_ns, before.balance_latency_ns),
        balance_samples_ns: after
            .balance_samples_ns
            .into_iter()
            .skip(balance_sample_start)
            .collect(),
        batches: delta(after.batches, before.batches),
        read_build_ns: delta(after.read_build_ns, before.read_build_ns),
        wal_sync_ns: delta(after.wal_sync_ns, before.wal_sync_ns),
        publish_ns: delta(after.publish_ns, before.publish_ns),
        checkpoint_count: delta(after.checkpoint_count, before.checkpoint_count),
        checkpoint_snapshot_ns: delta(after.checkpoint_snapshot_ns, before.checkpoint_snapshot_ns),
        checkpoint_queue_wait_ns: delta(
            after.checkpoint_queue_wait_ns,
            before.checkpoint_queue_wait_ns,
        ),
        checkpoint_chunk_sync_ns: delta(
            after.checkpoint_chunk_sync_ns,
            before.checkpoint_chunk_sync_ns,
        ),
        checkpoint_manifest_sync_ns: delta(
            after.checkpoint_manifest_sync_ns,
            before.checkpoint_manifest_sync_ns,
        ),
        checkpoint_duration_ns: delta(after.checkpoint_duration_ns, before.checkpoint_duration_ns),
        checkpoint_latest_seq: after.checkpoint_latest_seq,
        checkpoint_snapshots_enqueued: delta(
            after.checkpoint_snapshots_enqueued,
            before.checkpoint_snapshots_enqueued,
        ),
    }
}

#[derive(Clone, Copy, Default)]
struct Percentiles {
    p50: u64,
    p95: u64,
    p99: u64,
}

fn percentiles(mut values: Vec<u64>) -> Percentiles {
    if values.is_empty() {
        return Percentiles::default();
    }
    values.sort_unstable();
    Percentiles {
        p50: nearest_rank(&values, 50),
        p95: nearest_rank(&values, 95),
        p99: nearest_rank(&values, 99),
    }
}

fn nearest_rank(values: &[u64], percentile: usize) -> u64 {
    let index = (values.len() * percentile).div_ceil(100).saturating_sub(1);
    values[index]
}

fn request_percentiles(
    samples: &[LatencySample],
    stage: impl Fn(RequestStages) -> u64,
) -> Percentiles {
    percentiles(samples.iter().map(|sample| stage(sample.stages)).collect())
}

fn projection_percentiles(
    samples: &[ProjectionBatchSample],
    stage: impl Fn(ProjectionBatchSample) -> u64,
) -> Percentiles {
    percentiles(samples.iter().map(|sample| stage(*sample)).collect())
}

fn watermark_percentiles(
    samples: &[WatermarkSample],
    stage: impl Fn(WatermarkSample) -> u64,
) -> Percentiles {
    percentiles(samples.iter().map(|sample| stage(*sample)).collect())
}

struct CaseRow {
    case_name: String,
    old_pct: u8,
    requests: u64,
    clients: ClientStats,
    client_wall: Duration,
    settled_wall: Duration,
    cpu_seconds: f64,
    cpu_cores: f64,
    io: IoDelta,
    rocks: RocksDelta,
    projection: Vec<ProjectionBatchSample>,
    watermark: Vec<WatermarkSample>,
    watermark_updates_during_clients: usize,
    fenced_admissions: u64,
    fenced_admission_wait_ns: u64,
    max_fenced_admission_wait_ns: u64,
    expected_final_balance: u64,
    balance_checked_users: usize,
    initial_watermark: u64,
    final_watermark: u64,
    final_sequence: u64,
    peak_rss_bytes: u64,
    database_bytes: u64,
    setup_preflight: PreflightReport,
    preflight: PreflightReport,
    account_metrics: crate::ledger_account_store::MetricsSnapshot,
}

async fn run_case(
    config: &Config,
    old_pct: u8,
    run_id: u128,
    run_log: &mut BufWriter<File>,
) -> Result<CaseRow, String> {
    let case_name = format!("old-{old_pct:02}-pct");
    writeln!(run_log, "CASE_START case={case_name}").map_err(io_error)?;
    run_log.flush().map_err(io_error)?;
    println!("CASE_START case={case_name}");

    let case_nonce = run_id
        .checked_mul(10)
        .and_then(|value| value.checked_add(u128::from(old_pct)))
        .ok_or_else(|| "case database nonce overflow".to_owned())?;
    let mut preflight = config.preflight.clone();
    preflight.min_available_mem_bytes = config.expected_memory_bytes()?;
    preflight.min_free_bytes = config.expected_disk_bytes()?;
    let mut directory = DbDirectory::create(&config.output_root, &case_name, case_nonce)?;
    let setup_preflight = match ledger_preflight::ensure_idle(directory.path(), &preflight) {
        Ok(report) => report,
        Err(error) => {
            let logging = writeln!(
                run_log,
                "PREFLIGHT phase=before_setup status=failed error={}",
                clean_log(&error)
            )
            .map_err(io_error)
            .and_then(|()| run_log.flush().map_err(io_error));
            let cleanup = directory.cleanup();
            let mut message = format!("pre-setup preflight failed: {error}");
            if let Err(logging_error) = logging {
                message.push_str(&format!("; failure log write failed: {logging_error}"));
            }
            if let Err(cleanup_error) = cleanup {
                message.push_str(&format!("; case directory cleanup failed: {cleanup_error}"));
            }
            return Err(message);
        }
    };
    if let Err(error) = log_preflight(run_log, "before_setup", &setup_preflight) {
        let cleanup = directory.cleanup();
        return Err(match cleanup {
            Ok(()) => error,
            Err(cleanup_error) => {
                format!("{error}; case directory cleanup failed: {cleanup_error}")
            }
        });
    }
    let mut runtime = match CaseRuntime::open(config, directory.path()).await {
        Ok(runtime) => runtime,
        Err(error) => {
            let cleanup = directory.cleanup();
            return Err(match cleanup {
                Ok(()) => error,
                Err(cleanup_error) => {
                    format!("{error}; case directory cleanup failed: {cleanup_error}")
                }
            });
        }
    };
    let seed_timestamp = ledger_time_boundary::unix_time_micros()
        .saturating_sub(u64::try_from(config.retention.as_micros()).unwrap_or(u64::MAX))
        .saturating_sub(10_000_000);

    let result = run_case_measured(
        config,
        old_pct,
        &case_name,
        seed_timestamp,
        &directory,
        &mut runtime,
        setup_preflight,
        run_log,
    )
    .await;
    let runtime_cleanup = runtime.shutdown().await;
    drop(runtime);
    let directory_cleanup = directory.cleanup();
    drop(directory);
    let cleanup = match (runtime_cleanup, directory_cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(runtime_error), Err(directory_error)) => Err(format!(
            "{runtime_error}; case directory cleanup failed: {directory_error}"
        )),
    };
    match (result, cleanup) {
        (Ok(row), Ok(())) => {
            writeln!(run_log, "CASE_DONE case={case_name} status=passed").map_err(io_error)?;
            run_log.flush().map_err(io_error)?;
            println!("CASE_DONE case={case_name} status=passed");
            Ok(row)
        }
        (Err(error), Ok(())) => {
            writeln!(
                run_log,
                "CASE_DONE case={case_name} status=failed error={}",
                clean_log(&error)
            )
            .map_err(io_error)?;
            run_log.flush().map_err(io_error)?;
            Err(error)
        }
        (Ok(_), Err(error)) => {
            writeln!(
                run_log,
                "CASE_DONE case={case_name} status=failed cleanup_error={}",
                clean_log(&error)
            )
            .map_err(io_error)?;
            run_log.flush().map_err(io_error)?;
            Err(format!("case cleanup failed: {error}"))
        }
        (Err(error), Err(cleanup_error)) => {
            writeln!(
                run_log,
                "CASE_DONE case={case_name} status=failed error={} cleanup_error={}",
                clean_log(&error),
                clean_log(&cleanup_error)
            )
            .map_err(io_error)?;
            run_log.flush().map_err(io_error)?;
            Err(format!("{error}; cleanup also failed: {cleanup_error}"))
        }
    }
}

async fn run_case_measured(
    config: &Config,
    old_pct: u8,
    case_name: &str,
    seed_timestamp: u64,
    directory: &DbDirectory,
    runtime: &mut CaseRuntime,
    setup_preflight: PreflightReport,
    run_log: &mut BufWriter<File>,
) -> Result<CaseRow, String> {
    let initial_watermark = runtime
        .prefill_and_publish_initial(seed_timestamp, config)
        .await?;

    let mut preflight = config.preflight.clone();
    preflight.min_available_mem_bytes = config.expected_memory_bytes()?;
    preflight.min_free_bytes = config.expected_disk_bytes()?;
    let preflight_report = match ledger_preflight::ensure_idle(directory.path(), &preflight) {
        Ok(report) => report,
        Err(error) => {
            writeln!(
                run_log,
                "PREFLIGHT phase=before_measurement status=failed error={}",
                clean_log(&error)
            )
            .map_err(io_error)?;
            run_log.flush().map_err(io_error)?;
            return Err(format!("pre-measurement preflight failed: {error}"));
        }
    };
    log_preflight(run_log, "before_measurement", &preflight_report)?;

    runtime.start_queue(config)?;
    let queue = runtime
        .queue
        .as_ref()
        .ok_or_else(|| "commit queue was not started".to_owned())?
        .clone();
    let projection_samples_before = runtime.projection_metrics.snapshot()?.len();
    let watermark_samples_before = runtime.manager.metrics().snapshot()?.len();
    let account_metrics_before = runtime.store.metrics();
    let rocks_before = runtime.store.rocksdb_stats();
    let contention_before = runtime.gate.contention_snapshot();
    let io_before = ledger_preflight::sample_io(directory.path())?;
    let cpu_before = ProcessTime::now();
    let measured_started = Instant::now();
    let rss_sampler = PeakRssSampler::start();

    runtime.start_manager(config);
    let client_started = Instant::now();
    let client_result = run_clients(
        config,
        old_pct,
        seed_timestamp,
        Arc::clone(&runtime.gate),
        queue,
        Arc::clone(&runtime.projection),
        runtime.background_failure_rx.clone(),
        Arc::clone(&runtime.seed_results),
    )
    .await;
    let client_finished = Instant::now();
    let clients = client_result?;
    let client_wall = client_finished.duration_since(client_started);
    let contention_after = runtime.gate.contention_snapshot();
    let (fenced_admissions, fenced_admission_wait_ns, max_fenced_admission_wait_ns) =
        contention_delta(contention_after, contention_before);
    let old_requests_per_user = (config.requests_per_user as u64)
        .checked_mul(u64::from(old_pct))
        .ok_or_else(|| "per-user old request count overflowed".to_owned())?
        / 100;
    let expected_final_balance = 1_u64
        .checked_add(config.requests_per_user as u64 - old_requests_per_user)
        .ok_or_else(|| "expected final account balance overflowed".to_owned())?;
    for account_id in 0..config.users as u64 {
        let actual = runtime.store.balance(account_id)?;
        if actual != expected_final_balance {
            return Err(format!(
                "account {account_id} has balance {actual}, expected {expected_final_balance} after client completion"
            ));
        }
    }
    let balance_checked_users = config.users;
    writeln!(
        run_log,
        "BALANCE_CHECK users={balance_checked_users} expected_final_balance={expected_final_balance} status=passed"
    )
    .map_err(io_error)?;
    run_log.flush().map_err(io_error)?;
    let target = runtime.store.latest_seq();
    ledger_time_boundary::wait_for_projection_with_timeout(
        &runtime.progress,
        target,
        PROJECTION_TIMEOUT,
    )
    .await?;
    if runtime.projection.progress() != target {
        return Err(format!(
            "projection destination reached {}, expected {target} after client drain",
            runtime.projection.progress()
        ));
    }

    let watermark_metrics = runtime.manager.metrics();
    let deadline = Instant::now()
        .checked_add(config.watermark_interval.saturating_mul(4) + Duration::from_secs(5))
        .ok_or_else(|| "watermark observation deadline overflow".to_owned())?;
    while runtime.gate.watermark()? <= initial_watermark {
        if let Some(error) = runtime.background_failure_rx.borrow().clone() {
            return Err(error);
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "watermark did not advance during the measured window; remained at {initial_watermark}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let watermark_samples_after = watermark_metrics.snapshot()?;
    let watermark_samples = watermark_samples_after
        .into_iter()
        .skip(watermark_samples_before)
        .collect::<Vec<_>>();
    if watermark_samples.is_empty() {
        return Err("watermark advanced without recording a manager sample".to_owned());
    }
    let watermark_updates_during_clients = watermark_samples
        .iter()
        .filter(|sample| {
            sample.completed_at >= client_started && sample.completed_at <= client_finished
        })
        .count();

    // Stop producers and the manager before the final checkpoint drain. The
    // projected target and published boundary have already been verified.
    runtime.stop_manager().await?;
    runtime.stop_queue().await?;
    runtime.stop_projector().await?;
    runtime.shutdown_store().await?;

    let settled_finished = Instant::now();
    let cpu_after = ProcessTime::now();
    let io_after = ledger_preflight::sample_io(directory.path())?;
    let rocks_after = runtime.store.rocksdb_stats();
    let account_metrics_after = runtime.store.metrics();
    let account_metrics_delta = metrics_delta(account_metrics_after, account_metrics_before);
    let projection_samples_after = runtime.projection_metrics.snapshot()?;
    let projection = projection_samples_after
        .into_iter()
        .skip(projection_samples_before)
        .collect::<Vec<_>>();
    if projection.is_empty() {
        return Err("no projection batches ran in the measured window".to_owned());
    }
    if runtime.progress.sequence()? < target {
        return Err(format!(
            "projector progress {} fell below final target {target}",
            runtime.progress.sequence()?
        ));
    }
    if clients.fresh + clients.historical != config.total_requests() {
        return Err("fresh and historical counts do not sum to total requests".to_owned());
    }
    let expected_old = config.total_requests() * u64::from(old_pct) / 100;
    if clients.historical != expected_old {
        return Err(format!(
            "historical request count {} does not match expected {expected_old}",
            clients.historical
        ));
    }
    if clients.status_counts.get("old_hit").copied().unwrap_or(0) != clients.historical / 2
        || clients.status_counts.get("old_miss").copied().unwrap_or(0) != clients.historical / 2
    {
        return Err("historical workload did not produce an even hit/miss split".to_owned());
    }
    if old_pct > 0
        && (clients.historical_hit_samples.is_empty() || clients.historical_miss_samples.is_empty())
    {
        return Err(format!(
            "historical latency sampling missed a stratum: hit_samples={} miss_samples={}",
            clients.historical_hit_samples.len(),
            clients.historical_miss_samples.len()
        ));
    }
    let final_watermark = runtime.gate.watermark()?;
    let persisted_watermark = runtime
        .store
        .persisted_projected_before()
        .await?
        .ok_or_else(|| "persisted watermark metadata is missing".to_owned())?;
    if final_watermark != persisted_watermark {
        return Err(format!(
            "published watermark {final_watermark} differs from persisted watermark {persisted_watermark}"
        ));
    }
    let actual_final_sequence = runtime.store.latest_seq();
    if runtime.projection.progress() != actual_final_sequence {
        return Err(format!(
            "projected sequence {} differs from final sequence {actual_final_sequence}",
            runtime.projection.progress()
        ));
    }

    let peak_rss_bytes = rss_sampler.finish().await?;
    let cpu_elapsed = cpu_after.duration_since(cpu_before);
    let settled_wall = settled_finished.duration_since(measured_started);
    let cpu_seconds = cpu_elapsed.as_secs_f64();
    let cpu_cores = if settled_wall.is_zero() {
        0.0
    } else {
        cpu_seconds / settled_wall.as_secs_f64()
    };
    let database_bytes = directory_size(directory.path())?;
    let watermark_samples = watermark_metrics
        .snapshot()?
        .into_iter()
        .skip(watermark_samples_before)
        .collect::<Vec<_>>();
    Ok(CaseRow {
        case_name: case_name.to_owned(),
        old_pct,
        requests: config.total_requests(),
        clients,
        client_wall,
        settled_wall,
        cpu_seconds,
        cpu_cores,
        io: IoDelta::from_samples(io_before, io_after)?,
        rocks: rocksd_delta_full(rocks_after, rocks_before),
        projection,
        watermark: watermark_samples,
        watermark_updates_during_clients,
        fenced_admissions,
        fenced_admission_wait_ns,
        max_fenced_admission_wait_ns,
        expected_final_balance,
        balance_checked_users,
        initial_watermark,
        final_watermark,
        final_sequence: actual_final_sequence,
        peak_rss_bytes,
        database_bytes,
        setup_preflight,
        preflight: preflight_report,
        account_metrics: account_metrics_delta,
    })
}

impl CaseRuntime {
    async fn stop_manager(&mut self) -> Result<(), String> {
        if let Some(shutdown) = self.manager_shutdown.take() {
            shutdown.send_replace(true);
        }
        if let Some(task) = self.manager_task.take() {
            task.await
                .map_err(|error| format!("watermark manager task failed: {error}"))??;
        }
        self.check_watermark_consistency().await
    }

    async fn stop_queue(&mut self) -> Result<(), String> {
        self.queue.take();
        if let Some(worker) = self.worker.take() {
            worker.join().await?;
        }
        Ok(())
    }

    async fn stop_projector(&mut self) -> Result<(), String> {
        self.projector_shutdown.send_replace(true);
        if let Some(task) = self.projector_task.take() {
            task.await
                .map_err(|error| format!("projector task failed: {error}"))??;
        }
        Ok(())
    }

    async fn shutdown_store(&mut self) -> Result<(), String> {
        if !self.store_shutdown {
            self.store_shutdown = true;
            self.store.clone().shutdown().await?;
        }
        Ok(())
    }
}

fn directory_size(path: &Path) -> Result<u64, String> {
    let mut total = 0_u64;
    let entries = fs::read_dir(path)
        .map_err(|error| format!("cannot list database directory {}: {error}", path.display()))?;
    for entry in entries {
        let entry =
            entry.map_err(|error| format!("cannot read database directory entry: {error}"))?;
        let metadata = entry.metadata().map_err(|error| {
            format!(
                "cannot stat database file {}: {error}",
                entry.path().display()
            )
        })?;
        if metadata.is_dir() {
            total = total
                .checked_add(directory_size(&entry.path())?)
                .ok_or_else(|| "database directory byte count overflow".to_owned())?;
        } else {
            total = total
                .checked_add(metadata.len())
                .ok_or_else(|| "database directory byte count overflow".to_owned())?;
        }
    }
    Ok(total)
}

fn row_header() -> Vec<String> {
    let mut header: Vec<String> = [
        "case",
        "users",
        "coroutines",
        "requests_per_user",
        "requested_old_pct",
        "requests",
        "fresh_completed",
        "old_completed",
        "fresh_rps",
        "old_rps",
        "total_rps",
        "client_wall_s",
        "settled_wall_s",
        "old_hits",
        "old_misses",
        "old_conflicts",
        "fresh_applied",
        "fresh_insufficient_funds",
        "fresh_credit_overflow",
        "fresh_invalid_amount",
        "fresh_invalid_refund",
        "fresh_refund_already_used",
        "fresh_duplicate_replay",
        "fresh_conflict",
        "latency_sample_stride",
        "fresh_latency_sample_count",
        "old_latency_sample_count",
        "old_hit_latency_sample_count",
        "old_miss_latency_sample_count",
        "watermark_initial_us",
        "watermark_final_us",
        "watermark_updates",
        "watermark_updates_during_clients",
        "fenced_admissions",
        "fenced_admission_wait_ns",
        "fenced_admission_wait_max_ns",
        "expected_final_balance",
        "balance_checked_users",
        "watermark_target_sequence_final",
        "projection_final_sequence",
        "projection_batches",
        "projection_records",
        "process_cpu_s",
        "process_cpu_cores",
        "peak_rss_bytes",
        "database_bytes",
        "process_rchar_bytes",
        "process_wchar_bytes",
        "process_read_bytes",
        "process_write_bytes",
        "target_device",
        "target_major_minor",
        "target_read_bytes",
        "target_write_bytes",
        "target_busy_ms",
        "rocks_wal_sync_count",
        "rocks_wal_bytes",
        "rocks_writes_with_wal",
        "rocks_flush_write_bytes",
        "rocks_compaction_read_bytes",
        "rocks_compaction_write_bytes",
        "rocks_stall_micros",
        "store_transactions",
        "store_batches",
        "store_read_build_ns",
        "store_wal_sync_ns",
        "store_publish_ns",
        "checkpoint_count",
        "checkpoint_snapshot_ns",
        "checkpoint_queue_wait_ns",
        "checkpoint_chunk_sync_ns",
        "checkpoint_manifest_sync_ns",
        "checkpoint_duration_ns",
        "checkpoint_latest_seq",
        "checkpoint_snapshots_enqueued",
        "setup_preflight_attempts",
        "setup_preflight_cpu_busy_pct",
        "setup_preflight_disk_busy_pct",
        "setup_preflight_mem_available_bytes",
        "setup_preflight_free_bytes",
        "setup_preflight_device",
        "measurement_preflight_attempts",
        "measurement_preflight_cpu_busy_pct",
        "measurement_preflight_disk_busy_pct",
        "measurement_preflight_mem_available_bytes",
        "measurement_preflight_free_bytes",
        "measurement_preflight_device",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    let fresh_stages = [
        "total",
        "admission",
        "enqueue",
        "queue",
        "batch",
        "handler",
        "response",
    ];
    for stage in fresh_stages {
        push_latency_header(&mut header, &format!("fresh_{stage}"));
    }
    for stage in ["total", "admission", "old_lookup"] {
        push_latency_header(&mut header, &format!("old_{stage}"));
    }
    for stratum in ["old_hit", "old_miss"] {
        for stage in ["total", "admission", "old_lookup"] {
            push_latency_header(&mut header, &format!("{stratum}_{stage}"));
        }
    }
    for stage in ["read", "apply", "delay", "dispatch", "total"] {
        push_latency_header(&mut header, &format!("projection_{stage}"));
    }
    for stage in ["fence_wait", "projection_wait", "persist", "total"] {
        push_latency_header(&mut header, &format!("watermark_{stage}"));
    }
    header
}

fn push_latency_header(header: &mut Vec<String>, prefix: &str) {
    for percentile in ["p50_ns", "p95_ns", "p99_ns"] {
        header.push(format!("{prefix}_{percentile}"));
    }
}

fn row_values(config: &Config, row: &CaseRow) -> Vec<String> {
    let mut values = vec![
        row.case_name.clone(),
        config.users.to_string(),
        config.coroutines.to_string(),
        config.requests_per_user.to_string(),
        row.old_pct.to_string(),
        row.requests.to_string(),
        row.clients.fresh.to_string(),
        row.clients.historical.to_string(),
        rate(row.clients.fresh, row.client_wall),
        rate(row.clients.historical, row.client_wall),
        rate(row.clients.completed, row.client_wall),
        seconds(row.client_wall),
        seconds(row.settled_wall),
        status(&row.clients, "old_hit"),
        status(&row.clients, "old_miss"),
        status(&row.clients, "old_conflict"),
        status(&row.clients, "fresh_applied"),
        status(&row.clients, "fresh_insufficient_funds"),
        status(&row.clients, "fresh_credit_overflow"),
        status(&row.clients, "fresh_invalid_amount"),
        status(&row.clients, "fresh_invalid_refund"),
        status(&row.clients, "fresh_refund_already_used"),
        status(&row.clients, "fresh_duplicate_replay"),
        status(&row.clients, "fresh_conflict"),
        config.sample_stride.to_string(),
        row.clients.fresh_samples.len().to_string(),
        row.clients.historical_samples.len().to_string(),
        row.clients.historical_hit_samples.len().to_string(),
        row.clients.historical_miss_samples.len().to_string(),
        row.initial_watermark.to_string(),
        row.final_watermark.to_string(),
        row.watermark.len().to_string(),
        row.watermark_updates_during_clients.to_string(),
        row.fenced_admissions.to_string(),
        row.fenced_admission_wait_ns.to_string(),
        row.max_fenced_admission_wait_ns.to_string(),
        row.expected_final_balance.to_string(),
        row.balance_checked_users.to_string(),
        row.watermark.last().map_or_else(
            || "N/A".to_owned(),
            |sample| sample.target_sequence.to_string(),
        ),
        row.final_sequence.to_string(),
        row.projection.len().to_string(),
        row.projection
            .iter()
            .map(|sample| sample.records as u64)
            .sum::<u64>()
            .to_string(),
        format!("{:.6}", row.cpu_seconds),
        format!("{:.6}", row.cpu_cores),
        row.peak_rss_bytes.to_string(),
        row.database_bytes.to_string(),
        row.io.process_rchar_bytes.to_string(),
        row.io.process_wchar_bytes.to_string(),
        row.io.process_read_bytes.to_string(),
        row.io.process_write_bytes.to_string(),
        row.io.target_device.clone(),
        row.io.target_major_minor.clone(),
        row.io.target_read_bytes.to_string(),
        row.io.target_write_bytes.to_string(),
        row.io.target_busy_ms.to_string(),
        row.rocks.wal_sync_count.to_string(),
        row.rocks.wal_bytes.to_string(),
        row.rocks.writes_with_wal.to_string(),
        row.rocks.flush_write_bytes.to_string(),
        row.rocks.compaction_read_bytes.to_string(),
        row.rocks.compaction_write_bytes.to_string(),
        row.rocks.stall_micros.to_string(),
        row.account_metrics.transactions.to_string(),
        row.account_metrics.batches.to_string(),
        row.account_metrics.read_build_ns.to_string(),
        row.account_metrics.wal_sync_ns.to_string(),
        row.account_metrics.publish_ns.to_string(),
        row.account_metrics.checkpoint_count.to_string(),
        row.account_metrics.checkpoint_snapshot_ns.to_string(),
        row.account_metrics.checkpoint_queue_wait_ns.to_string(),
        row.account_metrics.checkpoint_chunk_sync_ns.to_string(),
        row.account_metrics.checkpoint_manifest_sync_ns.to_string(),
        row.account_metrics.checkpoint_duration_ns.to_string(),
        row.account_metrics.checkpoint_latest_seq.to_string(),
        row.account_metrics
            .checkpoint_snapshots_enqueued
            .to_string(),
    ];
    values.extend(preflight_values(&row.setup_preflight));
    values.extend(preflight_values(&row.preflight));

    let fresh_stages: [(&str, fn(RequestStages) -> u64); 7] = [
        ("total", |s| s.overall_ns),
        ("admission", |s| s.admission_ns),
        ("enqueue", |s| s.enqueue_ns),
        ("queue", |s| s.queue_ns),
        ("batch", |s| s.batch_ns),
        ("handler", |s| s.handler_ns),
        ("response", |s| s.response_ns),
    ];
    for (name, get) in fresh_stages {
        push_latency_values(
            &mut values,
            request_percentiles(&row.clients.fresh_samples, get),
        );
        let _ = name;
    }
    for get in [
        (|s: RequestStages| s.overall_ns) as fn(RequestStages) -> u64,
        |s| s.admission_ns,
        |s| s.old_lookup_ns,
    ] {
        push_latency_values(
            &mut values,
            request_percentiles(&row.clients.historical_samples, get),
        );
    }
    for samples in [
        &row.clients.historical_hit_samples,
        &row.clients.historical_miss_samples,
    ] {
        for get in [
            (|s: RequestStages| s.overall_ns) as fn(RequestStages) -> u64,
            |s| s.admission_ns,
            |s| s.old_lookup_ns,
        ] {
            push_latency_values(&mut values, request_percentiles(samples, get));
        }
    }
    for get in [
        (|s: ProjectionBatchSample| s.read_ns) as fn(ProjectionBatchSample) -> u64,
        |s| s.apply_ns,
        |s| s.delay_ns,
        |s| s.dispatch_ns,
        |s| s.total_ns,
    ] {
        push_latency_values(&mut values, projection_percentiles(&row.projection, get));
    }
    for get in [
        (|s: WatermarkSample| s.fence_wait_ns) as fn(WatermarkSample) -> u64,
        |s| s.projection_wait_ns,
        |s| s.persist_ns,
        |s| s.total_ns,
    ] {
        push_latency_values(&mut values, watermark_percentiles(&row.watermark, get));
    }
    values
}

fn preflight_values(report: &PreflightReport) -> Vec<String> {
    vec![
        report.attempts.to_string(),
        format!("{:.6}", report.cpu_busy_pct),
        format!("{:.6}", report.disk_busy_pct),
        report.mem_available_bytes.to_string(),
        report.free_bytes.to_string(),
        report.device.clone(),
    ]
}

fn push_latency_values(values: &mut Vec<String>, summary: Percentiles) {
    values.push(summary.p50.to_string());
    values.push(summary.p95.to_string());
    values.push(summary.p99.to_string());
}

fn status(stats: &ClientStats, name: &str) -> String {
    stats
        .status_counts
        .get(name)
        .copied()
        .unwrap_or(0)
        .to_string()
}

fn seconds(value: Duration) -> String {
    format!("{:.6}", value.as_secs_f64())
}

fn rate(count: u64, duration: Duration) -> String {
    if duration.is_zero() {
        "0.000".to_owned()
    } else {
        format!("{:.3}", count as f64 / duration.as_secs_f64())
    }
}

fn csv_line(values: &[String]) -> String {
    values
        .iter()
        .map(|value| {
            if value.contains([',', '"', '\n', '\r']) {
                format!("\"{}\"", value.replace('"', "\"\""))
            } else {
                value.clone()
            }
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn io_error(error: std::io::Error) -> String {
    error.to_string()
}

fn clean_log(value: &str) -> String {
    value.replace(['\n', '\r'], " ")
}

fn log_preflight(
    run_log: &mut BufWriter<File>,
    phase: &str,
    report: &PreflightReport,
) -> Result<(), String> {
    writeln!(
        run_log,
        "PREFLIGHT phase={phase} status=passed attempts={} cpu_busy_pct={:.6} disk_busy_pct={:.6} mem_available_bytes={} free_bytes={} device={} major_minor={}",
        report.attempts,
        report.cpu_busy_pct,
        report.disk_busy_pct,
        report.mem_available_bytes,
        report.free_bytes,
        clean_log(&report.device),
        clean_log(&report.major_minor)
    )
    .map_err(io_error)?;
    run_log.flush().map_err(io_error)
}

fn unix_run_id() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0)
}

pub fn run_from_args() -> Result<(), String> {
    let config = Config::parse()?;
    fs::create_dir_all(&config.output_root).map_err(|error| {
        format!(
            "cannot create output root {}: {error}",
            config.output_root.display()
        )
    })?;
    let run_id = unix_run_id();
    let csv_path = config.output_root.join(format!("run-{run_id}-results.csv"));
    let log_path = config.output_root.join(format!("run-{run_id}.log"));
    let csv_file = File::create(&csv_path)
        .map_err(|error| format!("cannot create {}: {error}", csv_path.display()))?;
    let log_file = File::create(&log_path)
        .map_err(|error| format!("cannot create {}: {error}", log_path.display()))?;
    let mut csv = BufWriter::new(csv_file);
    let mut run_log = BufWriter::new(log_file);

    let header = row_header();
    writeln!(csv, "{}", csv_line(&header)).map_err(io_error)?;
    let estimated_memory = config.expected_memory_bytes()?;
    let estimated_disk = config.expected_disk_bytes()?;
    writeln!(
        run_log,
        "RUN_START users={} coroutines={} requests_per_user={} requests={} queue_capacity={} batch_size={} batch_timeout_ms={} projection_batch_size={} projection_delay_ms={} old_lookup_delay_ms={} retention_ms={} watermark_interval_ms={} sample_stride={} estimated_projection_memory_bytes={} estimated_disk_bytes={} smoke={}",
        config.users,
        config.coroutines,
        config.requests_per_user,
        config.total_requests(),
        config.queue_capacity,
        config.batch_size,
        config.batch_timeout.as_millis(),
        config.projection_batch_size,
        config.projection_delay.as_millis(),
        config.history_delay.as_millis(),
        config.retention.as_millis(),
        config.watermark_interval.as_millis(),
        config.sample_stride,
        estimated_memory,
        estimated_disk,
        config.smoke
    )
    .map_err(io_error)?;
    run_log.flush().map_err(io_error)?;
    println!(
        "RUN_START requests_per_case={} cases={:?} estimated_projection_memory_bytes={} estimated_disk_bytes={}",
        config.total_requests(),
        CASES,
        estimated_memory,
        estimated_disk
    );

    let runtime = Builder::new_multi_thread()
        .worker_threads(4)
        .thread_name("ledger-time-boundary")
        .enable_all()
        .build()
        .map_err(|error| format!("cannot build Tokio runtime: {error}"))?;
    let mut completed_cases = 0_usize;
    let result = runtime.block_on(async {
        for old_pct in CASES {
            let row = run_case(&config, old_pct, run_id, &mut run_log).await?;
            let values = row_values(&config, &row);
            if values.len() != header.len() {
                return Err(format!(
                    "CSV row has {} fields but header has {}",
                    values.len(),
                    header.len()
                ));
            }
            writeln!(csv, "{}", csv_line(&values)).map_err(io_error)?;
            csv.flush().map_err(io_error)?;
            writeln!(
                run_log,
                "CASE_RESULT case={} requests={} fresh={} old={} old_hits={} old_misses={} fresh_rps={} old_rps={} client_wall_s={} settled_wall_s={} watermark={} watermark_updates={} fenced_admissions={} fence_wait_ns={} fence_contention_observed={} balance_check_users={} expected_final_balance={} projection_seq={} cpu_s={:.6} cpu_cores={:.4} device={} rocks_wal_bytes={} peak_rss_bytes={} preflight_attempts={}",
                row.case_name,
                row.requests,
                row.clients.fresh,
                row.clients.historical,
                status(&row.clients, "old_hit"),
                status(&row.clients, "old_miss"),
                rate(row.clients.fresh, row.client_wall),
                rate(row.clients.historical, row.client_wall),
                seconds(row.client_wall),
                seconds(row.settled_wall),
                row.final_watermark,
                row.watermark.len(),
                row.fenced_admissions,
                row.fenced_admission_wait_ns,
                row.fenced_admissions != 0,
                row.balance_checked_users,
                row.expected_final_balance,
                row.final_sequence,
                row.cpu_seconds,
                row.cpu_cores,
                row.io.target_device,
                row.rocks.wal_bytes,
                row.peak_rss_bytes,
                row.preflight.attempts
            )
            .map_err(io_error)?;
            run_log.flush().map_err(io_error)?;
            completed_cases += 1;
        }
        Ok::<_, String>(())
    });

    match result {
        Ok(()) => {
            if completed_cases != CASES.len() {
                return Err(format!(
                    "completed {completed_cases} cases, expected {}",
                    CASES.len()
                ));
            }
            writeln!(
                run_log,
                "RUN_COMPLETE status=passed results_csv={}",
                csv_path.display()
            )
            .map_err(io_error)?;
            csv.flush().map_err(io_error)?;
            run_log.flush().map_err(io_error)?;
            if is_full_default(&config) && completed_cases == CASES.len() {
                archive_full_run(&csv_path, &log_path)?;
            }
            println!(
                "RUN_COMPLETE status=passed results_csv={} run_log={}",
                csv_path.display(),
                log_path.display()
            );
            Ok(())
        }
        Err(error) => {
            writeln!(
                run_log,
                "RUN_COMPLETE status=failed error={}",
                clean_log(&error)
            )
            .map_err(io_error)?;
            csv.flush().map_err(io_error)?;
            run_log.flush().map_err(io_error)?;
            Err(format!(
                "{error}; partial results are in {} and {}",
                csv_path.display(),
                log_path.display()
            ))
        }
    }
}

fn is_full_default(config: &Config) -> bool {
    !config.smoke
        && config.users == DEFAULT_USERS
        && config.coroutines == DEFAULT_COROUTINES
        && config.requests_per_user == DEFAULT_REQUESTS_PER_USER
        && config.sample_stride == DEFAULT_SAMPLE_STRIDE
        && config.queue_capacity == DEFAULT_QUEUE_CAPACITY
        && config.batch_size == DEFAULT_BATCH_SIZE
        && config.batch_timeout == Duration::from_millis(DEFAULT_BATCH_TIMEOUT_MS)
        && config.projection_batch_size == DEFAULT_PROJECTION_BATCH_SIZE
        && config.projection_delay == Duration::from_millis(DEFAULT_PROJECTION_DELAY_MS)
        && config.history_delay == Duration::from_millis(DEFAULT_HISTORY_DELAY_MS)
        && config.retention == Duration::from_millis(DEFAULT_RETENTION_MS)
        && config.watermark_interval == Duration::from_millis(DEFAULT_WATERMARK_INTERVAL_MS)
        && config.output_root == PathBuf::from(DEFAULT_OUTPUT_ROOT)
        && config.preflight.observation == Duration::from_millis(DEFAULT_PREFLIGHT_OBSERVATION_MS)
        && config.preflight.timeout == Duration::from_millis(DEFAULT_PREFLIGHT_TIMEOUT_MS)
        && config.preflight.max_cpu_busy_pct == DEFAULT_MAX_CPU_BUSY_PCT
        && config.preflight.max_disk_busy_pct == DEFAULT_MAX_DISK_BUSY_PCT
        && config.memory_reserve_bytes == DEFAULT_MEMORY_RESERVE_BYTES
        && config.free_space_reserve_bytes == DEFAULT_FREE_SPACE_RESERVE_BYTES
        && CASES == [0, 1, 5, 10]
}

fn archive_full_run(csv_path: &Path, log_path: &Path) -> Result<(), String> {
    let archive = Path::new("benches/data/ledger_time_boundary");
    fs::create_dir_all(archive).map_err(|error| {
        format!(
            "cannot create archive directory {}: {error}",
            archive.display()
        )
    })?;
    fs::copy(csv_path, archive.join("full-10m-results.csv"))
        .map_err(|error| format!("cannot archive full result CSV: {error}"))?;
    fs::copy(log_path, archive.join("full-10m-run.log"))
        .map_err(|error| format!("cannot archive full run log: {error}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;

    #[test]
    fn only_the_exact_default_measurement_config_can_replace_the_full_run_archive() {
        let mut config = Config::default();
        assert!(is_full_default(&config));

        config.history_delay += Duration::from_millis(1);
        assert!(!is_full_default(&config));

        let mut config = Config::default();
        config.preflight.max_cpu_busy_pct += 1.0;
        assert!(!is_full_default(&config));

        let mut config = Config::default();
        config.output_root.push("alternate-device");
        assert!(!is_full_default(&config));
    }

    #[tokio::test]
    async fn background_worker_failure_is_observable_while_a_request_is_waiting() {
        let (sender, mut receiver) = watch::channel(None);
        let waiting = tokio::spawn(async move { wait_for_background_failure(&mut receiver).await });
        sender.send_replace(Some("projector failed: injected error".to_owned()));
        let error = tokio::time::timeout(Duration::from_millis(100), waiting)
            .await
            .expect("failure watch should wake the request")
            .expect("watch helper task should not panic");
        assert_eq!(error, "projector failed: injected error");
    }

    #[test]
    fn per_user_permutation_has_exact_mix_and_sampling_covers_each_stratum() {
        const USERS: u64 = 2_000;
        const REQUESTS_PER_USER: u64 = DEFAULT_REQUESTS_PER_USER as u64;
        for old_pct in [1_u8, 5, 10] {
            let mut hits_by_position = vec![0_u64; REQUESTS_PER_USER as usize];
            let mut misses_by_position = vec![0_u64; REQUESTS_PER_USER as usize];
            let mut sampled_hits = 0_u64;
            let mut sampled_misses = 0_u64;
            for user in 0..USERS {
                let mut user_hits = 0_u64;
                let mut user_misses = 0_u64;
                for request_index in 0..REQUESTS_PER_USER {
                    let logical_id = user * REQUESTS_PER_USER + request_index;
                    let (historical, hit) = classify_request(user, request_index, old_pct);
                    if historical {
                        if hit {
                            user_hits += 1;
                            hits_by_position[request_index as usize] += 1;
                            if splitmix64(logical_id) % DEFAULT_SAMPLE_STRIDE == 0 {
                                sampled_hits += 1;
                            }
                        } else {
                            user_misses += 1;
                            misses_by_position[request_index as usize] += 1;
                            if splitmix64(logical_id) % DEFAULT_SAMPLE_STRIDE == 0 {
                                sampled_misses += 1;
                            }
                        }
                    }
                }
                assert_eq!(user_hits, u64::from(old_pct));
                assert_eq!(user_misses, u64::from(old_pct));
            }
            assert!(hits_by_position.iter().all(|count| *count > 0));
            assert!(misses_by_position.iter().all(|count| *count > 0));
            assert!(sampled_hits > 0, "{old_pct}% case sampled no hits");
            assert!(sampled_misses > 0, "{old_pct}% case sampled no misses");
        }
    }
}
