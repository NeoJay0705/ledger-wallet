//! Paired Tokio benchmark for durable projection progress and safe RocksDB GC.

use crate::ledger_account_store::{
    AccountStore, BalanceMode, GcStepOutcome, IndexLookupBatchMetrics, IndexLookupConfig,
    IndexLookupMode, Operation, RefundHistory, Reply, Transaction, TransactionKey,
    TransactionStatus,
};
use crate::ledger_preflight::{self, IoSample, PreflightConfig, PreflightReport};
use crate::ledger_projection_worker::{HistoricalLookup, MockProjectionStore};
use crate::ledger_time_boundary::{
    self, AdmissionGate, GuardedReply, ProjectionProgress, RoutedReply, WatermarkManager,
    WatermarkSample,
};
use crate::request_batch_queue::{BatchQueue, BatchWorker};
use cpu_time::ProcessTime;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::runtime::Builder;
use tokio::sync::{watch, Barrier};
use tokio::task::{JoinHandle, JoinSet};

const DEFAULT_USERS: usize = 50_000;
const DEFAULT_REQUESTS_PER_USER: usize = 200;
const DEFAULT_SAMPLE_STRIDE: u64 = 64;
const DEFAULT_QUEUE_CAPACITY: usize = 50_000;
const DEFAULT_BATCH_SIZE: usize = 2_048;
const DEFAULT_BATCH_TIMEOUT_MS: u64 = 5;
const DEFAULT_PROJECTION_BATCH_SIZE: usize = 256;
const DEFAULT_GC_BATCH_SIZE: usize = 256;
const DEFAULT_GC_INTERVAL_MS: u64 = 100;
const DEFAULT_RETENTION_MS: u64 = 500;
const DEFAULT_WATERMARK_INTERVAL_MS: u64 = 100;
const DEFAULT_OLD_LOOKUP_DELAY_MS: u64 = 10;
const DEFAULT_CHECKPOINT_QUANTITY: u64 = 100_000;
const DEFAULT_OUTPUT_ROOT: &str = "target/ledger-pipeline-trials";
const DEFAULT_ARCHIVE_ROOT: &str = "benches/data/ledger_pipeline";
const DEFAULT_PREFLIGHT_OBSERVATION_MS: u64 = 3_000;
const DEFAULT_PREFLIGHT_TIMEOUT_MS: u64 = 60_000;
const DEFAULT_MAX_CPU_BUSY_PCT: f64 = 10.0;
const DEFAULT_MAX_DISK_BUSY_PCT: f64 = 5.0;
const DEFAULT_MEMORY_RESERVE_BYTES: u64 = 768 * 1024 * 1024;
const DEFAULT_FREE_SPACE_RESERVE_BYTES: u64 = 1024 * 1024 * 1024;
const ESTIMATED_DISK_BYTES_PER_RECORD: u64 = 768;
const ESTIMATED_DESTINATION_BYTES_PER_RECORD: u64 = 256;
const SEED_TRANSACTIONS_PER_USER: usize = 3;
const RUN_CASES: [(BalanceMode, bool, u8); 6] = [
    (BalanceMode::PerBatch, false, 0),
    (BalanceMode::PerBatch, true, 0),
    (BalanceMode::PerBatch, true, 5),
    (BalanceMode::Checkpoint, false, 0),
    (BalanceMode::Checkpoint, true, 0),
    (BalanceMode::Checkpoint, true, 5),
];
const WORKERS: usize = 4;

#[derive(Clone, Debug)]
pub(crate) struct Config {
    users: usize,
    requests_per_user: usize,
    sample_stride: u64,
    queue_capacity: usize,
    batch_size: usize,
    batch_timeout: Duration,
    projection_batch_size: usize,
    gc_batch_size: usize,
    gc_interval: Duration,
    retention: Duration,
    watermark_interval: Duration,
    old_lookup_delay: Duration,
    pub(crate) checkpoint_quantity: u64,
    output_root: PathBuf,
    preflight: PreflightConfig,
    memory_reserve_bytes: u64,
    free_space_reserve_bytes: u64,
    runtime_workers: usize,
    index_lookup: IndexLookupConfig,
    smoke: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            users: DEFAULT_USERS,
            requests_per_user: DEFAULT_REQUESTS_PER_USER,
            sample_stride: DEFAULT_SAMPLE_STRIDE,
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
            batch_size: DEFAULT_BATCH_SIZE,
            batch_timeout: Duration::from_millis(DEFAULT_BATCH_TIMEOUT_MS),
            projection_batch_size: DEFAULT_PROJECTION_BATCH_SIZE,
            gc_batch_size: DEFAULT_GC_BATCH_SIZE,
            gc_interval: Duration::from_millis(DEFAULT_GC_INTERVAL_MS),
            retention: Duration::from_millis(DEFAULT_RETENTION_MS),
            watermark_interval: Duration::from_millis(DEFAULT_WATERMARK_INTERVAL_MS),
            old_lookup_delay: Duration::from_millis(DEFAULT_OLD_LOOKUP_DELAY_MS),
            checkpoint_quantity: DEFAULT_CHECKPOINT_QUANTITY,
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
            runtime_workers: WORKERS,
            index_lookup: IndexLookupConfig::new(IndexLookupMode::Chunked {
                group_size: 256,
                max_in_flight: 4,
            })
            .expect("default transaction-index lookup settings are valid"),
            smoke: false,
        }
    }
}

impl Config {
    fn parse() -> Result<Self, String> {
        let args: Vec<_> = std::env::args().skip(1).collect();
        Self::parse_args(&args)
    }

    pub(crate) fn parse_args(args: &[String]) -> Result<Self, String> {
        let mut config = Self::default();
        let mut users_explicit = false;
        let mut requests_per_user_explicit = false;
        let mut sample_stride_explicit = false;
        let mut checkpoint_quantity_explicit = false;
        let mut index_lookup_name = None::<String>;
        let mut index_group_size = 256_usize;
        let mut index_group_size_explicit = false;
        let mut index_concurrency = 4_usize;
        let mut index_concurrency_explicit = false;
        let mut index = 0;
        while index < args.len() {
            let flag = args[index].as_str();
            if flag == "--help" || flag == "-h" {
                print_help();
                std::process::exit(0);
            }
            if flag == "--smoke" {
                config.smoke = true;
                index += 1;
                continue;
            }
            if flag == "--bench" {
                index += 1;
                continue;
            }
            index += 1;
            let value = args
                .get(index)
                .ok_or_else(|| format!("missing value for {flag}"))?;
            match flag {
                "--users" => {
                    config.users = parse_value(flag, value)?;
                    users_explicit = true;
                }
                "--requests-per-user" => {
                    config.requests_per_user = parse_value(flag, value)?;
                    requests_per_user_explicit = true;
                }
                "--sample-stride" => {
                    config.sample_stride = parse_value(flag, value)?;
                    sample_stride_explicit = true;
                }
                "--queue-capacity" => config.queue_capacity = parse_value(flag, value)?,
                "--batch-size" => config.batch_size = parse_value(flag, value)?,
                "--batch-timeout-ms" => {
                    config.batch_timeout = Duration::from_millis(parse_value(flag, value)?)
                }
                "--projection-batch-size" => {
                    config.projection_batch_size = parse_value(flag, value)?
                }
                "--gc-batch-size" => config.gc_batch_size = parse_value(flag, value)?,
                "--gc-interval-ms" => {
                    config.gc_interval = Duration::from_millis(parse_value(flag, value)?)
                }
                "--retention-ms" => {
                    config.retention = Duration::from_millis(parse_value(flag, value)?)
                }
                "--watermark-interval-ms" => {
                    config.watermark_interval = Duration::from_millis(parse_value(flag, value)?)
                }
                "--old-lookup-delay-ms" => {
                    config.old_lookup_delay = Duration::from_millis(parse_value(flag, value)?)
                }
                "--checkpoint-quantity" => {
                    config.checkpoint_quantity = parse_value(flag, value)?;
                    checkpoint_quantity_explicit = true;
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
                "--runtime-workers" => config.runtime_workers = parse_value(flag, value)?,
                "--index-lookup" => index_lookup_name = Some(value.clone()),
                "--index-group-size" => {
                    index_group_size = parse_value(flag, value)?;
                    index_group_size_explicit = true;
                }
                "--index-concurrency" => {
                    index_concurrency = parse_value(flag, value)?;
                    index_concurrency_explicit = true;
                }
                _ => return Err(format!("unknown option {flag}")),
            }
            index += 1;
        }
        if config.smoke {
            if !users_explicit {
                config.users = 200;
            }
            if !requests_per_user_explicit {
                config.requests_per_user = 200;
            }
            if !sample_stride_explicit {
                config.sample_stride = 1;
            }
            if !checkpoint_quantity_explicit {
                config.checkpoint_quantity = 100;
            }
        }
        let index_lookup_name = index_lookup_name.as_deref().unwrap_or("chunked");
        let index_lookup_mode = match index_lookup_name {
            "point_get" => IndexLookupMode::PointGet,
            "whole_batch_multiget" => IndexLookupMode::WholeBatchMultiGet,
            "chunked" => IndexLookupMode::Chunked {
                group_size: index_group_size,
                max_in_flight: index_concurrency,
            },
            _ => {
                return Err(format!(
                    "invalid value for --index-lookup: {index_lookup_name}; expected point_get, whole_batch_multiget, or chunked"
                ));
            }
        };
        if !matches!(index_lookup_mode, IndexLookupMode::Chunked { .. })
            && (index_group_size_explicit || index_concurrency_explicit)
        {
            return Err(
                "--index-group-size and --index-concurrency apply only to --index-lookup chunked"
                    .to_owned(),
            );
        }
        config.index_lookup = IndexLookupConfig::new(index_lookup_mode)?;
        config.validate()?;
        Ok(config)
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.users == 0 || self.users > u32::MAX as usize || self.requests_per_user < 200 {
            return Err(
                "users must be positive and requests per user must be at least 200".to_owned(),
            );
        }
        if self.requests_per_user % 200 != 0 {
            return Err(
                "requests per user must be a multiple of 200 for an exact old-request mix"
                    .to_owned(),
            );
        }
        let total = self
            .users
            .checked_mul(self.requests_per_user)
            .ok_or_else(|| "total request count overflows usize".to_owned())?;
        if total % 20 != 0 {
            return Err("total requests must divide into an exact 10% historical mix".to_owned());
        }
        if self.sample_stride == 0
            || self.queue_capacity == 0
            || self.batch_size == 0
            || self.batch_timeout.is_zero()
            || self.projection_batch_size == 0
            || self.gc_batch_size == 0
            || self.gc_interval.is_zero()
            || self.retention.is_zero()
            || self.watermark_interval.is_zero()
            || self.checkpoint_quantity == 0
            || self.runtime_workers == 0
        {
            return Err(
                "queue, batch, sample, retention, checkpoint, and interval values must be positive"
                    .to_owned(),
            );
        }
        if self.preflight.observation.is_zero()
            || self.preflight.timeout.is_zero()
            || self.preflight.observation > self.preflight.timeout
        {
            return Err(
                "preflight observation must be positive and no longer than its timeout".to_owned(),
            );
        }
        if !(0.0..=100.0).contains(&self.preflight.max_cpu_busy_pct)
            || !(0.0..=100.0).contains(&self.preflight.max_disk_busy_pct)
        {
            return Err("preflight CPU and disk percentages must be in 0..=100".to_owned());
        }
        Ok(())
    }

    pub(crate) fn total_requests(&self) -> u64 {
        u64::try_from(self.users)
            .unwrap_or(u64::MAX)
            .saturating_mul(u64::try_from(self.requests_per_user).unwrap_or(u64::MAX))
    }

    fn total_records(&self) -> Result<u64, String> {
        self.total_requests()
            .checked_add((self.users * SEED_TRANSACTIONS_PER_USER) as u64)
            .ok_or_else(|| "total durable record count overflowed".to_owned())
    }

    fn expected_disk_bytes(&self) -> Result<u64, String> {
        self.total_records()?
            .checked_mul(ESTIMATED_DISK_BYTES_PER_RECORD)
            .and_then(|value| value.checked_add(self.free_space_reserve_bytes))
            .ok_or_else(|| "disk requirement estimate overflowed".to_owned())
    }

    fn expected_memory_bytes(&self) -> Result<u64, String> {
        self.total_records()?
            .checked_mul(ESTIMATED_DESTINATION_BYTES_PER_RECORD)
            .and_then(|value| value.checked_add(self.memory_reserve_bytes))
            .ok_or_else(|| "destination memory estimate overflowed".to_owned())
    }

    fn uses_canonical_default_workload(&self) -> bool {
        let default = Self::default();
        !self.smoke
            && self.users == default.users
            && self.requests_per_user == default.requests_per_user
            && self.sample_stride == default.sample_stride
            && self.queue_capacity == default.queue_capacity
            && self.batch_size == default.batch_size
            && self.batch_timeout == default.batch_timeout
            && self.projection_batch_size == default.projection_batch_size
            && self.gc_batch_size == default.gc_batch_size
            && self.gc_interval == default.gc_interval
            && self.retention == default.retention
            && self.watermark_interval == default.watermark_interval
            && self.old_lookup_delay == default.old_lookup_delay
            && self.checkpoint_quantity == default.checkpoint_quantity
            && self.preflight.observation == default.preflight.observation
            && self.preflight.timeout == default.preflight.timeout
            && self.preflight.max_cpu_busy_pct == default.preflight.max_cpu_busy_pct
            && self.preflight.max_disk_busy_pct == default.preflight.max_disk_busy_pct
            && self.memory_reserve_bytes == default.memory_reserve_bytes
            && self.free_space_reserve_bytes == default.free_space_reserve_bytes
            && self.runtime_workers == default.runtime_workers
            && self.index_lookup == default.index_lookup
    }
}

fn print_help() {
    eprintln!(
        "Tokio single-shard integrated credit/debit ledger pipeline benchmark\n\
         Defaults: 50,000 users/coroutines x 200 requests in six cases\n\
         Each case has an exact 50/50 credit/debit mix. Cases: per-batch and\n\
         checkpoint balances, each with GC off + 0% history, GC on + 0%\n\
         history, and GC on + 5% history split equally between exact hits\n\
         and misses.\n\
         --smoke uses 200 users x 200 requests, stride 1, and checkpoint\n\
         quantity 100. Explicit values override smoke defaults in any order.\n\
         Options: --smoke --users N --requests-per-user N --sample-stride N\n\
         --queue-capacity N --batch-size N --batch-timeout-ms N\n\
         --projection-batch-size N --gc-batch-size N --gc-interval-ms N\n\
         --retention-ms N --watermark-interval-ms N\n\
         --old-lookup-delay-ms N --checkpoint-quantity N --output-root PATH\n\
         --index-lookup point_get|whole_batch_multiget|chunked (default chunked)\n\
         --index-group-size N (default 256, valid 1..=2048)\n\
         --index-concurrency N (default 4, valid 1..=8)\n\
         --preflight-observation-ms N --preflight-timeout-ms N\n\
         --max-cpu-busy-pct N --max-disk-busy-pct N\n\
         --memory-reserve-mib N --free-space-reserve-mib N --runtime-workers N"
    );
}

fn parse_value<T: std::str::FromStr>(flag: &str, value: &str) -> Result<T, String> {
    value
        .parse()
        .map_err(|_| format!("invalid value for {flag}: {value}"))
}

struct TrialDirectory {
    path: PathBuf,
    cleaned: bool,
}

impl TrialDirectory {
    fn create(path: PathBuf) -> Result<Self, String> {
        if path.exists() {
            return Err(format!(
                "trial DB directory already exists: {}",
                path.display()
            ));
        }
        fs::create_dir_all(&path).map_err(|error| {
            format!(
                "cannot create trial DB directory {}: {error}",
                path.display()
            )
        })?;
        Ok(Self {
            path,
            cleaned: false,
        })
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn cleanup(mut self) -> Result<(), String> {
        match fs::remove_dir_all(&self.path) {
            Ok(()) => {
                self.cleaned = true;
                Ok(())
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                self.cleaned = true;
                Ok(())
            }
            Err(error) => Err(format!(
                "cannot remove owned trial DB directory {}: {error}",
                self.path.display()
            )),
        }
    }
}

impl Drop for TrialDirectory {
    fn drop(&mut self) {
        if self.cleaned {
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

#[derive(Default)]
struct StageSamples {
    total: Vec<u64>,
    admission: Vec<u64>,
    enqueue: Vec<u64>,
    queue: Vec<u64>,
    batch: Vec<u64>,
    handler: Vec<u64>,
    response: Vec<u64>,
    historical_lookup: Vec<u64>,
    historical_hit_lookup: Vec<u64>,
    historical_miss_lookup: Vec<u64>,
}

impl StageSamples {
    fn record(&mut self, sample: ledger_time_boundary::RequestStages) {
        self.total.push(sample.overall_ns);
        self.admission.push(sample.admission_ns);
        self.enqueue.push(sample.enqueue_ns);
        self.queue.push(sample.queue_ns);
        self.batch.push(sample.batch_ns);
        self.handler.push(sample.handler_ns);
        self.response.push(sample.response_ns);
    }

    fn record_historical_lookup(&mut self, sample: ledger_time_boundary::RequestStages, hit: bool) {
        self.historical_lookup.push(sample.old_lookup_ns);
        if hit {
            self.historical_hit_lookup.push(sample.old_lookup_ns);
        } else {
            self.historical_miss_lookup.push(sample.old_lookup_ns);
        }
    }
}

#[derive(Default)]
struct ClientStats {
    requests: u64,
    fresh: u64,
    credits: u64,
    debits: u64,
    fresh_credits: u64,
    fresh_debits: u64,
    historical_credits: u64,
    historical_debits: u64,
    historical_hits: u64,
    historical_misses: u64,
    stages: StageSamples,
    completion: Option<ClientCompletion>,
}

#[derive(Clone, Copy)]
struct ClientCompletion {
    reply_at: Instant,
    cpu_seconds: f64,
    cpu_sampled_at: Instant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RequestClass {
    Fresh,
    HistoricalHit,
    HistoricalMiss,
}

pub(crate) fn request_class_and_operation(
    account_id: u64,
    request_index: usize,
    expired_pct: u8,
) -> (Operation, RequestClass) {
    let block_slot = request_index % 200;
    let offset = (splitmix64(account_id) % 200) as usize;
    let slot = (block_slot * 37 + offset) % 200;
    let operation = if slot % 2 == 0 {
        Operation::Credit
    } else {
        Operation::Debit
    };
    let class = if expired_pct == 5 && slot < 10 {
        let hit = if account_id % 2 == 0 {
            slot < 5
        } else {
            matches!(slot, 1 | 2 | 3 | 8 | 9)
        };
        if hit {
            RequestClass::HistoricalHit
        } else {
            RequestClass::HistoricalMiss
        }
    } else {
        RequestClass::Fresh
    };
    (operation, class)
}

#[derive(Clone, Copy, Debug, Default)]
struct LatencySummary {
    p50_ns: u64,
    p95_ns: u64,
    p99_ns: u64,
    sample_count: usize,
}

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

struct CaseResult {
    name: String,
    users: usize,
    projection_enabled: bool,
    gc_enabled: bool,
    mode: BalanceMode,
    index_lookup_mode: Option<IndexLookupMode>,
    runtime_workers: usize,
    expired_pct: u8,
    requests: u64,
    fresh: u64,
    credits: u64,
    debits: u64,
    historical_hits: u64,
    historical_misses: u64,
    measurement_started: Instant,
    client_wall: Duration,
    settled_wall: Duration,
    cpu_seconds: f64,
    cpu_cores: f64,
    cpu_sample_offset_us: u64,
    progress_sample_offset_us: u64,
    io_sample_offset_us: u64,
    storage_sample_offset_us: u64,
    io: IoDelta,
    rocks: (u64, u64, u64, u64, u64, u64, u64),
    db_bytes: u64,
    user_tx: LatencySummary,
    admission: LatencySummary,
    enqueue: LatencySummary,
    queue: LatencySummary,
    batch: LatencySummary,
    handler: LatencySummary,
    response: LatencySummary,
    historical_lookup: LatencySummary,
    historical_hit_lookup: LatencySummary,
    historical_miss_lookup: LatencySummary,
    projection_batches: Vec<ProjectionStageSample>,
    gc_steps: Vec<TimedGcStep>,
    checkpoint_steps: Vec<TimedCheckpointStep>,
    watermark_samples: Vec<WatermarkSample>,
    account_metrics: crate::ledger_account_store::MetricsSnapshot,
    index_lookup_batches: Vec<IndexLookupBatchMetrics>,
    watermark_updates: usize,
    initial_watermark: u64,
    final_watermark: u64,
    final_sequence: u64,
    gc_prefix_seq: u64,
    gc_prefix_near_client_end: u64,
    latest_seq_near_client_end: u64,
    projected_seq_near_client_end: u64,
    destination_seq_near_client_end: u64,
    gc_backlog_records_near_client_end: u64,
    projection_backlog_records_near_client_end: u64,
    checkpoint_lag_records_near_client_end: u64,
    recovery_seconds: f64,
    integrity_seconds: f64,
    peak_rss_bytes: u64,
    setup_preflight: PreflightReport,
    measure_preflight: PreflightReport,
}

#[derive(Clone, Copy, Debug)]
struct ProjectionStageSample {
    sequence: u64,
    records: usize,
    read_ns: u64,
    apply_ns: u64,
    progress_sync_ns: u64,
    total_ns: u64,
    elapsed_ns: u64,
    completed_at: Instant,
}

#[derive(Clone, Copy, Debug)]
struct TimedGcStep {
    elapsed_ns: u64,
    duration_ns: u64,
    completed_at: Instant,
    outcome: GcStepOutcome,
}

#[derive(Clone, Copy, Debug)]
struct TimedCheckpointStep {
    elapsed_ns: u64,
    sample: crate::ledger_account_store::CheckpointSample,
}

fn summarize(samples: &[u64]) -> LatencySummary {
    if samples.is_empty() {
        return LatencySummary::default();
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let at = |percent: usize| {
        let index = sorted
            .len()
            .saturating_mul(percent)
            .div_ceil(100)
            .saturating_sub(1);
        sorted[index.min(sorted.len() - 1)]
    };
    LatencySummary {
        p50_ns: at(50),
        p95_ns: at(95),
        p99_ns: at(99),
        sample_count: samples.len(),
    }
}

fn summarize_values(values: impl IntoIterator<Item = u64>) -> LatencySummary {
    let values = values.into_iter().collect::<Vec<_>>();
    summarize(&values)
}

fn case_latency_summaries(result: &CaseResult) -> Vec<(&'static str, LatencySummary)> {
    let mut rows = vec![
        ("request.total", result.user_tx),
        ("request.admission", result.admission),
        ("request.enqueue", result.enqueue),
        ("request.queue", result.queue),
        ("request.batch", result.batch),
        ("request.handler", result.handler),
        ("request.response", result.response),
        ("request.historical_lookup", result.historical_lookup),
        (
            "request.historical_hit_lookup",
            result.historical_hit_lookup,
        ),
        (
            "request.historical_miss_lookup",
            result.historical_miss_lookup,
        ),
    ];
    rows.extend([
        (
            "projection.read",
            summarize_values(
                result
                    .projection_batches
                    .iter()
                    .map(|sample| sample.read_ns),
            ),
        ),
        (
            "projection.apply",
            summarize_values(
                result
                    .projection_batches
                    .iter()
                    .map(|sample| sample.apply_ns),
            ),
        ),
        (
            "projection.progress_sync",
            summarize_values(
                result
                    .projection_batches
                    .iter()
                    .map(|sample| sample.progress_sync_ns),
            ),
        ),
        (
            "projection.total",
            summarize_values(
                result
                    .projection_batches
                    .iter()
                    .map(|sample| sample.total_ns),
            ),
        ),
        (
            "gc.scan",
            summarize_values(result.gc_steps.iter().map(|sample| sample.outcome.scan_ns)),
        ),
        (
            "gc.delete_build",
            summarize_values(
                result
                    .gc_steps
                    .iter()
                    .map(|sample| sample.outcome.delete_ns),
            ),
        ),
        (
            "gc.sync_write",
            summarize_values(result.gc_steps.iter().map(|sample| sample.outcome.write_ns)),
        ),
        (
            "gc.total",
            summarize_values(result.gc_steps.iter().map(|sample| sample.duration_ns)),
        ),
        (
            "watermark.fence_wait",
            summarize_values(
                result
                    .watermark_samples
                    .iter()
                    .map(|sample| sample.fence_wait_ns),
            ),
        ),
        (
            "watermark.projection_wait",
            summarize_values(
                result
                    .watermark_samples
                    .iter()
                    .map(|sample| sample.projection_wait_ns),
            ),
        ),
        (
            "watermark.persist",
            summarize_values(
                result
                    .watermark_samples
                    .iter()
                    .map(|sample| sample.persist_ns),
            ),
        ),
        (
            "watermark.total",
            summarize_values(
                result
                    .watermark_samples
                    .iter()
                    .map(|sample| sample.total_ns),
            ),
        ),
        (
            "checkpoint.total",
            summarize_values(
                result
                    .checkpoint_steps
                    .iter()
                    .map(|step| step.sample.duration_ns),
            ),
        ),
        (
            "checkpoint.chunk_sync",
            summarize_values(
                result
                    .checkpoint_steps
                    .iter()
                    .map(|step| step.sample.chunk_sync_ns),
            ),
        ),
        (
            "checkpoint.manifest_sync",
            summarize_values(
                result
                    .checkpoint_steps
                    .iter()
                    .map(|step| step.sample.manifest_sync_ns),
            ),
        ),
    ]);
    rows
}

fn write_index_lookup_trial_artifacts(
    config: &Config,
    mode: IndexLookupMode,
    result: &CaseResult,
    setup_preflight: &PreflightReport,
) -> Result<(), String> {
    let output_root = &config.output_root;
    let mode_name = index_lookup_mode_name(mode);
    let lookup_metrics = sum_index_lookup_metrics(&result.index_lookup_batches);
    let keys = lookup_metrics.keys_looked_up;
    let get_calls = lookup_metrics.native_get_calls;
    let misses = lookup_metrics.misses;
    let hits = lookup_metrics.hits;
    let summary_path = output_root.join("ledger_index_lookup_trial_summary.csv");
    let mut summary = BufWriter::new(
        File::create(&summary_path)
            .map_err(|error| format!("cannot create {}: {error}", summary_path.display()))?,
    );
    let summary_columns = [
        "mode",
        "index_lookup_strategy",
        "index_group_size",
        "index_concurrency",
        "workers",
        "users",
        "coroutines",
        "requests",
        "credits",
        "debits",
        "fresh_commits",
        "historical_hits",
        "historical_misses",
        "projection_enabled",
        "gc_enabled",
        "watermark_updates",
        "sample_stride",
        "client_wall_s",
        "client_rps",
        "cpu_s",
        "cpu_core_equivalents",
        "cpu_ns_per_request",
        "cpu_sample_offset_us",
        "progress_sample_offset_us",
        "io_sample_offset_us",
        "storage_sample_offset_us",
        "rss_bytes",
        "db_bytes",
        "process_rchar_bytes",
        "process_wchar_bytes",
        "process_read_bytes",
        "process_write_bytes",
        "device",
        "major_minor",
        "device_read_bytes",
        "device_write_bytes",
        "device_busy_ms",
        "wal_syncs",
        "wal_bytes",
        "writes_with_wal",
        "flush_write_bytes",
        "compaction_read_bytes",
        "compaction_write_bytes",
        "stall_us",
        "request_sample_count",
        "request_p50_ns",
        "request_p95_ns",
        "request_p99_ns",
        "batch_count",
        "native_get_calls",
        "keys_looked_up",
        "hits",
        "misses",
        "max_in_flight_groups",
        "max_running_query_jobs",
        "latest_seq_near_client_end",
        "projected_seq_near_client_end",
        "destination_seq_near_client_end",
        "gc_prefix_near_client_end",
        "final_seq",
        "initial_watermark",
        "final_watermark",
        "gc_prefix_after_settle",
        "recovery_s",
        "integrity_s",
        "setup_preflight_cpu_pct",
        "setup_preflight_disk_pct",
        "measure_preflight_cpu_pct",
        "measure_preflight_disk_pct",
    ];
    write_csv_record(
        &mut summary,
        summary_columns.iter().map(|value| value.to_string()),
    )
    .map_err(|error| format!("cannot write {}: {error}", summary_path.display()))?;
    let client_rps = result.requests as f64 / result.client_wall.as_secs_f64();
    let max_in_flight = result
        .index_lookup_batches
        .iter()
        .map(|batch| batch.max_observed_in_flight_groups)
        .max()
        .unwrap_or(0);
    let max_running = result
        .index_lookup_batches
        .iter()
        .map(|batch| batch.max_observed_running_query_jobs)
        .max()
        .unwrap_or(0);
    let rocks = result.rocks;
    let (strategy_name, configured_group_size, configured_concurrency) =
        index_lookup_details(Some(mode));
    let summary_values = [
        mode_name.clone(),
        strategy_name,
        configured_group_size,
        configured_concurrency,
        result.runtime_workers.to_string(),
        result.users.to_string(),
        result.users.to_string(),
        result.requests.to_string(),
        result.credits.to_string(),
        result.debits.to_string(),
        result.fresh.to_string(),
        result.historical_hits.to_string(),
        result.historical_misses.to_string(),
        result.projection_enabled.to_string(),
        result.gc_enabled.to_string(),
        result.watermark_updates.to_string(),
        config.sample_stride.to_string(),
        format!("{:.6}", result.client_wall.as_secs_f64()),
        format!("{client_rps:.3}"),
        format!("{:.6}", result.cpu_seconds),
        format!("{:.6}", result.cpu_cores),
        format!(
            "{:.3}",
            result.cpu_seconds * 1_000_000_000.0 / result.requests.max(1) as f64
        ),
        result.cpu_sample_offset_us.to_string(),
        result.progress_sample_offset_us.to_string(),
        result.io_sample_offset_us.to_string(),
        result.storage_sample_offset_us.to_string(),
        result.peak_rss_bytes.to_string(),
        result.db_bytes.to_string(),
        result.io.process_rchar_bytes.to_string(),
        result.io.process_wchar_bytes.to_string(),
        result.io.process_read_bytes.to_string(),
        result.io.process_write_bytes.to_string(),
        result.io.target_device.clone(),
        result.io.target_major_minor.clone(),
        result.io.target_read_bytes.to_string(),
        result.io.target_write_bytes.to_string(),
        result.io.target_busy_ms.to_string(),
        rocks.0.to_string(),
        rocks.1.to_string(),
        rocks.2.to_string(),
        rocks.3.to_string(),
        rocks.4.to_string(),
        rocks.5.to_string(),
        rocks.6.to_string(),
        result.user_tx.sample_count.to_string(),
        result.user_tx.p50_ns.to_string(),
        result.user_tx.p95_ns.to_string(),
        result.user_tx.p99_ns.to_string(),
        result.index_lookup_batches.len().to_string(),
        get_calls.to_string(),
        keys.to_string(),
        hits.to_string(),
        misses.to_string(),
        max_in_flight.to_string(),
        max_running.to_string(),
        result.latest_seq_near_client_end.to_string(),
        result.projected_seq_near_client_end.to_string(),
        result.destination_seq_near_client_end.to_string(),
        result.gc_prefix_near_client_end.to_string(),
        result.final_sequence.to_string(),
        result.initial_watermark.to_string(),
        result.final_watermark.to_string(),
        result.gc_prefix_seq.to_string(),
        format!("{:.6}", result.recovery_seconds),
        format!("{:.6}", result.integrity_seconds),
        format!("{:.3}", setup_preflight.cpu_busy_pct),
        format!("{:.3}", setup_preflight.disk_busy_pct),
        format!("{:.3}", result.measure_preflight.cpu_busy_pct),
        format!("{:.3}", result.measure_preflight.disk_busy_pct),
    ];
    if summary_columns.len() != summary_values.len() {
        return Err(format!(
            "index-lookup summary has {} columns and {} values",
            summary_columns.len(),
            summary_values.len()
        ));
    }
    write_csv_record(&mut summary, summary_values)
        .map_err(|error| format!("cannot write {}: {error}", summary_path.display()))?;
    summary
        .flush()
        .map_err(|error| format!("cannot flush {}: {error}", summary_path.display()))?;

    let batch_path = output_root.join("ledger_index_lookup_trial_batches.csv");
    let mut batches = BufWriter::new(
        File::create(&batch_path)
            .map_err(|error| format!("cannot create {}: {error}", batch_path.display()))?,
    );
    writeln!(
        batches,
        "mode,batch_index,transaction_count,dispatch_wait_ns,batch_gate_wait_ns,key_prep_ns,query_wall_ns,lookup_blocking_pool_wait_ns,native_get_ns,decode_ns,lookup_submit_to_collection_ns,apply_blocking_pool_wait_ns,apply_submit_to_collection_ns,sequential_apply_build_ns,sync_write_batch_ns,memory_publish_ns,get_calls,keys_looked_up,hits,misses,groups_submitted,max_in_flight_groups,max_running_query_jobs"
    )
    .map_err(|error| format!("cannot write {}: {error}", batch_path.display()))?;
    for (index, batch) in result.index_lookup_batches.iter().enumerate() {
        writeln!(
            batches,
            "{mode_name},{index},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            batch.transaction_count,
            batch.dispatch_wait_ns,
            batch.batch_gate_wait_ns,
            batch.key_prep_ns,
            optional_ns(batch.query_wall_ns),
            optional_ns(batch.blocking_pool_wait_ns),
            batch.native_get_ns,
            batch.decode_ns,
            optional_ns(batch.submit_to_collection_ns),
            batch.apply_blocking_pool_wait_ns,
            batch.apply_submit_to_collection_ns,
            batch.sequential_apply_build_ns,
            batch.sync_write_batch_ns,
            batch.memory_publish_ns,
            batch.get_calls,
            batch.keys_looked_up,
            batch.hits,
            batch.misses,
            batch.groups_submitted,
            batch.max_observed_in_flight_groups,
            batch.max_observed_running_query_jobs,
        )
        .map_err(|error| format!("cannot write {}: {error}", batch_path.display()))?;
    }
    batches
        .flush()
        .map_err(|error| format!("cannot flush {}: {error}", batch_path.display()))?;

    let stages_path = output_root.join("ledger_index_lookup_trial_stages.csv");
    let mut stages = BufWriter::new(
        File::create(&stages_path)
            .map_err(|error| format!("cannot create {}: {error}", stages_path.display()))?,
    );
    writeln!(stages, "stage,unit,scope,sample_count,p50_ns,p95_ns,p99_ns")
        .map_err(|error| format!("cannot write {}: {error}", stages_path.display()))?;
    for (name, values) in [
        ("request.total", &result.user_tx),
        ("request.admission", &result.admission),
        ("request.enqueue", &result.enqueue),
        ("request.queue", &result.queue),
        ("request.batch", &result.batch),
        ("request.handler", &result.handler),
        ("request.response", &result.response),
    ] {
        write_index_stage_summary(
            &mut stages,
            name,
            "ns",
            "per_sample_request",
            values.sample_count,
            values.p50_ns,
            values.p95_ns,
            values.p99_ns,
        )
        .map_err(|error| format!("cannot write {}: {error}", stages_path.display()))?;
    }

    let mut push_batch_stage =
        |name: &str, scope: &str, values: Option<Vec<u64>>| -> Result<(), String> {
            let Some(values) = values else {
                write_index_stage_summary(&mut stages, name, "ns", scope, 0, 0, 0, 0)
                    .map_err(|error| format!("cannot write {}: {error}", stages_path.display()))?;
                return Ok(());
            };
            let summary = summarize(&values);
            write_index_stage_summary(
                &mut stages,
                name,
                "ns",
                scope,
                summary.sample_count,
                summary.p50_ns,
                summary.p95_ns,
                summary.p99_ns,
            )
            .map_err(|error| format!("cannot write {}: {error}", stages_path.display()))
        };
    push_batch_stage(
        "dispatch.wait",
        "per_batch_wall",
        Some(
            result
                .index_lookup_batches
                .iter()
                .map(|batch| batch.dispatch_wait_ns)
                .collect(),
        ),
    )?;
    push_batch_stage(
        "batch_gate.wait",
        "per_batch_exclusive_batch_gate_lock_wait",
        Some(
            result
                .index_lookup_batches
                .iter()
                .map(|batch| batch.batch_gate_wait_ns)
                .collect(),
        ),
    )?;
    push_batch_stage(
        "keyprep",
        if mode == IndexLookupMode::PointGet {
            "per_batch_sum_of_key_encoding"
        } else {
            "per_batch_wall"
        },
        Some(
            result
                .index_lookup_batches
                .iter()
                .map(|batch| batch.key_prep_ns)
                .collect(),
        ),
    )?;
    push_batch_stage(
        "query.wall",
        "per_batch_wall_from_keyprep_start_to_all_groups_validated",
        if mode == IndexLookupMode::PointGet {
            None
        } else {
            Some(
                result
                    .index_lookup_batches
                    .iter()
                    .filter_map(|batch| batch.query_wall_ns)
                    .collect(),
            )
        },
    )?;
    push_batch_stage(
        "lookup.blocking_pool_wait",
        "per_batch_sum_of_group_waits_overlapping",
        if mode == IndexLookupMode::PointGet {
            None
        } else {
            Some(
                result
                    .index_lookup_batches
                    .iter()
                    .filter_map(|batch| batch.blocking_pool_wait_ns)
                    .collect(),
            )
        },
    )?;
    for (name, scope, values) in [
        (
            "lookup.native_get",
            "per_batch_sum_of_get_or_multiget_call_durations",
            result
                .index_lookup_batches
                .iter()
                .map(|batch| batch.native_get_ns)
                .collect(),
        ),
        (
            "lookup.decode_validate",
            "per_batch_sum_of_record_decode_and_key_validation",
            result
                .index_lookup_batches
                .iter()
                .map(|batch| batch.decode_ns)
                .collect(),
        ),
    ] {
        push_batch_stage(name, scope, Some(values))?;
    }
    push_batch_stage(
        "lookup.submit_to_collection",
        "per_batch_sum_of_overlapping_group_durations",
        if mode == IndexLookupMode::PointGet {
            None
        } else {
            Some(
                result
                    .index_lookup_batches
                    .iter()
                    .filter_map(|batch| batch.submit_to_collection_ns)
                    .collect(),
            )
        },
    )?;
    for (name, scope, values) in [
        (
            "apply.blocking_pool_wait",
            if mode == IndexLookupMode::PointGet {
                "per_batch_legacy_worker_wait_before_interleaved_loop"
            } else {
                "per_batch_sequential_apply_worker_wait"
            },
            result
                .index_lookup_batches
                .iter()
                .map(|batch| batch.apply_blocking_pool_wait_ns)
                .collect(),
        ),
        (
            "apply.submit_to_collection",
            "per_batch_worker_wall_including_pool_wait",
            result
                .index_lookup_batches
                .iter()
                .map(|batch| batch.apply_submit_to_collection_ns)
                .collect(),
        ),
        (
            "sequential_apply_build",
            if mode == IndexLookupMode::PointGet {
                "per_batch_legacy_interleaved_loop_including_point_reads_and_decode"
            } else {
                "per_batch_sequential_apply_and_build_after_prefetch"
            },
            result
                .index_lookup_batches
                .iter()
                .map(|batch| batch.sequential_apply_build_ns)
                .collect(),
        ),
        (
            "write.sync_write_batch",
            "per_batch_sync_write_call",
            result
                .index_lookup_batches
                .iter()
                .map(|batch| batch.sync_write_batch_ns)
                .collect(),
        ),
        (
            "memory.publish",
            "per_batch_state_publish_wall",
            result
                .index_lookup_batches
                .iter()
                .map(|batch| batch.memory_publish_ns)
                .collect(),
        ),
    ] {
        push_batch_stage(name, scope, Some(values))?;
    }
    drop(push_batch_stage);

    let group_stage_values = [
        (
            "lookup_group.blocking_pool_wait",
            "per_group_worker_start_minus_submit",
            result
                .index_lookup_batches
                .iter()
                .flat_map(|batch| batch.groups.iter())
                .map(|group| group.blocking_pool_wait_ns)
                .collect::<Vec<_>>(),
        ),
        (
            "lookup_group.native_get",
            "per_group_multiget_call_wall",
            result
                .index_lookup_batches
                .iter()
                .flat_map(|batch| batch.groups.iter())
                .map(|group| group.native_get_ns)
                .collect::<Vec<_>>(),
        ),
        (
            "lookup_group.decode_validate",
            "per_group_record_decode_and_key_validation_sum",
            result
                .index_lookup_batches
                .iter()
                .flat_map(|batch| batch.groups.iter())
                .map(|group| group.decode_ns)
                .collect::<Vec<_>>(),
        ),
        (
            "lookup_group.submit_to_collection",
            "per_group_submit_to_join_completion_wall",
            result
                .index_lookup_batches
                .iter()
                .flat_map(|batch| batch.groups.iter())
                .map(|group| group.submit_to_collection_ns)
                .collect::<Vec<_>>(),
        ),
    ];
    for (name, scope, values) in group_stage_values {
        if values.is_empty() {
            write_index_stage_summary(&mut stages, name, "ns", scope, 0, 0, 0, 0)
                .map_err(|error| format!("cannot write {}: {error}", stages_path.display()))?;
        } else {
            let summary = summarize(&values);
            write_index_stage_summary(
                &mut stages,
                name,
                "ns",
                scope,
                summary.sample_count,
                summary.p50_ns,
                summary.p95_ns,
                summary.p99_ns,
            )
            .map_err(|error| format!("cannot write {}: {error}", stages_path.display()))?;
        }
    }
    stages
        .flush()
        .map_err(|error| format!("cannot flush {}: {error}", stages_path.display()))?;

    let metadata_path = output_root.join("ledger_index_lookup_trial_metadata.txt");
    let mut metadata = BufWriter::new(
        File::create(&metadata_path)
            .map_err(|error| format!("cannot create {}: {error}", metadata_path.display()))?,
    );
    let (strategy_name, configured_group_size, configured_concurrency) =
        index_lookup_details(Some(mode));
    writeln!(
        metadata,
        "mode={mode_name}\nindex_lookup_strategy={strategy_name}\nindex_group_size={configured_group_size}\nindex_concurrency={configured_concurrency}\nworkers={}\nusers={}\ncoroutines={}\nrequests_per_user={}\nrequests={}\ncredits={}\ndebits={}\nfresh_commits={}\nhistorical_hits={}\nhistorical_misses={}\nprojection_enabled={}\ngc_enabled={}\nwatermark_updates={}\nsample_stride={}\nqueue_capacity={}\nbatch_size={}\nfirst_dequeue_timeout_ms={}\nbalance_mode=per_batch\nseed_transactions_per_user=3\nseed_initial_credit=100\nseed_credit=1\nseed_debit=1\nseed_sequence={}\nfinal_sequence={}\nprojector_spawned={}\nwatermark_manager_spawned={}\ngc_worker_spawned={}\nprojected_seq_near_client_end={}\ndestination_seq_near_client_end={}\ngc_prefix_near_client_end={}\ngc_prefix_after_settle={}\nprojection_backlog_records_near_client_end={}\ngc_backlog_records_near_client_end={}\ninitial_watermark={}\nfinal_watermark={}\ninitial_and_final_watermark_unchanged={}\nmock_destination_contract=successful_apply_is_durable_in_memory_only; no external database/process-crash proof\nmeasurement_wall_endpoint=latest_coroutine_last_reply\nmeasurement_cpu_endpoint=sampled_immediately_after_latest_coroutine_last_reply\nio_sample_offset_us={}\nstorage_sample_offset_us={}\nsetup_preflight_observation_ms={}\nsetup_preflight_timeout_ms={}\nsetup_preflight_cpu_busy_pct={:.3}\nsetup_preflight_disk_busy_pct={:.3}\nmeasure_preflight_cpu_busy_pct={:.3}\nmeasure_preflight_disk_busy_pct={:.3}\nlookup_native_get_calls={}\nlookup_keys_looked_up={}\nlookup_hits={}\nlookup_misses={}\n",
        result.runtime_workers,
        config.users,
        config.users,
        config.requests_per_user,
        result.requests,
        result.credits,
        result.debits,
        result.fresh,
        result.historical_hits,
        result.historical_misses,
        result.projection_enabled,
        result.gc_enabled,
        result.watermark_updates,
        config.sample_stride,
        config.queue_capacity,
        config.batch_size,
        config.batch_timeout.as_millis(),
        config.users as u64 * SEED_TRANSACTIONS_PER_USER as u64,
        result.final_sequence,
        result.projection_enabled,
        result.projection_enabled,
        result.gc_enabled,
        result.projected_seq_near_client_end,
        result.destination_seq_near_client_end,
        result.gc_prefix_near_client_end,
        result.gc_prefix_seq,
        result.projection_backlog_records_near_client_end,
        result.gc_backlog_records_near_client_end,
        result.initial_watermark,
        result.final_watermark,
        (result.initial_watermark == result.final_watermark),
        result.io_sample_offset_us,
        result.storage_sample_offset_us,
        setup_preflight.observation.as_millis(),
        config.preflight.timeout.as_millis(),
        setup_preflight.cpu_busy_pct,
        setup_preflight.disk_busy_pct,
        result.measure_preflight.cpu_busy_pct,
        result.measure_preflight.disk_busy_pct,
        get_calls,
        keys,
        hits,
        misses,
    )
    .map_err(|error| format!("cannot write {}: {error}", metadata_path.display()))?;
    metadata
        .flush()
        .map_err(|error| format!("cannot flush {}: {error}", metadata_path.display()))?;
    Ok(())
}

fn optional_ns(value: Option<u64>) -> String {
    value.map_or_else(|| "NA".to_owned(), |value| value.to_string())
}

fn write_csv_record(
    writer: &mut impl Write,
    values: impl IntoIterator<Item = String>,
) -> std::io::Result<()> {
    let mut first = true;
    for value in values {
        if !first {
            write!(writer, ",")?;
        }
        first = false;
        if value
            .chars()
            .any(|character| matches!(character, ',' | '"' | '\n' | '\r'))
        {
            write!(writer, "\"")?;
            for character in value.chars() {
                if character == '"' {
                    write!(writer, "\"\"")?;
                } else {
                    write!(writer, "{character}")?;
                }
            }
            write!(writer, "\"")?;
        } else {
            write!(writer, "{value}")?;
        }
    }
    writeln!(writer)
}

fn write_index_stage_summary(
    writer: &mut impl Write,
    stage: &str,
    unit: &str,
    scope: &str,
    sample_count: usize,
    p50_ns: u64,
    p95_ns: u64,
    p99_ns: u64,
) -> std::io::Result<()> {
    if sample_count == 0 {
        writeln!(writer, "{stage},{unit},{scope},0,NA,NA,NA")
    } else {
        writeln!(
            writer,
            "{stage},{unit},{scope},{sample_count},{p50_ns},{p95_ns},{p99_ns}"
        )
    }
}

fn delta(after: u64, before: u64) -> u64 {
    after.saturating_sub(before)
}

fn measured_account_metrics(
    initial: &crate::ledger_account_store::MetricsSnapshot,
    at_client_end: &crate::ledger_account_store::MetricsSnapshot,
    projection_batches: &[ProjectionStageSample],
    gc_steps: &[TimedGcStep],
    checkpoint_steps: &[TimedCheckpointStep],
) -> crate::ledger_account_store::MetricsSnapshot {
    crate::ledger_account_store::MetricsSnapshot {
        checkpoint_count: checkpoint_steps.len() as u64,
        checkpoint_snapshot_ns: delta(
            at_client_end.checkpoint_snapshot_ns,
            initial.checkpoint_snapshot_ns,
        ),
        checkpoint_queue_wait_ns: delta(
            at_client_end.checkpoint_queue_wait_ns,
            initial.checkpoint_queue_wait_ns,
        ),
        checkpoint_chunk_sync_ns: checkpoint_steps
            .iter()
            .map(|step| step.sample.chunk_sync_ns)
            .sum(),
        checkpoint_manifest_sync_ns: checkpoint_steps
            .iter()
            .map(|step| step.sample.manifest_sync_ns)
            .sum(),
        checkpoint_duration_ns: checkpoint_steps
            .iter()
            .map(|step| step.sample.duration_ns)
            .sum(),
        checkpoint_latest_seq: checkpoint_steps
            .iter()
            .map(|step| step.sample.sequence)
            .max()
            .unwrap_or(initial.checkpoint_latest_seq),
        checkpoint_snapshots_enqueued: delta(
            at_client_end.checkpoint_snapshots_enqueued,
            initial.checkpoint_snapshots_enqueued,
        ),
        checkpoint_samples: checkpoint_steps.iter().map(|step| step.sample).collect(),
        projection_progress_sync_ns: projection_batches
            .iter()
            .map(|sample| sample.progress_sync_ns)
            .sum(),
        gc_scan_ns: gc_steps.iter().map(|sample| sample.outcome.scan_ns).sum(),
        gc_delete_ns: gc_steps.iter().map(|sample| sample.outcome.delete_ns).sum(),
        gc_write_ns: gc_steps.iter().map(|sample| sample.outcome.write_ns).sum(),
        gc_records_scanned: gc_steps.iter().map(|sample| sample.outcome.scanned).sum(),
        gc_records_deleted: gc_steps.iter().map(|sample| sample.outcome.deleted).sum(),
        gc_bytes_deleted: gc_steps
            .iter()
            .map(|sample| sample.outcome.bytes_deleted)
            .sum(),
        ..Default::default()
    }
}

fn io_delta(before: IoSample, after: IoSample) -> Result<IoDelta, String> {
    if before.target_major_minor != after.target_major_minor {
        return Err("target block device changed during the measured window".to_owned());
    }
    Ok(IoDelta {
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

fn mode_name(mode: BalanceMode) -> &'static str {
    match mode {
        BalanceMode::PerBatch => "per_batch",
        BalanceMode::Checkpoint => "checkpoint",
    }
}

fn read_db_bytes(path: &Path) -> Result<u64, String> {
    fn visit(path: &Path, total: &mut u64) -> Result<(), String> {
        for entry in fs::read_dir(path)
            .map_err(|error| format!("cannot scan DB directory {}: {error}", path.display()))?
        {
            let entry =
                entry.map_err(|error| format!("cannot read DB directory entry: {error}"))?;
            let metadata = entry
                .metadata()
                .map_err(|error| format!("cannot read DB file metadata: {error}"))?;
            if metadata.is_dir() {
                visit(&entry.path(), total)?;
            } else {
                *total = total.saturating_add(metadata.len());
            }
        }
        Ok(())
    }
    let mut total = 0;
    visit(path, &mut total)?;
    Ok(total)
}

fn peak_rss_bytes() -> Result<u64, String> {
    let status = fs::read_to_string("/proc/self/status")
        .map_err(|error| format!("cannot read /proc/self/status: {error}"))?;
    for line in status.lines() {
        if let Some(value) = line.strip_prefix("VmHWM:") {
            let kib = value
                .split_whitespace()
                .next()
                .ok_or_else(|| "VmHWM has no numeric value".to_owned())?
                .parse::<u64>()
                .map_err(|error| format!("invalid VmHWM value: {error}"))?;
            return Ok(kib.saturating_mul(1024));
        }
    }
    Err("VmHWM is missing from /proc/self/status".to_owned())
}

pub fn run_from_args() -> Result<(), String> {
    let config = Config::parse()?;
    let runtime = Builder::new_multi_thread()
        .worker_threads(config.runtime_workers)
        .thread_name("ledger-safe-gc")
        .enable_all()
        .build()
        .map_err(|error| format!("cannot build Tokio runtime: {error}"))?;
    runtime.block_on(run(config))
}

pub(crate) fn ensure_index_lookup_root_idle(
    path: &Path,
    smoke: bool,
) -> Result<PreflightReport, String> {
    let mut config = Config::default();
    if smoke {
        config.users = 200;
        config.requests_per_user = 200;
        config.sample_stride = 1;
        config.preflight.observation = Duration::from_millis(100);
    }
    config.output_root = path.to_path_buf();
    let mut preflight = config.preflight.clone();
    preflight.min_available_mem_bytes = config.expected_memory_bytes()?;
    preflight.min_free_bytes = config.expected_disk_bytes()?;
    ledger_preflight::ensure_idle(path, &preflight)
        .map_err(|error| format!("strict index-lookup run preflight failed: {error}"))
}

/// Run exactly one foreground persistence trial for the independent index
/// lookup matrix. The existing pipeline setup, timing, settlement, recovery,
/// and cleanup stay in `run_case`; only its queue handler is selected here.
pub(crate) fn run_index_lookup_trial(
    args: &[String],
    mode: IndexLookupMode,
    integrated: bool,
) -> Result<(), String> {
    let mut config = Config::parse_args(args)?;
    config.runtime_workers = 4;
    if config.smoke {
        config.preflight.observation = Duration::from_millis(100);
    }
    if config.queue_capacity != DEFAULT_QUEUE_CAPACITY
        || config.batch_size != DEFAULT_BATCH_SIZE
        || config.batch_timeout != Duration::from_millis(DEFAULT_BATCH_TIMEOUT_MS)
        || config.projection_batch_size != DEFAULT_PROJECTION_BATCH_SIZE
        || config.gc_batch_size != DEFAULT_GC_BATCH_SIZE
    {
        return Err(
            "index-lookup trial requires the fixed queue, batch, and foreground profile".to_owned(),
        );
    }
    if config.smoke
        && (config.users != 200 || config.requests_per_user != 200 || config.sample_stride != 1)
    {
        return Err(
            "index-lookup smoke profile must use 200 users, 200 requests, and stride 1".to_owned(),
        );
    }
    if !config.smoke
        && (config.users != DEFAULT_USERS
            || config.requests_per_user != DEFAULT_REQUESTS_PER_USER
            || config.sample_stride != DEFAULT_SAMPLE_STRIDE)
    {
        return Err(
            "index-lookup full profile must use 50,000 users, 200 requests, and stride 64"
                .to_owned(),
        );
    }
    let lookup = IndexLookupConfig::new(mode)?;
    let total_disk = config.expected_disk_bytes()?;
    let total_memory = config.expected_memory_bytes()?;
    let mut preflight = config.preflight.clone();
    preflight.min_available_mem_bytes = total_memory;
    preflight.min_free_bytes = total_disk;
    let setup_preflight = ledger_preflight::ensure_idle(&config.output_root, &preflight)
        .map_err(|error| format!("strict index-lookup pre-setup check failed: {error}"))?;
    fs::create_dir_all(&config.output_root).map_err(|error| {
        format!(
            "cannot create index-lookup trial directory {}: {error}",
            config.output_root.display()
        )
    })?;
    let case_path = config.output_root.join(format!(
        ".ledger-index-lookup-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| format!("system clock is before Unix epoch: {error}"))?
            .as_nanos()
    ));
    let runtime = Builder::new_multi_thread()
        .worker_threads(4)
        .thread_name("ledger-index-lookup")
        .enable_all()
        .build()
        .map_err(|error| format!("cannot build index-lookup runtime: {error}"))?;
    let result = runtime.block_on(run_case(
        &config,
        &preflight,
        format!("index_lookup_{}", index_lookup_mode_name(mode)),
        case_path,
        BalanceMode::PerBatch,
        integrated,
        integrated,
        0,
        Some(lookup),
    ))?;
    if result.runtime_workers != 4
        || result.requests != (config.users * config.requests_per_user) as u64
        || result.final_sequence
            != (config.users * SEED_TRANSACTIONS_PER_USER) as u64 + result.requests
        || (integrated
            && (result.final_watermark <= result.initial_watermark
                || result.gc_prefix_seq > result.final_sequence
                || result.projected_seq_near_client_end
                    < (config.users * SEED_TRANSACTIONS_PER_USER) as u64
                || result.projected_seq_near_client_end > result.latest_seq_near_client_end
                || result.projected_seq_near_client_end > result.destination_seq_near_client_end
                || result.destination_seq_near_client_end > result.final_sequence
                || result.gc_prefix_near_client_end > result.projected_seq_near_client_end
                || result.gc_prefix_near_client_end > result.latest_seq_near_client_end
                || result.projection_backlog_records_near_client_end
                    != result
                        .latest_seq_near_client_end
                        .saturating_sub(result.projected_seq_near_client_end)
                || result.gc_backlog_records_near_client_end
                    != result
                        .latest_seq_near_client_end
                        .saturating_sub(result.gc_prefix_near_client_end)
                || result.watermark_updates == 0))
        || (!integrated
            && (result.initial_watermark != result.final_watermark
                || result.gc_prefix_seq != 0
                || result.projected_seq_near_client_end
                    != (config.users * SEED_TRANSACTIONS_PER_USER) as u64
                || result.destination_seq_near_client_end
                    != (config.users * SEED_TRANSACTIONS_PER_USER) as u64))
    {
        return Err("foreground index-lookup result differs from the fixed profile".to_owned());
    }
    let measured_batch_requests = result
        .index_lookup_batches
        .iter()
        .map(|batch| batch.transaction_count as u64)
        .sum::<u64>();
    if result.index_lookup_batches.is_empty() || measured_batch_requests != result.requests {
        return Err(format!(
            "index-lookup metrics cover {} batches and {} requests, expected {} requests",
            result.index_lookup_batches.len(),
            measured_batch_requests,
            result.requests
        ));
    }
    let metrics = sum_index_lookup_metrics(&result.index_lookup_batches);
    if metrics.keys_looked_up != result.requests
        || metrics.misses != result.requests
        || metrics.hits != 0
    {
        return Err(format!(
            "index lookups did not cover the fresh workload as misses: keys={} misses={} hits={} requests={}",
            metrics.keys_looked_up, metrics.misses, metrics.hits, result.requests
        ));
    }
    let expected_each_operation = result.requests / 2;
    if result.credits != expected_each_operation
        || result.debits != expected_each_operation
        || result.fresh != result.requests
        || result.historical_hits != 0
        || result.historical_misses != 0
        || (!integrated && result.watermark_updates != 0)
        || result.projection_enabled != integrated
        || result.gc_enabled != integrated
    {
        return Err(format!(
            "index-lookup workload/profile mismatch: credits={} debits={} fresh={} history_hits={} history_misses={} projection={} gc={} watermark_updates={}",
            result.credits,
            result.debits,
            result.fresh,
            result.historical_hits,
            result.historical_misses,
            result.projection_enabled,
            result.gc_enabled,
            result.watermark_updates
        ));
    }
    write_archive(
        &config.output_root,
        &config,
        std::slice::from_ref(&result),
        &setup_preflight,
    )?;
    write_index_lookup_trial_artifacts(&config, mode, &result, &setup_preflight)?;
    print_case_summary(&result);
    println!(
        "INDEX_LOOKUP_TRIAL mode={} workers=4 requests={} batches={} native_calls={} keys={} hits={} misses={} latest_seq={} projected_seq={} destination_seq={} gc_prefix={}",
        index_lookup_mode_name(mode),
        result.requests,
        result.index_lookup_batches.len(),
        metrics.native_get_calls,
        metrics.keys_looked_up,
        metrics.hits,
        metrics.misses,
        result.final_sequence,
        result.projected_seq_near_client_end,
        result.destination_seq_near_client_end,
        result.gc_prefix_seq,
    );
    Ok(())
}

fn index_lookup_mode_name(mode: IndexLookupMode) -> String {
    match mode {
        IndexLookupMode::PointGet => "point_get".to_owned(),
        IndexLookupMode::WholeBatchMultiGet => "whole_batch_multiget".to_owned(),
        IndexLookupMode::Chunked {
            group_size,
            max_in_flight,
        } => format!("chunked_{group_size}_p{max_in_flight}"),
    }
}

fn index_lookup_details(mode: Option<IndexLookupMode>) -> (String, String, String) {
    match mode {
        None => (
            "legacy_interleaved_point_get".to_owned(),
            "NA".to_owned(),
            "NA".to_owned(),
        ),
        Some(IndexLookupMode::PointGet) => {
            ("point_get".to_owned(), "NA".to_owned(), "NA".to_owned())
        }
        Some(IndexLookupMode::WholeBatchMultiGet) => (
            "whole_batch_multiget".to_owned(),
            "NA".to_owned(),
            "1".to_owned(),
        ),
        Some(IndexLookupMode::Chunked {
            group_size,
            max_in_flight,
        }) => (
            "chunked".to_owned(),
            group_size.to_string(),
            max_in_flight.to_string(),
        ),
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct IndexLookupMetricTotals {
    keys_looked_up: u64,
    native_get_calls: u64,
    misses: u64,
    hits: u64,
}

fn sum_index_lookup_metrics(batches: &[IndexLookupBatchMetrics]) -> IndexLookupMetricTotals {
    batches
        .iter()
        .fold(IndexLookupMetricTotals::default(), |totals, batch| {
            IndexLookupMetricTotals {
                keys_looked_up: totals.keys_looked_up.saturating_add(batch.keys_looked_up),
                native_get_calls: totals.native_get_calls.saturating_add(batch.get_calls),
                misses: totals.misses.saturating_add(batch.misses),
                hits: totals.hits.saturating_add(batch.hits),
            }
        })
}

/// Run one persistence profile for the thread-scaling matrix while reusing the
/// canonical account store, queue, workload, recovery checks, and archive.
pub(crate) fn run_thread_scaling_trial(
    args: &[String],
    projection_enabled: bool,
    gc_enabled: bool,
) -> Result<(), String> {
    let config = Config::parse_args(args)?;
    let runtime = Builder::new_multi_thread()
        .worker_threads(config.runtime_workers)
        .thread_name("ledger-thread-scaling")
        .enable_all()
        .build()
        .map_err(|error| format!("cannot build Tokio runtime: {error}"))?;
    runtime.block_on(run_thread_scaling_trial_inner(
        config,
        projection_enabled,
        gc_enabled,
    ))
}

async fn run_thread_scaling_trial_inner(
    config: Config,
    projection_enabled: bool,
    gc_enabled: bool,
) -> Result<(), String> {
    let total_disk = config.expected_disk_bytes()?;
    let total_memory = config.expected_memory_bytes()?;
    let mut preflight = config.preflight.clone();
    preflight.min_available_mem_bytes = total_memory;
    preflight.min_free_bytes = total_disk;
    let before_setup = ledger_preflight::ensure_idle(&config.output_root, &preflight)
        .map_err(|error| format!("strict pre-setup resource check failed: {error}"))?;
    fs::create_dir_all(&config.output_root).map_err(|error| {
        format!(
            "cannot create output root {}: {error}",
            config.output_root.display()
        )
    })?;
    let name = if projection_enabled {
        "integrated_pipeline"
    } else {
        "foreground_persistence"
    };
    let case_path = config.output_root.join(format!(
        ".ledger-thread-scaling-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| format!("system clock is before Unix epoch: {error}"))?
            .as_nanos()
    ));
    let result = run_case(
        &config,
        &preflight,
        name.to_owned(),
        case_path,
        BalanceMode::PerBatch,
        gc_enabled,
        projection_enabled,
        0,
        None,
    )
    .await?;
    print_case_summary(&result);
    write_archive(&config.output_root, &config, &[result], &before_setup)?;
    println!("ARCHIVE {}", config.output_root.display());
    Ok(())
}

async fn run(config: Config) -> Result<(), String> {
    let records = config.total_records()?;
    let total_disk = config.expected_disk_bytes()?;
    let total_memory = config.expected_memory_bytes()?;
    let mut preflight = config.preflight.clone();
    preflight.min_available_mem_bytes = total_memory;
    preflight.min_free_bytes = total_disk;
    let before_setup = ledger_preflight::ensure_idle(&config.output_root, &preflight)
        .map_err(|error| format!("strict pre-setup resource check failed: {error}"))?;
    fs::create_dir_all(&config.output_root).map_err(|error| {
        format!(
            "cannot create output root {}: {error}",
            config.output_root.display()
        )
    })?;
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("system clock is before Unix epoch: {error}"))?
        .as_nanos();
    let full_default = config.uses_canonical_default_workload();
    let mut results = Vec::with_capacity(RUN_CASES.len());
    for (index, (mode, gc_enabled, expired_pct)) in RUN_CASES.into_iter().enumerate() {
        let case_name = format!(
            "{}_gc_{}_history_{}pct",
            mode_name(mode),
            if gc_enabled { "on" } else { "off" },
            expired_pct
        );
        let case_path = config.output_root.join(format!(
            ".ledger-pipeline-{run_id}-{index}-{}",
            std::process::id()
        ));
        let result = run_case(
            &config,
            &preflight,
            case_name,
            case_path,
            mode,
            gc_enabled,
            true,
            expired_pct,
            Some(config.index_lookup),
        )
        .await?;
        results.push(result);
    }

    for result in &results {
        print_case_summary(result);
    }
    println!(
        "PREFLIGHT setup attempts={} CPU_busy_pct={:.2} disk_busy_pct={:.2} mem_available_bytes={} free_bytes={} device={}({})",
        before_setup.attempts,
        before_setup.cpu_busy_pct,
        before_setup.disk_busy_pct,
        before_setup.mem_available_bytes,
        before_setup.free_bytes,
        before_setup.device,
        before_setup.major_minor
    );
    println!(
        "RUN users={} coroutines={} requests_per_user={} total_requests_per_case={} cases={} operations=credit_50pct,debit_50pct expected_total_records={} estimated_free_disk_bytes={} estimated_available_memory_bytes={} smoke={}",
        config.users,
        config.users,
        config.requests_per_user,
        config.total_requests(),
        RUN_CASES.len(),
        records,
        total_disk,
        total_memory,
        config.smoke
    );

    if full_default {
        let archive = PathBuf::from(DEFAULT_ARCHIVE_ROOT).join(format!("run-{run_id}"));
        fs::create_dir_all(&archive).map_err(|error| {
            format!(
                "cannot create completed-run archive {}: {error}",
                archive.display()
            )
        })?;
        write_archive(&archive, &config, &results, &before_setup)?;
        println!("ARCHIVE {}", archive.display());
    }
    Ok(())
}

async fn run_case(
    config: &Config,
    preflight: &PreflightConfig,
    name: String,
    case_path: PathBuf,
    mode: BalanceMode,
    gc_enabled: bool,
    projection_enabled: bool,
    expired_pct: u8,
    index_lookup: Option<IndexLookupConfig>,
) -> Result<CaseResult, String> {
    let setup_preflight = ledger_preflight::ensure_idle(&case_path, preflight)
        .map_err(|error| format!("{name}: pre-setup resource check failed: {error}"))?;
    let trial = TrialDirectory::create(case_path)?;
    let source = AccountStore::open(trial.path(), config.users, mode, config.checkpoint_quantity)
        .await
        .map_err(|error| format!("{name}: source open failed: {error}"))?;
    let history_capacity = usize::try_from(config.total_records()?)
        .map_err(|_| "destination capacity does not fit usize".to_owned())?;
    let destination = Arc::new(
        MockProjectionStore::with_capacity(history_capacity)
            .map_err(|error| format!("{name}: destination allocation failed: {error}"))?,
    );
    let refund_history: Arc<dyn RefundHistory> = destination.clone();
    source
        .set_refund_history(refund_history)
        .map_err(|error| format!("{name}: cannot install historical destination: {error}"))?;

    let seed_time = ledger_time_boundary::unix_time_micros().saturating_sub(30_000_000);
    seed_source(&source, config.users, config.batch_size, seed_time).await?;
    source
        .drain_checkpoints()
        .await
        .map_err(|error| format!("{name}: seed checkpoint drain failed: {error}"))?;
    let seed_sequence = source.latest_seq();
    if mode == BalanceMode::Checkpoint && gc_enabled && config.smoke {
        let checkpoint_sequence = source.metrics().checkpoint_latest_seq;
        if checkpoint_sequence < seed_sequence {
            return Err(format!(
                "{name}: smoke checkpoint manifest sequence {checkpoint_sequence} does not cover seeded prefix {seed_sequence}"
            ));
        }
    }
    project_seed(
        &source,
        &destination,
        seed_sequence,
        config.projection_batch_size,
    )
    .await?;
    source
        .persist_projected_before_durable(seed_time.saturating_add(1), seed_sequence)
        .await
        .map_err(|error| format!("{name}: initial durable boundary failed: {error}"))?;
    let initial_watermark = seed_time.saturating_add(1);

    let measure_preflight = ledger_preflight::ensure_idle(trial.path(), preflight)
        .map_err(|error| format!("{name}: pre-measurement resource check failed: {error}"))?;
    let initial_rocks = source.rocksdb_stats();
    let initial_account_metrics = source.metrics();
    let initial_io = ledger_preflight::sample_io(trial.path())
        .map_err(|error| format!("{name}: cannot sample initial I/O counters: {error}"))?;
    let metrics_stage = Arc::new(Mutex::new(Vec::<ProjectionStageSample>::new()));
    let gc_stage = Arc::new(Mutex::new(Vec::<TimedGcStep>::new()));
    let progress = ProjectionProgress::new(seed_sequence);
    let gate = AdmissionGate::new(initial_watermark);
    let manager = Arc::new(WatermarkManager::new(
        Arc::clone(&gate),
        Arc::clone(&progress),
    ));
    let watermark_metrics = manager.metrics();
    let (committed_head, committed_rx) = watch::channel(seed_sequence);
    let (background_failure, background_rx) = watch::channel(None::<String>);
    let (shutdown, shutdown_rx) = watch::channel(false);

    let index_lookup_batches = index_lookup.map(|_| Arc::new(Mutex::new(Vec::new())));
    let (queue, worker) =
        if let (Some(lookup), Some(samples)) = (index_lookup, index_lookup_batches.clone()) {
            ledger_time_boundary::spawn_commit_queue_with_index_lookup(
                source.clone(),
                committed_head,
                config.queue_capacity,
                config.batch_size,
                config.batch_timeout,
                lookup,
                samples,
            )
        } else {
            ledger_time_boundary::spawn_commit_queue(
                source.clone(),
                committed_head,
                config.queue_capacity,
                config.batch_size,
                config.batch_timeout,
            )
        }
        .map_err(|error| format!("{name}: cannot start commit worker: {error}"))?;
    let background_workers = usize::from(projection_enabled) * 2 + usize::from(gc_enabled);
    let start_barrier = Arc::new(Barrier::new(config.users + 1 + background_workers));
    let projector = projection_enabled.then(|| {
        spawn_durable_projector(
            source.clone(),
            Arc::clone(&destination),
            Arc::clone(&progress),
            Arc::clone(&metrics_stage),
            background_failure.clone(),
            committed_rx,
            shutdown_rx.clone(),
            config.projection_batch_size,
            Arc::clone(&start_barrier),
        )
    });
    let watermark_task = projection_enabled.then(|| {
        spawn_watermark_manager(
            Arc::clone(&manager),
            source.clone(),
            config.retention,
            config.watermark_interval,
            shutdown_rx.clone(),
            background_failure.clone(),
            Arc::clone(&start_barrier),
        )
    });
    let gc_task = if gc_enabled {
        Some(spawn_gc_worker(
            source.clone(),
            config.gc_batch_size,
            config.gc_interval,
            shutdown_rx.clone(),
            background_failure.clone(),
            Arc::clone(&gc_stage),
            Arc::clone(&start_barrier),
        ))
    } else {
        None
    };

    let mut clients = JoinSet::new();
    let cpu_started = Arc::new(Mutex::new(ProcessTime::now()));
    for user in 0..config.users {
        clients.spawn(run_client(
            user as u64,
            config.requests_per_user,
            config.sample_stride,
            expired_pct,
            seed_time,
            Arc::clone(&gate),
            queue.clone(),
            Arc::clone(&destination),
            config.old_lookup_delay,
            Arc::clone(&start_barrier),
            background_rx.clone(),
            Arc::clone(&cpu_started),
        ));
    }

    let cpu_measurement_started = ProcessTime::now();
    *cpu_started
        .lock()
        .map_err(|_| format!("{name}: CPU start timer mutex poisoned"))? = cpu_measurement_started;
    let wall_started = Instant::now();
    let measurement_started = Instant::now();
    start_barrier.wait().await;
    let mut combined = ClientStats::default();
    let mut client_failure = None;
    while let Some(result) = clients.join_next().await {
        match result {
            Ok(Ok(stats)) => combine_client_stats(&mut combined, stats),
            Ok(Err(error)) => {
                client_failure.get_or_insert(error);
                clients.abort_all();
            }
            Err(error) => {
                client_failure.get_or_insert_with(|| format!("client coroutine failed: {error}"));
                clients.abort_all();
            }
        }
    }
    let completion = combined.completion.unwrap_or_else(|| ClientCompletion {
        reply_at: Instant::now(),
        cpu_seconds: cpu_measurement_started.elapsed().as_secs_f64(),
        cpu_sampled_at: Instant::now(),
    });
    let client_end = completion.reply_at;
    let client_wall = client_end.duration_since(wall_started);
    let settle_started = Instant::now();
    let cpu_seconds = completion.cpu_seconds;
    let cpu_sample_offset_us = u64::try_from(
        completion
            .cpu_sampled_at
            .duration_since(client_end)
            .as_micros(),
    )
    .unwrap_or(u64::MAX);
    let progress_snapshot_started = Instant::now();
    let progress_sample_offset_us = u64::try_from(
        progress_snapshot_started
            .duration_since(client_end)
            .as_micros(),
    )
    .unwrap_or(u64::MAX);
    let gc_prefix_near_client_end = source.gc_prefix_seq();
    let client_sequence_near_end = source.latest_seq();
    let projected_sequence_near_end = source.projected_seq();
    let destination_sequence_near_end = destination.progress();
    let account_metrics_at_client_end = source.metrics();
    let index_lookup_batches_at_client_end = match index_lookup_batches {
        Some(samples) => match samples.lock() {
            Ok(samples) => samples.clone(),
            Err(_) => {
                client_failure
                    .get_or_insert_with(|| format!("{name}: index-lookup metrics mutex poisoned"));
                Vec::new()
            }
        },
        None => Vec::new(),
    };
    let gc_backlog_records_near_client_end =
        client_sequence_near_end.saturating_sub(gc_prefix_near_client_end);
    let io_snapshot_started = Instant::now();
    let io_sample_offset_us =
        u64::try_from(io_snapshot_started.duration_since(client_end).as_micros())
            .unwrap_or(u64::MAX);
    let io_finished = ledger_preflight::sample_io(trial.path());
    let storage_snapshot_started = Instant::now();
    let storage_sample_offset_us = u64::try_from(
        storage_snapshot_started
            .duration_since(client_end)
            .as_micros(),
    )
    .unwrap_or(u64::MAX);
    let measured_rocks = subtract_rocks(source.rocksdb_stats(), initial_rocks);
    let measured_db_bytes = read_db_bytes(trial.path());
    let measured_peak_rss = peak_rss_bytes();
    let pre_catchup_failure = client_failure
        .as_ref()
        .map(|error| format!("client workload failed: {error}"))
        .or_else(|| background_rx.borrow().clone());
    if let Some(error) = pre_catchup_failure {
        progress.fail(error.clone());
        let teardown_errors = drain_pipeline_workers(
            &name,
            shutdown,
            queue,
            worker,
            projector,
            watermark_task,
            gc_task,
        )
        .await;
        let error = include_teardown_errors(error, teardown_errors);
        return Err(shutdown_source_after_error(source, &name, error).await);
    }

    let projection_settle = if projection_enabled {
        Some(
            tokio::time::timeout(
                Duration::from_secs(300),
                progress.wait_for(source.latest_seq()),
            )
            .await,
        )
    } else {
        None
    };
    let projection_error = match projection_settle {
        None | Some(Ok(Ok(()))) => None,
        Some(Ok(Err(error))) => Some(format!("projector catch-up failed: {error}")),
        Some(Err(_)) => Some("projector did not catch up within 300s".to_owned()),
    };
    if let Some(error) = projection_error {
        progress.fail(error.clone());
        let teardown_errors = drain_pipeline_workers(
            &name,
            shutdown,
            queue,
            worker,
            projector,
            watermark_task,
            gc_task,
        )
        .await;
        let error = include_teardown_errors(error, teardown_errors);
        return Err(shutdown_source_after_error(source, &name, error).await);
    }

    let teardown_errors = drain_pipeline_workers(
        &name,
        shutdown,
        queue,
        worker,
        projector,
        watermark_task,
        gc_task,
    )
    .await;
    let teardown_errors = include_watch_failure(teardown_errors, background_rx.borrow().clone());
    if !teardown_errors.is_empty() {
        let error = include_teardown_errors(
            "pipeline worker shutdown failed".to_owned(),
            teardown_errors,
        );
        return Err(shutdown_source_after_error(source, &name, error).await);
    }

    let projection_batches = metrics_stage
        .lock()
        .map_err(|_| format!("{name}: projection metrics mutex poisoned"))?
        .iter()
        .copied()
        .filter(|sample| sample.completed_at <= client_end)
        .collect::<Vec<_>>();
    let gc_steps = gc_stage
        .lock()
        .map_err(|_| format!("{name}: GC metrics mutex poisoned"))?
        .iter()
        .copied()
        .filter(|sample| sample.completed_at <= client_end)
        .collect::<Vec<_>>();
    let checkpoint_steps = account_metrics_at_client_end
        .checkpoint_samples
        .iter()
        .copied()
        .filter(|sample| {
            sample.completed_at >= measurement_started && sample.completed_at <= client_end
        })
        .map(|sample| TimedCheckpointStep {
            elapsed_ns: elapsed_ns(sample.completed_at.duration_since(measurement_started)),
            sample,
        })
        .collect::<Vec<_>>();
    let measured_account_metrics = measured_account_metrics(
        &initial_account_metrics,
        &account_metrics_at_client_end,
        &projection_batches,
        &gc_steps,
        &checkpoint_steps,
    );
    let watermark_samples = watermark_metrics
        .snapshot()
        .map_err(|error| format!("{name}: cannot read watermark metrics: {error}"))?
        .into_iter()
        .filter(|sample| sample.completed_at <= client_end)
        .collect::<Vec<_>>();
    let watermark_updates = watermark_samples.len();
    if !projection_enabled
        && (!projection_batches.is_empty()
            || !watermark_samples.is_empty()
            || watermark_updates != 0
            || measured_account_metrics.projection_progress_sync_ns != 0
            || projected_sequence_near_end != seed_sequence
            || destination_sequence_near_end != seed_sequence)
    {
        return Err(shutdown_source_after_error(
            source,
            &name,
            "foreground-only measurement recorded projector or watermark work".to_owned(),
        )
        .await);
    }
    if !gc_enabled
        && (measured_account_metrics.gc_scan_ns != 0
            || measured_account_metrics.gc_delete_ns != 0
            || measured_account_metrics.gc_write_ns != 0
            || measured_account_metrics.gc_records_scanned != 0
            || measured_account_metrics.gc_records_deleted != 0
            || measured_account_metrics.gc_bytes_deleted != 0)
    {
        return Err(shutdown_source_after_error(
            source,
            &name,
            "GC-off measurement recorded GC work".to_owned(),
        )
        .await);
    }

    let post_measurement = async {
        let io_finished = io_finished
            .map_err(|error| format!("cannot sample measured I/O end: {error}"))?;
        let sampled_io = io_delta(initial_io, io_finished)?;
        let measured_db_bytes = measured_db_bytes?;
        let measured_peak_rss = measured_peak_rss?;
        let final_sequence = source.latest_seq();
        let durable_projected = source
            .durable_projection_progress()
            .await
            .map_err(|error| format!("cannot read final durable projection progress: {error}"))?;
        let seed_sequence = config.users as u64 * SEED_TRANSACTIONS_PER_USER as u64;
        if projection_enabled {
            if durable_projected != final_sequence || destination.progress() != final_sequence {
                return Err(format!(
                    "projection did not settle: source={final_sequence}, durable={durable_projected}, destination={}",
                    destination.progress()
                ));
            }
            let final_candidate = ledger_time_boundary::unix_time_micros()
                .saturating_sub(u64::try_from(config.retention.as_micros()).unwrap_or(u64::MAX));
            manager
                .advance_once_durable(final_candidate, &source)
                .await
                .map_err(|error| format!("final durable boundary advance failed: {error}"))?;
        } else if durable_projected != seed_sequence
            || source.projected_seq() != seed_sequence
            || destination.progress() != seed_sequence
        {
            return Err(format!(
                "foreground-only trial changed seeded projection progress: expected={seed_sequence}, durable={durable_projected}, source={}, destination={}",
                source.projected_seq(),
                destination.progress()
            ));
        }
        if gc_enabled {
            loop {
                let outcome = source
                    .collect_garbage(config.gc_batch_size)
                    .await
                    .map_err(|error| format!("final GC sweep failed: {error}"))?;
                if outcome.deleted == 0 || outcome.blocked_at_seq.is_some() {
                    break;
                }
            }
        }
        let final_watermark = gate
            .watermark()
            .map_err(|error| format!("cannot read final watermark: {error}"))?;
        let gc_prefix_seq = source.gc_prefix_seq();
        let settled_wall = settle_started.elapsed();
        verify_case(
            &source,
            &destination,
            config,
            seed_time,
            expired_pct,
            gc_enabled,
            projection_enabled,
            &combined,
            final_sequence,
            gc_prefix_seq,
            seed_sequence,
        )
        .await?;
        Ok::<_, String>((
            sampled_io,
            measured_db_bytes,
            measured_peak_rss,
            final_sequence,
            durable_projected,
            final_watermark,
            gc_prefix_seq,
            settled_wall,
        ))
    }
    .await;
    let (
        sampled_io,
        measured_db_bytes,
        measured_peak_rss,
        final_sequence,
        durable_projected,
        final_watermark,
        gc_prefix_seq,
        settled_wall,
    ) = match post_measurement {
        Ok(result) => result,
        Err(error) => return Err(shutdown_source_after_error(source, &name, error).await),
    };

    let balances_before_restart = source.all_balances();
    if let Err(error) = source.drain_checkpoints().await {
        return Err(shutdown_source_after_error(
            source,
            &name,
            format!("final checkpoint drain failed: {error}"),
        )
        .await);
    }
    source
        .shutdown()
        .await
        .map_err(|error| format!("{name}: source shutdown failed: {error}"))?;
    let recovery_started = Instant::now();
    let reopened = AccountStore::open(trial.path(), config.users, mode, config.checkpoint_quantity)
        .await
        .map_err(|error| format!("{name}: source recovery failed: {error}"))?;
    let recovery_seconds = recovery_started.elapsed().as_secs_f64();
    let recovery_validation = async {
        let refund_history: Arc<dyn RefundHistory> = destination.clone();
        reopened
            .set_refund_history(refund_history)
            .map_err(|error| format!("cannot restore historical destination: {error}"))?;
        let restored_boundary = reopened
            .restored_projected_before()
            .await
            .map_err(|error| format!("durable watermark restore failed: {error}"))?
            .ok_or_else(|| "durable watermark metadata is absent after restart".to_owned())?;
        let boundary_valid = if projection_enabled {
            restored_boundary.0 == final_watermark && restored_boundary.1 <= durable_projected
        } else {
            restored_boundary == (initial_watermark, seed_sequence)
                && durable_projected == seed_sequence
        };
        if !boundary_valid {
            return Err(format!(
                "restored boundary {:?} does not match published watermark {final_watermark} and durable progress {durable_projected}",
                restored_boundary
            ));
        }
        if reopened.gc_prefix_seq() != gc_prefix_seq || reopened.latest_seq() != final_sequence {
            return Err(format!(
                "restart metadata changed (prefix {}, sequence {})",
                reopened.gc_prefix_seq(),
                reopened.latest_seq()
            ));
        }
        let integrity_started = Instant::now();
        reopened
            .validate_integrity()
            .await
            .map_err(|error| format!("post-restart integrity scan failed: {error}"))?;
        verify_balances(
            &reopened,
            config.users,
            100,
        )?;
        verify_historical_history(&destination, config.users, seed_time)?;
        Ok::<_, String>(integrity_started.elapsed().as_secs_f64())
    }
    .await;
    let recovered_shutdown = reopened.shutdown().await;
    let integrity_seconds = match recovery_validation {
        Ok(result) => result,
        Err(error) => {
            let error = match recovered_shutdown {
                Ok(()) => format!("{name}: {error}"),
                Err(shutdown_error) => format!(
                    "{name}: {error}; recovered source shutdown also failed: {shutdown_error}"
                ),
            };
            return Err(error);
        }
    };
    recovered_shutdown
        .map_err(|error| format!("{name}: recovered source shutdown failed: {error}"))?;

    if balances_before_restart.len() != config.users {
        return Err(format!("{name}: pre-restart balance set has wrong size"));
    }
    let sample_count = combined.stages.total.len();
    if sample_count == 0 {
        return Err(format!("{name}: no request latency samples were collected"));
    }
    if expired_pct != 0
        && (combined.stages.historical_hit_lookup.is_empty()
            || combined.stages.historical_miss_lookup.is_empty())
    {
        return Err(format!(
            "{name}: latency sampling missed the historical-hit or historical-miss class"
        ));
    }
    let checkpoint_lag_records_near_client_end = if mode == BalanceMode::Checkpoint {
        client_sequence_near_end.saturating_sub(measured_account_metrics.checkpoint_latest_seq)
    } else {
        0
    };
    let result = CaseResult {
        name,
        users: config.users,
        projection_enabled,
        gc_enabled,
        mode,
        index_lookup_mode: index_lookup.map(IndexLookupConfig::mode),
        runtime_workers: config.runtime_workers,
        requests: combined.requests,
        fresh: combined.fresh,
        credits: combined.credits,
        debits: combined.debits,
        historical_hits: combined.historical_hits,
        historical_misses: combined.historical_misses,
        measurement_started,
        expired_pct,
        client_wall,
        settled_wall,
        cpu_seconds,
        cpu_cores: cpu_seconds / client_wall.as_secs_f64().max(f64::MIN_POSITIVE),
        cpu_sample_offset_us,
        progress_sample_offset_us,
        io_sample_offset_us,
        storage_sample_offset_us,
        io: sampled_io,
        rocks: measured_rocks,
        db_bytes: measured_db_bytes,
        user_tx: summarize(&combined.stages.total),
        admission: summarize(&combined.stages.admission),
        enqueue: summarize(&combined.stages.enqueue),
        queue: summarize(&combined.stages.queue),
        batch: summarize(&combined.stages.batch),
        handler: summarize(&combined.stages.handler),
        response: summarize(&combined.stages.response),
        historical_lookup: summarize(&combined.stages.historical_lookup),
        historical_hit_lookup: summarize(&combined.stages.historical_hit_lookup),
        historical_miss_lookup: summarize(&combined.stages.historical_miss_lookup),
        projection_batches,
        gc_steps,
        checkpoint_steps,
        watermark_samples,
        account_metrics: measured_account_metrics,
        index_lookup_batches: index_lookup_batches_at_client_end,
        watermark_updates,
        initial_watermark,
        final_watermark,
        final_sequence,
        gc_prefix_seq,
        gc_prefix_near_client_end,
        latest_seq_near_client_end: client_sequence_near_end,
        projected_seq_near_client_end: projected_sequence_near_end,
        destination_seq_near_client_end: destination_sequence_near_end,
        gc_backlog_records_near_client_end,
        projection_backlog_records_near_client_end: client_sequence_near_end
            .saturating_sub(projected_sequence_near_end),
        checkpoint_lag_records_near_client_end,
        recovery_seconds,
        integrity_seconds,
        peak_rss_bytes: measured_peak_rss,
        setup_preflight,
        measure_preflight,
    };
    trial
        .cleanup()
        .map_err(|error| format!("{}: {error}", result.name))?;
    Ok(result)
}

async fn seed_source(
    source: &AccountStore,
    users: usize,
    batch_size: usize,
    seed_time: u64,
) -> Result<(), String> {
    let mut batch = Vec::with_capacity(batch_size);
    for account_id in 0..users as u64 {
        for tx_id in 1..=SEED_TRANSACTIONS_PER_USER as u64 {
            let (operation, amount) = match tx_id {
                1 => (Operation::Credit, 100),
                2 => (Operation::Credit, 1),
                3 => (Operation::Debit, 1),
                _ => return Err(format!("unexpected seed transaction ID {tx_id}")),
            };
            batch.push(Transaction {
                key: TransactionKey {
                    account_id,
                    tx_id,
                    transaction_at: seed_time,
                },
                operation,
                amount,
                refund_of: None,
            });
            if batch.len() == batch_size {
                source
                    .handle_batch(std::mem::replace(
                        &mut batch,
                        Vec::with_capacity(batch_size),
                    ))
                    .await
                    .map_err(|error| format!("seed commit failed: {error}"))?;
            }
        }
    }
    if !batch.is_empty() {
        source
            .handle_batch(batch)
            .await
            .map_err(|error| format!("final seed commit failed: {error}"))?;
    }
    Ok(())
}

async fn project_seed(
    source: &AccountStore,
    destination: &MockProjectionStore,
    target: u64,
    batch_size: usize,
) -> Result<(), String> {
    let mut next = 1;
    while next <= target {
        let read = source
            .read_ledger_range(next, batch_size)
            .await
            .map_err(|error| format!("seed projection source read failed: {error}"))?;
        if read.records.is_empty() {
            return Err(format!(
                "seed projection found no record at sequence {next}"
            ));
        }
        let projected = destination
            .apply_batch(&read.records)
            .map_err(|error| format!("seed projection apply failed: {error}"))?;
        next = projected
            .checked_add(1)
            .ok_or_else(|| "seed projection sequence overflow".to_owned())?;
    }
    source
        .persist_projection_progress(target)
        .await
        .map_err(|error| format!("seed projected-progress sync failed: {error}"))
}

async fn run_client(
    account_id: u64,
    requests: usize,
    sample_stride: u64,
    expired_pct: u8,
    seed_time: u64,
    gate: Arc<AdmissionGate>,
    queue: BatchQueue<ledger_time_boundary::AdmittedTransaction, GuardedReply>,
    history: Arc<MockProjectionStore>,
    old_lookup_delay: Duration,
    start_barrier: Arc<Barrier>,
    background_failure: watch::Receiver<Option<String>>,
    cpu_started: Arc<Mutex<ProcessTime>>,
) -> Result<ClientStats, String> {
    start_barrier.wait().await;
    let mut stats = ClientStats::default();
    for request_index in 0..requests {
        if let Some(error) = background_failure.borrow().clone() {
            return Err(format!("background failure observed by client: {error}"));
        }
        let (operation, class) =
            request_class_and_operation(account_id, request_index, expired_pct);
        let started_at = Instant::now();
        let transaction = match class {
            RequestClass::HistoricalHit => Transaction {
                key: TransactionKey {
                    account_id,
                    tx_id: match operation {
                        Operation::Credit => 2,
                        Operation::Debit => 3,
                        Operation::Refund => unreachable!("measured operations are credit/debit"),
                    },
                    transaction_at: seed_time,
                },
                operation,
                amount: 1,
                refund_of: None,
            },
            RequestClass::HistoricalMiss => {
                let request_id = account_id
                    .checked_mul(requests as u64)
                    .and_then(|base| base.checked_add(request_index as u64))
                    .ok_or_else(|| "historical request ID overflowed".to_owned())?;
                Transaction {
                    key: TransactionKey {
                        account_id,
                        tx_id: 1_000_000_u64
                            .checked_add(request_id)
                            .ok_or_else(|| "historical transaction ID overflowed".to_owned())?,
                        transaction_at: seed_time.saturating_sub(1),
                    },
                    operation,
                    amount: 1,
                    refund_of: None,
                }
            }
            RequestClass::Fresh => Transaction {
                key: TransactionKey {
                    account_id,
                    tx_id: SEED_TRANSACTIONS_PER_USER as u64 + request_index as u64 + 1,
                    // Generate transaction time immediately before admission
                    // so the retention window starts at the request boundary.
                    transaction_at: ledger_time_boundary::unix_time_micros(),
                },
                operation,
                amount: 1,
                refund_of: None,
            },
        };
        match operation {
            Operation::Credit => {
                stats.credits += 1;
                match class {
                    RequestClass::Fresh => stats.fresh_credits += 1,
                    RequestClass::HistoricalHit | RequestClass::HistoricalMiss => {
                        stats.historical_credits += 1
                    }
                }
            }
            Operation::Debit => {
                stats.debits += 1;
                match class {
                    RequestClass::Fresh => stats.fresh_debits += 1,
                    RequestClass::HistoricalHit | RequestClass::HistoricalMiss => {
                        stats.historical_debits += 1
                    }
                }
            }
            Operation::Refund => unreachable!("measured operations are credit/debit"),
        }
        let outcome = ledger_time_boundary::route_request(
            &gate,
            &queue,
            &history,
            old_lookup_delay,
            transaction,
            started_at,
        )
        .await?;
        if request_index + 1 == requests {
            let reply_at = Instant::now();
            let cpu_sample = ProcessTime::now();
            let cpu_sampled_at = Instant::now();
            let cpu_start = *cpu_started
                .lock()
                .map_err(|_| "CPU start timer mutex poisoned".to_owned())?;
            stats.completion = Some(ClientCompletion {
                reply_at,
                cpu_seconds: cpu_sample.duration_since(cpu_start).as_secs_f64(),
                cpu_sampled_at,
            });
        }
        stats.requests += 1;
        let logical_id = account_id
            .saturating_mul(requests as u64)
            .saturating_add(request_index as u64);
        let sampled = splitmix64(logical_id) % sample_stride == 0;
        if sampled {
            stats.stages.record(outcome.stages);
            match class {
                RequestClass::HistoricalHit => {
                    stats.stages.record_historical_lookup(outcome.stages, true)
                }
                RequestClass::HistoricalMiss => {
                    stats.stages.record_historical_lookup(outcome.stages, false)
                }
                RequestClass::Fresh => {}
            }
        }
        match class {
            RequestClass::HistoricalHit => match outcome.reply {
                RoutedReply::HistoricalHit(result)
                    if result.status == TransactionStatus::Applied
                        && result.balance
                            == match operation {
                                Operation::Credit => 101,
                                Operation::Debit => 100,
                                Operation::Refund => {
                                    unreachable!("measured operations are credit/debit")
                                }
                            } =>
                {
                    stats.historical_hits += 1;
                }
                reply => {
                    return Err(format!(
                        "expected historical hit for user {account_id}, request {request_index}; got {reply:?}"
                    ));
                }
            },
            RequestClass::HistoricalMiss => match outcome.reply {
                RoutedReply::HistoricalMiss => stats.historical_misses += 1,
                reply => {
                    return Err(format!(
                        "expected historical miss for user {account_id}, request {request_index}; got {reply:?}"
                    ));
                }
            },
            RequestClass::Fresh => match outcome.reply {
                RoutedReply::Commit(Reply::Transaction {
                    status: TransactionStatus::Applied,
                    ..
                }) => stats.fresh += 1,
                reply => {
                    return Err(format!(
                        "expected fresh applied commit for user {account_id}, request {request_index}; got {reply:?}"
                    ));
                }
            },
        }
    }
    let expected_historical_each = (requests as u64 * u64::from(expired_pct) / 100) / 2;
    let expected_fresh = requests as u64 - expected_historical_each * 2;
    let expected_fresh_each_operation = expected_fresh / 2;
    if stats.historical_hits != expected_historical_each
        || stats.historical_misses != expected_historical_each
        || stats.fresh != expected_fresh
        || stats.credits != requests as u64 / 2
        || stats.debits != requests as u64 / 2
        || stats.fresh_credits != expected_fresh_each_operation
        || stats.fresh_debits != expected_fresh_each_operation
        || stats.historical_credits != expected_historical_each
        || stats.historical_debits != expected_historical_each
    {
        return Err(format!(
            "user {account_id} workload counts differ from fixed mix: credit={}, debit={}, fresh_credit={}, fresh_debit={}, historical_credit={}, historical_debit={}, hits={}, misses={}",
            stats.credits,
            stats.debits,
            stats.fresh_credits,
            stats.fresh_debits,
            stats.historical_credits,
            stats.historical_debits,
            stats.historical_hits,
            stats.historical_misses
        ));
    }
    Ok(stats)
}

fn combine_client_stats(total: &mut ClientStats, mut next: ClientStats) {
    total.requests += next.requests;
    total.fresh += next.fresh;
    total.credits += next.credits;
    total.debits += next.debits;
    total.fresh_credits += next.fresh_credits;
    total.fresh_debits += next.fresh_debits;
    total.historical_credits += next.historical_credits;
    total.historical_debits += next.historical_debits;
    total.historical_hits += next.historical_hits;
    total.historical_misses += next.historical_misses;
    if next.completion.is_some_and(|completion| {
        total
            .completion
            .is_none_or(|current| completion.reply_at > current.reply_at)
    }) {
        total.completion = next.completion;
    }
    total.stages.total.append(&mut next.stages.total);
    total.stages.admission.append(&mut next.stages.admission);
    total.stages.enqueue.append(&mut next.stages.enqueue);
    total.stages.queue.append(&mut next.stages.queue);
    total.stages.batch.append(&mut next.stages.batch);
    total.stages.handler.append(&mut next.stages.handler);
    total.stages.response.append(&mut next.stages.response);
    total
        .stages
        .historical_lookup
        .append(&mut next.stages.historical_lookup);
    total
        .stages
        .historical_hit_lookup
        .append(&mut next.stages.historical_hit_lookup);
    total
        .stages
        .historical_miss_lookup
        .append(&mut next.stages.historical_miss_lookup);
}

fn spawn_durable_projector(
    source: AccountStore,
    destination: Arc<MockProjectionStore>,
    progress: Arc<ProjectionProgress>,
    samples: Arc<Mutex<Vec<ProjectionStageSample>>>,
    background_failure: watch::Sender<Option<String>>,
    mut committed_head: watch::Receiver<u64>,
    mut shutdown: watch::Receiver<bool>,
    batch_size: usize,
    start_barrier: Arc<Barrier>,
) -> JoinHandle<Result<(), String>> {
    tokio::spawn(async move {
        start_barrier.wait().await;
        let started_at = Instant::now();
        let result = async {
            if batch_size == 0 {
                return Err("projection batch size must be positive".to_owned());
            }
            let mut next_sequence = source
                .durable_projection_progress()
                .await?
                .checked_add(1)
                .ok_or_else(|| "projected sequence overflow".to_owned())?;
            loop {
                if *shutdown.borrow() {
                    return Ok(());
                }
                if next_sequence <= source.latest_seq() {
                    let total_started = Instant::now();
                    let read = source.read_ledger_range(next_sequence, batch_size).await?;
                    if read.records.is_empty() {
                        return Err(format!(
                            "projector found no source record at sequence {next_sequence}"
                        ));
                    }
                    let apply_started = Instant::now();
                    let projected = destination.apply_batch(&read.records)?;
                    let apply_ns = elapsed_ns(apply_started.elapsed());
                    let sync_started = Instant::now();
                    source.persist_projection_progress(projected).await?;
                    let sync_ns = elapsed_ns(sync_started.elapsed());
                    progress.acknowledge(projected)?;
                    let completed_at = Instant::now();
                    samples
                        .lock()
                        .map_err(|_| "projection metrics mutex poisoned".to_owned())?
                        .push(ProjectionStageSample {
                            sequence: projected,
                            records: read.records.len(),
                            read_ns: read.db_read_ns,
                            apply_ns,
                            progress_sync_ns: sync_ns,
                            total_ns: elapsed_ns(total_started.elapsed()),
                            elapsed_ns: elapsed_ns(started_at.elapsed()),
                            completed_at,
                        });
                    next_sequence = projected
                        .checked_add(1)
                        .ok_or_else(|| "projected sequence overflow".to_owned())?;
                    continue;
                }
                tokio::select! {
                    changed = committed_head.changed() => {
                        if changed.is_err() { return Ok(()); }
                    }
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() { return Ok(()); }
                    }
                }
            }
        }
        .await;
        if let Err(error) = &result {
            progress.fail(error.clone());
            background_failure.send_replace(Some(format!("durable projector failed: {error}")));
        }
        result
    })
}

fn spawn_watermark_manager(
    manager: Arc<WatermarkManager>,
    source: AccountStore,
    retention: Duration,
    interval: Duration,
    shutdown: watch::Receiver<bool>,
    background_failure: watch::Sender<Option<String>>,
    start_barrier: Arc<Barrier>,
) -> JoinHandle<Result<(), String>> {
    tokio::spawn(async move {
        start_barrier.wait().await;
        let result = Arc::clone(&manager)
            .run_periodic_durable(source, retention, interval, shutdown)
            .await;
        if let Err(error) = &result {
            background_failure.send_replace(Some(format!("watermark manager failed: {error}")));
        }
        result
    })
}

fn spawn_gc_worker(
    source: AccountStore,
    batch_size: usize,
    interval: Duration,
    mut shutdown: watch::Receiver<bool>,
    background_failure: watch::Sender<Option<String>>,
    samples: Arc<Mutex<Vec<TimedGcStep>>>,
    start_barrier: Arc<Barrier>,
) -> JoinHandle<Result<(), String>> {
    tokio::spawn(async move {
        start_barrier.wait().await;
        let started_at = Instant::now();
        let result = async {
            if interval.is_zero() {
                return Err("GC interval must be positive".to_owned());
            }
            loop {
                if *shutdown.borrow() {
                    return Ok(());
                }
                let prefix_before = source.gc_prefix_seq();
                let step_started = Instant::now();
                let outcome = source.collect_garbage(batch_size).await?;
                let completed_at = Instant::now();
                let made_progress = outcome.gc_prefix_seq > prefix_before;
                let blocked = outcome.blocked_at_seq.is_some();
                samples
                    .lock()
                    .map_err(|_| "GC metrics mutex poisoned".to_owned())?
                    .push(TimedGcStep {
                        elapsed_ns: elapsed_ns(started_at.elapsed()),
                        duration_ns: elapsed_ns(completed_at.duration_since(step_started)),
                        completed_at,
                        outcome,
                    });
                if made_progress && !blocked {
                    // Keep up with an eligible prefix, but let commit and
                    // projection tasks run between bounded sync batches.
                    tokio::task::yield_now().await;
                    continue;
                }
                tokio::select! {
                    _ = tokio::time::sleep(interval) => {},
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() { return Ok(()); }
                    }
                }
            }
        }
        .await;
        if let Err(error) = &result {
            background_failure.send_replace(Some(format!("safe GC worker failed: {error}")));
        }
        result
    })
}

async fn join_background(name: &str, task: JoinHandle<Result<(), String>>) -> Result<(), String> {
    task.await
        .map_err(|error| format!("{name}: background task panicked: {error}"))?
        .map_err(|error| format!("{name}: background task failed: {error}"))
}

async fn drain_pipeline_workers(
    name: &str,
    shutdown: watch::Sender<bool>,
    queue: BatchQueue<
        ledger_time_boundary::AdmittedTransaction,
        ledger_time_boundary::GuardedReply,
    >,
    worker: BatchWorker,
    projector: Option<JoinHandle<Result<(), String>>>,
    watermark_task: Option<JoinHandle<Result<(), String>>>,
    gc_task: Option<JoinHandle<Result<(), String>>>,
) -> Vec<String> {
    shutdown.send_replace(true);
    drop(queue);
    let mut errors = Vec::new();
    if let Err(error) = worker.join().await {
        errors.push(format!("{name}: commit worker failed: {error}"));
    }
    if let Some(task) = projector {
        if let Err(error) = join_background(name, task).await {
            errors.push(error);
        }
    }
    if let Some(task) = watermark_task {
        if let Err(error) = join_background(name, task).await {
            errors.push(error);
        }
    }
    if let Some(task) = gc_task {
        if let Err(error) = join_background(name, task).await {
            errors.push(error);
        }
    }
    errors
}

fn include_watch_failure(mut errors: Vec<String>, failure: Option<String>) -> Vec<String> {
    if let Some(failure) = failure {
        if !errors.iter().any(|error| error.contains(&failure)) {
            errors.push(failure);
        }
    }
    errors
}

fn include_teardown_errors(mut primary: String, teardown_errors: Vec<String>) -> String {
    if !teardown_errors.is_empty() {
        primary.push_str("; teardown: ");
        primary.push_str(&teardown_errors.join("; "));
    }
    primary
}

async fn shutdown_source_after_error(source: AccountStore, name: &str, error: String) -> String {
    match source.shutdown().await {
        Ok(()) => format!("{name}: {error}"),
        Err(shutdown_error) => {
            format!("{name}: {error}; source shutdown also failed: {shutdown_error}")
        }
    }
}

fn elapsed_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

async fn verify_case(
    source: &AccountStore,
    history: &MockProjectionStore,
    config: &Config,
    seed_time: u64,
    expired_pct: u8,
    gc_enabled: bool,
    projection_enabled: bool,
    clients: &ClientStats,
    final_sequence: u64,
    gc_prefix_seq: u64,
    seed_sequence: u64,
) -> Result<(), String> {
    let requests = config.total_requests();
    let historical = requests
        .checked_mul(u64::from(expired_pct))
        .ok_or_else(|| "expected history count overflowed".to_owned())?
        / 100;
    let expected_fresh = requests - historical;
    let expected_old_each = historical / 2;
    let expected_fresh_each_operation = expected_fresh / 2;
    let expected_historical_each_operation = historical / 2;
    if clients.requests != requests
        || clients.fresh != expected_fresh
        || clients.historical_hits != expected_old_each
        || clients.historical_misses != expected_old_each
        || clients.credits != requests / 2
        || clients.debits != requests / 2
        || clients.fresh_credits != expected_fresh_each_operation
        || clients.fresh_debits != expected_fresh_each_operation
        || clients.historical_credits != expected_historical_each_operation
        || clients.historical_debits != expected_historical_each_operation
    {
        return Err(format!(
            "request counts differ from fixed workload: requests={}, credit={}, debit={}, fresh={}, fresh_credit={}, fresh_debit={}, historical_credit={}, historical_debit={}, hits={}, misses={}",
            clients.requests,
            clients.credits,
            clients.debits,
            clients.fresh,
            clients.fresh_credits,
            clients.fresh_debits,
            clients.historical_credits,
            clients.historical_debits,
            clients.historical_hits,
            clients.historical_misses
        ));
    }
    if final_sequence != config.users as u64 * SEED_TRANSACTIONS_PER_USER as u64 + expected_fresh {
        return Err(format!(
            "final source sequence {final_sequence} differs from seeds plus fresh commits"
        ));
    }
    if gc_prefix_seq > source.latest_seq()
        || (projection_enabled && gc_prefix_seq > source.projected_seq())
        || (projection_enabled && history.progress() < source.projected_seq())
        || (!projection_enabled
            && (source.projected_seq() != seed_sequence || history.progress() != seed_sequence))
    {
        return Err(format!(
            "GC/projection invariant failed: prefix={gc_prefix_seq}, latest={}, source projection={}, destination projection={}",
            source.latest_seq(),
            source.projected_seq(),
            history.progress()
        ));
    }
    if gc_enabled && gc_prefix_seq == 0 {
        return Err(
            "GC-on case completed without collecting its eligible seeded prefix".to_owned(),
        );
    }
    if !gc_enabled && gc_prefix_seq != 0 {
        return Err("GC-off case advanced the source GC prefix".to_owned());
    }
    if !projection_enabled
        && (source.durable_projection_progress().await? != seed_sequence
            || final_sequence < seed_sequence)
    {
        return Err("foreground-only trial changed durable seeded projection progress".to_owned());
    }
    source
        .validate_integrity()
        .await
        .map_err(|error| format!("source integrity validation failed: {error}"))?;
    verify_balances(source, config.users, 100)?;

    verify_historical_history(history, config.users, seed_time)
}

fn verify_historical_history(
    history: &MockProjectionStore,
    users: usize,
    seed_time: u64,
) -> Result<(), String> {
    // Recheck both operation kinds and an absent key for every user after
    // source GC and source reopen; the destination remains query authority.
    for account_id in 0..users as u64 {
        for (tx_id, operation, expected_balance) in
            [(2, Operation::Credit, 101), (3, Operation::Debit, 100)]
        {
            let exact = Transaction {
                key: TransactionKey {
                    account_id,
                    tx_id,
                    transaction_at: seed_time,
                },
                operation,
                amount: 1,
                refund_of: None,
            };
            if !matches!(
                history
                    .lookup_transaction(&exact)
                    .map_err(|error| format!("historical exact lookup failed: {error}"))?,
                HistoricalLookup::ExactReplay(result)
                    if result.status == TransactionStatus::Applied
                        && result.balance == expected_balance
            ) {
                return Err(format!(
                    "historical exact seed key {tx_id} missing or changed for user {account_id}"
                ));
            }
        }
        let miss = Transaction {
            key: TransactionKey {
                account_id,
                tx_id: u64::MAX - account_id,
                transaction_at: seed_time.saturating_sub(1),
            },
            operation: Operation::Credit,
            amount: 1,
            refund_of: None,
        };
        if history
            .lookup_transaction(&miss)
            .map_err(|error| format!("historical miss lookup failed: {error}"))?
            != HistoricalLookup::NotFound
        {
            return Err(format!(
                "historical miss unexpectedly exists for user {account_id}"
            ));
        }
    }
    Ok(())
}

fn verify_balances(source: &AccountStore, users: usize, expected: u64) -> Result<(), String> {
    let balances = source.all_balances();
    if balances.len() != users {
        return Err(format!(
            "balance count {} does not match configured user count {users}",
            balances.len()
        ));
    }
    for (account_id, balance) in balances {
        if account_id >= users as u64 || balance != expected {
            return Err(format!(
                "balance mismatch for account {account_id}: expected {expected}, got {balance}"
            ));
        }
    }
    Ok(())
}

fn subtract_rocks(
    after: (u64, u64, u64, u64, u64, u64, u64),
    before: (u64, u64, u64, u64, u64, u64, u64),
) -> (u64, u64, u64, u64, u64, u64, u64) {
    (
        delta(after.0, before.0),
        delta(after.1, before.1),
        delta(after.2, before.2),
        delta(after.3, before.3),
        delta(after.4, before.4),
        delta(after.5, before.5),
        delta(after.6, before.6),
    )
}

fn print_case_summary(result: &CaseResult) {
    let rps = result.requests as f64 / result.client_wall.as_secs_f64().max(f64::MIN_POSITIVE);
    println!(
        "CASE name={} users={} requests={} fresh={} old_hit={} old_miss={} projection_enabled={} gc_enabled={} runtime_workers={} wall_s={:.3} settled_s={:.3} rps={:.1} cpu_s={:.3} cpu_cores={:.3} peak_rss_bytes={} db_bytes={} gc_prefix_near_client_end={} gc_backlog_records_near_client_end={} gc_prefix_after_settle={} final_seq={} watermark_updates={} recovery_s={:.6} integrity_s={:.6}",
        result.name,
        result.users,
        result.requests,
        result.fresh,
        result.historical_hits,
        result.historical_misses,
        result.projection_enabled,
        result.gc_enabled,
        result.runtime_workers,
        result.client_wall.as_secs_f64(),
        result.settled_wall.as_secs_f64(),
        rps,
        result.cpu_seconds,
        result.cpu_cores,
        result.peak_rss_bytes,
        result.db_bytes,
        result.gc_prefix_near_client_end,
        result.gc_backlog_records_near_client_end,
        result.gc_prefix_seq,
        result.final_sequence,
        result.watermark_updates,
        result.recovery_seconds,
        result.integrity_seconds
    );
    println!(
        "CASE_IO name={} cpu_sample_offset_us={} progress_sample_offset_us={} io_sample_offset_us={} storage_sample_offset_us={} device={}({}) process_rchar={} process_wchar={} process_read={} process_write={} device_read={} device_write={} device_busy_ms={} wal_syncs={} wal_bytes={} writes_with_wal={} flush_write_bytes={} compact_read_bytes={} compact_write_bytes={} stall_us={} projected_sync_ns={} gc_scan_ns={} gc_delete_ns={} gc_write_ns={} gc_scanned={} gc_deleted={} gc_deleted_bytes={}",
        result.name,
        result.cpu_sample_offset_us,
        result.progress_sample_offset_us,
        result.io_sample_offset_us,
        result.storage_sample_offset_us,
        result.io.target_device,
        result.io.target_major_minor,
        result.io.process_rchar_bytes,
        result.io.process_wchar_bytes,
        result.io.process_read_bytes,
        result.io.process_write_bytes,
        result.io.target_read_bytes,
        result.io.target_write_bytes,
        result.io.target_busy_ms,
        result.rocks.0,
        result.rocks.1,
        result.rocks.2,
        result.rocks.3,
        result.rocks.4,
        result.rocks.5,
        result.rocks.6,
        result.account_metrics.projection_progress_sync_ns,
        result.account_metrics.gc_scan_ns,
        result.account_metrics.gc_delete_ns,
        result.account_metrics.gc_write_ns,
        result.account_metrics.gc_records_scanned,
        result.account_metrics.gc_records_deleted,
        result.account_metrics.gc_bytes_deleted
    );
    for (stage, summary) in case_latency_summaries(result) {
        println!(
            "LATENCY name={} stage={} unit=ns samples={} p50={} p95={} p99={}",
            result.name,
            stage,
            summary.sample_count,
            summary.p50_ns,
            summary.p95_ns,
            summary.p99_ns
        );
    }
    let projection_records: usize = result
        .projection_batches
        .iter()
        .map(|sample| sample.records)
        .sum();
    let gc_records: u64 = result
        .gc_steps
        .iter()
        .map(|sample| sample.outcome.scanned)
        .sum();
    println!(
        "BACKGROUND name={} projection_batches={} projection_records={} gc_steps={} gc_scanned_during_clients={} gc_deleted_during_clients={} gc_backlog_records_near_client_end={} setup_preflight_cpu_pct={:.2} setup_preflight_disk_pct={:.2} measure_preflight_cpu_pct={:.2} measure_preflight_disk_pct={:.2}",
        result.name,
        result.projection_batches.len(),
        projection_records,
        result.gc_steps.len(),
        gc_records,
        result
            .gc_steps
            .iter()
            .map(|sample| sample.outcome.deleted)
            .sum::<u64>(),
        result.gc_backlog_records_near_client_end,
        result.setup_preflight.cpu_busy_pct,
        result.setup_preflight.disk_busy_pct,
        result.measure_preflight.cpu_busy_pct,
        result.measure_preflight.disk_busy_pct
    );
}

fn write_archive(
    archive: &Path,
    config: &Config,
    results: &[CaseResult],
    setup_preflight: &PreflightReport,
) -> Result<(), String> {
    let summary_path = archive.join("ledger_pipeline_summary.csv");
    let latency_path = archive.join("ledger_pipeline_stages.csv");
    let background_path = archive.join("ledger_pipeline_background.csv");
    let log_path = archive.join("ledger_pipeline_run.log");
    let index_lookup_path = archive.join("ledger_pipeline_index_lookup.csv");
    let mut summary = BufWriter::new(
        File::create(&summary_path)
            .map_err(|error| format!("cannot create {}: {error}", summary_path.display()))?,
    );
    let mut latency = BufWriter::new(
        File::create(&latency_path)
            .map_err(|error| format!("cannot create {}: {error}", latency_path.display()))?,
    );
    let mut background = BufWriter::new(
        File::create(&background_path)
            .map_err(|error| format!("cannot create {}: {error}", background_path.display()))?,
    );
    let mut log = BufWriter::new(
        File::create(&log_path)
            .map_err(|error| format!("cannot create {}: {error}", log_path.display()))?,
    );
    let mut index_lookup = BufWriter::new(
        File::create(&index_lookup_path)
            .map_err(|error| format!("cannot create {}: {error}", index_lookup_path.display()))?,
    );

    writeln!(
        summary,
        "case,users,coroutines,requests,expired_pct,credit_requests,debit_requests,fresh_commits,fresh_credits,fresh_debits,historical_credits,historical_debits,historical_hits,historical_misses,projection_enabled,gc_enabled,balance_mode,runtime_workers,client_wall_s,settle_after_client_s,rps,cpu_s,cpu_core_equivalents,cpu_sample_offset_us,progress_sample_offset_us,io_sample_offset_us,storage_sample_offset_us,peak_rss_bytes,db_bytes,process_rchar_bytes,process_wchar_bytes,process_read_bytes,process_write_bytes,device,major_minor,device_read_bytes,device_write_bytes,device_busy_ms,wal_syncs,wal_bytes,writes_with_wal,flush_write_bytes,compact_read_bytes,compact_write_bytes,stall_us,latest_seq_near_client_end,projected_seq_near_client_end,destination_seq_near_client_end,projection_backlog_records_near_client_end,gc_prefix_near_client_end,gc_backlog_records_near_client_end,gc_prefix_after_settle,initial_watermark,final_watermark,final_sequence,watermark_updates,checkpoint_count,checkpoint_snapshots_enqueued,checkpoint_snapshot_ns,checkpoint_queue_wait_ns,checkpoint_chunk_sync_ns,checkpoint_manifest_sync_ns,checkpoint_duration_ns,checkpoint_latest_seq,checkpoint_lag_records_near_client_end,projection_progress_sync_ns,gc_scan_ns,gc_delete_ns,gc_write_ns,gc_records_scanned,gc_records_deleted,gc_deleted_bytes,recovery_s,integrity_s,setup_preflight_cpu_pct,setup_preflight_disk_pct,measure_preflight_cpu_pct,measure_preflight_disk_pct"
    )
    .map_err(|error| format!("cannot write summary header: {error}"))?;
    writeln!(latency, "case,stage,unit,sample_count,p50,p95,p99")
        .map_err(|error| format!("cannot write stage header: {error}"))?;
    write_csv_record(
        &mut index_lookup,
        [
            "case",
            "index_lookup_strategy",
            "index_group_size",
            "index_concurrency",
            "runtime_workers",
            "batch_count",
            "lookup_requests",
            "native_calls",
            "hits",
            "misses",
            "max_in_flight_groups",
            "max_running_query_jobs",
        ]
        .into_iter()
        .map(str::to_owned),
    )
    .map_err(|error| format!("cannot write index-lookup metadata header: {error}"))?;
    writeln!(
        background,
        "case,event,elapsed_ns,sequence,records,read_ns,apply_ns,progress_sync_ns,checkpoint_chunk_sync_ns,checkpoint_manifest_sync_ns,total_ns,gc_scanned,gc_deleted,gc_bytes_deleted,gc_prefix_seq,gc_blocked_at_seq,gc_scan_ns,gc_delete_ns,gc_write_ns,watermark_target_sequence,watermark_value,watermark_fence_wait_ns,watermark_projection_wait_ns,watermark_persist_ns"
    )
    .map_err(|error| format!("cannot write background header: {error}"))?;

    writeln!(
        log,
        "Tokio single-shard integrated credit/debit ledger pipeline benchmark\nusers={} coroutines={} requests_per_user={} requests_per_case={} cases={} seed_transactions_per_user={} seed_records={} queue_capacity={} batch_size={} batch_timeout_ms={} projection_batch_size={} gc_batch_size={} gc_interval_ms={} retention_ms={} watermark_interval_ms={} checkpoint_quantity={} sample_stride={} old_lookup_delay_ms={} runtime_workers={} transaction_at=unix_micros_generated_immediately_before_fresh_admission\nworkload=each 200-request block uses a deterministic multiplier-37 permutation with account-specific offset; slot parity selects Credit or Debit for exact 50/50 operation counts; every request amount is 1. At 0% history every request is fresh. At 5% history each user block has 10 historical requests (5 exact hits, 5 misses; 5 Credit, 5 Debit) and 190 fresh requests (95 Credit, 95 Debit). Hit keys replay the seeded amount-1 Credit and Debit records; miss keys are unique and absent. Per-case fresh commits are derived from the history rate; three seeds per user establish balance 100 and history targets outside the measured request count.\nmeasurement=wall and process CPU start immediately before client release and end at the latest coroutine's last route reply. Client latencies exclude post-reply count validation and sample aggregation. ProcessTime is sampled immediately after that reply; process and target-device I/O and RocksDB/DB-size snapshots follow after JoinSet collection and their offsets are recorded, so nonzero offsets include post-reply tail work. Final projection catch-up, watermark advance, GC sweep, integrity scan and close/reopen recovery are settlement stages outside client RPS and latency. Request latency uses deterministic hash sampling at stride {}; stage percentiles use nearest-rank samples and are non-additive. Projection/GC/checkpoint/watermark background events are included only when their completion is at or before client_end.\ntrial_profile=projection_enabled_per_row; when false projector, watermark manager and GC are not spawned or measured, and seed projection/boundary metadata remain unchanged through reopen.\nGC=one safe contiguous ledger prefix; stop at the first record at or newer than durable W; require durable projection and balance coverage; bounded batches use a Tokio yield while prefix advances and sleep when blocked/no progress; watermark and retention are stress settings. Logical RocksDB deletes use synchronous WAL batches; no forced compaction.\nrecovery=source RocksDB is closed and reopened while the same in-memory mock destination is retained. A successful mock apply is treated as durable by contract; this does not assert process-crash durability for an external destination.\npreflight_setup=path:{} filesystem:{} device:{}({}) attempts:{} cpu_busy_pct:{:.3} disk_busy_pct:{:.3} available_mem_bytes:{} free_bytes:{}\n",
        config.users,
        config.users,
        config.requests_per_user,
        config.total_requests(),
        results.len(),
        SEED_TRANSACTIONS_PER_USER,
        config.users * SEED_TRANSACTIONS_PER_USER,
        config.queue_capacity,
        config.batch_size,
        config.batch_timeout.as_millis(),
        config.projection_batch_size,
        config.gc_batch_size,
        config.gc_interval.as_millis(),
        config.retention.as_millis(),
        config.watermark_interval.as_millis(),
        config.checkpoint_quantity,
        config.sample_stride,
        config.old_lookup_delay.as_millis(),
        config.runtime_workers,
        config.sample_stride,
        setup_preflight.path.display(),
        setup_preflight.filesystem,
        setup_preflight.device,
        setup_preflight.major_minor,
        setup_preflight.attempts,
        setup_preflight.cpu_busy_pct,
        setup_preflight.disk_busy_pct,
        setup_preflight.mem_available_bytes,
        setup_preflight.free_bytes,
    )
    .map_err(|error| format!("cannot write run metadata: {error}"))?;

    for result in results {
        let rps = result.requests as f64 / result.client_wall.as_secs_f64().max(f64::MIN_POSITIVE);
        let historical = result.requests * u64::from(result.expired_pct) / 100;
        let historical_each_operation = historical / 2;
        let fresh_credits = result.credits.saturating_sub(historical_each_operation);
        let fresh_debits = result.debits.saturating_sub(historical_each_operation);
        let row = vec![
            csv(&result.name),
            result.users.to_string(),
            result.users.to_string(),
            result.requests.to_string(),
            result.expired_pct.to_string(),
            result.credits.to_string(),
            result.debits.to_string(),
            result.fresh.to_string(),
            fresh_credits.to_string(),
            fresh_debits.to_string(),
            historical_each_operation.to_string(),
            historical_each_operation.to_string(),
            result.historical_hits.to_string(),
            result.historical_misses.to_string(),
            result.projection_enabled.to_string(),
            result.gc_enabled.to_string(),
            mode_name(result.mode).to_owned(),
            result.runtime_workers.to_string(),
            format!("{:.6}", result.client_wall.as_secs_f64()),
            format!("{:.6}", result.settled_wall.as_secs_f64()),
            format!("{rps:.3}"),
            format!("{:.6}", result.cpu_seconds),
            format!("{:.6}", result.cpu_cores),
            result.cpu_sample_offset_us.to_string(),
            result.progress_sample_offset_us.to_string(),
            result.io_sample_offset_us.to_string(),
            result.storage_sample_offset_us.to_string(),
            result.peak_rss_bytes.to_string(),
            result.db_bytes.to_string(),
            result.io.process_rchar_bytes.to_string(),
            result.io.process_wchar_bytes.to_string(),
            result.io.process_read_bytes.to_string(),
            result.io.process_write_bytes.to_string(),
            csv(&result.io.target_device),
            csv(&result.io.target_major_minor),
            result.io.target_read_bytes.to_string(),
            result.io.target_write_bytes.to_string(),
            result.io.target_busy_ms.to_string(),
            result.rocks.0.to_string(),
            result.rocks.1.to_string(),
            result.rocks.2.to_string(),
            result.rocks.3.to_string(),
            result.rocks.4.to_string(),
            result.rocks.5.to_string(),
            result.rocks.6.to_string(),
            result.latest_seq_near_client_end.to_string(),
            result.projected_seq_near_client_end.to_string(),
            result.destination_seq_near_client_end.to_string(),
            result
                .projection_backlog_records_near_client_end
                .to_string(),
            result.gc_prefix_near_client_end.to_string(),
            result.gc_backlog_records_near_client_end.to_string(),
            result.gc_prefix_seq.to_string(),
            result.initial_watermark.to_string(),
            result.final_watermark.to_string(),
            result.final_sequence.to_string(),
            result.watermark_updates.to_string(),
            result.account_metrics.checkpoint_count.to_string(),
            result
                .account_metrics
                .checkpoint_snapshots_enqueued
                .to_string(),
            result.account_metrics.checkpoint_snapshot_ns.to_string(),
            result.account_metrics.checkpoint_queue_wait_ns.to_string(),
            result.account_metrics.checkpoint_chunk_sync_ns.to_string(),
            result
                .account_metrics
                .checkpoint_manifest_sync_ns
                .to_string(),
            result.account_metrics.checkpoint_duration_ns.to_string(),
            result.account_metrics.checkpoint_latest_seq.to_string(),
            result.checkpoint_lag_records_near_client_end.to_string(),
            result
                .account_metrics
                .projection_progress_sync_ns
                .to_string(),
            result.account_metrics.gc_scan_ns.to_string(),
            result.account_metrics.gc_delete_ns.to_string(),
            result.account_metrics.gc_write_ns.to_string(),
            result.account_metrics.gc_records_scanned.to_string(),
            result.account_metrics.gc_records_deleted.to_string(),
            result.account_metrics.gc_bytes_deleted.to_string(),
            format!("{:.6}", result.recovery_seconds),
            format!("{:.6}", result.integrity_seconds),
            format!("{:.3}", result.setup_preflight.cpu_busy_pct),
            format!("{:.3}", result.setup_preflight.disk_busy_pct),
            format!("{:.3}", result.measure_preflight.cpu_busy_pct),
            format!("{:.3}", result.measure_preflight.disk_busy_pct),
        ];
        writeln!(summary, "{}", row.join(","))
            .map_err(|error| format!("cannot write summary row: {error}"))?;

        let (lookup_strategy, lookup_group_size, lookup_concurrency) =
            index_lookup_details(result.index_lookup_mode);
        let lookup_totals = sum_index_lookup_metrics(&result.index_lookup_batches);
        let max_lookup_in_flight = result
            .index_lookup_batches
            .iter()
            .map(|batch| batch.max_observed_in_flight_groups)
            .max()
            .unwrap_or(0);
        let max_lookup_running = result
            .index_lookup_batches
            .iter()
            .map(|batch| batch.max_observed_running_query_jobs)
            .max()
            .unwrap_or(0);
        write_csv_record(
            &mut index_lookup,
            [
                result.name.clone(),
                lookup_strategy,
                lookup_group_size,
                lookup_concurrency,
                result.runtime_workers.to_string(),
                result.index_lookup_batches.len().to_string(),
                lookup_totals.keys_looked_up.to_string(),
                lookup_totals.native_get_calls.to_string(),
                lookup_totals.hits.to_string(),
                lookup_totals.misses.to_string(),
                max_lookup_in_flight.to_string(),
                max_lookup_running.to_string(),
            ],
        )
        .map_err(|error| format!("cannot write index-lookup metadata row: {error}"))?;

        for (stage, values) in case_latency_summaries(result) {
            writeln!(
                latency,
                "{},{},ns,{},{},{},{}",
                csv(&result.name),
                stage,
                values.sample_count,
                values.p50_ns,
                values.p95_ns,
                values.p99_ns
            )
            .map_err(|error| format!("cannot write stage sample: {error}"))?;
        }
        for sample in &result.projection_batches {
            let mut row = vec![String::new(); 24];
            row[0] = result.name.clone();
            row[1] = "projection".to_owned();
            row[2] = sample.elapsed_ns.to_string();
            row[3] = sample.sequence.to_string();
            row[4] = sample.records.to_string();
            row[5] = sample.read_ns.to_string();
            row[6] = sample.apply_ns.to_string();
            row[7] = sample.progress_sync_ns.to_string();
            row[10] = sample.total_ns.to_string();
            write_csv_row(&mut background, &row, "projection sample")?;
        }
        for step in &result.checkpoint_steps {
            let sample = step.sample;
            let mut row = vec![String::new(); 24];
            row[0] = result.name.clone();
            row[1] = "checkpoint".to_owned();
            row[2] = step.elapsed_ns.to_string();
            row[3] = sample.sequence.to_string();
            row[8] = sample.chunk_sync_ns.to_string();
            row[9] = sample.manifest_sync_ns.to_string();
            row[10] = sample.duration_ns.to_string();
            write_csv_row(&mut background, &row, "checkpoint sample")?;
        }
        for sample in &result.gc_steps {
            let step = sample.outcome;
            let mut row = vec![String::new(); 24];
            row[0] = result.name.clone();
            row[1] = "gc".to_owned();
            row[2] = sample.elapsed_ns.to_string();
            row[3] = step.gc_prefix_seq.to_string();
            row[4] = step.scanned.to_string();
            row[10] = sample.duration_ns.to_string();
            row[11] = step.scanned.to_string();
            row[12] = step.deleted.to_string();
            row[13] = step.bytes_deleted.to_string();
            row[14] = step.gc_prefix_seq.to_string();
            row[15] = step
                .blocked_at_seq
                .map(|sequence| sequence.to_string())
                .unwrap_or_default();
            row[16] = step.scan_ns.to_string();
            row[17] = step.delete_ns.to_string();
            row[18] = step.write_ns.to_string();
            write_csv_row(&mut background, &row, "GC sample")?;
        }
        for sample in &result.watermark_samples {
            let elapsed = elapsed_ns(
                sample
                    .completed_at
                    .duration_since(result.measurement_started),
            );
            let mut row = vec![String::new(); 24];
            row[0] = result.name.clone();
            row[1] = "watermark".to_owned();
            row[2] = elapsed.to_string();
            row[10] = sample.total_ns.to_string();
            row[19] = sample.target_sequence.to_string();
            row[20] = sample.published_watermark.to_string();
            row[21] = sample.fence_wait_ns.to_string();
            row[22] = sample.projection_wait_ns.to_string();
            row[23] = sample.persist_ns.to_string();
            write_csv_row(&mut background, &row, "watermark sample")?;
        }
        writeln!(
            log,
            "case={} mode={} gc={} expired_pct={} requests={} credits={} debits={} fresh={} hits={} misses={} client_wall_s={:.6} settled_s={:.6} rps={:.3} cpu_s={:.6} cpu_cores={:.6} cpu_sample_offset_us={} progress_sample_offset_us={} projected_near_end={} destination_near_end={} projection_backlog_near_end={} gc_prefix_near_end={} gc_backlog_near_end={} gc_prefix_after_settle={} checkpoint_lag_near_end={} recovery_s={:.6} integrity_s={:.6} io_sample_offset_us={} storage_sample_offset_us={} db_bytes={} peak_rss_bytes={} setup_preflight_cpu_pct={:.3} setup_preflight_disk_pct={:.3} measure_preflight_cpu_pct={:.3} measure_preflight_disk_pct={:.3}",
            result.name,
            mode_name(result.mode),
            result.gc_enabled,
            result.expired_pct,
            result.requests,
            result.credits,
            result.debits,
            result.fresh,
            result.historical_hits,
            result.historical_misses,
            result.client_wall.as_secs_f64(),
            result.settled_wall.as_secs_f64(),
            rps,
            result.cpu_seconds,
            result.cpu_cores,
            result.cpu_sample_offset_us,
            result.progress_sample_offset_us,
            result.projected_seq_near_client_end,
            result.destination_seq_near_client_end,
            result.projection_backlog_records_near_client_end,
            result.gc_prefix_near_client_end,
            result.gc_backlog_records_near_client_end,
            result.gc_prefix_seq,
            result.checkpoint_lag_records_near_client_end,
            result.recovery_seconds,
            result.integrity_seconds,
            result.io_sample_offset_us,
            result.storage_sample_offset_us,
            result.db_bytes,
            result.peak_rss_bytes,
            result.setup_preflight.cpu_busy_pct,
            result.setup_preflight.disk_busy_pct,
            result.measure_preflight.cpu_busy_pct,
            result.measure_preflight.disk_busy_pct
        )
        .map_err(|error| format!("cannot write run log row: {error}"))?;
    }

    for writer in [
        &mut summary,
        &mut latency,
        &mut background,
        &mut log,
        &mut index_lookup,
    ] {
        writer
            .flush()
            .map_err(|error| format!("cannot flush completed run archive: {error}"))?;
    }
    Ok(())
}

fn csv(value: &str) -> String {
    if value
        .chars()
        .any(|character| matches!(character, ',' | '"' | '\n' | '\r'))
    {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_owned()
    }
}

fn write_csv_row<W: Write>(writer: &mut W, row: &[String], context: &str) -> Result<(), String> {
    let encoded = row
        .iter()
        .map(|field| csv(field))
        .collect::<Vec<_>>()
        .join(",");
    writeln!(writer, "{encoded}").map_err(|error| format!("cannot write {context}: {error}"))
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
