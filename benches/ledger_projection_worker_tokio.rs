#[allow(dead_code)]
#[path = "support/ledger_account_store.rs"]
mod ledger_account_store;
#[allow(dead_code)]
#[path = "support/ledger_account_store_workload.rs"]
mod ledger_account_store_workload;
#[allow(dead_code)]
#[path = "support/ledger_preflight.rs"]
mod ledger_preflight;
#[allow(dead_code)]
#[path = "support/ledger_projection_worker.rs"]
mod ledger_projection_worker;

use cpu_time::ProcessTime;
use ledger_account_store::{
    AccountStore, BalanceMode, Operation, Reply, RocksDbBudget, Transaction, TransactionKey,
    TransactionStatus,
};
use ledger_account_store_workload::{PatternKind, Workload, WorkloadKind, build_workload};
use ledger_preflight::{IoSample, PreflightConfig, PreflightReport};
use ledger_projection_worker::MockProjectionStore;
use rocksdb::statistics::Ticker;
use rocksdb::{DB, Options};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::runtime::Builder;
use tokio::sync::{oneshot, watch};

const TOTAL_USERS: usize = 50_000;
const REQUESTS_PER_USER: usize = 200;
const FOREGROUND_BATCH_SIZE: usize = 4_096;
const DEFAULT_PROJECTION_BATCH_SIZE: usize = 256;
const CHECKPOINT_QUANTITY: u64 = 100_000;
const WORKERS: usize = 4;
const DEFAULT_OUTPUT_ROOT: &str = "target/ledger-projection-worker-tokio-trials";
const TOTAL_WRITE_BUFFER_BYTES: usize = 128 * 1024 * 1024;
const TOTAL_BLOCK_CACHE_BYTES: usize = 128 * 1024 * 1024;
const MAX_BACKGROUND_JOBS: i32 = 4;
const MAX_WRITE_BUFFERS: i32 = 2;
const DEFAULT_MEMORY_RESERVE_BYTES: u64 = 768 * 1024 * 1024;
const ESTIMATED_MOCK_BYTES_PER_RECORD: u64 = 256;
const ESTIMATED_DISK_BYTES_PER_RECORD: u64 = 768;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CaseKind {
    LiveWritesControl,
    CatchUpAfterPrefill,
    ConcurrentLiveProjection,
}

impl CaseKind {
    fn name(self) -> &'static str {
        match self {
            Self::LiveWritesControl => "live-writes-control",
            Self::CatchUpAfterPrefill => "catch-up-after-prefill",
            Self::ConcurrentLiveProjection => "concurrent-live-projection",
        }
    }

    fn all() -> [Self; 3] {
        [
            Self::LiveWritesControl,
            Self::CatchUpAfterPrefill,
            Self::ConcurrentLiveProjection,
        ]
    }
}

#[derive(Clone, Debug)]
struct Config {
    users: usize,
    waves: usize,
    projection_batch_size: usize,
    projection_delay: Duration,
    output_root: PathBuf,
    preflight: PreflightConfig,
    free_space_reserve_bytes: u64,
    smoke: bool,
}

struct DbDirectory(PathBuf);

impl DbDirectory {
    fn create(path: PathBuf) -> Result<Self, String> {
        fs::create_dir(&path).map_err(|error| {
            format!(
                "cannot create unique case directory {}: {error}",
                path.display()
            )
        })?;
        Ok(Self(path))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for DbDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct Database {
    db: Arc<DB>,
    options: Arc<Options>,
}

#[derive(Clone, Debug, Default)]
struct RocksSnapshot {
    bytes_read: u64,
    bytes_written: u64,
    wal_sync_count: u64,
    wal_bytes: u64,
    writes_with_wal: u64,
    flush_write_bytes: u64,
    compaction_read_bytes: u64,
    compaction_write_bytes: u64,
    stall_micros: u64,
}

#[derive(Clone, Debug, Default)]
struct RocksDelta {
    bytes_read: u64,
    bytes_written: u64,
    wal_sync_count: u64,
    wal_bytes: u64,
    writes_with_wal: u64,
    flush_write_bytes: u64,
    compaction_read_bytes: u64,
    compaction_write_bytes: u64,
    stall_micros: u64,
}

impl RocksSnapshot {
    fn delta(self, before: Self) -> RocksDelta {
        RocksDelta {
            bytes_read: self.bytes_read.saturating_sub(before.bytes_read),
            bytes_written: self.bytes_written.saturating_sub(before.bytes_written),
            wal_sync_count: self.wal_sync_count.saturating_sub(before.wal_sync_count),
            wal_bytes: self.wal_bytes.saturating_sub(before.wal_bytes),
            writes_with_wal: self.writes_with_wal.saturating_sub(before.writes_with_wal),
            flush_write_bytes: self
                .flush_write_bytes
                .saturating_sub(before.flush_write_bytes),
            compaction_read_bytes: self
                .compaction_read_bytes
                .saturating_sub(before.compaction_read_bytes),
            compaction_write_bytes: self
                .compaction_write_bytes
                .saturating_sub(before.compaction_write_bytes),
            stall_micros: self.stall_micros.saturating_sub(before.stall_micros),
        }
    }
}

#[derive(Clone, Debug, Default)]
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

#[derive(Clone, Debug, Default)]
struct Percentiles {
    p50_ns: u64,
    p95_ns: u64,
    p99_ns: u64,
    samples: usize,
}

#[derive(Clone, Debug, Default)]
struct ProjectionLatency {
    ledger_read: Percentiles,
    mock_apply: Percentiles,
    synthetic_delay: Percentiles,
    dispatch_wait: Percentiles,
    total: Percentiles,
}

#[derive(Default)]
struct ProjectionSamples {
    ledger_read_ns: Vec<u64>,
    mock_apply_ns: Vec<u64>,
    synthetic_delay_ns: Vec<u64>,
    dispatch_wait_ns: Vec<u64>,
    total_ns: Vec<u64>,
}

impl ProjectionSamples {
    fn summarize(&self) -> ProjectionLatency {
        ProjectionLatency {
            ledger_read: percentiles(&self.ledger_read_ns),
            mock_apply: percentiles(&self.mock_apply_ns),
            synthetic_delay: percentiles(&self.synthetic_delay_ns),
            dispatch_wait: percentiles(&self.dispatch_wait_ns),
            total: percentiles(&self.total_ns),
        }
    }
}

struct CaseResult {
    case: CaseKind,
    users: usize,
    waves: usize,
    durable_records: u64,
    projection_batch_size: usize,
    projection_delay_ms: u64,
    foreground_transactions: u64,
    foreground_wall_s: f64,
    foreground_transaction_latency: Option<Percentiles>,
    measured_wall_s: f64,
    foreground_cpu_s: f64,
    measured_cpu_s: f64,
    projected_records: u64,
    projected_sequence: u64,
    foreground_end_lag: u64,
    peak_lag: u64,
    peak_rss_bytes: u64,
    io: IoDelta,
    rocks: RocksDelta,
    latency: ProjectionLatency,
    preflight: PreflightReport,
    db_bytes: u64,
}

struct RssSampler {
    peak: Arc<AtomicU64>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl RssSampler {
    fn start() -> Result<Self, String> {
        let peak = Arc::new(AtomicU64::new(current_rss_bytes()?));
        let worker_peak = Arc::clone(&peak);
        let (stop, mut stop_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(10));
            loop {
                tokio::select! {
                    _ = &mut stop_rx => break,
                    _ = interval.tick() => {
                        if let Ok(rss) = current_rss_bytes() {
                            worker_peak.fetch_max(rss, Ordering::Relaxed);
                        }
                    }
                }
            }
        });
        Ok(Self {
            peak,
            stop: Some(stop),
            task: Some(task),
        })
    }

    async fn stop(mut self) -> Result<u64, String> {
        if let Some(sender) = self.stop.take() {
            let _ = sender.send(());
        }
        if let Some(task) = self.task.take() {
            task.await
                .map_err(|error| format!("RSS sampler join failed: {error}"))?;
        }
        Ok(self.peak.load(Ordering::Relaxed))
    }
}

struct LagSampler {
    peak: Arc<AtomicU64>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl LagSampler {
    fn start(store: AccountStore, projected: Arc<MockProjectionStore>) -> Self {
        let initial = store.latest_seq().saturating_sub(projected.progress());
        let peak = Arc::new(AtomicU64::new(initial));
        let sampler_peak = Arc::clone(&peak);
        let (stop, mut stop_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(5));
            loop {
                tokio::select! {
                    _ = &mut stop_rx => break,
                    _ = interval.tick() => {
                        let lag = store.latest_seq().saturating_sub(projected.progress());
                        sampler_peak.fetch_max(lag, Ordering::Relaxed);
                    }
                }
            }
        });
        Self {
            peak,
            stop: Some(stop),
            task: Some(task),
        }
    }

    async fn stop(
        mut self,
        store: &AccountStore,
        projected: &MockProjectionStore,
    ) -> Result<u64, String> {
        let final_lag = store.latest_seq().saturating_sub(projected.progress());
        self.peak.fetch_max(final_lag, Ordering::Relaxed);
        if let Some(sender) = self.stop.take() {
            let _ = sender.send(());
        }
        if let Some(task) = self.task.take() {
            task.await
                .map_err(|error| format!("lag sampler join failed: {error}"))?;
        }
        Ok(self.peak.load(Ordering::Relaxed))
    }
}

fn main() -> Result<(), String> {
    let config = parse_args(std::env::args().skip(1))?;
    fs::create_dir_all(&config.output_root).map_err(|error| {
        format!(
            "cannot create output directory {}: {error}",
            config.output_root.display()
        )
    })?;
    let workload = Arc::new(build_workload(
        WorkloadKind::CreditDebit50_50,
        config.users,
        config.waves,
    )?);
    let runtime = Builder::new_multi_thread()
        .worker_threads(WORKERS)
        .thread_name("ledger-projection-worker")
        .enable_all()
        .build()
        .map_err(|error| format!("cannot build Tokio runtime: {error}"))?;
    runtime.block_on(run(&config, workload))
}

async fn run(config: &Config, workload: Arc<Workload>) -> Result<(), String> {
    let csv_path = config.output_root.join("results.csv");
    let log_path = config.output_root.join("run.log");
    let mut csv = BufWriter::new(
        File::create(&csv_path).map_err(|error| format!("cannot create CSV: {error}"))?,
    );
    let mut log = BufWriter::new(
        File::create(&log_path).map_err(|error| format!("cannot create run log: {error}"))?,
    );
    write_csv_header(&mut csv).map_err(io_error("write CSV header"))?;
    writeln!(
        log,
        "mode={} users={} waves={} durable_records={} foreground_batch_size={} projection_batch_size={} projection_delay_ms={} runtime_workers={WORKERS}",
        if config.smoke { "smoke" } else { "full-or-custom" },
        config.users,
        config.waves,
        workload.transactions,
        FOREGROUND_BATCH_SIZE,
        config.projection_batch_size,
        config.projection_delay.as_millis()
    )
    .map_err(io_error("write run header"))?;
    log.flush().map_err(io_error("flush run header"))?;
    println!(
        "ledger projection benchmark: users={} waves={} durable_records={} mode={}",
        config.users,
        config.waves,
        workload.transactions,
        if config.smoke {
            "smoke"
        } else {
            "full-or-custom"
        }
    );

    for case in CaseKind::all() {
        writeln!(log, "case_start={}", case.name()).map_err(io_error("write case start"))?;
        log.flush().map_err(io_error("flush case start"))?;
        println!("starting {}", case.name());
        let case_started = Instant::now();
        let result = run_case(config, Arc::clone(&workload), case).await;
        match result {
            Ok(result) => {
                write_result(&mut csv, &result).map_err(io_error("write result row"))?;
                csv.flush().map_err(io_error("flush result row"))?;
                writeln!(
                    log,
                    "case_result={} status=passed {}",
                    case.name(),
                    result_log(&result)
                )
                .map_err(io_error("write case result"))?;
                log.flush().map_err(io_error("flush case result"))?;
                println!(
                    "{}: tx_rps={:.2} projection_rps={:.2} projected_seq={} lag_end={} lag_peak={} wall_s={:.3}",
                    case.name(),
                    rate(result.foreground_transactions, result.foreground_wall_s),
                    rate(result.projected_records, result.measured_wall_s),
                    result.projected_sequence,
                    result.foreground_end_lag,
                    result.peak_lag,
                    result.measured_wall_s
                );
            }
            Err(error) => {
                writeln!(
                    log,
                    "case_result={} status=failed case_wall_s={:.6} error={error}",
                    case.name(),
                    case_started.elapsed().as_secs_f64()
                )
                .map_err(io_error("write failure"))?;
                log.flush().map_err(io_error("flush failure"))?;
                return Err(format!("{} failed: {error}", case.name()));
            }
        }
    }
    writeln!(log, "run_complete=true results_csv={}", csv_path.display())
        .map_err(io_error("write completion"))?;
    log.flush().map_err(io_error("flush completion"))?;
    println!("results: {}", config.output_root.display());
    Ok(())
}

async fn run_case(
    config: &Config,
    workload: Arc<Workload>,
    case: CaseKind,
) -> Result<CaseResult, String> {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("system clock error: {error}"))?
        .as_nanos();
    let case_dir = DbDirectory::create(config.output_root.join(format!(
        "{}-{}-{unique}",
        case.name(),
        std::process::id()
    )))?;
    let estimate = workload
        .transactions
        .checked_mul(ESTIMATED_DISK_BYTES_PER_RECORD)
        .and_then(|bytes| bytes.checked_add(config.free_space_reserve_bytes))
        .ok_or_else(|| "preflight free-space estimate overflow".to_owned())?;
    let preflight_config = PreflightConfig {
        min_free_bytes: config.preflight.min_free_bytes.max(estimate),
        ..config.preflight.clone()
    };
    let mut preflight = ledger_preflight::ensure_idle(case_dir.path(), &preflight_config)
        .map_err(|error| format!("strict idle preflight failed: {error}"))?;

    let write_buffer_size = TOTAL_WRITE_BUFFER_BYTES
        / usize::try_from(MAX_WRITE_BUFFERS).expect("positive buffer count");
    let budget = RocksDbBudget {
        write_buffer_size,
        max_write_buffer_number: MAX_WRITE_BUFFERS,
        block_cache_bytes: TOTAL_BLOCK_CACHE_BYTES,
        max_background_jobs: MAX_BACKGROUND_JOBS,
    };
    let db_path = case_dir.path().join("db");
    let (db, options) = AccountStore::open_database_with_budget(&db_path, budget).await?;
    let database = Database { db, options };
    let accounts = (0..workload.users as u64).collect();
    let store = AccountStore::open_on_database(
        Arc::clone(&database.db),
        Arc::clone(&database.options),
        accounts,
        0,
        BalanceMode::Checkpoint,
        CHECKPOINT_QUANTITY,
    )
    .await?;
    if store.namespace_id() != Some(0) || store.account_ids().len() != workload.users {
        return shutdown_after_error(
            store,
            "single-shard store opened with unexpected namespace or account set".to_owned(),
        )
        .await;
    }

    if case == CaseKind::CatchUpAfterPrefill {
        let prefill = run_foreground(store.clone(), Arc::clone(&workload), None, None).await;
        if let Err(error) = prefill {
            return shutdown_after_error(store, error).await;
        }
        if let Err(error) = store.drain_checkpoints().await {
            return shutdown_after_error(store, error).await;
        }
        if store.latest_seq() != workload.transactions {
            let committed = store.latest_seq();
            return shutdown_after_error(
                store,
                format!(
                    "prefill committed {} records, expected {}",
                    committed, workload.transactions
                ),
            )
            .await;
        }
    }

    let projected = if case == CaseKind::LiveWritesControl {
        None
    } else {
        let capacity = match usize::try_from(workload.transactions) {
            Ok(capacity) => capacity,
            Err(_) => {
                return shutdown_after_error(
                    store,
                    "record count does not fit memory address space".to_owned(),
                )
                .await;
            }
        };
        let estimated_memory = workload
            .transactions
            .checked_mul(ESTIMATED_MOCK_BYTES_PER_RECORD)
            .and_then(|bytes| bytes.checked_add(DEFAULT_MEMORY_RESERVE_BYTES))
            .unwrap_or(u64::MAX);
        let available_memory = match mem_available_bytes() {
            Ok(available) => available,
            Err(error) => return shutdown_after_error(store, error).await,
        };
        if available_memory < estimated_memory {
            return shutdown_after_error(
                store,
                format!(
                    "available memory {available_memory} is below mock destination estimate {estimated_memory}"
                ),
            )
            .await;
        }
        let mock = match MockProjectionStore::with_capacity(capacity) {
            Ok(mock) => mock,
            Err(error) => return shutdown_after_error(store, error).await,
        };
        Some(Arc::new(mock))
    };

    if case == CaseKind::CatchUpAfterPrefill {
        preflight = match ledger_preflight::ensure_idle(case_dir.path(), &preflight_config) {
            Ok(report) => report,
            Err(error) => {
                return shutdown_after_error(
                    store,
                    format!("strict measured-window preflight failed after prefill: {error}"),
                )
                .await;
            }
        };
    }

    let io_before = match ledger_preflight::sample_io(case_dir.path()) {
        Ok(sample) => sample,
        Err(error) => return shutdown_after_error(store, error).await,
    };
    let rocks_before = rocks_snapshot(&database.options);
    let rss_sampler = match RssSampler::start() {
        Ok(sampler) => sampler,
        Err(error) => return shutdown_after_error(store, error).await,
    };
    let measured_cpu_start = ProcessTime::now();
    let measured_started = Instant::now();

    let (
        foreground,
        foreground_cpu_s,
        foreground_wall_s,
        foreground_end_lag,
        sampled_peak_lag,
        samples,
    ) = match case {
        CaseKind::LiveWritesControl => {
            let cpu_start = ProcessTime::now();
            let timing = run_foreground(store.clone(), Arc::clone(&workload), None, None).await;
            let foreground_cpu_s = cpu_start.elapsed().as_secs_f64();
            let timing = match timing {
                Ok(timing) => timing,
                Err(error) => return shutdown_after_error(store, error).await,
            };
            if let Err(error) = store.drain_checkpoints().await {
                return shutdown_after_error(store, error).await;
            }
            (
                timing.transactions,
                foreground_cpu_s,
                timing.wall_s,
                workload.transactions,
                workload.transactions,
                ProjectionSamples::default(),
            )
        }
        CaseKind::CatchUpAfterPrefill => {
            let projected = projected
                .as_ref()
                .expect("catch-up requires mock destination");
            let lag_sampler = LagSampler::start(store.clone(), Arc::clone(projected));
            let initial_lag = store.latest_seq().saturating_sub(projected.progress());
            let projection = run_projector(
                store.clone(),
                Arc::clone(projected),
                config.projection_batch_size,
                config.projection_delay,
                Some(workload.transactions),
                None,
                None,
            )
            .await;
            let projection = match projection {
                Ok(projection) => projection,
                Err(error) => return shutdown_after_error(store, error).await,
            };
            let peak_lag = match lag_sampler.stop(&store, projected).await {
                Ok(peak) => peak,
                Err(error) => return shutdown_after_error(store, error).await,
            };
            (0, 0.0, 0.0, initial_lag, peak_lag, projection.samples)
        }
        CaseKind::ConcurrentLiveProjection => {
            let projected = projected
                .as_ref()
                .expect("concurrent case requires mock destination");
            let (head_tx, head_rx) = watch::channel(store.latest_seq());
            let (cancel_tx, cancel_rx) = watch::channel(false);
            let lag_sampler = LagSampler::start(store.clone(), Arc::clone(projected));
            let mut projector = tokio::spawn(run_projector(
                store.clone(),
                Arc::clone(projected),
                config.projection_batch_size,
                config.projection_delay,
                None,
                Some(head_rx),
                Some(cancel_rx.clone()),
            ));
            let cpu_start = ProcessTime::now();
            let mut writer = tokio::spawn(run_foreground(
                store.clone(),
                Arc::clone(&workload),
                Some(head_tx.clone()),
                Some(cancel_rx),
            ));
            let writer_result = tokio::select! {
                result = &mut writer => result,
                result = &mut projector => {
                    cancel_tx.send_replace(true);
                    drop(head_tx);
                    let writer_result = writer.await;
                    return match result {
                        Ok(Ok(_)) => shutdown_after_error(store, format!("projector stopped before foreground completed; writer_result={writer_result:?}")).await,
                        Ok(Err(error)) => shutdown_after_error(store, format!("projector failed before foreground completed: {error}")).await,
                        Err(error) => shutdown_after_error(store, format!("projector task failed before foreground completed: {error}")).await,
                    };
                }
            };
            let writer_result = match writer_result {
                Ok(result) => result,
                Err(error) => {
                    cancel_tx.send_replace(true);
                    drop(head_tx);
                    let _ = projector.await;
                    return shutdown_after_error(
                        store,
                        format!("foreground task join failed: {error}"),
                    )
                    .await;
                }
            };
            let foreground_cpu_s = cpu_start.elapsed().as_secs_f64();
            let foreground = match writer_result {
                Ok(timing) => timing,
                Err(error) => {
                    cancel_tx.send_replace(true);
                    drop(head_tx);
                    let _ = projector.await;
                    return shutdown_after_error(store, error).await;
                }
            };
            let foreground_end_lag = store.latest_seq().saturating_sub(projected.progress());
            drop(head_tx);
            if let Err(error) = store.drain_checkpoints().await {
                cancel_tx.send_replace(true);
                let _ = projector.await;
                return shutdown_after_error(store, error).await;
            }
            let projection = match projector.await {
                Ok(Ok(projection)) => projection,
                Ok(Err(error)) => return shutdown_after_error(store, error).await,
                Err(error) => {
                    return shutdown_after_error(
                        store,
                        format!("projector task join failed: {error}"),
                    )
                    .await;
                }
            };
            let peak_lag = match lag_sampler.stop(&store, projected).await {
                Ok(peak) => peak,
                Err(error) => return shutdown_after_error(store, error).await,
            };
            if projected.progress() != workload.transactions {
                return shutdown_after_error(
                    store,
                    format!(
                        "projector stopped at sequence {}, expected {}",
                        projected.progress(),
                        workload.transactions
                    ),
                )
                .await;
            }
            (
                foreground.transactions,
                foreground_cpu_s,
                foreground.wall_s,
                foreground_end_lag,
                peak_lag,
                projection.samples,
            )
        }
    };
    let peak_lag = sampled_peak_lag.max(foreground_end_lag);

    let measured_wall_s = measured_started.elapsed().as_secs_f64();
    let measured_cpu_s = measured_cpu_start.elapsed().as_secs_f64();
    let io_after = match ledger_preflight::sample_io(case_dir.path()) {
        Ok(sample) => sample,
        Err(error) => return shutdown_after_error(store, error).await,
    };
    let io = match io_delta(&io_before, &io_after) {
        Ok(delta) => delta,
        Err(error) => return shutdown_after_error(store, error).await,
    };
    let rocks = rocks_snapshot(&database.options).delta(rocks_before);
    let peak_rss_bytes = match rss_sampler.stop().await {
        Ok(peak) => peak,
        Err(error) => return shutdown_after_error(store, error).await,
    };
    let projected_records = projected.as_ref().map_or(0, |store| store.progress());
    let projected_sequence = projected_records;
    if case != CaseKind::LiveWritesControl && projected_sequence != workload.transactions {
        return shutdown_after_error(
            store,
            format!(
                "final projection sequence {projected_sequence} differs from {}",
                workload.transactions
            ),
        )
        .await;
    }
    if let Err(error) = store.validate_integrity().await {
        return shutdown_after_error(
            store,
            format!("post-run ledger integrity check failed: {error}"),
        )
        .await;
    }
    let foreground_transaction_latency = match case {
        CaseKind::LiveWritesControl | CaseKind::ConcurrentLiveProjection => {
            Some(percentiles(&store.metrics().transaction_samples_ns))
        }
        CaseKind::CatchUpAfterPrefill => None,
    };
    store.shutdown().await?;
    drop(database);
    let db_bytes = directory_bytes(case_dir.path())?;
    let latency = samples.summarize();
    Ok(CaseResult {
        case,
        users: workload.users,
        waves: workload.waves.len(),
        durable_records: workload.transactions,
        projection_batch_size: config.projection_batch_size,
        projection_delay_ms: config.projection_delay.as_millis() as u64,
        foreground_transactions: foreground,
        foreground_wall_s,
        foreground_transaction_latency,
        measured_wall_s,
        foreground_cpu_s,
        measured_cpu_s,
        projected_records,
        projected_sequence,
        foreground_end_lag,
        peak_lag,
        peak_rss_bytes,
        io,
        rocks,
        latency,
        preflight,
        db_bytes,
    })
}

#[derive(Clone, Copy, Debug)]
struct ForegroundTiming {
    transactions: u64,
    wall_s: f64,
}

async fn run_foreground(
    store: AccountStore,
    workload: Arc<Workload>,
    head: Option<watch::Sender<u64>>,
    cancel: Option<watch::Receiver<bool>>,
) -> Result<ForegroundTiming, String> {
    let started = Instant::now();
    let mut committed_transactions = 0_u64;
    let transactions_per_account = workload.waves.len() as u64;
    for (wave_index, wave) in workload.waves.iter().enumerate() {
        let cycle = wave_index / 10;
        let mut transactions = Vec::with_capacity(wave.transaction_accounts.len());
        for account_index in &wave.transaction_accounts {
            let account = u64::from(*account_index);
            let entry = workload.patterns[*account_index as usize][wave_index % 10];
            let operation = match entry.kind {
                PatternKind::Credit => Operation::Credit,
                PatternKind::Debit => Operation::Debit,
                other => return Err(format!("50/50 workload produced {other:?}")),
            };
            let tx_id = account
                .checked_mul(transactions_per_account)
                .and_then(|base| base.checked_add(cycle as u64 * workload.transactions_per_cycle))
                .and_then(|base| base.checked_add(u64::from(entry.transaction_offset) + 1))
                .ok_or_else(|| "transaction ID overflow".to_owned())?;
            let transaction_at = account
                .checked_mul(workload.waves.len() as u64)
                .and_then(|offset| 1_700_000_000_000_000_u64.checked_add(offset))
                .and_then(|base| base.checked_add(wave_index as u64))
                .ok_or_else(|| "transaction timestamp overflow".to_owned())?;
            transactions.push(Transaction {
                key: TransactionKey {
                    account_id: account,
                    tx_id,
                    transaction_at,
                },
                operation,
                amount: 1,
                refund_of: None,
            });
        }

        for batch in transactions.chunks(FOREGROUND_BATCH_SIZE) {
            if cancel.as_ref().is_some_and(|receiver| *receiver.borrow()) {
                return Err("foreground cancelled after projector failure".to_owned());
            }
            let replies = store.handle_batch(batch.to_vec()).await?;
            if replies.len() != batch.len() {
                return Err("account store returned the wrong reply count".to_owned());
            }
            for reply in replies {
                match reply {
                    Reply::Transaction {
                        status: TransactionStatus::Applied,
                        seq,
                        replayed: false,
                        ..
                    } => {
                        let expected = committed_transactions + 1;
                        if seq != expected {
                            return Err(format!(
                                "foreground received sequence {seq}, expected {expected}"
                            ));
                        }
                        committed_transactions += 1;
                    }
                    other => {
                        return Err(format!("foreground returned an invalid result: {other:?}"));
                    }
                }
            }
            if let Some(sender) = &head {
                sender.send_replace(store.latest_seq());
            }
        }
    }
    if committed_transactions != workload.transactions {
        return Err(format!(
            "foreground committed {committed_transactions} transactions, expected {}",
            workload.transactions
        ));
    }
    Ok(ForegroundTiming {
        transactions: committed_transactions,
        wall_s: started.elapsed().as_secs_f64(),
    })
}

#[derive(Default)]
struct ProjectionTiming {
    samples: ProjectionSamples,
}

async fn run_projector(
    store: AccountStore,
    projected: Arc<MockProjectionStore>,
    batch_size: usize,
    synthetic_delay: Duration,
    target_sequence: Option<u64>,
    mut source_updates: Option<watch::Receiver<u64>>,
    mut cancel: Option<watch::Receiver<bool>>,
) -> Result<ProjectionTiming, String> {
    let mut timing = ProjectionTiming::default();
    let mut source_closed = false;
    loop {
        if cancel.as_ref().is_some_and(|receiver| *receiver.borrow()) {
            return Err("projection worker was cancelled".to_owned());
        }
        let progress = projected.progress();
        if target_sequence.is_some_and(|target| progress >= target) {
            break;
        }
        let source_head = store.latest_seq();
        if progress >= source_head {
            if let Some(target) = target_sequence {
                return Err(format!(
                    "projection source ended at {source_head} before target {target}"
                ));
            }
            if source_closed {
                break;
            }
            let Some(source_updates) = source_updates.as_mut() else {
                return Err("projection worker has no source update channel".to_owned());
            };
            if let Some(cancel) = cancel.as_mut() {
                tokio::select! {
                    changed = source_updates.changed() => {
                        if changed.is_err() {
                            source_closed = true;
                        }
                    }
                    changed = cancel.changed() => {
                        if changed.is_err() || *cancel.borrow() {
                            return Err("projection worker was cancelled while waiting for source".to_owned());
                        }
                    }
                }
            } else if source_updates.changed().await.is_err() {
                source_closed = true;
            }
            continue;
        }

        let mut requested = u64::try_from(batch_size)
            .map_err(|_| "projection batch size does not fit sequence arithmetic".to_owned())?;
        if let Some(target) = target_sequence {
            requested = requested.min(target.saturating_sub(progress));
        }
        let first_seq = progress
            .checked_add(1)
            .ok_or_else(|| "projection sequence overflow".to_owned())?;
        let batch_started = Instant::now();
        let read = store
            .read_ledger_range(
                first_seq,
                usize::try_from(requested)
                    .map_err(|_| "projection read size does not fit memory".to_owned())?,
            )
            .await?;
        if read.records.is_empty() {
            return Err(format!(
                "projection source reported sequence {source_head} but returned no records at {first_seq}"
            ));
        }
        let expected_after = read
            .records
            .last()
            .expect("nonempty read checked above")
            .result
            .seq;

        let actual_delay_ns = if !synthetic_delay.is_zero() {
            let delay_started = Instant::now();
            tokio::time::sleep(synthetic_delay).await;
            nanos(delay_started.elapsed())
        } else {
            0
        };
        let apply_started = Instant::now();
        let applied_sequence = projected.apply_batch(&read.records)?;
        let mock_apply_duration = apply_started.elapsed();
        let total_ns = nanos(batch_started.elapsed());
        let mock_apply_ns = nanos(mock_apply_duration);
        if applied_sequence != expected_after {
            return Err(format!(
                "mock destination advanced to {applied_sequence}, expected {expected_after}"
            ));
        }

        let dispatch_wait_ns = total_ns
            .saturating_sub(read.db_read_ns)
            .saturating_sub(actual_delay_ns)
            .saturating_sub(mock_apply_ns);
        timing.samples.ledger_read_ns.push(read.db_read_ns);
        timing.samples.mock_apply_ns.push(mock_apply_ns);
        timing.samples.synthetic_delay_ns.push(actual_delay_ns);
        timing.samples.dispatch_wait_ns.push(dispatch_wait_ns);
        timing.samples.total_ns.push(total_ns);
    }
    if let Some(target) = target_sequence
        && projected.progress() != target
    {
        return Err(format!(
            "projection ended at {}, expected target {target}",
            projected.progress()
        ));
    }
    let sample_count = timing.samples.total_ns.len();
    if timing.samples.ledger_read_ns.len() != sample_count
        || timing.samples.mock_apply_ns.len() != sample_count
        || timing.samples.synthetic_delay_ns.len() != sample_count
        || timing.samples.dispatch_wait_ns.len() != sample_count
    {
        return Err("projection stage latency sample counts diverged".to_owned());
    }
    Ok(timing)
}

async fn shutdown_after_error<T>(store: AccountStore, error: String) -> Result<T, String> {
    match store.shutdown().await {
        Ok(()) => Err(error),
        Err(cleanup) => Err(format!("{error}; account-store cleanup failed: {cleanup}")),
    }
}

fn rocks_snapshot(options: &Options) -> RocksSnapshot {
    RocksSnapshot {
        bytes_read: options.get_ticker_count(Ticker::BytesRead),
        bytes_written: options.get_ticker_count(Ticker::BytesWritten),
        wal_sync_count: options.get_ticker_count(Ticker::WalFileSynced),
        wal_bytes: options.get_ticker_count(Ticker::WalFileBytes),
        writes_with_wal: options.get_ticker_count(Ticker::WriteWithWal),
        flush_write_bytes: options.get_ticker_count(Ticker::FlushWriteBytes),
        compaction_read_bytes: options.get_ticker_count(Ticker::CompactReadBytes),
        compaction_write_bytes: options.get_ticker_count(Ticker::CompactWriteBytes),
        stall_micros: options.get_ticker_count(Ticker::StallMicros),
    }
}

fn io_delta(before: &IoSample, after: &IoSample) -> Result<IoDelta, String> {
    if before.target_major_minor != after.target_major_minor {
        return Err("target block device changed during benchmark window".to_owned());
    }
    Ok(IoDelta {
        process_rchar_bytes: after
            .process_rchar_bytes
            .saturating_sub(before.process_rchar_bytes),
        process_wchar_bytes: after
            .process_wchar_bytes
            .saturating_sub(before.process_wchar_bytes),
        process_read_bytes: after
            .process_read_bytes
            .saturating_sub(before.process_read_bytes),
        process_write_bytes: after
            .process_write_bytes
            .saturating_sub(before.process_write_bytes),
        target_device: after.target_device.clone(),
        target_major_minor: after.target_major_minor.clone(),
        target_read_bytes: after
            .target_read_bytes
            .saturating_sub(before.target_read_bytes),
        target_write_bytes: after
            .target_write_bytes
            .saturating_sub(before.target_write_bytes),
        target_busy_ms: after.target_busy_ms.saturating_sub(before.target_busy_ms),
    })
}

fn percentiles(samples: &[u64]) -> Percentiles {
    let mut ordered = samples.to_vec();
    ordered.sort_unstable();
    let quantile = |percent: usize| {
        if ordered.is_empty() {
            0
        } else {
            let rank = ordered.len().saturating_mul(percent).div_ceil(100).max(1);
            ordered[(rank - 1).min(ordered.len() - 1)]
        }
    };
    Percentiles {
        p50_ns: quantile(50),
        p95_ns: quantile(95),
        p99_ns: quantile(99),
        samples: ordered.len(),
    }
}

fn rate(count: u64, wall_s: f64) -> f64 {
    if wall_s <= 0.0 {
        0.0
    } else {
        count as f64 / wall_s
    }
}

fn cores(cpu_s: f64, wall_s: f64) -> f64 {
    if wall_s <= 0.0 { 0.0 } else { cpu_s / wall_s }
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn current_rss_bytes() -> Result<u64, String> {
    let status = fs::read_to_string("/proc/self/status")
        .map_err(|error| format!("cannot read /proc/self/status: {error}"))?;
    let kib = status
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmRSS:")
                .and_then(|value| value.split_whitespace().next())
        })
        .ok_or_else(|| "VmRSS entry is missing from /proc/self/status".to_owned())?
        .parse::<u64>()
        .map_err(|error| format!("invalid VmRSS entry: {error}"))?;
    kib.checked_mul(1024)
        .ok_or_else(|| "RSS byte count overflow".to_owned())
}

fn mem_available_bytes() -> Result<u64, String> {
    let memory = fs::read_to_string("/proc/meminfo")
        .map_err(|error| format!("cannot read /proc/meminfo: {error}"))?;
    let kib = memory
        .lines()
        .find_map(|line| {
            line.strip_prefix("MemAvailable:")
                .and_then(|value| value.split_whitespace().next())
        })
        .ok_or_else(|| "MemAvailable entry is missing from /proc/meminfo".to_owned())?
        .parse::<u64>()
        .map_err(|error| format!("invalid MemAvailable entry: {error}"))?;
    kib.checked_mul(1024)
        .ok_or_else(|| "available memory byte count overflow".to_owned())
}

fn directory_bytes(path: &Path) -> Result<u64, String> {
    let mut total = 0_u64;
    for entry in
        fs::read_dir(path).map_err(|error| format!("cannot list {}: {error}", path.display()))?
    {
        let entry = entry.map_err(|error| format!("cannot read directory entry: {error}"))?;
        let metadata = entry
            .metadata()
            .map_err(|error| format!("cannot inspect {}: {error}", entry.path().display()))?;
        let size = if metadata.is_dir() {
            directory_bytes(&entry.path())?
        } else {
            metadata.len()
        };
        total = total
            .checked_add(size)
            .ok_or_else(|| "database byte count overflow".to_owned())?;
    }
    Ok(total)
}

fn parse_args<I>(args: I) -> Result<Config, String>
where
    I: IntoIterator<Item = String>,
{
    let mut config = Config {
        users: TOTAL_USERS,
        waves: REQUESTS_PER_USER,
        projection_batch_size: DEFAULT_PROJECTION_BATCH_SIZE,
        projection_delay: Duration::ZERO,
        output_root: PathBuf::from(DEFAULT_OUTPUT_ROOT),
        preflight: PreflightConfig {
            observation: Duration::from_secs(2),
            timeout: Duration::from_secs(60),
            max_cpu_busy_pct: 10.0,
            max_disk_busy_pct: 5.0,
            min_available_mem_bytes: 2 * 1024 * 1024 * 1024,
            min_free_bytes: 1024 * 1024 * 1024,
        },
        free_space_reserve_bytes: 1024 * 1024 * 1024,
        smoke: false,
    };
    let mut explicit_users = false;
    let mut explicit_waves = false;
    let mut explicit_min_mem = false;
    let mut smoke = false;
    let mut args = args.into_iter();
    while let Some(argument) = args.next() {
        if argument == "--bench" {
            continue;
        }
        if argument == "--help" || argument == "-h" {
            print_help();
            std::process::exit(0);
        }
        if argument == "--smoke" {
            smoke = true;
            continue;
        }
        let value = args
            .next()
            .ok_or_else(|| format!("missing value after {argument}"))?;
        match argument.as_str() {
            "--users" => {
                config.users = parse_positive_usize(&argument, &value)?;
                explicit_users = true;
            }
            "--waves" => {
                config.waves = parse_positive_usize(&argument, &value)?;
                explicit_waves = true;
            }
            "--projection-batch-size" => {
                config.projection_batch_size = parse_positive_usize(&argument, &value)?;
            }
            "--projection-delay-ms" => {
                let delay = parse_u64(&argument, &value)?;
                config.projection_delay = Duration::from_millis(delay);
            }
            "--output-root" => config.output_root = PathBuf::from(value),
            "--preflight-observation-ms" => {
                config.preflight.observation = Duration::from_millis(parse_u64(&argument, &value)?)
            }
            "--preflight-timeout-secs" => {
                config.preflight.timeout = Duration::from_secs(parse_u64(&argument, &value)?)
            }
            "--preflight-max-cpu-percent" => {
                config.preflight.max_cpu_busy_pct = parse_f64(&argument, &value)?
            }
            "--preflight-max-disk-percent" => {
                config.preflight.max_disk_busy_pct = parse_f64(&argument, &value)?
            }
            "--min-available-mem-gib" => {
                config.preflight.min_available_mem_bytes = gib_bytes(&argument, &value)?;
                explicit_min_mem = true;
            }
            "--free-reserve-gib" => {
                config.free_space_reserve_bytes = gib_bytes(&argument, &value)?;
            }
            other => return Err(format!("unknown argument {other}")),
        }
    }
    if smoke {
        if !explicit_users {
            config.users = 200;
        }
        if !explicit_waves {
            config.waves = 20;
        }
        config.smoke = true;
    }
    if config.users < 2 {
        return Err("users must be at least 2 for the deterministic 50/50 workload".to_owned());
    }
    if config.waves == 0 || config.waves % 10 != 0 {
        return Err("waves must be a positive multiple of 10".to_owned());
    }
    if config.preflight.observation.is_zero()
        || config.preflight.timeout.is_zero()
        || config.preflight.observation > config.preflight.timeout
    {
        return Err("preflight observation and timeout must be positive and ordered".to_owned());
    }
    if !(0.0..=100.0).contains(&config.preflight.max_cpu_busy_pct)
        || !(0.0..=100.0).contains(&config.preflight.max_disk_busy_pct)
    {
        return Err("preflight CPU/disk busy limits must be in 0..=100".to_owned());
    }
    if !explicit_min_mem {
        let records = (config.users as u64)
            .checked_mul(config.waves as u64)
            .ok_or_else(|| "record count overflow".to_owned())?;
        let estimate = records
            .checked_mul(ESTIMATED_MOCK_BYTES_PER_RECORD)
            .and_then(|bytes| bytes.checked_add(DEFAULT_MEMORY_RESERVE_BYTES))
            .ok_or_else(|| "memory estimate overflow".to_owned())?;
        config.preflight.min_available_mem_bytes =
            config.preflight.min_available_mem_bytes.max(estimate);
    }
    Ok(config)
}

fn parse_positive_usize(argument: &str, value: &str) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|error| format!("invalid {argument} value {value}: {error}"))?;
    if parsed == 0 {
        return Err(format!("{argument} must be positive"));
    }
    Ok(parsed)
}

fn parse_u64(argument: &str, value: &str) -> Result<u64, String> {
    value
        .parse::<u64>()
        .map_err(|error| format!("invalid {argument} value {value}: {error}"))
}

fn parse_f64(argument: &str, value: &str) -> Result<f64, String> {
    let parsed = value
        .parse::<f64>()
        .map_err(|error| format!("invalid {argument} value {value}: {error}"))?;
    if !parsed.is_finite() {
        return Err(format!("{argument} must be finite"));
    }
    Ok(parsed)
}

fn gib_bytes(argument: &str, value: &str) -> Result<u64, String> {
    let gib = parse_u64(argument, value)?;
    gib.checked_mul(1024 * 1024 * 1024)
        .ok_or_else(|| format!("{argument} value is too large"))
}

fn print_help() {
    println!(
        "Tokio ledger projection worker benchmark\n\
         Defaults to 50,000 users x 200 requests (10,000,000 durable records) and runs all three cases.\n\
         --smoke                         Run all cases with 200 users x 20 requests unless overridden\n\
         --users N --waves N            Explicit workload size; waves must be a multiple of 10\n\
         --projection-batch-size N      Projection batch size (default 256)\n\
         --projection-delay-ms N        Synthetic async destination delay per batch (default 0)\n\
         --output-root PATH              Result/log directory (default target/ledger-projection-worker-tokio-trials)\n\
         --preflight-observation-ms N    Idle preflight observation interval (default 2000)\n\
         --preflight-timeout-secs N      Idle preflight timeout (default 60)\n\
         --preflight-max-cpu-percent N   Maximum host CPU busy percentage (default 10)\n\
         --preflight-max-disk-percent N  Maximum target-device busy percentage (default 5)\n\
         --min-available-mem-gib N       Minimum available RAM (default workload estimate, at least 2 GiB)\n\
         --free-reserve-gib N            Extra free-space reserve (default 1 GiB)"
    );
}

fn write_csv_header(writer: &mut impl Write) -> std::io::Result<()> {
    writeln!(
        writer,
        "case,users,waves,durable_records,foreground_batch_size,projection_batch_size,projection_delay_ms,foreground_transactions,foreground_wall_s,foreground_tx_rps,foreground_handler_p50_ns,foreground_handler_p95_ns,foreground_handler_p99_ns,foreground_handler_samples,measured_wall_s,projected_records,projected_sequence,projection_rps_measured,lag_foreground_end,lag_peak,foreground_cpu_s,measured_cpu_s,measured_cpu_cores,process_rchar_bytes,process_wchar_bytes,process_read_bytes,process_write_bytes,target_device,target_major_minor,target_read_bytes,target_write_bytes,target_busy_ms,rocks_bytes_read,rocks_bytes_written,rocks_wal_sync_count,rocks_wal_bytes,rocks_writes_with_wal,rocks_flush_write_bytes,rocks_compaction_read_bytes,rocks_compaction_write_bytes,rocks_stall_micros,read_p50_ns,read_p95_ns,read_p99_ns,read_samples,apply_p50_ns,apply_p95_ns,apply_p99_ns,apply_samples,delay_p50_ns,delay_p95_ns,delay_p99_ns,delay_samples,dispatch_p50_ns,dispatch_p95_ns,dispatch_p99_ns,dispatch_samples,total_p50_ns,total_p95_ns,total_p99_ns,total_samples,peak_rss_bytes,db_bytes,preflight_cpu_busy_pct,preflight_disk_busy_pct,preflight_mem_available_bytes,preflight_free_bytes,preflight_device"
    )
}

fn write_result(writer: &mut impl Write, result: &CaseResult) -> std::io::Result<()> {
    let latency = &result.latency;
    let fields = [
        result.case.name().to_owned(),
        result.users.to_string(),
        result.waves.to_string(),
        result.durable_records.to_string(),
        FOREGROUND_BATCH_SIZE.to_string(),
        result.projection_batch_size.to_string(),
        result.projection_delay_ms.to_string(),
        result.foreground_transactions.to_string(),
        format!("{:.9}", result.foreground_wall_s),
        format!(
            "{:.3}",
            rate(result.foreground_transactions, result.foreground_wall_s)
        ),
        result
            .foreground_transaction_latency
            .as_ref()
            .map_or_else(String::new, |latency| latency.p50_ns.to_string()),
        result
            .foreground_transaction_latency
            .as_ref()
            .map_or_else(String::new, |latency| latency.p95_ns.to_string()),
        result
            .foreground_transaction_latency
            .as_ref()
            .map_or_else(String::new, |latency| latency.p99_ns.to_string()),
        result
            .foreground_transaction_latency
            .as_ref()
            .map_or_else(String::new, |latency| latency.samples.to_string()),
        format!("{:.9}", result.measured_wall_s),
        result.projected_records.to_string(),
        result.projected_sequence.to_string(),
        format!(
            "{:.3}",
            rate(result.projected_records, result.measured_wall_s)
        ),
        result.foreground_end_lag.to_string(),
        result.peak_lag.to_string(),
        format!("{:.9}", result.foreground_cpu_s),
        format!("{:.9}", result.measured_cpu_s),
        format!(
            "{:.6}",
            cores(result.measured_cpu_s, result.measured_wall_s)
        ),
        result.io.process_rchar_bytes.to_string(),
        result.io.process_wchar_bytes.to_string(),
        result.io.process_read_bytes.to_string(),
        result.io.process_write_bytes.to_string(),
        result.io.target_device.clone(),
        result.io.target_major_minor.clone(),
        result.io.target_read_bytes.to_string(),
        result.io.target_write_bytes.to_string(),
        result.io.target_busy_ms.to_string(),
        result.rocks.bytes_read.to_string(),
        result.rocks.bytes_written.to_string(),
        result.rocks.wal_sync_count.to_string(),
        result.rocks.wal_bytes.to_string(),
        result.rocks.writes_with_wal.to_string(),
        result.rocks.flush_write_bytes.to_string(),
        result.rocks.compaction_read_bytes.to_string(),
        result.rocks.compaction_write_bytes.to_string(),
        result.rocks.stall_micros.to_string(),
        latency.ledger_read.p50_ns.to_string(),
        latency.ledger_read.p95_ns.to_string(),
        latency.ledger_read.p99_ns.to_string(),
        latency.ledger_read.samples.to_string(),
        latency.mock_apply.p50_ns.to_string(),
        latency.mock_apply.p95_ns.to_string(),
        latency.mock_apply.p99_ns.to_string(),
        latency.mock_apply.samples.to_string(),
        latency.synthetic_delay.p50_ns.to_string(),
        latency.synthetic_delay.p95_ns.to_string(),
        latency.synthetic_delay.p99_ns.to_string(),
        latency.synthetic_delay.samples.to_string(),
        latency.dispatch_wait.p50_ns.to_string(),
        latency.dispatch_wait.p95_ns.to_string(),
        latency.dispatch_wait.p99_ns.to_string(),
        latency.dispatch_wait.samples.to_string(),
        latency.total.p50_ns.to_string(),
        latency.total.p95_ns.to_string(),
        latency.total.p99_ns.to_string(),
        latency.total.samples.to_string(),
        result.peak_rss_bytes.to_string(),
        result.db_bytes.to_string(),
        format!("{:.6}", result.preflight.cpu_busy_pct),
        format!("{:.6}", result.preflight.disk_busy_pct),
        result.preflight.mem_available_bytes.to_string(),
        result.preflight.free_bytes.to_string(),
        result.preflight.device.clone(),
    ];
    writeln!(writer, "{}", fields.join(","))
}

fn result_log(result: &CaseResult) -> String {
    let tx_handler = result.foreground_transaction_latency.as_ref();
    format!(
        "users={} waves={} records={} foreground_wall_s={:.6} measured_wall_s={:.6} tx_rps={:.3} tx_handler_p50_ns={} tx_handler_p95_ns={} tx_handler_p99_ns={} tx_handler_samples={} projection_rps={:.3} projected_seq={} lag_foreground_end={} lag_peak={} cpu_s={:.6} cpu_cores={:.4} io_process_read={} io_process_write={} io_device={} io_device_read={} io_device_write={} rocks_read={} rocks_write={} wal_bytes={} wal_syncs={} flush_write={} compaction_read={} compaction_write={} stall_us={} read_p95_ns={} apply_p95_ns={} delay_p95_ns={} dispatch_p95_ns={} total_p95_ns={} db_bytes={} peak_rss_bytes={} preflight_attempts={} preflight_cpu_busy_pct={:.2} preflight_disk_busy_pct={:.2}",
        result.users,
        result.waves,
        result.durable_records,
        result.foreground_wall_s,
        result.measured_wall_s,
        rate(result.foreground_transactions, result.foreground_wall_s),
        tx_handler.map_or_else(|| "na".to_owned(), |p| p.p50_ns.to_string()),
        tx_handler.map_or_else(|| "na".to_owned(), |p| p.p95_ns.to_string()),
        tx_handler.map_or_else(|| "na".to_owned(), |p| p.p99_ns.to_string()),
        tx_handler.map_or_else(|| "na".to_owned(), |p| p.samples.to_string()),
        rate(result.projected_records, result.measured_wall_s),
        result.projected_sequence,
        result.foreground_end_lag,
        result.peak_lag,
        result.measured_cpu_s,
        cores(result.measured_cpu_s, result.measured_wall_s),
        result.io.process_read_bytes,
        result.io.process_write_bytes,
        result.io.target_device,
        result.io.target_read_bytes,
        result.io.target_write_bytes,
        result.rocks.bytes_read,
        result.rocks.bytes_written,
        result.rocks.wal_bytes,
        result.rocks.wal_sync_count,
        result.rocks.flush_write_bytes,
        result.rocks.compaction_read_bytes,
        result.rocks.compaction_write_bytes,
        result.rocks.stall_micros,
        result.latency.ledger_read.p95_ns,
        result.latency.mock_apply.p95_ns,
        result.latency.synthetic_delay.p95_ns,
        result.latency.dispatch_wait.p95_ns,
        result.latency.total.p95_ns,
        result.db_bytes,
        result.peak_rss_bytes,
        result.preflight.attempts,
        result.preflight.cpu_busy_pct,
        result.preflight.disk_busy_pct,
    )
}

fn io_error(context: &'static str) -> impl FnOnce(std::io::Error) -> String {
    move |error| format!("{context}: {error}")
}
