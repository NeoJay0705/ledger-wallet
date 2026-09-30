#[path = "support/ledger_account_store.rs"]
mod ledger_account_store;
#[path = "support/ledger_account_store_workload.rs"]
mod ledger_account_store_workload;
#[path = "support/ledger_preflight.rs"]
mod ledger_preflight;

use cpu_time::ProcessTime;
use ledger_account_store::{
    AccountStore, BalanceMode, Operation, Reply, RocksDbBudget, Transaction, TransactionKey,
    TransactionStatus,
};
use ledger_account_store_workload::{PatternKind, Workload, WorkloadKind, build_workload};
use ledger_preflight::{IoSample, PreflightConfig, PreflightReport};
use rocksdb::statistics::Ticker;
use rocksdb::{DB, Options};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::runtime::Builder;

const TOTAL_USERS: usize = 50_000;
const REQUESTS_PER_USER: usize = 200;
const BATCH_SIZE: usize = 4_096;
const CHECKPOINT_QUANTITY: u64 = 100_000;
const WORKERS: usize = 4;
const DEFAULT_OUTPUT_ROOT: &str = "target/ledger-account-store-sharding-trials";
const TOTAL_WRITE_BUFFER_BYTES: usize = 128 * 1024 * 1024;
const TOTAL_BLOCK_CACHE_BYTES: usize = 128 * 1024 * 1024;
const TOTAL_BACKGROUND_JOBS: i32 = 4;
const MAX_WRITE_BUFFERS: i32 = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Layout {
    Shared,
    Dedicated,
}

impl Layout {
    fn name(self) -> &'static str {
        match self {
            Self::Shared => "shared-db",
            Self::Dedicated => "dedicated-dbs",
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct CaseSpec {
    shards: usize,
    layout: Layout,
}

impl CaseSpec {
    fn label(self) -> String {
        match (self.shards, self.layout) {
            (1, _) => "s1-one-db-control".to_owned(),
            (shards, layout) => format!("s{shards}-{}", layout.name()),
        }
    }
}

#[derive(Clone, Debug)]
struct Config {
    users: usize,
    waves: usize,
    output_root: PathBuf,
    preflight: PreflightConfig,
    preflight_free_reserve_bytes: u64,
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
struct RocksDelta {
    wal_sync_count: u64,
    wal_bytes: u64,
    writes_with_wal: u64,
    flush_write_bytes: u64,
    compaction_read_bytes: u64,
    compaction_write_bytes: u64,
    stall_micros: u64,
}

#[derive(Clone, Debug, Default)]
struct RocksSnapshot {
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
struct LatencySummary {
    mean_ns: f64,
    p50_ns: u64,
    p95_ns: u64,
    p99_ns: u64,
    sample_count: usize,
}

struct ShardResult {
    shard_id: usize,
    accounts: usize,
    transactions: u64,
    wall_s: f64,
    rps: f64,
    latency: LatencySummary,
    final_seq: u64,
    checkpoints: u64,
}

struct CaseResult {
    label: String,
    shards: usize,
    layout: Layout,
    users: usize,
    waves: usize,
    requests: u64,
    transactions: u64,
    credits: u64,
    debits: u64,
    foreground_wall_s: f64,
    settled_wall_s: f64,
    drain_wall_s: f64,
    completed_rps: f64,
    settled_rps: f64,
    foreground_cpu_s: f64,
    foreground_cores: f64,
    settled_cpu_s: f64,
    settled_cores: f64,
    foreground_io: IoDelta,
    settled_io: IoDelta,
    latency: LatencySummary,
    batch_count: u64,
    read_build_ns: u64,
    wal_sync_ns: u64,
    publish_ns: u64,
    checkpoint_count: u64,
    checkpoint_snapshots_enqueued: u64,
    checkpoint_snapshot_ns: u64,
    checkpoint_queue_wait_ns: u64,
    checkpoint_chunk_sync_ns: u64,
    checkpoint_manifest_sync_ns: u64,
    checkpoint_duration_ns: u64,
    rocks: RocksDelta,
    recovery_s: f64,
    integrity_s: f64,
    db_bytes: u64,
    peak_rss_bytes: u64,
    preflight: PreflightReport,
    shard_rows: Vec<ShardResult>,
}

struct ShardTiming {
    transactions: u64,
    wall_s: f64,
}

struct RssSampler {
    peak: Arc<AtomicU64>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl RssSampler {
    fn start() -> Result<Self, String> {
        let peak = Arc::new(AtomicU64::new(current_rss_bytes()?));
        let worker_peak = Arc::clone(&peak);
        let (stop, mut stop_rx) = tokio::sync::oneshot::channel();
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
                .map_err(|error| format!("RSS sampler task failed: {error}"))?;
        }
        Ok(self.peak.load(Ordering::Relaxed))
    }
}

impl Drop for RssSampler {
    fn drop(&mut self) {
        if let Some(sender) = self.stop.take() {
            let _ = sender.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

fn main() {
    if let Err(error) = run_from_args() {
        eprintln!("ledger_account_store_sharding_tokio failed: {error}");
        std::process::exit(1);
    }
}

fn run_from_args() -> Result<(), String> {
    let total_started = Instant::now();
    let config = parse_args(std::env::args().skip(1))?;
    let workload = Arc::new(build_workload(
        WorkloadKind::CreditDebit50_50,
        config.users,
        config.waves,
    )?);
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("system clock is before Unix epoch: {error}"))?
        .as_nanos();
    fs::create_dir_all(&config.output_root).map_err(|error| {
        format!(
            "cannot create output root {}: {error}",
            config.output_root.display()
        )
    })?;
    let output_dir = config
        .output_root
        .join(format!("run-{run_id}-{}", std::process::id()));
    fs::create_dir(&output_dir).map_err(|error| {
        format!(
            "cannot create run directory {}: {error}",
            output_dir.display()
        )
    })?;
    let csv_path = output_dir.join("results.csv");
    let shard_csv_path = output_dir.join("shards.csv");
    let log_path = output_dir.join("run.log");
    let mut csv = BufWriter::new(
        File::create(&csv_path).map_err(|error| format!("cannot create CSV: {error}"))?,
    );
    let mut shard_csv = BufWriter::new(
        File::create(&shard_csv_path)
            .map_err(|error| format!("cannot create shard CSV: {error}"))?,
    );
    let mut log = BufWriter::new(
        File::create(&log_path).map_err(|error| format!("cannot create run log: {error}"))?,
    );
    writeln!(csv, "{}", csv_header()).map_err(io_error("write result CSV header"))?;
    writeln!(shard_csv, "{}", shard_csv_header()).map_err(io_error("write shard CSV header"))?;
    let cases = [
        CaseSpec {
            shards: 1,
            layout: Layout::Shared,
        },
        CaseSpec {
            shards: 2,
            layout: Layout::Shared,
        },
        CaseSpec {
            shards: 2,
            layout: Layout::Dedicated,
        },
        CaseSpec {
            shards: 4,
            layout: Layout::Shared,
        },
        CaseSpec {
            shards: 4,
            layout: Layout::Dedicated,
        },
    ];
    writeln!(
        log,
        "mode={} users={} waves={} requests={} transactions={} credits={} debits={} batch_size={} checkpoint_quantity={} workers={} cases=5 smoke={} output_dir={}",
        "checkpoint",
        workload.users,
        workload.waves.len(),
        workload.requests,
        workload.transactions,
        workload.operation_counts.credits,
        workload.operation_counts.debits,
        BATCH_SIZE,
        CHECKPOINT_QUANTITY,
        WORKERS,
        config.smoke,
        output_dir.display()
    )
    .map_err(io_error("write run header"))?;
    writeln!(
        log,
        "rocksdb_budgets_total_write_buffer_bytes={} max_buffers_per_db={} total_block_cache_bytes={} total_background_jobs={} key_namespace=5-byte shard prefix; sampled handler latency=stable account_id/tx_id hash mask 0x3ff",
        TOTAL_WRITE_BUFFER_BYTES,
        MAX_WRITE_BUFFERS,
        TOTAL_BLOCK_CACHE_BYTES,
        TOTAL_BACKGROUND_JOBS
    )
    .map_err(io_error("write budget header"))?;
    log.flush().map_err(io_error("flush run header"))?;

    let runtime = Builder::new_multi_thread()
        .worker_threads(WORKERS)
        .thread_name("account-shard-tokio")
        .enable_all()
        .build()
        .map_err(|error| format!("cannot build Tokio runtime: {error}"))?;
    for (index, case) in cases.into_iter().enumerate() {
        let label = case.label();
        writeln!(log, "case_start={label} case={}/5", index + 1)
            .map_err(io_error("write case start"))?;
        log.flush().map_err(io_error("flush case start"))?;
        let case_started = Instant::now();
        let result = runtime.block_on(run_case(
            &config,
            Arc::clone(&workload),
            case,
            &label,
            &output_dir,
        ));
        let case_wall_s = case_started.elapsed().as_secs_f64();
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                writeln!(
                    log,
                    "case_result={label} status=failed case_wall_s={case_wall_s:.6} error={error}"
                )
                .map_err(io_error("write case failure"))?;
                log.flush().map_err(io_error("flush case failure"))?;
                return Err(error);
            }
        };
        write_case_csv(&mut csv, &result).map_err(io_error("write result row"))?;
        csv.flush().map_err(io_error("flush result row"))?;
        for shard in &result.shard_rows {
            write_shard_csv(&mut shard_csv, &result, shard)
                .map_err(io_error("write shard result row"))?;
        }
        shard_csv
            .flush()
            .map_err(io_error("flush shard result rows"))?;
        writeln!(
            log,
            "case_result={label} status=passed preflight_status=passed case_wall_s={case_wall_s:.6} {}",
            result_to_log(&result)
        )
            .map_err(io_error("write case result"))?;
        for shard in &result.shard_rows {
            writeln!(
                log,
                "shard_result={} shard={} accounts={} transactions={} rps={:.3} seq={} checkpoints={} latency_p50_ns={} latency_p95_ns={} latency_p99_ns={} samples={}",
                label,
                shard.shard_id,
                shard.accounts,
                shard.transactions,
                shard.rps,
                shard.final_seq,
                shard.checkpoints,
                shard.latency.p50_ns,
                shard.latency.p95_ns,
                shard.latency.p99_ns,
                shard.latency.sample_count
            )
            .map_err(io_error("write shard result"))?;
        }
        log.flush().map_err(io_error("flush case result"))?;
        println!(
            "{label}: completed_rps={:.2} settled_rps={:.2} seq={} recovery_s={:.3} db_bytes={}",
            result.completed_rps,
            result.settled_rps,
            result.transactions,
            result.recovery_s,
            result.db_bytes
        );
    }
    let total_wall_s = total_started.elapsed().as_secs_f64();
    writeln!(
        log,
        "run_complete=true total_wall_s={total_wall_s:.6} results_csv={} shards_csv={}",
        csv_path.display(),
        shard_csv_path.display()
    )
    .map_err(io_error("write run complete"))?;
    log.flush().map_err(io_error("flush run complete"))?;
    println!("results: {}", output_dir.display());
    Ok(())
}

async fn run_case(
    config: &Config,
    workload: Arc<Workload>,
    case: CaseSpec,
    label: &str,
    output_dir: &Path,
) -> Result<CaseResult, String> {
    let case_dir = DbDirectory::create(output_dir.join(format!("{label}.db")))?;
    let db_count = if case.layout == Layout::Shared {
        1
    } else {
        case.shards
    };
    let budgets = (0..db_count)
        .map(|index| budget_for_database(db_count, case.shards, case.layout, index))
        .collect::<Vec<_>>();
    let mut databases = Vec::with_capacity(db_count);
    for (index, budget) in budgets.iter().cloned().enumerate() {
        let path = case_dir.path().join(if case.layout == Layout::Shared {
            "db-shared".to_owned()
        } else {
            format!("db-{index}")
        });
        let (db, options) = AccountStore::open_database_with_budget(&path, budget).await?;
        databases.push(Database { db, options });
    }

    let mut accounts_by_shard = Vec::with_capacity(case.shards);
    let mut stores = Vec::with_capacity(case.shards);
    for shard in 0..case.shards {
        let accounts: Vec<u64> = (0..workload.users as u64)
            .filter(|account| account % case.shards as u64 == shard as u64)
            .collect();
        let db_index = if case.layout == Layout::Shared {
            0
        } else {
            shard
        };
        let database = &databases[db_index];
        let store = AccountStore::open_on_database(
            Arc::clone(&database.db),
            Arc::clone(&database.options),
            accounts.clone(),
            shard as u32,
            BalanceMode::Checkpoint,
            CHECKPOINT_QUANTITY,
        )
        .await;
        let store = match store {
            Ok(store) => store,
            Err(error) => {
                let cleanup = shutdown_stores(&mut stores).await;
                drop(databases);
                return match cleanup {
                    Ok(()) => Err(error),
                    Err(cleanup_error) => Err(format!("{error}; cleanup failed: {cleanup_error}")),
                };
            }
        };
        accounts_by_shard.push(accounts);
        stores.push(store);
        let store = &stores[shard];
        if store.namespace_id() != Some(shard as u32)
            || store.account_ids() != accounts_by_shard[shard].as_slice()
        {
            let error = format!("shard {shard} opened with an unexpected namespace or account set");
            let cleanup = shutdown_stores(&mut stores).await;
            drop(databases);
            return match cleanup {
                Ok(()) => Err(error),
                Err(cleanup_error) => Err(format!("{error}; cleanup failed: {cleanup_error}")),
            };
        }
    }

    let result = async {
    // Initialize every DB and namespace before the strict case-level idle gate.
    let estimate = workload
        .transactions
        .checked_mul(192)
        .and_then(|bytes| bytes.checked_add(config.preflight_free_reserve_bytes))
        .ok_or_else(|| "preflight free-space estimate overflow".to_owned())?;
    let preflight_config = PreflightConfig {
        min_free_bytes: estimate,
        ..config.preflight.clone()
    };
    let preflight = ledger_preflight::ensure_idle(case_dir.path(), &preflight_config)
        .map_err(|error| format!("strict idle preflight failed for {label}: {error}"))?;
    let io_before = ledger_preflight::sample_io(case_dir.path())?;
    // Each DB is included exactly once here. Shared-DB shard handles have the
    // same ticker source and must not duplicate the global counters.
    let rocks_before: Vec<_> = databases
        .iter()
        .map(|database| rocks_snapshot(&database.options))
        .collect();
    let rss_sampler = RssSampler::start()?;
    let settled_cpu_start = ProcessTime::now();
    let foreground_cpu_start = ProcessTime::now();
    let foreground_started = Instant::now();
    let mut tasks = Vec::with_capacity(case.shards);
    for (shard, (store, accounts)) in stores.iter().zip(&accounts_by_shard).enumerate() {
        let store = store.clone();
        let accounts = accounts.clone();
        let workload = Arc::clone(&workload);
        tasks.push(tokio::spawn(async move {
            run_shard(store, accounts, shard, workload).await
        }));
    }
    let mut shard_timings = Vec::with_capacity(case.shards);
    let mut pending_tasks = tasks.into_iter();
    while let Some(task) = pending_tasks.next() {
        match task.await {
            Ok(Ok(timing)) => shard_timings.push(timing),
            Ok(Err(error)) => {
                for pending in pending_tasks {
                    pending.abort();
                    let _ = pending.await;
                }
                return Err(error);
            }
            Err(error) => {
                for pending in pending_tasks {
                    pending.abort();
                    let _ = pending.await;
                }
                return Err(format!("shard loop task failed: {error}"));
            }
        }
    }
    let foreground_wall = foreground_started.elapsed();
    let foreground_cpu_s = foreground_cpu_start.elapsed().as_secs_f64();
    let io_after_foreground = ledger_preflight::sample_io(case_dir.path())?;

    // Queue a checkpoint barrier for every shard before awaiting any one of
    // them, so checkpoint writers settle concurrently after foreground work.
    let drain_started = Instant::now();
    let mut drain_tasks = Vec::with_capacity(case.shards);
    for store in &stores {
        let store = store.clone();
        drain_tasks.push(tokio::spawn(async move { store.drain_checkpoints().await }));
    }
    let mut drain_error = None;
    for task in drain_tasks {
        match task.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                drain_error.get_or_insert(error);
            }
            Err(error) => {
                drain_error.get_or_insert_with(|| {
                    format!("checkpoint drain task failed: {error}")
                });
            }
        };
    }
    if let Some(error) = drain_error {
        return Err(error);
    }
    let drain_wall_s = drain_started.elapsed().as_secs_f64();
    let settled_wall = foreground_started.elapsed();
    let settled_cpu_s = settled_cpu_start.elapsed().as_secs_f64();
    let io_after_settle = ledger_preflight::sample_io(case_dir.path())?;
    let peak_rss_bytes = rss_sampler.stop().await?;
    let rocks_after: Vec<_> = databases
        .iter()
        .map(|database| rocks_snapshot(&database.options))
        .collect();
    let rocks = aggregate_rocks(&rocks_after, &rocks_before);
    let foreground_io = io_delta(&io_before, &io_after_foreground)?;
    let settled_io = io_delta(&io_before, &io_after_settle)?;

    let per_store_metrics: Vec<_> = stores.iter().map(AccountStore::metrics).collect();
    let total_batches = per_store_metrics.iter().map(|metrics| metrics.batches).sum();
    let expected_total = workload.transactions;
    let mut expected_credits = 0_u64;
    let mut expected_debits = 0_u64;
    let mut latency_samples = Vec::new();
    let mut total_transaction_count = 0_u64;
    let mut total_latency_ns = 0_u64;
    let mut shard_rows = Vec::with_capacity(case.shards);
    for shard in 0..case.shards {
        let store = &stores[shard];
        let metrics = &per_store_metrics[shard];
        let accounts = &accounts_by_shard[shard];
        let expected_seq = (accounts.len() as u64)
            .checked_mul(workload.waves.len() as u64)
            .ok_or_else(|| "per-shard sequence count overflow".to_owned())?;
        if store.latest_seq() != expected_seq {
            return Err(format!(
                "shard {shard} latest sequence {} differs from expected {expected_seq}",
                store.latest_seq()
            ));
        }
        if metrics.transactions != expected_seq {
            return Err(format!(
                "shard {shard} handled {} transactions, expected {expected_seq}",
                metrics.transactions
            ));
        }
        let balances = store.all_balances();
        if balances.len() != accounts.len()
            || balances
                .iter()
                .zip(accounts)
                .any(|((actual_account, balance), expected_account)| {
                    actual_account != expected_account || *balance != 0
                })
        {
            return Err(format!("shard {shard} final account balances are invalid"));
        }
        let expected_checkpoints = expected_seq / CHECKPOINT_QUANTITY;
        if metrics.checkpoint_count != expected_checkpoints
            || metrics.checkpoint_snapshots_enqueued != expected_checkpoints
        {
            return Err(format!(
                "shard {shard} checkpoint count/enqueues {}/{} differ from {expected_checkpoints}",
                metrics.checkpoint_count, metrics.checkpoint_snapshots_enqueued
            ));
        }
        let credits = (workload.waves.len() / 10) as u64 * accounts.len() as u64 * 5;
        let debits = credits;
        expected_credits += credits;
        expected_debits += debits;
        total_transaction_count += metrics.transactions;
        total_latency_ns = total_latency_ns.saturating_add(metrics.transaction_latency_ns);
        latency_samples.extend(metrics.transaction_samples_ns.iter().copied());
        let latency = summarize(metrics.transaction_latency_ns, metrics.transactions, &metrics.transaction_samples_ns);
        let timing = shard_timings
            .get(shard)
            .ok_or_else(|| format!("missing timing for shard {shard}"))?;
        let expected_timing_count = metrics.transactions;
        if timing.transactions != expected_timing_count {
            return Err(format!("shard {shard} transaction loop count is invalid"));
        }
        shard_rows.push(ShardResult {
            shard_id: shard,
            accounts: accounts.len(),
            transactions: metrics.transactions,
            wall_s: timing.wall_s,
            rps: timing.transactions as f64 / timing.wall_s,
            latency,
            final_seq: store.latest_seq(),
            checkpoints: metrics.checkpoint_count,
        });
    }
    if total_transaction_count != expected_total
        || expected_credits != workload.operation_counts.credits
        || expected_debits != workload.operation_counts.debits
        || expected_credits + expected_debits != expected_total
    {
        return Err(format!(
            "aggregate counts transactions={total_transaction_count}, credits={expected_credits}, debits={expected_debits} do not match workload"
        ));
    }
    let tx_latency = summarize(total_latency_ns, total_transaction_count, &latency_samples);
    let checkpoint_count = per_store_metrics
        .iter()
        .map(|metrics| metrics.checkpoint_count)
        .sum();
    let checkpoint_snapshots_enqueued = per_store_metrics
        .iter()
        .map(|metrics| metrics.checkpoint_snapshots_enqueued)
        .sum();
    let checkpoint_snapshot_ns = per_store_metrics
        .iter()
        .map(|metrics| metrics.checkpoint_snapshot_ns)
        .sum();
    let checkpoint_queue_wait_ns = per_store_metrics
        .iter()
        .map(|metrics| metrics.checkpoint_queue_wait_ns)
        .sum();
    let checkpoint_chunk_sync_ns = per_store_metrics
        .iter()
        .map(|metrics| metrics.checkpoint_chunk_sync_ns)
        .sum();
    let checkpoint_manifest_sync_ns = per_store_metrics
        .iter()
        .map(|metrics| metrics.checkpoint_manifest_sync_ns)
        .sum();
    let checkpoint_duration_ns = per_store_metrics
        .iter()
        .map(|metrics| metrics.checkpoint_duration_ns)
        .sum();
    let read_build_ns = per_store_metrics.iter().map(|metrics| metrics.read_build_ns).sum();
    let wal_sync_ns = per_store_metrics.iter().map(|metrics| metrics.wal_sync_ns).sum();
    let publish_ns = per_store_metrics.iter().map(|metrics| metrics.publish_ns).sum();
    let batch_count = total_batches;
    let foreground_wall_s = foreground_wall.as_secs_f64();
    let settled_wall_s = settled_wall.as_secs_f64();

    shutdown_stores(&mut stores).await?;
    drop(std::mem::take(&mut databases));

    let expected_seqs: Vec<_> = shard_rows.iter().map(|row| row.final_seq).collect();
    let (recovery_s, integrity_s, db_bytes) = recover_and_validate(
        case_dir.path(),
        &budgets,
        case,
        &accounts_by_shard,
        &expected_seqs,
    )
    .await?;
    Ok(CaseResult {
        label: label.to_owned(),
        shards: case.shards,
        layout: case.layout,
        users: workload.users,
        waves: workload.waves.len(),
        requests: workload.requests,
        transactions: total_transaction_count,
        credits: expected_credits,
        debits: expected_debits,
        foreground_wall_s,
        settled_wall_s,
        drain_wall_s,
        completed_rps: expected_total as f64 / foreground_wall_s,
        settled_rps: expected_total as f64 / settled_wall_s,
        foreground_cpu_s,
        foreground_cores: foreground_cpu_s / foreground_wall_s,
        settled_cpu_s,
        settled_cores: settled_cpu_s / settled_wall_s,
        foreground_io,
        settled_io,
        latency: tx_latency,
        batch_count,
        read_build_ns,
        wal_sync_ns,
        publish_ns,
        checkpoint_count,
        checkpoint_snapshots_enqueued,
        checkpoint_snapshot_ns,
        checkpoint_queue_wait_ns,
        checkpoint_chunk_sync_ns,
        checkpoint_manifest_sync_ns,
        checkpoint_duration_ns,
        rocks,
        recovery_s,
        integrity_s,
        db_bytes,
        peak_rss_bytes,
        preflight,
        shard_rows,
    })
    }
    .await;
    let cleanup_result = shutdown_stores(&mut stores).await;
    drop(databases);
    match (result, cleanup_result) {
        (Ok(result), Ok(())) => Ok(result),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(cleanup_error)) => Err(format!("case cleanup failed: {cleanup_error}")),
        (Err(error), Err(cleanup_error)) => {
            Err(format!("{error}; case cleanup failed: {cleanup_error}"))
        }
    }
}

async fn run_shard(
    store: AccountStore,
    accounts: Vec<u64>,
    shard_id: usize,
    workload: Arc<Workload>,
) -> Result<ShardTiming, String> {
    let started = Instant::now();
    let mut transaction_count = 0_u64;
    let waves_per_cycle = 10_usize;
    let transactions_per_account =
        workload.transactions_per_cycle * (workload.waves.len() as u64 / waves_per_cycle as u64);
    for wave_index in 0..workload.waves.len() {
        let cycle = wave_index / waves_per_cycle;
        let mut transactions = Vec::with_capacity(accounts.len());
        for account in &accounts {
            let entry = workload.patterns[*account as usize][wave_index % waves_per_cycle];
            let operation = match entry.kind {
                PatternKind::Credit => Operation::Credit,
                PatternKind::Debit => Operation::Debit,
                other => return Err(format!("50/50 shard workload produced {other:?}")),
            };
            let tx_id = (*account)
                .checked_mul(transactions_per_account)
                .and_then(|base| base.checked_add(cycle as u64 * workload.transactions_per_cycle))
                .and_then(|base| base.checked_add(u64::from(entry.transaction_offset) + 1))
                .ok_or_else(|| "transaction ID overflow".to_owned())?;
            let transaction_at = (*account)
                .checked_mul(workload.waves.len() as u64)
                .and_then(|offset| 1_700_000_000_000_000_u64.checked_add(offset))
                .and_then(|base| base.checked_add(wave_index as u64))
                .ok_or_else(|| "transaction timestamp overflow".to_owned())?;
            transactions.push(Transaction {
                key: TransactionKey {
                    account_id: *account,
                    tx_id,
                    transaction_at,
                },
                operation,
                amount: 1,
                refund_of: None,
            });
        }
        let mut pending = transactions.into_iter();
        loop {
            let batch: Vec<_> = pending.by_ref().take(BATCH_SIZE).collect();
            if batch.is_empty() {
                break;
            }
            let batch_len = batch.len();
            let replies = store.handle_batch(batch).await?;
            if replies.len() != batch_len {
                return Err(format!(
                    "shard {shard_id} handler returned wrong reply count"
                ));
            }
            for reply in replies {
                match reply {
                    Reply::Transaction {
                        status: TransactionStatus::Applied,
                        seq,
                        replayed: false,
                        ..
                    } => {
                        let expected_seq = transaction_count + 1;
                        if seq != expected_seq {
                            return Err(format!(
                                "shard {shard_id} received sequence {seq}, expected {expected_seq}"
                            ));
                        }
                        transaction_count += 1;
                    }
                    other => {
                        return Err(format!(
                            "shard {shard_id} produced an invalid 50/50 transaction result: {other:?}"
                        ));
                    }
                }
            }
        }
    }
    if transaction_count != (accounts.len() as u64) * workload.waves.len() as u64 {
        return Err(format!("shard {shard_id} offered-work count is invalid"));
    }
    Ok(ShardTiming {
        transactions: transaction_count,
        wall_s: started.elapsed().as_secs_f64(),
    })
}

async fn shutdown_stores(stores: &mut Vec<AccountStore>) -> Result<(), String> {
    let pending = std::mem::take(stores);
    let mut first_error = None;
    for store in pending {
        if let Err(error) = store.shutdown().await {
            first_error.get_or_insert(error);
        }
    }
    first_error.map_or(Ok(()), Err)
}

async fn recover_and_validate(
    case_dir: &Path,
    budgets: &[RocksDbBudget],
    case: CaseSpec,
    accounts_by_shard: &[Vec<u64>],
    expected_seqs: &[u64],
) -> Result<(f64, f64, u64), String> {
    let recovery_started = Instant::now();
    let mut databases = Vec::with_capacity(budgets.len());
    for (index, budget) in budgets.iter().cloned().enumerate() {
        let path = case_dir.join(if case.layout == Layout::Shared {
            "db-shared".to_owned()
        } else {
            format!("db-{index}")
        });
        let (db, options) = AccountStore::open_database_with_budget(&path, budget).await?;
        databases.push(Database { db, options });
    }
    let mut stores = Vec::with_capacity(case.shards);
    let open_result = async {
        for shard in 0..case.shards {
            let db_index = if case.layout == Layout::Shared {
                0
            } else {
                shard
            };
            let store = AccountStore::open_on_database(
                Arc::clone(&databases[db_index].db),
                Arc::clone(&databases[db_index].options),
                accounts_by_shard[shard].clone(),
                shard as u32,
                BalanceMode::Checkpoint,
                CHECKPOINT_QUANTITY,
            )
            .await?;
            stores.push(store);
            let store = &stores[shard];
            if store.namespace_id() != Some(shard as u32)
                || store.account_ids() != accounts_by_shard[shard].as_slice()
            {
                return Err(format!(
                    "shard {shard} recovered with an unexpected namespace or account set"
                ));
            }
            if store.latest_seq() != expected_seqs[shard] {
                return Err(format!(
                    "shard {shard} recovery sequence {} differs from {}",
                    store.latest_seq(),
                    expected_seqs[shard]
                ));
            }
            let balances = store.all_balances();
            if balances.len() != accounts_by_shard[shard].len()
                || balances.iter().zip(&accounts_by_shard[shard]).any(
                    |((account, balance), expected_account)| {
                        account != expected_account || *balance != 0
                    },
                )
            {
                return Err(format!(
                    "shard {shard} recovery did not reproduce account balances"
                ));
            }
        }
        Ok::<(), String>(())
    }
    .await;
    if let Err(error) = open_result {
        let cleanup = shutdown_stores(&mut stores).await;
        drop(databases);
        return match cleanup {
            Ok(()) => Err(error),
            Err(cleanup_error) => Err(format!("{error}; recovery cleanup failed: {cleanup_error}")),
        };
    }
    let recovery_s = recovery_started.elapsed().as_secs_f64();
    let integrity_started = Instant::now();
    let mut integrity_result = Ok(());
    for (shard, store) in stores.iter().enumerate() {
        if let Err(error) = store.validate_integrity().await {
            integrity_result = Err(format!("shard {shard} integrity scan failed: {error}"));
            break;
        }
    }
    let integrity_s = integrity_started.elapsed().as_secs_f64();
    let cleanup_result = shutdown_stores(&mut stores).await;
    drop(databases);
    if let Err(error) = integrity_result {
        return match cleanup_result {
            Ok(()) => Err(error),
            Err(cleanup_error) => Err(format!("{error}; recovery cleanup failed: {cleanup_error}")),
        };
    }
    cleanup_result?;
    let db_bytes = directory_bytes(case_dir)?;
    Ok((recovery_s, integrity_s, db_bytes))
}

fn budget_for_database(
    db_count: usize,
    shard_count: usize,
    layout: Layout,
    _database_index: usize,
) -> RocksDbBudget {
    let buffers = usize::try_from(MAX_WRITE_BUFFERS).expect("positive write buffer count");
    let write_buffer_size = TOTAL_WRITE_BUFFER_BYTES / db_count / buffers;
    let block_cache_bytes = TOTAL_BLOCK_CACHE_BYTES / db_count;
    let job_groups = if layout == Layout::Shared {
        1
    } else {
        shard_count
    };
    let max_background_jobs = TOTAL_BACKGROUND_JOBS / job_groups as i32;
    RocksDbBudget {
        write_buffer_size,
        max_write_buffer_number: MAX_WRITE_BUFFERS,
        block_cache_bytes,
        max_background_jobs,
    }
}

fn rocks_snapshot(options: &Options) -> RocksSnapshot {
    RocksSnapshot {
        wal_sync_count: options.get_ticker_count(Ticker::WalFileSynced),
        wal_bytes: options.get_ticker_count(Ticker::WalFileBytes),
        writes_with_wal: options.get_ticker_count(Ticker::WriteWithWal),
        flush_write_bytes: options.get_ticker_count(Ticker::FlushWriteBytes),
        compaction_read_bytes: options.get_ticker_count(Ticker::CompactReadBytes),
        compaction_write_bytes: options.get_ticker_count(Ticker::CompactWriteBytes),
        stall_micros: options.get_ticker_count(Ticker::StallMicros),
    }
}

fn aggregate_rocks(after: &[RocksSnapshot], before: &[RocksSnapshot]) -> RocksDelta {
    after
        .iter()
        .cloned()
        .zip(before.iter().cloned())
        .map(|(after, before)| after.delta(before))
        .fold(RocksDelta::default(), |mut total, delta| {
            total.wal_sync_count += delta.wal_sync_count;
            total.wal_bytes += delta.wal_bytes;
            total.writes_with_wal += delta.writes_with_wal;
            total.flush_write_bytes += delta.flush_write_bytes;
            total.compaction_read_bytes += delta.compaction_read_bytes;
            total.compaction_write_bytes += delta.compaction_write_bytes;
            total.stall_micros += delta.stall_micros;
            total
        })
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

fn summarize(total_ns: u64, transactions: u64, samples: &[u64]) -> LatencySummary {
    let mut ordered = samples.to_vec();
    ordered.sort_unstable();
    let percentile = |numerator: usize| {
        if ordered.is_empty() {
            0
        } else {
            let rank = (ordered.len() * numerator).div_ceil(100).max(1);
            let index = (rank - 1).min(ordered.len() - 1);
            ordered[index]
        }
    };
    LatencySummary {
        mean_ns: if transactions == 0 {
            0.0
        } else {
            total_ns as f64 / transactions as f64
        },
        p50_ns: percentile(50),
        p95_ns: percentile(95),
        p99_ns: percentile(99),
        sample_count: ordered.len(),
    }
}

fn current_rss_bytes() -> Result<u64, String> {
    let status = fs::read_to_string("/proc/self/status")
        .map_err(|error| format!("cannot read /proc/self/status: {error}"))?;
    let value_kib = status
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmRSS:")
                .and_then(|value| value.split_whitespace().next())
        })
        .ok_or_else(|| "VmRSS entry is missing from /proc/self/status".to_owned())?
        .parse::<u64>()
        .map_err(|error| format!("invalid VmRSS entry: {error}"))?;
    value_kib
        .checked_mul(1024)
        .ok_or_else(|| "VmRSS value overflow".to_owned())
}

fn directory_bytes(path: &Path) -> Result<u64, String> {
    let entries = fs::read_dir(path)
        .map_err(|error| format!("cannot list DB directory {}: {error}", path.display()))?;
    let mut total = 0_u64;
    for entry in entries {
        let entry = entry.map_err(|error| format!("cannot inspect DB entry: {error}"))?;
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
            .ok_or_else(|| "database directory size overflow".to_owned())?;
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
        output_root: PathBuf::from(DEFAULT_OUTPUT_ROOT),
        preflight: PreflightConfig {
            observation: Duration::from_secs(3),
            timeout: Duration::from_secs(60),
            max_cpu_busy_pct: 10.0,
            max_disk_busy_pct: 5.0,
            min_available_mem_bytes: 128 * 1024 * 1024,
            min_free_bytes: 1024 * 1024 * 1024,
        },
        preflight_free_reserve_bytes: 1024 * 1024 * 1024,
        smoke: false,
    };
    let mut smoke_users = None;
    let mut smoke_waves = None;
    let mut args = args.into_iter();
    while let Some(argument) = args.next() {
        if argument == "--bench" {
            continue;
        }
        if argument == "--help" || argument == "-h" {
            print_help();
            std::process::exit(0);
        }
        let value = args
            .next()
            .ok_or_else(|| format!("missing value after {argument}"))?;
        match argument.as_str() {
            "--smoke-users" => smoke_users = Some(positive_usize(&value, "smoke-users")?),
            "--smoke-waves" => smoke_waves = Some(positive_usize(&value, "smoke-waves")?),
            "--output-dir" => config.output_root = PathBuf::from(value),
            "--preflight-observation-ms" => {
                config.preflight.observation =
                    Duration::from_millis(positive_u64(&value, "preflight-observation-ms")?)
            }
            "--preflight-timeout-ms" => {
                config.preflight.timeout =
                    Duration::from_millis(positive_u64(&value, "preflight-timeout-ms")?)
            }
            "--preflight-max-cpu-pct" => {
                config.preflight.max_cpu_busy_pct = parse_pct(&value, "preflight-max-cpu-pct")?
            }
            "--preflight-max-disk-busy-pct" => {
                config.preflight.max_disk_busy_pct =
                    parse_pct(&value, "preflight-max-disk-busy-pct")?
            }
            "--preflight-min-mem-bytes" => {
                config.preflight.min_available_mem_bytes =
                    positive_u64(&value, "preflight-min-mem-bytes")?
            }
            "--preflight-free-reserve-bytes" => {
                config.preflight_free_reserve_bytes =
                    positive_u64(&value, "preflight-free-reserve-bytes")?
            }
            _ => return Err(format!("unknown argument {argument}")),
        }
    }
    match (smoke_users, smoke_waves) {
        (Some(users), Some(waves)) => {
            if users < 2 {
                return Err(
                    "smoke-users must be at least 2 for valid mixed account patterns".to_owned(),
                );
            }
            if waves == 0 || waves % 10 != 0 {
                return Err("smoke-waves must be a positive multiple of 10".to_owned());
            }
            config.users = users;
            config.waves = waves;
            config.smoke = true;
        }
        (None, None) => {}
        _ => return Err("pass both --smoke-users and --smoke-waves together".to_owned()),
    }
    if config.users % 4 != 0 {
        return Err("user count must be divisible by 4 for balanced S=1,2,4 cases".to_owned());
    }
    if config.preflight.observation > config.preflight.timeout {
        return Err("preflight observation must not exceed timeout".to_owned());
    }
    Ok(config)
}

fn positive_usize(value: &str, label: &str) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|error| format!("invalid {label}: {error}"))?;
    if parsed == 0 {
        return Err(format!("{label} must be positive"));
    }
    Ok(parsed)
}

fn positive_u64(value: &str, label: &str) -> Result<u64, String> {
    let parsed = value
        .parse::<u64>()
        .map_err(|error| format!("invalid {label}: {error}"))?;
    if parsed == 0 {
        return Err(format!("{label} must be positive"));
    }
    Ok(parsed)
}

fn parse_pct(value: &str, label: &str) -> Result<f64, String> {
    let parsed = value
        .parse::<f64>()
        .map_err(|error| format!("invalid {label}: {error}"))?;
    if !parsed.is_finite() || !(0.0..=100.0).contains(&parsed) {
        return Err(format!("{label} must be finite and in 0..=100"));
    }
    Ok(parsed)
}

fn print_help() {
    println!(
        "ledger_account_store_sharding_tokio options:\n\
         (default) 50,000 accounts x 200 requests, batch 4096, checkpoint mode\n\
         --smoke-users N --smoke-waves N (explicit reduced smoke workload; both required)\n\
         --output-dir PATH (default target/ledger-account-store-sharding-trials)\n\
         --preflight-observation-ms N --preflight-timeout-ms N\n\
         --preflight-max-cpu-pct P --preflight-max-disk-busy-pct P\n\
         --preflight-min-mem-bytes N --preflight-free-reserve-bytes N"
    );
}

fn csv_header() -> &'static str {
    "case,shards,layout,users,requests_per_user,total_requests,durable_transactions,credits,debits,batch_size,balance_mode,checkpoint_quantity,foreground_wall_s,completed_rps,checkpoint_drain_wall_s,settled_wall_s,settled_rps,foreground_cpu_s,foreground_cores,settled_cpu_s,settled_cores,foreground_proc_rchar_bytes,foreground_proc_wchar_bytes,foreground_proc_read_bytes,foreground_proc_write_bytes,foreground_target_read_bytes,foreground_target_write_bytes,foreground_target_busy_ms,settled_proc_rchar_bytes,settled_proc_wchar_bytes,settled_proc_read_bytes,settled_proc_write_bytes,settled_target_device,settled_target_major_minor,settled_target_read_bytes,settled_target_write_bytes,settled_target_busy_ms,handler_mean_ns,handler_p50_ns,handler_p95_ns,handler_p99_ns,handler_samples,batches,read_build_ns,wal_sync_ns,publish_ns,checkpoint_count,checkpoint_enqueued,checkpoint_snapshot_ns,checkpoint_queue_wait_ns,checkpoint_chunk_sync_ns,checkpoint_manifest_sync_ns,checkpoint_duration_ns,rocks_wal_sync_count,rocks_wal_bytes,rocks_writes_with_wal,rocks_flush_write_bytes,rocks_compaction_read_bytes,rocks_compaction_write_bytes,rocks_stall_micros,recovery_s,integrity_scan_s,db_bytes,sampled_peak_rss_bytes,preflight_cpu_busy_pct,preflight_disk_busy_pct,preflight_mem_available_bytes,preflight_free_bytes"
}

fn shard_csv_header() -> &'static str {
    "case,shards,layout,shard_id,accounts,transactions,wall_s,transactions_per_second,sequence,checkpoints,handler_p50_ns,handler_p95_ns,handler_p99_ns,handler_samples"
}

fn write_case_csv(writer: &mut impl Write, result: &CaseResult) -> std::io::Result<()> {
    let f = |value: f64| format!("{value:.9}");
    let fields = vec![
        result.label.clone(),
        result.shards.to_string(),
        result.layout.name().to_owned(),
        result.users.to_string(),
        result.waves.to_string(),
        result.requests.to_string(),
        result.transactions.to_string(),
        result.credits.to_string(),
        result.debits.to_string(),
        BATCH_SIZE.to_string(),
        "checkpoint".to_owned(),
        CHECKPOINT_QUANTITY.to_string(),
        f(result.foreground_wall_s),
        f(result.completed_rps),
        f(result.drain_wall_s),
        f(result.settled_wall_s),
        f(result.settled_rps),
        f(result.foreground_cpu_s),
        f(result.foreground_cores),
        f(result.settled_cpu_s),
        f(result.settled_cores),
        result.foreground_io.process_rchar_bytes.to_string(),
        result.foreground_io.process_wchar_bytes.to_string(),
        result.foreground_io.process_read_bytes.to_string(),
        result.foreground_io.process_write_bytes.to_string(),
        result.foreground_io.target_read_bytes.to_string(),
        result.foreground_io.target_write_bytes.to_string(),
        result.foreground_io.target_busy_ms.to_string(),
        result.settled_io.process_rchar_bytes.to_string(),
        result.settled_io.process_wchar_bytes.to_string(),
        result.settled_io.process_read_bytes.to_string(),
        result.settled_io.process_write_bytes.to_string(),
        result.settled_io.target_device.clone(),
        result.settled_io.target_major_minor.clone(),
        result.settled_io.target_read_bytes.to_string(),
        result.settled_io.target_write_bytes.to_string(),
        result.settled_io.target_busy_ms.to_string(),
        f(result.latency.mean_ns),
        result.latency.p50_ns.to_string(),
        result.latency.p95_ns.to_string(),
        result.latency.p99_ns.to_string(),
        result.latency.sample_count.to_string(),
        result.batch_count.to_string(),
        result.read_build_ns.to_string(),
        result.wal_sync_ns.to_string(),
        result.publish_ns.to_string(),
        result.checkpoint_count.to_string(),
        result.checkpoint_snapshots_enqueued.to_string(),
        result.checkpoint_snapshot_ns.to_string(),
        result.checkpoint_queue_wait_ns.to_string(),
        result.checkpoint_chunk_sync_ns.to_string(),
        result.checkpoint_manifest_sync_ns.to_string(),
        result.checkpoint_duration_ns.to_string(),
        result.rocks.wal_sync_count.to_string(),
        result.rocks.wal_bytes.to_string(),
        result.rocks.writes_with_wal.to_string(),
        result.rocks.flush_write_bytes.to_string(),
        result.rocks.compaction_read_bytes.to_string(),
        result.rocks.compaction_write_bytes.to_string(),
        result.rocks.stall_micros.to_string(),
        f(result.recovery_s),
        f(result.integrity_s),
        result.db_bytes.to_string(),
        result.peak_rss_bytes.to_string(),
        f(result.preflight.cpu_busy_pct),
        f(result.preflight.disk_busy_pct),
        result.preflight.mem_available_bytes.to_string(),
        result.preflight.free_bytes.to_string(),
    ];

    let expected_fields = csv_header().split(',').count();
    if fields.len() != expected_fields {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "case CSV row has {} fields but header has {expected_fields}",
                fields.len()
            ),
        ));
    }

    writeln!(writer, "{}", fields.join(","))
}

fn write_shard_csv(
    writer: &mut impl Write,
    result: &CaseResult,
    shard: &ShardResult,
) -> std::io::Result<()> {
    writeln!(
        writer,
        "{},{},{},{},{},{},{:.9},{:.9},{},{},{},{},{},{}",
        result.label,
        result.shards,
        result.layout.name(),
        shard.shard_id,
        shard.accounts,
        shard.transactions,
        shard.wall_s,
        shard.rps,
        shard.final_seq,
        shard.checkpoints,
        shard.latency.p50_ns,
        shard.latency.p95_ns,
        shard.latency.p99_ns,
        shard.latency.sample_count
    )
}

fn result_to_log(result: &CaseResult) -> String {
    format!(
        "layout={} shards={} requests={} transactions={} credits={} debits={} completed_rps={:.3} settled_rps={:.3} handler_p50_ns={} handler_p95_ns={} handler_p99_ns={} samples={} cpu_cores={:.3} settled_cores={:.3} checkpoints={} checkpoint_enqueued={} rocks_wal_syncs={} rocks_wal_bytes={} rocks_flush_bytes={} compaction_read_bytes={} compaction_write_bytes={} stalls_us={} recovery_s={:.6} integrity_s={:.6} db_bytes={} sampled_peak_rss_bytes={} preflight_attempts={} preflight_observation_ms={:.3} preflight_cpu_busy_pct={:.3} preflight_disk_busy_pct={:.3} preflight_mem_available_bytes={} preflight_free_bytes={} target_device={}",
        result.layout.name(),
        result.shards,
        result.requests,
        result.transactions,
        result.credits,
        result.debits,
        result.completed_rps,
        result.settled_rps,
        result.latency.p50_ns,
        result.latency.p95_ns,
        result.latency.p99_ns,
        result.latency.sample_count,
        result.foreground_cores,
        result.settled_cores,
        result.checkpoint_count,
        result.checkpoint_snapshots_enqueued,
        result.rocks.wal_sync_count,
        result.rocks.wal_bytes,
        result.rocks.flush_write_bytes,
        result.rocks.compaction_read_bytes,
        result.rocks.compaction_write_bytes,
        result.rocks.stall_micros,
        result.recovery_s,
        result.integrity_s,
        result.db_bytes,
        result.peak_rss_bytes,
        result.preflight.attempts,
        result.preflight.observation.as_secs_f64() * 1_000.0,
        result.preflight.cpu_busy_pct,
        result.preflight.disk_busy_pct,
        result.preflight.mem_available_bytes,
        result.preflight.free_bytes,
        result.preflight.device
    )
}

fn io_error(context: &'static str) -> impl FnOnce(std::io::Error) -> String {
    move |error| format!("{context} failed: {error}")
}
