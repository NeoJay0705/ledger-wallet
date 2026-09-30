#[path = "support/ledger_account_store.rs"]
mod ledger_account_store;
#[path = "support/ledger_account_store_workload.rs"]
mod ledger_account_store_workload;
#[path = "support/ledger_preflight.rs"]
mod ledger_preflight;

use cpu_time::ProcessTime;
use ledger_account_store::{
    AccountStore, BalanceMode, Operation, Reply, Transaction, TransactionKey, TransactionStatus,
};
use ledger_account_store_workload::{
    build_workload, OperationCounts, PatternKind, Workload, WorkloadKind,
};
use ledger_preflight::{IoSample, PreflightConfig, PreflightReport};
use std::error::Error;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::runtime::Builder;

const DEFAULT_USERS: usize = 50_000;
const DEFAULT_WAVES: usize = 200;
const DEFAULT_BATCH_SIZES: &[usize] = &[2_048, 4_096];
const DEFAULT_CHECKPOINT_QUANTITY: u64 = 100_000;
const DEFAULT_DB_ROOT: &str = "target/ledger-account-store-trials";
const WORKERS: usize = 3;

#[derive(Clone, Debug)]
struct Config {
    workload: WorkloadKind,
    users: usize,
    waves: usize,
    batch_sizes: Vec<usize>,
    modes: Vec<BalanceMode>,
    repetitions: usize,
    checkpoint_quantity: u64,
    output_root: PathBuf,
    preflight: PreflightConfig,
    preflight_free_reserve_bytes: u64,
}

#[derive(Default)]
struct Counts {
    credits: u64,
    debits: u64,
    refunds: u64,
    queries: u64,
    applied_refunds: u64,
    last_seq: u64,
}

impl Counts {
    fn operation_counts(&self) -> OperationCounts {
        OperationCounts {
            credits: self.credits,
            debits: self.debits,
            refunds: self.refunds,
            queries: self.queries,
        }
    }
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

struct WindowMetrics {
    foreground_wall: Duration,
    drain_wall: Duration,
    settled_wall: Duration,
    foreground_cpu_seconds: f64,
    drain_cpu_seconds: f64,
    settled_cpu_seconds: f64,
    foreground_io: IoDelta,
    settled_io: IoDelta,
    rocks: (u64, u64, u64, u64, u64, u64, u64),
    counts: Counts,
    balances: Vec<u64>,
    latest_seq: u64,
    preflight: PreflightReport,
}

struct LatencySummary {
    mean_ns: f64,
    p50_ns: u64,
    p95_ns: u64,
    p99_ns: u64,
    sample_count: usize,
}

struct CaseResult {
    label: String,
    workload: WorkloadKind,
    batch_size: usize,
    mode: BalanceMode,
    repetition: usize,
    users: usize,
    waves: usize,
    plan_generation_s: f64,
    requests: u64,
    transaction_requests: u64,
    committed_transactions: u64,
    operation_counts: OperationCounts,
    completed_rps: f64,
    transaction_rps: f64,
    settled_request_rps: f64,
    settled_transaction_rps: f64,
    foreground_wall_s: f64,
    drain_wall_s: f64,
    settled_wall_s: f64,
    foreground_cpu_s: f64,
    foreground_cores: f64,
    drain_cpu_s: f64,
    settled_cpu_s: f64,
    settled_cores: f64,
    foreground_io: IoDelta,
    settled_io: IoDelta,
    tx_latency: LatencySummary,
    query_latency: Option<LatencySummary>,
    batch_count: u64,
    read_build_mean_ns: f64,
    wal_sync_mean_ns: f64,
    publish_mean_ns: f64,
    checkpoint_count: u64,
    checkpoint_snapshot_ns: u64,
    checkpoint_queue_wait_ns: u64,
    checkpoint_chunk_sync_ns: u64,
    checkpoint_manifest_sync_ns: u64,
    checkpoint_duration_ns: u64,
    checkpoint_lag: u64,
    checkpoint_snapshots_enqueued: u64,
    rocks: (u64, u64, u64, u64, u64, u64, u64),
    recovery_s: f64,
    integrity_s: f64,
    db_bytes: u64,
    preflight: PreflightReport,
}

struct DbDirectory(PathBuf);

impl DbDirectory {
    fn create(path: PathBuf) -> Result<Self, String> {
        if path.exists() {
            return Err(format!(
                "trial DB directory already exists: {}",
                path.display()
            ));
        }
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

fn main() {
    if let Err(error) = run_from_args() {
        eprintln!("ledger_account_store_tokio failed: {error}");
        std::process::exit(1);
    }
}

fn run_from_args() -> Result<(), String> {
    let config = parse_args(std::env::args().skip(1))?;
    let workload = build_workload(config.workload, config.users, config.waves)?;
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("system clock is before Unix epoch: {error}"))?
        .as_nanos();
    let output_dir = config
        .output_root
        .join(format!("run-{run_id}-{}", std::process::id()));
    fs::create_dir_all(&output_dir)
        .map_err(|error| format!("cannot create output dir {}: {error}", output_dir.display()))?;
    let csv_path = output_dir.join("results.csv");
    let log_path = output_dir.join("run.log");
    let mut csv = BufWriter::new(
        File::create(&csv_path).map_err(|error| format!("cannot create CSV: {error}"))?,
    );
    let mut log = BufWriter::new(
        File::create(&log_path).map_err(|error| format!("cannot create run log: {error}"))?,
    );
    writeln!(csv, "{}", csv_header()).map_err(io_error("write CSV header"))?;
    writeln!(
        log,
        "workload={} users={} waves={} requests={} transactions={} plan_generation_s={:.6} checkpoint_quantity={} time_trigger=disabled workers={} batch_sizes={:?} modes={:?} output_dir={}",
        workload.kind.name(),
        workload.users,
        workload.waves.len(),
        workload.requests,
        workload.transactions,
        workload.generation_ns as f64 / 1e9,
        config.checkpoint_quantity,
        WORKERS,
        config.batch_sizes,
        config.modes,
        output_dir.display()
    )
    .map_err(io_error("write run header"))?;
    log.flush().map_err(io_error("flush run header"))?;

    let runtime = Builder::new_multi_thread()
        .worker_threads(WORKERS)
        .thread_name("account-store-tokio")
        .enable_all()
        .build()
        .map_err(|error| format!("cannot build Tokio runtime: {error}"))?;
    let mut case_number = 0_usize;
    let total_cases = config.batch_sizes.len() * config.modes.len() * config.repetitions;
    for batch_size in &config.batch_sizes {
        for mode in &config.modes {
            for repetition in 1..=config.repetitions {
                case_number += 1;
                let label = format!(
                    "case-{case_number:02}-b{batch_size}-{}-r{repetition}",
                    mode_name(*mode)
                );
                writeln!(log, "case_start={label} case={case_number}/{total_cases}")
                    .map_err(io_error("write case start"))?;
                log.flush().map_err(io_error("flush case start"))?;
                let result = runtime.block_on(run_case(
                    &config,
                    &workload,
                    *batch_size,
                    *mode,
                    repetition,
                    label.clone(),
                    &output_dir,
                ));
                match result {
                    Ok(result) => {
                        write_case_csv(&mut csv, &result).map_err(io_error("write case CSV"))?;
                        csv.flush().map_err(io_error("flush case CSV"))?;
                        writeln!(
                            log,
                            "case_result={label} status=passed csv_row={}",
                            result_to_log(&result)
                        )
                        .map_err(io_error("write case result"))?;
                        log.flush().map_err(io_error("flush case result"))?;
                        println!(
                            "{label}: all_rps={:.2} tx_rps={:.2} settled_all_rps={:.2} recovery_s={:.3} db_bytes={}",
                            result.completed_rps,
                            result.transaction_rps,
                            result.settled_request_rps,
                            result.recovery_s,
                            result.db_bytes
                        );
                    }
                    Err(error) => {
                        writeln!(log, "case_result={label} status=failed error={error}")
                            .map_err(io_error("write failed case"))?;
                        log.flush().map_err(io_error("flush failed case"))?;
                        return Err(format!(
                            "{label}: {error}; details in {}",
                            log_path.display()
                        ));
                    }
                }
            }
        }
    }
    writeln!(log, "run_status=passed").map_err(io_error("write run status"))?;
    log.flush().map_err(io_error("flush run status"))?;
    println!("results_csv={}", csv_path.display());
    println!("run_log={}", log_path.display());
    Ok(())
}

async fn run_case(
    config: &Config,
    workload: &Workload,
    batch_size: usize,
    mode: BalanceMode,
    repetition: usize,
    label: String,
    output_dir: &Path,
) -> Result<CaseResult, String> {
    let db_dir = DbDirectory::create(output_dir.join(format!("{label}.db")))?;
    // DB initialization precedes strict preflight so its small setup I/O settles
    // before the measured request window starts.
    let store = AccountStore::open(
        db_dir.path(),
        workload.users,
        mode,
        config.checkpoint_quantity,
    )
    .await?;
    let measured = run_measured_window(config, workload, batch_size, &store, db_dir.path()).await;
    let metrics = store.metrics();
    let latest_seq = store.latest_seq();
    let final_balances = store.all_balances();
    let shutdown = store.shutdown().await;
    shutdown?;
    let measured = measured?;

    if latest_seq != workload.transactions {
        return Err(format!(
            "latest sequence {latest_seq} does not match {} new transactions",
            workload.transactions
        ));
    }
    if measured.latest_seq != latest_seq || measured.counts.last_seq != latest_seq {
        return Err("foreground sequence counter did not match committed sequence".to_owned());
    }
    let measured_balances: Vec<_> = measured
        .balances
        .iter()
        .enumerate()
        .map(|(account, balance)| (account as u64, *balance))
        .collect();
    if measured_balances != final_balances {
        return Err("foreground balance model differs from published memory balances".to_owned());
    }
    if measured.counts.operation_counts() != workload.operation_counts
        || measured.counts.applied_refunds != workload.operation_counts.refunds
    {
        return Err(format!(
            "unexpected operation totals for {}: credits={} debits={} refunds={} queries={} applied_refunds={}",
            workload.kind.name(),
            measured.counts.credits,
            measured.counts.debits,
            measured.counts.refunds,
            measured.counts.queries,
            measured.counts.applied_refunds
        ));
    }
    if final_balances.len() != workload.users
        || final_balances
            .iter()
            .any(|(_, balance)| *balance != workload.expected_final_balance)
    {
        return Err(format!(
            "pre-reopen balance verification failed; expected {} for workload {}",
            workload.expected_final_balance,
            workload.kind.name()
        ));
    }
    if mode == BalanceMode::Checkpoint
        && (metrics.checkpoint_count != workload.transactions / config.checkpoint_quantity
            || metrics.checkpoint_snapshots_enqueued
                != workload.transactions / config.checkpoint_quantity)
    {
        return Err(format!(
            "checkpoint completed/enqueued counts {}/{} differ from expected {}",
            metrics.checkpoint_count,
            metrics.checkpoint_snapshots_enqueued,
            workload.transactions / config.checkpoint_quantity
        ));
    }

    let recovery_started = Instant::now();
    let recovered = AccountStore::open(
        db_dir.path(),
        workload.users,
        mode,
        config.checkpoint_quantity,
    )
    .await?;
    let recovery_s = recovery_started.elapsed().as_secs_f64();
    let recovered_balances = recovered.all_balances();
    if recovered.latest_seq() != latest_seq || recovered_balances != final_balances {
        let _ = recovered.shutdown().await;
        return Err("close/reopen recovery did not reproduce sequence and all balances".to_owned());
    }
    let integrity_started = Instant::now();
    let integrity_result = recovered.validate_integrity().await;
    let integrity_s = integrity_started.elapsed().as_secs_f64();
    let recovered_shutdown = recovered.shutdown().await;
    integrity_result?;
    recovered_shutdown?;
    let db_bytes = directory_bytes(db_dir.path())?;
    let tx_latency = summarize(
        metrics.transaction_latency_ns,
        metrics.transactions,
        &metrics.transaction_samples_ns,
    );
    let query_latency = (workload.operation_counts.queries > 0).then(|| {
        summarize(
            metrics.balance_latency_ns,
            metrics.balance_queries,
            &metrics.balance_samples_ns,
        )
    });
    let settled_request_rps = workload.requests as f64 / measured.settled_wall.as_secs_f64();
    let settled_transaction_rps =
        workload.transactions as f64 / measured.settled_wall.as_secs_f64();
    let foreground_wall_s = measured.foreground_wall.as_secs_f64();
    let drain_wall_s = measured.drain_wall.as_secs_f64();
    let settled_wall_s = measured.settled_wall.as_secs_f64();
    Ok(CaseResult {
        label,
        workload: workload.kind,
        batch_size,
        mode,
        repetition,
        users: workload.users,
        waves: workload.waves.len(),
        plan_generation_s: workload.generation_ns as f64 / 1e9,
        requests: workload.requests,
        transaction_requests: workload.transactions,
        committed_transactions: latest_seq,
        operation_counts: measured.counts.operation_counts(),
        completed_rps: workload.requests as f64 / foreground_wall_s,
        transaction_rps: workload.transactions as f64 / foreground_wall_s,
        settled_request_rps,
        settled_transaction_rps,
        foreground_wall_s,
        drain_wall_s,
        settled_wall_s,
        foreground_cpu_s: measured.foreground_cpu_seconds,
        foreground_cores: measured.foreground_cpu_seconds / foreground_wall_s,
        drain_cpu_s: measured.drain_cpu_seconds,
        settled_cpu_s: measured.settled_cpu_seconds,
        settled_cores: measured.settled_cpu_seconds / settled_wall_s,
        foreground_io: measured.foreground_io,
        settled_io: measured.settled_io,
        tx_latency,
        query_latency,
        batch_count: metrics.batches,
        read_build_mean_ns: mean_phase(metrics.read_build_ns, metrics.batches),
        wal_sync_mean_ns: mean_phase(metrics.wal_sync_ns, metrics.batches),
        publish_mean_ns: mean_phase(metrics.publish_ns, metrics.batches),
        checkpoint_count: metrics.checkpoint_count,
        checkpoint_snapshot_ns: metrics.checkpoint_snapshot_ns,
        checkpoint_queue_wait_ns: metrics.checkpoint_queue_wait_ns,
        checkpoint_chunk_sync_ns: metrics.checkpoint_chunk_sync_ns,
        checkpoint_manifest_sync_ns: metrics.checkpoint_manifest_sync_ns,
        checkpoint_duration_ns: metrics.checkpoint_duration_ns,
        checkpoint_lag: if mode == BalanceMode::Checkpoint {
            latest_seq.saturating_sub(metrics.checkpoint_latest_seq)
        } else {
            0
        },
        checkpoint_snapshots_enqueued: metrics.checkpoint_snapshots_enqueued,
        rocks: measured.rocks,
        recovery_s,
        integrity_s,
        db_bytes,
        preflight: measured.preflight,
    })
}

async fn run_measured_window(
    config: &Config,
    workload: &Workload,
    batch_size: usize,
    store: &AccountStore,
    db_path: &Path,
) -> Result<WindowMetrics, String> {
    let db_estimate = workload
        .requests
        .checked_mul(192)
        .ok_or_else(|| "workload DB space estimate overflow".to_owned())?;
    let min_free_bytes = config
        .preflight_free_reserve_bytes
        .checked_add(db_estimate)
        .ok_or_else(|| "minimum free space estimate overflow".to_owned())?;
    let preflight_config = PreflightConfig {
        min_free_bytes,
        ..config.preflight.clone()
    };
    let preflight = ledger_preflight::ensure_idle(db_path, &preflight_config)
        .map_err(|error| format!("strict idle preflight failed: {error}"))?;
    let io_before = ledger_preflight::sample_io(db_path)?;
    let rocks_before = store.rocksdb_stats();
    let settled_cpu_start = ProcessTime::now();
    let foreground_cpu_start = ProcessTime::now();
    let foreground_started = Instant::now();
    let mut expected_balances = vec![0_u64; workload.users];
    let mut counts = Counts::default();
    let mut expected_seq = 0_u64;

    for (wave_index, wave) in workload.waves.iter().enumerate() {
        let mut transactions = Vec::with_capacity(wave.transaction_accounts.len());
        let mut metadata = Vec::with_capacity(wave.transaction_accounts.len());
        let cycle = wave_index as u64 / 10;
        let per_user_transactions =
            workload.transactions_per_cycle * (workload.waves.len() as u64 / 10);
        for account32 in &wave.transaction_accounts {
            let account = *account32 as usize;
            let entry = workload.patterns[account][wave_index % 10];
            let operation = match entry.kind {
                PatternKind::Credit => Operation::Credit,
                PatternKind::Debit => Operation::Debit,
                PatternKind::Refund => Operation::Refund,
                PatternKind::Balance => {
                    return Err("transaction list contains a balance entry".to_owned())
                }
            };
            let tx_id = account as u64 * per_user_transactions
                + cycle * workload.transactions_per_cycle
                + u64::from(entry.transaction_offset)
                + 1;
            let refund_of = if entry.kind == PatternKind::Refund {
                Some(TransactionKey {
                    account_id: account as u64,
                    tx_id: account as u64 * per_user_transactions
                        + cycle * workload.transactions_per_cycle
                        + u64::from(entry.refund_target_offset)
                        + 1,
                    transaction_at: transaction_time(
                        workload,
                        account,
                        refund_request_wave(
                            workload,
                            account,
                            wave_index,
                            entry.refund_target_offset,
                        ),
                    ),
                })
            } else {
                None
            };
            let transaction_at = transaction_time(workload, account, wave_index);
            transactions.push(Transaction {
                key: TransactionKey {
                    account_id: account as u64,
                    tx_id,
                    transaction_at,
                },
                operation,
                amount: 1,
                refund_of,
            });
            metadata.push((account, operation));
        }
        let mut transaction_iter = transactions.into_iter();
        let mut metadata_iter = metadata.into_iter();
        loop {
            let batch = transaction_iter
                .by_ref()
                .take(batch_size)
                .collect::<Vec<_>>();
            if batch.is_empty() {
                break;
            }
            let batch_metadata = metadata_iter.by_ref().take(batch_size).collect::<Vec<_>>();
            let replies = store.handle_batch(batch).await?;
            if replies.len() != batch_metadata.len() {
                return Err("handler returned the wrong reply count".to_owned());
            }
            for ((account, operation), reply) in batch_metadata.into_iter().zip(replies) {
                expected_seq += 1;
                let Reply::Transaction {
                    status: TransactionStatus::Applied,
                    balance,
                    seq,
                    replayed: false,
                } = reply
                else {
                    return Err(format!("normal workload transaction returned {reply:?}"));
                };
                if seq != expected_seq {
                    return Err(format!("ledger seq {seq} arrived, expected {expected_seq}"));
                }
                match operation {
                    Operation::Credit => {
                        expected_balances[account] = expected_balances[account]
                            .checked_add(1)
                            .ok_or_else(|| "expected credit balance overflow".to_owned())?;
                        counts.credits += 1;
                    }
                    Operation::Debit => {
                        expected_balances[account] = expected_balances[account]
                            .checked_sub(1)
                            .ok_or_else(|| "workload generated an overdraft".to_owned())?;
                        counts.debits += 1;
                    }
                    Operation::Refund => {
                        expected_balances[account] += 1;
                        counts.refunds += 1;
                        counts.applied_refunds += 1;
                    }
                }
                if balance != expected_balances[account] {
                    return Err(format!(
                        "account {account} reply balance {balance}, expected {}",
                        expected_balances[account]
                    ));
                }
            }
        }
        for account32 in &wave.balance_accounts {
            let account = *account32 as usize;
            let request_id = account as u64 * workload.waves.len() as u64 + wave_index as u64;
            let balance = store.balance_for_request(account as u64, request_id as u64)?;
            if balance != expected_balances[account] {
                return Err(format!(
                    "balance query for account {account} returned {balance}, expected {}",
                    expected_balances[account]
                ));
            }
            counts.queries += 1;
        }
    }
    counts.last_seq = expected_seq;
    let foreground_wall = foreground_started.elapsed();
    let foreground_cpu_seconds = foreground_cpu_start.elapsed().as_secs_f64();
    let io_after_foreground = ledger_preflight::sample_io(db_path)?;
    let drain_cpu_start = ProcessTime::now();
    let drain_started = Instant::now();
    store.drain_checkpoints().await?;
    let drain_wall = drain_started.elapsed();
    let drain_cpu_seconds = drain_cpu_start.elapsed().as_secs_f64();
    let settled_cpu_seconds = settled_cpu_start.elapsed().as_secs_f64();
    let settled_wall = foreground_started.elapsed();
    let io_after_settle = ledger_preflight::sample_io(db_path)?;
    let rocks_after = store.rocksdb_stats();
    let settled_io = io_delta(&io_before, &io_after_settle)?;
    let foreground_io = io_delta(&io_before, &io_after_foreground)?;
    Ok(WindowMetrics {
        foreground_wall,
        drain_wall,
        settled_wall,
        foreground_cpu_seconds,
        drain_cpu_seconds,
        settled_cpu_seconds,
        foreground_io,
        settled_io,
        rocks: stats_delta(rocks_after, rocks_before),
        counts,
        balances: expected_balances,
        latest_seq: store.latest_seq(),
        preflight,
    })
}

fn transaction_time(workload: &Workload, account: usize, request_wave: usize) -> u64 {
    1_700_000_000_000_000_u64 + account as u64 * workload.waves.len() as u64 + request_wave as u64
}

fn refund_request_wave(
    workload: &Workload,
    account: usize,
    current_wave: usize,
    target_offset: u8,
) -> usize {
    let pattern = workload.patterns[account];
    let cycle = current_wave / 10;
    let target_slot = pattern
        .iter()
        .position(|entry| {
            entry.kind == PatternKind::Debit && entry.transaction_offset == target_offset
        })
        .expect("refund target offset refers to a debit slot");
    cycle * 10 + target_slot
}

fn parse_args<I>(args: I) -> Result<Config, String>
where
    I: IntoIterator<Item = String>,
{
    let mut config = Config {
        workload: WorkloadKind::Baseline40_40_10_10,
        users: DEFAULT_USERS,
        waves: DEFAULT_WAVES,
        batch_sizes: DEFAULT_BATCH_SIZES.to_vec(),
        modes: vec![BalanceMode::PerBatch, BalanceMode::Checkpoint],
        repetitions: 1,
        checkpoint_quantity: DEFAULT_CHECKPOINT_QUANTITY,
        output_root: PathBuf::from(DEFAULT_DB_ROOT),
        preflight: PreflightConfig {
            observation: Duration::from_secs(3),
            timeout: Duration::from_secs(60),
            max_cpu_busy_pct: 10.0,
            max_disk_busy_pct: 5.0,
            min_available_mem_bytes: 128 * 1024 * 1024,
            min_free_bytes: 1024 * 1024 * 1024,
        },
        preflight_free_reserve_bytes: 1024 * 1024 * 1024,
    };
    let mut requested_iterations = None;
    let mut args = args.into_iter();
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            "--bench" => continue,
            _ => {}
        }
        let value = args
            .next()
            .ok_or_else(|| format!("missing value after {argument}"))?;
        match argument.as_str() {
            "--workload" => config.workload = WorkloadKind::parse(&value)?,
            "--users" => config.users = positive_usize(&value, "users")?,
            "--waves" => config.waves = positive_usize(&value, "waves")?,
            "--iterations" => requested_iterations = Some(positive_u64(&value, "iterations")?),
            "--batch-sizes" => {
                config.batch_sizes = parse_positive_list::<usize>(&value, "batch-sizes")?
            }
            "--modes" => {
                config.modes = value
                    .split(',')
                    .map(|mode| match mode {
                        "per-batch" => Ok(BalanceMode::PerBatch),
                        "checkpoint" => Ok(BalanceMode::Checkpoint),
                        _ => Err(format!("unknown balance mode {mode}")),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                if config.modes.is_empty() {
                    return Err("modes must not be empty".to_owned());
                }
            }
            "--repetitions" => config.repetitions = positive_usize(&value, "repetitions")?,
            "--checkpoint-quantity" => {
                config.checkpoint_quantity = positive_u64(&value, "checkpoint-quantity")?
            }
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
    if let Some(iterations) = requested_iterations {
        if iterations % config.users as u64 != 0 {
            return Err("iterations must be divisible by users".to_owned());
        }
        config.waves = usize::try_from(iterations / config.users as u64)
            .map_err(|_| "waves do not fit usize".to_owned())?;
    }
    if config.waves == 0 || config.waves % 10 != 0 {
        return Err("waves (or iterations/users) must be a positive multiple of 10".to_owned());
    }
    if config.batch_sizes.is_empty() || config.batch_sizes.contains(&0) {
        return Err("batch sizes must be positive".to_owned());
    }
    Ok(config)
}

fn print_help() {
    println!(
        "ledger_account_store_tokio options:\n\
         --workload baseline|credit-debit-50-50 (default baseline 40/40/10/10)\n\
         --users N --waves N (default 50,000 users x 200 waves)\n\
         --iterations N (optional; must divide evenly by users, waves must be divisible by 10)\n\
         --batch-sizes N[,N...] (default 2048,4096)\n\
         --modes per-batch,checkpoint (default both) --repetitions N (default 1)\n\
         --checkpoint-quantity N (default 100000; time trigger is disabled)\n\
         --output-dir PATH (default target/ledger-account-store-trials)\n\
         --preflight-observation-ms N --preflight-timeout-ms N\n\
         --preflight-max-cpu-pct P --preflight-max-disk-busy-pct P\n\
         --preflight-min-mem-bytes N --preflight-free-reserve-bytes N"
    );
}

fn positive_usize(value: &str, label: &str) -> Result<usize, String> {
    let value = value
        .parse::<usize>()
        .map_err(|error| format!("invalid {label}: {error}"))?;
    if value == 0 {
        return Err(format!("{label} must be positive"));
    }
    Ok(value)
}

fn positive_u64(value: &str, label: &str) -> Result<u64, String> {
    let value = value
        .parse::<u64>()
        .map_err(|error| format!("invalid {label}: {error}"))?;
    if value == 0 {
        return Err(format!("{label} must be positive"));
    }
    Ok(value)
}

fn parse_pct(value: &str, label: &str) -> Result<f64, String> {
    let value = value
        .parse::<f64>()
        .map_err(|error| format!("invalid {label}: {error}"))?;
    if !value.is_finite() || !(0.0..=100.0).contains(&value) {
        return Err(format!("{label} must be finite and in 0..=100"));
    }
    Ok(value)
}

fn parse_positive_list<T>(value: &str, label: &str) -> Result<Vec<T>, String>
where
    T: std::str::FromStr + PartialEq + Default,
    T::Err: Error,
{
    let values = value
        .split(',')
        .map(|value| {
            value
                .parse::<T>()
                .map_err(|error| format!("invalid {label}: {error}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if values.is_empty() || values.iter().any(|value| value == &T::default()) {
        return Err(format!("{label} values must be positive"));
    }
    Ok(values)
}

fn mode_name(mode: BalanceMode) -> &'static str {
    match mode {
        BalanceMode::PerBatch => "per-batch",
        BalanceMode::Checkpoint => "checkpoint",
    }
}

fn io_delta(before: &IoSample, after: &IoSample) -> Result<IoDelta, String> {
    if before.target_major_minor != after.target_major_minor {
        return Err("target block device changed during measured window".to_owned());
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

fn stats_delta(
    now: (u64, u64, u64, u64, u64, u64, u64),
    before: (u64, u64, u64, u64, u64, u64, u64),
) -> (u64, u64, u64, u64, u64, u64, u64) {
    (
        now.0.saturating_sub(before.0),
        now.1.saturating_sub(before.1),
        now.2.saturating_sub(before.2),
        now.3.saturating_sub(before.3),
        now.4.saturating_sub(before.4),
        now.5.saturating_sub(before.5),
        now.6.saturating_sub(before.6),
    )
}

fn summarize(sum_ns: u64, count: u64, values: &[u64]) -> LatencySummary {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    LatencySummary {
        mean_ns: if count == 0 {
            0.0
        } else {
            sum_ns as f64 / count as f64
        },
        p50_ns: percentile(&sorted, 0.50),
        p95_ns: percentile(&sorted, 0.95),
        p99_ns: percentile(&sorted, 0.99),
        sample_count: sorted.len(),
    }
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let index = ((sorted.len() as f64 * p).ceil() as usize).saturating_sub(1);
    sorted[index.min(sorted.len() - 1)]
}

fn mean_phase(total_ns: u64, count: u64) -> f64 {
    if count == 0 {
        0.0
    } else {
        total_ns as f64 / count as f64
    }
}

fn directory_bytes(path: &Path) -> Result<u64, String> {
    let mut total = 0_u64;
    let entries = fs::read_dir(path)
        .map_err(|error| format!("cannot list DB directory {}: {error}", path.display()))?;
    for entry in entries {
        let entry = entry.map_err(|error| format!("cannot inspect DB entry: {error}"))?;
        let metadata = entry.metadata().map_err(|error| {
            format!("cannot inspect DB path {}: {error}", entry.path().display())
        })?;
        if metadata.is_dir() {
            total = total
                .checked_add(directory_bytes(&entry.path())?)
                .ok_or_else(|| "DB size overflow".to_owned())?;
        } else {
            total = total
                .checked_add(metadata.len())
                .ok_or_else(|| "DB size overflow".to_owned())?;
        }
    }
    Ok(total)
}

fn csv_header() -> &'static str {
    "case,workload,batch_size,balance_mode,repetition,users,waves,total_requests,transaction_requests,committed_transactions,credits,debits,refunds,successful_refunds,balance_queries,foreground_wall_s,foreground_all_request_rps,foreground_transaction_rps,checkpoint_drain_wall_s,checkpoint_drain_cpu_s,settled_wall_s,settled_all_request_rps,settled_transaction_rps,foreground_cpu_s,foreground_cpu_cores,settled_cpu_s,settled_cpu_cores,foreground_proc_rchar_bytes,foreground_proc_wchar_bytes,foreground_proc_read_bytes,foreground_proc_write_bytes,foreground_target_read_bytes,foreground_target_write_bytes,foreground_target_busy_ms,settled_proc_rchar_bytes,settled_proc_wchar_bytes,settled_proc_read_bytes,settled_proc_write_bytes,settled_target_read_bytes,settled_target_write_bytes,settled_target_busy_ms,target_device,target_major_minor,tx_handler_mean_ns,tx_handler_p50_ns,tx_handler_p95_ns,tx_handler_p99_ns,tx_handler_sample_count,query_handler_latency_status,query_handler_mean_ns,query_handler_p50_ns,query_handler_p95_ns,query_handler_p99_ns,query_handler_sample_count,batch_count,batch_read_build_mean_ns,batch_wal_sync_mean_ns,memory_publish_mean_ns,checkpoint_count,checkpoint_snapshots_enqueued,checkpoint_snapshot_ns,checkpoint_queue_wait_ns,checkpoint_chunk_sync_ns,checkpoint_manifest_sync_ns,checkpoint_duration_ns,checkpoint_lag,rocks_wal_sync_count,rocks_wal_bytes,rocks_write_with_wal,rocks_flush_write_bytes,rocks_compaction_read_bytes,rocks_compaction_write_bytes,rocks_stall_micros,recovery_s,integrity_scan_s,db_bytes,plan_generation_s,preflight_cpu_busy_pct,preflight_disk_busy_pct,preflight_mem_available_bytes,preflight_free_bytes"
}

fn write_case_csv(writer: &mut impl Write, result: &CaseResult) -> std::io::Result<()> {
    let f = |value: f64| format!("{value:.9}");
    let rocks = result.rocks;
    let (query_status, query_mean, query_p50, query_p95, query_p99, query_samples) =
        match &result.query_latency {
            Some(latency) => (
                "measured",
                f(latency.mean_ns),
                latency.p50_ns.to_string(),
                latency.p95_ns.to_string(),
                latency.p99_ns.to_string(),
                latency.sample_count.to_string(),
            ),
            None => (
                "not_applicable",
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                "0".to_owned(),
            ),
        };
    let row = vec![
        result.label.clone(),
        result.workload.name().to_owned(),
        result.batch_size.to_string(),
        mode_name(result.mode).to_owned(),
        result.repetition.to_string(),
        result.users.to_string(),
        result.waves.to_string(),
        result.requests.to_string(),
        result.transaction_requests.to_string(),
        result.committed_transactions.to_string(),
        result.operation_counts.credits.to_string(),
        result.operation_counts.debits.to_string(),
        result.operation_counts.refunds.to_string(),
        result.operation_counts.refunds.to_string(),
        result.operation_counts.queries.to_string(),
        f(result.foreground_wall_s),
        f(result.completed_rps),
        f(result.transaction_rps),
        f(result.drain_wall_s),
        f(result.drain_cpu_s),
        f(result.settled_wall_s),
        f(result.settled_request_rps),
        f(result.settled_transaction_rps),
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
        result.settled_io.target_read_bytes.to_string(),
        result.settled_io.target_write_bytes.to_string(),
        result.settled_io.target_busy_ms.to_string(),
        result.settled_io.target_device.clone(),
        result.settled_io.target_major_minor.clone(),
        f(result.tx_latency.mean_ns),
        result.tx_latency.p50_ns.to_string(),
        result.tx_latency.p95_ns.to_string(),
        result.tx_latency.p99_ns.to_string(),
        result.tx_latency.sample_count.to_string(),
        query_status.to_owned(),
        query_mean,
        query_p50,
        query_p95,
        query_p99,
        query_samples,
        result.batch_count.to_string(),
        f(result.read_build_mean_ns),
        f(result.wal_sync_mean_ns),
        f(result.publish_mean_ns),
        result.checkpoint_count.to_string(),
        result.checkpoint_snapshots_enqueued.to_string(),
        result.checkpoint_snapshot_ns.to_string(),
        result.checkpoint_queue_wait_ns.to_string(),
        result.checkpoint_chunk_sync_ns.to_string(),
        result.checkpoint_manifest_sync_ns.to_string(),
        result.checkpoint_duration_ns.to_string(),
        result.checkpoint_lag.to_string(),
        rocks.0.to_string(),
        rocks.1.to_string(),
        rocks.2.to_string(),
        rocks.3.to_string(),
        rocks.4.to_string(),
        rocks.5.to_string(),
        rocks.6.to_string(),
        f(result.recovery_s),
        f(result.integrity_s),
        result.db_bytes.to_string(),
        f(result.plan_generation_s),
        f(result.preflight.cpu_busy_pct),
        f(result.preflight.disk_busy_pct),
        result.preflight.mem_available_bytes.to_string(),
        result.preflight.free_bytes.to_string(),
    ];
    writeln!(writer, "{}", row.join(","))
}

fn result_to_log(result: &CaseResult) -> String {
    format!(
        "mode={} batch={} requests={} transactions={} fg_wall_s={:.6} all_rps={:.3} tx_rps={:.3} settled_wall_s={:.6} settled_rps={:.3} fg_cpu_s={:.6} settled_cpu_s={:.6} checkpoints={} recovery_s={:.6} integrity_s={:.6} db_bytes={}",
        mode_name(result.mode),
        result.batch_size,
        result.requests,
        result.committed_transactions,
        result.foreground_wall_s,
        result.completed_rps,
        result.transaction_rps,
        result.settled_wall_s,
        result.settled_request_rps,
        result.foreground_cpu_s,
        result.settled_cpu_s,
        result.checkpoint_count,
        result.recovery_s,
        result.integrity_s,
        result.db_bytes
    )
}

fn io_error(context: &'static str) -> impl FnOnce(std::io::Error) -> String {
    move |error| format!("{context} failed: {error}")
}
