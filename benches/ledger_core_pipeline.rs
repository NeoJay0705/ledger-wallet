#[path = "support/ledger_preflight.rs"]
mod ledger_preflight;

use cpu_time::ProcessTime;
use ledger_preflight::{PreflightConfig, PreflightReport, ensure_idle, sample_io};
use ledger_wallet::{
    DatabaseLayout, HistoryStore, MockHistory, Pipeline, PipelineConfig, TransactionStatus,
};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::runtime::Builder;
use tokio::task::JoinSet;

const DEFAULT_USERS: usize = 50_000;
const DEFAULT_REQUESTS_PER_USER: usize = 200;
const WORKERS: usize = 4;
const BLOCKING_THREADS: usize = 32;
const DEFAULT_OUTPUT_ROOT: &str = "target/ledger-core-pipeline-trials";
const CANONICAL_TRANSACTIONS: u128 = 10_000_000;
const CANONICAL_MIN_MEMORY_BYTES: u128 = 8 * 1024 * 1024 * 1024;
const CANONICAL_MIN_FREE_BYTES: u128 = 16 * 1024 * 1024 * 1024;
const FLOOR_MIN_MEMORY_BYTES: u64 = 256 * 1024 * 1024;
const FLOOR_MIN_FREE_BYTES: u64 = 512 * 1024 * 1024;

struct OwnedScratchDirectory(PathBuf);

impl OwnedScratchDirectory {
    fn create(root: &Path) -> Result<Self, String> {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| format!("system clock is before Unix epoch: {error}"))?
            .as_nanos();
        let path = root.join(format!("run-{}-{nonce:x}", std::process::id()));
        fs::create_dir(&path).map_err(|error| {
            format!(
                "cannot create owned scratch directory {}: {error}",
                path.display()
            )
        })?;
        Ok(Self(path))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for OwnedScratchDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct Config {
    users: usize,
    requests_per_user: usize,
    shards: u32,
    layout: DatabaseLayout,
    output_root: PathBuf,
    preflight: PreflightConfig,
    smoke_relaxed: bool,
}

fn main() {
    let config = match parse_args(std::env::args().skip(1)) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("ledger_core_pipeline: {error}");
            std::process::exit(2);
        }
    };
    let runtime = Builder::new_multi_thread()
        .worker_threads(WORKERS)
        .max_blocking_threads(BLOCKING_THREADS)
        .enable_all()
        .build()
        .unwrap_or_else(|error| {
            eprintln!("cannot create Tokio runtime: {error}");
            std::process::exit(2);
        });
    if let Err(error) = runtime.block_on(run(config)) {
        eprintln!("ledger_core_pipeline failed: {error}");
        std::process::exit(2);
    }
}

async fn run(config: Config) -> Result<(), String> {
    if config.users == 0 || config.requests_per_user == 0 {
        return Err("users and requests-per-user must be positive".to_owned());
    }
    if config.requests_per_user % 2 != 0 {
        return Err("requests-per-user must be even for a 50/50 credit/debit workload".to_owned());
    }
    fs::create_dir_all(&config.output_root).map_err(|error| {
        format!(
            "cannot create benchmark output root {}: {error}",
            config.output_root.display()
        )
    })?;

    print_source_evidence()?;
    println!(
        "benchmark_mode smoke_relaxed={} run_scope={}",
        config.smoke_relaxed,
        if config.smoke_relaxed {
            "correctness-and-wiring-only"
        } else {
            "configured-throughput-workload"
        },
    );

    // Preflight is outside both the Pipeline runtime and the measured workload.
    let preflight = ensure_idle(&config.output_root, &config.preflight)?;
    print_preflight(&preflight, &config.preflight);
    let scratch = OwnedScratchDirectory::create(&config.output_root)?;
    let mut pipeline_config = PipelineConfig::new(scratch.path().join("data"));
    pipeline_config.shard_count = config.shards;
    pipeline_config.database_layout = config.layout;
    let histories: Vec<Arc<dyn HistoryStore>> = (0..config.shards)
        .map(|shard| {
            Arc::new(MockHistory::new(format!("bench-volatile-shard-{shard}")))
                as Arc<dyn HistoryStore>
        })
        .collect();
    let io_before = sample_io(&config.output_root)?;
    let pipeline = Pipeline::open(pipeline_config, histories)
        .await
        .map_err(|error| format!("pipeline startup failed: {error}"))?;

    let process_cpu_start = ProcessTime::now();
    let wall_start = Instant::now();
    let workload = run_workload(&pipeline, config.users, config.requests_per_user).await;
    let foreground_wall = wall_start.elapsed();
    let foreground_cpu = process_cpu_start.elapsed().as_secs_f64();
    let io_after = match sample_io(&config.output_root) {
        Ok(sample) => sample,
        Err(error) => {
            let shutdown = pipeline.shutdown().await;
            let extra = shutdown
                .err()
                .map(|shutdown_error| format!("; shutdown also failed: {shutdown_error}"));
            return Err(format!(
                "cannot sample I/O counters after workload: {error}{}",
                extra.unwrap_or_default()
            ));
        }
    };

    if let Err(error) = workload {
        let shutdown = pipeline.shutdown().await;
        let extra = shutdown
            .err()
            .map(|error| format!("; shutdown also failed: {error}"));
        return Err(format!(
            "workload failed: {error}{}",
            extra.unwrap_or_default()
        ));
    }
    let shutdown_start = Instant::now();
    pipeline
        .shutdown()
        .await
        .map_err(|error| format!("pipeline shutdown failed: {error}"))?;
    let shutdown_wall = shutdown_start.elapsed();
    let status = pipeline
        .status()
        .await
        .map_err(|error| format!("cannot read final status: {error}"))?;

    let total = config.users as u64 * config.requests_per_user as u64;
    let wall_seconds = foreground_wall.as_secs_f64();
    let cores = if wall_seconds > 0.0 {
        foreground_cpu / wall_seconds
    } else {
        0.0
    };
    let process_read_bytes = io_after
        .process_read_bytes
        .saturating_sub(io_before.process_read_bytes);
    let process_write_bytes = io_after
        .process_write_bytes
        .saturating_sub(io_before.process_write_bytes);
    let device_read_bytes = io_after
        .target_read_bytes
        .saturating_sub(io_before.target_read_bytes);
    let device_write_bytes = io_after
        .target_write_bytes
        .saturating_sub(io_before.target_write_bytes);
    println!(
        "workload users={} requests_per_user={} transactions={} credits={} debits={} shards={} layout={} workers={} blocking_threads={}",
        config.users,
        config.requests_per_user,
        total,
        total / 2,
        total / 2,
        config.shards,
        config.layout.as_str(),
        WORKERS,
        BLOCKING_THREADS,
    );
    println!(
        "foreground wall_seconds={:.3} transactions_per_second={:.1} process_cpu_seconds={:.3} cpu_equivalent_cores={:.3} shutdown_drain_seconds={:.3}",
        wall_seconds,
        if wall_seconds > 0.0 {
            total as f64 / wall_seconds
        } else {
            0.0
        },
        foreground_cpu,
        cores,
        shutdown_wall.as_secs_f64(),
    );
    println!(
        "process_io read_bytes={} write_bytes={} target_device={} target_read_bytes={} target_write_bytes={} scope=process-wide counters include Tokio and RocksDB; device counters can include unrelated filesystem activity",
        process_read_bytes,
        process_write_bytes,
        io_after.target_device,
        device_read_bytes,
        device_write_bytes,
    );
    println!(
        "service status lifecycle={} service_identity={} source_identities={:?} final_shard_sequences={:?} history_mode=volatile projector_sequence_is_process_local=true",
        status.lifecycle.as_str(),
        status.service_identity,
        status
            .shards
            .iter()
            .map(|shard| shard.source_identity.clone())
            .collect::<Vec<_>>(),
        status
            .shards
            .iter()
            .map(|shard| shard.source_sequence)
            .collect::<Vec<_>>(),
    );
    println!(
        "metrics quantiles use a deterministic uniform reservoir capped at 4096 observations per stage; nearest-rank p50/p95/p99 describe that sample, while counts and maxima cover all observations; stage distributions have different observation scopes and overlap, so do not sum them"
    );
    for stage in status.metrics.stages.iter().filter(|stage| stage.count > 0) {
        println!(
            "stage={} count={} sample={} average_ns={} p50_ns={} p95_ns={} p99_ns={} max_ns={}",
            stage.stage,
            stage.count,
            stage.sample_count,
            stage.average_ns,
            display_opt(stage.p50_ns),
            display_opt(stage.p95_ns),
            display_opt(stage.p99_ns),
            display_opt(stage.max_ns),
        );
    }
    println!("preflight_ran_before_pipeline_open_and_workload=true");
    Ok(())
}

fn print_source_evidence() -> Result<(), String> {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let head = command_output(
        "git",
        &["-C", repo_root.to_str().unwrap_or("."), "rev-parse", "HEAD"],
    )?;
    let status = command_output(
        "git",
        &[
            "-C",
            repo_root.to_str().unwrap_or("."),
            "status",
            "--porcelain",
            "--untracked-files=all",
        ],
    )?;
    println!(
        "source_revision git_head={} dirty={} worktree_status={:?}",
        head,
        !status.is_empty(),
        status,
    );
    for relative in [
        "src/pipeline.rs",
        "src/config.rs",
        "src/history.rs",
        "src/admission.rs",
        "src/metrics.rs",
        "src/error.rs",
        "src/lib.rs",
        "src/batch_queue.rs",
        "src/storage/mod.rs",
        "src/storage/account_store.rs",
        "src/main.rs",
        "benches/ledger_core_pipeline.rs",
        "benches/support/ledger_preflight.rs",
        "Cargo.toml",
        "Cargo.lock",
    ] {
        let path = repo_root.join(relative);
        let digest = command_output("sha256sum", &[path.to_str().unwrap_or(relative)])?;
        let hash = digest.split_whitespace().next().unwrap_or("missing");
        println!("source_sha256 path={relative} sha256={hash}");
    }
    Ok(())
}

fn command_output(program: &str, args: &[&str]) -> Result<String, String> {
    let output = Command::new(program)
        .args(args)
        .output()
        .map_err(|error| format!("cannot run {program}: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "{program} failed with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

async fn run_workload(
    pipeline: &Pipeline,
    users: usize,
    requests_per_user: usize,
) -> Result<(), String> {
    let mut tasks = JoinSet::new();
    for account_id in 0..users as u64 {
        let service = pipeline.clone();
        tasks.spawn(async move {
            for request_index in 0..requests_per_user {
                let transaction_id = request_index as u64 + 1;
                let transaction_at = 1_800_000_000_000_000_u64
                    .saturating_add(account_id.saturating_mul(requests_per_user as u64))
                    .saturating_add(request_index as u64);
                let result = if request_index % 2 == 0 {
                    service
                        .credit(account_id, transaction_id, transaction_at, 1)
                        .await
                } else {
                    service
                        .debit(account_id, transaction_id, transaction_at, 1)
                        .await
                };
                let receipt = result.map_err(|error| error.to_string())?;
                if receipt.status != TransactionStatus::Applied {
                    return Err(format!(
                        "unexpected {} receipt for account {account_id}, request {request_index}",
                        status_name(receipt.status)
                    ));
                }
            }
            Ok::<(), String>(())
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.map_err(|error| format!("user workload task failed: {error}"))??;
    }
    Ok(())
}

fn status_name(status: TransactionStatus) -> &'static str {
    match status {
        TransactionStatus::Applied => "applied",
        TransactionStatus::InsufficientFunds => "insufficient_funds",
        TransactionStatus::CreditOverflow => "credit_overflow",
        TransactionStatus::InvalidAmount => "invalid_amount",
        TransactionStatus::InvalidRefund => "invalid_refund",
        TransactionStatus::RefundAlreadyUsed => "refund_already_used",
    }
}

fn display_opt(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "n/a".to_owned())
}

fn print_preflight(report: &PreflightReport, config: &PreflightConfig) {
    println!(
        "preflight path={} mount={} filesystem={} device={} major_minor={} attempts={} observation_ms={} cpu_busy_pct={:.1} disk_busy_pct={:.1} max_cpu_busy_pct={:.1} max_disk_busy_pct={:.1} available_memory_bytes={} min_available_memory_bytes={} free_bytes={} min_free_bytes={}",
        report.path.display(),
        report.mount_point.display(),
        report.filesystem,
        report.device,
        report.major_minor,
        report.attempts,
        report.observation.as_millis(),
        report.cpu_busy_pct,
        report.disk_busy_pct,
        config.max_cpu_busy_pct,
        config.max_disk_busy_pct,
        report.mem_available_bytes,
        config.min_available_mem_bytes,
        report.free_bytes,
        config.min_free_bytes,
    );
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<Config, String> {
    let mut config = Config {
        users: DEFAULT_USERS,
        requests_per_user: DEFAULT_REQUESTS_PER_USER,
        shards: 1,
        layout: DatabaseLayout::Shared,
        output_root: PathBuf::from(DEFAULT_OUTPUT_ROOT),
        preflight: PreflightConfig {
            observation: Duration::from_secs(1),
            timeout: Duration::from_secs(60),
            max_cpu_busy_pct: 10.0,
            max_disk_busy_pct: 5.0,
            min_available_mem_bytes: 0,
            min_free_bytes: 0,
        },
        smoke_relaxed: false,
    };
    let mut memory_threshold_set = false;
    let mut free_threshold_set = false;
    let mut args = args.into_iter();
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            "--smoke" => {
                config.smoke_relaxed = true;
                config.users = 100;
                config.requests_per_user = 20;
                config.preflight.observation = Duration::from_millis(200);
                config.preflight.timeout = Duration::from_secs(10);
                config.preflight.max_cpu_busy_pct = 100.0;
                config.preflight.max_disk_busy_pct = 100.0;
                config.preflight.min_available_mem_bytes = 0;
                config.preflight.min_free_bytes = 0;
                memory_threshold_set = true;
                free_threshold_set = true;
            }
            "--users" => {
                config.users = next_arg(&mut args, &argument)?
                    .parse()
                    .map_err(|_| "--users must be an integer".to_owned())?
            }
            "--requests-per-user" => {
                config.requests_per_user = next_arg(&mut args, &argument)?
                    .parse()
                    .map_err(|_| "--requests-per-user must be an integer".to_owned())?
            }
            "--shards" => {
                config.shards = next_arg(&mut args, &argument)?
                    .parse()
                    .map_err(|_| "--shards must be an integer".to_owned())?
            }
            "--db-layout" => {
                config.layout = match next_arg(&mut args, &argument)?.as_str() {
                    "shared" => DatabaseLayout::Shared,
                    "dedicated" => DatabaseLayout::Dedicated,
                    other => return Err(format!("unsupported database layout {other:?}")),
                };
            }
            "--output-root" => config.output_root = PathBuf::from(next_arg(&mut args, &argument)?),
            "--preflight-timeout-seconds" => {
                config.preflight.timeout = Duration::from_secs(parse_value(&mut args, &argument)?);
            }
            "--preflight-observation-ms" => {
                config.preflight.observation =
                    Duration::from_millis(parse_value(&mut args, &argument)?);
            }
            "--preflight-min-free-bytes" => {
                config.preflight.min_free_bytes = parse_value(&mut args, &argument)?;
                free_threshold_set = true;
            }
            "--preflight-min-memory-bytes" => {
                config.preflight.min_available_mem_bytes = parse_value(&mut args, &argument)?;
                memory_threshold_set = true;
            }
            other => return Err(format!("unknown argument {other:?}; use --help")),
        }
    }
    if config.shards == 0 || config.shards > 64 {
        return Err("--shards must be in 1..=64".to_owned());
    }
    let transactions = (config.users as u128)
        .checked_mul(config.requests_per_user as u128)
        .ok_or_else(|| "users multiplied by requests-per-user overflows".to_owned())?;
    if transactions > u64::MAX as u128 {
        return Err("total transaction count exceeds the supported u64 range".to_owned());
    }
    if !config.smoke_relaxed {
        if !memory_threshold_set {
            config.preflight.min_available_mem_bytes = workload_threshold(
                transactions,
                CANONICAL_MIN_MEMORY_BYTES,
                FLOOR_MIN_MEMORY_BYTES,
            );
        }
        if !free_threshold_set {
            config.preflight.min_free_bytes =
                workload_threshold(transactions, CANONICAL_MIN_FREE_BYTES, FLOOR_MIN_FREE_BYTES);
        }
    }
    Ok(config)
}

fn workload_threshold(workload_transactions: u128, canonical_bytes: u128, floor_bytes: u64) -> u64 {
    let scaled = canonical_bytes
        .saturating_mul(workload_transactions)
        .div_ceil(CANONICAL_TRANSACTIONS)
        .min(u64::MAX as u128) as u64;
    scaled.max(floor_bytes)
}

fn next_arg<I>(args: &mut I, option: &str) -> Result<String, String>
where
    I: Iterator<Item = String>,
{
    args.next()
        .ok_or_else(|| format!("{option} requires a value"))
}

fn parse_value<I>(args: &mut I, option: &str) -> Result<u64, String>
where
    I: Iterator<Item = String>,
{
    next_arg(args, option)?
        .parse()
        .map_err(|_| format!("{option} must be an unsigned integer"))
}

fn print_help() {
    println!(
        "ledger_core_pipeline [--users N] [--requests-per-user N] [--shards N] [--db-layout shared|dedicated] [--output-root PATH] [--smoke]\n\
         [--preflight-timeout-seconds N] [--preflight-observation-ms N] [--preflight-min-free-bytes N] [--preflight-min-memory-bytes N]\n\
         Defaults: 50000 users x 200 requests, 50/50 credit/debit; CPU <= 10%, disk <= 5%, 60s preflight.\n\
         The default 10M workload uses 8 GiB available-memory and 16 GiB free-space thresholds; smaller workloads scale these thresholds by transaction count with 256 MiB / 512 MiB floors. These are startup thresholds, not capacity guarantees.\n\
         --smoke uses 100 x 20 and relaxes preflight; it reports smoke_relaxed=true and checks correctness/wiring only."
    );
}
