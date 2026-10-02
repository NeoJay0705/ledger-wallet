//! Integrated fixed-workload account-shard benchmark.
//!
//! This orchestration deliberately reuses the commit queue, request routing,
//! projector, watermark manager, safe GC worker, and recovery helpers from the
//! parent pipeline module. Only ownership, topology, aggregation, and archival
//! are specific to the multi-shard comparison.

use super::*;
use crate::ledger_account_store::{IndexLookupMode, RocksDbBudget};
use rocksdb::{DB, Env, Options};
use std::ffi::OsString;
use std::fs::OpenOptions;
use std::process::{Command, Stdio};

#[path = "ledger_pipeline_sharding_artifacts.rs"]
mod artifacts;

const SHARD_COUNTS: [usize; 2] = [2, 4];
const CONCURRENCIES: [usize; 2] = [4, 8];
const REPETITIONS: usize = 3;
const FORMAL_USERS: usize = 50_000;
const FORMAL_REQUESTS_PER_USER: usize = 200;
const SHARED_WRITE_BUFFER_BUDGET: usize = 128 * 1024 * 1024;
const SHARED_BLOCK_CACHE_BUDGET: usize = 128 * 1024 * 1024;
const SHARED_MAX_BACKGROUND_JOBS: i32 = 8;
const ROCKSDB_LOW_PRIORITY_THREADS: i32 = 6;
const ROCKSDB_HIGH_PRIORITY_THREADS: i32 = 2;
const DEFAULT_ARCHIVE_ROOT: &str = "benches/data/ledger_pipeline_sharding";
const DEFAULT_SMOKE_ROOT: &str = "target/ledger-pipeline-sharding-smoke";

pub(crate) type RocksDbCounters = (u64, u64, u64, u64, u64, u64, u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RocksLayout {
    Shared,
    Dedicated,
}

impl RocksLayout {
    fn as_str(self) -> &'static str {
        match self {
            Self::Shared => "shared",
            Self::Dedicated => "dedicated",
        }
    }

    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "shared" => Ok(Self::Shared),
            "dedicated" => Ok(Self::Dedicated),
            _ => Err(format!(
                "invalid RocksDB layout {value}; expected shared or dedicated"
            )),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ShardSummary {
    pub shard_id: usize,
    pub account_count: usize,
    pub requests: u64,
    pub credits: u64,
    pub debits: u64,
    pub final_sequence: u64,
    pub projected_sequence: u64,
    pub destination_sequence: u64,
    pub gc_prefix: u64,
    pub final_boundary: u64,
    pub final_boundary_target_sequence: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct TrialSummary {
    pub requests: u64,
    pub credits: u64,
    pub debits: u64,
    pub final_sequence_sum: u64,
    pub client_wall: Duration,
    pub settled_wall: Duration,
    pub shards: Vec<ShardSummary>,
}

#[cfg(test)]
#[derive(Clone, Debug)]
pub(crate) struct CrossShardFaultPlan {
    pub blocked_shard: usize,
    pub failed_shard: usize,
    pub group_index: usize,
    pub control: Arc<TestFaultControl>,
}

#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct TestFaultControl {
    blocked: std::sync::atomic::AtomicBool,
    failed: std::sync::atomic::AtomicBool,
    blocked_notify: tokio::sync::Notify,
    failed_notify: tokio::sync::Notify,
    release: Mutex<Option<Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>>>,
}

#[cfg(test)]
impl TestFaultControl {
    pub async fn wait_until_blocked(&self) {
        loop {
            let notified = self.blocked_notify.notified();
            if self.blocked.load(std::sync::atomic::Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }

    pub async fn wait_until_failure_propagated(&self) {
        loop {
            let notified = self.failed_notify.notified();
            if self.failed.load(std::sync::atomic::Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }

    pub fn release_blocked_read(&self) {
        let release = self
            .release
            .lock()
            .expect("test release mutex poisoned")
            .clone();
        if let Some(release) = release {
            let (lock, condition) = &*release;
            *lock.lock().expect("test release latch poisoned") = true;
            condition.notify_all();
        }
    }

    fn set_release(&self, release: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>) {
        *self.release.lock().expect("test release mutex poisoned") = Some(release);
    }

    fn mark_blocked(&self) {
        self.blocked
            .store(true, std::sync::atomic::Ordering::Release);
        self.blocked_notify.notify_waiters();
    }

    fn mark_failed(&self) {
        self.failed
            .store(true, std::sync::atomic::Ordering::Release);
        self.failed_notify.notify_waiters();
    }
}

#[derive(Clone, Debug)]
pub(crate) struct TrialOptions {
    pub shards: usize,
    pub layout: RocksLayout,
    pub concurrency: usize,
    pub users: usize,
    pub requests_per_user: usize,
    pub output_dir: PathBuf,
    pub preflight_observation: Duration,
    #[cfg(test)]
    pub cross_shard_fault: Option<CrossShardFaultPlan>,
}

#[derive(Clone, Debug)]
struct MatrixCase {
    shards: usize,
    layout: RocksLayout,
    concurrency: usize,
}

impl MatrixCase {
    fn name(&self) -> String {
        format!(
            "s{}_{}_chunked256_c{}",
            self.shards,
            self.layout.as_str(),
            self.concurrency
        )
    }
}

fn formal_matrix() -> Vec<MatrixCase> {
    let mut cases = Vec::with_capacity(8);
    for shards in SHARD_COUNTS {
        for layout in [RocksLayout::Shared, RocksLayout::Dedicated] {
            for concurrency in CONCURRENCIES {
                cases.push(MatrixCase {
                    shards,
                    layout,
                    concurrency,
                });
            }
        }
    }
    cases
}

/// Return the exact global account assignment used by every trial.
pub(crate) fn topology(shards: usize, users: usize) -> Result<Vec<Vec<u64>>, String> {
    if !SHARD_COUNTS.contains(&shards) {
        return Err("formal sharding topology requires exactly 2 or 4 shards".to_owned());
    }
    if users == 0 || users % shards != 0 {
        return Err(format!(
            "user count {users} must be positive and divisible by {shards}"
        ));
    }
    let mut result = (0..shards)
        .map(|_| Vec::with_capacity(users / shards))
        .collect::<Vec<_>>();
    for account_id in 0..users as u64 {
        result[(account_id % shards as u64) as usize].push(account_id);
    }
    Ok(result)
}

/// Divide a fixed aggregate RocksDB configuration budget across DB instances.
pub(crate) fn budget_for(shards: usize, layout: RocksLayout) -> Result<Vec<RocksDbBudget>, String> {
    if !SHARD_COUNTS.contains(&shards) {
        return Err("RocksDB budget is defined only for 2 or 4 shards".to_owned());
    }
    let database_count = match layout {
        RocksLayout::Shared => 1,
        RocksLayout::Dedicated => shards,
    };
    let budget = RocksDbBudget {
        write_buffer_size: SHARED_WRITE_BUFFER_BUDGET / (2 * database_count),
        max_write_buffer_number: 2,
        block_cache_bytes: SHARED_BLOCK_CACHE_BUDGET / database_count,
        max_background_jobs: SHARED_MAX_BACKGROUND_JOBS / database_count as i32,
    };
    Ok(vec![budget; database_count])
}

#[derive(Debug)]
struct Cli {
    smoke: bool,
    smoke_child: bool,
    output_root: PathBuf,
    internal_trial: bool,
    trial_output: Option<PathBuf>,
    shards: Option<usize>,
    layout: Option<RocksLayout>,
    concurrency: Option<usize>,
    users: Option<usize>,
    requests_per_user: Option<usize>,
    report_only: Option<PathBuf>,
}

impl Cli {
    fn parse(args: &[String], repo_root: &Path) -> Result<Self, String> {
        let mut cli = Self {
            smoke: false,
            smoke_child: false,
            output_root: repo_root.join(DEFAULT_ARCHIVE_ROOT),
            internal_trial: false,
            trial_output: None,
            shards: None,
            layout: None,
            concurrency: None,
            users: None,
            requests_per_user: None,
            report_only: None,
        };
        let mut index = 0;
        while index < args.len() {
            let flag = args[index].as_str();
            match flag {
                "--help" | "-h" => {
                    print_help();
                    std::process::exit(0);
                }
                "--smoke" => {
                    cli.smoke = true;
                    index += 1;
                    continue;
                }
                "--internal-trial" => {
                    cli.internal_trial = true;
                    index += 1;
                    continue;
                }
                "--smoke-child" => {
                    cli.smoke_child = true;
                    index += 1;
                    continue;
                }
                "--bench" => {
                    index += 1;
                    continue;
                }
                _ => {}
            }
            index += 1;
            let value = args
                .get(index)
                .ok_or_else(|| format!("missing value for {flag}"))?;
            match flag {
                "--output-root" => cli.output_root = repo_relative(repo_root, value),
                "--trial-output" => cli.trial_output = Some(PathBuf::from(value)),
                "--shards" => cli.shards = Some(parse_value(flag, value)?),
                "--rocks-layout" => cli.layout = Some(RocksLayout::parse(value)?),
                "--index-concurrency" => cli.concurrency = Some(parse_value(flag, value)?),
                "--users" => cli.users = Some(parse_value(flag, value)?),
                "--requests-per-user" => cli.requests_per_user = Some(parse_value(flag, value)?),
                "--report-only" => {
                    if cli.report_only.replace(PathBuf::from(value)).is_some() {
                        return Err("--report-only may be specified only once".to_owned());
                    }
                }
                _ => return Err(format!("unknown option {flag}; use --help")),
            }
            index += 1;
        }
        if cli.internal_trial {
            if cli.smoke
                || cli.trial_output.is_none()
                || cli.shards.is_none()
                || cli.layout.is_none()
                || cli.concurrency.is_none()
                || cli.users.is_none()
                || cli.requests_per_user.is_none()
            {
                return Err("internal trial requires trial output, topology, concurrency, and workload arguments".to_owned());
            }
            if cli.report_only.is_some() {
                return Err("internal trial cannot use --report-only".to_owned());
            }
        } else if cli.smoke_child
            || cli.trial_output.is_some()
            || cli.shards.is_some()
            || cli.layout.is_some()
            || cli.concurrency.is_some()
            || cli.users.is_some()
            || cli.requests_per_user.is_some()
        {
            return Err("trial settings require --internal-trial".to_owned());
        }
        if cli.report_only.is_some() && cli.smoke {
            return Err("--report-only cannot be combined with --smoke".to_owned());
        }
        if cli.smoke {
            cli.output_root = repo_root.join(DEFAULT_SMOKE_ROOT);
        }
        Ok(cli)
    }
}

fn repo_relative(repo_root: &Path, value: &str) -> PathBuf {
    let path = PathBuf::from(value);
    if path.is_absolute() {
        path
    } else {
        repo_root.join(path)
    }
}

fn print_help() {
    eprintln!(
        "Integrated Tokio account-sharding pipeline benchmark\n\
         Formal matrix: S2/S4 x shared/dedicated RocksDB x Chunked group 256 concurrency 4/8, 3 rotated repetitions.\n\
         Each formal trial uses 50,000 global accounts x 200 sequential requests, 4 Tokio workers, PerBatch balances,\n\
         a 50,000-entry total queue split across shards, 2,048-request batches, 5 ms first-dequeue timeout,\n\
         projector/GC batches of 256, 100 ms watermark/GC intervals, and 500 ms retention.\n\
         Options: --smoke (one reduced S2/shared/C4 trial under target/) --output-root PATH\n\
         --report-only RUN_DIR. Internal trial options are reserved for the parent runner."
    );
}

pub(crate) fn run_from_args() -> Result<(), String> {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .canonicalize()
        .map_err(|error| format!("cannot resolve repository root: {error}"))?;
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let cli = Cli::parse(&args, &repo_root)?;
    if cli.internal_trial {
        return run_internal_trial(cli);
    }
    if let Some(run) = cli.report_only {
        let run = run.to_string_lossy();
        return artifacts::report_only(&repo_root, &repo_relative(&repo_root, &run));
    }
    run_matrix(&repo_root, cli)
}

fn run_matrix(repo_root: &Path, cli: Cli) -> Result<(), String> {
    let smoke = cli.smoke;
    let (users, requests_per_user) = if smoke {
        (200, 200)
    } else {
        (FORMAL_USERS, FORMAL_REQUESTS_PER_USER)
    };
    let matrix = if smoke {
        vec![MatrixCase {
            shards: 2,
            layout: RocksLayout::Shared,
            concurrency: 4,
        }]
    } else {
        formal_matrix()
    };
    let output_root = cli.output_root;
    let preflight = resource_preflight(users, requests_per_user, smoke, Duration::from_secs(3));
    let setup = ledger_preflight::ensure_idle(&output_root, &preflight)
        .map_err(|error| format!("strict sharding-run preflight failed: {error}"))?;
    fs::create_dir_all(&output_root).map_err(|error| {
        format!(
            "cannot create output root {}: {error}",
            output_root.display()
        )
    })?;
    let output_root = output_root.canonicalize().map_err(|error| {
        format!(
            "cannot resolve output root {}: {error}",
            output_root.display()
        )
    })?;
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("system clock is before Unix epoch: {error}"))?
        .as_nanos();
    let run_dir = output_root.join(format!("run-{run_id}"));
    fs::create_dir(&run_dir).map_err(|error| {
        format!(
            "cannot create unique run archive {}: {error}",
            run_dir.display()
        )
    })?;
    fs::create_dir(run_dir.join("trials"))
        .map_err(|error| format!("cannot create trial archive: {error}"))?;
    write_run_manifest(
        repo_root,
        &run_dir,
        &matrix,
        users,
        requests_per_user,
        smoke,
        &setup,
    )?;

    let executable = std::env::current_exe()
        .map_err(|error| format!("cannot resolve benchmark executable: {error}"))?;
    let orders = if smoke {
        vec![(1, matrix.clone())]
    } else {
        (1..=REPETITIONS)
            .map(|repetition| {
                let mut order = matrix.clone();
                let rotation = ((repetition - 1) * 3) % order.len();
                order.rotate_left(rotation);
                (repetition, order)
            })
            .collect()
    };
    let mut completed_dirs = Vec::with_capacity(matrix.len() * orders.len());
    for (repetition, order) in orders {
        for (position, case) in order.into_iter().enumerate() {
            let case_name = case.name();
            let trial_name = format!("trial-{repetition:02}-{}-p{:02}", case_name, position + 1);
            let trial_dir = run_dir.join("trials").join(&trial_name);
            fs::create_dir(&trial_dir).map_err(|error| {
                format!(
                    "cannot create unique trial {}: {error}",
                    trial_dir.display()
                )
            })?;
            let stdout_path = trial_dir.join("stdout.log");
            let stderr_path = trial_dir.join("stderr.log");
            let stdout = File::create(&stdout_path)
                .map_err(|error| format!("cannot create {}: {error}", stdout_path.display()))?;
            let stderr = File::create(&stderr_path)
                .map_err(|error| format!("cannot create {}: {error}", stderr_path.display()))?;
            let mut command = Command::new(&executable);
            command.args([
                OsString::from("--internal-trial"),
                OsString::from("--trial-output"),
                OsString::from(trial_dir.as_os_str()),
                OsString::from("--shards"),
                OsString::from(case.shards.to_string()),
                OsString::from("--rocks-layout"),
                OsString::from(case.layout.as_str()),
                OsString::from("--index-concurrency"),
                OsString::from(case.concurrency.to_string()),
                OsString::from("--users"),
                OsString::from(users.to_string()),
                OsString::from("--requests-per-user"),
                OsString::from(requests_per_user.to_string()),
            ]);
            if smoke {
                command.arg("--smoke-child");
            }
            command
                .stdout(Stdio::from(stdout))
                .stderr(Stdio::from(stderr));
            let status = match command.status() {
                Ok(status) => status,
                Err(error) => {
                    let provenance = format!(
                        "trial={trial_name}\ncase={case_name}\nrepetition={repetition}\nposition={}\nlaunch_error={error}\nstdout=stdout.log\nstderr=stderr.log\n",
                        position + 1
                    );
                    write_create_new(&trial_dir.join("status.txt"), provenance.as_bytes())?;
                    return Err(format!(
                        "cannot launch {trial_name}: {error}; logs and status are retained in {}",
                        trial_dir.display()
                    ));
                }
            };
            let provenance = format!(
                "trial={trial_name}\ncase={case_name}\nrepetition={repetition}\nposition={}\nexit_status={status}\nstdout=stdout.log\nstderr=stderr.log\n",
                position + 1
            );
            write_create_new(&trial_dir.join("status.txt"), provenance.as_bytes())?;
            if !status.success() {
                return Err(format!(
                    "trial {trial_name} failed with {status}; raw stdout/stderr and status are retained in {}",
                    trial_dir.display()
                ));
            }
            artifacts::validate_trial_artifacts(&trial_dir)?;
            completed_dirs.push((repetition, case, trial_name, trial_dir));
        }
    }
    artifacts::write_matrix_reports(&run_dir, &completed_dirs)?;
    println!("SHARDING_ARCHIVE {}", run_dir.display());
    if smoke {
        println!(
            "NONFORMAL_SMOKE users={users} requests_per_user={requests_per_user} trials={}",
            completed_dirs.len()
        );
    }
    Ok(())
}

fn resource_preflight(
    users: usize,
    requests_per_user: usize,
    smoke: bool,
    observation: Duration,
) -> PreflightConfig {
    let records =
        (users as u64).saturating_mul(requests_per_user as u64 + SEED_TRANSACTIONS_PER_USER as u64);
    let mut config = PreflightConfig {
        observation,
        timeout: Duration::from_secs(60),
        max_cpu_busy_pct: 10.0,
        max_disk_busy_pct: 5.0,
        min_available_mem_bytes: records
            .saturating_mul(ESTIMATED_DESTINATION_BYTES_PER_RECORD)
            .saturating_add(DEFAULT_MEMORY_RESERVE_BYTES),
        min_free_bytes: records
            .saturating_mul(ESTIMATED_DISK_BYTES_PER_RECORD)
            .saturating_add(DEFAULT_FREE_SPACE_RESERVE_BYTES),
    };
    if smoke {
        config.min_available_mem_bytes = records
            .saturating_mul(ESTIMATED_DESTINATION_BYTES_PER_RECORD)
            .saturating_add(128 * 1024 * 1024);
        config.min_free_bytes = records
            .saturating_mul(ESTIMATED_DISK_BYTES_PER_RECORD)
            .saturating_add(256 * 1024 * 1024);
    }
    config
}

fn command_output(program: &str, args: &[&str]) -> String {
    match Command::new(program).args(args).output() {
        Ok(output) if output.status.success() => {
            String::from_utf8_lossy(&output.stdout).trim().to_owned()
        }
        Ok(output) => format!(
            "unavailable(status={}): {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ),
        Err(error) => format!("unavailable: {error}"),
    }
}

fn source_hash(path: &Path) -> Result<String, String> {
    let output = Command::new("sha256sum")
        .arg(path)
        .output()
        .map_err(|error| format!("cannot hash source {}: {error}", path.display()))?;
    if !output.status.success() {
        return Err(format!("sha256sum failed for {}", path.display()));
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .split_whitespace()
        .next()
        .unwrap_or("missing-hash")
        .to_owned())
}

fn shell_join(args: &[String]) -> String {
    args.iter()
        .map(|arg| {
            if arg
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || "_./:=+-".contains(ch))
            {
                arg.clone()
            } else {
                format!("'{}'", arg.replace('\'', "'\\''"))
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn write_run_manifest(
    repo_root: &Path,
    run_dir: &Path,
    matrix: &[MatrixCase],
    users: usize,
    requests_per_user: usize,
    smoke: bool,
    setup: &PreflightReport,
) -> Result<(), String> {
    let repo_root = repo_root
        .canonicalize()
        .map_err(|error| format!("cannot resolve repository root: {error}"))?;
    let head = command_output(
        "git",
        &["-C", repo_root.to_str().unwrap_or("."), "rev-parse", "HEAD"],
    );
    let rustc = command_output("rustc", &["--version", "--verbose"]);
    let cargo = command_output("cargo", &["--version"]);
    let uname = command_output("uname", &["-a"]);
    let cpus_online = fs::read_to_string("/sys/devices/system/cpu/online")
        .unwrap_or_else(|error| format!("unavailable: {error}"));
    let process_status = fs::read_to_string("/proc/self/status")
        .unwrap_or_else(|error| format!("unavailable: {error}"));
    let allowed_cpus = process_status
        .lines()
        .find(|line| line.starts_with("Cpus_allowed_list:"))
        .unwrap_or("Cpus_allowed_list: unavailable");
    let cpu_model = fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|contents| {
            contents
                .lines()
                .find(|line| line.starts_with("model name"))
                .map(str::to_owned)
        })
        .unwrap_or_else(|| "model name: unavailable".to_owned());
    let source_files = [
        repo_root.join("benches/ledger_pipeline_sharding_tokio.rs"),
        repo_root.join("benches/support/ledger_pipeline.rs"),
        repo_root.join("benches/support/ledger_pipeline_sharding.rs"),
        repo_root.join("benches/support/ledger_pipeline_sharding_artifacts.rs"),
        repo_root.join("benches/support/ledger_account_store.rs"),
        repo_root.join("benches/support/ledger_time_boundary.rs"),
        repo_root.join("benches/support/ledger_projection_worker.rs"),
        repo_root.join("benches/support/request_batch_queue.rs"),
        repo_root.join("benches/support/ledger_preflight.rs"),
        repo_root.join("Cargo.lock"),
    ];
    let mut source_lines = Vec::new();
    for (index, path) in source_files.into_iter().enumerate() {
        let path = path
            .canonicalize()
            .map_err(|error| format!("cannot resolve benchmark source: {error}"))?;
        source_lines.push(format!(
            "source_{index:02}_path={}\nsource_{index:02}_sha256={}",
            path.display(),
            source_hash(&path)?
        ));
    }
    let cargo_lock = fs::read_to_string(repo_root.join("Cargo.lock"))
        .map_err(|error| format!("cannot read Cargo.lock: {error}"))?;
    let rocksdb_versions = cargo_lock
        .lines()
        .fold((String::new(), String::new()), |mut current, line| {
            if line == "name = \"rocksdb\"" {
                current.0 = "rocksdb".to_owned();
            } else if line == "name = \"librocksdb-sys\"" {
                current.0 = "librocksdb-sys".to_owned();
            } else if line.starts_with("version = ") && !current.0.is_empty() {
                current.1.push_str(&format!(
                    "{} {} ",
                    current.0,
                    line.trim_start_matches("version = ").trim_matches('"')
                ));
                current.0.clear();
            }
            current
        })
        .1;
    let benchmark_arguments = std::env::args().skip(1).collect::<Vec<_>>();
    let formal_command = format!(
        "cargo bench --bench ledger_pipeline_sharding_tokio -- {}",
        shell_join(&benchmark_arguments),
    );
    let matrix_lines = matrix
        .iter()
        .map(MatrixCase::name)
        .collect::<Vec<_>>()
        .join(",");
    let profile = if smoke {
        "NONFORMAL_REDUCED_SMOKE"
    } else {
        "FORMAL_MATRIX"
    };
    let mut manifest = format!(
        "profile={profile}\nrun_id={}\nrepo_root={}\ngit_base_HEAD={}\nusers={}\ncoroutines={}\nrequests_per_user={}\nexpected_requests_per_trial={}\nexpected_ledger_records_per_trial={}\noperations=exactly_50_percent_credit_50_percent_debit_amount_1\naccount_routing=global_account_id_modulo_shard_count\nshard_cases={}\nrepetitions={}\nrotated_order=left_rotation_by_3_positions_per_repetition\nrocksdb_layouts=shared_or_dedicated\nindex_lookup=Chunked_group_256_concurrency_4_or_8\nbalance_mode=PerBatch\nruntime_worker_threads=4\nruntime_max_blocking_threads=Tokio_default_unmodified\naffinity=inherited\nqueue_capacity_total={}\nqueue_capacity_per_shard_formula=users_count_per_shard\nbatch_size={}\nfirst_dequeue_timeout_ms={}\nprojector_batch_size={}\nwatermark_interval_ms={}\nretention_ms={}\ngc_batch_size={}\ngc_interval_ms={}\nseed_transactions_per_account=3\nseed_records_per_trial={}\npreflight_setup_path={}\npreflight_filesystem={}\npreflight_device={}({})\npreflight_attempts={}\npreflight_cpu_busy_pct={:.3}\npreflight_device_busy_pct={:.3}\npreflight_available_memory_bytes={}\npreflight_free_bytes={}\nformal_command={}\nbenchmark_executable_args={}\nrustc=\n{rustc}\ncargo={cargo}\nrocksdb_crates={rocksdb_versions}\nkernel={uname}\ncpu_online={}\n{}\n{}\nsource_hashes:\n{}\n",
        run_dir
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("unknown-run")
            .trim_start_matches("run-"),
        repo_root.display(),
        head,
        users,
        users,
        requests_per_user,
        users.saturating_mul(requests_per_user),
        users.saturating_mul(requests_per_user + SEED_TRANSACTIONS_PER_USER),
        matrix_lines,
        if smoke { 1 } else { REPETITIONS },
        users,
        DEFAULT_BATCH_SIZE,
        DEFAULT_BATCH_TIMEOUT_MS,
        DEFAULT_PROJECTION_BATCH_SIZE,
        DEFAULT_WATERMARK_INTERVAL_MS,
        DEFAULT_RETENTION_MS,
        DEFAULT_GC_BATCH_SIZE,
        DEFAULT_GC_INTERVAL_MS,
        users * SEED_TRANSACTIONS_PER_USER,
        setup.path.display(),
        setup.filesystem,
        setup.device,
        setup.major_minor,
        setup.attempts,
        setup.cpu_busy_pct,
        setup.disk_busy_pct,
        setup.mem_available_bytes,
        setup.free_bytes,
        formal_command,
        shell_join(&benchmark_arguments),
        cpus_online.trim(),
        allowed_cpus,
        cpu_model,
        source_lines.join("\n"),
    );
    manifest.push_str(&format!(
        "rocksdb_env_scope=shared_default_env_for_child_process\nrocksdb_env_low_priority_threads={ROCKSDB_LOW_PRIORITY_THREADS}\nrocksdb_env_high_priority_threads={ROCKSDB_HIGH_PRIORITY_THREADS}\nrocksdb_environment_pools_are_configured_limits_not_hard_active_thread_or_cpu_caps=true\n"
    ));
    if !smoke {
        manifest.push_str("historical_S1_context=descriptive_only; prior report P4 56982.273 RPS CPU 2.093051 core_equivalents; P8 75076.856 RPS CPU 3.177564 core_equivalents; old default resource budget and old namespace make it unpaired; no saturation claim is valid.\n");
    }
    write_create_new(&run_dir.join("run_manifest.txt"), manifest.as_bytes())
}

fn write_create_new(path: &Path, contents: &[u8]) -> Result<(), String> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| format!("cannot create immutable file {}: {error}", path.display()))?;
    file.write_all(contents)
        .and_then(|_| file.sync_all())
        .map_err(|error| format!("cannot persist {}: {error}", path.display()))
}

type PipelineQueue = BatchQueue<ledger_time_boundary::AdmittedTransaction, GuardedReply>;

struct DbHandle {
    path: PathBuf,
    db: Arc<DB>,
    options: Arc<Options>,
    budget: RocksDbBudget,
}

struct OwnedScratch {
    path: PathBuf,
    workload_started: bool,
    cleaned: bool,
}

impl OwnedScratch {
    fn create(path: PathBuf) -> Result<Self, String> {
        if path.exists() {
            return Err(format!(
                "owned scratch path already exists: {}",
                path.display()
            ));
        }
        fs::create_dir_all(&path)
            .map_err(|error| format!("cannot create owned scratch {}: {error}", path.display()))?;
        Ok(Self {
            path,
            workload_started: false,
            cleaned: false,
        })
    }

    fn mark_workload_started(&mut self) {
        self.workload_started = true;
    }

    fn allow_cleanup_after_all_handles_closed(&mut self) {
        self.workload_started = false;
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
                "cannot remove owned scratch {}: {error}",
                self.path.display()
            )),
        }
    }
}

impl Drop for OwnedScratch {
    fn drop(&mut self) {
        if self.cleaned {
            return;
        }
        if self.workload_started {
            eprintln!(
                "SHARDING_SCRATCH_RETAINED_AFTER_UNEXPECTED_DROP path={}",
                self.path.display()
            );
            return;
        }
        if let Err(error) = fs::remove_dir_all(&self.path) {
            if error.kind() != std::io::ErrorKind::NotFound {
                eprintln!(
                    "SHARDING_SCRATCH_CLEANUP_WARNING path={} error={error}",
                    self.path.display()
                );
            }
        }
    }
}

struct ShardRuntime {
    id: usize,
    db_index: usize,
    account_ids: Vec<u64>,
    source: Option<AccountStore>,
    destination: Arc<MockProjectionStore>,
    progress: Arc<ProjectionProgress>,
    gate: Arc<AdmissionGate>,
    manager: Arc<WatermarkManager>,
    watermark_metrics: Arc<ledger_time_boundary::WatermarkMetrics>,
    projection_samples: Arc<Mutex<Vec<ProjectionStageSample>>>,
    gc_samples: Arc<Mutex<Vec<TimedGcStep>>>,
    index_batches: Arc<Mutex<Vec<crate::ledger_account_store::IndexLookupBatchMetrics>>>,
    seed_sequence: u64,
    queue: Option<PipelineQueue>,
    worker: Option<BatchWorker>,
    committed_rx: Option<watch::Receiver<u64>>,
    projector: Option<JoinHandle<Result<(), String>>>,
    watermark_task: Option<JoinHandle<Result<(), String>>>,
    gc_task: Option<JoinHandle<Result<(), String>>>,
}

#[derive(Clone)]
struct RawStage {
    metric: String,
    shard_id: usize,
    logical_id: Option<u64>,
    value_ns: u64,
    completed_at: Option<Instant>,
}

#[derive(Clone)]
struct RequestSample {
    logical_id: u64,
    shard_id: usize,
    total_ns: u64,
    admission_ns: u64,
    enqueue_ns: u64,
    queue_ns: u64,
    batch_ns: u64,
    handler_ns: u64,
    response_ns: u64,
}

#[derive(Clone)]
struct RawBackground {
    shard_id: usize,
    event: String,
    completed_at: Instant,
    elapsed_ns: u64,
    sequence: u64,
    records: u64,
    read_ns: u64,
    apply_ns: u64,
    progress_sync_ns: u64,
    total_ns: u64,
    scanned: u64,
    deleted: u64,
    bytes_deleted: u64,
    gc_prefix_seq: u64,
    blocked_at_seq: Option<u64>,
    gc_scan_ns: u64,
    gc_delete_ns: u64,
    gc_write_ns: u64,
    watermark_target_sequence: u64,
    watermark: u64,
    watermark_fence_wait_ns: u64,
    watermark_projection_wait_ns: u64,
    watermark_persist_ns: u64,
}

struct TrialArtifacts {
    trial_name: String,
    shards: usize,
    layout: RocksLayout,
    concurrency: usize,
    users: usize,
    requests_per_user: usize,
    sample_stride: u64,
    expected_request_sample_count: usize,
    summary: TrialSummary,
    client_wall: Duration,
    client_end_elapsed_ns: u64,
    cpu_seconds: f64,
    cpu_core_equivalents: f64,
    settled_wall: Duration,
    recovery_seconds: f64,
    integrity_seconds: f64,
    peak_rss_bytes: u64,
    db_bytes: u64,
    io: IoDelta,
    cpu_sample_offset_us: u64,
    progress_sample_offset_us: u64,
    io_sample_offset_us: u64,
    storage_sample_offset_us: u64,
    rocks: (u64, u64, u64, u64, u64, u64, u64),
    stages: Vec<RawStage>,
    request_samples: Vec<RequestSample>,
    background: Vec<RawBackground>,
    setup_preflight: PreflightReport,
    measure_preflight: PreflightReport,
    databases: Vec<(String, RocksDbBudget)>,
    shard_backlogs: Vec<(usize, u64, u64, u64, u64)>,
    final_boundary_by_shard: Vec<u64>,
    final_boundary_target_sequence: Vec<u64>,
    index_batch_rows: Vec<String>,
    index_group_rows: Vec<String>,
    max_in_flight_per_batch: usize,
    max_running_query_jobs_per_batch: usize,
}

struct ClientMeasurement {
    combined: ClientStats,
    per_shard_stats: Vec<ClientStats>,
    client_end: Instant,
    wall_started: Instant,
    settle_started: Instant,
    client_wall: Duration,
    cpu_seconds: f64,
    cpu_sample_offset_us: u64,
    progress_sample_offset_us: u64,
    io_sample_offset_us: u64,
    storage_sample_offset_us: u64,
    io: IoDelta,
    rocks: (u64, u64, u64, u64, u64, u64, u64),
    peak_rss_bytes: u64,
    latest_by_shard: Vec<u64>,
    projected_by_shard: Vec<u64>,
    destination_by_shard: Vec<u64>,
    gc_prefix_by_shard: Vec<u64>,
    index_batches_by_shard: Vec<Vec<crate::ledger_account_store::IndexLookupBatchMetrics>>,
}

struct PreparedTrial {
    topology: Vec<Vec<u64>>,
    rocks_env: Env,
    databases: Vec<DbHandle>,
    shards: Vec<ShardRuntime>,
    initial_rocks: Vec<(u64, u64, u64, u64, u64, u64, u64)>,
    initial_io: IoSample,
    seed_time: u64,
    setup_preflight: PreflightReport,
    measure_preflight: PreflightReport,
}

async fn run_client_phase(
    options: &TrialOptions,
    shards: &mut [ShardRuntime],
    topology: &[Vec<u64>],
    seed_time: u64,
    start_barrier: Arc<Barrier>,
    background_failure: watch::Sender<Option<String>>,
    mut background_rx: watch::Receiver<Option<String>>,
    scratch_path: &Path,
    databases: &[DbHandle],
    initial_rocks: &[(u64, u64, u64, u64, u64, u64, u64)],
    initial_io: IoSample,
    cpu_started: Arc<Mutex<ProcessTime>>,
) -> Result<ClientMeasurement, String> {
    let mut clients = JoinSet::new();
    for (shard_id, account_ids) in topology.iter().enumerate() {
        let shard = &shards[shard_id];
        let queue = shard
            .queue
            .as_ref()
            .ok_or_else(|| "shard queue was not initialized".to_owned())?
            .clone();
        for &account_id in account_ids {
            let gate = Arc::clone(&shard.gate);
            let destination = Arc::clone(&shard.destination);
            let queue = queue.clone();
            let failure_rx = background_rx.clone();
            let failure_tx = background_failure.clone();
            let barrier = Arc::clone(&start_barrier);
            let cpu_started = Arc::clone(&cpu_started);
            let requests = options.requests_per_user;
            let sample_stride = DEFAULT_SAMPLE_STRIDE;
            clients.spawn(async move {
                let result = run_client(
                    account_id,
                    requests,
                    sample_stride,
                    0,
                    seed_time,
                    gate,
                    queue,
                    destination,
                    Duration::ZERO,
                    barrier,
                    failure_rx,
                    cpu_started,
                )
                .await;
                if let Err(error) = &result {
                    let message = format!("client shard {shard_id} failed: {error}");
                    let _ = failure_tx.send_if_modified(|current| {
                        if current.is_none() {
                            *current = Some(message);
                            true
                        } else {
                            false
                        }
                    });
                }
                result.map(|stats| (shard_id, stats))
            });
        }
    }

    let cpu_measurement_started = ProcessTime::now();
    *cpu_started
        .lock()
        .map_err(|_| "global process CPU start mutex poisoned".to_owned())? =
        cpu_measurement_started;
    let wall_started = Instant::now();
    start_barrier.wait().await;

    let mut per_shard_stats = (0..options.shards)
        .map(|_| ClientStats::default())
        .collect::<Vec<_>>();
    let mut join_failure = None::<String>;
    let mut remaining = options.users;
    let mut failure_notified = false;
    while remaining > 0 {
        tokio::select! {
            result = clients.join_next() => {
                match result {
                    Some(Ok(Ok((shard_id, stats)))) => combine_client_stats(&mut per_shard_stats[shard_id], stats),
                    Some(Ok(Err(error))) => { join_failure.get_or_insert(error); },
                    Some(Err(error)) => { join_failure.get_or_insert_with(|| format!("client coroutine failed: {error}")); },
                    None => { join_failure.get_or_insert_with(|| "client task set ended before all users replied".to_owned()); remaining = 1; }
                }
                remaining = remaining.saturating_sub(1);
            }
            changed = background_rx.changed(), if !failure_notified => {
                match changed {
                    Ok(()) => {
                        if let Some(error) = background_rx.borrow().clone() {
                            join_failure.get_or_insert(error);
                            fail_all_progress(shards, "global background failure");
                            #[cfg(test)]
                            if let Some(fault) = &options.cross_shard_fault {
                                fault.control.mark_failed();
                            }
                            failure_notified = true;
                        }
                    }
                    Err(_) => { join_failure.get_or_insert_with(|| "global background failure watch closed".to_owned()); failure_notified = true; }
                }
            }
        }
    }
    if let Some(error) = background_rx.borrow().clone() {
        join_failure.get_or_insert(error);
        fail_all_progress(shards, "global background failure");
        #[cfg(test)]
        if let Some(fault) = &options.cross_shard_fault {
            fault.control.mark_failed();
        }
    }
    if let Some(error) = join_failure {
        return Err(error);
    }

    let mut combined = ClientStats::default();
    for stats in &per_shard_stats {
        combine_client_stats(&mut combined, stats.clone());
    }
    let completion = combined
        .completion
        .ok_or_else(|| "no global last-reply CPU sample was recorded".to_owned())?;
    let client_end = completion.reply_at;
    let client_wall = client_end.duration_since(wall_started);
    let settle_started = client_end;
    let cpu_sample_offset_us = micros_offset(completion.cpu_sampled_at, client_end);

    let latest_by_shard = shards
        .iter()
        .map(|shard| shard.source.as_ref().expect("source present").latest_seq())
        .collect::<Vec<_>>();
    let gc_prefix_by_shard = shards
        .iter()
        .map(|shard| {
            shard
                .source
                .as_ref()
                .expect("source present")
                .gc_prefix_seq()
        })
        .collect::<Vec<_>>();
    let mut projected_by_shard = Vec::with_capacity(shards.len());
    for shard in shards.iter() {
        let source = shard.source.as_ref().expect("source present");
        let projected = source
            .durable_projection_progress()
            .await
            .map_err(|error| {
                format!(
                    "shard {} client-end durable progress read failed: {error}",
                    shard.id
                )
            })?;
        projected_by_shard.push(projected);
    }
    let destination_by_shard = shards
        .iter()
        .map(|shard| shard.destination.progress())
        .collect::<Vec<_>>();
    let progress_sample_offset_us = micros_offset(Instant::now(), client_end);
    let index_batches_by_shard = shards
        .iter()
        .map(|shard| {
            shard
                .index_batches
                .lock()
                .map(|samples| samples.clone())
                .map_err(|_| "index lookup metrics mutex poisoned".to_owned())
        })
        .collect::<Result<Vec<_>, _>>()?;

    let io_finished = ledger_preflight::sample_io(scratch_path)?;
    let sampled_io = io_delta(initial_io, io_finished)?;
    let io_sample_offset_us = micros_offset(Instant::now(), client_end);

    let measured_rocks = aggregate_rocks(shards, databases, initial_rocks)?;
    let measured_peak_rss = peak_rss_bytes()?;
    let storage_sample_offset_us = micros_offset(Instant::now(), client_end);

    for shard in shards.iter() {
        let source = shard.source.as_ref().expect("source present");
        let target = source.latest_seq();
        match tokio::time::timeout(Duration::from_secs(300), shard.progress.wait_for(target)).await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                return Err(format!(
                    "shard {} projector catch-up failed: {error}",
                    shard.id
                ));
            }
            Err(_) => {
                return Err(format!(
                    "shard {} projector did not catch up within 300s",
                    shard.id
                ));
            }
        }
        if let Some(error) = background_rx.borrow().clone() {
            fail_all_progress(shards, &error);
            return Err(error);
        }
    }
    Ok(ClientMeasurement {
        combined,
        per_shard_stats,
        client_end,
        wall_started,
        settle_started,
        client_wall,
        cpu_seconds: completion.cpu_seconds,
        cpu_sample_offset_us,
        progress_sample_offset_us,
        io_sample_offset_us,
        storage_sample_offset_us,
        io: sampled_io,
        rocks: measured_rocks,
        peak_rss_bytes: measured_peak_rss,
        latest_by_shard,
        projected_by_shard,
        destination_by_shard,
        gc_prefix_by_shard,
        index_batches_by_shard,
    })
}

fn micros_offset(later: Instant, earlier: Instant) -> u64 {
    u64::try_from(later.duration_since(earlier).as_micros()).unwrap_or(u64::MAX)
}

fn fail_all_progress(shards: &[ShardRuntime], reason: &str) {
    for shard in shards {
        shard
            .progress
            .fail(format!("global pipeline failure: {reason}"));
    }
}

fn aggregate_rocks(
    shards: &[ShardRuntime],
    databases: &[DbHandle],
    initial: &[RocksDbCounters],
) -> Result<RocksDbCounters, String> {
    if initial.len() != databases.len() {
        return Err("initial RocksDB counter count does not match database count".to_owned());
    }
    let assignments = shards
        .iter()
        .map(|shard| shard.db_index)
        .collect::<Vec<_>>();
    let representatives = database_representatives(&assignments, databases.len())?;
    let mut current = Vec::with_capacity(representatives.len());
    for shard_id in representatives {
        current.push(
            shards[shard_id]
                .source
                .as_ref()
                .expect("source present")
                .rocksdb_stats(),
        );
    }
    sum_unique_database_deltas(initial, &current)
}

pub(crate) fn database_representatives(
    shard_database_ids: &[usize],
    database_count: usize,
) -> Result<Vec<usize>, String> {
    if database_count == 0 || shard_database_ids.is_empty() {
        return Err("RocksDB aggregation requires at least one shard and database".to_owned());
    }
    let mut representatives = vec![None; database_count];
    for (shard_id, &database_id) in shard_database_ids.iter().enumerate() {
        let representative = representatives
            .get_mut(database_id)
            .ok_or_else(|| format!("shard {shard_id} references unknown database {database_id}"))?;
        representative.get_or_insert(shard_id);
    }
    representatives
        .into_iter()
        .enumerate()
        .map(|(database_id, shard_id)| {
            shard_id.ok_or_else(|| format!("database {database_id} has no shard owner"))
        })
        .collect()
}

pub(crate) fn sum_unique_database_deltas(
    initial: &[RocksDbCounters],
    current: &[RocksDbCounters],
) -> Result<RocksDbCounters, String> {
    if initial.is_empty() || initial.len() != current.len() {
        return Err(
            "unique RocksDB counter snapshots must have matching nonempty DB rows".to_owned(),
        );
    }
    let mut total: RocksDbCounters = (0, 0, 0, 0, 0, 0, 0);
    for (&before, &after) in initial.iter().zip(current) {
        let delta = subtract_rocks(after, before);
        total.0 = total
            .0
            .checked_add(delta.0)
            .ok_or_else(|| "RocksDB counter sum overflow".to_owned())?;
        total.1 = total
            .1
            .checked_add(delta.1)
            .ok_or_else(|| "RocksDB counter sum overflow".to_owned())?;
        total.2 = total
            .2
            .checked_add(delta.2)
            .ok_or_else(|| "RocksDB counter sum overflow".to_owned())?;
        total.3 = total
            .3
            .checked_add(delta.3)
            .ok_or_else(|| "RocksDB counter sum overflow".to_owned())?;
        total.4 = total
            .4
            .checked_add(delta.4)
            .ok_or_else(|| "RocksDB counter sum overflow".to_owned())?;
        total.5 = total
            .5
            .checked_add(delta.5)
            .ok_or_else(|| "RocksDB counter sum overflow".to_owned())?;
        total.6 = total
            .6
            .checked_add(delta.6)
            .ok_or_else(|| "RocksDB counter sum overflow".to_owned())?;
    }
    Ok(total)
}

fn run_internal_trial(cli: Cli) -> Result<(), String> {
    let output_dir = cli
        .trial_output
        .ok_or_else(|| "missing internal output path".to_owned())?;
    let options = TrialOptions {
        shards: cli.shards.ok_or_else(|| "missing shard count".to_owned())?,
        layout: cli
            .layout
            .ok_or_else(|| "missing RocksDB layout".to_owned())?,
        concurrency: cli
            .concurrency
            .ok_or_else(|| "missing index concurrency".to_owned())?,
        users: cli.users.ok_or_else(|| "missing user count".to_owned())?,
        requests_per_user: cli
            .requests_per_user
            .ok_or_else(|| "missing request count".to_owned())?,
        output_dir,
        preflight_observation: if cli.smoke_child {
            Duration::from_millis(100)
        } else {
            Duration::from_secs(3)
        },
        #[cfg(test)]
        cross_shard_fault: None,
    };
    let runtime = Builder::new_multi_thread()
        .worker_threads(WORKERS)
        .enable_all()
        .build()
        .map_err(|error| format!("cannot create fixed four-worker Tokio runtime: {error}"))?;
    runtime.block_on(run_trial_and_write(options)).map(|_| ())
}

#[cfg(test)]
pub(crate) async fn run_trial_for_test(options: TrialOptions) -> Result<TrialSummary, String> {
    run_trial_and_write(options).await
}

#[cfg(test)]
pub(crate) fn validate_trial_archive(trial_dir: &Path) -> Result<(), String> {
    artifacts::validate_trial_artifacts(trial_dir)
}

async fn run_trial_and_write(options: TrialOptions) -> Result<TrialSummary, String> {
    validate_trial_options(&options)?;
    fs::create_dir_all(&options.output_dir).map_err(|error| {
        format!(
            "cannot create trial output {}: {error}",
            options.output_dir.display()
        )
    })?;
    let artifact = match execute_trial(&options).await {
        Ok(artifact) => artifact,
        Err(error) => {
            let failure_path = options.output_dir.join("failure.txt");
            let _ = write_create_new(&failure_path, format!("{error}\n").as_bytes());
            return Err(error);
        }
    };
    if let Err(error) = artifacts::write_trial_artifacts(&options.output_dir, &artifact)
        .and_then(|_| artifacts::validate_trial_artifacts(&options.output_dir))
    {
        let failure_path = options.output_dir.join("failure.txt");
        let _ = write_create_new(
            &failure_path,
            format!("artifact validation failed: {error}\n").as_bytes(),
        );
        return Err(error);
    }
    Ok(artifact.summary)
}

fn validate_trial_options(options: &TrialOptions) -> Result<(), String> {
    if !SHARD_COUNTS.contains(&options.shards) {
        return Err("trial shard count must be exactly 2 or 4".to_owned());
    }
    if options.concurrency != 4 && options.concurrency != 8 {
        return Err("trial index concurrency must be 4 or 8".to_owned());
    }
    if options.users == 0 || options.users % options.shards != 0 {
        return Err("trial user count must be positive and divisible by shard count".to_owned());
    }
    if options.requests_per_user == 0 || options.requests_per_user % 200 != 0 {
        return Err(
            "requests per user must be a positive multiple of the fixed 200-request mix".to_owned(),
        );
    }
    #[cfg(test)]
    if let Some(plan) = &options.cross_shard_fault {
        if plan.blocked_shard >= options.shards
            || plan.failed_shard >= options.shards
            || plan.blocked_shard == plan.failed_shard
        {
            return Err("cross-shard fault plan requires two distinct valid shards".to_owned());
        }
    }
    Ok(())
}

async fn prepare_trial(
    options: &TrialOptions,
    scratch: &mut OwnedScratch,
) -> Result<PreparedTrial, String> {
    let formal = options.users == FORMAL_USERS
        && options.requests_per_user == FORMAL_REQUESTS_PER_USER
        && options.preflight_observation >= Duration::from_secs(3);
    let preflight = resource_preflight(
        options.users,
        options.requests_per_user,
        !formal,
        options.preflight_observation,
    );
    let setup_preflight = ledger_preflight::ensure_idle(scratch.path.as_path(), &preflight)
        .map_err(|error| format!("pre-setup resource check failed: {error}"))?;
    let account_topology = topology(options.shards, options.users)?;
    let budgets = budget_for(options.shards, options.layout)?;
    let database_paths = match options.layout {
        RocksLayout::Shared => vec![scratch.path.join("shared-db")],
        RocksLayout::Dedicated => (0..options.shards)
            .map(|shard| scratch.path.join(format!("shard-{shard:02}-db")))
            .collect(),
    };
    let mut rocks_env = Env::new()
        .map_err(|error| format!("cannot create RocksDB default Env wrapper: {error}"))?;
    rocks_env.set_low_priority_background_threads(ROCKSDB_LOW_PRIORITY_THREADS);
    rocks_env.set_high_priority_background_threads(ROCKSDB_HIGH_PRIORITY_THREADS);
    let seed_time = ledger_time_boundary::unix_time_micros().saturating_sub(30_000_000);
    let mut databases = Vec::with_capacity(database_paths.len());
    let mut shards = Vec::with_capacity(options.shards);
    let mut measurement_preflight = preflight;
    measurement_preflight.min_available_mem_bytes = if formal {
        (options.users as u64)
            .saturating_mul(options.requests_per_user as u64 + SEED_TRANSACTIONS_PER_USER as u64)
            .saturating_mul(ESTIMATED_DESTINATION_BYTES_PER_RECORD)
            .saturating_add(DEFAULT_MEMORY_RESERVE_BYTES)
    } else {
        (options.users as u64)
            .saturating_mul(options.requests_per_user as u64 + SEED_TRANSACTIONS_PER_USER as u64)
            .saturating_mul(ESTIMATED_DESTINATION_BYTES_PER_RECORD)
            .saturating_add(128 * 1024 * 1024)
    };
    measurement_preflight.min_free_bytes = if formal {
        (options.users as u64)
            .saturating_mul(options.requests_per_user as u64 + SEED_TRANSACTIONS_PER_USER as u64)
            .saturating_mul(ESTIMATED_DISK_BYTES_PER_RECORD)
            .saturating_add(DEFAULT_FREE_SPACE_RESERVE_BYTES)
    } else {
        (options.users as u64)
            .saturating_mul(options.requests_per_user as u64 + SEED_TRANSACTIONS_PER_USER as u64)
            .saturating_mul(ESTIMATED_DISK_BYTES_PER_RECORD)
            .saturating_add(256 * 1024 * 1024)
    };
    scratch.mark_workload_started();
    let setup_result = async {
        for (path, budget) in database_paths.iter().cloned().zip(budgets.iter().cloned()) {
            let (db, options_handle) =
                AccountStore::open_database_with_budget(&path, budget.clone())
                    .await
                    .map_err(|error| format!("cannot open RocksDB {}: {error}", path.display()))?;
            databases.push(DbHandle {
                path,
                db,
                options: options_handle,
                budget,
            });
        }

        for shard_id in 0..options.shards {
            let db_index = match options.layout {
                RocksLayout::Shared => 0,
                RocksLayout::Dedicated => shard_id,
            };
            let destination_capacity = account_topology[shard_id]
                .len()
                .checked_mul(options.requests_per_user + SEED_TRANSACTIONS_PER_USER)
                .ok_or_else(|| "mock destination capacity overflowed".to_owned())?;
            let destination = Arc::new(
                MockProjectionStore::with_capacity(destination_capacity).map_err(|error| {
                    format!("shard {shard_id} destination allocation failed: {error}")
                })?,
            );
            let db = Arc::clone(&databases[db_index].db);
            let options_handle = Arc::clone(&databases[db_index].options);
            let source = AccountStore::open_on_database(
                db,
                options_handle,
                account_topology[shard_id].clone(),
                shard_id as u32,
                BalanceMode::PerBatch,
                DEFAULT_CHECKPOINT_QUANTITY,
            )
            .await
            .map_err(|error| format!("shard {shard_id} store open failed: {error}"))?;
            let seed_sequence = u64::try_from(account_topology[shard_id].len())
                .unwrap_or(u64::MAX)
                .checked_mul(SEED_TRANSACTIONS_PER_USER as u64)
                .ok_or_else(|| "shard seed sequence overflowed".to_owned())?;
            let initial_watermark = seed_time.saturating_add(1);
            let progress = ProjectionProgress::new(seed_sequence);
            let gate = AdmissionGate::new(initial_watermark);
            let manager = Arc::new(WatermarkManager::new(
                Arc::clone(&gate),
                Arc::clone(&progress),
            ));
            let watermark_metrics = manager.metrics();
            shards.push(ShardRuntime {
                id: shard_id,
                db_index,
                account_ids: account_topology[shard_id].clone(),
                source: Some(source),
                destination,
                progress,
                gate,
                manager,
                watermark_metrics,
                projection_samples: Arc::new(Mutex::new(Vec::new())),
                gc_samples: Arc::new(Mutex::new(Vec::new())),
                index_batches: Arc::new(Mutex::new(Vec::new())),
                seed_sequence,
                queue: None,
                worker: None,
                committed_rx: None,
                projector: None,
                watermark_task: None,
                gc_task: None,
            });

            let shard = &shards[shard_id];
            let source = shard
                .source
                .as_ref()
                .expect("source retained before seeding");
            if source.namespace_id() != Some(shard_id as u32)
                || source.account_ids() != account_topology[shard_id].as_slice()
            {
                return Err(format!(
                    "shard {shard_id} namespace or account routing differs from topology"
                ));
            }
            let refund_history: Arc<dyn RefundHistory> =
                Arc::clone(&shard.destination) as Arc<dyn RefundHistory>;
            source.set_refund_history(refund_history).map_err(|error| {
                format!("shard {shard_id} historical destination install failed: {error}")
            })?;
            seed_accounts(
                source,
                &account_topology[shard_id],
                DEFAULT_BATCH_SIZE,
                seed_time,
            )
            .await
            .map_err(|error| format!("shard {shard_id} seed failed: {error}"))?;
            source.drain_checkpoints().await.map_err(|error| {
                format!("shard {shard_id} seed checkpoint barrier failed: {error}")
            })?;
            if source.latest_seq() != seed_sequence {
                return Err(format!(
                    "shard {shard_id} seeded sequence {} differs from expected {seed_sequence}",
                    source.latest_seq()
                ));
            }
            project_seed(
                source,
                &shard.destination,
                seed_sequence,
                DEFAULT_PROJECTION_BATCH_SIZE,
            )
            .await
            .map_err(|error| format!("shard {shard_id} seed projection failed: {error}"))?;
            source
                .persist_projected_before_durable(initial_watermark, seed_sequence)
                .await
                .map_err(|error| {
                    format!("shard {shard_id} initial durable boundary failed: {error}")
                })?;
        }
        let measure_preflight =
            ledger_preflight::ensure_idle(scratch.path.as_path(), &measurement_preflight)
                .map_err(|error| format!("pre-measurement resource check failed: {error}"))?;
        let initial_rocks = initial_rocks_for_databases(&shards, &databases)?;
        let initial_io = ledger_preflight::sample_io(&scratch.path)
            .map_err(|error| format!("cannot sample initial global process/device I/O: {error}"))?;
        Ok::<_, String>((measure_preflight, initial_rocks, initial_io))
    }
    .await;
    match setup_result {
        Ok((measure_preflight, initial_rocks, initial_io)) => Ok(PreparedTrial {
            topology: account_topology,
            rocks_env,
            databases,
            shards,
            initial_rocks,
            initial_io,
            seed_time,
            setup_preflight,
            measure_preflight,
        }),
        Err(error) => {
            let source_errors = close_sources(&mut shards).await;
            databases.clear();
            Err(include_teardown_errors(error, source_errors))
        }
    }
}

async fn execute_trial(options: &TrialOptions) -> Result<TrialArtifacts, String> {
    validate_trial_options(options)?;
    let scratch_path = options.output_dir.join("owned-scratch");
    let mut scratch = OwnedScratch::create(scratch_path.clone())?;
    // Open/recovery/seeding use Tokio's blocking pool. If this future is
    // externally cancelled during any native operation, Drop retains scratch.
    scratch.mark_workload_started();
    let prepared = match prepare_trial(options, &mut scratch).await {
        Ok(prepared) => prepared,
        Err(error) => {
            scratch.allow_cleanup_after_all_handles_closed();
            let cleanup = scratch.cleanup();
            return match cleanup {
                Ok(()) => Err(error),
                Err(cleanup_error) => {
                    Err(format!("{error}; scratch cleanup failed: {cleanup_error}"))
                }
            };
        }
    };
    execute_prepared_trial(options, scratch, prepared).await
}

async fn execute_prepared_trial(
    options: &TrialOptions,
    mut scratch: OwnedScratch,
    mut prepared: PreparedTrial,
) -> Result<TrialArtifacts, String> {
    let (background_failure, background_rx) = watch::channel(None::<String>);
    let (shutdown, shutdown_rx) = watch::channel(false);
    let lookup = IndexLookupConfig::new(IndexLookupMode::Chunked {
        group_size: 256,
        max_in_flight: options.concurrency,
    })
    .expect("validated Chunked lookup configuration");
    if prepared.shards.iter().any(|shard| shard.source.is_none()) {
        return Err("prepared trial has a missing shard store".to_owned());
    }
    for shard in prepared.shards.iter_mut() {
        let source = shard.source.as_ref().expect("source present").clone();
        let (committed_head, committed_rx) = watch::channel(shard.seed_sequence);
        let index_metrics = Arc::clone(&shard.index_batches);
        match ledger_time_boundary::spawn_commit_queue_with_index_lookup(
            source,
            committed_head,
            options.users / options.shards,
            DEFAULT_BATCH_SIZE,
            Duration::from_millis(DEFAULT_BATCH_TIMEOUT_MS),
            lookup,
            index_metrics,
        ) {
            Ok((queue, worker)) => {
                shard.queue = Some(queue);
                shard.worker = Some(worker);
                shard.committed_rx = Some(committed_rx);
            }
            Err(error) => {
                let teardown =
                    drain_shards("commit queue setup", &mut prepared.shards, shutdown.clone())
                        .await;
                let source_errors = close_sources(&mut prepared.shards).await;
                drop(prepared);
                scratch.allow_cleanup_after_all_handles_closed();
                let cleanup = scratch.cleanup();
                return Err(include_teardown_errors(
                    format!("commit queue setup failed: {error}"),
                    teardown
                        .into_iter()
                        .chain(source_errors)
                        .chain(cleanup.err())
                        .collect(),
                ));
            }
        }
    }
    let start_barrier = Arc::new(Barrier::new(options.users + 1 + options.shards * 3));
    let cpu_started = Arc::new(Mutex::new(ProcessTime::now()));
    for shard in prepared.shards.iter_mut() {
        let source = shard.source.as_ref().expect("source present").clone();
        let committed_rx = shard
            .committed_rx
            .take()
            .expect("commit-head receiver stored with every queue");
        shard.projector = Some(spawn_durable_projector(
            source.clone(),
            Arc::clone(&shard.destination),
            Arc::clone(&shard.progress),
            Arc::clone(&shard.projection_samples),
            background_failure.clone(),
            committed_rx,
            shutdown_rx.clone(),
            DEFAULT_PROJECTION_BATCH_SIZE,
            Arc::clone(&start_barrier),
        ));
        shard.watermark_task = Some(spawn_watermark_manager(
            Arc::clone(&shard.manager),
            source.clone(),
            Duration::from_millis(DEFAULT_RETENTION_MS),
            Duration::from_millis(DEFAULT_WATERMARK_INTERVAL_MS),
            shutdown_rx.clone(),
            background_failure.clone(),
            Arc::clone(&start_barrier),
        ));
        shard.gc_task = Some(spawn_gc_worker(
            source,
            DEFAULT_GC_BATCH_SIZE,
            Duration::from_millis(DEFAULT_GC_INTERVAL_MS),
            shutdown_rx.clone(),
            background_failure.clone(),
            Arc::clone(&shard.gc_samples),
            Arc::clone(&start_barrier),
        ));
    }
    #[cfg(test)]
    let mut fault_trigger = if let Some(fault) = &options.cross_shard_fault {
        let blocked_source = prepared.shards[fault.blocked_shard]
            .source
            .as_ref()
            .expect("source present");
        let (started_rx, release) =
            blocked_source.block_index_lookup_group_for_test(fault.group_index);
        fault.control.set_release(release);
        let failed_source = prepared.shards[fault.failed_shard]
            .source
            .as_ref()
            .expect("source present")
            .clone();
        let fault = fault.clone();
        Some(tokio::task::spawn_blocking(move || {
            started_rx
                .recv()
                .map_err(|error| format!("blocked query did not start: {error}"))?;
            fault.control.mark_blocked();
            failed_source.fail_index_lookup_group_for_test(fault.group_index);
            Ok::<_, String>(())
        }))
    } else {
        None
    };
    #[cfg(not(test))]
    let mut fault_trigger: Option<JoinHandle<Result<(), String>>> = None;

    scratch.mark_workload_started();
    let active_result = run_client_phase(
        options,
        &mut prepared.shards,
        &prepared.topology,
        prepared.seed_time,
        Arc::clone(&start_barrier),
        background_failure.clone(),
        background_rx.clone(),
        scratch.path.as_path(),
        &prepared.databases,
        &prepared.initial_rocks,
        prepared.initial_io.clone(),
        Arc::clone(&cpu_started),
    )
    .await;
    let fault_error = match fault_trigger.take() {
        Some(task) => match task.await {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(error),
            Err(error) => Some(format!("test fault coordinator panicked: {error}")),
        },
        None => None,
    };
    let active_result = match (active_result, fault_error) {
        (Ok(_), Some(error)) => Err(error),
        (Err(error), Some(fault_error)) => Err(format!(
            "{error}; test fault coordinator failed: {fault_error}"
        )),
        (result, None) => result,
    };

    let measurement = match active_result {
        Ok(measurement) => measurement,
        Err(error) => {
            fail_all_progress(&prepared.shards, &error);
            let teardown =
                drain_shards("failed workload", &mut prepared.shards, shutdown.clone()).await;
            let source_errors = close_sources(&mut prepared.shards).await;
            drop(prepared);
            scratch.allow_cleanup_after_all_handles_closed();
            let cleanup = scratch.cleanup();
            return Err(include_teardown_errors(
                error,
                teardown
                    .into_iter()
                    .chain(source_errors)
                    .chain(cleanup.err())
                    .collect(),
            ));
        }
    };
    let teardown = drain_shards("completed workload", &mut prepared.shards, shutdown.clone()).await;
    let background_error = background_rx.borrow().clone();
    let teardown = include_watch_failure(teardown, background_error);
    if !teardown.is_empty() {
        let source_errors = close_sources(&mut prepared.shards).await;
        drop(prepared);
        scratch.allow_cleanup_after_all_handles_closed();
        let cleanup = scratch.cleanup();
        return Err(include_teardown_errors(
            "pipeline worker shutdown failed".to_owned(),
            teardown
                .into_iter()
                .chain(source_errors)
                .chain(cleanup.err())
                .collect(),
        ));
    }
    let final_result = finalize_trial(options, &mut prepared, &measurement).await;
    let source_errors = close_sources(&mut prepared.shards).await;
    prepared.databases.clear();
    let final_result = if source_errors.is_empty() {
        final_result
    } else {
        match final_result {
            Ok(_) => Err(format!(
                "source shutdown failed: {}",
                source_errors.join("; ")
            )),
            Err(error) => Err(format!(
                "{error}; source shutdown failed: {}",
                source_errors.join("; ")
            )),
        }
    };
    drop(prepared);
    scratch.allow_cleanup_after_all_handles_closed();
    let cleanup = scratch.cleanup();
    match (final_result, cleanup) {
        (Ok(artifact), Ok(())) => Ok(artifact),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(cleanup_error)) => Err(cleanup_error),
        (Err(error), Err(cleanup_error)) => Err(format!("{error}; {cleanup_error}")),
    }
}

async fn drain_shards(
    name: &str,
    shards: &mut [ShardRuntime],
    shutdown: watch::Sender<bool>,
) -> Vec<String> {
    shutdown.send_replace(true);
    let mut errors = Vec::new();
    for shard in shards.iter_mut() {
        drop(shard.queue.take());
    }
    for shard in shards.iter_mut() {
        if let Some(worker) = shard.worker.take() {
            if let Err(error) = worker.join().await {
                errors.push(format!(
                    "{name}: shard {} commit worker failed: {error}",
                    shard.id
                ));
            }
        }
        if let Some(task) = shard.projector.take() {
            if let Err(error) = join_background(&format!("{name}/shard-{}", shard.id), task).await {
                errors.push(error);
            }
        }
        if let Some(task) = shard.watermark_task.take() {
            if let Err(error) = join_background(&format!("{name}/shard-{}", shard.id), task).await {
                errors.push(error);
            }
        }
        if let Some(task) = shard.gc_task.take() {
            if let Err(error) = join_background(&format!("{name}/shard-{}", shard.id), task).await {
                errors.push(error);
            }
        }
    }
    let _ = shutdown;
    errors
}

async fn close_sources(shards: &mut [ShardRuntime]) -> Vec<String> {
    let mut errors = Vec::new();
    for shard in shards.iter_mut() {
        let Some(source) = shard.source.take() else {
            continue;
        };
        if let Err(error) = source.shutdown().await {
            errors.push(format!(
                "shard {} source shutdown failed: {error}",
                shard.id
            ));
        }
    }
    errors
}

async fn finalize_trial(
    options: &TrialOptions,
    prepared: &mut PreparedTrial,
    measurement: &ClientMeasurement,
) -> Result<TrialArtifacts, String> {
    validate_client_counts(options, &prepared.shards, measurement)?;
    let sample_artifacts = build_sample_artifacts(options, &prepared.shards, measurement)?;
    let sample_stride = DEFAULT_SAMPLE_STRIDE;
    let expected_sample_count =
        expected_request_sample_count(options.users, options.requests_per_user, sample_stride)?;
    if sample_artifacts.requests.len() != expected_sample_count {
        return Err(format!(
            "deterministic request sample count {} differs from expected {expected_sample_count}",
            sample_artifacts.requests.len()
        ));
    }
    let databases = prepared
        .databases
        .iter()
        .map(|database| {
            (
                database
                    .path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("database")
                    .to_owned(),
                database.budget.clone(),
            )
        })
        .collect::<Vec<_>>();
    let database_paths = prepared
        .databases
        .iter()
        .map(|database| database.path.clone())
        .collect::<Vec<_>>();

    let mut final_sequences = Vec::with_capacity(options.shards);
    let mut final_boundaries = Vec::with_capacity(options.shards);
    let mut final_boundary_target_sequences = Vec::with_capacity(options.shards);
    let mut final_watermarks = Vec::with_capacity(options.shards);
    let mut final_gc_prefixes = Vec::with_capacity(options.shards);
    let final_candidate = ledger_time_boundary::unix_time_micros().saturating_sub(
        u64::try_from(Duration::from_millis(DEFAULT_RETENTION_MS).as_micros()).unwrap_or(u64::MAX),
    );
    for shard in prepared.shards.iter_mut() {
        let source = shard.source.as_ref().expect("source present");
        shard
            .manager
            .advance_once_durable(final_candidate, source)
            .await
            .map_err(|error| {
                format!(
                    "shard {} final durable boundary advance failed: {error}",
                    shard.id
                )
            })?;
        loop {
            let before = source.gc_prefix_seq();
            let outcome = source
                .collect_garbage(DEFAULT_GC_BATCH_SIZE)
                .await
                .map_err(|error| {
                    format!("shard {} final safe GC sweep failed: {error}", shard.id)
                })?;
            if outcome.gc_prefix_seq > before && outcome.blocked_at_seq.is_none() {
                tokio::task::yield_now().await;
                continue;
            }
            break;
        }
        let sequence = source.latest_seq();
        let projected = source
            .durable_projection_progress()
            .await
            .map_err(|error| {
                format!(
                    "shard {} final durable progress read failed: {error}",
                    shard.id
                )
            })?;
        let destination = shard.destination.progress();
        if sequence != projected || sequence != destination || source.projected_seq() != projected {
            return Err(format!(
                "shard {} final projection mismatch: latest={sequence} source_projected={} durable={projected} destination={destination}",
                shard.id,
                source.projected_seq()
            ));
        }
        let boundary = source
            .restored_projected_before()
            .await
            .map_err(|error| {
                format!(
                    "shard {} final durable boundary validation failed: {error}",
                    shard.id
                )
            })?
            .ok_or_else(|| {
                format!(
                    "shard {} final durable boundary metadata is absent",
                    shard.id
                )
            })?;
        let watermark = shard
            .gate
            .watermark()
            .map_err(|error| format!("shard {} final watermark read failed: {error}", shard.id))?;
        let prefix = source.gc_prefix_seq();
        if boundary.0 != watermark
            || boundary.1 > projected
            || prefix > boundary.1
            || prefix > sequence
        {
            return Err(format!(
                "shard {} final boundary/GC coverage invalid: boundary={boundary:?} watermark={watermark} projected={projected} gc_prefix={prefix}",
                shard.id
            ));
        }
        let expected_sequence = u64::try_from(shard.account_ids.len())
            .unwrap_or(u64::MAX)
            .checked_mul((options.requests_per_user + SEED_TRANSACTIONS_PER_USER) as u64)
            .ok_or_else(|| "expected shard sequence overflow".to_owned())?;
        if sequence != expected_sequence {
            return Err(format!(
                "shard {} final sequence {sequence} differs from expected {expected_sequence}",
                shard.id
            ));
        }
        verify_account_balances(source, &shard.account_ids, 100).map_err(|error| {
            format!(
                "shard {} final balance validation failed: {error}",
                shard.id
            )
        })?;
        verify_historical_accounts(&shard.destination, &shard.account_ids, prepared.seed_time)
            .map_err(|error| {
                format!(
                    "shard {} seeded history validation failed: {error}",
                    shard.id
                )
            })?;
        final_sequences.push(sequence);
        final_boundaries.push(boundary.0);
        final_boundary_target_sequences.push(boundary.1);
        final_watermarks.push(watermark);
        final_gc_prefixes.push(prefix);
    }
    let sequence_sum = final_sequences
        .iter()
        .try_fold(0_u64, |sum, sequence| sum.checked_add(*sequence))
        .ok_or_else(|| "aggregate ledger sequence overflow".to_owned())?;
    let expected_sequence_sum = (options.users as u64)
        .checked_mul((options.requests_per_user + SEED_TRANSACTIONS_PER_USER) as u64)
        .ok_or_else(|| "aggregate expected sequence overflow".to_owned())?;
    if sequence_sum != expected_sequence_sum {
        return Err(format!(
            "aggregate final sequence {sequence_sum} differs from expected {expected_sequence_sum}"
        ));
    }
    let settled_wall = measurement.settle_started.elapsed();

    let shard_backlogs = (0..options.shards)
        .map(|shard_id| {
            (
                shard_id,
                measurement.latest_by_shard[shard_id],
                measurement.projected_by_shard[shard_id],
                measurement.destination_by_shard[shard_id],
                measurement.gc_prefix_by_shard[shard_id],
            )
        })
        .collect::<Vec<_>>();
    let destinations = prepared
        .shards
        .iter()
        .map(|shard| Arc::clone(&shard.destination))
        .collect::<Vec<_>>();
    let expected_final = final_sequences.clone();
    let expected_prefixes = final_gc_prefixes.clone();
    let expected_boundaries = final_boundaries.clone();
    let expected_boundary_target_sequences = final_boundary_target_sequences.clone();

    let source_close_errors = close_sources(&mut prepared.shards).await;
    if !source_close_errors.is_empty() {
        return Err(format!(
            "pre-recovery source close failed: {}",
            source_close_errors.join("; ")
        ));
    }
    prepared.databases.clear();

    let recovery_started = Instant::now();
    for (path, budget) in database_paths
        .iter()
        .cloned()
        .zip(databases.iter().map(|(_, budget)| budget.clone()))
    {
        let (db, options_handle) = AccountStore::open_database_with_budget(&path, budget.clone())
            .await
            .map_err(|error| {
                format!(
                    "recovery database open failed for {}: {error}",
                    path.display()
                )
            })?;
        prepared.databases.push(DbHandle {
            path,
            db,
            options: options_handle,
            budget,
        });
    }
    for shard in prepared.shards.iter_mut() {
        let db_index = shard.db_index;
        let source = AccountStore::open_on_database(
            Arc::clone(&prepared.databases[db_index].db),
            Arc::clone(&prepared.databases[db_index].options),
            shard.account_ids.clone(),
            shard.id as u32,
            BalanceMode::PerBatch,
            DEFAULT_CHECKPOINT_QUANTITY,
        )
        .await
        .map_err(|error| format!("shard {} recovery failed: {error}", shard.id))?;
        let refund_history: Arc<dyn RefundHistory> = destinations[shard.id].clone();
        source
            .set_refund_history(refund_history)
            .map_err(|error| format!("shard {} destination reinstall failed: {error}", shard.id))?;
        shard.source = Some(source);
    }
    let recovery_seconds = recovery_started.elapsed().as_secs_f64();
    let integrity_started = Instant::now();
    for shard in prepared.shards.iter() {
        let source = shard.source.as_ref().expect("reopened source present");
        let sequence = expected_final[shard.id];
        if source.namespace_id() != Some(shard.id as u32)
            || source.account_ids() != shard.account_ids.as_slice()
            || source.latest_seq() != sequence
            || source.projected_seq() != sequence
            || source.gc_prefix_seq() != expected_prefixes[shard.id]
        {
            return Err(format!(
                "shard {} recovered namespace/account/sequence/prefix differs from validated state",
                shard.id
            ));
        }
        let durable = source
            .durable_projection_progress()
            .await
            .map_err(|error| {
                format!(
                    "shard {} recovered durable progress read failed: {error}",
                    shard.id
                )
            })?;
        if durable != sequence || shard.destination.progress() != sequence {
            return Err(format!(
                "shard {} recovered source/destination progress mismatch: seq={sequence} durable={durable} destination={}",
                shard.id,
                shard.destination.progress()
            ));
        }
        let boundary = source
            .restored_projected_before()
            .await
            .map_err(|error| {
                format!(
                    "shard {} recovered boundary validation failed: {error}",
                    shard.id
                )
            })?
            .ok_or_else(|| format!("shard {} recovered durable boundary is absent", shard.id))?;
        if boundary.0 != final_watermarks[shard.id]
            || boundary.0 != expected_boundaries[shard.id]
            || boundary.1 != expected_boundary_target_sequences[shard.id]
            || boundary.1 > durable
            || source.gc_prefix_seq() > boundary.1
        {
            return Err(format!(
                "shard {} recovered boundary {:?} does not cover GC prefix {} and durable sequence {durable}",
                shard.id,
                boundary,
                source.gc_prefix_seq()
            ));
        }
        source.validate_integrity().await.map_err(|error| {
            format!(
                "shard {} recovered namespace integrity scan failed: {error}",
                shard.id
            )
        })?;
        verify_account_balances(source, &shard.account_ids, 100).map_err(|error| {
            format!(
                "shard {} recovered balance validation failed: {error}",
                shard.id
            )
        })?;
        verify_historical_accounts(&shard.destination, &shard.account_ids, prepared.seed_time)
            .map_err(|error| {
                format!(
                    "shard {} recovered seeded history validation failed: {error}",
                    shard.id
                )
            })?;
    }
    let integrity_seconds = integrity_started.elapsed().as_secs_f64();
    let recovery_close_errors = close_sources(&mut prepared.shards).await;
    if !recovery_close_errors.is_empty() {
        return Err(format!(
            "recovered source close failed: {}",
            recovery_close_errors.join("; ")
        ));
    }
    prepared.databases.clear();
    let db_bytes = database_paths.iter().try_fold(0_u64, |sum, path| {
        read_db_bytes(path).and_then(|size| {
            sum.checked_add(size)
                .ok_or_else(|| "aggregate final database size overflow".to_owned())
        })
    })?;
    copy_effective_options(&options.output_dir, &database_paths)?;

    let shard_summaries = (0..options.shards)
        .map(|shard_id| ShardSummary {
            shard_id,
            account_count: prepared.topology[shard_id].len(),
            requests: measurement.per_shard_stats[shard_id].requests,
            credits: measurement.per_shard_stats[shard_id].credits,
            debits: measurement.per_shard_stats[shard_id].debits,
            final_sequence: final_sequences[shard_id],
            projected_sequence: final_sequences[shard_id],
            destination_sequence: final_sequences[shard_id],
            gc_prefix: final_gc_prefixes[shard_id],
            final_boundary: final_boundaries[shard_id],
            final_boundary_target_sequence: final_boundary_target_sequences[shard_id],
        })
        .collect::<Vec<_>>();
    let total_requests = measurement.combined.requests;
    let summary = TrialSummary {
        requests: total_requests,
        credits: measurement.combined.credits,
        debits: measurement.combined.debits,
        final_sequence_sum: sequence_sum,
        client_wall: measurement.client_wall,
        settled_wall,
        shards: shard_summaries,
    };
    let cpu_core_equivalents =
        measurement.cpu_seconds / measurement.client_wall.as_secs_f64().max(f64::MIN_POSITIVE);
    Ok(TrialArtifacts {
        trial_name: options
            .output_dir
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("unknown-trial")
            .to_owned(),
        shards: options.shards,
        layout: options.layout,
        concurrency: options.concurrency,
        users: options.users,
        requests_per_user: options.requests_per_user,
        sample_stride,
        expected_request_sample_count: expected_sample_count,
        summary,
        client_wall: measurement.client_wall,
        client_end_elapsed_ns: elapsed_ns(
            measurement
                .client_end
                .duration_since(measurement.wall_started),
        ),
        cpu_seconds: measurement.cpu_seconds,
        cpu_core_equivalents,
        settled_wall,
        recovery_seconds,
        integrity_seconds,
        peak_rss_bytes: measurement.peak_rss_bytes,
        db_bytes,
        io: measurement.io.clone(),
        cpu_sample_offset_us: measurement.cpu_sample_offset_us,
        progress_sample_offset_us: measurement.progress_sample_offset_us,
        io_sample_offset_us: measurement.io_sample_offset_us,
        storage_sample_offset_us: measurement.storage_sample_offset_us,
        rocks: measurement.rocks,
        stages: sample_artifacts.stages,
        request_samples: sample_artifacts.requests,
        background: sample_artifacts.background,
        setup_preflight: prepared.setup_preflight.clone(),
        measure_preflight: prepared.measure_preflight.clone(),
        databases,
        shard_backlogs,
        final_boundary_by_shard: final_boundaries,
        final_boundary_target_sequence: final_boundary_target_sequences,
        index_batch_rows: sample_artifacts.index_batch_rows,
        index_group_rows: sample_artifacts.index_group_rows,
        max_in_flight_per_batch: sample_artifacts.max_in_flight_per_batch,
        max_running_query_jobs_per_batch: sample_artifacts.max_running_query_jobs_per_batch,
    })
}

fn validate_client_counts(
    options: &TrialOptions,
    shards: &[ShardRuntime],
    measurement: &ClientMeasurement,
) -> Result<(), String> {
    let expected_requests = (options.users as u64)
        .checked_mul(options.requests_per_user as u64)
        .ok_or_else(|| "expected request count overflow".to_owned())?;
    if measurement.combined.requests != expected_requests
        || measurement.combined.fresh != expected_requests
        || measurement.combined.credits != expected_requests / 2
        || measurement.combined.debits != expected_requests / 2
        || measurement.combined.historical_hits != 0
        || measurement.combined.historical_misses != 0
    {
        return Err(format!(
            "global request count mismatch: requests={} fresh={} credits={} debits={} hits={} misses={} expected={expected_requests}",
            measurement.combined.requests,
            measurement.combined.fresh,
            measurement.combined.credits,
            measurement.combined.debits,
            measurement.combined.historical_hits,
            measurement.combined.historical_misses
        ));
    }
    for shard in shards {
        let stats = &measurement.per_shard_stats[shard.id];
        let expected = (shard.account_ids.len() as u64)
            .checked_mul(options.requests_per_user as u64)
            .ok_or_else(|| "expected shard request count overflow".to_owned())?;
        if stats.requests != expected
            || stats.fresh != expected
            || stats.credits != expected / 2
            || stats.debits != expected / 2
            || stats.historical_hits != 0
            || stats.historical_misses != 0
            || measurement.latest_by_shard[shard.id]
                != (shard.account_ids.len() as u64)
                    .saturating_mul((options.requests_per_user + SEED_TRANSACTIONS_PER_USER) as u64)
        {
            return Err(format!(
                "shard {} request or sequence counts differ from expected {expected}",
                shard.id
            ));
        }
        let committed = measurement.index_batches_by_shard[shard.id]
            .iter()
            .map(|batch| batch.transaction_count as u64)
            .sum::<u64>();
        if committed != expected {
            return Err(format!(
                "shard {} foreground batch transaction count {committed} differs from {expected}",
                shard.id
            ));
        }
    }
    Ok(())
}

fn expected_request_sample_count(
    users: usize,
    requests_per_user: usize,
    stride: u64,
) -> Result<usize, String> {
    let total = users
        .checked_mul(requests_per_user)
        .ok_or_else(|| "request sample domain overflow".to_owned())?;
    let count = (0..total as u64)
        .filter(|logical_id| splitmix64(*logical_id) % stride == 0)
        .count();
    Ok(count)
}

fn copy_effective_options(output_dir: &Path, database_paths: &[PathBuf]) -> Result<(), String> {
    let options_dir = output_dir.join("rocksdb-options");
    fs::create_dir(&options_dir).map_err(|error| {
        format!(
            "cannot create RocksDB options archive {}: {error}",
            options_dir.display()
        )
    })?;
    for (database_index, database_path) in database_paths.iter().enumerate() {
        let entries = fs::read_dir(database_path).map_err(|error| {
            format!(
                "cannot list effective options under {}: {error}",
                database_path.display()
            )
        })?;
        let mut option_files = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|error| {
                format!(
                    "cannot read RocksDB directory entry under {}: {error}",
                    database_path.display()
                )
            })?;
            let path = entry.path();
            if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("OPTIONS-"))
            {
                option_files.push(path);
            }
        }
        option_files.sort();
        if option_files.is_empty() {
            return Err(format!(
                "database {} has no RocksDB OPTIONS file",
                database_path.display()
            ));
        }
        for path in option_files {
            let file_name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("OPTIONS");
            let target = options_dir.join(format!("db-{database_index:02}-{file_name}"));
            let bytes = fs::read(&path)
                .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
            write_create_new(&target, &bytes)?;
        }
    }
    Ok(())
}

struct SampleArtifacts {
    requests: Vec<RequestSample>,
    stages: Vec<RawStage>,
    background: Vec<RawBackground>,
    index_batch_rows: Vec<String>,
    index_group_rows: Vec<String>,
    max_in_flight_per_batch: usize,
    max_running_query_jobs_per_batch: usize,
}

fn build_sample_artifacts(
    options: &TrialOptions,
    shards: &[ShardRuntime],
    measurement: &ClientMeasurement,
) -> Result<SampleArtifacts, String> {
    let mut requests = Vec::new();
    let mut stages = Vec::new();
    let mut background = Vec::new();
    let mut index_batch_rows = Vec::new();
    let mut index_group_rows = Vec::new();
    let mut max_in_flight_per_batch = 0;
    let mut max_running_query_jobs_per_batch = 0;

    for (shard_id, stats) in measurement.per_shard_stats.iter().enumerate() {
        let shard = &shards[shard_id];
        let samples = &stats.stages;
        let count = samples.logical_ids.len();
        let lengths = [
            samples.total.len(),
            samples.admission.len(),
            samples.enqueue.len(),
            samples.queue.len(),
            samples.batch.len(),
            samples.handler.len(),
            samples.response.len(),
        ];
        if lengths.iter().any(|length| *length != count) {
            return Err(format!(
                "shard {shard_id} request sample columns have different counts"
            ));
        }
        for position in 0..count {
            let logical_id = samples.logical_ids[position];
            requests.push(RequestSample {
                logical_id,
                shard_id,
                total_ns: samples.total[position],
                admission_ns: samples.admission[position],
                enqueue_ns: samples.enqueue[position],
                queue_ns: samples.queue[position],
                batch_ns: samples.batch[position],
                handler_ns: samples.handler[position],
                response_ns: samples.response[position],
            });
        }

        let batches = &measurement.index_batches_by_shard[shard_id];
        for (batch_index, batch) in batches.iter().enumerate() {
            if batch.mode
                != (IndexLookupMode::Chunked {
                    group_size: 256,
                    max_in_flight: options.concurrency,
                })
            {
                return Err(format!(
                    "shard {shard_id} batch {batch_index} used the wrong index lookup strategy"
                ));
            }
            let query_wall = batch
                .query_wall_ns
                .ok_or_else(|| "Chunked batch omitted query-wall timing".to_owned())?;
            let blocking_wait = batch
                .blocking_pool_wait_ns
                .ok_or_else(|| "Chunked batch omitted blocking-pool wait".to_owned())?;
            let submit_collect = batch
                .submit_to_collection_ns
                .ok_or_else(|| "Chunked batch omitted submit-to-collection timing".to_owned())?;
            index_batch_rows.push(
                [
                    shard_id.to_string(),
                    batch_index.to_string(),
                    batch.transaction_count.to_string(),
                    batch.dispatch_wait_ns.to_string(),
                    batch.batch_gate_wait_ns.to_string(),
                    batch.key_prep_ns.to_string(),
                    query_wall.to_string(),
                    blocking_wait.to_string(),
                    batch.native_get_ns.to_string(),
                    batch.decode_ns.to_string(),
                    submit_collect.to_string(),
                    batch.apply_submit_to_collection_ns.to_string(),
                    batch.apply_blocking_pool_wait_ns.to_string(),
                    batch.sequential_apply_build_ns.to_string(),
                    batch.sync_write_batch_ns.to_string(),
                    batch.memory_publish_ns.to_string(),
                    batch.get_calls.to_string(),
                    batch.keys_looked_up.to_string(),
                    batch.hits.to_string(),
                    batch.misses.to_string(),
                    batch.groups_submitted.to_string(),
                    batch.max_observed_in_flight_groups.to_string(),
                    batch.max_observed_running_query_jobs.to_string(),
                ]
                .join(","),
            );
            max_in_flight_per_batch =
                max_in_flight_per_batch.max(batch.max_observed_in_flight_groups);
            max_running_query_jobs_per_batch =
                max_running_query_jobs_per_batch.max(batch.max_observed_running_query_jobs);
            push_raw_stage(
                &mut stages,
                "index.dispatch_wait",
                shard_id,
                batch.dispatch_wait_ns,
                None,
            );
            push_raw_stage(
                &mut stages,
                "index.batch_gate_wait",
                shard_id,
                batch.batch_gate_wait_ns,
                None,
            );
            push_raw_stage(
                &mut stages,
                "index.key_prep",
                shard_id,
                batch.key_prep_ns,
                None,
            );
            push_raw_stage(&mut stages, "index.query_wall", shard_id, query_wall, None);
            push_raw_stage(
                &mut stages,
                "index.blocking_pool_wait",
                shard_id,
                blocking_wait,
                None,
            );
            push_raw_stage(
                &mut stages,
                "index.native_get",
                shard_id,
                batch.native_get_ns,
                None,
            );
            push_raw_stage(&mut stages, "index.decode", shard_id, batch.decode_ns, None);
            push_raw_stage(
                &mut stages,
                "index.submit_to_collection",
                shard_id,
                submit_collect,
                None,
            );
            push_raw_stage(
                &mut stages,
                "index.apply_submit_to_collection",
                shard_id,
                batch.apply_submit_to_collection_ns,
                None,
            );
            push_raw_stage(
                &mut stages,
                "index.apply_blocking_pool_wait",
                shard_id,
                batch.apply_blocking_pool_wait_ns,
                None,
            );
            push_raw_stage(
                &mut stages,
                "index.sequential_apply_build",
                shard_id,
                batch.sequential_apply_build_ns,
                None,
            );
            push_raw_stage(
                &mut stages,
                "index.sync_write_batch",
                shard_id,
                batch.sync_write_batch_ns,
                None,
            );
            push_raw_stage(
                &mut stages,
                "index.memory_publish",
                shard_id,
                batch.memory_publish_ns,
                None,
            );
            for group in &batch.groups {
                index_group_rows.push(
                    [
                        shard_id.to_string(),
                        batch_index.to_string(),
                        group.group_index.to_string(),
                        group.first_position.to_string(),
                        group.key_count.to_string(),
                        group.blocking_pool_wait_ns.to_string(),
                        group.native_get_ns.to_string(),
                        group.decode_ns.to_string(),
                        group.submit_to_collection_ns.to_string(),
                    ]
                    .join(","),
                );
                push_raw_stage(
                    &mut stages,
                    "index.group_blocking_pool_wait",
                    shard_id,
                    group.blocking_pool_wait_ns,
                    None,
                );
                push_raw_stage(
                    &mut stages,
                    "index.group_native_get",
                    shard_id,
                    group.native_get_ns,
                    None,
                );
                push_raw_stage(
                    &mut stages,
                    "index.group_decode",
                    shard_id,
                    group.decode_ns,
                    None,
                );
                push_raw_stage(
                    &mut stages,
                    "index.group_submit_to_collection",
                    shard_id,
                    group.submit_to_collection_ns,
                    None,
                );
            }
        }

        let projection_samples = shard
            .projection_samples
            .lock()
            .map_err(|_| format!("shard {shard_id} projection metrics mutex poisoned"))?
            .iter()
            .copied()
            .filter(|sample| sample.completed_at <= measurement.client_end)
            .collect::<Vec<_>>();
        for sample in projection_samples {
            push_raw_stage(
                &mut stages,
                "projection.read",
                shard_id,
                sample.read_ns,
                Some(sample.completed_at),
            );
            push_raw_stage(
                &mut stages,
                "projection.apply",
                shard_id,
                sample.apply_ns,
                Some(sample.completed_at),
            );
            push_raw_stage(
                &mut stages,
                "projection.progress_sync",
                shard_id,
                sample.progress_sync_ns,
                Some(sample.completed_at),
            );
            push_raw_stage(
                &mut stages,
                "projection.total",
                shard_id,
                sample.total_ns,
                Some(sample.completed_at),
            );
            background.push(RawBackground {
                shard_id,
                event: "projection".to_owned(),
                completed_at: sample.completed_at,
                elapsed_ns: elapsed_ns(
                    sample.completed_at.duration_since(measurement.wall_started),
                ),
                sequence: sample.sequence,
                records: sample.records as u64,
                read_ns: sample.read_ns,
                apply_ns: sample.apply_ns,
                progress_sync_ns: sample.progress_sync_ns,
                total_ns: sample.total_ns,
                scanned: 0,
                deleted: 0,
                bytes_deleted: 0,
                gc_prefix_seq: 0,
                blocked_at_seq: None,
                gc_scan_ns: 0,
                gc_delete_ns: 0,
                gc_write_ns: 0,
                watermark_target_sequence: 0,
                watermark: 0,
                watermark_fence_wait_ns: 0,
                watermark_projection_wait_ns: 0,
                watermark_persist_ns: 0,
            });
        }
        let gc_samples = shard
            .gc_samples
            .lock()
            .map_err(|_| format!("shard {shard_id} GC metrics mutex poisoned"))?
            .iter()
            .copied()
            .filter(|sample| sample.completed_at <= measurement.client_end)
            .collect::<Vec<_>>();
        for sample in gc_samples {
            let outcome = sample.outcome;
            push_raw_stage(
                &mut stages,
                "gc.scan",
                shard_id,
                outcome.scan_ns,
                Some(sample.completed_at),
            );
            push_raw_stage(
                &mut stages,
                "gc.delete_build",
                shard_id,
                outcome.delete_ns,
                Some(sample.completed_at),
            );
            push_raw_stage(
                &mut stages,
                "gc.sync_write",
                shard_id,
                outcome.write_ns,
                Some(sample.completed_at),
            );
            push_raw_stage(
                &mut stages,
                "gc.total",
                shard_id,
                sample.duration_ns,
                Some(sample.completed_at),
            );
            background.push(RawBackground {
                shard_id,
                event: "gc".to_owned(),
                completed_at: sample.completed_at,
                elapsed_ns: elapsed_ns(
                    sample.completed_at.duration_since(measurement.wall_started),
                ),
                sequence: outcome.gc_prefix_seq,
                records: outcome.scanned,
                read_ns: 0,
                apply_ns: 0,
                progress_sync_ns: 0,
                total_ns: sample.duration_ns,
                scanned: outcome.scanned,
                deleted: outcome.deleted,
                bytes_deleted: outcome.bytes_deleted,
                gc_prefix_seq: outcome.gc_prefix_seq,
                blocked_at_seq: outcome.blocked_at_seq,
                gc_scan_ns: outcome.scan_ns,
                gc_delete_ns: outcome.delete_ns,
                gc_write_ns: outcome.write_ns,
                watermark_target_sequence: 0,
                watermark: 0,
                watermark_fence_wait_ns: 0,
                watermark_projection_wait_ns: 0,
                watermark_persist_ns: 0,
            });
        }
        let watermark_samples = shard
            .watermark_metrics
            .snapshot()
            .map_err(|error| format!("shard {shard_id} watermark metrics read failed: {error}"))?
            .into_iter()
            .filter(|sample| sample.completed_at <= measurement.client_end)
            .collect::<Vec<_>>();
        for sample in watermark_samples {
            push_raw_stage(
                &mut stages,
                "watermark.fence_wait",
                shard_id,
                sample.fence_wait_ns,
                Some(sample.completed_at),
            );
            push_raw_stage(
                &mut stages,
                "watermark.projection_wait",
                shard_id,
                sample.projection_wait_ns,
                Some(sample.completed_at),
            );
            push_raw_stage(
                &mut stages,
                "watermark.persist",
                shard_id,
                sample.persist_ns,
                Some(sample.completed_at),
            );
            push_raw_stage(
                &mut stages,
                "watermark.total",
                shard_id,
                sample.total_ns,
                Some(sample.completed_at),
            );
            background.push(RawBackground {
                shard_id,
                event: "watermark".to_owned(),
                completed_at: sample.completed_at,
                elapsed_ns: elapsed_ns(
                    sample.completed_at.duration_since(measurement.wall_started),
                ),
                sequence: sample.target_sequence,
                records: 0,
                read_ns: 0,
                apply_ns: 0,
                progress_sync_ns: 0,
                total_ns: sample.total_ns,
                scanned: 0,
                deleted: 0,
                bytes_deleted: 0,
                gc_prefix_seq: 0,
                blocked_at_seq: None,
                gc_scan_ns: 0,
                gc_delete_ns: 0,
                gc_write_ns: 0,
                watermark_target_sequence: sample.target_sequence,
                watermark: sample.published_watermark,
                watermark_fence_wait_ns: sample.fence_wait_ns,
                watermark_projection_wait_ns: sample.projection_wait_ns,
                watermark_persist_ns: sample.persist_ns,
            });
        }
    }
    requests.sort_unstable_by_key(|sample| sample.logical_id);
    background.sort_unstable_by_key(|sample| (sample.elapsed_ns, sample.shard_id));
    Ok(SampleArtifacts {
        requests,
        stages,
        background,
        index_batch_rows,
        index_group_rows,
        max_in_flight_per_batch,
        max_running_query_jobs_per_batch,
    })
}

fn push_raw_stage(
    stages: &mut Vec<RawStage>,
    metric: &str,
    shard_id: usize,
    value_ns: u64,
    completed_at: Option<Instant>,
) {
    stages.push(RawStage {
        metric: metric.to_owned(),
        shard_id,
        logical_id: None,
        value_ns,
        completed_at,
    });
}

fn initial_rocks_for_databases(
    shards: &[ShardRuntime],
    databases: &[DbHandle],
) -> Result<Vec<RocksDbCounters>, String> {
    let assignments = shards
        .iter()
        .map(|shard| shard.db_index)
        .collect::<Vec<_>>();
    database_representatives(&assignments, databases.len()).map(|representatives| {
        representatives
            .into_iter()
            .map(|shard_id| {
                shards[shard_id]
                    .source
                    .as_ref()
                    .expect("source present")
                    .rocksdb_stats()
            })
            .collect()
    })
}
