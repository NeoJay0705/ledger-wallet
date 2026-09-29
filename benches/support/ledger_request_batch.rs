//! Standalone closed-loop benchmark for the generic single-worker batch queue.

use crate::ledger_preflight::{IoSample, PreflightConfig, PreflightReport};
use crate::request_batch_queue::{self, BatchQueue};
use cpu_time::ProcessTime;
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Builder;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;

const DEFAULT_ITERATIONS: u64 = 10_000_000;
const DEFAULT_REPETITIONS: usize = 1;
const DEFAULT_USERS: usize = 50_000;
const DEFAULT_COROUTINES: usize = 50_000;
const DEFAULT_SAMPLE_STRIDE: u64 = 1_024;
const QUEUE_CAPACITY: usize = 50_000;
const DEFAULT_BATCH_SIZES: &[usize] = &[2_048];
const DEFAULT_TIMEOUTS_MS: &[u64] = &[5];
const SUPPORTED_BATCH_SIZES: &[usize] = &[2_048, 4_096];
const SUPPORTED_TIMEOUTS_MS: &[u64] = &[1, 5, 10, 20];
const RUNTIME_WORKERS: usize = 3;
const PREFLIGHT_BASE_MEM_BYTES: u64 = 128 * 1024 * 1024;
const PREFLIGHT_FREE_RESERVE_BYTES: u64 = 1024 * 1024 * 1024;

#[derive(Clone)]
struct Config {
    iterations: u64,
    repetitions: usize,
    users: usize,
    coroutines: usize,
    sample_stride: u64,
    batch_sizes: Vec<usize>,
    timeouts_ms: Vec<u64>,
    csv_path: PathBuf,
    preflight_path: PathBuf,
    preflight_observation_ms: u64,
    preflight_timeout_ms: u64,
    preflight_max_cpu_pct: f64,
    preflight_max_disk_busy_pct: f64,
    preflight_min_mem_bytes: u64,
    preflight_free_reserve_bytes: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            iterations: DEFAULT_ITERATIONS,
            repetitions: DEFAULT_REPETITIONS,
            users: DEFAULT_USERS,
            coroutines: DEFAULT_COROUTINES,
            sample_stride: DEFAULT_SAMPLE_STRIDE,
            batch_sizes: DEFAULT_BATCH_SIZES.to_vec(),
            timeouts_ms: DEFAULT_TIMEOUTS_MS.to_vec(),
            csv_path: PathBuf::from("target/ledger-request-batch-tokio.csv"),
            preflight_path: PathBuf::from("target/ledger-request-batch-preflight"),
            preflight_observation_ms: 3_000,
            preflight_timeout_ms: 60_000,
            preflight_max_cpu_pct: 10.0,
            preflight_max_disk_busy_pct: 5.0,
            preflight_min_mem_bytes: PREFLIGHT_BASE_MEM_BYTES,
            preflight_free_reserve_bytes: PREFLIGHT_FREE_RESERVE_BYTES,
        }
    }
}

#[derive(Clone, Copy)]
struct Payload {
    request_id: u64,
    user_id: usize,
}

#[derive(Clone, Copy)]
struct Reply {
    request_id: u64,
    opaque_result: u64,
}

struct IdVerifier {
    iterations: u64,
    seen: Vec<AtomicU64>,
}

struct ClientReport {
    completed: u64,
    totals_ns: [u128; 6],
    samples: Vec<[u64; 6]>,
    batch_request_counts: BTreeMap<(usize, &'static str), u64>,
    first_request_started_at: Option<Instant>,
    last_response_observed_at: Option<Instant>,
}

impl ClientReport {
    fn new() -> Self {
        Self {
            completed: 0,
            totals_ns: [0; 6],
            samples: Vec::new(),
            batch_request_counts: BTreeMap::new(),
            first_request_started_at: None,
            last_response_observed_at: None,
        }
    }
}

impl IdVerifier {
    fn new(iterations: u64) -> Result<Self, String> {
        let word_count = usize::try_from(iterations.div_ceil(64))
            .map_err(|_| "ID verifier is too large for this platform".to_owned())?;
        let mut seen = Vec::new();
        seen.try_reserve_exact(word_count)
            .map_err(|error| format!("cannot allocate ID verifier: {error}"))?;
        seen.extend((0..word_count).map(|_| AtomicU64::new(0)));
        Ok(Self { iterations, seen })
    }

    fn record(&self, request_id: u64) -> Result<(), String> {
        if request_id >= self.iterations {
            return Err(format!(
                "reply ID {request_id} is outside the requested range"
            ));
        }
        let word = (request_id / 64) as usize;
        let mask = 1_u64 << (request_id % 64);
        let previous = self.seen[word].fetch_or(mask, Ordering::Relaxed);
        if previous & mask != 0 {
            return Err(format!(
                "reply ID {request_id} was completed more than once"
            ));
        }
        Ok(())
    }

    fn validate(&self) -> Result<(), String> {
        for (word_index, word) in self.seen.iter().enumerate() {
            let first_id = word_index as u64 * 64;
            let remaining = self.iterations.saturating_sub(first_id);
            let expected = match remaining {
                0 => 0,
                1..=63 => (1_u64 << remaining) - 1,
                _ => u64::MAX,
            };
            let observed = word.load(Ordering::Relaxed);
            if observed != expected {
                return Err(format!(
                    "reply IDs are missing or duplicated in verifier word {word_index}: expected {expected:#018x}, found {observed:#018x}"
                ));
            }
        }
        Ok(())
    }
}

struct RunSummary {
    completed: u64,
    totals_ns: [u128; 6],
    samples: Vec<[u64; 6]>,
    batch_request_counts: BTreeMap<(usize, &'static str), u64>,
    first_request_started_at: Option<Instant>,
    last_response_observed_at: Option<Instant>,
}

impl RunSummary {
    fn new() -> Self {
        Self {
            completed: 0,
            totals_ns: [0; 6],
            samples: Vec::new(),
            batch_request_counts: BTreeMap::new(),
            first_request_started_at: None,
            last_response_observed_at: None,
        }
    }

    fn merge(&mut self, mut client: ClientReport) -> Result<(), String> {
        self.completed = self
            .completed
            .checked_add(client.completed)
            .ok_or_else(|| "completed request count overflowed".to_owned())?;
        for (total, client_total) in self.totals_ns.iter_mut().zip(client.totals_ns) {
            *total = total
                .checked_add(client_total)
                .ok_or_else(|| "latency nanosecond total overflowed".to_owned())?;
        }
        self.samples.append(&mut client.samples);
        for (key, count) in client.batch_request_counts {
            let entry = self.batch_request_counts.entry(key).or_default();
            *entry = entry
                .checked_add(count)
                .ok_or_else(|| "batch request count overflowed".to_owned())?;
        }
        if let Some(started_at) = client.first_request_started_at {
            self.first_request_started_at = Some(
                self.first_request_started_at
                    .map_or(started_at, |current| current.min(started_at)),
            );
        }
        if let Some(observed_at) = client.last_response_observed_at {
            self.last_response_observed_at = Some(
                self.last_response_observed_at
                    .map_or(observed_at, |current| current.max(observed_at)),
            );
        }
        Ok(())
    }

    fn batch_histograms(&self) -> Result<(BTreeMap<usize, u64>, [u64; 3]), String> {
        let mut by_size = BTreeMap::new();
        let mut by_reason = [0_u64; 3];
        let mut request_members = 0_u64;
        for (&(size, reason), &members) in &self.batch_request_counts {
            if size == 0 || members % size as u64 != 0 {
                return Err(format!(
                    "batch metadata has {members} request replies for batch size {size}"
                ));
            }
            let count = members / size as u64;
            let size_count = by_size.entry(size).or_insert(0_u64);
            *size_count = size_count
                .checked_add(count)
                .ok_or_else(|| "batch size count overflowed".to_owned())?;
            let reason_index = match reason {
                "size" => 0,
                "timeout" => 1,
                "channel_closed" => 2,
                other => return Err(format!("unknown batch flush reason {other}")),
            };
            by_reason[reason_index] = by_reason[reason_index]
                .checked_add(count)
                .ok_or_else(|| "batch flush count overflowed".to_owned())?;
            request_members = request_members
                .checked_add(members)
                .ok_or_else(|| "batch request member count overflowed".to_owned())?;
        }
        if request_members != self.completed {
            return Err(format!(
                "batch metadata covers {request_members} replies, expected {}",
                self.completed
            ));
        }
        Ok((by_size, by_reason))
    }
}

struct CaseResult {
    summary: RunSummary,
    request_wall: Duration,
    cpu_wall: Duration,
    process_cpu: Duration,
    io_delta: IoDelta,
}

#[derive(Default)]
struct IoDelta {
    process_rchar_bytes: u64,
    process_wchar_bytes: u64,
    process_read_bytes: u64,
    process_write_bytes: u64,
    target_read_bytes: u64,
    target_write_bytes: u64,
    target_busy_ms: u64,
}

#[derive(Clone, Copy)]
struct LatencyStats {
    mean_ns: f64,
    p50_ns: u64,
    p95_ns: u64,
    p99_ns: u64,
    count: u64,
    sample_count: u64,
}

pub fn run_from_args() -> Result<(), String> {
    let config = parse_args()?;
    validate_config(&config)?;
    if let Some(parent) = config
        .csv_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).map_err(|error| {
            format!("cannot create CSV directory {}: {error}", parent.display())
        })?;
    }
    fs::create_dir_all(&config.preflight_path).map_err(|error| {
        format!(
            "cannot create preflight path {}: {error}",
            config.preflight_path.display()
        )
    })?;

    let mut csv_file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&config.csv_path)
        .map_err(|error| format!("cannot create {}: {error}", config.csv_path.display()))?;
    let header = csv_header();
    println!("{header}");
    writeln!(csv_file, "{header}")
        .and_then(|_| csv_file.flush())
        .map_err(|error| format!("cannot write CSV header: {error}"))?;

    let runtime = Builder::new_multi_thread()
        .worker_threads(RUNTIME_WORKERS)
        .enable_time()
        .build()
        .map_err(|error| format!("cannot create Tokio runtime: {error}"))?;
    let total_cases = config
        .batch_sizes
        .len()
        .checked_mul(config.timeouts_ms.len())
        .and_then(|value| value.checked_mul(config.repetitions))
        .ok_or_else(|| "case count overflowed".to_owned())?;
    let mut case_index = 0_usize;

    for &batch_size in &config.batch_sizes {
        for &timeout_ms in &config.timeouts_ms {
            for repetition in 1..=config.repetitions {
                case_index += 1;
                let case_name = format!("b{batch_size}_t{timeout_ms}ms_r{repetition}");
                eprintln!(
                    "starting case {case_index}/{total_cases}: users={}, coroutines={}, iterations={}, batch_size={batch_size}, timeout_ms={timeout_ms}, repetition={repetition}",
                    config.users, config.coroutines, config.iterations
                );
                let preflight = run_preflight(&config, &case_name)?;
                let result = runtime.block_on(run_case(
                    &config,
                    batch_size,
                    timeout_ms,
                    &config.preflight_path,
                ))?;
                let row = csv_row(
                    &case_name, repetition, &config, batch_size, timeout_ms, &preflight, &result,
                )?;
                println!("{row}");
                writeln!(csv_file, "{row}")
                    .and_then(|_| csv_file.flush())
                    .map_err(|error| format!("cannot append CSV row: {error}"))?;
                eprintln!(
                    "finished case {case_index}/{total_cases}: completed={} batches={} flush_size={} flush_timeout={} flush_channel_closed={} rps={:.2}",
                    result.summary.completed,
                    result.summary.batch_histograms()?.0.values().sum::<u64>(),
                    result.summary.batch_histograms()?.1[0],
                    result.summary.batch_histograms()?.1[1],
                    result.summary.batch_histograms()?.1[2],
                    result.summary.completed as f64 / result.request_wall.as_secs_f64()
                );
            }
        }
    }
    eprintln!("csv={}", config.csv_path.display());
    Ok(())
}

fn parse_args() -> Result<Config, String> {
    let mut config = Config::default();
    let mut args = std::env::args().skip(1);
    while let Some(argument) = args.next() {
        if argument == "--bench" {
            continue;
        }
        if argument == "--help" || argument == "-h" {
            print_help();
            std::process::exit(0);
        }
        let (name, inline) = match argument.split_once('=') {
            Some((name, value)) => (name.to_owned(), Some(value.to_owned())),
            None => (argument, None),
        };
        let value = match inline {
            Some(value) => value,
            None => args
                .next()
                .ok_or_else(|| format!("{name} requires a value"))?,
        };
        match name.as_str() {
            "--iterations" => config.iterations = parse_value(&name, &value)?,
            "--repetitions" => config.repetitions = parse_value(&name, &value)?,
            "--users" => config.users = parse_value(&name, &value)?,
            "--coroutines" => config.coroutines = parse_value(&name, &value)?,
            "--sample-stride" => config.sample_stride = parse_value(&name, &value)?,
            "--batch-size" | "--batch-sizes" => config.batch_sizes = parse_batch_sizes(&value)?,
            "--batch-timeout-ms" | "--batch-timeouts-ms" => {
                config.timeouts_ms = parse_timeouts(&value)?
            }
            "--csv" => config.csv_path = PathBuf::from(value),
            "--preflight-path" => config.preflight_path = PathBuf::from(value),
            "--preflight-observation-ms" => {
                config.preflight_observation_ms = parse_value(&name, &value)?
            }
            "--preflight-timeout-ms" => config.preflight_timeout_ms = parse_value(&name, &value)?,
            "--preflight-max-cpu-pct" => config.preflight_max_cpu_pct = parse_value(&name, &value)?,
            "--preflight-max-disk-busy-pct" => {
                config.preflight_max_disk_busy_pct = parse_value(&name, &value)?
            }
            "--preflight-min-mem-bytes" => {
                config.preflight_min_mem_bytes = parse_value(&name, &value)?
            }
            "--preflight-free-reserve-bytes" => {
                config.preflight_free_reserve_bytes = parse_value(&name, &value)?
            }
            _ => return Err(format!("unknown option {name}; use --help")),
        }
    }
    Ok(config)
}

fn parse_value<T: std::str::FromStr>(name: &str, value: &str) -> Result<T, String>
where
    T::Err: std::fmt::Display,
{
    value
        .parse()
        .map_err(|error| format!("invalid value for {name}: {error}"))
}

fn parse_batch_sizes(value: &str) -> Result<Vec<usize>, String> {
    if value == "all" {
        return Ok(SUPPORTED_BATCH_SIZES.to_vec());
    }
    let values = value
        .split(',')
        .map(|part| parse_value("--batch-sizes", part))
        .collect::<Result<Vec<_>, _>>()?;
    if values.is_empty()
        || values
            .iter()
            .any(|size| !SUPPORTED_BATCH_SIZES.contains(size))
    {
        return Err("--batch-sizes accepts all or a comma list of 2048,4096".to_owned());
    }
    Ok(values)
}

fn parse_timeouts(value: &str) -> Result<Vec<u64>, String> {
    if value == "all" {
        return Ok(SUPPORTED_TIMEOUTS_MS.to_vec());
    }
    let values = value
        .split(',')
        .map(|part| parse_value("--batch-timeouts-ms", part))
        .collect::<Result<Vec<_>, _>>()?;
    if values.is_empty()
        || values
            .iter()
            .any(|timeout| !SUPPORTED_TIMEOUTS_MS.contains(timeout))
    {
        return Err("--batch-timeouts-ms accepts all or a comma list of 1,5,10,20".to_owned());
    }
    Ok(values)
}

fn validate_config(config: &Config) -> Result<(), String> {
    if config.iterations == 0 {
        return Err("--iterations must be greater than zero".to_owned());
    }
    if config.repetitions == 0 {
        return Err("--repetitions must be greater than zero".to_owned());
    }
    if config.users == 0 || config.coroutines == 0 {
        return Err("--users and --coroutines must be greater than zero".to_owned());
    }
    if config.coroutines as u64 > config.iterations {
        return Err("--coroutines must not exceed --iterations".to_owned());
    }
    if config.sample_stride == 0 {
        return Err("--sample-stride must be greater than zero".to_owned());
    }
    if config.batch_sizes.is_empty() || config.timeouts_ms.is_empty() {
        return Err("at least one batch size and timeout are required".to_owned());
    }
    if config.preflight_observation_ms == 0
        || config.preflight_timeout_ms == 0
        || config.preflight_observation_ms > config.preflight_timeout_ms
    {
        return Err(
            "preflight observation must be positive and no greater than its timeout".to_owned(),
        );
    }
    if !config.preflight_max_cpu_pct.is_finite()
        || !(0.0..=100.0).contains(&config.preflight_max_cpu_pct)
        || !config.preflight_max_disk_busy_pct.is_finite()
        || !(0.0..=100.0).contains(&config.preflight_max_disk_busy_pct)
    {
        return Err(
            "preflight CPU and disk thresholds must be finite percentages in 0..=100".to_owned(),
        );
    }
    Ok(())
}

fn print_help() {
    eprintln!("ledger_request_batch_tokio options:");
    eprintln!("  --iterations N (default 10000000)");
    eprintln!("  --repetitions N (default 1)");
    eprintln!("  --users N (default 50000)");
    eprintln!("  --coroutines N (default 50000; one outstanding request each)");
    eprintln!("  --batch-size all|2048|4096|2048,4096 (default 2048)");
    eprintln!("  --batch-timeout-ms all|1|5|10|20|1,5,10,20 (default 5)");
    eprintln!("  --sample-stride N (default 1024; select ID 0 or splitmix64(ID) % N == 0)");
    eprintln!("  coroutine C uses user ID C % users; multiple coroutines may share a user");
    eprintln!("  --csv PATH (default target/ledger-request-batch-tokio.csv; also stdout)");
    eprintln!("  --preflight-path PATH (default target/ledger-request-batch-preflight)");
    eprintln!("  --preflight-observation-ms N --preflight-timeout-ms N");
    eprintln!("  --preflight-max-cpu-pct N --preflight-max-disk-busy-pct N");
    eprintln!("  --preflight-min-mem-bytes N --preflight-free-reserve-bytes N");
}

fn run_preflight(config: &Config, case_name: &str) -> Result<PreflightReport, String> {
    let estimated_memory = estimated_working_set_bytes(config)?;
    let min_available_mem_bytes = config
        .preflight_min_mem_bytes
        .checked_add(estimated_memory)
        .ok_or_else(|| "preflight available-memory threshold overflowed".to_owned())?;
    let settings = PreflightConfig {
        observation: Duration::from_millis(config.preflight_observation_ms),
        timeout: Duration::from_millis(config.preflight_timeout_ms),
        max_cpu_busy_pct: config.preflight_max_cpu_pct,
        max_disk_busy_pct: config.preflight_max_disk_busy_pct,
        min_available_mem_bytes,
        min_free_bytes: config.preflight_free_reserve_bytes,
    };
    let report = crate::ledger_preflight::ensure_idle(&config.preflight_path, &settings)
        .map_err(|error| format!("preflight failed for {case_name}: {error}"))?;
    eprintln!(
        "PREFLIGHT case={case_name} path={} filesystem={} device={} attempts={} cpu_busy_pct={:.3} disk_busy_pct={:.3} available_mem_bytes={} free_bytes={} estimated_working_set_bytes={estimated_memory}",
        report.path.display(),
        report.filesystem,
        report.device,
        report.attempts,
        report.cpu_busy_pct,
        report.disk_busy_pct,
        report.mem_available_bytes,
        report.free_bytes
    );
    Ok(report)
}

fn estimated_working_set_bytes(config: &Config) -> Result<u64, String> {
    let sampled_requests = config
        .iterations
        .div_ceil(config.sample_stride)
        .saturating_mul(2)
        .saturating_add(64)
        .min(config.iterations);
    let sample_bytes = sampled_requests
        .checked_mul(6 * std::mem::size_of::<u64>() as u64)
        .ok_or_else(|| "sample memory estimate overflowed".to_owned())?;
    let verifier_bytes = config
        .iterations
        .div_ceil(64)
        .checked_mul(std::mem::size_of::<AtomicU64>() as u64)
        .ok_or_else(|| "ID verifier memory estimate overflowed".to_owned())?;
    let coroutine_bytes = (config.coroutines as u64)
        .checked_mul(1_024)
        .ok_or_else(|| "coroutine memory estimate overflowed".to_owned())?;
    sample_bytes
        .checked_add(verifier_bytes)
        .and_then(|value| value.checked_add(coroutine_bytes))
        .ok_or_else(|| "working-set memory estimate overflowed".to_owned())
}

async fn run_case(
    config: &Config,
    batch_size: usize,
    timeout_ms: u64,
    io_path: &Path,
) -> Result<CaseResult, String> {
    let verifier = Arc::new(IdVerifier::new(config.iterations)?);
    let queue_config = request_batch_queue::Config {
        capacity: QUEUE_CAPACITY,
        max_batch_size: batch_size,
        timeout: Duration::from_millis(timeout_ms),
    };
    let (queue, worker) =
        request_batch_queue::spawn(queue_config, |payloads: Vec<Payload>| async move {
            Ok(payloads
                .into_iter()
                .map(|payload| Reply {
                    request_id: payload.request_id,
                    opaque_result: opaque_result(payload.request_id, payload.user_id),
                })
                .collect())
        })?;

    let (start_sender, start_receiver) = watch::channel(false);
    let (ready_sender, mut ready_receiver) = mpsc::unbounded_channel();
    let mut clients = JoinSet::new();
    for coroutine_id in 0..config.coroutines {
        let client = queue.clone();
        let verifier = Arc::clone(&verifier);
        let start = start_receiver.clone();
        let ready = ready_sender.clone();
        let iterations = config.iterations;
        let coroutines = config.coroutines;
        let users = config.users;
        let sample_stride = config.sample_stride;
        clients.spawn(async move {
            let _ = ready.send(());
            wait_for_start(start).await?;
            run_client(
                coroutine_id,
                coroutines,
                users,
                iterations,
                sample_stride,
                client,
                verifier,
            )
            .await
        });
    }
    drop(ready_sender);

    for _ in 0..config.coroutines {
        if ready_receiver.recv().await.is_none() {
            clients.abort_all();
            while clients.join_next().await.is_some() {}
            drop(queue);
            let _ = worker.join().await;
            return Err("a client exited before reaching the start gate".to_owned());
        }
    }

    let io_before = crate::ledger_preflight::sample_io(io_path)
        .map_err(|error| format!("cannot sample I/O before case: {error}"))?;
    let cpu_started = ProcessTime::now();
    let cpu_wall_started = Instant::now();
    if start_sender.send(true).is_err() {
        clients.abort_all();
        while clients.join_next().await.is_some() {}
        drop(queue);
        let _ = worker.join().await;
        return Err("no clients were waiting at the start gate".to_owned());
    }
    drop(start_sender);

    let mut summary = RunSummary::new();
    let mut client_error = None;
    while let Some(joined) = clients.join_next().await {
        match joined {
            Ok(Ok(client)) => {
                if let Err(error) = summary.merge(client) {
                    client_error = Some(error);
                    break;
                }
            }
            Ok(Err(error)) => {
                client_error = Some(error);
                break;
            }
            Err(error) => {
                client_error = Some(format!("Tokio client task failed: {error}"));
                break;
            }
        }
    }
    if client_error.is_some() {
        clients.abort_all();
        while clients.join_next().await.is_some() {}
    }

    drop(queue);
    let worker_result = worker.join().await;
    let process_cpu = cpu_started.elapsed();
    let cpu_wall = cpu_wall_started.elapsed();
    let io_after = crate::ledger_preflight::sample_io(io_path)
        .map_err(|error| format!("cannot sample I/O after case: {error}"))?;
    if let Some(error) = client_error {
        return Err(error);
    }
    worker_result?;
    if summary.completed != config.iterations {
        return Err(format!(
            "completed {} requests, expected exactly {}",
            summary.completed, config.iterations
        ));
    }
    verifier.validate()?;
    let (batch_sizes, batch_reasons) = summary.batch_histograms()?;
    let batch_count: u64 = batch_sizes.values().sum();
    if batch_count != batch_reasons.iter().sum::<u64>() {
        return Err("batch size and flush reason counts do not agree".to_owned());
    }
    let first_request = summary
        .first_request_started_at
        .ok_or_else(|| "no request start time was recorded".to_owned())?;
    let last_response = summary
        .last_response_observed_at
        .ok_or_else(|| "no response completion time was recorded".to_owned())?;
    let request_wall = last_response
        .checked_duration_since(first_request)
        .ok_or_else(|| "request wall clock interval moved backwards".to_owned())?;
    if request_wall.is_zero() || cpu_wall.is_zero() {
        return Err("measured wall time must be greater than zero".to_owned());
    }

    Ok(CaseResult {
        summary,
        request_wall,
        cpu_wall,
        process_cpu,
        io_delta: io_delta(&io_before, &io_after)?,
    })
}

async fn wait_for_start(mut start: watch::Receiver<bool>) -> Result<(), String> {
    if *start.borrow() {
        return Ok(());
    }
    start
        .changed()
        .await
        .map_err(|_| "start gate closed before release".to_owned())?;
    if !*start.borrow() {
        return Err("start gate changed without release".to_owned());
    }
    Ok(())
}

async fn run_client(
    coroutine_id: usize,
    coroutines: usize,
    users: usize,
    iterations: u64,
    sample_stride: u64,
    queue: BatchQueue<Payload, Reply>,
    verifier: Arc<IdVerifier>,
) -> Result<ClientReport, String> {
    let mut report = ClientReport::new();
    let user_id = coroutine_id % users;
    let mut request_id = coroutine_id as u64;
    let stride = coroutines as u64;

    while request_id < iterations {
        let handle = queue
            .submit(Payload {
                request_id,
                user_id,
            })
            .await
            .map_err(|error| format!("request {request_id} enqueue failed: {error}"))?;
        let completed = handle
            .wait()
            .await
            .map_err(|error| format!("request {request_id} failed: {error}"))?;
        if completed.reply.request_id != request_id {
            return Err(format!(
                "coroutine {coroutine_id} expected reply ID {request_id}, received {}",
                completed.reply.request_id
            ));
        }
        let expected_opaque = opaque_result(request_id, user_id);
        if completed.reply.opaque_result != expected_opaque {
            return Err(format!(
                "reply {request_id} had opaque result {}, expected {expected_opaque}",
                completed.reply.opaque_result
            ));
        }
        verifier.record(completed.reply.request_id)?;

        let stages = [
            completed.total_time,
            completed.enqueue_wait,
            completed.queue_wait,
            completed.batch_wait,
            completed.handler_time,
            completed.response_wait,
        ];
        let stage_sum = stages[1..]
            .iter()
            .map(|duration| duration.as_nanos())
            .sum::<u128>();
        if stage_sum != stages[0].as_nanos() {
            return Err(format!(
                "request {request_id} stage total {stage_sum}ns differs from end-to-end {}ns",
                stages[0].as_nanos()
            ));
        }
        report.completed = report
            .completed
            .checked_add(1)
            .ok_or_else(|| "client completion count overflowed".to_owned())?;
        for (total, duration) in report.totals_ns.iter_mut().zip(stages) {
            *total = total
                .checked_add(duration.as_nanos())
                .ok_or_else(|| "client latency nanosecond total overflowed".to_owned())?;
        }
        if report.first_request_started_at.is_none() {
            report.first_request_started_at = Some(completed.request_started_at);
        }
        report.last_response_observed_at = Some(completed.response_observed_at);
        let reason_name = completed.flush_reason.as_str();
        let batch_members = report
            .batch_request_counts
            .entry((completed.batch_size, reason_name))
            .or_default();
        *batch_members = batch_members
            .checked_add(1)
            .ok_or_else(|| "client batch request counter overflowed".to_owned())?;

        if request_id == 0 || splitmix64(request_id) % sample_stride == 0 {
            report
                .samples
                .try_reserve(1)
                .map_err(|error| format!("cannot grow latency sample buffer: {error}"))?;
            report.samples.push(std::array::from_fn(|index| {
                stages[index].as_nanos().min(u64::MAX as u128) as u64
            }));
        }
        request_id = request_id
            .checked_add(stride)
            .ok_or_else(|| "logical request ID overflowed".to_owned())?;
    }
    Ok(report)
}

fn opaque_result(request_id: u64, user_id: usize) -> u64 {
    request_id.rotate_left(17) ^ (user_id as u64).rotate_left(41)
}

fn splitmix64(index: u64) -> u64 {
    let mut value = index.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn io_delta(before: &IoSample, after: &IoSample) -> Result<IoDelta, String> {
    if before.target_major_minor != after.target_major_minor {
        return Err("target device changed during the benchmark I/O bracket".to_owned());
    }
    let delta = |name: &str, end: u64, start: u64| {
        end.checked_sub(start)
            .ok_or_else(|| format!("{name} counter moved backwards during the benchmark"))
    };
    Ok(IoDelta {
        process_rchar_bytes: delta(
            "process rchar",
            after.process_rchar_bytes,
            before.process_rchar_bytes,
        )?,
        process_wchar_bytes: delta(
            "process wchar",
            after.process_wchar_bytes,
            before.process_wchar_bytes,
        )?,
        process_read_bytes: delta(
            "process read_bytes",
            after.process_read_bytes,
            before.process_read_bytes,
        )?,
        process_write_bytes: delta(
            "process write_bytes",
            after.process_write_bytes,
            before.process_write_bytes,
        )?,
        target_read_bytes: delta(
            "target device read bytes",
            after.target_read_bytes,
            before.target_read_bytes,
        )?,
        target_write_bytes: delta(
            "target device write bytes",
            after.target_write_bytes,
            before.target_write_bytes,
        )?,
        target_busy_ms: delta(
            "target device busy time",
            after.target_busy_ms,
            before.target_busy_ms,
        )?,
    })
}

fn csv_header() -> String {
    let mut fields = vec![
        "case",
        "repetition",
        "users",
        "coroutines",
        "iterations",
        "queue_capacity",
        "batch_size_limit",
        "batch_timeout_ms",
        "completed_count",
        "completed_rps",
        "request_wall_seconds",
        "process_cpu_seconds",
        "cpu_wall_seconds",
        "cpu_core_equivalents",
        "batch_count",
        "batch_size_flush_counts",
        "flush_size_count",
        "flush_timeout_count",
        "flush_channel_closed_count",
        "process_rchar_bytes_delta",
        "process_wchar_bytes_delta",
        "process_read_bytes_delta",
        "process_write_bytes_delta",
        "target_device_read_bytes_delta",
        "target_device_write_bytes_delta",
        "target_device_busy_ms_delta",
        "target_device",
        "target_major_minor",
        "preflight_filesystem",
        "preflight_attempts",
        "preflight_cpu_busy_pct",
        "preflight_disk_busy_pct",
        "preflight_available_mem_bytes",
        "preflight_free_bytes",
        "sample_selection",
        "percentile_method",
    ];
    for name in [
        "request_latency",
        "enqueue_wait",
        "queue_wait",
        "batch_wait",
        "handler_time",
        "response_wait",
    ] {
        fields.push(match name {
            "request_latency" => "request_latency_count",
            "enqueue_wait" => "enqueue_wait_count",
            "queue_wait" => "queue_wait_count",
            "batch_wait" => "batch_wait_count",
            "handler_time" => "handler_time_count",
            _ => "response_wait_count",
        });
        fields.push(match name {
            "request_latency" => "request_latency_mean_ns",
            "enqueue_wait" => "enqueue_wait_mean_ns",
            "queue_wait" => "queue_wait_mean_ns",
            "batch_wait" => "batch_wait_mean_ns",
            "handler_time" => "handler_time_mean_ns",
            _ => "response_wait_mean_ns",
        });
        fields.push(match name {
            "request_latency" => "request_latency_p50_ns",
            "enqueue_wait" => "enqueue_wait_p50_ns",
            "queue_wait" => "queue_wait_p50_ns",
            "batch_wait" => "batch_wait_p50_ns",
            "handler_time" => "handler_time_p50_ns",
            _ => "response_wait_p50_ns",
        });
        fields.push(match name {
            "request_latency" => "request_latency_p95_ns",
            "enqueue_wait" => "enqueue_wait_p95_ns",
            "queue_wait" => "queue_wait_p95_ns",
            "batch_wait" => "batch_wait_p95_ns",
            "handler_time" => "handler_time_p95_ns",
            _ => "response_wait_p95_ns",
        });
        fields.push(match name {
            "request_latency" => "request_latency_p99_ns",
            "enqueue_wait" => "enqueue_wait_p99_ns",
            "queue_wait" => "queue_wait_p99_ns",
            "batch_wait" => "batch_wait_p99_ns",
            "handler_time" => "handler_time_p99_ns",
            _ => "response_wait_p99_ns",
        });
        fields.push(match name {
            "request_latency" => "request_latency_sample_count",
            "enqueue_wait" => "enqueue_wait_sample_count",
            "queue_wait" => "queue_wait_sample_count",
            "batch_wait" => "batch_wait_sample_count",
            "handler_time" => "handler_time_sample_count",
            _ => "response_wait_sample_count",
        });
    }
    fields.join(",")
}

fn csv_row(
    case_name: &str,
    repetition: usize,
    config: &Config,
    batch_size: usize,
    timeout_ms: u64,
    preflight: &PreflightReport,
    result: &CaseResult,
) -> Result<String, String> {
    let (batch_histogram, flush_counts) = result.summary.batch_histograms()?;
    let batch_count: u64 = batch_histogram.values().sum();
    let batch_size_counts = batch_histogram
        .iter()
        .map(|(size, count)| format!("{size}:{count}"))
        .collect::<Vec<_>>()
        .join(";");
    let rps = result.summary.completed as f64 / result.request_wall.as_secs_f64();
    let cpu_core_equivalents = result.process_cpu.as_secs_f64() / result.cpu_wall.as_secs_f64();
    let mut fields = vec![
        csv_quote(case_name),
        repetition.to_string(),
        config.users.to_string(),
        config.coroutines.to_string(),
        config.iterations.to_string(),
        QUEUE_CAPACITY.to_string(),
        batch_size.to_string(),
        timeout_ms.to_string(),
        result.summary.completed.to_string(),
        format!("{rps:.3}"),
        format!("{:.9}", result.request_wall.as_secs_f64()),
        format!("{:.9}", result.process_cpu.as_secs_f64()),
        format!("{:.9}", result.cpu_wall.as_secs_f64()),
        format!("{cpu_core_equivalents:.6}"),
        batch_count.to_string(),
        csv_quote(&batch_size_counts),
        flush_counts[0].to_string(),
        flush_counts[1].to_string(),
        flush_counts[2].to_string(),
        result.io_delta.process_rchar_bytes.to_string(),
        result.io_delta.process_wchar_bytes.to_string(),
        result.io_delta.process_read_bytes.to_string(),
        result.io_delta.process_write_bytes.to_string(),
        result.io_delta.target_read_bytes.to_string(),
        result.io_delta.target_write_bytes.to_string(),
        result.io_delta.target_busy_ms.to_string(),
        csv_quote(&preflight.device),
        csv_quote(&preflight.major_minor),
        csv_quote(&preflight.filesystem),
        preflight.attempts.to_string(),
        format!("{:.4}", preflight.cpu_busy_pct),
        format!("{:.4}", preflight.disk_busy_pct),
        preflight.mem_available_bytes.to_string(),
        preflight.free_bytes.to_string(),
        csv_quote(&format!(
            "id0_or_splitmix64(id)%{}==0",
            config.sample_stride
        )),
        csv_quote("nearest_rank_on_deterministic_sample"),
    ];
    for index in 0..6 {
        let stats = latency_stats(&result.summary, index);
        fields.push(stats.count.to_string());
        fields.push(format!("{:.3}", stats.mean_ns));
        fields.push(stats.p50_ns.to_string());
        fields.push(stats.p95_ns.to_string());
        fields.push(stats.p99_ns.to_string());
        fields.push(stats.sample_count.to_string());
    }
    Ok(fields.join(","))
}

fn latency_stats(summary: &RunSummary, index: usize) -> LatencyStats {
    let mut samples = summary
        .samples
        .iter()
        .map(|sample| sample[index])
        .collect::<Vec<_>>();
    samples.sort_unstable();
    LatencyStats {
        mean_ns: if summary.completed == 0 {
            0.0
        } else {
            summary.totals_ns[index] as f64 / summary.completed as f64
        },
        p50_ns: percentile(&samples, 50),
        p95_ns: percentile(&samples, 95),
        p99_ns: percentile(&samples, 99),
        count: summary.completed,
        sample_count: samples.len() as u64,
    }
}

fn percentile(sorted_samples: &[u64], percentile: u64) -> u64 {
    if sorted_samples.is_empty() {
        return 0;
    }
    let rank = (percentile * sorted_samples.len() as u64).div_ceil(100);
    sorted_samples[rank.saturating_sub(1) as usize]
}

fn csv_quote(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
