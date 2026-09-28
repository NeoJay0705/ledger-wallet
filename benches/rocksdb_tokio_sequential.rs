#[path = "support/rocksdb_store.rs"]
mod store;

use cpu_time::ProcessTime;
use rocksdb::statistics::{Histogram, HistogramData, Ticker};
use rocksdb::{DB, Options, PerfContext, PerfMetric, PerfStatsLevel, WriteOptions};
use std::collections::BTreeMap;
use std::error::Error;
use std::fs::{self, OpenOptions as FsOpenOptions};
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DEFAULT_ITERATIONS: u64 = store::MAX_TRANSACTIONS;
const DEFAULT_BATCHES: &[usize] = &[64, 256, 1024, 2048, 4096];
const GIB: u64 = 1024 * 1024 * 1024;
const ESTIMATED_LIVE_BYTES_PER_TX: u64 = 160;
const WRITE_AMPLIFICATION_RESERVE_FACTOR: u64 = 4;
#[derive(Debug)]
struct Config {
    iterations: u64,
    repetitions: usize,
    batch_sizes: Vec<usize>,
    diagnostic: bool,
    output_dir: PathBuf,
    observation_ms: u64,
    timeout_ms: u64,
    max_cpu_pct: f64,
    max_disk_pct: f64,
    min_memory_bytes: u64,
    free_reserve_bytes: Option<u64>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            iterations: DEFAULT_ITERATIONS,
            repetitions: 3,
            batch_sizes: DEFAULT_BATCHES.to_vec(),
            diagnostic: false,
            output_dir: PathBuf::from("target/rocksdb-tokio-sequential"),
            observation_ms: 3000,
            timeout_ms: 60_000,
            max_cpu_pct: 10.0,
            max_disk_pct: 5.0,
            min_memory_bytes: 512 * 1024 * 1024,
            free_reserve_bytes: None,
        }
    }
}

#[derive(Default)]
struct Latencies {
    count: u64,
    total_ns: u128,
    values: Vec<u64>,
}

impl Latencies {
    fn add(&mut self, nanos: u64) {
        self.count += 1;
        self.total_ns += u128::from(nanos);
        self.values.push(nanos);
    }

    fn mean(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.total_ns as f64 / self.count as f64
        }
    }

    fn quantile(&mut self, numerator: usize) -> Option<u64> {
        quantile(&mut self.values, numerator)
    }
}

/// Each observed batch latency is assigned to every transaction in that batch.
/// This keeps percentile weighting transaction-based without retaining 10M
/// duplicate latency values.
#[derive(Default)]
struct TransactionLatencies {
    transactions: u64,
    total_ns: u128,
    batches: u64,
    values: Vec<(u64, u64)>,
}

impl TransactionLatencies {
    fn add_batch(&mut self, nanos: u64, transactions: usize) {
        let transactions = transactions as u64;
        self.transactions += transactions;
        self.total_ns += u128::from(nanos) * u128::from(transactions);
        self.batches += 1;
        self.values.push((nanos, transactions));
    }

    fn mean(&self) -> f64 {
        if self.transactions == 0 {
            0.0
        } else {
            self.total_ns as f64 / self.transactions as f64
        }
    }

    fn quantile(&mut self, numerator: usize) -> Option<u64> {
        if self.values.is_empty() {
            return None;
        }
        self.values.sort_unstable_by_key(|(value, _)| *value);
        let rank = (self.transactions as usize)
            .saturating_mul(numerator)
            .div_ceil(100)
            .max(1) as u64;
        let mut cumulative = 0;
        for &(value, weight) in &self.values {
            cumulative += weight;
            if cumulative >= rank {
                return Some(value);
            }
        }
        self.values.last().map(|(value, _)| *value)
    }
}

#[derive(Clone, Default)]
struct HeartbeatStats {
    end: Option<Instant>,
    lateness_ns: Vec<u64>,
    missed_tick_count: u64,
}

async fn run_heartbeat(
    stats: Arc<Mutex<HeartbeatStats>>,
    start_rx: tokio::sync::oneshot::Receiver<Instant>,
) {
    let Ok(start) = start_rx.await else {
        return;
    };
    let period = Duration::from_millis(1);
    let period_ns = period.as_nanos();
    let mut scheduled = start + period;
    loop {
        tokio::time::sleep_until(tokio::time::Instant::from_std(scheduled)).await;
        let actual = Instant::now();
        let mut stats = stats.lock().expect("heartbeat stats mutex poisoned");
        if stats.end.is_some_and(|end| scheduled > end) {
            break;
        }
        let observed = stats.end.map_or(actual, |end| actual.min(end));
        let lateness = observed.saturating_duration_since(scheduled);
        stats
            .lateness_ns
            .push(lateness.as_nanos().min(u128::from(u64::MAX)) as u64);
        let missed = lateness.as_nanos() / period_ns;
        stats.missed_tick_count = stats
            .missed_tick_count
            .saturating_add(missed.min(u128::from(u64::MAX)) as u64);
        if stats.end.is_some() {
            break;
        }
        drop(stats);

        let lateness = actual.saturating_duration_since(scheduled);
        let steps = lateness.as_nanos() / period_ns + 1;
        let advance_ns = period_ns.saturating_mul(steps);
        scheduled += Duration::from_nanos(advance_ns.min(u128::from(u64::MAX)) as u64);
    }
}

#[derive(Default)]
struct ProcIo {
    rchar: u64,
    wchar: u64,
    syscr: u64,
    syscw: u64,
    read_bytes: u64,
    write_bytes: u64,
    cancelled_write_bytes: u64,
}

#[derive(Default)]
struct SystemSample {
    fs_type: String,
    free_bytes: u64,
    mem_available: u64,
    rss_bytes: u64,
    cpu_busy_pct: Option<f64>,
    disk_busy_pct: Option<f64>,
    disk_device: Option<String>,
    disk_busy_ms: Option<u64>,
    required_free_bytes: u64,
    required_memory_bytes: u64,
}

struct TrialDirectory(Option<PathBuf>);

impl TrialDirectory {
    fn cleanup(mut self) -> io::Result<()> {
        if let Some(path) = self.0.as_ref() {
            fs::remove_dir_all(path)?;
            self.0 = None;
        }
        Ok(())
    }
}

impl Drop for TrialDirectory {
    fn drop(&mut self) {
        if let Some(path) = &self.0 {
            let _ = fs::remove_dir_all(path);
        }
    }
}

fn main() {
    if let Err(error) = run() {
        eprintln!("rocksdb_tokio_sequential: {error}");
        std::process::exit(2);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let config = parse_args()?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    create_dir_all_durable(&config.output_dir)?;
    reject_volatile_filesystem(&filesystem_type(&config.output_dir)?)?;

    let query_plan_bytes = config
        .iterations
        .saturating_mul(std::mem::size_of::<u64>() as u64);
    let balances_bytes =
        (store::USER_COUNT as u64).saturating_mul(std::mem::size_of::<u64>() as u64);
    let max_batch_size = config.batch_sizes.iter().copied().max().unwrap_or(1) as u64;
    let batch_working_bytes = max_batch_size.saturating_mul(256);
    let initial_memory_requirement = query_plan_bytes
        .saturating_add(balances_bytes)
        .saturating_add(batch_working_bytes)
        .saturating_add(config.min_memory_bytes);
    let initial_available = mem_available_bytes()?;
    if initial_available < initial_memory_requirement {
        return Err(format!(
            "preflight memory capacity check failed before query-plan generation: MemAvailable={} required={} (query_ids={} + balances={} + batch_working={} + configured_headroom={})",
            initial_available,
            initial_memory_requirement,
            query_plan_bytes,
            balances_bytes,
            batch_working_bytes,
            config.min_memory_bytes
        )
        .into());
    }

    eprintln!(
        "preparing deterministic shuffled query plan: {} IDs ({} bytes)",
        config.iterations, query_plan_bytes
    );
    let query_ids = make_shuffled_query_plan(config.iterations);
    print_csv_header();
    for batch_size in &config.batch_sizes {
        for repetition in 1..=config.repetitions {
            let label = format!("b{batch_size}-rep{repetition}");
            let reserve = config.free_reserve_bytes.unwrap_or(GIB);
            let write_free_requirement =
                estimated_database_bytes(config.iterations).saturating_add(reserve);
            let write_preflight = preflight(
                &config,
                "write",
                &label,
                *batch_size,
                write_free_requirement,
                batch_working_bytes,
            )?;
            let trial = runtime.block_on(run_trial(
                &config,
                &query_ids,
                *batch_size,
                repetition,
                &write_preflight,
                batch_working_bytes,
            ))?;
            println!("{}", trial.csv);
        }
    }
    Ok(())
}

fn parse_args() -> Result<Config, Box<dyn Error>> {
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
        if argument == "--diagnostic" {
            config.diagnostic = true;
            continue;
        }
        let value = args
            .next()
            .ok_or_else(|| format!("missing value after {argument}"))?;
        match argument.as_str() {
            "--iterations" => config.iterations = positive_u64(&argument, &value)?,
            "--repetitions" => config.repetitions = positive_usize(&argument, &value)?,
            "--batch-sizes" => config.batch_sizes = parse_usize_list(&argument, &value)?,
            "--output-dir" => config.output_dir = PathBuf::from(value),
            "--preflight-observation-ms" => {
                config.observation_ms = positive_u64(&argument, &value)?
            }
            "--preflight-timeout-ms" => config.timeout_ms = positive_u64(&argument, &value)?,
            "--preflight-max-cpu-pct" => config.max_cpu_pct = percentage(&argument, &value)?,
            "--preflight-max-disk-busy-pct" => config.max_disk_pct = percentage(&argument, &value)?,
            "--preflight-min-mem-bytes" => config.min_memory_bytes = value.parse()?,
            "--preflight-free-reserve-bytes" => config.free_reserve_bytes = Some(value.parse()?),
            _ => return Err(format!("unknown argument: {argument}").into()),
        }
    }

    if config.iterations > store::MAX_TRANSACTIONS {
        return Err(format!(
            "--iterations cannot exceed {} for the 100-round-per-user workload",
            store::MAX_TRANSACTIONS
        )
        .into());
    }
    if config.batch_sizes.is_empty() || config.batch_sizes.contains(&0) {
        return Err("batch sizes must be positive".into());
    }
    if config.batch_sizes.iter().any(|batch| *batch > 1_000_000) {
        return Err("batch sizes cannot exceed 1000000".into());
    }
    if config.diagnostic
        && config
            .batch_sizes
            .iter()
            .any(|batch| !matches!(batch, 2048 | 4096))
    {
        return Err("--diagnostic supports batch sizes 2048 and 4096".into());
    }
    Ok(config)
}

fn print_help() {
    println!(
        "rocksdb_tokio_sequential options:\n  --iterations N (default 10000000)\n  --repetitions N (default 3)\n  --batch-sizes N[,N...] (default 64,256,1024,2048,4096)\n  --diagnostic (capture per-batch write/query trace for batch sizes 2048 or 4096)\n  --output-dir PATH\n  --preflight-observation-ms N\n  --preflight-timeout-ms N\n  --preflight-max-cpu-pct 0..100\n  --preflight-max-disk-busy-pct 0..100\n  --preflight-min-mem-bytes N\n  --preflight-free-reserve-bytes N"
    );
}

fn positive_u64(argument: &str, value: &str) -> Result<u64, Box<dyn Error>> {
    let parsed: u64 = value
        .parse()
        .map_err(|_| format!("{argument} expects a positive integer"))?;
    if parsed == 0 {
        return Err(format!("{argument} expects a positive integer").into());
    }
    Ok(parsed)
}

fn positive_usize(argument: &str, value: &str) -> Result<usize, Box<dyn Error>> {
    let parsed: usize = value
        .parse()
        .map_err(|_| format!("{argument} expects a positive integer"))?;
    if parsed == 0 {
        return Err(format!("{argument} expects a positive integer").into());
    }
    Ok(parsed)
}

fn parse_usize_list(argument: &str, value: &str) -> Result<Vec<usize>, Box<dyn Error>> {
    value
        .split(',')
        .map(|item| {
            item.parse::<usize>()
                .map_err(|_| format!("{argument} expects comma-separated integers").into())
        })
        .collect()
}

fn percentage(argument: &str, value: &str) -> Result<f64, Box<dyn Error>> {
    let parsed: f64 = value
        .parse()
        .map_err(|_| format!("{argument} expects a percentage from 0 to 100"))?;
    if !parsed.is_finite() || !(0.0..=100.0).contains(&parsed) {
        return Err(format!("{argument} expects a percentage from 0 to 100").into());
    }
    Ok(parsed)
}

fn make_shuffled_query_plan(iterations: u64) -> Vec<u64> {
    let mut ids: Vec<_> = (1..=iterations).collect();
    let mut state = 0x6a09_e667_f3bc_c909_u64;
    for index in (1..ids.len()).rev() {
        state = splitmix64(state);
        let other = (state % (index as u64 + 1)) as usize;
        ids.swap(index, other);
    }
    ids
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn estimated_database_bytes(iterations: u64) -> u64 {
    iterations
        .saturating_mul(ESTIMATED_LIVE_BYTES_PER_TX)
        .saturating_mul(WRITE_AMPLIFICATION_RESERVE_FACTOR)
        .saturating_add((store::USER_COUNT as u64).saturating_mul(32))
}

fn preflight(
    config: &Config,
    phase: &str,
    label: &str,
    batch_size: usize,
    required_free_bytes: u64,
    batch_working_bytes: u64,
) -> Result<SystemSample, Box<dyn Error>> {
    let required_memory_bytes = config.min_memory_bytes.saturating_add(batch_working_bytes);
    let start = Instant::now();
    let mut attempt = 0;
    loop {
        attempt += 1;
        let sample = system_sample(&config.output_dir, config.observation_ms)?;
        if sample.disk_busy_pct.is_none() {
            return Err(format!(
                "preflight cannot measure target disk busy for phase={phase} trial={label}; no benchmark result emitted"
            )
            .into());
        }
        if sample.cpu_busy_pct.is_none() {
            return Err(format!(
                "preflight cannot measure aggregate CPU busy for phase={phase} trial={label}; no benchmark result emitted"
            )
            .into());
        }
        let memory_ok = sample.mem_available >= required_memory_bytes;
        let disk_ok = sample
            .disk_busy_pct
            .is_some_and(|busy| busy <= config.max_disk_pct);
        let cpu_ok = sample
            .cpu_busy_pct
            .is_some_and(|busy| busy <= config.max_cpu_pct);
        let free_ok = sample.free_bytes >= required_free_bytes;
        let ok = memory_ok && free_ok && cpu_ok && disk_ok;
        let line = format!(
            "PREFLIGHT phase={phase} trial={label} batch_size={batch_size} attempt={attempt} fs={} free_bytes={} required_free_bytes={} mem_available_bytes={} required_memory_bytes={} rss_bytes={} cpu_busy_pct={} cpu_limit_pct={} disk_busy_pct={} disk_limit_pct={} disk_device={} result={}",
            sample.fs_type,
            sample.free_bytes,
            required_free_bytes,
            sample.mem_available,
            required_memory_bytes,
            sample.rss_bytes,
            optional_float(sample.cpu_busy_pct),
            config.max_cpu_pct,
            optional_float(sample.disk_busy_pct),
            config.max_disk_pct,
            sample.disk_device.as_deref().unwrap_or("unavailable"),
            if ok { "ready" } else { "wait" }
        );
        eprintln!("{line}");
        append_preflight_log(&config.output_dir, &line)?;
        if ok {
            return Ok(SystemSample {
                required_free_bytes,
                required_memory_bytes,
                ..sample
            });
        }
        if start.elapsed() >= Duration::from_millis(config.timeout_ms) {
            return Err(format!(
                "preflight timed out for phase={phase} trial={label}; no benchmark result emitted (batch_size={batch_size})"
            )
            .into());
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn append_preflight_log(output_dir: &Path, line: &str) -> io::Result<()> {
    let mut file = FsOpenOptions::new()
        .create(true)
        .append(true)
        .open(output_dir.join("preflight.log"))?;
    writeln!(file, "{line}")?;
    file.flush()
}

fn system_sample(path: &Path, observation_ms: u64) -> Result<SystemSample, Box<dyn Error>> {
    let fs_type = filesystem_type(path)?;
    reject_volatile_filesystem(&fs_type)?;
    let free_bytes = free_bytes(path)?;
    let mem_available = mem_available_bytes()?;
    let rss_bytes = process_rss_bytes()?;
    let cpu_start = cpu_ticks()?;
    let device = target_device(path)?;
    let disk_start = device.as_ref().and_then(|name| disk_busy_ms(name));
    let observation_started = Instant::now();
    thread::sleep(Duration::from_millis(observation_ms));
    let cpu_end = cpu_ticks()?;
    let disk_end = device.as_ref().and_then(|name| disk_busy_ms(name));
    let elapsed_ms = observation_started.elapsed().as_secs_f64() * 1000.0;
    let total_ticks = cpu_end.0.saturating_sub(cpu_start.0);
    let idle_ticks = cpu_end.1.saturating_sub(cpu_start.1);
    let cpu_busy_pct = (total_ticks > 0)
        .then(|| (total_ticks.saturating_sub(idle_ticks) as f64 / total_ticks as f64) * 100.0);
    let disk_busy_pct = match (disk_start, disk_end) {
        (Some(start), Some(end)) if elapsed_ms > 0.0 => Some(
            (end.saturating_sub(start) as f64 / elapsed_ms)
                .mul_add(100.0, 0.0)
                .min(100.0),
        ),
        _ => None,
    };
    Ok(SystemSample {
        fs_type,
        free_bytes,
        mem_available,
        rss_bytes,
        cpu_busy_pct,
        disk_busy_pct,
        disk_device: device,
        disk_busy_ms: disk_end,
        required_free_bytes: 0,
        required_memory_bytes: 0,
    })
}

fn filesystem_type(path: &Path) -> Result<String, Box<dyn Error>> {
    let output = Command::new("stat")
        .args(["-f", "-c", "%T"])
        .arg(path)
        .output()?;
    if !output.status.success() {
        return Err(format!("could not inspect filesystem for {}", path.display()).into());
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_owned())
}

fn reject_volatile_filesystem(fs_type: &str) -> Result<(), Box<dyn Error>> {
    if matches!(fs_type, "tmpfs" | "ramfs" | "devtmpfs") {
        return Err(format!(
            "RocksDB target filesystem {fs_type} is volatile; choose persistent storage"
        )
        .into());
    }
    Ok(())
}

fn free_bytes(path: &Path) -> Result<u64, Box<dyn Error>> {
    let output = Command::new("df").arg("-Pk").arg(path).output()?;
    if !output.status.success() {
        return Err(format!("could not inspect free space for {}", path.display()).into());
    }
    let stdout = String::from_utf8(output.stdout)?;
    let row = stdout
        .lines()
        .last()
        .ok_or("df returned no filesystem row")?;
    let fields: Vec<_> = row.split_whitespace().collect();
    let available_kib: u64 = fields.get(3).ok_or("could not parse df output")?.parse()?;
    Ok(available_kib.saturating_mul(1024))
}

fn mem_available_bytes() -> Result<u64, Box<dyn Error>> {
    let text = fs::read_to_string("/proc/meminfo")?;
    let kib = text
        .lines()
        .find_map(|line| {
            line.strip_prefix("MemAvailable:")
                .and_then(|value| value.split_whitespace().next())
                .and_then(|value| value.parse::<u64>().ok())
        })
        .ok_or("MemAvailable is absent from /proc/meminfo")?;
    Ok(kib.saturating_mul(1024))
}

fn process_rss_bytes() -> Result<u64, Box<dyn Error>> {
    let text = fs::read_to_string("/proc/self/status")?;
    let kib = text
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmRSS:")
                .and_then(|value| value.split_whitespace().next())
                .and_then(|value| value.parse::<u64>().ok())
        })
        .unwrap_or(0);
    Ok(kib.saturating_mul(1024))
}

fn cpu_ticks() -> Result<(u64, u64), Box<dyn Error>> {
    let text = fs::read_to_string("/proc/stat")?;
    let line = text
        .lines()
        .find(|line| line.starts_with("cpu "))
        .ok_or("aggregate CPU line is absent from /proc/stat")?;
    let values: Vec<u64> = line
        .split_whitespace()
        .skip(1)
        .map(str::parse)
        .collect::<Result<_, _>>()?;
    if values.len() < 4 {
        return Err("aggregate CPU line is incomplete".into());
    }
    let total: u64 = values.iter().copied().sum();
    let idle = values[3].saturating_add(values.get(4).copied().unwrap_or(0));
    Ok((total, idle))
}

fn target_device(path: &Path) -> Result<Option<String>, Box<dyn Error>> {
    let canonical = fs::canonicalize(path)?;
    let text = fs::read_to_string("/proc/self/mountinfo")?;
    let mut best: Option<(usize, String)> = None;
    for line in text.lines() {
        let fields: Vec<_> = line.split_whitespace().collect();
        if fields.len() < 5 {
            continue;
        }
        let mountpoint = decode_mount_field(fields[4]);
        if canonical.starts_with(&mountpoint) {
            let major_minor = fields[2].to_owned();
            let length = mountpoint.as_os_str().len();
            if best.as_ref().is_none_or(|(best_len, _)| length > *best_len) {
                best = Some((length, major_minor));
            }
        }
    }
    let Some((_, major_minor)) = best else {
        return Ok(None);
    };
    let diskstats = fs::read_to_string("/proc/diskstats")?;
    for line in diskstats.lines() {
        let fields: Vec<_> = line.split_whitespace().collect();
        if fields.len() >= 13 && format!("{}:{}", fields[0], fields[1]) == major_minor {
            return Ok(Some(fields[2].to_owned()));
        }
    }
    Ok(None)
}

fn decode_mount_field(field: &str) -> PathBuf {
    PathBuf::from(
        field
            .replace("\\040", " ")
            .replace("\\011", "\t")
            .replace("\\134", "\\"),
    )
}

fn disk_busy_ms(device: &str) -> Option<u64> {
    let contents = fs::read_to_string("/proc/diskstats").ok()?;
    contents.lines().find_map(|line| {
        let fields: Vec<_> = line.split_whitespace().collect();
        if fields.len() >= 13 && fields[2] == device {
            fields[12].parse().ok()
        } else {
            None
        }
    })
}

fn proc_io() -> Option<ProcIo> {
    let contents = fs::read_to_string("/proc/self/io").ok()?;
    let values: BTreeMap<_, _> = contents
        .lines()
        .filter_map(|line| {
            let (key, value) = line.split_once(':')?;
            Some((key, value.trim().parse::<u64>().ok()?))
        })
        .collect();
    Some(ProcIo {
        rchar: *values.get("rchar")?,
        wchar: *values.get("wchar")?,
        syscr: *values.get("syscr")?,
        syscw: *values.get("syscw")?,
        read_bytes: *values.get("read_bytes")?,
        write_bytes: *values.get("write_bytes")?,
        cancelled_write_bytes: *values.get("cancelled_write_bytes")?,
    })
}

fn create_dir_all_durable(path: &Path) -> io::Result<()> {
    if path.as_os_str().is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "benchmark output directory path is empty",
        ));
    }
    let mut current = PathBuf::new();
    for component in path.components() {
        current.push(component.as_os_str());
        if !matches!(component, Component::Normal(_)) {
            continue;
        }
        match fs::metadata(&current) {
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::NotADirectory,
                    format!("{} is not a directory", current.display()),
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                match fs::create_dir(&current) {
                    Ok(()) => fs::File::open(parent_directory(&current))?.sync_all()?,
                    Err(create_error) if create_error.kind() == io::ErrorKind::AlreadyExists => {
                        if !fs::metadata(&current)?.is_dir() {
                            return Err(create_error);
                        }
                        fs::File::open(parent_directory(&current))?.sync_all()?;
                    }
                    Err(create_error) => return Err(create_error),
                }
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn parent_directory(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

#[derive(Default)]
struct WriteMetrics {
    transactions: u64,
    batches: u64,
    wall: Duration,
    cpu_seconds: f64,
    latency: TransactionLatencies,
    build: Latencies,
    db_write: Latencies,
    heartbeat: HeartbeatStats,
    io_start: ProcIo,
    io_end: ProcIo,
    disk_busy_start_ms: Option<u64>,
    disk_busy_end_ms: Option<u64>,
}

#[derive(Default)]
struct QueryMetrics {
    transactions: u64,
    batches: u64,
    wall: Duration,
    cpu_seconds: f64,
    latency: TransactionLatencies,
    prep: Latencies,
    read: Latencies,
    decode: Latencies,
    heartbeat: HeartbeatStats,
    io_start: ProcIo,
    io_end: ProcIo,
    disk_busy_start_ms: Option<u64>,
    disk_busy_end_ms: Option<u64>,
}

#[derive(Clone, Default)]
struct PerfSnapshot {
    user_key_comparison_count: u64,
    write_wal_time_ns: u64,
    write_memtable_time_ns: u64,
    write_delay_time_ns: u64,
    write_pre_post_process_time_ns: u64,
    block_cache_hit_count: u64,
    block_read_count: u64,
    block_read_bytes: u64,
    block_read_time_ns: u64,
    block_checksum_time_ns: u64,
    block_decompress_time_ns: u64,
    multiget_read_bytes: u64,
    read_index_block_ns: u64,
    read_filter_block_ns: u64,
    get_from_memtable_time_ns: u64,
    get_from_memtable_count: u64,
    get_from_output_files_time_ns: u64,
    bloom_memtable_hit_count: u64,
    bloom_memtable_miss_count: u64,
    bloom_sst_hit_count: u64,
    bloom_sst_miss_count: u64,
    new_table_block_iter_ns: u64,
    block_seek_ns: u64,
    find_table_ns: u64,
    db_mutex_lock_ns: u64,
    db_condition_wait_ns: u64,
}

impl PerfSnapshot {
    fn read(context: &PerfContext) -> Self {
        let metric = |metric| context.metric(metric);
        Self {
            user_key_comparison_count: metric(PerfMetric::UserKeyComparisonCount),
            write_wal_time_ns: metric(PerfMetric::WriteWalTime),
            write_memtable_time_ns: metric(PerfMetric::WriteMemtableTime),
            write_delay_time_ns: metric(PerfMetric::WriteDelayTime),
            write_pre_post_process_time_ns: metric(PerfMetric::WritePreAndPostProcessTime),
            block_cache_hit_count: metric(PerfMetric::BlockCacheHitCount),
            block_read_count: metric(PerfMetric::BlockReadCount),
            block_read_bytes: metric(PerfMetric::BlockReadByte),
            block_read_time_ns: metric(PerfMetric::BlockReadTime),
            block_checksum_time_ns: metric(PerfMetric::BlockChecksumTime),
            block_decompress_time_ns: metric(PerfMetric::BlockDecompressTime),
            multiget_read_bytes: metric(PerfMetric::MultigetReadBytes),
            read_index_block_ns: metric(PerfMetric::ReadIndexBlockNanos),
            read_filter_block_ns: metric(PerfMetric::ReadFilterBlockNanos),
            get_from_memtable_time_ns: metric(PerfMetric::GetFromMemtableTime),
            get_from_memtable_count: metric(PerfMetric::GetFromMemtableCount),
            get_from_output_files_time_ns: metric(PerfMetric::GetFromOutputFilesTime),
            bloom_memtable_hit_count: metric(PerfMetric::BloomMemtableHitCount),
            bloom_memtable_miss_count: metric(PerfMetric::BloomMemtableMissCount),
            bloom_sst_hit_count: metric(PerfMetric::BloomSstHitCount),
            bloom_sst_miss_count: metric(PerfMetric::BloomSstMissCount),
            new_table_block_iter_ns: metric(PerfMetric::NewTableBlockIterNanos),
            block_seek_ns: metric(PerfMetric::BlockSeekNanos),
            find_table_ns: metric(PerfMetric::FindTableNanos),
            db_mutex_lock_ns: metric(PerfMetric::DbMutexLockNanos),
            db_condition_wait_ns: metric(PerfMetric::DbConditionWaitNanos),
        }
    }
}

#[derive(Clone, Copy, Default)]
struct ThreadResourceSnapshot {
    cpu_time_ns: u64,
    voluntary_context_switches: u64,
    involuntary_context_switches: u64,
    minor_page_faults: u64,
    major_page_faults: u64,
}

#[derive(Clone, Copy, Default)]
struct NativeCallMetrics {
    thread_cpu_ns: u64,
    voluntary_context_switches: u64,
    involuntary_context_switches: u64,
    minor_page_faults: u64,
    major_page_faults: u64,
}

impl ThreadResourceSnapshot {
    fn capture() -> io::Result<Self> {
        let mut cpu_time: libc::timespec = unsafe { std::mem::zeroed() };
        if unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut cpu_time) } != 0 {
            return Err(io::Error::last_os_error());
        }
        if cpu_time.tv_sec < 0 || !(0..1_000_000_000).contains(&cpu_time.tv_nsec) {
            return Err(io::Error::other(
                "CLOCK_THREAD_CPUTIME_ID returned an invalid timespec",
            ));
        }
        let cpu_time_ns = (cpu_time.tv_sec as u128)
            .checked_mul(1_000_000_000)
            .and_then(|seconds| seconds.checked_add(cpu_time.tv_nsec as u128))
            .and_then(|nanos| u64::try_from(nanos).ok())
            .ok_or_else(|| io::Error::other("CLOCK_THREAD_CPUTIME_ID value overflowed u64 ns"))?;

        let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
        if unsafe { libc::getrusage(libc::RUSAGE_THREAD, &mut usage) } != 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(Self {
            cpu_time_ns,
            voluntary_context_switches: nonnegative_usage_counter(
                usage.ru_nvcsw,
                "RUSAGE_THREAD voluntary context switches",
            )?,
            involuntary_context_switches: nonnegative_usage_counter(
                usage.ru_nivcsw,
                "RUSAGE_THREAD involuntary context switches",
            )?,
            minor_page_faults: nonnegative_usage_counter(
                usage.ru_minflt,
                "RUSAGE_THREAD minor page faults",
            )?,
            major_page_faults: nonnegative_usage_counter(
                usage.ru_majflt,
                "RUSAGE_THREAD major page faults",
            )?,
        })
    }

    fn delta_since(self, start: Self) -> io::Result<NativeCallMetrics> {
        let delta = |end: u64, start: u64, name: &str| {
            end.checked_sub(start)
                .ok_or_else(|| io::Error::other(format!("{name} decreased during the native call")))
        };
        let thread_cpu_ns = delta(self.cpu_time_ns, start.cpu_time_ns, "thread CPU time")?;
        if thread_cpu_ns == 0 {
            return Err(io::Error::other(
                "CLOCK_THREAD_CPUTIME_ID did not advance during the native call",
            ));
        }
        Ok(NativeCallMetrics {
            thread_cpu_ns,
            voluntary_context_switches: delta(
                self.voluntary_context_switches,
                start.voluntary_context_switches,
                "voluntary context switches",
            )?,
            involuntary_context_switches: delta(
                self.involuntary_context_switches,
                start.involuntary_context_switches,
                "involuntary context switches",
            )?,
            minor_page_faults: delta(
                self.minor_page_faults,
                start.minor_page_faults,
                "minor page faults",
            )?,
            major_page_faults: delta(
                self.major_page_faults,
                start.major_page_faults,
                "major page faults",
            )?,
        })
    }
}

fn nonnegative_usage_counter(value: libc::c_long, name: &str) -> io::Result<u64> {
    u64::try_from(value).map_err(|_| io::Error::other(format!("{name} returned a negative value")))
}

struct WorkerObservation {
    started_at: Instant,
    native_started_at: Instant,
    native_finished_at: Instant,
    finished_at: Instant,
    thread_id: u64,
    cpu_id: Option<i32>,
    native_call: NativeCallMetrics,
    perf: PerfSnapshot,
}

struct DiagnosticBatch {
    phase: &'static str,
    batch_index: u64,
    item_count: usize,
    total_ns: u64,
    build_or_prep_ns: u64,
    dispatch_to_worker_start_ns: u64,
    worker_setup_ns: u64,
    native_call_ns: u64,
    decode_validate_ns: u64,
    worker_post_call_ns: u64,
    worker_finish_to_await_resume_ns: u64,
    await_resume_to_ack_ns: u64,
    worker_thread_id: u64,
    worker_cpu_id: Option<i32>,
    native_call: NativeCallMetrics,
    perf: PerfSnapshot,
}

impl DiagnosticBatch {
    fn from_worker(
        phase: &'static str,
        batch_index: u64,
        item_count: usize,
        total_ns: u64,
        build_or_prep_ns: u64,
        dispatch_at: Instant,
        await_resumed_at: Instant,
        acknowledged_at: Instant,
        decode_validate_ns: u64,
        worker: WorkerObservation,
    ) -> Self {
        Self {
            phase,
            batch_index,
            item_count,
            total_ns,
            build_or_prep_ns,
            dispatch_to_worker_start_ns: duration_ns(worker.started_at.duration_since(dispatch_at)),
            worker_setup_ns: duration_ns(
                worker.native_started_at.duration_since(worker.started_at),
            ),
            native_call_ns: duration_ns(
                worker
                    .native_finished_at
                    .duration_since(worker.native_started_at),
            ),
            decode_validate_ns,
            worker_post_call_ns: duration_ns(
                worker.finished_at.duration_since(worker.native_finished_at),
            )
            .checked_sub(decode_validate_ns)
            .expect("decode stage fits inside worker post-call interval"),
            worker_finish_to_await_resume_ns: duration_ns(
                await_resumed_at.duration_since(worker.finished_at),
            ),
            await_resume_to_ack_ns: duration_ns(acknowledged_at.duration_since(await_resumed_at)),
            worker_thread_id: worker.thread_id,
            worker_cpu_id: worker.cpu_id,
            native_call: worker.native_call,
            perf: worker.perf,
        }
    }

    fn accounted_ns(&self) -> u128 {
        [
            self.build_or_prep_ns,
            self.dispatch_to_worker_start_ns,
            self.worker_setup_ns,
            self.native_call_ns,
            self.decode_validate_ns,
            self.worker_post_call_ns,
            self.worker_finish_to_await_resume_ns,
            self.await_resume_to_ack_ns,
        ]
        .into_iter()
        .map(u128::from)
        .sum()
    }
}

fn worker_thread_id() -> u64 {
    unsafe { libc::syscall(libc::SYS_gettid) }.max(0) as u64
}

fn worker_cpu_id() -> Option<i32> {
    let cpu = unsafe { libc::sched_getcpu() };
    (cpu >= 0).then_some(cpu)
}

fn diagnostic_trace_csv(rows: &[DiagnosticBatch]) -> String {
    let mut lines = vec![
        [
            "phase",
            "batch_index",
            "item_count",
            "batch_total_ns",
            "build_or_prep_ns",
            "dispatch_to_worker_start_ns",
            "worker_setup_ns",
            "native_call_ns",
            "native_call_thread_cpu_ns",
            "native_call_voluntary_context_switches",
            "native_call_involuntary_context_switches",
            "native_call_minor_page_faults",
            "native_call_major_page_faults",
            "decode_validate_ns",
            "worker_post_call_ns",
            "worker_finish_to_await_resume_ns",
            "await_resume_to_ack_ns",
            "component_sum_ns",
            "residual_ns",
            "worker_thread_id",
            "worker_cpu_id",
            "rocksdb_user_key_comparison_count",
            "rocksdb_write_wal_time_ns",
            "rocksdb_write_memtable_time_ns",
            "rocksdb_write_delay_time_ns",
            "rocksdb_write_pre_post_process_time_ns",
            "rocksdb_block_cache_hit_count",
            "rocksdb_block_read_count",
            "rocksdb_block_read_bytes",
            "rocksdb_block_read_time_ns",
            "rocksdb_block_checksum_time_ns",
            "rocksdb_block_decompress_time_ns",
            "rocksdb_multiget_read_bytes",
            "rocksdb_read_index_block_ns",
            "rocksdb_read_filter_block_ns",
            "rocksdb_get_from_memtable_time_ns",
            "rocksdb_get_from_memtable_count",
            "rocksdb_get_from_output_files_time_ns",
            "rocksdb_bloom_memtable_hit_count",
            "rocksdb_bloom_memtable_miss_count",
            "rocksdb_bloom_sst_hit_count",
            "rocksdb_bloom_sst_miss_count",
            "rocksdb_new_table_block_iter_ns",
            "rocksdb_block_seek_ns",
            "rocksdb_find_table_ns",
            "rocksdb_db_mutex_lock_ns",
            "rocksdb_db_condition_wait_ns",
        ]
        .join(","),
    ];

    for row in rows {
        let perf = &row.perf;
        let component_sum_ns = row.accounted_ns();
        let residual_ns = i128::from(row.total_ns) - component_sum_ns as i128;
        lines.push(
            [
                row.phase.to_owned(),
                row.batch_index.to_string(),
                row.item_count.to_string(),
                row.total_ns.to_string(),
                row.build_or_prep_ns.to_string(),
                row.dispatch_to_worker_start_ns.to_string(),
                row.worker_setup_ns.to_string(),
                row.native_call_ns.to_string(),
                row.native_call.thread_cpu_ns.to_string(),
                row.native_call.voluntary_context_switches.to_string(),
                row.native_call.involuntary_context_switches.to_string(),
                row.native_call.minor_page_faults.to_string(),
                row.native_call.major_page_faults.to_string(),
                row.decode_validate_ns.to_string(),
                row.worker_post_call_ns.to_string(),
                row.worker_finish_to_await_resume_ns.to_string(),
                row.await_resume_to_ack_ns.to_string(),
                component_sum_ns.to_string(),
                residual_ns.to_string(),
                row.worker_thread_id.to_string(),
                row.worker_cpu_id
                    .map_or(String::new(), |cpu| cpu.to_string()),
                perf.user_key_comparison_count.to_string(),
                perf.write_wal_time_ns.to_string(),
                perf.write_memtable_time_ns.to_string(),
                perf.write_delay_time_ns.to_string(),
                perf.write_pre_post_process_time_ns.to_string(),
                perf.block_cache_hit_count.to_string(),
                perf.block_read_count.to_string(),
                perf.block_read_bytes.to_string(),
                perf.block_read_time_ns.to_string(),
                perf.block_checksum_time_ns.to_string(),
                perf.block_decompress_time_ns.to_string(),
                perf.multiget_read_bytes.to_string(),
                perf.read_index_block_ns.to_string(),
                perf.read_filter_block_ns.to_string(),
                perf.get_from_memtable_time_ns.to_string(),
                perf.get_from_memtable_count.to_string(),
                perf.get_from_output_files_time_ns.to_string(),
                perf.bloom_memtable_hit_count.to_string(),
                perf.bloom_memtable_miss_count.to_string(),
                perf.bloom_sst_hit_count.to_string(),
                perf.bloom_sst_miss_count.to_string(),
                perf.new_table_block_iter_ns.to_string(),
                perf.block_seek_ns.to_string(),
                perf.find_table_ns.to_string(),
                perf.db_mutex_lock_ns.to_string(),
                perf.db_condition_wait_ns.to_string(),
            ]
            .join(","),
        );
    }
    let mut csv = lines.join("\n");
    csv.push('\n');
    csv
}

async fn run_trial(
    config: &Config,
    query_ids: &[u64],
    batch_size: usize,
    repetition: usize,
    write_preflight: &SystemSample,
    batch_working_bytes: u64,
) -> Result<TrialResult, Box<dyn Error>> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let label = format!("b{batch_size}-rep{repetition}");
    let db_dir = config.output_dir.join(format!(
        "trial-{label}-pid{}-{timestamp}",
        std::process::id()
    ));
    fs::create_dir(&db_dir)?;
    let guard = TrialDirectory(Some(db_dir.clone()));
    fs::File::open(parent_directory(&db_dir))?.sync_all()?;

    let write_options = store::rocksdb_options();
    write_options.set_statistics_level(rocksdb::statistics::StatsLevel::ExceptDetailedTimers);
    let db = Arc::new(store::open_db(&write_options, &db_dir)?);
    let write_counters_start = RdbCounters::from_options(&write_options);
    let mut balances = vec![0_u64; store::USER_COUNT];

    let mut write_latency = TransactionLatencies::default();
    let mut write_build = Latencies::default();
    let mut write_db_call = Latencies::default();
    let mut diagnostic_rows = Vec::new();
    let heartbeat_stats = Arc::new(Mutex::new(HeartbeatStats::default()));
    let (heartbeat_start_tx, heartbeat_start_rx) = tokio::sync::oneshot::channel();
    let heartbeat_task = tokio::spawn(run_heartbeat(
        Arc::clone(&heartbeat_stats),
        heartbeat_start_rx,
    ));
    let mut heartbeat_start_tx = Some(heartbeat_start_tx);
    let mut write_phase_start: Option<Instant> = None;
    let mut write_cpu_start: Option<ProcessTime> = None;
    let mut write_cpu_seconds_end = None;
    let write_io_start = proc_io().ok_or("cannot read process I/O counters before write phase")?;
    let write_disk_start = disk_busy_ms(write_preflight.disk_device.as_deref().unwrap_or(""))
        .ok_or("cannot read target disk busy counter before write phase")?;
    let mut write_io_end = None;
    let mut write_disk_end = None;
    let mut write_counters_end = None;
    let mut write_histograms = None;
    let mut write_last_ack: Option<Instant> = None;
    let mut previous_seq = 0_u64;
    let mut written_transactions = 0_u64;
    let mut write_batches = 0_u64;

    while previous_seq < config.iterations {
        if write_phase_start.is_none() {
            let now = Instant::now();
            write_phase_start = Some(now);
            write_cpu_start = Some(ProcessTime::now());
            let _ = heartbeat_start_tx
                .take()
                .expect("write heartbeat sender is present")
                .send(now);
        }
        let count = ((config.iterations - previous_seq) as usize).min(batch_size);
        let batch_started = Instant::now();
        let build_started = Instant::now();
        let mut entries = Vec::with_capacity(count);
        for seq in previous_seq + 1..=previous_seq + count as u64 {
            match store::LedgerEntry::for_seq(seq) {
                Ok(entry) => entries.push(entry),
                Err(error) => {
                    stop_heartbeat(&heartbeat_stats, Instant::now());
                    let _ = heartbeat_task.await;
                    return Err(io::Error::other(error).into());
                }
            }
        }
        let (batch, balance_updates) =
            match store::build_write_batch(&entries, &balances, previous_seq) {
                Ok(prepared) => prepared,
                Err(error) => {
                    stop_heartbeat(&heartbeat_stats, Instant::now());
                    let _ = heartbeat_task.await;
                    return Err(io::Error::other(error).into());
                }
            };
        let dispatch_at = Instant::now();
        write_build.add(duration_ns(dispatch_at.duration_since(build_started)));
        let trace_build_ns = duration_ns(dispatch_at.duration_since(batch_started));

        let db = Arc::clone(&db);
        let collect_diagnostic = config.diagnostic;
        let write_result = blocking_call(move || {
            let worker_started_at = if collect_diagnostic {
                Some(Instant::now())
            } else {
                None
            };
            let thread_id = collect_diagnostic
                .then(worker_thread_id)
                .unwrap_or_default();
            let cpu_id = collect_diagnostic.then(worker_cpu_id).flatten();
            let mut options = WriteOptions::default();
            options.set_sync(true);
            options.disable_wal(false);
            let perf_context = if collect_diagnostic {
                rocksdb::perf::set_perf_stats(PerfStatsLevel::EnableTime);
                let mut context = PerfContext::default();
                context.reset();
                Some(context)
            } else {
                None
            };
            let native_observation = (|| -> Result<_, String> {
                let native_started_at = Instant::now();
                let thread_counters_start = if collect_diagnostic {
                    Some(ThreadResourceSnapshot::capture().map_err(|error| {
                        format!(
                            "could not read worker thread counters before RocksDB write: {error}"
                        )
                    })?)
                } else {
                    None
                };
                let write_result = db.write_opt(batch, &options);
                let thread_counters_end = if collect_diagnostic {
                    Some(ThreadResourceSnapshot::capture().map_err(|error| {
                        format!(
                            "could not read worker thread counters after RocksDB write: {error}"
                        )
                    })?)
                } else {
                    None
                };
                let native_finished_at = Instant::now();
                let native_call = match (thread_counters_start, thread_counters_end) {
                    (Some(start), Some(end)) => end.delta_since(start).map_err(|error| {
                        format!(
                            "could not measure worker thread resources for RocksDB write: {error}"
                        )
                    })?,
                    (None, None) => NativeCallMetrics::default(),
                    _ => unreachable!("diagnostic thread counters use a single mode"),
                };
                Ok((
                    native_started_at,
                    native_finished_at,
                    native_call,
                    write_result,
                ))
            })();
            let perf = perf_context.as_ref().map(PerfSnapshot::read);
            if collect_diagnostic {
                rocksdb::perf::set_perf_stats(PerfStatsLevel::Disable);
            }
            let finished_at = if collect_diagnostic {
                Some(Instant::now())
            } else {
                None
            };
            let (native_started_at, native_finished_at, native_call, write_result) =
                native_observation?;
            write_result
                .map_err(|error| format!("RocksDB synchronous batch write failed: {error}"))?;
            let worker = if collect_diagnostic {
                Some(WorkerObservation {
                    started_at: worker_started_at.expect("diagnostic worker has start timestamp"),
                    native_started_at,
                    native_finished_at,
                    finished_at: finished_at.expect("diagnostic worker has finish timestamp"),
                    thread_id,
                    cpu_id,
                    native_call,
                    perf: perf.expect("diagnostic worker has PerfContext snapshot"),
                })
            } else {
                None
            };
            Ok((
                duration_ns(native_finished_at.duration_since(native_started_at)),
                worker,
            ))
        })
        .await;
        let await_resumed_at = if config.diagnostic {
            Some(Instant::now())
        } else {
            None
        };
        let (native_write_ns, worker) = match write_result {
            Ok(result) => result,
            Err(error) => {
                stop_heartbeat(&heartbeat_stats, Instant::now());
                let _ = heartbeat_task.await;
                return Err(io::Error::other(error).into());
            }
        };
        write_db_call.add(native_write_ns);
        store::apply_balance_updates(&mut balances, &balance_updates);
        let acknowledged_at = Instant::now();
        if config.diagnostic {
            diagnostic_rows.push(DiagnosticBatch::from_worker(
                "write",
                write_batches + 1,
                count,
                duration_ns(acknowledged_at.duration_since(batch_started)),
                trace_build_ns,
                dispatch_at,
                await_resumed_at.expect("diagnostic write has await-resume timestamp"),
                acknowledged_at,
                0,
                worker.expect("diagnostic write returns worker observation"),
            ));
        }
        let final_write_batch = previous_seq + count as u64 == config.iterations;
        if final_write_batch {
            write_cpu_seconds_end = Some(
                write_cpu_start
                    .as_ref()
                    .expect("write CPU timer started with first batch")
                    .elapsed()
                    .as_secs_f64(),
            );
            write_counters_end = Some(RdbCounters::from_options(&write_options));
            write_histograms = Some(RdbHistograms::from_options(&write_options));
            write_io_end = proc_io();
            write_disk_end = disk_busy_ms(write_preflight.disk_device.as_deref().unwrap_or(""));
        }
        write_latency.add_batch(
            duration_ns(acknowledged_at.duration_since(batch_started)),
            count,
        );
        previous_seq += count as u64;
        written_transactions += count as u64;
        write_batches += 1;
        write_last_ack = Some(acknowledged_at);
    }

    let write_phase_end = write_last_ack.ok_or("no RocksDB write batch was acknowledged")?;
    stop_heartbeat(&heartbeat_stats, write_phase_end);
    let write_wall = write_phase_end
        .duration_since(write_phase_start.ok_or("write phase start was not recorded")?);
    let write_cpu_seconds = write_cpu_seconds_end.ok_or("write CPU end sample was not recorded")?;
    let write_io_end = write_io_end.ok_or("cannot read process I/O counters after write phase")?;
    let write_disk_end =
        write_disk_end.ok_or("cannot read target disk busy counter after write phase")?;
    let write_properties = {
        let db_for_properties = Arc::clone(&db);
        blocking_call(move || Ok(RdbProperties::read(&db_for_properties)))
            .await
            .map_err(io::Error::other)?
    };
    heartbeat_task.await?;
    let write_heartbeat = heartbeat_stats
        .lock()
        .expect("heartbeat stats mutex poisoned")
        .clone();

    let db_for_validation = Arc::clone(&db);
    let expected_balances = balances;
    let expected_seq = config.iterations;
    let touched_users = config.iterations.min(store::USER_COUNT as u64) as usize;
    blocking_call(move || {
        store::validate_stored_state(
            &db_for_validation,
            &expected_balances,
            expected_seq,
            touched_users,
        )
    })
    .await
    .map_err(io::Error::other)?;
    let write_stats = RdbObservation {
        counters: write_counters_end
            .ok_or("RocksDB write-phase counter snapshot was not recorded")?
            .delta_since(&write_counters_start),
        histograms: write_histograms
            .ok_or("RocksDB write-phase histogram snapshot was not recorded")?,
        properties: write_properties,
    };
    drop(db);
    drop(write_options);

    let query_options = store::rocksdb_options();
    query_options.set_statistics_level(rocksdb::statistics::StatsLevel::ExceptDetailedTimers);
    let query_db = Arc::new(store::open_db(&query_options, &db_dir)?);
    let query_preflight = preflight(
        config,
        "query",
        &label,
        batch_size,
        config.free_reserve_bytes.unwrap_or(GIB),
        batch_working_bytes,
    )?;
    let query_counters_start = RdbCounters::from_options(&query_options);

    let query_heartbeat_stats = Arc::new(Mutex::new(HeartbeatStats::default()));
    let (query_heartbeat_start_tx, query_heartbeat_start_rx) = tokio::sync::oneshot::channel();
    let query_heartbeat_task = tokio::spawn(run_heartbeat(
        Arc::clone(&query_heartbeat_stats),
        query_heartbeat_start_rx,
    ));
    let mut query_heartbeat_start_tx = Some(query_heartbeat_start_tx);
    let mut query_latency = TransactionLatencies::default();
    let mut query_prep = Latencies::default();
    let mut query_read = Latencies::default();
    let mut query_decode = Latencies::default();
    let query_io_start = proc_io().ok_or("cannot read process I/O counters before query phase")?;
    let query_disk_start = disk_busy_ms(query_preflight.disk_device.as_deref().unwrap_or(""))
        .ok_or("cannot read target disk busy counter before query phase")?;
    let mut query_phase_start: Option<Instant> = None;
    let mut query_cpu_start: Option<ProcessTime> = None;
    let mut query_cpu_seconds_end = None;
    let mut query_last_finish: Option<Instant> = None;
    let mut query_io_end = None;
    let mut query_disk_end = None;
    let mut query_counters_end = None;
    let mut query_histograms = None;
    let mut query_batches = 0_u64;
    let mut queried_transactions = 0_u64;

    for ids in query_ids.chunks(batch_size) {
        if query_phase_start.is_none() {
            let now = Instant::now();
            query_phase_start = Some(now);
            query_cpu_start = Some(ProcessTime::now());
            let _ = query_heartbeat_start_tx
                .take()
                .expect("query heartbeat sender is present")
                .send(now);
        }
        let batch_started = Instant::now();
        let prep_started = Instant::now();
        let keys: Vec<_> = ids.iter().map(|tx_id| store::ledger_key(*tx_id)).collect();
        let expected_ids = ids.to_vec();
        query_prep.add(duration_ns(prep_started.elapsed()));
        let dispatch_at = Instant::now();
        let trace_prep_ns = duration_ns(dispatch_at.duration_since(batch_started));

        let db = Arc::clone(&query_db);
        let collect_diagnostic = config.diagnostic;
        let query_result = blocking_call(move || {
            let worker_started_at = if collect_diagnostic {
                Some(Instant::now())
            } else {
                None
            };
            let thread_id = collect_diagnostic.then(worker_thread_id).unwrap_or_default();
            let cpu_id = collect_diagnostic.then(worker_cpu_id).flatten();
            let perf_context = if collect_diagnostic {
                rocksdb::perf::set_perf_stats(PerfStatsLevel::EnableTime);
                let mut context = PerfContext::default();
                context.reset();
                Some(context)
            } else {
                None
            };
            let cf = db
                .cf_handle("default")
                .ok_or_else(|| "RocksDB default column family is unavailable".to_owned())?;
            let native_observation = (|| -> Result<_, String> {
                let native_started_at = Instant::now();
                let thread_counters_start = if collect_diagnostic {
                    Some(ThreadResourceSnapshot::capture().map_err(|error| {
                        format!("could not read worker thread counters before RocksDB multiget: {error}")
                    })?)
                } else {
                    None
                };
                let values = db.batched_multi_get_cf(cf, keys.iter(), false);
                let thread_counters_end = if collect_diagnostic {
                    Some(ThreadResourceSnapshot::capture().map_err(|error| {
                        format!("could not read worker thread counters after RocksDB multiget: {error}")
                    })?)
                } else {
                    None
                };
                let native_finished_at = Instant::now();
                let native_call = match (thread_counters_start, thread_counters_end) {
                    (Some(start), Some(end)) => end.delta_since(start).map_err(|error| {
                        format!("could not measure worker thread resources for RocksDB multiget: {error}")
                    })?,
                    (None, None) => NativeCallMetrics::default(),
                    _ => unreachable!("diagnostic thread counters use a single mode"),
                };
                Ok((native_started_at, native_finished_at, native_call, values))
            })();
            let perf = perf_context.as_ref().map(PerfSnapshot::read);
            if collect_diagnostic {
                rocksdb::perf::set_perf_stats(PerfStatsLevel::Disable);
            }
            let (native_started_at, native_finished_at, native_call, values) =
                native_observation?;
            let read_ns = duration_ns(native_finished_at.duration_since(native_started_at));
            if values.len() != expected_ids.len() {
                return Err(format!(
                    "RocksDB multiget returned {} values for {} requested keys",
                    values.len(),
                    expected_ids.len()
                ));
            }
            let decode_started = Instant::now();
            let validation_result = (|| {
                for (index, (tx_id, value)) in expected_ids.iter().zip(values).enumerate() {
                    let value = value
                        .map_err(|error| {
                            format!("RocksDB multiget failed at result {index}: {error}")
                        })?
                        .ok_or_else(|| {
                            format!("RocksDB key at result {index} is missing")
                        })?;
                    let decoded = store::decode_ledger_entry(value.as_ref())?;
                    let expected = store::LedgerEntry::for_seq(*tx_id)?;
                    if decoded != expected {
                        return Err(format!(
                            "ledger lookup mismatch for tx_id={tx_id}: decoded={decoded:?}, expected={expected:?}"
                        ));
                    }
                }
                Ok(())
            })();
            let decode_finished_at = Instant::now();
            let decode_ns = duration_ns(decode_finished_at.duration_since(decode_started));
            let finished_at = if collect_diagnostic {
                Some(Instant::now())
            } else {
                None
            };
            validation_result?;
            let worker = if collect_diagnostic {
                Some(WorkerObservation {
                    started_at: worker_started_at.expect("diagnostic worker has start timestamp"),
                    native_started_at,
                    native_finished_at,
                    finished_at: finished_at.expect("diagnostic worker has finish timestamp"),
                    thread_id,
                    cpu_id,
                    native_call,
                    perf: perf.expect("diagnostic worker has PerfContext snapshot"),
                })
            } else {
                None
            };
            Ok((read_ns, decode_ns, worker))
        })
        .await;
        let await_resumed_at = if config.diagnostic {
            Some(Instant::now())
        } else {
            None
        };
        let (read_ns, decode_ns, worker) = match query_result {
            Ok(metrics) => metrics,
            Err(error) => {
                stop_heartbeat(&query_heartbeat_stats, Instant::now());
                let _ = query_heartbeat_task.await;
                return Err(io::Error::other(error).into());
            }
        };
        query_read.add(read_ns);
        query_decode.add(decode_ns);
        let finished_at = Instant::now();
        if config.diagnostic {
            diagnostic_rows.push(DiagnosticBatch::from_worker(
                "query",
                query_batches + 1,
                ids.len(),
                duration_ns(finished_at.duration_since(batch_started)),
                trace_prep_ns,
                dispatch_at,
                await_resumed_at.expect("diagnostic query has await-resume timestamp"),
                finished_at,
                decode_ns,
                worker.expect("diagnostic query returns worker observation"),
            ));
        }
        let final_query_batch = queried_transactions + ids.len() as u64 == config.iterations;
        if final_query_batch {
            query_cpu_seconds_end = Some(
                query_cpu_start
                    .as_ref()
                    .expect("query CPU timer started with first batch")
                    .elapsed()
                    .as_secs_f64(),
            );
            query_counters_end = Some(RdbCounters::from_options(&query_options));
            query_histograms = Some(RdbHistograms::from_options(&query_options));
            query_io_end = proc_io();
            query_disk_end = disk_busy_ms(query_preflight.disk_device.as_deref().unwrap_or(""));
        }
        query_latency.add_batch(
            duration_ns(finished_at.duration_since(batch_started)),
            ids.len(),
        );
        queried_transactions += ids.len() as u64;
        query_batches += 1;
        query_last_finish = Some(finished_at);
    }

    let query_phase_end = query_last_finish.ok_or("no ledger lookup batch completed")?;
    stop_heartbeat(&query_heartbeat_stats, query_phase_end);
    let query_wall = query_phase_end
        .duration_since(query_phase_start.ok_or("query phase start was not recorded")?);
    let query_cpu_seconds = query_cpu_seconds_end.ok_or("query CPU end sample was not recorded")?;
    let query_io_end = query_io_end.ok_or("cannot read process I/O counters after query phase")?;
    let query_disk_end =
        query_disk_end.ok_or("cannot read target disk busy counter after query phase")?;
    query_heartbeat_task.await?;
    let query_heartbeat = query_heartbeat_stats
        .lock()
        .expect("query heartbeat stats mutex poisoned")
        .clone();

    let query_properties = {
        let db_for_properties = Arc::clone(&query_db);
        blocking_call(move || Ok(RdbProperties::read(&db_for_properties)))
            .await
            .map_err(io::Error::other)?
    };
    let query_stats = RdbObservation {
        counters: query_counters_end
            .ok_or("RocksDB query-phase counter snapshot was not recorded")?
            .delta_since(&query_counters_start),
        histograms: query_histograms
            .ok_or("RocksDB query-phase histogram snapshot was not recorded")?,
        properties: query_properties,
    };

    if written_transactions != config.iterations || queried_transactions != config.iterations {
        return Err(format!(
            "transaction count mismatch: wrote={written_transactions}, queried={queried_transactions}, expected={}",
            config.iterations
        )
        .into());
    }
    drop(query_db);
    drop(query_options);
    guard.cleanup()?;
    if config.diagnostic {
        let trace_path = config.output_dir.join(format!(
            "rocksdb-tokio-sequential-{label}-{timestamp}.trace.csv"
        ));
        fs::write(&trace_path, diagnostic_trace_csv(&diagnostic_rows))?;
        eprintln!(
            "DIAGNOSTIC_TRACE path={} rows={} db_cleanup=complete",
            trace_path.display(),
            diagnostic_rows.len()
        );
    }

    let mut write_metrics = WriteMetrics {
        transactions: written_transactions,
        batches: write_batches,
        wall: write_wall,
        cpu_seconds: write_cpu_seconds,
        latency: write_latency,
        build: write_build,
        db_write: write_db_call,
        heartbeat: write_heartbeat,
        io_start: write_io_start,
        io_end: write_io_end,
        disk_busy_start_ms: Some(write_disk_start),
        disk_busy_end_ms: Some(write_disk_end),
    };
    let mut query_metrics = QueryMetrics {
        transactions: queried_transactions,
        batches: query_batches,
        wall: query_wall,
        cpu_seconds: query_cpu_seconds,
        latency: query_latency,
        prep: query_prep,
        read: query_read,
        decode: query_decode,
        heartbeat: query_heartbeat,
        io_start: query_io_start,
        io_end: query_io_end,
        disk_busy_start_ms: Some(query_disk_start),
        disk_busy_end_ms: Some(query_disk_end),
    };
    let csv = trial_csv(
        config,
        batch_size,
        repetition,
        &mut write_metrics,
        &mut query_metrics,
        write_preflight,
        &query_preflight,
        &write_stats,
        &query_stats,
    )?;
    Ok(TrialResult { csv })
}

async fn blocking_call<T, F>(operation: F) -> Result<T, String>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, String> + Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|error| format!("Tokio blocking-pool task failed: {error}"))?
}

fn stop_heartbeat(stats: &Arc<Mutex<HeartbeatStats>>, end: Instant) {
    stats.lock().expect("heartbeat stats mutex poisoned").end = Some(end);
}

fn duration_ns(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

struct TrialResult {
    csv: String,
}

#[derive(Default, Clone)]
struct RdbCounters {
    wal_file_synced: u64,
    wal_file_bytes: u64,
    write_with_wal: u64,
    flush_write_bytes: u64,
    compaction_read_bytes: u64,
    compaction_write_bytes: u64,
    stall_micros: u64,
}

impl RdbCounters {
    fn from_options(options: &Options) -> Self {
        Self {
            wal_file_synced: options.get_ticker_count(Ticker::WalFileSynced),
            wal_file_bytes: options.get_ticker_count(Ticker::WalFileBytes),
            write_with_wal: options.get_ticker_count(Ticker::WriteWithWal),
            flush_write_bytes: options.get_ticker_count(Ticker::FlushWriteBytes),
            compaction_read_bytes: options.get_ticker_count(Ticker::CompactReadBytes),
            compaction_write_bytes: options.get_ticker_count(Ticker::CompactWriteBytes),
            stall_micros: options.get_ticker_count(Ticker::StallMicros),
        }
    }

    fn delta_since(&self, before: &Self) -> Self {
        Self {
            wal_file_synced: self.wal_file_synced.saturating_sub(before.wal_file_synced),
            wal_file_bytes: self.wal_file_bytes.saturating_sub(before.wal_file_bytes),
            write_with_wal: self.write_with_wal.saturating_sub(before.write_with_wal),
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

#[derive(Default, Clone)]
struct HistSummary {
    count: u64,
    sum: u64,
    average: f64,
    p95: f64,
    p99: f64,
    max: f64,
}

impl HistSummary {
    fn from_data(data: &HistogramData) -> Self {
        Self {
            count: data.count(),
            sum: data.sum(),
            average: data.average(),
            p95: data.p95(),
            p99: data.p99(),
            max: data.max(),
        }
    }
}

#[derive(Default, Clone)]
struct RdbHistograms {
    wal_file_sync_micros: HistSummary,
    write_stall: HistSummary,
}

impl RdbHistograms {
    fn from_options(options: &Options) -> Self {
        Self {
            wal_file_sync_micros: HistSummary::from_data(
                &options.get_histogram_data(Histogram::WalFileSyncMicros),
            ),
            write_stall: HistSummary::from_data(&options.get_histogram_data(Histogram::WriteStall)),
        }
    }
}

#[derive(Default, Clone)]
struct RdbProperties {
    num_immutable_memtables: Option<u64>,
    memtable_flush_pending: Option<u64>,
    num_running_flushes: Option<u64>,
    compaction_pending: Option<u64>,
    num_running_compactions: Option<u64>,
    pending_compaction_bytes: Option<u64>,
    is_write_stopped: Option<u64>,
    actual_delayed_write_rate: Option<u64>,
    background_errors: Option<u64>,
}

impl RdbProperties {
    fn read(db: &DB) -> Self {
        let property = |name: &str| db.property_int_value(name).ok().flatten();
        Self {
            num_immutable_memtables: property("rocksdb.num-immutable-mem-table"),
            memtable_flush_pending: property("rocksdb.mem-table-flush-pending"),
            num_running_flushes: property("rocksdb.num-running-flushes"),
            compaction_pending: property("rocksdb.compaction-pending"),
            num_running_compactions: property("rocksdb.num-running-compactions"),
            pending_compaction_bytes: property("rocksdb.estimate-pending-compaction-bytes"),
            is_write_stopped: property("rocksdb.is-write-stopped"),
            actual_delayed_write_rate: property("rocksdb.actual-delayed-write-rate"),
            background_errors: property("rocksdb.background-errors"),
        }
    }
}

struct RdbObservation {
    counters: RdbCounters,
    histograms: RdbHistograms,
    properties: RdbProperties,
}

fn trial_csv(
    config: &Config,
    batch_size: usize,
    repetition: usize,
    write: &mut WriteMetrics,
    query: &mut QueryMetrics,
    write_preflight: &SystemSample,
    query_preflight: &SystemSample,
    write_stats: &RdbObservation,
    query_stats: &RdbObservation,
) -> Result<String, Box<dyn Error>> {
    let mut fields = vec![
        config.iterations.to_string(),
        store::USER_COUNT.to_string(),
        store::AMOUNT_CENTS.to_string(),
        batch_size.to_string(),
        repetition.to_string(),
        write.batches.to_string(),
        query.batches.to_string(),
        write.transactions.to_string(),
        query.transactions.to_string(),
        format!(
            "{:.6}",
            write.transactions as f64 / write.wall.as_secs_f64()
        ),
        format!("{:.9}", write.wall.as_secs_f64()),
        format!("{:.9}", write.cpu_seconds),
        format!("{:.9}", write.wall.as_secs_f64()),
        format!("{:.6}", write.cpu_seconds / write.wall.as_secs_f64()),
        format!(
            "{:.6}",
            query.transactions as f64 / query.wall.as_secs_f64()
        ),
        format!("{:.9}", query.wall.as_secs_f64()),
        format!("{:.9}", query.cpu_seconds),
        format!("{:.9}", query.wall.as_secs_f64()),
        format!("{:.6}", query.cpu_seconds / query.wall.as_secs_f64()),
    ];

    append_transaction_latency(&mut fields, "write_tx_latency", &mut write.latency);
    append_latency_columns(&mut fields, "write_batch_build", &mut write.build);
    append_latency_columns(&mut fields, "write_db_write_opt_call", &mut write.db_write);
    append_heartbeat_columns(&mut fields, "write_heartbeat", &mut write.heartbeat);

    append_transaction_latency(&mut fields, "query_tx_latency", &mut query.latency);
    append_latency_columns(&mut fields, "query_batch_prep", &mut query.prep);
    append_latency_columns(&mut fields, "query_batched_multi_get_call", &mut query.read);
    append_latency_columns(&mut fields, "query_decode_validate", &mut query.decode);
    append_heartbeat_columns(&mut fields, "query_heartbeat", &mut query.heartbeat);

    append_preflight_columns(&mut fields, "write_preflight", write_preflight, config);
    append_preflight_columns(&mut fields, "query_preflight", query_preflight, config);
    append_rocksdb_columns(&mut fields, "write_rocksdb", write_stats);
    append_rocksdb_columns(&mut fields, "query_rocksdb", query_stats);
    append_io_columns(&mut fields, "write", &write.io_start, &write.io_end);
    append_io_columns(&mut fields, "query", &query.io_start, &query.io_end);
    append_disk_columns(&mut fields, "write", write);
    append_disk_columns(&mut fields, "query", query);
    Ok(fields.join(","))
}

fn append_transaction_latency(
    fields: &mut Vec<String>,
    name: &str,
    latency: &mut TransactionLatencies,
) {
    fields.extend([
        latency.transactions.to_string(),
        latency.total_ns.to_string(),
        format!("{:.6}", latency.mean()),
        latency.batches.to_string(),
        latency
            .quantile(50)
            .map_or(String::new(), |value| value.to_string()),
        latency
            .quantile(95)
            .map_or(String::new(), |value| value.to_string()),
        latency
            .quantile(99)
            .map_or(String::new(), |value| value.to_string()),
    ]);
    let _ = name;
}

fn append_latency_columns(fields: &mut Vec<String>, name: &str, latency: &mut Latencies) {
    fields.extend([
        latency.count.to_string(),
        latency.total_ns.to_string(),
        format!("{:.6}", latency.mean()),
        latency.values.len().to_string(),
        latency
            .quantile(50)
            .map_or(String::new(), |value| value.to_string()),
        latency
            .quantile(95)
            .map_or(String::new(), |value| value.to_string()),
        latency
            .quantile(99)
            .map_or(String::new(), |value| value.to_string()),
    ]);
    let _ = name;
}

fn append_heartbeat_columns(fields: &mut Vec<String>, name: &str, stats: &mut HeartbeatStats) {
    fields.push(stats.lateness_ns.len().to_string());
    fields.push(stats.missed_tick_count.to_string());
    fields.push(stats.lateness_ns.len().to_string());
    fields.push(
        quantile(&mut stats.lateness_ns, 50).map_or(String::new(), |value| value.to_string()),
    );
    fields.push(
        quantile(&mut stats.lateness_ns, 95).map_or(String::new(), |value| value.to_string()),
    );
    fields.push(
        quantile(&mut stats.lateness_ns, 99).map_or(String::new(), |value| value.to_string()),
    );
    fields.push(
        quantile(&mut stats.lateness_ns, 100).map_or(String::new(), |value| value.to_string()),
    );
    let _ = name;
}

fn append_preflight_columns(
    fields: &mut Vec<String>,
    name: &str,
    sample: &SystemSample,
    config: &Config,
) {
    fields.extend([
        csv_quote(&sample.fs_type),
        sample.free_bytes.to_string(),
        sample.required_free_bytes.to_string(),
        sample.mem_available.to_string(),
        sample.required_memory_bytes.to_string(),
        sample.rss_bytes.to_string(),
        optional_float(sample.cpu_busy_pct),
        optional_float(sample.disk_busy_pct),
        csv_quote(sample.disk_device.as_deref().unwrap_or("")),
        config.max_cpu_pct.to_string(),
        config.max_disk_pct.to_string(),
        config.min_memory_bytes.to_string(),
        config.observation_ms.to_string(),
        config.timeout_ms.to_string(),
        optional_u64(sample.disk_busy_ms),
    ]);
    let _ = name;
}

fn append_rocksdb_columns(fields: &mut Vec<String>, name: &str, observation: &RdbObservation) {
    let c = &observation.counters;
    fields.extend([
        c.wal_file_synced.to_string(),
        c.wal_file_bytes.to_string(),
        c.write_with_wal.to_string(),
        c.flush_write_bytes.to_string(),
        c.compaction_read_bytes.to_string(),
        c.compaction_write_bytes.to_string(),
        c.stall_micros.to_string(),
    ]);
    append_histogram(fields, &observation.histograms.wal_file_sync_micros);
    append_histogram(fields, &observation.histograms.write_stall);
    let p = &observation.properties;
    for value in [
        p.num_immutable_memtables,
        p.memtable_flush_pending,
        p.num_running_flushes,
        p.compaction_pending,
        p.num_running_compactions,
        p.pending_compaction_bytes,
        p.is_write_stopped,
        p.actual_delayed_write_rate,
        p.background_errors,
    ] {
        fields.push(optional_u64(value));
    }
    let _ = name;
}

fn append_histogram(fields: &mut Vec<String>, histogram: &HistSummary) {
    fields.extend([
        histogram.count.to_string(),
        histogram.sum.to_string(),
        optional_float(Some(histogram.average)),
        optional_float(Some(histogram.p95)),
        optional_float(Some(histogram.p99)),
        optional_float(Some(histogram.max)),
    ]);
}

fn append_io_columns(fields: &mut Vec<String>, phase: &str, start: &ProcIo, end: &ProcIo) {
    for (a, b) in [
        (start.rchar, end.rchar),
        (start.wchar, end.wchar),
        (start.syscr, end.syscr),
        (start.syscw, end.syscw),
        (start.read_bytes, end.read_bytes),
        (start.write_bytes, end.write_bytes),
        (start.cancelled_write_bytes, end.cancelled_write_bytes),
    ] {
        fields.push(a.to_string());
        fields.push(b.to_string());
    }
    let _ = phase;
}

fn append_disk_columns(fields: &mut Vec<String>, phase: &str, metrics: &impl DiskMetrics) {
    fields.push(optional_u64(metrics.disk_busy_start_ms()));
    fields.push(optional_u64(metrics.disk_busy_end_ms()));
    let _ = phase;
}

trait DiskMetrics {
    fn disk_busy_start_ms(&self) -> Option<u64>;
    fn disk_busy_end_ms(&self) -> Option<u64>;
}

impl DiskMetrics for WriteMetrics {
    fn disk_busy_start_ms(&self) -> Option<u64> {
        self.disk_busy_start_ms
    }
    fn disk_busy_end_ms(&self) -> Option<u64> {
        self.disk_busy_end_ms
    }
}

impl DiskMetrics for QueryMetrics {
    fn disk_busy_start_ms(&self) -> Option<u64> {
        self.disk_busy_start_ms
    }
    fn disk_busy_end_ms(&self) -> Option<u64> {
        self.disk_busy_end_ms
    }
}

fn print_csv_header() {
    let mut fields: Vec<String> = "iterations,users,amount_cents,batch_size,repetition,write_batches,query_batches,write_transactions,query_transactions,write_rps,write_wall_seconds,write_process_cpu_seconds,write_cpu_measurement_wall_seconds,write_cpu_core_equivalents,query_rps,query_wall_seconds,query_process_cpu_seconds,query_cpu_measurement_wall_seconds,query_cpu_core_equivalents"
        .split(',')
        .map(str::to_owned)
        .collect();
    append_transaction_latency_header(&mut fields, "write_tx_latency");
    append_latency_header(&mut fields, "write_batch_build");
    append_latency_header(&mut fields, "write_db_write_opt_call");
    append_heartbeat_header(&mut fields, "write_heartbeat");
    append_transaction_latency_header(&mut fields, "query_tx_latency");
    append_latency_header(&mut fields, "query_batch_prep");
    append_latency_header(&mut fields, "query_batched_multi_get_call");
    append_latency_header(&mut fields, "query_decode_validate");
    append_heartbeat_header(&mut fields, "query_heartbeat");
    append_preflight_header(&mut fields, "write_preflight");
    append_preflight_header(&mut fields, "query_preflight");
    append_rocksdb_header(&mut fields, "write_rocksdb");
    append_rocksdb_header(&mut fields, "query_rocksdb");
    append_io_header(&mut fields, "write");
    append_io_header(&mut fields, "query");
    fields.extend([
        "write_measurement_disk_busy_ms_start".to_owned(),
        "write_measurement_disk_busy_ms_end".to_owned(),
        "query_measurement_disk_busy_ms_start".to_owned(),
        "query_measurement_disk_busy_ms_end".to_owned(),
    ]);
    println!("{}", fields.join(","));
}

fn append_transaction_latency_header(fields: &mut Vec<String>, name: &str) {
    fields.extend([
        format!("{name}_transaction_count"),
        format!("{name}_total_ns"),
        format!("{name}_mean_ns"),
        format!("{name}_batch_observation_count"),
        format!("{name}_p50_ns"),
        format!("{name}_p95_ns"),
        format!("{name}_p99_ns"),
    ]);
}

fn append_latency_header(fields: &mut Vec<String>, name: &str) {
    fields.extend([
        format!("{name}_batch_count"),
        format!("{name}_total_ns"),
        format!("{name}_mean_ns_per_batch"),
        format!("{name}_sample_count"),
        format!("{name}_p50_ns"),
        format!("{name}_p95_ns"),
        format!("{name}_p99_ns"),
    ]);
}

fn append_heartbeat_header(fields: &mut Vec<String>, name: &str) {
    fields.extend([
        format!("{name}_tick_count"),
        format!("{name}_missed_tick_count"),
        format!("{name}_lateness_sample_count"),
        format!("{name}_lateness_p50_ns"),
        format!("{name}_lateness_p95_ns"),
        format!("{name}_lateness_p99_ns"),
        format!("{name}_lateness_max_ns"),
    ]);
}

fn append_preflight_header(fields: &mut Vec<String>, name: &str) {
    for suffix in [
        "fs_type",
        "free_bytes",
        "required_free_bytes",
        "mem_available_bytes",
        "required_memory_bytes",
        "process_rss_bytes",
        "cpu_busy_pct",
        "disk_busy_pct",
        "disk_device",
        "cpu_limit_pct",
        "disk_limit_pct",
        "min_mem_bytes",
        "observation_ms",
        "timeout_ms",
        "disk_busy_ms",
    ] {
        fields.push(format!("{name}_{suffix}"));
    }
}

fn append_rocksdb_header(fields: &mut Vec<String>, name: &str) {
    for suffix in [
        "wal_file_synced_delta",
        "wal_file_bytes_delta",
        "write_with_wal_delta",
        "flush_write_bytes_delta",
        "compaction_read_bytes_delta",
        "compaction_write_bytes_delta",
        "stall_micros_delta",
    ] {
        fields.push(format!("{name}_{suffix}"));
    }
    append_histogram_header(fields, &format!("{name}_wal_file_sync_hist_since_open"));
    append_histogram_header(fields, &format!("{name}_write_stall_hist_since_open"));
    for suffix in [
        "num_immutable_memtables",
        "memtable_flush_pending",
        "num_running_flushes",
        "compaction_pending",
        "num_running_compactions",
        "estimate_pending_compaction_bytes",
        "is_write_stopped",
        "actual_delayed_write_rate",
        "background_errors",
    ] {
        fields.push(format!("{name}_{suffix}_after_phase"));
    }
}

fn append_histogram_header(fields: &mut Vec<String>, name: &str) {
    for suffix in [
        "count",
        "sum_us",
        "average_us",
        "p95_us",
        "p99_us",
        "max_us",
    ] {
        fields.push(format!("{name}_{suffix}"));
    }
}

fn append_io_header(fields: &mut Vec<String>, phase: &str) {
    for name in [
        "rchar",
        "wchar",
        "syscr",
        "syscw",
        "read_bytes",
        "write_bytes",
        "cancelled_write_bytes",
    ] {
        fields.push(format!("{phase}_proc_io_{name}_start"));
        fields.push(format!("{phase}_proc_io_{name}_end"));
    }
}

fn quantile(values: &mut [u64], numerator: usize) -> Option<u64> {
    if values.is_empty() {
        return None;
    }
    values.sort_unstable();
    let rank = values.len().saturating_mul(numerator).div_ceil(100).max(1);
    Some(values[rank - 1])
}

fn optional_float(value: Option<f64>) -> String {
    value.map_or_else(String::new, |number| format!("{number:.3}"))
}

fn optional_u64(value: Option<u64>) -> String {
    value.map_or_else(String::new, |number| number.to_string())
}

fn csv_quote(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
