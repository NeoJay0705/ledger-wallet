use cpu_time::ProcessTime;
#[allow(dead_code)]
#[path = "support/wal.rs"]
mod wal;
#[allow(dead_code)]
#[path = "support/wal_tokio.rs"]
mod wal_tokio;

use std::collections::BTreeMap;
use std::error::Error;
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use wal::{Record, generate_records, recover_and_verify};
use wal_tokio::WalWriter;

const DEFAULT_ITERATIONS: usize = 10_000_000;
const DEFAULT_BATCHES: &[usize] = &[64, 256, 1024, 2048, 4096];
const MAIN_ROTATION: u64 = 1024 * 1024 * 1024;
const EXTRA_ROTATION: u64 = 256 * 1024 * 1024;
const GIB: u64 = 1024 * 1024 * 1024;

#[derive(Debug)]
struct Config {
    iterations: usize,
    repetitions: usize,
    batch_sizes: Vec<usize>,
    rotations: Option<Vec<u64>>,
    output_dir: PathBuf,
    latency_stride: u64,
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
            rotations: None,
            output_dir: PathBuf::from("target/wal-tokio-benchmark"),
            latency_stride: 1024,
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
        if self.values.is_empty() {
            return None;
        }
        self.values.sort_unstable();
        let rank = self
            .values
            .len()
            .saturating_mul(numerator)
            .div_ceil(100)
            .max(1);
        Some(self.values[rank - 1])
    }
}

#[derive(Clone, Default)]
struct HeartbeatStats {
    start: Option<Instant>,
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
        if actual >= start {
            let lateness = observed.saturating_duration_since(scheduled);
            stats
                .lateness_ns
                .push(lateness.as_nanos().min(u128::from(u64::MAX)) as u64);
            let missed = lateness.as_nanos() / period_ns;
            stats.missed_tick_count = stats
                .missed_tick_count
                .saturating_add(missed.min(u128::from(u64::MAX)) as u64);
        }
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
}

struct TrialDirectory(Option<PathBuf>);

impl TrialDirectory {
    fn cleanup(mut self) -> io::Result<()> {
        if let Some(path) = self.0.as_ref() {
            cleanup_trial_directory(&path)?;
            self.0 = None;
        }
        Ok(())
    }
}

impl Drop for TrialDirectory {
    fn drop(&mut self) {
        if let Some(path) = &self.0 {
            let _ = cleanup_trial_directory(path);
        }
    }
}

fn cleanup_trial_directory(path: &Path) -> io::Result<()> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_type()?.is_file() && entry.path().extension().is_some_and(|ext| ext == "wal")
        {
            fs::remove_file(entry.path())?;
        }
    }
    fs::remove_dir(path)
}

fn sync_directory(path: &Path) -> io::Result<()> {
    fs::File::open(path)?.sync_all()
}

fn parent_directory(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

/// Create missing directories one at a time and sync each parent entry.
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
                    Ok(()) => sync_directory(parent_directory(&current))?,
                    Err(create_error) if create_error.kind() == io::ErrorKind::AlreadyExists => {
                        if !fs::metadata(&current)?.is_dir() {
                            return Err(create_error);
                        }
                        sync_directory(parent_directory(&current))?;
                    }
                    Err(create_error) => return Err(create_error),
                }
            }
            Err(error) => return Err(error),
        }
    }

    if !fs::metadata(path)?.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::NotADirectory,
            format!("{} is not a directory", path.display()),
        ));
    }
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("wal_tokio_sequential: {error}");
        std::process::exit(2);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let config = parse_args()?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    create_dir_all_durable(&config.output_dir)?;
    let fs_type = filesystem_type(&config.output_dir)?;
    reject_volatile_filesystem(&fs_type)?;

    let payload_bytes = (config.iterations as u64).saturating_mul(128);
    let initial_available = mem_available_bytes()?;
    let initial_required = payload_bytes.saturating_add(config.min_memory_bytes);
    if initial_available < initial_required {
        return Err(format!("preflight memory capacity check failed before record generation: MemAvailable={} required={} (payload={} + configured headroom={})", initial_available, initial_required, payload_bytes, config.min_memory_bytes).into());
    }
    eprintln!(
        "preparing {} deterministic records ({} bytes)",
        config.iterations, payload_bytes
    );
    let records = generate_records(config.iterations);
    let sample_indices = make_sample_plan(config.iterations, config.latency_stride);

    let scenarios = make_scenarios(&config);
    print_csv_header();
    for (batch_size, rotation_bytes) in scenarios {
        for repetition in 1..=config.repetitions {
            let label = format!("b{batch_size}-r{rotation_bytes}-rep{repetition}");
            let preflight = preflight(&config, payload_bytes, batch_size, rotation_bytes, &label)?;
            let trial = runtime.block_on(run_trial(
                &config,
                &records,
                &sample_indices,
                batch_size,
                rotation_bytes,
                repetition,
                &preflight,
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
        let value = args
            .next()
            .ok_or_else(|| format!("missing value after {argument}"))?;
        match argument.as_str() {
            "--iterations" => config.iterations = positive(&argument, &value)?,
            "--repetitions" => config.repetitions = positive(&argument, &value)?,
            "--batch-sizes" => {
                config.batch_sizes = parse_list(&argument, &value)?;
                if config.batch_sizes.contains(&0) {
                    return Err("batch sizes must be positive".into());
                }
            }
            "--rotation-bytes" => config.rotations = Some(parse_u64_list(&argument, &value)?),
            "--output-dir" => config.output_dir = PathBuf::from(value),
            "--latency-sample-stride" => {
                config.latency_stride = positive(&argument, &value)? as u64
            }
            "--preflight-observation-ms" => {
                config.observation_ms = positive(&argument, &value)? as u64
            }
            "--preflight-timeout-ms" => config.timeout_ms = positive(&argument, &value)? as u64,
            "--preflight-max-cpu-pct" => config.max_cpu_pct = percentage(&argument, &value)?,
            "--preflight-max-disk-busy-pct" => config.max_disk_pct = percentage(&argument, &value)?,
            "--preflight-min-mem-bytes" => config.min_memory_bytes = value.parse()?,
            "--preflight-free-reserve-bytes" => config.free_reserve_bytes = Some(value.parse()?),
            _ => return Err(format!("unknown argument: {argument}").into()),
        }
    }
    if config.iterations == 0 || config.repetitions == 0 || config.latency_stride == 0 {
        return Err("iterations, repetitions, and latency sample stride must be positive".into());
    }
    if config.batch_sizes.iter().any(|batch| *batch == 0) {
        return Err("batch sizes must be positive".into());
    }
    if config
        .rotations
        .as_ref()
        .is_some_and(|values| values.iter().any(|value| *value == 0))
    {
        return Err("rotation thresholds must be positive".into());
    }
    Ok(config)
}

fn print_help() {
    println!(
        "wal_tokio_sequential options:\n  --iterations N\n  --repetitions N\n  --batch-sizes N[,N...]\n  --rotation-bytes N[,N...]\n  --output-dir PATH\n  --latency-sample-stride N\n  --preflight-observation-ms N\n  --preflight-timeout-ms N\n  --preflight-max-cpu-pct 0..100\n  --preflight-max-disk-busy-pct 0..100\n  --preflight-min-mem-bytes N\n  --preflight-free-reserve-bytes N"
    );
}

fn positive(argument: &str, value: &str) -> Result<usize, Box<dyn Error>> {
    let parsed: usize = value
        .parse()
        .map_err(|_| format!("{argument} expects a positive integer"))?;
    if parsed == 0 {
        return Err(format!("{argument} expects a positive integer").into());
    }
    Ok(parsed)
}

fn parse_list(argument: &str, value: &str) -> Result<Vec<usize>, Box<dyn Error>> {
    value
        .split(',')
        .map(|item| {
            let parsed = item
                .parse::<usize>()
                .map_err(|_| format!("{argument} expects comma-separated integers"))?;
            Ok(parsed)
        })
        .collect()
}

fn parse_u64_list(argument: &str, value: &str) -> Result<Vec<u64>, Box<dyn Error>> {
    value
        .split(',')
        .map(|item| {
            item.parse::<u64>()
                .map_err(|_| format!("{argument} expects comma-separated integers"))
                .map_err(Into::into)
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

fn make_scenarios(config: &Config) -> Vec<(usize, u64)> {
    if let Some(rotations) = &config.rotations {
        return config
            .batch_sizes
            .iter()
            .flat_map(|batch| {
                rotations
                    .iter()
                    .map(move |rotation| (*batch, *rotation as u64))
            })
            .collect();
    }
    let mut scenarios: Vec<_> = config
        .batch_sizes
        .iter()
        .map(|batch| (*batch, MAIN_ROTATION))
        .collect();
    if config.batch_sizes.contains(&4096) {
        scenarios.push((4096, EXTRA_ROTATION));
    }
    scenarios
}

fn make_sample_plan(iterations: usize, stride: u64) -> Vec<usize> {
    (0..iterations)
        .filter(|index| *index == 0 || splitmix64(*index as u64) % stride == 0)
        .collect()
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn preflight(
    config: &Config,
    payload_bytes: u64,
    batch_size: usize,
    rotation_bytes: u64,
    label: &str,
) -> Result<SystemSample, Box<dyn Error>> {
    let expected_bytes = payload_bytes
        .saturating_add((config.iterations.div_ceil(batch_size) as u64).saturating_mul(56));
    let reserve = config
        .free_reserve_bytes
        .unwrap_or_else(|| (expected_bytes / 4).max(GIB));
    let required_free = expected_bytes.saturating_add(reserve);
    let start = Instant::now();
    let mut attempt = 0;
    loop {
        attempt += 1;
        let sample = system_sample(&config.output_dir, config.observation_ms)?;
        if sample.disk_busy_pct.is_none() {
            return Err(format!(
                "preflight cannot measure target disk busy for {label}; no benchmark result emitted"
            )
            .into());
        }
        let available_total = sample.mem_available.saturating_add(sample.rss_bytes);
        let memory_ok = sample.mem_available >= config.min_memory_bytes
            && available_total >= payload_bytes.saturating_add(config.min_memory_bytes);
        let disk_ok = sample
            .disk_busy_pct
            .is_some_and(|busy| busy <= config.max_disk_pct);
        let cpu_ok = sample
            .cpu_busy_pct
            .is_some_and(|busy| busy <= config.max_cpu_pct);
        let free_ok = sample.free_bytes >= required_free;
        let ok = memory_ok && free_ok && cpu_ok && disk_ok;
        eprintln!(
            "PREFLIGHT trial={label} attempt={attempt} fs={} free_bytes={} required_free_bytes={} mem_available_bytes={} rss_bytes={} memory_threshold_bytes={} cpu_busy_pct={} cpu_limit_pct={} disk_busy_pct={} disk_limit_pct={} disk_device={} result={}",
            sample.fs_type,
            sample.free_bytes,
            required_free,
            sample.mem_available,
            sample.rss_bytes,
            config.min_memory_bytes,
            optional_float(sample.cpu_busy_pct),
            config.max_cpu_pct,
            optional_float(sample.disk_busy_pct),
            config.max_disk_pct,
            sample.disk_device.as_deref().unwrap_or("unavailable"),
            if ok { "ready" } else { "wait" }
        );
        if ok {
            return Ok(SystemSample {
                required_free_bytes: required_free,
                ..sample
            });
        }
        if start.elapsed() >= Duration::from_millis(config.timeout_ms) {
            return Err(format!("preflight timed out for {label}; no benchmark result emitted (batch_size={batch_size}, rotation_bytes={rotation_bytes})").into());
        }
        thread::sleep(Duration::from_millis(100));
    }
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
        (Some(start), Some(end)) => Some(
            (end.saturating_sub(start) as f64 / elapsed_ms.max(1.0))
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
            "WAL target filesystem {fs_type} is volatile; choose persistent storage"
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
                .and_then(|v| v.split_whitespace().next())
                .and_then(|v| v.parse::<u64>().ok())
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
                .and_then(|v| v.split_whitespace().next())
                .and_then(|v| v.parse::<u64>().ok())
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

struct TrialResult {
    csv: String,
}

async fn run_trial(
    config: &Config,
    records: &[Record],
    sample_indices: &[usize],
    batch_size: usize,
    rotation_bytes: u64,
    repetition: usize,
    preflight: &SystemSample,
) -> Result<TrialResult, Box<dyn Error>> {
    let timestamp = SystemTimeNanos::now();
    let run_dir = config.output_dir.join(format!(
        "trial-b{batch_size}-rot{rotation_bytes}-rep{repetition}-pid{}-{timestamp}",
        std::process::id()
    ));
    tokio::fs::create_dir(&run_dir).await?;
    let guard = TrialDirectory(Some(run_dir.clone()));
    tokio::fs::File::open(parent_directory(&run_dir))
        .await?
        .sync_all()
        .await?;
    let mut writer = WalWriter::new(&run_dir, rotation_bytes)?;
    let mut record_latency = Latencies::default();
    let mut batch_latency = Latencies::default();
    let mut frame_encode_latency = Latencies::default();
    let mut write_all_await_latency = Latencies::default();
    let mut flush_latency = Latencies::default();
    let mut file_sync_latency = Latencies::default();
    let mut directory_sync_latency = Latencies::default();
    let mut segment_open_latency = Latencies::default();
    let mut rotation_latency = Latencies::default();
    let mut record_sample_values = Vec::with_capacity(sample_indices.len());
    let mut next_sample = 0usize;
    let mut records_written = 0u64;
    let mut frame_bytes = 0u64;
    let mut batches = 0u64;
    let mut segments = 0u64;
    let mut file_syncs = 0u64;
    let mut directory_syncs = 0u64;
    let mut first_handoff = None;
    let mut last_success = None;
    let mut cpu_started = None;
    let heartbeat_stats = Arc::new(Mutex::new(HeartbeatStats::default()));
    let (heartbeat_start_tx, heartbeat_start_rx) = tokio::sync::oneshot::channel();
    let heartbeat_task = tokio::spawn(run_heartbeat(
        Arc::clone(&heartbeat_stats),
        heartbeat_start_rx,
    ));
    let mut heartbeat_start_tx = Some(heartbeat_start_tx);
    let io_start = proc_io();
    let disk_start_ms = preflight.disk_device.as_deref().and_then(disk_busy_ms);
    let total_batches = records.len().div_ceil(batch_size);
    for (batch_index, batch) in records.chunks(batch_size).enumerate() {
        let handoff = Instant::now();
        if first_handoff.is_none() {
            first_handoff = Some(handoff);
            cpu_started = Some(ProcessTime::now());
            heartbeat_stats
                .lock()
                .expect("heartbeat stats mutex poisoned")
                .start = Some(handoff);
            let _ = heartbeat_start_tx
                .take()
                .expect("heartbeat start sender is present")
                .send(handoff);
        }
        let metrics = match writer.append_batch(batch).await {
            Ok(metrics) => metrics,
            Err(error) => {
                heartbeat_stats
                    .lock()
                    .expect("heartbeat stats mutex poisoned")
                    .end = Some(Instant::now());
                let _ = heartbeat_task.await;
                return Err(error.into());
            }
        };
        let durable_at = Instant::now();
        let total_ns = durable_at
            .duration_since(handoff)
            .as_nanos()
            .min(u128::from(u64::MAX)) as u64;
        batch_latency.add(total_ns);
        frame_encode_latency.add(metrics.frame_encode_ns);
        write_all_await_latency.add(metrics.write_all_await_ns);
        flush_latency.add(metrics.flush_ns);
        file_sync_latency.add(metrics.file_sync_ns);
        if metrics.created_segment {
            segments += 1;
            directory_syncs += 1;
            directory_sync_latency.add(metrics.directory_sync_ns);
            segment_open_latency.add(metrics.segment_open_ns);
        }
        if metrics.rotated {
            rotation_latency.add(metrics.rotation_ns);
        }
        file_syncs += 1;
        frame_bytes += metrics.frame_bytes;
        records_written += batch.len() as u64;
        batches += 1;

        let batch_end = (batch_index * batch_size + batch.len()).min(records.len());
        while next_sample < sample_indices.len() && sample_indices[next_sample] < batch_end {
            if sample_indices[next_sample] >= batch_index * batch_size {
                record_sample_values.push(total_ns);
            }
            next_sample += 1;
        }
        record_latency.count += batch.len() as u64;
        record_latency.total_ns += u128::from(total_ns) * batch.len() as u128;
        last_success = Some(durable_at);
        if batch_index + 1 == total_batches {
            heartbeat_stats
                .lock()
                .expect("heartbeat stats mutex poisoned")
                .end = Some(durable_at);
        }
    }
    let first = first_handoff.ok_or("no batches were written")?;
    let last = last_success.ok_or("no durable batch was acknowledged")?;
    let wall = last.duration_since(first);
    let cpu_started = cpu_started.ok_or("CPU timer was not started")?;
    let cpu_seconds = cpu_started.elapsed().as_secs_f64();
    let cpu_wall_seconds = first.elapsed().as_secs_f64();
    let io_end = proc_io();
    let disk_end_ms = preflight.disk_device.as_deref().and_then(disk_busy_ms);
    heartbeat_task.await?;
    let heartbeat_stats = heartbeat_stats
        .lock()
        .expect("heartbeat stats mutex poisoned")
        .clone();

    let recovery_started = Instant::now();
    let recovered = recover_and_verify(&run_dir, records)?;
    let recovery_seconds = recovery_started.elapsed().as_secs_f64();
    if recovered.records != config.iterations as u64 || recovered.truncated_tail_bytes != 0 {
        return Err(format!(
            "verification mismatch: recovered={} expected={} truncated_tail_bytes={}",
            recovered.records, config.iterations, recovered.truncated_tail_bytes
        )
        .into());
    }
    if records_written != config.iterations as u64 || file_syncs != batches {
        return Err(format!("writer accounting mismatch: records={records_written}, file_syncs={file_syncs}, batches={batches}").into());
    }
    drop(writer);
    guard.cleanup()?;

    let heartbeat_tick_count = heartbeat_stats.lateness_ns.len() as u64;
    let heartbeat_missed_tick_count = heartbeat_stats.missed_tick_count;
    let mut heartbeat_lateness_values = heartbeat_stats.lateness_ns;
    let mut fields = vec![
        config.iterations.to_string(),
        batch_size.to_string(),
        repetition.to_string(),
        rotation_bytes.to_string(),
        records_written.to_string(),
        batches.to_string(),
        segments.to_string(),
        frame_bytes.to_string(),
        file_syncs.to_string(),
        directory_syncs.to_string(),
        format!("{:.6}", records_written as f64 / wall.as_secs_f64()),
        format!("{:.9}", wall.as_secs_f64()),
        format!("{cpu_seconds:.9}"),
        format!("{cpu_wall_seconds:.9}"),
        format!("{:.6}", cpu_seconds / cpu_wall_seconds),
        heartbeat_tick_count.to_string(),
        heartbeat_missed_tick_count.to_string(),
        heartbeat_lateness_values.len().to_string(),
        quantile(&mut heartbeat_lateness_values, 50).map_or(String::new(), |x| x.to_string()),
        quantile(&mut heartbeat_lateness_values, 95).map_or(String::new(), |x| x.to_string()),
        quantile(&mut heartbeat_lateness_values, 99).map_or(String::new(), |x| x.to_string()),
        quantile(&mut heartbeat_lateness_values, 100).map_or(String::new(), |x| x.to_string()),
        format!("{:.6}", record_latency.mean()),
        record_latency.count.to_string(),
        record_latency.total_ns.to_string(),
        record_sample_values.len().to_string(),
        quantile(&mut record_sample_values, 50).map_or(String::new(), |x| x.to_string()),
        quantile(&mut record_sample_values, 95).map_or(String::new(), |x| x.to_string()),
        quantile(&mut record_sample_values, 99).map_or(String::new(), |x| x.to_string()),
        format!("{recovery_seconds:.9}"),
        recovered.bytes.to_string(),
        recovered.truncated_tail_bytes.to_string(),
        csv_quote(&preflight.fs_type),
        preflight.free_bytes.to_string(),
        preflight.required_free_bytes.to_string(),
        preflight.mem_available.to_string(),
        preflight.rss_bytes.to_string(),
        optional_float(preflight.cpu_busy_pct),
        optional_float(preflight.disk_busy_pct),
        csv_quote(preflight.disk_device.as_deref().unwrap_or("")),
        config.max_cpu_pct.to_string(),
        config.max_disk_pct.to_string(),
        config.min_memory_bytes.to_string(),
        config.observation_ms.to_string(),
        config.timeout_ms.to_string(),
        optional_u64(preflight.disk_busy_ms),
        optional_u64(disk_start_ms),
        optional_u64(disk_end_ms),
    ];
    append_latency_columns(&mut fields, &mut batch_latency);
    append_latency_columns(&mut fields, &mut frame_encode_latency);
    append_latency_columns(&mut fields, &mut write_all_await_latency);
    append_latency_columns(&mut fields, &mut flush_latency);
    append_latency_columns(&mut fields, &mut file_sync_latency);
    append_latency_columns(&mut fields, &mut directory_sync_latency);
    append_latency_columns(&mut fields, &mut segment_open_latency);
    append_latency_columns(&mut fields, &mut rotation_latency);
    append_io_columns(&mut fields, io_start.as_ref(), io_end.as_ref());
    Ok(TrialResult {
        csv: fields.join(","),
    })
}

fn append_latency_columns(fields: &mut Vec<String>, latencies: &mut Latencies) {
    fields.extend([
        latencies.count.to_string(),
        latencies.total_ns.to_string(),
        format!("{:.6}", latencies.mean()),
        latencies.values.len().to_string(),
        latencies
            .quantile(50)
            .map_or(String::new(), |value| value.to_string()),
        latencies
            .quantile(95)
            .map_or(String::new(), |value| value.to_string()),
        latencies
            .quantile(99)
            .map_or(String::new(), |value| value.to_string()),
    ]);
}

fn append_io_columns(fields: &mut Vec<String>, start: Option<&ProcIo>, end: Option<&ProcIo>) {
    for (a, b) in [
        (start.map(|x| x.rchar), end.map(|x| x.rchar)),
        (start.map(|x| x.wchar), end.map(|x| x.wchar)),
        (start.map(|x| x.syscr), end.map(|x| x.syscr)),
        (start.map(|x| x.syscw), end.map(|x| x.syscw)),
        (start.map(|x| x.read_bytes), end.map(|x| x.read_bytes)),
        (start.map(|x| x.write_bytes), end.map(|x| x.write_bytes)),
        (
            start.map(|x| x.cancelled_write_bytes),
            end.map(|x| x.cancelled_write_bytes),
        ),
    ] {
        fields.push(optional_u64(a));
        fields.push(optional_u64(b));
    }
}

fn print_csv_header() {
    let mut fields: Vec<String> = "iterations,batch_size,repetition,rotation_bytes,records,batches,segments,actual_frame_bytes,file_syncs,directory_syncs,durable_rps,wall_seconds,process_cpu_seconds,cpu_measurement_wall_seconds,cpu_core_equivalents,heartbeat_tick_count,heartbeat_missed_tick_count,heartbeat_lateness_sample_count,heartbeat_lateness_p50_ns,heartbeat_lateness_p95_ns,heartbeat_lateness_p99_ns,heartbeat_lateness_max_ns,record_latency_mean_ns,record_latency_count,record_latency_total_ns,record_latency_sample_count,record_latency_p50_ns,record_latency_p95_ns,record_latency_p99_ns,recovery_seconds,recovered_frame_bytes,truncated_tail_bytes,preflight_fs_type,preflight_free_bytes,preflight_required_free_bytes,preflight_mem_available_bytes,preflight_process_rss_bytes,preflight_cpu_busy_pct,preflight_disk_busy_pct,preflight_disk_device,preflight_cpu_limit_pct,preflight_disk_limit_pct,preflight_min_mem_bytes,preflight_observation_ms,preflight_timeout_ms,preflight_disk_busy_ms,measurement_disk_busy_ms_start,measurement_disk_busy_ms_end".split(',').map(str::to_owned).collect();
    for name in [
        "batch_latency",
        "frame_encode",
        "write_all_await",
        "flush",
        "file_sync",
        "directory_sync",
        "segment_open",
        "rotation",
    ] {
        fields.extend([
            format!("{name}_count"),
            format!("{name}_total_ns"),
            format!("{name}_mean_ns"),
            format!("{name}_sample_count"),
            format!("{name}_p50_ns"),
            format!("{name}_p95_ns"),
            format!("{name}_p99_ns"),
        ]);
    }
    for field in [
        "rchar",
        "wchar",
        "syscr",
        "syscw",
        "read_bytes",
        "write_bytes",
        "cancelled_write_bytes",
    ] {
        fields.push(format!("proc_io_{field}_start"));
        fields.push(format!("proc_io_{field}_end"));
    }
    println!("{}", fields.join(","));
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
    value.map_or_else(String::new, |n| format!("{n:.3}"))
}
fn optional_u64(value: Option<u64>) -> String {
    value.map_or_else(String::new, |n| n.to_string())
}
fn csv_quote(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

struct SystemTimeNanos;
impl SystemTimeNanos {
    fn now() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    }
}
