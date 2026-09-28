use cpu_time::ProcessTime;
use std::collections::BTreeMap;
use std::error::Error;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener};
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncBufReadExt;
use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tokio::time::sleep_until;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Request, Response, Status};

mod proto {
    include!("support/grpc_echo_loopback.rs");
}
use proto::echo_bench_client::EchoBenchClient;
use proto::echo_bench_server::{EchoBench, EchoBenchServer};
use proto::{EchoRequest, EchoResponse};

const PAYLOADS: &[usize] = &[128, 4096];
const CONCURRENCIES: &[usize] = &[1, 2, 4, 8, 16, 32, 64, 128, 256];
const DEFAULT_OUT: &str = "target/grpc-echo-loopback";
const FIXED_RPS: u64 = 100;
const FIXED_IN_FLIGHT: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
    Fixed,
    Closed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PerfMode {
    Off,
    Stat,
    Record,
}
impl PerfMode {
    fn name(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Stat => "stat",
            Self::Record => "record",
        }
    }
}
impl Mode {
    fn name(self) -> &'static str {
        match self {
            Self::Fixed => "fixed_rate",
            Self::Closed => "closed_loop",
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Layout {
    client: usize,
    server: usize,
}
impl Layout {
    fn parse(value: &str) -> Result<Self, String> {
        let (client, server) = value
            .split_once('/')
            .ok_or("layout must be 3/3, 2/4, or 4/2")?;
        let parsed = Self {
            client: client.parse().map_err(|_| "invalid layout")?,
            server: server.parse().map_err(|_| "invalid layout")?,
        };
        if ![(3, 3), (2, 4), (4, 2)].contains(&(parsed.client, parsed.server)) {
            return Err("layout must be 3/3, 2/4, or 4/2".into());
        }
        Ok(parsed)
    }
    fn name(self) -> String {
        format!("{}/{}", self.client, self.server)
    }
    fn cpus(self) -> (Vec<usize>, Vec<usize>) {
        let client = (0..self.client).chain(8..8 + self.client).collect();
        let server = (self.client..self.client + self.server)
            .chain(8 + self.client..8 + self.client + self.server)
            .collect();
        (client, server)
    }
}

#[derive(Clone, Debug)]
struct Config {
    payloads: Vec<usize>,
    concurrencies: Vec<usize>,
    channels: Vec<usize>,
    modes: Vec<Mode>,
    repetitions: usize,
    warmup_ms: u64,
    seconds_ms: u64,
    rate: u64,
    layout: Layout,
    out: PathBuf,
    preflight_ms: u64,
    timeout_ms: u64,
    max_cpu: f64,
    max_disk: f64,
    min_mem: u64,
    max_loopback_bytes: u64,
    max_loopback_packets: u64,
    kernel_sample_ms: u64,
    perf_mode: PerfMode,
    perf_bin: PathBuf,
}

#[derive(Clone, Copy, Debug, Default)]
struct Stats {
    count: u64,
    total_ns: u128,
    mean_ns: f64,
    p50: u64,
    p95: u64,
    p99: u64,
    p999: u64,
}
#[derive(Default)]
struct Latencies {
    e2e: Vec<u64>,
    t0_t1: Vec<u64>,
    t1_t2: Vec<u64>,
    t2_t3: Vec<u64>,
}
#[derive(Clone, Copy)]
struct Timing {
    e2e: u64,
    t0_t1: u64,
    t1_t2: u64,
    t2_t3: u64,
}
impl Latencies {
    fn push(&mut self, t: Timing) {
        self.e2e.push(t.e2e);
        self.t0_t1.push(t.t0_t1);
        self.t1_t2.push(t.t1_t2);
        self.t2_t3.push(t.t2_t3);
    }
    fn finish(&mut self) -> [Stats; 4] {
        [
            summarize(&mut self.e2e),
            summarize(&mut self.t0_t1),
            summarize(&mut self.t1_t2),
            summarize(&mut self.t2_t3),
        ]
    }
}
fn summarize(values: &mut Vec<u64>) -> Stats {
    if values.is_empty() {
        return Stats::default();
    }
    values.sort_unstable();
    let total_ns: u128 = values.iter().map(|n| u128::from(*n)).sum();
    Stats {
        count: values.len() as u64,
        total_ns,
        mean_ns: total_ns as f64 / values.len() as f64,
        p50: quantile(values, 500),
        p95: quantile(values, 950),
        p99: quantile(values, 990),
        p999: quantile(values, 999),
    }
}
fn quantile(values: &[u64], percentile_milli: u64) -> u64 {
    if values.is_empty() {
        0
    } else {
        let rank = (u128::from(percentile_milli) * values.len() as u128).div_ceil(1000);
        values[rank.saturating_sub(1) as usize]
    }
}

#[derive(Default)]
struct Heartbeat {
    values: Vec<u64>,
    missed: u64,
}
#[derive(Clone, Debug, Default)]
struct IoCounters {
    rchar: u64,
    wchar: u64,
    syscr: u64,
    syscw: u64,
    read_bytes: u64,
    write_bytes: u64,
    cancelled_write_bytes: u64,
}
#[derive(Clone, Debug, Default)]
struct ProcSample {
    cpu_ticks: u64,
    rss_bytes: u64,
    io: IoCounters,
}
#[derive(Clone, Debug, Default)]
struct TcpSocketSample {
    role: String,
    flow_id: String,
    local_endpoint: String,
    peer_endpoint: String,
    recv_q: u64,
    send_q: u64,
    skmem_raw: String,
    tcp_info_raw: String,
    info: BTreeMap<String, String>,
}
#[derive(Clone, Debug, Default)]
struct TcpKernelSnapshot {
    kind: &'static str,
    start_mono_ns: u64,
    end_mono_ns: u64,
    sockets: Vec<TcpSocketSample>,
}
struct PerfChild {
    role: &'static str,
    child: Child,
    output: PathBuf,
    stderr: PathBuf,
    start_mono_ns: u64,
    signal_mono_ns: Option<u64>,
}
struct PerfMonitors {
    mode: PerfMode,
    children: Vec<PerfChild>,
}
impl Drop for PerfMonitors {
    fn drop(&mut self) {
        for monitor in &mut self.children {
            if monitor.child.try_wait().ok().flatten().is_none() {
                if monitor.signal_mono_ns.is_none() {
                    unsafe {
                        libc::kill(monitor.child.id() as i32, libc::SIGINT);
                    }
                }
                let _ = monitor.child.wait();
            }
        }
    }
}
#[derive(Clone, Debug, Default)]
struct NetSample {
    rx_bytes: u64,
    rx_packets: u64,
    tx_bytes: u64,
    tx_packets: u64,
}
#[derive(Clone, Debug, Default)]
struct CpuSample {
    cpus: BTreeMap<String, [u64; 8]>,
}
#[derive(Clone, Debug, Default)]
struct Preflight {
    cpu: f64,
    disk: f64,
    mem: u64,
    loop_bytes: u64,
    loop_packets: u64,
    elapsed_ms: u64,
}
#[derive(Default)]
struct SystemMetrics {
    client_cpu_ns: u64,
    server_cpu_ns: u64,
    server_cpu_window_ns: u64,
    server_cpu_start_offset_ns: i64,
    server_cpu_end_offset_ns: i64,
    client_rss_peak: u64,
    server_rss_peak: u64,
    client_io_start: IoCounters,
    client_io_end: IoCounters,
    server_io_start: IoCounters,
    server_io_end: IoCounters,
    net_rx_bytes: u64,
    net_rx_packets: u64,
    net_tx_bytes: u64,
    net_tx_packets: u64,
    tcp_send_q: u64,
    tcp_recv_q: u64,
    per_core: String,
    disk_start: u64,
    disk_end: u64,
    disk_device: String,
}
struct Summary {
    mode: Mode,
    payload: usize,
    concurrency: usize,
    channels: usize,
    issued: u64,
    completed: u64,
    skipped: u64,
    issue_ns: u64,
    completion_ns: u64,
    drain_ns: u64,
    client_cpu_ns: u64,
    latency: [Stats; 4],
    heartbeat: Stats,
    heartbeat_max: u64,
    heartbeat_missed: u64,
    system: SystemMetrics,
}
struct Reader {
    rx: mpsc::Receiver<String>,
    _join: thread::JoinHandle<()>,
}
struct Children {
    server: Option<Child>,
    client: Option<Child>,
    server_in: Option<ChildStdin>,
    client_in: Option<ChildStdin>,
}
impl Drop for Children {
    fn drop(&mut self) {
        for child in [&mut self.client, &mut self.server].into_iter().flatten() {
            if child.try_wait().ok().flatten().is_none() {
                let _ = child.kill();
            }
            let _ = child.wait();
        }
    }
}

fn main() {
    if let Err(error) = entry() {
        eprintln!("grpc_echo_loopback: {error}");
        std::process::exit(2);
    }
}
fn entry() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args()
        .skip(1)
        .filter(|arg| arg != "--bench")
        .collect();
    if let Some(worker) = args.iter().find_map(|s| s.strip_prefix("--grpc-worker=")) {
        return run_worker(worker, &args);
    }
    if args.iter().any(|a| a == "--help" || a == "-h") {
        help();
        return Ok(());
    }
    let config = parse_args(&args)?;
    validate_host(&config)?;
    run_parent(config)
}
fn parse_args(args: &[String]) -> Result<Config, Box<dyn Error>> {
    let mut c = Config {
        payloads: PAYLOADS.to_vec(),
        concurrencies: CONCURRENCIES.to_vec(),
        channels: vec![1],
        modes: vec![Mode::Fixed, Mode::Closed],
        repetitions: 3,
        warmup_ms: 5000,
        seconds_ms: 20000,
        rate: FIXED_RPS,
        layout: Layout {
            client: 3,
            server: 3,
        },
        out: DEFAULT_OUT.into(),
        preflight_ms: 3000,
        timeout_ms: 60000,
        max_cpu: 10.0,
        max_disk: 5.0,
        min_mem: 512 * 1024 * 1024,
        max_loopback_bytes: 1024 * 1024,
        max_loopback_packets: 4096,
        kernel_sample_ms: 0,
        perf_mode: PerfMode::Off,
        perf_bin: PathBuf::from("perf"),
    };
    let mut seen = BTreeMap::new();
    let mut i = 0;
    while i < args.len() {
        let token = &args[i];
        let (name, in_line) = token
            .split_once('=')
            .map_or((token.as_str(), None), |(a, b)| (a, Some(b)));
        if !name.starts_with("--") {
            return Err(format!("unexpected argument {token:?}").into());
        }
        if seen.insert(name, ()).is_some() {
            return Err(format!("duplicate option {name}").into());
        }
        let value = if let Some(v) = in_line {
            v.to_owned()
        } else {
            i += 1;
            args.get(i)
                .filter(|v| !v.starts_with("--"))
                .ok_or_else(|| format!("{name} needs a value"))?
                .clone()
        };
        match name {
            "--payload-sizes" => c.payloads = parse_list(&value, name)?,
            "--concurrencies" => c.concurrencies = parse_list(&value, name)?,
            "--channels" => c.channels = parse_list(&value, name)?,
            "--modes" => {
                c.modes = value
                    .split(',')
                    .map(|s| match s {
                        "fixed" | "fixed_rate" => Ok(Mode::Fixed),
                        "closed" | "closed_loop" => Ok(Mode::Closed),
                        _ => Err(format!("unknown mode {s:?}")),
                    })
                    .collect::<Result<_, _>>()?
            }
            "--repetitions" => c.repetitions = positive(&value, name)?,
            "--warmup-seconds" => c.warmup_ms = seconds_ms(&value, name)?,
            "--seconds" => c.seconds_ms = seconds_ms(&value, name)?,
            "--rate" => c.rate = positive(&value, name)?,
            "--cpu-layout" => c.layout = Layout::parse(&value)?,
            "--output-dir" => c.out = PathBuf::from(value),
            "--preflight-observation-ms" => c.preflight_ms = positive(&value, name)?,
            "--preflight-timeout-ms" => c.timeout_ms = positive(&value, name)?,
            "--preflight-max-cpu-pct" => c.max_cpu = float_nonnegative(&value, name)?,
            "--preflight-max-disk-busy-pct" => c.max_disk = float_nonnegative(&value, name)?,
            "--preflight-min-mem-bytes" => c.min_mem = positive(&value, name)?,
            "--preflight-max-loopback-bytes" => c.max_loopback_bytes = positive(&value, name)?,
            "--preflight-max-loopback-packets" => c.max_loopback_packets = positive(&value, name)?,
            "--kernel-sample-ms" => {
                c.kernel_sample_ms = value.parse().map_err(|_| "invalid --kernel-sample-ms")?;
                if c.kernel_sample_ms > 60_000 {
                    return Err("--kernel-sample-ms cannot exceed 60000".into());
                }
            }
            "--perf-mode" => {
                c.perf_mode = match value.as_str() {
                    "off" => PerfMode::Off,
                    "stat" => PerfMode::Stat,
                    "record" => PerfMode::Record,
                    _ => return Err("--perf-mode must be off, stat, or record".into()),
                }
            }
            "--perf-bin" => c.perf_bin = PathBuf::from(value),
            _ => return Err(format!("unknown option {name:?}; use --help").into()),
        }
        i += 1;
    }
    if c.payloads.is_empty()
        || c.concurrencies.is_empty()
        || c.channels.is_empty()
        || c.modes.is_empty()
        || c.repetitions == 0
        || c.seconds_ms == 0
    {
        return Err(
            "selected values, repetitions, and measurement seconds must be positive".into(),
        );
    }
    if c.payloads.iter().any(|x| *x > 1024 * 1024) || c.concurrencies.iter().any(|x| *x > 4096) {
        return Err("payload limit is 1 MiB; concurrency limit is 4096".into());
    }
    if c.channels.iter().any(|x| ![1, 2, 4, 8].contains(x)) {
        return Err("channels must be 1, 2, 4, or 8".into());
    }
    if c.max_cpu > 100.0 || c.max_disk > 100.0 {
        return Err("CPU/disk thresholds cannot exceed 100".into());
    }
    Ok(c)
}
fn parse_list(s: &str, label: &str) -> Result<Vec<usize>, String> {
    let mut out = Vec::new();
    for item in s.split(',') {
        let n = positive(item, label)?;
        if out.contains(&n) {
            return Err(format!("duplicate {label} value {n}"));
        }
        out.push(n);
    }
    Ok(out)
}
fn positive<T: std::str::FromStr + PartialOrd + Default>(
    s: &str,
    label: &str,
) -> Result<T, String> {
    let n = s
        .parse::<T>()
        .map_err(|_| format!("invalid {label}: {s:?}"))?;
    if n <= T::default() {
        Err(format!("{label} must be positive"))
    } else {
        Ok(n)
    }
}
fn seconds_ms(s: &str, label: &str) -> Result<u64, String> {
    let n = s.parse::<f64>().map_err(|_| format!("invalid {label}"))?;
    if !n.is_finite() || n < 0.0 || n > 86400.0 {
        Err(format!("{label} must be between 0 and 86400 seconds"))
    } else {
        Ok((n * 1000.0).round() as u64)
    }
}
fn float_nonnegative(s: &str, label: &str) -> Result<f64, String> {
    let n = s.parse::<f64>().map_err(|_| format!("invalid {label}"))?;
    if !n.is_finite() || n < 0.0 {
        Err(format!("{label} must be finite and non-negative"))
    } else {
        Ok(n)
    }
}
fn help() {
    println!(
        "Tokio same-host gRPC unary echo benchmark\n\
Usage: cargo bench --bench grpc_echo_loopback -- [options]\n\
Defaults: payloads 128,4096; fixed-rate 100 RPS and closed-loop C=1,2,4,8,16,32,64,128,256; one channel; 5s warmup; 20s measured; 3 reps; CPU layout 3/3.\n\
--payload-sizes N[,N...] --concurrencies N[,N...] --channels 1[,2,4,8]\n\
--modes fixed,closed --rate RPS --warmup-seconds S --seconds S --repetitions N\n\
--cpu-layout 3/3|2/4|4/2 --output-dir PATH\n\
--preflight-observation-ms N --preflight-timeout-ms N --preflight-max-cpu-pct P\n\
--preflight-max-disk-busy-pct P --preflight-min-mem-bytes N\n\
--preflight-max-loopback-bytes N --preflight-max-loopback-packets N\n\
--kernel-sample-ms N (0 disables; default 0) --perf-mode off|stat|record --perf-bin PATH\n\
Multi-channel and alternate CPU layouts are explicit follow-up settings; output uses a unique run directory."
    );
}

fn validate_host(c: &Config) -> Result<(), Box<dyn Error>> {
    for core in 0..8 {
        let path = format!("/sys/devices/system/cpu/cpu{core}/topology/thread_siblings_list");
        let sibling = fs::read_to_string(&path).map_err(|e| format!("cannot read {path}: {e}"))?;
        if parse_cpu_list(sibling.trim())? != vec![core, core + 8] {
            return Err(format!(
                "unexpected SMT topology at core {core}; expected siblings {core},{}",
                core + 8
            )
            .into());
        }
    }
    let (client, server) = c.layout.cpus();
    for (role, cpus) in [("client", client), ("server", server)] {
        let list = format_cpus(&cpus);
        if !Command::new("taskset")
            .args(["-c", &list, "true"])
            .status()?
            .success()
        {
            return Err(format!("taskset cannot pin {role} to {list}").into());
        }
    }
    if Command::new("ss").arg("--version").output().is_err() {
        return Err("ss is required for TCP socket diagnostics".into());
    }
    Ok(())
}

fn run_parent(c: Config) -> Result<(), Box<dyn Error>> {
    fs::create_dir_all(&c.out)?;
    let run_dir = make_run_dir(&c.out)?;
    let mut csv = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(run_dir.join("results.csv"))?;
    write_csv_header(&mut csv)?;
    let mut preflight = OpenOptions::new()
        .create_new(true)
        .append(true)
        .open(run_dir.join("preflight.log"))?;
    write_run_metadata(&run_dir, &c)?;
    let mut perf_preflight = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(run_dir.join("perf_preflight.log"))?;
    perf_preflight_check(c.perf_mode, &c.perf_bin, &mut perf_preflight)?;
    println!("run_dir={}", run_dir.display());
    let mut trial = 0_u64;
    for &payload in &c.payloads {
        for &channels in &c.channels {
            for &mode in &c.modes {
                let concurrencies = if mode == Mode::Fixed {
                    vec![1]
                } else {
                    c.concurrencies.clone()
                };
                for &concurrency in &concurrencies {
                    for repetition in 1..=c.repetitions {
                        trial += 1;
                        let port = reserve_port()?;
                        let label = format!(
                            "trial-{trial:04}-{}-b{payload}-c{concurrency}-ch{channels}-rep{repetition}",
                            mode.name()
                        );
                        let pre = idle_preflight(&c, &mut preflight, &label, port)?;
                        let mut summary = match run_trial(
                            &c,
                            &run_dir,
                            &label,
                            port,
                            payload,
                            concurrency,
                            channels,
                            mode,
                        ) {
                            Ok(summary) => summary,
                            Err(error) => {
                                let diagnostic = run_dir.join(format!("{label}.diagnostic.log"));
                                if let Ok(mut log) =
                                    OpenOptions::new().append(true).open(diagnostic)
                                {
                                    let _ = writeln!(log, "trial_aborted={error}");
                                }
                                return Err(error);
                            }
                        };
                        let row = csv_row(&c, &summary, repetition, &pre);
                        write_csv(&mut csv, &row)?;
                        csv.flush()?;
                        println!(
                            "completed {label}: {} RPCs, {:.1} RPS, client {:.3} CPU cores, server {:.3} CPU cores",
                            summary.completed,
                            summary.completed as f64 / summary.completion_ns.max(1) as f64 * 1e9,
                            summary.system.client_cpu_ns as f64
                                / summary.completion_ns.max(1) as f64,
                            summary.system.server_cpu_ns as f64
                                / summary.system.server_cpu_window_ns.max(1) as f64
                        );
                        summary.system.per_core.clear();
                    }
                }
            }
        }
    }
    println!("results={}", run_dir.join("results.csv").display());
    println!("preflight_log={}", run_dir.join("preflight.log").display());
    Ok(())
}

const PERF_EVENTS: &str =
    "task-clock,cycles:u,cycles:k,instructions:u,instructions:k,context-switches,cpu-migrations";

fn perf_preflight_check(
    mode: PerfMode,
    perf_bin: &Path,
    log: &mut File,
) -> Result<(), Box<dyn Error>> {
    writeln!(log, "perf_mode={}", mode.name())?;
    writeln!(log, "perf_binary={}", perf_bin.display())?;
    writeln!(
        log,
        "perf_event_paranoid={}",
        fs::read_to_string("/proc/sys/kernel/perf_event_paranoid")
            .unwrap_or_else(|_| "unreadable".into())
            .trim()
    )?;
    writeln!(
        log,
        "unprivileged_bpf_disabled={}",
        fs::read_to_string("/proc/sys/kernel/unprivileged_bpf_disabled")
            .unwrap_or_else(|_| "unreadable".into())
            .trim()
    )?;
    writeln!(
        log,
        "cap_eff={}",
        process_cap_eff().unwrap_or_else(|| "unreadable".into())
    )?;
    if mode == PerfMode::Off {
        writeln!(log, "preflight=skipped (perf mode off)")?;
        log.flush()?;
        return Ok(());
    }
    let version = Command::new(perf_bin).arg("--version").output()?;
    writeln!(log, "version_command={:?} --version", perf_bin)?;
    writeln!(log, "version_status={}", version.status)?;
    writeln!(
        log,
        "version_output={}",
        String::from_utf8_lossy(&version.stdout).trim()
    )?;
    if !version.status.success() {
        return Err(format!(
            "perf binary {} is not executable; see perf_preflight.log",
            perf_bin.display()
        )
        .into());
    }
    let pid = std::process::id().to_string();
    let command_text = format!("{} stat -e {PERF_EVENTS} -p {pid}", perf_bin.display());
    writeln!(log, "permission_probe_command={command_text}")?;
    let stderr_path = std::env::temp_dir().join(format!(
        "grpc-echo-perf-preflight-{}.stderr",
        std::process::id()
    ));
    let stderr_file = File::create(&stderr_path)?;
    let mut child = match Command::new(perf_bin)
        .args(["stat", "-e", PERF_EVENTS, "-p", &pid])
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr_file))
        .spawn()
    {
        Ok(child) => child,
        Err(error) => {
            writeln!(log, "preflight_result=failed_to_spawn error={error}")?;
            log.flush()?;
            return Err(format!(
                "could not start perf permission probe: {error}; see perf_preflight.log"
            )
            .into());
        }
    };
    thread::sleep(Duration::from_millis(150));
    if child.try_wait()?.is_none() {
        unsafe {
            libc::kill(child.id() as i32, libc::SIGINT);
        }
    }
    let status = child.wait()?;
    let stderr = fs::read_to_string(&stderr_path)
        .unwrap_or_else(|error| format!("could not read probe stderr: {error}"));
    let _ = fs::remove_file(&stderr_path);
    writeln!(log, "probe_status={status}")?;
    writeln!(log, "probe_output={}", stderr.trim())?;
    let mut event_values_valid = true;
    for event in PERF_EVENTS.split(',') {
        let value = stderr.lines().find_map(|line| {
            let fields: Vec<_> = line.split_whitespace().collect();
            if fields.get(1).copied() == Some(event) {
                fields
                    .first()
                    .and_then(|raw| raw.replace(',', "").parse::<f64>().ok())
            } else {
                None
            }
        });
        let valid = value.is_some()
            && !stderr.contains("<not supported>")
            && !stderr.contains("<not counted>");
        event_values_valid &= valid;
        writeln!(log, "probe_event={event} value={value:?} valid={valid}")?;
    }
    let probe_output_lc = stderr.to_ascii_lowercase();
    let has_permission_denial = probe_output_lc.contains("permission denied")
        || probe_output_lc.contains("access denied")
        || probe_output_lc.contains("access to performance monitoring");
    let expected_sigint = status.signal() == Some(libc::SIGINT)
        && stderr.contains("Performance counter stats for process")
        && event_values_valid
        && !has_permission_denial;
    let probe_ok = status.success() && event_values_valid || expected_sigint;
    if !probe_ok {
        writeln!(
            log,
            "preflight_result=failed; no perf data will be silently substituted"
        )?;
        log.flush()?;
        return Err(format!(
            "perf permission preflight failed for --perf-mode {}; see perf_preflight.log",
            mode.name()
        )
        .into());
    }
    if expected_sigint {
        writeln!(
            log,
            "probe_status_interpretation=expected SIGINT after valid counters were collected"
        )?;
    }
    writeln!(log, "preflight_result=passed")?;
    log.flush()?;
    Ok(())
}

fn process_cap_eff() -> Option<String> {
    fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| {
            line.strip_prefix("CapEff:")
                .map(str::trim)
                .map(str::to_owned)
        })
}

fn start_perf_monitors(
    c: &Config,
    dir: &Path,
    label: &str,
    client_pid: u32,
    server_pid: u32,
    mode: Mode,
    concurrency: usize,
    diag: &mut File,
) -> Result<PerfMonitors, Box<dyn Error>> {
    let mut monitors = PerfMonitors {
        mode: c.perf_mode,
        children: Vec::new(),
    };
    if c.perf_mode == PerfMode::Off {
        return Ok(monitors);
    }
    if c.perf_mode == PerfMode::Record && !(mode == Mode::Closed && concurrency == 256) {
        writeln!(
            diag,
            "perf_record_skipped=record mode profiles only closed-loop C=256 trials"
        )?;
        return Ok(monitors);
    }
    for (role, pid) in [("client", client_pid), ("server", server_pid)] {
        let (output, stderr) = match c.perf_mode {
            PerfMode::Off => unreachable!(),
            PerfMode::Stat => (
                dir.join(format!("{label}.{role}.perf-stat.txt")),
                dir.join(format!("{label}.{role}.perf-stat.stderr.log")),
            ),
            PerfMode::Record => (
                dir.join(format!("{label}.{role}.perf.data")),
                dir.join(format!("{label}.{role}.perf-record.stderr.log")),
            ),
        };
        let stderr_file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&stderr)?;
        let pid_text = pid.to_string();
        let mut command = Command::new(&c.perf_bin);
        match c.perf_mode {
            PerfMode::Off => unreachable!(),
            PerfMode::Stat => {
                command.args(["stat", "-v", "-o"]).arg(&output).args([
                    "-e",
                    PERF_EVENTS,
                    "-p",
                    &pid_text,
                ]);
            }
            PerfMode::Record => {
                command
                    .args([
                        "record",
                        "-F",
                        "49",
                        "--call-graph",
                        "dwarf,4096",
                        "-p",
                        &pid_text,
                        "-o",
                    ])
                    .arg(&output);
            }
        }
        let command_desc = format!(
            "{} {:?}",
            c.perf_bin.display(),
            command.get_args().collect::<Vec<_>>()
        );
        writeln!(diag, "perf_{}_command={command_desc}", role)?;
        if c.perf_mode == PerfMode::Stat {
            writeln!(diag, "perf_{}_verbose_event_timing=true", role)?;
        }
        writeln!(
            diag,
            "perf_{}_attach_point=PHASE_START event received by parent",
            role
        )?;
        let child = command
            .stdout(Stdio::null())
            .stderr(Stdio::from(stderr_file))
            .spawn()
            .map_err(|error| format!("failed to start perf for {role}: {error}"))?;
        let start_mono_ns = mono_ns();
        monitors.children.push(PerfChild {
            role,
            child,
            output,
            stderr,
            start_mono_ns,
            signal_mono_ns: None,
        });
    }
    Ok(monitors)
}

fn signal_perf_monitors(
    monitors: &mut PerfMonitors,
    phase_end_ns: u64,
    diag: &mut File,
) -> Result<(), Box<dyn Error>> {
    for monitor in &mut monitors.children {
        if monitor.signal_mono_ns.is_none() {
            let result = unsafe { libc::kill(monitor.child.id() as i32, libc::SIGINT) };
            let signal_mono_ns = mono_ns();
            monitor.signal_mono_ns = Some(signal_mono_ns);
            writeln!(diag, "perf_{}_sigint_result={result}", monitor.role)?;
            writeln!(
                diag,
                "perf_{}_signal_mono_ns={signal_mono_ns}",
                monitor.role
            )?;
            writeln!(
                diag,
                "perf_{}_end_offset_ns={}",
                monitor.role,
                signed_offset(signal_mono_ns, phase_end_ns)
            )?;
        }
    }
    Ok(())
}

fn finalize_perf_monitors(
    monitors: &mut PerfMonitors,
    phase_start_ns: u64,
    phase_end_ns: u64,
    perf_bin: &Path,
    diag: &mut File,
) -> Result<(), Box<dyn Error>> {
    for monitor in &mut monitors.children {
        let status = monitor.child.wait()?;
        let finalize_mono_ns = mono_ns();
        let signal_mono_ns = monitor.signal_mono_ns.unwrap_or(finalize_mono_ns);
        writeln!(diag, "perf_{}_status={status}", monitor.role)?;
        writeln!(
            diag,
            "perf_{}_output={}",
            monitor.role,
            monitor.output.display()
        )?;
        writeln!(
            diag,
            "perf_{}_stderr={}",
            monitor.role,
            monitor.stderr.display()
        )?;
        writeln!(
            diag,
            "perf_{}_start_offset_ns={}",
            monitor.role,
            signed_offset(monitor.start_mono_ns, phase_start_ns)
        )?;
        writeln!(
            diag,
            "perf_{}_end_offset_ns={}",
            monitor.role,
            signed_offset(signal_mono_ns, phase_end_ns)
        )?;
        writeln!(
            diag,
            "perf_{}_observed_wall_ns={}",
            monitor.role,
            signal_mono_ns.saturating_sub(monitor.start_mono_ns)
        )?;
        writeln!(
            diag,
            "perf_{}_finalize_wall_ns={}",
            monitor.role,
            finalize_mono_ns.saturating_sub(signal_mono_ns)
        )?;
        let record_size = fs::metadata(&monitor.output)
            .map(|metadata| metadata.len())
            .ok();
        let record_stderr = fs::read_to_string(&monitor.stderr).unwrap_or_default();
        let record_stderr_lc = record_stderr.to_ascii_lowercase();
        let record_permission_denial = record_stderr_lc.contains("permission denied")
            || record_stderr_lc.contains("access denied")
            || record_stderr_lc.contains("access to performance monitoring");
        let expected_record_sigint = monitors.mode == PerfMode::Record
            && status.signal() == Some(libc::SIGINT)
            && record_size.is_some_and(|size| size > 0)
            && !record_permission_denial;
        if monitors.mode == PerfMode::Record {
            writeln!(diag, "perf_{}_record_size={record_size:?}", monitor.role)?;
            writeln!(
                diag,
                "perf_{}_record_permission_denial={record_permission_denial}",
                monitor.role
            )?;
            if expected_record_sigint {
                writeln!(
                    diag,
                    "perf_{}_status_interpretation=expected SIGINT after nonempty perf.data was collected",
                    monitor.role
                )?;
            }
        }
        if monitors.mode == PerfMode::Record
            && (status.success() || expected_record_sigint)
            && record_size.is_some_and(|size| size > 0)
            && !record_permission_denial
        {
            let report_path = monitor.output.with_extension("report.txt");
            let report_args = [
                "report",
                "--stdio",
                "--no-children",
                "--sort",
                "comm,dso,symbol",
                "-i",
            ];
            let report_start_ns = mono_ns();
            let report = Command::new(perf_bin)
                .args(report_args)
                .arg(&monitor.output)
                .output();
            let report_end_ns = mono_ns();
            writeln!(
                diag,
                "perf_{}_report_wall_ns={}",
                monitor.role,
                report_end_ns.saturating_sub(report_start_ns)
            )?;
            let report = match report {
                Ok(report) => report,
                Err(error) => {
                    writeln!(diag, "perf_{}_report_spawn_error={error}", monitor.role)?;
                    continue;
                }
            };
            fs::write(&report_path, &report.stdout)?;
            writeln!(
                diag,
                "perf_{}_report_command={} {} --stdio --no-children --sort comm,dso,symbol -i {}",
                monitor.role,
                perf_bin.display(),
                "report",
                monitor.output.display()
            )?;
            writeln!(
                diag,
                "perf_{}_report_status={}",
                monitor.role, report.status
            )?;
            if !report.stderr.is_empty() {
                writeln!(
                    diag,
                    "perf_{}_report_stderr={}",
                    monitor.role,
                    String::from_utf8_lossy(&report.stderr).trim()
                )?;
            }
            if !report.status.success() {
                writeln!(diag, "perf_{}_report_result=failed", monitor.role)?;
            }
        }
    }
    Ok(())
}

fn make_run_dir(parent: &Path) -> Result<PathBuf, Box<dyn Error>> {
    let stamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    for suffix in 0..1000 {
        let path = parent.join(format!("run-{stamp}-{}-{suffix}", std::process::id()));
        match fs::create_dir(&path) {
            Ok(()) => return Ok(path),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e.into()),
        }
    }
    Err("could not create unique run directory".into())
}

fn write_run_metadata(dir: &Path, c: &Config) -> Result<(), Box<dyn Error>> {
    let (client, server) = c.layout.cpus();
    let mut f = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(dir.join("run.txt"))?;
    writeln!(f, "benchmark=tokio_same_host_grpc_echo")?;
    writeln!(
        f,
        "network_scope=plaintext IPv4 loopback 127.0.0.1; no physical NIC/RSS/link claims"
    )?;
    writeln!(
        f,
        "cpu_layout={} client_cpus={} server_cpus={} client_runtime_workers={} server_runtime_workers={}",
        c.layout.name(),
        format_cpus(&client),
        format_cpus(&server),
        c.layout.client,
        c.layout.server
    )?;
    writeln!(
        f,
        "monitor_affinity=host-scheduled; physical cores 6,7 and SMT siblings 14,15 are unassigned to benchmark workers"
    )?;
    writeln!(
        f,
        "payloads={:?} concurrencies={:?} channels={:?} modes={:?}",
        c.payloads,
        c.concurrencies,
        c.channels,
        c.modes.iter().map(|m| m.name()).collect::<Vec<_>>()
    )?;
    writeln!(
        f,
        "warmup_ms={} measurement_ms={} repetitions={} fixed_rate_rps={}",
        c.warmup_ms, c.seconds_ms, c.repetitions, c.rate
    )?;
    writeln!(
        f,
        "instrumentation=per-call CLOCK_MONOTONIC timestamps; every response validates ID and payload"
    )?;
    writeln!(
        f,
        "latency_sample_collection=workers accumulate locally and return samples through JoinSet; parent merges completed samples without a shared latency mutex"
    )?;
    writeln!(
        f,
        "client_cpu_endpoint=ProcessTime delta captured with the client completion timestamp before latency summarization; heartbeat stop is requested at that endpoint"
    )?;
    writeln!(
        f,
        "phase_end_signal=emitted after the completion timestamp, CPU sample, and heartbeat stop request, before latency sample merge and sorting"
    )?;
    writeln!(
        f,
        "tcp_info=legacy ss -tinH sample is about once per second when kernel sampling is off; queue values are snapshot maxima; diagnostic mode also writes its denser ss -tinmH series"
    )?;
    writeln!(
        f,
        "kernel_sample_ms={} (0 disables ss kernel sampler)",
        c.kernel_sample_ms
    )?;
    writeln!(
        f,
        "perf_mode={}; perf permission is preflighted and failures abort this run",
        c.perf_mode.name()
    )?;
    writeln!(f, "perf_binary={}", c.perf_bin.display())?;
    writeln!(
        f,
        "kernel_tcp_command=taskset -c 6,7,14,15 ss -tinmH with a per-trial server-port filter; periodic samples start after PHASE_START and are scheduled at the configured interval"
    )?;
    writeln!(
        f,
        "kernel_tcp_fields=per-endpoint queue snapshots, skmem, raw TCP_INFO, selected counters and per-flow counter deltas; sampled queue maxima are snapshots, counter deltas are end minus start"
    )?;
    Ok(())
}

fn reserve_port() -> Result<u16, Box<dyn Error>> {
    let socket = TcpListener::bind(("127.0.0.1", 0))?;
    Ok(socket.local_addr()?.port())
}

fn idle_preflight(
    c: &Config,
    log: &mut File,
    label: &str,
    port: u16,
) -> Result<Preflight, Box<dyn Error>> {
    let disk = target_disk(&c.out)?.ok_or_else(|| {
        format!(
            "cannot identify target block device for {}",
            c.out.display()
        )
    })?;
    let until = Instant::now() + Duration::from_millis(c.timeout_ms);
    let mut attempt = 0_u64;
    loop {
        attempt += 1;
        let port_start = port_clear(port)?;
        let cpu_start = cpu_sample()?.aggregate()?;
        let disk_start = disk_busy_ms(&disk)?;
        let net_start = net_sample()?;
        let started = Instant::now();
        thread::sleep(Duration::from_millis(c.preflight_ms));
        let elapsed = started.elapsed();
        let cpu_end = cpu_sample()?.aggregate()?;
        let disk_end = disk_busy_ms(&disk)?;
        let net_end = net_sample()?;
        let mem = mem_available()?;
        let port_end = port_clear(port)?;
        let total: u64 = cpu_start
            .iter()
            .zip(cpu_end.iter())
            .map(|(a, b)| b.saturating_sub(*a))
            .sum();
        let idle = cpu_end[3]
            .saturating_sub(cpu_start[3])
            .saturating_add(cpu_end[4].saturating_sub(cpu_start[4]));
        if total == 0 || elapsed.is_zero() {
            return Err("strict preflight could not measure CPU busy".into());
        }
        let cpu = (total.saturating_sub(idle) as f64 / total as f64) * 100.0;
        let disk_pct =
            disk_end.saturating_sub(disk_start) as f64 / elapsed.as_millis().max(1) as f64 * 100.0;
        let bytes = net_end
            .rx_bytes
            .saturating_sub(net_start.rx_bytes)
            .saturating_add(net_end.tx_bytes.saturating_sub(net_start.tx_bytes));
        let packets = net_end
            .rx_packets
            .saturating_sub(net_start.rx_packets)
            .saturating_add(net_end.tx_packets.saturating_sub(net_start.tx_packets));
        let clear = port_start && port_end;
        let pass = cpu <= c.max_cpu
            && disk_pct <= c.max_disk
            && mem >= c.min_mem
            && bytes <= c.max_loopback_bytes
            && packets <= c.max_loopback_packets
            && clear;
        writeln!(
            log,
            "trial={label} attempt={attempt} port={port} observation_ms={} cpu_busy_pct={cpu:.6} cpu_limit_pct={} disk_device={disk} disk_start_ms={disk_start} disk_end_ms={disk_end} disk_busy_pct={disk_pct:.6} disk_limit_pct={} mem_available_bytes={mem} mem_min_bytes={} loopback_bytes={bytes} loopback_packets={packets} loopback_byte_limit={} loopback_packet_limit={} port_clear={clear} result={}",
            elapsed.as_millis(),
            c.max_cpu,
            c.max_disk,
            c.min_mem,
            c.max_loopback_bytes,
            c.max_loopback_packets,
            if pass { "accepted" } else { "retry" }
        )?;
        log.flush()?;
        if pass {
            return Ok(Preflight {
                cpu,
                disk: disk_pct,
                mem,
                loop_bytes: bytes,
                loop_packets: packets,
                elapsed_ms: elapsed.as_millis() as u64,
            });
        }
        if Instant::now() >= until {
            return Err(
                format!("strict idle preflight timed out for {label}; see preflight.log").into(),
            );
        }
    }
}

fn target_disk(path: &Path) -> Result<Option<String>, Box<dyn Error>> {
    let canonical = fs::canonicalize(path)?;
    let mounts = fs::read_to_string("/proc/self/mountinfo")?;
    let mut best: Option<(usize, String)> = None;
    for line in mounts.lines() {
        let fields: Vec<_> = line.split_whitespace().collect();
        if fields.len() < 5 {
            continue;
        }
        let mount = decode_mount(fields[4]);
        if canonical.starts_with(&mount) {
            let len = mount.as_os_str().len();
            if best.as_ref().is_none_or(|(old, _)| len > *old) {
                best = Some((len, fields[2].to_owned()));
            }
        }
    }
    let Some((_, id)) = best else { return Ok(None) };
    for line in fs::read_to_string("/proc/diskstats")?.lines() {
        let f: Vec<_> = line.split_whitespace().collect();
        if f.len() >= 13 && format!("{}:{}", f[0], f[1]) == id {
            return Ok(Some(f[2].to_owned()));
        }
    }
    Ok(None)
}
fn decode_mount(s: &str) -> PathBuf {
    PathBuf::from(
        s.replace("\\040", " ")
            .replace("\\011", "\t")
            .replace("\\134", "\\"),
    )
}
fn disk_busy_ms(device: &str) -> Result<u64, Box<dyn Error>> {
    for line in fs::read_to_string("/proc/diskstats")?.lines() {
        let f: Vec<_> = line.split_whitespace().collect();
        if f.len() >= 13 && f[2] == device {
            return Ok(f[12].parse()?);
        }
    }
    Err(format!("block device {device} missing from /proc/diskstats").into())
}
fn mem_available() -> Result<u64, Box<dyn Error>> {
    let text = fs::read_to_string("/proc/meminfo")?;
    let kib: u64 = text
        .lines()
        .find_map(|l| {
            l.strip_prefix("MemAvailable:")
                .and_then(|v| v.split_whitespace().next())
                .and_then(|v| v.parse().ok())
        })
        .ok_or("MemAvailable missing")?;
    Ok(kib.saturating_mul(1024))
}
fn cpu_sample() -> Result<CpuSample, Box<dyn Error>> {
    let mut cpus = BTreeMap::new();
    for line in fs::read_to_string("/proc/stat")?
        .lines()
        .filter(|l| l.starts_with("cpu"))
    {
        let f: Vec<_> = line.split_whitespace().collect();
        let name = f[0];
        if name != "cpu"
            && !name
                .strip_prefix("cpu")
                .is_some_and(|s| s.chars().all(char::is_numeric))
        {
            continue;
        }
        let v: Vec<u64> = f
            .iter()
            .skip(1)
            .map(|s| s.parse())
            .collect::<Result<_, _>>()?;
        if v.len() >= 8 {
            cpus.insert(
                name.to_owned(),
                [v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7]],
            );
        }
    }
    Ok(CpuSample { cpus })
}
impl CpuSample {
    fn aggregate(&self) -> Result<[u64; 8], Box<dyn Error>> {
        self.cpus
            .get("cpu")
            .copied()
            .ok_or_else(|| "aggregate CPU counters missing".into())
    }
    fn delta_text(&self, end: &Self) -> String {
        let mut out = Vec::new();
        for (name, start) in &self.cpus {
            if name == "cpu" {
                continue;
            }
            if let Some(finish) = end.cpus.get(name) {
                let total: u64 = start
                    .iter()
                    .zip(finish)
                    .map(|(a, b)| b.saturating_sub(*a))
                    .sum();
                let idle = finish[3]
                    .saturating_sub(start[3])
                    .saturating_add(finish[4].saturating_sub(start[4]));
                let soft = finish[6].saturating_sub(start[6]);
                out.push(format!(
                    "{name}:busy_pct={:.3},softirq_pct={:.3},ticks={total}",
                    if total == 0 {
                        0.0
                    } else {
                        (total.saturating_sub(idle) as f64 / total as f64) * 100.0
                    },
                    if total == 0 {
                        0.0
                    } else {
                        soft as f64 / total as f64 * 100.0
                    }
                ));
            }
        }
        out.join(";")
    }
}
fn net_sample() -> Result<NetSample, Box<dyn Error>> {
    let line = fs::read_to_string("/proc/net/dev")?
        .lines()
        .find(|l| l.trim_start().starts_with("lo:"))
        .ok_or("lo counters missing")?
        .to_owned();
    let values: Vec<u64> = line
        .split_once(':')
        .ok_or("malformed /proc/net/dev")?
        .1
        .split_whitespace()
        .map(str::parse)
        .collect::<Result<_, _>>()?;
    if values.len() < 10 {
        return Err("loopback counters incomplete".into());
    }
    Ok(NetSample {
        rx_bytes: values[0],
        rx_packets: values[1],
        tx_bytes: values[8],
        tx_packets: values[9],
    })
}
fn port_clear(port: u16) -> Result<bool, Box<dyn Error>> {
    let output = Command::new("ss").args(["-tanH"]).output()?;
    if !output.status.success() {
        return Err("ss could not inspect TCP sockets".into());
    }
    Ok(
        !String::from_utf8_lossy(&output.stdout).lines().any(|line| {
            line.split_whitespace()
                .any(|field| endpoint_has_port(field, port))
        }),
    )
}

fn endpoint_has_port(value: &str, port: u16) -> bool {
    value
        .rsplit_once(':')
        .and_then(|(_, suffix)| suffix.parse::<u16>().ok())
        == Some(port)
}

fn run_trial(
    c: &Config,
    dir: &Path,
    label: &str,
    port: u16,
    payload: usize,
    concurrency: usize,
    channels: usize,
    mode: Mode,
) -> Result<Summary, Box<dyn Error>> {
    let mut diag = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(dir.join(format!("{label}.diagnostic.log")))?;
    let mut kernel_csv = if c.kernel_sample_ms > 0 {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(dir.join(format!("{label}.kernel_tcp.csv")))?;
        write_kernel_csv_header(&mut file)?;
        Some(file)
    } else {
        None
    };
    writeln!(
        diag,
        "kernel_sample_command=taskset -c 6,7,14,15 ss -tinmH 'sport = :{port} or dport = :{port}'"
    )?;
    writeln!(diag, "kernel_sample_interval_ms={}", c.kernel_sample_ms)?;
    let (client_cpus, server_cpus) = c.layout.cpus();
    let mut children = Children {
        server: None,
        client: None,
        server_in: None,
        client_in: None,
    };
    let (server, server_in, server_reader) = spawn_pinned(
        &server_cpus,
        &[
            "--grpc-worker=server".to_owned(),
            format!("--port={port}"),
            format!("--workers={}", c.layout.server),
        ],
    )?;
    children.server = Some(server);
    children.server_in = server_in;
    let server_events = server_reader.ok_or("server stdout missing")?;
    let _ = wait_event(
        &server_events.rx,
        "SERVER_READY",
        &mut children.server,
        Duration::from_secs(10),
    )?;
    let server_pid = children.server.as_ref().ok_or("server child missing")?.id();
    verify_affinity(server_pid, &server_cpus)?;
    writeln!(
        diag,
        "server_pid={server_pid} server_cpus={}",
        format_cpus(&server_cpus)
    )?;

    let client_args = vec![
        "--grpc-worker=client".to_owned(),
        format!("--port={port}"),
        format!("--channels={channels}"),
        format!("--payload-size={payload}"),
        format!("--mode={}", mode.name()),
        format!("--concurrency={concurrency}"),
        format!("--warmup-ms={}", c.warmup_ms),
        format!("--measure-ms={}", c.seconds_ms),
        format!("--rate={}", c.rate),
        format!("--workers={}", c.layout.client),
    ];
    let (client, client_in, client_reader) = spawn_pinned(&client_cpus, &client_args)?;
    children.client = Some(client);
    children.client_in = client_in;
    let client_events = client_reader.ok_or("client stdout missing")?;
    let client_pid = children.client.as_ref().ok_or("client child missing")?.id();
    let _ = wait_event(
        &client_events.rx,
        "CLIENT_READY",
        &mut children.client,
        Duration::from_secs(180),
    )?;
    verify_affinity(client_pid, &client_cpus)?;
    writeln!(
        diag,
        "client_pid={client_pid} client_cpus={}",
        format_cpus(&client_cpus)
    )?;
    let socket_count = wait_for_sockets(port, channels, Duration::from_secs(3), &mut diag)?;
    writeln!(diag, "verified_tcp_connections={socket_count}")?;

    let proc_c0 = proc_sample(client_pid)?;
    let cpu0 = cpu_sample()?;
    let net0 = net_sample()?;
    let disk_device = target_disk(&c.out)?.ok_or("preflight disk no longer available")?;
    let disk0 = disk_busy_ms(&disk_device)?;
    let mut rss_c_peak = proc_c0.rss_bytes;
    let proc_s0 = proc_sample(server_pid)?;
    let mut rss_s_peak = proc_s0.rss_bytes;
    let server_cpu_start_ns = mono_ns();
    let mut max_send_q = 0;
    let mut max_recv_q = 0;
    let mut last_ss = Instant::now() - Duration::from_secs(2);
    let mut kernel_snapshots = Vec::new();
    let mut kernel_sample_errors = Vec::new();
    let mut kernel_sample_wall_ns = 0_u64;
    let mut kernel_sample_cpu_ns = 0_u64;
    let mut kernel_sample_child_cpu_ns = 0_u64;
    let mut kernel_sample_max_wall_ns = 0_u64;
    let mut kernel_boundary_wall_ns = 0_u64;
    let mut kernel_boundary_cpu_ns = 0_u64;
    let mut kernel_boundary_child_cpu_ns = 0_u64;
    let mut next_kernel_sample: Option<Instant> = None;
    let mut perf_monitors = PerfMonitors {
        mode: c.perf_mode,
        children: Vec::new(),
    };
    writeln!(diag, "parent_phase_command_mono_ns={}", mono_ns())?;
    writeln!(
        diag,
        "server_cpu_sample_start_mono_ns={server_cpu_start_ns}"
    )?;
    writeln!(diag, "client_start_sample={proc_c0:?}")?;
    writeln!(diag, "server_start_sample={proc_s0:?}")?;
    writeln!(diag, "loopback_start_sample={net0:?}")?;
    children
        .client_in
        .as_mut()
        .ok_or("client stdin missing")?
        .write_all(b"START\n")?;
    children.client_in.as_mut().unwrap().flush()?;

    let mut phase_start = None;
    let phase_end;
    let mut result = None;
    loop {
        let receive_timeout = next_kernel_sample
            .map(|due| {
                due.saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(100))
            })
            .unwrap_or(Duration::from_millis(100));
        match client_events.rx.recv_timeout(receive_timeout) {
            Ok(line) => {
                writeln!(diag, "client_event={line}")?;
                if let Some(ns) = line.strip_prefix("PHASE_START\t") {
                    let phase_start_ns = ns.parse::<u64>()?;
                    phase_start = Some(phase_start_ns);
                    if c.kernel_sample_ms > 0 {
                        next_kernel_sample = Some(Instant::now());
                    }
                    perf_monitors = start_perf_monitors(
                        c,
                        dir,
                        label,
                        client_pid,
                        server_pid,
                        mode,
                        concurrency,
                        &mut diag,
                    )?;
                }
                if let Some(ns) = line.strip_prefix("PHASE_END\t") {
                    phase_end = ns.parse::<u64>()?;
                    signal_perf_monitors(&mut perf_monitors, phase_end, &mut diag)?;
                    break;
                }
                if line.starts_with("RESULT\t") {
                    result = Some(line.clone());
                }
                if let Some(error) = line.strip_prefix("CLIENT_ERROR\t") {
                    return Err(format!("client failed: {error}").into());
                }
                if let Some(error) = line.strip_prefix("READER_ERROR\t") {
                    return Err(error.to_owned().into());
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err("client stdout closed before measurement ended".into());
            }
        }
        let pc = proc_sample(client_pid)?;
        let ps = proc_sample(server_pid)?;
        rss_c_peak = rss_c_peak.max(pc.rss_bytes);
        rss_s_peak = rss_s_peak.max(ps.rss_bytes);
        if c.kernel_sample_ms == 0 && last_ss.elapsed() >= Duration::from_secs(1) {
            let (send_q, recv_q, raw) = tcp_sample(port)?;
            max_send_q = max_send_q.max(send_q);
            max_recv_q = max_recv_q.max(recv_q);
            writeln!(
                diag,
                "tcp_sample_mono_ns={} send_q_max={send_q} recv_q_max={recv_q}\n{raw}",
                mono_ns()
            )?;
            last_ss = Instant::now();
        }
        if let Some(next) = next_kernel_sample.as_mut() {
            if Instant::now() >= *next {
                let sample_cpu_start = ProcessTime::now();
                let child_cpu_start_ns = child_cpu_time_ns()?;
                let sample_wall_start = Instant::now();
                match tcp_kernel_snapshot(port, "periodic") {
                    Ok(sample) => kernel_snapshots.push(sample),
                    Err(error) => {
                        let message = error.to_string();
                        writeln!(diag, "kernel_sample_error kind=periodic error={message}")?;
                        kernel_sample_errors.push(message);
                    }
                }
                let wall_ns = sample_wall_start
                    .elapsed()
                    .as_nanos()
                    .min(u128::from(u64::MAX)) as u64;
                let cpu_ns = ProcessTime::now()
                    .duration_since(sample_cpu_start)
                    .as_nanos()
                    .min(u128::from(u64::MAX)) as u64;
                let child_cpu_ns = child_cpu_time_ns()?.saturating_sub(child_cpu_start_ns);
                kernel_sample_wall_ns = kernel_sample_wall_ns.saturating_add(wall_ns);
                kernel_sample_cpu_ns = kernel_sample_cpu_ns.saturating_add(cpu_ns);
                kernel_sample_child_cpu_ns =
                    kernel_sample_child_cpu_ns.saturating_add(child_cpu_ns);
                kernel_sample_max_wall_ns = kernel_sample_max_wall_ns.max(wall_ns);
                let interval = Duration::from_millis(c.kernel_sample_ms);
                *next += interval;
                if *next <= Instant::now() {
                    *next = Instant::now() + interval;
                }
            }
        }
    }
    let phase_start = phase_start.ok_or("client omitted PHASE_START")?;
    if phase_end <= phase_start {
        return Err("non-positive CLOCK_MONOTONIC measurement window".into());
    }
    let proc_c1 = proc_sample(client_pid)?;
    let proc_s1 = proc_sample(server_pid)?;
    let server_cpu_end_ns = mono_ns();
    rss_c_peak = rss_c_peak.max(proc_c1.rss_bytes);
    rss_s_peak = rss_s_peak.max(proc_s1.rss_bytes);
    let cpu1 = cpu_sample()?;
    let net1 = net_sample()?;
    let disk1 = disk_busy_ms(&disk_device)?;
    if c.kernel_sample_ms > 0 {
        let sample_cpu_start = ProcessTime::now();
        let child_cpu_start_ns = child_cpu_time_ns()?;
        let sample_wall_start = Instant::now();
        match tcp_kernel_snapshot(port, "phase_end_boundary") {
            Ok(sample) => kernel_snapshots.push(sample),
            Err(error) => {
                let message = error.to_string();
                writeln!(
                    diag,
                    "kernel_sample_error kind=phase_end_boundary error={message}"
                )?;
                kernel_sample_errors.push(message);
            }
        }
        let wall_ns = sample_wall_start
            .elapsed()
            .as_nanos()
            .min(u128::from(u64::MAX)) as u64;
        let cpu_ns = ProcessTime::now()
            .duration_since(sample_cpu_start)
            .as_nanos()
            .min(u128::from(u64::MAX)) as u64;
        let child_cpu_ns = child_cpu_time_ns()?.saturating_sub(child_cpu_start_ns);
        kernel_boundary_wall_ns = wall_ns;
        kernel_boundary_cpu_ns = cpu_ns;
        kernel_boundary_child_cpu_ns = child_cpu_ns;
    }
    if let Some(file) = kernel_csv.as_mut() {
        for (sample_index, sample) in kernel_snapshots.iter().enumerate() {
            write_kernel_snapshot_rows(file, label, sample_index, sample, phase_start, phase_end)?;
            if sample.kind == "periodic"
                && sample.start_mono_ns >= phase_start
                && sample.end_mono_ns <= phase_end
            {
                for socket in &sample.sockets {
                    max_send_q = max_send_q.max(socket.send_q);
                    max_recv_q = max_recv_q.max(socket.recv_q);
                }
            }
        }
        write_kernel_counter_delta_rows(file, label, &kernel_snapshots, phase_start, phase_end)?;
        file.flush()?;
    }
    let (send_q, recv_q, raw) = if c.kernel_sample_ms > 0 {
        (
            max_send_q,
            max_recv_q,
            "queue maxima are computed from fully in-phase kernel TCP snapshots; raw endpoint details are in kernel_tcp.csv".to_owned(),
        )
    } else {
        let (send_q, recv_q, raw) = tcp_sample(port)?;
        max_send_q = max_send_q.max(send_q);
        max_recv_q = max_recv_q.max(recv_q);
        (send_q, recv_q, raw)
    };
    writeln!(
        diag,
        "measurement_start_mono_ns={phase_start} measurement_end_mono_ns={phase_end} phase_wall_ns={}",
        phase_end - phase_start
    )?;
    writeln!(diag, "final_client_sample={proc_c1:?}")?;
    writeln!(diag, "final_server_sample={proc_s1:?}")?;
    writeln!(diag, "server_cpu_sample_end_mono_ns={server_cpu_end_ns}")?;
    writeln!(
        diag,
        "per_core_cpu_and_softirq_delta={}",
        cpu0.delta_text(&cpu1)
    )?;
    writeln!(
        diag,
        "target_disk={disk_device} disk_busy_ms_start={disk0} disk_busy_ms_end={disk1}"
    )?;
    writeln!(diag, "loopback_final={net1:?}")?;
    if c.kernel_sample_ms > 0 {
        let periodic_in_phase: Vec<_> = kernel_snapshots
            .iter()
            .filter(|sample| {
                sample.kind == "periodic"
                    && sample.start_mono_ns >= phase_start
                    && sample.end_mono_ns <= phase_end
            })
            .collect();
        let mut role_max: BTreeMap<&str, (u64, u64)> = BTreeMap::new();
        for sample in &periodic_in_phase {
            for socket in &sample.sockets {
                let max = role_max.entry(socket.role.as_str()).or_default();
                max.0 = max.0.max(socket.send_q);
                max.1 = max.1.max(socket.recv_q);
            }
        }
        writeln!(
            diag,
            "kernel_sample_status={} periodic_samples={} periodic_samples_inside_phase={} sample_errors={} ss_monitor_periodic_wall_ns={} ss_monitor_parent_cpu_ns={} ss_monitor_child_cpu_ns={} ss_monitor_max_wall_ns={} ss_monitor_periodic_wall_pct={:.6} ss_boundary_wall_ns={} ss_boundary_parent_cpu_ns={} ss_boundary_child_cpu_ns={}",
            if kernel_sample_errors.is_empty() && !periodic_in_phase.is_empty() {
                "ok"
            } else if periodic_in_phase.is_empty() {
                "failed"
            } else {
                "partial"
            },
            kernel_snapshots
                .iter()
                .filter(|sample| sample.kind == "periodic")
                .count(),
            periodic_in_phase.len(),
            kernel_sample_errors.len(),
            kernel_sample_wall_ns,
            kernel_sample_cpu_ns,
            kernel_sample_child_cpu_ns,
            kernel_sample_max_wall_ns,
            kernel_sample_wall_ns as f64 / phase_end.saturating_sub(phase_start).max(1) as f64
                * 100.0,
            kernel_boundary_wall_ns,
            kernel_boundary_cpu_ns,
            kernel_boundary_child_cpu_ns
        )?;
        writeln!(
            diag,
            "kernel_tcp_timeseries_csv={}",
            dir.join(format!("{label}.kernel_tcp.csv")).display()
        )?;
        for (role, (send, recv)) in role_max {
            writeln!(
                diag,
                "kernel_snapshot_max_{role}_send_q={send} kernel_snapshot_max_{role}_recv_q={recv}"
            )?;
        }
        writeln!(
            diag,
            "kernel_counter_delta_definition=last phase-end boundary TCP_INFO counter minus first fully inside-phase sample, independently per client/server socket endpoint; busy_ms/rwnd_limited_ms/sndbuf_limited_ms are elapsed millisecond deltas"
        )?;
        writeln!(
            diag,
            "kernel_counter_delta_observation_window=elapsed monotonic interval from first in-phase snapshot start to phase-end boundary snapshot end; use this measured interval as the duration-delta denominator, not the lifetime percentage embedded in ss TCP_INFO"
        )?;
    }
    writeln!(
        diag,
        "tcp_final send_q_max={send_q} recv_q_max={recv_q}\n{raw}"
    )?;
    finalize_perf_monitors(
        &mut perf_monitors,
        phase_start,
        phase_end,
        &c.perf_bin,
        &mut diag,
    )?;
    diag.flush()?;
    if result.is_none() {
        result = wait_result(&client_events.rx, &mut diag, Duration::from_secs(10))?;
    }
    let mut summary = parse_summary(&result.ok_or("client omitted RESULT")?)?;
    children
        .client_in
        .as_mut()
        .ok_or("client stdin missing before result acknowledgement")?
        .write_all(b"ACK\n")?;
    children.client_in.as_mut().unwrap().flush()?;
    let status = children
        .client
        .as_mut()
        .ok_or("client child missing")?
        .wait()?;
    if !status.success() {
        return Err(format!("client exited with {status}").into());
    }
    children.client_in.take();
    stop_server(&mut children)?;
    let status = children
        .server
        .as_mut()
        .ok_or("server child missing")?
        .wait()?;
    if !status.success() {
        return Err(format!("server exited with {status}").into());
    }

    let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) }.max(1) as u64;
    let server_proc_ns = proc_s1
        .cpu_ticks
        .saturating_sub(proc_s0.cpu_ticks)
        .saturating_mul(1_000_000_000)
        / hz;
    summary.system = SystemMetrics {
        client_cpu_ns: summary.client_cpu_ns,
        server_cpu_ns: server_proc_ns,
        server_cpu_window_ns: server_cpu_end_ns.saturating_sub(server_cpu_start_ns),
        server_cpu_start_offset_ns: signed_offset(server_cpu_start_ns, phase_start),
        server_cpu_end_offset_ns: signed_offset(server_cpu_end_ns, phase_end),
        client_rss_peak: rss_c_peak,
        server_rss_peak: rss_s_peak,
        client_io_start: proc_c0.io,
        client_io_end: proc_c1.io,
        server_io_start: proc_s0.io,
        server_io_end: proc_s1.io,
        net_rx_bytes: net1.rx_bytes.saturating_sub(net0.rx_bytes),
        net_rx_packets: net1.rx_packets.saturating_sub(net0.rx_packets),
        net_tx_bytes: net1.tx_bytes.saturating_sub(net0.tx_bytes),
        net_tx_packets: net1.tx_packets.saturating_sub(net0.tx_packets),
        tcp_send_q: max_send_q,
        tcp_recv_q: max_recv_q,
        per_core: cpu0.delta_text(&cpu1),
        disk_start: disk0,
        disk_end: disk1,
        disk_device,
    };
    writeln!(
        diag,
        "client_cpu_process_time_ns={} server_cpu_proc_stat_ns={server_proc_ns} server_cpu_window_ns={} server_start_offset_ns={} server_end_offset_ns={}",
        summary.system.client_cpu_ns,
        summary.system.server_cpu_window_ns,
        summary.system.server_cpu_start_offset_ns,
        summary.system.server_cpu_end_offset_ns
    )?;
    writeln!(
        diag,
        "client_rss_peak_bytes={rss_c_peak} server_rss_peak_bytes={rss_s_peak}"
    )?;
    writeln!(
        diag,
        "loopback_delta rx_bytes={} rx_packets={} tx_bytes={} tx_packets={}",
        summary.system.net_rx_bytes,
        summary.system.net_rx_packets,
        summary.system.net_tx_bytes,
        summary.system.net_tx_packets
    )?;
    writeln!(
        diag,
        "tcp_sampled_max_send_q={} tcp_sampled_max_recv_q={}",
        summary.system.tcp_send_q, summary.system.tcp_recv_q
    )?;
    diag.flush()?;
    Ok(summary)
}

fn spawn_pinned(
    cpus: &[usize],
    args: &[String],
) -> Result<(Child, Option<ChildStdin>, Option<Reader>), Box<dyn Error>> {
    let list = format_cpus(cpus);
    let mut child = Command::new("taskset")
        .args(["-c", &list])
        .arg(std::env::current_exe()?)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()?;
    let input = child.stdin.take();
    let reader = child.stdout.take().map(event_reader);
    Ok((child, input, reader))
}
fn event_reader(stdout: std::process::ChildStdout) -> Reader {
    let (tx, rx) = mpsc::channel();
    let join = thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            match line {
                Ok(s) => {
                    if tx.send(s).is_err() {
                        break;
                    }
                }
                Err(e) => {
                    let _ = tx.send(format!("READER_ERROR\t{e}"));
                    break;
                }
            }
        }
    });
    Reader { rx, _join: join }
}
fn wait_event(
    rx: &mpsc::Receiver<String>,
    prefix: &str,
    child: &mut Option<Child>,
    timeout: Duration,
) -> Result<String, Box<dyn Error>> {
    let end = Instant::now() + timeout;
    loop {
        if Instant::now() >= end {
            return Err(format!("timeout waiting for {prefix}").into());
        }
        match rx.recv_timeout((end - Instant::now()).min(Duration::from_millis(250))) {
            Ok(line) if line.starts_with(prefix) => return Ok(line),
            Ok(line) if line.starts_with("READER_ERROR") => return Err(line.into()),
            Ok(line) => {
                if let Some(ch) = child.as_mut() {
                    if let Some(status) = ch.try_wait()? {
                        return Err(format!(
                            "child exited {status} before {prefix}; last event {line}"
                        )
                        .into());
                    }
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if let Some(ch) = child.as_mut() {
                    if let Some(status) = ch.try_wait()? {
                        return Err(format!("child exited {status} before {prefix}").into());
                    }
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(format!("child closed stdout before {prefix}").into());
            }
        }
    }
}
fn wait_result(
    rx: &mpsc::Receiver<String>,
    diag: &mut File,
    timeout: Duration,
) -> Result<Option<String>, Box<dyn Error>> {
    let end = Instant::now() + timeout;
    while Instant::now() < end {
        match rx.recv_timeout(Duration::from_millis(100)) {
            Ok(line) => {
                writeln!(diag, "client_event={line}")?;
                if line.starts_with("RESULT\t") {
                    return Ok(Some(line));
                }
                if let Some(e) = line.strip_prefix("CLIENT_ERROR\t") {
                    return Err(e.to_owned().into());
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(None),
        }
    }
    Ok(None)
}
fn verify_affinity(pid: u32, expected: &[usize]) -> Result<(), Box<dyn Error>> {
    let status = fs::read_to_string(format!("/proc/{pid}/status"))?;
    let found = status
        .lines()
        .find_map(|l| l.strip_prefix("Cpus_allowed_list:").map(str::trim))
        .ok_or("child affinity missing")?;
    if parse_cpu_list(found)? != expected {
        return Err(
            format!("pid {pid} affinity mismatch: expected {expected:?}, got {found}").into(),
        );
    }
    Ok(())
}
fn parse_cpu_list(value: &str) -> Result<Vec<usize>, Box<dyn Error>> {
    let mut out = Vec::new();
    for item in value.split(',') {
        if let Some((a, b)) = item.split_once('-') {
            let a: usize = a.parse()?;
            let b: usize = b.parse()?;
            if b < a || b - a > 4096 {
                return Err("invalid CPU list range".into());
            }
            out.extend(a..=b);
        } else {
            out.push(item.parse()?);
        }
    }
    out.sort_unstable();
    out.dedup();
    Ok(out)
}
fn format_cpus(cpus: &[usize]) -> String {
    cpus.iter()
        .map(usize::to_string)
        .collect::<Vec<_>>()
        .join(",")
}
fn wait_for_sockets(
    port: u16,
    expected: usize,
    timeout: Duration,
    diag: &mut File,
) -> Result<usize, Box<dyn Error>> {
    let end = Instant::now() + timeout;
    let mut got = 0;
    let mut raw = String::new();
    while Instant::now() < end {
        let (count, snapshot) = established_socket_snapshot(port)?;
        got = count;
        raw = snapshot;
        if got == expected {
            return Ok(got);
        }
        thread::sleep(Duration::from_millis(50));
    }
    writeln!(diag, "socket_discovery_failure_raw_ss={raw:?}")?;
    Err(format!("expected {expected} established TCP connections on port {port}, saw {got}").into())
}
fn established_socket_snapshot(port: u16) -> Result<(usize, String), Box<dyn Error>> {
    let output = Command::new("ss")
        .args(["-tnH", "state", "established"])
        .output()?;
    if !output.status.success() {
        return Err("ss failed while counting connections".into());
    }
    let raw = String::from_utf8_lossy(&output.stdout).into_owned();
    let mut tuples = BTreeMap::new();
    for line in raw.lines() {
        let f: Vec<_> = line.split_whitespace().collect();
        if f.len() >= 4 && (endpoint_has_port(f[2], port) || endpoint_has_port(f[3], port)) {
            let (first, second) = if f[2] <= f[3] {
                (f[2], f[3])
            } else {
                (f[3], f[2])
            };
            tuples.insert(format!("{first} {second}"), ());
        }
    }
    Ok((tuples.len(), raw))
}
fn tcp_sample(port: u16) -> Result<(u64, u64, String), Box<dyn Error>> {
    let output = Command::new("ss")
        .args(["-tinH", "state", "established"])
        .output()?;
    if !output.status.success() {
        return Err("ss failed while reading TCP_INFO".into());
    }
    let mut send = 0;
    let mut recv = 0;
    let mut lines = Vec::new();
    let mut detail = false;
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let f: Vec<_> = line.split_whitespace().collect();
        if f.len() >= 4 && (endpoint_has_port(f[2], port) || endpoint_has_port(f[3], port)) {
            recv = recv.max(f[0].parse::<u64>().unwrap_or(0));
            send = send.max(f[1].parse::<u64>().unwrap_or(0));
            lines.push(line.to_owned());
            detail = true;
        } else if detail && (line.starts_with('\t') || line.starts_with(' ')) {
            lines.push(line.to_owned());
        } else {
            detail = false;
        }
    }
    Ok((send, recv, lines.join("\n")))
}

fn tcp_kernel_snapshot(port: u16, kind: &'static str) -> Result<TcpKernelSnapshot, Box<dyn Error>> {
    let start_mono_ns = mono_ns();
    let filter = format!("sport = :{port} or dport = :{port}");
    let output = Command::new("taskset")
        .args(["-c", "6,7,14,15", "ss", "-tinmH"])
        .arg(&filter)
        .output()?;
    let end_mono_ns = mono_ns();
    if !output.status.success() {
        return Err(format!(
            "taskset -c 6,7,14,15 ss -tinmH {filter:?} failed with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    let raw = String::from_utf8_lossy(&output.stdout);
    let sockets = parse_ss_kernel_output(&raw, port)?;
    Ok(TcpKernelSnapshot {
        kind,
        start_mono_ns,
        end_mono_ns,
        sockets,
    })
}

fn parse_ss_kernel_output(
    raw: &str,
    server_port: u16,
) -> Result<Vec<TcpSocketSample>, Box<dyn Error>> {
    let mut out = Vec::new();
    let mut active: Option<TcpSocketSample> = None;
    let mut info_tokens = Vec::new();
    let finish = |socket: TcpSocketSample, tokens: &[String], out: &mut Vec<TcpSocketSample>| {
        let mut socket = socket;
        let mut tcp_tokens = Vec::new();
        let mut i = 0;
        while i < tokens.len() {
            let token = tokens[i].trim();
            if token.is_empty() {
                i += 1;
                continue;
            }
            if token.starts_with("skmem:") {
                socket.skmem_raw = token.to_owned();
                i += 1;
                continue;
            }
            if ["send", "pacing_rate", "delivery_rate"].contains(&token) && i + 1 < tokens.len() {
                let combined = format!("{token} {}", tokens[i + 1].trim());
                socket
                    .info
                    .insert(token.to_owned(), tokens[i + 1].trim().to_owned());
                tcp_tokens.push(combined);
                i += 2;
                continue;
            }
            tcp_tokens.push(token.to_owned());
            if let Some((key, value)) = token.split_once(':') {
                if value.is_empty()
                    && ["send", "pacing_rate", "delivery_rate"].contains(&key)
                    && i + 1 < tokens.len()
                {
                    i += 1;
                    let combined = format!("{key} {}", tokens[i].trim());
                    socket
                        .info
                        .insert(key.to_owned(), tokens[i].trim().to_owned());
                    if let Some(last) = tcp_tokens.last_mut() {
                        *last = combined;
                    }
                } else if !value.is_empty() {
                    socket.info.insert(key.to_owned(), value.to_owned());
                }
            } else if !token.contains('(') && !token.contains(')') {
                socket.info.insert(token.to_owned(), "1".to_owned());
            }
            i += 1;
        }
        socket.tcp_info_raw = tcp_tokens.join(" ");
        out.push(socket);
    };

    for line in raw.lines() {
        let fields: Vec<_> = line.split_whitespace().collect();
        if fields.len() >= 5 && fields[0] == "ESTAB" {
            if let Some(previous) = active.take() {
                finish(previous, &info_tokens, &mut out);
                info_tokens.clear();
            }
            let recv_q = fields[1].parse::<u64>().unwrap_or(0);
            let send_q = fields[2].parse::<u64>().unwrap_or(0);
            let local = fields[3].to_owned();
            let peer = fields[4].to_owned();
            let local_port = endpoint_port(&local);
            let peer_port = endpoint_port(&peer);
            if local_port == Some(server_port) || peer_port == Some(server_port) {
                let role = if local_port == Some(server_port) {
                    "server"
                } else {
                    "client"
                };
                let (first, second) = if local <= peer {
                    (local.as_str(), peer.as_str())
                } else {
                    (peer.as_str(), local.as_str())
                };
                active = Some(TcpSocketSample {
                    role: role.to_owned(),
                    flow_id: format!("{first}<->{second}"),
                    local_endpoint: local,
                    peer_endpoint: peer,
                    recv_q,
                    send_q,
                    ..TcpSocketSample::default()
                });
            }
        } else if active.is_some() && (line.starts_with('\t') || line.starts_with(' ')) {
            info_tokens.extend(fields.into_iter().map(str::to_owned));
        } else if active.is_some() {
            if let Some(previous) = active.take() {
                finish(previous, &info_tokens, &mut out);
                info_tokens.clear();
            }
        }
    }
    if let Some(previous) = active {
        finish(previous, &info_tokens, &mut out);
    }
    Ok(out)
}

fn endpoint_port(endpoint: &str) -> Option<u16> {
    endpoint.rsplit_once(':')?.1.parse().ok()
}

const TCP_COUNTER_FIELDS: &[&str] = &[
    "bytes_sent",
    "bytes_acked",
    "bytes_received",
    "bytes_retrans",
    "segs_out",
    "segs_in",
    "data_segs_out",
    "data_segs_in",
    "delivered",
];

const TCP_DURATION_COUNTER_FIELDS: &[(&str, &str)] = &[
    ("busy_ms", "busy"),
    ("rwnd_limited_ms", "rwnd_limited"),
    ("sndbuf_limited_ms", "sndbuf_limited"),
];

const TCP_INFO_FIELDS: &[&str] = &[
    "rtt",
    "rto",
    "cwnd",
    "ssthresh",
    "mss",
    "rcvmss",
    "unacked",
    "notsent",
    "bytes_sent",
    "bytes_acked",
    "bytes_received",
    "bytes_retrans",
    "segs_out",
    "segs_in",
    "data_segs_out",
    "data_segs_in",
    "delivered",
    "retrans",
    "busy",
    "rwnd_limited",
    "sndbuf_limited",
    "delivery_rate",
    "pacing_rate",
];

fn write_kernel_csv_header(file: &mut File) -> Result<(), Box<dyn Error>> {
    let mut fields: Vec<String> = "trial,record_type,sample_index,sample_kind,capture_start_mono_ns,capture_end_mono_ns,phase_start_offset_ns,phase_end_offset_ns,inside_measurement,endpoint_role,flow_id,local_endpoint,peer_endpoint,recv_q_bytes,send_q_bytes,recv_data_direction,send_data_direction,skmem_raw,tcp_info_raw,counter_name,counter_start,counter_end,counter_delta".split(',').map(str::to_owned).collect();
    fields.extend(TCP_INFO_FIELDS.iter().map(|field| (*field).to_owned()));
    write_csv(file, &fields)?;
    Ok(())
}

fn write_kernel_snapshot_rows(
    file: &mut File,
    trial: &str,
    sample_index: usize,
    snapshot: &TcpKernelSnapshot,
    phase_start_ns: u64,
    phase_end_ns: u64,
) -> Result<(), Box<dyn Error>> {
    let inside = snapshot.start_mono_ns >= phase_start_ns && snapshot.end_mono_ns <= phase_end_ns;
    for socket in &snapshot.sockets {
        let server_side = socket.role == "server";
        let recv_direction = if server_side {
            "client_to_server"
        } else {
            "server_to_client"
        };
        let send_direction = if server_side {
            "server_to_client"
        } else {
            "client_to_server"
        };
        let mut row = vec![
            trial.to_owned(),
            "snapshot".to_owned(),
            sample_index.to_string(),
            snapshot.kind.to_owned(),
            snapshot.start_mono_ns.to_string(),
            snapshot.end_mono_ns.to_string(),
            signed_offset(snapshot.start_mono_ns, phase_start_ns).to_string(),
            signed_offset(snapshot.end_mono_ns, phase_end_ns).to_string(),
            inside.to_string(),
            socket.role.clone(),
            socket.flow_id.clone(),
            socket.local_endpoint.clone(),
            socket.peer_endpoint.clone(),
            socket.recv_q.to_string(),
            socket.send_q.to_string(),
            recv_direction.to_owned(),
            send_direction.to_owned(),
            socket.skmem_raw.clone(),
            socket.tcp_info_raw.clone(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
        ];
        row.extend(
            TCP_INFO_FIELDS
                .iter()
                .map(|field| socket.info.get(*field).cloned().unwrap_or_default()),
        );
        write_csv(file, &row)?;
    }
    Ok(())
}

fn write_kernel_counter_delta_rows(
    file: &mut File,
    trial: &str,
    snapshots: &[TcpKernelSnapshot],
    phase_start_ns: u64,
    phase_end_ns: u64,
) -> Result<(), Box<dyn Error>> {
    let in_phase: Vec<_> = snapshots
        .iter()
        .filter(|sample| {
            sample.start_mono_ns >= phase_start_ns && sample.end_mono_ns <= phase_end_ns
        })
        .collect();
    let Some(first) = in_phase.first() else {
        return Ok(());
    };
    let last = snapshots
        .iter()
        .find(|sample| sample.kind == "phase_end_boundary")
        .unwrap_or_else(|| *in_phase.last().unwrap());
    let mut first_by_socket = BTreeMap::new();
    for socket in &first.sockets {
        first_by_socket.insert((socket.role.as_str(), socket.flow_id.as_str()), socket);
    }
    for end_socket in &last.sockets {
        let Some(start_socket) =
            first_by_socket.get(&(end_socket.role.as_str(), end_socket.flow_id.as_str()))
        else {
            continue;
        };
        let counter_fields = TCP_COUNTER_FIELDS
            .iter()
            .map(|counter| (*counter, *counter))
            .chain(TCP_DURATION_COUNTER_FIELDS.iter().copied());
        for (counter, info_field) in counter_fields {
            let Some(start_value) = start_socket
                .info
                .get(info_field)
                .and_then(|value| counter_value(value))
            else {
                continue;
            };
            let Some(end_value) = end_socket
                .info
                .get(info_field)
                .and_then(|value| counter_value(value))
            else {
                continue;
            };
            let delta = end_value.saturating_sub(start_value);
            let server_side = end_socket.role == "server";
            let recv_direction = if server_side {
                "client_to_server"
            } else {
                "server_to_client"
            };
            let send_direction = if server_side {
                "server_to_client"
            } else {
                "client_to_server"
            };
            let mut row = vec![
                trial.to_owned(),
                "counter_delta".to_owned(),
                String::new(),
                "first_in_phase_to_end_boundary".to_owned(),
                first.start_mono_ns.to_string(),
                last.end_mono_ns.to_string(),
                signed_offset(first.start_mono_ns, phase_start_ns).to_string(),
                signed_offset(last.end_mono_ns, phase_end_ns).to_string(),
                "false".to_owned(),
                end_socket.role.clone(),
                end_socket.flow_id.clone(),
                end_socket.local_endpoint.clone(),
                end_socket.peer_endpoint.clone(),
                String::new(),
                String::new(),
                recv_direction.to_owned(),
                send_direction.to_owned(),
                String::new(),
                String::new(),
                counter.to_owned(),
                start_value.to_string(),
                end_value.to_string(),
                delta.to_string(),
            ];
            row.extend(
                TCP_INFO_FIELDS
                    .iter()
                    .map(|field| end_socket.info.get(*field).cloned().unwrap_or_default()),
            );
            write_csv(file, &row)?;
        }
    }
    Ok(())
}

fn counter_value(value: &str) -> Option<u64> {
    let value = value.split('/').next()?.split('(').next()?.trim();
    value
        .strip_suffix("ms")
        .unwrap_or(value)
        .trim()
        .parse()
        .ok()
}
fn proc_sample(pid: u32) -> Result<ProcSample, Box<dyn Error>> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let end = stat.rfind(')').ok_or("invalid process stat")?;
    let f: Vec<_> = stat[end + 1..].split_whitespace().collect();
    let cpu = f
        .get(11)
        .ok_or("utime missing")?
        .parse::<u64>()?
        .saturating_add(f.get(12).ok_or("stime missing")?.parse::<u64>()?);
    let pages: u64 = f.get(21).ok_or("RSS missing")?.parse()?;
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) }.max(1) as u64;
    let io_text = fs::read_to_string(format!("/proc/{pid}/io"))?;
    let mut io = BTreeMap::new();
    for line in io_text.lines() {
        if let Some((k, v)) = line.split_once(':') {
            io.insert(k, v.trim().parse::<u64>()?);
        }
    }
    Ok(ProcSample {
        cpu_ticks: cpu,
        rss_bytes: pages.saturating_mul(page),
        io: IoCounters {
            rchar: *io.get("rchar").ok_or("rchar missing")?,
            wchar: *io.get("wchar").ok_or("wchar missing")?,
            syscr: *io.get("syscr").ok_or("syscr missing")?,
            syscw: *io.get("syscw").ok_or("syscw missing")?,
            read_bytes: *io.get("read_bytes").ok_or("read_bytes missing")?,
            write_bytes: *io.get("write_bytes").ok_or("write_bytes missing")?,
            cancelled_write_bytes: *io
                .get("cancelled_write_bytes")
                .ok_or("cancelled_write_bytes missing")?,
        },
    })
}
fn mono_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    let result = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    assert_eq!(result, 0, "CLOCK_MONOTONIC failed");
    (ts.tv_sec as u64)
        .saturating_mul(1_000_000_000)
        .saturating_add(ts.tv_nsec as u64)
}

fn child_cpu_time_ns() -> Result<u64, Box<dyn Error>> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    if unsafe { libc::getrusage(libc::RUSAGE_CHILDREN, usage.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error().into());
    }
    let usage = unsafe { usage.assume_init() };
    let timeval_ns = |value: libc::timeval| -> u128 {
        (value.tv_sec.max(0) as u128)
            .saturating_mul(1_000_000_000)
            .saturating_add((value.tv_usec.max(0) as u128).saturating_mul(1_000))
    };
    Ok(timeval_ns(usage.ru_utime)
        .saturating_add(timeval_ns(usage.ru_stime))
        .min(u128::from(u64::MAX)) as u64)
}

fn signed_offset(sample_ns: u64, phase_ns: u64) -> i64 {
    if sample_ns >= phase_ns {
        (sample_ns - phase_ns).min(i64::MAX as u64) as i64
    } else {
        -((phase_ns - sample_ns).min(i64::MAX as u64) as i64)
    }
}
fn stop_server(children: &mut Children) -> Result<(), Box<dyn Error>> {
    if let Some(mut input) = children.server_in.take() {
        input.write_all(b"STOP\n")?;
        input.flush()?;
    }
    Ok(())
}

fn run_worker(worker: &str, args: &[String]) -> Result<(), Box<dyn Error>> {
    let values: BTreeMap<String, String> = args
        .iter()
        .filter_map(|s| s.split_once('='))
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect();
    match worker {
        "server" => {
            let port = worker_arg(&values, "--port")?.parse::<u16>()?;
            let workers = worker_arg(&values, "--workers")?.parse::<usize>()?;
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(workers.max(1))
                .enable_all()
                .build()?;
            runtime.block_on(server_main(port))?;
        }
        "client" => {
            let port = worker_arg(&values, "--port")?.parse::<u16>()?;
            let channels = worker_arg(&values, "--channels")?.parse::<usize>()?;
            let payload = worker_arg(&values, "--payload-size")?.parse::<usize>()?;
            let mode = match worker_arg(&values, "--mode")? {
                "fixed_rate" => Mode::Fixed,
                "closed_loop" => Mode::Closed,
                _ => return Err("bad internal mode".into()),
            };
            let concurrency = worker_arg(&values, "--concurrency")?.parse::<usize>()?;
            let warmup = worker_arg(&values, "--warmup-ms")?.parse::<u64>()?;
            let measure = worker_arg(&values, "--measure-ms")?.parse::<u64>()?;
            let rate = worker_arg(&values, "--rate")?.parse::<u64>()?;
            let workers = worker_arg(&values, "--workers")?.parse::<usize>()?;
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(workers.max(1))
                .enable_all()
                .build()?;
            runtime.block_on(client_main(
                port,
                channels,
                payload,
                mode,
                concurrency,
                warmup,
                measure,
                rate,
            ))?;
        }
        _ => return Err(format!("unknown worker role {worker:?}").into()),
    }
    Ok(())
}
fn worker_arg<'a>(
    args: &'a BTreeMap<String, String>,
    name: &str,
) -> Result<&'a str, Box<dyn Error>> {
    args.get(name)
        .map(String::as_str)
        .ok_or_else(|| format!("missing internal arg {name}").into())
}

async fn server_main(port: u16) -> Result<(), Box<dyn Error>> {
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse()?;
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(EchoBenchServer::new(EchoService))
            .serve_with_shutdown(addr, async {
                let _ = stop_rx.await;
            })
            .await
    });
    let mut ready = false;
    for _ in 0..200 {
        if let Ok(stream) = tokio::net::TcpStream::connect(addr).await {
            drop(stream);
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    if !ready {
        let _ = stop_tx.send(());
        return Err(format!("server failed to bind {addr}").into());
    }
    println!("SERVER_READY\t{port}");
    io::stdout().flush()?;
    let mut line = String::new();
    let mut input = tokio::io::BufReader::new(tokio::io::stdin());
    let _ = input.read_line(&mut line).await?;
    let _ = stop_tx.send(());
    server.await??;
    Ok(())
}

struct EchoService;
#[tonic::async_trait]
impl EchoBench for EchoService {
    async fn echo(&self, request: Request<EchoRequest>) -> Result<Response<EchoResponse>, Status> {
        let entry = mono_ns();
        let request = request.into_inner();
        let mut response = EchoResponse {
            id: request.id,
            payload: request.payload,
            handler_entry_mono_ns: entry,
            handler_exit_mono_ns: 0,
        };
        response.handler_exit_mono_ns = mono_ns();
        Ok(Response::new(response))
    }
}

async fn client_main(
    port: u16,
    channels: usize,
    payload_size: usize,
    mode: Mode,
    concurrency: usize,
    warmup_ms: u64,
    measure_ms: u64,
    rate: u64,
) -> Result<(), Box<dyn Error>> {
    let endpoint = Endpoint::from_shared(format!("http://127.0.0.1:{port}"))?
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(30))
        .tcp_nodelay(true);
    let mut clients = Vec::with_capacity(channels);
    for _ in 0..channels {
        clients.push(EchoBenchClient::new(endpoint.clone().connect().await?));
    }
    let payload = Arc::new(
        (0..payload_size)
            .map(|i| ((i * 37 + 11) % 251) as u8)
            .collect::<Vec<_>>(),
    );
    let next_id = Arc::new(AtomicU64::new(1));
    for (index, client) in clients.iter_mut().enumerate() {
        let id = u64::MAX - index as u64;
        let bytes = payload.as_ref().clone();
        let response = client
            .echo(Request::new(EchoRequest {
                id,
                payload: bytes.clone(),
            }))
            .await?
            .into_inner();
        if response.id != id || response.payload != bytes {
            return Err("priming echo changed the ID or payload".into());
        }
        if !(response.handler_entry_mono_ns <= response.handler_exit_mono_ns
            && response.handler_exit_mono_ns <= mono_ns())
        {
            return Err("invalid server monotonic timestamps during channel priming".into());
        }
    }
    if warmup_ms > 0 {
        let _ = load(
            clients.clone(),
            Arc::clone(&payload),
            Arc::clone(&next_id),
            mode,
            concurrency,
            rate,
            Duration::from_millis(warmup_ms),
            None,
            false,
            None,
            None,
        )
        .await?;
    }
    let channel_hold = clients.clone();
    println!("CLIENT_READY\t{channels}");
    io::stdout().flush()?;
    let mut line = String::new();
    let mut input = tokio::io::BufReader::new(tokio::io::stdin());
    if input.read_line(&mut line).await? == 0 || line.trim() != "START" {
        return Err("parent did not send START".into());
    }

    let phase_start = Instant::now();
    let start_ns = mono_ns();
    let cpu_start = ProcessTime::now();
    println!("PHASE_START\t{start_ns}");
    io::stdout().flush()?;
    let heartbeat = Arc::new(Mutex::new(Heartbeat::default()));
    let (stop_tx, stop_rx) = oneshot::channel();
    let heartbeat_task = tokio::spawn(heartbeat_loop(Arc::clone(&heartbeat), stop_rx));
    let measured = load(
        clients,
        Arc::clone(&payload),
        next_id,
        mode,
        concurrency,
        rate,
        Duration::from_millis(measure_ms),
        Some((phase_start, start_ns)),
        true,
        Some(cpu_start),
        Some(stop_tx),
    )
    .await?;
    let end_ns = measured.completion_ns;
    heartbeat_task.await?;
    let (heartbeat_stats, heartbeat_max, heartbeat_missed) = {
        let mut h = heartbeat.lock().expect("heartbeat mutex poisoned");
        let max = h.values.iter().copied().max().unwrap_or(0);
        (summarize(&mut h.values), max, h.missed)
    };
    let summary = Summary {
        mode,
        payload: payload_size,
        concurrency,
        channels,
        issued: measured.issued,
        completed: measured.completed,
        skipped: measured.skipped,
        issue_ns: measured.issue_ns,
        completion_ns: end_ns.saturating_sub(start_ns),
        drain_ns: measured.drain_ns,
        client_cpu_ns: measured.client_cpu_ns,
        latency: measured.latency,
        heartbeat: heartbeat_stats,
        heartbeat_max,
        heartbeat_missed,
        system: SystemMetrics::default(),
    };
    println!("{}", summary_wire(&summary));
    io::stdout().flush()?;
    let mut acknowledgement = String::new();
    if input.read_line(&mut acknowledgement).await? == 0 || acknowledgement.trim() != "ACK" {
        return Err("parent did not acknowledge completed measurement".into());
    }
    drop(channel_hold);
    Ok(())
}

struct LoadOutcome {
    issued: u64,
    completed: u64,
    skipped: u64,
    issue_ns: u64,
    completion_ns: u64,
    drain_ns: u64,
    client_cpu_ns: u64,
    latency: [Stats; 4],
}
enum SampleBatch {
    One(Option<Timing>),
    Many(Vec<Timing>),
}
async fn load(
    clients: Vec<EchoBenchClient<Channel>>,
    payload: Arc<Vec<u8>>,
    next_id: Arc<AtomicU64>,
    mode: Mode,
    concurrency: usize,
    rate: u64,
    duration: Duration,
    phase_start: Option<(Instant, u64)>,
    collect_samples: bool,
    phase_cpu_start: Option<ProcessTime>,
    heartbeat_stop: Option<oneshot::Sender<()>>,
) -> Result<LoadOutcome, Box<dyn Error>> {
    let (start, start_mono_ns) = phase_start.unwrap_or_else(|| (Instant::now(), mono_ns()));
    let deadline = start + duration;
    let issued = Arc::new(AtomicU64::new(0));
    let completed = Arc::new(AtomicU64::new(0));
    let mut sample_batches = Vec::new();
    let mut skipped = 0_u64;
    let issue_end;
    match mode {
        Mode::Closed => {
            let mut tasks = JoinSet::new();
            for worker in 0..concurrency {
                let client = clients[worker % clients.len()].clone();
                let payload = Arc::clone(&payload);
                let ids = Arc::clone(&next_id);
                let issued = Arc::clone(&issued);
                let completed = Arc::clone(&completed);
                tasks.spawn(async move {
                    let mut samples = Vec::new();
                    while Instant::now() < deadline {
                        issued.fetch_add(1, Ordering::Relaxed);
                        let timing = echo(client.clone(), Arc::clone(&payload), &ids).await?;
                        completed.fetch_add(1, Ordering::Relaxed);
                        if collect_samples {
                            samples.push(timing);
                        }
                    }
                    Ok::<SampleBatch, String>(SampleBatch::Many(samples))
                });
            }
            sleep_until(tokio::time::Instant::from_std(deadline)).await;
            issue_end = mono_ns();
            while let Some(result) = tasks.join_next().await {
                match result {
                    Ok(Ok(batch)) => {
                        if collect_samples {
                            sample_batches.push(batch);
                        }
                    }
                    Ok(Err(e)) => return Err(io::Error::other(e).into()),
                    Err(e) => return Err(e.into()),
                }
            }
        }
        Mode::Fixed => {
            let interval = 1_000_000_000_u64 / rate;
            if interval == 0 {
                return Err("rate above one billion is unsupported".into());
            }
            let mut tasks = JoinSet::new();
            let mut tick = 0_u64;
            loop {
                let target = start + Duration::from_nanos(interval.saturating_mul(tick));
                if target >= deadline {
                    break;
                }
                sleep_until(tokio::time::Instant::from_std(target)).await;
                if Instant::now() >= deadline {
                    break;
                }
                while let Some(result) = tasks.try_join_next() {
                    match result {
                        Ok(Ok(batch)) => {
                            if collect_samples {
                                sample_batches.push(batch);
                            }
                        }
                        Ok(Err(e)) => return Err(io::Error::other(e).into()),
                        Err(e) => return Err(e.into()),
                    }
                }
                if tasks.len() < FIXED_IN_FLIGHT {
                    let index = issued.fetch_add(1, Ordering::Relaxed) as usize;
                    let client = clients[index % clients.len()].clone();
                    let payload = Arc::clone(&payload);
                    let ids = Arc::clone(&next_id);
                    let completed = Arc::clone(&completed);
                    tasks.spawn(async move {
                        let timing = echo(client, payload, &ids).await?;
                        completed.fetch_add(1, Ordering::Relaxed);
                        Ok::<SampleBatch, String>(SampleBatch::One(
                            collect_samples.then_some(timing),
                        ))
                    });
                } else {
                    skipped = skipped.saturating_add(1);
                }
                tick = tick.saturating_add(1);
                let elapsed = Instant::now().saturating_duration_since(start).as_nanos() as u64;
                let next = elapsed / interval + 1;
                if next > tick {
                    skipped = skipped.saturating_add(next - tick);
                    tick = next;
                }
            }
            issue_end = mono_ns();
            while let Some(result) = tasks.join_next().await {
                match result {
                    Ok(Ok(batch)) => {
                        if collect_samples {
                            sample_batches.push(batch);
                        }
                    }
                    Ok(Err(e)) => return Err(io::Error::other(e).into()),
                    Err(e) => return Err(e.into()),
                }
            }
        }
    }
    let completion = mono_ns();
    let client_cpu_ns = phase_cpu_start
        .map(|start| {
            ProcessTime::now()
                .duration_since(start)
                .as_nanos()
                .min(u128::from(u64::MAX)) as u64
        })
        .unwrap_or_default();
    if let Some(stop) = heartbeat_stop {
        let _ = stop.send(());
    }
    if phase_cpu_start.is_some() {
        println!("PHASE_END\t{completion}");
        io::stdout().flush()?;
    }
    let issued = issued.load(Ordering::Relaxed);
    let completed = completed.load(Ordering::Relaxed);
    if issued != completed {
        return Err(format!("issued/completed mismatch {issued}/{completed}").into());
    }
    let mut latency = Latencies::default();
    for batch in sample_batches {
        match batch {
            SampleBatch::One(Some(timing)) => latency.push(timing),
            SampleBatch::One(None) => {}
            SampleBatch::Many(samples) => {
                for timing in samples {
                    latency.push(timing);
                }
            }
        }
    }
    if collect_samples && latency.e2e.len() as u64 != completed {
        return Err(format!(
            "latency sample/completed mismatch {}/{}",
            latency.e2e.len(),
            completed
        )
        .into());
    }
    let stats = latency.finish();
    Ok(LoadOutcome {
        issued,
        completed,
        skipped,
        issue_ns: issue_end.saturating_sub(start_mono_ns),
        completion_ns: completion,
        drain_ns: completion.saturating_sub(issue_end),
        client_cpu_ns,
        latency: stats,
    })
}

async fn echo(
    mut client: EchoBenchClient<Channel>,
    payload: Arc<Vec<u8>>,
    ids: &AtomicU64,
) -> Result<Timing, String> {
    let id = ids.fetch_add(1, Ordering::Relaxed);
    let request = Request::new(EchoRequest {
        id,
        payload: payload.as_ref().clone(),
    });
    let t0 = mono_ns();
    let response = client
        .echo(request)
        .await
        .map_err(|e| format!("echo RPC {id} failed: {e}"))?
        .into_inner();
    let t3 = mono_ns();
    if response.id != id || response.payload.as_slice() != payload.as_slice() {
        return Err(format!("echo RPC {id} changed ID or payload"));
    }
    let t1 = response.handler_entry_mono_ns;
    let t2 = response.handler_exit_mono_ns;
    if !(t0 <= t1 && t1 <= t2 && t2 <= t3) {
        return Err(format!(
            "clock ordering failed for {id}: {t0}<={t1}<={t2}<={t3}"
        ));
    }
    Ok(Timing {
        e2e: t3 - t0,
        t0_t1: t1 - t0,
        t1_t2: t2 - t1,
        t2_t3: t3 - t2,
    })
}

async fn heartbeat_loop(stats: Arc<Mutex<Heartbeat>>, stop: oneshot::Receiver<()>) {
    let mut stop = Box::pin(stop);
    let period = Duration::from_millis(1);
    let period_ns = 1_000_000_u64;
    let mut target = Instant::now() + period;
    loop {
        tokio::select! {
            biased;
            _=&mut stop=>break,
            _=sleep_until(tokio::time::Instant::from_std(target))=>{
                let late=Instant::now().saturating_duration_since(target).as_nanos().min(u128::from(u64::MAX))as u64;
                let mut h=stats.lock().expect("heartbeat mutex poisoned");h.values.push(late);h.missed=h.missed.saturating_add(late/period_ns);drop(h);
                target+=Duration::from_nanos(period_ns.saturating_mul(late/period_ns+1));
            }
        }
    }
}

fn summary_wire(s: &Summary) -> String {
    let mut f = vec![
        "RESULT".to_owned(),
        s.mode.name().into(),
        s.payload.to_string(),
        s.concurrency.to_string(),
        s.channels.to_string(),
        s.issued.to_string(),
        s.completed.to_string(),
        s.skipped.to_string(),
        s.issue_ns.to_string(),
        s.completion_ns.to_string(),
        s.drain_ns.to_string(),
        s.client_cpu_ns.to_string(),
        s.heartbeat.count.to_string(),
        s.heartbeat.mean_ns.to_string(),
        s.heartbeat.p95.to_string(),
        s.heartbeat.p99.to_string(),
        s.heartbeat_max.to_string(),
        s.heartbeat_missed.to_string(),
    ];
    for x in s.latency {
        f.extend([
            x.count.to_string(),
            x.total_ns.to_string(),
            x.mean_ns.to_string(),
            x.p50.to_string(),
            x.p95.to_string(),
            x.p99.to_string(),
            x.p999.to_string(),
        ]);
    }
    f.join("\t")
}
fn parse_summary(line: &str) -> Result<Summary, Box<dyn Error>> {
    let f: Vec<_> = line.split('\t').collect();
    if f.len() != 46 || f[0] != "RESULT" {
        return Err(format!("malformed RESULT: expected 46 fields, got {}", f.len()).into());
    }
    let mode = match f[1] {
        "fixed_rate" => Mode::Fixed,
        "closed_loop" => Mode::Closed,
        _ => return Err("unknown client result mode".into()),
    };
    let mut latency = [Stats::default(); 4];
    let mut i = 18;
    for x in &mut latency {
        x.count = f[i].parse()?;
        x.total_ns = f[i + 1].parse()?;
        x.mean_ns = f[i + 2].parse()?;
        x.p50 = f[i + 3].parse()?;
        x.p95 = f[i + 4].parse()?;
        x.p99 = f[i + 5].parse()?;
        x.p999 = f[i + 6].parse()?;
        i += 7;
    }
    Ok(Summary {
        mode,
        payload: f[2].parse()?,
        concurrency: f[3].parse()?,
        channels: f[4].parse()?,
        issued: f[5].parse()?,
        completed: f[6].parse()?,
        skipped: f[7].parse()?,
        issue_ns: f[8].parse()?,
        completion_ns: f[9].parse()?,
        drain_ns: f[10].parse()?,
        client_cpu_ns: f[11].parse()?,
        heartbeat: Stats {
            count: f[12].parse()?,
            mean_ns: f[13].parse()?,
            p95: f[14].parse()?,
            p99: f[15].parse()?,
            ..Stats::default()
        },
        heartbeat_max: f[16].parse()?,
        heartbeat_missed: f[17].parse()?,
        latency,
        system: SystemMetrics::default(),
    })
}

fn csv_header() -> Vec<String> {
    let mut fields:Vec<String>="mode,payload_bytes,concurrency,independent_channels,repetition,cpu_layout,issued,completed,errors,completed_rps,issue_window_s,completion_window_s,drain_s,client_cpu_core_equiv,server_cpu_core_equiv,client_cpu_ns,server_cpu_ns,server_cpu_window_ns,server_cpu_start_offset_ns,server_cpu_end_offset_ns,client_peak_rss_bytes,server_peak_rss_bytes,fixed_rate_skipped_ticks,heartbeat_count,heartbeat_mean_ns,heartbeat_p95_ns,heartbeat_p99_ns,heartbeat_max_ns,heartbeat_missed_ticks,preflight_cpu_busy_pct,preflight_disk_busy_pct,preflight_mem_available_bytes,preflight_loopback_bytes,preflight_loopback_packets,preflight_observation_ms,measurement_loopback_rx_bytes,measurement_loopback_rx_packets,measurement_loopback_tx_bytes,measurement_loopback_tx_packets,tcp_sampled_max_send_q,tcp_sampled_max_recv_q,target_disk,disk_busy_ms_start,disk_busy_ms_end,client_read_bytes_start,client_read_bytes_end,client_write_bytes_start,client_write_bytes_end,server_read_bytes_start,server_read_bytes_end,server_write_bytes_start,server_write_bytes_end,client_rchar_start,client_rchar_end,client_wchar_start,client_wchar_end,client_syscr_start,client_syscr_end,client_syscw_start,client_syscw_end,client_cancelled_write_bytes_start,client_cancelled_write_bytes_end,server_rchar_start,server_rchar_end,server_wchar_start,server_wchar_end,server_syscr_start,server_syscr_end,server_syscw_start,server_syscw_end,server_cancelled_write_bytes_start,server_cancelled_write_bytes_end,per_core_cpu_softirq_delta,network_scope".split(',').map(str::to_owned).collect();
    for phase in ["e2e", "t0_t1", "t1_t2", "t2_t3"] {
        for metric in [
            "count", "total_ns", "mean_ns", "p50_ns", "p95_ns", "p99_ns", "p999_ns",
        ] {
            fields.push(format!("{phase}_{metric}"));
        }
    }
    fields
}
fn write_csv_header(file: &mut File) -> Result<(), Box<dyn Error>> {
    write_csv(file, &csv_header())
}
fn write_csv(file: &mut File, fields: &[String]) -> Result<(), Box<dyn Error>> {
    for (index, field) in fields.iter().enumerate() {
        if index > 0 {
            file.write_all(b",")?;
        }
        if field.contains([',', '"', '\n', '\r']) {
            file.write_all(b"\"")?;
            file.write_all(field.replace('"', "\"\"").as_bytes())?;
            file.write_all(b"\"")?;
        } else {
            file.write_all(field.as_bytes())?;
        }
    }
    file.write_all(b"\n")?;
    Ok(())
}
fn csv_row(c: &Config, s: &Summary, repetition: usize, pre: &Preflight) -> Vec<String> {
    let sys = &s.system;
    let wall = s.completion_ns.max(1);
    let mut row = vec![
        s.mode.name().into(),
        s.payload.to_string(),
        s.concurrency.to_string(),
        s.channels.to_string(),
        repetition.to_string(),
        c.layout.name(),
        s.issued.to_string(),
        s.completed.to_string(),
        "0".into(),
        float(s.completed as f64 / wall as f64 * 1e9),
        float(s.issue_ns as f64 / 1e9),
        float(s.completion_ns as f64 / 1e9),
        float(s.drain_ns as f64 / 1e9),
        float(sys.client_cpu_ns as f64 / wall as f64),
        float(sys.server_cpu_ns as f64 / sys.server_cpu_window_ns.max(1) as f64),
        sys.client_cpu_ns.to_string(),
        sys.server_cpu_ns.to_string(),
        sys.server_cpu_window_ns.to_string(),
        sys.server_cpu_start_offset_ns.to_string(),
        sys.server_cpu_end_offset_ns.to_string(),
        sys.client_rss_peak.to_string(),
        sys.server_rss_peak.to_string(),
        s.skipped.to_string(),
        s.heartbeat.count.to_string(),
        float(s.heartbeat.mean_ns),
        s.heartbeat.p95.to_string(),
        s.heartbeat.p99.to_string(),
        s.heartbeat_max.to_string(),
        s.heartbeat_missed.to_string(),
        float(pre.cpu),
        float(pre.disk),
        pre.mem.to_string(),
        pre.loop_bytes.to_string(),
        pre.loop_packets.to_string(),
        pre.elapsed_ms.to_string(),
        sys.net_rx_bytes.to_string(),
        sys.net_rx_packets.to_string(),
        sys.net_tx_bytes.to_string(),
        sys.net_tx_packets.to_string(),
        sys.tcp_send_q.to_string(),
        sys.tcp_recv_q.to_string(),
        sys.disk_device.clone(),
        sys.disk_start.to_string(),
        sys.disk_end.to_string(),
        sys.client_io_start.read_bytes.to_string(),
        sys.client_io_end.read_bytes.to_string(),
        sys.client_io_start.write_bytes.to_string(),
        sys.client_io_end.write_bytes.to_string(),
        sys.server_io_start.read_bytes.to_string(),
        sys.server_io_end.read_bytes.to_string(),
        sys.server_io_start.write_bytes.to_string(),
        sys.server_io_end.write_bytes.to_string(),
        sys.client_io_start.rchar.to_string(),
        sys.client_io_end.rchar.to_string(),
        sys.client_io_start.wchar.to_string(),
        sys.client_io_end.wchar.to_string(),
        sys.client_io_start.syscr.to_string(),
        sys.client_io_end.syscr.to_string(),
        sys.client_io_start.syscw.to_string(),
        sys.client_io_end.syscw.to_string(),
        sys.client_io_start.cancelled_write_bytes.to_string(),
        sys.client_io_end.cancelled_write_bytes.to_string(),
        sys.server_io_start.rchar.to_string(),
        sys.server_io_end.rchar.to_string(),
        sys.server_io_start.wchar.to_string(),
        sys.server_io_end.wchar.to_string(),
        sys.server_io_start.syscr.to_string(),
        sys.server_io_end.syscr.to_string(),
        sys.server_io_start.syscw.to_string(),
        sys.server_io_end.syscw.to_string(),
        sys.server_io_start.cancelled_write_bytes.to_string(),
        sys.server_io_end.cancelled_write_bytes.to_string(),
        sys.per_core.clone(),
        "localhost-loopback-only".into(),
    ];
    for stats in s.latency {
        row.extend([
            stats.count.to_string(),
            stats.total_ns.to_string(),
            float(stats.mean_ns),
            stats.p50.to_string(),
            stats.p95.to_string(),
            stats.p99.to_string(),
            stats.p999.to_string(),
        ]);
    }
    row
}
fn float(value: f64) -> String {
    format!("{value:.9}")
}
