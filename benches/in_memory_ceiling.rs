use cpu_time::ProcessTime;
use ledger_wallet::transaction::ports::ShardPorts;
use ledger_wallet::transaction::{
    InMemoryIndex, InMemoryProjection, InMemoryWal, ItemOutcome, Transaction, TransactionProcessor,
    TxKey,
};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

const DEFAULT_TRANSACTIONS: usize = 5_000;
const DEFAULT_WARMUP: usize = 500;
const DEFAULT_REPETITIONS: usize = 3;
const MAX_TRANSACTIONS: usize = 2_000_000;
const MAX_WARMUP: usize = 50_000;
const NORMAL_SUBMITTERS: [usize; 7] = [1, 2, 4, 8, 16, 32, 64];
const BATCH_SUBMITTERS: [usize; 3] = [1, 4, 16];
const BATCH_SIZES: [usize; 3] = [16, 64, 256];
const SHARD_COUNT: usize = 1;
const WORKER_COUNT: usize = 1;
const MAX_QUEUED_ITEMS_PER_SHARD: usize = 4_096;

#[derive(Clone, Copy)]
struct Config {
    transactions: usize,
    warmup: usize,
    repetitions: usize,
    mode: Mode,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
    All,
    Normal,
    Batch,
}

impl Mode {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "all" => Ok(Self::All),
            "normal" => Ok(Self::Normal),
            "batch" => Ok(Self::Batch),
            _ => Err("--mode must be one of: all, normal, batch".to_owned()),
        }
    }

    fn includes_normal(self) -> bool {
        matches!(self, Self::All | Self::Normal)
    }

    fn includes_batch(self) -> bool {
        matches!(self, Self::All | Self::Batch)
    }
}

#[derive(Clone, Copy, Debug)]
enum Workload {
    SixtyFourAccounts,
    HotAccount,
}

impl Workload {
    fn name(self) -> &'static str {
        match self {
            Self::SixtyFourAccounts => "sixty_four_accounts",
            Self::HotAccount => "hot_account",
        }
    }

    fn account_id(self, sequence: usize) -> u64 {
        match self {
            Self::SixtyFourAccounts => (sequence % 64) as u64,
            Self::HotAccount => 0,
        }
    }

    fn account_count(self) -> usize {
        match self {
            Self::SixtyFourAccounts => 64,
            Self::HotAccount => 1,
        }
    }
}

#[derive(Clone, Debug, Default)]
struct Counts {
    requests: usize,
    attempted_transactions: usize,
    applied: usize,
    duplicate: usize,
    rejected: usize,
    pending_errors: usize,
    missing_outcomes: usize,
    extra_outcomes: usize,
}

impl Counts {
    fn record_call(&mut self, attempted_transactions: usize, outcomes: &[ItemOutcome]) {
        self.requests += 1;
        self.attempted_transactions += attempted_transactions;
        for (position, outcome) in outcomes.iter().enumerate() {
            if position >= attempted_transactions {
                self.extra_outcomes += 1;
                continue;
            }
            match outcome {
                ItemOutcome::Applied(_) => self.applied += 1,
                ItemOutcome::Duplicate { .. } => self.duplicate += 1,
                ItemOutcome::Rejected(_) => self.rejected += 1,
                ItemOutcome::Pending { .. } => self.pending_errors += 1,
            }
        }
        self.missing_outcomes += attempted_transactions.saturating_sub(outcomes.len());
    }

    fn merge(&mut self, other: &Self) {
        self.requests += other.requests;
        self.attempted_transactions += other.attempted_transactions;
        self.applied += other.applied;
        self.duplicate += other.duplicate;
        self.rejected += other.rejected;
        self.pending_errors += other.pending_errors;
        self.missing_outcomes += other.missing_outcomes;
        self.extra_outcomes += other.extra_outcomes;
    }

    fn errors(&self) -> usize {
        self.pending_errors + self.missing_outcomes + self.extra_outcomes
    }
}

struct PhaseResult {
    counts: Counts,
    latencies: Vec<Duration>,
    wall_time: Duration,
    process_cpu_time: Duration,
}

struct SubmitterResult {
    counts: Counts,
    latencies: Vec<Duration>,
}

struct RoundResult {
    counts: Counts,
    requests_per_second: f64,
    applied_transactions_per_second: f64,
    wall_seconds: f64,
    process_cpu_cores: f64,
    process_cpu_available_percent: f64,
    p50_request_latency_ms: f64,
    p95_request_latency_ms: f64,
    p99_request_latency_ms: f64,
    latency_samples: usize,
}

fn parse_usize(flag: &str, value: Option<String>) -> Result<usize, String> {
    value
        .ok_or_else(|| format!("{flag} requires a value"))?
        .parse::<usize>()
        .map_err(|_| format!("{flag} must be a non-negative integer"))
}

fn parse_args() -> Result<Option<Config>, String> {
    let mut config = Config {
        transactions: DEFAULT_TRANSACTIONS,
        warmup: DEFAULT_WARMUP,
        repetitions: DEFAULT_REPETITIONS,
        mode: Mode::All,
    };
    let mut args = std::env::args().skip(1);
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--transactions" => {
                config.transactions = parse_usize("--transactions", args.next())?;
            }
            "--warmup" => {
                config.warmup = parse_usize("--warmup", args.next())?;
            }
            "--repetitions" => {
                config.repetitions = parse_usize("--repetitions", args.next())?;
            }
            "--mode" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--mode requires a value".to_owned())?;
                config.mode = Mode::parse(&value)?;
            }
            "--bench" => {}
            "--help" | "-h" => return Ok(None),
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    if config.transactions == 0 {
        return Err("--transactions must be greater than zero".to_owned());
    }
    if config.transactions > MAX_TRANSACTIONS {
        return Err(format!("--transactions must be at most {MAX_TRANSACTIONS}"));
    }
    if config.warmup > MAX_WARMUP {
        return Err(format!("--warmup must be at most {MAX_WARMUP}"));
    }
    if config.repetitions == 0 {
        return Err("--repetitions must be greater than zero".to_owned());
    }
    Ok(Some(config))
}

fn transaction(workload: Workload, sequence: usize) -> Transaction {
    let mut payload_hash = [0; 32];
    payload_hash[..16].copy_from_slice(&(sequence as u128).to_le_bytes());
    payload_hash[16] = match workload {
        Workload::SixtyFourAccounts => 1,
        Workload::HotAccount => 2,
    };
    Transaction::new(
        TxKey::new(workload.account_id(sequence), sequence as u128 + 1),
        1,
        payload_hash,
    )
}

fn run_phase(
    processor: &Arc<TransactionProcessor>,
    workload: Workload,
    first_sequence: usize,
    transactions: usize,
    batch_size: usize,
    submitters: usize,
    collect_latencies: bool,
) -> Result<PhaseResult, String> {
    if transactions == 0 {
        return Ok(PhaseResult {
            counts: Counts::default(),
            latencies: Vec::new(),
            wall_time: Duration::ZERO,
            process_cpu_time: Duration::ZERO,
        });
    }

    let submitter_count = submitters.max(1);
    let barrier = Arc::new(Barrier::new(submitter_count + 1));
    let base_count = transactions / submitter_count;
    let extra = transactions % submitter_count;
    let request_capacity = (0..submitter_count)
        .map(|worker| (base_count + if worker < extra { 1 } else { 0 }).div_ceil(batch_size))
        .sum();

    let (submitter_results, wall_time, process_cpu_time) = thread::scope(|scope| {
        let mut handles = Vec::with_capacity(submitter_count);
        let mut worker_start = first_sequence;
        for worker in 0..submitter_count {
            let count = base_count + if worker < extra { 1 } else { 0 };
            let start = worker_start;
            worker_start += count;
            let processor = Arc::clone(processor);
            let barrier = Arc::clone(&barrier);
            handles.push(scope.spawn(move || {
                let mut result = SubmitterResult {
                    counts: Counts::default(),
                    latencies: if collect_latencies {
                        Vec::with_capacity(count.div_ceil(batch_size))
                    } else {
                        Vec::new()
                    },
                };
                let end = start + count;
                let mut sequence = start;
                barrier.wait();
                while sequence < end {
                    let batch_end = sequence.saturating_add(batch_size).min(end);
                    let batch = (sequence..batch_end)
                        .map(|item| transaction(workload, item))
                        .collect::<Vec<_>>();
                    let call_start = collect_latencies.then(Instant::now);
                    let outcomes = processor.process_batch(&batch);
                    if let Some(call_start) = call_start {
                        result.latencies.push(call_start.elapsed());
                    }
                    result.counts.record_call(batch.len(), &outcomes);
                    sequence = batch_end;
                }
                result
            }));
        }

        let wall_start = Instant::now();
        let process_start = match ProcessTime::try_now() {
            Ok(process_start) => process_start,
            Err(error) => {
                barrier.wait();
                for handle in handles {
                    let _ = handle.join();
                }
                return Err(format!("could not read process CPU time: {error}"));
            }
        };
        barrier.wait();

        let mut results = Vec::with_capacity(handles.len());
        for handle in handles {
            results.push(
                handle
                    .join()
                    .map_err(|_| "benchmark submitter panicked".to_owned())?,
            );
        }
        let wall_time = wall_start.elapsed();
        let process_cpu_time = process_start
            .try_elapsed()
            .map_err(|error| format!("could not read elapsed process CPU time: {error}"))?;
        Ok((results, wall_time, process_cpu_time))
    })?;

    let mut result = PhaseResult {
        counts: Counts::default(),
        latencies: Vec::with_capacity(request_capacity),
        wall_time,
        process_cpu_time,
    };
    for submitter_result in submitter_results {
        result.counts.merge(&submitter_result.counts);
        result.latencies.extend(submitter_result.latencies);
    }
    Ok(result)
}

fn percentile(samples: &[Duration], percent: usize) -> Duration {
    let rank = (samples.len() * percent).div_ceil(100).max(1);
    samples[rank - 1]
}

fn measure_round(
    config: Config,
    workload: Workload,
    batch_size: usize,
    submitters: usize,
    available_parallelism: usize,
) -> Result<RoundResult, String> {
    let shards = (0..SHARD_COUNT)
        .map(|_| {
            ShardPorts::new(
                Arc::new(InMemoryWal::new()),
                Arc::new(InMemoryProjection::new()),
                Arc::new(InMemoryIndex::new()),
            )
        })
        .collect();
    let processor = Arc::new(
        TransactionProcessor::new(shards, WORKER_COUNT, MAX_QUEUED_ITEMS_PER_SHARD)
            .map_err(|error| format!("invalid processor configuration: {error}"))?,
    );

    let warmup = run_phase(
        &processor,
        workload,
        0,
        config.warmup,
        batch_size,
        submitters,
        false,
    )?;
    if warmup.counts.applied != config.warmup
        || warmup.counts.duplicate != 0
        || warmup.counts.rejected != 0
        || warmup.counts.errors() != 0
    {
        return Err(format!(
            "warmup did not apply every unique transaction: {:?}",
            warmup.counts
        ));
    }

    let measured = run_phase(
        &processor,
        workload,
        config.warmup,
        config.transactions,
        batch_size,
        submitters,
        true,
    )?;
    if measured.counts.attempted_transactions != config.transactions
        || measured.counts.requests != measured.latencies.len()
        || measured.latencies.is_empty()
    {
        return Err(format!(
            "measured work or latency samples are incomplete: tx={} expected={} requests={} latency_samples={}",
            measured.counts.attempted_transactions,
            config.transactions,
            measured.counts.requests,
            measured.latencies.len()
        ));
    }

    let mut latencies = measured.latencies;
    latencies.sort_unstable();
    let wall_seconds = measured.wall_time.as_secs_f64();
    let process_cpu_seconds = measured.process_cpu_time.as_secs_f64();
    let process_cpu_cores = process_cpu_seconds / wall_seconds;
    Ok(RoundResult {
        requests_per_second: measured.counts.requests as f64 / wall_seconds,
        applied_transactions_per_second: measured.counts.applied as f64 / wall_seconds,
        wall_seconds,
        process_cpu_cores,
        process_cpu_available_percent: process_cpu_cores / available_parallelism as f64 * 100.0,
        p50_request_latency_ms: percentile(&latencies, 50).as_secs_f64() * 1_000.0,
        p95_request_latency_ms: percentile(&latencies, 95).as_secs_f64() * 1_000.0,
        p99_request_latency_ms: percentile(&latencies, 99).as_secs_f64() * 1_000.0,
        latency_samples: latencies.len(),
        counts: measured.counts,
    })
}

fn median(mut values: Vec<f64>) -> f64 {
    values.sort_by(f64::total_cmp);
    let middle = values.len() / 2;
    if values.len() % 2 == 0 {
        (values[middle - 1] + values[middle]) / 2.0
    } else {
        values[middle]
    }
}

fn run_scenario(
    config: Config,
    workload: Workload,
    mode_name: &str,
    batch_size: usize,
    submitters: usize,
    available_parallelism: usize,
) -> Result<(), String> {
    let mut rounds = Vec::with_capacity(config.repetitions);
    for repetition in 1..=config.repetitions {
        let round = measure_round(
            config,
            workload,
            batch_size,
            submitters,
            available_parallelism,
        )?;
        println!(
            "round workload={} account_count={} mode={} batch_size={} submitters={} active_submitters={} repetition={} requests={} attempted_transactions={} applied={} duplicate={} rejected={} pending_errors={} missing_outcomes={} extra_outcomes={} errors={} wall_s={:.6} attempted_req_s={:.2} applied_tx_s={:.2} p50_req_ms={:.3} p95_req_ms={:.3} p99_req_ms={:.3} process_cpu_cores={:.4} process_cpu_available_pct={:.2} latency_samples={} subsecond={}",
            workload.name(),
            workload.account_count(),
            mode_name,
            batch_size,
            submitters,
            submitters.min(config.transactions),
            repetition,
            round.counts.requests,
            round.counts.attempted_transactions,
            round.counts.applied,
            round.counts.duplicate,
            round.counts.rejected,
            round.counts.pending_errors,
            round.counts.missing_outcomes,
            round.counts.extra_outcomes,
            round.counts.errors(),
            round.wall_seconds,
            round.requests_per_second,
            round.applied_transactions_per_second,
            round.p50_request_latency_ms,
            round.p95_request_latency_ms,
            round.p99_request_latency_ms,
            round.process_cpu_cores,
            round.process_cpu_available_percent,
            round.latency_samples,
            round.wall_seconds < 1.0,
        );
        rounds.push(round);
    }

    let mut total_counts = Counts::default();
    for round in &rounds {
        total_counts.merge(&round.counts);
    }
    let median_wall_seconds = median(rounds.iter().map(|round| round.wall_seconds).collect());
    let median_requests_per_second = median(
        rounds
            .iter()
            .map(|round| round.requests_per_second)
            .collect(),
    );
    let median_applied_transactions_per_second = median(
        rounds
            .iter()
            .map(|round| round.applied_transactions_per_second)
            .collect(),
    );
    let median_p50 = median(
        rounds
            .iter()
            .map(|round| round.p50_request_latency_ms)
            .collect(),
    );
    let median_p95 = median(
        rounds
            .iter()
            .map(|round| round.p95_request_latency_ms)
            .collect(),
    );
    let median_p99 = median(
        rounds
            .iter()
            .map(|round| round.p99_request_latency_ms)
            .collect(),
    );
    let median_process_cpu_cores =
        median(rounds.iter().map(|round| round.process_cpu_cores).collect());
    let median_process_cpu_available_percent = median(
        rounds
            .iter()
            .map(|round| round.process_cpu_available_percent)
            .collect(),
    );
    let subsecond_rounds = rounds
        .iter()
        .filter(|round| round.wall_seconds < 1.0)
        .count();
    println!(
        "summary workload={} account_count={} mode={} batch_size={} submitters={} repetitions={} requests_total={} attempted_transactions_total={} applied_total={} duplicate_total={} rejected_total={} pending_errors_total={} missing_outcomes_total={} extra_outcomes_total={} errors_total={} median_wall_s={:.6} median_attempted_req_s={:.2} median_applied_tx_s={:.2} median_p50_req_ms={:.3} median_p95_req_ms={:.3} median_p99_req_ms={:.3} median_process_cpu_cores={:.4} median_process_cpu_available_pct={:.2} latency_samples_per_round={} subsecond_rounds={} cpu_scope=process",
        workload.name(),
        workload.account_count(),
        mode_name,
        batch_size,
        submitters,
        config.repetitions,
        total_counts.requests,
        total_counts.attempted_transactions,
        total_counts.applied,
        total_counts.duplicate,
        total_counts.rejected,
        total_counts.pending_errors,
        total_counts.missing_outcomes,
        total_counts.extra_outcomes,
        total_counts.errors(),
        median_wall_seconds,
        median_requests_per_second,
        median_applied_transactions_per_second,
        median_p50,
        median_p95,
        median_p99,
        median_process_cpu_cores,
        median_process_cpu_available_percent,
        rounds[0].latency_samples,
        subsecond_rounds,
    );
    Ok(())
}

fn run(config: Config) -> Result<(), String> {
    let available_parallelism = std::thread::available_parallelism()
        .map(|value| value.get())
        .unwrap_or(1);
    let cpu_time_platform = if cfg!(unix) { "unix" } else { "windows" };
    println!(
        "in_memory_ceiling mode={:?} transactions={} warmup={} repetitions={} shard_count={SHARD_COUNT} worker_count={WORKER_COUNT} queue_capacity={MAX_QUEUED_ITEMS_PER_SHARD} available_parallelism={available_parallelism} cpu_time_platform={cpu_time_platform} cpu_scope=process",
        config.mode, config.transactions, config.warmup, config.repetitions
    );

    let workloads = [Workload::SixtyFourAccounts, Workload::HotAccount];
    if config.mode.includes_normal() {
        for workload in workloads {
            for submitters in NORMAL_SUBMITTERS {
                run_scenario(
                    config,
                    workload,
                    "single_request",
                    1,
                    submitters,
                    available_parallelism,
                )?;
            }
        }
    }
    if config.mode.includes_batch() {
        for workload in workloads {
            for batch_size in BATCH_SIZES {
                for submitters in BATCH_SUBMITTERS {
                    run_scenario(
                        config,
                        workload,
                        "client_batch",
                        batch_size,
                        submitters,
                        available_parallelism,
                    )?;
                }
            }
        }
    }
    Ok(())
}

fn main() {
    match parse_args() {
        Ok(Some(config)) => {
            if let Err(error) = run(config) {
                eprintln!("benchmark failed: {error}");
                std::process::exit(1);
            }
        }
        Ok(None) => println!(
            "Usage: cargo bench --bench in_memory_ceiling -- [--transactions N] [--warmup N] [--repetitions N] [--mode all|normal|batch]\n\
Defaults: --transactions {DEFAULT_TRANSACTIONS} --warmup {DEFAULT_WARMUP} --repetitions {DEFAULT_REPETITIONS} --mode all; limits: --transactions <= {MAX_TRANSACTIONS}, --warmup <= {MAX_WARMUP}."
        ),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2);
        }
    }
}
