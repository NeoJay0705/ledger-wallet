use cpu_time::{ProcessTime, ThreadTime};
use futures_util::stream::{FuturesUnordered, StreamExt};
use ledger_wallet::transaction::ports::ShardPorts;
use ledger_wallet::transaction::processor::TransactionProcessor;
use ledger_wallet::transaction::{
    BatchPolicy, BatchStats, InMemoryIndex, InMemoryProjection, InMemoryWal, ItemOutcome,
    Transaction, TxKey,
};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Condvar, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};
use tokio::runtime::{Builder, Runtime};
use tokio::sync::{mpsc, oneshot};

const DEFAULT_TRANSACTIONS: usize = 20_000;
const DEFAULT_WARMUP: usize = 2_000;
const DEFAULT_REPETITIONS: usize = 3;
const DEFAULT_BATCH_SIZE: usize = 64;
const DEFAULT_BATCH_TIMEOUT_US: u64 = 1_000;
const DEFAULT_MAX_IN_FLIGHT: usize = 4_096;
const DEFAULT_PRODUCERS: usize = 8;
const DEFAULT_COLLECTORS: usize = 4;
const MAX_TRANSACTIONS: usize = 2_000_000;
const MAX_WARMUP: usize = 50_000;
const MAX_REPETITIONS: usize = 100;
const MAX_THREADS: usize = 256;
const MAX_IN_FLIGHT: usize = 2_000_000;
const SHARD_COUNT: usize = 1;
const WORKER_COUNT: usize = 1;

#[derive(Clone, Copy, Debug)]
struct Config {
    transactions: usize,
    warmup: usize,
    repetitions: usize,
    batch_size: usize,
    batch_timeout_us: u64,
    max_in_flight: usize,
    producers: usize,
    collectors: usize,
    workload: WorkloadChoice,
}

#[derive(Clone, Copy, Debug)]
enum WorkloadChoice {
    All,
    HotAccount,
    Accounts64,
}

#[derive(Clone, Copy, Debug)]
enum Workload {
    HotAccount,
    Accounts64,
}

impl Workload {
    fn name(self) -> &'static str {
        match self {
            Self::HotAccount => "hot_account",
            Self::Accounts64 => "accounts64",
        }
    }

    fn account_id(self, sequence: usize) -> u64 {
        match self {
            Self::HotAccount => 0,
            Self::Accounts64 => (sequence % 64) as u64,
        }
    }
}

impl WorkloadChoice {
    fn workloads(self) -> &'static [Workload] {
        const ALL: [Workload; 2] = [Workload::HotAccount, Workload::Accounts64];
        const HOT: [Workload; 1] = [Workload::HotAccount];
        const ACCOUNTS: [Workload; 1] = [Workload::Accounts64];
        match self {
            Self::All => &ALL,
            Self::HotAccount => &HOT,
            Self::Accounts64 => &ACCOUNTS,
        }
    }

    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "all" => Ok(Self::All),
            "hot" | "hot_account" => Ok(Self::HotAccount),
            "accounts64" | "sixty_four_accounts" => Ok(Self::Accounts64),
            _ => Err("--workload must be one of: all, hot, accounts64".to_owned()),
        }
    }
}

fn parse_usize(flag: &str, value: Option<String>) -> Result<usize, String> {
    value
        .ok_or_else(|| format!("{flag} requires a value"))?
        .parse::<usize>()
        .map_err(|_| format!("{flag} must be a non-negative integer"))
}

fn parse_u64(flag: &str, value: Option<String>) -> Result<u64, String> {
    value
        .ok_or_else(|| format!("{flag} requires a value"))?
        .parse::<u64>()
        .map_err(|_| format!("{flag} must be a non-negative integer"))
}

fn parse_args() -> Result<Option<Config>, String> {
    let mut config = Config {
        transactions: DEFAULT_TRANSACTIONS,
        warmup: DEFAULT_WARMUP,
        repetitions: DEFAULT_REPETITIONS,
        batch_size: DEFAULT_BATCH_SIZE,
        batch_timeout_us: DEFAULT_BATCH_TIMEOUT_US,
        max_in_flight: DEFAULT_MAX_IN_FLIGHT,
        producers: DEFAULT_PRODUCERS,
        collectors: DEFAULT_COLLECTORS,
        workload: WorkloadChoice::All,
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
            "--batch-size" => {
                config.batch_size = parse_usize("--batch-size", args.next())?;
            }
            "--batch-timeout-us" => {
                config.batch_timeout_us = parse_u64("--batch-timeout-us", args.next())?;
            }
            "--max-in-flight" => {
                config.max_in_flight = parse_usize("--max-in-flight", args.next())?;
            }
            "--producers" => {
                config.producers = parse_usize("--producers", args.next())?;
            }
            "--collectors" => {
                config.collectors = parse_usize("--collectors", args.next())?;
            }
            "--workload" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--workload requires a value".to_owned())?;
                config.workload = WorkloadChoice::parse(&value)?;
            }
            "--bench" => {}
            "--help" | "-h" => return Ok(None),
            other => return Err(format!("unknown argument: {other}")),
        }
    }

    if config.transactions == 0 || config.transactions > MAX_TRANSACTIONS {
        return Err(format!("--transactions must be in 1..={MAX_TRANSACTIONS}"));
    }
    if config.warmup > MAX_WARMUP {
        return Err(format!("--warmup must be at most {MAX_WARMUP}"));
    }
    if config.repetitions == 0 || config.repetitions > MAX_REPETITIONS {
        return Err(format!("--repetitions must be in 1..={MAX_REPETITIONS}"));
    }
    if config.batch_size == 0 || config.batch_size > MAX_IN_FLIGHT {
        return Err(format!("--batch-size must be in 1..={MAX_IN_FLIGHT}"));
    }
    if config.max_in_flight == 0 || config.max_in_flight > MAX_IN_FLIGHT {
        return Err(format!("--max-in-flight must be in 1..={MAX_IN_FLIGHT}"));
    }
    if config.producers == 0 || config.producers > MAX_THREADS {
        return Err(format!("--producers must be in 1..={MAX_THREADS}"));
    }
    if config.collectors == 0 || config.collectors > MAX_THREADS {
        return Err(format!("--collectors must be in 1..={MAX_THREADS}"));
    }
    Ok(Some(config))
}

fn transaction(workload: Workload, sequence: usize) -> Transaction {
    let mut payload_hash = [0; 32];
    payload_hash[..16].copy_from_slice(&(sequence as u128).to_le_bytes());
    payload_hash[16] = match workload {
        Workload::HotAccount => 1,
        Workload::Accounts64 => 2,
    };
    Transaction::new(
        TxKey::new(workload.account_id(sequence), sequence as u128 + 1),
        1,
        payload_hash,
    )
}

fn lock_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct InFlightPool {
    capacity: usize,
    active: Mutex<usize>,
    changed: Condvar,
}

impl InFlightPool {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            active: Mutex::new(0),
            changed: Condvar::new(),
        }
    }

    fn acquire(self: &Arc<Self>) -> (InFlightPermit, bool, Duration) {
        let started = Instant::now();
        let mut active = lock_recover(&self.active);
        let mut blocked = false;
        while *active >= self.capacity {
            blocked = true;
            active = self
                .changed
                .wait(active)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        *active += 1;
        drop(active);
        let waited = if blocked {
            started.elapsed()
        } else {
            Duration::ZERO
        };
        (InFlightPermit(Arc::clone(self)), blocked, waited)
    }

    fn release(&self) {
        let mut active = lock_recover(&self.active);
        *active = active.saturating_sub(1);
        self.changed.notify_all();
    }

    fn wait_empty(&self) {
        let mut active = lock_recover(&self.active);
        while *active != 0 {
            active = self
                .changed
                .wait(active)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }
}

struct InFlightPermit(Arc<InFlightPool>);

impl Drop for InFlightPermit {
    fn drop(&mut self) {
        self.0.release();
    }
}

#[derive(Default)]
struct PhaseMetrics {
    offered: AtomicU64,
    backpressure_wait_count: AtomicU64,
    backpressure_total_ns: AtomicU64,
    backpressure_max_ns: AtomicU64,
}

impl PhaseMetrics {
    fn record_backpressure(&self, waited: Duration) {
        let nanos = duration_nanos(waited);
        self.backpressure_wait_count.fetch_add(1, Ordering::Relaxed);
        self.backpressure_total_ns
            .fetch_add(nanos, Ordering::Relaxed);
        self.backpressure_max_ns.fetch_max(nanos, Ordering::Relaxed);
    }
}

#[derive(Default)]
struct CollectorMetrics {
    finished: u64,
    applied: u64,
    response_rejected: u64,
    duplicate: u64,
    pending: u64,
    cancelled: u64,
    response_latencies: Vec<Duration>,
    collect_latencies: bool,
}

impl CollectorMetrics {
    fn new(collect_latencies: bool, expected_latency_samples: usize) -> Self {
        Self {
            response_latencies: if collect_latencies {
                Vec::with_capacity(expected_latency_samples)
            } else {
                Vec::new()
            },
            collect_latencies,
            ..Self::default()
        }
    }

    fn record_completion(
        &mut self,
        outcome: Result<ItemOutcome, oneshot::error::RecvError>,
        latency: Duration,
        permit: InFlightPermit,
        expected: usize,
    ) -> Option<InFlightPermit> {
        self.finished += 1;
        if self.collect_latencies {
            self.response_latencies.push(latency);
        }
        match outcome {
            Ok(ItemOutcome::Applied(_)) => {
                self.applied += 1;
            }
            Ok(ItemOutcome::Rejected(_)) => {
                self.response_rejected += 1;
            }
            Ok(ItemOutcome::Duplicate { .. }) => {
                self.duplicate += 1;
            }
            Ok(ItemOutcome::Pending { .. }) => {
                self.pending += 1;
            }
            Err(_) => {
                self.cancelled += 1;
            }
        }

        if self.finished == expected as u64 {
            Some(permit)
        } else {
            drop(permit);
            None
        }
    }

    fn append(&mut self, mut other: Self) {
        self.finished += other.finished;
        self.applied += other.applied;
        self.response_rejected += other.response_rejected;
        self.duplicate += other.duplicate;
        self.pending += other.pending;
        self.cancelled += other.cancelled;
        self.response_latencies
            .append(&mut other.response_latencies);
    }
}

struct ResponseJob {
    receiver: oneshot::Receiver<ItemOutcome>,
    submitted_at: Instant,
    permit: InFlightPermit,
}

async fn await_response(
    job: ResponseJob,
) -> (
    Result<ItemOutcome, oneshot::error::RecvError>,
    Duration,
    InFlightPermit,
) {
    let outcome = job.receiver.await;
    let latency = job.submitted_at.elapsed();
    (outcome, latency, job.permit)
}

async fn response_loop(
    mut receiver: mpsc::Receiver<ResponseJob>,
    expected: usize,
    collect_latencies: bool,
    thread_cpu_start: Option<ThreadTime>,
) -> (CollectorMetrics, Option<Duration>) {
    let mut metrics = CollectorMetrics::new(collect_latencies, expected);
    if expected == 0 {
        let cpu_time = thread_cpu_start
            .as_ref()
            .and_then(|started| started.try_elapsed().ok());
        return (metrics, cpu_time);
    }

    let mut pending = FuturesUnordered::new();
    let mut receiver_closed = false;

    loop {
        enum ResponseEvent {
            Job(Option<ResponseJob>),
            Completed(
                Option<(
                    Result<ItemOutcome, oneshot::error::RecvError>,
                    Duration,
                    InFlightPermit,
                )>,
            ),
        }

        let event = if receiver_closed {
            ResponseEvent::Completed(pending.next().await)
        } else if pending.is_empty() {
            ResponseEvent::Job(receiver.recv().await)
        } else {
            tokio::select! {
                job = receiver.recv() => ResponseEvent::Job(job),
                completed = pending.next() => ResponseEvent::Completed(completed),
            }
        };

        match event {
            ResponseEvent::Job(Some(job)) => pending.push(await_response(job)),
            ResponseEvent::Job(None) => receiver_closed = true,
            ResponseEvent::Completed(Some((outcome, latency, permit))) => {
                if let Some(final_permit) =
                    metrics.record_completion(outcome, latency, permit, expected)
                {
                    let cpu_time = thread_cpu_start
                        .as_ref()
                        .and_then(|started| started.try_elapsed().ok());
                    drop(final_permit);
                    return (metrics, cpu_time);
                }
            }
            ResponseEvent::Completed(None) => {
                if receiver_closed {
                    break;
                }
            }
        }
    }

    let cpu_time = thread_cpu_start
        .as_ref()
        .and_then(|started| started.try_elapsed().ok());
    (metrics, cpu_time)
}

struct PhaseResult {
    offered: u64,
    finished: u64,
    applied: u64,
    response_rejected: u64,
    duplicate: u64,
    pending: u64,
    cancelled: u64,
    backpressure_wait_count: u64,
    backpressure_total: Duration,
    backpressure_max: Duration,
    response_latencies: Vec<Duration>,
    wall_time: Duration,
    process_cpu_time: Duration,
    producer_cpu_time: Duration,
    producer_cpu_threads: usize,
    collector_cpu_time: Duration,
    collector_cpu_threads: usize,
    stats: BatchStats,
}

fn queue_capacity(config: Config) -> usize {
    config.max_in_flight.max(config.batch_size)
}

fn collector_request_count(
    first_sequence: usize,
    transactions: usize,
    collectors: usize,
    collector_id: usize,
) -> usize {
    let first_offset = (collector_id + collectors - first_sequence % collectors) % collectors;
    if first_offset >= transactions {
        0
    } else {
        1 + (transactions - 1 - first_offset) / collectors
    }
}

fn build_processor(config: Config) -> Result<Arc<TransactionProcessor>, String> {
    let shards = vec![ShardPorts::new(
        Arc::new(InMemoryWal::new()),
        Arc::new(InMemoryProjection::new()),
        Arc::new(InMemoryIndex::new()),
    )];
    let policy = BatchPolicy {
        max_items: config.batch_size,
        max_wait: Duration::from_micros(config.batch_timeout_us),
    };
    TransactionProcessor::new_with_batch_policy(
        shards,
        WORKER_COUNT,
        queue_capacity(config),
        policy,
    )
    .map(Arc::new)
    .map_err(|error| format!("invalid processor configuration: {error}"))
}

fn run_phase(
    config: Config,
    processor: Arc<TransactionProcessor>,
    workload: Workload,
    first_sequence: usize,
    transactions: usize,
    collect_latencies: bool,
) -> Result<PhaseResult, String> {
    let metrics = Arc::new(PhaseMetrics::default());
    let pool = Arc::new(InFlightPool::new(config.max_in_flight));
    let stats_before = processor.batch_stats();

    let mut senders = Vec::with_capacity(config.collectors);
    let mut receivers = Vec::with_capacity(config.collectors);
    for _ in 0..config.collectors {
        let (sender, receiver) = mpsc::channel(config.max_in_flight.max(1));
        senders.push(sender);
        receivers.push(receiver);
    }
    let runtimes = (0..config.collectors)
        .map(|_| {
            Builder::new_current_thread()
                .build()
                .map_err(|error| format!("failed to build response runtime: {error}"))
        })
        .collect::<Result<Vec<Runtime>, _>>()?;

    let participant_count = config.producers + config.collectors + 1;
    let ready_barrier = Arc::new(Barrier::new(participant_count));
    let start_barrier = Arc::new(Barrier::new(participant_count));

    let (
        wall_time,
        process_cpu_result,
        producer_cpu_time,
        producer_cpu_threads,
        collector_metrics,
        collector_cpu_time,
        collector_cpu_threads,
        thread_failure,
    ) = thread::scope(|scope| {
        let mut collector_handles = Vec::with_capacity(config.collectors);
        for (collector_id, (receiver, runtime)) in receivers.into_iter().zip(runtimes).enumerate() {
            let expected = collector_request_count(
                first_sequence,
                transactions,
                config.collectors,
                collector_id,
            );
            let ready = Arc::clone(&ready_barrier);
            let start = Arc::clone(&start_barrier);
            collector_handles.push(scope.spawn(move || {
                ready.wait();
                start.wait();
                let thread_cpu_start = ThreadTime::try_now().ok();
                runtime.block_on(response_loop(
                    receiver,
                    expected,
                    collect_latencies,
                    thread_cpu_start,
                ))
            }));
        }

        let base_count = transactions / config.producers;
        let extra_count = transactions % config.producers;
        let mut next_sequence = first_sequence;
        let mut producer_handles = Vec::with_capacity(config.producers);
        for producer_id in 0..config.producers {
            let count = base_count + usize::from(producer_id < extra_count);
            let producer_start = next_sequence;
            next_sequence += count;
            let senders = senders.clone();
            let processor = Arc::clone(&processor);
            let pool = Arc::clone(&pool);
            let metrics = Arc::clone(&metrics);
            let ready = Arc::clone(&ready_barrier);
            let start = Arc::clone(&start_barrier);
            producer_handles.push(scope.spawn(move || -> Result<Option<Duration>, String> {
                ready.wait();
                start.wait();
                let thread_cpu_start = ThreadTime::try_now().ok();
                for sequence in producer_start..producer_start + count {
                    let (permit, was_blocked, waited) = pool.acquire();
                    if was_blocked {
                        metrics.record_backpressure(waited);
                    }

                    let transaction = transaction(workload, sequence);
                    metrics.offered.fetch_add(1, Ordering::Relaxed);
                    let submitted_at = Instant::now();
                    let receiver = processor.submit(transaction);
                    let job = ResponseJob {
                        receiver,
                        submitted_at,
                        permit,
                    };
                    let sender = &senders[sequence % senders.len()];
                    if let Err(error) = sender.blocking_send(job) {
                        drop(error.0);
                        return Err("response collector closed its handoff channel".to_owned());
                    }
                }
                let thread_cpu_time = thread_cpu_start
                    .as_ref()
                    .and_then(|started| started.try_elapsed().ok());
                Ok(thread_cpu_time)
            }));
        }

        ready_barrier.wait();
        let wall_start = Instant::now();
        let process_start = ProcessTime::try_now();
        start_barrier.wait();

        let mut failure = None;
        let mut producer_cpu_time = Duration::ZERO;
        let mut producer_cpu_threads = 0;
        for handle in producer_handles {
            match handle.join() {
                Ok(Ok(Some(cpu_time))) => {
                    producer_cpu_time += cpu_time;
                    producer_cpu_threads += 1;
                }
                Ok(Ok(None)) => {}
                Ok(Err(error)) => {
                    if failure.is_none() {
                        failure = Some(error);
                    }
                }
                Err(_) => {
                    if failure.is_none() {
                        failure = Some("producer thread panicked".to_owned());
                    }
                }
            }
        }

        pool.wait_empty();
        let wall_time = wall_start.elapsed();
        let process_cpu_result = process_start
            .map_err(|error| format!("could not read process CPU start time: {error}"))
            .and_then(|started| {
                started
                    .try_elapsed()
                    .map_err(|error| format!("could not read process CPU elapsed time: {error}"))
            });
        drop(senders);

        let mut collector_metrics = CollectorMetrics::new(collect_latencies, transactions);
        let mut collector_cpu_time = Duration::ZERO;
        let mut collector_cpu_threads = 0;
        for handle in collector_handles {
            match handle.join() {
                Ok((metrics, Some(cpu_time))) => {
                    collector_metrics.append(metrics);
                    collector_cpu_time += cpu_time;
                    collector_cpu_threads += 1;
                }
                Ok((metrics, None)) => collector_metrics.append(metrics),
                Err(_) => {
                    if failure.is_none() {
                        failure = Some("response collector thread panicked".to_owned());
                    }
                }
            }
        }

        (
            wall_time,
            process_cpu_result,
            producer_cpu_time,
            producer_cpu_threads,
            collector_metrics,
            collector_cpu_time,
            collector_cpu_threads,
            failure,
        )
    });

    if let Some(error) = thread_failure {
        return Err(error);
    }
    let process_cpu_time = process_cpu_result?;
    let stats = stats_delta(&processor.batch_stats(), &stats_before);

    Ok(PhaseResult {
        offered: metrics.offered.load(Ordering::Relaxed),
        finished: collector_metrics.finished,
        applied: collector_metrics.applied,
        response_rejected: collector_metrics.response_rejected,
        duplicate: collector_metrics.duplicate,
        pending: collector_metrics.pending,
        cancelled: collector_metrics.cancelled,
        backpressure_wait_count: metrics.backpressure_wait_count.load(Ordering::Relaxed),
        backpressure_total: Duration::from_nanos(
            metrics.backpressure_total_ns.load(Ordering::Relaxed),
        ),
        backpressure_max: Duration::from_nanos(metrics.backpressure_max_ns.load(Ordering::Relaxed)),
        response_latencies: collector_metrics.response_latencies,
        wall_time,
        process_cpu_time,
        producer_cpu_time,
        producer_cpu_threads,
        collector_cpu_time,
        collector_cpu_threads,
        stats,
    })
}

fn validate_phase(result: &PhaseResult, expected: usize) -> Result<(), String> {
    let offered = result.offered;
    let admitted = result.stats.accepted_single_requests;
    let rejected = result.stats.rejected_single_requests;
    if offered != expected as u64
        || admitted + rejected != offered
        || result.finished != offered
        || result.applied != expected as u64
        || result.response_rejected != rejected
        || result.duplicate != 0
        || result.pending != 0
        || result.cancelled != 0
    {
        return Err(format!(
            "benchmark requests were not all Applied: offered={offered} admitted={admitted} scheduler_rejected={rejected} finished={} applied={} response_rejected={} duplicate={} pending={} cancelled={}",
            result.finished,
            result.applied,
            result.response_rejected,
            result.duplicate,
            result.pending,
            result.cancelled,
        ));
    }
    if !result.response_latencies.is_empty() && result.response_latencies.len() != expected {
        return Err(format!(
            "latency sample count {} does not match expected transaction count {expected}",
            result.response_latencies.len()
        ));
    }
    Ok(())
}

fn stats_delta(after: &BatchStats, before: &BatchStats) -> BatchStats {
    let mut batch_size_distribution = BTreeMap::new();
    for (&size, &count) in &after.batch_size_distribution {
        let delta = count.saturating_sub(
            before
                .batch_size_distribution
                .get(&size)
                .copied()
                .unwrap_or(0),
        );
        if delta > 0 {
            batch_size_distribution.insert(size, delta);
        }
    }

    BatchStats {
        accepted_single_requests: after
            .accepted_single_requests
            .saturating_sub(before.accepted_single_requests),
        rejected_single_requests: after
            .rejected_single_requests
            .saturating_sub(before.rejected_single_requests),
        automatic_batch_count: after
            .automatic_batch_count
            .saturating_sub(before.automatic_batch_count),
        automatic_transaction_count: after
            .automatic_transaction_count
            .saturating_sub(before.automatic_transaction_count),
        full_flush_count: after
            .full_flush_count
            .saturating_sub(before.full_flush_count),
        timeout_flush_count: after
            .timeout_flush_count
            .saturating_sub(before.timeout_flush_count),
        boundary_flush_count: after
            .boundary_flush_count
            .saturating_sub(before.boundary_flush_count),
        batch_size_distribution,
        queue_wait_count: after
            .queue_wait_count
            .saturating_sub(before.queue_wait_count),
        queue_wait_total: after
            .queue_wait_total
            .saturating_sub(before.queue_wait_total),
        // The available max is cumulative for the entire repetition.
        queue_wait_max: after.queue_wait_max,
    }
}

fn duration_nanos(duration: Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

fn percentile(samples: &[Duration], percent: usize) -> Duration {
    let rank = (samples.len() * percent).div_ceil(100).max(1);
    samples[rank - 1]
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(f64::total_cmp);
    let middle = values.len() / 2;
    if values.len() % 2 == 0 {
        (values[middle - 1] + values[middle]) / 2.0
    } else {
        values[middle]
    }
}

fn histogram_text(histogram: &BTreeMap<usize, u64>) -> String {
    let values = histogram
        .iter()
        .map(|(size, count)| format!("{size}:{count}"))
        .collect::<Vec<_>>();
    if values.is_empty() {
        "none".to_owned()
    } else {
        values.join(",")
    }
}

fn run_scenario(config: Config, workload: Workload) -> Result<(), String> {
    let mut applied_rates = Vec::with_capacity(config.repetitions);
    let mut request_p50 = Vec::with_capacity(config.repetitions);
    let mut request_p95 = Vec::with_capacity(config.repetitions);
    let mut request_p99 = Vec::with_capacity(config.repetitions);

    for repetition in 1..=config.repetitions {
        let processor = build_processor(config)?;
        if config.warmup > 0 {
            let warmup = run_phase(
                config,
                Arc::clone(&processor),
                workload,
                0,
                config.warmup,
                false,
            )?;
            validate_phase(&warmup, config.warmup)?;
        }

        let result = run_phase(
            config,
            processor,
            workload,
            config.warmup,
            config.transactions,
            true,
        )?;
        validate_phase(&result, config.transactions)?;

        let mut latencies = result.response_latencies.clone();
        latencies.sort_unstable();
        let p50 = percentile(&latencies, 50).as_secs_f64() * 1_000.0;
        let p95 = percentile(&latencies, 95).as_secs_f64() * 1_000.0;
        let p99 = percentile(&latencies, 99).as_secs_f64() * 1_000.0;
        let wall_seconds = result.wall_time.as_secs_f64();
        let process_cpu_cores = result.process_cpu_time.as_secs_f64() / wall_seconds;
        let producer_cpu_cores = result.producer_cpu_time.as_secs_f64() / wall_seconds;
        let collector_cpu_cores = result.collector_cpu_time.as_secs_f64() / wall_seconds;
        let available_parallelism = thread::available_parallelism()
            .map(|count| count.get())
            .unwrap_or(1);
        let process_cpu_available_pct = process_cpu_cores / available_parallelism as f64 * 100.0;
        let applied_tx_per_s = result.applied as f64 / wall_seconds;

        println!(
            "round workload={} repetition={} shard_count={SHARD_COUNT} worker_count={WORKER_COUNT} producers={} collectors={} target_transactions={} offered={} admitted={} rejected={} finished={} applied={} response_rejected={} duplicate={} pending={} cancelled={} wall_s={:.6} applied_tx_s={:.2} response_p50_ms={:.3} response_p95_ms={:.3} response_p99_ms={:.3} latency_samples={} backpressure_waiters={} backpressure_total_ms={:.3} backpressure_max_ms={:.3} process_cpu_cores={:.4} process_cpu_available_pct={:.2} producer_cpu_cores={:.4} producer_cpu_threads_measured={} collector_cpu_cores={:.4} collector_cpu_threads_measured={} batch_count={} transaction_count={} batch_size_histogram={} flush_full={} flush_timeout={} flush_boundary={} scheduler_queue_wait_count={} scheduler_queue_wait_total_ms={:.3} scheduler_queue_wait_max_ms_since_processor_created={:.3}",
            workload.name(),
            repetition,
            config.producers,
            config.collectors,
            config.transactions,
            result.offered,
            result.stats.accepted_single_requests,
            result.stats.rejected_single_requests,
            result.finished,
            result.applied,
            result.response_rejected,
            result.duplicate,
            result.pending,
            result.cancelled,
            wall_seconds,
            applied_tx_per_s,
            p50,
            p95,
            p99,
            latencies.len(),
            result.backpressure_wait_count,
            result.backpressure_total.as_secs_f64() * 1_000.0,
            result.backpressure_max.as_secs_f64() * 1_000.0,
            process_cpu_cores,
            process_cpu_available_pct,
            producer_cpu_cores,
            result.producer_cpu_threads,
            collector_cpu_cores,
            result.collector_cpu_threads,
            result.stats.automatic_batch_count,
            result.stats.automatic_transaction_count,
            histogram_text(&result.stats.batch_size_distribution),
            result.stats.full_flush_count,
            result.stats.timeout_flush_count,
            result.stats.boundary_flush_count,
            result.stats.queue_wait_count,
            result.stats.queue_wait_total.as_secs_f64() * 1_000.0,
            result.stats.queue_wait_max.as_secs_f64() * 1_000.0,
        );

        if wall_seconds < 1.0 {
            println!(
                "round_warning workload={} repetition={} wall_under_one_second=true",
                workload.name(),
                repetition
            );
        }
        applied_rates.push(applied_tx_per_s);
        request_p50.push(p50);
        request_p95.push(p95);
        request_p99.push(p99);
    }

    println!(
        "summary workload={} repetitions={} median_applied_tx_s={:.2} median_response_p50_ms={:.3} median_response_p95_ms={:.3} median_response_p99_ms={:.3}",
        workload.name(),
        config.repetitions,
        median(&mut applied_rates),
        median(&mut request_p50),
        median(&mut request_p95),
        median(&mut request_p99),
    );
    Ok(())
}

fn run(config: Config) -> Result<(), String> {
    let available_parallelism = thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(1);
    println!(
        "auto_batch_ceiling workload={:?} transactions={} warmup={} repetitions={} batch_size={} batch_timeout_us={} max_in_flight={} shard_count={SHARD_COUNT} worker_count={WORKER_COUNT} producers={} collectors={} queue_capacity={} available_parallelism={}",
        config.workload,
        config.transactions,
        config.warmup,
        config.repetitions,
        config.batch_size,
        config.batch_timeout_us,
        config.max_in_flight,
        config.producers,
        config.collectors,
        queue_capacity(config),
        available_parallelism,
    );

    for &workload in config.workload.workloads() {
        run_scenario(config, workload)?;
    }
    Ok(())
}

fn usage() -> &'static str {
    "Usage: cargo bench --bench auto_batch_ceiling -- [--transactions N] [--warmup N] [--repetitions N] [--batch-size N] [--batch-timeout-us N] [--max-in-flight N] [--producers N] [--collectors N] [--workload all|hot|accounts64]"
}

fn main() {
    match parse_args() {
        Ok(Some(config)) => {
            if let Err(error) = run(config) {
                eprintln!("auto batch benchmark failed: {error}");
                std::process::exit(1);
            }
        }
        Ok(None) => println!(
            "{}\nDefaults: --transactions {DEFAULT_TRANSACTIONS} --warmup {DEFAULT_WARMUP} --repetitions {DEFAULT_REPETITIONS} --batch-size {DEFAULT_BATCH_SIZE} --batch-timeout-us {DEFAULT_BATCH_TIMEOUT_US} --max-in-flight {DEFAULT_MAX_IN_FLIGHT} --producers {DEFAULT_PRODUCERS} --collectors {DEFAULT_COLLECTORS} --workload all; limits: --transactions <= {MAX_TRANSACTIONS}, --warmup <= {MAX_WARMUP}, --repetitions <= {MAX_REPETITIONS}.",
            usage()
        ),
        Err(error) => {
            eprintln!("{error}\n{}", usage());
            std::process::exit(2);
        }
    }
}
