use cpu_time::ProcessTime;
use std::collections::HashMap;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

const DEFAULT_ITERATIONS: u64 = 10_000_000;
const DEFAULT_REPETITIONS: usize = 1;
const DEFAULT_SAMPLE_STRIDE: u64 = 1_024;
const KEY_SPACE: u64 = 100_000;
const CHANNEL_CAPACITY: usize = 50_000;
const BATCH_SIZES: [usize; 2] = [2_048, 4_096];
const BATCH_TIMEOUTS_MS: [u64; 4] = [1, 5, 10, 20];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MapState {
    Empty,
    Prefilled,
}

impl MapState {
    fn name(self) -> &'static str {
        match self {
            Self::Empty => "empty",
            Self::Prefilled => "prefilled",
        }
    }
}

#[derive(Clone)]
struct Config {
    iterations: u64,
    repetitions: usize,
    sample_stride: u64,
    map_states: Vec<MapState>,
    batch_sizes: Vec<usize>,
    batch_timeouts_ms: Vec<u64>,
}

struct Request {
    id: u64,
    started_at: Instant,
}

struct Response {
    id: u64,
    count: u64,
    started_at: Instant,
    batch_ready_at: Instant,
}

struct DequeuedRequest {
    request: Request,
    dequeued_at: Instant,
    sample_selected: bool,
}

struct SamplePlan {
    selected_bits: Vec<u64>,
}

impl SamplePlan {
    fn new(iterations: u64, stride: u64) -> Result<Self, String> {
        let word_count = iterations / 64 + u64::from(iterations % 64 != 0);
        let word_count = usize::try_from(word_count)
            .map_err(|_| "latency sample plan is too large for this platform".to_owned())?;
        let mut selected_bits = Vec::new();
        selected_bits
            .try_reserve_exact(word_count)
            .map_err(|error| format!("could not allocate latency sample plan: {error}"))?;
        selected_bits.resize(word_count, 0_u64);

        for index in 0..iterations {
            if stride == 1 || splitmix64(index) % stride == 0 {
                selected_bits[(index / 64) as usize] |= 1_u64 << (index % 64);
            }
        }
        Ok(Self { selected_bits })
    }

    fn contains(&self, index: u64) -> bool {
        let word = self.selected_bits[(index / 64) as usize];
        word & (1_u64 << (index % 64)) != 0
    }
}

fn splitmix64(index: u64) -> u64 {
    let mut value = index.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[derive(Clone, Copy)]
struct LatencyStats {
    mean_ns: f64,
    p50_ns: f64,
    p95_ns: f64,
    p99_ns: f64,
    count: u64,
    sample_count: u64,
    total_nanos: u128,
}

struct LatencyAccumulator {
    total_nanos: u128,
    count: u64,
    samples: Vec<u64>,
}

impl LatencyAccumulator {
    fn new() -> Self {
        Self {
            total_nanos: 0,
            count: 0,
            samples: Vec::new(),
        }
    }

    fn record(&mut self, duration: Duration, selected: bool) {
        self.count += 1;
        let nanos = duration.as_nanos();
        self.total_nanos = self.total_nanos.saturating_add(nanos);
        if selected {
            self.samples.push(nanos.min(u64::MAX as u128) as u64);
        }
    }

    fn finish(self) -> LatencyStats {
        let mean_ns = if self.count == 0 {
            0.0
        } else {
            self.total_nanos as f64 / self.count as f64
        };
        let mut samples = self.samples;
        samples.sort_unstable();
        LatencyStats {
            mean_ns,
            p50_ns: percentile(&samples, 50),
            p95_ns: percentile(&samples, 95),
            p99_ns: percentile(&samples, 99),
            count: self.count,
            sample_count: samples.len() as u64,
            total_nanos: self.total_nanos,
        }
    }
}

#[derive(Clone, Copy)]
struct EnqueueMetrics {
    wall_seconds: f64,
    rps: f64,
    calls: u64,
    latency: LatencyStats,
}

#[derive(Clone, Copy)]
struct DequeueMetrics {
    observed_wall_seconds: f64,
    observed_rps: f64,
    active_rps: f64,
    calls: u64,
    latency: LatencyStats,
}

#[derive(Clone, Copy)]
struct BatchMetrics {
    active_seconds: f64,
    active_rps: f64,
    mean_ns_per_item: f64,
    batch_duration: LatencyStats,
    batch_count: u64,
    flush_full: u64,
    flush_timeout: u64,
    flush_tail: u64,
    average_batch_size: f64,
    maximum_batch_size: usize,
}

#[derive(Clone, Copy)]
struct PipelineMetrics {
    wall_seconds: f64,
    completed_rps: f64,
    latency: LatencyStats,
    completed: u64,
}

#[derive(Clone, Copy)]
struct LatencyBreakdown {
    submit_to_dequeue: LatencyStats,
    batch_wait: LatencyStats,
    batch_service: LatencyStats,
    response_delivery: LatencyStats,
}

#[derive(Clone, Copy)]
struct CpuMetrics {
    process_cpu_seconds: f64,
    measurement_wall_seconds: f64,
    core_equivalents: f64,
    one_core_percent: f64,
}

#[derive(Clone, Copy)]
struct RoundResult {
    enqueue: EnqueueMetrics,
    dequeue: DequeueMetrics,
    batch: BatchMetrics,
    pipeline: PipelineMetrics,
    latency_breakdown: LatencyBreakdown,
    cpu: CpuMetrics,
    final_map_len: usize,
}

struct ProducerReport {
    wall_time: Duration,
    calls: u64,
    latency: LatencyStats,
}

struct WorkerReport {
    dequeue_wall_time: Duration,
    recv_calls: u64,
    recv_latency: LatencyStats,
    submit_to_dequeue: LatencyStats,
    batch_wait: LatencyStats,
    batch_service: LatencyStats,
    active_time: Duration,
    batch_duration: LatencyStats,
    batch_count: u64,
    flush_full: u64,
    flush_timeout: u64,
    flush_tail: u64,
    total_batch_size: u64,
    maximum_batch_size: usize,
    final_map_len: usize,
}

struct WorkerCounters {
    active_time: Duration,
    batch_durations: LatencyAccumulator,
    batch_wait: LatencyAccumulator,
    batch_service: LatencyAccumulator,
    batch_count: u64,
    flush_full: u64,
    flush_timeout: u64,
    flush_tail: u64,
    total_batch_size: u64,
    maximum_batch_size: usize,
}

impl WorkerCounters {
    fn new() -> Self {
        Self {
            active_time: Duration::ZERO,
            batch_durations: LatencyAccumulator::new(),
            batch_wait: LatencyAccumulator::new(),
            batch_service: LatencyAccumulator::new(),
            batch_count: 0,
            flush_full: 0,
            flush_timeout: 0,
            flush_tail: 0,
            total_batch_size: 0,
            maximum_batch_size: 0,
        }
    }
}

fn parse_positive<T>(flag: &str, value: Option<String>) -> Result<T, String>
where
    T: std::str::FromStr,
{
    value
        .ok_or_else(|| format!("{flag} requires a value"))?
        .parse::<T>()
        .map_err(|_| format!("{flag} must be a positive integer"))
}

fn parse_args() -> Result<Option<Config>, String> {
    let mut config = Config {
        iterations: DEFAULT_ITERATIONS,
        repetitions: DEFAULT_REPETITIONS,
        sample_stride: DEFAULT_SAMPLE_STRIDE,
        map_states: vec![MapState::Empty, MapState::Prefilled],
        batch_sizes: BATCH_SIZES.to_vec(),
        batch_timeouts_ms: BATCH_TIMEOUTS_MS.to_vec(),
    };

    let mut args = std::env::args().skip(1);
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--iterations" => {
                config.iterations = parse_positive("--iterations", args.next())?;
            }
            "--repetitions" => {
                config.repetitions = parse_positive("--repetitions", args.next())?;
            }
            "--latency-sample-stride" => {
                config.sample_stride = parse_positive("--latency-sample-stride", args.next())?;
            }
            "--map-state" => {
                let value = args.next().ok_or("--map-state requires a value")?;
                config.map_states = match value.as_str() {
                    "all" => vec![MapState::Empty, MapState::Prefilled],
                    "empty" => vec![MapState::Empty],
                    "prefilled" => vec![MapState::Prefilled],
                    _ => return Err("--map-state must be all, empty, or prefilled".to_owned()),
                };
            }
            "--batch-size" => {
                let value = args.next().ok_or("--batch-size requires a value")?;
                config.batch_sizes = match value.as_str() {
                    "all" => BATCH_SIZES.to_vec(),
                    "2048" => vec![2_048],
                    "4096" => vec![4_096],
                    _ => return Err("--batch-size must be all, 2048, or 4096".to_owned()),
                };
            }
            "--batch-timeout-ms" => {
                let value = args.next().ok_or("--batch-timeout-ms requires a value")?;
                config.batch_timeouts_ms = match value.as_str() {
                    "all" => BATCH_TIMEOUTS_MS.to_vec(),
                    "1" => vec![1],
                    "5" => vec![5],
                    "10" => vec![10],
                    "20" => vec![20],
                    _ => {
                        return Err("--batch-timeout-ms must be all, 1, 5, 10, or 20".to_owned());
                    }
                };
            }
            "--bench" => {}
            "--help" | "-h" => return Ok(None),
            other => return Err(format!("unknown argument: {other}")),
        }
    }

    if config.iterations == 0 {
        return Err("--iterations must be greater than zero".to_owned());
    }
    if config.repetitions == 0 {
        return Err("--repetitions must be greater than zero".to_owned());
    }
    if config.sample_stride == 0 {
        return Err("--latency-sample-stride must be greater than zero".to_owned());
    }
    Ok(Some(config))
}

fn print_help() {
    println!(
        "Usage: bounded_queue_hashmap [--iterations N] [--repetitions N]\n\
         --map-state all|empty|prefilled --batch-size all|2048|4096\n\
         --batch-timeout-ms all|1|5|10|20 --latency-sample-stride N\n\
         Defaults: --iterations {DEFAULT_ITERATIONS}, --repetitions {DEFAULT_REPETITIONS},\n\
         all map states, batch sizes, and timeouts, sample stride {DEFAULT_SAMPLE_STRIDE}"
    );
}
fn percentile(sorted_samples: &[u64], percentile: u64) -> f64 {
    if sorted_samples.is_empty() {
        return 0.0;
    }
    let rank = (percentile * sorted_samples.len() as u64).div_ceil(100);
    sorted_samples[rank.saturating_sub(1) as usize] as f64
}

fn run_producer(
    sender: SyncSender<Request>,
    iterations: u64,
    sample_plan: Arc<SamplePlan>,
    barrier: Arc<Barrier>,
) -> Result<ProducerReport, String> {
    barrier.wait();
    let mut latency = LatencyAccumulator::new();
    let mut first_attempt = None;

    for id in 0..iterations {
        let started_at = Instant::now();
        first_attempt.get_or_insert(started_at);
        let request = Request { id, started_at };
        if sender.send(request).is_err() {
            return Err(format!("request channel closed before sending id {id}"));
        }
        latency.record(started_at.elapsed(), sample_plan.contains(id));
    }

    let wall_time = first_attempt
        .expect("iterations are validated as positive")
        .elapsed();
    Ok(ProducerReport {
        wall_time,
        calls: iterations,
        latency: latency.finish(),
    })
}

fn flush_batch(
    batch: &mut Vec<DequeuedRequest>,
    map: &mut HashMap<u64, u64>,
    response_sender: &SyncSender<Response>,
    counters: &mut WorkerCounters,
    flush_reason: FlushReason,
) -> Result<(), String> {
    if batch.is_empty() {
        return Ok(());
    }

    let batch_size = batch.len();
    let mut responses = Vec::with_capacity(batch_size);
    let batch_started_at = Instant::now();
    for dequeued in batch.iter() {
        let request = &dequeued.request;
        let key = request.id % KEY_SPACE;
        let count = map.entry(key).or_insert(0);
        *count += 1;
        responses.push(Response {
            id: request.id,
            count: *count,
            started_at: request.started_at,
            // The batch timestamp is stamped once, immediately after the active section.
            // This placeholder is overwritten below without taking per-item timestamps.
            batch_ready_at: dequeued.dequeued_at,
        });
    }
    let batch_ready_at = Instant::now();
    let active_time = batch_ready_at.duration_since(batch_started_at);
    for response in &mut responses {
        response.batch_ready_at = batch_ready_at;
    }
    for dequeued in batch.iter() {
        counters.batch_wait.record(
            batch_started_at.duration_since(dequeued.dequeued_at),
            dequeued.sample_selected,
        );
        counters
            .batch_service
            .record(active_time, dequeued.sample_selected);
    }
    counters.active_time += active_time;
    counters.batch_durations.record(active_time, true);
    counters.batch_count += 1;
    counters.total_batch_size += batch_size as u64;
    counters.maximum_batch_size = counters.maximum_batch_size.max(batch_size);
    match flush_reason {
        FlushReason::Full => counters.flush_full += 1,
        FlushReason::Timeout => counters.flush_timeout += 1,
        FlushReason::Tail => counters.flush_tail += 1,
    }

    for response in responses {
        response_sender
            .send(response)
            .map_err(|_| "response collector closed before all responses were sent".to_owned())?;
    }
    batch.clear();
    Ok(())
}

#[derive(Clone, Copy)]
enum FlushReason {
    Full,
    Timeout,
    Tail,
}

fn validate_map(
    map: &HashMap<u64, u64>,
    map_state: MapState,
    iterations: u64,
) -> Result<(), String> {
    let used_keys = iterations.min(KEY_SPACE);
    let expected_len = match map_state {
        MapState::Empty => used_keys,
        MapState::Prefilled => KEY_SPACE,
    } as usize;
    if map.len() != expected_len {
        return Err(format!(
            "final map length {} did not match expected {expected_len}",
            map.len()
        ));
    }

    for key in 0..KEY_SPACE {
        let expected = if key < iterations {
            (iterations - 1 - key) / KEY_SPACE + 1
        } else {
            0
        };
        match (map.get(&key), map_state) {
            (Some(actual), _) if *actual == expected => {}
            (None, MapState::Empty) if expected == 0 => {}
            (Some(actual), _) => {
                return Err(format!(
                    "final count for key {key} was {actual}, expected {expected}"
                ));
            }
            (None, MapState::Prefilled) => {
                return Err(format!("prefilled map is missing key {key}"));
            }
            (None, MapState::Empty) => {
                return Err(format!("empty map is missing used key {key}"));
            }
        }
    }
    Ok(())
}

fn run_worker(
    receiver: Receiver<Request>,
    response_sender: SyncSender<Response>,
    mut map: HashMap<u64, u64>,
    map_state: MapState,
    iterations: u64,
    batch_size: usize,
    batch_timeout: Duration,
    sample_plan: Arc<SamplePlan>,
    barrier: Arc<Barrier>,
) -> Result<WorkerReport, String> {
    barrier.wait();
    let mut batch = Vec::with_capacity(batch_size);
    let mut counters = WorkerCounters::new();
    let mut recv_latency = LatencyAccumulator::new();
    let mut submit_to_dequeue = LatencyAccumulator::new();
    let mut recv_calls = 0_u64;
    let mut first_recv_started = None;
    let mut last_recv_completed = None;
    let mut deadline = None;

    loop {
        if batch.is_empty() {
            let call_started = Instant::now();
            let received = receiver.recv();
            let dequeued_at = Instant::now();
            match received {
                Ok(request) => {
                    first_recv_started.get_or_insert(call_started);
                    let sample_selected = sample_plan.contains(request.id);
                    recv_latency.record(dequeued_at.duration_since(call_started), sample_selected);
                    submit_to_dequeue.record(
                        dequeued_at.duration_since(request.started_at),
                        sample_selected,
                    );
                    recv_calls += 1;
                    last_recv_completed = Some(dequeued_at);
                    batch.push(DequeuedRequest {
                        request,
                        dequeued_at,
                        sample_selected,
                    });
                    deadline = Some(Instant::now() + batch_timeout);
                }
                Err(_) => break,
            }
            continue;
        }

        let batch_deadline = deadline.expect("a nonempty batch has a deadline");
        let now = Instant::now();
        if now >= batch_deadline {
            flush_batch(
                &mut batch,
                &mut map,
                &response_sender,
                &mut counters,
                FlushReason::Timeout,
            )?;
            deadline = None;
            continue;
        }

        let call_started = Instant::now();
        let received = receiver.recv_timeout(batch_deadline - now);
        let dequeued_at = Instant::now();

        match received {
            Ok(request) => {
                first_recv_started.get_or_insert(call_started);
                let sample_selected = sample_plan.contains(request.id);
                recv_latency.record(dequeued_at.duration_since(call_started), sample_selected);
                submit_to_dequeue.record(
                    dequeued_at.duration_since(request.started_at),
                    sample_selected,
                );
                recv_calls += 1;
                last_recv_completed = Some(dequeued_at);
                batch.push(DequeuedRequest {
                    request,
                    dequeued_at,
                    sample_selected,
                });
                if batch.len() == batch_size {
                    flush_batch(
                        &mut batch,
                        &mut map,
                        &response_sender,
                        &mut counters,
                        FlushReason::Full,
                    )?;
                    deadline = None;
                } else if Instant::now() >= batch_deadline {
                    flush_batch(
                        &mut batch,
                        &mut map,
                        &response_sender,
                        &mut counters,
                        FlushReason::Timeout,
                    )?;
                    deadline = None;
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                flush_batch(
                    &mut batch,
                    &mut map,
                    &response_sender,
                    &mut counters,
                    FlushReason::Timeout,
                )?;
                deadline = None;
            }
            Err(RecvTimeoutError::Disconnected) => {
                flush_batch(
                    &mut batch,
                    &mut map,
                    &response_sender,
                    &mut counters,
                    FlushReason::Tail,
                )?;
                break;
            }
        }
    }

    drop(response_sender);
    if recv_calls != iterations {
        return Err(format!(
            "worker completed {recv_calls} successful recv calls for {iterations} requests"
        ));
    }
    let dequeue_wall_time = last_recv_completed
        .expect("positive iterations cause at least one recv")
        .duration_since(first_recv_started.expect("positive iterations cause a first recv"));
    validate_map(&map, map_state, iterations)?;

    Ok(WorkerReport {
        dequeue_wall_time,
        recv_calls,
        recv_latency: recv_latency.finish(),
        submit_to_dequeue: submit_to_dequeue.finish(),
        batch_wait: counters.batch_wait.finish(),
        batch_service: counters.batch_service.finish(),
        active_time: counters.active_time,
        batch_duration: counters.batch_durations.finish(),
        batch_count: counters.batch_count,
        flush_full: counters.flush_full,
        flush_timeout: counters.flush_timeout,
        flush_tail: counters.flush_tail,
        total_batch_size: counters.total_batch_size,
        maximum_batch_size: counters.maximum_batch_size,
        final_map_len: map.len(),
    })
}

fn run_round(
    iterations: u64,
    sample_plan: Arc<SamplePlan>,
    map_state: MapState,
    batch_size: usize,
    timeout_ms: u64,
) -> Result<RoundResult, String> {
    let mut map = HashMap::new();
    if map_state == MapState::Prefilled {
        for key in 0..KEY_SPACE {
            map.insert(key, 0);
        }
    }

    let (request_sender, request_receiver) = mpsc::sync_channel(CHANNEL_CAPACITY);
    let (response_sender, response_receiver) = mpsc::sync_channel(CHANNEL_CAPACITY);
    let barrier = Arc::new(Barrier::new(3));

    let worker_barrier = Arc::clone(&barrier);
    let worker_sample_plan = Arc::clone(&sample_plan);
    let worker_handle = thread::spawn(move || {
        run_worker(
            request_receiver,
            response_sender,
            map,
            map_state,
            iterations,
            batch_size,
            Duration::from_millis(timeout_ms),
            worker_sample_plan,
            worker_barrier,
        )
    });

    let producer_barrier = Arc::clone(&barrier);
    let producer_sample_plan = Arc::clone(&sample_plan);
    let producer_handle = thread::spawn(move || {
        run_producer(
            request_sender,
            iterations,
            producer_sample_plan,
            producer_barrier,
        )
    });

    barrier.wait();
    let cpu_wall_started = Instant::now();
    let process_cpu_started = ProcessTime::now();
    let mut pipeline_latency = LatencyAccumulator::new();
    let mut response_delivery = LatencyAccumulator::new();
    let mut completed = 0_u64;
    let mut first_attempt_at = None;
    let mut last_response_at = None;
    let mut first_validation_error = None;

    while completed < iterations {
        let response = response_receiver
            .recv()
            .map_err(|_| format!("response channel closed after {completed} of {iterations}"))?;
        let response_received_at = Instant::now();
        if response.id != completed {
            first_validation_error.get_or_insert_with(|| {
                format!(
                    "response id {} arrived at position {completed}",
                    response.id
                )
            });
        }
        let expected_count = response.id / KEY_SPACE + 1;
        if response.count != expected_count {
            first_validation_error.get_or_insert_with(|| {
                format!(
                    "response id {} had count {}, expected {expected_count}",
                    response.id, response.count
                )
            });
        }
        if response.id == 0 {
            first_attempt_at = Some(response.started_at);
        }
        pipeline_latency.record(
            response_received_at.duration_since(response.started_at),
            sample_plan.contains(response.id),
        );
        response_delivery.record(
            response_received_at.duration_since(response.batch_ready_at),
            sample_plan.contains(response.id),
        );
        last_response_at = Some(response_received_at);
        completed += 1;
    }

    let process_cpu_seconds = process_cpu_started.elapsed().as_secs_f64();
    let cpu_measurement_wall_seconds = cpu_wall_started.elapsed().as_secs_f64();
    let producer_report = producer_handle
        .join()
        .map_err(|_| "producer thread panicked".to_owned())??;
    let worker_report = worker_handle
        .join()
        .map_err(|_| "worker thread panicked".to_owned())??;
    if let Some(error) = first_validation_error {
        return Err(error);
    }

    let pipeline_wall_time = last_response_at
        .expect("positive iterations produce a response")
        .duration_since(first_attempt_at.expect("the first response has id zero"));
    let pipeline_wall_seconds = pipeline_wall_time.as_secs_f64();
    let enqueue_wall_seconds = producer_report.wall_time.as_secs_f64();
    let dequeue_wall_seconds = worker_report.dequeue_wall_time.as_secs_f64();
    let active_seconds = worker_report.active_time.as_secs_f64();
    let dequeue_latency = worker_report.recv_latency;
    let batch_duration = worker_report.batch_duration;
    let pipeline_latency = pipeline_latency.finish();
    let latency_breakdown = LatencyBreakdown {
        submit_to_dequeue: worker_report.submit_to_dequeue,
        batch_wait: worker_report.batch_wait,
        batch_service: worker_report.batch_service,
        response_delivery: response_delivery.finish(),
    };
    for (stage_name, stage) in [
        ("submit_to_dequeue", latency_breakdown.submit_to_dequeue),
        ("batch_wait", latency_breakdown.batch_wait),
        ("batch_service", latency_breakdown.batch_service),
        ("response_delivery", latency_breakdown.response_delivery),
    ] {
        if stage.count != iterations {
            return Err(format!(
                "{stage_name} recorded {} requests, expected {iterations}",
                stage.count
            ));
        }
    }
    let breakdown_total_nanos = latency_breakdown
        .submit_to_dequeue
        .total_nanos
        .checked_add(latency_breakdown.batch_wait.total_nanos)
        .and_then(|total| total.checked_add(latency_breakdown.batch_service.total_nanos))
        .and_then(|total| total.checked_add(latency_breakdown.response_delivery.total_nanos))
        .ok_or_else(|| "latency breakdown total nanoseconds overflowed u128".to_owned())?;
    if breakdown_total_nanos != pipeline_latency.total_nanos {
        return Err(format!(
            "latency breakdown total {breakdown_total_nanos} ns did not equal pipeline total {} ns",
            pipeline_latency.total_nanos
        ));
    }

    Ok(RoundResult {
        enqueue: EnqueueMetrics {
            wall_seconds: enqueue_wall_seconds,
            rps: iterations as f64 / enqueue_wall_seconds,
            calls: producer_report.calls,
            latency: producer_report.latency,
        },
        dequeue: DequeueMetrics {
            observed_wall_seconds: dequeue_wall_seconds,
            observed_rps: iterations as f64 / dequeue_wall_seconds,
            active_rps: iterations as f64 * 1_000_000_000.0 / dequeue_latency.total_nanos as f64,
            calls: worker_report.recv_calls,
            latency: dequeue_latency,
        },
        batch: BatchMetrics {
            active_seconds,
            active_rps: iterations as f64 / active_seconds,
            mean_ns_per_item: active_seconds * 1_000_000_000.0 / iterations as f64,
            batch_duration,
            batch_count: worker_report.batch_count,
            flush_full: worker_report.flush_full,
            flush_timeout: worker_report.flush_timeout,
            flush_tail: worker_report.flush_tail,
            average_batch_size: worker_report.total_batch_size as f64
                / worker_report.batch_count as f64,
            maximum_batch_size: worker_report.maximum_batch_size,
        },
        pipeline: PipelineMetrics {
            wall_seconds: pipeline_wall_seconds,
            completed_rps: iterations as f64 / pipeline_wall_seconds,
            latency: pipeline_latency,
            completed,
        },
        latency_breakdown,
        cpu: CpuMetrics {
            process_cpu_seconds,
            measurement_wall_seconds: cpu_measurement_wall_seconds,
            core_equivalents: process_cpu_seconds / cpu_measurement_wall_seconds,
            one_core_percent: 100.0 * process_cpu_seconds / cpu_measurement_wall_seconds,
        },
        final_map_len: worker_report.final_map_len,
    })
}

fn print_round(round_number: usize, result: RoundResult) {
    println!(
        "round={round_number} enqueue_wall_s={:.6} enqueue_rps={:.2} enqueue_calls={} enqueue_avg_ns={:.3} enqueue_p50_ns={:.3} enqueue_p95_ns={:.3} enqueue_p99_ns={:.3} enqueue_percentile_samples={}",
        result.enqueue.wall_seconds,
        result.enqueue.rps,
        result.enqueue.calls,
        result.enqueue.latency.mean_ns,
        result.enqueue.latency.p50_ns,
        result.enqueue.latency.p95_ns,
        result.enqueue.latency.p99_ns,
        result.enqueue.latency.sample_count,
    );
    println!(
        "dequeue_observed_span_s={:.6} dequeue_observed_rps={:.2} dequeue_active_rps={:.2} dequeue_successful_recv_calls={} dequeue_avg_call_ns={:.3} dequeue_p50_call_ns={:.3} dequeue_p95_call_ns={:.3} dequeue_p99_call_ns={:.3} dequeue_percentile_samples={}",
        result.dequeue.observed_wall_seconds,
        result.dequeue.observed_rps,
        result.dequeue.active_rps,
        result.dequeue.calls,
        result.dequeue.latency.mean_ns,
        result.dequeue.latency.p50_ns,
        result.dequeue.latency.p95_ns,
        result.dequeue.latency.p99_ns,
        result.dequeue.latency.sample_count,
    );
    println!(
        "batch_active_wall_s={:.6} batch_active_rps={:.2} map_update_avg_ns_per_item={:.3} batch_count={} batch_duration_p50_ns={:.3} batch_duration_p95_ns={:.3} batch_duration_p99_ns={:.3} batch_duration_samples={} flush_full={} flush_timeout={} flush_tail={} average_batch_size={:.2} maximum_batch_size={} final_map_len={}",
        result.batch.active_seconds,
        result.batch.active_rps,
        result.batch.mean_ns_per_item,
        result.batch.batch_count,
        result.batch.batch_duration.p50_ns,
        result.batch.batch_duration.p95_ns,
        result.batch.batch_duration.p99_ns,
        result.batch.batch_duration.sample_count,
        result.batch.flush_full,
        result.batch.flush_timeout,
        result.batch.flush_tail,
        result.batch.average_batch_size,
        result.batch.maximum_batch_size,
        result.final_map_len,
    );
    println!(
        "pipeline_wall_s={:.6} completed={} completed_rps={:.2} e2e_avg_latency_ns={:.3} e2e_p50_ns={:.3} e2e_p95_ns={:.3} e2e_p99_ns={:.3} e2e_percentile_samples={} process_cpu_s={:.6} cpu_measurement_wall_s={:.6} cpu_core_equivalents={:.4} one_core_cpu_percent={:.2}%",
        result.pipeline.wall_seconds,
        result.pipeline.completed,
        result.pipeline.completed_rps,
        result.pipeline.latency.mean_ns,
        result.pipeline.latency.p50_ns,
        result.pipeline.latency.p95_ns,
        result.pipeline.latency.p99_ns,
        result.pipeline.latency.sample_count,
        result.cpu.process_cpu_seconds,
        result.cpu.measurement_wall_seconds,
        result.cpu.core_equivalents,
        result.cpu.one_core_percent,
    );
    print_breakdown_stage(
        "submit_to_dequeue",
        result.latency_breakdown.submit_to_dequeue,
    );
    print_breakdown_stage("batch_wait", result.latency_breakdown.batch_wait);
    print_breakdown_stage("batch_service", result.latency_breakdown.batch_service);
    print_breakdown_stage(
        "response_delivery",
        result.latency_breakdown.response_delivery,
    );
    let breakdown_total_nanos = result.latency_breakdown.submit_to_dequeue.total_nanos
        + result.latency_breakdown.batch_wait.total_nanos
        + result.latency_breakdown.batch_service.total_nanos
        + result.latency_breakdown.response_delivery.total_nanos;
    println!(
        "latency_breakdown_total_ns={breakdown_total_nanos} pipeline_total_ns={} latency_breakdown_verified={}",
        result.pipeline.latency.total_nanos,
        breakdown_total_nanos == result.pipeline.latency.total_nanos,
    );
}

fn print_breakdown_stage(name: &str, stats: LatencyStats) {
    println!(
        "latency_{name}_count={} latency_{name}_avg_ns={:.3} latency_{name}_p50_ns={:.3} latency_{name}_p95_ns={:.3} latency_{name}_p99_ns={:.3} latency_{name}_sample_count={} latency_{name}_total_ns={}",
        stats.count,
        stats.mean_ns,
        stats.p50_ns,
        stats.p95_ns,
        stats.p99_ns,
        stats.sample_count,
        stats.total_nanos,
    );
}

fn median_by(rounds: &[RoundResult], value: impl Fn(&RoundResult) -> f64) -> f64 {
    let mut values: Vec<_> = rounds.iter().map(value).collect();
    values.sort_by(f64::total_cmp);
    let middle = values.len() / 2;
    if values.len() % 2 == 0 {
        (values[middle - 1] + values[middle]) / 2.0
    } else {
        values[middle]
    }
}

fn median_u128(values: impl Iterator<Item = u128>) -> u128 {
    let mut values: Vec<_> = values.collect();
    values.sort_unstable();
    let middle = values.len() / 2;
    if values.len() % 2 == 0 {
        values[middle - 1] + (values[middle] - values[middle - 1]) / 2
    } else {
        values[middle]
    }
}

fn print_median_breakdown_stage(
    name: &str,
    rounds: &[RoundResult],
    select: impl Fn(&RoundResult) -> LatencyStats,
) {
    let first_round = select(&rounds[0]);
    println!(
        "median_latency_{name}_count={} median_latency_{name}_avg_ns={:.3} median_latency_{name}_p50_ns={:.3} median_latency_{name}_p95_ns={:.3} median_latency_{name}_p99_ns={:.3} latency_{name}_sample_count={}",
        first_round.count,
        median_by(rounds, |round| select(round).mean_ns),
        median_by(rounds, |round| select(round).p50_ns),
        median_by(rounds, |round| select(round).p95_ns),
        median_by(rounds, |round| select(round).p99_ns),
        first_round.sample_count,
    );
}

fn print_median(rounds: &[RoundResult]) {
    println!(
        "median_of_rounds={} enqueue_wall_s={:.6} enqueue_rps={:.2} enqueue_avg_ns={:.3} enqueue_p50_ns={:.3} enqueue_p95_ns={:.3} enqueue_p99_ns={:.3}",
        rounds.len(),
        median_by(rounds, |round| round.enqueue.wall_seconds),
        median_by(rounds, |round| round.enqueue.rps),
        median_by(rounds, |round| round.enqueue.latency.mean_ns),
        median_by(rounds, |round| round.enqueue.latency.p50_ns),
        median_by(rounds, |round| round.enqueue.latency.p95_ns),
        median_by(rounds, |round| round.enqueue.latency.p99_ns),
    );
    println!(
        "median_dequeue_observed_span_s={:.6} dequeue_observed_rps={:.2} dequeue_active_rps={:.2} dequeue_avg_call_ns={:.3} dequeue_p50_call_ns={:.3} dequeue_p95_call_ns={:.3} dequeue_p99_call_ns={:.3}",
        median_by(rounds, |round| round.dequeue.observed_wall_seconds),
        median_by(rounds, |round| round.dequeue.observed_rps),
        median_by(rounds, |round| round.dequeue.active_rps),
        median_by(rounds, |round| round.dequeue.latency.mean_ns),
        median_by(rounds, |round| round.dequeue.latency.p50_ns),
        median_by(rounds, |round| round.dequeue.latency.p95_ns),
        median_by(rounds, |round| round.dequeue.latency.p99_ns),
    );
    println!(
        "median_batch_active_wall_s={:.6} batch_active_rps={:.2} map_update_avg_ns_per_item={:.3} batch_count={:.1} batch_duration_p50_ns={:.3} batch_duration_p95_ns={:.3} batch_duration_p99_ns={:.3} flush_full={:.1} flush_timeout={:.1} flush_tail={:.1} average_batch_size={:.2} maximum_batch_size={:.1}",
        median_by(rounds, |round| round.batch.active_seconds),
        median_by(rounds, |round| round.batch.active_rps),
        median_by(rounds, |round| round.batch.mean_ns_per_item),
        median_by(rounds, |round| round.batch.batch_count as f64),
        median_by(rounds, |round| round.batch.batch_duration.p50_ns),
        median_by(rounds, |round| round.batch.batch_duration.p95_ns),
        median_by(rounds, |round| round.batch.batch_duration.p99_ns),
        median_by(rounds, |round| round.batch.flush_full as f64),
        median_by(rounds, |round| round.batch.flush_timeout as f64),
        median_by(rounds, |round| round.batch.flush_tail as f64),
        median_by(rounds, |round| round.batch.average_batch_size),
        median_by(rounds, |round| round.batch.maximum_batch_size as f64),
    );
    println!(
        "median_pipeline_wall_s={:.6} completed_rps={:.2} e2e_avg_latency_ns={:.3} e2e_p50_ns={:.3} e2e_p95_ns={:.3} e2e_p99_ns={:.3} process_cpu_s={:.6} cpu_measurement_wall_s={:.6} cpu_core_equivalents={:.4} one_core_cpu_percent={:.2}%",
        median_by(rounds, |round| round.pipeline.wall_seconds),
        median_by(rounds, |round| round.pipeline.completed_rps),
        median_by(rounds, |round| round.pipeline.latency.mean_ns),
        median_by(rounds, |round| round.pipeline.latency.p50_ns),
        median_by(rounds, |round| round.pipeline.latency.p95_ns),
        median_by(rounds, |round| round.pipeline.latency.p99_ns),
        median_by(rounds, |round| round.cpu.process_cpu_seconds),
        median_by(rounds, |round| round.cpu.measurement_wall_seconds),
        median_by(rounds, |round| round.cpu.core_equivalents),
        median_by(rounds, |round| round.cpu.one_core_percent),
    );
    print_median_breakdown_stage("submit_to_dequeue", rounds, |round| {
        round.latency_breakdown.submit_to_dequeue
    });
    print_median_breakdown_stage("batch_wait", rounds, |round| {
        round.latency_breakdown.batch_wait
    });
    print_median_breakdown_stage("batch_service", rounds, |round| {
        round.latency_breakdown.batch_service
    });
    print_median_breakdown_stage("response_delivery", rounds, |round| {
        round.latency_breakdown.response_delivery
    });
    let median_breakdown_total_ns = median_u128(rounds.iter().map(|round| {
        round.latency_breakdown.submit_to_dequeue.total_nanos
            + round.latency_breakdown.batch_wait.total_nanos
            + round.latency_breakdown.batch_service.total_nanos
            + round.latency_breakdown.response_delivery.total_nanos
    }));
    let median_pipeline_total_ns = median_u128(
        rounds
            .iter()
            .map(|round| round.pipeline.latency.total_nanos),
    );
    println!(
        "median_latency_breakdown_total_ns={median_breakdown_total_ns} median_pipeline_total_ns={median_pipeline_total_ns} latency_breakdown_verified={}",
        median_breakdown_total_ns == median_pipeline_total_ns,
    );
}

fn run(config: Config) -> Result<(), String> {
    let sample_plan = Arc::new(SamplePlan::new(config.iterations, config.sample_stride)?);
    println!(
        "bounded_queue_hashmap iterations={} repetitions={} sample_stride={} queue_capacity={CHANNEL_CAPACITY} key_space={KEY_SPACE}",
        config.iterations, config.repetitions, config.sample_stride
    );

    for map_state in config.map_states.iter().copied() {
        for batch_size in config.batch_sizes.iter().copied() {
            for timeout_ms in config.batch_timeouts_ms.iter().copied() {
                println!(
                    "\nscenario map_state={} batch_size={} timeout_ms={}",
                    map_state.name(),
                    batch_size,
                    timeout_ms
                );
                let mut rounds = Vec::with_capacity(config.repetitions);
                for round_number in 1..=config.repetitions {
                    let result = run_round(
                        config.iterations,
                        Arc::clone(&sample_plan),
                        map_state,
                        batch_size,
                        timeout_ms,
                    )?;
                    print_round(round_number, result);
                    rounds.push(result);
                }
                print_median(&rounds);
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
        Ok(None) => print_help(),
        Err(error) => {
            eprintln!("argument error: {error}");
            print_help();
            std::process::exit(2);
        }
    }
}
