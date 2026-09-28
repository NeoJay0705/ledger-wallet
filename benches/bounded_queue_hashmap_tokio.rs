use cpu_time::ProcessTime;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::runtime::Builder;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinSet;
use tokio::time::timeout_at;

const DEFAULT_ITERATIONS: u64 = 10_000_000;
const DEFAULT_REPETITIONS: usize = 1;
const DEFAULT_SAMPLE_STRIDE: u64 = 1_024;
const DEFAULT_COROUTINES: [u64; 8] = [100_000, 50_000, 25_000, 12_500, 6_250, 3_125, 1_562, 781];
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

struct Config {
    iterations: u64,
    repetitions: usize,
    sample_stride: u64,
    coroutines: Vec<u64>,
    map_states: Vec<MapState>,
    batch_sizes: Vec<usize>,
    batch_timeouts_ms: Vec<u64>,
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

        for id in 0..iterations {
            if stride == 1 || splitmix64(id) % stride == 0 {
                selected_bits[(id / 64) as usize] |= 1_u64 << (id % 64);
            }
        }
        Ok(Self { selected_bits })
    }

    fn contains(&self, id: u64) -> bool {
        self.selected_bits[(id / 64) as usize] & (1_u64 << (id % 64)) != 0
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
        self.record_nanos(duration.as_nanos(), selected);
    }

    fn record_nanos(&mut self, nanos: u128, selected: bool) {
        self.count += 1;
        self.total_nanos += nanos;
        if selected {
            self.samples.push(nanos.min(u64::MAX as u128) as u64);
        }
    }

    fn merge(&mut self, mut other: Self) {
        self.total_nanos += other.total_nanos;
        self.count += other.count;
        self.samples.append(&mut other.samples);
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

fn percentile(sorted_samples: &[u64], percentile: u64) -> f64 {
    if sorted_samples.is_empty() {
        return 0.0;
    }
    let rank = (percentile * sorted_samples.len() as u64).div_ceil(100);
    sorted_samples[rank.saturating_sub(1) as usize] as f64
}

struct Request {
    id: u64,
    sample_selected: bool,
    response_sender: oneshot::Sender<Response>,
}

struct Response {
    id: u64,
    count: u64,
    t2: Instant,
    t3: Instant,
    t4: Instant,
}

struct DequeuedRequest {
    request: Request,
    t2: Instant,
}

struct StagedResponse {
    sender: oneshot::Sender<Response>,
    id: u64,
    count: u64,
    t2: Instant,
    t3: Instant,
}

struct ResponseVerifier {
    words_per_key: usize,
    seen: Vec<AtomicU64>,
}

impl ResponseVerifier {
    fn new(iterations: u64) -> Result<Self, String> {
        let max_count = iterations.div_ceil(KEY_SPACE);
        let words_per_key = usize::try_from(max_count.div_ceil(64))
            .map_err(|_| "response verifier is too large for this platform".to_owned())?;
        let words_per_key = words_per_key.max(1);
        let word_count = usize::try_from(KEY_SPACE)
            .ok()
            .and_then(|keys| keys.checked_mul(words_per_key))
            .ok_or_else(|| "response verifier is too large for this platform".to_owned())?;
        let mut seen = Vec::new();
        seen.try_reserve_exact(word_count)
            .map_err(|error| format!("could not allocate response verifier: {error}"))?;
        seen.extend((0..word_count).map(|_| AtomicU64::new(0)));
        Ok(Self {
            words_per_key,
            seen,
        })
    }

    fn record(&self, id: u64, count: u64, iterations: u64) -> Result<(), String> {
        let expected = expected_key_count(id % KEY_SPACE, iterations);
        if count == 0 || count > expected {
            return Err(format!(
                "response id {id} had count {count}, outside 1..={expected}"
            ));
        }
        let zero_based_count = count - 1;
        let word = usize::try_from(id % KEY_SPACE).expect("key space fits in usize")
            * self.words_per_key
            + (zero_based_count / 64) as usize;
        let mask = 1_u64 << (zero_based_count % 64);
        let previous = self.seen[word].fetch_or(mask, Ordering::Relaxed);
        if previous & mask != 0 {
            return Err(format!(
                "response count {count} was repeated for key {}",
                id % KEY_SPACE
            ));
        }
        Ok(())
    }

    fn validate(&self, iterations: u64) -> Result<(), String> {
        for key in 0..KEY_SPACE {
            let expected_count = expected_key_count(key, iterations);
            for word_offset in 0..self.words_per_key {
                let first_count = word_offset as u64 * 64;
                let remaining = expected_count.saturating_sub(first_count);
                let expected_bits = match remaining {
                    0 => 0,
                    1..=63 => (1_u64 << remaining) - 1,
                    _ => u64::MAX,
                };
                let index = key as usize * self.words_per_key + word_offset;
                let actual = self.seen[index].load(Ordering::Relaxed);
                if actual != expected_bits {
                    return Err(format!(
                        "response counts for key {key} did not form 1..={expected_count} at word {word_offset}"
                    ));
                }
            }
        }
        Ok(())
    }
}

fn expected_key_count(key: u64, iterations: u64) -> u64 {
    if key >= iterations {
        0
    } else {
        (iterations - 1 - key) / KEY_SPACE + 1
    }
}

#[derive(Clone, Copy)]
enum FlushReason {
    Full,
    Timeout,
    Tail,
}

struct WorkerCounters {
    active_nanos: u128,
    batch_duration: LatencyAccumulator,
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
            active_nanos: 0,
            batch_duration: LatencyAccumulator::new(),
            batch_count: 0,
            flush_full: 0,
            flush_timeout: 0,
            flush_tail: 0,
            total_batch_size: 0,
            maximum_batch_size: 0,
        }
    }

    fn record_batch(&mut self, size: usize, active: Duration, reason: FlushReason) {
        self.active_nanos += active.as_nanos();
        self.batch_duration.record(active, true);
        self.batch_count += 1;
        self.total_batch_size += size as u64;
        self.maximum_batch_size = self.maximum_batch_size.max(size);
        match reason {
            FlushReason::Full => self.flush_full += 1,
            FlushReason::Timeout => self.flush_timeout += 1,
            FlushReason::Tail => self.flush_tail += 1,
        }
    }
}

struct WorkerReport {
    dequeue_latency: LatencyAccumulator,
    dequeue_active_nanos: u128,
    first_dequeue_call_started: Option<Instant>,
    first_dequeued_at: Option<Instant>,
    last_dequeued_at: Option<Instant>,
    successful_dequeues: u64,
    batch_active_nanos: u128,
    batch_duration: LatencyAccumulator,
    batch_count: u64,
    flush_full: u64,
    flush_timeout: u64,
    flush_tail: u64,
    total_batch_size: u64,
    maximum_batch_size: usize,
    final_map: HashMap<u64, u64>,
}

enum TaskOutput {
    Client(ClientReport),
    Worker(WorkerReport),
}

struct ClientReport {
    enqueue_latency: LatencyAccumulator,
    completion_latency: LatencyAccumulator,
    post_dequeue_latency: LatencyAccumulator,
    stages: [LatencyAccumulator; 4],
    completed: u64,
    first_t0: Option<Instant>,
    last_t1: Option<Instant>,
    last_t6: Option<Instant>,
}

impl ClientReport {
    fn new() -> Self {
        Self {
            enqueue_latency: LatencyAccumulator::new(),
            completion_latency: LatencyAccumulator::new(),
            post_dequeue_latency: LatencyAccumulator::new(),
            stages: std::array::from_fn(|_| LatencyAccumulator::new()),
            completed: 0,
            first_t0: None,
            last_t1: None,
            last_t6: None,
        }
    }
}

struct CompletionMark {
    t6: Instant,
    process_cpu_seconds: f64,
    cpu_measured_at: Instant,
}

struct RoundResult {
    configured_coroutines: u64,
    effective_coroutines: u64,
    completed: u64,
    completed_rps: f64,
    completion_latency: LatencyStats,
    enqueue_rps: f64,
    enqueue_latency: LatencyStats,
    dequeue_observed_rps: f64,
    dequeue_active_rps: f64,
    dequeue_latency: LatencyStats,
    post_dequeue_rps: f64,
    post_dequeue_latency: LatencyStats,
    stages: [LatencyStats; 4],
    cpu_process_seconds: f64,
    cpu_wall_seconds: f64,
    cpu_core_equivalents: f64,
    cpu_one_core_percent: f64,
    batch_active_seconds: f64,
    batch_active_rps: f64,
    batch_ns_per_item: f64,
    batch_duration: LatencyStats,
    batch_count: u64,
    average_batch_size: f64,
    maximum_batch_size: usize,
    flush_full: u64,
    flush_timeout: u64,
    flush_tail: u64,
    final_map_len: usize,
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
        let expected = expected_key_count(key, iterations);
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

async fn wait_for_start(
    mut start: watch::Receiver<bool>,
    ready: mpsc::UnboundedSender<()>,
) -> Result<(), String> {
    ready
        .send(())
        .map_err(|_| "benchmark coordinator dropped the readiness channel".to_owned())?;
    loop {
        if *start.borrow() {
            return Ok(());
        }
        start
            .changed()
            .await
            .map_err(|_| "benchmark start gate was dropped".to_owned())?;
    }
}

async fn run_client(
    first_id: u64,
    coroutine_count: u64,
    iterations: u64,
    sender: mpsc::Sender<Request>,
    start: watch::Receiver<bool>,
    ready: mpsc::UnboundedSender<()>,
    sample_plan: Arc<SamplePlan>,
    verifier: Arc<ResponseVerifier>,
    cpu_started: Arc<Mutex<ProcessTime>>,
    latest_completion: Arc<Mutex<Option<CompletionMark>>>,
) -> Result<TaskOutput, String> {
    let mut report = ClientReport::new();
    wait_for_start(start, ready).await?;
    let mut id = first_id;

    while id < iterations {
        let sample_selected = sample_plan.contains(id);
        let (response_sender, response_receiver) = oneshot::channel();
        let t0 = Instant::now();
        report.first_t0.get_or_insert(t0);
        sender
            .send(Request {
                id,
                sample_selected,
                response_sender,
            })
            .await
            .map_err(|_| format!("request queue closed before sending id {id}"))?;
        let t1 = Instant::now();
        report
            .enqueue_latency
            .record(t1.duration_since(t0), sample_selected);
        report.last_t1 = Some(t1);

        let response = response_receiver
            .await
            .map_err(|_| format!("response channel closed before id {id} completed"))?;
        let t6 = Instant::now();
        if response.id != id {
            return Err(format!(
                "request id {id} received response id {}",
                response.id
            ));
        }
        verifier.record(id, response.count, iterations)?;
        report
            .completion_latency
            .record(t6.duration_since(t0), sample_selected);
        report
            .post_dequeue_latency
            .record(t6.duration_since(response.t2), sample_selected);
        report.stages[0].record(response.t2.duration_since(t0), sample_selected);
        report.stages[1].record(response.t3.duration_since(response.t2), sample_selected);
        report.stages[2].record(response.t4.duration_since(response.t3), sample_selected);
        report.stages[3].record(t6.duration_since(response.t4), sample_selected);
        report.completed += 1;
        report.last_t6 = Some(t6);
        id = id.saturating_add(coroutine_count);
    }

    if let Some(t6) = report.last_t6 {
        let mut latest = latest_completion
            .lock()
            .map_err(|_| "completion timestamp mutex was poisoned".to_owned())?;
        if latest.as_ref().is_none_or(|mark| t6 > mark.t6) {
            let process_cpu_seconds = cpu_started
                .lock()
                .map_err(|_| "process CPU timer mutex was poisoned".to_owned())?
                .elapsed()
                .as_secs_f64();
            *latest = Some(CompletionMark {
                t6,
                process_cpu_seconds,
                cpu_measured_at: Instant::now(),
            });
        }
    }
    Ok(TaskOutput::Client(report))
}

fn record_dequeue(
    request: Request,
    call_started: Instant,
    t2: Instant,
    batch: &mut Vec<DequeuedRequest>,
    dequeue_latency: &mut LatencyAccumulator,
    dequeue_active_nanos: &mut u128,
    first_dequeue_call_started: &mut Option<Instant>,
    first_dequeued_at: &mut Option<Instant>,
    last_dequeued_at: &mut Option<Instant>,
    successful_dequeues: &mut u64,
) {
    let call_duration = t2.duration_since(call_started);
    dequeue_latency.record(call_duration, request.sample_selected);
    *dequeue_active_nanos += call_duration.as_nanos();
    first_dequeue_call_started.get_or_insert(call_started);
    first_dequeued_at.get_or_insert(t2);
    *last_dequeued_at = Some(t2);
    *successful_dequeues += 1;
    batch.push(DequeuedRequest { request, t2 });
}

fn flush_batch(
    batch: &mut Vec<DequeuedRequest>,
    staged: &mut Vec<StagedResponse>,
    map: &mut HashMap<u64, u64>,
    counters: &mut WorkerCounters,
    reason: FlushReason,
) -> Result<(), String> {
    if batch.is_empty() {
        return Ok(());
    }

    let size = batch.len();
    staged.clear();
    let t3 = Instant::now();
    for dequeued in batch.drain(..) {
        let key = dequeued.request.id % KEY_SPACE;
        let count = map.entry(key).or_insert(0);
        *count += 1;
        staged.push(StagedResponse {
            sender: dequeued.request.response_sender,
            id: dequeued.request.id,
            count: *count,
            t2: dequeued.t2,
            t3,
        });
    }
    let t4 = Instant::now();
    let active = t4.duration_since(t3);
    counters.record_batch(size, active, reason);

    for staged_response in staged.drain(..) {
        if staged_response
            .sender
            .send(Response {
                id: staged_response.id,
                count: staged_response.count,
                t2: staged_response.t2,
                t3: staged_response.t3,
                t4,
            })
            .is_err()
        {
            return Err("request coroutine dropped its response receiver".to_owned());
        }
    }
    Ok(())
}

async fn run_worker(
    mut receiver: mpsc::Receiver<Request>,
    start: watch::Receiver<bool>,
    ready: mpsc::UnboundedSender<()>,
    mut map: HashMap<u64, u64>,
    batch_size: usize,
    batch_timeout: Duration,
) -> Result<TaskOutput, String> {
    let mut batch = Vec::with_capacity(batch_size);
    let mut staged = Vec::with_capacity(batch_size);
    let mut counters = WorkerCounters::new();
    let mut dequeue_latency = LatencyAccumulator::new();
    let mut dequeue_active_nanos = 0_u128;
    let mut first_dequeue_call_started = None;
    let mut first_dequeued_at = None;
    let mut last_dequeued_at = None;
    let mut successful_dequeues = 0_u64;
    let mut deadline = None;
    wait_for_start(start, ready).await?;

    loop {
        if batch.is_empty() {
            let call_started = Instant::now();
            let received = receiver.recv().await;
            let t2 = Instant::now();
            match received {
                Some(request) => {
                    record_dequeue(
                        request,
                        call_started,
                        t2,
                        &mut batch,
                        &mut dequeue_latency,
                        &mut dequeue_active_nanos,
                        &mut first_dequeue_call_started,
                        &mut first_dequeued_at,
                        &mut last_dequeued_at,
                        &mut successful_dequeues,
                    );
                    deadline = Some(t2 + batch_timeout);
                }
                None => break,
            }
            continue;
        }

        let batch_deadline = deadline.expect("nonempty batch has a timeout deadline");
        let call_started = Instant::now();
        let received = timeout_at(
            tokio::time::Instant::from_std(batch_deadline),
            receiver.recv(),
        )
        .await;
        let t2 = Instant::now();
        match received {
            Ok(Some(request)) => {
                record_dequeue(
                    request,
                    call_started,
                    t2,
                    &mut batch,
                    &mut dequeue_latency,
                    &mut dequeue_active_nanos,
                    &mut first_dequeue_call_started,
                    &mut first_dequeued_at,
                    &mut last_dequeued_at,
                    &mut successful_dequeues,
                );
                if batch.len() == batch_size {
                    flush_batch(
                        &mut batch,
                        &mut staged,
                        &mut map,
                        &mut counters,
                        FlushReason::Full,
                    )?;
                    deadline = None;
                } else if t2 >= batch_deadline {
                    flush_batch(
                        &mut batch,
                        &mut staged,
                        &mut map,
                        &mut counters,
                        FlushReason::Timeout,
                    )?;
                    deadline = None;
                }
            }
            Ok(None) => {
                flush_batch(
                    &mut batch,
                    &mut staged,
                    &mut map,
                    &mut counters,
                    FlushReason::Tail,
                )?;
                break;
            }
            Err(_) => {
                if receiver.is_closed() {
                    let try_call_started = Instant::now();
                    match receiver.try_recv() {
                        Ok(request) => {
                            let dequeued_at = Instant::now();
                            record_dequeue(
                                request,
                                try_call_started,
                                dequeued_at,
                                &mut batch,
                                &mut dequeue_latency,
                                &mut dequeue_active_nanos,
                                &mut first_dequeue_call_started,
                                &mut first_dequeued_at,
                                &mut last_dequeued_at,
                                &mut successful_dequeues,
                            );
                            if batch.len() == batch_size {
                                flush_batch(
                                    &mut batch,
                                    &mut staged,
                                    &mut map,
                                    &mut counters,
                                    FlushReason::Full,
                                )?;
                                deadline = None;
                            }
                        }
                        Err(
                            mpsc::error::TryRecvError::Empty
                            | mpsc::error::TryRecvError::Disconnected,
                        ) => {
                            flush_batch(
                                &mut batch,
                                &mut staged,
                                &mut map,
                                &mut counters,
                                FlushReason::Tail,
                            )?;
                            break;
                        }
                    }
                } else {
                    flush_batch(
                        &mut batch,
                        &mut staged,
                        &mut map,
                        &mut counters,
                        FlushReason::Timeout,
                    )?;
                    deadline = None;
                }
            }
        }
    }

    Ok(TaskOutput::Worker(WorkerReport {
        dequeue_latency,
        dequeue_active_nanos,
        first_dequeue_call_started,
        first_dequeued_at,
        last_dequeued_at,
        successful_dequeues,
        batch_active_nanos: counters.active_nanos,
        batch_duration: counters.batch_duration,
        batch_count: counters.batch_count,
        flush_full: counters.flush_full,
        flush_timeout: counters.flush_timeout,
        flush_tail: counters.flush_tail,
        total_batch_size: counters.total_batch_size,
        maximum_batch_size: counters.maximum_batch_size,
        final_map: map,
    }))
}

async fn run_round(
    iterations: u64,
    configured_coroutines: u64,
    sample_plan: Arc<SamplePlan>,
    map_state: MapState,
    batch_size: usize,
    timeout_ms: u64,
) -> Result<RoundResult, String> {
    let effective_coroutines = configured_coroutines.min(iterations);
    let mut map = HashMap::new();
    if map_state == MapState::Prefilled {
        map.reserve(KEY_SPACE as usize);
        for key in 0..KEY_SPACE {
            map.insert(key, 0);
        }
    }

    let verifier = Arc::new(ResponseVerifier::new(iterations)?);
    let (request_sender, request_receiver) = mpsc::channel(CHANNEL_CAPACITY);
    let (start_sender, start_receiver) = watch::channel(false);
    let (ready_sender, mut ready_receiver) = mpsc::unbounded_channel();
    let latest_completion = Arc::new(Mutex::new(None));
    let cpu_started = Arc::new(Mutex::new(ProcessTime::now()));
    let mut tasks = JoinSet::new();

    let worker_ready = ready_sender.clone();
    tasks.spawn(run_worker(
        request_receiver,
        start_receiver.clone(),
        worker_ready,
        map,
        batch_size,
        Duration::from_millis(timeout_ms),
    ));

    for coroutine_index in 0..effective_coroutines {
        tasks.spawn(run_client(
            coroutine_index,
            effective_coroutines,
            iterations,
            request_sender.clone(),
            start_receiver.clone(),
            ready_sender.clone(),
            Arc::clone(&sample_plan),
            Arc::clone(&verifier),
            Arc::clone(&cpu_started),
            Arc::clone(&latest_completion),
        ));
    }
    drop(request_sender);
    drop(ready_sender);

    for _ in 0..=effective_coroutines {
        if ready_receiver.recv().await.is_none() {
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
            return Err("a task exited before reaching the start gate".to_owned());
        }
    }

    *cpu_started
        .lock()
        .map_err(|_| "process CPU timer mutex was poisoned".to_owned())? = ProcessTime::now();
    let release_at = Instant::now();
    if start_sender.send(true).is_err() {
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        return Err("no benchmark tasks were waiting at the start gate".to_owned());
    }
    drop(start_sender);

    let mut clients = 0_u64;
    let mut client_reports = Vec::with_capacity(effective_coroutines as usize);
    let mut worker_report = None;
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok(Ok(TaskOutput::Client(report))) => {
                clients += 1;
                client_reports.push(report);
            }
            Ok(Ok(TaskOutput::Worker(report))) => worker_report = Some(report),
            Ok(Err(error)) => {
                tasks.abort_all();
                while tasks.join_next().await.is_some() {}
                return Err(error);
            }
            Err(error) => {
                tasks.abort_all();
                while tasks.join_next().await.is_some() {}
                return Err(format!("Tokio task failed: {error}"));
            }
        }
    }
    if clients != effective_coroutines {
        return Err(format!(
            "completed {clients} of {effective_coroutines} request coroutines"
        ));
    }
    let worker = worker_report.ok_or_else(|| "batch worker did not return a report".to_owned())?;
    if worker.successful_dequeues != iterations {
        return Err(format!(
            "worker completed {} successful receives for {iterations} requests",
            worker.successful_dequeues
        ));
    }

    let completion = latest_completion
        .lock()
        .map_err(|_| "completion timestamp mutex was poisoned".to_owned())?
        .take()
        .ok_or_else(|| "request coroutines did not record a completion time".to_owned())?;

    let mut enqueue_latency = LatencyAccumulator::new();
    let mut completion_latency = LatencyAccumulator::new();
    let mut post_dequeue_latency = LatencyAccumulator::new();
    let mut stages: [LatencyAccumulator; 4] = std::array::from_fn(|_| LatencyAccumulator::new());
    let mut completed = 0_u64;
    let mut first_t0 = None;
    let mut last_t1 = None;
    let mut last_t6 = None;
    for report in client_reports {
        completed += report.completed;
        enqueue_latency.merge(report.enqueue_latency);
        completion_latency.merge(report.completion_latency);
        post_dequeue_latency.merge(report.post_dequeue_latency);
        for (aggregate, task_stage) in stages.iter_mut().zip(report.stages) {
            aggregate.merge(task_stage);
        }
        if let Some(t0) = report.first_t0 {
            first_t0 = Some(first_t0.map_or(t0, |current: Instant| current.min(t0)));
        }
        if let Some(t1) = report.last_t1 {
            last_t1 = Some(last_t1.map_or(t1, |current: Instant| current.max(t1)));
        }
        if let Some(t6) = report.last_t6 {
            last_t6 = Some(last_t6.map_or(t6, |current: Instant| current.max(t6)));
        }
    }
    if completed != iterations {
        return Err(format!("completed {completed} of {iterations} responses"));
    }
    let first_t0 = first_t0.ok_or_else(|| "no request start timestamp was recorded".to_owned())?;
    let last_t1 =
        last_t1.ok_or_else(|| "no enqueue completion timestamp was recorded".to_owned())?;
    let last_t6 =
        last_t6.ok_or_else(|| "no response completion timestamp was recorded".to_owned())?;
    if last_t6 != completion.t6 {
        return Err(
            "last response completion timestamp did not match the completion gate".to_owned(),
        );
    }

    let completion_latency = completion_latency.finish();
    let enqueue_latency = enqueue_latency.finish();
    let post_dequeue_latency = post_dequeue_latency.finish();
    let stages = stages.map(LatencyAccumulator::finish);
    let dequeue_latency = worker.dequeue_latency.finish();
    let batch_duration = worker.batch_duration.finish();
    let request_latencies = [
        ("completion", completion_latency),
        ("enqueue call", enqueue_latency),
        ("dequeue call", dequeue_latency),
        ("post-dequeue", post_dequeue_latency),
    ];
    for (name, latency) in request_latencies {
        if latency.count != iterations {
            return Err(format!(
                "{name} latency recorded {} requests, expected {iterations}",
                latency.count
            ));
        }
    }
    let expected_sample_count = completion_latency.sample_count;
    for (name, latency) in request_latencies {
        if latency.sample_count != expected_sample_count {
            return Err(format!(
                "{name} latency had {} samples, expected {expected_sample_count}",
                latency.sample_count
            ));
        }
    }
    for (index, latency) in stages.iter().copied().enumerate() {
        if latency.count != iterations {
            return Err(format!(
                "stage {index} recorded {} requests, expected {iterations}",
                latency.count
            ));
        }
        if latency.sample_count != expected_sample_count {
            return Err(format!(
                "stage {index} had {} samples, expected {expected_sample_count}",
                latency.sample_count
            ));
        }
    }
    if worker.total_batch_size != iterations {
        return Err(format!(
            "worker batched {} requests, expected {iterations}",
            worker.total_batch_size
        ));
    }
    if worker.flush_full + worker.flush_timeout + worker.flush_tail != worker.batch_count {
        return Err("batch flush reason counts did not equal batch count".to_owned());
    }
    if batch_duration.count != worker.batch_count {
        return Err(format!(
            "batch duration recorded {} batches, expected {}",
            batch_duration.count, worker.batch_count
        ));
    }
    let pipeline_total = completion_latency.total_nanos;
    let stages_total: u128 = stages.iter().map(|stage| stage.total_nanos).sum();
    if pipeline_total != stages_total {
        return Err(format!(
            "stage totals {stages_total} ns did not equal pipeline total {pipeline_total} ns"
        ));
    }
    let post_dequeue_total: u128 = stages[1..].iter().map(|stage| stage.total_nanos).sum();
    if post_dequeue_latency.total_nanos != post_dequeue_total {
        return Err(format!(
            "post-dequeue total {} ns did not equal final three stages {post_dequeue_total} ns",
            post_dequeue_latency.total_nanos
        ));
    }
    verifier.validate(iterations)?;
    validate_map(&worker.final_map, map_state, iterations)?;

    let pipeline_wall = last_t6.duration_since(first_t0).as_secs_f64();
    let enqueue_wall = last_t1.duration_since(first_t0).as_secs_f64();
    let dequeue_observed_wall = worker
        .last_dequeued_at
        .expect("positive iterations have a dequeue")
        .duration_since(
            worker
                .first_dequeue_call_started
                .expect("positive iterations have a first receive call"),
        )
        .as_secs_f64();
    let dequeue_active_seconds = worker.dequeue_active_nanos as f64 / 1_000_000_000.0;
    let post_dequeue_wall = last_t6
        .duration_since(
            worker
                .first_dequeued_at
                .expect("positive iterations have a first dequeue"),
        )
        .as_secs_f64();
    let batch_active_seconds = worker.batch_active_nanos as f64 / 1_000_000_000.0;
    let cpu_wall_seconds = completion
        .cpu_measured_at
        .duration_since(release_at)
        .as_secs_f64();
    let cpu_core_equivalents = completion.process_cpu_seconds / cpu_wall_seconds;

    Ok(RoundResult {
        configured_coroutines,
        effective_coroutines,
        completed,
        completed_rps: completed as f64 / pipeline_wall,
        completion_latency,
        enqueue_rps: iterations as f64 / enqueue_wall,
        enqueue_latency,
        dequeue_observed_rps: iterations as f64 / dequeue_observed_wall,
        dequeue_active_rps: iterations as f64 / dequeue_active_seconds,
        dequeue_latency,
        post_dequeue_rps: iterations as f64 / post_dequeue_wall,
        post_dequeue_latency,
        stages,
        cpu_process_seconds: completion.process_cpu_seconds,
        cpu_wall_seconds,
        cpu_core_equivalents,
        cpu_one_core_percent: cpu_core_equivalents * 100.0,
        batch_active_seconds,
        batch_active_rps: iterations as f64 / batch_active_seconds,
        batch_ns_per_item: worker.batch_active_nanos as f64 / iterations as f64,
        batch_duration,
        batch_count: worker.batch_count,
        average_batch_size: if worker.batch_count == 0 {
            0.0
        } else {
            worker.total_batch_size as f64 / worker.batch_count as f64
        },
        maximum_batch_size: worker.maximum_batch_size,
        flush_full: worker.flush_full,
        flush_timeout: worker.flush_timeout,
        flush_tail: worker.flush_tail,
        final_map_len: worker.final_map.len(),
    })
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
        coroutines: DEFAULT_COROUTINES.to_vec(),
        map_states: vec![MapState::Empty, MapState::Prefilled],
        batch_sizes: BATCH_SIZES.to_vec(),
        batch_timeouts_ms: BATCH_TIMEOUTS_MS.to_vec(),
    };

    let mut args = std::env::args().skip(1);
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--iterations" => config.iterations = parse_positive("--iterations", args.next())?,
            "--repetitions" => {
                config.repetitions = parse_positive("--repetitions", args.next())?;
            }
            "--latency-sample-stride" => {
                config.sample_stride = parse_positive("--latency-sample-stride", args.next())?;
            }
            "--coroutines" => {
                let value = args.next().ok_or("--coroutines requires a value")?;
                config.coroutines = if value == "all" {
                    DEFAULT_COROUTINES.to_vec()
                } else {
                    value
                        .split(',')
                        .map(|item| {
                            if item.is_empty() {
                                return Err("--coroutines requires positive integers".to_owned());
                            }
                            let value = item.parse::<u64>().map_err(|_| {
                                "--coroutines must be all or a comma-separated list of positive integers".to_owned()
                            })?;
                            if value == 0 {
                                return Err("--coroutines values must be greater than zero".to_owned());
                            }
                            Ok(value)
                        })
                        .collect::<Result<Vec<_>, _>>()?
                };
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
    if config.coroutines.is_empty() || config.coroutines.contains(&0) {
        return Err("--coroutines values must be greater than zero".to_owned());
    }
    Ok(Some(config))
}

fn print_help() {
    println!(
        "Usage: bounded_queue_hashmap_tokio [OPTIONS]\n\
         --iterations N --repetitions N --coroutines all|N[,N...]\n\
         --map-state all|empty|prefilled --batch-size all|2048|4096\n\
         --batch-timeout-ms all|1|5|10|20 --latency-sample-stride N\n\
         Defaults: {DEFAULT_ITERATIONS} requests, {DEFAULT_REPETITIONS} repetition,\n\
         coroutine counts {DEFAULT_COROUTINES:?}, all 16 map/batch/timeout variants,\n\
         deterministic latency sample stride {DEFAULT_SAMPLE_STRIDE}. For a short matrix,\n\
         try --iterations 10000 --coroutines 8192 --repetitions 1."
    );
}

fn push_latency(fields: &mut Vec<String>, latency: LatencyStats) {
    fields.extend([
        latency.mean_ns.to_string(),
        latency.p50_ns.to_string(),
        latency.p95_ns.to_string(),
        latency.p99_ns.to_string(),
        latency.count.to_string(),
        latency.sample_count.to_string(),
        latency.total_nanos.to_string(),
    ]);
}

fn print_csv_header() {
    let mut fields = vec![
        "repetition",
        "configured_coroutines",
        "effective_coroutines",
        "map_state",
        "batch_size",
        "batch_timeout_ms",
        "completed",
        "completed_rps",
        "enqueue_rps",
        "dequeue_observed_rps",
        "dequeue_active_rps",
        "post_dequeue_rps",
        "cpu_process_seconds",
        "cpu_wall_seconds",
        "cpu_core_equivalents",
        "cpu_one_core_percent",
        "batch_active_seconds",
        "batch_active_rps",
        "batch_ns_per_item",
        "batch_count",
        "batch_average_size",
        "batch_maximum_size",
        "flush_full",
        "flush_timeout",
        "flush_tail",
        "final_map_len",
        "stage0_name",
        "stage1_name",
        "stage2_name",
        "stage3_name",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect::<Vec<_>>();
    for name in ["completion", "enqueue", "dequeue_call", "post_dequeue"] {
        for stat in [
            "mean_ns",
            "p50_ns",
            "p95_ns",
            "p99_ns",
            "count",
            "sample_count",
            "total_ns",
        ] {
            fields.push(format!("{name}_{stat}"));
        }
    }
    for index in 0..4 {
        for stat in [
            "mean_ns",
            "p50_ns",
            "p95_ns",
            "p99_ns",
            "count",
            "sample_count",
            "total_ns",
        ] {
            fields.push(format!("stage{index}_{stat}"));
        }
    }
    for stat in ["mean_ns", "p50_ns", "p95_ns", "p99_ns", "count"] {
        fields.push(format!("batch_duration_{stat}"));
    }
    println!("{}", fields.join(","));
}

fn print_row(
    repetition: usize,
    map_state: MapState,
    batch_size: usize,
    timeout_ms: u64,
    result: RoundResult,
) {
    let mut fields = vec![
        repetition.to_string(),
        result.configured_coroutines.to_string(),
        result.effective_coroutines.to_string(),
        map_state.name().to_owned(),
        batch_size.to_string(),
        timeout_ms.to_string(),
        result.completed.to_string(),
        result.completed_rps.to_string(),
        result.enqueue_rps.to_string(),
        result.dequeue_observed_rps.to_string(),
        result.dequeue_active_rps.to_string(),
        result.post_dequeue_rps.to_string(),
        result.cpu_process_seconds.to_string(),
        result.cpu_wall_seconds.to_string(),
        result.cpu_core_equivalents.to_string(),
        result.cpu_one_core_percent.to_string(),
        result.batch_active_seconds.to_string(),
        result.batch_active_rps.to_string(),
        result.batch_ns_per_item.to_string(),
        result.batch_count.to_string(),
        result.average_batch_size.to_string(),
        result.maximum_batch_size.to_string(),
        result.flush_full.to_string(),
        result.flush_timeout.to_string(),
        result.flush_tail.to_string(),
        result.final_map_len.to_string(),
        "submit_to_dequeue".to_owned(),
        "batch_wait".to_owned(),
        "batch_service".to_owned(),
        "response_delivery".to_owned(),
    ];
    for latency in [
        result.completion_latency,
        result.enqueue_latency,
        result.dequeue_latency,
        result.post_dequeue_latency,
    ] {
        push_latency(&mut fields, latency);
    }
    for latency in result.stages {
        push_latency(&mut fields, latency);
    }
    fields.extend([
        result.batch_duration.mean_ns.to_string(),
        result.batch_duration.p50_ns.to_string(),
        result.batch_duration.p95_ns.to_string(),
        result.batch_duration.p99_ns.to_string(),
        result.batch_duration.count.to_string(),
    ]);
    println!("{}", fields.join(","));
}

fn main() {
    let config = match parse_args() {
        Ok(Some(config)) => config,
        Ok(None) => {
            print_help();
            return;
        }
        Err(error) => {
            eprintln!("{error}");
            print_help();
            std::process::exit(2);
        }
    };

    let sample_plan = match SamplePlan::new(config.iterations, config.sample_stride) {
        Ok(plan) => Arc::new(plan),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2);
        }
    };
    let runtime = match Builder::new_multi_thread()
        .worker_threads(3)
        .enable_time()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("could not build Tokio runtime: {error}");
            std::process::exit(2);
        }
    };

    print_csv_header();
    for &coroutines in &config.coroutines {
        for &map_state in &config.map_states {
            for &batch_size in &config.batch_sizes {
                for &timeout_ms in &config.batch_timeouts_ms {
                    for repetition in 1..=config.repetitions {
                        let result = runtime.block_on(run_round(
                            config.iterations,
                            coroutines,
                            Arc::clone(&sample_plan),
                            map_state,
                            batch_size,
                            timeout_ms,
                        ));
                        match result {
                            Ok(result) => {
                                print_row(repetition, map_state, batch_size, timeout_ms, result)
                            }
                            Err(error) => {
                                eprintln!(
                                    "scenario failed for configured C={coroutines}, map={}, batch_size={batch_size}, timeout={timeout_ms}ms, repetition={repetition}: {error}",
                                    map_state.name()
                                );
                                std::process::exit(1);
                            }
                        }
                    }
                }
            }
        }
    }
}
