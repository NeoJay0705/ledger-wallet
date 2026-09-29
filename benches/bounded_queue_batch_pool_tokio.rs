use cpu_time::ProcessTime;
use std::collections::{HashMap, VecDeque};
use std::io::{self, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::runtime::Builder;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};
use tokio::task::JoinSet;
use tokio::time::sleep_until;

const DEFAULT_ITERATIONS: u64 = 10_000_000;
const DEFAULT_REPETITIONS: usize = 1;
const DEFAULT_SAMPLE_STRIDE: u64 = 1_024;
const DEFAULT_COROUTINES: [u64; 8] = [100_000, 50_000, 25_000, 12_500, 6_250, 3_125, 1_562, 781];
const KEY_SPACE: u64 = 100_000;
const MAX_ADMITTED: usize = 50_000;
const BATCH_SIZES: [usize; 2] = [2_048, 4_096];
const BATCH_TIMEOUTS_MS: [u64; 4] = [1, 5, 10, 20];
const RUNTIME_WORKERS: usize = 3;

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
        self.count += 1;
        self.total_nanos += duration.as_nanos();
        if selected {
            self.samples
                .push(duration.as_nanos().min(u64::MAX as u128) as u64);
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

struct ResponseVerifier {
    words_per_key: usize,
    seen: Vec<AtomicU64>,
}

impl ResponseVerifier {
    fn new(iterations: u64) -> Result<Self, String> {
        let max_count = iterations.div_ceil(KEY_SPACE);
        let words_per_key = usize::try_from(max_count.div_ceil(64))
            .map_err(|_| "response verifier is too large for this platform".to_owned())?
            .max(1);
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
        let word =
            (id % KEY_SPACE) as usize * self.words_per_key + (zero_based_count / 64) as usize;
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
                if self.seen[index].load(Ordering::Relaxed) != expected_bits {
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

#[derive(Clone, Copy, Debug)]
enum FlushReason {
    Full,
    Timeout,
    Tail,
}

struct Request {
    id: u64,
    t_admit: Instant,
    mapped_count: u64,
    response_sender: Option<oneshot::Sender<Response>>,
    _permit: OwnedSemaphorePermit,
}

struct Response {
    id: u64,
    count: u64,
    t_admit: Instant,
    t_seal: Instant,
    t_start: Instant,
    t_done: Instant,
}

struct Batch {
    sequence: u64,
    requests: Vec<Request>,
    t_seal: Instant,
    reason: FlushReason,
}

struct BatchState {
    active: Option<Vec<Request>>,
    active_first_admitted: Option<Instant>,
    ready: VecDeque<Batch>,
    free: VecDeque<Vec<Request>>,
    total_buffers: usize,
    preallocated_buffers: usize,
    pool_expansions: u64,
    peak_buffers: usize,
    buffer_allocating: bool,
    generation: u64,
    next_batch_sequence: u64,
    next_worker_sequence: u64,
    closed: bool,
    admitted_requests: u64,
    dispatched_requests: u64,
}

impl BatchState {
    fn seal_active(&mut self, reason: FlushReason, t_seal: Instant) {
        let Some(requests) = self.active.take() else {
            return;
        };
        if requests.is_empty() {
            self.active = Some(requests);
            return;
        }
        let sequence = self.next_batch_sequence;
        self.next_batch_sequence += 1;
        self.ready.push_back(Batch {
            sequence,
            requests,
            t_seal,
            reason,
        });
        self.active_first_admitted = None;
        self.generation += 1;
        self.active = self.free.pop_front();
    }
}

struct Shared {
    state: tokio::sync::Mutex<BatchState>,
    changed: Notify,
    admitted_outstanding: AtomicU64,
    peak_admitted_outstanding: AtomicU64,
    reserved_unadmitted: AtomicU64,
    peak_reserved_unadmitted: AtomicU64,
}

fn update_peak(target: &AtomicU64, value: u64) {
    target.fetch_max(value, Ordering::Relaxed);
}

fn requests_per_second(requests: u64, wall_seconds: f64, name: &str) -> Result<f64, String> {
    if !wall_seconds.is_finite() || wall_seconds <= 0.0 {
        return Err(format!(
            "{name} measurement window was not positive and finite"
        ));
    }
    let rate = requests as f64 / wall_seconds;
    if !rate.is_finite() {
        return Err(format!("{name} rate was not finite"));
    }
    Ok(rate)
}

async fn wait_for_start(
    mut start: watch::Receiver<bool>,
    ready: mpsc::UnboundedSender<()>,
) -> Result<(), String> {
    ready
        .send(())
        .map_err(|_| "benchmark coordinator dropped readiness channel".to_owned())?;
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

struct ClientReport {
    completion: LatencyAccumulator,
    capacity_wait: LatencyAccumulator,
    reserved_unadmitted: LatencyAccumulator,
    buffer_wait: LatencyAccumulator,
    stages: [LatencyAccumulator; 5],
    completed: u64,
    buffer_wait_events: u64,
    first_t0: Option<Instant>,
    last_t_admit: Option<Instant>,
    last_t6: Option<Instant>,
}

impl ClientReport {
    fn new() -> Self {
        Self {
            completion: LatencyAccumulator::new(),
            capacity_wait: LatencyAccumulator::new(),
            reserved_unadmitted: LatencyAccumulator::new(),
            buffer_wait: LatencyAccumulator::new(),
            stages: std::array::from_fn(|_| LatencyAccumulator::new()),
            completed: 0,
            buffer_wait_events: 0,
            first_t0: None,
            last_t_admit: None,
            last_t6: None,
        }
    }
}

struct CompletionMark {
    t6: Instant,
    process_cpu_seconds: f64,
    cpu_measured_at: Instant,
}

struct WorkerCounters {
    handler_active_nanos: u128,
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
            handler_active_nanos: 0,
            batch_duration: LatencyAccumulator::new(),
            batch_count: 0,
            flush_full: 0,
            flush_timeout: 0,
            flush_tail: 0,
            total_batch_size: 0,
            maximum_batch_size: 0,
        }
    }

    fn record_batch(&mut self, size: usize, elapsed: Duration, reason: FlushReason) {
        self.handler_active_nanos += elapsed.as_nanos();
        self.batch_duration.record(elapsed, true);
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
    handler_active_nanos: u128,
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
    Timer,
}

struct RoundResult {
    configured_coroutines: u64,
    effective_coroutines: u64,
    completed: u64,
    completed_rps: f64,
    admission_rps: f64,
    completion: LatencyStats,
    capacity_wait: LatencyStats,
    reserved_unadmitted: LatencyStats,
    buffer_wait: LatencyStats,
    stages: [LatencyStats; 5],
    cpu_process_seconds: f64,
    cpu_wall_seconds: f64,
    cpu_core_equivalents: f64,
    handler_active_seconds: f64,
    handler_active_rps: f64,
    handler_ns_per_item: f64,
    batch_duration: LatencyStats,
    batch_count: u64,
    average_batch_size: f64,
    maximum_batch_size: usize,
    flush_full: u64,
    flush_timeout: u64,
    flush_tail: u64,
    pool_expansions: u64,
    preallocated_buffers: usize,
    peak_buffers: usize,
    buffer_wait_events: u64,
    peak_admitted_outstanding: u64,
    peak_reserved_unadmitted: u64,
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
            (None, MapState::Empty) => return Err(format!("empty map is missing used key {key}")),
        }
    }
    Ok(())
}

async fn admit_request(
    shared: &Arc<Shared>,
    batch_size: usize,
    batch_timeout: Duration,
    request: Request,
) -> Result<(Instant, Duration, bool), String> {
    let mut unavailable_since = None;
    let mut buffer_wait = Duration::ZERO;
    let mut saw_buffer_wait = false;

    loop {
        let notified = shared.changed.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let mut state = shared.state.lock().await;
        if state.closed {
            return Err("batch pool closed before request admission".to_owned());
        }

        if let (Some(first), Some(active)) = (state.active_first_admitted, state.active.as_ref()) {
            if !active.is_empty() && Instant::now() >= first + batch_timeout {
                state.seal_active(FlushReason::Timeout, Instant::now());
                drop(state);
                shared.changed.notify_waiters();
                continue;
            }
        }

        if state.active.is_none() {
            if let Some(buffer) = state.free.pop_front() {
                state.active = Some(buffer);
            } else if !state.buffer_allocating && state.total_buffers < MAX_ADMITTED {
                state.total_buffers += 1;
                state.peak_buffers = state.peak_buffers.max(state.total_buffers);
                state.buffer_allocating = true;
                unavailable_since.get_or_insert_with(Instant::now);
                saw_buffer_wait = true;
                drop(state);

                let mut buffer = Vec::new();
                let allocation = buffer.try_reserve_exact(batch_size);
                let mut state = shared.state.lock().await;
                state.buffer_allocating = false;
                match allocation {
                    Ok(()) => {
                        state.pool_expansions += 1;
                        if state.closed {
                            state.total_buffers -= 1;
                            drop(state);
                            shared.changed.notify_waiters();
                            return Err("batch pool closed during buffer allocation".to_owned());
                        }
                        if state.active.is_none() {
                            state.active = Some(buffer);
                        } else {
                            state.free.push_back(buffer);
                        }
                        drop(state);
                        shared.changed.notify_waiters();
                        continue;
                    }
                    Err(error) => {
                        state.total_buffers -= 1;
                        drop(state);
                        shared.changed.notify_waiters();
                        return Err(format!("could not expand batch buffer pool: {error}"));
                    }
                }
            } else {
                unavailable_since.get_or_insert_with(Instant::now);
                saw_buffer_wait = true;
                drop(state);
                notified.await;
                continue;
            }
        }

        if let Some(started) = unavailable_since.take() {
            buffer_wait += started.elapsed();
        }

        let starts_new_batch = state.active_first_admitted.is_none();
        let active = state.active.as_mut().expect("active buffer was acquired");
        active.push(request);
        let t_admit = Instant::now();
        active
            .last_mut()
            .expect("request was just appended")
            .t_admit = t_admit;
        if starts_new_batch {
            state.active_first_admitted = Some(t_admit);
        }
        state.admitted_requests += 1;
        let outstanding = shared.admitted_outstanding.fetch_add(1, Ordering::Relaxed) + 1;
        update_peak(&shared.peak_admitted_outstanding, outstanding);
        let sealed_full = state
            .active
            .as_ref()
            .is_some_and(|batch| batch.len() == batch_size);
        if sealed_full {
            state.seal_active(FlushReason::Full, Instant::now());
        }
        drop(state);
        if starts_new_batch || sealed_full {
            shared.changed.notify_waiters();
        }
        return Ok((t_admit, buffer_wait, saw_buffer_wait));
    }
}

struct ReservedPermitGuard {
    shared: Arc<Shared>,
    active: bool,
}

impl ReservedPermitGuard {
    fn new(shared: Arc<Shared>) -> Self {
        let current = shared.reserved_unadmitted.fetch_add(1, Ordering::Relaxed) + 1;
        update_peak(&shared.peak_reserved_unadmitted, current);
        Self {
            shared,
            active: true,
        }
    }

    fn admitted(&mut self) {
        if self.active {
            self.shared
                .reserved_unadmitted
                .fetch_sub(1, Ordering::Relaxed);
            self.active = false;
        }
    }
}

impl Drop for ReservedPermitGuard {
    fn drop(&mut self) {
        if self.active {
            self.shared
                .reserved_unadmitted
                .fetch_sub(1, Ordering::Relaxed);
        }
    }
}

async fn run_client(
    first_id: u64,
    coroutine_count: u64,
    iterations: u64,
    semaphore: Arc<Semaphore>,
    shared: Arc<Shared>,
    start: watch::Receiver<bool>,
    ready: mpsc::UnboundedSender<()>,
    sample_plan: Arc<SamplePlan>,
    verifier: Arc<ResponseVerifier>,
    cpu_started: Arc<Mutex<ProcessTime>>,
    latest_completion: Arc<Mutex<Option<CompletionMark>>>,
    batch_size: usize,
    batch_timeout: Duration,
) -> Result<TaskOutput, String> {
    let mut report = ClientReport::new();
    wait_for_start(start, ready).await?;
    let mut id = first_id;

    while id < iterations {
        let sample_selected = sample_plan.contains(id);
        let (response_sender, response_receiver) = oneshot::channel();
        let t0 = Instant::now();
        report.first_t0.get_or_insert(t0);
        let permit = Arc::clone(&semaphore)
            .acquire_owned()
            .await
            .map_err(|_| "request capacity semaphore closed".to_owned())?;
        let t_capacity = Instant::now();
        report
            .capacity_wait
            .record(t_capacity.duration_since(t0), sample_selected);
        let reserved_started = t_capacity;
        let mut reserved = ReservedPermitGuard::new(Arc::clone(&shared));
        let request = Request {
            id,
            t_admit: t_capacity,
            mapped_count: 0,
            response_sender: Some(response_sender),
            _permit: permit,
        };
        let admission = admit_request(&shared, batch_size, batch_timeout, request).await;
        match admission {
            Ok((t_admit, buffer_wait, saw_buffer_wait)) => {
                reserved.admitted();
                report
                    .reserved_unadmitted
                    .record(t_admit.duration_since(reserved_started), sample_selected);
                report.buffer_wait.record(buffer_wait, sample_selected);
                report.buffer_wait_events += u64::from(saw_buffer_wait);
                report.last_t_admit = Some(t_admit);
            }
            Err(error) => return Err(error),
        }

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
            .completion
            .record(t6.duration_since(t0), sample_selected);
        report.stages[0].record(response.t_admit.duration_since(t0), sample_selected);
        report.stages[1].record(
            response.t_seal.duration_since(response.t_admit),
            sample_selected,
        );
        report.stages[2].record(
            response.t_start.duration_since(response.t_seal),
            sample_selected,
        );
        report.stages[3].record(
            response.t_done.duration_since(response.t_start),
            sample_selected,
        );
        report.stages[4].record(t6.duration_since(response.t_done), sample_selected);
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

async fn run_timer(
    shared: Arc<Shared>,
    start: watch::Receiver<bool>,
    ready: mpsc::UnboundedSender<()>,
    batch_timeout: Duration,
) -> Result<TaskOutput, String> {
    wait_for_start(start, ready).await?;
    loop {
        let notified = shared.changed.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        let state = shared.state.lock().await;
        if state.closed {
            return Ok(TaskOutput::Timer);
        }
        let deadline = state
            .active
            .as_ref()
            .filter(|batch| !batch.is_empty())
            .and_then(|_| {
                state
                    .active_first_admitted
                    .map(|first| (first + batch_timeout, state.generation))
            });
        drop(state);

        let Some((deadline, generation)) = deadline else {
            notified.await;
            continue;
        };

        tokio::select! {
            _ = sleep_until(tokio::time::Instant::from_std(deadline)) => {
                let mut state = shared.state.lock().await;
                if !state.closed
                    && state.generation == generation
                    && state.active.as_ref().is_some_and(|batch| !batch.is_empty())
                    && state.active_first_admitted.is_some_and(|first| first + batch_timeout <= Instant::now())
                {
                    state.seal_active(FlushReason::Timeout, Instant::now());
                    drop(state);
                    shared.changed.notify_waiters();
                }
            }
            _ = notified => {}
        }
    }
}

async fn next_batch(shared: &Arc<Shared>) -> Result<Option<Batch>, String> {
    loop {
        let notified = shared.changed.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        let mut state = shared.state.lock().await;
        if let Some(batch) = state.ready.pop_front() {
            let expected = state.next_worker_sequence;
            if batch.sequence != expected {
                return Err(format!(
                    "ready batch sequence {} arrived before expected {expected}",
                    batch.sequence
                ));
            }
            state.next_worker_sequence += 1;
            return Ok(Some(batch));
        }
        if state.closed {
            return Ok(None);
        }
        drop(state);
        notified.await;
    }
}

async fn run_worker(
    shared: Arc<Shared>,
    start: watch::Receiver<bool>,
    ready: mpsc::UnboundedSender<()>,
    mut map: HashMap<u64, u64>,
) -> Result<TaskOutput, String> {
    wait_for_start(start, ready).await?;
    let mut counters = WorkerCounters::new();
    loop {
        let Some(mut batch) = next_batch(&shared).await? else {
            break;
        };
        let t_start = Instant::now();
        for request in &mut batch.requests {
            let key = request.id % KEY_SPACE;
            let count = map.entry(key).or_insert(0);
            *count += 1;
            // The request's ID, key count, and timestamps are staged in its
            // own Vec slot; the batch-wide t_done is filled after this loop.
            request.mapped_count = *count;
        }
        let t_done = Instant::now();
        let size = batch.requests.len();
        counters.record_batch(size, t_done.duration_since(t_start), batch.reason);

        for mut request in batch.requests.drain(..) {
            let sender = request
                .response_sender
                .take()
                .expect("request response sender is present until dispatch");
            let response = Response {
                id: request.id,
                count: request.mapped_count,
                t_admit: request.t_admit,
                t_seal: batch.t_seal,
                t_start,
                t_done,
            };
            let send_result = sender.send(response);
            let previous = shared.admitted_outstanding.fetch_sub(1, Ordering::Relaxed);
            if previous == 0 {
                return Err("admitted request counter underflowed".to_owned());
            }
            drop(request);
            if send_result.is_err() {
                return Err("request coroutine dropped its response receiver".to_owned());
            }
        }
        {
            let mut state = shared.state.lock().await;
            state.dispatched_requests += size as u64;
            state.free.push_back(batch.requests);
        }
        shared.changed.notify_waiters();
    }

    Ok(TaskOutput::Worker(WorkerReport {
        handler_active_nanos: counters.handler_active_nanos,
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

async fn close_and_flush_tail(shared: &Arc<Shared>) {
    let mut state = shared.state.lock().await;
    if !state.closed {
        state.seal_active(FlushReason::Tail, Instant::now());
        state.closed = true;
    }
    drop(state);
    shared.changed.notify_waiters();
}

async fn cleanup_tasks<T: 'static>(tasks: &mut JoinSet<T>) {
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
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
    let batch_timeout = Duration::from_millis(timeout_ms);
    let mut map = HashMap::new();
    if map_state == MapState::Prefilled {
        map.reserve(KEY_SPACE as usize);
        for key in 0..KEY_SPACE {
            map.insert(key, 0);
        }
    }

    let preallocated_buffers = MAX_ADMITTED.div_ceil(batch_size) + 2;
    let mut buffers = Vec::new();
    buffers
        .try_reserve_exact(preallocated_buffers)
        .map_err(|error| format!("could not allocate batch buffer pool: {error}"))?;
    for _ in 0..preallocated_buffers {
        let mut buffer = Vec::new();
        buffer
            .try_reserve_exact(batch_size)
            .map_err(|error| format!("could not allocate batch buffer: {error}"))?;
        buffers.push(buffer);
    }
    let mut buffers = buffers.into_iter();
    let active = buffers.next().expect("buffer pool has an active buffer");
    let free = buffers.collect::<VecDeque<_>>();

    let verifier = Arc::new(ResponseVerifier::new(iterations)?);
    let shared = Arc::new(Shared {
        state: tokio::sync::Mutex::new(BatchState {
            active: Some(active),
            active_first_admitted: None,
            ready: VecDeque::new(),
            free,
            total_buffers: preallocated_buffers,
            preallocated_buffers,
            pool_expansions: 0,
            peak_buffers: preallocated_buffers,
            buffer_allocating: false,
            generation: 0,
            next_batch_sequence: 0,
            next_worker_sequence: 0,
            closed: false,
            admitted_requests: 0,
            dispatched_requests: 0,
        }),
        changed: Notify::new(),
        admitted_outstanding: AtomicU64::new(0),
        peak_admitted_outstanding: AtomicU64::new(0),
        reserved_unadmitted: AtomicU64::new(0),
        peak_reserved_unadmitted: AtomicU64::new(0),
    });
    let semaphore = Arc::new(Semaphore::new(MAX_ADMITTED));
    let (start_sender, start_receiver) = watch::channel(false);
    let (ready_sender, mut ready_receiver) = mpsc::unbounded_channel();
    let latest_completion = Arc::new(Mutex::new(None));
    let cpu_started = Arc::new(Mutex::new(ProcessTime::now()));
    let mut tasks = JoinSet::new();

    let worker_ready = ready_sender.clone();
    tasks.spawn(run_worker(
        Arc::clone(&shared),
        start_receiver.clone(),
        worker_ready,
        map,
    ));
    let timer_ready = ready_sender.clone();
    tasks.spawn(run_timer(
        Arc::clone(&shared),
        start_receiver.clone(),
        timer_ready,
        batch_timeout,
    ));
    for coroutine_index in 0..effective_coroutines {
        tasks.spawn(run_client(
            coroutine_index,
            effective_coroutines,
            iterations,
            Arc::clone(&semaphore),
            Arc::clone(&shared),
            start_receiver.clone(),
            ready_sender.clone(),
            Arc::clone(&sample_plan),
            Arc::clone(&verifier),
            Arc::clone(&cpu_started),
            Arc::clone(&latest_completion),
            batch_size,
            batch_timeout,
        ));
    }
    drop(ready_sender);

    let mut ready_count = 0_u64;
    while ready_count < effective_coroutines + 2 {
        tokio::select! {
            ready = ready_receiver.recv() => {
                if ready.is_none() {
                    cleanup_tasks(&mut tasks).await;
                    return Err("a task exited before reaching the start gate".to_owned());
                }
                ready_count += 1;
            }
            joined = tasks.join_next() => {
                cleanup_tasks(&mut tasks).await;
                return match joined {
                    Some(Ok(Ok(_))) => Err("a task exited before reaching the start gate".to_owned()),
                    Some(Ok(Err(error))) => Err(error),
                    Some(Err(error)) => Err(format!("Tokio task failed before start: {error}")),
                    None => Err("Tokio task set ended before reaching the start gate".to_owned()),
                };
            }
        }
    }

    match cpu_started.lock() {
        Ok(mut timer) => *timer = ProcessTime::now(),
        Err(_) => {
            cleanup_tasks(&mut tasks).await;
            return Err("process CPU timer mutex was poisoned".to_owned());
        }
    }
    let release_at = Instant::now();
    if start_sender.send(true).is_err() {
        cleanup_tasks(&mut tasks).await;
        return Err("no benchmark tasks were waiting at the start gate".to_owned());
    }
    drop(start_sender);

    let mut clients = 0_u64;
    let mut client_reports = Vec::with_capacity(effective_coroutines as usize);
    while clients < effective_coroutines {
        let Some(joined) = tasks.join_next().await else {
            cleanup_tasks(&mut tasks).await;
            return Err("Tokio task set ended before all clients completed".to_owned());
        };
        match joined {
            Ok(Ok(TaskOutput::Client(report))) => {
                clients += 1;
                client_reports.push(report);
            }
            Ok(Ok(TaskOutput::Worker(_))) => {
                cleanup_tasks(&mut tasks).await;
                return Err("batch worker stopped before normal shutdown".to_owned());
            }
            Ok(Ok(TaskOutput::Timer)) => {
                cleanup_tasks(&mut tasks).await;
                return Err("batch timeout task stopped before normal shutdown".to_owned());
            }
            Ok(Err(error)) => {
                cleanup_tasks(&mut tasks).await;
                return Err(error);
            }
            Err(error) => {
                cleanup_tasks(&mut tasks).await;
                return Err(format!("Tokio task failed: {error}"));
            }
        }
    }

    close_and_flush_tail(&shared).await;
    let mut worker_report = None;
    let mut timer_completed = false;
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok(Ok(TaskOutput::Worker(report))) => worker_report = Some(report),
            Ok(Ok(TaskOutput::Timer)) => timer_completed = true,
            Ok(Ok(TaskOutput::Client(_))) => {
                cleanup_tasks(&mut tasks).await;
                return Err("unexpected extra request coroutine completed".to_owned());
            }
            Ok(Err(error)) => {
                cleanup_tasks(&mut tasks).await;
                return Err(error);
            }
            Err(error) => {
                cleanup_tasks(&mut tasks).await;
                return Err(format!("Tokio task failed during shutdown: {error}"));
            }
        }
    }
    if !timer_completed {
        return Err("batch timeout task did not finish during shutdown".to_owned());
    }
    let worker = worker_report.ok_or_else(|| "batch worker did not return a report".to_owned())?;

    let completion_mark = latest_completion
        .lock()
        .map_err(|_| "completion timestamp mutex was poisoned".to_owned())?
        .take()
        .ok_or_else(|| "request coroutines did not record a completion time".to_owned())?;
    let mut completion = LatencyAccumulator::new();
    let mut capacity_wait = LatencyAccumulator::new();
    let mut reserved_unadmitted = LatencyAccumulator::new();
    let mut buffer_wait = LatencyAccumulator::new();
    let mut stages: [LatencyAccumulator; 5] = std::array::from_fn(|_| LatencyAccumulator::new());
    let mut completed = 0_u64;
    let mut buffer_wait_events = 0_u64;
    let mut first_t0 = None;
    let mut last_t_admit = None;
    let mut last_t6 = None;
    for report in client_reports {
        completed += report.completed;
        completion.merge(report.completion);
        capacity_wait.merge(report.capacity_wait);
        reserved_unadmitted.merge(report.reserved_unadmitted);
        buffer_wait.merge(report.buffer_wait);
        for (aggregate, task_stage) in stages.iter_mut().zip(report.stages) {
            aggregate.merge(task_stage);
        }
        buffer_wait_events += report.buffer_wait_events;
        if let Some(t0) = report.first_t0 {
            first_t0 = Some(first_t0.map_or(t0, |current: Instant| current.min(t0)));
        }
        if let Some(t_admit) = report.last_t_admit {
            last_t_admit =
                Some(last_t_admit.map_or(t_admit, |current: Instant| current.max(t_admit)));
        }
        if let Some(t6) = report.last_t6 {
            last_t6 = Some(last_t6.map_or(t6, |current: Instant| current.max(t6)));
        }
    }
    if completed != iterations {
        return Err(format!("completed {completed} of {iterations} responses"));
    }
    let first_t0 = first_t0.ok_or_else(|| "no request start timestamp was recorded".to_owned())?;
    let last_t_admit =
        last_t_admit.ok_or_else(|| "no admission timestamp was recorded".to_owned())?;
    let last_t6 =
        last_t6.ok_or_else(|| "no response completion timestamp was recorded".to_owned())?;
    if completion_mark.t6 != last_t6 {
        return Err("last response timestamp did not match completion gate".to_owned());
    }

    let completion = completion.finish();
    let capacity_wait = capacity_wait.finish();
    let reserved_unadmitted = reserved_unadmitted.finish();
    let buffer_wait = buffer_wait.finish();
    let stages = stages.map(LatencyAccumulator::finish);
    let batch_duration = worker.batch_duration.finish();
    let latency_metrics = [
        ("completion", completion),
        ("capacity_acquire_wait", capacity_wait),
        ("reserved_unadmitted_wait", reserved_unadmitted),
        ("buffer_availability_wait", buffer_wait),
    ];
    for (name, latency) in latency_metrics {
        if latency.count != iterations {
            return Err(format!(
                "{name} recorded {} requests, expected {iterations}",
                latency.count
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
        if latency.sample_count != completion.sample_count {
            return Err(format!(
                "stage {index} had {} samples, expected {}",
                latency.sample_count, completion.sample_count
            ));
        }
    }
    for (name, latency) in [
        ("capacity_acquire_wait", capacity_wait),
        ("reserved_unadmitted_wait", reserved_unadmitted),
        ("buffer_availability_wait", buffer_wait),
    ] {
        if latency.sample_count != completion.sample_count {
            return Err(format!(
                "{name} had {} samples, expected {}",
                latency.sample_count, completion.sample_count
            ));
        }
    }

    if capacity_wait.total_nanos + reserved_unadmitted.total_nanos != stages[0].total_nanos {
        return Err(format!(
            "capacity wait plus reserved-unadmitted wait ({} ns) did not equal t0-to-admit total ({} ns)",
            capacity_wait.total_nanos + reserved_unadmitted.total_nanos,
            stages[0].total_nanos
        ));
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
    let stage_total: u128 = stages.iter().map(|stage| stage.total_nanos).sum();
    if stage_total != completion.total_nanos {
        return Err(format!(
            "five stage totals {stage_total} ns did not equal end-to-end total {} ns",
            completion.total_nanos
        ));
    }
    verifier.validate(iterations)?;
    validate_map(&worker.final_map, map_state, iterations)?;

    let state = shared.state.lock().await;
    if state.admitted_requests != iterations || state.dispatched_requests != iterations {
        return Err(format!(
            "admitted/dispatched accounting was {}/{}, expected {iterations}",
            state.admitted_requests, state.dispatched_requests
        ));
    }
    let pool_expansions = state.pool_expansions;
    let preallocated_buffers = state.preallocated_buffers;
    let peak_buffers = state.peak_buffers;
    if state.total_buffers != state.preallocated_buffers + state.pool_expansions as usize {
        return Err(
            "batch pool buffer accounting did not match preallocation plus expansions".to_owned(),
        );
    }
    if state.ready.len() != 0
        || state.free.len() + usize::from(state.active.is_some()) != state.total_buffers
    {
        return Err("batch pool did not recycle all buffers after completion".to_owned());
    }
    drop(state);
    if shared.admitted_outstanding.load(Ordering::Relaxed) != 0 {
        return Err("admitted request permits remained outstanding after completion".to_owned());
    }
    if shared.reserved_unadmitted.load(Ordering::Relaxed) != 0 {
        return Err("reserved but unadmitted request permits remained after completion".to_owned());
    }
    if semaphore.available_permits() != MAX_ADMITTED {
        return Err("request capacity permits were not fully returned".to_owned());
    }
    let peak_admitted_outstanding = shared.peak_admitted_outstanding.load(Ordering::Relaxed);
    let peak_reserved_unadmitted = shared.peak_reserved_unadmitted.load(Ordering::Relaxed);
    if peak_admitted_outstanding > MAX_ADMITTED as u64 {
        return Err(format!(
            "peak admitted requests {peak_admitted_outstanding} exceeded {MAX_ADMITTED}"
        ));
    }
    if peak_reserved_unadmitted > MAX_ADMITTED as u64 {
        return Err(format!(
            "peak reserved requests {peak_reserved_unadmitted} exceeded {MAX_ADMITTED}"
        ));
    }

    let pipeline_wall = last_t6.duration_since(first_t0).as_secs_f64();
    let admission_wall = last_t_admit.duration_since(first_t0).as_secs_f64();
    let handler_active_seconds = worker.handler_active_nanos as f64 / 1_000_000_000.0;
    let cpu_wall_seconds = completion_mark
        .cpu_measured_at
        .duration_since(release_at)
        .as_secs_f64();
    if !cpu_wall_seconds.is_finite() || cpu_wall_seconds <= 0.0 {
        return Err("CPU measurement window was not positive and finite".to_owned());
    }
    let cpu_core_equivalents = completion_mark.process_cpu_seconds / cpu_wall_seconds;
    if !cpu_core_equivalents.is_finite() {
        return Err("CPU core equivalents were not finite".to_owned());
    }

    Ok(RoundResult {
        configured_coroutines,
        effective_coroutines,
        completed,
        completed_rps: requests_per_second(completed, pipeline_wall, "completed RPS")?,
        admission_rps: requests_per_second(iterations, admission_wall, "admission RPS")?,
        completion,
        capacity_wait,
        reserved_unadmitted,
        buffer_wait,
        stages,
        cpu_process_seconds: completion_mark.process_cpu_seconds,
        cpu_wall_seconds,
        cpu_core_equivalents,
        handler_active_seconds,
        handler_active_rps: requests_per_second(
            iterations,
            handler_active_seconds,
            "handler-active RPS",
        )?,
        handler_ns_per_item: worker.handler_active_nanos as f64 / iterations as f64,
        batch_duration,
        batch_count: worker.batch_count,
        average_batch_size: worker.total_batch_size as f64 / worker.batch_count as f64,
        maximum_batch_size: worker.maximum_batch_size,
        flush_full: worker.flush_full,
        flush_timeout: worker.flush_timeout,
        flush_tail: worker.flush_tail,
        pool_expansions,
        preallocated_buffers,
        peak_buffers,
        buffer_wait_events,
        peak_admitted_outstanding,
        peak_reserved_unadmitted,
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
            "--repetitions" => config.repetitions = parse_positive("--repetitions", args.next())?,
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
                            let count = item.parse::<u64>().map_err(|_| {
                                "--coroutines must be all or comma-separated positive integers"
                                    .to_owned()
                            })?;
                            if count == 0 {
                                return Err(
                                    "--coroutines values must be greater than zero".to_owned()
                                );
                            }
                            Ok(count)
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
                    _ => return Err("--batch-timeout-ms must be all, 1, 5, 10, or 20".to_owned()),
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
        "Usage: bounded_queue_batch_pool_tokio [OPTIONS]\n\
         --iterations N --repetitions N --coroutines all|N[,N...]\n\
         --map-state all|empty|prefilled --batch-size all|2048|4096\n\
         --batch-timeout-ms all|1|5|10|20 --latency-sample-stride N\n\
         Defaults: {DEFAULT_ITERATIONS} requests, {DEFAULT_REPETITIONS} repetition,\n\
         coroutine counts {DEFAULT_COROUTINES:?}, all map/batch/timeout variants,\n\
         deterministic latency sample stride {DEFAULT_SAMPLE_STRIDE}."
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
        "iterations",
        "repetition",
        "configured_coroutines",
        "effective_coroutines",
        "map_state",
        "batch_size",
        "batch_timeout_ms",
        "completed",
        "completed_rps",
        "admission_rps",
        "cpu_process_seconds",
        "cpu_wall_seconds",
        "cpu_core_equivalents",
        "cpu_one_core_percent",
        "handler_active_seconds",
        "handler_active_rps",
        "handler_ns_per_item",
        "batch_count",
        "batch_average_size",
        "batch_maximum_size",
        "flush_full",
        "flush_timeout",
        "flush_tail",
        "preallocated_buffers",
        "pool_expansions",
        "peak_buffers",
        "buffer_wait_events",
        "peak_admitted_outstanding",
        "peak_reserved_unadmitted",
        "final_map_len",
        "stage0_name",
        "stage1_name",
        "stage2_name",
        "stage3_name",
        "stage4_name",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect::<Vec<_>>();
    for name in [
        "completion",
        "capacity_acquire_wait",
        "reserved_unadmitted_wait",
        "buffer_availability_wait",
    ] {
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
    for index in 0..5 {
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
    for stat in [
        "mean_ns",
        "p50_ns",
        "p95_ns",
        "p99_ns",
        "count",
        "sample_count",
        "total_ns",
    ] {
        fields.push(format!("batch_handler_duration_{stat}"));
    }
    println!("{}", fields.join(","));
    io::stdout()
        .flush()
        .expect("could not flush benchmark CSV header");
}

fn print_row(
    iterations: u64,
    repetition: usize,
    map_state: MapState,
    batch_size: usize,
    timeout_ms: u64,
    result: RoundResult,
) {
    let mut fields = vec![
        iterations.to_string(),
        repetition.to_string(),
        result.configured_coroutines.to_string(),
        result.effective_coroutines.to_string(),
        map_state.name().to_owned(),
        batch_size.to_string(),
        timeout_ms.to_string(),
        result.completed.to_string(),
        result.completed_rps.to_string(),
        result.admission_rps.to_string(),
        result.cpu_process_seconds.to_string(),
        result.cpu_wall_seconds.to_string(),
        result.cpu_core_equivalents.to_string(),
        (result.cpu_core_equivalents * 100.0).to_string(),
        result.handler_active_seconds.to_string(),
        result.handler_active_rps.to_string(),
        result.handler_ns_per_item.to_string(),
        result.batch_count.to_string(),
        result.average_batch_size.to_string(),
        result.maximum_batch_size.to_string(),
        result.flush_full.to_string(),
        result.flush_timeout.to_string(),
        result.flush_tail.to_string(),
        result.preallocated_buffers.to_string(),
        result.pool_expansions.to_string(),
        result.peak_buffers.to_string(),
        result.buffer_wait_events.to_string(),
        result.peak_admitted_outstanding.to_string(),
        result.peak_reserved_unadmitted.to_string(),
        result.final_map_len.to_string(),
        "t0_to_admit".to_owned(),
        "admit_to_seal".to_owned(),
        "seal_to_start".to_owned(),
        "start_to_done_map_update".to_owned(),
        "done_to_t6_response_delivery".to_owned(),
    ];
    for latency in [
        result.completion,
        result.capacity_wait,
        result.reserved_unadmitted,
        result.buffer_wait,
    ] {
        push_latency(&mut fields, latency);
    }
    for latency in result.stages {
        push_latency(&mut fields, latency);
    }
    push_latency(&mut fields, result.batch_duration);
    println!("{}", fields.join(","));
    io::stdout()
        .flush()
        .expect("could not flush benchmark CSV row");
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
        .worker_threads(RUNTIME_WORKERS)
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
    let total_scenarios = config.coroutines.len()
        * config.map_states.len()
        * config.batch_sizes.len()
        * config.batch_timeouts_ms.len()
        * config.repetitions;
    let mut scenario_index = 0;
    for &coroutines in &config.coroutines {
        for &map_state in &config.map_states {
            for &batch_size in &config.batch_sizes {
                for &timeout_ms in &config.batch_timeouts_ms {
                    for repetition in 1..=config.repetitions {
                        scenario_index += 1;
                        eprintln!(
                            "starting scenario {scenario_index}/{total_scenarios}: C={coroutines}, map={}, B={batch_size}, T={timeout_ms}ms, repetition={repetition}",
                            map_state.name()
                        );
                        match runtime.block_on(run_round(
                            config.iterations,
                            coroutines,
                            Arc::clone(&sample_plan),
                            map_state,
                            batch_size,
                            timeout_ms,
                        )) {
                            Ok(result) => {
                                print_row(
                                    config.iterations,
                                    repetition,
                                    map_state,
                                    batch_size,
                                    timeout_ms,
                                    result,
                                );
                                eprintln!("finished scenario {scenario_index}/{total_scenarios}");
                            }
                            Err(error) => {
                                eprintln!(
                                    "scenario failed for C={coroutines}, map={}, batch_size={batch_size}, timeout={timeout_ms}ms, repetition={repetition}: {error}",
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
