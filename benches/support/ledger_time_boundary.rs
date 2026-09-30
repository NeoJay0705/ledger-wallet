//! Safe transaction-time watermark publication for the single-shard benchmark.
//!
//! The admission gate is the only boundary router. A commit admission carries
//! an owned guard into the batch queue and back through its response; dropping
//! the caller future therefore cannot make a not-yet-committed request
//! invisible to a watermark fence.

use crate::ledger_account_store::{AccountStore, Reply, Transaction};
use crate::ledger_projection_worker::{HistoricalLookup, MockProjectionStore};
use crate::request_batch_queue::{self, BatchQueue, BatchWorker, Completed};
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tokio::task::JoinHandle;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RoutedReply {
    Commit(Reply),
    HistoricalHit(crate::ledger_account_store::TransactionResult),
    HistoricalMiss,
    Conflict,
}

pub struct AdmittedTransaction {
    pub transaction: Transaction,
    guard: CommitGuard,
}

pub struct GuardedReply {
    pub reply: RoutedReply,
    guard: Option<CommitGuard>,
}

impl GuardedReply {
    fn historical(reply: RoutedReply) -> Self {
        Self { reply, guard: None }
    }

    fn committed(reply: RoutedReply, guard: CommitGuard) -> Self {
        Self {
            reply,
            guard: Some(guard),
        }
    }

    /// Consume the queue response after the caller has observed it. The guard
    /// then stops counting this commit as in flight. If the caller drops its
    /// request future first, the queue's oneshot response owns and drops it.
    pub fn observe(mut self) -> RoutedReply {
        self.guard.take();
        self.reply
    }
}

#[derive(Clone, Copy, Debug)]
struct ActiveFence {
    id: u64,
    lower_bound: u64,
    candidate: u64,
}

#[derive(Default)]
struct GateState {
    watermark: u64,
    next_fence_id: u64,
    fence: Option<ActiveFence>,
    active_by_timestamp: BTreeMap<u64, usize>,
}

pub struct AdmissionGate {
    state: Mutex<GateState>,
    changed: watch::Sender<u64>,
    blocked_admissions: AtomicU64,
    blocked_wait_ns: AtomicU64,
    max_blocked_wait_ns: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct GateContentionSnapshot {
    pub blocked_admissions: u64,
    pub blocked_wait_ns: u64,
    pub max_blocked_wait_ns: u64,
}

impl AdmissionGate {
    pub fn new(initial_watermark: u64) -> Arc<Self> {
        let (changed, _) = watch::channel(0_u64);
        Arc::new(Self {
            state: Mutex::new(GateState {
                watermark: initial_watermark,
                ..GateState::default()
            }),
            changed,
            blocked_admissions: AtomicU64::new(0),
            blocked_wait_ns: AtomicU64::new(0),
            max_blocked_wait_ns: AtomicU64::new(0),
        })
    }

    pub fn watermark(&self) -> Result<u64, String> {
        self.state
            .lock()
            .map(|state| state.watermark)
            .map_err(|_| "admission gate mutex poisoned".to_owned())
    }

    pub fn active_fence(&self) -> Result<Option<u64>, String> {
        self.state
            .lock()
            .map(|state| state.fence.map(|fence| fence.candidate))
            .map_err(|_| "admission gate mutex poisoned".to_owned())
    }

    pub fn contention_snapshot(&self) -> GateContentionSnapshot {
        GateContentionSnapshot {
            blocked_admissions: self.blocked_admissions.load(Ordering::Relaxed),
            blocked_wait_ns: self.blocked_wait_ns.load(Ordering::Relaxed),
            max_blocked_wait_ns: self.max_blocked_wait_ns.load(Ordering::Relaxed),
        }
    }

    /// Route a request using strict `< watermark` history semantics.
    /// Requests in `[watermark, candidate)` wait before queue admission while
    /// a fence is active. Requests at or above the candidate keep committing.
    pub async fn admit(self: &Arc<Self>, timestamp: u64) -> Result<GateDecision, String> {
        let mut changed = self.changed.subscribe();
        let mut blocked_since: Option<Instant> = None;
        loop {
            let decision = {
                let mut state = self
                    .state
                    .lock()
                    .map_err(|_| "admission gate mutex poisoned".to_owned())?;
                if timestamp < state.watermark {
                    Some(GateDecision::Historical)
                } else if state.fence.is_some_and(|fence| {
                    timestamp >= fence.lower_bound && timestamp < fence.candidate
                }) {
                    None
                } else {
                    let count = state.active_by_timestamp.entry(timestamp).or_default();
                    *count = count
                        .checked_add(1)
                        .ok_or_else(|| "active admission count overflow".to_owned())?;
                    Some(GateDecision::Commit(CommitGuard {
                        gate: Arc::clone(self),
                        timestamp,
                    }))
                }
            };
            if let Some(decision) = decision {
                if let Some(started) = blocked_since {
                    let elapsed = elapsed_ns(started.elapsed());
                    self.blocked_admissions.fetch_add(1, Ordering::Relaxed);
                    self.blocked_wait_ns.fetch_add(elapsed, Ordering::Relaxed);
                    self.max_blocked_wait_ns
                        .fetch_max(elapsed, Ordering::Relaxed);
                }
                return Ok(decision);
            }
            blocked_since.get_or_insert_with(Instant::now);
            changed
                .changed()
                .await
                .map_err(|_| "admission gate notification channel closed".to_owned())?;
        }
    }

    fn begin_fence(self: &Arc<Self>, candidate: u64) -> Result<Option<FenceLease>, String> {
        let fence = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| "admission gate mutex poisoned".to_owned())?;
            if candidate <= state.watermark {
                return Ok(None);
            }
            if state.fence.is_some() {
                return Err("an admission fence is already active".to_owned());
            }
            state.next_fence_id = state
                .next_fence_id
                .checked_add(1)
                .ok_or_else(|| "admission fence ID overflow".to_owned())?;
            let fence = ActiveFence {
                id: state.next_fence_id,
                lower_bound: state.watermark,
                candidate,
            };
            state.fence = Some(fence);
            fence
        };
        self.notify_changed();
        Ok(Some(FenceLease {
            gate: Arc::clone(self),
            fence,
            active: true,
        }))
    }

    fn release_guard(&self, timestamp: u64) {
        if let Ok(mut state) = self.state.lock() {
            if let Some(count) = state.active_by_timestamp.get_mut(&timestamp) {
                *count -= 1;
                if *count == 0 {
                    state.active_by_timestamp.remove(&timestamp);
                }
            }
        }
        self.notify_changed();
    }

    fn publish_fence(&self, fence: ActiveFence) -> Result<(), String> {
        {
            let mut state = self
                .state
                .lock()
                .map_err(|_| "admission gate mutex poisoned while publishing".to_owned())?;
            if state.fence.map(|active| active.id) != Some(fence.id) {
                return Err("admission fence changed before publication".to_owned());
            }
            state.watermark = fence.candidate;
            state.fence = None;
        }
        self.notify_changed();
        Ok(())
    }

    fn cancel_fence(&self, fence_id: u64) {
        if let Ok(mut state) = self.state.lock() {
            if state.fence.map(|fence| fence.id) == Some(fence_id) {
                state.fence = None;
            }
        }
        self.notify_changed();
    }

    fn notify_changed(&self) {
        self.changed
            .send_modify(|revision| *revision = revision.saturating_add(1));
    }
}

pub enum GateDecision {
    Historical,
    Commit(CommitGuard),
}

pub struct CommitGuard {
    gate: Arc<AdmissionGate>,
    timestamp: u64,
}

impl Drop for CommitGuard {
    fn drop(&mut self) {
        self.gate.release_guard(self.timestamp);
    }
}

pub struct FenceLease {
    gate: Arc<AdmissionGate>,
    fence: ActiveFence,
    active: bool,
}

impl FenceLease {
    pub async fn wait_for_admitted_commits(&self) -> Result<(), String> {
        let mut changed = self.gate.changed.subscribe();
        loop {
            let drained = {
                let state = self
                    .gate
                    .state
                    .lock()
                    .map_err(|_| "admission gate mutex poisoned while draining".to_owned())?;
                if state.fence.map(|fence| fence.id) != Some(self.fence.id) {
                    return Err("admission fence was cancelled while draining".to_owned());
                }
                state
                    .active_by_timestamp
                    .range(self.fence.lower_bound..self.fence.candidate)
                    .next()
                    .is_none()
            };
            if drained {
                return Ok(());
            }
            changed
                .changed()
                .await
                .map_err(|_| "admission gate notification channel closed".to_owned())?;
        }
    }

    pub fn publish(mut self) -> Result<(), String> {
        self.gate.publish_fence(self.fence)?;
        self.active = false;
        Ok(())
    }
}

impl Drop for FenceLease {
    fn drop(&mut self) {
        if self.active {
            self.gate.cancel_fence(self.fence.id);
        }
    }
}

#[derive(Default)]
struct ProgressState {
    sequence: u64,
    failure: Option<String>,
}

pub struct ProjectionProgress {
    state: Mutex<ProgressState>,
    changed: watch::Sender<u64>,
}

impl ProjectionProgress {
    pub fn new(initial_sequence: u64) -> Arc<Self> {
        let (changed, _) = watch::channel(initial_sequence);
        Arc::new(Self {
            state: Mutex::new(ProgressState {
                sequence: initial_sequence,
                failure: None,
            }),
            changed,
        })
    }

    pub fn sequence(&self) -> Result<u64, String> {
        self.state
            .lock()
            .map(|state| state.sequence)
            .map_err(|_| "projection progress mutex poisoned".to_owned())
    }

    pub fn acknowledge(&self, sequence: u64) -> Result<(), String> {
        {
            let mut state = self
                .state
                .lock()
                .map_err(|_| "projection progress mutex poisoned".to_owned())?;
            if state.failure.is_some() {
                return Err("cannot advance a failed projector".to_owned());
            }
            if sequence < state.sequence {
                return Err(format!(
                    "projector progress moved backwards from {} to {sequence}",
                    state.sequence
                ));
            }
            state.sequence = sequence;
        }
        self.changed.send_replace(sequence);
        Ok(())
    }

    pub fn fail(&self, error: impl Into<String>) {
        let sequence = if let Ok(mut state) = self.state.lock() {
            state.failure = Some(error.into());
            state.sequence
        } else {
            0
        };
        self.changed.send_replace(sequence);
    }

    pub async fn wait_for(&self, target: u64) -> Result<(), String> {
        let mut changed = self.changed.subscribe();
        loop {
            {
                let state = self
                    .state
                    .lock()
                    .map_err(|_| "projection progress mutex poisoned".to_owned())?;
                if let Some(error) = &state.failure {
                    return Err(format!("projector failed: {error}"));
                }
                if state.sequence >= target {
                    return Ok(());
                }
            }
            changed
                .changed()
                .await
                .map_err(|_| "projection progress notification channel closed".to_owned())?;
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ProjectionBatchSample {
    pub records: usize,
    pub read_ns: u64,
    pub apply_ns: u64,
    pub delay_ns: u64,
    pub dispatch_ns: u64,
    pub total_ns: u64,
}

#[derive(Default)]
pub struct ProjectionMetrics {
    samples: Mutex<Vec<ProjectionBatchSample>>,
}

impl ProjectionMetrics {
    pub fn record(&self, sample: ProjectionBatchSample) -> Result<(), String> {
        self.samples
            .lock()
            .map_err(|_| "projection metrics mutex poisoned".to_owned())?
            .push(sample);
        Ok(())
    }

    pub fn snapshot(&self) -> Result<Vec<ProjectionBatchSample>, String> {
        self.samples
            .lock()
            .map(|samples| samples.clone())
            .map_err(|_| "projection metrics mutex poisoned".to_owned())
    }
}

pub async fn run_projector(
    source: AccountStore,
    destination: Arc<MockProjectionStore>,
    progress: Arc<ProjectionProgress>,
    metrics: Arc<ProjectionMetrics>,
    mut committed_head: watch::Receiver<u64>,
    mut shutdown: watch::Receiver<bool>,
    batch_size: usize,
    synthetic_delay: Duration,
) -> Result<(), String> {
    if batch_size == 0 {
        return Err("projection batch size must be positive".to_owned());
    }
    let mut next_sequence = progress
        .sequence()?
        .checked_add(1)
        .ok_or_else(|| "projection sequence overflow".to_owned())?;
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        let latest_sequence = source.latest_seq();
        if next_sequence <= latest_sequence {
            let total_started = Instant::now();
            let read = source.read_ledger_range(next_sequence, batch_size).await?;
            if read.records.is_empty() {
                return Err(format!(
                    "projector found no source records at sequence {next_sequence}"
                ));
            }
            let read_ns = read.db_read_ns;
            let delay_started = Instant::now();
            if !synthetic_delay.is_zero() {
                tokio::select! {
                    _ = tokio::time::sleep(synthetic_delay) => {},
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            return Ok(());
                        }
                    }
                }
            }
            let delay_ns = u64::try_from(delay_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            let apply_started = Instant::now();
            let acknowledged = destination.apply_batch(&read.records)?;
            let apply_ns = u64::try_from(apply_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            let total_ns = u64::try_from(total_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            let measured = read_ns.saturating_add(delay_ns).saturating_add(apply_ns);
            metrics.record(ProjectionBatchSample {
                records: read.records.len(),
                read_ns,
                apply_ns,
                delay_ns,
                dispatch_ns: total_ns.saturating_sub(measured),
                total_ns,
            })?;
            progress.acknowledge(acknowledged)?;
            next_sequence = acknowledged
                .checked_add(1)
                .ok_or_else(|| "projection sequence overflow".to_owned())?;
            continue;
        }
        tokio::select! {
            changed = committed_head.changed() => {
                if changed.is_err() {
                    return Ok(());
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct WatermarkSample {
    pub fence_wait_ns: u64,
    pub projection_wait_ns: u64,
    pub persist_ns: u64,
    pub total_ns: u64,
    pub target_sequence: u64,
    pub published_watermark: u64,
    pub completed_at: Instant,
}

#[derive(Default)]
pub struct WatermarkMetrics {
    samples: Mutex<Vec<WatermarkSample>>,
}

impl WatermarkMetrics {
    pub fn snapshot(&self) -> Result<Vec<WatermarkSample>, String> {
        self.samples
            .lock()
            .map(|samples| samples.clone())
            .map_err(|_| "watermark metrics mutex poisoned".to_owned())
    }

    fn record(&self, sample: WatermarkSample) -> Result<(), String> {
        self.samples
            .lock()
            .map_err(|_| "watermark metrics mutex poisoned".to_owned())?
            .push(sample);
        Ok(())
    }
}

pub struct WatermarkManager {
    gate: Arc<AdmissionGate>,
    progress: Arc<ProjectionProgress>,
    metrics: Arc<WatermarkMetrics>,
}

impl WatermarkManager {
    pub fn new(gate: Arc<AdmissionGate>, progress: Arc<ProjectionProgress>) -> Self {
        Self {
            gate,
            progress,
            metrics: Arc::new(WatermarkMetrics::default()),
        }
    }

    pub fn metrics(&self) -> Arc<WatermarkMetrics> {
        Arc::clone(&self.metrics)
    }

    pub async fn advance_once(&self, candidate: u64, store: &AccountStore) -> Result<bool, String> {
        self.advance_with(
            candidate,
            || store.latest_seq(),
            |value| store.persist_projected_before(value),
        )
        .await
    }

    /// Advance and persist a restart-safe boundary tied to the drained source
    /// sequence. Callers must acknowledge projector progress only after its
    /// source-side progress sync succeeds.
    pub async fn advance_once_durable(
        &self,
        candidate: u64,
        store: &AccountStore,
    ) -> Result<bool, String> {
        let Some(fence) = self.gate.begin_fence(candidate)? else {
            return Ok(false);
        };
        let total_started = Instant::now();
        let fence_started = Instant::now();
        fence.wait_for_admitted_commits().await?;
        let fence_wait_ns = elapsed_ns(fence_started.elapsed());
        let target_sequence = store.latest_seq();
        let projection_started = Instant::now();
        tokio::time::timeout(
            Duration::from_secs(300),
            self.progress.wait_for(target_sequence),
        )
        .await
        .map_err(|_| {
            format!(
                "projector did not reach watermark target sequence {target_sequence} within 300s"
            )
        })??;
        let projection_wait_ns = elapsed_ns(projection_started.elapsed());
        let persist_started = Instant::now();
        store
            .persist_projected_before_durable(candidate, target_sequence)
            .await?;
        let persist_ns = elapsed_ns(persist_started.elapsed());
        fence.publish()?;
        self.metrics.record(WatermarkSample {
            fence_wait_ns,
            projection_wait_ns,
            persist_ns,
            total_ns: elapsed_ns(total_started.elapsed()),
            target_sequence,
            published_watermark: candidate,
            completed_at: Instant::now(),
        })?;
        Ok(true)
    }

    async fn advance_with<F, Fut>(
        &self,
        candidate: u64,
        latest_sequence: F,
        persist: impl FnOnce(u64) -> Fut,
    ) -> Result<bool, String>
    where
        F: FnOnce() -> u64,
        Fut: Future<Output = Result<(), String>>,
    {
        let Some(fence) = self.gate.begin_fence(candidate)? else {
            return Ok(false);
        };
        let total_started = Instant::now();
        let fence_started = Instant::now();
        fence.wait_for_admitted_commits().await?;
        let fence_wait_ns = elapsed_ns(fence_started.elapsed());

        // The target is captured only after all commit guards in the fenced
        // interval have been released by observed replies (or dropped callers).
        let target_sequence = latest_sequence();
        let projection_started = Instant::now();
        tokio::time::timeout(
            Duration::from_secs(300),
            self.progress.wait_for(target_sequence),
        )
        .await
        .map_err(|_| {
            format!(
                "projector did not reach watermark target sequence {target_sequence} within 300s"
            )
        })??;
        let projection_wait_ns = elapsed_ns(projection_started.elapsed());

        let persist_started = Instant::now();
        persist(candidate).await?;
        let persist_ns = elapsed_ns(persist_started.elapsed());

        fence.publish()?;
        self.metrics.record(WatermarkSample {
            fence_wait_ns,
            projection_wait_ns,
            persist_ns,
            total_ns: elapsed_ns(total_started.elapsed()),
            target_sequence,
            published_watermark: candidate,
            completed_at: Instant::now(),
        })?;
        Ok(true)
    }

    pub async fn run_periodic(
        self: Arc<Self>,
        store: AccountStore,
        retention: Duration,
        interval: Duration,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), String> {
        if interval.is_zero() {
            return Err("watermark interval must be positive".to_owned());
        }
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            tokio::select! {
                _ = ticker.tick() => {},
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
            }
            let candidate = unix_time_micros()
                .saturating_sub(u64::try_from(retention.as_micros()).unwrap_or(u64::MAX));
            // Once a candidate has installed its fence, let persistence and
            // in-memory publication finish as one ordered stage. Shutdown is
            // observed before the next candidate; dropping a persistence
            // future could otherwise race its spawn_blocking WAL write.
            self.advance_once(candidate, &store).await?;
        }
    }

    pub async fn run_periodic_durable(
        self: Arc<Self>,
        store: AccountStore,
        retention: Duration,
        interval: Duration,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), String> {
        if interval.is_zero() {
            return Err("watermark interval must be positive".to_owned());
        }
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            tokio::select! {
                _ = ticker.tick() => {},
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
            }
            let candidate = unix_time_micros()
                .saturating_sub(u64::try_from(retention.as_micros()).unwrap_or(u64::MAX));
            self.advance_once_durable(candidate, &store).await?;
        }
    }
}

pub fn spawn_commit_queue(
    store: AccountStore,
    committed_head: watch::Sender<u64>,
    capacity: usize,
    max_batch_size: usize,
    timeout: Duration,
) -> Result<(BatchQueue<AdmittedTransaction, GuardedReply>, BatchWorker), String> {
    request_batch_queue::spawn(
        request_batch_queue::Config {
            capacity,
            max_batch_size,
            timeout,
        },
        move |requests: Vec<AdmittedTransaction>| {
            let store = store.clone();
            let committed_head = committed_head.clone();
            async move {
                let mut guards = Vec::with_capacity(requests.len());
                let transactions = requests
                    .into_iter()
                    .map(|request| {
                        guards.push(request.guard);
                        request.transaction
                    })
                    .collect();
                let replies = store.handle_batch(transactions).await?;
                committed_head.send_replace(store.latest_seq());
                Ok(replies
                    .into_iter()
                    .zip(guards)
                    .map(|(reply, guard)| {
                        GuardedReply::committed(RoutedReply::Commit(reply), guard)
                    })
                    .collect())
            }
        },
    )
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RequestStages {
    pub overall_ns: u64,
    pub admission_ns: u64,
    pub enqueue_ns: u64,
    pub queue_ns: u64,
    pub batch_ns: u64,
    pub handler_ns: u64,
    pub response_ns: u64,
    pub old_lookup_ns: u64,
}

pub struct RequestOutcome {
    pub reply: RoutedReply,
    pub stages: RequestStages,
}

pub async fn route_request(
    gate: &Arc<AdmissionGate>,
    queue: &BatchQueue<AdmittedTransaction, GuardedReply>,
    projection: &MockProjectionStore,
    history_delay: Duration,
    transaction: Transaction,
    started_at: Instant,
) -> Result<RequestOutcome, String> {
    let decision = gate.admit(transaction.key.transaction_at).await?;
    let admitted_at = Instant::now();
    let admission_ns = elapsed_ns(admitted_at.duration_since(started_at));
    match decision {
        GateDecision::Historical => {
            let lookup_started = Instant::now();
            if !history_delay.is_zero() {
                tokio::time::sleep(history_delay).await;
            }
            let reply = match projection.lookup_transaction(&transaction)? {
                HistoricalLookup::ExactReplay(result) => RoutedReply::HistoricalHit(result),
                HistoricalLookup::Conflict => RoutedReply::Conflict,
                HistoricalLookup::NotFound => RoutedReply::HistoricalMiss,
            };
            let observed_at = Instant::now();
            Ok(RequestOutcome {
                reply,
                stages: RequestStages {
                    overall_ns: elapsed_ns(observed_at.duration_since(started_at)),
                    admission_ns,
                    old_lookup_ns: elapsed_ns(observed_at.duration_since(lookup_started)),
                    ..RequestStages::default()
                },
            })
        }
        GateDecision::Commit(guard) => {
            let handle = queue
                .submit(AdmittedTransaction { transaction, guard })
                .await?;
            let completed = handle.wait().await?;
            complete_commit(completed, started_at, admission_ns)
        }
    }
}

fn complete_commit(
    completed: Completed<GuardedReply>,
    started_at: Instant,
    admission_ns: u64,
) -> Result<RequestOutcome, String> {
    let overall_ns = elapsed_ns(completed.response_observed_at.duration_since(started_at));
    let reply = completed.reply.observe();
    Ok(RequestOutcome {
        reply,
        stages: RequestStages {
            overall_ns,
            admission_ns,
            enqueue_ns: elapsed_ns(completed.enqueue_wait),
            queue_ns: elapsed_ns(completed.queue_wait),
            batch_ns: elapsed_ns(completed.batch_wait),
            handler_ns: elapsed_ns(completed.handler_time),
            response_ns: elapsed_ns(completed.response_wait),
            old_lookup_ns: 0,
        },
    })
}

pub fn spawn_projector(
    source: AccountStore,
    destination: Arc<MockProjectionStore>,
    progress: Arc<ProjectionProgress>,
    metrics: Arc<ProjectionMetrics>,
    background_failure: watch::Sender<Option<String>>,
    committed_head: watch::Receiver<u64>,
    shutdown: watch::Receiver<bool>,
    batch_size: usize,
    synthetic_delay: Duration,
) -> JoinHandle<Result<(), String>> {
    tokio::spawn(async move {
        let result = run_projector(
            source,
            destination,
            Arc::clone(&progress),
            metrics,
            committed_head,
            shutdown,
            batch_size,
            synthetic_delay,
        )
        .await;
        if let Err(error) = &result {
            progress.fail(error.clone());
            background_failure.send_replace(Some(format!("projector failed: {error}")));
        }
        result
    })
}

pub async fn wait_for_projection_with_timeout(
    progress: &ProjectionProgress,
    target: u64,
    timeout: Duration,
) -> Result<(), String> {
    tokio::time::timeout(timeout, progress.wait_for(target))
        .await
        .map_err(|_| {
            format!("projector did not reach target sequence {target} within {timeout:?}")
        })?
}

fn elapsed_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

pub fn unix_time_micros() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_micros()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

pub async fn wait_until_progress(
    progress: &ProjectionProgress,
    sequence: u64,
) -> Result<(), String> {
    progress.wait_for(sequence).await
}

#[cfg(test)]
mod tests {
    use super::*;
    #[allow(unused_imports)]
    use crate::ledger_account_store::{BalanceMode, Operation, RocksDbBudget, TransactionKey};
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    #[allow(unused_imports)]
    use tokio::sync::{Barrier, Semaphore, oneshot};

    static NEXT_PATH: AtomicU64 = AtomicU64::new(0);

    struct TempDb(PathBuf);

    impl TempDb {
        fn new(test: &str) -> Self {
            let nonce = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "ledger-time-boundary-{test}-{}-{nonce}",
                std::process::id()
            ));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn txn(account: u64, id: u64, at: u64, amount: u64) -> Transaction {
        Transaction {
            key: TransactionKey {
                account_id: account,
                tx_id: id,
                transaction_at: at,
            },
            operation: Operation::Credit,
            amount,
            refund_of: None,
        }
    }

    #[tokio::test]
    async fn strict_boundaries_allow_old_and_candidate_times_during_fence() {
        let gate = AdmissionGate::new(10);
        assert!(matches!(
            gate.admit(9).await.unwrap(),
            GateDecision::Historical
        ));
        let current = gate.admit(10).await.unwrap();
        assert!(matches!(current, GateDecision::Commit(_)));
        drop(current);

        let prior = gate.admit(15).await.unwrap();
        let lease = gate.begin_fence(20).unwrap().unwrap();
        let waiting_gate = Arc::clone(&gate);
        let mut waiter = tokio::spawn(async move { waiting_gate.admit(19).await });
        assert!(matches!(
            gate.admit(5).await.unwrap(),
            GateDecision::Historical
        ));
        let candidate_equal = gate.admit(20).await.unwrap();
        assert!(matches!(candidate_equal, GateDecision::Commit(_)));
        drop(candidate_equal);

        assert!(
            tokio::time::timeout(Duration::from_millis(20), lease.wait_for_admitted_commits())
                .await
                .is_err()
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut waiter)
                .await
                .is_err()
        );
        drop(prior);
        lease.wait_for_admitted_commits().await.unwrap();
        lease.publish().unwrap();
        assert!(matches!(
            waiter.await.unwrap().unwrap(),
            GateDecision::Historical
        ));
        assert_eq!(gate.watermark().unwrap(), 20);
        let at_boundary = gate.admit(20).await.unwrap();
        assert!(matches!(at_boundary, GateDecision::Commit(_)));
    }

    #[tokio::test]
    async fn projection_lag_blocks_persist_and_publish_and_success_persists_first() {
        let gate = AdmissionGate::new(10);
        let progress = ProjectionProgress::new(2);
        let manager = WatermarkManager::new(Arc::clone(&gate), Arc::clone(&progress));
        let persisted = Arc::new(Mutex::new(Vec::new()));
        let persisted_by_task = Arc::clone(&persisted);
        let gate_for_persist = Arc::clone(&gate);
        let advance = tokio::spawn(async move {
            manager
                .advance_with(
                    20,
                    || 3,
                    move |watermark| async move {
                        assert_eq!(gate_for_persist.watermark().unwrap(), 10);
                        persisted_by_task.lock().unwrap().push(watermark);
                        Ok(())
                    },
                )
                .await
        });
        tokio::task::yield_now().await;
        assert_eq!(gate.active_fence().unwrap(), Some(20));
        assert!(persisted.lock().unwrap().is_empty());
        assert_eq!(gate.watermark().unwrap(), 10);
        progress.acknowledge(3).unwrap();
        assert!(advance.await.unwrap().unwrap());
        assert_eq!(*persisted.lock().unwrap(), [20]);
        assert_eq!(gate.watermark().unwrap(), 20);
    }

    #[tokio::test]
    async fn persistence_failure_keeps_old_boundary_and_releases_waiters() {
        let gate = AdmissionGate::new(10);
        let progress = ProjectionProgress::new(4);
        let manager = WatermarkManager::new(Arc::clone(&gate), progress);
        let result = manager
            .advance_with(
                20,
                || 4,
                |_| async { Err("injected sync failure".to_owned()) },
            )
            .await;
        assert!(result.unwrap_err().contains("injected sync failure"));
        assert_eq!(gate.watermark().unwrap(), 10);
        assert_eq!(gate.active_fence().unwrap(), None);
        assert!(matches!(
            gate.admit(19).await.unwrap(),
            GateDecision::Commit(_)
        ));
    }

    #[tokio::test]
    async fn canceling_queued_caller_keeps_guard_until_queue_response_is_dropped() {
        let gate = AdmissionGate::new(10);
        let GateDecision::Commit(guard) = gate.admit(20).await.unwrap() else {
            panic!("timestamp should use the commit path");
        };
        let blocked_handler = Arc::new(Semaphore::new(0));
        let handler_gate = Arc::clone(&blocked_handler);
        let (started_tx, started_rx) = oneshot::channel();
        let started_tx = Arc::new(Mutex::new(Some(started_tx)));
        let (queue, worker) = request_batch_queue::spawn(
            request_batch_queue::Config {
                capacity: 1,
                max_batch_size: 1,
                timeout: Duration::from_secs(1),
            },
            move |requests: Vec<AdmittedTransaction>| {
                let semaphore = Arc::clone(&handler_gate);
                let started_tx = Arc::clone(&started_tx);
                async move {
                    let _ = started_tx.lock().unwrap().take().unwrap().send(());
                    semaphore.acquire().await.unwrap().forget();
                    Ok(requests
                        .into_iter()
                        .map(|request| {
                            GuardedReply::committed(
                                RoutedReply::Commit(Reply::Conflict),
                                request.guard,
                            )
                        })
                        .collect())
                }
            },
        )
        .unwrap();
        let request = queue
            .submit(AdmittedTransaction {
                transaction: txn(0, 1, 20, 1),
                guard,
            })
            .await
            .unwrap();
        started_rx.await.unwrap();
        drop(request);

        let lease = gate.begin_fence(30).unwrap().unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), lease.wait_for_admitted_commits())
                .await
                .is_err()
        );
        assert_eq!(gate.active_fence().unwrap(), Some(30));
        blocked_handler.add_permits(1);
        tokio::time::timeout(Duration::from_secs(1), lease.wait_for_admitted_commits())
            .await
            .unwrap()
            .unwrap();
        lease.publish().unwrap();
        drop(queue);
        worker.join().await.unwrap();
    }

    #[tokio::test]
    async fn account_store_boundary_metadata_is_durable_and_shard_namespaced() {
        let temp = TempDb::new("metadata");
        let (db, options) = AccountStore::open_database_with_budget(
            &temp.0,
            RocksDbBudget {
                write_buffer_size: 4 * 1024 * 1024,
                max_write_buffer_number: 2,
                block_cache_bytes: 8 * 1024 * 1024,
                max_background_jobs: 1,
            },
        )
        .await
        .unwrap();
        let shard_a = AccountStore::open_on_database(
            Arc::clone(&db),
            Arc::clone(&options),
            vec![0],
            31,
            BalanceMode::PerBatch,
            100,
        )
        .await
        .unwrap();
        let shard_b =
            AccountStore::open_on_database(db, options, vec![0], 32, BalanceMode::PerBatch, 100)
                .await
                .unwrap();
        shard_a.persist_projected_before(1234).await.unwrap();
        shard_b.persist_projected_before(5678).await.unwrap();
        assert_eq!(shard_a.namespace_id(), Some(31));
        assert_eq!(shard_b.namespace_id(), Some(32));
        assert_eq!(
            shard_a.persisted_projected_before().await.unwrap(),
            Some(1234)
        );
        assert_eq!(
            shard_b.persisted_projected_before().await.unwrap(),
            Some(5678)
        );
        shard_a.shutdown().await.unwrap();
        shard_b.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn admitted_commit_is_in_target_sequence_before_projection_wait() {
        let temp = TempDb::new("target-sequence");
        let store = AccountStore::open(&temp.0, 1, BalanceMode::PerBatch, 100)
            .await
            .unwrap();
        let replies = store.handle_batch(vec![txn(0, 1, 15, 1)]).await.unwrap();
        assert_eq!(replies.len(), 1);
        assert_eq!(store.latest_seq(), 1);
        let progress = ProjectionProgress::new(0);
        let gate = AdmissionGate::new(10);
        let manager = WatermarkManager::new(gate, Arc::clone(&progress));
        let advance = tokio::spawn(async move {
            manager
                .advance_with(20, || store.latest_seq(), |_| async { Ok(()) })
                .await
        });
        tokio::task::yield_now().await;
        progress.acknowledge(1).unwrap();
        assert!(advance.await.unwrap().unwrap());
    }

    #[tokio::test]
    async fn shutdown_or_error_does_not_hold_the_admission_fence() {
        let gate = AdmissionGate::new(10);
        let progress = ProjectionProgress::new(0);
        let manager = Arc::new(WatermarkManager::new(
            Arc::clone(&gate),
            Arc::clone(&progress),
        ));
        let barrier = Arc::new(Barrier::new(2));
        let task_manager = Arc::clone(&manager);
        let task_barrier = Arc::clone(&barrier);
        let task = tokio::spawn(async move {
            task_barrier.wait().await;
            task_manager
                .advance_with(20, || 1, |_| async { Ok(()) })
                .await
        });
        barrier.wait().await;
        tokio::task::yield_now().await;
        assert_eq!(gate.active_fence().unwrap(), Some(20));
        task.abort();
        let _ = task.await;
        assert_eq!(gate.watermark().unwrap(), 10);
        assert_eq!(gate.active_fence().unwrap(), None);
        assert!(matches!(
            gate.admit(19).await.unwrap(),
            GateDecision::Commit(_)
        ));
    }
}
