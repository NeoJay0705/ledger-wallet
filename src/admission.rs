//! Transaction-time gate and durable projection progress notifications.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::watch;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GateDecision {
    Historical,
    Commit,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct GateContentionSnapshot {
    pub blocked_admissions: u64,
    pub blocked_wait_ns: u64,
    pub max_blocked_wait_ns: u64,
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

/// The single boundary router for one source shard.
pub struct AdmissionGate {
    state: Mutex<GateState>,
    changed: watch::Sender<u64>,
    blocked_admissions: AtomicU64,
    blocked_wait_ns: AtomicU64,
    max_blocked_wait_ns: AtomicU64,
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

    /// Route by strict `< watermark` semantics. Commits admitted below an
    /// active candidate hold a guard until their response is observed/dropped.
    pub async fn admit(
        self: &Arc<Self>,
        timestamp: u64,
    ) -> Result<(GateDecision, Option<CommitGuard>), String> {
        let mut changed = self.changed.subscribe();
        let mut blocked_since: Option<Instant> = None;
        loop {
            let decision = {
                let mut state = self
                    .state
                    .lock()
                    .map_err(|_| "admission gate mutex poisoned".to_owned())?;
                if timestamp < state.watermark {
                    Some((GateDecision::Historical, None))
                } else if state.fence.is_some_and(|fence| {
                    timestamp >= fence.lower_bound && timestamp < fence.candidate
                }) {
                    None
                } else {
                    let count = state.active_by_timestamp.entry(timestamp).or_default();
                    *count = count
                        .checked_add(1)
                        .ok_or_else(|| "active admission count overflow".to_owned())?;
                    Some((
                        GateDecision::Commit,
                        Some(CommitGuard {
                            gate: Arc::clone(self),
                            timestamp,
                        }),
                    ))
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

    pub(crate) fn begin_fence(
        self: &Arc<Self>,
        candidate: u64,
    ) -> Result<Option<FenceLease>, String> {
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

pub struct CommitGuard {
    gate: Arc<AdmissionGate>,
    timestamp: u64,
}

impl Drop for CommitGuard {
    fn drop(&mut self) {
        self.gate.release_guard(self.timestamp);
    }
}

pub(crate) struct FenceLease {
    gate: Arc<AdmissionGate>,
    fence: ActiveFence,
    active: bool,
}

impl FenceLease {
    pub(crate) async fn wait_for_admitted_commits(&self) -> Result<(), String> {
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

    pub(crate) fn publish(mut self) -> Result<(), String> {
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

/// Monotonic projector progress for one open service instance.
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

fn elapsed_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}
