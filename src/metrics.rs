//! Bounded process-local metrics for the service.
//!
//! Each stage keeps a deterministic reservoir of at most 4,096 observations.
//! Quantiles are nearest-rank values computed exactly over that reservoir, so
//! they describe the retained uniform sample, not every observation. Counts
//! and sums include every event. Reservoir selection is seeded by the stage
//! and observation count, which keeps runs reproducible without an ever-growing
//! sample vector.

use std::time::Instant;

const RESERVOIR_CAPACITY: usize = 4_096;
const RATE_WINDOW_SECONDS: usize = 60;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
pub enum Stage {
    Request = 0,
    Admission = 1,
    Enqueue = 2,
    Queue = 3,
    Batch = 4,
    IndexRead = 5,
    Apply = 6,
    SyncPublish = 7,
    Response = 8,
    HistoryLookup = 9,
    Projector = 10,
    Watermark = 11,
    GarbageCollection = 12,
    Balance = 13,
    BatchWait = 14,
}

impl Stage {
    pub const ALL: [Self; 15] = [
        Self::Request,
        Self::Admission,
        Self::Enqueue,
        Self::Queue,
        Self::Batch,
        Self::IndexRead,
        Self::Apply,
        Self::SyncPublish,
        Self::Response,
        Self::HistoryLookup,
        Self::Projector,
        Self::Watermark,
        Self::GarbageCollection,
        Self::Balance,
        Self::BatchWait,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Request => "request",
            Self::Admission => "admission",
            Self::Enqueue => "enqueue",
            Self::Queue => "queue",
            Self::Batch => "batch",
            Self::IndexRead => "index_read",
            Self::Apply => "apply",
            Self::SyncPublish => "sync_publish",
            Self::Response => "response",
            Self::HistoryLookup => "history_lookup",
            Self::Projector => "projector",
            Self::Watermark => "watermark",
            Self::GarbageCollection => "gc",
            Self::Balance => "balance",
            Self::BatchWait => "batch_wait",
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct StageSnapshot {
    pub stage: &'static str,
    pub count: u64,
    pub average_ns: u64,
    pub sample_count: usize,
    pub p50_ns: Option<u64>,
    pub p95_ns: Option<u64>,
    pub p99_ns: Option<u64>,
    pub max_ns: Option<u64>,
}

#[derive(Clone, Debug, Default)]
pub struct MetricsSnapshot {
    pub stages: Vec<StageSnapshot>,
    pub requests_per_second_1s: f64,
    pub requests_per_second_10s: f64,
    pub requests_per_second_60s: f64,
    pub rps_window_seconds: u64,
}

#[derive(Default)]
struct Reservoir {
    count: u64,
    sum_ns: u128,
    max_ns: u64,
    values: Vec<u64>,
}

impl Reservoir {
    fn record(&mut self, stage: Stage, value: u64) {
        self.count = self.count.saturating_add(1);
        self.sum_ns = self.sum_ns.saturating_add(value as u128);
        self.max_ns = self.max_ns.max(value);
        if self.values.len() < RESERVOIR_CAPACITY {
            self.values.push(value);
            return;
        }
        let choice = deterministic_choice(stage, self.count) as usize;
        if choice < RESERVOIR_CAPACITY {
            self.values[choice] = value;
        }
    }

    fn snapshot(&self, stage: Stage) -> StageSnapshot {
        let mut ordered = self.values.clone();
        ordered.sort_unstable();
        StageSnapshot {
            stage: stage.as_str(),
            count: self.count,
            average_ns: if self.count == 0 {
                0
            } else {
                u64::try_from(self.sum_ns / self.count as u128).unwrap_or(u64::MAX)
            },
            sample_count: ordered.len(),
            p50_ns: quantile(&ordered, 0.50),
            p95_ns: quantile(&ordered, 0.95),
            p99_ns: quantile(&ordered, 0.99),
            max_ns: (self.count > 0).then_some(self.max_ns),
        }
    }
}

#[derive(Default)]
struct RpsWindow {
    last_second: Option<u64>,
    buckets: Vec<(u64, u64)>,
}

impl RpsWindow {
    fn record(&mut self, second: u64) {
        if let Some((last, count)) = self.buckets.last_mut()
            && *last == second
        {
            *count = count.saturating_add(1);
            return;
        }
        if self.last_second.is_some_and(|last| second < last) {
            return;
        }
        self.buckets.push((second, 1));
        self.last_second = Some(second);
        let oldest = second.saturating_sub((RATE_WINDOW_SECONDS - 1) as u64);
        self.buckets.retain(|(at, _)| *at >= oldest);
    }

    fn rates(&self, now: u64) -> (f64, f64, f64) {
        let sum_since = |width: u64| -> u64 {
            let first = now.saturating_sub(width.saturating_sub(1));
            self.buckets
                .iter()
                .filter(|(at, _)| *at >= first && *at <= now)
                .map(|(_, count)| *count)
                .sum()
        };
        (
            sum_since(1) as f64,
            sum_since(10) as f64 / 10.0,
            sum_since(RATE_WINDOW_SECONDS as u64) as f64 / RATE_WINDOW_SECONDS as f64,
        )
    }
}

struct State {
    stages: [Reservoir; 15],
    requests: RpsWindow,
}

pub struct Metrics {
    started: Instant,
    state: std::sync::Mutex<State>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            started: Instant::now(),
            state: std::sync::Mutex::new(State {
                stages: std::array::from_fn(|_| Reservoir::default()),
                requests: RpsWindow::default(),
            }),
        }
    }

    pub fn record(&self, stage: Stage, duration_ns: u64) {
        if let Ok(mut state) = self.state.lock() {
            state.stages[stage as usize].record(stage, duration_ns);
        }
    }

    pub fn record_request(&self) {
        if let Ok(mut state) = self.state.lock() {
            let second = self.started.elapsed().as_secs();
            state.requests.record(second);
        }
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let Ok(state) = self.state.lock() else {
            return MetricsSnapshot::default();
        };
        let second = self.started.elapsed().as_secs();
        let (requests_per_second_1s, requests_per_second_10s, requests_per_second_60s) =
            state.requests.rates(second);
        MetricsSnapshot {
            stages: Stage::ALL
                .into_iter()
                .map(|stage| state.stages[stage as usize].snapshot(stage))
                .collect(),
            requests_per_second_1s,
            requests_per_second_10s,
            requests_per_second_60s,
            rps_window_seconds: RATE_WINDOW_SECONDS as u64,
        }
    }
}

fn quantile(sorted: &[u64], quantile: f64) -> Option<u64> {
    if sorted.is_empty() {
        return None;
    }
    let rank = (quantile * sorted.len() as f64).ceil() as usize;
    sorted.get(rank.saturating_sub(1)).copied()
}

fn deterministic_choice(stage: Stage, count: u64) -> u64 {
    let mut value = count ^ ((stage as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15));
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    (value ^ (value >> 31)) % count
}
