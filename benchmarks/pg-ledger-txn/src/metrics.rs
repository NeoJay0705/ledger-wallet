use std::sync::atomic::{AtomicU64, Ordering};

use hdrhistogram::{
    serialization::{Serializer, V2Serializer},
    Histogram as HdrHistogram,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const LOWEST_US: u64 = 1;
const HIGHEST_US: u64 = 3_600_000_000;
const SIGNATURE: u128 = 0x4c454447455242454e43480000000000;

/// A per-user HDR histogram whose recorded values are integer microseconds.
#[derive(Clone, Debug)]
pub struct LatencyHistogram(HdrHistogram<u64>);

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self(
            HdrHistogram::new_with_bounds(LOWEST_US, HIGHEST_US, 3)
                .expect("valid histogram bounds"),
        )
    }
}

impl LatencyHistogram {
    pub fn record_us(&mut self, micros: u64) -> Result<(), hdrhistogram::RecordError> {
        self.0.record(micros)
    }

    pub fn count(&self) -> u64 {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn merge(&mut self, other: &Self) -> Result<(), hdrhistogram::AdditionError> {
        self.0.add(&other.0)
    }

    /// Nearest-rank percentile (rank = ceil(q*n)); q must be in [0, 1].
    pub fn percentile_us(&self, q: f64) -> Option<u64> {
        if self.is_empty() || !q.is_finite() || !(0.0..=1.0).contains(&q) {
            return None;
        }
        let rank = if q == 0.0 {
            1
        } else {
            (q * self.count() as f64).ceil() as u64
        };
        let target = rank.max(1);
        let mut seen = 0u64;
        for value in self.0.iter_recorded() {
            seen += value.count_since_last_iteration();
            if seen >= target {
                return Some(value.value_iterated_to());
            }
        }
        Some(self.0.max())
    }

    pub fn summary(&self) -> HistogramSummary {
        HistogramSummary {
            count: self.count(),
            min_us: if self.is_empty() {
                None
            } else {
                Some(self.0.min())
            },
            max_us: if self.is_empty() {
                None
            } else {
                Some(self.0.max())
            },
            p50_us: self.percentile_us(0.50),
            p95_us: self.percentile_us(0.95),
            p99_us: self.percentile_us(0.99),
        }
    }

    /// HDR Histogram V2 binary encoding.
    pub fn serialize(&self) -> anyhow::Result<Vec<u8>> {
        let mut bytes = Vec::new();
        V2Serializer::new().serialize(&self.0, &mut bytes)?;
        Ok(bytes)
    }
}

#[derive(Clone, Debug, Default)]
pub struct UserMetrics {
    pub user_id: u32,
    pub end_to_end: LatencyHistogram,
    pub pg_checkout: LatencyHistogram,
    pub scylla_read: LatencyHistogram,
    pub scylla_write: LatencyHistogram,
}

#[derive(Clone, Debug, Default)]
pub struct Metrics {
    pub end_to_end: LatencyHistogram,
    pub pg_checkout: LatencyHistogram,
    pub scylla_read: LatencyHistogram,
    pub scylla_write: LatencyHistogram,
}

impl Metrics {
    pub fn merge_user(&mut self, user: &UserMetrics) -> Result<(), hdrhistogram::AdditionError> {
        self.end_to_end.merge(&user.end_to_end)?;
        self.pg_checkout.merge(&user.pg_checkout)?;
        self.scylla_read.merge(&user.scylla_read)?;
        self.scylla_write.merge(&user.scylla_write)
    }

    pub fn summary(&self) -> MetricsSummary {
        MetricsSummary {
            end_to_end: self.end_to_end.summary(),
            pg_checkout: self.pg_checkout.summary(),
            scylla_read: self.scylla_read.summary(),
            scylla_write: self.scylla_write.summary(),
        }
    }

    pub fn serialized_histograms(&self) -> anyhow::Result<SerializedHistograms> {
        Ok(SerializedHistograms {
            end_to_end: self.end_to_end.serialize()?,
            pg_checkout: self.pg_checkout.serialize()?,
            scylla_read: self.scylla_read.serialize()?,
            scylla_write: self.scylla_write.serialize()?,
        })
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct HistogramSummary {
    pub count: u64,
    pub min_us: Option<u64>,
    pub max_us: Option<u64>,
    pub p50_us: Option<u64>,
    pub p95_us: Option<u64>,
    pub p99_us: Option<u64>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct MetricsSummary {
    pub end_to_end: HistogramSummary,
    pub pg_checkout: HistogramSummary,
    pub scylla_read: HistogramSummary,
    pub scylla_write: HistogramSummary,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct SerializedHistograms {
    pub end_to_end: Vec<u8>,
    pub pg_checkout: Vec<u8>,
    pub scylla_read: Vec<u8>,
    pub scylla_write: Vec<u8>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Validation {
    pub accounts: u64,
    pub expected_accounts: u64,
    pub ledger_rows: u64,
    pub expected_rows: u64,
    pub amount_sum: i128,
    pub expected_sum: u128,
}

pub type Verification = Validation;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RunResult {
    pub backend: String,
    pub users: u32,
    pub requests_per_user: u64,
    pub expected_total: u64,
    pub completed: u64,
    pub elapsed_ms: u128,
    pub measurement_start_unix_ns: u128,
    pub measurement_elapsed_ns: u128,
    pub measurement_end_unix_ns: u128,
    pub business_rps: f64,
    pub metrics: MetricsSummary,
    pub validation: Validation,
    pub per_user: Vec<(u32, MetricsSummary)>,
    pub per_second: Vec<PerSecondSample>,
    pub diagnostics: std::collections::BTreeMap<String, String>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PerSecondSample {
    pub second: u64,
    pub completed: u64,
    pub active_users: u64,
}

pub type Second = PerSecondSample;

/// Exact counters; active users are maintained by start/finish events, never inferred from K.
#[derive(Debug, Default)]
pub struct RunCounters {
    completed: AtomicU64,
    active_users: AtomicU64,
}

impl RunCounters {
    pub fn user_started(&self) -> u64 {
        self.active_users.fetch_add(1, Ordering::SeqCst) + 1
    }

    pub fn user_finished(&self) -> u64 {
        let prior = self.active_users.fetch_sub(1, Ordering::SeqCst);
        assert!(
            prior > 0,
            "user_finished called without a matching user_started"
        );
        prior - 1
    }

    pub fn operation_completed(&self) -> u64 {
        self.completed.fetch_add(1, Ordering::SeqCst) + 1
    }

    pub fn completed(&self) -> u64 {
        self.completed.load(Ordering::SeqCst)
    }

    pub fn active_users(&self) -> u64 {
        self.active_users.load(Ordering::SeqCst)
    }

    pub fn sample(&self, second: u64, completed_since_previous: u64) -> PerSecondSample {
        PerSecondSample {
            second,
            completed: completed_since_previous,
            active_users: self.active_users(),
        }
    }
}

/// Stable, non-random account identifiers. The reserved initializer UUID is outside this range.
pub fn account_uuid(account: u32) -> Uuid {
    Uuid::from_u128(SIGNATURE | (u128::from(account) + 1))
}

pub fn reserved_init_uuid() -> Uuid {
    Uuid::nil()
}

/// Generate a fresh business transaction identifier.
///
/// The arguments are retained for API compatibility; transaction IDs are deliberately
/// random so they remain unique across users, sequences, and separate benchmark runs.
pub fn new_business_id(_user: u32, _sequence: u64) -> Uuid {
    Uuid::new_v4()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn nearest_rank_percentiles_and_summary() {
        let mut h = LatencyHistogram::default();
        for x in [1, 2, 4, 8] {
            h.record_us(x).unwrap();
        }
        assert_eq!(h.percentile_us(0.0), Some(1));
        assert_eq!(h.percentile_us(0.5), Some(2));
        assert_eq!(h.percentile_us(0.95), Some(8));
        assert_eq!(h.percentile_us(1.0), Some(8));
        assert_eq!(h.percentile_us(f64::NAN), None);
        assert_eq!(h.summary().count, 4);
        let bytes = h.serialize().unwrap();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn merge_preserves_population_and_percentiles() {
        let mut a = UserMetrics::default();
        a.end_to_end.record_us(1).unwrap();
        let mut b = UserMetrics::default();
        b.end_to_end.record_us(9).unwrap();
        let mut merged = Metrics::default();
        merged.merge_user(&a).unwrap();
        merged.merge_user(&b).unwrap();
        assert_eq!(merged.end_to_end.count(), 2);
        assert_eq!(merged.end_to_end.percentile_us(0.5), Some(1));
        assert_eq!(merged.end_to_end.percentile_us(0.99), Some(9));
    }

    #[test]
    fn business_ids_are_unique_across_users_sequences_and_runs() {
        let account_ids = (0..1000).map(account_uuid).collect::<HashSet<_>>();
        assert_eq!(account_ids.len(), 1000);
        assert!(reserved_init_uuid().is_nil());

        let ids = (0..=100)
            .flat_map(|user| (0..=100).map(move |sequence| new_business_id(user, sequence)))
            .collect::<HashSet<_>>();
        assert_eq!(ids.len(), 101 * 101);
        assert!(ids.iter().all(|id| !id.is_nil()));
        assert!(ids.iter().all(|id| !account_ids.contains(id)));
        assert!(!ids.contains(&reserved_init_uuid()));

        // A later run using the same pair must receive a fresh transaction ID.
        assert_ne!(new_business_id(7, 8), new_business_id(7, 8));
    }

    #[test]
    fn exact_active_and_completed_counters() {
        let c = RunCounters::default();
        c.user_started();
        c.user_started();
        c.operation_completed();
        assert_eq!(c.completed(), 1);
        assert_eq!(c.sample(1, 1).active_users, 2);
        c.user_finished();
        assert_eq!(c.active_users(), 1);
        c.user_finished();
        assert_eq!(c.active_users(), 0);
    }
}
