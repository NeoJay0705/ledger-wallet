use hdrhistogram::Histogram;
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

const SIGNIFICANT_FIGURES: u8 = 3;
const MIN_NANOS: u64 = 1;
const MAX_NANOS: u64 = 600_000_000_000; // Ten minutes.

#[derive(Debug)]
struct StageHistogram {
    histogram: Histogram<u64>,
    observed_min_ns: u64,
    observed_max_ns: u64,
    sum_ns: u128,
    overflow_count: u64,
}

impl StageHistogram {
    fn new() -> Self {
        Self {
            histogram: Histogram::new_with_bounds(MIN_NANOS, MAX_NANOS, SIGNIFICANT_FIGURES)
                .expect("fixed histogram bounds are valid"),
            observed_min_ns: u64::MAX,
            observed_max_ns: 0,
            sum_ns: 0,
            overflow_count: 0,
        }
    }

    fn observe(&mut self, duration: Duration) {
        let nanos = u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
        let recorded = nanos.clamp(MIN_NANOS, MAX_NANOS);
        if nanos > MAX_NANOS {
            self.overflow_count = self.overflow_count.saturating_add(1);
        }
        self.observed_min_ns = self.observed_min_ns.min(nanos);
        self.observed_max_ns = self.observed_max_ns.max(nanos);
        self.sum_ns = self.sum_ns.saturating_add(nanos as u128);
        self.histogram
            .record(recorded)
            .expect("duration was clamped to configured histogram bounds");
    }

    fn snapshot(&self) -> StageSnapshot {
        StageSnapshot {
            count: self.histogram.len(),
            min_ns: if self.histogram.is_empty() {
                0
            } else {
                self.observed_min_ns
            },
            mean_ns: if self.histogram.is_empty() {
                0
            } else {
                (self.sum_ns / self.histogram.len() as u128).min(u64::MAX as u128) as u64
            },
            p50_ns: self.histogram.value_at_quantile(0.50),
            p95_ns: self.histogram.value_at_quantile(0.95),
            p99_ns: self.histogram.value_at_quantile(0.99),
            p999_ns: self.histogram.value_at_quantile(0.999),
            max_ns: self.observed_max_ns,
            overflow_count: self.overflow_count,
        }
    }
}

/// A bounded, concurrency-safe collection of stage and in-flight measurements.
///
/// Histogram storage is bounded by the configured ten-minute range and is shared
/// per stage. Call `reset` and then `enable` at the start of a measurement window.
#[derive(Debug)]
pub struct Metrics {
    enabled: AtomicBool,
    stages: Mutex<BTreeMap<&'static str, StageHistogram>>,
    counters: Mutex<BTreeMap<&'static str, u64>>,
    cql_in_flight: AtomicUsize,
    cql_in_flight_peak: AtomicUsize,
    worker_count: AtomicUsize,
    workers_busy: AtomicUsize,
    workers_busy_peak: AtomicUsize,
    workers_idle_peak: AtomicUsize,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            enabled: AtomicBool::new(false),
            stages: Mutex::new(BTreeMap::new()),
            counters: Mutex::new(BTreeMap::new()),
            cql_in_flight: AtomicUsize::new(0),
            cql_in_flight_peak: AtomicUsize::new(0),
            worker_count: AtomicUsize::new(0),
            workers_busy: AtomicUsize::new(0),
            workers_busy_peak: AtomicUsize::new(0),
            workers_idle_peak: AtomicUsize::new(0),
        }
    }

    pub fn enable(&self) {
        self.enabled.store(true, Ordering::Release);
        let worker_count = self.worker_count.load(Ordering::Acquire);
        let workers_busy = self.workers_busy.load(Ordering::Acquire);
        atomic_max(
            &self.workers_idle_peak,
            worker_count.saturating_sub(workers_busy),
        );
    }

    pub fn disable(&self) {
        self.enabled.store(false, Ordering::Release);
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Acquire)
    }

    /// Clears the measurement and disables collection until `enable` is called.
    pub fn reset(&self) {
        self.reset_with_backend_counters(&[
            "cql_read_metadata",
            "cql_read_ledger",
            "cql_write_lwt",
        ]);
    }

    /// Clears the measurement without creating Scylla-specific counters.
    pub fn reset_postgres(&self) {
        self.reset_with_backend_counters(&[
            "pg_function_calls",
            "pg_ledger_insert_attempts",
            "pg_account_updates",
            "pg_duplicate_conflicts",
        ]);
    }

    fn reset_with_backend_counters(&self, backend_counters: &[&'static str]) {
        self.disable();
        lock(&self.stages).clear();
        let mut counters = lock(&self.counters);
        counters.clear();
        for name in backend_counters
            .iter()
            .copied()
            .chain(["accepted_requests"])
        {
            counters.insert(name, 0);
        }
        drop(counters);
        self.cql_in_flight.store(0, Ordering::Relaxed);
        self.cql_in_flight_peak.store(0, Ordering::Relaxed);
        self.workers_busy.store(0, Ordering::Relaxed);
        self.workers_busy_peak.store(0, Ordering::Relaxed);
        self.workers_idle_peak.store(0, Ordering::Relaxed);
    }

    pub fn observe(&self, stage: &'static str, duration: Duration) {
        if !self.is_enabled() {
            return;
        }
        lock(&self.stages)
            .entry(stage)
            .or_insert_with(StageHistogram::new)
            .observe(duration);
    }

    pub fn increment_counter(&self, name: &'static str) {
        self.add_counter(name, 1);
    }

    pub fn add_counter(&self, name: &'static str, amount: u64) {
        if !self.is_enabled() {
            return;
        }
        let mut counters = lock(&self.counters);
        let counter = counters.entry(name).or_default();
        *counter = counter.saturating_add(amount);
    }

    pub fn set_worker_count(&self, count: usize) {
        self.worker_count.store(count, Ordering::Release);
        let busy = self.workers_busy.load(Ordering::Acquire);
        atomic_max(&self.workers_idle_peak, count.saturating_sub(busy));
    }

    /// Marks one workload CQL operation as in flight until the guard is dropped.
    pub fn cql_enter(&self) -> ActivityGuard<'_> {
        self.enter_activity(&self.cql_in_flight, &self.cql_in_flight_peak, None)
    }

    /// Marks a generic database operation as in flight. Scylla keeps using the
    /// compatibility-named `cql_enter`; PostgreSQL reports these values as DB
    /// in-flight metrics in its JSON output.
    pub fn database_enter(&self) -> ActivityGuard<'_> {
        self.enter_activity(&self.cql_in_flight, &self.cql_in_flight_peak, None)
    }

    /// Marks one DB worker as busy until the guard is dropped.
    pub fn worker_enter(&self) -> ActivityGuard<'_> {
        self.enter_activity(
            &self.workers_busy,
            &self.workers_busy_peak,
            Some(&self.workers_idle_peak),
        )
    }

    fn enter_activity<'a>(
        &'a self,
        current: &'a AtomicUsize,
        peak: &'a AtomicUsize,
        idle_peak: Option<&'a AtomicUsize>,
    ) -> ActivityGuard<'a> {
        if !self.is_enabled() {
            return ActivityGuard {
                current: None,
                idle_peak: None,
                worker_count: None,
                enabled: None,
            };
        }

        let next = current.fetch_add(1, Ordering::AcqRel).saturating_add(1);
        atomic_max(peak, next);
        if let Some(idle_peak) = idle_peak {
            let total = self.worker_count.load(Ordering::Acquire);
            atomic_max(idle_peak, total.saturating_sub(next));
        }
        ActivityGuard {
            current: Some(current),
            idle_peak,
            worker_count: idle_peak.map(|_| &self.worker_count),
            enabled: Some(&self.enabled),
        }
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let stages = lock(&self.stages)
            .iter()
            .map(|(name, histogram)| ((*name).to_owned(), histogram.snapshot()))
            .collect();
        let counters = lock(&self.counters)
            .iter()
            .map(|(name, count)| ((*name).to_owned(), *count))
            .collect();
        let worker_count = self.worker_count.load(Ordering::Acquire);
        let workers_busy_current = self.workers_busy.load(Ordering::Acquire);

        MetricsSnapshot {
            enabled: self.is_enabled(),
            stages,
            counters,
            cql_in_flight_current: self.cql_in_flight.load(Ordering::Acquire),
            cql_in_flight_peak: self.cql_in_flight_peak.load(Ordering::Acquire),
            workers_total: worker_count,
            workers_busy_current,
            workers_busy_peak: self.workers_busy_peak.load(Ordering::Acquire),
            workers_idle_current: worker_count.saturating_sub(workers_busy_current),
            workers_idle_peak: self.workers_idle_peak.load(Ordering::Acquire),
        }
    }
}

#[derive(Debug)]
pub struct ActivityGuard<'a> {
    current: Option<&'a AtomicUsize>,
    idle_peak: Option<&'a AtomicUsize>,
    worker_count: Option<&'a AtomicUsize>,
    enabled: Option<&'a AtomicBool>,
}

impl Drop for ActivityGuard<'_> {
    fn drop(&mut self) {
        if let Some(current) = self.current {
            let remaining = current.fetch_sub(1, Ordering::AcqRel).saturating_sub(1);
            if let (Some(idle_peak), Some(worker_count), Some(enabled)) =
                (self.idle_peak, self.worker_count, self.enabled)
                && enabled.load(Ordering::Acquire)
            {
                let total = worker_count.load(Ordering::Acquire);
                atomic_max(idle_peak, total.saturating_sub(remaining));
            }
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct StageSnapshot {
    pub count: u64,
    pub min_ns: u64,
    pub mean_ns: u64,
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: u64,
    pub max_ns: u64,
    pub overflow_count: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct MetricsSnapshot {
    pub enabled: bool,
    pub stages: BTreeMap<String, StageSnapshot>,
    pub counters: BTreeMap<String, u64>,
    pub cql_in_flight_current: usize,
    pub cql_in_flight_peak: usize,
    pub workers_total: usize,
    pub workers_busy_current: usize,
    pub workers_busy_peak: usize,
    pub workers_idle_current: usize,
    pub workers_idle_peak: usize,
}

fn atomic_max(target: &AtomicUsize, value: usize) {
    let mut current = target.load(Ordering::Relaxed);
    while value > current {
        match target.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(actual) => current = actual,
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::Metrics;
    use std::time::Duration;

    #[test]
    fn disabled_metrics_do_not_collect_samples_or_counters() {
        let metrics = Metrics::new();
        metrics.observe("db_read", Duration::from_micros(10));
        metrics.increment_counter("accepted_requests");
        let snapshot = metrics.snapshot();
        assert!(snapshot.stages.is_empty());
        assert!(snapshot.counters.is_empty());
    }

    #[test]
    fn enabled_samples_guards_and_reset_have_consistent_counts() {
        let metrics = Metrics::new();
        metrics.set_worker_count(3);
        metrics.reset();
        metrics.enable();
        metrics.observe("db_read", Duration::from_micros(10));
        metrics.increment_counter("accepted_requests");
        {
            let _cql = metrics.cql_enter();
            let _worker = metrics.worker_enter();
            let active = metrics.snapshot();
            assert_eq!(active.cql_in_flight_current, 1);
            assert_eq!(active.cql_in_flight_peak, 1);
            assert_eq!(active.workers_busy_current, 1);
            assert_eq!(active.workers_idle_current, 2);
        }

        let snapshot = metrics.snapshot();
        let stage = snapshot.stages.get("db_read").unwrap();
        assert_eq!(stage.count, 1);
        assert_eq!(stage.min_ns, 10_000);
        assert_eq!(stage.mean_ns, 10_000);
        assert_eq!(stage.max_ns, 10_000);
        assert_eq!(snapshot.counters.get("accepted_requests"), Some(&1));
        assert_eq!(snapshot.cql_in_flight_current, 0);
        assert_eq!(snapshot.workers_busy_current, 0);
        assert_eq!(snapshot.workers_idle_current, 3);
        assert_eq!(snapshot.workers_busy_peak, 1);
        assert_eq!(snapshot.workers_idle_peak, 3);

        metrics.reset();
        let reset = metrics.snapshot();
        assert!(!reset.enabled);
        assert!(reset.stages.is_empty());
        assert_eq!(reset.counters.get("cql_read_metadata"), Some(&0));
        assert_eq!(reset.counters.get("cql_read_ledger"), Some(&0));
        assert_eq!(reset.counters.get("cql_write_lwt"), Some(&0));
        assert_eq!(reset.counters.get("accepted_requests"), Some(&0));
        assert_eq!(reset.counters.len(), 4);
        assert_eq!(reset.workers_total, 3);
        assert_eq!(reset.workers_busy_current, 0);
        assert_eq!(reset.workers_idle_current, 3);
        assert_eq!(reset.workers_idle_peak, 0);
    }

    #[test]
    fn postgres_reset_reports_only_postgres_and_shared_counters() {
        let metrics = Metrics::new();
        metrics.reset_postgres();
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.counters.get("pg_function_calls"), Some(&0));
        assert_eq!(snapshot.counters.get("pg_ledger_insert_attempts"), Some(&0));
        assert_eq!(snapshot.counters.get("pg_account_updates"), Some(&0));
        assert_eq!(snapshot.counters.get("pg_duplicate_conflicts"), Some(&0));
        assert_eq!(snapshot.counters.get("accepted_requests"), Some(&0));
        assert_eq!(snapshot.counters.len(), 5);
        assert!(!snapshot.counters.contains_key("cql_write_lwt"));
        assert!(!snapshot.counters.contains_key("cql_read_metadata"));
        assert!(!snapshot.counters.contains_key("cql_read_ledger"));
    }
}
