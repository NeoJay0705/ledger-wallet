//! Bounded configuration for the formal ledger pipeline.

use std::path::PathBuf;
use std::time::Duration;

pub const MAX_SHARDS: u32 = 64;
pub const MAX_BATCH_SIZE: usize = 2_048;
pub const MAX_GLOBAL_QUEUE: usize = 1_000_000;
pub const MAX_GLOBAL_IN_FLIGHT: usize = 1_000_000;
pub const MAX_HISTORY_IO_CONCURRENCY: usize = 32;
pub const DEFAULT_ROCKSDB_MEMORY_BUDGET_BYTES: usize = 128 * 1024 * 1024;
pub const DEFAULT_ROCKSDB_BACKGROUND_JOBS: i32 = 8;
pub const DEFAULT_MIN_FREE_DISK_BYTES: usize = 512 * 1024 * 1024;
pub const DEFAULT_MIN_AVAILABLE_MEMORY_BYTES: usize = 256 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DatabaseLayout {
    Shared,
    Dedicated,
}

impl DatabaseLayout {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Shared => "shared",
            Self::Dedicated => "dedicated",
        }
    }
}

#[derive(Clone, Debug)]
pub struct PipelineConfig {
    pub data_dir: PathBuf,
    pub shard_count: u32,
    pub database_layout: DatabaseLayout,
    /// Total Tokio channel capacity divided deterministically among shards.
    pub global_queue_capacity: usize,
    /// Bounds both requests waiting for queue capacity and accepted work across
    /// the whole process. Permits follow accepted work until its response is
    /// observed or dropped.
    pub global_in_flight_limit: usize,
    pub batch_size: usize,
    pub batch_flush_timeout: Duration,
    pub admission_timeout: Duration,
    pub response_timeout: Duration,
    pub history_io_concurrency: usize,
    pub history_operation_timeout: Duration,
    /// Process-wide cap shared by foreground batches and concrete source
    /// storage operations. It is never held while waiting for a fence or for
    /// history IO.
    pub global_storage_batches: usize,
    pub projector_batch_size: usize,
    pub gc_batch_size: usize,
    pub retention: Duration,
    pub watermark_interval: Duration,
    pub blocked_gc_sleep: Duration,
    pub shutdown_timeout: Duration,
    pub index_lookup_group_size: usize,
    pub index_lookup_max_in_flight: usize,
    /// Total source DB memory budget. Dedicated DBs divide these budgets by
    /// shard count; they do not each receive the full value.
    pub rocksdb_write_buffer_budget_bytes: usize,
    pub rocksdb_block_cache_budget_bytes: usize,
    pub rocksdb_background_jobs_budget: i32,
    /// Startup admission threshold checked against the data directory's
    /// filesystem. Zero disables this check for focused tests.
    pub startup_min_free_disk_bytes: usize,
    /// Startup admission threshold from Linux MemAvailable. Zero disables
    /// this check for focused tests; it is not a lifetime memory cap.
    pub startup_min_available_memory_bytes: usize,
}

impl PipelineConfig {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            shard_count: 1,
            database_layout: DatabaseLayout::Shared,
            global_queue_capacity: 50_000,
            global_in_flight_limit: 50_000,
            batch_size: 2_048,
            batch_flush_timeout: Duration::from_millis(5),
            admission_timeout: Duration::from_secs(5),
            response_timeout: Duration::from_secs(30),
            history_io_concurrency: 4,
            history_operation_timeout: Duration::from_secs(5),
            global_storage_batches: 4,
            projector_batch_size: 256,
            gc_batch_size: 256,
            retention: Duration::from_secs(60),
            watermark_interval: Duration::from_secs(1),
            blocked_gc_sleep: Duration::from_millis(100),
            shutdown_timeout: Duration::from_secs(30),
            index_lookup_group_size: 256,
            index_lookup_max_in_flight: 4,
            rocksdb_write_buffer_budget_bytes: DEFAULT_ROCKSDB_MEMORY_BUDGET_BYTES,
            rocksdb_block_cache_budget_bytes: DEFAULT_ROCKSDB_MEMORY_BUDGET_BYTES,
            rocksdb_background_jobs_budget: DEFAULT_ROCKSDB_BACKGROUND_JOBS,
            startup_min_free_disk_bytes: DEFAULT_MIN_FREE_DISK_BYTES,
            startup_min_available_memory_bytes: DEFAULT_MIN_AVAILABLE_MEMORY_BYTES,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.data_dir.as_os_str().is_empty() {
            return Err("data directory must not be empty".to_owned());
        }
        if !(1..=MAX_SHARDS).contains(&self.shard_count) {
            return Err(format!("shard count must be in 1..={MAX_SHARDS}"));
        }
        let shards = self.shard_count as usize;
        if self.global_queue_capacity < shards || self.global_queue_capacity > MAX_GLOBAL_QUEUE {
            return Err(format!(
                "global queue capacity must be in shard_count..={MAX_GLOBAL_QUEUE}"
            ));
        }
        if self.global_in_flight_limit < shards
            || self.global_in_flight_limit > MAX_GLOBAL_IN_FLIGHT
        {
            return Err(format!(
                "global in-flight limit must be in shard_count..={MAX_GLOBAL_IN_FLIGHT}"
            ));
        }
        if self.batch_size == 0 || self.batch_size > MAX_BATCH_SIZE {
            return Err(format!("batch size must be in 1..={MAX_BATCH_SIZE}"));
        }
        if self.batch_flush_timeout.is_zero() || self.batch_flush_timeout > Duration::from_secs(5) {
            return Err("batch flush timeout must be in 1ns..=5s".to_owned());
        }
        for (name, timeout) in [
            ("admission timeout", self.admission_timeout),
            ("response timeout", self.response_timeout),
            ("shutdown timeout", self.shutdown_timeout),
        ] {
            if timeout.is_zero() || timeout > Duration::from_secs(300) {
                return Err(format!("{name} must be in 1ns..=300s"));
            }
        }
        if self.history_io_concurrency == 0
            || self.history_io_concurrency > MAX_HISTORY_IO_CONCURRENCY
        {
            return Err(format!(
                "history IO concurrency must be in 1..={MAX_HISTORY_IO_CONCURRENCY}"
            ));
        }
        if self.global_storage_batches == 0 || self.global_storage_batches > 256 {
            return Err("global storage batches must be in 1..=256".to_owned());
        }
        if self.history_operation_timeout.is_zero()
            || self.history_operation_timeout > Duration::from_secs(300)
        {
            return Err("history operation timeout must be in 1ns..=300s".to_owned());
        }
        for (name, batch) in [
            ("projector batch size", self.projector_batch_size),
            ("GC batch size", self.gc_batch_size),
        ] {
            if batch == 0 || batch > MAX_BATCH_SIZE {
                return Err(format!("{name} must be in 1..={MAX_BATCH_SIZE}"));
            }
        }
        if self.retention.is_zero() || self.retention > Duration::from_secs(31_536_000) {
            return Err("retention must be in 1ns..=365d".to_owned());
        }
        if self.watermark_interval.is_zero() || self.watermark_interval > Duration::from_secs(60) {
            return Err("watermark interval must be in 1ns..=60s".to_owned());
        }
        if self.blocked_gc_sleep.is_zero() || self.blocked_gc_sleep > Duration::from_secs(60) {
            return Err("blocked GC sleep must be in 1ns..=60s".to_owned());
        }
        if self.index_lookup_group_size == 0 || self.index_lookup_group_size > MAX_BATCH_SIZE {
            return Err(format!(
                "index lookup group size must be in 1..={MAX_BATCH_SIZE}"
            ));
        }
        if !(1..=8).contains(&self.index_lookup_max_in_flight) {
            return Err("index lookup max in-flight groups must be in 1..=8".to_owned());
        }
        if self.rocksdb_write_buffer_budget_bytes < 4 * 1024 * 1024
            || self.rocksdb_write_buffer_budget_bytes > 64 * 1024 * 1024 * 1024
        {
            return Err("RocksDB write buffer budget must be in 4MiB..=64GiB".to_owned());
        }
        if self.rocksdb_block_cache_budget_bytes < 8 * 1024 * 1024
            || self.rocksdb_block_cache_budget_bytes > 64 * 1024 * 1024 * 1024
        {
            return Err("RocksDB block cache budget must be in 8MiB..=64GiB".to_owned());
        }
        let required_jobs = match self.database_layout {
            DatabaseLayout::Shared => 1,
            DatabaseLayout::Dedicated => self.shard_count as i32,
        };
        if self.rocksdb_background_jobs_budget < required_jobs
            || self.rocksdb_background_jobs_budget > 1_024
        {
            return Err(format!(
                "RocksDB background jobs budget must be in {required_jobs}..=1024 for this layout"
            ));
        }
        for (name, threshold) in [
            ("minimum free disk", self.startup_min_free_disk_bytes),
            (
                "minimum available memory",
                self.startup_min_available_memory_bytes,
            ),
        ] {
            if threshold > 1024 * 1024 * 1024 * 1024 {
                return Err(format!("{name} threshold must be 0..=1TiB"));
            }
        }
        let databases = match self.database_layout {
            DatabaseLayout::Shared => 1,
            DatabaseLayout::Dedicated => shards,
        };
        let write_bytes_per_db = self.rocksdb_write_buffer_budget_bytes / databases / 2;
        let cache_bytes_per_db = self.rocksdb_block_cache_budget_bytes / databases;
        // Two write buffers and a nonempty block cache are required by the
        // storage opener. The write-buffer budget includes both buffers.
        if write_bytes_per_db < 1 * 1024 * 1024 || cache_bytes_per_db < 1 * 1024 * 1024 {
            return Err(
                "RocksDB memory budgets are too small after division across the configured DBs"
                    .to_owned(),
            );
        }
        Ok(())
    }

    /// Split a process-wide queue bound without rounding the total upward.
    pub fn queue_capacity_for_shard(&self, shard_id: u32) -> usize {
        let count = self.shard_count as usize;
        let id = shard_id as usize;
        self.global_queue_capacity / count + usize::from(id < self.global_queue_capacity % count)
    }
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self::new("ledger-data")
    }
}
