#![recursion_limit = "256"]

use async_trait::async_trait;
use cpu_time::ProcessTime;
use futures::{StreamExt, TryStreamExt};
use hdrhistogram::Histogram;
use ledger_wallet_scylla_bench::metrics::Metrics;
use ledger_wallet_scylla_bench::{
    AccountState, CommitResolution, ConsistencyLevel, DriverCounters, Error, LedgerRecord,
    Operation, Outcome, PostgresOperationCounters, PostgresStore, ScyllaStore, Service,
    Transaction, TransactionResult, WalletStore, WorkerRouting,
};
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{Barrier, Notify};
use tokio::task::JoinSet;
use tokio::time::{sleep, sleep_until};

const DEFAULT_SHARDS: usize = 1;
const DEFAULT_DB_WORKERS: usize = 12;
const DEFAULT_USERS: usize = 50_000;
const DEFAULT_REQUESTS_PER_USER: usize = 200;
const DEFAULT_QUEUE_CAPACITY: usize = 50_000;
const DEFAULT_MODE: &str = "smoke";
const START_GATE_DEADLINE: Duration = Duration::from_secs(180);
const FINISH_GATE_DEADLINE: Duration = Duration::from_secs(180);
const START_GATE_POLL: Duration = Duration::from_millis(25);
const E2E_STRIPES: usize = 16;
const E2E_RANGE_NS: u64 = 600_000_000_000;
const HISTOGRAM_SIGNIFICANT_FIGURES: u8 = 3;
const HEARTBEAT_PERIOD: Duration = Duration::from_millis(1);
const CONNECTION_SAMPLE_PERIOD: Duration = Duration::from_millis(100);
const PROCESS_IO_PATH: &str = "/proc/self/io";
const DEFAULT_POSTGRES_WORKERS: usize = 512;
const DEFAULT_POSTGRES_CONNECTIONS: usize = 512;
const DEFAULT_POSTGRES_CPUS: usize = 2;
const POSTGRES_SETUP_CONCURRENCY: usize = 6;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Backend {
    Scylla,
    Postgres,
}

impl Backend {
    fn name(self) -> &'static str {
        match self {
            Self::Scylla => "scylla",
            Self::Postgres => "postgres",
        }
    }

    fn wallet_profile(self) -> &'static str {
        match self {
            Self::Scylla => ScyllaStore::WALLET_PROFILE,
            Self::Postgres => PostgresStore::WALLET_PROFILE,
        }
    }
}

#[derive(Clone)]
struct Config {
    backend: Backend,
    nodes: Vec<String>,
    keyspace: String,
    shards: usize,
    db_workers: usize,
    users: usize,
    requests_per_user: usize,
    queue_capacity: usize,
    output_dir: PathBuf,
    mode: String,
    start_gate: Option<PathBuf>,
    finish_gate: Option<PathBuf>,
    total_requests: u64,
    per_shard_pool: usize,
    requested_per_shard_pool: Option<usize>,
    per_shard_pool_overridden: bool,
    data_connection_target: usize,
    worker_routing: WorkerRouting,
    setup_concurrency: usize,
    per_worker_queue_capacity: usize,
    replication_factor: u8,
    read_consistency: ConsistencyLevel,
    write_consistency: ConsistencyLevel,
    pg_dsn: Option<String>,
    pg_schema: Option<String>,
    pg_connections: usize,
    db_cpu_count: usize,
}

struct EngineStore {
    inner: EngineStoreKind,
}

enum EngineStoreKind {
    Scylla(Arc<ScyllaStore>),
    Postgres(Arc<PostgresStore>),
}

impl EngineStore {
    async fn connect(config: &Config, metrics: Arc<Metrics>) -> Result<Arc<Self>, Error> {
        let inner = match config.backend {
            Backend::Scylla => EngineStoreKind::Scylla(
                ScyllaStore::connect_with_profile(
                    &config.nodes,
                    &config.keyspace,
                    config.shards,
                    config.per_shard_pool,
                    config.replication_factor,
                    config.read_consistency,
                    config.write_consistency,
                    metrics,
                )
                .await?,
            ),
            Backend::Postgres => EngineStoreKind::Postgres(
                PostgresStore::connect(
                    config.pg_dsn.as_deref().ok_or_else(|| {
                        Error::InvalidConfiguration("PostgreSQL DSN is required".to_owned())
                    })?,
                    config.pg_schema.as_deref().ok_or_else(|| {
                        Error::InvalidConfiguration("PostgreSQL schema is required".to_owned())
                    })?,
                    config.pg_connections,
                    metrics,
                )
                .await?,
            ),
        };
        Ok(Arc::new(Self { inner }))
    }

    fn session_id(&self) -> String {
        match &self.inner {
            EngineStoreKind::Scylla(store) => store.session_id(),
            EngineStoreKind::Postgres(store) => store.session_id(),
        }
    }

    fn data_connections(&self) -> u64 {
        match &self.inner {
            EngineStoreKind::Scylla(store) => store.data_connections(),
            EngineStoreKind::Postgres(store) => store.data_connections(),
        }
    }

    fn driver_counters(&self) -> DriverCounters {
        match &self.inner {
            EngineStoreKind::Scylla(store) => store.driver_counters(),
            EngineStoreKind::Postgres(store) => store.driver_counters(),
        }
    }

    async fn seed_account(&self, account_id: u64, balance: u64) -> Result<(), Error> {
        match &self.inner {
            EngineStoreKind::Scylla(store) => store.seed_account(account_id, balance).await,
            EngineStoreKind::Postgres(store) => store.seed_account(account_id, balance).await,
        }
    }

    async fn verify_account(
        &self,
        account_id: u64,
    ) -> Result<(AccountState, Vec<LedgerRecord>), Error> {
        match &self.inner {
            EngineStoreKind::Scylla(store) => store.verify_account(account_id).await,
            EngineStoreKind::Postgres(store) => store.verify_account(account_id).await,
        }
    }

    fn postgres_metadata(&self) -> Option<Value> {
        match &self.inner {
            EngineStoreKind::Scylla(_) => None,
            EngineStoreKind::Postgres(store) => Some(json!({
                "database_name": "walletbench",
                "schema_name": store.schema(),
                "schema_sql_file": PostgresStore::SQL_FILE,
                "schema_sql_sha256": store.schema_sql_sha256(),
                "function_name": PostgresStore::FUNCTION_NAME,
                "application_name": PostgresStore::APPLICATION_NAME,
                "accounts_table": "accounts",
                "accounts_primary_key_constraint": "accounts_pkey",
                "ledger_table": "ledger",
                "ledger_primary_key_constraint": "ledger_pkey",
                "ledger_primary_key": ["account_id", "tx_id"],
                "connection_settings": store.connection_settings(),
                "client_query_timeout_ms": PostgresStore::CLIENT_QUERY_TIMEOUT.as_millis(),
                "pool_wait_excluded_from_client_timeout": true,
                "implicit_transaction_ack": "query_one completion after PostgreSQL ReadyForQuery"
            })),
        }
    }

    fn postgres_operation_counters(&self) -> Option<PostgresOperationCounters> {
        match &self.inner {
            EngineStoreKind::Scylla(_) => None,
            EngineStoreKind::Postgres(store) => Some(store.operation_counters()),
        }
    }

    fn postgres_backend_pids(&self) -> Option<Vec<i32>> {
        match &self.inner {
            EngineStoreKind::Scylla(_) => None,
            EngineStoreKind::Postgres(store) => Some(
                store
                    .connection_settings()
                    .into_iter()
                    .map(|settings| settings.backend_pid)
                    .collect(),
            ),
        }
    }
}

#[async_trait]
impl WalletStore for EngineStore {
    async fn preload_state(&self, account_id: u64) -> Result<AccountState, Error> {
        match &self.inner {
            EngineStoreKind::Scylla(store) => store.preload_state(account_id).await,
            EngineStoreKind::Postgres(store) => store.preload_state(account_id).await,
        }
    }

    async fn commit(
        &self,
        account_id: u64,
        expected_state: &AccountState,
        next_state: &AccountState,
        record: &LedgerRecord,
    ) -> Result<CommitResolution, Error> {
        match &self.inner {
            EngineStoreKind::Scylla(store) => {
                store
                    .commit(account_id, expected_state, next_state, record)
                    .await
            }
            EngineStoreKind::Postgres(store) => {
                store
                    .commit(account_id, expected_state, next_state, record)
                    .await
            }
        }
    }
}

#[derive(Debug)]
struct ParseError(String);

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

enum ParseResult {
    Help,
    Config(Config),
}

struct EventLog {
    writer: BufWriter<File>,
    program_started: Instant,
    config: Config,
    session_id: Option<String>,
}

impl EventLog {
    fn create(config: &Config) -> Result<Self, String> {
        fs::create_dir_all(&config.output_dir)
            .map_err(|error| format!("create output directory: {error}"))?;
        let path = config.output_dir.join("events.jsonl");
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|error| format!("create {}: {error}", path.display()))?;
        Ok(Self {
            writer: BufWriter::new(file),
            program_started: Instant::now(),
            config: config.clone(),
            session_id: None,
        })
    }

    fn set_session_id(&mut self, session_id: String) {
        self.session_id = Some(session_id);
    }

    fn emit(&mut self, phase: &str, extra: Value) -> Result<(), String> {
        let mut event = Map::new();
        event.insert("phase".to_owned(), json!(phase));
        event.insert("timestamp_unix_ns".to_owned(), json!(unix_time_ns()?));
        event.insert(
            "elapsed_ns".to_owned(),
            json!(duration_ns(self.program_started.elapsed())),
        );
        event.insert("session_id".to_owned(), json!(self.session_id));
        event.insert("backend".to_owned(), json!(self.config.backend.name()));
        event.insert(
            "wallet_profile".to_owned(),
            json!(self.config.backend.wallet_profile()),
        );
        event.insert("mode".to_owned(), json!(self.config.mode));
        event.insert("C".to_owned(), json!(self.config.db_workers));
        match self.config.backend {
            Backend::Scylla => {
                event.insert("keyspace".to_owned(), json!(self.config.keyspace));
                event.insert("node_count".to_owned(), json!(self.config.nodes.len()));
                event.insert("S".to_owned(), json!(self.config.shards));
                event.insert("P".to_owned(), json!(self.config.per_shard_pool));
                event.insert(
                    "replication_factor".to_owned(),
                    json!(self.config.replication_factor),
                );
                event.insert(
                    "read_cl".to_owned(),
                    json!(self.config.read_consistency.canonical_name()),
                );
                event.insert(
                    "write_cl".to_owned(),
                    json!(self.config.write_consistency.canonical_name()),
                );
                event.insert(
                    "ledger_key".to_owned(),
                    json!(["account_id", "kind", "tx_id"]),
                );
                event.insert(
                    "serial_cl".to_owned(),
                    json!(ScyllaStore::SERIAL_CONSISTENCY),
                );
                event.insert(
                    "cql_requests_per_transaction".to_owned(),
                    json!(ScyllaStore::CQL_REQUESTS_PER_TRANSACTION),
                );
                event.insert(
                    "cql_statement_count".to_owned(),
                    json!(ScyllaStore::CQL_STATEMENT_COUNT),
                );
                event.insert(
                    "per_shard_pool_overridden".to_owned(),
                    json!(self.config.per_shard_pool_overridden),
                );
                event.insert(
                    "requested_per_shard_pool".to_owned(),
                    json!(self.config.requested_per_shard_pool),
                );
                event.insert("nodes".to_owned(), json!(self.config.nodes));
            }
            Backend::Postgres => {
                event.insert("database_name".to_owned(), json!("walletbench"));
                event.insert("pg_schema".to_owned(), json!(self.config.pg_schema));
                event.insert(
                    "pg_connections".to_owned(),
                    json!(self.config.pg_connections),
                );
                event.insert("db_cpu_count".to_owned(), json!(self.config.db_cpu_count));
                event.insert("schema_sql_file".to_owned(), json!(PostgresStore::SQL_FILE));
                event.insert(
                    "schema_sql_sha256".to_owned(),
                    json!(postgres_schema_sql_sha256()),
                );
                event.insert(
                    "application_name".to_owned(),
                    json!(PostgresStore::APPLICATION_NAME),
                );
                event.insert("ledger_key".to_owned(), json!(["account_id", "tx_id"]));
                event.insert("accounts_table".to_owned(), json!("accounts"));
                event.insert(
                    "accounts_primary_key_constraint".to_owned(),
                    json!("accounts_pkey"),
                );
                event.insert("ledger_table".to_owned(), json!("ledger"));
                event.insert(
                    "ledger_primary_key_constraint".to_owned(),
                    json!("ledger_pkey"),
                );
                event.insert(
                    "ledger_primary_key".to_owned(),
                    json!(["account_id", "tx_id"]),
                );
                event.insert("sql_function_calls_per_transaction".to_owned(), json!(1));
                event.insert(
                    "client_query_timeout_ms".to_owned(),
                    json!(PostgresStore::CLIENT_QUERY_TIMEOUT.as_millis()),
                );
            }
        }
        let account_state_source = if matches!(
            phase,
            "ready_to_measure"
                | "measuring"
                | "client_end"
                | "drained"
                | "verifying"
                | "verified"
                | "awaiting_runner_audit"
                | "runner_audit_released"
                | "finish_gate_timeout"
        ) {
            "memory"
        } else {
            "db"
        };
        event.insert(
            "account_state_source".to_owned(),
            json!(account_state_source),
        );
        event.insert(
            "data_connection_target".to_owned(),
            json!(self.config.data_connection_target),
        );
        event.insert(
            "worker_routing".to_owned(),
            json!(worker_routing_name(self.config.worker_routing)),
        );
        event.insert(
            "setup_concurrency".to_owned(),
            json!(self.config.setup_concurrency),
        );
        event.insert(
            "state_preload_concurrency".to_owned(),
            json!(self.config.setup_concurrency),
        );
        event.insert(
            "per_worker_queue_capacity".to_owned(),
            json!(self.config.per_worker_queue_capacity),
        );
        event.insert(
            "global_admission_capacity".to_owned(),
            json!(self.config.queue_capacity),
        );
        event.insert("users".to_owned(), json!(self.config.users));
        event.insert(
            "requests_per_user".to_owned(),
            json!(self.config.requests_per_user),
        );
        event.insert(
            "total_requests".to_owned(),
            json!(self.config.total_requests),
        );
        event.insert("finish_gate".to_owned(), json!(self.config.finish_gate));
        if let Some(extra) = extra.as_object() {
            for (key, value) in extra {
                event.insert(key.clone(), value.clone());
            }
        }
        serde_json::to_writer(&mut self.writer, &Value::Object(event))
            .map_err(|error| format!("serialize event {phase}: {error}"))?;
        self.writer
            .write_all(b"\n")
            .and_then(|()| self.writer.flush())
            .map_err(|error| format!("write event {phase}: {error}"))
    }
}

#[derive(Clone, Copy, Debug)]
struct IoCounters {
    rchar: u64,
    wchar: u64,
    syscr: u64,
    syscw: u64,
    read_bytes: u64,
    write_bytes: u64,
    cancelled_write_bytes: u64,
}

#[derive(Debug)]
struct LatencyStripes {
    stripes: Vec<Mutex<Histogram<u64>>>,
    observed_max_ns: AtomicU64,
    overflow_count: AtomicU64,
}

impl LatencyStripes {
    fn new() -> Self {
        let stripes = (0..E2E_STRIPES)
            .map(|_| {
                Mutex::new(
                    Histogram::new_with_bounds(1, E2E_RANGE_NS, HISTOGRAM_SIGNIFICANT_FIGURES)
                        .expect("fixed end-to-end histogram bounds are valid"),
                )
            })
            .collect();
        Self {
            stripes,
            observed_max_ns: AtomicU64::new(0),
            overflow_count: AtomicU64::new(0),
        }
    }

    fn record(&self, account_id: u64, latency: Duration) {
        let nanos = duration_ns(latency).max(1);
        atomic_max_u64(&self.observed_max_ns, nanos);
        let recorded = nanos.min(E2E_RANGE_NS);
        if nanos > E2E_RANGE_NS {
            self.overflow_count.fetch_add(1, Ordering::Relaxed);
        }
        let stripe = account_id as usize % E2E_STRIPES;
        lock(&self.stripes[stripe])
            .record(recorded)
            .expect("latency was clamped to configured histogram bounds");
    }

    fn snapshot(&self) -> Result<Value, String> {
        let mut combined =
            Histogram::new_with_bounds(1, E2E_RANGE_NS, HISTOGRAM_SIGNIFICANT_FIGURES)
                .map_err(|error| format!("create combined end-to-end histogram: {error}"))?;
        for stripe in &self.stripes {
            combined
                .add(&*lock(stripe))
                .map_err(|error| format!("merge end-to-end histogram: {error}"))?;
        }
        Ok(histogram_json(
            &combined,
            self.observed_max_ns.load(Ordering::Relaxed),
            self.overflow_count.load(Ordering::Relaxed),
        ))
    }
}

#[derive(Debug)]
struct ConnectionSamples {
    sample_count: u64,
    minimum: Option<u64>,
    maximum: Option<u64>,
    mismatch_count: u64,
    first_mismatch: Option<u64>,
    last_sample_offset_ns: Option<u64>,
}

impl ConnectionSamples {
    fn new() -> Self {
        Self {
            sample_count: 0,
            minimum: None,
            maximum: None,
            mismatch_count: 0,
            first_mismatch: None,
            last_sample_offset_ns: None,
        }
    }

    fn record(&mut self, value: u64, target: u64, offset_ns: u64) {
        self.sample_count = self.sample_count.saturating_add(1);
        self.minimum = Some(self.minimum.map_or(value, |old| old.min(value)));
        self.maximum = Some(self.maximum.map_or(value, |old| old.max(value)));
        self.last_sample_offset_ns = Some(offset_ns);
        if value != target {
            self.mismatch_count = self.mismatch_count.saturating_add(1);
            self.first_mismatch.get_or_insert(value);
        }
    }

    fn is_valid(&self, after_last_reply: u64, target: u64, sample_required: bool) -> bool {
        self.mismatch_count == 0
            && self.minimum.is_none_or(|value| value == target)
            && self.maximum.is_none_or(|value| value == target)
            && after_last_reply == target
            && (!sample_required || self.sample_count > 0)
    }
}

#[derive(Debug)]
struct HeartbeatSummary {
    histogram: Histogram<u64>,
    tick_count: u64,
    missed_ticks: u64,
    maximum_lag_ns: u64,
    overflow_count: u64,
    end_offset_ns: Option<u64>,
}

impl HeartbeatSummary {
    fn new() -> Self {
        Self {
            histogram: Histogram::new_with_bounds(1, 60_000_000_000, 3)
                .expect("fixed heartbeat histogram bounds are valid"),
            tick_count: 0,
            missed_ticks: 0,
            maximum_lag_ns: 0,
            overflow_count: 0,
            end_offset_ns: None,
        }
    }

    fn observe(&mut self, lag: Duration, offset_ns: u64) {
        let nanos = duration_ns(lag);
        let recorded = nanos.max(1).min(60_000_000_000);
        if nanos > 60_000_000_000 {
            self.overflow_count = self.overflow_count.saturating_add(1);
        }
        self.histogram
            .record(recorded)
            .expect("heartbeat lag was clamped to histogram bounds");
        self.tick_count = self.tick_count.saturating_add(1);
        self.maximum_lag_ns = self.maximum_lag_ns.max(nanos);
        self.end_offset_ns = Some(offset_ns);
    }

    fn json(&self) -> Value {
        json!({
            "period_ns": duration_ns(HEARTBEAT_PERIOD),
            "tick_count": self.tick_count,
            "missed_ticks": self.missed_ticks,
            "maximum_lag_ns": self.maximum_lag_ns,
            "last_sample_offset_ns": self.end_offset_ns,
            "lag_histogram": histogram_json(&self.histogram, self.maximum_lag_ns, self.overflow_count),
        })
    }
}

struct WindowCompletion {
    terminal_clients: AtomicUsize,
    expected_clients: usize,
    done: AtomicBool,
    done_notify: Notify,
    last_reply_offset_ns: AtomicU64,
}

impl WindowCompletion {
    fn new(expected_clients: usize) -> Self {
        Self {
            terminal_clients: AtomicUsize::new(0),
            expected_clients,
            done: AtomicBool::new(false),
            done_notify: Notify::new(),
            last_reply_offset_ns: AtomicU64::new(0),
        }
    }

    fn mark_done(&self) {
        self.done.store(true, Ordering::Release);
        self.done_notify.notify_waiters();
    }

    fn force_end(&self) {
        self.terminal_clients
            .store(self.expected_clients, Ordering::Release);
        self.mark_done();
    }

    fn force_end_if_incomplete(&self) -> bool {
        if self.done.load(Ordering::Acquire) {
            return false;
        }
        self.force_end();
        true
    }

    fn end_offset_ns(&self) -> u64 {
        self.last_reply_offset_ns.load(Ordering::Acquire)
    }
}

struct WindowSignals {
    started: OnceLock<Instant>,
    completion: Arc<WindowCompletion>,
    metrics: Arc<Metrics>,
    store: Option<Arc<EngineStore>>,
    driver_counters_start: Mutex<Option<(Instant, DriverCounters)>>,
    driver_counters_end: Mutex<Option<(Instant, DriverCounters)>>,
    end_candidate: Mutex<Option<(Instant, Option<(Duration, Instant)>, Option<String>)>>,
    cpu_sample_error: Mutex<Option<String>>,
    io_end: Mutex<Option<(Instant, Result<IoCounters, String>)>>,
}

impl WindowSignals {
    fn new(expected_clients: usize, metrics: Arc<Metrics>, store: Arc<EngineStore>) -> Self {
        Self::with_optional_store(expected_clients, metrics, Some(store))
    }

    #[cfg(test)]
    fn for_test(expected_clients: usize, metrics: Arc<Metrics>) -> Self {
        Self::with_optional_store(expected_clients, metrics, None)
    }

    fn with_optional_store(
        expected_clients: usize,
        metrics: Arc<Metrics>,
        store: Option<Arc<EngineStore>>,
    ) -> Self {
        Self {
            started: OnceLock::new(),
            completion: Arc::new(WindowCompletion::new(expected_clients)),
            metrics,
            store,
            driver_counters_start: Mutex::new(None),
            driver_counters_end: Mutex::new(None),
            end_candidate: Mutex::new(None),
            cpu_sample_error: Mutex::new(None),
            io_end: Mutex::new(None),
        }
    }

    fn set_started(&self, started: Instant) {
        let _ = self.started.set(started);
    }

    fn started(&self) -> Instant {
        *self
            .started
            .get()
            .expect("measurement start is set before release")
    }

    fn set_driver_counters_start(&self, sampled_at: Instant, counters: DriverCounters) {
        *lock(&self.driver_counters_start) = Some((sampled_at, counters));
    }

    fn driver_counters(&self) -> DriverCounters {
        self.store
            .as_ref()
            .map(|store| store.driver_counters())
            .unwrap_or(DriverCounters {
                queries: 0,
                errors: 0,
                retries: 0,
                connection_timeouts: 0,
                request_timeouts: 0,
            })
    }

    fn mark_terminal(&self, last_reply: Option<Instant>) {
        if let Some(reply_at) = last_reply {
            let started = self.started();
            let offset_ns = duration_ns(reply_at.saturating_duration_since(started));
            atomic_max_u64(&self.completion.last_reply_offset_ns, offset_ns);
            let mut candidate = lock(&self.end_candidate);
            if candidate
                .as_ref()
                .is_none_or(|(old, _, _)| reply_at >= *old)
            {
                let sampled_at = Instant::now();
                let cpu = match ProcessTime::try_now() {
                    Ok(now) => Some((now.as_duration(), sampled_at)),
                    Err(error) => {
                        *lock(&self.cpu_sample_error) =
                            Some(format!("read end process CPU time: {error}"));
                        None
                    }
                };
                *candidate = Some((reply_at, cpu, self.cpu_sample_error_value()));
            }
        }

        let terminal = self
            .completion
            .terminal_clients
            .fetch_add(1, Ordering::AcqRel)
            + 1;
        if terminal == self.completion.expected_clients {
            self.metrics.disable();
            let driver_sampled_at = Instant::now();
            *lock(&self.driver_counters_end) = Some((driver_sampled_at, self.driver_counters()));
            let io_result = read_process_io();
            *lock(&self.io_end) = Some((Instant::now(), io_result));
            self.completion.mark_done();
        }
    }

    fn cpu_sample_error_value(&self) -> Option<String> {
        lock(&self.cpu_sample_error).clone()
    }

    fn end_offset_ns(&self) -> u64 {
        self.completion.end_offset_ns()
    }

    fn mark_failed_end(&self) {
        self.metrics.disable();
        if lock(&self.driver_counters_end).is_none() {
            let sampled_at = Instant::now();
            *lock(&self.driver_counters_end) = Some((sampled_at, self.driver_counters()));
        }
        if lock(&self.io_end).is_none() {
            let io_result = read_process_io();
            *lock(&self.io_end) = Some((Instant::now(), io_result));
        }
        self.completion.force_end_if_incomplete();
    }
}

#[derive(Debug)]
struct ClientSample {
    global_request_id: u64,
    account_id: u64,
    step: usize,
    latency_ns: u64,
}

#[derive(Debug)]
struct ClientSummary {
    attempts: u64,
    replies: u64,
    errors: u64,
    unexpected_business_results: u64,
    operation_counts: BTreeMap<&'static str, u64>,
    outcome_counts: BTreeMap<&'static str, u64>,
    error_counts: BTreeMap<&'static str, u64>,
    error_samples: BTreeMap<&'static str, String>,
    first_error: Option<String>,
    samples: Vec<ClientSample>,
}

const ERROR_SAMPLE_MAX_BYTES: usize = 4096;
const ERROR_SAMPLE_TRUNCATION_MARKER: &str = " [truncated]";

fn bounded_error_sample(message: &str) -> String {
    if message.len() <= ERROR_SAMPLE_MAX_BYTES {
        return message.to_owned();
    }

    let mut prefix_end = ERROR_SAMPLE_MAX_BYTES - ERROR_SAMPLE_TRUNCATION_MARKER.len();
    while !message.is_char_boundary(prefix_end) {
        prefix_end -= 1;
    }

    let mut sample = String::with_capacity(ERROR_SAMPLE_MAX_BYTES);
    sample.push_str(&message[..prefix_end]);
    sample.push_str(ERROR_SAMPLE_TRUNCATION_MARKER);
    sample
}

impl ClientSummary {
    fn new() -> Self {
        Self {
            attempts: 0,
            replies: 0,
            errors: 0,
            unexpected_business_results: 0,
            operation_counts: BTreeMap::new(),
            outcome_counts: BTreeMap::new(),
            error_counts: BTreeMap::new(),
            error_samples: BTreeMap::new(),
            first_error: None,
            samples: Vec::new(),
        }
    }

    fn add_operation(&mut self, operation: &'static str) {
        *self.operation_counts.entry(operation).or_default() += 1;
    }

    fn add_outcome(&mut self, outcome: &'static str) {
        *self.outcome_counts.entry(outcome).or_default() += 1;
    }

    fn add_error(&mut self, kind: &'static str, message: String) {
        self.errors = self.errors.saturating_add(1);
        *self.error_counts.entry(kind).or_default() += 1;
        self.error_samples
            .entry(kind)
            .or_insert_with(|| bounded_error_sample(&message));
        self.first_error.get_or_insert(message);
    }

    fn merge_error_samples_into(&self, merged: &mut BTreeMap<&'static str, String>) {
        for (kind, sample) in &self.error_samples {
            merged.entry(kind).or_insert_with(|| sample.clone());
        }
    }
}

#[derive(Debug)]
struct VerificationSummary {
    accounts_expected: usize,
    memory_cache_accounts_compared: usize,
    accounts_verified: usize,
    ledger_rows_expected: u64,
    ledger_rows_seen: u64,
    failures: u64,
    first_failures: Vec<String>,
}

impl VerificationSummary {
    fn is_valid(&self) -> bool {
        self.failures == 0
            && self.accounts_verified == self.accounts_expected
            && self.memory_cache_accounts_compared == self.accounts_expected
            && self.ledger_rows_seen == self.ledger_rows_expected
    }

    fn json(&self, backend: Backend) -> Value {
        json!({
            "complete": self.accounts_verified == self.accounts_expected,
            "valid": self.is_valid(),
            "accounts_expected": self.accounts_expected,
            "account_state_source": match backend {
                Backend::Scylla => "memory_cache_vs_scylla",
                Backend::Postgres => "memory_cache_vs_postgres",
            },
            "memory_cache_accounts_compared": self.memory_cache_accounts_compared,
            "accounts_verified": self.accounts_verified,
            "ledger_rows_expected": self.ledger_rows_expected,
            "ledger_rows_seen": self.ledger_rows_seen,
            "failures": self.failures,
            "first_failures": self.first_failures,
        })
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let config = match parse_args(args) {
        Ok(ParseResult::Help) => {
            print_help();
            return;
        }
        Ok(ParseResult::Config(config)) => config,
        Err(error) => {
            eprintln!("{error}\n\nRun with --help for usage.");
            std::process::exit(2);
        }
    };

    let mut events = match EventLog::create(&config) {
        Ok(events) => events,
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2);
        }
    };
    let mut report = base_report(&config);
    let result = run_trial(&config, &mut events, &mut report).await;
    if let Err(error) = result {
        record_error(&mut report, error);
    }

    if report
        .get("valid")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        report["status"] = json!("verified");
    } else {
        report["valid"] = json!(false);
        report["status"] = json!("invalid");
    }

    let samples_path = config.output_dir.join("latency_samples.csv");
    if !samples_path.exists() {
        if let Err(error) = write_samples_csv(&samples_path, &[]) {
            record_error(&mut report, error);
        }
    }

    let result_path = config.output_dir.join("result.json");
    if let Err(error) = write_atomic_json(&result_path, &report) {
        eprintln!("write {}: {error}", result_path.display());
        std::process::exit(2);
    }
    if !report["valid"].as_bool().unwrap_or(false) {
        eprintln!("trial is invalid; see {}", result_path.display());
        std::process::exit(1);
    }
}

fn parse_args(args: Vec<String>) -> Result<ParseResult, ParseError> {
    let mut values = BTreeMap::<String, String>::new();
    let mut index = 0;
    while index < args.len() {
        let flag = &args[index];
        if flag == "--help" || flag == "-h" {
            return Ok(ParseResult::Help);
        }
        if !flag.starts_with("--") {
            return Err(ParseError(format!(
                "unexpected positional argument {flag:?}"
            )));
        }
        let key = flag.trim_start_matches("--").to_owned();
        if ![
            "backend",
            "nodes",
            "keyspace",
            "shards",
            "db-workers",
            "per-shard-pool",
            "worker-routing",
            "replication-factor",
            "read-cl",
            "write-cl",
            "users",
            "requests-per-user",
            "queue-capacity",
            "output-dir",
            "mode",
            "start-gate",
            "finish-gate",
            "pg-dsn",
            "pg-schema",
            "pg-connections",
            "db-cpus",
        ]
        .contains(&key.as_str())
        {
            return Err(ParseError(format!("unknown option {flag}")));
        }
        if values.contains_key(&key) {
            return Err(ParseError(format!(
                "option {flag} was supplied more than once"
            )));
        }
        index += 1;
        let value = args
            .get(index)
            .ok_or_else(|| ParseError(format!("option {flag} requires a value")))?;
        if value.starts_with("--") {
            return Err(ParseError(format!("option {flag} requires a value")));
        }
        values.insert(key, value.clone());
        index += 1;
    }

    let backend = match values
        .get("backend")
        .map(String::as_str)
        .unwrap_or("scylla")
    {
        "scylla" => Backend::Scylla,
        "postgres" => Backend::Postgres,
        _ => {
            return Err(ParseError(
                "--backend must be scylla or postgres".to_owned(),
            ));
        }
    };
    if backend == Backend::Postgres {
        return parse_postgres_args(values);
    }
    if ["pg-dsn", "pg-schema", "pg-connections", "db-cpus"]
        .iter()
        .any(|key| values.contains_key(*key))
    {
        return Err(ParseError(
            "PostgreSQL options require --backend postgres".to_owned(),
        ));
    }

    let nodes = required(&values, "nodes")?
        .split(',')
        .map(str::trim)
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if !matches!(nodes.len(), 1 | 3) || nodes.iter().any(String::is_empty) {
        return Err(ParseError(
            "--nodes must contain exactly one or three non-empty endpoints".to_owned(),
        ));
    }
    let distinct_nodes = nodes.iter().collect::<std::collections::BTreeSet<_>>();
    if distinct_nodes.len() != nodes.len() {
        return Err(ParseError("--nodes endpoints must be distinct".to_owned()));
    }
    let keyspace = required(&values, "keyspace")?.to_owned();
    if keyspace.len() <= "wallet_bench_".len()
        || !keyspace.starts_with("wallet_bench_")
        || !keyspace
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(ParseError(
            "--keyspace must match wallet_bench_[a-z0-9_]+".to_owned(),
        ));
    }
    let shards = parse_usize(&values, "shards", DEFAULT_SHARDS)?;
    let db_workers = parse_usize(&values, "db-workers", DEFAULT_DB_WORKERS)?;
    let replication_factor_value = parse_usize(&values, "replication-factor", 3)?;
    let replication_factor = u8::try_from(replication_factor_value)
        .map_err(|_| ParseError("--replication-factor must be between 1 and 3".to_owned()))?;
    if !(1..=3).contains(&replication_factor) {
        return Err(ParseError(
            "--replication-factor must be between 1 and 3".to_owned(),
        ));
    }
    if usize::from(replication_factor) > nodes.len() {
        return Err(ParseError(format!(
            "--replication-factor {replication_factor} must not exceed the {} configured node(s)",
            nodes.len()
        )));
    }
    let read_consistency = parse_consistency_level(&values, "read-cl", 3)?;
    let write_consistency = parse_consistency_level(&values, "write-cl", 3)?;
    for (option, consistency) in [
        ("--read-cl", read_consistency),
        ("--write-cl", write_consistency),
    ] {
        if consistency.replica_count() > replication_factor {
            return Err(ParseError(format!(
                "{option} {} must not exceed --replication-factor {replication_factor}",
                consistency.replica_count()
            )));
        }
    }
    let requested_per_shard_pool = values
        .get("per-shard-pool")
        .map(|value| {
            value.parse::<usize>().map_err(|error| {
                ParseError(format!("invalid --per-shard-pool value {value:?}: {error}"))
            })
        })
        .transpose()?;
    let worker_routing = match values
        .get("worker-routing")
        .map(String::as_str)
        .unwrap_or("round-robin")
    {
        "round-robin" => WorkerRouting::RoundRobin,
        "per-user" => WorkerRouting::PerUser,
        other => {
            return Err(ParseError(format!(
                "--worker-routing must be round-robin or per-user, got {other:?}"
            )));
        }
    };
    let users = parse_usize(&values, "users", DEFAULT_USERS)?;
    let requests_per_user = parse_usize(&values, "requests-per-user", DEFAULT_REQUESTS_PER_USER)?;
    let queue_capacity = parse_usize(&values, "queue-capacity", DEFAULT_QUEUE_CAPACITY)?;
    let output_dir = PathBuf::from(required(&values, "output-dir")?);
    let mode = values
        .get("mode")
        .cloned()
        .unwrap_or_else(|| DEFAULT_MODE.to_owned());
    if !["smoke", "pilot", "formal"].contains(&mode.as_str()) {
        return Err(ParseError(
            "--mode must be one of smoke, pilot, or formal".to_owned(),
        ));
    }
    let start_gate = values.get("start-gate").map(PathBuf::from);
    let finish_gate = values.get("finish-gate").map(PathBuf::from);

    if shards == 0 || db_workers == 0 || users == 0 || queue_capacity == 0 {
        return Err(ParseError(
            "--shards, --db-workers, --users, and --queue-capacity must be positive".to_owned(),
        ));
    }
    if requests_per_user == 0 || requests_per_user % 2 != 0 {
        return Err(ParseError(
            "--requests-per-user must be positive and even".to_owned(),
        ));
    }
    if requests_per_user > 1_000 {
        return Err(ParseError(
            "--requests-per-user must not exceed the store verification limit of 1000".to_owned(),
        ));
    }
    if i64::try_from(requests_per_user).is_err() {
        return Err(ParseError(
            "transaction ids must fit in a non-negative CQL bigint".to_owned(),
        ));
    }
    let worker_divisor = shards
        .checked_mul(nodes.len())
        .ok_or_else(|| ParseError("node count * --shards overflows usize".to_owned()))?;
    let per_shard_pool_overridden = requested_per_shard_pool.is_some();
    let per_shard_pool = match requested_per_shard_pool {
        Some(per_shard_pool) => per_shard_pool,
        None if db_workers % worker_divisor == 0 => db_workers / worker_divisor,
        None => {
            return Err(ParseError(format!(
                "--db-workers ({db_workers}) must be divisible by node count ({}) * --shards ({shards}) unless --per-shard-pool is provided",
                nodes.len()
            )));
        }
    };
    if per_shard_pool == 0 {
        return Err(ParseError("per-shard pool P must be positive".to_owned()));
    }
    let data_connection_target = worker_divisor.checked_mul(per_shard_pool).ok_or_else(|| {
        ParseError("node count * --shards * --per-shard-pool overflows usize".to_owned())
    })?;
    if worker_routing == WorkerRouting::PerUser && db_workers != users {
        return Err(ParseError(format!(
            "--worker-routing per-user requires --db-workers ({db_workers}) to equal --users ({users})"
        )));
    }
    let setup_concurrency = db_workers.min(data_connection_target);
    let per_worker_queue_capacity = match worker_routing {
        WorkerRouting::RoundRobin => queue_capacity,
        WorkerRouting::PerUser => 1,
    };
    let total_requests_usize = users
        .checked_mul(requests_per_user)
        .ok_or_else(|| ParseError("users * requests-per-user overflows usize".to_owned()))?;
    let total_requests = u64::try_from(total_requests_usize)
        .map_err(|_| ParseError("total request count does not fit in u64".to_owned()))?;
    let last_account = u64::try_from(users - 1)
        .map_err(|_| ParseError("last account id does not fit in u64".to_owned()))?;
    if i64::try_from(last_account).is_err() {
        return Err(ParseError(
            "account ids must fit in a non-negative CQL bigint".to_owned(),
        ));
    }
    Ok(ParseResult::Config(Config {
        backend,
        nodes,
        keyspace,
        shards,
        db_workers,
        users,
        requests_per_user,
        queue_capacity,
        output_dir,
        mode,
        start_gate,
        finish_gate,
        total_requests,
        per_shard_pool,
        requested_per_shard_pool,
        per_shard_pool_overridden,
        data_connection_target,
        worker_routing,
        setup_concurrency,
        per_worker_queue_capacity,
        replication_factor,
        read_consistency,
        write_consistency,
        pg_dsn: None,
        pg_schema: None,
        pg_connections: 0,
        db_cpu_count: 0,
    }))
}

fn parse_postgres_args(values: BTreeMap<String, String>) -> Result<ParseResult, ParseError> {
    for flag in [
        "nodes",
        "keyspace",
        "shards",
        "per-shard-pool",
        "worker-routing",
        "replication-factor",
        "read-cl",
        "write-cl",
    ] {
        if values.contains_key(flag) {
            return Err(ParseError(format!(
                "--{flag} is a Scylla option and cannot be used with --backend postgres"
            )));
        }
    }
    let pg_dsn = required(&values, "pg-dsn")?.to_owned();
    if pg_dsn.trim().is_empty() {
        return Err(ParseError("--pg-dsn must not be empty".to_owned()));
    }
    let pg_schema = required(&values, "pg-schema")?.to_owned();
    if pg_schema.len() <= "wallet_bench_".len()
        || !pg_schema.starts_with("wallet_bench_")
        || pg_schema.len() > 63
        || !pg_schema
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(ParseError(
            "--pg-schema must match wallet_bench_[a-z0-9_]+ and fit PostgreSQL's 63-byte identifier limit".to_owned(),
        ));
    }
    let db_workers = parse_usize(&values, "db-workers", DEFAULT_POSTGRES_WORKERS)?;
    if db_workers != DEFAULT_POSTGRES_WORKERS {
        return Err(ParseError(format!(
            "PostgreSQL profile requires --db-workers={DEFAULT_POSTGRES_WORKERS}"
        )));
    }
    let pg_connections = parse_usize(&values, "pg-connections", DEFAULT_POSTGRES_CONNECTIONS)?;
    if pg_connections < POSTGRES_SETUP_CONCURRENCY {
        return Err(ParseError(format!(
            "--pg-connections must be at least {POSTGRES_SETUP_CONCURRENCY}"
        )));
    }
    let db_cpu_count = parse_usize(&values, "db-cpus", DEFAULT_POSTGRES_CPUS)?;
    if db_cpu_count == 0 {
        return Err(ParseError("--db-cpus must be positive".to_owned()));
    }
    let users = parse_usize(&values, "users", DEFAULT_USERS)?;
    let requests_per_user = parse_usize(&values, "requests-per-user", DEFAULT_REQUESTS_PER_USER)?;
    let queue_capacity = parse_usize(&values, "queue-capacity", DEFAULT_QUEUE_CAPACITY)?;
    let output_dir = PathBuf::from(required(&values, "output-dir")?);
    let mode = values
        .get("mode")
        .cloned()
        .unwrap_or_else(|| DEFAULT_MODE.to_owned());
    if !["smoke", "pilot", "formal"].contains(&mode.as_str()) {
        return Err(ParseError(
            "--mode must be one of smoke, pilot, or formal".to_owned(),
        ));
    }
    let start_gate = values.get("start-gate").map(PathBuf::from);
    let finish_gate = values.get("finish-gate").map(PathBuf::from);
    if users == 0 || queue_capacity == 0 {
        return Err(ParseError(
            "--users and --queue-capacity must be positive".to_owned(),
        ));
    }
    if requests_per_user == 0 || requests_per_user % 2 != 0 {
        return Err(ParseError(
            "--requests-per-user must be positive and even".to_owned(),
        ));
    }
    if requests_per_user > 1_000 {
        return Err(ParseError(
            "--requests-per-user must not exceed the store verification limit of 1000".to_owned(),
        ));
    }
    if i64::try_from(requests_per_user).is_err() {
        return Err(ParseError(
            "transaction ids must fit in a non-negative PostgreSQL bigint".to_owned(),
        ));
    }
    let total_requests = users
        .checked_mul(requests_per_user)
        .and_then(|count| u64::try_from(count).ok())
        .ok_or_else(|| ParseError("users * requests-per-user does not fit in u64".to_owned()))?;
    let last_account = u64::try_from(users - 1)
        .map_err(|_| ParseError("last account id does not fit in u64".to_owned()))?;
    if i64::try_from(last_account).is_err() {
        return Err(ParseError(
            "account ids must fit in a non-negative PostgreSQL bigint".to_owned(),
        ));
    }
    Ok(ParseResult::Config(Config {
        backend: Backend::Postgres,
        nodes: Vec::new(),
        keyspace: String::new(),
        shards: 0,
        db_workers,
        users,
        requests_per_user,
        queue_capacity,
        output_dir,
        mode,
        start_gate,
        finish_gate,
        total_requests,
        per_shard_pool: 0,
        requested_per_shard_pool: None,
        per_shard_pool_overridden: false,
        data_connection_target: pg_connections,
        worker_routing: WorkerRouting::RoundRobin,
        setup_concurrency: POSTGRES_SETUP_CONCURRENCY,
        per_worker_queue_capacity: queue_capacity,
        replication_factor: 0,
        read_consistency: ConsistencyLevel::One,
        write_consistency: ConsistencyLevel::One,
        pg_dsn: Some(pg_dsn),
        pg_schema: Some(pg_schema),
        pg_connections,
        db_cpu_count,
    }))
}

fn required<'a>(values: &'a BTreeMap<String, String>, name: &str) -> Result<&'a str, ParseError> {
    values
        .get(name)
        .map(String::as_str)
        .ok_or_else(|| ParseError(format!("missing required option --{name}")))
}

fn parse_usize(
    values: &BTreeMap<String, String>,
    name: &str,
    default: usize,
) -> Result<usize, ParseError> {
    values.get(name).map_or(Ok(default), |value| {
        value
            .parse::<usize>()
            .map_err(|error| ParseError(format!("invalid --{name} value {value:?}: {error}")))
    })
}

fn parse_consistency_level(
    values: &BTreeMap<String, String>,
    name: &str,
    default: u8,
) -> Result<ConsistencyLevel, ParseError> {
    let value = match values.get(name) {
        Some(value) => value
            .parse::<u8>()
            .map_err(|error| ParseError(format!("invalid --{name} value {value:?}: {error}")))?,
        None => default,
    };
    ConsistencyLevel::from_replica_count(value)
        .ok_or_else(|| ParseError(format!("--{name} must be one of 1, 2, or 3")))
}

fn print_help() {
    println!(
        "ledger-wallet-scylla-bench\n\
         Usage: ledger-wallet-scylla-bench --backend scylla --nodes HOST:PORT[,HOST:PORT...] --keyspace NAME --output-dir DIR [options]\n\
         Usage: ledger-wallet-scylla-bench --backend postgres --pg-dsn DSN --pg-schema NAME --output-dir DIR [options]\n\
         Options:\n\
           --backend scylla|postgres   Storage engine (default: scylla)\n\
           --nodes HOST,...           One or three distinct Scylla endpoints\n\
           --shards N                 Scylla shards per node (default: 1)\n\
           --db-workers N             Maximum in-flight DB workers C (default: 12)\n\
           --per-shard-pool N         Driver pool target P per node/shard; override C divisibility\n\
           --worker-routing MODE      round-robin (default) or per-user (one worker per account)\n\
           --replication-factor N     NetworkTopologyStrategy replication factor 1..3 (default: 3)\n\
           --read-cl N                Wallet SELECT/verification consistency 1..RF (default: 3)\n\
           --write-cl N               Wallet seed/LWT consistency 1..RF (default: 3)\n\
           --users N                  Account clients (default: 50000)\n\
           --requests-per-user N      Sequential requests per account; must be even (default: 200)\n\
           --queue-capacity N         Bounded accepted-work capacity (default: 50000)\n\
           --mode smoke|pilot|formal  Trial label (default: smoke)\n\
           --start-gate PATH          Wait for runner-created gate file, up to 180 seconds\n\
           --finish-gate PATH         Keep Scylla session open for runner audit, up to 180 seconds\n\
           --pg-dsn DSN                PostgreSQL DSN (PG backend only; never written to reports)\n\
           --pg-schema NAME            Fresh wallet_bench_* schema in the walletbench database\n\
           --pg-connections D           Fixed PostgreSQL pool size (PG default: 512)\n\
           --db-cpus N                 Reserved PG server CPU count (default: 2)\n\
           PostgreSQL profile uses C=512 service workers and setup concurrency 6\n\
           Single-node runs must explicitly set --replication-factor 1 --read-cl 1 --write-cl 1\n\
           --help                     Show this help"
    );
}

fn base_report(config: &Config) -> Value {
    if config.backend == Backend::Postgres {
        return postgres_base_report(config);
    }
    json!({
        "schema_version": 1,
        "backend": "scylla",
        "valid": false,
        "status": "in_progress",
        "mode": config.mode,
        "session_id": null,
        "config": {
            "backend": "scylla",
            "nodes": config.nodes,
            "node_count": config.nodes.len(),
            "keyspace": config.keyspace,
            "C": config.db_workers,
            "S": config.shards,
            "P": config.per_shard_pool,
            "replication_factor": config.replication_factor,
            "read_cl": config.read_consistency.canonical_name(),
            "write_cl": config.write_consistency.canonical_name(),
            "data_connection_target": config.data_connection_target,
            "worker_routing": worker_routing_name(config.worker_routing),
            "per_shard_pool_overridden": config.per_shard_pool_overridden,
            "requested_per_shard_pool": config.requested_per_shard_pool,
            "setup_concurrency": config.setup_concurrency,
            "state_preload_concurrency": config.setup_concurrency,
            "account_states_preloaded": 0,
            "per_worker_queue_capacity": config.per_worker_queue_capacity,
            "users": config.users,
            "requests_per_user": config.requests_per_user,
            "total_requests_expected": config.total_requests,
            "queue_capacity": config.queue_capacity,
            "global_admission_capacity": config.queue_capacity,
            "start_gate": config.start_gate,
            "finish_gate": config.finish_gate,
            "runtime_worker_threads": 4,
            "wallet_profile": ScyllaStore::WALLET_PROFILE,
            "account_state_source": "db",
            "ledger_key": ["account_id", "kind", "tx_id"],
            "serial_cl": ScyllaStore::SERIAL_CONSISTENCY,
            "cql_requests_per_transaction": ScyllaStore::CQL_REQUESTS_PER_TRANSACTION,
            "cql_statement_count": ScyllaStore::CQL_STATEMENT_COUNT,
            "write_clock": "logical per-account monotonic sequence; not a CQL timestamp",
            "measured_select_requests": 0,
            "measured_cql_requests_per_transaction": ScyllaStore::CQL_REQUESTS_PER_TRANSACTION,
            "measured_cql_request_count": 0,
        },
        "durability_precondition": {
            "required_consistency": (config.read_consistency == config.write_consistency)
                .then(|| config.read_consistency.canonical_name()),
            "required_read_consistency": config.read_consistency.canonical_name(),
            "required_write_consistency": config.write_consistency.canonical_name(),
            "fsync_acknowledgement_replica_count": config.write_consistency.replica_count(),
            "replication_factor": config.replication_factor,
            "required_commitlog_sync": "batch fsync before write-consistency acknowledgement; wallet request is one conditional INSERT with LOCAL_SERIAL",
            "cli_verified": false,
            "runner_verification_reference": null,
            "note": "The CLI does not inspect effective ScyllaDB node settings. The runner must attach its verified commitlog and consistency report before citing this result as a formal durability result."
        },
        "limitations": [
            "Service and load generator run in one process. Process CPU includes both.",
            "The finite closed-loop workload does not measure steady-state saturation capacity.",
            "The CLI does not verify effective commitlog configuration; durability evidence belongs to the runner report.",
            "Duplicate requests return the original acknowledged ledger result without mutating wallet rows again; the conditional LWT may still persist Paxos protocol state."
        ],
        "errors": [],
        "requests": null,
        "measurement": null,
        "end_to_end_latency": null,
        "stage_metrics": null,
        "connection_pool": null,
        "heartbeat": null,
        "process_cpu": null,
        "process_io": null,
        "driver_counters": null,
        "verification": null
    })
}

fn postgres_base_report(config: &Config) -> Value {
    json!({
        "schema_version": 1,
        "backend": "postgres",
        "valid": false,
        "status": "in_progress",
        "mode": config.mode,
        "session_id": null,
        "wallet_profile": PostgresStore::WALLET_PROFILE,
        "config": {
            "backend": "postgres",
            "database_name": "walletbench",
            "pg_schema": config.pg_schema,
            "C": config.db_workers,
            "db_cpu_count": config.db_cpu_count,
            "pg_connections": config.pg_connections,
            "data_connection_target": config.data_connection_target,
            "worker_routing": worker_routing_name(config.worker_routing),
            "setup_concurrency": config.setup_concurrency,
            "state_preload_concurrency": config.setup_concurrency,
            "account_states_preloaded": 0,
            "per_worker_queue_capacity": config.per_worker_queue_capacity,
            "users": config.users,
            "requests_per_user": config.requests_per_user,
            "total_requests_expected": config.total_requests,
            "queue_capacity": config.queue_capacity,
            "global_admission_capacity": config.queue_capacity,
            "start_gate": config.start_gate,
            "finish_gate": config.finish_gate,
            "runtime_worker_threads": 4,
            "wallet_profile": PostgresStore::WALLET_PROFILE,
            "account_state_source": "db",
            "ledger_key": ["account_id", "tx_id"],
            "transaction_function": PostgresStore::FUNCTION_NAME,
            "sql_function_calls_per_transaction": 1,
            "client_query_timeout_ms": PostgresStore::CLIENT_QUERY_TIMEOUT.as_millis(),
            "measured_select_requests": 0,
            "measured_sql_transaction_count": 0,
            "measured_internal_write_operations": 0,
        },
        "durability_precondition": {
            "required_synchronous_commit": "on",
            "required_fsync": "on",
            "required_full_page_writes": "on",
            "required_commit_delay": "0",
            "required_synchronous_standby_names": "",
            "required_server_state": "primary (pg_is_in_recovery() = false)",
            "required_commit_ack": "implicit transaction completed through ReadyForQuery with synchronous_commit=on",
            "cli_connection_settings_verified": false,
            "runner_verification_reference": null,
            "note": "The CLI verifies each pool connection's effective settings. The runner must verify remaining server durability configuration and the owned PostgreSQL cluster before citing a formal durability result."
        },
        "limitations": [
            "Service and load generator run in one process. Process CPU includes both.",
            "The finite closed-loop workload does not measure steady-state saturation capacity.",
            "The owned PostgreSQL server configuration is verified by the runner.",
            "Duplicate requests read the current account state and original ledger record inside wallet_commit, without updating accounts."
        ],
        "errors": [],
        "requests": null,
        "measurement": null,
        "end_to_end_latency": null,
        "stage_metrics": null,
        "postgres": null,
        "postgres_accounting": null,
        "connection_pool": null,
        "heartbeat": null,
        "process_cpu": null,
        "process_io": null,
        "driver_counters": null,
        "verification": null
    })
}

fn postgres_schema_sql_sha256() -> String {
    use sha2::{Digest, Sha256};
    format!(
        "{:x}",
        Sha256::digest(include_bytes!("../sql/postgres_wallet.sql"))
    )
}

async fn run_trial(
    config: &Config,
    events: &mut EventLog,
    report: &mut Value,
) -> Result<(), String> {
    events.emit("startup", json!({"output_dir": config.output_dir}))?;

    let metrics = Arc::new(Metrics::new());
    let store = EngineStore::connect(config, metrics.clone())
        .await
        .map_err(|error| format!("connect to {}: {error}", config.backend.name()))?;
    let session_id = store.session_id();
    events.set_session_id(session_id.clone());
    report["session_id"] = json!(session_id);
    let pool_ready_connections = store.data_connections();
    if let Some(postgres) = store.postgres_metadata() {
        report["postgres"] = postgres;
        report["config"]["schema_sql_sha256"] = json!(postgres_schema_sql_sha256());
        put(
            report,
            "connection_pool",
            json!({
                "backend": "postgres",
                "database_name": "walletbench",
                "pg_schema": config.pg_schema,
                "target_total_data_connections": config.data_connection_target,
                "C": config.db_workers,
                "db_cpu_count": config.db_cpu_count,
                "pg_connections": config.pg_connections,
                "worker_routing": worker_routing_name(config.worker_routing),
                "setup_concurrency": config.setup_concurrency,
                "account_state_source": "db",
                "account_states_preloaded": 0,
                "state_preload_concurrency": config.setup_concurrency,
                "pool_ready_connections": pool_ready_connections,
                "samples_during_measurement": 0,
                "minimum_sampled_connections": null,
                "maximum_sampled_connections": null,
                "mismatch_samples": 0,
                "first_mismatch": null,
                "sampling_period_ns": duration_ns(CONNECTION_SAMPLE_PERIOD),
                "metric_source": "tokio-postgres persistent connection pool liveness"
            }),
        );
    } else {
        put(
            report,
            "connection_pool",
            json!({
                "node_count": config.nodes.len(),
                "target_total_data_connections": config.data_connection_target,
                "C": config.db_workers,
                "S": config.shards,
                "P": config.per_shard_pool,
                "worker_routing": worker_routing_name(config.worker_routing),
                "per_shard_pool_overridden": config.per_shard_pool_overridden,
                "requested_per_shard_pool": config.requested_per_shard_pool,
                "setup_concurrency": config.setup_concurrency,
                "account_state_source": "db",
                "account_states_preloaded": 0,
                "state_preload_concurrency": config.setup_concurrency,
                "pool_ready_connections": pool_ready_connections,
                "samples_during_measurement": 0,
                "minimum_sampled_connections": null,
                "maximum_sampled_connections": null,
                "mismatch_samples": 0,
                "first_mismatch": null,
                "sampling_period_ns": duration_ns(CONNECTION_SAMPLE_PERIOD),
                "metric_source": "Scylla Rust driver Session::get_metrics().get_total_connections()"
            }),
        );
    }
    if pool_ready_connections != config.data_connection_target as u64 {
        record_error(
            report,
            format!(
                "{} pool reported {pool_ready_connections} data connections before measurement; expected D={}",
                config.backend.name(),
                config.data_connection_target,
            ),
        );
        events.emit(
            "seeding",
            json!({"status": "skipped", "reason": "connection pool did not reach D"}),
        )?;
        return Ok(());
    }

    events.emit(
        "seeding",
        json!({"status": "started", "balance_per_account": 100, "setup_concurrency": config.setup_concurrency}),
    )?;
    let seed_result = futures::stream::iter(0..config.users)
        .map(Ok::<usize, String>)
        .try_for_each_concurrent(config.setup_concurrency, |account| {
            let store = store.clone();
            async move {
                store
                    .seed_account(account as u64, 100)
                    .await
                    .map_err(|error| format!("seed account {account}: {error}"))
            }
        })
        .await;
    if let Err(error) = seed_result {
        record_error(report, error);
        events.emit("seeding", json!({"status": "failed"}))?;
        return Ok(());
    }
    events.emit(
        "seeding",
        json!({"status": "complete", "accounts_seeded": config.users, "balance_per_account": 100, "setup_concurrency": config.setup_concurrency}),
    )?;

    events.emit(
        "preloading",
        json!({
            "status": "started",
            "account_state_source": "db",
            "accounts_expected": config.users,
            "state_preload_concurrency": config.setup_concurrency
        }),
    )?;
    let initial_states = match preload_account_states(
        store.clone() as Arc<dyn WalletStore>,
        config.users,
        config.setup_concurrency,
    )
    .await
    {
        Ok(states) => states,
        Err(error) => {
            record_error(report, error.clone());
            events.emit(
                "preloading",
                json!({"status": "failed", "account_state_source": "db", "error": error}),
            )?;
            return Ok(());
        }
    };
    put(
        report,
        "state_preload",
        json!({
            "account_state_source": "db",
            "account_states_preloaded": initial_states.len(),
            "accounts_expected": config.users,
            "state_preload_concurrency": config.setup_concurrency,
            "complete": initial_states.len() == config.users
        }),
    );
    report["config"]["account_states_preloaded"] = json!(initial_states.len());
    report["config"]["account_state_source"] = json!("memory");
    report["connection_pool"]["account_states_preloaded"] = json!(initial_states.len());
    report["connection_pool"]["account_state_source"] = json!("memory");
    events.emit(
        "preloading",
        json!({
            "status": "complete",
            "account_state_source": "db",
            "account_states_preloaded": initial_states.len(),
            "state_preload_concurrency": config.setup_concurrency
        }),
    )?;

    let service = match Service::start_with_routing(
        store.clone() as Arc<dyn WalletStore>,
        config.db_workers,
        initial_states,
        config.queue_capacity,
        config.per_worker_queue_capacity,
        config.worker_routing,
        metrics.clone(),
    ) {
        Ok(service) => service,
        Err(error) => {
            record_error(report, format!("start service: {error}"));
            return Ok(());
        }
    };

    let client_phase = run_clients(
        config,
        store.clone(),
        service.clone(),
        metrics.clone(),
        events,
        report,
    )
    .await;
    if let Err(error) = client_phase {
        record_error(report, error);
    }
    metrics.disable();
    let shutdown_result = service
        .shutdown()
        .await
        .map_err(|error| format!("drain service: {error}"));
    if let Err(error) = &shutdown_result {
        record_error(report, error.clone());
    }
    if let Err(error) = events.emit("drained", json!({"service_failed": service.failed()})) {
        record_error(report, error);
    }

    let measured = report
        .get("measurement")
        .and_then(Value::as_object)
        .is_some_and(|measurement| {
            measurement.get("started").and_then(Value::as_bool) == Some(true)
        });
    let metric_snapshot = metrics.snapshot();
    put(
        report,
        "stage_metrics",
        metrics_json(&metric_snapshot, config.backend),
    );
    if metric_snapshot
        .stages
        .values()
        .any(|stage| stage.overflow_count > 0)
    {
        record_error(
            report,
            "one or more stage histograms exceeded their 600-second range; reported quantiles are clipped".to_owned(),
        );
    }
    put(
        report,
        "histogram_memory",
        json!({
            "end_to_end_stripes": E2E_STRIPES,
            "stage_histogram_count": metric_snapshot.stages.len(),
            "range_max_ns": E2E_RANGE_NS,
            "significant_figures": HISTOGRAM_SIGNIFICANT_FIGURES,
            "estimated_bytes_per_histogram": 350 * 1024,
            "estimated_total_bytes": (E2E_STRIPES + metric_snapshot.stages.len()) * 350 * 1024,
            "raw_latency_sample_stride": 64
        }),
    );
    if measured {
        let e2e_count = report["end_to_end_latency"]["count"].as_u64().unwrap_or(0);
        record_metric_invariant_failures(report, &metric_snapshot, config, e2e_count);
    }
    if measured {
        if let Err(error) = events.emit("verifying", json!({"accounts": config.users})) {
            record_error(report, error);
        }
        let cached_states = service.cache_snapshot();
        let verification = verify_all(config, store.clone(), cached_states).await;
        let valid = verification.is_valid();
        put(report, "verification", verification.json(config.backend));
        if !valid {
            record_error(
                report,
                "full account and ledger verification failed".to_owned(),
            );
        }
        if let Err(error) = events.emit(
            "verified",
            json!({"valid": valid, "accounts_verified": verification.accounts_verified, "failures": verification.failures}),
        ) {
            record_error(report, error);
        }
    }

    let accepted = metric_snapshot
        .counters
        .get("accepted_requests")
        .copied()
        .unwrap_or(0);
    let driver_query_delta = report["driver_counters"]["delta"]["queries"].as_u64();
    match config.backend {
        Backend::Scylla => {
            let measured_metadata_selects = metric_snapshot
                .counters
                .get("cql_read_metadata")
                .copied()
                .unwrap_or(0);
            let measured_ledger_selects = metric_snapshot
                .counters
                .get("cql_read_ledger")
                .copied()
                .unwrap_or(0);
            let measured_select_requests =
                measured_metadata_selects.saturating_add(measured_ledger_selects);
            let measured_cql_request_count = metric_snapshot
                .counters
                .get("cql_write_lwt")
                .copied()
                .unwrap_or(0);
            let query_accounting = json!({
                "account_state_source": "memory",
                "measured_select_requests": measured_select_requests,
                "measured_metadata_select_requests": measured_metadata_selects,
                "measured_ledger_select_requests": measured_ledger_selects,
                "measured_cql_request_count": measured_cql_request_count,
                "accepted_requests": accepted,
                "driver_query_delta": driver_query_delta,
                "cql_requests_per_transaction": ScyllaStore::CQL_REQUESTS_PER_TRANSACTION,
                "cql_statement_count": ScyllaStore::CQL_STATEMENT_COUNT
            });
            put(report, "query_accounting", query_accounting.clone());
            report["requests"]["query_accounting"] = query_accounting;
            report["config"]["measured_select_requests"] = json!(measured_select_requests);
            report["config"]["measured_cql_request_count"] = json!(measured_cql_request_count);
            put(
                report,
                "measured_select_requests",
                json!(measured_select_requests),
            );
            put(
                report,
                "measured_cql_request_count",
                json!(measured_cql_request_count),
            );
            if measured && measured_select_requests != 0 {
                record_error(
                    report,
                    format!(
                        "memory-state workload issued {measured_select_requests} measured SELECT requests"
                    ),
                );
            }
            if measured && measured_cql_request_count != accepted {
                record_error(
                    report,
                    format!(
                        "measured CQL request count is {measured_cql_request_count}; accepted count is {accepted}"
                    ),
                );
            }
        }
        Backend::Postgres => {
            let operations = store.postgres_operation_counters().unwrap_or_default();
            let internal_write_operations = operations
                .ledger_insert_attempts
                .saturating_add(operations.account_updates);
            let postgres_accounting = json!({
                "function_calls": operations.function_calls,
                "measured_sql_transaction_count": operations.function_calls,
                "sql_transactions_per_request": 1,
                "ledger_insert_attempts": operations.ledger_insert_attempts,
                "account_updates": operations.account_updates,
                "internal_write_operations": internal_write_operations,
                "duplicate_conflicts": operations.duplicate_conflicts,
                "accepted_requests": accepted,
                "driver_query_delta": driver_query_delta,
                "driver_query_calls_per_request": 1,
                "query_completion": "ReadyForQuery after implicit transaction completion"
            });
            let query_accounting = json!({
                "backend": "postgres",
                "account_state_source": "memory",
                "measured_select_requests": 0,
                "measured_sql_transaction_count": operations.function_calls,
                "accepted_requests": accepted,
                "driver_query_delta": driver_query_delta,
                "internal_write_operations": internal_write_operations
            });
            put(report, "query_accounting", query_accounting.clone());
            report["requests"]["query_accounting"] = query_accounting;
            put(report, "postgres_accounting", postgres_accounting.clone());
            report["config"]["measured_sql_transaction_count"] = json!(operations.function_calls);
            report["config"]["measured_internal_write_operations"] =
                json!(internal_write_operations);
            if measured && operations.function_calls != accepted {
                record_error(
                    report,
                    format!(
                        "PostgreSQL function call count is {}; accepted count is {accepted}",
                        operations.function_calls
                    ),
                );
            }
            if measured && operations.ledger_insert_attempts != accepted {
                record_error(
                    report,
                    format!(
                        "PostgreSQL ledger insert attempt count is {}; accepted count is {accepted}",
                        operations.ledger_insert_attempts
                    ),
                );
            }
            if measured && operations.account_updates != accepted {
                record_error(
                    report,
                    format!(
                        "PostgreSQL account update count is {}; accepted count is {accepted}",
                        operations.account_updates
                    ),
                );
            }
            if measured && operations.duplicate_conflicts != 0 {
                record_error(
                    report,
                    format!(
                        "PostgreSQL duplicate conflict count is {}; expected zero",
                        operations.duplicate_conflicts
                    ),
                );
            }
            if measured && internal_write_operations != accepted.saturating_mul(2) {
                record_error(
                    report,
                    format!(
                        "PostgreSQL internal write operation count is {internal_write_operations}; expected two per accepted request"
                    ),
                );
            }
        }
    }
    if measured && driver_query_delta != Some(accepted) {
        record_error(
            report,
            format!(
                "{} driver query delta is {driver_query_delta:?}; accepted count is {accepted}",
                config.backend.name()
            ),
        );
    }
    let request_replies = report["requests"]["replies_received"].as_u64().unwrap_or(0);
    if measured {
        report["requests"]["accepted"] = json!(accepted);
    }
    if measured && request_replies != config.total_requests {
        record_error(
            report,
            format!(
                "received {request_replies} replies for {} expected requests",
                config.total_requests
            ),
        );
    }
    if measured && accepted != config.total_requests {
        record_error(
            report,
            format!(
                "service accepted {accepted} requests for {} expected requests",
                config.total_requests
            ),
        );
    }
    if measured
        && let Some(measurement) = report.get_mut("measurement").and_then(Value::as_object_mut)
    {
        measurement.insert("accepted_requests".to_owned(), json!(accepted));
    }

    let no_errors = report["errors"].as_array().is_some_and(Vec::is_empty);
    let verified = report["verification"]["valid"].as_bool().unwrap_or(false);
    report["valid"] = json!(measured && no_errors && verified && shutdown_result.is_ok());
    if measured && let Some(gate) = &config.finish_gate {
        if let Err(error) = await_finish_gate(gate, events).await {
            record_error(report, error);
        } else if report["session_id"].as_str() != Some(store.session_id().as_str()) {
            record_error(
                report,
                "database session id changed during the runner audit wait".to_owned(),
            );
        }
    }
    Ok(())
}

async fn await_finish_gate(gate: &Path, events: &mut EventLog) -> Result<(), String> {
    events.emit(
        "awaiting_runner_audit",
        json!({"finish_gate": gate, "deadline_seconds": FINISH_GATE_DEADLINE.as_secs()}),
    )?;
    let deadline = Instant::now() + FINISH_GATE_DEADLINE;
    loop {
        if gate.exists() {
            events.emit("runner_audit_released", json!({"finish_gate": gate}))?;
            return Ok(());
        }
        if Instant::now() >= deadline {
            events.emit(
                "finish_gate_timeout",
                json!({"finish_gate": gate, "deadline_seconds": FINISH_GATE_DEADLINE.as_secs()}),
            )?;
            return Err(format!(
                "finish gate {} was not created within {} seconds",
                gate.display(),
                FINISH_GATE_DEADLINE.as_secs()
            ));
        }
        sleep(START_GATE_POLL).await;
    }
}

async fn preload_account_states(
    store: Arc<dyn WalletStore>,
    accounts: usize,
    concurrency: usize,
) -> Result<Vec<AccountState>, String> {
    if concurrency == 0 {
        return Err("account state preload concurrency must be positive".to_owned());
    }
    let loaded = futures::stream::iter(0..accounts)
        .map(|account| {
            let store = store.clone();
            async move {
                store
                    .preload_state(account as u64)
                    .await
                    .map(|state| (account, state))
                    .map_err(|error| format!("preload account {account}: {error}"))
            }
        })
        .buffer_unordered(concurrency)
        .try_collect::<Vec<_>>()
        .await?;
    order_preloaded_states(accounts, loaded)
}

fn order_preloaded_states(
    accounts: usize,
    loaded: Vec<(usize, AccountState)>,
) -> Result<Vec<AccountState>, String> {
    if loaded.len() != accounts {
        return Err(format!(
            "preloaded {} account states; expected {accounts}",
            loaded.len()
        ));
    }
    let mut ordered = vec![None; accounts];
    for (account, state) in loaded {
        let slot = ordered
            .get_mut(account)
            .ok_or_else(|| format!("preload returned out-of-range account index {account}"))?;
        if slot.replace(state).is_some() {
            return Err(format!("preload returned account {account} more than once"));
        }
    }
    ordered
        .into_iter()
        .enumerate()
        .map(|(account, state)| state.ok_or_else(|| format!("preload omitted account {account}")))
        .collect()
}

fn metric_invariant_failures(
    snapshot: &ledger_wallet_scylla_bench::metrics::MetricsSnapshot,
    config: &Config,
    e2e_count: u64,
) -> Vec<String> {
    let mut failures = Vec::new();
    let expected = config.total_requests;
    let counter = |name: &str| snapshot.counters.get(name).copied().unwrap_or(0);
    let stage_count = |name: &str| snapshot.stages.get(name).map_or(0, |stage| stage.count);

    if snapshot.enabled {
        failures.push("metrics were still enabled after the measurement window".to_owned());
    }
    let in_flight_name = if config.backend == Backend::Scylla {
        "CQL"
    } else {
        "database"
    };
    if snapshot.cql_in_flight_current != 0 {
        failures.push(format!(
            "{in_flight_name} in-flight current is {}; expected zero after drain",
            snapshot.cql_in_flight_current
        ));
    }
    if snapshot.cql_in_flight_peak > config.db_workers {
        failures.push(format!(
            "{in_flight_name} in-flight peak is {}; exceeds C={}",
            snapshot.cql_in_flight_peak, config.db_workers
        ));
    }
    if expected > 0 && snapshot.cql_in_flight_peak == 0 {
        failures.push(format!(
            "{in_flight_name} in-flight peak is zero despite measured requests"
        ));
    }
    if snapshot.workers_total != config.db_workers {
        failures.push(format!(
            "reported DB worker count is {}; expected C={}",
            snapshot.workers_total, config.db_workers
        ));
    }
    if snapshot.workers_busy_current != 0 {
        failures.push(format!(
            "busy DB workers current is {}; expected zero after drain",
            snapshot.workers_busy_current
        ));
    }
    if snapshot.workers_idle_current != config.db_workers {
        failures.push(format!(
            "idle DB workers current is {}; expected C={}",
            snapshot.workers_idle_current, config.db_workers
        ));
    }
    if snapshot.workers_busy_peak > config.db_workers {
        failures.push(format!(
            "busy DB worker peak is {}; exceeds C={}",
            snapshot.workers_busy_peak, config.db_workers
        ));
    }
    if expected > 0 && snapshot.workers_busy_peak == 0 {
        failures.push("busy DB worker peak is zero despite measured requests".to_owned());
    }
    if snapshot.workers_idle_peak > config.db_workers {
        failures.push(format!(
            "idle DB worker peak is {}; exceeds C={}",
            snapshot.workers_idle_peak, config.db_workers
        ));
    }

    let stages: &[&str] = match config.backend {
        Backend::Scylla => {
            let metadata_reads = counter("cql_read_metadata");
            let ledger_reads = counter("cql_read_ledger");
            expect_count(
                &mut failures,
                "measured metadata SELECTs",
                metadata_reads,
                0,
            );
            expect_count(&mut failures, "measured ledger SELECTs", ledger_reads, 0);
            expect_count(
                &mut failures,
                "total measured SELECT requests",
                metadata_reads.saturating_add(ledger_reads),
                0,
            );
            expect_count(
                &mut failures,
                "CQL LWT writes",
                counter("cql_write_lwt"),
                expected,
            );
            &[
                "admission_wait",
                "account_wait",
                "db_queue_wait",
                "compute",
                "db_write",
                "cql_write_lwt",
                "reply",
            ]
        }
        Backend::Postgres => {
            expect_count(
                &mut failures,
                "PostgreSQL wallet function calls",
                counter("pg_function_calls"),
                expected,
            );
            expect_count(
                &mut failures,
                "PostgreSQL ledger insert attempts",
                counter("pg_ledger_insert_attempts"),
                expected,
            );
            expect_count(
                &mut failures,
                "PostgreSQL account updates",
                counter("pg_account_updates"),
                expected,
            );
            expect_count(
                &mut failures,
                "PostgreSQL duplicate conflicts",
                counter("pg_duplicate_conflicts"),
                0,
            );
            &[
                "admission_wait",
                "account_wait",
                "db_queue_wait",
                "compute",
                "pool_wait",
                "db_write",
                "sql_transaction",
                "reply",
            ]
        }
    };
    expect_count(
        &mut failures,
        "accepted requests",
        counter("accepted_requests"),
        expected,
    );
    expect_count(
        &mut failures,
        "end-to-end latency histogram",
        e2e_count,
        expected,
    );

    for stage in stages {
        expect_count(
            &mut failures,
            &format!("{stage} stage"),
            stage_count(stage),
            expected,
        );
    }
    if config.mode == "formal" && stage_count("admission_wait") == 0 {
        failures.push("formal run has no admission-wait observations".to_owned());
    }
    failures
}

fn expect_count(failures: &mut Vec<String>, label: &str, actual: u64, expected: u64) {
    if actual != expected {
        failures.push(format!("{label} count is {actual}; expected {expected}"));
    }
}

fn record_metric_invariant_failures(
    report: &mut Value,
    snapshot: &ledger_wallet_scylla_bench::metrics::MetricsSnapshot,
    config: &Config,
    e2e_count: u64,
) -> Vec<String> {
    let failures = metric_invariant_failures(snapshot, config, e2e_count);
    for failure in &failures {
        record_error(report, failure.clone());
    }
    failures
}

fn validate_driver_counters(
    report: &mut Value,
    backend: Backend,
    start: DriverCounters,
    end: DriverCounters,
    expected_requests: u64,
) -> Value {
    let mut failures = Vec::new();
    let (driver_label, request_label, requests_per_operation) = match backend {
        Backend::Scylla => (
            "Scylla driver",
            "CQL request",
            ScyllaStore::CQL_REQUESTS_PER_TRANSACTION,
        ),
        Backend::Postgres => ("PostgreSQL client", "SQL function call", 1),
    };
    let expected_queries = expected_requests.checked_mul(requests_per_operation);
    if expected_queries.is_none() {
        failures.push(format!(
            "expected {driver_label} query count overflowed for {expected_requests} requests"
        ));
    }

    let queries = checked_counter_delta(
        driver_label,
        "queries",
        start.queries,
        end.queries,
        &mut failures,
    );
    let errors = checked_counter_delta(
        driver_label,
        "errors",
        start.errors,
        end.errors,
        &mut failures,
    );
    let retries = checked_counter_delta(
        driver_label,
        "retries",
        start.retries,
        end.retries,
        &mut failures,
    );
    let connection_timeouts = checked_counter_delta(
        driver_label,
        "connection_timeouts",
        start.connection_timeouts,
        end.connection_timeouts,
        &mut failures,
    );
    let request_timeouts = checked_counter_delta(
        driver_label,
        "request_timeouts",
        start.request_timeouts,
        end.request_timeouts,
        &mut failures,
    );

    if let (Some(observed), Some(expected)) = (queries, expected_queries)
        && observed != expected
    {
        let expected_detail = match backend {
            Backend::Scylla => format!(
                "{} * {expected_requests} = {expected}",
                ScyllaStore::CQL_REQUESTS_PER_TRANSACTION
            ),
            Backend::Postgres => format!("1 {request_label} * {expected_requests} = {expected}"),
        };
        failures.push(format!(
            "{driver_label} query delta is {observed}; expected exactly {expected_detail}"
        ));
    }
    for (name, delta) in [
        ("errors", errors),
        ("retries", retries),
        ("connection_timeouts", connection_timeouts),
        ("request_timeouts", request_timeouts),
    ] {
        if let Some(delta) = delta
            && delta != 0
        {
            failures.push(format!(
                "{driver_label} {name} delta is {delta}; expected zero"
            ));
        }
    }

    for failure in &failures {
        record_error(report, failure.clone());
    }
    json!({
        "start": start,
        "end": end,
        "delta": {
            "queries": queries,
            "errors": errors,
            "retries": retries,
            "connection_timeouts": connection_timeouts,
            "request_timeouts": request_timeouts
        },
        "expected_query_count": expected_queries,
        "valid": failures.is_empty(),
        "validation_failures": failures
    })
}

fn checked_counter_delta(
    driver_label: &str,
    name: &str,
    start: u64,
    end: u64,
    failures: &mut Vec<String>,
) -> Option<u64> {
    match end.checked_sub(start) {
        Some(delta) => Some(delta),
        None => {
            failures.push(format!(
                "{driver_label} {name} counter decreased from {start} to {end}; reset/wrap is invalid"
            ));
            None
        }
    }
}

async fn run_clients(
    config: &Config,
    store: Arc<EngineStore>,
    service: Arc<Service>,
    metrics: Arc<Metrics>,
    events: &mut EventLog,
    report: &mut Value,
) -> Result<(), String> {
    match config.backend {
        Backend::Scylla => metrics.reset(),
        Backend::Postgres => metrics.reset_postgres(),
    }
    let latency = Arc::new(LatencyStripes::new());
    let abort = Arc::new(AtomicBool::new(false));
    let signals = Arc::new(WindowSignals::new(
        config.users,
        metrics.clone(),
        store.clone(),
    ));
    let connections = Arc::new(Mutex::new(ConnectionSamples::new()));
    let ready_barrier = Arc::new(Barrier::new(config.users + 1));
    let start_barrier = Arc::new(Barrier::new(config.users + 1));
    let mut tasks = JoinSet::new();

    let shared_config = Arc::new(config.clone());
    for account in 0..config.users {
        let ready_barrier = ready_barrier.clone();
        let start_barrier = start_barrier.clone();
        let service = service.clone();
        let abort = abort.clone();
        let signals = signals.clone();
        let latency = latency.clone();
        let client_config = shared_config.clone();
        tasks.spawn(async move {
            ready_barrier.wait().await;
            start_barrier.wait().await;
            run_client(
                account as u64,
                client_config.as_ref(),
                service,
                abort,
                signals,
                latency,
            )
            .await
        });
    }
    ready_barrier.wait().await;
    let mut ready_event = json!({
        "all_clients_ready": true,
        "pool_ready_connections": store.data_connections(),
        "C": config.db_workers,
        "data_connection_target": config.data_connection_target,
        "worker_routing": worker_routing_name(config.worker_routing),
        "setup_concurrency": config.setup_concurrency,
        "account_state_source": "memory",
        "account_states_preloaded": config.users,
        "per_worker_queue_capacity": config.per_worker_queue_capacity,
        "start_gate": config.start_gate,
        "start_gate_deadline_seconds": START_GATE_DEADLINE.as_secs()
    });
    match config.backend {
        Backend::Scylla => {
            ready_event["S"] = json!(config.shards);
            ready_event["P"] = json!(config.per_shard_pool);
            ready_event["per_shard_pool_overridden"] = json!(config.per_shard_pool_overridden);
            ready_event["requested_per_shard_pool"] = json!(config.requested_per_shard_pool);
        }
        Backend::Postgres => {
            ready_event["database_name"] = json!("walletbench");
            ready_event["pg_schema"] = json!(config.pg_schema);
            ready_event["pg_connections"] = json!(config.pg_connections);
            ready_event["db_cpu_count"] = json!(config.db_cpu_count);
            ready_event["schema_sql_sha256"] = json!(postgres_schema_sql_sha256());
            ready_event["postgres_backend_pids"] = json!(store.postgres_backend_pids());
            ready_event["accounts_table"] = json!("accounts");
            ready_event["accounts_primary_key_constraint"] = json!("accounts_pkey");
            ready_event["ledger_table"] = json!("ledger");
            ready_event["ledger_primary_key_constraint"] = json!("ledger_pkey");
            ready_event["ledger_primary_key"] = json!(["account_id", "tx_id"]);
        }
    }
    events.emit("ready_to_measure", ready_event)?;

    if let Some(gate) = &config.start_gate {
        let deadline = Instant::now() + START_GATE_DEADLINE;
        loop {
            if gate.exists() {
                break;
            }
            if Instant::now() >= deadline {
                abort.store(true, Ordering::Release);
                start_barrier.wait().await;
                while tasks.join_next().await.is_some() {}
                metrics.disable();
                let _ = events.emit(
                    "client_end",
                    json!({"started": false, "reason": "start-gate deadline elapsed", "replies_received": 0}),
                );
                put(
                    report,
                    "measurement",
                    json!({"started": false, "start_gate": gate, "deadline_seconds": START_GATE_DEADLINE.as_secs()}),
                );
                return Err(format!(
                    "start gate {} was not created within {} seconds",
                    gate.display(),
                    START_GATE_DEADLINE.as_secs()
                ));
            }
            sleep(START_GATE_POLL).await;
        }
    }

    let io_start = match read_process_io() {
        Ok(counters) => counters,
        Err(error) => {
            abort.store(true, Ordering::Release);
            start_barrier.wait().await;
            while tasks.join_next().await.is_some() {}
            metrics.disable();
            return Err(format!(
                "read {PROCESS_IO_PATH} before measurement: {error}"
            ));
        }
    };
    let io_start_at = Instant::now();
    let cpu_start = ProcessTime::try_now()
        .map_err(|error| format!("read process CPU time before measurement: {error}"))?;
    let cpu_start_at = Instant::now();
    let driver_counters_start_at = Instant::now();
    let driver_counters_start = store.driver_counters();
    signals.set_driver_counters_start(driver_counters_start_at, driver_counters_start);
    let started = Instant::now();
    let measurement_start_unix_ns = unix_time_ns()?;
    let measurement_utc_sample_offset_ns =
        duration_ns(Instant::now().saturating_duration_since(started));
    let wall_start_utc_ns =
        measurement_start_unix_ns.saturating_sub(measurement_utc_sample_offset_ns as u128);
    signals.set_started(started);
    metrics.enable();
    events.emit(
        "measuring",
        json!({
            "measurement_start_unix_ns": wall_start_utc_ns,
            "start_gate_open": config.start_gate.as_ref().is_none_or(|path| path.exists()),
            "histogram_stripes": E2E_STRIPES,
            "histogram_range_ns": E2E_RANGE_NS,
            "histogram_significant_figures": HISTOGRAM_SIGNIFICANT_FIGURES,
            "window_starts_before_event_flush": true
        }),
    )?;
    let measuring_event_flushed_at = Instant::now();
    let measurement_start_to_event_flush_ns =
        duration_ns(measuring_event_flushed_at.saturating_duration_since(started));
    let (heartbeat_start_tx, heartbeat_start_rx) = tokio::sync::oneshot::channel();
    let heartbeat = tokio::spawn(heartbeat_task(
        heartbeat_start_rx,
        signals.completion.clone(),
    ));
    let connection_sampler = tokio::spawn(connection_sampler_task(
        store.clone(),
        config.data_connection_target as u64,
        signals.clone(),
        connections.clone(),
    ));
    let _ = heartbeat_start_tx.send(started);
    start_barrier.wait().await;

    let mut summaries = Vec::with_capacity(config.users);
    let mut join_error = None;
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok(summary) => summaries.push(summary),
            Err(error) => {
                abort.store(true, Ordering::Release);
                join_error.get_or_insert_with(|| format!("client task failed: {error}"));
            }
        }
    }
    if !signals.completion.done.load(Ordering::Acquire) {
        signals.mark_failed_end();
        record_error(
            report,
            "not all client tasks reported a terminal reply; measurement monitors were stopped after task completion".to_owned(),
        );
    }
    metrics.disable();
    if let Some(error) = join_error {
        record_error(report, error);
    }

    let heartbeat_summary = match heartbeat.await {
        Ok(summary) => summary,
        Err(error) => {
            record_error(report, format!("heartbeat task failed: {error}"));
            HeartbeatSummary::new()
        }
    };
    if let Err(error) = connection_sampler.await {
        record_error(report, format!("connection sampler task failed: {error}"));
    }

    let end_offset_ns = signals.end_offset_ns();
    let window_ns = end_offset_ns;
    let exact_replies = summaries.iter().map(|summary| summary.replies).sum::<u64>();
    let attempts = summaries
        .iter()
        .map(|summary| summary.attempts)
        .sum::<u64>();
    let errors = summaries.iter().map(|summary| summary.errors).sum::<u64>();
    let unexpected = summaries
        .iter()
        .map(|summary| summary.unexpected_business_results)
        .sum::<u64>();
    let mut operation_counts = BTreeMap::<&'static str, u64>::new();
    let mut outcome_counts = BTreeMap::<&'static str, u64>::new();
    let mut error_counts = BTreeMap::<&'static str, u64>::new();
    let mut error_samples = BTreeMap::<&'static str, String>::new();
    let mut samples = Vec::new();
    let mut first_error = None;
    for mut summary in summaries {
        summary.merge_error_samples_into(&mut error_samples);
        for (operation, count) in summary.operation_counts {
            *operation_counts.entry(operation).or_default() += count;
        }
        for (outcome, count) in summary.outcome_counts {
            *outcome_counts.entry(outcome).or_default() += count;
        }
        for (kind, count) in summary.error_counts {
            *error_counts.entry(kind).or_default() += count;
        }
        if first_error.is_none() {
            first_error = summary.first_error.take();
        }
        samples.append(&mut summary.samples);
    }
    if errors > 0 || unexpected > 0 {
        record_error(
            report,
            format!(
                "load clients received {errors} errors and {unexpected} unexpected business outcomes{}",
                first_error
                    .as_ref()
                    .map(|message| format!("; first: {message}"))
                    .unwrap_or_default()
            ),
        );
    }

    let latency_report = latency.snapshot()?;
    let mut latency_report = latency_report;
    latency_report["stripes"] = json!(E2E_STRIPES);
    latency_report["estimated_memory_bytes"] = json!(E2E_STRIPES * 350 * 1024);
    put(report, "end_to_end_latency", latency_report.clone());
    let histogram_overflow = latency_report["overflow_count"].as_u64().unwrap_or(0);
    if histogram_overflow > 0 {
        record_error(
            report,
            format!(
                "{histogram_overflow} end-to-end latencies exceeded the 600-second histogram range"
            ),
        );
    }
    let pool_after_last_reply = store.data_connections();
    let connection_samples = lock(&connections);
    let enough_connection_samples = config.mode != "formal" || connection_samples.sample_count > 0;
    let pool_valid = connection_samples.is_valid(
        pool_after_last_reply,
        config.data_connection_target as u64,
        enough_connection_samples,
    );
    let mut pool_report = json!({
        "target_total_data_connections": config.data_connection_target,
        "C": config.db_workers,
        "worker_routing": worker_routing_name(config.worker_routing),
        "setup_concurrency": config.setup_concurrency,
        "pool_ready_connections": report["connection_pool"]["pool_ready_connections"],
        "pool_after_last_reply": pool_after_last_reply,
        "samples_during_measurement": connection_samples.sample_count,
        "minimum_sampled_connections": connection_samples.minimum,
        "maximum_sampled_connections": connection_samples.maximum,
        "mismatch_samples": connection_samples.mismatch_count,
        "first_mismatch": connection_samples.first_mismatch,
        "last_sample_offset_ns": connection_samples.last_sample_offset_ns,
        "sampling_period_ns": duration_ns(CONNECTION_SAMPLE_PERIOD),
        "valid": pool_valid,
        "formal_sample_required": config.mode == "formal"
    });
    match config.backend {
        Backend::Scylla => {
            pool_report["S"] = json!(config.shards);
            pool_report["P"] = json!(config.per_shard_pool);
            pool_report["per_shard_pool_overridden"] = json!(config.per_shard_pool_overridden);
            pool_report["requested_per_shard_pool"] = json!(config.requested_per_shard_pool);
            pool_report["metric_source"] =
                json!("Scylla Rust driver Session::get_metrics().get_total_connections()");
        }
        Backend::Postgres => {
            pool_report["backend"] = json!("postgres");
            pool_report["database_name"] = json!("walletbench");
            pool_report["pg_schema"] = json!(config.pg_schema);
            pool_report["pg_connections"] = json!(config.pg_connections);
            pool_report["db_cpu_count"] = json!(config.db_cpu_count);
            pool_report["metric_source"] =
                json!("tokio-postgres persistent connection pool liveness");
        }
    }
    put(report, "connection_pool", pool_report);
    drop(connection_samples);
    if !pool_valid {
        record_error(
            report,
            format!(
                "{} data-connection count differed from D during measurement",
                config.backend.name()
            ),
        );
    }

    let heartbeat_valid = heartbeat_summary.overflow_count == 0;
    put(report, "heartbeat", heartbeat_summary.json());
    if !heartbeat_valid {
        record_error(
            report,
            "heartbeat lag exceeded its 60-second histogram range".to_owned(),
        );
    }

    let mut csv_error = None;
    if let Err(error) = write_samples_csv(&config.output_dir.join("latency_samples.csv"), &samples)
    {
        csv_error = Some(error);
    }
    if let Some(error) = csv_error {
        record_error(report, error);
    }

    let reply_rps =
        (window_ns > 0).then(|| exact_replies as f64 / (window_ns as f64 / 1_000_000_000.0));
    let applied = outcome_counts.get("applied").copied().unwrap_or(0);
    let accepted = metrics
        .snapshot()
        .counters
        .get("accepted_requests")
        .copied()
        .unwrap_or(0);
    let wall_end_utc_ns = wall_start_utc_ns.saturating_add(window_ns as u128);
    let end_candidate = lock(&signals.end_candidate).clone();
    let cpu_end = end_candidate
        .as_ref()
        .and_then(|(_, cpu, _)| cpu.map(|(duration, _)| duration));
    let cpu_end_sample_offset_ns = end_candidate.as_ref().and_then(|(_, cpu, _)| {
        cpu.map(|(_, sampled_at)| duration_ns(sampled_at.saturating_duration_since(started)))
    });
    let cpu_end_sample_at = end_candidate
        .as_ref()
        .and_then(|(_, cpu, _)| cpu.map(|(_, sampled_at)| sampled_at));
    let cpu_measurement_end =
        cpu_end.map(|duration| duration.saturating_sub(cpu_start.as_duration()));
    let end_cpu_sample = cpu_measurement_end.map(duration_ns);
    let cpu_end_sample_offset_from_window_ns =
        cpu_end_sample_at.map(|at| signed_offset_ns(at, started + Duration::from_nanos(window_ns)));
    let io_end = lock(&signals.io_end).clone();
    let io_json = io_end.as_ref().map(|(captured_at, result)| match result {
        Ok(end_io) => io_delta_json(
            io_start,
            *end_io,
            signed_offset_ns(io_start_at, started),
            signed_offset_ns(*captured_at, started + Duration::from_nanos(window_ns)),
        ),
        Err(error) => json!({"error": error}),
    });
    let cpu_error = lock(&signals.cpu_sample_error).clone();
    let driver_counters_start = lock(&signals.driver_counters_start).to_owned();
    let driver_counters_end = lock(&signals.driver_counters_end).to_owned();
    let driver_counters_report = match (driver_counters_start, driver_counters_end) {
        (Some((start_at, start)), Some((end_at, end))) => {
            let mut counter_report =
                validate_driver_counters(report, config.backend, start, end, config.total_requests);
            if let Some(object) = counter_report.as_object_mut() {
                object.insert(
                    "start_sample_offset_ns".to_owned(),
                    json!(signed_offset_ns(start_at, started)),
                );
                object.insert(
                    "end_sample_offset_from_window_end_ns".to_owned(),
                    json!(signed_offset_ns(
                        end_at,
                        started + Duration::from_nanos(window_ns)
                    )),
                );
                object.insert(
                    "window_end_definition".to_owned(),
                    json!("last client reply received"),
                );
            }
            counter_report
        }
        _ => Value::Null,
    };
    if driver_counters_report.is_null() {
        record_error(
            report,
            "driver counters did not have both window boundary samples".to_owned(),
        );
    }
    put(
        report,
        "process_cpu",
        json!({
            "scope": "whole process; service, clients, heartbeat and monitor share this process",
            "start_process_cpu_ns": duration_ns(cpu_start.as_duration()),
            "end_process_cpu_ns": cpu_end.map(duration_ns),
            "cpu_used_ns": end_cpu_sample,
            "cpu_start_sample_offset_ns": signed_offset_ns(cpu_start_at, started),
            "cpu_end_sample_offset_from_window_end_ns": cpu_end_sample_offset_from_window_ns,
            "cpu_end_sample_offset_ns": cpu_end_sample_offset_ns,
            "end_reply_offset_ns": end_candidate.as_ref().map(|(reply_at, _, _)| duration_ns(reply_at.saturating_duration_since(started))),
            "error": cpu_error.clone().or_else(|| end_candidate.and_then(|(_, _, error)| error))
        }),
    );
    put(report, "process_io", io_json.unwrap_or(Value::Null));
    put(report, "driver_counters", driver_counters_report);
    if cpu_end.is_none() || cpu_error.is_some() {
        record_error(report, "process CPU end sample was unavailable".to_owned());
    }
    match &io_end {
        Some((_, Ok(_))) => {}
        Some((_, Err(error))) => {
            record_error(report, format!("read process I/O at window end: {error}"));
        }
        None => record_error(report, "process I/O end sample was unavailable".to_owned()),
    }

    put(
        report,
        "requests",
        json!({
            "expected": config.total_requests,
            "attempted": attempts,
            "replies_received": exact_replies,
            "accepted": accepted,
            "applied": applied,
            "unexpected_business_results": unexpected,
            "errors": errors,
            "operation_counts": {
                "credit": operation_counts.get("credit").copied().unwrap_or(0),
                "debit": operation_counts.get("debit").copied().unwrap_or(0)
            },
            "outcome_counts": {
                "applied": outcome_counts.get("applied").copied().unwrap_or(0),
                "insufficient_funds": outcome_counts.get("insufficient_funds").copied().unwrap_or(0),
                "credit_overflow": outcome_counts.get("credit_overflow").copied().unwrap_or(0),
                "invalid_amount": outcome_counts.get("invalid_amount").copied().unwrap_or(0)
            },
            "error_counts": error_counts,
            "error_samples": error_samples,
            "first_error": first_error
        }),
    );
    put(
        report,
        "measurement",
        json!({
            "started": true,
            "start_unix_ns": wall_start_utc_ns,
            "end_unix_ns": wall_end_utc_ns,
            "utc_sample_unix_ns": measurement_start_unix_ns,
            "utc_sample_offset_after_window_start_ns": measurement_utc_sample_offset_ns,
            "measurement_start_to_measuring_event_flush_ns": measurement_start_to_event_flush_ns,
            "start_offset_ns": 0,
            "end_offset_ns": window_ns,
            "wall_duration_ns": window_ns,
            "wall_seconds": window_ns as f64 / 1_000_000_000.0,
            "request_count": exact_replies,
            "attempted_count": attempts,
            "rps": reply_rps,
            "successful_applied_rps": (window_ns > 0).then(|| applied as f64 / (window_ns as f64 / 1_000_000_000.0)),
            "accepted_requests": accepted,
            "start_gate": config.start_gate,
            "client_count": config.users,
            "service_and_load_share_process": true
        }),
    );
    put(
        report,
        "latency_samples",
        json!({"file": "latency_samples.csv", "stride": 64, "rows": samples.len()}),
    );

    events.emit(
        "client_end",
        json!({
            "started": true,
            "window_start_unix_ns": wall_start_utc_ns,
            "measurement_start_unix_ns": wall_start_utc_ns,
            "measurement_end_unix_ns": wall_end_utc_ns,
            "window_start_utc_sample_offset_ns": measurement_utc_sample_offset_ns,
            "measurement_start_to_measuring_event_flush_ns": measurement_start_to_event_flush_ns,
            "window_end_offset_ns": window_ns,
            "window_duration_ns": window_ns,
            "replies_received": exact_replies,
            "attempted": attempts,
            "applied": applied,
            "errors": errors,
            "histogram_overflow_count": histogram_overflow
        }),
    )?;
    Ok(())
}

async fn run_client(
    account_id: u64,
    config: &Config,
    service: Arc<Service>,
    abort: Arc<AtomicBool>,
    signals: Arc<WindowSignals>,
    latency: Arc<LatencyStripes>,
) -> ClientSummary {
    let mut summary = ClientSummary::new();
    let mut last_reply = None;
    let mut terminal = false;
    for step in 0..config.requests_per_user {
        if abort.load(Ordering::Acquire) {
            signals.mark_terminal(last_reply);
            terminal = true;
            break;
        }

        let global_id = match global_request_id(account_id, step, config.requests_per_user) {
            Some(id) => id,
            None => {
                summary.add_error(
                    "invalid_request_id",
                    "global request id overflow".to_owned(),
                );
                abort.store(true, Ordering::Release);
                signals.mark_terminal(last_reply);
                terminal = true;
                break;
            }
        };
        let operation = if step % 2 == 0 {
            Operation::Credit
        } else {
            Operation::Debit
        };
        let operation_name = match operation {
            Operation::Credit => "credit",
            Operation::Debit => "debit",
        };
        summary.attempts = summary.attempts.saturating_add(1);
        summary.add_operation(operation_name);
        let request = Transaction {
            account_id,
            tx_id: (step + 1) as u64,
            operation,
            amount: 1,
        };
        let submitted_at = Instant::now();
        let result = service.submit(request).await;
        let reply_at = Instant::now();
        let latency_duration = reply_at.saturating_duration_since(submitted_at);
        let latency_ns = duration_ns(latency_duration);
        last_reply = Some(reply_at);
        summary.replies = summary.replies.saturating_add(1);

        let good_result = result
            .as_ref()
            .is_ok_and(|transaction_result| expected_result(transaction_result, step));
        let terminal_after_reply = step + 1 == config.requests_per_user
            || result.is_err()
            || !good_result
            || abort.load(Ordering::Acquire);
        if terminal_after_reply {
            signals.mark_terminal(last_reply);
            terminal = true;
        }

        latency.record(account_id, latency_duration);
        if global_id % 64 == 0 {
            summary.samples.push(ClientSample {
                global_request_id: global_id,
                account_id,
                step: step + 1,
                latency_ns,
            });
        }

        match result {
            Ok(transaction_result) => {
                let outcome = outcome_name(transaction_result.outcome);
                summary.add_outcome(outcome);
                if !expected_result(&transaction_result, step) {
                    summary.unexpected_business_results =
                        summary.unexpected_business_results.saturating_add(1);
                    let error = format!(
                        "account {account_id} step {} returned {:?}, balance {}, version {}, replayed {}; expected Applied, balance {}, version {}, replayed=false",
                        step + 1,
                        transaction_result.outcome,
                        transaction_result.balance,
                        transaction_result.version,
                        transaction_result.replayed,
                        expected_balance(step),
                        step + 1
                    );
                    summary.first_error.get_or_insert(error);
                    abort.store(true, Ordering::Release);
                }
            }
            Err(error) => {
                let kind = error_kind(&error);
                summary.add_error(kind, error.to_string());
                abort.store(true, Ordering::Release);
            }
        }
        if terminal_after_reply {
            break;
        }
    }
    if !terminal {
        signals.mark_terminal(last_reply);
    }
    summary
}

fn expected_result(result: &TransactionResult, step: usize) -> bool {
    result.outcome == Outcome::Applied
        && result.balance == expected_balance(step)
        && result.version == (step + 1) as u64
        && !result.replayed
}

fn expected_balance(step: usize) -> u64 {
    if step % 2 == 0 { 101 } else { 100 }
}

async fn heartbeat_task(
    start_receiver: tokio::sync::oneshot::Receiver<Instant>,
    completion: Arc<WindowCompletion>,
) -> HeartbeatSummary {
    let Ok(started) = start_receiver.await else {
        return HeartbeatSummary::new();
    };
    let mut summary = HeartbeatSummary::new();
    let mut next_tick = started + HEARTBEAT_PERIOD;
    loop {
        let notified = completion.done_notify.notified();
        if completion.done.load(Ordering::Acquire) {
            record_final_heartbeat(&mut summary, started, next_tick, &completion);
            break;
        }
        tokio::select! {
            _ = notified => {
                record_final_heartbeat(&mut summary, started, next_tick, &completion);
                break;
            }
            _ = sleep_until(tokio::time::Instant::from_std(next_tick)) => {
                let now = Instant::now();
                if completion.done.load(Ordering::Acquire) {
                    record_final_heartbeat(&mut summary, started, next_tick, &completion);
                    break;
                }
                let lag = now.saturating_duration_since(next_tick);
                let offset_ns = duration_ns(now.saturating_duration_since(started));
                summary.observe(lag, offset_ns);
                if lag >= HEARTBEAT_PERIOD {
                    summary.missed_ticks = summary
                        .missed_ticks
                        .saturating_add(duration_ns(lag) / duration_ns(HEARTBEAT_PERIOD));
                    next_tick = now + HEARTBEAT_PERIOD;
                } else {
                    next_tick += HEARTBEAT_PERIOD;
                }
            }
        }
    }
    summary
}

fn record_final_heartbeat(
    summary: &mut HeartbeatSummary,
    started: Instant,
    next_tick: Instant,
    completion: &WindowCompletion,
) {
    let end_offset = completion.end_offset_ns();
    let end = started + Duration::from_nanos(end_offset);
    if next_tick <= end {
        let clipped_now = Instant::now().min(end);
        let lag = clipped_now.saturating_duration_since(next_tick);
        summary.observe(lag, end_offset);
        if lag >= HEARTBEAT_PERIOD {
            summary.missed_ticks = summary
                .missed_ticks
                .saturating_add(duration_ns(lag) / duration_ns(HEARTBEAT_PERIOD));
        }
    }
}

async fn connection_sampler_task(
    store: Arc<EngineStore>,
    target: u64,
    signals: Arc<WindowSignals>,
    samples: Arc<Mutex<ConnectionSamples>>,
) {
    let started = loop {
        if let Some(started) = signals.started.get() {
            break *started;
        }
        tokio::task::yield_now().await;
    };
    let mut next_sample = started + CONNECTION_SAMPLE_PERIOD;
    loop {
        let notified = signals.completion.done_notify.notified();
        if signals.completion.done.load(Ordering::Acquire) {
            break;
        }
        tokio::select! {
            _ = notified => break,
            _ = sleep_until(tokio::time::Instant::from_std(next_sample)) => {
                if signals.completion.done.load(Ordering::Acquire) {
                    break;
                }
                let now = Instant::now();
                let offset_ns = duration_ns(now.saturating_duration_since(started));
                lock(&samples).record(store.data_connections(), target, offset_ns);
                next_sample = now + CONNECTION_SAMPLE_PERIOD;
            }
        }
    }
}

async fn verify_all(
    config: &Config,
    store: Arc<EngineStore>,
    cached_states: Vec<AccountState>,
) -> VerificationSummary {
    let failure_count = Arc::new(AtomicUsize::new(0));
    let cache_compare_count = Arc::new(AtomicUsize::new(0));
    let verified_count = Arc::new(AtomicUsize::new(0));
    let ledger_count = Arc::new(AtomicU64::new(0));
    let first_failures = Arc::new(Mutex::new(Vec::<String>::new()));
    let requests_per_user = config.requests_per_user;
    let cached_states = Arc::new(cached_states);
    let store_for_accounts = store.clone();
    let failure_count_for_accounts = failure_count.clone();
    let cache_compare_count_for_accounts = cache_compare_count.clone();
    let verified_count_for_accounts = verified_count.clone();
    let ledger_count_for_accounts = ledger_count.clone();
    let first_failures_for_accounts = first_failures.clone();

    let result = futures::stream::iter(0..config.users)
        .map(Ok::<usize, String>)
        .try_for_each_concurrent(config.setup_concurrency, move |account_index| {
            let store = store_for_accounts.clone();
            let failure_count = failure_count_for_accounts.clone();
            let cache_compare_count = cache_compare_count_for_accounts.clone();
            let verified_count = verified_count_for_accounts.clone();
            let ledger_count = ledger_count_for_accounts.clone();
            let first_failures = first_failures_for_accounts.clone();
            let cached_states = cached_states.clone();
            async move {
                let account_id = account_index as u64;
                match store.verify_account(account_id).await {
                    Ok((state, records)) => {
                        ledger_count.fetch_add(records.len() as u64, Ordering::Relaxed);
                        let Some(cached_state) = cached_states.get(account_index).copied() else {
                            failure_count.fetch_add(1, Ordering::Relaxed);
                            let mut examples = lock(&first_failures);
                            if examples.len() < 100 {
                                examples.push(format!("memory cache omitted account {account_id}"));
                            }
                            return Ok(());
                        };
                        cache_compare_count.fetch_add(1, Ordering::Relaxed);
                        if let Err(problem) =
                            ensure_cache_matches_db(account_id, &state, &cached_state)
                        {
                            failure_count.fetch_add(1, Ordering::Relaxed);
                            let mut examples = lock(&first_failures);
                            if examples.len() < 100 {
                                examples.push(problem);
                            }
                            return Ok(());
                        }
                        if let Err(problem) =
                            verify_one_account(account_id, &state, records, requests_per_user)
                        {
                            failure_count.fetch_add(1, Ordering::Relaxed);
                            let mut examples = lock(&first_failures);
                            if examples.len() < 100 {
                                examples.push(problem);
                            }
                        } else {
                            verified_count.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(error) => {
                        failure_count.fetch_add(1, Ordering::Relaxed);
                        let mut examples = lock(&first_failures);
                        if examples.len() < 100 {
                            examples.push(format!("verify account {account_id}: {error}"));
                        }
                    }
                }
                Ok(())
            }
        })
        .await;
    if let Err(error) = result {
        let mut examples = lock(&first_failures);
        examples.push(error);
        failure_count.fetch_add(1, Ordering::Relaxed);
    }
    let failures = failure_count.load(Ordering::Relaxed) as u64;
    let memory_cache_accounts_compared = cache_compare_count.load(Ordering::Relaxed);
    let accounts_verified = verified_count.load(Ordering::Relaxed);
    let ledger_rows_seen = ledger_count.load(Ordering::Relaxed);
    VerificationSummary {
        accounts_expected: config.users,
        memory_cache_accounts_compared,
        accounts_verified,
        ledger_rows_expected: config.total_requests,
        ledger_rows_seen,
        failures,
        first_failures: lock(&first_failures).clone(),
    }
}

fn ensure_cache_matches_db(
    account_id: u64,
    db_state: &AccountState,
    cached_state: &AccountState,
) -> Result<(), String> {
    if db_state != cached_state {
        return Err(format!(
            "account {account_id} DB static state differs from drained memory cache"
        ));
    }
    Ok(())
}

fn verify_one_account(
    account_id: u64,
    state: &AccountState,
    mut records: Vec<LedgerRecord>,
    requests_per_user: usize,
) -> Result<(), String> {
    if state.balance != 100 {
        return Err(format!(
            "account {account_id} balance is {}, expected 100",
            state.balance
        ));
    }
    if state.version != requests_per_user as u64 {
        return Err(format!(
            "account {account_id} version is {}, expected {requests_per_user}",
            state.version
        ));
    }
    if state.write_clock <= 0 {
        return Err(format!(
            "account {account_id} has invalid write clock {}",
            state.write_clock
        ));
    }
    if records.len() != requests_per_user {
        return Err(format!(
            "account {account_id} has {} ledger rows, expected {requests_per_user}",
            records.len()
        ));
    }
    records.sort_by_key(|record| record.request.tx_id);
    let mut previous_write_clock = 0_i64;
    for (index, record) in records.iter().enumerate() {
        let step = index + 1;
        let expected_operation = if index % 2 == 0 {
            Operation::Credit
        } else {
            Operation::Debit
        };
        if record.request.account_id != account_id
            || record.request.tx_id != step as u64
            || record.request.operation != expected_operation
            || record.request.amount != 1
        {
            return Err(format!(
                "account {account_id} ledger row {step} has the wrong key or request body"
            ));
        }
        if record.result.outcome != Outcome::Applied
            || record.result.balance != expected_balance(index)
            || record.result.version != step as u64
            || record.result.replayed
        {
            return Err(format!(
                "account {account_id} ledger row {step} has an unexpected stored result"
            ));
        }
        if record.write_clock <= previous_write_clock {
            return Err(format!(
                "account {account_id} ledger row {step} write clock {} is not strictly greater than {previous_write_clock}",
                record.write_clock
            ));
        }
        if record.write_clock > state.write_clock {
            return Err(format!(
                "account {account_id} ledger row {step} write clock {} exceeds static write clock {}",
                record.write_clock, state.write_clock
            ));
        }
        previous_write_clock = record.write_clock;
    }
    if previous_write_clock != state.write_clock {
        return Err(format!(
            "account {account_id} final ledger write clock {previous_write_clock} differs from static write clock {}",
            state.write_clock
        ));
    }
    Ok(())
}

fn write_samples_csv(path: &Path, samples: &[ClientSample]) -> Result<(), String> {
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| format!("create {}: {error}", path.display()))?;
    let mut writer = BufWriter::new(file);
    writeln!(writer, "global_request_id,account_id,step,latency_ns")
        .map_err(|error| format!("write latency sample header: {error}"))?;
    for sample in samples {
        writeln!(
            writer,
            "{},{},{},{}",
            sample.global_request_id, sample.account_id, sample.step, sample.latency_ns
        )
        .map_err(|error| format!("write latency sample row: {error}"))?;
    }
    writer
        .flush()
        .map_err(|error| format!("flush latency samples: {error}"))
}

fn write_atomic_json(path: &Path, value: &Value) -> Result<(), String> {
    if path.exists() {
        return Err(format!(
            "{} already exists; refusing to overwrite a trial result",
            path.display()
        ));
    }
    let temp_path = path.with_extension(format!("json.tmp-{}", std::process::id()));
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp_path)
        .map_err(|error| format!("create {}: {error}", temp_path.display()))?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, value)
        .map_err(|error| format!("serialize result JSON: {error}"))?;
    writer
        .write_all(b"\n")
        .and_then(|()| writer.flush())
        .map_err(|error| format!("write result JSON: {error}"))?;
    writer
        .get_ref()
        .sync_all()
        .map_err(|error| format!("sync result JSON: {error}"))?;
    fs::rename(&temp_path, path).map_err(|error| format!("rename atomic result JSON: {error}"))
}

fn histogram_json(histogram: &Histogram<u64>, observed_max: u64, overflow_count: u64) -> Value {
    let count = histogram.len();
    json!({
        "unit": "nanoseconds",
        "count": count,
        "min_ns": if count == 0 {Value::Null} else {json!(histogram.min())},
        "mean_ns": if count == 0 {Value::Null} else {json!(histogram.mean())},
        "p50_ns": if count == 0 {Value::Null} else {json!(histogram.value_at_quantile(0.50))},
        "p95_ns": if count == 0 {Value::Null} else {json!(histogram.value_at_quantile(0.95))},
        "p99_ns": if count == 0 {Value::Null} else {json!(histogram.value_at_quantile(0.99))},
        "p999_ns": if count == 0 {Value::Null} else {json!(histogram.value_at_quantile(0.999))},
        "max_ns": if count == 0 {Value::Null} else {json!(observed_max.max(histogram.max()))},
        "overflow_count": overflow_count,
        "range_max_ns": E2E_RANGE_NS,
        "significant_figures": HISTOGRAM_SIGNIFICANT_FIGURES
    })
}

fn io_delta_json(
    start: IoCounters,
    end: IoCounters,
    start_sample_offset_ns: i64,
    end_sample_offset_ns: i64,
) -> Value {
    json!({
        "source": PROCESS_IO_PATH,
        "start_sample_offset_ns": start_sample_offset_ns,
        "end_sample_offset_ns": end_sample_offset_ns,
        "sample_span_ns": end_sample_offset_ns.saturating_sub(start_sample_offset_ns),
        "start": io_counters_json(start),
        "end": io_counters_json(end),
        "delta": {
            "rchar": end.rchar.saturating_sub(start.rchar),
            "wchar": end.wchar.saturating_sub(start.wchar),
            "syscr": end.syscr.saturating_sub(start.syscr),
            "syscw": end.syscw.saturating_sub(start.syscw),
            "read_bytes": end.read_bytes.saturating_sub(start.read_bytes),
            "write_bytes": end.write_bytes.saturating_sub(start.write_bytes),
            "cancelled_write_bytes": end.cancelled_write_bytes.saturating_sub(start.cancelled_write_bytes)
        }
    })
}

fn io_counters_json(io: IoCounters) -> Value {
    json!({
        "rchar": io.rchar,
        "wchar": io.wchar,
        "syscr": io.syscr,
        "syscw": io.syscw,
        "read_bytes": io.read_bytes,
        "write_bytes": io.write_bytes,
        "cancelled_write_bytes": io.cancelled_write_bytes
    })
}

fn read_process_io() -> Result<IoCounters, String> {
    let contents = fs::read_to_string(PROCESS_IO_PATH)
        .map_err(|error| format!("read {PROCESS_IO_PATH}: {error}"))?;
    let mut values = BTreeMap::new();
    for line in contents.lines() {
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| format!("invalid line in {PROCESS_IO_PATH}: {line:?}"))?;
        let value = value
            .trim()
            .parse::<u64>()
            .map_err(|error| format!("parse {name} from {PROCESS_IO_PATH}: {error}"))?;
        values.insert(name, value);
    }
    let get = |name: &str| {
        values
            .get(name)
            .copied()
            .ok_or_else(|| format!("{PROCESS_IO_PATH} omitted {name}"))
    };
    Ok(IoCounters {
        rchar: get("rchar")?,
        wchar: get("wchar")?,
        syscr: get("syscr")?,
        syscw: get("syscw")?,
        read_bytes: get("read_bytes")?,
        write_bytes: get("write_bytes")?,
        cancelled_write_bytes: get("cancelled_write_bytes")?,
    })
}

fn global_request_id(account_id: u64, step: usize, requests_per_user: usize) -> Option<u64> {
    account_id
        .checked_mul(u64::try_from(requests_per_user).ok()?)?
        .checked_add(u64::try_from(step).ok()?)?
        .checked_add(1)
}

fn worker_routing_name(routing: WorkerRouting) -> &'static str {
    match routing {
        WorkerRouting::RoundRobin => "round-robin",
        WorkerRouting::PerUser => "per-user",
    }
}

fn outcome_name(outcome: Outcome) -> &'static str {
    match outcome {
        Outcome::Applied => "applied",
        Outcome::InsufficientFunds => "insufficient_funds",
        Outcome::CreditOverflow => "credit_overflow",
        Outcome::InvalidAmount => "invalid_amount",
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AccountState, Backend, ClientSummary, Config, ConnectionSamples, ConsistencyLevel,
        DEFAULT_QUEUE_CAPACITY, DriverCounters, ERROR_SAMPLE_MAX_BYTES,
        ERROR_SAMPLE_TRUNCATION_MARKER, EventLog, HEARTBEAT_PERIOD, LedgerRecord, Metrics,
        Operation, Outcome, ParseResult, Transaction, TransactionResult, WindowSignals,
        WorkerRouting, base_report, bounded_error_sample, expected_balance, heartbeat_task,
        order_preloaded_states, parse_args, record_metric_invariant_failures,
        validate_driver_counters, verify_one_account, worker_routing_name,
    };
    use ledger_wallet_scylla_bench::metrics::{MetricsSnapshot, StageSnapshot};
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    fn valid_args() -> Vec<String> {
        [
            "--nodes",
            "127.0.0.1:9042,127.0.0.2:9042,127.0.0.3:9042",
            "--keyspace",
            "wallet_bench_smoke",
            "--output-dir",
            "/tmp/scylla-wallet-smoke",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect()
    }

    fn parsed_config(args: Vec<String>) -> Config {
        match parse_args(args).expect("arguments should parse") {
            ParseResult::Config(config) => config,
            ParseResult::Help => panic!("expected parsed config, got help"),
        }
    }

    fn single_node_args(shards: usize) -> Vec<String> {
        let mut args = valid_args();
        args[1] = "127.0.0.1:9042".to_owned();
        args.extend([
            "--replication-factor".to_owned(),
            "1".to_owned(),
            "--read-cl".to_owned(),
            "1".to_owned(),
            "--write-cl".to_owned(),
            "1".to_owned(),
            "--db-workers".to_owned(),
            "128".to_owned(),
            "--shards".to_owned(),
            shards.to_string(),
            "--per-shard-pool".to_owned(),
            "3".to_owned(),
        ]);
        args
    }

    #[test]
    fn error_sample_merge_preserves_first_message_for_each_kind() {
        let mut not_accepted = ClientSummary::new();
        not_accepted.add_error(
            "not_accepted",
            "request was not accepted: account is frozen".to_owned(),
        );
        let mut commit_unknown = ClientSummary::new();
        commit_unknown.add_error(
            "commit_outcome_unknown",
            "commit outcome unknown: coordinator write timeout".to_owned(),
        );

        let mut merged = BTreeMap::new();
        not_accepted.merge_error_samples_into(&mut merged);
        commit_unknown.merge_error_samples_into(&mut merged);

        assert_eq!(merged.len(), 2);
        assert_eq!(
            merged.get("not_accepted").map(String::as_str),
            Some("request was not accepted: account is frozen")
        );
        assert_eq!(
            merged.get("commit_outcome_unknown").map(String::as_str),
            Some("commit outcome unknown: coordinator write timeout")
        );
        assert_eq!(
            not_accepted.first_error.as_deref(),
            Some("request was not accepted: account is frozen")
        );
    }

    #[test]
    fn error_samples_keep_only_one_bounded_utf8_sample_per_kind() {
        let mut summary = ClientSummary::new();
        let oversized_message = "é".repeat(3_000);
        summary.add_error("storage", oversized_message.clone());
        summary.add_error("storage", "later message for the same kind".to_owned());

        assert_eq!(summary.error_samples.len(), 1);
        let sample = summary.error_samples.get("storage").unwrap();
        assert!(sample.len() <= ERROR_SAMPLE_MAX_BYTES);
        assert!(sample.ends_with(ERROR_SAMPLE_TRUNCATION_MARKER));
        assert_eq!(sample, &bounded_error_sample(&oversized_message));
        assert_eq!(
            summary.first_error.as_deref(),
            Some(oversized_message.as_str())
        );
    }

    #[test]
    fn defaults_match_the_formal_workload_and_bounded_queue_design() {
        let config = parsed_config(valid_args());
        assert_eq!(config.users, 50_000);
        assert_eq!(config.requests_per_user, 200);
        assert_eq!(config.db_workers, 12);
        assert_eq!(config.shards, 1);
        assert_eq!(config.queue_capacity, DEFAULT_QUEUE_CAPACITY);
        assert_eq!(config.total_requests, 10_000_000);
        assert_eq!(config.per_shard_pool, 4);
        assert_eq!(config.data_connection_target, 12);
        assert_eq!(config.setup_concurrency, 12);
        assert_eq!(config.worker_routing, WorkerRouting::RoundRobin);
        assert_eq!(config.per_worker_queue_capacity, DEFAULT_QUEUE_CAPACITY);
        assert!(!config.per_shard_pool_overridden);
        assert_eq!(config.replication_factor, 3);
        assert_eq!(config.read_consistency, ConsistencyLevel::Three);
        assert_eq!(config.write_consistency, ConsistencyLevel::Three);
        let report = base_report(&config);
        assert_eq!(
            report["durability_precondition"]["required_consistency"],
            "THREE"
        );
        assert_eq!(
            report["durability_precondition"]["fsync_acknowledgement_replica_count"],
            3
        );
    }

    #[test]
    fn postgres_cli_uses_fixed_profile_without_exposing_dsn() {
        let args = [
            "--backend",
            "postgres",
            "--pg-dsn",
            "postgresql://bench:secret@127.0.0.1:5432/walletbench",
            "--pg-schema",
            "wallet_bench_case_20261013",
            "--pg-connections",
            "16",
            "--db-cpus",
            "2",
            "--users",
            "8",
            "--requests-per-user",
            "2",
            "--output-dir",
            "/tmp/wallet-postgres-report-contract",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect();
        let config = parsed_config(args);
        assert_eq!(config.backend, Backend::Postgres);
        assert_eq!(config.db_workers, 512);
        assert_eq!(config.pg_connections, 16);
        assert_eq!(config.data_connection_target, 16);
        assert_eq!(config.db_cpu_count, 2);
        assert_eq!(config.setup_concurrency, 6);
        assert_eq!(config.worker_routing, WorkerRouting::RoundRobin);
        assert_eq!(config.total_requests, 16);

        let report = base_report(&config);
        assert_eq!(report["backend"], "postgres");
        assert_eq!(report["wallet_profile"], "postgres-memory-tx-v1");
        assert_eq!(report["config"]["pg_connections"], 16);
        assert_eq!(report["config"]["C"], 512);
        assert_eq!(report["config"]["setup_concurrency"], 6);
        assert_eq!(report["postgres_accounting"], serde_json::Value::Null);
        assert!(!report.to_string().contains("secret"));
        let event_dir = std::env::temp_dir().join(format!(
            "wallet-pg-event-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let event_config = Config {
            output_dir: event_dir.clone(),
            ..config.clone()
        };
        let mut events = EventLog::create(&event_config).expect("create event output");
        events.set_session_id("postgres:test:1,2,3".to_owned());
        events
            .emit("ready_to_measure", serde_json::json!({}))
            .expect("write ready event");
        drop(events);
        let event_text =
            std::fs::read_to_string(event_dir.join("events.jsonl")).expect("read event output");
        assert!(event_text.contains("ready_to_measure"));
        assert!(!event_text.contains("secret"));
        std::fs::remove_dir_all(event_dir).expect("remove event output");
        assert!(
            parse_args(
                [
                    "--backend",
                    "postgres",
                    "--pg-dsn",
                    "postgresql://bench@127.0.0.1:5432/walletbench",
                    "--pg-schema",
                    "wallet_bench_case",
                    "--nodes",
                    "127.0.0.1:9042",
                    "--output-dir",
                    "/tmp/wallet-postgres-report-contract",
                ]
                .into_iter()
                .map(str::to_owned)
                .collect()
            )
            .is_err()
        );
    }

    #[test]
    fn explicit_rf1_single_node_profiles_use_node_shards_times_pool_for_d() {
        for (shards, expected_d) in [(1, 3), (2, 6)] {
            let config = parsed_config(single_node_args(shards));
            assert_eq!(config.nodes.len(), 1);
            assert_eq!(config.replication_factor, 1);
            assert_eq!(config.read_consistency, ConsistencyLevel::One);
            assert_eq!(config.write_consistency, ConsistencyLevel::One);
            assert_eq!(config.db_workers, 128);
            assert_eq!(config.per_shard_pool, 3);
            assert_eq!(config.data_connection_target, expected_d);
        }
    }

    #[test]
    fn parser_rejects_unsupported_node_counts_and_single_node_rf3_defaults() {
        let mut two_nodes = valid_args();
        two_nodes[1] = "127.0.0.1:9042,127.0.0.2:9042".to_owned();
        assert!(
            parse_args(two_nodes).is_err(),
            "two endpoints must be rejected"
        );

        let mut default_rf = valid_args();
        default_rf[1] = "127.0.0.1:9042".to_owned();
        assert!(
            parse_args(default_rf).is_err(),
            "single node must not lower RF=3 implicitly"
        );

        let mut explicit_rf3 = single_node_args(1);
        let rf_index = explicit_rf3
            .iter()
            .position(|argument| argument == "--replication-factor")
            .unwrap();
        explicit_rf3[rf_index + 1] = "3".to_owned();
        assert!(parse_args(explicit_rf3).is_err());
    }

    #[test]
    fn parser_rejects_duplicate_nodes_bad_single_node_cl_and_connection_overflow() {
        let mut duplicate = valid_args();
        duplicate[1] = "127.0.0.1:9042,127.0.0.1:9042,127.0.0.3:9042".to_owned();
        assert!(parse_args(duplicate).is_err());

        let mut bad_cl = single_node_args(1);
        let cl_index = bad_cl
            .iter()
            .position(|argument| argument == "--read-cl")
            .unwrap();
        bad_cl[cl_index + 1] = "2".to_owned();
        assert!(parse_args(bad_cl).is_err());

        let mut overflow = single_node_args(1);
        let shards_index = overflow
            .iter()
            .position(|argument| argument == "--shards")
            .unwrap();
        overflow[shards_index + 1] = usize::MAX.to_string();
        assert!(parse_args(overflow).is_err());
    }

    #[test]
    fn numeric_rf_and_cl_options_map_to_canonical_metadata() {
        let mut args = valid_args();
        args.extend([
            "--replication-factor".to_owned(),
            "1".to_owned(),
            "--read-cl".to_owned(),
            "1".to_owned(),
            "--write-cl".to_owned(),
            "1".to_owned(),
        ]);
        let config = parsed_config(args);
        assert_eq!(config.replication_factor, 1);
        assert_eq!(config.read_consistency, ConsistencyLevel::One);
        assert_eq!(config.write_consistency, ConsistencyLevel::One);

        let report = base_report(&config);
        assert_eq!(report["config"]["replication_factor"], 1);
        assert_eq!(report["config"]["read_cl"], "ONE");
        assert_eq!(report["config"]["write_cl"], "ONE");
        assert_eq!(
            report["durability_precondition"]["required_read_consistency"],
            "ONE"
        );
        assert_eq!(
            report["durability_precondition"]["required_write_consistency"],
            "ONE"
        );
        assert_eq!(
            report["durability_precondition"]["required_consistency"],
            "ONE"
        );
        assert_eq!(
            report["durability_precondition"]["fsync_acknowledgement_replica_count"],
            1
        );
    }

    #[test]
    fn unequal_read_and_write_cl_do_not_claim_a_single_legacy_consistency() {
        let mut args = valid_args();
        args.extend([
            "--replication-factor".to_owned(),
            "3".to_owned(),
            "--read-cl".to_owned(),
            "1".to_owned(),
            "--write-cl".to_owned(),
            "2".to_owned(),
        ]);
        let config = parsed_config(args);
        let report = base_report(&config);
        assert_eq!(report["config"]["read_cl"], "ONE");
        assert_eq!(report["config"]["write_cl"], "TWO");
        assert_eq!(
            report["durability_precondition"]["required_consistency"],
            serde_json::Value::Null
        );
        assert_eq!(
            report["durability_precondition"]["fsync_acknowledgement_replica_count"],
            2
        );
    }

    #[test]
    fn rf_and_cl_values_outside_the_profile_are_rejected_during_parse() {
        for (flag, value) in [("--replication-factor", "0"), ("--replication-factor", "4")] {
            let mut args = valid_args();
            args.extend([flag.to_owned(), value.to_owned()]);
            assert!(parse_args(args).is_err(), "accepted {flag} {value}");
        }
        for flag in ["--read-cl", "--write-cl"] {
            for value in ["0", "4"] {
                let mut args = valid_args();
                args.extend([
                    "--replication-factor".to_owned(),
                    "1".to_owned(),
                    flag.to_owned(),
                    value.to_owned(),
                ]);
                assert!(parse_args(args).is_err(), "accepted {flag} {value}");
            }
        }
        for flag in ["--read-cl", "--write-cl"] {
            let mut args = valid_args();
            args.extend([
                "--replication-factor".to_owned(),
                "1".to_owned(),
                flag.to_owned(),
                "2".to_owned(),
            ]);
            assert!(parse_args(args).is_err(), "accepted {flag} TWO above RF=1");
        }
    }

    #[test]
    fn explicit_pool_decouples_c_from_d_and_per_user_routing_accepts_s4_c50000_p4() {
        let mut args = valid_args();
        args.extend([
            "--shards".to_owned(),
            "4".to_owned(),
            "--db-workers".to_owned(),
            "50000".to_owned(),
            "--users".to_owned(),
            "50000".to_owned(),
            "--per-shard-pool".to_owned(),
            "4".to_owned(),
            "--worker-routing".to_owned(),
            "per-user".to_owned(),
        ]);
        let config = parsed_config(args.clone());
        assert_eq!(config.db_workers, 50_000);
        assert_eq!(config.users, 50_000);
        assert_eq!(config.shards, 4);
        assert_eq!(config.per_shard_pool, 4);
        assert_eq!(config.data_connection_target, 48);
        assert_eq!(config.setup_concurrency, 48);
        assert_eq!(config.worker_routing, WorkerRouting::PerUser);
        assert_eq!(worker_routing_name(config.worker_routing), "per-user");
        assert!(config.per_shard_pool_overridden);
        assert_eq!(config.requested_per_shard_pool, Some(4));
        assert_eq!(config.per_worker_queue_capacity, 1);

        let mut implicit_pool = args;
        let pool_flag = implicit_pool
            .iter()
            .position(|argument| argument == "--per-shard-pool")
            .unwrap();
        implicit_pool.drain(pool_flag..pool_flag + 2);
        assert!(parse_args(implicit_pool).is_err());
    }

    #[test]
    fn connection_pool_samples_validate_against_d_independently_of_c() {
        let mut args = valid_args();
        args.extend([
            "--shards".to_owned(),
            "4".to_owned(),
            "--db-workers".to_owned(),
            "50000".to_owned(),
            "--users".to_owned(),
            "50000".to_owned(),
            "--per-shard-pool".to_owned(),
            "4".to_owned(),
            "--worker-routing".to_owned(),
            "per-user".to_owned(),
        ]);
        let config = parsed_config(args);
        let mut samples = ConnectionSamples::new();
        samples.record(48, config.data_connection_target as u64, 100_000_000);
        assert!(samples.is_valid(48, config.data_connection_target as u64, true));

        let mut wrong_target = ConnectionSamples::new();
        wrong_target.record(48, config.db_workers as u64, 100_000_000);
        assert!(!wrong_target.is_valid(48, config.db_workers as u64, true));
        assert!(!wrong_target.is_valid(48, config.data_connection_target as u64, true));
    }

    #[test]
    fn per_user_route_rejects_a_worker_account_count_mismatch() {
        let mut args = valid_args();
        args.extend([
            "--db-workers".to_owned(),
            "12".to_owned(),
            "--users".to_owned(),
            "13".to_owned(),
            "--worker-routing".to_owned(),
            "per-user".to_owned(),
        ]);
        let Err(error) = parse_args(args) else {
            panic!("per-user requires one worker per account");
        };
        assert!(error.to_string().contains("requires --db-workers"));
    }

    #[test]
    fn rejects_duplicate_nodes_invalid_keyspace_and_unverifiable_request_counts() {
        let mut duplicate_nodes = valid_args();
        duplicate_nodes[1] = "127.0.0.1:9042,127.0.0.1:9042,127.0.0.3:9042".to_owned();
        assert!(parse_args(duplicate_nodes).is_err());

        let mut invalid_keyspace = valid_args();
        invalid_keyspace[3] = "wallet_bench_BAD".to_owned();
        assert!(parse_args(invalid_keyspace).is_err());

        let mut too_many_rows = valid_args();
        too_many_rows.extend(["--requests-per-user".to_owned(), "1002".to_owned()]);
        assert!(parse_args(too_many_rows).is_err());
    }

    #[test]
    fn help_short_circuits_required_arguments() {
        assert!(matches!(
            parse_args(vec!["--help".to_owned()]),
            Ok(ParseResult::Help)
        ));
    }

    #[test]
    fn preload_order_uses_account_index_and_rejects_missing_duplicate_or_unknown_accounts() {
        let state0 = AccountState {
            balance: 100,
            version: 0,
            write_clock: 0,
        };
        let state1 = AccountState {
            balance: 211,
            version: 4,
            write_clock: 4,
        };
        let ordered = order_preloaded_states(2, vec![(1, state1), (0, state0)]).unwrap();
        assert_eq!(ordered, vec![state0, state1]);

        assert!(order_preloaded_states(2, vec![(0, state0)]).is_err());
        assert!(order_preloaded_states(2, vec![(0, state0), (0, state1)]).is_err());
        assert!(order_preloaded_states(1, vec![(1, state1)]).is_err());
    }

    #[test]
    fn parses_optional_runner_handshake_gates() {
        let mut args = valid_args();
        args.extend([
            "--start-gate".to_owned(),
            "/tmp/start".to_owned(),
            "--finish-gate".to_owned(),
            "/tmp/finish".to_owned(),
        ]);
        let config = parsed_config(args);
        assert_eq!(
            config.start_gate.as_deref(),
            Some(std::path::Path::new("/tmp/start"))
        );
        assert_eq!(
            config.finish_gate.as_deref(),
            Some(std::path::Path::new("/tmp/finish"))
        );
    }

    #[tokio::test]
    async fn client_panic_with_missing_terminal_signal_stops_heartbeat_promptly() {
        let metrics = Arc::new(Metrics::new());
        metrics.set_worker_count(2);
        let signals = Arc::new(WindowSignals::for_test(2, metrics.clone()));
        let started = Instant::now();
        signals.set_started(started);

        let (start_sender, start_receiver) = tokio::sync::oneshot::channel();
        let heartbeat = tokio::spawn(heartbeat_task(start_receiver, signals.completion.clone()));
        start_sender.send(started).unwrap();

        signals.mark_terminal(Some(started + HEARTBEAT_PERIOD / 2));
        assert_eq!(
            signals
                .completion
                .terminal_clients
                .load(std::sync::atomic::Ordering::Acquire),
            1
        );
        assert!(
            !signals
                .completion
                .done
                .load(std::sync::atomic::Ordering::Acquire)
        );

        let panicking_client = tokio::spawn(async { panic!("simulated client task panic") });
        assert!(panicking_client.await.unwrap_err().is_panic());

        // This is the same incomplete-window recovery path used after run_clients joins all tasks.
        signals.mark_failed_end();
        assert!(
            signals
                .completion
                .done
                .load(std::sync::atomic::Ordering::Acquire)
        );
        assert_eq!(
            signals
                .completion
                .terminal_clients
                .load(std::sync::atomic::Ordering::Acquire),
            2
        );

        tokio::time::timeout(Duration::from_secs(1), heartbeat)
            .await
            .expect("heartbeat did not stop after forced window completion")
            .expect("heartbeat task should return normally");
    }

    #[test]
    fn verification_rejects_decreasing_and_nonmatching_final_write_clocks() {
        let decreasing = make_records(0, &[101, 100]);
        let state = AccountState {
            balance: 100,
            version: 2,
            write_clock: 200,
        };
        let error = verify_one_account(0, &state, decreasing, 2)
            .expect_err("decreasing ledger write clocks must be rejected");
        assert!(error.contains("is not strictly greater"), "{error}");

        let increasing = make_records(0, &[101, 102]);
        let state = AccountState {
            balance: 100,
            version: 2,
            write_clock: 103,
        };
        let error = verify_one_account(0, &state, increasing, 2)
            .expect_err("final ledger clock must match the static account clock");
        assert!(error.contains("differs from static write clock"), "{error}");
    }

    #[test]
    fn metric_invariants_reject_bad_counts_and_peaks_above_configured_c() {
        let mut args = valid_args();
        args.extend([
            "--db-workers".to_owned(),
            "3".to_owned(),
            "--users".to_owned(),
            "1".to_owned(),
            "--requests-per-user".to_owned(),
            "2".to_owned(),
        ]);
        let config = parsed_config(args);
        let expected = config.total_requests;

        let mut snapshot = valid_metric_snapshot(config.db_workers, expected);
        snapshot
            .counters
            .insert("cql_write_lwt".to_owned(), expected - 1);
        let mut report = serde_json::json!({"valid": true, "errors": []});
        let failures =
            record_metric_invariant_failures(&mut report, &snapshot, &config, expected - 1);
        assert_eq!(report["valid"], false);
        assert!(
            failures
                .iter()
                .any(|failure| failure.contains("CQL LWT writes count is 1; expected 2")),
            "expected a write-count failure, got {failures:?}"
        );
        assert!(
            failures
                .iter()
                .any(|failure| failure
                    .contains("end-to-end latency histogram count is 1; expected 2")),
            "expected an end-to-end count failure, got {failures:?}"
        );

        let mut snapshot = valid_metric_snapshot(config.db_workers, expected);
        snapshot.cql_in_flight_peak = config.db_workers + 1;
        snapshot.workers_busy_peak = config.db_workers + 1;
        let mut report = serde_json::json!({"valid": true, "errors": []});
        let failures = record_metric_invariant_failures(&mut report, &snapshot, &config, expected);
        assert_eq!(report["valid"], false);
        assert!(
            failures
                .iter()
                .any(|failure| { failure.contains("CQL in-flight peak is 4; exceeds C=3") }),
            "expected a CQL peak failure based on the configured C, got {failures:?}"
        );
        assert!(
            failures
                .iter()
                .any(|failure| failure.contains("busy DB worker peak is 4; exceeds C=3")),
            "expected a worker peak failure based on the configured C, got {failures:?}"
        );
    }

    #[test]
    fn driver_counter_validation_rejects_retries_resets_and_query_mismatch() {
        let start = DriverCounters {
            queries: 0,
            errors: 0,
            retries: 0,
            connection_timeouts: 0,
            request_timeouts: 0,
        };
        let end_with_retry = DriverCounters {
            queries: 6,
            errors: 0,
            retries: 1,
            connection_timeouts: 0,
            request_timeouts: 0,
        };
        let mut report = serde_json::json!({"valid": true, "errors": []});
        let retry_report =
            validate_driver_counters(&mut report, Backend::Scylla, start, end_with_retry, 2);
        assert_eq!(retry_report["valid"], false);
        assert!(
            retry_report["validation_failures"]
                .as_array()
                .unwrap()
                .iter()
                .any(|failure| failure
                    .as_str()
                    .is_some_and(|failure| failure.contains("retries delta is 1; expected zero"))),
            "expected retry rejection, got {retry_report}"
        );
        assert_eq!(report["valid"], false);

        let start_with_error_count = DriverCounters { errors: 2, ..start };
        let end_with_reset_and_query_mismatch = DriverCounters {
            queries: 7,
            errors: 1,
            ..start
        };
        let mut report = serde_json::json!({"valid": true, "errors": []});
        let counter_report = validate_driver_counters(
            &mut report,
            Backend::Scylla,
            start_with_error_count,
            end_with_reset_and_query_mismatch,
            2,
        );
        assert_eq!(counter_report["valid"], false);
        assert_eq!(counter_report["delta"]["errors"], serde_json::Value::Null);
        let failures = counter_report["validation_failures"].as_array().unwrap();
        assert!(
            failures
                .iter()
                .any(|failure| failure.as_str().is_some_and(|failure| {
                    failure.contains("query delta is 7; expected exactly 1 * 2 = 2")
                })),
            "expected query-count mismatch rejection, got {counter_report}"
        );
        assert!(
            failures
                .iter()
                .any(|failure| failure.as_str().is_some_and(|failure| {
                    failure.contains("errors counter decreased from 2 to 1; reset/wrap is invalid")
                })),
            "expected counter-reset rejection, got {counter_report}"
        );
        assert_eq!(report["valid"], false);
    }

    #[test]
    fn postgres_driver_counter_validation_uses_sql_calls_without_scylla_labels() {
        let start = DriverCounters {
            queries: 100,
            errors: 2,
            retries: 0,
            connection_timeouts: 0,
            request_timeouts: 0,
        };
        let end = DriverCounters {
            queries: 102,
            ..start
        };
        let mut report = serde_json::json!({"valid": true, "errors": []});
        let counters = validate_driver_counters(&mut report, Backend::Postgres, start, end, 2);
        assert_eq!(counters["valid"], true);
        assert_eq!(counters["expected_query_count"], 2);
        assert_eq!(counters["delta"]["queries"], 2);
        assert!(report["errors"].as_array().unwrap().is_empty());

        let mut report = serde_json::json!({"valid": true, "errors": []});
        let mismatch = validate_driver_counters(&mut report, Backend::Postgres, start, end, 3);
        let failures = mismatch["validation_failures"].as_array().unwrap();
        assert!(failures.iter().any(|failure| {
            failure.as_str().is_some_and(|failure| {
                failure.contains("PostgreSQL client query delta is 2; expected exactly 1 SQL function call * 3 = 3")
            })
        }));
        assert!(failures.iter().all(|failure| {
            failure
                .as_str()
                .is_some_and(|failure| !failure.contains("Scylla") && !failure.contains("CQL"))
        }));
    }

    fn make_records(account_id: u64, clocks: &[i64]) -> Vec<LedgerRecord> {
        clocks
            .iter()
            .enumerate()
            .map(|(index, write_clock)| {
                let operation = if index % 2 == 0 {
                    Operation::Credit
                } else {
                    Operation::Debit
                };
                LedgerRecord {
                    request: Transaction {
                        account_id,
                        tx_id: (index + 1) as u64,
                        operation,
                        amount: 1,
                    },
                    result: TransactionResult {
                        outcome: Outcome::Applied,
                        balance: expected_balance(index),
                        version: (index + 1) as u64,
                        replayed: false,
                    },
                    write_clock: *write_clock,
                }
            })
            .collect()
    }

    fn valid_metric_snapshot(worker_count: usize, expected: u64) -> MetricsSnapshot {
        let stages = [
            "admission_wait",
            "account_wait",
            "db_queue_wait",
            "compute",
            "db_write",
            "cql_write_lwt",
            "reply",
        ]
        .into_iter()
        .map(|stage| {
            (
                stage.to_owned(),
                StageSnapshot {
                    count: expected,
                    min_ns: 1,
                    mean_ns: 1,
                    p50_ns: 1,
                    p95_ns: 1,
                    p99_ns: 1,
                    p999_ns: 1,
                    max_ns: 1,
                    overflow_count: 0,
                },
            )
        })
        .collect();
        let counters = [
            ("cql_read_metadata".to_owned(), 0),
            ("cql_read_ledger".to_owned(), 0),
            ("cql_write_lwt".to_owned(), expected),
            ("accepted_requests".to_owned(), expected),
        ]
        .into_iter()
        .collect::<BTreeMap<_, _>>();
        MetricsSnapshot {
            enabled: false,
            stages,
            counters,
            cql_in_flight_current: 0,
            cql_in_flight_peak: 1,
            workers_total: worker_count,
            workers_busy_current: 0,
            workers_busy_peak: 1,
            workers_idle_current: worker_count,
            workers_idle_peak: worker_count,
        }
    }
}

fn error_kind(error: &Error) -> &'static str {
    match error {
        Error::InvalidConfiguration(_) => "invalid_configuration",
        Error::NotAccepted(_) => "not_accepted",
        Error::Conflict(_) => "conflict",
        Error::Storage(_) => "storage",
        Error::CommitConfirmedRollback(_) => "commit_confirmed_rollback",
        Error::CommitOutcomeUnknown(_) => "commit_outcome_unknown",
        Error::Corruption(_) => "corruption",
        Error::Frozen(_) => "frozen",
        Error::Closed => "closed",
    }
}

fn put(report: &mut Value, key: &str, value: Value) {
    if let Some(object) = report.as_object_mut() {
        object.insert(key.to_owned(), value);
    }
}

fn metrics_json(
    snapshot: &ledger_wallet_scylla_bench::metrics::MetricsSnapshot,
    backend: Backend,
) -> Value {
    let mut value = serde_json::to_value(snapshot).unwrap_or(Value::Null);
    if backend == Backend::Postgres
        && let Some(object) = value.as_object_mut()
    {
        let current = object
            .remove("cql_in_flight_current")
            .unwrap_or(Value::Null);
        let peak = object.remove("cql_in_flight_peak").unwrap_or(Value::Null);
        object.insert("database_in_flight_current".to_owned(), current);
        object.insert("database_in_flight_peak".to_owned(), peak);
    }
    value
}

fn record_error(report: &mut Value, message: String) {
    if let Some(errors) = report.get_mut("errors").and_then(Value::as_array_mut) {
        errors.push(json!(message));
    }
    report["valid"] = json!(false);
}

fn unix_time_ns() -> Result<u128, String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .map_err(|error| format!("read system UTC clock: {error}"))
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn signed_offset_ns(sample: Instant, origin: Instant) -> i64 {
    if sample >= origin {
        duration_ns(sample.duration_since(origin)).min(i64::MAX as u64) as i64
    } else {
        -(duration_ns(origin.duration_since(sample)).min(i64::MAX as u64) as i64)
    }
}

fn atomic_max_u64(target: &AtomicU64, value: u64) {
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
