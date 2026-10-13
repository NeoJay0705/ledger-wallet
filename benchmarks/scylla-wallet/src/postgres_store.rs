use crate::metrics::{ActivityGuard, Metrics};
use crate::store::WalletStore;
use crate::{
    AccountState, CommitResolution, DriverCounters, Error, LedgerRecord, Operation, Outcome,
    Result, Transaction, TransactionResult,
};
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, mpsc, watch};
use tokio::task::AbortHandle;
use tokio_postgres::error::Severity;
use tokio_postgres::{Client, NoTls, Row, Statement};

const POSTGRES_DATABASE: &str = "walletbench";
const APPLICATION_NAME: &str = "ledger-wallet-postgres-wallet-bench";
const WALLET_SCHEMA_SQL: &str = include_str!("../sql/postgres_wallet.sql");
const MAX_LEDGER_ROWS: usize = 1_000;
const CLIENT_QUERY_TIMEOUT: Duration = Duration::from_secs(30);
const SETUP_CONCURRENCY: usize = 6;

const COMMIT_SQL: &str = "SELECT * FROM wallet_commit($1, $2, $3, $4, $5, $6, $7, $8)";
const SEED_SQL: &str =
    "INSERT INTO accounts (account_id, balance, version, write_clock) VALUES ($1, $2, 0, 0)";
const PRELOAD_SQL: &str =
    "SELECT balance, version, write_clock FROM accounts WHERE account_id = $1";
const LEDGER_SCAN_SQL: &str = "SELECT account_id, tx_id, operation, amount, outcome, balance_after, version, write_clock FROM ledger WHERE account_id = $1 ORDER BY tx_id LIMIT 1001";

#[derive(Clone, Debug, serde::Serialize)]
pub struct PostgresConnectionSettings {
    pub backend_pid: i32,
    pub application_name: String,
    pub synchronous_commit: String,
    pub fsync: String,
    pub full_page_writes: String,
    pub commit_delay: String,
    pub is_standby: bool,
    pub synchronous_standby_names: String,
}

#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub struct PostgresOperationCounters {
    pub function_calls: u64,
    pub ledger_insert_attempts: u64,
    pub account_updates: u64,
    pub duplicate_conflicts: u64,
}

#[derive(Default)]
struct AtomicDriverCounters {
    queries: AtomicU64,
    errors: AtomicU64,
    request_timeouts: AtomicU64,
}

struct RawConnection {
    client: Client,
    alive: Arc<AtomicBool>,
    abort: AbortHandle,
    settings: PostgresConnectionSettings,
}

struct PoolConnection {
    client: Client,
    alive: Arc<AtomicBool>,
    abort: AbortHandle,
    settings: PostgresConnectionSettings,
    commit: Statement,
    seed: Statement,
    preload: Statement,
    ledger_scan: Statement,
}

pub struct PostgresStore {
    schema: String,
    session_id: String,
    connections: Vec<PoolConnection>,
    available_tx: mpsc::UnboundedSender<usize>,
    available_rx: Mutex<mpsc::UnboundedReceiver<usize>>,
    metrics: Arc<Metrics>,
    driver_counters: AtomicDriverCounters,
    schema_sql_sha256: String,
    fatal_tx: watch::Sender<bool>,
    fatal_rx: watch::Receiver<bool>,
}

struct PoolLease<'a> {
    store: &'a PostgresStore,
    index: usize,
    complete: bool,
}

impl Drop for PoolLease<'_> {
    fn drop(&mut self) {
        if self.complete {
            return;
        }
        let connection = &self.store.connections[self.index];
        fail_closed_connection(&connection.alive, &connection.abort, &self.store.fatal_tx);
    }
}

impl PoolLease<'_> {
    fn connection(&self) -> &PoolConnection {
        &self.store.connections[self.index]
    }

    fn finish(mut self) {
        self.complete = true;
        let _ = self.store.available_tx.send(self.index);
    }

    fn poison(mut self) {
        let connection = &self.store.connections[self.index];
        fail_closed_connection(&connection.alive, &connection.abort, &self.store.fatal_tx);
        self.complete = true;
    }
}

impl PostgresStore {
    pub const WALLET_PROFILE: &'static str = "postgres-memory-tx-v1";
    pub const FUNCTION_NAME: &'static str = "wallet_commit";
    pub const APPLICATION_NAME: &'static str = APPLICATION_NAME;
    pub const SQL_FILE: &'static str = "benchmarks/scylla-wallet/sql/postgres_wallet.sql";
    pub const CLIENT_QUERY_TIMEOUT: Duration = CLIENT_QUERY_TIMEOUT;

    pub async fn connect(
        dsn: &str,
        schema: &str,
        connections: usize,
        metrics: Arc<Metrics>,
    ) -> Result<Arc<Self>> {
        validate_schema_name(schema)?;
        if connections < SETUP_CONCURRENCY {
            return Err(Error::InvalidConfiguration(format!(
                "PostgreSQL pool D must be at least {SETUP_CONCURRENCY}"
            )));
        }

        let (fatal_tx, fatal_rx) = watch::channel(false);
        let mut raw_connections = Vec::with_capacity(connections);
        let first = Self::connect_raw(dsn, fatal_tx.clone()).await?;
        raw_connections.push(first);
        let dsn = Arc::<str>::from(dsn);
        let remaining = futures::stream::iter(1..connections)
            .map(|_| {
                let dsn = dsn.clone();
                let fatal_tx = fatal_tx.clone();
                async move { Self::connect_raw(&dsn, fatal_tx).await }
            })
            .buffer_unordered(SETUP_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
        raw_connections.extend(remaining);

        for raw in &raw_connections {
            validate_connection_settings(&raw.settings)?;
        }
        let first_client = &raw_connections[0].client;
        let existing_schema = first_client
            .query_opt("SELECT 1 FROM pg_namespace WHERE nspname = $1", &[&schema])
            .await
            .map_err(|_| Error::Storage("inspect PostgreSQL schema namespace".to_owned()))?;
        if existing_schema.is_some() {
            return Err(Error::InvalidConfiguration(format!(
                "PostgreSQL schema {schema} already exists; use a fresh owned schema"
            )));
        }

        let quoted_schema = format!("\"{schema}\"");
        first_client
            .batch_execute(&format!("CREATE SCHEMA {quoted_schema}"))
            .await
            .map_err(|_| Error::Storage("create fresh PostgreSQL wallet schema".to_owned()))?;
        for raw in &raw_connections {
            raw.client
                .batch_execute(&format!("SET search_path TO {quoted_schema}"))
                .await
                .map_err(|_| {
                    Error::Storage("set PostgreSQL wallet schema search path".to_owned())
                })?;
        }
        first_client
            .batch_execute(WALLET_SCHEMA_SQL)
            .await
            .map_err(|_| Error::Storage("install PostgreSQL wallet schema".to_owned()))?;

        let mut ready =
            futures::stream::iter(raw_connections.into_iter().enumerate())
                .map(|(index, raw)| async move {
                    let commit = raw.client.prepare(COMMIT_SQL).await.map_err(|_| {
                        Error::Storage("prepare PostgreSQL wallet function".to_owned())
                    })?;
                    let seed = raw.client.prepare(SEED_SQL).await.map_err(|_| {
                        Error::Storage("prepare PostgreSQL account seed".to_owned())
                    })?;
                    let preload = raw.client.prepare(PRELOAD_SQL).await.map_err(|_| {
                        Error::Storage("prepare PostgreSQL account preload".to_owned())
                    })?;
                    let ledger_scan = raw.client.prepare(LEDGER_SCAN_SQL).await.map_err(|_| {
                        Error::Storage("prepare PostgreSQL ledger verification".to_owned())
                    })?;
                    Ok::<_, Error>((
                        index,
                        PoolConnection {
                            client: raw.client,
                            alive: raw.alive,
                            abort: raw.abort,
                            settings: raw.settings,
                            commit,
                            seed,
                            preload,
                            ledger_scan,
                        },
                    ))
                })
                .buffer_unordered(SETUP_CONCURRENCY)
                .try_collect::<Vec<_>>()
                .await?;
        ready.sort_by_key(|(index, _)| *index);
        let pool = ready
            .into_iter()
            .map(|(_, connection)| connection)
            .collect::<Vec<_>>();
        if pool.len() != connections {
            return Err(Error::Storage(format!(
                "PostgreSQL pool established {} connections; expected {connections}",
                pool.len()
            )));
        }

        let pids = pool
            .iter()
            .map(|connection| connection.settings.backend_pid.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let session_id = format!("postgres:{schema}:{pids}");
        let (available_tx, available_rx) = mpsc::unbounded_channel();
        for index in 0..pool.len() {
            available_tx
                .send(index)
                .map_err(|_| Error::Storage("initialize PostgreSQL connection pool".to_owned()))?;
        }
        let schema_sql_sha256 = format!("{:x}", Sha256::digest(WALLET_SCHEMA_SQL.as_bytes()));

        Ok(Arc::new(Self {
            schema: schema.to_owned(),
            session_id,
            connections: pool,
            available_tx,
            available_rx: Mutex::new(available_rx),
            metrics,
            driver_counters: AtomicDriverCounters::default(),
            schema_sql_sha256,
            fatal_tx,
            fatal_rx,
        }))
    }

    async fn connect_raw(dsn: &str, fatal_tx: watch::Sender<bool>) -> Result<RawConnection> {
        let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.map_err(|_| {
            Error::Storage("connect to PostgreSQL failed; connection details withheld".to_owned())
        })?;
        let alive = Arc::new(AtomicBool::new(true));
        let connection_alive = alive.clone();
        let task = tokio::spawn(async move {
            let _ = connection.await;
            connection_alive.store(false, Ordering::Release);
            fatal_tx.send_replace(true);
        });
        let abort = task.abort_handle();

        let app_name = APPLICATION_NAME;
        client
            .query_one(
                "SELECT set_config('application_name', $1, false)",
                &[&app_name],
            )
            .await
            .map_err(|_| Error::Storage("set PostgreSQL application name".to_owned()))?;
        let row = client
            .query_one(
                "SELECT current_database(), pg_backend_pid(), current_setting('application_name'), current_setting('synchronous_commit'), current_setting('fsync'), current_setting('full_page_writes'), current_setting('commit_delay'), pg_is_in_recovery(), current_setting('synchronous_standby_names')",
                &[],
            )
            .await
            .map_err(|_| Error::Storage("read PostgreSQL durability settings".to_owned()))?;
        let database: String = row
            .try_get(0)
            .map_err(|_| Error::Corruption("decode PostgreSQL database name".to_owned()))?;
        if database != POSTGRES_DATABASE {
            abort.abort();
            return Err(Error::InvalidConfiguration(format!(
                "PostgreSQL DSN must connect to database {POSTGRES_DATABASE}"
            )));
        }
        let settings = PostgresConnectionSettings {
            backend_pid: row
                .try_get(1)
                .map_err(|_| Error::Corruption("decode PostgreSQL backend PID".to_owned()))?,
            application_name: row
                .try_get(2)
                .map_err(|_| Error::Corruption("decode PostgreSQL application name".to_owned()))?,
            synchronous_commit: row.try_get(3).map_err(|_| {
                Error::Corruption("decode PostgreSQL synchronous_commit".to_owned())
            })?,
            fsync: row
                .try_get(4)
                .map_err(|_| Error::Corruption("decode PostgreSQL fsync".to_owned()))?,
            full_page_writes: row
                .try_get(5)
                .map_err(|_| Error::Corruption("decode PostgreSQL full_page_writes".to_owned()))?,
            commit_delay: row
                .try_get(6)
                .map_err(|_| Error::Corruption("decode PostgreSQL commit_delay".to_owned()))?,
            is_standby: row
                .try_get(7)
                .map_err(|_| Error::Corruption("decode PostgreSQL recovery status".to_owned()))?,
            synchronous_standby_names: row.try_get(8).map_err(|_| {
                Error::Corruption("decode PostgreSQL synchronous_standby_names".to_owned())
            })?,
        };
        Ok(RawConnection {
            client,
            alive,
            abort,
            settings,
        })
    }

    pub fn schema(&self) -> &str {
        &self.schema
    }

    pub fn session_id(&self) -> String {
        self.session_id.clone()
    }

    pub fn data_connections(&self) -> u64 {
        self.connections
            .iter()
            .filter(|connection| {
                connection.alive.load(Ordering::Acquire) && !connection.client.is_closed()
            })
            .count() as u64
    }

    pub fn driver_counters(&self) -> DriverCounters {
        DriverCounters {
            queries: self.driver_counters.queries.load(Ordering::Relaxed),
            errors: self.driver_counters.errors.load(Ordering::Relaxed),
            retries: 0,
            connection_timeouts: 0,
            request_timeouts: self
                .driver_counters
                .request_timeouts
                .load(Ordering::Relaxed),
        }
    }

    pub fn connection_settings(&self) -> Vec<PostgresConnectionSettings> {
        self.connections
            .iter()
            .map(|connection| connection.settings.clone())
            .collect()
    }

    pub fn schema_sql_sha256(&self) -> &str {
        &self.schema_sql_sha256
    }

    pub fn operation_counters(&self) -> PostgresOperationCounters {
        let counters = self.metrics.snapshot().counters;
        PostgresOperationCounters {
            function_calls: counters.get("pg_function_calls").copied().unwrap_or(0),
            ledger_insert_attempts: counters
                .get("pg_ledger_insert_attempts")
                .copied()
                .unwrap_or(0),
            account_updates: counters.get("pg_account_updates").copied().unwrap_or(0),
            duplicate_conflicts: counters.get("pg_duplicate_conflicts").copied().unwrap_or(0),
        }
    }

    async fn checkout(&self) -> Result<PoolLease<'_>> {
        let started = Instant::now();
        let index = match checkout_index(&self.available_rx, &self.fatal_rx).await {
            Ok(index) => index,
            Err(error) => {
                self.note_error();
                self.metrics.observe("pool_wait", started.elapsed());
                return Err(error);
            }
        };
        self.metrics.observe("pool_wait", started.elapsed());
        let connection = &self.connections[index];
        if *self.fatal_rx.borrow()
            || !connection.alive.load(Ordering::Acquire)
            || connection.client.is_closed()
        {
            self.note_error();
            fail_closed_connection(&connection.alive, &connection.abort, &self.fatal_tx);
            return Err(Error::CommitOutcomeUnknown(
                "PostgreSQL pool returned a dead connection".to_owned(),
            ));
        }
        Ok(PoolLease {
            store: self,
            index,
            complete: false,
        })
    }

    fn note_query(&self) {
        self.driver_counters.queries.fetch_add(1, Ordering::Relaxed);
    }

    fn note_error(&self) {
        self.driver_counters.errors.fetch_add(1, Ordering::Relaxed);
    }

    pub async fn seed_account(&self, account_id: u64, balance: u64) -> Result<()> {
        let account = postgres_key(account_id, "account id")?;
        let encoded_balance = balance.to_be_bytes().to_vec();
        let lease = self.checkout().await?;
        self.note_query();
        let result = lease
            .connection()
            .client
            .execute(&lease.connection().seed, &[&account, &encoded_balance])
            .await;
        match result {
            Ok(1) => {
                lease.finish();
                Ok(())
            }
            Ok(_) => {
                lease.finish();
                Err(Error::Corruption(
                    "PostgreSQL account seed affected an unexpected row count".to_owned(),
                ))
            }
            Err(error) if postgres_error_confirms_rollback(&error) => {
                self.note_error();
                lease.finish();
                Err(Error::Conflict(format!(
                    "account {account_id} already has a seed row"
                )))
            }
            Err(_) => {
                self.note_error();
                lease.poison();
                Err(Error::Storage(
                    "PostgreSQL seed connection failed".to_owned(),
                ))
            }
        }
    }

    pub async fn preload_account_state(&self, account_id: u64) -> Result<AccountState> {
        let account = postgres_key(account_id, "account id")?;
        let lease = self.checkout().await?;
        self.note_query();
        let result = lease
            .connection()
            .client
            .query_opt(&lease.connection().preload, &[&account])
            .await;
        match result {
            Ok(Some(row)) => {
                lease.finish();
                decode_account_state(&row, account_id)
            }
            Ok(None) => {
                lease.finish();
                Err(Error::Corruption(format!("missing account {account_id}")))
            }
            Err(_) => {
                self.note_error();
                lease.poison();
                Err(Error::Storage(
                    "PostgreSQL account preload failed".to_owned(),
                ))
            }
        }
    }

    pub async fn verify_account(
        &self,
        account_id: u64,
    ) -> Result<(AccountState, Vec<LedgerRecord>)> {
        let account = postgres_key(account_id, "account id")?;
        let lease = self.checkout().await?;
        self.note_query();
        let state_result = lease
            .connection()
            .client
            .query_opt(&lease.connection().preload, &[&account])
            .await;
        let state_row = match state_result {
            Ok(Some(row)) => row,
            Ok(None) => {
                lease.finish();
                return Err(Error::Corruption(format!("missing account {account_id}")));
            }
            Err(_) => {
                self.note_error();
                lease.poison();
                return Err(Error::Storage(
                    "PostgreSQL verification account read failed".to_owned(),
                ));
            }
        };
        let state = match decode_account_state(&state_row, account_id) {
            Ok(state) => state,
            Err(error) => {
                lease.finish();
                return Err(error);
            }
        };

        self.note_query();
        let rows = match lease
            .connection()
            .client
            .query(&lease.connection().ledger_scan, &[&account])
            .await
        {
            Ok(rows) => rows,
            Err(_) => {
                self.note_error();
                lease.poison();
                return Err(Error::Storage(
                    "PostgreSQL verification ledger read failed".to_owned(),
                ));
            }
        };
        lease.finish();
        if rows.len() > MAX_LEDGER_ROWS {
            return Err(Error::Corruption(format!(
                "account {account_id} exceeds bounded verification limit {MAX_LEDGER_ROWS}"
            )));
        }
        let mut records = rows
            .iter()
            .map(|row| decode_ledger_record(row, account_id))
            .collect::<Result<Vec<_>>>()?;
        records.sort_by_key(|record| record.result.version);
        Ok((state, records))
    }
}

#[async_trait]
impl WalletStore for PostgresStore {
    async fn preload_state(&self, account_id: u64) -> Result<AccountState> {
        self.preload_account_state(account_id).await
    }

    async fn commit(
        &self,
        account_id: u64,
        expected_state: &AccountState,
        next_state: &AccountState,
        record: &LedgerRecord,
    ) -> Result<CommitResolution> {
        validate_commit(account_id, expected_state, next_state, record)?;
        let account = postgres_key(account_id, "account id")?;
        let tx_id = postgres_key(record.request.tx_id, "transaction id")?;
        let operation = encode_operation(record.request.operation);
        let amount = record.request.amount.to_be_bytes().to_vec();
        let outcome = encode_outcome(record.result.outcome);
        let balance_after = record.result.balance.to_be_bytes().to_vec();
        let version = postgres_key(record.result.version, "version")?;
        let write_clock = record.write_clock;

        let lease = self.checkout().await?;
        let database_activity = self.metrics.database_enter();
        self.metrics.increment_counter("pg_function_calls");
        self.metrics.increment_counter("pg_ledger_insert_attempts");
        let transaction_started = Instant::now();
        self.note_query();
        let response = tokio::time::timeout(
            CLIENT_QUERY_TIMEOUT,
            lease.connection().client.query_one(
                &lease.connection().commit,
                &[
                    &account,
                    &tx_id,
                    &operation,
                    &amount,
                    &outcome,
                    &balance_after,
                    &version,
                    &write_clock,
                ],
            ),
        )
        .await;
        let row = finish_query_activity_before_pool_action(
            &self.metrics,
            transaction_started,
            database_activity,
            || match response {
                Ok(Ok(row)) => {
                    lease.finish();
                    Ok(row)
                }
                Ok(Err(error)) if postgres_error_confirms_rollback(&error) => {
                    self.note_error();
                    lease.finish();
                    Err(Error::CommitConfirmedRollback(
                        "PostgreSQL returned a server error and rolled back the implicit transaction"
                            .to_owned(),
                    ))
                }
                Ok(Err(_)) => {
                    self.note_error();
                    lease.poison();
                    Err(Error::CommitOutcomeUnknown(
                        "PostgreSQL connection failed while awaiting transaction completion"
                            .to_owned(),
                    ))
                }
                Err(_) => {
                    self.note_error();
                    self.driver_counters
                        .request_timeouts
                        .fetch_add(1, Ordering::Relaxed);
                    lease.poison();
                    Err(Error::CommitOutcomeUnknown(
                        "PostgreSQL client query timed out; commit outcome is unknown".to_owned(),
                    ))
                }
            },
        )?;

        let inserted = match row.try_get::<_, bool>("did_insert") {
            Ok(inserted) => inserted,
            Err(_) => {
                return Err(Error::CommitOutcomeUnknown(
                    "PostgreSQL transaction completed but its response could not be decoded"
                        .to_owned(),
                ));
            }
        };
        if inserted {
            self.metrics.increment_counter("pg_account_updates");
        } else {
            self.metrics.increment_counter("pg_duplicate_conflicts");
        }
        let current = match decode_returned_account(&row) {
            Ok(state) => state,
            Err(error) => {
                return Err(Error::CommitOutcomeUnknown(format!(
                    "PostgreSQL transaction completed but account result could not be decoded: {error}"
                )));
            }
        };
        let stored = match decode_returned_record(&row, account_id, record.request.tx_id) {
            Ok(record) => record,
            Err(error) => {
                return Err(Error::CommitOutcomeUnknown(format!(
                    "PostgreSQL transaction completed but ledger result could not be decoded: {error}"
                )));
            }
        };

        if inserted {
            if current != *next_state || stored != *record {
                return Err(Error::CommitOutcomeUnknown(
                    "PostgreSQL committed a wallet transaction with an inconsistent result"
                        .to_owned(),
                ));
            }
            Ok(CommitResolution::Applied)
        } else {
            Ok(CommitResolution::Existing {
                current,
                record: stored,
            })
        }
    }
}

fn finish_query_activity_before_pool_action<T>(
    metrics: &Metrics,
    transaction_started: Instant,
    database_activity: ActivityGuard<'_>,
    pool_action: impl FnOnce() -> T,
) -> T {
    metrics.observe("sql_transaction", transaction_started.elapsed());
    drop(database_activity);
    pool_action()
}

fn validate_connection_settings(settings: &PostgresConnectionSettings) -> Result<()> {
    let valid = settings.application_name == APPLICATION_NAME
        && settings.synchronous_commit == "on"
        && settings.fsync == "on"
        && settings.full_page_writes == "on"
        && settings.commit_delay == "0"
        && !settings.is_standby
        && settings.synchronous_standby_names.is_empty();
    if valid {
        Ok(())
    } else {
        Err(Error::InvalidConfiguration(format!(
            "PostgreSQL connection {} fails required durability settings",
            settings.backend_pid
        )))
    }
}

fn postgres_error_confirms_rollback(error: &tokio_postgres::Error) -> bool {
    error.as_db_error().is_some_and(|database_error| {
        postgres_sqlstate_confirms_rollback(
            database_error.parsed_severity(),
            database_error.code().code(),
        )
    })
}

fn postgres_sqlstate_confirms_rollback(severity: Option<Severity>, code: &str) -> bool {
    severity == Some(Severity::Error)
        && (code == "P0001" || code.starts_with("23") || code == "40001" || code == "40P01")
}

async fn wait_for_pool_failure(fatal_rx: &mut watch::Receiver<bool>) {
    loop {
        if *fatal_rx.borrow() || fatal_rx.changed().await.is_err() {
            return;
        }
    }
}

async fn checkout_index(
    available_rx: &Mutex<mpsc::UnboundedReceiver<usize>>,
    fatal_rx: &watch::Receiver<bool>,
) -> Result<usize> {
    let mut fatal_rx = fatal_rx.clone();
    if *fatal_rx.borrow() {
        return Err(Error::CommitOutcomeUnknown(
            "PostgreSQL pool failed closed".to_owned(),
        ));
    }
    let mut available_rx = tokio::select! {
        biased;
        _ = wait_for_pool_failure(&mut fatal_rx) => {
            return Err(Error::CommitOutcomeUnknown("PostgreSQL pool failed closed".to_owned()));
        }
        receiver = available_rx.lock() => receiver,
    };
    if *fatal_rx.borrow() {
        return Err(Error::CommitOutcomeUnknown(
            "PostgreSQL pool failed closed".to_owned(),
        ));
    }
    let index = tokio::select! {
        biased;
        _ = wait_for_pool_failure(&mut fatal_rx) => {
            Err(Error::CommitOutcomeUnknown("PostgreSQL pool failed closed".to_owned()))
        }
        index = available_rx.recv() => index.ok_or_else(|| {
            Error::CommitOutcomeUnknown("PostgreSQL pool is closed".to_owned())
        }),
    }?;
    if *fatal_rx.borrow() {
        return Err(Error::CommitOutcomeUnknown(
            "PostgreSQL pool failed closed".to_owned(),
        ));
    }
    Ok(index)
}

fn fail_closed_connection(alive: &AtomicBool, abort: &AbortHandle, fatal_tx: &watch::Sender<bool>) {
    fatal_tx.send_replace(true);
    alive.store(false, Ordering::Release);
    abort.abort();
}

fn validate_schema_name(schema: &str) -> Result<()> {
    if schema.len() <= "wallet_bench_".len()
        || !schema.starts_with("wallet_bench_")
        || schema.len() > 63
        || !schema
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(Error::InvalidConfiguration(
            "PostgreSQL schema must match wallet_bench_[a-z0-9_]+ and fit PostgreSQL's 63-byte identifier limit".to_owned(),
        ));
    }
    Ok(())
}

fn validate_commit(
    account_id: u64,
    expected: &AccountState,
    next: &AccountState,
    record: &LedgerRecord,
) -> Result<()> {
    postgres_key(account_id, "account id")?;
    postgres_key(record.request.tx_id, "transaction id")?;
    if record.request.account_id != account_id {
        return Err(Error::Corruption(
            "commit account id does not match ledger request".to_owned(),
        ));
    }
    postgres_key(record.result.version, "version")?;
    if record.result.version != next.version
        || record.result.balance != next.balance
        || record.write_clock != next.write_clock
        || next.version
            != expected.version.checked_add(1).ok_or_else(|| {
                Error::Corruption("account version exhausted before commit".to_owned())
            })?
        || next.write_clock
            != expected
                .write_clock
                .checked_add(1)
                .ok_or_else(|| Error::Corruption("logical account clock exhausted".to_owned()))?
    {
        return Err(Error::Corruption(
            "proposed account state and ledger result are inconsistent".to_owned(),
        ));
    }
    Ok(())
}

fn postgres_key(value: u64, label: &str) -> Result<i64> {
    i64::try_from(value)
        .map_err(|_| Error::Corruption(format!("{label} exceeds PostgreSQL bigint")))
}

fn encode_operation(operation: Operation) -> i16 {
    match operation {
        Operation::Credit => 0,
        Operation::Debit => 1,
    }
}

fn decode_operation(operation: i16) -> Result<Operation> {
    match operation {
        0 => Ok(Operation::Credit),
        1 => Ok(Operation::Debit),
        other => Err(Error::Corruption(format!(
            "unknown PostgreSQL wallet operation code {other}"
        ))),
    }
}

fn encode_outcome(outcome: Outcome) -> i16 {
    match outcome {
        Outcome::Applied => 0,
        Outcome::InsufficientFunds => 1,
        Outcome::CreditOverflow => 2,
        Outcome::InvalidAmount => 3,
    }
}

fn decode_outcome(outcome: i16) -> Result<Outcome> {
    match outcome {
        0 => Ok(Outcome::Applied),
        1 => Ok(Outcome::InsufficientFunds),
        2 => Ok(Outcome::CreditOverflow),
        3 => Ok(Outcome::InvalidAmount),
        other => Err(Error::Corruption(format!(
            "unknown PostgreSQL wallet outcome code {other}"
        ))),
    }
}

fn decode_account_state(row: &Row, account_id: u64) -> Result<AccountState> {
    let balance = decode_u64(
        &row.try_get::<_, Vec<u8>>("balance")
            .map_err(|_| Error::Corruption("decode PostgreSQL account balance".to_owned()))?,
        "PostgreSQL account balance",
    )?;
    let version = u64::try_from(
        row.try_get::<_, i64>("version")
            .map_err(|_| Error::Corruption("decode PostgreSQL account version".to_owned()))?,
    )
    .map_err(|_| Error::Corruption(format!("account {account_id} has a negative version")))?;
    let write_clock = row
        .try_get::<_, i64>("write_clock")
        .map_err(|_| Error::Corruption("decode PostgreSQL account write clock".to_owned()))?;
    if write_clock < 0 {
        return Err(Error::Corruption(format!(
            "account {account_id} has a negative write clock"
        )));
    }
    Ok(AccountState {
        balance,
        version,
        write_clock,
    })
}

fn decode_ledger_record(row: &Row, account_id: u64) -> Result<LedgerRecord> {
    let stored_account = row
        .try_get::<_, i64>("account_id")
        .map_err(|_| Error::Corruption("decode PostgreSQL ledger account id".to_owned()))?;
    let tx_id = row
        .try_get::<_, i64>("tx_id")
        .map_err(|_| Error::Corruption("decode PostgreSQL ledger transaction id".to_owned()))?;
    if stored_account != postgres_key(account_id, "account id")? {
        return Err(Error::Corruption(
            "PostgreSQL ledger row belongs to another account".to_owned(),
        ));
    }
    let typed_tx_id = u64::try_from(tx_id)
        .map_err(|_| Error::Corruption("negative PostgreSQL ledger transaction id".to_owned()))?;
    let operation = decode_operation(
        row.try_get::<_, i16>("operation")
            .map_err(|_| Error::Corruption("decode PostgreSQL ledger operation".to_owned()))?,
    )?;
    let amount = decode_u64(
        &row.try_get::<_, Vec<u8>>("amount")
            .map_err(|_| Error::Corruption("decode PostgreSQL ledger amount".to_owned()))?,
        "PostgreSQL ledger amount",
    )?;
    let outcome = decode_outcome(
        row.try_get::<_, i16>("outcome")
            .map_err(|_| Error::Corruption("decode PostgreSQL ledger outcome".to_owned()))?,
    )?;
    let balance = decode_u64(
        &row.try_get::<_, Vec<u8>>("balance_after")
            .map_err(|_| Error::Corruption("decode PostgreSQL ledger balance".to_owned()))?,
        "PostgreSQL ledger balance_after",
    )?;
    let version = u64::try_from(
        row.try_get::<_, i64>("version")
            .map_err(|_| Error::Corruption("decode PostgreSQL ledger version".to_owned()))?,
    )
    .map_err(|_| Error::Corruption("negative PostgreSQL ledger version".to_owned()))?;
    let write_clock = row
        .try_get::<_, i64>("write_clock")
        .map_err(|_| Error::Corruption("decode PostgreSQL ledger write clock".to_owned()))?;
    if write_clock < 0 {
        return Err(Error::Corruption(
            "negative PostgreSQL ledger write clock".to_owned(),
        ));
    }
    Ok(LedgerRecord {
        request: Transaction {
            account_id,
            tx_id: typed_tx_id,
            operation,
            amount,
        },
        result: TransactionResult {
            outcome,
            balance,
            version,
            replayed: false,
        },
        write_clock,
    })
}

fn decode_returned_account(row: &Row) -> Result<AccountState> {
    let balance = decode_u64(
        &row.try_get::<_, Vec<u8>>("current_balance").map_err(|_| {
            Error::Corruption("decode PostgreSQL returned account balance".to_owned())
        })?,
        "PostgreSQL returned account balance",
    )?;
    let version =
        u64::try_from(row.try_get::<_, i64>("current_version").map_err(|_| {
            Error::Corruption("decode PostgreSQL returned account version".to_owned())
        })?)
        .map_err(|_| {
            Error::Corruption("negative PostgreSQL returned account version".to_owned())
        })?;
    let write_clock = row.try_get::<_, i64>("current_write_clock").map_err(|_| {
        Error::Corruption("decode PostgreSQL returned account write clock".to_owned())
    })?;
    if write_clock < 0 {
        return Err(Error::Corruption(
            "negative PostgreSQL returned account write clock".to_owned(),
        ));
    }
    Ok(AccountState {
        balance,
        version,
        write_clock,
    })
}

fn decode_returned_record(row: &Row, account_id: u64, tx_id: u64) -> Result<LedgerRecord> {
    let amount = decode_u64(
        &row.try_get::<_, Vec<u8>>("record_amount")
            .map_err(|_| Error::Corruption("decode PostgreSQL returned amount".to_owned()))?,
        "PostgreSQL returned amount",
    )?;
    let operation = decode_operation(
        row.try_get::<_, i16>("record_operation")
            .map_err(|_| Error::Corruption("decode PostgreSQL returned operation".to_owned()))?,
    )?;
    let version = u64::try_from(
        row.try_get::<_, i64>("record_version")
            .map_err(|_| Error::Corruption("decode PostgreSQL returned version".to_owned()))?,
    )
    .map_err(|_| Error::Corruption("negative PostgreSQL returned version".to_owned()))?;
    let write_clock = row
        .try_get::<_, i64>("record_write_clock")
        .map_err(|_| Error::Corruption("decode PostgreSQL returned write clock".to_owned()))?;
    if write_clock < 0 {
        return Err(Error::Corruption(
            "negative PostgreSQL returned write clock".to_owned(),
        ));
    }
    let balance = decode_u64(
        &row.try_get::<_, Vec<u8>>("record_balance_after")
            .map_err(|_| Error::Corruption("decode PostgreSQL returned balance".to_owned()))?,
        "PostgreSQL returned balance_after",
    )?;
    let outcome = decode_outcome(
        row.try_get::<_, i16>("record_outcome")
            .map_err(|_| Error::Corruption("decode PostgreSQL returned outcome".to_owned()))?,
    )?;
    Ok(LedgerRecord {
        request: Transaction {
            account_id,
            tx_id,
            operation,
            amount,
        },
        result: TransactionResult {
            outcome,
            balance,
            version,
            replayed: false,
        },
        write_clock,
    })
}

fn decode_u64(bytes: &[u8], label: &str) -> Result<u64> {
    let encoded: [u8; 8] = bytes.try_into().map_err(|_| {
        Error::Corruption(format!(
            "{label} has {} bytes; expected exactly 8",
            bytes.len()
        ))
    })?;
    Ok(u64::from_be_bytes(encoded))
}

#[cfg(test)]
mod tests {
    use super::{
        PostgresConnectionSettings, checkout_index, fail_closed_connection,
        finish_query_activity_before_pool_action, postgres_sqlstate_confirms_rollback,
        validate_connection_settings, validate_schema_name,
    };
    use crate::Error;
    use crate::metrics::Metrics;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};
    use tokio::sync::{Mutex, mpsc, watch};

    #[test]
    fn postgres_schema_names_are_owned_and_safe_to_quote_as_identifiers() {
        assert!(validate_schema_name("wallet_bench_case_20261013").is_ok());
        for invalid in [
            "public",
            "wallet_bench_",
            "wallet_bench_bad-name",
            "wallet_bench_Ünicode",
        ] {
            assert!(matches!(
                validate_schema_name(invalid),
                Err(Error::InvalidConfiguration(_))
            ));
        }
        assert!(validate_schema_name(&format!("wallet_bench_{}", "x".repeat(52))).is_err());
    }

    #[test]
    fn durability_settings_require_local_sync_commit_and_no_synchronous_standby() {
        let settings = PostgresConnectionSettings {
            backend_pid: 123,
            application_name: "ledger-wallet-postgres-wallet-bench".to_owned(),
            synchronous_commit: "on".to_owned(),
            fsync: "on".to_owned(),
            full_page_writes: "on".to_owned(),
            commit_delay: "0".to_owned(),
            is_standby: false,
            synchronous_standby_names: String::new(),
        };
        assert!(validate_connection_settings(&settings).is_ok());
        let mut invalid = settings;
        invalid.synchronous_commit = "local".to_owned();
        assert!(validate_connection_settings(&invalid).is_err());
    }

    #[test]
    fn only_known_statement_rollback_errors_are_classified_as_confirmed() {
        use tokio_postgres::error::Severity;

        assert!(postgres_sqlstate_confirms_rollback(
            Some(Severity::Error),
            "P0001"
        ));
        assert!(postgres_sqlstate_confirms_rollback(
            Some(Severity::Error),
            "23505"
        ));
        assert!(postgres_sqlstate_confirms_rollback(
            Some(Severity::Error),
            "40001"
        ));
        assert!(postgres_sqlstate_confirms_rollback(
            Some(Severity::Error),
            "40P01"
        ));
        assert!(!postgres_sqlstate_confirms_rollback(
            Some(Severity::Fatal),
            "P0001"
        ));
        assert!(!postgres_sqlstate_confirms_rollback(
            Some(Severity::Panic),
            "23505"
        ));
        assert!(!postgres_sqlstate_confirms_rollback(
            Some(Severity::Error),
            "57014"
        ));
        assert!(!postgres_sqlstate_confirms_rollback(
            Some(Severity::Error),
            "57P01"
        ));
        assert!(!postgres_sqlstate_confirms_rollback(None, "40001"));
    }

    #[test]
    fn completed_sql_activity_is_gone_before_connection_pool_handoff() {
        let metrics = Metrics::new();
        metrics.reset_postgres();
        metrics.enable();
        let database_activity = metrics.database_enter();
        assert_eq!(metrics.snapshot().cql_in_flight_current, 1);

        let (handoff_tx, handoff_rx) = std::sync::mpsc::channel();
        finish_query_activity_before_pool_action(
            &metrics,
            Instant::now(),
            database_activity,
            || {
                // This callback is where commit returns or poisons its checked-out lease.
                let snapshot = metrics.snapshot();
                handoff_tx
                    .send((
                        snapshot.cql_in_flight_current,
                        snapshot.stages["sql_transaction"].count,
                    ))
                    .expect("pool handoff observer remains connected");
            },
        );

        assert_eq!(
            handoff_rx.recv().expect("pool handoff callback ran"),
            (0, 1),
            "the SQL stage is observed and shared DB activity is cleared before lease action"
        );
    }

    #[tokio::test]
    async fn poisoned_pool_wakes_every_checkout_waiter_without_reconnecting() {
        let (fatal_tx, fatal_rx) = watch::channel(false);
        let (available_tx, available_rx) = mpsc::unbounded_channel::<usize>();
        let available_rx = Arc::new(Mutex::new(available_rx));
        let mut waiters = Vec::new();
        for _ in 0..8 {
            let fatal_rx = fatal_rx.clone();
            let available_rx = available_rx.clone();
            waiters.push(tokio::spawn(async move {
                checkout_index(&available_rx, &fatal_rx).await
            }));
        }

        tokio::task::yield_now().await;
        let connection_alive = AtomicBool::new(true);
        let connection_task = tokio::spawn(std::future::pending::<()>());
        let abort = connection_task.abort_handle();
        fail_closed_connection(&connection_alive, &abort, &fatal_tx);
        assert!(!connection_alive.load(Ordering::Acquire));
        assert!(*fatal_tx.borrow());
        assert!(connection_task.await.unwrap_err().is_cancelled());
        tokio::time::timeout(Duration::from_secs(1), async {
            for waiter in waiters {
                assert!(matches!(
                    waiter.await.expect("pool waiter should exit normally"),
                    Err(Error::CommitOutcomeUnknown(_))
                ));
            }
        })
        .await
        .expect("all waiters must observe fatal pool state");
        assert!(available_rx.lock().await.try_recv().is_err());
        drop(available_tx);
    }
}
