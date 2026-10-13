use crate::metrics::Metrics;
use crate::{
    AccountState, CommitResolution, ConsistencyLevel, DriverCounters, Error, LedgerRecord,
    Operation, Outcome, Result, Transaction, TransactionResult,
};
use async_trait::async_trait;
use scylla::client::PoolSize;
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::policies::retry::FallthroughRetryPolicy;
use scylla::response::query_result::QueryResult;
use scylla::statement::Consistency;
use scylla::statement::SerialConsistency;
use scylla::statement::Statement;
use scylla::statement::prepared::PreparedStatement;
use scylla::value::{CqlValue, Row};
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;

const TABLE: &str = "wallet";
const VERIFY_LIMIT: usize = 1_000;
const POOL_READY_DEADLINE: Duration = Duration::from_secs(60);
const LEDGER_KIND: i8 = 1;
const ACCOUNT_KIND: i8 = 0;

#[async_trait]
pub trait WalletStore: Send + Sync {
    async fn preload_state(&self, account_id: u64) -> Result<AccountState>;

    async fn commit(
        &self,
        account_id: u64,
        expected_state: &AccountState,
        next_state: &AccountState,
        record: &LedgerRecord,
    ) -> Result<CommitResolution>;
}

pub struct ScyllaStore {
    session: Arc<Session>,
    keyspace: String,
    account_state_read: PreparedStatement,
    ledger_scan: PreparedStatement,
    seed_insert: PreparedStatement,
    conditional_insert: PreparedStatement,
    _metrics: Arc<Metrics>,
}

impl ScyllaStore {
    pub const WALLET_PROFILE: &'static str = "wallet-memory-lwt-v1";
    pub const LEDGER_KIND: i8 = LEDGER_KIND;
    pub const ACCOUNT_KIND: i8 = ACCOUNT_KIND;
    pub const CQL_REQUESTS_PER_TRANSACTION: u64 = 1;
    pub const CQL_STATEMENT_COUNT: usize = 1;
    pub const SERIAL_CONSISTENCY: &'static str = "LOCAL_SERIAL";

    pub async fn connect(
        nodes: &[String],
        keyspace: &str,
        shards: usize,
        workers: usize,
        metrics: Arc<Metrics>,
    ) -> Result<Arc<Self>> {
        validate_configuration(nodes, keyspace, shards, workers)?;
        let per_shard = workers / (nodes.len() * shards);
        Self::connect_with_pool(nodes, keyspace, shards, per_shard, metrics).await
    }

    /// Connect with an explicit per-node, per-shard driver pool target. Worker
    /// concurrency is configured by Service and is intentionally independent.
    pub async fn connect_with_pool(
        nodes: &[String],
        keyspace: &str,
        shards: usize,
        per_shard_pool: usize,
        metrics: Arc<Metrics>,
    ) -> Result<Arc<Self>> {
        Self::connect_with_profile(
            nodes,
            keyspace,
            shards,
            per_shard_pool,
            3,
            ConsistencyLevel::Three,
            ConsistencyLevel::Three,
            metrics,
        )
        .await
    }

    /// Connect using explicit replication and wallet consistency settings.
    /// Schema metadata and DDL continue to use CL ONE independently.
    pub async fn connect_with_profile(
        nodes: &[String],
        keyspace: &str,
        shards: usize,
        per_shard_pool: usize,
        replication_factor: u8,
        read_consistency: ConsistencyLevel,
        write_consistency: ConsistencyLevel,
        metrics: Arc<Metrics>,
    ) -> Result<Arc<Self>> {
        validate_pool_configuration(nodes, keyspace, shards, per_shard_pool)?;
        validate_consistency_profile(
            replication_factor,
            read_consistency,
            write_consistency,
            nodes.len(),
        )?;
        let data_connection_target =
            checked_data_connection_target(nodes.len(), shards, per_shard_pool)?;
        let pool_size =
            PoolSize::PerShard(NonZeroUsize::new(per_shard_pool).ok_or_else(|| {
                Error::InvalidConfiguration("PerShard pool size is zero".to_owned())
            })?);
        let profile = ExecutionProfile::builder()
            .consistency(read_consistency.into())
            .retry_policy(Arc::new(FallthroughRetryPolicy::new()))
            .speculative_execution_policy(None)
            .build();
        let profile_handle = profile.into_handle();
        let session = SessionBuilder::new()
            .known_nodes(nodes.iter().cloned())
            .pool_size(pool_size)
            .default_execution_profile_handle(profile_handle)
            .build()
            .await
            .map_err(|error| Error::Storage(format!("connect to ScyllaDB: {error}")))?;
        let session = Arc::new(session);

        let datacenter = wait_for_pool_ready(&session, nodes.len(), data_connection_target).await?;
        ensure_keyspace_absent(&session, keyspace).await?;
        let escaped_datacenter = escape_cql_string(&datacenter);
        let create_keyspace =
            create_keyspace_statement(keyspace, &escaped_datacenter, replication_factor);
        execute_schema_change(&session, &create_keyspace, "create keyspace").await?;

        session
            .use_keyspace(keyspace, false)
            .await
            .map_err(|error| Error::Storage(format!("select benchmark keyspace: {error}")))?;

        let create_table = format!(
            "CREATE TABLE {TABLE} (account_id bigint, kind tinyint, tx_id bigint, balance blob static, version bigint static, write_clock bigint static, op tinyint, amount blob, outcome tinyint, balance_after blob, ledger_version bigint, ledger_write_clock bigint, PRIMARY KEY ((account_id), kind, tx_id)) WITH CLUSTERING ORDER BY (kind ASC, tx_id ASC)"
        );
        execute_schema_change(&session, &create_table, "create wallet table").await?;

        let mut account_state_read = prepare(
            &session,
            "SELECT kind, tx_id, balance, version, write_clock FROM wallet WHERE account_id = ? AND kind = 0 AND tx_id = 0",
        )
        .await?;
        let mut ledger_scan = prepare(
            &session,
            "SELECT kind, tx_id, op, amount, outcome, balance_after, ledger_version, ledger_write_clock FROM wallet WHERE account_id = ? AND kind = 1 LIMIT 1001",
        )
        .await?;
        let mut seed_insert = prepare(
            &session,
            "INSERT INTO wallet (account_id, kind, tx_id, balance, version, write_clock) VALUES (?, 0, 0, ?, 0, 0) IF NOT EXISTS",
        )
        .await?;
        let mut conditional_insert = prepare(
            &session,
            "INSERT INTO wallet (account_id, kind, tx_id, balance, version, write_clock, op, amount, outcome, balance_after, ledger_version, ledger_write_clock) VALUES (?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS",
        )
        .await?;

        for statement in [&mut account_state_read, &mut ledger_scan] {
            statement.set_consistency(read_consistency.into());
            statement.set_retry_policy(Some(Arc::new(FallthroughRetryPolicy::new())));
        }
        for statement in [&mut seed_insert, &mut conditional_insert] {
            statement.set_consistency(write_consistency.into());
            statement.set_serial_consistency(Some(SerialConsistency::LocalSerial));
            statement.set_retry_policy(Some(Arc::new(FallthroughRetryPolicy::new())));
        }

        Ok(Arc::new(Self {
            session,
            keyspace: keyspace.to_owned(),
            account_state_read,
            ledger_scan,
            seed_insert,
            conditional_insert,
            _metrics: metrics,
        }))
    }

    pub fn session_id(&self) -> String {
        self.session.session_id().to_string()
    }

    pub fn data_connections(&self) -> u64 {
        self.session.get_metrics().get_total_connections()
    }

    #[allow(deprecated)]
    pub fn driver_counters(&self) -> DriverCounters {
        let metrics = self.session.get_metrics();
        DriverCounters {
            queries: metrics.get_queries_num(),
            errors: metrics.get_errors_num(),
            retries: metrics.get_retries_num(),
            connection_timeouts: metrics.get_connection_timeouts(),
            request_timeouts: metrics.get_request_timeouts(),
        }
    }

    pub fn keyspace(&self) -> &str {
        &self.keyspace
    }

    pub async fn seed_account(&self, account_id: u64, balance: u64) -> Result<()> {
        let account = cql_key(account_id, "account id")?;
        let amount = balance.to_be_bytes().to_vec();
        self._metrics.increment_counter("cql_seed_lwt");
        let started = Instant::now();
        let query = {
            let _cql = self._metrics.cql_enter();
            self.session
                .execute_unpaged(&self.seed_insert, (account, amount))
                .await
                .map_err(|error| {
                    Error::CommitOutcomeUnknown(format!("seed account {account_id}: {error}"))
                })?
        };
        self._metrics.observe("cql_seed_lwt", started.elapsed());
        let fields = decode_lwt_fields(query).map_err(|error| {
            Error::CommitOutcomeUnknown(format!(
                "decode seed LWT response for account {account_id}: {error}"
            ))
        })?;
        match applied_field(&fields).map_err(|error| {
            Error::CommitOutcomeUnknown(format!(
                "decode seed LWT response for account {account_id}: {error}"
            ))
        })? {
            true => Ok(()),
            false => Err(Error::Conflict(format!(
                "account {account_id} already has a seed row"
            ))),
        }
    }

    /// Reads static account state during bounded setup, before the measurement gate.
    pub async fn preload_account_state(&self, account_id: u64) -> Result<AccountState> {
        self.read_account_state(account_id, "cql_preload_state")
            .await
    }

    pub async fn verify_account(
        &self,
        account_id: u64,
    ) -> Result<(AccountState, Vec<LedgerRecord>)> {
        let account = cql_key(account_id, "account id")?;
        let state = self
            .read_account_state(account_id, "cql_verify_state")
            .await?;
        self._metrics.increment_counter("cql_verify_ledger");
        let started = Instant::now();
        let query = {
            let _cql = self._metrics.cql_enter();
            self.session
                .execute_unpaged(&self.ledger_scan, (account,))
                .await
                .map_err(|error| {
                    Error::Storage(format!("read account {account_id} ledger: {error}"))
                })?
        };
        self._metrics
            .observe("cql_verify_ledger", started.elapsed());
        let rows = query
            .into_rows_result()
            .map_err(|error| Error::Corruption(format!("decode account ledger result: {error}")))?;
        if rows.rows_num() > VERIFY_LIMIT {
            return Err(Error::Corruption(format!(
                "account {account_id} exceeds bounded verification limit {VERIFY_LIMIT}"
            )));
        }

        let mut records = Vec::with_capacity(rows.rows_num());
        for row in rows
            .rows::<(i8, i64, i8, Vec<u8>, i8, Vec<u8>, i64, i64)>()
            .map_err(|error| Error::Corruption(format!("decode ledger rows: {error}")))?
        {
            let (kind, tx_id, operation, amount, outcome, balance_after, version, clock) =
                row.map_err(|error| Error::Corruption(format!("decode ledger row: {error}")))?;
            if kind != LEDGER_KIND {
                return Err(Error::Corruption(format!(
                    "account {account_id} ledger row has kind {kind}; expected {LEDGER_KIND}"
                )));
            }
            records.push(decode_record(
                account_id,
                tx_id,
                operation,
                amount,
                outcome,
                balance_after,
                version,
                clock,
            )?);
        }
        records.sort_by_key(|record| record.result.version);
        verify_ledger_state(account_id, &state, &records)?;
        Ok((state, records))
    }

    async fn read_account_state(
        &self,
        account_id: u64,
        counter: &'static str,
    ) -> Result<AccountState> {
        let account = cql_key(account_id, "account id")?;
        self._metrics.increment_counter(counter);
        let started = Instant::now();
        let query = {
            let _cql = self._metrics.cql_enter();
            self.session
                .execute_unpaged(&self.account_state_read, (account,))
                .await
                .map_err(|error| {
                    Error::Storage(format!("read account {account_id} static state: {error}"))
                })?
        };
        self._metrics.observe(counter, started.elapsed());
        decode_account_state_query(query, account_id)
    }
}

#[async_trait]
impl WalletStore for ScyllaStore {
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
        let account = cql_key(account_id, "account id")?;
        let tx = cql_key(record.request.tx_id, "transaction id")?;
        if record.request.account_id != account_id {
            return Err(Error::Corruption(
                "commit account id does not match ledger request".to_owned(),
            ));
        }
        if record.result.version > i64::MAX as u64 || next_state.version > i64::MAX as u64 {
            return Err(Error::Corruption("version exceeds CQL bigint".to_owned()));
        }
        if record.result.version != next_state.version
            || record.result.balance != next_state.balance
            || record.write_clock != next_state.write_clock
            || next_state.version
                != expected_state.version.checked_add(1).ok_or_else(|| {
                    Error::Corruption("account version exhausted before commit".to_owned())
                })?
            || next_state.write_clock
                != expected_state.write_clock.checked_add(1).ok_or_else(|| {
                    Error::Corruption("logical account clock exhausted".to_owned())
                })?
        {
            return Err(Error::Corruption(
                "proposed static state and ledger result are inconsistent".to_owned(),
            ));
        }

        let balance = next_state.balance.to_be_bytes().to_vec();
        let amount = record.request.amount.to_be_bytes().to_vec();
        let balance_after = record.result.balance.to_be_bytes().to_vec();
        let operation = encode_operation(record.request.operation);
        let outcome = encode_outcome(record.result.outcome);
        self._metrics.increment_counter("cql_write_lwt");
        let started = Instant::now();
        let response = {
            let _cql = self._metrics.cql_enter();
            self.session
                .execute_unpaged(
                    &self.conditional_insert,
                    (
                        account,
                        tx,
                        balance,
                        next_state.version as i64,
                        next_state.write_clock,
                        operation,
                        amount,
                        outcome,
                        balance_after,
                        record.result.version as i64,
                        record.write_clock,
                    ),
                )
                .await
        };
        self._metrics.observe("cql_write_lwt", started.elapsed());
        let query = response.map_err(|error| {
            Error::CommitOutcomeUnknown(format!("wallet conditional insert: {error}"))
        })?;
        let fields = decode_lwt_fields(query).map_err(|error| {
            Error::CommitOutcomeUnknown(format!("decode wallet LWT response: {error}"))
        })?;
        match applied_field(&fields).map_err(|error| {
            Error::CommitOutcomeUnknown(format!("decode wallet LWT response: {error}"))
        })? {
            true => Ok(CommitResolution::Applied),
            false => match decode_existing_lwt_fields(&fields, account_id, tx as u64) {
                Ok((current, existing)) => Ok(CommitResolution::Existing {
                    current,
                    record: existing,
                }),
                Err(Error::Conflict(message)) => Err(Error::Conflict(message)),
                Err(error) => Err(Error::CommitOutcomeUnknown(format!(
                    "decode existing wallet LWT row: {error}"
                ))),
            },
        }
    }
}

fn validate_configuration(
    nodes: &[String],
    keyspace: &str,
    shards: usize,
    workers: usize,
) -> Result<()> {
    validate_base_configuration(nodes, keyspace, shards)?;
    validate_consistency_profile(
        3,
        ConsistencyLevel::Three,
        ConsistencyLevel::Three,
        nodes.len(),
    )?;
    if workers == 0 {
        return Err(Error::InvalidConfiguration(
            "worker count must be positive".to_owned(),
        ));
    }
    let per_node_shards = shards.checked_mul(nodes.len()).ok_or_else(|| {
        Error::InvalidConfiguration("shard count overflows connection calculation".to_owned())
    })?;
    if workers % per_node_shards != 0 {
        return Err(Error::InvalidConfiguration(format!(
            "worker count {workers} must be divisible by {} nodes times {shards} shards",
            nodes.len()
        )));
    }
    if workers / per_node_shards == 0 {
        return Err(Error::InvalidConfiguration(
            "PerShard pool target must be positive".to_owned(),
        ));
    }
    Ok(())
}

fn validate_pool_configuration(
    nodes: &[String],
    keyspace: &str,
    shards: usize,
    per_shard_pool: usize,
) -> Result<()> {
    validate_base_configuration(nodes, keyspace, shards)?;
    if per_shard_pool == 0 {
        return Err(Error::InvalidConfiguration(
            "PerShard pool target must be positive".to_owned(),
        ));
    }
    let _ = checked_data_connection_target(nodes.len(), shards, per_shard_pool)?;
    Ok(())
}

fn validate_base_configuration(nodes: &[String], keyspace: &str, shards: usize) -> Result<()> {
    if !matches!(nodes.len(), 1 | 3)
        || nodes
            .iter()
            .any(|node| node.trim().is_empty() || node.trim() != node)
    {
        return Err(Error::InvalidConfiguration(
            "one or three non-empty Scylla node addresses without surrounding whitespace are required"
                .to_owned(),
        ));
    }
    let unique_nodes = nodes.iter().collect::<BTreeSet<_>>();
    if unique_nodes.len() != nodes.len() {
        return Err(Error::InvalidConfiguration(
            "Scylla node addresses must be distinct".to_owned(),
        ));
    }
    if !keyspace.starts_with("wallet_bench_")
        || keyspace.len() <= "wallet_bench_".len()
        || !keyspace
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(Error::InvalidConfiguration(
            "keyspace must match wallet_bench_[a-z0-9_]+".to_owned(),
        ));
    }
    if shards == 0 {
        return Err(Error::InvalidConfiguration(
            "shards must be positive".to_owned(),
        ));
    }
    Ok(())
}

fn validate_consistency_profile(
    replication_factor: u8,
    read_consistency: ConsistencyLevel,
    write_consistency: ConsistencyLevel,
    node_count: usize,
) -> Result<()> {
    if !(1..=3).contains(&replication_factor) {
        return Err(Error::InvalidConfiguration(
            "replication factor must be between 1 and 3".to_owned(),
        ));
    }
    if usize::from(replication_factor) > node_count {
        return Err(Error::InvalidConfiguration(format!(
            "replication factor {replication_factor} exceeds configured node count {node_count}"
        )));
    }
    for (name, consistency) in [
        ("read consistency", read_consistency),
        ("write consistency", write_consistency),
    ] {
        if consistency.replica_count() > replication_factor {
            return Err(Error::InvalidConfiguration(format!(
                "{name} {} exceeds replication factor {replication_factor}",
                consistency.canonical_name()
            )));
        }
    }
    Ok(())
}

fn checked_data_connection_target(
    node_count: usize,
    shards: usize,
    per_shard_pool: usize,
) -> Result<usize> {
    node_count
        .checked_mul(shards)
        .and_then(|node_shards| node_shards.checked_mul(per_shard_pool))
        .ok_or_else(|| {
            Error::InvalidConfiguration("total data connection target overflows usize".to_owned())
        })
}

impl From<ConsistencyLevel> for Consistency {
    fn from(value: ConsistencyLevel) -> Self {
        match value {
            ConsistencyLevel::One => Self::One,
            ConsistencyLevel::Two => Self::Two,
            ConsistencyLevel::Three => Self::Three,
        }
    }
}

fn create_keyspace_statement(keyspace: &str, escaped_datacenter: &str, rf: u8) -> String {
    format!(
        "CREATE KEYSPACE {keyspace} WITH replication = {{'class': 'NetworkTopologyStrategy', '{escaped_datacenter}': {rf}}} AND durable_writes = true AND tablets = {{'enabled': false}}"
    )
}

async fn wait_for_pool_ready(
    session: &Session,
    expected_node_count: usize,
    expected_connections: usize,
) -> Result<String> {
    let deadline = Instant::now() + POOL_READY_DEADLINE;
    loop {
        let (datacenter, nodes_connected, connections) = {
            let cluster = session.get_cluster_state();
            let nodes = cluster.get_nodes_info();
            if nodes.len() != expected_node_count {
                return Err(Error::InvalidConfiguration(format!(
                    "expected exactly {expected_node_count} Scylla nodes, driver discovered {}",
                    nodes.len()
                )));
            }
            let datacenters = nodes
                .iter()
                .map(|node| node.datacenter.as_deref())
                .collect::<Option<BTreeSet<_>>>()
                .ok_or_else(|| {
                    Error::InvalidConfiguration(
                        "driver did not discover a datacenter for every Scylla node".to_owned(),
                    )
                })?;
            if datacenters.len() != 1 {
                return Err(Error::InvalidConfiguration(
                    "the benchmark requires all nodes in one datacenter".to_owned(),
                ));
            }
            let datacenter = datacenters
                .iter()
                .next()
                .expect("one datacenter was validated")
                .to_string();
            let nodes_connected = nodes.iter().all(|node| node.is_connected());
            let connections = session.get_metrics().get_total_connections();
            (datacenter, nodes_connected, connections)
        };
        if nodes_connected && connections == u64::try_from(expected_connections).unwrap_or(u64::MAX)
        {
            return Ok(datacenter);
        }
        if Instant::now() >= deadline {
            return Err(Error::Storage(format!(
                "timed out waiting for all {expected_node_count} node pools and {expected_connections} data connections (observed {connections})"
            )));
        }
        sleep(Duration::from_millis(25)).await;
    }
}

async fn ensure_keyspace_absent(session: &Session, keyspace: &str) -> Result<()> {
    let mut statement = Statement::new(format!(
        "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = '{keyspace}'"
    ));
    // system_schema uses LocalStrategy, so a cluster-wide consistency level is
    // impossible here. This metadata lookup stays at ONE outside workload timing.
    statement.set_consistency(Consistency::One);
    statement.set_retry_policy(Some(Arc::new(FallthroughRetryPolicy::new())));
    let query = session
        .query_unpaged(statement, &[])
        .await
        .map_err(|error| Error::Storage(format!("check owned keyspace name: {error}")))?;
    let rows = query
        .into_rows_result()
        .map_err(|error| Error::Corruption(format!("decode keyspace metadata: {error}")))?;
    if rows
        .maybe_first_row::<(String,)>()
        .map_err(|error| Error::Corruption(format!("decode keyspace metadata: {error}")))?
        .is_some()
    {
        return Err(Error::InvalidConfiguration(format!(
            "refusing to reuse existing keyspace {keyspace}"
        )));
    }
    Ok(())
}

async fn execute_schema_change(
    session: &Session,
    query: &str,
    action: &str,
) -> Result<QueryResult> {
    // DDL is cluster schema metadata, not a wallet read/write. Scylla's
    // system_schema tables are local-only, so issue DDL at ONE and explicitly
    // await schema agreement before using the resulting metadata.
    let mut statement = Statement::new(query);
    statement.set_consistency(Consistency::One);
    statement.set_retry_policy(Some(Arc::new(FallthroughRetryPolicy::new())));
    let result = session
        .query_unpaged(statement, &[])
        .await
        .map_err(|error| Error::Storage(format!("{action} failed: {error}")))?;
    session
        .await_schema_agreement()
        .await
        .map_err(|error| Error::Storage(format!("schema agreement after {action}: {error}")))?;
    Ok(result)
}

async fn prepare(session: &Session, query: &str) -> Result<PreparedStatement> {
    session
        .prepare(query)
        .await
        .map_err(|error| Error::Storage(format!("prepare statement: {error}")))
}

fn escape_cql_string(value: &str) -> String {
    value.replace('\'', "''")
}

fn cql_key(value: u64, label: &str) -> Result<i64> {
    i64::try_from(value).map_err(|_| {
        Error::NotAccepted(format!(
            "{label} is outside the non-negative CQL bigint range"
        ))
    })
}

fn decode_u64(bytes: &[u8], label: &str) -> Result<u64> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| Error::Corruption(format!("{label} blob must contain exactly 8 bytes")))?;
    Ok(u64::from_be_bytes(bytes))
}

fn decode_account_state_query(query: QueryResult, account_id: u64) -> Result<AccountState> {
    let rows = query.into_rows_result().map_err(|error| {
        Error::Corruption(format!("decode account {account_id} state result: {error}"))
    })?;
    if rows.rows_num() != 1 {
        return Err(Error::Corruption(format!(
            "account {account_id} state query returned {} rows; expected exactly one seed row",
            rows.rows_num()
        )));
    }
    let (kind, tx_id, balance, version, write_clock) = rows
        .maybe_first_row::<(i8, i64, Vec<u8>, i64, i64)>()
        .map_err(|error| {
            Error::Corruption(format!("decode account {account_id} state row: {error}"))
        })?
        .ok_or_else(|| Error::Corruption(format!("account {account_id} has no seed row")))?;
    if kind != ACCOUNT_KIND || tx_id != 0 {
        return Err(Error::Corruption(format!(
            "account {account_id} seed key is ({kind}, {tx_id}); expected (0, 0)"
        )));
    }
    let version = u64::try_from(version)
        .map_err(|_| Error::Corruption(format!("account {account_id} has negative version")))?;
    if write_clock < 0 {
        return Err(Error::Corruption(format!(
            "account {account_id} has negative logical write clock"
        )));
    }
    Ok(AccountState {
        balance: decode_u64(&balance, "account balance")?,
        version,
        write_clock,
    })
}

type LwtFields = BTreeMap<String, Option<CqlValue>>;

fn decode_lwt_fields(query: QueryResult) -> Result<LwtFields> {
    let rows = query
        .into_rows_result()
        .map_err(|error| Error::Corruption(format!("LWT result is not a row response: {error}")))?;
    if rows.rows_num() != 1 {
        return Err(Error::Corruption(format!(
            "LWT returned {} rows; expected exactly one conditional result row",
            rows.rows_num()
        )));
    }
    let specs = rows.column_specs();
    let row = rows
        .maybe_first_row::<Row>()
        .map_err(|error| Error::Corruption(format!("decode LWT result row: {error}")))?
        .ok_or_else(|| Error::Corruption("LWT returned no result row".to_owned()))?;
    if row.columns.len() != specs.len() {
        return Err(Error::Corruption(format!(
            "LWT returned {} values for {} named columns",
            row.columns.len(),
            specs.len()
        )));
    }
    let mut fields = BTreeMap::new();
    for (index, spec) in specs.iter().enumerate() {
        let name = spec.name().to_owned();
        if fields
            .insert(name.clone(), row.columns[index].clone())
            .is_some()
        {
            return Err(Error::Corruption(format!(
                "LWT response contains duplicate column name {name:?}"
            )));
        }
    }
    Ok(fields)
}

fn applied_field(fields: &LwtFields) -> Result<bool> {
    let applied = fields
        .get("[applied]")
        .or_else(|| fields.get("applied"))
        .ok_or_else(|| Error::Corruption("LWT response has no [applied] column".to_owned()))?
        .as_ref()
        .ok_or_else(|| Error::Corruption("LWT [applied] value is null".to_owned()))?;
    match applied {
        CqlValue::Boolean(value) => Ok(*value),
        other => Err(Error::Corruption(format!(
            "LWT [applied] has unexpected CQL value {other:?}"
        ))),
    }
}

fn required_lwt_value<'a>(fields: &'a LwtFields, name: &str) -> Result<&'a CqlValue> {
    fields
        .get(name)
        .ok_or_else(|| Error::Corruption(format!("LWT response has no {name} column")))?
        .as_ref()
        .ok_or_else(|| Error::Corruption(format!("LWT response column {name} is null")))
}

fn lwt_bigint(fields: &LwtFields, name: &str) -> Result<i64> {
    match required_lwt_value(fields, name)? {
        CqlValue::BigInt(value) => Ok(*value),
        other => Err(Error::Corruption(format!(
            "LWT column {name} has unexpected value {other:?}; expected bigint"
        ))),
    }
}

fn lwt_tinyint(fields: &LwtFields, name: &str) -> Result<i8> {
    match required_lwt_value(fields, name)? {
        CqlValue::TinyInt(value) => Ok(*value),
        other => Err(Error::Corruption(format!(
            "LWT column {name} has unexpected value {other:?}; expected tinyint"
        ))),
    }
}

fn lwt_blob(fields: &LwtFields, name: &str) -> Result<Vec<u8>> {
    match required_lwt_value(fields, name)? {
        CqlValue::Blob(value) => Ok(value.clone()),
        other => Err(Error::Corruption(format!(
            "LWT column {name} has unexpected value {other:?}; expected blob"
        ))),
    }
}

fn decode_existing_lwt_fields(
    fields: &LwtFields,
    account_id: u64,
    tx_id: u64,
) -> Result<(AccountState, LedgerRecord)> {
    let returned_account = lwt_bigint(fields, "account_id")?;
    let returned_kind = lwt_tinyint(fields, "kind")?;
    let returned_tx = lwt_bigint(fields, "tx_id")?;
    if returned_account != cql_key(account_id, "account id")?
        || returned_kind != LEDGER_KIND
        || returned_tx != cql_key(tx_id, "transaction id")?
    {
        return Err(Error::Corruption(format!(
            "LWT existing row key ({returned_account}, {returned_kind}, {returned_tx}) does not match ({account_id}, {LEDGER_KIND}, {tx_id})"
        )));
    }

    let returned_operation = lwt_tinyint(fields, "op")?;
    let returned_amount = decode_u64(&lwt_blob(fields, "amount")?, "LWT ledger amount")?;

    let version = u64::try_from(lwt_bigint(fields, "version")?)
        .map_err(|_| Error::Corruption("LWT static version is negative".to_owned()))?;
    let write_clock = lwt_bigint(fields, "write_clock")?;
    if write_clock < 0 {
        return Err(Error::Corruption(
            "LWT static logical clock is negative".to_owned(),
        ));
    }
    let state = AccountState {
        balance: decode_u64(&lwt_blob(fields, "balance")?, "LWT static balance")?,
        version,
        write_clock,
    };
    let record = decode_record(
        account_id,
        returned_tx,
        returned_operation,
        returned_amount.to_be_bytes().to_vec(),
        lwt_tinyint(fields, "outcome")?,
        lwt_blob(fields, "balance_after")?,
        lwt_bigint(fields, "ledger_version")?,
        lwt_bigint(fields, "ledger_write_clock")?,
    )?;
    if record.request.account_id != account_id || record.request.tx_id != tx_id {
        return Err(Error::Corruption(
            "decoded LWT row body has a different account or transaction key".to_owned(),
        ));
    }
    if record.result.version > state.version
        || record.write_clock > state.write_clock
        || (record.result.version == state.version
            && (record.result.balance != state.balance || record.write_clock != state.write_clock))
    {
        return Err(Error::Corruption(
            "LWT existing ledger row is ahead of or inconsistent with its static account state"
                .to_owned(),
        ));
    }
    Ok((state, record))
}

fn decode_record(
    account_id: u64,
    tx_id: i64,
    operation: i8,
    amount: Vec<u8>,
    outcome: i8,
    balance_after: Vec<u8>,
    version: i64,
    write_clock: i64,
) -> Result<LedgerRecord> {
    if write_clock < 0 {
        return Err(Error::Corruption("negative ledger write clock".to_owned()));
    }
    let tx_id = u64::try_from(tx_id)
        .map_err(|_| Error::Corruption("negative transaction id in ledger".to_owned()))?;
    let version = u64::try_from(version)
        .map_err(|_| Error::Corruption("negative ledger version".to_owned()))?;
    Ok(LedgerRecord {
        request: Transaction {
            account_id,
            tx_id,
            operation: decode_operation(operation)?,
            amount: decode_u64(&amount, "ledger amount")?,
        },
        result: TransactionResult {
            outcome: decode_outcome(outcome)?,
            balance: decode_u64(&balance_after, "ledger balance_after")?,
            version,
            replayed: false,
        },
        write_clock,
    })
}

fn verify_ledger_state(
    account_id: u64,
    state: &AccountState,
    records: &[LedgerRecord],
) -> Result<()> {
    if state.write_clock < 0 {
        return Err(Error::Corruption("negative account write clock".to_owned()));
    }
    if u64::try_from(records.len()).ok() != Some(state.version) {
        return Err(Error::Corruption(format!(
            "account {account_id} has version {} but {} ledger records",
            state.version,
            records.len()
        )));
    }
    let mut previous_clock = 0_i64;
    for (index, record) in records.iter().enumerate() {
        let expected_version = u64::try_from(index + 1)
            .map_err(|_| Error::Corruption("ledger row count exceeds u64".to_owned()))?;
        if record.request.account_id != account_id || record.result.version != expected_version {
            return Err(Error::Corruption(format!(
                "account {account_id} ledger versions are missing, duplicated, or out of sequence"
            )));
        }
        if record.write_clock <= previous_clock {
            return Err(Error::Corruption(format!(
                "account {account_id} ledger write clocks are not increasing"
            )));
        }
        previous_clock = record.write_clock;
    }
    match records.last() {
        Some(last)
            if last.result.balance != state.balance || last.write_clock != state.write_clock =>
        {
            return Err(Error::Corruption(format!(
                "account {account_id} current static state does not match its latest ledger row"
            )));
        }
        None if state.version != 0 || state.write_clock != 0 => {
            return Err(Error::Corruption(format!(
                "account {account_id} has no ledger rows but nonzero version or logical clock"
            )));
        }
        _ => {}
    }
    Ok(())
}

fn encode_operation(operation: Operation) -> i8 {
    match operation {
        Operation::Credit => 0,
        Operation::Debit => 1,
    }
}

fn decode_operation(operation: i8) -> Result<Operation> {
    match operation {
        0 => Ok(Operation::Credit),
        1 => Ok(Operation::Debit),
        other => Err(Error::Corruption(format!(
            "unknown ledger operation {other}"
        ))),
    }
}

fn encode_outcome(outcome: Outcome) -> i8 {
    match outcome {
        Outcome::Applied => 0,
        Outcome::InsufficientFunds => 1,
        Outcome::CreditOverflow => 2,
        Outcome::InvalidAmount => 3,
    }
}

fn decode_outcome(outcome: i8) -> Result<Outcome> {
    match outcome {
        0 => Ok(Outcome::Applied),
        1 => Ok(Outcome::InsufficientFunds),
        2 => Ok(Outcome::CreditOverflow),
        3 => Ok(Outcome::InvalidAmount),
        other => Err(Error::Corruption(format!("unknown ledger outcome {other}"))),
    }
}

#[cfg(test)]
mod profile_tests {
    use super::{
        Consistency, ConsistencyLevel, checked_data_connection_target, create_keyspace_statement,
        validate_base_configuration, validate_configuration, validate_consistency_profile,
        validate_pool_configuration,
    };
    use crate::Error;

    fn nodes(count: usize) -> Vec<String> {
        (0..count)
            .map(|index| format!("127.0.0.{}:9042", index + 1))
            .collect()
    }

    #[test]
    fn consistency_profiles_map_to_driver_levels_and_canonical_names() {
        let profiles = [
            (ConsistencyLevel::One, Consistency::One, "ONE", 1),
            (ConsistencyLevel::Two, Consistency::Two, "TWO", 2),
            (ConsistencyLevel::Three, Consistency::Three, "THREE", 3),
        ];
        for (profile, driver, canonical, replicas) in profiles {
            assert_eq!(Consistency::from(profile), driver);
            assert_eq!(profile.canonical_name(), canonical);
            assert_eq!(profile.replica_count(), replicas);
            assert_eq!(
                ConsistencyLevel::from_replica_count(replicas),
                Some(profile)
            );
        }
    }

    #[test]
    fn replication_factor_is_applied_to_keyspace_ddl_and_bounds_read_write_cl() {
        let ddl = create_keyspace_statement("wallet_bench_test", "dc1", 1);
        assert!(ddl.contains("'dc1': 1"));
        assert!(ddl.contains("durable_writes = true"));
        assert!(ddl.contains("'enabled': false"));

        assert!(
            validate_consistency_profile(1, ConsistencyLevel::One, ConsistencyLevel::One, 1)
                .is_ok()
        );
        assert!(
            validate_consistency_profile(1, ConsistencyLevel::Two, ConsistencyLevel::One, 3)
                .is_err()
        );
        assert!(
            validate_consistency_profile(0, ConsistencyLevel::One, ConsistencyLevel::One, 3)
                .is_err()
        );
        assert!(
            validate_consistency_profile(3, ConsistencyLevel::Three, ConsistencyLevel::Three, 1)
                .is_err()
        );
    }

    #[test]
    fn explicit_single_node_profiles_accept_s1_and_s2_pool_targets() {
        let single = nodes(1);
        assert!(validate_pool_configuration(&single, "wallet_bench_test", 1, 3).is_ok());
        assert!(
            validate_consistency_profile(
                1,
                ConsistencyLevel::One,
                ConsistencyLevel::One,
                single.len()
            )
            .is_ok()
        );
        assert_eq!(checked_data_connection_target(1, 1, 3).unwrap(), 3);
        assert!(validate_pool_configuration(&single, "wallet_bench_test", 2, 3).is_ok());
        assert_eq!(checked_data_connection_target(1, 2, 3).unwrap(), 6);
    }

    #[test]
    fn legacy_three_node_configuration_remains_valid_and_legacy_single_node_rejects_rf3() {
        let three = nodes(3);
        assert!(validate_configuration(&three, "wallet_bench_test", 1, 12).is_ok());

        let single = nodes(1);
        let error = validate_configuration(&single, "wallet_bench_test", 1, 128)
            .expect_err("legacy wrapper must retain RF=3 defaults");
        assert!(matches!(error, Error::InvalidConfiguration(_)));
        assert!(validate_pool_configuration(&single, "wallet_bench_test", 1, 3).is_ok());
    }

    #[test]
    fn store_boundaries_reject_two_duplicate_invalid_and_overflow_configurations() {
        let two = nodes(2);
        assert!(validate_base_configuration(&two, "wallet_bench_test", 1).is_err());

        let duplicate = vec!["127.0.0.1:9042".to_owned(); 3];
        assert!(validate_base_configuration(&duplicate, "wallet_bench_test", 1).is_err());

        let single = nodes(1);
        assert!(checked_data_connection_target(1, usize::MAX, 2).is_err());
        assert!(validate_pool_configuration(&single, "wallet_bench_test", usize::MAX, 2).is_err());
    }
}

#[cfg(test)]
mod lwt_response_tests {
    use super::{
        AccountState, CqlValue, LwtFields, Operation, Outcome, applied_field,
        decode_existing_lwt_fields,
    };
    use std::collections::BTreeMap;

    fn existing_lwt_row() -> LwtFields {
        BTreeMap::from([
            ("[applied]".to_owned(), Some(CqlValue::Boolean(false))),
            ("account_id".to_owned(), Some(CqlValue::BigInt(7))),
            ("kind".to_owned(), Some(CqlValue::TinyInt(1))),
            ("tx_id".to_owned(), Some(CqlValue::BigInt(3))),
            (
                "balance".to_owned(),
                Some(CqlValue::Blob(106_u64.to_be_bytes().to_vec())),
            ),
            ("version".to_owned(), Some(CqlValue::BigInt(2))),
            ("write_clock".to_owned(), Some(CqlValue::BigInt(2))),
            ("op".to_owned(), Some(CqlValue::TinyInt(0))),
            (
                "amount".to_owned(),
                Some(CqlValue::Blob(5_u64.to_be_bytes().to_vec())),
            ),
            ("outcome".to_owned(), Some(CqlValue::TinyInt(0))),
            (
                "balance_after".to_owned(),
                Some(CqlValue::Blob(106_u64.to_be_bytes().to_vec())),
            ),
            ("ledger_version".to_owned(), Some(CqlValue::BigInt(2))),
            ("ledger_write_clock".to_owned(), Some(CqlValue::BigInt(2))),
        ])
    }

    #[test]
    fn applied_lwt_response_allows_nullable_unselected_result_fields() {
        let fields = BTreeMap::from([
            ("[applied]".to_owned(), Some(CqlValue::Boolean(true))),
            ("balance".to_owned(), None),
            ("version".to_owned(), None),
            ("write_clock".to_owned(), None),
            ("op".to_owned(), None),
        ]);
        assert!(applied_field(&fields).unwrap());
    }

    #[test]
    fn applied_false_decodes_named_existing_row_and_original_result() {
        let fields = existing_lwt_row();
        assert!(!applied_field(&fields).unwrap());
        let (state, record) = decode_existing_lwt_fields(&fields, 7, 3).unwrap();
        assert_eq!(
            state,
            AccountState {
                balance: 106,
                version: 2,
                write_clock: 2
            }
        );
        assert_eq!(record.request.account_id, 7);
        assert_eq!(record.request.tx_id, 3);
        assert_eq!(record.request.operation, Operation::Credit);
        assert_eq!(record.request.amount, 5);
        assert_eq!(record.result.outcome, Outcome::Applied);
        assert_eq!((record.result.balance, record.result.version), (106, 2));
    }

    #[test]
    fn applied_false_rejects_null_required_and_incorrectly_typed_columns() {
        let mut null_version = existing_lwt_row();
        null_version.insert("version".to_owned(), None);
        assert!(decode_existing_lwt_fields(&null_version, 7, 3).is_err());

        let mut wrong_version_type = existing_lwt_row();
        wrong_version_type.insert("version".to_owned(), Some(CqlValue::TinyInt(2)));
        assert!(decode_existing_lwt_fields(&wrong_version_type, 7, 3).is_err());

        let mut wrong_key = existing_lwt_row();
        wrong_key.insert("tx_id".to_owned(), Some(CqlValue::BigInt(4)));
        assert!(decode_existing_lwt_fields(&wrong_key, 7, 3).is_err());
    }
}
