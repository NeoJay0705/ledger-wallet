//! Real database clients. This module deliberately contains no workload orchestration.
use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{anyhow, bail, Context, Result};
use futures::StreamExt;
use scylla::{
    batch::{Batch, BatchType},
    transport::{execution_profile::ExecutionProfile, session::PoolSize},
    Session, SessionBuilder,
};
use tokio::time::timeout;
use tokio_postgres::NoTls;
use uuid::Uuid;

use crate::{
    config::{Backend, ValidatedConfig},
    metrics::{account_uuid, reserved_init_uuid},
};

const PG_SQL: &str = "WITH inserted AS (INSERT INTO ledger(account_id,transaction_id,amount) VALUES ($1,$2,$3) ON CONFLICT (account_id,transaction_id) DO NOTHING RETURNING account_id,amount), updated AS (UPDATE accounts a SET balance=a.balance+i.amount FROM inserted i WHERE a.account_id=i.account_id RETURNING a.account_id,a.balance) SELECT account_id,balance FROM updated";
const SCYLLA_READ: &str = "SELECT balance FROM ledger WHERE account_id=? LIMIT 1";
const SCYLLA_UPDATE: &str = "UPDATE ledger SET balance=? WHERE account_id=? IF balance=?";
const SCYLLA_INSERT: &str =
    "INSERT INTO ledger(account_id,transaction_id,amount) VALUES (?,?,?) IF NOT EXISTS";

#[derive(Clone, Debug, Default)]
pub struct OperationTiming {
    /// Aggregate backend time includes checkout or read/write and all stale-condition retries.
    pub backend: Duration,
    pub pg_checkout: Duration,
    pub scylla_read: Duration,
    pub scylla_write: Duration,
    pub condition_retries: u32,
}
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct AccountReport {
    pub account_id: Uuid,
    pub balance: i64,
    pub ledger_count: i64,
    pub amount_sum: i64,
}
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct VerificationReport {
    pub accounts: Vec<AccountReport>,
    pub valid: bool,
    pub summary: BTreeMap<String, i128>,
}

#[derive(Clone)]
pub struct BackendClient(Arc<ClientInner>);
enum ClientInner {
    Pg {
        pool: deadpool_postgres::Pool,
        config: ValidatedConfig,
    },
    Scylla {
        session: Session,
        read: scylla::statement::prepared_statement::PreparedStatement,
        batch: Box<Batch>,
        config: Box<ValidatedConfig>,
        keyspace: String,
        shards: usize,
    },
}
impl BackendClient {
    /// Connect, create only the project-owned benchmark schema when absent, initialize N accounts,
    /// prepare timed statements and prime the configured pool where the driver exposes readiness.
    pub async fn connect_and_initialize(config: ValidatedConfig) -> Result<Self> {
        match config.args.backend {
            Backend::Pg => Self::connect_pg(config).await,
            Backend::Scylla => Self::connect_scylla(config).await,
        }
    }
    async fn connect_pg(config: ValidatedConfig) -> Result<Self> {
        let pg = config.args.pg_url.parse::<tokio_postgres::Config>()?;
        let mgr = deadpool_postgres::Manager::new(pg, NoTls);
        let pool = deadpool_postgres::Pool::builder(mgr)
            .max_size(config.args.pg_data_pool_size)
            .build()?;
        let deadline = config.connect_timeout;
        let admin = timeout(deadline, pool.get())
            .await
            .context("PostgreSQL connect timeout")??;
        admin.batch_execute("CREATE TABLE IF NOT EXISTS accounts(account_id uuid PRIMARY KEY,balance bigint NOT NULL DEFAULT 0); CREATE TABLE IF NOT EXISTS ledger(account_id uuid NOT NULL REFERENCES accounts(account_id),transaction_id uuid NOT NULL,amount bigint NOT NULL CHECK(amount>0),PRIMARY KEY(account_id,transaction_id));").await?;
        let populated: bool = admin
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM accounts) OR EXISTS(SELECT 1 FROM ledger)",
                &[],
            )
            .await?
            .get(0);
        if populated {
            bail!("refusing to initialize populated PostgreSQL benchmark schema; use an exclusively test database");
        }
        for id in 0..config.args.users {
            admin
                .execute(
                    "INSERT INTO accounts(account_id,balance) VALUES($1,0)",
                    &[&account_uuid(id)],
                )
                .await?;
        }
        drop(admin);
        // Build every pool connection and prepare on each connection before timed work.
        let mut clients = Vec::with_capacity(config.args.pg_data_pool_size);
        for _ in 0..config.args.pg_data_pool_size {
            let c = timeout(config.connect_timeout, pool.get())
                .await
                .context("PostgreSQL pool readiness timeout")??;
            c.prepare_cached(PG_SQL)
                .await
                .context("prepare PostgreSQL business CTE on pool connection")?;
            clients.push(c);
        }
        drop(clients.pop());
        // Remaining handles would pin all pool slots; discard them after preparation.
        drop(clients);
        Ok(Self(Arc::new(ClientInner::Pg { pool, config })))
    }
    async fn connect_scylla(config: ValidatedConfig) -> Result<Self> {
        let keyspace = "ledger_bench".to_string();
        let profile = ExecutionProfile::builder()
            .speculative_execution_policy(None)
            .retry_policy(Arc::new(
                scylla::transport::retry_policy::FallthroughRetryPolicy::new(),
            ))
            .build();
        let session = SessionBuilder::new()
            .known_node(config.args.scylla_url.clone())
            .pool_size(PoolSize::PerShard(
                NonZeroUsize::new(config.scylla_connections_per_shard).unwrap(),
            ))
            .default_execution_profile_handle(profile.into_handle())
            .build()
            .await
            .context("connect Scylla")?;
        timeout(config.connect_timeout, async {
            session.query_unpaged("CREATE KEYSPACE IF NOT EXISTS ledger_bench WITH replication = {'class':'SimpleStrategy','replication_factor':1} AND durable_writes=true", &[]).await?;
            session.use_keyspace(&keyspace, false).await?;
            session.query_unpaged("CREATE TABLE IF NOT EXISTS ledger(account_id uuid,transaction_id uuid,balance bigint static,amount bigint,PRIMARY KEY(account_id,transaction_id))", &[]).await?;
            Ok::<_, scylla::transport::errors::QueryError>(())
        }).await.context("Scylla schema initialization timeout")??;
        // Never clear an existing schema: do not risk deleting unrelated/user data.
        let existing = session
            .query_unpaged("SELECT DISTINCT account_id FROM ledger LIMIT 1", &[])
            .await?;
        if existing.into_rows_result()?.rows_num() > 0 {
            bail!("refusing to initialize populated Scylla benchmark keyspace; use an exclusively test keyspace");
        }
        for id in 0..config.args.users {
            session
                .query_unpaged(
                    "INSERT INTO ledger(account_id,transaction_id,balance,amount) VALUES(?,?,0,0)",
                    (account_uuid(id), reserved_init_uuid()),
                )
                .await?;
        }
        let mut read = session.prepare(SCYLLA_READ).await?;
        read.set_consistency(scylla::frame::types::Consistency::LocalQuorum);
        read.set_retry_policy(None);
        let mut update = session.prepare(SCYLLA_UPDATE).await?;
        update.set_consistency(scylla::frame::types::Consistency::LocalQuorum);
        update.set_serial_consistency(Some(scylla::statement::SerialConsistency::LocalSerial));
        update.set_retry_policy(None);
        let mut insert = session.prepare(SCYLLA_INSERT).await?;
        insert.set_consistency(scylla::frame::types::Consistency::LocalQuorum);
        insert.set_serial_consistency(Some(scylla::statement::SerialConsistency::LocalSerial));
        insert.set_retry_policy(None);
        let mut batch = Batch::new(BatchType::Logged);
        batch.append_statement(update);
        batch.append_statement(insert);
        batch.set_consistency(scylla::frame::types::Consistency::LocalQuorum);
        batch.set_serial_consistency(Some(scylla::statement::SerialConsistency::LocalSerial));
        batch.set_retry_policy(None);
        let shard_counts: Vec<usize> = session
            .get_cluster_data()
            .get_nodes_info()
            .iter()
            .filter_map(|node| node.sharder().map(|s| usize::from(s.nr_shards.get())))
            .collect();
        if shard_counts.is_empty()
            || shard_counts
                .iter()
                .any(|&n| n != config.args.expected_shards)
        {
            bail!(
                "Scylla driver shard count mismatch: expected {} on every node, observed {:?}",
                config.args.expected_shards,
                shard_counts
            );
        }
        let shards = *shard_counts
            .iter()
            .max()
            .expect("non-empty shard counts checked");
        Ok(Self(Arc::new(ClientInner::Scylla {
            session,
            read,
            batch: Box::new(batch),
            config: Box::new(config),
            keyspace,
            shards,
        })))
    }
    /// Reset only the benchmark tables after the initial empty-schema ownership check.
    pub async fn reset_benchmark_data(&self, users: u32) -> Result<()> {
        match self.0.as_ref() {
            ClientInner::Pg { pool, config } => timeout(config.setup_timeout, async {
                let c = pool.get().await?;
                c.batch_execute("TRUNCATE TABLE ledger; UPDATE accounts SET balance=0;").await?;
                Ok::<_, anyhow::Error>(())
            }).await.context("PostgreSQL benchmark reset timeout")??,
            ClientInner::Scylla { session, config, .. } => timeout(config.setup_timeout, async {
                session.query_unpaged("TRUNCATE ledger", &[]).await?;
                for id in 0..users {
                    session.query_unpaged("INSERT INTO ledger(account_id,transaction_id,balance,amount) VALUES(?,?,0,0)", (account_uuid(id), reserved_init_uuid())).await?;
                }
                Ok::<_, anyhow::Error>(())
            }).await.context("Scylla benchmark reset timeout")??,
        }
        Ok(())
    }
    /// Executes exactly one logical +1 transaction; outer timeout covers the complete operation.
    pub async fn transact(&self, user_id: u32, fresh_uuid: Uuid) -> Result<OperationTiming> {
        match self.0.as_ref() {
            ClientInner::Pg { pool, config } => timeout(config.request_timeout, async {
                let start = Instant::now();
                let checkout_start = Instant::now();
                let client = pool.get().await?;
                let checkout = checkout_start.elapsed();
                let prepared = client.prepare_cached(PG_SQL).await?;
                let rows = client
                    .query(&prepared, &[&account_uuid(user_id), &fresh_uuid, &1i64])
                    .await?;
                if rows.len() != 1 {
                    bail!("PostgreSQL CTE did not report exactly one applied account update");
                }
                let _balance: i64 = rows[0].get(1);
                Ok(OperationTiming {
                    backend: start.elapsed(),
                    pg_checkout: checkout,
                    ..Default::default()
                })
            })
            .await
            .context("PostgreSQL business transaction timed out")?,
            ClientInner::Scylla {
                session,
                read,
                batch,
                config,
                ..
            } => timeout(config.request_timeout, async {
                let start = Instant::now();
                let mut timing = OperationTiming::default();
                for attempt in 0..=3 {
                    let t = Instant::now();
                    let result = session
                        .execute_unpaged(read, (account_uuid(user_id),))
                        .await?;
                    timing.scylla_read += t.elapsed();
                    let rows_result = result.into_rows_result()?;
                    let mut rows = rows_result.rows::<(Option<i64>,)>()?;
                    let old = rows
                        .next()
                        .ok_or_else(|| anyhow!("Scylla account missing or unmaterialized"))??
                        .0
                        .ok_or_else(|| anyhow!("null static balance"))?;
                    let t = Instant::now();
                    let result = session
                        .batch(
                            batch,
                            (
                                (old + 1, account_uuid(user_id), old),
                                (account_uuid(user_id), fresh_uuid, 1i64),
                            ),
                        )
                        .await?;
                    timing.scylla_write += t.elapsed();
                    let rows_result = result.into_rows_result()?;
                    // Conditional batches expose the complete five-column LWT result. Decode
                    // the nullable key/value columns too, including the synthetic second row.
                    let rows = rows_result
                        .rows::<(bool, Option<Uuid>, Option<Uuid>, Option<i64>, Option<i64>)>()?
                        .collect::<std::result::Result<Vec<_>, _>>()?;
                    let applied = conditional_batch_applied(&rows);
                    if applied {
                        timing.backend = start.elapsed();
                        return Ok(timing);
                    }
                    if attempt == 3 {
                        bail!("Scylla condition failed after bounded 3 retries");
                    }
                    timing.condition_retries += 1;
                }
                unreachable!()
            })
            .await
            .context(
                "Scylla business transaction timed out (run invalid; outcome may be ambiguous)",
            )?,
        }
    }
    pub async fn verify(&self, n: u32, k: u64) -> Result<VerificationReport> {
        match self.0.as_ref() {
            ClientInner::Pg { pool, config, .. } => timeout(config.setup_timeout, async {
                let c=pool.get().await?;
                let rows=c.query("SELECT a.account_id,a.balance,count(l.transaction_id)::bigint,COALESCE(sum(l.amount),0)::bigint FROM accounts a LEFT JOIN ledger l USING(account_id) GROUP BY a.account_id,a.balance ORDER BY a.account_id", &[]).await?;
                let mut report=VerificationReport::default();
                for r in rows { report.accounts.push(AccountReport {account_id:r.get(0),balance:r.get(1),ledger_count:r.get(2),amount_sum:r.get(3)}); }
                verify_report(report,n,k)
            }).await.context("PostgreSQL verification timeout")?,
            ClientInner::Scylla { session, config, .. } => timeout(config.setup_timeout, async {
                let mut distinct=session.query_iter("SELECT DISTINCT account_id FROM ledger", &[]).await?.rows_stream::<(Uuid,)>()?;
                let mut report=VerificationReport::default();
                while let Some(row) = distinct.next().await { let id=row?.0;
                    let q=session.query_unpaged("SELECT balance,count(transaction_id),sum(amount) FROM ledger WHERE account_id=? AND transaction_id>?", (id,reserved_init_uuid())).await?;
                    let row=q.into_rows_result()?.rows::<(Option<i64>,Option<i64>,Option<i64>)>()?.next().ok_or_else(||anyhow!("missing Scylla account aggregate"))??;
                    let balance = if let Some(balance) = row.0 { balance } else {
                        let q = session.query_unpaged("SELECT balance FROM ledger WHERE account_id=? LIMIT 1", (id,)).await?;
                        q.into_rows_result()?.rows::<(Option<i64>,)>()?.next().ok_or_else(|| anyhow!("missing Scylla account balance"))??.0.ok_or_else(|| anyhow!("null Scylla account balance"))?
                    };
                    let count=row.1.unwrap_or(0);
                    let sum=row.2.unwrap_or(0);
                    report.accounts.push(AccountReport{account_id:id,balance,ledger_count:count,amount_sum:sum});
                }
                verify_report(report,n,k)
            }).await.context("Scylla verification timeout")?,
        }
    }
    pub fn diagnostics(&self) -> BTreeMap<String, String> {
        let mut d = BTreeMap::new();
        match self.0.as_ref() {
            ClientInner::Pg { pool, config, .. } => {
                d.insert("backend".into(), "postgresql".into());
                d.insert(
                    "data_pool_max".into(),
                    config.args.pg_data_pool_size.to_string(),
                );
                d.insert("pool_status".into(), format!("{}", pool.status().size));
                d.insert("prepared_business_cte".into(), "true".into());
            }
            ClientInner::Scylla {
                session,
                config,
                keyspace,
                shards,
                ..
            } => {
                d.insert("backend".into(), "scylla".into());
                d.insert("keyspace".into(), keyspace.clone());
                d.insert("driver_shards".into(), shards.to_string());
                d.insert(
                    "expected_shards".into(),
                    config.args.expected_shards.to_string(),
                );
                d.insert(
                    "connections_per_shard_configured".into(),
                    config.scylla_connections_per_shard.to_string(),
                );
                d.insert(
                    "session_nodes".into(),
                    session
                        .get_cluster_data()
                        .get_nodes_info()
                        .len()
                        .to_string(),
                );
                d.insert(
                    "pool_policy".into(),
                    "shard-aware; actual socket count not exposed by scylla 0.15 API".into(),
                );
                d.insert("speculation".into(), "disabled".into());
                d.insert(
                    "retry_policy".into(),
                    "disabled; condition retries explicit".into(),
                );
            }
        }
        d
    }
}
type ConditionalBatchRow = (bool, Option<Uuid>, Option<Uuid>, Option<i64>, Option<i64>);

fn conditional_batch_applied(rows: &[ConditionalBatchRow]) -> bool {
    !rows.is_empty() && rows.iter().all(|row| row.0)
}

fn verify_report(mut r: VerificationReport, n: u32, k: u64) -> Result<VerificationReport> {
    r.accounts.sort_by_key(|a| a.account_id);
    let expected: Vec<_> = (0..n).map(account_uuid).collect();
    r.valid = r.accounts.len() == n as usize
        && r.accounts.iter().zip(expected).all(|(a, id)| {
            a.account_id == id
                && a.balance == k as i64
                && a.ledger_count == k as i64
                && a.amount_sum == k as i64
        });
    r.summary
        .insert("accounts".into(), r.accounts.len() as i128);
    r.summary.insert("expected_accounts".into(), n as i128);
    r.summary.insert(
        "ledger_rows".into(),
        r.accounts.iter().map(|a| i128::from(a.ledger_count)).sum(),
    );
    r.summary
        .insert("expected_rows".into(), i128::from(n) * i128::from(k));
    r.summary.insert(
        "amount_sum".into(),
        r.accounts.iter().map(|a| i128::from(a.amount_sum)).sum(),
    );
    r.summary
        .insert("expected_sum".into(), i128::from(n) * i128::from(k));
    Ok(r)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn account(id: u32, balance: i64, count: i64, sum: i64) -> AccountReport {
        AccountReport {
            account_id: account_uuid(id),
            balance,
            ledger_count: count,
            amount_sum: sum,
        }
    }

    #[test]
    fn verification_report_accepts_exact_expected_accounts() {
        let report = verify_report(
            VerificationReport {
                accounts: vec![account(1, 3, 3, 3), account(0, 3, 3, 3)],
                ..Default::default()
            },
            2,
            3,
        )
        .unwrap();
        assert!(report.valid);
        assert_eq!(report.accounts[0].account_id, account_uuid(0));
        assert_eq!(report.summary["ledger_rows"], 6);
        assert_eq!(report.summary["amount_sum"], 6);
    }

    #[test]
    fn verification_report_rejects_missing_extra_and_wrong_balance() {
        let missing = verify_report(
            VerificationReport {
                accounts: vec![account(0, 2, 2, 2)],
                ..Default::default()
            },
            2,
            2,
        )
        .unwrap();
        assert!(!missing.valid);

        let extra = verify_report(
            VerificationReport {
                accounts: vec![
                    account(0, 2, 2, 2),
                    account(1, 2, 2, 2),
                    account(2, 2, 2, 2),
                ],
                ..Default::default()
            },
            2,
            2,
        )
        .unwrap();
        assert!(!extra.valid);

        let wrong_balance = verify_report(
            VerificationReport {
                accounts: vec![account(0, 9, 2, 2)],
                ..Default::default()
            },
            1,
            2,
        )
        .unwrap();
        assert!(!wrong_balance.valid);
    }

    #[test]
    fn applied_flags_cover_all_gate1_like_batch_rows() {
        let applied_rows = [
            (true, Some(account_uuid(0)), None, Some(0), None),
            (true, None, None, None, None),
        ];
        assert!(conditional_batch_applied(&applied_rows));

        let failed_rows = [
            (true, Some(account_uuid(0)), None, Some(7), None),
            (false, None, None, None, None),
        ];
        assert!(!conditional_batch_applied(&failed_rows));
        assert!(!conditional_batch_applied(&[]));
    }
}
