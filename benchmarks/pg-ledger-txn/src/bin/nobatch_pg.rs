use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use serde::Serialize;
use std::{
    fs,
    io::Write,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{task::JoinSet, time::timeout};
use tokio_postgres::{Client, NoTls, Statement};
use uuid::Uuid;

#[path = "../metrics.rs"]
mod metrics;
use metrics::{account_uuid, LatencyHistogram};

const SINGLE_SQL: &str = "WITH ins AS (INSERT INTO ledger(account_id,transaction_id,amount) VALUES ($1,$2,$3) ON CONFLICT (account_id,transaction_id) DO NOTHING RETURNING account_id,amount) UPDATE accounts SET balance=accounts.balance+ins.amount FROM ins WHERE accounts.account_id=ins.account_id";

#[derive(Parser, Debug)]
#[command(name = "nobatch_pg")]
struct Args {
    #[arg(
        long,
        default_value = "host=127.0.0.1 user=postgres password=postgres dbname=ledger"
    )]
    pg_url: String,
    #[arg(long, default_value_t = 100_000)]
    users: u32,
    #[arg(long, default_value_t = 10)]
    requests_per_user: u32,
    #[arg(long, default_value_t = 256)]
    connections: usize,
    #[arg(long, default_value_t = 30_000)]
    request_timeout_ms: u64,
    #[arg(long, default_value_t = 1000)]
    sample_interval_ms: u64,
    #[arg(long)]
    output_dir: PathBuf,
    #[arg(long)]
    ready_file: PathBuf,
    #[arg(long)]
    go_file: PathBuf,
    #[arg(long, default_value_t = 600_000)]
    handshake_timeout_ms: u64,
}

#[derive(Clone, Debug)]
struct Request {
    account: Uuid,
    tx: Uuid,
}
fn build_requests(users: u32, per_user: u32) -> Vec<Request> {
    let mut result = Vec::with_capacity(users as usize * per_user as usize);
    for _round in 0..per_user {
        for user in 0..users {
            result.push(Request {
                account: account_uuid(user),
                tx: Uuid::new_v4(),
            });
        }
    }
    result
}

#[derive(Default)]
struct Counters {
    completed: AtomicU64,
    statements: AtomicU64,
    inflight: AtomicU64,
    busy: AtomicU64,
    remaining: AtomicU64,
}
#[derive(Serialize)]
struct Verification {
    accounts: u64,
    bad_accounts: u64,
    sum_balance: i64,
    ledger_rows: i64,
    valid: bool,
}
#[derive(Serialize)]
struct ResultJson {
    measurement_start_unix_ns: u128,
    measurement_end_unix_ns: u128,
    duration_s: f64,
    requests: u64,
    statements: u64,
    rps_request: f64,
    rps_io: f64,
    latency_us: LatencyJson,
    data_connections: usize,
    monitor_connections: usize,
    effective: Effective,
    verification: Verification,
    mode: &'static str,
    wal_stats: WalStats,
}
#[derive(Serialize)]
struct WalStats {
    before: serde_json::Value,
    after: serde_json::Value,
    lsn_before: String,
    lsn_after: String,
    lsn_delta_bytes: i64,
    settled: bool,
}
#[derive(Serialize)]
struct LatencyJson {
    p50: Option<u64>,
    p95: Option<u64>,
    p99: Option<u64>,
    max: Option<u64>,
}
#[derive(Serialize)]
struct Effective {
    pg_url: String,
    users: u32,
    requests_per_user: u32,
    connections: usize,
    request_timeout_ms: u64,
    sample_interval_ms: u64,
    postgres_version: String,
}

fn unix_ns() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}
fn utc_now() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let s = now.as_secs() as i64;
    let ms = now.subsec_millis();
    let days = s.div_euclid(86400);
    let sec = s.rem_euclid(86400);
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let mut y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    if m <= 2 {
        y += 1;
    }
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{ms:03}Z",
        sec / 3600,
        (sec % 3600) / 60,
        sec % 60
    )
}

async fn connect(url: &str) -> Result<Client> {
    let (client, connection) = tokio_postgres::connect(url, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("postgres connection driver: {e}");
        }
    });
    Ok(client)
}
async fn wal_snapshot(client: &Client) -> Result<serde_json::Value> {
    let text: String = client.query_one("SELECT json_build_object('pg_stat_wal',(SELECT row_to_json(w) FROM pg_stat_wal w),'pg_stat_checkpointer',(SELECT row_to_json(c) FROM pg_stat_checkpointer c),'pg_stat_io',coalesce((SELECT json_agg(row_to_json(i)) FROM pg_stat_io i WHERE object='wal'),'[]'::json),'lsn',pg_current_wal_lsn()::text)::text", &[]).await?.get(0);
    Ok(serde_json::from_str(&text)?)
}

async fn settle_data_backends(client: &Client) -> Result<bool> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let count: i64 = client.query_one("SELECT count(*) FROM pg_stat_activity WHERE backend_type='client backend' AND pid<>pg_backend_pid()", &[]).await?.get(0);
        if count == 0 {
            return Ok(true);
        }
        if Instant::now() >= deadline {
            return Ok(false);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wal_delta(client: &Client, after: &str, before: &str) -> Result<i64> {
    Ok(client
        .query_one(
            "SELECT pg_wal_lsn_diff($1::text::pg_lsn,$2::text::pg_lsn)::bigint",
            &[&after, &before],
        )
        .await?
        .get(0))
}

async fn sample_row(
    client: &Client,
    file: &mut fs::File,
    counters: &Counters,
    prior: &mut Instant,
    prev_req: &mut u64,
    prev_stmt: &mut u64,
    second: u64,
) -> Result<()> {
    let now = Instant::now();
    let elapsed = now.duration_since(*prior).as_secs_f64().max(1e-9);
    *prior = now;
    let req = counters.completed.load(Ordering::Relaxed);
    let stmt = counters.statements.load(Ordering::Relaxed);
    let active: i64=client.query_one("SELECT count(*) FROM pg_stat_activity WHERE state='active' AND backend_type='client backend' AND datname='ledger' AND pid<>pg_backend_pid()", &[]).await?.get(0);
    let dr = req - *prev_req;
    let ds = stmt - *prev_stmt;
    *prev_req = req;
    *prev_stmt = stmt;
    writeln!(
        file,
        "{second},{},{dr},{ds},{:.3},{:.3},{},{},{},{}",
        utc_now(),
        dr as f64 / elapsed,
        ds as f64 / elapsed,
        counters.inflight.load(Ordering::Relaxed),
        counters.busy.load(Ordering::Relaxed),
        counters.remaining.load(Ordering::Relaxed),
        active.max(0)
    )?;
    Ok(())
}

async fn run(args: Args) -> Result<()> {
    if args.users == 0
        || args.requests_per_user == 0
        || args.connections == 0
        || args.sample_interval_ms == 0
    {
        bail!("users, requests-per-user, connections, and sample interval must be positive");
    }
    let total = u64::from(args.users) * u64::from(args.requests_per_user);
    fs::create_dir_all(&args.output_dir)?;
    let monitor = connect(&args.pg_url).await.context("connect monitor")?;
    monitor.batch_execute("CREATE TABLE IF NOT EXISTS accounts(account_id uuid PRIMARY KEY,balance bigint NOT NULL DEFAULT 0); CREATE TABLE IF NOT EXISTS ledger(account_id uuid NOT NULL REFERENCES accounts(account_id),transaction_id uuid NOT NULL,amount bigint NOT NULL CHECK(amount>0),PRIMARY KEY(account_id,transaction_id));").await.context("create benchmark schema")?;
    let ids: Vec<Uuid> = (0..args.users).map(account_uuid).collect();
    monitor.execute("INSERT INTO accounts(account_id,balance) SELECT id,0 FROM unnest($1::uuid[]) AS u(id) ON CONFLICT(account_id) DO NOTHING", &[&ids]).await.context("bulk account insert")?;
    monitor
        .query_one("SELECT pg_stat_force_next_flush()", &[])
        .await?;
    let version: String = monitor.query_one("SHOW server_version", &[]).await?.get(0);
    let requests = Arc::new(build_requests(args.users, args.requests_per_user));
    let next = Arc::new(AtomicUsize::new(0));
    let mut data = Vec::with_capacity(args.connections);
    for _ in 0..args.connections {
        let c = connect(&args.pg_url).await?;
        let p = c.prepare(SINGLE_SQL).await?;
        data.push((c, p));
    }
    let counters = Arc::new(Counters::default());
    counters.remaining.store(total, Ordering::Relaxed);
    fs::write(&args.ready_file, format!("ready {}\n", utc_now())).context("write ready file")?;
    let wait_start = Instant::now();
    while !args.go_file.exists() {
        if wait_start.elapsed() > Duration::from_millis(args.handshake_timeout_ms) {
            bail!("GO handshake timeout");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let before = wal_snapshot(&monitor).await?;
    let lsn_before: String = before["lsn"]
        .as_str()
        .ok_or_else(|| anyhow!("missing lsn in before WAL snapshot"))?
        .to_owned();
    let measurement_start = unix_ns();
    let start = Instant::now();
    let mut csv = fs::File::create(args.output_dir.join("per_second.csv"))?;
    writeln!(csv,"second,wall_utc,requests_delta,statements_delta,rps_request,rps_io,inflight_requests,busy_conns,queue_remaining,server_active")?;
    let interval = Duration::from_millis(args.sample_interval_ms);
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ticker.tick().await;
    let mut sample_at = Instant::now();
    let mut prev_req = 0;
    let mut prev_stmt = 0;
    let mut second = 0u64;
    let mut set = JoinSet::new();
    for (client, stmt) in data {
        let c = counters.clone();
        let reqs = requests.clone();
        let claim = next.clone();
        let timeout_d = Duration::from_millis(args.request_timeout_ms);
        set.spawn(async move { worker(reqs, claim, client, stmt, c, timeout_d).await });
    }
    let mut histogram = LatencyHistogram::default();
    let mut workers_left = args.connections;
    while workers_left > 0 {
        tokio::select! {
            joined=set.join_next()=> match joined {
                Some(Ok(Ok(h))) => { histogram.merge(&h)?; workers_left-=1; }
                Some(Ok(Err(e))) => { set.abort_all(); while set.join_next().await.is_some() {} return Err(e); }
                Some(Err(e)) => { set.abort_all(); while set.join_next().await.is_some() {} return Err(anyhow!(e)); }
                None => break,
            },
            _=ticker.tick()=> { sample_row(&monitor,&mut csv,&counters,&mut sample_at,&mut prev_req,&mut prev_stmt,second).await?; second+=1; }
        }
    }
    let measurement_end = unix_ns();
    let duration = start.elapsed().as_secs_f64();
    sample_row(
        &monitor,
        &mut csv,
        &counters,
        &mut sample_at,
        &mut prev_req,
        &mut prev_stmt,
        second,
    )
    .await?;
    csv.flush()?;
    let lsn_after: String = monitor
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await?
        .get(0);
    let settled = settle_data_backends(&monitor).await?;
    let after = wal_snapshot(&monitor).await?;
    let lsn_delta_bytes = wal_delta(&monitor, &lsn_after, &lsn_before).await?;
    let completed = counters.completed.load(Ordering::Relaxed);
    let statements = counters.statements.load(Ordering::Relaxed);
    let row=monitor.query_one("SELECT count(*)::bigint, count(*) FILTER (WHERE balance<>$1 OR ledger_count<>$1)::bigint, COALESCE(sum(balance),0)::bigint, COALESCE(sum(ledger_count),0)::bigint FROM (SELECT a.account_id,a.balance,count(l.transaction_id)::bigint ledger_count FROM accounts a LEFT JOIN ledger l USING(account_id) GROUP BY a.account_id,a.balance) q", &[&i64::from(args.requests_per_user)]).await?;
    let accounts: u64 = row.get::<_, i64>(0) as u64;
    let bad: u64 = row.get::<_, i64>(1) as u64;
    let sum: i64 = row.get(2);
    let rows: i64 = row.get(3);
    let valid = accounts == u64::from(args.users)
        && bad == 0
        && sum == total as i64
        && rows == total as i64
        && completed == total;
    let verification = Verification {
        accounts,
        bad_accounts: bad,
        sum_balance: sum,
        ledger_rows: rows,
        valid,
    };
    let summary = histogram.summary();
    let result = ResultJson {
        measurement_start_unix_ns: measurement_start,
        measurement_end_unix_ns: measurement_end,
        duration_s: duration,
        requests: completed,
        statements,
        rps_request: completed as f64 / duration,
        rps_io: statements as f64 / duration,
        latency_us: LatencyJson {
            p50: summary.p50_us,
            p95: summary.p95_us,
            p99: summary.p99_us,
            max: summary.max_us,
        },
        data_connections: args.connections,
        monitor_connections: 1,
        effective: Effective {
            pg_url: args.pg_url.clone(),
            users: args.users,
            requests_per_user: args.requests_per_user,
            connections: args.connections,
            request_timeout_ms: args.request_timeout_ms,
            sample_interval_ms: args.sample_interval_ms,
            postgres_version: version,
        },
        verification,
        mode: "single",
        wal_stats: WalStats {
            before,
            after,
            lsn_before,
            lsn_after,
            lsn_delta_bytes,
            settled,
        },
    };
    fs::write(
        args.output_dir.join("result.json"),
        serde_json::to_vec_pretty(&result)?,
    )?;
    fs::write(args.output_dir.join("latency.hdr"), histogram.serialize()?)?;
    if !valid {
        let invalid = serde_json::json!({"error":"verification failed","verification":result.verification,"completed_requests":completed,"expected_requests":total});
        fs::write(
            args.output_dir.join("invalid.json"),
            serde_json::to_vec_pretty(&invalid)?,
        )?;
        bail!("verification failed");
    }
    Ok(())
}

async fn worker(
    requests: Arc<Vec<Request>>,
    next: Arc<AtomicUsize>,
    client: Client,
    stmt: Statement,
    counters: Arc<Counters>,
    timeout_d: Duration,
) -> Result<LatencyHistogram> {
    let mut histogram = LatencyHistogram::default();
    loop {
        let i = next.fetch_add(1, Ordering::Relaxed);
        if i >= requests.len() {
            break;
        }
        counters.remaining.fetch_sub(1, Ordering::Relaxed);
        counters.inflight.fetch_add(1, Ordering::Relaxed);
        counters.busy.fetch_add(1, Ordering::Relaxed);
        let request = &requests[i];
        let before = Instant::now();
        let result = timeout(
            timeout_d,
            client.execute(&stmt, &[&request.account, &request.tx, &1i64]),
        )
        .await;
        let us = before.elapsed().as_micros().max(1).min(u64::MAX as u128) as u64;
        counters.busy.fetch_sub(1, Ordering::Relaxed);
        counters.inflight.fetch_sub(1, Ordering::Relaxed);
        result.context("request timeout")??;
        histogram.record_us(us)?;
        counters.statements.fetch_add(1, Ordering::Relaxed);
        counters.completed.fetch_add(1, Ordering::Relaxed);
    }
    Ok(histogram)
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let output = args.output_dir.clone();
    if let Err(e) = run(args).await {
        eprintln!("nobatch_pg failed: {e:#}");
        if output.exists() {
            let _ = fs::write(
                output.join("invalid.json"),
                serde_json::json!({"error":format!("{e:#}")}).to_string() + "\n",
            );
        }
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn flat_global_order_is_round_robin_and_complete() {
        let users = 17;
        let per_user = 4;
        let requests = build_requests(users, per_user);
        assert_eq!(requests.len(), users as usize * per_user as usize);
        assert_eq!(
            requests.iter().map(|r| r.account).collect::<Vec<_>>(),
            (0..per_user)
                .flat_map(|_| (0..users).map(account_uuid))
                .collect::<Vec<_>>()
        );
    }
}
