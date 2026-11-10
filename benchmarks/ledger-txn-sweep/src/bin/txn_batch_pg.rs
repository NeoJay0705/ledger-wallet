use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use serde::Serialize;
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
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

const BATCH_SQL: &str = "WITH input AS (\n  SELECT acc, tx, amt, ord\n  FROM unnest($1::uuid[], $2::uuid[], $3::bigint[]) WITH ORDINALITY AS i(acc, tx, amt, ord)\n),\nprobe AS (                                   -- one probe per row: does it already exist?\n  SELECT i.*, (l.transaction_id IS NOT NULL) AS ex,\n         l.amount AS o_amt, l.before_balance AS o_before, l.after_balance AS o_after\n  FROM input i LEFT JOIN ledger l ON l.account_id = i.acc AND l.transaction_id = i.tx\n),\nvalid AS (                                   -- fresh, amount > 0, account exists\n  SELECT p.* FROM probe p\n  WHERE NOT p.ex AND p.amt > 0\n    AND EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = p.acc)\n),\nagg  AS (SELECT acc, sum(amt)::bigint AS s FROM valid GROUP BY acc),\nupd  AS (UPDATE accounts a SET balance = a.balance + agg.s\n         FROM agg WHERE a.account_id = agg.acc\n         RETURNING a.account_id, a.balance - agg.s AS start_bal),\ncalc AS (SELECT v.acc, v.tx, v.amt,\n                u.start_bal + sum(v.amt) OVER (PARTITION BY v.acc ORDER BY v.ord) - v.amt AS before_bal\n         FROM valid v JOIN upd u ON u.account_id = v.acc),\nins  AS (INSERT INTO ledger(transaction_id, account_id, amount, before_balance, after_balance)\n         SELECT tx, acc, amt, before_bal, before_bal + amt FROM calc\n         RETURNING transaction_id, account_id, amount, before_balance, after_balance)\nSELECT 'applied', transaction_id, account_id, amount, before_balance, after_balance FROM ins\nUNION ALL SELECT 'existing', tx, acc, o_amt, o_before, o_after FROM probe WHERE ex\nUNION ALL SELECT 'rejected:invalid_amount', tx, acc, amt, NULL, NULL FROM probe WHERE NOT ex AND amt <= 0\nUNION ALL SELECT 'rejected:unknown_account', tx, acc, amt, NULL, NULL FROM probe p\n          WHERE NOT ex AND amt > 0 AND NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = p.acc);";

#[derive(Parser, Debug)]
#[command(name = "txn_batch_pg")]
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
    queues: usize,
    #[arg(long)]
    connections: Option<usize>,
    #[arg(long, default_value_t = 1000)]
    batch_max: usize,
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
    #[arg(long, default_value_t = 0)]
    retry_pct: u32,
    #[arg(long, default_value_t = false)]
    inject_rejects: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Request {
    user: u32,
    tx: Uuid,
    amount: i64,
    is_retry: bool,
    is_retry_source: bool,
}
#[derive(Clone, Debug)]
struct Workload {
    windows: Vec<Vec<Request>>,
    retries_sent: u64,
    injected_unknown: u64,
    injected_invalid: u64,
}
fn build_queues(users: u32, per_user: u32, queues: usize) -> Vec<Vec<Request>> {
    let mut result = vec![Vec::new(); queues];
    for _ in 0..per_user {
        for user in 0..users {
            result[user as usize % queues].push(Request {
                user,
                tx: Uuid::new_v4(),
                amount: 1,
                is_retry: false,
                is_retry_source: false,
            });
        }
    }
    result
}
fn merge_workloads(workloads: &[Workload], connections: usize) -> Vec<Workload> {
    (0..connections).map(|c| {
        let owned: Vec<usize> = (0..workloads.len()).filter(|q| q % connections == c).collect();
        let mut merged = Vec::new(); let mut i = 0;
        loop { let mut found = false; for &q in &owned { if let Some(w) = workloads[q].windows.get(i) { merged.push(w.clone()); found = true; } } if !found { break; } i += 1; }
        Workload { windows: merged, retries_sent: owned.iter().map(|&q| workloads[q].retries_sent).sum(), injected_unknown: owned.iter().map(|&q| workloads[q].injected_unknown).sum(), injected_invalid: owned.iter().map(|&q| workloads[q].injected_invalid).sum() }
    }).collect()
}
fn retry_count(n: usize, pct: u32) -> usize {
    ((n as f64 * pct as f64 / 100.0).round()) as usize
}
fn build_workload(
    base: Vec<Request>,
    batch_max: usize,
    retry_pct: u32,
    inject_rejects: bool,
    users: u32,
    queue_index: usize,
) -> Result<Workload> {
    if batch_max == 0 {
        bail!("batch-max must be positive");
    }
    let mut windows = base
        .chunks(batch_max)
        .map(|w| w.to_vec())
        .collect::<Vec<_>>();
    if windows.is_empty() {
        windows.push(Vec::new());
    }
    let n = base.len();
    let count = retry_count(n, retry_pct);
    let source_count: usize = windows
        .iter()
        .take(windows.len().saturating_sub(1))
        .map(Vec::len)
        .sum();
    if count > source_count {
        bail!("retry count {count} exceeds non-last-window source requests {source_count}");
    }
    let mut retries_sent = 0u64;
    if count > 0 {
        let mut source_positions = Vec::with_capacity(source_count);
        for wi in 0..windows.len() - 1 {
            for ri in 0..windows[wi].len() {
                source_positions.push((wi, ri));
            }
        }
        let mut chosen = Vec::with_capacity(count);
        for j in 0..count {
            let at = ((j + 1) * source_count / (count + 1)).min(source_count - 1);
            chosen.push(source_positions[at]);
        }
        for (wi, ri) in chosen {
            let mut source = windows[wi][ri].clone();
            source.is_retry_source = true;
            windows[wi][ri] = source.clone();
            source.is_retry = true;
            source.is_retry_source = false;
            windows[wi + 1].push(source);
            retries_sent += 1;
        }
    }
    let mut injected_unknown = 0;
    let mut injected_invalid = 0;
    if inject_rejects {
        let known = windows
            .iter()
            .flatten()
            .next()
            .map(|r| r.user)
            .ok_or_else(|| anyhow!("cannot inject rejects into an empty queue"))?;
        let first = &mut windows[0];
        first.push(Request {
            user: users.saturating_add(queue_index as u32),
            tx: Uuid::new_v4(),
            amount: 1,
            is_retry: false,
            is_retry_source: false,
        });
        injected_unknown = 1;
        first.push(Request {
            user: known,
            tx: Uuid::new_v4(),
            amount: 0,
            is_retry: false,
            is_retry_source: false,
        });
        injected_invalid = 1;
    }
    Ok(Workload {
        windows,
        retries_sent,
        injected_unknown,
        injected_invalid,
    })
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct Key {
    account: Uuid,
    tx: Uuid,
}
#[derive(Clone, Debug, Eq, PartialEq)]
enum Outcome {
    Applied,
    Existing,
    RejectedInvalidAmount,
    RejectedUnknownAccount,
}
#[derive(Clone, Debug, Eq, PartialEq)]
struct Record {
    outcome: Outcome,
    amount: Option<i64>,
    before: Option<i64>,
    after: Option<i64>,
}
#[derive(Debug, Default)]
struct Dedupe {
    canonical: Vec<Request>,
    fanout: Vec<usize>,
    keys: HashMap<Key, usize>,
}
fn dedupe_first(requests: &[Request]) -> Dedupe {
    let mut d = Dedupe::default();
    for r in requests {
        let key = Key {
            account: account_uuid(r.user),
            tx: r.tx,
        };
        let idx = if let Some(&idx) = d.keys.get(&key) {
            idx
        } else {
            let idx = d.canonical.len();
            d.canonical.push(r.clone());
            d.keys.insert(key, idx);
            idx
        };
        d.fanout.push(idx);
    }
    d
}
fn stable_sort_by_account(requests: &mut [Request]) {
    requests.sort_by_key(|r| account_uuid(r.user));
}
fn map_outcomes(
    canonical: &[Request],
    returned: Vec<(Key, Record)>,
) -> Result<(Vec<Record>, u64, u64)> {
    let expected = canonical
        .iter()
        .map(|r| Key {
            account: account_uuid(r.user),
            tx: r.tx,
        })
        .collect::<HashSet<_>>();
    let mut mapped = HashMap::new();
    let mut unexpected = 0;
    let mut duplicate = 0;
    for (key, row) in returned {
        if !expected.contains(&key) {
            unexpected += 1;
        } else if mapped.insert(key, row).is_some() {
            duplicate += 1;
        }
    }
    let missing = expected.len().saturating_sub(mapped.len()) as u64;
    if missing > 0 || unexpected > 0 || duplicate > 0 {
        bail!("outcome mapping bijection failed: missing={missing}, unexpected={}, duplicate={duplicate}", unexpected);
    }
    Ok((
        canonical
            .iter()
            .map(|r| {
                mapped[&Key {
                    account: account_uuid(r.user),
                    tx: r.tx,
                }]
                    .clone()
            })
            .collect(),
        missing,
        unexpected,
    ))
}

#[derive(Default)]
struct Counters {
    completed: AtomicU64,
    statements: AtomicU64,
    attempts: AtomicU64,
    inflight: AtomicU64,
    busy: AtomicU64,
    remaining: AtomicU64,
    applied: AtomicU64,
    existing: AtomicU64,
    rejected_invalid_amount: AtomicU64,
    rejected_unknown_account: AtomicU64,
    missing: AtomicU64,
    unexpected: AtomicU64,
    replay_mismatch: AtomicU64,
    statement_retries: AtomicU64,
    retries_sent: AtomicU64,
    injected_unknown: AtomicU64,
    injected_invalid: AtomicU64,
    sent_requests: AtomicU64,
    windows: AtomicU64,
    max_window_rows: AtomicU64,
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
struct Outcomes {
    applied: u64,
    existing: u64,
    rejected_invalid_amount: u64,
    rejected_unknown_account: u64,
    missing: u64,
    unexpected: u64,
    replay_mismatch: u64,
    statement_retries: u64,
    attempts: u64,
    retries_sent: u64,
    injected_unknown: u64,
    injected_invalid: u64,
    sent_requests: u64,
    windows: u64,
    max_window_rows: u64,
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
    rps_applied_unique: f64,
    latency_us: LatencyJson,
    batch_size: BatchJson,
    data_connections: usize,
    monitor_connections: usize,
    effective: Effective,
    verification: Verification,
    wal_stats: WalStats,
    outcomes: Outcomes,
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
struct BatchJson {
    min: usize,
    p50: usize,
    max: usize,
    mean: f64,
}
#[derive(Serialize)]
struct Effective {
    pg_url: String,
    users: u32,
    requests_per_user: u32,
    queues: usize,
    connections: usize,
    batch_max: usize,
    request_timeout_ms: u64,
    sample_interval_ms: u64,
    postgres_version: String,
    retry_pct: u32,
    inject_rejects: bool,
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
    let text:String=client.query_one("SELECT json_build_object('pg_stat_wal',(SELECT row_to_json(w) FROM pg_stat_wal w),'pg_stat_checkpointer',(SELECT row_to_json(c) FROM pg_stat_checkpointer c),'pg_stat_io',coalesce((SELECT json_agg(row_to_json(i)) FROM pg_stat_io i WHERE object='wal'),'[]'::json),'lsn',pg_current_wal_lsn()::text)::text",&[]).await?.get(0);
    Ok(serde_json::from_str(&text)?)
}
async fn settle_data_backends(client: &Client) -> Result<bool> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let count:i64=client.query_one("SELECT count(*) FROM pg_stat_activity WHERE backend_type='client backend' AND pid<>pg_backend_pid()",&[]).await?.get(0);
        if count == 0 {
            return Ok(true);
        }
        if Instant::now() >= deadline {
            return Ok(false);
        }
        tokio::time::sleep(Duration::from_millis(100)).await
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
    c: &Counters,
    prior: &mut Instant,
    prev_req: &mut u64,
    prev_stmt: &mut u64,
    second: u64,
) -> Result<()> {
    let now = Instant::now();
    let elapsed = now.duration_since(*prior).as_secs_f64().max(1e-9);
    *prior = now;
    let req = c.completed.load(Ordering::Relaxed);
    let stmt = c.statements.load(Ordering::Relaxed);
    let active:i64=client.query_one("SELECT count(*) FROM pg_stat_activity WHERE state='active' AND backend_type='client backend' AND datname='ledger' AND pid<>pg_backend_pid()",&[]).await?.get(0);
    let dr = req - *prev_req;
    let ds = stmt - *prev_stmt;
    *prev_req = req;
    *prev_stmt = stmt;
    use std::io::Write;
    writeln!(
        file,
        "{second},{},{dr},{ds},{:.3},{:.3},{},{},{},{}",
        utc_now(),
        dr as f64 / elapsed,
        ds as f64 / elapsed,
        c.inflight.load(Ordering::Relaxed),
        c.busy.load(Ordering::Relaxed),
        c.remaining.load(Ordering::Relaxed),
        active.max(0)
    )?;
    Ok(())
}

async fn run(args: Args) -> Result<()> {
    if args.users == 0
        || args.requests_per_user == 0
        || args.queues == 0
        || args.batch_max == 0
        || args.sample_interval_ms == 0
    {
        bail!("users, requests-per-user, queues, batch-max, and sample interval must be positive");
    }
    let connections = args.connections.unwrap_or(args.queues);
    if connections == 0 || connections > args.queues { bail!("connections must be between 1 and queues"); }
    if args.retry_pct > 100 {
        bail!("retry-pct must be at most 100");
    }
    let total = u64::from(args.users) * u64::from(args.requests_per_user);
    fs::create_dir_all(&args.output_dir)?;
    let monitor = connect(&args.pg_url).await.context("connect monitor")?;
    monitor.batch_execute("CREATE TABLE IF NOT EXISTS accounts(account_id uuid PRIMARY KEY,balance bigint NOT NULL DEFAULT 0); CREATE TABLE IF NOT EXISTS ledger(transaction_id uuid NOT NULL,account_id uuid NOT NULL REFERENCES accounts(account_id),amount bigint NOT NULL CHECK(amount>0),before_balance bigint NOT NULL,after_balance bigint NOT NULL,PRIMARY KEY(account_id,transaction_id),CHECK(after_balance=before_balance+amount));").await.context("create benchmark schema")?;
    let ids: Vec<Uuid> = (0..args.users).map(account_uuid).collect();
    monitor.execute("INSERT INTO accounts(account_id,balance) SELECT id,0 FROM unnest($1::uuid[]) AS u(id) ON CONFLICT(account_id) DO NOTHING",&[&ids]).await.context("bulk account insert")?;
    monitor
        .query_one("SELECT pg_stat_force_next_flush()", &[])
        .await?;
    let version: String = monitor.query_one("SHOW server_version", &[]).await?.get(0);
    let bases = build_queues(args.users, args.requests_per_user, args.queues);
    let mut workloads = Vec::with_capacity(args.queues);
    for (qi, base) in bases.into_iter().enumerate() {
        workloads.push(build_workload(
            base,
            args.batch_max,
            args.retry_pct,
            args.inject_rejects,
            args.users,
            qi,
        )?);
    }
    let merged = merge_workloads(&workloads, connections);
    let mut data = Vec::with_capacity(connections);
    for _ in 0..connections {
        let c = connect(&args.pg_url).await?;
        let p = c.prepare(BATCH_SQL).await?;
        data.push((c, p));
    }
    let counters = Arc::new(Counters::default());
    let all_sent: u64 = workloads
        .iter()
        .flat_map(|w| &w.windows)
        .map(|w| w.len() as u64)
        .sum();
    counters.remaining.store(all_sent, Ordering::Relaxed);
    counters.retries_sent.store(
        workloads.iter().map(|w| w.retries_sent).sum(),
        Ordering::Relaxed,
    );
    counters.injected_unknown.store(
        workloads.iter().map(|w| w.injected_unknown).sum(),
        Ordering::Relaxed,
    );
    counters.injected_invalid.store(
        workloads.iter().map(|w| w.injected_invalid).sum(),
        Ordering::Relaxed,
    );
    counters.sent_requests.store(all_sent, Ordering::Relaxed);
    fs::write(&args.ready_file, format!("ready {}\n", utc_now())).context("write ready file")?;
    let wait_start = Instant::now();
    while !args.go_file.exists() {
        if wait_start.elapsed() > Duration::from_millis(args.handshake_timeout_ms) {
            bail!("GO handshake timeout")
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let before = wal_snapshot(&monitor).await?;
    let lsn_before = before["lsn"]
        .as_str()
        .ok_or_else(|| anyhow!("missing lsn in before WAL snapshot"))?
        .to_owned();
    let measurement_start = unix_ns();
    let start = Instant::now();
    let mut csv = fs::File::create(args.output_dir.join("per_second.csv"))?;
    use std::io::Write;
    writeln!(csv,"second,wall_utc,requests_delta,statements_delta,rps_request,rps_io,inflight_requests,busy_conns,queue_remaining,server_active")?;
    let mut ticker = tokio::time::interval(Duration::from_millis(args.sample_interval_ms));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ticker.tick().await;
    let mut sample_at = Instant::now();
    let mut prev_req = 0;
    let mut prev_stmt = 0;
    let mut second = 0u64;
    let mut set = JoinSet::new();
    for (work, (client, stmt)) in merged.into_iter().zip(data) {
        let c = counters.clone();
        let td = Duration::from_millis(args.request_timeout_ms);
        let bm = args.batch_max;
        set.spawn(async move { worker(work, client, stmt, c, td, bm).await });
    }
    let mut histogram = LatencyHistogram::default();
    let mut batch_sizes = Vec::new();
    let mut workers_left = connections;
    while workers_left > 0 {
        tokio::select! {joined=set.join_next()=>match joined{Some(Ok(Ok((h,sizes))))=>{histogram.merge(&h)?;batch_sizes.extend(sizes);workers_left-=1},Some(Ok(Err(e)))=>{set.abort_all();while set.join_next().await.is_some(){}return Err(e)},Some(Err(e))=>{set.abort_all();while set.join_next().await.is_some(){}return Err(anyhow!(e))},None=>break},_=ticker.tick()=>{sample_row(&monitor,&mut csv,&counters,&mut sample_at,&mut prev_req,&mut prev_stmt,second).await?;second+=1;}}
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
    let applied = counters.applied.load(Ordering::Relaxed);
    let row=monitor.query_one("WITH ordered AS (SELECT account_id,amount,before_balance,after_balance,lag(after_balance) OVER (PARTITION BY account_id ORDER BY before_balance) AS prev_after,row_number() OVER (PARTITION BY account_id ORDER BY before_balance) AS rn,count(*) OVER (PARTITION BY account_id) AS n FROM ledger), checks AS (SELECT account_id,count(*)::bigint AS ledger_count,min(before_balance) FILTER (WHERE rn=1) AS first_before,max(after_balance) FILTER (WHERE rn=n) AS last_after,bool_and(after_balance=before_balance+amount) AS amount_ok,bool_and(before_balance=coalesce(prev_after,0)) AS chain_ok FROM ordered GROUP BY account_id), per_account AS (SELECT a.account_id,a.balance,coalesce(c.ledger_count,0) AS ledger_count,c.first_before,c.last_after,c.amount_ok,c.chain_ok FROM accounts a LEFT JOIN checks c USING(account_id)) SELECT count(*)::bigint,count(*) FILTER (WHERE balance<>$1 OR ledger_count<>$2 OR first_before<>0 OR last_after<>balance OR NOT coalesce(amount_ok,false) OR NOT coalesce(chain_ok,false))::bigint,coalesce(sum(balance),0)::bigint,coalesce(sum(ledger_count),0)::bigint FROM per_account",&[&i64::from(args.requests_per_user),&i64::from(args.requests_per_user)]).await.context("verification query")?;
    let accounts = row.get::<_, i64>(0) as u64;
    let bad = row.get::<_, i64>(1) as u64;
    let sum: i64 = row.get(2);
    let ledger_rows: i64 = row.get(3);
    let o = outcomes(&counters);
    let completed = counters.completed.load(Ordering::Relaxed);
    let valid = completed == o.sent_requests
        && o.missing == 0
        && o.unexpected == 0
        && o.replay_mismatch == 0
        && o.applied == total
        && o.existing == o.retries_sent
        && o.rejected_invalid_amount == o.injected_invalid
        && o.rejected_unknown_account == o.injected_unknown
        && accounts == u64::from(args.users)
        && bad == 0
        && sum == applied as i64
        && ledger_rows == total as i64;
    let verification = Verification {
        accounts,
        bad_accounts: bad,
        sum_balance: sum,
        ledger_rows,
        valid,
    };
    let summary = histogram.summary();
    let mut sorted = batch_sizes.clone();
    sorted.sort_unstable();
    let batch = BatchJson {
        min: *sorted.first().unwrap_or(&0),
        p50: *sorted.get(sorted.len() / 2).unwrap_or(&0),
        max: *sorted.last().unwrap_or(&0),
        mean: if sorted.is_empty() {
            0.0
        } else {
            sorted.iter().sum::<usize>() as f64 / sorted.len() as f64
        },
    };
    let result = ResultJson {
        measurement_start_unix_ns: measurement_start,
        measurement_end_unix_ns: measurement_end,
        duration_s: duration,
        requests: completed,
        statements: o.windows,
        rps_request: completed as f64 / duration,
        rps_io: o.windows as f64 / duration,
        rps_applied_unique: applied as f64 / duration,
        latency_us: LatencyJson {
            p50: summary.p50_us,
            p95: summary.p95_us,
            p99: summary.p99_us,
            max: summary.max_us,
        },
        batch_size: batch,
        data_connections: connections,
        monitor_connections: 1,
        effective: Effective {
            pg_url: args.pg_url.clone(),
            users: args.users,
            requests_per_user: args.requests_per_user,
            queues: args.queues,
            connections,
            batch_max: args.batch_max,
            request_timeout_ms: args.request_timeout_ms,
            sample_interval_ms: args.sample_interval_ms,
            postgres_version: version,
            retry_pct: args.retry_pct,
            inject_rejects: args.inject_rejects,
        },
        verification,
        wal_stats: WalStats {
            before,
            after,
            lsn_before,
            lsn_after,
            lsn_delta_bytes,
            settled,
        },
        outcomes: o,
    };
    fs::write(
        args.output_dir.join("result.json"),
        serde_json::to_vec_pretty(&result)?,
    )?;
    fs::write(args.output_dir.join("latency.hdr"), histogram.serialize()?)?;
    if !valid {
        fs::write(args.output_dir.join("invalid.json"),serde_json::json!({"error":"verification failed","verification":result.verification,"outcomes":result.outcomes}).to_string()+"\n")?;
        bail!("verification failed")
    }
    Ok(())
}
fn outcomes(c: &Counters) -> Outcomes {
    let load = |x: &AtomicU64| x.load(Ordering::Relaxed);
    Outcomes {
        applied: load(&c.applied),
        existing: load(&c.existing),
        rejected_invalid_amount: load(&c.rejected_invalid_amount),
        rejected_unknown_account: load(&c.rejected_unknown_account),
        missing: load(&c.missing),
        unexpected: load(&c.unexpected),
        replay_mismatch: load(&c.replay_mismatch),
        statement_retries: load(&c.statement_retries),
        attempts: load(&c.attempts),
        retries_sent: load(&c.retries_sent),
        injected_unknown: load(&c.injected_unknown),
        injected_invalid: load(&c.injected_invalid),
        sent_requests: load(&c.sent_requests),
        windows: load(&c.windows),
        max_window_rows: load(&c.max_window_rows),
    }
}
async fn worker(
    work: Workload,
    client: Client,
    stmt: Statement,
    counters: Arc<Counters>,
    timeout_d: Duration,
    batch_max: usize,
) -> Result<(LatencyHistogram, Vec<usize>)> {
    let mut histogram = LatencyHistogram::default();
    let mut sizes = Vec::new();
    let mut first_applied: HashMap<Key, Record> = HashMap::new();
    for window in work.windows {
        let n = window.len() as u64;
        counters.remaining.fetch_sub(n, Ordering::Relaxed);
        counters.inflight.fetch_add(n, Ordering::Relaxed);
        counters.busy.fetch_add(1, Ordering::Relaxed);
        counters.statements.fetch_add(1, Ordering::Relaxed);
        counters.windows.fetch_add(1, Ordering::Relaxed);
        counters.max_window_rows.fetch_max(n, Ordering::Relaxed);
        sizes.push(window.len());
        let mut d = dedupe_first(&window);
        let mut order = (0..d.canonical.len()).collect::<Vec<_>>();
        order.sort_by_key(|&i| account_uuid(d.canonical[i].user));
        let mut remap = vec![0; order.len()];
        d.canonical = order
            .into_iter()
            .enumerate()
            .map(|(new, old)| {
                remap[old] = new;
                d.canonical[old].clone()
            })
            .collect();
        for idx in &mut d.fanout {
            *idx = remap[*idx];
        }
        let acc: Vec<Uuid> = d.canonical.iter().map(|r| account_uuid(r.user)).collect();
        let tx: Vec<Uuid> = d.canonical.iter().map(|r| r.tx).collect();
        let amount: Vec<i64> = d.canonical.iter().map(|r| r.amount).collect();
        let before = Instant::now();
        let query = timeout(timeout_d, async {
            let mut attempts = 0;
            loop {
                attempts += 1;
                counters.attempts.fetch_add(1, Ordering::Relaxed);
                match client.query(&stmt, &[&acc, &tx, &amount]).await {
                    Ok(rows) => break Ok((rows, attempts)),
                    Err(e)
                        if (e
                            .code()
                            .map(|c| c.code() == "23505" || c.code() == "40P01")
                            .unwrap_or(false))
                            && attempts < 3 =>
                    {
                        counters.statement_retries.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    Err(e) => break Err(e),
                }
            }
        })
        .await;
        let us = before.elapsed().as_micros().max(1).min(u64::MAX as u128) as u64;
        counters.busy.fetch_sub(1, Ordering::Relaxed);
        counters.inflight.fetch_sub(n, Ordering::Relaxed);
        let (rows, _) = query.context("batch query timeout")??;
        let mut raw = Vec::new();
        for row in rows {
            let status: String = row.get(0);
            let tx: Uuid = row.get(1);
            let account: Uuid = row.get(2);
            let amount: Option<i64> = row.get(3);
            let before: Option<i64> = row.get(4);
            let after: Option<i64> = row.get(5);
            let outcome = match status.as_str() {
                "applied" => Outcome::Applied,
                "existing" => Outcome::Existing,
                "rejected:invalid_amount" => Outcome::RejectedInvalidAmount,
                "rejected:unknown_account" => Outcome::RejectedUnknownAccount,
                _ => bail!("unknown outcome status {status}"),
            };
            raw.push((
                Key { account, tx },
                Record {
                    outcome,
                    amount,
                    before,
                    after,
                },
            ));
        }
        let (records, missing, unexpected) = match map_outcomes(&d.canonical, raw) {
            Ok(v) => v,
            Err(e) => {
                counters
                    .missing
                    .fetch_add(d.canonical.len() as u64, Ordering::Relaxed);
                return Err(e);
            }
        };
        counters.missing.fetch_add(missing, Ordering::Relaxed);
        counters.unexpected.fetch_add(unexpected, Ordering::Relaxed);
        let mut canonical_records = HashMap::new();
        for (r, record) in d.canonical.iter().zip(records.iter()) {
            let key = Key {
                account: account_uuid(r.user),
                tx: r.tx,
            };
            canonical_records.insert(key, record.clone());
            if r.is_retry_source && record.outcome == Outcome::Applied {
                first_applied.insert(key, record.clone());
            }
            if r.is_retry {
                let prior = first_applied.remove(&key);
                if record.outcome != Outcome::Existing
                    || prior.as_ref().map(|p| (p.amount, p.before, p.after))
                        != Some((record.amount, record.before, record.after))
                {
                    counters.replay_mismatch.fetch_add(1, Ordering::Relaxed);
                }
            }
            match record.outcome {
                Outcome::Applied => {
                    counters.applied.fetch_add(1, Ordering::Relaxed);
                }
                Outcome::Existing => {
                    counters.existing.fetch_add(1, Ordering::Relaxed);
                }
                Outcome::RejectedInvalidAmount => {
                    counters
                        .rejected_invalid_amount
                        .fetch_add(1, Ordering::Relaxed);
                }
                Outcome::RejectedUnknownAccount => {
                    counters
                        .rejected_unknown_account
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        // Fan out terminal outcomes to duplicate requests; duplicates of successful rows are existing.
        let mut seen = HashSet::new();
        for (request, idx) in window.iter().zip(d.fanout.iter()) {
            let canon = &d.canonical[*idx];
            let key = Key {
                account: account_uuid(canon.user),
                tx: canon.tx,
            };
            let record = &canonical_records[&key];
            if !seen.insert(key) {
                match record.outcome {
                    Outcome::Applied | Outcome::Existing => {
                        counters.existing.fetch_add(1, Ordering::Relaxed);
                    }
                    Outcome::RejectedInvalidAmount => {
                        counters
                            .rejected_invalid_amount
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    Outcome::RejectedUnknownAccount => {
                        counters
                            .rejected_unknown_account
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            let _ = request;
        }
        for _ in 0..window.len() {
            histogram.record_us(us)?;
        }
        counters.completed.fetch_add(n, Ordering::Relaxed);
        // Release retry records once their matching replay has been processed (remove above); no other source records retained.
        let _ = batch_max;
    }
    Ok((histogram, sizes))
}
#[tokio::main]
async fn main() {
    let args = Args::parse();
    let output = args.output_dir.clone();
    if let Err(e) = run(args).await {
        eprintln!("txn_batch_pg failed: {e:#}");
        if output.exists() {
            let _ = fs::write(
                output.join("invalid.json"),
                serde_json::json!({"error":format!("{e:#}")}).to_string() + "\n",
            );
        }
        std::process::exit(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn req(user: u32, tx: u128, amount: i64) -> Request {
        Request {
            user,
            tx: Uuid::from_u128(tx),
            amount,
            is_retry: false,
            is_retry_source: false,
        }
    }
    #[test]
    fn retry_counts_match_rounding_examples() {
        assert_eq!(retry_count(15_640, 5), 782);
        assert_eq!(retry_count(15_600, 5), 780);
        let queues = build_queues(100_000, 40, 256);
        assert_eq!(
            queues
                .iter()
                .map(|q| retry_count(q.len(), 5))
                .sum::<usize>(),
            200_000
        );
    }
    #[test]
    fn retries_are_in_later_windows_and_never_source_last_window() {
        let work = build_workload(
            (0..23).map(|i| req(i, 100 + i as u128, 1)).collect(),
            10,
            20,
            false,
            30,
            0,
        )
        .unwrap();
        for (wi, w) in work.windows.iter().enumerate() {
            for r in w.iter().filter(|r| r.is_retry) {
                assert!(wi > 0);
                assert!(work.windows[..wi]
                    .iter()
                    .any(|prior| prior.iter().any(|p| p.tx == r.tx && p.is_retry_source)));
            }
        }
        assert!(!work
            .windows
            .last()
            .unwrap()
            .iter()
            .any(|r| r.is_retry_source));
        assert_eq!(work.retries_sent, 5);
    }
    #[test]
    fn retry_sources_spread_and_append_to_next_window() {
        let work = build_workload(
            (0..31).map(|i| req(i, 100 + i as u128, 1)).collect(),
            10,
            10,
            false,
            40,
            0,
        )
        .unwrap();
        for wi in 1..work.windows.len() {
            let w = &work.windows[wi];
            let retries = w.iter().filter(|r| r.is_retry).collect::<Vec<_>>();
            let new_rows = (31usize.saturating_sub(wi * 10)).min(10);
            assert!(w.iter().position(|r| r.is_retry).unwrap_or(w.len()) >= new_rows);
            for r in retries {
                assert!(work.windows[wi - 1]
                    .iter()
                    .any(|s| s.tx == r.tx && s.is_retry_source));
            }
        }
    }
    #[test]
    fn reject_injection_adds_one_unknown_and_one_invalid() {
        let base = (0..10).map(|i| req(i, 100 + i as u128, 1)).collect();
        let w = build_workload(base, 5, 0, true, 10, 2).unwrap();
        assert_eq!((w.injected_unknown, w.injected_invalid), (1, 1));
        assert_eq!(w.windows[0].len(), 7);
        assert_eq!(w.windows[0][5].user, 12);
        assert_eq!(w.windows[0][6].amount, 0);
    }
    #[test]
    fn dedupe_keeps_first_amount_and_rejected_canonical_fans_out() {
        let a = req(1, 1, 0);
        let b = req(1, 1, 9);
        let d = dedupe_first(&[a.clone(), b]);
        assert_eq!(d.canonical.len(), 1);
        assert_eq!(d.canonical[0].amount, 0);
        assert_eq!(d.fanout, vec![0, 0]);
        let record = Record {
            outcome: Outcome::RejectedInvalidAmount,
            amount: Some(0),
            before: None,
            after: None,
        };
        let fanout = d
            .fanout
            .iter()
            .map(|idx| {
                assert_eq!(d.canonical[*idx].amount, 0);
                record.clone()
            })
            .collect::<Vec<_>>();
        assert_eq!(fanout, vec![record.clone(), record]);
    }
    #[test]
    fn stable_sort_preserves_each_accounts_input_order() {
        let mut v = vec![req(2, 1, 1), req(1, 2, 1), req(2, 3, 1), req(1, 4, 1)];
        stable_sort_by_account(&mut v);
        assert_eq!(
            v.iter()
                .map(|r| (r.user, r.tx.as_u128()))
                .collect::<Vec<_>>(),
            vec![(1, 2), (1, 4), (2, 1), (2, 3)]
        );
    }
    #[test]
    fn outcome_mapping_detects_missing_unexpected_and_duplicate_keys() {
        let a = req(1, 1, 1);
        let b = req(2, 2, 1);
        let k1 = Key {
            account: account_uuid(1),
            tx: a.tx,
        };
        let k2 = Key {
            account: account_uuid(2),
            tx: b.tx,
        };
        let row = Record {
            outcome: Outcome::Applied,
            amount: Some(1),
            before: Some(0),
            after: Some(1),
        };
        assert!(map_outcomes(&[a.clone(), b.clone()], vec![(k1, row.clone())]).is_err());
        assert!(map_outcomes(&[a.clone()], vec![(k1, row.clone()), (k2, row.clone())]).is_err());
        assert!(map_outcomes(&[a], vec![(k1, row.clone()), (k1, row)]).is_err());
    }
    fn test_workloads(users: u32, per_user: u32, queues: usize, batch: usize) -> Vec<Workload> {
        build_queues(users, per_user, queues)
            .into_iter()
            .enumerate()
            .map(|(q, base)| build_workload(base, batch, 0, false, users, q).unwrap())
            .collect()
    }

    fn request_multiset(workloads: &[Workload]) -> HashMap<(u32, Uuid), usize> {
        let mut counts = HashMap::new();
        for request in workloads.iter().flat_map(|w| &w.windows).flatten() {
            *counts.entry((request.user, request.tx)).or_insert(0) += 1;
        }
        counts
    }

    #[test]
    fn merger_with_one_connection_per_queue_matches_old_per_queue_sequence() {
        let workloads = test_workloads(1000, 2, 8, 50);
        let merged = merge_workloads(&workloads, workloads.len());
        assert_eq!(merged.len(), workloads.len());
        for (original, actual) in workloads.iter().zip(merged) {
            assert_eq!(actual.windows, original.windows);
        }
    }

    #[test]
    fn merger_handles_unequal_queue_lengths_and_exhausted_queues() {
        for (users, per_user, batch) in [(1000, 40, 50), (1000, 40, 100), (1000, 40, 157), (2000, 40, 50)] {
            let workloads = test_workloads(users, per_user, 256, batch);
            assert!(workloads.iter().any(|w| w.windows.iter().map(Vec::len).sum::<usize>()
                != workloads[0].windows.iter().map(Vec::len).sum::<usize>()));
            if batch == 157 {
                assert!(workloads.iter().any(|w| w.windows.len() != workloads[0].windows.len()));
            }
            for connections in [1, 4, 256] {
                let merged = merge_workloads(&workloads, connections);
                assert_eq!(merged.iter().map(|w| w.windows.len()).sum::<usize>(),
                    workloads.iter().map(|w| w.windows.len()).sum::<usize>());
            }
        }
    }

    #[test]
    fn merger_emits_every_request_exactly_once_as_a_multiset() {
        let workloads = test_workloads(1000, 40, 256, 50);
        for connections in [1, 4, 256] {
            let merged = merge_workloads(&workloads, connections);
            assert_eq!(request_multiset(&merged), request_multiset(&workloads));
        }
    }

    #[test]
    fn merger_preserves_each_queue_window_order() {
        let workloads = test_workloads(1000, 40, 256, 50);
        for connections in [1, 4, 256] {
            let merged = merge_workloads(&workloads, connections);
            for (q, original) in workloads.iter().enumerate() {
                let actual: Vec<_> = merged[q % connections].windows.iter()
                    .filter(|window| window.first().is_some_and(|r| r.user as usize % 256 == q))
                    .cloned().collect();
                assert_eq!(actual, original.windows, "queue {q}, connections {connections}");
            }
        }
    }

    #[test]
    fn merger_supports_one_and_four_connections_for_256_queues() {
        let workloads = test_workloads(2000, 40, 256, 50);
        for connections in [1, 4] {
            let merged = merge_workloads(&workloads, connections);
            assert_eq!(merged.len(), connections);
            assert_eq!(merged.iter().map(|w| w.windows.len()).sum::<usize>(),
                workloads.iter().map(|w| w.windows.len()).sum::<usize>());
        }
    }

    #[test]
    fn every_merged_window_belongs_to_its_connection_owner_queues() {
        let workloads = test_workloads(1000, 40, 256, 50);
        for connections in [1, 4, 256] {
            for (connection, work) in merge_workloads(&workloads, connections).iter().enumerate() {
                for window in &work.windows {
                    let q = window[0].user as usize % 256;
                    assert_eq!(q % connections, connection);
                    assert!(window.iter().all(|r| r.user as usize % 256 == q));
                }
            }
        }
    }
}
