use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use mysql_async::{prelude::Queryable, Conn, Opts, Row};
use serde::Serialize;
use serde_json::{json, Value};
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
use uuid::Uuid;

#[path = "../metrics.rs"]
mod metrics;
use metrics::{account_uuid, LatencyHistogram};

#[derive(Parser, Debug)]
#[command(name = "txn_batch_tidb")]
struct Args {
    #[arg(long, default_value = "mysql://root@127.0.0.1:14000")]
    mysql_url: String,
    #[arg(long, default_value_t = 1)]
    stores: u32,
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
    shards: Vec<usize>,
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
        let mut windows = Vec::new();
        let mut shards = Vec::new();
        for i in 0.. {
            let mut found = false;
            for &q in &owned {
                if let Some(window) = workloads[q].windows.get(i) {
                    windows.push(window.clone());
                    shards.push(q);
                    found = true;
                }
            }
            if !found { break; }
        }
        Workload { windows, shards,
            retries_sent: owned.iter().map(|&q| workloads[q].retries_sent).sum(),
            injected_unknown: owned.iter().map(|&q| workloads[q].injected_unknown).sum(),
            injected_invalid: owned.iter().map(|&q| workloads[q].injected_invalid).sum() }
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
    let shards = vec![queue_index; windows.len()];
    Ok(Workload {
        windows,
        shards,
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
    round_trips: AtomicU64,
    sql_statements: AtomicU64,
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
    round_trips: u64,
    sql_statements: u64,
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
    outcomes: Outcomes,
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
    mysql_url: String,
    stores: u32,
    users: u32,
    requests_per_user: u32,
    queues: usize,
    connections: usize,
    batch_max: usize,
    request_timeout_ms: u64,
    sample_interval_ms: u64,
    tidb_version: String,
    retry_pct: u32,
    inject_rejects: bool,
    topology: Value,
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
async fn connect(url: &str) -> Result<Conn> {
    let mut conn = Conn::new(Opts::from_url(url)?).await?;
    conn.query_drop("USE ledger").await?;
    Ok(conn)
}
async fn sample_row(
    client: &mut Conn,
    monitor_id: u64,
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
    let processes: Vec<Row> = client.query("SHOW PROCESSLIST").await?;
    let active = processes.iter().filter(|row| {
        row.get::<String, _>(4).as_deref() != Some("Sleep")
            && row.get::<u64, _>(0) != Some(monitor_id)
    }).count() as i64;
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

fn store_balance_ok(rows: &[(u64, u64, u64)], expected: u32) -> bool {
    if rows.len() != expected as usize || rows.is_empty() { return false; }
    let regions: u64 = rows.iter().map(|r| r.1).sum();
    let leaders: u64 = rows.iter().map(|r| r.2).sum();
    rows.iter().all(|r| {
        let share = |n: u64, total: u64| total > 0 && n * 100 >= total * 40 && n * 100 <= total * 60;
        share(r.1, regions) && share(r.2, leaders)
    })
}
fn plan_ok(text: &str) -> bool { text.contains("Batch_Point_Get") && !text.contains("TableFullScan") }

async fn setup_gates(monitor: &mut Conn, args: &Args, workloads: &[Workload]) -> Result<Value> {
    let mut topology = json!({"stores_expected":args.stores,"table_regions":{"accounts":0,"ledger":0},"regions_with_peer_count_ne_1":null,"store_status":[],"store_balance":{"poll_seconds":0.0,"ok":false},"fk_enforced":false,"plan":{"q1":"","q2":"","q1_ok":false,"q2_ok":false},"tikv_config":{},"tikv_config_missing":true,"gates_passed":false});
    fs::write(args.output_dir.join("topology.json"),serde_json::to_vec_pretty(&topology)?)?;
    let config_result: Result<(String, Value, Value)> = async {
        let cfg: Vec<Row> = monitor.query("SELECT type,instance,`key`,value FROM information_schema.cluster_config").await?;
        let mut tsv = String::from("type\tinstance\tkey\tvalue\n");
        let keys = ["readpool.unified.max-thread-count","storage.scheduler-worker-pool-size","raftstore.store-pool-size","raftstore.apply-pool-size","raftstore.store-io-pool-size","raftstore.raft-write-wait-duration","storage.block-cache.capacity","raft-engine.enable"];
        let mut tikv = serde_json::Map::new(); let mut has_tikv=false;
        for r in &cfg {
            let ty: String=r.get("type").unwrap_or_default(); let inst: String=r.get("instance").unwrap_or_default(); let key: String=r.get("key").unwrap_or_default(); let val: String=r.get("value").unwrap_or_default();
            tsv.push_str(&format!("{ty}\t{inst}\t{key}\t{val}\n"));
            if ty=="tikv" { has_tikv=true; if keys.contains(&key.as_str()) && !tikv.contains_key(&key) { tikv.insert(key, json!(val)); } }
        }
        let vars: Vec<Row> = monitor.query("SELECT @@tidb_txn_mode,@@tidb_enable_1pc,@@tidb_enable_async_commit,@@foreign_key_checks,@@tidb_multi_statement_mode,@@tidb_scatter_region,version()").await?;
        let row=vars.first().ok_or_else(||anyhow!("variables query returned no row"))?;
        let names=["tidb_txn_mode","tidb_enable_1pc","tidb_enable_async_commit","foreign_key_checks","tidb_multi_statement_mode","tidb_scatter_region","version()"];
        let values=names.iter().enumerate().map(|(i,n)| (n.to_string(),json!(row.get::<String,_>(i).unwrap_or_default()))).collect::<serde_json::Map<_,_>>();
        Ok((tsv,Value::Object(values),json!({"values":tikv,"missing":!has_tikv})))
    }.await;
    let (tsv, vars, tikv) = config_result?;
    fs::write(args.output_dir.join("cluster_config.tsv"),tsv)?;
    fs::write(args.output_dir.join("variables.json"),serde_json::to_vec_pretty(&vars)?)?;
    topology["tikv_config"]=tikv["values"].clone(); topology["tikv_config_missing"]=tikv["missing"].clone();
    let gates: Result<()> = async {
        for table in ["accounts","ledger"] {
            let sql=format!("SELECT count(*) FROM information_schema.tikv_region_status WHERE db_name='ledger' AND is_index=0 AND table_name='{table}'");
            let n: u64=monitor.query_first(sql).await?.unwrap_or(0); topology["table_regions"][table]=json!(n);
            if n < args.queues as u64 { bail!("{table} region count {n} is below queues {}",args.queues); }
        }
        let bad: u64=monitor.query_first("SELECT count(*) FROM (SELECT s.region_id,count(p.peer_id) peers FROM information_schema.tikv_region_status s LEFT JOIN information_schema.tikv_region_peers p ON p.region_id=s.region_id WHERE s.db_name='ledger' AND s.is_index=0 AND s.table_name IN ('accounts','ledger') GROUP BY s.region_id HAVING peers<>1) x").await?.unwrap_or(0);
        topology["regions_with_peer_count_ne_1"]=json!(bad); if bad!=0 { bail!("{bad} regions do not have exactly one peer"); }
        let poll=Instant::now(); let deadline=poll+Duration::from_secs(120); let mut rows=Vec::new(); let mut balanced=false;
        loop {
            let rr: Vec<Row>=monitor.query("SELECT store_id,region_count,leader_count FROM information_schema.tikv_store_status").await?;
            rows=rr.iter().map(|r| (r.get::<u64,_>(0).unwrap_or(0),r.get::<u64,_>(1).unwrap_or(0),r.get::<u64,_>(2).unwrap_or(0))).collect();
            balanced=store_balance_ok(&rows,args.stores);
            if (args.stores==1 && rows.len()==1) || balanced || Instant::now()>=deadline {break;}
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        topology["store_status"]=json!(rows.iter().map(|r|json!({"store_id":r.0,"region_count":r.1,"leader_count":r.2})).collect::<Vec<_>>());
        topology["store_balance"]=json!({"poll_seconds":poll.elapsed().as_secs_f64(),"ok":if args.stores==1 {rows.len()==1} else {balanced}});
        if rows.len()!=args.stores as usize { bail!("store count mismatch: expected {}, observed {}",args.stores,rows.len()); }
        if args.stores>1 && !balanced { bail!("store balance gate failed after {:.1}s",poll.elapsed().as_secs_f64()); }
        let probe="INSERT INTO ledger(shard,account_id,transaction_id,amount,before_balance,after_balance) VALUES (0,x'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF',x'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF',1,0,1)";
        match monitor.query_drop(probe).await { Err(mysql_async::Error::Server(e)) if e.code==1452 => topology["fk_enforced"]=json!(true), Err(e)=>return Err(e.into()), Ok(())=>{ monitor.query_drop("DELETE FROM ledger WHERE shard=0 AND account_id=x'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF' AND transaction_id=x'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF'").await?; bail!("FK probe unexpectedly succeeded"); } }
        let first=workloads.first().and_then(|w|w.windows.first()).ok_or_else(||anyhow!("queue 0 has no first window"))?;
        let q1=q1_sql(0,first); let q2=q2_sql(0,first);
        let p1: Vec<Row>=monitor.query(format!("EXPLAIN {q1}")).await?; let p2: Vec<Row>=monitor.query(format!("EXPLAIN {q2}")).await?;
        let explain_text = |rows: &[Row]| {
            rows.iter().map(|row| {
                (0..row.len()).filter_map(|i| row.get::<Vec<u8>, _>(i))
                    .map(|bytes| String::from_utf8_lossy(&bytes).into_owned()).collect::<Vec<_>>().join(" | ")
            }).collect::<Vec<_>>().join("\n")
        };
        let text1=explain_text(&p1); let text2=explain_text(&p2);
        let ok1=plan_ok(&text1); let ok2=plan_ok(&text2);
        topology["plan"]=json!({"q1":text1,"q2":text2,"q1_ok":ok1,"q2_ok":ok2});
        if !ok1 || !ok2 { bail!("plan gate failed: q1_ok={ok1}, q2_ok={ok2}"); }
        Ok(())
    }.await;
    topology["gates_passed"]=json!(gates.is_ok());
    fs::write(args.output_dir.join("topology.json"),serde_json::to_vec_pretty(&topology)?)?;
    gates?;
    Ok(topology)
}

async fn run(args: Args) -> Result<()> {
    if args.users == 0
        || args.requests_per_user == 0
        || args.queues == 0
        || args.batch_max == 0
        || args.sample_interval_ms == 0
        || args.stores == 0
    {
        bail!("users, requests-per-user, queues, batch-max, stores, and sample interval must be positive");
    }
    let connections = args.connections.unwrap_or(args.queues);
    if connections == 0 || connections > args.queues { bail!("connections must be between 1 and queues"); }
    if args.retry_pct > 100 {
        bail!("retry-pct must be at most 100");
    }
    let total = u64::from(args.users) * u64::from(args.requests_per_user);
    fs::create_dir_all(&args.output_dir)?;
    let connect_until = Instant::now() + Duration::from_secs(600);
    let mut monitor = loop {
        match Conn::new(Opts::from_url(&args.mysql_url)?).await {
            Ok(mut c) => {
                c.query_drop("CREATE DATABASE IF NOT EXISTS ledger")
                    .await
                    .context("create database")?;
                c.query_drop("USE ledger").await.context("use ledger")?;
                break c;
            }
            Err(e) if Instant::now() < connect_until => {
                eprintln!("waiting for TiDB: {e}");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(e) => return Err(e.into()),
        }
    };
    let monitor_id: u64 = monitor.query_first("SELECT CONNECTION_ID()").await?.ok_or_else(|| anyhow!("monitor connection id query returned no row"))?;
    monitor
        .query_drop("SET GLOBAL tidb_scatter_region='table'")
        .await
        .context("enable region scatter")?;
    monitor.query_drop("CREATE TABLE IF NOT EXISTS accounts(shard INT NOT NULL,account_id BINARY(16) NOT NULL,balance BIGINT NOT NULL DEFAULT 0,PRIMARY KEY(shard,account_id) CLUSTERED)").await?;
    monitor.query_drop("CREATE TABLE IF NOT EXISTS ledger(shard INT NOT NULL,account_id BINARY(16) NOT NULL,transaction_id BINARY(16) NOT NULL,amount BIGINT NOT NULL,before_balance BIGINT NOT NULL,after_balance BIGINT NOT NULL,PRIMARY KEY(shard,account_id,transaction_id) CLUSTERED,FOREIGN KEY(shard,account_id) REFERENCES accounts(shard,account_id))").await?;
    let accounts_split = (1..=255)
        .map(|k| format!("({k},x'00000000000000000000000000000000')"))
        .collect::<Vec<_>>()
        .join(",");
    let ledger_split = (1..=255)
        .map(|k| {
            format!("({k},x'00000000000000000000000000000000',x'00000000000000000000000000000000')")
        })
        .collect::<Vec<_>>()
        .join(",");
    monitor
        .query_drop(format!("SPLIT TABLE accounts BY {accounts_split}"))
        .await
        .context("split accounts")?;
    monitor
        .query_drop(format!("SPLIT TABLE ledger BY {ledger_split}"))
        .await
        .context("split ledger")?;
    for start in (0..args.users).step_by(5000) {
        let end = (start + 5000).min(args.users);
        let vals = (start..end)
            .map(|u| {
                format!(
                    "({},x'{}',0)",
                    u as usize % args.queues,
                    hex_uuid(account_uuid(u))
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        monitor
            .query_drop(format!(
                "INSERT INTO accounts(shard,account_id,balance) VALUES {vals}"
            ))
            .await
            .context("bulk account insert")?;
    }
    let version: String = monitor
        .query_first("SELECT version()")
        .await?
        .unwrap_or_default();
    let mut workloads = Vec::with_capacity(args.queues);
    for (qi, base) in build_queues(args.users, args.requests_per_user, args.queues)
        .into_iter()
        .enumerate()
    {
        workloads.push(build_workload(
            base,
            args.batch_max,
            args.retry_pct,
            args.inject_rejects,
            args.users,
            qi,
        )?);
    }
    let topology = setup_gates(&mut monitor, &args, &workloads).await?;
    let merged = merge_workloads(&workloads, connections);
    let mut data = Vec::with_capacity(connections);
    for _ in 0..connections {
        let mut c = connect(&args.mysql_url).await?;
        c.query_drop("SELECT 1").await?;
        data.push(c);
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
    for (work, client) in merged.into_iter().zip(data) {
        let c = counters.clone();
        let td = Duration::from_millis(args.request_timeout_ms);
        set.spawn(async move { worker(work, client, c, td).await });
    }
    let mut histogram = LatencyHistogram::default();
    let mut batch_sizes = Vec::new();
    let mut workers_left = connections;
    while workers_left > 0 {
        tokio::select! { joined=set.join_next()=>match joined {Some(Ok(Ok((h,sizes))))=>{histogram.merge(&h)?;batch_sizes.extend(sizes);workers_left-=1},Some(Ok(Err(e)))=>{set.abort_all();while set.join_next().await.is_some(){}return Err(e)},Some(Err(e))=>{set.abort_all();while set.join_next().await.is_some(){}return Err(anyhow!(e))},None=>break}, _=ticker.tick()=>{sample_row(&mut monitor,monitor_id,&mut csv,&counters,&mut sample_at,&mut prev_req,&mut prev_stmt,second).await?;second+=1;} }
    }
    let measurement_end = unix_ns();
    let duration = start.elapsed().as_secs_f64();
    sample_row(
        &mut monitor,
        monitor_id,
        &mut csv,
        &counters,
        &mut sample_at,
        &mut prev_req,
        &mut prev_stmt,
        second,
    )
    .await?;
    csv.flush()?;
    let k = i64::from(args.requests_per_user);
    let row:Row=monitor.query_first(format!("SELECT count(*),COALESCE(sum(balance),0),COALESCE(sum(bad),0) FROM (SELECT a.balance,((a.balance<>{k}) OR (c.n IS NULL) OR (c.n<>{k}) OR c.min_before<>0 OR c.max_after<>a.balance OR c.dist_before<>c.n OR c.amount_bad>0) AS bad FROM accounts a LEFT JOIN (SELECT shard,account_id,count(*) n,min(before_balance) min_before,max(after_balance) max_after,count(DISTINCT before_balance) dist_before,sum(after_balance<>before_balance+amount) amount_bad FROM ledger GROUP BY shard,account_id) c USING(shard,account_id)) t")).await?.ok_or_else(||anyhow!("verification returned no row"))?;
    let accounts: u64 = row.get::<u64, _>(0).unwrap_or(0);
    let sum: i64 = row.get::<i64, _>(1).unwrap_or(0);
    let bad_accounts: u64 = row.get::<u64, _>(2).unwrap_or(0);
    let ledger_rows: i64 = monitor
        .query_first("SELECT count(*) FROM ledger")
        .await?
        .unwrap_or(0);
    let applied = counters.applied.load(Ordering::Relaxed);
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
        && bad_accounts == 0
        && sum == applied as i64
        && ledger_rows == total as i64;
    let verification = Verification {
        accounts,
        bad_accounts,
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
            mysql_url: args.mysql_url.clone(),
            stores: args.stores,
            users: args.users,
            requests_per_user: args.requests_per_user,
            queues: args.queues,
            connections,
            batch_max: args.batch_max,
            request_timeout_ms: args.request_timeout_ms,
            sample_interval_ms: args.sample_interval_ms,
            tidb_version: version,
            retry_pct: args.retry_pct,
            inject_rejects: args.inject_rejects,
            topology,
        },
        verification,
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
        round_trips: load(&c.round_trips),
        sql_statements: load(&c.sql_statements),
        retries_sent: load(&c.retries_sent),
        injected_unknown: load(&c.injected_unknown),
        injected_invalid: load(&c.injected_invalid),
        sent_requests: load(&c.sent_requests),
        windows: load(&c.windows),
        max_window_rows: load(&c.max_window_rows),
    }
}
fn hex_uuid(id: Uuid) -> String {
    id.as_bytes().iter().map(|b| format!("{b:02x}")).collect()
}
fn decode_uuid(bytes: Vec<u8>) -> Result<Uuid> {
    if bytes.len() != 16 {
        bail!("BINARY(16) UUID has length {}", bytes.len());
    }
    Ok(Uuid::from_slice(&bytes)?)
}
fn q1_sql(shard: usize, requests: &[Request]) -> String {
    let mut seen = HashSet::new();
    let keys = requests
        .iter()
        .map(|r| account_uuid(r.user))
        .filter(|a| seen.insert(*a))
        .map(|a| format!("x'{}'", hex_uuid(a)))
        .collect::<Vec<_>>()
        .join(",");
    format!("SELECT account_id,balance FROM accounts WHERE shard={shard} AND account_id IN ({keys}) FOR UPDATE")
}
fn q2_sql(shard: usize, requests: &[Request]) -> String {
    let keys = requests
        .iter()
        .map(|r| {
            format!(
                "({shard},x'{a}',x'{t}')",
                a = hex_uuid(account_uuid(r.user)),
                t = hex_uuid(r.tx)
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("SELECT account_id,transaction_id,amount,before_balance,after_balance FROM ledger WHERE (shard,account_id,transaction_id) IN ({keys})")
}
fn packet1_sql(shard: usize, requests: &[Request]) -> String {
    format!(
        "BEGIN PESSIMISTIC; {}; {};",
        q1_sql(shard, requests),
        q2_sql(shard, requests)
    )
}
fn packet2_sql(shard: usize, records: &[(Request, Record)]) -> String {
    let applied = records
        .iter()
        .filter(|(_, r)| r.outcome == Outcome::Applied)
        .collect::<Vec<_>>();
    if applied.is_empty() {
        return "COMMIT;".into();
    }
    let ledger = applied
        .iter()
        .map(|(r, x)| {
            format!(
                "({shard},x'{a}',x'{t}',{amt},{before},{after})",
                a = hex_uuid(account_uuid(r.user)),
                t = hex_uuid(r.tx),
                amt = x.amount.unwrap(),
                before = x.before.unwrap(),
                after = x.after.unwrap()
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    let mut balances: HashMap<Uuid, (u32, i64)> = HashMap::new();
    for (r, x) in applied {
        balances.insert(account_uuid(r.user), (r.user, x.after.unwrap()));
    }
    let mut vals = balances
        .values()
        .map(|(user, b)| format!("({shard},x'{a}',{b})", a = hex_uuid(account_uuid(*user))))
        .collect::<Vec<_>>();
    vals.sort();
    format!("INSERT INTO ledger(shard,account_id,transaction_id,amount,before_balance,after_balance) VALUES {ledger}; INSERT INTO accounts(shard,account_id,balance) VALUES {} ON DUPLICATE KEY UPDATE balance=VALUES(balance); COMMIT;",vals.join(","))
}
fn classify_and_compute(
    canonical: &[Request],
    balances: &HashMap<Uuid, i64>,
    existing: &HashMap<Key, Record>,
) -> Vec<Record> {
    let mut running: HashMap<Uuid, i64> = HashMap::new();
    canonical
        .iter()
        .map(|r| {
            let account = account_uuid(r.user);
            let key = Key { account, tx: r.tx };
            if let Some(x) = existing.get(&key) {
                return x.clone();
            }
            if r.amount <= 0 {
                return Record {
                    outcome: Outcome::RejectedInvalidAmount,
                    amount: Some(r.amount),
                    before: None,
                    after: None,
                };
            }
            let Some(start) = balances.get(&account) else {
                return Record {
                    outcome: Outcome::RejectedUnknownAccount,
                    amount: Some(r.amount),
                    before: None,
                    after: None,
                };
            };
            let before = *start + running.get(&account).copied().unwrap_or(0);
            let after = before + r.amount;
            running.insert(account, before - *start + r.amount);
            Record {
                outcome: Outcome::Applied,
                amount: Some(r.amount),
                before: Some(before),
                after: Some(after),
            }
        })
        .collect()
}
async fn worker(
    work: Workload,
    mut conn: Conn,
    counters: Arc<Counters>,
    timeout_d: Duration,
) -> Result<(LatencyHistogram, Vec<usize>)> {
    let mut histogram = LatencyHistogram::default();
    let mut sizes = Vec::new();
    let mut first_applied: HashMap<Key, Record> = HashMap::new();
    for (window, shard) in work.windows.into_iter().zip(work.shards) {
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
        for i in &mut d.fanout {
            *i = remap[*i];
        }
        let started = Instant::now();
        let response = timeout(timeout_d, async {
            let sql = packet1_sql(shard, &d.canonical);
            let mut result = conn.query_iter(sql).await?;
            let _: Vec<Row> = result.collect().await?; // BEGIN OK result
            let q1rows: Vec<Row> = result.collect().await?;
            let q2rows: Vec<Row> = result.collect().await?;
            result.drop_result().await?;
            counters.round_trips.fetch_add(1, Ordering::Relaxed);
            counters.sql_statements.fetch_add(3, Ordering::Relaxed);
            let mut balances = HashMap::new();
            for row in q1rows {
                let id = decode_uuid(
                    row.get::<Vec<u8>, _>(0)
                        .ok_or_else(|| anyhow!("bad account id"))?,
                )?;
                let bal = row.get::<i64, _>(1).ok_or_else(|| anyhow!("bad balance"))?;
                balances.insert(id, bal);
            }
            let mut existing = HashMap::new();
            for row in q2rows {
                let account = decode_uuid(
                    row.get::<Vec<u8>, _>(0)
                        .ok_or_else(|| anyhow!("bad ledger account"))?,
                )?;
                let tx = decode_uuid(
                    row.get::<Vec<u8>, _>(1)
                        .ok_or_else(|| anyhow!("bad transaction id"))?,
                )?;
                existing.insert(
                    Key { account, tx },
                    Record {
                        outcome: Outcome::Existing,
                        amount: row.get(2),
                        before: row.get(3),
                        after: row.get(4),
                    },
                );
            }
            let records = classify_and_compute(&d.canonical, &balances, &existing);
            let pairs = d
                .canonical
                .iter()
                .cloned()
                .zip(records.iter().cloned())
                .collect::<Vec<_>>();
            let packet = packet2_sql(shard, &pairs);
            let applied = records
                .iter()
                .filter(|r| r.outcome == Outcome::Applied)
                .count();
            conn.query_drop(packet).await?;
            counters.round_trips.fetch_add(1, Ordering::Relaxed);
            counters
                .sql_statements
                .fetch_add(if applied > 0 { 3 } else { 1 }, Ordering::Relaxed);
            Ok::<_, anyhow::Error>((records, balances))
        })
        .await;
        counters.busy.fetch_sub(1, Ordering::Relaxed);
        counters.inflight.fetch_sub(n, Ordering::Relaxed);
        let (records, _balances) = match response {
            Err(_) => bail!("30-second transaction timeout; drop connection"),
            Ok(Ok(value)) => value,
            Ok(Err(error)) => {
                let _ = timeout(Duration::from_secs(5), conn.query_drop("ROLLBACK")).await;
                return Err(error).context("TiDB transaction");
            }
        };
        let us = started.elapsed().as_micros().max(1).min(u64::MAX as u128) as u64;
        let returned = d
            .canonical
            .iter()
            .zip(records)
            .map(|(r, row)| {
                (
                    Key {
                        account: account_uuid(r.user),
                        tx: r.tx,
                    },
                    row,
                )
            })
            .collect();
        let (records, missing, unexpected) = map_outcomes(&d.canonical, returned)?;
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
        let mut seen = HashSet::new();
        for (_request, idx) in window.iter().zip(d.fanout.iter()) {
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
        }
        for _ in 0..window.len() {
            histogram.record_us(us)?;
        }
        counters.completed.fetch_add(n, Ordering::Relaxed);
    }
    Ok((histogram, sizes))
}
#[tokio::main]
async fn main() {
    let args = Args::parse();
    let output = args.output_dir.clone();
    if let Err(e) = run(args).await {
        eprintln!("txn_batch_tidb failed: {e:#}");
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
    #[test]
    fn classify_precedence_running_balances_and_unknowns() {
        let old = req(0, 10, 99);
        let r1 = req(0, 11, 2);
        let r2 = req(0, 12, 3);
        let invalid = req(0, 13, 0);
        let unknown = req(4, 14, 1);
        let existing = HashMap::from([(
            Key {
                account: account_uuid(0),
                tx: old.tx,
            },
            Record {
                outcome: Outcome::Existing,
                amount: Some(1),
                before: Some(8),
                after: Some(9),
            },
        )]);
        let got = classify_and_compute(
            &[old, r1, r2, invalid, unknown],
            &HashMap::from([(account_uuid(0), 10)]),
            &existing,
        );
        assert_eq!(
            got[0],
            Record {
                outcome: Outcome::Existing,
                amount: Some(1),
                before: Some(8),
                after: Some(9)
            }
        );
        assert_eq!((got[1].before, got[1].after), (Some(10), Some(12)));
        assert_eq!((got[2].before, got[2].after), (Some(12), Some(15)));
        assert_eq!(got[3].outcome, Outcome::RejectedInvalidAmount);
        assert_eq!(got[4].outcome, Outcome::RejectedUnknownAccount);
        assert!(got[4].before.is_none());
    }
    #[test]
    fn store_balance_gate_checks_both_shares_and_count() {
        assert!(store_balance_ok(&[(1,50,50),(2,50,50)],2));
        assert!(!store_balance_ok(&[(1,80,50),(2,20,50)],2));
        assert!(!store_balance_ok(&[(1,50,50)],2));
    }
    #[test]
    fn plan_gate_requires_batch_lookup_and_no_full_scan() {
        assert!(plan_ok("Batch_Point_Get"));
        assert!(!plan_ok("TableFullScan Batch_Point_Get"));
        assert!(!plan_ok("Point_Get"));
    }
    #[test]
    fn sql_builders_encode_hex_and_tuple_shapes_and_queue_shard() {
        let rows = vec![req(7, 0x1234, 2), req(8, 0x5678, 3)];
        let p1 = packet1_sql(9, &rows);
        assert!(p1.starts_with(
            "BEGIN PESSIMISTIC; SELECT account_id,balance FROM accounts WHERE shard=9"
        ));
        assert!(p1.contains(&format!("x'{}'", hex_uuid(account_uuid(7)))));
        assert!(p1.matches("SELECT ").count() == 2);
        assert!(p1.contains("(9,x'"));
        assert_eq!(p1.matches("FOR UPDATE").count(), 1);
        let applied = vec![
            (
                rows[0].clone(),
                Record {
                    outcome: Outcome::Applied,
                    amount: Some(2),
                    before: Some(0),
                    after: Some(2),
                },
            ),
            (
                rows[1].clone(),
                Record {
                    outcome: Outcome::Applied,
                    amount: Some(3),
                    before: Some(0),
                    after: Some(3),
                },
            ),
        ];
        let p2 = packet2_sql(9, &applied);
        assert!(p2.starts_with("INSERT INTO ledger"));
        assert!(p2.contains("VALUES (9,x'"));
        assert_eq!(p2.matches("),(").count(), 2);
        assert!(p2.contains("ON DUPLICATE KEY UPDATE"));
        assert!(p2.ends_with("COMMIT;"));
        assert_eq!(packet2_sql(9, &[]), "COMMIT;");
        assert!(q1_sql(9, &rows).contains("shard=9"));
        assert!(q2_sql(9, &rows).contains("(9,x'"));
        assert_eq!(7usize % 4, 3); // account 7 belongs to queue/shard 3.
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
            assert_eq!(actual.shards, original.shards);
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
                    .zip(&merged[q % connections].shards)
                    .filter(|(_, shard)| **shard == q)
                    .map(|(window, _)| window.clone()).collect();
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
                for (window, shard) in work.windows.iter().zip(&work.shards) {
                    assert_eq!(*shard % connections, connection);
                    assert!(window.iter().all(|r| r.user as usize % 256 == *shard));
                }
            }
        }
    }

    #[test]
    fn every_merged_window_carries_its_original_queue_shard() {
        let workloads = test_workloads(1000, 40, 256, 50);
        for connections in [1, 4, 256] {
            for work in merge_workloads(&workloads, connections) {
                for (window, shard) in work.windows.iter().zip(&work.shards) {
                    assert!(window.iter().all(|r| r.user as usize % 256 == *shard));
                }
            }
        }
    }

    #[test]
    fn merged_window_sql_uses_its_shard_not_connection_index() {
        let workloads = test_workloads(1000, 40, 256, 50);
        let merged = merge_workloads(&workloads, 4);
        let (window, shard) = merged[3].windows.iter().zip(&merged[3].shards)
            .find(|(_, shard)| **shard == 7).unwrap();
        assert_ne!(*shard, 3);
        let packet1 = packet1_sql(*shard, window);
        assert!(packet1.contains("shard=7"));
        assert!(!packet1.contains("shard=3"));
        let records = window.iter().cloned().map(|request| (request, Record {
            outcome: Outcome::Applied,
            amount: Some(1),
            before: Some(0),
            after: Some(1),
        })).collect::<Vec<_>>();
        let packet2 = packet2_sql(*shard, &records);
        assert!(packet2.contains("VALUES (7,x'"));
        assert!(!packet2.contains("(3,x'"));
    }
}
