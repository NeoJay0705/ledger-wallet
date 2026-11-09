use std::{
    fs,
    path::Path,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use ledger_bench::{
    backend::BackendClient,
    config::{Args, Backend},
    metrics::{new_business_id, Metrics, PerSecondSample, RunCounters, RunResult, UserMetrics},
};
use tokio::{sync::Barrier, task::JoinSet};

#[derive(Clone, Debug)]
struct Sample {
    elapsed: Duration,
    interval: Duration,
    completed: u64,
    active_users: u64,
}

struct EngineRun {
    users: Vec<UserMetrics>,
    samples: Vec<Sample>,
    elapsed: Duration,
    completed: u64,
    condition_retries: u64,
    measurement_start_unix_ns: u128,
}

fn micros(d: Duration) -> u64 {
    d.as_micros().clamp(1, u128::from(u64::MAX)) as u64
}

async fn execute<F, Fut>(users: u32, per_user: u64, op: F) -> Result<EngineRun>
where
    F: Fn(u32, u64) -> Fut + Clone + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<ledger_bench::backend::OperationTiming>>
        + Send
        + 'static,
{
    execute_with_prepare(users, per_user, |_| async {}, op).await
}

async fn execute_with_prepare<P, PrepFut, F, Fut>(
    users: u32,
    per_user: u64,
    prepare: P,
    op: F,
) -> Result<EngineRun>
where
    P: Fn(u32) -> PrepFut + Clone + Send + Sync + 'static,
    PrepFut: std::future::Future<Output = ()> + Send + 'static,
    F: Fn(u32, u64) -> Fut + Clone + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<ledger_bench::backend::OperationTiming>>
        + Send
        + 'static,
{
    let expected = u64::from(users)
        .checked_mul(per_user)
        .ok_or_else(|| anyhow!("expected operation count overflow"))?;
    let ready_barrier = Arc::new(Barrier::new(users as usize + 1));
    let start_barrier = Arc::new(Barrier::new(users as usize + 1));
    let counters = Arc::new(RunCounters::default());
    let retries = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let release_time = Arc::new(std::sync::Mutex::new(None::<Instant>));
    let last_operation = Arc::new(std::sync::Mutex::new(Duration::ZERO));
    let mut tasks = JoinSet::new();
    for id in 0..users {
        let ready_barrier = ready_barrier.clone();
        let start_barrier = start_barrier.clone();
        let counters = counters.clone();
        let op = op.clone();
        let prepare = prepare.clone();
        let release_time = release_time.clone();
        let last_operation = last_operation.clone();
        let retries = retries.clone();
        tasks.spawn(async move {
            let mut metrics = UserMetrics {
                user_id: id,
                ..Default::default()
            };
            prepare(id).await;
            ready_barrier.wait().await;
            start_barrier.wait().await;
            counters.user_started();
            let result = async {
                for seq in 0..per_user {
                    let started = Instant::now();
                    let timing = op(id, seq).await?;
                    retries.fetch_add(
                        u64::from(timing.condition_retries),
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    if seq + 1 == per_user {
                        if let Some(t0) = *release_time.lock().expect("release time mutex poisoned")
                        {
                            let offset = Instant::now().duration_since(t0);
                            let mut last =
                                last_operation.lock().expect("completion mutex poisoned");
                            *last = (*last).max(offset);
                        }
                    }
                    counters.operation_completed();
                    metrics.end_to_end.record_us(micros(started.elapsed()))?;
                    if timing.pg_checkout != Duration::ZERO {
                        metrics.pg_checkout.record_us(micros(timing.pg_checkout))?;
                    }
                    if timing.scylla_read != Duration::ZERO {
                        metrics.scylla_read.record_us(micros(timing.scylla_read))?;
                    }
                    if timing.scylla_write != Duration::ZERO {
                        metrics
                            .scylla_write
                            .record_us(micros(timing.scylla_write))?;
                    }
                }
                Ok::<_, anyhow::Error>(metrics)
            }
            .await;
            counters.user_finished();
            result
        });
    }
    // Exclude task setup (including histogram allocation) from the measured interval.
    ready_barrier.wait().await;
    let t0 = Instant::now();
    let measurement_start_unix_ns = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    *release_time.lock().expect("release time mutex poisoned") = Some(t0);
    start_barrier.wait().await;
    let mut next_tick = tokio::time::Instant::from_std(t0) + Duration::from_secs(1);
    let mut prior_tick = t0;
    let mut prior_completed = 0;
    let mut samples = Vec::new();
    let mut finished = Vec::with_capacity(users as usize);
    let _join_completion = loop {
        tokio::select! {
            joined = tasks.join_next() => {
                match joined {
                    Some(Ok(Ok(m))) => finished.push(m),
                    Some(Ok(Err(e))) => { tasks.abort_all(); while tasks.join_next().await.is_some() {} return Err(e.context("business operation failed")); },
                    Some(Err(e)) => { tasks.abort_all(); while tasks.join_next().await.is_some() {} return Err(anyhow!("user task failed: {e}")); },
                    None => break Instant::now(),
                }
                if finished.len() == users as usize {
                    break Instant::now();
                }
            }
            _ = tokio::time::sleep_until(next_tick) => {
                let completed = counters.completed();
                let observed = Instant::now();
                let now = if completed == expected {
                    observed.min(t0 + *last_operation.lock().expect("completion mutex poisoned"))
                } else {
                    observed
                };
                samples.push(Sample {
                    elapsed: now.duration_since(t0),
                    interval: now.duration_since(prior_tick),
                    completed: completed - prior_completed,
                    active_users: counters.active_users(),
                });
                prior_tick = now;
                prior_completed = completed;
                next_tick += Duration::from_secs(1);
            }
        }
    };
    let elapsed = *last_operation.lock().expect("completion mutex poisoned");
    let completed = counters.completed();
    let sampled_duration = prior_tick.duration_since(t0);
    if sampled_duration < elapsed || completed != prior_completed {
        samples.push(Sample {
            elapsed,
            interval: elapsed - sampled_duration,
            completed: completed - prior_completed,
            active_users: 0,
        });
    }
    if completed != expected {
        bail!("engine counted {completed} completions; expected {expected}");
    }
    let sampled_completions: u64 = samples.iter().map(|sample| sample.completed).sum();
    let sampled_duration: Duration = samples.iter().map(|sample| sample.interval).sum();
    if sampled_completions != expected || sampled_duration != elapsed {
        bail!("engine sample totals do not match measured run duration/completions");
    }
    finished.sort_by_key(|m| m.user_id);
    Ok(EngineRun {
        users: finished,
        samples,
        elapsed,
        completed,
        condition_retries: retries.load(std::sync::atomic::Ordering::Relaxed),
        measurement_start_unix_ns,
    })
}

fn write_json(path: &Path, value: &impl serde::Serialize) -> Result<()> {
    fs::write(path, serde_json::to_vec_pretty(value)?)
        .with_context(|| format!("write {}", path.display()))
}

fn write_histograms(dir: &Path, metrics: &Metrics) -> Result<()> {
    for (name, bytes) in [
        ("end_to_end", metrics.end_to_end.serialize()?),
        ("pg_checkout", metrics.pg_checkout.serialize()?),
        ("scylla_read", metrics.scylla_read.serialize()?),
        ("scylla_write", metrics.scylla_write.serialize()?),
    ] {
        fs::write(dir.join(format!("{name}.hdr")), bytes)?;
    }
    Ok(())
}

fn write_csv(path: &Path, samples: &[Sample]) -> Result<()> {
    let mut csv = String::from("actual_time_seconds,interval_seconds,completed,active_users\n");
    for s in samples {
        csv.push_str(&format!(
            "{:.9},{:.9},{},{}\n",
            s.elapsed.as_secs_f64(),
            s.interval.as_secs_f64(),
            s.completed,
            s.active_users
        ));
    }
    fs::write(path, csv).with_context(|| format!("write {}", path.display()))
}

fn main() {
    // Parse and validate before creating a runtime with the requested worker count.
    let parsed = Args::try_parse();
    let args = match parsed {
        Ok(args) => args,
        Err(e) => e.exit(),
    };
    let output_dir = args.output_dir.clone();
    let config = match args.validate() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("invalid configuration: {e}");
            if output_dir.is_dir() {
                let _ = fs::write(
                    output_dir.join("invalid.json"),
                    serde_json::to_vec_pretty(
                        &serde_json::json!({"valid":false,"error":e.to_string()}),
                    )
                    .unwrap_or_default(),
                );
            }
            std::process::exit(2);
        }
    };
    let result = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.args.client_workers)
        .enable_all()
        .build();
    match result {
        Ok(rt) => {
            if let Err(err) = rt.block_on(run_config(config)) {
                let message = format!("{err:#}");
                let _ = fs::create_dir_all(&output_dir);
                let _ = fs::write(
                    output_dir.join("invalid.json"),
                    serde_json::to_vec_pretty(&serde_json::json!({"valid":false,"error":message}))
                        .unwrap_or_default(),
                );
                let _ = fs::remove_file(output_dir.join("result.json"));
                eprintln!("run failed: {err:#}");
                std::process::exit(1);
            }
        }
        Err(e) => {
            eprintln!("could not build Tokio runtime: {e}");
            std::process::exit(1);
        }
    }
}

async fn run_config(config: ledger_bench::config::ValidatedConfig) -> Result<()> {
    // Reuse run body while keeping CLI parsing outside runtime construction.
    fs::create_dir_all(&config.args.output_dir)?;
    let phase_path = config.args.output_dir.join("phase-timestamps.json");
    let mut phases = serde_json::Map::new();
    let phase_start = Instant::now();
    let client = tokio::time::timeout(
        config.setup_timeout,
        BackendClient::connect_and_initialize(config.clone()),
    )
    .await
    .context("backend initialization setup timeout")??;
    phases.insert(
        "init_elapsed_ms".into(),
        serde_json::json!(phase_start.elapsed().as_millis()),
    );
    write_json(&phase_path, &serde_json::Value::Object(phases.clone()))?;
    let phase_start = Instant::now();
    let mut warmup_completed = 0u64;
    for user in 0..config.args.users {
        for seq in 0..config.args.warmup_requests_per_user {
            client.transact(user, new_business_id(user, seq)).await?;
            warmup_completed += 1;
        }
    }
    phases.insert(
        "warmup_elapsed_ms".into(),
        serde_json::json!(phase_start.elapsed().as_millis()),
    );
    write_json(&phase_path, &serde_json::Value::Object(phases.clone()))?;
    let phase_start = Instant::now();
    let warmup_state = client
        .verify(config.args.users, config.args.warmup_requests_per_user)
        .await?;
    phases.insert(
        "warmup_verify_elapsed_ms".into(),
        serde_json::json!(phase_start.elapsed().as_millis()),
    );
    write_json(&phase_path, &serde_json::Value::Object(phases.clone()))?;
    if !warmup_state.valid {
        bail!("warmup database verification failed");
    }
    let phase_start = Instant::now();
    client.reset_benchmark_data(config.args.users).await?;
    phases.insert(
        "reset_elapsed_ms".into(),
        serde_json::json!(phase_start.elapsed().as_millis()),
    );
    write_json(&phase_path, &serde_json::Value::Object(phases.clone()))?;
    let phase_start = Instant::now();
    let reset_state = client.verify(config.args.users, 0).await?;
    phases.insert(
        "reset_verify_elapsed_ms".into(),
        serde_json::json!(phase_start.elapsed().as_millis()),
    );
    write_json(&phase_path, &serde_json::Value::Object(phases.clone()))?;
    if !reset_state.valid {
        bail!("post-warmup reset verification failed");
    }
    write_json(
        &config.args.output_dir.join("warmup-reset.json"),
        &serde_json::json!({
            "warmup_requests_per_user": config.args.warmup_requests_per_user,
            "warmup_completed": warmup_completed,
            "warmup_state": warmup_state,
            "reset_state": reset_state,
            "reset_method": if config.args.backend == Backend::Pg { "TRUNCATE ledger; UPDATE accounts SET balance=0" } else { "TRUNCATE ledger; reinsert reserved nil-init rows" }
        }),
    )?;
    let phase_start = Instant::now();
    tokio::time::sleep(Duration::from_millis(config.args.settle_ms)).await;
    phases.insert(
        "settle_elapsed_ms".into(),
        serde_json::json!(phase_start.elapsed().as_millis()),
    );
    write_json(&phase_path, &serde_json::Value::Object(phases.clone()))?;
    if let (Some(ready), Some(go)) = (&config.args.ready_file, &config.args.go_file) {
        if let Some(parent) = ready.parent() {
            fs::create_dir_all(parent)?;
        }
        write_json(
            ready,
            &serde_json::json!({
                "state": "prepared", "backend": format!("{:?}", config.args.backend).to_lowercase(),
                "users": config.args.users, "requests_per_user": config.args.requests_per_user,
                "expected_total": config.expected_total, "diagnostics": client.diagnostics()
            }),
        )?;
        let deadline =
            tokio::time::Instant::now() + Duration::from_millis(config.args.handshake_timeout_ms);
        loop {
            if go.exists() {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                bail!(
                    "wrapper GO handshake timed out after {} ms",
                    config.args.handshake_timeout_ms
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    let c = client.clone();
    let engine = execute(
        config.args.users,
        config.args.requests_per_user,
        move |user, seq| {
            let c = c.clone();
            async move { c.transact(user, new_business_id(user, seq)).await }
        },
    )
    .await?;
    let phase_start = Instant::now();
    let verification = client
        .verify(config.args.users, config.args.requests_per_user)
        .await?;
    phases.insert(
        "final_verify_elapsed_ms".into(),
        serde_json::json!(phase_start.elapsed().as_millis()),
    );
    write_json(&phase_path, &serde_json::Value::Object(phases))?;
    write_json(
        &config.args.output_dir.join("verification.json"),
        &verification,
    )?;
    if !verification.valid {
        bail!("database verification failed; refusing accepted result");
    }
    let mut metrics = Metrics::default();
    for user in &engine.users {
        metrics.merge_user(user)?;
    }
    if metrics.end_to_end.count() != engine.completed || engine.completed != config.expected_total {
        bail!("histogram/completion mismatch");
    }
    for h in [
        &metrics.pg_checkout,
        &metrics.scylla_read,
        &metrics.scylla_write,
    ] {
        if !h.is_empty() && h.count() != engine.completed {
            bail!("diagnostic histogram count mismatch");
        }
    }
    let get = |k: &str| verification.summary.get(k).copied().unwrap_or_default();
    let validation = ledger_bench::metrics::Validation {
        accounts: get("accounts") as u64,
        expected_accounts: get("expected_accounts") as u64,
        ledger_rows: get("ledger_rows") as u64,
        expected_rows: get("expected_rows") as u64,
        amount_sum: get("amount_sum"),
        expected_sum: get("expected_sum").max(0) as u128,
    };
    if validation.accounts != validation.expected_accounts
        || validation.ledger_rows != engine.completed
        || validation.expected_rows != engine.completed
        || validation.amount_sum != validation.expected_sum as i128
    {
        bail!("verification totals do not match workload");
    }
    let per_second = engine
        .samples
        .iter()
        .enumerate()
        .map(|(i, s)| PerSecondSample {
            second: i as u64 + 1,
            completed: s.completed,
            active_users: s.active_users,
        })
        .collect();
    let mut diagnostics = client.diagnostics();
    diagnostics.insert(
        "condition_retries".into(),
        engine.condition_retries.to_string(),
    );
    let result = RunResult {
        backend: match config.args.backend {
            Backend::Pg => "pg",
            Backend::Scylla => "scylla",
        }
        .into(),
        users: config.args.users,
        requests_per_user: config.args.requests_per_user,
        expected_total: config.expected_total,
        completed: engine.completed,
        elapsed_ms: engine.elapsed.as_millis(),
        measurement_start_unix_ns: engine.measurement_start_unix_ns,
        measurement_elapsed_ns: engine.elapsed.as_nanos(),
        measurement_end_unix_ns: engine.measurement_start_unix_ns + engine.elapsed.as_nanos(),
        business_rps: engine.completed as f64 / engine.elapsed.as_secs_f64(),
        metrics: metrics.summary(),
        validation,
        per_user: engine
            .users
            .iter()
            .map(|u| {
                let m = Metrics {
                    end_to_end: u.end_to_end.clone(),
                    pg_checkout: u.pg_checkout.clone(),
                    scylla_read: u.scylla_read.clone(),
                    scylla_write: u.scylla_write.clone(),
                };
                (u.user_id, m.summary())
            })
            .collect(),
        per_second,
        diagnostics,
    };
    let dir = &config.args.output_dir;
    write_histograms(dir, &metrics)?;
    write_csv(&dir.join("per_second.csv"), &engine.samples)?;
    write_json(&dir.join("result.json"), &result)?;
    let _ = fs::remove_file(dir.join("invalid.json"));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ledger_bench::backend::OperationTiming;

    #[tokio::test]
    async fn duration_is_not_rounded_and_final_sample_is_fractional() {
        let run = execute(2, 1, |_, _| async {
            tokio::time::sleep(Duration::from_millis(25)).await;
            Ok(OperationTiming::default())
        })
        .await
        .unwrap();
        assert!(run.elapsed < Duration::from_secs(1));
        assert_eq!(run.completed, 2);
        assert_eq!(run.samples.len(), 1);
        assert!(run.samples[0].interval < Duration::from_secs(1));
        assert_eq!(run.samples[0].active_users, 0);
    }

    #[tokio::test]
    async fn preparation_delay_is_excluded_and_sample_totals_match() {
        let setup_delay = Duration::from_millis(250);
        let run = execute_with_prepare(
            2,
            1,
            move |user| async move {
                if user == 1 {
                    tokio::time::sleep(setup_delay).await;
                }
            },
            |_, _| async {
                tokio::time::sleep(Duration::from_millis(20)).await;
                Ok(OperationTiming::default())
            },
        )
        .await
        .unwrap();

        assert!(
            run.elapsed < Duration::from_millis(150),
            "setup delay leaked into elapsed: {:?}",
            run.elapsed
        );
        assert_eq!(
            run.samples
                .iter()
                .map(|sample| sample.completed)
                .sum::<u64>(),
            run.completed
        );
        assert_eq!(
            run.samples
                .iter()
                .map(|sample| sample.interval)
                .sum::<Duration>(),
            run.elapsed
        );
        assert!(run
            .samples
            .iter()
            .all(|sample| sample.elapsed <= run.elapsed));
    }

    #[tokio::test]
    async fn interval_reports_uneven_active_user_counts() {
        let run = execute(2, 1, |user, _| async move {
            if user == 1 {
                tokio::time::sleep(Duration::from_millis(1_150)).await;
            }
            Ok(OperationTiming::default())
        })
        .await
        .unwrap();
        assert!(run.samples.len() >= 2);
        assert_eq!(run.samples[0].active_users, 1);
        assert_eq!(run.samples.last().unwrap().active_users, 0);
    }

    #[tokio::test]
    async fn failure_aborts_other_users_promptly() {
        let run = tokio::time::timeout(
            Duration::from_millis(100),
            execute(3, 2, |user, _| async move {
                if user == 0 {
                    anyhow::bail!("expected failure");
                }
                tokio::time::sleep(Duration::from_secs(60)).await;
                Ok(OperationTiming::default())
            }),
        )
        .await
        .unwrap();
        assert!(run.is_err());
    }
}
