use ledger_wallet::{
    DatabaseLayout, HistoryStore, MockHistory, Pipeline, PipelineConfig, PipelineError,
    TransactionReceipt, TransactionStatus,
};
use std::io::{self, BufRead};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::runtime::Builder;
use tokio::sync::mpsc;

fn main() {
    let runtime = Builder::new_multi_thread()
        .worker_threads(4)
        .max_blocking_threads(32)
        .enable_all()
        .build()
        .unwrap_or_else(|error| {
            eprintln!("ledger-wallet: cannot create Tokio runtime: {error}");
            std::process::exit(2);
        });
    if let Err(error) = runtime.block_on(run()) {
        eprintln!("ledger-wallet: {error}");
        std::process::exit(2);
    }
}

async fn run() -> Result<(), String> {
    let config = parse_args(std::env::args().skip(1))?;
    let histories: Vec<Arc<dyn HistoryStore>> = (0..config.shard_count)
        .map(|shard| Arc::new(MockHistory::new(format!("cli-volatile-shard-{shard}"))) as _)
        .collect();
    let pipeline = Pipeline::open(config, histories)
        .await
        .map_err(|error| format!("startup failed: {error}"))?;
    print_help();

    let (line_sender, mut lines) = mpsc::channel::<io::Result<String>>(128);
    if let Err(error) = std::thread::Builder::new()
        .name("ledger-wallet-stdin".to_owned())
        .spawn(move || {
            let stdin = io::stdin();
            let mut reader = stdin.lock();
            loop {
                let mut line = String::new();
                match reader.read_line(&mut line) {
                    Ok(0) => break,
                    Ok(_) => {
                        if line_sender.blocking_send(Ok(line)).is_err() {
                            break;
                        }
                    }
                    Err(error) => {
                        let _ = line_sender.blocking_send(Err(error));
                        break;
                    }
                }
            }
        })
    {
        let _ = pipeline.shutdown().await;
        return Err(format!("cannot start stdin reader: {error}"));
    }

    loop {
        tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                match signal {
                    Ok(()) => println!("shutdown requested by SIGINT"),
                    Err(error) => eprintln!("cannot listen for SIGINT: {error}; shutting down"),
                }
                break;
            }
            line = lines.recv() => {
                let Some(line) = line else {
                    println!("stdin reached EOF; shutting down");
                    break;
                };
                match line {
                    Err(error) => {
                        eprintln!("stdin read failed: {error}; shutting down");
                        break;
                    }
                    Ok(line) => match dispatch_line(&pipeline, &line).await {
                        Ok(true) => break,
                        Ok(false) => {},
                        Err(error) => println!("ERR {error}"),
                    },
                }
            }
        }
    }

    pipeline
        .shutdown()
        .await
        .map_err(|error| format!("graceful shutdown failed: {error}"))?;
    Ok(())
}

async fn dispatch_line(pipeline: &Pipeline, line: &str) -> Result<bool, String> {
    let fields: Vec<_> = line.split_whitespace().collect();
    if fields.is_empty() {
        return Ok(false);
    }
    match fields[0] {
        "help" if fields.len() == 1 => print_help(),
        "shutdown" if fields.len() == 1 => {
            println!("shutdown requested");
            return Ok(true);
        }
        "credit" if fields.len() == 5 => {
            let receipt = pipeline
                .credit(
                    number(fields[1], "account_id")?,
                    number(fields[2], "tx_id")?,
                    number(fields[3], "tx_at_us")?,
                    number(fields[4], "amount")?,
                )
                .await;
            print_transaction(receipt);
        }
        "debit" if fields.len() == 5 => {
            let receipt = pipeline
                .debit(
                    number(fields[1], "account_id")?,
                    number(fields[2], "tx_id")?,
                    number(fields[3], "tx_at_us")?,
                    number(fields[4], "amount")?,
                )
                .await;
            print_transaction(receipt);
        }
        "refund" if fields.len() == 7 => {
            let receipt = pipeline
                .refund(
                    number(fields[1], "account_id")?,
                    number(fields[2], "tx_id")?,
                    number(fields[3], "tx_at_us")?,
                    number(fields[4], "amount")?,
                    number(fields[5], "debit_tx_id")?,
                    number(fields[6], "debit_tx_at_us")?,
                )
                .await;
            print_transaction(receipt);
        }
        "balance" if fields.len() == 2 => {
            match pipeline.balance(number(fields[1], "account_id")?).await {
                Ok(receipt) => println!(
                    "OK balance shard={} account={} minor_units={} sequence={}",
                    receipt.shard_id, receipt.account_id, receipt.balance, receipt.sequence
                ),
                Err(error) => println!("ERR {error}"),
            }
        }
        "status" if fields.len() == 1 => match pipeline.status().await {
            Ok(status) => print_status(status),
            Err(error) => println!("ERR status: {error}"),
        },
        _ => println!("ERR unknown command or wrong argument count; type help"),
    }
    Ok(false)
}

fn print_transaction(result: Result<TransactionReceipt, PipelineError>) {
    match result {
        Ok(receipt) => println!(
            "OK shard={} sequence={} status={} balance={} replayed={} historical_replay={}",
            receipt.shard_id,
            receipt.sequence,
            status_name(receipt.status),
            receipt.balance,
            receipt.replayed,
            receipt.historical_replay
        ),
        Err(error) => println!("ERR {error}"),
    }
}

fn status_name(status: TransactionStatus) -> &'static str {
    match status {
        TransactionStatus::Applied => "applied",
        TransactionStatus::InsufficientFunds => "insufficient_funds",
        TransactionStatus::CreditOverflow => "credit_overflow",
        TransactionStatus::InvalidAmount => "invalid_amount",
        TransactionStatus::InvalidRefund => "invalid_refund",
        TransactionStatus::RefundAlreadyUsed => "refund_already_used",
    }
}

fn print_status(status: ledger_wallet::PipelineStatus) {
    let cpu = process_cpu_seconds().unwrap_or(f64::NAN);
    let io = process_io().unwrap_or_default();
    println!(
        "status lifecycle={} service={} data_dir={} shards={} layout={} in_flight={}/{} queue_capacity={} rps_1s={:.2} rps_10s={:.2} rps_60s={:.2}",
        status.lifecycle.as_str(),
        status.service_identity,
        status.data_dir.display(),
        status.shard_count,
        status.database_layout.as_str(),
        status.in_flight,
        status.in_flight_limit,
        status.global_queue_capacity,
        status.metrics.requests_per_second_1s,
        status.metrics.requests_per_second_10s,
        status.metrics.requests_per_second_60s,
    );
    println!(
        "process_scope cpu_seconds={cpu:.3} io_read_bytes={} io_write_bytes={} note=process-wide CPU and I/O include Tokio and RocksDB work; stage durations overlap",
        io.read_bytes, io.write_bytes
    );
    for shard in &status.shards {
        println!(
            "shard id={} source={} source_seq={} projected_seq={} history_seq={} watermark={} gc_prefix={} queue={}/{} history={:?}:{} health={}",
            shard.shard_id,
            shard.source_identity,
            shard.source_sequence,
            shard.projected_sequence,
            shard.history_sequence,
            shard.watermark,
            shard.gc_prefix_sequence,
            shard.queued_requests,
            shard.queue_capacity,
            shard.history_durability,
            shard.history_identity,
            if shard.health.is_empty() {
                "ok".to_owned()
            } else {
                shard.health.join(" | ")
            }
        );
    }
    for stage in &status.metrics.stages {
        if stage.count == 0 {
            continue;
        }
        println!(
            "stage={} count={} sample={} average_ns={} p50_ns={} p95_ns={} p99_ns={} max_ns={}",
            stage.stage,
            stage.count,
            stage.sample_count,
            stage.average_ns,
            optional_number(stage.p50_ns),
            optional_number(stage.p95_ns),
            optional_number(stage.p99_ns),
            optional_number(stage.max_ns),
        );
    }
}

fn optional_number(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "n/a".to_owned())
}

#[derive(Default)]
struct IoCounters {
    read_bytes: u64,
    write_bytes: u64,
}

fn process_io() -> io::Result<IoCounters> {
    let contents = std::fs::read_to_string("/proc/self/io")?;
    let mut counters = IoCounters::default();
    for line in contents.lines() {
        if let Some(value) = line.strip_prefix("read_bytes:") {
            counters.read_bytes = value.trim().parse().unwrap_or_default();
        } else if let Some(value) = line.strip_prefix("write_bytes:") {
            counters.write_bytes = value.trim().parse().unwrap_or_default();
        }
    }
    Ok(counters)
}

fn process_cpu_seconds() -> io::Result<f64> {
    #[cfg(target_os = "linux")]
    {
        let mut value = std::mem::MaybeUninit::<libc::timespec>::uninit();
        // SAFETY: clock_gettime writes one timespec into the provided pointer.
        let result =
            unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, value.as_mut_ptr()) };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }
        let value = unsafe { value.assume_init() };
        Ok(value.tv_sec as f64 + value.tv_nsec as f64 / 1_000_000_000.0)
    }
    #[cfg(not(target_os = "linux"))]
    {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "process CPU metrics are currently implemented on Linux",
        ))
    }
}

fn number(value: &str, field: &str) -> Result<u64, String> {
    value
        .parse::<u64>()
        .map_err(|_| format!("{field} must be an unsigned integer"))
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<PipelineConfig, String> {
    let mut args = args.into_iter();
    let mut config = PipelineConfig::default();
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            "--data-dir" => config.data_dir = PathBuf::from(next_arg(&mut args, &argument)?),
            "--shards" => {
                config.shard_count = next_arg(&mut args, &argument)?
                    .parse()
                    .map_err(|_| "--shards must be an integer".to_owned())?;
            }
            "--db-layout" => {
                config.database_layout = match next_arg(&mut args, &argument)?.as_str() {
                    "shared" => DatabaseLayout::Shared,
                    "dedicated" => DatabaseLayout::Dedicated,
                    other => return Err(format!("unsupported database layout {other:?}")),
                };
            }
            "--min-free-disk-bytes" => {
                config.startup_min_free_disk_bytes = next_arg(&mut args, &argument)?
                    .parse()
                    .map_err(|_| "--min-free-disk-bytes must be an integer".to_owned())?;
            }
            "--min-available-memory-bytes" => {
                config.startup_min_available_memory_bytes = next_arg(&mut args, &argument)?
                    .parse()
                    .map_err(|_| "--min-available-memory-bytes must be an integer".to_owned())?;
            }
            other => return Err(format!("unknown argument {other:?}; use --help")),
        }
    }
    config.validate().map_err(|error| error.to_string())?;
    Ok(config)
}

fn next_arg<I>(args: &mut I, option: &str) -> Result<String, String>
where
    I: Iterator<Item = String>,
{
    args.next()
        .ok_or_else(|| format!("{option} requires a value"))
}

fn print_help() {
    println!(
        "ledger-wallet [--data-dir PATH] [--shards N] [--db-layout shared|dedicated] [--min-free-disk-bytes N] [--min-available-memory-bytes N]\n\
         stdin commands:\n\
         credit ACCOUNT TX_ID TX_AT_US AMOUNT\n\
         debit ACCOUNT TX_ID TX_AT_US AMOUNT\n\
         refund ACCOUNT TX_ID TX_AT_US AMOUNT DEBIT_TX_ID DEBIT_TX_AT_US\n\
         balance ACCOUNT\n\
         status\n\
         shutdown\n\
         help\n\
         Amounts are unsigned minor units; tx_at_us is immutable Unix microseconds."
    );
}
