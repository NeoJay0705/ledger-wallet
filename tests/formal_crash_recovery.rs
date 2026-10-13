#![cfg(target_os = "linux")]

use ledger_wallet::{
    DatabaseLayout, HistoryStore, MockHistory, Operation, Pipeline, PipelineConfig, Transaction,
    TransactionKey, TransactionStatus,
};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const CHILD_MODE_ENV: &str = "LEDGER_WALLET_FORMAL_CRASH_CHILD";
const DATA_DIR_ENV: &str = "LEDGER_WALLET_FORMAL_CRASH_DATA_DIR";
const DURABLE_MARKER: &str = "FORMAL_DURABLE_BEFORE_EXTERNAL_REPLY";
const CHILD_MARKER_TIMEOUT: Duration = Duration::from_secs(30);

struct OwnedTempDir(PathBuf);

impl OwnedTempDir {
    fn new() -> io::Result<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let target = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target");
        std::fs::create_dir_all(&target)?;
        let path = target.join(format!(
            "ledger-wallet-formal-crash-{}-{nonce}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path)?;
        Ok(Self(path))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for OwnedTempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct ChildGuard(Child);

impl ChildGuard {
    fn kill_and_wait(&mut self) -> io::Result<ExitStatus> {
        if self.0.try_wait()?.is_none() {
            self.0.kill()?;
        }
        self.0.wait()
    }

    fn wait_or_kill(&mut self, timeout: Duration) -> io::Result<(ExitStatus, bool)> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if let Some(status) = self.0.try_wait()? {
                return Ok((status, false));
            }
            if std::time::Instant::now() >= deadline {
                self.0.kill()?;
                return self.0.wait().map(|status| (status, true));
            }
            thread::sleep(Duration::from_millis(20));
        }
    }

    fn wait_for_output_or_exit(
        &mut self,
        output: &OutputCapture,
        needle: &[u8],
        timeout: Duration,
    ) -> io::Result<bool> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if output.contains(needle) {
                return Ok(true);
            }
            if self.0.try_wait()?.is_some() {
                return Ok(false);
            }
            if std::time::Instant::now() >= deadline {
                return Ok(false);
            }
            thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if matches!(self.0.try_wait(), Ok(None)) {
            let _ = self.0.kill();
        }
        let _ = self.0.wait();
    }
}

struct PipelineGuard {
    runtime: tokio::runtime::Runtime,
    pipeline: Option<Pipeline>,
}

impl PipelineGuard {
    fn new(runtime: tokio::runtime::Runtime, pipeline: Pipeline) -> Self {
        Self {
            runtime,
            pipeline: Some(pipeline),
        }
    }

    fn pipeline(&self) -> &Pipeline {
        self.pipeline.as_ref().expect("pipeline is still open")
    }

    fn block_on<F: std::future::Future>(&self, future: F) -> F::Output {
        self.runtime.block_on(future)
    }

    fn close(mut self) -> Result<(), String> {
        self.close_inner()
    }

    fn close_inner(&mut self) -> Result<(), String> {
        let Some(pipeline) = self.pipeline.take() else {
            return Ok(());
        };
        let (shutdown, closed) = self.runtime.block_on(async {
            let shutdown = pipeline.shutdown().await.map_err(|error| error.to_string());
            let closed = pipeline
                .wait_closed()
                .await
                .map_err(|error| error.to_string());
            (shutdown, closed)
        });
        drop(pipeline);
        shutdown.and(closed)
    }
}

impl Drop for PipelineGuard {
    fn drop(&mut self) {
        let _ = self.close_inner();
    }
}

struct OutputCapture {
    bytes: Arc<std::sync::Mutex<Vec<u8>>>,
    reader: Option<thread::JoinHandle<()>>,
}

impl OutputCapture {
    fn new(mut pipe: impl Read + Send + 'static) -> Self {
        let bytes = Arc::new(std::sync::Mutex::new(Vec::new()));
        let shared = Arc::clone(&bytes);
        let reader = thread::spawn(move || {
            let mut buffer = [0; 8_192];
            loop {
                match pipe.read(&mut buffer) {
                    Ok(0) | Err(_) => break,
                    Ok(count) => {
                        if let Ok(mut output) = shared.lock() {
                            output.extend_from_slice(&buffer[..count]);
                        } else {
                            break;
                        }
                    }
                }
            }
        });
        Self {
            bytes,
            reader: Some(reader),
        }
    }

    fn contains(&self, needle: &[u8]) -> bool {
        self.bytes
            .lock()
            .map(|output| output.windows(needle.len()).any(|window| window == needle))
            .unwrap_or(false)
    }

    fn finish(mut self) -> String {
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
        let output = self
            .bytes
            .lock()
            .map(|output| String::from_utf8_lossy(&output).into_owned())
            .unwrap_or_else(|_| "output capture lock poisoned".to_owned());
        output
    }
}

fn test_config(data_dir: impl Into<PathBuf>) -> PipelineConfig {
    let mut config = PipelineConfig::new(data_dir);
    config.database_layout = DatabaseLayout::Shared;
    config.global_queue_capacity = 16;
    config.global_in_flight_limit = 16;
    config.batch_size = 8;
    config.batch_flush_timeout = Duration::from_millis(1);
    config.admission_timeout = Duration::from_secs(5);
    config.response_timeout = Duration::from_secs(5);
    config.history_io_concurrency = 2;
    config.history_operation_timeout = Duration::from_secs(5);
    config.global_storage_batches = 2;
    config.projector_batch_size = 8;
    config.gc_batch_size = 8;
    config.shutdown_timeout = Duration::from_secs(10);
    config.rocksdb_write_buffer_budget_bytes = 8 * 1024 * 1024;
    config.rocksdb_block_cache_budget_bytes = 8 * 1024 * 1024;
    config.rocksdb_background_jobs_budget = 1;
    config.startup_min_free_disk_bytes = 0;
    config.startup_min_available_memory_bytes = 0;
    config
}

fn fresh_volatile_histories(config: &PipelineConfig) -> Vec<Arc<dyn HistoryStore>> {
    (0..config.shard_count)
        .map(|shard| {
            Arc::new(MockHistory::new(format!("formal-crash-shard-{shard}")))
                as Arc<dyn HistoryStore>
        })
        .collect()
}

fn stable_credit() -> Transaction {
    Transaction {
        key: TransactionKey {
            account_id: 7,
            tx_id: 1,
            transaction_at: 1,
        },
        operation: Operation::Credit,
        amount: 10,
        refund_of: None,
    }
}

fn child_stdout_reader(
    stdout: impl Read + Send + 'static,
) -> (
    mpsc::Receiver<Result<String, String>>,
    thread::JoinHandle<String>,
) {
    let (sender, receiver) = mpsc::channel();
    let reader = thread::spawn(move || {
        let mut output = String::new();
        let mut lines = BufReader::new(stdout);
        loop {
            let mut line = String::new();
            match lines.read_line(&mut line) {
                Ok(0) => {
                    let _ = sender.send(Err("child exited before the durable marker".to_owned()));
                    break;
                }
                Ok(_) => {
                    output.push_str(&line);
                    if line.trim_end() == DURABLE_MARKER {
                        let _ = sender.send(Ok(output.clone()));
                        break;
                    }
                }
                Err(error) => {
                    let _ = sender.send(Err(format!("could not read child stdout: {error}")));
                    break;
                }
            }
        }
        output
    });
    (receiver, reader)
}

fn child_stderr_reader(stderr: impl Read + Send + 'static) -> mpsc::Receiver<String> {
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let mut output = String::new();
        let result = BufReader::new(stderr).read_to_string(&mut output);
        if let Err(error) = result {
            output.push_str(&format!("\n[could not read child stderr: {error}]"));
        }
        let _ = sender.send(output);
    });
    receiver
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_recovery_child_writes_durable_marker_then_blocks() {
    if std::env::var_os(CHILD_MODE_ENV).is_none() {
        return;
    }

    let data_dir = std::env::var_os(DATA_DIR_ENV).expect("parent provides source directory");
    let config = test_config(PathBuf::from(data_dir));
    let histories = fresh_volatile_histories(&config);
    let pipeline = Pipeline::open(config, histories)
        .await
        .expect("child opens the source ledger");
    let receipt = pipeline
        .submit(stable_credit())
        .await
        .expect("first credit commits before its result is lost");
    assert_eq!(receipt.shard_id, 0);
    assert_eq!(receipt.sequence, 1);
    assert_eq!(receipt.status, TransactionStatus::Applied);
    assert_eq!(receipt.balance, 10);
    assert!(!receipt.replayed);

    // This marker is test IPC: it tells the parent that the sync-WAL commit
    // returned inside the child. The child sends no client receipt to the parent.
    let stdout = io::stdout();
    let mut output = stdout.lock();
    writeln!(output, "{DURABLE_MARKER}").expect("write internal crash marker");
    output.flush().expect("flush internal crash marker");
    drop(output);

    // Keep the source handle open and the child alive until the parent kills it.
    std::future::pending::<()>().await;
}

#[test]
fn process_crash_after_sync_commit_and_lost_reply_replays_same_key() {
    let owned_dir = OwnedTempDir::new().expect("create a unique test-owned source directory");
    let executable = std::env::current_exe().expect("resolve integration-test executable");
    let mut command = Command::new(executable);
    command
        .arg("--exact")
        .arg("crash_recovery_child_writes_durable_marker_then_blocks")
        .arg("--nocapture")
        .env(CHILD_MODE_ENV, "1")
        .env(DATA_DIR_ENV, owned_dir.path().as_os_str())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = ChildGuard(command.spawn().expect("spawn isolated crash child"));
    let stdout = child.0.stdout.take().expect("child stdout is piped");
    let stderr = child.0.stderr.take().expect("child stderr is piped");
    let (marker_receiver, stdout_reader) = child_stdout_reader(stdout);
    let stderr_receiver = child_stderr_reader(stderr);
    let marker = marker_receiver.recv_timeout(CHILD_MARKER_TIMEOUT);

    // Kill and reap before any assertion can unwind and remove the owned DB.
    let status = child.kill_and_wait().expect("kill and reap crash child");
    let stdout = stdout_reader
        .join()
        .unwrap_or_else(|_| "child stdout reader panicked".to_owned());
    let stderr = stderr_receiver
        .recv_timeout(Duration::from_secs(5))
        .unwrap_or_else(|_| "child stderr reader did not finish".to_owned());
    let marker_seen = matches!(&marker, Ok(Ok(_)));
    let marker_detail = match &marker {
        Ok(Ok(_)) => "durable marker received".to_owned(),
        Ok(Err(error)) => error.clone(),
        Err(error) => format!("marker wait failed: {error:?}"),
    };
    assert!(
        marker_seen,
        "child did not report its durable commit ({marker_detail}); status={status:?}; stdout={stdout}; stderr={stderr}"
    );
    assert_eq!(
        status.signal(),
        Some(9),
        "child must be terminated by SIGKILL"
    );

    let config = test_config(owned_dir.path());
    let histories = fresh_volatile_histories(&config);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(16)
        .enable_all()
        .build()
        .expect("build bounded test runtime");
    let pipeline = runtime
        .block_on(Pipeline::open(config, histories))
        .expect("reopen the same source after process death");
    let pipeline = PipelineGuard::new(runtime, pipeline);
    pipeline.block_on(async {
        let service = pipeline.pipeline();
        let replay = service
            .submit(stable_credit())
            .await
            .expect("retry the exact key and body");
        assert_eq!(replay.shard_id, 0);
        assert_eq!(replay.sequence, 1);
        assert_eq!(replay.status, TransactionStatus::Applied);
        assert_eq!(replay.balance, 10);
        assert!(replay.replayed);
        assert!(!replay.historical_replay);

        let balance = service.balance(7).await.expect("read recovered balance");
        assert_eq!(balance.balance, 10);

        let debit = Transaction {
            key: TransactionKey {
                account_id: 7,
                tx_id: 2,
                transaction_at: 2,
            },
            operation: Operation::Debit,
            amount: 3,
            refund_of: None,
        };
        let debit_receipt = service.submit(debit).await.expect("commit next sequence");
        assert_eq!(debit_receipt.shard_id, 0);
        assert_eq!(debit_receipt.sequence, 2);
        assert_eq!(debit_receipt.status, TransactionStatus::Applied);
        assert_eq!(debit_receipt.balance, 7);
        assert!(!debit_receipt.replayed);

        let final_balance = service
            .balance(7)
            .await
            .expect("read post-recovery balance");
        assert_eq!(final_balance.balance, 7);
    });
    pipeline
        .close()
        .expect("drain workers and close recovered source before cleanup");
}

fn cli_command(data_dir: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ledger-wallet"));
    command
        .arg("--data-dir")
        .arg(data_dir)
        .arg("--min-free-disk-bytes")
        .arg("0")
        .arg("--min-available-memory-bytes")
        .arg("0")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

#[test]
fn cli_sigint_with_open_stdin_exits_and_same_data_dir_reopens_on_eof() {
    let owned_dir = OwnedTempDir::new().expect("create a unique test-owned source directory");

    let mut first = ChildGuard(cli_command(owned_dir.path()).spawn().expect("spawn CLI"));
    let mut first_stdin = first.0.stdin.take().expect("CLI stdin is piped");
    let first_stdout = OutputCapture::new(first.0.stdout.take().expect("CLI stdout is piped"));
    let first_stderr = OutputCapture::new(first.0.stderr.take().expect("CLI stderr is piped"));

    // Leave stdin open with a partial line. The input reader blocks awaiting a
    // newline while the main task must still observe SIGINT and shut down.
    first_stdin
        .write_all(b"help")
        .expect("write a partial stdin command without a newline");
    let startup_ready = first
        .wait_for_output_or_exit(&first_stdout, b"stdin commands:", Duration::from_secs(15))
        .expect("wait for CLI startup output");
    if !startup_ready {
        let status = first
            .kill_and_wait()
            .expect("reap CLI that failed to start");
        drop(first_stdin);
        let stdout = first_stdout.finish();
        let stderr = first_stderr.finish();
        panic!("CLI did not become ready; status={status:?}; stdout={stdout}; stderr={stderr}");
    }

    // SAFETY: `first` is a live child process owned by this test.
    let signal_result = unsafe { libc::kill(first.0.id() as libc::pid_t, libc::SIGINT) };
    if signal_result != 0 {
        let error = io::Error::last_os_error();
        let status = first
            .kill_and_wait()
            .expect("reap CLI after signal failure");
        drop(first_stdin);
        let stdout = first_stdout.finish();
        let stderr = first_stderr.finish();
        panic!(
            "could not send SIGINT: {error}; status={status:?}; stdout={stdout}; stderr={stderr}"
        );
    }

    let (first_status, first_timed_out) = first
        .wait_or_kill(Duration::from_secs(15))
        .expect("wait for SIGINT shutdown");
    drop(first_stdin);
    let first_stdout = first_stdout.finish();
    let first_stderr = first_stderr.finish();
    assert!(
        !first_timed_out,
        "CLI did not exit after SIGINT; stdout={first_stdout}; stderr={first_stderr}"
    );
    assert!(
        first_status.success(),
        "CLI exited unsuccessfully after SIGINT ({first_status:?}); stdout={first_stdout}; stderr={first_stderr}"
    );
    assert!(
        first_stdout.contains("shutdown requested by SIGINT"),
        "CLI did not report SIGINT shutdown; stdout={first_stdout}; stderr={first_stderr}"
    );

    // Reopen the same source while retaining the reaped first Child handle.
    // EOF, rather than an explicit command, must then complete the second run.
    let mut second = ChildGuard(cli_command(owned_dir.path()).spawn().expect("reopen CLI"));
    drop(second.0.stdin.take().expect("second CLI stdin is piped"));
    let second_stdout =
        OutputCapture::new(second.0.stdout.take().expect("second CLI stdout is piped"));
    let second_stderr =
        OutputCapture::new(second.0.stderr.take().expect("second CLI stderr is piped"));
    let (second_status, second_timed_out) = second
        .wait_or_kill(Duration::from_secs(15))
        .expect("wait for EOF shutdown");
    let second_stdout = second_stdout.finish();
    let second_stderr = second_stderr.finish();
    assert!(
        !second_timed_out,
        "reopened CLI did not exit on stdin EOF; stdout={second_stdout}; stderr={second_stderr}"
    );
    assert!(
        second_status.success(),
        "reopened CLI exited unsuccessfully ({second_status:?}); stdout={second_stdout}; stderr={second_stderr}"
    );
    assert!(
        second_stdout.contains("stdin commands:"),
        "reopened CLI did not reach startup; stdout={second_stdout}; stderr={second_stderr}"
    );
    assert!(
        second_stdout.contains("stdin reached EOF; shutting down"),
        "reopened CLI did not report EOF shutdown; stdout={second_stdout}; stderr={second_stderr}"
    );
}
