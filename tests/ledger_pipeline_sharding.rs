#![allow(dead_code)]

#[path = "../benches/support/ledger_account_store.rs"]
mod ledger_account_store;
#[path = "../benches/support/ledger_pipeline.rs"]
mod ledger_pipeline;
#[path = "../benches/support/ledger_preflight.rs"]
mod ledger_preflight;
#[path = "../benches/support/ledger_projection_worker.rs"]
mod ledger_projection_worker;
#[path = "../benches/support/ledger_time_boundary.rs"]
mod ledger_time_boundary;
#[path = "../benches/support/request_batch_queue.rs"]
mod request_batch_queue;

use ledger_account_store::{
    AccountStore, BalanceMode, Operation, RefundHistory, Reply, RocksDbBudget, Transaction,
    TransactionKey, TransactionStatus,
};
use ledger_pipeline::sharding::{
    CrossShardFaultPlan, RocksLayout, TestFaultControl, TrialOptions, TrialSummary,
};
use ledger_projection_worker::MockProjectionStore;
use rocksdb::{DB, Options};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::timeout;

static ROCKSDB_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct TempDb {
    path: PathBuf,
    preserve_if_present: Vec<PathBuf>,
}

impl TempDb {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let nonce = NEXT.fetch_add(1, Ordering::Relaxed);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after Unix epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "ledger-wallet-pipeline-sharding-test-{}-{timestamp}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&path).expect("create unique test directory");
        Self {
            path,
            preserve_if_present: Vec::new(),
        }
    }

    fn database(&self, name: &str) -> PathBuf {
        self.path.join(name)
    }

    fn preserve_if_present(&mut self, path: PathBuf) {
        self.preserve_if_present.push(path);
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        let remaining = self
            .preserve_if_present
            .iter()
            .filter(|path| path.exists())
            .collect::<Vec<_>>();
        if !remaining.is_empty() {
            for path in remaining {
                eprintln!(
                    "TEST_SCRATCH_RETAINED path={} because owned RocksDB scratch remains",
                    path.display()
                );
            }
            return;
        }
        let _ = fs::remove_dir_all(&self.path);
    }
}

#[derive(Clone, Copy, Debug)]
enum Layout {
    Shared,
    Dedicated,
}

struct StorePair {
    first: AccountStore,
    second: AccountStore,
    // Keep every database owner alive until both namespace workers stop.
    db_handles: Vec<(Arc<DB>, Arc<Options>)>,
}

impl StorePair {
    async fn open(temp: &TempDb, layout: Layout) -> Self {
        match layout {
            Layout::Shared => {
                let (db, options) =
                    AccountStore::open_database_with_budget(&temp.database("shared"), budget())
                        .await
                        .expect("open shared RocksDB");
                let first = AccountStore::open_on_database(
                    db.clone(),
                    options.clone(),
                    vec![7],
                    1,
                    BalanceMode::PerBatch,
                    100,
                )
                .await
                .expect("open first shared namespace");
                let second = AccountStore::open_on_database(
                    db.clone(),
                    options.clone(),
                    vec![7],
                    2,
                    BalanceMode::PerBatch,
                    100,
                )
                .await
                .expect("open second shared namespace");
                assert_eq!(first.namespace_id(), Some(1));
                assert_eq!(second.namespace_id(), Some(2));
                Self {
                    first,
                    second,
                    db_handles: vec![(db, options)],
                }
            }
            Layout::Dedicated => {
                let (first_db, first_options) = AccountStore::open_database_with_budget(
                    &temp.database("dedicated-first"),
                    budget(),
                )
                .await
                .expect("open first dedicated RocksDB");
                let (second_db, second_options) = AccountStore::open_database_with_budget(
                    &temp.database("dedicated-second"),
                    budget(),
                )
                .await
                .expect("open second dedicated RocksDB");
                let first = AccountStore::open_on_database(
                    first_db.clone(),
                    first_options.clone(),
                    vec![7],
                    1,
                    BalanceMode::PerBatch,
                    100,
                )
                .await
                .expect("open first dedicated namespace");
                let second = AccountStore::open_on_database(
                    second_db.clone(),
                    second_options.clone(),
                    vec![7],
                    2,
                    BalanceMode::PerBatch,
                    100,
                )
                .await
                .expect("open second dedicated namespace");
                assert_eq!(first.namespace_id(), Some(1));
                assert_eq!(second.namespace_id(), Some(2));
                Self {
                    first,
                    second,
                    db_handles: vec![(first_db, first_options), (second_db, second_options)],
                }
            }
        }
    }

    async fn close(self) {
        let Self {
            first,
            second,
            db_handles,
        } = self;
        first.shutdown().await.expect("close first store");
        second.shutdown().await.expect("close second store");
        drop(db_handles);
    }
}

fn budget() -> RocksDbBudget {
    RocksDbBudget {
        write_buffer_size: 1024 * 1024,
        max_write_buffer_number: 2,
        block_cache_bytes: 1024 * 1024,
        max_background_jobs: 2,
    }
}

fn trial_options(
    shards: usize,
    layout: RocksLayout,
    concurrency: usize,
    users: usize,
    requests_per_user: usize,
    output_dir: PathBuf,
) -> TrialOptions {
    TrialOptions {
        shards,
        layout,
        concurrency,
        users,
        requests_per_user,
        output_dir,
        preflight_observation: std::time::Duration::from_millis(100),
        cross_shard_fault: None,
    }
}

fn assert_trial_counts(
    summary: &TrialSummary,
    shards: usize,
    users: usize,
    requests_per_user: usize,
) {
    let requests = (users * requests_per_user) as u64;
    assert_eq!(summary.requests, requests);
    assert_eq!(summary.credits, requests / 2);
    assert_eq!(summary.debits, requests / 2);
    assert_eq!(summary.final_sequence_sum, (users as u64) * 203);
    assert_eq!(summary.shards.len(), shards);
    assert!(summary.client_wall > std::time::Duration::ZERO);
    assert!(summary.settled_wall > std::time::Duration::ZERO);

    let mut request_sum = 0_u64;
    let mut credit_sum = 0_u64;
    let mut debit_sum = 0_u64;
    let mut sequence_sum = 0_u64;
    for (shard_id, shard) in summary.shards.iter().enumerate() {
        let accounts = users / shards;
        assert_eq!(shard.shard_id, shard_id);
        assert_eq!(shard.account_count, accounts);
        assert_eq!(shard.requests, (accounts * requests_per_user) as u64);
        assert_eq!(shard.credits, shard.requests / 2);
        assert_eq!(shard.debits, shard.requests / 2);
        assert_eq!(shard.final_sequence, (accounts as u64) * 203);
        assert_eq!(shard.projected_sequence, shard.final_sequence);
        assert_eq!(shard.destination_sequence, shard.final_sequence);
        assert!(
            shard.gc_prefix >= (accounts as u64) * 3,
            "shard {shard_id} did not collect the three old seed records per account"
        );
        request_sum += shard.requests;
        credit_sum += shard.credits;
        debit_sum += shard.debits;
        sequence_sum += shard.final_sequence;
    }
    assert_eq!(request_sum, summary.requests);
    assert_eq!(credit_sum, summary.credits);
    assert_eq!(debit_sum, summary.debits);
    assert_eq!(sequence_sum, summary.final_sequence_sum);
}

#[test]
fn sharding_topology_and_database_budgets_are_stable_and_keep_aggregate_limits() {
    for shard_count in [2, 4] {
        let users = 2_048;
        let topology = ledger_pipeline::sharding::topology(shard_count, users)
            .expect("valid account topology");
        assert_eq!(topology.len(), shard_count);
        assert_eq!(
            topology,
            ledger_pipeline::sharding::topology(shard_count, users)
                .expect("account assignment is deterministic")
        );
        let mut seen = vec![false; users];
        for (shard_id, accounts) in topology.iter().enumerate() {
            assert_eq!(accounts.len(), users / shard_count);
            for account_id in accounts {
                assert_eq!((*account_id as usize) % shard_count, shard_id);
                assert!(!std::mem::replace(&mut seen[*account_id as usize], true));
            }
        }
        assert!(seen.into_iter().all(|assigned| assigned));

        for layout in [RocksLayout::Shared, RocksLayout::Dedicated] {
            let budgets = ledger_pipeline::sharding::budget_for(shard_count, layout)
                .expect("valid aggregate database budget");
            let database_count = match layout {
                RocksLayout::Shared => 1,
                RocksLayout::Dedicated => shard_count,
            };
            assert_eq!(budgets.len(), database_count);
            let write_buffer_total: usize = budgets
                .iter()
                .map(|budget| budget.write_buffer_size * budget.max_write_buffer_number as usize)
                .sum();
            let block_cache_total: usize =
                budgets.iter().map(|budget| budget.block_cache_bytes).sum();
            let background_jobs_total: i32 = budgets
                .iter()
                .map(|budget| budget.max_background_jobs)
                .sum();
            assert_eq!(write_buffer_total, 128 * 1024 * 1024);
            assert_eq!(block_cache_total, 128 * 1024 * 1024);
            assert_eq!(background_jobs_total, 8);
        }
    }
}

fn transaction(
    account_id: u64,
    tx_id: u64,
    at: u64,
    operation: Operation,
    amount: u64,
) -> Transaction {
    Transaction {
        key: TransactionKey {
            account_id,
            tx_id,
            transaction_at: at,
        },
        operation,
        amount,
        refund_of: None,
    }
}

fn attach_history(store: &AccountStore, history: &Arc<MockProjectionStore>) {
    let destination: Arc<dyn RefundHistory> = history.clone();
    store
        .set_refund_history(destination)
        .expect("install destination history");
}

async fn project_all(store: &AccountStore, history: &MockProjectionStore) {
    let latest = store.latest_seq();
    let read = store
        .read_ledger_range(
            1,
            usize::try_from(latest).expect("small sequence fits usize"),
        )
        .await
        .expect("read committed source records");
    assert_eq!(read.records.len(), latest as usize);
    assert_eq!(history.apply_batch(&read.records).unwrap(), latest);
    store
        .persist_projection_progress(latest)
        .await
        .expect("persist destination progress");
}

async fn verify_recovered_store(
    store: &AccountStore,
    history: &MockProjectionStore,
    latest: u64,
    balance: u64,
    projected_before: (u64, u64),
    gc_prefix: u64,
) {
    assert_eq!(store.latest_seq(), latest);
    assert_eq!(store.balance(store.account_ids()[0]).unwrap(), balance);
    assert_eq!(store.gc_prefix_seq(), gc_prefix);
    assert_eq!(store.durable_projection_progress().await.unwrap(), latest);
    assert_eq!(store.verify_projection_destination().await.unwrap(), latest);
    assert_eq!(history.progress(), latest);
    assert_eq!(
        store.restored_projected_before().await.unwrap(),
        Some(projected_before)
    );
    store.validate_integrity().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sharding_projected_watermarks_gc_and_recovery_stay_isolated_per_database_namespace() {
    let _rocksdb_guard = ROCKSDB_TEST_LOCK.lock().await;
    for layout in [Layout::Shared, Layout::Dedicated] {
        let temp = TempDb::new();
        let account_id = 7;
        let first_one = transaction(account_id, 1, 100, Operation::Credit, 13);
        let first_two = transaction(account_id, 2, 600, Operation::Debit, 4);
        // This intentionally has the same request key as first_one. The shared
        // topology must keep it independent in namespace 2 at its own seq 1.
        let second_one = transaction(account_id, 1, 100, Operation::Credit, 29);
        let duplicate_key = first_one.key;
        let first_history = Arc::new(MockProjectionStore::default());
        let second_history = Arc::new(MockProjectionStore::default());

        let pair = StorePair::open(&temp, layout).await;
        attach_history(&pair.first, &first_history);
        attach_history(&pair.second, &second_history);
        assert_eq!(
            pair.first
                .handle_batch(vec![first_one.clone()])
                .await
                .unwrap()[0],
            Reply::Transaction {
                status: TransactionStatus::Applied,
                balance: 13,
                seq: 1,
                replayed: false,
            }
        );
        assert_eq!(
            pair.first
                .handle_batch(vec![first_two.clone()])
                .await
                .unwrap()[0],
            Reply::Transaction {
                status: TransactionStatus::Applied,
                balance: 9,
                seq: 2,
                replayed: false,
            }
        );
        assert_eq!(
            pair.second
                .handle_batch(vec![second_one.clone()])
                .await
                .unwrap()[0],
            Reply::Transaction {
                status: TransactionStatus::Applied,
                balance: 29,
                seq: 1,
                replayed: false,
            }
        );
        assert_eq!(pair.first.latest_seq(), 2);
        assert_eq!(pair.second.latest_seq(), 1);

        project_all(&pair.first, &first_history).await;
        project_all(&pair.second, &second_history).await;
        pair.first
            .persist_projected_before_durable(500, 2)
            .await
            .expect("publish first namespace watermark");
        pair.second
            .persist_projected_before_durable(150, 1)
            .await
            .expect("publish second namespace watermark");
        assert!(first_history.contains(duplicate_key));
        assert!(second_history.contains(duplicate_key));

        let gc = pair
            .first
            .collect_garbage(1)
            .await
            .expect("collect first namespace's safe prefix");
        assert_eq!(gc.deleted, 1);
        assert_eq!(gc.gc_prefix_seq, 1);
        assert!(!pair.first.has_ledger_sequence(1).await.unwrap());
        assert!(pair.first.has_ledger_sequence(2).await.unwrap());
        assert!(pair.second.has_ledger_sequence(1).await.unwrap());
        assert_eq!(pair.first.durable_projection_progress().await.unwrap(), 2);
        assert_eq!(pair.second.durable_projection_progress().await.unwrap(), 1);
        pair.close().await;

        // Reopen after every DB handle and store worker has closed. The same
        // destination objects model the retained projection across source reopen.
        let recovered = StorePair::open(&temp, layout).await;
        attach_history(&recovered.first, &first_history);
        attach_history(&recovered.second, &second_history);
        verify_recovered_store(&recovered.first, &first_history, 2, 9, (500, 2), 1).await;
        verify_recovered_store(&recovered.second, &second_history, 1, 29, (150, 1), 0).await;
        assert!(!recovered.first.has_ledger_sequence(1).await.unwrap());
        assert!(recovered.first.has_ledger_sequence(2).await.unwrap());
        assert!(recovered.second.has_ledger_sequence(1).await.unwrap());
        assert_eq!(
            recovered
                .first
                .handle_batch(vec![first_two.clone()])
                .await
                .unwrap()[0],
            Reply::Transaction {
                status: TransactionStatus::Applied,
                balance: 9,
                seq: 2,
                replayed: true,
            }
        );
        assert_eq!(
            recovered
                .second
                .handle_batch(vec![second_one.clone()])
                .await
                .unwrap()[0],
            Reply::Transaction {
                status: TransactionStatus::Applied,
                balance: 29,
                seq: 1,
                replayed: true,
            }
        );
        recovered.close().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sharding_reduced_trials_cover_both_layouts_shard_counts_and_query_concurrencies() {
    let _rocksdb_guard = ROCKSDB_TEST_LOCK.lock().await;
    let users = 2_048;
    let requests_per_user = 200;
    let mut temp = TempDb::new();
    for shards in [2, 4] {
        for layout in [RocksLayout::Shared, RocksLayout::Dedicated] {
            for concurrency in [4, 8] {
                let case = format!(
                    "s{shards}-{}-c{concurrency}",
                    match layout {
                        RocksLayout::Shared => "shared",
                        RocksLayout::Dedicated => "dedicated",
                    }
                );
                let output_dir = temp.database(&case);
                let scratch_dir = output_dir.join("owned-scratch");
                temp.preserve_if_present(scratch_dir.clone());
                let options = trial_options(
                    shards,
                    layout,
                    concurrency,
                    users,
                    requests_per_user,
                    output_dir.clone(),
                );
                let summary = timeout(
                    std::time::Duration::from_secs(300),
                    ledger_pipeline::sharding::run_trial_for_test(options),
                )
                .await
                .unwrap_or_else(|_| panic!("reduced trial {case} exceeded five minutes"))
                .unwrap_or_else(|error| panic!("reduced trial {case} failed: {error}"));
                assert_trial_counts(&summary, shards, users, requests_per_user);
                assert!(
                    !scratch_dir.exists(),
                    "successful trial {case} must release all RocksDB instances"
                );
                let group_report = fs::read_to_string(output_dir.join("index_groups.csv"))
                    .expect("read raw index-group samples");
                let mut groups_per_batch =
                    std::collections::BTreeMap::<(usize, usize), usize>::new();
                for row in group_report.lines().skip(1) {
                    let fields = row.split(',').collect::<Vec<_>>();
                    assert_eq!(fields.len(), 9, "malformed raw index-group row: {row}");
                    let key = (
                        fields[0].parse().expect("numeric shard ID"),
                        fields[1].parse().expect("numeric batch ID"),
                    );
                    *groups_per_batch.entry(key).or_default() += 1;
                }
                assert!(
                    groups_per_batch.values().any(|count| *count >= 2),
                    "trial {case} never submitted multiple index query groups for a batch"
                );
                if shards == 2 && matches!(layout, RocksLayout::Shared) && concurrency == 4 {
                    assert_archive_leaf_validation_rejects_bad_percentile_and_missing_file(
                        &temp,
                        &output_dir,
                    );
                }
            }
        }
    }
}

struct ReleaseBlockedRead(Arc<TestFaultControl>);

impl Drop for ReleaseBlockedRead {
    fn drop(&mut self) {
        self.0.release_blocked_read();
    }
}

fn paths_named(root: &Path, name: &str) -> std::io::Result<Vec<PathBuf>> {
    let mut found = Vec::new();
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(found),
        Err(error) => return Err(error),
    };
    for entry in entries {
        let entry = entry?;
        if entry.file_name() == name {
            found.push(entry.path());
        }
        if entry.file_type()?.is_dir() {
            found.extend(paths_named(&entry.path(), name)?);
        }
    }
    Ok(found)
}

fn copy_tree(source: &Path, destination: &Path) -> std::io::Result<()> {
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let target = destination.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_tree(&entry.path(), &target)?;
        } else {
            fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

fn assert_archive_leaf_validation_rejects_bad_percentile_and_missing_file(
    temp: &TempDb,
    trial_dir: &Path,
) {
    let trial_name = trial_dir.file_name().expect("trial archive has a basename");
    let original_stages = fs::read(trial_dir.join("stages.csv")).expect("read original stages");
    let original_raw =
        fs::read(trial_dir.join("raw_stage_samples.csv")).expect("read original raw stages");

    let percentile_copy = temp.database("validation-percentile").join(trial_name);
    copy_tree(trial_dir, &percentile_copy).expect("copy archive for percentile validation");
    let stages =
        fs::read_to_string(percentile_copy.join("stages.csv")).expect("read copied stage summary");
    let mut rows = stages.lines().map(str::to_owned).collect::<Vec<_>>();
    let header = rows
        .first()
        .expect("stages CSV has a header")
        .split(',')
        .collect::<Vec<_>>();
    let scope_index = header.iter().position(|field| *field == "scope").unwrap();
    let metric_index = header.iter().position(|field| *field == "metric").unwrap();
    let p99_index = header.iter().position(|field| *field == "p99_ns").unwrap();
    let row = rows
        .iter_mut()
        .skip(1)
        .find(|row| {
            let fields = row.split(',').collect::<Vec<_>>();
            fields[scope_index] == "global" && fields[metric_index] == "request.total"
        })
        .expect("global request.total stage row exists");
    let mut fields = row.split(',').map(str::to_owned).collect::<Vec<_>>();
    fields[p99_index] = if fields[p99_index] == "0" { "1" } else { "0" }.to_owned();
    *row = fields.join(",");
    fs::write(
        percentile_copy.join("stages.csv"),
        format!("{}\n", rows.join("\n")),
    )
    .expect("tamper only the copied percentile summary");
    let error = ledger_pipeline::sharding::validate_trial_archive(&percentile_copy)
        .expect_err("incorrect pooled percentile must be rejected");
    assert!(
        error.contains("stage summary for request.total does not match raw samples"),
        "unexpected validation error: {error}"
    );

    let missing_file_copy = temp.database("validation-missing-file").join(trial_name);
    copy_tree(trial_dir, &missing_file_copy).expect("copy archive for missing-file validation");
    fs::remove_file(missing_file_copy.join("raw_stage_samples.csv"))
        .expect("remove a required raw stage artifact from the copy");
    let error = ledger_pipeline::sharding::validate_trial_archive(&missing_file_copy)
        .expect_err("missing required raw artifact must be rejected");
    assert!(
        error.contains("raw_stage_samples.csv"),
        "missing-file validation returned an unrelated error: {error}"
    );

    assert_eq!(
        fs::read(trial_dir.join("stages.csv")).expect("reread original stages"),
        original_stages,
        "validation copies must not mutate original stage artifacts"
    );
    assert_eq!(
        fs::read(trial_dir.join("raw_stage_samples.csv")).expect("reread original raw stages"),
        original_raw,
        "validation copies must not mutate original raw samples"
    );
    fs::remove_dir_all(percentile_copy.parent().unwrap()).expect("remove percentile test copy");
    fs::remove_dir_all(missing_file_copy.parent().unwrap()).expect("remove missing-file test copy");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sharding_cross_shard_failure_drains_a_held_native_query_before_removing_db_scratch() {
    let _rocksdb_guard = ROCKSDB_TEST_LOCK.lock().await;
    let mut temp = TempDb::new();
    let output_dir = temp.database("failure-output");
    let scratch_dir = output_dir.join("owned-scratch");
    temp.preserve_if_present(scratch_dir.clone());
    let control = Arc::new(TestFaultControl::default());
    let _release_if_test_exits_early = ReleaseBlockedRead(Arc::clone(&control));
    let mut options = trial_options(2, RocksLayout::Shared, 4, 1_024, 200, output_dir.clone());
    options.cross_shard_fault = Some(CrossShardFaultPlan {
        blocked_shard: 0,
        failed_shard: 1,
        group_index: 0,
        control: Arc::clone(&control),
    });
    let mut trial =
        tokio::spawn(async move { ledger_pipeline::sharding::run_trial_for_test(options).await });

    let blocked = timeout(
        std::time::Duration::from_secs(30),
        control.wait_until_blocked(),
    )
    .await
    .is_ok();
    if !blocked {
        control.release_blocked_read();
        let drained = timeout(std::time::Duration::from_secs(30), &mut trial)
            .await
            .is_ok();
        assert!(drained, "trial still running after hold-point timeout");
        panic!("native query group did not reach its hold point within 30 seconds");
    }
    let failure_propagated = timeout(
        std::time::Duration::from_secs(30),
        control.wait_until_failure_propagated(),
    )
    .await
    .is_ok();
    if !failure_propagated {
        control.release_blocked_read();
        let drained = timeout(std::time::Duration::from_secs(30), &mut trial)
            .await
            .is_ok();
        assert!(
            drained,
            "trial still running after failure-propagation timeout"
        );
        panic!("failure on the other shard did not reach the trial supervisor");
    }
    let scratch_existed_during_native_call = scratch_dir.is_dir();
    let lock_existed_during_native_call = paths_named(&scratch_dir, "LOCK")
        .map(|paths| !paths.is_empty())
        .unwrap_or(false);

    control.release_blocked_read();
    let result = match timeout(std::time::Duration::from_secs(30), &mut trial).await {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => panic!("sharded trial task panicked: {error}"),
        Err(_) => {
            control.release_blocked_read();
            let drained = timeout(std::time::Duration::from_secs(30), &mut trial)
                .await
                .is_ok();
            assert!(drained, "trial still running after the drain timeout");
            panic!("failed trial did not join every worker within 30 seconds");
        }
    };
    let error = result.expect_err("one shard failure must fail the complete trial");
    assert!(error.contains("injected MultiGet read failure"), "{error}");
    assert!(
        scratch_existed_during_native_call && lock_existed_during_native_call,
        "RocksDB scratch and its LOCK file must remain while a native query is held"
    );
    assert!(output_dir.join("failure.txt").is_file());
    assert!(
        !scratch_dir.exists(),
        "owned RocksDB scratch must be removed after the failure returns"
    );
}
