use ledger_wallet_scylla_bench::{
    AccountState, Error, Metrics, Operation, Outcome, ScyllaStore, Service, Transaction,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::task::JoinSet;
use tokio::time::sleep;

static KEYSPACE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an owned three-node Scylla cluster; set SCYLLA_NODES and run with --ignored"]
async fn commitlog_sync_path_smoke() {
    let nodes = scylla_nodes();
    let workers = env_usize("SCYLLA_WORKERS", 3);
    let shards = env_usize("SCYLLA_SHARDS", 1);
    let keyspace = fresh_keyspace("trace");
    println!("commitlog_trace_smoke_keyspace={keyspace}");

    let metrics = Arc::new(Metrics::new());
    let store = ScyllaStore::connect(&nodes, &keyspace, shards, workers, metrics.clone())
        .await
        .expect("create a fresh owned keyspace for the commitlog trace smoke");
    assert_eq!(store.data_connections(), u64::try_from(workers).unwrap());
    store
        .seed_account(0, 100)
        .await
        .expect("seed trace account");

    // Setup reads are outside the measured window and are not counted below.
    let initial = store
        .preload_account_state(0)
        .await
        .expect("preload trace account before the gate");
    assert_eq!(
        initial,
        AccountState {
            balance: 100,
            version: 0,
            write_clock: 0
        }
    );
    metrics.reset();
    let service = Service::start(store.clone(), workers, vec![initial], 8, metrics.clone())
        .expect("start the bounded trace smoke service");
    metrics.enable();
    let driver_before = store.driver_counters();

    let credit = service
        .submit(tx(1, Operation::Credit, 10))
        .await
        .expect("credit must receive a CL=THREE acknowledgement");
    assert_eq!(
        (credit.outcome, credit.balance, credit.version),
        (Outcome::Applied, 110, 1)
    );
    let debit = service
        .submit(tx(2, Operation::Debit, 3))
        .await
        .expect("debit must receive a CL=THREE acknowledgement");
    assert_eq!(
        (debit.outcome, debit.balance, debit.version),
        (Outcome::Applied, 107, 2)
    );
    service.shutdown().await.expect("drain trace smoke work");
    let cache = service.cache_snapshot();
    metrics.disable();

    let (state, ledger) = store
        .verify_account(0)
        .await
        .expect("verify complete trace smoke account");
    assert_eq!(state, cache[0]);
    assert_eq!(
        (state.balance, state.version, state.write_clock),
        (107, 2, 2)
    );
    assert_eq!(ledger.len(), 2);
    assert_eq!(ledger[0].request.operation, Operation::Credit);
    assert_eq!(ledger[1].request.operation, Operation::Debit);

    let driver_after = store.driver_counters();
    assert_eq!(driver_after.retries, driver_before.retries);
    assert_eq!(driver_after.errors, driver_before.errors);
    assert_eq!(
        driver_after.queries.saturating_sub(driver_before.queries),
        4,
        "two LWT transactions plus two post-window verification SELECTs are four client queries"
    );
    let counters = metrics.snapshot().counters;
    assert_eq!(counters.get("cql_write_lwt"), Some(&2));
    assert_eq!(counters.get("cql_read_metadata").copied().unwrap_or(0), 0);
    assert_eq!(counters.get("cql_read_ledger").copied().unwrap_or(0), 0);
    assert!(!service.failed());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an owned three-node Scylla cluster; set SCYLLA_NODES and run with --ignored"]
async fn durable_wallet_contract_against_three_node_scylla() {
    let nodes = scylla_nodes();
    let workers = env_usize("SCYLLA_WORKERS", 3);
    let shards = env_usize("SCYLLA_SHARDS", 1);
    assert_eq!(workers % (3 * shards), 0);

    let keyspace = fresh_keyspace("contract");
    let metrics = Arc::new(Metrics::new());
    let store = ScyllaStore::connect(&nodes, &keyspace, shards, workers, metrics.clone())
        .await
        .expect("connect must create a fresh owned RF=3 keyspace and schema");
    assert_eq!(store.keyspace(), keyspace);
    assert!(!store.session_id().is_empty());
    assert_eq!(store.data_connections(), u64::try_from(workers).unwrap());
    store
        .seed_account(0, 100)
        .await
        .expect("seed account zero with LWT");
    store
        .seed_account(1, 100)
        .await
        .expect("seed account one with LWT");

    // Preload the complete bounded account set before resetting metrics or starting work.
    let initial_zero = store
        .preload_account_state(0)
        .await
        .expect("preload account zero before the measurement gate");
    let initial_one = store
        .preload_account_state(1)
        .await
        .expect("preload account one before the measurement gate");
    assert!(
        store.preload_account_state(99).await.is_err(),
        "preload must reject an account without its seeded metadata row"
    );
    assert_eq!(
        initial_zero,
        AccountState {
            balance: 100,
            version: 0,
            write_clock: 0
        }
    );
    assert_eq!(initial_one, initial_zero);

    metrics.reset();
    let service = Service::start(
        store.clone(),
        workers,
        vec![initial_zero, initial_one],
        64,
        metrics.clone(),
    )
    .expect("start a bounded account owner service with preloaded states");
    metrics.enable();
    let driver_before = store.driver_counters();

    let first = tx(1, Operation::Credit, 10);
    let first_result = service
        .submit(first.clone())
        .await
        .expect("first LWT must receive a durable CL=THREE acknowledgement");
    assert_eq!(first_result.outcome, Outcome::Applied);
    assert_eq!((first_result.balance, first_result.version), (110, 1));

    let mut callers = JoinSet::new();
    for tx_id in 2..=13 {
        let service = service.clone();
        callers.spawn(async move { service.submit(tx(tx_id, Operation::Credit, 1)).await });
    }
    while let Some(result) = callers.join_next().await {
        assert_eq!(result.unwrap().unwrap().outcome, Outcome::Applied);
    }

    let different_account = tx_for_account(1, 1, Operation::Credit, 5);
    let other_account_result = service
        .submit(different_account.clone())
        .await
        .expect("the same tx_id in another account partition is independent");
    assert_eq!(
        (
            other_account_result.outcome,
            other_account_result.balance,
            other_account_result.version
        ),
        (Outcome::Applied, 105, 1)
    );

    let replay = service
        .submit(first.clone())
        .await
        .expect("same-body replay must return the original durable result");
    assert!(replay.replayed);
    assert_eq!((replay.balance, replay.version), (110, 1));
    let mut conflict = first.clone();
    conflict.amount = 11;
    assert!(matches!(
        service.submit(conflict).await,
        Err(Error::Conflict(_))
    ));

    let rejected_request = tx(14, Operation::Debit, 1_000);
    let rejected = service
        .submit(rejected_request.clone())
        .await
        .expect("business rejection must be saved in the ledger");
    assert_eq!(rejected.outcome, Outcome::InsufficientFunds);
    assert_eq!((rejected.balance, rejected.version), (122, 14));
    let rejected_replay = service
        .submit(rejected_request)
        .await
        .expect("replayed rejection must return its original durable result");
    assert!(rejected_replay.replayed);
    assert_eq!(rejected_replay.outcome, Outcome::InsufficientFunds);
    assert_eq!(rejected_replay.version, 14);

    let fault_container = std::env::var("SCYLLA_FAULT_CONTAINER").ok();
    let mut before_outage = None;
    if let Some(container) = fault_container.as_deref() {
        validate_container_name(container);
        let expected_owner = std::env::var("SCYLLA_FAULT_OWNER")
            .expect("SCYLLA_FAULT_OWNER must be provided for an owned-node fault test");
        let actual_owner = docker_container_owner_nonce(container)
            .await
            .expect("verify owner and nonce labels on the supplied node");
        assert_eq!(
            actual_owner, expected_owner,
            "refusing to stop a container without the matching owner nonce"
        );
        let snapshot = store
            .verify_account(0)
            .await
            .expect("capture the complete account snapshot before node fault");
        assert_eq!(
            (
                snapshot.0.balance,
                snapshot.0.version,
                snapshot.0.write_clock
            ),
            (122, 14, 14)
        );
        assert_eq!(snapshot.1.len(), 14);
        before_outage = Some(snapshot);

        docker_container_action("kill", container)
            .await
            .expect("SIGKILL only the explicitly supplied runner-owned node");
        let during_outage = service.submit(tx(15, Operation::Credit, 1)).await;
        assert!(
            matches!(during_outage, Err(Error::CommitOutcomeUnknown(_))),
            "a post-submission LWT failure must be reported as unknown, never as a success"
        );
        assert!(
            service.failed(),
            "unknown outcome must fail the service closed"
        );
        assert!(matches!(
            service.submit(tx(16, Operation::Credit, 1)).await,
            Err(Error::Frozen(_)) | Err(Error::NotAccepted(_))
        ));
    }

    service.shutdown().await.expect("drain accepted work");
    let cache_after_drain = service.cache_snapshot();
    metrics.disable();
    assert_eq!(cache_after_drain.len(), 2);
    assert_eq!(
        cache_after_drain[0],
        AccountState {
            balance: 122,
            version: 14,
            write_clock: 14
        },
        "replay keeps the current cache; unknown outcome must not publish a speculative state"
    );
    assert_eq!(
        cache_after_drain[1],
        AccountState {
            balance: 105,
            version: 1,
            write_clock: 1
        }
    );

    if let Some(container) = fault_container.as_deref() {
        docker_container_action("start", container)
            .await
            .expect("restart the explicitly supplied runner-owned node");
    }
    let (final_zero, records_zero) = if fault_container.is_some() {
        tokio::time::timeout(Duration::from_secs(120), async {
            loop {
                match store.verify_account(0).await {
                    Ok(state) => break state,
                    Err(Error::Storage(_)) => sleep(Duration::from_millis(250)).await,
                    Err(error) => panic!("account data is invalid after node restart: {error}"),
                }
            }
        })
        .await
        .expect("verification should resume after restarting the owned node")
    } else {
        store
            .verify_account(0)
            .await
            .expect("verify account zero in this test's own keyspace")
    };
    let (final_one, records_one) = store
        .verify_account(1)
        .await
        .expect("verify account one in this test's own keyspace");

    assert_eq!(final_one, cache_after_drain[1]);
    assert_eq!(records_one.len(), 1);
    assert_eq!(records_one[0].request, different_account);
    assert_eq!(records_one[0].result.outcome, Outcome::Applied);
    assert_eq!(records_one[0].write_clock, 1);

    assert_eq!(records_zero[0].request, first);
    assert_eq!(records_zero[0].result.balance, 110);
    assert_eq!(
        records_zero
            .iter()
            .filter(|record| record.request.tx_id == 1)
            .count(),
        1,
        "same-body replay and conflicting body must not add another ledger row"
    );
    assert_eq!(
        records_zero
            .iter()
            .filter(|record| record.request.tx_id == 14)
            .count(),
        1
    );
    assert_eq!(
        records_zero[13].result.outcome,
        Outcome::InsufficientFunds,
        "business rejection remains durable"
    );
    assert_eq!(records_zero[13].write_clock, 14);

    if let Some(container) = fault_container.as_deref() {
        assert!(service.failed());
        let (before_state, before_records) = before_outage
            .as_ref()
            .expect("owned-node fault must capture a pre-fault snapshot");
        assert_eq!(cache_after_drain[0], *before_state);
        assert!(records_zero.len() == 14 || records_zero.len() == 15);
        assert_eq!(
            records_zero.get(..before_records.len()).unwrap(),
            before_records.as_slice()
        );
        if records_zero.len() == 14 {
            assert_eq!(final_zero, *before_state);
        } else {
            let possible_commit = records_zero.last().unwrap();
            assert_eq!(possible_commit.request, tx(15, Operation::Credit, 1));
            assert_eq!(possible_commit.result.outcome, Outcome::Applied);
            assert_eq!(
                (
                    possible_commit.result.balance,
                    possible_commit.result.version
                ),
                (123, 15)
            );
            assert_eq!(possible_commit.write_clock, 15);
            assert_eq!(
                final_zero,
                AccountState {
                    balance: 123,
                    version: 15,
                    write_clock: 15
                }
            );
        }
        let expected_owner = std::env::var("SCYLLA_FAULT_OWNER")
            .expect("SCYLLA_FAULT_OWNER must be provided for an owned-node fault test");
        let actual_owner = docker_container_owner_nonce(container)
            .await
            .expect("confirm owner and nonce labels remain after node restart");
        assert_eq!(actual_owner, expected_owner);
    } else {
        assert!(!service.failed());
        assert_eq!(final_zero, cache_after_drain[0]);
        assert_eq!(records_zero.len(), 14);
    }

    let driver_after = store.driver_counters();
    assert_eq!(driver_after.retries, driver_before.retries);
    let counters = metrics.snapshot().counters;
    let expected_lwt_attempts = if fault_container.is_some() { 19 } else { 18 };
    assert_eq!(counters.get("cql_write_lwt"), Some(&expected_lwt_attempts));
    assert_eq!(
        counters.get("accepted_requests"),
        Some(&expected_lwt_attempts)
    );
    assert_eq!(counters.get("cql_read_metadata").copied().unwrap_or(0), 0);
    assert_eq!(counters.get("cql_read_ledger").copied().unwrap_or(0), 0);
    if fault_container.is_none() {
        assert_eq!(
            driver_after.queries.saturating_sub(driver_before.queries),
            22,
            "18 single-statement LWT attempts plus four post-window verification SELECTs"
        );
    }
}

async fn docker_container_owner_nonce(container: &str) -> std::io::Result<String> {
    let output = tokio::process::Command::new("docker")
        .args([
            "inspect",
            "--format",
            "{{ printf \"%s\\n%s\" (index .Config.Labels \"ledger-wallet.owner\") (index .Config.Labels \"ledger-wallet.nonce\") }}",
            container,
        ])
        .output()
        .await?;
    if !output.status.success() {
        return Err(std::io::Error::other(format!(
            "docker inspect failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    let output = String::from_utf8_lossy(&output.stdout);
    let mut lines = output.lines();
    let owner = lines.next().unwrap_or_default();
    let nonce = lines.next().unwrap_or_default();
    if owner != "scylla-wallet-runner" || nonce.is_empty() || lines.next().is_some() {
        return Err(std::io::Error::other(format!(
            "container ownership labels are invalid: owner={owner:?}, nonce={nonce:?}"
        )));
    }
    Ok(nonce.to_owned())
}

async fn docker_container_action(action: &str, container: &str) -> std::io::Result<()> {
    let output = tokio::process::Command::new("docker")
        .arg(action)
        .args((action == "kill").then_some("--signal=SIGKILL"))
        .arg(container)
        .output()
        .await?;
    if output.status.success() {
        Ok(())
    } else {
        Err(std::io::Error::other(format!(
            "docker {action} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )))
    }
}

fn validate_container_name(container: &str) {
    assert!(
        !container.is_empty()
            && container
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.')),
        "SCYLLA_FAULT_CONTAINER must be the runner-supplied container name"
    );
}

fn scylla_nodes() -> Vec<String> {
    let nodes = std::env::var("SCYLLA_NODES")
        .expect("SCYLLA_NODES must list three comma-separated node endpoints")
        .split(',')
        .map(str::trim)
        .filter(|node| !node.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    assert_eq!(
        nodes.len(),
        3,
        "SCYLLA_NODES must list exactly three endpoints"
    );
    nodes
}

fn fresh_keyspace(purpose: &str) -> String {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after Unix epoch")
        .as_nanos();
    let sequence = KEYSPACE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("wallet_bench_{purpose}_{nonce:x}_{sequence:x}")
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .map(|value| {
            value
                .parse()
                .unwrap_or_else(|error| panic!("invalid {name}: {error}"))
        })
        .unwrap_or(default)
}

fn tx(tx_id: u64, operation: Operation, amount: u64) -> Transaction {
    tx_for_account(0, tx_id, operation, amount)
}

fn tx_for_account(account_id: u64, tx_id: u64, operation: Operation, amount: u64) -> Transaction {
    Transaction {
        account_id,
        tx_id,
        operation,
        amount,
    }
}
