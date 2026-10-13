use async_trait::async_trait;
use ledger_wallet_scylla_bench::{
    AccountState, CommitResolution, Error, LedgerRecord, Metrics, Operation, Outcome, Result,
    Service, Transaction, TransactionResult, WalletStore, WorkerRouting,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tokio::sync::{Mutex, Semaphore, mpsc};

#[derive(Default)]
struct MockData {
    accounts: HashMap<u64, AccountState>,
    ledger: HashMap<(u64, u64), LedgerRecord>,
}

struct MockStore {
    data: Mutex<MockData>,
    active_commits: AtomicUsize,
    peak_commits: AtomicUsize,
    writes: AtomicUsize,
    active_by_account: Arc<StdMutex<HashMap<u64, usize>>>,
    peak_account_commits: AtomicUsize,
    unknown_next_write: AtomicBool,
    confirmed_rollback_next_write: AtomicBool,
    corrupt_existing_next: AtomicBool,
    gated_commits: AtomicBool,
    release_commit: Arc<Semaphore>,
    commit_started: mpsc::UnboundedSender<u64>,
    delay: Duration,
}

struct Activity<'a> {
    current: &'a AtomicUsize,
}

impl Activity<'_> {
    fn enter<'a>(current: &'a AtomicUsize, peak: &'a AtomicUsize) -> Activity<'a> {
        let active = current.fetch_add(1, Ordering::AcqRel) + 1;
        atomic_max(peak, active);
        Activity { current }
    }
}

impl Drop for Activity<'_> {
    fn drop(&mut self) {
        self.current.fetch_sub(1, Ordering::AcqRel);
    }
}

struct AccountActivity {
    active: Arc<StdMutex<HashMap<u64, usize>>>,
    account_id: u64,
}

impl AccountActivity {
    fn enter(
        active: Arc<StdMutex<HashMap<u64, usize>>>,
        account_id: u64,
        peak: &AtomicUsize,
    ) -> Self {
        let count = {
            let mut by_account = active.lock().unwrap();
            let count = by_account.entry(account_id).or_default();
            *count += 1;
            *count
        };
        atomic_max(peak, count);
        Self { active, account_id }
    }
}

impl Drop for AccountActivity {
    fn drop(&mut self) {
        let mut by_account = self.active.lock().unwrap();
        let count = by_account.get_mut(&self.account_id).unwrap();
        *count -= 1;
        if *count == 0 {
            by_account.remove(&self.account_id);
        }
    }
}

impl MockStore {
    fn new(accounts: &[(u64, u64)], delay: Duration) -> (Arc<Self>, mpsc::UnboundedReceiver<u64>) {
        let mut data = MockData::default();
        for (account_id, balance) in accounts {
            data.accounts.insert(
                *account_id,
                AccountState {
                    balance: *balance,
                    version: 0,
                    write_clock: 0,
                },
            );
        }
        let (commit_started, receiver) = mpsc::unbounded_channel();
        (
            Arc::new(Self {
                data: Mutex::new(data),
                active_commits: AtomicUsize::new(0),
                peak_commits: AtomicUsize::new(0),
                writes: AtomicUsize::new(0),
                active_by_account: Arc::new(StdMutex::new(HashMap::new())),
                peak_account_commits: AtomicUsize::new(0),
                unknown_next_write: AtomicBool::new(false),
                confirmed_rollback_next_write: AtomicBool::new(false),
                corrupt_existing_next: AtomicBool::new(false),
                gated_commits: AtomicBool::new(false),
                release_commit: Arc::new(Semaphore::new(0)),
                commit_started,
                delay,
            }),
            receiver,
        )
    }

    async fn initial_states(&self) -> Vec<AccountState> {
        let data = self.data.lock().await;
        (0..data.accounts.len())
            .map(|account| *data.accounts.get(&(account as u64)).unwrap())
            .collect()
    }

    async fn snapshot(&self, account_id: u64) -> (AccountState, Vec<LedgerRecord>) {
        let data = self.data.lock().await;
        let state = *data.accounts.get(&account_id).expect("account is seeded");
        let mut ledger = data
            .ledger
            .iter()
            .filter(|((account, _), _)| *account == account_id)
            .map(|(_, record)| record.clone())
            .collect::<Vec<_>>();
        ledger.sort_by_key(|record| record.result.version);
        (state, ledger)
    }

    fn enable_commit_gate(&self) {
        self.gated_commits.store(true, Ordering::Release);
    }

    fn release_one_commit(&self) {
        self.release_commit.add_permits(1);
    }
}

#[async_trait]
impl WalletStore for MockStore {
    async fn preload_state(&self, account_id: u64) -> Result<AccountState> {
        self.data
            .lock()
            .await
            .accounts
            .get(&account_id)
            .copied()
            .ok_or_else(|| Error::Corruption(format!("missing preload account {account_id}")))
    }

    async fn commit(
        &self,
        account_id: u64,
        expected_state: &AccountState,
        next_state: &AccountState,
        record: &LedgerRecord,
    ) -> Result<CommitResolution> {
        let _active = Activity::enter(&self.active_commits, &self.peak_commits);
        let _account = AccountActivity::enter(
            self.active_by_account.clone(),
            account_id,
            &self.peak_account_commits,
        );
        self.writes.fetch_add(1, Ordering::Relaxed);

        if self.gated_commits.load(Ordering::Acquire) {
            let _ = self.commit_started.send(account_id);
            self.release_commit
                .clone()
                .acquire_owned()
                .await
                .expect("test commit semaphore remains open")
                .forget();
        }
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }

        if self
            .confirmed_rollback_next_write
            .swap(false, Ordering::AcqRel)
        {
            return Err(Error::CommitConfirmedRollback(
                "mock server confirmed the implicit transaction rolled back".to_owned(),
            ));
        }

        let resolution = {
            let mut data = self.data.lock().await;
            let actual = data
                .accounts
                .get(&account_id)
                .copied()
                .ok_or_else(|| Error::Corruption(format!("missing account {account_id}")))?;
            if actual != *expected_state {
                return Err(Error::Corruption(format!(
                    "mock static state differs from cache for account {account_id}"
                )));
            }
            let key = (account_id, record.request.tx_id);
            if let Some(existing) = data.ledger.get(&key).cloned() {
                let current = if self.corrupt_existing_next.swap(false, Ordering::AcqRel) {
                    AccountState {
                        balance: actual.balance.saturating_add(1),
                        ..actual
                    }
                } else {
                    actual
                };
                CommitResolution::Existing {
                    current,
                    record: existing,
                }
            } else {
                data.accounts.insert(account_id, *next_state);
                data.ledger.insert(key, record.clone());
                CommitResolution::Applied
            }
        };

        if self.unknown_next_write.swap(false, Ordering::AcqRel) {
            return Err(Error::CommitOutcomeUnknown(
                "mock applied the conditional insert but lost its response".to_owned(),
            ));
        }
        Ok(resolution)
    }
}

fn transaction(account_id: u64, tx_id: u64, operation: Operation, amount: u64) -> Transaction {
    Transaction {
        account_id,
        tx_id,
        operation,
        amount,
    }
}

fn service(
    store: Arc<MockStore>,
    workers: usize,
    initial_states: Vec<AccountState>,
    global_queue: usize,
) -> Arc<Service> {
    Service::start(
        store,
        workers,
        initial_states,
        global_queue,
        Arc::new(Metrics::new()),
    )
    .unwrap()
}

fn atomic_max(target: &AtomicUsize, value: usize) {
    let mut current = target.load(Ordering::Relaxed);
    while value > current {
        match target.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(actual) => current = actual,
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_account_callers_are_serialized_around_cache_and_conditional_insert() {
    let (store, _) = MockStore::new(&[(0, 0)], Duration::from_millis(4));
    let service = service(store.clone(), 4, store.initial_states().await, 16);
    let mut callers = Vec::new();
    for id in 1..=12 {
        let service = service.clone();
        callers.push(tokio::spawn(async move {
            service
                .submit(transaction(0, id, Operation::Credit, 1))
                .await
        }));
    }
    let mut versions = Vec::new();
    for caller in callers {
        let result = caller.await.unwrap().unwrap();
        assert_eq!(result.outcome, Outcome::Applied);
        versions.push(result.version);
    }
    versions.sort_unstable();
    assert_eq!(versions, (1..=12).collect::<Vec<_>>());
    let (state, ledger) = store.snapshot(0).await;
    assert_eq!(
        state,
        AccountState {
            balance: 12,
            version: 12,
            write_clock: 12
        }
    );
    assert_eq!(ledger.len(), 12);
    assert_eq!(store.peak_account_commits.load(Ordering::Relaxed), 1);
    assert_eq!(service.cache_snapshot()[0], state);
    service.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distinct_accounts_can_wait_in_the_store_concurrently_within_worker_limit() {
    let seeded = (0..12).map(|id| (id, 10)).collect::<Vec<_>>();
    let (store, _) = MockStore::new(&seeded, Duration::from_millis(15));
    let service = service(store.clone(), 3, store.initial_states().await, 12);
    let mut callers = Vec::new();
    for account_id in 0..12 {
        let service = service.clone();
        callers.push(tokio::spawn(async move {
            service
                .submit(transaction(account_id, 1, Operation::Credit, 1))
                .await
        }));
    }
    for caller in callers {
        assert_eq!(caller.await.unwrap().unwrap().outcome, Outcome::Applied);
    }
    let peak = store.peak_commits.load(Ordering::Relaxed);
    assert!(
        peak > 1,
        "expected parallel conditional inserts, peak was {peak}"
    );
    assert!(peak <= 3, "worker count is the storage concurrency ceiling");
    service.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_user_mode_preserves_owner_serialization_and_cross_user_parallelism() {
    let (store, _) = MockStore::new(&[(0, 10), (1, 10)], Duration::from_millis(12));
    let service = Service::start_with_routing(
        store.clone(),
        2,
        store.initial_states().await,
        4,
        1,
        WorkerRouting::PerUser,
        Arc::new(Metrics::new()),
    )
    .unwrap();
    let mut callers = Vec::new();
    for account_id in 0..2 {
        for tx_id in 1..=2 {
            let service = service.clone();
            callers.push(tokio::spawn(async move {
                service
                    .submit(transaction(account_id, tx_id, Operation::Credit, 1))
                    .await
            }));
        }
    }
    for caller in callers {
        assert_eq!(caller.await.unwrap().unwrap().outcome, Outcome::Applied);
    }
    assert_eq!(store.peak_account_commits.load(Ordering::Relaxed), 1);
    assert!(store.peak_commits.load(Ordering::Relaxed) > 1);
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn replay_after_later_transaction_returns_original_without_cache_rollback() {
    let (store, _) = MockStore::new(&[(0, 100)], Duration::ZERO);
    let service = service(store.clone(), 2, store.initial_states().await, 8);
    let request = transaction(0, 7, Operation::Credit, 5);
    let first = service.submit(request.clone()).await.unwrap();
    let later = service
        .submit(transaction(0, 8, Operation::Debit, 2))
        .await
        .unwrap();
    let before_replay = service.cache_snapshot()[0];
    let writes_before_replay = store.writes.load(Ordering::Relaxed);

    let replay = service.submit(request.clone()).await.unwrap();
    assert_eq!(replay, first.clone().with_replayed());
    assert_eq!(later.version, 2);
    assert_eq!(service.cache_snapshot()[0], before_replay);
    assert_eq!(store.snapshot(0).await.0, before_replay);
    assert_eq!(
        store.writes.load(Ordering::Relaxed),
        writes_before_replay + 1,
        "replay is one conditional request but does not mutate wallet rows"
    );

    let mut different_body = request;
    different_body.amount = 6;
    assert!(matches!(
        service.submit(different_body).await,
        Err(Error::Conflict(_))
    ));
    assert_eq!(service.cache_snapshot()[0], before_replay);
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn same_txid_is_partition_scoped_across_accounts() {
    let (store, _) = MockStore::new(&[(0, 10), (1, 20)], Duration::ZERO);
    let service = service(store.clone(), 2, store.initial_states().await, 4);
    let first = service
        .submit(transaction(0, 1, Operation::Credit, 1))
        .await
        .unwrap();
    let second = service
        .submit(transaction(1, 1, Operation::Debit, 1))
        .await
        .unwrap();
    assert_eq!((first.balance, second.balance), (11, 19));
    assert_eq!(store.snapshot(0).await.1.len(), 1);
    assert_eq!(store.snapshot(1).await.1.len(), 1);
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn business_rejections_are_recorded_and_replayed_from_original_result() {
    let (store, _) = MockStore::new(&[(0, 3)], Duration::ZERO);
    let service = service(store.clone(), 1, store.initial_states().await, 4);
    let debit = transaction(0, 1, Operation::Debit, 5);
    let rejected = service.submit(debit.clone()).await.unwrap();
    assert_eq!(rejected.outcome, Outcome::InsufficientFunds);
    assert_eq!((rejected.balance, rejected.version), (3, 1));
    let next = service
        .submit(transaction(0, 2, Operation::Credit, 0))
        .await
        .unwrap();
    assert_eq!(next.outcome, Outcome::InvalidAmount);
    assert_eq!(next.version, 2);
    let replay = service.submit(debit).await.unwrap();
    assert_eq!(replay.outcome, Outcome::InsufficientFunds);
    assert_eq!(replay.version, 1);
    assert!(replay.replayed);
    let (state, records) = store.snapshot(0).await;
    assert_eq!((state.balance, state.version, state.write_clock), (3, 2, 2));
    assert_eq!(records.len(), 2);
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn credit_overflow_is_recorded_as_a_business_rejection() {
    let (store, _) = MockStore::new(&[(0, u64::MAX)], Duration::ZERO);
    let service = service(store.clone(), 1, store.initial_states().await, 2);
    let result = service
        .submit(transaction(0, 1, Operation::Credit, 1))
        .await
        .unwrap();
    assert_eq!(result.outcome, Outcome::CreditOverflow);
    assert_eq!((result.balance, result.version), (u64::MAX, 1));
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn unknown_commit_does_not_advance_cache_and_freezes_account_and_trial() {
    let (store, _) = MockStore::new(&[(0, 10), (1, 10)], Duration::ZERO);
    store.unknown_next_write.store(true, Ordering::Release);
    let service = service(store.clone(), 2, store.initial_states().await, 4);
    let initial_cache = service.cache_snapshot();
    assert!(matches!(
        service
            .submit(transaction(0, 1, Operation::Credit, 1))
            .await,
        Err(Error::CommitOutcomeUnknown(_))
    ));
    assert!(service.failed());
    assert_eq!(service.cache_snapshot(), initial_cache);
    assert_ne!(store.snapshot(0).await.0, initial_cache[0]);
    assert!(matches!(
        service
            .submit(transaction(0, 2, Operation::Credit, 1))
            .await,
        Err(Error::Frozen(_))
    ));
    assert!(matches!(
        service
            .submit(transaction(1, 1, Operation::Credit, 1))
            .await,
        Err(Error::NotAccepted(_))
    ));
    assert_eq!(store.writes.load(Ordering::Relaxed), 1);
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn confirmed_rollback_does_not_freeze_account_or_trial() {
    let (store, _) = MockStore::new(&[(0, 10)], Duration::ZERO);
    store
        .confirmed_rollback_next_write
        .store(true, Ordering::Release);
    let service = service(store.clone(), 1, store.initial_states().await, 4);
    let request = transaction(0, 1, Operation::Credit, 1);
    assert!(matches!(
        service.submit(request.clone()).await,
        Err(Error::CommitConfirmedRollback(_))
    ));
    assert!(!service.failed());
    assert_eq!(
        service.cache_snapshot()[0],
        AccountState {
            balance: 10,
            version: 0,
            write_clock: 0
        }
    );

    let retry = service
        .submit(request)
        .await
        .expect("a caller may retry after the server confirmed rollback");
    assert_eq!((retry.balance, retry.version), (11, 1));
    assert!(!retry.replayed);
    assert!(!service.failed());
    service.shutdown().await.unwrap();
    assert_eq!(store.snapshot(0).await.0.balance, 11);
}

#[tokio::test]
async fn caller_cancellation_does_not_cancel_commit_or_cache_publication() {
    let (store, mut started) = MockStore::new(&[(0, 0)], Duration::ZERO);
    store.enable_commit_gate();
    let service = service(store.clone(), 2, store.initial_states().await, 4);
    let first_service = service.clone();
    let first = tokio::spawn(async move {
        first_service
            .submit(transaction(0, 1, Operation::Credit, 1))
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), started.recv())
        .await
        .unwrap()
        .unwrap();
    first.abort();

    let second_service = service.clone();
    let second = tokio::spawn(async move {
        second_service
            .submit(transaction(0, 2, Operation::Credit, 1))
            .await
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(store.writes.load(Ordering::Relaxed), 1);
    assert_eq!(
        service.cache_snapshot()[0],
        AccountState {
            balance: 0,
            version: 0,
            write_clock: 0
        },
        "cache publication must wait for the LWT acknowledgement"
    );
    store.release_one_commit();
    tokio::time::timeout(Duration::from_secs(1), started.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(store.snapshot(0).await.0.version, 1);
    store.release_one_commit();
    assert_eq!(second.await.unwrap().unwrap().balance, 2);
    assert_eq!(service.cache_snapshot()[0].version, 2);
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_drains_accepted_work_and_closes_admission() {
    let (store, mut started) = MockStore::new(&[(0, 0)], Duration::ZERO);
    store.enable_commit_gate();
    let service = service(store.clone(), 1, store.initial_states().await, 4);
    let caller_service = service.clone();
    let caller = tokio::spawn(async move {
        caller_service
            .submit(transaction(0, 1, Operation::Credit, 1))
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), started.recv())
        .await
        .unwrap()
        .unwrap();

    let shutdown_service = service.clone();
    let shutdown = tokio::spawn(async move { shutdown_service.shutdown().await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !shutdown.is_finished(),
        "shutdown should wait for accepted work"
    );
    assert!(matches!(
        service
            .submit(transaction(0, 2, Operation::Credit, 1))
            .await,
        Err(Error::Closed)
    ));

    store.release_one_commit();
    assert_eq!(caller.await.unwrap().unwrap().balance, 1);
    shutdown.await.unwrap().unwrap();
    assert_eq!(
        service.cache_snapshot()[0],
        AccountState {
            balance: 1,
            version: 1,
            write_clock: 1
        }
    );
    assert!(matches!(
        service
            .submit(transaction(0, 3, Operation::Credit, 1))
            .await,
        Err(Error::Closed)
    ));
}

#[tokio::test]
async fn cancelled_shutdown_can_be_retried_and_still_drains_accepted_work() {
    let (store, mut started) = MockStore::new(&[(0, 0)], Duration::ZERO);
    store.enable_commit_gate();
    let service = service(store.clone(), 1, store.initial_states().await, 4);
    let caller_service = service.clone();
    let caller = tokio::spawn(async move {
        caller_service
            .submit(transaction(0, 1, Operation::Credit, 1))
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), started.recv())
        .await
        .unwrap()
        .unwrap();

    let shutdown_service = service.clone();
    let first_shutdown = tokio::spawn(async move { shutdown_service.shutdown().await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!first_shutdown.is_finished());
    first_shutdown.abort();
    assert!(first_shutdown.await.unwrap_err().is_cancelled());
    assert!(matches!(
        service
            .submit(transaction(0, 2, Operation::Credit, 1))
            .await,
        Err(Error::Closed)
    ));

    store.release_one_commit();
    assert_eq!(caller.await.unwrap().unwrap().balance, 1);
    tokio::time::timeout(Duration::from_secs(1), service.shutdown())
        .await
        .expect("retrying shutdown should finish after accepted work drains")
        .unwrap();
    assert_eq!(
        service.cache_snapshot()[0],
        AccountState {
            balance: 1,
            version: 1,
            write_clock: 1
        }
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_user_mode_obeys_the_global_admission_bound() {
    let seeded = (0..4).map(|account| (account, 0)).collect::<Vec<_>>();
    let (store, mut started) = MockStore::new(&seeded, Duration::ZERO);
    store.enable_commit_gate();
    let service = Service::start_with_routing(
        store.clone(),
        4,
        store.initial_states().await,
        2,
        1,
        WorkerRouting::PerUser,
        Arc::new(Metrics::new()),
    )
    .unwrap();

    let callers = (0..4)
        .map(|account| {
            let service = service.clone();
            tokio::spawn(async move {
                service
                    .submit(transaction(account, 1, Operation::Credit, 1))
                    .await
            })
        })
        .collect::<Vec<_>>();
    for _ in 0..2 {
        tokio::time::timeout(Duration::from_secs(1), started.recv())
            .await
            .expect("two jobs should fit the global admission bound")
            .expect("worker should start the admitted job");
    }
    assert_eq!(store.writes.load(Ordering::Relaxed), 2);
    assert!(
        tokio::time::timeout(Duration::from_millis(40), started.recv())
            .await
            .is_err(),
        "no third per-user job may reach storage before global admission frees a permit"
    );

    store.release_commit.add_permits(4);
    for caller in callers {
        assert_eq!(caller.await.unwrap().unwrap().outcome, Outcome::Applied);
    }
    assert_eq!(store.writes.load(Ordering::Relaxed), 4);
    assert_eq!(
        store.peak_commits.load(Ordering::Relaxed),
        2,
        "the global admission semaphore bounds active and queued jobs"
    );
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn cache_disagreement_in_existing_lwt_response_freezes_without_advancing_cache() {
    let (store, _) = MockStore::new(&[(0, 10)], Duration::ZERO);
    store.corrupt_existing_next.store(true, Ordering::Release);
    let initial_state = store.initial_states().await;
    // Install the corresponding already-existing ledger row, then trigger the malformed
    // current static-state value in the simulated IF NOT EXISTS response.
    {
        let mut data = store.data.lock().await;
        let request = transaction(0, 1, Operation::Credit, 1);
        let next = TransactionResult {
            outcome: Outcome::Applied,
            balance: 11,
            version: 1,
            replayed: false,
        };
        data.ledger.insert(
            (0, 1),
            LedgerRecord {
                request,
                result: next,
                write_clock: 1,
            },
        );
    }
    let mut cached = initial_state;
    cached[0] = AccountState {
        balance: 10,
        version: 0,
        write_clock: 0,
    };
    let service = Service::start(
        store.clone(),
        1,
        cached.clone(),
        2,
        Arc::new(Metrics::new()),
    )
    .unwrap();
    assert!(matches!(
        service
            .submit(transaction(0, 1, Operation::Credit, 1))
            .await,
        Err(Error::Corruption(_))
    ));
    assert!(service.failed());
    assert_eq!(service.cache_snapshot(), cached);
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn per_user_routing_and_global_admission_configuration_are_validated() {
    let (store, _) = MockStore::new(&[(0, 10), (1, 10)], Duration::ZERO);
    let metrics = Arc::new(Metrics::new());
    let states = store.initial_states().await;
    assert!(matches!(
        Service::start_with_routing(
            store.clone(),
            2,
            states.clone(),
            2,
            1,
            WorkerRouting::PerUser,
            metrics.clone(),
        ),
        Ok(_)
    ));
    assert!(matches!(
        Service::start_with_routing(
            store.clone(),
            1,
            states.clone(),
            2,
            1,
            WorkerRouting::PerUser,
            metrics.clone(),
        ),
        Err(Error::InvalidConfiguration(_))
    ));
    assert!(matches!(
        Service::start_with_routing(store, 2, states, 2, 2, WorkerRouting::PerUser, metrics),
        Err(Error::InvalidConfiguration(_))
    ));
}

trait Replayed {
    fn with_replayed(self) -> Self;
}

impl Replayed for TransactionResult {
    fn with_replayed(mut self) -> Self {
        self.replayed = true;
        self
    }
}
