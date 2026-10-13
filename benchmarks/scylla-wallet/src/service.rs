use crate::metrics::Metrics;
use crate::store::WalletStore;
use crate::{
    AccountState, CommitResolution, Error, LedgerRecord, Operation, Outcome, Result, Transaction,
    TransactionResult,
};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, OwnedMutexGuard, OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::task::JoinHandle;

const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(120);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerRouting {
    RoundRobin,
    PerUser,
}

struct Job {
    request: Transaction,
    _owner_guard: OwnedMutexGuard<()>,
    _permit: OwnedSemaphorePermit,
    frozen_account: Arc<AtomicBool>,
    cached_state: Arc<StdMutex<AccountState>>,
    enqueued_at: Instant,
    reply: oneshot::Sender<(Result<TransactionResult>, Instant)>,
}

struct Control {
    closed: bool,
    senders: Vec<Option<mpsc::Sender<Job>>>,
}

struct WorkerShutdown {
    handles: VecDeque<JoinHandle<()>>,
    result: Option<Result<()>>,
}

pub struct Service {
    account_locks: Vec<Arc<Mutex<()>>>,
    frozen_accounts: Vec<Arc<AtomicBool>>,
    cached_states: Vec<Arc<StdMutex<AccountState>>>,
    admission: Arc<Semaphore>,
    metrics: Arc<Metrics>,
    next_worker: AtomicUsize,
    routing: WorkerRouting,
    failed: Arc<AtomicBool>,
    control: RwLock<Control>,
    workers: Mutex<WorkerShutdown>,
}

impl Service {
    pub fn start(
        store: Arc<dyn WalletStore>,
        workers: usize,
        initial_states: Vec<AccountState>,
        queue_capacity: usize,
        metrics: Arc<Metrics>,
    ) -> Result<Arc<Self>> {
        Self::start_with_routing(
            store,
            workers,
            initial_states,
            queue_capacity,
            queue_capacity,
            WorkerRouting::RoundRobin,
            metrics,
        )
    }

    pub fn start_with_routing(
        store: Arc<dyn WalletStore>,
        workers: usize,
        initial_states: Vec<AccountState>,
        global_queue_capacity: usize,
        per_worker_queue_capacity: usize,
        routing: WorkerRouting,
        metrics: Arc<Metrics>,
    ) -> Result<Arc<Self>> {
        if workers == 0 {
            return Err(Error::InvalidConfiguration(
                "worker count must be positive".to_owned(),
            ));
        }
        if initial_states.is_empty() || u64::try_from(initial_states.len()).is_err() {
            return Err(Error::InvalidConfiguration(
                "account count must be positive and fit in u64".to_owned(),
            ));
        }
        if global_queue_capacity == 0 || per_worker_queue_capacity == 0 {
            return Err(Error::InvalidConfiguration(
                "global and per-worker queue capacities must be positive".to_owned(),
            ));
        }
        if routing == WorkerRouting::PerUser && workers != initial_states.len() {
            return Err(Error::InvalidConfiguration(format!(
                "per-user routing requires worker count ({workers}) to equal account count ({})",
                initial_states.len()
            )));
        }
        if routing == WorkerRouting::PerUser && per_worker_queue_capacity != 1 {
            return Err(Error::InvalidConfiguration(
                "per-user routing requires per-worker queue capacity 1".to_owned(),
            ));
        }
        metrics.set_worker_count(workers);

        let locks = (0..initial_states.len())
            .map(|_| Arc::new(Mutex::new(())))
            .collect::<Vec<_>>();
        let frozen_accounts = (0..initial_states.len())
            .map(|_| Arc::new(AtomicBool::new(false)))
            .collect::<Vec<_>>();
        let cached_states = initial_states
            .into_iter()
            .map(|state| Arc::new(StdMutex::new(state)))
            .collect::<Vec<_>>();
        let failed = Arc::new(AtomicBool::new(false));
        let mut senders = Vec::with_capacity(workers);
        let mut worker_handles = Vec::with_capacity(workers);

        for _ in 0..workers {
            // The global semaphore bounds all queued and active jobs together.
            // Per-user mode keeps only one waiting job in each routed channel.
            let (sender, receiver) = mpsc::channel(per_worker_queue_capacity);
            senders.push(Some(sender));
            worker_handles.push(tokio::spawn(worker_loop(
                store.clone(),
                receiver,
                failed.clone(),
                metrics.clone(),
            )));
        }

        Ok(Arc::new(Self {
            account_locks: locks,
            frozen_accounts,
            cached_states,
            admission: Arc::new(Semaphore::new(global_queue_capacity)),
            metrics,
            next_worker: AtomicUsize::new(0),
            routing,
            failed,
            control: RwLock::new(Control {
                closed: false,
                senders,
            }),
            workers: Mutex::new(WorkerShutdown {
                handles: worker_handles.into(),
                result: None,
            }),
        }))
    }

    pub async fn submit(&self, request: Transaction) -> Result<TransactionResult> {
        let account_index = usize::try_from(request.account_id).map_err(|_| {
            Error::NotAccepted("account id is outside the configured account range".to_owned())
        })?;
        if account_index >= self.account_locks.len() {
            return Err(Error::NotAccepted(
                "account id is outside the configured account range".to_owned(),
            ));
        }
        if self.frozen_accounts[account_index].load(Ordering::Acquire) {
            return Err(Error::Frozen(format!(
                "account {} has an unknown earlier commit",
                request.account_id
            )));
        }
        if i64::try_from(request.account_id).is_err() || i64::try_from(request.tx_id).is_err() {
            return Err(Error::NotAccepted(
                "database BIGINT key is outside the non-negative i64 range".to_owned(),
            ));
        }
        if self.failed.load(Ordering::Acquire) {
            return Err(Error::NotAccepted(
                "trial has failed closed after a storage failure".to_owned(),
            ));
        }

        let admission_wait_started = Instant::now();
        let permit = self
            .admission
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Closed)?;
        self.metrics
            .observe("admission_wait", admission_wait_started.elapsed());
        if self.failed.load(Ordering::Acquire) {
            return Err(Error::NotAccepted(
                "trial has failed closed after a storage failure".to_owned(),
            ));
        }

        let account_wait_started = Instant::now();
        let owner_guard = self.account_locks[account_index].clone().lock_owned().await;
        self.metrics
            .observe("account_wait", account_wait_started.elapsed());

        if self.frozen_accounts[account_index].load(Ordering::Acquire) {
            return Err(Error::Frozen(format!(
                "account {} has an unknown earlier commit",
                request.account_id
            )));
        }
        if self.failed.load(Ordering::Acquire) {
            return Err(Error::NotAccepted(
                "trial has failed closed after a storage failure".to_owned(),
            ));
        }

        let (reply, response) = oneshot::channel();
        let job = Job {
            request,
            _owner_guard: owner_guard,
            _permit: permit,
            frozen_account: self.frozen_accounts[account_index].clone(),
            cached_state: self.cached_states[account_index].clone(),
            enqueued_at: Instant::now(),
            reply,
        };

        {
            let control = self
                .control
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if control.closed {
                return Err(Error::Closed);
            }
            let worker_index = select_worker_index(
                self.routing,
                account_index,
                control.senders.len(),
                &self.next_worker,
            );
            let sender = control
                .senders
                .get(worker_index)
                .and_then(Option::as_ref)
                .ok_or(Error::Closed)?;
            match sender.try_send(job) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    self.metrics
                        .increment_counter("infrastructure_worker_queue_full");
                    return Err(Error::NotAccepted(
                        "worker queue is full despite the global admission bound".to_owned(),
                    ));
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    self.failed.store(true, Ordering::Release);
                    return Err(Error::Closed);
                }
            }
        }
        self.metrics.increment_counter("accepted_requests");

        match response.await {
            Ok((result, sent_at)) => {
                self.metrics.observe("reply", sent_at.elapsed());
                result
            }
            Err(_) => {
                self.failed.store(true, Ordering::Release);
                self.frozen_accounts[account_index].store(true, Ordering::Release);
                Err(Error::CommitOutcomeUnknown(
                    "worker ended before returning an accepted request result".to_owned(),
                ))
            }
        }
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.admission.close();
        let senders = {
            let mut control = self
                .control
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            control.closed = true;
            std::mem::take(&mut control.senders)
        };
        drop(senders);

        let mut shutdown = self.workers.lock().await;
        if let Some(result) = &shutdown.result {
            return result.clone();
        }
        let deadline = tokio::time::Instant::now() + SHUTDOWN_DEADLINE;
        while !shutdown.handles.is_empty() {
            self.metrics.increment_counter("shutdown_drain_wait");
            let result = tokio::time::timeout_at(
                deadline,
                shutdown.handles.front_mut().expect("queue is not empty"),
            )
            .await;
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    self.failed.store(true, Ordering::Release);
                    shutdown.handles.pop_front();
                    abort_and_join(&mut shutdown.handles).await;
                    let result = Err(Error::Storage(format!(
                        "worker task failed during shutdown: {error}"
                    )));
                    shutdown.result = Some(result.clone());
                    return result;
                }
                Err(_) => {
                    self.failed.store(true, Ordering::Release);
                    abort_and_join(&mut shutdown.handles).await;
                    let result = Err(Error::CommitOutcomeUnknown(
                        "shutdown deadline elapsed while draining accepted work".to_owned(),
                    ));
                    shutdown.result = Some(result.clone());
                    return result;
                }
            }
            // The completed JoinHandle remains in shared state until this synchronous pop.
            // Cancellation while awaiting a worker therefore leaves it available for retry.
            shutdown.handles.pop_front();
        }
        let result = Ok(());
        shutdown.result = Some(result.clone());
        result
    }

    pub fn failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    /// Captures bounded per-account cache state. Call after shutdown has drained workers.
    pub fn cache_snapshot(&self) -> Vec<AccountState> {
        self.cached_states
            .iter()
            .map(|state| *lock_std(state))
            .collect()
    }
}

fn lock_std<T>(mutex: &StdMutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn select_worker_index(
    routing: WorkerRouting,
    account_index: usize,
    workers: usize,
    next_worker: &AtomicUsize,
) -> usize {
    match routing {
        WorkerRouting::RoundRobin => next_worker.fetch_add(1, Ordering::Relaxed) % workers,
        WorkerRouting::PerUser => account_index,
    }
}

#[cfg(test)]
mod routing_tests {
    use super::{WorkerRouting, select_worker_index};
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn per_user_route_is_fixed_even_when_round_robin_counter_advances() {
        let next_worker = AtomicUsize::new(917);
        for _ in 0..4 {
            assert_eq!(
                select_worker_index(WorkerRouting::PerUser, 7, 32, &next_worker),
                7
            );
        }
        assert_eq!(next_worker.load(std::sync::atomic::Ordering::Relaxed), 917);
    }

    #[test]
    fn round_robin_route_remains_the_legacy_default_behavior() {
        let next_worker = AtomicUsize::new(2);
        assert_eq!(
            select_worker_index(WorkerRouting::RoundRobin, 7, 4, &next_worker),
            2
        );
        assert_eq!(
            select_worker_index(WorkerRouting::RoundRobin, 7, 4, &next_worker),
            3
        );
        assert_eq!(
            select_worker_index(WorkerRouting::RoundRobin, 7, 4, &next_worker),
            0
        );
    }
}

async fn worker_loop(
    store: Arc<dyn WalletStore>,
    mut receiver: mpsc::Receiver<Job>,
    failed: Arc<AtomicBool>,
    metrics: Arc<Metrics>,
) {
    while let Some(Job {
        request,
        _owner_guard,
        _permit,
        frozen_account,
        cached_state,
        enqueued_at,
        reply,
    }) = receiver.recv().await
    {
        metrics.observe("db_queue_wait", enqueued_at.elapsed());
        let _worker_activity = metrics.worker_enter();
        if failed.load(Ordering::Acquire) {
            let _ = reply.send((
                Err(Error::NotAccepted(
                    "trial has failed closed after a storage failure".to_owned(),
                )),
                Instant::now(),
            ));
            drop((_owner_guard, _permit));
            continue;
        }
        let result = process_job(store.as_ref(), &cached_state, &request, &metrics).await;
        if matches!(&result, Err(Error::CommitOutcomeUnknown(_))) {
            failed.store(true, Ordering::Release);
            frozen_account.store(true, Ordering::Release);
        } else if matches!(&result, Err(Error::Storage(_) | Error::Corruption(_))) {
            failed.store(true, Ordering::Release);
            frozen_account.store(true, Ordering::Release);
        }
        let _ = reply.send((result, Instant::now()));
        drop((_owner_guard, _permit));
    }
}

async fn abort_and_join(handles: &mut VecDeque<JoinHandle<()>>) {
    for handle in handles.iter() {
        handle.abort();
    }
    while let Some(handle) = handles.front_mut() {
        let _ = handle.await;
        handles.pop_front();
    }
}

async fn process_job(
    store: &dyn WalletStore,
    cached_state: &StdMutex<AccountState>,
    request: &Transaction,
    metrics: &Metrics,
) -> Result<TransactionResult> {
    // Owner serialization protects the cache through commit, response handling, and reply.
    // The standard mutex is held only for this copy, never across an await.
    let current = *lock_std(cached_state);

    let compute_started = Instant::now();
    let new_version = current
        .version
        .checked_add(1)
        .filter(|version| *version <= i64::MAX as u64)
        .ok_or_else(|| Error::Corruption("account version exceeded database BIGINT".to_owned()))?;
    let (outcome, balance) = if request.amount == 0 {
        (Outcome::InvalidAmount, current.balance)
    } else {
        match request.operation {
            Operation::Credit => match current.balance.checked_add(request.amount) {
                Some(balance) => (Outcome::Applied, balance),
                None => (Outcome::CreditOverflow, current.balance),
            },
            Operation::Debit if request.amount > current.balance => {
                (Outcome::InsufficientFunds, current.balance)
            }
            Operation::Debit => (Outcome::Applied, current.balance - request.amount),
        }
    };
    let write_clock = current.write_clock.checked_add(1).ok_or_else(|| {
        Error::Corruption("logical account clock exhausted database BIGINT".to_owned())
    })?;
    metrics.observe("compute", compute_started.elapsed());

    let next_state = AccountState {
        balance,
        version: new_version,
        write_clock,
    };
    let record = LedgerRecord {
        request: request.clone(),
        result: TransactionResult {
            outcome,
            balance,
            version: new_version,
            replayed: false,
        },
        write_clock,
    };
    let write_started = Instant::now();
    let committed = store
        .commit(request.account_id, &current, &next_state, &record)
        .await;
    metrics.observe("db_write", write_started.elapsed());
    match committed {
        Ok(CommitResolution::Applied) => {
            // Publish only after the regular write-CL acknowledgement returns.
            *lock_std(cached_state) = next_state;
            Ok(record.result)
        }
        Ok(CommitResolution::Existing {
            current: returned_state,
            record: existing,
        }) => {
            // A failed IF NOT EXISTS returns the current static state and the original
            // ledger row. The ledger may be older after later transactions, while static
            // state must still exactly match this account's memory cache.
            if returned_state != current {
                return Err(Error::Corruption(format!(
                    "account {} conditional-write response static state differs from memory cache",
                    request.account_id
                )));
            }
            if existing.result.version > current.version
                || existing.write_clock > current.write_clock
                || (existing.result.version == current.version
                    && (existing.result.balance != current.balance
                        || existing.write_clock != current.write_clock))
            {
                return Err(Error::Corruption(format!(
                    "ledger row for account {} is ahead of or inconsistent with cached state",
                    request.account_id
                )));
            }
            if existing.request != *request {
                return Err(Error::Conflict(format!(
                    "key ({}, {}) already contains a different request body",
                    request.account_id, request.tx_id
                )));
            }
            let mut replay_result = existing.result;
            replay_result.replayed = true;
            // A confirmed duplicate returns its original result without another wallet-row
            // mutation. Storage-engine protocol work is outside this service result.
            Ok(replay_result)
        }
        // The store marks every post-submission transport or response-decode failure as
        // unknown. Pre-submission validation failures retain their original type.
        Err(error) => Err(error),
    }
}
