use ledger_wallet_scylla_bench::{
    AccountState, Error, LedgerRecord, Metrics, Operation, Outcome, PostgresStore, Service,
    Transaction, TransactionResult, WalletStore,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_postgres::NoTls;

static SCHEMA_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an owned PostgreSQL 18 walletbench database; set LEDGER_WALLET_PG_DSN and run with --ignored"]
async fn postgres_atomic_commit_replay_conflict_and_full_verification() {
    let dsn = std::env::var("LEDGER_WALLET_PG_DSN")
        .expect("set LEDGER_WALLET_PG_DSN to the owned walletbench database");
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after the epoch")
        .as_nanos();
    let sequence = SCHEMA_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let schema = format!("wallet_bench_test_{now}_{sequence}");
    let metrics = Arc::new(Metrics::new());
    let store = PostgresStore::connect(&dsn, &schema, 6, metrics.clone())
        .await
        .expect("create a fresh owned PostgreSQL wallet schema");
    assert_eq!(store.data_connections(), 6);

    // The SQL function inserts ledger first; a missing account must raise and roll that
    // insert back with the same implicit transaction.
    let initial = AccountState {
        balance: 100,
        version: 0,
        write_clock: 0,
    };
    let missing_account_next = AccountState {
        balance: 101,
        version: 1,
        write_clock: 1,
    };
    let missing_account_record = LedgerRecord {
        request: Transaction {
            account_id: 0,
            tx_id: 1,
            operation: Operation::Credit,
            amount: 1,
        },
        result: TransactionResult {
            outcome: Outcome::Applied,
            balance: 101,
            version: 1,
            replayed: false,
        },
        write_clock: 1,
    };
    assert!(matches!(
        store
            .commit(0, &initial, &missing_account_next, &missing_account_record,)
            .await,
        Err(Error::CommitConfirmedRollback(_))
    ));
    assert_eq!(
        store.data_connections(),
        6,
        "known rollback keeps its drained connection reusable"
    );

    store
        .seed_account(0, initial.balance)
        .await
        .expect("seed account after rollback");
    let preloaded = store
        .preload_state(0)
        .await
        .expect("preload account before service startup");
    assert_eq!(preloaded, initial);

    metrics.reset_postgres();
    let service = Service::start(store.clone(), 1, vec![preloaded], 8, metrics.clone())
        .expect("start one owner for one account");
    metrics.enable();
    let request = Transaction {
        account_id: 0,
        tx_id: 1,
        operation: Operation::Credit,
        amount: 10,
    };
    let first = service
        .submit(request.clone())
        .await
        .expect("first wallet request commits");
    assert_eq!(
        (first.outcome, first.balance, first.version),
        (Outcome::Applied, 110, 1)
    );
    let replay = service
        .submit(request.clone())
        .await
        .expect("identical request replays the original record");
    assert!(replay.replayed);
    assert_eq!((replay.balance, replay.version), (110, 1));
    let conflict = Transaction {
        amount: 11,
        ..request
    };
    assert!(matches!(
        service.submit(conflict).await,
        Err(Error::Conflict(_))
    ));
    service
        .shutdown()
        .await
        .expect("drain accepted service requests");
    metrics.disable();

    let cached = service.cache_snapshot()[0];
    let (persisted, ledger) = store
        .verify_account(0)
        .await
        .expect("full account and ledger verification");
    assert_eq!(persisted, cached);
    assert_eq!(
        persisted,
        AccountState {
            balance: 110,
            version: 1,
            write_clock: 1
        }
    );
    assert_eq!(ledger.len(), 1);
    assert_eq!(ledger[0].request.amount, 10);
    assert_eq!(ledger[0].result.outcome, Outcome::Applied);

    let counters = store.operation_counters();
    assert_eq!(counters.function_calls, 3);
    assert_eq!(counters.ledger_insert_attempts, 3);
    assert_eq!(counters.account_updates, 1);
    assert_eq!(counters.duplicate_conflicts, 2);

    drop(service);
    drop(store);
    let (admin, connection) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .expect("reconnect for isolated schema cleanup");
    tokio::spawn(connection);
    admin
        .batch_execute(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
        .await
        .expect("drop only the unique test schema");
}
