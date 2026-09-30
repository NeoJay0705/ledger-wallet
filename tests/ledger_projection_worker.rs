#[allow(dead_code)]
#[path = "../benches/support/ledger_account_store.rs"]
mod ledger_account_store;
#[allow(dead_code)]
#[path = "../benches/support/ledger_projection_worker.rs"]
mod ledger_projection_worker;

use ledger_account_store::{AccountStore, BalanceMode, Operation, Transaction, TransactionKey};
use ledger_projection_worker::MockProjectionStore;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::watch;

struct TempDb(PathBuf);

impl TempDb {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let nonce = NEXT.fetch_add(1, Ordering::Relaxed);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock is after Unix epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "ledger-projection-worker-test-{}-{timestamp}-{nonce}",
            std::process::id()
        ));
        Self(path)
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn transaction(account_id: u64, tx_id: u64, amount: u64) -> Transaction {
    Transaction {
        key: TransactionKey {
            account_id,
            tx_id,
            transaction_at: tx_id + 1000,
        },
        operation: Operation::Credit,
        amount,
        refund_of: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_projector_drains_committed_ranges_when_writer_closes_updates() {
    let temp = TempDb::new();
    let store = AccountStore::open(&temp.0, 2, BalanceMode::Checkpoint, 100)
        .await
        .unwrap();
    let projected = Arc::new(MockProjectionStore::default());
    let (head_tx, mut head_rx) = watch::channel(0_u64);
    let projector_store = store.clone();
    let projector_destination = Arc::clone(&projected);
    let projector = tokio::spawn(async move {
        loop {
            let progress = projector_destination.progress();
            let latest = projector_store.latest_seq();
            if progress < latest {
                let read = projector_store.read_ledger_range(progress + 1, 2).await?;
                if read.records.is_empty() {
                    return Err("live projector observed a sequence gap".to_owned());
                }
                assert!(read.db_read_ns > 0);
                projector_destination.apply_batch(&read.records)?;
                continue;
            }
            if head_rx.changed().await.is_err() {
                if projector_destination.progress() == projector_store.latest_seq() {
                    return Ok::<(), String>(());
                }
            }
        }
    });

    let first = store
        .handle_batch(vec![transaction(0, 1, 3), transaction(1, 1, 5)])
        .await
        .unwrap();
    assert_eq!(first.len(), 2);
    head_tx.send_replace(store.latest_seq());
    let second = store
        .handle_batch(vec![transaction(0, 2, 7), transaction(1, 2, 11)])
        .await
        .unwrap();
    assert_eq!(second.len(), 2);
    head_tx.send_replace(store.latest_seq());
    drop(head_tx);

    projector.await.unwrap().unwrap();
    assert_eq!(projected.progress(), 4);
    assert!(projected.contains(TransactionKey {
        account_id: 0,
        tx_id: 2,
        transaction_at: 1002,
    }));
    store.shutdown().await.unwrap();
}
