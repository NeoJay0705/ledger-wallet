use ledger_wallet::history::{
    HistoricalDebit, HistoricalLookup, HistoryDurability, HistoryErrorKind, HistoryStore,
    MockHistory,
};
use ledger_wallet::metrics::{Metrics, Stage};
use ledger_wallet::{
    LedgerRecord, Operation, Transaction, TransactionKey, TransactionResult, TransactionStatus,
};

fn key(account_id: u64, tx_id: u64, transaction_at: u64) -> TransactionKey {
    TransactionKey {
        account_id,
        tx_id,
        transaction_at,
    }
}

fn transaction(
    key: TransactionKey,
    operation: Operation,
    amount: u64,
    refund_of: Option<TransactionKey>,
) -> Transaction {
    Transaction {
        key,
        operation,
        amount,
        refund_of,
    }
}

fn record(request: Transaction, status: TransactionStatus, balance: u64, seq: u64) -> LedgerRecord {
    LedgerRecord {
        request,
        result: TransactionResult {
            status,
            balance,
            seq,
        },
    }
}

fn applied(request: Transaction, balance: u64, seq: u64) -> LedgerRecord {
    record(request, TransactionStatus::Applied, balance, seq)
}

fn request_stage(metrics: &Metrics) -> ledger_wallet::metrics::StageSnapshot {
    metrics
        .snapshot()
        .stages
        .into_iter()
        .find(|snapshot| snapshot.stage == Stage::Request.as_str())
        .expect("request stage is present in the public metrics snapshot")
}

#[tokio::test]
async fn volatile_mock_projects_atomically_and_accepts_exact_prefix_replays() {
    let history = MockHistory::new("formal-history-positive");
    assert_eq!(history.durability(), HistoryDurability::Volatile);

    let account_id = 41;
    let credit_key = key(account_id, 1, 10);
    let debit_key = key(account_id, 2, 20);
    let credit = transaction(credit_key, Operation::Credit, 100, None);
    let debit = transaction(debit_key, Operation::Debit, 40, None);
    let refund = transaction(
        key(account_id, 3, 30),
        Operation::Refund,
        40,
        Some(debit_key),
    );
    let initial = vec![
        applied(credit.clone(), 100, 1),
        applied(debit.clone(), 60, 2),
        applied(refund.clone(), 100, 3),
    ];

    assert_eq!(history.apply_batch(&initial).await.unwrap(), 3);
    assert_eq!(history.progress().await.unwrap(), 3);
    assert_eq!(
        history.lookup_debit(debit_key).await.unwrap(),
        Some(HistoricalDebit {
            amount: 40,
            already_refunded: true,
        })
    );

    // Reapplying the entire committed prefix is idempotent and does not move progress.
    assert_eq!(history.apply_batch(&initial).await.unwrap(), 3);
    assert_eq!(history.progress().await.unwrap(), 3);

    // A replayed suffix followed by the next contiguous record is also valid.
    let next_credit = transaction(key(account_id, 4, 40), Operation::Credit, 5, None);
    let mut suffix = initial[1..].to_vec();
    suffix.push(applied(next_credit.clone(), 105, 4));
    assert_eq!(history.apply_batch(&suffix).await.unwrap(), 4);
    assert_eq!(history.progress().await.unwrap(), 4);
    assert_eq!(
        history.lookup_transaction(&next_credit).await.unwrap(),
        HistoricalLookup::ExactReplay(TransactionResult {
            status: TransactionStatus::Applied,
            balance: 105,
            seq: 4,
        })
    );
    assert_eq!(
        history.lookup_debit(debit_key).await.unwrap(),
        Some(HistoricalDebit {
            amount: 40,
            already_refunded: true,
        })
    );
}

#[tokio::test]
async fn invalid_projection_batches_leave_rows_progress_and_refund_state_unchanged() {
    let gap_history = MockHistory::new("formal-history-gap");
    let gap_first = transaction(key(1, 1, 1), Operation::Credit, 10, None);
    let gap_second = transaction(key(1, 2, 2), Operation::Debit, 1, None);
    let gap_error = gap_history
        .apply_batch(&[
            applied(gap_first.clone(), 10, 1),
            applied(gap_second.clone(), 9, 3),
        ])
        .await
        .unwrap_err();
    assert_eq!(gap_error.kind, HistoryErrorKind::Corruption);
    assert_eq!(gap_history.progress().await.unwrap(), 0);
    assert_eq!(
        gap_history.lookup_transaction(&gap_first).await.unwrap(),
        HistoricalLookup::NotFound
    );
    assert_eq!(
        gap_history.lookup_transaction(&gap_second).await.unwrap(),
        HistoricalLookup::NotFound
    );

    let conflict_history = MockHistory::new("formal-history-conflict");
    let original_credit = transaction(key(2, 1, 10), Operation::Credit, 100, None);
    let debit_key = key(2, 2, 20);
    let debit = transaction(debit_key, Operation::Debit, 40, None);
    assert_eq!(
        conflict_history
            .apply_batch(&[
                applied(original_credit.clone(), 100, 1),
                applied(debit.clone(), 60, 2),
            ])
            .await
            .unwrap(),
        2
    );

    // The refund is valid and staged first; the later same-key/different-body
    // conflict must roll back its row, the refund marker, and progress together.
    let staged_refund = transaction(key(2, 3, 30), Operation::Refund, 40, Some(debit_key));
    let conflicting_credit = transaction(key(2, 1, 10), Operation::Credit, 101, None);
    let conflict_error = conflict_history
        .apply_batch(&[
            applied(staged_refund.clone(), 100, 3),
            applied(conflicting_credit.clone(), 101, 4),
        ])
        .await
        .unwrap_err();
    assert_eq!(conflict_error.kind, HistoryErrorKind::Conflict);
    assert_eq!(conflict_history.progress().await.unwrap(), 2);
    assert_eq!(
        conflict_history
            .lookup_transaction(&staged_refund)
            .await
            .unwrap(),
        HistoricalLookup::NotFound
    );
    assert_eq!(
        conflict_history
            .lookup_transaction(&original_credit)
            .await
            .unwrap(),
        HistoricalLookup::ExactReplay(TransactionResult {
            status: TransactionStatus::Applied,
            balance: 100,
            seq: 1,
        })
    );
    assert_eq!(
        conflict_history
            .lookup_transaction(&conflicting_credit)
            .await
            .unwrap(),
        HistoricalLookup::Conflict
    );
    assert_eq!(
        conflict_history.lookup_debit(debit_key).await.unwrap(),
        Some(HistoricalDebit {
            amount: 40,
            already_refunded: false,
        })
    );

    let refund_history = MockHistory::new("formal-history-second-refund");
    let credit = transaction(key(3, 1, 10), Operation::Credit, 100, None);
    let debit_key = key(3, 2, 20);
    let debit = transaction(debit_key, Operation::Debit, 40, None);
    let first_refund = transaction(key(3, 3, 30), Operation::Refund, 40, Some(debit_key));
    assert_eq!(
        refund_history
            .apply_batch(&[
                applied(credit, 100, 1),
                applied(debit.clone(), 60, 2),
                applied(first_refund, 100, 3),
            ])
            .await
            .unwrap(),
        3
    );

    let staged_credit = transaction(key(3, 4, 40), Operation::Credit, 5, None);
    let second_refund = transaction(key(3, 5, 50), Operation::Refund, 40, Some(debit_key));
    let refund_error = refund_history
        .apply_batch(&[
            applied(staged_credit.clone(), 105, 4),
            applied(second_refund.clone(), 145, 5),
        ])
        .await
        .unwrap_err();
    assert_eq!(refund_error.kind, HistoryErrorKind::Corruption);
    assert_eq!(refund_history.progress().await.unwrap(), 3);
    assert_eq!(
        refund_history
            .lookup_transaction(&staged_credit)
            .await
            .unwrap(),
        HistoricalLookup::NotFound
    );
    assert_eq!(
        refund_history
            .lookup_transaction(&second_refund)
            .await
            .unwrap(),
        HistoricalLookup::NotFound
    );
    assert_eq!(
        refund_history.lookup_debit(debit_key).await.unwrap(),
        Some(HistoricalDebit {
            amount: 40,
            already_refunded: true,
        })
    );
}

#[tokio::test]
async fn transaction_lookup_distinguishes_exact_replay_conflict_and_missing_key() {
    let history = MockHistory::new("formal-history-lookup");
    let original = transaction(key(7, 11, 123), Operation::Credit, 25, None);
    let original_result = TransactionResult {
        status: TransactionStatus::Applied,
        balance: 25,
        seq: 1,
    };
    history
        .apply_batch(&[LedgerRecord {
            request: original.clone(),
            result: original_result,
        }])
        .await
        .unwrap();

    assert_eq!(
        history.lookup_transaction(&original).await.unwrap(),
        HistoricalLookup::ExactReplay(original_result)
    );
    assert_eq!(
        history
            .lookup_transaction(&transaction(
                original.key,
                Operation::Debit,
                original.amount,
                None,
            ))
            .await
            .unwrap(),
        HistoricalLookup::Conflict
    );
    assert_eq!(
        history
            .lookup_transaction(&transaction(
                original.key,
                original.operation,
                original.amount + 1,
                None,
            ))
            .await
            .unwrap(),
        HistoricalLookup::Conflict
    );
    assert_eq!(
        history
            .lookup_transaction(&transaction(
                original.key,
                original.operation,
                original.amount,
                Some(key(7, 99, 999)),
            ))
            .await
            .unwrap(),
        HistoricalLookup::Conflict
    );

    let missing = transaction(key(7, 12, 124), Operation::Credit, 25, None);
    let changed_timestamp = transaction(key(7, 11, 124), Operation::Credit, 25, None);
    assert_eq!(
        history.lookup_transaction(&missing).await.unwrap(),
        HistoricalLookup::NotFound
    );
    assert_eq!(
        history
            .lookup_transaction(&changed_timestamp)
            .await
            .unwrap(),
        HistoricalLookup::NotFound
    );
}

#[test]
fn request_rate_recording_does_not_create_zero_duration_latency_samples() {
    let metrics = Metrics::new();
    metrics.record_request();
    metrics.record_request();

    let snapshot = metrics.snapshot();
    let request = snapshot
        .stages
        .iter()
        .find(|stage| stage.stage == Stage::Request.as_str())
        .unwrap();
    assert_eq!(request.count, 0);
    assert_eq!(request.average_ns, 0);
    assert_eq!(request.sample_count, 0);
    assert_eq!(request.p50_ns, None);
    assert_eq!(request.p95_ns, None);
    assert_eq!(request.p99_ns, None);
    assert!((snapshot.requests_per_second_10s - 0.2).abs() < f64::EPSILON);

    metrics.record(Stage::Request, 73);
    let request = request_stage(&metrics);
    assert_eq!(request.count, 1);
    assert_eq!(request.average_ns, 73);
    assert_eq!(request.sample_count, 1);
    assert_eq!(request.p50_ns, Some(73));
    assert_eq!(request.p95_ns, Some(73));
    assert_eq!(request.p99_ns, Some(73));
}

#[test]
fn batch_wait_stage_uses_a_new_stable_stage_id() {
    assert_eq!(Stage::Request as usize, 0);
    assert_eq!(Stage::Balance as usize, 13);
    assert_eq!(Stage::BatchWait as usize, 14);
    assert_eq!(Stage::BatchWait.as_str(), "batch_wait");
    assert_eq!(Stage::ALL.len(), 15);

    let metrics = Metrics::new();
    let snapshot = metrics.snapshot();
    assert!(
        snapshot
            .stages
            .iter()
            .any(|stage| stage.stage == "batch_wait")
    );
}

#[test]
fn latency_snapshot_uses_nearest_rank_quantiles_and_bounds_retained_samples() {
    let metrics = Metrics::new();
    for latency in [10, 20, 30, 40, 100] {
        metrics.record(Stage::Request, latency);
    }
    let request = request_stage(&metrics);
    assert_eq!(request.count, 5);
    assert_eq!(request.average_ns, 40);
    assert_eq!(request.sample_count, 5);
    assert_eq!(request.p50_ns, Some(30));
    assert_eq!(request.p95_ns, Some(100));
    assert_eq!(request.p99_ns, Some(100));
    assert_eq!(request.max_ns, Some(100));

    let metrics = Metrics::new();
    let mut sum = 0u128;
    for observation in 1..=5_000 {
        let latency = if observation == 4_158 {
            // This is outside the deterministic retained sample. The public
            // maximum must still include every recorded observation.
            1_000_000
        } else {
            observation as u64
        };
        sum += latency as u128;
        metrics.record(Stage::Request, latency);
    }

    let request = request_stage(&metrics);
    assert_eq!(request.count, 5_000);
    assert_eq!(request.average_ns, (sum / 5_000) as u64);
    assert_eq!(request.max_ns, Some(1_000_000));
    assert!(request.sample_count <= 4_096);
    assert!(request.p50_ns.is_some());
    assert!(request.p95_ns.is_some());
    assert!(request.p99_ns.is_some());
}
