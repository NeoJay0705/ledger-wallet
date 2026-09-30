#[path = "../benches/support/ledger_account_store_workload.rs"]
mod workload;

use std::collections::HashSet;
use workload::{build_workload, PatternKind, WorkloadKind};

#[test]
fn workload_selector_keeps_baseline_as_default_and_accepts_credit_debit_mix() {
    assert_eq!(
        WorkloadKind::parse("baseline").unwrap(),
        WorkloadKind::Baseline40_40_10_10
    );
    assert_eq!(
        WorkloadKind::parse("baseline-40-40-10-10").unwrap(),
        WorkloadKind::Baseline40_40_10_10
    );
    assert_eq!(
        WorkloadKind::parse("credit-debit-50-50").unwrap(),
        WorkloadKind::CreditDebit50_50
    );
    assert!(WorkloadKind::parse("refund-heavy").is_err());
}

#[test]
fn baseline_plan_preserves_operation_totals_and_final_balance() {
    let plan = build_workload(WorkloadKind::Baseline40_40_10_10, 10, 20).unwrap();
    assert_eq!(plan.requests, 200);
    assert_eq!(plan.transactions, 180);
    assert_eq!(plan.transactions_per_cycle, 9);
    assert_eq!(plan.operation_counts.credits, 80);
    assert_eq!(plan.operation_counts.debits, 80);
    assert_eq!(plan.operation_counts.refunds, 20);
    assert_eq!(plan.operation_counts.queries, 20);
    assert_eq!(plan.expected_final_balance, 2);

    for pattern in &plan.patterns {
        assert_eq!(
            pattern
                .iter()
                .filter(|entry| entry.kind == PatternKind::Credit)
                .count(),
            4
        );
        assert_eq!(
            pattern
                .iter()
                .filter(|entry| entry.kind == PatternKind::Debit)
                .count(),
            4
        );
        assert_eq!(
            pattern
                .iter()
                .filter(|entry| entry.kind == PatternKind::Refund)
                .count(),
            1
        );
        assert_eq!(
            pattern
                .iter()
                .filter(|entry| entry.kind == PatternKind::Balance)
                .count(),
            1
        );
        assert_eq!(pattern[0].kind, PatternKind::Credit);
    }
}

#[test]
fn credit_debit_plan_is_deterministic_valid_and_mixed_on_interior_waves() {
    let plan = build_workload(WorkloadKind::CreditDebit50_50, 64, 200).unwrap();
    let second_plan = build_workload(WorkloadKind::CreditDebit50_50, 64, 200).unwrap();
    assert_eq!(plan.requests, 12_800);
    assert_eq!(plan.transactions, 12_800);
    assert_eq!(plan.transactions_per_cycle, 10);
    assert_eq!(plan.operation_counts.credits, 6_400);
    assert_eq!(plan.operation_counts.debits, 6_400);
    assert_eq!(plan.operation_counts.refunds, 0);
    assert_eq!(plan.operation_counts.queries, 0);
    assert_eq!(plan.expected_final_balance, 0);
    assert!(plan
        .waves
        .iter()
        .all(|wave| wave.balance_accounts.is_empty()));

    let mut unique_patterns = HashSet::new();
    for (account, pattern) in plan.patterns.iter().enumerate() {
        let other_pattern = &second_plan.patterns[account];
        let kinds: Vec<_> = pattern.iter().map(|entry| entry.kind).collect();
        assert_eq!(
            kinds,
            other_pattern
                .iter()
                .map(|entry| entry.kind)
                .collect::<Vec<_>>()
        );
        unique_patterns.insert(kinds);

        assert_eq!(pattern[0].kind, PatternKind::Credit);
        assert_eq!(
            pattern
                .iter()
                .filter(|entry| entry.kind == PatternKind::Credit)
                .count(),
            5
        );
        assert_eq!(
            pattern
                .iter()
                .filter(|entry| entry.kind == PatternKind::Debit)
                .count(),
            5
        );
        assert!(pattern
            .iter()
            .all(|entry| { matches!(entry.kind, PatternKind::Credit | PatternKind::Debit) }));
        assert!(pattern
            .iter()
            .enumerate()
            .all(|(offset, entry)| entry.transaction_offset == offset as u8));

        let mut balance = 0_i32;
        for entry in pattern {
            match entry.kind {
                PatternKind::Credit => balance += 1,
                PatternKind::Debit => balance -= 1,
                _ => unreachable!("credit/debit pattern contains only transactions"),
            }
            assert!(balance >= 0, "account {account} would overdraw");
        }
        assert_eq!(balance, 0, "account {account} should finish at zero");
    }
    assert!(
        unique_patterns.len() > 10,
        "patterns should vary by account"
    );

    for slot in 1..9 {
        let (credits, debits) =
            plan.patterns
                .iter()
                .fold((0, 0), |(credits, debits), p| match p[slot].kind {
                    PatternKind::Credit => (credits + 1, debits),
                    PatternKind::Debit => (credits, debits + 1),
                    _ => unreachable!("credit/debit pattern contains only transactions"),
                });
        assert!(
            credits > 0 && debits > 0,
            "interior slot {slot} must mix operations"
        );
    }
}
