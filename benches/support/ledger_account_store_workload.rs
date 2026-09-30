//! Deterministic request patterns for the standalone account-store benchmark.

use std::time::Instant;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkloadKind {
    Baseline40_40_10_10,
    CreditDebit50_50,
}

impl WorkloadKind {
    pub fn name(self) -> &'static str {
        match self {
            Self::Baseline40_40_10_10 => "baseline-40-40-10-10",
            Self::CreditDebit50_50 => "credit-debit-50-50",
        }
    }

    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "baseline" | "baseline-40-40-10-10" => Ok(Self::Baseline40_40_10_10),
            "credit-debit-50-50" => Ok(Self::CreditDebit50_50),
            _ => Err(format!("unknown workload {value}")),
        }
    }

    fn transactions_per_cycle(self) -> u64 {
        match self {
            Self::Baseline40_40_10_10 => 9,
            Self::CreditDebit50_50 => 10,
        }
    }

    fn counts_per_cycle(self) -> OperationCounts {
        match self {
            Self::Baseline40_40_10_10 => OperationCounts {
                credits: 4,
                debits: 4,
                refunds: 1,
                queries: 1,
            },
            Self::CreditDebit50_50 => OperationCounts {
                credits: 5,
                debits: 5,
                refunds: 0,
                queries: 0,
            },
        }
    }

    fn balance_per_cycle(self) -> u64 {
        match self {
            Self::Baseline40_40_10_10 => 1,
            Self::CreditDebit50_50 => 0,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OperationCounts {
    pub credits: u64,
    pub debits: u64,
    pub refunds: u64,
    pub queries: u64,
}

impl OperationCounts {
    pub fn transactions(self) -> u64 {
        self.credits + self.debits + self.refunds
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PatternKind {
    Credit,
    Debit,
    Refund,
    Balance,
}

impl PatternKind {
    pub fn is_transaction(self) -> bool {
        self != Self::Balance
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PatternEntry {
    pub kind: PatternKind,
    pub transaction_offset: u8,
    pub refund_target_offset: u8,
}

#[derive(Clone, Debug)]
pub struct WavePlan {
    pub transaction_accounts: Vec<u32>,
    pub balance_accounts: Vec<u32>,
}

pub struct Workload {
    pub kind: WorkloadKind,
    pub patterns: Vec<[PatternEntry; 10]>,
    pub waves: Vec<WavePlan>,
    pub users: usize,
    pub requests: u64,
    pub transactions: u64,
    pub transactions_per_cycle: u64,
    pub operation_counts: OperationCounts,
    pub expected_final_balance: u64,
    pub generation_ns: u64,
}

pub fn build_workload(kind: WorkloadKind, users: usize, waves: usize) -> Result<Workload, String> {
    if users == 0 || users > u32::MAX as usize {
        return Err("users must be in 1..=u32::MAX".to_owned());
    }
    if waves == 0 || waves % 10 != 0 {
        return Err("waves must be a positive multiple of 10".to_owned());
    }
    let started = Instant::now();
    let patterns = (0..users)
        .map(|account| make_pattern(kind, account))
        .collect::<Vec<_>>();
    let mut wave_plans = Vec::with_capacity(waves);
    for wave in 0..waves {
        let mut transaction_accounts =
            Vec::with_capacity(users * kind.transactions_per_cycle() as usize / 10);
        let mut balance_accounts = Vec::with_capacity(users / 10);
        for (account, pattern) in patterns.iter().enumerate() {
            match pattern[wave % 10].kind {
                PatternKind::Balance => balance_accounts.push(account as u32),
                _ => transaction_accounts.push(account as u32),
            }
        }
        wave_plans.push(WavePlan {
            transaction_accounts,
            balance_accounts,
        });
    }
    let cycles = (waves / 10) as u64;
    let requests = u64::try_from(users)
        .ok()
        .and_then(|count| count.checked_mul(waves as u64))
        .ok_or_else(|| "request count overflow".to_owned())?;
    let operation_counts = kind.counts_per_cycle().scaled(
        (users as u64)
            .checked_mul(cycles)
            .ok_or_else(|| "operation count overflow".to_owned())?,
    )?;
    let transactions = operation_counts.transactions();
    if operation_counts.credits
        + operation_counts.debits
        + operation_counts.refunds
        + operation_counts.queries
        != requests
    {
        return Err("workload operation counts do not match request count".to_owned());
    }
    Ok(Workload {
        kind,
        patterns,
        waves: wave_plans,
        users,
        requests,
        transactions,
        transactions_per_cycle: kind.transactions_per_cycle(),
        operation_counts,
        expected_final_balance: kind.balance_per_cycle() * cycles,
        generation_ns: u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
    })
}

impl OperationCounts {
    fn scaled(self, factor: u64) -> Result<Self, String> {
        Ok(Self {
            credits: self
                .credits
                .checked_mul(factor)
                .ok_or_else(|| "credit count overflow".to_owned())?,
            debits: self
                .debits
                .checked_mul(factor)
                .ok_or_else(|| "debit count overflow".to_owned())?,
            refunds: self
                .refunds
                .checked_mul(factor)
                .ok_or_else(|| "refund count overflow".to_owned())?,
            queries: self
                .queries
                .checked_mul(factor)
                .ok_or_else(|| "query count overflow".to_owned())?,
        })
    }
}

fn make_pattern(kind: WorkloadKind, account: usize) -> [PatternEntry; 10] {
    match kind {
        WorkloadKind::Baseline40_40_10_10 => make_baseline_pattern(account),
        WorkloadKind::CreditDebit50_50 => make_credit_debit_pattern(account),
    }
}

// Keep this generator stable: it defines the original baseline's account order,
// operation order, deterministic transaction offsets, and query placement.
fn make_baseline_pattern(account: usize) -> [PatternEntry; 10] {
    let placeholder = PatternEntry {
        kind: PatternKind::Balance,
        transaction_offset: 0,
        refund_target_offset: 0,
    };
    let mut seed = mix64(account as u64 ^ 0x4c4544474552);
    loop {
        let mut raw = [
            PatternKind::Credit,
            PatternKind::Credit,
            PatternKind::Credit,
            PatternKind::Credit,
            PatternKind::Debit,
            PatternKind::Debit,
            PatternKind::Debit,
            PatternKind::Debit,
            PatternKind::Refund,
            PatternKind::Balance,
        ];
        for index in (1..raw.len()).rev() {
            seed = xorshift(seed);
            let target = (seed as usize) % (index + 1);
            raw.swap(index, target);
        }
        if raw[0] != PatternKind::Credit {
            let credit = raw
                .iter()
                .position(|kind| *kind == PatternKind::Credit)
                .expect("pattern always contains credits");
            raw.swap(0, credit);
        }
        let mut balance = 0_u64;
        let mut tx_offset = 0_u8;
        let mut last_unreffed_debit = None;
        let mut entries = [placeholder; 10];
        let mut valid = true;
        for (slot, kind) in raw.into_iter().enumerate() {
            let mut refund_target_offset = 0;
            let this_tx_offset = tx_offset;
            match kind {
                PatternKind::Credit => {
                    balance += 1;
                    tx_offset += 1;
                }
                PatternKind::Debit => {
                    if balance == 0 {
                        valid = false;
                        break;
                    }
                    balance -= 1;
                    last_unreffed_debit = Some(tx_offset);
                    tx_offset += 1;
                }
                PatternKind::Refund => {
                    let Some(target) = last_unreffed_debit.take() else {
                        valid = false;
                        break;
                    };
                    refund_target_offset = target;
                    balance += 1;
                    tx_offset += 1;
                }
                PatternKind::Balance => {}
            }
            entries[slot] = PatternEntry {
                kind,
                transaction_offset: this_tx_offset,
                refund_target_offset,
            };
        }
        if valid && baseline_pattern_is_valid(&entries) {
            return entries;
        }
    }
}

fn baseline_pattern_is_valid(entries: &[PatternEntry; 10]) -> bool {
    let mut credits = 0;
    let mut debits = 0;
    let mut refunds = 0;
    let mut balances = 0;
    let mut amount = 0_i64;
    let mut pending_debit = false;
    for entry in entries {
        match entry.kind {
            PatternKind::Credit => {
                credits += 1;
                amount += 1;
            }
            PatternKind::Debit => {
                debits += 1;
                if amount == 0 {
                    return false;
                }
                amount -= 1;
                pending_debit = true;
            }
            PatternKind::Refund => {
                refunds += 1;
                if !pending_debit {
                    return false;
                }
                amount += 1;
                pending_debit = false;
            }
            PatternKind::Balance => balances += 1,
        }
    }
    credits == 4
        && debits == 4
        && refunds == 1
        && balances == 1
        && entries[0].kind == PatternKind::Credit
}

fn make_credit_debit_pattern(account: usize) -> [PatternEntry; 10] {
    // These two complementary valid paths guarantee mixed interior waves for
    // every workload with at least two accounts. The first and last slots are
    // necessarily all credit and all debit respectively.
    const CREDIT_FIRST: [PatternKind; 10] = [
        PatternKind::Credit,
        PatternKind::Debit,
        PatternKind::Credit,
        PatternKind::Debit,
        PatternKind::Credit,
        PatternKind::Debit,
        PatternKind::Credit,
        PatternKind::Debit,
        PatternKind::Credit,
        PatternKind::Debit,
    ];
    const COMPLEMENTARY: [PatternKind; 10] = [
        PatternKind::Credit,
        PatternKind::Credit,
        PatternKind::Debit,
        PatternKind::Credit,
        PatternKind::Debit,
        PatternKind::Credit,
        PatternKind::Debit,
        PatternKind::Credit,
        PatternKind::Debit,
        PatternKind::Debit,
    ];
    if account == 0 || account == 1 {
        let pattern = if account == 0 {
            CREDIT_FIRST
        } else {
            COMPLEMENTARY
        };
        return pattern_from_kinds(pattern);
    }

    let mut seed = mix64(account as u64 ^ 0x434445424954);
    loop {
        let mut raw = [
            PatternKind::Credit,
            PatternKind::Credit,
            PatternKind::Credit,
            PatternKind::Credit,
            PatternKind::Credit,
            PatternKind::Debit,
            PatternKind::Debit,
            PatternKind::Debit,
            PatternKind::Debit,
            PatternKind::Debit,
        ];
        for index in (1..raw.len()).rev() {
            seed = xorshift(seed);
            let target = (seed as usize) % (index + 1);
            raw.swap(index, target);
        }
        if raw[0] != PatternKind::Credit {
            let credit = raw
                .iter()
                .position(|kind| *kind == PatternKind::Credit)
                .expect("pattern always contains credits");
            raw.swap(0, credit);
        }
        if credit_debit_pattern_is_valid(&raw) {
            return pattern_from_kinds(raw);
        }
    }
}

fn credit_debit_pattern_is_valid(kinds: &[PatternKind; 10]) -> bool {
    let mut balance = 0_i8;
    let mut credits = 0;
    let mut debits = 0;
    for kind in kinds {
        match kind {
            PatternKind::Credit => {
                balance += 1;
                credits += 1;
            }
            PatternKind::Debit => {
                balance -= 1;
                debits += 1;
            }
            _ => return false,
        }
        if balance < 0 {
            return false;
        }
    }
    credits == 5 && debits == 5 && balance == 0
}

fn pattern_from_kinds(kinds: [PatternKind; 10]) -> [PatternEntry; 10] {
    let mut tx_offset = 0_u8;
    kinds.map(|kind| {
        let entry = PatternEntry {
            kind,
            transaction_offset: tx_offset,
            refund_target_offset: 0,
        };
        if kind.is_transaction() {
            tx_offset += 1;
        }
        entry
    })
}

fn mix64(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
    value ^ (value >> 31)
}

fn xorshift(mut value: u64) -> u64 {
    value ^= value << 13;
    value ^= value >> 7;
    value ^= value << 17;
    value
}
