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

use ledger_account_store::Operation;
use ledger_pipeline::{Config, RequestClass, request_class_and_operation};

mod workload_tests {
    use super::*;

    #[test]
    fn paired_user_blocks_have_exact_fresh_hit_miss_and_operation_mix() {
        let mut counts = [[0_u64; 2]; 3];
        for account_id in 0..2 {
            for request_index in 0..200 {
                let (operation, class) = request_class_and_operation(account_id, request_index, 5);
                let class_index = match class {
                    RequestClass::Fresh => 0,
                    RequestClass::HistoricalHit => 1,
                    RequestClass::HistoricalMiss => 2,
                };
                let operation_index = match operation {
                    Operation::Credit => 0,
                    Operation::Debit => 1,
                    Operation::Refund => {
                        panic!("the integrated workload only emits credit and debit")
                    }
                };
                counts[class_index][operation_index] += 1;
            }
        }
        assert_eq!(counts, [[190, 190], [5, 5], [5, 5]]);
    }

    #[test]
    fn default_case_is_ten_million_requests_with_exact_five_percent_history() {
        let config = Config::default();
        config
            .validate()
            .expect("canonical benchmark config is valid");
        let total = config.total_requests();
        let historical = total * 5 / 100;

        assert_eq!(total, 10_000_000);
        assert_eq!(historical, 500_000);
        assert_eq!(historical / 2, 250_000);
        assert_eq!(total - historical, 9_500_000);
        assert_eq!((total - historical) / 2, 4_750_000);
    }

    #[test]
    fn smoke_checkpoint_default_is_order_independent_and_explicit_value_wins() {
        let parse = |args: &[&str]| {
            let args: Vec<String> = args.iter().map(|value| (*value).to_owned()).collect();
            Config::parse_args(&args).expect("smoke CLI config is valid")
        };

        assert_eq!(parse(&["--smoke"]).checkpoint_quantity, 100);
        assert_eq!(
            parse(&["--checkpoint-quantity", "123", "--smoke"]).checkpoint_quantity,
            123
        );
        assert_eq!(
            parse(&["--smoke", "--checkpoint-quantity", "456"]).checkpoint_quantity,
            456
        );
    }
}
