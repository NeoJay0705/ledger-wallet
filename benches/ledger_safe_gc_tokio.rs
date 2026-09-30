#[allow(dead_code)]
#[path = "support/ledger_account_store.rs"]
mod ledger_account_store;
#[allow(dead_code)]
#[path = "support/ledger_preflight.rs"]
mod ledger_preflight;
#[allow(dead_code)]
#[path = "support/ledger_projection_worker.rs"]
mod ledger_projection_worker;
#[allow(dead_code)]
#[path = "support/ledger_time_boundary.rs"]
mod ledger_time_boundary;
#[allow(dead_code)]
#[path = "support/request_batch_queue.rs"]
mod request_batch_queue;

fn main() {
    if let Err(error) = ledger_safe_gc_benchmark::run_from_args() {
        eprintln!("ledger_safe_gc_tokio failed: {error}");
        std::process::exit(1);
    }
}

#[path = "support/ledger_safe_gc_benchmark.rs"]
mod ledger_safe_gc_benchmark;
