#[path = "support/ledger_account_store.rs"]
mod ledger_account_store;
#[path = "support/ledger_pipeline.rs"]
mod ledger_pipeline;
#[path = "support/ledger_preflight.rs"]
mod ledger_preflight;
#[path = "support/ledger_projection_worker.rs"]
mod ledger_projection_worker;
#[path = "support/ledger_time_boundary.rs"]
mod ledger_time_boundary;
#[path = "support/request_batch_queue.rs"]
mod request_batch_queue;

fn main() {
    if let Err(error) = ledger_pipeline::run_from_args() {
        eprintln!("ledger_pipeline_tokio failed: {error}");
        std::process::exit(1);
    }
}
