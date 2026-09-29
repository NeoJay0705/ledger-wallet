#[path = "support/ledger_preflight.rs"]
#[allow(dead_code)]
mod ledger_preflight;
#[path = "support/ledger_request_batch.rs"]
mod ledger_request_batch;
#[path = "support/request_batch_queue.rs"]
mod request_batch_queue;

fn main() {
    if let Err(error) = ledger_request_batch::run_from_args() {
        eprintln!("ledger_request_batch_tokio failed: {error}");
        std::process::exit(1);
    }
}
