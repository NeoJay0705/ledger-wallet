#[allow(dead_code)]
#[path = "../benches/support/wal.rs"]
mod wal;
#[allow(dead_code)]
#[path = "../benches/support/wal_tokio.rs"]
mod wal_tokio;

use std::fs;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use wal::{RECORD_SIZE, generate_records, recover_and_verify, recover_prefix};
use wal_tokio::WalWriter;

static NEXT_DIR: AtomicU64 = AtomicU64::new(0);

fn temp_dir() -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "ledger-wallet-wal-tokio-integration-{}-{nonce}-{}",
        std::process::id(),
        NEXT_DIR.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&dir).unwrap();
    dir
}

#[tokio::test(flavor = "current_thread")]
async fn awaited_batches_recover_across_strict_post_batch_rotation() {
    let dir = temp_dir();
    let records = generate_records(7);
    let frame_size = 3 * RECORD_SIZE as u64 + 56;
    let mut writer = WalWriter::new(&dir, frame_size).unwrap();

    let first = writer.append_batch(&records[..3]).await.unwrap();
    let second = writer.append_batch(&records[3..6]).await.unwrap();
    let third = writer.append_batch(&records[6..]).await.unwrap();

    assert!(first.created_segment);
    assert_eq!(first.frame_bytes, frame_size);
    assert!(first.directory_sync_ns > 0);
    assert!(!second.rotated);
    assert!(!second.created_segment);
    assert!(third.rotated);
    assert!(third.created_segment);
    assert!(first.write_all_await_ns > 0);
    assert!(first.flush_ns > 0);
    assert!(first.file_sync_ns > 0);

    let recovered = recover_and_verify(&dir, &records).unwrap();
    assert_eq!(recovered.records, records.len() as u64);
    assert_eq!(recovered.segments, 2);
    fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn an_append_error_poisoning_prevents_a_later_false_acknowledgement() {
    let dir = temp_dir();
    let records = generate_records(2);
    let mut writer = WalWriter::new(&dir, 1_000_000).unwrap();
    fs::remove_dir(&dir).unwrap();

    let first_error = writer.append_batch(&records).await.unwrap_err();
    assert_eq!(first_error.kind(), io::ErrorKind::NotFound);
    fs::create_dir(&dir).unwrap();
    let second_error = writer.append_batch(&records).await.unwrap_err();
    assert_eq!(second_error.kind(), io::ErrorKind::Other);
    assert_eq!(recover_prefix(&dir).unwrap().records, 0);
    fs::remove_dir_all(dir).unwrap();
}
