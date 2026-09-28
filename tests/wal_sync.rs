#[allow(dead_code)]
#[path = "../benches/support/wal.rs"]
mod wal;

use std::fs::{self, OpenOptions};
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use wal::{
    RECORD_SIZE, Record, WalWriter, generate_records, recover_and_verify, recover_prefix,
    recover_with,
};

static NEXT_DIR: AtomicU64 = AtomicU64::new(0);

fn temp_dir() -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "ledger-wallet-wal-integration-{}-{nonce}-{}",
        std::process::id(),
        NEXT_DIR.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&dir).unwrap();
    dir
}

#[test]
fn each_complete_batch_is_synced_and_rotation_keeps_batches_whole() {
    let dir = temp_dir();
    let records = generate_records(7);
    let mut writer = WalWriter::new(&dir, 300).unwrap();

    let first = writer.append_batch(&records[..3]).unwrap();
    let second = writer.append_batch(&records[3..6]).unwrap();
    let third = writer.append_batch(&records[6..]).unwrap();

    assert!(first.created_segment);
    assert!(first.directory_sync_ns > 0);
    assert_eq!(first.frame_bytes, 3 * RECORD_SIZE as u64 + 56);
    assert_eq!(first.file_sync_ns > 0, true);
    assert!(second.rotated);
    assert!(third.rotated);
    let recovered = recover_and_verify(&dir, &records).unwrap();
    assert_eq!(recovered.records, 7);
    assert_eq!(recovered.segments, 3);
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn recovery_returns_only_the_complete_prefix_after_a_torn_final_batch() {
    let dir = temp_dir();
    let records = generate_records(8);
    let mut writer = WalWriter::new(&dir, 1_000_000).unwrap();
    writer.append_batch(&records[..4]).unwrap();
    writer.append_batch(&records[4..]).unwrap();

    let segment = dir.join("segment-000000.wal");
    let first_frame_bytes = 36 + 4 * RECORD_SIZE as u64 + 20;
    OpenOptions::new()
        .write(true)
        .open(&segment)
        .unwrap()
        .set_len(first_frame_bytes + 36 + 11)
        .unwrap();

    let recovered = recover_prefix(&dir).unwrap();
    assert_eq!(recovered.records, 4);
    assert_eq!(recovered.truncated_tail_bytes, 47);
    assert_eq!(
        recover_and_verify(&dir, &records).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn recovery_replays_custom_records_across_rotated_segments() {
    let dir = temp_dir();
    let records: Vec<Record> = (1u64..=7)
        .map(|lsn| {
            let mut bytes = [0u8; RECORD_SIZE];
            bytes[0..8].copy_from_slice(&lsn.to_le_bytes());
            for (index, byte) in bytes[8..].iter_mut().enumerate() {
                *byte = (lsn as usize).wrapping_mul(17).wrapping_add(index) as u8;
            }
            Record(bytes)
        })
        .collect();
    let mut writer = WalWriter::new(&dir, 250).unwrap();
    for batch in records.chunks(2) {
        writer.append_batch(batch).unwrap();
    }

    let mut replayed = Vec::new();
    let report = recover_with(&dir, |record| {
        replayed.push(record);
        Ok(())
    })
    .unwrap();

    assert_eq!(replayed, records);
    assert_eq!(report.records, records.len() as u64);
    assert!(report.segments > 1);
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn recovery_rejects_a_discontinuous_embedded_record_lsn() {
    let dir = temp_dir();
    let mut records = generate_records(2);
    records[1].0[0..8].copy_from_slice(&3u64.to_le_bytes());
    WalWriter::new(&dir, 1_000_000)
        .unwrap()
        .append_batch(&records)
        .unwrap();

    let error = recover_prefix(&dir).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("record LSN gap"));
    fs::remove_dir_all(dir).unwrap();
}
