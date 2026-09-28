//! Sequential asynchronous WAL writer used by the Tokio durability benchmark.
//!
//! File creation, writes, flushes, and syncs use `tokio::fs`; Tokio runs those
//! filesystem operations on its blocking pool so a current-thread runtime can
//! continue polling its event loop while storage work is in progress.

use crate::wal::{Record, encode_frame, segment_name};
use std::io;
use std::path::{Path, PathBuf};
use std::time::Instant;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;

#[derive(Clone, Copy, Debug, Default)]
pub struct AppendMetrics {
    pub frame_bytes: u64,
    pub frame_encode_ns: u64,
    pub write_all_await_ns: u64,
    pub flush_ns: u64,
    pub file_sync_ns: u64,
    pub directory_sync_ns: u64,
    pub segment_open_ns: u64,
    pub rotation_ns: u64,
    pub created_segment: bool,
    pub rotated: bool,
}

/// One async WAL writer. The caller awaits each batch before submitting the
/// next one, so at most one batch can be in flight.
pub struct WalWriter {
    directory: PathBuf,
    rotation_bytes: u64,
    segment: Option<File>,
    segment_bytes: u64,
    next_segment: u64,
    rotate_before_next: bool,
    failed: bool,
}

impl WalWriter {
    pub fn new(directory: impl AsRef<Path>, rotation_bytes: u64) -> io::Result<Self> {
        if rotation_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "rotation threshold must be positive",
            ));
        }
        Ok(Self {
            directory: directory.as_ref().to_path_buf(),
            rotation_bytes,
            segment: None,
            segment_bytes: 0,
            next_segment: 0,
            rotate_before_next: false,
            failed: false,
        })
    }

    /// Append one whole frame and acknowledge it only after write, flush, and
    /// file sync all succeed.
    pub async fn append_batch(&mut self, records: &[Record]) -> io::Result<AppendMetrics> {
        if self.failed {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "WAL writer is failed and cannot acknowledge more batches",
            ));
        }
        let result = self.append_batch_inner(records).await;
        if result.is_err() {
            // A failed create, directory sync, write, flush, or file sync leaves
            // the durability state uncertain. Never retry or acknowledge it.
            self.failed = true;
            self.segment = None;
        }
        result
    }

    async fn append_batch_inner(&mut self, records: &[Record]) -> io::Result<AppendMetrics> {
        if records.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "empty WAL batch",
            ));
        }

        let encode_start = Instant::now();
        let frame = encode_frame(records)?;
        let mut metrics = AppendMetrics {
            frame_bytes: frame.len() as u64,
            frame_encode_ns: elapsed_ns(encode_start),
            ..AppendMetrics::default()
        };

        if self.segment.is_none() || self.rotate_before_next {
            let rotated = self.segment.is_some();
            let rotation_start = Instant::now();
            let open_start = Instant::now();
            let file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(self.directory.join(segment_name(self.next_segment)))
                .await?;
            metrics.segment_open_ns = elapsed_ns(open_start);
            self.next_segment += 1;
            self.segment = Some(file);
            self.segment_bytes = 0;
            self.rotate_before_next = false;
            metrics.created_segment = true;
            metrics.rotated = rotated;

            let sync_start = Instant::now();
            File::open(&self.directory).await?.sync_all().await?;
            metrics.directory_sync_ns = elapsed_ns(sync_start);
            if rotated {
                metrics.rotation_ns = elapsed_ns(rotation_start);
            }
        }

        let file = self.segment.as_mut().expect("segment opened above");
        let write_start = Instant::now();
        file.write_all(&frame).await?;
        metrics.write_all_await_ns = elapsed_ns(write_start);

        // `sync_all` can wait for in-flight writes without reporting their
        // errors. Flush first so write failures reach the caller explicitly.
        let flush_start = Instant::now();
        file.flush().await?;
        metrics.flush_ns = elapsed_ns(flush_start);

        let sync_start = Instant::now();
        file.sync_all().await?;
        metrics.file_sync_ns = elapsed_ns(sync_start);

        self.segment_bytes = self.segment_bytes.saturating_add(frame.len() as u64);
        // Decide rotation only after the complete frame; the next batch opens
        // the next segment if this batch crossed the threshold.
        self.rotate_before_next = self.segment_bytes > self.rotation_bytes;
        Ok(metrics)
    }
}

fn elapsed_ns(start: Instant) -> u64 {
    start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
}
