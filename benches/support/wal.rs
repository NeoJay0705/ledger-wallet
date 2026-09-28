//! Synchronous, framed write-ahead log used by the durability benchmark.
//!
//! A successful [`WalWriter::append_batch`] has written a complete frame and
//! called `sync_all` exactly once on its segment. Newly created segment names
//! are also made durable with a directory sync before the batch is acknowledged.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

pub const RECORD_SIZE: usize = 128;
const MAGIC: &[u8; 8] = b"LWALSYNC";
const TRAILER_MAGIC: &[u8; 8] = b"SYNCEND!";
const VERSION: u16 = 1;
const HEADER_SIZE: usize = 36;
const TRAILER_SIZE: usize = 20;
const MAX_FRAME_PAYLOAD: u64 = 64 * 1024 * 1024;

/// A deterministic fixed-size transaction record stored as exactly 128 bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Record(pub [u8; RECORD_SIZE]);

impl Record {
    pub fn lsn(&self) -> u64 {
        u64::from_le_bytes(self.0[0..8].try_into().expect("fixed record field"))
    }
}

/// Generate the same record sequence for each benchmark scenario.
pub fn generate_records(count: usize) -> Vec<Record> {
    (0..count).map(make_record).collect()
}

fn make_record(index: usize) -> Record {
    let sequence = index as u64 + 1;
    let mut bytes = [0u8; RECORD_SIZE];
    bytes[0..8].copy_from_slice(&sequence.to_le_bytes());
    bytes[8..16].copy_from_slice(&sequence.to_le_bytes());
    bytes[16..24].copy_from_slice(&((index % 100_000) as u64).to_le_bytes());
    let cents = ((index % 10_000) as i64 + 1) * if index % 2 == 0 { -1 } else { 1 };
    bytes[24..32].copy_from_slice(&cents.to_le_bytes());
    for (chunk, offset) in bytes[32..]
        .chunks_exact_mut(8)
        .zip((32..RECORD_SIZE).step_by(8))
    {
        let value = splitmix64(sequence ^ offset as u64);
        chunk.copy_from_slice(&value.to_le_bytes());
    }
    Record(bytes)
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[derive(Clone, Copy, Debug, Default)]
pub struct AppendMetrics {
    pub frame_bytes: u64,
    pub write_ns: u64,
    pub file_sync_ns: u64,
    pub directory_sync_ns: u64,
    pub segment_open_ns: u64,
    pub rotation_ns: u64,
    pub created_segment: bool,
    pub rotated: bool,
}

/// One synchronous WAL writer. Batches are never split between segments.
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

    /// Append a whole batch and return only after its single file sync succeeds.
    pub fn append_batch(&mut self, records: &[Record]) -> io::Result<AppendMetrics> {
        if self.failed {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "WAL writer is failed and cannot acknowledge more batches",
            ));
        }
        let result = self.append_batch_inner(records);
        if result.is_err() {
            // A failed create, directory sync, write, or file sync leaves the
            // durability state uncertain. The writer never retries/acks it.
            self.failed = true;
            self.segment = None;
        }
        result
    }

    fn append_batch_inner(&mut self, records: &[Record]) -> io::Result<AppendMetrics> {
        if records.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "empty WAL batch",
            ));
        }
        let frame = encode_frame(records)?;
        let mut metrics = AppendMetrics {
            frame_bytes: frame.len() as u64,
            ..AppendMetrics::default()
        };

        if self.segment.is_none() || self.rotate_before_next {
            let rotated = self.segment.is_some();
            let rotation_start = Instant::now();
            let open_start = Instant::now();
            let file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(self.directory.join(segment_name(self.next_segment)))?;
            metrics.segment_open_ns = elapsed_ns(open_start);
            self.next_segment += 1;
            self.segment = Some(file);
            self.segment_bytes = 0;
            self.rotate_before_next = false;
            metrics.created_segment = true;
            metrics.rotated = rotated;

            let sync_start = Instant::now();
            File::open(&self.directory)?.sync_all()?;
            metrics.directory_sync_ns = elapsed_ns(sync_start);
            if rotated {
                metrics.rotation_ns = elapsed_ns(rotation_start);
            }
        }

        let file = self.segment.as_mut().expect("segment opened above");
        let write_start = Instant::now();
        file.write_all(&frame)?;
        metrics.write_ns = elapsed_ns(write_start);

        let sync_start = Instant::now();
        file.sync_all()?;
        metrics.file_sync_ns = elapsed_ns(sync_start);

        self.segment_bytes = self.segment_bytes.saturating_add(frame.len() as u64);
        // Rotation is decided after a complete frame. The next batch opens the
        // next segment, even when this frame itself crossed the threshold.
        self.rotate_before_next = self.segment_bytes > self.rotation_bytes;
        Ok(metrics)
    }
}

#[derive(Clone, Debug, Default)]
pub struct RecoveryReport {
    pub records: u64,
    pub bytes: u64,
    pub segments: u64,
    pub truncated_tail_bytes: u64,
}

/// Verify every recovered record against the original deterministic sequence.
/// A partial frame is truncated only when it is in the final segment.
pub fn recover_and_verify(
    directory: impl AsRef<Path>,
    expected: &[Record],
) -> io::Result<RecoveryReport> {
    let mut record_offset = 0usize;
    let report = recover_with(directory, |record| {
        let expected_record = expected
            .get(record_offset)
            .ok_or_else(|| invalid_data("WAL contains more records than expected"))?;
        if record != *expected_record {
            return Err(invalid_data(format!(
                "WAL record mismatch at sequence {}",
                record_offset + 1
            )));
        }
        record_offset += 1;
        Ok(())
    })?;
    if report.records != expected.len() as u64 {
        return Err(invalid_data(format!(
            "recovered {} records, expected {}",
            report.records,
            expected.len()
        )));
    }
    Ok(report)
}

/// Recover the valid record prefix. A torn final frame is truncated and
/// reported; corruption or a torn frame in an earlier segment is an error.
pub fn recover_prefix(directory: impl AsRef<Path>) -> io::Result<RecoveryReport> {
    recover_with(directory, |_| Ok(()))
}

/// Recover records in WAL order after each containing frame has passed its
/// checksum and trailer checks. A torn final frame is truncated and reported;
/// corruption or a torn frame in an earlier segment is an error.
pub fn recover_with(
    directory: impl AsRef<Path>,
    mut callback: impl FnMut(Record) -> io::Result<()>,
) -> io::Result<RecoveryReport> {
    recover_inner(directory.as_ref(), &mut callback)
}

fn recover_inner(
    directory: &Path,
    callback: &mut impl FnMut(Record) -> io::Result<()>,
) -> io::Result<RecoveryReport> {
    let mut segments = Vec::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(number) = parse_segment_name(&name) {
            segments.push((number, entry.path()));
        }
    }
    segments.sort_by_key(|(number, _)| *number);
    for (expected_number, (number, _)) in segments.iter().enumerate() {
        if *number != expected_number as u64 {
            return Err(invalid_data(format!(
                "WAL segment gap: expected {expected_number}, found {number}"
            )));
        }
    }

    let mut report = RecoveryReport {
        segments: segments.len() as u64,
        ..RecoveryReport::default()
    };
    let mut next_lsn = 1u128;
    for (segment_index, (_, path)) in segments.iter().enumerate() {
        let final_segment = segment_index + 1 == segments.len();
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        let length = file.metadata()?.len();
        let mut frame_start = 0u64;
        loop {
            let mut header = [0u8; HEADER_SIZE];
            let header_read = read_partial(&mut file, &mut header)?;
            if header_read == 0 {
                break;
            }
            if header_read != HEADER_SIZE {
                if !final_segment {
                    return Err(invalid_data("partial frame header before final segment"));
                }
                truncate_tail(&mut file, frame_start, &mut report)?;
                break;
            }
            let fields = parse_header(&header)?;
            if fields.payload_len > MAX_FRAME_PAYLOAD {
                return Err(invalid_data("WAL frame exceeds the recovery size limit"));
            }
            let after_header = file.stream_position()?;
            let remaining = length.saturating_sub(after_header);
            if fields.payload_len.saturating_add(TRAILER_SIZE as u64) > remaining {
                if !final_segment {
                    return Err(invalid_data("incomplete frame before final segment"));
                }
                truncate_tail(&mut file, frame_start, &mut report)?;
                break;
            }
            let payload_len = fields.payload_len as usize;
            let mut payload = vec![0u8; payload_len];
            let payload_read = read_partial(&mut file, &mut payload)?;
            if payload_read != payload_len {
                if !final_segment {
                    return Err(invalid_data("partial frame payload before final segment"));
                }
                truncate_tail(&mut file, frame_start, &mut report)?;
                break;
            }
            let mut trailer = [0u8; TRAILER_SIZE];
            let trailer_read = read_partial(&mut file, &mut trailer)?;
            if trailer_read != TRAILER_SIZE {
                if !final_segment {
                    return Err(invalid_data("partial frame trailer before final segment"));
                }
                truncate_tail(&mut file, frame_start, &mut report)?;
                break;
            }
            validate_frame(&header, &payload, &trailer, fields.frame_len)?;
            if u128::from(fields.first_lsn) != next_lsn {
                return Err(invalid_data(format!(
                    "WAL LSN gap: expected {}, found {}",
                    next_lsn, fields.first_lsn
                )));
            }
            let count = fields.count as usize;
            for record_index in 0..count {
                let start = record_index * RECORD_SIZE;
                let recovered = &payload[start..start + RECORD_SIZE];
                let expected_record_lsn = next_lsn + record_index as u128;
                let record_lsn =
                    u64::from_le_bytes(recovered[0..8].try_into().expect("fixed record field"));
                if u128::from(record_lsn) != expected_record_lsn {
                    return Err(invalid_data(format!(
                        "WAL record LSN gap: expected {expected_record_lsn}, found {record_lsn}"
                    )));
                }
            }
            for record_index in 0..count {
                let start = record_index * RECORD_SIZE;
                let record = Record(
                    payload[start..start + RECORD_SIZE]
                        .try_into()
                        .expect("fixed record size"),
                );
                callback(record)?;
            }
            next_lsn += u128::from(fields.count);
            report.records = report
                .records
                .checked_add(u64::from(fields.count))
                .ok_or_else(|| invalid_data("WAL record count exceeds supported range"))?;
            report.bytes = report
                .bytes
                .checked_add(fields.frame_len)
                .ok_or_else(|| invalid_data("WAL byte count exceeds supported range"))?;
            frame_start += fields.frame_len;
        }
        if frame_start > length {
            return Err(invalid_data("WAL frame offset exceeded segment length"));
        }
    }
    Ok(report)
}

#[derive(Clone, Copy)]
struct HeaderFields {
    count: u32,
    payload_len: u64,
    first_lsn: u64,
    frame_len: u64,
}

pub(crate) fn encode_frame(records: &[Record]) -> io::Result<Vec<u8>> {
    let count = u32::try_from(records.len()).map_err(|_| invalid_input("WAL batch too large"))?;
    let payload_len = records
        .len()
        .checked_mul(RECORD_SIZE)
        .ok_or_else(|| invalid_input("WAL frame too large"))?;
    if payload_len as u64 > MAX_FRAME_PAYLOAD {
        return Err(invalid_input(
            "WAL batch exceeds the 64 MiB recovery frame limit",
        ));
    }
    let frame_len = HEADER_SIZE
        .checked_add(payload_len)
        .and_then(|n| n.checked_add(TRAILER_SIZE))
        .ok_or_else(|| invalid_input("WAL frame too large"))?;
    let mut frame = Vec::with_capacity(frame_len);
    frame.extend_from_slice(MAGIC);
    frame.extend_from_slice(&VERSION.to_le_bytes());
    frame.extend_from_slice(&(HEADER_SIZE as u16).to_le_bytes());
    frame.extend_from_slice(&(RECORD_SIZE as u32).to_le_bytes());
    frame.extend_from_slice(&count.to_le_bytes());
    frame.extend_from_slice(&(payload_len as u64).to_le_bytes());
    frame.extend_from_slice(&records[0].lsn().to_le_bytes());
    for record in records {
        frame.extend_from_slice(&record.0);
    }
    let checksum = crc32(&frame);
    frame.extend_from_slice(&checksum.to_le_bytes());
    frame.extend_from_slice(&(frame_len as u64).to_le_bytes());
    frame.extend_from_slice(TRAILER_MAGIC);
    Ok(frame)
}

fn parse_header(header: &[u8; HEADER_SIZE]) -> io::Result<HeaderFields> {
    if &header[0..8] != MAGIC {
        return Err(invalid_data("invalid WAL frame magic"));
    }
    let version = u16::from_le_bytes(header[8..10].try_into().expect("fixed field"));
    let header_size = u16::from_le_bytes(header[10..12].try_into().expect("fixed field"));
    let record_size = u32::from_le_bytes(header[12..16].try_into().expect("fixed field"));
    let count = u32::from_le_bytes(header[16..20].try_into().expect("fixed field"));
    let payload_len = u64::from_le_bytes(header[20..28].try_into().expect("fixed field"));
    let first_lsn = u64::from_le_bytes(header[28..36].try_into().expect("fixed field"));
    if version != VERSION
        || header_size as usize != HEADER_SIZE
        || record_size as usize != RECORD_SIZE
        || count == 0
    {
        return Err(invalid_data("unsupported or invalid WAL frame header"));
    }
    if payload_len != u64::from(count) * RECORD_SIZE as u64 {
        return Err(invalid_data(
            "WAL payload length does not match record count",
        ));
    }
    let frame_len = HEADER_SIZE as u64 + payload_len + TRAILER_SIZE as u64;
    Ok(HeaderFields {
        count,
        payload_len,
        first_lsn,
        frame_len,
    })
}

fn validate_frame(
    header: &[u8],
    payload: &[u8],
    trailer: &[u8; TRAILER_SIZE],
    frame_len: u64,
) -> io::Result<()> {
    let checksum = u32::from_le_bytes(trailer[0..4].try_into().expect("fixed field"));
    let declared_len = u64::from_le_bytes(trailer[4..12].try_into().expect("fixed field"));
    if declared_len != frame_len || &trailer[12..20] != TRAILER_MAGIC {
        return Err(invalid_data("invalid WAL frame trailer"));
    }
    let mut actual = crc32(header);
    actual = crc32_extend(actual, payload);
    if actual != checksum {
        return Err(invalid_data("WAL frame checksum mismatch"));
    }
    Ok(())
}

fn read_partial(reader: &mut impl Read, bytes: &mut [u8]) -> io::Result<usize> {
    let mut total = 0;
    while total < bytes.len() {
        match reader.read(&mut bytes[total..])? {
            0 => break,
            count => total += count,
        }
    }
    Ok(total)
}

fn truncate_tail(file: &mut File, start: u64, report: &mut RecoveryReport) -> io::Result<()> {
    let end = file.seek(SeekFrom::End(0))?;
    report.truncated_tail_bytes += end.saturating_sub(start);
    file.set_len(start)?;
    file.sync_all()?;
    Ok(())
}

const fn make_crc_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut index = 0;
    while index < table.len() {
        let mut crc = index as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = (crc >> 1) ^ (0xedb8_8320 & (0u32.wrapping_sub(crc & 1)));
            bit += 1;
        }
        table[index] = crc;
        index += 1;
    }
    table
}

const CRC_TABLE: [u32; 256] = make_crc_table();

fn crc32(bytes: &[u8]) -> u32 {
    crc32_extend(0, bytes)
}

fn crc32_extend(previous: u32, bytes: &[u8]) -> u32 {
    let mut crc = !previous;
    for byte in bytes {
        let index = ((crc ^ u32::from(*byte)) & 0xff) as usize;
        crc = (crc >> 8) ^ CRC_TABLE[index];
    }
    !crc
}

pub(crate) fn segment_name(number: u64) -> String {
    format!("segment-{number:06}.wal")
}

fn parse_segment_name(name: &str) -> Option<u64> {
    let number = name.strip_prefix("segment-")?.strip_suffix(".wal")?;
    if number.len() < 6 || !number.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let parsed = number.parse().ok()?;
    (segment_name(parsed) == name).then_some(parsed)
}

fn elapsed_ns(start: Instant) -> u64 {
    start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
}
fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}
fn invalid_input(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEMP_DIR: AtomicU64 = AtomicU64::new(0);

    fn temp_dir(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "ledger-wallet-wal-{label}-{}-{}-{}",
            std::process::id(),
            SystemTimeNow::nanos(),
            NEXT_TEMP_DIR.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        path
    }

    #[test]
    fn appends_batches_and_recovers_across_rotated_segments() {
        let dir = temp_dir("rotate");
        let records = generate_records(9);
        let mut writer = WalWriter::new(&dir, 300).unwrap();
        for batch in records.chunks(3) {
            writer.append_batch(batch).unwrap();
        }
        let recovered = recover_and_verify(&dir, &records).unwrap();
        assert_eq!(recovered.records, 9);
        assert!(recovered.segments >= 2);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn recovers_valid_prefix_and_truncates_a_torn_final_batch() {
        let dir = temp_dir("torn");
        let records = generate_records(8);
        let mut writer = WalWriter::new(&dir, 1_000_000).unwrap();
        writer.append_batch(&records[..4]).unwrap();
        writer.append_batch(&records[4..]).unwrap();
        let path = dir.join(segment_name(0));
        let first_frame_len = HEADER_SIZE as u64 + 4 * RECORD_SIZE as u64 + TRAILER_SIZE as u64;
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(first_frame_len + HEADER_SIZE as u64 + 9)
            .unwrap();
        let report = recover_prefix(&dir).unwrap();
        assert_eq!(report.records, 4);
        assert_eq!(report.truncated_tail_bytes, HEADER_SIZE as u64 + 9);
        assert_eq!(fs::metadata(&path).unwrap().len(), first_frame_len);
        assert_eq!(
            recover_and_verify(&dir, &records).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn rejects_corrupt_complete_frames() {
        let dir = temp_dir("corrupt");
        let records = generate_records(4);
        WalWriter::new(&dir, 1_000_000)
            .unwrap()
            .append_batch(&records)
            .unwrap();
        let path = dir.join(segment_name(0));
        let mut bytes = fs::read(&path).unwrap();
        bytes[HEADER_SIZE + 3] ^= 0x40;
        fs::write(&path, bytes).unwrap();
        assert_eq!(
            recover_and_verify(&dir, &records).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn append_failure_poisoning_prevents_a_later_false_acknowledgement() {
        let dir = temp_dir("poisoned");
        let records = generate_records(1);
        let mut writer = WalWriter::new(&dir, 1_000_000).unwrap();
        fs::remove_dir_all(&dir).unwrap();
        assert!(writer.append_batch(&records).is_err());
        fs::create_dir(&dir).unwrap();
        assert!(writer.append_batch(&records).is_err());
        assert_eq!(fs::read_dir(&dir).unwrap().count(), 0);
        fs::remove_dir_all(dir).unwrap();
    }
}

#[cfg(test)]
struct SystemTimeNow;
#[cfg(test)]
impl SystemTimeNow {
    fn nanos() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    }
}
