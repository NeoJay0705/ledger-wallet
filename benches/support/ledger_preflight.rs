//! Linux-only helpers for checking benchmark conditions and bracketing I/O.
//!
//! This module deliberately reads kernel procfs interfaces directly. A missing
//! or unparsable metric is an error so callers cannot accidentally run a
//! benchmark without the requested preflight checks.

use std::ffi::CString;
use std::fs;
use std::io;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

const DISKSTATS_SECTOR_BYTES: u64 = 512;
const RETRY_WAIT: Duration = Duration::from_millis(200);

/// Thresholds used by [`ensure_idle`].
#[derive(Clone, Debug)]
pub struct PreflightConfig {
    pub observation: Duration,
    pub timeout: Duration,
    pub max_cpu_busy_pct: f64,
    pub max_disk_busy_pct: f64,
    pub min_available_mem_bytes: u64,
    pub min_free_bytes: u64,
}

/// Measurements from the first observation that met every configured limit.
#[derive(Clone, Debug)]
pub struct PreflightReport {
    /// Canonical path used to resolve the target filesystem.
    pub path: PathBuf,
    pub mount_point: PathBuf,
    pub filesystem: String,
    pub device: String,
    pub major_minor: String,
    pub attempts: u32,
    pub observation: Duration,
    pub cpu_busy_pct: f64,
    pub disk_busy_pct: f64,
    pub mem_available_bytes: u64,
    pub free_bytes: u64,
}

/// A point-in-time pair of process I/O counters and target block-device counters.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IoSample {
    pub process_rchar_bytes: u64,
    pub process_wchar_bytes: u64,
    pub process_read_bytes: u64,
    pub process_write_bytes: u64,
    pub target_device: String,
    pub target_major_minor: String,
    pub target_read_bytes: u64,
    pub target_write_bytes: u64,
    pub target_busy_ms: u64,
}

/// Wait until CPU, memory, free-space, and target-device activity meet `cfg`.
///
/// The filesystem and block-device mapping are checked before retrying. The
/// resource readings are then rechecked on each bounded retry until `timeout`.
/// The target path (or its nearest existing parent) must resolve to a
/// nonvolatile filesystem whose block device appears in readable diskstats.
pub fn ensure_idle(path: &Path, cfg: &PreflightConfig) -> Result<PreflightReport, String> {
    validate_config(cfg)?;

    let resolved_path = canonical_existing_path(path)?;
    let mount = find_mount(&resolved_path)?;
    if matches!(mount.filesystem.as_str(), "tmpfs" | "ramfs" | "devtmpfs") {
        return Err(format!(
            "benchmark target {} is on volatile filesystem {} mounted at {}",
            path.display(),
            mount.filesystem,
            mount.mount_point.display()
        ));
    }
    let device = disk_device_for_mount(&mount)?;

    let started = Instant::now();
    let deadline = started
        .checked_add(cfg.timeout)
        .ok_or_else(|| "preflight timeout is too large".to_owned())?;
    let mut attempts = 0_u32;
    let mut last = None;

    loop {
        if Instant::now() >= deadline {
            return Err(timeout_message(attempts, last.as_ref(), cfg));
        }
        if attempts > 0 {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining < cfg.observation {
                wait_bounded(remaining);
                return Err(timeout_message(attempts, last.as_ref(), cfg));
            }
        }

        attempts = attempts.saturating_add(1);
        let observation_started = Instant::now();
        let cpu_before = read_cpu_ticks()?;
        let disk_before = read_disk_sample(&device.major_minor)?;
        thread::sleep(cfg.observation);
        let disk_after = read_disk_sample(&device.major_minor)?;
        let cpu_after = read_cpu_ticks()?;
        let elapsed = observation_started.elapsed();

        let cpu_busy_pct = cpu_busy_percent(cpu_before, cpu_after)?;
        let busy_delta = disk_after
            .busy_ms
            .checked_sub(disk_before.busy_ms)
            .ok_or_else(|| {
                "target device busy time moved backwards in /proc/diskstats".to_owned()
            })?;
        let elapsed_ms = elapsed.as_secs_f64() * 1_000.0;
        if !elapsed_ms.is_finite() || elapsed_ms <= 0.0 {
            return Err("preflight could not measure a positive observation interval".to_owned());
        }
        let disk_busy_pct = busy_delta as f64 / elapsed_ms * 100.0;
        let mem_available_bytes = read_mem_available_bytes()?;
        let free_bytes = available_space_bytes(&resolved_path)?;

        let report = PreflightReport {
            path: resolved_path.clone(),
            mount_point: mount.mount_point.clone(),
            filesystem: mount.filesystem.clone(),
            device: device.name.clone(),
            major_minor: device.major_minor.clone(),
            attempts,
            observation: elapsed,
            cpu_busy_pct,
            disk_busy_pct,
            mem_available_bytes,
            free_bytes,
        };
        let ready = cpu_busy_pct <= cfg.max_cpu_busy_pct
            && disk_busy_pct <= cfg.max_disk_busy_pct
            && mem_available_bytes >= cfg.min_available_mem_bytes
            && free_bytes >= cfg.min_free_bytes;
        last = Some(report.clone());
        if ready {
            return Ok(report);
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(timeout_message(attempts, last.as_ref(), cfg));
        }
        wait_bounded(remaining.min(RETRY_WAIT));
    }
}

/// Read process I/O counters and cumulative counters for `path`'s block device.
pub fn sample_io(path: &Path) -> Result<IoSample, String> {
    let resolved_path = canonical_existing_path(path)?;
    let mount = find_mount(&resolved_path)?;
    if matches!(mount.filesystem.as_str(), "tmpfs" | "ramfs" | "devtmpfs") {
        return Err(format!(
            "benchmark target {} is on volatile filesystem {} mounted at {}",
            path.display(),
            mount.filesystem,
            mount.mount_point.display()
        ));
    }
    let device = disk_device_for_mount(&mount)?;
    let process = read_process_io()?;
    let disk = read_disk_sample(&device.major_minor)?;
    Ok(IoSample {
        process_rchar_bytes: process.rchar,
        process_wchar_bytes: process.wchar,
        process_read_bytes: process.read_bytes,
        process_write_bytes: process.write_bytes,
        target_device: device.name,
        target_major_minor: device.major_minor,
        target_read_bytes: disk.read_bytes,
        target_write_bytes: disk.write_bytes,
        target_busy_ms: disk.busy_ms,
    })
}

fn validate_config(cfg: &PreflightConfig) -> Result<(), String> {
    if cfg.observation.is_zero() {
        return Err("preflight observation must be greater than zero".to_owned());
    }
    if cfg.timeout.is_zero() {
        return Err("preflight timeout must be greater than zero".to_owned());
    }
    if cfg.observation > cfg.timeout {
        return Err("preflight observation must not exceed its timeout".to_owned());
    }
    if !cfg.max_cpu_busy_pct.is_finite() || !(0.0..=100.0).contains(&cfg.max_cpu_busy_pct) {
        return Err(
            "preflight maximum CPU busy percentage must be finite and in 0..=100".to_owned(),
        );
    }
    if !cfg.max_disk_busy_pct.is_finite() || !(0.0..=100.0).contains(&cfg.max_disk_busy_pct) {
        return Err(
            "preflight maximum disk busy percentage must be finite and in 0..=100".to_owned(),
        );
    }
    Ok(())
}

fn canonical_existing_path(path: &Path) -> Result<PathBuf, String> {
    let absolute_path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|error| format!("cannot resolve current directory: {error}"))?
            .join(path)
    };
    let mut candidate = absolute_path.as_path();
    loop {
        match fs::canonicalize(candidate) {
            Ok(canonical) => return Ok(canonical),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                candidate = candidate.parent().ok_or_else(|| {
                    format!(
                        "cannot resolve benchmark target {}: {error}",
                        path.display()
                    )
                })?;
            }
            Err(error) => {
                return Err(format!(
                    "cannot resolve benchmark target {}: {error}",
                    path.display()
                ));
            }
        }
    }
}

#[derive(Clone, Debug)]
struct MountInfo {
    mount_point: PathBuf,
    major_minor: String,
    filesystem: String,
}

fn find_mount(path: &Path) -> Result<MountInfo, String> {
    let contents = fs::read_to_string("/proc/self/mountinfo")
        .map_err(|error| format!("cannot read /proc/self/mountinfo: {error}"))?;
    find_mount_in(&contents, path)
}

fn find_mount_in(contents: &str, path: &Path) -> Result<MountInfo, String> {
    let mut best: Option<(usize, MountInfo)> = None;
    for (line_no, line) in contents.lines().enumerate() {
        let Some((left, right)) = line.split_once(" - ") else {
            continue;
        };
        let before: Vec<_> = left.split_whitespace().collect();
        let after: Vec<_> = right.split_whitespace().collect();
        if before.len() < 5 || after.len() < 2 {
            continue;
        }
        let mount_point = decode_mount_field(before[4]).map_err(|error| {
            format!(
                "invalid mountpoint escaping on mountinfo line {}: {error}",
                line_no + 1
            )
        })?;
        if !path.starts_with(&mount_point) {
            continue;
        }
        let depth = mount_point.components().count();
        if best
            .as_ref()
            .is_none_or(|(old_depth, _)| depth >= *old_depth)
        {
            best = Some((
                depth,
                MountInfo {
                    mount_point,
                    major_minor: before[2].to_owned(),
                    filesystem: after[0].to_owned(),
                },
            ));
        }
    }
    best.map(|(_, mount)| mount)
        .ok_or_else(|| format!("no mountinfo entry contains target {}", path.display()))
}

fn decode_mount_field(field: &str) -> Result<PathBuf, String> {
    let encoded = field.as_bytes();
    let mut decoded = Vec::with_capacity(encoded.len());
    let mut i = 0;
    while i < encoded.len() {
        if encoded[i] == b'\\' {
            if i + 3 >= encoded.len() {
                return Err("truncated octal escape".to_owned());
            }
            let digits = &encoded[i + 1..i + 4];
            if !digits.iter().all(|digit| (b'0'..=b'7').contains(digit)) {
                return Err("invalid octal escape".to_owned());
            }
            let value = (digits[0] - b'0') * 64 + (digits[1] - b'0') * 8 + (digits[2] - b'0');
            decoded.push(value);
            i += 4;
        } else {
            decoded.push(encoded[i]);
            i += 1;
        }
    }
    Ok(PathBuf::from(std::ffi::OsString::from_vec(decoded)))
}

#[derive(Clone, Debug)]
struct DeviceInfo {
    name: String,
    major_minor: String,
}

fn disk_device_for_mount(mount: &MountInfo) -> Result<DeviceInfo, String> {
    let diskstats = fs::read_to_string("/proc/diskstats")
        .map_err(|error| format!("cannot read /proc/diskstats: {error}"))?;
    let mut matches = parse_diskstats(&diskstats)
        .into_iter()
        .filter(|entry| entry.major_minor == mount.major_minor);
    let entry = matches.next().ok_or_else(|| {
        format!(
            "target block device {} for {} is missing from /proc/diskstats",
            mount.major_minor,
            mount.mount_point.display()
        )
    })?;
    if matches.next().is_some() {
        return Err(format!(
            "target block device {} has duplicate /proc/diskstats entries",
            mount.major_minor
        ));
    }
    Ok(DeviceInfo {
        name: entry.name,
        major_minor: entry.major_minor,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DiskSample {
    major_minor: String,
    name: String,
    read_bytes: u64,
    write_bytes: u64,
    busy_ms: u64,
}

fn parse_diskstats(contents: &str) -> Vec<DiskSample> {
    contents
        .lines()
        .filter_map(|line| {
            let fields: Vec<_> = line.split_whitespace().collect();
            if fields.len() < 13 {
                return None;
            }
            let major: u32 = fields[0].parse().ok()?;
            let minor: u32 = fields[1].parse().ok()?;
            let sectors_read: u64 = fields[5].parse().ok()?;
            let sectors_written: u64 = fields[9].parse().ok()?;
            let busy_ms: u64 = fields[12].parse().ok()?;
            Some(DiskSample {
                major_minor: format!("{major}:{minor}"),
                name: fields[2].to_owned(),
                read_bytes: sectors_read.checked_mul(DISKSTATS_SECTOR_BYTES)?,
                write_bytes: sectors_written.checked_mul(DISKSTATS_SECTOR_BYTES)?,
                busy_ms,
            })
        })
        .collect()
}

fn read_disk_sample(major_minor: &str) -> Result<DiskSample, String> {
    let contents = fs::read_to_string("/proc/diskstats")
        .map_err(|error| format!("cannot read /proc/diskstats: {error}"))?;
    let mut matches = parse_diskstats(&contents)
        .into_iter()
        .filter(|entry| entry.major_minor == major_minor);
    let sample = matches.next().ok_or_else(|| {
        format!("target block device {major_minor} is missing from /proc/diskstats")
    })?;
    if matches.next().is_some() {
        return Err(format!(
            "target block device {major_minor} has duplicate /proc/diskstats entries"
        ));
    }
    Ok(sample)
}

fn read_mem_available_bytes() -> Result<u64, String> {
    let contents = fs::read_to_string("/proc/meminfo")
        .map_err(|error| format!("cannot read /proc/meminfo: {error}"))?;
    parse_mem_available_bytes(&contents)
}

fn parse_mem_available_bytes(contents: &str) -> Result<u64, String> {
    let kib: u64 = contents
        .lines()
        .find_map(|line| {
            line.strip_prefix("MemAvailable:")
                .and_then(|value| value.split_whitespace().next())
                .and_then(|value| value.parse().ok())
        })
        .ok_or_else(|| "MemAvailable is absent or malformed in /proc/meminfo".to_owned())?;
    kib.checked_mul(1024)
        .ok_or_else(|| "MemAvailable byte count overflowed".to_owned())
}

#[derive(Clone, Copy, Debug)]
struct CpuTicks {
    total: u64,
    idle: u64,
}

fn read_cpu_ticks() -> Result<CpuTicks, String> {
    let contents = fs::read_to_string("/proc/stat")
        .map_err(|error| format!("cannot read /proc/stat: {error}"))?;
    parse_cpu_ticks(&contents)
}

fn parse_cpu_ticks(contents: &str) -> Result<CpuTicks, String> {
    let line = contents
        .lines()
        .find(|line| line.split_whitespace().next() == Some("cpu"))
        .ok_or_else(|| "aggregate CPU line is absent from /proc/stat".to_owned())?;
    let fields: Vec<_> = line.split_whitespace().skip(1).collect();
    if fields.len() < 4 {
        return Err("aggregate CPU line is incomplete in /proc/stat".to_owned());
    }
    // guest and guest_nice are already included in user and nice, so exclude
    // those optional fields when calculating the total.
    let mut values = Vec::with_capacity(fields.len().min(8));
    for field in fields.iter().take(8) {
        values.push(
            field
                .parse::<u64>()
                .map_err(|_| "aggregate CPU line has a malformed tick count".to_owned())?,
        );
    }
    let total = values
        .iter()
        .try_fold(0_u64, |sum, value| sum.checked_add(*value))
        .ok_or_else(|| "aggregate CPU tick total overflowed".to_owned())?;
    let idle = values[3]
        .checked_add(values.get(4).copied().unwrap_or(0))
        .ok_or_else(|| "aggregate CPU idle tick total overflowed".to_owned())?;
    Ok(CpuTicks { total, idle })
}

fn cpu_busy_percent(before: CpuTicks, after: CpuTicks) -> Result<f64, String> {
    let total = after
        .total
        .checked_sub(before.total)
        .ok_or_else(|| "aggregate CPU ticks moved backwards".to_owned())?;
    let idle = after
        .idle
        .checked_sub(before.idle)
        .ok_or_else(|| "aggregate CPU idle ticks moved backwards".to_owned())?;
    if total == 0 || idle > total {
        return Err("preflight could not measure aggregate CPU busy time".to_owned());
    }
    Ok((total - idle) as f64 / total as f64 * 100.0)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ProcessIo {
    rchar: u64,
    wchar: u64,
    read_bytes: u64,
    write_bytes: u64,
}

fn read_process_io() -> Result<ProcessIo, String> {
    let contents = fs::read_to_string("/proc/self/io")
        .map_err(|error| format!("cannot read /proc/self/io: {error}"))?;
    parse_process_io(&contents)
}

fn parse_process_io(contents: &str) -> Result<ProcessIo, String> {
    let mut rchar = None;
    let mut wchar = None;
    let mut read_bytes = None;
    let mut write_bytes = None;
    for line in contents.lines() {
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        let target = match key.trim() {
            "rchar" => &mut rchar,
            "wchar" => &mut wchar,
            "read_bytes" => &mut read_bytes,
            "write_bytes" => &mut write_bytes,
            _ => continue,
        };
        *target = Some(
            value
                .trim()
                .parse::<u64>()
                .map_err(|_| format!("malformed {key} counter in /proc/self/io"))?,
        );
    }
    Ok(ProcessIo {
        rchar: rchar.ok_or_else(|| "/proc/self/io is missing rchar".to_owned())?,
        wchar: wchar.ok_or_else(|| "/proc/self/io is missing wchar".to_owned())?,
        read_bytes: read_bytes.ok_or_else(|| "/proc/self/io is missing read_bytes".to_owned())?,
        write_bytes: write_bytes
            .ok_or_else(|| "/proc/self/io is missing write_bytes".to_owned())?,
    })
}

fn available_space_bytes(path: &Path) -> Result<u64, String> {
    let c_path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| format!("path {} contains a NUL byte", path.display()))?;
    // SAFETY: `c_path` is NUL terminated and `stats` points to writable memory.
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    let result = unsafe { libc::statvfs(c_path.as_ptr(), stats.as_mut_ptr()) };
    if result != 0 {
        return Err(format!(
            "cannot inspect available space for {}: {}",
            path.display(),
            io::Error::last_os_error()
        ));
    }
    // SAFETY: statvfs initialized the output structure on success.
    let stats = unsafe { stats.assume_init() };
    let blocks = u128::try_from(stats.f_bavail)
        .map_err(|_| "filesystem available block count is negative".to_owned())?;
    let block_size = if stats.f_frsize == 0 {
        u128::try_from(stats.f_bsize).map_err(|_| "filesystem block size is negative".to_owned())?
    } else {
        u128::try_from(stats.f_frsize)
            .map_err(|_| "filesystem fragment size is negative".to_owned())?
    };
    let bytes = blocks
        .checked_mul(block_size)
        .and_then(|value| u64::try_from(value).ok())
        .ok_or_else(|| "filesystem available byte count overflowed".to_owned())?;
    Ok(bytes)
}

fn wait_bounded(duration: Duration) {
    let mut remaining = duration;
    while !remaining.is_zero() {
        let chunk = remaining.min(RETRY_WAIT);
        let started = Instant::now();
        thread::sleep(chunk);
        remaining = remaining.saturating_sub(started.elapsed());
    }
}

fn timeout_message(attempts: u32, last: Option<&PreflightReport>, cfg: &PreflightConfig) -> String {
    match last {
        Some(report) => format!(
            "idle preflight timed out after {attempts} attempt(s); last observation: CPU busy {:.3}% / {:.3}% max, target {} busy {:.3}% / {:.3}% max, MemAvailable {} / {} bytes min, free {} / {} bytes min",
            report.cpu_busy_pct,
            cfg.max_cpu_busy_pct,
            report.device,
            report.disk_busy_pct,
            cfg.max_disk_busy_pct,
            report.mem_available_bytes,
            cfg.min_available_mem_bytes,
            report.free_bytes,
            cfg.min_free_bytes,
        ),
        None => format!(
            "idle preflight timed out after {attempts} attempt(s) without a complete observation"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_config() -> PreflightConfig {
        PreflightConfig {
            observation: Duration::from_millis(10),
            timeout: Duration::from_secs(1),
            max_cpu_busy_pct: 10.0,
            max_disk_busy_pct: 5.0,
            min_available_mem_bytes: 0,
            min_free_bytes: 0,
        }
    }

    #[test]
    fn decodes_mountinfo_octal_escapes_and_selects_longest_mount() {
        let mounts = concat!(
            "36 25 8:1 / / rw,relatime - ext4 /dev/sda1 rw\n",
            "37 36 8:17 / /mnt/a\\040b rw,relatime - ext4 /dev/sda17 rw\n",
            "38 37 8:18 / /mnt/a\\040b/deep\\011mount rw,relatime - xfs /dev/sdb1 rw\n",
        );
        let path = PathBuf::from("/mnt/a b/deep\tmount/db");
        let mount = find_mount_in(mounts, &path).unwrap();
        assert_eq!(mount.mount_point, PathBuf::from("/mnt/a b/deep\tmount"));
        assert_eq!(mount.major_minor, "8:18");
        assert_eq!(mount.filesystem, "xfs");
    }

    #[test]
    fn rejects_malformed_mount_escape_for_matching_line() {
        let mounts = "36 25 8:1 / /mnt/bad\\04 rw - ext4 /dev/sda1 rw\n";
        assert!(find_mount_in(mounts, Path::new("/mnt/bad/db")).is_err());
    }

    #[test]
    fn parses_cpu_idle_and_excludes_guest_ticks_from_total() {
        let before = parse_cpu_ticks("cpu 100 10 20 200 30 4 5 6 70 8\n").unwrap();
        let after = parse_cpu_ticks("cpu 120 10 30 220 40 4 5 6 90 8\n").unwrap();
        assert_eq!(before.total, 375);
        assert_eq!(before.idle, 230);
        assert!((cpu_busy_percent(before, after).unwrap() - 50.0).abs() < f64::EPSILON);
    }

    #[test]
    fn parses_memavailable_and_rejects_missing_or_overflowing_value() {
        assert_eq!(
            parse_mem_available_bytes("MemTotal: 100 kB\nMemAvailable: 42 kB\n").unwrap(),
            42 * 1024
        );
        assert!(parse_mem_available_bytes("MemTotal: 100 kB\n").is_err());
        assert!(parse_mem_available_bytes("MemAvailable: 18446744073709551615 kB\n").is_err());
    }

    #[test]
    fn parses_proc_io_required_byte_counters() {
        let sample = parse_process_io(
            "rchar: 100\nwchar: 200\nsyscr: 4\nsyscw: 5\nread_bytes: 300\nwrite_bytes: 400\n",
        )
        .unwrap();
        assert_eq!(
            sample,
            ProcessIo {
                rchar: 100,
                wchar: 200,
                read_bytes: 300,
                write_bytes: 400
            }
        );
        assert!(parse_process_io("rchar: 1\nwchar: 2\nread_bytes: 3\n").is_err());
    }

    #[test]
    fn parses_target_diskstats_sectors_and_busy_time() {
        let samples = parse_diskstats("   8       1 sda1 10 2 3 4 5 6 7 8 9 10 11 12 13\n");
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].major_minor, "8:1");
        assert_eq!(samples[0].name, "sda1");
        assert_eq!(samples[0].read_bytes, 3 * DISKSTATS_SECTOR_BYTES);
        assert_eq!(samples[0].write_bytes, 7 * DISKSTATS_SECTOR_BYTES);
        assert_eq!(samples[0].busy_ms, 10);
        assert!(parse_diskstats("8 1 sda1 1 2 3\n").is_empty());
    }

    #[test]
    fn invalid_thresholds_and_observation_windows_fail_closed() {
        let mut cfg = valid_config();
        cfg.observation = Duration::ZERO;
        assert!(validate_config(&cfg).unwrap_err().contains("observation"));

        let mut cfg = valid_config();
        cfg.observation = Duration::from_secs(2);
        assert!(
            validate_config(&cfg)
                .unwrap_err()
                .contains("must not exceed")
        );

        let mut cfg = valid_config();
        cfg.max_cpu_busy_pct = f64::NAN;
        assert!(validate_config(&cfg).unwrap_err().contains("CPU"));

        let mut cfg = valid_config();
        cfg.max_disk_busy_pct = 100.01;
        assert!(validate_config(&cfg).unwrap_err().contains("disk"));
    }

    #[test]
    fn cpu_counters_that_move_backwards_are_rejected() {
        let before = CpuTicks { total: 10, idle: 5 };
        let after = CpuTicks { total: 9, idle: 4 };
        assert!(cpu_busy_percent(before, after).is_err());
    }
}
