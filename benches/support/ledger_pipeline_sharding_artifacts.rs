//! Immutable per-trial artifacts and validated matrix reports for the
//! multi-shard Ledger Pipeline benchmark.

use super::*;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const SUMMARY_FILE: &str = "summary.csv";
const SHARDS_FILE: &str = "shards.csv";
const STAGES_FILE: &str = "stages.csv";
const RAW_STAGES_FILE: &str = "raw_stage_samples.csv";
const REQUEST_SAMPLES_FILE: &str = "request_samples.csv";
const BACKGROUND_FILE: &str = "background.csv";
const INDEX_BATCHES_FILE: &str = "index_batches.csv";
const INDEX_GROUPS_FILE: &str = "index_groups.csv";
const METADATA_FILE: &str = "metadata.txt";

const INDEX_BATCH_HEADER: &str = "shard_id,batch_index,transaction_count,dispatch_wait_ns,batch_gate_wait_ns,key_prep_ns,query_wall_ns,blocking_pool_wait_ns,native_get_ns,decode_ns,submit_to_collection_ns,apply_submit_to_collection_ns,apply_blocking_pool_wait_ns,sequential_apply_build_ns,sync_write_batch_ns,memory_publish_ns,get_calls,keys_looked_up,hits,misses,groups_submitted,max_observed_in_flight_groups,max_observed_running_query_jobs";
const INDEX_GROUP_HEADER: &str = "shard_id,batch_index,group_index,first_position,key_count,blocking_pool_wait_ns,native_get_ns,decode_ns,submit_to_collection_ns";

const REQUEST_METRICS: [(&str, &str); 7] = [
    ("request.total", "total_ns"),
    ("request.admission", "admission_ns"),
    ("request.enqueue", "enqueue_ns"),
    ("request.queue", "queue_ns"),
    ("request.batch", "batch_ns"),
    ("request.handler", "handler_ns"),
    ("request.response", "response_ns"),
];
const STAGE_METRICS: [&str; 36] = [
    "request.total",
    "request.admission",
    "request.enqueue",
    "request.queue",
    "request.batch",
    "request.handler",
    "request.response",
    "index.dispatch_wait",
    "index.batch_gate_wait",
    "index.key_prep",
    "index.query_wall",
    "index.blocking_pool_wait",
    "index.native_get",
    "index.decode",
    "index.submit_to_collection",
    "index.apply_submit_to_collection",
    "index.apply_blocking_pool_wait",
    "index.sequential_apply_build",
    "index.sync_write_batch",
    "index.memory_publish",
    "index.group_blocking_pool_wait",
    "index.group_native_get",
    "index.group_decode",
    "index.group_submit_to_collection",
    "projection.read",
    "projection.apply",
    "projection.progress_sync",
    "projection.total",
    "watermark.fence_wait",
    "watermark.projection_wait",
    "watermark.persist",
    "watermark.total",
    "gc.scan",
    "gc.delete_build",
    "gc.sync_write",
    "gc.total",
];

const SUMMARY_PREFIX: &str = "trial_name,case,shards,layout,index_group_size,index_concurrency,users,requests_per_user,requests,credits,debits,seed_records,final_sequence_sum,client_wall_s,client_rps,cpu_s,cpu_core_equivalents,cpu_us_per_request,settlement_s,finite_drain_rps,recovery_s,integrity_s,rss_peak_bytes,db_bytes,database_count,configured_write_buffer_bytes,configured_block_cache_bytes,configured_max_background_jobs,wal_syncs,wal_bytes,writes_with_wal,flush_write_bytes,compaction_read_bytes,compaction_write_bytes,rocks_stall_us,process_rchar_bytes,process_wchar_bytes,process_read_bytes,process_write_bytes,device,target_major_minor,device_read_bytes,device_write_bytes,device_busy_ms,cpu_sample_offset_us,progress_sample_offset_us,io_sample_offset_us,storage_sample_offset_us,sample_stride,request_sample_count,projection_backlog_sum,gc_backlog_sum,global_theoretical_query_group_cap,global_observed_query_group_peak,global_observed_query_job_peak,max_in_flight_per_batch,max_running_query_jobs_per_batch,setup_preflight_cpu_pct,setup_preflight_device_pct,measure_preflight_cpu_pct,measure_preflight_device_pct,request_total_sample_count,request_total_p50_ns,request_total_p95_ns,request_total_p99_ns,request_admission_sample_count,request_admission_p50_ns,request_admission_p95_ns,request_admission_p99_ns,request_enqueue_sample_count,request_enqueue_p50_ns,request_enqueue_p95_ns,request_enqueue_p99_ns,request_queue_sample_count,request_queue_p50_ns,request_queue_p95_ns,request_queue_p99_ns,request_batch_sample_count,request_batch_p50_ns,request_batch_p95_ns,request_batch_p99_ns,request_handler_sample_count,request_handler_p50_ns,request_handler_p95_ns,request_handler_p99_ns,request_response_sample_count,request_response_p50_ns,request_response_p95_ns,request_response_p99_ns";
const SHARDS_HEADER: &str = "trial_name,case,shards,shard_id,account_count,requests,credits,debits,client_end_latest_seq,client_end_projected_seq,client_end_destination_seq,client_end_gc_prefix,projection_backlog_records,destination_backlog_records,gc_backlog_records,final_sequence,final_projected_sequence,final_destination_sequence,final_gc_prefix,final_boundary_timestamp_us,final_boundary_target_sequence,client_wall_s,common_window_rps,max_in_flight_groups_per_batch,max_running_query_jobs_per_batch";
const STAGES_HEADER: &str =
    "trial_name,case,scope,shard_id,metric,sample_count,mean_ns,p50_ns,p95_ns,p99_ns";
const RAW_STAGES_HEADER: &str = "metric,shard_id,logical_id,value_ns";
const REQUEST_SAMPLES_HEADER: &str =
    "logical_id,shard_id,total_ns,admission_ns,enqueue_ns,queue_ns,batch_ns,handler_ns,response_ns";
const BACKGROUND_HEADER: &str = "shard_id,event,elapsed_ns,sequence,records,read_ns,apply_ns,progress_sync_ns,total_ns,scanned,deleted,bytes_deleted,gc_prefix_seq,blocked_at_seq,gc_scan_ns,gc_delete_ns,gc_write_ns,watermark_target_sequence,watermark,watermark_fence_wait_ns,watermark_projection_wait_ns,watermark_persist_ns";

#[derive(Clone)]
struct CsvData {
    header: Vec<String>,
    rows: Vec<Vec<String>>,
}

#[derive(Clone)]
struct TrialRecord {
    repetition: usize,
    position: usize,
    case: MatrixCase,
    trial_name: String,
    trial_dir: PathBuf,
    status: String,
    summary_header: Vec<String>,
    summary_row: Vec<String>,
    shard_header: Vec<String>,
    shard_rows: Vec<Vec<String>>,
    stage_header: Vec<String>,
    stage_rows: Vec<Vec<String>>,
    metadata: BTreeMap<String, String>,
    stdout: String,
    stderr: String,
}

#[derive(Clone, Copy)]
struct Distribution {
    count: usize,
    mean_ns: f64,
    p50: u64,
    p95: u64,
    p99: u64,
}

pub(super) fn write_trial_artifacts(
    output_dir: &Path,
    artifact: &TrialArtifacts,
) -> Result<(), String> {
    validate_artifact_values(artifact)?;
    create_raw_csv(
        output_dir,
        SUMMARY_FILE,
        &summary_header(),
        &[summary_row(artifact)?],
    )?;
    create_raw_csv(
        output_dir,
        SHARDS_FILE,
        &shard_header(),
        &shard_rows(artifact)?,
    )?;
    let stages = stage_rows(artifact)?;
    create_raw_csv(output_dir, STAGES_FILE, &stage_header(), &stages)?;
    let raw_stage_rows = effective_stage_samples(artifact)?
        .iter()
        .map(|sample| {
            vec![
                sample.metric.clone(),
                sample.shard_id.to_string(),
                option_u64(sample.logical_id),
                sample.value_ns.to_string(),
            ]
        })
        .collect::<Vec<_>>();
    create_raw_csv(
        output_dir,
        RAW_STAGES_FILE,
        &raw_stage_header(),
        &raw_stage_rows,
    )?;
    let request_rows = artifact
        .request_samples
        .iter()
        .map(|sample| {
            vec![
                sample.logical_id.to_string(),
                sample.shard_id.to_string(),
                sample.total_ns.to_string(),
                sample.admission_ns.to_string(),
                sample.enqueue_ns.to_string(),
                sample.queue_ns.to_string(),
                sample.batch_ns.to_string(),
                sample.handler_ns.to_string(),
                sample.response_ns.to_string(),
            ]
        })
        .collect::<Vec<_>>();
    create_raw_csv(
        output_dir,
        REQUEST_SAMPLES_FILE,
        &request_samples_header(),
        &request_rows,
    )?;
    let background_rows = artifact
        .background
        .iter()
        .filter(|sample| sample.elapsed_ns <= artifact.client_end_elapsed_ns)
        .map(|sample| {
            vec![
                sample.shard_id.to_string(),
                sample.event.clone(),
                sample.elapsed_ns.to_string(),
                sample.sequence.to_string(),
                sample.records.to_string(),
                sample.read_ns.to_string(),
                sample.apply_ns.to_string(),
                sample.progress_sync_ns.to_string(),
                sample.total_ns.to_string(),
                sample.scanned.to_string(),
                sample.deleted.to_string(),
                sample.bytes_deleted.to_string(),
                sample.gc_prefix_seq.to_string(),
                option_u64(sample.blocked_at_seq),
                sample.gc_scan_ns.to_string(),
                sample.gc_delete_ns.to_string(),
                sample.gc_write_ns.to_string(),
                sample.watermark_target_sequence.to_string(),
                sample.watermark.to_string(),
                sample.watermark_fence_wait_ns.to_string(),
                sample.watermark_projection_wait_ns.to_string(),
                sample.watermark_persist_ns.to_string(),
            ]
        })
        .collect::<Vec<_>>();
    create_raw_csv(
        output_dir,
        BACKGROUND_FILE,
        &background_header(),
        &background_rows,
    )?;
    create_index_file(
        output_dir,
        INDEX_BATCHES_FILE,
        INDEX_BATCH_HEADER,
        &artifact.index_batch_rows,
    )?;
    create_index_file(
        output_dir,
        INDEX_GROUPS_FILE,
        INDEX_GROUP_HEADER,
        &artifact.index_group_rows,
    )?;
    create_raw_text(output_dir, METADATA_FILE, &metadata_text(artifact)?)?;
    Ok(())
}

pub(super) fn validate_trial_artifacts(trial_dir: &Path) -> Result<(), String> {
    let metadata_path = trial_dir.join(METADATA_FILE);
    let metadata = read_metadata(&metadata_path)?;
    let shards = required_usize(&metadata, "shards", &metadata_path)?;
    let layout = required(&metadata, "layout", &metadata_path)?;
    let concurrency = required_usize(&metadata, "index_concurrency", &metadata_path)?;
    let users = required_usize(&metadata, "users", &metadata_path)?;
    let requests_per_user = required_usize(&metadata, "requests_per_user", &metadata_path)?;
    let case = case_name(shards, layout, concurrency)?;
    let trial_name = required(&metadata, "trial_name", &metadata_path)?;
    if required(&metadata, "case", &metadata_path)? != case
        || trial_name
            != trial_dir
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_default()
    {
        return Err(format!(
            "{} trial identity is inconsistent",
            metadata_path.display()
        ));
    }
    if ![2, 4].contains(&shards)
        || ![4, 8].contains(&concurrency)
        || users == 0
        || users % shards != 0
        || requests_per_user == 0
    {
        return Err(format!(
            "{} has invalid workload configuration",
            metadata_path.display()
        ));
    }
    let layout = RocksLayout::parse(layout)?;
    let expected_db_count = if layout == RocksLayout::Shared {
        1
    } else {
        shards
    };
    if required_usize(&metadata, "database_count", &metadata_path)? != expected_db_count {
        return Err(format!(
            "{} database count does not match layout",
            metadata_path.display()
        ));
    }
    let formal = users == 50_000 && requests_per_user == 200;
    let reduced_test = cfg!(test) && users == 2_048 && requests_per_user == 200;
    if !(formal || (users == 200 && requests_per_user == 200) || reduced_test) {
        return Err(format!(
            "{} workload is neither the formal matrix nor the reduced smoke profile",
            metadata_path.display()
        ));
    }
    for (key, value) in [
        ("index_strategy", "chunked"),
        ("index_group_size", "256"),
        ("balance_mode", "PerBatch"),
        ("runtime_worker_threads", "4"),
        ("batch_size", "2048"),
        ("first_dequeue_timeout_ms", "5"),
        ("projector_batch_size", "256"),
        ("watermark_interval_ms", "100"),
        ("retention_ms", "500"),
        ("gc_batch_size", "256"),
        ("gc_idle_interval_ms", "100"),
        ("request_sample_timestamps", "not_captured"),
        ("global_observed_query_group_peak", "NA"),
        ("global_observed_query_job_peak", "NA"),
        (
            "db_bytes_scope",
            "all_unique_database_directory_files_after_recovery_validation_and_close_before_scratch_cleanup",
        ),
        (
            "rocks_stats_scope",
            "unique_database_instances_counted_once",
        ),
        ("rocksdb_env_scope", "shared_default_env_for_child_process"),
        ("rocksdb_env_low_priority_threads", "6"),
        ("rocksdb_env_high_priority_threads", "2"),
        (
            "rocksdb_environment_pools_are_configured_limits_not_hard_active_thread_or_cpu_caps",
            "true",
        ),
        (
            "projection_durable_equals_destination_equals_latest",
            "true",
        ),
        ("cleanup_status", "owned_scratch_removed"),
        ("integrity_status", "passed"),
    ] {
        if required(&metadata, key, &metadata_path)? != value {
            return Err(format!(
                "{} has unsupported {key} value",
                metadata_path.display()
            ));
        }
    }
    let expected_requests_per_shard = checked_product(
        (users / shards) as u64,
        requests_per_user as u64,
        "per-shard requests",
    )?;
    let records_per_shard = expected_requests_per_shard
        .checked_add((users / shards) as u64 * 3)
        .ok_or_else(|| "per-shard final sequence overflow".to_owned())?;
    if required_usize(&metadata, "queue_capacity_total", &metadata_path)? != users
        || required_usize(&metadata, "batch_size", &metadata_path)? != 2_048
        || required_usize(&metadata, "projector_batch_size", &metadata_path)? != 256
        || required_u64(&metadata, "requests", &metadata_path)?
            != expected_requests_per_shard * shards as u64
    {
        return Err(format!(
            "{} fixed workload parameters do not match",
            metadata_path.display()
        ));
    }
    let database_count = expected_db_count;
    let expected_buffer_size = (128_u64 * 1024 * 1024) / (database_count as u64 * 2);
    let expected_cache_size = (128_u64 * 1024 * 1024) / database_count as u64;
    let expected_jobs_per_db = if layout == RocksLayout::Shared {
        8
    } else {
        (8 / shards) as u64
    };
    let mut db_names = BTreeSet::new();
    for index in 0..database_count {
        let name = required(&metadata, &format!("database_{index}_name"), &metadata_path)?;
        if name.is_empty() || !db_names.insert(name) {
            return Err(format!(
                "{} has a missing or duplicate database identity",
                metadata_path.display()
            ));
        }
        if required_u64(
            &metadata,
            &format!("database_{index}_write_buffer_size"),
            &metadata_path,
        )? != expected_buffer_size
            || required_u64(
                &metadata,
                &format!("database_{index}_max_write_buffer_number"),
                &metadata_path,
            )? != 2
            || required_u64(
                &metadata,
                &format!("database_{index}_block_cache_bytes"),
                &metadata_path,
            )? != expected_cache_size
            || required_u64(
                &metadata,
                &format!("database_{index}_max_background_jobs"),
                &metadata_path,
            )? != expected_jobs_per_db
        {
            return Err(format!(
                "{} database {index} options violate the aggregate budget",
                metadata_path.display()
            ));
        }
    }
    if required_u64(&metadata, "configured_write_buffer_bytes", &metadata_path)?
        != 128 * 1024 * 1024
        || required_u64(&metadata, "configured_block_cache_bytes", &metadata_path)?
            != 128 * 1024 * 1024
        || required_u64(&metadata, "configured_max_background_jobs", &metadata_path)? != 8
        || required_u64(&metadata, "final_sequence_sum", &metadata_path)?
            != records_per_shard * shards as u64
    {
        return Err(format!(
            "{} aggregate options or final sequence do not match the fixed configuration",
            metadata_path.display()
        ));
    }
    if required(
        &metadata,
        "global_observed_query_group_peak",
        &metadata_path,
    )? != "NA"
        || required(&metadata, "global_observed_query_job_peak", &metadata_path)? != "NA"
    {
        return Err(format!(
            "{} fabricates a global query peak",
            metadata_path.display()
        ));
    }

    let summary_path = trial_dir.join(SUMMARY_FILE);
    let summary = read_csv(&summary_path)?;
    require_header(&summary, &summary_header(), &summary_path)?;
    if summary.rows.len() != 1 {
        return Err(format!(
            "{} must contain exactly one global row",
            summary_path.display()
        ));
    }
    let summary_map = row_map(&summary.header, &summary.rows[0], &summary_path)?;
    let expected_requests = checked_product(users as u64, requests_per_user as u64, "requests")?;
    let stride = required_u64(&metadata, "request_sample_stride", &metadata_path)?;
    if stride == 0 {
        return Err(format!(
            "{} has a zero sample stride",
            metadata_path.display()
        ));
    }
    let expected_samples = expected_sample_ids(expected_requests, stride);
    if required_u64(&summary_map, "requests", &summary_path)? != expected_requests
        || required_u64(&summary_map, "credits", &summary_path)? != expected_requests / 2
        || required_u64(&summary_map, "debits", &summary_path)? != expected_requests / 2
        || required_u64(&summary_map, "seed_records", &summary_path)? != (users as u64) * 3
        || required_usize(&summary_map, "request_sample_count", &summary_path)?
            != expected_samples.len()
    {
        return Err(format!(
            "{} request totals or sample count do not match workload",
            summary_path.display()
        ));
    }
    if required_usize(&metadata, "expected_request_sample_count", &metadata_path)?
        != expected_samples.len()
    {
        return Err(format!(
            "{} has an invalid request sampling contract",
            metadata_path.display()
        ));
    }

    validate_request_samples(
        trial_dir,
        &metadata,
        &summary_map,
        &expected_samples,
        shards,
        requests_per_user,
    )?;
    validate_shards(
        trial_dir,
        &metadata,
        &summary_map,
        shards,
        users,
        requests_per_user,
    )?;
    validate_stage_artifacts(trial_dir, shards)?;
    validate_index_artifacts(trial_dir, expected_requests, shards)?;
    validate_background_artifacts(trial_dir, shards)?;
    validate_summary_metrics(&summary_map, trial_dir)?;
    Ok(())
}

fn validate_artifact_values(artifact: &TrialArtifacts) -> Result<(), String> {
    let requests = checked_product(
        artifact.users as u64,
        artifact.requests_per_user as u64,
        "requests",
    )?;
    if artifact.summary.requests != requests
        || artifact.summary.credits != requests / 2
        || artifact.summary.debits != requests / 2
        || artifact.summary.shards.len() != artifact.shards
        || artifact.shard_backlogs.len() != artifact.shards
        || artifact.final_boundary_by_shard.len() != artifact.shards
        || artifact.final_boundary_target_sequence.len() != artifact.shards
        || artifact.sample_stride == 0
        || artifact.request_samples.len() != artifact.expected_request_sample_count
        || artifact.client_wall.is_zero()
    {
        return Err(
            "trial artifact totals, shards, sampling, or client window are inconsistent".to_owned(),
        );
    }
    if artifact.index_batch_rows.is_empty() || artifact.index_group_rows.is_empty() {
        return Err("trial artifact has no index batch or group metrics".to_owned());
    }
    if artifact.stages.is_empty() {
        return Err("trial artifact has no raw stage metrics".to_owned());
    }
    let expected_ids = expected_sample_ids(requests, artifact.sample_stride);
    let mut actual_ids = artifact
        .request_samples
        .iter()
        .map(|sample| sample.logical_id)
        .collect::<Vec<_>>();
    actual_ids.sort_unstable();
    if actual_ids != expected_ids {
        return Err(
            "request sample IDs do not match the deterministic global stride plan".to_owned(),
        );
    }
    Ok(())
}

fn summary_header() -> Vec<String> {
    SUMMARY_PREFIX.split(',').map(str::to_owned).collect()
}
fn shard_header() -> Vec<String> {
    SHARDS_HEADER.split(',').map(str::to_owned).collect()
}
fn stage_header() -> Vec<String> {
    STAGES_HEADER.split(',').map(str::to_owned).collect()
}
fn raw_stage_header() -> Vec<String> {
    RAW_STAGES_HEADER.split(',').map(str::to_owned).collect()
}
fn request_samples_header() -> Vec<String> {
    REQUEST_SAMPLES_HEADER
        .split(',')
        .map(str::to_owned)
        .collect()
}
fn background_header() -> Vec<String> {
    BACKGROUND_HEADER.split(',').map(str::to_owned).collect()
}

fn summary_row(artifact: &TrialArtifacts) -> Result<Vec<String>, String> {
    let requests = artifact.summary.requests;
    let client_wall_s = artifact.client_wall.as_secs_f64();
    if !client_wall_s.is_finite() || client_wall_s <= 0.0 || requests == 0 {
        return Err("summary requires positive finite request count and client wall".to_owned());
    }
    let finite_run_s = client_wall_s + artifact.settled_wall.as_secs_f64();
    if !finite_run_s.is_finite() || finite_run_s <= 0.0 {
        return Err("finite-run denominator is not positive and finite".to_owned());
    }
    let total_write_buffers = artifact.databases.iter().fold(0_u64, |sum, (_, budget)| {
        sum.saturating_add(
            (budget.write_buffer_size as u64)
                .saturating_mul(budget.max_write_buffer_number.max(0) as u64),
        )
    });
    let total_block_cache = artifact.databases.iter().fold(0_u64, |sum, (_, budget)| {
        sum.saturating_add(budget.block_cache_bytes as u64)
    });
    let total_background_jobs = artifact.databases.iter().fold(0_i64, |sum, (_, budget)| {
        sum.saturating_add(budget.max_background_jobs as i64)
    });
    let mut backlog_projection = 0_u64;
    let mut backlog_gc = 0_u64;
    for (_, latest, projected, _, gc_prefix) in &artifact.shard_backlogs {
        backlog_projection = backlog_projection.saturating_add(latest.saturating_sub(*projected));
        backlog_gc = backlog_gc.saturating_add(latest.saturating_sub(*gc_prefix));
    }
    let mut row = vec![
        artifact.trial_name.clone(),
        case_name(
            artifact.shards,
            artifact.layout.as_str(),
            artifact.concurrency,
        )?,
        artifact.shards.to_string(),
        artifact.layout.as_str().to_owned(),
        "256".to_owned(),
        artifact.concurrency.to_string(),
        artifact.users.to_string(),
        artifact.requests_per_user.to_string(),
        requests.to_string(),
        artifact.summary.credits.to_string(),
        artifact.summary.debits.to_string(),
        artifact
            .summary
            .shards
            .iter()
            .map(|shard| shard.account_count as u64 * 3)
            .sum::<u64>()
            .to_string(),
        artifact.summary.final_sequence_sum.to_string(),
        float(client_wall_s),
        float(requests as f64 / client_wall_s),
        float(artifact.cpu_seconds),
        float(artifact.cpu_core_equivalents),
        float(artifact.cpu_seconds * 1_000_000.0 / requests as f64),
        float(artifact.settled_wall.as_secs_f64()),
        float(requests as f64 / finite_run_s),
        float(artifact.recovery_seconds),
        float(artifact.integrity_seconds),
        artifact.peak_rss_bytes.to_string(),
        artifact.db_bytes.to_string(),
        artifact.databases.len().to_string(),
        total_write_buffers.to_string(),
        total_block_cache.to_string(),
        total_background_jobs.to_string(),
        artifact.rocks.0.to_string(),
        artifact.rocks.1.to_string(),
        artifact.rocks.2.to_string(),
        artifact.rocks.3.to_string(),
        artifact.rocks.4.to_string(),
        artifact.rocks.5.to_string(),
        artifact.rocks.6.to_string(),
        artifact.io.process_rchar_bytes.to_string(),
        artifact.io.process_wchar_bytes.to_string(),
        artifact.io.process_read_bytes.to_string(),
        artifact.io.process_write_bytes.to_string(),
        artifact.io.target_device.clone(),
        artifact.io.target_major_minor.clone(),
        artifact.io.target_read_bytes.to_string(),
        artifact.io.target_write_bytes.to_string(),
        artifact.io.target_busy_ms.to_string(),
        artifact.cpu_sample_offset_us.to_string(),
        artifact.progress_sample_offset_us.to_string(),
        artifact.io_sample_offset_us.to_string(),
        artifact.storage_sample_offset_us.to_string(),
        artifact.sample_stride.to_string(),
        artifact.request_samples.len().to_string(),
        backlog_projection.to_string(),
        backlog_gc.to_string(),
        artifact
            .shards
            .saturating_mul(artifact.concurrency)
            .to_string(),
        "NA".to_owned(),
        "NA".to_owned(),
        artifact.max_in_flight_per_batch.to_string(),
        artifact.max_running_query_jobs_per_batch.to_string(),
        float(artifact.setup_preflight.cpu_busy_pct),
        float(artifact.setup_preflight.disk_busy_pct),
        float(artifact.measure_preflight.cpu_busy_pct),
        float(artifact.measure_preflight.disk_busy_pct),
    ];
    let distributions = request_distributions(&artifact.request_samples);
    for (name, _) in REQUEST_METRICS {
        let distribution = distributions
            .get(name)
            .ok_or_else(|| format!("missing request metric {name}"))?;
        row.push(distribution.count.to_string());
        row.push(distribution.p50.to_string());
        row.push(distribution.p95.to_string());
        row.push(distribution.p99.to_string());
    }
    if row.len() != summary_header().len() {
        return Err(format!(
            "summary row has {} fields, expected {}",
            row.len(),
            summary_header().len()
        ));
    }
    Ok(row)
}

fn shard_rows(artifact: &TrialArtifacts) -> Result<Vec<Vec<String>>, String> {
    let peaks = batch_peaks(&artifact.index_batch_rows, artifact.shards)?;
    let backlogs = artifact
        .shard_backlogs
        .iter()
        .map(|(id, latest, projected, destination, gc)| {
            (*id, (*latest, *projected, *destination, *gc))
        })
        .collect::<BTreeMap<_, _>>();
    let mut rows = Vec::with_capacity(artifact.shards);
    let mut summaries = artifact.summary.shards.iter().collect::<Vec<_>>();
    summaries.sort_by_key(|shard| shard.shard_id);
    for shard in summaries {
        let (latest, projected, destination, gc_prefix) = backlogs
            .get(&shard.shard_id)
            .copied()
            .ok_or_else(|| format!("missing client-end backlog for shard {}", shard.shard_id))?;
        let artifact_boundary = *artifact
            .final_boundary_by_shard
            .get(shard.shard_id)
            .ok_or_else(|| format!("missing final boundary for shard {}", shard.shard_id))?;
        let boundary_target = shard.final_boundary_target_sequence;
        if artifact_boundary != shard.final_boundary
            || artifact.final_boundary_target_sequence.get(shard.shard_id) != Some(&boundary_target)
        {
            return Err(format!(
                "shard {} boundary differs between summary and artifact",
                shard.shard_id
            ));
        }
        let (max_in_flight, max_running) = peaks
            .get(&shard.shard_id)
            .copied()
            .ok_or_else(|| format!("missing observed query peaks for shard {}", shard.shard_id))?;
        rows.push(vec![
            artifact.trial_name.clone(),
            case_name(
                artifact.shards,
                artifact.layout.as_str(),
                artifact.concurrency,
            )?,
            artifact.shards.to_string(),
            shard.shard_id.to_string(),
            shard.account_count.to_string(),
            shard.requests.to_string(),
            shard.credits.to_string(),
            shard.debits.to_string(),
            latest.to_string(),
            projected.to_string(),
            destination.to_string(),
            gc_prefix.to_string(),
            latest.saturating_sub(projected).to_string(),
            latest.saturating_sub(destination).to_string(),
            latest.saturating_sub(gc_prefix).to_string(),
            shard.final_sequence.to_string(),
            shard.projected_sequence.to_string(),
            shard.destination_sequence.to_string(),
            shard.gc_prefix.to_string(),
            shard.final_boundary.to_string(),
            boundary_target.to_string(),
            float(artifact.client_wall.as_secs_f64()),
            float(shard.requests as f64 / artifact.client_wall.as_secs_f64()),
            max_in_flight.to_string(),
            max_running.to_string(),
        ]);
    }
    if rows.len() != artifact.shards || rows.iter().any(|row| row.len() != shard_header().len()) {
        return Err("per-shard summary rows have invalid dimensions".to_owned());
    }
    Ok(rows)
}

fn stage_rows(artifact: &TrialArtifacts) -> Result<Vec<Vec<String>>, String> {
    let mut values = BTreeMap::<(String, Option<usize>), Vec<u64>>::new();
    for stage in effective_stage_samples(artifact)? {
        if stage.shard_id >= artifact.shards || !STAGE_METRICS.contains(&stage.metric.as_str()) {
            return Err("raw stage has an invalid shard ID or empty metric identity".to_owned());
        }
        values
            .entry((stage.metric.clone(), None))
            .or_default()
            .push(stage.value_ns);
        values
            .entry((stage.metric.clone(), Some(stage.shard_id)))
            .or_default()
            .push(stage.value_ns);
    }
    let mut rows = Vec::new();
    for metric in STAGE_METRICS {
        let metric = metric.to_owned();
        for shard_id in std::iter::once(None).chain((0..artifact.shards).map(Some)) {
            let scope = if shard_id.is_some() {
                "shard"
            } else {
                "global"
            };
            let dist = values
                .get(&(metric.clone(), shard_id))
                .and_then(|samples| distribution(samples));
            let (count, mean, p50, p95, p99) = match dist {
                Some(stats) => (
                    stats.count.to_string(),
                    float(stats.mean_ns),
                    stats.p50.to_string(),
                    stats.p95.to_string(),
                    stats.p99.to_string(),
                ),
                None => (
                    "0".to_owned(),
                    "NA".to_owned(),
                    "NA".to_owned(),
                    "NA".to_owned(),
                    "NA".to_owned(),
                ),
            };
            rows.push(vec![
                artifact.trial_name.clone(),
                case_name(
                    artifact.shards,
                    artifact.layout.as_str(),
                    artifact.concurrency,
                )?,
                scope.to_owned(),
                option_usize(shard_id),
                metric.clone(),
                count,
                mean,
                p50,
                p95,
                p99,
            ]);
        }
    }
    Ok(rows)
}

fn effective_stage_samples(artifact: &TrialArtifacts) -> Result<Vec<RawStage>, String> {
    let mut samples = artifact.stages.clone();
    for request in &artifact.request_samples {
        for (metric, field) in REQUEST_METRICS {
            let value_ns = match field {
                "total_ns" => request.total_ns,
                "admission_ns" => request.admission_ns,
                "enqueue_ns" => request.enqueue_ns,
                "queue_ns" => request.queue_ns,
                "batch_ns" => request.batch_ns,
                "handler_ns" => request.handler_ns,
                "response_ns" => request.response_ns,
                _ => return Err(format!("unknown request stage field {field}")),
            };
            samples.push(RawStage {
                metric: metric.to_owned(),
                shard_id: request.shard_id,
                logical_id: Some(request.logical_id),
                value_ns,
                completed_at: None,
            });
        }
    }
    Ok(samples)
}

fn metadata_text(artifact: &TrialArtifacts) -> Result<String, String> {
    let case = case_name(
        artifact.shards,
        artifact.layout.as_str(),
        artifact.concurrency,
    )?;
    let requests = artifact.summary.requests;
    let seed_records = artifact
        .summary
        .shards
        .iter()
        .map(|shard| shard.account_count as u64 * 3)
        .sum::<u64>();
    let formal = artifact.users == 50_000 && artifact.requests_per_user == 200;
    let reserve_memory = if formal {
        DEFAULT_MEMORY_RESERVE_BYTES
    } else {
        128 * 1024 * 1024
    };
    let reserve_disk = if formal {
        DEFAULT_FREE_SPACE_RESERVE_BYTES
    } else {
        256 * 1024 * 1024
    };
    let total_records = (artifact.users as u64)
        .checked_mul(artifact.requests_per_user as u64 + 3)
        .ok_or_else(|| "metadata record count overflow".to_owned())?;
    let required_mem = total_records
        .saturating_mul(ESTIMATED_DESTINATION_BYTES_PER_RECORD)
        .saturating_add(reserve_memory);
    let required_disk = total_records
        .saturating_mul(ESTIMATED_DISK_BYTES_PER_RECORD)
        .saturating_add(reserve_disk);
    let mut text = format!(
        "schema_version=1\ntrial_name={}\ncase={}\nshards={}\nlayout={}\nindex_strategy=chunked\nindex_group_size=256\nindex_concurrency={}\nusers={}\nrequests_per_user={}\nrequests={}\ncredits={}\ndebits={}\nseed_transactions_per_account=3\nseed_records={}\nfinal_sequence_sum={}\nbalance_mode=PerBatch\nruntime_worker_threads=4\nqueue_capacity_total=50000\nqueue_capacity_per_shard_formula=users_count_per_shard\nbatch_size=2048\nfirst_dequeue_timeout_ms=5\nprojector_batch_size=256\nwatermark_interval_ms=100\nretention_ms=500\ngc_batch_size=256\ngc_idle_interval_ms=100\nrequest_sample_stride={}\nexpected_request_sample_count={}\nclient_wall_seconds={:.9}\nclient_end_elapsed_ns={}\nclient_rps={:.9}\ncpu_seconds={:.9}\ncpu_core_equivalents={:.9}\ncpu_microseconds_per_request={:.9}\nsettlement_seconds={:.9}\nfinite_run_drain_rps={:.9}\nrecovery_seconds={:.9}\nintegrity_seconds={:.9}\nrss_peak_bytes={}\ndatabase_count={}\ndb_bytes={}\ndb_bytes_scope=all_unique_database_directory_files_after_recovery_validation_and_close_before_scratch_cleanup\nrocks_stats_scope=unique_database_instances_counted_once\nrocksdb_env_scope=shared_default_env_for_child_process\nrocksdb_env_low_priority_threads=6\nrocksdb_env_high_priority_threads=2\nrocksdb_environment_pools_are_configured_limits_not_hard_active_thread_or_cpu_caps=true\nshared_database_counted_once={}\nconfigured_write_buffer_bytes={}\nconfigured_block_cache_bytes={}\nconfigured_max_background_jobs={}\nrocks_wal_syncs={}\nrocks_wal_bytes={}\nrocks_writes_with_wal={}\nrocks_flush_write_bytes={}\nrocks_compaction_read_bytes={}\nrocks_compaction_write_bytes={}\nrocks_stall_us={}\nprocess_io_scope=whole_child_process_sampled_once\ndevice_io_scope=whole_target_device_sampled_once\nprocess_rchar_bytes={}\nprocess_wchar_bytes={}\nprocess_read_bytes={}\nprocess_write_bytes={}\ntarget_device={}\ntarget_major_minor={}\ntarget_read_bytes={}\ntarget_write_bytes={}\ntarget_busy_ms={}\ncpu_sample_offset_us={}\nprogress_sample_offset_us={}\nio_sample_offset_us={}\nstorage_sample_offset_us={}\nsetup_preflight_observation_ms={}\nsetup_preflight_attempts={}\nsetup_preflight_cpu_busy_pct={:.3}\nsetup_preflight_device_busy_pct={:.3}\nsetup_preflight_available_memory_bytes={}\nsetup_preflight_free_bytes={}\nsetup_preflight_required_memory_bytes={}\nsetup_preflight_required_disk_bytes={}\nsetup_preflight_timeout_ms=60000\nsetup_preflight_cpu_limit_pct=10\nsetup_preflight_device_limit_pct=5\nmeasure_preflight_observation_ms={}\nmeasure_preflight_attempts={}\nmeasure_preflight_cpu_busy_pct={:.3}\nmeasure_preflight_device_busy_pct={:.3}\nmeasure_preflight_available_memory_bytes={}\nmeasure_preflight_free_bytes={}\nmeasure_preflight_required_memory_bytes={}\nmeasure_preflight_required_disk_bytes={}\nmeasure_preflight_timeout_ms=60000\nmeasure_preflight_cpu_limit_pct=10\nmeasure_preflight_device_limit_pct=5\npreflight_target_path={}\npreflight_filesystem={}\npreflight_device={}\npreflight_major_minor={}\nfinal_request_count={}\nfinal_credit_count={}\nfinal_debit_count={}\nfinal_sequence_verified=true\nfinal_balance_per_account=100\nprojection_durable_equals_destination_equals_latest=true\nfinal_boundary_timestamps_per_shard_us={}\nfinal_boundary_target_sequences_per_shard={}\ncleanup_status=owned_scratch_removed\nintegrity_status=passed\nmock_destination_durability=successful_apply_in_memory_contract_only\nrequest_sample_timestamps=not_captured\nraw_stage_timestamp_semantics=completion_time_exported_only_when_available; internal_capture_instant_is_not_request_completion\nbackground_elapsed_origin=global_wall_start; events_after_client_end_excluded_from_window_csv\nwall_scope=global_start_barrier_to_last_reply\ncpu_scope=whole_process_cpu_delta_over_client_window_all_runtime_native_and_background_threads\npeak_rss_scope=process_lifetime_peak_including_startup_and_seed\nglobal_theoretical_query_group_cap={}\nglobal_observed_query_group_peak=NA\nglobal_observed_query_job_peak=NA\nper_batch_peak_max_in_flight_groups={}\nper_batch_peak_running_query_jobs={}\n",
        metadata_escape(&artifact.trial_name),
        case,
        artifact.shards,
        artifact.layout.as_str(),
        artifact.concurrency,
        artifact.users,
        artifact.requests_per_user,
        requests,
        artifact.summary.credits,
        artifact.summary.debits,
        seed_records,
        artifact.summary.final_sequence_sum,
        artifact.sample_stride,
        artifact.expected_request_sample_count,
        artifact.client_wall.as_secs_f64(),
        artifact.client_end_elapsed_ns,
        requests as f64 / artifact.client_wall.as_secs_f64(),
        artifact.cpu_seconds,
        artifact.cpu_core_equivalents,
        artifact.cpu_seconds * 1_000_000.0 / requests as f64,
        artifact.settled_wall.as_secs_f64(),
        requests as f64
            / (artifact.client_wall.as_secs_f64() + artifact.settled_wall.as_secs_f64()),
        artifact.recovery_seconds,
        artifact.integrity_seconds,
        artifact.peak_rss_bytes,
        artifact.databases.len(),
        artifact.db_bytes,
        artifact.layout == RocksLayout::Shared,
        artifact
            .databases
            .iter()
            .map(|(_, b)| (b.write_buffer_size as u64)
                .saturating_mul(b.max_write_buffer_number.max(0) as u64))
            .sum::<u64>(),
        artifact
            .databases
            .iter()
            .map(|(_, b)| b.block_cache_bytes as u64)
            .sum::<u64>(),
        artifact
            .databases
            .iter()
            .map(|(_, b)| b.max_background_jobs as i64)
            .sum::<i64>(),
        artifact.rocks.0,
        artifact.rocks.1,
        artifact.rocks.2,
        artifact.rocks.3,
        artifact.rocks.4,
        artifact.rocks.5,
        artifact.rocks.6,
        artifact.io.process_rchar_bytes,
        artifact.io.process_wchar_bytes,
        artifact.io.process_read_bytes,
        artifact.io.process_write_bytes,
        metadata_escape(&artifact.io.target_device),
        artifact.io.target_major_minor,
        artifact.io.target_read_bytes,
        artifact.io.target_write_bytes,
        artifact.io.target_busy_ms,
        artifact.cpu_sample_offset_us,
        artifact.progress_sample_offset_us,
        artifact.io_sample_offset_us,
        artifact.storage_sample_offset_us,
        artifact.setup_preflight.observation.as_millis(),
        artifact.setup_preflight.attempts,
        artifact.setup_preflight.cpu_busy_pct,
        artifact.setup_preflight.disk_busy_pct,
        artifact.setup_preflight.mem_available_bytes,
        artifact.setup_preflight.free_bytes,
        required_mem,
        required_disk,
        artifact.measure_preflight.observation.as_millis(),
        artifact.measure_preflight.attempts,
        artifact.measure_preflight.cpu_busy_pct,
        artifact.measure_preflight.disk_busy_pct,
        artifact.measure_preflight.mem_available_bytes,
        artifact.measure_preflight.free_bytes,
        required_mem,
        required_disk,
        metadata_escape(&artifact.setup_preflight.path.display().to_string()),
        metadata_escape(&artifact.setup_preflight.filesystem),
        metadata_escape(&artifact.setup_preflight.device),
        artifact.setup_preflight.major_minor,
        requests,
        artifact.summary.credits,
        artifact.summary.debits,
        join_u64(
            &artifact
                .summary
                .shards
                .iter()
                .map(|shard| shard.final_boundary)
                .collect::<Vec<_>>()
        ),
        join_u64(
            &artifact
                .summary
                .shards
                .iter()
                .map(|shard| shard.final_boundary_target_sequence)
                .collect::<Vec<_>>()
        ),
        artifact.shards * artifact.concurrency,
        artifact.max_in_flight_per_batch,
        artifact.max_running_query_jobs_per_batch,
    );
    text = text.replace(
        "queue_capacity_total=50000",
        &format!("queue_capacity_total={}", artifact.users),
    );
    for (index, (name, budget)) in artifact.databases.iter().enumerate() {
        text.push_str(&format!(
            "database_{index}_name={}\ndatabase_{index}_write_buffer_size={}\ndatabase_{index}_max_write_buffer_number={}\ndatabase_{index}_block_cache_bytes={}\ndatabase_{index}_max_background_jobs={}\n",
            metadata_escape(name), budget.write_buffer_size, budget.max_write_buffer_number,
            budget.block_cache_bytes, budget.max_background_jobs,
        ));
    }
    for shard in &artifact.summary.shards {
        text.push_str(&format!(
            "shard_{}_account_count={}\nshard_{}_requests={}\nshard_{}_credits={}\nshard_{}_debits={}\nshard_{}_final_sequence={}\nshard_{}_final_projected_sequence={}\nshard_{}_final_destination_sequence={}\nshard_{}_final_gc_prefix={}\nshard_{}_final_boundary_timestamp_us={}\nshard_{}_final_boundary_target_sequence={}\n",
            shard.shard_id, shard.account_count,
            shard.shard_id, shard.requests,
            shard.shard_id, shard.credits,
            shard.shard_id, shard.debits,
            shard.shard_id, shard.final_sequence,
            shard.shard_id, shard.projected_sequence,
            shard.shard_id, shard.destination_sequence,
            shard.shard_id, shard.gc_prefix,
            shard.shard_id, shard.final_boundary,
            shard.shard_id, shard.final_boundary_target_sequence,
        ));
    }
    for (prefix, report) in [
        ("setup_preflight", &artifact.setup_preflight),
        ("measure_preflight", &artifact.measure_preflight),
    ] {
        text.push_str(&format!(
            "{prefix}_mount_point={}\n{prefix}_major_minor={}\n",
            metadata_escape(&report.mount_point.display().to_string()),
            report.major_minor,
        ));
    }
    Ok(text)
}

fn create_raw_csv(
    dir: &Path,
    name: &str,
    header: &[String],
    rows: &[Vec<String>],
) -> Result<(), String> {
    let mut contents = String::new();
    contents.push_str(&encode_csv_record(header));
    for row in rows {
        if row.len() != header.len() {
            return Err(format!(
                "raw {name} row has {} fields, expected {}",
                row.len(),
                header.len()
            ));
        }
        contents.push_str(&encode_csv_record(row));
        if contents.len() > 100 * 1024 * 1024 {
            return Err(format!(
                "raw file {name} exceeds the 100 MiB per-file limit"
            ));
        }
    }
    create_raw_text(dir, name, &contents)
}

fn create_index_file(
    dir: &Path,
    name: &str,
    expected_header: &str,
    rows: &[String],
) -> Result<(), String> {
    let path = dir.join(name);
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .map_err(|error| {
            format!(
                "cannot create immutable raw file {}: {error}",
                path.display()
            )
        })?;
    writeln!(output, "{expected_header}")
        .map_err(|error| format!("cannot write {}: {error}", path.display()))?;
    let mut bytes_written = expected_header.len() as u64 + 1;
    let width = parse_csv_record(expected_header)?.len();
    for (index, row) in rows.iter().enumerate() {
        let fields = parse_csv_record(row)
            .map_err(|error| format!("invalid {name} row {}: {error}", index + 1))?;
        if fields.len() != width {
            return Err(format!(
                "{name} row {} has {} fields, expected {width}",
                index + 1,
                fields.len()
            ));
        }
        writeln!(output, "{row}")
            .map_err(|error| format!("cannot write {}: {error}", path.display()))?;
        bytes_written = bytes_written.saturating_add(row.len() as u64 + 1);
        if bytes_written > 100 * 1024 * 1024 {
            return Err(format!(
                "raw file {name} exceeds the 100 MiB per-file limit"
            ));
        }
    }
    output
        .flush()
        .and_then(|_| output.sync_all())
        .map_err(|error| format!("cannot persist {}: {error}", path.display()))
}

fn create_raw_text(dir: &Path, name: &str, contents: &str) -> Result<(), String> {
    if contents.len() > 100 * 1024 * 1024 {
        return Err(format!(
            "raw file {name} exceeds the 100 MiB per-file limit"
        ));
    }
    let path = dir.join(name);
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .map_err(|error| {
            format!(
                "cannot create immutable raw file {}: {error}",
                path.display()
            )
        })?;
    output
        .write_all(contents.as_bytes())
        .and_then(|_| output.flush())
        .and_then(|_| output.sync_all())
        .map_err(|error| format!("cannot persist {}: {error}", path.display()))
}

fn validate_request_samples(
    trial_dir: &Path,
    metadata: &BTreeMap<String, String>,
    summary: &BTreeMap<String, String>,
    expected_ids: &[u64],
    shards: usize,
    requests_per_user: usize,
) -> Result<(), String> {
    let path = trial_dir.join(REQUEST_SAMPLES_FILE);
    let data = read_csv(&path)?;
    require_header(&data, &request_samples_header(), &path)?;
    if data.rows.len() != expected_ids.len() {
        return Err(format!(
            "{} has {} request samples, expected {}",
            path.display(),
            data.rows.len(),
            expected_ids.len()
        ));
    }
    let expected = expected_ids.iter().copied().collect::<BTreeSet<_>>();
    let mut seen = BTreeSet::new();
    let mut by_metric = REQUEST_METRICS
        .iter()
        .map(|(metric, _)| ((*metric).to_owned(), Vec::<u64>::new()))
        .collect::<BTreeMap<_, _>>();
    let total_requests = required_u64(summary, "requests", &trial_dir.join(SUMMARY_FILE))?;
    let users = required_usize(metadata, "users", &trial_dir.join(METADATA_FILE))?;
    for row in &data.rows {
        let values = row_map(&data.header, row, &path)?;
        let logical_id = required_u64(&values, "logical_id", &path)?;
        let shard_id = required_usize(&values, "shard_id", &path)?;
        if !expected.contains(&logical_id)
            || !seen.insert(logical_id)
            || logical_id >= total_requests
        {
            return Err(format!(
                "{} has an unexpected or duplicate logical request ID {logical_id}",
                path.display()
            ));
        }
        let account_id = logical_id / requests_per_user as u64;
        if account_id >= users as u64 || account_id as usize % shards != shard_id {
            return Err(format!(
                "{} routes logical request {logical_id} to the wrong shard",
                path.display()
            ));
        }
        for (metric, field) in REQUEST_METRICS {
            by_metric
                .get_mut(metric)
                .expect("request metric map initialized")
                .push(required_u64(&values, field, &path)?);
        }
    }
    if seen != expected {
        return Err(format!(
            "{} does not contain the deterministic global request sample IDs",
            path.display()
        ));
    }
    for (metric, _) in REQUEST_METRICS {
        let stats = distribution(
            by_metric
                .get(metric)
                .expect("request metric map initialized"),
        )
        .ok_or_else(|| format!("{} has no samples for {metric}", path.display()))?;
        let prefix = metric.replace('.', "_");
        for (suffix, actual) in [
            ("sample_count", stats.count as u64),
            ("p50_ns", stats.p50),
            ("p95_ns", stats.p95),
            ("p99_ns", stats.p99),
        ] {
            let field = format!("{prefix}_{suffix}");
            let declared = required_u64(summary, &field, &trial_dir.join(SUMMARY_FILE))?;
            if declared != actual {
                return Err(format!(
                    "{} {field}={declared}, request samples produce {actual}",
                    trial_dir.display()
                ));
            }
        }
    }
    if required_u64(
        metadata,
        "expected_request_sample_count",
        &trial_dir.join(METADATA_FILE),
    )? != seen.len() as u64
    {
        return Err(format!(
            "{} request sample metadata count differs from raw IDs",
            metadata_path_display(trial_dir)
        ));
    }
    Ok(())
}

fn validate_shards(
    trial_dir: &Path,
    metadata: &BTreeMap<String, String>,
    summary: &BTreeMap<String, String>,
    shards: usize,
    users: usize,
    requests_per_user: usize,
) -> Result<(), String> {
    let path = trial_dir.join(SHARDS_FILE);
    let data = read_csv(&path)?;
    require_header(&data, &shard_header(), &path)?;
    if data.rows.len() != shards {
        return Err(format!(
            "{} has {} shard rows, expected {shards}",
            path.display(),
            data.rows.len()
        ));
    }
    let mut seen = BTreeSet::new();
    let mut sequence_sum = 0_u64;
    let users_per_shard = users / shards;
    let expected_requests = checked_product(
        users_per_shard as u64,
        requests_per_user as u64,
        "per-shard requests",
    )?;
    for row in &data.rows {
        let values = row_map(&data.header, row, &path)?;
        let shard_id = required_usize(&values, "shard_id", &path)?;
        if shard_id >= shards || !seen.insert(shard_id) {
            return Err(format!(
                "{} has an invalid or duplicate shard ID {shard_id}",
                path.display()
            ));
        }
        let accounts = required_usize(&values, "account_count", &path)?;
        let requests = required_u64(&values, "requests", &path)?;
        let credits = required_u64(&values, "credits", &path)?;
        let debits = required_u64(&values, "debits", &path)?;
        let expected_local_sequence = checked_product(
            accounts as u64,
            requests_per_user as u64 + 3,
            "local sequence",
        )?;
        let latest_client = required_u64(&values, "client_end_latest_seq", &path)?;
        let projected_client = required_u64(&values, "client_end_projected_seq", &path)?;
        let destination_client = required_u64(&values, "client_end_destination_seq", &path)?;
        let gc_client = required_u64(&values, "client_end_gc_prefix", &path)?;
        let final_latest = required_u64(&values, "final_sequence", &path)?;
        let final_projected = required_u64(&values, "final_projected_sequence", &path)?;
        let final_destination = required_u64(&values, "final_destination_sequence", &path)?;
        let final_gc = required_u64(&values, "final_gc_prefix", &path)?;
        let boundary_time = required_u64(&values, "final_boundary_timestamp_us", &path)?;
        let boundary_target = required_u64(&values, "final_boundary_target_sequence", &path)?;
        if accounts != users_per_shard
            || requests != expected_requests
            || credits != requests / 2
            || debits != requests / 2
            || final_latest != expected_local_sequence
            || final_projected != final_latest
            || final_destination != final_latest
            || boundary_target > final_projected
            || final_gc > boundary_target
            || final_gc > final_projected
            || projected_client < (accounts as u64 * 3)
            || projected_client > destination_client
            || destination_client > latest_client
            || gc_client > projected_client
        {
            return Err(format!(
                "{} shard {shard_id} violates load, sequence, projection, or safe-GC invariants",
                path.display()
            ));
        }
        let expected_boundary_time = required_u64(
            metadata,
            &format!("shard_{shard_id}_final_boundary_timestamp_us"),
            &trial_dir.join(METADATA_FILE),
        )?;
        let expected_boundary_target = required_u64(
            metadata,
            &format!("shard_{shard_id}_final_boundary_target_sequence"),
            &trial_dir.join(METADATA_FILE),
        )?;
        if boundary_time != expected_boundary_time || boundary_target != expected_boundary_target {
            return Err(format!(
                "{} shard {shard_id} boundary differs from trial metadata",
                path.display()
            ));
        }
        sequence_sum = sequence_sum
            .checked_add(final_latest)
            .ok_or_else(|| "aggregate sequence overflow".to_owned())?;
    }
    if seen.len() != shards
        || sequence_sum
            != required_u64(summary, "final_sequence_sum", &trial_dir.join(SUMMARY_FILE))?
        || required_u64(summary, "requests", &trial_dir.join(SUMMARY_FILE))?
            != expected_requests * shards as u64
    {
        return Err(format!(
            "{} aggregate shard totals differ from global summary",
            path.display()
        ));
    }
    Ok(())
}

fn validate_stage_artifacts(trial_dir: &Path, shards: usize) -> Result<(), String> {
    let raw_path = trial_dir.join(RAW_STAGES_FILE);
    let raw = read_csv(&raw_path)?;
    require_header(&raw, &raw_stage_header(), &raw_path)?;
    let request_path = trial_dir.join(REQUEST_SAMPLES_FILE);
    let request = read_csv(&request_path)?;
    let mut request_values = BTreeMap::<(String, u64), u64>::new();
    for row in &request.rows {
        let values = row_map(&request.header, row, &request_path)?;
        let logical_id = required_u64(&values, "logical_id", &request_path)?;
        for (metric, field) in REQUEST_METRICS {
            request_values.insert(
                (metric.to_owned(), logical_id),
                required_u64(&values, field, &request_path)?,
            );
        }
    }
    let mut observed = BTreeMap::<(String, Option<usize>), Vec<u64>>::new();
    let mut observed_requests = BTreeMap::<(String, u64), u64>::new();
    for row in &raw.rows {
        let values = row_map(&raw.header, row, &raw_path)?;
        let metric = required(&values, "metric", &raw_path)?.to_owned();
        let shard_id = required_usize(&values, "shard_id", &raw_path)?;
        if shard_id >= shards || !STAGE_METRICS.contains(&metric.as_str()) {
            return Err(format!("{} has invalid stage identity", raw_path.display()));
        }
        let logical_id = optional_u64(&values, "logical_id", &raw_path)?;
        let value = required_u64(&values, "value_ns", &raw_path)?;
        observed
            .entry((metric.clone(), None))
            .or_default()
            .push(value);
        observed
            .entry((metric.clone(), Some(shard_id)))
            .or_default()
            .push(value);
        if REQUEST_METRICS.iter().any(|(name, _)| *name == metric) {
            let id = logical_id.ok_or_else(|| {
                format!("{} {metric} sample lacks logical ID", raw_path.display())
            })?;
            if observed_requests
                .insert((metric.clone(), id), value)
                .is_some()
            {
                return Err(format!(
                    "{} has duplicate {metric} sample ID {id}",
                    raw_path.display()
                ));
            }
            if request_values.get(&(metric.clone(), id)) != Some(&value) {
                return Err(format!(
                    "{} {metric} sample {id} differs from request sample vector",
                    raw_path.display()
                ));
            }
        }
    }
    for (metric, _) in REQUEST_METRICS {
        let count = observed_requests
            .keys()
            .filter(|(name, _)| name == metric)
            .count();
        let request_count = request.rows.len();
        if count != request_count {
            return Err(format!(
                "{} has {count} {metric} samples, expected {request_count}",
                raw_path.display()
            ));
        }
    }
    validate_stage_source_values(trial_dir, shards, &observed)?;
    let path = trial_dir.join(STAGES_FILE);
    let stages = read_csv(&path)?;
    require_header(&stages, &stage_header(), &path)?;
    let expected_rows = STAGE_METRICS.len() * (shards + 1);
    if stages.rows.len() != expected_rows {
        return Err(format!(
            "{} has {} rows, expected {expected_rows}",
            path.display(),
            stages.rows.len()
        ));
    }
    let mut stage_keys = BTreeSet::new();
    for row in &stages.rows {
        let values = row_map(&stages.header, row, &path)?;
        let metric = required(&values, "metric", &path)?.to_owned();
        let scope = required(&values, "scope", &path)?;
        let shard_id = optional_usize(&values, "shard_id", &path)?;
        let key = match (scope, shard_id) {
            ("global", None) => (metric.clone(), None),
            ("shard", Some(id)) if id < shards => (metric.clone(), Some(id)),
            _ => return Err(format!("{} has an invalid stage scope", path.display())),
        };
        if !STAGE_METRICS.contains(&metric.as_str()) || !stage_keys.insert(key.clone()) {
            return Err(format!(
                "{} has unknown or duplicate stage identity",
                path.display()
            ));
        }
        let expected = observed.get(&key).and_then(|samples| distribution(samples));
        match expected {
            Some(stats) => {
                if required_usize(&values, "sample_count", &path)? != stats.count
                    || required_u64(&values, "p50_ns", &path)? != stats.p50
                    || required_u64(&values, "p95_ns", &path)? != stats.p95
                    || required_u64(&values, "p99_ns", &path)? != stats.p99
                    || !close_enough(required_f64(&values, "mean_ns", &path)?, stats.mean_ns)
                {
                    return Err(format!(
                        "{} stage summary for {metric} does not match raw samples",
                        path.display()
                    ));
                }
            }
            None => {
                if required_usize(&values, "sample_count", &path)? != 0
                    || required(&values, "mean_ns", &path)? != "NA"
                    || required(&values, "p50_ns", &path)? != "NA"
                    || required(&values, "p95_ns", &path)? != "NA"
                    || required(&values, "p99_ns", &path)? != "NA"
                {
                    return Err(format!(
                        "{} unsupported {metric} shard scope must use NA",
                        path.display()
                    ));
                }
            }
        }
    }
    if stage_keys.len() != expected_rows {
        return Err(format!(
            "{} omits one or more stage identities",
            path.display()
        ));
    }
    Ok(())
}

fn validate_index_artifacts(
    trial_dir: &Path,
    expected_requests: u64,
    shards: usize,
) -> Result<(), String> {
    let batch_path = trial_dir.join(INDEX_BATCHES_FILE);
    let batches = read_csv(&batch_path)?;
    require_header(
        &batches,
        &parse_csv_record(INDEX_BATCH_HEADER)?,
        &batch_path,
    )?;
    if batches.rows.is_empty() {
        return Err(format!("{} has no batch metrics", batch_path.display()));
    }
    let group_path = trial_dir.join(INDEX_GROUPS_FILE);
    let groups = read_csv(&group_path)?;
    require_header(&groups, &parse_csv_record(INDEX_GROUP_HEADER)?, &group_path)?;
    if groups.rows.is_empty() {
        return Err(format!("{} has no group metrics", group_path.display()));
    }
    let mut seen_batches = BTreeSet::new();
    let mut batch_requests = 0_u64;
    let mut batch_keys = 0_u64;
    let mut batch_hits = 0_u64;
    let mut batch_misses = 0_u64;
    let mut groups_submitted = 0_u64;
    let mut local_peaks = BTreeMap::<usize, (usize, usize)>::new();
    let mut batch_count_by_id = BTreeMap::<(usize, u64), u64>::new();
    let mut per_shard_totals = BTreeMap::<usize, u64>::new();
    for row in &batches.rows {
        let values = row_map(&batches.header, row, &batch_path)?;
        let shard = required_usize(&values, "shard_id", &batch_path)?;
        let batch = required_u64(&values, "batch_index", &batch_path)?;
        if shard >= shards || !seen_batches.insert((shard, batch)) {
            return Err(format!(
                "{} contains invalid or duplicate batch identity",
                batch_path.display()
            ));
        }
        let transactions = required_u64(&values, "transaction_count", &batch_path)?;
        let keys = required_u64(&values, "keys_looked_up", &batch_path)?;
        let hits = required_u64(&values, "hits", &batch_path)?;
        let misses = required_u64(&values, "misses", &batch_path)?;
        let submitted = required_u64(&values, "groups_submitted", &batch_path)?;
        let in_flight = required_usize(&values, "max_observed_in_flight_groups", &batch_path)?;
        let running = required_usize(&values, "max_observed_running_query_jobs", &batch_path)?;
        if transactions == 0
            || transactions > 2_048
            || keys != transactions
            || hits.saturating_add(misses) != keys
        {
            return Err(format!(
                "{} batch {batch} shard {shard} has inconsistent transaction/index counts",
                batch_path.display()
            ));
        }
        batch_requests = batch_requests
            .checked_add(transactions)
            .ok_or_else(|| "batch transaction count overflow".to_owned())?;
        batch_keys = batch_keys
            .checked_add(keys)
            .ok_or_else(|| "batch query key count overflow".to_owned())?;
        batch_hits = batch_hits
            .checked_add(hits)
            .ok_or_else(|| "batch hit count overflow".to_owned())?;
        batch_misses = batch_misses
            .checked_add(misses)
            .ok_or_else(|| "batch miss count overflow".to_owned())?;
        groups_submitted = groups_submitted
            .checked_add(submitted)
            .ok_or_else(|| "group count overflow".to_owned())?;
        batch_count_by_id.insert((shard, batch), transactions);
        *per_shard_totals.entry(shard).or_default() += transactions;
        let peak = local_peaks.entry(shard).or_default();
        peak.0 = peak.0.max(in_flight);
        peak.1 = peak.1.max(running);
    }
    if batch_requests != expected_requests
        || batch_keys != expected_requests
        || batch_hits != 0
        || batch_misses != expected_requests
        || per_shard_totals.len() != shards
        || per_shard_totals
            .values()
            .any(|total| *total != expected_requests / shards as u64)
    {
        return Err(format!(
            "{} totals do not equal all fresh requests and expected misses",
            batch_path.display()
        ));
    }
    let metadata_path = trial_dir.join(METADATA_FILE);
    let metadata = read_metadata(&metadata_path)?;
    let concurrency = required_usize(&metadata, "index_concurrency", &metadata_path)?;
    let mut seen_groups = BTreeSet::new();
    let mut group_keys = 0_u64;
    let mut groups_by_batch = BTreeMap::<(usize, u64), Vec<(u64, u64, u64)>>::new();
    for row in &groups.rows {
        let values = row_map(&groups.header, row, &group_path)?;
        let shard = required_usize(&values, "shard_id", &group_path)?;
        let batch = required_u64(&values, "batch_index", &group_path)?;
        let group = required_u64(&values, "group_index", &group_path)?;
        let first = required_u64(&values, "first_position", &group_path)?;
        let keys = required_u64(&values, "key_count", &group_path)?;
        let batch_transactions = batch_count_by_id.get(&(shard, batch)).ok_or_else(|| {
            format!(
                "{} references a missing batch {batch} on shard {shard}",
                group_path.display()
            )
        })?;
        if shard >= shards
            || keys == 0
            || keys > 256
            || first
                .checked_add(keys)
                .is_none_or(|end| end > *batch_transactions)
            || !seen_groups.insert((shard, batch, group))
        {
            return Err(format!(
                "{} contains invalid or duplicate lookup group",
                group_path.display()
            ));
        }
        group_keys = group_keys
            .checked_add(keys)
            .ok_or_else(|| "group query key count overflow".to_owned())?;
        groups_by_batch
            .entry((shard, batch))
            .or_default()
            .push((group, first, keys));
    }
    if group_keys != expected_requests || groups_submitted != groups.rows.len() as u64 {
        return Err(format!(
            "{} group keys/submitted totals do not match batch metrics",
            group_path.display()
        ));
    }
    for row in &batches.rows {
        let values = row_map(&batches.header, row, &batch_path)?;
        let key = (
            required_usize(&values, "shard_id", &batch_path)?,
            required_u64(&values, "batch_index", &batch_path)?,
        );
        let submitted = required_u64(&values, "groups_submitted", &batch_path)?;
        let mut coverage = groups_by_batch.get(&key).cloned().unwrap_or_default();
        coverage.sort_unstable_by_key(|(group, _, _)| *group);
        let expected_group_count =
            (required_u64(&values, "transaction_count", &batch_path)? + 255) / 256;
        if coverage.len() as u64 != submitted || submitted != expected_group_count {
            return Err(format!(
                "{} batch {} group rows disagree with groups_submitted",
                batch_path.display(),
                key.1
            ));
        }
        let transaction_count = required_u64(&values, "transaction_count", &batch_path)?;
        for (index, (group_index, first_position, key_count)) in coverage.iter().enumerate() {
            let expected_first = index as u64 * 256;
            let expected_count = (transaction_count - expected_first).min(256);
            if *group_index != index as u64
                || *first_position != expected_first
                || *key_count != expected_count
            {
                return Err(format!(
                    "{} batch {} group rows overlap, omit keys, or use invalid chunk order",
                    group_path.display(),
                    key.1
                ));
            }
        }
        let in_flight = required_usize(&values, "max_observed_in_flight_groups", &batch_path)?;
        let running = required_usize(&values, "max_observed_running_query_jobs", &batch_path)?;
        if in_flight > concurrency || running > concurrency {
            return Err(format!(
                "{} records query peak above configured per-batch cap {concurrency}",
                batch_path.display()
            ));
        }
    }
    let max_in_flight = local_peaks
        .values()
        .map(|(value, _)| *value)
        .max()
        .unwrap_or(0);
    let max_running = local_peaks
        .values()
        .map(|(_, value)| *value)
        .max()
        .unwrap_or(0);
    if required_usize(
        &metadata,
        "per_batch_peak_max_in_flight_groups",
        &metadata_path,
    )? != max_in_flight
        || required_usize(
            &metadata,
            "per_batch_peak_running_query_jobs",
            &metadata_path,
        )? != max_running
    {
        return Err(format!(
            "{} per-batch query peaks disagree with raw batch rows",
            metadata_path.display()
        ));
    }
    let shard_path = trial_dir.join(SHARDS_FILE);
    let shard_data = read_csv(&shard_path)?;
    for row in &shard_data.rows {
        let values = row_map(&shard_data.header, row, &shard_path)?;
        let shard = required_usize(&values, "shard_id", &shard_path)?;
        let expected = local_peaks.get(&shard).copied().ok_or_else(|| {
            format!(
                "{} has no local query peaks for shard {shard}",
                batch_path.display()
            )
        })?;
        if required_usize(&values, "max_in_flight_groups_per_batch", &shard_path)? != expected.0
            || required_usize(&values, "max_running_query_jobs_per_batch", &shard_path)?
                != expected.1
        {
            return Err(format!(
                "{} shard {shard} query peaks disagree with batch rows",
                shard_path.display()
            ));
        }
    }
    Ok(())
}

fn validate_stage_source_values(
    trial_dir: &Path,
    shards: usize,
    observed: &BTreeMap<(String, Option<usize>), Vec<u64>>,
) -> Result<(), String> {
    let mut expected = BTreeMap::<(String, usize), Vec<u64>>::new();
    let batch_path = trial_dir.join(INDEX_BATCHES_FILE);
    let batches = read_csv(&batch_path)?;
    require_header(
        &batches,
        &parse_csv_record(INDEX_BATCH_HEADER)?,
        &batch_path,
    )?;
    let batch_map = [
        ("index.dispatch_wait", "dispatch_wait_ns"),
        ("index.batch_gate_wait", "batch_gate_wait_ns"),
        ("index.key_prep", "key_prep_ns"),
        ("index.query_wall", "query_wall_ns"),
        ("index.blocking_pool_wait", "blocking_pool_wait_ns"),
        ("index.native_get", "native_get_ns"),
        ("index.decode", "decode_ns"),
        ("index.submit_to_collection", "submit_to_collection_ns"),
        (
            "index.apply_submit_to_collection",
            "apply_submit_to_collection_ns",
        ),
        (
            "index.apply_blocking_pool_wait",
            "apply_blocking_pool_wait_ns",
        ),
        ("index.sequential_apply_build", "sequential_apply_build_ns"),
        ("index.sync_write_batch", "sync_write_batch_ns"),
        ("index.memory_publish", "memory_publish_ns"),
    ];
    for row in &batches.rows {
        let values = row_map(&batches.header, row, &batch_path)?;
        let shard = required_usize(&values, "shard_id", &batch_path)?;
        if shard >= shards {
            return Err(format!("{} has invalid shard ID", batch_path.display()));
        }
        for (metric, field) in batch_map {
            expected
                .entry((metric.to_owned(), shard))
                .or_default()
                .push(required_u64(&values, field, &batch_path)?);
        }
    }
    let group_path = trial_dir.join(INDEX_GROUPS_FILE);
    let groups = read_csv(&group_path)?;
    require_header(&groups, &parse_csv_record(INDEX_GROUP_HEADER)?, &group_path)?;
    let group_map = [
        ("index.group_blocking_pool_wait", "blocking_pool_wait_ns"),
        ("index.group_native_get", "native_get_ns"),
        ("index.group_decode", "decode_ns"),
        (
            "index.group_submit_to_collection",
            "submit_to_collection_ns",
        ),
    ];
    for row in &groups.rows {
        let values = row_map(&groups.header, row, &group_path)?;
        let shard = required_usize(&values, "shard_id", &group_path)?;
        if shard >= shards {
            return Err(format!("{} has invalid shard ID", group_path.display()));
        }
        for (metric, field) in group_map {
            expected
                .entry((metric.to_owned(), shard))
                .or_default()
                .push(required_u64(&values, field, &group_path)?);
        }
    }
    let background_path = trial_dir.join(BACKGROUND_FILE);
    let background = read_csv(&background_path)?;
    require_header(&background, &background_header(), &background_path)?;
    let background_map: [(&str, &str, &str); 12] = [
        ("projection", "projection.read", "read_ns"),
        ("projection", "projection.apply", "apply_ns"),
        ("projection", "projection.progress_sync", "progress_sync_ns"),
        ("projection", "projection.total", "total_ns"),
        ("gc", "gc.scan", "gc_scan_ns"),
        ("gc", "gc.delete_build", "gc_delete_ns"),
        ("gc", "gc.sync_write", "gc_write_ns"),
        ("gc", "gc.total", "total_ns"),
        (
            "watermark",
            "watermark.fence_wait",
            "watermark_fence_wait_ns",
        ),
        (
            "watermark",
            "watermark.projection_wait",
            "watermark_projection_wait_ns",
        ),
        ("watermark", "watermark.persist", "watermark_persist_ns"),
        ("watermark", "watermark.total", "total_ns"),
    ];
    for row in &background.rows {
        let values = row_map(&background.header, row, &background_path)?;
        let event = required(&values, "event", &background_path)?;
        let shard = required_usize(&values, "shard_id", &background_path)?;
        if shard >= shards {
            return Err(format!(
                "{} has invalid shard ID",
                background_path.display()
            ));
        }
        for (expected_event, metric, field) in background_map {
            if event == expected_event {
                expected
                    .entry((metric.to_owned(), shard))
                    .or_default()
                    .push(required_u64(&values, field, &background_path)?);
            }
        }
    }
    for metric in STAGE_METRICS {
        for shard in 0..shards {
            let key = (metric.to_owned(), Some(shard));
            let mut actual = observed.get(&key).cloned().unwrap_or_default();
            let mut expected_values = expected
                .get(&(metric.to_owned(), shard))
                .cloned()
                .unwrap_or_default();
            if REQUEST_METRICS
                .iter()
                .any(|(request_metric, _)| *request_metric == metric)
            {
                // The request values were checked against their own logical IDs; they
                // are not sourced from the batch/background CSVs.
                continue;
            }
            actual.sort_unstable();
            expected_values.sort_unstable();
            if actual != expected_values {
                return Err(format!(
                    "raw stage values for {metric} shard {shard} differ from source artifact rows"
                ));
            }
        }
    }
    Ok(())
}

fn validate_background_artifacts(trial_dir: &Path, shards: usize) -> Result<(), String> {
    let path = trial_dir.join(BACKGROUND_FILE);
    let data = read_csv(&path)?;
    require_header(&data, &background_header(), &path)?;
    let metadata_path = trial_dir.join(METADATA_FILE);
    let metadata = read_metadata(&metadata_path)?;
    let cutoff = required_u64(&metadata, "client_end_elapsed_ns", &metadata_path)?;
    let formal = required_usize(&metadata, "users", &metadata_path)? == 50_000
        && required_usize(&metadata, "requests_per_user", &metadata_path)? == 200;
    if formal && data.rows.is_empty() {
        return Err(format!(
            "{} has no client-window background events in a formal trial",
            path.display()
        ));
    }
    let mut seen = BTreeMap::<(usize, String), usize>::new();
    for row in &data.rows {
        let values = row_map(&data.header, row, &path)?;
        let shard = required_usize(&values, "shard_id", &path)?;
        let event = required(&values, "event", &path)?.to_owned();
        let elapsed = required_u64(&values, "elapsed_ns", &path)?;
        if shard >= shards || event.is_empty() || elapsed > cutoff {
            return Err(format!(
                "{} contains an invalid or post-client-end event",
                path.display()
            ));
        }
        for field in [
            "sequence",
            "records",
            "read_ns",
            "apply_ns",
            "progress_sync_ns",
            "total_ns",
            "scanned",
            "deleted",
            "bytes_deleted",
            "gc_prefix_seq",
            "gc_scan_ns",
            "gc_delete_ns",
            "gc_write_ns",
            "watermark_target_sequence",
            "watermark",
            "watermark_fence_wait_ns",
            "watermark_projection_wait_ns",
            "watermark_persist_ns",
        ] {
            required_u64(&values, field, &path)?;
        }
        optional_u64(&values, "blocked_at_seq", &path)?;
        *seen.entry((shard, event)).or_default() += 1;
    }
    if formal {
        for shard in 0..shards {
            for event in ["projection", "watermark", "gc"] {
                if !seen
                    .keys()
                    .any(|(seen_shard, seen_event)| *seen_shard == shard && seen_event == event)
                {
                    return Err(format!(
                        "{} is missing {event} events for shard {shard}",
                        path.display()
                    ));
                }
            }
        }
    }
    Ok(())
}

fn validate_summary_metrics(
    summary: &BTreeMap<String, String>,
    trial_dir: &Path,
) -> Result<(), String> {
    let path = trial_dir.join(SUMMARY_FILE);
    let requests = required_u64(summary, "requests", &path)?;
    let wall = required_f64(summary, "client_wall_s", &path)?;
    let rps = required_f64(summary, "client_rps", &path)?;
    let cpu = required_f64(summary, "cpu_s", &path)?;
    let cores = required_f64(summary, "cpu_core_equivalents", &path)?;
    let cpu_us = required_f64(summary, "cpu_us_per_request", &path)?;
    let settle = required_f64(summary, "settlement_s", &path)?;
    let drain = required_f64(summary, "finite_drain_rps", &path)?;
    if requests == 0
        || wall <= 0.0
        || cpu < 0.0
        || settle < 0.0
        || !close_enough(rps, requests as f64 / wall)
        || !close_enough(cores, cpu / wall)
        || !close_enough(cpu_us, cpu * 1_000_000.0 / requests as f64)
        || !close_enough(drain, requests as f64 / (wall + settle))
    {
        return Err(format!(
            "{} contains invalid wall, CPU, or throughput metrics",
            path.display()
        ));
    }
    for field in [
        "rss_peak_bytes",
        "db_bytes",
        "database_count",
        "configured_write_buffer_bytes",
        "configured_block_cache_bytes",
        "configured_max_background_jobs",
        "wal_syncs",
        "wal_bytes",
        "writes_with_wal",
        "flush_write_bytes",
        "compaction_read_bytes",
        "compaction_write_bytes",
        "rocks_stall_us",
        "process_read_bytes",
        "process_write_bytes",
        "device_read_bytes",
        "device_write_bytes",
        "device_busy_ms",
    ] {
        required_u64(summary, field, &path)?;
    }
    if required(summary, "global_observed_query_group_peak", &path)? != "NA"
        || required(summary, "global_observed_query_job_peak", &path)? != "NA"
    {
        return Err(format!(
            "{} must leave unavailable global query peaks as NA",
            path.display()
        ));
    }
    let setup_cpu = required_f64(summary, "setup_preflight_cpu_pct", &path)?;
    let setup_disk = required_f64(summary, "setup_preflight_device_pct", &path)?;
    let measure_cpu = required_f64(summary, "measure_preflight_cpu_pct", &path)?;
    let measure_disk = required_f64(summary, "measure_preflight_device_pct", &path)?;
    if setup_cpu > 10.0 || setup_disk > 5.0 || measure_cpu > 10.0 || measure_disk > 5.0 {
        return Err(format!(
            "{} violates the strict preflight thresholds",
            path.display()
        ));
    }
    Ok(())
}

fn request_distributions(samples: &[RequestSample]) -> BTreeMap<String, Distribution> {
    let mut values = REQUEST_METRICS
        .iter()
        .map(|(metric, _)| ((*metric).to_owned(), Vec::with_capacity(samples.len())))
        .collect::<BTreeMap<_, _>>();
    for sample in samples {
        values
            .get_mut("request.total")
            .unwrap()
            .push(sample.total_ns);
        values
            .get_mut("request.admission")
            .unwrap()
            .push(sample.admission_ns);
        values
            .get_mut("request.enqueue")
            .unwrap()
            .push(sample.enqueue_ns);
        values
            .get_mut("request.queue")
            .unwrap()
            .push(sample.queue_ns);
        values
            .get_mut("request.batch")
            .unwrap()
            .push(sample.batch_ns);
        values
            .get_mut("request.handler")
            .unwrap()
            .push(sample.handler_ns);
        values
            .get_mut("request.response")
            .unwrap()
            .push(sample.response_ns);
    }
    values
        .into_iter()
        .filter_map(|(metric, values)| distribution(&values).map(|dist| (metric, dist)))
        .collect()
}

fn distribution(values: &[u64]) -> Option<Distribution> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let sum = sorted.iter().map(|value| *value as f64).sum::<f64>();
    Some(Distribution {
        count: sorted.len(),
        mean_ns: sum / sorted.len() as f64,
        p50: nearest_rank(&sorted, 50),
        p95: nearest_rank(&sorted, 95),
        p99: nearest_rank(&sorted, 99),
    })
}

fn nearest_rank(sorted: &[u64], percentile: usize) -> u64 {
    debug_assert!(!sorted.is_empty());
    let rank = (sorted.len() as u128 * percentile as u128)
        .div_ceil(100)
        .max(1) as usize;
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn expected_sample_ids(requests: u64, stride: u64) -> Vec<u64> {
    if stride == 0 {
        return Vec::new();
    }
    (0..requests)
        .filter(|logical_id| splitmix64_local(*logical_id) % stride == 0)
        .collect()
}

fn splitmix64_local(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn batch_peaks(rows: &[String], shards: usize) -> Result<BTreeMap<usize, (usize, usize)>, String> {
    let header = parse_csv_record(INDEX_BATCH_HEADER)?;
    let mut peaks = BTreeMap::<usize, (usize, usize)>::new();
    for (index, row) in rows.iter().enumerate() {
        let fields = parse_csv_record(row)
            .map_err(|error| format!("index batch row {}: {error}", index + 1))?;
        if fields.len() != header.len() {
            return Err(format!(
                "index batch row {} has {} fields, expected {}",
                index + 1,
                fields.len(),
                header.len()
            ));
        }
        let map = row_map(&header, &fields, Path::new("index batch rows"))?;
        let shard = required_usize(&map, "shard_id", Path::new("index batch rows"))?;
        if shard >= shards {
            return Err(format!(
                "index batch row {} has invalid shard {shard}",
                index + 1
            ));
        }
        let in_flight = required_usize(
            &map,
            "max_observed_in_flight_groups",
            Path::new("index batch rows"),
        )?;
        let running = required_usize(
            &map,
            "max_observed_running_query_jobs",
            Path::new("index batch rows"),
        )?;
        let peak = peaks.entry(shard).or_default();
        peak.0 = peak.0.max(in_flight);
        peak.1 = peak.1.max(running);
    }
    if peaks.len() != shards {
        return Err(format!(
            "index batch metrics cover {} shards, expected {shards}",
            peaks.len()
        ));
    }
    Ok(peaks)
}

fn encode_csv_record(values: &[String]) -> String {
    let mut output = String::new();
    for (index, value) in values.iter().enumerate() {
        if index != 0 {
            output.push(',');
        }
        if value
            .chars()
            .any(|character| matches!(character, ',' | '"' | '\n' | '\r'))
        {
            output.push('"');
            for character in value.chars() {
                if character == '"' {
                    output.push_str("\"\"");
                } else {
                    output.push(character);
                }
            }
            output.push('"');
        } else {
            output.push_str(value);
        }
    }
    output.push('\n');
    output
}

fn read_csv(path: &Path) -> Result<CsvData, String> {
    let size = fs::metadata(path)
        .map_err(|error| format!("cannot stat {}: {error}", path.display()))?
        .len();
    if size > 100 * 1024 * 1024 {
        return Err(format!(
            "{} exceeds the 100 MiB per-file limit",
            path.display()
        ));
    }
    let contents = fs::read_to_string(path)
        .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
    let mut lines = contents.lines();
    let first = lines
        .next()
        .ok_or_else(|| format!("{} is empty", path.display()))?;
    let header =
        parse_csv_record(first).map_err(|error| format!("{} header: {error}", path.display()))?;
    if header.is_empty() {
        return Err(format!("{} has an empty header", path.display()));
    }
    let mut names = BTreeSet::new();
    for name in &header {
        if name.is_empty() || !names.insert(name.clone()) {
            return Err(format!(
                "{} has an empty or duplicate header field",
                path.display()
            ));
        }
    }
    let mut rows = Vec::new();
    for (index, line) in lines.enumerate() {
        let row = parse_csv_record(line)
            .map_err(|error| format!("{} row {}: {error}", path.display(), index + 2))?;
        if row.len() != header.len() {
            return Err(format!(
                "{} row {} has {} fields, expected {}",
                path.display(),
                index + 2,
                row.len(),
                header.len()
            ));
        }
        rows.push(row);
    }
    Ok(CsvData { header, rows })
}

fn parse_csv_record(line: &str) -> Result<Vec<String>, String> {
    let mut fields = Vec::new();
    let mut field = String::new();
    let mut chars = line.chars().peekable();
    let mut quoted = false;
    let mut closed_quote = false;
    while let Some(character) = chars.next() {
        if quoted {
            match character {
                '"' if chars.peek() == Some(&'"') => {
                    chars.next();
                    field.push('"');
                }
                '"' => {
                    quoted = false;
                    closed_quote = true;
                }
                _ => field.push(character),
            }
        } else if closed_quote {
            if character != ',' {
                return Err(format!("unexpected {character:?} after quoted field"));
            }
            fields.push(std::mem::take(&mut field));
            closed_quote = false;
        } else {
            match character {
                ',' => fields.push(std::mem::take(&mut field)),
                '"' if field.is_empty() => quoted = true,
                '"' => return Err("quote inside unquoted field".to_owned()),
                _ => field.push(character),
            }
        }
    }
    if quoted {
        return Err("unterminated quoted field".to_owned());
    }
    fields.push(field);
    Ok(fields)
}

fn require_header(data: &CsvData, expected: &[String], path: &Path) -> Result<(), String> {
    if data.header != expected {
        return Err(format!(
            "{} header does not match the fixed artifact schema",
            path.display()
        ));
    }
    Ok(())
}

fn row_map(
    header: &[String],
    row: &[String],
    path: &Path,
) -> Result<BTreeMap<String, String>, String> {
    if header.len() != row.len() {
        return Err(format!("{} row width differs from header", path.display()));
    }
    Ok(header.iter().cloned().zip(row.iter().cloned()).collect())
}

fn read_metadata(path: &Path) -> Result<BTreeMap<String, String>, String> {
    let size = fs::metadata(path)
        .map_err(|error| format!("cannot stat {}: {error}", path.display()))?
        .len();
    if size > 100 * 1024 * 1024 {
        return Err(format!(
            "{} exceeds the 100 MiB per-file limit",
            path.display()
        ));
    }
    let contents = fs::read_to_string(path)
        .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
    let mut values = BTreeMap::new();
    for line in contents.lines() {
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        if key.is_empty() || values.insert(key.to_owned(), value.to_owned()).is_some() {
            return Err(format!(
                "{} has an empty or duplicate metadata key",
                path.display()
            ));
        }
    }
    Ok(values)
}

fn required<'a>(
    values: &'a BTreeMap<String, String>,
    key: &str,
    path: &Path,
) -> Result<&'a str, String> {
    values
        .get(key)
        .map(String::as_str)
        .ok_or_else(|| format!("{} is missing required field {key}", path.display()))
}

fn required_u64(values: &BTreeMap<String, String>, key: &str, path: &Path) -> Result<u64, String> {
    required(values, key, path)?
        .parse::<u64>()
        .map_err(|error| format!("{} field {key} is not u64: {error}", path.display()))
}

fn required_usize(
    values: &BTreeMap<String, String>,
    key: &str,
    path: &Path,
) -> Result<usize, String> {
    usize::try_from(required_u64(values, key, path)?)
        .map_err(|_| format!("{} field {key} does not fit usize", path.display()))
}

fn required_f64(values: &BTreeMap<String, String>, key: &str, path: &Path) -> Result<f64, String> {
    let value = required(values, key, path)?
        .parse::<f64>()
        .map_err(|error| format!("{} field {key} is not f64: {error}", path.display()))?;
    if !value.is_finite() {
        return Err(format!("{} field {key} is not finite", path.display()));
    }
    Ok(value)
}

fn optional_u64(
    values: &BTreeMap<String, String>,
    key: &str,
    path: &Path,
) -> Result<Option<u64>, String> {
    match required(values, key, path)? {
        "NA" => Ok(None),
        value => value.parse::<u64>().map(Some).map_err(|error| {
            format!(
                "{} field {key} is not optional u64: {error}",
                path.display()
            )
        }),
    }
}

fn optional_usize(
    values: &BTreeMap<String, String>,
    key: &str,
    path: &Path,
) -> Result<Option<usize>, String> {
    optional_u64(values, key, path)?
        .map(|value| {
            usize::try_from(value)
                .map_err(|_| format!("{} field {key} does not fit usize", path.display()))
        })
        .transpose()
}

fn metadata_escape(value: &str) -> String {
    value
        .replace('%', "%25")
        .replace('\r', "%0D")
        .replace('\n', "%0A")
        .replace('=', "%3D")
}

fn float(value: f64) -> String {
    if value.is_finite() {
        format!("{value:.9}")
    } else {
        "NA".to_owned()
    }
}

fn close_enough(left: f64, right: f64) -> bool {
    (left - right).abs() <= 1.0e-8_f64.max(left.abs().max(right.abs()) * 1.0e-8)
}

fn option_u64(value: Option<u64>) -> String {
    value.map_or_else(|| "NA".to_owned(), |value| value.to_string())
}

fn option_usize(value: Option<usize>) -> String {
    value.map_or_else(|| "NA".to_owned(), |value| value.to_string())
}

fn join_u64(values: &[u64]) -> String {
    values
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

fn case_name(shards: usize, layout: &str, concurrency: usize) -> Result<String, String> {
    if ![2, 4].contains(&shards)
        || ![4, 8].contains(&concurrency)
        || !["shared", "dedicated"].contains(&layout)
    {
        return Err(format!(
            "unsupported matrix case S{shards}/{layout}/P{concurrency}"
        ));
    }
    Ok(format!("s{shards}_{layout}_chunked256_c{concurrency}"))
}

fn checked_product(left: u64, right: u64, what: &str) -> Result<u64, String> {
    left.checked_mul(right)
        .ok_or_else(|| format!("{what} count overflow"))
}

fn metadata_path_display(trial_dir: &Path) -> String {
    trial_dir.join(METADATA_FILE).display().to_string()
}

pub(super) fn write_matrix_reports(
    run_dir: &Path,
    completed_dirs: &[(usize, MatrixCase, String, PathBuf)],
) -> Result<(), String> {
    let run_dir = run_dir.canonicalize().map_err(|error| {
        format!(
            "cannot resolve run directory {}: {error}",
            run_dir.display()
        )
    })?;
    let manifest_path = run_dir.join("run_manifest.txt");
    let manifest = read_metadata(&manifest_path)?;
    let profile = required(&manifest, "profile", &manifest_path)?;
    let formal = profile == "FORMAL_MATRIX";
    if !formal && profile != "NONFORMAL_REDUCED_SMOKE" {
        return Err(format!(
            "{} has an unsupported run profile",
            manifest_path.display()
        ));
    }
    let expected_cases = if formal {
        formal_matrix_names()
    } else {
        vec!["s2_shared_chunked256_c4".to_owned()]
    };
    let actual_cases = required(&manifest, "shard_cases", &manifest_path)?
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let expected_repetitions = if formal { 3 } else { 1 };
    let users = required_usize(&manifest, "users", &manifest_path)?;
    let requests_per_user = required_usize(&manifest, "requests_per_user", &manifest_path)?;
    let expected_users = if formal { 50_000 } else { 200 };
    if actual_cases != expected_cases
        || required_usize(&manifest, "repetitions", &manifest_path)? != expected_repetitions
        || users != expected_users
        || requests_per_user != 200
        || required_u64(&manifest, "expected_requests_per_trial", &manifest_path)?
            != users as u64 * requests_per_user as u64
    {
        return Err(format!(
            "{} does not describe the required fixed matrix",
            manifest_path.display()
        ));
    }
    let expected_order = expected_trial_order(formal);
    if completed_dirs.len() != expected_order.len() {
        return Err(format!(
            "run has {} completed trials, expected {}",
            completed_dirs.len(),
            expected_order.len()
        ));
    }
    let mut records = Vec::with_capacity(completed_dirs.len());
    for (
        position,
        (
            (repetition, case, trial_name, trial_dir),
            (expected_rep, expected_pos, expected_case, expected_name),
        ),
    ) in completed_dirs.iter().zip(expected_order.iter()).enumerate()
    {
        let expected_dir = run_dir.join("trials").join(expected_name);
        let dir = trial_dir.canonicalize().map_err(|error| {
            format!(
                "cannot resolve trial directory {}: {error}",
                trial_dir.display()
            )
        })?;
        if repetition != expected_rep
            || case.name() != expected_case.as_str()
            || trial_name != expected_name
            || dir != expected_dir
        {
            return Err(format!(
                "trial at matrix position {} is out of the required rotated order",
                position + 1
            ));
        }
        validate_trial_artifacts(&dir)?;
        records.push(load_trial_record(
            *repetition,
            *expected_pos,
            case.clone(),
            trial_name.clone(),
            dir,
            users,
            requests_per_user,
        )?);
    }
    let root_from_manifest = PathBuf::from(required(&manifest, "repo_root", &manifest_path)?);
    let repo_root = root_from_manifest
        .canonicalize()
        .map_err(|error| format!("cannot resolve recorded repository root: {error}"))?;
    let archive_report = make_report(&run_dir, &repo_root, &records, formal, false)?;
    write_derived_csv(
        &run_dir.join("matrix_trials.csv"),
        &matrix_trial_rows(&records)?,
    )?;
    write_derived_csv(
        &run_dir.join("matrix_shards.csv"),
        &matrix_shard_rows(&records)?,
    )?;
    write_derived_csv(
        &run_dir.join("matrix_stages.csv"),
        &matrix_stage_rows(&records)?,
    )?;
    write_derived_csv(
        &run_dir.join("trial_manifest.csv"),
        &trial_manifest_rows(&records)?,
    )?;
    write_derived_text(&run_dir.join("report.md"), &archive_report)?;
    if formal {
        let canonical_report = make_report(&run_dir, &repo_root, &records, true, true)?;
        write_derived_text(
            &repo_root.join("benches/ledger_pipeline_sharding_tokio_report.md"),
            &canonical_report,
        )?;
    }
    Ok(())
}

pub(super) fn report_only(repo_root: &Path, run_dir: &Path) -> Result<(), String> {
    let repo_root = repo_root.canonicalize().map_err(|error| {
        format!(
            "cannot resolve repository root {}: {error}",
            repo_root.display()
        )
    })?;
    let requested = if run_dir.is_absolute() {
        run_dir.to_path_buf()
    } else {
        repo_root.join(run_dir)
    };
    let expected_parent = repo_root.join("benches/data/ledger_pipeline_sharding");
    let leaf = requested
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| "--report-only must name a run-<numeric-id> directory".to_owned())?;
    let id = leaf
        .strip_prefix("run-")
        .ok_or_else(|| "--report-only must name a run-<numeric-id> directory".to_owned())?;
    if id.is_empty()
        || !id.bytes().all(|byte| byte.is_ascii_digit())
        || requested.parent() != Some(expected_parent.as_path())
    {
        return Err("--report-only path must be a direct run-<numeric-id> child of benches/data/ledger_pipeline_sharding".to_owned());
    }
    ensure_no_symlink_path(&repo_root, &requested)?;
    let canonical_run = requested.canonicalize().map_err(|error| {
        format!(
            "cannot resolve report-only run {}: {error}",
            requested.display()
        )
    })?;
    if canonical_run != requested {
        return Err(
            "--report-only path resolves through a symlink or noncanonical path".to_owned(),
        );
    }
    let manifest_path = canonical_run.join("run_manifest.txt");
    let manifest = read_metadata(&manifest_path)?;
    if required(&manifest, "profile", &manifest_path)? != "FORMAL_MATRIX"
        || required(&manifest, "repo_root", &manifest_path)? != repo_root.to_string_lossy()
    {
        return Err(
            "--report-only requires a formal matrix archive from this repository".to_owned(),
        );
    }
    let expected_order = expected_trial_order(true);
    let mut completed = Vec::with_capacity(expected_order.len());
    for (repetition, position, case_name, trial_name) in expected_order {
        let trial_dir = canonical_run.join("trials").join(&trial_name);
        let metadata = read_metadata(&trial_dir.join(METADATA_FILE))?;
        let shards = required_usize(&metadata, "shards", &trial_dir.join(METADATA_FILE))?;
        let layout = RocksLayout::parse(required(
            &metadata,
            "layout",
            &trial_dir.join(METADATA_FILE),
        )?)?;
        let concurrency = required_usize(
            &metadata,
            "index_concurrency",
            &trial_dir.join(METADATA_FILE),
        )?;
        let case = MatrixCase {
            shards,
            layout,
            concurrency,
        };
        if case.name() != case_name {
            return Err(format!(
                "{} case differs from the formal matrix",
                trial_dir.display()
            ));
        }
        completed.push((repetition, case, trial_name, trial_dir));
        let _ = position;
    }
    write_matrix_reports(&canonical_run, &completed)
}

fn load_trial_record(
    repetition: usize,
    position: usize,
    case: MatrixCase,
    trial_name: String,
    trial_dir: PathBuf,
    users: usize,
    requests_per_user: usize,
) -> Result<TrialRecord, String> {
    let status_path = trial_dir.join("status.txt");
    let status_text = fs::read_to_string(&status_path)
        .map_err(|error| format!("cannot read {}: {error}", status_path.display()))?;
    let status = read_metadata(&status_path)?;
    if required(&status, "trial", &status_path)? != trial_name
        || required(&status, "case", &status_path)? != case.name()
        || required_usize(&status, "repetition", &status_path)? != repetition
        || required_usize(&status, "position", &status_path)? != position
        || required(&status, "exit_status", &status_path)? != "exit status: 0"
        || required(&status, "stdout", &status_path)? != "stdout.log"
        || required(&status, "stderr", &status_path)? != "stderr.log"
    {
        return Err(format!(
            "{} does not record a successful matching child trial",
            status_path.display()
        ));
    }
    for name in ["stdout.log", "stderr.log"] {
        if !trial_dir.join(name).is_file() {
            return Err(format!(
                "{} is missing required child log {name}",
                trial_dir.display()
            ));
        }
    }
    let metadata_path = trial_dir.join(METADATA_FILE);
    let metadata = read_metadata(&metadata_path)?;
    if required(&metadata, "trial_name", &metadata_path)? != trial_name
        || required(&metadata, "case", &metadata_path)? != case.name()
        || required_usize(&metadata, "shards", &metadata_path)? != case.shards
        || required(&metadata, "layout", &metadata_path)? != case.layout.as_str()
        || required_usize(&metadata, "index_concurrency", &metadata_path)? != case.concurrency
        || required_usize(&metadata, "users", &metadata_path)? != users
        || required_usize(&metadata, "requests_per_user", &metadata_path)? != requests_per_user
    {
        return Err(format!(
            "{} does not match its matrix and status identity",
            metadata_path.display()
        ));
    }
    let summary = read_csv(&trial_dir.join(SUMMARY_FILE))?;
    let shards = read_csv(&trial_dir.join(SHARDS_FILE))?;
    let stages = read_csv(&trial_dir.join(STAGES_FILE))?;
    require_header(&summary, &summary_header(), &trial_dir.join(SUMMARY_FILE))?;
    require_header(&shards, &shard_header(), &trial_dir.join(SHARDS_FILE))?;
    require_header(&stages, &stage_header(), &trial_dir.join(STAGES_FILE))?;
    if summary.rows.len() != 1 {
        return Err(format!(
            "{} must have one row",
            trial_dir.join(SUMMARY_FILE).display()
        ));
    }
    Ok(TrialRecord {
        repetition,
        position,
        case,
        trial_name,
        trial_dir,
        status: status_text
            .lines()
            .find(|line| line.starts_with("exit_status="))
            .unwrap_or("exit_status=missing")
            .to_owned(),
        summary_header: summary.header,
        summary_row: summary.rows.into_iter().next().unwrap(),
        shard_header: shards.header,
        shard_rows: shards.rows,
        stage_header: stages.header,
        stage_rows: stages.rows,
        metadata,
        stdout: "stdout.log".to_owned(),
        stderr: "stderr.log".to_owned(),
    })
}

fn expected_trial_order(formal: bool) -> Vec<(usize, usize, String, String)> {
    let mut order = Vec::new();
    let base = if formal {
        formal_matrix_names()
    } else {
        vec!["s2_shared_chunked256_c4".to_owned()]
    };
    let repetitions = if formal { 3 } else { 1 };
    for repetition in 1..=repetitions {
        let mut cases = base.clone();
        if formal {
            let rotation = ((repetition - 1) * 3) % cases.len();
            cases.rotate_left(rotation);
        }
        for (index, case) in cases.into_iter().enumerate() {
            let position = index + 1;
            let trial_name = format!("trial-{repetition:02}-{case}-p{position:02}");
            order.push((repetition, position, case, trial_name));
        }
    }
    order
}

fn formal_matrix_names() -> Vec<String> {
    [2, 4]
        .into_iter()
        .flat_map(|shards| {
            ["shared", "dedicated"].into_iter().flat_map(move |layout| {
                [4, 8]
                    .into_iter()
                    .map(move |concurrency| format!("s{shards}_{layout}_chunked256_c{concurrency}"))
            })
        })
        .collect()
}

fn ensure_no_symlink_path(root: &Path, target: &Path) -> Result<(), String> {
    let relative = target
        .strip_prefix(root)
        .map_err(|_| "--report-only path is outside the repository".to_owned())?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        let Component::Normal(name) = component else {
            return Err("--report-only path contains a non-normal component".to_owned());
        };
        current.push(name);
        let metadata = fs::symlink_metadata(&current)
            .map_err(|error| format!("cannot inspect {}: {error}", current.display()))?;
        if metadata.file_type().is_symlink() {
            return Err(format!(
                "--report-only path traverses symlink {}",
                current.display()
            ));
        }
    }
    Ok(())
}

fn matrix_trial_rows(records: &[TrialRecord]) -> Result<(Vec<String>, Vec<Vec<String>>), String> {
    let mut header = [
        "repetition",
        "position",
        "manifest_case",
        "manifest_trial_name",
        "trial_dir",
        "status",
        "stdout",
        "stderr",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect::<Vec<_>>();
    let summary_header = records
        .first()
        .ok_or_else(|| "empty validated matrix".to_owned())?
        .summary_header
        .clone();
    header.extend(summary_header);
    let mut rows = Vec::new();
    for record in records {
        let mut row = vec![
            record.repetition.to_string(),
            record.position.to_string(),
            record.case.name(),
            record.trial_name.clone(),
            format!("trials/{}", record.trial_name),
            record.status.clone(),
            record.stdout.clone(),
            record.stderr.clone(),
        ];
        row.extend(record.summary_row.clone());
        rows.push(row);
    }
    Ok((header, rows))
}

fn matrix_shard_rows(records: &[TrialRecord]) -> Result<(Vec<String>, Vec<Vec<String>>), String> {
    let mut header = [
        "repetition",
        "position",
        "manifest_case",
        "manifest_trial_name",
        "trial_dir",
    ]
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    header.extend(
        records
            .first()
            .ok_or_else(|| "empty validated matrix".to_owned())?
            .shard_header
            .clone(),
    );
    let mut rows = Vec::new();
    for record in records {
        for shard in &record.shard_rows {
            let mut row = vec![
                record.repetition.to_string(),
                record.position.to_string(),
                record.case.name(),
                record.trial_name.clone(),
                format!("trials/{}", record.trial_name),
            ];
            row.extend(shard.clone());
            rows.push(row);
        }
    }
    Ok((header, rows))
}

fn matrix_stage_rows(records: &[TrialRecord]) -> Result<(Vec<String>, Vec<Vec<String>>), String> {
    let mut header = [
        "repetition",
        "position",
        "manifest_case",
        "manifest_trial_name",
        "trial_dir",
    ]
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    header.extend(
        records
            .first()
            .ok_or_else(|| "empty validated matrix".to_owned())?
            .stage_header
            .clone(),
    );
    let mut rows = Vec::new();
    for record in records {
        for stage in &record.stage_rows {
            let mut row = vec![
                record.repetition.to_string(),
                record.position.to_string(),
                record.case.name(),
                record.trial_name.clone(),
                format!("trials/{}", record.trial_name),
            ];
            row.extend(stage.clone());
            rows.push(row);
        }
    }
    Ok((header, rows))
}

fn trial_manifest_rows(records: &[TrialRecord]) -> Result<(Vec<String>, Vec<Vec<String>>), String> {
    let header = [
        "repetition",
        "position",
        "case",
        "trial_name",
        "trial_dir",
        "status",
        "stdout",
        "stderr",
        "summary",
        "shards",
        "stages",
        "raw_stage_samples",
        "request_samples",
        "background",
        "index_batches",
        "index_groups",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    let rows = records
        .iter()
        .map(|record| {
            let base = format!("trials/{}", record.trial_name);
            vec![
                record.repetition.to_string(),
                record.position.to_string(),
                record.case.name(),
                record.trial_name.clone(),
                base.clone(),
                record.status.clone(),
                format!("{base}/{}", record.stdout),
                format!("{base}/{}", record.stderr),
                format!("{base}/{SUMMARY_FILE}"),
                format!("{base}/{SHARDS_FILE}"),
                format!("{base}/{STAGES_FILE}"),
                format!("{base}/{RAW_STAGES_FILE}"),
                format!("{base}/{REQUEST_SAMPLES_FILE}"),
                format!("{base}/{BACKGROUND_FILE}"),
                format!("{base}/{INDEX_BATCHES_FILE}"),
                format!("{base}/{INDEX_GROUPS_FILE}"),
            ]
        })
        .collect();
    Ok((header, rows))
}

fn make_report(
    run_dir: &Path,
    repo_root: &Path,
    records: &[TrialRecord],
    formal: bool,
    canonical: bool,
) -> Result<String, String> {
    let mut report = String::new();
    report.push_str("# Ledger Pipeline 分片基準報告\n\n");
    let profile = if formal {
        "正式矩陣"
    } else {
        "非正式縮小 smoke"
    };
    report.push_str(&format!("本報告來自 `{}`，profile 為 **{}**。原始 trial CSV、日誌、狀態與 run manifest 保存在該 archive；這些報告表是由通過驗證的 trial 資料衍生。\n\n", run_dir.file_name().and_then(|name| name.to_str()).unwrap_or("unknown-run"), profile));
    report.push_str("## 固定工作負載\n\n");
    if formal {
        report.push_str("正式比較只包含 S2/S4、shared/dedicated RocksDB、Chunked group 256 的 P4/P8，共 8 cases；每 case 3 次旋轉順序的全新 trial。每 trial 為 50,000 個全域 account、每 account 200 個循序請求，共 10,000,000 個請求；每 account 先在計時窗外寫入 3 筆 seed。請求各半 credit/debit、金額 1，期末餘額 100。account ID 以 `account_id % shard_count` 路由，同一 account 的請求保持順序。\n\n");
    } else {
        report.push_str("這是 correctness smoke：單一 S2/shared/P4 trial、200 個 account、每 account 200 個循序請求。它不屬於正式 24-trial 比較，也不代表效能結論。\n\n");
    }
    let queue_total = records
        .first()
        .and_then(|record| record.metadata.get("queue_capacity_total"))
        .ok_or_else(|| "trial metadata is missing queue capacity".to_owned())?;
    report.push_str(&format!("固定參數：4 個 Tokio async workers；每 trial 有 {queue_total} 個總 queue slots，按 shard 平分；batch 2048、首次 dequeue 等待 5 ms；PerBatch 原子同步帳本寫入；projector/GC batch 256、watermark tick 100 ms、retention 500 ms、GC 閒置 tick 100 ms。4 workers 不代表 CPU 配額。每個 DB 的 write buffer 與 cache 按 DB 數切分，總設定各 128 MiB；每 DB 的 `max_background_jobs` 合計 8。child 共用 RocksDB default Env，LOW/HIGH thread pool 分別設為 6/2；pool 與 jobs 是設定值，不是硬體或活躍執行緒上限。\n\n"));
    if formal {
        append_formal_results(&mut report, run_dir, repo_root, records, canonical)?;
        append_comparisons(&mut report, records)?;
        let report_base = if canonical {
            repo_root.join("benches")
        } else {
            run_dir.to_path_buf()
        };
        let old_report_link = relative_path(
            &report_base,
            &repo_root.join("benches/ledger_pipeline_index_lookup_tokio_report.md"),
        )?
        .to_string_lossy()
        .replace('\\', "/");
        report.push_str(&format!("歷史 S1 基線只作背景描述：P4 為 56,982.273 RPS、2.093051 CPU cores、request p50/p95/p99 為 733.594762/1,131.694630/5,850.762527 ms；P8 為 75,076.856 RPS、3.177564 CPU cores、652.001851/957.783005/1,124.482976 ms。該基線使用不同時點、RocksDB 預設資源配置與舊 namespace，屬未配對比較，不能用來主張因果速度提升。詳見[舊報告]({old_report_link})。\n\n"));
    } else {
        let record = records
            .first()
            .ok_or_else(|| "smoke report has no trial".to_owned())?;
        let rps = summary_value(record, "client_rps")?;
        let cpu = summary_value(record, "cpu_core_equivalents")?;
        report.push_str(&format!("## Smoke 觀測\n\n本次單次觀測為 {:.3} client RPS、{:.3} CPU core equivalents。這只用來確認工作負載與 artifact 完整性，不作矩陣比較。\n\n", rps, cpu));
    }
    report.push_str("## 測量口徑與限制\n\n");
    report.push_str("client RPS 使用全域 start barrier 到最後一個 reply 的時間；background workers 在最後 reply 後仍運作。finite drain RPS 使用請求數除以 client wall 加 settlement，代表這次有限工作集的完整耗時，不是穩態容量。settlement 包含收集、snapshot、catch-up、durable boundary、GC 與結束前檢查。各 shard 的 common-window RPS 與最後 progress/backlog 在 `shards.csv`；CPU 只量整個 child process 在 client window 的總 delta，涵蓋 runtime、native 與 background threads，沒有 per-shard CPU 歸因。\n\n");
    report.push_str("每 trial 的理論 query-group 上限為 S×P；全域實際 peak 無法由現有指標觀測，所以明確記為 NA，不把 per-shard peaks 相加。`index_batches.csv` 與 `shards.csv` 保留可觀測的 batch/shard maxima。shared DB 的 RocksDB stats 只計一次，dedicated 則加總唯一 DB；WAL sync 數為觀測值，RocksDB 可能協調或合併同步寫入，不應等同 foreground batch 數或 shard commit 數。stage quantiles 由原始樣本彙整，native operation 可能重疊，不能相加成總延遲。請求樣本只有原始 duration，未捕捉逐請求完成時間；沒有補造 timestamp。\n\n");
    report.push_str("前置檢查要求 CPU busy ≤10%、目標裝置 busy ≤5%，最多等待 60 秒。正式試次 setup 與 measurement preflight 各觀測 3 秒；smoke child 與 reduced test 觀測 100 ms，並使用縮小資源預留。RSS 是 process lifetime peak；DB bytes 是 recovery/integrity 驗證並關閉 unique DB 後、scratch cleanup 前的目錄大小。Mock destination 以成功 apply 的 in-memory contract 驗證，不代表外部 DB process crash durability。兩個 shard 數不足以證明整機 CPU 飽和、效能平台期或穩態背壓 SLA。\n\n");
    report.push_str("### Archive 與原始資料\n\n");
    let base = if canonical {
        let rel = relative_path(&repo_root.join("benches"), run_dir)?;
        rel.to_string_lossy().replace('\\', "/")
    } else {
        String::new()
    };
    if canonical {
        report.push_str(&format!("完整試次索引：[trial_manifest.csv]({base}/trial_manifest.csv)、[matrix_trials.csv]({base}/matrix_trials.csv)、[matrix_shards.csv]({base}/matrix_shards.csv)、[matrix_stages.csv]({base}/matrix_stages.csv)、[run_manifest.txt]({base}/run_manifest.txt)。\n\n"));
    } else {
        report.push_str("完整試次索引：[trial_manifest.csv](trial_manifest.csv)、[matrix_trials.csv](matrix_trials.csv)、[matrix_shards.csv](matrix_shards.csv)、[matrix_stages.csv](matrix_stages.csv)、[run_manifest.txt](run_manifest.txt)。\n\n");
    }
    Ok(report)
}

fn append_formal_results(
    report: &mut String,
    run_dir: &Path,
    repo_root: &Path,
    records: &[TrialRecord],
    canonical: bool,
) -> Result<(), String> {
    report.push_str("## 正式矩陣結果\n\n");
    report.push_str("每列彙整同一 case 的 3 個 trial：RPS、CPU、有限 drain RPS 顯示 median [min–max]；request p50/p95/p99 顯示各 trial percentile 的 median，p99 另列 trial 間範圍。延遲單位為 ms。\n\n");
    report.push_str("| Case | client RPS | CPU cores | CPU µs/request | request p50 / p95 / p99 ms | p99 trial 範圍 ms | finite drain RPS | client-end projection / destination / GC backlog records | raw trials |\n|---|---:|---:|---:|---:|---:|---:|---:|---|\n");
    for case in formal_matrix_names() {
        let group = records
            .iter()
            .filter(|record| record.case.name() == case)
            .collect::<Vec<_>>();
        if group.len() != 3 {
            return Err(format!(
                "formal report has {} trials for {case}, expected 3",
                group.len()
            ));
        }
        let p50 = trial_median(&group, "request_total_p50_ns")? / 1_000_000.0;
        let p95 = trial_median(&group, "request_total_p95_ns")? / 1_000_000.0;
        let p99 = trial_median(&group, "request_total_p99_ns")? / 1_000_000.0;
        let p99_min = group
            .iter()
            .map(|record| summary_value(record, "request_total_p99_ns"))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .fold(f64::INFINITY, f64::min)
            / 1_000_000.0;
        let p99_max = group
            .iter()
            .map(|record| summary_value(record, "request_total_p99_ns"))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .fold(f64::NEG_INFINITY, f64::max)
            / 1_000_000.0;
        let projection = median(
            group
                .iter()
                .map(|record| summary_value(record, "projection_backlog_sum"))
                .collect::<Result<Vec<_>, _>>()?,
        );
        let gc = median(
            group
                .iter()
                .map(|record| summary_value(record, "gc_backlog_sum"))
                .collect::<Result<Vec<_>, _>>()?,
        );
        let destination = median(
            group
                .iter()
                .map(|record| {
                    record.shard_rows.iter().try_fold(0_f64, |sum, row| {
                        let map = row_map(&record.shard_header, row, Path::new("shards.csv"))?;
                        Ok::<_, String>(
                            sum + required_u64(
                                &map,
                                "destination_backlog_records",
                                Path::new("shards.csv"),
                            )? as f64,
                        )
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
        );
        let rps = format_median_range(&group, "client_rps", 3)?;
        let cpu = format_median_range(&group, "cpu_core_equivalents", 3)?;
        let cpu_us = format_median_range(&group, "cpu_us_per_request", 3)?;
        let drain = format_median_range(&group, "finite_drain_rps", 3)?;
        let link_base = if canonical {
            format!(
                "{}/trials",
                relative_path(&repo_root.join("benches"), run_dir)?
                    .to_string_lossy()
                    .replace('\\', "/")
            )
        } else {
            "trials".to_owned()
        };
        let links = group
            .iter()
            .map(|record| {
                format!(
                    "[r{}]({link_base}/{}/summary.csv)",
                    record.repetition, record.trial_name
                )
            })
            .collect::<Vec<_>>()
            .join(" ");
        report.push_str(&format!("| {case} | {rps} | {cpu} | {cpu_us} | {:.3} / {:.3} / {:.3} | {:.3}–{:.3} | {drain} | {:.0} / {:.0} / {:.0} | {links} |\n", p50, p95, p99, p99_min, p99_max, projection, destination, gc));
    }
    report.push_str("\n儲存與資源量測使用每 case 三次 trial 的 median；bytes/syncs 為 RocksDB/process/device counter delta，非每 shard 歸因。\n\n");
    report.push_str("| Case | WAL syncs | WAL bytes | flush write bytes | compaction read/write bytes | stall µs | process read/write bytes | device read/write bytes | RSS peak MiB | final DB MiB | preflight CPU/device max % |\n|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
    for case in formal_matrix_names() {
        let group = records
            .iter()
            .filter(|record| record.case.name() == case)
            .collect::<Vec<_>>();
        let val = |key: &str| -> Result<f64, String> { trial_median(&group, key) };
        let preflight = group
            .iter()
            .map(|record| {
                Ok::<_, String>([
                    summary_value(record, "setup_preflight_cpu_pct")?,
                    summary_value(record, "measure_preflight_cpu_pct")?,
                    summary_value(record, "setup_preflight_device_pct")?,
                    summary_value(record, "measure_preflight_device_pct")?,
                ])
            })
            .collect::<Result<Vec<_>, String>>()?;
        let pre_cpu = preflight
            .iter()
            .map(|values| values[0].max(values[1]))
            .fold(0.0_f64, f64::max);
        let pre_disk = preflight
            .iter()
            .map(|values| values[2].max(values[3]))
            .fold(0.0_f64, f64::max);
        report.push_str(&format!("| {case} | {:.0} | {:.0} | {:.0} | {:.0} / {:.0} | {:.0} | {:.0} / {:.0} | {:.0} / {:.0} | {:.1} | {:.1} | {:.2} / {:.2} |\n",
            val("wal_syncs")?, val("wal_bytes")?, val("flush_write_bytes")?, val("compaction_read_bytes")?, val("compaction_write_bytes")?, val("rocks_stall_us")?, val("process_read_bytes")?, val("process_write_bytes")?, val("device_read_bytes")?, val("device_write_bytes")?, val("rss_peak_bytes")? / 1024.0 / 1024.0, val("db_bytes")? / 1024.0 / 1024.0, pre_cpu, pre_disk));
    }
    report.push_str("\nPer-shard common-window RPS、client-end 與 final sequence/progress、GC prefix、final watermark timestamp/target 序號、每 batch 查詢峰值見各 trial 的 `shards.csv`；36 個固定 stage 在 `stages.csv`，raw samples 分別在 `raw_stage_samples.csv` 與 `request_samples.csv`。\n\n");
    Ok(())
}

fn append_comparisons(report: &mut String, records: &[TrialRecord]) -> Result<(), String> {
    report.push_str("## 矩陣內描述性差異\n\n");
    report.push_str(
        "下表使用各組 trial median 計算百分比差異；只描述本矩陣的觀測，不作因果或飽和推論。\n\n",
    );
    report.push_str("| 配對 | client RPS 差異 | CPU cores 差異 |\n|---|---:|---:|\n");
    let mut row = |label: String, left: &str, right: &str| -> Result<(), String> {
        let a = records
            .iter()
            .filter(|record| record.case.name() == left)
            .collect::<Vec<_>>();
        let b = records
            .iter()
            .filter(|record| record.case.name() == right)
            .collect::<Vec<_>>();
        if a.len() != 3 || b.len() != 3 {
            return Err(format!("missing trial pair {left} / {right}"));
        }
        let rps_delta =
            (trial_median(&b, "client_rps")? / trial_median(&a, "client_rps")? - 1.0) * 100.0;
        let cpu_delta = (trial_median(&b, "cpu_core_equivalents")?
            / trial_median(&a, "cpu_core_equivalents")?
            - 1.0)
            * 100.0;
        report.push_str(&format!(
            "| {label} | {rps_delta:+.2}% | {cpu_delta:+.2}% |\n"
        ));
        Ok(())
    };
    for shards in [2, 4] {
        for concurrency in [4, 8] {
            row(
                format!("S{shards} P{concurrency}: dedicated 相對 shared"),
                &format!("s{shards}_shared_chunked256_c{concurrency}"),
                &format!("s{shards}_dedicated_chunked256_c{concurrency}"),
            )?;
        }
        for layout in ["shared", "dedicated"] {
            row(
                format!("S{shards} {layout}: P8 相對 P4"),
                &format!("s{shards}_{layout}_chunked256_c4"),
                &format!("s{shards}_{layout}_chunked256_c8"),
            )?;
        }
    }
    for layout in ["shared", "dedicated"] {
        for concurrency in [4, 8] {
            row(
                format!("{layout} P{concurrency}: S4 相對 S2"),
                &format!("s2_{layout}_chunked256_c{concurrency}"),
                &format!("s4_{layout}_chunked256_c{concurrency}"),
            )?;
        }
    }
    report.push('\n');
    Ok(())
}

fn summary_value(record: &TrialRecord, key: &str) -> Result<f64, String> {
    let map = row_map(
        &record.summary_header,
        &record.summary_row,
        Path::new("summary.csv"),
    )?;
    required_f64(&map, key, Path::new("summary.csv"))
}

fn trial_median(records: &[&TrialRecord], key: &str) -> Result<f64, String> {
    Ok(median(
        records
            .iter()
            .map(|record| summary_value(record, key))
            .collect::<Result<Vec<_>, _>>()?,
    ))
}

fn median(mut values: Vec<f64>) -> f64 {
    values.sort_by(f64::total_cmp);
    if values.is_empty() {
        return f64::NAN;
    }
    if values.len() % 2 == 1 {
        values[values.len() / 2]
    } else {
        (values[values.len() / 2 - 1] + values[values.len() / 2]) / 2.0
    }
}

fn format_median_range(
    records: &[&TrialRecord],
    key: &str,
    decimals: usize,
) -> Result<String, String> {
    let values = records
        .iter()
        .map(|record| summary_value(record, key))
        .collect::<Result<Vec<_>, _>>()?;
    let med = median(values.clone());
    let min = values.iter().copied().fold(f64::INFINITY, f64::min);
    let max = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    Ok(format!(
        "{med:.decimals$} [{min:.decimals$}–{max:.decimals$}]"
    ))
}

fn write_derived_csv(path: &Path, data: &(Vec<String>, Vec<Vec<String>>)) -> Result<(), String> {
    let (header, rows) = data;
    if header.is_empty() || header.iter().any(|field| field.trim().is_empty()) {
        return Err(format!(
            "derived {} header must contain nonempty column names",
            path.display()
        ));
    }
    let mut unique_header = BTreeSet::new();
    for field in header {
        if !unique_header.insert(field) {
            return Err(format!(
                "derived {} header contains duplicate column {:?}",
                path.display(),
                field
            ));
        }
    }
    let mut contents = encode_csv_record(header);
    for row in rows {
        if row.len() != header.len() {
            return Err(format!(
                "derived {} row has {} fields, expected {}",
                path.display(),
                row.len(),
                header.len()
            ));
        }
        contents.push_str(&encode_csv_record(row));
    }
    write_derived_text(path, &contents)
}

fn write_derived_text(path: &Path, contents: &str) -> Result<(), String> {
    if contents.len() > 100 * 1024 * 1024 {
        return Err(format!(
            "derived file {} exceeds the 100 MiB artifact limit",
            path.display()
        ));
    }
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("derived");
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let temp = path.with_file_name(format!(".{file_name}.tmp-{}-{nonce}", std::process::id()));
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)
        .map_err(|error| {
            format!(
                "cannot create derived temporary file {}: {error}",
                temp.display()
            )
        })?;
    if let Err(error) = output
        .write_all(contents.as_bytes())
        .and_then(|_| output.flush())
        .and_then(|_| output.sync_all())
    {
        let _ = fs::remove_file(&temp);
        return Err(format!(
            "cannot persist derived file {}: {error}",
            temp.display()
        ));
    }
    fs::rename(&temp, path).map_err(|error| {
        let _ = fs::remove_file(&temp);
        format!("cannot atomically replace {}: {error}", path.display())
    })?;
    if let Some(parent) = path.parent() {
        if let Ok(directory) = fs::File::open(parent) {
            let _ = directory.sync_all();
        }
    }
    Ok(())
}

fn relative_path(from: &Path, target: &Path) -> Result<PathBuf, String> {
    let from = from
        .canonicalize()
        .map_err(|error| format!("cannot resolve link base {}: {error}", from.display()))?;
    let target = target
        .canonicalize()
        .map_err(|error| format!("cannot resolve link target {}: {error}", target.display()))?;
    let from_parts = from.components().collect::<Vec<_>>();
    let target_parts = target.components().collect::<Vec<_>>();
    let common = from_parts
        .iter()
        .zip(&target_parts)
        .take_while(|(left, right)| left == right)
        .count();
    let mut relative = PathBuf::new();
    for _ in common..from_parts.len() {
        relative.push("..");
    }
    for component in &target_parts[common..] {
        if let Component::Normal(value) = component {
            relative.push(value);
        }
    }
    Ok(relative)
}
