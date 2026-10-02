//! Fresh-process Tokio runtime worker scaling runner for the ledger pipeline.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

const SCENARIOS: &[Scenario] = &[
    Scenario::QueueEcho,
    Scenario::ForegroundPersistence,
    Scenario::IntegratedPipeline,
];
const WORKER_COUNTS: &[usize] = &[3, 4, 6, 8];
const REPETITIONS: usize = 3;
const FULL_USERS: usize = 50_000;
const FULL_REQUESTS_PER_USER: usize = 200;
const FULL_REQUESTS: u64 = 10_000_000;
const SAMPLE_STRIDE: u64 = 64;
const DEFAULT_OUTPUT_ROOT: &str = "benches/data/ledger_thread_scaling";
const DEFAULT_PREFLIGHT_OBSERVATION_MS: u64 = 3_000;
const DEFAULT_SMOKE_PREFLIGHT_OBSERVATION_MS: u64 = 100;
const DEFAULT_PREFLIGHT_TIMEOUT_MS: u64 = 60_000;
const MIN_FULL_PREFLIGHT_OBSERVATION_MS: u64 = 3_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum Scenario {
    QueueEcho,
    ForegroundPersistence,
    IntegratedPipeline,
}

impl Scenario {
    fn as_str(self) -> &'static str {
        match self {
            Self::QueueEcho => "queue_echo",
            Self::ForegroundPersistence => "foreground_persistence",
            Self::IntegratedPipeline => "integrated_pipeline",
        }
    }

    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "queue_echo" => Ok(Self::QueueEcho),
            "foreground_persistence" => Ok(Self::ForegroundPersistence),
            "integrated_pipeline" => Ok(Self::IntegratedPipeline),
            _ => Err(format!(
                "invalid scenario {value}; expected queue_echo, foreground_persistence, or integrated_pipeline"
            )),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Trial {
    index: usize,
    scenario: Scenario,
    workers: usize,
    repetition: usize,
}

#[derive(Debug)]
struct Cli {
    smoke: bool,
    scenarios: Vec<Scenario>,
    workers: Vec<usize>,
    repetitions: usize,
    preflight_observation_ms: u64,
    preflight_timeout_ms: u64,
    output_root: PathBuf,
    validate_only: bool,
    internal_trial: bool,
    internal_repetition: Option<usize>,
    trial_output: Option<PathBuf>,
}

impl Default for Cli {
    fn default() -> Self {
        Self {
            smoke: false,
            scenarios: SCENARIOS.to_vec(),
            workers: WORKER_COUNTS.to_vec(),
            repetitions: REPETITIONS,
            preflight_observation_ms: DEFAULT_PREFLIGHT_OBSERVATION_MS,
            preflight_timeout_ms: DEFAULT_PREFLIGHT_TIMEOUT_MS,
            output_root: PathBuf::from(DEFAULT_OUTPUT_ROOT),
            validate_only: false,
            internal_trial: false,
            internal_repetition: None,
            trial_output: None,
        }
    }
}

impl Cli {
    fn parse(args: &[String]) -> Result<Self, String> {
        let mut cli = Self::default();
        let mut scenario_explicit = false;
        let mut workers_explicit = false;
        let mut observation_explicit = false;
        let mut index = 0;
        while index < args.len() {
            let flag = args[index].as_str();
            match flag {
                "--help" | "-h" => {
                    print_help();
                    std::process::exit(0);
                }
                "--smoke" => {
                    cli.smoke = true;
                    index += 1;
                    continue;
                }
                "--validate-only" => {
                    cli.validate_only = true;
                    index += 1;
                    continue;
                }
                "--internal-trial" => {
                    cli.internal_trial = true;
                    index += 1;
                    continue;
                }
                "--bench" => {
                    index += 1;
                    continue;
                }
                _ => {}
            }
            index += 1;
            let value = args
                .get(index)
                .ok_or_else(|| format!("missing value for {flag}"))?;
            match flag {
                "--scenario" => {
                    scenario_explicit = true;
                    cli.scenarios = parse_scenarios(value)?;
                }
                "--threads" => {
                    workers_explicit = true;
                    cli.workers = parse_workers(value)?;
                }
                "--repetitions" => cli.repetitions = parse_num(flag, value)?,
                "--preflight-observation-ms" => {
                    observation_explicit = true;
                    cli.preflight_observation_ms = parse_num(flag, value)?;
                }
                "--preflight-timeout-ms" => cli.preflight_timeout_ms = parse_num(flag, value)?,
                "--output-root" => cli.output_root = PathBuf::from(value),
                "--trial-repetition" => cli.internal_repetition = Some(parse_num(flag, value)?),
                "--trial-output" => cli.trial_output = Some(PathBuf::from(value)),
                _ => return Err(format!("unknown option {flag}; use --help")),
            }
            index += 1;
        }
        if cli.smoke && !observation_explicit {
            cli.preflight_observation_ms = DEFAULT_SMOKE_PREFLIGHT_OBSERVATION_MS;
        }
        if cli.internal_trial {
            if !scenario_explicit || cli.scenarios.len() != 1 {
                return Err("internal trial requires exactly one --scenario".to_owned());
            }
            if !workers_explicit || cli.workers.len() != 1 {
                return Err("internal trial requires exactly one --threads value".to_owned());
            }
            if cli.internal_repetition.is_none() {
                return Err("internal trial requires --trial-repetition".to_owned());
            }
            if cli.trial_output.is_none() {
                return Err("internal trial requires --trial-output".to_owned());
            }
        }
        cli.validate()?;
        Ok(cli)
    }

    fn validate(&self) -> Result<(), String> {
        if self.scenarios.is_empty() {
            return Err("at least one scenario must be selected".to_owned());
        }
        if self.workers.is_empty() {
            return Err("at least one runtime worker count must be selected".to_owned());
        }
        if self
            .workers
            .iter()
            .any(|workers| !WORKER_COUNTS.contains(workers))
        {
            return Err(format!(
                "--threads accepts only {}",
                WORKER_COUNTS
                    .iter()
                    .map(usize::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            ));
        }
        if self.repetitions == 0 || self.repetitions > REPETITIONS {
            return Err(format!("--repetitions must be in 1..={REPETITIONS}"));
        }
        if let Some(repetition) = self.internal_repetition {
            if repetition == 0 || repetition > REPETITIONS {
                return Err(format!("--trial-repetition must be in 1..={REPETITIONS}"));
            }
        }
        if self.preflight_observation_ms == 0
            || self.preflight_timeout_ms == 0
            || self.preflight_observation_ms > self.preflight_timeout_ms
        {
            return Err(
                "preflight observation must be positive and no longer than its timeout".to_owned(),
            );
        }
        if !self.smoke && self.preflight_observation_ms < MIN_FULL_PREFLIGHT_OBSERVATION_MS {
            return Err(format!(
                "full trials require at least {MIN_FULL_PREFLIGHT_OBSERVATION_MS}ms preflight observations"
            ));
        }
        Ok(())
    }
}

pub fn run_from_args() -> Result<(), String> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let cli = Cli::parse(&args)?;
    if cli.internal_trial {
        run_internal_trial(&cli)
    } else if cli.validate_only {
        print_schedule(&build_schedule(&cli));
        Ok(())
    } else {
        run_matrix(cli)
    }
}

fn run_internal_trial(cli: &Cli) -> Result<(), String> {
    let scenario = cli.scenarios[0];
    let workers = cli.workers[0];
    let repetition = cli.internal_repetition.unwrap_or(1);
    let output_dir = cli
        .trial_output
        .as_deref()
        .ok_or_else(|| "internal trial requires an output directory".to_owned())?;
    let case_name = format!("{}_t{workers}_r{repetition}", scenario.as_str());
    match scenario {
        Scenario::QueueEcho => crate::ledger_request_batch::run_thread_scaling_trial(
            &case_name,
            if cli.smoke { 40_000 } else { FULL_REQUESTS },
            if cli.smoke { 200 } else { FULL_USERS },
            if cli.smoke { 200 } else { FULL_USERS },
            if cli.smoke { 1 } else { SAMPLE_STRIDE },
            workers,
            output_dir,
            cli.preflight_observation_ms,
            cli.preflight_timeout_ms,
        ),
        Scenario::ForegroundPersistence | Scenario::IntegratedPipeline => {
            let mut pipeline_args = Vec::new();
            if cli.smoke {
                pipeline_args.push("--smoke".to_owned());
            }
            pipeline_args.extend([
                "--users".to_owned(),
                (if cli.smoke { 200 } else { FULL_USERS }).to_string(),
                "--requests-per-user".to_owned(),
                FULL_REQUESTS_PER_USER.to_string(),
                "--sample-stride".to_owned(),
                (if cli.smoke { 1 } else { SAMPLE_STRIDE }).to_string(),
                "--runtime-workers".to_owned(),
                workers.to_string(),
                "--output-root".to_owned(),
                output_dir.to_string_lossy().into_owned(),
                "--preflight-observation-ms".to_owned(),
                cli.preflight_observation_ms.to_string(),
                "--preflight-timeout-ms".to_owned(),
                cli.preflight_timeout_ms.to_string(),
            ]);
            crate::ledger_pipeline::run_thread_scaling_trial(
                &pipeline_args,
                scenario == Scenario::IntegratedPipeline,
                scenario == Scenario::IntegratedPipeline,
            )
        }
    }
}

fn run_matrix(cli: Cli) -> Result<(), String> {
    let schedule = build_schedule(&cli);
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("system clock is before Unix epoch: {error}"))?
        .as_nanos();
    let output_root = absolute_path(&cli.output_root)?;
    fs::create_dir_all(&output_root).map_err(|error| {
        format!(
            "cannot create output root {}: {error}",
            output_root.display()
        )
    })?;
    let run_dir = output_root.join(format!("run-{run_id}"));
    fs::create_dir(&run_dir)
        .map_err(|error| format!("cannot create run directory {}: {error}", run_dir.display()))?;
    fs::create_dir(run_dir.join("trials"))
        .map_err(|error| format!("cannot create trial output directory: {error}"))?;

    let initial_affinity = allowed_cpu_list()?;
    let topology = cpu_topology(&initial_affinity)?;
    let is_full_matrix = is_full_selection(&cli);
    let run_kind = match (cli.smoke, is_full_matrix) {
        (true, true) => "complete_smoke_matrix",
        (true, false) => "partial_smoke_matrix",
        (false, true) => "complete_full_matrix",
        (false, false) => "partial_full_matrix",
    };
    let metadata_path = run_dir.join("run_metadata.txt");
    let metadata = format!(
        "run_id={run_id}\nrun_kind={run_kind}\nrun_status=running\ntrial_count={}\nsmoke={}\nexpected_requests_per_trial={}\nusers_or_coroutines_per_trial={}\nrequests_per_user={}\nsample_stride={}\nqueue_capacity=50000\nforeground_batch_size=2048\nfirst_dequeue_timeout_ms=5\nprojection_batch_size=256\ngc_batch_size=256\ngc_interval_ms=100\nretention_ms=500\nwatermark_interval_ms=100\npreflight_observation_ms={}\npreflight_timeout_ms={}\npreflight_max_cpu_busy_pct=10\npreflight_max_device_busy_pct=5\npreflight_policy=per-trial setup and pre-measurement gates retain strict CPU<=10%, device busy<=5%, and scenario-specific adequate memory/free-space reserves\ninherited_cpus_allowed_list={}\navailable_logical_cpus={}\ncpu_topology={}\nsmt_enabled={}\ncpu_model={}\nkernel={}\nrustc={}\nrocksdb_note=Tokio worker_threads sets async runtime workers only; Tokio blocking-pool sizing and RocksDB internal background/compaction thread pools remain at their existing defaults.\nio_note=Process and target-device counters are bracketed by each reused benchmark adapter; pipeline snapshots retain their recorded endpoint offsets. No per-file I/O attribution is available.\nmeasurement_note=Each child is a fresh copy of this executable and inherits the parent's CPU affinity unchanged. Queue echo RPS spans earliest individual request start through latest reply; storage RPS spans measurement start immediately before client release through latest route reply. Process CPU uses the corresponding release/start through latest-reply sample endpoint. Setup, seed, preflight, settlement, recovery, and integrity checks are outside client RPS.\n",
        schedule.len(),
        cli.smoke,
        if cli.smoke { 40_000 } else { FULL_REQUESTS },
        if cli.smoke { 200 } else { FULL_USERS },
        FULL_REQUESTS_PER_USER,
        if cli.smoke { 1 } else { SAMPLE_STRIDE },
        cli.preflight_observation_ms,
        cli.preflight_timeout_ms,
        initial_affinity,
        parse_cpu_list(&initial_affinity)?.len(),
        topology.description,
        topology.smt_enabled,
        first_cpu_model().unwrap_or_else(|| "unknown".to_owned()),
        command_version("uname", &["-srmo"]).unwrap_or_else(|| "unknown".to_owned()),
        command_version("rustc", &["--version"]).unwrap_or_else(|| "unknown".to_owned()),
    );
    fs::write(&metadata_path, metadata).map_err(|error| {
        format!(
            "cannot write run metadata {}: {error}",
            metadata_path.display()
        )
    })?;

    let trial_manifest = run_dir.join("trial_manifest.csv");
    fs::write(
        &trial_manifest,
        "trial_index,scenario,runtime_workers,repetition,expected_requests,cpu_affinity,status,exit_code,stdout_log,stderr_log\n",
    )
    .map_err(|error| format!("cannot initialize trial manifest: {error}"))?;
    let summary_path = run_dir.join("ledger_thread_scaling_summary.csv");
    fs::write(&summary_path, SUMMARY_HEADER).map_err(|error| {
        format!(
            "cannot initialize trial summary {}: {error}",
            summary_path.display()
        )
    })?;
    let stages_path = run_dir.join("ledger_thread_scaling_stages.csv");
    fs::write(&stages_path, STAGE_HEADER)
        .map_err(|error| format!("cannot initialize stage summary: {error}"))?;
    let background_path = run_dir.join("ledger_thread_scaling_background_summary.csv");
    fs::write(&background_path, BACKGROUND_HEADER)
        .map_err(|error| format!("cannot initialize background summary: {error}"))?;

    let executable = std::env::current_exe()
        .map_err(|error| format!("cannot locate thread scaling executable: {error}"))?;
    let mut completed_count = 0;
    for trial in schedule.iter().copied() {
        let current_affinity = allowed_cpu_list()?;
        if current_affinity != initial_affinity {
            return finish_failed_run(
                &run_dir,
                &cli,
                &schedule,
                completed_count,
                format!(
                    "CPU affinity changed from {initial_affinity} to {current_affinity} before trial {}",
                    trial.index
                ),
            );
        }
        let trial_dir = run_dir.join("trials").join(format!(
            "trial-{:02}-{}_t{}_r{}",
            trial.index,
            trial.scenario.as_str(),
            trial.workers,
            trial.repetition
        ));
        fs::create_dir(&trial_dir).map_err(|error| {
            format!(
                "cannot create trial directory {}: {error}",
                trial_dir.display()
            )
        })?;
        eprintln!(
            "TRIAL_START {}/{} scenario={} runtime_workers={} repetition={} smoke={}",
            trial.index,
            schedule.len(),
            trial.scenario.as_str(),
            trial.workers,
            trial.repetition,
            cli.smoke
        );
        let output = Command::new(&executable)
            .arg("--internal-trial")
            .arg("--scenario")
            .arg(trial.scenario.as_str())
            .arg("--threads")
            .arg(trial.workers.to_string())
            .arg("--trial-repetition")
            .arg(trial.repetition.to_string())
            .arg("--trial-output")
            .arg(&trial_dir)
            .arg("--preflight-observation-ms")
            .arg(cli.preflight_observation_ms.to_string())
            .arg("--preflight-timeout-ms")
            .arg(cli.preflight_timeout_ms.to_string())
            .args(if cli.smoke { vec!["--smoke"] } else { vec![] })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .map_err(|error| format!("cannot launch trial child: {error}"))?;
        let stdout_log = format!("trial-{:02}.stdout.log", trial.index);
        let stderr_log = format!("trial-{:02}.stderr.log", trial.index);
        let relative_trial_dir = Path::new("trials").join(
            trial_dir
                .file_name()
                .ok_or_else(|| "trial directory has no final path component".to_owned())?,
        );
        let stdout_manifest = relative_trial_dir.join(&stdout_log).display().to_string();
        let stderr_manifest = relative_trial_dir.join(&stderr_log).display().to_string();
        fs::write(trial_dir.join(&stdout_log), &output.stdout)
            .map_err(|error| format!("cannot retain trial stdout: {error}"))?;
        fs::write(trial_dir.join(&stderr_log), &output.stderr)
            .map_err(|error| format!("cannot retain trial stderr: {error}"))?;

        let cleanup_result = remove_owned_database_scratch(&trial_dir);
        if let Err(error) = cleanup_result {
            append_trial_manifest(
                &trial_manifest,
                trial,
                if cli.smoke { 40_000 } else { FULL_REQUESTS },
                &initial_affinity,
                "failed",
                Some(output.status.code().unwrap_or(-1)),
                &stdout_manifest,
                &stderr_manifest,
            )?;
            return finish_failed_run(
                &run_dir,
                &cli,
                &schedule,
                completed_count,
                format!("trial database cleanup failed: {error}"),
            );
        }

        if !output.status.success() {
            append_trial_manifest(
                &trial_manifest,
                trial,
                if cli.smoke { 40_000 } else { FULL_REQUESTS },
                &initial_affinity,
                "failed",
                Some(output.status.code().unwrap_or(-1)),
                &stdout_manifest,
                &stderr_manifest,
            )?;
            return finish_failed_run(
                &run_dir,
                &cli,
                &schedule,
                completed_count,
                format!(
                    "trial {} failed with status {}; see {} and {}",
                    trial.index,
                    output.status,
                    trial_dir.join(&stdout_log).display(),
                    trial_dir.join(&stderr_log).display()
                ),
            );
        }

        let trial_data = match collect_trial_data(&trial_dir, trial, &output.stdout, cli.smoke) {
            Ok(data) => data,
            Err(error) => {
                append_trial_manifest(
                    &trial_manifest,
                    trial,
                    if cli.smoke { 40_000 } else { FULL_REQUESTS },
                    &initial_affinity,
                    "failed",
                    Some(0),
                    &stdout_manifest,
                    &stderr_manifest,
                )?;
                return finish_failed_run(
                    &run_dir,
                    &cli,
                    &schedule,
                    completed_count,
                    format!("trial {} result validation failed: {error}", trial.index),
                );
            }
        };
        append_line(&summary_path, &trial_data.summary_row)?;
        for row in trial_data.stage_rows {
            append_line(&stages_path, &row)?;
        }
        append_line(&background_path, &trial_data.background_row)?;
        append_trial_manifest(
            &trial_manifest,
            trial,
            if cli.smoke { 40_000 } else { FULL_REQUESTS },
            &initial_affinity,
            "complete",
            Some(0),
            &stdout_manifest,
            &stderr_manifest,
        )?;
        completed_count += 1;
        eprintln!(
            "TRIAL_COMPLETE {}/{} scenario={} runtime_workers={} repetition={}",
            trial.index,
            schedule.len(),
            trial.scenario.as_str(),
            trial.workers,
            trial.repetition
        );
    }

    let is_complete_full = !cli.smoke && is_full_matrix && completed_count == 36;
    if let Err(error) = validate_archive(&run_dir, &schedule, completed_count) {
        return finish_failed_run(
            &run_dir,
            &cli,
            &schedule,
            completed_count,
            format!("aggregate archive validation failed: {error}"),
        );
    }
    let report = match generate_report(&run_dir, &cli, completed_count, None) {
        Ok(report) => report,
        Err(error) => {
            return finish_failed_run(
                &run_dir,
                &cli,
                &schedule,
                completed_count,
                format!("report generation failed: {error}"),
            );
        }
    };
    if is_complete_full {
        let canonical_report = PathBuf::from("benches/ledger_thread_scaling_tokio_report.md");
        if let Err(error) = fs::write(&canonical_report, report) {
            return finish_failed_run(
                &run_dir,
                &cli,
                &schedule,
                completed_count,
                format!("cannot update canonical thread scaling report: {error}"),
            );
        }
    }
    update_run_status(
        &metadata_path,
        if is_complete_full {
            "complete_full_matrix"
        } else if cli.smoke && is_full_matrix && completed_count == 36 {
            "complete_smoke_matrix"
        } else {
            "partial_matrix"
        },
    )?;
    println!("THREAD_SCALING_RUN {}", run_dir.display());
    if is_complete_full {
        println!("THREAD_SCALING_REPORT benches/ledger_thread_scaling_tokio_report.md");
    }
    Ok(())
}

fn build_schedule(cli: &Cli) -> Vec<Trial> {
    let mut result = Vec::new();
    let mut index = 0;
    for repetition in 1..=cli.repetitions {
        for rotation in 0..cli.workers.len() {
            let workers = cli.workers[(rotation + repetition - 1) % cli.workers.len()];
            for scenario in SCENARIOS.iter().copied() {
                if cli.scenarios.contains(&scenario) {
                    index += 1;
                    result.push(Trial {
                        index,
                        scenario,
                        workers,
                        repetition,
                    });
                }
            }
        }
    }
    result
}

fn is_full_selection(cli: &Cli) -> bool {
    cli.scenarios.len() == SCENARIOS.len()
        && SCENARIOS
            .iter()
            .all(|scenario| cli.scenarios.contains(scenario))
        && cli.workers.len() == WORKER_COUNTS.len()
        && WORKER_COUNTS
            .iter()
            .all(|worker| cli.workers.contains(worker))
        && cli.repetitions == REPETITIONS
}

fn print_schedule(schedule: &[Trial]) {
    for trial in schedule {
        println!(
            "SCHEDULE index={} scenario={} runtime_workers={} repetition={}",
            trial.index,
            trial.scenario.as_str(),
            trial.workers,
            trial.repetition
        );
    }
    println!("SCHEDULE_COUNT {}", schedule.len());
}

fn parse_scenarios(value: &str) -> Result<Vec<Scenario>, String> {
    if value == "all" {
        return Ok(SCENARIOS.to_vec());
    }
    let scenarios = value
        .split(',')
        .map(Scenario::parse)
        .collect::<Result<Vec<_>, _>>()?;
    if scenarios.is_empty() {
        return Err("at least one scenario must be selected".to_owned());
    }
    if scenarios
        .iter()
        .enumerate()
        .any(|(index, scenario)| scenarios[..index].contains(scenario))
    {
        return Err("--scenario contains a duplicate value".to_owned());
    }
    Ok(scenarios)
}

fn parse_workers(value: &str) -> Result<Vec<usize>, String> {
    if value == "all" {
        return Ok(WORKER_COUNTS.to_vec());
    }
    let workers = value
        .split(',')
        .map(|part| parse_num("--threads", part))
        .collect::<Result<Vec<_>, _>>()?;
    if workers
        .iter()
        .enumerate()
        .any(|(index, worker)| workers[..index].contains(worker))
    {
        return Err("--threads contains a duplicate value".to_owned());
    }
    if workers.iter().any(|worker| !WORKER_COUNTS.contains(worker)) {
        return Err(format!(
            "--threads accepts only {}",
            WORKER_COUNTS
                .iter()
                .map(usize::to_string)
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    Ok(workers)
}

fn parse_num<T: std::str::FromStr>(flag: &str, value: &str) -> Result<T, String>
where
    T::Err: std::fmt::Display,
{
    value
        .parse()
        .map_err(|error| format!("invalid value for {flag}: {error}"))
}

fn print_help() {
    eprintln!(
        "ledger_thread_scaling_tokio options:\n\
         Defaults: 3 scenarios x 4 Tokio worker counts x 3 repetitions (36 child processes).\n\
         --smoke uses 200 users/coroutines x 200 requests and 100ms resource observations.\n\
         --scenario all|queue_echo|foreground_persistence|integrated_pipeline[,..]\n\
         --threads all|3,4,6,8 (only these runtime worker counts are valid)\n\
         --repetitions 1..=3 --output-root PATH --validate-only\n\
         --preflight-observation-ms N --preflight-timeout-ms N\n\
         Full trials enforce 10% maximum CPU busy, 5% maximum target-device busy,\n\
         and at least 3000ms for each resource observation."
    );
}

#[derive(Debug)]
struct CpuTopology {
    description: String,
    smt_enabled: bool,
}

fn allowed_cpu_list() -> Result<String, String> {
    let status = fs::read_to_string("/proc/self/status")
        .map_err(|error| format!("cannot read /proc/self/status: {error}"))?;
    status
        .lines()
        .find_map(|line| line.strip_prefix("Cpus_allowed_list:").map(str::trim))
        .map(str::to_owned)
        .ok_or_else(|| "Cpus_allowed_list is missing from /proc/self/status".to_owned())
}

fn parse_cpu_list(value: &str) -> Result<Vec<usize>, String> {
    let mut cpus = Vec::new();
    for part in value.split(',') {
        if let Some((start, end)) = part.split_once('-') {
            let start = parse_num::<usize>("CPU affinity", start)?;
            let end = parse_num::<usize>("CPU affinity", end)?;
            if start > end {
                return Err(format!("invalid CPU range {part}"));
            }
            cpus.extend(start..=end);
        } else {
            cpus.push(parse_num("CPU affinity", part)?);
        }
    }
    if cpus.is_empty() {
        return Err("CPU affinity list is empty".to_owned());
    }
    cpus.sort_unstable();
    cpus.dedup();
    Ok(cpus)
}

fn cpu_topology(allowed: &str) -> Result<CpuTopology, String> {
    let cpus = parse_cpu_list(allowed)?;
    let mut cores = BTreeMap::<(String, String), Vec<usize>>::new();
    let mut smt_enabled = false;
    for cpu in cpus {
        let root = PathBuf::from(format!("/sys/devices/system/cpu/cpu{cpu}/topology"));
        let package = fs::read_to_string(root.join("physical_package_id"))
            .map_err(|error| format!("cannot read CPU {cpu} package ID: {error}"))?
            .trim()
            .to_owned();
        let core = fs::read_to_string(root.join("core_id"))
            .map_err(|error| format!("cannot read CPU {cpu} core ID: {error}"))?
            .trim()
            .to_owned();
        let siblings = fs::read_to_string(root.join("thread_siblings_list"))
            .map_err(|error| format!("cannot read CPU {cpu} sibling list: {error}"))?;
        if parse_cpu_list(siblings.trim())?.len() > 1 {
            smt_enabled = true;
        }
        cores.entry((package, core)).or_default().push(cpu);
    }
    let description = cores
        .iter()
        .map(|((package, core), cpus)| {
            format!(
                "package{package}:core{core}[{}]",
                cpus.iter()
                    .map(usize::to_string)
                    .collect::<Vec<_>>()
                    .join("/")
            )
        })
        .collect::<Vec<_>>()
        .join(";");
    Ok(CpuTopology {
        description,
        smt_enabled,
    })
}

fn first_cpu_model() -> Option<String> {
    let cpuinfo = fs::read_to_string("/proc/cpuinfo").ok()?;
    cpuinfo.lines().find_map(|line| {
        line.strip_prefix("model name\t:")
            .or_else(|| line.strip_prefix("Hardware\t:"))
            .map(str::trim)
            .map(str::to_owned)
    })
}

fn command_version(command: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(command).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn absolute_path(path: &Path) -> Result<PathBuf, String> {
    if path.is_absolute() {
        Ok(path.to_owned())
    } else {
        std::env::current_dir()
            .map(|cwd| cwd.join(path))
            .map_err(|error| format!("cannot resolve current directory: {error}"))
    }
}

fn remove_owned_database_scratch(trial_dir: &Path) -> Result<(), String> {
    for entry in fs::read_dir(trial_dir)
        .map_err(|error| format!("cannot inspect trial output directory: {error}"))?
    {
        let entry = entry.map_err(|error| format!("cannot inspect trial output entry: {error}"))?;
        if entry
            .file_name()
            .to_string_lossy()
            .starts_with(".ledger-thread-scaling-")
        {
            fs::remove_dir_all(entry.path()).map_err(|error| {
                format!(
                    "cannot remove owned trial database {}: {error}",
                    entry.path().display()
                )
            })?;
        }
        if entry.file_name() == "ledger_pipeline_background.csv" {
            fs::remove_file(entry.path()).map_err(|error| {
                format!("cannot remove intermediate event diagnostics: {error}")
            })?;
        }
    }
    Ok(())
}

const SUMMARY_HEADER: &str = "trial_index,scenario,runtime_workers,repetition,expected_requests,completed_requests,credits,debits,fresh_commits,historical_hits,historical_misses,request_wall_s,rps,process_cpu_s,cpu_wall_s,cpu_cores,cpu_ns_per_request,cpu_sample_after_reply_ns,process_io_sample_offset_us,request_sample_count,request_p50_ns,request_p95_ns,request_p99_ns,peak_rss_bytes,db_bytes,process_rchar_bytes,process_wchar_bytes,process_read_bytes,process_write_bytes,target_device_read_bytes,target_device_write_bytes,target_device_busy_ms,wal_syncs,wal_bytes,writes_with_wal,flush_write_bytes,compact_read_bytes,compact_write_bytes,stall_us,latest_seq_near_client_end,projected_seq_near_client_end,destination_seq_near_client_end,projection_backlog_records_near_client_end,gc_prefix_near_client_end,gc_backlog_records_near_client_end,gc_prefix_after_settle,watermark_updates\n";
const STAGE_HEADER: &str =
    "trial_index,scenario,runtime_workers,repetition,stage,unit,sample_count,p50,p95,p99\n";
const BACKGROUND_HEADER: &str = "trial_index,scenario,runtime_workers,repetition,projection_enabled,gc_enabled,projection_batches,projection_records,gc_steps,gc_scanned_during_clients,gc_deleted_during_clients,gc_backlog_records_near_client_end,latest_seq_near_client_end,projected_seq_near_client_end,destination_seq_near_client_end,projection_backlog_records_near_client_end,gc_prefix_near_client_end,gc_prefix_after_settle,watermark_updates,wal_syncs,wal_bytes,writes_with_wal,flush_write_bytes,compact_read_bytes,compact_write_bytes,stall_us\n";

struct TrialData {
    summary_row: String,
    stage_rows: Vec<String>,
    background_row: String,
}

fn collect_trial_data(
    trial_dir: &Path,
    trial: Trial,
    stdout: &[u8],
    smoke: bool,
) -> Result<TrialData, String> {
    let expected = if smoke { 40_000 } else { FULL_REQUESTS };
    let stdout = String::from_utf8_lossy(stdout);
    let (summary, stages) = match trial.scenario {
        Scenario::QueueEcho => {
            let path = trial_dir.join("queue_echo_summary.csv");
            let (header, row) = read_single_csv_row(&path)?;
            let values = zip_csv(&header, &row)?;
            validate_source_profile(&values, trial)?;
            let completed = required(&values, "completed_count")?
                .parse::<u64>()
                .map_err(|error| format!("invalid completed_count: {error}"))?;
            if completed != expected {
                return Err(format!(
                    "queue echo completed {completed}, expected {expected}"
                ));
            }
            let summary = common_summary_row(trial, expected, &values, None)?;
            let stages = queue_stage_rows(trial, &values)?;
            let background_row = common_background_row(trial, &values, &stdout)?;
            return Ok(TrialData {
                summary_row: summary,
                stage_rows: stages,
                background_row,
            });
        }
        Scenario::ForegroundPersistence | Scenario::IntegratedPipeline => {
            let summary_path = trial_dir.join("ledger_pipeline_summary.csv");
            let (header, row) = read_single_csv_row(&summary_path)?;
            let values = zip_csv(&header, &row)?;
            validate_source_profile(&values, trial)?;
            let completed = required(&values, "requests")?
                .parse::<u64>()
                .map_err(|error| format!("invalid requests: {error}"))?;
            if completed != expected {
                return Err(format!(
                    "ledger pipeline completed {completed}, expected {expected}"
                ));
            }
            let stage_path = trial_dir.join("ledger_pipeline_stages.csv");
            let stage_rows = storage_stage_rows(&stage_path, trial)?;
            let user_tx = csv_file_stage_values(&stage_path, "request.total")?;
            let summary = common_summary_row(trial, expected, &values, Some(&user_tx))?;
            (summary, stage_rows)
        }
    };
    let background_row = common_background_row(
        trial,
        &read_trial_summary_values(trial_dir, trial.scenario)?,
        &stdout,
    )?;
    Ok(TrialData {
        summary_row: summary,
        stage_rows: stages,
        background_row,
    })
}

fn read_trial_summary_values(
    trial_dir: &Path,
    scenario: Scenario,
) -> Result<HashMap<String, String>, String> {
    let file_name = match scenario {
        Scenario::QueueEcho => "queue_echo_summary.csv",
        _ => "ledger_pipeline_summary.csv",
    };
    let (header, row) = read_single_csv_row(&trial_dir.join(file_name))?;
    zip_csv(&header, &row)
}

fn validate_source_profile(values: &HashMap<String, String>, trial: Trial) -> Result<(), String> {
    let workers = required(values, "runtime_workers")?
        .parse::<usize>()
        .map_err(|error| format!("invalid source runtime_workers: {error}"))?;
    if workers != trial.workers {
        return Err(format!(
            "source runtime_workers is {workers}, expected {}",
            trial.workers
        ));
    }
    let expected_case = if trial.scenario == Scenario::QueueEcho {
        format!(
            "{}_t{}_r{}",
            trial.scenario.as_str(),
            trial.workers,
            trial.repetition
        )
    } else {
        trial.scenario.as_str().to_owned()
    };
    let case = required(values, "case")?;
    if case != expected_case {
        return Err(format!(
            "source case name {case:?} does not match {expected_case:?}"
        ));
    }
    if trial.scenario != Scenario::QueueEcho {
        let expected_projection = trial.scenario == Scenario::IntegratedPipeline;
        let expected_gc = expected_projection;
        let projection = required(values, "projection_enabled")?;
        let gc = required(values, "gc_enabled")?;
        if projection != expected_projection.to_string() || gc != expected_gc.to_string() {
            return Err(format!(
                "source background profile projection={projection}, gc={gc}; expected projection={}, gc={}",
                expected_projection, expected_gc
            ));
        }
        if trial.scenario == Scenario::ForegroundPersistence {
            let users = required_u64(values, "users")?;
            let requests = required_u64(values, "requests")?;
            let seed_sequence = users
                .checked_mul(3)
                .ok_or_else(|| "foreground seed sequence overflowed".to_owned())?;
            let latest_sequence = seed_sequence
                .checked_add(requests)
                .ok_or_else(|| "foreground final sequence overflowed".to_owned())?;
            if required_u64(values, "latest_seq_near_client_end")? != latest_sequence
                || required_u64(values, "final_sequence")? != latest_sequence
                || required_u64(values, "projected_seq_near_client_end")? != seed_sequence
                || required_u64(values, "destination_seq_near_client_end")? != seed_sequence
                || required_u64(values, "projection_backlog_records_near_client_end")? != requests
                || required_u64(values, "gc_prefix_near_client_end")? != 0
                || required_u64(values, "gc_prefix_after_settle")? != 0
                || required_u64(values, "watermark_updates")? != 0
                || required_u64(values, "initial_watermark")?
                    != required_u64(values, "final_watermark")?
            {
                return Err(
                    "foreground run advanced projection, watermark, GC, or sequence state unexpectedly"
                        .to_owned(),
                );
            }
        }
    }
    Ok(())
}

fn common_summary_row(
    trial: Trial,
    expected: u64,
    values: &HashMap<String, String>,
    user_tx: Option<&HashMap<String, String>>,
) -> Result<String, String> {
    let queue_echo = trial.scenario == Scenario::QueueEcho;
    let request_wall_key = if queue_echo {
        "request_wall_seconds"
    } else {
        "client_wall_s"
    };
    let rps_key = if queue_echo { "completed_rps" } else { "rps" };
    let cpu_key = if queue_echo {
        "process_cpu_seconds"
    } else {
        "cpu_s"
    };
    let request_wall = required_f64(values, request_wall_key, true)?;
    let rps = required_f64(values, rps_key, true)?;
    let cpu_s = required_f64(values, cpu_key, false)?;
    let cpu_wall = if queue_echo {
        required_f64(values, "cpu_wall_seconds", true)?
    } else {
        request_wall.clone()
    };
    let cpu_cores = required_f64(values, "cpu_core_equivalents", false)?;
    let cpu_sample_offset = if queue_echo {
        required_u64(values, "cpu_sample_after_reply_ns")?.to_string()
    } else {
        required_u64(values, "cpu_sample_offset_us")?
            .saturating_mul(1_000)
            .to_string()
    };
    let io_sample_offset = required_u64(values, "io_sample_offset_us")?.to_string();
    let request_p50 = if queue_echo {
        required_u64(values, "request_latency_p50_ns")?.to_string()
    } else {
        required_u64(
            user_tx.ok_or_else(|| "storage request.total stage is missing".to_owned())?,
            "p50",
        )?
        .to_string()
    };
    let request_p95 = if queue_echo {
        required_u64(values, "request_latency_p95_ns")?.to_string()
    } else {
        required_u64(
            user_tx.ok_or_else(|| "storage request.total stage is missing".to_owned())?,
            "p95",
        )?
        .to_string()
    };
    let request_p99 = if queue_echo {
        required_u64(values, "request_latency_p99_ns")?.to_string()
    } else {
        required_u64(
            user_tx.ok_or_else(|| "storage request.total stage is missing".to_owned())?,
            "p99",
        )?
        .to_string()
    };
    let request_sample_count = if queue_echo {
        required_u64(values, "request_latency_sample_count")?.to_string()
    } else {
        required_u64(
            user_tx.ok_or_else(|| "storage request.total stage is missing".to_owned())?,
            "sample_count",
        )?
        .to_string()
    };
    let cpu_ns_per_request = format!("{:.3}", cpu_s * 1_000_000_000.0 / expected as f64);
    let request_count_key = if queue_echo {
        "completed_count"
    } else {
        "requests"
    };
    let completed = required_u64(values, request_count_key)?;
    if completed != expected {
        return Err(format!(
            "completed {completed} requests, expected {expected}"
        ));
    }
    let mut fields = vec![
        trial.index.to_string(),
        trial.scenario.as_str().to_owned(),
        trial.workers.to_string(),
        trial.repetition.to_string(),
        expected.to_string(),
        completed.to_string(),
        if queue_echo {
            String::new()
        } else {
            required_u64(values, "credit_requests")?.to_string()
        },
        if queue_echo {
            String::new()
        } else {
            required_u64(values, "debit_requests")?.to_string()
        },
        if queue_echo {
            String::new()
        } else {
            required_u64(values, "fresh_commits")?.to_string()
        },
        if queue_echo {
            String::new()
        } else {
            required_u64(values, "historical_hits")?.to_string()
        },
        if queue_echo {
            String::new()
        } else {
            required_u64(values, "historical_misses")?.to_string()
        },
        request_wall.to_string(),
        rps.to_string(),
        cpu_s.to_string(),
        cpu_wall.to_string(),
        cpu_cores.to_string(),
        cpu_ns_per_request,
        cpu_sample_offset,
        io_sample_offset,
        request_sample_count,
        request_p50,
        request_p95,
        request_p99,
        required_u64(values, "peak_rss_bytes")?.to_string(),
        if queue_echo {
            String::new()
        } else {
            required_u64(values, "db_bytes")?.to_string()
        },
        required_u64(
            values,
            if queue_echo {
                "process_rchar_bytes_delta"
            } else {
                "process_rchar_bytes"
            },
        )?
        .to_string(),
        required_u64(
            values,
            if queue_echo {
                "process_wchar_bytes_delta"
            } else {
                "process_wchar_bytes"
            },
        )?
        .to_string(),
        required_u64(
            values,
            if queue_echo {
                "process_read_bytes_delta"
            } else {
                "process_read_bytes"
            },
        )?
        .to_string(),
        required_u64(
            values,
            if queue_echo {
                "process_write_bytes_delta"
            } else {
                "process_write_bytes"
            },
        )?
        .to_string(),
        required_u64(
            values,
            if queue_echo {
                "target_device_read_bytes_delta"
            } else {
                "device_read_bytes"
            },
        )?
        .to_string(),
        required_u64(
            values,
            if queue_echo {
                "target_device_write_bytes_delta"
            } else {
                "device_write_bytes"
            },
        )?
        .to_string(),
        required_u64(
            values,
            if queue_echo {
                "target_device_busy_ms_delta"
            } else {
                "device_busy_ms"
            },
        )?
        .to_string(),
    ];
    for name in [
        "wal_syncs",
        "wal_bytes",
        "writes_with_wal",
        "flush_write_bytes",
        "compact_read_bytes",
        "compact_write_bytes",
        "stall_us",
        "latest_seq_near_client_end",
        "projected_seq_near_client_end",
        "destination_seq_near_client_end",
        "projection_backlog_records_near_client_end",
        "gc_prefix_near_client_end",
        "gc_backlog_records_near_client_end",
        "gc_prefix_after_settle",
        "watermark_updates",
    ] {
        if queue_echo {
            fields.push(String::new());
        } else {
            fields.push(required_u64(values, name)?.to_string());
        }
    }
    Ok(fields.join(","))
}

fn common_background_row(
    trial: Trial,
    values: &HashMap<String, String>,
    stdout: &str,
) -> Result<String, String> {
    let mut background = HashMap::new();
    for line in stdout.lines() {
        if line.starts_with("BACKGROUND ") {
            background.extend(parse_key_values(line));
        }
    }
    let projection_enabled = if trial.scenario == Scenario::QueueEcho {
        "false".to_owned()
    } else {
        required(values, "projection_enabled")?.to_owned()
    };
    let gc_enabled = if trial.scenario == Scenario::QueueEcho {
        "false".to_owned()
    } else {
        required(values, "gc_enabled")?.to_owned()
    };
    let mut fields = vec![
        trial.index.to_string(),
        trial.scenario.as_str().to_owned(),
        trial.workers.to_string(),
        trial.repetition.to_string(),
        projection_enabled,
        gc_enabled,
    ];
    if trial.scenario != Scenario::QueueEcho {
        for name in [
            "projection_batches",
            "projection_records",
            "gc_steps",
            "gc_scanned_during_clients",
            "gc_deleted_during_clients",
            "gc_backlog_records_near_client_end",
        ] {
            if !background.contains_key(name) {
                return Err(format!("pipeline BACKGROUND line is missing {name}"));
            }
        }
        for name in [
            "projection_batches",
            "projection_records",
            "gc_steps",
            "gc_scanned_during_clients",
            "gc_deleted_during_clients",
            "gc_backlog_records_near_client_end",
            "latest_seq_near_client_end",
            "projected_seq_near_client_end",
            "destination_seq_near_client_end",
            "projection_backlog_records_near_client_end",
            "gc_prefix_near_client_end",
            "gc_prefix_after_settle",
            "watermark_updates",
            "wal_syncs",
            "wal_bytes",
            "writes_with_wal",
            "flush_write_bytes",
            "compact_read_bytes",
            "compact_write_bytes",
            "stall_us",
        ] {
            let value = background
                .get(name)
                .or_else(|| values.get(name))
                .ok_or_else(|| format!("pipeline background metric {name} is missing"))?;
            value
                .parse::<u64>()
                .map_err(|error| format!("invalid pipeline background metric {name}: {error}"))?;
        }
        if trial.scenario == Scenario::ForegroundPersistence {
            for name in [
                "projection_batches",
                "projection_records",
                "gc_steps",
                "gc_scanned_during_clients",
                "gc_deleted_during_clients",
            ] {
                let value = required(&background, name)?
                    .parse::<u64>()
                    .map_err(|error| format!("invalid foreground {name}: {error}"))?;
                if value != 0 {
                    return Err(format!(
                        "foreground background metric {name} was {value}, expected zero"
                    ));
                }
            }
        }
    }
    for name in [
        "projection_batches",
        "projection_records",
        "gc_steps",
        "gc_scanned_during_clients",
        "gc_deleted_during_clients",
        "gc_backlog_records_near_client_end",
        "latest_seq_near_client_end",
        "projected_seq_near_client_end",
        "destination_seq_near_client_end",
        "projection_backlog_records_near_client_end",
        "gc_prefix_near_client_end",
        "gc_prefix_after_settle",
        "watermark_updates",
        "wal_syncs",
        "wal_bytes",
        "writes_with_wal",
        "flush_write_bytes",
        "compact_read_bytes",
        "compact_write_bytes",
        "stall_us",
    ] {
        let value = if let Some(item) = background.get(name) {
            item.clone()
        } else {
            values.get(name).cloned().unwrap_or_default()
        };
        fields.push(value);
    }
    Ok(fields.join(","))
}

fn queue_stage_rows(trial: Trial, values: &HashMap<String, String>) -> Result<Vec<String>, String> {
    let mut rows = Vec::new();
    for (name, prefix) in [
        ("request.total", "request_latency"),
        ("request.enqueue", "enqueue_wait"),
        ("request.queue", "queue_wait"),
        ("request.batch", "batch_wait"),
        ("request.handler", "handler_time"),
        ("request.response", "response_wait"),
    ] {
        rows.push(format!(
            "{},{},{},{},{},ns,{},{},{},{}",
            trial.index,
            trial.scenario.as_str(),
            trial.workers,
            trial.repetition,
            name,
            field(values, &format!("{prefix}_sample_count"))?,
            field(values, &format!("{prefix}_p50_ns"))?,
            field(values, &format!("{prefix}_p95_ns"))?,
            field(values, &format!("{prefix}_p99_ns"))?
        ));
    }
    Ok(rows)
}

fn storage_stage_rows(path: &Path, trial: Trial) -> Result<Vec<String>, String> {
    let mut lines = fs::read_to_string(path)
        .map_err(|error| {
            format!(
                "cannot read storage stage summary {}: {error}",
                path.display()
            )
        })?
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if lines.len() < 2 {
        return Err(format!(
            "storage stage summary {} has no rows",
            path.display()
        ));
    }
    let header = parse_csv_line(&lines.remove(0));
    let mut rows = Vec::new();
    for line in lines {
        let values = zip_csv(&header, &parse_csv_line(&line))?;
        let stage = field(&values, "stage")?;
        rows.push(format!(
            "{},{},{},{},{},{},{},{},{},{}",
            trial.index,
            trial.scenario.as_str(),
            trial.workers,
            trial.repetition,
            stage,
            values
                .get("unit")
                .cloned()
                .unwrap_or_else(|| "ns".to_owned()),
            field(&values, "sample_count")?,
            field(&values, "p50")?,
            field(&values, "p95")?,
            field(&values, "p99")?
        ));
    }
    Ok(rows)
}

fn csv_file_stage_values(path: &Path, stage_name: &str) -> Result<HashMap<String, String>, String> {
    let text = fs::read_to_string(path)
        .map_err(|error| format!("cannot read stage summary {}: {error}", path.display()))?;
    let mut lines = text.lines();
    let header = parse_csv_line(
        lines
            .next()
            .ok_or_else(|| "stage CSV header is missing".to_owned())?,
    );
    for line in lines {
        let values = zip_csv(&header, &parse_csv_line(line))?;
        if values.get("stage").is_some_and(|stage| stage == stage_name) {
            return Ok(values);
        }
    }
    Err(format!(
        "stage {stage_name} is missing from {}",
        path.display()
    ))
}

fn read_single_csv_row(path: &Path) -> Result<(Vec<String>, Vec<String>), String> {
    let contents = fs::read_to_string(path)
        .map_err(|error| format!("cannot read trial summary {}: {error}", path.display()))?;
    let mut lines = contents.lines();
    let header = parse_csv_line(
        lines
            .next()
            .ok_or_else(|| "CSV header is missing".to_owned())?,
    );
    let row = parse_csv_line(
        lines
            .next()
            .ok_or_else(|| "CSV result row is missing".to_owned())?,
    );
    if lines.next().is_some() {
        return Err(format!("expected one result row in {}", path.display()));
    }
    Ok((header, row))
}

fn validate_archive(run_dir: &Path, schedule: &[Trial], completed: usize) -> Result<(), String> {
    let expected_trials = schedule.iter().take(completed).copied().collect::<Vec<_>>();
    if expected_trials.len() != completed {
        return Err(format!(
            "completed count {completed} exceeds schedule size {}",
            schedule.len()
        ));
    }
    let expected_by_index = expected_trials
        .iter()
        .map(|trial| (trial.index, trial))
        .collect::<BTreeMap<_, _>>();
    let summary = read_csv_table(&run_dir.join("ledger_thread_scaling_summary.csv"), 47)?;
    let background = read_csv_table(
        &run_dir.join("ledger_thread_scaling_background_summary.csv"),
        26,
    )?;
    let stages = read_csv_table(&run_dir.join("ledger_thread_scaling_stages.csv"), 10)?;
    if summary.rows.len() != completed || background.rows.len() != completed {
        return Err(format!(
            "summary/background row counts are {}/{}; expected {completed}/{completed}",
            summary.rows.len(),
            background.rows.len()
        ));
    }
    let mut summary_indices = HashSet::new();
    for row in &summary.rows {
        let values = zip_csv(&summary.header, row)?;
        let index = required(&values, "trial_index")?
            .parse::<usize>()
            .map_err(|error| format!("invalid summary trial index: {error}"))?;
        if !summary_indices.insert(index) {
            return Err(format!("duplicate summary trial index {index}"));
        }
        validate_trial_identity(&values, index, &expected_by_index, "summary")?;
        required_u64(&values, "completed_requests")?;
    }
    if summary_indices.len() != completed {
        return Err(format!(
            "summary contains {} unique trials; expected {completed}",
            summary_indices.len()
        ));
    }
    let mut background_indices = HashSet::new();
    for row in &background.rows {
        let values = zip_csv(&background.header, row)?;
        let index = required(&values, "trial_index")?
            .parse::<usize>()
            .map_err(|error| format!("invalid background trial index: {error}"))?;
        if !background_indices.insert(index) {
            return Err(format!("duplicate background trial index {index}"));
        }
        validate_trial_identity(&values, index, &expected_by_index, "background")?;
    }
    if background_indices != summary_indices {
        return Err("background trial indices do not match summary indices".to_owned());
    }

    let mut stage_counts = BTreeMap::<usize, usize>::new();
    for row in &stages.rows {
        let values = zip_csv(&stages.header, row)?;
        let index = required(&values, "trial_index")?
            .parse::<usize>()
            .map_err(|error| format!("invalid stage trial index: {error}"))?;
        validate_trial_identity(&values, index, &expected_by_index, "stage")?;
        *stage_counts.entry(index).or_default() += 1;
        required_u64(&values, "sample_count")?;
        required_u64(&values, "p50")?;
        required_u64(&values, "p95")?;
        required_u64(&values, "p99")?;
    }
    let mut expected_stage_rows = 0;
    for trial in &expected_trials {
        let expected_for_trial = if trial.scenario == Scenario::QueueEcho {
            6
        } else {
            25
        };
        expected_stage_rows += expected_for_trial;
        let actual = stage_counts.get(&trial.index).copied().unwrap_or_default();
        if actual != expected_for_trial {
            return Err(format!(
                "trial {} has {actual} stage rows, expected {expected_for_trial}",
                trial.index
            ));
        }
    }
    if stages.rows.len() != expected_stage_rows {
        return Err(format!(
            "stage row count {} differs from expected {expected_stage_rows}",
            stages.rows.len()
        ));
    }

    let mut groups = BTreeMap::<(Scenario, usize), usize>::new();
    for trial in &expected_trials {
        *groups.entry((trial.scenario, trial.workers)).or_default() += 1;
    }
    if schedule.len() == 36 && completed == 36 {
        if summary.rows.len() != 36
            || background.rows.len() != 36
            || stages.rows.len() != 672
            || groups.len() != 12
            || groups.values().any(|count| *count != 3)
        {
            return Err(format!(
                "complete matrix cardinality mismatch: summary={}, stages={}, background={}, settings={}, repetition_counts={:?}",
                summary.rows.len(),
                stages.rows.len(),
                background.rows.len(),
                groups.len(),
                groups.values().collect::<Vec<_>>()
            ));
        }
    }
    Ok(())
}

struct CsvTable {
    header: Vec<String>,
    rows: Vec<Vec<String>>,
}

fn read_csv_table(path: &Path, expected_columns: usize) -> Result<CsvTable, String> {
    let contents = fs::read_to_string(path)
        .map_err(|error| format!("cannot read aggregate CSV {}: {error}", path.display()))?;
    let mut lines = contents.lines();
    let header = parse_csv_line(
        lines
            .next()
            .ok_or_else(|| format!("aggregate CSV {} has no header", path.display()))?,
    );
    if header.len() != expected_columns {
        return Err(format!(
            "aggregate CSV {} has {} header fields; expected {expected_columns}",
            path.display(),
            header.len()
        ));
    }
    let mut rows = Vec::new();
    for (index, line) in lines.enumerate() {
        let row = parse_csv_line(line);
        if row.len() != expected_columns {
            return Err(format!(
                "aggregate CSV {} row {} has {} fields; expected {expected_columns}",
                path.display(),
                index + 2,
                row.len()
            ));
        }
        rows.push(row);
    }
    Ok(CsvTable { header, rows })
}

fn validate_trial_identity(
    values: &HashMap<String, String>,
    index: usize,
    expected: &BTreeMap<usize, &Trial>,
    table: &str,
) -> Result<(), String> {
    let trial = expected
        .get(&index)
        .ok_or_else(|| format!("{table} row refers to unknown trial index {index}"))?;
    let scenario = required(values, "scenario")?;
    let workers = required(values, "runtime_workers")?
        .parse::<usize>()
        .map_err(|error| format!("invalid {table} worker count: {error}"))?;
    let repetition = required(values, "repetition")?
        .parse::<usize>()
        .map_err(|error| format!("invalid {table} repetition: {error}"))?;
    if scenario != trial.scenario.as_str()
        || workers != trial.workers
        || repetition != trial.repetition
    {
        return Err(format!(
            "{table} trial {index} identity is {scenario}/t{workers}/r{repetition}, expected {}/{}/r{}",
            trial.scenario.as_str(),
            trial.workers,
            trial.repetition
        ));
    }
    Ok(())
}

fn zip_csv(header: &[String], row: &[String]) -> Result<HashMap<String, String>, String> {
    if header.len() != row.len() {
        return Err(format!(
            "CSV row has {} fields but header has {}",
            row.len(),
            header.len()
        ));
    }
    Ok(header.iter().cloned().zip(row.iter().cloned()).collect())
}

fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut field = String::new();
    let mut chars = line.chars().peekable();
    let mut quoted = false;
    while let Some(character) = chars.next() {
        match character {
            '"' if quoted && chars.peek() == Some(&'"') => {
                field.push('"');
                chars.next();
            }
            '"' => quoted = !quoted,
            ',' if !quoted => fields.push(std::mem::take(&mut field)),
            _ => field.push(character),
        }
    }
    fields.push(field);
    fields
}

fn required<'a>(values: &'a HashMap<String, String>, name: &str) -> Result<&'a str, String> {
    values
        .get(name)
        .map(String::as_str)
        .ok_or_else(|| format!("required CSV field {name} is missing"))
}

fn required_u64(values: &HashMap<String, String>, name: &str) -> Result<u64, String> {
    required(values, name)?
        .parse::<u64>()
        .map_err(|error| format!("invalid numeric CSV field {name}: {error}"))
}

fn required_f64(
    values: &HashMap<String, String>,
    name: &str,
    positive: bool,
) -> Result<f64, String> {
    let value = required(values, name)?
        .parse::<f64>()
        .map_err(|error| format!("invalid numeric CSV field {name}: {error}"))?;
    if !value.is_finite() || (positive && value <= 0.0) || (!positive && value < 0.0) {
        return Err(format!("numeric CSV field {name} is out of range: {value}"));
    }
    Ok(value)
}

fn field(values: &HashMap<String, String>, name: &str) -> Result<String, String> {
    required(values, name).map(str::to_owned)
}

fn parse_key_values(line: &str) -> HashMap<String, String> {
    line.split_whitespace()
        .skip(1)
        .filter_map(|pair| pair.split_once('='))
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .collect()
}

fn append_trial_manifest(
    path: &Path,
    trial: Trial,
    expected: u64,
    affinity: &str,
    status: &str,
    exit_code: Option<i32>,
    stdout_log: &str,
    stderr_log: &str,
) -> Result<(), String> {
    let row = format!(
        "{},{},{},{},{},{},{},{},{},{}",
        trial.index,
        trial.scenario.as_str(),
        trial.workers,
        trial.repetition,
        expected,
        affinity,
        status,
        exit_code.map(|code| code.to_string()).unwrap_or_default(),
        stdout_log,
        stderr_log
    );
    append_line(path, &row)
}

fn append_line(path: &Path, line: &str) -> Result<(), String> {
    let mut file = OpenOptions::new()
        .append(true)
        .open(path)
        .map_err(|error| format!("cannot append {}: {error}", path.display()))?;
    writeln!(file, "{line}")
        .map_err(|error| format!("cannot append {}: {error}", path.display()))?;
    file.flush()
        .map_err(|error| format!("cannot flush {}: {error}", path.display()))
}

fn update_run_status(path: &Path, status: &str) -> Result<(), String> {
    let mut content = fs::read_to_string(path)
        .map_err(|error| format!("cannot read run metadata {}: {error}", path.display()))?;
    if let Some(line) = content.lines().find(|line| line.starts_with("run_status=")) {
        content = content.replace(line, &format!("run_status={status}"));
    }
    fs::write(path, content)
        .map_err(|error| format!("cannot update run metadata {}: {error}", path.display()))
}

fn finish_failed_run<T>(
    run_dir: &Path,
    cli: &Cli,
    _schedule: &[Trial],
    completed: usize,
    error: String,
) -> Result<T, String> {
    let metadata = run_dir.join("run_metadata.txt");
    let _ = update_run_status(&metadata, "failed_partial_matrix");
    let _ = generate_report(run_dir, cli, completed, Some(&error));
    Err(format!(
        "{error}; partial run archived at {}",
        run_dir.display()
    ))
}

fn generate_report(
    run_dir: &Path,
    cli: &Cli,
    completed: usize,
    failure: Option<&str>,
) -> Result<String, String> {
    let summary_path = run_dir.join("ledger_thread_scaling_summary.csv");
    let summary_text = fs::read_to_string(&summary_path)
        .map_err(|error| format!("cannot read aggregate summary: {error}"))?;
    let mut lines = summary_text.lines();
    let header = parse_csv_line(
        lines
            .next()
            .ok_or_else(|| "aggregate header missing".to_owned())?,
    );
    let mut observations = Vec::<HashMap<String, String>>::new();
    for line in lines {
        observations.push(zip_csv(&header, &parse_csv_line(line))?);
    }
    let mut groups = BTreeMap::<(String, usize), Vec<&HashMap<String, String>>>::new();
    for row in &observations {
        let scenario = required(row, "scenario")?.to_owned();
        let workers = required(row, "runtime_workers")?
            .parse::<usize>()
            .map_err(|error| format!("invalid aggregate worker count: {error}"))?;
        groups.entry((scenario, workers)).or_default().push(row);
    }
    let complete_full =
        !cli.smoke && is_full_selection(cli) && completed == 36 && failure.is_none();
    let run_kind = if complete_full {
        "Complete full matrix"
    } else if cli.smoke && is_full_selection(cli) && completed == 36 && failure.is_none() {
        "Complete smoke matrix"
    } else if failure.is_some() {
        "Failed partial matrix"
    } else {
        "Partial matrix"
    };
    let mut report = format!(
        "# Tokio runtime thread scaling benchmark\n\nRun kind: **{run_kind}**. Completed trials: {completed}/{}.\n\n",
        build_schedule(cli).len()
    );
    if let Some(error) = failure {
        report.push_str(&format!("Run error: `{error}`.\n\n"));
    }
    if cli.smoke {
        report.push_str(&format!(
            "Workload for this run: **smoke**, 200 users/coroutines x 200 requests (40,000 requests per trial), sample stride 1, with {}ms resource observations. These rows are smoke output and do not represent the 10M-request full profile.\n\n",
            cli.preflight_observation_ms
        ));
    } else {
        report.push_str(&format!(
            "Workload for this run: **full**, 50,000 users/coroutines x 200 requests (10,000,000 requests per trial), sample stride 64, with {}ms resource observations.\n\n",
            cli.preflight_observation_ms
        ));
    }
    report.push_str("## Throughput by scenario and worker count\n\n");
    report.push_str("| Scenario | Workers | RPS median | RPS min | RPS max | Repetition observations (1, 2, 3) |\n|---|---:|---:|---:|---:|---|\n");
    for ((scenario, workers), rows) in &groups {
        let mut rps = rows
            .iter()
            .filter_map(|row| row.get("rps")?.parse::<f64>().ok())
            .collect::<Vec<_>>();
        let median = median(&mut rps);
        let min = rps.iter().copied().reduce(f64::min).unwrap_or_default();
        let max = rps.iter().copied().reduce(f64::max).unwrap_or_default();
        let by_rep = repetition_values(rows, "rps");
        report.push_str(&format!(
            "| {scenario} | {workers} | {median:.3} | {min:.3} | {max:.3} | {} |\n",
            by_rep.join(", ")
        ));
    }
    report.push_str("\nPercentiles are trial-level nearest-rank samples. This report lists trial percentiles below and does not treat their median as a pooled percentile.\n\n");
    report.push_str("## Request latency observations\n\n");
    report.push_str("| Scenario | Workers | p50 observations (ns) | p95 observations (ns) | p99 observations (ns) |\n|---|---:|---|---|---|\n");
    for ((scenario, workers), rows) in &groups {
        report.push_str(&format!(
            "| {scenario} | {workers} | {} | {} | {} |\n",
            repetition_values(rows, "request_p50_ns").join(", "),
            repetition_values(rows, "request_p95_ns").join(", "),
            repetition_values(rows, "request_p99_ns").join(", ")
        ));
    }
    report.push_str("\n## Per-trial observations\n\n");
    report.push_str("| Trial | Scenario | Workers | Rep | Requests | RPS | Process CPU cores | CPU ns/request | p50 ns | p95 ns | p99 ns |\n|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
    for row in &observations {
        report.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            required(row, "trial_index")?,
            required(row, "scenario")?,
            required(row, "runtime_workers")?,
            required(row, "repetition")?,
            required(row, "completed_requests")?,
            required(row, "rps")?,
            required(row, "cpu_cores")?,
            required(row, "cpu_ns_per_request")?,
            required(row, "request_p50_ns")?,
            required(row, "request_p95_ns")?,
            required(row, "request_p99_ns")?
        ));
    }
    report.push_str("\n## Method and limits\n\n");
    report.push_str("The full profile uses 50,000 request coroutines/accounts and exactly 10,000,000 requests per trial, with one outstanding request per coroutine, a 50,000-entry bounded queue, batch size 2,048, a 5 ms first-dequeue timeout, and deterministic 50/50 amount-1 credits and debits for storage trials. Foreground persistence uses PerBatch balances and seeds 150,000 history records at balance 100; it leaves projector, watermark manager, and GC unspawned during measurement and does not perform final projection catch-up. Its unprojected record backlog after client completion is expected by design. Integrated pipeline uses projection batches of 256, GC batches of 256, 100 ms GC/watermark intervals, 500 ms retention, and a bounded GC worker that yields during catch-up and sleeps while blocked. GC backlog includes records newer than or otherwise ineligible for its durable retention prefix.\n\n");
    report.push_str("Each trial runs in a fresh child process and Tokio runtime. Thread order rotates by repetition; actual order is in `trial_manifest.csv`. All children inherit the same CPU affinity. Full preflights require CPU busy at or below 10%, target-device busy at or below 5%, plus workload-specific memory and free-space reserves. The runtime worker count controls Tokio async workers; Tokio blocking-pool and RocksDB internal threads keep their existing defaults.\n\n");
    report.push_str("Process CPU is measured to the latest reply endpoint in each scenario. Queue echo samples process CPU immediately after a coroutine's final reply and records `cpu_sample_after_reply_ns`; storage trials retain the source benchmark's recorded CPU sample offset. This adds one process CPU clock sample after each coroutine's last reply. RPS interval boundaries differ slightly: queue echo spans the earliest individual request start through latest response observation, while storage spans the pre-release measurement start through latest route reply. Request latency is sampled at stride 64 in full trials and 1 in smoke runs. Queue echo process and device I/O counters are sampled after worker join and their delay after the latest reply is recorded in `io_sample_offset_us`; pipeline snapshots retain the original `io_sample_offset_us` and storage snapshot offsets. Process and device counters for these scopes include any tail work in the recorded delay. RocksDB read timing includes range-read decoding; GC sync-write timing covers synchronous WAL batch write and is not an fsync-only measure. Per-file I/O attribution is unsupported.\n\n");
    report.push_str("Setup, seed preparation, preflight, settlement, final projection/GC work, recovery, and integrity validation are excluded from client throughput. The aggregate CSV preserves every trial's p50/p95/p99; medians across repetitions are descriptive and are not pooled percentiles or significance claims.\n\n");
    report.push_str("## Files\n\n");
    report.push_str("`ledger_thread_scaling_summary.csv` contains one row per completed trial; `ledger_thread_scaling_stages.csv` contains request and background stage percentile summaries; `ledger_thread_scaling_background_summary.csv` contains projector/GC progress and RocksDB background counters; `trial_manifest.csv`, `run_metadata.txt`, and each trial's stdout/stderr retain execution evidence. Full per-event pipeline diagnostics are removed after extracting these summaries.\n\n");
    report.push_str("## Reproduction\n\n```sh\nBINDGEN_EXTRA_CLANG_ARGS='-I/usr/lib/gcc/x86_64-linux-gnu/13/include' cargo bench --locked --bench ledger_thread_scaling_tokio\n```

Smoke validation runs the same 36 settings with 200 users/coroutines x 200 requests and 100ms resource observations:

```sh
BINDGEN_EXTRA_CLANG_ARGS='-I/usr/lib/gcc/x86_64-linux-gnu/13/include' cargo bench --locked --bench ledger_thread_scaling_tokio -- --smoke
```

" );
    let report_path = run_dir.join("ledger_thread_scaling_report.md");
    fs::write(&report_path, &report)
        .map_err(|error| format!("cannot write run report {}: {error}", report_path.display()))?;
    Ok(report)
}

fn median(values: &mut [f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.sort_by(f64::total_cmp);
    let middle = values.len() / 2;
    if values.len() % 2 == 0 {
        (values[middle - 1] + values[middle]) / 2.0
    } else {
        values[middle]
    }
}

fn repetition_values(rows: &[&HashMap<String, String>], name: &str) -> Vec<String> {
    let mut rows = rows.to_vec();
    rows.sort_by_key(|row| {
        row.get("repetition")
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(usize::MAX)
    });
    rows.iter()
        .map(|row| row.get(name).cloned().unwrap_or_default())
        .collect()
}
