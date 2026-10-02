//! Fresh-process Tokio transaction-index lookup comparison.

use crate::ledger_account_store::IndexLookupMode;
use crate::ledger_preflight::PreflightReport;
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

const DEFAULT_ARCHIVE_ROOT: &str = "benches/data/ledger_index_lookup";
const DEFAULT_SMOKE_ROOT: &str = "target/ledger-index-lookup-smoke";
const DEFAULT_INTEGRATED_ARCHIVE_ROOT: &str = "benches/data/ledger_pipeline_index_lookup";
const DEFAULT_INTEGRATED_SMOKE_ROOT: &str = "target/ledger-pipeline-index-lookup-smoke";
const DEFAULT_FOREGROUND_REPORT: &str = "benches/ledger_index_lookup_tokio_report.md";
const DEFAULT_INTEGRATED_REPORT: &str = "benches/ledger_pipeline_index_lookup_tokio_report.md";
const FULL_REPETITIONS: &[usize] = &[1, 2, 3];
const FULL_USERS: usize = 50_000;
const SMOKE_USERS: usize = 200;
const REQUESTS_PER_USER: usize = 200;
const SUMMARY_PREFIX: &str =
    "trial_index,repetition,partial_designation,trial_directory,stdout_log,stderr_log,";
const STAGE_PREFIX: &str = "trial_index,repetition,mode,";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ModeChoice {
    name: &'static str,
    mode: IndexLookupMode,
}

const MODES: [ModeChoice; 6] = [
    ModeChoice {
        name: "point_get",
        mode: IndexLookupMode::PointGet,
    },
    ModeChoice {
        name: "whole_batch_multiget",
        mode: IndexLookupMode::WholeBatchMultiGet,
    },
    ModeChoice {
        name: "chunked_256_p1",
        mode: IndexLookupMode::Chunked {
            group_size: 256,
            max_in_flight: 1,
        },
    },
    ModeChoice {
        name: "chunked_256_p2",
        mode: IndexLookupMode::Chunked {
            group_size: 256,
            max_in_flight: 2,
        },
    },
    ModeChoice {
        name: "chunked_256_p4",
        mode: IndexLookupMode::Chunked {
            group_size: 256,
            max_in_flight: 4,
        },
    },
    ModeChoice {
        name: "chunked_256_p8",
        mode: IndexLookupMode::Chunked {
            group_size: 256,
            max_in_flight: 8,
        },
    },
];

const INTEGRATED_MODES: [ModeChoice; 3] = [MODES[0], MODES[4], MODES[5]];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Profile {
    ForegroundPersistence,
    IntegratedPipeline,
}

impl Profile {
    fn name(self) -> &'static str {
        match self {
            Self::ForegroundPersistence => "foreground_persistence",
            Self::IntegratedPipeline => "integrated_pipeline",
        }
    }

    fn archive_root(self) -> &'static str {
        match self {
            Self::ForegroundPersistence => DEFAULT_ARCHIVE_ROOT,
            Self::IntegratedPipeline => DEFAULT_INTEGRATED_ARCHIVE_ROOT,
        }
    }

    fn smoke_root(self) -> &'static str {
        match self {
            Self::ForegroundPersistence => DEFAULT_SMOKE_ROOT,
            Self::IntegratedPipeline => DEFAULT_INTEGRATED_SMOKE_ROOT,
        }
    }

    fn modes(self) -> &'static [ModeChoice] {
        match self {
            Self::ForegroundPersistence => &MODES,
            Self::IntegratedPipeline => &INTEGRATED_MODES,
        }
    }

    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "foreground_persistence" => Ok(Self::ForegroundPersistence),
            "integrated_pipeline" => Ok(Self::IntegratedPipeline),
            _ => Err(format!(
                "invalid profile {value}; expected foreground_persistence or integrated_pipeline"
            )),
        }
    }
}

#[derive(Debug)]
struct Cli {
    profile: Profile,
    profile_explicit: bool,
    smoke: bool,
    validate_only: bool,
    internal_trial: bool,
    report_only: Option<PathBuf>,
    modes: Vec<ModeChoice>,
    repetitions: Vec<usize>,
    output_root: PathBuf,
    output_root_explicit: bool,
    trial_mode: Option<ModeChoice>,
    trial_output: Option<PathBuf>,
}

impl Default for Cli {
    fn default() -> Self {
        Self {
            profile: Profile::ForegroundPersistence,
            profile_explicit: false,
            smoke: false,
            validate_only: false,
            internal_trial: false,
            report_only: None,
            modes: MODES.to_vec(),
            repetitions: FULL_REPETITIONS.to_vec(),
            output_root: PathBuf::from(DEFAULT_ARCHIVE_ROOT),
            output_root_explicit: false,
            trial_mode: None,
            trial_output: None,
        }
    }
}

impl Cli {
    fn parse(args: &[String]) -> Result<Self, String> {
        let mut cli = Self::default();
        let mut modes_explicit = false;
        let mut repetitions_explicit = false;
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
                "--profile" => {
                    cli.profile_explicit = true;
                    cli.profile = Profile::parse(value)?;
                }
                "--modes" => {
                    modes_explicit = true;
                    cli.modes = parse_modes(value)?;
                }
                "--repetitions" => {
                    repetitions_explicit = true;
                    cli.repetitions = parse_repetitions(value)?;
                }
                "--output-root" => {
                    cli.output_root_explicit = true;
                    cli.output_root = PathBuf::from(value);
                }
                "--report-only" => {
                    if cli.report_only.replace(PathBuf::from(value)).is_some() {
                        return Err("--report-only may be specified only once".to_owned());
                    }
                }
                "--trial-mode" => cli.trial_mode = Some(parse_mode(value)?),
                "--trial-output" => cli.trial_output = Some(PathBuf::from(value)),
                _ => return Err(format!("unknown option {flag}; use --help")),
            }
            index += 1;
        }
        if cli.smoke && !cli.output_root_explicit {
            cli.output_root = PathBuf::from(cli.profile.smoke_root());
        } else if !cli.output_root_explicit {
            cli.output_root = PathBuf::from(cli.profile.archive_root());
        }
        if !modes_explicit {
            cli.modes = cli.profile.modes().to_vec();
        }
        if cli.internal_trial {
            if cli.trial_mode.is_none() || cli.trial_output.is_none() {
                return Err("internal trial requires --trial-mode and --trial-output".to_owned());
            }
            if modes_explicit || repetitions_explicit || cli.validate_only {
                return Err(
                    "internal trial cannot select a matrix or use --validate-only".to_owned(),
                );
            }
        } else if cli.trial_mode.is_some() || cli.trial_output.is_some() {
            return Err("--trial-mode and --trial-output require --internal-trial".to_owned());
        }
        if cli.report_only.is_some() {
            if !cli.profile_explicit {
                return Err("--report-only requires an explicit --profile".to_owned());
            }
            if cli.smoke
                || cli.validate_only
                || cli.internal_trial
                || modes_explicit
                || repetitions_explicit
                || cli.output_root_explicit
                || cli.trial_mode.is_some()
                || cli.trial_output.is_some()
            {
                return Err(
                    "--report-only cannot be combined with smoke, matrix selections, internal-trial, validate-only, or output-root overrides".to_owned(),
                );
            }
        }
        cli.validate()?;
        Ok(cli)
    }

    fn validate(&self) -> Result<(), String> {
        if self.modes.is_empty() || self.repetitions.is_empty() {
            return Err("at least one mode and repetition must be selected".to_owned());
        }
        if self.smoke && self.output_root_explicit && !path_is_under_target(&self.output_root)? {
            return Err("smoke output must be under target/".to_owned());
        }
        if self
            .modes
            .iter()
            .any(|mode| !self.profile.modes().contains(mode))
        {
            let supported = self
                .profile
                .modes()
                .iter()
                .map(|mode| mode.name)
                .collect::<Vec<_>>()
                .join(",");
            return Err(format!(
                "profile {} supports only these modes: {supported}",
                self.profile.name()
            ));
        }
        Ok(())
    }

    fn is_partial(&self) -> bool {
        self.modes.len() != self.profile.modes().len()
            || self.repetitions.len() != FULL_REPETITIONS.len()
    }
}

#[derive(Clone, Copy)]
struct Trial {
    index: usize,
    mode: ModeChoice,
    repetition: usize,
}

#[derive(Debug)]
struct HostMetadata {
    cpu_model: String,
    logical_cpus: String,
    sockets: String,
    cores_per_socket: String,
    threads_per_core: String,
    smt_enabled: String,
    cpus_allowed_list: String,
    filesystem_source: String,
    filesystem_type: String,
    rustc_version: String,
    tokio_version: String,
    rocksdb_version: String,
}

pub fn run_from_args() -> Result<(), String> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let cli = Cli::parse(&args)?;
    if let Some(run_dir) = cli.report_only.as_deref() {
        recover_canonical_report(&cli, run_dir)
    } else if cli.internal_trial {
        run_internal_trial(&cli)
    } else if cli.validate_only {
        print_schedule(&cli);
        Ok(())
    } else {
        run_matrix(cli)
    }
}

fn recover_canonical_report(cli: &Cli, requested_run_dir: &Path) -> Result<(), String> {
    let run_dir = canonical_report_archive_dir(cli.profile, requested_run_dir)?;
    let metadata_path = run_dir.join("run_metadata.txt");
    let metadata = read_key_value_metadata(&metadata_path)?;
    let run_id = metadata
        .get("run_id")
        .ok_or_else(|| format!("{} is missing run_id", metadata_path.display()))?
        .parse::<u128>()
        .map_err(|error| format!("{} has invalid run_id: {error}", metadata_path.display()))?;
    let directory_name = run_dir
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| "report-only archive directory has no valid name".to_owned())?;
    if directory_name != format!("run-{run_id}") {
        return Err(format!(
            "archive directory {directory_name} does not match metadata run_id {run_id}"
        ));
    }

    let schedule = build_schedule(cli);
    let expected_requests = (FULL_USERS * REQUESTS_PER_USER) as u64;
    let metadata_matches = required(&metadata, "profile", &metadata_path)? == cli.profile.name()
        && required(&metadata, "run_kind", &metadata_path)? == "complete_full_matrix"
        && matches!(
            required(&metadata, "run_status", &metadata_path)?,
            "complete" | "failed"
        )
        && required(&metadata, "profile_modes", &metadata_path)?
            == cli
                .profile
                .modes()
                .iter()
                .map(|mode| mode.name)
                .collect::<Vec<_>>()
                .join(",")
        && required_u64(&metadata, "trial_count", &metadata_path)? == schedule.len() as u64
        && required(&metadata, "smoke", &metadata_path)? == "false"
        && required(&metadata, "partial_designation", &metadata_path)? == "false"
        && required_u64(&metadata, "users_or_coroutines_per_trial", &metadata_path)?
            == FULL_USERS as u64
        && required_u64(&metadata, "requests_per_user", &metadata_path)?
            == REQUESTS_PER_USER as u64
        && required_u64(&metadata, "requests_per_trial", &metadata_path)? == expected_requests
        && required_u64(&metadata, "sample_stride", &metadata_path)? == 64;
    if !metadata_matches {
        return Err(format!(
            "{} does not describe a complete full {} matrix",
            metadata_path.display(),
            cli.profile.name()
        ));
    }

    let trials_dir = run_dir.join("trials");
    let canonical_trials_dir = fs::canonicalize(&trials_dir)
        .map_err(|error| format!("cannot resolve {}: {error}", trials_dir.display()))?;
    if canonical_trials_dir.parent() != Some(run_dir.as_path()) {
        return Err(format!(
            "trial directory {} escapes the archive run directory",
            trials_dir.display()
        ));
    }
    for trial in schedule.iter().copied() {
        let trial_name = format!(
            "trial-{:02}-{}_r{}",
            trial.index, trial.mode.name, trial.repetition
        );
        let trial_dir = trials_dir.join(&trial_name);
        let canonical_trial_dir = fs::canonicalize(&trial_dir)
            .map_err(|error| format!("cannot resolve {}: {error}", trial_dir.display()))?;
        if canonical_trial_dir.parent() != Some(canonical_trials_dir.as_path()) {
            return Err(format!(
                "trial artifact directory {} escapes the archive trials directory",
                trial_dir.display()
            ));
        }
        validate_trial_artifacts(
            &canonical_trial_dir,
            trial,
            FULL_USERS,
            expected_requests,
            64,
            cli.profile,
        )?;
        for log_name in [
            format!("trial-{:02}.stdout.log", trial.index),
            format!("trial-{:02}.stderr.log", trial.index),
        ] {
            if !canonical_trial_dir.join(&log_name).is_file() {
                return Err(format!(
                    "trial {} is missing manifest log {}",
                    trial.index, log_name
                ));
            }
        }
    }

    let summary_path = run_dir.join("ledger_index_lookup_summary.csv");
    let stages_path = run_dir.join("ledger_index_lookup_stages.csv");
    let manifest_path = run_dir.join("trial_manifest.csv");
    validate_completed_matrix(&summary_path, &stages_path, &manifest_path, &schedule, cli)?;

    write_canonical_report(&run_dir, run_id, cli.profile)?;
    let canonical_report = canonical_report_path(cli.profile)?;
    let original_status = required(&metadata, "run_status", &metadata_path)?;
    let original_failure_details =
        "original_runner_exit_code=not_recorded_in_run_metadata\noriginal_report_error=not_recorded_in_run_metadata\n";
    let recovery_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("system clock is before the Unix epoch: {error}"))?
        .as_secs();
    let recovery_command = format!(
        "ledger_index_lookup_tokio --profile {} --report-only {}",
        cli.profile.name(),
        requested_run_dir.display()
    );
    let recovery_note = format!(
        "recovery_status=complete\nprofile={}\nrun_id={run_id}\nvalidated_trials={}/{}\nvalidated_requests={}\naggregate_matrix_validation=passed\nper_trial_artifact_validation=passed\nmanifest_success={}/{}\noriginal_run_metadata_status={original_status}\noriginal_run_metadata_retained=true\n{original_failure_details}recovery_command={recovery_command}\nrecovery_time_unix_seconds={recovery_time}\ncanonical_report={}\nreport_generation=success\noriginal_runner_command_status=unchanged; report recovery does not mark the original runner successful\n",
        cli.profile.name(),
        schedule.len(),
        schedule.len(),
        expected_requests * schedule.len() as u64,
        schedule.len(),
        schedule.len(),
        canonical_report.display()
    );
    fs::write(run_dir.join("report_recovery.txt"), recovery_note).map_err(|error| {
        format!("canonical report was written, but recovery note could not be stored: {error}")
    })?;
    append_report_recovery_status(&canonical_report, &run_dir, original_status, schedule.len())?;
    println!(
        "REPORT_RECOVERED profile={} run_id={} trials={} report={}",
        cli.profile.name(),
        run_id,
        schedule.len(),
        canonical_report.display()
    );
    Ok(())
}

fn append_report_recovery_status(
    report_path: &Path,
    run_dir: &Path,
    original_status: &str,
    validated_trials: usize,
) -> Result<(), String> {
    let cwd = std::env::current_dir().map_err(|error| format!("cannot read cwd: {error}"))?;
    let benches = fs::canonicalize(cwd.join("benches"))
        .map_err(|error| format!("cannot resolve benches/ for recovery links: {error}"))?;
    let relative_run = run_dir
        .strip_prefix(&benches)
        .map_err(|_| "report-only archive is outside benches/".to_owned())?
        .to_string_lossy();
    let mut section = format!(
        "\n## Report recovery status\n\nThe original `run_metadata.txt` remains unchanged with `run_status={original_status}`. Report-only revalidated the complete aggregate matrix and all {validated_trials} trial artifacts, then regenerated this canonical report. The original runner status is unchanged.\n\n- [report recovery record]({relative_run}/report_recovery.txt)\n"
    );
    if run_dir.join("report_incident.txt").is_file() {
        section.push_str(&format!(
            "- [original report-generation incident]({relative_run}/report_incident.txt)\n"
        ));
    }
    let mut report = OpenOptions::new()
        .append(true)
        .open(report_path)
        .map_err(|error| {
            format!(
                "cannot append recovery status to {}: {error}",
                report_path.display()
            )
        })?;
    report.write_all(section.as_bytes()).map_err(|error| {
        format!(
            "cannot append recovery status to {}: {error}",
            report_path.display()
        )
    })
}

fn canonical_report_archive_dir(profile: Profile, requested: &Path) -> Result<PathBuf, String> {
    if requested
        .components()
        .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return Err("--report-only path cannot contain parent-directory traversal".to_owned());
    }
    let archive_root = fs::canonicalize(profile.archive_root()).map_err(|error| {
        format!(
            "cannot resolve default {} archive root {}: {error}",
            profile.name(),
            profile.archive_root()
        )
    })?;
    let requested_absolute = absolute_path(requested)?;
    let run_dir = fs::canonicalize(&requested_absolute).map_err(|error| {
        format!(
            "cannot resolve report-only archive {}: {error}",
            requested_absolute.display()
        )
    })?;
    if !run_dir.is_dir() || run_dir.parent() != Some(archive_root.as_path()) {
        return Err(format!(
            "--report-only must name a direct existing run directory under the default {} archive root {}",
            profile.name(),
            archive_root.display()
        ));
    }
    Ok(run_dir)
}

fn canonical_report_path(profile: Profile) -> Result<PathBuf, String> {
    let cwd = std::env::current_dir().map_err(|error| format!("cannot read cwd: {error}"))?;
    Ok(cwd.join(match profile {
        Profile::ForegroundPersistence => DEFAULT_FOREGROUND_REPORT,
        Profile::IntegratedPipeline => DEFAULT_INTEGRATED_REPORT,
    }))
}

fn run_internal_trial(cli: &Cli) -> Result<(), String> {
    let mode = cli
        .trial_mode
        .ok_or_else(|| "internal trial has no selected mode".to_owned())?;
    let output = cli
        .trial_output
        .as_deref()
        .ok_or_else(|| "internal trial has no output directory".to_owned())?;
    let mut args = vec![
        "--output-root".to_owned(),
        output.to_string_lossy().into_owned(),
    ];
    if cli.smoke {
        args.push("--smoke".to_owned());
    }
    crate::ledger_pipeline::run_index_lookup_trial(
        &args,
        mode.mode,
        cli.profile == Profile::IntegratedPipeline,
    )
}

fn run_matrix(cli: Cli) -> Result<(), String> {
    let schedule = build_schedule(&cli);
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("system clock is before the Unix epoch: {error}"))?
        .as_nanos();
    let output_root = absolute_path(&cli.output_root)?;
    let initial_preflight =
        crate::ledger_pipeline::ensure_index_lookup_root_idle(&output_root, cli.smoke)?;
    let host_metadata = collect_host_metadata(&initial_preflight.path);
    fs::create_dir_all(&output_root).map_err(|error| {
        format!(
            "cannot create index-lookup output root {}: {error}",
            output_root.display()
        )
    })?;
    let run_dir = output_root.join(format!("run-{run_id}"));
    fs::create_dir(&run_dir)
        .map_err(|error| format!("cannot create run directory {}: {error}", run_dir.display()))?;
    let trials_dir = run_dir.join("trials");
    fs::create_dir(&trials_dir).map_err(|error| {
        format!(
            "cannot create trial directory {}: {error}",
            trials_dir.display()
        )
    })?;

    let run_kind = format!(
        "{}_{}",
        if cli.is_partial() {
            "partial"
        } else {
            "complete"
        },
        if cli.smoke {
            "smoke_matrix"
        } else {
            "full_matrix"
        }
    );
    let summary_path = run_dir.join("ledger_index_lookup_summary.csv");
    let stages_path = run_dir.join("ledger_index_lookup_stages.csv");
    let manifest_path = run_dir.join("trial_manifest.csv");
    fs::write(&summary_path, SUMMARY_PREFIX)
        .map_err(|error| format!("cannot initialize run summary: {error}"))?;
    fs::write(&stages_path, STAGE_PREFIX)
        .map_err(|error| format!("cannot initialize run stages: {error}"))?;
    fs::write(
        &manifest_path,
        "trial_index,mode,repetition,users,requests,partial_designation,status,exit_code,trial_directory,stdout_log,stderr_log\n",
    )
    .map_err(|error| format!("cannot initialize trial manifest: {error}"))?;
    let initial_meta = run_metadata(
        run_id,
        &run_kind,
        &cli,
        schedule.len(),
        &initial_preflight,
        &host_metadata,
        "running",
    );
    let metadata_path = run_dir.join("run_metadata.txt");
    fs::write(&metadata_path, initial_meta)
        .map_err(|error| format!("cannot write run metadata: {error}"))?;

    let executable = std::env::current_exe()
        .map_err(|error| format!("cannot locate index-lookup executable: {error}"))?;
    let mut completed = 0_usize;
    let expected_requests = if cli.smoke {
        (SMOKE_USERS * REQUESTS_PER_USER) as u64
    } else {
        (FULL_USERS * REQUESTS_PER_USER) as u64
    };
    for trial in schedule.iter().copied() {
        let trial_name = format!(
            "trial-{:02}-{}_r{}",
            trial.index, trial.mode.name, trial.repetition
        );
        let trial_dir = trials_dir.join(&trial_name);
        fs::create_dir(&trial_dir).map_err(|error| {
            format!(
                "cannot create trial directory {}: {error}",
                trial_dir.display()
            )
        })?;
        let stdout_name = format!("trial-{:02}.stdout.log", trial.index);
        let stderr_name = format!("trial-{:02}.stderr.log", trial.index);
        eprintln!(
            "TRIAL_START {}/{} mode={} repetition={} smoke={} partial={}",
            trial.index,
            schedule.len(),
            trial.mode.name,
            trial.repetition,
            cli.smoke,
            cli.is_partial()
        );
        let mut command = Command::new(&executable);
        command
            .arg("--internal-trial")
            .arg("--profile")
            .arg(cli.profile.name())
            .arg("--trial-mode")
            .arg(trial.mode.name)
            .arg("--trial-output")
            .arg(&trial_dir);
        if cli.smoke {
            command.arg("--smoke");
        }
        let output = command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .map_err(|error| format!("cannot launch trial child {}: {error}", trial.index))?;
        fs::write(trial_dir.join(&stdout_name), output.stdout)
            .map_err(|error| format!("cannot store trial stdout {}: {error}", trial.index))?;
        fs::write(trial_dir.join(&stderr_name), output.stderr)
            .map_err(|error| format!("cannot store trial stderr {}: {error}", trial.index))?;
        let status = if output.status.success() {
            "ok"
        } else {
            "failed"
        };
        append_manifest(
            &manifest_path,
            trial,
            if cli.smoke { SMOKE_USERS } else { FULL_USERS },
            expected_requests,
            cli.is_partial(),
            status,
            output.status.code(),
            &trial_name,
            &stdout_name,
            &stderr_name,
        )?;
        if !output.status.success() {
            fs::write(
                &metadata_path,
                run_metadata(
                    run_id,
                    &run_kind,
                    &cli,
                    schedule.len(),
                    &initial_preflight,
                    &host_metadata,
                    "failed",
                ),
            )
            .map_err(|error| format!("cannot update failed run metadata: {error}"))?;
            return Err(format!(
                "index-lookup trial {} ({}) failed with status {}; see {}/{}",
                trial.index,
                trial.mode.name,
                output.status,
                trial_dir.display(),
                stderr_name
            ));
        }
        if let Err(error) = append_trial_artifacts(
            &summary_path,
            &stages_path,
            &trial_dir,
            trial,
            cli.is_partial(),
            if cli.smoke { SMOKE_USERS } else { FULL_USERS },
            expected_requests,
            if cli.smoke { 1 } else { 64 },
            cli.profile,
        ) {
            fs::write(
                &metadata_path,
                run_metadata(
                    run_id,
                    &run_kind,
                    &cli,
                    schedule.len(),
                    &initial_preflight,
                    &host_metadata,
                    "failed",
                ),
            )
            .map_err(|write_error| {
                format!("{error}; cannot update failed run metadata: {write_error}")
            })?;
            return Err(error);
        }
        completed += 1;
        eprintln!(
            "TRIAL_DONE {}/{} mode={} repetition={} output={}",
            completed,
            schedule.len(),
            trial.mode.name,
            trial.repetition,
            trial_dir.display()
        );
    }

    if let Err(error) =
        validate_completed_matrix(&summary_path, &stages_path, &manifest_path, &schedule, &cli)
    {
        fs::write(
            &metadata_path,
            run_metadata(
                run_id,
                &run_kind,
                &cli,
                schedule.len(),
                &initial_preflight,
                &host_metadata,
                "failed",
            ),
        )
        .map_err(|write_error| format!("cannot mark invalid run as failed: {write_error}"))?;
        return Err(error);
    }

    if !cli.smoke
        && !cli.is_partial()
        && output_root == absolute_path(Path::new(cli.profile.archive_root()))?
    {
        if let Err(error) = write_canonical_report(&run_dir, run_id, cli.profile) {
            fs::write(
                &metadata_path,
                run_metadata(
                    run_id,
                    &run_kind,
                    &cli,
                    schedule.len(),
                    &initial_preflight,
                    &host_metadata,
                    "failed",
                ),
            )
            .map_err(|write_error| {
                format!("cannot mark report-generation failure: {write_error}")
            })?;
            return Err(error);
        }
    }

    fs::write(
        &metadata_path,
        run_metadata(
            run_id,
            &run_kind,
            &cli,
            schedule.len(),
            &initial_preflight,
            &host_metadata,
            "complete",
        ),
    )
    .map_err(|error| format!("cannot finalize run metadata: {error}"))?;
    println!("ARCHIVE {}", run_dir.display());
    Ok(())
}

fn append_trial_artifacts(
    summary_path: &Path,
    stages_path: &Path,
    trial_dir: &Path,
    trial: Trial,
    partial: bool,
    expected_users: usize,
    expected_requests: u64,
    expected_stride: usize,
    profile: Profile,
) -> Result<(), String> {
    validate_trial_artifacts(
        trial_dir,
        trial,
        expected_users,
        expected_requests,
        expected_stride,
        profile,
    )?;
    let source_summary = trial_dir.join("ledger_index_lookup_trial_summary.csv");
    let summary = fs::read_to_string(&source_summary)
        .map_err(|error| format!("cannot read {}: {error}", source_summary.display()))?;
    let mut lines = summary.lines();
    let header = lines
        .next()
        .ok_or_else(|| format!("{} has no header", source_summary.display()))?;
    let row = lines
        .next()
        .ok_or_else(|| format!("{} has no data row", source_summary.display()))?;
    if lines.next().is_some() {
        return Err(format!(
            "{} has more than one data row",
            source_summary.display()
        ));
    }
    let mut summary_file = OpenOptions::new()
        .append(true)
        .open(summary_path)
        .map_err(|error| format!("cannot append {}: {error}", summary_path.display()))?;
    if fs::metadata(summary_path)
        .map_err(|error| format!("cannot stat {}: {error}", summary_path.display()))?
        .len()
        == SUMMARY_PREFIX.len() as u64
    {
        writeln!(summary_file, "{header}")
            .map_err(|error| format!("cannot write {}: {error}", summary_path.display()))?;
    } else {
        let existing = fs::read_to_string(summary_path)
            .map_err(|error| format!("cannot inspect {}: {error}", summary_path.display()))?;
        let existing_header = existing
            .lines()
            .next()
            .ok_or_else(|| format!("{} has no appended header", summary_path.display()))?;
        if existing_header != format!("{SUMMARY_PREFIX}{header}") {
            return Err(format!(
                "trial {} summary schema differs from prior trials",
                trial.index
            ));
        }
    }
    writeln!(
        summary_file,
        "{},{},{},{},{},{},{}",
        trial.index,
        trial.repetition,
        partial,
        format!(
            "trial-{:02}-{}_r{}",
            trial.index, trial.mode.name, trial.repetition
        ),
        format!("trial-{:02}.stdout.log", trial.index),
        format!("trial-{:02}.stderr.log", trial.index),
        row
    )
    .map_err(|error| format!("cannot append {}: {error}", summary_path.display()))?;

    let source_stages = trial_dir.join("ledger_index_lookup_trial_stages.csv");
    let stage_data = fs::read_to_string(&source_stages)
        .map_err(|error| format!("cannot read {}: {error}", source_stages.display()))?;
    let mut stage_lines = stage_data.lines();
    let stage_header = stage_lines
        .next()
        .ok_or_else(|| format!("{} has no header", source_stages.display()))?;
    let mut stages_file = OpenOptions::new()
        .append(true)
        .open(stages_path)
        .map_err(|error| format!("cannot append {}: {error}", stages_path.display()))?;
    if fs::metadata(stages_path)
        .map_err(|error| format!("cannot stat {}: {error}", stages_path.display()))?
        .len()
        == STAGE_PREFIX.len() as u64
    {
        writeln!(stages_file, "{stage_header}")
            .map_err(|error| format!("cannot write {}: {error}", stages_path.display()))?;
    } else {
        let existing = fs::read_to_string(stages_path)
            .map_err(|error| format!("cannot inspect {}: {error}", stages_path.display()))?;
        let existing_header = existing
            .lines()
            .next()
            .ok_or_else(|| format!("{} has no appended header", stages_path.display()))?;
        if existing_header != format!("{STAGE_PREFIX}{stage_header}") {
            return Err(format!(
                "trial {} stage schema differs from prior trials",
                trial.index
            ));
        }
    }
    for row in stage_lines {
        writeln!(
            stages_file,
            "{},{},{},{}",
            trial.index, trial.repetition, trial.mode.name, row
        )
        .map_err(|error| format!("cannot append {}: {error}", stages_path.display()))?;
    }
    Ok(())
}

fn validate_trial_artifacts(
    trial_dir: &Path,
    trial: Trial,
    expected_users: usize,
    expected_requests: u64,
    expected_stride: usize,
    profile: Profile,
) -> Result<(), String> {
    let summary_path = trial_dir.join("ledger_index_lookup_trial_summary.csv");
    let (summary_header, summary_row) = read_one_row_csv(&summary_path)?;
    let summary = row_map(&summary_header, &summary_row, &summary_path)?;
    let expected_seed_seq = (expected_users as u64) * 3;
    let expected_final_seq = expected_seed_seq + expected_requests;
    let expected_each_operation = expected_requests / 2;
    let expected_batches = required_u64(&summary, "batch_count", &summary_path)?;
    let sample_count = required_u64(&summary, "request_sample_count", &summary_path)?;
    let common_values_match = required(&summary, "mode", &summary_path)? == trial.mode.name
        && required_u64(&summary, "workers", &summary_path)? == 4
        && required(&summary, "index_lookup_strategy", &summary_path)?
            == match trial.mode.mode {
                IndexLookupMode::PointGet => "point_get",
                IndexLookupMode::WholeBatchMultiGet => "whole_batch_multiget",
                IndexLookupMode::Chunked { .. } => "chunked",
            }
        && required_u64(&summary, "users", &summary_path)? == expected_users as u64
        && required_u64(&summary, "coroutines", &summary_path)? == expected_users as u64
        && required_u64(&summary, "requests", &summary_path)? == expected_requests
        && required_u64(&summary, "credits", &summary_path)? == expected_each_operation
        && required_u64(&summary, "debits", &summary_path)? == expected_each_operation
        && required_u64(&summary, "fresh_commits", &summary_path)? == expected_requests
        && required_u64(&summary, "historical_hits", &summary_path)? == 0
        && required_u64(&summary, "historical_misses", &summary_path)? == 0
        && required_u64(&summary, "sample_stride", &summary_path)? == expected_stride as u64
        && required_u64(&summary, "keys_looked_up", &summary_path)? == expected_requests
        && required_u64(&summary, "misses", &summary_path)? == expected_requests
        && required_u64(&summary, "hits", &summary_path)? == 0
        && required_u64(&summary, "final_seq", &summary_path)? == expected_final_seq
        && sample_count > 0
        && sample_count <= expected_requests
        && required_u64(&summary, "native_get_calls", &summary_path)? > 0
        && required_u64(&summary, "native_get_calls", &summary_path)? <= expected_requests
        && required_f64(&summary, "client_wall_s", &summary_path)? > 0.0
        && required_f64(&summary, "client_rps", &summary_path)? > 0.0
        && required_f64(&summary, "cpu_s", &summary_path)? >= 0.0
        && required_f64(&summary, "cpu_core_equivalents", &summary_path)? >= 0.0
        && required_f64(&summary, "cpu_ns_per_request", &summary_path)? >= 0.0
        && required_u64(&summary, "request_p50_ns", &summary_path)?
            <= required_u64(&summary, "request_p95_ns", &summary_path)?
        && required_u64(&summary, "request_p95_ns", &summary_path)?
            <= required_u64(&summary, "request_p99_ns", &summary_path)?
        && required_f64(&summary, "recovery_s", &summary_path)? >= 0.0
        && required_f64(&summary, "integrity_s", &summary_path)? >= 0.0
        && required_f64(&summary, "setup_preflight_cpu_pct", &summary_path)? <= 10.0
        && required_f64(&summary, "setup_preflight_disk_pct", &summary_path)? <= 5.0
        && required_f64(&summary, "measure_preflight_cpu_pct", &summary_path)? <= 10.0
        && required_f64(&summary, "measure_preflight_disk_pct", &summary_path)? <= 5.0
        && expected_batches > 0;
    let profile_values_match = match profile {
        Profile::ForegroundPersistence => {
            required(&summary, "projection_enabled", &summary_path)? == "false"
                && required(&summary, "gc_enabled", &summary_path)? == "false"
                && required_u64(&summary, "watermark_updates", &summary_path)? == 0
                && required_u64(&summary, "latest_seq_near_client_end", &summary_path)?
                    == expected_final_seq
                && required_u64(&summary, "projected_seq_near_client_end", &summary_path)?
                    == expected_seed_seq
                && required_u64(&summary, "destination_seq_near_client_end", &summary_path)?
                    == expected_seed_seq
                && required_u64(&summary, "gc_prefix_near_client_end", &summary_path)? == 0
                && required_u64(&summary, "gc_prefix_after_settle", &summary_path)? == 0
                && required_u64(&summary, "initial_watermark", &summary_path)?
                    == required_u64(&summary, "final_watermark", &summary_path)?
        }
        Profile::IntegratedPipeline => {
            let latest = required_u64(&summary, "latest_seq_near_client_end", &summary_path)?;
            let projected = required_u64(&summary, "projected_seq_near_client_end", &summary_path)?;
            let destination =
                required_u64(&summary, "destination_seq_near_client_end", &summary_path)?;
            let gc_prefix = required_u64(&summary, "gc_prefix_near_client_end", &summary_path)?;
            required(&summary, "projection_enabled", &summary_path)? == "true"
                && required(&summary, "gc_enabled", &summary_path)? == "true"
                && required_u64(&summary, "watermark_updates", &summary_path)? > 0
                && latest == expected_final_seq
                && projected >= expected_seed_seq
                && projected <= latest
                && projected <= destination
                && destination <= expected_final_seq
                && gc_prefix <= projected
                && gc_prefix <= latest
                && required_u64(&summary, "gc_prefix_after_settle", &summary_path)?
                    <= expected_final_seq
                && required_u64(&summary, "final_watermark", &summary_path)?
                    > required_u64(&summary, "initial_watermark", &summary_path)?
        }
    };
    let values_match = common_values_match && profile_values_match;
    if !values_match {
        return Err(format!(
            "trial {} summary does not match the fixed {} profile",
            trial.index, trial.mode.name
        ));
    }
    if required(&summary, "device", &summary_path)?.is_empty()
        || required(&summary, "major_minor", &summary_path)?.is_empty()
    {
        return Err(format!(
            "trial {} has no target device identity in its summary",
            trial.index
        ));
    }
    if trial.repetition == 0 || trial.index == 0 {
        return Err("trial identity is invalid".to_owned());
    }

    let metadata_path = trial_dir.join("ledger_index_lookup_trial_metadata.txt");
    let metadata = read_key_value_metadata(&metadata_path)?;
    let (expected_lookup_strategy, expected_group_size, expected_concurrency) =
        index_lookup_metadata_values(trial.mode.mode);
    let common_metadata_mismatch = required(&metadata, "mode", &metadata_path)? != trial.mode.name
        || required(&metadata, "index_lookup_strategy", &metadata_path)?
            != expected_lookup_strategy
        || required(&metadata, "index_group_size", &metadata_path)? != expected_group_size
        || required(&metadata, "index_concurrency", &metadata_path)? != expected_concurrency
        || required_u64(&metadata, "workers", &metadata_path)? != 4
        || required_u64(&metadata, "users", &metadata_path)? != expected_users as u64
        || required_u64(&metadata, "coroutines", &metadata_path)? != expected_users as u64
        || required_u64(&metadata, "requests_per_user", &metadata_path)?
            != REQUESTS_PER_USER as u64
        || required_u64(&metadata, "requests", &metadata_path)? != expected_requests
        || required_u64(&metadata, "credits", &metadata_path)? != expected_each_operation
        || required_u64(&metadata, "debits", &metadata_path)? != expected_each_operation
        || required_u64(&metadata, "fresh_commits", &metadata_path)? != expected_requests
        || required_u64(&metadata, "historical_hits", &metadata_path)? != 0
        || required_u64(&metadata, "historical_misses", &metadata_path)? != 0
        || required_u64(&metadata, "sample_stride", &metadata_path)? != expected_stride as u64
        || required(&metadata, "balance_mode", &metadata_path)? != "per_batch"
        || required_u64(&metadata, "seed_transactions_per_user", &metadata_path)? != 3
        || required_u64(&metadata, "seed_initial_credit", &metadata_path)? != 100
        || required_u64(&metadata, "seed_credit", &metadata_path)? != 1
        || required_u64(&metadata, "seed_debit", &metadata_path)? != 1
        || required_u64(&metadata, "seed_sequence", &metadata_path)? != expected_seed_seq
        || required_u64(&metadata, "final_sequence", &metadata_path)? != expected_final_seq
        || required_u64(&metadata, "setup_preflight_observation_ms", &metadata_path)?
            != if expected_stride == 1 { 100 } else { 3000 }
        || required_u64(&metadata, "setup_preflight_timeout_ms", &metadata_path)? != 60_000;
    let profile_metadata_match = match profile {
        Profile::ForegroundPersistence => {
            required(&metadata, "projection_enabled", &metadata_path)? == "false"
                && required(&metadata, "gc_enabled", &metadata_path)? == "false"
                && required_u64(&metadata, "watermark_updates", &metadata_path)? == 0
                && required(&metadata, "projector_spawned", &metadata_path)? == "false"
                && required(&metadata, "watermark_manager_spawned", &metadata_path)? == "false"
                && required(&metadata, "gc_worker_spawned", &metadata_path)? == "false"
                && required(
                    &metadata,
                    "initial_and_final_watermark_unchanged",
                    &metadata_path,
                )? == "true"
                && required_u64(&metadata, "projected_seq_near_client_end", &metadata_path)?
                    == expected_seed_seq
                && required_u64(&metadata, "destination_seq_near_client_end", &metadata_path)?
                    == expected_seed_seq
                && required_u64(&metadata, "gc_prefix_near_client_end", &metadata_path)? == 0
                && required_u64(&metadata, "gc_prefix_after_settle", &metadata_path)? == 0
        }
        Profile::IntegratedPipeline => {
            let latest = required_u64(&metadata, "final_sequence", &metadata_path)?;
            let projected =
                required_u64(&metadata, "projected_seq_near_client_end", &metadata_path)?;
            let destination =
                required_u64(&metadata, "destination_seq_near_client_end", &metadata_path)?;
            let gc_prefix = required_u64(&metadata, "gc_prefix_near_client_end", &metadata_path)?;
            required(&metadata, "projection_enabled", &metadata_path)? == "true"
                && required(&metadata, "gc_enabled", &metadata_path)? == "true"
                && required_u64(&metadata, "watermark_updates", &metadata_path)? > 0
                && required(&metadata, "projector_spawned", &metadata_path)? == "true"
                && required(&metadata, "watermark_manager_spawned", &metadata_path)? == "true"
                && required(&metadata, "gc_worker_spawned", &metadata_path)? == "true"
                && required(
                    &metadata,
                    "initial_and_final_watermark_unchanged",
                    &metadata_path,
                )? == "false"
                && projected >= expected_seed_seq
                && projected <= latest
                && projected <= destination
                && destination <= latest
                && gc_prefix <= projected
                && gc_prefix <= latest
                && required_u64(&metadata, "gc_prefix_after_settle", &metadata_path)? <= latest
        }
    };
    if common_metadata_mismatch || !profile_metadata_match {
        return Err(format!(
            "trial {} metadata does not match the fixed profile",
            trial.index
        ));
    }

    let batches_path = trial_dir.join("ledger_index_lookup_trial_batches.csv");
    let batch_rows = read_csv_rows(&batches_path)?;
    if batch_rows.rows.len() as u64 != expected_batches {
        return Err(format!(
            "trial {} has {} batch rows but summary reports {expected_batches}",
            trial.index,
            batch_rows.rows.len()
        ));
    }
    let mut requests_seen = 0_u64;
    let mut native_calls_seen = 0_u64;
    let mut groups_seen = 0_u64;
    let mut max_in_flight = 0_u64;
    let mut max_running = 0_u64;
    for (expected_index, row) in batch_rows.rows.iter().enumerate() {
        let map = row_map(&batch_rows.header, row, &batches_path)?;
        let transaction_count = required_u64(&map, "transaction_count", &batches_path)?;
        let batch_keys = required_u64(&map, "keys_looked_up", &batches_path)?;
        let batch_misses = required_u64(&map, "misses", &batches_path)?;
        let batch_hits = required_u64(&map, "hits", &batches_path)?;
        let calls = required_u64(&map, "get_calls", &batches_path)?;
        let groups = required_u64(&map, "groups_submitted", &batches_path)?;
        let in_flight = required_u64(&map, "max_in_flight_groups", &batches_path)?;
        let running = required_u64(&map, "max_running_query_jobs", &batches_path)?;
        if required(&map, "mode", &batches_path)? != trial.mode.name
            || required_u64(&map, "batch_index", &batches_path)? != expected_index as u64
            || transaction_count == 0
            || batch_keys != transaction_count
            || batch_misses != transaction_count
            || batch_hits != 0
        {
            return Err(format!(
                "trial {} batch {} has invalid identity or lookup totals",
                trial.index, expected_index
            ));
        }
        let expected_groups = match trial.mode.mode {
            IndexLookupMode::PointGet => 0,
            IndexLookupMode::WholeBatchMultiGet => 1,
            IndexLookupMode::Chunked { group_size, .. } => {
                transaction_count.div_ceil(group_size as u64)
            }
        };
        let expected_calls = match trial.mode.mode {
            IndexLookupMode::PointGet => transaction_count,
            _ => expected_groups,
        };
        if groups != expected_groups || calls != expected_calls {
            return Err(format!(
                "trial {} batch {} submitted {groups} groups and {calls} native calls; expected {expected_groups} and {expected_calls}",
                trial.index, expected_index
            ));
        }
        let query_wall = required(&map, "query_wall_ns", &batches_path)?;
        let lookup_pool_wait = required(&map, "lookup_blocking_pool_wait_ns", &batches_path)?;
        let lookup_collect = required(&map, "lookup_submit_to_collection_ns", &batches_path)?;
        match trial.mode.mode {
            IndexLookupMode::PointGet => {
                if query_wall != "NA"
                    || lookup_pool_wait != "NA"
                    || lookup_collect != "NA"
                    || in_flight != 0
                    || running != 0
                {
                    return Err(format!(
                        "trial {} point_get batch {} reports a nonexistent prefetch phase",
                        trial.index, expected_index
                    ));
                }
            }
            IndexLookupMode::WholeBatchMultiGet | IndexLookupMode::Chunked { .. } => {
                if query_wall == "NA"
                    || lookup_pool_wait == "NA"
                    || lookup_collect == "NA"
                    || in_flight == 0
                    || running == 0
                    || running > in_flight
                {
                    return Err(format!(
                        "trial {} prefetch batch {} has invalid query phase or in-flight observations",
                        trial.index, expected_index
                    ));
                }
                let configured_max = match trial.mode.mode {
                    IndexLookupMode::WholeBatchMultiGet => 1,
                    IndexLookupMode::Chunked { max_in_flight, .. } => max_in_flight as u64,
                    IndexLookupMode::PointGet => unreachable!(),
                };
                if in_flight > configured_max {
                    return Err(format!(
                        "trial {} batch {} observed {in_flight} in-flight groups above bound {configured_max}",
                        trial.index, expected_index
                    ));
                }
            }
        }
        requests_seen = requests_seen.saturating_add(transaction_count);
        native_calls_seen = native_calls_seen.saturating_add(calls);
        groups_seen = groups_seen.saturating_add(groups);
        max_in_flight = max_in_flight.max(in_flight);
        max_running = max_running.max(running);
    }
    if requests_seen != expected_requests
        || native_calls_seen != required_u64(&summary, "native_get_calls", &summary_path)?
        || max_in_flight != required_u64(&summary, "max_in_flight_groups", &summary_path)?
        || max_running != required_u64(&summary, "max_running_query_jobs", &summary_path)?
    {
        return Err(format!(
            "trial {} batch totals disagree with the run summary",
            trial.index
        ));
    }

    let stages_path = trial_dir.join("ledger_index_lookup_trial_stages.csv");
    let stage_rows = read_csv_rows(&stages_path)?;
    let mut seen_stages = std::collections::BTreeSet::new();
    let mut stages_by_name = BTreeMap::new();
    for row in &stage_rows.rows {
        let map = row_map(&stage_rows.header, row, &stages_path)?;
        let name = required(&map, "stage", &stages_path)?.to_owned();
        let sample_count = required_u64(&map, "sample_count", &stages_path)?;
        if !seen_stages.insert(name.clone()) {
            return Err(format!(
                "duplicate stage {name} in {}",
                stages_path.display()
            ));
        }
        if required(&map, "unit", &stages_path)? != "ns"
            || required(&map, "scope", &stages_path)?.is_empty()
        {
            return Err(format!("stage {name} has missing unit or scope"));
        }
        let percentile_values = [
            required(&map, "p50_ns", &stages_path)?,
            required(&map, "p95_ns", &stages_path)?,
            required(&map, "p99_ns", &stages_path)?,
        ];
        if sample_count == 0 {
            if percentile_values.iter().any(|value| *value != "NA") {
                return Err(format!(
                    "stage {name} has percentiles without samples in {}",
                    stages_path.display()
                ));
            }
        } else {
            let p50 = percentile_values[0].parse::<u64>().map_err(|error| {
                format!(
                    "invalid p50 for {name} in {}: {error}",
                    stages_path.display()
                )
            })?;
            let p95 = percentile_values[1].parse::<u64>().map_err(|error| {
                format!(
                    "invalid p95 for {name} in {}: {error}",
                    stages_path.display()
                )
            })?;
            let p99 = percentile_values[2].parse::<u64>().map_err(|error| {
                format!(
                    "invalid p99 for {name} in {}: {error}",
                    stages_path.display()
                )
            })?;
            if p50 > p95 || p95 > p99 {
                return Err(format!("stage {name} percentiles are out of order"));
            }
        }
        stages_by_name.insert(name, map);
    }
    for request_stage in [
        "request.total",
        "request.admission",
        "request.enqueue",
        "request.queue",
        "request.batch",
        "request.handler",
        "request.response",
    ] {
        validate_stage_count(&stages_by_name, request_stage, sample_count, &stages_path)?;
    }
    for batch_stage in [
        "dispatch.wait",
        "batch_gate.wait",
        "keyprep",
        "lookup.native_get",
        "lookup.decode_validate",
        "apply.blocking_pool_wait",
        "apply.submit_to_collection",
        "sequential_apply_build",
        "write.sync_write_batch",
        "memory.publish",
    ] {
        validate_stage_count(&stages_by_name, batch_stage, expected_batches, &stages_path)?;
    }
    for prefetch_stage in [
        "query.wall",
        "lookup.blocking_pool_wait",
        "lookup.submit_to_collection",
    ] {
        let expected_count = if trial.mode.mode == IndexLookupMode::PointGet {
            0
        } else {
            expected_batches
        };
        validate_stage_count(
            &stages_by_name,
            prefetch_stage,
            expected_count,
            &stages_path,
        )?;
    }
    let expected_group_samples = groups_seen;
    for group_stage in [
        "lookup_group.blocking_pool_wait",
        "lookup_group.native_get",
        "lookup_group.decode_validate",
        "lookup_group.submit_to_collection",
    ] {
        validate_stage_count(
            &stages_by_name,
            group_stage,
            expected_group_samples,
            &stages_path,
        )?;
    }
    if stages_by_name.len() != 24 {
        return Err(format!(
            "trial {} contains {} stage summaries, expected 24",
            trial.index,
            stages_by_name.len()
        ));
    }
    let point_get = trial.mode.mode == IndexLookupMode::PointGet;
    let expected_scopes = [
        ("request.total", "per_sample_request"),
        ("request.admission", "per_sample_request"),
        ("request.enqueue", "per_sample_request"),
        ("request.queue", "per_sample_request"),
        ("request.batch", "per_sample_request"),
        ("request.handler", "per_sample_request"),
        ("request.response", "per_sample_request"),
        ("dispatch.wait", "per_batch_wall"),
        (
            "batch_gate.wait",
            "per_batch_exclusive_batch_gate_lock_wait",
        ),
        (
            "keyprep",
            if point_get {
                "per_batch_sum_of_key_encoding"
            } else {
                "per_batch_wall"
            },
        ),
        (
            "query.wall",
            "per_batch_wall_from_keyprep_start_to_all_groups_validated",
        ),
        (
            "lookup.blocking_pool_wait",
            "per_batch_sum_of_group_waits_overlapping",
        ),
        (
            "lookup.native_get",
            "per_batch_sum_of_get_or_multiget_call_durations",
        ),
        (
            "lookup.decode_validate",
            "per_batch_sum_of_record_decode_and_key_validation",
        ),
        (
            "lookup.submit_to_collection",
            "per_batch_sum_of_overlapping_group_durations",
        ),
        (
            "apply.blocking_pool_wait",
            if point_get {
                "per_batch_legacy_worker_wait_before_interleaved_loop"
            } else {
                "per_batch_sequential_apply_worker_wait"
            },
        ),
        (
            "apply.submit_to_collection",
            "per_batch_worker_wall_including_pool_wait",
        ),
        (
            "sequential_apply_build",
            if point_get {
                "per_batch_legacy_interleaved_loop_including_point_reads_and_decode"
            } else {
                "per_batch_sequential_apply_and_build_after_prefetch"
            },
        ),
        ("write.sync_write_batch", "per_batch_sync_write_call"),
        ("memory.publish", "per_batch_state_publish_wall"),
        (
            "lookup_group.blocking_pool_wait",
            "per_group_worker_start_minus_submit",
        ),
        ("lookup_group.native_get", "per_group_multiget_call_wall"),
        (
            "lookup_group.decode_validate",
            "per_group_record_decode_and_key_validation_sum",
        ),
        (
            "lookup_group.submit_to_collection",
            "per_group_submit_to_join_completion_wall",
        ),
    ];
    for (stage, expected_scope) in expected_scopes {
        let map = stages_by_name
            .get(stage)
            .ok_or_else(|| format!("{} is missing stage {stage}", stages_path.display()))?;
        if required(map, "scope", &stages_path)? != expected_scope {
            return Err(format!(
                "trial {} stage {stage} has an unexpected scope",
                trial.index
            ));
        }
    }
    if profile == Profile::IntegratedPipeline {
        validate_integrated_pipeline_artifacts(trial_dir, trial, expected_requests)?;
    }
    Ok(())
}

fn validate_integrated_pipeline_artifacts(
    trial_dir: &Path,
    trial: Trial,
    expected_requests: u64,
) -> Result<(), String> {
    let summary_path = trial_dir.join("ledger_pipeline_summary.csv");
    let (summary_header, summary_row) = read_one_row_csv(&summary_path)?;
    let summary = row_map(&summary_header, &summary_row, &summary_path)?;
    if required_u64(&summary, "requests", &summary_path)? != expected_requests
        || required(&summary, "balance_mode", &summary_path)? != "per_batch"
        || required_u64(&summary, "runtime_workers", &summary_path)? != 4
        || required(&summary, "projection_enabled", &summary_path)? != "true"
        || required(&summary, "gc_enabled", &summary_path)? != "true"
    {
        return Err(format!(
            "trial {} pipeline summary does not match the integrated profile",
            trial.index
        ));
    }
    let stages_path = trial_dir.join("ledger_pipeline_stages.csv");
    let stages = read_csv_rows(&stages_path)?;
    for expected_stage in [
        "projection.read",
        "projection.apply",
        "projection.progress_sync",
        "gc.scan",
        "gc.delete_build",
        "gc.sync_write",
        "watermark.fence_wait",
        "watermark.projection_wait",
        "watermark.persist",
    ] {
        let row = stages
            .rows
            .iter()
            .map(|row| row_map(&stages.header, row, &stages_path))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .find(|row| {
                row.get("stage")
                    .is_some_and(|stage| stage == expected_stage)
            })
            .ok_or_else(|| {
                format!(
                    "{} is missing required background stage {expected_stage}",
                    stages_path.display()
                )
            })?;
        if required_u64(&row, "sample_count", &stages_path)? == 0 {
            return Err(format!(
                "trial {} has no measured {expected_stage} background samples",
                trial.index
            ));
        }
    }
    let background_path = trial_dir.join("ledger_pipeline_background.csv");
    let background = read_csv_rows(&background_path)?;
    let events = background
        .rows
        .iter()
        .map(|row| row_map(&background.header, row, &background_path))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .filter_map(|row| row.get("event").cloned())
        .collect::<std::collections::BTreeSet<_>>();
    for event in ["projection", "gc", "watermark"] {
        if !events.contains(event) {
            return Err(format!(
                "trial {} has no {event} background trace rows",
                trial.index
            ));
        }
    }
    let lookup_path = trial_dir.join("ledger_pipeline_index_lookup.csv");
    let (lookup_header, lookup_row) = read_one_row_csv(&lookup_path)?;
    let lookup = row_map(&lookup_header, &lookup_row, &lookup_path)?;
    let (expected_strategy, expected_group_size, expected_concurrency) =
        index_lookup_metadata_values(trial.mode.mode);
    if required(&lookup, "index_lookup_strategy", &lookup_path)? != expected_strategy
        || required(&lookup, "index_group_size", &lookup_path)? != expected_group_size
        || required(&lookup, "index_concurrency", &lookup_path)? != expected_concurrency
        || required_u64(&lookup, "runtime_workers", &lookup_path)? != 4
        || required_u64(&lookup, "lookup_requests", &lookup_path)? != expected_requests
        || required_u64(&lookup, "hits", &lookup_path)? != 0
        || required_u64(&lookup, "misses", &lookup_path)? != expected_requests
    {
        return Err(format!(
            "trial {} actual lookup metadata is inconsistent",
            trial.index
        ));
    }
    Ok(())
}

fn validate_stage_count(
    stages: &BTreeMap<String, BTreeMap<String, String>>,
    stage: &str,
    expected_count: u64,
    path: &Path,
) -> Result<(), String> {
    let row = stages
        .get(stage)
        .ok_or_else(|| format!("{} is missing stage {stage}", path.display()))?;
    let actual = required_u64(row, "sample_count", path)?;
    if actual != expected_count {
        return Err(format!(
            "stage {stage} has {actual} samples, expected {expected_count} in {}",
            path.display()
        ));
    }
    Ok(())
}

struct CsvRows {
    header: Vec<String>,
    rows: Vec<Vec<String>>,
}

fn read_one_row_csv(path: &Path) -> Result<(Vec<String>, Vec<String>), String> {
    let csv = read_csv_rows(path)?;
    if csv.rows.len() != 1 {
        return Err(format!(
            "{} has {} rows, expected exactly one",
            path.display(),
            csv.rows.len()
        ));
    }
    Ok((csv.header, csv.rows.into_iter().next().unwrap()))
}

fn read_csv_rows(path: &Path) -> Result<CsvRows, String> {
    let text = fs::read_to_string(path)
        .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
    let mut lines = text.lines();
    let header_line = lines
        .next()
        .ok_or_else(|| format!("{} has no header", path.display()))?;
    let header = parse_csv_record(header_line)?;
    if header.is_empty() {
        return Err(format!("{} has an empty header", path.display()));
    }
    let mut unique_columns = std::collections::BTreeSet::new();
    for name in &header {
        if name.is_empty() || !unique_columns.insert(name) {
            return Err(format!(
                "{} has an empty or duplicate column",
                path.display()
            ));
        }
    }
    let mut rows = Vec::new();
    for (row_index, line) in lines.enumerate() {
        let row = parse_csv_record(line)?;
        if row.len() != header.len() {
            return Err(format!(
                "{} row {} has {} fields, expected {}",
                path.display(),
                row_index + 2,
                row.len(),
                header.len()
            ));
        }
        rows.push(row);
    }
    Ok(CsvRows { header, rows })
}

fn parse_csv_record(line: &str) -> Result<Vec<String>, String> {
    let mut fields = Vec::new();
    let mut field = String::new();
    let mut characters = line.chars().peekable();
    let mut quoted = false;
    let mut closed_quote = false;
    while let Some(character) = characters.next() {
        if quoted {
            match character {
                '"' if characters.peek() == Some(&'"') => {
                    characters.next();
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
                return Err(format!(
                    "invalid character after quoted CSV value: {character:?}"
                ));
            }
            fields.push(std::mem::take(&mut field));
            closed_quote = false;
        } else {
            match character {
                ',' => fields.push(std::mem::take(&mut field)),
                '"' if field.is_empty() => quoted = true,
                '"' => return Err("quote appears in an unquoted CSV value".to_owned()),
                _ => field.push(character),
            }
        }
    }
    if quoted {
        return Err("unterminated quoted CSV value".to_owned());
    }
    fields.push(field);
    Ok(fields)
}

fn row_map(
    header: &[String],
    row: &[String],
    path: &Path,
) -> Result<BTreeMap<String, String>, String> {
    if header.len() != row.len() {
        return Err(format!(
            "{} row width {} does not match header width {}",
            path.display(),
            row.len(),
            header.len()
        ));
    }
    Ok(header.iter().cloned().zip(row.iter().cloned()).collect())
}

fn required<'a>(
    row: &'a BTreeMap<String, String>,
    name: &str,
    path: &Path,
) -> Result<&'a str, String> {
    row.get(name)
        .map(String::as_str)
        .ok_or_else(|| format!("{} is missing column {name}", path.display()))
}

fn required_u64(row: &BTreeMap<String, String>, name: &str, path: &Path) -> Result<u64, String> {
    required(row, name, path)?
        .parse::<u64>()
        .map_err(|error| format!("{} has invalid integer {name}: {error}", path.display()))
}

fn required_f64(row: &BTreeMap<String, String>, name: &str, path: &Path) -> Result<f64, String> {
    required(row, name, path)?
        .parse::<f64>()
        .map_err(|error| format!("{} has invalid number {name}: {error}", path.display()))
}

fn read_key_value_metadata(path: &Path) -> Result<BTreeMap<String, String>, String> {
    let text = fs::read_to_string(path)
        .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
    let mut values = BTreeMap::new();
    for (line_number, line) in text.lines().enumerate() {
        if line.is_empty() {
            continue;
        }
        let (name, value) = line.split_once('=').ok_or_else(|| {
            format!(
                "{} line {} has no key/value separator",
                path.display(),
                line_number + 1
            )
        })?;
        if name.is_empty() || values.insert(name.to_owned(), value.to_owned()).is_some() {
            return Err(format!(
                "{} line {} has an empty or duplicate key",
                path.display(),
                line_number + 1
            ));
        }
    }
    Ok(values)
}

fn validate_completed_matrix(
    summary_path: &Path,
    stages_path: &Path,
    manifest_path: &Path,
    schedule: &[Trial],
    cli: &Cli,
) -> Result<(), String> {
    let expected_users = if cli.smoke { SMOKE_USERS } else { FULL_USERS };
    let expected_requests = (expected_users * REQUESTS_PER_USER) as u64;
    let expected_partial = cli.is_partial().to_string();
    let summary = read_csv_rows(summary_path)?;
    let manifest = read_csv_rows(manifest_path)?;
    let stages = read_csv_rows(stages_path)?;
    if summary.rows.len() != schedule.len() || manifest.rows.len() != schedule.len() {
        return Err(format!(
            "matrix has {} summary rows and {} manifest rows, expected {}",
            summary.rows.len(),
            manifest.rows.len(),
            schedule.len()
        ));
    }
    let expected_pairs = schedule
        .iter()
        .map(|trial| (trial.repetition, trial.mode.name.to_owned()))
        .collect::<std::collections::BTreeSet<_>>();
    if expected_pairs.len() != schedule.len() {
        return Err("matrix schedule contains duplicate mode/repetition pairs".to_owned());
    }
    let mut summary_pairs = std::collections::BTreeSet::new();
    let mut manifest_pairs = std::collections::BTreeSet::new();
    let mut stage_sets =
        BTreeMap::<(usize, String, usize), std::collections::BTreeSet<String>>::new();
    for (row_index, (summary_row, manifest_row)) in
        summary.rows.iter().zip(&manifest.rows).enumerate()
    {
        let summary_map = row_map(&summary.header, summary_row, summary_path)?;
        let manifest_map = row_map(&manifest.header, manifest_row, manifest_path)?;
        let trial_index = required_u64(&summary_map, "trial_index", summary_path)? as usize;
        let repetition = required_u64(&summary_map, "repetition", summary_path)? as usize;
        let mode = required(&summary_map, "mode", summary_path)?.to_owned();
        let pair = (repetition, mode.clone());
        let expected_latest = (expected_users as u64) * 3 + expected_requests;
        let profile_values_match = match cli.profile {
            Profile::ForegroundPersistence => {
                required(&summary_map, "projection_enabled", summary_path)? == "false"
                    && required(&summary_map, "gc_enabled", summary_path)? == "false"
                    && required_u64(&summary_map, "watermark_updates", summary_path)? == 0
                    && required_u64(&summary_map, "latest_seq_near_client_end", summary_path)?
                        == expected_latest
                    && required_u64(&summary_map, "gc_prefix_near_client_end", summary_path)? == 0
                    && required_u64(&summary_map, "gc_prefix_after_settle", summary_path)? == 0
                    && required_u64(&summary_map, "projected_seq_near_client_end", summary_path)?
                        == (expected_users as u64) * 3
                    && required_u64(
                        &summary_map,
                        "destination_seq_near_client_end",
                        summary_path,
                    )? == (expected_users as u64) * 3
                    && required_u64(&summary_map, "initial_watermark", summary_path)?
                        == required_u64(&summary_map, "final_watermark", summary_path)?
            }
            Profile::IntegratedPipeline => {
                required(&summary_map, "projection_enabled", summary_path)? == "true"
                    && required(&summary_map, "gc_enabled", summary_path)? == "true"
                    && required_u64(&summary_map, "watermark_updates", summary_path)? > 0
                    && required_u64(&summary_map, "latest_seq_near_client_end", summary_path)?
                        == expected_latest
                    && required_u64(&summary_map, "projected_seq_near_client_end", summary_path)?
                        >= (expected_users as u64) * 3
                    && required_u64(
                        &summary_map,
                        "destination_seq_near_client_end",
                        summary_path,
                    )? >= required_u64(
                        &summary_map,
                        "projected_seq_near_client_end",
                        summary_path,
                    )?
                    && required_u64(
                        &summary_map,
                        "destination_seq_near_client_end",
                        summary_path,
                    )? <= expected_latest
                    && required_u64(&summary_map, "gc_prefix_near_client_end", summary_path)?
                        <= required_u64(
                            &summary_map,
                            "projected_seq_near_client_end",
                            summary_path,
                        )?
                    && required_u64(&summary_map, "final_watermark", summary_path)?
                        > required_u64(&summary_map, "initial_watermark", summary_path)?
            }
        };
        if trial_index != row_index + 1
            || !summary_pairs.insert(pair.clone())
            || required(&summary_map, "partial_designation", summary_path)? != expected_partial
            || required_u64(&summary_map, "users", summary_path)? != expected_users as u64
            || required_u64(&summary_map, "requests", summary_path)? != expected_requests
            || required_u64(&summary_map, "credits", summary_path)? != expected_requests / 2
            || required_u64(&summary_map, "debits", summary_path)? != expected_requests / 2
            || required_u64(&summary_map, "fresh_commits", summary_path)? != expected_requests
            || required_u64(&summary_map, "historical_hits", summary_path)? != 0
            || required_u64(&summary_map, "historical_misses", summary_path)? != 0
            || !profile_values_match
            || required_u64(&summary_map, "keys_looked_up", summary_path)? != expected_requests
            || required_u64(&summary_map, "misses", summary_path)? != expected_requests
            || required_u64(&summary_map, "hits", summary_path)? != 0
        {
            return Err(format!(
                "run summary row {} has an invalid identity or workload profile",
                row_index + 1
            ));
        }
        if required(&manifest_map, "status", manifest_path)? != "ok"
            || required(&manifest_map, "exit_code", manifest_path)? != "0"
            || required(&manifest_map, "partial_designation", manifest_path)? != expected_partial
            || required_u64(&manifest_map, "trial_index", manifest_path)? != trial_index as u64
            || required_u64(&manifest_map, "repetition", manifest_path)? != repetition as u64
            || required(&manifest_map, "mode", manifest_path)? != mode
            || required_u64(&manifest_map, "users", manifest_path)? != expected_users as u64
            || required_u64(&manifest_map, "requests", manifest_path)? != expected_requests
            || required(&manifest_map, "trial_directory", manifest_path)?
                != required(&summary_map, "trial_directory", summary_path)?
            || required(&manifest_map, "stdout_log", manifest_path)?
                != required(&summary_map, "stdout_log", summary_path)?
            || required(&manifest_map, "stderr_log", manifest_path)?
                != required(&summary_map, "stderr_log", summary_path)?
            || !manifest_pairs.insert(pair)
        {
            return Err(format!(
                "trial manifest row {} does not match its successful summary",
                row_index + 1
            ));
        }
    }
    if summary_pairs != expected_pairs || manifest_pairs != expected_pairs {
        return Err("matrix is missing or duplicates a mode/repetition pair".to_owned());
    }

    for stage_row in &stages.rows {
        let map = row_map(&stages.header, stage_row, stages_path)?;
        let trial_index = required_u64(&map, "trial_index", stages_path)? as usize;
        let repetition = required_u64(&map, "repetition", stages_path)? as usize;
        let mode = required(&map, "mode", stages_path)?.to_owned();
        let stage_name = required(&map, "stage", stages_path)?.to_owned();
        let key = (trial_index, mode.clone(), repetition);
        if !schedule.iter().any(|trial| {
            trial.index == trial_index && trial.repetition == repetition && trial.mode.name == mode
        }) || !stage_sets.entry(key).or_default().insert(stage_name)
        {
            return Err("run stage rows contain an unknown trial or duplicate stage".to_owned());
        }
    }
    if stage_sets.len() != schedule.len()
        || stage_sets
            .values()
            .any(std::collections::BTreeSet::is_empty)
    {
        return Err("run stage rows do not cover every trial".to_owned());
    }
    let reference_stages = stage_sets
        .values()
        .next()
        .cloned()
        .ok_or_else(|| "run stage table is empty".to_owned())?;
    if stage_sets
        .values()
        .any(|stages| stages != &reference_stages)
    {
        return Err("run trials do not have the same stage summary schema".to_owned());
    }
    Ok(())
}

fn append_manifest(
    path: &Path,
    trial: Trial,
    users: usize,
    requests: u64,
    partial: bool,
    status: &str,
    exit_code: Option<i32>,
    trial_directory: &str,
    stdout_log: &str,
    stderr_log: &str,
) -> Result<(), String> {
    let mut manifest = OpenOptions::new()
        .append(true)
        .open(path)
        .map_err(|error| format!("cannot append trial manifest: {error}"))?;
    writeln!(
        manifest,
        "{},{},{},{},{},{},{},{},{},{},{}",
        trial.index,
        trial.mode.name,
        trial.repetition,
        users,
        requests,
        partial,
        status,
        exit_code.map_or_else(|| "signal".to_owned(), |code| code.to_string()),
        trial_directory,
        stdout_log,
        stderr_log,
    )
    .map_err(|error| format!("cannot write trial manifest: {error}"))
}

fn collect_host_metadata(path: &Path) -> HostMetadata {
    let lscpu = Command::new("lscpu")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).into_owned())
        .unwrap_or_default();
    let lscpu_values = lscpu
        .lines()
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_owned(), value.trim().to_owned()))
        .collect::<BTreeMap<_, _>>();
    let proc_cpuinfo = fs::read_to_string("/proc/cpuinfo").unwrap_or_default();
    let cpu_model = lscpu_values
        .get("Model name")
        .cloned()
        .or_else(|| {
            proc_cpuinfo.lines().find_map(|line| {
                let (name, value) = line.split_once(':')?;
                (name.trim() == "model name").then(|| value.trim().to_owned())
            })
        })
        .unwrap_or_else(|| "unknown".to_owned());
    let threads_per_core = lscpu_values
        .get("Thread(s) per core")
        .cloned()
        .unwrap_or_else(|| "unknown".to_owned());
    let smt_enabled = threads_per_core
        .parse::<usize>()
        .map(|threads| (threads > 1).to_string())
        .unwrap_or_else(|_| "unknown".to_owned());
    let cpus_allowed_list = fs::read_to_string("/proc/self/status")
        .unwrap_or_default()
        .lines()
        .find_map(|line| line.strip_prefix("Cpus_allowed_list:"))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("unknown")
        .to_owned();
    let filesystem = Command::new("df")
        .args(["-T", "-P"])
        .arg(path)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).into_owned())
        .unwrap_or_default();
    let filesystem_fields = filesystem
        .lines()
        .last()
        .map(|line| line.split_whitespace().collect::<Vec<_>>())
        .unwrap_or_default();
    let filesystem_source = filesystem_fields
        .first()
        .copied()
        .unwrap_or("unknown")
        .to_owned();
    let filesystem_type = filesystem_fields
        .get(1)
        .copied()
        .unwrap_or("unknown")
        .to_owned();
    let rustc_version = Command::new("rustc")
        .arg("--version")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .unwrap_or_else(|| "unknown".to_owned());
    let (tokio_version, rocksdb_version) = locked_package_versions();
    HostMetadata {
        cpu_model,
        logical_cpus: lscpu_values
            .get("CPU(s)")
            .cloned()
            .unwrap_or_else(|| "unknown".to_owned()),
        sockets: lscpu_values
            .get("Socket(s)")
            .cloned()
            .unwrap_or_else(|| "unknown".to_owned()),
        cores_per_socket: lscpu_values
            .get("Core(s) per socket")
            .cloned()
            .unwrap_or_else(|| "unknown".to_owned()),
        threads_per_core,
        smt_enabled,
        cpus_allowed_list,
        filesystem_source,
        filesystem_type,
        rustc_version,
        tokio_version,
        rocksdb_version,
    }
}

fn locked_package_versions() -> (String, String) {
    let lockfile = fs::read_to_string("Cargo.lock").unwrap_or_default();
    let mut package_name = None;
    let mut tokio = None;
    let mut rocksdb = None;
    for line in lockfile.lines() {
        if line == "[[package]]" {
            package_name = None;
        } else if let Some(value) = line
            .strip_prefix("name = \"")
            .and_then(|s| s.strip_suffix('"'))
        {
            package_name = Some(value.to_owned());
        } else if let Some(value) = line
            .strip_prefix("version = \"")
            .and_then(|s| s.strip_suffix('"'))
        {
            match package_name.as_deref() {
                Some("tokio") if tokio.is_none() => tokio = Some(value.to_owned()),
                Some("rocksdb") if rocksdb.is_none() => rocksdb = Some(value.to_owned()),
                _ => {}
            }
        }
    }
    (
        tokio.unwrap_or_else(|| "unknown".to_owned()),
        rocksdb.unwrap_or_else(|| "unknown".to_owned()),
    )
}

fn run_metadata(
    run_id: u128,
    run_kind: &str,
    cli: &Cli,
    trial_count: usize,
    preflight: &PreflightReport,
    host: &HostMetadata,
    status: &str,
) -> String {
    let users = if cli.smoke { SMOKE_USERS } else { FULL_USERS };
    let requests = (users * REQUESTS_PER_USER) as u64;
    let selected_modes = cli
        .modes
        .iter()
        .map(|mode| mode.name)
        .collect::<Vec<_>>()
        .join(",");
    let (projection_enabled, gc_enabled, boundary_policy, gc_policy) = match cli.profile {
        Profile::ForegroundPersistence => (
            "false",
            "false",
            "seed_time_plus_1_unchanged",
            "zero_prefix",
        ),
        Profile::IntegratedPipeline => (
            "true",
            "true",
            "advances_during_measurement_and_final_settlement",
            "safe_contiguous_prefix_through_durable_boundary",
        ),
    };
    format!(
        "run_id={run_id}\nrun_kind={run_kind}\nprofile={}\nprofile_modes={selected_modes}\nrun_status={status}\ntrial_count={trial_count}\nsmoke={}\npartial_designation={}\nusers_or_coroutines_per_trial={users}\nrequests_per_user={REQUESTS_PER_USER}\nrequests_per_trial={requests}\nsample_stride={}\nqueue_capacity=50000\nbatch_size=2048\nfirst_dequeue_timeout_ms=5\nbalance_mode=per_batch\ninitial_seed_transactions_per_user=3\nseed_initial_credit=100\nseed_followup_credit=1\nseed_debit=1\ninitial_and_final_balance=100\nlatest_sequence_per_trial={}\ninitial_seed_projected_seq={}\nboundary_policy={boundary_policy}\ngc_policy={gc_policy}\nprojector_spawned={projection_enabled}\nwatermark_manager_spawned={projection_enabled}\ngc_worker_spawned={gc_enabled}\nruntime_async_workers=4\ntokio_blocking_pool=existing_default\nrocksdb_internal_pools=existing_default\npreflight_observation_ms={}\npreflight_timeout_ms={}\npreflight_cpu_limit_pct=10\npreflight_device_busy_limit_pct=5\npreflight_setup_observed_cpu_pct={:.3}\npreflight_setup_observed_device_busy_pct={:.3}\npreflight_available_memory_bytes={}\npreflight_free_bytes={}\npreflight_device={}({})\npreflight_filesystem_source={}\npreflight_filesystem_type={}\npreflight_path={}\ncpu_model={}\ncpu_logical_processors={}\ncpu_sockets={}\ncpu_cores_per_socket={}\ncpu_threads_per_core={}\ncpu_smt_enabled={}\ncpus_allowed_list={}\nrustc_version={}\nlocked_tokio_version={}\nlocked_rocksdb_version={}\naffinity=inherited_unchanged_by_benchmark; recorded cpus_allowed_list only\nmeasurement_wall=from_client_release_to_latest_coroutine_last_reply\nmeasurement_cpu=process_time_sampled_at_latest_reply\nio_sampling=process_and_target_device_deltas_with_offsets_in_trial_summary\nmock_destination_contract=in_memory_successful_apply_is_durable_by_contract; not external-db or process-crash proof\ntrial_isolation=fresh child process, Tokio runtime, and owned RocksDB directory per trial; successful child closes, reopens, validates, and removes its DB\npartial_run=all partial selections are marked explicitly in metadata, manifest, and run summary\n",
        cli.profile.name(),
        cli.smoke,
        cli.is_partial(),
        if cli.smoke { 1 } else { 64 },
        users as u64 * 3 + requests,
        users as u64 * 3,
        preflight.observation.as_millis(),
        60_000,
        preflight.cpu_busy_pct,
        preflight.disk_busy_pct,
        preflight.mem_available_bytes,
        preflight.free_bytes,
        preflight.device,
        preflight.major_minor,
        host.filesystem_source,
        host.filesystem_type,
        preflight.path.display(),
        host.cpu_model,
        host.logical_cpus,
        host.sockets,
        host.cores_per_socket,
        host.threads_per_core,
        host.smt_enabled,
        host.cpus_allowed_list,
        host.rustc_version,
        host.tokio_version,
        host.rocksdb_version,
    )
}

fn write_canonical_report(run_dir: &Path, run_id: u128, profile: Profile) -> Result<(), String> {
    let cwd = std::env::current_dir().map_err(|error| format!("cannot read cwd: {error}"))?;
    let benches = cwd.join("benches");
    let relative_run = run_dir
        .strip_prefix(&benches)
        .map_err(|_| "default index-lookup archive is outside benches/".to_owned())?
        .to_string_lossy()
        .into_owned();
    let summary_path = run_dir.join("ledger_index_lookup_summary.csv");
    let summary_csv = read_csv_rows(&summary_path)?;
    let summary_rows = summary_csv
        .rows
        .iter()
        .map(|row| row_map(&summary_csv.header, row, &summary_path))
        .collect::<Result<Vec<_>, _>>()?;
    let stages_path = run_dir.join("ledger_index_lookup_stages.csv");
    let stages_csv = read_csv_rows(&stages_path)?;
    let stage_rows = stages_csv
        .rows
        .iter()
        .map(|row| row_map(&stages_csv.header, row, &stages_path))
        .collect::<Result<Vec<_>, _>>()?;
    let report = match profile {
        Profile::ForegroundPersistence => {
            build_canonical_report(run_id, &relative_run, &summary_rows, &stage_rows)
        }
        Profile::IntegratedPipeline => {
            build_integrated_canonical_report(run_id, &relative_run, &summary_rows, &stage_rows)?
        }
    };
    let path = canonical_report_path(profile)?;
    fs::write(&path, report)
        .map_err(|error| format!("cannot write canonical report {}: {error}", path.display()))
}

fn build_canonical_report(
    run_id: u128,
    relative_run: &str,
    summary_rows: &[BTreeMap<String, String>],
    stage_rows: &[BTreeMap<String, String>],
) -> String {
    let mut report = format!(
        "# Tokio Ledger Transaction-Index Lookup Benchmark\n\nRun `{run_id}` completed with {} trials and 180,000,000 requests. Each trial ran in its own child process, Tokio runtime, and owned RocksDB directory.\n\n",
        summary_rows.len()
    );
    report.push_str("## Fixed profile\n\n");
    report.push_str("The full profile uses one single-shard account store, 50,000 users/coroutines, 200 sequential requests per user, 10,000,000 requests per trial, one outstanding request per user, exactly 50% Credit and 50% Debit, amount 1, and no historical requests. Before measurement, each user receives Credit 100, Credit 1, and Debit 1, leaving balance 100 and seed sequence 150,000. Every successful trial ends at sequence 10,150,000 with all balances 100. PerBatch balances are persisted in the same synchronous WAL WriteBatch as ledger rows, transaction indexes, and latest sequence. The queue holds 50,000 requests and forms batches up to 2,048 after a 5 ms first-dequeue timeout. Tokio uses four async workers. Projector, watermark manager, and GC worker stay unspawned; projected and destination sequence remain 150,000, the seeded time boundary stays unchanged, and the GC prefix stays zero. The in-memory mock destination treats successful apply as durable under its benchmark contract; this does not establish durability for an external database or process-crash recovery.\n\n");
    report.push_str("The modes are the original per-record `get` loop, one native whole-batch `batched_multi_get_cf`, and 256-key groups with p1, p2, p4, or p8 maximum in-flight groups. PointGet keeps the original staged lookup, `get`, and sequential apply order. MultiGet modes finish and validate every group before sequentially applying transactions. All strategies retain the store batch gate from balance/sequence snapshot through reads, apply, sync write, and memory publication.\n\n");
    report.push_str("## Median trial results\n\nMedian of the three repetitions for each mode. Latencies below are medians of the corresponding per-trial nearest-rank percentile. Request latency samples use deterministic HASH sampling at stride 64, approximately 1/64 of requests.\n\n");
    report.push_str("| Mode | Median RPS | Request p50 / p95 / p99 (ms) | CPU cores | CPU ns/request | RSS (GiB) | Process read / write (GiB) | Device read / write (GiB) | WAL (GiB) | Native calls / keys / misses |\n|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
    for mode in MODES {
        let rows = summary_rows
            .iter()
            .filter(|row| row.get("mode").is_some_and(|name| name == mode.name))
            .collect::<Vec<_>>();
        if rows.is_empty() {
            continue;
        }
        let median = |field: &str| median_field(&rows, field);
        report.push_str(&format!(
            "| {} | {:.1} | {:.3} / {:.3} / {:.3} | {:.3} | {:.2} | {:.3} | {:.3} / {:.3} | {:.3} / {:.3} | {:.3} | {:.0} / {:.0} / {:.0} |\n",
            mode.name,
            median("client_rps"),
            median("request_p50_ns") / 1e6,
            median("request_p95_ns") / 1e6,
            median("request_p99_ns") / 1e6,
            median("cpu_core_equivalents"),
            median("cpu_ns_per_request"),
            median("rss_bytes") / 1_073_741_824.0,
            median("process_read_bytes") / 1_073_741_824.0,
            median("process_write_bytes") / 1_073_741_824.0,
            median("device_read_bytes") / 1_073_741_824.0,
            median("device_write_bytes") / 1_073_741_824.0,
            median("wal_bytes") / 1_073_741_824.0,
            median("native_get_calls"),
            median("keys_looked_up"),
            median("misses"),
        ));
    }
    report.push_str("\n## Median lookup and write attribution\n\nGroup blocking-pool waits and submit-to-collection durations are cumulative over overlapping groups within each batch. `query.wall` spans key preparation through all group results and validation. PointGet has no standalone query phase; its sequential apply/build loop includes the original interleaved index reads and decode. PointGet records key preparation, native `get`, and decode timing per individual key, while MultiGet records native timing per group; those timing stages call the clock a different number of times. All modes share the coordinator and overall metrics. Results are instrumented benchmark observations, not a calibrated promise of uninstrumented production gains. Stage durations overlap and must not be summed into end-to-end latency.\n\n| Mode | Stage | p50 / p95 / p99 (ms) | Samples per trial | Scope |\n|---|---|---:|---:|---|\n");
    for mode in MODES {
        for stage in [
            "query.wall",
            "lookup.native_get",
            "lookup.decode_validate",
            "lookup.submit_to_collection",
            "apply.blocking_pool_wait",
            "apply.submit_to_collection",
            "sequential_apply_build",
            "write.sync_write_batch",
            "memory.publish",
            "lookup_group.blocking_pool_wait",
            "lookup_group.native_get",
            "lookup_group.decode_validate",
            "lookup_group.submit_to_collection",
        ] {
            let selected = stage_rows
                .iter()
                .filter(|row| {
                    row.get("mode").is_some_and(|name| name == mode.name)
                        && row.get("stage").is_some_and(|value| value == stage)
                })
                .collect::<Vec<_>>();
            if selected.is_empty() {
                continue;
            }
            let p50 = median_field(&selected, "p50_ns");
            let p95 = median_field(&selected, "p95_ns");
            let p99 = median_field(&selected, "p99_ns");
            let sample_count = sample_count_range(&selected);
            let scope = field(selected[0], "scope").to_owned();
            let no_samples = selected
                .iter()
                .all(|row| number(row, "sample_count") == 0.0);
            let display = if no_samples {
                "N/A".to_owned()
            } else {
                format!("{:.3} / {:.3} / {:.3}", p50 / 1e6, p95 / 1e6, p99 / 1e6)
            };
            report.push_str(&format!(
                "| {} | {} | {} | {} | {} |\n",
                mode.name, stage, display, sample_count, scope
            ));
        }
    }
    report.push_str("\n## Every trial\n\nEach row retains its request p50/p95/p99, CPU, process and device I/O, WAL/flush/compaction/stall deltas, and exact lookup counts. Full per-trial stage p50/p95/p99 values are in the linked stage CSV.\n\n| Trial | Mode | Rep | RPS | Request p50 / p95 / p99 (ms) | CPU cores | CPU ns/request | RSS (GiB) | Process read / write (GiB) | Device read / write (GiB) | WAL (GiB) | Flush write (GiB) | Compaction read / write (GiB) | Stall (ms) | Calls / keys / misses |\n|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
    for row in summary_rows {
        report.push_str(&format!(
            "| {} | {} | {} | {:.1} | {:.3} / {:.3} / {:.3} | {:.3} | {:.2} | {:.3} | {:.3} / {:.3} | {:.3} / {:.3} | {:.3} | {:.3} | {:.3} / {:.3} | {:.3} | {:.0} / {:.0} / {:.0} |\n",
            field(row, "trial_index"),
            field(row, "mode"),
            field(row, "repetition"),
            number(row, "client_rps"),
            number(row, "request_p50_ns") / 1e6,
            number(row, "request_p95_ns") / 1e6,
            number(row, "request_p99_ns") / 1e6,
            number(row, "cpu_core_equivalents"),
            number(row, "cpu_ns_per_request"),
            number(row, "rss_bytes") / 1_073_741_824.0,
            number(row, "process_read_bytes") / 1_073_741_824.0,
            number(row, "process_write_bytes") / 1_073_741_824.0,
            number(row, "device_read_bytes") / 1_073_741_824.0,
            number(row, "device_write_bytes") / 1_073_741_824.0,
            number(row, "wal_bytes") / 1_073_741_824.0,
            number(row, "flush_write_bytes") / 1_073_741_824.0,
            number(row, "compaction_read_bytes") / 1_073_741_824.0,
            number(row, "compaction_write_bytes") / 1_073_741_824.0,
            number(row, "stall_us") / 1e3,
            number(row, "native_get_calls"),
            number(row, "keys_looked_up"),
            number(row, "misses"),
        ));
    }
    report.push_str("\n## Raw evidence\n\n");
    report.push_str(&format!(
        "- [run summary CSV]({relative_run}/ledger_index_lookup_summary.csv)\n- [all-trial stage percentile CSV]({relative_run}/ledger_index_lookup_stages.csv)\n- [trial manifest]({relative_run}/trial_manifest.csv)\n- [run metadata]({relative_run}/run_metadata.txt)\n"
    ));
    for row in summary_rows {
        let index = field(row, "trial_index");
        let trial_directory = field(row, "trial_directory");
        let stdout_log = field(row, "stdout_log");
        let stderr_log = field(row, "stderr_log");
        let trial_path = format!("{relative_run}/trials/{trial_directory}");
        report.push_str(&format!(
            "- [trial {index} summary]({trial_path}/ledger_index_lookup_trial_summary.csv), [batch timings]({trial_path}/ledger_index_lookup_trial_batches.csv), [stage percentiles]({trial_path}/ledger_index_lookup_trial_stages.csv), [metadata]({trial_path}/ledger_index_lookup_trial_metadata.txt), [stdout]({relative_run}/trials/{trial_directory}/{stdout_log}), [stderr]({relative_run}/trials/{trial_directory}/{stderr_log})\n",
        ));
    }
    report
}

fn build_integrated_canonical_report(
    run_id: u128,
    relative_run: &str,
    summary_rows: &[BTreeMap<String, String>],
    stage_rows: &[BTreeMap<String, String>],
) -> Result<String, String> {
    for row in summary_rows {
        integrated_client_end_backlogs(row)?;
    }
    let request_total = summary_rows
        .iter()
        .map(|row| number(row, "requests"))
        .sum::<f64>();
    let mut report = format!(
        "# Tokio Ledger Integrated Pipeline Index Lookup Benchmark\n\nRun `{run_id}` completed with {} trials and {:.0} foreground requests. Each trial ran in a fresh child process, Tokio runtime, and owned RocksDB directory.\n\n",
        summary_rows.len(),
        request_total
    );
    report.push_str("## Fixed profile\n\nEach trial uses 50,000 users/coroutines with 200 sequential amount-1 requests each, exact 50/50 credits and debits, 10,000,000 foreground requests, and PerBatch balances. Before measurement, every account receives Credit 100, Credit 1, and Debit 1, leaving balance 100 at seed sequence 150,000. The bounded request queue has capacity 50,000, batches up to 2,048 requests, and a 5 ms first-dequeue timeout. Tokio uses four async workers. The projector processes successive bounded 256-record batches. The safe GC worker yields between advancing batches and sleeps for 100 ms only when blocked or no progress is available. The watermark manager ticks every 100 ms, and the safe-GC retention window is 500 ms. Historical request routing is disabled for this comparison. The mock destination treats successful in-memory apply as durable by contract; it does not prove external database or process-crash durability.\n\n");
    report.push_str("The selected modes are PointGet, Chunked(256, 4), and Chunked(256, 8), each repeated three times in rotated order. Every trial measures foreground lookup and write work while projection progress, watermark advancement, and safe prefix GC continue in the background. WAL, flush, compaction, stall, CPU, memory, and device I/O totals cover the whole measured DB activity, including background writes. Progress and backlog snapshots are taken at client end; settlement then catches projection up, advances the final boundary, drains safe GC, and validates recovery and balances.\n\n");
    report.push_str("## Median trial results\n\nMedian of three repetitions for each mode. Request latencies are medians of the per-trial nearest-rank percentiles. The exact progress and backlog snapshot at client end remains in each trial summary.\n\n");
    report.push_str("| Mode | Median RPS | Request p50 / p95 / p99 (ms) | CPU cores | RSS (GiB) | Device read / write (GiB) | Whole-DB WAL (GiB) | Calls / keys / misses | Projection / GC backlog at client end |\n|---|---:|---:|---:|---:|---:|---:|---:|---:|\n");
    for mode in INTEGRATED_MODES {
        let rows = summary_rows
            .iter()
            .filter(|row| row.get("mode").is_some_and(|name| name == mode.name))
            .collect::<Vec<_>>();
        if rows.is_empty() {
            continue;
        }
        let projection_backlogs = rows
            .iter()
            .map(|row| integrated_client_end_backlogs(row).map(|backlogs| backlogs.0))
            .collect::<Result<Vec<_>, _>>()?;
        let gc_backlogs = rows
            .iter()
            .map(|row| integrated_client_end_backlogs(row).map(|backlogs| backlogs.1))
            .collect::<Result<Vec<_>, _>>()?;
        let median = |field: &str| median_field(&rows, field);
        report.push_str(&format!(
            "| {} | {:.1} | {:.3} / {:.3} / {:.3} | {:.3} | {:.3} | {:.3} / {:.3} | {:.3} | {:.0} / {:.0} / {:.0} | {:.0} / {:.0} records |\n",
            mode.name,
            median("client_rps"),
            median("request_p50_ns") / 1e6,
            median("request_p95_ns") / 1e6,
            median("request_p99_ns") / 1e6,
            median("cpu_core_equivalents"),
            median("rss_bytes") / 1_073_741_824.0,
            median("device_read_bytes") / 1_073_741_824.0,
            median("device_write_bytes") / 1_073_741_824.0,
            median("wal_bytes") / 1_073_741_824.0,
            median("native_get_calls"),
            median("keys_looked_up"),
            median("misses"),
            median_u64(&projection_backlogs),
            median_u64(&gc_backlogs),
        ));
    }
    report.push_str("\n## Lookup and batch-gate stages\n\n`batch_gate.wait` measures waiting to acquire the exclusive store gate, separately from `dispatch.wait` for coordinator scheduling. Lookup worker waits and submit-to-collection durations can overlap. Stage durations are diagnostic and are not additive to request latency.\n\n| Mode | Stage | Median p50 / p95 / p99 (ms) | Samples per trial | Scope |\n|---|---|---:|---:|---|\n");
    for mode in INTEGRATED_MODES {
        for stage in [
            "request.total",
            "request.admission",
            "request.enqueue",
            "request.queue",
            "request.batch",
            "request.handler",
            "request.response",
            "dispatch.wait",
            "batch_gate.wait",
            "keyprep",
            "query.wall",
            "lookup.blocking_pool_wait",
            "lookup.native_get",
            "lookup.decode_validate",
            "lookup.submit_to_collection",
            "apply.blocking_pool_wait",
            "apply.submit_to_collection",
            "sequential_apply_build",
            "write.sync_write_batch",
            "memory.publish",
            "lookup_group.blocking_pool_wait",
            "lookup_group.native_get",
            "lookup_group.decode_validate",
            "lookup_group.submit_to_collection",
        ] {
            let selected = stage_rows
                .iter()
                .filter(|row| {
                    row.get("mode").is_some_and(|name| name == mode.name)
                        && row.get("stage").is_some_and(|value| value == stage)
                })
                .collect::<Vec<_>>();
            if selected.is_empty() {
                continue;
            }
            let no_samples = selected
                .iter()
                .all(|row| number(row, "sample_count") == 0.0);
            let display = if no_samples {
                "N/A".to_owned()
            } else {
                format!(
                    "{:.3} / {:.3} / {:.3}",
                    median_field(&selected, "p50_ns") / 1e6,
                    median_field(&selected, "p95_ns") / 1e6,
                    median_field(&selected, "p99_ns") / 1e6,
                )
            };
            report.push_str(&format!(
                "| {} | {} | {} | {} | {} |\n",
                mode.name,
                stage,
                display,
                sample_count_range(&selected),
                field(selected[0], "scope"),
            ));
        }
    }
    report.push_str("\n## Every trial\n\nWAL and storage counters include foreground, projection, watermark, and GC writes. Background phase samples and client-end progress values are preserved in each linked pipeline artifact.\n\n| Trial | Mode | Rep | RPS | Request p50 / p95 / p99 (ms) | CPU cores | RSS (GiB) | Device read / write (GiB) | WAL (GiB) | Calls / keys / misses | Projection / GC backlog |\n|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
    for row in summary_rows {
        let (projection_backlog, gc_backlog) = integrated_client_end_backlogs(row)?;
        report.push_str(&format!(
            "| {} | {} | {} | {:.1} | {:.3} / {:.3} / {:.3} | {:.3} | {:.3} | {:.3} / {:.3} | {:.3} | {:.0} / {:.0} / {:.0} | {:.0} / {:.0} |\n",
            field(row, "trial_index"),
            field(row, "mode"),
            field(row, "repetition"),
            number(row, "client_rps"),
            number(row, "request_p50_ns") / 1e6,
            number(row, "request_p95_ns") / 1e6,
            number(row, "request_p99_ns") / 1e6,
            number(row, "cpu_core_equivalents"),
            number(row, "rss_bytes") / 1_073_741_824.0,
            number(row, "device_read_bytes") / 1_073_741_824.0,
            number(row, "device_write_bytes") / 1_073_741_824.0,
            number(row, "wal_bytes") / 1_073_741_824.0,
            number(row, "native_get_calls"),
            number(row, "keys_looked_up"),
            number(row, "misses"),
            projection_backlog,
            gc_backlog,
        ));
    }
    report.push_str("\n## Raw evidence\n\n");
    report.push_str(&format!(
        "- [run summary CSV]({relative_run}/ledger_index_lookup_summary.csv)\n- [all-trial stage percentile CSV]({relative_run}/ledger_index_lookup_stages.csv)\n- [trial manifest]({relative_run}/trial_manifest.csv)\n- [run metadata]({relative_run}/run_metadata.txt)\n"
    ));
    for row in summary_rows {
        let index = field(row, "trial_index");
        let trial_directory = field(row, "trial_directory");
        let stdout_log = field(row, "stdout_log");
        let stderr_log = field(row, "stderr_log");
        let trial_path = format!("{relative_run}/trials/{trial_directory}");
        report.push_str(&format!(
            "- [trial {index} index summary]({trial_path}/ledger_index_lookup_trial_summary.csv), [batches]({trial_path}/ledger_index_lookup_trial_batches.csv), [lookup stages]({trial_path}/ledger_index_lookup_trial_stages.csv), [pipeline summary]({trial_path}/ledger_pipeline_summary.csv), [pipeline stages]({trial_path}/ledger_pipeline_stages.csv), [background phases]({trial_path}/ledger_pipeline_background.csv), [actual lookup parameters]({trial_path}/ledger_pipeline_index_lookup.csv), [metadata]({trial_path}/ledger_index_lookup_trial_metadata.txt), [stdout]({relative_run}/trials/{trial_directory}/{stdout_log}), [stderr]({relative_run}/trials/{trial_directory}/{stderr_log})\n"
        ));
    }
    Ok(report)
}

fn integrated_client_end_backlogs(row: &BTreeMap<String, String>) -> Result<(u64, u64), String> {
    let trial_index = field(row, "trial_index");
    if trial_index.is_empty() {
        return Err("integrated report row is missing trial_index".to_owned());
    }
    let parse_sequence = |name: &str| -> Result<u64, String> {
        row.get(name)
            .ok_or_else(|| format!("trial {trial_index} is missing {name}"))?
            .parse::<u64>()
            .map_err(|error| format!("trial {trial_index} has invalid {name}: {error}"))
    };
    let latest = parse_sequence("latest_seq_near_client_end")?;
    let projected = parse_sequence("projected_seq_near_client_end")?;
    let gc_prefix = parse_sequence("gc_prefix_near_client_end")?;
    if projected > latest || gc_prefix > projected {
        return Err(format!(
            "trial {trial_index} has invalid client-end progress: latest={latest}, projected={projected}, gc_prefix={gc_prefix}"
        ));
    }
    Ok((latest - projected, latest - gc_prefix))
}

fn median_u64(values: &[u64]) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    if sorted.is_empty() {
        return 0.0;
    }
    let middle = sorted.len() / 2;
    if sorted.len() % 2 == 0 {
        (sorted[middle - 1] as f64 + sorted[middle] as f64) / 2.0
    } else {
        sorted[middle] as f64
    }
}

fn sample_count_range(rows: &[&BTreeMap<String, String>]) -> String {
    let mut counts = rows
        .iter()
        .filter_map(|row| row.get("sample_count")?.parse::<u64>().ok())
        .collect::<Vec<_>>();
    if counts.is_empty() {
        return "N/A".to_owned();
    }
    counts.sort_unstable();
    let minimum = counts[0];
    let maximum = *counts.last().unwrap();
    if minimum == maximum {
        minimum.to_string()
    } else {
        format!("{minimum}–{maximum}")
    }
}

fn field<'a>(row: &'a BTreeMap<String, String>, name: &str) -> &'a str {
    row.get(name).map(String::as_str).unwrap_or("")
}

fn number(row: &BTreeMap<String, String>, name: &str) -> f64 {
    row.get(name)
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(0.0)
}

fn median_field(rows: &[&BTreeMap<String, String>], name: &str) -> f64 {
    let mut values = rows
        .iter()
        .filter_map(|row| row.get(name)?.parse::<f64>().ok())
        .collect::<Vec<_>>();
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

fn build_schedule(cli: &Cli) -> Vec<Trial> {
    let mut schedule = Vec::with_capacity(cli.modes.len() * cli.repetitions.len());
    for repetition in cli.repetitions.iter().copied() {
        let rotate = (repetition - 1) % cli.modes.len();
        let mut order = cli.modes.clone();
        order.rotate_left(rotate);
        for mode in order {
            schedule.push(Trial {
                index: schedule.len() + 1,
                mode,
                repetition,
            });
        }
    }
    schedule
}

fn print_schedule(cli: &Cli) {
    let schedule = build_schedule(cli);
    println!(
        "VALIDATED profile={} run_kind={} trials={} users={} requests_per_user={} requests_per_trial={} sample_stride={} workers=4 partial_designation={}",
        cli.profile.name(),
        format!(
            "{}_{}",
            if cli.is_partial() { "partial" } else { "complete" },
            if cli.smoke { "smoke_matrix" } else { "full_matrix" }
        ),
        schedule.len(),
        if cli.smoke { SMOKE_USERS } else { FULL_USERS },
        REQUESTS_PER_USER,
        if cli.smoke {
            SMOKE_USERS * REQUESTS_PER_USER
        } else {
            FULL_USERS * REQUESTS_PER_USER
        },
        if cli.smoke { 1 } else { 64 },
        cli.is_partial(),
    );
    for trial in schedule {
        println!(
            "TRIAL index={} mode={} repetition={}",
            trial.index, trial.mode.name, trial.repetition
        );
    }
}

fn parse_modes(value: &str) -> Result<Vec<ModeChoice>, String> {
    let mut selected = Vec::new();
    for name in value.split(',') {
        let mode = parse_mode(name)?;
        if selected.contains(&mode) {
            return Err(format!("mode {name} was selected more than once"));
        }
        selected.push(mode);
    }
    Ok(selected)
}

fn parse_mode(value: &str) -> Result<ModeChoice, String> {
    MODES
        .iter()
        .copied()
        .find(|mode| mode.name == value)
        .ok_or_else(|| {
            format!(
                "invalid mode {value}; expected {}",
                MODES
                    .iter()
                    .map(|mode| mode.name)
                    .collect::<Vec<_>>()
                    .join(",")
            )
        })
}

fn index_lookup_metadata_values(mode: IndexLookupMode) -> (&'static str, String, String) {
    match mode {
        IndexLookupMode::PointGet => ("point_get", "NA".to_owned(), "NA".to_owned()),
        IndexLookupMode::WholeBatchMultiGet => {
            ("whole_batch_multiget", "NA".to_owned(), "1".to_owned())
        }
        IndexLookupMode::Chunked {
            group_size,
            max_in_flight,
        } => ("chunked", group_size.to_string(), max_in_flight.to_string()),
    }
}

fn parse_repetitions(value: &str) -> Result<Vec<usize>, String> {
    let mut selected = Vec::new();
    for item in value.split(',') {
        let repetition = item
            .parse::<usize>()
            .map_err(|error| format!("invalid repetition {item}: {error}"))?;
        if !FULL_REPETITIONS.contains(&repetition) {
            return Err(format!("repetitions must be in 1..=3; got {repetition}"));
        }
        if selected.contains(&repetition) {
            return Err(format!(
                "repetition {repetition} was selected more than once"
            ));
        }
        selected.push(repetition);
    }
    Ok(selected)
}

fn absolute_path(path: &Path) -> Result<PathBuf, String> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        std::env::current_dir()
            .map(|directory| directory.join(path))
            .map_err(|error| format!("cannot resolve working directory: {error}"))
    }
}

fn path_is_under_target(path: &Path) -> Result<bool, String> {
    if path
        .components()
        .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return Ok(false);
    }
    let path = canonicalize_existing_ancestor(&absolute_path(path)?)?;
    let target = canonicalize_existing_ancestor(&absolute_path(Path::new("target"))?)?;
    Ok(path.starts_with(target))
}

fn canonicalize_existing_ancestor(path: &Path) -> Result<PathBuf, String> {
    let mut ancestor = path.to_path_buf();
    let mut missing = Vec::new();
    while !ancestor.exists() {
        let name = ancestor.file_name().ok_or_else(|| {
            format!(
                "cannot find an existing ancestor for output path {}",
                path.display()
            )
        })?;
        missing.push(name.to_os_string());
        if !ancestor.pop() {
            return Err(format!(
                "cannot find an existing ancestor for output path {}",
                path.display()
            ));
        }
    }
    let mut resolved = fs::canonicalize(&ancestor).map_err(|error| {
        format!(
            "cannot resolve existing output path ancestor {}: {error}",
            ancestor.display()
        )
    })?;
    for component in missing.iter().rev() {
        resolved.push(component);
    }
    Ok(resolved)
}

fn print_help() {
    println!(
        "Tokio single-shard ledger transaction-index lookup benchmark\n\n\
Usage:\n  cargo bench --bench ledger_index_lookup_tokio\n  cargo bench --bench ledger_index_lookup_tokio -- --smoke\n\n\
  cargo bench --bench ledger_index_lookup_tokio -- --profile integrated_pipeline --report-only benches/data/ledger_pipeline_index_lookup/run-<ID>\n\n\
Options:\n  --profile PROFILE          foreground_persistence (default) or integrated_pipeline\n  --smoke                    Run the selected matrix at 200 users x 200 requests (40k/trial), stride 1\n  --validate-only            Validate and print the schedule without preflight or trial execution\n  --modes CSV                Filter modes; integrated_pipeline allows point_get,chunked_256_p4,chunked_256_p8\n  --repetitions CSV          Select repetitions from 1,2,3; partial selections are designated in metadata\n  --output-root PATH         Archive root; smoke output must remain under target/\n\n\
  --report-only RUN_DIR      Validate a complete full matrix in the selected profile's default archive and regenerate its canonical report; requires explicit --profile and rejects smoke, subsets, trial mode, validate-only, and output-root overrides\n\n\
Foreground defaults to six modes and three repetitions under benches/data/ledger_index_lookup. Integrated defaults to point_get,chunked_256_p4,chunked_256_p8 and repetitions 1,2,3 under benches/data/ledger_pipeline_index_lookup. Smoke output uses the matching target/ directory. Each repetition rotates selected mode order. Every trial runs in a fresh child process and owns a fresh RocksDB directory. Full trials use strict 3-second preflight observations, CPU busy <=10%, target-device busy <=5%, and pipeline memory/free-space reserves. No preflight threshold option is exposed."
    );
}
