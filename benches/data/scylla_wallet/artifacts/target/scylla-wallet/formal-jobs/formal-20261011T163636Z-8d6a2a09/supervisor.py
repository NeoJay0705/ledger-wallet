#!/usr/bin/env python3
import datetime, hashlib, json, os, pathlib, subprocess, sys, tempfile, time
job = pathlib.Path(__file__).resolve().parent
launch_path = job / "launch.json"
repo = pathlib.Path("/home/neojhou/repos/ledger-wallet")
RUNS = repo / "target/scylla-wallet/runs"
def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()
def proc_start_ticks(pid):
    raw = pathlib.Path(f"/proc/{pid}/stat").read_text()
    tail = raw[raw.rfind(")") + 2:].split()
    return int(tail[19])
def atomic_json(path, value):
    fd, tmp = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=job)
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(value, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, path)
    finally:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass
def sha_file(path):
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()
def source_hashes_match(launch):
    return all(sha_file(repo / rel) == digest for rel, digest in launch["source_sha256"].items())
def new_run_dirs(launch):
    baseline = set(launch["baseline_run_dirs"])
    return sorted(p for p in RUNS.iterdir() if p.is_dir() and p.name not in baseline)
def summarize_trial(trial):
    out = {}
    before = trial / "trial-outcome-before-cleanup.json"
    result = trial / "runner-result.json"
    for path in (before, result):
        if path.exists():
            try:
                d = json.loads(path.read_text())
                for k in ("status", "error", "validation", "metrics_valid", "rps",
                          "p99_ms", "pool_connections", "driver_query_delta",
                          "formal_status", "trial_id", "trial_order", "started_utc"):
                    if k in d:
                        out[k] = d[k]
            except Exception as exc:
                out["summary_parse_error"] = str(exc)
            break
    events = trial / "service/events.jsonl"
    if events.exists():
        try:
            lines = events.read_text(errors="replace").splitlines()
            if lines:
                out["last_service_event"] = json.loads(lines[-1])
        except Exception:
            pass
    for marker in ("trial-config.json", "cluster.json", "aio-budget.json",
                   "required-metrics-validation.json", "cleanup.json"):
        if (trial / marker).exists():
            out.setdefault("evidence_files", []).append(marker)
    return out
launch = json.loads(launch_path.read_text())
launch["supervisor_pid"] = os.getpid()
launch["supervisor_proc_start_ticks"] = proc_start_ticks(os.getpid())
launch["supervisor_started_utc"] = utc()
launch["supervisor_status"] = "starting"
atomic_json(launch_path, launch)
if not source_hashes_match(launch):
    launch["supervisor_status"] = "refused_source_drift_before_launch"
    launch["source_drift_utc"] = utc()
    atomic_json(launch_path, launch)
    atomic_json(job / "result.json", {
        "exitcode": None, "launch_error": "source hashes changed before runner launch",
        "started_utc": launch["supervisor_started_utc"], "ended_utc": utc()
    })
    sys.exit(125)
started = utc()
stdout = (job / "runner.stdout.log").open("ab", buffering=0)
stderr = (job / "runner.stderr.log").open("ab", buffering=0)
try:
    proc = subprocess.Popen(
        launch["argv"], cwd=repo, stdin=subprocess.DEVNULL,
        stdout=stdout, stderr=stderr, close_fds=True, start_new_session=True
    )
except Exception as exc:
    stdout.close()
    stderr.close()
    atomic_json(job / "result.json", {
        "exitcode": None, "launch_error": repr(exc), "started_utc": started, "ended_utc": utc()
    })
    sys.exit(126)
launch["runner_pid"] = proc.pid
launch["runner_proc_start_ticks"] = proc_start_ticks(proc.pid)
launch["runner_started_utc"] = utc()
launch["supervisor_status"] = "runner_active"
atomic_json(launch_path, launch)
latest_phase = None
while proc.poll() is None:
    created = new_run_dirs(launch)
    if len(created) == 1:
        launch["run_dir"] = str(created[0])
    elif created:
        launch["new_run_dirs_observed"] = [str(p) for p in created]
    progress = {
        "captured_utc": utc(),
        "supervisor_pid": os.getpid(),
        "runner_pid": proc.pid,
        "runner_returncode": proc.poll(),
        "run_dir": launch.get("run_dir"),
        "source_hashes_match": source_hashes_match(launch),
    }
    run_dir = pathlib.Path(launch["run_dir"]) if launch.get("run_dir") else None
    if run_dir and run_dir.exists():
        rr = run_dir / "run-result.json"
        pf = run_dir / "formal-preflight.json"
        for label, path in (("run_result", rr), ("formal_preflight", pf)):
            if path.exists():
                try:
                    data = json.loads(path.read_text())
                    if label == "formal_preflight":
                        host = data.get("host_inventory", {})
                        progress["preflight"] = {
                            "status": data.get("status"),
                            "passed": data.get("passed"),
                            "required_disk_bytes": data.get("required_disk_bytes"),
                            "disk_available_bytes": host.get("disk_available_bytes"),
                            "backing_device": host.get("backing_device"),
                            "cpu_samples": data.get("cpu_samples"),
                            "device_samples": data.get("device_samples"),
                            "memory_samples": data.get("memory_samples"),
                        }
                    else:
                        progress["run_result"] = {
                            k: data.get(k) for k in ("mode", "formal_status", "actual_trial_count",
                                "expected_case_count", "formal_complete", "blocked_error")
                        }
                except Exception as exc:
                    progress[label + "_parse_error"] = str(exc)
        trials = run_dir / "trials"
        if trials.exists():
            trial_dirs = sorted(p for p in trials.iterdir() if p.is_dir())
            progress["trial_dirs_seen"] = len(trial_dirs)
            if trial_dirs:
                last = trial_dirs[-1]
                progress["latest_trial_dir"] = str(last)
                progress["latest_trial"] = summarize_trial(last)
    atomic_json(job / "progress.json", progress)
    atomic_json(launch_path, launch)
    time.sleep(3)
rc = proc.wait()
ended = utc()
launch["supervisor_status"] = "runner_exited"
launch["runner_exitcode"] = rc
launch["runner_ended_utc"] = ended
created = new_run_dirs(launch)
if len(created) == 1:
    launch["run_dir"] = str(created[0])
elif created:
    launch["new_run_dirs_observed"] = [str(p) for p in created]
atomic_json(launch_path, launch)
run_summary = {}
if launch.get("run_dir"):
    rr = pathlib.Path(launch["run_dir"]) / "run-result.json"
    if rr.exists():
        try:
            d = json.loads(rr.read_text())
            run_summary = {k: d.get(k) for k in ("mode", "formal_status", "formal_complete",
                "actual_trial_count", "expected_case_count", "blocked_error", "missing_trial_orders")}
        except Exception as exc:
            run_summary = {"parse_error": str(exc)}
atomic_json(job / "result.json", {
    "exitcode": rc, "started_utc": started, "ended_utc": ended,
    "supervisor_pid": os.getpid(), "supervisor_proc_start_ticks": launch["supervisor_proc_start_ticks"],
    "runner_pid": proc.pid, "runner_proc_start_ticks": launch["runner_proc_start_ticks"],
    "run_dir": launch.get("run_dir"), "run_summary": run_summary,
})
stdout.close()
stderr.close()
