#!/usr/bin/env python3
"""Build reviewable summaries from the checked-in gRPC kernel bench artifacts."""

from __future__ import annotations

import csv
import re
import statistics
from pathlib import Path


ROOT = Path(__file__).resolve().parent / "grpc_echo_kernel_data"
COHORTS = {
    "kernel_off_bracket_before": "kernel_off_bracket_before",
    "diagnostic_baseline": "diagnostic_baseline",
    "diagnostic_saturation": "diagnostic_saturation",
    "perf_stat_targeted": "perf_stat_targeted",
    "perf_stat_verbose_targeted": "perf_stat_verbose_targeted",
    "perf_record_targeted": "perf_record_targeted",
    "kernel_off_saturation": "kernel_off_saturation",
    "kernel_off_bracket_after": "kernel_off_bracket_after",
}


def run_dir_for(name: str) -> Path:
    candidates = sorted((ROOT / name).glob("run-*/results.csv"))
    if len(candidates) != 1:
        raise RuntimeError(f"expected one results.csv for {name}, found {candidates}")
    return candidates[0].parent


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as source:
        return list(csv.DictReader(source))


def write_csv(path: Path, rows: list[dict[str, object]], fields: list[str]) -> None:
    with path.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def log_values(path: Path) -> dict[str, str]:
    found: dict[str, str] = {}
    for line in path.read_text(errors="replace").splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        found[key] = value.strip()
    return found


def tcp_info_values(raw: str) -> dict[str, int]:
    values: dict[str, int] = {}
    for match in re.finditer(r"(?:^|\s)([A-Za-z_]+):([0-9]+)ms(?:\([^)]+\))?", raw):
        values[match.group(1)] = int(match.group(2))
    return values


def skmem_values(raw: str) -> dict[str, int]:
    match = re.search(r"skmem:\(([^)]*)\)", raw)
    if not match:
        return {}
    values: dict[str, int] = {}
    for item in match.group(1).split(","):
        field = re.fullmatch(r"([a-z]+)([0-9]+)", item)
        if field:
            values[field.group(1)] = int(field.group(2))
    return values


def parse_number(text: str) -> float | None:
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def main() -> None:
    output_dir = ROOT / "analysis"
    output_dir.mkdir(exist_ok=True)
    run_dirs = {name: run_dir_for(name) for name in COHORTS}

    result_rows: list[dict[str, object]] = []
    results_by_run: dict[Path, dict[str, dict[str, str]]] = {}
    for cohort, run_dir in run_dirs.items():
        rows = read_csv(run_dir / "results.csv")
        trial_logs = sorted(run_dir.glob("trial-*.diagnostic.log"))
        if len(rows) != len(trial_logs):
            raise RuntimeError(f"trial/results count mismatch in {run_dir}")
        results_by_run[run_dir] = {
            trial_log.name.removesuffix(".diagnostic.log"): row
            for trial_log, row in zip(trial_logs, rows)
        }
        for trial_log, row in zip(trial_logs, rows):
            result_rows.append(
                {
                    "cohort": cohort,
                    "run_id": run_dir.name,
                    "trial": trial_log.name.removesuffix(".diagnostic.log"),
                    "mode": row.get("mode", ""),
                    "payload_bytes": row.get("payload_bytes", ""),
                    "concurrency": row.get("concurrency", ""),
                    "channels": row.get("independent_channels", ""),
                    "repetition": row.get("repetition", ""),
                    "issued": row.get("issued", ""),
                    "completed": row.get("completed", ""),
                    "completed_rps": row.get("completed_rps", ""),
                    "e2e_p50_ns": row.get("e2e_p50_ns", ""),
                    "e2e_p95_ns": row.get("e2e_p95_ns", ""),
                    "e2e_p99_ns": row.get("e2e_p99_ns", ""),
                    "client_cpu_core_equiv": row.get("client_cpu_core_equiv", ""),
                    "server_cpu_core_equiv": row.get("server_cpu_core_equiv", ""),
                    "client_cpu_ns": row.get("client_cpu_ns", ""),
                    "server_cpu_ns": row.get("server_cpu_ns", ""),
                    "server_cpu_start_offset_ns": row.get("server_cpu_start_offset_ns", ""),
                    "server_cpu_end_offset_ns": row.get("server_cpu_end_offset_ns", ""),
                    "legacy_tcp_sampled_max_send_q": row.get("tcp_sampled_max_send_q", ""),
                    "legacy_tcp_sampled_max_recv_q": row.get("tcp_sampled_max_recv_q", ""),
                    "measurement_loopback_rx_bytes": row.get("measurement_loopback_rx_bytes", ""),
                    "measurement_loopback_tx_bytes": row.get("measurement_loopback_tx_bytes", ""),
                    "measurement_loopback_rx_packets": row.get("measurement_loopback_rx_packets", ""),
                    "measurement_loopback_tx_packets": row.get("measurement_loopback_tx_packets", ""),
                    "per_core_cpu_softirq_delta": row.get("per_core_cpu_softirq_delta", ""),
                }
            )
    write_csv(
        output_dir / "trial_summaries.csv",
        result_rows,
        list(result_rows[0]),
    )

    metric_names = (
        "completed_rps",
        "e2e_p50_ns",
        "e2e_p95_ns",
        "e2e_p99_ns",
        "client_cpu_core_equiv",
        "server_cpu_core_equiv",
        "legacy_tcp_sampled_max_send_q",
        "legacy_tcp_sampled_max_recv_q",
        "measurement_loopback_rx_bytes",
        "measurement_loopback_tx_bytes",
        "measurement_loopback_rx_packets",
        "measurement_loopback_tx_packets",
    )
    grouped_results: dict[tuple[str, str, str, str], list[dict[str, object]]] = {}
    for row in result_rows:
        key = (str(row["cohort"]), str(row["mode"]), str(row["payload_bytes"]), str(row["channels"]))
        grouped_results.setdefault(key, []).append(row)
    group_rows: list[dict[str, object]] = []
    for key, group in sorted(grouped_results.items()):
        aggregate: dict[str, object] = {
            "cohort": key[0],
            "mode": key[1],
            "payload_bytes": key[2],
            "channels": key[3],
            "repeats": len(group),
        }
        for metric in metric_names:
            numbers = [parse_number(str(row.get(metric, ""))) for row in group]
            numbers = [number for number in numbers if number is not None]
            if numbers:
                aggregate[f"median_{metric}"] = f"{statistics.median(numbers):.6f}"
                aggregate[f"min_{metric}"] = f"{min(numbers):.6f}"
                aggregate[f"max_{metric}"] = f"{max(numbers):.6f}"
        group_rows.append(aggregate)
    write_csv(output_dir / "group_medians.csv", group_rows, list(group_rows[0]))

    per_core_rows: list[dict[str, object]] = []
    for trial in result_rows:
        source = results_by_run[run_dirs[str(trial["cohort"])]]
        result = source[str(trial["trial"])]
        raw = result.get("per_core_cpu_softirq_delta", "")
        for match in re.finditer(
            r"cpu(\d+):busy_pct=([0-9.]+),softirq_pct=([0-9.]+),ticks=(\d+)", raw
        ):
            per_core_rows.append(
                {
                    "cohort": trial["cohort"],
                    "trial": trial["trial"],
                    "payload_bytes": trial["payload_bytes"],
                    "mode": trial["mode"],
                    "channels": trial["channels"],
                    "repetition": trial["repetition"],
                    "cpu": match.group(1),
                    "busy_pct": match.group(2),
                    "softirq_pct": match.group(3),
                    "ticks": match.group(4),
                }
            )
    if per_core_rows:
        write_csv(output_dir / "per_core_cpu.csv", per_core_rows, list(per_core_rows[0]))

    hottest_rows: list[dict[str, object]] = []
    trial_keys = sorted({(str(row["cohort"]), str(row["trial"])) for row in result_rows})
    for cohort, trial_id in trial_keys:
        trial = next(
            row
            for row in result_rows
            if row["cohort"] == cohort and row["trial"] == trial_id
        )
        cores = [
            row
            for row in per_core_rows
            if row["cohort"] == cohort and row["trial"] == trial_id
        ]
        if not cores:
            continue
        hottest_busy = max(cores, key=lambda row: float(str(row["busy_pct"])))
        hottest_softirq = max(cores, key=lambda row: float(str(row["softirq_pct"])))
        hottest_rows.append(
            {
                "cohort": trial["cohort"],
                "trial": trial_id,
                "payload_bytes": trial["payload_bytes"],
                "mode": trial["mode"],
                "channels": trial["channels"],
                "repetition": trial["repetition"],
                "max_busy_cpu": hottest_busy["cpu"],
                "max_busy_pct": hottest_busy["busy_pct"],
                "max_softirq_cpu": hottest_softirq["cpu"],
                "max_softirq_pct": hottest_softirq["softirq_pct"],
            }
        )
    if hottest_rows:
        write_csv(output_dir / "per_core_hottest.csv", hottest_rows, list(hottest_rows[0]))

    time_rows: list[dict[str, object]] = []
    monitor_rows: list[dict[str, object]] = []
    socket_snapshot_rows: list[dict[str, object]] = []
    queue_occupancy_rows: list[dict[str, object]] = []
    tcp_counter_rows: list[dict[str, object]] = []
    for cohort in ("diagnostic_baseline", "diagnostic_saturation"):
        run_dir = run_dirs[cohort]
        for csv_path in sorted(run_dir.glob("*.kernel_tcp.csv")):
            trial = csv_path.name.removesuffix(".kernel_tcp.csv")
            result = results_by_run[run_dir][trial]
            snapshots = read_csv(csv_path)
            by_endpoint: dict[tuple[str, str], list[dict[str, str]]] = {}
            for sample in snapshots:
                if sample.get("record_type") != "snapshot":
                    continue
                key = (sample.get("endpoint_role", ""), sample.get("flow_id", ""))
                by_endpoint.setdefault(key, []).append(sample)
            occupancy_by_role: dict[str, dict[str, int]] = {}
            for (role, flow_id), samples in sorted(by_endpoint.items()):
                in_phase = [
                    sample
                    for sample in samples
                    if sample.get("inside_measurement") == "true"
                    and sample.get("sample_kind") == "periodic"
                ]
                boundary = [
                    sample
                    for sample in samples
                    if sample.get("sample_kind") == "phase_end_boundary"
                ]
                if not in_phase or not boundary:
                    continue
                first = min(in_phase, key=lambda sample: int(sample["capture_start_mono_ns"]))
                last = max(boundary, key=lambda sample: int(sample["capture_end_mono_ns"]))
                elapsed_ns = int(last["capture_end_mono_ns"]) - int(first["capture_start_mono_ns"])
                first_info = tcp_info_values(first.get("tcp_info_raw", ""))
                last_info = tcp_info_values(last.get("tcp_info_raw", ""))
                dense_samples = [
                    sample
                    for sample in samples
                    if sample.get("sample_kind") == "periodic"
                    and sample.get("inside_measurement") == "true"
                ]
                if not dense_samples:
                    continue
                role_counts = occupancy_by_role.setdefault(
                    role,
                    {"sample_count": 0, "send_nonzero": 0, "recv_nonzero": 0},
                )
                role_counts["sample_count"] += len(dense_samples)
                role_counts["send_nonzero"] += sum(
                    int(sample.get("send_q_bytes", "0")) > 0 for sample in dense_samples
                )
                role_counts["recv_nonzero"] += sum(
                    int(sample.get("recv_q_bytes", "0")) > 0 for sample in dense_samples
                )
                parsed_skmem = [skmem_values(sample.get("skmem_raw", "")) for sample in dense_samples]
                skmem_keys = {field for values in parsed_skmem for field in values}
                skmem_max = {
                    field: max(values[field] for values in parsed_skmem if field in values)
                    for field in skmem_keys
                    if any(field in values for values in parsed_skmem)
                }
                socket_snapshot_rows.append(
                    {
                        "cohort": cohort,
                        "run_id": run_dir.name,
                        "trial": trial,
                        "payload_bytes": result.get("payload_bytes", ""),
                        "channels": result.get("independent_channels", ""),
                        "repetition": result.get("repetition", ""),
                        "endpoint_role": role,
                        "flow_id": flow_id,
                        "direction_sent": "client_to_server" if role == "client" else "server_to_client",
                        "periodic_samples_inside_phase": len(dense_samples),
                        "snapshot_max_send_q_bytes": max(int(sample.get("send_q_bytes", "0")) for sample in dense_samples),
                        "snapshot_max_recv_q_bytes": max(int(sample.get("recv_q_bytes", "0")) for sample in dense_samples),
                        **{f"snapshot_max_skmem_{field}_bytes": value for field, value in skmem_max.items()},
                    }
                )
                for field in ("busy", "rwnd_limited", "sndbuf_limited"):
                    if field not in first_info or field not in last_info:
                        continue
                    delta_ms = max(0, last_info[field] - first_info[field])
                    observed_ms = elapsed_ns / 1_000_000
                    time_rows.append(
                        {
                            "cohort": cohort,
                            "run_id": run_dir.name,
                            "trial": trial,
                            "mode": result.get("mode", ""),
                            "payload_bytes": result.get("payload_bytes", ""),
                            "concurrency": result.get("concurrency", ""),
                            "channels": result.get("independent_channels", ""),
                            "repetition": result.get("repetition", ""),
                            "endpoint_role": role,
                            "flow_id": flow_id,
                            "direction_sent": "client_to_server" if role == "client" else "server_to_client",
                            "counter_name": field + "_ms",
                            "counter_start_ms": first_info[field],
                            "counter_end_ms": last_info[field],
                            "counter_delta_ms": delta_ms,
                            "observed_window_ms": f"{observed_ms:.3f}",
                            "delta_pct_of_observed_window": f"{100 * delta_ms / observed_ms:.5f}" if observed_ms else "",
                            "first_sample_phase_start_offset_ns": first.get("phase_start_offset_ns", ""),
                            "boundary_phase_end_offset_ns": last.get("phase_end_offset_ns", ""),
                            "source_kernel_tcp_csv": str(csv_path.relative_to(ROOT.parent.parent)),
                        }
                    )
            for sample in snapshots:
                if sample.get("record_type") != "counter_delta":
                    continue
                tcp_counter_rows.append(
                    {
                        "cohort": cohort,
                        "run_id": run_dir.name,
                        "trial": trial,
                        "payload_bytes": result.get("payload_bytes", ""),
                        "channels": result.get("independent_channels", ""),
                        "repetition": result.get("repetition", ""),
                        "endpoint_role": sample.get("endpoint_role", ""),
                        "flow_id": sample.get("flow_id", ""),
                        "direction_sent": sample.get("send_data_direction", ""),
                        "counter_name": sample.get("counter_name", ""),
                        "counter_start": sample.get("counter_start", ""),
                        "counter_end": sample.get("counter_end", ""),
                        "counter_delta": sample.get("counter_delta", ""),
                        "boundary_phase_end_offset_ns": sample.get("phase_end_offset_ns", ""),
                        "source_kernel_tcp_csv": str(csv_path.relative_to(ROOT.parent.parent)),
                    }
                )
            for role, counts in occupancy_by_role.items():
                total = counts["sample_count"]
                queue_occupancy_rows.append(
                    {
                        "cohort": cohort,
                        "run_id": run_dir.name,
                        "trial": trial,
                        "payload_bytes": result.get("payload_bytes", ""),
                        "channels": result.get("independent_channels", ""),
                        "repetition": result.get("repetition", ""),
                        "endpoint_role": role,
                        "endpoint_snapshots": total,
                        "send_q_nonzero_snapshots": counts["send_nonzero"],
                        "send_q_nonzero_pct": f"{100 * counts['send_nonzero'] / total:.4f}" if total else "",
                        "recv_q_nonzero_snapshots": counts["recv_nonzero"],
                        "recv_q_nonzero_pct": f"{100 * counts['recv_nonzero'] / total:.4f}" if total else "",
                    }
                )

            diagnostic_path = run_dir / f"{trial}.diagnostic.log"
            diag = log_values(diagnostic_path)
            monitor = re.search(
                r"periodic_samples=(\d+) periodic_samples_inside_phase=(\d+) sample_errors=(\d+) "
                r"ss_monitor_periodic_wall_ns=(\d+) ss_monitor_parent_cpu_ns=(\d+) "
                r"ss_monitor_child_cpu_ns=(\d+) ss_monitor_max_wall_ns=(\d+) "
                r"ss_monitor_periodic_wall_pct=([0-9.]+) ss_boundary_wall_ns=(\d+) "
                r"ss_boundary_parent_cpu_ns=(\d+) ss_boundary_child_cpu_ns=(\d+)",
                diag.get("kernel_sample_status", "") + " " + " ".join(
                    f"{key}={value}" for key, value in diag.items()
                ),
            )
            if monitor:
                groups = monitor.groups()
                measured_ns = parse_number(result.get("issue_window_s", ""))
                duty = float(groups[5]) / (measured_ns * 1e9) * 100 if measured_ns else None
                monitor_rows.append(
                    {
                        "cohort": cohort,
                        "run_id": run_dir.name,
                        "trial": trial,
                        "payload_bytes": result.get("payload_bytes", ""),
                        "channels": result.get("independent_channels", ""),
                        "repetition": result.get("repetition", ""),
                        "periodic_samples": groups[0],
                        "periodic_samples_inside_phase": groups[1],
                        "sample_errors": groups[2],
                        "ss_periodic_wall_ns": groups[3],
                        "ss_parent_cpu_ns": groups[4],
                        "ss_child_cpu_ns": groups[5],
                        "ss_child_core_equiv": f"{float(groups[5]) / (measured_ns * 1e9):.6f}" if measured_ns else "",
                        "ss_monitor_max_wall_ns": groups[6],
                        "wall_duty_pct": groups[7],
                        "boundary_wall_ns": groups[8],
                        "boundary_parent_cpu_ns": groups[9],
                        "boundary_child_cpu_ns": groups[10],
                        "server_send_q_snapshot_max": diag.get("kernel_snapshot_max_server_send_q", "").split()[0],
                        "server_recv_q_snapshot_max": re.search(r"kernel_snapshot_max_server_recv_q=(\d+)", " ".join(diag.values())).group(1) if re.search(r"kernel_snapshot_max_server_recv_q=(\d+)", " ".join(diag.values())) else "",
                        "client_send_q_snapshot_max": re.search(r"kernel_snapshot_max_client_send_q=(\d+)", " ".join(diag.values())).group(1) if re.search(r"kernel_snapshot_max_client_send_q=(\d+)", " ".join(diag.values())) else "",
                        "client_recv_q_snapshot_max": re.search(r"kernel_snapshot_max_client_recv_q=(\d+)", " ".join(diag.values())).group(1) if re.search(r"kernel_snapshot_max_client_recv_q=(\d+)", " ".join(diag.values())) else "",
                    }
                )
    if time_rows:
        write_csv(output_dir / "tcp_duration_deltas.csv", time_rows, list(time_rows[0]))
    if socket_snapshot_rows:
        write_csv(output_dir / "tcp_snapshot_endpoint_summary.csv", socket_snapshot_rows, list(socket_snapshot_rows[0]))
        snapshot_keys = (
            "snapshot_max_send_q_bytes",
            "snapshot_max_recv_q_bytes",
            "snapshot_max_skmem_r_bytes",
            "snapshot_max_skmem_w_bytes",
            "snapshot_max_skmem_rb_bytes",
            "snapshot_max_skmem_tb_bytes",
            "snapshot_max_skmem_d_bytes",
        )
        per_trial_role: dict[tuple[str, str, str, str, str, str], list[float]] = {}
        snapshot_trial_groups: dict[tuple[str, str, str, str, str], list[float]] = {}
        for row in socket_snapshot_rows:
            for metric in snapshot_keys:
                value = parse_number(str(row.get(metric, "")))
                if value is None:
                    continue
                key = (
                    str(row["cohort"]),
                    str(row["trial"]),
                    str(row["payload_bytes"]),
                    str(row["channels"]),
                    str(row["endpoint_role"]),
                    metric,
                )
                per_trial_role.setdefault(key, []).append(value)
        for (cohort, _trial, payload, channels, role, metric), values in per_trial_role.items():
            key = (cohort, payload, channels, role, metric)
            snapshot_trial_groups.setdefault(key, []).append(max(values))
        snapshot_group_rows = [
            {
                "cohort": key[0],
                "payload_bytes": key[1],
                "channels": key[2],
                "endpoint_role": key[3],
                "metric": key[4],
                "trial_count": len(values),
                "median_trial_max": f"{statistics.median(values):.3f}",
                "min_trial_max": f"{min(values):.3f}",
                "max_trial_max": f"{max(values):.3f}",
            }
            for key, values in sorted(snapshot_trial_groups.items())
        ]
        if snapshot_group_rows:
            write_csv(output_dir / "tcp_snapshot_group_medians.csv", snapshot_group_rows, list(snapshot_group_rows[0]))
    if queue_occupancy_rows:
        write_csv(output_dir / "tcp_queue_sample_occupancy.csv", queue_occupancy_rows, list(queue_occupancy_rows[0]))
    if tcp_counter_rows:
        write_csv(output_dir / "tcp_counter_deltas.csv", tcp_counter_rows, list(tcp_counter_rows[0]))
    if monitor_rows:
        write_csv(output_dir / "ss_monitor_cost.csv", monitor_rows, list(monitor_rows[0]))

    perf_rows: list[dict[str, object]] = []
    for cohort in ("perf_stat_targeted", "perf_stat_verbose_targeted"):
        run_dir = run_dirs[cohort]
        for stat_path in sorted(run_dir.glob("*.perf-stat.txt")):
            name = stat_path.name
            role = "client" if ".client." in name else "server"
            trial = name.split(f".{role}.perf-stat.txt")[0]
            result = results_by_run[run_dir][trial]
            content = stat_path.read_text(errors="replace")
            stderr_path = stat_path.with_name(stat_path.name.replace(".perf-stat.txt", ".perf-stat.stderr.log"))
            stderr_content = stderr_path.read_text(errors="replace") if stderr_path.exists() else ""
            status_flags = []
            lower = (content + "\n" + stderr_content).lower()
            for marker in ("<not counted>", "<not supported>", " n/a", "not available"):
                if marker in lower:
                    status_flags.append(marker.strip())
            timing: dict[str, tuple[int, int]] = {}
            for line in (content + "\n" + stderr_content).splitlines():
                match = re.match(r"^\s*([A-Za-z_:.-]+):\s+([0-9,]+)\s+([0-9,]+)\s+([0-9,]+)\s*$", line)
                if not match:
                    continue
                event = match.group(1).removesuffix(":")
                timing[event] = (
                    int(match.group(3).replace(",", "")),
                    int(match.group(4).replace(",", "")),
                )
            for line in content.splitlines():
                match = re.match(r"^\s*([0-9][0-9,]*(?:\.[0-9]+)?)\s+([A-Za-z_:.-]+)(?:\s|$)", line)
                if not match:
                    continue
                value = float(match.group(1).replace(",", ""))
                event = match.group(2)
                enabled_running = timing.get(event)
                enabled_ns = enabled_running[0] if enabled_running else None
                running_ns = enabled_running[1] if enabled_running else None
                running_pct = running_ns / enabled_ns * 100 if enabled_ns else None
                multiplex = (
                    "unsupported_or_missing" if status_flags else
                    "multiplexed_or_not_fully_running" if running_pct is not None and running_pct < 99.999 else
                    "fully_running_100pct" if running_pct is not None else
                    "running_pct_not_reported"
                )
                perf_rows.append(
                    {
                        "cohort": cohort,
                        "run_id": run_dir.name,
                        "trial": trial,
                        "role": role,
                        "payload_bytes": result.get("payload_bytes", ""),
                        "channels": result.get("independent_channels", ""),
                        "repetition": result.get("repetition", ""),
                        "event": event,
                        "value": f"{value:.0f}" if value.is_integer() else f"{value:.4f}",
                        "value_per_completed_rpc": f"{value / float(result['completed']):.8f}" if parse_number(result.get("completed", "")) else "",
                        "value_per_second": f"{value / float(result['completion_window_s']):.6f}" if parse_number(result.get("completion_window_s", "")) else "",
                        "time_enabled_ns": enabled_ns if enabled_ns is not None else "",
                        "time_running_ns": running_ns if running_ns is not None else "",
                        "time_running_pct": f"{running_pct:.6f}" if running_pct is not None else "",
                        "multiplexing_note": multiplex,
                        "source_perf_stat": str(stat_path.relative_to(ROOT.parent.parent)),
                    }
                )
    if perf_rows:
        write_csv(output_dir / "perf_stat_events.csv", perf_rows, list(perf_rows[0]))
        perf_cases: dict[tuple[str, str, str, str, str], dict[str, dict[str, str]]] = {}
        for row in perf_rows:
            if row["cohort"] != "perf_stat_verbose_targeted":
                continue
            key = (
                str(row["cohort"]),
                str(row["trial"]),
                str(row["payload_bytes"]),
                str(row["channels"]),
                str(row["role"]),
            )
            perf_cases.setdefault(key, {})[str(row["event"])] = row  # type: ignore[assignment]
        case_metrics: list[dict[str, object]] = []
        for (cohort, trial, payload, channels, role), events in sorted(perf_cases.items()):
            user_cycles = parse_number(events.get("cycles:u", {}).get("value", ""))
            kernel_cycles = parse_number(events.get("cycles:k", {}).get("value", ""))
            cycle_total = (user_cycles or 0) + (kernel_cycles or 0)
            run_pcts = [
                parse_number(event.get("time_running_pct", ""))
                for event in events.values()
            ]
            run_pcts = [value for value in run_pcts if value is not None]
            case_metrics.append(
                {
                    "cohort": cohort,
                    "trial": trial,
                    "payload_bytes": payload,
                    "channels": channels,
                    "endpoint_role": role,
                    "task_clock_core_equiv": f"{float(events['task-clock']['value_per_second']) / 1e9:.6f}" if "task-clock" in events else "",
                    "context_switches_per_second": events.get("context-switches", {}).get("value_per_second", ""),
                    "context_switches_per_rpc": events.get("context-switches", {}).get("value_per_completed_rpc", ""),
                    "cpu_migrations_per_second": events.get("cpu-migrations", {}).get("value_per_second", ""),
                    "cycles_user": user_cycles if user_cycles is not None else "",
                    "cycles_kernel": kernel_cycles if kernel_cycles is not None else "",
                    "cycles_kernel_pct_of_user_plus_kernel": f"{kernel_cycles / cycle_total * 100:.6f}" if cycle_total else "",
                    "min_event_time_running_pct": f"{min(run_pcts):.6f}" if run_pcts else "",
                    "max_event_time_running_pct": f"{max(run_pcts):.6f}" if run_pcts else "",
                }
            )
        case_groups: dict[tuple[str, str, str, str], list[dict[str, object]]] = {}
        for row in case_metrics:
            key = (
                str(row["cohort"]),
                str(row["payload_bytes"]),
                str(row["channels"]),
                str(row["endpoint_role"]),
            )
            case_groups.setdefault(key, []).append(row)
        perf_group_rows: list[dict[str, object]] = []
        group_numeric_fields = (
            "task_clock_core_equiv",
            "context_switches_per_second",
            "context_switches_per_rpc",
            "cpu_migrations_per_second",
            "cycles_kernel_pct_of_user_plus_kernel",
            "min_event_time_running_pct",
            "max_event_time_running_pct",
        )
        for key, group in sorted(case_groups.items()):
            item: dict[str, object] = {
                "cohort": key[0],
                "payload_bytes": key[1],
                "channels": key[2],
                "endpoint_role": key[3],
                "repeats": len(group),
            }
            for metric in group_numeric_fields:
                values = [parse_number(str(row.get(metric, ""))) for row in group]
                values = [value for value in values if value is not None]
                if values:
                    item[f"median_{metric}"] = f"{statistics.median(values):.6f}"
                    item[f"min_{metric}"] = f"{min(values):.6f}"
                    item[f"max_{metric}"] = f"{max(values):.6f}"
            perf_group_rows.append(item)
        if perf_group_rows:
            write_csv(output_dir / "perf_stat_group_medians.csv", perf_group_rows, list(perf_group_rows[0]))

    tcp_groups: dict[tuple[str, str, str, str, str], list[float]] = {}
    for row in time_rows:
        value = parse_number(str(row.get("counter_delta_ms", "")))
        if value is None:
            continue
        key = (
            str(row["cohort"]),
            str(row["payload_bytes"]),
            str(row["channels"]),
            str(row["endpoint_role"]),
            str(row["counter_name"]),
        )
        tcp_groups.setdefault(key, []).append(value)
    tcp_group_rows = [
        {
            "cohort": key[0],
            "payload_bytes": key[1],
            "channels": key[2],
            "endpoint_role": key[3],
            "counter_name": key[4],
            "endpoint_flow_rows": len(values),
            "median_delta_ms": f"{statistics.median(values):.3f}",
            "min_delta_ms": f"{min(values):.3f}",
            "max_delta_ms": f"{max(values):.3f}",
        }
        for key, values in sorted(tcp_groups.items())
    ]
    if tcp_group_rows:
        write_csv(output_dir / "tcp_duration_group_medians.csv", tcp_group_rows, list(tcp_group_rows[0]))

    trial_counter_sums: dict[tuple[str, str, str, str, str, str], int] = {}
    for row in tcp_counter_rows:
        counter = str(row["counter_name"])
        delta = parse_number(str(row["counter_delta"]))
        if delta is None:
            continue
        key = (
            str(row["cohort"]),
            str(row["trial"]),
            str(row["payload_bytes"]),
            str(row["channels"]),
            str(row["endpoint_role"]),
            counter,
        )
        trial_counter_sums[key] = trial_counter_sums.get(key, 0) + int(delta)
    by_counter_group: dict[tuple[str, str, str, str, str], list[int]] = {}
    for (cohort, trial, payload, channels, role, counter), delta in trial_counter_sums.items():
        key = (cohort, payload, channels, role, counter)
        by_counter_group.setdefault(key, []).append(delta)
    tcp_counter_group_rows = [
        {
            "cohort": key[0],
            "payload_bytes": key[1],
            "channels": key[2],
            "endpoint_role": key[3],
            "counter_name": key[4],
            "trial_count": len(values),
            "median_trial_sum_delta": f"{statistics.median(values):.0f}",
            "min_trial_sum_delta": min(values),
            "max_trial_sum_delta": max(values),
        }
        for key, values in sorted(by_counter_group.items())
    ]
    if tcp_counter_group_rows:
        write_csv(output_dir / "tcp_counter_group_medians.csv", tcp_counter_group_rows, list(tcp_counter_group_rows[0]))

    with (output_dir / "manifest.txt").open("w") as output:
        for cohort, run_dir in run_dirs.items():
            output.write(f"{cohort}={run_dir.relative_to(ROOT.parent.parent)}\n")

    print(f"analysis_dir={output_dir}")
    print(f"trial_summaries={len(result_rows)}")
    print(f"tcp_duration_delta_rows={len(time_rows)}")
    print(f"ss_monitor_cost_rows={len(monitor_rows)}")
    print(f"perf_stat_event_rows={len(perf_rows)}")


if __name__ == "__main__":
    main()
