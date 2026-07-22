#!/usr/bin/env python3
"""Run the adapted FAST'25 Fig.17 LevelDB/YCSB-shaped comparison."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import math
import pathlib
import re
import shlex
import statistics
import subprocess

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import Patch  # noqa: E402

from run_reproduction import Case, ROOT, Runner, SEED, append_manifest


PHASES = ("load", "a", "b", "c", "d", "e", "f")
FIELDS = [
    "figure", "phase", "version", "scale", "repeat", "status",
    "exit_code", "operations", "records", "value_size",
    "write_buffer_size", "sync", "reads", "writes", "scans",
    "seconds", "ops_per_s", "failure_reason", "raw_dir", "commit",
    "driver",
]


def parse_result(path: pathlib.Path) -> dict[str, str]:
    text = path.read_text(encoding="utf-8", errors="replace")
    matches = re.findall(r"^orchfs_ycsb_result\s+(.+)$", text, re.MULTILINE)
    if not matches:
        errors = re.findall(r"^orchfs_ycsb_error\s+(.+)$", text, re.MULTILINE)
        if errors:
            return {"failure_reason": errors[-1]}
        return {"failure_reason": "missing-result"}
    parsed: dict[str, str] = {}
    for token in shlex.split(matches[-1]):
        if "=" in token:
            key, value = token.split("=", 1)
            parsed[key] = value
    return parsed


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--scale", choices=("smoke", "paper"), default="smoke")
    value.add_argument("--versions", default="sync-fair,async-current")
    value.add_argument("--phases", default=",".join(PHASES))
    value.add_argument("--repeats", type=int)
    value.add_argument("--output", type=pathlib.Path)
    value.add_argument("--bdf", default="0000:b2:00.0")
    value.add_argument("--nsid", type=int, default=1)
    value.add_argument("--dax", default="/dev/dax0.1")
    value.add_argument("--expected-model", default="SAMSUNG MZPLJ3T2HBJR-00007")
    value.add_argument("--expected-serial", default="S55HNC0W100205")
    value.add_argument("--spdk-root", default="/opt/orchfs/spdk")
    value.add_argument("--hugemem-mb", type=int, default=4096)
    value.add_argument("--sync-worktree", type=pathlib.Path,
                       default=pathlib.Path("/tmp/orchfs-sync-baseline"))
    value.add_argument("--sync-build", type=pathlib.Path,
                       default=pathlib.Path("/tmp/orchfs-sync-baseline/build-fair"))
    value.add_argument("--async-build", type=pathlib.Path,
                       default=ROOT / "build-repro")
    value.add_argument("--async-trace-build", type=pathlib.Path,
                       default=ROOT / "build-repro-trace")
    value.add_argument("--client-workers", type=int, default=1)
    value.add_argument("--client-lanes", type=int, default=4)
    value.add_argument("--trace-sample-every", type=int, default=1)
    value.add_argument("--case-timeout", type=float, default=900)
    value.add_argument(
        "--allow-failures", action="store_true",
        help="write partial reports but return success when samples fail")
    return value


def command_arguments(executable: pathlib.Path, database: str, phase: str,
                      records: int, operations: int, *, prepare: bool) -> list[str]:
    return [
        str(executable), "--db", database, "--phase", phase,
        "--records", str(records), "--operations", str(operations),
        "--value-size", "1024", "--write-buffer-size", str(64 * 1024**2),
        "--seed", str(SEED), "--scan-length", "10", "--sync", "1",
        "--prepare", "1" if prepare else "0",
    ]


def write_csv(output: pathlib.Path, rows: list[dict[str, object]]) -> None:
    target = output / "csv/fig17_ycsb_leveldb.csv"
    temporary = target.with_suffix(".csv.tmp")
    with temporary.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(target)


def plot(output: pathlib.Path, rows: list[dict[str, object]], scale: str) -> None:
    versions = ("sync-fair", "async-current")
    figure, axis = plt.subplots(figsize=(9.2, 4.8), constrained_layout=True)
    width = 0.36
    x = list(range(len(PHASES)))
    colors = {"sync-fair": "#4c78a8", "async-current": "#f58518"}
    for index, version in enumerate(versions):
        values = []
        for phase in PHASES:
            samples = [float(row["ops_per_s"]) for row in rows
                       if row["phase"] == phase and row["version"] == version
                       and row["status"] == "ok"]
            values.append(statistics.fmean(samples) if samples else math.nan)
        shifted = [item + (index - 0.5) * width for item in x]
        if any(not math.isnan(value) for value in values):
            axis.bar(shifted, values, width=width, label=version,
                     color=colors[version])
        else:
            axis.bar([], [], width=width, label=version,
                     color=colors[version])
        for position, value, phase in zip(shifted, values, PHASES):
            if math.isnan(value) and any(
                    row["phase"] == phase and row["version"] == version
                    and row["status"] != "ok" for row in rows):
                axis.text(position, 0.02, "failed", rotation=90,
                          ha="center", va="bottom", fontsize=7,
                          color=colors[version],
                          transform=axis.get_xaxis_transform())
    axis.set_xticks(x, ["Load", "A", "B", "C", "D", "E", "F"])
    axis.set_ylabel("logical operations/s")
    axis.set_title(f"Adapted paper Fig.17 LevelDB/YCSB shapes ({scale})")
    axis.grid(axis="y", alpha=0.25)
    axis.legend(handles=[Patch(facecolor=colors[version], label=version)
                         for version in versions])
    for extension in ("png", "pdf"):
        figure.savefig(output / f"plots/fig17.{extension}", dpi=180)
    plt.close(figure)


def run_sample(runner: Runner, args: argparse.Namespace,
               executable: pathlib.Path, version: str, phase: str,
               repeat: int, records: int,
               operations: int) -> dict[str, object]:
    run_dir = runner.raw / version / "fig17" / phase / f"run-{repeat}"
    run_dir.mkdir(parents=True, exist_ok=False)
    case = Case("17", phase, workers=16)
    database = f"/Or/ycsb-{phase}-r{repeat}"
    process: subprocess.Popen[str] | None = None
    exit_code = 125
    result_log = run_dir / "bench.log"
    try:
        runner.format(version, run_dir)
        process, endpoint = runner.start_server(version, case, run_dir)
        environment = runner.client_environment(version, endpoint, run_dir)
        if version == "async-current":
            environment["ORCHFS_REPRO_LEVELDB_LOCK_NOOP"] = "1"
        measured_operations = records if phase == "load" else operations
        command = runner.sudo_env(
            environment,
            command_arguments(executable, database, phase, records,
                              measured_operations, prepare=phase != "load"))
        completed = runner.logged_run(
            command, check=False, timeout=args.case_timeout,
            stdout=result_log)
        exit_code = completed.returncode
        runner.stop_server(version, process)
        process = None
    except (OSError, RuntimeError, subprocess.TimeoutExpired) as error:
        (run_dir / "runner-error.log").write_text(
            f"{type(error).__name__}: {error}\n", encoding="utf-8")
        if isinstance(error, subprocess.TimeoutExpired):
            exit_code = 124
    finally:
        if process is not None:
            runner.stop_server(version, process)
    parsed = (parse_result(result_log) if result_log.exists()
              else {"failure_reason": "missing-log"})
    reason = parsed.get("failure_reason", "")
    if exit_code != 0 and not reason:
        reason = f"exit-{exit_code}"
    row: dict[str, object] = {field: "" for field in FIELDS}
    row |= {
        "figure": "17", "phase": phase, "version": version,
        "scale": args.scale, "repeat": repeat,
        "status": "ok" if exit_code == 0 and not reason else "failed",
        "exit_code": exit_code, "failure_reason": reason,
        "raw_dir": str(run_dir), "commit": runner.commits[version],
        "driver": "embedded-cpp-ycsb-shapes",
    }
    for key in ("operations", "records", "value_size", "write_buffer_size",
                "sync", "reads", "writes", "scans", "seconds",
                "ops_per_s"):
        row[key] = parsed.get(key, "")
    return row


def main() -> int:
    args = parser().parse_args()
    args.bdf = args.bdf.lower()
    args.repeats = args.repeats or (1 if args.scale == "smoke" else 3)
    versions = [item.strip() for item in args.versions.split(",") if item.strip()]
    phases = [item.strip().lower() for item in args.phases.split(",")
              if item.strip()]
    if any(item not in {"sync-fair", "async-current"} for item in versions):
        raise SystemExit("invalid --versions")
    if any(item not in PHASES for item in phases):
        raise SystemExit("invalid --phases")
    records = 5_000_000 if args.scale == "paper" else 1000
    operations = 5_000_000 if args.scale == "paper" else 1000
    timestamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    output = (args.output or ROOT / "benchmark-results" /
              f"leveldb-reproduction-{timestamp}").resolve()
    output.mkdir(parents=True, exist_ok=False)
    runner = Runner(args, output)
    executable = args.async_build / "orchfs_ycsb_leveldb"
    rows: list[dict[str, object]] = []
    (output / "manifest.tsv").write_text(
        "key\tvalue\n"
        f"scale\t{args.scale}\nseed\t{SEED}\nrecords\t{records}\n"
        f"operations\t{operations}\nvalue_size\t1024\n"
        f"write_buffer_size\t{64 * 1024**2}\nwrite_sync\t1\n"
        "workload_prepare\tin-process, excluded from measured interval\n"
        "driver\tembedded-cpp-ycsb-shapes\n"
        "leveldb_package\tlib-leveldb 1.23 ABI\n"
        "async_advisory_lock\treproduction-only no-op via ORCHFS_REPRO_LEVELDB_LOCK_NOOP=1\n"
        "interleave\tper-phase, direction reversed by phase and repeat\n",
        encoding="utf-8")
    try:
        runner.validate()
        if not executable.is_file():
            raise RuntimeError(f"missing {executable}")
        for repeat in range(1, args.repeats + 1):
            for phase_index, phase in enumerate(phases):
                order = (versions if (repeat + phase_index) % 2 == 1
                         else list(reversed(versions)))
                for version in order:
                    rows.append(run_sample(
                        runner, args, executable, version, phase, repeat,
                        records, operations))
                    write_csv(output, rows)
    finally:
        try:
            runner.restore_driver()
        finally:
            runner.close()
            append_manifest(output, {
                "finished_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
                "sample_count": len(rows),
                "successful_samples": sum(
                    row["status"] == "ok" for row in rows),
                "async_commit": runner.commits["async-current"],
                "sync_commit": runner.commits["sync-fair"],
                "final_driver": runner.driver(),
            })
    plot(output, rows, args.scale)
    failures = [row for row in rows if row["status"] != "ok"]
    table = "".join(
        f"| {row['phase'].upper()} | {row['version']} | "
        f"{row['failure_reason']} |\n" for row in failures)
    (output / "report.md").write_text(
        "# Adapted LevelDB/YCSB reproduction\n\n"
        "This uses an embedded C++ driver with YCSB A--F operation mixes; "
        "it is not the Java YCSB framework.\n\n"
        f"Successful samples: {len(rows) - len(failures)}/{len(rows)}.\n\n"
        "| Phase | Version | Failure |\n| --- | --- | --- |\n" + table,
        encoding="utf-8")
    print(output)
    expected = len(phases) * len(versions) * args.repeats
    complete = len(rows) == expected and not failures
    return 0 if complete or args.allow_failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
