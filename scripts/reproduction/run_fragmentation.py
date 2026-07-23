#!/usr/bin/env python3
"""Run a scaled, migration-disabled adaptation of FAST'25 Fig.19/Table 2.

The synchronous baseline has no reliable allocation-counter interface, so it
is recorded as unsupported rather than assigned an inferred value.  The async
run formats a fresh file system per request-size case and takes out-of-band KFS
bitmap snapshots after the SSD prefill and every cumulative-write checkpoint.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import math
import pathlib
import signal
import statistics
import subprocess
import time

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from run_reproduction import Case, GIB, MIB, ROOT, Runner, SEED, append_manifest


PAGE_SIZE = 4096
SSD_BLOCK_SIZE = 32 * 1024
FIGURE_FIELDS = [
    "figure", "case", "request_size", "size_mode", "checkpoint_bytes",
    "version", "scale", "repeat", "status", "exit_code", "error_stage",
    "error_number", "nvm_upage_count", "logical_page_count",
    "nvm_upage_percentage", "nvm_page_count", "ssd_block_count",
    "raw_dir", "commit",
]
TABLE_FIELDS = [
    "table", "case", "request_size", "phase", "written_bytes", "version",
    "scale", "repeat", "status", "nvm_page_count", "nvm_page_bytes",
    "nvm_upage_count", "nvm_upage_bytes", "ssd_block_count",
    "ssd_block_bytes", "raw_dir", "commit",
]


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--scale", choices=("smoke", "paper"), default="smoke")
    value.add_argument("--cases", default="1KiB,4KiB,16KiB,32KiB,64KiB,256KiB,1MiB,uniform")
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


def case_definitions() -> dict[str, tuple[str, int, int, int]]:
    return {
        "1KiB": ("fixed", 1024, 1024, 1024),
        "4KiB": ("fixed", 4 * 1024, 4 * 1024, 4 * 1024),
        "16KiB": ("fixed", 16 * 1024, 16 * 1024, 16 * 1024),
        "32KiB": ("fixed", 32 * 1024, 32 * 1024, 32 * 1024),
        "64KiB": ("fixed", 64 * 1024, 64 * 1024, 64 * 1024),
        "256KiB": ("fixed", 256 * 1024, 256 * 1024, 256 * 1024),
        "1MiB": ("fixed", MIB, MIB, MIB),
        "uniform": ("uniform", MIB, 1, 2 * MIB),
    }


def load_snapshots(path: pathlib.Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as stream:
        return list(csv.DictReader(stream))


def request_snapshot(runner: Runner, process: subprocess.Popen[str],
                     path: pathlib.Path) -> dict[str, str]:
    before = len(load_snapshots(path))
    runner.logged_run(["sudo", "-n", "kill", f"-{signal.SIGUSR1.value}",
                       str(process.pid)], timeout=10)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        rows = load_snapshots(path)
        if len(rows) > before:
            return rows[-1]
        if process.poll() is not None:
            raise RuntimeError("KFS exited while waiting for allocation snapshot")
        time.sleep(0.05)
    raise RuntimeError(f"timed out waiting for snapshot in {path}")


def counts(row: dict[str, str], baseline: dict[str, str]) -> tuple[int, int, int]:
    raw_pages = max(0, int(row["nvm_page_count"]) -
                    int(baseline["nvm_page_count"]))
    upages = max(0, int(row["nvm_upage_proxy_count"]) -
                 int(baseline["nvm_upage_proxy_count"]))
    blocks = max(0, int(row["ssd_block_count"]) -
                 int(baseline["ssd_block_count"]))
    # Each Upage owns one PAGE_BMP allocation plus one BUFMETA_BMP record.
    # Subtract it to report mutually exclusive Table-2 Page/Upage counts.
    return max(0, raw_pages - upages), upages, blocks


def write_csv(path: pathlib.Path, fields: list[str],
              rows: list[dict[str, object]]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(path)


def run_bench(runner: Runner, environment: dict[str, str],
              arguments: list[str], log: pathlib.Path,
              timeout: float) -> tuple[int, dict[str, str]]:
    executable = runner.args.async_build / "orchfs_repro_bench"
    completed = runner.logged_run(
        runner.sudo_env(environment, [str(executable), *arguments]),
        check=False, timeout=timeout, stdout=log)
    parsed = runner.parse_log(log)
    return completed.returncode, parsed


def plot(output: pathlib.Path, rows: list[dict[str, object]], scale: str) -> None:
    successful = [row for row in rows if row["status"] == "ok"]
    if not successful:
        return
    figure, axis = plt.subplots(figsize=(9.4, 5.0), constrained_layout=True)
    for case in case_definitions():
        selected = [row for row in successful if row["case"] == case]
        if not selected:
            continue
        grouped: dict[int, list[float]] = {}
        for row in selected:
            grouped.setdefault(int(row["checkpoint_bytes"]), []).append(
                float(row["nvm_upage_percentage"]))
        x = sorted(grouped)
        y = [statistics.fmean(grouped[item]) for item in x]
        axis.plot([item / MIB for item in x], y, marker="o", label=case)
    axis.set_xlabel("Cumulative random writes (MiB; true scaled amount)")
    axis.set_ylabel("NVM Upages / logical file pages (%)")
    axis.set_title(f"Adapted paper Fig.19, migration disabled ({scale})")
    axis.grid(alpha=0.25)
    axis.legend(ncol=2)
    for extension in ("png", "pdf"):
        figure.savefig(output / f"plots/fig19.{extension}", dpi=180)
    plt.close(figure)


def main() -> int:
    args = parser().parse_args()
    args.bdf = args.bdf.lower()
    args.repeats = args.repeats or (1 if args.scale == "smoke" else 3)
    if args.repeats <= 0:
        raise SystemExit("--repeats must be positive")
    definitions = case_definitions()
    selected = [item.strip() for item in args.cases.split(",") if item.strip()]
    unknown = sorted(set(selected) - set(definitions))
    if unknown:
        raise SystemExit(f"unknown cases: {unknown}")

    file_bytes = 10 * GIB if args.scale == "paper" else 4 * MIB
    checkpoints = ([GIB, 2 * GIB, 5 * GIB, 10 * GIB, 20 * GIB,
                    50 * GIB, 100 * GIB, 1024 * GIB]
                   if args.scale == "paper"
                   else [256 * 1024, 512 * 1024, MIB, 2 * MIB, 4 * MIB])
    timestamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    output = (args.output or ROOT / "benchmark-results" /
              f"fragmentation-reproduction-{timestamp}").resolve()
    output.mkdir(parents=True, exist_ok=False)
    runner = Runner(args, output)
    figure_rows: list[dict[str, object]] = []
    table_rows: list[dict[str, object]] = []
    (output / "manifest.tsv").write_text(
        "key\tvalue\n"
        f"scale\t{args.scale}\nseed\t{SEED}\nfile_bytes\t{file_bytes}\n"
        f"checkpoints\t{','.join(str(item) for item in checkpoints)}\n"
        "migration\tdisabled-via-ORCHFS_REPRO_DISABLE_MIGRATION\n"
        "upage_counter\tBUFMETA_BMP delta from fresh-server baseline\n"
        "sync_fair_space_counters\tunsupported\n",
        encoding="utf-8")
    (output / "experiment_status.csv").write_text(
        "version,status,reason\n"
        "sync-fair,unsupported,no reliable allocation counters in detached baseline\n"
        "async-current,scheduled,migration-disabled source snapshot run\n",
        encoding="utf-8")

    try:
        runner.validate()
        for repeat in range(1, args.repeats + 1):
            for case_name in selected:
                mode, fixed, minimum, maximum = definitions[case_name]
                run_dir = (runner.raw / "async-current" / "fig19" /
                           case_name / f"run-{repeat}")
                run_dir.mkdir(parents=True, exist_ok=False)
                snapshot_path = run_dir / "space-raw.csv"
                case = Case("19", case_name, workers=16)
                process: subprocess.Popen[str] | None = None
                failed = False
                try:
                    runner.format("async-current", run_dir)
                    extra = {
                        "ORCHFS_REPRO_DISABLE_MIGRATION": "1",
                        "ORCHFS_REPRO_SPACE_FILE": str(snapshot_path),
                    }
                    process, endpoint = runner.start_server(
                        "async-current", case, run_dir,
                        extra_environment=extra)
                    environment = runner.client_environment(
                        "async-current", endpoint, run_dir, case) | extra
                    deadline = time.monotonic() + 10
                    while not load_snapshots(snapshot_path):
                        if time.monotonic() >= deadline:
                            raise RuntimeError("missing server_ready snapshot")
                        time.sleep(0.05)
                    server_baseline = load_snapshots(snapshot_path)[0]
                    prefix = f"orchfs-frag-{case_name.lower()}-r{repeat}"
                    prepare = [
                        "--path-prefix", prefix, "--operation", "write",
                        "--access", "sequential", "--size-mode", "fixed",
                        "--size", str(MIB), "--min-size", str(MIB),
                        "--max-size", str(MIB), "--threads", "1", "--files", "1",
                        "--fsync", "end", "--offset-alignment", str(MIB),
                        "--seed", str(SEED), "--label", f"fig19-{case_name}-prepare",
                        "--prepare-bytes", str(file_bytes), "--bytes-per-thread", "0",
                        "--latency-samples", "0",
                    ]
                    code, parsed = run_bench(
                        runner, environment, prepare, run_dir / "prepare.log",
                        args.case_timeout)
                    if code != 0:
                        raise RuntimeError(
                            "prefill failed: " + str(parsed or {"exit_code": code}))
                    prefill_snapshot = request_snapshot(
                        runner, process, snapshot_path)
                    page_count, upage_count, block_count = counts(
                        prefill_snapshot, server_baseline)
                    table_rows.append({
                        "table": "02", "case": case_name,
                        "request_size": fixed if mode == "fixed" else "1B-2MiB",
                        "phase": "prefill", "written_bytes": 0,
                        "version": "async-current", "scale": args.scale,
                        "repeat": repeat, "status": "ok",
                        "nvm_page_count": page_count,
                        "nvm_page_bytes": page_count * PAGE_SIZE,
                        "nvm_upage_count": upage_count,
                        "nvm_upage_bytes": upage_count * PAGE_SIZE,
                        "ssd_block_count": block_count,
                        "ssd_block_bytes": block_count * SSD_BLOCK_SIZE,
                        "raw_dir": str(run_dir),
                        "commit": runner.commits["async-current"],
                    })
                    previous = 0
                    for checkpoint in checkpoints:
                        delta = checkpoint - previous
                        measurement = [
                            "--path-prefix", prefix, "--operation", "write",
                            "--access", "random", "--size-mode", mode,
                            "--size", str(fixed), "--min-size", str(minimum),
                            "--max-size", str(maximum), "--threads", "1", "--files", "1",
                            "--fsync", "each", "--offset-alignment", "1",
                            "--unaligned-to", str(PAGE_SIZE),
                            "--seed", str(SEED + checkpoint),
                            "--label", f"fig19-{case_name}-{checkpoint}",
                            "--existing-bytes", str(file_bytes),
                            "--bytes-per-thread", str(delta),
                            "--latency-samples", "0",
                        ]
                        code, parsed = run_bench(
                            runner, environment, measurement,
                            run_dir / f"write-{checkpoint}.log",
                            args.case_timeout)
                        row: dict[str, object] = {
                            "figure": "19", "case": case_name,
                            "request_size": fixed if mode == "fixed" else "1B-2MiB",
                            "size_mode": mode, "checkpoint_bytes": checkpoint,
                            "version": "async-current", "scale": args.scale,
                            "repeat": repeat, "status": "failed" if code else "ok",
                            "exit_code": code,
                            "error_stage": parsed.get("error_stage", ""),
                            "error_number": parsed.get("error_errno", ""),
                            "nvm_upage_count": "", "logical_page_count": file_bytes // PAGE_SIZE,
                            "nvm_upage_percentage": "", "nvm_page_count": "",
                            "ssd_block_count": "", "raw_dir": str(run_dir),
                            "commit": runner.commits["async-current"],
                        }
                        if code != 0:
                            figure_rows.append(row)
                            failed = True
                            break
                        snapshot = request_snapshot(runner, process, snapshot_path)
                        page_count, upage_count, block_count = counts(
                            snapshot, server_baseline)
                        row |= {
                            "nvm_upage_count": upage_count,
                            "nvm_upage_percentage":
                                100.0 * upage_count / (file_bytes // PAGE_SIZE),
                            "nvm_page_count": page_count,
                            "ssd_block_count": block_count,
                        }
                        figure_rows.append(row)
                        previous = checkpoint
                    if not failed:
                        table_rows.append({
                            "table": "02", "case": case_name,
                            "request_size": fixed if mode == "fixed" else "1B-2MiB",
                            "phase": "random-overwrite",
                            "written_bytes": checkpoints[-1],
                            "version": "async-current", "scale": args.scale,
                            "repeat": repeat, "status": "ok",
                            "nvm_page_count": page_count,
                            "nvm_page_bytes": page_count * PAGE_SIZE,
                            "nvm_upage_count": upage_count,
                            "nvm_upage_bytes": upage_count * PAGE_SIZE,
                            "ssd_block_count": block_count,
                            "ssd_block_bytes": block_count * SSD_BLOCK_SIZE,
                            "raw_dir": str(run_dir),
                            "commit": runner.commits["async-current"],
                        })
                except (OSError, RuntimeError, subprocess.TimeoutExpired) as error:
                    (run_dir / "runner-error.log").write_text(
                        f"{type(error).__name__}: {error}\n", encoding="utf-8")
                    if not failed:
                        figure_rows.append({
                            "figure": "19", "case": case_name,
                            "request_size": fixed if mode == "fixed" else "1B-2MiB",
                            "size_mode": mode, "checkpoint_bytes": 0,
                            "version": "async-current", "scale": args.scale,
                            "repeat": repeat, "status": "failed",
                            "exit_code": 124 if isinstance(
                                error, subprocess.TimeoutExpired) else 125,
                            "error_stage": "runner", "error_number": "",
                            "nvm_upage_count": "", "logical_page_count": file_bytes // PAGE_SIZE,
                            "nvm_upage_percentage": "", "nvm_page_count": "",
                            "ssd_block_count": "", "raw_dir": str(run_dir),
                            "commit": runner.commits["async-current"],
                        })
                finally:
                    if process is not None:
                        runner.stop_server("async-current", process)
                write_csv(output / "csv/fig19_fragmentation.csv",
                          FIGURE_FIELDS, figure_rows)
                write_csv(output / "csv/table02_space.csv",
                          TABLE_FIELDS, table_rows)
    finally:
        try:
            runner.restore_driver()
        finally:
            runner.close()
            append_manifest(output, {
                "finished_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
                "case_rows": len(figure_rows),
                "successful_checkpoints": sum(
                    row["status"] == "ok" for row in figure_rows),
                "table_rows": len(table_rows),
                "async_commit": runner.commits["async-current"],
                "final_driver": runner.driver(),
            })

    plot(output, figure_rows, args.scale)
    ok = sum(row["status"] == "ok" for row in figure_rows)
    failed_count = len(figure_rows) - ok
    (output / "report.md").write_text(
        "# Adapted Table 2 / Fig.19 allocation reproduction\n\n"
        f"Scale: `{args.scale}`. Successful checkpoints: {ok}; "
        f"failed case/checkpoints: {failed_count}.\n\n"
        "`sync-fair` is explicitly unsupported because the detached baseline "
        "has no trustworthy live allocation counters. `async-current` uses "
        "fresh-format bitmap deltas with migration disabled. Upage count is "
        "the BUFMETA allocation delta; Page excludes those Upage backing "
        "pages. Smoke checkpoints are scaled and must not be relabeled as "
        "the paper's 10 GiB/1 TiB result.\n",
        encoding="utf-8")
    print(output)
    expected_checkpoints = len(selected) * len(checkpoints) * args.repeats
    # Table 2 records both the sequential SSD prefill and the final random
    # overwrite allocation mix for every case.
    expected_tables = 2 * len(selected) * args.repeats
    complete = (len(figure_rows) == expected_checkpoints and
                len(table_rows) == expected_tables and failed_count == 0)
    return 0 if complete or args.allow_failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
