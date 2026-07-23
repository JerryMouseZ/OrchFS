#!/usr/bin/env python3
"""Run the adapted FAST'25 Fig.16 Filebench sync/async comparison."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import math
import pathlib
import re
import statistics
import subprocess

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from generate_filebench_workload import generate
from run_reproduction import (
    Case, ROOT, Runner, append_manifest, worktree_fingerprint,
)


FIELDS = [
    "figure", "workload", "threads", "version", "scale", "repeat",
    "status", "exit_code", "operations", "ops_per_s", "Kops_per_s",
    "MiB_per_s", "ms_per_op", "failure_reason", "raw_dir", "commit",
]


def parse_summary(path: pathlib.Path) -> dict[str, str]:
    text = path.read_text(encoding="utf-8", errors="replace")
    pattern = re.compile(
        r"IO Summary:\s+(?P<operations>\d+) ops\s+"
        r"(?P<ops_per_s>[0-9.]+) ops/s.*?"
        r"(?P<MiB_per_s>[0-9.]+)mb/s\s+"
        r"(?P<ms_per_op>[0-9.]+)ms/op")
    matches = list(pattern.finditer(text))
    return matches[-1].groupdict() if matches else {}


def failure_reason(path: pathlib.Path, exit_code: int,
                   parsed: dict[str, str]) -> str:
    text = path.read_text(encoding="utf-8", errors="replace")
    markers = (
        ("NO VALID RESULTS", "no-valid-results"),
        ("Failed to write", "write-eio"),
        ("cannot search path", "namespace-lookup-failed"),
        ("Run stopped early", "fileset-exhausted"),
        ("buffer overflow detected", "libc-buffer-overflow"),
    )
    for marker, reason in markers:
        if marker in text:
            return reason
    if exit_code != 0:
        return f"exit-{exit_code}"
    if not parsed:
        return "missing-io-summary"
    return ""


def write_results(output: pathlib.Path, rows: list[dict[str, object]]) -> None:
    target = output / "csv/fig16_filebench.csv"
    with target.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def plot(output: pathlib.Path, rows: list[dict[str, object]], scale: str) -> None:
    successful = [row for row in rows if row["status"] == "ok"]
    if not successful:
        return
    labels = [(workload, threads) for workload in ("fileserver", "webproxy", "varmail")
              for threads in (1, 16)]
    versions = ("sync-fair", "async-current")
    grouped: dict[tuple[str, int, str], list[float]] = {}
    for workload, threads in labels:
        for version in versions:
            grouped[(workload, threads, version)] = [
                float(row["Kops_per_s"]) for row in successful
                if row["workload"] == workload and row["threads"] == threads
                and row["version"] == version]
    figure, axis = plt.subplots(figsize=(10, 4.8), constrained_layout=True)
    x = list(range(len(labels)))
    width = 0.36
    colors = {"sync-fair": "#4c78a8", "async-current": "#f58518"}
    for index, version in enumerate(versions):
        values = [statistics.fmean(grouped[(workload, threads, version)])
                  if grouped[(workload, threads, version)] else math.nan
                  for workload, threads in labels]
        shifted = [value + (index - 0.5) * width for value in x]
        axis.bar(shifted, values,
                 width=width, label=version, color=colors[version])
        for position, value, (workload, threads) in zip(
                shifted, values, labels):
            if math.isnan(value) and any(
                    row["workload"] == workload and row["threads"] == threads
                    and row["version"] == version and row["status"] != "ok"
                    for row in rows):
                axis.text(position, 0.02, "failed", rotation=90,
                          ha="center", va="bottom", fontsize=7,
                          color=colors[version],
                          transform=axis.get_xaxis_transform())
    axis.set_xticks(x, [f"{workload}\n{threads}T" for workload, threads in labels])
    positive = [float(row["Kops_per_s"]) for row in successful]
    if positive and max(positive) / min(positive) > 100:
        axis.set_yscale("log")
        axis.set_ylabel("Kops/s (log scale)")
    else:
        axis.set_ylabel("Kops/s")
    axis.set_title(f"Adapted paper Fig.16 Filebench ({scale})")
    axis.grid(axis="y", alpha=0.25)
    axis.legend()
    for extension in ("png", "pdf"):
        figure.savefig(output / f"plots/fig16.{extension}", dpi=180)
    plt.close(figure)


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--scale", choices=("smoke", "paper"), default="smoke")
    value.add_argument("--versions", default="sync-fair,async-current")
    value.add_argument("--workloads", default="fileserver,webproxy,varmail")
    value.add_argument("--threads", default="1,16")
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
                       default=pathlib.Path(
                           "/tmp/orchfs-sync-baseline/build-filebench"))
    value.add_argument("--async-build", type=pathlib.Path, default=ROOT / "build-repro")
    value.add_argument("--async-trace-build", type=pathlib.Path,
                       default=ROOT / "build-repro-trace")
    value.add_argument("--client-workers", type=int, default=0)
    value.add_argument("--client-lanes", type=int, default=0)
    value.add_argument("--ipc-ring-capacity", type=int, default=16)
    value.add_argument("--trace-sample-every", type=int, default=1)
    value.add_argument("--case-timeout", type=float, default=600)
    value.add_argument(
        "--allow-failures", action="store_true",
        help="write partial reports but return success when samples fail")
    value.add_argument(
        "--filebench-bin", type=pathlib.Path,
        default=pathlib.Path("filebench"),
        help=("Filebench executable. Use the repository-patched Filebench "
              "build; the distro binary recursively removes /Or via a "
              "preloaded shell and is not OrchFS-compatible."))
    return value


def run_sample(runner: Runner, args: argparse.Namespace, filebench_bin: str,
               version: str, workload: str, thread_count: int,
               repeat: int) -> dict[str, object]:
    name = f"{workload}-t{thread_count}"
    run_dir = runner.raw / version / "fig16" / name / f"run-{repeat}"
    run_dir.mkdir(parents=True, exist_ok=False)
    case = Case("16", name, threads=thread_count, workers=16)
    process = None
    exit_code = 125
    try:
        runner.format(version, run_dir)
        process, endpoint = runner.start_server(version, case, run_dir)
        directory = f"/Or/fb-{workload}-t{thread_count}-r{repeat}"
        workload_path = run_dir / f"{workload}.f"
        workload_path.write_text(
            generate(workload, directory, thread_count, args.scale),
            encoding="utf-8")
        environment = runner.client_environment(
            version, endpoint, run_dir, case)
        create = runner.logged_run(
            runner.sudo_env(environment, [
                str(args.async_build / "orchfs_repro_mkdir"), directory]),
            check=False, timeout=60, stdout=run_dir / "mkdir.log")
        if create.returncode != 0:
            raise RuntimeError(f"failed to create Filebench root {directory}")
        # The legacy LibFS destructor shuts down its global KFS when the
        # helper exits. Restart both versions so the measured Filebench
        # process begins from the same persisted namespace and lifecycle.
        if version == "sync-fair":
            try:
                process.wait(timeout=10)
                stream = getattr(process, "_orchfs_log_stream", None)
                if stream is not None:
                    stream.close()
            except subprocess.TimeoutExpired:
                runner.stop_server(version, process)
        else:
            runner.stop_server(version, process)
        process = None
        process, endpoint = runner.start_server(version, case, run_dir)
        environment = runner.client_environment(
            version, endpoint, run_dir, case)
        # Put LD_PRELOAD after setarch. If it is applied to setarch itself,
        # the legacy wrapper registers a throw-away LibFS client before
        # exec(2), so FILEBENCH's parent-only metadata writeback is skipped.
        assignments = [f"{key}={value}" for key, value in environment.items()]
        command = [
            "sudo", "-n", "-E", "timeout", "--signal=TERM",
            "--kill-after=10s", f"{args.case_timeout}s", "setarch",
            subprocess.check_output(["uname", "-m"], text=True).strip(), "-R",
            "env", *assignments, filebench_bin, "-f", str(workload_path),
        ]
        completed = runner.logged_run(
            command, check=False, timeout=args.case_timeout + 20,
            stdout=run_dir / "filebench.log")
        exit_code = completed.returncode
        runner.stop_server(version, process)
        process = None
    except (OSError, RuntimeError, subprocess.TimeoutExpired) as error:
        (run_dir / "runner-error.log").write_text(
            f"{type(error).__name__}: {error}\n", encoding="utf-8")
        exit_code = 124 if isinstance(error, subprocess.TimeoutExpired) else 125
    finally:
        if process is not None:
            runner.stop_server(version, process)
    parsed = (parse_summary(run_dir / "filebench.log")
              if (run_dir / "filebench.log").exists() else {})
    reason = (failure_reason(run_dir / "filebench.log", exit_code, parsed)
              if (run_dir / "filebench.log").exists()
              else f"exit-{exit_code}")
    ops = float(parsed.get("ops_per_s", "0"))
    return {
        "figure": "16", "workload": workload, "threads": thread_count,
        "version": version, "scale": args.scale, "repeat": repeat,
        "status": "ok" if not reason else "failed", "exit_code": exit_code,
        "operations": parsed.get("operations", ""),
        "ops_per_s": parsed.get("ops_per_s", ""),
        "Kops_per_s": ops / 1000.0 if parsed else "",
        "MiB_per_s": parsed.get("MiB_per_s", ""),
        "ms_per_op": parsed.get("ms_per_op", ""),
        "failure_reason": reason, "raw_dir": str(run_dir),
        "commit": runner.commits[version],
    }


def main() -> int:
    args = parser().parse_args()
    args.bdf = args.bdf.lower()
    if args.repeats is None:
        args.repeats = 1 if args.scale == "smoke" else 3
    versions = [item.strip() for item in args.versions.split(",") if item.strip()]
    workloads = [item.strip() for item in args.workloads.split(",") if item.strip()]
    threads = [int(item) for item in args.threads.split(",") if item.strip()]
    if any(item not in {"sync-fair", "async-current"} for item in versions):
        raise SystemExit("invalid --versions")
    if any(item not in {"fileserver", "webproxy", "varmail"} for item in workloads):
        raise SystemExit("invalid --workloads")
    if any(item not in {1, 16} for item in threads):
        raise SystemExit("--threads accepts 1,16")

    timestamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    output = (args.output or ROOT / "benchmark-results" /
              f"filebench-reproduction-{timestamp}").resolve()
    output.mkdir(parents=True, exist_ok=False)
    runner = Runner(args, output)
    filebench_bin = (str(args.filebench_bin.resolve())
                     if args.filebench_bin.parent != pathlib.Path(".")
                     or args.filebench_bin.exists()
                     else str(args.filebench_bin))
    filebench_path = pathlib.Path(filebench_bin)
    filebench_hash = (hashlib.sha256(filebench_path.read_bytes()).hexdigest()
                      if filebench_path.is_file() else "unavailable")
    filebench_version = subprocess.run(
        [filebench_bin, "-h"], text=True, capture_output=True)
    version_lines = (filebench_version.stdout + filebench_version.stderr).splitlines()
    (output / "manifest.tsv").write_text(
        "key\tvalue\n"
        f"scale\t{args.scale}\n"
        f"filebench_bin\t{filebench_bin}\n"
        f"filebench_sha256\t{filebench_hash}\n"
        f"filebench_version\t{version_lines[0] if version_lines else 'unavailable'}\n"
        f"sync_build\t{args.sync_build.resolve()}\n"
        f"async_build\t{args.async_build.resolve()}\n"
        f"async_worktree_sha256\t{worktree_fingerprint(ROOT)}\n"
        f"bdf\t{args.bdf}\nnsid\t{args.nsid}\ndax\t{args.dax}\n"
        "interleave\tper-case, direction reversed by case and repeat\n"
        "namespace_boundary\tfresh format per sample; restart after root creation\n",
        encoding="utf-8")
    rows: list[dict[str, object]] = []
    try:
        runner.validate()
        cases = [(workload, thread_count) for workload in workloads
                 for thread_count in threads]
        for repeat in range(1, args.repeats + 1):
            for case_index, (workload, thread_count) in enumerate(cases):
                order = (versions if (repeat + case_index) % 2 == 1
                         else list(reversed(versions)))
                for version in order:
                    rows.append(run_sample(
                        runner, args, filebench_bin, version, workload,
                        thread_count, repeat))
                    write_results(output, rows)
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
    failed = [row for row in rows if row["status"] != "ok"]
    failure_table = ""
    if failed:
        failure_table = (
            "\n## Failed samples\n\n"
            "| Workload | Threads | Version | Reason |\n"
            "| --- | ---: | --- | --- |\n" +
            "".join(
                f"| {row['workload']} | {row['threads']} | "
                f"{row['version']} | {row['failure_reason']} |\n"
                for row in failed))
    (output / "report.md").write_text(
        "# Filebench differential reproduction\n\n"
        f"Scale: `{args.scale}`. Successful samples: "
        f"{sum(row['status'] == 'ok' for row in rows)}/{len(rows)}.\n\n"
        "See `csv/fig16_filebench.csv`, `raw/`, and `plots/fig16.*`.\n" +
        failure_table,
        encoding="utf-8")
    print(output)
    expected = len(workloads) * len(threads) * len(versions) * args.repeats
    complete = len(rows) == expected and not failed
    return 0 if complete or args.allow_failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
