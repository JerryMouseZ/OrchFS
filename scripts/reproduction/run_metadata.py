#!/usr/bin/env python3
"""Run isolated open/stat/listdir/mkdir workloads with optional KFS tracing."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import os
import pathlib
import shlex
import statistics
import subprocess

from run_reproduction import (
    Case, ROOT, Runner, append_manifest, worktree_fingerprint,
)
from summarize_trace import METADATA_FIELDS, summarize_metadata, write_csv


FIELDS = [
    "operation", "threads", "entries", "operations_per_thread",
    "version", "scale", "repeat", "trace", "status", "exit_code",
    "operations", "started_ns", "ended_ns", "seconds", "ops_per_s",
    "mean_us", "p50_us",
    "p99_us", "returned_items", "items_per_operation", "failure_reason",
    "raw_dir", "commit",
]
OPERATIONS = ("open", "stat", "listdir", "mkdir")


def validate_build(build: pathlib.Path, trace: bool) -> None:
    cache_path = build / "CMakeCache.txt"
    if not cache_path.is_file():
        raise SystemExit(f"missing CMake cache: {cache_path}")
    cache: dict[str, str] = {}
    for line in cache_path.read_text(
            encoding="utf-8", errors="replace").splitlines():
        if "=" not in line or ":" not in line.split("=", 1)[0]:
            continue
        key, value = line.split("=", 1)
        cache[key.split(":", 1)[0]] = value
    expected_trace = "ON" if trace else "OFF"
    if cache.get("CMAKE_BUILD_TYPE") != "RelWithDebInfo":
        raise SystemExit(
            f"{build} must use CMAKE_BUILD_TYPE=RelWithDebInfo")
    if cache.get("ORCHFS_BUILD_KFS") != "ON" or \
            cache.get("ORCHFS_REPRO_TRACE") != expected_trace:
        raise SystemExit(
            f"{build} must set ORCHFS_BUILD_KFS=ON and "
            f"ORCHFS_REPRO_TRACE={expected_trace}")
    for name in ("kfs_main", "mkfs", "libOrchFS.so",
                 "orchfs_metadata_bench"):
        path = build / name
        if not path.is_file() or (name != "libOrchFS.so" and
                                  not os.access(path, os.X_OK)):
            raise SystemExit(f"missing benchmark artifact: {path}")


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--scale", choices=("smoke", "paper"), default="smoke")
    value.add_argument("--versions", default="async-current")
    value.add_argument("--operations", default=",".join(OPERATIONS))
    value.add_argument("--threads", default="1,16")
    value.add_argument("--operations-per-thread", type=int)
    value.add_argument("--entries", type=int)
    value.add_argument("--repeats", type=int)
    value.add_argument("--server-workers", type=int, default=16)
    value.add_argument("--trace", action="store_true")
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
                           "/tmp/orchfs-sync-baseline/build-fair"))
    value.add_argument("--async-build", type=pathlib.Path,
                       default=ROOT / "build-repro")
    value.add_argument("--async-trace-build", type=pathlib.Path,
                       default=ROOT / "build-repro-trace")
    value.add_argument("--client-workers", type=int, default=0)
    value.add_argument("--client-lanes", type=int, default=0)
    value.add_argument("--ipc-ring-capacity", type=int, default=16)
    value.add_argument("--trace-sample-every", type=int, default=1)
    value.add_argument("--case-timeout", type=float, default=600)
    value.add_argument("--allow-failures", action="store_true")
    return value


def parse_tokens(path: pathlib.Path, prefix: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.startswith(prefix + " "):
            continue
        result = {}
        for token in shlex.split(line[len(prefix) + 1:]):
            if "=" in token:
                key, value = token.split("=", 1)
                result[key] = value
    return result


def operations_for(args: argparse.Namespace, operation: str) -> int:
    if args.operations_per_thread is not None:
        return args.operations_per_thread
    if args.scale == "paper":
        return 10000 if operation in {"open", "stat"} else 1000
    return 100 if operation in {"open", "stat"} else 20


def run_sample(runner: Runner, args: argparse.Namespace, version: str,
               operation: str, threads: int, repeat: int) -> dict[str, object]:
    name = f"{operation}-t{threads}"
    run_dir = runner.raw / version / "figmetadata" / name / f"run-{repeat}"
    run_dir.mkdir(parents=True, exist_ok=False)
    traced = args.trace and version == "async-current"
    case = Case("metadata", name, threads=threads,
                workers=args.server_workers, separate_prepare=False)
    process: subprocess.Popen[str] | None = None
    exit_code = 125
    benchmark_log = run_dir / "benchmark.log"
    try:
        runner.format(version, run_dir, trace_build=traced)
        process, endpoint = runner.start_server(
            version, case, run_dir, traced=traced)
        environment = runner.client_environment(
            version, endpoint, run_dir, case, traced=traced)
        build = args.async_trace_build if traced else args.async_build
        command = [
            str(build / "orchfs_metadata_bench"),
            "--path-prefix", f"/Or/orchfs-metadata-{operation}-t{threads}-r{repeat}",
            "--operation", operation,
            "--threads", str(threads),
            "--operations-per-thread", str(operations_for(args, operation)),
            "--entries", str(args.entries),
            "--label", name,
        ]
        completed = runner.logged_run(
            runner.sudo_env(environment, command), check=False,
            timeout=args.case_timeout, stdout=benchmark_log)
        exit_code = completed.returncode
        runner.stop_server(version, process)
        process = None
    except (OSError, RuntimeError, subprocess.SubprocessError) as error:
        (run_dir / "runner-error.log").write_text(
            f"{type(error).__name__}: {error}\n", encoding="utf-8")
        exit_code = 124 if isinstance(error, subprocess.TimeoutExpired) else 125
    finally:
        if process is not None:
            runner.stop_server(version, process)

    parsed = parse_tokens(benchmark_log, "orchfs_metadata_result") \
        if benchmark_log.exists() else {}
    failure = parse_tokens(benchmark_log, "orchfs_metadata_error") \
        if benchmark_log.exists() else {}
    reason = ""
    if exit_code != 0:
        reason = (f"errno-{failure.get('errno')}" if failure else
                  f"exit-{exit_code}")
    elif not parsed:
        reason = "missing-result"
    return {
        "operation": operation,
        "threads": threads,
        "entries": args.entries,
        "operations_per_thread": operations_for(args, operation),
        "version": version,
        "scale": args.scale,
        "repeat": repeat,
        "trace": int(traced),
        "status": "ok" if not reason else "failed",
        "exit_code": exit_code,
        "operations": parsed.get("operations", ""),
        "started_ns": parsed.get("started_ns", ""),
        "ended_ns": parsed.get("ended_ns", ""),
        "seconds": parsed.get("seconds", ""),
        "ops_per_s": parsed.get("ops_per_s", ""),
        "mean_us": parsed.get("mean_us", ""),
        "p50_us": parsed.get("p50_us", ""),
        "p99_us": parsed.get("p99_us", ""),
        "returned_items": parsed.get("returned_items", ""),
        "items_per_operation": parsed.get("items_per_operation", ""),
        "failure_reason": reason,
        "raw_dir": str(run_dir),
        "commit": runner.commits[version],
    }


def write_results(output: pathlib.Path,
                  rows: list[dict[str, object]]) -> None:
    with (output / "csv/metadata_benchmark.csv").open(
            "w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def report(output: pathlib.Path, rows: list[dict[str, object]]) -> None:
    successful = [row for row in rows if row["status"] == "ok"]
    lines = [
        "# OrchFS metadata microbenchmark",
        "",
        f"Successful samples: {len(successful)}/{len(rows)}.",
        "",
        "| Operation | Threads | Version | Trace | Mean ops/s | Mean p99 us |",
        "| --- | ---: | --- | ---: | ---: | ---: |",
    ]
    keys = sorted({
        (str(row["operation"]), int(row["threads"]), str(row["version"]),
         int(row["trace"])) for row in successful
    })
    for operation, threads, version, traced in keys:
        samples = [row for row in successful
                   if row["operation"] == operation and
                   row["threads"] == threads and row["version"] == version and
                   row["trace"] == traced]
        throughput = statistics.fmean(float(row["ops_per_s"])
                                      for row in samples)
        p99 = statistics.fmean(float(row["p99_us"]) for row in samples)
        lines.append(
            f"| {operation} | {threads} | {version} | {traced} | "
            f"{throughput:.3f} | {p99:.3f} |")
    failed = [row for row in rows if row["status"] != "ok"]
    if failed:
        lines += ["", "## Failed samples", "",
                  "| Operation | Threads | Version | Reason |",
                  "| --- | ---: | --- | --- |"]
        lines += [
            f"| {row['operation']} | {row['threads']} | {row['version']} | "
            f"{row['failure_reason']} |" for row in failed
        ]
    (output / "report.md").write_text("\n".join(lines) + "\n",
                                      encoding="utf-8")


def main() -> int:
    args = parser().parse_args()
    args.bdf = args.bdf.lower()
    versions = [item.strip() for item in args.versions.split(",") if item.strip()]
    operations = [item.strip() for item in args.operations.split(",")
                  if item.strip()]
    thread_counts = [int(item) for item in args.threads.split(",") if item.strip()]
    if not versions or not operations or not thread_counts:
        raise SystemExit("versions, operations, and threads must be non-empty")
    if any(version not in {"sync-fair", "async-current"}
           for version in versions):
        raise SystemExit("invalid --versions")
    if any(operation not in OPERATIONS for operation in operations):
        raise SystemExit("invalid --operations")
    if any(threads <= 0 for threads in thread_counts):
        raise SystemExit("--threads must contain positive integers")
    if args.server_workers <= 0 or args.server_workers > 64:
        raise SystemExit("--server-workers must be in [1, 64]")
    if args.entries is None:
        args.entries = 256 if args.scale == "paper" else 32
    if args.entries <= 0:
        raise SystemExit("--entries must be positive")
    if args.repeats is None:
        args.repeats = 5 if args.scale == "paper" else 1
    if args.repeats <= 0:
        raise SystemExit("--repeats must be positive")
    if args.operations_per_thread is not None and \
            args.operations_per_thread <= 0:
        raise SystemExit("--operations-per-thread must be positive")
    if not args.trace or "sync-fair" in versions:
        validate_build(args.async_build, False)
    if args.trace and "async-current" in versions:
        validate_build(args.async_trace_build, True)

    timestamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    output = (args.output or ROOT / "benchmark-results" /
              f"metadata-reproduction-{timestamp}").resolve()
    output.mkdir(parents=True, exist_ok=False)
    runner = Runner(args, output)
    append_manifest(output, {
        "created_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "scale": args.scale,
        "versions": ",".join(versions),
        "operations": ",".join(operations),
        "threads": ",".join(str(value) for value in thread_counts),
        "repeats": args.repeats,
        "trace": int(args.trace),
        "trace_sample_every": args.trace_sample_every,
        "entries": args.entries,
        "server_workers": args.server_workers,
        "bdf": args.bdf,
        "nsid": args.nsid,
        "dax": args.dax,
        "expected_model": args.expected_model,
        "expected_serial": args.expected_serial,
        "async_build": args.async_build.resolve(),
        "async_trace_build": args.async_trace_build.resolve(),
        "sync_build": args.sync_build.resolve(),
        "original_driver": runner.original_driver,
        "async_worktree_sha256": worktree_fingerprint(ROOT),
        "sync_worktree_sha256": worktree_fingerprint(args.sync_worktree),
    })
    rows: list[dict[str, object]] = []
    try:
        runner.validate()
        cases = [(operation, threads) for operation in operations
                 for threads in thread_counts]
        for repeat in range(1, args.repeats + 1):
            for case_index, (operation, threads) in enumerate(cases):
                order = (versions if (repeat + case_index) % 2 == 1
                         else list(reversed(versions)))
                for version in order:
                    rows.append(run_sample(
                        runner, args, version, operation, threads, repeat))
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

    write_csv(output / "csv/metadata_trace.csv", METADATA_FIELDS,
              summarize_metadata(output))
    report(output, rows)
    print(output)
    expected = (len(operations) * len(thread_counts) * len(versions) *
                args.repeats)
    complete = len(rows) == expected and all(
        row["status"] == "ok" for row in rows)
    return 0 if complete or args.allow_failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
