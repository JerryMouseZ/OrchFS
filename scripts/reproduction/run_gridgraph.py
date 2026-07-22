#!/usr/bin/env python3
"""Run the adapted FAST'25 Fig.18 GridGraph PageRank comparison."""

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
import time

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from run_reproduction import Case, ROOT, Runner, append_manifest


FIELDS = [
    "figure", "dataset", "version", "scale", "repeat", "status",
    "exit_code", "vertices", "edges", "partitions", "iterations",
    "memory_gib", "grid_bytes", "stage_seconds", "degree_seconds",
    "pagerank_seconds", "wall_seconds", "edge_passes", "edge_read_bytes",
    "failure_reason", "raw_dir", "commit",
]
BASE_NAMES = {"meta", "row", "row_offset", "column", "column_offset"}


def sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def grid_files(directory: pathlib.Path) -> list[pathlib.Path]:
    files = [path for path in directory.iterdir()
             if path.is_file()
             and (path.name in BASE_NAMES or path.name.startswith("block-"))]
    required = BASE_NAMES - {path.name for path in files}
    if required or not any(path.name.startswith("block-") for path in files):
        raise RuntimeError(f"incomplete GridGraph directory; missing {sorted(required)}")
    return sorted(files, key=lambda path: path.name)


def parse_meta(directory: pathlib.Path) -> tuple[int, int, int]:
    tokens = (directory / "meta").read_text(encoding="ascii").split()
    if len(tokens) != 4 or tokens[0] != "0":
        raise RuntimeError(f"unexpected unweighted GridGraph meta: {tokens}")
    return int(tokens[1]), int(tokens[2]), int(tokens[3])


def parse_pagerank(path: pathlib.Path, iterations: int) -> dict[str, str]:
    text = path.read_text(encoding="utf-8", errors="replace")
    degree = re.findall(r"degree calculation used ([0-9.]+) seconds", text)
    pagerank = re.findall(
        rf"{iterations} iterations of pagerank took ([0-9.]+) seconds", text)
    edge_spans = re.findall(
        r"gridgraph_stream_edges update_mode=\d+ expected_bytes=(\d+) "
        r"read_bytes=(\d+)", text)
    result: dict[str, str] = {}
    if degree:
        result["degree_seconds"] = degree[-1]
    if pagerank:
        result["pagerank_seconds"] = pagerank[-1]
    result["edge_passes"] = str(len(edge_spans))
    result["edge_read_bytes"] = str(sum(int(item[1]) for item in edge_spans))
    return result


def stop_after_client(runner: Runner, version: str,
                      process: subprocess.Popen[str]) -> None:
    if version == "sync-fair":
        try:
            process.wait(timeout=10)
            stream = getattr(process, "_orchfs_log_stream", None)
            if stream is not None:
                stream.close()
            return
        except subprocess.TimeoutExpired:
            pass
    runner.stop_server(version, process)


def run_sample(runner: Runner, args: argparse.Namespace,
               files: list[pathlib.Path], version: str,
               repeat: int, vertices: int, edges: int,
               partitions: int) -> dict[str, object]:
    run_dir = runner.raw / version / "fig18" / args.dataset / f"run-{repeat}"
    run_dir.mkdir(parents=True, exist_ok=False)
    case = Case("18", args.dataset, workers=args.kfs_workers)
    # The leading backslash is both wrappers' explicit OrchFS-root marker.
    # It also prevents the legacy adapter from falling back to a host file if
    # a create fails; its equivalent /Or-prefixed bulk-loader open is broken.
    root = "\\"
    process: subprocess.Popen[str] | None = None
    exit_code = 125
    stage_seconds = 0.0
    wall_seconds = 0.0
    failure = ""
    pagerank_log = run_dir / "pagerank.log"
    phase = "stage"
    try:
        runner.format(version, run_dir)
        process, endpoint = runner.start_server(
            version, case, run_dir, log_label="stage")
        environment = runner.client_environment(version, endpoint, run_dir)
        copy_command = [str(args.async_build / "orchfs_repro_copy")]
        for source in files:
            copy_command += [str(source), f"\\{source.name}"]
        assignments = [f"{key}={value}" for key, value in environment.items()]
        copy_command = [
            "sudo", "-n", "-E", "timeout", "--signal=TERM",
            "--kill-after=5", str(args.stage_timeout), "env", *assignments,
            *copy_command,
        ]
        started = time.monotonic()
        copied = runner.logged_run(
            copy_command, check=False,
            timeout=args.stage_timeout + 15, stdout=run_dir / "stage.log")
        stage_seconds = time.monotonic() - started
        exit_code = copied.returncode
        stop_after_client(runner, version, process)
        process = None
        if exit_code != 0:
            failure = ("stage-timeout" if exit_code == 124
                       else f"stage-copy-exit-{exit_code}")
        else:
            phase = "pagerank"
            process, endpoint = runner.start_server(
                version, case, run_dir, log_label="restart")
            environment = runner.client_environment(version, endpoint, run_dir)
            command = [str(args.pagerank_bin), root, str(args.iterations),
                       str(args.memory_gib)]
            assignments = [f"{key}={value}" for key, value in environment.items()]
            command = [
                "sudo", "-n", "-E", "timeout", "--signal=TERM",
                "--kill-after=5", str(args.case_timeout), "env", *assignments,
                *command,
            ]
            started = time.monotonic()
            completed = runner.logged_run(
                command, check=False,
                timeout=args.case_timeout + 15, stdout=pagerank_log)
            wall_seconds = time.monotonic() - started
            exit_code = completed.returncode
            stop_after_client(runner, version, process)
            process = None
            parsed = parse_pagerank(pagerank_log, args.iterations)
            if exit_code != 0:
                failure = ("pagerank-timeout" if exit_code == 124
                           else f"pagerank-exit-{exit_code}")
            elif "pagerank_seconds" not in parsed:
                failure = "missing-pagerank-summary"
            elif int(parsed["edge_passes"]) != args.iterations + 1:
                failure = f"edge-pass-count-{parsed['edge_passes']}"
            elif int(parsed["edge_read_bytes"]) < ((args.iterations + 1) *
                                                    edges * 8):
                failure = "short-edge-stream"
    except subprocess.TimeoutExpired:
        failure = f"{phase}-timeout"
        exit_code = 124
    except (OSError, RuntimeError) as error:
        failure = f"{type(error).__name__}: {error}"
        (run_dir / "runner-error.log").write_text(
            failure + "\n", encoding="utf-8")
    finally:
        if process is not None:
            stop_after_client(runner, version, process)

    parsed = (parse_pagerank(pagerank_log, args.iterations)
              if pagerank_log.exists() else {})
    row: dict[str, object] = {field: "" for field in FIELDS}
    row |= {
        "figure": "18", "dataset": args.dataset, "version": version,
        "scale": args.scale, "repeat": repeat,
        "status": "ok" if exit_code == 0 and not failure else "failed",
        "exit_code": exit_code, "vertices": vertices, "edges": edges,
        "partitions": partitions, "iterations": args.iterations,
        "memory_gib": args.memory_gib,
        "grid_bytes": sum(path.stat().st_size for path in files),
        "stage_seconds": f"{stage_seconds:.6f}",
        "degree_seconds": parsed.get("degree_seconds", ""),
        "pagerank_seconds": parsed.get("pagerank_seconds", ""),
        "wall_seconds": f"{wall_seconds:.6f}" if wall_seconds else "",
        "edge_passes": parsed.get("edge_passes", ""),
        "edge_read_bytes": parsed.get("edge_read_bytes", ""),
        "failure_reason": failure, "raw_dir": str(run_dir),
        "commit": runner.commits[version],
    }
    return row


def write_csv(output: pathlib.Path, rows: list[dict[str, object]]) -> None:
    target = output / "csv/fig18_gridgraph.csv"
    with target.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def plot(output: pathlib.Path, rows: list[dict[str, object]], scale: str) -> None:
    datasets = sorted({str(row["dataset"]) for row in rows})
    versions = ("sync-fair", "async-current")
    colors = {"sync-fair": "#4c78a8", "async-current": "#f58518"}
    figure, axis = plt.subplots(figsize=(7.8, 4.8), constrained_layout=True)
    width = 0.36
    x = list(range(len(datasets)))
    positive: list[float] = []
    for index, version in enumerate(versions):
        values: list[float] = []
        for dataset in datasets:
            samples = [float(row["pagerank_seconds"]) for row in rows
                       if row["dataset"] == dataset
                       and row["version"] == version
                       and row["status"] == "ok"]
            values.append(statistics.fmean(samples) if samples else math.nan)
            positive.extend(samples)
        shifted = [item + (index - 0.5) * width for item in x]
        plotted = [0.0 if math.isnan(value) else value for value in values]
        axis.bar(shifted, plotted, width=width, color=colors[version],
                 label=version)
        for position, value, dataset in zip(shifted, values, datasets):
            if math.isnan(value) and any(
                    row["dataset"] == dataset and row["version"] == version
                    and row["status"] != "ok" for row in rows):
                axis.text(position, 0.02, "failed", rotation=90,
                          ha="center", va="bottom", fontsize=7,
                          color=colors[version],
                          transform=axis.get_xaxis_transform())
    if positive and max(positive) / min(positive) > 100:
        axis.set_yscale("log")
        axis.set_ylabel("PageRank seconds (log scale, lower is better)")
    else:
        axis.set_ylabel("PageRank seconds (lower is better)")
    if not positive:
        axis.set_ylim(0, 1)
    axis.set_xticks(x, datasets)
    axis.set_title(f"Adapted paper Fig.18 GridGraph ({scale})")
    axis.grid(axis="y", alpha=0.25)
    axis.legend()
    for extension in ("png", "pdf"):
        figure.savefig(output / f"plots/fig18.{extension}", dpi=180)
    plt.close(figure)


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--scale", choices=("smoke", "paper"), default="smoke")
    value.add_argument("--versions", default="sync-fair,async-current")
    value.add_argument("--repeats", type=int)
    value.add_argument("--dataset", default="LiveJournal")
    value.add_argument("--grid-dir", type=pathlib.Path, required=True)
    value.add_argument("--gridgraph-root", type=pathlib.Path, required=True)
    value.add_argument("--pagerank-bin", type=pathlib.Path, required=True)
    value.add_argument("--source-archive", type=pathlib.Path)
    value.add_argument("--iterations", type=int, default=20)
    value.add_argument("--memory-gib", type=int, default=8)
    value.add_argument("--kfs-workers", type=int, default=16)
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
    value.add_argument("--stage-timeout", type=float, default=300)
    value.add_argument("--case-timeout", type=float, default=1800)
    value.add_argument(
        "--allow-failures", action="store_true",
        help="write partial reports but return success when samples fail")
    return value


def main() -> int:
    args = parser().parse_args()
    args.bdf = args.bdf.lower()
    args.grid_dir = args.grid_dir.resolve()
    args.gridgraph_root = args.gridgraph_root.resolve()
    args.pagerank_bin = args.pagerank_bin.resolve()
    args.repeats = args.repeats or (1 if args.scale == "smoke" else 3)
    versions = [item.strip() for item in args.versions.split(",") if item.strip()]
    if any(item not in {"sync-fair", "async-current"} for item in versions):
        raise SystemExit("invalid --versions")
    if not args.pagerank_bin.is_file():
        raise SystemExit(f"missing pagerank binary: {args.pagerank_bin}")
    files = grid_files(args.grid_dir)
    vertices, edges, partitions = parse_meta(args.grid_dir)
    timestamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    output = (args.output or ROOT / "benchmark-results" /
              f"gridgraph-reproduction-{timestamp}").resolve()
    output.mkdir(parents=True, exist_ok=False)
    runner = Runner(args, output)
    rows: list[dict[str, object]] = []
    manifest = {
        "scale": args.scale,
        "dataset": args.dataset,
        "vertices": vertices,
        "edges": edges,
        "partitions": partitions,
        "iterations": args.iterations,
        "memory_gib": args.memory_gib,
        "grid_bytes": sum(path.stat().st_size for path in files),
        "gridgraph_commit": subprocess.check_output(
            ["git", "-C", str(args.gridgraph_root), "rev-parse", "HEAD"],
            text=True).strip(),
        "pagerank_sha256": sha256(args.pagerank_bin),
        "compat_patch_sha256": sha256(
            ROOT / "scripts/reproduction/gridgraph-compat.patch"),
        "source_archive_sha256": (
            sha256(args.source_archive.resolve())
            if args.source_archive and args.source_archive.is_file()
            else "not-recorded"),
        "interleave": "per-dataset, direction reversed by repeat",
        "vertex_mapping": (
            "ORCHFS_GRIDGRAPH_ANON_VERTEX=1; same binary for both versions; "
            "required because neither public OrchFS adapter exposes mmap on virtual fds"),
        "io_validation": (
            "ORCHFS_GRIDGRAPH_RECORD_IO=1; require iterations+1 complete edge passes"),
    }
    with (output / "manifest.tsv").open("w", encoding="utf-8") as stream:
        stream.write("key\tvalue\n")
        for key, value in manifest.items():
            stream.write(f"{key}\t{value}\n")
    try:
        runner.validate()
        for repeat in range(1, args.repeats + 1):
            order = versions if repeat % 2 == 1 else list(reversed(versions))
            for version in order:
                rows.append(run_sample(
                    runner, args, files, version, repeat,
                    vertices, edges, partitions))
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
    failure_rows = "".join(
        f"| {row['dataset']} | {row['version']} | {row['failure_reason']} |\n"
        for row in failures) or "| - | - | none |\n"
    success_rows = "".join(
        f"| {row['dataset']} | {row['version']} | {row['stage_seconds']} | "
        f"{row['degree_seconds']} | {row['pagerank_seconds']} | "
        f"{row['edge_passes']} |\n"
        for row in rows if row["status"] == "ok") or "| - | - | - | - | - |\n"
    (output / "report.md").write_text(
        "# Adapted paper Fig.18 GridGraph\n\n"
        f"Scale: `{args.scale}`; iterations: {args.iterations}; memory: "
        f"{args.memory_gib} GiB; repeats: {args.repeats}.\n\n"
        "The same compatibility-patched GridGraph binary was used for both "
        "OrchFS versions. Vertex arrays are anonymous memory because the "
        "public virtual-fd adapters do not implement mmap; edge files remain "
        "on OrchFS and are streamed through pread.\n\n"
        "| Dataset | Version | Stage s | Degree s | PageRank s | Edge passes |\n"
        "| --- | --- | ---: | ---: | ---: | ---: |\n" + success_rows +
        "\n| Dataset | Version | Failure |\n"
        "| --- | --- | --- |\n" + failure_rows,
        encoding="utf-8")
    expected = len(versions) * args.repeats
    complete = len(rows) == expected and not failures
    return 0 if complete or args.allow_failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
