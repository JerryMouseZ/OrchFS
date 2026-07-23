#!/usr/bin/env python3
"""Summarize async source spans and sync eBPF maps from a result directory."""

from __future__ import annotations

import argparse
import csv
import math
import pathlib
import re
import shlex
import statistics
from collections import defaultdict


STAGE_FIELDS = [
    "figure", "case", "version", "repeat", "stage", "records",
    "estimated_events", "total_ms", "mean_us", "p50_us", "p99_us",
    "bytes", "child_io_count", "errors", "sample_every", "dropped_records",
    "trace_file",
]

METADATA_FIELDS = [
    "figure", "case", "version", "repeat", "namespace_events",
    "namespace_wait_ns", "namespace_active_mean", "namespace_active_max",
    "namespace_peak", "dirent_scan_events", "dirents_scanned",
    "dirents_per_scan", "snapshot_copy_events", "snapshot_copy_items",
    "range_copy_events", "range_copy_items", "journal_wait_events",
    "journal_wait_ns", "journal_queue_wait_events", "journal_queue_wait_ns",
    "sample_every", "dropped_records", "trace_file",
]


def percentile(values: list[int], percent: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, math.ceil(len(ordered) * percent) - 1)
    return ordered[index] / 1000.0


def identity(path: pathlib.Path) -> tuple[str, str, str, str]:
    # .../raw/<version>/figNN/<case>/run-N/<file>
    parts = path.parts
    raw = parts.index("raw")
    version = parts[raw + 1]
    figure = parts[raw + 2].removeprefix("fig")
    case = parts[raw + 3]
    repeat = parts[raw + 4].removeprefix("run-")
    return figure, case, version, repeat


def read_trace(path: pathlib.Path) -> tuple[list[dict[str, str]], int]:
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    dropped = 0
    data: list[str] = []
    for line in lines:
        if line.startswith("# dropped_records="):
            dropped = int(line.split("=", 1)[1])
        elif line and not line.startswith("#"):
            data.append(line)
    if not data:
        return [], dropped
    return list(csv.DictReader(data)), dropped


def measurement_records(path: pathlib.Path,
                        records: list[dict[str, str]]) -> list[dict[str, str]]:
    benchmark = path.with_name("benchmark.log")
    if benchmark.exists():
        for line in reversed(benchmark.read_text(
                encoding="utf-8", errors="replace").splitlines()):
            if not line.startswith("orchfs_metadata_result "):
                continue
            fields = dict(token.split("=", 1) for token in shlex.split(
                line.removeprefix("orchfs_metadata_result ")) if "=" in token)
            if "started_ns" in fields and "ended_ns" in fields:
                try:
                    window_start = int(fields["started_ns"])
                    window_end = int(fields["ended_ns"])
                except ValueError:
                    break
                return [record for record in records
                        if int(record["started_ns"]) >= window_start and
                        int(record["ended_ns"]) <= window_end]
            break

    dispatch = [record for record in records
                if record["stage"] == "server_dispatch"]
    if not dispatch:
        return records
    window_start = min(int(record["started_ns"]) for record in dispatch)
    window_end = max(int(record["ended_ns"]) for record in dispatch)
    return [record for record in records
            if int(record["ended_ns"]) >= window_start and
            int(record["started_ns"]) <= window_end]


def summarize_async(result: pathlib.Path) -> tuple[list[dict[str, object]],
                                                    list[dict[str, object]]]:
    stage_rows: list[dict[str, object]] = []
    request_rows: list[dict[str, object]] = []
    trace_paths = sorted((result / "raw").glob(
        "async-current/fig*/*/run-*/server-trace.csv"))
    for server_path in trace_paths:
        figure, case, version, repeat = identity(server_path)
        server, server_dropped = read_trace(server_path)
        server = measurement_records(server_path, server)
        client_path = server_path.with_name("client-trace.csv")
        client, client_dropped = read_trace(client_path) if client_path.exists() else ([], 0)
        client = measurement_records(client_path, client)

        by_stage: dict[str, list[dict[str, str]]] = defaultdict(list)
        for row in server + client:
            by_stage[row["stage"]].append(row)
        for stage, records in sorted(by_stage.items()):
            durations = [int(row["duration_ns"]) for row in records]
            sample_every = max(int(row["sample_every"]) for row in records)
            stage_rows.append({
                "figure": figure,
                "case": case,
                "version": version,
                "repeat": repeat,
                "stage": stage,
                "records": len(records),
                "estimated_events": len(records) * sample_every,
                "total_ms": sum(durations) * sample_every / 1e6,
                "mean_us": statistics.fmean(durations) / 1000.0,
                "p50_us": percentile(durations, 0.50),
                "p99_us": percentile(durations, 0.99),
                "bytes": sum(int(row["bytes"]) for row in records) * sample_every,
                "child_io_count": sum(int(row["child_io_count"]) for row in records) * sample_every,
                "errors": sum(int(row["error_number"]) != 0 for row in records),
                "sample_every": sample_every,
                "dropped_records": server_dropped + client_dropped,
                "trace_file": str(server_path),
            })

        server_by_id = {int(row["request_id"]): row for row in server
                        if row["stage"] == "server_dispatch" and
                        int(row["request_id"]) != 0}
        for row in client:
            if row["stage"] != "client_round_trip":
                continue
            request_id = int(row["request_id"])
            server_row = server_by_id.get(request_id)
            client_ns = int(row["duration_ns"])
            server_ns = int(server_row["duration_ns"]) if server_row else 0
            request_rows.append({
                "figure": figure,
                "case": case,
                "version": version,
                "repeat": repeat,
                "request_id": request_id,
                "client_round_trip_us": client_ns / 1000.0,
                "server_dispatch_us": server_ns / 1000.0,
                "outside_server_us": max(client_ns - server_ns, 0) / 1000.0,
                "bytes": row["bytes"],
                "error_number": row["error_number"],
            })
    return stage_rows, request_rows


def summarize_metadata(result: pathlib.Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    metadata_stages = {
        "namespace_wait", "dirent_scan", "snapshot_map_copy",
        "range_map_copy", "journal_commit", "journal_queue_wait",
    }
    trace_paths = sorted((result / "raw").glob(
        "async-current/fig*/*/run-*/server-trace.csv"))
    for server_path in trace_paths:
        figure, case, version, repeat = identity(server_path)
        records, dropped = read_trace(server_path)
        records = measurement_records(server_path, records)
        by_stage: dict[str, list[dict[str, str]]] = defaultdict(list)
        for record in records:
            if record["stage"] in metadata_stages:
                by_stage[record["stage"]].append(record)
        if not by_stage:
            continue

        def values(stage: str, field: str) -> list[int]:
            return [int(record[field]) for record in by_stage[stage]]

        def sample(stage: str) -> int:
            return max(values(stage, "sample_every"), default=1)

        def scaled_sum(stage: str, field: str) -> int:
            return sum(values(stage, field)) * sample(stage)

        namespace_active = values("namespace_wait", "bytes")
        namespace_peak = values("namespace_wait", "child_io_count")
        dirents = values("dirent_scan", "bytes")
        snapshot_items = values("snapshot_map_copy", "bytes")
        range_items = values("range_map_copy", "bytes")
        sample_every = max(
            (int(record["sample_every"])
             for stage_records in by_stage.values()
             for record in stage_records),
            default=1)
        rows.append({
            "figure": figure,
            "case": case,
            "version": version,
            "repeat": repeat,
            "namespace_events": (len(by_stage["namespace_wait"]) *
                                 sample("namespace_wait")),
            "namespace_wait_ns": scaled_sum(
                "namespace_wait", "duration_ns"),
            "namespace_active_mean": (
                statistics.fmean(namespace_active)
                if namespace_active else 0.0),
            "namespace_active_max": max(namespace_active, default=0),
            "namespace_peak": max(namespace_peak, default=0),
            "dirent_scan_events": (len(by_stage["dirent_scan"]) *
                                   sample("dirent_scan")),
            "dirents_scanned": scaled_sum("dirent_scan", "bytes"),
            "dirents_per_scan": (statistics.fmean(dirents)
                                  if dirents else 0.0),
            "snapshot_copy_events": (
                len(by_stage["snapshot_map_copy"]) *
                sample("snapshot_map_copy")),
            "snapshot_copy_items": scaled_sum(
                "snapshot_map_copy", "bytes"),
            "range_copy_events": (len(by_stage["range_map_copy"]) *
                                  sample("range_map_copy")),
            "range_copy_items": scaled_sum("range_map_copy", "bytes"),
            "journal_wait_events": (
                len(by_stage["journal_commit"]) *
                sample("journal_commit")),
            "journal_wait_ns": scaled_sum(
                "journal_commit", "duration_ns"),
            "journal_queue_wait_events": (
                len(by_stage["journal_queue_wait"]) *
                sample("journal_queue_wait")),
            "journal_queue_wait_ns": scaled_sum(
                "journal_queue_wait", "duration_ns"),
            "sample_every": sample_every,
            "dropped_records": dropped,
            "trace_file": str(server_path),
        })
    return rows


def summarize_ebpf(result: pathlib.Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for path in sorted((result / "raw").glob(
            "sync-fair/fig*/*/run-*/profile.log")):
        figure, case, version, repeat = identity(path)
        text = path.read_text(encoding="utf-8", errors="replace")
        values: dict[str, int] = {}
        for name, value in re.findall(r"@(\w+):\s*([0-9]+)", text):
            values[name] = int(value)
        for name, value in sorted(values.items()):
            rows.append({
                "figure": figure, "case": case, "version": version,
                "repeat": repeat, "metric": name, "value": value,
                "profile_file": str(path),
            })
    return rows


def trace_overhead(result: pathlib.Path) -> list[dict[str, object]]:
    path = result / "csv/results.csv"
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    grouped: dict[tuple[str, str], dict[str, list[float]]] = defaultdict(
        lambda: defaultdict(list))
    for row in rows:
        if (row["figure"] != "04" or row["status"] != "ok" or
                row["version"] != "async-current"):
            continue
        mode = "trace_on" if row["case"].endswith("_profile") else "trace_off"
        grouped[(row["version"], row["scale"])][mode].append(
            float(row["MiB_per_s"]))
    output: list[dict[str, object]] = []
    for (version, scale), modes in grouped.items():
        if not modes["trace_on"] or not modes["trace_off"]:
            continue
        on = statistics.fmean(modes["trace_on"])
        off = statistics.fmean(modes["trace_off"])
        output.append({
            "version": version, "scale": scale,
            "trace_off_MiB_per_s": off, "trace_on_MiB_per_s": on,
            "overhead_percent": (off - on) / off * 100.0 if off else 0.0,
            "passes_3_percent_gate": abs(off - on) / off <= 0.03 if off else False,
        })
    return output


def write_csv(path: pathlib.Path, fields: list[str],
              rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("result", type=pathlib.Path)
    args = parser.parse_args()
    result = args.result.resolve()
    csv_dir = result / "csv"
    csv_dir.mkdir(exist_ok=True)
    stages, requests = summarize_async(result)
    metadata = summarize_metadata(result)
    ebpf = summarize_ebpf(result)
    overhead = trace_overhead(result)
    write_csv(csv_dir / "trace_stages.csv", STAGE_FIELDS, stages)
    write_csv(csv_dir / "trace_requests.csv", [
        "figure", "case", "version", "repeat", "request_id",
        "client_round_trip_us", "server_dispatch_us", "outside_server_us",
        "bytes", "error_number"], requests)
    write_csv(csv_dir / "metadata_trace.csv", METADATA_FIELDS, metadata)
    write_csv(csv_dir / "sync_ebpf.csv", [
        "figure", "case", "version", "repeat", "metric", "value",
        "profile_file"], ebpf)
    write_csv(csv_dir / "trace_overhead.csv", [
        "version", "scale", "trace_off_MiB_per_s", "trace_on_MiB_per_s",
        "overhead_percent", "passes_3_percent_gate"], overhead)
    print(f"async stages={len(stages)} requests={len(requests)} "
          f"metadata={len(metadata)} "
          f"sync eBPF metrics={len(ebpf)} overhead rows={len(overhead)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
