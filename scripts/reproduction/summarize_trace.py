#!/usr/bin/env python3
"""Summarize async source spans and sync eBPF maps from a result directory."""

from __future__ import annotations

import argparse
import csv
import math
import pathlib
import re
import statistics
from collections import defaultdict


STAGE_FIELDS = [
    "figure", "case", "version", "repeat", "stage", "records",
    "estimated_events", "total_ms", "mean_us", "p50_us", "p99_us",
    "bytes", "child_io_count", "errors", "sample_every", "dropped_records",
    "trace_file",
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


def summarize_async(result: pathlib.Path) -> tuple[list[dict[str, object]],
                                                    list[dict[str, object]]]:
    stage_rows: list[dict[str, object]] = []
    request_rows: list[dict[str, object]] = []
    trace_paths = sorted((result / "raw").glob(
        "async-current/fig*/*/run-*/server-trace.csv"))
    for server_path in trace_paths:
        figure, case, version, repeat = identity(server_path)
        server, server_dropped = read_trace(server_path)
        client_path = server_path.with_name("client-trace.csv")
        client, client_dropped = read_trace(client_path) if client_path.exists() else ([], 0)

        dispatch = [row for row in server if row["stage"] == "server_dispatch"]
        if dispatch:
            window_start = min(int(row["started_ns"]) for row in dispatch)
            window_end = max(int(row["ended_ns"]) for row in dispatch)
            server = [row for row in server
                      if int(row["ended_ns"]) >= window_start and
                      int(row["started_ns"]) <= window_end]

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
    ebpf = summarize_ebpf(result)
    overhead = trace_overhead(result)
    write_csv(csv_dir / "trace_stages.csv", STAGE_FIELDS, stages)
    write_csv(csv_dir / "trace_requests.csv", [
        "figure", "case", "version", "repeat", "request_id",
        "client_round_trip_us", "server_dispatch_us", "outside_server_us",
        "bytes", "error_number"], requests)
    write_csv(csv_dir / "sync_ebpf.csv", [
        "figure", "case", "version", "repeat", "metric", "value",
        "profile_file"], ebpf)
    write_csv(csv_dir / "trace_overhead.csv", [
        "version", "scale", "trace_off_MiB_per_s", "trace_on_MiB_per_s",
        "overhead_percent", "passes_3_percent_gate"], overhead)
    print(f"async stages={len(stages)} requests={len(requests)} "
          f"sync eBPF metrics={len(ebpf)} overhead rows={len(overhead)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
