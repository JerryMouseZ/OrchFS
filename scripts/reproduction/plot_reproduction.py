#!/usr/bin/env python3
"""Validate, split, plot, and report an OrchFS differential result set."""

from __future__ import annotations

import argparse
import csv
import math
import pathlib
import re
import statistics
from collections import defaultdict

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import Patch  # noqa: E402


FIGURE_FILES = {
    "01": "fig01_random_write.csv",
    "02": "fig02_write_latency.csv",
    "03": "fig03_alignment.csv",
    "04": "fig04_profile.csv",
    "05": "fig05_split_io.csv",
    "10": "fig10_write_latency.csv",
    "11": "fig11_parallel.csv",
    "12": "fig12_workers.csv",
    "13": "fig13_read_timeseries.csv",
    "14": "fig14_thread_scale.csv",
    "15": "fig15_single_device.csv",
}


def load_csv(path: pathlib.Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as stream:
        return list(csv.DictReader(stream))


def write_rows(path: pathlib.Path, rows: list[dict[str, str]],
               fields: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def validate(rows: list[dict[str, str]]) -> tuple[str, dict[str, str]]:
    if not rows:
        raise ValueError("results.csv has no rows")
    scales = {row["scale"] for row in rows}
    if len(scales) != 1:
        raise ValueError(f"mixed scales are not plottable: {sorted(scales)}")
    commits: dict[str, str] = {}
    for row in rows:
        version, commit = row["version"], row["commit"]
        previous = commits.setdefault(version, commit)
        if previous != commit:
            raise ValueError(f"mixed commits for {version}: {previous}, {commit}")
    return next(iter(scales)), commits


def mean_std(values: list[float]) -> tuple[float, float]:
    if not values:
        return math.nan, math.nan
    return statistics.fmean(values), statistics.stdev(values) if len(values) > 1 else 0.0


def ordered_cases(figure: str, names: set[str]) -> list[str]:
    def key(name: str) -> tuple[object, ...]:
        size = re.search(r"(?:_|^)(\d+)KiB", name)
        workers = re.search(r"workers(\d+)", name)
        threads = re.search(r"_t(\d+)$", name)
        alignment = 0 if name.startswith("aligned_") else 1
        if figure == "02":
            return (0 if name.startswith("sequential_") else 1, name)
        if figure == "03" and size:
            return (int(size.group(1)), alignment)
        if figure == "05" and size:
            return (int(size.group(1)),
                    0 if name.startswith("single_") else 1)
        if figure == "10":
            return ((0, int(size.group(1))) if size else (1, name))
        if figure == "11" and size and workers:
            return (int(size.group(1)), int(workers.group(1)))
        if figure == "12" and workers:
            return (int(workers.group(1)),)
        if figure in {"14", "15"} and threads:
            return (name[:threads.start()], int(threads.group(1)))
        return (name,)
    return sorted(names, key=key)


def plot_generic(result: pathlib.Path, figure: str,
                 rows: list[dict[str, str]], scale: str) -> None:
    successful = [row for row in rows if row["status"] == "ok"]
    if not successful:
        return
    metric = "avg_latency_us" if figure in {"02", "03", "10", "11", "12"} else "MiB_per_s"
    ylabel = "Average latency (us)" if metric == "avg_latency_us" else "Throughput (MiB/s)"
    cases = ordered_cases(figure, {row["case"] for row in successful})
    versions = [version for version in ("sync-fair", "async-current")
                if any(row["version"] == version for row in rows)]
    grouped: dict[tuple[str, str], list[float]] = defaultdict(list)
    for row in successful:
        grouped[(row["case"], row["version"])].append(float(row[metric]))

    width = 0.36 if len(versions) == 2 else 0.65
    positions = list(range(len(cases)))
    fig_width = max(7.0, min(22.0, len(cases) * 0.65 + 2.5))
    fig, axis = plt.subplots(figsize=(fig_width, 4.6), constrained_layout=True)
    colors = {"sync-fair": "#4c78a8", "async-current": "#f58518"}
    for version_index, version in enumerate(versions):
        shift = (version_index - (len(versions) - 1) / 2) * width
        means, errors = [], []
        for case in cases:
            mean, deviation = mean_std(grouped[(case, version)])
            means.append(mean)
            errors.append(0.0 if math.isnan(deviation) else deviation)
        shifted = [position + shift for position in positions]
        if any(not math.isnan(mean) for mean in means):
            axis.bar(shifted, means, width, yerr=errors, capsize=2,
                     label=version, color=colors[version], alpha=0.9)
        else:
            axis.bar([], [], width, label=version,
                     color=colors[version], alpha=0.9)
        for position, case, mean in zip(positions, cases, means):
            if math.isnan(mean) and any(
                    row["case"] == case and row["version"] == version
                    and row["status"] != "ok" for row in rows):
                axis.text(position + shift, 0.02, "failed", rotation=90,
                          ha="center", va="bottom", fontsize=7,
                          color=colors[version],
                          transform=axis.get_xaxis_transform())
    axis.set_xticks(positions, cases, rotation=45, ha="right")
    axis.set_ylabel(ylabel)
    axis.set_title(f"Adapted paper Fig.{int(figure)}: sync-fair vs async-current ({scale})")
    axis.grid(axis="y", alpha=0.25)
    axis.legend(handles=[Patch(facecolor=colors[version], label=version,
                               alpha=0.9)
                         for version in versions], loc="upper right")
    if metric == "avg_latency_us" and any(float(row[metric]) > 10000 for row in successful):
        axis.set_yscale("log")
    for extension in ("png", "pdf"):
        fig.savefig(result / "plots" / f"fig{figure}.{extension}", dpi=180)
    plt.close(fig)


def plot_timeseries(result: pathlib.Path, rows: list[dict[str, str]], scale: str) -> None:
    successful = [row for row in rows if row["status"] == "ok" and row["series_file"]]
    if not successful:
        return
    cases = ordered_cases("13", {row["case"] for row in successful})
    figure, axes = plt.subplots(len(cases), 1, figsize=(8, 3.0 * len(cases)),
                                squeeze=False, constrained_layout=True)
    colors = {"sync-fair": "#4c78a8", "async-current": "#f58518"}
    for index, case in enumerate(cases):
        axis = axes[index][0]
        series_by_version: dict[str, list[list[dict[str, str]]]] = defaultdict(list)
        for row in successful:
            if row["case"] != case:
                continue
            duration = float(row["duration_sec"] or 0)
            points = load_csv(pathlib.Path(row["series_file"]))
            if duration > 0:
                points = [point for point in points
                          if float(point["interval_end_s"]) <= duration + 1e-9]
            series_by_version[row["version"]].append(points)
        for version, runs in series_by_version.items():
            slots: dict[float, list[float]] = defaultdict(list)
            for run in runs:
                for point in run:
                    slots[float(point["interval_end_s"])].append(float(point["MiB_per_s"]))
            x = sorted(slots)
            y = [statistics.fmean(slots[value]) for value in x]
            axis.plot(x, y, marker="o", markersize=3, label=version,
                      color=colors.get(version))
        axis.set_title(case)
        axis.set_xlabel("Elapsed time (s)")
        axis.set_ylabel("MiB/s")
        axis.grid(alpha=0.25)
        axis.legend()
        positive = [value for runs in series_by_version.values()
                    for run in runs for point in run
                    if (value := float(point["MiB_per_s"])) > 0]
        if positive and max(positive) / min(positive) > 100:
            axis.set_yscale("log")
            axis.set_ylabel("MiB/s (log scale)")
    figure.suptitle(f"Adapted paper Fig.13 read time series ({scale})")
    for extension in ("png", "pdf"):
        figure.savefig(result / "plots" / f"fig13.{extension}", dpi=180)
    plt.close(figure)


def plot_trace_breakdown(result: pathlib.Path, scale: str) -> None:
    rows = load_csv(result / "csv/trace_stages.csv")
    rows = [row for row in rows if row["figure"] == "04"]
    if not rows:
        return
    grouped: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        grouped[row["stage"]].append(float(row["mean_us"]))
    stages = sorted(grouped)
    values = [statistics.fmean(grouped[stage]) for stage in stages]
    fig, axis = plt.subplots(figsize=(max(7, len(stages) * 0.8), 4.6),
                             constrained_layout=True)
    axis.bar(stages, values, color="#f58518")
    axis.set_yscale("log")
    axis.set_ylabel("Mean sampled span (us, log scale)")
    axis.set_title(f"Async source-span breakdown ({scale}; spans may overlap)")
    axis.tick_params(axis="x", rotation=45)
    axis.grid(axis="y", alpha=0.25)
    for extension in ("png", "pdf"):
        fig.savefig(result / "plots" / f"fig04_async_breakdown.{extension}", dpi=180)
    plt.close(fig)


def comparison_rows(rows: list[dict[str, str]]) -> list[tuple[str, str, float, float, float]]:
    grouped: dict[tuple[str, str, str], list[float]] = defaultdict(list)
    for row in rows:
        if row["status"] == "ok":
            grouped[(row["figure"], row["case"], row["version"])].append(
                float(row["MiB_per_s"]))
    output = []
    cases = sorted({(figure, case) for figure, case, _ in grouped})
    for figure, case in cases:
        sync = grouped.get((figure, case, "sync-fair"), [])
        async_values = grouped.get((figure, case, "async-current"), [])
        if not sync or not async_values:
            continue
        sync_mean = statistics.fmean(sync)
        async_mean = statistics.fmean(async_values)
        output.append((figure, case, sync_mean, async_mean,
                       async_mean / sync_mean if sync_mean else math.nan))
    return output


def write_report(result: pathlib.Path, rows: list[dict[str, str]], scale: str,
                 commits: dict[str, str]) -> None:
    successes = [row for row in rows if row["status"] == "ok"]
    failures = [row for row in rows if row["status"] != "ok"]
    comparisons = comparison_rows(rows)
    overhead = load_csv(result / "csv/trace_overhead.csv")
    statuses = load_csv(result / "experiment_status.csv")
    lines = [
        "# OrchFS 同步/异步差分复现报告",
        "",
        f"- 规模：`{scale}`",
        f"- 成功样本：{len(successes)}；失败样本：{len(failures)}；总样本：{len(rows)}",
    ]
    for version, commit in sorted(commits.items()):
        lines.append(f"- {version} commit：`{commit}`")
    if scale == "smoke":
        lines += ["", "> smoke 只验证路径、语义和趋势，不能作为论文规模结果。"]

    lines += ["", "## 成功的同 case 吞吐对比", "",
              "| Figure | Case | sync MiB/s | async MiB/s | async/sync |",
              "| --- | --- | ---: | ---: | ---: |"]
    for figure, case, sync, async_value, ratio in comparisons:
        lines.append(f"| {figure} | {case} | {sync:.3f} | {async_value:.3f} | {ratio:.3f}x |")
    if not comparisons:
        lines.append("| - | 没有两边都成功的同 case | - | - | - |")

    lines += ["", "## 失败样本", "",
              "| Figure | Case | Version | exit | stage | errno | raw |",
              "| --- | --- | --- | ---: | --- | ---: | --- |"]
    for row in failures:
        raw = pathlib.Path(row["raw_dir"])
        lines.append(
            f"| {row['figure']} | {row['case']} | {row['version']} | "
            f"{row['exit_code']} | {row['error_stage'] or '-'} | "
            f"{row['error_number'] or '-'} | `{raw}` |")
    if not failures:
        lines.append("| - | 无 | - | - | - | - | - |")

    lines += ["", "## Trace 扰动", ""]
    if overhead:
        lines += ["| Version | off MiB/s | on MiB/s | overhead | 3% gate |",
                  "| --- | ---: | ---: | ---: | --- |"]
        for row in overhead:
            lines.append(
                f"| {row['version']} | {float(row['trace_off_MiB_per_s']):.3f} | "
                f"{float(row['trace_on_MiB_per_s']):.3f} | "
                f"{float(row['overhead_percent']):.2f}% | "
                f"{row['passes_3_percent_gate']} |")
    else:
        lines.append("未形成 trace-on/off 成对成功样本。")

    lines += ["", "## 未运行或不适用", "",
              "| Figure | Experiment | Status | Reason |",
              "| --- | --- | --- | --- |"]
    for row in statuses:
        lines.append(f"| {row['figure']} | {row['experiment']} | {row['status']} | {row['reason']} |")
    lines += ["", "## 输出", "",
              "原始日志位于 `raw/`，规范化 CSV 位于 `csv/`，重绘的 PNG/PDF 位于 `plots/`。",
              "任何失败项都保留为 failed/not-run，不用 0 或论文图中的数值填充。", ""]
    (result / "report.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("result", type=pathlib.Path)
    args = parser.parse_args()
    result = args.result.resolve()
    rows = load_csv(result / "csv/results.csv")
    scale, commits = validate(rows)
    fields = list(rows[0])
    for figure, filename in FIGURE_FILES.items():
        selected = [row for row in rows if row["figure"] == figure]
        write_rows(result / "csv" / filename, selected, fields)
        if figure == "13":
            plot_timeseries(result, selected, scale)
        else:
            plot_generic(result, figure, selected, scale)
    plot_trace_breakdown(result, scale)
    write_report(result, rows, scale, commits)
    print(result / "report.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
