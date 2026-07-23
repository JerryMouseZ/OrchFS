#!/usr/bin/env python3
"""Run the FAST'25 sync-fair versus async-current differential suite.

The runner is deliberately conservative: it accepts one exact destructive
token, identifies the namespace by PCI BDF/NSID/model/serial, refuses mounted
or partitioned targets, uses a fresh OrchFS format for every sample, and keeps
failed samples in the result set instead of silently dropping them.
"""

from __future__ import annotations

import argparse
import csv
import dataclasses
import datetime as dt
import hashlib
import json
import os
import pathlib
import re
import shlex
import signal
import stat
import subprocess
import sys
import time
from typing import Iterable


ROOT = pathlib.Path(__file__).resolve().parents[2]
MIB = 1024**2
GIB = 1024**3
IPC_DATA_SLOT_SIZE = 2 * MIB + 128
SEED = 20260722
RESULT_FIELDS = [
    "figure", "case", "version", "scale", "repeat", "status",
    "exit_code", "operation", "access", "size_mode", "fixed_size",
    "min_size", "max_size", "threads", "files", "kfs_workers",
    "client_workers",
    "kfs_cpu_list", "client_cpu_list", "client_lanes",
    "ipc_ring_capacity", "fsync",
    "offset_alignment", "unaligned_to", "prepare_bytes",
    "bytes_per_thread", "duration_sec", "seconds", "MiB_per_s", "IOPS",
    "avg_latency_us", "p50_us", "p99_us", "error_stage", "error_number",
    "profile", "trace", "series_file", "raw_dir", "commit",
]


@dataclasses.dataclass(frozen=True)
class Case:
    figure: str
    name: str
    operation: str = "write"
    access: str = "random"
    size_mode: str = "fixed"
    size: int = 64 * 1024
    min_size: int = 64 * 1024
    max_size: int = 64 * 1024
    prepare_bytes: int = 16 * MIB
    bytes_per_thread: int = 4 * MIB
    duration_sec: float = 0.0
    threads: int = 1
    files: int = 1
    fsync: str = "each"
    offset_alignment: int = 1
    unaligned_to: int = 0
    # Zero selects one KFS worker per benchmark thread, capped at the default
    # 16-worker service width.  A single outstanding POSIX request otherwise
    # pays two needless cross-worker handoffs between its SHM lane and inode
    # owner; the worker-scaling figures set this field explicitly.
    workers: int = 0
    series_ms: int = 0
    # Recreate the KFS/LibFS processes between prefill and measurement.  This
    # is the closest available equivalent of the paper's cache-clear boundary
    # and avoids comparing a warm in-process extent snapshot with a fresh one.
    separate_prepare: bool = True
    profile: bool = False
    trace: bool = False


def size_cases(scale: str, figures: set[str], *,
               prefill_override: int | None = None,
               measured_override: int | None = None) -> list[Case]:
    paper = scale == "paper"
    prefill = (prefill_override if prefill_override is not None else
               10 * GIB if paper else 16 * MIB)
    measured = (measured_override if measured_override is not None else
                10 * GIB if paper else 4 * MIB)
    custom_prefill = prefill_override is not None
    custom_measured = measured_override is not None
    cases: list[Case] = []

    def add(case: Case) -> None:
        if case.figure in figures:
            if case.workers == 0:
                case = dataclasses.replace(
                    case, workers=max(1, min(16, case.threads)))
            cases.append(case)

    add(Case("01", "uniform_1B_2MiB_fsync_end", size_mode="uniform",
             min_size=1, max_size=2 * MIB, prepare_bytes=prefill,
             bytes_per_thread=measured, fsync="end"))
    add(Case("01", "pm10_1MiB_fsync_end", size_mode="pm10", size=MIB,
             prepare_bytes=prefill, bytes_per_thread=measured, fsync="end"))

    add(Case("02", "sequential_1MiB_fsync_each", access="sequential",
             size=MIB, prepare_bytes=prefill,
             bytes_per_thread=measured if paper or custom_measured else 8 * MIB,
             fsync="each", offset_alignment=MIB))
    add(Case("02", "random_1B_2MiB_fsync_each", size_mode="uniform",
             min_size=1, max_size=2 * MIB, prepare_bytes=prefill,
             bytes_per_thread=measured if paper or custom_measured else 8 * MIB,
             fsync="each"))

    alignment_sizes = ([4, 8, 16, 32, 64, 128, 256, 512, 1024]
                       if paper else [4, 64, 1024])
    for kib in alignment_sizes:
        request = kib * 1024
        sample_bytes = (measured if paper or custom_measured else
                        max(MIB, min(4 * MIB, request * 32)))
        local_prefill = (prefill if paper or custom_prefill else
                         max(4 * MIB, request * 16))
        add(Case("03", f"aligned_{kib}KiB", size=request,
                 prepare_bytes=local_prefill, bytes_per_thread=sample_bytes,
                 offset_alignment=request))
        add(Case("03", f"unaligned_{kib}KiB", size=request,
                 prepare_bytes=local_prefill, bytes_per_thread=sample_bytes,
                 offset_alignment=request, unaligned_to=4096))

    add(Case("04", "fixed_64KiB_trace_off", size=64 * 1024,
             prepare_bytes=prefill, bytes_per_thread=measured,
             offset_alignment=64 * 1024, separate_prepare=True))
    add(Case("04", "fixed_64KiB_profile", size=64 * 1024,
             prepare_bytes=prefill, bytes_per_thread=measured,
             offset_alignment=64 * 1024, profile=True, trace=True))

    split_sizes = [64, 128, 256, 512, 1024] if paper else [64, 256, 1024]
    for kib in split_sizes:
        request = kib * 1024
        add(Case("05", f"single_{kib}KiB", size=request,
                 prepare_bytes=prefill, bytes_per_thread=measured,
                 offset_alignment=request))
        split_threads = max(1, request // (32 * 1024))
        add(Case("05", f"split32_{kib}KiB", size=32 * 1024,
                 prepare_bytes=prefill,
                 bytes_per_thread=max(32 * 1024, measured // split_threads),
                 threads=split_threads, files=1,
                 offset_alignment=32 * 1024,
                 workers=16 if split_threads >= 8 else split_threads))

    evaluation_sizes = ([1, 4, 8, 16, 32, 64, 128, 256, 512, 1024]
                        if paper else [4, 64, 1024])
    for kib in evaluation_sizes:
        add(Case("10", f"pm10_{kib}KiB", size_mode="pm10",
                 size=kib * 1024, prepare_bytes=prefill,
                 bytes_per_thread=measured))
    add(Case("10", "uniform_1B_2MiB", size_mode="uniform", min_size=1,
             max_size=2 * MIB, prepare_bytes=prefill,
             bytes_per_thread=measured))

    parallel_sizes = [64, 128, 256, 512, 1024] if paper else [64, 1024]
    for kib in parallel_sizes:
        for workers in (1, 16):
            add(Case("11", f"workers{workers}_{kib}KiB", size=kib * 1024,
                     prepare_bytes=prefill, bytes_per_thread=measured,
                     offset_alignment=kib * 1024, workers=workers))

    worker_values = [1, 2, 4, 8, 16, 32, 48, 64]
    worker_sizes = [64, 128, 256, 512, 1024] if paper else [256]
    for kib in worker_sizes:
        for workers in worker_values:
            add(Case("12", f"workers{workers}_{kib}KiB", size=kib * 1024,
                     prepare_bytes=prefill, bytes_per_thread=measured,
                     offset_alignment=kib * 1024, workers=workers))

    duration = 30.0 if paper else 5.0
    read_distributions = [
        ("pm10_1MiB", "pm10", MIB, MIB, MIB),
        ("uniform_1B_2MiB", "uniform", MIB, 1, 2 * MIB),
        ("pm10_4KiB", "pm10", 4 * 1024, 4 * 1024, 4 * 1024),
        ("uniform_1B_128KiB", "uniform", 64 * 1024, 1, 128 * 1024),
    ]
    for name, mode, fixed, minimum, maximum in read_distributions:
        add(Case("13", name, operation="read", size_mode=mode, size=fixed,
                 min_size=minimum, max_size=maximum, prepare_bytes=prefill,
                 bytes_per_thread=0, duration_sec=duration, fsync="none",
                 series_ms=1000))

    thread_values = list(range(1, 11)) + [12, 14, 16] if paper else [1, 4, 16]
    multi_distributions = [
        ("pm10_4KiB", "pm10", 4 * 1024, 4 * 1024, 4 * 1024),
        ("pm10_1MiB", "pm10", MIB, MIB, MIB),
        ("uniform_1B_2MiB", "uniform", MIB, 1, 2 * MIB),
    ]
    for distribution, mode, fixed, minimum, maximum in multi_distributions:
        for threads in thread_values:
            per_file_prefill = (5 * GIB if paper else
                                prefill if custom_prefill else 2 * MIB)
            add(Case("14", f"multifile_{distribution}_t{threads}",
                     size_mode=mode, size=fixed, min_size=minimum,
                     max_size=maximum, prepare_bytes=per_file_prefill,
                     bytes_per_thread=(3 * GIB if paper else
                                       measured if custom_measured else MIB),
                     threads=threads, files=16))
    for threads in thread_values:
        add(Case("14", f"singlefile_uniform_1B_2MiB_t{threads}",
                 size_mode="uniform", size=MIB, min_size=1,
                 max_size=2 * MIB, prepare_bytes=prefill,
                 bytes_per_thread=(3 * GIB if paper else
                                   measured if custom_measured else MIB),
                 threads=threads, files=1))

    for access in ("append", "random"):
        for threads in thread_values:
            add(Case("15", f"{access}_uniform_1B_2MiB_t{threads}",
                     access=access, size_mode="uniform", size=MIB,
                     min_size=1, max_size=2 * MIB,
                     prepare_bytes=0 if access == "append" else prefill,
                     bytes_per_thread=(3 * GIB if paper else
                                       measured if custom_measured else MIB),
                     threads=threads, files=threads))
    return cases


class Runner:
    def __init__(self, args: argparse.Namespace, output: pathlib.Path):
        self.args = args
        self.output = output
        self.raw = output / "raw"
        self.csv_dir = output / "csv"
        self.trace_dir = output / "trace"
        self.plot_dir = output / "plots"
        for directory in (self.raw, self.csv_dir, self.trace_dir,
                          self.plot_dir):
            directory.mkdir(parents=True, exist_ok=False)
        self.command_log = (output / "commands.log").open("w", encoding="utf-8")
        self.original_driver = self.driver()
        self.block_device: pathlib.Path | None = None
        self.commits = {
            "sync-fair": self.git_commit(args.sync_worktree),
            "async-current": self.git_commit(ROOT),
        }
        self.rows: list[dict[str, object]] = []
        self._async_cpu_order: list[int] | None = None

    def close(self) -> None:
        self.command_log.close()

    @staticmethod
    def git_commit(path: pathlib.Path) -> str:
        return subprocess.check_output(
            ["git", "-C", str(path), "rev-parse", "HEAD"], text=True
        ).strip()

    @staticmethod
    def parse_linux_cpu_list(text: str) -> set[int]:
        cpus: set[int] = set()
        for item in text.strip().split(","):
            if not item:
                continue
            bounds = item.split("-", 1)
            first = int(bounds[0])
            last = int(bounds[-1])
            if first < 0 or last < first:
                raise ValueError(f"invalid Linux CPU list: {text!r}")
            cpus.update(range(first, last + 1))
        return cpus

    @staticmethod
    def cpu_sibling_layers(cpus: Iterable[int]) -> list[list[int]]:
        groups: dict[tuple[int, int], list[int]] = {}
        for cpu in sorted(cpus):
            topology = pathlib.Path(f"/sys/devices/system/cpu/cpu{cpu}/topology")
            try:
                package = int((topology / "physical_package_id").read_text())
                core = int((topology / "core_id").read_text())
                key = (package, core)
            except (OSError, ValueError):
                key = (cpu, cpu)
            groups.setdefault(key, []).append(cpu)

        siblings = sorted(groups.values(), key=lambda group: group[0])
        return [
            [group[index] for group in siblings if index < len(group)]
            for index in range(max((len(group) for group in siblings),
                                   default=0))
        ]

    def async_cpu_order(self) -> list[int]:
        if self._async_cpu_order is not None:
            return self._async_cpu_order

        allowed = set(os.sched_getaffinity(0))
        if not allowed:
            raise RuntimeError("the reproduction runner has an empty CPU affinity")
        local: set[int] = set()
        numa_path = pathlib.Path(
            f"/sys/bus/pci/devices/{self.args.bdf}/numa_node")
        try:
            numa_node = int(numa_path.read_text().strip())
            if numa_node >= 0:
                local = self.parse_linux_cpu_list(pathlib.Path(
                    f"/sys/devices/system/node/node{numa_node}/cpulist"
                ).read_text()) & allowed
        except (OSError, ValueError):
            local = set()

        # This is not the paper's hard-coded binding table.  Derive a stable
        # topology from the runner's actual affinity, prefer physical cores
        # local to the selected NVMe controller, then remote physical cores,
        # and only then use SMT siblings.  This avoids placing a client poller
        # on the sibling of a busy KFS poller while idle cores still exist.
        local_layers = self.cpu_sibling_layers(local)
        remote_layers = self.cpu_sibling_layers(allowed - local)
        self._async_cpu_order = []
        for index in range(max(len(local_layers), len(remote_layers))):
            if index < len(local_layers):
                self._async_cpu_order.extend(local_layers[index])
            if index < len(remote_layers):
                self._async_cpu_order.extend(remote_layers[index])
        return self._async_cpu_order

    def client_worker_count(self, case: Case) -> int:
        configured = self.args.client_workers
        if configured < 0:
            raise RuntimeError("client worker count must be non-negative")
        if configured > 0:
            return configured
        # Eight client pollers saturated the measured transport while leaving
        # the NVMe-local physical cores available to the KFS workers.
        return max(1, min(8, case.threads))

    def client_runtime_cpus(self, case: Case) -> list[int]:
        count = self.client_worker_count(case)
        ordered = self.async_cpu_order()
        first = case.workers
        last = first + count
        if last > len(ordered):
            raise RuntimeError(
                f"KFS+client workers require {last} CPUs, only "
                f"{len(ordered)} are allowed")
        return ordered[first:last]

    def kfs_runtime_cpus(self, worker_count: int,
                         client_workers: int) -> list[int]:
        ordered = self.async_cpu_order()
        total = worker_count + client_workers
        if worker_count <= 0 or client_workers < 0 or total > len(ordered):
            raise RuntimeError(
                f"KFS+client workers require {total} CPUs, only "
                f"{len(ordered)} are allowed")
        return ordered[:worker_count]

    def client_lane_count(self, case: Case) -> int:
        configured = self.args.client_lanes
        if configured < 0:
            raise RuntimeError("client lane count must be non-negative")
        if configured > 0:
            return configured
        # Blocking POSIX callers hold a transport lane until completion.  Give
        # concurrent benchmark threads independent lanes, while retaining four
        # lanes for the single-thread direct path and bounding SHM consumption.
        return max(4, min(32, case.threads))

    def ipc_ring_capacity(self, case: Case) -> int:
        configured = getattr(self.args, "ipc_ring_capacity", 0)
        if configured < 0 or configured == 1:
            raise RuntimeError("IPC ring capacity must be zero or at least 2")
        if configured > 0:
            return configured
        lanes = self.client_lane_count(case)
        # Each lane owns an SQ and CQ data ring.  Keep high-lane runs near a
        # 2 GiB payload budget with 2 MiB slots instead of exhausting the
        # runner's 4 GiB hugepage pool.
        return max(16, min(64, 512 // lanes))

    @staticmethod
    def format_cpu_list(cpus: Iterable[int]) -> str:
        return ",".join(str(cpu) for cpu in cpus)

    def driver(self) -> str:
        driver = pathlib.Path(f"/sys/bus/pci/devices/{self.args.bdf}/driver")
        return driver.resolve().name if driver.exists() else "unbound"

    def logged_run(self, command: list[str], *, check: bool = True,
                   timeout: float | None = None,
                   stdout: pathlib.Path | None = None) -> subprocess.CompletedProcess[str]:
        stamp = dt.datetime.now(dt.timezone.utc).isoformat()
        self.command_log.write(f"{stamp}\t{shlex.join(command)}\n")
        self.command_log.flush()
        if stdout is None:
            return subprocess.run(command, text=True, check=check,
                                  timeout=timeout, capture_output=True)
        with stdout.open("w", encoding="utf-8") as stream:
            return subprocess.run(command, text=True, check=check,
                                  timeout=timeout, stdout=stream,
                                  stderr=subprocess.STDOUT)

    def sudo_env(self, environment: dict[str, str], command: list[str]) -> list[str]:
        assignments = [f"{key}={value}" for key, value in environment.items()]
        return ["sudo", "-n", "-E", "env", *assignments, *command]

    def switch_driver(self, target: str) -> None:
        if self.driver() == target:
            if target == "nvme":
                self.block_device = self.wait_for_namespace()
            return
        setup = pathlib.Path(self.args.spdk_root) / "scripts/setup.sh"
        environment = {"PCI_ALLOWED": self.args.bdf}
        if target == "nvme":
            command = self.sudo_env(environment, [str(setup), "reset"])
        elif target == "vfio-pci":
            environment["HUGEMEM"] = str(self.args.hugemem_mb)
            command = self.sudo_env(environment, [str(setup)])
        else:
            raise ValueError(target)
        result = self.logged_run(command, check=False, timeout=60)
        if result.returncode != 0:
            raise RuntimeError(result.stdout + result.stderr)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and self.driver() != target:
            time.sleep(0.1)
        if self.driver() != target:
            raise RuntimeError(f"BDF {self.args.bdf} did not bind to {target}")
        self.block_device = self.wait_for_namespace() if target == "nvme" else None

    def wait_for_namespace(self) -> pathlib.Path:
        deadline = time.monotonic() + 15
        last_error: RuntimeError | None = None
        while time.monotonic() < deadline:
            try:
                return self.identify_namespace()
            except RuntimeError as error:
                last_error = error
                time.sleep(0.2)
        assert last_error is not None
        raise last_error

    def identify_namespace(self) -> pathlib.Path:
        controllers: list[pathlib.Path] = []
        for controller in pathlib.Path("/sys/class/nvme").glob("nvme[0-9]*"):
            if (controller / "device").resolve().name == self.args.bdf:
                controllers.append(controller)
        if len(controllers) != 1:
            raise RuntimeError(f"expected one NVMe controller at {self.args.bdf}, got {controllers}")
        controller = controllers[0]
        model = (controller / "model").read_text().strip()
        serial = (controller / "serial").read_text().strip()
        if model != self.args.expected_model or serial != self.args.expected_serial:
            raise RuntimeError(
                f"target identity mismatch: model={model!r} serial={serial!r}")
        namespaces: list[pathlib.Path] = []
        for namespace in pathlib.Path("/sys/class/block").glob("nvme*n*"):
            if not re.fullmatch(r"nvme\d+n\d+", namespace.name):
                continue
            if not (namespace / "device" / controller.name).exists():
                continue
            nsid_path = namespace / "nsid"
            if nsid_path.exists() and int(nsid_path.read_text().strip()) == self.args.nsid:
                namespaces.append(namespace)
        if len(namespaces) != 1:
            raise RuntimeError(f"expected NSID {self.args.nsid}, got {namespaces}")
        block = pathlib.Path("/dev") / namespaces[0].name
        listing = subprocess.check_output(
            ["lsblk", "-nrpo", "NAME,TYPE,MOUNTPOINTS", str(block)], text=True
        ).splitlines()
        if len(listing) != 1 or listing[0].split()[1] != "disk":
            raise RuntimeError(f"target must have no partitions: {listing}")
        if len(listing[0].split()) > 2:
            raise RuntimeError(f"target is mounted: {listing[0]}")
        holders = list((namespaces[0] / "holders").iterdir())
        if holders:
            raise RuntimeError(f"target has device-mapper/md holders: {holders}")
        return block

    def validate(self) -> None:
        expected = f"BDF={self.args.bdf};NSID={self.args.nsid};DAX={self.args.dax}"
        if os.environ.get("ORCHFS_REPRO_DESTRUCTIVE") != expected:
            raise RuntimeError(
                "destructive token mismatch; export "
                f"ORCHFS_REPRO_DESTRUCTIVE={shlex.quote(expected)}")
        if not re.fullmatch(r"[0-9a-f]{4}:[0-9a-f]{2}:[0-9a-f]{2}\.[0-7]",
                            self.args.bdf):
            raise RuntimeError("BDF must be domain-qualified lower-case PCI syntax")
        dax = pathlib.Path(self.args.dax)
        if not dax.exists() or not stat.S_ISCHR(dax.stat().st_mode):
            raise RuntimeError(f"{dax} is not a character devdax device")
        subprocess.run(["sudo", "-n", "true"], check=True)
        if subprocess.run(["pgrep", "-x", "kfs_main"], capture_output=True).returncode == 0:
            raise RuntimeError("a kfs_main process is already running")
        self.switch_driver("nvme")
        assert self.block_device is not None

    def spdk_environment(self) -> dict[str, str]:
        return {
            "ORCHFS_SPDK_PCI_BDF": self.args.bdf,
            "ORCHFS_SPDK_NSID": str(self.args.nsid),
            "ORCHFS_SPDK_QUEUE_DEPTH": "32",
            "ORCHFS_SPDK_BOUNCE_BUFFERS": "32",
            "ORCHFS_SPDK_MAX_TRANSFER_SIZE": str(MIB),
            "ORCHFS_SPDK_HUGEPAGE_DIR": "/dev/hugepages",
            "ORCHFS_SPDK_SHM_ID": "-1",
            "ORCHFS_SPDK_WRITE_DURABILITY": getattr(
                self.args, "async_durability", "auto"),
        }

    def format(self, version: str, run_dir: pathlib.Path,
               *, trace_build: bool = False) -> None:
        if version == "sync-fair":
            self.switch_driver("nvme")
            command = [str(self.args.sync_build / "mkfs")]
        else:
            self.switch_driver("vfio-pci")
            build = self.args.async_trace_build if trace_build else self.args.async_build
            formatter_cpu = self.kfs_runtime_cpus(1, 0)[0]
            env = self.spdk_environment() | {
                "ORCHFS_SPDK_CPU_LIST": str(formatter_cpu),
                "ORCHFS_SPDK_POLLER_COUNT": "1",
                "ORCHFS_SPDK_REACTOR_MASK": f"0x{1 << formatter_cpu:x}",
            }
            command = self.sudo_env(env, [str(build / "mkfs")])
        if version == "sync-fair":
            command = ["sudo", "-n", *command]
        self.logged_run(command, timeout=180, stdout=run_dir / "mkfs.log")

    def start_server(self, version: str, case: Case, run_dir: pathlib.Path,
                     *, traced: bool = False,
                     log_label: str | None = None,
                     extra_environment: dict[str, str] | None = None
                     ) -> tuple[subprocess.Popen[str], str | None]:
        if log_label is not None and not re.fullmatch(r"[a-z0-9-]+", log_label):
            raise ValueError(f"invalid server log label: {log_label!r}")
        stem = "server-trace" if traced else "server"
        log_path = run_dir / f"{stem}{'-' + log_label if log_label else ''}.log"
        stream = log_path.open("w", encoding="utf-8")
        if version == "sync-fair":
            server = [str(self.args.sync_build / "kfs_main")]
            command = (self.sudo_env(extra_environment, server)
                       if extra_environment else ["sudo", "-n", *server])
            endpoint = None
        else:
            build = self.args.async_trace_build if traced else self.args.async_build
            endpoint_tag = hashlib.sha256(
                f"{run_dir}:{int(traced)}".encode()).hexdigest()[:16]
            endpoint = f"/tmp/orchfs-r-{endpoint_tag}.sock"
            env = self.spdk_environment() | {
                "ORCHFS_KFS_WORKERS": str(case.workers),
                "ORCHFS_KFS_CPU_LIST": self.format_cpu_list(
                    self.kfs_runtime_cpus(
                        case.workers, self.client_worker_count(case))),
                "ORCHFS_IPC_HUGEPAGES": "1",
                "ORCHFS_IPC_RING_CAPACITY": str(
                    self.ipc_ring_capacity(case)),
                "ORCHFS_IPC_DATA_SLOT_SIZE": str(IPC_DATA_SLOT_SIZE),
                "ORCHFS_ASYNC_ENDPOINT": endpoint,
            }
            if traced:
                env |= {
                    "ORCHFS_REPRO_TRACE_FILE": str(run_dir / "server-trace.csv"),
                    "ORCHFS_REPRO_SPACE_FILE": str(run_dir / "space.csv"),
                    "ORCHFS_REPRO_TRACE_SAMPLE_EVERY": str(self.args.trace_sample_every),
                }
            if extra_environment:
                env |= extra_environment
            command = self.sudo_env(env, [str(build / "kfs_main")])
        self.command_log.write(
            f"{dt.datetime.now(dt.timezone.utc).isoformat()}\t{shlex.join(command)} &\n")
        self.command_log.flush()
        process = subprocess.Popen(command, text=True, stdout=stream,
                                   stderr=subprocess.STDOUT)
        process._orchfs_log_stream = stream  # type: ignore[attr-defined]
        if version == "sync-fair":
            time.sleep(1.0)
        else:
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    break
                if pathlib.Path(endpoint).is_socket():
                    return process, endpoint
                time.sleep(0.1)
        if process.poll() is not None:
            stream.flush()
            stream.close()
            raise RuntimeError(f"KFS exited early; see {log_path}")
        if version != "sync-fair" and not pathlib.Path(str(endpoint)).is_socket():
            subprocess.run(["sudo", "-n", "kill", "-TERM", str(process.pid)],
                           check=False, capture_output=True)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                subprocess.run(["sudo", "-n", "kill", "-KILL",
                                str(process.pid)], check=False,
                               capture_output=True)
                process.wait(timeout=10)
            stream.close()
            raise RuntimeError(f"KFS did not create endpoint {endpoint}; see {log_path}")
        return process, endpoint

    def stop_server(self, version: str, process: subprocess.Popen[str],
                    *, snapshot: bool = False) -> None:
        try:
            if version == "sync-fair":
                self.logged_run(["sudo", "-n", str(self.args.sync_build / "close_kfs")],
                                check=False, timeout=30)
            else:
                if snapshot:
                    self.logged_run(["sudo", "-n", "kill", "-USR1",
                                     str(process.pid)], check=False, timeout=10)
                    time.sleep(0.2)
                self.logged_run(["sudo", "-n", "kill", "-TERM",
                                 str(process.pid)], check=False, timeout=10)
            try:
                process.wait(timeout=60)
            except subprocess.TimeoutExpired:
                self.logged_run(["sudo", "-n", "kill", "-KILL",
                                 str(process.pid)], check=False, timeout=10)
                process.wait(timeout=10)
        finally:
            stream = getattr(process, "_orchfs_log_stream", None)
            if stream is not None:
                stream.close()

    def client_environment(self, version: str, endpoint: str | None,
                           run_dir: pathlib.Path, case: Case, *,
                           traced: bool = False) -> dict[str, str]:
        if version == "sync-fair":
            return {"LD_PRELOAD": str(self.args.sync_build / "libOrchFS.so")}
        build = self.args.async_trace_build if traced else self.args.async_build
        environment = {
            "LD_PRELOAD": str(build / "libOrchFS.so"),
            "ORCHFS_ASYNC_ENDPOINT": str(endpoint),
            "ORCHFS_CLIENT_WORKERS": str(self.client_worker_count(case)),
            "ORCHFS_CLIENT_CPU_LIST": self.format_cpu_list(
                self.client_runtime_cpus(case)),
            "ORCHFS_CLIENT_LANES": str(self.client_lane_count(case)),
            "ORCHFS_IPC_RING_CAPACITY": str(self.ipc_ring_capacity(case)),
            "ORCHFS_IPC_DATA_SLOT_SIZE": str(IPC_DATA_SLOT_SIZE),
        }
        if traced:
            environment |= {
                "ORCHFS_REPRO_TRACE_FILE": str(run_dir / "client-trace.csv"),
                "ORCHFS_REPRO_TRACE_SAMPLE_EVERY": str(self.args.trace_sample_every),
            }
        return environment

    @staticmethod
    def case_path(case: Case, repeat: int) -> str:
        safe = re.sub(r"[^A-Za-z0-9_.-]", "-", case.name)
        return f"orchfs-repro-f{case.figure}-{safe}-r{repeat}"

    def bench_arguments(self, case: Case, repeat: int, run_dir: pathlib.Path,
                        *, phase: str) -> list[str]:
        if phase not in {"all", "prepare", "measure"}:
            raise ValueError(phase)
        arguments = [
            "--path-prefix", self.case_path(case, repeat),
            "--operation", case.operation,
            "--access", case.access,
            "--size-mode", case.size_mode,
            "--size", str(case.size),
            "--min-size", str(case.min_size),
            "--max-size", str(case.max_size),
            "--threads", str(case.threads),
            "--files", str(case.files),
            "--fsync", case.fsync,
            "--offset-alignment", str(case.offset_alignment),
            "--seed", str(SEED),
            "--label", f"fig{case.figure}-{case.name}",
        ]
        if case.unaligned_to:
            arguments += ["--unaligned-to", str(case.unaligned_to)]
        if phase in {"all", "prepare"}:
            arguments += ["--prepare-bytes", str(case.prepare_bytes)]
        else:
            arguments += ["--existing-bytes", str(case.prepare_bytes)]
        if phase == "prepare":
            arguments += ["--bytes-per-thread", "0"]
        elif case.duration_sec:
            arguments += ["--duration-sec", str(case.duration_sec)]
        else:
            arguments += ["--bytes-per-thread", str(case.bytes_per_thread)]
        if phase != "prepare" and case.series_ms:
            series = run_dir / "series.csv"
            arguments += ["--series-ms", str(case.series_ms),
                          "--series-file", str(series)]
        if phase != "prepare":
            arguments.append("--cleanup")
        return arguments

    def run_benchmark(self, version: str, case: Case, repeat: int,
                      run_dir: pathlib.Path, endpoint: str | None,
                      *, phase: str, traced: bool = False,
                      ebpf: bool = False) -> tuple[int, pathlib.Path]:
        build = self.args.async_trace_build if traced else self.args.async_build
        benchmark = build / "orchfs_repro_bench"
        arguments = self.bench_arguments(case, repeat, run_dir, phase=phase)
        environment = self.client_environment(
            version, endpoint, run_dir, case, traced=traced)
        command = [str(benchmark), *arguments]
        log_path = run_dir / ("profile.log" if ebpf else
                              "prepare.log" if phase == "prepare" else
                              "bench.log")
        if ebpf:
            if self.block_device is None:
                raise RuntimeError("sync block device was not identified")
            device = self.block_device.stat().st_rdev
            encoded = (os.major(device) << 20) | os.minor(device)
            child = ["env", *[f"{key}={value}" for key, value in environment.items()],
                     *command]
            command = ["sudo", "-n", "-E", "bpftrace", "-c",
                       shlex.join(child), str(ROOT / "scripts/reproduction/sync_profile.bt"),
                       str(encoded)]
        else:
            command = self.sudo_env(environment, command)
        result = self.logged_run(command, check=False,
                                 timeout=self.args.case_timeout,
                                 stdout=log_path)
        return result.returncode, log_path

    @staticmethod
    def parse_log(path: pathlib.Path) -> dict[str, str]:
        text = path.read_text(encoding="utf-8", errors="replace")
        result: dict[str, str] = {}
        matches = re.findall(r"^orchfs_repro_result\s+(.+)$", text, re.MULTILINE)
        if matches:
            for token in shlex.split(matches[-1]):
                if "=" in token:
                    key, value = token.split("=", 1)
                    result[key] = value
        errors = re.findall(r"^orchfs_repro_error\s+(.+)$", text, re.MULTILINE)
        if errors:
            for token in shlex.split(errors[-1]):
                if "=" in token:
                    key, value = token.split("=", 1)
                    result[f"error_{key}"] = value
        return result

    def run_case(self, version: str, case: Case, repeat: int) -> None:
        run_dir = self.raw / version / f"fig{case.figure}" / case.name / f"run-{repeat}"
        run_dir.mkdir(parents=True, exist_ok=False)
        exit_code = 1
        log_path = run_dir / "bench.log"
        process: subprocess.Popen[str] | None = None
        try:
            self.format(version, run_dir)
            if case.profile or case.separate_prepare:
                process, endpoint = self.start_server(version, case, run_dir)
                prep_code, prep_log = self.run_benchmark(
                    version, case, repeat, run_dir, endpoint, phase="prepare")
                self.stop_server(version, process)
                process = None
                if prep_code != 0:
                    exit_code, log_path = prep_code, prep_log
                else:
                    process, endpoint = self.start_server(
                        version, case, run_dir,
                        traced=(version == "async-current" and case.trace))
                    exit_code, log_path = self.run_benchmark(
                        version, case, repeat, run_dir, endpoint,
                        phase="measure",
                        traced=(version == "async-current" and case.trace),
                        ebpf=(version == "sync-fair" and case.profile))
                    self.stop_server(version, process,
                                     snapshot=(version == "async-current" and case.trace))
                    process = None
            else:
                process, endpoint = self.start_server(version, case, run_dir)
                exit_code, log_path = self.run_benchmark(
                    version, case, repeat, run_dir, endpoint, phase="all")
                self.stop_server(version, process)
                process = None
        except (OSError, RuntimeError, subprocess.TimeoutExpired) as error:
            (run_dir / "runner-error.log").write_text(f"{type(error).__name__}: {error}\n")
            exit_code = 124 if isinstance(error, subprocess.TimeoutExpired) else 125
        finally:
            if process is not None:
                self.stop_server(version, process)

        parsed = self.parse_log(log_path) if log_path.exists() else {}
        row: dict[str, object] = {field: "" for field in RESULT_FIELDS}
        row |= {
            "figure": case.figure,
            "case": case.name,
            "version": version,
            "scale": self.args.scale,
            "repeat": repeat,
            "status": "ok" if exit_code == 0 and "seconds" in parsed else "failed",
            "exit_code": exit_code,
            "operation": parsed.get("operation", case.operation),
            "access": parsed.get("access", case.access),
            "size_mode": parsed.get("size_mode", case.size_mode),
            "fixed_size": parsed.get("fixed_size", case.size),
            "min_size": parsed.get("min_size", case.min_size),
            "max_size": parsed.get("max_size", case.max_size),
            "threads": case.threads,
            "files": case.files,
            "kfs_workers": case.workers,
            "client_workers": (self.client_worker_count(case)
                               if version == "async-current" else ""),
            "kfs_cpu_list": (self.format_cpu_list(
                self.kfs_runtime_cpus(
                    case.workers, self.client_worker_count(case)))
                if version == "async-current" else ""),
            "client_cpu_list": (self.format_cpu_list(
                self.client_runtime_cpus(case))
                if version == "async-current" else ""),
            "client_lanes": (self.client_lane_count(case)
                             if version == "async-current" else ""),
            "ipc_ring_capacity": (self.ipc_ring_capacity(case)
                                  if version == "async-current" else ""),
            "fsync": case.fsync,
            "offset_alignment": case.offset_alignment,
            "unaligned_to": case.unaligned_to,
            "prepare_bytes": case.prepare_bytes,
            "bytes_per_thread": case.bytes_per_thread,
            "duration_sec": case.duration_sec,
            "error_stage": parsed.get("error_stage", ""),
            "error_number": parsed.get("error_errno", ""),
            "profile": int(case.profile),
            "trace": int(version == "async-current" and case.trace),
            "series_file": str(run_dir / "series.csv") if case.series_ms else "",
            "raw_dir": str(run_dir),
            "commit": self.commits[version],
        }
        for key in ("seconds", "MiB_per_s", "IOPS", "avg_latency_us",
                    "p50_us", "p99_us"):
            row[key] = parsed.get(key, "")
        self.rows.append(row)
        self.write_results()

    def write_results(self) -> None:
        target = self.csv_dir / "results.csv"
        temporary = target.with_suffix(".csv.tmp")
        with temporary.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=RESULT_FIELDS)
            writer.writeheader()
            writer.writerows(self.rows)
        temporary.replace(target)

    def restore_driver(self) -> None:
        if self.original_driver in {"nvme", "vfio-pci"}:
            self.switch_driver(self.original_driver)


def worktree_fingerprint(path: pathlib.Path) -> str:
    """Hash tracked changes plus source-like untracked files, not build output."""
    digest = hashlib.sha256()
    digest.update(subprocess.check_output(
        ["git", "-C", str(path), "diff", "--binary", "HEAD", "--",
         "CMakeLists.txt", "Async", "KernelFS", "LibFS", "config",
         "filebench", "include", "scripts", "test", "util"]))
    untracked = subprocess.check_output(
        ["git", "-C", str(path), "ls-files", "--others",
         "--exclude-standard", "-z", "--", "CMakeLists.txt", "Async",
         "KernelFS", "LibFS", "config", "filebench", "include", "scripts",
         "test", "util"])
    for encoded in sorted(item for item in untracked.split(b"\0") if item):
        relative = pathlib.Path(encoded.decode("utf-8", errors="surrogateescape"))
        digest.update(encoded)
        digest.update(b"\0")
        digest.update((path / relative).read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def append_manifest(output: pathlib.Path,
                    values: dict[str, object]) -> None:
    with (output / "manifest.tsv").open("a", encoding="utf-8") as stream:
        for key, value in values.items():
            stream.write(f"{key}\t{value}\n")


def write_manifest(args: argparse.Namespace, output: pathlib.Path,
                   cases: Iterable[Case]) -> None:
    filebench = subprocess.run(["filebench", "-h"], text=True,
                               capture_output=True)
    filebench_text = (filebench.stdout + filebench.stderr).splitlines()
    commands = {
        "created_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "scale": args.scale,
        "seed": str(SEED),
        "kernel": subprocess.check_output(["uname", "-r"], text=True).strip(),
        "cpu": subprocess.check_output(
            ["lscpu", "-J"], text=True).strip().replace("\n", " "),
        "async_commit": subprocess.check_output(
            ["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True).strip(),
        "async_worktree_sha256": worktree_fingerprint(ROOT),
        "sync_commit": subprocess.check_output(
            ["git", "-C", str(args.sync_worktree), "rev-parse", "HEAD"], text=True).strip(),
        "sync_fair_worktree_sha256": worktree_fingerprint(args.sync_worktree),
        "bdf": args.bdf,
        "nsid": str(args.nsid),
        "dax": args.dax,
        "expected_model": args.expected_model,
        "expected_serial": args.expected_serial,
        "async_write_durability": args.async_durability,
        "initial_driver": (pathlib.Path(
            f"/sys/bus/pci/devices/{args.bdf}/driver").resolve().name
            if pathlib.Path(f"/sys/bus/pci/devices/{args.bdf}/driver").exists()
            else "unbound"),
        "numa_node": (pathlib.Path(
            f"/sys/bus/pci/devices/{args.bdf}/numa_node").read_text().strip()
            if pathlib.Path(f"/sys/bus/pci/devices/{args.bdf}/numa_node").exists()
            else "unknown"),
        "spdk_commit": subprocess.run(
            ["git", "-C", str(args.spdk_root), "rev-parse", "HEAD"],
            text=True, capture_output=True).stdout.strip() or "not-a-git-checkout",
        "bpftrace": subprocess.run(
            ["bpftrace", "--version"], text=True,
            capture_output=True).stdout.strip() or "unavailable",
        "interleave": "per-case, direction reversed by case and repeat",
        "prefill_boundary": "restart KFS and LibFS before measurement",
        "async_cpu_policy": (
            "derive from runner affinity; reserve KFS workers first; "
            "client workers use a disjoint list; NVMe-local physical cores first"),
        "runner_affinity": ",".join(
            str(cpu) for cpu in sorted(os.sched_getaffinity(0))),
        "argv": shlex.join(sys.argv),
        "fio": subprocess.run(["fio", "--version"], text=True,
                              capture_output=True).stdout.strip(),
        "filebench": filebench_text[0] if filebench_text else "unavailable",
        "leveldb": "1.23 (local package/source; YCSB status recorded separately)",
        "case_count": str(len(list(cases))),
    }
    with (output / "manifest.tsv").open("w", encoding="utf-8") as stream:
        for key, value in commands.items():
            stream.write(f"{key}\t{value}\n")
    statuses = [
        ("06-09", "design figures", "not-applicable", "no runtime experiment"),
        ("16", "Filebench", "not-run", "run_filebench.py is a separate destructive phase"),
        ("17", "YCSB+LevelDB", "not-run",
         "run_leveldb_ycsb.py is a separate embedded-driver phase; Java YCSB is absent"),
        ("18", "GridGraph", "not-run",
         "run_gridgraph.py is a separate compatibility-patched dataset phase"),
        ("19", "fragmentation", "not-run", "run_fragmentation.py is a separate high-write phase"),
    ]
    with (output / "experiment_status.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(["figure", "experiment", "status", "reason"])
        writer.writerows(statuses)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scale", choices=("smoke", "paper"), default="smoke")
    parser.add_argument("--figures", default="01,02,03,04,05,10,11,12,13,14,15")
    parser.add_argument("--versions", default="sync-fair,async-current")
    parser.add_argument("--repeats", type=int)
    parser.add_argument("--case-regex", default="")
    parser.add_argument("--list-cases", action="store_true")
    parser.add_argument("--output", type=pathlib.Path)
    parser.add_argument("--bdf", default="0000:b2:00.0")
    parser.add_argument("--nsid", type=int, default=1)
    parser.add_argument("--dax", default="/dev/dax0.1")
    parser.add_argument("--expected-model", default="SAMSUNG MZPLJ3T2HBJR-00007")
    parser.add_argument("--expected-serial", default="S55HNC0W100205")
    parser.add_argument("--spdk-root", default="/opt/orchfs/spdk")
    parser.add_argument("--hugemem-mb", type=int, default=4096)
    parser.add_argument("--sync-worktree", type=pathlib.Path,
                        default=pathlib.Path("/tmp/orchfs-sync-baseline"))
    parser.add_argument("--sync-build", type=pathlib.Path,
                        default=pathlib.Path("/tmp/orchfs-sync-baseline/build-fair"))
    parser.add_argument("--async-build", type=pathlib.Path,
                        default=ROOT / "build-repro")
    parser.add_argument("--async-trace-build", type=pathlib.Path,
                        default=ROOT / "build-repro-trace")
    parser.add_argument(
        "--client-workers", type=int, default=0,
        help="0 chooses max(1, min(8, benchmark threads))")
    parser.add_argument(
        "--client-lanes", type=int, default=0,
        help="0 chooses max(4, min(32, benchmark threads))")
    parser.add_argument(
        "--ipc-ring-capacity", type=int, default=0,
        help="0 bounds total high-lane payload rings near 2 GiB")
    parser.add_argument("--async-durability",
                        choices=("auto", "completion", "fua", "flush"),
                        default="auto")
    parser.add_argument("--trace-sample-every", type=int, default=1)
    parser.add_argument("--case-timeout", type=float, default=900)
    parser.add_argument("--prefill-bytes", type=int)
    parser.add_argument("--measured-bytes", type=int)
    parser.add_argument(
        "--allow-failures", action="store_true",
        help="write partial reports but return success when samples fail")
    args = parser.parse_args()
    args.bdf = args.bdf.lower()
    if args.repeats is None:
        args.repeats = 1 if args.scale == "smoke" else 3
    if args.repeats <= 0:
        parser.error("--repeats must be positive")
    if args.client_workers < 0:
        parser.error("--client-workers must be non-negative")
    if args.client_lanes < 0:
        parser.error("--client-lanes must be non-negative")
    if args.ipc_ring_capacity != 0 and args.ipc_ring_capacity < 2:
        parser.error("--ipc-ring-capacity must be zero or at least 2")
    if args.prefill_bytes is not None and args.prefill_bytes <= 0:
        parser.error("--prefill-bytes must be positive")
    if args.measured_bytes is not None and args.measured_bytes <= 0:
        parser.error("--measured-bytes must be positive")
    return args


def main() -> int:
    args = parse_args()
    figures = {item.strip().removeprefix("fig") for item in args.figures.split(",")}
    cases = size_cases(args.scale, figures,
                       prefill_override=args.prefill_bytes,
                       measured_override=args.measured_bytes)
    if args.case_regex:
        expression = re.compile(args.case_regex)
        cases = [case for case in cases
                 if expression.search(f"fig{case.figure}/{case.name}")]
    if args.list_cases:
        for case in cases:
            print(f"fig{case.figure}/{case.name}")
        return 0
    versions = [item.strip() for item in args.versions.split(",") if item.strip()]
    if not versions or any(item not in {"sync-fair", "async-current"}
                           for item in versions):
        raise SystemExit("--versions accepts sync-fair,async-current")
    timestamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    output = (args.output or
              ROOT / "benchmark-results" / f"paper-reproduction-{timestamp}").resolve()
    output.mkdir(parents=True, exist_ok=False)
    write_manifest(args, output, cases)
    runner = Runner(args, output)
    completed = False
    try:
        runner.validate()
        for repeat in range(1, args.repeats + 1):
            for case_index, case in enumerate(cases):
                forward = (repeat + case_index) % 2 == 1
                ordered = versions if forward else list(reversed(versions))
                for version in ordered:
                    runner.run_case(version, case, repeat)
        completed = True
    finally:
        try:
            runner.restore_driver()
        finally:
            runner.close()
            append_manifest(output, {
                "finished_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
                "completed": int(completed),
                "sample_count": len(runner.rows),
                "successful_samples": sum(
                    row["status"] == "ok" for row in runner.rows),
                "final_driver": runner.driver(),
            })
    print(output)
    expected = len(cases) * len(versions) * args.repeats
    complete = (len(runner.rows) == expected and all(
        row["status"] == "ok" for row in runner.rows))
    return 0 if complete or args.allow_failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
