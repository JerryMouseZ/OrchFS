# C++20 coroutine and SPDK build/run guide

OrchFS now separates the public asynchronous API from the authoritative KFS
execution process:

- each LibFS process owns one `orchfs::async::Runtime` and submits filesystem
  requests through `Client`, `File`, and `Directory`;
- KFS owns a separate runtime and all authoritative filesystem state;
- coroutine handles and pointers never cross the process boundary; the IPC
  transport carries fixed wire descriptors and copied request metadata;
- only KFS opens the NVMe controller through SPDK. LibFS has no SPDK qpair;
- the existing POSIX/`LD_PRELOAD` surface is a blocking compatibility adapter
  over `Runtime::submit()` plus `JoinHandle::join()`.

The supported SPDK baseline is **v26.01**. Keep SPDK in a fixed external prefix
instead of copying its headers or libraries into this repository. The default
prefix is `/opt/orchfs/spdk`; override it only with an absolute `SPDK_ROOT` at
configure time. See the [official SPDK downloads](https://spdk.io/downloads/)
for release sources.

## Build without SPDK

The default build needs no SPDK installation. It builds the coroutine runtime,
IPC, client/server libraries, the POSIX/LD_PRELOAD adapter, a non-SPDK NVMe
bridge, and hardware-independent tests. It intentionally does not build KFS:

```bash
cmake -S . -B build-async \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DBUILD_TESTING=ON
cmake --build build-async -j"$(nproc)"
ctest --test-dir build-async --output-on-failure
```

`spdk_nvme_logic_test` is safe in this configuration. It validates request
splitting, alignment, read-modify-write planning, and the `ENOTSUP` fallback;
it does not open a controller.

The main CMake targets are:

| Target | Role |
| --- | --- |
| `orchfs_async_runtime` | C++20 `Task`, `JoinHandle`, scheduling, and range arbitration |
| `orchfs_async_ipc` | Unix control socket and shared-memory SQ/CQ transport |
| `orchfs_async_client` | LibFS-side asynchronous proxy API |
| `orchfs_async_server` | KFS-side authoritative request dispatcher |
| `orchfs_async_device` | Runtime-resuming asynchronous block-device phases |
| `orchfs_kfs_coroutine_core` | Runtime-native filesystem implementation behind `AsyncFilesystem` |
| `orchfs_spdk_backend` | SPDK NVMe backend, or an `ENOTSUP` stub when disabled |
| `OrchFS` | POSIX/LD_PRELOAD compatibility adapter backed by the async client |

The old LibFS/KFS service targets, NOASYNC mode, and `close_kfs` helper have
been removed. `kfs_main` and the standalone `mkfs` formatter are created only
when `ORCHFS_BUILD_KFS=ON`; that option force-enables SPDK.

## Build with SPDK v26.01

Build SPDK v26.01 and its DPDK submodule in the fixed prefix. CMake requires the
SPDK-generated metadata below that prefix, normally under
`build/lib/pkgconfig`; it deliberately does not select an unrelated system
`spdk_*.pc` file.

```bash
cmake -S . -B build-spdk \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DBUILD_TESTING=ON \
  -DORCHFS_BUILD_KFS=ON \
  -DSPDK_ROOT=/opt/orchfs/spdk
cmake --build build-spdk -j"$(nproc)"
ctest --test-dir build-spdk --output-on-failure
```

Configuration fails early unless `${SPDK_ROOT}/VERSION` identifies the v26.01
LTS line and the tree contains both `spdk_nvme.pc` and `spdk_env_dpdk.pc`.
This is intentional: silently falling back to a stale system package can
compile against one tree and link against another.

The CTest suite still performs no media I/O in an SPDK-enabled build. Device
tests must be launched explicitly after the checks in the next section.

## Comparable four-layer performance benchmark

Performance work uses one fixed ladder so an optimization can be attributed to
the layer it actually changes:

1. the official `spdk_nvme_perf` raw path;
2. `AsyncBlockDevice` called directly on KFS Runtime workers;
3. `KfsCoroutineCore` called directly, with no shared-memory RPC;
4. `Client` / shared-memory RPC / KFS end to end.

All four use 64 KiB requests, total QD 64, the same four KFS CPUs, 64
coroutines where the layer has coroutines, 16,384 timed operations, and five
runs. The direct and E2E write samples overwrite fully allocated extents and
include the final sync. The runner records every sample plus the five-run
median for throughput, IOPS, and p99:

```bash
sudo -E env \
  ORCHFS_LAYERED_BENCHMARK_DESTRUCTIVE=ERASE_AND_REFORMAT \
  ORCHFS_SPDK_PCI_BDF='0000:BB:DD.F' \
  ORCHFS_SPDK_NSID='1' \
  ORCHFS_ASYNC_BENCHMARK_SERVER_CPUS='1,3,5,7' \
  ORCHFS_ASYNC_BENCHMARK_CLIENT_CPUS='9,11,13,15' \
  scripts/async/run_layered_benchmark.sh
```

This runner is intentionally destructive. It refuses to run unless the exact
opt-in token is present, the controller is already bound to `vfio-pci` or
`uio_pci_generic`, hugepages exist, and all benchmark binaries are available.
Raw SPDK writes invalidate the selected namespace; the runner then executes
`mkfs` before measuring either KFS layer. It does not bind or reset a PCI
device. The direct block-device range defaults to 2 TiB and is rejected unless
it is 64 KiB aligned, lies entirely above OrchFS's 1 TiB on-media format, and
fits in the selected namespace. Override it only with an explicitly reserved
range through `ORCHFS_LAYERED_BENCHMARK_DEVICE_OFFSET`.

The official raw tool reports completed writes but does not issue the
filesystem-level sync used by the three OrchFS write samples. Its raw write
phase is therefore labeled `write`, while the other layers are labeled
`write+sync`; this keeps the peak-device denominator explicit rather than
silently presenting unlike durability semantics as identical.

`spdk_nvme_device_test` is deliberately not a CTest. It refuses to read the
namespace unless all of the following are supplied: the destructive opt-in,
an explicit scratch offset, an external backup path, and an exact range token
which includes BDF, NSID, namespace geometry, write range, and aligned save
range. First run it
without the confirmation token to print the exact token without reading or
modifying namespace data, then repeat with that exact value:

```bash
export ORCHFS_RUN_DESTRUCTIVE_SPDK_TEST=1
export ORCHFS_SPDK_TEST_OFFSET='<reserved-unaligned-byte-offset>'
export ORCHFS_SPDK_TEST_BACKUP_FILE='/independent-storage/orchfs-spdk.backup'
./build-spdk/spdk_nvme_device_test       # prints the required token and exits
export ORCHFS_SPDK_TEST_CONFIRM_RANGE='<exact-token-printed-above>'
./build-spdk/spdk_nvme_device_test
```

The backup file is created with `O_EXCL`, contains a recovery header followed
by the original aligned bytes, and is fsynced before the first namespace write.
It is retained after success. The path must not reside on the tested namespace.
The scratch range must be reserved and offline; the end of a whole-disk
namespace commonly contains backup GPT metadata and is not a safe default.
Normal failures and SIGHUP/SIGINT/SIGQUIT/SIGPIPE/SIGTERM trigger a best-effort
restore and NVMe flush after the current command completes. SIGKILL, a process
crash, a controller hang, or power loss cannot run that in-process guard; in
those cases the retained external backup is the recovery source.

## Device preparation (destructive boundary)

SPDK takes exclusive userspace ownership of a PCIe controller. Binding a
controller to `vfio-pci` removes its kernel `/dev/nvme*` devices; formatting or
running OrchFS can destroy every namespace on that controller. Before binding:

1. Back up all required data.
2. Confirm the exact PCI BDF and namespace ID with `nvme list`, `nvme id-ctrl`,
   `nvme id-ns`, and sysfs. Never infer them from the model name.
3. Confirm that the namespace and every partition, dm-crypt, LVM, mdraid, and
   filesystem layered on it are stopped and unmounted (`findmnt`, `lsblk`).
4. Confirm that no other process owns the controller.

Create a host-local configuration from the non-operative template:

```bash
cp config/config-template.sh /secure/host/orchfs-async.conf
$EDITOR /secure/host/orchfs-async.conf
source /secure/host/orchfs-async.conf
```

Replace `0000:BB:DD.F` with the verified domain-qualified BDF. `NSID=1` is only
an example and must also be verified. Then reserve hugepages and bind only that
controller with SPDK's setup script:

```bash
sudo env \
  HUGEMEM="${ORCHFS_SPDK_HUGEMEM_MB}" \
  PCI_ALLOWED="${ORCHFS_SPDK_PCI_BDF}" \
  /opt/orchfs/spdk/scripts/setup.sh
```

On NUMA hosts, also select the node appropriate for the controller according
to SPDK's setup documentation. Verify hugepages and the driver before starting
KFS:

```bash
grep -E 'HugePages_(Total|Free)|Hugepagesize' /proc/meminfo
readlink "/sys/bus/pci/devices/${ORCHFS_SPDK_PCI_BDF}/driver"
```

Start KFS with the edited environment preserved. Stop it with `SIGINT` or
`SIGTERM`; shutdown stops admission, drains client/core/journal/migration work,
stops SPDK while the Runtime is alive, and stops the Runtime last:

```bash
sudo -E ./build-spdk/kfs_main
# In another terminal:
sudo kill -TERM "$(pidof kfs_main)"
```

Stop KFS and let it drain before resetting the device. Do not reset a
controller while any qpair or coroutine request remains active:

```bash
sudo /opt/orchfs/spdk/scripts/setup.sh reset
```

## Runtime and buffer contract

The coroutine runtime is cooperative and intentionally has **no watchdog**.
A runtime worker may do bounded CPU work, submit nonblocking IPC/SPDK work, and
suspend. It must not call a blocking syscall, sleep, wait on a condition
variable, perform synchronous device I/O, or hold an OS mutex across a
suspension. Violating this rule can stall an inode owner, delay unrelated work,
and prevent shutdown from draining.

The blocking POSIX adapter may wait only from the caller's external thread; it
must not call `JoinHandle::join()` from a runtime worker.

After `fork()`, a child lazily creates a fresh process-local Runtime and IPC
session. OrchFS descriptors, directory streams, and cookie-backed `FILE*`
objects inherited from the parent are intentionally invalid in the child and
return `EBADF`; reopen them there. Host descriptors retain normal libc fork
semantics. Relative read-only and single-path compatibility operations use
OrchFS first and may fall back to the host namespace when no session has ever
connected, or when a live OrchFS lookup returns `ENOENT`. Two-path `rename` is
stricter: after a live OrchFS session selects it, every error is fail-closed
because `ENOENT` cannot distinguish a missing source from a missing destination
parent. It falls back to libc only when the adapter has never connected.
Transport/storage errors never silently redirect any request to a same-named
host file.

A virtual descriptor opened on a directory is a valid relative `openat`
anchor even if the caller omitted `O_DIRECTORY`. The adapter derives that
directory capability lazily on the first relative `openat`, from the already
opened file handle rather than by resolving its pathname again; regular-file
descriptors are negatively cached and return `ENOTDIR`.

Paths are copied into coroutine/RPC state. Read and write spans are borrowed:
their storage must remain valid and must not be accessed concurrently by the
caller until the operation completes. `File::close()` and `Directory::close()`
report close errors; destructors schedule best-effort background close, and a
clean runtime shutdown drains those operations.

`RangePermit` may move through structured nested `Task` calls, but every permit
must be explicitly released before its submitted root completes. Letting a
permit escape as a `JoinHandle` result is a contract violation and terminates
the process; a root result has no coroutine frame that could safely retain the
worker pin or perform direct lock handoff. The awaiter returned by `release()`
must itself be consumed with `co_await`; discarding it also terminates instead
of silently leaving an active range and worker pin behind.

The shared-memory footprint grows approximately with
`2 * lanes * ring_capacity * data_slot_size`, plus descriptors. Keep client
worker counts and IPC sizing explicit. The protocol currently supports at most
120 lanes per client.

Each KFS Runtime worker owns one SPDK qpair. Submission, completion polling,
and qpair destruction stay on that worker; SPDK creates no OrchFS service
thread. The backend uses DMA bounce buffers
for unaligned reads and read-modify-write, splits transfers at the configured
maximum, retries transient queue pressure, and maps filesystem sync to a real
NVMe flush. A physical LBA-range reservation spans the complete logical write,
so overlapping RMW operations serialize while non-overlapping writes remain
concurrent. Flush captures a global write fence and waits for every logical
write accepted through that fence on every worker before issuing the namespace
flush.

SPDK completion callbacks execute on their qpair's owning Runtime worker. They may
submit more asynchronous work, but must not invoke blocking device entry points,
stop the device service, or recursively poll the same qpair; these cases return
`EDEADLK`.

The authoritative KFS path is `KfsCoroutineCore`: namespace operations are
owned by Runtime worker 0, inode data operations are owned by a stable worker,
and every SSD phase suspends through `AsyncBlockDevice`. Metadata and directory
lookups use the same coroutine data plans, so no executor, fd bridge, socket
service, or LibFS I/O pool exists in the production call graph. Per-inode range
arbiters are retained for the server lifetime so every later open of the same
inode observes the same serialization domain. Individual write callbacks
report their own NVMe completion errors; a later flush waits for the write
fence but does not replay an error that its caller already received.

## Configuration reference

| Variable | Meaning |
| --- | --- |
| `ORCHFS_ASYNC_ENDPOINT` | Unix-domain control socket shared by LibFS and KFS |
| `ORCHFS_CLIENT_WORKERS` | workers in each LibFS runtime (also bounds its IPC lanes) |
| `ORCHFS_KFS_WORKERS` | authoritative KFS coroutine workers |
| `ORCHFS_IPC_RING_CAPACITY` | descriptors per SQ/CQ lane |
| `ORCHFS_IPC_DATA_SLOT_SIZE` | maximum payload bytes per descriptor; larger I/O is chunked |
| `ORCHFS_SPDK_PCI_BDF` | required exact domain-qualified controller BDF; there is no device default |
| `ORCHFS_SPDK_NSID` | required namespace ID on that controller; there is no namespace default |
| `ORCHFS_SPDK_POLLER_COUNT` | standalone caller-polled formatter qpair count; production KFS uses `ORCHFS_KFS_WORKERS` |
| `ORCHFS_SPDK_QUEUE_DEPTH` | NVMe qpair depth; default 32, leaving headroom above the measured 16 requests per worker |
| `ORCHFS_SPDK_BOUNCE_BUFFERS` | DMA bounce buffers per poller; default 32 (registered shared slots bypass them) |
| `ORCHFS_SPDK_MAX_TRANSFER_SIZE` | maximum bytes in one NVMe command; default 1 MiB |
| `ORCHFS_SPDK_CPU_LIST` | standalone formatter qpair CPUs; production KFS uses its Runtime worker CPUs |
| `ORCHFS_SPDK_HUGEPAGE_DIR` | hugetlbfs mount used by SPDK/DPDK |
| `ORCHFS_SPDK_SHM_ID` | SPDK shared-memory ID; `-1` selects private mode |
| `ORCHFS_SPDK_REACTOR_MASK` | standalone formatter EAL mask; production KFS derives a one-control-lcore mask |
| `ORCHFS_SPDK_TEST_OFFSET` | explicit unaligned byte offset in a reserved scratch range; no default |
| `ORCHFS_SPDK_TEST_BACKUP_FILE` | new `O_EXCL` backup file on independent storage |
| `ORCHFS_SPDK_TEST_CONFIRM_RANGE` | exact BDF/NSID/write/save token printed by the first manual test run |

CPU affinity should be explicit in production. Pin the KFS process to CPUs local
to the NVMe controller; Runtime maps workers across that affinity set and each
worker directly polls its own qpair, without a second SPDK service-thread set.
