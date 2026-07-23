# Metadata trace and microbenchmark

This benchmark isolates the namespace costs that Filebench mixes with data I/O.
It is intentionally a measurement-only seam: it does not change namespace
locking, inode ownership, journal ordering, or persistence semantics.

## Trace fields

Configure a KFS build with `-DORCHFS_REPRO_TRACE=ON`. The server trace keeps the
existing CSV schema and adds five stages:

| Stage | `duration_ns` | `bytes` | `child_io_count` |
| --- | --- | --- | --- |
| `namespace_wait` | Time spent acquiring the global namespace gate | Active namespace permits immediately after acquisition | Highest active count observed by this KFS process |
| `dirent_scan` | One child-name lookup, including directory reads | Directory-entry slots examined | 1 |
| `snapshot_map_copy` | Copy of one immutable inode snapshot map | Source-map items copied | 1 |
| `range_map_copy` | Copy of one immutable inode range map | Source-map items copied | 1 |
| `journal_queue_wait` | Time from a non-empty commit awaiter becoming ready until worker 0 dequeues it | 0 | 1 |

The pre-existing `journal_commit` stage remains unchanged: it spans the
caller's complete commit wait. `journal_queue_wait` isolates only serialization
in front of the journal worker.

`scripts/reproduction/summarize_trace.py` writes the explicit aggregate fields
to `csv/metadata_trace.csv`:

- `namespace_wait_ns`, `namespace_active_mean`, `namespace_active_max`, and
  `namespace_peak`;
- `dirents_scanned` and `dirents_per_scan`;
- `snapshot_copy_items` and `range_copy_items`;
- `journal_wait_ns` for the caller's complete commit wait and
  `journal_queue_wait_ns` for its worker-0 queue portion.

Counts and total time are sampling-rate estimates. Means and maxima come from
the recorded samples. Use `ORCHFS_REPRO_TRACE_SAMPLE_EVERY=1` when the exact
short-run peak matters; always compare throughput with a trace-off build.

## Workloads

`orchfs_metadata_bench` creates one parent directory per benchmark thread. This
layout exposes whether a future parent-inode owner design can execute
independent directories concurrently instead of measuring unavoidable
same-directory write contention.

| Operation | Timed scope |
| --- | --- |
| `open` | Existing-file `open` plus the required `close` |
| `stat` | Existing-file `stat` |
| `listdir` | `opendir`, full `readdir` scan, and `closedir` |
| `mkdir` | Unique child-directory creation; removal is outside the timed interval |

Fixture construction and all cleanup are outside the timed interval. Open and
stat rotate over the configured entries, while listdir scans every entry in its
thread-local directory, so `dirent_scan` observes different lookup positions.
The benchmark prints its `CLOCK_MONOTONIC_RAW` start/end timestamps; the trace
summarizer uses that window to exclude fixture creation and cleanup on the same
host. Before creating the measured fixture, it opens an adapter probe and
refuses a host fd unless `--allow-host-fs` was explicitly selected.

For a host-only functional smoke test:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build -j --target orchfs_metadata_bench
ctest --test-dir build -R '^metadata_benchmark_.*_host$' --output-on-failure
```

## Current-device baseline

The runner reformats the selected NVMe namespace before every sample. It uses
the same exact device-identity checks, CPU partitioning, raw logs, manifests,
and driver restoration as the paper reproduction runner. The destructive token
must match all three selected devices. It also rejects benchmark builds that
are not `RelWithDebInfo` or whose trace option does not match the requested run:

```bash
cmake -S . -B build-repro \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DORCHFS_BUILD_KFS=ON -DORCHFS_REPRO_TRACE=OFF
cmake -S . -B build-repro-trace \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DORCHFS_BUILD_KFS=ON -DORCHFS_REPRO_TRACE=ON
cmake --build build-repro -j
cmake --build build-repro-trace -j

export ORCHFS_REPRO_DESTRUCTIVE='BDF=0000:b2:00.0;NSID=1;DAX=/dev/dax0.1'

# Throughput baseline: trace-off, five repetitions at 1T and 16T.
scripts/reproduction/run_metadata.py --scale paper

# Attribution baseline: separate trace-on run; do not use its throughput as
# the performance gate.
scripts/reproduction/run_metadata.py --scale smoke --trace \
  --trace-sample-every 1
```

The runner emits `csv/metadata_benchmark.csv`, `csv/metadata_trace.csv`, all raw
client/server traces and logs, a manifest with commit/device state, and a short
`report.md`. Keep the trace-off pre-change result directory as the paired
baseline for the later `NamespaceCoordinator` commit; do not compare against an
unrelated historical machine state.
