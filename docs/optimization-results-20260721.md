# Coroutine/SPDK optimization results (2026-07-21)

This report records the staged measurements for the eight optimizations in
[`async-optimization-plan.md`](async-optimization-plan.md). Every row is the
median of exactly five runs with 64 KiB requests,
64 coroutines, four KFS workers, four client workers, and fixed CPU affinity.
The server CPUs were `1,3,5,7`; the client CPUs were `9,11,13,15`. All eight
CPUs and NVMe controller `0000:b2:00.0` are on NUMA node 1.

The benchmark destroys and reformats namespace 1. Raw SPDK, direct
`AsyncBlockDevice`, direct `KfsCoroutineCore`, and shared-memory RPC E2E use the
same one-GiB read/write workload. The write phases include the OrchFS sync
operation; raw SPDK reports its native write workload.

## Staged E2E result

| Stage | Result | Write MiB/s | Write p99 us | Read MiB/s | Read p99 us | CPU profile |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| Pre-optimization baseline | five-run median | 199.91 | 37342.41 | 258.17 | 30770.12 | baseline inspection |
| 1. Lane-sharded session data plane | five-run median | 400.10 | 20061.84 | 494.07 | 16537.18 | captured |
| 2. Shared DMA slots and zero-copy server path | five-run median | 446.72 | 17115.92 | 587.49 | 12475.66 | captured |
| 3. Adjacent 32 KiB I/O merge | five-run median | 454.28 | 16396.58 | 598.30 | 12371.57 | captured |
| 4. Empty-journal fast path and group commit | five-run median | 595.57 | 12259.56 | 605.74 | 12338.27 | captured |
| 5. Owner-local arbitration and Runtime fast path | five-run median | 3379.82 | 1781.94 | 3789.63 | 1709.47 | captured |
| 6. Per-worker/per-poller object pools | five-run median | 3584.98 | 3432.23 | 6538.58 | 1047.59 | captured |
| 7. Versioned inode extent snapshots | five-run median | 3854.67 | 1819.37 | 6551.22 | 1028.53 | captured |
| 8. Busy-poll, NUMA, and SPDK tuning | five-run median | 3578.47 | 3255.74 | 6790.91 | 967.12 | captured |

The final E2E path is 17.90 times faster for writes and 26.30 times faster for
reads than the pre-optimization baseline. Write p99 fell by 91.28%; read p99
fell by 96.86%.

## Final layer gates

The final comparable five-run set is retained locally under
`benchmark-results/layered-20260721-151945`; generated benchmark artifacts are
excluded from version control.

| Gate | Write | Read | Target | Result |
| --- | ---: | ---: | ---: | --- |
| AsyncBlockDevice / Raw SPDK | 88.14% | 111.62% | at least 90% | write misses; read passes |
| KfsCoroutineCore / AsyncBlockDevice | 99.59% | 98.74% | at least 80% | pass |
| RPC E2E / KfsCoroutineCore | 93.98% | 88.05% | at least 75% | pass |
| RPC E2E / Raw SPDK | 82.50% | 97.04% | at least 50%, then 70% | pass 70% |

Write throughput was bimodal on this device during the repeated acceptance
runs. The queue-depth-32 sets measured AsyncBlockDevice at 82.02% to 88.64% of
raw; the final set above measured 88.14%. None clears the 90% direct-write gate,
so it is reported as a real remaining gap rather than hidden by selecting the
fastest run.

## Parameter and profile conclusions

- Queue depth 32 is the selected default. The workload has 16 outstanding
  requests per worker, so 32 retains headroom. In the queue-depth-16 comparison,
  E2E read median fell to 5936.95 MiB/s.
- Bounce buffers were reduced from 64 to 32 per poller. Registered shared data
  slots bypass bounce copies for the steady aligned path. Maximum transfer
  remains 1 MiB.
- Completion eventfd drains were removed from busy-poll loops. Socket health,
  listener accept, and SPDK admin polling are sampled every 1024 iterations.
- The final profile has no eventfd-drain or `accept4` sample. SPDK admin
  polling and poller-entry bookkeeping are below sampling resolution.
- Removing steady `shared_from_this()` traffic from client lane polling reduced
  `Session::poll_lane` self time from 22.93% in the diagnostic profile to
  0.26% in the final profile, while a shutdown-only lifetime guard preserves
  callback safety.
- The only sampled allocation stacks total 0.11% and belong to benchmark setup:
  journal population and initial extent-snapshot publication. There is no
  `CoroutineFramePool::allocate_heap` or Runtime `WorkNode` heap-fallback
  sample on the steady request path.
- SPDK request state, bridge completion contexts, device completion contexts,
  common RPC pending objects, coroutine frames, local and cross-worker Runtime
  work nodes, range waiters, fixed completion queues, and journal records use
  retained or fixed pools on the common path.
