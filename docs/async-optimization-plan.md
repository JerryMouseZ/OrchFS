# Coroutine/SPDK optimization plan

## 前置工作：增加分层基准

  不计入优化排名，但必须先完成：

  Raw SPDK
    ↓
  AsyncBlockDevice direct
    ↓
  KfsCoroutineCore direct
    ↓
  Client/SHM RPC/KFS E2E

  保持相同的 64 KiB、QD、64 协程、绑核和五次中位数。否则无法判断每项优化实际消除了哪一层损耗。

   排名    优化项                                         影响范围                 预计影响
  ━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━
      1    Session 数据面按 lane 分片                     Read/Write               极高
  ──────  ─────────────────────────────────────────────  ───────────────────────  ──────────
      2    共享 DMA buffer 和零拷贝                       Read/Write，尤其 Read    极高
  ──────  ─────────────────────────────────────────────  ───────────────────────  ──────────
      3    合并相邻 32 KiB I/O                            Read/Write               高
  ──────  ─────────────────────────────────────────────  ───────────────────────  ──────────
      4    空 journal 快速路径和 group commit             Write                    高
  ──────  ─────────────────────────────────────────────  ───────────────────────  ──────────
      5    owner-local arbitration 和 Runtime 快速路径    Read/Write、p99          中高
  ──────  ─────────────────────────────────────────────  ───────────────────────  ──────────
      6    消除每请求堆分配、使用对象池                   Read/Write、p99          中高
  ──────  ─────────────────────────────────────────────  ───────────────────────  ──────────
      7    inode extent 快照缓存                          Read/Write               中
  ──────  ─────────────────────────────────────────────  ───────────────────────  ──────────
      8    busy-poll、NUMA 和 SPDK 参数调优               Read/Write               中低

  ## 1. Session 数据面按 lane 分片

  当前一个 Client/Server session 的所有 lane 都由单个 owner 轮询：Async/client.cpp:415、Async/server.cpp:1551。

  改为：

  - session 控制面仍由固定 owner 管理握手、句柄表和退出。
  - lane i 固定到 Runtime worker i % worker_count。
  - 每个 worker 直接处理自己的 submission/completion ring。
  - completion 直接写回原 lane，不再经过 session owner 的集中队列。
  - shutdown owner 只聚合各 lane 的 inflight 计数。

  这是第一优先级，因为不消除单 worker 汇聚点，后续增加 qpair、worker 或 QD 都无法线性扩展。

  ## 2. 共享 DMA buffer 和零拷贝

  当前请求经过多次完整复制，例如 Server 先读取到临时 buffer，再复制到 Incoming::payload：Async/server.cpp:1491。

  方案：

  - control ring 与 data slot 分离。
  - data slot 使用对齐的 hugepage/memfd 内存。
  - Server 侧通过 spdk_mem_register 注册共享数据区。
  - Write：NVMe 直接从 submission data slot DMA。
  - Read：NVMe 直接 DMA 到 completion data slot。
  - Incoming/Completion 保存 slot lease，不再保存 vector<byte>。
  - slot 只有在 NVMe completion、RPC completion及取消回收全部结束后才能复用。

  应用缓冲区与共享区之间仍可能需要一次复制，但可以移除 Server 中间复制和 SPDK bounce copy。

  ## 3. 合并相邻 32 KiB I/O

  磁盘格式块大小是 32 KiB：Async/kfs_coroutine_core.cpp:38。当前一个 64 KiB RPC 通常生成两个独立 SPDK request。

  在 AsyncBlockDevice::read_batch/write_batch 前加入合并器：

  - 设备地址连续；
  - 用户 buffer 连续；
  - 操作类型相同；
  - 不跨越错误语义或 transaction phase；

  满足条件时合并为一个 64/128/256 KiB NVMe command。同时让 SSD allocator 优先分配连续 extent。

  ## 4. Journal 快速路径与批量提交

  当前 aligned overwrite 也会无条件创建 transaction，并提交到 worker 0：Async/kfs_coroutine_core.cpp:1213。

  优化：

  - mapping、size、metadata 均未变化时，直接判定为空 transaction，不进入 journal。
  - metadata transaction 进入 worker 0 的 group-commit 队列。
  - 一批 transaction 共享日志写入和持久化 fence。
  - fsync/sync 强制关闭当前 batch 并等待落盘。
  - namespace 操作继续保持单 transaction 原子性。

  如果只优化 Write，这一项应提升到第二优先级。

  ## 5. Owner-local arbitration 与 Runtime 快速路径

  RangeArbiter 每次 acquire/release 都复制整个不可变快照：Async/range_arbiter.cpp:148。

  由于 I/O 已固定到 inode owner，可以改成：

  - owner-local intrusive interval queue；
  - 无 CAS、无 shared_ptr、无 snapshot clone；
  - 跨 worker 请求只通过 MPSC 进入 owner；
  - Runtime poller 列表使用 epoch/generation，避免每轮 poll 增减 shared_ptr 引用；
  - 热路径使用 _mm_pause，不要频繁 sched_yield。

  ## 6. 每 worker 对象池

  预分配并复用：

  - RPC request/completion；
  - SPDK request 和 completion context；
  - 常见两块 I/O 的固定大小 plan；
  - journal record；
  - range waiter；
  - 64 KiB/128 KiB DMA slot。

  常见 64 KiB 请求不应调用 new/delete、make_shared 或动态扩容 vector。

  ## 7. Inode extent 快照

  为每个 inode 发布只读 extent snapshot：

  logical block range → SSD/NVM/STRATA + physical range

  steady read和对齐 overwrite 直接读取快照，不再为每个 32 KiB 块重复遍历旧索引。truncate、migration和新 extent commit 后原子发布新版本。

  ## 8. 最后再做参数调优

  架构瓶颈消除后再调整：

  - SPDK queue depth；
  - 每 poller bounce/DMA slot 数；
  - max transfer size；
  - completion batch；
  - Runtime spin 时间；
  - NVMe、DAX和 worker NUMA 亲和性。

  单纯增加 QD、worker 或 spin 当前不会解决 18–31 倍差距。

  ### 建议性能门禁

  - AsyncBlockDevice direct：达到 Raw SPDK 的至少 90%。
  - KfsCoroutineCore direct：达到 AsyncBlockDevice 的至少 80%。
  - RPC E2E：达到 KfsCoroutineCore direct 的至少 75%。
  - 第一阶段总目标：KFS E2E 达到 Raw 的 50%以上，再向 70%推进。
  - 每完成一个排名项，都重新运行五次中位数、p99和 CPU profile，禁止多项合并后才测量。
