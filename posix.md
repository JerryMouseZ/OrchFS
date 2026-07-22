# POSIX Adapter 优化执行结果

日期：2026-07-21

## 结论

已在获准覆盖的 Samsung PM173X SPDK namespace（`0000:b2:00.0`，NSID 1）上完成分层基线、QD64、QD1、100-session 长稳态和覆盖性回归测试。POSIX Adapter 的最终 QD64 五轮中位数为写 **3799.70 MiB/s**、读 **5544.79 MiB/s**，分别达到 native Client/SHM-RPC/KFS E2E 的 **99.65%** 和 **84.10%**，写读均超过原计划的 70% 性能门槛。

本轮同时解除进程级 I/O 串行化，落地零共享完成态的同步快速桥、单 client worker/多 SHM lane、Runtime/poller 热路径减负和低并发自适应短自旋。QD1 相对优化前的 POSIX no-spin 基线，吞吐写/读提高 **5.64%/10.54%**，p99 降低 **38.54%/4.73%**；因此 QD1“吞吐和 p99 回退不超过5%”门槛通过。native QD1 p99 仍比 POSIX 好 6.14%/23.69%，这是后续优化空间，不把它误报为已经达到 native p99 parity。

## 2026-07-22 读路径后续

已完成 `goal.md` 中优先级最高的同步边界调整：positioned I/O 不再创建并等待 root coroutine。外部调用线程现在直接向 Client session 发布同协议 pending；client worker 只取得 completion slot lease 并发布完成，原 `pread` 调用线程负责把 SHM payload 复制到用户 buffer，并在 lease 析构时释放 CQ slot。Runtime 的自适应阻塞等待策略仍封装在 Runtime/Session 内部，没有扩散到 POSIX Adapter 接口。

同一机器、同一 SPDK namespace 上，以优化前 `HEAD=32f6cf0` 编译出的独立二进制为基线，做五组交替顺序、fresh-server、64 KiB、QD64、每轮4 GiB的配对 A/B：

| 版本 | 读 MiB/s 中位数 | p99 µs 中位数 |
| --- | ---: | ---: |
| 优化前 root-coroutine POSIX 路径 | 5797.63 | 3000.00 |
| **调用线程 copy + 直接同步 pending** | **6073.83** | **3082.05** |
| 变化 | **+4.76%** | **+2.74%** |

配对原始数据见[`metrics.tsv`](/mnt/home/jz/Projects/OrchFS/benchmark-results/direct-pending-ab-20260722-092032/metrics.tsv)。相对前一日 native SHM-RPC 的6593.27 MiB/s，这个长窗口结果为92.12%；由于 native 数值不是同一时段的配对样本，只把它用于剩余差距定位，不把7.88%的差距当成严格 A/B 结论。

profile 也与所有权变化一致：client 侧 `memmove` cycles 从4.04%降到0.89%，root coroutine 调度符号退出主要热点，剩余热点转为 completion ring acquire、client worker loop和poll lane。新旧 profile 分别见[`posix-direct-pending-profile-20260722-091727`](/mnt/home/jz/Projects/OrchFS/benchmark-results/posix-direct-pending-profile-20260722-091727/)和[`posix-profile-finalpath-20260721-222319`](/mnt/home/jz/Projects/OrchFS/benchmark-results/posix-profile-finalpath-20260721-222319/)。

最终代码的独立五轮短窗口回归为：QD64写/读3818.04/5623.40 MiB/s、p99 3111.95/3008.57 µs；QD1写/读3679.23/1864.43 MiB/s、p99 119.39/260.81 µs。QD1相对前一轮接受值的吞吐变化为-2.64%/+2.71%，两项p99均改善，因此“不回退超过5%”门槛继续通过。样本见[`QD64`](/mnt/home/jz/Projects/OrchFS/benchmark-results/posix-direct-pending-qd64-20260722-091544/samples.log)和[`QD1`](/mnt/home/jz/Projects/OrchFS/benchmark-results/posix-direct-pending-qd1-20260722-091823/samples.log)。

后两个建议均做了可运行原型和配对 A/B，但没有留下无收益复杂度：

| 原型 | 吞吐变化 | p99变化 | 处理 |
| --- | ---: | ---: | --- |
| Runtime 每轮批量执行8个 WorkItem | +0.46% | +0.54% | 噪声范围，完整回滚 |
| idle-poller ready bitmap + 按需 quiescence | -0.56% | -0.08% | 无吞吐收益，完整回滚 |
| 仅按需 quiescence | -0.62% | +0.62% | 无收益，完整回滚 |
| aligned SSD read 按物理 block 分散到 data worker | -0.16% | +1.31% | 多一次 worker hop 未换来吞吐，完整回滚 |

对应数据见[`work batch`](/mnt/home/jz/Projects/OrchFS/benchmark-results/runtime-work-batch-ab-20260722-092859/metrics.tsv)、[`ready bitmap`](/mnt/home/jz/Projects/OrchFS/benchmark-results/runtime-ready-bitmap-ab-20260722-094022/metrics.tsv)、[`quiescence`](/mnt/home/jz/Projects/OrchFS/benchmark-results/runtime-quiescence-ab-20260722-094356/metrics.tsv)和[`read sharding`](/mnt/home/jz/Projects/OrchFS/benchmark-results/kfs-read-sharding-ab-20260722-095427/metrics.tsv)。这些结果说明当前4-stream/QD64口径下，固定轮询和inode owner并不是再加一层调度就能兑现的瓶颈；后续若重做，应先增加按poller/worker/queue的负载计数，再设计无需额外跨worker往返的数据面。

## 执行状态

| 优先级 | 状态 | 本轮结果 |
| --- | --- | --- |
| 1. 深化 POSIX Adapter | **完成** | `SessionEpoch`、lease、slot inflight 与 `open → closing → closed` 已落地；全局 lifecycle gate 不再包住远端等待；peak inflight 实测64。 |
| 2. Session 长稳态 | **完成** | Session retirement 和 Runtime poller generation 均由 owner worker 回收；1000次 hugepage churn 无资源增长；session 1→100 吞吐差异 +2.75%。 |
| 3. POSIX 同步快速桥 | **完成** | `Runtime::block_on()` 使用借用的栈上 completion，绕过 `shared_ptr` completion/root registry；并发 block-on 回归已覆盖。 |
| 4. SHM/runtime | **完成本轮有效项** | inbox 只读空检查；poller callback 去除每次调用的两次锁定 RMW；1个 client worker 轮转4条 SHM lane；自适应短自旋只作用于低并发。descriptor 批处理未实施。 |
| 5. 混合轮询 | **原型未通过门槛，已完整回滚** | 外部 fd/epoll 原型在同一短负载 A/B 中观察到约21%的 QD64吞吐下降，未达到“下降不超过3%”；没有把该实现留在当前代码中。 |

## 已落地的边界

### POSIX Adapter

- [`SessionEpoch` 与 slot 生命周期](/mnt/home/jz/Projects/OrchFS/LibFS/async_adapter.cpp)把 `Runtime`、`Client` 和 generation 绑定在一起；旧 fd 在重连后继续指向旧 epoch，不会误发到新 session。
- `acquire_epoch()` 和 `acquire_file()` 只在短临界区内检查状态、pin epoch/slot、增加 inflight，远端请求与同步等待发生在 gate 外。
- `close()` 先切换为 `closing`，阻止新 lease，再等待原有 inflight 清零；活连接上的远端 close 失败会恢复 `open`，死连接则只回收本地代理。
- `shutdown()` 先停止接收新调用，等待所有 active call，再按各自 epoch 关闭 slot，最后停止 Runtime。
- positioned I/O 只有 lifetime lease，可以在相同 fd 上并发；隐式 offset 和 append 的原子顺序由 server 的 inode/offset gate 保证；目录 cursor 仍在 slot owner 下串行推进。

### Session 与 Runtime

- ServerSession 完成后通过 MPSC retirement 链表通知 worker 0；[`drain_retired_sessions()`](/mnt/home/jz/Projects/OrchFS/Async/server.cpp)在 owner-local `sessions_` 中完成移除和 join，不再每轮扫描并复制共享列表。
- Runtime poller 列表仍是不可变 generation，但[`reclaim_poller_generations()`](/mnt/home/jz/Projects/OrchFS/Async/runtime.cpp)由 owner worker 安全删除历史 generation；poll pass 发布 epoch，注销 poller 等待下一个 quiescent epoch，不再为每次 callback 做锁定 RMW。
- [`Runtime::block_on()`](/mnt/home/jz/Projects/OrchFS/include/orchfs/async/runtime.hpp)使用栈上 completion，并确保 worker 完成最后一次 `resume()` 后才发布外部等待者，避免 root frame 生命周期竞态。
- [`ClientOptions::lane_count`](/mnt/home/jz/Projects/OrchFS/include/orchfs/async/client.hpp)把 client worker 数与 IPC lane 数解耦；默认1个 client worker、4条 lane，单个热 worker 轮转 lane，继续利用4个 server worker。
- Client/server completion inbox 在 atomic exchange 前先做只读空检查，见[`client.cpp`](/mnt/home/jz/Projects/OrchFS/Async/client.cpp)和[`server.cpp`](/mnt/home/jz/Projects/OrchFS/Async/server.cpp)。
- Adapter 默认低并发短自旋1024次；并发阻塞等待者超过4后，本 session 永久关闭自旋，因此 QD64 不消耗这条路径。

## 验收结果

| 门槛 | 结果 | 判定 |
| --- | --- | --- |
| Adapter 最大并发请求至少32，理想64 | 64线程并发 `pwrite` 的 `orchfs_async_adapter_peak_inflight()` 为64 | **通过** |
| 同 fd `pread/pwrite`、隐式 offset/append | 64路同 fd positioned 读写回读一致；双线程 append 各64次，无覆盖或丢失 | **通过** |
| I/O 与 close、fork、重连、shutdown 竞争 | 已加入确定性 race fixture；regular、ASan/UBSan、TSan 均通过 | **通过** |
| 连续1000次连接/断开无资源增长 | hugepage run 通过；`HugePages_Free` 为 `2048 → 2048`，DMA register/unregister 平衡，max RSS 19368 KiB | **通过** |
| run 1 与 run 100 吞吐差异不超过5% | 同一 server：session 1为5352.15，session 100为5499.49 MiB/s，差异 +2.75%；早/晚五轮中位数差异 -4.46% | **通过** |
| POSIX 达到 native E2E 的50%，后续70% | QD64写3799.70、读5544.79 MiB/s；为 native 的99.65%/84.10%，也超过固定70%目标2505/4754 | **通过70%** |
| QD1吞吐/p99回退不超过5% | 相对同代码 no-spin A/B：吞吐 +5.64%/+10.54%，p99 -38.54%/-4.73% | **通过** |
| 混合轮询：QD64下降不超过3%，QD1 cycles/op下降25% | QD64短负载 A/B 未过门槛，原型已回滚；QD1收益未声明 | **不通过，不合入** |

## 生产 SPDK 性能结果

统一口径为64 KiB、总量1 GiB、4条 stream、server CPU `1,3,5,7`、client CPU `9,11,13,15`、fresh server、五轮中位数。分层基线见[`median.tsv`](/mnt/home/jz/Projects/OrchFS/benchmark-results/layered-20260721-220133/median.tsv)，最终 QD64 样本见[`samples.log`](/mnt/home/jz/Projects/OrchFS/benchmark-results/posix-qd64-accepted-20260721-224721/samples.log)。

| 路径 | 写 MiB/s | 读 MiB/s | 写 p99 µs | 读 p99 µs |
| --- | ---: | ---: | ---: | ---: |
| Raw SPDK | 4293.52 | 7639.34 | 1689.15 | 1696.97 |
| AsyncBlockDevice direct | 3807.17 | 7766.19 | 2069.67 | 951.88 |
| KfsCoroutineCore direct | 3606.16 | 7606.71 | 3349.16 | 995.64 |
| native Client/SHM-RPC/KFS E2E | 3813.11 | 6593.27 | 2022.32 | 985.16 |
| **POSIX Adapter QD64** | **3799.70** | **5544.79** | **3031.72** | **3000.18** |

QD1 使用每条 stream 一个在途请求，即4条 stream、global QD4；这是 native fixture 可公平对照的“per-stream QD1”。五轮中位数如下：

| QD1 路径 | 写 MiB/s | 读 MiB/s | 写 p99 µs | 读 p99 µs |
| --- | ---: | ---: | ---: | ---: |
| native E2E | 3883.55 | 2049.42 | 113.37 | 218.82 |
| POSIX no-spin A/B 基线 | 3577.14 | 1642.01 | 195.77 | 284.11 |
| **POSIX adaptive-spin 最终值** | **3778.84** | **1815.16** | **120.33** | **270.66** |

样本分别见[`native QD1`](/mnt/home/jz/Projects/OrchFS/benchmark-results/native-qd1-per-stream-five-20260721-222611/samples.log)、[`POSIX no-spin`](/mnt/home/jz/Projects/OrchFS/benchmark-results/posix-qd1-per-stream-five-20260721-222803/samples.log)和[`POSIX 最终 QD1`](/mnt/home/jz/Projects/OrchFS/benchmark-results/posix-qd1-accepted-20260721-224926/samples.log)。最终吞吐为 native 的97.30%/88.57%；p99 相对 native 慢6.14%/23.69%。验收项是本轮优化相对 POSIX 基线不回退，不是 native p99 parity。

## 长稳态与回归

100-session 测试在同一个 live server 上先跑 session 1–5 的完整 QD64 read，再建立/关闭 session 6–95，等待10秒 quiescence 后跑 session 96–100。早/晚窗口中位数为5756.08/5499.49 MiB/s，变化 -4.46%；session 1→100 为 +2.75%，均在5%内。样本见[`samples.log`](/mnt/home/jz/Projects/OrchFS/benchmark-results/posix-session-quiescent-window-20260721-225458/samples.log)。不等待 quiescence 的突发连接会混入 retirement backlog，因此不把瞬时 burst 尾部当作长稳态判定。

验证结果：

- `build-kfs` regular CTest：8/8通过。
- ASan/UBSan：直接相关的 `async_runtime_test` 和 `async_client_server_test` 通过；完整 CTest 为7/8，唯一失败是 `legacy_wrapper_host` 启动时的既有 sanitizer DSO loader 问题：`undefined symbol: __ubsan_vptr_type_cache`。
- TSan：`async_runtime_test` 通过；`async_client_server_test` 在跳过 sanitizer 不可靠的 nested-fork fixture 后通过。
- hugepage session churn 1000次通过，wall 12.16 s、max RSS 19368 KiB、`HugePages_Free` 为 `2048 → 2048`；记录见[`churn-1000-final.time`](/mnt/home/jz/Projects/OrchFS/benchmark-results/churn-1000-final.time)。
- 新增8线程、每线程128次的 concurrent `Runtime::block_on()` 回归；`git diff --check` 通过。

## 未保留的混合轮询原型

外部 fd/epoll 原型使用相同的64 KiB、QD64、4+4 CPU、每流256次操作做10轮 A/B：per-worker futex 版本平均写2740.70、读3119.97 MiB/s；原型平均写2152.52、读2446.02 MiB/s，分别低21.46%和21.60%。这组64 MiB短样本本身存在明显抖动，因此不能把差异全部归因为 epoll；但它足以说明原型没有证明“QD64回退不超过3%”，正确动作是回滚而不是带着已知未过 gate 的实现继续前进。

后续若重做优先级5，应使用至少1 GiB的稳态窗口，同时报告 QD64吞吐、QD1 cycles/op、p99和空闲CPU四项结果。

---

# 原始优化计划（执行前基线）

> 以下内容保留2026-07-21实施前的分析和验收设计。里面描述的“当前代码”和源代码行号是历史快照，不代表上面的执行后状态。

结论：下一轮优化重点应从 SPDK/KFS 核心转向 POSIX Adapter 接缝。原生协程/SHM-RPC 路径已经达到 Raw SPDK 的写 82.50%、读 97.04%；而 POSIX 路径只有原生路径的写 10.83%、读 3.91%。最大的性能空间不在设备层，而在 Adapter 把并发重新串行化了。

当前证据见[公平对比报告](/mnt/home/jz/Projects/OrchFS/benchmark-results/fair-sync-async-20260721/report.md:1)和[原生异步分层结果](/mnt/home/jz/Projects/OrchFS/docs/optimization-results-20260721.md:35)。

## 严格优化顺序

| 优先级 | 优化项 | 主要目标 |
| --- | --- | --- |
| 1 | 解除 POSIX Adapter 全局串行化 | 让 QD64 真正进入异步核心 |
| 2 | 修复长生命周期 Session 退化 | 消除多次连接后的 22%–29% 降速 |
| 3 | POSIX 同步调用快速桥 | 消除每 syscall 的 root coroutine、堆分配和全局登记开销 |
| 4 | SHM ring、inbox、worker 调度优化 | 降低当前剩余的 runtime/transport cycles |
| 5 | 混合轮询与按 worker 唤醒 | 降低 QD1、空闲和轻负载 CPU |

### 1. 深化 POSIX Adapter Module

当前 `pread/pwrite` 从取得进程级 `lifecycle_owner`，一直持有到 `submit().join()` 返回：[pread](/mnt/home/jz/Projects/OrchFS/LibFS/async_adapter.cpp:879)、[pwrite](/mnt/home/jz/Projects/OrchFS/LibFS/async_adapter.cpp:910)、[run](/mnt/home/jz/Projects/OrchFS/LibFS/async_adapter.cpp:228)。因此64个应用线程实际上接近 QD1。

建议把它改造成一个深的 `PosixSession` Module：

- 外部 Interface 仍保持 `open/read/pread/write/pwrite/fsync/close`，不改变 LD_PRELOAD 兼容方式。
- `lifecycle_owner` 只管理连接、重连、Session epoch 切换和 shutdown。
- 每次操作获取一个 RAII `FileLease`，它同时 pin 住 `FileSlot` 和对应 `SessionEpoch`。
- 取得 lease 后立即释放全局 gate，再执行远端请求和同步等待。
- `run()` 接收稳定的 epoch，而不是直接访问可能被重连或 shutdown 替换的全局 `runtime/client`。
- `FileSlot` 使用 `open → closing → closed` 状态和 `inflight` 计数。
- `close` 先禁止新 lease，等待已有操作退出，再远端 close；失败时按现有语义恢复为 open，连接已死时只做本地回收。
- shutdown 先停止新 lease，再等待所有 epoch/slot drain，最后停止 Runtime。

不同操作的同步要求应分开：

| 操作 | Adapter 内需要的同步 |
| --- | --- |
| `pread/pwrite` | 只有 lifetime lease；相同或不同 fd 均可并发 |
| `read/write/lseek/O_APPEND` | lifetime lease + 文件偏移顺序 |
| `fsync/ftruncate` | lifetime lease；整文件 barrier 由 server/KFS 仲裁 |
| `close` | 阻止新 lease并等待 inflight 清零 |
| `readdir` | 保留目录 cursor 串行化 |

Server 已经只对隐式 offset 和 append 使用 `offset_gate`，positioned I/O 可以并发，[读路径](/mnt/home/jz/Projects/OrchFS/Async/server.cpp:880)和[写路径](/mnt/home/jz/Projects/OrchFS/Async/server.cpp:965)具备承接这种并发的基础。不能只删除锁而不增加 epoch/lease，否则会产生 close、重连和 shutdown 生命周期竞争。

第一阶段验收目标——这是门槛，不是收益预测：

- 实测最大并发请求从约1提升到至少32，理想为64。
- POSIX 路径首先达到原生 E2E 的50%：写不低于1790 MiB/s、读不低于3395 MiB/s。
- 后续目标为70%：写2505 MiB/s、读4754 MiB/s。
- QD1 吞吐和p99回退不超过5%。
- 增加同 fd 并发 `pread/pwrite`、隐式 offset、append、I/O 与 close、fork、重连和 shutdown 竞争测试。

### 2. 修复长生命周期 Session

当前 fresh-server 中位数为写387.51、读265.31 MiB/s；连续复用 server 时降到300.05和189.09 MiB/s。原因还不能只凭现有数据定论，但已有两个明确结构开销：

- `reap_finished_sessions()` 在每轮 accept poll 都扫描 Session 列表，read profile 中占2.14%，而1024轮采样判断发生在扫描之后：[server.cpp](/mnt/home/jz/Projects/OrchFS/Async/server.cpp:1926)。
- Runtime 保存所有历史 `poller_generations`，目前没有运行期回收：[runtime.cpp](/mnt/home/jz/Projects/OrchFS/Async/runtime.cpp:984)。

建议：

- Session 完成时向 worker 0 发布 retirement event，替代每轮扫描。
- Session registry 改成 owner-local，shutdown 才获取稳定快照。
- Poller generation 使用 worker epoch 安全回收。
- 记录 active/retired session、poller generation、DMA region、RSS和hugepage映射数。
- 单独检查 SPDK DMA register/unregister 是否随客户端连接产生累积；先计数验证，不预判它是主因。

门槛是连续1000次连接/断开无资源增长，run 1与run 100吞吐差异不超过5%。

### 3. 替换 `submit().join()` 同步快速路径

解除全局 gate 后，每个 POSIX syscall 都会暴露新的成本：

- 应用线程创建 coroutine 时无法使用 worker frame pool，会走堆分配。
- `Runtime::submit()` 每次创建 `shared_ptr<CompletionState>`：[runtime.hpp](/mnt/home/jz/Projects/OrchFS/include/orchfs/async/runtime.hpp:69)。
- root registry 使用全局 `roots_update`。
- 然后应用线程再用 atomic wait 阻塞。

应直接替换这个 Adapter fast path，不再叠加新线程池：

- 每个应用线程复用 TLS `CallCell`。
- 请求通过 worker-local MPSC 队列进入固定 worker。
- coroutine 在目标 worker 上创建，从而使用现有 frame pool。
- 完成后直接唤醒 `CallCell` 的 futex。
- fd保存 preferred lane；条件允许时与 server inode owner 对齐。

是否进入这一阶段，应由完成优先级1后的新 profile 决定。

### 4. 收缩 SHM/runtime 开销

当前 profile 中，server `drain_completion_inbox` 约4.9%，ring acquire约3%，client inbox约2.3%。建议依次做：

- inbox 先执行只读空检查，非空才做 atomic exchange；同时记录真实 cross-worker completion 数量。
- 一次处理多个连续 ring descriptor，批量更新 consumer/producer位置。
- open返回 preferred lane，减少“lane worker → inode owner → lane worker”的跨 worker 往返。
- 将 `Runtime::notify(worker)` 改成真正的 per-worker wake epoch；当前实现实际执行 `wake_all()`。

已有对象池、lane分片、共享DMA、extent快照和group commit不要重复实现。

### 5. 混合轮询

Server lane 当前为了等待外部 eventfd，无请求时仍返回 `busy`：[server.cpp](/mnt/home/jz/Projects/OrchFS/Async/server.cpp:1716)。建议 Runtime 支持 worker-local外部fd等待：

- 有 inflight或刚发生进展时 busy-poll。
- ring为空且设备无 inflight时进入 epoll/futex等待。
- listener、lane eventfd和Runtime wake fd统一唤醒目标 worker。

门槛：QD64吞吐下降不超过3%，QD1 cycles/op下降至少25%，无客户端时CPU接近空闲。
