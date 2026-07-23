# FAST'25 论文负载的同步/异步差分复现

本文档定义一套可审计、可重跑的差分实验：保留 FAST'25 论文中的负载形态，
只比较原始同步 OrchFS 与当前异步 OrchFS。它不是论文全部基线文件系统的逐项
复现，也不能用来复述论文中的跨文件系统倍数。

论文原文与公开代码：

- [FAST'25 论文](https://www.usenix.org/system/files/fast25-zhan.pdf)
- [USENIX 论文页面](https://www.usenix.org/conference/fast25/presentation/zhan)
- [OrchFS 公开仓库](https://github.com/YekangZhan/OrchFS)
- [GridGraph 公开仓库](https://github.com/thu-pacman/GridGraph)
- [SNAP LiveJournal 数据集](https://snap.stanford.edu/data/soc-LiveJournal1.html)

## 固定口径

| 项目 | 本复现的取值 |
| --- | --- |
| 同步版本 | `288fbfc4250966829d06aa1d3c02c92b9e806162`，外加下述两处公平性修正 |
| 异步版本 | 运行器启动时记录当前 `HEAD`；结果不能跨提交拼接 |
| 随机种子 | `20260722` |
| 比较对象 | `sync-fair`、`async-current`，不运行其他文件系统 |
| 持久化 | async 的成功 `pwrite` 返回时已持久化；sync-fair 通过紧随其后的真实 `fsync()` 达到同一测量边界；async `fsync()` 仅做本地 descriptor 校验 |
| page cache | 同步 SSD 读写使用 `O_DIRECT`；异步 SPDK 天然绕过内核 block page cache |
| 重复次数 | 论文规模为至少 3 次，绘图使用算术平均；smoke 规模只用于验证流程，不能标为论文规模 |
| 交替顺序 | 微基准逐 case、Filebench 逐 workload/thread、LevelDB 逐 phase 运行两版，并随 case/repeat 反转先后顺序 |
| CPU 放置 | 不照搬论文绑定表；runner 从自身 affinity 和 NVMe NUMA node 推导，优先物理核，为 LibFS 和 KFS Runtime 分配互斥 CPU 并逐行记录 |
| 软件版本 | FIO、Filebench、LevelDB/YCSB、GridGraph 的实际提交或包版本写入每个结果目录的 `manifest.tsv` |
| 忽略项 | 不复现基线 FS；不复现 RAID/第二块 SSD；不控制 Turbo；不照搬论文 CPU 绑定表 |

同步公平版只允许两处语义修正，不能带入异步算法：

1. `KernelFS/device.c` 的 SSD 读 fd 从 `O_SYNC` 改为 `O_DIRECT`，避免旧版读
   命中 Linux block page cache；
2. `LibFS/orchfs_wrapper.c` 的 OrchFS `fsync()` 从直接返回 0 改为 NVM
   `_mm_sfence()` 加 SSD fd 的真实 `fsync()`。

两边必须使用同一 `/dev/dax` namespace、同一 NVMe namespace、同一编译优化
级别和相同的应用请求序列。同步版经 kernel NVMe 驱动访问 SSD，异步版经
SPDK 访问同一控制器；切换驱动后必须重新确认设备身份，绝不能根据变化后的
`/dev/nvmeXnY` 编号猜测设备。

异步版通过 `--async-durability auto|completion|fua|flush` 选择 SSD 策略并写入
manifest。`auto` 仅在控制器报告没有 volatile write cache 时采用 completion，
否则使用 FUA；显式 `completion` 在存在 VWC 时会启动失败。因此不能为了得到更好
数字在有易失缓存的设备上把 completion 当成持久化完成。当前 PM173X 的显式
completion 运行通过，KFS 日志同时记录 effective policy 与 VWC bit。

## 规模档位

运行器提供两个档位：

| 档位 | 用途 | 数据规模 | 可否作为论文规模结果 |
| --- | --- | --- | --- |
| `smoke` | 尽快发现 API、格式化、持久化、绘图和采集错误 | 微基准单文件通常 4--16 MiB；时间序列 5 s；每 case 1 次；应用负载另见 manifest | 否 |
| `paper` | 对齐论文负载 | 10 GiB prefill/overwrite、30 s read、至少 3 次；Fig.14/19 使用论文规模 | 是，但仍只是同步/异步差分复现 |

结果必须把 `scale` 作为数据列和图标题的一部分。禁止把 smoke 数据和 paper
数据画在同一条曲线上。

## 论文图表到本复现的映射

| 论文项目 | 差分复现 | 本地输出 | 说明 |
| --- | --- | --- | --- |
| Fig.1 | 两种 size range 的单线程随机写吞吐 | `fig01_random_write.csv` | 只画 sync/async；不画 PCIe5/基线 FS |
| Fig.2 | 1 MiB 顺序写与 1 B--2 MiB 随机写，逐写 `fsync` | `fig02_write_latency.csv` | 用同一持久化口径 |
| Fig.3 | 4 KiB--1 MiB 对齐与非对齐随机写 | `fig03_alignment.csv` | 对比 OrchFS 自身转换路径，不再解释为 EXT4/F2FS host-RMW |
| Fig.4 | 写路径开销分解 | `fig04_profile.csv`、`trace_stages.csv`、`trace_requests.csv`、`sync_ebpf.csv` | sync 用 eBPF；async 用源码记录；不能把 block tracepoint 的 0 当成无 SSD 开销 |
| Fig.5 | 单线程大 I/O 与拆为 32 KiB 的并行 I/O | `fig05_split_io.csv` | 同一总字节数 |
| Fig.6--9 | 无运行实验 | 无 | 架构/算法图，保留原论文即可 |
| Fig.10 | 10 GiB prefill 后 10 GiB 单线程随机 overwrite | `fig10_write_latency.csv` | 1 KiB--1 MiB 和 1 B--2 MiB |
| Fig.11a | async 源码阶段记录 | `trace_stages.csv`、`trace_requests.csv` | 阶段 span 可并行重叠，另给 request wall time，不强行堆成 100% |
| Fig.11b | KFS worker=1 与默认 worker 数 | `fig11_parallel.csv` | 是当前实现的串行/并行代理，不宣称等同旧论文开关 |
| Fig.12 | KFS Runtime worker 数 1,2,4,8,16,32,48,64 | `fig12_workers.csv` | SPDK qpair 默认最多 4 个，并由 worker owner 轮询；记录实际 CPU，不能把横轴误称为 qpair 数 |
| Table 2 | NVM page、NVM sub-page/strata、SSD block 占用 | `table02_space.csv` | async 用 KFS bitmap 快照；同步版没有可信计数接口，标 `unsupported`，不估算 |
| Fig.13 | 四种分布 prefill 后 30 s 随机读时间序列 | `fig13_read_timeseries.csv` | 本复现没有 page cache，因此只比较稳定设备/软件路径 |
| Fig.14 | 多文件与单文件线程扩展 | `fig14_thread_scale.csv` | paper 档位：16 个 5 GiB 文件；单文件 10 GiB、每线程 3 GiB |
| Fig.15 | 单设备 append/random 线程扩展 | `fig15_single_device.csv` | RAID、PCIe5 被明确省略，不再称“different SSD setups” |
| Fig.16 | Fileserver/Webproxy/Varmail，1/16 threads | `fig16_filebench.csv` | 每个 workload 的生成文件需持久化写；记录 prepare 与 run 是否成功 |
| Fig.17 | LevelDB + YCSB Load/A--F 形态 | `fig17_ycsb_leveldb.csv` | 当前是嵌入式 C++ 驱动，不是 Java YCSB；64 MiB write buffer、同步写、1 KiB value；paper 档位 5M ops |
| Fig.18 | GridGraph PageRank | `fig18_gridgraph.csv` | 8 GiB memory、20 iterations；LiveJournal（539 MiB）、Twitter（11.5 GiB）、Friendster（28.2 GiB）；公开 wrapper 不支持 virtual-fd `mmap`，故两版使用同一 compatibility patch 将 vertex vector 放在匿名内存，实际比较 edge `pread` 路径 |
| Fig.19 | 关闭 migration 后的 NVM sub-page 比例 | `fig19_fragmentation.csv` | paper 检查点到 1 TiB；缩放结果必须在 x 轴标真实写入量 |

## Profiling 与异步源码记录

同步版使用 eBPF 采集 syscall、调度和 kernel block I/O；推荐的稳定 tracepoint
集合为 `syscalls:sys_enter_pwrite64`、`syscalls:sys_exit_pwrite64`、
`syscalls:sys_enter_fsync`、`syscalls:sys_exit_fsync`、
`block:block_rq_issue`、`block:block_rq_complete`、`sched:sched_switch`。
若当前内核缺少某 tracepoint，采集器应失败并在 manifest 中记录，不能静默少列。

异步版的 SSD 数据路径绕过 kernel block layer，因此使用编译期开关
`ORCHFS_REPRO_TRACE=ON` 的源码记录。记录项至少包括：

```text
request_id, operation, bytes, adapter/client round trip,
server dispatch span, metadata/index/plan CPU span,
NVM read/write bytes and span, SPDK submit-to-complete span,
flush span, child IO count, result errno
```

当前实现只在 client round trip 与 server dispatch 之间保留可关联的
`request_id`；core/device/NVM/SPDK 的子 span 以 `request_id=0` 记录并做聚合统计。
因此现有结果可以可靠计算 `client RTT - server dispatch`，但不能把每个底层 span
逐请求还原成精确 critical path。若要发布逐请求瀑布图，需要先增加跨 coroutine
suspend/resume 安全的 request context 传播，不能用普通线程局部变量代替。

热路径禁止 `printf`、逐事件文件写和全局锁。当前记录器在每个线程第一次采样
时惰性分配一个固定容量 buffer，后续事件不再分配；发布用途若要把首次分配也
排除在 measurement 之外，应在 worker 启动后显式预热。所有记录在有序退出时
批量落盘。并行 NVM/SSD child I/O 保留独立 span，因此阶段总和可能大于 wall
time。发布 breakdown 时同时提供：

- 每阶段累计工作时间/字节数；
- 请求 wall latency；
- 各阶段在 critical path 上的近似占比；
- trace-off 与 trace-on 的 A/B 扰动。扰动超过 3% 时，降低采样率并重跑。

普通性能图使用 trace-off 构建；只有 Fig.4、Fig.11 与 Table 2/Fig.19 的内部
状态实验使用 trace-on 构建。

## 从干净 checkout 构建

当前异步版需要普通构建和源码 trace 构建各一份：

```bash
cmake -S . -B build-repro \
  -DCMAKE_BUILD_TYPE=Release \
  -DORCHFS_BUILD_KFS=ON -DORCHFS_ENABLE_SPDK=ON \
  -DORCHFS_REPRO_TRACE=OFF -DSPDK_ROOT=/opt/orchfs/spdk
cmake --build build-repro -j"$(nproc)"

cmake -S . -B build-repro-trace \
  -DCMAKE_BUILD_TYPE=Release \
  -DORCHFS_BUILD_KFS=ON -DORCHFS_ENABLE_SPDK=ON \
  -DORCHFS_REPRO_TRACE=ON -DSPDK_ROOT=/opt/orchfs/spdk
cmake --build build-repro-trace -j"$(nproc)"
```

同步公平版由固定提交和可审查的小补丁构成。补丁中的 DAX 与 NVMe `by-id`
路径必须先按目标机器改成经过核验的真实设备；不能照抄设备编号：

```bash
git worktree add --detach /tmp/orchfs-sync-baseline \
  288fbfc4250966829d06aa1d3c02c92b9e806162
git -C /tmp/orchfs-sync-baseline apply \
  "$PWD/scripts/reproduction/sync-fair.patch"
cmake -S /tmp/orchfs-sync-baseline \
  -B /tmp/orchfs-sync-baseline/build-fair \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DCMAKE_C_FLAGS='-g -fno-omit-frame-pointer' \
  -DCMAKE_CXX_FLAGS='-g -fno-omit-frame-pointer'
cmake --build /tmp/orchfs-sync-baseline/build-fair -j"$(nproc)"
```

Filebench 必须使用仓库内含 `FILEBENCH` 适配的旧 fork。发行版二进制会经 shell
递归处理 `/Or`，不是同一条路径。当前 glibc 对旧解析器的 fortify 检查会中止，
因此兼容构建仅对 Filebench 自身关闭 fortify/stack protector：

```bash
filebench_bin=$(scripts/reproduction/build_filebench.sh)
cmake -S /tmp/orchfs-sync-baseline \
  -B /tmp/orchfs-sync-baseline/build-filebench \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_C_FLAGS=-DFILEBENCH -DCMAKE_CXX_FLAGS=-DFILEBENCH
cmake --build /tmp/orchfs-sync-baseline/build-filebench -j"$(nproc)"
```

这不是通用 Filebench 安装建议；实际结果目录会记录二进制路径、版本和 SHA-256。

## 运行与重绘命令

所有破坏性运行前都设置下一节定义的精确 token。以下命令使用本机已核验的
BDF/NSID/DAX，仅作为命令形状示例：

```bash
export ORCHFS_REPRO_DESTRUCTIVE='BDF=0000:b2:00.0;NSID=1;DAX=/dev/dax0.1'

# Fig.1--3、5、10--15；每个 prefill 后重启 KFS/LibFS，再计量。
scripts/reproduction/run_reproduction.py --scale smoke \
  --figures 01,02,03,05,10,11,12,13,14,15 \
  --repeats 1 --case-timeout 60

# 可审计的长 smoke A/B；override 对所有选中 case 生效。
scripts/reproduction/run_reproduction.py --scale smoke --figures 04 \
  --case-regex 'fig04/fixed_64KiB_trace_off' --repeats 5 \
  --prefill-bytes 268435456 --measured-bytes 268435456 \
  --async-durability completion --case-timeout 180

# Fig.4 profiling：3 次 trace-off/on，sync 用 eBPF、async 用源码 trace。
scripts/reproduction/run_reproduction.py --scale smoke \
  --figures 04 --repeats 3 --case-timeout 60

# Fig.16。
scripts/reproduction/run_filebench.py --scale smoke \
  --filebench-bin "$filebench_bin"

# Fig.17：嵌入式 LevelDB/YCSB-shape 驱动。
scripts/reproduction/run_leveldb_ycsb.py --scale smoke

# Table 2 / Fig.19：严格随机字节偏移并关闭 migration。
scripts/reproduction/run_fragmentation.py --scale smoke

# Fig.18：完整 LiveJournal、20 iterations、8 GiB memory；smoke 表示单次。
# 先按下文“GridGraph 与 LiveJournal 准备”生成 grid_root。
scripts/reproduction/run_gridgraph.py --scale smoke --repeats 1 \
  --grid-dir "$grid_root/LiveJournal_Grid" \
  --gridgraph-root "$grid_root/GridGraph" \
  --pagerank-bin "$grid_root/GridGraph/bin/pagerank" \
  --source-archive "$grid_root/soc-LiveJournal1.txt.gz"

# 微基准目录可独立重新规范化 CSV 和画 PNG/PDF。
scripts/reproduction/summarize_trace.py benchmark-results/paper-reproduction-TIMESTAMP
scripts/reproduction/plot_reproduction.py benchmark-results/paper-reproduction-TIMESTAMP
```

微基准发生硬件/API 失败时保留 failed 行并继续后续 case。基准程序默认拒绝
小于 `1048576` 的普通 host fd，防止错误的绝对路径绕过 `LD_PRELOAD` 后把宿主
文件系统成绩当成 OrchFS；仅生成器自测可显式使用 `--allow-host-fs`。

runner 不使用论文的硬编码 CPU 表。它读取当前进程允许的 CPU 和目标 NVMe 的
NUMA node，按“本地物理核、远端物理核、本地 SMT sibling、远端 sibling”分层
排序；先为每个 case 分配 KFS worker CPU，再为 LibFS Runtime 分配互斥 CPU，
避免还有空闲物理核时让两个 busy poller 共用 SMT core。`--client-workers 0`
按 benchmark thread 数自动选择 1--8 个 client worker，`--client-lanes 0` 自动
选择 4--32 条 lane；显式正数仍覆盖自动值。实际 worker/lane 数和 CPU 列表写入
`commands.log`，主微基准还逐行写入 `results.csv`。若二者所需 CPU 总数超过
runner 的 affinity，case 直接失败，不能退回到共享同一逻辑 CPU。

每个 IPC data slot 可容纳 2 MiB 请求再加协议头。默认 ring capacity 随 lane
数从 64 缩到 16，使 SQ/CQ payload ring 总量保持在约 2 GiB 内；Filebench 的
parent prepare 与 fork 后 worker 会同时持有 session，因此固定使用 16 lane、
capacity 16，避免耗尽 runner 配置的 4 GiB hugepage pool。

`run_fragmentation.py` 通过 `ORCHFS_REPRO_DISABLE_MIGRATION=1` 关闭 migration，
并用 `SIGUSR1` 在 KFS 主线程安全点写 bitmap 快照。Upage 数取
`BUFMETA_BMP` 相对 fresh-server 的增量，Table 2 的 Page 数再扣除这些 Upage
的 backing page，避免重复计数。

LevelDB runner 只对 async client 设置
`ORCHFS_REPRO_LEVELDB_LOCK_NOOP=1`，使 `F_SETLK/F_SETLKW` 与旧同步 LibFS 一样
返回成功。这只适用于单进程复现，不提供进程间互斥；普通 async 运行不设置该
变量时仍返回 `EOPNOTSUPP`，不能把这个复现开关当成文件锁实现。

## GridGraph 与 LiveJournal 准备

Fig.18 使用公开 GridGraph 提交 `1f1a6262ef6ffbc61bcc229357e106843bd457ac`
和 SNAP LiveJournal。公开同步与异步 wrapper 都没有对 OrchFS virtual fd 实现
`mmap/msync`，而 GridGraph 的 `BigVector` 默认依赖 file-backed mmap。复现补丁
`gridgraph-compat.patch` 对两版共用同一个应用二进制：8 GiB memory 档位下将
vertex vector 放入匿名内存、跳过无意义的 backing-file 初始化和 `mlock`，edge
文件仍由 OrchFS `pread`。补丁还记录每次 edge pass 的实际 read bytes；少于
`iterations + 1` 个完整 pass 的结果必须判为失败，不能采用应用打印的耗时。

本次使用的可重复准备命令如下；转换器输出 x86-64 native little-endian 的
`<uint32 source, uint32 target>`：

```bash
grid_root=/tmp/orchfs-gridgraph
mkdir -p "$grid_root"
git clone https://github.com/thu-pacman/GridGraph.git "$grid_root/GridGraph"
git -C "$grid_root/GridGraph" checkout --detach \
  1f1a6262ef6ffbc61bcc229357e106843bd457ac
test "$(git -C "$grid_root/GridGraph" rev-parse HEAD)" = \
  1f1a6262ef6ffbc61bcc229357e106843bd457ac
git -C "$grid_root/GridGraph" apply \
  "$PWD/scripts/reproduction/gridgraph-compat.patch"

make -C "$grid_root/GridGraph" -j"$(nproc)" \
  CXXFLAGS="-O3 -Wall -std=c++11 -g -fopenmp -I$grid_root/GridGraph \
  -DORCHFS_GRIDGRAPH_ANON_VERTEX=1 -DORCHFS_GRIDGRAPH_RECORD_IO=1" \
  bin/preprocess bin/pagerank

curl -L --fail -o "$grid_root/soc-LiveJournal1.txt.gz" \
  https://snap.stanford.edu/data/soc-LiveJournal1.txt.gz
echo 'd7bcd5a87b88c896c35fdb9611e804c3f4033c39b58c4c9ea3ba53c680d516d8  '"$grid_root/soc-LiveJournal1.txt.gz" | \
  sha256sum -c -
gzip -dc "$grid_root/soc-LiveJournal1.txt.gz" | \
  build-repro/gridgraph_edge_converter > "$grid_root/LiveJournal.bin"
echo '80199ecebb7ebdf3b4861748e009d16b1c5f93c35eba837a7ce37f94ada35f83  '"$grid_root/LiveJournal.bin" | \
  sha256sum -c -

"$grid_root/GridGraph/bin/preprocess" \
  -i "$grid_root/LiveJournal.bin" -o "$grid_root/LiveJournal_Grid" \
  -v 4847571 -p 4 -t 0
```

LiveJournal grid 包含 69,993,773 条边，edge stream 每个完整 pass 应至少读取
`551950184` bytes。`orchfs_repro_copy` 用 4 KiB 对齐写入 staging 文件并立即逐字节
读回校验；runner 重启 KFS/LibFS 后再运行 PageRank，因此同进程读回成功不能掩盖
持久化/恢复问题。

## 安全边界与执行顺序

本实验会反复格式化 OrchFS 的 NVM 和 NVMe namespace。运行前逐项验证：

1. 目标 BDF、NSID、型号、容量和 NUMA node；
2. 目标 NVMe 控制器没有 mount、partition、dm/LVM/md 或其他使用者；
3. DAX namespace 是专用 devdax，容量至少覆盖 OrchFS 格式；
4. 工作机上其他已挂载 NVMe（尤其系统盘和数据盘）不在目标 BDF；
5. `mkfs`、`kfs_main`、benchmark、Filebench、LevelDB/GridGraph 均无残留进程；
6. 结果目录位于独立的非测试设备上。

运行器必须要求精确 token：

```bash
export ORCHFS_REPRO_DESTRUCTIVE='BDF=0000:BB:DD.F;NSID=1;DAX=/dev/daxX.Y'
```

缺少 token、token 与实时探测不符、目标已挂载或无法证明身份时立即停止。
严禁把 `/dev/nvme0n1`、通配符、环境变量展开后的空字符串或 workspace 根目录
作为格式化/清理目标。

推荐顺序：

1. 构建并运行全部无硬件 CTest；
2. 运行 `smoke` 全矩阵，修复功能性失败；
3. 交替运行 Fig.1--5、10--15 的 paper 规模微基准；
4. 运行 Filebench 和 LevelDB/YCSB；
5. 数据集已校验且 compatibility patch 的 edge-pass 计数正确时运行 GridGraph；
6. 最后运行写放大最大的 Fig.19；
7. 运行绘图脚本，并从 manifest/CSV 重新计算所有汇总值。

## 结果目录与可审计性

每次顶层运行创建不可复用的目录：

```text
benchmark-results/paper-reproduction-YYYYMMDD-HHMMSS/
  manifest.tsv
  commands.log
  raw/<version>/<figure>/<case>/run-N/
  csv/fig*.csv
  csv/trace_*.csv
  plots/*.png
  plots/*.pdf
  report.md
```

主微基准的 `manifest.tsv` 记录 git commit、source-like 工作区 hash、同步公平
补丁/工作区 hash、内核、CPU、NUMA、DAX、NVMe BDF/NSID/型号/序列号、初始及
最终驱动、SPDK/bpftrace 版本、交替顺序、prefill 边界、scale、seed、开始/结束
时间和完成状态。应用 runner 还记录对应二进制、包版本和 workload 参数；每行
CSV 同时携带版本 commit。每张图只读取其对应 CSV；绘图脚本拒绝混合
commit/scale，失败 run 保留在 CSV 和报告中但不作为 0 画入柱形。paper 规模
必须人工确认每 case 至少三次成功；smoke 允许一次并在图题和报告中标出。

## 当前解释边界

- 这套图回答“相同论文负载下，旧同步架构与当前异步架构如何变化”，不回答
  “当前版本是否复现论文相对九种基线 FS 的绝对倍数”。
- 同步版用 kernel eBPF，异步版用源码 trace；二者先统一成语义阶段再比较，
  原始 event 不应直接按名称相减。
- 当前异步读不使用 Linux page cache。Fig.13 若没有逐步升速是预期现象，不能
  据此声称采集失败。
- Fig.15、Fig.18、Fig.19 最受硬件/数据集/运行时长约束；未完成项保留明确的
  `not-run`/`unsupported` 状态，不填 0，不沿用旧结果，也不从论文图反推数字。
- 2026-07-22 的实测 smoke 结果、失败边界和图目录见
  [reproduction-results-20260722.md](reproduction-results-20260722.md)。它不是
  paper 规模验收结果。
