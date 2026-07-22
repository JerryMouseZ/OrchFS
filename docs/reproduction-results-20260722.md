# 2026-07-22 FAST'25 负载差分复现结果

本页记录一次可审计的 `smoke` 复现：只比较原始同步 OrchFS 的公平修正版
`sync-fair` 与当前协程/SPDK 版 `async-current`。负载形态取自 FAST'25 论文，
但数据规模缩小、每个普通 case 只跑一次，所以这些数值用于暴露语义、稳定性和
主要开销，不能替代论文规模实验，也不能与论文中的其他文件系统柱形直接比较。

完整口径、构建方法和重跑命令见
[paper-reproduction.md](paper-reproduction.md)。本页引用的结果目录均保留原始
日志、CSV、manifest 以及重新绘制的 PNG/PDF。

## 版本与机器

| 项目 | 实测值 |
| --- | --- |
| async-current | `87db833feecc62ffd0954a502252716c5beda79e` 加结果 manifest 中记录的工作区改动 |
| sync-fair | `288fbfc4250966829d06aa1d3c02c92b9e806162` 加 `scripts/reproduction/sync-fair.patch` |
| CPU / kernel | 2 x Intel Xeon Gold 5318Y，96 个逻辑 CPU；Linux `7.0.0-rc2` |
| NVM | `/dev/dax0.1`，约 126 GiB devdax |
| SSD | `0000:b2:00.0`，NSID 1，Samsung `MZPLJ3T2HBJR-00007`，序列号 `S55HNC0W100205` |
| 异步 SSD 路径 | SPDK v26.01；运行前后按 BDF 切换并恢复驱动 |
| 随机种子 | `20260722` |
| LevelDB / Filebench | 系统 LevelDB ABI 1.23；仓库 Filebench 1.5-alpha3 fork |

同步 SSD fd 使用 `O_DIRECT`，异步 SSD 使用 SPDK，因此公平批次两边都不经过
Linux block page cache。每个微基准 case 单独格式化；prefill 完成后退出客户端和
KFS，再启动新进程测量，避免把旧版或新版的进程内 extent/metadata 状态混入计时。
两版按 case 交替先后顺序；Filebench 按 workload/thread、LevelDB 按 phase 做同样
的交替。微基准、Filebench 和 LevelDB 仍是单次缩小规模；GridGraph 使用完整
LiveJournal 但也只有一次尝试，因此应用结果只能看数量级和失败边界。

## 完成状态

| 论文项目 | 本次状态 | 结果 |
| --- | --- | --- |
| Fig.1--3、5、10--15 | attempted | 公平重启边界的完整 smoke case 矩阵；成功和失败均保留 |
| Fig.4 / 11a | completed-smoke | 3 次 trace-off/on；同步 eBPF、异步源码 span |
| Fig.6--9 | not-applicable | 论文中的架构/算法图，没有可运行实验 |
| Table 2 | partial | async 的 8 个 fresh-prefill bitmap 快照成功；sync 没有可信在线计数接口 |
| Fig.16 | partial | Filebench 9/12 成功；async 的三个 case 因写 EIO 无有效结果 |
| Fig.17 | blocked | sync 的 Load/A--F 成功；async 全部在 LevelDB `DB::Open` 失败 |
| Fig.18 | blocked | 已取得并校验完整 LiveJournal，按 8 GiB/20 iterations 尝试；sync staging 超时，async 重启后 21 个 edge pass 全部实际读取 0 byte，因此没有有效对比值 |
| Fig.19 | blocked | 8/8 严格 byte-unaligned case 在首个 256 KiB 检查点写 EIO |

这里的 `attempted` 不等于论文规模复现成功。Fig.1--3、5、10--15 的主批次只有
1 次、4--16 MiB 级别；Fig.13 为 5 秒；Filebench 为 5 秒；LevelDB 每 phase
1000 次操作；Fig.19 的文件为 4 MiB。Fig.18 使用完整 LiveJournal 和论文的
20 iterations/8 GiB 参数，但没有得到两版都有效的样本，也没有 3 次重复。

## 公平微基准

公平主批次位于
`benchmark-results/paper-reproduction-20260722-165618/`。它取代更早的
同进程 prefill/measurement 试跑；早期试跑只能用于说明进程生命周期状态会显著
影响结果，不能作为本页的同步/异步性能结论。

主批次共 108 个样本：sync 51/54 成功，async 28/54 成功，总计 79 成功、
29 失败。下面列出有代表性的同 case 吞吐；完整 28 组配对值和 29 条失败记录
在该目录的 `report.md` 与 `csv/results.csv`：

| Figure / case | sync MiB/s | async MiB/s | async/sync |
| --- | ---: | ---: | ---: |
| Fig.2 sequential 1 MiB, fsync each | 982.899 | 296.500 | 0.302x |
| Fig.3 aligned 4 KiB | 599.358 | 1.255 | 0.002x |
| Fig.3 aligned 64 KiB | 626.908 | 18.695 | 0.030x |
| Fig.3 aligned 1 MiB | 693.594 | 191.080 | 0.275x |
| Fig.5 single 256 KiB | 839.052 | 81.681 | 0.097x |
| Fig.5 single 1 MiB | 819.145 | 190.786 | 0.233x |
| Fig.5 1 MiB split into 32 KiB requests | 728.834 | 165.627 | 0.227x |
| Fig.11 1 worker, 1 MiB | 665.501 | 80.029 | 0.120x |
| Fig.11 16 workers, 1 MiB | 645.764 | 286.322 | 0.443x |
| Fig.12 1 worker, 256 KiB | 861.812 | 20.645 | 0.024x |
| Fig.12 8 workers, 256 KiB | 684.070 | 81.630 | 0.119x |
| Fig.12 16 workers, 256 KiB | 765.223 | 81.758 | 0.107x |
| Fig.14 multi-file 4 KiB, 1 thread | 693.781 | 1.248 | 0.002x |
| Fig.14 multi-file 4 KiB, 16 threads | 668.398 | 18.323 | 0.027x |
| Fig.15 append uniform 1 B--2 MiB, 1 thread | 161.037 | 45.492 | 0.282x |

在成功路径中，大请求和更多 worker 能明显摊薄 async 的固定开销，例如 Fig.11
的 1 MiB 从 1 worker 的 80.0 MiB/s 增至 16 workers 的 286.3 MiB/s；但 8 到
48 workers 的 256 KiB 结果都停在约 77--82 MiB/s，继续加 worker 没有收益。
这与后述 client 外等待占 RTT 主导的 profile 一致。Fig.12 的 64 worker case
因可用 SPDK qpair ID 不足而无法启动。

Fig.13 只有 pm10-4 KiB 两版都完成；其 16 MiB smoke 工作集没有配套的逐请求
介质归因，不能当成 SSD 随机读上限。另三个 sync measurement 在进程重启后超过 60 秒超时，
对应 async 则在 prefill 的任意长度写上先返回 EIO，因此本次没有可比较的
Fig.13 大请求时间序列。

变量长度或显式非对齐 case 的失败不是 0 吞吐。CSV 保留 `failed` 行，绘图时
不画柱形。当前 async 对 4 KiB 对齐边界以内的任意用户指针/长度支持不完整：
大请求日志明确出现 SPDK `virt_addr ... not dword aligned`，其他小请求也可能以
`EIO` 返回。64 个 KFS worker 的 async case 则在 SPDK qpair 资源不足时启动失败。

## Fig.4 profiling（CPU 冲突修复前）

profiling 批次位于
`benchmark-results/paper-reproduction-20260722-154159/`，每种模式重复 3 次：

| 模式 | sync MiB/s | async MiB/s | async/sync |
| --- | ---: | ---: | ---: |
| trace/profile off | 814.860 | 20.447 | 0.025x |
| eBPF/source trace on | 756.368 | 20.412 | 0.027x |

这个旧批次中异步源码 trace 的表观吞吐扰动是 `0.17%`；同步 eBPF 扰动约
`7.18%`。后续发现 async 的 5--8 ms 调度等待淹没了记录器成本，因此这里的
0.17% 不能作为修复后 trace 扰动的依据。

异步 trace 每次记录 132 个 client round trip。3 次合计 396 个请求的平均值为：

| span | mean |
| --- | ---: |
| client round trip | 1607.50 us |
| server dispatch | 123.80 us |
| client RTT 中 server dispatch 以外部分 | 1483.70 us |
| core write | 52.72 us |
| core sync | 39.49 us |
| device/SPDK write | 44.45 / 43.31 us |
| device flush | 31.10 us |

平均约 `92.3%` 的 client RTT 位于 server dispatch 之外；396 个请求中 108 个
`outside_server_us > 1 ms`，表现为周期性约 5--8 ms 的长等待。进一步按请求序号
和确定性 round-robin 规则推断 lane 后，lane 0 的 `fsync` 平均为
`5667.83 us`，lane 2 只有 `48.35 us`。
当时 KFS worker 0 与唯一的 LibFS worker 都被 Runtime 固定到继承 affinity 的
第一个逻辑 CPU；lane 0 在两端也恰好都归 worker 0。两端对 active lane 都返回
`PollState::busy` 并持续轮询，因而在同一 CFS runqueue 上按毫秒时间片交替。
这不是 NVMe/SPDK RTT，也不是总体 worker 数不足或普通意义的负载不均衡。
阶段 span 会重叠，不能把表中均值直接相加成一次请求的 100% 堆叠。

同一批 trace 每次约记录 707 次 NVM write 和 64 次 SPDK write，且没有丢记录，
所以当前 async 实现确实同时使用 NVM 和 SSD；它不是“只走 SPDK”的版本。

同步 eBPF 观测到每次 64 个 `fsync`，总 syscall 时间为 68--123 us；目标 block
tracepoint 在该批次为 0。后一个 0 可能同时受实际分配路径、异步下发进程和
tracepoint/filter 语义影响，不能据此推断同步版没有 SSD I/O。

## LibFS/KFS CPU 冲突修复与复测

修复包含三层保护：Runtime 拒绝重复 CPU；KFS/LibFS 分别支持严格解析的
`ORCHFS_KFS_CPU_LIST` 与 `ORCHFS_CLIENT_CPU_LIST`；未显式配置时 KFS 从 affinity
低端取 CPU、单个 LibFS 从高端取 CPU。复现 runner 不照搬论文表，而是根据
实际 affinity 和 NVMe NUMA node 优先选择物理核，并为 client 与 KFS 生成互斥
列表。本机复测实际使用 client `1`、KFS
`3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33`，每行 CSV 和 commands log
都记录了该映射。

公平交替复测位于 `benchmark-results/affinity-fix-fair-20260722/`，每种模式和
版本各 3 次，仍是 smoke 规模：

| 模式 | sync MiB/s | async MiB/s | async/sync |
| --- | ---: | ---: | ---: |
| trace/profile off | 740.196 | 341.194 | 0.461x |
| eBPF/source trace on | 697.518 | 323.420 | 0.464x |

另用最终源码做了 3 次故意重叠 CPU 的因果负对照，位于
`benchmark-results/affinity-collision-control-20260722/`：client 与 KFS worker 0
同时绑定 CPU 3 时只有 `5.206 MiB/s`、平均延迟 `11996.08 us`；恢复互斥列表的
同源码批次为 `347.662 MiB/s`、`172.59 us`，分别改善 `66.78x` 和 `69.51x`。
这比跨工作区比较历史批次更直接地证明了修复对象就是同 CPU 忙轮询。

新的 396 个 trace 请求中，client RTT 均值为 `89.99 us`、server dispatch 为
`75.59 us`、二者差值仅 `14.40 us`；`outside_server_us > 1 ms` 从 108 个降为
0 个。仍有 6 个大于 1 ms 的 client RTT，全部时间已进入 server dispatch，
主要是每次短测开始时的 cold request；稳态 SHM 往返已不再出现原来的 CFS
时间片尖峰。

修复后 source trace 相对 trace-off 的扰动为 `5.21%`，没有通过 3% gate；采样
每 8 个事件一次仍为 `4.88%`，且固定周期会与阶段序列混叠。因此普通性能图只能
使用 trace-off 数据，当前 sample=1 trace 只用于归因，不能把 profile-on 绝对
吞吐当发布值。修复消除了主要异常，但 async 在这项 64 KiB、逐写 `fsync` 的
吞吐仍只有同步版约 46%，剩余差距是两次 RPC、POSIX blocking bridge、KFS
metadata/data plan 与持久化路径的真实固定开销，不应再归因于 lane CPU 冲突。

## Fig.16 Filebench

结果位于 `benchmark-results/filebench-reproduction-20260722-173145/`：

| workload | threads | sync Kops/s | async Kops/s | async/sync |
| --- | ---: | ---: | ---: | ---: |
| fileserver | 1 | 21.834 | 0.159 | 0.007x |
| fileserver | 16 | 61.061 | failed | - |
| webproxy | 1 | 2.478 | 0.124 | 0.050x |
| webproxy | 16 | 42.412 | 1.564 | 0.037x |
| varmail | 1 | 17.272 | failed | - |
| varmail | 16 | 133.808 | failed | - |

三个 async 失败 case 都由真实写 `EIO` 终止；server 日志可见 SPDK payload
buffer 非 dword 对齐。Filebench 是 metadata-heavy 混合负载，但不是隔离的
inode/dentry benchmark。在任意长度写正确性修好前，不能从这些总分反推
inode/dentry cache 的独立收益。

## Fig.17 LevelDB/YCSB 形态

结果位于 `benchmark-results/leveldb-reproduction-20260722-173705/`。驱动是
嵌入式 C++ 的 YCSB Load/A--F 操作比例，不是 Java YCSB；每个 phase 为
1000 records/1000 operations、1 KiB value、64 MiB write buffer、同步写。

sync 的 Load/A--F 吞吐依次为 `131480.3`、`384832.4`、`889735.9`、
`1197189.0`、`913820.3`、`370688.3`、`344425.1 ops/s`。async 七个 phase
全部在 `DB::Open` 失败，错误均为 `000002.dbtmp: File exists`。这说明当前
create/rename/temp-file POSIX 语义仍不满足 LevelDB；已加入的 `F_SETLK/F_SETLKW`
兼容 no-op 只在 runner 设置 `ORCHFS_REPRO_LEVELDB_LOCK_NOOP=1` 时启用，用来匹配
旧同步版的单进程行为。它没有解决该问题，也不提供真正的跨进程锁；普通 async
运行仍对这些命令返回 `EOPNOTSUPP`。将 no-op 改为环境变量门控并重新构建后，
`benchmark-results/leveldb-reproduction-20260722-174216/` 的 async Load 定向复测
仍得到相同 `000002.dbtmp: File exists`，确认失败点不是门控改动造成的。

## Table 2 / Fig.19

结果位于 `benchmark-results/fragmentation-reproduction-20260722-164627/`。
migration 已通过复现专用开关关闭，async KFS 在 `SIGUSR1` 安全点写 bitmap
计数。8 个 case 的 4 MiB 顺序 prefill 都得到 128 个 SSD 32 KiB block、0 个
新增 NVM Page/Upage。随后按严格随机字节偏移覆盖时，8/8 case 都在第一个
256 KiB 累计检查点返回 `pwrite EIO`，所以没有伪造空的 Fig.19 曲线。

这组 prefill 快照证明计数管道可用，但不构成论文 Table 2 的随机写最终占用；
Fig.19 必须先修复非对齐数据路径，再按 paper 档位写到真实 1 TiB 检查点。

## Fig.18 GridGraph

结果位于 `benchmark-results/gridgraph-reproduction-20260722-181605/`。使用公开
GridGraph 提交 `1f1a6262ef6ffbc61bcc229357e106843bd457ac`、校验过的 SNAP
LiveJournal（4,847,571 vertices、69,993,773 edges、551,950,184 edge bytes），
参数为 8 GiB memory、20 iterations。输入转换后的 binary SHA-256 为
`80199ecebb7ebdf3b4861748e009d16b1c5f93c35eba837a7ce37f94ada35f83`。

公开同步/异步 wrapper 都不能把 virtual fd 交给真实 `mmap`，因此两版共用同一
GridGraph compatibility patch：vertex vectors 使用匿名内存，edge files 仍从
OrchFS `pread`。在 host tmpfs 上先验证过同一二进制：degree 加 20 次迭代共
21 个 edge pass，每次读取 551,950,184 bytes；PageRank 部分为 5.10 s。这个 host
数值只用于验证应用和计数器，不是 OrchFS 比较基线。

本次两版都没有产生可画的有效柱：

- sync-fair 在 60 s staging gate 内只完成前 8 个逐字节校验文件，server 日志报
  `read data error! -1 31264 487358464`；更长尝试停在相同位置，因此没有
  进入 PageRank。
- async-current 在 24.13 s 内完成 21 个 grid 文件、共 1,655,850,844 bytes 的
  同进程逐字节读回校验；但重启 KFS/LibFS 后，应用虽打印 0.59 s，源码记录显示
  degree 加 20 次迭代的 21 个 edge pass 全部 `read_bytes=0`。该数值是无效的空跑，
  CSV 标为 `short-edge-stream`，绘图不把它当成性能结果。现象说明 staging 后的
  内容/映射在重启边界没有被应用重新读到；精确落点仍需进一步 trace 恢复路径。

因此没有继续下载 Twitter（11.5 GiB）和 Friendster（28.2 GiB）：LiveJournal
已经同时覆盖公开应用兼容、完整数据 staging、进程重启和 21-pass 校验，并暴露
两版各自的硬阻塞；扩大数据集只会重复同一失败，不能增加有效对比覆盖。

## page cache、缓存与下一步优化

旧版早期试跑确实受“prefill 后不重启进程”的状态影响，但不能笼统称为 Linux
page cache：公平版同步 SSD fd 已改为 `O_DIRECT`，async SPDK 也绕过内核 block
page cache。重启后约 20 MiB/s 的主要原因现已定位为 LibFS/KFS worker 0 的
确定性同 CPU 忙轮询冲突；分离 CPU 后同一 64 KiB 逐写 `fsync` 负载达到
341 MiB/s。因此旧的低值既不是 Linux page cache，也不是 SHM 介质本身的 RTT。

当前没有数据 page cache 会使“不要求持久化、重复读或热小文件”场景失去缓存
命中的上限优势；这是设计取舍，不能用持久化微基准替它下结论。应单独增加
`fsync=none` 的 cold/warm 两阶段读写实验，并报告工作集相对 DRAM 的大小。

按本次 evidence，优化顺序应为：

1. 修复 async 在 KFS/LibFS 重启后已 `fsync` 文件的 extent/内容恢复；GridGraph
   的同进程校验成功、重启后 21 次空读说明这是比吞吐更优先的持久化正确性问题。
2. 修复任意 POSIX buffer/offset/length 的 SPDK 对齐与 bounce/RMW 路径，并修复
   LevelDB 暴露的 create/rename/temp-file 语义；这些是 paper workload 的硬阻塞。
3. 保持 LibFS/KFS Runtime CPU 集互斥，并在多 client 部署中显式分配列表；当前
   runner 已修复且记录映射，但跨进程通用配置仍不能自动解决多个 client 互撞。
4. 合并可安全合并的 RPC 与持久化 fence，减少一次用户 I/O 的两次往返，
   同时保留 `fsync` 的真实持久化语义。
5. 再引入/完善 inode、dentry cache，并用隔离的 `open/stat/lookup/create/unlink`
   benchmark 测收益。目前仓库只有混合 Filebench 和功能/数据路径测试，没有
   已接入当前异步 runner、能单独归因的 metadata 性能测试；因此现在无法给出
   可信的百分比提升。对本次 64 KiB 写主瓶颈，它也不会消除 client 外等待。
6. 正确性与 lifecycle 稳定后，再跑 paper 档位、至少 3 次，报告均值/标准差，
   最后才讨论绝对吞吐验收。

## durable pwrite 策略与最后一轮优化

把异步版改成“成功的 `pwrite` 返回即持久化、`fsync` 只在本地校验 fd”后，分别
实测了 completion、FUA、每批 flush 三种 SSD 策略。控制器为 PM173X，显式
completion 能通过后端的 VWC 安全检查；若控制器报告存在 volatile write cache，
同一配置会以 `ENOTSUP` 拒绝启动。

64 KiB 单线程随机 overwrite、16 MiB prefill、64 MiB measurement、3 次的阶段性
结果如下（MiB/s）：

| async 策略 | sync-fair | async-current | async/sync |
| --- | ---: | ---: | ---: |
| completion | 1310.622 | 1335.372 | 1.019x |
| FUA | 1391.641 | 1341.390 | 0.964x |
| flush | 1298.100 | 770.046 | 0.593x |

completion 是该设备上的正确最优策略；FUA 没有提高 async 绝对吞吐，每批 flush
则显著增加持久化命令。为了减少 POSIX bridge 的固定开销，blocking positioned I/O
增加了由调用线程直接提交并轮询的 SHM lane，绕过 Client Runtime 的 Pending/
MPSC/唤醒路径；server positioned write 也用 active-write lease 代替每次 handle
lifecycle range lock。上述 completion 批次中 async p50 从先前的 53.627 us 降到
41.817 us，并在三次配对样本中均超过 sync。

但更长的 256 MiB、5 次复测没有保持这个小幅领先：sync 平均/中位数为
1466.686/1435.419 MiB/s，async 为 1405.325/1407.420 MiB/s，即均值 0.958x、
中位数 0.980x。4 线程、每线程 64 MiB 的约 1 MiB 多文件请求中，sync 为
4076.947 MiB/s，async 为 1566.738 MiB/s。为 blocking caller 分配稳定的多条 SHM
lane 已消除客户端单 lane 串行化，但 journal/core/device 路径仍不能达到同步版的
多线程吞吐。把相邻设备请求的合并上限从 256 KiB 提到 1 MiB 又使 async 降到
1457.821 MiB/s，故已回退。

最终源码候选又做了一次 64 MiB、3 次 completion 复测，KFS 日志三次均确认
`durability=completion volatile_write_cache=0`。sync/async 均值分别为
1226.472/1232.458 MiB/s（1.005x），中位数为 1182.100/1211.338 MiB/s
（1.025x）；但三组配对中仍有一组 async 较慢，且 sync 样本波动明显，所以这只能
说明当前短 case 总体持平，不能替代下面的长样本结论。

因此当前证据不能支持“async 在所有公平负载下必然高于 sync”。单线程 64 KiB 的
固定开销已经接近持平并出现过 1.019x，但长样本优势不稳；同步版是同进程调用，
而 async 的 durable blocking POSIX 路径仍必须支付一次 64 KiB 用户缓冲区到 SHM
的 copy、跨进程 dispatch 和 completion 往返。在不放宽 `pwrite` 持久化语义的
前提下，要获得稳定领先，下一步必须用真正的多 outstanding async API 端到端批处理
和 journal group commit，而不是继续把 `fsync` 或 NVMe completion 当作 no-op。

## 实现与校验

为这次复现新增了同一 POSIX workload generator、同步公平补丁、破坏性设备
identity guard、逐 case 格式化/交替 runner、eBPF 脚本、async trace、allocation
snapshot、Filebench/LevelDB/GridGraph/fragmentation runner、GridGraph 转换与
staging 读回校验，以及 CSV-to-PNG/PDF 绘图。普通与
trace 两个 SPDK build 都完整构建通过；修复后的普通 build CTest 为 9/9；全部 Python
runner 通过 `py_compile`，Filebench helper 通过 shell syntax 检查，同步补丁通过
reverse apply 检查。最后一次硬件运行结束后，目标 `0000:b2:00.0` 已恢复为
`vfio-pci`，没有残留 KFS、benchmark、Filebench、LevelDB 或 GridGraph 进程。

## 输出索引

| 内容 | 目录 |
| --- | --- |
| 公平 Fig.1--3、5、10--15 | `benchmark-results/paper-reproduction-20260722-165618/` |
| Fig.4 profile/trace | `benchmark-results/paper-reproduction-20260722-154159/` |
| CPU 冲突修复后的公平 Fig.4 A/B | `benchmark-results/affinity-fix-fair-20260722/` |
| 最终源码故意重现 CPU 冲突的负对照 | `benchmark-results/affinity-collision-control-20260722/` |
| Fig.16 Filebench | `benchmark-results/filebench-reproduction-20260722-173145/` |
| Fig.17 LevelDB/YCSB-shape | `benchmark-results/leveldb-reproduction-20260722-173705/` |
| Fig.18 GridGraph LiveJournal | `benchmark-results/gridgraph-reproduction-20260722-181605/` |
| Table 2 / Fig.19 | `benchmark-results/fragmentation-reproduction-20260722-164627/` |

所有绘图都从规范化 CSV 重新生成；failed/not-run/unsupported 不会当作 0 写入图。
