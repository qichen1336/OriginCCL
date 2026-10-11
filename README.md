# OriginCCL

C++ 集合通信库。

支持 AllReduce、Broadcast、Reduce、AllGather、ReduceScatter，以及同步点对点 Send/Recv。
接口参数和缓冲区布局见 [Communicator 层文档](docs/layers/communicator.md)。四种 executor 均通过统一的三阶段接口驱动集合操作。

## 批量下发（Group API）

`GroupStart()` / `GroupEnd()` 之间的通信调用只入队，最外层 `GroupEnd()` 排序后统一规划并执行到完成，返回是否全部成功。单独调用等价于只有一个任务的组，走同一条路径：

```cpp
comm.GroupStart();
comm.AllReduce(a, a_out, n, DataType::FLOAT32, ReduceOp::SUM);
comm.Send(buf, n, DataType::FLOAT32, peer);
comm.Recv(recv, n, DataType::FLOAT32, peer);
bool ok = comm.GroupEnd();
```

- 组内集合按 `(操作类型, 字节量, dtype, 有效 op, 有效 root)` 稳定升序排列，P2P 一律排在集合之后。
- P2P 按轮次 `i = 1..world_size-1` 执行：第 i 轮向 `(rank-i+world_size)%world_size` 发送、从 `(rank+i)%world_size` 接收；发送任务进 channel 0、接收任务进 channel 1，两方向并发推进。同有向 rank 对保持 FIFO。
- 支持嵌套：内层 `GroupEnd` 不执行，深度归零的最外层才执行。组内操作彼此独立（可共享只读输入、允许各自原有合法 in-place），不支持前一操作的输出作为同组后一操作的输入；这类依赖用 Group 边界表达。
- 需要至少两个 channel（发送/接收各一个）。参数错误沿用 `LOG_ERROR` + `false`，配对与跨组消息匹配由调用方保证。

## 构建

```bash
cmake -S . -B build
cmake --build build -j"$(nproc)"
```

## 运行测试

多进程启动只支持 **Open MPI** 的 `mpirun`。rank / world size 都由 MPI 给出，bootstrap 地址也不再从环境变量读：rank0 调 `Communicator::GetUniqueId` 挑一个空闲端口、连同自己的 IP 生成 `UniqueId`，再用 `MPI_Bcast` 发给其余 rank，所以测试必须链接并启动 MPI（不依赖 MPICH、PMI 或 Slurm）。

测试入口只有一个脚本，`-h` 有完整说明：

```bash
scripts/run_tests.sh -h
# 本机可跑的最小档（单机 4 rank、模拟多机 4×1、四种传输、集合用例）：
scripts/run_tests.sh --level 0 -j 2 --oversubscribe
```

等级决定进程布局与数据量。默认构建带 `-DOCCL_SMALL_TESTS=ON`，chunk 颗粒度与集合用例 count **等比缩小 256 倍**，让低配虚拟机也能跑完整矩阵；下表的 count 是生产值（`--full-data` 的取值，也是物理机验证使用的值）。

| 等级 | 单机 | 模拟多机 | 每 rank 的 count | 组合强度 |
| --- | --- | --- | --- | --- |
| 0 | 4 rank | 4 机 × 1 rank | 32768、131072 | 每个接口遍历全部 dtype / op / count |
| 1 | 8 rank | 4 机 × 2 rank | 65536、262144 | 补齐 dtype/op/count 两两组合 |
| 2 | 32 rank | 8 机 × 4 rank | 262144、1048576 | 完整合法核心集 |

```bash
# 需要足够核数，或加 --oversubscribe：
scripts/run_tests.sh --level 1 -j 2 --oversubscribe
scripts/run_tests.sh --level 2 -j 2 --oversubscribe
```

- **低配机器**：`run_tests.sh` 默认按小数据构建/运行（`-DOCCL_SMALL_TESTS=ON`，chunk 32 KiB→128 B，count 同时除 256），等价地覆盖同一批 planner 路径。物理机验证时加 `--full-data` 用生产规模：

```bash
scripts/run_tests.sh --level 0 -j 2 --oversubscribe --full-data
```

- 传输套件（`test_transport_*`）**不缩放**：其 5 MiB 边界与 8 MiB stall 必须超过 SHM 2 MiB / RDMA 1 MiB 环容量才能验证背压。除 TCP / SHM / RDMA 外还有零拷贝 RDMA 档位（`test_transport_rdma_zc`）：阈值 16 MiB 以上注册用户缓冲为 MR、用独立 RC QP 直接 SEND/RECV，以下完全走基类环形缓冲。
- 只覆盖 **polling** 与 **epoll** 两个 executor，且不再由构建选择：库在 `Init` 时比较全机在线核数与本地 rank 数——每 rank 已绑核独占一核，核数 ≥ local rank 数用 `polling`（低延迟忙等），否则用 `epoll`（不忙等抢 CPU）。`multi_thread` / `reactor` 仍编译在库里，但已无选取路径，因此一次构建就覆盖全矩阵。
- **多机是单机模拟**：通过 `CommConfig::get_hostname` 注入逻辑 hostname，验证分组与传输选择，不代表真实跨主机。
- `OCCL_DISABLE_SHM` / `OCCL_DISABLE_RDMA` 不作矩阵维度；集合用例按自动选择走 SHM / RDMA / TCP，测试只做黑盒接口断言，不检查实际路径。
- 无可用 RDMA 设备时 RDMA 与零拷贝 RDMA 档位记为 **SKIP**，不算通过；退出码 `0` 全过 / `1` 失败或超时 / `2` 只剩 SKIP / `4` 参数非法。
- **没有 RDMA 硬件时**，可用软件 RoCE 让 RDMA 与零拷贝档位真正跑起来——而不再只是 SKIP：

```bash
sudo scripts/setup_softroce.sh          # 交互式，逐阶段确认
scripts/setup_softroce.sh --status      # 查看当前状态，不需要 root
sudo scripts/setup_softroce.sh --down   # 拆除
```

  它加载 `ib_uverbs` / `rdma_cm` / `rdma_ucm` / `rdma_rxe`，建一个私有 dummy 网卡（`occl-rxe0`，`10.99.0.1/24`）并把 `rxe0` 挂上去，然后用 `ibv_rc_pingpong` 预检同机回环。**只建一个 rxe 设备**是刻意的：内核按 `skb->dev` 找设备，两个本地地址互发时 `skb->dev` 是 `lo`、报文会被丢掉；单设备时 `rxe_prepare()` 因目的 MAC 等于本机 MAC 而走 `rxe_loopback()` 直投，两个 rank 共用同一设备即可互通，这也正是库「取第一个 active 设备」的 `Probe()` 所期望的。装好后集合用例会自动选 RDMA，覆盖面比 SKIP 时更大。

查看某一等级的完整用例矩阵（需先构建一次）：

```bash
scripts/run_tests.sh --level 0 --list-cases --no-build
```

`tests/regression/` 下七个功能回归测试入口：`test_single_machine`、`test_multi_machine`、`test_transport_tcp`、`test_transport_shm`、`test_transport_rdma`、`test_transport_rdma_zc`、`test_p2p`。用例生成与结果校验在 `tests/test_common.*`，传输套件共用 `tests/transport_check.*`。报告落在 `test-reports/`（`summary.txt` 汇总）。三个性能基准在 `tests/benchmark/`，不属回归矩阵，手动 `mpirun` 运行（二进制在 `build/tests/benchmark/`）。

P2P 使用 `Send(buffer, count, dtype, peer)` / `Recv(buffer, count, dtype, peer)`，双方按顺序配对，不支持 tag、自发自收或同 communicator 并发调用。发送连接固定在 channel 0、接收连接固定在 channel 1，按需建立并复用，整段传输；同机优先 SHM，跨机必须 RDMA_ZC，无设备时非空操作失败，不回退 TCP。完整契约见 [Communicator 层文档](docs/layers/communicator.md)。

`scripts/run_tests.sh --suite p2p --oversubscribe` 固定用四 rank，覆盖同机和模拟跨机、连接缓存与隔离、乱序接入、与集合操作交替、polling/epoll 及最大 48 MiB 数据。P2P 数据量不缩放；无 RDMA 时验证拒绝回退，再将跨机数据传输记为 SKIP。

集合用例的 payload 会逐元素精确比对，传输用例覆盖边界尺寸与就绪活性；这些比对无法被任何单一动态检查替代，因此始终是必跑项。

## 性能基准结果（阿里云 ecs.g8y.8xlarge，2 节点，eRDMA，2026-10）

环境：两台 `ecs.g8y.8xlarge`（aarch64，32 核），跨机 eRDMA（iWARP，`erdma_0` PORT_ACTIVE）。
perftest 实测链路：单向 ~2.9 GB/s、双向合计 ~5.6–5.8 GB/s（每方向 ~2.9 GB/s）。
三个性能程序（源码在 `tests/benchmark/`，二进制在 `build/tests/benchmark/`）都用 `mpirun -np 2 --host <ip1>:1,<ip2>:1` 跨机启动。

### `test_rdma_zc_benchmark`（双向聚合带宽，`2 × bytes / elapsed`）

每条消息各建两条单向连接（一收一发），单线程内 `if` 交错推进两方向；连接建立后 `SetWaitMode(Polling)`，
与忙轮询驱动对齐（否则每个 `Try*` 都付一次 EventDriven 的 `poll(fd,0)` syscall，吞吐掉到 ~3.5 GB/s）。
表格 `GB/s` 是双向聚合值，折合每方向 ~3 GB/s，与 perftest 单向一致：

| size | copy | ZC |
| --- | --- | --- |
| 32 KiB | 5.59 | 1.68 |
| 128 KiB | 5.96 | 5.19 |
| 512 KiB | 5.54 | 6.07 |
| 2 MiB | 5.47 | 6.02 |
| 8 MiB | 5.30 | 6.02 |
| 32 MiB | 5.64 | 6.06 |

ZC 小尺寸（<512 KiB）慢是延迟主导：signaled SEND 的完成要等对端 ACK，每条消息含一个完整 RTT，与循环结构无关。

### `test_allreduce_benchmark`（N=2 时 `busbw == algbw`）

`busbw = algbw × 2(N-1)/N`，N=2 时系数 = 1，即**每方向在网速率**。每档的计时窗按 `--target-bytes`（默认 4 GiB）反推调用次数 `calls = min(target_bytes / bytes, 16384)`，保证每档搬够数据量、避免小尺寸的突发假象（云主机突发带宽不稳）。默认单档 256 MiB：

```
bytes=268435456  count=67108864  busGB/s=3.19  PASS
```

跨机 eRDMA（occ209↔occ11，N=2）尺寸阶梯（target 4 GiB，稳态）：1 MiB 3.28 / 4 MiB 3.19 / 16 MiB 3.22 / 64 MiB 3.19 / 256 MiB 3.19 GB/s，全档打平每方向 ~2.9 GB/s 的链路可持续上限。

### `test_allreduce_ompi_benchmark`（OriginCCL 的 OpenMPI 公平对照）

复用与 `test_allreduce_benchmark` **逐行相同**的 harness（选项、尺寸阶梯、输入值、逐元素独立 oracle、`MPI_Wtime` 计时口径、`busbw` 公式与表格列）。两者由同一个 `tests/benchmark/test_allreduce_benchmark.cpp` 编译两次得到，唯一差别是 `OCCL_BENCH_OMPI` 宏切换 `BenchAllReduce` 的实现——OriginCCL 后端调 `comm.AllReduce(...)`，OpenMPI 后端调阻塞的 `MPI_Allreduce(..., MPI_FLOAT, MPI_SUM, MPI_COMM_WORLD)` out-of-place；宏同时切换初始化（OriginCCL 建 communicator，MPI 用 `Utils::PinProcessToCpu(local_rank)` 绑到与 OriginCCL 相同的本机核）与元数据标签，其余完全一致，因此两边可逐档对读。

`--channels` 为命令行兼容而被接受并忽略（MPI 后端无 channel 概念），其余选项默认值与 `test_allreduce_benchmark` 一致：

```bash
mpirun -np 2 --host <ip1>:1,<ip2>:1 build/tests/benchmark/test_allreduce_ompi_benchmark
```

两侧表格同为 `bytes / count / min_us / median_us / max_us / algGB/s / busGB/s / check`，直接并排比较即可。

**注意传输层不对等**：本仓库验证过的 eRDMA(iWARP) 网卡上，OpenMPI 4.1.2 **无法**驱动该设备——`openib` BTL 只编了 `udcm`（iWARP 需要 `rdmacm`）直接禁用端口，`ucx` PML 初始化失败——因此 OMPI 回退到 `tcp` BTL，而 OriginCCL 直发 `erdma_0`。同机 eRDMA 实测（N=2，target 4 GiB）：OriginCCL RDMA 全档 ~3.19 GB/s，OMPI TCP 1.48–2.14 GB/s。要严格对等，可用 `OCCL_DISABLE_RDMA=1` 把 OriginCCL 也压到 TCP：256 MiB 时 OriginCCL TCP 2.28 GB/s vs OMPI TCP 1.52 GB/s（同传输层下 OriginCCL 仍快 ~1.5×）。


## 案例：copy 路径双向吞吐「一个方向被饿死」的定位（2026-10-11，已坐实）

### 现象

`test_rdma_zc_benchmark` 双向跑时，`rdmatop` 观察到 copy 路径的 TX / RX 不是同时打满，而是一侧 ~26 Gb/s、另一侧 ~1.7 Gb/s，持续数秒后**翻面**；整体停在 ~3.3 GB/s（半双工）。ZC 路径无此现象：两向各 ~26 Gb/s，满双工 ~6.4 GB/s。用户原问「为什么一个方向的吞吐被饿死、另一个方向起不来」，本节记录完整的定位链。

### 前因：copy 的发送被 credit 门控，不是 CPU

copy 路径发送由 credit 窗口门控：`TrySend` 里 `limit = CreditsReceived() + kRdmaSlotCount`，`CreditsReceived()` 读的是本端注册内存里一个 8 字节计数器——对端用一条 8 字节 signaled RDMA_WRITE 直接改写它。发送端的命根子就是这一个数：它涨就发得动，它停住则 `available=0`、一条 WRITE 都发不出，只能空转。因此「给再多 CPU 轮询也没用」——瓶颈是 credit 回不来，不是缺时间片。ZC 用 SEND/RECV、无 credit 环，所以免疫。

### 决定性实验：`credit_probe` 逐拍时间戳（`tests/benchmark/credit_probe.cpp`）

子类化 `TransportRDMA`（不改传输层），在两处挂钩：① `HandleCompletion` 识别 credit 帧的 CQE（credit 用 `wr_id=0`，数据帧 `wr_id=slot+1`，天然可区分），测 credit 帧 `ibv_post_send → CQE`（signaled WRITE，CQE 在对端 ACK 后产生）的完整往返；② `TrySend` 覆盖测发送端「credit 耗尽到重新到账」的 stall 时长。**同一条连接先跑单向（反向线空闲）再跑双向**，唯一变量是「有没有反向满速流量」。

结果（两台 eRDMA，12 GB/方向/阶段，两次运行完全一致）：

| credit 帧往返 | 单向 | 双向 rank0 | 双向 rank1 |
| --- | --- | --- | --- |
| mean | 37.9 µs | **360.2 µs** | 38.7 µs |
| p50 | 37.7 µs | 38.0 µs | 38.2 µs |
| p99 | 50.9 µs | **2951 µs** | 52.7 µs |
| max | 636 µs | **7020 µs** | 592 µs |

铁证：双向下 rank0 回传的 credit 帧往返尾部炸到 **p99≈3 ms、max≈7 ms（≈200× 基线）**，rank1 的 credit 往返干净如单向。credit 帧的「post→CQE」往返确实被拖到毫秒级，且**只发生在占优方向**。

### 机制（精确定位）

credit 帧不是被**对向**数据堵，是被**自己同方向 egress 上满速的 outgoing 数据**堵：每个 rank 的 NIC 物理 egress 同时承载两样东西——①本方向 outgoing 数据、②本方向回传的 credit 帧（receiver→sender 走同一条物理线）。占优方满速发数据 → 它的 egress 满载 → 它回传的 credit 帧排在数据后 → 对端等 credit → 对端被饿 → 对端 egress 空出来 → 对端 credit 畅通 → 占优方保持满速。**双向正反馈自增强**；p50 快 / p99 慢 的不对称正是「占优期 credit 被堵 / 让出线路期 credit 畅通」的翻面形态。

### 为什么 `rdma_write_lat.cpp` 复现不出

它是单向单条、对端 rank1 只 park 保活 QP 不回任何软件，`RDMA_WRITE` 的 ACK 由 NIC 硬件自动回、不经过 credit 反馈环，且 4 KiB / 17.6 µs ≈ 0.23 GB/s 远低于满速，无法占道也无对向流量。真正暴露 3.4× 的是双向软件往返的 `rdma_pingpong.cpp`。

### 附带：512 槽死锁的根因

`TrySend` 里 `send_seq += count` 在 `ibv_post_send` 成功**立即**加，而 credit 是接收端**消费后**才回的真实计数。当窗口 `kRdmaSlotCount` 512 > recv pool 64 时，`WRITE_WITH_IMM` 超出可匹配 WQE → RNR NACK → 该 WRITE 不写数据、不产生 IMM CQE、也不为它回 credit，但 `send_seq` 已虚增 → `send_seq - credits = 512`（窗口打满）→ 停发；接收端环空等数据。**双方都等对方 → 永久死锁**。32 槽（≤64）不死锁；128 槽虚增有限、credit 追平 → 慢但推进。不变量：**`kRdmaSlotCount ≤ kRdmaRecvPool`**。
