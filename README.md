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

`tests/regression/` 下七个功能回归测试入口：`test_single_machine`、`test_multi_machine`、`test_transport_tcp`、`test_transport_shm`、`test_transport_rdma`、`test_transport_rdma_zc`、`test_p2p`。用例生成与结果校验在 `tests/test_common.*`，传输套件共用 `tests/transport_check.*`。报告落在 `test-reports/`（`summary.txt` 汇总）。两个性能基准在 `tests/benchmark/`，不属回归矩阵，手动 `mpirun` 运行（二进制在 `build/tests/benchmark/`）。

P2P 使用 `Send(buffer, count, dtype, peer)` / `Recv(buffer, count, dtype, peer)`，双方按顺序配对，不支持 tag、自发自收或同 communicator 并发调用。发送连接固定在 channel 0、接收连接固定在 channel 1，按需建立并复用，整段传输；同机优先 SHM，跨机必须 RDMA_ZC，无设备时非空操作失败，不回退 TCP。完整契约见 [Communicator 层文档](docs/layers/communicator.md)。

`scripts/run_tests.sh --suite p2p --oversubscribe` 固定用四 rank，覆盖同机和模拟跨机、连接缓存与隔离、乱序接入、与集合操作交替、polling/epoll 及最大 48 MiB 数据。P2P 数据量不缩放；无 RDMA 时验证拒绝回退，再将跨机数据传输记为 SKIP。

集合用例的 payload 会逐元素精确比对，传输用例覆盖边界尺寸与就绪活性；这些比对无法被任何单一动态检查替代，因此始终是必跑项。

## 性能基准结果（阿里云 ecs.g8y.8xlarge，2 节点，eRDMA，2026-10）

环境：两台 `ecs.g8y.8xlarge`（aarch64，32 核），跨机 eRDMA（iWARP，`erdma_0` PORT_ACTIVE）。
perftest 实测链路：单向 ~2.9 GB/s、双向合计 ~5.6–5.8 GB/s（每方向 ~2.9 GB/s）。
两个性能程序（源码在 `tests/benchmark/`，二进制在 `build/tests/benchmark/`）都用 `mpirun -np 2 --host <ip1>:1,<ip2>:1` 跨机启动。

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

`busbw = algbw × 2(N-1)/N`，N=2 时系数 = 1，即**每方向在网速率**。默认 256 MiB / 100 次迭代：

```
bytes=268435456  count=67108864  busGB/s=2.92  PASS
```

2.92 GB/s 已打满每方向 ~2.9 GB/s 的链路可持续上限，与 perftest 单向一致。尺寸阶梯（短窗，偏高）：
1 MiB 3.56 / 4 MiB 6.17 / 16 MiB 7.08 / 64 MiB 5.95 / 128 MiB 3.70 / 256 MiB 2.92 GB/s。
