# Communicator 层（`include/communicator.h`、`src/communicator.cpp`、`src/bootstrap.cpp`）

进程内集合通信的顶层对象：初始化建连 + 发起集合操作 + 生命周期管理。改动初始化/建连/生命周期前阅读本文件。

# 核心职责边界

- 初始化链路：`TopologyRing` 填 ring → 铺 channel 骨架（id + send/recv 槽位）→ 建三类 listener（数据面 TCP + 共享内存 rendezvous + RDMA）→ Bootstrap 交换 `NodeInfo`（含 `rdma_addr`/`rdma_port`）→ local 分组 + 按 `local_rank` 绑核 → 全局判定所有 rank 都有可用 RDMA 才启用 RDMA → `InitChannels` 逐 channel 建独立 send/recv transport（本机 edge 走共享内存，跨机 edge 走 RDMA 或 TCP）。
- 执行链路：五个 collective 公开方法创建 `CollTask` → `planner.Plan` → `executor->Run(plan)`，不在入口展开算法。planner 组装 task 时经 `Topology::FillTransports` 把该 channel 的连接（向量）挂到 `PlanTask`；communicator 不碰单条 transport 的选取。
- 暴露本机视角：`GetLocalRank()` / `GetLocalSize()` / `GetLocalRanks()` / `IsSingleMachine()`。
- 不负责：不决定算法（topology）、不决定等待策略（executor）、不切片（planner）、不实现共享内存环（transport）。

# 文件介绍

| 文件 | 职责 |
|------|------|
| `include/communicator.h` | `Communicator` 顶层接口（GetUniqueId / Init / 五种 collective / Finalize / local 视图） |
| `include/channel.h` | `Channel` / `Connector` / `Ring`：`send[p]` / `recv[p]` 是两条有向边的两个槽位；骨架（id + 各槽位 peer/channel_id/is_send）由 communicator 在 `Init` 铺，`ring` 由 topology 填，transport 由 `InitChannels` 装 |
| `src/communicator.cpp` | `GetUniqueId` + `Init` + `#ifdef` 构造 executor + `ConnectActiveEdges` 内联按边三选一（SHM/RDMA/TCP）+ `Finalize` |
| `src/bootstrap.cpp` | master/worker 交换 `NodeInfo`（hostname 与 RDMA 端点） |

# 实现原理

- **unique id**：`GetUniqueId` 只 `CreateListenSocket(0)` 绑空闲端口（fd 记在 `bootstrap_listen_fd`，持到 `Finalize`），把 IP + 端口装进定长 `UniqueId` 返回，不做通信；分发给其余 rank 是启动方责任（`tests/` 用 `MPI_Bcast` 播原始字节）。ip/port 都从 `config.unique_id` 读。
- **listener 在 bootstrap 前绑好并持有**：bootstrap listener 就是 `GetUniqueId` 绑的那个 fd，`Bootstrap::RunMaster` 只借用不关闭；数据面 TCP listener 在 bootstrap 前 `Listen("", 0)` 建好（`NodeInfo.data_port` 就是它），channel init 完才 `Close()`；RDMA listener 同理，全局不用 RDMA 随即关闭；共享内存 listener 在 bootstrap 前 `Listen(<path>, 0)` 绑 `/tmp/originccl/<port>-<rank>.sock`。
- **连边方向**：每 channel 连 prev/next 两条边，按 `rank < peer` 决定主动 connect、否则被动 accept（避免启动死锁）。
- **主动边 5 次重试**：`ConnectActiveEdges` 每条边最多试 `kConnectRetryCount`（5）次、间隔 `kConnectRetryIntervalMs`（100ms），每次失败先 `transport->Close()` 复位再重试，耗尽才初始化失败。
- **传输选择**：本机 edge（`peer hostname == 本机 hostname`）用共享内存，仅 `OCCL_DISABLE_SHM` 恰好等于 `"1"` 时禁用；网络 edge 是全局决策——所有 rank 都 `rdma_port != 0` 才统一用 RDMA，否则统一 TCP（`OCCL_DISABLE_RDMA=1` 等价本 rank 宣布不支持）。被动端用同一规则（`handshake.rank` → `all_nodes[rank]`），两端判定必然一致。
- **集合接口语义**：

| 方法 | count 与缓冲区 |
|------|---------------|
| AllReduce(send, recv, count, dtype, op) | 每 rank 输入/输出 count 个元素，允许 send==recv |
| Broadcast(buffer, count, dtype, root) | 单 buffer；root 提供输入，每 rank 获得 count 个元素 |
| Reduce(send, recv, count, dtype, op, root) | 每 rank 输入 count，仅 root 输出；非 root recv 可 null |
| AllGather(send, recv, count, dtype) | 每 rank 输入 count，输出 N*count，按来源 rank 排列 |
| ReduceScatter(send, recv, count, dtype, op) | 每 rank 输入 N*count，归约后每 rank 获得目标块 count 个元素 |

- count=0 是成功空操作，允许空缓冲区；单 rank 直接本地完成。归约支持 SUM/MAX/MIN/AVG，AVG 先求和再对最终输出除以 N。所有 rank 必须按相同顺序调用相同操作，同一 communicator 调用不得并发。

# 隐含约定

- **unique id 只由 rank0 生成**，其余 rank 从外部拿到同一份 id 调 `Init`；不做「非 rank0」校验，同一对象重复调用 `GetUniqueId` 明确报错。
- **listener 都在 bootstrap 前绑好并持有**，id 公布出去的端口不可能在首次 accept 前被抢走——不能退回「先取空闲端口 → 关闭 → 重绑」。
- **连边方向 `rank < peer` 勿回退**（改方向重引入启动死锁）；传输选择不影响方向判定。
- **channel 边是有向的，每条边一个独立 transport**：`send[p]` 只发送、`recv[p]` 只接收，是两条有向边、两个 `Connector`、两个 fd。2 rank 时 `prev == next`，同一对 rank 仍是两条独立边、两个 transport，不因 peer 相同而合并。
- **主动边有 5 次连接重试**（非无重试）；listener 先于 bootstrap 绑好，重试是防御性的。
- **共享内存失败即初始化失败**，无自动回退 TCP；RDMA 同理，全局选定后建链/QP/MR 错误直接 `LOG_ERROR` + `false`，不静默改走 TCP（`OCCL_DISABLE_RDMA=1` 是显式选择而非回退）。
- **机器身份 = hostname**（`Utils::GetHostname()` → `gethostname()`），不是 IP。
- **`Finalize` 先 `executor->Shutdown()`（多线程需 join worker），再关 channel transport**。`PlanTask` 经 `shared_ptr<Transport>` 保证 transport 在 plan 执行期间有效。
- **绑核用 `local_rank`（非全局 rank），非致命**：失败只 `LOG_WARN` 并继续初始化，不能让 `Init` 失败。
- 改动传输选择后跑 `scripts/run_tests.sh`：`test_multi_machine` 用 `CommConfig::get_hostname` 注入逻辑 hostname（`rank / ranks_per_machine` 推导机器号），断言 local 视角与**每条环边**（`ring.prev`/`ring.next`）的具体传输类型——同机必为 SHM、跨机必为统一的 RDMA 或 TCP，且 send/recv 两条边的 fd 独立；`test_single_machine` 断言真实单机的每条边都是 SHM。等级决定布局（0/1/2 = 4×1 / 4×2 / 8×4），`OCCL_DISABLE_SHM`/`OCCL_DISABLE_RDMA` 不再是测试维度。
