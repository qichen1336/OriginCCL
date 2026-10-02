# Communicator 层（`include/communicator.h`、`src/communicator.cpp`、`src/bootstrap.cpp`）

进程内集合通信的顶层对象：初始化建连 + 发起集合操作 + 生命周期管理。改动初始化/建连/生命周期前阅读本文件。

# 核心职责边界

- 初始化链路：同时构造 `TopologyRing` 与 `TopologyTree` → 铺 channel 骨架（id + send/recv 槽位）→ 建三类 listener（数据面 TCP + 共享内存 rendezvous + RDMA）→ Bootstrap 交换 `NodeInfo`（含 `rdma_addr`/`rdma_port`）→ local 分组 + 机器分组（写入成员）+ 按 `local_rank` 绑核 → 全局判定所有 rank 都有可用 RDMA 才启用 RDMA → 两个拓扑 `FillChannels(*this, channels)` → `InitChannels` 按 `FillPeers` 的边并集建 send/recv transport（本机 edge 走共享内存，跨机 edge 走 RDMA 或 TCP）。
- 执行链路：五个 collective 与 Send/Recv 公开方法构造 `CollTask` 后统一提交；非组内调用是单元素批次，直接走批量规划；`GroupStart()`/`GroupEnd()` 之间的调用只入队，最外层 `GroupEnd()` 统一执行。planner 负责排序与批量规划，入口不展开算法。P2P 固定由 `TopologyP2p` 执行，planner 按轮次建立其独立连接。
- 暴露本机视角：`GetLocalRank()` / `GetLocalSize()` / `GetLocalRanks()` / `IsSingleMachine()`；以及两个拓扑的访问器 `GetRingTopology()` / `GetTreeTopology()`（planner 用它二选一，见 [planner.md](planner.md)）。
- 不负责：不决定算法（topology）、不决定等待策略（executor）、不切片（planner）、不实现共享内存环（transport）。

# 文件介绍

| 文件 | 职责 |
|------|------|
| `include/communicator.h` | `Communicator` 顶层接口（GetUniqueId / Init / 五种 collective / Send / Recv / Finalize / local 视图） |
| `include/channel.h` | `Channel` / `Connector` / `Ring` / `Tree`：`send[p]` / `recv[p]` 是两条有向边的两个槽位；骨架（id + 各槽位 peer/channel_id/is_send）由 communicator 在 `Init` 铺，`ring` 与 `tree`（`parent`/`children`/`star_peers`）由 topology 填，transport 由 `InitChannels` 装 |
| `src/communicator.cpp` | `GetUniqueId` + `Init`（双拓扑 + 机器分组 + `FillPeers` 建边）+ 构造 executor + `ConnectActiveEdges` 内联按边三选一（SHM/RDMA/TCP）+ `Finalize` |
| `src/bootstrap.cpp` | master/worker 交换 `NodeInfo`（hostname 与 RDMA 端点） |

# 实现原理

- **unique id**：`GetUniqueId` 只 `CreateListenSocket(0)` 绑空闲端口（fd 记在 `bootstrap_listen_fd`，持到 `Finalize`），把 IP + 端口装进定长 `UniqueId` 返回，不做通信；分发给其余 rank 是启动方责任（`tests/` 用 `MPI_Bcast` 播原始字节）。ip/port 都从 `config.unique_id` 读。
- **listener 在 bootstrap 前绑好并持有**：bootstrap listener 就是 `GetUniqueId` 绑的那个 fd，`Bootstrap::RunMaster` 只借用不关闭；数据面 TCP listener 在 bootstrap 前 `Listen("", 0)` 建好（`NodeInfo.data_port` 就是它），channel init 完才 `Close()`；RDMA listener 同理，全局不用 RDMA 随即关闭；共享内存 listener 在 bootstrap 前 `Listen(<path>, 0)` 绑 `/tmp/originccl/<port>-<rank>.sock`。
- **连边方向**：每 channel 按拓扑 `FillPeers` 返回的有向边集合（ring ∪ tree 的去重并集）逐条建边，按 `rank < peer` 决定主动 connect、否则被动 accept（避免启动死锁）。连接是**公共池**：ring 边与 tree 边共用同一套 `send[p]`/`recv[p]` 槽位，同一条 (i,j) 边只建一次，ring 与 tree 复用同一 transport（同一 channel 同一时刻只有一个算法在跑，方向固定无冲突）。
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

# P2P 接口与生命周期

- `bool Send(const void* buffer, size_t count, DataType dtype, int peer)` 与 `bool Recv(void* buffer, size_t count, DataType dtype, int peer)` 为同步接口。成功返回后发送缓冲区可复用，接收缓冲区可读取；Send 成功不保证对端 Recv 已返回。
- 只需收发双方参与。明确指定 peer，无 tag、任意来源或长度探测；同一有向 rank 对按调用顺序一一匹配，调用方保证 count/dtype 相同，不拆分或合并消息。禁止同一 communicator 并发调用，调用方保证收发与集合操作之间不形成等待环。
- 不支持 peer==rank，越界 peer 同样 LOG_ERROR + false。合法 peer 的 count=0 直接成功，允许空 buffer，不建连接、不产生消息，也不提供同步语义。
- `Channel.send_p2p` / `recv_p2p` 按 peer 索引，初始 transport 为空；发送固定用 channel 0、接收固定用 channel 1（因此要求 `n_channels >= 2`）。它们与集合连接隔离，方向也各用独立 transport；planner 首次使用时建连，之后复用。
- bootstrap 前创建独立 P2P TCP、SHM 和可用的 RDMA_ZC listener。`NodeInfo.p2p_port` / `p2p_rdma_port` 公布两个网络端点；SHM 路径为 `/tmp/originccl/<port>-<rank>.sock.p2p`。独立端点防止较快 rank 的首次 Send 被较慢 rank 的集合初始化误接。原有集合 listener 仍在 InitChannels 后关闭，P2P listener 和节点表保留至 Finalize。
- P2P 没有后台接入线程，首次 Send 可以等待 Recv。发送端主动连接、接收端接受握手；planner 在每一轮建连时临时开一个发送建连线程与一个接收接受线程，两个方向的连接同时建立，返回前 join，完成即退出（无长驻线程）。接收端按握手 rank 缓存连接，保存到 channel 1。

# Group 批量下发

`GroupStart()` 递增组深度，`GroupEnd()` 递减；深度大于 0 时通信调用只把 `CollTask` 推入 `pending_tasks`，只有深度归零的最外层 `GroupEnd()` 触发执行，返回是否全部成功。非组内调用等价于单元素批次，走同一条排序与规划路径。

- 一批任务先按集合在前、P2P 在后排序，集合按 `(CollFunc, count*type_size, dtype, 有效 op, 有效 root)` 稳定升序；各 rank 提交相同集合任务，同键重复任务保留入队对应顺序。
- 集合任务按现有单操作切片规则轮转分配到 channel 并执行完，随后进入 P2P 阶段。
- P2P 按轮次 `i = 1..world_size-1` 执行：`Send` 到 `(rank-i+world_size)%world_size` 的任务放 channel 0、`Recv` 自 `(rank+i)%world_size` 的任务放 channel 1（轮次由 planner 按 peer 推导，调用方不传）。每轮所有发送与接收任务放入同一 `CollPlan` 的两个 channel，由 executor 并发推进，整轮完成后进入下一轮。
- 组内操作彼此独立，buffer 存活到最外层 `GroupEnd()` 返回；嵌套时内层 `GroupEnd()` 不执行。
- 同机优先 SHM；禁用 SHM 后沿用全局网络选择，P2P 的 RDMA 使用零拷贝变体。跨机严格 RDMA_ZC，任一端无可用端点则在非空 P2P 操作时失败，不影响原有集合初始化，不自动降级。
- Finalize 先停 executor，再关闭两套 channel 连接，最后关闭 P2P listener、清空节点表。细节见 [planner.md](planner.md) 和 [topology.md](topology.md)。

# 隐含约定

- **unique id 只由 rank0 生成**，其余 rank 从外部拿到同一份 id 调 `Init`；不做「非 rank0」校验，同一对象重复调用 `GetUniqueId` 明确报错。
- **listener 都在 bootstrap 前绑好并持有**，id 公布出去的端口不可能在首次 accept 前被抢走——不能退回「先取空闲端口 → 关闭 → 重绑」。
- **集合连边方向 `rank < peer` 勿回退**（改方向重引入启动死锁）；P2P 则固定发送方 Connect、接收方 Accept。
- **机器分组在 bootstrap 后从 `all_nodes` 现算并存进 `Communicator` 成员**：按 hostname 首见序给机器编号（`machine_index`），`machine_leaders[m]` 取第 m 台机器上最小的 rank（即 `local_ranks[0]`）。两个拓扑的 `FillChannels(*this, channels)` 读取这份成员：ring 填 `ring.next/prev`，tree 填 `tree.{parent,children,star_peers}` 并按 channel 记 `channel_roles_`/`is_leader_`。`FillChannels` 必须在 `InitChannels` 之前调用。
- **channel 边是有向的，每条边一个独立 transport**：`send[p]` 只发送、`recv[p]` 只接收，是两条有向边、两个 `Connector`、两个 fd。2 rank 时 `prev == next`，同一对 rank 仍是两条独立边、两个 transport，不因 peer 相同而合并。
- **主动边有 5 次连接重试**（非无重试）；listener 先于 bootstrap 绑好，重试是防御性的。
- **共享内存失败即初始化失败**，无自动回退 TCP；RDMA 同理，全局选定后建链/QP/MR 错误直接 `LOG_ERROR` + `false`，不静默改走 TCP（`OCCL_DISABLE_RDMA=1` 是显式选择而非回退）。
- **机器身份 = hostname**（`Utils::GetHostname()` → `gethostname()`），不是 IP。
- **`Finalize` 先 `executor->Shutdown()`（多线程需 join worker），再关 channel transport**。`PlanTask` 经 `shared_ptr<Transport>` 保证 transport 在 plan 执行期间有效。
- **绑核用 `local_rank`（非全局 rank），非致命**：失败只 `LOG_WARN` 并继续初始化，不能让 `Init` 失败。
- 改动传输选择后跑 `scripts/run_tests.sh`：`test_multi_machine` 用 `CommConfig::get_hostname` 注入逻辑 hostname（`rank / ranks_per_machine` 推导机器号），断言 local 视角与全量集合用例；`test_single_machine` 断言真实单机的 local 视角。等级决定布局（0/1/2 = 4×1 / 4×2 / 8×4），`OCCL_DISABLE_SHM`/`OCCL_DISABLE_RDMA` 不再是测试维度。集合档位只做黑盒接口断言，不再检查环边的具体传输类型。
