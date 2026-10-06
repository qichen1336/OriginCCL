# Planner 与数据模型（`src/planner.cpp`、`include/types.h`、`include/planner.h`）

把外层 collective API 转成按 channel 分组的可执行计划。改动 plan 类型或 planner 前阅读本文件。

# 核心职责边界

- `CollTask` 表达五种 collective 与 Send/Recv 的 API 语义：`func`、`send_buf`/`recv_buf`、`count`/`dtype`、`ReduceOp`、`root`、`peer`。peer 仅用于 P2P，无根操作忽略 root，非归约操作忽略 op。
- `Planner::SortTasks` 与 `Planner::Plan(comm, tasks, plan)` 分别对一个任务批次稳定排序、将批次中全部集合与 P2P 任务组装到一个 `CollPlan`。Plan 先切片集合任务，再按轮次准备 P2P 连接并把任务追加到固定方向 channel。集合操作按 count 代表的元素区间切片。多 rank 块布局下，每个 channel 处理每个块的相同子区间，而非切整个 `count*world_size` 缓冲区。
- **排序规则**：集合在前，按 `(CollFunc, count*type_size, dtype, 有效 op, 有效 root)` 稳定升序（无效维度取 0）；P2P 在后，按轮次升序。轮次由 planner 推导：`Send` 到 `p` 的轮次是 `world_size - (p-rank+world_size)%world_size`，`Recv` 自 `p` 的轮次是 `world_size - (rank-p+world_size)%world_size`，两端对同一消息得到同一轮次编号。同轮内 `Send` 先于 `Recv`、同方向按 peer 稳定排序。该规则只依赖任务内容，因此同 key 集合任务的相对顺序在各 rank 上一致。
- **两种计划形态**：`Plan(comm, tasks, plan, preempt=false)` 默认把切片按 `n_used` 个连续环绕 channel 摊入 `plan.channels`；`preempt=true`（纯集合 Group 且选了 preempt executor）则把同一批切片按规范顺序平铺进 `plan.collectives`，不摊入任何 channel，由 executor 在运行期把 slice 重绑到某条 lane。切片内容不依赖 channel 号，这是运行期改绑的前提。
- **channel 分配**：每个集合任务都从 channel 0 开始，用 `n_used` 个连续环绕物理 channel；多个任务可排入同一 channel 的 FIFO 队列。P2P 固定 `Send`→channel 0、`Recv`→channel 1。
- **拓扑选择**：planner 每次调用按 `func` + 数据量选拓扑挂到 `PlanTask.topology`。Broadcast / Reduce / ReduceScatter / AllGather 永远 ring；只有 AllReduce 在 `count * type_size / kChunkBytes < kTreeThresholdChunks` 时走 tree，否则 ring（见 [occl_config.h](../../include/occl_config.h) 的 `kTreeThresholdChunks`）。阈值以 chunk 为单位，`OCCL_SMALL_TESTS` 同除 256 后选择不变。tree 仅实现 AllReduce（见 [topology.md](topology.md)）。
- `PlanTask` 显式携带该 slice 的 send/recv 地址、元素数、dtype、reduce op、rank/world size、root、peer、rank_stride、topology 指针、**所属 channel 的 `channel_id`**、**对应 channel 的 send/recv transport 向量**。
- 两个 transport 向量由 `Topology::FillTransports(Channel&, PlanTask&)` 填充。集合操作仍由拓扑按形状选连接；P2P 拓扑依据 task.func/peer 取一个方向的一条连接。planner 仅在 P2P 懒建连时写入对应 Connector。
- 不负责：不展开算法步骤（topology）、不决定等待策略（executor）。

# 文件介绍

| 文件 | 职责 |
|------|------|
| `include/planner.h` / `src/planner.cpp` | `CollTask` → `CollPlan` 切片与组装；transport 向量经 `Topology::FillTransports` 填充 |
| `include/types.h` | 核心数据模型：`CommConfig`、`NodeInfo`、`CollTask`、`CollEvent`、`CollOpState`、`PlanTask`（含 transport 向量与 `state`）、`ChannelPlan`、`CollPlan`（含 `channels` 与 preempt 的 `collectives`） |

# 实现原理

- **P2P 规划**：`Plan` 开头的 `ValidateP2p` 检查 peer 范围且不能是自己，非空消息检查 buffer 与字节数；合法零长度不建连。每个轮次用 `PrepareRound` 取该轮去重后的收发 peer，并先剔除已缓存连接：双向都有新连接时并行开一个 `ConnectP2p` 线程与一个 `AcceptP2p` 线程同时连接，join 后再组装任务；只有一侧有新连接时在当前线程内联完成；两侧连接均已缓存则不启动建连线程。发送连接写 `channel 0` 的 `send_p2p[peer]`，接收连接写 `channel 1` 的 `recv_p2p[peer]`（非目标来源只缓存连接）。成功后每个任务生成一个完整长度的单向 `PlanTask`，绑定 `TopologyP2p`，不切片、不设置流水 chunk，并追加至统一 plan 的对应 channel。
- **P2P 建连由 planner 执行**：communicator 持有节点表和独立 P2P listener，Planner 通过 friend 访问；Send 主动连接并发送本 rank 握手，Recv 轮询 listener、接入并按来源 rank 存入 channel 1 的 `recv_p2p`，直到目标连接就绪。Plan 在 executor 启动前按轮次准备全部连接，失败经 Plan 的 false 上报。
- **P2P 传输选择**：同 hostname 且 SHM 启用时用 SHM；同机禁用 SHM 时沿用集合的全局 RDMA/TCP 网络选择（P2P 的 RDMA 使用 RDMA_ZC）。不同 hostname 必须用双方已公布端点的 RDMA_ZC，任一端不可用直接失败，不回退 TCP。无设备不妨碍集合通信初始化或零长度 P2P。
- `PlanTask.chunk_size` 语义按拓扑解释：ring = `ceil(elem_count / world_size)`（AllReduce 块划分）；tree = `kChunkBytes / type_size`（流水粒度，逐 chunk 推进）。
- `rank_stride` 保存原始 `CollTask.count`（元素数）；非空 send/recv 基址偏移 `offset*type_size`，null 原样保留。topology 用 `block_rank*rank_stride` 找下一 rank 块，不能用 channel 的 elem_count 代替跨度。
- `PlanTask.state`（`CollOpState`）见 [topology.md](topology.md)——planner 只值初始化，不展开算法阶段。
- collective 参数由 `ValidateCollectiveTask` 统一检查：函数、dtype、有效 reduce op/root、必需 buffer，以及单块字节量与 AllGather/ReduceScatter 总跨度的 `size_t` 溢出。提交入口、`SortTasks` 和 `Plan` 都在排序或地址/字节计算前执行该校验；`SortTasks` 和 `Plan` 返回 `false` 表示拒绝无效任务。
- `PlanTask.topology` 复用 communicator 持有的 ring/tree/p2p 拓扑，planner 不新建拓扑；集合任务的 `PlanTask.channel_id` 取 `comm_channel.id`，P2P 固定为 0。tree 据此选择树形与运行时角色（见 [topology.md](topology.md)），executor 不解释该字段。
- 启用 channel 规则：tree 下的 AllReduce 与 ring 下的 AllReduce，每 channel 最小颗粒度是 `OcclConfig::kChunkBytes × world_size`；ring 下其余操作（含 ReduceScatter / AllGather）是 `kChunkBytes`（合称 `unit`）。使用数 `n_used = max(1, min(total_bytes / unit, GetNChannels()))`，小消息只触发单 channel。`total_bytes = count*type_size`（AG/RS 也用单块大小，不是 `count*world_size`），多块操作按单块大小选通道。切分按 unit 对齐：每 channel 分 `⌊units_total/n_used⌋` 或 `⌈` 个 unit，`total_bytes % unit` 的余数并入最后一个 channel。count=0 仍生成一个立即完成的任务。
- tree AllReduce 可多 channel：每 channel 只处理每 rank 块的同一子区间，`elem_count` 是切片长度、`rank_stride` 仍是原始块跨度 `count`；tree 直接用 `elem_count` 定位切片，不做打包区。
- `CollPlan(n_channels)` 构造时创建 `ChannelPlan[0..N-1]` 并初始化 `channel_id`，planner 不重复赋值。

# 隐含约定

- **chunk 颗粒度是编译期常量**：`OcclConfig::kChunkBytes`（`include/occl_config.h`）定义在 `32 KiB / kTestScale`，`kTestScale` 由 `OCCL_SMALL_TESTS` 决定（OFF→1，ON→256）。宏只等比缩放 unit 与阻塞用例的 count，`units_total`、`n_used` 与每 channel 切分完全不变，因此覆盖的 planner 路径等价，但内存降 256 倍。`kTreeThresholdChunks`（tree/ring 阈值，单位 chunk）同样不缩放，故 tree/ring 选择在两种模式下不变。
- **plan 无行为铁律**：plan 不使用 `std::function`、`execute` 回调、`pre_execute`/`post_execute`。`CollOpState` 是纯数据游标，不是回调；游标只被 executor 推进，拓扑不越权。
- **改 `types.h` 字段必须同步 planner 与 executor 两者**。`PlanTask::send_transports` / `recv_transports`（及 `CollOpState` 的 progress/done 向量）是与连接一一对应的向量，`Try*` 保持单连接语义，多连接遍历在 topology（推进）与 executor（就绪注册）两侧。
- planner 做集合切片、P2P 连接准备和任务组装，不展开数据面算法步骤；集合连接的选择仍一律经 `Topology::FillTransports`。
