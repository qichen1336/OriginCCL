# Topology 层（`include/topology/*.h`、`src/topology/*.cpp`）

集合算法层。**只做事件处理**，把"如何等待 socket 就绪"完全交给 executor。改动算法或状态机前阅读本文件。

topology 与 executor、transport 一样单独成目录；头文件从 include 根限定引用，形如 `#include "topology/topology.h"`。其余层仍平铺。

# 核心职责边界

- 拥有 AllReduce、Broadcast、Reduce、AllGather、ReduceScatter 算法与 P2P Send/Recv，通过非阻塞状态机接口推进。
- 状态机接口：
  - `CollectiveInit(PlanTask&)`：按 `task.func` 分派初始化，按操作和 rank 角色检查缓冲区。合法零元素任务直接完成；单 rank 只做本地操作，无数据面。
  - `CollectiveStep(PlanTask&, CollEvent)`：非阻塞推进一次。`Writable` → 推进 send，`Readable` → 推进 recv；当前阶段完成后连续结算已完成阶段并主动尝试新阶段启用的传输。
  - `CollectiveDone`：仅表示成功完成（`phase == kPhaseDone`）；失败经 Init/Step 的 false 上报。
- `FillChannels(comm, channels)`：bootstrap 之后、`FillPeers`/`InitChannels` 之前调用，把拓扑形状写入每个 `Channel`（ring 填 `channel.ring.prev/next`，tree 填 `channel.tree` 的 `parent`/`children`/`star_peers`）；本机视角与机器分组从 `comm` 成员读取，tree 同时把每个 channel 的运行时角色表 `channel_roles_` 与 `is_leader_` 记到自己成员。
- `FillPeers(channel, edges)`：返回本 rank 在该拓扑下要连接的全部 peer（`TopoEdge{peer, is_send}`，每条有向边一条），形状来源是 `channel`（ring 读 `channel.ring`，tree 读 `channel.tree` 三桶）。communicator 据此建连接（见 [communicator.md](communicator.md)）。
- `FillTransports(channel, task)`：按拓扑语义填充 task.send_transports/recv_transports；tree 按 `[star_peers, children, parent]` 顺序输出，与该 channel 的角色表一一对齐；P2P 按 task.func/peer 选择独立连接。
- 算法游标 `CollOpState`（`include/types.h`）：纯数据、无回调、无 mutable，存 `phase`、`send_progress`、`recv_progress`、`send_done`、`recv_done`、`temp_buffer`，以及**拓扑私有的 `algo` union**（`algo.ring.step` 与 `algo.tree.step` 互斥重叠，同一时刻只有当前拓扑的一份有效）。progress/done 是与 transport 向量一一对应的向量（`send_done`/`recv_done` 用 `std::vector<char>` 而非 `vector<bool>`，因 `Try*` 的 done 是 `bool*` 出参）；整侧完成由 `std::all_of` 现算。失败不存于游标（Init/Step 返回值即错误通道）。
- 不负责：不监听 fd、不决定等待策略、不开线程；无可变成员状态。

# 文件介绍

| 文件 | 职责 |
|------|------|
| `include/topology/topology.h` | `Topology` 抽象基类 + 状态机接口（含 `FillChannels` / `FillPeers` / `FillTransports`）+ `TopoEdge` |
| `include/topology/topology_ring.h` / `src/topology/topology_ring.cpp` | Ring 实现：`ring.prev=(rank-1+ws)%ws`、`ring.next=(rank+1)%ws`；`FillTransports` 取 `send[next]` / `recv[prev]` 并过滤空 transport；`FillPeers` 返回 next/prev 两条有向边 |
| `include/topology/topology_tree.h` / `src/topology/topology_tree.cpp` | Tree 实现：机器内星型 + 机器间 double binary tree（DBT），见下节 |
| `include/topology/topology_p2p.h` / `src/topology/topology_p2p.cpp` | Send/Recv：单 peer、单方向、整段传输（发送在 channel 0、接收在 channel 1，由 planner 决定） |

# 实现原理

- 所有算法仅向 next 发送、从 prev 接收，不反向使用已有 transport。chunk 布局/指针/字节数由 `phase/step/rank` 现算（`topology_ring.cpp` 匿名命名空间的 helper），不存进 state。

| 操作 | 阶段与布局 |
|------|------------|
| AllReduce | ReduceScatter + AllGather 两阶段，支持原地；AVG 在 RS→AG 边界仅对本 rank 完成块除一次 |
| Broadcast | root 发起，非 root 收齐后转发，root 的前驱只接收 |
| Reduce | root 的 next 发起部分结果，各节点合并自身输入后转发，root 最后归约；非 root 使用 scratch |
| AllGather | 先复制本 rank 块，再执行 N-1 轮并发收发，recv_buf 按来源 rank 排列 |
| ReduceScatter | 滑动部分和：每轮发当前累计块、收 prev 部分和并入本 rank 对应输入块；`recv_buf` 与 1 块 scratch 按 step 奇偶交替承担发/收，N-1 轮后结果落在 `recv_buf`；`send_buf` 只读 |

- Reduce、ReduceScatter 与 AllReduce 的 AVG 均先按 SUM 归约，再对最终输出除以 world_size（整数规则复用 `ApplyAverage`）。
- 临时数据只存于 `state.temp_buffer`（Reduce 2 块、ReduceScatter 1 块、AllReduce 1 个 chunk），不改写 `task.func/root`，不递归调用 communicator 或 executor。
- 多连接数据面语义：`TrySend`/`TryRecv` 的 `size` 是「每条连接各收发多少字节」，同一份 `send_data`/`recv_data` 对每条连接各自维护进度。

# Tree 拓扑（`TopologyTree`）

- **形态**：机器内星型（同机非 leader 全部只连本机 leader）+ 机器间 DBT（double binary tree，两棵互补二叉堆，leader 参与）。星型是 DBT 的前/后置一跳，不是第三棵树。`channel.tree` 三桶按角色填：leader 的 `star_peers`=本机其余 rank、`children`/`parent`=DBT 子/父；非 leader 只有 `star_peers=[本机 leader]`，`children`/`parent` 为空/-1。
- **两棵树按 channel 分配**：偶数 `channel.id` 用 Tree0，奇数 `channel.id` 用 Tree1。两棵树都以机器编号为节点、在 `[0, M-1]`（`M = machine_count`）上定义同一套完全二叉堆公式，再经 `machine_leaders` 换算成全局 rank。Tree0 直接以 `machine_index` 为节点；Tree1 先取镜像节点 `M-1-machine_index`，再把算出的父/子节点镜像回 `M-1-node`，两棵树因此互为镜像。
- **互补性**：Tree0 的内部节点是 `[0, ⌊M/2⌋-1]`，Tree1 的内部节点是 `[M-⌊M/2⌋, M-1]`，两集合不相交——即每个机器在一棵树里做内部节点、在另一棵树里做叶子。`M` 为偶数时完全互换；`M` 为奇数时中间那台机器在两棵树里都是叶子（这是必然的剩余节点，不是缺陷）。每棵树各自连通、无环、`M-1` 条边、每节点出度 ≤2。
- **机器分组来自 `Communicator` 成员（`GetLocalRank`/`GetLocalRanks`/`GetMachineIndex`/`GetMachineCount`/`GetMachineLeaders`），由 `FillChannels` 读取**：`local_ranks` 是本机 rank 排序序列，`machine_index` 是本机在首见序机器列表里的下标，`machine_leaders[m]` 是第 m 台机器的 `local_ranks[0]`。`is_leader_` 即 `local_rank == 0`。DBT 父/子公式（`parent(n) = ⌊(n-1)/2⌋`、`children(n) = {2n+1, 2n+2}`，只保留 < `M` 的子节点；根 `n = 0` 设 `parent = -1`）对两棵树相同，区别只在节点取值与是否镜像。机器数与机器内节点数**不再要求是 2 的幂**。单机（`machine_count == 1`）时两棵树都退化为只有 leader 自己，机器间阶段立即完成。
- **只实现三种操作**：AllReduce / ReduceScatter / AllGather。Broadcast / Reduce 永远走 ring，planner 不选树（见 [planner.md](planner.md)），树的 `CollectiveInit` 收到不支持 func 时 `LOG_ERROR` + `false` 防御。
- **两阶段推进**：整个 channel 先跑完所有 chunk 的上行，再跑完所有 chunk 的下行；同一方向内 chunk 连续推进（上行 chunk i 发出即开始 chunk i+1，不等下行）。上行（leader）：`UpStar`（收 local children → 归约）→ `UpTreeRecv`（收 DBT children → 归约）→ `UpTreeSend`（发 DBT parent），每 chunk 完成后 `algo.tree.step` 前进，最后一个 chunk 发出后 `step` 归零转入下行；根节点没有 parent 发送，该步立即完成。非 leader 上行只有 `UpStar`（发 leader）。下行（leader）：`DownTreeRecv`（收 DBT parent 结果）→ `DownTreeSend`（发 DBT children）→ `DownStar`（发 local children）；非 leader 只有 `DownStar`（收 leader）。下行逐 chunk 推进，最后一个 chunk 完成后一次性 `ExtractResult`。上行/下行不重叠，故单 task 只需一份 `algo.tree.step` 游标。
- **打包累加区与 chunk 粒度收发**：planner 按「每 rank 块的同一子区间」下发 channel，`elem_count` 是切片长度、`rank_stride` 仍是原始块跨度 `count`。tree 在 `temp_buffer` 里按 rank 紧凑打包（内部 rank 间距 = `elem_count`），AG/RS 的 `SeedAccumulator`/`ExtractResult` 负责用户缓冲区（`rank_stride` 布局）与打包区（`elem_count` 布局）的转换。每次 `TrySend`/`TryRecv` 的字节数是一个 chunk（`min(chunk_size, 剩余)`），不是整个 channel payload，最后一个 chunk 可能不满；chunk 数 `ceil(TotalElems / chunk_size)` 现算（AllReduce 为 `elem_count`，AG/RS 为 `world_size*elem_count`）。
- **连接角色**：`channel_roles_[channel_id]` 是与该 channel 连接输出顺序并行的角色向量，取值 `Star / TreeDown / TreeUp`；`PlanTask.channel_id` 决定用哪张表。`FillTransports` 按 `channel.tree` 三桶顺序（先 `star_peers`、再 `children`、再 `parent`）输出连接，`CollectiveStep` 用该表 + 当前 phase 现算每条连接活跃与否，不活跃置 `done = 1` 跳过（等价于"不同阶段用连接子集的交集"）。`TreeDown` 必须在 `TreeUp` 之前，归约接收角色的前缀顺序保证 `ScratchSlot` 按连接索引寻址正确。leader 归约多连接时，每条连接收到的数据落在独立 scratch 槽（`state.temp_buffer` 的 `TotalElems` 之后），收齐后逐条 `PerformReduce` 进累加区。
- **单游标足以表达两阶段**：`CollOpState.algo.tree` 只有一个 `step`，上行与下行各自从头推进该游标，且两阶段不相交，故无需为两阶段或每棵树维护独立 phase。

# P2P 拓扑

- `FillChannels` 初始化各 channel 的 `send_p2p[peer]` / `recv_p2p[peer]` 槽位，transport 留空；`FillPeers` 返回空集，不参与集合初始化建边。
- 参数检查和连接准备已由 planner 完成。planner 把发送任务绑定到 channel 0、接收任务绑定到 channel 1，`FillTransports` 只为非空任务选该 channel 上对应方向的一条连接；空任务不挂 transport。
- `CollectiveInit` 初始化该方向的 progress/done 并主动尝试一次传输，零长度直接完成。`CollectiveStep` 将原始 buffer 和完整 `elem_count * type_size` 交给 `TrySend`/`TryRecv`，不改地址、不切 chunk；`phase == 1` 表示完成。
- 无需 topology 层流水线不等于阻塞直发：executor 仍等待就绪并推进；transport 保留背压、RDMA_ZC 阈值和内部分块。

# 隐含约定

- **集合算法同一步内 send 与 recv 必须并发推进**。2 rank 时 `prev == next`，串行（先 send 完再 recv）会双方互等 → 死锁。P2P 每个任务只有一个方向，planner 把同一轮的全部 Send 放 channel 0、全部 Recv 放 channel 1，由 executor 的两个 channel 并发推进双向；同一有向 rank 对的消息顺序由轮次与 FIFO 保证。
- **一条连接一份进度，遍历在 topology 层**：`PushBuffer` 按 `CollEvent` 选定一侧后遍历该侧全部 transport 逐条 `TrySend`/`TryRecv`；`Try*` 接口本身保持单连接语义。未就绪连接无进展不影响其他连接；一条失败即整体 false。该侧每次事件语义是「推进该侧全部连接一次」。
- **错误只有一个通道**：`CollectiveInit`/`CollectiveStep` 及具名 Init/Step 是 `noexcept`，返回 `false` 即失败（拓扑内已 `LOG_ERROR`）；executor 见到 false 立即放弃。意外异常（如 `bad_alloc`）直接终止，不转换为可恢复失败。
- executor 只使用 `CollectiveInit/CollectiveStep/CollectiveDone`；新增操作不修改 executor，不向 plan 添加函数指针或回调。
- 并发交换的收发必须同时推进；依赖接收结果的转发阶段必须先收齐再发送。阶段转换经 `BeginPhase`（ring）/`Activate`（tree）主动尝试传输，遵守 EPOLLET 契约。
- **`send_done`/`recv_done` 不可改成 `vector<bool>`**（`Try*` 的 done 是 `bool*` 出参，位压缩取不到 `bool&`）。
- 新增拓扑只实现 `FillChannels` + `FillPeers` + `FillTransports` 即可；新增算法实现须继承 `Topology` 基类，实现三件套接口，算法步骤不得在 planner/executor 展开。
- **`algo` union 一次只有一种拓扑有效**：ring 写 `algo.ring.step`、tree 写 `algo.tree.step`，同一 task 只绑定一种拓扑，不会交叉读写；P2P 不使用 algo。
