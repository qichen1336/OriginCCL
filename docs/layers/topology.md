# Topology 层（`include/topology*.h`、`src/topology_ring.cpp`）

集合算法层。**只做事件处理**，把"如何等待 socket 就绪"完全交给 executor。改动算法或状态机前阅读本文件。

# 核心职责边界

- 拥有 AllReduce、Broadcast、Reduce、AllGather、ReduceScatter 算法，通过非阻塞状态机接口推进。
- 状态机接口：
  - `CollectiveInit(PlanTask&)`：按 `task.func` 分派初始化，按操作和 rank 角色检查缓冲区。合法零元素任务直接完成；单 rank 只做本地操作，无数据面。
  - `CollectiveStep(PlanTask&, CollEvent)`：非阻塞推进一次。`Writable` → 推进 send，`Readable` → 推进 recv；当前阶段完成后连续结算已完成阶段并主动尝试新阶段启用的传输。
  - `CollectiveDone`：仅表示成功完成（`phase == kPhaseDone`）；失败经 Init/Step 的 false 上报。
- `FillTransports(channel, send_out, recv_out)`：按拓扑语义挑出该 channel 上本 task 真正要用的连接；planner 不自己看 `ring`/`Connector`。`FillChannels` 只填拓扑形状（`channel.ring`），send/recv 槽位骨架由 communicator 铺。
- 算法游标 `CollOpState`（`include/types.h`）：纯数据、无回调、无 mutable，只存 `phase, step, send_progress, recv_progress, send_done, recv_done, temp_buffer`。progress/done 是与 transport 向量一一对应的向量（`send_done`/`recv_done` 用 `std::vector<char>` 而非 `vector<bool>`，因 `Try*` 的 done 是 `bool*` 出参）；整侧完成由 `std::all_of` 现算。失败不存于游标（Init/Step 返回值即错误通道）。
- 不负责：不监听 fd、不决定等待策略、不开线程；无可变成员状态。

# 文件介绍

| 文件 | 职责 |
|------|------|
| `include/topology.h` | `Topology` 抽象基类 + 状态机接口（含 `FillChannels` / `FillTransports`） |
| `include/topology_ring.h` / `src/topology_ring.cpp` | Ring 实现：`ring.prev=(rank-1+ws)%ws`、`ring.next=(rank+1)%ws`；`FillTransports` 取 `send[next]` / `recv[prev]` 并过滤空 transport |

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

# 隐含约定

- **一步内 send 与 recv 必须并发推进**。2 rank 时 `prev == next`，串行（先 send 完再 recv）会双方互等 → 死锁。故"一步"不是原子状态：send/recv 各有独立 progress + done 标志（各自又是与连接一一对应的向量）。
- **一条连接一份进度，遍历在 topology 层**：`PushBuffer` 按 `CollEvent` 选定一侧后遍历该侧全部 transport 逐条 `TrySend`/`TryRecv`；`Try*` 接口本身保持单连接语义。未就绪连接无进展不影响其他连接；一条失败即整体 false。该侧每次事件语义是「推进该侧全部连接一次」。
- **错误只有一个通道**：`CollectiveInit`/`CollectiveStep` 及具名 Init/Step 是 `noexcept`，返回 `false` 即失败（拓扑内已 `LOG_ERROR`）；executor 见到 false 立即放弃。意外异常（如 `bad_alloc`）直接终止，不转换为可恢复失败。
- executor 只使用 `CollectiveInit/CollectiveStep/CollectiveDone`；新增操作不修改 executor，不向 plan 添加函数指针或回调。
- 并发交换的收发必须同时推进；依赖接收结果的转发阶段必须先收齐再发送。阶段转换经 `BeginPhase` 主动尝试传输，遵守 EPOLLET 契约。
- **`send_done`/`recv_done` 不可改成 `vector<bool>`**（`Try*` 的 done 是 `bool*` 出参，位压缩取不到 `bool&`）。
- 新增拓扑只实现 `FillTransports` 即可；新增算法实现须继承 `Topology` 基类，实现三件套接口，算法步骤不得在 planner/executor 展开。
