# Topology 层（`include/topology*.h`、`src/topology_ring.cpp`）

集合算法层。**只做事件处理**，把"如何等待 socket 就绪"完全交给 executor。改动算法或状态机前阅读本文件。

## 核心职责边界

- 拥有 AllReduce、Broadcast、Reduce、AllGather、ReduceScatter 算法，通过非阻塞状态机接口推进。
- 状态机接口：
  - `CollectiveInit(PlanTask&)`：根据 `task.func` 分派初始化，按操作和 rank 角色检查缓冲区。合法的零元素任务直接完成，允许空缓冲区；单 rank 只做本地操作，无数据面。
  - `CollectiveStep(PlanTask&, CollEvent)`：非阻塞推进一次。`CollEvent::Writable` → 推进 send（`TrySend`）；`CollEvent::Readable` → 推进 recv（`TryRecv`）。当前阶段完成后连续结算已完成阶段，并主动尝试新阶段启用的传输，再交还 executor 等待。
  - `CollectiveDone`：**仅表示成功完成**（`phase == kPhaseDone`）。失败通过 Init/Step 的 false 上报，executor 不再推进失败任务。
  - `TopologyRing` 保留 `AllreduceInit/Step/Done`，并提供其余四种操作的具名三件套。通用入口负责分派，executor 不依赖具名方法。既有 `Topology` 派生类需迁移到通用三件套；不保证旧虚接口的 ABI 兼容。
- 算法游标 `CollOpState`（`include/types.h`）：纯数据、无回调、无 mutable，只存不可现算的最小状态 `phase, step, send_progress, recv_progress, send_done, recv_done, temp_buffer`。失败不存于游标（Init/Step 返回值即错误通道）。
- 不负责：不监听 fd、不决定等待策略、不开线程；无可变成员状态（算法游标存于 `PlanTask.state`）；不暴露 WantRead/WantWrite（executor 默认对 read+write fd 都监听）。

## 不变式与设计区间

### 不变式

- **一步内 send 与 recv 必须并发推进**。2 rank 时 `prev == next`，串行（先 send 完再 recv）会双方互等对方 recv → **死锁**。所以"一步"不是原子状态：send/recv 各有独立 progress + done 标志。
- **错误只有一个通道**：`CollectiveInit`/`CollectiveStep` 及具名 Init/Step 声明为 `noexcept`，返回 `false` 即失败，拓扑内已 `LOG_ERROR`；executor 见到 false 立即放弃该次集合。意外异常（如 `bad_alloc`）直接终止，不转换为可恢复失败，不增加失败游标或重试。
- executor 只使用 `CollectiveInit/CollectiveStep/CollectiveDone`；新增操作不修改 executor，不向 plan 添加函数指针或回调。
- 并发交换的收发必须同时推进；依赖接收结果的转发阶段则必须先收齐再发送，不能把未初始化数据发送出去。阶段转换经 `BeginPhase` 主动尝试传输，遵守 EPOLLET 契约。

### 设计区间

- `Topology` 基类允许新算法实现（不止 ring）。
- chunk 布局/指针/字节数由 `phase/step/rank` **现算**（`AllreduceSendChunk`/`AllreduceRecvChunk`/`AllreduceSendPtr`/`AllreduceRecvPtr`/`AllreduceSendBytes`/`AllreduceRecvBytes` helper 在 `topology_ring.cpp` 匿名命名空间），不必存进 state。隐含保证是 `state` 只存不可现算的最小状态，据此不为这些量设缓存字段。
- `task.chunk_size`（`ceil(elem_count / world_size)`）仍服务 AllReduce。其余操作使用每 rank 块内的 `elem_count` 和原始 rank 块跨度 `rank_stride`；寻址为已偏移 slice 基址加 `rank * rank_stride * type_size`。

## Ring 算法

所有算法仅向 next 发送、从 prev 接收，不反向使用已有 transport。

| 操作 | 阶段与布局 |
|------|------------|
| AllReduce | 保留 ReduceScatter + AllGather 两阶段，支持原地；AVG 最后除一次 |
| Broadcast | root 发起，非 root 收齐后转发，root 的前驱只接收 |
| Reduce | root 的 next 发起部分结果，各节点合并自身输入后转发，root 最后归约；非 root 使用 scratch |
| AllGather | 先复制本 rank 块，再执行 N-1 轮并发收发，recv_buf 按来源 rank 排列 |
| ReduceScatter | 将 channel 的各目标块打包到 scratch，N-1 轮并发交换并归约；索引旋转保证最终持有本 rank 块 |

Reduce 和 ReduceScatter 的 AVG 均先 SUM，再对最终输出除以 world_size；整数规则复用 `ApplyAverage`。
临时数据只存于每个任务的 `state.temp_buffer`，不改写 `task.func/root`，不递归调用 communicator 或 executor。

## 文件介绍

| 文件 | 职责 |
|------|------|
| `include/topology.h` | `Topology` 抽象基类 + 状态机接口 |
| `include/topology_ring.h` / `src/topology_ring.cpp` | Ring 实现：`ring.prev=(rank-1+ws)%ws`、`ring.next=(rank+1)%ws` |

## 修改原则

- 勿回退：一步内 send+recv 并发（旧 `Exchange` 的临时 send 线程已删除，并发靠非阻塞 + executor 全喂就绪事件）。
- 勿回退：`CollOpState` 必须保留 send/recv 独立的 progress + done 标志，不可再删。
- 新增算法实现须继承 `Topology` 基类，实现三件套接口；算法步骤不得在 planner/executor 展开。
