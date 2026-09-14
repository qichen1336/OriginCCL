# Topology 层（`include/topology*.h`、`src/topology_ring.cpp`）

集合算法层。**只做事件处理**，把"如何等待 socket 就绪"完全交给 executor。改动算法或状态机前阅读本文件。

## 核心职责边界

- 拥有算法（copy → ReduceScatter → AllGather → AVG），通过非阻塞状态机接口推进。
- 状态机接口：
  - `AllreduceInit(PlanTask&)`：初始化 `state`（out-of-place copy、分配 temp_buffer、布置首个 ReduceScatter step）。`world_size==1` 直接完成（含 AVG）。
  - `AllreduceStep(PlanTask&, CollEvent)`：非阻塞推进一次。`CollEvent::Writable` → 推进 send（`TrySend`）；`CollEvent::Readable` → 推进 recv（`TryRecv`）。只推进对应传输，不盲目两试。一步的 send+recv 都完成则 `CompleteStep`。
  - `AllreduceDone`：**仅表示成功完成**（`phase == kPhaseDone`）。失败的 task 永远不 done。
- 算法游标 `CollOpState`（`include/types.h`）：纯数据、无回调、无 mutable，只存不可现算的最小状态 `phase, step, send_progress, recv_progress, send_done, recv_done, temp_buffer`。失败不存于游标（Init/Step 返回值即错误通道）。
- 不负责：不监听 fd、不决定等待策略、不开线程；无可变成员状态（算法游标存于 `PlanTask.state`）；不暴露 WantRead/WantWrite（executor 默认对 read+write fd 都监听）。

## 不变式与设计区间

### 不变式

- **一步内 send 与 recv 必须并发推进**。2 rank 时 `prev == next`，串行（先 send 完再 recv）会双方互等对方 recv → **死锁**。所以"一步"不是原子状态：send/recv 各有独立 progress + done 标志。
- **错误只有一个通道**：`AllreduceInit`/`AllreduceStep` 返回 `false` 即失败，拓扑内已 `LOG_ERROR`；executor 见到 false 立即放弃该次集合，而非等一个永不到来的 done。
- 状态机三件套接口固定（`AllreduceInit`/`AllreduceStep`/`AllreduceDone`），算法推进必须经由它们。

### 设计区间

- `Topology` 基类允许新算法实现（不止 ring）。
- chunk 布局/指针/字节数由 `phase/step/rank` **现算**（`SendChunk`/`RecvChunk`/`SendPtr`/`RecvPtr`/`SendBytes`/`RecvBytes` helper 在 `topology_ring.cpp` 匿名命名空间），不必存进 state。
- `task.chunk_size`（每 chunk 元素数 = `ceil(elem_count / world_size)`）由 planner 计算填入，拓扑只读。

## 文件介绍

| 文件 | 职责 |
|------|------|
| `include/topology.h` | `Topology` 抽象基类 + 状态机接口 |
| `include/topology_ring.h` / `src/topology_ring.cpp` | Ring 实现：`ring.prev=(rank-1+ws)%ws`、`ring.next=(rank+1)%ws` |

## 修改原则

- 勿回退：一步内 send+recv 并发（旧 `Exchange` 的临时 send 线程已删除，并发靠非阻塞 + executor 全喂就绪事件）。
- 勿回退：`CollOpState` 必须保留 send/recv 独立的 progress + done 标志，不可再删。
- 新增算法实现须继承 `Topology` 基类，实现三件套接口；算法步骤不得在 planner/executor 展开。
