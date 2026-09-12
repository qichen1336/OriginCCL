# Topology 层（`include/topology*.h`、`src/topology_ring.cpp`）

集合算法层。**只做事件处理**，把"如何等待 socket 就绪"完全交给 executor。改动算法或状态机前阅读本文件。

## 核心职责边界

- Topology 拥有算法（copy → ReduceScatter → AllGather → AVG），通过非阻塞状态机接口推进。
- Topology **不**监听 fd、**不**决定等待策略、**不**开线程。
- Topology 无可变成员状态：算法游标存于 `PlanTask.state`，不在 Topology 实例上缓存。

## 状态机接口（`Topology`）

- `AllreduceInit(PlanTask&)`：初始化 `state`（out-of-place copy、分配 temp_buffer、布置首个 ReduceScatter step）。`world_size==1` 直接完成（含 AVG）。
- `AllreduceStep(PlanTask&, CollEvent)`：非阻塞推进一次。executor 把监听到的事件作参数传入：
  - `CollEvent::Writable` → 推进 send（`TrySend`）。
  - `CollEvent::Readable` → 推进 recv（`TryRecv`）。
  - 只推进对应传输，不盲目两试。一步的 send+recv 都完成则 `CompleteStep`。
- `AllreduceDone` / `AllreduceSucceeded`：完成与结果查询。
- Topology 不暴露 WantRead/WantWrite——executor 默认对 read+write fd 都监听。

## 算法游标 `CollOpState`（`include/types.h`）

纯数据、无回调、无 mutable。只存不可现算的最小状态：
`phase, step, failed, send_progress, recv_progress, send_done, recv_done, temp_buffer`。

chunk 布局/指针/字节数由 `phase/step/rank` **现算**（`SendChunk`/`RecvChunk`/`SendPtr`/`RecvPtr`/`SendBytes`/`RecvBytes` helper 在 `topology_ring.cpp` 匿名命名空间）。

## 关键不变式：一步内 send 与 recv 并发

Ring 每步的 send 与 recv **必须并发推进**。2 rank 时 `prev == next`，若串行（先 send 完再 recv），双方互等对方 recv → **死锁**。所以：

- "一步"不是原子状态，send/recv 各有独立 progress + done 标志（这是 `CollOpState` 必须保留它们的原因，不可再删）。
- 旧 `Exchange` 的临时 send 线程已删除；并发靠非阻塞 + executor 把就绪事件全部喂给 Step。

## 文件

| 文件 | 职责 |
|------|------|
| `include/topology.h` | `Topology` 抽象基类 + 状态机接口 |
| `include/topology_ring.h` / `src/topology_ring.cpp` | Ring 实现：`ring.prev=(rank-1+ws)%ws`、`ring.next=(rank+1)%ws`、`DefaultChannelCount()=4` |
