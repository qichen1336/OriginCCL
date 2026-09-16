# Planner 与数据模型（`src/planner.cpp`、`include/types.h`、`include/planner.h`）

把外层 collective API 转成按 channel 分组的可执行计划。改动 plan 类型或 planner 前阅读本文件。

## 核心职责边界

- `CollTask` 表达外层 collective API 语义（目前为 AllReduce）：`func`、`send_buf`/`recv_buf`、`count`/`dtype`、`ReduceOp`。调用方不关心 worker 数、线程池、Ring step 或 channel 调度。
- `Planner::Plan(Communicator&, const CollTask&)` → `CollPlan`：tensor 按使用中的 channel 数切片。
- `PlanTask` 显式携带该 slice 的 send/recv 地址、元素数、dtype、reduce op、rank/world size、topology 指针、对应 channel 的 send/recv transport。
- `PlanTask.chunk_size`（每 chunk 元素数 = `ceil(elem_count / world_size)`）由 planner 计算填入，拓扑只读。
- `PlanTask.state`（`CollOpState`）见 [topology.md](topology.md)——planner 只值初始化，不展开算法阶段。
- `PlanTask.topology` 复用 `comm.GetTopology()`；planner 不新建拓扑。
- 不负责：不展开算法步骤（topology）、不决定等待策略（executor）。

## 不变式与设计区间

### 不变式

- **plan 无行为铁律**：plan 不使用 `std::function`、`execute` 回调、`pre_execute` 或 `post_execute`。`CollOpState` 是纯数据游标，不是回调。
- `CollPlan(n_channels)` 构造时创建 `ChannelPlan[0..N-1]` 并初始化 `channel_id`；planner 不重复赋值。
- `PlanTask`、`ChannelPlan`、`CollPlan` 在 `include/types.h`：改字段必须同步 planner 与 executor。

### 设计区间

- 启用 channel 的规则可调：当前 `src/planner.cpp` 每 channel 最少 `64 KiB`，使用数 `clamp(total_bytes / 64KiB, 1, comm.GetNChannels())`。小消息只触发单 channel。
- `CollTask` 可扩展新 op（不只 AllReduce）。
- 每个 `ChannelPlan` 的 `PlanTask` 数量可扩展（当前仅一条）。

## 文件介绍

| 文件 | 职责 |
|------|------|
| `include/planner.h` / `src/planner.cpp` | `CollTask` → `CollPlan` 切片与组装 |
| `include/types.h` | 核心数据模型：`CommConfig`、`NodeInfo`、`CollTask`、`CollEvent`（`Readable`/`Writable`）、`CollOpState`、`PlanTask`（含 `CollOpState state`）、`ChannelPlan`、`CollPlan` |

## 修改原则

- 勿回退：plan 无行为铁律（不得引入回调）。
- 改 `types.h` 字段必须同步 planner 与 executor 两者。
- planner 只切片 + 组装任务，不得展开算法步骤。
- 数据模型新字段须先确认必要性，保持 `CollOpState` 只存不可现算的最小状态。
