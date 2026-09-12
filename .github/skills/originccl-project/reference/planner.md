# Planner 与数据模型（`src/planner.cpp`、`include/types.h`、`include/planner.h`）

把外层 collective API 转成按 channel 分组的可执行计划。改动 plan 类型或 planner 前阅读本文件。

## API Task

`CollTask` 表达外层 collective API 语义（目前为 AllReduce）：`func`、`send_buf`/`recv_buf`、`count`/`dtype`、`ReduceOp`。调用方不关心 worker 数、线程池、Ring step 或 channel 调度。

## `Planner::Plan(Communicator&, const CollTask&)` → `CollPlan`

- tensor 按使用中的 channel 数切片。
- `CollPlan(n_channels)` 构造时创建 `ChannelPlan[0..N-1]` 并初始化 `channel_id`；planner 不重复赋值。
- 每个 `ChannelPlan` 当前只含一条 `PlanTask`，携带 `CollFunc` 与 `shared_ptr<Topology>`。
- `PlanTask` 显式携带该 slice 的 send/recv 地址、元素数、dtype、reduce op、rank/world size、topology 指针、对应 channel 的 send/recv transport。
- `PlanTask.state`（`CollOpState`）见 [topology.md](topology.md)——planner 只值初始化，不展开算法阶段。
- `PlanTask.topology` 复用 `comm.GetTopology()`；planner 不新建拓扑。

## plan 无行为铁律

plan 不使用 `std::function`、`execute` 回调、`pre_execute` 或 `post_execute`。`CollOpState` 是纯数据游标，不是回调。

## 启用 channel 的规则

`src/planner.cpp`：每 channel 最少 `64 KiB`，使用数 `clamp(total_bytes / 64KiB, 1, comm.GetNChannels())`。小消息只触发单 channel。

## 核心数据模型（`include/types.h`）

`CommConfig`、`NodeInfo`、`CollTask`、`CollEvent`（`Readable`/`Writable`）、`CollOpState`、`PlanTask`（含 `CollOpState state`）、`ChannelPlan`、`CollPlan`。
