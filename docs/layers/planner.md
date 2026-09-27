# Planner 与数据模型（`src/planner.cpp`、`include/types.h`、`include/planner.h`）

把外层 collective API 转成按 channel 分组的可执行计划。改动 plan 类型或 planner 前阅读本文件。

## 核心职责边界

- `CollTask` 表达五种 collective 的 API 语义：`func`、`send_buf`/`recv_buf`、`count`/`dtype`、`ReduceOp`、`root`。无根操作忽略 root，非归约操作忽略 op。
- `Planner::Plan(Communicator&, const CollTask&)` → `CollPlan`：按 count 代表的元素区间切片。对于多 rank 块布局，每个 channel 处理每个块的相同子区间，而非切整个 count*world_size 缓冲区。
- `PlanTask` 显式携带该 slice 的 send/recv 地址、元素数、dtype、reduce op、rank/world size、root、rank_stride、topology 指针、**对应 channel 的 send/recv transport 向量** `send_transports` / `recv_transports`。
- `PlanTask` 的两个 transport 向量由 `Topology::FillTransports(channel, send_out, recv_out)` 填充：planner 只负责构造 `PlanTask`（先设 `topology`）再调它，**不自己取 `Channel::ring` / `Connector`**。哪条 `Connector` 属于本 task（当前 Ring 取 `ring.next` 发送、`ring.prev` 接收）是拓扑语义，归 topology 决定；`Channel` 只是其输入。隐式保证是 `Channel::send`/`recv` 数组中除本拓扑选中的边外 transport 均为空，`FillTransports` 过滤 null 后输出的是「该 task 真正要用的全部连接」。
- `PlanTask.chunk_size`（`ceil(elem_count / world_size)`）由 planner 计算，仅用于 AllReduce 的块划分。
- `rank_stride` 保存原始 `CollTask.count`（元素数）。非空 send/recv 基址偏移 `offset*type_size`，null 原样保留，不做空指针算术。topology 用 `block_rank*rank_stride` 找下一 rank 块，不能用 channel 的 elem_count 代替跨度。
- `PlanTask.state`（`CollOpState`）见 [topology.md](topology.md)——planner 只值初始化，不展开算法阶段。
- `PlanTask.topology` 复用 `comm.GetTopology()`；planner 不新建拓扑。隐式保证是拓扑在 `Init` 已建好且只此一份，据此不做「拓扑是否为空」的防御检查（`FillTransports` 直接经 `topology` 指针调用）。
- 不负责：不展开算法步骤（topology）、不决定等待策略（executor）。

## 不变式与设计区间

### 不变式

- **plan 无行为铁律**：plan 不使用 `std::function`、`execute` 回调、`pre_execute` 或 `post_execute`。`CollOpState` 是纯数据游标，不是回调。隐含保证是游标只被 executor 推进、拓扑不越权，据此不为 plan 注入任何执行回调。
- `CollPlan(n_channels)` 构造时创建 `ChannelPlan[0..N-1]` 并初始化 `channel_id`；planner 不重复赋值。
- `PlanTask`、`ChannelPlan`、`CollPlan` 在 `include/types.h`：改字段必须同步 planner 与 executor。`PlanTask::send_transports` / `recv_transports`（以及 `CollOpState` 的 `send_progress`/`recv_progress`/`send_done`/`recv_done`）是**与连接一一对应的向量**，`Try*` 接口保持「单连接」语义不变，多连接遍历发生在 topology（推进）与 executor（就绪注册）两侧。

### 设计区间

- 启用 channel 的规则可调：当前 `src/planner.cpp` 每 channel 最少 `64 KiB`，使用数 `clamp(total_bytes / 64KiB, 1, comm.GetNChannels())`。小消息只触发单 channel。
- `total_bytes=count*type_size`，多块操作也按单块大小选通道。测试用 49153/65537 个 32 位元素覆盖三/四通道不均匀切分；count=0 仍生成一个立即完成的任务。
- `CollTask` 可扩展新 op（不只 AllReduce）。
- 每个 `ChannelPlan` 的 `PlanTask` 数量可扩展（当前仅一条）。

## 文件介绍

| 文件 | 职责 |
|------|------|
| `include/planner.h` / `src/planner.cpp` | `CollTask` → `CollPlan` 切片与组装；transport 向量经 `Topology::FillTransports` 填充 |
| `include/types.h` | 核心数据模型：`CommConfig`、`NodeInfo`、`CollTask`、`CollEvent`（`Readable`/`Writable`）、`CollOpState`（连接间平行的 progress/done 向量）、`PlanTask`（含多连接 transport 向量与 `CollOpState state`）、`ChannelPlan`、`CollPlan` |

## 修改原则

- 勿回退：plan 无行为铁律（不得引入回调）。
- 改 `types.h` 字段必须同步 planner 与 executor 两者。
- planner 只切片 + 组装任务，不得展开算法步骤；也不得直接读取 `Channel` 的 `ring`/`Connector`——取哪些连接属拓扑语义，一律经 `Topology::FillTransports`。
- 数据模型新字段须先确认必要性，保持 `CollOpState` 只存不可现算的最小状态。
