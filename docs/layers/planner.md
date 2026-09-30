# Planner 与数据模型（`src/planner.cpp`、`include/types.h`、`include/planner.h`）

把外层 collective API 转成按 channel 分组的可执行计划。改动 plan 类型或 planner 前阅读本文件。

# 核心职责边界

- `CollTask` 表达五种 collective 的 API 语义：`func`、`send_buf`/`recv_buf`、`count`/`dtype`、`ReduceOp`、`root`。无根操作忽略 root，非归约操作忽略 op。
- `Planner::Plan(Communicator&, const CollTask&)` → `CollPlan`：按 count 代表的元素区间切片。多 rank 块布局下，每个 channel 处理每个块的相同子区间，而非切整个 `count*world_size` 缓冲区。
- **拓扑选择**：planner 每次调用按 `func` + 数据量选拓扑挂到 `PlanTask.topology`。Broadcast / Reduce 永远 ring；AllReduce / ReduceScatter / AllGather 在 `count * type_size / kChunkBytes < kTreeThresholdChunks` 时走 tree，否则 ring（见 [occl_config.h](../../include/occl_config.h) 的 `kTreeThresholdChunks`）。阈值以 chunk 为单位，`OCCL_SMALL_TESTS` 同除 256 后选择不变。
- `PlanTask` 显式携带该 slice 的 send/recv 地址、元素数、dtype、reduce op、rank/world size、root、rank_stride、topology 指针、**对应 channel 的 send/recv transport 向量**。
- 两个 transport 向量由 `Topology::FillTransports` 填充：planner 只构造 `PlanTask`（先设 `topology`）再调它，**不自己取 `Channel::ring` / `Connector`**。哪条 `Connector` 属于本 task 是拓扑语义；`FillTransports` 过滤 null 后输出「该 task 真正要用的全部连接」。
- 不负责：不展开算法步骤（topology）、不决定等待策略（executor）。

# 文件介绍

| 文件 | 职责 |
|------|------|
| `include/planner.h` / `src/planner.cpp` | `CollTask` → `CollPlan` 切片与组装；transport 向量经 `Topology::FillTransports` 填充 |
| `include/types.h` | 核心数据模型：`CommConfig`、`NodeInfo`、`CollTask`、`CollEvent`、`CollOpState`、`PlanTask`（含 transport 向量与 `state`）、`ChannelPlan`、`CollPlan` |

# 实现原理

- `PlanTask.chunk_size` 语义按拓扑解释：ring = `ceil(elem_count / world_size)`（AllReduce 块划分）；tree = `kChunkBytes / type_size`（流水粒度，逐 chunk 推进）。
- `rank_stride` 保存原始 `CollTask.count`（元素数）；非空 send/recv 基址偏移 `offset*type_size`，null 原样保留。topology 用 `block_rank*rank_stride` 找下一 rank 块，不能用 channel 的 elem_count 代替跨度。
- `PlanTask.state`（`CollOpState`）见 [topology.md](topology.md)——planner 只值初始化，不展开算法阶段。
- `PlanTask.topology` 复用 communicator 的 `GetRingTopology()`/`GetTreeTopology()`，planner 不新建拓扑。
- 启用 channel 规则：AllReduce 每 channel 最小颗粒度 `OcclConfig::kChunkBytes × world_size`，其余操作 `kChunkBytes`（合称 `unit`）；使用数 `n_used = min(total_bytes / unit, GetNChannels())`，小消息只触发单 channel。`total_bytes = count*type_size`，多块操作也按单块大小选通道。切分按 unit 对齐：每 channel 分 `⌊units_total/n_used⌋` 或 `⌈` 个 unit，`total_bytes % unit` 的余数并入最后一个 channel。count=0 仍生成一个立即完成的任务。
- tree 下 AllGather / ReduceScatter 强制单 channel（`channel_cap = 1`），因它们的块布局跨度 `rank_stride = count`，切多 channel 会破坏「每 rank 一整块」的对齐；AllReduce 仍可按 unit 多 channel 切。
- `CollPlan(n_channels)` 构造时创建 `ChannelPlan[0..N-1]` 并初始化 `channel_id`，planner 不重复赋值。

# 隐含约定

- **chunk 颗粒度是编译期常量**：`OcclConfig::kChunkBytes`（`include/occl_config.h`）定义在 `32 KiB / kTestScale`，`kTestScale` 由 `OCCL_SMALL_TESTS` 决定（OFF→1，ON→256）。宏只等比缩放 unit 与阻塞用例的 count，`units_total`、`n_used` 与每 channel 切分完全不变，因此覆盖的 planner 路径等价，但内存降 256 倍。`kTreeThresholdChunks`（tree/ring 阈值，单位 chunk）同样不缩放，故 tree/ring 选择在两种模式下不变。
- **plan 无行为铁律**：plan 不使用 `std::function`、`execute` 回调、`pre_execute`/`post_execute`。`CollOpState` 是纯数据游标，不是回调；游标只被 executor 推进，拓扑不越权。
- **改 `types.h` 字段必须同步 planner 与 executor 两者**。`PlanTask::send_transports` / `recv_transports`（及 `CollOpState` 的 progress/done 向量）是与连接一一对应的向量，`Try*` 保持单连接语义，多连接遍历在 topology（推进）与 executor（就绪注册）两侧。
- planner 只切片 + 组装任务，不得展开算法步骤；也不得直接读 `Channel` 的 `ring`/`Connector`——一律经 `Topology::FillTransports`。
