# Communicator 层（`include/communicator.h`、`src/communicator.cpp`、`src/bootstrap.cpp`）

进程内集合通信的顶层对象：初始化建连 + 发起 AllReduce + 生命周期管理。改动初始化/建连/生命周期前阅读本文件。

## 初始化链路

```mermaid
flowchart LR
    A[Communicator::Init] --> B[TopologyRing]
    B --> C[Bootstrap 交换 NodeInfo]
    C --> D[InitChannels]
    D --> E[每 channel 建立独立 send/recv TCP transport]
```

- **双端口分工**：`master_port`（默认与测试均 12345）只用于 bootstrap 握手；数据面每个进程临时 `Listen(0)` 取随机空闲 `data_port` 再广播。
- **连边方向**：每 channel 连 prev/next 两条边；按 `rank < peer` 决定主动 connect、否则被动 accept（`InitChannels` 附近），避免启动死锁。**改这个方向会重新引入死锁**。
- `n_channels` 默认 4（`config.n_channels<=0` 时取 `DefaultChannelCount()`）；初始化建全部 channel 连接，planner 按消息大小选用本次实际数量。
- `world_size<=1` 时跳过数据面建连，并就地赋 local 默认值（见下）。

## Local rank 视图

`Communicator` 暴露本机视角：`GetLocalRank()`、`GetLocalSize()`、`GetLocalRanks()`、`IsSingleMachine()`。

- **机器身份 = hostname**（`Utils::GetHostname()` → `gethostname()`），不是 IP：IP 有 `127.0.0.1` 特判且属数据面语义，hostname 与网络无关、单机多进程天然一致。
- `NodeInfo` 携带 `hostname`（`EncodeNodeInfo`/`DecodeNodeInfo` 同步编解码），master 与 worker 各自在 bootstrap 时填入 `Utils::GetHostname()`，随节点信息广播给所有人。
- `Init` 在 bootstrap 成功后**内联**分组（不抽纯函数）：取 `all_nodes[config.rank].hostname`，收集 hostname 相同的 rank、升序排序得 `local_ranks`；`local_size = local_ranks.size()`；`local_rank` = 自身 rank 在该向量中的下标；`is_single_machine = (local_size == world_size)`。
- `world_size<=1` 提前返回路径赋默认值 `local_rank=0, local_size=1, local_ranks={0}, is_single_machine=true`——这是唯一拿不到 `all_nodes` 的分支。


## 执行链路

`Communicator::AllReduce` 创建 `CollTask` → `planner.Plan` → `executor->Run(plan)`。

- executor 由编译宏选定（见 [executor.md](executor.md)），`Communicator` 用 `#ifdef` 构造，无运行时注入接口。

## 生命周期（顺序是死锁/崩溃防线）

`Communicator::Finalize` 必须先 `executor->Shutdown()`（多线程 executor 还需 join worker），**再**关闭 channel transports，防止使用已销毁 socket。`PlanTask` 通过 `shared_ptr<Transport>` 保证 transport 在 plan 执行期间有效。

## Bootstrap（`src/bootstrap.cpp`）

rank0 为 master 监听 `config.master_port`，收齐各 rank 的 `NodeInfo`(rank/ip/hostname/data_port) 后广播给所有人。

## 文件

| 文件 | 职责 |
|------|------|
| `src/communicator.cpp` | `Init`（选 data_port → bootstrap → local 分组 → InitChannels）+ `#ifdef` 构造 executor + `Finalize` 顺序 |
| `src/bootstrap.cpp` | master/节点信息交换（含 hostname 采集） |
