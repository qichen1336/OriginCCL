# Communicator 层（`include/communicator.h`、`src/communicator.cpp`、`src/bootstrap.cpp`）

进程内集合通信的顶层对象：初始化建连 + 发起 AllReduce + 生命周期管理。改动初始化/建连/生命周期前阅读本文件。

## 核心职责边界

- 初始化链路：

```mermaid
flowchart LR
    A[Communicator::Init] --> B[TopologyRing]
    B --> C[Bootstrap 交换 NodeInfo]
    C --> D[InitChannels]
    D --> E[逐 edge 选路：同机 SHM / 跳机 TCP]
```

- 执行链路：`Communicator::AllReduce` 创建 `CollTask` → `planner.Plan` → `executor->Run(plan)`。
- 暴露本机视角：`GetLocalRank()`、`GetLocalSize()`、`GetLocalRanks()`、`IsSingleMachine()`。
- 不负责：不决定算法（topology）、不决定等待策略（executor）、不切片（planner）、不实现传输细节（transport）。
- 选路规则：逐 edge 比较 `all_nodes[rank].hostname` 与 `all_nodes[peer].hostname`，相同则给该 edge 建 `TransportSHM`，否则建 `TransportTCP`；`OCCL_DISABLE_SHM=1` 时全部走 TCP。

## 不变式与设计区间

### 不变式

- **双端口分工**：`master_port`（默认与测试均 12345）只用于 bootstrap 握手；数据面每个进程 `Listen(0)` 取随机空闲 `data_port` 再广播。
- **listener 是端口租约**：`Init` 取到 `data_port` 的临时 TCP listener 不再提前关闭，而是**保留到 channel 初始化结束**，既照看跨机被动 TCP edge，又保证该端口在初始化期间不被其它进程占用——同机共享内存 rendezvous 名由 `data_port` 派生，靠这份租约保证唯一。
- **每个 rank 一个监听 socket**：TCP 被动 edge 用保留的 listener；共享内存被动 edge 另建 `TransportSHM` 监听同一 `data_port`（abstract UDS 名与 TCP 端口互不冲突）。两类 accept 各跑一个线程。
- **选路一致性靠配置而非协商**：`OCCL_DISABLE_SHM` 按进程读取，不做逐 edge 协商，混用会让两端选路不同；同机边共享内存初始化失败即 `Init` 失败，不回退 TCP。
- **连边方向**：每 channel 连 prev/next 两条边；按 `rank < peer` 决定主动 connect、否则被动 accept（`InitChannels` 附近），避免启动死锁。**改这个方向会重新引入死锁**；SHM 与 TCP 共用这条规则。
- **生命周期顺序（死锁/崩溃防线）**：`Finalize` 必须先 `executor->Shutdown()`（多线程 executor 还需 join worker），**再**关闭 channel transports。`PlanTask` 通过 `shared_ptr<Transport>` 保证 transport 在 plan 执行期间有效。
- **机器身份 = hostname**（`Utils::GetHostname()` → `gethostname()`），不是 IP：IP 有 `127.0.0.1` 特判且属数据面语义，hostname 与网络无关、单机多进程天然一致。`NodeInfo` 携带 `hostname`（`EncodeNodeInfo`/`DecodeNodeInfo` 同步编解码），master 与 worker 各自在 bootstrap 时填入 `Utils::GetHostname()`。
- **可观测性**：每条 edge 在建连前打一行 `transport=SHM` / `transport=TCP`；测试脚本靠它断言实际选路，不要静默改成别的措辞。

### 设计区间

- `n_channels` 默认值可调：`config.n_channels<=0` 时取 `kDefaultChannelCount`（当前 4）；初始化建全部 channel 连接，planner 按消息大小选用本次实际数量。
- 初始化内部步骤可重构，守住生命周期顺序即可。
- local 分组可内联实现（现已在 `Init` 内联，不抽纯函数）：取 `all_nodes[config.rank].hostname`，收集 hostname 相同的 rank、升序排序得 `local_ranks`；`local_size = local_ranks.size()`；`local_rank` = 自身 rank 下标；`is_single_machine = (local_size == world_size)`。`world_size<=1` 提前返回赋 `local_rank=0, local_size=1, local_ranks={0}, is_single_machine=true`（唯一拿不到 `all_nodes` 的分支）。
- executor 由编译宏选定（见 [executor.md](executor.md)），`Communicator` 用 `#ifdef` 构造，无运行时注入接口。

## 文件介绍

| 文件 | 职责 |
|------|------|
| `include/communicator.h` | `Communicator` 顶层接口（Init / AllReduce / Finalize / local 视图） |
| `src/communicator.cpp` | `Init`（保留 data_port listener → bootstrap → local 分组 → 逐 edge 选路 + `InitChannels`）+ `#ifdef` 构造 executor + `Finalize` 顺序 |
| `src/bootstrap.cpp` | master/节点信息交换（含 hostname 采集） |

## 修改原则

- 勿回退：连边方向 `rank < peer`（改方向重引入启动死锁）。
- 勿回退：`Finalize` 先 `Shutdown()` 再关 transport。
- 勿回退：`data_port` listener 必须活到 channel 初始化结束（既是跨机 accept 通道，也是同机 rendezvous 名的唯一性来源）。
- 机器身份用 hostname，不要改回 IP。
- 新增传输类型时只需在此按 edge 选构造；不要把传输细节（fd 交接、ring、通知）搬进本层。
- Bootstrap（`src/bootstrap.cpp`）：rank0 为 master 监听 `config.master_port`，收齐各 rank 的 `NodeInfo`(rank/ip/hostname/data_port) 后广播给所有人。
