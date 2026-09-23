# Communicator 层（`include/communicator.h`、`src/communicator.cpp`、`src/bootstrap.cpp`）

进程内集合通信的顶层对象：初始化建连 + 发起 AllReduce + 生命周期管理。改动初始化/建连/生命周期前阅读本文件。

## 核心职责边界

- 初始化链路：

```mermaid
flowchart LR
    A[Communicator::Init] --> B[TopologyRing]
    B --> L[建数据面 TCP listener + 共享内存 rendezvous listener + RDMA listener]
    L --> C[Bootstrap 交换 NodeInfo（含 rdma_addr/rdma_port）]
    C --> P[local 分组 + 按 local_rank 绑核]
    P --> S[全局判定：所有 rank 都有可用 RDMA 才启用 RDMA]
    S --> D[InitChannels]
    D --> E[每 channel 建立独立 send/recv transport：本机 edge 走共享内存，跨机 edge 走 RDMA 或 TCP]
```

- 执行链路：`Communicator::AllReduce` 创建 `CollTask` → `planner.Plan` → `executor->Run(plan)`。
- 暴露本机视角：`GetLocalRank()`、`GetLocalSize()`、`GetLocalRanks()`、`IsSingleMachine()`。
- 不负责：不决定算法（topology）、不决定等待策略（executor）、不切片（planner）、不实现共享内存环（transport）。

## 不变式与设计区间

### 不变式

- **unique id 只由 rank0 生成，其余 rank 从外部拿到**：`Communicator::GetUniqueId(UniqueId&)` 只 `CreateListenSocket(0)` 绑一个空闲端口（fd 记在 `bootstrap_listen_fd`，一直持到 `Finalize`），把绑定端口与本机 IP 一起装进 `UniqueId` 返回；它不做任何通信，把 id 分发给其余 rank 是启动方的责任（`tests/` 里用 `MPI_Bcast` 播原始字节，所以 `UniqueId` 是定长 trivially-copyable 结构）。所有 rank 用**同一个 id** 调 `Init`（rank0 用的是自己刚生成的那份），ip/port 都从 `config.unique_id` 读，不再有 `master_addr` / `master_port` 配置项。id 里没别的状态，`world_size<=1` 时 rank0 照样能生成但没人用它。rank0 之外调 `GetUniqueId` 是错的（白绑一个没人连的端口），据此不做「非 rank0」校验；同一对象重复调用则明确报错，避免悄悄漏掉上一个 fd。
- **两个 listener 都在 bootstrap 前绑好并一直持有**：bootstrap listener 就是 `GetUniqueId` 里 `CreateListenSocket(0)` 的那个 fd，`Bootstrap::RunMaster` 只借用不关闭（绑/关都归 communicator），所以 id 公布出去的端口不可能在首次 accept 前被抢走 —— 不能退回「先取空闲端口 → 关闭 → 之后重绑」。数据面 TCP listener 同理：bootstrap 之前 `Listen(0)` 建好并一直持有（`NodeInfo.data_port` 就是它对应的端口），`InitChannels` 全部 edge 建完才 `Close()`；全局选定 RDMA 后 TCP listener 才被关闭（它的端口只用于广播，不再 listen）。RDMA listener 同样在 bootstrap 前 `ListenAddr(rdma_addr, 0)` 建好，用于广播能力与接收连接；若全局判定不用 RDMA，则随即 `Close()`。共享内存端在 bootstrap 前 `ListenPath()` 绑 `/tmp/originccl/<port>-<rank>.sock`（`<port>` 取自 unique id，路径含 port 与 rank，互不冲突；绑定前 `unlink` 旧路径，关闭时 `unlink`），目录 `/tmp/originccl` 由 `Init` 创建，创建失败即初始化失败；保证 active edge 开工时对端 path 一定在 listening。
- **连边方向**：每 channel 连 prev/next 两条边；按 `rank < peer` 决定主动 connect、否则被动 accept（`InitChannels` 附近），避免启动死锁。**改这个方向会重新引入死锁**。传输选择不影响这个方向判定。
- **主动边对 `Connect` 做最多 5 次重试**（TCP/SHM/RDMA 一致）：bootstrap 是全体参与的同步交换，而所有 listener 都在 bootstrap **之前**绑好，所以任一 rank 从 bootstrap 返回时其余 rank 必然已在 listen，主动端理论上不可能撞上「对端还没监听」。尽管如此仍做防御性重试——`ConnectActiveEdges` 对每条边最多试 `kConnectRetryCount`（5）次、间隔 `kConnectRetryIntervalMs`（100ms），每次失败先 `transport->Close()` 复位再重试（RDMA 靠 `Close()` 清理残留的 `cm_id`/`cm_channel` 才能干净重连；TCP 重新 `socket`，SHM 重新 `CreateRing`），5 次耗尽才 `LOG_ERROR` + 初始化失败。RDMA 自身另有 CM 的 `retry_count`/`rnr_retry_count` 承担链路层超时与重试。
- **channel 边是有向的，每条边一个独立 transport**（这是贯穿全库的关键隐含保证：同一 channel 的 send/recv 必是不同 fd）：`Channel::send` / `Channel::recv` 字段虽对称，实践中却从不双向复用 —— `send[p]` 只发送、`recv[p]` 只接收，二者是**两条有向边**、两个 `Connector`，各自持有专用的 `Transport`（接口本身仍是双向的）。`InitChannels` 把每个 channel 展开成 `(peer=next, is_send=true)` 与 `(peer=prev, is_send=false)` 两条独立边，分别走主动 connect / 被动 accept，所以 `send[p]` 与 `recv[p]` 是不同对象、不同 fd，永不共享。2 rank 时环退化为 `prev == next`，同一对 rank 之间**仍是两条独立边、两个 transport**，不因 peer 相同而合并。推论：一条 channel 边只需为单一方向备妥就绪，等待方（executor）不会把同一个对象当成双向的用，也不为 fd 相同做防御合并。
- **edge 传输选择 = hostname 相等 + 全局 RDMA 能力**：只有 `OCCL_DISABLE_SHM` **恰好等于 `"1"`** 时才强制不用共享内存（`0`、`yes`、空串、未设置都保持共享内存）。启用共享内存时，`peer hostname == 本机 hostname` 的 edge 用 `TransportShm`（active 端 `SetDirection()` 后 `Connect(rendezvous_path, 0)`，方向必须在 connect 前设，它决定拿环的哪一半）。
  - 网络 edge（跨机）不是按边协商，而是**全局决策**：每个 rank 在 bootstrap 前用 `TransportRDMA::Probe` + `ListenAddr` 探测并绑定自己的 RDMA 端点，把 `rdma_addr`/`rdma_port`（`rdma_port==0` 表示不支持）通过 `NodeInfo` 广播；所有 rank 对同一份 `all_nodes` 计算 `all_of(rdma_port != 0)`，**只有全体都支持才把网络数据面统一用 RDMA**，否则全体用 TCP。这个全局性保证了不可能出现「一半边 RDMA、一半边 TCP」的错配，也就不需要按边协商。
  - `OCCL_DISABLE_RDMA=1` 运行时关闭 RDMA，等价于本 rank 宣布不支持，从而通过上述全局规则把全集群网络数据面拉到 TCP。它是**选择**而不是回退：一旦全局选定 RDMA，建链/QP/MR 的错误直接让初始化失败（`LOG_ERROR` + `false`），不静默改走 TCP。
  - 被动端用同一规则（`handshake.rank` → `all_nodes[rank]`）判定，所以一条 edge 两端判定必然一致——这是隐含保证，据此不做「两端传输类型不一致」的协商/校验。选择只发生在 communicator，planner/topology/channel/executor 都不知道传输类型。
- **共享内存失败即初始化失败**：listener / 连接 / 映射 / 传描述符 / 握手任一失败都 `LOG_ERROR` + `false`，**没有自动回退 TCP**（`OCCL_DISABLE_SHM=1` 是显式选择，不是回退）。主路径优先：失败即停，宁可初始化失败也不悄悄换一条降级路径。
- **被动端单 `poll()` 循环**：TCP 与 rendezvous 两个 listener 用一个 `poll()` 循环等（掩码取 `Transport::GetPollEvents()`，不是硬编码），谁就绪就 `Accept()`，再走共用握手把 transport 装到对应 connector；不需要独立 accept 线程，也不看 executor 类型。该函数只需要 listener 列表与条数，不需要 `all_nodes`（对端 rank 的数组索引交给 `FindConnector` 自己越界检查）。
- **listener 生命周期**：channel init 完成后两个 listener 都 `Close()`（rendezvous 路径被 `unlink`），临时 control socket 由 transport 自己在握手后关闭；已建立的 channel transport 活到 `Finalize`。
- **生命周期顺序（死锁/崩溃防线）**：`Finalize` 必须先 `executor->Shutdown()`（多线程 executor 还需 join worker），**再**关闭 channel transports。`PlanTask` 通过 `shared_ptr<Transport>` 保证 transport 在 plan 执行期间有效。
- **机器身份 = hostname**（`Utils::GetHostname()` → `gethostname()`），不是 IP：IP 有 `127.0.0.1` 特判且属数据面语义，hostname 与网络无关、单机多进程天然一致——这是隐含保证，据此不做「同 IP」这类需要特判的防御。`NodeInfo` 携带 `hostname`、`ip_addr`/`data_port` 与 `rdma_addr`/`rdma_port`（`EncodeNodeInfo`/`DecodeNodeInfo` 同步编解码），各 rank 在 bootstrap 时填入自己探测到的值（`CommConfig::get_hostname()` 默认指向 `Utils::GetHostname`，测试可注入按 rank 推导的假值以在单机模拟多机；正常路径仍是真实 hostname）。

### 设计区间

- `n_channels` 默认值可调：`config.n_channels<=0` 时取 `kDefaultChannelCount`（当前 4）；初始化建全部 channel 连接，planner 按消息大小选用本次实际数量。
- 初始化内部步骤可重构，守住生命周期顺序即可。
- local 分组可内联实现（现已在 `Init` 内联，不抽纯函数）：取 `all_nodes[config.rank].hostname`，收集 hostname 相同的 rank、升序排序得 `local_ranks`；`local_size = local_ranks.size()`；`local_rank` = 自身 rank 下标；`is_single_machine = (local_size == world_size)`。`world_size<=1` 提前返回（此时**不建任何 listener**，也不装任何 edge）赋 `local_rank=0, local_size=1, local_ranks={0}, is_single_machine=true`（唯一拿不到 `all_nodes` 的分支）。
- **绑核用 local_rank，不是全局 rank**：`Init` 在两处调 `Utils::PinProcessToCpu(local_rank)`（`world_size<=1` 的提前返回分支与正常 local 分组之后），把进程亲和性设为单核 `local_rank % 在线 CPU 数`。原语只做 `sysconf(_SC_NPROCESSORS_ONLN)` + `sched_setaffinity`（`CPU_SET` 单核），**非致命**：取不到 CPU 数或 `sched_setaffinity` 失败只 `LOG_WARN` 并继续初始化（绑核是性能优化，不是正确性前提，失败不得让 Init 返回 false）。取模而非报错，是为了让一台机器上 local rank 多于 CPU 数的超订场景仍能跑（多个 rank 共享核）。绑核在 bootstrap 之后（此时才知道 `local_rank`），且只执行一次；executor 的 worker 线程继承进程亲和性，不加额外 per-thread 绑定。
- executor 由编译宏选定（见 [executor.md](executor.md)），`Communicator` 用 `#ifdef` 构造，无运行时注入接口。

## 文件介绍

| 文件 | 职责 |
|------|------|
| `include/communicator.h` | `Communicator` 顶层接口（GetUniqueId / Init / AllReduce / Finalize / local 视图） |
| `include/channel.h` | `Channel` / `Connector` / `Ring` 结构：`send[p]` / `recv[p]` 是两条有向边的两个槽位，`ring` 由 topology 填（`FillChannels`），transport 由 `InitChannels` 装 |
| `src/communicator.cpp` | `GetUniqueId`（绑 bootstrap listener）+ `Init`（探 RDMA 端点 → 建三类 listener → bootstrap → local 分组 → 绑核 → 全局 RDMA 判定 → InitChannels）+ `#ifdef` 构造 executor + `ConnectActiveEdges` 内联的按边三选一（SHM/RDMA/TCP，各设方向后单次 `Connect`）+ `Finalize` 顺序 |
| `src/bootstrap.cpp` | master/节点信息交换（按 `NodeInfo` 交换 hostname 与 RDMA 端点） |

## 修改原则

- 勿回退：连边方向 `rank < peer`（改方向重引入启动死锁）。
- 勿回退：TCP listener 在 bootstrap 前建好并保留到 channel init 结束（「先取端口再关闭重绑」是被消除的竞态）。
- 勿回退：主动边不做连接重试。listener 已先绑好，重试永不触发；重加它就是把已消除的竞态假设重新编回来。
- 勿回退：`Finalize` 先 `Shutdown()` 再关 transport。
- 勿新增共享内存 → TCP 的自动回退；失败必须让初始化失败。RDMA 同理：全局选定 RDMA 后建链失败也必须让初始化失败。
- 勿回退：RDMA 是全局决策（所有 rank 都支持才用），不是按边协商。改成按边会引入两端选择不一致的风险。
- 机器身份用 hostname，不要改回 IP。
- 绑核保持非致命且按 `local_rank` 取模：失败只 `LOG_WARN`（不能让 Init 失败），不要改成全局 rank 或硬编码 CPU 数。
- 改动传输选择后跑 `scripts/run_tests.sh --transport all`（或 `run_all_executors.sh`）：tier 5 与 tier 8 把 rendezvous 目录 `/tmp/originccl` 用普通文件占住（端口由 rank0 运行时挑，脚本猜不到具体路径，只能占目录），分别要求「共享内存开启时初始化失败（不回退）」与「共享内存被禁用时照常成功」；tier 9/10 用假 hostname 断言每条边上的具体传输类型（纯 RDMA / SHM+RDMA），tier 12 断言 `OCCL_DISABLE_RDMA=1` 时网络边全是 TCP。
- Bootstrap（`src/bootstrap.cpp`）：rank0 为 master 在借来的 bootstrap listener（`config.unique_id.port`）上收齐各 rank 的 `NodeInfo` 后广播给所有人；worker 连 `config.unique_id.ip_addr:port`。调用方（communicator）把已构造好的本地 `NodeInfo` 传给 `Bootstrap::Run`，避免在 bootstrap 里重复一份 IP/hostname 采集逻辑。
- 多机行为单机验证：`tests/test_multi_machine.cpp` 经 `CommConfig::get_hostname` 注入假 hostname，并接受 `rdma` / `shm+rdma` / `tcp` 参数断言实际传输类型；`scripts/run_tests.sh` 的 tier 9（np=2 全不同机，纯 RDMA）、tier 10（np=4 每机 2 rank，混合 SHM+RDMA）与 tier 12（`OCCL_DISABLE_RDMA=1`，全 TCP）跑它，既断言 local 视图与端到端 AllReduce，也断言每条边的 transport 具体类型。
