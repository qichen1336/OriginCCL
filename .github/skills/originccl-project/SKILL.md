---
name: originccl-project
description: 'OriginCCL：C++17 集合通信项目，实现 TCP transport、Ring AllReduce、CollTask→Planner→CollPlan→MultiThreadExecutor 分层与按 channel 懒加载 worker。当用户需要构建、测试、排障或修改 OriginCCL 架构（bootstrap、planner、PlanTask、ChannelPlan、MultiThreadExecutor、communicator、topology_ring、transport_tcp）时使用。关键词：OriginCCL、AllReduce、Ring、集合通信、执行计划、多线程执行器、channel、fmt、mpirun、OpenMPI、TCP、bootstrap。'
---

# OriginCCL 项目架构 / 构建 / 测试 / 排障 Skill

本 skill 随仓库分发，clone 后即对 Copilot 可用。下述路径均相对仓库根。

## 使用后必须维护本 Skill

每次调用本 skill 完成 OriginCCL 任务后，必须执行以下维护动作：

1. 检查本次修改是否改变了架构边界、核心类型、文件名、调用链、构建命令、测试方式或已验证环境。
2. 若有变化，立即更新本文件对应章节；不得只在会话记忆中记录。
3. 只记录通过源码检查或实际命令验证的事实，不记录尚未实现的设计提议。
4. 删除被新实现取代的旧描述，避免同时保留互相矛盾的架构。
5. 保持内容精炼；优先更新既有条目，不追加流水账式变更日志。

## 项目概览

C++17 集合通信库，单机多进程 TCP AllReduce，参考 NCCL 分 bootstrap / 数据面两步建连的设计。

```mermaid
flowchart LR
    A[AllReduce API: CollTask] --> B[Planner::Plan]
    B --> C[CollPlan]
    C --> D[ChannelPlan 0..N-1]
    D --> E[PlanTask: CollFunc + Topology]
    E --> F[MultiThreadExecutor]
    F --> G[每 channel 常驻 worker]
```

初始化链路独立于执行链路：

```mermaid
flowchart LR
    A[Communicator::Init] --> B[TopologyRing]
    B --> C[Bootstrap 交换 NodeInfo]
    C --> D[InitChannels]
    D --> E[每 channel 建立独立 send/recv TCP transport]
```

## 已确认的架构边界

### 1. API Task

`CollTask` 表达外层 collective API 语义，目前为 AllReduce：

- `func`
- `send_buf` / `recv_buf`
- `count` / `dtype`
- `ReduceOp`

调用方不关心 worker 数、线程池、Ring step 或 channel 调度。

### 2. Planner 与可执行计划

`Planner::Plan(Communicator&, const CollTask&)` 将 API task 转成完整的 `CollPlan`：

- tensor 按使用中的 channel 数切片。
- `CollPlan(n_channels)` 构造时创建 `ChannelPlan[0..N-1]` 并初始化 `channel_id`；planner 不重复赋值。
- 每个 `ChannelPlan` 当前只包含一条完整的 `PlanTask`，携带 `CollFunc` 与 `shared_ptr<Topology>`。
- `PlanTask` 显式携带该 slice 的 send/recv 地址、元素数、dtype、reduce op、rank/world size、topology 指针，以及对应 channel 的 send/recv transport。
- plan 不使用 `std::function`、`execute` 回调、`pre_execute` 或 `post_execute`。
- planner 不展开 copy、reduce-scatter step、all-gather step 或 AVG 子任务；这些是 `Topology::AllReduce` 内部阶段。
- `PlanTask.topology` 复用 `comm.GetTopology()` 返回的已初始化拓扑（当前为 `TopologyRing`），planner 不新建拓扑。

启用 channel 的规则在 `src/planner.cpp`：每 channel 最少 `64 KiB`，使用数为 `clamp(total_bytes / 64KiB, 1, comm.GetNChannels())`。

### 3. MultiThreadExecutor

`MultiThreadExecutor::Run(const CollPlan&)` 只消费执行计划：

- 按 `plan.channels` 懒加载 worker，channel `i` 固定由 worker `i` 执行。
- worker 常驻并跨多次 AllReduce 复用。
- executor 从 1 channel 扩容到更多 channel 时，新 worker 以当前 batch id 初始化，不能误执行旧 plan。
- 同一 channel 的 `tasks` 按容器顺序执行，失败后停止该 channel 的后续任务。
- 每批次等待所有已创建 worker 汇合；没有任务的高编号 worker也参与完成计数。
- `ExecuteTask` 只做通用校验后按 `task.func` 分发，`CollFunc::AllReduce` 调用 `task.topology->AllReduce(task)`；算法本身（out-of-place copy、ReduceScatter、AllGather、AVG 后处理）在拓扑实现中。
- 同一 `Communicator` 的多个 AllReduce 由调用方保证不并发；executor 不提供调用级互斥。

### 4. 生命周期

- `Communicator` 直接持有一个默认 `MultiThreadExecutor`，不从构造参数注入 executor。
- `Communicator::AllReduce` 创建 `CollTask`，调用 planner，再执行 `executor.Run(plan)`。
- `Communicator::Finalize` 必须先 `executor.Shutdown()` 并 join worker，再关闭 channel transports，防止 worker 使用已销毁 socket。
- `PlanTask` 通过 `shared_ptr<Transport>` 保持 transport 在 plan 执行期间有效。

### 源码结构

| 文件 | 职责 |
|------|------|
| `src/bootstrap.cpp` | rank0 为 master 监听 `config.master_port`，收齐各 rank 的 `NodeInfo`(rank/ip/data_port) 后广播给所有人 |
| `src/communicator.cpp` | `Init`: 先选随机空闲 data_port → bootstrap 交换节点信息 → 建立 channel 边连接 |
| `src/topology_ring.cpp` | Ring 拓扑：`ring.prev = (rank-1+ws)%ws`, `ring.next = (rank+1)%ws`；`DefaultChannelCount()=4`；实现 `AllReduce`（out-of-place copy → ReduceScatter → AllGather → AVG） |
| `src/planner.cpp` | 把 `CollTask` 编译成按 channel 分组的 `PlanTask`，填充 `func` 与 `topology`（复用 `comm.GetTopology()`） |
| `src/multi_thread_executor.cpp` | 懒加载每 channel worker，按 `task.func` 分发到 `topology->AllReduce`，完成批次同步与错误汇总 |
| `src/transport_tcp.cpp` | TCP socket 封装（Listen/Connect/Send/Recv） |
| `include/types.h` | `CollTask`、`CollFunc`、`PlanTask`、`ChannelPlan`、`CollPlan` 等核心数据模型 |
| `include/multi_thread_executor.h` | 通用多线程执行器接口和线程池状态 |

关键设计点：
- **双端口分工**：`master_port`（当前默认和测试均为 12345）只用于 bootstrap 握手；数据面每个进程临时 `Listen(0)` 获取随机空闲 `data_port` 再广播。
- **连边方向**：每 channel 需连 prev/next 两条边；代码按 `rank < peer` 决定主动 connect、否则被动 accept（`src/communicator.cpp` InitChannels 附近），避免死锁。
- `n_channels` 默认 4（`config.n_channels<=0` 时取 `DefaultChannelCount()`）；初始化时建立全部 channel 连接，planner 按消息大小选择本次实际使用数。
- 不存在 `ring_executor.*`；该旧实现已被 `multi_thread_executor.*` 替代。
- 测试程序从 `OMPI_COMM_WORLD_RANK/SIZE` 读 rank（只兼容 Open MPI，不依赖 MPICH/PMI/Slurm）；没有该环境变量时回落 `argv[1]`=rank、`argv[2]`=world_size。

## 构建

```bash
cmake -S . -B build
cmake --build build -j"$(nproc)"
```

- 依赖 **fmt（必须 ≥9，logger.h 用了 `fmt::format_string` 编译期格式化检查）**，版本 10.2.1 已验证。
- fmt 源码 **vendor 在项目内** `third_party/fmt/`（已 git 管理，clone 即可离线构建，无需系统预装）。
- `CMakeLists.txt` 先查项目内 `third_party/fmt/CMakeLists.txt`，缺失才回落 `find_package(fmt REQUIRED)`（系统没装 fmt 时会失败）。
- **OpenMPI 需预装**才能跑 mpirun 方式测试。
- 产物：`build/liboriginccl.so`、`build/tests/test_allreduce`。

## 运行测试

```bash
cmake --build build --target run_test_allreduce
mpirun -np 2 build/tests/test_allreduce
mpirun -np 4 build/tests/test_allreduce
# 手动模式（无 mpirun 时，需自行并行拉起多个进程，rank0 必须先启动做 master）：
build/tests/test_allreduce <rank> <world_size>
```

- 当前测试显式配置 4 channels，先用小消息触发 1 channel，再用 `65536` 个 float 触发 4 channels，覆盖 worker 懒加载扩容。
- 测试使用独立 send/recv buffer，覆盖 out-of-place copy；连续两轮验证 SUM 和 AVG。
- 验证预期：rank i 填充 `(i+1)`；SUM 为 `world_size*(world_size+1)/2`，AVG 为 `(world_size+1)/2`。
- 单进程 `build/tests/test_allreduce 0 1` 走无数据面连接路径。

架构或并发改动后的最低验证矩阵：

```bash
cmake --build build -j"$(nproc)"
build/tests/test_allreduce 0 1
mpirun -np 2 build/tests/test_allreduce
mpirun -np 4 build/tests/test_allreduce
git diff --check
```

## 开发机环境事实（2026-09 记录，随机器而异）

- 本机：chenqi-VirtualBox，Ubuntu jammy（22.04），GCC 11.4，apt 源为内网镜像 rdsource.tp-link.com。
- 已装 OpenMPI 4.1.2（`openmpi-bin` + `libopenmpi-dev`，`sudo apt-get install`）。
- sudo 需要密码（不能免密）；装系统包要提示用户在终端输入，不可经模型中转。
- 网络：可达外网（GitHub 可下载）。
- 迁移到新机器：`git clone` 后无需装 fmt（vendored），只需装 OpenMPI（`sudo apt-get install -y openmpi-bin libopenmpi-dev`）即可 `cmake .. && make`。

## 常见排障

| 症状 | 原因 / 处理 |
|------|------|
| `cmake ..` 报 `Could not find a package configuration file provided by "fmt"` | 项目内 `third_party/fmt/` 缺失或为空 → 从 fmt 官方 10.2.1 补全该目录（构建需 include/src/support/CMakeLists.txt） |
| `make run_test_allreduce` 不存在 | cmake 配置时 PATH 里没有 mpirun（`find_program` 失败）→ 安装 openmpi-bin 后重新 `cmake ..` |
| 多进程跑起来后卡住/hang | rank0(master) 未先启动，或 bootstrap 端口（默认 12345）被占用；确认所有进程同时拉起 |
| mpirun 下 `OMPI_COMM_WORLD_RANK` 读不到 | 用了非 Open MPI 的启动器（该测试只兼容 Open MPI） |
| 换机器/换 fmt 版本后编译错 | fmt 版本过低（需 ≥9） |
| 小消息看不到多 channel 并发 | planner 的每 channel 阈值为 64 KiB；使用至少 `n_channels * 64 KiB` 的总数据量 |
| 懒加载扩容后重复执行旧 plan | 新 worker 必须用创建时的当前 `batch_id_` 初始化 `completed_batch_id` |
| Finalize 卡住或访问关闭 socket | 必须先 `executor.Shutdown()`，再关闭/reset channel transports |
