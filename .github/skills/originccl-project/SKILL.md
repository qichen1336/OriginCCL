---
name: originccl-project
description: 'OriginCCL：C++17 集合通信项目，实现 TCP transport、Ring AllReduce、CollTask→Planner→CollPlan→Executor 分层。主 skill 存目录与通用指导；各层细节按需加载 reference/ 下对应知识库。当用户需要构建、测试、排障或修改 OriginCCL（bootstrap、planner、executor、communicator、topology_ring、transport_tcp）时使用。关键词：OriginCCL、AllReduce、Ring、集合通信、执行计划、executor、epoll、channel、fmt、mpirun、OpenMPI、TCP、bootstrap。'
---

# OriginCCL 项目 Skill（主入口 / 目录）

C++17 集合通信库，单机多进程 TCP AllReduce，参考 NCCL 分 bootstrap / 数据面两步建连。本 skill 随仓库分发，clone 后即可用。路径均相对仓库根。

## 知识库结构（按需加载，勿一次全读）

通用指导在本文件；**某一层的接口契约与不变式在 `reference/` 下对应文件**，改哪一层读哪一层：

| 要改动的层 / 任务 | 加载文件 |
|------|------|
| Transport 非阻塞收发、GetFd | [reference/transport.md](reference/transport.md) |
| Topology 状态机、算法、CollOpState | [reference/topology.md](reference/topology.md) |
| Executor（多线程/epoll/轮询）、事件驱动 | [reference/executor.md](reference/executor.md) |
| Planner、PlanTask/CollPlan 数据模型 | [reference/planner.md](reference/planner.md) |
| Communicator 初始化、建连、生命周期、Bootstrap | [reference/communicator.md](reference/communicator.md) |

## 架构总览

```mermaid
flowchart LR
    A[AllReduce API: CollTask] --> B[Planner::Plan]
    B --> C[CollPlan]
    C --> D[ChannelPlan 0..N-1]
    D --> E[PlanTask: CollFunc + Topology + CollOpState]
    E --> F[Executor 编译期选定]
    F --> G[驱动 Topology::AllreduceStep 推进算法]
```

两条独立链路：**执行**（上图）与**初始化**（`Communicator::Init` → TopologyRing → Bootstrap → InitChannels）。

核心分层原则：
- **Topology 只做事件处理**（`AllreduceInit`/`AllreduceStep`/`AllreduceDone`/`AllreduceSucceeded`，非阻塞），**Executor 只决定如何等待 socket 就绪**。
- 三种 executor 编译期选定：`MultiThreadExecutor`（默认）、`EpollExecutor`、`PollingExecutor`。

## 使用后必须维护本 Skill

1. 改动架构边界、核心类型、文件名、调用链、构建命令、测试方式或已验证环境时，更新**对应层**的 reference 文件；跨层或构建/测试/排障变化改本文件。
2. 只记录通过源码检查或实际命令验证的事实，不记录未实现的设计提议。
3. 删除被新实现取代的旧描述，避免互相矛盾。
4. 保持精炼；优先更新既有条目，不追加流水账。

## 构建

```bash
cmake -S . -B build                          # 默认 multi_thread executor
cmake -S . -B build-epoll -DOCCL_EXECUTOR=epoll
cmake -S . -B build-polling -DOCCL_EXECUTOR=polling
cmake --build build -j"$(nproc)"
```

- 依赖 **fmt（必须 ≥9，logger.h 用了 `fmt::format_string` 编译期检查）**，10.2.1 已验证。
- fmt 与 OpenMPI 是**系统依赖**：需预装并暴露 CMake package（`find_package(fmt REQUIRED)`），不 vendor。
- **OpenMPI 需预装**才能跑 mpirun 测试。
- `OCCL_EXECUTOR` 取值非法时 cmake 配置即 `FATAL_ERROR`。
- 产物：`build/liboriginccl.so`、`build/tests/test_allreduce`。

## 运行测试

```bash
scripts/run_all_executors.sh    # 三种 executor × 1/2/4 rank 全矩阵（推荐）
scripts/run_tests.sh --executor epoll --build-dir build-epoll   # 单种 executor
mpirun -np 2 build/tests/test_allreduce
mpirun -np 4 build/tests/test_allreduce
build/tests/test_allreduce <rank> <world_size>   # 手动模式，rank0 先启动做 master
```

- 测试显式配置 4 channels，先小消息触发 1 channel，再 `65536` 个 float 触发 4 channels。
- 独立 send/recv buffer 覆盖 out-of-place copy；两轮验证 SUM 与 AVG。rank i 填 `i+1`，SUM=`ws*(ws+1)/2`，AVG=`(ws+1)/2`。
- 单进程 `0 1` 走无数据面路径（task 立即完成，epoll 不注册 fd）。
- 测试只兼容 Open MPI（读 `OMPI_COMM_WORLD_RANK/SIZE`），否则回落 `argv[1]`=rank、`argv[2]`=world_size。

架构或并发改动后的最低验证矩阵：

```bash
scripts/run_all_executors.sh
git diff --check
```

## 开发机环境事实（2026-09 记录，随机器而异）

- 本机：chenqi-VirtualBox，Ubuntu jammy（22.04），GCC 11.4，apt 源内网镜像 rdsource.tp-link.com。
- 已装 OpenMPI 4.1.2（`openmpi-bin` + `libopenmpi-dev`）。fmt 10.2.1 源码装到 `/usr/local`（jammy 的 `libfmt-dev` 仅 8.1.1 过旧）。
- sudo 需密码（不能免密）；装系统包要提示用户在终端输入，不经模型中转。
- 迁移新机器：`git clone` 后预装 fmt ≥9 与 OpenMPI；jammy 需 fmt 10.2.1 源码装到 `/usr/local`。

## 常见排障

| 症状 | 原因 / 处理 |
|------|------|
| `Could not find a package configuration file provided by "fmt"` | 系统未装 fmt 或版本过低 → fmt 10.2.1 源码装到 `/usr/local` |
| `make run_test_allreduce` 不存在 | cmake 配置时 PATH 无 mpirun → 装 openmpi-bin 后重新 `cmake ..` |
| 多进程跑起来 hang | rank0(master) 未先启动，或 bootstrap 端口（默认 12345）被占用 |
| mpirun 下 `OMPI_COMM_WORLD_RANK` 读不到 | 用了非 Open MPI 启动器（只兼容 Open MPI） |
| 换机器/fmt 版本后编译错 | fmt 版本过低（需 ≥9） |
| 小消息看不到多 channel 并发 | planner 每 channel 阈值 64 KiB；用至少 `n_channels*64KiB` 数据量 |
| 懒加载扩容后重复执行旧 plan | 新 worker 必须用当前 `batch_id_` 初始化 `completed_batch_id` |
| Finalize 卡住或访问关闭 socket | 必须先 `executor->Shutdown()`，再关闭/reset channel transports |
| 2 rank 死锁（send/recv 互等） | executor 一次等待返回的多个就绪事件没全部喂 `AllreduceStep` → 见 [reference/executor.md](reference/executor.md) |
| epoll `ADD ... File exists` | task 完成时没 `EPOLL_CTL_DEL`，fd 跨 plan 复用 → 见 [reference/executor.md](reference/executor.md) |
