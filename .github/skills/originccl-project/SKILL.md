---
name: originccl-project
description: 'OriginCCL：C++17 集合通信（collective communication）项目，实现 TCP transport + Ring 拓扑 AllReduce。当用户需要：构建/编译该项目、运行 test_allreduce 多进程测试、排障（cmake、fmt 依赖、mpirun/OpenMPI、bootstrap 握手、TCP data plane 连边）、理解源码结构（bootstrap/planner/ring_executor/communicator/topology_ring/transport_tcp）时使用。关键词：OriginCCL、AllReduce、Ring、集合通信、fmt、mpirun、OpenMPI、TCP、bootstrap。'
---

# OriginCCL 项目构建 / 运行 / 排障 Skill

本 skill 随仓库分发，clone 后即对 Copilot 可用。下述路径均相对仓库根。

## 项目概览

C++17 集合通信库，单机多进程 TCP AllReduce，参考 NCCL 分 bootstrap / 数据面两步建连的设计。

```mermaid
flowchart LR
    A[main: CommConfig] --> B[Communicator.Init]
    B --> C[TopologyRing: n_channels=4]
    C --> D[Bootstrap: rank0 作 master 收集/广播 NodeInfo]
    D --> E[InitChannels: 每 channel 前驱/后继 建立 TCP 连接]
    E --> F[AllReduce: ring_executor 分片循环]
```

### 源码结构

| 文件 | 职责 |
|------|------|
| `src/bootstrap.cpp` | rank0 为 master 监听 `config.master_port`，收齐各 rank 的 `NodeInfo`(rank/ip/data_port) 后广播给所有人 |
| `src/communicator.cpp` | `Init`: 先选随机空闲 data_port → bootstrap 交换节点信息 → 建立 channel 边连接 |
| `src/topology_ring.cpp` | Ring 拓扑：`ring.prev = (rank-1+ws)%ws`, `ring.next = (rank+1)%ws`；`DefaultChannelCount()=4` |
| `src/planner.cpp` | 把 tensor 按 channel 数分片成 `ChannelWork`(offset/count) |
| `src/ring_executor.cpp` | 各 channel 独立 ring 上的 reduce-scatter + all-gather |
| `src/transport_tcp.cpp` | TCP socket 封装（Listen/Connect/Send/Recv） |
| `include/types.h` | 数据结构：`CommConfig`(rank/world_size/master_addr/master_port/n_channels)、`NodeInfo`、`CollTask`、`ChannelWork`、`CollPlan` |

关键设计点：
- **双端口分工**：`master_port`(默认 12345，测试用 12321) 只用于 bootstrap 握手；数据面每个进程临时 `Listen(0)` 拿一个**随机空闲 data_port** 再广播。
- **连边方向**：每 channel 需连 prev/next 两条边；代码按 `rank < peer` 决定主动 connect、否则被动 accept（`src/communicator.cpp` InitChannels 附近），避免死锁。
- `n_channels` 默认 4（`config.n_channels<=0` 时取 `DefaultChannelCount()`），与 tensor 无关，纯粹并行建多条 ring。
- 测试程序从 `OMPI_COMM_WORLD_RANK/SIZE` 读 rank（只兼容 Open MPI，不依赖 MPICH/PMI/Slurm）；没有该环境变量时回落 `argv[1]`=rank、`argv[2]`=world_size。

## 构建

```bash
mkdir -p build && cd build
cmake .. && make -j$(nproc)
```

- 依赖 **fmt（必须 ≥9，logger.h 用了 `fmt::format_string` 编译期格式化检查）**，版本 10.2.1 已验证。
- fmt 源码 **vendor 在项目内** `third_party/fmt/`（已 git 管理，clone 即可离线构建，无需系统预装）。
- `CMakeLists.txt` 先查项目内 `third_party/fmt/CMakeLists.txt`，缺失才回落 `find_package(fmt REQUIRED)`（系统没装 fmt 时会失败）。
- **OpenMPI 需预装**才能跑 mpirun 方式测试。
- 产物：`build/liboriginccl.so`、`build/tests/test_allreduce`。

## 运行测试

```bash
cd build
make run_test_allreduce          # 等价 mpirun -np 2 ./tests/test_allreduce（cmake 找到 mpirun 才生成此 target）
# 或直接：
mpirun -np 2 ./tests/test_allreduce
# 手动模式（无 mpirun 时，需自行并行拉起多个进程，rank0 必须先启动做 master）：
./tests/test_allreduce <rank> <world_size>
```

- 验证预期：rank i 填充 `(i+1)`，SUM 后各 rank 都得 `world_size*(world_size+1)/2`（np=2 → 3）。
- 单进程 `./tests/test_allreduce 0 1` 走 "Single-rank communicator, skip data-plane" 分支。

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
| 多进程跑起来后卡住/hang | rank0(master) 未先启动，或 bootstrap 端口(12321)被占用；确认所有进程同时拉起 |
| mpirun 下 `OMPI_COMM_WORLD_RANK` 读不到 | 用了非 Open MPI 的启动器（该测试只兼容 Open MPI） |
| 换机器/换 fmt 版本后编译错 | fmt 版本过低（需 ≥9） |
