# OriginCCL — Agent 指南（AGENTS.md）
本文件是 AI 编码代理与贡献者在本仓库工作的权威指引，如果本文件内容与代码现状冲突，内容以**代码现状**为准，并主动提醒是否更新本文件。本文档不允许自行改动，一定要主动提醒经过同意！

## Project overview
OriginCCL 是一个受 NCCL 启发的 C++ 集合通信（collective communication）库。

- 语言/标准：C++17
- 构建：CMake（≥ 3.10），产物为共享库 `originccl`
- 外部依赖（系统级，需预装）：
  - **fmt**（≥ 9，已验证 10.2.1），必须提供 CMake package
  - **Threads**
  - **MPI**（Open MPI 4.1.2 已验证）：测试链接 `MPI::MPI_CXX`，rank / world size / `UniqueId` 都走它；多进程测试仅支持 `mpirun` 启动。

## Golden rules (the things agents most often get wrong)!!!!
- **主路径优先于防御性编码。** 先把核心功能跑通 —— 这比守住每个边界情况更重要。事实上，当前代码的逻辑设计已经有了很多“隐含保证”，例如同一个channel的send和recv一定使用不同的fd。你应该妥善利用这些“隐含保证”，不要做不可能发生的错误处理、回退与防御性检查。`docs/`目录下的文件有助于你理解这些“隐含保证”，你在更新docs/`目录下的文件也要注意维护和增删这些隐含保证。
- 你添加的每个函数、变量、结构体与类都必须**语义清晰且确实必要**。如果某个被提议的实体删掉后既不损失清晰度也不损失能力，那它就不该存在：不要"以防万一"地添加包装、参数或占位。特别是**新增类之前先确认，尤其是基类。** 未经明确批准绝不引入新的抽象基类。优先使用自由函数，或扩展既有类型。
- **优先采用最简设计与实现。** 做能解决问题的最小改动，不要大规模重写，不要顺手重构，使用 git diff 最小设计。写直接、可读的版本：不要为处理不了的错误加 `try`/`catch`。
- **不要添加任何注释。**`src/` 与 `include/` 倾向于完全无注释。对于特别不显而易见的 *why*，写**一行**短注释是可以的。
- 公开入口返回 `bool`，不跨 API 抛异常。失败时先 `LOG_ERROR`，再 `return false`。
- 日志一律走 `LOG_DEBUG/INFO/WARN/ERROR` 宏（fmt 风格 `{}` 占位），不用 `printf`/`iostream`。

## Architecture
```
include/                   公开头（平铺）
include/executor/          executor 头（限定路径引用：#include "executor/executor.h"）
include/transport/         transport 头（限定路径引用：#include "transport/transport.h"）
src/                       实现（平铺）
src/executor/              四种 executor 实现
src/transport/             TCP 与共享内存传输实现
tests/                     三个测试二进制
docs/layers/               各层规则文档（改哪层读哪层，勿一次全读）
scripts/                   run_tests.sh / run_all_executors.sh
```

| 层 | 头文件 | 职责 | 铁律 |
| --- | --- | --- | --- |
| **transport** | `transport/transport.h` / `transport/transport_tcp.h` / `transport/transport_shm.h` | 字节搬运 + 就绪可等待性（readiness） | 见下方“transport 就绪契约”；TCP 与 SHM 同构（listener + connection 双形态） |
| **topology** | `topology.h` / `topology_ring.h` | 拥有集合算法，把 `PlanTask.state` 当游标推进 | 只做事件处理，非阻塞，见下方契约 |
| **planner** | `planner.h` | 把 `CollTask` 规划为 `CollPlan` | 纯规划，无回调、无 `std::function` |
| **executor** | `executor/executor.h` + 四实现 | 决定“如何等待 transport 就绪”，驱动 topology | 编译期选定；是 task 游标的**唯一推进者** |
| **communicator** | `communicator.h` | 顶层编排：建 listener → bootstrap → 分组 → 建 channel → 选 executor | 唯一对外入口，暴露 `AllReduce` |
| **bootstrap** | `bootstrap.h` | master/worker 交换 `NodeInfo`（含 `data_port`/`hostname`） | 用于本地分组与建连 |
| **utils / logger / types** | `utils.h` / `logger.h` / `types.h` | 编解码、socket 辅助、reduce 运算、日志宏、公共数据结构 | 日志统一走 `LOG_*` 宏 |

## Coding discipline
- 格式化由 `.clang-format` 统一（LLVM 基底：4 空格缩进、120 列、`Attach` 大括号、`PointerAlignment: Left`、`SortIncludes: Never`）。Stop 阶段的 format hook 会对改动的 `.cpp/.h` 跑 `clang-format -i`。
- 命名：类/类型 PascalCase，成员 `snake_case_`（尾部下划线），自由函数 `PascalCase` 或 `namespace Utils` 内小写开头。与你正在编辑的文件的风格保持一致。不要以此作为顺手重构（drive-by refactor）的借口。
- 优先使用智能指针管理所有权，使用 decltype 进行类型推导，在类型明显或冗长时使用 auto，使用 using 代替 typedef，编码风格贴近 C++17 而非C语言。
- 缩进：4个空格，不使用制表符；大括号：K&R 风格（不另起一行）；最大行长度：200 字符；逗号后加空格；

## Build and test
测试进程的 rank / world size 来自 MPI（`MPI_Comm_rank` / `MPI_Comm_size`）；bootstrap 的 `UniqueId`（rank0 的 IP + 端口）由 rank0 用 `Communicator::GetUniqueId` 生成后经 `MPI_Bcast` 分发给其余 rank，不再有 `OCCL_MASTER_ADDR` / `OCCL_MASTER_PORT`：

```bash
mpirun -np 2 build/tests/test_allreduce
mpirun -np 4 build/tests/test_allreduce
# 单进程直接跑 = MPI singleton（rank 0、world size 1，走无数据面路径）：
build/tests/test_allreduce
```

- `cmake --build build --target run_test_allreduce` 只跑 **np=2** 一档，不是全矩阵。
- **完整验证请用脚本**（各 tier 是不同的代码路径，不是“同一条多跑几次”）：
  - `scripts/run_tests.sh` —— 单次构建下的 tier 0–10 矩阵（含 shm / tcp 两种传输模式与单机多机模拟）。
  - `scripts/run_all_executors.sh` —— 对 4 种 executor 各跑一遍完整矩阵，退出码 0 才算全过。
  - 主要参数：`--coverage` / `--no-build` / `--build-dir` / `--build-type` / `--executor` / `--transport` / `--timeout` / `--allow-skip`。
- 退出码约定（`run_tests.sh`）：`0` 全过 / `1` 失败 / `2` 有 SKIP / `3` 覆盖率报告无法生成。
- **mpirun 缺失 → 报 SKIP 而非通过**：在一台只跑了单 rank tier 的机器上“静默变绿”正是该脚本要杜绝的失败模式。
- 四个测试二进制：
  - `test_allreduce`：端到端 AllReduce 正确性（各 rank 填 `rank+1`，断言 reduce 结果）。
  - `test_local_info`：单机 local rank 视角（`local_rank`/`local_size`/`local_ranks`/`is_single_machine`）断言。
  - `test_multi_machine`：单机模拟多机（`CommConfig::get_hostname` 注入按 rank 推导的假 hostname，每机 rank 数按 world size 在测试内写死），断言 local 视角与跨机（TCP）/混合（SHM+TCP）路径的 AllReduce。
  - `test_transport_shm`：共享内存传输的 fork 端点对，**无需 mpirun**（rendezvous、描述符传递、控制握手、阻塞/非阻塞、回绕/背压、方向拒绝、拆除）。


## commit rules
- **提交comment**：简短、小写、朴素英文 —— `add fmt`、`modify channels`、`root ip and port from env`。
不使用 Conventional Commits 前缀，也不加 AI 工具署名。
- **提交边界**：不要顺路修复不相关的bug或者重构，把它作为独立提交落下来，或者主动询问是否修改。
- **文档同步**：架构边界、构建命令、或测试运行方式发生变化时，必须在**同一次改动**中更新 `docs/layers/` 对应文档（`executor.md` / `topology.md` / `transport.md` / `planner.md` / `communicator.md`）。Stop 阶段的 docs hook 会在 `src/` 改动 ≥ `OCCL_DOCS_SYNC_MIN_LINES`（默认 500）行且 `docs/` 未动时提醒一次；确认无需更新时说明理由即可。
