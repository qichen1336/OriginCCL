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
  - **librdmacm / libibverbs**（含 `rdma/rdma_cma.h`、`infiniband/verbs.h` 开发头）：RDMA 传输始终编译进来，缺开发库时 CMake 配置直接失败（不是可选依赖，也不做 dlopen）。运行时是否真的用 RDMA 由设备探测决定。

## Golden rules (the things agents most often get wrong)!!!!
- **主路径优先于防御性编码** 。先把核心功能跑通不要过度关注边界情况。事实上，当前代码的逻辑设计已经有了很多“隐含保证”，例如同一个channel的send和recv一定使用不同的fd。你应该妥善利用这些“隐含保证”，不要做不可能发生的错误处理、回退与防御性检查，严格控制代码量膨胀和熵增。`docs/`目录下的文件有助于你理解这些“隐含保证”，你在更新docs/`目录下的文件也要注意维护和增删这些隐含保证。
- 你添加的每个函数、变量、结构体与类都必须**语义清晰且确实必要**。如果某个被提议的实体删掉后既不损失清晰度也不损失能力，那它就不该存在：不要"以防万一"地添加包装、参数或占位。特别是**新增类之前先确认，尤其是基类。** 未经明确批准绝不引入新的抽象基类。优先使用自由函数，或扩展既有类型。
- **优先采用最简设计与实现** 。做能解决问题的最小改动，不要大规模重写，不要顺手重构，使用 git diff 最小设计。写直接、可读的版本：不要为处理不了的错误加 `try`/`catch`。
- **不要添加任何注释。**`src/` 与 `include/` 倾向于完全无注释。对于特别不显而易见的 *why*，写**一行**短注释是可以的。
- 如果你对我的需求有任何疑问，或者觉得是我没有理解清楚问题，**大胆提问澄清，无需过度思考**。
- 公开入口返回 `bool`，不跨 API 抛异常。失败时先 `LOG_ERROR`，再 `return false`。
- 日志一律走 `LOG_DEBUG/INFO/WARN/ERROR` 宏（fmt 风格 `{}` 占位），不用 `printf`/`iostream`。

## Architecture
```
include/                   公开头（平铺）
include/executor/          executor 头（限定路径引用：#include "executor/executor.h"）
include/transport/         transport 头（限定路径引用：#include "transport/transport.h"）
src/                       实现（平铺）
src/executor/              四种 executor 实现
src/transport/             TCP、共享内存与 RDMA 传输实现
tests/                     五个测试入口 + test_common / transport_check 共享支持
docs/layers/               各层规则文档（改哪层读哪层，勿一次全读）
scripts/                   run_tests.sh（唯一入口）/ coverage_report.sh
```

| 层 | 头文件 | 职责 | 铁律 |
| --- | --- | --- | --- |
| **transport** | `transport/transport.h` / `transport/transport_tcp.h` / `transport/transport_shm.h` / `transport/transport_rdma.h` | 字节搬运 + 就绪可等待性（readiness） | TCP/SHM/RDMA 同构（listener + connection 双形态） |
| **topology** | `topology.h` / `topology_ring.h` | 拥有集合算法，把 `PlanTask.state` 当游标推进 | 通用 `CollectiveInit/Step/Done` 三阶段接口，只做非阻塞事件处理 |
| **planner** | `planner.h` | 把 `CollTask` 规划为 `CollPlan` | 纯规划，无回调、无 `std::function` |
| **executor** | `executor/executor.h` + 四实现 | 决定“如何等待 transport 就绪”，驱动 topology | 编译期选定；是 task 游标的**唯一推进者**；只调用通用三阶段接口，不按集合类型分派 |
| **communicator** | `communicator.h` | 顶层编排：建 listener → bootstrap → 分组 → 建 channel → 选 executor | 唯一对外入口，暴露五种集合操作 |
| **bootstrap** | `bootstrap.h` | master/worker 交换 `NodeInfo`（含 `data_port`/`hostname`/`rdma_addr`/`rdma_port`） | 用于本地分组与建连；本地 `NodeInfo` 由 communicator 构造后传入 |
| **utils / logger / types** | `utils.h` / `logger.h` / `types.h` | 编解码、socket 辅助、reduce 运算、日志宏、公共数据结构 | 日志统一走 `LOG_*` 宏 |

## Coding discipline
- 格式化由 `.clang-format` 统一（LLVM 基底：4 空格缩进、120 列、`Attach` 大括号、`PointerAlignment: Left`、`SortIncludes: Never`）。Stop 阶段的 format hook 会对改动的 `.cpp/.h` 跑 `clang-format -i`。
- 命名：类/类型 PascalCase，成员 `snake_case_`（尾部下划线），自由函数 `PascalCase` 或 `namespace Utils` 内小写开头。与你正在编辑的文件的风格保持一致。不要以此作为顺手重构（drive-by refactor）的借口。
- 优先使用智能指针管理所有权，使用 decltype 进行类型推导，在类型明显或冗长时使用 auto，使用 using 代替 typedef，编码风格贴近 C++17 而非C语言。
- 缩进：4个空格，不使用制表符；大括号：K&R 风格（不另起一行）；最大行长度：200 字符；逗号后加空格；

## Build and test
测试进程的 rank / world size 来自 MPI（`MPI_Comm_rank` / `MPI_Comm_size`）；bootstrap 的 `UniqueId`（rank0 的 IP + 端口）由 rank0 用 `Communicator::GetUniqueId` 生成后经 `MPI_Bcast` 分发给其余 rank，不再有 `OCCL_MASTER_ADDR` / `OCCL_MASTER_PORT`：

```bash
# 入口只有一个：run_tests.sh（-h 有完整说明）。等级 0 是本机可跑的最小档。
scripts/run_tests.sh --level 0 --executor all --profile all -j 2 --oversubscribe
scripts/run_tests.sh --level 1 --executor all --profile all -j 2 --oversubscribe
scripts/run_tests.sh --level 2 --executor all --profile all -j 2 --oversubscribe
# 查看某一等级的完整用例矩阵（需先构建一次）：
scripts/run_tests.sh --level 0 --list-cases --no-build
```

- **等级决定进程布局与数据量**：等级 0 = 单机 4 rank / 多机 4×1 rank / count {1,1024,10240}；等级 1 = 8 rank / 4×2 / 加 65536；等级 2 = 32 rank / 8×4 / 同等级 1 的 count。等级 0 每个接口遍历全部 dtype、op、count；等级 1 补齐两两组合与 `n_channels=3`；等级 2 跑完整合法核心集并加 `n_channels=1`。
- **只覆盖 polling 与 epoll**（`--executor all` 即这两个）；另外两个 executor 的实现仍在库里，但不在本矩阵内。
- **多机是单机模拟**：`CommConfig::get_hostname` 注入逻辑 hostname，`rank / ranks_per_machine` 推导机器号；它验证分组与传输选择，**不等于真实跨主机**。
- **OCCL_DISABLE_SHM / OCCL_DISABLE_RDMA 不是矩阵维度**：测试子进程不设置它们，集合用例按自动选择走 SHM / RDMA / TCP，并把实际路径写进报告。
- **mpirun 缺失或无可用 RDMA 设备 → SKIP 而非通过**（RDMA 无设备时 `test_transport_rdma` 返回 2）；等级 1/2 在核数不足时需 `--oversubscribe`。
- 退出码约定（`run_tests.sh`）：`0` 全过 / `1` 有用例失败、sanitizer 报错或超时 / `2` 只剩 SKIP / `3` 覆盖率报告生成失败 / `4` 参数非法。
- 五个测试入口：
  - `test_single_machine`：真实单机的 rank 视角（`local_rank`/`local_size`/`local_ranks`/`is_single_machine`）、每边必须是共享内存，以及全量集合用例。
  - `test_multi_machine`：注入逻辑 hostname 的模拟多机，断言 local 视角、**每条环边（`ring.prev`/`ring.next`）的具体传输类型**（同机 SHM、跨机统一 RDMA 或 TCP，且单边 fd 独立）与全量集合用例。
  - `test_transport_tcp` / `test_transport_shm` / `test_transport_rdma`：三种传输的接口语义套件（建连、握手、阻塞与非阻塞收发、progress/done 单调、零长度、边界尺寸到 5 MiB、背压与恢复、就绪与方向约束、关闭语义）。TCP 与 SHM 只需两个进程，RDMA 需设备。
  - 用例生成、独立期望值与结果上报都在 `tests/test_common.*`；三种传输共用 `tests/transport_check.*`。
- `--profile asan-ubsan` 是必跑的动态检查档（`--profile all` 含它）；`--profile coverage` 出 gcovr/lcov/gcov 报告。sanitizer 无法覆盖 RDMA DMA 与跨进程共享内存竞态，完整数据比对与就绪测试是必要补充。

## commit rules
- **提交comment**：简短、小写、朴素英文 —— `add fmt`、`modify channels`、`root ip and port from env`。
不使用 Conventional Commits 前缀，也不加 AI 工具署名。
- **提交边界**：不要顺路修复不相关的bug或者重构，把它作为独立提交落下来，或者主动询问是否修改。
- **文档同步**：架构边界、构建命令、或测试运行方式发生变化时，必须在**同一次改动**中更新 `docs/layers/` 对应文档（`executor.md` / `topology.md` / `transport.md` / `planner.md` / `communicator.md`）。Stop 阶段的 docs hook 会在 `src/` 改动 ≥ `OCCL_DOCS_SYNC_MIN_LINES`（默认 500）行且 `docs/` 未动时提醒一次；确认无需更新时说明理由即可。
