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
include/topology/          topology 头（限定路径引用：#include "topology/topology.h"）
src/                       实现（平铺）
src/executor/              四种 executor 实现
src/transport/             TCP、共享内存与 RDMA（含零拷贝变体）传输实现
src/topology/              ring / tree / p2p 拓扑实现
tests/                     七个测试入口 + test_common / transport_check 共享支持
docs/layers/               各层规则文档（改哪层读哪层，勿一次全读）
scripts/                   run_tests.sh（唯一入口）
```

| 层 | 头文件 | 职责 | 铁律 |
| --- | --- | --- | --- |
| **transport** | `transport/transport.h` / `transport/transport_tcp.h` / `transport/transport_shm.h` / `transport/transport_rdma.h` / `transport/transport_rdma_zc.h` | 字节搬运 + 就绪可等待性（readiness） | TCP/SHM/RDMA 同构（listener + connection 双形态）；`transport_rdma_zc.h` 是 RDMA 的零拷贝子类，阈值 (256 KiB) 以上用独立 QP 直发用户 MR，以下完全走基类环形缓冲 |
| **topology** | `topology/topology.h` / `topology/topology_ring.h` / `topology/topology_tree.h` / `topology/topology_p2p.h` | 拥有集合与 P2P 算法，把 `PlanTask.state` 当游标推进 | 通用 `CollectiveInit/Step/Done` 三阶段接口，只做非阻塞事件处理 |
| **planner** | `planner.h` | 把一批 `CollTask` 排序（集合在前、P2P 轮次在后）并规划为 `CollPlan`；集合切片，P2P 按轮次建连 | 集合纯规划，P2P 准备连接；无回调、无 `std::function`；排序/分批为独立函数（`SortTasks`/`Plan`/`PlanRound`） |
| **executor** | `executor/executor.h` + 四实现 | 决定“如何等待 transport 就绪”，驱动 topology | `Init` 按核数与 local rank 数在 polling 与 epoll 间**运行时**选定；是 task 游标的**唯一推进者**；只调用通用三阶段接口，不按集合类型分派 |
| **communicator** | `communicator.h` | 顶层编排：建 listener → bootstrap → 分组 → 建 channel → 选 executor | 唯一对外入口，暴露五种集合操作、同步 Send/Recv 与 `GroupStart`/`GroupEnd` 批量下发（单独调用即单元素组，同一路径） |
| **bootstrap** | `bootstrap.h` | master/worker 交换 `NodeInfo`（含集合与 P2P 端点、hostname） | 用于本地分组与建连；本地 `NodeInfo` 由 communicator 构造后传入 |
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
scripts/run_tests.sh --level 0 -j 2 --oversubscribe
scripts/run_tests.sh --level 1 -j 2 --oversubscribe
scripts/run_tests.sh --level 2 -j 2 --oversubscribe
# 查看某一等级的完整用例矩阵（需先构建一次）：
scripts/run_tests.sh --level 0 --list-cases --no-build
```

- **等级决定进程布局与数据量**：等级 0 = 单机 4 rank / 多机 4×1 rank / count {32768,131072}；等级 1 = 8 rank / 4×2 / count {65536,262144}；等级 2 = 32 rank / 8×4 / count {262144,1048576}。用例矩阵：等级 0 对 reduce 类在每个 (dtype, op) 上只采样一个 count（两个 count 间轮转），非 reduce 类遍历全部 dtype×count；
- **executor 由库在 `Init` 时运行时选取**：`sysconf(_SC_NPROCESSORS_ONLN)` 全机在线核数 ≥ 本机 rank 数（每 rank 已绑核独占一核）用 `polling`，否则用 `epoll`。一次构建覆盖全部运行，脚本无 executor 维度（`--executor` 已删除）。`reactor` / `multi_thread` 的实现仍全量编译在库里，但已无选取路径。
- **多机是单机模拟**：`CommConfig::get_hostname` 注入逻辑 hostname，`rank / ranks_per_machine` 推导机器号；它验证分组与传输选择，**不等于真实跨主机**。
- **OCCL_DISABLE_SHM / OCCL_DISABLE_RDMA 不是矩阵维度**：测试子进程不设置它们，集合用例按自动选择走 SHM / RDMA / TCP。
- **mpirun 缺失或无可用 RDMA 设备 → SKIP 而非通过**（RDMA 与零拷贝 RDMA 无设备时对应测试返回 2）；等级 1/2 在核数不足时需 `--oversubscribe`。
- 退出码约定（`run_tests.sh`）：`0` 全过 / `1` 有用例失败或超时 / `2` 只剩 SKIP / `4` 参数非法。
- 默认跑缩小数据量的测试。
- 七个测试入口：
  - `test_single_machine`：真实单机，断言 `is_single_machine` / `local_size` / `local_rank`，再跑集合用例矩阵。
  - `test_multi_machine`：注入逻辑 hostname 的模拟多机，断言 `is_single_machine` / `local_size` / `local_rank` / `local_ranks`，再跑集合用例矩阵。两个集合套件都只比对接口输入输出，不检查每条环边的具体传输类型。
  - `test_transport_tcp` / `test_transport_shm` / `test_transport_rdma` / `test_transport_rdma_zc`：四种传输的接口语义套件（建连、握手、阻塞与非阻塞收发、progress/done 单调、零长度、边界尺寸到 5 MiB、背压与恢复、就绪与方向约束、关闭语义）。TCP 与 SHM 只需两个进程，RDMA 与零拷贝 RDMA 需设备。零拷贝档位额外覆盖阈值前后、多 chunk 与两条路径交替，并断言 `progress` 全有或全无。
  - 用例生成、独立期望值与结果上报都在 `tests/test_common.*`；传输套件共用 `tests/transport_check.*`。
  - `test_p2p`：`--suite p2p` 固定四 rank 跑同机和模拟跨机，验证懒建连、乱序来源缓存、连接隔离与复用、边界契约、集合交替、嵌套 Group 批处理（冷建连整轮全互联、同 peer 重复消息 FIFO），以及 polling/epoll 下阈值前后到 48 MiB 的完整数据。无 RDMA 时验证跨机拒绝回退，再将跨机传输记为 SKIP；不缩放数据量。
- **P2P 契约与建连**：改 Send/Recv、`Channel.send_p2p/recv_p2p`、Group 批处理或懒建连时读 `docs/layers/communicator.md`、`planner.md`、`topology.md`。发送固定用 channel 0、接收固定用 channel 1（要求 `n_channels >= 2`），方向连接与集合隔离；跨机强制 RDMA_ZC；同 communicator 禁止并发，调用方保证配对顺序。planner 每轮用两个短生命周期线程分别 Connect 与 Accept、join 后执行，无长驻后台线程。
- **Group 批量下发**：`GroupStart`/`GroupEnd` 支持嵌套，只有深度归零的最外层 End 排序后执行。集合按 `(func, 字节量, dtype, 有效 op, 有效 root)` 稳定升序，P2P 一律排在最后并按轮次升序（`Send` 到 `(rank-i+N)%N`、`Recv` 自 `(rank+i)%N`，轮次由 planner 推导、调用方不传）。组内操作彼此独立（可共享只读输入），不支持同组内前操输出作为后操输入，依赖用 Group 边界表达；同轮的 Send 放 channel 0、Recv 放 channel 1，由 executor 并发推进。
- **回归只看摘要，不读 `.out`**：`run_tests.sh` 已把结果既打到 stdout 又写进 `test-reports/summary.txt`。摘要非 PASS 时才用 `grep -n` 在对应 `.out` 里定位。`single.out` / `multi.out` 每个约 29k token、等级 2 更大，一旦读进上下文会随每轮重发，是主要的 token 开销来源。
- **PASS 的 run 里本来就带 `[ERROR]` 行**：集合 contract 的 6 个负向用例（`ExpectFailure`）× rank 数，包括缺 buffer 与越界 root；P2P 也覆盖非法 peer、空 buffer 与无 RDMA 时拒绝跨机传输。这是「库正确拒绝非法 task」的直接证据，不是失败信号，以测试摘要为准。
- 想要 sanitizer（asan-ubsan）或覆盖率，需要自己配 CMake 构建。sanitizer 无法覆盖 RDMA DMA 与跨进程共享内存竞态，完整数据比对与就绪测试是必要补充。

## commit rules
- **提交comment**：简短、小写、朴素英文 —— `add fmt`、`modify channels`、`root ip and port from env`。
不使用 Conventional Commits 前缀，也不加 AI 工具署名。
- **提交边界**：不要顺路修复不相关的bug或者重构，把它作为独立提交落下来，或者主动询问是否修改。
- **文档同步**：架构边界、构建命令、或测试运行方式发生变化时，必须在**同一次改动**中更新 `docs/layers/` 对应文档（`executor.md` / `topology.md` / `transport.md` / `planner.md` / `communicator.md`）。Stop 阶段的 docs hook 会在 `src/` 改动 ≥ `OCCL_DOCS_SYNC_MIN_LINES`（默认 500）行且 `docs/` 未动时提醒一次；确认无需更新时说明理由即可。
