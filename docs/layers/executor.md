# Executor 层（`include/executor/*.h`、`src/executor/*.cpp`）

执行策略层。**只决定如何等待 socket 就绪**，算法推进交给 `Topology::CollectiveStep`。改动 executor 前阅读本文件。

executor、transport 与 topology 单独成目录，其余层仍平铺。头文件从 include 根限定引用，形如 `#include "executor/executor.h"`。

# 核心职责边界

- `Executor` 基类只有 `Run(const CollPlan&)` 与 `Shutdown()`。
- 执行 plan 前设置各 transport 的等待模式：polling executor 选择 `Polling`，epoll executor 选择 `EventDriven`；SHM 据此启停 eventfd 通知与排空。
- 等待 task 各 transport 的就绪（`GetFd()` + `GetPollEvents()`），把就绪喂给 `Topology::CollectiveStep`；不展开算法步骤。
- 就绪到逻辑操作的映射由 transport 在 task 中的位置决定：`send_transports` 任一连接就绪推进 Writable（send），`recv_transports` 任一连接就绪推进 Readable（recv）。executor 不认具体 transport 类型，也不把就绪位当成方向（共享内存发送端等的是可读的 eventfd）。
- 是 task 游标（`PlanTask.state`）的唯一推进者，故以非 const 引用推进；隐含保证 worker/单线程独占自己 channel，`PlanTask.state` 单写者，不加锁。
- 运行时选定：`Communicator::Init` 在本地分组算完后按 `sysconf(_SC_NPROCESSORS_ONLN)` 与 `local_size` 选 executor——核数 ≥ local rank 数（每 rank 已 `PinProcessToCpu` 独占一核）用 `multi_thread`，核数不够（已超订）用 `epoll`，避免 worker 争抢 CPU。`polling` / `reactor` 仍编译在库里，但没有选取它们的路径。

# 文件介绍

| 文件 | 职责 |
|------|------|
| `include/executor/executor.h` | `Executor` 抽象基类（`Run`/`Shutdown`） |
| `include/executor/multi_thread_executor.h` / `src/executor/multi_thread_executor.cpp` | 懒加载 worker + per-task `epoll`（`EPOLLET`），遍历注册两侧连接、就绪全喂 Step，批次同步与错误汇总 |
| `include/executor/epoll_executor.h` / `src/executor/epoll_executor.cpp` | 单线程 epoll，就绪喂 Step，task 完成 `EPOLL_CTL_DEL` |
| `include/executor/polling_executor.h` / `src/executor/polling_executor.cpp` | 单线程轮询，对未完成 send/recv 分别喂 Step，无进展 `sched_yield()` |
| `include/executor/reactor_executor.h` / `src/executor/reactor_executor.cpp` | epoll 主线程 + worker 池：FIFO 队列下发 init/step job，eventfd 回收 completion |

# 实现原理

- **MultiThreadExecutor**（默认）：按 `plan.channels` 懒加载 worker，channel `i` 固定由 worker `i` 执行；每 worker 在每个 task 上建一个 `epoll`（`EPOLLET`），遍历该 task 两侧 transport 注册就绪，等待后全喂 Step。
- **EpollExecutor**：单线程把每 channel 队首 task 的各 transport 就绪注册进一个 epoll；channel 内多 task 顺序推进。
- **PollingExecutor**：单线程不监听任何 fd，循环对所有未完成 task 的未完成 send/recv 分别喂事件，一轮无进展 `sched_yield()`。
- **ReactorExecutor**：调用线程只跑 epoll，全部 `Collective*` 调用在 worker 池执行；主线程用 mutex + condition_variable 的 FIFO 队列下发 job（job 带逻辑 step 位，不带原始 epoll 位），worker 用 mutex + completion 队列 + eventfd 回报结果；eventfd 与 transport fd 注册在同一个 epoll 里。
- task 的就绪注册由各 executor 遍历 `send_transports`/`recv_transports`、用 `GetFd()` + `GetPollEvents()` 逐条拼出；epoll/reactor 的事件 tag 为 `slot * 2 + side`（side 0 = recv、side 1 = send），同一 side 的多个连接共用 tag，分发时遍历该侧 transport 用 `GetPollEvents()` 复核。
- 新增 executor 须在 `Communicator` 的选取处（`MakeExecutor`）注册；只实现 `Run`/`Shutdown`，只推进 `CollectiveStep`，不改 `PlanTask` 算法语义字段（只推进 `state`）。

# 隐含约定

- **多事件必须全喂**：一次 poll/epoll 可能同时返回 recv 和 send 两个就绪，两者都要喂 `CollectiveStep`；只喂一个会饿死对方 → 2 rank（`prev == next`）死锁。
- **就绪位不是方向，方向由位置决定**：`send_transports` 就绪推进 send、`recv_transports` 推进 recv；tag 只区分两侧、不区分同一侧第几条连接，故「一条连接就绪」即推进该侧全部连接（topology 内遍历，未就绪者无进展）。executor 一律用 `GetPollEvents()`，不硬编码 `EPOLLIN`/`EPOLLOUT`。
- **epoll fd 跨 plan 复用**：task 完成必须 `EPOLL_CTL_DEL`，否则下个 plan `ADD` 报 EEXIST。
- **单 rank（无数据面）task 立即完成**：不注册 fd；启动时循环结算立即完成的 task，只有真等网络的才计入 active，否则 `epoll_wait(-1)` 永久阻塞。
- **`Finalize` 先 `executor->Shutdown()`（多线程还需 join worker），再关 channel transport**。
- **边沿触发（EPOLLET）勿回退**：四种 executor 全以 `EPOLLET` 注册，配套两条——(a) transport 的 `Try*` 必须排空到不能再推进（eventfd 是 drain+复检、socket 是读到 EAGAIN），否则漏下一个边沿死锁；(b) step 完成、`BeginPhase` 重置 done 后立刻对两侧各喂一次（`BeginPhase` 内调 `PushBuffer`），让刚重置那侧立刻消费并复位计数，之后的 0→1 边沿才能再触发。**删掉这次补喂会死锁**（SHM 的 `Try*` 是 drain-to-empty，不补喂计数永不复位）。`polling` 不监听 fd，与触发模式无关。
- **Reactor 派发期间必须注销 fd**：fd 交 worker 前 `EPOLL_CTL_DEL`，completion 返回 Waiting 后再注册，保证 `PlanTask.state` 单写者；`Run` 返回前必须 drain 完所有 in-flight job，否则 worker 引用已销毁的 `CollPlan`。
- **`CollectiveInit`/`CollectiveStep` 是 `noexcept`，`false` 是唯一可恢复失败通道**：executor 见到 false 立即补打 channel + init/step 上下文日志，停止派发后续任务，清理资源（注销 fd、join worker）并让 `Run()` 返回 `false`，不调用 `exit()`。`std::thread` 构造异常直接逃逸（不可恢复资源错误）；多线程 executor 不提供在途 `poll` 的抢占式取消，已阻塞 worker 需自然返回。
