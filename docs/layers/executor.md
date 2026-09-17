# Executor 层（`include/executor/*.h`、`src/executor/*.cpp`）

执行策略层。**只决定如何等待 socket 就绪**，算法推进交给 `Topology::AllreduceStep`。改动 executor 前阅读本文件。

其他层仍是 `include/`、`src/` 平铺，只有 executor 单独成目录。头文件从 include 根限定引用，
形如 `#include "executor/executor.h"`、`#include "executor/epoll_executor.h"`。

## 核心职责边界

- `Executor` 基类只有 `Run(const CollPlan&)` 与 `Shutdown()`。
- 等待 task 各 transport 提供的就绪（`GetFd()` + `GetPollEvents()`），把就绪喂给 `Topology::AllreduceStep`；不展开算法步骤。
- 就绪到逻辑操作的映射由 transport 在 task 中的位置决定：`send_transport` 的就绪推进可写（send），`recv_transport` 的就绪推进可读（recv）。executor 不认具体 transport 类型，也不把就绪位当成方向（共享内存发送端等的是可读的 eventfd）。
- 是 task 游标（`PlanTask.state`）的唯一推进者，故以非 const 引用推进（worker/单线程独占自己 channel）。
- 不提供调用级互斥：同一 `Communicator` 的多个 AllReduce 由调用方保证不并发。
- 编译期选定：由 CMake `OCCL_EXECUTOR`（`multi_thread`/`epoll`/`polling`/`reactor`，默认 `multi_thread`）→ 编译宏 → `Communicator` 用 `#ifdef` 构造。一次构建一种，**无运行时切换接口**。

## 不变式与设计区间

### 不变式（都曾踩过坑，勿回退）

1. **多事件必须全喂**：一次 poll/epoll 等待返回多个就绪（或同一 fd 上的多个注册同时就绪）时，send 和 recv 都要喂 `AllreduceStep`。只喂一个会饿死对方 → 2 rank（`prev == next`）死锁。
2. **就绪位不是方向**：哪些位代表可推进、代表发送还是接收，全由 transport 给的注册决定（见 `executor/transport_wait.h`）。同一 fd 上出现两个方向时合并成一次注册并同时喂两者，否则同样饿死 2 rank 环。
3. **epoll fd 跨 plan 复用**：task 完成必须 `EPOLL_CTL_DEL`，否则下个 plan `ADD` 报 EEXIST。
4. **单 rank（无数据面）task 立即完成**：epoll 不注册 fd；启动时循环结算立即完成的 task，只有真等网络的才计入 active，否则 `epoll_wait(-1)` 永久阻塞。
5. **生命周期**：`Communicator::Finalize` 先 `executor->Shutdown()`（多线程还需 join worker），再关闭 channel transport。
6. **Reactor 的派发期间必须注销 fd**：fd 交给 worker 前 `EPOLL_CTL_DEL`，completion 返回 Waiting 后再注册。既避免 level-triggered epoll 在 worker 推进同一 task 时反复唤醒 reactor，也保证 `PlanTask.state` 单写者。`Run` 返回前必须 drain 完所有 in-flight job（异常路径用 `StopWorkers()` 兜底），否则 worker 会引用已销毁的局部 `CollPlan`。

### 设计区间

- 允许新增第五种 executor：须在 CMake `OCCL_EXECUTOR`、编译宏、`Communicator` 的 `#ifdef` 构造处同步注册。
- 各 executor 内部等待策略自由（poll / epoll / 轮询 / 线程池）。
- task 的就绪注册统一由 `include/executor/transport_wait.h` 的 `BuildTransportWaits()` 生成（最多 `kTransportWaitMax` 条，含 fd、就绪掩码、可写/可读标志）；executor 只负责等待与喂事件，不自己拼 fd 与掩码。
- worker 数等参数自由（如 `ReactorExecutor(size_t worker_count = 4)`）。

四种 executor 现状：

- **MultiThreadExecutor**（默认）：按 `plan.channels` 懒加载 worker，channel `i` 固定由 worker `i` 执行；新 worker 以当前 batch id 初始化。每 worker `poll()` 自己 channel 各 transport 的就绪（掩码取自 transport，poll 与 epoll 位值一致）。
- **EpollExecutor**：单线程把每 channel 队首 task 的各 transport 就绪注册进一个 epoll；channel 内多 task 顺序推进。
- **PollingExecutor**：单线程不监听任何 fd，循环对所有未完成 task 的未完成 send/recv 分别喂事件，一轮无进展 `sched_yield()`。
- **ReactorExecutor**：调用线程只跑 epoll，全部 `Allreduce*` 调用在 worker 池执行；主线程用 mutex + condition_variable 的 FIFO 队列下发 job（job 带逻辑 step 位，不带原始 epoll 位），worker 用 mutex + completion 队列加 eventfd 回报结果；eventfd 与 transport fd 注册在同一个 epoll 里。

## 文件介绍

| 文件 | 职责 |
|------|------|
| `include/executor/executor.h` | `Executor` 抽象基类（`Run`/`Shutdown`） |
| `include/executor/transport_wait.h` | `TransportWait` + `BuildTransportWaits()`：把 task 的两个 transport 翻译成 fd/就绪掩码/逻辑方向 |
| `include/executor/multi_thread_executor.h` / `src/executor/multi_thread_executor.cpp` | 懒加载 worker + `poll()`，就绪事件全喂 Step，批次同步与错误汇总 |
| `include/executor/epoll_executor.h` / `src/executor/epoll_executor.cpp` | 单线程 epoll，就绪事件喂 Step，task 完成 `EPOLL_CTL_DEL` |
| `include/executor/polling_executor.h` / `src/executor/polling_executor.cpp` | 单线程轮询，对未完成 send/recv 分别喂 Step，无进展 `sched_yield()` |
| `include/executor/reactor_executor.h` / `src/executor/reactor_executor.cpp` | epoll 主线程 + worker 池：FIFO 队列下发 init/step job，eventfd 回收 completion |

## 修改原则

- 勿回退：多事件必须全喂（只喂一个 → 2 rank 死锁）。
- 勿回退：禁止在 executor 里硬编码 `EPOLLIN`/`EPOLLOUT` 或「读 fd/写 fd」——一律用 `BuildTransportWaits()`；`epoll`/`reactor` 的事件 tag 为 `slot * kTransportWaitMax + 注册序号`。
- 勿回退：task 完成必须 `EPOLL_CTL_DEL`。
- 勿回退：Finalize 先 `Shutdown()` 再关 transport。
- 新增 executor 须实现 `Run`/`Shutdown`，只推进 `AllreduceStep`，不展开算法步骤。
- executor 不改 `PlanTask` 的算法语义字段，只推进 `state`。
- `AllreduceInit`/`AllreduceStep` 是 `noexcept`，`false` 是唯一可恢复失败通道：executor 见到 false 立即补打 channel + init/step 上下文日志，停止派发后续任务，清理自有资源（注销 fd、join worker）并让 `Run()` 返回 `false`，不调用 `exit()`。
- worker 线程创建失败不再 try/catch 转换：`std::thread` 构造异常直接逃逸或终止，属不可恢复资源错误；epoll/eventfd 等系统调用失败仍是 `return false`。多线程执行器不提供在途 `poll` 的抢占式取消，已阻塞的 worker 需自然返回。
