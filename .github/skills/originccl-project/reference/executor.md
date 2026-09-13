# Executor 层（`include/executor/*.h`、`src/executor/*.cpp`）

执行策略层。**只决定如何等待 socket 就绪**，算法推进交给 `Topology::AllreduceStep`。改动 executor 前阅读本文件。

其他层仍是 `include/`、`src/` 平铺，只有 executor 单独成目录。头文件从 include 根限定引用，
形如 `#include "executor/executor.h"`、`#include "executor/epoll_executor.h"`。

## 核心职责边界

- `Executor` 基类只有 `Run(const CollPlan&)` 与 `Shutdown()`。
- executor 监听 task 的 read+write fd，把就绪事件喂给 `Topology::AllreduceStep`；不展开算法步骤。
- executor 是 task 游标（`PlanTask.state`）的唯一推进者，故以非 const 引用推进（worker/单线程独占自己 channel）。
- 同一 `Communicator` 的多个 AllReduce 由调用方保证不并发；executor 不提供调用级互斥。

## 编译期选定

由 CMake `OCCL_EXECUTOR`（`multi_thread`/`epoll`/`polling`/`reactor`，默认 `multi_thread`）→ 编译宏 → `Communicator` 用 `#ifdef` 构造。一次构建一种，**无运行时切换接口**。

## 四种 executor

- **MultiThreadExecutor**（默认）：按 `plan.channels` 懒加载 worker，channel `i` 固定由 worker `i` 执行；新 worker 以当前 batch id 初始化。每 worker `poll()` 自己 channel 的 read+write fd。
- **EpollExecutor**：单线程把每 channel 队首 task 的 read+write fd 注册进一个 epoll；channel 内多 task 顺序推进。
- **PollingExecutor**：单线程不监听任何 fd，循环对所有未完成 task 的未完成 send/recv 分别喂事件，一轮无进展 `sched_yield()`。
- **ReactorExecutor**：调用线程只跑 epoll，全部 `Allreduce*` 调用在 worker 池执行；worker 数由 `ReactorExecutor(size_t worker_count = 4)` 决定，`Communicator` 用默认 4。主线程用 mutex + condition_variable 的 FIFO 队列下发 job，worker 用 mutex + completion 队列加 eventfd 回报结果；eventfd 与 transport fd 注册在同一个 epoll 里。

## 关键不变式（都曾踩过坑，勿回退）

1. **多事件必须全喂**：一次 poll/epoll 等待返回多个就绪（或单 event 同置 IN|OUT）时，send 和 recv 都要喂 `AllreduceStep`。只喂一个会饿死对方 → 2 rank（`prev==next`）死锁。
2. **epoll fd 跨 plan 复用**：task 完成必须 `EPOLL_CTL_DEL`，否则下个 plan `ADD` 报 EEXIST。
3. **单 rank（无数据面）task 立即完成**：epoll 不注册 fd；启动时循环结算立即完成的 task，只有真等网络的才计入 active，否则 `epoll_wait(-1)` 永久阻塞。
4. **生命周期**：`Communicator::Finalize` 先 `executor->Shutdown()`（多线程还需 join worker），再关闭 channel transport。
5. **Reactor 的派发期间必须注销 fd**：fd 交给 worker 前 `EPOLL_CTL_DEL`，completion 返回 Waiting 后再注册。既避免 level-triggered epoll 在 worker 推进同一 task 时反复唤醒 reactor，也保证 `PlanTask.state` 单写者。`Run` 返回前必须 drain 完所有 in-flight job（异常路径用 `StopWorkers()` 兜底），否则 worker 会引用已销毁的局部 `CollPlan`。

## 文件

| 文件 | 职责 |
|------|------|
| `include/executor/executor.h` | `Executor` 抽象基类（`Run`/`Shutdown`） |
| `include/executor/multi_thread_executor.h` / `src/executor/multi_thread_executor.cpp` | 懒加载 worker + `poll()`，就绪事件全喂 Step，批次同步与错误汇总 |
| `include/executor/epoll_executor.h` / `src/executor/epoll_executor.cpp` | 单线程 epoll，就绪事件喂 Step，task 完成 `EPOLL_CTL_DEL` |
| `include/executor/polling_executor.h` / `src/executor/polling_executor.cpp` | 单线程轮询，对未完成 send/recv 分别喂 Step，无进展 `sched_yield()` |
| `include/executor/reactor_executor.h` / `src/executor/reactor_executor.cpp` | epoll 主线程 + worker 池：FIFO 队列下发 init/step job，eventfd 回收 completion |
