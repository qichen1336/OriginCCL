# Transport 层（`include/transport*.h`、`src/transport_tcp.cpp`、`src/transport_shm.cpp`）

两种传输实现：TCP socket 与同机共享内存。改动本层前阅读本文件。

## 核心职责边界

- 负责 socket 封装：建连、收发、关闭，并把 fd 暴露给 executor 注册监听。
- 负责两套接口：
  - 阻塞接口（控制面）：`Listen` / `Accept` / `Connect` 建连（成功后 socket 置 `O_NONBLOCK`）；`Send` / `Recv`（内部 `SendRaw`/`RecvRaw`，对 EAGAIN 忙等重试），供 bootstrap 握手等偶发控制面使用。
  - 非阻塞接口（数据面）：`TrySend(data, size, *progress, *done)` / `TryRecv(...)` 单次推进到 EAGAIN 为止，`*progress` 累计已做字节，`*done` 表示完成；返回 false 表对端关闭或真错误。
  - 就绪契约：`GetFd()` 给出 executor 要等待的描述符（listening 时为 listen fd，连上后为数据面 fd）；`GetPollEvents()` 给出该描述符上要等待的原生就绪掩码（Linux epoll 掩码 `EPOLLIN`/`EPOLLOUT`，与 poll 位值一致）；`SetDirection()` / `GetDirection()` 维护方向元数据（`Bidirectional`（默认）/ `Send` / `Receive`）。
  - `Close()` / `IsConnected()`。
- 不负责：不决定等待策略（executor）、不推进算法（topology）、不监听 fd（executor）。

### 共享内存传输（`TransportShm`）

- 与 TCP 同构的 listener/connection 双形态：`ListenPath(path)` + `Accept()`（被动端），`Connect(rendezvous_path, port)`（主动端，`port` 忽略）。基类的 `Listen(uint16_t)` / `GetListenPort()` 只用来报告「共享内存绑的是路径不是端口」的误用，永远失败/返回 0。
- 建立流程：主动端建 2 MiB 数据容量的 memfd 环 + data-ready/space-ready 两个 eventfd，用 `SOCK_SEQPACKET` + `SCM_RIGHTS` 一次性传给对端并带上自己的方向；被动端取补方向。环元数据是 cache line 分隔、单调递增的 `std::atomic<uint64_t>` head/tail（producer 写 head，consumer 写 tail，acquire/release 配对）。
- 控制 socket 与数据面分开：调用方**第一次**阻塞 `Send`/`Recv`（即 communicator 的连接握手）走 control socket，接收方回 1 字节 ack，双方随即关闭 control socket；之后所有阻塞与非阻塞操作都走环。所以调用点不需要区分传输类型。
- 方向在此**是**约束（与 TCP 不同）：producer 只 `Send`、consumer 只 `Recv`，反向调用 `LOG_ERROR` + `false`。方向管的是数据面，控制面握手两种方向都要用。
- 控制面同样按角色设限：主动端（建环并发出 rendezvous 的那一端）只能发出握手、被动端只能接收握手。否则 producer 未握手时的第一个 `Recv` 会把握手字节当成数据，绕过方向约束。
- 就绪：`GetFd()` listening 返回 rendezvous fd，建立后返回本端 eventfd（producer 等 space-ready，consumer 等 data-ready）；`GetPollEvents()` 恒为 `EPOLLIN`（eventfd 的可读就是就绪）。space-ready 初值 1，所以事件驱动 producer 首发送不必先轮询；data-ready 初值 0。建立到握手完成之间真正可推进的是 control socket，但该窗口内 executor 不会运行（握手在 channel init 阶段完成），executor 拿到的始终是 eventfd。
- 阻塞 `Send`/`Recv` 在环上靠 `poll()` 等本端 eventfd 完成；`TrySend`/`TryRecv` 绝不阻塞：推不动时先排空本端 eventfd 再复检环状态，然后才报「无进展」，一次成功调用最多通知对端一次。
- **Linux 专属**：实现依赖 `memfd_create`、`eventfd`、Unix domain socket（`SOCK_SEQPACKET`）、`SCM_RIGHTS`、`poll`/`epoll`。非 Linux 平台不提供该实现，也不加条件编译下的降级路径。

## 不变式与设计区间

### 不变式

- 数据面传输只走 `TrySend`/`TryRecv`（由 topology 状态机驱动）；executor 不直接调 `Send`/`Recv`。
- bootstrap/握手用 `Utils::SendAll`/`RecvAll`（带长度前缀），不走 Transport 的阻塞 `Send`/`Recv`。
- 非阻塞语义是硬约束：`TrySend`/`TryRecv` 绝不阻塞。
- 方向只是元数据，不是操作许可：TCP 在任何方向下都能收发（bootstrap 在同一个双向对象上收发控制消息）。方向只决定 `GetPollEvents()`（`Send`→`EPOLLOUT`，`Receive`→`EPOLLIN`，`Bidirectional`→两者）；只有按方向拒绝反向操作的实现（如共享内存端点）才把方向当约束。
- 就绪位的含义由 transport 决定，不由 operation 决定：socket 是「可写=发送可推进、可读=接收可推进」，而共享内存发送端等的是**可读**的 eventfd。所以 executor 一律用 `GetPollEvents()` 拿掩码，并用「就绪来自 send 还是 recv transport」决定推进哪个逻辑操作，不得自行把位解释成方向。
- `GetFd()` 返回的 fd 生命周期由 transport 管理；executor 只注册/注销。channel 边的 send/recv 是各自独立的 transport 对象，fd 必然不同（见 `AGENTS.md` 的 Known Conventions），executor 可以按「一个 transport 一个等待」处理，无需合并同一 fd 的多路注册。

### 设计区间

- `Transport` 抽象基类允许新增实现（TCP 之外，如 RDMA、共享内存、Unix domain socket）。
- `TrySend`/`TryRecv` 内部缓冲管理自由：是否缓存、每次推进多少字节均可自决。
- `SendRaw`/`RecvRaw` 的 EAGAIN 重试策略自由（忙等或其他）。

## 文件介绍

| 文件 | 职责 |
|------|------|
| `include/transport.h` | `Transport` 抽象基类 + `TransportDirection` |
| `include/transport_tcp.h` / `src/transport_tcp.cpp` | TCP 实现（含非阻塞、`GetFd`、`GetPollEvents`） |
| `include/transport_shm.h` / `src/transport_shm.cpp` | 共享内存实现：memfd 环 + 两个 eventfd、rendezvous 控制 socket、方向约束 |
| `tests/test_transport_shm.cpp` | fork 端点对的传输测试（rendezvous/描述符传递/握手/阻塞与非阻塞/回绕/反压/方向拒绝/释放） |

## 修改原则

- 改动本层不得破坏非阻塞语义：`TrySend`/`TryRecv` 绝不阻塞。
- 新增实现必须继承 `Transport` 基类并实现全部接口（含 `GetFd`/`GetPollEvents`）；`GetFd` 返回的 fd 生命周期由 transport 管理，executor 只注册/注销。
- 勿回退：数据面只走 `TrySend`/`TryRecv`（历史曾有人把数据面改成阻塞 `Send`/`Recv`，导致死锁）。共享内存的阻塞 `Send`/`Recv` 只服务于调用方的首个握手与同类控制用途，executor 与 topology 一律走 `Try*`。
- 勿回退：executor 不得硬编码 `EPOLLIN`/`EPOLLOUT`，一律取 `GetPollEvents()`。
- 共享内存实现不得引入自动回退到 TCP：任何建立/映射/传描述符/握手失败都直接让 communicator 初始化失败（`LOG_ERROR` + `false`）。
- 环容量固定 2 MiB，不做可配置/动态扩容；一个环只允许单 producer + 单 consumer，不新增双向或多生产者模型。
- 改动本层后跑 `scripts/run_tests.sh`：tier 0 只测传输本身（fork 端点对，无需 mpirun）；2/4 rank 的集合通信档位在默认选择与 `OCCL_DISABLE_SHM=1` 两种模式下各跑一遍（1 rank 无数据面，只跑一次）。
