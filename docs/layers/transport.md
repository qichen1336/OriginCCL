# Transport 层（`include/transport*.h`、`src/transport_tcp.cpp`）

TCP socket 封装。改动本层前阅读本文件。

## 核心职责边界

- 负责 socket 封装：建连、收发、关闭，并把 fd 暴露给 executor 注册监听。
- 负责两套接口：
  - 阻塞接口（控制面）：`Listen` / `Accept` / `Connect` 建连（成功后 socket 置 `O_NONBLOCK`）；`Send` / `Recv`（内部 `SendRaw`/`RecvRaw`，对 EAGAIN 忙等重试），供 bootstrap 握手等偶发控制面使用。
  - 非阻塞接口（数据面）：`TrySend(data, size, *progress, *done)` / `TryRecv(...)` 单次推进到 EAGAIN 为止，`*progress` 累计已做字节，`*done` 表示完成；返回 false 表对端关闭或真错误。
  - 就绪契约：`GetFd()` 给出 executor 要等待的描述符（listening 时为 listen fd，连上后为数据面 fd）；`GetPollEvents()` 给出该描述符上要等待的原生就绪掩码（Linux epoll 掩码 `EPOLLIN`/`EPOLLOUT`，与 poll 位值一致）；`SetDirection()` / `GetDirection()` 维护方向元数据（`Bidirectional`（默认）/ `Send` / `Receive`）。
  - `Close()` / `IsConnected()`。
- 不负责：不决定等待策略（executor）、不推进算法（topology）、不监听 fd（executor）。

## 不变式与设计区间

### 不变式

- 数据面传输只走 `TrySend`/`TryRecv`（由 topology 状态机驱动）；executor 不直接调 `Send`/`Recv`。
- bootstrap/握手用 `Utils::SendAll`/`RecvAll`（带长度前缀），不走 Transport 的阻塞 `Send`/`Recv`。
- 非阻塞语义是硬约束：`TrySend`/`TryRecv` 绝不阻塞。
- 方向只是元数据，不是操作许可：TCP 在任何方向下都能收发（bootstrap 在同一个双向对象上收发控制消息）。方向只决定 `GetPollEvents()`（`Send`→`EPOLLOUT`，`Receive`→`EPOLLIN`，`Bidirectional`→两者）；只有按方向拒绝反向操作的实现（如共享内存端点）才把方向当约束。
- 就绪位的含义由 transport 决定，不由 operation 决定：socket 是「可写=发送可推进、可读=接收可推进」，而共享内存发送端等的是**可读**的 eventfd。所以 executor 一律用 `GetPollEvents()` 拿掩码，并用「就绪来自 send 还是 recv transport」决定推进哪个逻辑操作，不得自行把位解释成方向。
- `GetFd()` 返回的 fd 生命周期由 transport 管理；同一个 transport 在两个方向上可以给出同一个 fd，executor 必须按 fd 合并注册。

### 设计区间

- `Transport` 抽象基类允许新增实现（TCP 之外，如 RDMA、共享内存、Unix domain socket）。
- `TrySend`/`TryRecv` 内部缓冲管理自由：是否缓存、每次推进多少字节均可自决。
- `SendRaw`/`RecvRaw` 的 EAGAIN 重试策略自由（忙等或其他）。

## 文件介绍

| 文件 | 职责 |
|------|------|
| `include/transport.h` | `Transport` 抽象基类 + `TransportDirection` |
| `include/transport_tcp.h` / `src/transport_tcp.cpp` | TCP 实现（含非阻塞、`GetFd`、`GetPollEvents`） |

## 修改原则

- 改动本层不得破坏非阻塞语义：`TrySend`/`TryRecv` 绝不阻塞。
- 新增实现必须继承 `Transport` 基类并实现全部接口（含 `GetFd`/`GetPollEvents`）；`GetFd` 返回的 fd 生命周期由 transport 管理，executor 只注册/注销。
- 勿回退：数据面只走 `TrySend`/`TryRecv`（历史曾有人把数据面改成阻塞 `Send`/`Recv`，导致死锁）。
- 勿回退：executor 不得硬编码 `EPOLLIN`/`EPOLLOUT`，一律取 `GetPollEvents()`。
