# Transport 层（`include/transport*.h`、`src/transport_tcp.cpp`、`src/transport_shm.cpp`）

TCP socket 与同机共享内存 ring 的封装。改动本层前阅读本文件。

## 核心职责边界

- 负责数据面连接封装：建连、收发、关闭，并把等待句柄暴露给 executor。
- 负责两套接口：
  - 阻塞接口（控制面）：`Listen` / `Accept` / `Connect` 建连（成功后 socket 置 `O_NONBLOCK`）；`Send` / `Recv`（内部 `SendRaw`/`RecvRaw`，对 EAGAIN 忙等重试），供 bootstrap 握手等偶发控制面使用。
  - 非阻塞接口（数据面）：`TrySend(data, size, *progress, *done)` / `TryRecv(...)` 单次推进到 EAGAIN 为止，`*progress` 累计已做字节，`*done` 表示完成；返回 false 表对端关闭或真错误。
  - 等待接口：`SendWait()` / `RecvWait()` 各返回一个 `WaitDescriptor{fd, condition}`，说明该方向要等的 fd 与就绪条件（readable / writable）。
  - `Close()` / `IsConnected()`。
- 不负责：不决定等待策略（executor）、不推进算法（topology）、不监听 fd（executor）。

## 不变式与设计区间

### 不变式

- 数据面传输只走 `TrySend`/`TryRecv`（由 topology 状态机驱动）；executor 不直接调 `Send`/`Recv`。
- bootstrap/握手用 `Utils::SendAll`/`RecvAll`（带长度前缀），不走 Transport 的阻塞 `Send`/`Recv`。
- 非阻塞语义是硬约束：`TrySend`/`TryRecv` 绝不阻塞。
- 等待契约是「每方向一个描述符，condition 说明该 fd 要变成 readable 还是 writable」：TCP 发送等 socket 可写、接收等可读；共享内存两个方向都等 eventfd 可读。executor 不得假设 fd 固定对应 socket 的 IN/OUT，也不得识别 transport 类型。
- 描述符只表达**逻辑方向**：recv transport 的描述符触发 `CollEvent::Readable`，send transport 的触发 `CollEvent::Writable`；描述符的 fd 在 transport 整个生命周期内有效（executor 跨 plan 注册/注销）。
- 发送侧描述符必须是**电平**（有空间即可读）：首个 send 与后续每个 step 的 send 都靠它被喂事件才会发生。
- 对端关闭后 `TryRecv` 先排空 ring 中已到达的字节，排空且未完成才失败（等价于 socket 先读缓冲再 EOF）。
- 共享内存 ring 的 head/tail 只存在共享头（各占一条 cache line、单调字节计数），进程内不缓存任何一端游标。

### 设计区间

- `Transport` 抽象基类允许新增实现（TCP 之外，如 RDMA、共享内存、Unix domain socket）。
- `TrySend`/`TryRecv` 内部缓冲管理自由：是否缓存、每次推进多少字节均可自决。
- `SendRaw`/`RecvRaw` 的 EAGAIN 重试策略自由（忙等或其他）。

## 文件介绍

| 文件 | 职责 |
|------|------|
| `include/transport.h` | `Transport` 抽象基类 + `WaitDescriptor` / `WaitCondition` |
| `include/transport_tcp.h` / `src/transport_tcp.cpp` | TCP 实现（发送=可写、接收=可读） |
| `include/transport_shm.h` / `src/transport_shm.cpp` | 同机共享内存实现：每条有向 edge 一个 memfd SPSC ring（1 MiB）+ 两个 eventfd 电平，资源经 abstract UDS + `SCM_RIGHTS` 交接 |

## 修改原则

- 改动本层不得破坏非阻塞语义：`TrySend`/`TryRecv` 绝不阻塞。
- 新增实现必须继承 `Transport` 基类并实现全部接口；`SendWait`/`RecvWait` 返回的 fd 生命周期由 transport 管理，executor 只注册/注销。
- 共享内存 ring 的两端分属两个进程，所以游标只放共享头，绝不缓存在 transport 对象里（缓存会让另一端的写不可见）。owner 端用 relaxed 读自己的游标、acquire 读对端游标，发布用 release。
- 共享内存 `Ring`（含 `std::atomic`）创建端必须 `placement new` 启动对象生命周期，接收端只 `mmap` 不初始化；编译期 `static_assert(std::atomic<uint64_t/uint32_t>::is_always_lock_free)`。
- 共享内存通知是「电平」而非一次性事件，且**清电平必须在读 ring 之前**：先清再查才能既不丢唤醒又不留下永久可读的假电平。
- 勿回退：数据面只走 `TrySend`/`TryRecv`（历史曾有人把数据面改成阻塞 `Send`/`Recv`，导致死锁）。
