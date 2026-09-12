# Transport 层（`include/transport*.h`、`src/transport_tcp.cpp`）

TCP socket 封装。改动本层前阅读本文件。

## 接口契约

- 阻塞建连：`Listen` / `Accept` / `Connect`。建连成功后 socket 置 `O_NONBLOCK`。
- 阻塞收发：`Send` / `Recv`（内部 `SendRaw`/`RecvRaw`，对 EAGAIN 忙等重试）。供 bootstrap 握手等偶发控制面使用。
- 非阻塞收发（数据面）：
  - `TrySend(data, size, *progress, *done)` / `TryRecv(...)`：单次推进到 EAGAIN 为止，`*progress` 累计已做字节，`*done` 表示完成。返回 false 表对端关闭或真错误。
  - `GetFd()`：暴露 `sockfd` 供 executor 注册监听（poll/epoll）。
- `Close()` / `IsConnected()`。

## 不变式

- 数据面传输只走 `TrySend`/`TryRecv`（由 topology 状态机驱动）；executor 不直接调 `Send`/`Recv`。
- bootstrap/握手用 `Utils::SendAll`/`RecvAll`（带长度前缀），不走 Transport 的阻塞 `Send`/`Recv`。
- 修改本层不得破坏非阻塞语义：`TrySend`/`TryRecv` 绝不阻塞。

## 文件

| 文件 | 职责 |
|------|------|
| `include/transport.h` | `Transport` 抽象基类 |
| `include/transport_tcp.h` / `src/transport_tcp.cpp` | TCP 实现（含非阻塞与 `GetFd`） |
