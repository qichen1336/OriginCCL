# Transport 层（`include/transport/*.h`、`src/transport/*.cpp`）

三种传输实现：TCP socket、同机共享内存与 RDMA（CM 建链 + RC + write）。改动本层前阅读本文件。

transport 与 executor 一样单独成目录；头文件从 include 根限定引用，形如 `#include
"transport/transport.h"`、`#include "transport/transport_shm.h"`。其余层仍是
`include/`、`src/` 平铺。

## 核心职责边界

- 负责 socket 封装：建连、收发、关闭，并把 fd 暴露给 executor 注册监听。
- 负责两套接口：
  - 阻塞接口（控制面）：`Listen(addr, port)` / `Accept` / `Connect` 建连（成功后 socket 置 `O_NONBLOCK`）；`Send` / `Recv`（内部 `SendRaw`/`RecvRaw`，对 EAGAIN 忙等重试），供 bootstrap 握手等偶发控制面使用。三种传输共用同一个 `Listen` 签名，`addr` 语义按传输不同：TCP 忽略它（仍绑 `INADDR_ANY`）、共享内存把它当 rendezvous 路径、RDMA 把它当绑定的设备地址。
  - 非阻塞接口（数据面）：`TrySend(data, size, *progress, *done)` / `TryRecv(...)` 单次推进到 EAGAIN 为止，`*progress` 累计已做字节，`*done` 表示完成；返回 false 表对端关闭或真错误。
  - 就绪契约：`GetFd()` 给出 executor 要等待的描述符（listening 时为 listen fd，连上后为数据面 fd）；`GetPollEvents()` 给出该描述符上要等待的原生就绪掩码（Linux epoll 掩码 `EPOLLIN`/`EPOLLOUT`，与 poll 位值一致）；`SetDirection()` / `GetDirection()` 维护方向元数据（`Bidirectional`（默认）/ `Send` / `Receive`）。
  - `Close()` / `IsConnected()`。
- 不负责：不决定等待策略（executor）、不推进算法（topology）、不监听 fd（executor）。

### 共享内存传输（`TransportShm`）

- 与 TCP 同构的 listener/connection 双形态：`Listen(path, 0)` + `Accept()`（被动端），`Connect(rendezvous_path, port)`（主动端，`port` 忽略）。`Listen` 的 `port` 参数对共享内存无意义（`(void)port`），`GetListenPort()` 恒返回 0。空路径被 `RendezvousAddress::Set` 拒绝。
- 建立流程：主动端建 2 MiB 数据容量的 memfd 环 + data-ready/space-ready 两个 eventfd，用 `SOCK_SEQPACKET` + `SCM_RIGHTS` 一次性传给对端并带上自己的方向；被动端取补方向。环元数据是 cache line 分隔、单调递增的 `std::atomic<uint64_t>` head/tail（producer 写 head，consumer 写 tail，acquire/release 配对）。
- 控制 socket 与数据面分开：调用方**第一次**阻塞 `Send`/`Recv`（即 communicator 的连接握手）走 control socket，接收方回 1 字节 ack，双方随即关闭 control socket；之后所有阻塞与非阻塞操作都走环。所以调用点不需要区分传输类型。
- 方向在此**是**约束（与 TCP 不同）：producer 只 `Send`、consumer 只 `Recv`，反向调用 `LOG_ERROR` + `false`。方向只约束数据面；control socket 存续期间不发方向校验，由调用方按自己的角色使用。
- 就绪：`GetFd()` listening 返回 rendezvous fd，建立后返回本端 eventfd（producer 等 space-ready，consumer 等 data-ready）；`GetPollEvents()` 恒为 `EPOLLIN`（eventfd 的可读就是就绪）。space-ready 初值 1，所以事件驱动 producer 首发送不必先轮询；data-ready 初值 0。建立到握手完成之间真正可推进的是 control socket，但该窗口内 executor 不会运行（握手在 channel init 阶段完成），executor 拿到的始终是 eventfd。eventfd 计数单调累积（`Notify` 只增、`Drain` 只在 `Try*` 内发生），是 executor 用 `EPOLLET` 的前提：挂起未 Drain 的通知不会因切换触发模式而丢失。
- 阻塞 `Send`/`Recv` 在环上靠 `poll()` 等本端 eventfd 完成；`TrySend`/`TryRecv` 绝不阻塞：推不动时先排空本端 eventfd 再复检环状态，然后才报「无进展」，一次成功调用最多通知对端一次。`Try*` 必须排空到不能再推进（drain+复检），这是 `EPOLLET` 下不漏边沿的契约；TCP 侧对应「读到 EAGAIN 才停」（`include/transport/transport.h` 有同一条注释）。
- **Linux 专属**：实现依赖 `memfd_create`、`eventfd`、Unix domain socket（`SOCK_SEQPACKET`）、`SCM_RIGHTS`、`poll`/`epoll`。非 Linux 平台不提供该实现，也不加条件编译下的降级路径。

### RDMA 传输（`TransportRDMA`）

- **建链用 RDMA CM，数据面用 RC + `IBV_WR_RDMA_WRITE_WITH_IMM`**：`Listen(addr, port)` 在 RDMA 设备地址上 `rdma_listen`（`port=0` 时由内核选端口，用 `rdma_get_local_addr` 读回真实端口，因为 `rdma_get_src_port` 在部分 librdmacm 版本上返回错误值）；主动端 `rdma_resolve_addr` → `rdma_resolve_route` → `rdma_connect`，被动端在 `CONNECT_REQUEST` 上 `rdma_accept`，两端 QP 都是 `IBV_QPT_RC`。
- **对端内存信息走 CM private data，不是额外一轮阻塞握手**：`private_data` 携带 `{base_addr, rkey, magic}`，两端在事件里直接得到，避免与调用方握手时序耦合。**被动端的对端信息在 `CONNECT_REQUEST` 事件里，主动端在 `ESTABLISHED` 事件里**——这是本实现的隐含保证，据此不同一套「先 RECV 再 ACCEPT」的同步。
- **握手与控制流与数据方向解耦**：控制通道由「主动连接方先 `Send`、被动方先 `Recv`」决定，与 `SetDirection` 无关（主动连接方可能是数据消费者）。调用方首次阻塞 `Send`/`Recv` 走 RC `IBV_WR_SEND`，**完成即返回，不会顺便把同一 buffer 当成数据流的第一段**；之后非生产者方向的阻塞 `Send`/`Recv` 是 no-op 且成功。bootstrap 的控制流量因此可以跑在单向连接上。
- **数据面是 2 MiB 预注册环形缓冲，`64 KiB × 32` 槽位**：producer 把调用方数据 `memcpy` 进当前槽（staging）后再 post write，consumer 从槽里读。**这解释了 `TrySend` 的语义**：`*progress` 表示已被读入 transport 自有槽并成功提交的字节；`*done` 置位后调用方缓冲区即可立即复用（transport 不再读用户缓冲）。
- **槽位复用有两个独立门控，缺一不可**：本地槽必须等对应 send CQE（`local_completed`）才能重写，远端槽必须等 consumer 归还 credit（`credits_received`）才能重写，发送窗口取两者较小值。只用其中之一会破坏数据。
- **credit 反向归还**：consumer 在把整个槽交给调用方后，用一次 `WRITE_WITH_IMM` 写对端控制区，immediate 高位标记「这是 credit」、低位是归还槽数；producer 用这一路回收远端槽。credit 用 write 而不是 SEND，是为了不受方向限制。
- **`WRITE_WITH_IMM` 会消耗接收方 RQ 里的 WQE，payload 却落在 WR 指定的 `remote_addr`**（不占接收 buffer）：因此接收队列是纯 credit 池（`kRdmaRecvPool` 个空 WQE，绑定 scratch 区），每收到一个 write-imm 就立刻补投一个。没投就发会得到 RNR，环形缓冲无法启动。
- **就绪与 EPOLLET**：`GetFd()` 连接后返回内部聚合 epoll fd（同时监听 comp channel 与 CM channel，所以断链也能唤醒），监听态返回 CM channel fd；`GetPollEvents()` 恒为 `EPOLLIN`。`TrySend`/`TryRecv` 非阻塞地 drain 完成后，**必须** `ibv_get_cq_event` → `ibv_ack_cq_events` → `ibv_req_notify_cq` 重新 arm 再 poll CQ，只 poll CQ 而不排空 completion channel 会漏掉下一次边沿。
- **设备探测**：`TransportRDMA::Probe(addr)` 遍历非 loopback 的 IPv4 地址，用 `rdma_bind_addr` + `ibv_query_port` 找出「绑定成功且端口 `ACTIVE`」的那个地址。本机 `GetLocalIPAddress()` 返回的 `enp0s3` 不是 RDMA 网卡，所以**不能把通用 IP 探测结果当 RDMA 地址用**；找不到这样的地址就表示本机不做 RDMA。
- **限制**：Linux + `libibverbs`/`librdmacm` 是硬依赖（CMake 缺库直接失败）。首版接受一次用户缓冲 ↔ 注册缓冲的拷贝，不做端到端零拷贝、不做 RDMA Read、不做多 rail 或动态缓冲扩缩。
- **测试用 `mpirun` 而不是 fork**：verbs/CM 初始化后若只 `fork` 不 `exec`，子进程的 `ibv_post_send` 会以 `EPERM` 失败（本机 rxe 实测），所以 `tests/test_transport_rdma.cpp` 用两个独立进程。

## 不变式与设计区间

### 不变式

- 数据面传输只走 `TrySend`/`TryRecv`（由 topology 状态机驱动）；executor 不直接调 `Send`/`Recv`。
- bootstrap/握手用 `Utils::SendAll`/`RecvAll`（带长度前缀），不走 Transport 的阻塞 `Send`/`Recv`。
- 非阻塞语义是硬约束：`TrySend`/`TryRecv` 绝不阻塞。
- 方向只是元数据，不是操作许可：TCP 在任何方向下都能收发。方向只决定 `GetPollEvents()`（`Send`→`EPOLLOUT`，`Receive`→`EPOLLIN`，`Bidirectional`→两者）；只有按方向拒绝反向操作的实现（如共享内存端点）才把方向当约束。**TCP 不能像共享内存那样拒绝反向，这是代码的隐含保证**：bootstrap 把控制流量跑在单个双向对象上（同一个 `Transport` 既发又收），拒绝反向会立刻打断握手；因此「方向是硬约束」只发生在数据面端点（见 [communicator.md](communicator.md) 的 channel 有向边），不要据此给 TCP 加反向拒绝的防御。
- 就绪位的含义由 transport 决定，不由 operation 决定：socket 是「可写=发送可推进、可读=接收可推进」，而共享内存发送端等的是**可读**的 eventfd。所以 executor 一律用 `GetPollEvents()` 拿掩码，并用「就绪来自 send 还是 recv transport」决定推进哪个逻辑操作，不得自行把位解释成方向。
- `GetFd()` 返回的 fd 生命周期由 transport 管理；executor 只注册/注销。**隐含保证：channel 边的 send/recv 是各自独立的 transport 对象，fd 必然不同**（含 2 rank 下 `prev == next` 的退化情形，见 [communicator.md](communicator.md) 的「channel 边是有向的」）；executor 据此按「一个 transport 一个等待」处理，无需（也不该）为同一 fd 的多路注册做合并这类防御。

### 设计区间

- `Transport` 抽象基类允许新增实现（TCP 之外，如 RDMA、共享内存、Unix domain socket）。
- `TrySend`/`TryRecv` 内部缓冲管理自由：是否缓存、每次推进多少字节均可自决。
- `SendRaw`/`RecvRaw` 的 EAGAIN 重试策略自由（忙等或其他）。

## 文件介绍

| 文件 | 职责 |
|------|------|
| `include/transport/transport.h` | `Transport` 抽象基类 + `TransportDirection` |
| `include/transport/transport_tcp.h` / `src/transport/transport_tcp.cpp` | TCP 实现（含非阻塞、`GetFd`、`GetPollEvents`） |
| `include/transport/transport_shm.h` / `src/transport/transport_shm.cpp` | 共享内存实现：memfd 环 + 两个 eventfd、rendezvous 控制 socket、方向约束 |
| `include/transport/transport_rdma.h` / `src/transport/transport_rdma.cpp` | RDMA 实现：CM 建链、RC QP、`WRITE_WITH_IMM` 环形缓冲、credit 回收、聚合就绪 fd、设备探测 |
| `tests/test_transport_shm.cpp` | fork 端点对的传输测试（rendezvous/描述符传递/握手/阻塞与非阻塞/回绕/反压/方向拒绝/释放） |
| `tests/test_transport_rdma.cpp` | mpirun 端点对的 RDMA 测试（CM 握手、RC、write、槽位回收、1B 到 5MiB 传输） |

## 修改原则

- 改动本层不得破坏非阻塞语义：`TrySend`/`TryRecv` 绝不阻塞。
- 新增实现必须继承 `Transport` 基类并实现全部接口（含 `GetFd`/`GetPollEvents`）；`GetFd` 返回的 fd 生命周期由 transport 管理，executor 只注册/注销。
- 勿回退：数据面只走 `TrySend`/`TryRecv`（历史曾有人把数据面改成阻塞 `Send`/`Recv`，导致死锁）。共享内存的阻塞 `Send`/`Recv` 只服务于调用方的首个握手与同类控制用途，executor 与 topology 一律走 `Try*`。
- 勿回退：control socket 阶段**不加**角色门禁（主动端只能发、被动端只能收）。隐含保证是调用方（communicator）两端角色固定，门禁永不触发；加它就是多一个死分支。
- 勿回退：executor 不得硬编码 `EPOLLIN`/`EPOLLOUT`，一律取 `GetPollEvents()`。
- 共享内存实现不得引入自动回退到 TCP：任何建立/映射/传描述符/握手失败都直接让 communicator 初始化失败（`LOG_ERROR` + `false`）。主路径优先：失败即停，不做降级重试。
- RDMA 同理不得自动回退：设备可用且全局协商成立后，CM/QP/MR/写路径的任何错误都必须让初始化失败，不静默换成 TCP（`OCCL_DISABLE_RDMA=1` 是显式选择，不是回退）。
- 勿回退：RDMA 的接收队列必须预先投递。`WRITE_WITH_IMM` 消耗 RQ WQE，少投一个就会 RNR，环形缓冲无法建立。
- 勿回退：`Try*` 必须同时排空 completion channel（ack + re-arm）并 poll CQ。直接 poll CQ 不重新 arm 会在 EPOLLET 下漏掉边沿。
- 勿回退：RDMA 地址必须由 `Probe` 通过 `rdma_bind_addr` + `ibv_query_port` 验证，不能复用 `Utils::GetLocalIPAddress()` 的结果（那可能不是 RDMA 网卡）。
- 环容量固定 2 MiB，不做可配置/动态扩容；**隐含保证是单环单 producer + 单 consumer**，据此不做容量协商、多生产者或双向的防御分支。
- 改动本层后跑 `scripts/run_tests.sh`：tier 0 只测传输本身（fork 端点对，无需 mpirun），tier 11 测 RDMA 传输本身（mpirun 端点对，无设备时 SKIP）；2/4 rank 的集合通信档位在默认选择与 `OCCL_DISABLE_SHM=1`/`OCCL_DISABLE_RDMA=1` 覆盖下各跑一遍（1 rank 无数据面，只跑一次）。
