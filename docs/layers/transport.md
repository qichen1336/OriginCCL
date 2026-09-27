# Transport 层（`include/transport/*.h`、`src/transport/*.cpp`）

三种传输实现：TCP socket、同机共享内存与 RDMA（CM 建链 + RC + write）。改动本层前阅读本文件。

transport 与 executor 一样单独成目录；头文件从 include 根限定引用，形如 `#include "transport/transport.h"`。其余层仍平铺。

# 核心职责边界

- 负责 socket 封装：建连、收发、关闭，并把 fd 暴露给 executor 注册监听。
- 两套接口：
  - 阻塞接口（控制面）：`Listen(addr, port)` / `Accept` / `Connect` 建连（成功后 socket 置 `O_NONBLOCK`）；`Send` / `Recv` 供 communicator 的 channel 握手等偶发控制面使用（bootstrap 的 `NodeInfo` 交换走裸 TCP socket + `Utils::SendAll`/`RecvAll`，不经过 Transport）。三种传输共用同一 `Listen` 签名，`addr` 语义按传输不同：TCP 忽略它（仍绑 `INADDR_ANY`）、共享内存把它当 rendezvous 路径、RDMA 把它当绑定设备地址。
  - 非阻塞接口（数据面）：`TrySend(data, size, *progress, *done)` / `TryRecv(...)` 单次推进到 EAGAIN 为止；返回 false 表对端关闭或真错误。
  - 就绪契约：`GetFd()` 给出要等待的描述符（listening 时为 listen fd，连上后为数据面 fd）；`GetPollEvents()` 给出该描述符的原生就绪掩码（Linux epoll 掩码）；`SetDirection()`/`GetDirection()` 维护方向元数据（`Bidirectional`（默认）/ `Send` / `Receive`）。
  - `Close()` / `IsConnected()`。
- 不负责：不决定等待策略（executor）、不推进算法（topology）、不监听 fd（executor）。

# 文件介绍

| 文件 | 职责 |
|------|------|
| `include/transport/transport.h` | `Transport` 抽象基类 + `TransportDirection` |
| `include/transport/transport_tcp.h` / `src/transport/transport_tcp.cpp` | TCP 实现（含非阻塞、`GetFd`、`GetPollEvents`） |
| `include/transport/transport_shm.h` / `src/transport/transport_shm.cpp` | 共享内存实现：memfd 环 + 两个 eventfd、rendezvous 控制 socket、方向约束 |
| `include/transport/transport_rdma.h` / `src/transport/transport_rdma.cpp` | RDMA 实现：CM 建链、RC QP、`WRITE_WITH_IMM` 环形缓冲、credit 回收、completion channel 就绪 fd、设备探测 |
| `tests/test_transport_shm.cpp` | fork 端点对的传输测试（rendezvous/描述符传递/握手/阻塞与非阻塞/回绕/反压/方向拒绝/释放） |
| `tests/test_transport_rdma.cpp` | mpirun 端点对的 RDMA 测试（CM 握手、RC、write、槽位回收、1B 到 5MiB 传输） |

# 实现原理

### 共享内存（`TransportShm`）

- 与 TCP 同构的 listener/connection 双形态：`Listen(path, 0)` + `Accept()`（被动端），`Connect(rendezvous_path, port)`（主动端，`port` 忽略）。`Listen` 的 `port` 参数无意义，`GetListenPort()` 恒返回 0。
- 建立流程：主动端建 2 MiB 数据容量的 memfd 环 + data-ready/space-ready 两个 eventfd，用 `SOCK_SEQPACKET` + `SCM_RIGHTS` 一次性传给对端并带方向；被动端取补方向。环元数据是 cache line 分隔、单调递增的 `std::atomic<uint64_t>` head/tail（producer 写 head、consumer 写 tail，acquire/release 配对）。
- 控制 socket 与数据面分开：调用方第一次阻塞 `Send`/`Recv`（即 communicator 握手）走 control socket，接收方回 1 字节 ack，双方随即关闭；之后所有操作走环。
- 方向在此是约束：producer 只 `Send`、consumer 只 `Recv`，反向 `LOG_ERROR` + `false`。方向只约束数据面。
- 就绪：`GetFd()` listening 返回 rendezvous fd，建立后返回本端 eventfd（producer 等 space-ready，consumer 等 data-ready）；`GetPollEvents()` 恒 `EPOLLIN`。space-ready 初值 1（事件驱动 producer 首发送不必先轮询），data-ready 初值 0。eventfd 计数单调累积（`Notify` 只增、`Drain` 只在 `Try*` 内），是 EPOLLET 的前提。
- 阻塞 `Send`/`Recv` 在环上靠 `poll()` 等本端 eventfd；`TrySend`/`TryRecv` 绝不阻塞，推不动时先排空本端 eventfd 再复检环状态，一次成功调用最多通知对端一次。
- Linux 专属：依赖 `memfd_create`、`eventfd`、Unix domain socket、`SCM_RIGHTS`、`poll`/`epoll`。

### RDMA（`TransportRDMA`）

- 建链用 RDMA CM，数据面用 RC + `IBV_WR_RDMA_WRITE_WITH_IMM`：`Listen(addr, port)` 用 `rdma_listen`（`port=0` 时内核选端口，用 `rdma_get_local_addr` 读回真实端口）；主动端 `rdma_resolve_addr` → `rdma_resolve_route` → `rdma_connect`，被动端 `rdma_accept`，QP 都是 `IBV_QPT_RC`。
- 对端内存信息走 CM private data（`Wire{base_addr, rkey}`），两端在事件里直接得到；被动端在 `CONNECT_REQUEST`、主动端在 `ESTABLISHED` 事件里。
- 握手与数据方向解耦：控制通道由「主动连接方先 `Send`、被动方先 `Recv`」决定，与 `SetDirection` 无关——谁主动连接只看 rank 大小（`rank < peer` 的一端 `Connect`）。调用方首次阻塞 `Send`/`Recv` 走 RC `IBV_WR_SEND`。
- 数据面是 2 MiB 预注册环形缓冲，`64 KiB × 32` 槽位：producer 把数据 `memcpy` 进当前槽后 post write；`*progress` 表示已被读入自有槽并提交的字节，`*done` 置位后调用方缓冲区即可复用。
- 槽位复用只由 credit 一个门控：可发窗口是 `credits_received + kRdmaSlotCount`；credit 蕴含「本地读已完成」，故 `IBV_WC_RDMA_WRITE` 完成事件被忽略。credit 反向归还：consumer 交还整个槽后用一次 `WRITE_WITH_IMM` 写对端控制区（payload 1 B，不受方向限制）。immediate 的位布局是两种用途共用：bit 31 为 credit 标志，bit 16–30 是槽号，bit 0–15 是 `length - 1`（长度减 1 才能双射进 16 bit）。`kCreditBatch = 8` 批量归还，队列排空时立刻归还余数。
- 环容量是在途窗口（在途 write ≤ 32 槽），不是每条消息配额；远大于 2 MiB 的消息分多轮推完。
- `WRITE_WITH_IMM` 消耗接收方 RQ 的 WQE，接收队列是纯 credit 池（预投 `kRdmaRecvPool` 个空 WQE，每收到一个 write-imm 立即补投），不预投会 RNR。
- 就绪与 EPOLLET：`GetFd()` 连接后返回 completion channel fd，监听态返回 CM channel fd；`GetPollEvents()` 恒 `EPOLLIN`。就绪 fd 只表达数据到来，对端消失经被 flush 的接收 WQE 产生 CQE、`HandleCompletion` 判为 `Try*` 的 `false`。`Try*` 非阻塞 drain 后必须 `ibv_get_cq_event` → `ibv_ack_cq_events` → `ibv_req_notify_cq` 重新 arm 再 poll CQ，否则漏下一次边沿。
- 设备探测：`Probe(addr)` 遍历 `ibv_get_device_list()` 的每个设备与端口，要求 `IBV_PORT_ACTIVE`，再扫 GID 表找 IPv4-mapped GID（前十个字节为 0 且 `raw[10] == raw[11] == 0xFF`），取末 4 字节成地址。不能复用 `Utils::GetLocalIPAddress()` 的结果（那可能不是 RDMA 网卡）。
- 限制：Linux + `libibverbs`/`librdmacm` 是硬依赖。首版接受一次用户缓冲 ↔ 注册缓冲的拷贝，不做零拷贝/RDMA Read/多 rail。
- 测试用 `mpirun` 而非 fork（verbs/CM 初始化后只 fork 不 exec，子进程 `ibv_post_send` 报 EPERM）。

# 隐含约定

- **数据面传输只走 `TrySend`/`TryRecv`**（由 topology 状态机驱动）；executor 不直接调 `Send`/`Recv`。历史曾有人把数据面改成阻塞 `Send`/`Recv` 导致死锁。
- **两类控制流量走两条路**：bootstrap 的 `NodeInfo` 交换走裸 TCP socket（`Utils::SendAll`/`RecvAll`），不碰 Transport；communicator 的 channel 握手（`ConnHandshake`）走 Transport 的阻塞 `Send`/`Recv`——这是阻塞接口存在的唯一理由（SHM 走 control socket、RDMA 走一次 RC `IBV_WR_SEND`，三种语义一致，调用点不区分传输类型）。
- **非阻塞语义是硬约束**：`TrySend`/`TryRecv` 绝不阻塞。
- **方向对 TCP 只是元数据、不是操作许可**：TCP 任何方向都能收发，方向只决定 `GetPollEvents()`。channel 握手恒由主动连接方先 `Send`、被动方先 `Recv`，所以一条标成 `Receive` 的连接的主动端仍要在它上面 `Send`——不要给 TCP 加反向拒绝的防御。共享内存端点才把方向当硬约束。
- **就绪位含义由 transport 决定**：socket 是「可写=发送推进、可读=接收推进」，共享内存发送端等的是可读的 eventfd。executor 一律用 `GetPollEvents()`，用「就绪来自 send 还是 recv transport」决定推进哪个逻辑操作，不得自行把位解释成方向。
- `GetFd()` 返回的 fd 生命周期由 transport 管理，executor 只注册/注销。channel 边 send/recv 是各自独立 transport，fd 必然不同（含 2 rank `prev == next` 退化情形，见 [communicator.md](communicator.md)）。
- **共享内存失败即初始化失败**，不得自动回退 TCP（`OCCL_DISABLE_SHM=1` 是显式选择）。RDMA 同理不得自动回退（`OCCL_DISABLE_RDMA=1` 是显式选择）。
- **RDMA 接收队列必须预投递**：`WRITE_WITH_IMM` 消耗 RQ WQE，少投一个会 RNR，环形缓冲无法建立。
- **RDMA 槽位复用只保留 credit 一道门控**：credit 已蕴含「本地已读完该槽」，补一道本地 send CQE（`local_completed`）只会收紧窗口、不会更安全。
- **RDMA `private_data` 保持裸 `Wire{base_addr, rkey}`**：连接合法性由 CM 保证，无 magic 校验。
- **`Try*` 必须同时排空 completion channel（ack + re-arm）并 poll CQ**：直接 poll CQ 不重新 arm 会在 EPOLLET 下漏边沿。
- **RDMA 地址必须由 `Probe` 从设备的 IPv4-mapped GID 得出**，不能复用 `Utils::GetLocalIPAddress()` 结果。
- 环容量固定 2 MiB 不做配置/扩容；隐含保证是单环单 producer + 单 consumer，不做容量协商/多生产者/双向的防御分支。
- 改动本层后跑 `scripts/run_tests.sh`：tier 0 测 SHM 传输本身（fork 端点对），tier 11 测 RDMA 传输本身（mpirun 端点对，无设备 SKIP）；2/4 rank 集合通信档位在默认选择与 `OCCL_DISABLE_SHM=1`/`OCCL_DISABLE_RDMA=1` 覆盖下各跑一遍。
