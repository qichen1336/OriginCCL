# Transport 层（`include/transport/*.h`、`src/transport/*.cpp`）

三种传输实现：TCP socket、同机共享内存与 RDMA（CM 建链 + RC + write）。改动本层前阅读本文件。

transport 与 executor 一样单独成目录；头文件从 include 根限定引用，形如 `#include
"transport/transport.h"`、`#include "transport/transport_shm.h"`。其余层仍是
`include/`、`src/` 平铺。

## 核心职责边界

- 负责 socket 封装：建连、收发、关闭，并把 fd 暴露给 executor 注册监听。
- 负责两套接口：
  - 阻塞接口（控制面）：`Listen(addr, port)` / `Accept` / `Connect` 建连（成功后 socket 置 `O_NONBLOCK`）；`Send` / `Recv`（TCP 内部 `SendRaw`/`RecvRaw`，对 EAGAIN 忙等重试），供 communicator 的 channel 握手等偶发控制面使用（bootstrap 的 `NodeInfo` 交换走裸 TCP socket + `Utils::SendAll`/`RecvAll`，不经过 Transport）。三种传输共用同一个 `Listen` 签名，`addr` 语义按传输不同：TCP 忽略它（仍绑 `INADDR_ANY`）、共享内存把它当 rendezvous 路径、RDMA 把它当绑定的设备地址。
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
- **对端内存信息走 CM private data，不是额外一轮阻塞握手**：`private_data` 携带 `TransportRDMA::Wire`，只有 `{base_addr, rkey}` 两个字段（无校验魔法数，连接本身由 CM 保证），两端在事件里直接得到，避免与调用方握手时序耦合。**被动端的对端信息在 `CONNECT_REQUEST` 事件里，主动端在 `ESTABLISHED` 事件里**——这是本实现的隐含保证，据此不需要一套「先 RECV 再 ACCEPT」的同步。
- **握手与控制流与数据方向解耦**：控制通道由「主动连接方先 `Send`、被动方先 `Recv`」决定，与 `SetDirection` 无关——**谁主动连接只看 rank 大小**（`rank < peer` 的一端 `Connect`），与它是该边的生产者还是消费者无关。调用方首次阻塞 `Send`/`Recv` 走 RC `IBV_WR_SEND`，**完成即返回，不会顺便把同一 buffer 当成数据流的第一段**；因为握手只发生一次，之后非生产者方向的阻塞 `Send`（生产者方向的阻塞 `Recv`）走到的是硬错误分支（`LOG_ERROR` + `false`），communicator 不会触发它。跑在 RC 连接上的是 channel 握手（`ConnHandshake`）；bootstrap 自身的 `NodeInfo` 交换走 TCP（`Utils::SendAll`/`RecvAll`），与 RDMA 无关。
- **数据面是 2 MiB 预注册环形缓冲，`64 KiB × 32` 槽位**：producer 把调用方数据 `memcpy` 进当前槽（staging）后再 post write，consumer 从槽里读。**这解释了 `TrySend` 的语义**：`*progress` 表示已被读入 transport 自有槽并成功提交的字节；`*done` 置位后调用方缓冲区即可立即复用（transport 不再读用户缓冲）。
- **槽位复用只由 credit 一个门控**：`TrySend` 的可发窗口是 `credits_received + kRdmaSlotCount`，即「已发出的 write 数」最多比「consumer 已归还的槽数」多一整圈（`send_seq` 与槽号是 `send_seq % kRdmaSlotCount` 的取模关系）。**隐含保证：credit 蕴含本地读已完成**——credit 是 consumer 收到 payload 之后才回的，而对端拿到 payload 之前本地 HCA 必然已读完该槽，所以不需要另设本地 CQE（`local_completed`）门控，`IBV_WC_RDMA_WRITE` 完成事件因此被直接忽略。再加一道本地门控只会收紧窗口，不会更安全。
- **credit 反向归还**：consumer 在把整个槽交给调用方后，用一次 `WRITE_WITH_IMM` 写对端控制区（`PostCredit` 目标是 `peer.base_addr + kRdmaCreditOffset`，payload 只有 1 B，不受方向限制，所以不用 SEND）；producer 用这一路回收远端槽。
- **immediate 的位布局是两种用途共用的隐含契约**：bit 31 为 credit 标志（`kCreditImmediate`），bit 16–30 是槽号，bit 0–15 是 `length - 1`。数据写的槽号 ≤ 31（`slot << 16` 最高只到 `0x1F0000`），永远碰不到 bit 31，所以 credit 判定不会误伤数据写。长度必须减 1 是因为 `length ∈ [1, 64 KiB]` 共 65536 个取值，只有 `length - 1` 才能双射进 16 bit；接收端除了 imm 没有别的长度来源，这个字段丢了就没有分帧信息。
- **环容量是在途窗口，不是每条消息的配额**：槽号是 `send_seq % kRdmaSlotCount` 的单调循环，在途 write 数被 credit 卡在 32 以内；一条消息占几个槽只看 `size / 64 KiB`（向上取整），用完即随 credit 归还。所以小消息只会拉低字节利用率（窗口只装得下 32 条消息的字节数），**不会「缓冲区不够用」；远大于 2 MiB 的消息只是分成多轮推完**，`TrySend` 返回 `*done = false` 交由 executor 等 `GetFd()` 就绪后重试。credit 侧 `kCreditBatch = 8` 批量归还，但 `arrival_head == arrival_tail`（队列排空）时立刻归还余数，所以小消息不会因归还滞后而停摆。固定 `64 KiB × 32` 的取舍是「一次 write 一槽、长度随 imm 到达」，省掉了字节粒度环必需的回绕拆分与额外长度元数据。
- **`WRITE_WITH_IMM` 会消耗接收方 RQ 里的 WQE，payload 却落在 WR 指定的 `remote_addr`**（不占接收 buffer）：因此接收队列是纯 credit 池（`kRdmaRecvPool` 个空 WQE，绑定 scratch 区），每收到一个 write-imm 就立刻补投一个。没投就发会得到 RNR，环形缓冲无法启动。
- **就绪与 EPOLLET**：`GetFd()` 连接后返回 completion channel fd，监听态返回 CM channel fd；`GetPollEvents()` 恒为 `EPOLLIN`。就绪 fd 只表达**数据到来**，不表达断连：对端消失时是常驻的接收 WQE 被 flush、产生 CQE 让 completion channel 变可读，再经 `HandleCompletion` 的 `IBV_WC_SUCCESS` 判定变成 `Try*` 的 `false`（已无 `peer_closed` 这种独立断连标志）；CM 事件不在就绪路径上（`cm_channel` 只在建链阶段被 `AwaitEvent` 消费）。`TrySend`/`TryRecv` 非阻塞地 drain 完成后，**必须** `ibv_get_cq_event` → `ibv_ack_cq_events` → `ibv_req_notify_cq` 重新 arm 再 poll CQ，只 poll CQ 而不排空 completion channel 会漏掉下一次边沿。
- **设备探测**：`TransportRDMA::Probe(addr)` 遍历 `ibv_get_device_list()` 的每个设备与每个端口，要求 `ibv_query_port` 的 `state == IBV_PORT_ACTIVE`，再扫该端口的 GID 表找 **IPv4-mapped GID**（前十个字节为 0 且 `raw[10] == raw[11] == 0xFF`，见 `IsIpv4MappedGid`），取其末 4 字节 `inet_ntop` 成地址。**不是**遍历系统 IPv4 地址（也不在此处 `rdma_bind_addr`，绑定发生在随后的 `Listen`）：本机 `GetLocalIPAddress()` 返回的 `enp0s3` 不是 RDMA 网卡，所以**不能把通用 IP 探测结果当 RDMA 地址用**；找不到这样的地址就表示本机不做 RDMA。
- **限制**：Linux + `libibverbs`/`librdmacm` 是硬依赖（CMake 缺库直接失败）。首版接受一次用户缓冲 ↔ 注册缓冲的拷贝，不做端到端零拷贝、不做 RDMA Read、不做多 rail 或动态缓冲扩缩。
- **测试用 `mpirun` 而不是 fork**：verbs/CM 初始化后若只 `fork` 不 `exec`，子进程的 `ibv_post_send` 会以 `EPERM` 失败（本机 rxe 实测），所以 `tests/test_transport_rdma.cpp` 用两个独立进程。

## 不变式与设计区间

### 不变式

- 数据面传输只走 `TrySend`/`TryRecv`（由 topology 状态机驱动）；executor 不直接调 `Send`/`Recv`。
- 两类控制流量走两条路，不要混：bootstrap 的 `NodeInfo` 交换走**裸 TCP socket**（`Utils::CreateListenSocket`/`CreateConnectSocket` + `Utils::SendAll`/`RecvAll`，带长度前缀），完全不碰 Transport；communicator 的 **channel 握手（`ConnHandshake`）恰恰走 Transport 的阻塞 `Send`/`Recv`**——这正是阻塞接口存在的唯一理由（共享内存走 control socket、RDMA 走一次 RC `IBV_WR_SEND`，三种实现语义一致，所以调用点不区分传输类型）。数据面仍只走 `Try*`。
- 非阻塞语义是硬约束：`TrySend`/`TryRecv` 绝不阻塞。
- 方向只是元数据，不是操作许可：TCP 在任何方向下都能收发。方向只决定 `GetPollEvents()`（`Send`→`EPOLLOUT`，`Receive`→`EPOLLIN`，`Bidirectional`→两者）；只有按方向拒绝反向操作的实现（如共享内存端点）才把方向当约束。**TCP 不能像共享内存那样拒绝反向，这是代码的隐含保证**：direction 由「这条边我是生产者还是消费者」决定，而 channel 握手恒由**主动连接方**先 `Send`、被动方先 `Recv`（见 [communicator.md](communicator.md) 的 `ConnHandshake`），所以一条被标成 `Receive` 的连接的主动端仍要在它上面 `Send`——拒绝反向会立刻打断握手。因此「方向是硬约束」只发生在数据面端点，不要据此给 TCP 加反向拒绝的防御。
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
| `include/transport/transport_rdma.h` / `src/transport/transport_rdma.cpp` | RDMA 实现：CM 建链、RC QP、`WRITE_WITH_IMM` 环形缓冲、credit 回收、completion channel 就绪 fd、设备探测 |
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
- 勿回退：RDMA 的槽位复用只保留 credit 一道门控。credit 已蕴含「本地已读完该槽」，再补一道本地 send CQE（`local_completed`）只会收紧窗口、不会更安全；`IBV_WC_RDMA_WRITE` 完成事件保持忽略。
- 勿回退：RDMA 的 `private_data` 保持裸 `Wire{base_addr, rkey}`。历史上曾有 `magic` 校验与 `ReadWire`/`AwaitEstablished` 包装，现均删除：连接合法性由 CM 本身保证，没有 magic 就不需要那层校验分支。
- 勿回退：`Try*` 必须同时排空 completion channel（ack + re-arm）并 poll CQ。直接 poll CQ 不重新 arm 会在 EPOLLET 下漏掉边沿。
- 勿回退：RDMA 地址必须由 `Probe` 从设备的 **IPv4-mapped GID**（`IBV_PORT_ACTIVE`）得出，不能复用 `Utils::GetLocalIPAddress()` 的结果（那可能不是 RDMA 网卡）。
- 环容量固定 2 MiB，不做可配置/动态扩容；**隐含保证是单环单 producer + 单 consumer**，据此不做容量协商、多生产者或双向的防御分支。
- 改动本层后跑 `scripts/run_tests.sh`：tier 0 只测传输本身（fork 端点对，无需 mpirun），tier 11 测 RDMA 传输本身（mpirun 端点对，无设备时 SKIP）；2/4 rank 的集合通信档位在默认选择与 `OCCL_DISABLE_SHM=1`/`OCCL_DISABLE_RDMA=1` 覆盖下各跑一遍（1 rank 无数据面，只跑一次）。
