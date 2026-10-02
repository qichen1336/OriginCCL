# OriginCCL

OriginCCL 的通信术语。接口契约见 [communicator 层文档](docs/layers/communicator.md)。

## Language

**集合操作**：同一 communicator 的全部 rank 按一致顺序参与的一次通信操作，包括 AllReduce、Broadcast、Reduce、AllGather 和 ReduceScatter。

**P2P 操作**：同一 communicator 内两个不同 rank 之间配对的 Send 与 Recv；其他 rank 无需参与。
_Avoid_: 单 rank 集合操作

**Peer**：一次 P2P 操作明确指定的对端 rank；发送端指定接收者，接收端指定发送者。

**配对消息**：同一有向 rank 对上按顺序对应、count 与 dtype 一致的一次非空 Send 和一次 Recv。

**通信组**：`GroupStart()` 到最外层 `GroupEnd()` 之间提交的一批操作。组内操作彼此独立，最外层 `GroupEnd()` 才执行。单独一次通信调用是只含一个操作的组。

**通信轮次**：P2P 按 `i = 1..world_size-1` 分轮，第 i 轮每个 rank 向 `(rank-i+world_size)%world_size` 发送、并从 `(rank+i)%world_size` 接收。轮次是 P2P 的排序依据，保证各 rank 步调一致、不产生等待环。