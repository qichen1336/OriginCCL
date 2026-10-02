# OriginCCL

OriginCCL 的通信术语。接口契约见 [communicator 层文档](docs/layers/communicator.md)。

## Language

**集合操作**：同一 communicator 的全部 rank 按一致顺序参与的一次通信操作，包括 AllReduce、Broadcast、Reduce、AllGather 和 ReduceScatter。

**P2P 操作**：同一 communicator 内两个不同 rank 之间配对的 Send 与 Recv；其他 rank 无需参与。
_Avoid_: 单 rank 集合操作

**Peer**：一次 P2P 操作明确指定的对端 rank；发送端指定接收者，接收端指定发送者。

**配对消息**：同一有向 rank 对上按顺序对应、count 与 dtype 一致的一次非空 Send 和一次 Recv。