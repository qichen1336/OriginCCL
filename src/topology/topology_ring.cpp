#include <algorithm>
#include <cstring>
#include <vector>
#include "topology/topology_ring.h"
#include "transport/transport.h"
#include "utils.h"
#include "logger.h"

namespace {
constexpr int kPhaseUnstarted = 0;
constexpr int kPhaseReduceScatter = 1;
constexpr int kPhaseDone = 3;

int RingPartition(int partition, int world_size) {
    return (partition + world_size) % world_size;
}

size_t AllreducePartitionStart(const PlanTask& task, int partition) {
    const size_t world_size = static_cast<size_t>(task.world_size);
    const size_t base = task.elem_count / world_size;
    const size_t remainder = task.elem_count % world_size;
    const size_t index = static_cast<size_t>(partition);
    return index * base + std::min(index, remainder);
}

size_t AllreducePartitionElems(const PlanTask& task, int partition) {
    const size_t world_size = static_cast<size_t>(task.world_size);
    const size_t base = task.elem_count / world_size;
    const size_t remainder = task.elem_count % world_size;
    return base + (static_cast<size_t>(partition) < remainder ? 1 : 0);
}

size_t AllreducePartitionChunks(const PlanTask& task, int partition) {
    const size_t elems = AllreducePartitionElems(task, partition);
    return elems == 0 ? 0 : (elems + task.chunk_size - 1) / task.chunk_size;
}

size_t AllreduceChunkElems(const PlanTask& task, int partition, size_t chunk) {
    const size_t start = chunk * task.chunk_size;
    const size_t elems = AllreducePartitionElems(task, partition);
    return start >= elems ? 0 : std::min(task.chunk_size, elems - start);
}

size_t AllreduceChunkBytes(const PlanTask& task, int partition, size_t chunk) {
    return AllreduceChunkElems(task, partition, chunk) * Utils::GetDataTypeSize(task.dtype);
}

size_t AllreduceChunkOffset(const PlanTask& task, int partition, size_t chunk) {
    return (AllreducePartitionStart(task, partition) + chunk * task.chunk_size) * Utils::GetDataTypeSize(task.dtype);
}

size_t AllreducePhaseSteps(const PlanTask& task) {
    return static_cast<size_t>(task.world_size - 1);
}

size_t AllreduceTotalSteps(const PlanTask& task) {
    return 2 * AllreducePhaseSteps(task);
}

bool AllreduceSendReducing(const PlanTask& task) {
    return task.state.algo.ring.send_step < AllreducePhaseSteps(task);
}

bool AllreduceRecvReducing(const PlanTask& task) {
    return task.state.algo.ring.recv_step < AllreducePhaseSteps(task);
}

int AllreduceSendPartition(const PlanTask& task) {
    const CollOpState& s = task.state;
    const size_t phase_steps = AllreducePhaseSteps(task);
    const int offset = AllreduceSendReducing(task)
                           ? task.rank - static_cast<int>(s.algo.ring.send_step)
                           : task.rank + 1 - static_cast<int>(s.algo.ring.send_step - phase_steps);
    return RingPartition(offset, task.world_size);
}

int AllreduceRecvPartition(const PlanTask& task) {
    const CollOpState& s = task.state;
    const size_t phase_steps = AllreducePhaseSteps(task);
    const int offset = AllreduceRecvReducing(task) ? task.rank - static_cast<int>(s.algo.ring.recv_step) - 1
                                                   : task.rank - static_cast<int>(s.algo.ring.recv_step - phase_steps);
    return RingPartition(offset, task.world_size);
}

const char* AllreduceSendPtr(const PlanTask& task) {
    const CollOpState& s = task.state;
    return static_cast<const char*>(task.recv_buf) +
           AllreduceChunkOffset(task, AllreduceSendPartition(task), s.algo.ring.send_chunk);
}

char* AllreduceRecvPtr(const PlanTask& task) {
    const CollOpState& s = task.state;
    if (AllreduceRecvReducing(task)) {
        return const_cast<char*>(s.temp_buffer.data());
    }
    return static_cast<char*>(task.recv_buf) +
           AllreduceChunkOffset(task, AllreduceRecvPartition(task), s.algo.ring.recv_chunk);
}

constexpr int kPhaseSend = 4;
constexpr int kPhaseRecv = 5;
constexpr int kPhaseExchange = 6;

size_t BlockBytes(const PlanTask& task) {
    return task.elem_count * Utils::GetDataTypeSize(task.dtype);
}

char* OutputBlock(const PlanTask& task, int block) {
    return static_cast<char*>(task.recv_buf) +
           static_cast<size_t>(block) * task.rank_stride * Utils::GetDataTypeSize(task.dtype);
}

const char* InputBlock(const PlanTask& task, int block) {
    return static_cast<const char*>(task.send_buf) +
           static_cast<size_t>(block) * task.rank_stride * Utils::GetDataTypeSize(task.dtype);
}

int RootPosition(const PlanTask& task) {
    return (task.rank - task.root + task.world_size) % task.world_size;
}

void ReduceBlock(const PlanTask& task, const void* input, void* output) {
    Utils::PerformReduce(input, output, task.elem_count, task.dtype,
                         task.reduce_op == ReduceOp::AVG ? ReduceOp::SUM : task.reduce_op);
}

int ReduceScatterInputChunk(const PlanTask& task) {
    return (task.rank - task.state.algo.ring.step - 2 + 2 * task.world_size) % task.world_size;
}

bool SideDone(const std::vector<char>& done) {
    return std::all_of(done.begin(), done.end(), [](char d) { return d != 0; });
}

void MarkAllDone(PlanTask& task) {
    task.state.send_done.assign(task.send_transports.size(), 1);
    task.state.recv_done.assign(task.recv_transports.size(), 1);
}

bool PushBuffer(PlanTask& task, CollEvent event, const char* send_data, char* recv_data, size_t send_bytes,
                size_t recv_bytes) {
    CollOpState& s = task.state;
    if (event == CollEvent::Writable) {
        for (size_t i = 0; i < task.send_transports.size(); ++i) {
            if (s.send_done[i]) {
                continue;
            }
            bool done = false;
            if (!task.send_transports[i]->TrySend(send_data, send_bytes, &s.send_progress[i], &done)) {
                LOG_ERROR("Ring {} send failed on rank {} (phase {}, step {})", Utils::GetCollFuncName(task.func),
                          task.rank, s.phase, s.algo.ring.step);
                return false;
            }
            s.send_done[i] = done ? 1 : 0;
        }
        return true;
    }
    for (size_t i = 0; i < task.recv_transports.size(); ++i) {
        if (s.recv_done[i]) {
            continue;
        }
        bool done = false;
        if (!task.recv_transports[i]->TryRecv(recv_data, recv_bytes, &s.recv_progress[i], &done)) {
            LOG_ERROR("Ring {} recv failed on rank {} (phase {}, step {})", Utils::GetCollFuncName(task.func),
                      task.rank, s.phase, s.algo.ring.step);
            return false;
        }
        s.recv_done[i] = done ? 1 : 0;
    }
    return true;
}

bool BeginPhase(PlanTask& task, int phase, const char* send_data, char* recv_data, size_t send_bytes,
                size_t recv_bytes) {
    CollOpState& s = task.state;
    s.phase = phase;
    bool send_skip = (phase == kPhaseRecv || phase == kPhaseDone) || send_bytes == 0;
    bool recv_skip = (phase == kPhaseSend || phase == kPhaseDone) || recv_bytes == 0;
    s.send_progress.assign(task.send_transports.size(), 0);
    s.recv_progress.assign(task.recv_transports.size(), 0);
    s.send_done.assign(task.send_transports.size(), send_skip ? 1 : 0);
    s.recv_done.assign(task.recv_transports.size(), recv_skip ? 1 : 0);
    return PushBuffer(task, CollEvent::Writable, send_data, recv_data, send_bytes, recv_bytes) &&
           PushBuffer(task, CollEvent::Readable, send_data, recv_data, send_bytes, recv_bytes);
}

const char* AllGatherSendPtr(const PlanTask& task) {
    return OutputBlock(task, (task.rank - task.state.algo.ring.step + task.world_size) % task.world_size);
}

char* AllGatherRecvPtr(const PlanTask& task) {
    return OutputBlock(task, (task.rank - task.state.algo.ring.step - 1 + task.world_size) % task.world_size);
}

char* ReduceScatterSendPtr(const PlanTask& task) {
    return task.state.algo.ring.step % 2 == 0 ? static_cast<char*>(task.recv_buf)
                                              : const_cast<char*>(task.state.temp_buffer.data());
}

char* ReduceScatterRecvPtr(const PlanTask& task) {
    return task.state.algo.ring.step % 2 == 0 ? const_cast<char*>(task.state.temp_buffer.data())
                                              : static_cast<char*>(task.recv_buf);
}

bool CompleteBroadcast(PlanTask& task) {
    CollOpState& s = task.state;
    while (SideDone(s.send_done) && SideDone(s.recv_done) && s.phase != kPhaseDone) {
        int next_phase = kPhaseDone;
        if (s.phase == kPhaseRecv && (task.rank + 1) % task.world_size != task.root) {
            next_phase = kPhaseSend;
        }
        if (!BeginPhase(task, next_phase, static_cast<const char*>(task.recv_buf), static_cast<char*>(task.recv_buf),
                        BlockBytes(task), BlockBytes(task))) {
            return false;
        }
    }
    return true;
}

bool CompleteAllGather(PlanTask& task) {
    CollOpState& s = task.state;
    while (SideDone(s.send_done) && SideDone(s.recv_done) && s.phase != kPhaseDone) {
        int next_phase = kPhaseDone;
        if (++s.algo.ring.step < task.world_size - 1) {
            next_phase = kPhaseExchange;
        }
        if (!BeginPhase(task, next_phase, AllGatherSendPtr(task), AllGatherRecvPtr(task), BlockBytes(task),
                        BlockBytes(task))) {
            return false;
        }
    }
    return true;
}

bool CompleteReduce(PlanTask& task) {
    CollOpState& s = task.state;
    while (SideDone(s.send_done) && SideDone(s.recv_done) && s.phase != kPhaseDone) {
        int next_phase = kPhaseDone;
        if (s.phase == kPhaseRecv) {
            ReduceBlock(task, s.temp_buffer.data() + BlockBytes(task), s.temp_buffer.data());
            if (task.rank == task.root) {
                std::memcpy(task.recv_buf, s.temp_buffer.data(), BlockBytes(task));
                if (task.reduce_op == ReduceOp::AVG) {
                    Utils::ApplyAverage(task.recv_buf, task.elem_count, task.dtype, task.world_size);
                }
            } else {
                next_phase = kPhaseSend;
            }
        }
        if (!BeginPhase(task, next_phase, s.temp_buffer.data(), s.temp_buffer.data() + BlockBytes(task),
                        BlockBytes(task), BlockBytes(task))) {
            return false;
        }
    }
    return true;
}

bool CompleteReduceScatter(PlanTask& task) {
    CollOpState& s = task.state;
    while (SideDone(s.send_done) && SideDone(s.recv_done) && s.phase != kPhaseDone) {
        char* incoming = ReduceScatterRecvPtr(task);
        ReduceBlock(task, InputBlock(task, ReduceScatterInputChunk(task)), incoming);
        int next_phase = kPhaseDone;
        if (++s.algo.ring.step < task.world_size - 1) {
            next_phase = kPhaseExchange;
        } else {
            if (incoming != task.recv_buf) {
                std::memcpy(task.recv_buf, incoming, BlockBytes(task));
            }
            if (task.reduce_op == ReduceOp::AVG) {
                Utils::ApplyAverage(task.recv_buf, task.elem_count, task.dtype, task.world_size);
            }
        }
        if (!BeginPhase(task, next_phase, ReduceScatterSendPtr(task), ReduceScatterRecvPtr(task), BlockBytes(task),
                        BlockBytes(task))) {
            return false;
        }
    }
    return true;
}

bool AllreduceSendComplete(const PlanTask& task) {
    return task.state.algo.ring.send_step >= AllreduceTotalSteps(task);
}

bool AllreduceRecvComplete(const PlanTask& task) {
    return task.state.algo.ring.recv_step >= AllreduceTotalSteps(task);
}

void AdvanceAllreduceSend(PlanTask& task) {
    CollOpState& s = task.state;
    const int partition = AllreduceSendPartition(task);
    if (++s.algo.ring.send_chunk == AllreducePartitionChunks(task, partition)) {
        s.algo.ring.send_chunk = 0;
        ++s.algo.ring.send_step;
    }
}

void AdvanceAllreduceRecv(PlanTask& task) {
    CollOpState& s = task.state;
    const int partition = AllreduceRecvPartition(task);
    if (++s.algo.ring.recv_chunk == AllreducePartitionChunks(task, partition)) {
        s.algo.ring.recv_chunk = 0;
        ++s.algo.ring.recv_step;
    }
}

bool PushAllreduceSend(PlanTask& task) {
    CollOpState& s = task.state;
    for (size_t i = 0; i < task.send_transports.size(); ++i) {
        while (!AllreduceSendComplete(task)) {
            const int partition = AllreduceSendPartition(task);
            const std::vector<size_t>& ready =
                AllreduceSendReducing(task) ? s.algo.ring.ready_chunks : s.algo.ring.allgather_ready_chunks;
            if (s.algo.ring.send_chunk >= ready[static_cast<size_t>(partition)]) {
                s.send_done[i] = 0;
                break;
            }
            bool done = false;
            if (!task.send_transports[i]->TrySend(AllreduceSendPtr(task),
                                                  AllreduceChunkBytes(task, partition, s.algo.ring.send_chunk),
                                                  &s.send_progress[i], &done)) {
                LOG_ERROR("Ring AllReduce send failed on rank {} (phase {}, step {})", task.rank, s.phase,
                          s.algo.ring.send_step);
                return false;
            }
            s.send_done[i] = done ? 1 : 0;
            if (!done) {
                break;
            }
            s.send_progress[i] = 0;
            AdvanceAllreduceSend(task);
        }
        if (AllreduceSendComplete(task)) {
            s.send_done[i] = 1;
        }
    }
    return true;
}

bool PushAllreduceRecv(PlanTask& task) {
    CollOpState& s = task.state;
    for (size_t i = 0; i < task.recv_transports.size(); ++i) {
        while (!AllreduceRecvComplete(task)) {
            const int partition = AllreduceRecvPartition(task);
            const size_t chunk = s.algo.ring.recv_chunk;
            bool done = false;
            if (!task.recv_transports[i]->TryRecv(AllreduceRecvPtr(task), AllreduceChunkBytes(task, partition, chunk),
                                                  &s.recv_progress[i], &done)) {
                LOG_ERROR("Ring AllReduce recv failed on rank {} (phase {}, step {})", task.rank, s.phase,
                          s.algo.ring.recv_step);
                return false;
            }
            s.recv_done[i] = done ? 1 : 0;
            if (!done) {
                break;
            }
            if (AllreduceRecvReducing(task)) {
                char* output = static_cast<char*>(task.recv_buf) + AllreduceChunkOffset(task, partition, chunk);
                Utils::PerformReduce(s.temp_buffer.data(), output, AllreduceChunkElems(task, partition, chunk),
                                     task.dtype, task.reduce_op == ReduceOp::AVG ? ReduceOp::SUM : task.reduce_op);
                if (task.reduce_op == ReduceOp::AVG && s.algo.ring.recv_step + 1 == AllreducePhaseSteps(task)) {
                    Utils::ApplyAverage(output, AllreduceChunkElems(task, partition, chunk), task.dtype,
                                        task.world_size);
                }
                ++s.algo.ring.ready_chunks[static_cast<size_t>(partition)];
                if (s.algo.ring.recv_step + 1 == AllreducePhaseSteps(task)) {
                    ++s.algo.ring.allgather_ready_chunks[static_cast<size_t>(partition)];
                }
            } else {
                ++s.algo.ring.allgather_ready_chunks[static_cast<size_t>(partition)];
            }
            s.recv_progress[i] = 0;
            AdvanceAllreduceRecv(task);
        }
        if (AllreduceRecvComplete(task)) {
            s.recv_done[i] = 1;
        }
    }
    return true;
}

bool PushAllreduce(PlanTask& task, CollEvent event) {
    if (event == CollEvent::Writable) {
        return PushAllreduceSend(task);
    }
    if (!PushAllreduceRecv(task)) {
        return false;
    }
    return PushAllreduceSend(task);
}

bool BeginAllreduce(PlanTask& task) {
    CollOpState& s = task.state;
    s.phase = kPhaseReduceScatter;
    s.algo.ring.send_step = 0;
    s.algo.ring.recv_step = 0;
    s.algo.ring.send_chunk = 0;
    s.algo.ring.recv_chunk = 0;
    s.algo.ring.ready_chunks.assign(static_cast<size_t>(task.world_size), 0);
    s.algo.ring.allgather_ready_chunks.assign(static_cast<size_t>(task.world_size), 0);
    s.algo.ring.ready_chunks[static_cast<size_t>(task.rank)] = AllreducePartitionChunks(task, task.rank);
    s.send_progress.assign(task.send_transports.size(), 0);
    s.recv_progress.assign(task.recv_transports.size(), 0);
    s.send_done.assign(task.send_transports.size(), 0);
    s.recv_done.assign(task.recv_transports.size(), 0);
    return PushAllreduce(task, CollEvent::Writable) && PushAllreduce(task, CollEvent::Readable);
}

bool CompleteAllreduce(PlanTask& task) {
    CollOpState& s = task.state;
    if (AllreduceSendComplete(task) && AllreduceRecvComplete(task)) {
        s.phase = kPhaseDone;
    }
    return true;
}

} // namespace

void TopologyRing::FillChannels(const Communicator&, std::vector<Channel>& channels) {
    for (Channel& channel : channels) {
        channel.ring.prev = GetPrevRank(rank);
        channel.ring.next = GetNextRank(rank);
    }
}

void TopologyRing::FillPeers(const Channel& channel, std::vector<TopoEdge>& edges) const {
    edges.clear();
    edges.push_back(TopoEdge{channel.ring.next, true});
    edges.push_back(TopoEdge{channel.ring.prev, false});
}

int TopologyRing::GetPrevRank(int r) const {
    return (r - 1 + world_size) % world_size;
}

void TopologyRing::FillTransports(Channel& channel, PlanTask& task) const {
    auto& send_out = task.send_transports;
    auto& recv_out = task.recv_transports;
    send_out.clear();
    recv_out.clear();
    Connector* send_conn = channel.SendConnector(channel.ring.next);
    if (send_conn && send_conn->transport) {
        send_out.push_back(send_conn->transport);
    }
    Connector* recv_conn = channel.RecvConnector(channel.ring.prev);
    if (recv_conn && recv_conn->transport) {
        recv_out.push_back(recv_conn->transport);
    }
}

int TopologyRing::GetNextRank(int r) const {
    return (r + 1) % world_size;
}

bool TopologyRing::CollectiveInit(PlanTask& task) const noexcept {
    switch (task.func) {
    case CollFunc::AllReduce:
        return AllreduceInit(task);
    case CollFunc::Broadcast:
        return BroadcastInit(task);
    case CollFunc::AllGather:
        return AllGatherInit(task);
    case CollFunc::Reduce:
        return ReduceInit(task);
    case CollFunc::ReduceScatter:
        return ReduceScatterInit(task);
    default:
        LOG_ERROR("Ring received unsupported collective");
        return false;
    }
}

bool TopologyRing::CollectiveStep(PlanTask& task, CollEvent event) const noexcept {
    switch (task.func) {
    case CollFunc::AllReduce:
        return AllreduceStep(task, event);
    case CollFunc::Broadcast:
        return BroadcastStep(task, event);
    case CollFunc::AllGather:
        return AllGatherStep(task, event);
    case CollFunc::Reduce:
        return ReduceStep(task, event);
    case CollFunc::ReduceScatter:
        return ReduceScatterStep(task, event);
    default:
        LOG_ERROR("Ring received unsupported collective");
        return false;
    }
}

bool TopologyRing::CollectiveDone(const PlanTask& task) const {
    return task.state.phase == kPhaseDone;
}

bool TopologyRing::AllreduceInit(PlanTask& task) const noexcept {
    CollOpState& s = task.state;
    s = CollOpState{};

    if (task.world_size <= 0) {
        LOG_ERROR("Ring AllReduce received invalid task");
        return false;
    }

    if (task.elem_count == 0) {
        s.phase = kPhaseDone;
        MarkAllDone(task);
        return true;
    }
    if (!task.send_buf || !task.recv_buf) {
        LOG_ERROR("Ring AllReduce received missing buffer");
        return false;
    }

    size_t type_size = Utils::GetDataTypeSize(task.dtype);
    if (task.send_buf != task.recv_buf) {
        std::memcpy(task.recv_buf, task.send_buf, task.elem_count * type_size);
    }

    if (task.world_size == 1) {
        if (task.reduce_op == ReduceOp::AVG) {
            Utils::ApplyAverage(task.recv_buf, task.elem_count, task.dtype, task.world_size);
        }
        s.phase = kPhaseDone;
        return true;
    }

    if (task.send_transports.empty() || task.recv_transports.empty()) {
        LOG_ERROR("Ring AllReduce received task without transports");
        return false;
    }

    s.temp_buffer.resize(task.chunk_size * type_size);
    return BeginAllreduce(task) && CompleteAllreduce(task);
}

bool TopologyRing::AllreduceStep(PlanTask& task, CollEvent event) const noexcept {
    CollOpState& s = task.state;
    if (s.phase == kPhaseUnstarted) {
        if (!AllreduceInit(task)) {
            return false;
        }
    }
    if (s.phase == kPhaseDone) {
        return true;
    }

    if (!PushAllreduce(task, event)) {
        return false;
    }
    return CompleteAllreduce(task);
}

bool TopologyRing::BroadcastInit(PlanTask& task) const noexcept {
    CollOpState& s = task.state;
    s = CollOpState{};

    if (task.world_size <= 0 || task.root < 0 || task.root >= task.world_size) {
        LOG_ERROR("Ring Broadcast received invalid task");
        return false;
    }
    if (task.elem_count == 0) {
        s.phase = kPhaseDone;
        MarkAllDone(task);
        return true;
    }
    if (!task.recv_buf) {
        LOG_ERROR("Ring Broadcast received missing buffer");
        return false;
    }
    if (task.world_size == 1) {
        s.phase = kPhaseDone;
        MarkAllDone(task);
        return true;
    }
    if (task.send_transports.empty() || task.recv_transports.empty()) {
        LOG_ERROR("Ring Broadcast received task without transports");
        return false;
    }
    int phase = task.rank == task.root ? kPhaseSend : kPhaseRecv;
    return BeginPhase(task, phase, static_cast<const char*>(task.recv_buf), static_cast<char*>(task.recv_buf),
                      BlockBytes(task), BlockBytes(task)) &&
           CompleteBroadcast(task);
}

bool TopologyRing::BroadcastStep(PlanTask& task, CollEvent event) const noexcept {
    CollOpState& s = task.state;
    if (s.phase == kPhaseUnstarted && !BroadcastInit(task)) {
        return false;
    }
    if (s.phase == kPhaseDone) {
        return true;
    }
    if (!PushBuffer(task, event, static_cast<const char*>(task.recv_buf), static_cast<char*>(task.recv_buf),
                    BlockBytes(task), BlockBytes(task))) {
        return false;
    }
    return CompleteBroadcast(task);
}

bool TopologyRing::AllGatherInit(PlanTask& task) const noexcept {
    CollOpState& s = task.state;
    s = CollOpState{};

    if (task.world_size <= 0) {
        LOG_ERROR("Ring AllGather received invalid task");
        return false;
    }
    if (task.elem_count == 0) {
        s.phase = kPhaseDone;
        MarkAllDone(task);
        return true;
    }
    if (!task.send_buf || !task.recv_buf) {
        LOG_ERROR("Ring AllGather received missing buffer");
        return false;
    }
    std::memcpy(OutputBlock(task, task.rank), task.send_buf, BlockBytes(task));
    if (task.world_size == 1) {
        s.phase = kPhaseDone;
        MarkAllDone(task);
        return true;
    }
    if (task.send_transports.empty() || task.recv_transports.empty()) {
        LOG_ERROR("Ring AllGather received task without transports");
        return false;
    }
    return BeginPhase(task, kPhaseExchange, AllGatherSendPtr(task), AllGatherRecvPtr(task), BlockBytes(task),
                      BlockBytes(task)) &&
           CompleteAllGather(task);
}

bool TopologyRing::AllGatherStep(PlanTask& task, CollEvent event) const noexcept {
    CollOpState& s = task.state;
    if (s.phase == kPhaseUnstarted && !AllGatherInit(task)) {
        return false;
    }
    if (s.phase == kPhaseDone) {
        return true;
    }
    if (!PushBuffer(task, event, AllGatherSendPtr(task), AllGatherRecvPtr(task), BlockBytes(task), BlockBytes(task))) {
        return false;
    }
    return CompleteAllGather(task);
}

bool TopologyRing::ReduceInit(PlanTask& task) const noexcept {
    CollOpState& s = task.state;
    s = CollOpState{};

    if (task.world_size <= 0 || task.root < 0 || task.root >= task.world_size) {
        LOG_ERROR("Ring Reduce received invalid task");
        return false;
    }
    if (task.elem_count == 0) {
        s.phase = kPhaseDone;
        MarkAllDone(task);
        return true;
    }
    if (!task.send_buf || (task.rank == task.root && !task.recv_buf)) {
        LOG_ERROR("Ring Reduce received missing buffer");
        return false;
    }
    if (task.world_size == 1) {
        std::memcpy(task.recv_buf, task.send_buf, BlockBytes(task));
        s.phase = kPhaseDone;
        MarkAllDone(task);
        return true;
    }
    if (task.send_transports.empty() || task.recv_transports.empty()) {
        LOG_ERROR("Ring Reduce received task without transports");
        return false;
    }
    s.temp_buffer.resize(2 * BlockBytes(task));
    std::memcpy(s.temp_buffer.data(), task.send_buf, BlockBytes(task));
    int phase = RootPosition(task) == 1 ? kPhaseSend : kPhaseRecv;
    return BeginPhase(task, phase, s.temp_buffer.data(), s.temp_buffer.data() + BlockBytes(task), BlockBytes(task),
                      BlockBytes(task)) &&
           CompleteReduce(task);
}

bool TopologyRing::ReduceStep(PlanTask& task, CollEvent event) const noexcept {
    CollOpState& s = task.state;
    if (s.phase == kPhaseUnstarted && !ReduceInit(task)) {
        return false;
    }
    if (s.phase == kPhaseDone) {
        return true;
    }
    if (!PushBuffer(task, event, s.temp_buffer.data(), s.temp_buffer.data() + BlockBytes(task), BlockBytes(task),
                    BlockBytes(task))) {
        return false;
    }
    return CompleteReduce(task);
}

bool TopologyRing::ReduceScatterInit(PlanTask& task) const noexcept {
    CollOpState& s = task.state;
    s = CollOpState{};

    if (task.world_size <= 0) {
        LOG_ERROR("Ring ReduceScatter received invalid task");
        return false;
    }
    if (task.elem_count == 0) {
        s.phase = kPhaseDone;
        MarkAllDone(task);
        return true;
    }
    if (!task.send_buf || !task.recv_buf) {
        LOG_ERROR("Ring ReduceScatter received missing buffer");
        return false;
    }
    if (task.world_size == 1) {
        std::memcpy(task.recv_buf, task.send_buf, BlockBytes(task));
        s.phase = kPhaseDone;
        MarkAllDone(task);
        return true;
    }
    if (task.send_transports.empty() || task.recv_transports.empty()) {
        LOG_ERROR("Ring ReduceScatter received task without transports");
        return false;
    }
    s.temp_buffer.resize(BlockBytes(task));
    std::memcpy(task.recv_buf, InputBlock(task, (task.rank - 1 + task.world_size) % task.world_size), BlockBytes(task));
    return BeginPhase(task, kPhaseExchange, ReduceScatterSendPtr(task), ReduceScatterRecvPtr(task), BlockBytes(task),
                      BlockBytes(task)) &&
           CompleteReduceScatter(task);
}

bool TopologyRing::ReduceScatterStep(PlanTask& task, CollEvent event) const noexcept {
    CollOpState& s = task.state;
    if (s.phase == kPhaseUnstarted && !ReduceScatterInit(task)) {
        return false;
    }
    if (s.phase == kPhaseDone) {
        return true;
    }
    if (!PushBuffer(task, event, ReduceScatterSendPtr(task), ReduceScatterRecvPtr(task), BlockBytes(task),
                    BlockBytes(task))) {
        return false;
    }
    return CompleteReduceScatter(task);
}
