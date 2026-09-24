#include <algorithm>
#include <cstring>
#include <vector>
#include "topology_ring.h"
#include "transport/transport.h"
#include "utils.h"
#include "logger.h"

namespace {
constexpr int kPhaseUnstarted = 0;
constexpr int kPhaseReduceScatter = 1;
constexpr int kPhaseAllGather = 2;
constexpr int kPhaseDone = 3;

size_t ChunkElemCount(size_t count, size_t chunk_size, int chunk_index) {
    size_t start = static_cast<size_t>(chunk_index) * chunk_size;
    return start >= count ? 0 : std::min(chunk_size, count - start);
}

int AllreduceSendChunk(const PlanTask& task) {
    const CollOpState& s = task.state;
    if (s.phase == kPhaseReduceScatter) {
        return (task.rank - s.step + task.world_size) % task.world_size;
    }
    return (task.rank - s.step + 1 + task.world_size) % task.world_size;
}

int AllreduceRecvChunk(const PlanTask& task) {
    const CollOpState& s = task.state;
    if (s.phase == kPhaseReduceScatter) {
        return (task.rank - s.step + task.world_size - 1) % task.world_size;
    }
    return (task.rank - s.step + task.world_size) % task.world_size;
}

size_t AllreduceSendBytes(const PlanTask& task) {
    return ChunkElemCount(task.elem_count, task.chunk_size, AllreduceSendChunk(task)) * Utils::GetDataTypeSize(task.dtype);
}

size_t AllreduceRecvBytes(const PlanTask& task) {
    return ChunkElemCount(task.elem_count, task.chunk_size, AllreduceRecvChunk(task)) * Utils::GetDataTypeSize(task.dtype);
}

char* AllreduceRecvPtr(const PlanTask& task) {
    const CollOpState& s = task.state;
    if (s.phase == kPhaseReduceScatter) {
        return const_cast<char*>(s.temp_buffer.data());
    }
    return static_cast<char*>(task.recv_buf) +
           static_cast<size_t>(AllreduceRecvChunk(task)) * task.chunk_size * Utils::GetDataTypeSize(task.dtype);
}

const char* AllreduceSendPtr(const PlanTask& task) {
    return static_cast<const char*>(task.recv_buf) +
           static_cast<size_t>(AllreduceSendChunk(task)) * task.chunk_size * Utils::GetDataTypeSize(task.dtype);
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

void PackInput(PlanTask& task) {
    for (int block = 0; block < task.world_size; ++block) {
        std::memcpy(task.state.temp_buffer.data() + static_cast<size_t>(block) * BlockBytes(task),
                    InputBlock(task, block), BlockBytes(task));
    }
}

int ReducedChunk(const PlanTask& task) {
    return (task.rank - task.state.step - 2 + 2 * task.world_size) % task.world_size;
}

bool PushBuffer(PlanTask& task, CollEvent event, const char* send_data, char* recv_data, size_t send_bytes,
                size_t recv_bytes, const char* op) {
    CollOpState& s = task.state;
    if (event == CollEvent::Writable) {
        if (s.send_done) {
            return true;
        }
        if (!task.send_transport->TrySend(send_data, send_bytes, &s.send_progress, &s.send_done)) {
            LOG_ERROR("Ring {} send failed on rank {} (phase {}, step {})", op, task.rank, s.phase, s.step);
            return false;
        }
        return true;
    }
    if (s.recv_done) {
        return true;
    }
    if (!task.recv_transport->TryRecv(recv_data, recv_bytes, &s.recv_progress, &s.recv_done)) {
        LOG_ERROR("Ring {} recv failed on rank {} (phase {}, step {})", op, task.rank, s.phase, s.step);
        return false;
    }
    return true;
}

bool BeginPhase(PlanTask& task, int phase, const char* send_data, char* recv_data, size_t send_bytes, size_t recv_bytes,
                const char* op) {
    CollOpState& s = task.state;
    s.phase = phase;
    s.send_progress = 0;
    s.recv_progress = 0;
    s.send_done = (phase == kPhaseRecv || phase == kPhaseDone) || send_bytes == 0;
    s.recv_done = (phase == kPhaseSend || phase == kPhaseDone) || recv_bytes == 0;
    return PushBuffer(task, CollEvent::Writable, send_data, recv_data, send_bytes, recv_bytes, op) &&
           PushBuffer(task, CollEvent::Readable, send_data, recv_data, send_bytes, recv_bytes, op);
}

const char* AllGatherSendPtr(const PlanTask& task) {
    return OutputBlock(task, (task.rank - task.state.step + task.world_size) % task.world_size);
}

char* AllGatherRecvPtr(const PlanTask& task) {
    return OutputBlock(task, (task.rank - task.state.step - 1 + task.world_size) % task.world_size);
}

const char* ReduceScatterSendPtr(const PlanTask& task) {
    return task.state.temp_buffer.data() +
           static_cast<size_t>((task.rank - task.state.step - 1 + task.world_size) % task.world_size) *
               BlockBytes(task);
}

char* ReduceScatterRecvPtr(const PlanTask& task) {
    return task.state.temp_buffer.data() + static_cast<size_t>(task.world_size) * BlockBytes(task);
}

bool CompleteBroadcast(PlanTask& task) {
    CollOpState& s = task.state;
    while (s.send_done && s.recv_done && s.phase != kPhaseDone) {
        int next_phase = kPhaseDone;
        if (s.phase == kPhaseRecv && (task.rank + 1) % task.world_size != task.root) {
            next_phase = kPhaseSend;
        }
        if (!BeginPhase(task, next_phase, static_cast<const char*>(task.recv_buf),
                        static_cast<char*>(task.recv_buf), BlockBytes(task), BlockBytes(task), "Broadcast")) {
            return false;
        }
    }
    return true;
}

bool CompleteAllGather(PlanTask& task) {
    CollOpState& s = task.state;
    while (s.send_done && s.recv_done && s.phase != kPhaseDone) {
        int next_phase = kPhaseDone;
        if (++s.step < task.world_size - 1) {
            next_phase = kPhaseExchange;
        }
        if (!BeginPhase(task, next_phase, AllGatherSendPtr(task), AllGatherRecvPtr(task), BlockBytes(task),
                        BlockBytes(task), "AllGather")) {
            return false;
        }
    }
    return true;
}

bool CompleteReduce(PlanTask& task) {
    CollOpState& s = task.state;
    while (s.send_done && s.recv_done && s.phase != kPhaseDone) {
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
                        BlockBytes(task), BlockBytes(task), "Reduce")) {
            return false;
        }
    }
    return true;
}

bool CompleteReduceScatter(PlanTask& task) {
    CollOpState& s = task.state;
    while (s.send_done && s.recv_done && s.phase != kPhaseDone) {
        int next_phase = kPhaseDone;
        ReduceBlock(task, s.temp_buffer.data() + static_cast<size_t>(task.world_size) * BlockBytes(task),
                    s.temp_buffer.data() + static_cast<size_t>(ReducedChunk(task)) * BlockBytes(task));
        if (++s.step < task.world_size - 1) {
            next_phase = kPhaseExchange;
        } else {
            std::memcpy(task.recv_buf, s.temp_buffer.data() + static_cast<size_t>(task.rank) * BlockBytes(task),
                        BlockBytes(task));
            if (task.reduce_op == ReduceOp::AVG) {
                Utils::ApplyAverage(task.recv_buf, task.elem_count, task.dtype, task.world_size);
            }
        }
        if (!BeginPhase(task, next_phase, ReduceScatterSendPtr(task), ReduceScatterRecvPtr(task), BlockBytes(task),
                        BlockBytes(task), "ReduceScatter")) {
            return false;
        }
    }
    return true;
}

bool CompleteAllreduce(PlanTask& task) {
    CollOpState& s = task.state;
    while (s.send_done && s.recv_done && s.phase != kPhaseDone) {
        size_t type_size = Utils::GetDataTypeSize(task.dtype);
        size_t chunk = task.chunk_size;
        char* data = static_cast<char*>(task.recv_buf);
        if (s.phase == kPhaseReduceScatter) {
            size_t recv_count = ChunkElemCount(task.elem_count, chunk, AllreduceRecvChunk(task));
            Utils::PerformReduce(s.temp_buffer.data(),
                                 data + static_cast<size_t>(AllreduceRecvChunk(task)) * chunk * type_size, recv_count,
                                 task.dtype, task.reduce_op == ReduceOp::AVG ? ReduceOp::SUM : task.reduce_op);
        }

        ++s.step;
        if (s.phase == kPhaseReduceScatter && s.step >= task.world_size - 1) {
            s.phase = kPhaseAllGather;
            s.step = 0;
        } else if (s.phase == kPhaseAllGather && s.step >= task.world_size - 1) {
            if (task.reduce_op == ReduceOp::AVG) {
                Utils::ApplyAverage(task.recv_buf, task.elem_count, task.dtype, task.world_size);
            }
            s.phase = kPhaseDone;
            return true;
        }

        if (!BeginPhase(task, s.phase, AllreduceSendPtr(task), AllreduceRecvPtr(task), AllreduceSendBytes(task),
                        AllreduceRecvBytes(task), "AllReduce")) {
            return false;
        }
    }
    return true;
}

} // namespace

void TopologyRing::FillChannels(std::vector<Channel>& channels) const {
    for (size_t i = 0; i < channels.size(); ++i) {
        channels[i].id = static_cast<int>(i);
        channels[i].ring.prev = GetPrevRank(rank);
        channels[i].ring.next = GetNextRank(rank);

        channels[i].send.resize(static_cast<size_t>(world_size));
        channels[i].recv.resize(static_cast<size_t>(world_size));
        for (int peer = 0; peer < world_size; ++peer) {
            channels[i].send[static_cast<size_t>(peer)].peer = peer;
            channels[i].send[static_cast<size_t>(peer)].channel_id = channels[i].id;
            channels[i].send[static_cast<size_t>(peer)].is_send = true;
            channels[i].recv[static_cast<size_t>(peer)].peer = peer;
            channels[i].recv[static_cast<size_t>(peer)].channel_id = channels[i].id;
            channels[i].recv[static_cast<size_t>(peer)].is_send = false;
        }
    }
}

int TopologyRing::GetPrevRank(int r) const {
    return (r - 1 + world_size) % world_size;
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
        s.send_done = s.recv_done = true;
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

    if (!task.send_transport || !task.recv_transport) {
        LOG_ERROR("Ring AllReduce received task without transports");
        return false;
    }

    s.temp_buffer.resize(task.chunk_size * type_size);
    s.phase = kPhaseReduceScatter;
    s.step = 0;
    return BeginPhase(task, kPhaseReduceScatter, AllreduceSendPtr(task), AllreduceRecvPtr(task),
                      AllreduceSendBytes(task), AllreduceRecvBytes(task), "AllReduce") &&
           CompleteAllreduce(task);
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

    if (!PushBuffer(task, event, AllreduceSendPtr(task), AllreduceRecvPtr(task), AllreduceSendBytes(task),
                    AllreduceRecvBytes(task), "AllReduce")) {
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
        s.send_done = s.recv_done = true;
        return true;
    }
    if (!task.recv_buf) {
        LOG_ERROR("Ring Broadcast received missing buffer");
        return false;
    }
    if (task.world_size == 1) {
        s.phase = kPhaseDone;
        s.send_done = s.recv_done = true;
        return true;
    }
    if (!task.send_transport || !task.recv_transport) {
        LOG_ERROR("Ring Broadcast received task without transports");
        return false;
    }
    int phase = task.rank == task.root ? kPhaseSend : kPhaseRecv;
    return BeginPhase(task, phase, static_cast<const char*>(task.recv_buf), static_cast<char*>(task.recv_buf),
                      BlockBytes(task), BlockBytes(task), "Broadcast") &&
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
                    BlockBytes(task), BlockBytes(task), "Broadcast")) {
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
        s.send_done = s.recv_done = true;
        return true;
    }
    if (!task.send_buf || !task.recv_buf) {
        LOG_ERROR("Ring AllGather received missing buffer");
        return false;
    }
    std::memcpy(OutputBlock(task, task.rank), task.send_buf, BlockBytes(task));
    if (task.world_size == 1) {
        s.phase = kPhaseDone;
        s.send_done = s.recv_done = true;
        return true;
    }
    if (!task.send_transport || !task.recv_transport) {
        LOG_ERROR("Ring AllGather received task without transports");
        return false;
    }
    return BeginPhase(task, kPhaseExchange, AllGatherSendPtr(task), AllGatherRecvPtr(task), BlockBytes(task),
                      BlockBytes(task), "AllGather") &&
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
    if (!PushBuffer(task, event, AllGatherSendPtr(task), AllGatherRecvPtr(task), BlockBytes(task), BlockBytes(task),
                    "AllGather")) {
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
        s.send_done = s.recv_done = true;
        return true;
    }
    if (!task.send_buf || (task.rank == task.root && !task.recv_buf)) {
        LOG_ERROR("Ring Reduce received missing buffer");
        return false;
    }
    if (task.world_size == 1) {
        std::memcpy(task.recv_buf, task.send_buf, BlockBytes(task));
        s.phase = kPhaseDone;
        s.send_done = s.recv_done = true;
        return true;
    }
    if (!task.send_transport || !task.recv_transport) {
        LOG_ERROR("Ring Reduce received task without transports");
        return false;
    }
    s.temp_buffer.resize(2 * BlockBytes(task));
    std::memcpy(s.temp_buffer.data(), task.send_buf, BlockBytes(task));
    int phase = RootPosition(task) == 1 ? kPhaseSend : kPhaseRecv;
    return BeginPhase(task, phase, s.temp_buffer.data(), s.temp_buffer.data() + BlockBytes(task), BlockBytes(task),
                      BlockBytes(task), "Reduce") &&
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
                    BlockBytes(task), "Reduce")) {
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
        s.send_done = s.recv_done = true;
        return true;
    }
    if (!task.send_buf || !task.recv_buf) {
        LOG_ERROR("Ring ReduceScatter received missing buffer");
        return false;
    }
    if (task.world_size == 1) {
        std::memcpy(task.recv_buf, task.send_buf, BlockBytes(task));
        s.phase = kPhaseDone;
        s.send_done = s.recv_done = true;
        return true;
    }
    if (!task.send_transport || !task.recv_transport) {
        LOG_ERROR("Ring ReduceScatter received task without transports");
        return false;
    }
    s.temp_buffer.resize((static_cast<size_t>(task.world_size) + 1) * BlockBytes(task));
    PackInput(task);
    return BeginPhase(task, kPhaseExchange, ReduceScatterSendPtr(task), ReduceScatterRecvPtr(task), BlockBytes(task),
                      BlockBytes(task), "ReduceScatter") &&
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
                    BlockBytes(task), "ReduceScatter")) {
        return false;
    }
    return CompleteReduceScatter(task);
}
