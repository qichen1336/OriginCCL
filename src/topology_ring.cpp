#include <algorithm>
#include <cstring>
#include <vector>
#include "topology_ring.h"
#include "transport.h"
#include "utils.h"
#include "logger.h"

namespace {
// CollOpState.phase values.
constexpr int kPhaseUnstarted = 0;
constexpr int kPhaseReduceScatter = 1;
constexpr int kPhaseAllGather = 2;
constexpr int kPhaseDone = 3;

size_t ChunkElemCount(size_t count, size_t chunk_size, int chunk_index) {
    size_t start = static_cast<size_t>(chunk_index) * chunk_size;
    return start >= count ? 0 : std::min(chunk_size, count - start);
}

// Recompute the current step's chunk indices from the cursor. Everything about a step's
// layout is derivable from phase/step/rank, so the cursor does not store it.
int SendChunk(const PlanTask& task) {
    const CollOpState& s = task.state;
    if (s.phase == kPhaseReduceScatter) {
        return (task.rank - s.step + task.world_size) % task.world_size;
    }
    return (task.rank - s.step + 1 + task.world_size) % task.world_size;
}

int RecvChunk(const PlanTask& task) {
    const CollOpState& s = task.state;
    if (s.phase == kPhaseReduceScatter) {
        return (task.rank - s.step + task.world_size - 1) % task.world_size;
    }
    return (task.rank - s.step + task.world_size) % task.world_size;
}

size_t SendBytes(const PlanTask& task) {
    return ChunkElemCount(task.elem_count, task.chunk_size, SendChunk(task)) * Utils::GetDataTypeSize(task.dtype);
}

size_t RecvBytes(const PlanTask& task) {
    return ChunkElemCount(task.elem_count, task.chunk_size, RecvChunk(task)) * Utils::GetDataTypeSize(task.dtype);
}

// ReduceScatter receives into the scratch buffer (folded in on completion); AllGather
// receives straight into the destination chunk.
char* RecvPtr(const PlanTask& task) {
    const CollOpState& s = task.state;
    if (s.phase == kPhaseReduceScatter) {
        return const_cast<char*>(s.temp_buffer.data());
    }
    return static_cast<char*>(task.recv_buf) +
           static_cast<size_t>(RecvChunk(task)) * task.chunk_size * Utils::GetDataTypeSize(task.dtype);
}

const char* SendPtr(const PlanTask& task) {
    return static_cast<const char*>(task.recv_buf) +
           static_cast<size_t>(SendChunk(task)) * task.chunk_size * Utils::GetDataTypeSize(task.dtype);
}

// Reset the per-step transfer progress in the cursor for the step just entered.
void BeginStep(PlanTask& task) {
    CollOpState& s = task.state;
    s.send_progress = 0;
    s.recv_progress = 0;
    s.send_done = (SendBytes(task) == 0);
    s.recv_done = (RecvBytes(task) == 0);
}

// A step is complete when both its transfers finish; fold the scratch buffer into the
// reduced chunk (ReduceScatter only), then advance the phase machine.
void CompleteStep(PlanTask& task) {
    CollOpState& s = task.state;
    size_t type_size = Utils::GetDataTypeSize(task.dtype);
    size_t chunk = task.chunk_size;
    char* data = static_cast<char*>(task.recv_buf);

    if (s.phase == kPhaseReduceScatter) {
        size_t recv_count = ChunkElemCount(task.elem_count, chunk, RecvChunk(task));
        Utils::PerformReduce(s.temp_buffer.data(), data + static_cast<size_t>(RecvChunk(task)) * chunk * type_size,
                             recv_count, task.dtype, task.reduce_op == ReduceOp::AVG ? ReduceOp::SUM : task.reduce_op);
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
        return;
    }

    BeginStep(task);
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

bool TopologyRing::AllreduceInit(PlanTask& task) const noexcept {
    CollOpState& s = task.state;
    s = CollOpState{};

    if (!task.send_buf || !task.recv_buf || task.world_size <= 0) {
        LOG_ERROR("Ring AllReduce received invalid task");
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
    BeginStep(task);
    return true;
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

    // Advance only the transfer whose readiness fired. A send-fd writable event drives the
    // send; a recv-fd readable event drives the recv. Both run concurrently within a step.
    // A false return is the failure channel; the executor abandons the collective on it.
    if (event == CollEvent::Writable && !s.send_done) {
        if (!task.send_transport->TrySend(SendPtr(task), SendBytes(task), &s.send_progress, &s.send_done)) {
            LOG_ERROR("Ring AllReduce send failed on rank {} (phase {}, step {})", task.rank, s.phase, s.step);
            return false;
        }
    } else if (event == CollEvent::Readable && !s.recv_done) {
        if (!task.recv_transport->TryRecv(RecvPtr(task), RecvBytes(task), &s.recv_progress, &s.recv_done)) {
            LOG_ERROR("Ring AllReduce recv failed on rank {} (phase {}, step {})", task.rank, s.phase, s.step);
            return false;
        }
    }

    if (s.send_done && s.recv_done) {
        CompleteStep(task);
    }
    return true;
}

bool TopologyRing::AllreduceDone(const PlanTask& task) const {
    return task.state.phase == kPhaseDone;
}
