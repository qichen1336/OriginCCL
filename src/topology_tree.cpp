#include <algorithm>
#include <cstring>
#include "topology_tree.h"
#include "communicator.h"
#include "transport/transport.h"
#include "utils.h"
#include "logger.h"

namespace {

constexpr int kPhaseUnstarted = 0;
constexpr int kPhaseUpStar = 1;
constexpr int kPhaseUpTreeRecv = 2;
constexpr int kPhaseUpTreeSend = 3;
constexpr int kPhaseDownTreeRecv = 4;
constexpr int kPhaseDownTreeSend = 5;
constexpr int kPhaseDownStar = 6;
constexpr int kPhaseDone = 7;

constexpr int kRoleStar = 0;
constexpr int kRoleTreeDown = 1;
constexpr int kRoleTreeUp = 2;

constexpr uint32_t kStarMask = 1u << kRoleStar;
constexpr uint32_t kDownMask = 1u << kRoleTreeDown;
constexpr uint32_t kUpMask = 1u << kRoleTreeUp;

size_t TypeSize(const PlanTask& task) {
    return Utils::GetDataTypeSize(task.dtype);
}

size_t TotalElems(const PlanTask& task) {
    if (task.func == CollFunc::AllReduce) {
        return task.elem_count;
    }
    return static_cast<size_t>(task.world_size) * task.elem_count;
}

size_t ChunkCount(const PlanTask& task) {
    if (task.chunk_size == 0) {
        return 0;
    }
    return (TotalElems(task) + task.chunk_size - 1) / task.chunk_size;
}

size_t ChunkElems(const PlanTask& task, size_t chunk) {
    size_t total = TotalElems(task);
    size_t start = chunk * task.chunk_size;
    return start >= total ? 0 : std::min(task.chunk_size, total - start);
}

bool SideDone(const std::vector<char>& done) {
    return std::all_of(done.begin(), done.end(), [](char d) { return d != 0; });
}

uint32_t Bit(int role) {
    return 1u << role;
}

char* Accumulator(PlanTask& task) {
    return task.state.temp_buffer.data();
}

char* ScratchSlot(PlanTask& task, size_t i) {
    return task.state.temp_buffer.data() + TotalElems(task) * TypeSize(task) + i * task.chunk_size * TypeSize(task);
}

ReduceOp CombineOp(const PlanTask& task) {
    if (task.func == CollFunc::AllGather) {
        return ReduceOp::SUM;
    }
    return task.reduce_op == ReduceOp::AVG ? ReduceOp::SUM : task.reduce_op;
}

size_t CountReduceRecv(const std::vector<int>& roles) {
    size_t n = 0;
    for (int role : roles) {
        if (role == kRoleStar || role == kRoleTreeDown) {
            ++n;
        }
    }
    return n;
}

void SeedAccumulator(PlanTask& task) {
    size_t type_size = TypeSize(task);
    size_t stride = task.elem_count;
    char* acc = Accumulator(task);
    if (task.func == CollFunc::AllReduce) {
        std::memcpy(acc, task.send_buf, stride * type_size);
        return;
    }
    if (task.func == CollFunc::ReduceScatter) {
        for (int block = 0; block < task.world_size; ++block) {
            std::memcpy(acc + static_cast<size_t>(block) * stride * type_size,
                        static_cast<const char*>(task.send_buf) +
                            static_cast<size_t>(block) * task.rank_stride * type_size,
                        stride * type_size);
        }
        return;
    }
    std::memset(acc, 0, TotalElems(task) * type_size);
    std::memcpy(acc + static_cast<size_t>(task.rank) * stride * type_size, task.send_buf, stride * type_size);
}

void ExtractResult(PlanTask& task) {
    size_t type_size = TypeSize(task);
    size_t stride = task.elem_count;
    const char* acc = Accumulator(task);
    if (task.func == CollFunc::AllReduce) {
        std::memcpy(task.recv_buf, acc, stride * type_size);
        if (task.reduce_op == ReduceOp::AVG) {
            Utils::ApplyAverage(task.recv_buf, stride, task.dtype, task.world_size);
        }
        return;
    }
    if (task.func == CollFunc::ReduceScatter) {
        std::memcpy(task.recv_buf, acc + static_cast<size_t>(task.rank) * stride * type_size, stride * type_size);
        if (task.reduce_op == ReduceOp::AVG) {
            Utils::ApplyAverage(task.recv_buf, stride, task.dtype, task.world_size);
        }
        return;
    }
    for (int block = 0; block < task.world_size; ++block) {
        std::memcpy(static_cast<char*>(task.recv_buf) + static_cast<size_t>(block) * task.rank_stride * type_size,
                    acc + static_cast<size_t>(block) * stride * type_size, stride * type_size);
    }
}

void CombineChunk(PlanTask& task, const std::vector<int>& roles, size_t chunk, int role) {
    size_t type_size = TypeSize(task);
    size_t nelems = ChunkElems(task, chunk);
    char* acc = Accumulator(task) + chunk * task.chunk_size * type_size;
    ReduceOp op = CombineOp(task);
    for (size_t i = 0; i < task.recv_transports.size(); ++i) {
        if (roles[i] != role) {
            continue;
        }
        Utils::PerformReduce(ScratchSlot(task, i), acc, nelems, task.dtype, op);
    }
}

bool PushSend(PlanTask& task, const char* send_data, size_t send_bytes) {
    CollOpState& s = task.state;
    for (size_t i = 0; i < task.send_transports.size(); ++i) {
        if (s.send_done[i]) {
            continue;
        }
        bool done = false;
        if (!task.send_transports[i]->TrySend(send_data, send_bytes, &s.send_progress[i], &done)) {
            LOG_ERROR("Tree send failed on rank {} (phase {})", task.rank, s.phase);
            return false;
        }
        s.send_done[i] = done ? 1 : 0;
    }
    return true;
}

bool PushRecv(PlanTask& task, char* shared_recv, size_t recv_bytes, bool per_conn) {
    CollOpState& s = task.state;
    for (size_t i = 0; i < task.recv_transports.size(); ++i) {
        if (s.recv_done[i]) {
            continue;
        }
        char* dst = per_conn ? ScratchSlot(task, i) : shared_recv;
        bool done = false;
        if (!task.recv_transports[i]->TryRecv(dst, recv_bytes, &s.recv_progress[i], &done)) {
            LOG_ERROR("Tree recv failed on rank {} (phase {})", task.rank, s.phase);
            return false;
        }
        s.recv_done[i] = done ? 1 : 0;
    }
    return true;
}

bool RecvPerConn(int phase) {
    return phase == kPhaseUpStar || phase == kPhaseUpTreeRecv;
}

bool Activate(PlanTask& task, const std::vector<int>& roles, uint32_t send_mask, uint32_t recv_mask, size_t chunk) {
    CollOpState& s = task.state;
    for (size_t i = 0; i < task.send_transports.size(); ++i) {
        if ((send_mask & Bit(roles[i])) != 0) {
            if (s.send_done[i]) {
                s.send_done[i] = 0;
                s.send_progress[i] = 0;
            }
        } else {
            s.send_done[i] = 1;
        }
    }
    for (size_t i = 0; i < task.recv_transports.size(); ++i) {
        if ((recv_mask & Bit(roles[i])) != 0) {
            if (s.recv_done[i]) {
                s.recv_done[i] = 0;
                s.recv_progress[i] = 0;
            }
        } else {
            s.recv_done[i] = 1;
        }
    }
    size_t nbytes = ChunkElems(task, chunk) * TypeSize(task);
    char* acc_chunk = Accumulator(task) + chunk * task.chunk_size * TypeSize(task);
    return PushSend(task, acc_chunk, nbytes) && PushRecv(task, acc_chunk, nbytes, RecvPerConn(s.phase));
}

bool StartUpStar(PlanTask& task, const std::vector<int>& roles, bool is_leader) {
    task.state.phase = kPhaseUpStar;
    return Activate(task, roles, is_leader ? 0 : kStarMask, is_leader ? kStarMask : 0, task.state.algo.tree.step);
}

bool StartUpTreeRecv(PlanTask& task, const std::vector<int>& roles) {
    task.state.phase = kPhaseUpTreeRecv;
    return Activate(task, roles, 0, kDownMask, task.state.algo.tree.step);
}

bool StartUpTreeSend(PlanTask& task, const std::vector<int>& roles) {
    task.state.phase = kPhaseUpTreeSend;
    return Activate(task, roles, kUpMask, 0, task.state.algo.tree.step);
}

bool StartDownTreeRecv(PlanTask& task, const std::vector<int>& roles) {
    task.state.phase = kPhaseDownTreeRecv;
    return Activate(task, roles, 0, kUpMask, task.state.algo.tree.step);
}

bool StartDownTreeSend(PlanTask& task, const std::vector<int>& roles) {
    task.state.phase = kPhaseDownTreeSend;
    return Activate(task, roles, kDownMask, 0, task.state.algo.tree.step);
}

bool StartDownStar(PlanTask& task, const std::vector<int>& roles, bool is_leader) {
    task.state.phase = kPhaseDownStar;
    return Activate(task, roles, is_leader ? kStarMask : 0, is_leader ? 0 : kStarMask, task.state.algo.tree.step);
}

bool StartDown(PlanTask& task, const std::vector<int>& roles, bool is_leader) {
    if (is_leader) {
        return StartDownTreeRecv(task, roles);
    }
    return StartDownStar(task, roles, is_leader);
}

bool NextUpChunk(PlanTask& task, const std::vector<int>& roles, bool is_leader) {
    CollOpState& s = task.state;
    if (static_cast<size_t>(s.algo.tree.step + 1) >= ChunkCount(task)) {
        s.algo.tree.step = 0;
        return StartDown(task, roles, is_leader);
    }
    ++s.algo.tree.step;
    return StartUpStar(task, roles, is_leader);
}

bool NextDownChunk(PlanTask& task, const std::vector<int>& roles, bool is_leader) {
    CollOpState& s = task.state;
    if (static_cast<size_t>(s.algo.tree.step + 1) >= ChunkCount(task)) {
        s.phase = kPhaseDone;
        ExtractResult(task);
        return true;
    }
    ++s.algo.tree.step;
    return StartDown(task, roles, is_leader);
}

bool Complete(PlanTask& task, const std::vector<int>& roles, bool is_leader) {
    CollOpState& s = task.state;
    while (s.phase != kPhaseDone) {
        switch (s.phase) {
        case kPhaseUpStar:
            if (is_leader) {
                if (!SideDone(s.recv_done)) {
                    return true;
                }
                CombineChunk(task, roles, static_cast<size_t>(s.algo.tree.step), kRoleStar);
                if (!StartUpTreeRecv(task, roles)) {
                    return false;
                }
            } else {
                if (!SideDone(s.send_done)) {
                    return true;
                }
                if (!NextUpChunk(task, roles, is_leader)) {
                    return false;
                }
            }
            continue;
        case kPhaseUpTreeRecv:
            if (!SideDone(s.recv_done)) {
                return true;
            }
            CombineChunk(task, roles, static_cast<size_t>(s.algo.tree.step), kRoleTreeDown);
            if (!StartUpTreeSend(task, roles)) {
                return false;
            }
            continue;
        case kPhaseUpTreeSend:
            if (!SideDone(s.send_done)) {
                return true;
            }
            if (!NextUpChunk(task, roles, is_leader)) {
                return false;
            }
            continue;
        case kPhaseDownTreeRecv:
            if (!SideDone(s.recv_done)) {
                return true;
            }
            if (!StartDownTreeSend(task, roles)) {
                return false;
            }
            continue;
        case kPhaseDownTreeSend:
            if (!SideDone(s.send_done)) {
                return true;
            }
            if (!StartDownStar(task, roles, is_leader)) {
                return false;
            }
            continue;
        case kPhaseDownStar:
            if (is_leader) {
                if (!SideDone(s.send_done)) {
                    return true;
                }
            } else {
                if (!SideDone(s.recv_done)) {
                    return true;
                }
            }
            if (!NextDownChunk(task, roles, is_leader)) {
                return false;
            }
            continue;
        default:
            return true;
        }
    }
    return true;
}

bool PushStep(PlanTask& task, CollEvent event) {
    CollOpState& s = task.state;
    size_t chunk = static_cast<size_t>(s.algo.tree.step);
    size_t nbytes = ChunkElems(task, chunk) * TypeSize(task);
    char* acc_chunk = Accumulator(task) + chunk * task.chunk_size * TypeSize(task);
    if (event == CollEvent::Writable) {
        return PushSend(task, acc_chunk, nbytes);
    }
    return PushRecv(task, acc_chunk, nbytes, RecvPerConn(s.phase));
}

bool TreeInit(PlanTask& task, const std::vector<int>& roles, bool is_leader) {
    CollOpState& s = task.state;
    s = CollOpState{};
    s.algo.tree.step = 0;

    if (task.world_size <= 0) {
        LOG_ERROR("Tree received invalid world size");
        return false;
    }
    if (task.elem_count == 0) {
        s.phase = kPhaseDone;
        return true;
    }
    if (!task.send_buf || !task.recv_buf) {
        LOG_ERROR("Tree received missing buffer");
        return false;
    }

    size_t total = TotalElems(task);
    size_t total_bytes = total * TypeSize(task);
    size_t n_reduce_recv = is_leader ? CountReduceRecv(roles) : 0;
    s.temp_buffer.resize(total_bytes + n_reduce_recv * task.chunk_size * TypeSize(task));

    s.send_progress.assign(task.send_transports.size(), 0);
    s.recv_progress.assign(task.recv_transports.size(), 0);
    s.send_done.assign(task.send_transports.size(), 1);
    s.recv_done.assign(task.recv_transports.size(), 1);

    SeedAccumulator(task);

    if (task.world_size == 1) {
        ExtractResult(task);
        s.phase = kPhaseDone;
        return true;
    }
    if (task.send_transports.empty() || task.recv_transports.empty()) {
        LOG_ERROR("Tree received task without transports");
        return false;
    }

    return StartUpStar(task, roles, is_leader) && Complete(task, roles, is_leader);
}

} // namespace

void TopologyTree::FillChannels(const Communicator& comm, std::vector<Channel>& channels) {
    is_leader_ = comm.GetLocalRank() == 0;
    const std::vector<int>& local_ranks = comm.GetLocalRanks();
    int machine_index = comm.GetMachineIndex();
    int machine_count = comm.GetMachineCount();
    const std::vector<int>& machine_leaders = comm.GetMachineLeaders();

    std::vector<int> star_peers;
    if (is_leader_) {
        for (size_t i = 1; i < local_ranks.size(); ++i) {
            star_peers.push_back(local_ranks[i]);
        }
    } else {
        star_peers.push_back(local_ranks[0]);
    }

    channel_roles_.assign(channels.size(), std::vector<int>{});
    for (Channel& channel : channels) {
        std::vector<int> children;
        int parent = -1;
        if (is_leader_) {
            const bool mirrored = channel.id % 2 != 0;
            int node = mirrored ? (machine_count - 1 - machine_index) : machine_index;
            int parent_node = (node == 0) ? -1 : (node - 1) / 2;
            int child0 = 2 * node + 1;
            int child1 = 2 * node + 2;
            if (mirrored) {
                parent_node = (parent_node < 0) ? -1 : (machine_count - 1 - parent_node);
                child0 = (child0 < machine_count) ? (machine_count - 1 - child0) : machine_count;
                child1 = (child1 < machine_count) ? (machine_count - 1 - child1) : machine_count;
            }
            if (parent_node >= 0) {
                parent = machine_leaders[static_cast<size_t>(parent_node)];
            }
            if (child0 < machine_count) {
                children.push_back(machine_leaders[static_cast<size_t>(child0)]);
            }
            if (child1 < machine_count) {
                children.push_back(machine_leaders[static_cast<size_t>(child1)]);
            }
        }
        channel.tree.parent = parent;
        channel.tree.children = children;
        channel.tree.star_peers = star_peers;

        std::vector<int>& roles = channel_roles_[static_cast<size_t>(channel.id)];
        if (is_leader_) {
            roles.insert(roles.end(), star_peers.size(), kRoleStar);
            roles.insert(roles.end(), children.size(), kRoleTreeDown);
            if (parent >= 0) {
                roles.push_back(kRoleTreeUp);
            }
        } else {
            roles.push_back(kRoleStar);
        }
    }
}

void TopologyTree::FillPeers(const Channel& channel, std::vector<TopoEdge>& edges) const {
    edges.clear();
    auto add_peer = [&edges](int peer) {
        edges.push_back(TopoEdge{peer, true});
        edges.push_back(TopoEdge{peer, false});
    };
    for (int peer : channel.tree.star_peers) {
        add_peer(peer);
    }
    for (int peer : channel.tree.children) {
        add_peer(peer);
    }
    if (channel.tree.parent >= 0) {
        add_peer(channel.tree.parent);
    }
}

void TopologyTree::FillTransports(Channel& channel, PlanTask& task) const {
    auto& send_out = task.send_transports;
    auto& recv_out = task.recv_transports;
    send_out.clear();
    recv_out.clear();
    auto add_peer = [&](int peer) {
        Connector* send_conn = channel.SendConnector(peer);
        if (send_conn && send_conn->transport) {
            send_out.push_back(send_conn->transport);
        }
        Connector* recv_conn = channel.RecvConnector(peer);
        if (recv_conn && recv_conn->transport) {
            recv_out.push_back(recv_conn->transport);
        }
    };
    for (int peer : channel.tree.star_peers) {
        add_peer(peer);
    }
    for (int peer : channel.tree.children) {
        add_peer(peer);
    }
    if (channel.tree.parent >= 0) {
        add_peer(channel.tree.parent);
    }
}

bool TopologyTree::CollectiveInit(PlanTask& task) const noexcept {
    switch (task.func) {
    case CollFunc::AllReduce:
    case CollFunc::ReduceScatter:
    case CollFunc::AllGather:
        return TreeInit(task, channel_roles_[static_cast<size_t>(task.channel_id)], IsLeader());
    default:
        LOG_ERROR("Tree received unsupported collective");
        return false;
    }
}

bool TopologyTree::CollectiveStep(PlanTask& task, CollEvent event) const noexcept {
    CollOpState& s = task.state;
    const std::vector<int>& roles = channel_roles_[static_cast<size_t>(task.channel_id)];
    if (s.phase == kPhaseUnstarted && !TreeInit(task, roles, IsLeader())) {
        return false;
    }
    if (s.phase == kPhaseDone) {
        return true;
    }
    if (!PushStep(task, event)) {
        return false;
    }
    return Complete(task, roles, IsLeader());
}

bool TopologyTree::CollectiveDone(const PlanTask& task) const {
    return task.state.phase == kPhaseDone;
}
