#include <algorithm>
#include <cstring>
#include "topology/topology_tree.h"
#include "communicator.h"
#include "transport/transport.h"
#include "utils.h"
#include "logger.h"

namespace {

constexpr int kPhaseUnstarted = 0;
constexpr int kPhaseUp = 1;
constexpr int kPhaseDown = 2;
constexpr int kPhaseDone = 3;

constexpr int kRoleStar = 0;
constexpr int kRoleTreeDown = 1;
constexpr int kRoleTreeUp = 2;

size_t TypeSize(const PlanTask& task) {
    return Utils::GetDataTypeSize(task.dtype);
}

size_t ChunkCount(const PlanTask& task) {
    if (task.chunk_size == 0) {
        return 0;
    }
    return (task.elem_count + task.chunk_size - 1) / task.chunk_size;
}

size_t ChunkElems(const PlanTask& task, size_t chunk) {
    size_t start = chunk * task.chunk_size;
    return start >= task.elem_count ? 0 : std::min(task.chunk_size, task.elem_count - start);
}

size_t ChunkBytes(const PlanTask& task, size_t chunk) {
    return ChunkElems(task, chunk) * TypeSize(task);
}

size_t ChunkOffset(const PlanTask& task, size_t chunk) {
    return chunk * task.chunk_size * TypeSize(task);
}

bool IsInputRole(int role) {
    return role == kRoleStar || role == kRoleTreeDown;
}

int ParentEdge(const std::vector<int>& roles) {
    for (size_t i = 0; i < roles.size(); ++i) {
        if (roles[i] == kRoleTreeUp) {
            return static_cast<int>(i);
        }
    }
    return -1;
}

const char* SendSource(const PlanTask& task, bool is_leader, size_t chunk) {
    const char* base = is_leader ? static_cast<const char*>(task.recv_buf) : static_cast<const char*>(task.send_buf);
    return base + ChunkOffset(task, chunk);
}

char* RecvTarget(PlanTask& task, bool is_leader, const std::vector<int>& roles, size_t edge, size_t chunk) {
    if (is_leader && IsInputRole(roles[edge])) {
        return task.state.temp_buffer.data() + edge * task.chunk_size * TypeSize(task);
    }
    return static_cast<char*>(task.recv_buf) + ChunkOffset(task, chunk);
}

void ReduceChunk(PlanTask& task, size_t edge, size_t chunk) {
    const char* src = task.state.temp_buffer.data() + edge * task.chunk_size * TypeSize(task);
    char* acc = static_cast<char*>(task.recv_buf) + ChunkOffset(task, chunk);
    Utils::PerformReduce(src, acc, ChunkElems(task, chunk), task.dtype,
                         task.reduce_op == ReduceOp::AVG ? ReduceOp::SUM : task.reduce_op);
}

size_t MinInputPrefix(const PlanTask& task, const std::vector<int>& roles) {
    const CollOpState& s = task.state;
    size_t best = ChunkCount(task);
    for (size_t i = 0; i < roles.size(); ++i) {
        if (IsInputRole(roles[i])) {
            best = std::min(best, static_cast<size_t>(s.algo.tree.recv_chunk[i]));
        }
    }
    return best;
}

bool UpComplete(const PlanTask& task, const std::vector<int>& roles, bool is_leader) {
    const CollOpState& s = task.state;
    size_t n_chunks = ChunkCount(task);
    if (is_leader) {
        for (size_t i = 0; i < roles.size(); ++i) {
            if (IsInputRole(roles[i]) && static_cast<size_t>(s.algo.tree.recv_chunk[i]) < n_chunks) {
                return false;
            }
        }
        for (size_t j = 0; j < roles.size(); ++j) {
            if (roles[j] == kRoleTreeUp && static_cast<size_t>(s.algo.tree.send_chunk[j]) < n_chunks) {
                return false;
            }
        }
        return true;
    }
    for (size_t j = 0; j < roles.size(); ++j) {
        if (static_cast<size_t>(s.algo.tree.send_chunk[j]) < n_chunks) {
            return false;
        }
    }
    return true;
}

bool DownComplete(const PlanTask& task, const std::vector<int>& roles, bool is_leader) {
    const CollOpState& s = task.state;
    size_t n_chunks = ChunkCount(task);
    if (is_leader) {
        for (size_t i = 0; i < roles.size(); ++i) {
            if (roles[i] == kRoleTreeUp && static_cast<size_t>(s.algo.tree.recv_chunk[i]) < n_chunks) {
                return false;
            }
        }
        for (size_t j = 0; j < roles.size(); ++j) {
            if (IsInputRole(roles[j]) && static_cast<size_t>(s.algo.tree.send_chunk[j]) < n_chunks) {
                return false;
            }
        }
        return true;
    }
    for (size_t i = 0; i < roles.size(); ++i) {
        if (static_cast<size_t>(s.algo.tree.recv_chunk[i]) < n_chunks) {
            return false;
        }
    }
    return true;
}

bool PushRecvs(PlanTask& task, const std::vector<int>& roles, bool is_leader) {
    CollOpState& s = task.state;
    size_t n_chunks = ChunkCount(task);
    for (size_t i = 0; i < task.recv_transports.size(); ++i) {
        bool eligible = is_leader ? (s.phase == kPhaseUp ? IsInputRole(roles[i]) : roles[i] == kRoleTreeUp)
                                  : s.phase == kPhaseDown;
        if (!eligible) {
            continue;
        }
        while (static_cast<size_t>(s.algo.tree.recv_chunk[i]) < n_chunks) {
            size_t chunk = static_cast<size_t>(s.algo.tree.recv_chunk[i]);
            if (s.recv_done[i]) {
                s.recv_progress[i] = 0;
                s.recv_done[i] = 0;
            }
            bool done = false;
            if (!task.recv_transports[i]->TryRecv(RecvTarget(task, is_leader, roles, i, chunk),
                                                  ChunkBytes(task, chunk), &s.recv_progress[i], &done)) {
                LOG_ERROR("Tree recv failed on rank {} (phase {})", task.rank, s.phase);
                return false;
            }
            s.recv_done[i] = done ? 1 : 0;
            if (!done) {
                break;
            }
            if (is_leader && IsInputRole(roles[i])) {
                ReduceChunk(task, i, chunk);
            }
            ++s.algo.tree.recv_chunk[i];
        }
    }
    return true;
}

bool PushSends(PlanTask& task, const std::vector<int>& roles, bool is_leader) {
    CollOpState& s = task.state;
    size_t n_chunks = ChunkCount(task);
    size_t gate = n_chunks;
    if (is_leader) {
        if (s.phase == kPhaseUp) {
            gate = MinInputPrefix(task, roles);
        } else {
            int parent = ParentEdge(roles);
            gate = parent < 0 ? n_chunks : static_cast<size_t>(s.algo.tree.recv_chunk[static_cast<size_t>(parent)]);
        }
    }
    for (size_t j = 0; j < task.send_transports.size(); ++j) {
        bool eligible = is_leader ? (s.phase == kPhaseUp ? roles[j] == kRoleTreeUp : IsInputRole(roles[j]))
                                  : s.phase == kPhaseUp;
        if (!eligible) {
            continue;
        }
        while (static_cast<size_t>(s.algo.tree.send_chunk[j]) < gate) {
            size_t chunk = static_cast<size_t>(s.algo.tree.send_chunk[j]);
            if (s.send_done[j]) {
                s.send_progress[j] = 0;
                s.send_done[j] = 0;
            }
            bool done = false;
            if (!task.send_transports[j]->TrySend(SendSource(task, is_leader, chunk), ChunkBytes(task, chunk),
                                                  &s.send_progress[j], &done)) {
                LOG_ERROR("Tree send failed on rank {} (phase {})", task.rank, s.phase);
                return false;
            }
            s.send_done[j] = done ? 1 : 0;
            if (!done) {
                break;
            }
            ++s.algo.tree.send_chunk[j];
        }
    }
    return true;
}

bool EnterDown(PlanTask& task, const std::vector<int>& roles, bool is_leader) {
    CollOpState& s = task.state;
    s.phase = kPhaseDown;
    s.algo.tree.recv_chunk.assign(task.recv_transports.size(), 0);
    s.algo.tree.send_chunk.assign(task.send_transports.size(), 0);
    s.recv_progress.assign(task.recv_transports.size(), 0);
    s.send_progress.assign(task.send_transports.size(), 0);
    s.recv_done.assign(task.recv_transports.size(), 1);
    s.send_done.assign(task.send_transports.size(), 1);
    if (is_leader && ParentEdge(roles) < 0 && task.reduce_op == ReduceOp::AVG) {
        Utils::ApplyAverage(task.recv_buf, task.elem_count, task.dtype, task.world_size);
    }
    return true;
}

bool Advance(PlanTask& task, const std::vector<int>& roles, bool is_leader) {
    CollOpState& s = task.state;
    while (s.phase == kPhaseUp || s.phase == kPhaseDown) {
        if (s.phase == kPhaseUp && !UpComplete(task, roles, is_leader)) {
            return true;
        }
        if (s.phase == kPhaseDown && !DownComplete(task, roles, is_leader)) {
            return true;
        }
        if (s.phase == kPhaseUp) {
            if (!EnterDown(task, roles, is_leader)) {
                return false;
            }
        } else {
            s.phase = kPhaseDone;
            return true;
        }
        if (!PushRecvs(task, roles, is_leader) || !PushSends(task, roles, is_leader)) {
            return false;
        }
    }
    return true;
}

bool TreeInit(PlanTask& task, const std::vector<int>& roles, bool is_leader) {
    CollOpState& s = task.state;
    s = CollOpState{};
    s.send_progress.assign(task.send_transports.size(), 0);
    s.recv_progress.assign(task.recv_transports.size(), 0);
    s.send_done.assign(task.send_transports.size(), 1);
    s.recv_done.assign(task.recv_transports.size(), 1);
    s.algo.tree.send_chunk.assign(task.send_transports.size(), 0);
    s.algo.tree.recv_chunk.assign(task.recv_transports.size(), 0);

    if (task.func != CollFunc::AllReduce) {
        LOG_ERROR("Tree received unsupported collective {}", Utils::GetCollFuncName(task.func));
        return false;
    }
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

    size_t type_size = TypeSize(task);
    if (task.world_size == 1) {
        if (task.send_buf != task.recv_buf) {
            std::memcpy(task.recv_buf, task.send_buf, task.elem_count * type_size);
        }
        if (task.reduce_op == ReduceOp::AVG) {
            Utils::ApplyAverage(task.recv_buf, task.elem_count, task.dtype, task.world_size);
        }
        s.phase = kPhaseDone;
        return true;
    }
    if (task.send_transports.empty() || task.recv_transports.empty()) {
        LOG_ERROR("Tree received task without transports");
        return false;
    }

    if (is_leader) {
        size_t n_scratch = 0;
        for (int role : roles) {
            if (IsInputRole(role)) {
                ++n_scratch;
            }
        }
        s.temp_buffer.resize(n_scratch * task.chunk_size * type_size);
        if (task.send_buf != task.recv_buf) {
            std::memcpy(task.recv_buf, task.send_buf, task.elem_count * type_size);
        }
    }

    s.phase = kPhaseUp;
    return PushRecvs(task, roles, is_leader) && PushSends(task, roles, is_leader) && Advance(task, roles, is_leader);
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
    if (task.func != CollFunc::AllReduce) {
        LOG_ERROR("Tree received unsupported collective {}", Utils::GetCollFuncName(task.func));
        return false;
    }
    return TreeInit(task, channel_roles_[static_cast<size_t>(task.channel_id)], IsLeader());
}

bool TopologyTree::CollectiveStep(PlanTask& task, CollEvent) const noexcept {
    CollOpState& s = task.state;
    const std::vector<int>& roles = channel_roles_[static_cast<size_t>(task.channel_id)];
    if (s.phase == kPhaseUnstarted && !TreeInit(task, roles, IsLeader())) {
        return false;
    }
    if (s.phase == kPhaseDone) {
        return true;
    }
    if (!PushRecvs(task, roles, IsLeader()) || !PushSends(task, roles, IsLeader())) {
        return false;
    }
    return Advance(task, roles, IsLeader());
}

bool TopologyTree::CollectiveDone(const PlanTask& task) const {
    return task.state.phase == kPhaseDone;
}
