#include <algorithm>
#include <cstring>
#include "topology_tree.h"
#include "communicator.h"
#include "transport/transport.h"
#include "utils.h"
#include "logger.h"

namespace {

constexpr int kPhaseUnstarted = 0;
constexpr int kPhaseStarUp = 1;
constexpr int kPhaseDbtUpRecv = 2;
constexpr int kPhaseDbtUpSend = 3;
constexpr int kPhaseDbtDownRecv = 4;
constexpr int kPhaseDbtDownSend = 5;
constexpr int kPhaseStarDown = 6;
constexpr int kPhaseDone = 7;

constexpr int kRoleStar = 0;
constexpr int kRoleT0Down = 1;
constexpr int kRoleT0Up = 2;

size_t TypeSize(const PlanTask& task) {
    return Utils::GetDataTypeSize(task.dtype);
}

size_t TotalElems(const PlanTask& task) {
    if (task.func == CollFunc::AllReduce) {
        return task.elem_count;
    }
    return static_cast<size_t>(task.world_size) * task.rank_stride;
}

size_t ChunkElems(const PlanTask& task, size_t total, size_t chunk) {
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
        if (role == kRoleStar || role == kRoleT0Down) {
            ++n;
        }
    }
    return n;
}

void SeedAccumulator(PlanTask& task) {
    size_t type_size = TypeSize(task);
    char* acc = Accumulator(task);
    if (task.func == CollFunc::AllReduce) {
        std::memcpy(acc, task.send_buf, task.elem_count * type_size);
    } else if (task.func == CollFunc::ReduceScatter) {
        std::memcpy(acc, task.send_buf, TotalElems(task) * type_size);
    } else {
        std::memset(acc, 0, TotalElems(task) * type_size);
        std::memcpy(acc + static_cast<size_t>(task.rank) * task.rank_stride * type_size, task.send_buf,
                    task.elem_count * type_size);
    }
}

void ExtractResult(PlanTask& task) {
    size_t type_size = TypeSize(task);
    char* acc = Accumulator(task);
    if (task.func == CollFunc::AllReduce) {
        std::memcpy(task.recv_buf, acc, task.elem_count * type_size);
        if (task.reduce_op == ReduceOp::AVG) {
            Utils::ApplyAverage(task.recv_buf, task.elem_count, task.dtype, task.world_size);
        }
    } else if (task.func == CollFunc::ReduceScatter) {
        std::memcpy(task.recv_buf, acc + static_cast<size_t>(task.rank) * task.rank_stride * type_size,
                    task.rank_stride * type_size);
        if (task.reduce_op == ReduceOp::AVG) {
            Utils::ApplyAverage(task.recv_buf, task.rank_stride, task.dtype, task.world_size);
        }
    } else {
        std::memcpy(task.recv_buf, acc, TotalElems(task) * type_size);
    }
}

void CombineRole(PlanTask& task, const std::vector<int>& roles, int role, size_t chunk) {
    size_t total = TotalElems(task);
    size_t nelems = ChunkElems(task, total, chunk);
    size_t type_size = TypeSize(task);
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
    return phase == kPhaseStarUp || phase == kPhaseDbtUpRecv;
}

void SetActive(PlanTask& task, const std::vector<int>& roles, uint32_t send_mask, uint32_t recv_mask) {
    CollOpState& s = task.state;
    for (size_t i = 0; i < task.send_transports.size(); ++i) {
        bool active = (send_mask & Bit(roles[i])) != 0;
        if (active) {
            if (s.send_done[i]) {
                s.send_done[i] = 0;
                s.send_progress[i] = 0;
            }
        } else {
            s.send_done[i] = 1;
        }
    }
    for (size_t i = 0; i < task.recv_transports.size(); ++i) {
        bool active = (recv_mask & Bit(roles[i])) != 0;
        if (active) {
            if (s.recv_done[i]) {
                s.recv_done[i] = 0;
                s.recv_progress[i] = 0;
            }
        } else {
            s.recv_done[i] = 1;
        }
    }
}

bool Activate(PlanTask& task, const std::vector<int>& roles, uint32_t send_mask, uint32_t recv_mask) {
    SetActive(task, roles, send_mask, recv_mask);
    CollOpState& s = task.state;
    size_t total = TotalElems(task);
    size_t nelems = ChunkElems(task, total, s.algo.tree.step);
    size_t nbytes = nelems * TypeSize(task);
    char* acc_chunk = Accumulator(task) + s.algo.tree.step * task.chunk_size * TypeSize(task);
    return PushSend(task, acc_chunk, nbytes) && PushRecv(task, acc_chunk, nbytes, RecvPerConn(s.phase));
}

bool StartStarUp(PlanTask& task, const std::vector<int>& roles, bool is_leader) {
    task.state.phase = kPhaseStarUp;
    return Activate(task, roles, is_leader ? 0 : Bit(kRoleStar), is_leader ? Bit(kRoleStar) : 0);
}

bool StartDbtUpRecv(PlanTask& task, const std::vector<int>& roles) {
    task.state.phase = kPhaseDbtUpRecv;
    return Activate(task, roles, 0, Bit(kRoleT0Down));
}

bool StartDbtUpSend(PlanTask& task, const std::vector<int>& roles) {
    task.state.phase = kPhaseDbtUpSend;
    return Activate(task, roles, Bit(kRoleT0Up), 0);
}

bool StartDbtDownRecv(PlanTask& task, const std::vector<int>& roles) {
    task.state.phase = kPhaseDbtDownRecv;
    return Activate(task, roles, 0, Bit(kRoleT0Up));
}

bool StartDbtDownSend(PlanTask& task, const std::vector<int>& roles) {
    task.state.phase = kPhaseDbtDownSend;
    return Activate(task, roles, Bit(kRoleT0Down), 0);
}

bool StartStarDown(PlanTask& task, const std::vector<int>& roles, bool is_leader) {
    task.state.phase = kPhaseStarDown;
    return Activate(task, roles, is_leader ? Bit(kRoleStar) : 0, is_leader ? 0 : Bit(kRoleStar));
}

bool AdvanceChunk(PlanTask& task, const std::vector<int>& roles, bool is_leader) {
    CollOpState& s = task.state;
    size_t total = TotalElems(task);
    if (static_cast<size_t>(s.algo.tree.step + 1) * task.chunk_size >= total) {
        s.phase = kPhaseDone;
        ExtractResult(task);
        return true;
    }
    ++s.algo.tree.step;
    return StartStarUp(task, roles, is_leader);
}

bool Complete(PlanTask& task, const std::vector<int>& roles, bool is_leader) {
    CollOpState& s = task.state;
    while (s.phase != kPhaseDone) {
        switch (s.phase) {
        case kPhaseStarUp:
            if (is_leader) {
                if (!SideDone(s.recv_done)) {
                    return true;
                }
                CombineRole(task, roles, kRoleStar, s.algo.tree.step);
                if (!StartDbtUpRecv(task, roles)) {
                    return false;
                }
            } else {
                if (!SideDone(s.send_done)) {
                    return true;
                }
                if (!StartStarDown(task, roles, is_leader)) {
                    return false;
                }
            }
            continue;
        case kPhaseDbtUpRecv:
            if (!SideDone(s.recv_done)) {
                return true;
            }
            CombineRole(task, roles, kRoleT0Down, s.algo.tree.step);
            if (!StartDbtUpSend(task, roles)) {
                return false;
            }
            continue;
        case kPhaseDbtUpSend:
            if (!SideDone(s.send_done)) {
                return true;
            }
            if (!StartDbtDownRecv(task, roles)) {
                return false;
            }
            continue;
        case kPhaseDbtDownRecv:
            if (!SideDone(s.recv_done)) {
                return true;
            }
            if (!StartDbtDownSend(task, roles)) {
                return false;
            }
            continue;
        case kPhaseDbtDownSend:
            if (!SideDone(s.send_done)) {
                return true;
            }
            if (!StartStarDown(task, roles, is_leader)) {
                return false;
            }
            continue;
        case kPhaseStarDown:
            if (is_leader) {
                if (!SideDone(s.send_done)) {
                    return true;
                }
            } else {
                if (!SideDone(s.recv_done)) {
                    return true;
                }
            }
            if (!AdvanceChunk(task, roles, is_leader)) {
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
    size_t total = TotalElems(task);
    size_t nelems = ChunkElems(task, total, s.algo.tree.step);
    size_t nbytes = nelems * TypeSize(task);
    char* acc_chunk = Accumulator(task) + s.algo.tree.step * task.chunk_size * TypeSize(task);
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

    return StartStarUp(task, roles, is_leader) && Complete(task, roles, is_leader);
}

} // namespace

void TopologyTree::FillChannels(const Communicator& comm, std::vector<Channel>& channels) {
    is_leader_ = comm.GetLocalRank() == 0;
    const std::vector<int>& local_ranks = comm.GetLocalRanks();
    int machine_index = comm.GetMachineIndex();
    int machine_count = comm.GetMachineCount();
    const std::vector<int>& machine_leaders = comm.GetMachineLeaders();

    std::vector<int> star_peers;
    std::vector<int> children;
    int parent = -1;
    roles_.clear();
    if (is_leader_) {
        int parent_machine = (machine_index == 0) ? -1 : (machine_index - 1) / 2;
        if (parent_machine >= 0) {
            parent = machine_leaders[static_cast<size_t>(parent_machine)];
        }
        int c0 = 2 * machine_index + 1;
        int c1 = 2 * machine_index + 2;
        if (c0 < machine_count) {
            children.push_back(machine_leaders[static_cast<size_t>(c0)]);
        }
        if (c1 < machine_count) {
            children.push_back(machine_leaders[static_cast<size_t>(c1)]);
        }

        for (size_t i = 1; i < local_ranks.size(); ++i) {
            star_peers.push_back(local_ranks[i]);
            roles_.push_back(kRoleStar);
        }
        roles_.insert(roles_.end(), children.size(), kRoleT0Down);
        if (parent >= 0) {
            roles_.push_back(kRoleT0Up);
        }
    } else {
        star_peers.push_back(local_ranks[0]);
        roles_.push_back(kRoleStar);
    }

    for (Channel& channel : channels) {
        channel.tree.parent = parent;
        channel.tree.children = children;
        channel.tree.star_peers = star_peers;
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

void TopologyTree::FillTransports(Channel& channel, std::vector<std::shared_ptr<Transport>>& send_out,
                                  std::vector<std::shared_ptr<Transport>>& recv_out) const {
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
        return TreeInit(task, roles_, IsLeader());
    default:
        LOG_ERROR("Tree received unsupported collective");
        return false;
    }
}

bool TopologyTree::CollectiveStep(PlanTask& task, CollEvent event) const noexcept {
    CollOpState& s = task.state;
    if (s.phase == kPhaseUnstarted && !TreeInit(task, roles_, IsLeader())) {
        return false;
    }
    if (s.phase == kPhaseDone) {
        return true;
    }
    if (!PushStep(task, event)) {
        return false;
    }
    return Complete(task, roles_, IsLeader());
}

bool TopologyTree::CollectiveDone(const PlanTask& task) const {
    return task.state.phase == kPhaseDone;
}
