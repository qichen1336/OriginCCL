#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstring>
#include <limits>
#include <poll.h>
#include <thread>
#include "planner.h"
#include "communicator.h"
#include "channel.h"
#include "occl_config.h"
#include "topology/topology.h"
#include "utils.h"
#include "logger.h"
#include "transport/transport_tcp.h"
#include "transport/transport_shm.h"
#include "transport/transport_rdma_zc.h"

namespace {

bool IsP2p(CollFunc func) {
    return func == CollFunc::Send || func == CollFunc::Recv;
}

int P2pRoundOf(CollFunc func, int rank, int peer, int world_size) {
    const int forward =
        (func == CollFunc::Send) ? (peer - rank + world_size) % world_size : (rank - peer + world_size) % world_size;
    return world_size - forward;
}

int EffectiveOp(const CollTask& task) {
    return (task.func == CollFunc::AllReduce || task.func == CollFunc::Reduce || task.func == CollFunc::ReduceScatter)
               ? static_cast<int>(task.op)
               : 0;
}

int EffectiveRoot(const CollTask& task) {
    return (task.func == CollFunc::Broadcast || task.func == CollFunc::Reduce) ? task.root : 0;
}

} // namespace

void Planner::SortTasks(std::vector<CollTask>& tasks, int rank, int world_size) const {
    std::vector<CollTask> collectives;
    std::vector<CollTask> p2p;
    for (const CollTask& task : tasks) {
        (IsP2p(task.func) ? p2p : collectives).push_back(task);
    }
    auto less = [](const CollTask& a, const CollTask& b) {
        if (a.func != b.func) {
            return a.func < b.func;
        }
        const size_t a_bytes = a.count * Utils::GetDataTypeSize(a.dtype);
        const size_t b_bytes = b.count * Utils::GetDataTypeSize(b.dtype);
        if (a_bytes != b_bytes) {
            return a_bytes < b_bytes;
        }
        if (a.dtype != b.dtype) {
            return a.dtype < b.dtype;
        }
        if (EffectiveOp(a) != EffectiveOp(b)) {
            return EffectiveOp(a) < EffectiveOp(b);
        }
        return EffectiveRoot(a) < EffectiveRoot(b);
    };
    std::stable_sort(collectives.begin(), collectives.end(), less);
    std::stable_sort(p2p.begin(), p2p.end(), [rank, world_size](const CollTask& a, const CollTask& b) {
        const int a_round = P2pRoundOf(a.func, rank, a.peer, world_size);
        const int b_round = P2pRoundOf(b.func, rank, b.peer, world_size);
        if (a_round != b_round) {
            return a_round < b_round;
        }
        return a.func < b.func;
    });
    tasks.clear();
    tasks.insert(tasks.end(), collectives.begin(), collectives.end());
    tasks.insert(tasks.end(), p2p.begin(), p2p.end());
}

bool Planner::Plan(Communicator& comm, const std::vector<CollTask>& tasks, CollPlan& plan) const {
    return PlanCollectives(comm, tasks, plan);
}

bool Planner::PlanCollectives(Communicator& comm, const std::vector<CollTask>& tasks, CollPlan& plan) const {
    plan = CollPlan(comm.GetNChannels());
    int first_channel = 0;
    for (const CollTask& task : tasks) {
        if (IsP2p(task.func)) {
            if (!ValidateP2p(comm, task)) {
                return false;
            }
            continue;
        }
        int n_used = 0;
        if (!PlanCollective(comm, task, plan, first_channel, n_used)) {
            return false;
        }
        first_channel = (first_channel + n_used) % comm.GetNChannels();
    }
    return true;
}

bool Planner::PlanRound(Communicator& comm, const std::vector<CollTask>& tasks, int round, CollPlan& plan) const {
    const int rank = comm.GetRank();
    const int world_size = comm.GetWorldSize();
    std::vector<CollTask> sends;
    std::vector<CollTask> recvs;
    for (const CollTask& task : tasks) {
        if (!IsP2p(task.func) || P2pRoundOf(task.func, rank, task.peer, world_size) != round) {
            continue;
        }
        (task.func == CollFunc::Send ? sends : recvs).push_back(task);
    }
    if (sends.empty() && recvs.empty()) {
        return true;
    }
    if (!PrepareRound(comm, tasks, round)) {
        return false;
    }
    plan = CollPlan(kP2pChannelCount);
    auto append = [&](const CollTask& task, int channel_id) {
        PlanTask plantask;
        plantask.func = task.func;
        plantask.send_buf = task.send_buf;
        plantask.recv_buf = task.recv_buf;
        plantask.elem_count = task.count;
        plantask.dtype = task.dtype;
        plantask.peer = task.peer;
        plantask.channel_id = channel_id;
        plantask.rank = rank;
        plantask.world_size = world_size;
        plantask.topology = comm.p2p_topology_;
        plantask.topology->FillTransports(comm.GetChannel(channel_id), plantask);
        plan.channels[static_cast<size_t>(channel_id)].tasks.push_back(std::move(plantask));
    };
    for (const CollTask& task : sends) {
        append(task, kP2pSendChannel);
    }
    for (const CollTask& task : recvs) {
        append(task, kP2pRecvChannel);
    }
    return true;
}

bool Planner::PlanCollective(Communicator& comm, const CollTask& task, CollPlan& plan, int first_channel,
                             int& n_used_out) const {
    size_t type_size = Utils::GetDataTypeSize(task.dtype);
    size_t total_bytes = task.count * type_size;
    int max_channels = std::max(comm.GetNChannels(), 1);

    const bool use_tree =
        task.func == CollFunc::AllReduce && total_bytes / OcclConfig::kChunkBytes < OcclConfig::kTreeThresholdChunks;
    std::shared_ptr<Topology> topology = use_tree ? comm.GetTreeTopology() : comm.GetRingTopology();
    const char* func_name = Utils::GetCollFuncName(task.func);
    LOG_DEBUG("Rank {}: {} uses the {} topology for {} bytes", comm.GetRank(), func_name, use_tree ? "tree" : "ring",
              total_bytes);

    size_t unit_bytes = (use_tree || task.func == CollFunc::AllReduce)
                            ? OcclConfig::kChunkBytes * static_cast<size_t>(comm.GetWorldSize())
                            : OcclConfig::kChunkBytes;

    size_t units_total = total_bytes / unit_bytes;
    size_t rem_bytes = total_bytes % unit_bytes;
    int n_used = 1;
    if (units_total > 0) {
        n_used = static_cast<int>(std::min(units_total, static_cast<size_t>(max_channels)));
    }

    size_t base_units = units_total / static_cast<size_t>(n_used);
    size_t rem_units = units_total % static_cast<size_t>(n_used);

    size_t offset = 0;
    for (int c = 0; c < n_used; ++c) {
        size_t channel_units = base_units + (static_cast<size_t>(c) < rem_units ? 1 : 0);
        size_t channel_bytes = channel_units * unit_bytes;
        if (c == n_used - 1) {
            channel_bytes += rem_bytes;
        }
        size_t elem_count = channel_bytes / type_size;
        const int channel_id = (first_channel + c) % max_channels;
        ChannelPlan& channel = plan.channels[static_cast<size_t>(channel_id)];
        Channel& comm_channel = comm.GetChannel(channel_id);
        PlanTask plantask;
        plantask.func = task.func;
        plantask.topology = topology;
        plantask.send_buf = task.send_buf ? static_cast<const char*>(task.send_buf) + offset * type_size : nullptr;
        plantask.recv_buf = task.recv_buf ? static_cast<char*>(task.recv_buf) + offset * type_size : nullptr;
        plantask.elem_count = elem_count;
        plantask.rank_stride = task.count;
        plantask.root = task.root;
        plantask.dtype = task.dtype;
        plantask.reduce_op = task.op;
        plantask.channel_id = comm_channel.id;
        plantask.rank = comm.GetRank();
        plantask.world_size = comm.GetWorldSize();
        plantask.chunk_size = use_tree ? OcclConfig::kChunkBytes / type_size
                                       : (elem_count + static_cast<size_t>(plantask.world_size) - 1) /
                                             static_cast<size_t>(plantask.world_size);
        plantask.topology->FillTransports(comm_channel, plantask);
        channel.tasks.push_back(std::move(plantask));
        offset += elem_count;
    }
    n_used_out = n_used;
    return true;
}

bool Planner::PrepareRound(Communicator& comm, const std::vector<CollTask>& tasks, int round) const {
    const int rank = comm.GetRank();
    const int world_size = comm.GetWorldSize();
    std::vector<int> send_peers;
    std::vector<int> recv_peers;
    for (const CollTask& task : tasks) {
        if (!IsP2p(task.func) || P2pRoundOf(task.func, rank, task.peer, world_size) != round) {
            continue;
        }
        if (!ValidateP2p(comm, task)) {
            return false;
        }
        if (task.count == 0) {
            continue;
        }
        (task.func == CollFunc::Send ? send_peers : recv_peers).push_back(task.peer);
    }
    auto dedup = [](std::vector<int>& peers) {
        std::sort(peers.begin(), peers.end());
        peers.erase(std::unique(peers.begin(), peers.end()), peers.end());
    };
    dedup(send_peers);
    dedup(recv_peers);
    if (!send_peers.empty() && !recv_peers.empty()) {
        std::atomic<bool> error(false);
        std::thread connect_thread([&]() {
            for (int peer : send_peers) {
                if (error) {
                    return;
                }
                if (!ConnectP2p(comm, peer)) {
                    error = true;
                }
            }
        });
        std::thread accept_thread([&]() {
            for (int peer : recv_peers) {
                if (error) {
                    return;
                }
                if (!AcceptP2pOne(comm, peer)) {
                    error = true;
                }
            }
        });
        connect_thread.join();
        accept_thread.join();
        return !error;
    }
    for (int peer : send_peers) {
        if (!ConnectP2p(comm, peer)) {
            return false;
        }
    }
    for (int peer : recv_peers) {
        if (!AcceptP2pOne(comm, peer)) {
            return false;
        }
    }
    return true;
}

bool Planner::ValidateP2p(const Communicator& comm, const CollTask& task) const {
    const size_t type_size = Utils::GetDataTypeSize(task.dtype);
    if (task.peer < 0 || task.peer >= comm.GetWorldSize() || task.peer == comm.GetRank()) {
        LOG_ERROR("Rank {}: Invalid P2P peer {}", comm.GetRank(), task.peer);
        return false;
    }
    const bool is_send = task.func == CollFunc::Send;
    if (task.count != 0 && (!(is_send ? task.send_buf : task.recv_buf) || type_size == 0 ||
                            task.count > std::numeric_limits<size_t>::max() / type_size)) {
        LOG_ERROR("Rank {}: Invalid P2P buffer, dtype or count", comm.GetRank());
        return false;
    }
    return true;
}

bool Planner::P2pUsable(const Communicator& comm, int peer) const {
    const auto& local_node = comm.all_nodes_[static_cast<size_t>(comm.GetRank())];
    const auto& peer_node = comm.all_nodes_[static_cast<size_t>(peer)];
    const bool same_machine = local_node.hostname == peer_node.hostname;
    const bool use_shm = same_machine && comm.use_shm_;
    const bool use_rdma = !use_shm && (!same_machine || comm.rdma_ready_);
    if (use_rdma && (local_node.p2p_rdma_port == 0 || peer_node.p2p_rdma_port == 0)) {
        LOG_ERROR("Rank {}: P2P peer {} requires RDMA_ZC but an endpoint is unavailable", comm.GetRank(), peer);
        return false;
    }
    return true;
}

bool Planner::ConnectP2p(Communicator& comm, int peer) const {
    if (!P2pUsable(comm, peer)) {
        return false;
    }
    Channel& channel = comm.GetChannel(kP2pSendChannel);
    auto& connector = channel.send_p2p[static_cast<size_t>(peer)];
    if (connector.transport) {
        return true;
    }
    const auto& local_node = comm.all_nodes_[static_cast<size_t>(comm.GetRank())];
    const auto& peer_node = comm.all_nodes_[static_cast<size_t>(peer)];
    const bool same_machine = local_node.hostname == peer_node.hostname;
    const bool use_shm = same_machine && comm.use_shm_;
    const bool use_rdma = !use_shm && (!same_machine || comm.rdma_ready_);
    std::shared_ptr<Transport> transport;
    std::string addr;
    uint16_t port = 0;
    if (use_shm) {
        transport = std::make_shared<TransportShm>();
        addr = fmt::format("/tmp/originccl/{}-{}.sock.p2p", comm.config.unique_id.port, peer);
    } else if (use_rdma) {
        transport = std::make_shared<TransportRDMAZc>();
        addr = peer_node.rdma_addr;
        port = peer_node.p2p_rdma_port;
    } else {
        transport = std::make_shared<TransportTCP>();
        addr = peer_node.ip_addr;
        port = peer_node.p2p_port;
    }
    transport->SetDirection(TransportDirection::Send);
    const int rank = comm.GetRank();
    if (!transport->Connect(addr, port) || !transport->Send(&rank, sizeof(rank))) {
        LOG_ERROR("Rank {}: Failed to connect P2P send to peer {}", rank, peer);
        return false;
    }
    connector.transport = std::move(transport);
    return true;
}

bool Planner::AcceptP2pOne(Communicator& comm, int peer) const {
    if (!P2pUsable(comm, peer)) {
        return false;
    }
    Channel& channel = comm.GetChannel(kP2pRecvChannel);
    auto& connector = channel.recv_p2p[static_cast<size_t>(peer)];
    if (connector.transport) {
        return true;
    }
    std::vector<pollfd> poll_fds;
    for (const auto& listener : comm.p2p_listeners_) {
        poll_fds.push_back(pollfd{listener->GetFd(), static_cast<short>(listener->GetPollEvents()), 0});
    }
    while (!connector.transport) {
        int ready;
        do {
            ready = poll(poll_fds.data(), poll_fds.size(), -1);
        } while (ready < 0 && errno == EINTR);
        if (ready < 0) {
            LOG_ERROR("Rank {}: P2P accept poll failed: {}", comm.GetRank(), std::strerror(errno));
            return false;
        }
        for (size_t index = 0; index < poll_fds.size(); ++index) {
            if (poll_fds[index].revents == 0) {
                continue;
            }
            if (poll_fds[index].revents & (POLLERR | POLLHUP | POLLNVAL)) {
                LOG_ERROR("Rank {}: P2P listener failed", comm.GetRank());
                return false;
            }
            auto transport = comm.p2p_listeners_[index]->Accept();
            int source = -1;
            if (!transport || !transport->Recv(&source, sizeof(source))) {
                LOG_ERROR("Rank {}: P2P accept handshake failed", comm.GetRank());
                return false;
            }
            transport->SetDirection(TransportDirection::Receive);
            channel.recv_p2p[static_cast<size_t>(source)].transport = std::move(transport);
        }
    }
    return true;
}
