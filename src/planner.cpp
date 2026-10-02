#include <algorithm>
#include <cerrno>
#include <cstring>
#include <limits>
#include <poll.h>
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

bool Planner::Plan(Communicator& comm, const CollTask& task, CollPlan& plan) const {
    size_t type_size = Utils::GetDataTypeSize(task.dtype);
    if (task.func == CollFunc::Send || task.func == CollFunc::Recv) {
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
        if (task.count != 0 && !PrepareP2p(comm, task.peer, is_send)) {
            return false;
        }
        plan = CollPlan(1);
        PlanTask plantask;
        plantask.func = task.func;
        plantask.send_buf = task.send_buf;
        plantask.recv_buf = task.recv_buf;
        plantask.elem_count = task.count;
        plantask.dtype = task.dtype;
        plantask.peer = task.peer;
        plantask.rank = comm.GetRank();
        plantask.world_size = comm.GetWorldSize();
        plantask.topology = comm.p2p_topology_;
        plantask.topology->FillTransports(comm.GetChannel(0), plantask);
        plan.channels[0].tasks.push_back(std::move(plantask));
        return true;
    }
    size_t total_bytes = task.count * type_size;
    int max_channels = std::max(comm.GetNChannels(), 1);

    const bool use_tree = (task.func == CollFunc::AllReduce || task.func == CollFunc::ReduceScatter ||
                           task.func == CollFunc::AllGather) &&
                          total_bytes / OcclConfig::kChunkBytes < OcclConfig::kTreeThresholdChunks;
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

    plan = CollPlan(n_used);

    size_t offset = 0;
    for (int c = 0; c < n_used; ++c) {
        size_t channel_units = base_units + (static_cast<size_t>(c) < rem_units ? 1 : 0);
        size_t channel_bytes = channel_units * unit_bytes;
        if (c == n_used - 1) {
            channel_bytes += rem_bytes;
        }
        size_t elem_count = channel_bytes / type_size;
        ChannelPlan& channel = plan.channels[static_cast<size_t>(c)];
        Channel& comm_channel = comm.GetChannel(c);
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
    return true;
}

bool Planner::PrepareP2p(Communicator& comm, int peer, bool is_send) const {
    Channel& channel = comm.GetChannel(0);
    auto& connector = (is_send ? channel.send_p2p : channel.recv_p2p)[peer];
    if (connector.transport) {
        return true;
    }
    const auto& local_node = comm.all_nodes_[comm.GetRank()];
    const auto& peer_node = comm.all_nodes_[peer];
    const bool same_machine = local_node.hostname == peer_node.hostname;
    const bool use_shm = same_machine && comm.use_shm_;
    const bool use_rdma = !use_shm && (!same_machine || comm.rdma_ready_);
    if (use_rdma && (local_node.p2p_rdma_port == 0 || peer_node.p2p_rdma_port == 0)) {
        LOG_ERROR("Rank {}: P2P peer {} requires RDMA_ZC but an endpoint is unavailable", comm.GetRank(), peer);
        return false;
    }
    if (is_send) {
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
            channel.recv_p2p[source].transport = std::move(transport);
        }
    }
    return true;
}
