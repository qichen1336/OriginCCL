#include <thread>
#include <atomic>
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <poll.h>
#include <unistd.h>
#include "communicator.h"
#include "logger.h"
#include "utils.h"
#include "bootstrap.h"
#include "transport/transport_tcp.h"
#include "transport/transport_shm.h"
#include "transport/transport_rdma.h"
#include "transport/transport_rdma_zc.h"
#include "topology/topology_ring.h"
#include "topology/topology_tree.h"
#include "topology/topology_p2p.h"
#include "executor/epoll_executor.h"
#include "executor/multi_thread_executor.h"

namespace {
constexpr int kDefaultChannelCount = 4;

constexpr int kConnectRetryCount = 5;
constexpr int kConnectRetryIntervalMs = 100;

constexpr const char* kRendezvousDir = "/tmp/originccl";

std::unique_ptr<Executor> MakeExecutor(int local_size) {
    const long cores = sysconf(_SC_NPROCESSORS_ONLN);
    if (cores >= local_size) {
        LOG_INFO("Using the multi-thread executor ({} cores for {} local ranks)", cores, local_size);
        return std::make_unique<MultiThreadExecutor>();
    }
    LOG_INFO("Using the epoll executor ({} cores for {} local ranks)", cores, local_size);
    return std::make_unique<EpollExecutor>();
}

bool SharedMemoryEnabled() {
    const char* value = std::getenv("OCCL_DISABLE_SHM");
    return value == nullptr || std::strcmp(value, "1") != 0;
}

bool RdmaEnabled() {
    const char* value = std::getenv("OCCL_DISABLE_RDMA");
    return value == nullptr || std::strcmp(value, "1") != 0;
}

std::string RendezvousPath(uint16_t port, int rank) {
    return std::string(kRendezvousDir) + "/" + std::to_string(port) + "-" + std::to_string(rank) + ".sock";
}
} // namespace

Communicator::Communicator() {}

Communicator::~Communicator() {
    Finalize();
}

bool Communicator::GetUniqueId(UniqueId& unique_id) {
    if (bootstrap_listen_fd >= 0) {
        LOG_ERROR("Bootstrap listener is already bound");
        return false;
    }

    uint16_t port = 0;
    bootstrap_listen_fd = Utils::CreateListenSocket(0, &port);
    if (bootstrap_listen_fd < 0) {
        LOG_ERROR("Failed to bind the bootstrap listener");
        return false;
    }

    const std::string ip_addr = Utils::GetLocalIPAddress();
    std::snprintf(unique_id.ip_addr, sizeof(unique_id.ip_addr), "%s", ip_addr.c_str());
    unique_id.port = port;
    LOG_INFO("Rank 0: Bootstrap listener is on {}:{}", ip_addr, port);
    return true;
}

bool Communicator::Init(const CommConfig& cfg) {
    config = cfg;

    ring_topology_ = std::make_shared<TopologyRing>();
    tree_topology_ = std::make_shared<TopologyTree>();
    p2p_topology_ = std::make_shared<TopologyP2p>();
    int n_channels = config.n_channels > 0 ? config.n_channels : kDefaultChannelCount;
    if (!ring_topology_->Init(config.rank, config.world_size, n_channels) ||
        !tree_topology_->Init(config.rank, config.world_size, n_channels) ||
        !p2p_topology_->Init(config.rank, config.world_size, n_channels)) {
        LOG_ERROR("Rank {}: Failed to init Topology", config.rank);
        return false;
    }

    channels.resize(static_cast<size_t>(n_channels));
    for (size_t i = 0; i < channels.size(); ++i) {
        Channel& channel = channels[i];
        channel.id = static_cast<int>(i);
        channel.send.resize(static_cast<size_t>(config.world_size));
        channel.recv.resize(static_cast<size_t>(config.world_size));
        for (int peer = 0; peer < config.world_size; ++peer) {
            Connector& send = channel.send[static_cast<size_t>(peer)];
            send.peer = peer;
            send.channel_id = channel.id;
            send.is_send = true;
            Connector& recv = channel.recv[static_cast<size_t>(peer)];
            recv.peer = peer;
            recv.channel_id = channel.id;
            recv.is_send = false;
        }
    }

    p2p_topology_->FillChannels(*this, channels);
    const bool use_shm = SharedMemoryEnabled();
    use_shm_ = use_shm;
    LOG_INFO("Rank {}: Init Communicator world_size = {}, n_channels = {}, topology = {} + {}", config.rank,
             config.world_size, n_channels, ring_topology_->GetName(), tree_topology_->GetName());

    if (config.world_size <= 1) {
        local_rank = 0;
        local_size = 1;
        local_ranks = {0};
        is_single_machine = true;
        LOG_INFO("Rank {}: Single-rank communicator, skip data-plane connections", config.rank);
        Utils::PinProcessToCpu(local_rank);
        executor = MakeExecutor(local_size);
        return true;
    }

    auto tcp_listener = std::make_shared<TransportTCP>();
    if (!tcp_listener->Listen("", 0)) {
        LOG_ERROR("Rank {}: Failed to create the data-plane listener", config.rank);
        return false;
    }
    const uint16_t data_port = tcp_listener->GetListenPort();
    LOG_INFO("Rank {}: Will use port {} for data plane", config.rank, data_port);

    NodeInfo local_node(config.rank, Utils::GetLocalIPAddress(), data_port, config.get_hostname());

    // The RDMA endpoint is announced through the bootstrap NodeInfo so every rank can decide
    // the network transport with the same cluster-wide view: all ranks must be able to do
    // RDMA, otherwise the whole network data plane stays on TCP.
    std::shared_ptr<TransportRDMA> rdma_listener;
    if (RdmaEnabled()) {
        std::string rdma_addr;
        auto candidate = std::make_shared<TransportRDMA>();
        if (TransportRDMA::Probe(rdma_addr) && candidate->Listen(rdma_addr, 0)) {
            local_node.rdma_addr = rdma_addr;
            local_node.rdma_port = candidate->GetListenPort();
            rdma_listener = std::move(candidate);
            LOG_INFO("Rank {}: RDMA listener on {}:{}", config.rank, rdma_addr, local_node.rdma_port);
        } else {
            LOG_INFO("Rank {}: No usable RDMA device, the network data plane will use TCP", config.rank);
        }
    } else {
        LOG_INFO("Rank {}: RDMA is disabled by OCCL_DISABLE_RDMA", config.rank);
    }

    std::shared_ptr<TransportShm> shm_listener;
    if (use_shm) {
        std::error_code error;
        std::filesystem::create_directories(kRendezvousDir, error);
        if (error) {
            LOG_ERROR("Rank {}: Failed to create the shared-memory rendezvous directory {}: {}", config.rank,
                      kRendezvousDir, error.message());
            return false;
        }
        const std::string rendezvous = RendezvousPath(config.unique_id.port, config.rank);
        shm_listener = std::make_shared<TransportShm>();
        if (!shm_listener->Listen(rendezvous, 0)) {
            LOG_ERROR("Rank {}: Failed to create the shared-memory rendezvous listener on {}", config.rank, rendezvous);
            return false;
        }
        LOG_INFO("Rank {}: Shared-memory rendezvous listener on {}", config.rank, rendezvous);
    }

    auto p2p_tcp_listener = std::make_shared<TransportTCP>();
    if (!p2p_tcp_listener->Listen("", 0)) {
        LOG_ERROR("Rank {}: Failed to create P2P TCP listener", config.rank);
        return false;
    }
    local_node.p2p_port = p2p_tcp_listener->GetListenPort();
    p2p_listeners_.push_back(p2p_tcp_listener);
    if (use_shm) {
        auto p2p_shm_listener = std::make_shared<TransportShm>();
        if (!p2p_shm_listener->Listen(RendezvousPath(config.unique_id.port, config.rank) + ".p2p", 0)) {
            LOG_ERROR("Rank {}: Failed to create P2P shared-memory listener", config.rank);
            return false;
        }
        p2p_listeners_.push_back(p2p_shm_listener);
    }
    if (rdma_listener) {
        auto p2p_rdma_listener = std::make_shared<TransportRDMAZc>();
        if (p2p_rdma_listener->Listen(local_node.rdma_addr, 0)) {
            local_node.p2p_rdma_port = p2p_rdma_listener->GetListenPort();
            p2p_listeners_.push_back(p2p_rdma_listener);
        } else {
            LOG_WARN("Rank {}: P2P RDMA listener unavailable", config.rank);
        }
    }

    Bootstrap bootstrap;
    auto& all_nodes = all_nodes_;
    if (!bootstrap.Run(config, local_node, bootstrap_listen_fd, all_nodes)) {
        LOG_ERROR("Rank {}: Failed to run bootstrap", config.rank);
        return false;
    }
    LOG_INFO("Rank {}: Bootstrap is ready, get {} nodes info", config.rank, all_nodes.size());

    const bool rdma_ready =
        rdma_listener != nullptr &&
        std::all_of(all_nodes.begin(), all_nodes.end(), [](const NodeInfo& node) { return node.rdma_port != 0; });
    rdma_ready_ = rdma_ready;
    std::vector<std::shared_ptr<Transport>> listeners;
    if (use_shm) {
        listeners.push_back(shm_listener);
    }
    if (rdma_ready) {
        listeners.push_back(rdma_listener);
        tcp_listener->Close();
    } else {
        listeners.push_back(tcp_listener);
        if (rdma_listener) {
            rdma_listener->Close();
            rdma_listener.reset();
        }
    }
    LOG_INFO("Rank {}: Network data plane uses {}", config.rank, rdma_ready ? "RDMA" : "TCP");

    const std::string& my_hostname = all_nodes[config.rank].hostname;
    local_ranks.clear();
    for (const auto& node : all_nodes) {
        if (node.hostname == my_hostname) {
            local_ranks.push_back(node.rank);
        }
    }
    std::sort(local_ranks.begin(), local_ranks.end());
    local_size = static_cast<int>(local_ranks.size());
    auto self = std::find(local_ranks.begin(), local_ranks.end(), config.rank);
    local_rank = static_cast<int>(std::distance(local_ranks.begin(), self));
    is_single_machine = (local_size == config.world_size);
    LOG_INFO("Rank {}: local_rank = {}, local_size = {}, single_machine = {}, hostname = {}", config.rank, local_rank,
             local_size, is_single_machine, my_hostname);

    std::vector<std::string> machine_hosts;
    for (const auto& node : all_nodes) {
        if (std::find(machine_hosts.begin(), machine_hosts.end(), node.hostname) == machine_hosts.end()) {
            machine_hosts.push_back(node.hostname);
        }
    }
    machine_count = static_cast<int>(machine_hosts.size());
    auto machine_it = std::find(machine_hosts.begin(), machine_hosts.end(), my_hostname);
    machine_index = static_cast<int>(std::distance(machine_hosts.begin(), machine_it));
    machine_leaders.assign(machine_hosts.size(), -1);
    for (const auto& node : all_nodes) {
        auto it = std::find(machine_hosts.begin(), machine_hosts.end(), node.hostname);
        const int m = static_cast<int>(std::distance(machine_hosts.begin(), it));
        if (machine_leaders[static_cast<size_t>(m)] < 0) {
            machine_leaders[static_cast<size_t>(m)] = node.rank;
        }
    }

    ring_topology_->FillChannels(*this, channels);
    tree_topology_->FillChannels(*this, channels);

    executor = MakeExecutor(local_size);
    Utils::PinProcessToCpu(local_rank);

    if (!InitChannels(all_nodes, listeners, use_shm, rdma_ready)) {
        LOG_ERROR("Rank {}: Failed to init channel connections", config.rank);
        return false;
    }

    LOG_INFO("Rank {}: Communicator init successfully", config.rank);
    return true;
}
void Communicator::Finalize() {
    if (executor) {
        executor->Shutdown();
    }

    if (bootstrap_listen_fd >= 0) {
        close(bootstrap_listen_fd);
        bootstrap_listen_fd = -1;
    }

    for (auto& channel : channels) {
        for (auto& conn : channel.send) {
            if (conn.transport) {
                conn.transport->Close();
                conn.transport.reset();
            }
        }
        for (auto& conn : channel.recv) {
            if (conn.transport) {
                conn.transport->Close();
                conn.transport.reset();
            }
        }
        for (auto* connectors : {&channel.send_p2p, &channel.recv_p2p}) {
            for (auto& conn : *connectors) {
                if (conn.transport) {
                    conn.transport->Close();
                    conn.transport.reset();
                }
            }
        }
    }
    channels.clear();
    pending_tasks.clear();
    group_depth = 0;
    for (const auto& listener : p2p_listeners_) {
        listener->Close();
    }
    p2p_listeners_.clear();
    all_nodes_.clear();
    LOG_INFO("Rank {}: Communicator finalized", config.rank);
}

Channel& Communicator::GetChannel(int channel_id) {
    return channels.at(static_cast<size_t>(channel_id));
}

const Channel& Communicator::GetChannel(int channel_id) const {
    return channels.at(static_cast<size_t>(channel_id));
}

void Communicator::GroupStart() {
    ++group_depth;
}

bool Communicator::GroupEnd() {
    if (group_depth <= 0) {
        LOG_ERROR("Rank {}: GroupEnd without GroupStart", config.rank);
        return false;
    }
    if (--group_depth > 0) {
        return true;
    }
    return Flush();
}

bool Communicator::Submit(const CollTask& task) {
    const bool is_p2p = task.func == CollFunc::Send || task.func == CollFunc::Recv;
    if (!is_p2p && !planner.ValidateCollectiveTask(task, config.rank, config.world_size)) {
        return false;
    }
    pending_tasks.push_back(task);
    if (group_depth == 0) {
        return Flush();
    }
    return true;
}

bool Communicator::Flush() {
    std::vector<CollTask> tasks;
    tasks.swap(pending_tasks);
    if (!planner.SortTasks(tasks, config.rank, config.world_size)) {
        return false;
    }

    CollPlan plan;
    return planner.Plan(*this, tasks, plan) && executor->Run(plan);
}

bool Communicator::AllReduce(const void* send_buf, void* recv_buf, size_t count, DataType dtype, ReduceOp op) {
    LOG_DEBUG("Rank {}: AllReduce count {}, dtype {}, op {}", config.rank, count, Utils::GetDataTypeName(dtype),
              Utils::GetReduceOpName(op));
    CollTask task;
    task.func = CollFunc::AllReduce;
    task.send_buf = send_buf;
    task.recv_buf = recv_buf;
    task.count = count;
    task.dtype = dtype;
    task.op = op;
    return Submit(task);
}

bool Communicator::Broadcast(void* buffer, size_t count, DataType dtype, int root) {
    CollTask task{CollFunc::Broadcast, buffer, buffer, count, dtype, ReduceOp::SUM, root};
    return Submit(task);
}

bool Communicator::AllGather(const void* send_buf, void* recv_buf, size_t count, DataType dtype) {
    CollTask task{CollFunc::AllGather, send_buf, recv_buf, count, dtype};
    return Submit(task);
}

bool Communicator::Reduce(const void* send_buf, void* recv_buf, size_t count, DataType dtype, ReduceOp op, int root) {
    CollTask task{CollFunc::Reduce, send_buf, recv_buf, count, dtype, op, root};
    return Submit(task);
}

bool Communicator::ReduceScatter(const void* send_buf, void* recv_buf, size_t count, DataType dtype, ReduceOp op) {
    CollTask task{CollFunc::ReduceScatter, send_buf, recv_buf, count, dtype, op};
    return Submit(task);
}

bool Communicator::Send(const void* buffer, size_t count, DataType dtype, int peer) {
    CollTask task{CollFunc::Send, buffer, nullptr, count, dtype};
    task.peer = peer;
    return Submit(task);
}

bool Communicator::Recv(void* buffer, size_t count, DataType dtype, int peer) {
    CollTask task{CollFunc::Recv, nullptr, buffer, count, dtype};
    task.peer = peer;
    return Submit(task);
}

Connector* Communicator::FindConnector(int channel_id, int peer, bool is_send) {
    if (channel_id < 0 || channel_id >= static_cast<int>(channels.size())) {
        return nullptr;
    }
    Channel& channel = channels[static_cast<size_t>(channel_id)];
    if (is_send) {
        return channel.SendConnector(peer);
    }
    return channel.RecvConnector(peer);
}

bool Communicator::InitChannels(const std::vector<NodeInfo>& all_nodes,
                                const std::vector<std::shared_ptr<Transport>>& listeners, bool use_shm,
                                bool rdma_ready) {
    std::vector<ChannelEdge> connect_edges;
    std::vector<ChannelEdge> accept_edges;

    for (auto& channel : channels) {
        std::vector<TopoEdge> peers;
        std::vector<TopoEdge> ring_peers;
        std::vector<TopoEdge> tree_peers;
        ring_topology_->FillPeers(channel, ring_peers);
        tree_topology_->FillPeers(channel, tree_peers);
        peers.insert(peers.end(), ring_peers.begin(), ring_peers.end());
        peers.insert(peers.end(), tree_peers.begin(), tree_peers.end());
        std::sort(peers.begin(), peers.end(), [](const TopoEdge& a, const TopoEdge& b) {
            return a.peer != b.peer ? a.peer < b.peer : a.is_send < b.is_send;
        });
        peers.erase(std::unique(peers.begin(), peers.end(),
                                [](const TopoEdge& a, const TopoEdge& b) {
                                    return a.peer == b.peer && a.is_send == b.is_send;
                                }),
                    peers.end());

        for (const TopoEdge& peer : peers) {
            if (peer.peer < 0 || peer.peer >= config.world_size || peer.peer == config.rank) {
                continue;
            }
            ChannelEdge edge{channel.id, peer.peer, peer.is_send};
            if (config.rank < peer.peer) {
                connect_edges.push_back(edge);
            } else {
                accept_edges.push_back(edge);
            }
        }
    }

    LOG_INFO("Rank {}: will actively connect {} edges, passively accept {} edges", config.rank, connect_edges.size(),
             accept_edges.size());

    std::atomic<bool> error_occurred(false);
    std::thread connect_thread([&]() {
        if (!connect_edges.empty()) {
            if (!ConnectActiveEdges(all_nodes, connect_edges, use_shm, rdma_ready, error_occurred)) {
                error_occurred = true;
            }
        }
    });
    std::thread accept_thread([&]() {
        if (!accept_edges.empty()) {
            if (!AcceptPassiveEdges(listeners, accept_edges.size(), error_occurred)) {
                error_occurred = true;
            }
        }
    });
    connect_thread.join();
    accept_thread.join();
    for (const auto& listener : listeners) {
        listener->Close();
    }
    if (error_occurred) {
        return false;
    }

    LOG_INFO("Rank {}: All channel connections are init", config.rank);
    return true;
}

bool Communicator::ConnectActiveEdges(const std::vector<NodeInfo>& all_nodes, const std::vector<ChannelEdge>& edges,
                                      bool use_shm, bool rdma_ready, std::atomic<bool>& error_occurred) {
    for (const ChannelEdge& edge : edges) {
        if (error_occurred) {
            return false;
        }

        const auto& peer_info = all_nodes[edge.peer];
        const bool local_edge = use_shm && peer_info.hostname == all_nodes[config.rank].hostname;
        std::shared_ptr<Transport> transport;
        std::string addr;
        uint16_t port = 0;
        const char* kind = nullptr;
        if (local_edge) {
            transport = std::make_shared<TransportShm>();
            addr = RendezvousPath(config.unique_id.port, edge.peer);
            kind = "shared memory";
        } else if (rdma_ready) {
            transport = std::make_shared<TransportRDMA>();
            addr = peer_info.rdma_addr;
            port = peer_info.rdma_port;
            kind = "RDMA";
        } else {
            transport = std::make_shared<TransportTCP>();
            addr = peer_info.ip_addr;
            port = peer_info.data_port;
            kind = "TCP";
        }

        transport->SetDirection(edge.is_send ? TransportDirection::Send : TransportDirection::Receive);
        bool connected = false;
        for (int attempt = 1; attempt <= kConnectRetryCount; ++attempt) {
            if (transport->Connect(addr, port)) {
                connected = true;
                break;
            }
            transport->Close();
            if (attempt < kConnectRetryCount) {
                LOG_WARN("Rank {}: Failed to connect to rank {} channel {} over {} ({}:{}) on attempt {}/{}, retrying",
                         config.rank, edge.peer, edge.channel_id, kind, addr, port, attempt, kConnectRetryCount);
                std::this_thread::sleep_for(std::chrono::milliseconds(kConnectRetryIntervalMs));
            }
        }
        if (!connected) {
            LOG_ERROR("Rank {}: Failed to connect to rank {} channel {} over {} ({}:{})", config.rank, edge.peer,
                      edge.channel_id, kind, addr, port);
            error_occurred = true;
            return false;
        }

        ConnHandshake handshake;
        handshake.rank = config.rank;
        handshake.channel_id = edge.channel_id;
        handshake.is_send = edge.is_send ? 1 : 0;
        if (!transport->Send(&handshake, sizeof(handshake))) {
            LOG_ERROR("Rank {}: Failed to send handshake to rank {} channel {}", config.rank, edge.peer,
                      edge.channel_id);
            error_occurred = true;
            return false;
        }

        Connector* connector = FindConnector(edge.channel_id, edge.peer, edge.is_send);
        if (!connector) {
            LOG_ERROR("Rank {}: No connector for peer {} channel {} is_send {}", config.rank, edge.peer,
                      edge.channel_id, edge.is_send);
            error_occurred = true;
            return false;
        }
        connector->transport = transport;
    }

    return true;
}

bool Communicator::AcceptPassiveEdges(const std::vector<std::shared_ptr<Transport>>& listeners, size_t accept_count,
                                      std::atomic<bool>& error_occurred) {
    std::vector<pollfd> poll_fds;
    poll_fds.reserve(listeners.size());
    for (const auto& listener : listeners) {
        poll_fds.push_back(pollfd{listener->GetFd(), static_cast<short>(listener->GetPollEvents()), 0});
    }

    for (size_t i = 0; i < accept_count; ++i) {
        if (error_occurred) {
            return false;
        }

        int ready = 0;
        do {
            ready = poll(poll_fds.data(), poll_fds.size(), -1);
        } while (ready < 0 && errno == EINTR);
        if (ready < 0) {
            LOG_ERROR("Rank {}: Failed to wait for incoming channel connections: {}", config.rank, strerror(errno));
            return false;
        }

        std::shared_ptr<Transport> transport;
        for (size_t j = 0; j < poll_fds.size(); ++j) {
            if (poll_fds[j].revents == 0) {
                continue;
            }
            if ((poll_fds[j].revents & (POLLERR | POLLHUP | POLLNVAL)) != 0) {
                LOG_ERROR("Rank {}: Listener {} failed with revents {:#x}", config.rank, j, poll_fds[j].revents);
                return false;
            }
            transport = listeners[j]->Accept();
            break;
        }
        if (!transport) {
            LOG_ERROR("Rank {}: Failed to accept connection {}/{}", config.rank, i + 1, accept_count);
            return false;
        }

        ConnHandshake handshake;
        if (!transport->Recv(&handshake, sizeof(handshake))) {
            LOG_ERROR("Rank {}: Failed to recv handshake on accept {}", config.rank, i);
            return false;
        }

        bool peer_is_send = handshake.is_send != 0;
        bool local_is_send = !peer_is_send;
        Connector* connector = FindConnector(handshake.channel_id, handshake.rank, local_is_send);
        if (!connector) {
            LOG_ERROR("Rank {}: No connector for handshake peer {} channel {} local_is_send {}", config.rank,
                      handshake.rank, handshake.channel_id, local_is_send);
            return false;
        }
        transport->SetDirection(local_is_send ? TransportDirection::Send : TransportDirection::Receive);
        connector->transport = transport;
    }

    return true;
}
