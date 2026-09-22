#include <thread>
#include <chrono>
#include <atomic>
#include <algorithm>
#include <cerrno>
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
#include "topology_ring.h"

#if defined(OCCL_EXECUTOR_EPOLL)
#include "executor/epoll_executor.h"
#elif defined(OCCL_EXECUTOR_POLLING)
#include "executor/polling_executor.h"
#elif defined(OCCL_EXECUTOR_REACTOR)
#include "executor/reactor_executor.h"
#else
#include "executor/multi_thread_executor.h"
#endif

namespace {
#if defined(OCCL_EXECUTOR_EPOLL)
constexpr const char* kExecutorName = "epoll";
using DefaultExecutor = EpollExecutor;
#elif defined(OCCL_EXECUTOR_POLLING)
constexpr const char* kExecutorName = "polling";
using DefaultExecutor = PollingExecutor;
#elif defined(OCCL_EXECUTOR_REACTOR)
constexpr const char* kExecutorName = "reactor";
using DefaultExecutor = ReactorExecutor;
#else
constexpr const char* kExecutorName = "multi_thread";
using DefaultExecutor = MultiThreadExecutor;
#endif

constexpr int kDefaultChannelCount = 4;

constexpr const char* kRendezvousDir = "/tmp/originccl";

bool SharedMemoryEnabled() {
    const char* value = std::getenv("OCCL_DISABLE_SHM");
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

    topology = std::make_shared<TopologyRing>();
    int n_channels = config.n_channels > 0 ? config.n_channels : kDefaultChannelCount;
    if (!topology->Init(config.rank, config.world_size, n_channels)) {
        LOG_ERROR("Rank {}: Failed to init Topology", config.rank);
        return false;
    }

    channels.resize(static_cast<size_t>(n_channels));
    topology->FillChannels(channels);

    if (!executor) {
        executor = std::make_unique<DefaultExecutor>();
    }

    const bool use_shm = SharedMemoryEnabled();
    LOG_INFO(
        "Rank {}: Init Communicator world_size = {}, n_channels = {}, transport = {}, topology = {}, executor = {}",
        config.rank, config.world_size, n_channels, use_shm ? "shared-memory + TCP" : "TCP", topology->GetName(),
        kExecutorName);

    if (config.world_size <= 1) {
        local_rank = 0;
        local_size = 1;
        local_ranks = {0};
        is_single_machine = true;
        LOG_INFO("Rank {}: Single-rank communicator, skip data-plane connections", config.rank);
        Utils::PinProcessToCpu(local_rank);
        return true;
    }

    auto tcp_listener = std::make_shared<TransportTCP>();
    if (!tcp_listener->Listen(0)) {
        LOG_ERROR("Rank {}: Failed to create the data-plane listener", config.rank);
        return false;
    }
    const uint16_t data_port = tcp_listener->GetListenPort();
    LOG_INFO("Rank {}: Will use port {} for data plane", config.rank, data_port);

    std::vector<std::shared_ptr<Transport>> listeners;
    listeners.push_back(tcp_listener);
    if (use_shm) {
        std::error_code error;
        std::filesystem::create_directories(kRendezvousDir, error);
        if (error) {
            LOG_ERROR("Rank {}: Failed to create the shared-memory rendezvous directory {}: {}", config.rank,
                      kRendezvousDir, error.message());
            return false;
        }
        const std::string rendezvous = RendezvousPath(config.unique_id.port, config.rank);
        auto shm_listener = std::make_shared<TransportShm>();
        if (!shm_listener->ListenPath(rendezvous)) {
            LOG_ERROR("Rank {}: Failed to create the shared-memory rendezvous listener on {}", config.rank, rendezvous);
            return false;
        }
        LOG_INFO("Rank {}: Shared-memory rendezvous listener on {}", config.rank, rendezvous);
        listeners.push_back(shm_listener);
    }

    Bootstrap bootstrap;
    std::vector<NodeInfo> all_nodes;
    if (!bootstrap.Run(config, data_port, bootstrap_listen_fd, all_nodes)) {
        LOG_ERROR("Rank {}: Failed to run bootstrap", config.rank);
        return false;
    }
    LOG_INFO("Rank {}: Bootstrap is ready, get {} nodes info", config.rank, all_nodes.size());

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

    Utils::PinProcessToCpu(local_rank);

    if (!InitChannels(all_nodes, listeners, use_shm)) {
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
    }
    channels.clear();
    LOG_INFO("Rank {}: Communicator finalized", config.rank);
}

Channel& Communicator::GetChannel(int channel_id) {
    return channels.at(static_cast<size_t>(channel_id));
}

const Channel& Communicator::GetChannel(int channel_id) const {
    return channels.at(static_cast<size_t>(channel_id));
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

    CollPlan plan = planner.Plan(*this, task);
    return executor->Run(plan);
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
                                const std::vector<std::shared_ptr<Transport>>& listeners, bool use_shm) {
    std::vector<ChannelEdge> connect_edges;
    std::vector<ChannelEdge> accept_edges;

    for (auto& channel : channels) {
        ChannelEdge send_edge{channel.id, channel.ring.next, true};
        ChannelEdge recv_edge{channel.id, channel.ring.prev, false};
        if (config.rank < send_edge.peer) {
            connect_edges.push_back(send_edge);
        } else {
            accept_edges.push_back(send_edge);
        }
        if (config.rank < recv_edge.peer) {
            connect_edges.push_back(recv_edge);
        } else {
            accept_edges.push_back(recv_edge);
        }
    }

    LOG_INFO("Rank {}: will actively connect {} edges, passively accept {} edges", config.rank, connect_edges.size(),
             accept_edges.size());

    std::atomic<bool> error_occurred(false);
    std::thread connect_thread([&]() {
        if (!connect_edges.empty()) {
            if (!ConnectActiveEdges(all_nodes, connect_edges, use_shm, error_occurred)) {
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
                                      bool use_shm, std::atomic<bool>& error_occurred) {
    for (const ChannelEdge& edge : edges) {
        if (error_occurred) {
            return false;
        }

        const auto& peer_info = all_nodes[edge.peer];
        const bool local_edge = use_shm && peer_info.hostname == all_nodes[config.rank].hostname;
        std::shared_ptr<Transport> transport;
        if (local_edge) {
            auto shm_transport = std::make_shared<TransportShm>();
            shm_transport->SetDirection(edge.is_send ? TransportDirection::Send : TransportDirection::Receive);
            if (!shm_transport->Connect(RendezvousPath(config.unique_id.port, edge.peer), 0)) {
                LOG_ERROR("Rank {}: Failed to connect to rank {} channel {} over shared memory", config.rank, edge.peer,
                          edge.channel_id);
                error_occurred = true;
                return false;
            }
            transport = std::move(shm_transport);
        } else {
            auto tcp_transport = std::make_shared<TransportTCP>();
            bool connected = false;
            for (int retry = 0; retry < 30; ++retry) {
                if (tcp_transport->Connect(peer_info.ip_addr, peer_info.data_port)) {
                    connected = true;
                    break;
                }
                LOG_DEBUG("Rank {}: Failed to connect to rank {} channel {} retry {}/30", config.rank, edge.peer,
                          edge.channel_id, retry);
                std::this_thread::sleep_for(std::chrono::microseconds(200));
            }

            if (!connected) {
                LOG_ERROR("Rank {}: Failed to connect to rank {}:{} after 30 retries", config.rank, peer_info.ip_addr,
                          peer_info.data_port);
                error_occurred = true;
                return false;
            }
            tcp_transport->SetDirection(edge.is_send ? TransportDirection::Send : TransportDirection::Receive);
            transport = std::move(tcp_transport);
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
