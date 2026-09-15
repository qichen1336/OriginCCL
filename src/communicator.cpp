#include <thread>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <atomic>
#include <algorithm>
#include "communicator.h"
#include "logger.h"
#include "utils.h"
#include "bootstrap.h"
#include "transport_tcp.h"
#include "transport_shm.h"
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
// The executor is a compile-time choice: exactly one OCCL_EXECUTOR_* macro is defined by
// the build, so there is a single concrete type per library build and no runtime branch.
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

// Default number of channels when config.n_channels is unset or non-positive.
constexpr int kDefaultChannelCount = 4;
} // namespace

Communicator::Communicator() {}

Communicator::~Communicator() {
    Finalize();
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

    LOG_INFO("Rank {}: Init Communicator world_size = {}, n_channels = {}, transport = auto (SHM on the same host, TCP "
             "across hosts), topology = {}, executor = {}",
             config.rank, config.world_size, n_channels, topology->GetName(), kExecutorName);

    const char* disable_shm = std::getenv("OCCL_DISABLE_SHM");
    shm_disabled = (disable_shm != nullptr && std::strcmp(disable_shm, "1") == 0);
    if (shm_disabled) {
        LOG_INFO("Rank {}: OCCL_DISABLE_SHM=1, every edge uses TCP", config.rank);
    }

    if (config.world_size <= 1) {
        local_rank = 0;
        local_size = 1;
        local_ranks = {0};
        is_single_machine = true;
        LOG_INFO("Rank {}: Single-rank communicator, skip data-plane connections", config.rank);
        return true;
    }

    // This listener is kept for the whole initialization: it carries the TCP edges that are
    // accepted passively, and while it stays bound nothing else can take this rank's data
    // port, which is what makes the shared-memory rendezvous name derived from it unique.
    auto data_listener = std::make_shared<TransportTCP>();
    if (!data_listener->Listen(0)) {
        LOG_ERROR("Rank {}: Failed to listen to get the available port", config.rank);
        return false;
    }

    uint16_t data_port = data_listener->GetListenPort();
    LOG_INFO("Rank {}: Will use port {} for data plane", config.rank, data_port);

    Bootstrap bootstrap;
    std::vector<NodeInfo> all_nodes;
    if (!bootstrap.Run(config, data_port, all_nodes)) {
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

    if (!InitChannels(all_nodes, data_listener)) {
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
                                const std::shared_ptr<Transport>& tcp_listener) {
    const NodeInfo& my_info = all_nodes[config.rank];
    const std::string& my_hostname = my_info.hostname;

    std::vector<ChannelEdge> connect_edges;
    std::vector<ChannelEdge> accept_tcp_edges;
    std::vector<ChannelEdge> accept_shm_edges;

    for (auto& channel : channels) {
        const ChannelEdge edges[2] = {{channel.id, channel.ring.next, true, false},
                                      {channel.id, channel.ring.prev, false, false}};
        for (const ChannelEdge& edge : edges) {
            ChannelEdge classified = edge;
            // An edge can carry a shared-memory ring only when both ranks run on this
            // machine, and only when the whole job left that enabled: the switch is read
            // per rank, so a job that sets it inconsistently is a configuration error
            // rather than something to negotiate here.
            classified.local = !shm_disabled && all_nodes[edge.peer].hostname == my_hostname;
            LOG_INFO("Rank {}: channel {} {} edge to rank {} uses transport={}", config.rank, edge.channel_id,
                     edge.is_send ? "send" : "recv", edge.peer, classified.local ? "SHM" : "TCP");
            if (config.rank < edge.peer) {
                connect_edges.push_back(classified);
            } else if (classified.local) {
                accept_shm_edges.push_back(classified);
            } else {
                accept_tcp_edges.push_back(classified);
            }
        }
    }

    LOG_INFO("Rank {}: will actively connect {} edges, passively accept {} TCP and {} SHM edges", config.rank,
             connect_edges.size(), accept_tcp_edges.size(), accept_shm_edges.size());

    std::shared_ptr<Transport> shm_listener;
    if (!accept_shm_edges.empty()) {
        shm_listener = std::make_shared<TransportSHM>();
        if (!shm_listener->Listen(my_info.data_port)) {
            LOG_ERROR("Rank {}: Failed to listen for shared-memory edges on port {}", config.rank, my_info.data_port);
            return false;
        }
    }

    std::atomic<bool> error_occurred(false);
    std::thread connect_thread([&]() {
        if (!connect_edges.empty()) {
            if (!ConnectActiveEdges(all_nodes, connect_edges, error_occurred)) {
                error_occurred = true;
            }
        }
    });
    std::thread accept_tcp_thread([&]() {
        if (!accept_tcp_edges.empty() && !AcceptPassiveEdges(tcp_listener, accept_tcp_edges.size(), error_occurred)) {
            error_occurred = true;
        }
    });
    std::thread accept_shm_thread([&]() {
        if (!accept_shm_edges.empty() && !AcceptPassiveEdges(shm_listener, accept_shm_edges.size(), error_occurred)) {
            error_occurred = true;
        }
    });
    connect_thread.join();
    accept_tcp_thread.join();
    accept_shm_thread.join();
    if (shm_listener) {
        shm_listener->Close();
    }
    if (tcp_listener) {
        // Nothing else accepted passively; the port lease is no longer needed either.
        tcp_listener->Close();
    }
    if (error_occurred) {
        return false;
    }

    LOG_INFO("Rank {}: All channel connections are init", config.rank);
    return true;
}

bool Communicator::ConnectActiveEdges(const std::vector<NodeInfo>& all_nodes, const std::vector<ChannelEdge>& edges,
                                      std::atomic<bool>& error_occurred) {
    std::mutex connector_mutex;
    std::vector<std::thread> threads;

    for (const ChannelEdge& edge : edges) {
        threads.emplace_back([&, edge]() {
            if (error_occurred) {
                return;
            }

            const auto& peer_info = all_nodes[edge.peer];
            // A same-host edge carries a shared-memory ring, whose Connect() only needs the
            // peer's data port: the rendezvous endpoint is host-local, named after it.
            std::shared_ptr<Transport> transport;
            if (edge.local) {
                transport = std::make_shared<TransportSHM>();
            } else {
                transport = std::make_shared<TransportTCP>();
            }
            bool connected = false;
            for (int retry = 0; retry < 30; ++retry) {
                if (transport->Connect(peer_info.ip_addr, peer_info.data_port)) {
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
                return;
            }

            ConnHandshake handshake;
            handshake.rank = config.rank;
            handshake.channel_id = edge.channel_id;
            handshake.is_send = edge.is_send ? 1 : 0;
            if (!transport->Send(&handshake, sizeof(handshake))) {
                LOG_ERROR("Rank {}: Failed to send handshake to rank {} channel {}", config.rank, edge.peer,
                          edge.channel_id);
                error_occurred = true;
                return;
            }

            {
                std::lock_guard<std::mutex> lock(connector_mutex);
                Connector* connector = FindConnector(edge.channel_id, edge.peer, edge.is_send);
                if (!connector) {
                    LOG_ERROR("Rank {}: No connector for peer {} channel {} is_send {}", config.rank, edge.peer,
                              edge.channel_id, edge.is_send);
                    error_occurred = true;
                    return;
                }
                connector->transport = transport;
            }
        });
    }

    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }

    return !error_occurred;
}

bool Communicator::AcceptPassiveEdges(const std::shared_ptr<Transport>& listen_transport, size_t accept_count,
                                      std::atomic<bool>& error_occurred) {
    for (size_t i = 0; i < accept_count; ++i) {
        if (error_occurred) {
            return false;
        }

        auto transport = listen_transport->Accept();
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
        connector->transport = transport;
    }

    return true;
}
