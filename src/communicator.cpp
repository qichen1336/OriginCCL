#include <thread>
#include <chrono>
#include <mutex>
#include <atomic>
#include "communicator.h"
#include "logger.h"
#include "utils.h"
#include "bootstrap.h"
#include "transport_tcp.h"
#include "topology_ring.h"
#include "multi_thread_executor.h"

Communicator::Communicator() {}

Communicator::~Communicator() {
    Finalize();
}

bool Communicator::Init(const CommConfig& cfg) {
    config = cfg;

    topology = std::make_shared<TopologyRing>();
    int n_channels = config.n_channels > 0 ? config.n_channels : topology->DefaultChannelCount();
    if (!topology->Init(config.rank, config.world_size, n_channels)) {
        LOG_ERROR("Rank {}: Failed to init Topology", config.rank);
        return false;
    }

    channels.resize(static_cast<size_t>(n_channels));
    topology->FillChannels(channels);

    LOG_INFO("Rank {}: Init Communicator world_size = {}, n_channels = {}, transport = TCP, topology = {}", config.rank,
             config.world_size, n_channels, topology->GetName());

    if (config.world_size <= 1) {
        LOG_INFO("Rank {}: Single-rank communicator, skip data-plane connections", config.rank);
        return true;
    }

    auto temp_transport = std::make_shared<TransportTCP>();
    if (!temp_transport->Listen(0)) {
        LOG_ERROR("Rank {}: Failed to listen to get the available port", config.rank);
        return false;
    }

    uint16_t data_port = temp_transport->GetListenPort();
    LOG_INFO("Rank {}: Will use port {} for data plane", config.rank, data_port);
    temp_transport->Close();

    Bootstrap bootstrap;
    std::vector<NodeInfo> all_nodes;
    if (!bootstrap.Run(config, data_port, all_nodes)) {
        LOG_ERROR("Rank {}: Failed to run bootstrap", config.rank);
        return false;
    }
    LOG_INFO("Rank {}: Bootstrap is ready, get {} nodes info", config.rank, all_nodes.size());

    if (!InitChannels(all_nodes)) {
        LOG_ERROR("Rank {}: Failed to init channel connections", config.rank);
        return false;
    }

    LOG_INFO("Rank {}: Communicator init successfully", config.rank);
    return true;
}

void Communicator::Finalize() {
    executor.Shutdown();

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
    return executor.Run(plan);
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

bool Communicator::InitChannels(const std::vector<NodeInfo>& all_nodes) {
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

    const NodeInfo& my_info = all_nodes[config.rank];
    std::shared_ptr<Transport> listen_transport;
    if (!accept_edges.empty()) {
        listen_transport = std::make_shared<TransportTCP>();
        if (!listen_transport->Listen(my_info.data_port)) {
            LOG_ERROR("Rank {}: Failed to create listen transport on port {}", config.rank, my_info.data_port);
            return false;
        }
        LOG_INFO("Rank {}: Listening on port {} for {} accept edges", config.rank, my_info.data_port,
                 accept_edges.size());
    }

    std::atomic<bool> error_occurred(false);
    std::thread connect_thread([&]() {
        if (!connect_edges.empty()) {
            if (!ConnectActiveEdges(all_nodes, connect_edges, error_occurred)) {
                error_occurred = true;
            }
        }
    });
    std::thread accept_thread([&]() {
        if (!accept_edges.empty() && listen_transport) {
            if (!AcceptPassiveEdges(listen_transport, accept_edges.size(), error_occurred)) {
                error_occurred = true;
            }
        }
    });
    connect_thread.join();
    accept_thread.join();
    if (listen_transport) {
        listen_transport->Close();
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
            auto transport = std::make_shared<TransportTCP>();
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
