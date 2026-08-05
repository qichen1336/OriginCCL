#include <algorithm>
#include <thread>
#include <chrono>
#include <mutex>
#include <atomic>
#include <cstring>
#include "communicator.h"
#include "logger.h"
#include "utils.h"
#include "bootstrap.h"
#include "transport_tcp.h"
#include "topology_ring.h"

Communicator::Communicator() {}

Communicator::~Communicator() {
    Finalize();
}

bool Communicator::Init(const CommConfig& cfg) {
    config = cfg;

    topology = std::make_shared<TopologyRing>();
    if (!topology->Init(config.rank, config.world_size)) {
        LOG_ERROR("Rank {}: Failed to init Topology", config.rank);
        return false;
    }

    LOG_INFO("Rank {}: Init Communicator world_size = {}, transport = TCP, topology = Ring", config.rank,
             config.world_size);

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

    if (!InitTransports(all_nodes)) {
        LOG_ERROR("Rank {}: Failed to init transports for all nodes", config.rank);
        return false;
    }

    LOG_INFO("Rank {}: Communicator init successfully", config.rank);
    return true;
}

void Communicator::Finalize() {
    for (auto& pair : transports) {
        pair.second->Close();
    }
    transports.clear();
    LOG_INFO("Rank {}: Communicator finalized", config.rank);
}

std::shared_ptr<Transport> Communicator::GetTransport(int peer_rank) const {
    auto it = transports.find(peer_rank);
    if (it != transports.end()) {
        return it->second;
    } else {
        return nullptr;
    }
}

bool Communicator::AllReduce(const void* send_buf, void* recv_buf, size_t count, DataType dtype, ReduceOp op) {
    LOG_DEBUG("Rank {}: AllReduce count {}, dtype {}, op {}", config.rank, count, Utils::GetDataTypeName(dtype),
              Utils::GetReduceOpName(op));
    return topology->AllReduce(*this, send_buf, recv_buf, count, dtype, op);
}

bool Communicator::InitTransports(const std::vector<NodeInfo>& all_nodes) {

    std::vector<int> neighbors = topology->GetNeighbors(config.rank);
    LOG_INFO("Rank {}: Topology {} needs connections to {} neighbors", config.rank, topology->GetName(),
             neighbors.size());

    std::vector<int> connect_peers;

    std::vector<int> accept_peers;
    for (int peer : neighbors) {
        if (topology->ShouldConnect(config.rank, peer)) {
            connect_peers.push_back(peer);
        } else {
            accept_peers.push_back(peer);
        }
    }
    LOG_INFO("Rank {}: will actively connect to {} peers, passively accept {} peers", config.rank, connect_peers.size(),
             accept_peers.size());

    const NodeInfo& my_info = all_nodes[config.rank];

    std::shared_ptr<Transport> listen_transport;
    if (!accept_peers.empty()) {
        listen_transport = std::make_shared<TransportTCP>();
        if (!listen_transport || !listen_transport->Listen(my_info.data_port)) {
            LOG_ERROR("Rank {}: Failed to create listen transport on port {}", config.rank, my_info.data_port);
            return false;
        }
        LOG_INFO("Rank {}: Listening on port {} for {} accept peers", config.rank, my_info.data_port,
                 accept_peers.size());
    }

    std::atomic<bool> error_occurred(false);
    std::thread connect_thread([&]() {
        if (!connect_peers.empty()) {
            if (!ConnectActivePeers(all_nodes, connect_peers, error_occurred)) {
                error_occurred = true;
            }
        }
    });
    std::thread accept_thread([&]() {
        if (!accept_peers.empty() && listen_transport) {
            if (!AcceptPassivePeers(listen_transport, accept_peers, error_occurred)) {
                error_occurred = true;
            }
        }
    });
    connect_thread.join();
    accept_thread.join();
    if (error_occurred) {
        return false;
    }

    for (int peer : connect_peers) {
        if (!SendOrFail(transports[peer].get(), &config.rank, sizeof(int), peer, "InitTransports")) {
            return false;
        }
    }

    LOG_INFO("Rank {}: All transports are init, with {} connect + {} accept = {} connections", config.rank,
             connect_peers.size(), accept_peers.size(), transports.size());
    return true;
}

bool Communicator::ConnectActivePeers(const std::vector<NodeInfo>& all_nodes, const std::vector<int>& connect_peers,
                                      std::atomic<bool>& error_occured) {
    std::mutex transport_mutex;
    std::vector<std::thread> threads;

    for (int peer : connect_peers) {
        threads.emplace_back([&, peer]() {
            if (error_occured) {
                return;
            }

            const auto& peer_info = all_nodes[peer];
            auto transport = std::make_shared<TransportTCP>();
            if (!transport) {
                LOG_ERROR("Rank {}: Failed to create transport for connecting to rank {}", config.rank, peer);
                error_occured = true;
                return;
            }

            bool connected = false;
            for (int retry = 0; retry < 30; ++retry) {
                if (transport->Connect(peer_info.ip_addr, peer_info.data_port)) {
                    connected = true;
                    break;
                }

                LOG_DEBUG("Rank {}: Failed to connect to rank {}, retry {}/30", config.rank, peer, retry);
                std::this_thread::sleep_for(std::chrono::microseconds(200));
            }

            if (!connected) {
                LOG_ERROR("Rank {}: Failed to connect to rank {}:{} after 30 reties", config.rank, peer_info.ip_addr,
                          peer_info.data_port);
                error_occured = true;
                return;
            }

            {
                std::lock_guard<std::mutex> lock(transport_mutex);
                transports[peer] = transport;
            }
        });
    }

    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }

    return !error_occured;
}

bool Communicator::AcceptPassivePeers(const std::shared_ptr<Transport>& listen_transport,
                                      const std::vector<int>& accept_peers, std::atomic<bool>& error_occured) {
    std::mutex transports_mutex;
    std::vector<std::thread> threads;

    for (size_t i = 0; i < accept_peers.size(); ++i) {
        threads.emplace_back([&, i]() {
            if (error_occured) {
                return;
            }

            auto transport = listen_transport->CreateAcceptedConnection();
            if (!transport) {
                LOG_ERROR("Rank {}: Failed to accept connection for {}/{}", config.rank, i + 1, accept_peers.size());
                error_occured = true;
                return;
            }

            int peer_rank = -1;
            if (!RecvOrFail(transport.get(), &peer_rank, sizeof(int), -1, "AcceptPassivePeers")) {
                error_occured = true;
                return;
            }

            {
                std::lock_guard<std::mutex> lock(transports_mutex);
                transports[peer_rank] = transport;
            }
        });
    }

    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }

    return !error_occured;
}

bool Communicator::SendOrFail(Transport* transport, const void* data, size_t size, int peer, const char* context) {
    if (!transport->Send(data, size)) {
        LOG_ERROR("Rank {}: {} - Failed to send to rank {}", config.rank, context, peer);
        return false;
    }
    return true;
}

bool Communicator::RecvOrFail(Transport* transport, void* data, size_t size, int peer, const char* context) {
    if (!transport->Recv(data, size)) {
        LOG_ERROR("Rank {}: {} - Failed to recv from rank {}", config.rank, context, peer);
        return false;
    }
    LOG_INFO("Rank {}: {} - Recv {} from rank {}", config.rank, context, size, peer);
    return true;
}
