#pragma once

#include <vector>
#include <map>
#include <memory>
#include <atomic>
#include "types.h"

class Transport;
class Topology;

class Communicator {
public:
    Communicator();
    ~Communicator();

    bool Init(const CommConfig& cfg);
    void Finalize();

    int GetRank() const {
        return config.rank;
    }
    int GetWorldSize() const {
        return config.world_size;
    }
    std::shared_ptr<Transport> GetTransport(int peer_rank) const;

    const std::map<int, std::shared_ptr<Transport>>& GetTransports() const {
        return transports;
    }

    bool AllReduce(const void* send_buf, void* recv_buf, size_t count, DataType dtype, ReduceOp op);

private:
    bool InitTransports(const std::vector<NodeInfo>& all_nodes);
    bool ConnectActivePeers(const std::vector<NodeInfo>& all_nodes, const std::vector<int>& connect_peers,
                            std::atomic<bool>& error_occured);
    bool AcceptPassivePeers(const std::shared_ptr<Transport>& listen_transport, const std::vector<int>& accept_peers,
                            std::atomic<bool>& error_occured);

    bool SendOrFail(Transport* transport, const void* data, size_t size, int peer, const char* context);
    bool RecvOrFail(Transport* transport, void* data, size_t size, int peer, const char* context);

private:
    CommConfig config;

    std::shared_ptr<Topology> topology;
    std::map<int, std::shared_ptr<Transport>> transports;
};
