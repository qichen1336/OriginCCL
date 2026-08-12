#pragma once

#include <vector>
#include <memory>
#include <atomic>
#include "types.h"
#include "channel.h"
#include "planner.h"

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
    int GetNChannels() const {
        return static_cast<int>(channels.size());
    }

    Channel& GetChannel(int channel_id);
    const Channel& GetChannel(int channel_id) const;

    bool AllReduce(const void* send_buf, void* recv_buf, size_t count, DataType dtype, ReduceOp op);

private:
    struct ConnHandshake {
        int rank = -1;
        int channel_id = -1;
        int is_send = 0;
    };

    struct ChannelEdge {
        int channel_id = -1;
        int peer = -1;
        bool is_send = false;
    };

    bool InitChannels(const std::vector<NodeInfo>& all_nodes);
    bool ConnectActiveEdges(const std::vector<NodeInfo>& all_nodes, const std::vector<ChannelEdge>& edges,
                            std::atomic<bool>& error_occurred);
    bool AcceptPassiveEdges(const std::shared_ptr<Transport>& listen_transport, size_t accept_count,
                            std::atomic<bool>& error_occurred);
    Connector* FindConnector(int channel_id, int peer, bool is_send);

private:
    CommConfig config;
    std::shared_ptr<Topology> topology;
    std::vector<Channel> channels;
    Planner planner;
};
