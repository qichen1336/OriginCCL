#pragma once

#include <vector>
#include <memory>
#include <atomic>
#include "types.h"
#include "channel.h"
#include "planner.h"
#include "executor/executor.h"

class Transport;
class Topology;

class Communicator {
public:
    Communicator();
    ~Communicator();

    bool GetUniqueId(UniqueId& unique_id);

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

    int GetLocalRank() const {
        return local_rank;
    }
    int GetLocalSize() const {
        return local_size;
    }
    const std::vector<int>& GetLocalRanks() const {
        return local_ranks;
    }
    bool IsSingleMachine() const {
        return is_single_machine;
    }
    int GetMachineIndex() const {
        return machine_index;
    }
    int GetMachineCount() const {
        return machine_count;
    }
    const std::vector<int>& GetMachineLeaders() const {
        return machine_leaders;
    }

    std::shared_ptr<Topology> GetRingTopology() const {
        return ring_topology_;
    }
    std::shared_ptr<Topology> GetTreeTopology() const {
        return tree_topology_;
    }

    Channel& GetChannel(int channel_id);
    const Channel& GetChannel(int channel_id) const;

    bool AllReduce(const void* send_buf, void* recv_buf, size_t count, DataType dtype, ReduceOp op);
    bool Broadcast(void* buffer, size_t count, DataType dtype, int root);
    bool AllGather(const void* send_buf, void* recv_buf, size_t count, DataType dtype);
    bool Reduce(const void* send_buf, void* recv_buf, size_t count, DataType dtype, ReduceOp op, int root);
    bool ReduceScatter(const void* send_buf, void* recv_buf, size_t count, DataType dtype, ReduceOp op);
    bool Send(const void* buffer, size_t count, DataType dtype, int peer);
    bool Recv(void* buffer, size_t count, DataType dtype, int peer);

private:
    friend class Planner;

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

    bool InitChannels(const std::vector<NodeInfo>& all_nodes, const std::vector<std::shared_ptr<Transport>>& listeners,
                      bool use_shm, bool rdma_ready);
    bool ConnectActiveEdges(const std::vector<NodeInfo>& all_nodes, const std::vector<ChannelEdge>& edges, bool use_shm,
                            bool rdma_ready, std::atomic<bool>& error_occurred);
    bool AcceptPassiveEdges(const std::vector<std::shared_ptr<Transport>>& listeners, size_t accept_count,
                            std::atomic<bool>& error_occurred);
    Connector* FindConnector(int channel_id, int peer, bool is_send);

private:
    CommConfig config;
    std::shared_ptr<Topology> ring_topology_;
    std::shared_ptr<Topology> tree_topology_;
    std::shared_ptr<Topology> p2p_topology_;
    std::vector<NodeInfo> all_nodes_;
    std::vector<std::shared_ptr<Transport>> p2p_listeners_;
    bool use_shm_ = false;
    bool rdma_ready_ = false;
    std::vector<Channel> channels;
    Planner planner;
    std::unique_ptr<Executor> executor;
    int bootstrap_listen_fd = -1;
    int local_rank = 0;
    int local_size = 1;
    std::vector<int> local_ranks;
    bool is_single_machine = true;
    int machine_index = 0;
    int machine_count = 0;
    std::vector<int> machine_leaders;
};
