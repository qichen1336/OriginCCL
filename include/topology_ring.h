#pragma once

#include <memory>
#include <vector>

#include "topology.h"

class Transport;

class TopologyRing : public Topology {
public:
    TopologyRing() {
        topo_name = "Ring";
    }
    ~TopologyRing() override = default;

    std::vector<int> GetNeighbors(int r) const override;
    bool ShouldConnect(int my_rank, int peer_rank) const override;

    bool AllReduce(Communicator& comm, const void* send_buf, void* recv_buf, size_t count, DataType dtype,
                   ReduceOp op) override;

    int GetPrevRank(int r) const;
    int GetNextRank(int r) const;

private:
    struct RingTransports {
        std::shared_ptr<Transport> prev;
        std::shared_ptr<Transport> next;
        int prev_rank;
        int next_rank;
    };

private:
    bool GetRingTransports(Communicator& comm, RingTransports& rt, int world_size);
    bool AllReduceTwoRanks(Communicator& comm, const void* send_buf, void* recv_buf, size_t count, DataType dtype,
                           ReduceOp op);
    bool AllReduceReduceScatter(void* recv_buf, size_t count, DataType dtype, ReduceOp op, RingTransports& rt);
    bool AllReduceAllGather(void* recv_buf, size_t count, DataType dtype, RingTransports& rt);
};
