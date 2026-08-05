#pragma once

#include <string>
#include <memory>
#include <vector>
#include <cstddef>
#include "types.h"

class Communicator;

class Topology {
public:
    virtual ~Topology() = default;

    virtual bool Init(int r, int ws) {
        rank = r;
        world_size = ws;
        return true;
    }

    virtual std::vector<int> GetNeighbors(int r) const = 0;
    virtual bool ShouldConnect(int my_rank, int peer_rank) const = 0;

    const char* GetName() const {
        return topo_name.c_str();
    }

    virtual bool AllReduce(Communicator& comm, const void* send_buf, void* recv_buf, size_t count, DataType dtype,
                           ReduceOp op) = 0;

protected:
    int rank = -1;
    int world_size = 0;
    std::string topo_name;
};
