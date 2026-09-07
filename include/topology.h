#pragma once

#include <string>
#include <vector>
#include "channel.h"
#include "types.h"

class Topology {
public:
    virtual ~Topology() = default;

    virtual bool Init(int r, int ws, int n_ch) {
        rank = r;
        world_size = ws;
        n_channels = n_ch;
        return true;
    }

    virtual void FillChannels(std::vector<Channel>& channels) const = 0;

    virtual bool AllReduce(const PlanTask& task) const = 0;

    virtual int DefaultChannelCount() const {
        return 1;
    }

    const char* GetName() const {
        return topo_name.c_str();
    }

protected:
    int rank = -1;
    int world_size = 0;
    int n_channels = 0;
    std::string topo_name;
};
