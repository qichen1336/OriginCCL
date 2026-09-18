#pragma once

#include <string>
#include <vector>
#include <memory>
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

    virtual bool AllreduceInit(PlanTask& task) const noexcept = 0;
    virtual bool AllreduceStep(PlanTask& task, CollEvent event) const noexcept = 0;
    virtual bool AllreduceDone(const PlanTask& task) const = 0;

    const char* GetName() const {
        return topo_name.c_str();
    }

protected:
    int rank = -1;
    int world_size = 0;
    int n_channels = 0;
    std::string topo_name;
};
