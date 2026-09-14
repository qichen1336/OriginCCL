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

    // The topology owns the collective algorithm and treats the task's CollOpState as
    // its cursor; the executor owns how to wait for socket readiness. The executor
    // registers the task's send/recv fds (both read and write) and calls AllreduceStep()
    // with the event that fired. It never blocks and advances only the matching transfer.
    //
    // Init and Step report failure through their return value — that is the only error
    // channel, and the executor must abandon the collective as soon as it sees false.
    // Done therefore means "completed successfully"; a failed task never becomes done.
    virtual bool AllreduceInit(PlanTask& task) const = 0;
    virtual bool AllreduceStep(PlanTask& task, CollEvent event) const = 0;
    virtual bool AllreduceDone(const PlanTask& task) const = 0;

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
