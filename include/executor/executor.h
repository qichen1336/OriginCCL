#pragma once

#include "types.h"

// Execution strategy for a CollPlan. The executor owns how to wait for transport
// readiness (per-channel worker threads, a single epoll loop, or polling); the topology
// owns the algorithm and is driven through Topology::Step(). Implementations must be
// Shutdown() before the channel transports are closed.
class Executor {
public:
    virtual ~Executor() = default;

    virtual bool Run(const CollPlan& plan) = 0;
    virtual void Shutdown() = 0;
};
