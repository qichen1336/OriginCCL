#pragma once

#include "types.h"

class Communicator;

class Planner {
public:
    bool Plan(Communicator& comm, const CollTask& task, CollPlan& plan) const;

private:
    bool PrepareP2p(Communicator& comm, int peer, bool is_send) const;
};
