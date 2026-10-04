#pragma once

#include <vector>
#include "types.h"

class Communicator;

class Planner {
public:
    static constexpr int kP2pSendChannel = 0;
    static constexpr int kP2pRecvChannel = 1;

    void SortTasks(std::vector<CollTask>& tasks, int rank, int world_size) const;
    bool Plan(Communicator& comm, const std::vector<CollTask>& tasks, CollPlan& plan) const;

    bool ConnectP2p(Communicator& comm, int peer) const;
    bool AcceptP2pOne(Communicator& comm, int peer) const;

private:
    bool PlanCollective(Communicator& comm, const CollTask& task, CollPlan& plan) const;
    bool PlanP2pRound(Communicator& comm, const std::vector<CollTask>& tasks, int round, CollPlan& plan) const;
    bool PrepareRound(Communicator& comm, const std::vector<CollTask>& tasks, int round) const;
    bool ValidateP2p(const Communicator& comm, const CollTask& task) const;
    bool P2pUsable(const Communicator& comm, int peer) const;
};
