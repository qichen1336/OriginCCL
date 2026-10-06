#pragma once

#include <vector>
#include "types.h"

class Communicator;

class Planner {
public:
    static constexpr int kP2pSendChannel = 0;
    static constexpr int kP2pRecvChannel = 1;

    bool ValidateCollectiveTask(const CollTask& task, int rank, int world_size) const;
    bool SortTasks(std::vector<CollTask>& tasks, int rank, int world_size) const;
    bool Plan(Communicator& comm, const std::vector<CollTask>& tasks, CollPlan& plan, bool preempt = false) const;

    bool ConnectP2p(Communicator& comm, int peer) const;
    bool AcceptP2pOne(Communicator& comm, int peer) const;

private:
    bool PlanCollectiveSlices(Communicator& comm, const CollTask& task, std::vector<PlanTask>& slices) const;
    bool PlanP2pRound(Communicator& comm, const std::vector<CollTask>& tasks, int round, CollPlan& plan) const;
    bool PrepareRound(Communicator& comm, const std::vector<CollTask>& sends, const std::vector<CollTask>& recvs) const;
    bool ValidateP2p(const Communicator& comm, const CollTask& task) const;
    bool P2pUsable(const Communicator& comm, int peer) const;
};
