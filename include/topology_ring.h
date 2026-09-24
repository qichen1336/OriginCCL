#pragma once

#include "topology.h"

class TopologyRing : public Topology {
public:
    TopologyRing() {
        topo_name = "Ring";
    }
    ~TopologyRing() override = default;

    void FillChannels(std::vector<Channel>& channels) const override;

    bool CollectiveInit(PlanTask& task) const noexcept override;
    bool CollectiveStep(PlanTask& task, CollEvent event) const noexcept override;
    bool CollectiveDone(const PlanTask& task) const override;

    bool AllreduceInit(PlanTask& task) const noexcept;
    bool AllreduceStep(PlanTask& task, CollEvent event) const noexcept;

    bool BroadcastInit(PlanTask& task) const noexcept;
    bool BroadcastStep(PlanTask& task, CollEvent event) const noexcept;

    bool AllGatherInit(PlanTask& task) const noexcept;
    bool AllGatherStep(PlanTask& task, CollEvent event) const noexcept;

    bool ReduceInit(PlanTask& task) const noexcept;
    bool ReduceStep(PlanTask& task, CollEvent event) const noexcept;

    bool ReduceScatterInit(PlanTask& task) const noexcept;
    bool ReduceScatterStep(PlanTask& task, CollEvent event) const noexcept;

    int GetPrevRank(int r) const;
    int GetNextRank(int r) const;
};
