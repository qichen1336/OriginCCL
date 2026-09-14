#pragma once

#include "topology.h"

class TopologyRing : public Topology {
public:
    TopologyRing() {
        topo_name = "Ring";
    }
    ~TopologyRing() override = default;

    void FillChannels(std::vector<Channel>& channels) const override;

    bool AllreduceInit(PlanTask& task) const override;
    bool AllreduceStep(PlanTask& task, CollEvent event) const override;
    bool AllreduceDone(const PlanTask& task) const override;

    int GetPrevRank(int r) const;
    int GetNextRank(int r) const;
};
