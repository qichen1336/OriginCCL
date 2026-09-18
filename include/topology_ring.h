#pragma once

#include "topology.h"

class TopologyRing : public Topology {
public:
    TopologyRing() {
        topo_name = "Ring";
    }
    ~TopologyRing() override = default;

    void FillChannels(std::vector<Channel>& channels) const override;

    bool AllreduceInit(PlanTask& task) const noexcept override;
    bool AllreduceStep(PlanTask& task, CollEvent event) const noexcept override;
    bool AllreduceDone(const PlanTask& task) const override;

    int GetPrevRank(int r) const;
    int GetNextRank(int r) const;

private:
    bool BeginStep(PlanTask& task) const;
    bool CompleteStep(PlanTask& task) const;
    bool CompleteSteps(PlanTask& task) const;
};
