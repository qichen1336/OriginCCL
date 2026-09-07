#pragma once

#include "topology.h"

class TopologyRing : public Topology {
public:
    TopologyRing() {
        topo_name = "Ring";
    }
    ~TopologyRing() override = default;

    void FillChannels(std::vector<Channel>& channels) const override;

    bool AllReduce(const PlanTask& task) const override;

    int DefaultChannelCount() const override {
        return 4;
    }

    int GetPrevRank(int r) const;
    int GetNextRank(int r) const;
};
