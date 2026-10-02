#pragma once

#include <vector>
#include "topology/topology.h"

class TopologyTree : public Topology {
public:
    TopologyTree() {
        topo_name = "Tree";
    }
    ~TopologyTree() override = default;

    void FillChannels(const Communicator& comm, std::vector<Channel>& channels) override;
    void FillPeers(const Channel& channel, std::vector<TopoEdge>& edges) const override;
    void FillTransports(Channel& channel, PlanTask& task) const override;

    bool CollectiveInit(PlanTask& task) const noexcept override;
    bool CollectiveStep(PlanTask& task, CollEvent event) const noexcept override;
    bool CollectiveDone(const PlanTask& task) const override;

private:
    bool IsLeader() const {
        return is_leader_;
    }

    bool is_leader_ = false;
    std::vector<std::vector<int>> channel_roles_;
};
