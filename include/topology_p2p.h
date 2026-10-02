#pragma once

#include "topology.h"

class TopologyP2p : public Topology {
public:
    TopologyP2p() {
        topo_name = "P2P";
    }

    void FillChannels(const Communicator& comm, std::vector<Channel>& channels) override;
    void FillPeers(const Channel& channel, std::vector<TopoEdge>& edges) const override;
    void FillTransports(Channel& channel, PlanTask& task) const override;

    bool CollectiveInit(PlanTask& task) const noexcept override;
    bool CollectiveStep(PlanTask& task, CollEvent event) const noexcept override;
    bool CollectiveDone(const PlanTask& task) const override;
};
