#include "topology/topology_p2p.h"
#include "logger.h"
#include "utils.h"

void TopologyP2p::FillChannels(const Communicator&, std::vector<Channel>& channels) {
    for (auto& channel : channels) {
        channel.send_p2p.resize(static_cast<size_t>(world_size));
        channel.recv_p2p.resize(static_cast<size_t>(world_size));
        for (int peer = 0; peer < world_size; ++peer) {
            channel.send_p2p[peer] = Connector{peer, channel.id, true, nullptr};
            channel.recv_p2p[peer] = Connector{peer, channel.id, false, nullptr};
        }
    }
}

void TopologyP2p::FillPeers(const Channel&, std::vector<TopoEdge>& edges) const {
    edges.clear();
}

void TopologyP2p::FillTransports(Channel& channel, PlanTask& task) const {
    task.send_transports.clear();
    task.recv_transports.clear();
    if (task.elem_count == 0) {
        return;
    }
    if (task.func == CollFunc::Send) {
        task.send_transports.push_back(channel.send_p2p[task.peer].transport);
    } else {
        task.recv_transports.push_back(channel.recv_p2p[task.peer].transport);
    }
}

bool TopologyP2p::CollectiveInit(PlanTask& task) const noexcept {
    task.state = {};
    task.state.send_progress.assign(task.send_transports.size(), 0);
    task.state.recv_progress.assign(task.recv_transports.size(), 0);
    task.state.send_done.assign(task.send_transports.size(), 0);
    task.state.recv_done.assign(task.recv_transports.size(), 0);
    if (task.elem_count == 0) {
        task.state.phase = 1;
        return true;
    }
    return CollectiveStep(task, task.func == CollFunc::Send ? CollEvent::Writable : CollEvent::Readable);
}

bool TopologyP2p::CollectiveStep(PlanTask& task, CollEvent event) const noexcept {
    if (CollectiveDone(task)) {
        return true;
    }
    const bool is_send = task.func == CollFunc::Send;
    if ((event == CollEvent::Writable) != is_send) {
        return true;
    }
    const size_t bytes = task.elem_count * Utils::GetDataTypeSize(task.dtype);
    bool done = false;
    const bool success =
        is_send ? task.send_transports[0]->TrySend(task.send_buf, bytes, &task.state.send_progress[0], &done)
                : task.recv_transports[0]->TryRecv(task.recv_buf, bytes, &task.state.recv_progress[0], &done);
    if (!success) {
        LOG_ERROR("P2P {} failed on rank {} with peer {}", Utils::GetCollFuncName(task.func), task.rank, task.peer);
        return false;
    }
    (is_send ? task.state.send_done : task.state.recv_done)[0] = done;
    task.state.phase = done ? 1 : 0;
    return true;
}

bool TopologyP2p::CollectiveDone(const PlanTask& task) const {
    return task.state.phase == 1;
}
