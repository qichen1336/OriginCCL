#include "topology_ring.h"

void TopologyRing::FillChannels(std::vector<Channel>& channels) const {
    for (size_t i = 0; i < channels.size(); ++i) {
        channels[i].id = static_cast<int>(i);
        channels[i].ring.prev = GetPrevRank(rank);
        channels[i].ring.next = GetNextRank(rank);
        channels[i].send.peer = channels[i].ring.next;
        channels[i].send.channel_id = channels[i].id;
        channels[i].send.is_send = true;
        channels[i].recv.peer = channels[i].ring.prev;
        channels[i].recv.channel_id = channels[i].id;
        channels[i].recv.is_send = false;
    }
}

int TopologyRing::GetPrevRank(int r) const {
    return (r - 1 + world_size) % world_size;
}

int TopologyRing::GetNextRank(int r) const {
    return (r + 1) % world_size;
}
