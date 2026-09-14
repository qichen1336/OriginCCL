#include <algorithm>
#include "planner.h"
#include "communicator.h"
#include "channel.h"
#include "utils.h"

namespace {
constexpr size_t kMinBytesPerChannel = 64 * 1024;
}

CollPlan Planner::Plan(Communicator& comm, const CollTask& task) const {
    size_t type_size = Utils::GetDataTypeSize(task.dtype);
    size_t total_bytes = task.count * type_size;
    int max_channels = std::max(comm.GetNChannels(), 1);
    int n_used = 1;
    if (kMinBytesPerChannel > 0 && total_bytes >= kMinBytesPerChannel) {
        n_used = static_cast<int>(total_bytes / kMinBytesPerChannel);
        n_used = std::clamp(n_used, 1, max_channels);
    }

    size_t base = task.count / static_cast<size_t>(n_used);
    size_t rem = task.count % static_cast<size_t>(n_used);

    CollPlan plan(n_used);

    size_t offset = 0;
    for (int c = 0; c < n_used; ++c) {
        size_t elem_count = base + (static_cast<size_t>(c) < rem ? 1 : 0);
        ChannelPlan& channel = plan.channels[static_cast<size_t>(c)];
        Channel& comm_channel = comm.GetChannel(c);
        Connector* send_conn = comm_channel.SendConnector(comm_channel.ring.next);
        Connector* recv_conn = comm_channel.RecvConnector(comm_channel.ring.prev);

        PlanTask plantask;
        plantask.func = task.func;
        plantask.topology = comm.GetTopology();
        plantask.send_buf = static_cast<const char*>(task.send_buf) + offset * type_size;
        plantask.recv_buf = static_cast<char*>(task.recv_buf) + offset * type_size;
        plantask.elem_count = elem_count;
        plantask.dtype = task.dtype;
        plantask.reduce_op = task.op;
        plantask.rank = comm.GetRank();
        plantask.world_size = comm.GetWorldSize();
        plantask.chunk_size =
            (elem_count + static_cast<size_t>(plantask.world_size) - 1) / static_cast<size_t>(plantask.world_size);
        plantask.send_transport = send_conn ? send_conn->transport : nullptr;
        plantask.recv_transport = recv_conn ? recv_conn->transport : nullptr;
        channel.tasks.push_back(std::move(plantask));
        offset += elem_count;
    }
    return plan;
}
