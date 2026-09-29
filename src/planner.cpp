#include <algorithm>
#include "planner.h"
#include "communicator.h"
#include "channel.h"
#include "topology.h"
#include "utils.h"

namespace {
constexpr size_t kChunkBytes = 32 * 1024;
}

CollPlan Planner::Plan(Communicator& comm, const CollTask& task) const {
    size_t type_size = Utils::GetDataTypeSize(task.dtype);
    size_t total_bytes = task.count * type_size;
    int max_channels = std::max(comm.GetNChannels(), 1);
    size_t unit_bytes =
        task.func == CollFunc::AllReduce ? kChunkBytes * static_cast<size_t>(comm.GetWorldSize()) : kChunkBytes;

    size_t units_total = total_bytes / unit_bytes;
    size_t rem_bytes = total_bytes % unit_bytes;
    int n_used = 1;
    if (units_total > 0) {
        n_used = static_cast<int>(std::min(units_total, static_cast<size_t>(max_channels)));
    }

    size_t base_units = units_total / static_cast<size_t>(n_used);
    size_t rem_units = units_total % static_cast<size_t>(n_used);

    CollPlan plan(n_used);

    size_t offset = 0;
    for (int c = 0; c < n_used; ++c) {
        size_t channel_units = base_units + (static_cast<size_t>(c) < rem_units ? 1 : 0);
        size_t channel_bytes = channel_units * unit_bytes;
        if (c == n_used - 1) {
            channel_bytes += rem_bytes;
        }
        size_t elem_count = channel_bytes / type_size;
        ChannelPlan& channel = plan.channels[static_cast<size_t>(c)];
        Channel& comm_channel = comm.GetChannel(c);
        PlanTask plantask;
        plantask.func = task.func;
        plantask.topology = comm.GetTopology();
        plantask.send_buf = task.send_buf ? static_cast<const char*>(task.send_buf) + offset * type_size : nullptr;
        plantask.recv_buf = task.recv_buf ? static_cast<char*>(task.recv_buf) + offset * type_size : nullptr;
        plantask.elem_count = elem_count;
        plantask.rank_stride = task.count;
        plantask.root = task.root;
        plantask.dtype = task.dtype;
        plantask.reduce_op = task.op;
        plantask.rank = comm.GetRank();
        plantask.world_size = comm.GetWorldSize();
        plantask.chunk_size =
            (elem_count + static_cast<size_t>(plantask.world_size) - 1) / static_cast<size_t>(plantask.world_size);
        plantask.topology->FillTransports(comm_channel, plantask.send_transports, plantask.recv_transports);
        channel.tasks.push_back(std::move(plantask));
        offset += elem_count;
    }
    return plan;
}
