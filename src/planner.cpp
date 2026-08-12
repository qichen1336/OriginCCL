#include <algorithm>
#include "planner.h"
#include "communicator.h"
#include "utils.h"

namespace {
constexpr size_t kMinBytesPerChannel = 64 * 1024;
}

CollPlan Planner::Plan(const Communicator& comm, const CollTask& task) const {
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

    CollPlan plan;
    plan.algo = Algorithm::Ring;
    plan.n_channels = n_used;

    size_t offset = 0;
    for (int c = 0; c < n_used; ++c) {
        size_t elem_count = base + (static_cast<size_t>(c) < rem ? 1 : 0);
        plan.works.push_back(ChannelWork{c, offset, elem_count});
        offset += elem_count;
    }
    return plan;
}
