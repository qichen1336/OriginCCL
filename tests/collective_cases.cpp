#include <algorithm>
#include <vector>
#include "collective_cases.h"
#include "logger.h"
#include "topology_ring.h"

namespace {
int Value(int source, size_t index) {
    return source * 101 + static_cast<int>(index % 97) - 50;
}

template<typename Element>
Element Expected(int size, size_t index, ReduceOp op, int target = 0) {
    Element result = Value(0, index) + target * 1000;
    for (int source = 1; source < size; ++source) {
        Element value = Value(source, index) + target * 1000;
        if (op == ReduceOp::MIN)
            result = std::min(result, value);
        else if (op == ReduceOp::MAX)
            result = std::max(result, value);
        else
            result += value;
    }
    return op == ReduceOp::AVG ? result / static_cast<Element>(size) : result;
}

template<typename Element>
bool TestCount(Communicator& comm, size_t count, DataType dtype) {
    int rank = comm.GetRank();
    int size = comm.GetWorldSize();
    std::vector<Element> send(count + 1, -12345);
    std::vector<Element> recv(count * size + 1, -12345);
    for (size_t index = 0; index < count; ++index) {
        send[index] = Value(rank, index);
    }
    if (!comm.AllGather(send.data(), recv.data(), count, dtype)) {
        return false;
    }
    for (int source = 0; source < size; ++source) {
        for (size_t index = 0; index < count; ++index) {
            if (recv[source * count + index] != Value(source, index)) {
                LOG_ERROR("AllGather rank {} source {} index {} count {}", rank, source, index, count);
                return false;
            }
        }
    }
    if (recv.back() != -12345) {
        return false;
    }
    for (int root = 0; root < size; ++root) {
        for (size_t index = 0; index < count; ++index) {
            send[index] = Value(rank, index);
        }
        std::fill(recv.begin(), recv.end(), -12345);
        for (ReduceOp op : {ReduceOp::SUM, ReduceOp::AVG, ReduceOp::MIN, ReduceOp::MAX}) {
            if (!comm.Reduce(send.data(), rank == root ? recv.data() : nullptr, count, dtype, op, root)) {
                return false;
            }
            if (rank == root) {
                for (size_t index = 0; index < count; ++index) {
                    Element expected = Expected<Element>(size, index, op);
                    if (recv[index] != expected) {
                        LOG_ERROR("Reduce rank {} index {} count {} op {}", rank, index, count, static_cast<int>(op));
                        return false;
                    }
                }
            }
        }
        for (size_t index = 0; index < count; ++index) {
            send[index] = Value(rank, index);
        }
        if (!comm.Broadcast(send.data(), count, dtype, root)) {
            return false;
        }
        for (size_t index = 0; index < count; ++index) {
            if (send[index] != Value(root, index)) {
                LOG_ERROR("Broadcast rank {} root {} index {} count {}", rank, root, index, count);
                return false;
            }
        }
    }
    std::vector<Element> exchange(count * size);
    for (int target = 0; target < size; ++target) {
        for (size_t index = 0; index < count; ++index) {
            exchange[target * count + index] = Value(rank, index) + target * 1000;
        }
    }
    for (ReduceOp op : {ReduceOp::SUM, ReduceOp::AVG, ReduceOp::MIN, ReduceOp::MAX}) {
        if (!comm.ReduceScatter(exchange.data(), send.data(), count, dtype, op)) {
            return false;
        }
        for (size_t index = 0; index < count; ++index) {
            Element expected = Expected<Element>(size, index, op, rank);
            if (send[index] != expected) {
                LOG_ERROR("ReduceScatter rank {} index {} count {} op {}", rank, index, count, static_cast<int>(op));
                return false;
            }
        }
    }
    for (int target = 0; target < size; ++target) {
        for (size_t index = 0; index < count; ++index) {
            if (exchange[target * count + index] != Value(rank, index) + target * 1000) {
                LOG_ERROR("Collective modified input on rank {}", rank);
                return false;
            }
        }
    }
    for (ReduceOp op : {ReduceOp::SUM, ReduceOp::AVG, ReduceOp::MIN, ReduceOp::MAX}) {
        for (size_t index = 0; index < count; ++index)
            send[index] = Value(rank, index);
        if (!comm.AllReduce(send.data(), send.data(), count, dtype, op))
            return false;
        for (size_t index = 0; index < count; ++index) {
            if (send[index] != Expected<Element>(size, index, op)) {
                LOG_ERROR("In-place AllReduce rank {} count {} index {}", rank, count, index);
                return false;
            }
        }
    }
    return recv.back() == -12345 && send.back() == -12345;
}

bool TestPlan(Communicator& comm) {
    std::vector<int> data(65537 * comm.GetWorldSize());
    CollTask task{CollFunc::Reduce, data.data(), nullptr, 65537, DataType::INT32, ReduceOp::SUM, 1};
    auto plan = Planner{}.Plan(comm, task);
    size_t channels = std::min(comm.GetNChannels(), 4);
    if (plan.channels.size() != channels)
        return false;
    size_t offset = 0;
    for (const auto& channel : plan.channels) {
        const auto& slice = channel.tasks.front();
        if (slice.rank_stride != task.count || slice.root != task.root || slice.recv_buf != nullptr ||
            slice.send_buf != data.data() + offset)
            return false;
        offset += slice.elem_count;
    }
    return offset == task.count;
}

bool TestEmptyAndInvalid(Communicator& comm) {
    if (!comm.Broadcast(nullptr, 0, DataType::FLOAT32, 0) ||
        !comm.Reduce(nullptr, nullptr, 0, DataType::FLOAT32, ReduceOp::SUM, 0) ||
        !comm.AllGather(nullptr, nullptr, 0, DataType::FLOAT32) ||
        !comm.ReduceScatter(nullptr, nullptr, 0, DataType::FLOAT32, ReduceOp::SUM) ||
        !comm.AllReduce(nullptr, nullptr, 0, DataType::FLOAT32, ReduceOp::SUM))
        return false;
    TopologyRing topology;
    PlanTask task;
    task.func = CollFunc::Broadcast;
    task.root = -1;
    LOG_INFO("Rank {}: expected invalid-task errors follow", comm.GetRank());
    if (topology.CollectiveInit(task) || topology.CollectiveDone(task))
        return false;
    task.root = task.world_size;
    if (topology.CollectiveInit(task))
        return false;
    task.root = 0;
    task.elem_count = 1;
    if (topology.CollectiveInit(task))
        return false;
    task.func = static_cast<CollFunc>(-1);
    return !topology.CollectiveInit(task);
}
}

bool TestCollectives(Communicator& comm, bool large) {
    if (!TestPlan(comm) || !TestEmptyAndInvalid(comm))
        return false;
    if (large)
        return TestCount<float>(comm, 600001, DataType::FLOAT32);
    for (size_t count : {size_t{0}, size_t{1}, size_t{2}, size_t{7}, size_t{49153}, size_t{65537}}) {
        if (!TestCount<int32_t>(comm, count, DataType::INT32) || !TestCount<int64_t>(comm, count, DataType::INT64) ||
            !TestCount<float>(comm, count, DataType::FLOAT32) || !TestCount<double>(comm, count, DataType::FLOAT64)) {
            return false;
        }
    }
    LOG_INFO("Rank {}: collective cases passed", comm.GetRank());
    return true;
}