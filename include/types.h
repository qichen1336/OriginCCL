#pragma once

#include <string>
#include <cstdint>
#include <cstddef>
#include <memory>
#include <vector>

class Transport;
class Topology;

enum class DataType {
    FLOAT32,
    FLOAT64,
    INT32,
    INT64
};

enum class ReduceOp {
    SUM,
    MAX,
    MIN,
    AVG
};

enum class CollFunc {
    AllReduce
};

struct CommConfig {
    int rank = 0;
    int world_size = 1;
    std::string master_addr = "127.0.0.1";
    uint16_t master_port = 12345;
    int n_channels = 0;
};

struct NodeInfo {
    int rank = 0;
    std::string ip_addr;
    uint16_t data_port = 0;

    NodeInfo() = default;

    NodeInfo(int r, const std::string& ip, uint16_t port) : rank(r), ip_addr(ip), data_port(port) {}
};

struct CollTask {
    CollFunc func = CollFunc::AllReduce;
    const void* send_buf = nullptr;
    void* recv_buf = nullptr;
    size_t count = 0;
    DataType dtype = DataType::FLOAT32;
    ReduceOp op = ReduceOp::SUM;
};

struct PlanTask {
    CollFunc func = CollFunc::AllReduce;
    const void* send_buf = nullptr;
    void* recv_buf = nullptr;
    size_t elem_count = 0;
    DataType dtype = DataType::FLOAT32;
    ReduceOp reduce_op = ReduceOp::SUM;
    int rank = 0;
    int world_size = 1;
    std::shared_ptr<Topology> topology;
    std::shared_ptr<Transport> send_transport;
    std::shared_ptr<Transport> recv_transport;
};

struct ChannelPlan {
    explicit ChannelPlan(int id) : channel_id(id) {}

    int channel_id;
    std::vector<PlanTask> tasks;
};

struct CollPlan {
    explicit CollPlan(int n_channels = 0) {
        channels.reserve(static_cast<size_t>(n_channels));
        for (int channel_id = 0; channel_id < n_channels; ++channel_id) {
            channels.emplace_back(channel_id);
        }
    }

    std::vector<ChannelPlan> channels;
};
