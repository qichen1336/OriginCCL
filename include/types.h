#pragma once

#include <string>
#include <cstdint>
#include <cstddef>
#include <memory>
#include <vector>

class Transport;
class Topology;

namespace Utils {
std::string GetHostname();
}

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

struct UniqueId {
    char ip_addr[64] = {};
    uint16_t port = 0;
};

struct CommConfig {
    int rank = 0;
    int world_size = 1;
    UniqueId unique_id;
    int n_channels = 0;
    std::string (*get_hostname)() = Utils::GetHostname;
};

struct NodeInfo {
    int rank = 0;
    std::string ip_addr;
    uint16_t data_port = 0;
    std::string hostname;
    std::string rdma_addr;
    uint16_t rdma_port = 0;

    NodeInfo() = default;

    NodeInfo(int r, const std::string& ip, uint16_t port, const std::string& host = std::string())
        : rank(r), ip_addr(ip), data_port(port), hostname(host) {}
};

struct CollTask {
    CollFunc func = CollFunc::AllReduce;
    const void* send_buf = nullptr;
    void* recv_buf = nullptr;
    size_t count = 0;
    DataType dtype = DataType::FLOAT32;
    ReduceOp op = ReduceOp::SUM;
};

enum class CollEvent {
    Readable,
    Writable
};

struct CollOpState {
    int phase = 0;
    int step = 0;
    size_t send_progress = 0;
    size_t recv_progress = 0;
    bool send_done = false;
    bool recv_done = false;
    std::vector<char> temp_buffer;
};

struct PlanTask {
    CollFunc func = CollFunc::AllReduce;
    const void* send_buf = nullptr;
    void* recv_buf = nullptr;
    size_t elem_count = 0;
    size_t chunk_size = 0;
    DataType dtype = DataType::FLOAT32;
    ReduceOp reduce_op = ReduceOp::SUM;
    int rank = 0;
    int world_size = 1;
    std::shared_ptr<Topology> topology;
    std::shared_ptr<Transport> send_transport;
    std::shared_ptr<Transport> recv_transport;
    CollOpState state;
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
