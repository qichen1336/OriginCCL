#pragma once

#include <string>
#include <cstdint>
#include <cstddef>

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

struct CommConfig {
    int rank = 0;
    int world_size = 1;
    std::string master_addr = "127.0.0.1";
    uint16_t master_port = 12345;
};

struct NodeInfo {
    int rank = 0;
    std::string ip_addr;
    uint16_t data_port = 0;

    NodeInfo() = default;

    NodeInfo(int r, const std::string& ip, uint16_t port) : rank(r), ip_addr(ip), data_port(port) {}
};
