#include <vector>
#include <cstdlib>
#include <cstring>
#include "types.h"
#include "logger.h"
#include "communicator.h"

void PrintUsage(const char* prog) {
    LOG_ERROR("Usage:");
    LOG_ERROR("  Manual:    {} <rank> <world_size>", prog);
    LOG_ERROR("  Open MPI:  mpirun -np <N> {}", prog);
}

// 通过环境变量依次为 config 各字段赋值（不存在该环境变量时保留默认值）。
// 返回 true 表示检测到 OpenMPI 环境变量（由 mpirun 启动）。
bool LoadConfigFromEnv(CommConfig& config) {
    bool found = false;

    const char* ompi_rank = std::getenv("OMPI_COMM_WORLD_RANK");
    const char* ompi_size = std::getenv("OMPI_COMM_WORLD_SIZE");
    if (ompi_rank && ompi_size) {
        config.rank = std::atoi(ompi_rank);
        config.world_size = std::atoi(ompi_size);
        found = true;
    }

    const char* master_addr_env = std::getenv("OCCL_MASTER_ADDR");
    if (master_addr_env && master_addr_env[0] != '\0') {
        config.master_addr = master_addr_env;
    }

    const char* master_port_env = std::getenv("OCCL_MASTER_PORT");
    if (master_port_env && master_port_env[0] != '\0') {
        config.master_port = static_cast<uint16_t>(std::atoi(master_port_env));
    }

    return found;
}

bool TestAllReduce(Communicator& comm) {
    int rank = comm.GetRank();
    int world_size = comm.GetWorldSize();

    size_t count = 10;
    std::vector<float> data(count);
    for (size_t i = 0; i < count; ++i) {
        data[i] = static_cast<float>(rank + 1);
    }
    LOG_INFO("Rank {}: Original data filled with {} (count = {})", rank, data[0], count);

    if (!comm.AllReduce(data.data(), data.data(), count, DataType::FLOAT32, ReduceOp::SUM)) {
        LOG_ERROR("Rank {}: Failed to AllReduce", rank);
        return false;
    }

    float expected = static_cast<float>(world_size * (world_size + 1) / 2);
    for (size_t i = 0; i < count; ++i) {
        if (data[i] != expected) {
            LOG_ERROR("Rank {}: AllReduce verification failed at index {}, expected {}, got {}", rank, i, expected,
                      data[i]);
            return false;
        }
    }

    LOG_INFO("Rank {}: AllReduce verification passed (expected {})", rank, expected);
    return true;
}

int main(int argc, char* argv[]) {
    CommConfig config;

    if (!LoadConfigFromEnv(config)) {
        if (argc < 3) {
            PrintUsage(argv[0]);
            return 1;
        }
        config.rank = std::atoi(argv[1]);
        config.world_size = std::atoi(argv[2]);
    }

    LOG_INFO("Configuration: rank={}, world_size={}, transport=TCP, topology=Ring, master_addr={}, master_port={}",
             config.rank, config.world_size, config.master_addr, config.master_port);

    Communicator comm;
    if (!comm.Init(config)) {
        LOG_ERROR("Rank {}: Failed to init", config.rank);
        return 1;
    }
    LOG_INFO("Rank {}: Init", config.rank);

    LOG_INFO("Rank {}: Test AllReduce", config.rank);
    if (!TestAllReduce(comm)) {
        return 1;
    }

    return 0;
}
