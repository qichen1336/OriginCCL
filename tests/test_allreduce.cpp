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

// Correct collective results alone do not say which transport carried them.
bool TestTransportSelection(const Communicator& comm) {
    if (comm.GetWorldSize() <= 1) {
        return true;
    }

    const int rank = comm.GetRank();
    const int expected = 2 * comm.GetNChannels();
    const int shm_edges = comm.GetShmEdgeCount();
    const int tcp_edges = comm.GetTcpEdgeCount();
    LOG_INFO("Rank {}: {} channel edges: {} shared-memory, {} TCP", rank, expected, shm_edges, tcp_edges);

    if (shm_edges + tcp_edges != expected) {
        LOG_ERROR("Rank {}: {} channel edges installed, expected {}", rank, shm_edges + tcp_edges, expected);
        return false;
    }

    const char* disable_shm = std::getenv("OCCL_DISABLE_SHM");
    if (disable_shm != nullptr && std::strcmp(disable_shm, "1") == 0) {
        if (shm_edges != 0) {
            LOG_ERROR("Rank {}: OCCL_DISABLE_SHM=1 but {} edges still run over shared memory", rank, shm_edges);
            return false;
        }
        return true;
    }

    if (comm.IsSingleMachine() && shm_edges != expected) {
        LOG_ERROR("Rank {}: single-host run must select shared memory for all {} edges, got {}", rank, expected,
                  shm_edges);
        return false;
    }
    return true;
}

bool TestAllReduce(Communicator& comm, size_t count, ReduceOp op, const char* op_name) {
    int rank = comm.GetRank();
    int world_size = comm.GetWorldSize();

    std::vector<float> send_data(count);
    std::vector<float> recv_data(count, 0.0f);
    for (size_t i = 0; i < count; ++i) {
        send_data[i] = static_cast<float>(rank + 1);
    }
    LOG_INFO("Rank {}: Original data filled with {} (count = {}, op = {})", rank, send_data[0], count, op_name);

    if (!comm.AllReduce(send_data.data(), recv_data.data(), count, DataType::FLOAT32, op)) {
        LOG_ERROR("Rank {}: Failed to AllReduce", rank);
        return false;
    }

    float expected = op == ReduceOp::AVG ? static_cast<float>(world_size + 1) / 2.0f
                                         : static_cast<float>(world_size * (world_size + 1) / 2);
    for (size_t i = 0; i < count; ++i) {
        if (recv_data[i] != expected) {
            LOG_ERROR("Rank {}: AllReduce verification failed at index {}, expected {}, got {}", rank, i, expected,
                      recv_data[i]);
            return false;
        }
    }

    LOG_INFO("Rank {}: AllReduce {} verification passed (expected {})", rank, op_name, expected);
    return true;
}

int main(int argc, char* argv[]) {
    CommConfig config;
    config.n_channels = 4;

    if (!LoadConfigFromEnv(config)) {
        if (argc < 3) {
            PrintUsage(argv[0]);
            return 1;
        }
        config.rank = std::atoi(argv[1]);
        config.world_size = std::atoi(argv[2]);
    }

    LOG_INFO("Configuration: rank={}, world_size={}, transport=auto, topology=Ring, master_addr={}, master_port={}",
             config.rank, config.world_size, config.master_addr, config.master_port);

    Communicator comm;
    if (!comm.Init(config)) {
        LOG_ERROR("Rank {}: Failed to init", config.rank);
        return 1;
    }
    LOG_INFO("Rank {}: Init", config.rank);

    if (!TestTransportSelection(comm)) {
        return 1;
    }

    if (!TestAllReduce(comm, 10, ReduceOp::SUM, "SUM")) {
        return 1;
    }

    for (int iteration = 0; iteration < 2; ++iteration) {
        LOG_INFO("Rank {}: Test AllReduce iteration {}", config.rank, iteration);
        if (!TestAllReduce(comm, 64 * 1024, ReduceOp::SUM, "SUM") ||
            !TestAllReduce(comm, 64 * 1024, ReduceOp::AVG, "AVG")) {
            return 1;
        }
    }

    return 0;
}
