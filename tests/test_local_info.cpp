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

// On a single host (the only setup this tier can exercise) every rank must map to the
// same machine, so the local view degenerates to the global one.
bool TestLocalInfo(const Communicator& comm) {
    int rank = comm.GetRank();
    int world_size = comm.GetWorldSize();

    if (comm.GetLocalRank() != rank) {
        LOG_ERROR("Rank {}: local_rank {} != rank {}", rank, comm.GetLocalRank(), rank);
        return false;
    }
    if (comm.GetLocalSize() != world_size) {
        LOG_ERROR("Rank {}: local_size {} != world_size {}", rank, comm.GetLocalSize(), world_size);
        return false;
    }
    if (!comm.IsSingleMachine()) {
        LOG_ERROR("Rank {}: expected single_machine = true", rank);
        return false;
    }

    const std::vector<int>& local_ranks = comm.GetLocalRanks();
    if (static_cast<int>(local_ranks.size()) != world_size) {
        LOG_ERROR("Rank {}: local_ranks size {} != world_size {}", rank, local_ranks.size(), world_size);
        return false;
    }
    for (int i = 0; i < world_size; ++i) {
        if (local_ranks[static_cast<size_t>(i)] != i) {
            LOG_ERROR("Rank {}: local_ranks[{}] = {}, expected {}", rank, i, local_ranks[static_cast<size_t>(i)], i);
            return false;
        }
    }

    LOG_INFO("Rank {}: local info verification passed (local_rank = {}, local_size = {}, single_machine = {})", rank,
             comm.GetLocalRank(), comm.GetLocalSize(), comm.IsSingleMachine());
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

    LOG_INFO("Configuration: rank={}, world_size={}, transport=TCP, topology=Ring, master_addr={}, master_port={}",
             config.rank, config.world_size, config.master_addr, config.master_port);

    Communicator comm;
    if (!comm.Init(config)) {
        LOG_ERROR("Rank {}: Failed to init", config.rank);
        return 1;
    }
    LOG_INFO("Rank {}: Init", config.rank);

    if (!TestLocalInfo(comm)) {
        return 1;
    }

    return 0;
}
