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

bool GetOpenMPIEnv(int& rank, int& world_size) {
    const char* ompi_rank = std::getenv("OMPI_COMM_WORLD_RANK");
    const char* ompi_size = std::getenv("OMPI_COMM_WORLD_SIZE");
    if (ompi_rank && ompi_size) {
        rank = std::atoi(ompi_rank);
        world_size = std::atoi(ompi_size);
        return true;
    }

    return false;
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
    int rank = 0;
    int world_size = 1;

    if (!GetOpenMPIEnv(rank, world_size)) {
        if (argc < 3) {
            PrintUsage(argv[0]);
            return 1;
        }
        rank = std::atoi(argv[1]);
        world_size = std::atoi(argv[2]);
    }

    LOG_INFO("Configuration: rank={}, world_size={}, transport=TCP, topology=Ring", rank, world_size);

    CommConfig config;
    config.rank = rank;
    config.world_size = world_size;
    config.master_addr = "127.0.0.1";
    config.master_port = 12321;

    Communicator comm;
    if (!comm.Init(config)) {
        LOG_ERROR("Rank {}: Failed to init", rank);
        return 1;
    }
    LOG_INFO("Rank {}: Init", rank);

    LOG_INFO("Rank {}: Test AllReduce", rank);
    if (!TestAllReduce(comm)) {
        return 1;
    }

    return 0;
}
