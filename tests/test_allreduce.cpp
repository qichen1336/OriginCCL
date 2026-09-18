#include <vector>
#include <mpi.h>
#include "types.h"
#include "logger.h"
#include "communicator.h"

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
    MPI_Init(&argc, &argv);

    CommConfig config;
    config.n_channels = 4;
    MPI_Comm_rank(MPI_COMM_WORLD, &config.rank);
    MPI_Comm_size(MPI_COMM_WORLD, &config.world_size);

    LOG_INFO("Configuration: rank={}, world_size={}, transport=auto, topology=Ring", config.rank, config.world_size);

    Communicator comm;
    if (config.rank == 0 && !comm.GetUniqueId(config.unique_id)) {
        LOG_ERROR("Rank 0: Failed to get the unique id");
        return 1;
    }
    MPI_Bcast(&config.unique_id, sizeof(config.unique_id), MPI_BYTE, 0, MPI_COMM_WORLD);

    if (!comm.Init(config)) {
        LOG_ERROR("Rank {}: Failed to init", config.rank);
        return 1;
    }
    LOG_INFO("Rank {}: Init", config.rank);

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

    MPI_Finalize();
    return 0;
}
