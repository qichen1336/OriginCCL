#include <vector>
#include <mpi.h>
#include "types.h"
#include "logger.h"
#include "communicator.h"

// This tier runs on one host, so the local view degenerates to the global one.
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
    MPI_Init(&argc, &argv);

    CommConfig config;
    config.n_channels = 4;
    MPI_Comm_rank(MPI_COMM_WORLD, &config.rank);
    MPI_Comm_size(MPI_COMM_WORLD, &config.world_size);

    LOG_INFO("Configuration: rank={}, world_size={}, transport=TCP, topology=Ring", config.rank, config.world_size);

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

    if (!TestLocalInfo(comm)) {
        return 1;
    }

    MPI_Finalize();
    return 0;
}
