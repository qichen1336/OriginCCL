#include <string>
#include <vector>
#include <mpi.h>
#include "types.h"
#include "logger.h"
#include "communicator.h"

namespace {
int RanksPerMachine(int world_size) {
    return world_size == 4 ? 2 : 1;
}

std::string FakeHostname() {
    int rank = 0;
    int world_size = 1;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    return "fake-machine-" + std::to_string(rank / RanksPerMachine(world_size));
}
} // namespace

bool TestLocalInfo(const Communicator& comm, int ranks_per_machine) {
    int rank = comm.GetRank();
    int world_size = comm.GetWorldSize();

    int machine = rank / ranks_per_machine;
    int machine_start = machine * ranks_per_machine;
    int machine_end = machine_start + ranks_per_machine;
    if (machine_end > world_size) {
        machine_end = world_size;
    }
    int expected_local_size = machine_end - machine_start;
    int expected_local_rank = rank - machine_start;

    if (comm.GetLocalRank() != expected_local_rank) {
        LOG_ERROR("Rank {}: local_rank {} != expected {}", rank, comm.GetLocalRank(), expected_local_rank);
        return false;
    }
    if (comm.GetLocalSize() != expected_local_size) {
        LOG_ERROR("Rank {}: local_size {} != expected {}", rank, comm.GetLocalSize(), expected_local_size);
        return false;
    }
    if (comm.IsSingleMachine() != (expected_local_size == world_size)) {
        LOG_ERROR("Rank {}: is_single_machine {} != expected {}", rank, comm.IsSingleMachine(),
                  expected_local_size == world_size);
        return false;
    }

    const std::vector<int>& local_ranks = comm.GetLocalRanks();
    if (static_cast<int>(local_ranks.size()) != expected_local_size) {
        LOG_ERROR("Rank {}: local_ranks size {} != expected {}", rank, local_ranks.size(), expected_local_size);
        return false;
    }
    for (int i = 0; i < expected_local_size; ++i) {
        if (local_ranks[static_cast<size_t>(i)] != machine_start + i) {
            LOG_ERROR("Rank {}: local_ranks[{}] = {}, expected {}", rank, i, local_ranks[static_cast<size_t>(i)],
                      machine_start + i);
            return false;
        }
    }

    LOG_INFO("Rank {}: local info passed (machine = {}, local_rank = {}, local_size = {}, single_machine = {})", rank,
             machine, comm.GetLocalRank(), comm.GetLocalSize(), comm.IsSingleMachine());
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

    if (!comm.AllReduce(send_data.data(), recv_data.data(), count, DataType::FLOAT32, op)) {
        LOG_ERROR("Rank {}: Failed to AllReduce", rank);
        return false;
    }

    float expected = op == ReduceOp::AVG ? static_cast<float>(world_size + 1) / 2.0f
                                         : static_cast<float>(world_size * (world_size + 1) / 2);
    for (size_t i = 0; i < count; ++i) {
        if (recv_data[i] != expected) {
            LOG_ERROR("Rank {}: AllReduce {} verification failed at index {}, expected {}, got {}", rank, op_name, i,
                      expected, recv_data[i]);
            return false;
        }
    }

    LOG_INFO("Rank {}: AllReduce {} passed (expected {})", rank, op_name, expected);
    return true;
}

int main(int argc, char* argv[]) {
    MPI_Init(&argc, &argv);

    CommConfig config;
    config.n_channels = 4;
    MPI_Comm_rank(MPI_COMM_WORLD, &config.rank);
    MPI_Comm_size(MPI_COMM_WORLD, &config.world_size);
    config.get_hostname = &FakeHostname;

    int ranks_per_machine = RanksPerMachine(config.world_size);
    LOG_INFO("Configuration: rank={}, world_size={}, ranks_per_machine={}, topology=Ring", config.rank,
             config.world_size, ranks_per_machine);

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

    if (!TestLocalInfo(comm, ranks_per_machine)) {
        return 1;
    }
    if (!TestAllReduce(comm, 10, ReduceOp::SUM, "SUM") || !TestAllReduce(comm, 64 * 1024, ReduceOp::SUM, "SUM") ||
        !TestAllReduce(comm, 64 * 1024, ReduceOp::AVG, "AVG")) {
        return 1;
    }

    MPI_Finalize();
    return 0;
}
