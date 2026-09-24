#include <mpi.h>
#include <string>
#include "collective_cases.h"

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);
    CommConfig config;
    bool large = argc > 1 && std::string(argv[1]) == "--large";
    config.n_channels = large ? 1 : 4;
    MPI_Comm_rank(MPI_COMM_WORLD, &config.rank);
    MPI_Comm_size(MPI_COMM_WORLD, &config.world_size);
    Communicator comm;
    if (config.rank == 0 && !comm.GetUniqueId(config.unique_id)) {
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    MPI_Bcast(&config.unique_id, sizeof(config.unique_id), MPI_BYTE, 0, MPI_COMM_WORLD);
    if (!comm.Init(config) || !TestCollectives(comm, large)) {
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    comm.Finalize();
    MPI_Finalize();
    return 0;
}