#include <cstdio>
#include <memory>
#include <string>
#include <unistd.h>
#include <mpi.h>
#include "logger.h"
#include "transport_check.h"
#include "transport/transport_shm.h"

namespace {
std::shared_ptr<Transport> Make() {
    return std::make_shared<TransportShm>();
}
} // namespace

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);
    int rank = 0;
    int world_size = 1;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    // Shared memory has no numeric port: the rendezvous path is the address and both ends
    // must use the same one, so rank 0 derives it from the job id and hands it to the peer.
    char buffer[256] = {};
    if (rank == 0) {
        std::snprintf(buffer, sizeof(buffer), "/tmp/originccl-test-%ld.sock", static_cast<long>(getpid()));
    }
    MPI_Bcast(buffer, sizeof(buffer), MPI_CHAR, 0, MPI_COMM_WORLD);
    const std::string path = buffer;

    const TransportCheck::Setup setup{"shm", path, path, true, true, false, &Make};
    const int result = TransportCheck::RunSuite(setup, rank, world_size);
    MPI_Finalize();
    return result;
}
