#include <cstdio>
#include <memory>
#include <string>
#include <mpi.h>
#include "logger.h"
#include "transport_check.h"
#include "transport/transport_tcp.h"

namespace {
std::shared_ptr<Transport> Make() {
    return std::make_shared<TransportTCP>();
}
} // namespace

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);
    int rank = 0;
    int world_size = 1;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    const TransportCheck::Setup setup{"tcp", "", "127.0.0.1", false, false, true, &Make};
    const int result = TransportCheck::RunSuite(setup, rank, world_size);
    MPI_Finalize();
    return result;
}
