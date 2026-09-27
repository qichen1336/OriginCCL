#include <cstdio>
#include <memory>
#include <string>
#include <mpi.h>
#include "logger.h"
#include "transport_check.h"
#include "transport/transport_rdma.h"

namespace {
std::shared_ptr<Transport> Make() {
    return std::make_shared<TransportRDMA>();
}
} // namespace

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);
    int rank = 0;
    int world_size = 1;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    // No usable device is an environment SKIP, never a pass: every rank probes, and the run
    // is skipped unless all of them can open a device with an active port.
    std::string device_addr;
    int device_ok = TransportRDMA::Probe(device_addr) ? 1 : 0;
    int global_ok = 0;
    MPI_Allreduce(&device_ok, &global_ok, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);
    if (global_ok == 0) {
        if (rank == 0) {
            LOG_INFO("rdma: no device with an active port is available, skipping");
        }
        MPI_Finalize();
        return 2;
    }

    char buffer[64] = {};
    if (rank == 0) {
        std::snprintf(buffer, sizeof(buffer), "%s", device_addr.c_str());
    }
    MPI_Bcast(buffer, sizeof(buffer), MPI_CHAR, 0, MPI_COMM_WORLD);

    // The peer's QP teardown does not reliably flush the local receive queue, so RDMA does not
    // promise a peer-close signal; the suite checks the local Close contract instead.
    const TransportCheck::Setup setup{"rdma", buffer, buffer, true, true, false, &Make};
    const int result = TransportCheck::RunSuite(setup, rank, world_size);
    MPI_Finalize();
    return result;
}
