#include <algorithm>
#include <cstdio>
#include <string>
#include <vector>
#include <mpi.h>
#include "communicator.h"
#include "logger.h"
#include "test_common.h"
#include "utils.h"

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);
    int rank = 0;
    int world_size = 1;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    const TestCommon::TestOptions options = TestCommon::ParseOptions(argc, argv);
    const int expected_world = TestCommon::ExpectedWorldSize(options.level);
    if (options.list_cases) {
        if (rank == 0) {
            const std::vector<TestCommon::CaseSpec> cases = TestCommon::BuildCases(options.level);
            fmt::print("cases={} digest={}\n", cases.size(), TestCommon::CaseListDigest(cases));
            const std::vector<TestCommon::CaseSpec> contract = TestCommon::BuildContractCases(options.level);
            fmt::print("contract cases={} digest={}\n", contract.size(), TestCommon::CaseListDigest(contract));
        }
        MPI_Finalize();
        return 0;
    }

    if (world_size != expected_world) {
        if (rank == 0) {
            LOG_ERROR("Level {} needs {} ranks but MPI launched {}, use mpirun -np {}", options.level, expected_world,
                      world_size, expected_world);
        }
        MPI_Finalize();
        return 4;
    }

    int failed = 0;
    CommConfig config;
    config.rank = rank;
    config.world_size = world_size;
    config.n_channels = 4;
    config.get_hostname = Utils::GetHostname;

    Communicator comm;
    if (!TestCommon::InitCommunicator(comm, config, rank, world_size)) {
        LOG_ERROR("Rank {}: failed to initialise", rank);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    if (!comm.IsSingleMachine() || comm.GetLocalSize() != world_size || comm.GetLocalRank() != rank) {
        LOG_ERROR("Rank {}: the single-machine view is wrong: single={} local_size={} local_rank={}", rank,
                  comm.IsSingleMachine(), comm.GetLocalSize(), comm.GetLocalRank());
        failed = 1;
    }

    char report[512] = {};
    if (!options.report_path.empty()) {
        std::snprintf(report, sizeof(report), "%s.single.%d", options.report_path.c_str(), rank);
    }
    TestCommon::Runner runner(comm, "single_machine", options.level, report);
    runner.Run(TestCommon::BuildCases(options.level));
    if (!runner.Passed()) {
        failed = 1;
    }
    TestCommon::Runner contract(comm, "single_machine_contract", options.level, report);
    contract.Run(TestCommon::BuildContractCases(options.level));
    if (!contract.Passed()) {
        failed = 1;
    }
    comm.Finalize();

    int global_failed = 0;
    MPI_Allreduce(&failed, &global_failed, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
    if (global_failed != 0) {
        MPI_Finalize();
        return 1;
    }

    MPI_Finalize();
    return 0;
}
