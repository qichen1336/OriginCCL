#include <algorithm>
#include <cstdio>
#include <string>
#include <vector>
#include <mpi.h>
#include "communicator.h"
#include "logger.h"
#include "test_common.h"
#include "utils.h"

namespace {
// A single host is made to look like several machines. CommConfig::get_hostname is a plain
// function pointer, so the layout is kept in file scope instead of in a capturing lambda.
int g_ranks_per_machine = 1;

std::string LogicalHostname() {
    int rank = 0;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    return "logical-machine-" + std::to_string(rank / g_ranks_per_machine);
}
} // namespace

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);
    int rank = 0;
    int world_size = 1;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    const TestCommon::TestOptions options = TestCommon::ParseOptions(argc, argv);
    const int expected_world = TestCommon::ExpectedWorldSize(options.level);
    const int ranks_per_machine = TestCommon::ExpectedRanksPerMachine(options.level, true);
    g_ranks_per_machine = ranks_per_machine;

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

    const int expected_local_size =
        std::min(ranks_per_machine, world_size - (rank / ranks_per_machine) * ranks_per_machine);
    const int expected_local_rank = rank % ranks_per_machine;
    const int expected_local_start = (rank / ranks_per_machine) * ranks_per_machine;

    int failed = 0;
    CommConfig config;
    config.rank = rank;
    config.world_size = world_size;
    config.n_channels = 4;
    config.get_hostname = &LogicalHostname;

    Communicator comm;
    if (!TestCommon::InitCommunicator(comm, config, rank, world_size)) {
        LOG_ERROR("Rank {}: failed to initialise", rank);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    if (comm.IsSingleMachine()) {
        LOG_ERROR("Rank {}: a multi-machine layout must not report IsSingleMachine", rank);
        failed = 1;
    }
    if (comm.GetLocalSize() != expected_local_size || comm.GetLocalRank() != expected_local_rank) {
        LOG_ERROR("Rank {}: local view is wrong: local_size={} (expected {}), local_rank={} (expected {})", rank,
                  comm.GetLocalSize(), expected_local_size, comm.GetLocalRank(), expected_local_rank);
        failed = 1;
    }
    for (int i = 0; i < expected_local_size; ++i) {
        if (comm.GetLocalRanks()[static_cast<size_t>(i)] != expected_local_start + i) {
            LOG_ERROR("Rank {}: local_ranks[{}] is {} but {} was expected", rank, i,
                      comm.GetLocalRanks()[static_cast<size_t>(i)], expected_local_start + i);
            failed = 1;
        }
    }

    char report[512] = {};
    if (!options.report_path.empty()) {
        std::snprintf(report, sizeof(report), "%s.multi.%d", options.report_path.c_str(), rank);
    }
    TestCommon::Runner runner(comm, "multi_machine", options.level, report);
    runner.Run(TestCommon::BuildCases(options.level));
    if (!runner.Passed()) {
        failed = 1;
    }
    TestCommon::Runner contract(comm, "multi_machine_contract", options.level, report);
    contract.Run(TestCommon::BuildContractCases(options.level));
    if (!contract.Passed()) {
        failed = 1;
    }
    std::string group_reason;
    if (!TestCommon::RunGroupCases(comm, group_reason)) {
        LOG_ERROR("Rank {}: group cases failed: {}", rank, group_reason);
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
