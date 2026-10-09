#include <algorithm>
#include <cstdio>
#include <limits>
#include <string>
#include <vector>
#include <mpi.h>
#include "communicator.h"
#include "logger.h"
#include "test_common.h"
#include "utils.h"

namespace {
bool CheckCollectiveValidation(Communicator& comm) {
    int value = 1;
    const size_t max_size = std::numeric_limits<size_t>::max();
    const size_t total_count = max_size / static_cast<size_t>(comm.GetWorldSize()) / sizeof(int32_t) + 1;
    bool passed = true;
    auto expect_rejected = [&passed](bool success, const char* name) {
        if (success) {
            LOG_ERROR("Collective validation accepted {}", name);
            passed = false;
        }
    };

    expect_rejected(comm.AllReduce(&value, &value, max_size / sizeof(double) + 1, DataType::FLOAT64, ReduceOp::SUM),
                    "an overflowing byte count");
    expect_rejected(comm.AllGather(&value, &value, total_count, DataType::INT32), "an overflowing AllGather extent");
    expect_rejected(comm.ReduceScatter(&value, &value, total_count, DataType::INT32, ReduceOp::SUM),
                    "an overflowing ReduceScatter extent");
    expect_rejected(comm.AllReduce(&value, &value, 1, static_cast<DataType>(99), ReduceOp::SUM), "an invalid dtype");
    expect_rejected(comm.AllReduce(&value, &value, 1, DataType::INT32, static_cast<ReduceOp>(99)),
                    "an invalid reduce operation");
    expect_rejected(comm.Broadcast(&value, 1, DataType::INT32, comm.GetWorldSize()), "an invalid root");
    expect_rejected(comm.AllReduce(nullptr, &value, 1, DataType::INT32, ReduceOp::SUM), "a missing input buffer");

    comm.GroupStart();
    expect_rejected(comm.AllReduce(nullptr, &value, 1, DataType::INT32, ReduceOp::SUM),
                    "a grouped task with a missing input buffer");
    if (!comm.GroupEnd()) {
        LOG_ERROR("A group must remain empty when submission validation rejects its task");
        passed = false;
    }
    return passed;
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
    if (!CheckCollectiveValidation(comm)) {
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
