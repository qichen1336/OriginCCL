#include <algorithm>
#include <cerrno>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <limits>
#include <string>
#include <vector>
#include <fmt/format.h>
#include <mpi.h>
#include "communicator.h"
#include "logger.h"
#ifdef OCCL_BENCH_OMPI
#include "utils.h"
#else
#include "occl_config.h"
#include "test_common.h"
#endif

#ifndef OCCL_BENCH_BUILD_TYPE
#define OCCL_BENCH_BUILD_TYPE "unknown"
#endif

// Raw AllReduce measurements behind the README "全矩阵对照" table.
// 4 x Aliyun ecs.g8y.4xlarge (aarch64, 16 cores), all four hosts with eRDMA
// (erdma_0 PORT_ACTIVE), cross-host link ~3.2 GB/s per direction. Release build,
// dtype=float32, op=sum, out-of-place, channels=4, sizes 2/8/32/128 MiB,
// warmup=5, repeats=10, target_bytes=40 GiB per size. Private IPs:
// A 172.18.10.0, B 172.18.9.255, C 172.18.10.2, D 172.18.10.3.
// busbw = algbw * 2(N-1)/N  (N=4 -> 1.5, N=16 -> 1.875).
// Columns: bytes count min_us median_us max_us algGB/s busGB/s check
//
// [1] OriginCCL auto, 1 machine x 4 ranks (SHM)
//    2097152     524288   255.490   262.499   268.646     7.989    11.984  PASS
//    8388608    2097152  1239.242  1269.040  1290.493     6.610     9.915  PASS
//   33554432    8388608  6894.320  6950.743  7059.215     4.827     7.241  PASS
//  134217728   33554432 26576.229 26783.594 27483.406     5.011     7.517  PASS
//
// [2] OriginCCL auto, 2 machines x 2 ranks (eRDMA)
//    2097152     524288   978.981   985.550   999.975     2.128     3.192  PASS
//    8388608    2097152  3914.472  3922.758  3990.393     2.138     3.208  PASS
//   33554432    8388608 15713.862 15882.698 16186.891     2.113     3.169  PASS
//  134217728   33554432 63645.705 65554.758 66452.018     2.047     3.071  PASS
//
// [3] OriginCCL auto, 4 machines x 4 ranks (eRDMA, N=16)
//    2097152     524288  1718.778  1984.422  2890.308     1.057     1.982  PASS
//    8388608    2097152  6620.889  9838.443 12657.879     0.853     1.599  PASS
//   33554432    8388608 22811.109 33285.174 60183.290     1.008     1.890  PASS
//  134217728   33554432 109566.146 161205.341 204351.090     0.833     1.961  PASS
//
// [4] OpenMPI RDMA (openib), 1 machine x 4 ranks (intra-node vader)
//    2097152     524288   884.735   888.441   900.325     2.360     3.541  PASS
//    8388608    2097152  3660.762  3670.809  3680.519     2.285     3.428  PASS
//   33554432    8388608 19767.909 19815.675 19893.264     1.693     2.540  PASS
//  134217728   33554432 76193.498 76601.281 76808.904     1.752     2.628  PASS
//
// [5] OpenMPI RDMA (openib), 2 machines x 2 ranks
//    2097152     524288  1194.348  1197.204  1209.157     1.752     2.628  PASS
//    8388608    2097152  4010.541  4021.321  4042.681     2.086     3.129  PASS
//   33554432    8388608 22965.599 24344.971 25973.899     1.378     2.067  PASS
//  134217728   33554432 83288.627 84335.799 84827.378     1.591     2.387  PASS
//
// [6] OpenMPI RDMA (openib), 4 machines x 4 ranks (N=16)
//    2097152     524288  4066.292  4350.843  4584.972     0.482     0.904  PASS
//    8388608    2097152  8785.185  9119.122  9512.040     0.920     1.725  PASS
//   33554432    8388608 34194.150 36957.595 37767.301     0.908     1.702  PASS
//  134217728   33554432 124676.458 127586.462 130345.609     1.052     1.972  PASS
//
// [7] OriginCCL TCP (OCCL_DISABLE_RDMA=1), 1 machine x 4 ranks (SHM; no cross edge)
//    2097152     524288   250.413   264.740   266.702     7.922    11.882  PASS
//    8388608    2097152  1279.011  1296.377  1306.043     6.471     9.706  PASS
//   33554432    8388608  6879.122  6916.726  7057.352     4.851     7.277  PASS
//  134217728   33554432 26626.401 26940.881 27105.623     4.982     7.473  PASS
//
// [8] OriginCCL TCP, 2 machines x 2 ranks
//    2097152     524288   964.134   971.243   977.544     2.159     3.239  PASS
//    8388608    2097152  3856.913  3882.810  3907.462     2.160     3.241  PASS
//   33554432    8388608 15845.942 15959.526 16080.990     2.102     3.154  PASS
//  134217728   33554432 66737.115 67141.297 67201.337     1.999     2.999  PASS
//
// [9] OriginCCL TCP, 4 machines x 4 ranks (N=16)
//    2097152     524288  1226.951  1239.948  1531.328     1.691     3.171  PASS
//    8388608    2097152  4944.539  5341.312  6591.425     1.571     2.945  PASS
//   33554432    8388608 24120.910 26129.268 30580.848     1.284     2.408  PASS
//  134217728   33554432 86288.079 92857.579 117571.679     1.445     2.710  PASS
//
// [10] OpenMPI TCP, 1 machine x 4 ranks
//    2097152     524288  1481.511  1492.248  1498.313     1.405     2.108  PASS
//    8388608    2097152  5834.086  5852.911  5881.737     1.433     2.150  PASS
//   33554432    8388608 31223.547 31384.520 31516.109     1.069     1.604  PASS
//  134217728   33554432 125045.647 125529.855 126267.542     1.069     1.604  PASS
//
// [11] OpenMPI TCP, 2 machines x 2 ranks
//    2097152     524288  1929.669  1938.095  1958.753     1.082     1.623  PASS
//    8388608    2097152  6992.825  7009.617  7034.906     1.197     1.795  PASS
//   33554432    8388608 33697.475 33835.372 33927.954     0.992     1.488  PASS
//  134217728   33554432 137682.572 137893.872 138190.544     0.973     1.460  PASS
//
// [12] OpenMPI TCP, 4 machines x 4 ranks (N=16)
//    2097152     524288  2978.773  2984.548  2994.112     0.703     1.318  PASS
//    8388608    2097152  9813.338  9858.246  9872.529     0.851     1.595  PASS
//   33554432    8388608 48908.317 48993.534 49117.380     0.685     1.284  PASS
//  134217728   33554432 204094.848 204665.778 211010.460     0.656     1.230  PASS
//
namespace {
#ifdef OCCL_BENCH_OMPI
constexpr const char* kBenchName = "allreduce_ompi_benchmark";
#else
constexpr const char* kBenchName = "allreduce_benchmark";
#endif

struct Options {
    uint64_t min_bytes = 256ULL * 1024 * 1024;
    uint64_t max_bytes = 256ULL * 1024 * 1024;
    uint64_t factor = 2;
    uint64_t target_bytes = 40ULL * 1024 * 1024 * 1024;
    int warmup = 5;
    int repeats = 1;
    int channels = 4;
};

// The timed window moves at least target_bytes, so small messages are bounded by a call cap
// rather than a fixed count: the point is to avoid a burst-sized window on the cloud NIC.
constexpr uint64_t kMaxCallsPerBatch = 16384;

enum class ParseResult : int {
    Error = -1,
    Run = 0,
    Help = 1
};

bool ParseUnsigned(const std::string& text, uint64_t& value) {
    if (text.empty() || text.front() == '-') {
        return false;
    }
    size_t suffix = 0;
    while (suffix < text.size() && text[suffix] >= '0' && text[suffix] <= '9') {
        ++suffix;
    }
    if (suffix == 0) {
        return false;
    }
    uint64_t multiplier = 1;
    if (suffix < text.size()) {
        if (suffix + 1 != text.size()) {
            return false;
        }
        switch (text[suffix]) {
        case 'K':
        case 'k':
            multiplier = 1024;
            break;
        case 'M':
        case 'm':
            multiplier = 1024ULL * 1024;
            break;
        case 'G':
        case 'g':
            multiplier = 1024ULL * 1024 * 1024;
            break;
        default:
            return false;
        }
    }
    errno = 0;
    char* end = nullptr;
    const unsigned long long parsed = std::strtoull(text.c_str(), &end, 10);
    if (errno == ERANGE || end != text.c_str() + suffix || parsed > std::numeric_limits<uint64_t>::max() / multiplier) {
        return false;
    }
    value = static_cast<uint64_t>(parsed) * multiplier;
    return true;
}

bool ParseOptions(int argc, char** argv, Options& options, ParseResult& result, std::string& error) {
    for (int i = 1; i < argc; ++i) {
        const std::string key = argv[i];
        if (key == "--help" || key == "-h") {
            result = ParseResult::Help;
            return true;
        }
        if (i + 1 >= argc) {
            error = fmt::format("missing value for {}", key);
            return false;
        }
        const std::string value = argv[++i];
        uint64_t parsed = 0;
        if (key == "--min-bytes" || key == "--max-bytes" || key == "--factor" || key == "--target-bytes" ||
            key == "--warmup" || key == "--repeats" || key == "--channels") {
            if (!ParseUnsigned(value, parsed)) {
                error = fmt::format("invalid value '{}' for {}", value, key);
                return false;
            }
            if (key == "--min-bytes") {
                options.min_bytes = parsed;
            } else if (key == "--max-bytes") {
                options.max_bytes = parsed;
            } else if (key == "--factor") {
                options.factor = parsed;
            } else if (key == "--target-bytes") {
                options.target_bytes = parsed;
            } else if (key == "--warmup") {
                if (parsed > static_cast<uint64_t>(std::numeric_limits<int>::max())) {
                    error = "--warmup is too large";
                    return false;
                }
                options.warmup = static_cast<int>(parsed);
            } else if (key == "--repeats") {
                if (parsed > static_cast<uint64_t>(std::numeric_limits<int>::max())) {
                    error = "--repeats is too large";
                    return false;
                }
                options.repeats = static_cast<int>(parsed);
            } else {
                if (parsed > static_cast<uint64_t>(std::numeric_limits<int>::max())) {
                    error = "--channels is too large";
                    return false;
                }
                options.channels = static_cast<int>(parsed);
            }
        } else {
            error = fmt::format("unknown option {}", key);
            return false;
        }
    }
    if (options.min_bytes == 0 || options.max_bytes < options.min_bytes || options.factor < 2 ||
        options.target_bytes == 0 || options.min_bytes % sizeof(float) != 0 ||
        options.max_bytes % sizeof(float) != 0 || options.repeats <= 0 || options.channels <= 0) {
        error = "require positive 4-byte-aligned sizes, max >= min, factor >= 2, target-bytes/repeats/channels > 0";
        return false;
    }
    result = ParseResult::Run;
    return true;
}

void PrintUsage(const char* program) {
    fmt::print("Usage: {} [options]\n"
               "  --min-bytes N   first per-rank message size (default 256M)\n"
               "  --max-bytes N   maximum per-rank message size (default 256M)\n"
               "  --factor N      size multiplier, integer >= 2 (default 2)\n"
               "  --target-bytes N  payload bytes moved per size, sets the call count (default 4G)\n"
               "  --warmup N      untimed warmup calls (default 5)\n"
               "  --repeats N     timed batches per size (default 1)\n"
#ifdef OCCL_BENCH_OMPI
               "  --channels N    accepted and ignored (MPI backend has no channels)\n"
#else
               "  --channels N    communicator channel count (default 4)\n"
#endif
               "Sizes accept B (omitted), K, M, or G binary units.\n",
               program);
}

std::vector<uint64_t> MakeSizes(const Options& options) {
    std::vector<uint64_t> sizes;
    for (uint64_t bytes = options.min_bytes;;) {
        sizes.push_back(bytes);
        if (bytes > options.max_bytes / options.factor) {
            break;
        }
        bytes *= options.factor;
        if (bytes > options.max_bytes) {
            break;
        }
    }
    return sizes;
}

float InputValue(int rank, size_t index) {
    return static_cast<float>((rank % 8) * 13 + static_cast<int>(index % 17)) * 0.125f;
}

bool ValidateOutput(const std::vector<float>& output, size_t count, int world_size, std::string& error) {
    for (size_t i = 0; i < count; ++i) {
        float expected = 0.0f;
        for (int rank = 0; rank < world_size; ++rank) {
            expected += InputValue(rank, i);
        }
        if (!std::isfinite(output[i]) ||
            std::fabs(output[i] - expected) > 1e-5f * std::max(1.0f, std::fabs(expected))) {
            error = fmt::format("output mismatch at element {}: expected {}, got {}", i, expected, output[i]);
            return false;
        }
    }
    return true;
}

[[noreturn]] void AbortAll(int rank, const std::string& error) {
    LOG_ERROR("{} rank {}: {}", kBenchName, rank, error);
    MPI_Abort(MPI_COMM_WORLD, 1);
    std::abort();
}

bool BenchAllReduce(Communicator& comm, const float* input, float* output, size_t count) {
#ifdef OCCL_BENCH_OMPI
    (void)comm;
    if (count > static_cast<size_t>(std::numeric_limits<int>::max())) {
        return false;
    }
    return MPI_Allreduce(input, output, static_cast<int>(count), MPI_FLOAT, MPI_SUM, MPI_COMM_WORLD) == MPI_SUCCESS;
#else
    return comm.AllReduce(input, output, count, DataType::FLOAT32, ReduceOp::SUM);
#endif
}

bool ValidateOnAllRanks(const std::vector<float>& output, size_t count, int world_size, int rank) {
    std::string error;
    int local_ok = ValidateOutput(output, count, world_size, error) ? 1 : 0;
    int all_ok = 0;
    MPI_Allreduce(&local_ok, &all_ok, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);
    if (!all_ok) {
        AbortAll(rank, local_ok ? "another rank failed output validation" : error);
    }
    return true;
}

void PrintMetadata(const Options& options, const Communicator& comm, int world_size, int nodes) {
#ifndef OCCL_BENCH_OMPI
    fmt::print("OriginCCL AllReduce benchmark | ranks={} machines={} channels={} (actual={})\n", world_size, nodes,
               options.channels, comm.GetNChannels());
    fmt::print("dtype=float32 op=sum mode=out-of-place warmup={} repeats={} target_bytes={} build={} chunk_bytes={}\n",
               options.warmup, options.repeats, options.target_bytes, OCCL_BENCH_BUILD_TYPE, OcclConfig::kChunkBytes);
#else
    (void)comm;
    fmt::print("OpenMPI AllReduce benchmark | ranks={} nodes={} impl=MPI_Allreduce\n", world_size, nodes);
    fmt::print("dtype=float32 op=sum mode=out-of-place warmup={} repeats={} target_bytes={} build={}\n", options.warmup,
               options.repeats, options.target_bytes, OCCL_BENCH_BUILD_TYPE);
#endif
    fmt::print("{:>10} {:>10} {:>9} {:>9} {:>9} {:>9} {:>9} {:>5}\n", "bytes", "count", "min_us", "median_us", "max_us",
               "algGB/s", "busGB/s", "check");
}

void RunSize(Communicator& comm, const Options& options, uint64_t bytes, int rank, int world_size) {
    const size_t count = static_cast<size_t>(bytes / sizeof(float));
    const uint64_t per_repeat_target = options.target_bytes / static_cast<uint64_t>(options.repeats);
    uint64_t calls = (per_repeat_target + bytes - 1) / bytes;
    calls = std::min(std::max(calls, static_cast<uint64_t>(1)), kMaxCallsPerBatch);
    const int iterations = static_cast<int>(calls);
    std::vector<float> input;
    std::vector<float> output;
    int local_allocated = 1;
    try {
        input.resize(count);
        output.resize(count, std::numeric_limits<float>::quiet_NaN());
    } catch (const std::exception&) {
        local_allocated = 0;
    }
    int all_allocated = 0;
    MPI_Allreduce(&local_allocated, &all_allocated, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);
    if (!all_allocated) {
        AbortAll(rank, "unable to allocate benchmark buffers on every rank");
    }
    for (size_t i = 0; i < count; ++i) {
        input[i] = InputValue(rank, i);
    }

    if (!BenchAllReduce(comm, input.data(), output.data(), count)) {
        AbortAll(rank, "AllReduce failed during validation preflight");
    }
    ValidateOnAllRanks(output, count, world_size, rank);
    for (int i = 0; i < options.warmup; ++i) {
        if (!BenchAllReduce(comm, input.data(), output.data(), count)) {
            AbortAll(rank, "AllReduce failed during warmup");
        }
    }
    ValidateOnAllRanks(output, count, world_size, rank);

    std::vector<double> batch_seconds;
    int samples_allocated = 1;
    try {
        if (rank == 0) {
            batch_seconds.resize(static_cast<size_t>(options.repeats));
        }
    } catch (const std::exception&) {
        samples_allocated = 0;
    }
    int all_samples_allocated = 0;
    MPI_Allreduce(&samples_allocated, &all_samples_allocated, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);
    if (!all_samples_allocated) {
        AbortAll(rank, "unable to allocate timing samples");
    }
    for (int repeat = 0; repeat < options.repeats; ++repeat) {
        MPI_Barrier(MPI_COMM_WORLD);
        const double start = MPI_Wtime();
        for (int iteration = 0; iteration < iterations; ++iteration) {
            if (!BenchAllReduce(comm, input.data(), output.data(), count)) {
                AbortAll(rank, "AllReduce failed during timed measurement");
            }
        }
        const double elapsed = MPI_Wtime() - start;
        double slowest_elapsed = 0.0;
        MPI_Reduce(&elapsed, &slowest_elapsed, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        if (rank == 0) {
            batch_seconds[static_cast<size_t>(repeat)] = slowest_elapsed / iterations;
        }
        ValidateOnAllRanks(output, count, world_size, rank);
    }

    if (rank == 0) {
        std::sort(batch_seconds.begin(), batch_seconds.end());
        const double median =
            batch_seconds.size() % 2 == 0
                ? (batch_seconds[batch_seconds.size() / 2 - 1] + batch_seconds[batch_seconds.size() / 2]) / 2.0
                : batch_seconds[batch_seconds.size() / 2];
        const double min_us = batch_seconds.front() * 1e6;
        const double median_us = median * 1e6;
        const double max_us = batch_seconds.back() * 1e6;
        const double algbw = static_cast<double>(bytes) / median / 1e9;
        const double busbw = algbw * (2.0 * (world_size - 1) / world_size);
        fmt::print("{:>10} {:>10} {:>9.3f} {:>9.3f} {:>9.3f} {:>9.3f} {:>9.3f} {:>5}\n", bytes, count, min_us,
                   median_us, max_us, algbw, busbw, "PASS");
    }
}
} // namespace

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);
    int rank = 0;
    int world_size = 1;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    Logger::Instance().SetLogLevel(LogLevel::WARN);

    Options options;
    ParseResult parse_result = ParseResult::Run;
    std::string parse_error;
    if (rank == 0 && !ParseOptions(argc, argv, options, parse_result, parse_error)) {
        parse_result = ParseResult::Error;
    }
    int parse_status = static_cast<int>(parse_result);
    MPI_Bcast(&parse_status, 1, MPI_INT, 0, MPI_COMM_WORLD);
    if (parse_status != static_cast<int>(ParseResult::Run)) {
        if (rank == 0) {
            if (parse_status == static_cast<int>(ParseResult::Help)) {
                PrintUsage(argv[0]);
            } else {
                LOG_ERROR("{}: {}", kBenchName, parse_error);
                PrintUsage(argv[0]);
            }
        }
        MPI_Finalize();
        return parse_status == static_cast<int>(ParseResult::Help) ? 0 : 4;
    }

    uint64_t wire_options[] = {options.min_bytes,
                               options.max_bytes,
                               options.factor,
                               options.target_bytes,
                               static_cast<uint64_t>(options.warmup),
                               static_cast<uint64_t>(options.repeats),
                               static_cast<uint64_t>(options.channels)};
    MPI_Bcast(wire_options, 7, MPI_UINT64_T, 0, MPI_COMM_WORLD);
    options.min_bytes = wire_options[0];
    options.max_bytes = wire_options[1];
    options.factor = wire_options[2];
    options.target_bytes = wire_options[3];
    options.warmup = static_cast<int>(wire_options[4]);
    options.repeats = static_cast<int>(wire_options[5]);
    options.channels = static_cast<int>(wire_options[6]);

    if (world_size < 2) {
        if (rank == 0) {
            LOG_ERROR("{} requires at least 2 MPI ranks; use mpirun -np N", kBenchName);
        }
        MPI_Finalize();
        return 4;
    }

    Communicator comm;
    int nodes = 1;
#ifndef OCCL_BENCH_OMPI
    CommConfig config;
    config.rank = rank;
    config.world_size = world_size;
    config.n_channels = options.channels;
    if (!TestCommon::InitCommunicator(comm, config, rank, world_size)) {
        AbortAll(rank, "communicator initialization failed");
    }
    nodes = comm.GetMachineCount();
#else
    int local_rank = 0;
    MPI_Comm node_comm = MPI_COMM_NULL;
    if (MPI_Comm_split_type(MPI_COMM_WORLD, MPI_COMM_TYPE_SHARED, 0, MPI_INFO_NULL, &node_comm) == MPI_SUCCESS) {
        MPI_Comm_rank(node_comm, &local_rank);
    }
    int node_leaders = local_rank == 0 ? 1 : 0;
    MPI_Allreduce(&node_leaders, &nodes, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
    Utils::PinProcessToCpu(local_rank);
#endif

    if (rank == 0) {
        PrintMetadata(options, comm, world_size, nodes);
    }
    for (const uint64_t bytes : MakeSizes(options)) {
        RunSize(comm, options, bytes, rank, world_size);
    }
#ifndef OCCL_BENCH_OMPI
    comm.Finalize();
#else
    if (node_comm != MPI_COMM_NULL) {
        MPI_Comm_free(&node_comm);
    }
#endif
    MPI_Finalize();
    return 0;
}