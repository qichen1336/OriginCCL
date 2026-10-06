#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>
#include <fmt/format.h>
#include <mpi.h>
#include "communicator.h"
#include "logger.h"
#include "occl_config.h"
#include "test_common.h"

namespace {

struct Case {
    size_t bytes = 0;
};

struct Profile {
    const char* name = "";
    std::vector<Case> cases;
    size_t peak_bytes = 0;
    long long total_bytes = 0;
};

std::vector<Profile> MakeProfiles() {
    std::vector<Profile> profiles;

    Profile balanced;
    balanced.name = "balanced";
    for (int i = 0; i < 4; ++i) {
        balanced.cases.push_back(Case{4u << 20});
    }
    balanced.peak_bytes = 4u << 20;
    for (const Case& item : balanced.cases) {
        balanced.total_bytes += static_cast<long long>(item.bytes);
    }
    profiles.push_back(std::move(balanced));

    Profile imbalanced;
    imbalanced.name = "imbalanced";
    for (int i = 0; i < 8; ++i) {
        imbalanced.cases.push_back(Case{256u << 10});
    }
    imbalanced.cases.push_back(Case{1u << 20});
    imbalanced.cases.push_back(Case{1u << 20});
    imbalanced.cases.push_back(Case{4u << 20});
    imbalanced.peak_bytes = 4u << 20;
    for (const Case& item : imbalanced.cases) {
        imbalanced.total_bytes += static_cast<long long>(item.bytes);
    }
    profiles.push_back(std::move(imbalanced));

    return profiles;
}

void AbortAll(int rank, const std::string& message) {
    if (rank == 0) {
        LOG_ERROR("group_benchmark: {}", message);
    }
    MPI_Abort(MPI_COMM_WORLD, 1);
}

double Median(std::vector<double>& samples) {
    std::sort(samples.begin(), samples.end());
    const size_t n = samples.size();
    if (n % 2 == 0) {
        return (samples[n / 2 - 1] + samples[n / 2]) / 2.0;
    }
    return samples[n / 2];
}

int ParseInt(const char* text, int fallback) {
    if (!text) {
        return fallback;
    }
    char* end = nullptr;
    const long value = std::strtol(text, &end, 10);
    if (end == text || value <= 0 || value > 1000000) {
        return fallback;
    }
    return static_cast<int>(value);
}

void RunProfile(Communicator& comm, const Profile& profile, int repeats, int rank) {
    std::vector<float> send(profile.peak_bytes / sizeof(float));
    std::vector<float> recv(profile.peak_bytes / sizeof(float));
    for (size_t i = 0; i < send.size(); ++i) {
        send[i] = static_cast<float>(rank + 1);
    }

    std::vector<double> seconds;
    if (rank == 0) {
        seconds.resize(static_cast<size_t>(repeats));
    }

    for (int repeat = 0; repeat < repeats; ++repeat) {
        MPI_Barrier(MPI_COMM_WORLD);
        const double start = MPI_Wtime();
        comm.GroupStart();
        for (const Case& item : profile.cases) {
            const size_t count = item.bytes / sizeof(float);
            if (!comm.AllReduce(send.data(), recv.data(), count, DataType::FLOAT32, ReduceOp::SUM)) {
                AbortAll(rank, "AllReduce failed inside a group");
            }
        }
        if (!comm.GroupEnd()) {
            AbortAll(rank, "GroupEnd failed");
        }
        const double elapsed = MPI_Wtime() - start;
        double slowest = 0.0;
        MPI_Reduce(&elapsed, &slowest, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        if (rank == 0) {
            seconds[static_cast<size_t>(repeat)] = slowest;
        }
    }

    if (rank == 0) {
        const double median = Median(seconds) * 1e3;
        const double best = seconds.front() * 1e3;
        const double worst = seconds.back() * 1e3;
        const double throughput =
            static_cast<double>(profile.total_bytes) / (median / 1e3) / 1e9;
        fmt::print("{:<12} {:>4} {:>12} {:>12.3f} {:>12.3f} {:>12.3f} {:>10.3f}\n", profile.name, repeats,
                   profile.total_bytes, best, median, worst, throughput);
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

    int repeats = 5;
    int channels = 4;
    for (int i = 1; i < argc; ++i) {
        const std::string key = argv[i];
        if ((key == "--repeats" || key == "--channels") && i + 1 < argc) {
            if (key == "--repeats") {
                repeats = ParseInt(argv[++i], repeats);
            } else {
                channels = ParseInt(argv[++i], channels);
            }
        }
    }

    if (world_size < 2) {
        if (rank == 0) {
            LOG_ERROR("group_benchmark requires at least 2 MPI ranks; use mpirun -np N");
        }
        MPI_Finalize();
        return 4;
    }

    Communicator comm;
    CommConfig config;
    config.rank = rank;
    config.world_size = world_size;
    config.n_channels = channels;
    if (!TestCommon::InitCommunicator(comm, config, rank, world_size)) {
        AbortAll(rank, "communicator initialization failed");
    }

    if (rank == 0) {
        const char* executor = std::getenv("OCCL_EXECUTOR");
        fmt::print("group_benchmark: world_size={} channels={} executor={}\n", world_size, comm.GetNChannels(),
                   executor ? executor : "auto");
        fmt::print("{:<12} {:>4} {:>12} {:>12} {:>12} {:>12} {:>10}\n", "profile", "rep", "bytes", "best_ms",
                   "median_ms", "worst_ms", "GB/s");
    }

    for (const Profile& profile : MakeProfiles()) {
        RunProfile(comm, profile, repeats, rank);
    }

    comm.Finalize();
    MPI_Finalize();
    return 0;
}
