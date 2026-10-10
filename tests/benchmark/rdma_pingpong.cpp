#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <vector>
#include <mpi.h>
#include <fmt/format.h>
#include "transport/transport.h"
#include "transport/transport_rdma.h"

using Clock = std::chrono::steady_clock;

namespace {
std::shared_ptr<TransportRDMA> ConnectOne(int rank, bool rank0_sends, int tag, const std::string& address) {
    const TransportDirection direction =
        (rank == 0) == rank0_sends ? TransportDirection::Send : TransportDirection::Receive;
    if (rank == 0) {
        auto listener = std::make_shared<TransportRDMA>();
        if (!listener->Listen(address, 0)) {
            return nullptr;
        }
        const uint16_t port = listener->GetListenPort();
        MPI_Send(&port, 1, MPI_UNSIGNED_SHORT, 1, tag, MPI_COMM_WORLD);
        auto connection = std::dynamic_pointer_cast<TransportRDMA>(listener->Accept());
        listener->Close();
        if (!connection) {
            return nullptr;
        }
        connection->SetDirection(direction);
        return connection;
    }
    uint16_t port = 0;
    MPI_Recv(&port, 1, MPI_UNSIGNED_SHORT, 0, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    auto connection = std::make_shared<TransportRDMA>();
    connection->SetDirection(direction);
    if (!connection->Connect(address, port)) {
        return nullptr;
    }
    return connection;
}

bool DriveSend(TransportRDMA& transport, const char* data, size_t size) {
    size_t progress = 0;
    bool done = false;
    while (!done) {
        if (!transport.TrySend(data, size, &progress, &done)) {
            return false;
        }
    }
    return true;
}

bool DriveRecv(TransportRDMA& transport, char* data, size_t size) {
    size_t progress = 0;
    bool done = false;
    while (!done) {
        if (!transport.TryRecv(data, size, &progress, &done)) {
            return false;
        }
    }
    return true;
}
} // namespace

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);
    int rank = 0;
    int world_size = 1;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    if (world_size != 2) {
        MPI_Finalize();
        return 1;
    }

    const uint64_t iterations = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 1000000;
    const size_t size = 4096;

    std::string address;
    if (!TransportRDMA::Probe(address)) {
        MPI_Finalize();
        return 2;
    }
    char address_buffer[64] = {};
    if (rank == 0) {
        std::snprintf(address_buffer, sizeof(address_buffer), "%s", address.c_str());
    }
    MPI_Bcast(address_buffer, sizeof(address_buffer), MPI_CHAR, 0, MPI_COMM_WORLD);
    const std::string peer_address(address_buffer);

    std::vector<char> outgoing(size, 1);
    std::vector<char> incoming(size, 0);

    // One connection per direction: rank 0 sends on a and receives on b, rank 1 the reverse.
    auto a = ConnectOne(rank, true, 11, peer_address);
    auto b = ConnectOne(rank, false, 12, peer_address);
    if (!a || !b) {
        MPI_Finalize();
        return 1;
    }
    a->SetWaitMode(TransportWaitMode::Polling);
    b->SetWaitMode(TransportWaitMode::Polling);

    const auto round_trip = [&]() {
        if (rank == 0) {
            return DriveSend(*a, outgoing.data(), size) && DriveRecv(*b, incoming.data(), size);
        }
        return DriveRecv(*a, incoming.data(), size) && DriveSend(*b, outgoing.data(), size);
    };

    for (int i = 0; i < 200; ++i) {
        if (!round_trip()) {
            MPI_Abort(MPI_COMM_WORLD, 1);
            return 1;
        }
    }
    MPI_Barrier(MPI_COMM_WORLD);

    const auto start = Clock::now();
    double window_min = 0.0;
    double window_max = 0.0;
    double window_sum = 0.0;
    uint64_t window_count = 0;
    double total_min = 0.0;
    double total_max = 0.0;
    const auto record = [&](double sample_us) {
        if (window_count == 0 || sample_us < window_min) {
            window_min = sample_us;
        }
        if (window_count == 0 || sample_us > window_max) {
            window_max = sample_us;
        }
        window_sum += sample_us;
        ++window_count;
        if (window_count == 1 || sample_us < total_min) {
            total_min = sample_us;
        }
        if (window_count == 1 || sample_us > total_max) {
            total_max = sample_us;
        }
    };
    for (uint64_t i = 0; i < iterations; ++i) {
        const auto before = Clock::now();
        if (!round_trip()) {
            MPI_Abort(MPI_COMM_WORLD, 1);
            return 1;
        }
        record(std::chrono::duration<double, std::micro>(Clock::now() - before).count());
        if (rank == 0 && window_count == 10000) {
            const double since_us = std::chrono::duration<double, std::micro>(Clock::now() - start).count();
            fmt::print("  {:>4.1f}s  min={:7.2f}  mean={:7.2f}  max={:7.2f} us\n", since_us / 1e6, window_min,
                       window_sum / window_count, window_max);
            std::fflush(stdout);
            window_min = 0.0;
            window_max = 0.0;
            window_sum = 0.0;
            window_count = 0;
        }
    }
    const double total_us = std::chrono::duration<double, std::micro>(Clock::now() - start).count();
    if (rank == 0) {
        fmt::print("RESULT iterations={} min_RTT={:.2f} mean_RTT={:.2f} max_RTT={:.2f} us\n", iterations, total_min,
                   total_us / iterations, total_max);
    }

    a->Close();
    b->Close();
    MPI_Finalize();
    return 0;
}
