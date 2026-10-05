#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <poll.h>
#include <string>
#include <vector>
#include <fmt/format.h>
#include <mpi.h>
#include "logger.h"
#include "transport/transport.h"
#include "transport/transport_rdma.h"
#include "transport/transport_rdma_zc.h"

namespace {
constexpr int kWaitMs = 10000;
constexpr int kMaxIterations = 400000;
constexpr int kWarmups = 3;
constexpr uint64_t kBytesPerCase = 45ULL * 1000 * 1000 * 1000;
constexpr size_t kBufferSize = 48 * 1024 * 1024;
constexpr uint32_t kControlMagic = 0x4f43434c;
constexpr size_t kMessageSizes[] = {32 * 1024, 64 * 1024, 128 * 1024, 256 * 1024, 512 * 1024,
                                    1 * 1024 * 1024, 5 * 1024 * 1024, 16 * 1024 * 1024 - 1,
                                    16 * 1024 * 1024, 17 * 1024 * 1024, 32 * 1024 * 1024,
                                    48 * 1024 * 1024};

struct ControlMessage {
    uint32_t magic;
    int32_t role;
};

struct TransferCounts {
    uint64_t calls = 0;
    uint64_t waits = 0;
};

using Clock = std::chrono::steady_clock;

bool Fail(int rank, const char* step, const std::string& detail) {
    LOG_ERROR("rdma_zc_benchmark rank {}: {} ({})", rank, step, detail);
    return false;
}

void FillPattern(char* data, size_t size) {
    for (size_t i = 0; i < size; ++i) {
        data[i] = static_cast<char>((i * 31 + 7) & 0xFF);
    }
}

bool CheckPattern(const char* data, size_t size, size_t* bad_index) {
    for (size_t i = 0; i < size; ++i) {
        if (data[i] != static_cast<char>((i * 31 + 7) & 0xFF)) {
            *bad_index = i;
            return false;
        }
    }
    return true;
}

bool WaitReady(Transport& transport) {
    pollfd entry{transport.GetFd(), static_cast<short>(transport.GetPollEvents()), 0};
    int ready = 0;
    do {
        ready = poll(&entry, 1, kWaitMs);
    } while (ready < 0 && errno == EINTR);
    return ready > 0;
}

bool Transfer(TransportRDMAZc& transport, int rank, bool producer, bool zero_copy, const char* payload,
              char* received, size_t size, TransferCounts* counts) {
    size_t progress = 0;
    bool done = false;
    for (int iterations = 0; !done; ++iterations) {
        if (iterations >= kMaxIterations) {
            return Fail(rank, "transfer iteration limit reached", std::to_string(progress));
        }
        const size_t before = progress;
        ++counts->calls;
        const bool ok = zero_copy
                    ? (producer ? transport.TrySend(payload, size, &progress, &done)
                        : transport.TryRecv(received, size, &progress, &done))
                    : (producer ? transport.TransportRDMA::TrySend(payload, size, &progress, &done)
                        : transport.TransportRDMA::TryRecv(received, size, &progress, &done));
        if (!ok) {
            return Fail(rank, "TrySend/TryRecv failed", std::to_string(size));
        }
        if (progress < before || progress > size || done != (progress == size)) {
            return Fail(rank, "invalid progress or done state", std::to_string(progress));
        }
        if (!done && progress == before) {
            ++counts->waits;
            if (!WaitReady(transport)) {
                return Fail(rank, "timed out waiting for RDMA readiness", std::to_string(size));
            }
        }
    }
    return true;
}

std::shared_ptr<TransportRDMAZc> Connect(int rank, bool zero_copy, int tag, const std::string& address) {
    const size_t threshold = zero_copy ? 0 : kRdmaZcThreshold;
    if (rank == 0) {
        auto listener = std::make_shared<TransportRDMAZc>(threshold);
        if (!listener->Listen(address, 0)) {
            Fail(rank, "listen failed", address);
            return nullptr;
        }
        const uint16_t port = listener->GetListenPort();
        MPI_Send(&port, 1, MPI_UNSIGNED_SHORT, 1, tag, MPI_COMM_WORLD);
        auto connection = std::dynamic_pointer_cast<TransportRDMAZc>(listener->Accept());
        listener->Close();
        if (!connection) {
            Fail(rank, "accept failed", "");
            return nullptr;
        }
        connection->SetDirection(TransportDirection::Receive);
        ControlMessage message{};
        if (!connection->Recv(&message, sizeof(message))) {
            Fail(rank, "control receive failed", "");
            return nullptr;
        }
        if (message.magic != kControlMagic || message.role != 1) {
            Fail(rank, "control message mismatch", "");
            return nullptr;
        }
        return connection;
    }

    uint16_t port = 0;
    MPI_Recv(&port, 1, MPI_UNSIGNED_SHORT, 0, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    auto connection = std::make_shared<TransportRDMAZc>(threshold);
    connection->SetDirection(TransportDirection::Send);
    if (!connection->Connect(address, port)) {
        Fail(rank, "connect failed", address);
        return nullptr;
    }
    ControlMessage message{kControlMagic, rank};
    if (!connection->Send(&message, sizeof(message))) {
        Fail(rank, "control send failed", "");
        return nullptr;
    }
    return connection;
}

bool RunSize(TransportRDMAZc& transport, int rank, bool zero_copy, size_t size, const char* payload,
             char* received) {
    TransferCounts warmup_counts;
    for (int iteration = 0; iteration < kWarmups; ++iteration) {
        if (!Transfer(transport, rank, rank == 1, zero_copy, payload, received, size, &warmup_counts)) {
            return false;
        }
        if (rank == 0) {
            size_t bad_index = 0;
            if (!CheckPattern(received, size, &bad_index)) {
                return Fail(rank, "warmup payload mismatch", std::to_string(bad_index));
            }
        }
    }
    MPI_Barrier(MPI_COMM_WORLD);

    TransferCounts timed_counts;
    uint64_t remaining_bytes = kBytesPerCase;
    uint64_t message_count = 0;
    // The ECS link bursts above its sustained rate until the credits drain, so the first half of
    // every case is transferred untimed; the table reports the sustained rate, not a burst average.
    const uint64_t timed_bytes = kBytesPerCase - kBytesPerCase / 2;
    while (remaining_bytes > timed_bytes) {
        const size_t transfer_size = static_cast<size_t>(std::min<uint64_t>(size, remaining_bytes - timed_bytes));
        if (!Transfer(transport, rank, rank == 1, zero_copy, payload, received, transfer_size, &timed_counts)) {
            return false;
        }
        remaining_bytes -= transfer_size;
    }
    const auto start = Clock::now();
    while (remaining_bytes > 0) {
        const size_t transfer_size = static_cast<size_t>(std::min<uint64_t>(size, remaining_bytes));
        if (!Transfer(transport, rank, rank == 1, zero_copy, payload, received, transfer_size, &timed_counts)) {
            return false;
        }
        remaining_bytes -= transfer_size;
        ++message_count;
    }
    const uint64_t elapsed_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start).count());

    uint64_t gathered_elapsed_ns[2] = {};
    MPI_Gather(&elapsed_ns, 1, MPI_UINT64_T, gathered_elapsed_ns, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);
    const uint64_t local_counts[] = {timed_counts.calls, timed_counts.waits};
    uint64_t total_counts[2] = {};
    MPI_Reduce(local_counts, total_counts, 2, MPI_UINT64_T, MPI_SUM, 0, MPI_COMM_WORLD);

    if (rank == 0) {
        const double receiver_seconds = static_cast<double>(gathered_elapsed_ns[0]) / 1e9;
        const double sender_seconds = static_cast<double>(gathered_elapsed_ns[1]) / 1e9;
        const double gigabytes_per_second = static_cast<double>(timed_bytes) / receiver_seconds / 1e9;
        fmt::print("{:<4} {:>10} {:>12} {:>8.3f} {:>9.2f} {:>9.2f} {:>9} {:>7} {:>5}\n",
                   zero_copy ? "ZC" : "copy", size, timed_bytes, gigabytes_per_second,
                   receiver_seconds, sender_seconds, message_count, total_counts[0], total_counts[1]);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    return true;
}

bool RunMode(int rank, bool zero_copy, int tag, const std::string& address, std::vector<char>& payload,
             std::vector<char>& received) {
    auto transport = Connect(rank, zero_copy, tag, address);
    if (!transport) {
        return false;
    }
    for (const size_t size : kMessageSizes) {
        if (!RunSize(*transport, rank, zero_copy, size, payload.data(), received.data())) {
            return false;
        }
    }
    transport->Close();
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
        if (rank == 0) {
            LOG_ERROR("rdma_zc_benchmark: expected exactly two ranks, got {}", world_size);
        }
        MPI_Finalize();
        return 1;
    }

    std::string address;
    int local_device_ok = TransportRDMA::Probe(address) ? 1 : 0;
    int all_devices_ok = 0;
    MPI_Allreduce(&local_device_ok, &all_devices_ok, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);
    if (!all_devices_ok) {
        if (rank == 0) {
            LOG_INFO("rdma_zc_benchmark: no active RDMA port found; skipping");
        }
        MPI_Finalize();
        return 2;
    }

    char address_buffer[64] = {};
    if (rank == 0) {
        std::snprintf(address_buffer, sizeof(address_buffer), "%s", address.c_str());
    }
    MPI_Bcast(address_buffer, sizeof(address_buffer), MPI_CHAR, 0, MPI_COMM_WORLD);

    std::vector<char> payload(kBufferSize);
    std::vector<char> received(kBufferSize);
    FillPattern(payload.data(), payload.size());

    if (rank == 0) {
        fmt::print("RDMA-ZC strategy benchmark: address={} warmups={} payload_per_case={} GB\n", address_buffer,
                   kWarmups, kBytesPerCase / 1000 / 1000 / 1000);
        fmt::print("{:<4} {:>10} {:>12} {:>8} {:>9} {:>9} {:>9} {:>7} {:>5}\n", "mode", "size", "bytes",
                   "GB/s", "recv_s", "send_s", "messages", "calls", "waits");
    }

    if (!RunMode(rank, false, 41, address_buffer, payload, received) ||
        !RunMode(rank, true, 42, address_buffer, payload, received)) {
        MPI_Abort(MPI_COMM_WORLD, 1);
        return 1;
    }

    if (rank == 0) {
        LOG_INFO("rdma_zc_benchmark: payload validation passed");
    }
    MPI_Finalize();
    return 0;
}