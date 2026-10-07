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
constexpr int kSmallCaseCalibrationMessages = 128;
constexpr uint64_t kSmallCaseDurationNs = 3ULL * 1000 * 1000 * 1000;
constexpr uint64_t kBytesPerCase = 45ULL * 1000 * 1000 * 1000;
constexpr uint64_t kSmallCaseWarmupBytes = 3200000;
constexpr uint64_t kPipelineDepth = 8;
constexpr size_t kBufferSize = 48 * 1024 * 1024;
constexpr uint32_t kControlMagic = 0x4f43434c;
constexpr size_t kMessageSizes[] = {32 * 1024, 128 * 1024, 512 * 1024, 2 * 1024 * 1024,
                                    8 * 1024 * 1024, 32 * 1024 * 1024};

struct ControlMessage {
    uint32_t magic;
    int32_t role;
};

struct TransferCounts {
    uint64_t calls = 0;
    uint64_t waits = 0;
};

using Clock = std::chrono::steady_clock;

// The data path is direction gated, so one connection only ever carries one way; the benchmark
// keeps two, one per direction, and each rank owns the one it sends on and the one it receives on.
struct Endpoints {
    std::shared_ptr<TransportRDMAZc> outgoing;
    std::shared_ptr<TransportRDMAZc> incoming;
};

bool Fail(int rank, const char* step, const std::string& detail) {
    LOG_ERROR("rdma_zc_benchmark rank {}: {} ({})", rank, step, detail);
    return false;
}

void FillPattern(char* data, size_t size) {
    for (size_t i = 0; i < size; ++i) {
        data[i] = static_cast<char>((i * 31 + 7) & 0xFF);
    }
}

bool WaitReady(const Endpoints& endpoints, bool send_stalled, bool recv_stalled) {
    pollfd entries[2];
    size_t count = 0;
    if (send_stalled) {
        entries[count++] = pollfd{endpoints.outgoing->GetFd(),
                                  static_cast<short>(endpoints.outgoing->GetPollEvents()), 0};
    }
    if (recv_stalled) {
        entries[count++] = pollfd{endpoints.incoming->GetFd(),
                                  static_cast<short>(endpoints.incoming->GetPollEvents()), 0};
    }
    int ready = 0;
    do {
        ready = poll(entries, count, kWaitMs);
    } while (ready < 0 && errno == EINTR);
    return ready > 0;
}

struct Stream {
    std::shared_ptr<TransportRDMAZc> transport;
    bool producer = false;
    uint64_t budget = 0;
    size_t message_size = 0;
    size_t progress = 0;
    size_t current = 0;
    uint64_t calls = 0;
    uint64_t waits = 0;
    uint64_t messages = 0;
};

bool AdvanceStream(Stream& stream, int rank, bool zero_copy, const char* payload, char* received) {
    if (stream.progress == 0) {
        stream.current = static_cast<size_t>(std::min<uint64_t>(stream.message_size, stream.budget));
    }
    const size_t before = stream.progress;
    ++stream.calls;
    bool done = false;
    const bool ok = zero_copy
                ? (stream.producer ? stream.transport->TrySend(payload, stream.current, &stream.progress, &done)
                    : stream.transport->TryRecv(received, stream.current, &stream.progress, &done))
                : (stream.producer ? stream.transport->TransportRDMA::TrySend(payload, stream.current, &stream.progress, &done)
                    : stream.transport->TransportRDMA::TryRecv(received, stream.current, &stream.progress, &done));
    if (!ok) {
        return Fail(rank, "TrySend/TryRecv failed", std::to_string(stream.current));
    }
    if (stream.progress < before || stream.progress > stream.current || done != (stream.progress == stream.current)) {
        return Fail(rank, "invalid progress or done state", std::to_string(stream.progress));
    }
    if (done) {
        stream.budget -= stream.current;
        stream.progress = 0;
        ++stream.messages;
    }
    return true;
}

// The two directions advance in one loop but share nothing: each owns its transport, byte budget,
// progress and counters, so messages are never paired across directions. A direction stops only
// when its own budget is spent; the loop ends once both budgets are spent. When every unfinished
// direction stalls, the stalled descriptors are polled together.
bool RunBidirectional(const Endpoints& endpoints, int rank, bool zero_copy, const char* payload, char* received,
                      size_t size, uint64_t bytes_per_direction, TransferCounts* counts, uint64_t* messages) {
    Stream outgoing{endpoints.outgoing, true, bytes_per_direction, size};
    Stream incoming{endpoints.incoming, false, bytes_per_direction, size};
    uint64_t idle = 0;
    while (outgoing.budget > 0 || incoming.budget > 0) {
        bool advanced = false;
        bool send_stalled = false;
        bool recv_stalled = false;
        while (outgoing.budget > 0 && outgoing.messages < incoming.messages + kPipelineDepth) {
            const uint64_t budget_before = outgoing.budget;
            const size_t progress_before = outgoing.progress;
            if (!AdvanceStream(outgoing, rank, zero_copy, payload, received)) {
                return false;
            }
            if (outgoing.budget == budget_before && outgoing.progress == progress_before) {
                send_stalled = true;
                break;
            }
            advanced = true;
        }
        while (incoming.budget > 0) {
            const uint64_t budget_before = incoming.budget;
            const size_t progress_before = incoming.progress;
            if (!AdvanceStream(incoming, rank, zero_copy, payload, received)) {
                return false;
            }
            if (incoming.budget == budget_before && incoming.progress == progress_before) {
                recv_stalled = true;
                break;
            }
            advanced = true;
        }
        if (advanced) {
            idle = 0;
            continue;
        }
        if (++idle >= kMaxIterations) {
            return Fail(rank, "bidirectional iteration limit reached", std::to_string(size));
        }
        if (send_stalled) {
            ++outgoing.waits;
        }
        if (recv_stalled) {
            ++incoming.waits;
        }
        if (!WaitReady(endpoints, send_stalled, recv_stalled)) {
            return Fail(rank, "timed out waiting for RDMA readiness", std::to_string(size));
        }
    }
    counts->calls += outgoing.calls + incoming.calls;
    counts->waits += outgoing.waits + incoming.waits;
    *messages = outgoing.messages;
    return true;
}

std::shared_ptr<TransportRDMAZc> ConnectOne(int rank, bool zero_copy, bool rank0_sends, int tag,
                                            const std::string& address) {
    const size_t threshold = zero_copy ? 0 : kRdmaZcThreshold;
    const TransportDirection direction =
        (rank == 0) == rank0_sends ? TransportDirection::Send : TransportDirection::Receive;
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
        connection->SetDirection(direction);
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
    connection->SetDirection(direction);
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

bool Connect(int rank, bool zero_copy, int tag, const std::string& address, Endpoints& endpoints) {
    auto from_rank0 = ConnectOne(rank, zero_copy, true, tag, address);
    auto from_rank1 = ConnectOne(rank, zero_copy, false, tag + 1, address);
    if (!from_rank0 || !from_rank1) {
        return false;
    }
    endpoints.outgoing = rank == 0 ? from_rank0 : from_rank1;
    endpoints.incoming = rank == 0 ? from_rank1 : from_rank0;
    return true;
}

bool RunSize(const Endpoints& endpoints, int rank, bool zero_copy, size_t size, const char* payload,
             char* received) {
    TransferCounts warmup_counts;
    uint64_t unused_messages = 0;
    for (int iteration = 0; iteration < kWarmups; ++iteration) {
        if (!RunBidirectional(endpoints, rank, zero_copy, payload, received, size, size, &warmup_counts,
                              &unused_messages)) {
            return false;
        }
    }
    MPI_Barrier(MPI_COMM_WORLD);

    TransferCounts timed_counts;
    uint64_t message_count = 0;
    uint64_t timed_bytes = 0;
    uint64_t local_ns = 0;
    uint64_t window_ns = 0;
    if (size < 32 * 1024) {
        if (!RunBidirectional(endpoints, rank, zero_copy, payload, received, size, kSmallCaseWarmupBytes,
                              &timed_counts, &unused_messages)) {
            return false;
        }

        MPI_Barrier(MPI_COMM_WORLD);
        const auto calibration_start = Clock::now();
        if (!RunBidirectional(endpoints, rank, zero_copy, payload, received, size,
                              kSmallCaseCalibrationMessages * static_cast<uint64_t>(size), &timed_counts,
                              &unused_messages)) {
            return false;
        }
        local_ns = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - calibration_start).count());
        MPI_Allreduce(&local_ns, &window_ns, 1, MPI_UINT64_T, MPI_MAX, MPI_COMM_WORLD);
        message_count = std::max<uint64_t>(
            1, (kSmallCaseDurationNs * kSmallCaseCalibrationMessages + window_ns - 1) / window_ns);
        const uint64_t timed_target_bytes = message_count * static_cast<uint64_t>(size);

        MPI_Barrier(MPI_COMM_WORLD);
        const auto start = Clock::now();
        if (!RunBidirectional(endpoints, rank, zero_copy, payload, received, size, timed_target_bytes, &timed_counts,
                              &unused_messages)) {
            return false;
        }
        local_ns = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start).count());
        MPI_Allreduce(&local_ns, &window_ns, 1, MPI_UINT64_T, MPI_MAX, MPI_COMM_WORLD);
        timed_bytes = timed_target_bytes;
    } else {
        const uint64_t timed_target_bytes = kBytesPerCase - kBytesPerCase / 2;
        // The ECS link bursts above its sustained rate until the credits drain, so the first half of
        // every case is transferred untimed; the table reports the sustained rate, not a burst average.
        if (!RunBidirectional(endpoints, rank, zero_copy, payload, received, size, kBytesPerCase - timed_target_bytes,
                              &timed_counts, &unused_messages)) {
            return false;
        }
        const auto start = Clock::now();
        if (!RunBidirectional(endpoints, rank, zero_copy, payload, received, size, timed_target_bytes, &timed_counts,
                              &message_count)) {
            return false;
        }
        local_ns = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start).count());
        MPI_Allreduce(&local_ns, &window_ns, 1, MPI_UINT64_T, MPI_MAX, MPI_COMM_WORLD);
        timed_bytes = timed_target_bytes;
    }

    const uint64_t local_counts[] = {timed_counts.calls, timed_counts.waits};
    uint64_t total_counts[2] = {};
    MPI_Reduce(local_counts, total_counts, 2, MPI_UINT64_T, MPI_SUM, 0, MPI_COMM_WORLD);

    if (rank == 0) {
        const double elapsed_seconds = static_cast<double>(window_ns) / 1e9;
        const double gigabytes_per_second = 2.0 * static_cast<double>(timed_bytes) / elapsed_seconds / 1e9;
        fmt::print("{:<4} {:>10} {:>12} {:>8.3f} {:>9.3f} {:>9} {:>7} {:>5}\n",
                   zero_copy ? "ZC" : "copy", size, timed_bytes, gigabytes_per_second, elapsed_seconds,
                   message_count, total_counts[0], total_counts[1]);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    return true;
}

bool RunMode(int rank, bool zero_copy, int tag, const std::string& address, std::vector<char>& payload,
             std::vector<char>& received) {
    Endpoints endpoints;
    if (!Connect(rank, zero_copy, tag, address, endpoints)) {
        return false;
    }
    for (const size_t size : kMessageSizes) {
        if (!RunSize(endpoints, rank, zero_copy, size, payload.data(), received.data())) {
            return false;
        }
    }
    endpoints.outgoing->Close();
    endpoints.incoming->Close();
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
        fmt::print("RDMA-ZC bidirectional bandwidth benchmark: address={} warmups={} bytes_per_direction=up to {} GB\n",
               address_buffer, kWarmups, kBytesPerCase / 1000 / 1000 / 1000);
        fmt::print("{:<4} {:>10} {:>12} {:>8} {:>9} {:>9} {:>7} {:>5}\n", "mode", "size", "bytes", "GB/s",
                   "elapsed_s", "messages", "calls", "waits");
    }

    if (!RunMode(rank, false, 41, address_buffer, payload, received) ||
        !RunMode(rank, true, 43, address_buffer, payload, received)) {
        MPI_Abort(MPI_COMM_WORLD, 1);
        return 1;
    }

    MPI_Finalize();
    return 0;
}