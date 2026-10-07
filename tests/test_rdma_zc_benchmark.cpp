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
constexpr size_t kBufferSize = 48 * 1024 * 1024;
constexpr size_t kPipelineDepth = 8;
constexpr uint32_t kControlMagic = 0x4f43434c;
constexpr size_t kMessageSizes[] = {32, 1024, 4096, 16384, 32 * 1024, 64 * 1024,
                                    128 * 1024, 256 * 1024, 512 * 1024, 1 * 1024 * 1024,
                                    5 * 1024 * 1024, 16 * 1024 * 1024 - 1, 16 * 1024 * 1024,
                                    17 * 1024 * 1024, 32 * 1024 * 1024, 48 * 1024 * 1024};

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

bool CheckPattern(const char* data, size_t size, size_t* bad_index) {
    for (size_t i = 0; i < size; ++i) {
        if (data[i] != static_cast<char>((i * 31 + 7) & 0xFF)) {
            *bad_index = i;
            return false;
        }
    }
    return true;
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

// Keeps up to `depth` messages in flight per direction. A depth of one pays a full round trip per
// message and leaves the credit window unused; the callers size `depth` so the send slots cap at the
// window and the receive slots stay inside the caller buffer.
bool Transfer(const Endpoints& endpoints, int rank, bool zero_copy, const char* payload, char* received, size_t size,
              uint64_t messages, size_t depth, TransferCounts* counts) {
    std::vector<size_t> send_progress(depth, 0);
    std::vector<uint64_t> send_slot(depth, UINT64_MAX);
    std::vector<size_t> recv_progress(depth, 0);
    std::vector<uint64_t> recv_slot(depth, UINT64_MAX);
    uint64_t send_issued = 0;
    uint64_t recv_completed = 0;
    const uint64_t iteration_limit = messages * 64 + kMaxIterations;
    for (uint64_t iterations = 0; recv_completed < messages; ++iterations) {
        if (iterations >= iteration_limit) {
            return Fail(rank, "transfer iteration limit reached", std::to_string(send_issued));
        }
        bool progressed = false;
        while (send_issued < messages && send_issued - recv_completed < depth) {
            const size_t slot = static_cast<size_t>(send_issued % depth);
            if (send_slot[slot] != send_issued) {
                send_slot[slot] = send_issued;
                send_progress[slot] = 0;
            }
            const size_t before = send_progress[slot];
            ++counts->calls;
            bool done = false;
            const bool ok = zero_copy
                        ? endpoints.outgoing->TrySend(payload, size, &send_progress[slot], &done)
                        : endpoints.outgoing->TransportRDMA::TrySend(payload, size, &send_progress[slot], &done);
            if (!ok) {
                return Fail(rank, "TrySend failed", std::to_string(size));
            }
            if (send_progress[slot] < before || send_progress[slot] > size || done != (send_progress[slot] == size)) {
                return Fail(rank, "invalid send progress or done state", std::to_string(send_progress[slot]));
            }
            if (!done) {
                break;
            }
            ++send_issued;
            progressed = true;
        }
        while (recv_completed < send_issued) {
            const size_t slot = static_cast<size_t>(recv_completed % depth);
            if (recv_slot[slot] != recv_completed) {
                recv_slot[slot] = recv_completed;
                recv_progress[slot] = 0;
            }
            const size_t before = recv_progress[slot];
            ++counts->calls;
            bool done = false;
            const bool ok = zero_copy
                        ? endpoints.incoming->TryRecv(received + slot * size, size, &recv_progress[slot], &done)
                        : endpoints.incoming->TransportRDMA::TryRecv(received + slot * size, size, &recv_progress[slot], &done);
            if (!ok) {
                return Fail(rank, "TryRecv failed", std::to_string(size));
            }
            if (recv_progress[slot] < before || recv_progress[slot] > size || done != (recv_progress[slot] == size)) {
                return Fail(rank, "invalid recv progress or done state", std::to_string(recv_progress[slot]));
            }
            if (!done) {
                break;
            }
            ++recv_completed;
            progressed = true;
        }
        if (!progressed) {
            ++counts->waits;
            if (!WaitReady(endpoints, true, true)) {
                return Fail(rank, "timed out waiting for RDMA readiness", std::to_string(size));
            }
        }
    }
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
    // Zero copy holds one registered buffer and one send and receive state per transport, so it stays
    // single message; the copy path fills the credit window instead. The caller buffer bounds the
    // depth, since every message in flight needs its own receive slot.
    const size_t depth =
        zero_copy || size < 32 * 1024 ? 1 : std::min<size_t>(kPipelineDepth, kBufferSize / size);
    TransferCounts warmup_counts;
    for (int iteration = 0; iteration < kWarmups; ++iteration) {
        if (!Transfer(endpoints, rank, zero_copy, payload, received, size, 1, depth, &warmup_counts)) {
            return false;
        }
        size_t bad_index = 0;
        if (!CheckPattern(received, size, &bad_index)) {
            return Fail(rank, "warmup payload mismatch", std::to_string(bad_index));
        }
    }
    MPI_Barrier(MPI_COMM_WORLD);

    TransferCounts timed_counts;
    uint64_t message_count = 0;
    uint64_t timed_bytes = 0;
    uint64_t elapsed_ns = 0;
    if (size < 32 * 1024) {
        uint64_t remaining_bytes = kSmallCaseWarmupBytes;
        while (remaining_bytes > 0) {
            const size_t transfer_size = static_cast<size_t>(std::min<uint64_t>(size, remaining_bytes));
            if (!Transfer(endpoints, rank, zero_copy, payload, received, transfer_size, 1, 1, &timed_counts)) {
                return false;
            }
            remaining_bytes -= transfer_size;
        }

        MPI_Barrier(MPI_COMM_WORLD);
        const auto calibration_start = Clock::now();
        for (int iteration = 0; iteration < kSmallCaseCalibrationMessages; ++iteration) {
            if (!Transfer(endpoints, rank, zero_copy, payload, received, size, 1, 1, &timed_counts)) {
                return false;
            }
        }
        if (rank == 0) {
            elapsed_ns = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - calibration_start).count());
        }
        MPI_Bcast(&elapsed_ns, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);
        message_count = std::max<uint64_t>(
            1, (kSmallCaseDurationNs * kSmallCaseCalibrationMessages + elapsed_ns - 1) / elapsed_ns);

        MPI_Barrier(MPI_COMM_WORLD);
        const auto start = Clock::now();
        for (uint64_t iteration = 0; iteration < message_count; ++iteration) {
            if (!Transfer(endpoints, rank, zero_copy, payload, received, size, 1, 1, &timed_counts)) {
                return false;
            }
        }
        elapsed_ns = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start).count());
        timed_bytes = message_count * size;
    } else {
        const uint64_t total_messages = kBytesPerCase / size;
        const uint64_t warmup_messages = total_messages / 2;
        // The ECS link bursts above its sustained rate until the credits drain, so the first half of
        // every case is transferred untimed; the table reports the sustained rate, not a burst average.
        if (!Transfer(endpoints, rank, zero_copy, payload, received, size, warmup_messages, depth, &timed_counts)) {
            return false;
        }
        MPI_Barrier(MPI_COMM_WORLD);
        const auto start = Clock::now();
        message_count = total_messages - warmup_messages;
        if (!Transfer(endpoints, rank, zero_copy, payload, received, size, message_count, depth, &timed_counts)) {
            return false;
        }
        elapsed_ns = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start).count());
        timed_bytes = message_count * size;
    }

    uint64_t gathered_elapsed_ns[2] = {};
    MPI_Gather(&elapsed_ns, 1, MPI_UINT64_T, gathered_elapsed_ns, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);
    const uint64_t local_counts[] = {timed_counts.calls, timed_counts.waits};
    uint64_t total_counts[2] = {};
    MPI_Reduce(local_counts, total_counts, 2, MPI_UINT64_T, MPI_SUM, 0, MPI_COMM_WORLD);

    if (rank == 0) {
        const double window_seconds =
            static_cast<double>(std::max(gathered_elapsed_ns[0], gathered_elapsed_ns[1])) / 1e9;
        const double direction_gigabytes_per_second = static_cast<double>(timed_bytes) / window_seconds / 1e9;
        const double bidirectional_gigabytes_per_second = 2.0 * direction_gigabytes_per_second;
        fmt::print("{:<4} {:>10} {:>12} {:>9.3f} {:>10.3f} {:>9.3f} {:>9} {:>7} {:>5}\n",
                   zero_copy ? "ZC" : "copy", size, timed_bytes, direction_gigabytes_per_second,
                   bidirectional_gigabytes_per_second, window_seconds, message_count, total_counts[0],
                   total_counts[1]);
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
        fmt::print("RDMA-ZC bidirectional benchmark: address={} warmups={} bytes_per_case=up to {} GB per direction\n",
               address_buffer, kWarmups, kBytesPerCase / 1000 / 1000 / 1000);
        fmt::print("{:<4} {:>10} {:>12} {:>9} {:>10} {:>9} {:>9} {:>7} {:>5}\n", "mode", "size", "bytes",
                   "GB/s", "GB/s(bidi)", "window_s", "messages", "calls", "waits");
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