#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <poll.h>
#include <string>
#include <vector>
#include <cerrno>
#include <mpi.h>
#include "logger.h"
#include "transport/transport.h"
#include "transport/transport_rdma.h"
#include "transport/transport_rdma_zc.h"

namespace {
constexpr int kWaitMs = 10000;
constexpr int kMaxIterations = 400000;
constexpr size_t kBufferSize = 48 * 1024 * 1024;
constexpr uint32_t kControlMagic = 0x4f43434c; // "OCCL"

static_assert(TransportRDMAZc::IsTransferSizeSupported(kRdmaZcMaxTransferSize));
static_assert(!TransportRDMAZc::IsTransferSizeSupported(kRdmaZcMaxTransferSize + 1));

struct ControlMessage {
    uint32_t magic;
    int32_t role;
};

std::shared_ptr<Transport> Make() {
    return std::make_shared<TransportRDMAZc>();
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

bool WaitReady(Transport& transport, int timeout_ms) {
    pollfd entry{transport.GetFd(), static_cast<short>(transport.GetPollEvents()), 0};
    int ready = 0;
    do {
        ready = poll(&entry, 1, timeout_ms);
    } while (ready < 0 && errno == EINTR);
    return ready > 0;
}

bool Fail(const char* step, const std::string& detail) {
    int rank = 0;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    LOG_ERROR("rdma_zc rank {}: {} ({})", rank, step, detail);
    return false;
}

// The zero-copy path only reports 0 or size: the transfer is a single logical message, so the
// caller may reuse its buffer on done but must not read partial bytes before that.
bool TransferZeroCopy(Transport& local, bool local_producer, const char* payload, char* received, size_t size,
                      bool expect_zero_copy) {
    size_t progress = 0;
    bool done = false;
    for (int iterations = 0; !done; ++iterations) {
        if (iterations > kMaxIterations) {
            return Fail("transfer did not finish", std::to_string(progress));
        }
        const size_t before = progress;
        const bool ok = local_producer ? local.TrySend(payload, size, &progress, &done)
                                       : local.TryRecv(received, size, &progress, &done);
        if (!ok) {
            return Fail("Try* returned false", std::to_string(progress));
        }
        if (expect_zero_copy) {
            if (progress != 0 && progress != size) {
                return Fail("the zero-copy progress must be 0 or size", std::to_string(progress));
            }
        } else if (progress > size) {
            return Fail("progress left the range 0..size", std::to_string(progress));
        }
        if (done != (progress == size)) {
            return Fail("done disagrees with progress", std::to_string(progress));
        }
        if (!done && progress == before && !WaitReady(local, kWaitMs)) {
            return Fail("no progress and no readiness", std::to_string(progress));
        }
    }
    return true;
}

bool RunCase(Transport& local, bool local_producer, std::vector<char>& payload, std::vector<char>& received,
             size_t size, bool expect_zero_copy) {
    FillPattern(payload.data(), size);
    if (!TransferZeroCopy(local, local_producer, payload.data(), received.data(), size, expect_zero_copy)) {
        return Fail("transfer failed", std::to_string(size));
    }
    if (local_producer) {
        std::memset(payload.data(), 0xFF, size);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    if (!local_producer) {
        size_t bad = 0;
        if (!CheckPattern(received.data(), size, &bad)) {
            return Fail("payload mismatch", std::to_string(size) + " at " + std::to_string(bad));
        }
    }
    return true;
}

bool RunConnection(int rank, bool rank1_producer, int tag, std::vector<char>& payload, std::vector<char>& received,
                   const std::string& listen_addr, const std::string& connect_addr) {
    std::shared_ptr<Transport> local;
    if (rank == 0) {
        auto listener = Make();
        if (!listener->Listen(listen_addr, 0)) {
            return Fail("rank 0 failed to listen", listen_addr);
        }
        const uint16_t port = listener->GetListenPort();
        MPI_Send(&port, 1, MPI_UNSIGNED_SHORT, 1, tag, MPI_COMM_WORLD);
        local = listener->Accept();
        listener->Close();
        if (!local) {
            return Fail("rank 0 failed to accept", "");
        }
        local->SetDirection(rank1_producer ? TransportDirection::Receive : TransportDirection::Send);
    } else {
        uint16_t port = 0;
        MPI_Recv(&port, 1, MPI_UNSIGNED_SHORT, 0, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        local = Make();
        local->SetDirection(rank1_producer ? TransportDirection::Send : TransportDirection::Receive);
        if (!local->Connect(connect_addr, port)) {
            return Fail("rank 1 failed to connect", connect_addr);
        }
    }

    // The connector always writes the control message first, exactly like the channel handshake.
    if (rank == 1) {
        ControlMessage message{kControlMagic, rank};
        if (!local->Send(&message, sizeof(message))) {
            return Fail("control send failed", "");
        }
    } else {
        ControlMessage received_message{};
        if (!local->Recv(&received_message, sizeof(received_message))) {
            return Fail("control recv failed", "");
        }
        if (received_message.magic != kControlMagic || received_message.role != 1) {
            return Fail("control message mismatch", "");
        }
    }

    const bool local_producer = (rank == 1) == rank1_producer;
    const size_t threshold = kRdmaZcThreshold;

    // Below the threshold the ring path is used, so progress may advance in slot-sized steps.
    if (!RunCase(*local, local_producer, payload, received, threshold - 1, false)) {
        return false;
    }
    // At the threshold the zero-copy path takes over.
    if (!RunCase(*local, local_producer, payload, received, threshold, true)) {
        return false;
    }
    // Not a chunk multiple: two zero-copy chunks.
    if (!RunCase(*local, local_producer, payload, received, threshold + 1024 * 1024, true)) {
        return false;
    }
    // Three zero-copy chunks.
    if (!RunCase(*local, local_producer, payload, received, threshold * 3, true)) {
        return false;
    }
    // Back below the threshold on the same connection, so the two paths interleave.
    if (!RunCase(*local, local_producer, payload, received, threshold / 2, false)) {
        return false;
    }

    local->Close();
    if (local->IsConnected()) {
        return Fail("IsConnected stayed true after Close", "");
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
        if (rank == 0) {
            LOG_ERROR("rdma_zc: the suite needs exactly two ranks, got {}", world_size);
        }
        MPI_Finalize();
        return 1;
    }

    std::string device_addr;
    int device_ok = TransportRDMA::Probe(device_addr) ? 1 : 0;
    int global_ok = 0;
    MPI_Allreduce(&device_ok, &global_ok, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);
    if (global_ok == 0) {
        if (rank == 0) {
            LOG_INFO("rdma_zc: no device with an active port is available, skipping");
        }
        MPI_Finalize();
        return 2;
    }

    char buffer[64] = {};
    if (rank == 0) {
        std::snprintf(buffer, sizeof(buffer), "%s", device_addr.c_str());
    }
    MPI_Bcast(buffer, sizeof(buffer), MPI_CHAR, 0, MPI_COMM_WORLD);

    std::vector<char> payload(kBufferSize);
    std::vector<char> received(kBufferSize);

    // Connection one has the connector as the producer, connection two as the consumer, so both
    // data-plane roles run through the zero-copy path.
    if (!RunConnection(rank, true, 31, payload, received, buffer, buffer)) {
        MPI_Abort(MPI_COMM_WORLD, 1);
        return 1;
    }
    if (!RunConnection(rank, false, 32, payload, received, buffer, buffer)) {
        MPI_Abort(MPI_COMM_WORLD, 1);
        return 1;
    }

    if (rank == 0) {
        LOG_INFO("rdma_zc: zero-copy transport suite passed");
    }
    MPI_Finalize();
    return 0;
}
