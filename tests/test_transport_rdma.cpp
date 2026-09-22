#include <algorithm>
#include <cstring>
#include <string>
#include <vector>
#include <mpi.h>
#include <poll.h>
#include "logger.h"
#include "transport/transport_rdma.h"

namespace {
constexpr size_t kMiB = 1024 * 1024;
constexpr uint64_t kHandshake = 0x5A5A5A5Aull;

std::vector<char> Pattern(size_t size, unsigned seed) {
    std::vector<char> data(size);
    for (size_t i = 0; i < size; ++i) {
        data[i] = static_cast<char>((i * 131 + seed * 17) & 0xFF);
    }
    return data;
}

bool SameData(const std::vector<char>& left, const std::vector<char>& right) {
    return left.size() == right.size() && std::memcmp(left.data(), right.data(), left.size()) == 0;
}

// progress accumulates within one Try* call for a fixed (data, size) pair, and a call that
// cannot advance must never block, so the caller waits on the transport's own readiness fd.
bool Advance(TransportRDMA& transport, const void* data, size_t size, bool send) {
    size_t progress = 0;
    bool done = false;
    while (!done) {
        const size_t before = progress;
        const bool ok = send ? transport.TrySend(data, size, &progress, &done)
                             : transport.TryRecv(const_cast<void*>(data), size, &progress, &done);
        if (!ok) {
            return false;
        }
        if (done) {
            return true;
        }
        if (progress == before) {
            pollfd entry{transport.GetFd(), static_cast<short>(transport.GetPollEvents()), 0};
            int ready = 0;
            do {
                ready = poll(&entry, 1, 20000);
            } while (ready < 0 && errno == EINTR);
            if (ready <= 0) {
                LOG_ERROR("Timed out waiting for RDMA progress");
                return false;
            }
        }
    }
    return true;
}
} // namespace

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);
    int rank = 0;
    int world_size = 0;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    if (world_size != 2) {
        if (rank == 0) {
            LOG_ERROR("This test needs exactly 2 ranks, got {}", world_size);
        }
        MPI_Finalize();
        return 1;
    }

    std::string addr;
    if (!TransportRDMA::Probe(addr)) {
        if (rank == 0) {
            LOG_ERROR("No RDMA device with an active port is available");
        }
        MPI_Finalize();
        return 2;
    }
    LOG_INFO("Rank {}: RDMA address {}", rank, addr);

    TransportRDMA listener;
    uint16_t port = 0;
    if (rank == 0) {
        if (!listener.ListenAddr(addr, 0)) {
            LOG_ERROR("Rank 0: failed to listen over RDMA");
            MPI_Finalize();
            return 1;
        }
        port = listener.GetListenPort();
    }
    MPI_Bcast(&port, 1, MPI_UNSIGNED_SHORT, 0, MPI_COMM_WORLD);

    // Rank 1 is the active connector and the data producer; rank 0 accepts and consumes.
    // transport must be the shared_ptr itself on the passive side: copying a TransportRDMA by
    // value would leave two owners of the same CM id and memory region.
    std::shared_ptr<Transport> owned;
    TransportRDMA active;
    if (rank == 1) {
        active.SetDirection(TransportDirection::Send);
        if (!active.Connect(addr, port)) {
            LOG_ERROR("Rank 1: failed to connect over RDMA");
            MPI_Finalize();
            return 1;
        }
    } else {
        owned = listener.Accept();
        if (!owned) {
            LOG_ERROR("Rank 0: failed to accept the RDMA connection");
            MPI_Finalize();
            return 1;
        }
        owned->SetDirection(TransportDirection::Receive);
    }
    TransportRDMA& connection = rank == 1 ? active : *static_cast<TransportRDMA*>(owned.get());
    LOG_INFO("Rank {}: RDMA connection established", rank);

    // The active side's first blocking Send carries the channel handshake; the passive side's
    // first blocking Recv returns it. Both then switch to Try* for the data plane.
    uint64_t handshake = kHandshake;
    bool ok = true;
    if (rank == 1) {
        if (!connection.Send(&handshake, sizeof(handshake))) {
            LOG_ERROR("Rank 1: control handshake failed");
            ok = false;
        }
    } else {
        uint64_t received = 0;
        if (!connection.Recv(&received, sizeof(received)) || received != kHandshake) {
            LOG_ERROR("Rank 0: control handshake failed, got {:#x}", received);
            ok = false;
        }
    }

    const size_t sizes[] = {1, 4096, kRdmaSlotSize, kRdmaSlotSize + 1, 2 * kMiB, 2 * kMiB + 1, 5 * kMiB};
    for (size_t size : sizes) {
        if (!ok) {
            break;
        }
        std::vector<char> send = Pattern(size, 3);
        std::vector<char> recv(size, 0);
        MPI_Barrier(MPI_COMM_WORLD);
        const bool advanced =
            rank == 1 ? Advance(connection, send.data(), size, true) : Advance(connection, recv.data(), size, false);
        MPI_Barrier(MPI_COMM_WORLD);
        if (!advanced) {
            LOG_ERROR("Rank {}: transfer of {} bytes failed", rank, size);
            ok = false;
            break;
        }
        if (rank == 0 && !SameData(send, recv)) {
            LOG_ERROR("Rank 0: {} byte payload mismatch (expected {}, got {})", size, static_cast<int>(send[0]),
                      static_cast<int>(recv[0]));
            ok = false;
            break;
        }
        LOG_INFO("Rank {}: {} bytes ok", rank, size);
    }

    // Repeated rounds prove ring slots are recycled through credits instead of exhausting the
    // window after the first fill.
    for (int round = 0; round < 8 && ok; ++round) {
        std::vector<char> send = Pattern(1500 * 1000, static_cast<unsigned>(round));
        std::vector<char> recv(send.size(), 0);
        MPI_Barrier(MPI_COMM_WORLD);
        const bool advanced = rank == 1 ? Advance(connection, send.data(), send.size(), true)
                                        : Advance(connection, recv.data(), recv.size(), false);
        MPI_Barrier(MPI_COMM_WORLD);
        if (!advanced) {
            LOG_ERROR("Rank {}: round {} failed", rank, round);
            ok = false;
            break;
        }
        if (rank == 0 && !SameData(send, recv)) {
            LOG_ERROR("Rank 0: round {} payload mismatch", round);
            ok = false;
            break;
        }
    }

    int failed = ok ? 0 : 1;
    MPI_Allreduce(MPI_IN_PLACE, &failed, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
    if (rank == 0 && failed == 0) {
        LOG_INFO("RDMA transport test passed");
    }

    connection.Close();
    owned.reset();
    listener.Close();
    MPI_Finalize();
    return failed;
}
