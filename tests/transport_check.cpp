#include "transport_check.h"

#include <cerrno>
#include <cstring>
#include <poll.h>
#include <string>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <vector>
#include <mpi.h>
#include "logger.h"

namespace TransportCheck {
namespace {

constexpr int kWaitMs = 10000;
constexpr int kMaxIterations = 200000;
constexpr size_t kStallSize = 8 * 1024 * 1024;
constexpr uint32_t kControlMagic = 0x4f43434c; // "OCCL"

const size_t kBoundaries[] = {
    1, 65535, 65536, 65537, 2 * 1024 * 1024 - 1, 2 * 1024 * 1024, 2 * 1024 * 1024 + 1, 5 * 1024 * 1024};

struct ControlMessage {
    uint32_t magic;
    int32_t role;
};

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

bool WaitFd(int fd, short events, int timeout_ms) {
    pollfd entry{fd, events, 0};
    int ready = 0;
    do {
        ready = poll(&entry, 1, timeout_ms);
    } while (ready < 0 && errno == EINTR);
    return ready > 0;
}

bool WaitReady(Transport& transport, int timeout_ms) {
    return WaitFd(transport.GetFd(), static_cast<short>(transport.GetPollEvents()), timeout_ms);
}

void Sync() {
    MPI_Barrier(MPI_COMM_WORLD);
}

bool Fail(const Setup& setup, int rank, const char* step, const std::string& detail) {
    LOG_ERROR("{} rank {}: {} ({})", setup.name, rank, step, detail);
    return false;
}

std::shared_ptr<Transport> BuildConnection(const Setup& setup, int rank, bool rank1_producer, int tag,
                                           uint32_t* listener_events) {
    if (rank == 0) {
        std::shared_ptr<Transport> listener = setup.make();
        if (!listener->Listen(setup.listen_addr, 0)) {
            LOG_ERROR("{}: rank 0 failed to listen on '{}'", setup.name, setup.listen_addr);
            return nullptr;
        }
        *listener_events = listener->GetPollEvents();
        const uint16_t port = listener->GetListenPort();
        MPI_Send(&port, 1, MPI_UNSIGNED_SHORT, 1, tag, MPI_COMM_WORLD);

        std::shared_ptr<Transport> accepted = listener->Accept();
        listener->Close();
        if (!accepted) {
            LOG_ERROR("{}: rank 0 failed to accept", setup.name);
            return nullptr;
        }
        accepted->SetDirection(rank1_producer ? TransportDirection::Receive : TransportDirection::Send);
        return accepted;
    }

    uint16_t port = 0;
    MPI_Recv(&port, 1, MPI_UNSIGNED_SHORT, 0, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    std::shared_ptr<Transport> local = setup.make();
    local->SetDirection(rank1_producer ? TransportDirection::Send : TransportDirection::Receive);
    if (!local->Connect(setup.connect_addr, port)) {
        LOG_ERROR("{}: rank 1 failed to connect to '{}:{}'", setup.name, setup.connect_addr, port);
        return nullptr;
    }
    return local;
}

// The connector (rank 1) always writes the control message first, exactly like the channel
// handshake: the blocking Send/Recv pair exists for that control exchange alone.
bool ExchangeControl(const Setup& setup, int rank, Transport& local) {
    ControlMessage message{kControlMagic, rank};
    if (rank == 1) {
        if (!local.Send(&message, sizeof(message))) {
            return Fail(setup, rank, "control send failed", "");
        }
        return true;
    }
    ControlMessage received{};
    if (!local.Recv(&received, sizeof(received))) {
        return Fail(setup, rank, "control recv failed", "");
    }
    if (received.magic != kControlMagic || received.role != 1) {
        return Fail(setup, rank, "control message mismatch", "");
    }
    return true;
}

bool CheckConnection(const Setup& setup, int rank, Transport& local, bool local_producer) {
    if (!local.IsConnected()) {
        return Fail(setup, rank, "IsConnected is false", "");
    }
    if (local.GetFd() < 0) {
        return Fail(setup, rank, "GetFd returned a negative descriptor", "");
    }
    const uint32_t events = local.GetPollEvents();
    if (setup.fixed_poll_events) {
        if (events != EPOLLIN) {
            return Fail(setup, rank, "GetPollEvents must always be EPOLLIN", std::to_string(events));
        }
    } else {
        const uint32_t expected = local_producer ? EPOLLOUT : EPOLLIN;
        if (events != expected) {
            return Fail(setup, rank, "GetPollEvents does not follow the direction", std::to_string(events));
        }
    }
    return true;
}

bool TransferToDone(const Setup& setup, int rank, Transport& local, bool local_producer, const char* payload,
                    char* received, size_t size, size_t start_progress = 0) {
    size_t progress = start_progress;
    bool done = false;
    for (int iterations = 0; !done; ++iterations) {
        if (iterations > kMaxIterations) {
            return Fail(setup, rank, "transfer did not finish", std::to_string(progress));
        }
        const size_t before = progress;
        if (local_producer) {
            if (!local.TrySend(payload, size, &progress, &done)) {
                return Fail(setup, rank, "TrySend returned false", std::to_string(progress));
            }
        } else {
            if (!local.TryRecv(received, size, &progress, &done)) {
                return Fail(setup, rank, "TryRecv returned false", std::to_string(progress));
            }
        }
        if (progress < before || progress > size) {
            return Fail(setup, rank, "progress left the range 0..size", std::to_string(progress));
        }
        if (done != (progress == size)) {
            return Fail(setup, rank, "done disagrees with progress", std::to_string(progress));
        }
        if (!done && progress == before && !WaitReady(local, kWaitMs)) {
            return Fail(setup, rank, "no progress and no readiness", std::to_string(progress));
        }
    }
    return true;
}

bool CheckZeroSize(const Setup& setup, int rank, Transport& local, bool local_producer, std::vector<char>& buffer) {
    size_t progress = 0;
    bool done = false;
    const bool ok = local_producer ? local.TrySend(buffer.data(), 0, &progress, &done)
                                   : local.TryRecv(buffer.data(), 0, &progress, &done);
    if (!ok) {
        return Fail(setup, rank, "a zero-size transfer must succeed", "");
    }
    if (progress != 0 || !done) {
        return Fail(setup, rank, "a zero-size transfer must report done with zero progress", "");
    }
    return true;
}

bool CheckDirection(const Setup& setup, int rank, Transport& local, bool local_producer, std::vector<char>& buffer) {
    size_t progress = 0;
    bool done = false;
    if (setup.enforces_direction) {
        const bool ok = local_producer ? local.TryRecv(buffer.data(), 8, &progress, &done)
                                       : local.TrySend(buffer.data(), 8, &progress, &done);
        if (ok) {
            return Fail(setup, rank, "the wrong data-plane direction must be rejected", "");
        }
        return true;
    }

    // TCP keeps the direction as metadata only, so the consumer may still send and the
    // producer may still receive. The eight marker bytes are drained here so the payload
    // stream stays aligned.
    static const char kMarker[8] = {'o', 'c', 'c', 'l', 'm', 'a', 'r', 'k'};
    if (local_producer) {
        char got[sizeof(kMarker)] = {};
        size_t received = 0;
        bool received_done = false;
        for (int i = 0; i < kMaxIterations && !received_done; ++i) {
            if (!local.TryRecv(got, sizeof(got), &received, &received_done)) {
                return Fail(setup, rank, "the producer could not receive on a send edge", "");
            }
            if (!received_done && !WaitFd(local.GetFd(), POLLIN, kWaitMs)) {
                return Fail(setup, rank, "the reverse marker never arrived", "");
            }
        }
        if (std::memcmp(got, kMarker, sizeof(kMarker)) != 0) {
            return Fail(setup, rank, "the reverse marker was corrupted", "");
        }
        return true;
    }

    size_t sent = 0;
    bool sent_done = false;
    for (int i = 0; i < kMaxIterations && !sent_done; ++i) {
        if (!local.TrySend(kMarker, sizeof(kMarker), &sent, &sent_done)) {
            return Fail(setup, rank, "the consumer could not send on a receive edge", "");
        }
        if (!sent_done && !WaitFd(local.GetFd(), POLLOUT, kWaitMs)) {
            return Fail(setup, rank, "the send edge never became writable", "");
        }
    }
    return sent_done;
}

bool CheckBoundaries(const Setup& setup, int rank, Transport& local, bool local_producer, std::vector<char>& payload,
                     std::vector<char>& received) {
    for (const size_t size : kBoundaries) {
        FillPattern(payload.data(), size);
        if (!TransferToDone(setup, rank, local, local_producer, payload.data(), received.data(), size)) {
            return Fail(setup, rank, "boundary transfer failed", std::to_string(size));
        }
        // The transport must not reference the caller buffer once done is set, so the
        // producer overwrites it before the consumer checks the bytes it already holds.
        if (local_producer) {
            std::memset(payload.data(), 0xFF, size);
        }
        Sync();
        if (!local_producer) {
            size_t bad = 0;
            if (!CheckPattern(received.data(), size, &bad)) {
                return Fail(setup, rank, "payload mismatch", std::to_string(size) + " at " + std::to_string(bad));
            }
        }
    }
    return true;
}

bool CheckBackpressure(const Setup& setup, int rank, Transport& local, bool local_producer, std::vector<char>& payload,
                       std::vector<char>& received) {
    if (!setup.fixed_poll_events) {
        // 64 KiB is the smallest window that stalls well short of kStallSize without a
        // collapsing receive window: at 32 KiB every reopen waits out a 200ms probe.
        const int buffer_size = 64 * 1024;
        if (setsockopt(local.GetFd(), SOL_SOCKET, local_producer ? SO_SNDBUF : SO_RCVBUF, &buffer_size,
                       sizeof(buffer_size)) != 0) {
            return Fail(setup, rank, "could not bound the TCP socket buffer", strerror(errno));
        }
    }

    const int peer = 1 - rank;
    FillPattern(payload.data(), kStallSize);

    size_t progress = 0;
    bool done = false;
    if (local_producer) {
        // The producer pushes until the window refuses to take more. TrySend reports a stall by
        // leaving progress unchanged, and it must never block or fail. No readiness wait belongs
        // here: the consumer is idle until the stall is announced.
        bool stalled = false;
        for (int i = 0; i < kMaxIterations && !done && !stalled; ++i) {
            const size_t before = progress;
            if (!local.TrySend(payload.data(), kStallSize, &progress, &done)) {
                return Fail(setup, rank, "TrySend failed while the window filled", "");
            }
            stalled = progress == before;
        }
        if (!done && !stalled) {
            return Fail(setup, rank, "the window neither filled nor drained", std::to_string(progress));
        }
        if (!done) {
            const size_t held = progress;
            bool repeat_done = false;
            if (!local.TrySend(payload.data(), kStallSize, &progress, &repeat_done)) {
                return Fail(setup, rank, "TrySend failed on a stalled window", "");
            }
            if (progress != held || repeat_done) {
                return Fail(setup, rank, "a stalled TrySend advanced or completed", std::to_string(progress));
            }
        }
        MPI_Send(&progress, sizeof(progress), MPI_BYTE, peer, 21, MPI_COMM_WORLD);
    } else {
        size_t reported = 0;
        MPI_Recv(&reported, sizeof(reported), MPI_BYTE, peer, 21, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        if (reported == 0 || reported > kStallSize) {
            return Fail(setup, rank, "the producer reported an impossible window", std::to_string(reported));
        }
    }

    // Drain the whole payload. The producer resumes from the byte it stalled on, so a transport
    // that silently dropped the buffered part would still be caught by the pattern check below.
    if (!TransferToDone(setup, rank, local, local_producer, payload.data(), received.data(), kStallSize, progress)) {
        return Fail(setup, rank, "the stalled transfer never completed", "");
    }

    // done means the transport no longer reads the caller buffer, so overwriting it here is
    // safe; the consumer checks the bytes it already copied before the barrier.
    if (!local_producer) {
        size_t bad = 0;
        if (!CheckPattern(received.data(), kStallSize, &bad)) {
            return Fail(setup, rank, "the stalled payload was corrupted", std::to_string(bad));
        }
    }
    std::memset(payload.data(), 0xFF, kStallSize);
    Sync();
    return true;
}

bool CheckClose(const Setup& setup, int rank, Transport& local, bool local_producer, std::vector<char>& buffer) {
    if (!setup.detects_peer_close) {
        // Shared memory has no peer-death signal: closing must release the endpoint, and
        // every further use of it must fail.
        local.Close();
        if (local.IsConnected()) {
            return Fail(setup, rank, "IsConnected stayed true after Close", "");
        }
        size_t progress = 0;
        bool done = false;
        const bool ok = local_producer ? local.TrySend(buffer.data(), 8, &progress, &done)
                                       : local.TryRecv(buffer.data(), 8, &progress, &done);
        if (ok) {
            return Fail(setup, rank, "a closed endpoint must reject the transfer", "");
        }
        return true;
    }

    if (local_producer) {
        // The producer leaves first so that the consumer can observe the disappearance. A
        // barrier here would deadlock: the consumer waits inside its receive loop.
        local.Close();
        return true;
    }

    size_t progress = 0;
    bool done = false;
    for (int i = 0; i < kMaxIterations; ++i) {
        if (!local.TryRecv(buffer.data(), 8, &progress, &done)) {
            local.Close();
            return true;
        }
        if (done) {
            return Fail(setup, rank, "an orderly close must not deliver data", "");
        }
        WaitFd(local.GetFd(), POLLIN, 1000);
    }
    local.Close();
    return Fail(setup, rank, "the peer close was never observed", "");
}

bool RunConnection(const Setup& setup, int rank, bool rank1_producer, int tag, std::vector<char>& payload,
                   std::vector<char>& received) {
    uint32_t listener_events = 0;
    std::shared_ptr<Transport> local = BuildConnection(setup, rank, rank1_producer, tag, &listener_events);
    if (!local) {
        return false;
    }
    if (rank == 0 && listener_events != EPOLLIN) {
        return Fail(setup, rank, "a listening transport must report EPOLLIN", std::to_string(listener_events));
    }
    const bool local_producer = (rank == 1) == rank1_producer;
    if (!CheckConnection(setup, rank, *local, local_producer)) {
        return false;
    }
    if (!ExchangeControl(setup, rank, *local)) {
        return false;
    }
    if (!CheckZeroSize(setup, rank, *local, local_producer, payload)) {
        return false;
    }
    if (!CheckDirection(setup, rank, *local, local_producer, payload)) {
        return false;
    }
    if (!CheckBoundaries(setup, rank, *local, local_producer, payload, received)) {
        return false;
    }
    if (!CheckBackpressure(setup, rank, *local, local_producer, payload, received)) {
        return false;
    }
    return CheckClose(setup, rank, *local, local_producer, received);
}
} // namespace

int RunSuite(const Setup& setup, int rank, int world_size) {
    if (world_size != 2) {
        if (rank == 0) {
            LOG_ERROR("{}: the transport suite needs exactly two ranks, got {}", setup.name, world_size);
        }
        return 1;
    }

    std::vector<char> payload(kStallSize);
    std::vector<char> received(kStallSize);

    // Connection one has the connector as the producer, connection two as the consumer, so
    // both data-plane roles are covered on the same pair of ranks.
    if (!RunConnection(setup, rank, true, 11, payload, received)) {
        MPI_Abort(MPI_COMM_WORLD, 1);
        return 1;
    }
    if (!RunConnection(setup, rank, false, 12, payload, received)) {
        MPI_Abort(MPI_COMM_WORLD, 1);
        return 1;
    }
    if (!RunConnection(setup, rank, true, 13, payload, received)) {
        MPI_Abort(MPI_COMM_WORLD, 1);
        return 1;
    }

    if (rank == 0) {
        LOG_INFO("{}: transport semantics suite passed", setup.name);
    }
    return 0;
}
} // namespace TransportCheck
