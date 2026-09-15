#include <arpa/inet.h>
#include <dirent.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <vector>
#include "transport_shm.h"
#include "logger.h"

namespace {
// Larger than the ring, and not a multiple of it, so the payload wraps the shared buffer
// several times and the sender has to resume from partial progress.
constexpr size_t kForwardBytes = 3 * 1024 * 1024 + 517;
constexpr size_t kBackwardBytes = 1024 * 1024;
constexpr size_t kRingCapacity = 1024 * 1024;
constexpr int kDeadlineSeconds = 60;
constexpr int kWakeupTimeoutMs = 5000;

int g_failures = 0;

// The two sides hand over their ring fds and then close the rendezvous socket, so the rings
// are all that is left. These pipes order the phases that race: the passive side must not
// close an edge before the active side has finished reading it.
int g_to_child = -1;
int g_to_parent = -1;

void Check(bool condition, const std::string& what) {
    if (!condition) {
        LOG_ERROR("FAIL: {}", what);
        ++g_failures;
    }
}

char Pattern(size_t index, size_t direction) {
    return static_cast<char>(((index * 31 + direction * 7) & 0x7f) + 1);
}

std::vector<char> MakePayload(size_t size, size_t direction) {
    std::vector<char> payload(size);
    for (size_t i = 0; i < size; ++i) {
        payload[i] = Pattern(i, direction);
    }
    return payload;
}

bool Matches(const std::vector<char>& payload, size_t direction) {
    for (size_t i = 0; i < payload.size(); ++i) {
        if (payload[i] != Pattern(i, direction)) {
            LOG_ERROR("FAIL: payload mismatch at byte {} of {}", i, payload.size());
            return false;
        }
    }
    return true;
}

bool SyncOn(int fd) {
    char byte = 0;
    ssize_t received;
    do {
        received = read(fd, &byte, 1);
    } while (received < 0 && errno == EINTR);
    return received == 1;
}

bool SignalSync(int fd) {
    char byte = 1;
    ssize_t written;
    do {
        written = write(fd, &byte, 1);
    } while (written < 0 && errno == EINTR);
    return written == 1;
}

// One entry per open descriptor: the rendezvous socket is expected to disappear from this
// count once both directions of the control handshake have happened.
int OpenFdCount() {
    DIR* dir = opendir("/proc/self/fd");
    if (dir == nullptr) {
        return -1;
    }
    int count = 0;
    while (readdir(dir) != nullptr) {
        ++count;
    }
    closedir(dir);
    return count;
}

// A bound TCP socket keeps the port number out of everyone else's reach for as long as it
// stays bound, which is what makes the rendezvous name derived from it unique on this host.
int LeaseFreePort(uint16_t* port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }
    struct sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    socklen_t length = sizeof(addr);
    if (bind(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) != 0 ||
        getsockname(fd, reinterpret_cast<struct sockaddr*>(&addr), &length) != 0) {
        close(fd);
        return -1;
    }
    *port = ntohs(addr.sin_port);
    return fd;
}

struct Handshake {
    int value = 0;
};

// An executor waits on a transport's readiness handle; the test waits on the very same fds.
struct pollfd PollFor(int fd) {
    struct pollfd entry;
    entry.fd = fd;
    entry.events = POLLIN;
    entry.revents = 0;
    return entry;
}

bool HandshakeEdge(const std::shared_ptr<Transport>& transport, int sent_value) {
    Handshake handshake;
    handshake.value = sent_value;
    return transport->Send(&handshake, sizeof(handshake));
}

bool AcceptHandshake(const std::shared_ptr<Transport>& transport) {
    Handshake handshake;
    return transport->Recv(&handshake, sizeof(handshake));
}

// Passive side: creates the rings for every edge and hands their fds over. It consumes what
// the peer produces and produces what the peer consumes.
int RunPassiveSide(uint16_t port) {
    auto listener = std::make_shared<TransportSHM>();
    if (!listener->Listen(port)) {
        return 1;
    }

    auto incoming = listener->Accept();
    auto outgoing = listener->Accept();
    if (!incoming || !outgoing) {
        LOG_ERROR("FAIL: could not accept both shared-memory edges");
        return 1;
    }

    const int fds_before = OpenFdCount();
    if (!AcceptHandshake(incoming) || !AcceptHandshake(outgoing)) {
        LOG_ERROR("FAIL: control handshake failed");
        return 1;
    }
    if (fds_before - OpenFdCount() != 2) {
        LOG_ERROR("FAIL: both rendezvous sockets must be closed after the handshake");
        return 1;
    }

    // Delaying the reads guarantees the producer has to stop at a full ring, which is the
    // backpressure path the peer asserts on.
    usleep(150 * 1000);

    const std::vector<char> payload = MakePayload(kBackwardBytes, 2);
    std::vector<char> received(kForwardBytes, 0);
    size_t produce_progress = 0;
    size_t consume_progress = 0;
    bool produce_done = false;
    bool consume_done = false;

    const int deadline = kDeadlineSeconds * 1000;
    for (int waited = 0; (!produce_done || !consume_done) && waited < deadline;) {
        bool progressed = false;
        if (!consume_done) {
            const size_t before = consume_progress;
            if (!incoming->TryRecv(received.data(), received.size(), &consume_progress, &consume_done)) {
                LOG_ERROR("FAIL: receive from the peer failed");
                return 1;
            }
            progressed = progressed || consume_progress != before;
        }
        if (!produce_done) {
            const size_t before = produce_progress;
            if (!outgoing->TrySend(payload.data(), payload.size(), &produce_progress, &produce_done)) {
                LOG_ERROR("FAIL: send to the peer failed");
                return 1;
            }
            progressed = progressed || produce_progress != before;
        }
        if (produce_done && consume_done) {
            break;
        }

        // Wait exactly the way an executor does: on the descriptors the transport reports.
        struct pollfd waits[2] = {PollFor(incoming->RecvWait().fd), PollFor(outgoing->SendWait().fd)};
        if (poll(waits, 2, progressed ? 0 : 200) < 0) {
            LOG_ERROR("FAIL: poll on the shared-memory descriptors failed");
            return 1;
        }
        if (!progressed) {
            waited += 200;
        }
    }

    if (!produce_done || !consume_done) {
        LOG_ERROR("FAIL: transfer did not finish within {}s", kDeadlineSeconds);
        return 1;
    }
    if (!Matches(received, 1)) {
        return 1;
    }

    // The peer says it has read everything, so closing can no longer cut a transfer short.
    if (!SyncOn(g_to_child)) {
        LOG_ERROR("FAIL: could not synchronize before closing the data edges");
        return 1;
    }
    incoming->Close();
    outgoing->Close();
    if (!SignalSync(g_to_parent)) {
        LOG_ERROR("FAIL: could not report the close");
        return 1;
    }

    // A second, untouched edge pair: closing an idle ring is the only event it ever sees,
    // which is what makes the peer's descriptor check deterministic.
    auto quiet_incoming = listener->Accept();
    auto quiet_outgoing = listener->Accept();
    if (!quiet_incoming || !quiet_outgoing) {
        LOG_ERROR("FAIL: could not accept the idle edges");
        return 1;
    }
    if (!AcceptHandshake(quiet_incoming) || !AcceptHandshake(quiet_outgoing)) {
        LOG_ERROR("FAIL: idle control handshake failed");
        return 1;
    }
    // The peer fills the send edge while this side does not read, then says so; only then is
    // closing it what makes the peer's blocked send-wait become readable.
    if (!SyncOn(g_to_child)) {
        LOG_ERROR("FAIL: could not synchronize the idle edges");
        return 1;
    }
    quiet_incoming->Close();
    quiet_outgoing->Close();
    listener->Close();
    return 0;
}

int RunActiveSide(uint16_t port) {
    auto forward = std::make_shared<TransportSHM>();
    auto backward = std::make_shared<TransportSHM>();
    if (!forward->Connect("127.0.0.1", port) || !backward->Connect("127.0.0.1", port)) {
        LOG_ERROR("FAIL: could not connect to the shared-memory rendezvous");
        return 1;
    }

    const int fds_before = OpenFdCount();
    if (!HandshakeEdge(forward, 7) || !HandshakeEdge(backward, 7)) {
        LOG_ERROR("FAIL: control handshake failed");
        return 1;
    }
    if (fds_before - OpenFdCount() != 2) {
        LOG_ERROR("FAIL: both rendezvous sockets must be closed after the handshake");
        return 1;
    }

    const std::vector<char> payload = MakePayload(kForwardBytes, 1);
    std::vector<char> received(kBackwardBytes, 0);
    size_t produce_progress = 0;
    size_t consume_progress = 0;
    bool produce_done = false;
    bool consume_done = false;
    bool blocked_on_full_ring = false;

    const int deadline = kDeadlineSeconds * 1000;
    for (int waited = 0; (!produce_done || !consume_done) && waited < deadline;) {
        bool progressed = false;
        if (!produce_done) {
            const size_t before = produce_progress;
            if (!forward->TrySend(payload.data(), payload.size(), &produce_progress, &produce_done)) {
                LOG_ERROR("FAIL: send to the peer failed");
                return 1;
            }
            progressed = progressed || produce_progress != before;
            blocked_on_full_ring = blocked_on_full_ring || (produce_progress > 0 && !produce_done);
        }
        if (!consume_done) {
            const size_t before = consume_progress;
            if (!backward->TryRecv(received.data(), received.size(), &consume_progress, &consume_done)) {
                LOG_ERROR("FAIL: receive from the peer failed");
                return 1;
            }
            progressed = progressed || consume_progress != before;
        }
        if (produce_done && consume_done) {
            break;
        }

        struct pollfd waits[2] = {PollFor(forward->SendWait().fd), PollFor(backward->RecvWait().fd)};
        if (poll(waits, 2, progressed ? 0 : 200) < 0) {
            LOG_ERROR("FAIL: poll on the shared-memory descriptors failed");
            return 1;
        }
        if (!progressed) {
            waited += 200;
        }
    }

    if (!produce_done || !consume_done) {
        LOG_ERROR("FAIL: transfer did not finish within {}s", kDeadlineSeconds);
        return 1;
    }
    Check(blocked_on_full_ring, "the payload must not fit the ring in a single write");
    Check(Matches(received, 2), "the payload received from the peer must match");

    // Let the peer close the data edges, then both directions must fail instead of reporting
    // an unfinished transfer.
    if (!SignalSync(g_to_child) || !SyncOn(g_to_parent)) {
        LOG_ERROR("FAIL: could not synchronize the data edges");
        return 1;
    }
    size_t progress = 0;
    bool done = false;
    Check(!forward->TrySend(payload.data(), 8, &progress, &done), "a closed ring must fail to send");
    progress = 0;
    done = false;
    Check(!backward->TryRecv(received.data(), 8, &progress, &done), "a closed ring must fail to receive once drained");
    forward->Close();
    backward->Close();

    auto quiet_forward = std::make_shared<TransportSHM>();
    auto quiet_backward = std::make_shared<TransportSHM>();
    if (!quiet_forward->Connect("127.0.0.1", port) || !quiet_backward->Connect("127.0.0.1", port)) {
        LOG_ERROR("FAIL: could not connect the idle edges");
        return 1;
    }
    if (!HandshakeEdge(quiet_forward, 9) || !HandshakeEdge(quiet_backward, 9)) {
        LOG_ERROR("FAIL: idle control handshake failed");
        return 1;
    }

    // A fresh space_fd starts readable, so a send-wait check would pass vacuously. Fill the
    // send edge while the peer does not read, draining the space level, before the peer
    // closes: only then does the send side prove the close really woke it.
    size_t fill_progress = 0;
    bool fill_done = false;
    if (!quiet_forward->TrySend(payload.data(), payload.size(), &fill_progress, &fill_done)) {
        LOG_ERROR("FAIL: idle fill send failed");
        return 1;
    }
    Check(fill_progress > 0 && !fill_done, "the fill send must stop at a full ring");
    {
        struct pollfd wait = PollFor(quiet_forward->SendWait().fd);
        Check(poll(&wait, 1, 0) == 0, "a full ring must not leave the send descriptor readable");
    }
    if (!SignalSync(g_to_child)) {
        LOG_ERROR("FAIL: could not synchronize the idle edges");
        return 1;
    }

    const auto wakeup_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(kDeadlineSeconds);
    bool forward_ready = false;
    bool backward_ready = false;
    while ((!forward_ready || !backward_ready) && std::chrono::steady_clock::now() < wakeup_deadline) {
        struct pollfd waits[2] = {PollFor(quiet_forward->SendWait().fd), PollFor(quiet_backward->RecvWait().fd)};
        if (poll(waits, 2, 50) < 0) {
            LOG_ERROR("FAIL: poll on the shared-memory descriptors failed");
            return 1;
        }
        forward_ready = forward_ready || (waits[0].revents & POLLIN) != 0;
        backward_ready = backward_ready || (waits[1].revents & POLLIN) != 0;
    }
    Check(forward_ready, "the send descriptor must report the peer's close");
    Check(backward_ready, "the recv descriptor must report the peer's close");

    quiet_forward->Close();
    quiet_backward->Close();
    return g_failures == 0 ? 0 : 1;
}
} // namespace

int main() {
    uint16_t port = 0;
    const int lease_fd = LeaseFreePort(&port);
    if (lease_fd < 0) {
        LOG_ERROR("FAIL: could not lease a port for the rendezvous");
        return 1;
    }

    int to_child[2] = {-1, -1};
    int to_parent[2] = {-1, -1};
    if (pipe(to_child) != 0 || pipe(to_parent) != 0) {
        LOG_ERROR("FAIL: could not create the synchronization pipes");
        close(lease_fd);
        return 1;
    }

    const pid_t pid = fork();
    if (pid < 0) {
        LOG_ERROR("FAIL: could not fork the passive side");
        close(lease_fd);
        return 1;
    }
    if (pid == 0) {
        close(to_child[1]);
        close(to_parent[0]);
        g_to_child = to_child[0];
        g_to_parent = to_parent[1];
        // Every log line is flushed as it is written, so _exit cannot lose output.
        _exit(RunPassiveSide(port));
    }

    close(to_child[0]);
    close(to_parent[1]);
    g_to_child = to_child[1];
    g_to_parent = to_parent[0];

    const int active_result = RunActiveSide(port);

    int status = 0;
    waitpid(pid, &status, 0);
    close(g_to_child);
    close(g_to_parent);
    close(lease_fd);

    const bool passive_ok = WIFEXITED(status) && WEXITSTATUS(status) == 0;
    Check(passive_ok, "the passive side must complete");

    if (active_result == 0 && passive_ok) {
        LOG_INFO("PASS: shared-memory transport verified ({} bytes streamed through a {} byte ring)", kForwardBytes,
                 kRingCapacity);
        return 0;
    }
    LOG_ERROR("FAIL: shared-memory transport verification failed");
    return 1;
}
