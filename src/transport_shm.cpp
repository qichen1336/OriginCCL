#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <new>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>
#include <fmt/format.h>
#include "transport_shm.h"
#include "logger.h"

namespace {
// Ring size per channel edge. Any message, however large, is streamed through it.
constexpr size_t kRingCapacity = 1024 * 1024;
constexpr size_t kRingFdCount = 3;
constexpr int kRendezvousBacklog = 64;
constexpr int kRendezvousRetries = 30;
constexpr int kMaxReceivedFds = 8;
constexpr int kConnectRetryMicros = 200;

// The rendezvous endpoint is host-local and named after the peer's data port, which that
// rank keeps bound as a TCP listener for the whole run and so is unique on this host.
socklen_t RendezvousAddress(uint16_t port, struct sockaddr_un* addr) {
    const std::string name = fmt::format("occl-shm-{}", port);
    std::memset(addr, 0, sizeof(*addr));
    addr->sun_family = AF_UNIX;
    if (name.size() + 1 > sizeof(addr->sun_path)) {
        return 0;
    }
    addr->sun_path[0] = '\0';
    std::memcpy(addr->sun_path + 1, name.data(), name.size());
    return static_cast<socklen_t>(offsetof(struct sockaddr_un, sun_path) + 1 + name.size());
}

bool SendFds(int sockfd, const int* fds, size_t count) {
    char marker = static_cast<char>(count);
    struct iovec io;
    io.iov_base = &marker;
    io.iov_len = 1;

    char control[CMSG_SPACE(sizeof(int) * kRingFdCount)];
    struct msghdr msg;
    std::memset(&msg, 0, sizeof(msg));
    msg.msg_iov = &io;
    msg.msg_iovlen = 1;
    msg.msg_control = control;
    msg.msg_controllen = CMSG_SPACE(sizeof(int) * count);

    struct cmsghdr* cmsg = CMSG_FIRSTHDR(&msg);
    cmsg->cmsg_level = SOL_SOCKET;
    cmsg->cmsg_type = SCM_RIGHTS;
    cmsg->cmsg_len = CMSG_LEN(sizeof(int) * count);
    std::memcpy(CMSG_DATA(cmsg), fds, sizeof(int) * count);

    ssize_t sent;
    do {
        sent = sendmsg(sockfd, &msg, 0);
    } while (sent < 0 && errno == EINTR);
    if (sent != 1) {
        LOG_ERROR("Failed to hand over the ring fds: {}", strerror(errno));
        return false;
    }
    return true;
}

bool ReceiveFds(int sockfd, int* fds, size_t expected) {
    char marker = 0;
    struct iovec io;
    io.iov_base = &marker;
    io.iov_len = 1;

    char control[CMSG_SPACE(sizeof(int) * kMaxReceivedFds)];
    struct msghdr msg;
    std::memset(&msg, 0, sizeof(msg));
    msg.msg_iov = &io;
    msg.msg_iovlen = 1;
    msg.msg_control = control;
    msg.msg_controllen = sizeof(control);

    ssize_t received;
    do {
        received = recvmsg(sockfd, &msg, 0);
    } while (received < 0 && errno == EINTR);
    if (received < 0) {
        LOG_ERROR("Failed to receive the ring fds: {}", strerror(errno));
        return false;
    }
    if (received == 0) {
        LOG_ERROR("Rendezvous closed before the ring was handed over");
        return false;
    }

    int gathered[kMaxReceivedFds];
    size_t found = 0;
    for (struct cmsghdr* cmsg = CMSG_FIRSTHDR(&msg); cmsg != nullptr; cmsg = CMSG_NXTHDR(&msg, cmsg)) {
        if (cmsg->cmsg_level != SOL_SOCKET || cmsg->cmsg_type != SCM_RIGHTS) {
            continue;
        }
        const size_t count = (cmsg->cmsg_len - CMSG_LEN(0)) / sizeof(int);
        for (size_t i = 0; i < count && found < kMaxReceivedFds; ++i) {
            std::memcpy(&gathered[found], CMSG_DATA(cmsg) + i * sizeof(int), sizeof(int));
            ++found;
        }
    }

    // Anything unexpected is closed rather than leaked, including the fds of a peer that
    // sent the wrong ring layout.
    // Anything unexpected is closed rather than leaked, including the fds of a peer that
    // sent the wrong ring layout.
    if (marker != static_cast<char>(expected) || found != expected) {
        LOG_ERROR("Rendezvous handed over {} fds, expected {}", found, expected);
        for (size_t i = 0; i < found; ++i) {
            close(gathered[i]);
        }
        return false;
    }
    for (size_t i = 0; i < expected; ++i) {
        fds[i] = gathered[i];
    }
    return true;
}

void Signal(int fd) {
    if (fd < 0) {
        return;
    }
    uint64_t one = 1;
    ssize_t written = write(fd, &one, sizeof(one));
    (void)written;
}

void ConsumeSignal(int fd) {
    if (fd < 0) {
        return;
    }
    uint64_t value = 0;
    ssize_t read_count = read(fd, &value, sizeof(value));
    (void)read_count;
}
} // namespace

// Shared header plus payload. The cursors are monotonic byte counters: a full ring is
// "head - tail == capacity", so no slot is sacrificed to tell full from empty. Each counter
// owns its cache line; the producer writes head, the consumer writes tail.
struct TransportSHM::Ring {
    alignas(64) std::atomic<uint64_t> head{0};
    alignas(64) std::atomic<uint64_t> tail{0};
    alignas(64) std::atomic<uint32_t> closed{0};
};

static_assert(std::atomic<uint64_t>::is_always_lock_free, "shared ring cursors must be lock-free");
static_assert(std::atomic<uint32_t>::is_always_lock_free, "shared ring closed flag must be lock-free");

TransportSHM::TransportSHM() {}

TransportSHM::~TransportSHM() {
    Close();
}

bool TransportSHM::Listen(uint16_t port) {
    struct sockaddr_un addr;
    const socklen_t addr_length = RendezvousAddress(port, &addr);
    if (addr_length == 0) {
        LOG_ERROR("Shared-memory rendezvous name for port {} is too long", port);
        return false;
    }

    listen_fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (listen_fd < 0) {
        LOG_ERROR("Failed to create the shared-memory rendezvous socket: {}", strerror(errno));
        return false;
    }
    if (bind(listen_fd, reinterpret_cast<struct sockaddr*>(&addr), addr_length) != 0) {
        LOG_ERROR("Failed to bind the shared-memory rendezvous socket for port {}: {}", port, strerror(errno));
        Close();
        return false;
    }
    if (listen(listen_fd, kRendezvousBacklog) != 0) {
        LOG_ERROR("Failed to listen on the shared-memory rendezvous socket for port {}: {}", port, strerror(errno));
        Close();
        return false;
    }

    listen_port = port;
    return true;
}

std::shared_ptr<Transport> TransportSHM::Accept() {
    if (listen_fd < 0) {
        LOG_ERROR("Not in listen mode");
        return nullptr;
    }

    int fd;
    do {
        fd = accept(listen_fd, nullptr, nullptr);
    } while (fd < 0 && errno == EINTR);
    if (fd < 0) {
        LOG_ERROR("Failed to accept a shared-memory rendezvous connection: {}", strerror(errno));
        return nullptr;
    }

    auto transport = std::make_shared<TransportSHM>();
    transport->rendezvous_fd = fd;
    if (!transport->CreateRing() || !transport->HandOverRing()) {
        transport->Close();
        return nullptr;
    }
    return transport;
}

bool TransportSHM::Connect(const std::string& addr, uint16_t port) {
    (void)addr;

    struct sockaddr_un peer_addr;
    const socklen_t addr_length = RendezvousAddress(port, &peer_addr);
    if (addr_length == 0) {
        LOG_ERROR("Shared-memory rendezvous name for port {} is too long", port);
        return false;
    }

    int fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        LOG_ERROR("Failed to create the shared-memory rendezvous socket: {}", strerror(errno));
        return false;
    }

    int rc = -1;
    for (int retry = 0; retry < kRendezvousRetries; ++retry) {
        rc = connect(fd, reinterpret_cast<struct sockaddr*>(&peer_addr), addr_length);
        if (rc == 0) {
            break;
        }
        usleep(kConnectRetryMicros);
    }
    if (rc != 0) {
        LOG_ERROR("Failed to connect to the shared-memory rendezvous socket for port {}: {}", port, strerror(errno));
        close(fd);
        return false;
    }

    rendezvous_fd = fd;
    if (!RecvRingFds()) {
        Close();
        return false;
    }
    connected = true;
    return true;
}

bool TransportSHM::CreateRing() {
    const size_t region = sizeof(Ring) + kRingCapacity;

    ring_fd = memfd_create("originccl-shm-ring", MFD_CLOEXEC);
    if (ring_fd < 0) {
        LOG_ERROR("Failed to create the shared-memory ring: {}", strerror(errno));
        return false;
    }
    if (ftruncate(ring_fd, static_cast<off_t>(region)) != 0) {
        LOG_ERROR("Failed to size the shared-memory ring: {}", strerror(errno));
        return false;
    }

    void* base = mmap(nullptr, region, PROT_READ | PROT_WRITE, MAP_SHARED, ring_fd, 0);
    if (base == MAP_FAILED) {
        LOG_ERROR("Failed to map the shared-memory ring: {}", strerror(errno));
        return false;
    }
    // The mapping is the ring's storage but not yet a Ring object; begin its lifetime here so
    // the atomics have a constructor and can be used. The receiving peer must not do this.
    ring = new (base) Ring();

    // The notification fds describe the ring's levels: recv is readable while bytes are
    // unread, send while there is room to write. An empty ring has room, so the send side
    // starts readable and that first level is what drives the very first send.
    data_fd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    space_fd = eventfd(1, EFD_CLOEXEC | EFD_NONBLOCK);
    if (data_fd < 0 || space_fd < 0) {
        LOG_ERROR("Failed to create the shared-memory notification fds: {}", strerror(errno));
        return false;
    }

    connected = true;
    return true;
}

bool TransportSHM::HandOverRing() {
    const int fds[kRingFdCount] = {ring_fd, data_fd, space_fd};
    if (!SendFds(rendezvous_fd, fds, kRingFdCount)) {
        return false;
    }
    sent = true;
    FinishRendezvous();
    return true;
}

bool TransportSHM::RecvRingFds() {
    int fds[kRingFdCount] = {-1, -1, -1};
    if (!ReceiveFds(rendezvous_fd, fds, kRingFdCount)) {
        return false;
    }

    // The ring arrives as an fd, so there is no name to trust and nothing to unlink; the
    // only thing to verify is that the mapping really is one whole ring region.
    const size_t region = sizeof(Ring) + kRingCapacity;
    struct stat info;
    if (fstat(fds[0], &info) != 0 || info.st_size != static_cast<off_t>(region)) {
        LOG_ERROR("Peer handed over a shared-memory ring of unexpected size");
        close(fds[0]);
        close(fds[1]);
        close(fds[2]);
        return false;
    }

    ring_fd = fds[0];
    data_fd = fds[1];
    space_fd = fds[2];

    void* base = mmap(nullptr, region, PROT_READ | PROT_WRITE, MAP_SHARED, ring_fd, 0);
    if (base == MAP_FAILED) {
        LOG_ERROR("Failed to map the peer's shared-memory ring: {}", strerror(errno));
        return false;
    }
    // The creating peer already began the Ring object's lifetime; this side only maps it.
    ring = static_cast<Ring*>(base);

    received = true;
    FinishRendezvous();
    return true;
}

// The rendezvous socket only carries the fd hand-over and the one control handshake, so it
// is closed once both have happened and the ring is the whole connection.
void TransportSHM::FinishRendezvous() {
    if (!sent || !received || rendezvous_fd < 0) {
        return;
    }
    close(rendezvous_fd);
    rendezvous_fd = -1;
}

bool TransportSHM::Send(const void* data, size_t size) {
    if (rendezvous_fd < 0) {
        LOG_ERROR("Not connected to send");
        return false;
    }

    const char* buffer = static_cast<const char*>(data);
    size_t total = 0;
    while (total < size) {
        ssize_t sent_bytes;
        do {
            sent_bytes = send(rendezvous_fd, buffer + total, size - total, MSG_NOSIGNAL);
        } while (sent_bytes < 0 && errno == EINTR);
        if (sent_bytes <= 0) {
            LOG_ERROR("Failed to send on the shared-memory rendezvous: {}", strerror(errno));
            return false;
        }
        total += static_cast<size_t>(sent_bytes);
    }

    sent = true;
    FinishRendezvous();
    return true;
}

bool TransportSHM::Recv(void* data, size_t size) {
    if (rendezvous_fd < 0) {
        LOG_ERROR("Not connected to recv");
        return false;
    }

    char* buffer = static_cast<char*>(data);
    size_t total = 0;
    while (total < size) {
        ssize_t received;
        do {
            received = recv(rendezvous_fd, buffer + total, size - total, 0);
        } while (received < 0 && errno == EINTR);
        if (received <= 0) {
            LOG_ERROR("Failed to recv on the shared-memory rendezvous: {}", strerror(errno));
            return false;
        }
        total += static_cast<size_t>(received);
    }

    received = true;
    FinishRendezvous();
    return true;
}

char* TransportSHM::Data() {
    return reinterpret_cast<char*>(ring) + sizeof(Ring);
}

bool TransportSHM::PeerClosed() const {
    return ring == nullptr || ring->closed.load(std::memory_order_acquire) != 0;
}

size_t TransportSHM::Room() const {
    const uint64_t head_seen = ring->head.load(std::memory_order_relaxed);
    const uint64_t tail_seen = ring->tail.load(std::memory_order_acquire);
    return kRingCapacity - static_cast<size_t>(head_seen - tail_seen);
}

// The cursors live in the shared header and nowhere else: each ring has one producer and one
// consumer, but which of the two this object drives is decided by the caller, so the shared
// state is the only truth both roles read.
size_t TransportSHM::Produce(const char* src, size_t size) {
    const uint64_t head_seen = ring->head.load(std::memory_order_relaxed);
    const uint64_t tail_seen = ring->tail.load(std::memory_order_acquire);
    const size_t used = static_cast<size_t>(head_seen - tail_seen);
    const size_t n = std::min(kRingCapacity - used, size);
    if (n == 0) {
        return 0;
    }

    char* base = Data();
    const size_t offset = static_cast<size_t>(head_seen % kRingCapacity);
    const size_t first = std::min(n, kRingCapacity - offset);
    std::memcpy(base + offset, src, first);
    if (n > first) {
        std::memcpy(base, src + first, n - first);
    }

    ring->head.store(head_seen + n, std::memory_order_release);
    return n;
}

size_t TransportSHM::Consume(char* dst, size_t size) {
    // Clear the recv level before looking at the ring, so an append that races with this is
    // either seen by the load below or re-raises the wakeup.
    ConsumeSignal(data_fd);

    const uint64_t head_seen = ring->head.load(std::memory_order_acquire);
    const uint64_t tail_seen = ring->tail.load(std::memory_order_relaxed);
    const size_t used = static_cast<size_t>(head_seen - tail_seen);
    if (used == 0) {
        return 0;
    }

    const size_t n = std::min(used, size);
    const char* base = Data();
    const size_t offset = static_cast<size_t>(tail_seen % kRingCapacity);
    const size_t first = std::min(n, kRingCapacity - offset);
    std::memcpy(dst, base + offset, first);
    if (n > first) {
        std::memcpy(dst + first, base, n - first);
    }

    ring->tail.store(tail_seen + n, std::memory_order_release);

    if (n < used) {
        // Bytes stay behind for a later step, and the producer only announces what it
        // appends, so keep the recv level readable for whichever step consumes them.
        Signal(data_fd);
    }
    if (n > 0) {
        Signal(space_fd);
    }
    return n;
}

bool TransportSHM::TrySend(const void* data, size_t size, size_t* progress, bool* done) {
    if (!connected || ring == nullptr) {
        LOG_ERROR("Not connected to send");
        return false;
    }
    if (PeerClosed()) {
        LOG_INFO("Shared-memory ring closed by peer when send");
        return false;
    }

    // Clear the send level before reading the ring, then re-raise it if room is left: room
    // freed from here on raises it again, so a send that stops at a full ring cannot miss it.
    ConsumeSignal(space_fd);

    const char* src = static_cast<const char*>(data);
    const size_t written = Produce(src + *progress, size - *progress);
    if (written > 0) {
        *progress += written;
        Signal(data_fd);
    }
    if (Room() > 0) {
        Signal(space_fd);
    }

    *done = (*progress == size);
    return true;
}

bool TransportSHM::TryRecv(void* data, size_t size, size_t* progress, bool* done) {
    if (!connected || ring == nullptr) {
        LOG_ERROR("Not connected to recv");
        return false;
    }

    if (*progress < size) {
        *progress += Consume(static_cast<char*>(data) + *progress, size - *progress);
    }
    *done = (*progress == size);

    // A peer that finished closes while bytes it already wrote may still sit in the ring, so
    // the close only ends the transfer once there is nothing left to drain.
    if (!*done && PeerClosed()) {
        LOG_INFO("Shared-memory ring closed by peer when recv");
        return false;
    }
    return true;
}

void TransportSHM::Close() {
    if (ring != nullptr) {
        // Announce the shutdown while the mapping is still there, so a blocked peer fails
        // instead of waiting for data that will never come.
        ring->closed.store(1, std::memory_order_release);
        Signal(data_fd);
        Signal(space_fd);
        munmap(ring, sizeof(Ring) + kRingCapacity);
        ring = nullptr;
    }
    if (ring_fd >= 0) {
        close(ring_fd);
        ring_fd = -1;
    }
    if (data_fd >= 0) {
        close(data_fd);
        data_fd = -1;
    }
    if (space_fd >= 0) {
        close(space_fd);
        space_fd = -1;
    }
    if (rendezvous_fd >= 0) {
        close(rendezvous_fd);
        rendezvous_fd = -1;
    }
    if (listen_fd >= 0) {
        close(listen_fd);
        listen_fd = -1;
    }
    connected = false;
}
