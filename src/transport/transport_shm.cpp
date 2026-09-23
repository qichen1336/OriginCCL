#include <algorithm>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <poll.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include "transport/transport_shm.h"
#include "logger.h"

namespace {
constexpr uint32_t kShmResourceMagic = 0x53484d31; // "SHM1"
constexpr size_t kShmResourceFdCount = 3;
constexpr int kShmRendezvousBacklog = 128;

std::string ErrnoText() {
    return strerror(errno);
}

struct RendezvousAddress {
    sockaddr_un addr{};
    socklen_t length = 0;

    bool Set(const std::string& path) {
        if (path.empty() || path.size() >= sizeof(addr.sun_path)) {
            LOG_ERROR("Invalid shared-memory rendezvous path '{}'", path);
            return false;
        }
        addr.sun_family = AF_UNIX;
        std::memcpy(addr.sun_path, path.c_str(), path.size());
        length = static_cast<socklen_t>(sizeof(addr));
        return true;
    }
};
} // namespace

void TransportShm::RingResources::Close() {
    if (ring >= 0) {
        close(ring);
        ring = -1;
    }
    if (data_ready >= 0) {
        close(data_ready);
        data_ready = -1;
    }
    if (space_ready >= 0) {
        close(space_ready);
        space_ready = -1;
    }
}

TransportShm::~TransportShm() {
    Close();
}

bool TransportShm::Listen(const std::string& addr, uint16_t port) {
    (void)port;
    RendezvousAddress address;
    if (!address.Set(addr)) {
        return false;
    }
    unlink(addr.c_str());

    const int fd = socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        LOG_ERROR("Failed to create shared-memory rendezvous socket: {}", ErrnoText());
        return false;
    }
    if (bind(fd, reinterpret_cast<sockaddr*>(&address.addr), address.length) != 0) {
        LOG_ERROR("Failed to bind shared-memory rendezvous {}: {}", addr, ErrnoText());
        close(fd);
        return false;
    }
    if (listen(fd, kShmRendezvousBacklog) != 0) {
        LOG_ERROR("Failed to listen on shared-memory rendezvous {}: {}", addr, ErrnoText());
        close(fd);
        unlink(addr.c_str());
        return false;
    }
    listen_fd = fd;
    listen_path = addr;
    return true;
}

std::shared_ptr<Transport> TransportShm::Accept() {
    if (listen_fd < 0) {
        LOG_ERROR("Not in listen mode");
        return nullptr;
    }
    const int control = accept4(listen_fd, nullptr, nullptr, SOCK_CLOEXEC);
    if (control < 0) {
        LOG_ERROR("Failed to accept a shared-memory rendezvous: {}", ErrnoText());
        return nullptr;
    }

    RingResources incoming;
    TransportDirection peer_direction = TransportDirection::Bidirectional;
    if (!RecvResources(control, &incoming, &peer_direction)) {
        close(control);
        return nullptr;
    }

    auto transport = std::make_shared<TransportShm>();
    transport->control_fd = control;
    const TransportDirection local =
        peer_direction == TransportDirection::Send ? TransportDirection::Receive : TransportDirection::Send;
    if (!transport->Adopt(incoming, local)) {
        transport->Close();
        return nullptr;
    }
    return transport;
}

bool TransportShm::Connect(const std::string& rendezvous_path, uint16_t port) {
    (void)port;
    if (direction_ != TransportDirection::Send && direction_ != TransportDirection::Receive) {
        LOG_ERROR("A shared-memory endpoint must be a producer or a consumer, got direction {}",
                  static_cast<int>(direction_));
        return false;
    }
    RendezvousAddress address;
    if (!address.Set(rendezvous_path)) {
        return false;
    }
    if (!CreateRing()) {
        Close();
        return false;
    }

    const int fd = socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        LOG_ERROR("Failed to create shared-memory control socket: {}", ErrnoText());
        Close();
        return false;
    }
    if (connect(fd, reinterpret_cast<sockaddr*>(&address.addr), address.length) != 0) {
        LOG_ERROR("Failed to connect to shared-memory rendezvous {}: {}", rendezvous_path, ErrnoText());
        close(fd);
        Close();
        return false;
    }
    if (!SendResources(fd)) {
        close(fd);
        Close();
        return false;
    }

    control_fd = fd;
    connected = true;
    return true;
}

bool TransportShm::CreateRing() {
    resources.ring = memfd_create("originccl-shm", MFD_CLOEXEC);
    if (resources.ring < 0) {
        LOG_ERROR("Failed to create the shared-memory ring: {}", ErrnoText());
        return false;
    }
    if (ftruncate(resources.ring, static_cast<off_t>(kShmRingMapSize)) != 0) {
        LOG_ERROR("Failed to size the shared-memory ring: {}", ErrnoText());
        return false;
    }
    if (!MapRing(resources.ring)) {
        return false;
    }
    resources.data_ready = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    resources.space_ready = eventfd(1, EFD_NONBLOCK | EFD_CLOEXEC);
    if (resources.data_ready < 0 || resources.space_ready < 0) {
        LOG_ERROR("Failed to create the shared-memory notifications: {}", ErrnoText());
        return false;
    }
    return true;
}

bool TransportShm::MapRing(int fd) {
    resources.ring = fd;
    void* addr = mmap(nullptr, kShmRingMapSize, PROT_READ | PROT_WRITE, MAP_SHARED, resources.ring, 0);
    if (addr == MAP_FAILED) {
        map = nullptr;
        LOG_ERROR("Failed to map the shared-memory ring: {}", ErrnoText());
        return false;
    }
    map = static_cast<char*>(addr);
    new (map) ShmRingCursors{};
    return true;
}

bool TransportShm::Adopt(const RingResources& incoming, TransportDirection direction) {
    resources = incoming;
    direction_ = direction;
    if (!MapRing(resources.ring)) {
        return false;
    }
    connected = true;
    return true;
}

bool TransportShm::SendResources(int fd) const {
    const int fds[kShmResourceFdCount] = {resources.ring, resources.data_ready, resources.space_ready};
    const ResourceMessage message{kShmResourceMagic, static_cast<int32_t>(direction_)};

    alignas(cmsghdr) char control[CMSG_SPACE(kShmResourceFdCount * sizeof(int))] = {};
    iovec iov{const_cast<ResourceMessage*>(&message), sizeof(message)};
    msghdr header{};
    header.msg_iov = &iov;
    header.msg_iovlen = 1;
    header.msg_control = control;
    header.msg_controllen = sizeof(control);

    cmsghdr* cmsg = CMSG_FIRSTHDR(&header);
    cmsg->cmsg_level = SOL_SOCKET;
    cmsg->cmsg_type = SCM_RIGHTS;
    cmsg->cmsg_len = CMSG_LEN(sizeof(fds));
    std::memcpy(CMSG_DATA(cmsg), fds, sizeof(fds));

    ssize_t sent = 0;
    do {
        sent = sendmsg(fd, &header, MSG_NOSIGNAL);
    } while (sent < 0 && errno == EINTR);
    if (sent != static_cast<ssize_t>(sizeof(message))) {
        LOG_ERROR("Failed to transfer the shared-memory ring descriptors: {}", ErrnoText());
        return false;
    }
    return true;
}

bool TransportShm::RecvResources(int fd, RingResources* incoming, TransportDirection* direction) const {
    ResourceMessage message{};
    alignas(cmsghdr) char control[CMSG_SPACE(kShmResourceFdCount * sizeof(int))] = {};
    iovec iov{&message, sizeof(message)};
    msghdr header{};
    header.msg_iov = &iov;
    header.msg_iovlen = 1;
    header.msg_control = control;
    header.msg_controllen = sizeof(control);

    ssize_t received = 0;
    do {
        received = recvmsg(fd, &header, MSG_CMSG_CLOEXEC);
    } while (received < 0 && errno == EINTR);

    int fds[kShmResourceFdCount] = {-1, -1, -1};
    bool transferred = false;
    for (cmsghdr* cmsg = CMSG_FIRSTHDR(&header); cmsg != nullptr; cmsg = CMSG_NXTHDR(&header, cmsg)) {
        if (cmsg->cmsg_level != SOL_SOCKET || cmsg->cmsg_type != SCM_RIGHTS) {
            continue;
        }
        if (cmsg->cmsg_len != CMSG_LEN(sizeof(int) * kShmResourceFdCount)) {
            continue;
        }
        std::memcpy(fds, CMSG_DATA(cmsg), sizeof(int) * kShmResourceFdCount);
        transferred = true;
    }

    RingResources received_resources{fds[0], fds[1], fds[2]};
    const bool valid = transferred && received == static_cast<ssize_t>(sizeof(message)) &&
                       message.magic == kShmResourceMagic &&
                       (message.direction == static_cast<int32_t>(TransportDirection::Send) ||
                        message.direction == static_cast<int32_t>(TransportDirection::Receive));
    if (!valid) {
        if (received < 0) {
            LOG_ERROR("Failed to receive the shared-memory rendezvous message: {}", ErrnoText());
        } else {
            LOG_ERROR("Invalid shared-memory rendezvous message");
        }
        received_resources.Close();
        return false;
    }
    *incoming = received_resources;
    *direction = static_cast<TransportDirection>(message.direction);
    return true;
}

bool TransportShm::Send(const void* data, size_t size) {
    if (control_fd >= 0) {
        if (!SendControl(data, size)) {
            return false;
        }
        char acknowledgement = 0;
        if (!RecvControl(&acknowledgement, sizeof(acknowledgement))) {
            return false;
        }
        CloseControl();
        return true;
    }
    if (!connected || !IsProducer()) {
        LOG_ERROR("Shared-memory endpoint is not a connected producer, cannot send");
        return false;
    }

    size_t progress = 0;
    bool done = false;
    while (!done) {
        if (!TrySend(data, size, &progress, &done)) {
            return false;
        }
        if (!done && !WaitReadable(resources.space_ready)) {
            return false;
        }
    }
    return true;
}

bool TransportShm::Recv(void* data, size_t size) {
    if (control_fd >= 0) {
        if (!RecvControl(data, size)) {
            return false;
        }
        const char acknowledgement = 1;
        if (!SendControl(&acknowledgement, sizeof(acknowledgement))) {
            return false;
        }
        CloseControl();
        return true;
    }
    if (!connected || IsProducer()) {
        LOG_ERROR("Shared-memory endpoint is not a connected consumer, cannot receive");
        return false;
    }

    size_t progress = 0;
    bool done = false;
    while (!done) {
        if (!TryRecv(data, size, &progress, &done)) {
            return false;
        }
        if (!done && !WaitReadable(resources.data_ready)) {
            return false;
        }
    }
    return true;
}

bool TransportShm::TrySend(const void* data, size_t size, size_t* progress, bool* done) {
    if (!connected || !IsProducer()) {
        LOG_ERROR("Shared-memory endpoint is not a connected producer, cannot send");
        return false;
    }

    size_t moved = 0;
    while (*progress < size) {
        const size_t chunk = Produce(static_cast<const char*>(data), size, *progress);
        *progress += chunk;
        moved += chunk;
        if (chunk > 0) {
            continue;
        }
        Drain(resources.space_ready);
        if (SpaceAvailable() == 0) {
            break;
        }
    }
    *done = (*progress == size);
    if (moved > 0) {
        Notify(resources.data_ready);
    }
    return true;
}

bool TransportShm::TryRecv(void* data, size_t size, size_t* progress, bool* done) {
    if (!connected || IsProducer()) {
        LOG_ERROR("Shared-memory endpoint is not a connected consumer, cannot receive");
        return false;
    }

    size_t moved = 0;
    while (*progress < size) {
        const size_t chunk = Consume(static_cast<char*>(data), size, *progress);
        *progress += chunk;
        moved += chunk;
        if (chunk > 0) {
            continue;
        }
        Drain(resources.data_ready);
        if (DataAvailable() == 0) {
            break;
        }
    }
    *done = (*progress == size);
    if (moved > 0) {
        Notify(resources.space_ready);
    }
    return true;
}

size_t TransportShm::Produce(const char* data, size_t size, size_t progress) {
    const uint64_t head = Cursors()->head.load(std::memory_order_relaxed);
    const uint64_t tail = Cursors()->tail.load(std::memory_order_acquire);
    const size_t room = kShmRingCapacity - static_cast<size_t>(head - tail);
    if (room == 0) {
        return 0;
    }
    const size_t offset = static_cast<size_t>(head % kShmRingCapacity);
    const size_t chunk = std::min({room, size - progress, kShmRingCapacity - offset});
    std::memcpy(RingData() + offset, data + progress, chunk);
    Cursors()->head.store(head + chunk, std::memory_order_release);
    return chunk;
}

size_t TransportShm::Consume(char* data, size_t size, size_t progress) {
    const uint64_t tail = Cursors()->tail.load(std::memory_order_relaxed);
    const uint64_t head = Cursors()->head.load(std::memory_order_acquire);
    const size_t available = static_cast<size_t>(head - tail);
    if (available == 0) {
        return 0;
    }
    const size_t offset = static_cast<size_t>(tail % kShmRingCapacity);
    const size_t chunk = std::min({available, size - progress, kShmRingCapacity - offset});
    std::memcpy(data + progress, RingData() + offset, chunk);
    Cursors()->tail.store(tail + chunk, std::memory_order_release);
    return chunk;
}

size_t TransportShm::SpaceAvailable() const {
    const uint64_t head = Cursors()->head.load(std::memory_order_relaxed);
    const uint64_t tail = Cursors()->tail.load(std::memory_order_acquire);
    return kShmRingCapacity - static_cast<size_t>(head - tail);
}

size_t TransportShm::DataAvailable() const {
    const uint64_t head = Cursors()->head.load(std::memory_order_acquire);
    const uint64_t tail = Cursors()->tail.load(std::memory_order_relaxed);
    return static_cast<size_t>(head - tail);
}

void TransportShm::Drain(int efd) const {
    uint64_t value = 0;
    while (read(efd, &value, sizeof(value)) > 0) {}
}

void TransportShm::Notify(int efd) const {
    const uint64_t one = 1;
    if (write(efd, &one, sizeof(one)) != static_cast<ssize_t>(sizeof(one))) {
        LOG_WARN("Failed to notify the shared-memory peer: {}", ErrnoText());
    }
}

bool TransportShm::WaitReadable(int efd) const {
    pollfd pfd{efd, POLLIN, 0};
    int ready = 0;
    do {
        ready = poll(&pfd, 1, -1);
    } while (ready < 0 && errno == EINTR);
    if (ready <= 0) {
        LOG_ERROR("Failed to wait for shared-memory readiness: {}", ErrnoText());
        return false;
    }
    return true;
}

bool TransportShm::SendControl(const void* data, size_t size) const {
    const char* buffer = static_cast<const char*>(data);
    size_t sent = 0;
    while (sent < size) {
        const ssize_t rc = send(control_fd, buffer + sent, size - sent, MSG_NOSIGNAL);
        if (rc > 0) {
            sent += static_cast<size_t>(rc);
            continue;
        }
        if (rc < 0 && errno == EINTR) {
            continue;
        }
        LOG_ERROR("Failed to send on the shared-memory control socket: {}", ErrnoText());
        return false;
    }
    return true;
}

bool TransportShm::RecvControl(void* data, size_t size) const {
    char* buffer = static_cast<char*>(data);
    size_t received = 0;
    while (received < size) {
        const ssize_t rc = recv(control_fd, buffer + received, size - received, 0);
        if (rc > 0) {
            received += static_cast<size_t>(rc);
            continue;
        }
        if (rc < 0 && errno == EINTR) {
            continue;
        }
        LOG_ERROR("Failed to receive on the shared-memory control socket: {}", ErrnoText());
        return false;
    }
    return true;
}

void TransportShm::CloseControl() {
    if (control_fd >= 0) {
        close(control_fd);
        control_fd = -1;
    }
}

int TransportShm::GetFd() const {
    return listen_fd >= 0 ? listen_fd : WaitFd();
}

uint32_t TransportShm::GetPollEvents() const {
    return EPOLLIN;
}

void TransportShm::Close() {
    CloseControl();
    if (map != nullptr) {
        munmap(map, kShmRingMapSize);
        map = nullptr;
    }
    resources.Close();
    if (listen_fd >= 0) {
        close(listen_fd);
        listen_fd = -1;
    }
    if (!listen_path.empty()) {
        unlink(listen_path.c_str());
        listen_path.clear();
    }
    connected = false;
}
