#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <arpa/inet.h>
#include <poll.h>
#include <sys/epoll.h>
#include <unistd.h>
#include "transport/transport_rdma.h"
#include "logger.h"

namespace {
constexpr uint32_t kWireMagic = 0x52444331; // "RDC1"
constexpr uint32_t kCreditImmediate = 0x80000000;
constexpr size_t kCreditBatch = 8;
constexpr int kResolveTimeoutMs = 5000;
constexpr int kEstablishTimeoutMs = 15000;
constexpr int kControlTimeoutMs = 30000;

// RoCE and IPoIB advertise an IPv4 address as ::ffff:a.b.c.d, so a GID whose first ten bytes
// are zero and whose next two are 0xff carries the address in its last four bytes.
bool IsIpv4MappedGid(const ibv_gid& gid) {
    for (int i = 0; i < 10; ++i) {
        if (gid.raw[i] != 0) {
            return false;
        }
    }
    return gid.raw[10] == 0xFF && gid.raw[11] == 0xFF;
}

uint32_t EncodeDataImmediate(size_t slot, size_t length) {
    return static_cast<uint32_t>((slot << 16) | (length - 1));
}

size_t ImmediateSlot(uint32_t immediate) {
    return immediate >> 16;
}

size_t ImmediateLength(uint32_t immediate) {
    return (immediate & 0xFFFFu) + 1;
}

std::string ErrnoText() {
    return strerror(errno);
}

bool WaitReadable(int fd, int timeout_ms) {
    pollfd entry{fd, POLLIN, 0};
    int ready = 0;
    do {
        ready = poll(&entry, 1, timeout_ms);
    } while (ready < 0 && errno == EINTR);
    return ready > 0;
}
} // namespace

TransportRDMA::TransportRDMA() {}

TransportRDMA::~TransportRDMA() {
    Close();
}

// The address has to come from the device: the first non-loopback IPv4 belongs to whichever
// interface getifaddrs() returns first, which is usually not the RDMA netdev.
bool TransportRDMA::Probe(std::string& addr) {
    ibv_device** devices = ibv_get_device_list(nullptr);
    if (devices == nullptr) {
        LOG_ERROR("Failed to enumerate the RDMA devices: {}", ErrnoText());
        return false;
    }

    bool found = false;
    for (int device_index = 0; devices[device_index] != nullptr && !found; ++device_index) {
        ibv_context* context = ibv_open_device(devices[device_index]);
        if (context == nullptr) {
            continue;
        }

        ibv_device_attr device{};
        if (ibv_query_device(context, &device) == 0) {
            for (uint8_t port = 1; port <= device.phys_port_cnt && !found; ++port) {
                ibv_port_attr attributes{};
                if (ibv_query_port(context, port, &attributes) != 0 || attributes.state != IBV_PORT_ACTIVE) {
                    continue;
                }
                for (int index = 0; index < kMaxRdmGidPerPort && index < attributes.gid_tbl_len && !found; ++index) {
                    ibv_gid gid{};
                    char text[INET_ADDRSTRLEN] = {};
                    if (ibv_query_gid(context, port, index, &gid) == 0 && IsIpv4MappedGid(gid) &&
                        inet_ntop(AF_INET, &gid.raw[12], text, sizeof(text)) != nullptr) {
                        addr = text;
                        found = true;
                    }
                }
            }
        }
        ibv_close_device(context);
    }

    ibv_free_device_list(devices);
    return found;
}

bool TransportRDMA::Listen(uint16_t port) {
    std::string addr;
    if (!Probe(addr)) {
        LOG_ERROR("No RDMA device with an active port is available on this host");
        return false;
    }
    return ListenAddr(addr, port);
}

bool TransportRDMA::ListenAddr(const std::string& addr, uint16_t port) {
    if (!OpenChannel()) {
        return false;
    }
    if (rdma_create_id(cm_channel, &cm_id, nullptr, RDMA_PS_TCP) != 0) {
        LOG_ERROR("Failed to create the RDMA listener: {}", ErrnoText());
        return false;
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    if (inet_pton(AF_INET, addr.c_str(), &address.sin_addr) != 1) {
        LOG_ERROR("Invalid RDMA listen address {}", addr);
        return false;
    }
    if (rdma_bind_addr(cm_id, reinterpret_cast<sockaddr*>(&address)) != 0) {
        LOG_ERROR("Failed to bind the RDMA listener to {}: {}", addr, ErrnoText());
        return false;
    }
    if (rdma_listen(cm_id, 128) != 0) {
        LOG_ERROR("Failed to listen on the RDMA address {}: {}", addr, ErrnoText());
        return false;
    }

    const auto* bound = reinterpret_cast<const sockaddr_in*>(rdma_get_local_addr(cm_id));
    listen_port = ntohs(bound->sin_port);
    return true;
}

std::shared_ptr<Transport> TransportRDMA::Accept() {
    if (cm_id == nullptr) {
        LOG_ERROR("Not in listen mode");
        return nullptr;
    }

    rdma_cm_event* event = AwaitEvent(RDMA_CM_EVENT_CONNECT_REQUEST);
    if (event == nullptr) {
        return nullptr;
    }
    rdma_cm_id* connection = event->id;
    // The active side's private data arrives with the connection request, not with ESTABLISHED.
    Wire wire{};
    const bool valid = ReadWire(*event, wire);
    rdma_ack_cm_event(event);
    if (!valid) {
        rdma_destroy_id(connection);
        return nullptr;
    }

    auto transport = std::make_shared<TransportRDMA>();
    if (!transport->Adopt(connection, wire)) {
        // Adopt took ownership of the connection id, so the destructor releases it.
        return nullptr;
    }
    return transport;
}

bool TransportRDMA::Adopt(rdma_cm_id* connection, const Wire& wire) {
    if (!OpenChannel()) {
        return false;
    }
    if (rdma_migrate_id(connection, cm_channel) != 0) {
        LOG_ERROR("Failed to move the accepted RDMA connection onto its own event channel: {}", ErrnoText());
        return false;
    }
    cm_id = connection;
    peer = wire;
    peer_known = true;
    connected = true;

    if (!SetupResources()) {
        return false;
    }

    Wire mine{reinterpret_cast<uint64_t>(buffer), mr->rkey, kWireMagic};
    rdma_conn_param parameter{};
    parameter.private_data = &mine;
    parameter.private_data_len = sizeof(mine);
    if (rdma_accept(cm_id, &parameter) != 0) {
        LOG_ERROR("Failed to accept the RDMA connection: {}", ErrnoText());
        return false;
    }
    return AwaitEstablished();
}

bool TransportRDMA::Connect(const std::string& addr, uint16_t port) {
    if (!OpenChannel()) {
        return false;
    }
    if (rdma_create_id(cm_channel, &cm_id, nullptr, RDMA_PS_TCP) != 0) {
        LOG_ERROR("Failed to create the RDMA connection: {}", ErrnoText());
        return false;
    }

    sockaddr_in remote{};
    remote.sin_family = AF_INET;
    remote.sin_port = htons(port);
    if (inet_pton(AF_INET, addr.c_str(), &remote.sin_addr) != 1) {
        LOG_ERROR("Invalid RDMA address {}", addr);
        return false;
    }

    if (rdma_resolve_addr(cm_id, nullptr, reinterpret_cast<sockaddr*>(&remote), kResolveTimeoutMs) != 0) {
        LOG_ERROR("Failed to resolve the RDMA address {}: {}", addr, ErrnoText());
        return false;
    }
    rdma_cm_event* event = AwaitEvent(RDMA_CM_EVENT_ADDR_RESOLVED);
    if (event == nullptr) {
        return false;
    }
    rdma_ack_cm_event(event);

    if (rdma_resolve_route(cm_id, kResolveTimeoutMs) != 0) {
        LOG_ERROR("Failed to resolve the RDMA route to {}: {}", addr, ErrnoText());
        return false;
    }
    event = AwaitEvent(RDMA_CM_EVENT_ROUTE_RESOLVED);
    if (event == nullptr) {
        return false;
    }
    rdma_ack_cm_event(event);

    if (!SetupResources()) {
        return false;
    }

    Wire mine{reinterpret_cast<uint64_t>(buffer), mr->rkey, kWireMagic};
    rdma_conn_param parameter{};
    parameter.retry_count = 7;
    parameter.rnr_retry_count = 7;
    parameter.private_data = &mine;
    parameter.private_data_len = sizeof(mine);
    if (rdma_connect(cm_id, &parameter) != 0) {
        LOG_ERROR("Failed to connect to the RDMA peer {}: {}", addr, ErrnoText());
        return false;
    }
    connected = true;
    return AwaitEstablished();
}

bool TransportRDMA::OpenChannel() {
    if (cm_channel != nullptr) {
        return true;
    }
    cm_channel = rdma_create_event_channel();
    if (cm_channel == nullptr) {
        LOG_ERROR("Failed to create an RDMA event channel: {}", ErrnoText());
        return false;
    }
    return true;
}

bool TransportRDMA::AwaitEstablished() {
    rdma_cm_event* event = AwaitEvent(RDMA_CM_EVENT_ESTABLISHED);
    if (event == nullptr) {
        return false;
    }

    Wire wire{};
    // Only the active side still needs the peer announcement here; the passive side has it.
    const bool valid = peer_known || ReadWire(*event, wire);
    rdma_ack_cm_event(event);
    if (!valid) {
        return false;
    }
    if (!peer_known) {
        peer = wire;
        peer_known = true;
    }
    return true;
}

bool TransportRDMA::ReadWire(const rdma_cm_event& event, Wire& wire) const {
    if (event.param.conn.private_data == nullptr || event.param.conn.private_data_len < sizeof(Wire)) {
        LOG_ERROR("The RDMA peer did not announce its memory region (ptr {}, length {})",
                  event.param.conn.private_data == nullptr ? "null" : "set",
                  static_cast<unsigned>(event.param.conn.private_data_len));
        return false;
    }
    std::memcpy(&wire, event.param.conn.private_data, sizeof(Wire));
    if (wire.magic != kWireMagic) {
        LOG_ERROR("The RDMA peer is not an OriginCCL RDMA endpoint");
        return false;
    }
    return true;
}

rdma_cm_event* TransportRDMA::AwaitEvent(rdma_cm_event_type type) {
    for (;;) {
        rdma_cm_event* event = nullptr;
        if (rdma_get_cm_event(cm_channel, &event) == 0) {
            if (event->event == type) {
                return event;
            }
            const bool fatal = event->event == RDMA_CM_EVENT_REJECTED || event->event == RDMA_CM_EVENT_DEVICE_REMOVAL;
            LOG_ERROR("The RDMA connection ended with event {} (status {})", rdma_event_str(event->event),
                      event->status);
            rdma_ack_cm_event(event);
            if (fatal) {
                return nullptr;
            }
            continue;
        }
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            LOG_ERROR("Failed to read an RDMA event: {}", ErrnoText());
            return nullptr;
        }
        if (!WaitReadable(cm_channel->fd, kEstablishTimeoutMs)) {
            LOG_ERROR("Timed out waiting for the RDMA event {}", rdma_event_str(type));
            return nullptr;
        }
    }
}

bool TransportRDMA::SetupResources() {
    pd = ibv_alloc_pd(cm_id->verbs);
    if (pd == nullptr) {
        LOG_ERROR("Failed to allocate the RDMA protection domain: {}", ErrnoText());
        return false;
    }

    buffer = static_cast<char*>(std::aligned_alloc(4096, kRdmaBufferSize));
    if (buffer == nullptr) {
        LOG_ERROR("Failed to allocate the RDMA buffer");
        return false;
    }
    std::memset(buffer, 0, kRdmaBufferSize);

    mr = ibv_reg_mr(pd, buffer, kRdmaBufferSize, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    if (mr == nullptr) {
        LOG_ERROR("Failed to register the RDMA buffer: {}", ErrnoText());
        return false;
    }

    comp_channel = ibv_create_comp_channel(cm_id->verbs);
    if (comp_channel == nullptr) {
        LOG_ERROR("Failed to create the RDMA completion channel: {}", ErrnoText());
        return false;
    }
    cq = ibv_create_cq(cm_id->verbs, kRdmaQueueDepth, nullptr, comp_channel, 0);
    if (cq == nullptr) {
        LOG_ERROR("Failed to create the RDMA completion queue: {}", ErrnoText());
        return false;
    }
    if (ibv_req_notify_cq(cq, 0) != 0) {
        LOG_ERROR("Failed to arm the RDMA completion queue: {}", ErrnoText());
        return false;
    }

    ibv_qp_init_attr attributes{};
    attributes.send_cq = cq;
    attributes.recv_cq = cq;
    attributes.qp_type = IBV_QPT_RC;
    attributes.cap.max_send_wr = kRdmaQueueDepth;
    attributes.cap.max_recv_wr = kRdmaQueueDepth;
    attributes.cap.max_send_sge = 1;
    attributes.cap.max_recv_sge = 1;
    if (rdma_create_qp(cm_id, pd, &attributes) != 0) {
        LOG_ERROR("Failed to create the RDMA queue pair: {}", ErrnoText());
        return false;
    }

    if (kRdmaRecvPool > static_cast<size_t>(kRdmaQueueDepth)) {
        LOG_ERROR("The RDMA receive queue depth {} cannot hold the {} entry pool", kRdmaQueueDepth, kRdmaRecvPool);
        return false;
    }
    for (size_t i = 0; i < kRdmaRecvPool; ++i) {
        if (!PostPoolRecv(i)) {
            return false;
        }
    }

    epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd < 0) {
        LOG_ERROR("Failed to create the RDMA readiness instance: {}", ErrnoText());
        return false;
    }
    epoll_event interest{};
    interest.events = EPOLLIN;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, comp_channel->fd, &interest) != 0 ||
        epoll_ctl(epoll_fd, EPOLL_CTL_ADD, cm_channel->fd, &interest) != 0) {
        LOG_ERROR("Failed to register the RDMA events for readiness: {}", ErrnoText());
        return false;
    }
    return true;
}

bool TransportRDMA::PostPoolRecv(size_t index) {
    ibv_sge element{reinterpret_cast<uintptr_t>(ScratchData(index)), static_cast<uint32_t>(kRdmaControlSize), mr->lkey};
    ibv_recv_wr request{};
    request.wr_id = index;
    request.sg_list = &element;
    request.num_sge = 1;
    ibv_recv_wr* failed = nullptr;
    if (ibv_post_recv(cm_id->qp, &request, &failed) != 0) {
        LOG_ERROR("Failed to post RDMA receive {}: {}", index, ErrnoText());
        return false;
    }
    return true;
}

bool TransportRDMA::PostControlSend(size_t length) {
    ibv_sge element{reinterpret_cast<uintptr_t>(buffer + kRdmaControlOffset), static_cast<uint32_t>(length), mr->lkey};
    ibv_send_wr request{};
    request.opcode = IBV_WR_SEND;
    request.send_flags = IBV_SEND_SIGNALED;
    request.sg_list = &element;
    request.num_sge = 1;
    ibv_send_wr* failed = nullptr;
    if (ibv_post_send(cm_id->qp, &request, &failed) != 0) {
        LOG_ERROR("Failed to post the RDMA control message: {}", ErrnoText());
        return false;
    }
    return true;
}

bool TransportRDMA::PostWrite(size_t slot, size_t length) {
    ibv_sge element{reinterpret_cast<uintptr_t>(SlotData(slot)), static_cast<uint32_t>(length), mr->lkey};
    ibv_send_wr request{};
    request.wr_id = slot + 1;
    request.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
    request.send_flags = IBV_SEND_SIGNALED;
    request.imm_data = htonl(EncodeDataImmediate(slot, length));
    request.wr.rdma.remote_addr = peer.base_addr + kRdmaRingOffset + slot * kRdmaSlotSize;
    request.wr.rdma.rkey = peer.rkey;
    request.sg_list = &element;
    request.num_sge = 1;
    ibv_send_wr* failed = nullptr;
    if (ibv_post_send(cm_id->qp, &request, &failed) != 0) {
        LOG_ERROR("Failed to post the RDMA write for ring slot {}: {}", slot, ErrnoText());
        return false;
    }
    return true;
}

bool TransportRDMA::PostCredit(size_t slots) {
    ibv_sge element{reinterpret_cast<uintptr_t>(buffer + kRdmaCreditOffset), 1, mr->lkey};
    ibv_send_wr request{};
    request.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
    request.send_flags = IBV_SEND_SIGNALED;
    request.imm_data = htonl(kCreditImmediate | static_cast<uint32_t>(slots));
    request.wr.rdma.remote_addr = peer.base_addr + kRdmaCreditOffset;
    request.wr.rdma.rkey = peer.rkey;
    request.sg_list = &element;
    request.num_sge = 1;
    ibv_send_wr* failed = nullptr;
    if (ibv_post_send(cm_id->qp, &request, &failed) != 0) {
        LOG_ERROR("Failed to post an RDMA credit for {} ring slots: {}", slots, ErrnoText());
        return false;
    }
    return true;
}

bool TransportRDMA::ProcessCompletions() {
    if (comp_channel != nullptr) {
        while (WaitReadable(comp_channel->fd, 0)) {
            ibv_cq* event_cq = nullptr;
            void* context = nullptr;
            if (ibv_get_cq_event(comp_channel, &event_cq, &context) != 0) {
                LOG_ERROR("Failed to read an RDMA completion event: {}", ErrnoText());
                return false;
            }
            ibv_ack_cq_events(event_cq, 1);
            if (ibv_req_notify_cq(event_cq, 0) != 0) {
                LOG_ERROR("Failed to re-arm the RDMA completion queue: {}", ErrnoText());
                return false;
            }
        }
    }
    if (!DrainCmEvents()) {
        return false;
    }

    ibv_wc completions[16];
    for (;;) {
        const int count = ibv_poll_cq(cq, 16, completions);
        if (count < 0) {
            LOG_ERROR("Failed to poll the RDMA completion queue");
            return false;
        }
        if (count == 0) {
            return true;
        }
        for (int i = 0; i < count; ++i) {
            if (!HandleCompletion(completions[i])) {
                return false;
            }
        }
    }
}

bool TransportRDMA::DrainCmEvents() {
    if (cm_channel == nullptr) {
        return true;
    }
    while (WaitReadable(cm_channel->fd, 0)) {
        rdma_cm_event* event = nullptr;
        if (rdma_get_cm_event(cm_channel, &event) != 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return true;
            }
            LOG_ERROR("Failed to read an RDMA event: {}", ErrnoText());
            return false;
        }
        const rdma_cm_event_type type = event->event;
        const int status = event->status;
        rdma_ack_cm_event(event);
        if (type == RDMA_CM_EVENT_DISCONNECTED || type == RDMA_CM_EVENT_DEVICE_REMOVAL ||
            type == RDMA_CM_EVENT_TIMEWAIT_EXIT || type == RDMA_CM_EVENT_REJECTED) {
            peer_closed = true;
        } else if (type != RDMA_CM_EVENT_ESTABLISHED) {
            LOG_DEBUG("Ignoring the RDMA event {} (status {})", rdma_event_str(type), status);
        }
    }
    return true;
}

bool TransportRDMA::HandleCompletion(const ibv_wc& completion) {
    if (completion.status != IBV_WC_SUCCESS) {
        if (peer_closed && completion.status == IBV_WC_WR_FLUSH_ERR) {
            return true;
        }
        LOG_ERROR("An RDMA completion failed with status {}", ibv_wc_status_str(completion.status));
        return false;
    }

    switch (completion.opcode) {
    case IBV_WC_SEND:
        control_sent = true;
        return true;

    case IBV_WC_RDMA_WRITE:
        if (completion.wr_id != 0) {
            ++local_completed;
        }
        return true;

    case IBV_WC_RECV:
        if (completion.byte_len > 0) {
            control_received = true;
            control_index = static_cast<size_t>(completion.wr_id);
        }
        return PostPoolRecv(static_cast<size_t>(completion.wr_id));

    case IBV_WC_RECV_RDMA_WITH_IMM: {
        const uint32_t immediate = ntohl(completion.imm_data);
        if ((immediate & kCreditImmediate) != 0) {
            credits_received += immediate & ~kCreditImmediate;
        } else {
            const size_t slot = ImmediateSlot(immediate);
            if (slot >= kRdmaSlotCount) {
                LOG_ERROR("The RDMA peer announced an out of range ring slot {}", slot);
                return false;
            }
            arrival_lengths[arrival_tail % kRdmaSlotCount] = ImmediateLength(immediate);
            ++arrival_tail;
        }
        return PostPoolRecv(static_cast<size_t>(completion.wr_id));
    }

    default:
        return true;
    }
}

bool TransportRDMA::WaitFlag(bool& flag) {
    for (int waited = 0; waited < kControlTimeoutMs; waited += 10) {
        if (flag) {
            return true;
        }
        if (!ProcessCompletions()) {
            return false;
        }
        if (flag) {
            return true;
        }
        if (peer_closed) {
            LOG_ERROR("The RDMA peer closed the connection during the control handshake");
            return false;
        }
        WaitReadable(GetFd(), 10);
    }
    LOG_ERROR("Timed out during the RDMA control handshake");
    return false;
}

bool TransportRDMA::Send(const void* data, size_t size) {
    if (!connected) {
        LOG_ERROR("Not connected to send");
        return false;
    }
    if (!control_sent) {
        if (size > kRdmaControlSize) {
            LOG_ERROR("The RDMA control message of {} bytes exceeds the {} byte control area", size, kRdmaControlSize);
            return false;
        }
        std::memcpy(buffer + kRdmaControlOffset, data, size);
        if (!PostControlSend(size) || !WaitFlag(control_sent)) {
            return false;
        }
        // The handshake is its own message, not the head of the data stream, so it ends here.
        return true;
    }
    if (!IsProducer()) {
        return true;
    }

    size_t progress = 0;
    bool done = false;
    while (!done) {
        const size_t before = progress;
        if (!TrySend(data, size, &progress, &done)) {
            return false;
        }
        if (!done && progress == before && !WaitReadable(GetFd(), kControlTimeoutMs)) {
            LOG_ERROR("Timed out waiting for the RDMA ring to advance");
            return false;
        }
    }
    return true;
}

bool TransportRDMA::Recv(void* data, size_t size) {
    if (!connected) {
        LOG_ERROR("Not connected to recv");
        return false;
    }
    if (!control_received) {
        if (!WaitFlag(control_received)) {
            return false;
        }
        if (size > kRdmaControlSize) {
            LOG_ERROR("The RDMA control message of {} bytes exceeds the {} byte control area", size, kRdmaControlSize);
            return false;
        }
        std::memcpy(data, ScratchData(control_index), size);
        // The handshake is returned on its own, not as the first bytes of the data stream.
        return true;
    }
    if (IsProducer()) {
        return true;
    }

    size_t progress = 0;
    bool done = false;
    while (!done) {
        const size_t before = progress;
        if (!TryRecv(data, size, &progress, &done)) {
            return false;
        }
        if (!done && progress == before && !WaitReadable(GetFd(), kControlTimeoutMs)) {
            LOG_ERROR("Timed out waiting for the RDMA ring to deliver data");
            return false;
        }
    }
    return true;
}

bool TransportRDMA::TrySend(const void* data, size_t size, size_t* progress, bool* done) {
    if (!connected || !IsProducer()) {
        LOG_ERROR("The RDMA endpoint is not a connected producer, cannot send");
        return false;
    }
    if (!ProcessCompletions()) {
        return false;
    }
    if (peer_closed) {
        LOG_INFO("The RDMA peer closed the connection while sending");
        return false;
    }

    const char* source = static_cast<const char*>(data);
    const size_t local_limit = local_completed + kRdmaSlotCount;
    const size_t remote_limit = credits_received + kRdmaSlotCount;
    const size_t limit = std::min(local_limit, remote_limit);
    while (*progress < size && send_seq < limit) {
        const size_t slot = send_seq % kRdmaSlotCount;
        const size_t length = std::min(kRdmaSlotSize, size - *progress);
        std::memcpy(SlotData(slot), source + *progress, length);
        if (!PostWrite(slot, length)) {
            return false;
        }
        ++send_seq;
        *progress += length;
    }

    *done = (*progress == size);
    return true;
}

bool TransportRDMA::TryRecv(void* data, size_t size, size_t* progress, bool* done) {
    if (!connected || IsProducer()) {
        LOG_ERROR("The RDMA endpoint is not a connected consumer, cannot receive");
        return false;
    }
    if (!ProcessCompletions()) {
        return false;
    }

    char* destination = static_cast<char*>(data);
    while (*progress < size && arrival_head != arrival_tail) {
        const size_t slot = arrival_head % kRdmaSlotCount;
        const size_t length = arrival_lengths[slot];
        const size_t chunk = std::min(length - arrival_offset, size - *progress);
        std::memcpy(destination + *progress, SlotData(slot) + arrival_offset, chunk);
        arrival_offset += chunk;
        *progress += chunk;
        if (arrival_offset == length) {
            arrival_offset = 0;
            ++arrival_head;
            ++pending_credit;
        }
    }

    if (pending_credit >= kCreditBatch || (pending_credit > 0 && arrival_head == arrival_tail)) {
        if (!PostCredit(pending_credit)) {
            return false;
        }
        pending_credit = 0;
    }

    *done = (*progress == size);
    if (!*done && arrival_head == arrival_tail && peer_closed) {
        LOG_INFO("The RDMA peer closed the connection while receiving");
        return false;
    }
    return true;
}

int TransportRDMA::GetFd() const {
    if (epoll_fd >= 0) {
        return epoll_fd;
    }
    if (cm_channel != nullptr) {
        return cm_channel->fd;
    }
    return -1;
}

uint32_t TransportRDMA::GetPollEvents() const {
    return EPOLLIN;
}

void TransportRDMA::Close() {
    if (cm_id != nullptr && cm_id->qp != nullptr) {
        rdma_destroy_qp(cm_id);
    }
    if (mr != nullptr) {
        ibv_dereg_mr(mr);
        mr = nullptr;
    }
    if (cq != nullptr) {
        ibv_destroy_cq(cq);
        cq = nullptr;
    }
    if (comp_channel != nullptr) {
        ibv_destroy_comp_channel(comp_channel);
        comp_channel = nullptr;
    }
    if (pd != nullptr) {
        ibv_dealloc_pd(pd);
        pd = nullptr;
    }
    if (cm_id != nullptr) {
        rdma_destroy_id(cm_id);
        cm_id = nullptr;
    }
    CloseCm();
    if (epoll_fd >= 0) {
        close(epoll_fd);
        epoll_fd = -1;
    }
    if (buffer != nullptr) {
        free(buffer);
        buffer = nullptr;
    }
    connected = false;
    peer_closed = false;
    peer_known = false;
    control_received = false;
    control_sent = false;
    control_index = 0;
    listen_port = 0;
    send_seq = 0;
    local_completed = 0;
    credits_received = 0;
    arrival_head = 0;
    arrival_tail = 0;
    arrival_offset = 0;
    pending_credit = 0;
}

void TransportRDMA::CloseCm() {
    if (cm_channel != nullptr) {
        rdma_destroy_event_channel(cm_channel);
        cm_channel = nullptr;
    }
}
