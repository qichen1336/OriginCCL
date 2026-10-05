#include <algorithm>
#include <cstring>
#include <cerrno>
#include <arpa/inet.h>
#include "transport/transport_rdma_zc.h"
#include "logger.h"

namespace {
constexpr size_t kZcRecvBase = kRdmaRecvPool;
constexpr int kZcResolveTimeoutMs = 5000;
}

TransportRDMAZc::TransportRDMAZc(size_t threshold) : threshold_(threshold) {}

TransportRDMAZc::~TransportRDMAZc() {
    Close();
}

// The zero-copy data path needs its own queue pair so large SEND/RECV never match the base
// credit pool. A hand-built queue pair cannot be driven on iWARP (the CM owns the QP state
// machine), so the pair comes from a second CM connection that shares the base PD and CQ.
bool TransportRDMAZc::SetupResources() {
    if (!TransportRDMA::SetupResources()) {
        return false;
    }

    zc_channel = rdma_create_event_channel();
    if (zc_channel == nullptr) {
        LOG_ERROR("Failed to create the zero-copy event channel: {}", strerror(errno));
        return false;
    }
    // Only the accepting side listens; connected is already true on the Adopt path.
    if (!connected) {
        return true;
    }

    if (rdma_create_id(zc_channel, &zc_listener, nullptr, RDMA_PS_TCP) != 0) {
        LOG_ERROR("Failed to create the zero-copy listener: {}", strerror(errno));
        return false;
    }
    sockaddr local{};
    std::memcpy(&local, rdma_get_local_addr(cm_id), sizeof(sockaddr));
    reinterpret_cast<sockaddr_in*>(&local)->sin_port = 0;
    if (rdma_bind_addr(zc_listener, &local) != 0) {
        LOG_ERROR("Failed to bind the zero-copy listener: {}", strerror(errno));
        return false;
    }
    if (rdma_listen(zc_listener, 1) != 0) {
        LOG_ERROR("Failed to listen for the zero-copy connection: {}", strerror(errno));
        return false;
    }
    zc_port = ntohs(reinterpret_cast<const sockaddr_in*>(rdma_get_local_addr(zc_listener))->sin_port);
    return true;
}

void TransportRDMAZc::PrepareWire(Wire& wire) const {
    ZcWire extra{};
    extra.port = zc_port;
    extra.chunk = static_cast<uint32_t>(kRdmaZcChunk);
    std::memcpy(wire.tail, &extra, sizeof(extra));
}

bool TransportRDMAZc::AwaitZcEvent(rdma_cm_event_type type) {
    for (;;) {
        rdma_cm_event* event = nullptr;
        if (rdma_get_cm_event(zc_channel, &event) != 0) {
            LOG_ERROR("Failed to read a zero-copy RDMA event: {}", strerror(errno));
            return false;
        }
        const bool matched = event->event == type;
        const bool fatal = event->event == RDMA_CM_EVENT_REJECTED || event->event == RDMA_CM_EVENT_DEVICE_REMOVAL;
        if (!matched) {
            LOG_ERROR("The zero-copy RDMA connection ended with event {} (status {})", rdma_event_str(event->event),
                      event->status);
        }
        rdma_ack_cm_event(event);
        if (matched) {
            return true;
        }
        if (fatal) {
            return false;
        }
    }
}

bool TransportRDMAZc::CreateZcQueuePair() {
    ibv_qp_init_attr attributes{};
    attributes.send_cq = cq;
    attributes.recv_cq = cq;
    attributes.qp_type = IBV_QPT_RC;
    attributes.cap.max_send_wr = kRdmaZcWindow;
    attributes.cap.max_recv_wr = kRdmaZcWindow;
    attributes.cap.max_send_sge = 1;
    attributes.cap.max_recv_sge = 1;
    if (rdma_create_qp(zc_id, pd, &attributes) != 0) {
        LOG_ERROR("Failed to create the zero-copy queue pair: {}", strerror(errno));
        return false;
    }
    zc_qp = zc_id->qp;
    return true;
}

// The zero-copy queue pair belongs to a second CM connection, so the CM drives its path and
// state transitions on every transport, iWARP included; only the port travels in the tail.
bool TransportRDMAZc::FinalizeConnection() {
    ZcWire extra{};
    std::memcpy(&extra, peer.tail, sizeof(extra));
    if (extra.chunk == 0) {
        LOG_ERROR("The RDMA peer did not advertise a zero-copy chunk");
        return false;
    }
    zc_chunk = std::min(kRdmaZcChunk, static_cast<size_t>(extra.chunk));

    if (zc_listener != nullptr) {
        rdma_cm_event* request = nullptr;
        if (rdma_get_cm_event(zc_channel, &request) != 0 || request->event != RDMA_CM_EVENT_CONNECT_REQUEST) {
            LOG_ERROR("The zero-copy listener did not report a connect request");
            if (request != nullptr) {
                rdma_ack_cm_event(request);
            }
            return false;
        }
        zc_id = request->id;
        rdma_ack_cm_event(request);
        if (!CreateZcQueuePair()) {
            return false;
        }
        rdma_conn_param parameter{};
        parameter.retry_count = 7;
        parameter.rnr_retry_count = 7;
        if (rdma_accept(zc_id, &parameter) != 0) {
            LOG_ERROR("Failed to accept the zero-copy connection: {}", strerror(errno));
            return false;
        }
        return AwaitZcEvent(RDMA_CM_EVENT_ESTABLISHED);
    }

    if (extra.port == 0) {
        LOG_ERROR("The RDMA peer did not advertise a zero-copy port");
        return false;
    }
    if (rdma_create_id(zc_channel, &zc_id, nullptr, RDMA_PS_TCP) != 0) {
        LOG_ERROR("Failed to create the zero-copy connection: {}", strerror(errno));
        return false;
    }
    sockaddr remote{};
    std::memcpy(&remote, rdma_get_peer_addr(cm_id), sizeof(sockaddr));
    reinterpret_cast<sockaddr_in*>(&remote)->sin_port = htons(static_cast<uint16_t>(extra.port));
    if (rdma_resolve_addr(zc_id, nullptr, &remote, kZcResolveTimeoutMs) != 0 ||
        !AwaitZcEvent(RDMA_CM_EVENT_ADDR_RESOLVED)) {
        LOG_ERROR("Failed to resolve the zero-copy peer address: {}", strerror(errno));
        return false;
    }
    if (rdma_resolve_route(zc_id, kZcResolveTimeoutMs) != 0 || !AwaitZcEvent(RDMA_CM_EVENT_ROUTE_RESOLVED)) {
        LOG_ERROR("Failed to resolve the zero-copy route: {}", strerror(errno));
        return false;
    }
    if (!CreateZcQueuePair()) {
        return false;
    }
    rdma_conn_param parameter{};
    parameter.retry_count = 7;
    parameter.rnr_retry_count = 7;
    if (rdma_connect(zc_id, &parameter) != 0 || !AwaitZcEvent(RDMA_CM_EVENT_ESTABLISHED)) {
        LOG_ERROR("Failed to connect the zero-copy peer: {}", strerror(errno));
        return false;
    }
    return true;
}

std::shared_ptr<TransportRDMA> TransportRDMAZc::MakePeer() {
    return std::make_shared<TransportRDMAZc>(threshold_);
}

bool TransportRDMAZc::HandleCompletion(const ibv_wc& completion) {
    // The two queue pairs share one CQ, so the queue pair number is what separates them.
    if (completion.status == IBV_WC_SUCCESS && zc_qp != nullptr && completion.qp_num == zc_qp->qp_num) {
        if (completion.opcode == IBV_WC_SEND && zc_send_active && completion.wr_id >= 1 &&
            completion.wr_id <= zc_send_posted) {
            zc_send_completed = std::max(zc_send_completed, static_cast<size_t>(completion.wr_id));
            return true;
        }
        if (completion.opcode == IBV_WC_RECV) {
            ++zc_recv_completed;
            return true;
        }
    }
    return TransportRDMA::HandleCompletion(completion);
}

size_t TransportRDMAZc::ZcChunkLength(size_t index) const {
    return std::min(zc_chunk, zc_size - index * zc_chunk);
}

// The MR is kept registered across transfers: steady-state messages reuse the same buffer, so
// re-registering every time would dominate the cost of the transfer itself. A new buffer, size
// or direction replaces it.
bool TransportRDMAZc::AcquireZcMr(void* buffer, size_t size, bool send) {
    if (zc_mr != nullptr && zc_mr_buffer == buffer && zc_mr_size == size && zc_mr_send == send) {
        return true;
    }
    DropZcMr();
    zc_mr = ibv_reg_mr(pd, buffer, size, IBV_ACCESS_LOCAL_WRITE);
    if (zc_mr == nullptr) {
        LOG_ERROR("Failed to register the zero-copy buffer: {}", strerror(errno));
        return false;
    }
    zc_mr_buffer = buffer;
    zc_mr_size = size;
    zc_mr_send = send;
    return true;
}

void TransportRDMAZc::DropZcMr() {
    if (zc_mr != nullptr) {
        ibv_dereg_mr(zc_mr);
        zc_mr = nullptr;
    }
    zc_mr_buffer = nullptr;
    zc_mr_size = 0;
    zc_mr_send = false;
}

bool TransportRDMAZc::BeginZcSend(const void* data, size_t size) {
    if (!AcquireZcMr(const_cast<void*>(data), size, true)) {
        return false;
    }
    zc_send_data = static_cast<const char*>(data);
    zc_size = size;
    zc_total = (size + zc_chunk - 1) / zc_chunk;
    zc_send_active = true;
    return true;
}

bool TransportRDMAZc::BeginZcRecv(void* data, size_t size) {
    if (!AcquireZcMr(data, size, false)) {
        return false;
    }
    zc_recv_data = static_cast<char*>(data);
    zc_size = size;
    zc_total = (size + zc_chunk - 1) / zc_chunk;
    zc_recv_active = true;
    return true;
}

bool TransportRDMAZc::PostZcSends(size_t count) {
    ibv_sge elements[kRdmaZcWindow]{};
    ibv_send_wr requests[kRdmaZcWindow]{};
    for (size_t i = 0; i < count; ++i) {
        const size_t index = zc_send_posted + i;
        elements[i] = ibv_sge{reinterpret_cast<uintptr_t>(zc_send_data + index * zc_chunk),
                              static_cast<uint32_t>(ZcChunkLength(index)), zc_mr->lkey};
        requests[i].wr_id = index + 1;
        requests[i].opcode = IBV_WR_SEND;
        requests[i].sg_list = &elements[i];
        requests[i].num_sge = 1;
        requests[i].next = i + 1 < count ? &requests[i + 1] : nullptr;
    }
    requests[count - 1].send_flags = IBV_SEND_SIGNALED;
    ibv_send_wr* failed = nullptr;
    if (ibv_post_send(zc_qp, requests, &failed) != 0) {
        LOG_ERROR("Failed to post {} chained zero-copy sends: {}", count, strerror(errno));
        return false;
    }
    return true;
}

bool TransportRDMAZc::PostZcRecvs(size_t count) {
    ibv_sge elements[kRdmaZcWindow]{};
    ibv_recv_wr requests[kRdmaZcWindow]{};
    for (size_t i = 0; i < count; ++i) {
        const size_t index = zc_recv_posted + i;
        elements[i] = ibv_sge{reinterpret_cast<uintptr_t>(zc_recv_data + index * zc_chunk),
                              static_cast<uint32_t>(ZcChunkLength(index)), zc_mr->lkey};
        requests[i].wr_id = kZcRecvBase + index;
        requests[i].sg_list = &elements[i];
        requests[i].num_sge = 1;
        requests[i].next = i + 1 < count ? &requests[i + 1] : nullptr;
    }
    ibv_recv_wr* failed = nullptr;
    if (ibv_post_recv(zc_qp, requests, &failed) != 0) {
        LOG_ERROR("Failed to post {} chained zero-copy receives: {}", count, strerror(errno));
        return false;
    }
    return true;
}

void TransportRDMAZc::ReleaseZc() {
    zc_send_data = nullptr;
    zc_recv_data = nullptr;
    zc_size = 0;
    zc_total = 0;
    zc_send_active = false;
    zc_recv_active = false;
    zc_send_posted = 0;
    zc_send_completed = 0;
    zc_recv_posted = 0;
    zc_recv_completed = 0;
}

bool TransportRDMAZc::TrySend(const void* data, size_t size, size_t* progress, bool* done) {
    if (size < threshold_) {
        return TransportRDMA::TrySend(data, size, progress, done);
    }
    if (!IsTransferSizeSupported(size)) {
        LOG_ERROR("The zero-copy send size {} exceeds the {} byte limit", size, kRdmaZcMaxTransferSize);
        return false;
    }
    if (!connected || !IsProducer()) {
        LOG_ERROR("The RDMA endpoint is not a connected producer, cannot send");
        return false;
    }
    if (!ProcessCompletions()) {
        return false;
    }

    if (!zc_send_active) {
        if (!BeginZcSend(data, size)) {
            return false;
        }
    } else if (data != zc_send_data || size != zc_size) {
        LOG_ERROR("The zero-copy send must keep the same buffer until it is done");
        return false;
    }

    const size_t send_window = kRdmaZcWindow - (zc_send_posted - zc_send_completed);
    const size_t send_count = std::min(send_window, zc_total - zc_send_posted);
    if (send_count > 0) {
        if (!PostZcSends(send_count)) {
            return false;
        }
        zc_send_posted += send_count;
    }

    *done = zc_send_completed == zc_total;
    *progress = *done ? size : 0;
    if (*done) {
        ReleaseZc();
    }
    return true;
}

bool TransportRDMAZc::TryRecv(void* data, size_t size, size_t* progress, bool* done) {
    if (size < threshold_) {
        return TransportRDMA::TryRecv(data, size, progress, done);
    }
    if (!IsTransferSizeSupported(size)) {
        LOG_ERROR("The zero-copy receive size {} exceeds the {} byte limit", size, kRdmaZcMaxTransferSize);
        return false;
    }
    if (!connected || IsProducer()) {
        LOG_ERROR("The RDMA endpoint is not a connected consumer, cannot receive");
        return false;
    }
    if (!ProcessCompletions()) {
        return false;
    }

    if (!zc_recv_active) {
        if (!BeginZcRecv(data, size)) {
            return false;
        }
    } else if (data != zc_recv_data || size != zc_size) {
        LOG_ERROR("The zero-copy receive must keep the same buffer until it is done");
        return false;
    }

    const size_t recv_window = kRdmaZcWindow - (zc_recv_posted - zc_recv_completed);
    const size_t recv_count = std::min(recv_window, zc_total - zc_recv_posted);
    if (recv_count > 0) {
        if (!PostZcRecvs(recv_count)) {
            return false;
        }
        zc_recv_posted += recv_count;
    }

    *done = zc_recv_completed == zc_total;
    *progress = *done ? size : 0;
    if (*done) {
        ReleaseZc();
    }
    return true;
}

void TransportRDMAZc::CloseZc() {
    if (zc_id != nullptr) {
        rdma_destroy_id(zc_id);
        zc_id = nullptr;
        zc_qp = nullptr;
    }
    if (zc_listener != nullptr) {
        rdma_destroy_id(zc_listener);
        zc_listener = nullptr;
    }
    if (zc_channel != nullptr) {
        rdma_destroy_event_channel(zc_channel);
        zc_channel = nullptr;
    }
}

void TransportRDMAZc::CloseResources() {
    ReleaseZc();
    DropZcMr();
    CloseZc();
    TransportRDMA::CloseResources();
}
