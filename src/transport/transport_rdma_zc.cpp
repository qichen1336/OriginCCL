#include <algorithm>
#include <cstring>
#include <cerrno>
#include "transport/transport_rdma_zc.h"
#include "logger.h"

namespace {
constexpr size_t kZcRecvBase = kRdmaRecvPool;
}

TransportRDMAZc::TransportRDMAZc(size_t threshold) : threshold_(threshold) {}

TransportRDMAZc::~TransportRDMAZc() {
    Close();
}

bool TransportRDMAZc::SetupResources() {
    if (!TransportRDMA::SetupResources()) {
        return false;
    }

    ibv_qp_init_attr attributes{};
    attributes.send_cq = cq;
    attributes.recv_cq = cq;
    attributes.qp_type = IBV_QPT_RC;
    attributes.cap.max_send_wr = kRdmaZcWindow;
    attributes.cap.max_recv_wr = kRdmaZcWindow;
    attributes.cap.max_send_sge = 1;
    attributes.cap.max_recv_sge = 1;
    zc_qp = ibv_create_qp(pd, &attributes);
    if (zc_qp == nullptr) {
        LOG_ERROR("Failed to create the zero-copy queue pair: {}", strerror(errno));
        return false;
    }

    ibv_qp_attr state{};
    state.qp_state = IBV_QPS_INIT;
    state.pkey_index = 0;
    state.port_num = cm_id->port_num;
    state.qp_access_flags = IBV_ACCESS_LOCAL_WRITE;
    if (ibv_modify_qp(zc_qp, &state, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS) != 0) {
        LOG_ERROR("Failed to move the zero-copy queue pair to INIT: {}", strerror(errno));
        return false;
    }
    return true;
}

void TransportRDMAZc::PrepareWire(Wire& wire) const {
    ZcWire extra{};
    extra.qp_num = zc_qp == nullptr ? 0 : zc_qp->qp_num;
    extra.chunk = static_cast<uint32_t>(kRdmaZcChunk);
    std::memcpy(wire.tail, &extra, sizeof(extra));
}

// The zero-copy queue pair is a plain ibv queue pair: the CM only drives the queue pair it
// created itself, so the peer's queue pair number travels in the private data tail and the
// path comes from rdma_init_qp_attr once the CM connection is established.
bool TransportRDMAZc::FinalizeConnection() {
    ZcWire extra{};
    std::memcpy(&extra, peer.tail, sizeof(extra));
    if (extra.qp_num == 0 || extra.chunk == 0) {
        LOG_ERROR("The RDMA peer did not advertise a zero-copy queue pair");
        return false;
    }
    zc_chunk = std::min(kRdmaZcChunk, static_cast<size_t>(extra.chunk));

    ibv_qp_attr state{};
    state.qp_state = IBV_QPS_RTR;
    int mask = 0;
    if (rdma_init_qp_attr(cm_id, &state, &mask) != 0) {
        LOG_ERROR("Failed to build the zero-copy path attributes: {}", strerror(errno));
        return false;
    }
    // rdma_init_qp_attr overwrites the whole block, so our fields are re-applied afterwards.
    state.qp_state = IBV_QPS_RTR;
    state.dest_qp_num = extra.qp_num;
    state.rq_psn = 0;
    state.max_dest_rd_atomic = 1;
    state.min_rnr_timer = 12;
    if ((mask & IBV_QP_PATH_MTU) == 0) {
        state.path_mtu = IBV_MTU_1024;
    }
    mask |= IBV_QP_STATE | IBV_QP_AV | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC |
            IBV_QP_MIN_RNR_TIMER | IBV_QP_PATH_MTU;
    if (ibv_modify_qp(zc_qp, &state, mask) != 0) {
        LOG_ERROR("Failed to move the zero-copy queue pair to RTR: {}", strerror(errno));
        return false;
    }

    state = {};
    mask = 0;
    state.qp_state = IBV_QPS_RTS;
    if (rdma_init_qp_attr(cm_id, &state, &mask) != 0) {
        LOG_ERROR("Failed to build the zero-copy send attributes: {}", strerror(errno));
        return false;
    }
    state.qp_state = IBV_QPS_RTS;
    state.sq_psn = 0;
    state.timeout = 14;
    state.retry_cnt = 7;
    state.rnr_retry = 7;
    state.max_rd_atomic = 1;
    mask |=
        IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;
    if (ibv_modify_qp(zc_qp, &state, mask) != 0) {
        LOG_ERROR("Failed to move the zero-copy queue pair to RTS: {}", strerror(errno));
        return false;
    }
    return true;
}

std::shared_ptr<TransportRDMA> TransportRDMAZc::MakePeer() {
    return std::make_shared<TransportRDMAZc>(threshold_);
}

bool TransportRDMAZc::HandleCompletion(const ibv_wc& completion) {
    if (completion.status == IBV_WC_SUCCESS) {
        if (completion.opcode == IBV_WC_SEND && completion.wr_id >= 1 && completion.wr_id <= kRdmaZcWindow) {
            ++zc_send_completed;
            return true;
        }
        if (completion.opcode == IBV_WC_RECV && completion.wr_id >= kZcRecvBase) {
            ++zc_recv_completed;
            return true;
        }
    }
    return TransportRDMA::HandleCompletion(completion);
}

size_t TransportRDMAZc::ZcChunkLength(size_t index) const {
    return std::min(zc_chunk, zc_size - index * zc_chunk);
}

bool TransportRDMAZc::BeginZcSend(const void* data, size_t size) {
    zc_mr = ibv_reg_mr(pd, const_cast<void*>(data), size, IBV_ACCESS_LOCAL_WRITE);
    if (zc_mr == nullptr) {
        LOG_ERROR("Failed to register the zero-copy send buffer: {}", strerror(errno));
        return false;
    }
    zc_send_data = static_cast<const char*>(data);
    zc_size = size;
    zc_total = (size + zc_chunk - 1) / zc_chunk;
    zc_send_active = true;
    return true;
}

bool TransportRDMAZc::BeginZcRecv(void* data, size_t size) {
    zc_mr = ibv_reg_mr(pd, data, size, IBV_ACCESS_LOCAL_WRITE);
    if (zc_mr == nullptr) {
        LOG_ERROR("Failed to register the zero-copy receive buffer: {}", strerror(errno));
        return false;
    }
    zc_recv_data = static_cast<char*>(data);
    zc_size = size;
    zc_total = (size + zc_chunk - 1) / zc_chunk;
    zc_recv_active = true;
    return true;
}

bool TransportRDMAZc::PostZcSend(size_t index) {
    ibv_sge element{reinterpret_cast<uintptr_t>(zc_send_data + index * zc_chunk),
                    static_cast<uint32_t>(ZcChunkLength(index)), zc_mr->lkey};
    ibv_send_wr request{};
    request.wr_id = index + 1;
    request.opcode = IBV_WR_SEND;
    request.send_flags = IBV_SEND_SIGNALED;
    request.sg_list = &element;
    request.num_sge = 1;
    ibv_send_wr* failed = nullptr;
    if (ibv_post_send(zc_qp, &request, &failed) != 0) {
        LOG_ERROR("Failed to post the zero-copy send {}: {}", index, strerror(errno));
        return false;
    }
    return true;
}

bool TransportRDMAZc::PostZcRecv(size_t index) {
    ibv_sge element{reinterpret_cast<uintptr_t>(zc_recv_data + index * zc_chunk),
                    static_cast<uint32_t>(ZcChunkLength(index)), zc_mr->lkey};
    ibv_recv_wr request{};
    request.wr_id = kZcRecvBase + index;
    request.sg_list = &element;
    request.num_sge = 1;
    ibv_recv_wr* failed = nullptr;
    if (ibv_post_recv(zc_qp, &request, &failed) != 0) {
        LOG_ERROR("Failed to post the zero-copy receive {}: {}", index, strerror(errno));
        return false;
    }
    return true;
}

void TransportRDMAZc::ReleaseZc() {
    if (zc_mr != nullptr) {
        ibv_dereg_mr(zc_mr);
        zc_mr = nullptr;
    }
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

    while (zc_send_posted < zc_total && zc_send_posted - zc_send_completed < kRdmaZcWindow) {
        if (!PostZcSend(zc_send_posted)) {
            return false;
        }
        ++zc_send_posted;
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

    while (zc_recv_posted < zc_total && zc_recv_posted - zc_recv_completed < kRdmaZcWindow) {
        if (!PostZcRecv(zc_recv_posted)) {
            return false;
        }
        ++zc_recv_posted;
    }

    *done = zc_recv_completed == zc_total;
    *progress = *done ? size : 0;
    if (*done) {
        ReleaseZc();
    }
    return true;
}

void TransportRDMAZc::CloseResources() {
    if (zc_qp != nullptr) {
        ibv_destroy_qp(zc_qp);
        zc_qp = nullptr;
    }
    ReleaseZc();
    TransportRDMA::CloseResources();
}
