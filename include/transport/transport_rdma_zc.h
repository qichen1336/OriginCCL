#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include "transport/transport_rdma.h"

constexpr size_t kRdmaZcThreshold = 256 * 1024;
constexpr size_t kRdmaZcChunk = 16 * 1024 * 1024;
constexpr size_t kRdmaZcMaxTransferSize = size_t{1} << 30;
constexpr uint32_t kRdmaZcWindow = 8;

class TransportRDMAZc : public TransportRDMA {
public:
    explicit TransportRDMAZc(size_t threshold = kRdmaZcThreshold);
    ~TransportRDMAZc() override;

    static constexpr bool IsTransferSizeSupported(size_t size) {
        return size <= kRdmaZcMaxTransferSize;
    }

    bool TrySend(const void* data, size_t size, size_t* progress, bool* done) override;
    bool TryRecv(void* data, size_t size, size_t* progress, bool* done) override;

protected:
    struct ZcWire {
        uint32_t port;
        uint32_t chunk;
    };

    bool SetupResources() override;
    void PrepareWire(Wire& wire) const override;
    bool FinalizeConnection() override;
    bool HandleCompletion(const ibv_wc& completion) override;
    std::shared_ptr<TransportRDMA> MakePeer() override;
    void CloseResources() override;

private:
    bool AwaitZcEvent(rdma_cm_event_type type);
    bool CreateZcQueuePair();
    void CloseZc();

    size_t ZcChunkLength(size_t index) const;
    bool AcquireZcMr(void* buffer, size_t size, bool send);
    void DropZcMr();
    bool BeginZcSend(const void* data, size_t size);
    bool BeginZcRecv(void* data, size_t size);
    bool PostZcSends(size_t count);
    bool PostZcRecvs(size_t count);
    void ReleaseZc();

    size_t threshold_;
    rdma_event_channel* zc_channel = nullptr;
    rdma_cm_id* zc_listener = nullptr;
    rdma_cm_id* zc_id = nullptr;
    uint16_t zc_port = 0;
    ibv_qp* zc_qp = nullptr;
    ibv_mr* zc_mr = nullptr;
    void* zc_mr_buffer = nullptr;
    size_t zc_mr_size = 0;
    bool zc_mr_send = false;
    const char* zc_send_data = nullptr;
    char* zc_recv_data = nullptr;
    size_t zc_size = 0;
    size_t zc_chunk = 0;
    size_t zc_total = 0;
    bool zc_send_active = false;
    bool zc_recv_active = false;
    size_t zc_send_posted = 0;
    size_t zc_send_completed = 0;
    size_t zc_recv_posted = 0;
    size_t zc_recv_completed = 0;
};
