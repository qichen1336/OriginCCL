#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include "transport/transport_rdma.h"

constexpr size_t kRdmaZcThreshold = 16 * 1024 * 1024;
constexpr size_t kRdmaZcChunk = 16 * 1024 * 1024;
constexpr uint32_t kRdmaZcWindow = 8;

class TransportRDMAZc : public TransportRDMA {
public:
    TransportRDMAZc();
    ~TransportRDMAZc() override;

    bool TrySend(const void* data, size_t size, size_t* progress, bool* done) override;
    bool TryRecv(void* data, size_t size, size_t* progress, bool* done) override;

protected:
    struct ZcWire {
        uint32_t qp_num;
        uint32_t chunk;
    };

    bool SetupResources() override;
    void PrepareWire(Wire& wire) const override;
    bool FinalizeConnection() override;
    bool HandleCompletion(const ibv_wc& completion) override;
    std::shared_ptr<TransportRDMA> MakePeer() override;
    void CloseResources() override;

private:
    size_t ZcChunkLength(size_t index) const;
    bool BeginZcSend(const void* data, size_t size);
    bool BeginZcRecv(void* data, size_t size);
    bool PostZcSend(size_t index);
    bool PostZcRecv(size_t index);
    void ReleaseZc();

    ibv_qp* zc_qp = nullptr;
    ibv_mr* zc_mr = nullptr;
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
