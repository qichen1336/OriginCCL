#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include "transport/transport.h"

constexpr size_t kRdmaSlotSize = 64 * 1024;
constexpr size_t kRdmaSlotCount = 32;
constexpr size_t kRdmaRingCapacity = kRdmaSlotSize * kRdmaSlotCount;
constexpr size_t kRdmaControlOffset = 0;
constexpr size_t kRdmaControlSize = 256;
constexpr size_t kRdmaCreditOffset = 256;
constexpr size_t kRdmaScratchOffset = 4096;
constexpr size_t kRdmaRecvPool = 64;
constexpr size_t kRdmaRingOffset = 32768;
constexpr size_t kRdmaBufferSize = kRdmaRingOffset + kRdmaRingCapacity;
constexpr int kRdmaQueueDepth = 128;

class TransportRDMA : public Transport {
public:
    TransportRDMA();
    ~TransportRDMA() override;
    TransportRDMA(const TransportRDMA&) = delete;
    TransportRDMA& operator=(const TransportRDMA&) = delete;

    static bool Probe(std::string& addr);

    bool Listen(const std::string& addr, uint16_t port) override;
    uint16_t GetListenPort() const override {
        return listen_port;
    }

    std::shared_ptr<Transport> Accept() override;

    bool Connect(const std::string& addr, uint16_t port) override;

    bool Send(const void* data, size_t size) override;
    bool Recv(void* data, size_t size) override;

    bool TrySend(const void* data, size_t size, size_t* progress, bool* done) override;
    bool TryRecv(void* data, size_t size, size_t* progress, bool* done) override;

    int GetFd() const override;
    uint32_t GetPollEvents() const override;

    void Close() override;
    bool IsConnected() const override {
        return connected;
    }

private:
    struct Wire {
        uint64_t base_addr;
        uint32_t rkey;
        uint32_t magic;
    };

    char* SlotData(size_t slot) const {
        return buffer + kRdmaRingOffset + slot * kRdmaSlotSize;
    }
    char* ScratchData(size_t index) const {
        return buffer + kRdmaScratchOffset + index * kRdmaControlSize;
    }
    bool IsProducer() const {
        return direction_ == TransportDirection::Send;
    }

    bool OpenChannel();
    bool SetupResources();
    bool Adopt(rdma_cm_id* connection, const Wire& wire);
    bool AwaitEstablished();
    rdma_cm_event* AwaitEvent(rdma_cm_event_type type);
    bool ReadWire(const rdma_cm_event& event, Wire& wire) const;

    bool PostPoolRecv(size_t index);
    bool PostControlSend(size_t length);
    bool PostWrite(size_t slot, size_t length);
    bool PostCredit(size_t slots);

    bool ProcessCompletions();
    bool DrainCmEvents();
    bool HandleCompletion(const ibv_wc& completion);
    bool WaitFlag(bool& flag);
    void CloseCm();

    rdma_event_channel* cm_channel = nullptr;
    rdma_cm_id* cm_id = nullptr;
    ibv_pd* pd = nullptr;
    ibv_cq* cq = nullptr;
    ibv_comp_channel* comp_channel = nullptr;
    ibv_mr* mr = nullptr;
    char* buffer = nullptr;
    Wire peer{};
    uint16_t listen_port = 0;
    int epoll_fd = -1;
    bool connected = false;
    bool peer_known = false;
    bool peer_closed = false;
    bool control_received = false;
    bool control_sent = false;
    size_t control_index = 0;

    size_t send_seq = 0;
    size_t credits_received = 0;

    size_t arrival_head = 0;
    size_t arrival_tail = 0;
    size_t arrival_offset = 0;
    size_t pending_credit = 0;
    size_t arrival_lengths[kRdmaSlotCount] = {};
};
