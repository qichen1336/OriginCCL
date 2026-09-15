#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include "transport.h"

// Shared-memory data plane for edges whose two ranks sit on the same machine: a memfd-backed
// SPSC ring streamed in one direction per instance, announced by two eventfds handed to the
// peer over a short-lived abstract Unix socket.
class TransportSHM : public Transport {
public:
    TransportSHM();
    ~TransportSHM() override;

    bool Listen(uint16_t port) override;
    uint16_t GetListenPort() const override {
        return listen_port;
    }

    std::shared_ptr<Transport> Accept() override;
    bool Connect(const std::string& addr, uint16_t port) override;

    bool Send(const void* data, size_t size) override;
    bool Recv(void* data, size_t size) override;

    bool TrySend(const void* data, size_t size, size_t* progress, bool* done) override;
    bool TryRecv(void* data, size_t size, size_t* progress, bool* done) override;
    WaitDescriptor SendWait() const override {
        return WaitDescriptor{space_fd, WaitCondition::Readable};
    }

    WaitDescriptor RecvWait() const override {
        return WaitDescriptor{data_fd, WaitCondition::Readable};
    }

    void Close() override;
    bool IsConnected() const override {
        return connected;
    }

private:
    struct Ring;

    bool CreateRing();
    bool HandOverRing();
    bool RecvRingFds();
    void FinishRendezvous();

    size_t Produce(const char* src, size_t size);
    size_t Consume(char* dst, size_t size);
    size_t Room() const;
    char* Data();
    bool PeerClosed() const;

    int listen_fd = -1;
    int rendezvous_fd = -1;
    uint16_t listen_port = 0;

    int ring_fd = -1;
    int data_fd = -1;
    int space_fd = -1;
    Ring* ring = nullptr;

    bool sent = false;
    bool received = false;
    bool connected = false;
};
