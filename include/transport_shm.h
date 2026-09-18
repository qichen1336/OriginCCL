#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <string>
#include "transport.h"

constexpr size_t kShmRingCapacity = 2 * 1024 * 1024;
constexpr size_t kShmCacheLine = 64;

struct ShmRingCursors {
    alignas(kShmCacheLine) std::atomic<uint64_t> head{0};
    alignas(kShmCacheLine) std::atomic<uint64_t> tail{0};
};

static_assert(std::atomic<uint64_t>::is_always_lock_free, "shared-memory cursors require lock-free 64-bit atomics");
static_assert(sizeof(ShmRingCursors) == 2 * kShmCacheLine, "the ring cursors must not share a cache line");

constexpr size_t kShmRingMapSize = sizeof(ShmRingCursors) + kShmRingCapacity;

class TransportShm : public Transport {
public:
    ~TransportShm() override;

    bool ListenPath(const std::string& rendezvous_path);
    bool Listen(uint16_t port) override;
    uint16_t GetListenPort() const override {
        return 0;
    }
    std::shared_ptr<Transport> Accept() override;

    bool Connect(const std::string& rendezvous_path, uint16_t port) override;

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
    struct RingResources {
        int ring = -1;
        int data_ready = -1;
        int space_ready = -1;

        void Close();
    };

    struct ResourceMessage {
        uint32_t magic;
        int32_t direction;
    };

    bool IsProducer() const {
        return direction_ == TransportDirection::Send;
    }
    int WaitFd() const {
        return IsProducer() ? resources.space_ready : resources.data_ready;
    }
    ShmRingCursors* Cursors() const {
        return std::launder(reinterpret_cast<ShmRingCursors*>(map));
    }
    char* RingData() const {
        return map + sizeof(ShmRingCursors);
    }

    bool CreateRing();
    bool MapRing(int fd);
    bool Adopt(const RingResources& incoming, TransportDirection direction);
    bool SendResources(int fd) const;
    bool RecvResources(int fd, RingResources* incoming, TransportDirection* direction) const;

    bool SendControl(const void* data, size_t size) const;
    bool RecvControl(void* data, size_t size) const;

    size_t Produce(const char* data, size_t size, size_t progress);
    size_t Consume(char* data, size_t size, size_t progress);
    size_t SpaceAvailable() const;
    size_t DataAvailable() const;
    void Drain(int efd) const;
    void Notify(int efd) const;
    bool WaitReadable(int efd) const;
    void CloseControl();

    int listen_fd = -1;
    std::string listen_path;
    int control_fd = -1;
    RingResources resources;
    char* map = nullptr;
    bool connected = false;
};
