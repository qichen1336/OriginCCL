#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <string>
#include "transport.h"

// Data capacity of one shared-memory ring, excluding its metadata.
constexpr size_t kShmRingCapacity = 2 * 1024 * 1024;
constexpr size_t kShmCacheLine = 64;

// Monotonically increasing cursors shared through the ring's memfd. Only their difference
// matters, and each sits on its own cache line so the producer and the consumer never
// write the same one. head is written by the producer, tail by the consumer.
struct ShmRingCursors {
    alignas(kShmCacheLine) std::atomic<uint64_t> head{0};
    alignas(kShmCacheLine) std::atomic<uint64_t> tail{0};
};

static_assert(std::atomic<uint64_t>::is_always_lock_free, "shared-memory cursors require lock-free 64-bit atomics");
static_assert(sizeof(ShmRingCursors) == 2 * kShmCacheLine, "the ring cursors must not share a cache line");

constexpr size_t kShmRingMapSize = sizeof(ShmRingCursors) + kShmRingCapacity;

// One directed endpoint of a shared-memory ring, following the listener/connection
// pattern of TransportTCP. The active endpoint builds the ring and transfers it over a
// SOCK_SEQPACKET Unix domain socket; the passive endpoint adopts the complementary
// direction. Both then exchange the caller's first blocking handshake through that
// control socket and close it, which is why no caller has to know which transport it
// holds. Direction is a data-plane constraint here, unlike TCP: a producer endpoint only
// sends and a consumer endpoint only receives.
class TransportShm : public Transport {
public:
    TransportShm();
    ~TransportShm() override;

    bool ListenPath(const std::string& rendezvous_path);
    // Shared memory binds a path, not a port; this only reports the mismatch.
    bool Listen(uint16_t port) override;
    uint16_t GetListenPort() const override {
        return 0;
    }
    std::shared_ptr<Transport> Accept() override;

    // Active side. SetDirection() picks which half of the ring this endpoint owns; `port`
    // belongs to the signature shared with TCP and is unused.
    bool Connect(const std::string& rendezvous_path, uint16_t port) override;

    bool Send(const void* data, size_t size) override;
    bool Recv(void* data, size_t size) override;

    bool TrySend(const void* data, size_t size, size_t* progress, bool* done) override;
    bool TryRecv(void* data, size_t size, size_t* progress, bool* done) override;

    // The rendezvous descriptor while listening, otherwise the wait eventfd -- valid from
    // the moment the endpoint is established, which is after the channel handshake has
    // already completed, so an executor only ever sees the eventfd.
    int GetFd() const override;
    uint32_t GetPollEvents() const override;

    void Close() override;
    bool IsConnected() const override {
        return connected;
    }

private:
    // The three descriptors every ring owns: the memfd and the two notification eventfds.
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
    bool active = false;
    bool connected = false;
};
