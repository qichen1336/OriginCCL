#pragma once

#include <string>
#include <memory>
#include <cstddef>
#include <cstdint>

// Which operations a transport instance carries. Direction selects the readiness
// GetPollEvents() advertises; TCP keeps working in both directions regardless, because
// bootstrap drives its control traffic over one bidirectional object.
enum class TransportDirection {
    Bidirectional,
    Send,
    Receive
};

class Transport {
public:
    virtual ~Transport() = default;

    virtual bool Listen(uint16_t port) = 0;
    virtual uint16_t GetListenPort() const = 0;
    virtual std::shared_ptr<Transport> Accept() = 0;

    virtual bool Connect(const std::string& addr, uint16_t port) = 0;

    virtual bool Send(const void* data, size_t size) = 0;
    virtual bool Recv(void* data, size_t size) = 0;

    // Non-blocking progress used by event-driven executors. Each call advances the
    // transfer as far as the socket allows without blocking; *progress accumulates the
    // total bytes moved so far. Returns true while the operation may continue (including
    // when it would block), false on error or peer shutdown. Completion is reported via
    // the out parameter.
    virtual bool TrySend(const void* data, size_t size, size_t* progress, bool* done) = 0;
    virtual bool TryRecv(void* data, size_t size, size_t* progress, bool* done) = 0;

    // Listener descriptor while listening, data-plane descriptor once connected. The
    // transport owns it; the executor only registers and deregisters it.
    virtual int GetFd() const = 0;
    // Native readiness of GetFd() to wait for, as a Linux epoll mask (EPOLLIN/EPOLLOUT).
    // Not the operation being waited for: a send endpoint can wait on a readable eventfd.
    virtual uint32_t GetPollEvents() const = 0;

    void SetDirection(TransportDirection direction) {
        direction_ = direction;
    }
    TransportDirection GetDirection() const {
        return direction_;
    }

    virtual void Close() = 0;
    virtual bool IsConnected() const = 0;

protected:
    TransportDirection direction_ = TransportDirection::Bidirectional;
};
