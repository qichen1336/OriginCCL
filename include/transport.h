#pragma once

#include <string>
#include <memory>
#include <cstddef>
#include <cstdint>

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

    virtual bool TrySend(const void* data, size_t size, size_t* progress, bool* done) = 0;
    virtual bool TryRecv(void* data, size_t size, size_t* progress, bool* done) = 0;

    virtual int GetFd() const = 0;
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
