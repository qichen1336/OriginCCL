#pragma once

#include <string>
#include <memory>
#include <cstddef>

class Transport {
public:
    virtual ~Transport() = default;

    virtual bool Listen(uint16_t port) = 0;
    virtual uint16_t GetListenPort() const = 0;
    virtual std::shared_ptr<Transport> Accept() = 0;

    virtual bool Connect(const std::string& addr, uint16_t port) = 0;

    virtual bool Send(const void* data, size_t size) = 0;
    virtual bool Recv(void* data, size_t size) = 0;

    virtual void Close() = 0;
    virtual bool IsConnected() const = 0;
};
