#pragma once

#include <string>
#include <memory>
#include <cstddef>
#include <cstdint>

// Readiness condition an executor waits for on a descriptor's fd. It says nothing about
// the transport behind the fd: a TCP socket is waited on as writable to send, while a
// shared-memory ring waits on a readable notification fd in both directions.
enum class WaitCondition {
    Readable,
    Writable
};

// How an executor waits for one direction of a channel edge to become progressable. fd is
// the handle to wait on and condition is the readiness that means "this direction can
// advance"; fd < 0 means the direction exposes nothing to wait on. A transport may hand
// back the same fd for both directions, so executors must merge interest per fd.
struct WaitDescriptor {
    int fd = -1;
    WaitCondition condition = WaitCondition::Readable;
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

    // Readiness handles the executor waits on to drive each direction: SendWait() gates
    // TrySend and RecvWait() gates TryRecv. Both must stay valid for the transport's whole
    // life, so the fd may be registered across plans.
    virtual WaitDescriptor SendWait() const = 0;
    virtual WaitDescriptor RecvWait() const = 0;

    virtual void Close() = 0;
    virtual bool IsConnected() const = 0;
};
