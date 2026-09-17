#pragma once

#include <cstddef>
#include <cstdint>
#include "types.h"
#include "transport.h"

// A task needs at most one wait per direction.
constexpr size_t kTransportWaitMax = 2;

// One readiness registration for a task: the descriptor to wait on, the readiness to wait
// for, and which logical operation its readiness advances.
struct TransportWait {
    int fd = -1;
    uint32_t events = 0;
    bool writable = false;
    bool readable = false;
};

// Fill `waits` and return how many registrations the task needs. Readiness on the send
// transport advances the writable operation, readiness on the receive transport the
// readable one. Because a descriptor shared by both is registered once and carries both
// directions, neither side of a two-rank ring (prev == next) is starved.
inline size_t BuildTransportWaits(const PlanTask& task, TransportWait waits[kTransportWaitMax]) {
    size_t count = 0;
    if (task.recv_transport) {
        int fd = task.recv_transport->GetFd();
        uint32_t events = task.recv_transport->GetPollEvents();
        if (fd >= 0 && events != 0) {
            waits[count++] = TransportWait{fd, events, false, true};
        }
    }
    if (!task.send_transport) {
        return count;
    }

    int fd = task.send_transport->GetFd();
    uint32_t events = task.send_transport->GetPollEvents();
    if (fd < 0 || events == 0) {
        return count;
    }
    if (count > 0 && waits[0].fd == fd) {
        waits[0].events |= events;
        waits[0].writable = true;
        return count;
    }
    waits[count++] = TransportWait{fd, events, true, false};
    return count;
}
