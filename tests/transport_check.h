#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include "transport/transport.h"

namespace TransportCheck {

// One semantics suite drives all three transports. The differences that cannot be shared are
// declared here instead of being guessed inside the suite:
//   enforces_direction      shared memory and RDMA reject the wrong data-plane direction, TCP
//                           only records the direction as metadata
//   fixed_poll_events       shared memory and RDMA always report EPOLLIN, TCP reports the
//                           readiness mask of the direction
//   detects_peer_close      TCP sees the orderly close through a zero-length read. Shared
//                           memory and RDMA do not promise a peer-death signal (closing the
//                           peer does not reliably flush the local ring or receive queue),
//                           so they only verify that Close() releases the endpoint
struct Setup {
    const char* name = "";
    std::string listen_addr;
    std::string connect_addr;
    bool enforces_direction = true;
    bool fixed_poll_events = true;
    bool detects_peer_close = true;
    std::shared_ptr<Transport> (*make)() = nullptr;
};

// Returns 0 when the suite passes, 1 when it fails. Rank 0 listens, rank 1 connects, and the
// suite builds one connection in each data direction so both roles are covered.
int RunSuite(const Setup& setup, int rank, int world_size);
} // namespace TransportCheck
