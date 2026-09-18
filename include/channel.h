#pragma once

#include <memory>
#include <vector>
#include "transport/transport.h"

struct Ring {
    int prev = -1;
    int next = -1;
};

class Connector {
public:
    int peer = -1;
    int channel_id = -1;
    bool is_send = false;
    std::shared_ptr<Transport> transport;
};

class Channel {
public:
    int id = 0;
    Ring ring;
    std::vector<Connector> send;
    std::vector<Connector> recv;

    Connector* SendConnector(int peer) {
        if (peer < 0 || static_cast<size_t>(peer) >= send.size()) {
            return nullptr;
        }
        return &send[static_cast<size_t>(peer)];
    }

    Connector* RecvConnector(int peer) {
        if (peer < 0 || static_cast<size_t>(peer) >= recv.size()) {
            return nullptr;
        }
        return &recv[static_cast<size_t>(peer)];
    }
};
