#pragma once

#include <memory>
#include "transport.h"

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
    Connector send;
    Connector recv;

    Connector* SendConnector(int peer) {
        if (send.peer == peer) {
            return &send;
        }
        return nullptr;
    }

    Connector* RecvConnector(int peer) {
        if (recv.peer == peer) {
            return &recv;
        }
        return nullptr;
    }
};
