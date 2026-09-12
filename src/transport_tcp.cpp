#include <cstring>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include "transport_tcp.h"
#include "logger.h"
#include "utils.h"

TransportTCP::TransportTCP() {}

TransportTCP::~TransportTCP() {
    Close();
}

bool TransportTCP::Listen(uint16_t port) {
    listen_fd = Utils::CreateListenSocket(port, &listen_port);
    if (listen_fd < 0) {
        return false;
    }

    return true;
}

std::shared_ptr<Transport> TransportTCP::Accept() {
    if (listen_fd < 0) {
        LOG_ERROR("Not in listen mode");
        return nullptr;
    }

    int client_fd = Utils::AcceptConnection(listen_fd);
    if (client_fd < 0) {
        return nullptr;
    }

    auto new_transport = std::make_shared<TransportTCP>();
    new_transport->SetSocket(client_fd);
    return new_transport;
}

bool TransportTCP::Connect(const std::string& addr, uint16_t port) {
    sockfd = Utils::CreateConnectSocket(addr, port, 1);
    if (sockfd < 0) {
        return false;
    }

    connected = true;
    return SetNonBlocking();
}

bool TransportTCP::Send(const void* data, size_t size) {
    if (!connected || sockfd < 0) {
        LOG_ERROR("Not connected to send");
        return false;
    }

    return SendRaw(data, size);
}

bool TransportTCP::Recv(void* data, size_t size) {
    if (!connected || sockfd < 0) {
        LOG_ERROR("Not connected to recv");
        return false;
    }

    return RecvRaw(data, size);
}

void TransportTCP::Close() {
    if (sockfd >= 0) {
        close(sockfd);
        sockfd = -1;
    }
    if (listen_fd >= 0) {
        close(listen_fd);
        listen_fd = -1;
    }
    connected = false;
}

void TransportTCP::SetSocket(int fd) {
    sockfd = fd;
    connected = true;
    SetNonBlocking();
}

bool TransportTCP::SetNonBlocking() {
    if (sockfd < 0) {
        return false;
    }
    int flags = fcntl(sockfd, F_GETFL, 0);
    if (flags < 0 || fcntl(sockfd, F_SETFL, flags | O_NONBLOCK) < 0) {
        LOG_ERROR("Failed to set socket {} non-blocking", sockfd);
        return false;
    }
    return true;
}

bool TransportTCP::TrySend(const void* data, size_t size, size_t* progress, bool* done) {
    if (!connected || sockfd < 0) {
        LOG_ERROR("Not connected to send");
        return false;
    }

    const char* buffer = static_cast<const char*>(data);
    while (*progress < size) {
        ssize_t sent = send(sockfd, buffer + *progress, size - *progress, 0);
        if (sent > 0) {
            *progress += static_cast<size_t>(sent);
            continue;
        }
        if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
            break;
        }
        if (sent == 0) {
            LOG_INFO("Connection closed by peer when send");
        } else {
            LOG_ERROR("Failed to send: {}", strerror(errno));
        }
        return false;
    }

    *done = (*progress == size);
    return true;
}

bool TransportTCP::TryRecv(void* data, size_t size, size_t* progress, bool* done) {
    if (!connected || sockfd < 0) {
        LOG_ERROR("Not connected to recv");
        return false;
    }

    char* buffer = static_cast<char*>(data);
    while (*progress < size) {
        ssize_t recvd = recv(sockfd, buffer + *progress, size - *progress, 0);
        if (recvd > 0) {
            *progress += static_cast<size_t>(recvd);
            continue;
        }
        if (recvd < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
            break;
        }
        if (recvd == 0) {
            LOG_INFO("Connection closed by peer when recv");
        } else {
            LOG_ERROR("Failed to recv: {}", strerror(errno));
        }
        return false;
    }

    *done = (*progress == size);
    return true;
}

bool TransportTCP::SendRaw(const void* data, size_t size) {
    const char* buffer = static_cast<const char*>(data);
    size_t total = 0;
    while (total < size) {
        ssize_t sent = send(sockfd, buffer + total, size - total, 0);
        if (sent > 0) {
            total += static_cast<size_t>(sent);
            continue;
        }
        if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
            continue;
        }
        if (sent == 0) {
            LOG_INFO("Connection closed by peer when send");
        } else {
            LOG_ERROR("Failed to send: {}", strerror(errno));
        }
        return false;
    }

    return true;
}

bool TransportTCP::RecvRaw(void* data, size_t size) {
    char* buffer = static_cast<char*>(data);
    size_t total = 0;
    while (total < size) {
        ssize_t recvd = recv(sockfd, buffer + total, size - total, 0);
        if (recvd > 0) {
            total += static_cast<size_t>(recvd);
            continue;
        }
        if (recvd < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
            continue;
        }
        if (recvd == 0) {
            LOG_INFO("Connection close by peer when recv");
        } else {
            LOG_ERROR("Failed to recv: {}", strerror(errno));
        }
        return false;
    }

    return true;
}
