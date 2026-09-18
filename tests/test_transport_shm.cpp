#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <fstream>
#include <functional>
#include <memory>
#include <poll.h>
#include <string>
#include <sys/epoll.h>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>
#include "logger.h"
#include "transport/transport_shm.h"

namespace {

int failures = 0;

// A silent forked endpoint must fail rather than deadlock.
constexpr int kAwaitTimeoutMs = 5000;

bool Expect(bool condition, const std::string& what) {
    if (!condition) {
        LOG_ERROR("FAIL: {}", what);
        ++failures;
    }
    return condition;
}

int CountOpenFds() {
    DIR* dir = opendir("/proc/self/fd");
    if (dir == nullptr) {
        return -1;
    }
    int count = 0;
    while (readdir(dir) != nullptr) {
        ++count;
    }
    closedir(dir);
    return count;
}

int CountShmMappings() {
    std::ifstream maps("/proc/self/maps");
    int count = 0;
    std::string line;
    while (std::getline(maps, line)) {
        if (line.find("memfd:originccl") != std::string::npos) {
            ++count;
        }
    }
    return count;
}

bool ReadableNow(int fd) {
    pollfd pfd{fd, POLLIN, 0};
    return poll(&pfd, 1, 0) > 0;
}

// Orders the two forked endpoints around operations that would otherwise race.
struct Pipe {
    int read_fd = -1;
    int write_fd = -1;
};

Pipe OpenPipe() {
    int fds[2];
    if (pipe(fds) != 0) {
        LOG_ERROR("pipe failed");
        std::exit(1);
    }
    return Pipe{fds[0], fds[1]};
}

void Signal(const Pipe& pipe) {
    const char byte = 1;
    if (write(pipe.write_fd, &byte, 1) != 1) {
        LOG_ERROR("pipe write failed");
        std::exit(1);
    }
}

bool Await(const Pipe& pipe) {
    pollfd pfd{pipe.read_fd, POLLIN, 0};
    if (poll(&pfd, 1, kAwaitTimeoutMs) <= 0) {
        LOG_ERROR("The forked endpoint did not reach this step within {} ms", kAwaitTimeoutMs);
        return false;
    }
    char byte = 0;
    ssize_t rc = 0;
    do {
        rc = read(pipe.read_fd, &byte, 1);
    } while (rc < 0 && errno == EINTR);
    return rc == 1;
}

// Derived by both endpoints independently, so corruption is caught without sharing a buffer.
char PayloadByte(size_t index) {
    return static_cast<char>((index * 7 + 13) % 251);
}

std::vector<char> MakePayload(size_t size) {
    std::vector<char> payload(size);
    for (size_t i = 0; i < size; ++i) {
        payload[i] = PayloadByte(i);
    }
    return payload;
}

bool PayloadMatches(const char* buffer, size_t length, size_t offset) {
    for (size_t i = 0; i < length; ++i) {
        if (buffer[i] != PayloadByte(offset + i)) {
            LOG_ERROR("Payload mismatch at {}: expected {}, got {}", offset + i, PayloadByte(offset + i), buffer[i]);
            return false;
        }
    }
    return true;
}

// The communicator's connection handshake: on shared memory it travels over the control socket.
struct ControlHandshake {
    int rank = 0;
    int channel_id = 0;
    int is_send = 0;
};

// One less open descriptor after the handshake means the control socket was really closed.
struct HandshakeFacts {
    int fds_before = 0;
    int fds_after = 0;
};

using ActiveWork = std::function<bool(Transport&, const HandshakeFacts&, Pipe&, Pipe&)>;
using PassiveWork = std::function<bool(Transport&, const HandshakeFacts&, Pipe&, Pipe&)>;

// One shared-memory pair: listener here, active endpoint in a forked child, handshake first.
bool RunPair(const std::string& tag, const ActiveWork& active, const PassiveWork& passive) {
    const std::string path = "/tmp/originccl-shm-test-" + std::to_string(getpid()) + "-" + tag + ".sock";
    Pipe to_child = OpenPipe();
    Pipe to_parent = OpenPipe();
    ControlHandshake handshake;
    handshake.rank = 1;
    handshake.channel_id = 0;
    handshake.is_send = 1;

    auto listener = std::make_shared<TransportShm>();
    if (!listener->ListenPath(path)) {
        LOG_ERROR("[{}] Failed to listen on {}", tag, path);
        return false;
    }

    const pid_t pid = fork();
    if (pid < 0) {
        LOG_ERROR("[{}] fork failed", tag);
        return false;
    }
    if (pid == 0) {
        const int inherited_failures = failures;
        TransportShm producer;
        producer.SetDirection(TransportDirection::Send);
        if (!producer.Connect(path, 0)) {
            LOG_ERROR("[{}] child: failed to connect to {}", tag, path);
            _exit(2);
        }
        HandshakeFacts facts;
        facts.fds_before = CountOpenFds();
        const bool handshook = producer.Send(&handshake, sizeof(handshake));
        facts.fds_after = CountOpenFds();
        if (!handshook) {
            LOG_ERROR("[{}] child: control handshake failed", tag);
            _exit(3);
        }
        const bool passed = active(producer, facts, to_child, to_parent) && failures == inherited_failures;
        producer.Close();
        _exit(passed ? 0 : 1);
    }

    std::shared_ptr<Transport> consumer = listener->Accept();
    if (!consumer) {
        LOG_ERROR("[{}] Failed to accept on {}", tag, path);
        return false;
    }
    HandshakeFacts facts;
    facts.fds_before = CountOpenFds();
    ControlHandshake received;
    const bool handshook = consumer->Recv(&received, sizeof(received));
    facts.fds_after = CountOpenFds();
    bool passed = handshook;
    if (handshook) {
        Expect(received.rank == 1 && received.channel_id == 0 && received.is_send == 1,
               "the connection handshake arrives intact");
        passed = passive(*consumer, facts, to_child, to_parent);
    } else {
        LOG_ERROR("[{}] control handshake failed on the accepted endpoint", tag);
    }
    consumer->Close();
    listener->Close();

    int status = 0;
    if (waitpid(pid, &status, 0) < 0) {
        LOG_ERROR("[{}] waitpid failed", tag);
        return false;
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        LOG_ERROR("[{}] child endpoint exited with status {:#x}", tag, status);
        passed = false;
    }
    if (!passed) {
        LOG_ERROR("[{}] scenario failed", tag);
    }
    return passed;
}

// Checks the readiness contract and drives one blocking transfer larger than the ring.
bool TestBlockingTransfer() {
    const size_t size = 3 * 1024 * 1024;

    const ActiveWork active = [size](Transport& producer, const HandshakeFacts& facts, Pipe& to_child,
                                     Pipe& to_parent) {
        Expect(producer.GetPollEvents() == EPOLLIN, "a shared-memory endpoint waits for readable readiness");
        Expect(ReadableNow(producer.GetFd()), "a producer endpoint starts ready");
        Expect(facts.fds_after == facts.fds_before - 1,
               "the producer closes the control socket after the acknowledgement");
        Signal(to_parent);
        Expect(Await(to_child), "the consumer checked its side of the empty ring");
        std::vector<char> payload = MakePayload(size);
        return Expect(producer.Send(payload.data(), payload.size()), "blocking send of a payload larger than the ring");
    };

    const PassiveWork passive = [size](Transport& consumer, const HandshakeFacts& facts, Pipe& to_child,
                                       Pipe& to_parent) {
        Expect(Await(to_parent), "the producer checked its readiness first");
        Expect(!ReadableNow(consumer.GetFd()), "a consumer endpoint starts empty");
        Expect(facts.fds_after == facts.fds_before - 1, "the consumer closes the control socket after acknowledging");

        int probe = 0;
        size_t progress = 0;
        bool done = false;
        Expect(consumer.TryRecv(&probe, sizeof(probe), &progress, &done), "non-blocking receive on an empty ring");
        Expect(progress == 0 && !done, "the connection handshake did not travel through the ring");

        Signal(to_child);
        std::vector<char> payload(size);
        Expect(consumer.Recv(payload.data(), payload.size()), "blocking receive of a payload larger than the ring");
        Expect(PayloadMatches(payload.data(), payload.size(), 0),
               "the blocking receive returns what the producer sent");
        return true;
    };

    return RunPair("blocking", active, passive);
}

// Non-blocking only: partial progress, backpressure and both wrap points, step by step.
bool TestNonBlockingWrapAndBackpressure() {
    constexpr size_t kSlice = 1024 * 1024;
    const size_t size = kShmRingCapacity + 2 * kSlice + kSlice / 2; // 2 MiB + 1 MiB + 1.5 MiB

    const ActiveWork active = [size](Transport& producer, const HandshakeFacts&, Pipe& to_child, Pipe& to_parent) {
        std::vector<char> payload = MakePayload(size);
        size_t progress = 0;
        bool done = false;

        Expect(producer.TrySend(payload.data(), payload.size(), &progress, &done),
               "a non-blocking send reports progress");
        Expect(progress == kShmRingCapacity && !done, "a non-blocking send stops at the ring capacity");

        const size_t filled = progress;
        Expect(producer.TrySend(payload.data(), payload.size(), &progress, &done) && progress == filled && !done,
               "a full ring makes the next send move nothing");
        Expect(!ReadableNow(producer.GetFd()), "a send that cannot progress drains its readiness");

        Signal(to_parent);
        Expect(Await(to_child), "the consumer freed one slice");
        Expect(ReadableNow(producer.GetFd()), "the consumer's notification wakes the producer");
        Expect(producer.TrySend(payload.data(), payload.size(), &progress, &done) && progress - filled == kSlice &&
                   !done,
               "the producer writes exactly the space the consumer freed");

        const size_t refilled = progress;
        Signal(to_parent);
        Expect(Await(to_child), "the consumer freed the ring again");
        Expect(producer.TrySend(payload.data(), payload.size(), &progress, &done) &&
                   progress - refilled == kSlice + kSlice / 2 && done,
               "the last slice crosses the end of the ring and completes the send");
        Expect(progress == payload.size(), "the whole payload was sent through the ring");
        Signal(to_parent);
        return true;
    };

    const PassiveWork passive = [](Transport& consumer, const HandshakeFacts&, Pipe& to_child, Pipe& to_parent) {
        Expect(Await(to_parent), "the producer filled the ring first");
        Expect(ReadableNow(consumer.GetFd()), "the producer's notification makes the consumer ready");

        std::vector<char> slice(kShmRingCapacity);
        size_t progress = 0;
        bool done = false;
        Expect(consumer.TryRecv(slice.data(), 1024 * 1024, &progress, &done) && done && progress == 1024 * 1024,
               "the consumer takes the first slice");
        Expect(PayloadMatches(slice.data(), 1024 * 1024, 0), "the first slice carries the payload");
        Signal(to_child);

        Expect(Await(to_parent), "the producer refilled the freed space");
        progress = 0;
        done = false;
        const size_t second = 2 * 1024 * 1024;
        Expect(consumer.TryRecv(slice.data(), second, &progress, &done) && done && progress == second,
               "the consumer takes the wrapped second slice in one call");
        Expect(PayloadMatches(slice.data(), second, 1024 * 1024),
               "the second slice carries the payload across the wrap");
        Signal(to_child);

        Expect(Await(to_parent), "the producer sent the last slice");
        progress = 0;
        done = false;
        const size_t third = 3 * 1024 * 1024 / 2;
        Expect(consumer.TryRecv(slice.data(), third, &progress, &done) && done && progress == third,
               "the consumer takes the last slice");
        Expect(PayloadMatches(slice.data(), third, 3 * 1024 * 1024),
               "the last slice carries the payload after the wrap");
        return true;
    };

    return RunPair("non_blocking", active, passive);
}

// Repeated rounds: no endpoint stays readable from stale counts, and later sends still wake.
bool TestRepeatedOperations() {
    constexpr int kRounds = 3;
    constexpr size_t kRoundBytes = 64;

    const ActiveWork active = [](Transport& producer, const HandshakeFacts&, Pipe& to_child, Pipe& to_parent) {
        for (int round = 0; round < kRounds; ++round) {
            std::vector<char> payload(kRoundBytes, static_cast<char>(round));
            size_t progress = 0;
            bool done = false;
            Expect(producer.TrySend(payload.data(), payload.size(), &progress, &done) && done &&
                       progress == payload.size(),
                   "a small send completes in one call");
            Signal(to_parent);
            Expect(Await(to_child), "the consumer finished the round");
        }
        return true;
    };

    const PassiveWork passive = [](Transport& consumer, const HandshakeFacts&, Pipe& to_child, Pipe& to_parent) {
        for (int round = 0; round < kRounds; ++round) {
            Expect(Await(to_parent), "the producer sent a round");
            Expect(ReadableNow(consumer.GetFd()), "a new send wakes the consumer every round");

            std::vector<char> payload(kRoundBytes);
            size_t progress = 0;
            bool done = false;
            Expect(consumer.TryRecv(payload.data(), payload.size(), &progress, &done) && done &&
                       progress == payload.size(),
                   "a small receive completes in one call");
            Expect(std::all_of(payload.begin(), payload.end(),
                               [round](char byte) { return byte == static_cast<char>(round); }),
                   "the round carries the byte the producer wrote");

            progress = 0;
            done = false;
            Expect(consumer.TryRecv(payload.data(), payload.size(), &progress, &done) && progress == 0 && !done,
                   "a receive with an empty ring moves nothing");
            Expect(!ReadableNow(consumer.GetFd()), "a receive that cannot progress drains its readiness");
            Signal(to_child);
        }
        return true;
    };

    return RunPair("repeated", active, passive);
}

// The opposite operation is a reported error, not a silently unusable transfer.
bool TestDirectionRejection() {
    LOG_INFO("Expect direction errors: the next checks call the wrong operation on purpose");
    const ActiveWork active = [](Transport& producer, const HandshakeFacts&, Pipe&, Pipe&) {
        Expect(producer.GetDirection() == TransportDirection::Send,
               "the active endpoint keeps the direction it declared");
        std::vector<char> buffer(16);
        size_t progress = 0;
        bool done = false;
        Expect(!producer.Recv(buffer.data(), buffer.size()), "a producer endpoint rejects a blocking receive");
        Expect(!producer.TryRecv(buffer.data(), buffer.size(), &progress, &done),
               "a producer endpoint rejects a non-blocking receive");
        return true;
    };

    const PassiveWork passive = [](Transport& consumer, const HandshakeFacts&, Pipe&, Pipe&) {
        Expect(consumer.GetDirection() == TransportDirection::Receive,
               "the passive endpoint adopts the complementary direction");
        const std::vector<char> buffer(16, 0);
        size_t progress = 0;
        bool done = false;
        Expect(!consumer.Send(buffer.data(), buffer.size()), "a consumer endpoint rejects a blocking send");
        Expect(!consumer.TrySend(buffer.data(), buffer.size(), &progress, &done),
               "a consumer endpoint rejects a non-blocking send");
        return true;
    };

    return RunPair("direction", active, passive);
}

// Closing an endpoint gives back its wait descriptor and its ring mapping.
bool TestResourceRelease() {
    const ActiveWork active = [](Transport&, const HandshakeFacts&, Pipe&, Pipe&) { return true; };

    const PassiveWork passive = [](Transport& consumer, const HandshakeFacts&, Pipe&, Pipe&) {
        Expect(consumer.IsConnected(), "the accepted endpoint is connected");
        const int wait_fd = consumer.GetFd();
        Expect(wait_fd >= 0, "an established endpoint exposes a wait descriptor");
        const int mappings = CountShmMappings();
        Expect(mappings > 0, "an established endpoint maps the ring");

        consumer.Close();
        Expect(!consumer.IsConnected(), "a closed endpoint is disconnected");
        Expect(fcntl(wait_fd, F_GETFD) == -1, "closing the endpoint closes its wait descriptor");
        Expect(CountShmMappings() == mappings - 1, "closing the endpoint unmaps the ring");
        return true;
    };

    return RunPair("release", active, passive);
}

// Setup failures fail the endpoint, with no fallback to TCP.
bool TestSetupFailures() {
    LOG_INFO("Expect setup errors: the next checks use a missing path and a port on purpose");
    const std::string prefix = "/tmp/originccl-shm-test-" + std::to_string(getpid());

    const std::string absent = prefix + "-absent.sock";
    unlink(absent.c_str());
    TransportShm endpoint;
    endpoint.SetDirection(TransportDirection::Send);
    Expect(!endpoint.Connect(absent, 0), "connecting to a missing rendezvous path fails");
    Expect(!endpoint.IsConnected(), "a failed connection leaves the endpoint disconnected");
    Expect(!endpoint.Listen(0), "shared memory does not bind a port");

    const std::string stale = prefix + "-stale.sock";
    FILE* leftover = fopen(stale.c_str(), "w");
    if (leftover != nullptr) {
        fclose(leftover);
    }
    TransportShm listener;
    Expect(listener.ListenPath(stale), "a stale rendezvous path does not prevent binding");
    Expect(access(stale.c_str(), F_OK) == 0, "the rendezvous path exists while the listener is open");
    Expect(listener.GetPollEvents() == EPOLLIN, "a listener waits for readable readiness");
    listener.Close();
    Expect(access(stale.c_str(), F_OK) != 0, "closing the listener removes its rendezvous path");
    return true;
}

} // namespace

int main() {
    // Every scenario is a bounded sequence of exchanges; anything longer is a deadlock.
    alarm(120);

    bool passed = true;
    passed = TestBlockingTransfer() && passed;
    passed = TestNonBlockingWrapAndBackpressure() && passed;
    passed = TestRepeatedOperations() && passed;
    passed = TestDirectionRejection() && passed;
    passed = TestResourceRelease() && passed;
    passed = TestSetupFailures() && passed;

    if (!passed || failures != 0) {
        LOG_ERROR("Shared-memory transport test failed with {} failed checks", failures);
        return 1;
    }
    LOG_INFO("Shared-memory transport test passed");
    return 0;
}
