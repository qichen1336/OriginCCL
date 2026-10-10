#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <deque>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <fmt/format.h>
#include <mpi.h>
#include "logger.h"
#include "transport/transport.h"
#include "transport/transport_rdma.h"

using Clock = std::chrono::steady_clock;

namespace {
constexpr size_t kMessageSize = 32 * 1024;
constexpr uint64_t kBytesPerPhase = 12ULL * 1000 * 1000 * 1000;
constexpr uint32_t kControlMagic = 0x4f43434c;

struct ControlMessage {
    uint32_t magic;
    int32_t role;
};

double ToUs(Clock::duration d) {
    return std::chrono::duration<double, std::micro>(d).count();
}

struct Telemetry {
    std::deque<Clock::time_point> credit_posts;
    std::vector<double> credit_latencies_us;
    std::vector<double> stall_durations_us;
    bool in_stall = false;
    Clock::time_point stall_start;
};

class InstrumentedTransport : public TransportRDMA {
public:
    Telemetry telemetry;

    std::shared_ptr<TransportRDMA> MakePeer() override {
        return std::make_shared<InstrumentedTransport>();
    }

    bool TrySend(const void* data, size_t size, size_t* progress, bool* done) override {
        const size_t before = *progress;
        const bool ok = TransportRDMA::TrySend(data, size, progress, done);
        if (!ok) {
            return false;
        }
        if (*progress > before) {
            if (telemetry.in_stall) {
                telemetry.stall_durations_us.push_back(ToUs(Clock::now() - telemetry.stall_start));
                telemetry.in_stall = false;
            }
        } else if (!*done) {
            if (!telemetry.in_stall) {
                telemetry.stall_start = Clock::now();
                telemetry.in_stall = true;
            }
        }
        return true;
    }

    bool TryRecv(void* data, size_t size, size_t* progress, bool* done) override {
        const size_t before = *progress;
        const bool ok = TransportRDMA::TryRecv(data, size, progress, done);
        if (!ok) {
            return false;
        }
        if (*progress > before) {
            telemetry.credit_posts.push_back(Clock::now());
        }
        return true;
    }

protected:
    bool HandleCompletion(const ibv_wc& completion) override {
        if (completion.opcode == IBV_WC_RDMA_WRITE && completion.wr_id == 0) {
            const auto now = Clock::now();
            if (!telemetry.credit_posts.empty()) {
                telemetry.credit_latencies_us.push_back(ToUs(now - telemetry.credit_posts.front()));
                telemetry.credit_posts.pop_front();
            }
        }
        return TransportRDMA::HandleCompletion(completion);
    }
};

void Report(int rank, const char* label, const std::vector<double>& samples) {
    if (samples.empty()) {
        fmt::print("rank {}  {}: n=0\n", rank, label);
        return;
    }
    std::vector<double> s = samples;
    std::sort(s.begin(), s.end());
    double sum = 0;
    for (double v : s) {
        sum += v;
    }
    const size_t n = s.size();
    fmt::print("rank {}  {}: n={} mean={:.1f}us p50={:.1f}us p99={:.1f}us max={:.1f}us\n", rank, label, n, sum / n,
               s[n / 2], s[static_cast<size_t>(n * 0.99)], s.back());
}

std::shared_ptr<InstrumentedTransport> ConnectOne(int rank, bool rank0_sends, int tag, const std::string& address) {
    const TransportDirection direction =
        (rank == 0) == rank0_sends ? TransportDirection::Send : TransportDirection::Receive;
    if (rank == 0) {
        auto listener = std::make_shared<InstrumentedTransport>();
        if (!listener->Listen(address, 0)) {
            return nullptr;
        }
        const uint16_t port = listener->GetListenPort();
        MPI_Send(&port, 1, MPI_UNSIGNED_SHORT, 1, tag, MPI_COMM_WORLD);
        auto connection = std::dynamic_pointer_cast<InstrumentedTransport>(listener->Accept());
        listener->Close();
        if (!connection) {
            return nullptr;
        }
        connection->SetDirection(direction);
        ControlMessage message{};
        if (!connection->Recv(&message, sizeof(message))) {
            return nullptr;
        }
        if (message.magic != kControlMagic || message.role != 1) {
            return nullptr;
        }
        return connection;
    }
    uint16_t port = 0;
    MPI_Recv(&port, 1, MPI_UNSIGNED_SHORT, 0, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    auto connection = std::make_shared<InstrumentedTransport>();
    connection->SetDirection(direction);
    if (!connection->Connect(address, port)) {
        return nullptr;
    }
    ControlMessage message{kControlMagic, rank};
    if (!connection->Send(&message, sizeof(message))) {
        return nullptr;
    }
    return connection;
}

void Drive(InstrumentedTransport& transport, bool producer, const char* payload, char* received, uint64_t budget,
           std::atomic<bool>& ok, uint64_t* messages) {
    size_t progress = 0;
    uint64_t msgs = 0;
    while (ok.load() && budget > 0) {
        const size_t current = static_cast<size_t>(std::min<uint64_t>(kMessageSize, budget));
        bool done = false;
        const bool okc = producer ? transport.TrySend(payload, current, &progress, &done)
                                  : transport.TryRecv(received, current, &progress, &done);
        if (!okc) {
            ok.store(false);
            return;
        }
        if (done) {
            budget -= current;
            progress = 0;
            ++msgs;
        }
    }
    *messages = msgs;
}
} // namespace

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);
    int rank = 0;
    int world_size = 1;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    if (world_size != 2) {
        if (rank == 0) {
            LOG_ERROR("credit_probe: expected exactly two ranks, got {}", world_size);
        }
        MPI_Finalize();
        return 1;
    }

    std::string address;
    int local_device_ok = TransportRDMA::Probe(address) ? 1 : 0;
    int all_devices_ok = 0;
    MPI_Allreduce(&local_device_ok, &all_devices_ok, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);
    if (!all_devices_ok) {
        if (rank == 0) {
            LOG_INFO("credit_probe: no active RDMA port found; skipping");
        }
        MPI_Finalize();
        return 2;
    }

    char address_buffer[64] = {};
    if (rank == 0) {
        std::snprintf(address_buffer, sizeof(address_buffer), "%s", address.c_str());
    }
    MPI_Bcast(address_buffer, sizeof(address_buffer), MPI_CHAR, 0, MPI_COMM_WORLD);

    auto from_rank0 = ConnectOne(rank, true, 41, std::string(address_buffer));
    auto from_rank1 = ConnectOne(rank, false, 42, std::string(address_buffer));
    if (!from_rank0 || !from_rank1) {
        MPI_Abort(MPI_COMM_WORLD, 1);
        return 1;
    }
    auto outgoing = rank == 0 ? from_rank0 : from_rank1;
    auto incoming = rank == 0 ? from_rank1 : from_rank0;
    outgoing->SetWaitMode(TransportWaitMode::Polling);
    incoming->SetWaitMode(TransportWaitMode::Polling);

    std::vector<char> payload(kMessageSize);
    std::vector<char> received(kMessageSize);

    if (rank == 0) {
        fmt::print("credit probe: uni then bi on the same connections, {} bytes per phase per direction\n",
                   kBytesPerPhase);
    }

    // Phase 1: unidirectional (rank0 -> rank1 only). The reverse wire is idle, so rank1's credit
    // frames (which travel rank1 -> rank0) face no reverse traffic.
    std::atomic<bool> ok{true};
    uint64_t msgs = 0;
    if (rank == 0) {
        Drive(*outgoing, true, payload.data(), received.data(), kBytesPerPhase, ok, &msgs);
    } else {
        Drive(*incoming, false, payload.data(), received.data(), kBytesPerPhase, ok, &msgs);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    fmt::print("=== rank {}  [uni] ===\n", rank);
    if (rank == 0) {
        Report(rank, "sender stall (outgoing)", outgoing->telemetry.stall_durations_us);
    } else {
        Report(rank, "receiver credit latency (incoming)", incoming->telemetry.credit_latencies_us);
    }
    outgoing->telemetry = Telemetry{};
    incoming->telemetry = Telemetry{};

    // Phase 2: bidirectional. Both directions saturate, so each receiver's credit frames now share
    // the egress direction with the opposite data stream.
    ok.store(true);
    std::thread sender(Drive, std::ref(*outgoing), true, payload.data(), received.data(), kBytesPerPhase, std::ref(ok),
                       &msgs);
    std::thread receiver(Drive, std::ref(*incoming), false, payload.data(), received.data(), kBytesPerPhase,
                         std::ref(ok), &msgs);
    sender.join();
    receiver.join();
    MPI_Barrier(MPI_COMM_WORLD);
    fmt::print("=== rank {}  [bi] ===\n", rank);
    Report(rank, "sender stall (outgoing)", outgoing->telemetry.stall_durations_us);
    Report(rank, "receiver credit latency (incoming)", incoming->telemetry.credit_latencies_us);

    MPI_Finalize();
    return 0;
}
