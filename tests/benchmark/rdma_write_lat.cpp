#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <numeric>
#include <string>
#include <vector>
#include <fmt/format.h>
#include <mpi.h>
#include "logger.h"
#include "transport/transport.h"
#include "transport/transport_rdma.h"

using Clock = std::chrono::steady_clock;

namespace {
constexpr size_t kLatencyBufferSize = 256 * 1024;
constexpr size_t kMessageSize = 4096;
constexpr uint64_t kDefaultIterations = 1000;
constexpr uint64_t kWarmup = 200;

// The CM managed queue pair is the only kind iWARP can run, so the probe lets the transport
// establish the connection and only adds the two verbs under test: a signaled one sided
// RDMA_WRITE, and the ibv_poll_cq that reaps its completion. The completion arrives after the
// peer acks the write, so the measured interval is one write plus one acknowledgement.
class WriteLatencyProbe : public TransportRDMA {
public:
    ~WriteLatencyProbe() override {
        if (mr_ != nullptr) {
            ibv_dereg_mr(mr_);
        }
        std::free(buffer_);
    }

    bool RegisterBuffer() {
        buffer_ = static_cast<char*>(std::aligned_alloc(4096, kLatencyBufferSize));
        if (buffer_ == nullptr) {
            return false;
        }
        std::memset(buffer_, 0, kLatencyBufferSize);
        mr_ = ibv_reg_mr(pd, buffer_, kLatencyBufferSize, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
        return mr_ != nullptr;
    }

    uint64_t LocalAddress() const {
        return reinterpret_cast<uint64_t>(buffer_);
    }
    uint32_t LocalRkey() const {
        return mr_->rkey;
    }

    bool PostWrite(size_t size, uint64_t remote_addr, uint32_t remote_rkey) {
        ibv_sge element{reinterpret_cast<uintptr_t>(buffer_), static_cast<uint32_t>(size), mr_->lkey};
        ibv_send_wr request{};
        request.wr_id = 1;
        request.opcode = IBV_WR_RDMA_WRITE;
        request.send_flags = IBV_SEND_SIGNALED;
        request.sg_list = &element;
        request.num_sge = 1;
        request.wr.rdma.remote_addr = remote_addr;
        request.wr.rdma.rkey = remote_rkey;
        ibv_send_wr* failed = nullptr;
        return ibv_post_send(cm_id->qp, &request, &failed) == 0;
    }

    bool PollWriteCompletion() {
        ibv_wc completion{};
        for (;;) {
            const int count = ibv_poll_cq(cq, 1, &completion);
            if (count < 0) {
                return false;
            }
            if (count == 1) {
                return completion.status == IBV_WC_SUCCESS;
            }
        }
    }

protected:
    std::shared_ptr<TransportRDMA> MakePeer() override {
        return std::make_shared<WriteLatencyProbe>();
    }

private:
    char* buffer_ = nullptr;
    ibv_mr* mr_ = nullptr;
};

std::shared_ptr<WriteLatencyProbe> Connect(int rank, const std::string& address) {
    if (rank == 0) {
        WriteLatencyProbe listener;
        if (!listener.Listen(address, 0)) {
            return nullptr;
        }
        const uint16_t port = listener.GetListenPort();
        MPI_Send(&port, 1, MPI_UNSIGNED_SHORT, 1, 0, MPI_COMM_WORLD);
        auto connection = std::dynamic_pointer_cast<WriteLatencyProbe>(listener.Accept());
        listener.Close();
        if (connection != nullptr) {
            connection->SetDirection(TransportDirection::Send);
        }
        return connection;
    }
    uint16_t port = 0;
    MPI_Recv(&port, 1, MPI_UNSIGNED_SHORT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    auto connection = std::make_shared<WriteLatencyProbe>();
    if (!connection->Connect(address, port)) {
        return nullptr;
    }
    connection->SetDirection(TransportDirection::Receive);
    return connection;
}

void Report(const std::vector<double>& samples) {
    const size_t count = samples.size();
    const double sum = std::accumulate(samples.begin(), samples.end(), 0.0);
    fmt::print("{:>8.2f}  {:>8.2f}  {:>8.2f}  {:>8.2f}  {:>8.2f}\n", samples.front(), samples[count / 2], sum / count,
               samples[static_cast<size_t>(count * 0.99)], samples.back());
    std::fflush(stdout);
}
} // namespace

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);
    int rank = 0;
    int world_size = 1;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    if (world_size != 2) {
        MPI_Finalize();
        return 1;
    }

    const uint64_t iterations = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : kDefaultIterations;

    std::string address;
    if (!TransportRDMA::Probe(address)) {
        MPI_Finalize();
        return 2;
    }
    char address_buffer[64] = {};
    if (rank == 0) {
        std::snprintf(address_buffer, sizeof(address_buffer), "%s", address.c_str());
    }
    MPI_Bcast(address_buffer, sizeof(address_buffer), MPI_CHAR, 0, MPI_COMM_WORLD);

    auto probe = Connect(rank, std::string(address_buffer));
    if (probe == nullptr || !probe->RegisterBuffer()) {
        MPI_Finalize();
        return 1;
    }

    uint64_t local_address = probe->LocalAddress();
    uint32_t local_rkey = probe->LocalRkey();
    uint64_t remote_address = 0;
    uint32_t remote_rkey = 0;
    MPI_Sendrecv(&local_address, 1, MPI_UINT64_T, 1 - rank, 1, &remote_address, 1, MPI_UINT64_T, 1 - rank, 1,
                 MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    MPI_Sendrecv(&local_rkey, 1, MPI_UINT32_T, 1 - rank, 2, &remote_rkey, 1, MPI_UINT32_T, 1 - rank, 2, MPI_COMM_WORLD,
                 MPI_STATUS_IGNORE);

    // Rank 1 is only the write target, so it stays parked in the barrier and keeps its queue
    // pair alive while rank 0 measures.
    if (rank != 0) {
        MPI_Barrier(MPI_COMM_WORLD);
        MPI_Finalize();
        return 0;
    }

    fmt::print("rdma write latency: post_send -> CQE, {} bytes, {} iterations ({} warmup)\n", kMessageSize, iterations,
               kWarmup);
    fmt::print("{:>8}  {:>8}  {:>8}  {:>8}  {:>8}\n", "min", "median", "mean", "p99", "max");
    std::vector<double> samples(iterations);
    for (uint64_t i = 0; i < kWarmup; ++i) {
        if (!probe->PostWrite(kMessageSize, remote_address, remote_rkey) || !probe->PollWriteCompletion()) {
            LOG_ERROR("rdma write latency rank {}: warmup failed", rank);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
    }
    for (uint64_t i = 0; i < iterations; ++i) {
        const auto before = Clock::now();
        if (!probe->PostWrite(kMessageSize, remote_address, remote_rkey) || !probe->PollWriteCompletion()) {
            LOG_ERROR("rdma write latency rank {}: transfer failed", rank);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        samples[i] = std::chrono::duration<double, std::micro>(Clock::now() - before).count();
    }
    std::sort(samples.begin(), samples.end());
    Report(samples);

    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Finalize();
    return 0;
}
