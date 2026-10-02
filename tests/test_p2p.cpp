#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <mpi.h>
#include "communicator.h"
#include "executor/epoll_executor.h"
#include "executor/polling_executor.h"
#include "logger.h"
#include "test_common.h"
#include "transport/transport_rdma_zc.h"
#include "transport/transport_shm.h"
#include "transport/transport_tcp.h"

namespace {
void Require(bool success, const char* message) {
    if (!success) {
        LOG_ERROR("P2P test failed: {}", message);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
}

std::string LogicalHostname() {
    int rank = 0;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    return "p2p-machine-" + std::to_string(rank);
}

bool Disabled(const char* name) {
    const char* value = std::getenv(name);
    return value && std::strcmp(value, "1") == 0;
}

void CheckUnusedChannels(Communicator& comm, int first_channel) {
    for (int channel_id = first_channel; channel_id < comm.GetNChannels(); ++channel_id) {
        const auto& channel = comm.GetChannel(channel_id);
        for (int peer = 0; peer < comm.GetWorldSize(); ++peer) {
            Require(!channel.send_p2p[peer].transport && !channel.recv_p2p[peer].transport,
                    "unexpected P2P connection");
        }
    }
}

void CheckContract(Communicator& comm) {
    const int rank = comm.GetRank();
    const int peer = rank ^ 1;
    int value = rank;
    CheckUnusedChannels(comm, 0);
    Require(comm.Send(nullptr, 0, DataType::INT32, peer), "zero Send");
    Require(comm.Recv(nullptr, 0, DataType::INT32, peer), "zero Recv");
    for (int invalid : {-1, rank, comm.GetWorldSize()}) {
        Require(!comm.Send(&value, 1, DataType::INT32, invalid), "invalid Send peer accepted");
        Require(!comm.Recv(&value, 1, DataType::INT32, invalid), "invalid Recv peer accepted");
        Require(!comm.Send(nullptr, 0, DataType::INT32, invalid), "zero Send invalid peer accepted");
        Require(!comm.Recv(nullptr, 0, DataType::INT32, invalid), "zero Recv invalid peer accepted");
    }
    Require(!comm.Send(nullptr, 1, DataType::INT32, peer), "null Send accepted");
    Require(!comm.Recv(nullptr, 1, DataType::INT32, peer), "null Recv accepted");
    CheckUnusedChannels(comm, 0);
}

void CheckOutOfOrder(Communicator& comm) {
    int value = comm.GetRank();
    if (comm.GetRank() == 0) {
        Require(comm.Recv(&value, 1, DataType::INT32, 1) && value == 1, "target source receive");
        Require(comm.GetChannel(0).recv_p2p[2].transport != nullptr, "non-target source was not cached");
        Require(comm.Recv(&value, 1, DataType::INT32, 2) && value == 2, "cached source receive");
    } else if (comm.GetRank() == 1) {
        MPI_Recv(&value, 1, MPI_INT, 2, 7, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        value = 1;
        Require(comm.Send(&value, 1, DataType::INT32, 0), "target source send");
    } else if (comm.GetRank() == 2) {
        Require(comm.Send(&value, 1, DataType::INT32, 0), "early source send");
        MPI_Send(&value, 1, MPI_INT, 1, 7, MPI_COMM_WORLD);
    }
    MPI_Barrier(MPI_COMM_WORLD);
}

template<typename Value>
void Exchange(Communicator& comm, size_t count, DataType dtype, Executor* executor = nullptr) {
    const int rank = comm.GetRank();
    const int peer = rank ^ 1;
    const Value sentinel = static_cast<Value>(-99);
    std::vector<Value> send(count + 2, sentinel);
    std::vector<Value> recv(count + 2, sentinel);
    for (size_t index = 0; index < count; ++index) {
        send[index + 1] = static_cast<Value>(rank * 1000 + index % 997);
    }
    const auto old_send = comm.GetChannel(0).send_p2p[peer].transport;
    const auto old_recv = comm.GetChannel(0).recv_p2p[peer].transport;
    auto transfer = [&](bool is_send) {
        bool success;
        if (executor) {
            CollTask task;
            task.func = is_send ? CollFunc::Send : CollFunc::Recv;
            task.send_buf = is_send ? send.data() + 1 : nullptr;
            task.recv_buf = is_send ? nullptr : recv.data() + 1;
            task.count = count;
            task.dtype = dtype;
            task.peer = peer;
            CollPlan plan;
            Require(Planner().Plan(comm, task, plan), "P2P planning");
            Require(plan.channels.size() == 1 && plan.channels[0].channel_id == 0 && plan.channels[0].tasks.size() == 1,
                    "P2P plan must use channel 0 only");
            const auto& planned = plan.channels[0].tasks[0];
            Require(planned.elem_count == count && planned.chunk_size == 0 &&
                        std::strcmp(planned.topology->GetName(), "P2P") == 0 &&
                        planned.send_transports.size() == (is_send ? 1u : 0u) &&
                        planned.recv_transports.size() == (is_send ? 0u : 1u),
                    "P2P plan must keep the entire payload on one direction");
            success = executor->Run(plan);
        } else {
            success = is_send ? comm.Send(send.data() + 1, count, dtype, peer)
                              : comm.Recv(recv.data() + 1, count, dtype, peer);
        }
        Require(success, "P2P exchange");
        if (is_send) {
            std::fill(send.begin() + 1, send.end() - 1, sentinel);
        }
    };
    transfer(rank % 2 == 0);
    transfer(rank % 2 != 0);
    for (size_t index = 0; index < count; ++index) {
        Require(recv[index + 1] == static_cast<Value>(peer * 1000 + index % 997), "P2P payload mismatch");
    }
    Require(send.front() == sentinel && send.back() == sentinel && recv.front() == sentinel && recv.back() == sentinel,
            "P2P buffer boundary overwritten");
    const auto& channel = comm.GetChannel(0);
    Require(!old_send || old_send == channel.send_p2p[peer].transport, "send connection not reused");
    Require(!old_recv || old_recv == channel.recv_p2p[peer].transport, "recv connection not reused");
    Require(channel.send_p2p[peer].transport != channel.recv_p2p[peer].transport, "directions share a connection");
    Require(channel.send_p2p[peer].transport != channel.send[peer].transport &&
                channel.recv_p2p[peer].transport != channel.recv[peer].transport,
            "P2P shares a collective connection");
    CheckUnusedChannels(comm, 1);
    MPI_Barrier(MPI_COMM_WORLD);
}

void CheckCollective(Communicator& comm) {
    int send = comm.GetRank() + 1;
    int recv = 0;
    Require(comm.AllReduce(&send, &recv, 1, DataType::INT32, ReduceOp::SUM), "interleaved AllReduce");
    Require(recv == comm.GetWorldSize() * (comm.GetWorldSize() + 1) / 2, "interleaved AllReduce result");
}
}

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);
    int rank = 0;
    int world_size = 0;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    Require(world_size == 4, "P2P suite requires four ranks");
    bool cross_machine = false;
    for (int index = 1; index < argc; ++index) {
        cross_machine |= std::strcmp(argv[index], "--cross-machine") == 0;
    }
    std::string addr;
    int rdma_available = !Disabled("OCCL_DISABLE_RDMA") && TransportRDMA::Probe(addr);
    MPI_Allreduce(MPI_IN_PLACE, &rdma_available, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);

    Communicator comm;
    CommConfig config;
    config.rank = rank;
    config.world_size = world_size;
    config.n_channels = 4;
    if (cross_machine) {
        config.get_hostname = LogicalHostname;
    }
    Require(TestCommon::InitCommunicator(comm, config, rank, world_size), "communicator init");
    CheckContract(comm);
    if (cross_machine && !rdma_available) {
        int value = rank;
        Require(!comm.Send(&value, 1, DataType::INT32, rank ^ 1), "cross-machine Send fell back without RDMA");
        Require(!comm.Recv(&value, 1, DataType::INT32, rank ^ 1), "cross-machine Recv fell back without RDMA");
        CheckUnusedChannels(comm, 0);
        CheckCollective(comm);
        LOG_INFO("P2P cross-machine rejection PASS; RDMA_ZC transfers SKIP: no usable RDMA device");
        comm.Finalize();
        MPI_Finalize();
        return 2;
    }

    CheckOutOfOrder(comm);
    Exchange<float>(comm, 257, DataType::FLOAT32);
    Exchange<double>(comm, 513, DataType::FLOAT64);
    Exchange<int64_t>(comm, 129, DataType::INT64);
    PollingExecutor polling;
    EpollExecutor epoll;
    for (Executor* executor : {static_cast<Executor*>(&polling), static_cast<Executor*>(&epoll)}) {
        for (size_t bytes : {size_t{4}, size_t{5 * 1024 * 1024}, kRdmaZcThreshold - 4, kRdmaZcThreshold,
                             kRdmaZcThreshold + 4, 3 * kRdmaZcThreshold, size_t{1028}}) {
            Exchange<int32_t>(comm, bytes / sizeof(int32_t), DataType::INT32, executor);
        }
        CheckCollective(comm);
        Exchange<int32_t>(comm, 257, DataType::INT32);
    }
    const auto& channel = comm.GetChannel(0);
    for (const auto& transport : {channel.send_p2p[rank ^ 1].transport, channel.recv_p2p[rank ^ 1].transport}) {
        if (cross_machine || (Disabled("OCCL_DISABLE_SHM") && rdma_available)) {
            Require(dynamic_cast<TransportRDMAZc*>(transport.get()) != nullptr, "expected RDMA_ZC");
        } else if (!Disabled("OCCL_DISABLE_SHM")) {
            Require(dynamic_cast<TransportShm*>(transport.get()) != nullptr, "expected SHM");
        } else {
            Require(dynamic_cast<TransportTCP*>(transport.get()) != nullptr, "expected TCP");
        }
    }
    const auto connection = channel.send_p2p[rank ^ 1].transport;
    MPI_Barrier(MPI_COMM_WORLD);
    comm.Finalize();
    Require(!connection->IsConnected(), "Finalize did not close cached P2P connection");
    LOG_INFO("P2P {} PASS on rank {}", cross_machine ? "RDMA_ZC" : "local", rank);
    MPI_Finalize();
    return 0;
}