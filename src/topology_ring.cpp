#include <algorithm>
#include <cstring>
#include <thread>
#include <vector>
#include "topology_ring.h"
#include "transport.h"
#include "utils.h"
#include "logger.h"

namespace {
bool Exchange(Transport* send_transport, Transport* recv_transport, const void* send_buf, void* recv_buf,
              size_t send_bytes, size_t recv_bytes) {
    bool send_ok = false;
    std::thread send_thread(
        [send_transport, send_buf, send_bytes, &send_ok]() { send_ok = send_transport->Send(send_buf, send_bytes); });
    bool recv_ok = recv_transport->Recv(recv_buf, recv_bytes);
    send_thread.join();
    return send_ok && recv_ok;
}

size_t ChunkElemCount(size_t count, size_t chunk_size, int chunk_index) {
    size_t start = static_cast<size_t>(chunk_index) * chunk_size;
    return start >= count ? 0 : std::min(chunk_size, count - start);
}
}

void TopologyRing::FillChannels(std::vector<Channel>& channels) const {
    for (size_t i = 0; i < channels.size(); ++i) {
        channels[i].id = static_cast<int>(i);
        channels[i].ring.prev = GetPrevRank(rank);
        channels[i].ring.next = GetNextRank(rank);

        channels[i].send.resize(static_cast<size_t>(world_size));
        channels[i].recv.resize(static_cast<size_t>(world_size));
        for (int peer = 0; peer < world_size; ++peer) {
            channels[i].send[static_cast<size_t>(peer)].peer = peer;
            channels[i].send[static_cast<size_t>(peer)].channel_id = channels[i].id;
            channels[i].send[static_cast<size_t>(peer)].is_send = true;
            channels[i].recv[static_cast<size_t>(peer)].peer = peer;
            channels[i].recv[static_cast<size_t>(peer)].channel_id = channels[i].id;
            channels[i].recv[static_cast<size_t>(peer)].is_send = false;
        }
    }
}

int TopologyRing::GetPrevRank(int r) const {
    return (r - 1 + world_size) % world_size;
}

int TopologyRing::GetNextRank(int r) const {
    return (r + 1) % world_size;
}

bool TopologyRing::AllReduce(const PlanTask& task) const {
    if (!task.send_buf || !task.recv_buf || task.world_size <= 0) {
        LOG_ERROR("Ring AllReduce received invalid task");
        return false;
    }

    size_t type_size = Utils::GetDataTypeSize(task.dtype);
    size_t total_bytes = task.elem_count * type_size;
    if (task.send_buf != task.recv_buf) {
        std::memcpy(task.recv_buf, task.send_buf, total_bytes);
    }

    if (task.world_size == 1) {
        if (task.reduce_op == ReduceOp::AVG) {
            Utils::ApplyAverage(task.recv_buf, task.elem_count, task.dtype, task.world_size);
        }
        return true;
    }
    if (!task.send_transport || !task.recv_transport) {
        LOG_ERROR("Ring AllReduce received task without transports");
        return false;
    }

    size_t chunk_size =
        (task.elem_count + static_cast<size_t>(task.world_size) - 1) / static_cast<size_t>(task.world_size);
    size_t chunk_bytes = chunk_size * type_size;
    std::vector<char> temp_buffer(chunk_bytes);
    char* data = static_cast<char*>(task.recv_buf);

    for (int step = 0; step < task.world_size - 1; ++step) {
        int send_chunk = (task.rank - step + task.world_size) % task.world_size;
        int recv_chunk = (task.rank - step + task.world_size - 1) % task.world_size;
        size_t send_count = ChunkElemCount(task.elem_count, chunk_size, send_chunk);
        size_t recv_count = ChunkElemCount(task.elem_count, chunk_size, recv_chunk);
        if (!Exchange(task.send_transport.get(), task.recv_transport.get(),
                      data + static_cast<size_t>(send_chunk) * chunk_bytes, temp_buffer.data(), send_count * type_size,
                      recv_count * type_size)) {
            LOG_ERROR("Ring ReduceScatter failed at step {}", step);
            return false;
        }
        Utils::PerformReduce(temp_buffer.data(), data + static_cast<size_t>(recv_chunk) * chunk_bytes, recv_count,
                             task.dtype, task.reduce_op == ReduceOp::AVG ? ReduceOp::SUM : task.reduce_op);
    }

    for (int step = 0; step < task.world_size - 1; ++step) {
        int send_chunk = (task.rank - step + 1 + task.world_size) % task.world_size;
        int recv_chunk = (task.rank - step + task.world_size) % task.world_size;
        size_t send_count = ChunkElemCount(task.elem_count, chunk_size, send_chunk);
        size_t recv_count = ChunkElemCount(task.elem_count, chunk_size, recv_chunk);
        if (!Exchange(task.send_transport.get(), task.recv_transport.get(),
                      data + static_cast<size_t>(send_chunk) * chunk_bytes,
                      data + static_cast<size_t>(recv_chunk) * chunk_bytes, send_count * type_size,
                      recv_count * type_size)) {
            LOG_ERROR("Ring AllGather failed at step {}", step);
            return false;
        }
    }

    if (task.reduce_op == ReduceOp::AVG) {
        Utils::ApplyAverage(task.recv_buf, task.elem_count, task.dtype, task.world_size);
    }
    return true;
}
