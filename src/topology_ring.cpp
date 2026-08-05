#include <vector>
#include <algorithm>
#include <thread>
#include <cstring>
#include "topology_ring.h"
#include "logger.h"
#include "utils.h"
#include "transport.h"
#include "communicator.h"

std::vector<int> TopologyRing::GetNeighbors(int r) const {
    std::vector<int> neighbors;
    int prev = GetPrevRank(r);
    int next = GetNextRank(r);

    neighbors.push_back(prev);
    if (next != prev) {
        neighbors.push_back(next);
    }

    return neighbors;
}

bool TopologyRing::ShouldConnect(int my_rank, int peer_rank) const {
    int prev = GetPrevRank(my_rank);
    int next = GetNextRank(my_rank);

    if (peer_rank == next && my_rank < peer_rank) {
        return true;
    }
    if (peer_rank == prev && my_rank < peer_rank) {
        return true;
    }

    return false;
}

int TopologyRing::GetPrevRank(int r) const {
    return (r - 1 + world_size) % world_size;
}

int TopologyRing::GetNextRank(int r) const {
    return (r + 1) % world_size;
}

bool TopologyRing::GetRingTransports(Communicator& comm, RingTransports& rt, int world_size) {
    rt.prev_rank = (rank - 1 + world_size) % world_size;
    rt.next_rank = (rank + 1) % world_size;
    rt.prev = comm.GetTransport(rt.prev_rank);
    rt.next = comm.GetTransport(rt.next_rank);

    if (!rt.prev || !rt.next) {
        LOG_ERROR("Rank {}: Failed to get ring transports", rank);
        return false;
    }
    return true;
}

bool TopologyRing::AllReduceTwoRanks(Communicator& comm, const void* send_buf, void* recv_buf, size_t count,
                                     DataType dtype, ReduceOp op) {
    size_t type_size = Utils::GetDataTypeSize(dtype);
    size_t chunk_bytes = count * type_size;
    int peer = 1 - rank;

    auto transport = comm.GetTransport(peer);
    if (!transport) {
        LOG_ERROR("Rank {}: Failed to get transport to peer {} for AllReduceTwoRanks", rank, peer);
        return false;
    }

    memcpy(recv_buf, send_buf, chunk_bytes);

    std::vector<char> peer_data(chunk_bytes);

    if (rank == 0) {
        if (!transport->Send(recv_buf, chunk_bytes)) {
            LOG_ERROR("Rank {}: Failed to send data to peer {} for AllReduceTwoRanks", rank, peer);
            return false;
        }
        if (!transport->Recv(peer_data.data(), chunk_bytes)) {
            LOG_ERROR("Rank {}: Failed to recv data from peer {} for AllReduceTwoRanks", rank, peer);
            return false;
        }
    } else {
        if (!transport->Recv(peer_data.data(), chunk_bytes)) {
            LOG_ERROR("Rank {}: Failed to recv data from peer {} for AllReduceTwoRanks", rank, peer);
            return false;
        }
        if (!transport->Send(recv_buf, chunk_bytes)) {
            LOG_ERROR("Rank {}: Failed to send data to peer {} for AllReduceTwoRanks", rank, peer);
            return false;
        }
    }

    Utils::PerformReduce(peer_data.data(), recv_buf, count, dtype, op, 2);
    return true;
}

bool TopologyRing::AllReduceReduceScatter(void* recv_buf, size_t count, DataType dtype, ReduceOp op,
                                          RingTransports& rt) {
    size_t type_size = Utils::GetDataTypeSize(dtype);
    size_t chunk_size = (count + world_size - 1) / world_size;
    size_t chunk_bytes = chunk_size * type_size;

    std::vector<char> temp_buffer(chunk_bytes);
    char* data = static_cast<char*>(recv_buf);

    for (int step = 0; step < world_size - 1; ++step) {
        int send_chunk = (rank - step + world_size) % world_size;
        int recv_chunk = (rank - step + world_size - 1) % world_size;

        size_t send_offset = send_chunk * chunk_bytes;
        size_t recv_offset = recv_chunk * chunk_bytes;

        size_t actual_send_count = std::min(chunk_size, count - send_chunk * chunk_size);
        size_t actual_recv_count = std::min(chunk_size, count - recv_chunk * chunk_size);
        size_t actual_send_bytes = actual_send_count * type_size;
        size_t actual_recv_bytes = actual_recv_count * type_size;

        std::thread send_thread(
            [&rt, data, send_offset, actual_send_bytes]() { rt.next->Send(data + send_offset, actual_send_bytes); });

        if (!rt.prev->Recv(temp_buffer.data(), actual_recv_bytes)) {
            LOG_ERROR("Rank {}: Failed to recv data from prev for AllReduceReduceScatter in step {}", rank, step);
            send_thread.join();
            return false;
        }

        send_thread.join();

        Utils::PerformReduce(temp_buffer.data(), data + recv_offset, actual_recv_count, dtype, op, world_size);
    }

    return true;
}

bool TopologyRing::AllReduceAllGather(void* recv_buf, size_t count, DataType dtype, RingTransports& rt) {
    size_t type_size = Utils::GetDataTypeSize(dtype);
    size_t chunk_size = (count + world_size - 1) / world_size;
    size_t chunk_bytes = chunk_size * type_size;
    char* data = static_cast<char*>(recv_buf);

    for (int step = 0; step < world_size - 1; ++step) {
        int send_chunk = (rank - step + 1 + world_size) % world_size;
        int recv_chunk = (rank - step + world_size) % world_size;

        size_t send_offset = send_chunk * chunk_bytes;
        size_t recv_offset = recv_chunk * chunk_bytes;

        size_t actual_send_count = std::min(chunk_size, count - send_chunk * chunk_size);
        size_t actual_recv_count = std::min(chunk_size, count - recv_chunk * chunk_size);
        size_t actual_send_bytes = actual_send_count * type_size;
        size_t actual_recv_bytes = actual_recv_count * type_size;

        std::thread send_thread(
            [&rt, data, send_offset, actual_send_bytes]() { rt.next->Send(data + send_offset, actual_send_bytes); });

        if (!rt.prev->Recv(data + recv_offset, actual_recv_bytes)) {
            LOG_ERROR("Rank {}: Failed to recv data from prev for RingAllReduceAllGather in step {}", rank, step);
            send_thread.join();
            return false;
        }

        send_thread.join();
    }

    return true;
}

bool TopologyRing::AllReduce(Communicator& comm, const void* send_buf, void* recv_buf, size_t count, DataType dtype,
                             ReduceOp op) {

    if (world_size == 2) {
        return AllReduceTwoRanks(comm, send_buf, recv_buf, count, dtype, op);
    }

    size_t type_size = Utils::GetDataTypeSize(dtype);
    size_t chunk_bytes = count * type_size;

    RingTransports rt;
    if (!GetRingTransports(comm, rt, world_size)) {
        LOG_ERROR("Rank {}: Failed to get ring transports for AllReduce", rank);
        return false;
    }

    memcpy(recv_buf, send_buf, chunk_bytes);

    if (!AllReduceReduceScatter(recv_buf, count, dtype, op, rt)) {
        LOG_ERROR("Rank {}: Failed to AllReduceReduceScatter for AllReduce", rank);
        return false;
    }

    if (!AllReduceAllGather(recv_buf, count, dtype, rt)) {
        LOG_ERROR("Rank {}: Failed to AllReduceAllGather for AllReduce", rank);
        return false;
    }

    return true;
}
