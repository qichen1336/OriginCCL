#include <algorithm>
#include <cstring>
#include <thread>
#include <vector>
#include "ring_executor.h"
#include "communicator.h"
#include "channel.h"
#include "logger.h"
#include "utils.h"
#include "transport.h"

namespace {
bool ExchangeChunk(Transport* send_transport, Transport* recv_transport, const void* send_ptr, void* recv_ptr,
                   size_t send_bytes, size_t recv_bytes) {
    std::thread send_thread([send_transport, send_ptr, send_bytes]() {
        send_transport->Send(send_ptr, send_bytes);
    });

    bool recv_ok = recv_transport->Recv(recv_ptr, recv_bytes);
    send_thread.join();
    return recv_ok;
}

size_t ChunkElemCount(size_t count, size_t chunk_size, int chunk_index) {
    size_t start = static_cast<size_t>(chunk_index) * chunk_size;
    if (start >= count) {
        return 0;
    }
    return std::min(chunk_size, count - start);
}

bool ReduceScatter(void* recv_buf, size_t count, DataType dtype, ReduceOp op, int rank, int world_size,
                   Transport* send_transport, Transport* recv_transport) {
    size_t type_size = Utils::GetDataTypeSize(dtype);
    size_t chunk_size = (count + static_cast<size_t>(world_size) - 1) / static_cast<size_t>(world_size);
    size_t chunk_bytes = chunk_size * type_size;
    std::vector<char> temp_buffer(chunk_bytes);
    char* data = static_cast<char*>(recv_buf);

    for (int step = 0; step < world_size - 1; ++step) {
        int send_chunk = (rank - step + world_size) % world_size;
        int recv_chunk = (rank - step + world_size - 1) % world_size;

        size_t actual_send_count = ChunkElemCount(count, chunk_size, send_chunk);
        size_t actual_recv_count = ChunkElemCount(count, chunk_size, recv_chunk);
        size_t actual_send_bytes = actual_send_count * type_size;
        size_t actual_recv_bytes = actual_recv_count * type_size;

        if (!ExchangeChunk(send_transport, recv_transport, data + static_cast<size_t>(send_chunk) * chunk_bytes,
                           temp_buffer.data(), actual_send_bytes, actual_recv_bytes)) {
            LOG_ERROR("Rank {}: Failed to recv data from prev for ReduceScatter in step {}", rank, step);
            return false;
        }

        ReduceOp step_op = (op == ReduceOp::AVG) ? ReduceOp::SUM : op;
        Utils::PerformReduce(temp_buffer.data(), data + static_cast<size_t>(recv_chunk) * chunk_bytes, actual_recv_count,
                             dtype, step_op);
    }

    return true;
}

bool AllGather(void* recv_buf, size_t count, DataType dtype, int rank, int world_size, Transport* send_transport,
               Transport* recv_transport) {
    size_t type_size = Utils::GetDataTypeSize(dtype);
    size_t chunk_size = (count + static_cast<size_t>(world_size) - 1) / static_cast<size_t>(world_size);
    size_t chunk_bytes = chunk_size * type_size;
    char* data = static_cast<char*>(recv_buf);

    for (int step = 0; step < world_size - 1; ++step) {
        int send_chunk = (rank - step + 1 + world_size) % world_size;
        int recv_chunk = (rank - step + world_size) % world_size;

        size_t actual_send_count = ChunkElemCount(count, chunk_size, send_chunk);
        size_t actual_recv_count = ChunkElemCount(count, chunk_size, recv_chunk);
        size_t actual_send_bytes = actual_send_count * type_size;
        size_t actual_recv_bytes = actual_recv_count * type_size;

        if (!ExchangeChunk(send_transport, recv_transport, data + static_cast<size_t>(send_chunk) * chunk_bytes,
                           data + static_cast<size_t>(recv_chunk) * chunk_bytes, actual_send_bytes,
                           actual_recv_bytes)) {
            LOG_ERROR("Rank {}: Failed to recv data from prev for AllGather in step {}", rank, step);
            return false;
        }
    }

    return true;
}
}  // namespace

bool RingExecutor::Run(Communicator& comm, const CollPlan& plan, const CollTask& task) {
    int rank = comm.GetRank();
    int world_size = comm.GetWorldSize();
    size_t type_size = Utils::GetDataTypeSize(task.dtype);
    size_t total_bytes = task.count * type_size;

    if (task.send_buf != task.recv_buf) {
        memcpy(task.recv_buf, task.send_buf, total_bytes);
    }

    if (world_size == 1) {
        if (task.op == ReduceOp::AVG) {
            Utils::ApplyAverage(task.recv_buf, task.count, task.dtype, world_size);
        }
        return true;
    }

    char* recv_bytes = static_cast<char*>(task.recv_buf);

    for (const ChannelWork& work : plan.works) {
        if (work.elem_count == 0) {
            continue;
        }

        Channel& channel = comm.GetChannel(work.channel_id);
        Connector* send_conn = channel.SendConnector(channel.ring.next);
        Connector* recv_conn = channel.RecvConnector(channel.ring.prev);
        if (!send_conn || !send_conn->transport || !recv_conn || !recv_conn->transport) {
            LOG_ERROR("Rank {}: Missing ring connectors on channel {}", rank, work.channel_id);
            return false;
        }

        void* slice = recv_bytes + work.elem_offset * type_size;
        if (!ReduceScatter(slice, work.elem_count, task.dtype, task.op, rank, world_size, send_conn->transport.get(),
                           recv_conn->transport.get())) {
            LOG_ERROR("Rank {}: ReduceScatter failed on channel {}", rank, work.channel_id);
            return false;
        }
        if (!AllGather(slice, work.elem_count, task.dtype, rank, world_size, send_conn->transport.get(),
                       recv_conn->transport.get())) {
            LOG_ERROR("Rank {}: AllGather failed on channel {}", rank, work.channel_id);
            return false;
        }
    }

    if (task.op == ReduceOp::AVG) {
        Utils::ApplyAverage(task.recv_buf, task.count, task.dtype, world_size);
    }
    return true;
}
