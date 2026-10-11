#include <pthread.h>
#include <sched.h>
#include <unistd.h>
#include <cstdlib>
#include <algorithm>
#include <numeric>
#include "executor/dynamic_multi_thread_executor.h"
#include "communicator.h"
#include "channel.h"
#include "logger.h"
#include "transport/transport.h"
#include "topology/topology.h"

namespace {

// In-band control marker carried on the ring Broadcast that a channel runs whenever it goes
// idle. The first bytes distinguish a control broadcast from a real collective Broadcast.
constexpr uint64_t kControlMagic = 0x4F43434C434D4431ULL;
constexpr uint32_t kControlTypeBatch = 1;
constexpr uint32_t kControlTypeEnd = 2;
constexpr size_t kDefaultBatchSize = 2;

struct ControlMessage {
    uint64_t magic = 0;
    uint32_t type = 0;
    uint32_t count = 0;
    uint64_t start = 0;
};
static_assert(sizeof(ControlMessage) % sizeof(int64_t) == 0, "the control message must align to an int64");

void PinThreadToCpu(int cpu) {
    const long cpu_count = sysconf(_SC_NPROCESSORS_ONLN);
    if (cpu_count <= 0) {
        LOG_WARN("Failed to get the online CPU count, skip worker pinning");
        return;
    }
    const int target = cpu % static_cast<int>(cpu_count);
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(target, &set);
    if (pthread_setaffinity_np(pthread_self(), sizeof(set), &set) != 0) {
        LOG_WARN("Failed to pin worker to CPU {}", target);
    }
}

void CpuPause() {
#if defined(__aarch64__)
    __asm__ __volatile__("yield");
#elif defined(__x86_64__) || defined(__i386__)
    __asm__ __volatile__("pause");
#endif
}

size_t BatchSize() {
    const char* value = std::getenv("OCCL_DYNAMIC_BATCH");
    if (value != nullptr && *value != '\0') {
        const long parsed = std::strtol(value, nullptr, 10);
        if (parsed > 0 && parsed <= 1024) {
            return static_cast<size_t>(parsed);
        }
    }
    return kDefaultBatchSize;
}

void SetChannelsPolling(Communicator& comm) {
    for (int c = 0; c < comm.GetNChannels(); ++c) {
        Channel& channel = comm.GetChannel(c);
        for (auto& connector : channel.send) {
            if (connector.transport) {
                connector.transport->SetWaitMode(TransportWaitMode::Polling);
            }
        }
        for (auto& connector : channel.recv) {
            if (connector.transport) {
                connector.transport->SetWaitMode(TransportWaitMode::Polling);
            }
        }
    }
}
} // namespace

DynamicMultiThreadExecutor::DynamicMultiThreadExecutor(Communicator& comm, int base_cpu)
    : comm_(comm), base_cpu_(base_cpu) {}

DynamicMultiThreadExecutor::~DynamicMultiThreadExecutor() {
    StopWorkers();
}

void DynamicMultiThreadExecutor::Shutdown() {
    StopWorkers();
}

void DynamicMultiThreadExecutor::EnsureWorkers() {
    uint64_t completed_batch_id = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        completed_batch_id = batch_id_;
    }

    const size_t channel_count = static_cast<size_t>(comm_.GetNChannels());
    workers_.reserve(channel_count);
    while (workers_.size() < channel_count) {
        const int channel_id = static_cast<int>(workers_.size());
        workers_.emplace_back(&DynamicMultiThreadExecutor::WorkerLoop, this, channel_id, completed_batch_id);
    }
}

void DynamicMultiThreadExecutor::StopWorkers() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stop_ = true;
    }
    work_ready_.notify_all();

    for (std::thread& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    workers_.clear();
    stop_ = false;
}

bool DynamicMultiThreadExecutor::ExecuteTask(int channel_id, PlanTask& task) {
    if (!task.topology) {
        LOG_ERROR("Dynamic executor received invalid task on channel {}", channel_id);
        return false;
    }

    task.channel_id = channel_id;
    Channel& channel = comm_.GetChannel(channel_id);
    task.topology->FillTransports(channel, task);

    Topology* topo = task.topology.get();
    if (!topo->CollectiveInit(task)) {
        LOG_ERROR("Dynamic executor failed to init task on channel {}", channel_id);
        return false;
    }
    if (topo->CollectiveDone(task)) {
        return true;
    }

    while (!topo->CollectiveDone(task)) {
        const size_t before_send =
            std::accumulate(task.state.send_progress.begin(), task.state.send_progress.end(), size_t{0});
        const size_t before_recv =
            std::accumulate(task.state.recv_progress.begin(), task.state.recv_progress.end(), size_t{0});
        const int before_phase = task.state.phase;

        const bool send_done =
            std::all_of(task.state.send_done.begin(), task.state.send_done.end(), [](char d) { return d != 0; });
        const bool recv_done =
            std::all_of(task.state.recv_done.begin(), task.state.recv_done.end(), [](char d) { return d != 0; });

        if (!send_done && !topo->CollectiveStep(task, CollEvent::Writable)) {
            LOG_ERROR("Dynamic executor step failed on channel {}", channel_id);
            return false;
        }
        if (!recv_done && !topo->CollectiveStep(task, CollEvent::Readable)) {
            LOG_ERROR("Dynamic executor step failed on channel {}", channel_id);
            return false;
        }

        const size_t after_send =
            std::accumulate(task.state.send_progress.begin(), task.state.send_progress.end(), size_t{0});
        const size_t after_recv =
            std::accumulate(task.state.recv_progress.begin(), task.state.recv_progress.end(), size_t{0});
        if (after_send == before_send && after_recv == before_recv && task.state.phase == before_phase) {
            CpuPause();
        }
    }

    return true;
}

bool DynamicMultiThreadExecutor::ProcessChannel(int channel_id, const CollPlan& plan) {
    while (true) {
        ControlMessage message;
        message.magic = kControlMagic;
        if (comm_.GetRank() == 0) {
            size_t start = 0;
            size_t count = 0;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                start = next_slice_;
                count = std::min(plan.slices.size() - next_slice_, BatchSize());
                next_slice_ += count;
            }
            if (count == 0) {
                message.type = kControlTypeEnd;
            } else {
                message.type = kControlTypeBatch;
                message.start = start;
                message.count = static_cast<uint32_t>(count);
            }
        }

        PlanTask control;
        control.func = CollFunc::Broadcast;
        control.recv_buf = &message;
        control.elem_count = sizeof(ControlMessage) / sizeof(int64_t);
        control.dtype = DataType::INT64;
        control.root = 0;
        control.rank = comm_.GetRank();
        control.world_size = comm_.GetWorldSize();
        control.channel_id = channel_id;
        control.topology = comm_.GetRingTopology();
        if (!ExecuteTask(channel_id, control)) {
            LOG_ERROR("Rank {}: dynamic control broadcast failed on channel {}", comm_.GetRank(), channel_id);
            return false;
        }

        if (message.type == kControlTypeEnd) {
            return true;
        }

        for (uint32_t i = 0; i < message.count; ++i) {
            const size_t index = message.start + i;
            if (index >= plan.slices.size()) {
                LOG_ERROR("Rank {}: dynamic slice index {} out of range (slices={}, type={}, start={}, count={})",
                          comm_.GetRank(), index, plan.slices.size(), message.type, message.start, message.count);
                return false;
            }
            PlanTask task = plan.slices[index];
            if (!ExecuteTask(channel_id, task)) {
                return false;
            }
        }
    }
}

void DynamicMultiThreadExecutor::WorkerLoop(int channel_id, uint64_t completed_batch_id) {
    PinThreadToCpu(base_cpu_ + channel_id);
    while (true) {
        const CollPlan* plan = nullptr;
        uint64_t batch_id = 0;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            work_ready_.wait(lock, [this, completed_batch_id]() { return stop_ || batch_id_ != completed_batch_id; });
            if (stop_) {
                return;
            }
            plan = plan_;
            batch_id = batch_id_;
        }

        bool success = ProcessChannel(channel_id, *plan);

        {
            std::lock_guard<std::mutex> lock(mutex_);
            batch_success_ = batch_success_ && success;
            ++completed_channels_;
            completed_batch_id = batch_id;
            if (completed_channels_ == active_channels_) {
                work_done_.notify_one();
            }
        }
    }
}

bool DynamicMultiThreadExecutor::Run(const CollPlan& plan) {
    EnsureWorkers();

    if (!plan.slices.empty()) {
        SetChannelsPolling(comm_);
        std::unique_lock<std::mutex> lock(mutex_);
        plan_ = &plan;
        next_slice_ = 0;
        active_channels_ = workers_.size();
        completed_channels_ = 0;
        batch_success_ = true;
        ++batch_id_;
        work_ready_.notify_all();
        work_done_.wait(lock, [this]() { return completed_channels_ == active_channels_; });
        if (!batch_success_) {
            return false;
        }
    }

    return true;
}
