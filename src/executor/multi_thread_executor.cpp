#include <pthread.h>
#include <sched.h>
#include <unistd.h>
#include <algorithm>
#include <memory>
#include <numeric>
#include <vector>
#include "executor/multi_thread_executor.h"
#include "transport/transport.h"
#include "topology/topology.h"
#include "logger.h"

namespace {
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

void SetPlanWaitMode(const CollPlan& plan) {
    for (const ChannelPlan& channel : plan.channels) {
        for (const PlanTask& task : channel.tasks) {
            for (const auto& transport : task.send_transports) {
                if (transport) {
                    transport->SetWaitMode(TransportWaitMode::Polling);
                }
            }
            for (const auto& transport : task.recv_transports) {
                if (transport) {
                    transport->SetWaitMode(TransportWaitMode::Polling);
                }
            }
        }
    }
}
} // namespace

MultiThreadExecutor::~MultiThreadExecutor() {
    StopWorkers();
}

void MultiThreadExecutor::Shutdown() {
    StopWorkers();
}

void MultiThreadExecutor::EnsureWorkers(size_t channel_count) {
    uint64_t completed_batch_id = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        completed_batch_id = batch_id_;
    }

    workers_.reserve(channel_count);
    while (workers_.size() < channel_count) {
        size_t channel_id = workers_.size();
        workers_.emplace_back(&MultiThreadExecutor::WorkerLoop, this, channel_id, completed_batch_id);
    }
}

void MultiThreadExecutor::StopWorkers() {
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

bool MultiThreadExecutor::ExecuteTask(int channel_id, PlanTask& task) {
    if (!task.topology) {
        LOG_ERROR("Executor received invalid task on channel {}", channel_id);
        return false;
    }

    Topology* topo = task.topology.get();
    if (!topo->CollectiveInit(task)) {
        LOG_ERROR("MultiThreadExecutor failed to init task on channel {}", channel_id);
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
            LOG_ERROR("MultiThreadExecutor step failed on channel {}", channel_id);
            return false;
        }
        if (!recv_done && !topo->CollectiveStep(task, CollEvent::Readable)) {
            LOG_ERROR("MultiThreadExecutor step failed on channel {}", channel_id);
            return false;
        }

        const size_t after_send =
            std::accumulate(task.state.send_progress.begin(), task.state.send_progress.end(), size_t{0});
        const size_t after_recv =
            std::accumulate(task.state.recv_progress.begin(), task.state.recv_progress.end(), size_t{0});
        if (after_send == before_send && after_recv == before_recv && task.state.phase == before_phase) {
            sched_yield();
        }
    }

    return true;
}

void MultiThreadExecutor::WorkerLoop(size_t channel_id, uint64_t completed_batch_id) {
    PinThreadToCpu(base_cpu_ + static_cast<int>(channel_id));
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

        bool success = true;
        if (channel_id < plan->channels.size()) {
            const ChannelPlan& channel = plan->channels[channel_id];
            for (const PlanTask& task : channel.tasks) {
                if (!ExecuteTask(channel.channel_id, const_cast<PlanTask&>(task))) {
                    success = false;
                    break;
                }
            }
        }

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

bool MultiThreadExecutor::Run(const CollPlan& plan) {
    EnsureWorkers(plan.channels.size());

    if (!plan.channels.empty()) {
        SetPlanWaitMode(plan);
        std::unique_lock<std::mutex> lock(mutex_);
        plan_ = &plan;
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
