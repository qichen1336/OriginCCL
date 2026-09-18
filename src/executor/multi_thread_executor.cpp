#include <poll.h>
#include "executor/multi_thread_executor.h"
#include "topology.h"
#include "logger.h"

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
    if (!task.topology || !task.send_buf || !task.recv_buf || task.world_size <= 0) {
        LOG_ERROR("Executor received invalid task on channel {}", channel_id);
        return false;
    }
    if (task.func != CollFunc::AllReduce) {
        LOG_ERROR("Executor received unsupported collective on channel {}", channel_id);
        return false;
    }

    Topology* topo = task.topology.get();
    if (!topo->AllreduceInit(task)) {
        LOG_ERROR("MultiThreadExecutor failed to init task on channel {}", channel_id);
        return false;
    }

    while (!topo->AllreduceDone(task)) {
        struct pollfd fds[2];
        CollEvent ops[2];
        nfds_t nfds = 0;

        if (task.recv_transport) {
            int fd = task.recv_transport->GetFd();
            uint32_t events = task.recv_transport->GetPollEvents();
            if (fd >= 0 && events != 0) {
                fds[nfds] = {fd, static_cast<short>(events), 0};
                ops[nfds] = CollEvent::Readable;
                ++nfds;
            }
        }
        if (task.send_transport) {
            int fd = task.send_transport->GetFd();
            uint32_t events = task.send_transport->GetPollEvents();
            if (fd >= 0 && events != 0) {
                fds[nfds] = {fd, static_cast<short>(events), 0};
                ops[nfds] = CollEvent::Writable;
                ++nfds;
            }
        }

        if (nfds == 0) {
            if (!topo->AllreduceStep(task, CollEvent::Readable)) {
                LOG_ERROR("MultiThreadExecutor step failed on channel {}", channel_id);
                return false;
            }
            continue;
        }

        int ready = poll(fds, nfds, -1);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            LOG_ERROR("Executor poll failed on channel {}", channel_id);
            return false;
        }

        for (nfds_t i = 0; i < nfds; ++i) {
            if ((fds[i].revents & fds[i].events) == 0) {
                continue;
            }
            if (!topo->AllreduceStep(task, ops[i])) {
                LOG_ERROR("MultiThreadExecutor step failed on channel {}", channel_id);
                return false;
            }
        }
    }
    return true;
}

void MultiThreadExecutor::WorkerLoop(size_t channel_id, uint64_t completed_batch_id) {
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
