#include <exception>
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

bool MultiThreadExecutor::EnsureWorkers(size_t channel_count) {
    uint64_t completed_batch_id = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        completed_batch_id = batch_id_;
    }

    try {
        workers_.reserve(channel_count);
        while (workers_.size() < channel_count) {
            size_t channel_id = workers_.size();
            workers_.emplace_back(&MultiThreadExecutor::WorkerLoop, this, channel_id, completed_batch_id);
        }
    } catch (const std::system_error& error) {
        LOG_ERROR("Failed to create executor worker: {}", error.what());
        StopWorkers();
        return false;
    }
    return true;
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
        return false;
    }

    // Poll both the read and the write fd of this channel and feed each readiness event to
    // AllreduceStep. Watching both means a blocked send never starves the matching recv,
    // so no helper send thread is needed even when prev == next (2-rank degenerate).
    int read_fd = task.recv_transport ? task.recv_transport->GetFd() : -1;
    int write_fd = task.send_transport ? task.send_transport->GetFd() : -1;
    while (!topo->AllreduceDone(task)) {
        struct pollfd fds[2];
        nfds_t nfds = 0;
        if (read_fd >= 0) {
            fds[nfds].fd = read_fd;
            fds[nfds].events = POLLIN;
            fds[nfds].revents = 0;
            ++nfds;
        }
        if (write_fd >= 0 && write_fd != read_fd) {
            fds[nfds].fd = write_fd;
            fds[nfds].events = POLLOUT;
            fds[nfds].revents = 0;
            ++nfds;
        }

        if (nfds == 0) {
            if (!topo->AllreduceStep(task, CollEvent::Readable)) {
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

        // Feed every readiness event that fired. Within one step the send and the recv
        // transfer proceed concurrently, so driving only one of them starves the other and
        // deadlocks the 2-rank degenerate ring (prev == next).
        for (nfds_t i = 0; i < nfds; ++i) {
            if ((fds[i].revents & POLLOUT) != 0 && !topo->AllreduceStep(task, CollEvent::Writable)) {
                return false;
            }
            if ((fds[i].revents & POLLIN) != 0 && !topo->AllreduceStep(task, CollEvent::Readable)) {
                return false;
            }
        }
    }
    return topo->AllreduceSucceeded(task);
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
                try {
                    // This worker exclusively owns its channel's task cursor for the batch.
                    if (!ExecuteTask(channel.channel_id, const_cast<PlanTask&>(task))) {
                        success = false;
                        break;
                    }
                } catch (const std::exception& error) {
                    LOG_ERROR("Executor failed on channel {}: {}", channel.channel_id, error.what());
                    success = false;
                    break;
                } catch (...) {
                    LOG_ERROR("Executor failed on channel {} with an unknown exception", channel.channel_id);
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
    if (!EnsureWorkers(plan.channels.size())) {
        return false;
    }

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
