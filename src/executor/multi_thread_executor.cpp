#include <poll.h>
#include "executor/multi_thread_executor.h"
#include "topology.h"
#include "logger.h"

namespace {
short WaitInterest(WaitCondition condition) {
    return condition == WaitCondition::Writable ? POLLOUT : POLLIN;
}

WaitDescriptor RecvWait(const PlanTask& task) {
    return task.recv_transport ? task.recv_transport->RecvWait() : WaitDescriptor{};
}

WaitDescriptor SendWait(const PlanTask& task) {
    return task.send_transport ? task.send_transport->SendWait() : WaitDescriptor{};
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

    // Wait on each transport's readiness handle rather than a raw socket fd, so this loop
    // drives a socket (send fd writable, recv fd readable) and a shared-memory ring (a
    // readable notification fd in both directions) without knowing which one it got.
    // Watching both means a blocked send never starves the matching recv, so no helper send
    // thread is needed even when prev == next (2-rank degenerate).
    const WaitDescriptor recv_wait = RecvWait(task);
    const WaitDescriptor send_wait = SendWait(task);
    while (!topo->AllreduceDone(task)) {
        struct pollfd fds[2];
        bool recv_on[2] = {false, false};
        bool send_on[2] = {false, false};
        nfds_t nfds = 0;
        // Both directions may report the same fd (one handle for the whole edge); poll
        // takes one entry per fd, so the interests are merged and the entry remembers which
        // directions it drives.
        auto add_wait = [&](const WaitDescriptor& wait, bool is_send) {
            if (wait.fd < 0) {
                return;
            }
            for (nfds_t i = 0; i < nfds; ++i) {
                if (fds[i].fd == wait.fd) {
                    fds[i].events |= WaitInterest(wait.condition);
                    (is_send ? send_on[i] : recv_on[i]) = true;
                    return;
                }
            }
            fds[nfds].fd = wait.fd;
            fds[nfds].events = WaitInterest(wait.condition);
            fds[nfds].revents = 0;
            (is_send ? send_on[nfds] : recv_on[nfds]) = true;
            ++nfds;
        };
        add_wait(recv_wait, false);
        add_wait(send_wait, true);

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

        // Feed every readiness event that fired. Within one step the send and the recv
        // transfer proceed concurrently, so driving only one of them starves the other and
        // deadlocks the 2-rank degenerate ring (prev == next).
        for (nfds_t i = 0; i < nfds; ++i) {
            if (fds[i].revents == 0) {
                continue;
            }
            if (send_on[i] && (fds[i].revents & WaitInterest(send_wait.condition)) != 0 &&
                !topo->AllreduceStep(task, CollEvent::Writable)) {
                LOG_ERROR("MultiThreadExecutor step failed on channel {}", channel_id);
                return false;
            }
            if (recv_on[i] && (fds[i].revents & WaitInterest(recv_wait.condition)) != 0 &&
                !topo->AllreduceStep(task, CollEvent::Readable)) {
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
                // This worker exclusively owns its channel's task cursor for the batch.
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
