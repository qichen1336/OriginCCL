#include <cerrno>
#include <unistd.h>
#include <sys/epoll.h>
#include "executor/multi_thread_executor.h"
#include "transport/transport.h"
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

    const int epfd = epoll_create1(EPOLL_CLOEXEC);
    if (epfd < 0) {
        LOG_ERROR("MultiThreadExecutor failed to create epoll on channel {}", channel_id);
        return false;
    }

    auto add_fd = [&](Transport* transport, uint32_t tag) -> bool {
        if (!transport) {
            return true;
        }
        int fd = transport->GetFd();
        uint32_t events = transport->GetPollEvents();
        if (fd < 0 || events == 0) {
            return true;
        }
        struct epoll_event ev;
        ev.events = events | EPOLLET;
        ev.data.u32 = tag;
        if (epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev) < 0) {
            LOG_ERROR("MultiThreadExecutor epoll_ctl ADD fd {} failed on channel {}", fd, channel_id);
            return false;
        }
        return true;
    };

    bool registered = add_fd(task.recv_transport.get(), 0u) && add_fd(task.send_transport.get(), 1u);

    struct epoll_event events[2];
    while (registered && !topo->CollectiveDone(task)) {
        int ready = epoll_wait(epfd, events, 2, -1);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            LOG_ERROR("MultiThreadExecutor epoll_wait failed on channel {}", channel_id);
            break;
        }
        for (int i = 0; i < ready; ++i) {
            CollEvent op = (events[i].data.u32 == 0u) ? CollEvent::Readable : CollEvent::Writable;
            if (!topo->CollectiveStep(task, op)) {
                LOG_ERROR("MultiThreadExecutor step failed on channel {}", channel_id);
                registered = false;
                break;
            }
        }
    }

    close(epfd);
    return registered && topo->CollectiveDone(task);
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
