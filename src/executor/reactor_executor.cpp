#include <errno.h>
#include <string.h>
#include <algorithm>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>
#include <vector>
#include "executor/reactor_executor.h"
#include "topology.h"
#include "logger.h"

namespace {
constexpr uint32_t kNotifyTag = 0xFFFFFFFFu;
constexpr uint32_t kStepWritable = 1u;
constexpr uint32_t kStepReadable = 2u;
} // namespace

ReactorExecutor::ReactorExecutor(size_t worker_count)
    : worker_count_(worker_count > 0 ? worker_count : kDefaultWorkers) {}

ReactorExecutor::~ReactorExecutor() {
    Shutdown();
}

void ReactorExecutor::Shutdown() {
    StopWorkers();
    if (notify_fd_ >= 0) {
        close(notify_fd_);
        notify_fd_ = -1;
    }
    if (epoll_fd_ >= 0) {
        close(epoll_fd_);
        epoll_fd_ = -1;
    }
}

void ReactorExecutor::StopWorkers() {
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

    std::lock_guard<std::mutex> lock(mutex_);
    std::queue<WorkItem> no_work;
    std::swap(work_queue_, no_work);
    std::queue<Completion> no_completion;
    std::swap(completions_, no_completion);
    stop_ = false;
}

bool ReactorExecutor::EnsureEpoll() {
    if (epoll_fd_ >= 0) {
        return true;
    }

    epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) {
        LOG_ERROR("Failed to create epoll instance: {}", strerror(errno));
        return false;
    }

    notify_fd_ = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (notify_fd_ < 0) {
        LOG_ERROR("Failed to create reactor eventfd: {}", strerror(errno));
        close(epoll_fd_);
        epoll_fd_ = -1;
        return false;
    }

    struct epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.u32 = kNotifyTag;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, notify_fd_, &ev) < 0) {
        LOG_ERROR("Failed to register reactor eventfd: {}", strerror(errno));
        close(notify_fd_);
        notify_fd_ = -1;
        close(epoll_fd_);
        epoll_fd_ = -1;
        return false;
    }
    return true;
}

void ReactorExecutor::EnsureWorkers() {
    if (!workers_.empty()) {
        return;
    }

    workers_.reserve(worker_count_);
    for (size_t i = 0; i < worker_count_; ++i) {
        workers_.emplace_back(&ReactorExecutor::WorkerLoop, this);
    }
}

bool ReactorExecutor::RegisterTask(size_t slot, const PlanTask& task) {
    const uint32_t tag_base = static_cast<uint32_t>(slot) * 2u;
    if (task.recv_transport) {
        int fd = task.recv_transport->GetFd();
        uint32_t events = task.recv_transport->GetPollEvents();
        if (fd >= 0 && events != 0) {
            struct epoll_event ev;
            ev.events = events | EPOLLET;
            ev.data.u32 = tag_base;
            if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) < 0) {
                LOG_ERROR("epoll_ctl ADD fd {} failed: {}", fd, strerror(errno));
                return false;
            }
        }
    }
    if (task.send_transport) {
        int fd = task.send_transport->GetFd();
        uint32_t events = task.send_transport->GetPollEvents();
        if (fd >= 0 && events != 0) {
            struct epoll_event ev;
            ev.events = events | EPOLLET;
            ev.data.u32 = tag_base + 1u;
            if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) < 0) {
                LOG_ERROR("epoll_ctl ADD fd {} failed: {}", fd, strerror(errno));
                return false;
            }
        }
    }
    return true;
}

void ReactorExecutor::UnregisterTask(const PlanTask& task) {
    if (task.recv_transport) {
        int fd = task.recv_transport->GetFd();
        if (fd >= 0) {
            epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
        }
    }
    if (task.send_transport) {
        int fd = task.send_transport->GetFd();
        if (fd >= 0) {
            epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
        }
    }
}

void ReactorExecutor::PostWork(const WorkItem& item) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        work_queue_.push(item);
    }
    work_ready_.notify_one();
}

void ReactorExecutor::PostCompletion(const Completion& completion) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        completions_.push(completion);
    }
    uint64_t one = 1;
    ssize_t written = write(notify_fd_, &one, sizeof(one));
    (void)written;
}

void ReactorExecutor::DrainNotify() {
    uint64_t value = 0;
    while (read(notify_fd_, &value, sizeof(value)) > 0) {}
}

void ReactorExecutor::WorkerLoop() {
    while (true) {
        WorkItem item;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            work_ready_.wait(lock, [this]() { return stop_ || !work_queue_.empty(); });
            if (stop_) {
                return;
            }
            item = work_queue_.front();
            work_queue_.pop();
        }

        Completion completion;
        completion.slot = item.slot;
        completion.init = item.init;

        Topology* topo = item.task->topology.get();
        bool ok = topo != nullptr;
        if (!ok) {
            LOG_ERROR("Reactor worker received a task without topology on channel {}", item.slot);
        } else if (item.init) {
            ok = topo->AllreduceInit(*item.task);
            if (!ok) {
                LOG_ERROR("Reactor worker failed to init task on channel {}", item.slot);
            }
        } else {
            if ((item.steps & kStepWritable) != 0) {
                ok = topo->AllreduceStep(*item.task, CollEvent::Writable);
            }
            if (ok && (item.steps & kStepReadable) != 0) {
                ok = topo->AllreduceStep(*item.task, CollEvent::Readable);
            }
            if (!ok) {
                LOG_ERROR("Reactor worker step failed on channel {}", item.slot);
            }
        }

        if (!ok) {
            completion.state = JobState::Failed;
        } else if (topo->AllreduceDone(*item.task)) {
            completion.state = JobState::Done;
        } else {
            completion.state = JobState::Waiting;
        }

        PostCompletion(completion);
    }
}

bool ReactorExecutor::Run(const CollPlan& plan) {
    if (plan.channels.empty()) {
        return true;
    }
    if (!EnsureEpoll()) {
        return false;
    }
    EnsureWorkers();

    const size_t channel_count = plan.channels.size();
    std::vector<PlanTask*> current(channel_count, nullptr);
    std::vector<size_t> task_index(channel_count, 0);
    size_t active = 0;
    size_t in_flight = 0;

    auto abort_plan = [&]() {
        for (PlanTask* task : current) {
            if (task) {
                UnregisterTask(*task);
            }
        }
        StopWorkers();
    };

    auto start_next = [&](size_t slot) -> bool {
        while (task_index[slot] < plan.channels[slot].tasks.size()) {
            PlanTask& task = const_cast<PlanTask&>(plan.channels[slot].tasks[task_index[slot]]);
            if (!task.topology) {
                LOG_ERROR("Reactor executor received a task without topology on channel {}", slot);
                return false;
            }
            ++task_index[slot];
            current[slot] = &task;
            ++in_flight;
            PostWork(WorkItem{slot, &task, true, 0});
            return true;
        }
        return true;
    };

    for (size_t slot = 0; slot < channel_count; ++slot) {
        if (!start_next(slot)) {
            abort_plan();
            return false;
        }
    }

    std::vector<struct epoll_event> events(channel_count * 2 + 1);
    std::vector<uint32_t> ready_steps(channel_count, 0);
    while (active > 0 || in_flight > 0) {
        int ready = epoll_wait(epoll_fd_, events.data(), static_cast<int>(events.size()), -1);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            LOG_ERROR("Reactor epoll_wait failed: {}", strerror(errno));
            abort_plan();
            return false;
        }

        std::fill(ready_steps.begin(), ready_steps.end(), 0);
        bool notify = false;
        for (int e = 0; e < ready; ++e) {
            uint32_t tag = events[e].data.u32;
            if (tag == kNotifyTag) {
                notify = true;
                continue;
            }
            size_t slot = tag / 2u;
            if (slot >= channel_count || current[slot] == nullptr) {
                continue;
            }

            size_t side = tag % 2u;
            Transport* transport =
                (side == 0) ? current[slot]->recv_transport.get() : current[slot]->send_transport.get();
            if (!transport || (events[e].events & transport->GetPollEvents()) == 0) {
                continue;
            }
            if (side == 0) {
                ready_steps[slot] |= kStepReadable;
            } else {
                ready_steps[slot] |= kStepWritable;
            }
        }

        for (size_t slot = 0; slot < channel_count; ++slot) {
            if (ready_steps[slot] == 0 || current[slot] == nullptr) {
                continue;
            }
            UnregisterTask(*current[slot]);
            ++in_flight;
            PostWork(WorkItem{slot, current[slot], false, ready_steps[slot]});
        }

        if (!notify) {
            continue;
        }

        DrainNotify();

        std::queue<Completion> completed;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            std::swap(completed, completions_);
        }
        while (!completed.empty()) {
            Completion completion = completed.front();
            completed.pop();
            --in_flight;

            size_t slot = completion.slot;
            const bool counted = !completion.init;

            if (completion.state == JobState::Failed) {
                abort_plan();
                return false;
            }

            if (completion.state == JobState::Waiting) {
                if (!RegisterTask(slot, *current[slot])) {
                    abort_plan();
                    return false;
                }
                if (!counted) {
                    ++active;
                }
                continue;
            }

            if (counted) {
                --active;
            }
            current[slot] = nullptr;
            if (!start_next(slot)) {
                abort_plan();
                return false;
            }
        }
    }

    return true;
}
