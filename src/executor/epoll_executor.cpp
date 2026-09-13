#include <vector>
#include <unistd.h>
#include <string.h>
#include <sys/epoll.h>
#include "executor/epoll_executor.h"
#include "logger.h"

EpollExecutor::~EpollExecutor() {
    Shutdown();
}

void EpollExecutor::Shutdown() {
    if (epoll_fd_ >= 0) {
        close(epoll_fd_);
        epoll_fd_ = -1;
    }
}

bool EpollExecutor::EnsureEpoll() {
    if (epoll_fd_ >= 0) {
        return true;
    }
    epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) {
        LOG_ERROR("Failed to create epoll instance: {}", strerror(errno));
        return false;
    }
    return true;
}

// Register a task's fds in epoll. Both read and write are always watched; the fired event
// is forwarded to AllreduceStep. The slot index travels in data.u32 so an event maps
// straight back to its task without any lookup structure.
bool EpollExecutor::RegisterTask(int slot, const PlanTask& task) {
    int read_fd = task.recv_transport ? task.recv_transport->GetFd() : -1;
    int write_fd = task.send_transport ? task.send_transport->GetFd() : -1;

    if (read_fd >= 0) {
        struct epoll_event ev;
        ev.events = EPOLLIN;
        if (write_fd == read_fd) {
            ev.events |= EPOLLOUT;
        }
        ev.data.u32 = static_cast<uint32_t>(slot);
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, read_fd, &ev) < 0) {
            LOG_ERROR("epoll_ctl ADD read fd {} failed: {}", read_fd, strerror(errno));
            return false;
        }
    }
    if (write_fd >= 0 && write_fd != read_fd) {
        struct epoll_event ev;
        ev.events = EPOLLOUT;
        ev.data.u32 = static_cast<uint32_t>(slot);
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, write_fd, &ev) < 0) {
            LOG_ERROR("epoll_ctl ADD write fd {} failed: {}", write_fd, strerror(errno));
            return false;
        }
    }
    return true;
}

// Remove a task's fds from epoll. fds belong to channel transports reused across plans,
// so a finished task must be deregistered before the next plan re-adds them, otherwise
// EPOLL_CTL_ADD fails with EEXIST.
void EpollExecutor::UnregisterTask(const PlanTask& task) {
    int read_fd = task.recv_transport ? task.recv_transport->GetFd() : -1;
    int write_fd = task.send_transport ? task.send_transport->GetFd() : -1;
    if (read_fd >= 0) {
        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, read_fd, nullptr);
    }
    if (write_fd >= 0 && write_fd != read_fd) {
        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, write_fd, nullptr);
    }
}

bool EpollExecutor::Run(const CollPlan& plan) {
    if (plan.channels.empty()) {
        return true;
    }
    if (!EnsureEpoll()) {
        return false;
    }

    // One outstanding task per channel keeps the single-threaded loop free of
    // cross-channel head-of-line blocking. current[i] is the task we are waiting on. The
    // executor owns each channel's cursor, so it steps tasks through a mutable reference.
    std::vector<PlanTask*> current(plan.channels.size(), nullptr);
    std::vector<size_t> task_index(plan.channels.size(), 0);
    std::vector<bool> channel_ok(plan.channels.size(), true);
    size_t active = 0;

    // Start a channel's front task and, while tasks complete immediately (the no-data-
    // plane single-rank path), keep advancing. Only a task that genuinely waits on the
    // network is left registered in epoll and counted in `active`.
    auto start = [&](size_t slot) -> bool {
        while (task_index[slot] < plan.channels[slot].tasks.size()) {
            PlanTask& task = const_cast<PlanTask&>(plan.channels[slot].tasks[task_index[slot]]);
            Topology* topo = task.topology.get();
            if (!topo || !topo->AllreduceInit(task)) {
                LOG_ERROR("EpollExecutor failed to init task on channel {}", slot);
                return false;
            }
            ++task_index[slot];
            if (topo->AllreduceDone(task)) {
                channel_ok[slot] = channel_ok[slot] && topo->AllreduceSucceeded(task);
                continue;
            }
            current[slot] = &task;
            ++active;
            return RegisterTask(static_cast<int>(slot), task);
        }
        return true;
    };

    for (size_t i = 0; i < plan.channels.size(); ++i) {
        if (!start(i)) {
            return false;
        }
    }

    std::vector<struct epoll_event> events(plan.channels.size() * 2);
    while (active > 0) {
        int ready = epoll_wait(epoll_fd_, events.data(), static_cast<int>(events.size()), -1);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            LOG_ERROR("epoll_wait failed: {}", strerror(errno));
            return false;
        }

        for (int e = 0; e < ready; ++e) {
            size_t slot = events[e].data.u32;
            PlanTask* task = current[slot];
            if (!task) {
                continue;
            }
            Topology* topo = task->topology.get();
            // Feed every readiness bit that fired (a single event can carry both EPOLLIN
            // and EPOLLOUT). Send and recv proceed concurrently within a step.
            if ((events[e].events & EPOLLOUT) != 0 && !topo->AllreduceStep(*task, CollEvent::Writable)) {
                channel_ok[slot] = false;
            }
            if (channel_ok[slot] && (events[e].events & EPOLLIN) != 0 &&
                !topo->AllreduceStep(*task, CollEvent::Readable)) {
                channel_ok[slot] = false;
            }
            if (!topo->AllreduceDone(*task)) {
                continue;
            }

            channel_ok[slot] = channel_ok[slot] && topo->AllreduceSucceeded(*task);
            --active;
            UnregisterTask(*task);
            current[slot] = nullptr;
            if (channel_ok[slot] && !start(slot)) {
                channel_ok[slot] = false;
            }
        }
    }

    for (size_t i = 0; i < plan.channels.size(); ++i) {
        if (!channel_ok[i]) {
            return false;
        }
    }
    return true;
}
