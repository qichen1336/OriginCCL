#include <vector>
#include <unistd.h>
#include <string.h>
#include <sys/epoll.h>
#include "executor/epoll_executor.h"
#include "transport.h"
#include "logger.h"

namespace {
// epoll carries only one u32 tag per registered fd, so the tag packs the channel slot
// together with the direction(s) that fd drives. An event can then be turned back into the
// logical CollEvent without any lookup structure.
constexpr uint32_t kDirRecv = 1u;
constexpr uint32_t kDirSend = 2u;
constexpr uint32_t kDirMask = kDirRecv | kDirSend;
constexpr uint32_t kDirShift = 2;

uint32_t Tag(size_t slot, uint32_t dir) {
    return (static_cast<uint32_t>(slot) << kDirShift) | dir;
}

uint32_t WaitInterest(WaitCondition condition) {
    return condition == WaitCondition::Writable ? EPOLLOUT : EPOLLIN;
}

WaitDescriptor RecvWait(const PlanTask& task) {
    return task.recv_transport ? task.recv_transport->RecvWait() : WaitDescriptor{};
}

WaitDescriptor SendWait(const PlanTask& task) {
    return task.send_transport ? task.send_transport->SendWait() : WaitDescriptor{};
}
} // namespace

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

bool EpollExecutor::AddFd(int fd, uint32_t events, uint32_t tag) {
    struct epoll_event ev;
    ev.events = events;
    ev.data.u32 = tag;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) < 0) {
        LOG_ERROR("epoll_ctl ADD fd {} failed: {}", fd, strerror(errno));
        return false;
    }
    return true;
}

// Register a task's fds in epoll. Interest comes from each transport's readiness handle, so
// a socket and a shared-memory notification fd are registered the same way. Both directions
// are always watched; the fired event is forwarded to AllreduceStep.
bool EpollExecutor::RegisterTask(int slot, const PlanTask& task) {
    const WaitDescriptor recv_wait = RecvWait(task);
    const WaitDescriptor send_wait = SendWait(task);

    // A transport may expose one fd for both directions. epoll accepts a single
    // registration per fd, so those interests merge into one ADD tagged for both.
    if (recv_wait.fd >= 0 && recv_wait.fd == send_wait.fd) {
        return AddFd(recv_wait.fd, WaitInterest(recv_wait.condition) | WaitInterest(send_wait.condition),
                     Tag(static_cast<size_t>(slot), kDirRecv | kDirSend));
    }
    if (recv_wait.fd >= 0 &&
        !AddFd(recv_wait.fd, WaitInterest(recv_wait.condition), Tag(static_cast<size_t>(slot), kDirRecv))) {
        return false;
    }
    if (send_wait.fd >= 0 &&
        !AddFd(send_wait.fd, WaitInterest(send_wait.condition), Tag(static_cast<size_t>(slot), kDirSend))) {
        return false;
    }
    return true;
}

// Remove a task's fds from epoll. fds belong to channel transports reused across plans,
// so a finished task must be deregistered before the next plan re-adds them, otherwise
// EPOLL_CTL_ADD fails with EEXIST.
void EpollExecutor::UnregisterTask(const PlanTask& task) {
    const WaitDescriptor recv_wait = RecvWait(task);
    const WaitDescriptor send_wait = SendWait(task);
    if (recv_wait.fd >= 0) {
        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, recv_wait.fd, nullptr);
    }
    if (send_wait.fd >= 0 && send_wait.fd != recv_wait.fd) {
        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, send_wait.fd, nullptr);
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
    size_t active = 0;

    // A failure abandons the whole plan. Every fd still registered must leave epoll
    // first: they belong to channel transports reused across plans, so a leftover
    // registration makes the next plan's EPOLL_CTL_ADD fail with EEXIST.
    auto abort_plan = [&]() {
        for (PlanTask* task : current) {
            if (task) {
                UnregisterTask(*task);
            }
        }
    };

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
            abort_plan();
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
            abort_plan();
            return false;
        }

        for (int e = 0; e < ready; ++e) {
            const uint32_t tag = events[e].data.u32;
            const size_t slot = tag >> kDirShift;
            const uint32_t dir = tag & kDirMask;
            if (slot >= plan.channels.size()) {
                continue;
            }
            PlanTask* task = current[slot];
            if (!task) {
                continue;
            }
            Topology* topo = task->topology.get();
            // Feed every direction carried by this registration whose readiness actually
            // fired (one registration can carry both, and one event can set both bits). Send
            // and recv proceed concurrently within a step. A false return is the failure
            // channel, so abandon the plan there and then instead of spinning on a task that
            // can never reach a done state.
            const uint32_t fired = events[e].events;
            if ((dir & kDirSend) != 0 && (fired & WaitInterest(SendWait(*task).condition)) != 0 &&
                !topo->AllreduceStep(*task, CollEvent::Writable)) {
                LOG_ERROR("EpollExecutor step failed on channel {}", slot);
                abort_plan();
                return false;
            }
            if ((dir & kDirRecv) != 0 && (fired & WaitInterest(RecvWait(*task).condition)) != 0 &&
                !topo->AllreduceStep(*task, CollEvent::Readable)) {
                LOG_ERROR("EpollExecutor step failed on channel {}", slot);
                abort_plan();
                return false;
            }
            if (!topo->AllreduceDone(*task)) {
                continue;
            }

            --active;
            UnregisterTask(*task);
            current[slot] = nullptr;
            if (!start(slot)) {
                abort_plan();
                return false;
            }
        }
    }

    return true;
}
