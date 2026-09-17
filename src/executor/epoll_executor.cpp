#include <vector>
#include <unistd.h>
#include <string.h>
#include <sys/epoll.h>
#include "executor/epoll_executor.h"
#include "executor/transport_wait.h"
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

// Register the readiness a task's transports ask to be waited on. The tag carries the slot
// and the wait index within it, so an event maps back to its task and logical operation.
bool EpollExecutor::RegisterTask(int slot, const PlanTask& task) {
    TransportWait waits[kTransportWaitMax];
    size_t count = BuildTransportWaits(task, waits);
    const uint32_t tag_base = static_cast<uint32_t>(slot) * static_cast<uint32_t>(kTransportWaitMax);

    for (size_t i = 0; i < count; ++i) {
        struct epoll_event ev;
        ev.events = waits[i].events;
        ev.data.u32 = tag_base + static_cast<uint32_t>(i);
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, waits[i].fd, &ev) < 0) {
            LOG_ERROR("epoll_ctl ADD fd {} failed: {}", waits[i].fd, strerror(errno));
            return false;
        }
    }
    return true;
}

// Remove a task's registrations. Transports are reused across plans, so a finished task
// must be deregistered or the next plan's EPOLL_CTL_ADD fails with EEXIST.
void EpollExecutor::UnregisterTask(const PlanTask& task) {
    TransportWait waits[kTransportWaitMax];
    size_t count = BuildTransportWaits(task, waits);
    for (size_t i = 0; i < count; ++i) {
        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, waits[i].fd, nullptr);
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

    std::vector<struct epoll_event> events(plan.channels.size() * kTransportWaitMax);
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
            uint32_t tag = events[e].data.u32;
            size_t slot = tag / kTransportWaitMax;
            PlanTask* task = current[slot];
            if (!task) {
                continue;
            }

            // The registration, not the event bits, says which operation to advance:
            // a shared descriptor carries both directions and must feed both.
            TransportWait waits[kTransportWaitMax];
            size_t count = BuildTransportWaits(*task, waits);
            size_t side = tag % kTransportWaitMax;
            if (side >= count || (events[e].events & waits[side].events) == 0) {
                continue;
            }

            Topology* topo = task->topology.get();
            // A false return is the failure channel, so abandon the plan there and then
            // instead of spinning on a task that can never reach a done state.
            if (waits[side].writable && !topo->AllreduceStep(*task, CollEvent::Writable)) {
                LOG_ERROR("EpollExecutor step failed on channel {}", slot);
                abort_plan();
                return false;
            }
            if (waits[side].readable && !topo->AllreduceStep(*task, CollEvent::Readable)) {
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
