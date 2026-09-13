#pragma once

#include <memory>
#include <vector>
#include "types.h"
#include "executor/executor.h"
#include "topology.h"

// Single-threaded event-driven executor. One thread registers every channel's current
// task fds into a single epoll instance and advances whichever becomes ready. No worker
// threads are spawned; the calling thread drives the whole plan to completion.
class EpollExecutor : public Executor {
public:
    EpollExecutor() = default;
    ~EpollExecutor() override;

    EpollExecutor(const EpollExecutor&) = delete;
    EpollExecutor& operator=(const EpollExecutor&) = delete;

    bool Run(const CollPlan& plan) override;
    void Shutdown() override;

private:
    bool EnsureEpoll();
    bool RegisterTask(int slot, const PlanTask& task);
    void UnregisterTask(const PlanTask& task);

private:
    int epoll_fd_ = -1;
};
