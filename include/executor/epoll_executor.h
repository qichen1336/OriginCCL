#pragma once

#include <memory>
#include <vector>
#include "types.h"
#include "executor/executor.h"
#include "topology.h"

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
