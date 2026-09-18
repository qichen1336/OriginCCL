#pragma once

#include <memory>
#include <vector>
#include "types.h"
#include "executor/executor.h"
#include "topology.h"

class PollingExecutor : public Executor {
public:
    PollingExecutor() = default;
    ~PollingExecutor() override = default;

    PollingExecutor(const PollingExecutor&) = delete;
    PollingExecutor& operator=(const PollingExecutor&) = delete;

    bool Run(const CollPlan& plan) override;
    void Shutdown() override {}

private:
    bool StartFrontTask(size_t slot, const CollPlan& plan, std::vector<PlanTask*>& current,
                        std::vector<size_t>& task_index, size_t& active);
};
