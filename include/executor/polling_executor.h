#pragma once

#include <memory>
#include <vector>
#include "types.h"
#include "executor/executor.h"
#include "topology.h"

// Single-threaded polling executor. It never waits on any fd; it repeatedly calls Step()
// on every channel's outstanding task. Tasks are non-blocking, so a round with no
// progress yields the CPU instead of spinning. No worker threads are spawned.
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
