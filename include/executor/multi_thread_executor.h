#pragma once

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <thread>
#include <vector>
#include "types.h"
#include "executor/executor.h"
#include "topology.h"

class MultiThreadExecutor : public Executor {
public:
    MultiThreadExecutor() = default;
    ~MultiThreadExecutor() override;

    MultiThreadExecutor(const MultiThreadExecutor&) = delete;
    MultiThreadExecutor& operator=(const MultiThreadExecutor&) = delete;

    bool Run(const CollPlan& plan) override;
    void Shutdown() override;

private:
    bool EnsureWorkers(size_t channel_count);
    void StopWorkers();
    void WorkerLoop(size_t channel_id, uint64_t completed_batch_id);
    bool ExecuteTask(int channel_id, PlanTask& task);

private:
    std::vector<std::thread> workers_;
    std::mutex mutex_;
    std::condition_variable work_ready_;
    std::condition_variable work_done_;
    bool stop_ = false;
    uint64_t batch_id_ = 0;
    size_t active_channels_ = 0;
    size_t completed_channels_ = 0;
    bool batch_success_ = true;
    const CollPlan* plan_ = nullptr;
};