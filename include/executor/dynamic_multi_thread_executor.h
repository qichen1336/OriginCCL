#pragma once

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <thread>
#include <vector>
#include "types.h"
#include "executor/executor.h"
#include "topology/topology.h"

class Communicator;

class DynamicMultiThreadExecutor : public Executor {
public:
    explicit DynamicMultiThreadExecutor(Communicator& comm, int base_cpu = 0);
    ~DynamicMultiThreadExecutor() override;

    DynamicMultiThreadExecutor(const DynamicMultiThreadExecutor&) = delete;
    DynamicMultiThreadExecutor& operator=(const DynamicMultiThreadExecutor&) = delete;

    bool Run(const CollPlan& plan) override;
    void Shutdown() override;

private:
    void EnsureWorkers();
    void StopWorkers();
    void WorkerLoop(int channel_id, uint64_t completed_batch_id);
    bool ProcessChannel(int channel_id, const CollPlan& plan);
    bool ExecuteTask(int channel_id, PlanTask& task);

private:
    Communicator& comm_;
    std::vector<std::thread> workers_;
    std::mutex mutex_;
    std::condition_variable work_ready_;
    std::condition_variable work_done_;
    bool stop_ = false;
    uint64_t batch_id_ = 0;
    size_t active_channels_ = 0;
    size_t completed_channels_ = 0;
    bool batch_success_ = true;
    size_t next_slice_ = 0;
    const CollPlan* plan_ = nullptr;
    int base_cpu_ = 0;
};
