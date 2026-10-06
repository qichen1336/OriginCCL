#pragma once

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <thread>
#include <vector>
#include "types.h"
#include "executor/executor.h"

class Communicator;

class PreemptMultiThreadExecutor : public Executor {
public:
    explicit PreemptMultiThreadExecutor(Communicator& comm);
    ~PreemptMultiThreadExecutor() override;

    PreemptMultiThreadExecutor(const PreemptMultiThreadExecutor&) = delete;
    PreemptMultiThreadExecutor& operator=(const PreemptMultiThreadExecutor&) = delete;

    bool Run(const CollPlan& plan) override;
    void Shutdown() override;

private:
    void EnsureWorkers(size_t lane_count);
    void StopWorkers();
    void WorkerLoop(size_t lane_id, uint64_t completed_batch_id);
    bool ExecuteTask(int lane_id, PlanTask& task);
    bool ExchangeLease(size_t lane_id, int32_t frame[4]);

    Communicator& comm_;
    std::vector<std::thread> workers_;
    std::mutex mutex_;
    std::condition_variable work_ready_;
    std::condition_variable work_done_;
    bool stop_ = false;
    uint64_t batch_id_ = 0;
    size_t active_lanes_ = 0;
    size_t completed_lanes_ = 0;
    bool batch_success_ = true;
    const CollPlan* plan_ = nullptr;
    size_t next_ = 0;
    size_t remaining_bytes_ = 0;
};
