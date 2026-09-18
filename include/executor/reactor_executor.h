#pragma once

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>
#include "types.h"
#include "executor/executor.h"

class ReactorExecutor : public Executor {
public:
    static constexpr size_t kDefaultWorkers = 4;

    explicit ReactorExecutor(size_t worker_count = kDefaultWorkers);
    ~ReactorExecutor() override;

    ReactorExecutor(const ReactorExecutor&) = delete;
    ReactorExecutor& operator=(const ReactorExecutor&) = delete;

    bool Run(const CollPlan& plan) override;
    void Shutdown() override;

private:
    enum class JobState {
        Waiting,
        Done,
        Failed
    };

    struct WorkItem {
        size_t slot = 0;
        PlanTask* task = nullptr;
        bool init = false;
        uint32_t steps = 0;
    };

    struct Completion {
        size_t slot = 0;
        bool init = false;
        JobState state = JobState::Failed;
    };

    bool EnsureEpoll();
    void EnsureWorkers();
    bool RegisterTask(size_t slot, const PlanTask& task);
    void UnregisterTask(const PlanTask& task);
    void PostWork(const WorkItem& item);
    void PostCompletion(const Completion& completion);
    void WorkerLoop();
    void StopWorkers();
    void DrainNotify();

private:
    const size_t worker_count_;

    int epoll_fd_ = -1;
    int notify_fd_ = -1;

    std::vector<std::thread> workers_;
    std::mutex mutex_;
    std::condition_variable work_ready_;
    std::queue<WorkItem> work_queue_;
    std::queue<Completion> completions_;
    bool stop_ = false;
};
