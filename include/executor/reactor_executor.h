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

// Reactor executor: the calling thread only waits for readiness in epoll, and a pool of
// worker threads runs every topology call. The reactor thread owns fd registration, the
// per-channel task cursor and the decision to start the next task; a worker only runs
// AllreduceInit/AllreduceStep on the one task it was handed. A PlanTask cursor therefore
// never has two writers, because a channel's fds are removed from epoll while its step is
// in flight.
//
// Thread hand-off:
//   reactor -> worker  a mutex-protected FIFO queue plus a condition variable; an idle
//                      worker pops the next job, so no channel is bound to a worker.
//   worker  -> reactor a mutex-protected completion queue plus an eventfd write. The
//                      eventfd is registered in the same epoll instance, so the reactor
//                      needs a single wait to observe both socket readiness and results.
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
        uint32_t events = 0;
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
