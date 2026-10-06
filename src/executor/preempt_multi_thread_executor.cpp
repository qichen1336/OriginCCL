#include <cerrno>
#include <unistd.h>
#include <sys/epoll.h>
#include <algorithm>
#include <memory>
#include <vector>
#include "executor/preempt_multi_thread_executor.h"
#include "communicator.h"
#include "utils.h"
#include "transport/transport.h"
#include "topology/topology.h"
#include "logger.h"

namespace {

constexpr int32_t kControlMagic = 0x4F434C50;
constexpr size_t kControlWords = 4;
constexpr size_t kMaxLease = 2;

size_t TaskBytes(const PlanTask& task) {
    return task.elem_count * Utils::GetDataTypeSize(task.dtype);
}

} // namespace

PreemptMultiThreadExecutor::PreemptMultiThreadExecutor(Communicator& comm) : comm_(comm) {}

PreemptMultiThreadExecutor::~PreemptMultiThreadExecutor() {
    StopWorkers();
}

void PreemptMultiThreadExecutor::Shutdown() {
    StopWorkers();
}

void PreemptMultiThreadExecutor::EnsureWorkers(size_t lane_count) {
    uint64_t completed_batch_id = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        completed_batch_id = batch_id_;
    }

    workers_.reserve(lane_count);
    while (workers_.size() < lane_count) {
        size_t lane_id = workers_.size();
        workers_.emplace_back(&PreemptMultiThreadExecutor::WorkerLoop, this, lane_id, completed_batch_id);
    }
}

void PreemptMultiThreadExecutor::StopWorkers() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stop_ = true;
    }
    work_ready_.notify_all();

    for (std::thread& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    workers_.clear();
    stop_ = false;
}

bool PreemptMultiThreadExecutor::ExecuteTask(int lane_id, PlanTask& task) {
    if (!task.topology) {
        LOG_ERROR("Executor received invalid task on lane {}", lane_id);
        return false;
    }

    Topology* topo = task.topology.get();
    if (!topo->CollectiveInit(task)) {
        LOG_ERROR("PreemptMultiThreadExecutor failed to init task on lane {}", lane_id);
        return false;
    }
    if (topo->CollectiveDone(task)) {
        return true;
    }

    const int epfd = epoll_create1(EPOLL_CLOEXEC);
    if (epfd < 0) {
        LOG_ERROR("PreemptMultiThreadExecutor failed to create epoll on lane {}", lane_id);
        return false;
    }

    auto add_fd = [&](Transport* transport, uint32_t tag) -> bool {
        if (!transport) {
            return true;
        }
        int fd = transport->GetFd();
        uint32_t events = transport->GetPollEvents();
        if (fd < 0 || events == 0) {
            return true;
        }
        struct epoll_event ev;
        ev.events = events | EPOLLET;
        ev.data.u32 = tag;
        if (epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev) < 0) {
            LOG_ERROR("PreemptMultiThreadExecutor epoll_ctl ADD fd {} failed on lane {}", fd, lane_id);
            return false;
        }
        return true;
    };

    const size_t transport_count = task.recv_transports.size() + task.send_transports.size();
    bool registered = true;
    for (const auto& transport : task.recv_transports) {
        if (!add_fd(transport.get(), 0u)) {
            registered = false;
            break;
        }
    }
    if (registered) {
        for (const auto& transport : task.send_transports) {
            if (!add_fd(transport.get(), 1u)) {
                registered = false;
                break;
            }
        }
    }

    std::vector<struct epoll_event> events(std::max<size_t>(transport_count, 1));
    while (registered && !topo->CollectiveDone(task)) {
        int ready = epoll_wait(epfd, events.data(), static_cast<int>(events.size()), -1);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            LOG_ERROR("PreemptMultiThreadExecutor epoll_wait failed on lane {}", lane_id);
            break;
        }
        for (int i = 0; i < ready; ++i) {
            CollEvent op = (events[i].data.u32 == 0u) ? CollEvent::Readable : CollEvent::Writable;
            if (!topo->CollectiveStep(task, op)) {
                LOG_ERROR("PreemptMultiThreadExecutor step failed on lane {}", lane_id);
                registered = false;
                break;
            }
        }
    }

    close(epfd);
    return registered && topo->CollectiveDone(task);
}

bool PreemptMultiThreadExecutor::ExchangeLease(size_t lane_id, int32_t frame[4]) {
    PlanTask control;
    control.func = CollFunc::Broadcast;
    control.recv_buf = frame;
    control.elem_count = kControlWords;
    control.dtype = DataType::INT32;
    control.root = 0;
    control.rank = comm_.GetRank();
    control.world_size = comm_.GetWorldSize();
    control.topology = comm_.GetRingTopology();
    control.channel_id = static_cast<int>(lane_id);
    control.topology->FillTransports(comm_.GetChannel(static_cast<int>(lane_id)), control);

    if (comm_.GetRank() == 0) {
        std::lock_guard<std::mutex> lock(mutex_);
        const size_t total = plan_->collectives.size();
        size_t lease = 0;
        if (next_ < total) {
            const size_t budget = std::max<size_t>(remaining_bytes_ / active_lanes_, 1);
            size_t accumulated = 0;
            while (next_ + lease < total && lease < kMaxLease) {
                accumulated += TaskBytes(plan_->collectives[next_ + lease]);
                ++lease;
                if (accumulated >= budget) {
                    break;
                }
            }
            if (lease == 0) {
                lease = 1;
            }
            for (size_t i = 0; i < lease; ++i) {
                remaining_bytes_ -= TaskBytes(plan_->collectives[next_ + i]);
            }
        }
        frame[0] = kControlMagic;
        frame[1] = static_cast<int32_t>(lane_id);
        frame[2] = static_cast<int32_t>(next_);
        frame[3] = static_cast<int32_t>(lease);
        next_ += lease;
    }

    if (!ExecuteTask(static_cast<int>(lane_id), control)) {
        return false;
    }
    return comm_.GetRank() == 0 || frame[0] == kControlMagic;
}

void PreemptMultiThreadExecutor::WorkerLoop(size_t lane_id, uint64_t completed_batch_id) {
    while (true) {
        const CollPlan* plan = nullptr;
        uint64_t batch_id = 0;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            work_ready_.wait(lock, [this, completed_batch_id]() { return stop_ || batch_id_ != completed_batch_id; });
            if (stop_) {
                return;
            }
            plan = plan_;
            batch_id = batch_id_;
        }

        bool success = true;
        while (true) {
            int32_t frame[kControlWords] = {0, 0, 0, 0};
            if (!ExchangeLease(lane_id, frame)) {
                success = false;
                break;
            }
            const int32_t lease = frame[3];
            if (lease <= 0) {
                break;
            }
            for (int32_t i = 0; i < lease; ++i) {
                const size_t index = static_cast<size_t>(frame[2]) + static_cast<size_t>(i);
                PlanTask task = plan->collectives[index];
                task.channel_id = static_cast<int>(lane_id);
                task.topology->FillTransports(comm_.GetChannel(static_cast<int>(lane_id)), task);
                if (!ExecuteTask(static_cast<int>(lane_id), task)) {
                    success = false;
                    break;
                }
            }
            if (!success) {
                break;
            }
        }

        {
            std::lock_guard<std::mutex> lock(mutex_);
            batch_success_ = batch_success_ && success;
            ++completed_lanes_;
            completed_batch_id = batch_id;
            if (completed_lanes_ == active_lanes_) {
                work_done_.notify_one();
            }
        }
    }
}

bool PreemptMultiThreadExecutor::Run(const CollPlan& plan) {
    EnsureWorkers(static_cast<size_t>(comm_.GetNChannels()));

    if (plan.collectives.empty()) {
        return true;
    }

    std::unique_lock<std::mutex> lock(mutex_);
    plan_ = &plan;
    next_ = 0;
    remaining_bytes_ = 0;
    for (const PlanTask& task : plan.collectives) {
        remaining_bytes_ += TaskBytes(task);
    }
    active_lanes_ = workers_.size();
    completed_lanes_ = 0;
    batch_success_ = true;
    ++batch_id_;
    work_ready_.notify_all();
    work_done_.wait(lock, [this]() { return completed_lanes_ == active_lanes_; });
    return batch_success_;
}
