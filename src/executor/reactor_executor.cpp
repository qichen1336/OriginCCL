#include <errno.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>
#include <vector>
#include "executor/reactor_executor.h"
#include "transport.h"
#include "topology.h"
#include "logger.h"

namespace {
// Channel slots are small indices, so a saturated tag can never collide with one.
constexpr uint32_t kNotifyTag = 0xFFFFFFFFu;

// epoll carries only one u32 tag per registered fd, so the tag packs the channel slot
// together with the direction(s) that fd drives. An event can then be turned back into the
// logical CollEvent without any lookup structure.
constexpr uint32_t kDirRecv = 1u;
constexpr uint32_t kDirSend = 2u;
constexpr uint32_t kDirMask = kDirRecv | kDirSend;
constexpr uint32_t kDirShift = 2;

uint32_t Tag(size_t slot, uint32_t dir) {
    return (static_cast<uint32_t>(slot) << kDirShift) | dir;
}

uint32_t WaitInterest(WaitCondition condition) {
    return condition == WaitCondition::Writable ? EPOLLOUT : EPOLLIN;
}

WaitDescriptor RecvWait(const PlanTask& task) {
    return task.recv_transport ? task.recv_transport->RecvWait() : WaitDescriptor{};
}

WaitDescriptor SendWait(const PlanTask& task) {
    return task.send_transport ? task.send_transport->SendWait() : WaitDescriptor{};
}
} // namespace

ReactorExecutor::ReactorExecutor(size_t worker_count)
    : worker_count_(worker_count > 0 ? worker_count : kDefaultWorkers) {}

ReactorExecutor::~ReactorExecutor() {
    Shutdown();
}

void ReactorExecutor::Shutdown() {
    StopWorkers();
    if (notify_fd_ >= 0) {
        close(notify_fd_);
        notify_fd_ = -1;
    }
    if (epoll_fd_ >= 0) {
        close(epoll_fd_);
        epoll_fd_ = -1;
    }
}

// Stop and join every worker, then drop anything still queued. Run() calls this on a
// fatal wait failure so that no worker can still hold a PlanTask pointer once Run
// returns; Shutdown() relies on it too, before the channel transports go away.
void ReactorExecutor::StopWorkers() {
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

    std::lock_guard<std::mutex> lock(mutex_);
    std::queue<WorkItem> no_work;
    std::swap(work_queue_, no_work);
    std::queue<Completion> no_completion;
    std::swap(completions_, no_completion);
    stop_ = false;
}

bool ReactorExecutor::EnsureEpoll() {
    if (epoll_fd_ >= 0) {
        return true;
    }

    epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) {
        LOG_ERROR("Failed to create epoll instance: {}", strerror(errno));
        return false;
    }

    // The completion eventfd shares the epoll instance with the transport fds, so one
    // epoll_wait covers both readiness and worker results.
    notify_fd_ = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (notify_fd_ < 0) {
        LOG_ERROR("Failed to create reactor eventfd: {}", strerror(errno));
        close(epoll_fd_);
        epoll_fd_ = -1;
        return false;
    }

    struct epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.u32 = kNotifyTag;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, notify_fd_, &ev) < 0) {
        LOG_ERROR("Failed to register reactor eventfd: {}", strerror(errno));
        close(notify_fd_);
        notify_fd_ = -1;
        close(epoll_fd_);
        epoll_fd_ = -1;
        return false;
    }
    return true;
}

void ReactorExecutor::EnsureWorkers() {
    if (!workers_.empty()) {
        return;
    }

    workers_.reserve(worker_count_);
    for (size_t i = 0; i < worker_count_; ++i) {
        workers_.emplace_back(&ReactorExecutor::WorkerLoop, this);
    }
}

bool ReactorExecutor::AddFd(int fd, uint32_t events, uint32_t tag) {
    struct epoll_event ev;
    ev.events = events;
    ev.data.u32 = tag;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) < 0) {
        LOG_ERROR("epoll_ctl ADD fd {} failed: {}", fd, strerror(errno));
        return false;
    }
    return true;
}

// Register a task's fds in epoll. Interest comes from each transport's readiness handle, so
// a socket and a shared-memory notification fd are registered the same way. Both directions
// are always watched; the fired event is forwarded to the worker running the step.
bool ReactorExecutor::RegisterTask(size_t slot, const PlanTask& task) {
    const WaitDescriptor recv_wait = RecvWait(task);
    const WaitDescriptor send_wait = SendWait(task);

    // A transport may expose one fd for both directions. epoll accepts a single
    // registration per fd, so those interests merge into one ADD tagged for both.
    if (recv_wait.fd >= 0 && recv_wait.fd == send_wait.fd) {
        return AddFd(recv_wait.fd, WaitInterest(recv_wait.condition) | WaitInterest(send_wait.condition),
                     Tag(slot, kDirRecv | kDirSend));
    }
    if (recv_wait.fd >= 0 && !AddFd(recv_wait.fd, WaitInterest(recv_wait.condition), Tag(slot, kDirRecv))) {
        return false;
    }
    if (send_wait.fd >= 0 && !AddFd(send_wait.fd, WaitInterest(send_wait.condition), Tag(slot, kDirSend))) {
        return false;
    }
    return true;
}

// Remove a task's fds from epoll. Channel transports are reused across plans, so a
// finished or in-flight task must be deregistered before the fds are registered again,
// otherwise EPOLL_CTL_ADD fails with EEXIST.
void ReactorExecutor::UnregisterTask(const PlanTask& task) {
    const WaitDescriptor recv_wait = RecvWait(task);
    const WaitDescriptor send_wait = SendWait(task);
    if (recv_wait.fd >= 0) {
        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, recv_wait.fd, nullptr);
    }
    if (send_wait.fd >= 0 && send_wait.fd != recv_wait.fd) {
        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, send_wait.fd, nullptr);
    }
}

void ReactorExecutor::PostWork(const WorkItem& item) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        work_queue_.push(item);
    }
    work_ready_.notify_one();
}

// The completion must reach the queue before the eventfd is written: the reactor drains
// the eventfd first, so a wakeup without a visible completion would be lost.
void ReactorExecutor::PostCompletion(const Completion& completion) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        completions_.push(completion);
    }
    uint64_t one = 1;
    ssize_t written = write(notify_fd_, &one, sizeof(one));
    (void)written;
}

void ReactorExecutor::DrainNotify() {
    uint64_t value = 0;
    while (read(notify_fd_, &value, sizeof(value)) > 0) {}
}

void ReactorExecutor::WorkerLoop() {
    while (true) {
        WorkItem item;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            work_ready_.wait(lock, [this]() { return stop_ || !work_queue_.empty(); });
            if (stop_) {
                return;
            }
            item = work_queue_.front();
            work_queue_.pop();
        }

        Completion completion;
        completion.slot = item.slot;
        completion.init = item.init;

        Topology* topo = item.task->topology.get();
        bool ok = topo != nullptr;
        if (!ok) {
            LOG_ERROR("Reactor worker received a task without topology on channel {}", item.slot);
        } else if (item.init) {
            ok = topo->AllreduceInit(*item.task);
            if (!ok) {
                LOG_ERROR("Reactor worker failed to init task on channel {}", item.slot);
            }
        } else {
            // Feed every direction this job was dispatched for. A single step's send and
            // recv transfers must both advance: 2-rank rings degenerate to prev == next, so
            // starving the matching recv deadlocks.
            if (item.writable) {
                ok = topo->AllreduceStep(*item.task, CollEvent::Writable);
            }
            if (ok && item.readable) {
                ok = topo->AllreduceStep(*item.task, CollEvent::Readable);
            }
            if (!ok) {
                LOG_ERROR("Reactor worker step failed on channel {}", item.slot);
            }
        }

        if (!ok) {
            completion.state = JobState::Failed;
        } else if (topo->AllreduceDone(*item.task)) {
            completion.state = JobState::Done;
        } else {
            completion.state = JobState::Waiting;
        }

        PostCompletion(completion);
    }
}

bool ReactorExecutor::Run(const CollPlan& plan) {
    if (plan.channels.empty()) {
        return true;
    }
    if (!EnsureEpoll()) {
        return false;
    }
    EnsureWorkers();

    // One outstanding task per channel keeps the loop free of cross-channel head-of-line
    // blocking. The reactor thread owns these cursors and is the only writer; a worker
    // sees a task only through the WorkItem it was handed.
    const size_t channel_count = plan.channels.size();
    std::vector<PlanTask*> current(channel_count, nullptr);
    std::vector<size_t> task_index(channel_count, 0);
    size_t active = 0;
    size_t in_flight = 0;

    auto abort_plan = [&]() {
        for (PlanTask* task : current) {
            if (task) {
                UnregisterTask(*task);
            }
        }
        StopWorkers();
    };

    // Queue a channel's front task. AllreduceInit runs on a worker like any other step,
    // so the reactor thread never calls into a topology itself.
    auto start_next = [&](size_t slot) -> bool {
        while (task_index[slot] < plan.channels[slot].tasks.size()) {
            PlanTask& task = const_cast<PlanTask&>(plan.channels[slot].tasks[task_index[slot]]);
            if (!task.topology) {
                LOG_ERROR("Reactor executor received a task without topology on channel {}", slot);
                return false;
            }
            ++task_index[slot];
            current[slot] = &task;
            ++in_flight;
            PostWork(WorkItem{slot, &task, true, false, false});
            return true;
        }
        return true;
    };

    for (size_t slot = 0; slot < channel_count; ++slot) {
        if (!start_next(slot)) {
            abort_plan();
            return false;
        }
    }

    std::vector<struct epoll_event> events(channel_count * 2 + 1);
    while (active > 0 || in_flight > 0) {
        int ready = epoll_wait(epoll_fd_, events.data(), static_cast<int>(events.size()), -1);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            LOG_ERROR("Reactor epoll_wait failed: {}", strerror(errno));
            // There is no safe way to keep waiting; tear the plan down before returning.
            abort_plan();
            return false;
        }

        // Merge readiness per slot first: the two directions of one channel are separate
        // epoll entries that carry the same slot tag, and a single step must feed every
        // direction that actually became ready.
        std::vector<bool> read_ready(channel_count, false);
        std::vector<bool> write_ready(channel_count, false);
        bool notify = false;
        for (int e = 0; e < ready; ++e) {
            const uint32_t tag = events[e].data.u32;
            if (tag == kNotifyTag) {
                notify = true;
                continue;
            }
            const size_t slot = tag >> kDirShift;
            const uint32_t dir = tag & kDirMask;
            if (slot >= channel_count || current[slot] == nullptr) {
                continue;
            }
            const PlanTask& task = *current[slot];
            const uint32_t fired = events[e].events;
            if ((dir & kDirRecv) != 0 && (fired & WaitInterest(RecvWait(task).condition)) != 0) {
                read_ready[slot] = true;
            }
            if ((dir & kDirSend) != 0 && (fired & WaitInterest(SendWait(task).condition)) != 0) {
                write_ready[slot] = true;
            }
        }

        // Hand every ready channel to the pool. The fds leave epoll for the duration of
        // the step, so level-triggered epoll cannot keep reporting a writable socket
        // while a worker is already advancing that task.
        for (size_t slot = 0; slot < channel_count; ++slot) {
            if ((!read_ready[slot] && !write_ready[slot]) || current[slot] == nullptr) {
                continue;
            }
            UnregisterTask(*current[slot]);
            ++in_flight;
            PostWork(WorkItem{slot, current[slot], false, read_ready[slot], write_ready[slot]});
        }

        if (!notify) {
            continue;
        }

        DrainNotify();

        std::queue<Completion> completed;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            std::swap(completed, completions_);
        }
        while (!completed.empty()) {
            Completion completion = completed.front();
            completed.pop();
            --in_flight;

            size_t slot = completion.slot;
            // A step job belongs to a channel already counted in `active`. An init job
            // only joins that count once it turns out to need the network, which is what
            // keeps a single-rank plan (every task completes immediately) out of an
            // empty epoll_wait. Every path that retires the channel's current task has to
            // return the count, or a lone channel keeps the loop alive forever.
            const bool counted = !completion.init;

            if (completion.state == JobState::Failed) {
                abort_plan();
                return false;
            }

            if (completion.state == JobState::Waiting) {
                // current[slot] stays set on failure: RegisterTask may have added the read
                // fd before the write fd failed, so abort_plan must still see the task to
                // deregister that half-registered pair.
                if (!RegisterTask(slot, *current[slot])) {
                    abort_plan();
                    return false;
                }
                if (!counted) {
                    ++active;
                }
                continue;
            }

            // JobState::Done: settle this task and queue the channel's next one.
            if (counted) {
                --active;
            }
            current[slot] = nullptr;
            if (!start_next(slot)) {
                abort_plan();
                return false;
            }
        }
    }

    return true;
}
