#include <vector>
#include <unistd.h>
#include <string.h>
#include <sys/epoll.h>
#include "executor/epoll_executor.h"
#include "logger.h"

EpollExecutor::~EpollExecutor() {
    Shutdown();
}

void EpollExecutor::Shutdown() {
    if (epoll_fd_ >= 0) {
        close(epoll_fd_);
        epoll_fd_ = -1;
    }
}

bool EpollExecutor::EnsureEpoll() {
    if (epoll_fd_ >= 0) {
        return true;
    }
    epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) {
        LOG_ERROR("Failed to create epoll instance: {}", strerror(errno));
        return false;
    }
    return true;
}

bool EpollExecutor::RegisterTask(int slot, const PlanTask& task) {
    const uint32_t tag_base = static_cast<uint32_t>(slot) * 2u;
    if (task.recv_transport) {
        int fd = task.recv_transport->GetFd();
        uint32_t events = task.recv_transport->GetPollEvents();
        if (fd >= 0 && events != 0) {
            struct epoll_event ev;
            ev.events = events | EPOLLET;
            ev.data.u32 = tag_base;
            if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) < 0) {
                LOG_ERROR("epoll_ctl ADD fd {} failed: {}", fd, strerror(errno));
                return false;
            }
        }
    }
    if (task.send_transport) {
        int fd = task.send_transport->GetFd();
        uint32_t events = task.send_transport->GetPollEvents();
        if (fd >= 0 && events != 0) {
            struct epoll_event ev;
            ev.events = events | EPOLLET;
            ev.data.u32 = tag_base + 1u;
            if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) < 0) {
                LOG_ERROR("epoll_ctl ADD fd {} failed: {}", fd, strerror(errno));
                return false;
            }
        }
    }
    return true;
}

void EpollExecutor::UnregisterTask(const PlanTask& task) {
    if (task.recv_transport) {
        int fd = task.recv_transport->GetFd();
        if (fd >= 0) {
            epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
        }
    }
    if (task.send_transport) {
        int fd = task.send_transport->GetFd();
        if (fd >= 0) {
            epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
        }
    }
}

bool EpollExecutor::Run(const CollPlan& plan) {
    if (plan.channels.empty()) {
        return true;
    }
    if (!EnsureEpoll()) {
        return false;
    }

    std::vector<PlanTask*> current(plan.channels.size(), nullptr);
    std::vector<size_t> task_index(plan.channels.size(), 0);
    size_t active = 0;

    auto abort_plan = [&]() {
        for (PlanTask* task : current) {
            if (task) {
                UnregisterTask(*task);
            }
        }
    };

    auto start = [&](size_t slot) -> bool {
        while (task_index[slot] < plan.channels[slot].tasks.size()) {
            PlanTask& task = const_cast<PlanTask&>(plan.channels[slot].tasks[task_index[slot]]);
            Topology* topo = task.topology.get();
            if (!topo || !topo->AllreduceInit(task)) {
                LOG_ERROR("EpollExecutor failed to init task on channel {}", slot);
                return false;
            }
            ++task_index[slot];
            if (topo->AllreduceDone(task)) {
                continue;
            }
            current[slot] = &task;
            ++active;
            return RegisterTask(static_cast<int>(slot), task);
        }
        return true;
    };

    for (size_t i = 0; i < plan.channels.size(); ++i) {
        if (!start(i)) {
            abort_plan();
            return false;
        }
    }

    std::vector<struct epoll_event> events(plan.channels.size() * 2);
    while (active > 0) {
        int ready = epoll_wait(epoll_fd_, events.data(), static_cast<int>(events.size()), -1);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            LOG_ERROR("epoll_wait failed: {}", strerror(errno));
            abort_plan();
            return false;
        }

        for (int e = 0; e < ready; ++e) {
            uint32_t tag = events[e].data.u32;
            size_t slot = tag / 2u;
            PlanTask* task = current[slot];
            if (!task) {
                continue;
            }

            size_t side = tag % 2u;
            Transport* transport = (side == 0) ? task->recv_transport.get() : task->send_transport.get();
            if (!transport || (events[e].events & transport->GetPollEvents()) == 0) {
                continue;
            }

            Topology* topo = task->topology.get();
            CollEvent op = (side == 0) ? CollEvent::Readable : CollEvent::Writable;
            if (!topo->AllreduceStep(*task, op)) {
                LOG_ERROR("EpollExecutor step failed on channel {}", slot);
                abort_plan();
                return false;
            }
            if (!topo->AllreduceDone(*task)) {
                continue;
            }

            --active;
            UnregisterTask(*task);
            current[slot] = nullptr;
            if (!start(slot)) {
                abort_plan();
                return false;
            }
        }
    }

    return true;
}
