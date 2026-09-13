#include <sched.h>
#include "polling_executor.h"
#include "logger.h"

bool PollingExecutor::StartFrontTask(size_t slot, const CollPlan& plan, std::vector<PlanTask*>& current,
                                     std::vector<size_t>& task_index, size_t& active) {
    const ChannelPlan& channel = plan.channels[slot];
    if (task_index[slot] >= channel.tasks.size()) {
        return false;
    }
    PlanTask& task = const_cast<PlanTask&>(channel.tasks[task_index[slot]]);
    if (!task.topology || !task.topology->AllreduceInit(task)) {
        LOG_ERROR("PollingExecutor failed to init task on channel {}", channel.channel_id);
        return false;
    }
    current[slot] = &task;
    ++task_index[slot];
    ++active;
    return true;
}

bool PollingExecutor::Run(const CollPlan& plan) {
    if (plan.channels.empty()) {
        return true;
    }

    // One outstanding task per channel; never wait on any fd, just keep stepping. The
    // executor owns each channel's cursor, so it steps tasks through a mutable reference.
    std::vector<PlanTask*> current(plan.channels.size(), nullptr);
    std::vector<size_t> task_index(plan.channels.size(), 0);
    std::vector<bool> channel_ok(plan.channels.size(), true);
    size_t active = 0;

    for (size_t i = 0; i < plan.channels.size(); ++i) {
        if (plan.channels[i].tasks.empty()) {
            continue;
        }
        if (!StartFrontTask(i, plan, current, task_index, active)) {
            return false;
        }
    }

    while (active > 0) {
        bool progressed = false;
        for (size_t i = 0; i < plan.channels.size(); ++i) {
            PlanTask* task = current[i];
            if (!task) {
                continue;
            }
            Topology* topo = task->topology.get();

            size_t before_send = task->state.send_progress;
            size_t before_recv = task->state.recv_progress;
            int before_phase = task->state.phase;

            // No real readiness event exists when polling: drive whichever transfer is
            // still unfinished (both may advance, send first).
            if (!task->state.send_done && !topo->AllreduceStep(*task, CollEvent::Writable)) {
                channel_ok[i] = false;
            }
            if (channel_ok[i] && !task->state.recv_done && !topo->AllreduceStep(*task, CollEvent::Readable)) {
                channel_ok[i] = false;
            }
            if (!channel_ok[i]) {
                --active;
                current[i] = nullptr;
                continue;
            }

            if (task->state.send_progress != before_send || task->state.recv_progress != before_recv ||
                task->state.phase != before_phase) {
                progressed = true;
            }

            if (!topo->AllreduceDone(*task)) {
                continue;
            }

            channel_ok[i] = channel_ok[i] && topo->AllreduceSucceeded(*task);
            --active;
            current[i] = nullptr;
            progressed = true;
            if (channel_ok[i] && task_index[i] < plan.channels[i].tasks.size()) {
                if (!StartFrontTask(i, plan, current, task_index, active)) {
                    channel_ok[i] = false;
                }
            }
        }

        if (!progressed) {
            sched_yield();
        }
    }

    for (size_t i = 0; i < plan.channels.size(); ++i) {
        if (!channel_ok[i]) {
            return false;
        }
    }
    return true;
}
