#pragma once

#include "types.h"

class Communicator;

class RingExecutor {
public:
    static bool Run(Communicator& comm, const CollPlan& plan, const CollTask& task);
};
