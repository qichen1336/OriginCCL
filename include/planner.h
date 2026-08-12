#pragma once

#include "types.h"

class Communicator;

class Planner {
public:
    CollPlan Plan(const Communicator& comm, const CollTask& task) const;
};
