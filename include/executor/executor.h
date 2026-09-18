#pragma once

#include "types.h"

class Executor {
public:
    virtual ~Executor() = default;

    virtual bool Run(const CollPlan& plan) = 0;
    virtual void Shutdown() = 0;
};
