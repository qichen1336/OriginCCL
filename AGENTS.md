# AGENTS.md

Guidance for AI coding agents (and the humans driving them) working on OriginCCL.

This file is an *orientation map*, not the full rulebook. It holds the cross-layer
rules, build/test thresholds, style, and commit conventions. Per-layer guidance on how
to write and modify each layer's code — its responsibilities, invariants, design space,
and modification rules — lives in `docs/layers/`.
When this file and a layer doc disagree, **the layer doc wins** — and please fix this file.

## Overview

OriginCCL is a C++17 collective-communication library. It follows the two-phase design
used by NCCL: a bootstrap phase that exchanges endpoint information, then a data-plane
phase that moves buffers. The code is deliberately modular — `Planner`, `CollPlan`,
`MultiThreadExecutor`, `Topology` — so that algorithms and execution strategies can
evolve independently.

We want code that is correct and clear, and that still holds up on 1, 2, and 4 ranks
after moving to another machine. Not code that happens to work in the one configuration
it was first tried in.

## Architecture

Two chains, deliberately independent.

- **Execution:** `CollTask` → `Planner::Plan` → `CollPlan` → `ChannelPlan` → `PlanTask`
  → `MultiThreadExecutor` → `Topology::AllReduce`
- **Initialization:** `Communicator::Init` → `TopologyRing` → `Bootstrap` (exchange
  `NodeInfo`) → `InitChannels` (per-channel send/recv transports)

These boundaries are **not enforced by the compiler**. Everything ships as one shared
library, and nothing mechanically stops a lower layer from reaching upward. The
separation holds only if you keep it.

## Project Layout

Key directories. Per-file responsibilities live in each layer doc's "file map"
section under `docs/layers/`, not here.

| Path | Purpose |
|------|---------|
| `include/` | Public headers, one per layer (`transport.h`, `topology.h`, `planner.h`, `communicator.h`, `bootstrap.h`, `types.h`, `utils.h`, `logger.h`, …) |
| `src/` | Implementation, one `.cpp` per layer |
| `include/executor/`, `src/executor/` | The only layer in its own subdirectory; headers are included with a qualified path, e.g. `#include "executor/executor.h"` |
| `tests/` | Test harness (`test_allreduce`, `test_local_info`) |
| `scripts/` | `run_all_executors.sh`, `run_tests.sh` |
| `docs/layers/` | Per-layer docs: responsibilities, invariants, design space, file map, modification rules |
| `build*/` | Generated build directories (see Do NOT touch) |

## Code Conventions

`.clang-format` is the single source of truth for formatting — read it rather than
relying on a summary here.

- Naming: classes and methods `PascalCase`; data members and locals `snake_case`;
  private members take a trailing `_`; constants are `k` plus `PascalCase`
  (`kMinBytesPerChannel`).
- `SortIncludes` is off and include blocks are preserved. Do not reorder existing
  `#include` groups.
- Logging goes through the `LOG_DEBUG` / `LOG_INFO` / `LOG_WARN` / `LOG_ERROR` macros
  with fmt `{}` placeholders.
- Public entry points return `bool` and do not throw across the API. On failure, log
  with `LOG_ERROR` first, then `return false`.
- Prefer modern C++17 idioms where they make intent clearer: `std::optional` for
  values that may be absent, `std::variant` for type-safe alternatives, smart pointers
  for ownership, and `std::promise` / `std::future` / `std::async` for async results.
  The project is C++17, so avoid C++20-only features (`std::span`, `std::jthread`,
  concepts, ranges). The rule above still holds: public entry points return `bool`, so
  use `std::optional` and friends in internals.
- Match the style of the file you are editing. The tree is not perfectly uniform — for
  example, `Communicator`'s private members carry no trailing `_` while
  `MultiThreadExecutor`'s do. Don't use that as an excuse for a drive-by refactor.

### Naming

| Name | Meaning |
|------|---------|
| `OriginCCL` | the project |
| `OCCL_` | environment-variable prefix (`OCCL_MASTER_ADDR`, `OCCL_MASTER_PORT`) |
| `originccl` | CMake target and shared library name |
| `ORIGIN_CCL_SOURCES` | source list variable in `CMakeLists.txt` |

`n_channels` defaults to 4 when unset or non-positive.

## Golden Rules (Do and Don't)

Layer-specific rules live in the per-layer docs, not here. Before touching a layer's
code, read its doc under `docs/layers/` (`transport.md`, `topology.md`, `executor.md`,
`planner.md`, `communicator.md`). Each file has four sections: core responsibility
boundary, invariants & design space, file map, and modification rules.

Cross-layer rules:

- **Shut down before closing sockets.** `Communicator::Finalize` must call
  `executor.Shutdown()` and join the workers *before* closing channel transports.
  A worker must never touch a socket that is already gone. (Details in `executor.md`
  and `communicator.md`.)
- **Confirm before adding a class, especially a base class.** New classes — and above all
  new abstract base classes — add architecture surface and coupling. Never introduce one
  without explicit approval first. Prefer free functions or extending an existing type.
- **Prefer the simplest design and implementation.** Make the smallest change that solves
  the problem. Do not do large-scale rewrites, speculative abstractions, or drive-by
  refactors unless explicitly asked. Write the direct, readable version: straight-line
  logic beats an extra layer of functions. A helper called once, with no meaning of its
  own, is indirection, not abstraction. Do not add `try`/`catch` for errors you cannot
  handle, and do not use exceptions as control flow. Public entry points log with
  `LOG_ERROR` and `return false`; internals return `bool` or `std::optional`.
- **Prioritize the main path over defensive coding.** Get the core functionality
  working first — that matters more than guarding every edge case. Avoid
  over-engineering error handling, fallbacks, and defensive checks: don't add retries,
  speculative validation, or recovery paths for failures that can't happen in normal
  use. Handle the errors that can actually occur, log them, and fail cleanly; don't
  build speculative defenses.
- **Do not add comment blocks — good code is self-documenting.** Let names carry the
  intent; `src/` and `include/` lean toward no comments at all. One short line is fine
  for a non-obvious *why* — a past deadlock, a kernel or system-call quirk, a
  load-bearing ordering constraint. Not fine: restating what the code plainly does, or
  narrating the change you just made. If a block feels necessary, the fix is usually a
  better name or a smaller function.
- **When adding code, reconsider what it can replace.** Before adding a feature, check
  whether existing code can be removed or simplified. Constrain the project's
  complexity: avoid over-engineering and over-encapsulation when there is no need.
- Read the relevant `docs/layers/` file before inventing a new pattern.

### Performance (data path)

- Pay attention to execution performance. Trading a bounded amount of memory for speed
  is acceptable where it is measured to help.
- Don't add allocations, locks, or branches to the critical send/recv path without a
  clear, measured justification.
- `Topology` is shared: every channel worker calls `AllReduce` on the same instance
  concurrently. Keep it free of mutable member state — no cached scratch buffers, no
  member counters. Scratch space belongs in locals.

### Do NOT touch

- **`build/`** is generated. Note that `build/third_party/fmt/` is a stale leftover from
  an earlier layout; there is no `third_party/` in the source tree. Do not treat it as a
  source of truth, and do not edit anything under it.
- **Do not re-vendor fmt.** fmt and the MPI runtime are system dependencies, resolved via
  `find_package(fmt REQUIRED)` (fmt ≥ 9 required — `logger.h` uses `fmt::format_string`
  compile-time checks). They are not part of this tree.
- **Do not weaken a test to make it pass.** When a test fails, the default assumption is
  that the code is wrong. Fix the code, or report the bug.

## Build and Verification

**Tier 1 — build.** The build enables `-Wall -Wextra` but *not* `-Werror`, so warnings
will not stop you. Read them, and do not add new ones.

```sh
cmake -S . -B build
cmake --build build -j"$(nproc)"
```

The executor is chosen at compile time via `OCCL_EXECUTOR`
(`multi_thread` / `epoll` / `polling` / `reactor`; default `multi_thread`). An invalid
value is a cmake `FATAL_ERROR`. Build the other variants in separate build dirs:

```sh
cmake -S . -B build-epoll -DOCCL_EXECUTOR=epoll
cmake -S . -B build-polling -DOCCL_EXECUTOR=polling
cmake -S . -B build-reactor -DOCCL_EXECUTOR=reactor
```

Artifacts: `build/liboriginccl.so`, `build/tests/test_allreduce`,
`build/tests/test_local_info`.

**Tier 2 — Markdown-only changes** need no build.

**Tier 3 — single-process smoke test**, which takes the no-data-plane path:

```sh
build/tests/test_allreduce 0 1
```

**Tier 4 — multi-rank.** These are *different code paths*, not just more of the same:
at 2 ranks the ring degenerates (`prev == next`), which exercises the split between
`Channel::send` and `Channel::recv`.

```sh
mpirun -np 2 build/tests/test_allreduce
mpirun -np 4 build/tests/test_allreduce
```

For architecture or concurrency changes, run the full executor matrix:

```sh
scripts/run_all_executors.sh    # four executors × 1/2/4 ranks
```

Finish with `git diff --check`. After a clean build, `git status` must be clean; an
untracked build product means a missing `.gitignore` entry, and committing the artifact
is never the fix.

**Do not report untested code as verified.** State which tiers you actually ran.

## Testing

The test harness reads `OMPI_COMM_WORLD_RANK` and `OMPI_COMM_WORLD_SIZE`; other launchers
(MPICH, PMI, Slurm) are not supported. Without those variables it falls back to
`argv[1]` = rank and `argv[2]` = world_size, and then the ranks must be started by hand.

Rank `i` fills its buffer with `i + 1`, so SUM is `ws * (ws + 1) / 2` and AVG is
`(ws + 1) / 2`. The planner enables one channel per 64 KiB, so small messages only ever
exercise a single channel.

## Commit Conventions

Short, lower-case, plain English — `add fmt`, `modify channels`, `root ip and port from
env`. No Conventional Commits prefixes, and no AI-tool attribution. If you fix an
unrelated bug along the way, land it as its own commit.

If a change alters an architecture boundary, a build command, or the way tests are run,
update the corresponding layer doc under `docs/layers/` in the same change. Those docs
are the authority on how to write and modify each layer.
