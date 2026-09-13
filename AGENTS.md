# AGENTS.md

Guidance for AI coding agents (and the humans driving them) working on OriginCCL.

This file is an *orientation map*, not the full rulebook. The authoritative description
of the architecture, the build, the test workflow, and known failure modes lives in
[`.github/skills/originccl-project/SKILL.md`](.github/skills/originccl-project/SKILL.md).
When this file and that skill disagree, **the skill wins** — and please fix this file.

OriginCCL is a C++17 collective-communication library. It follows the two-phase design
used by NCCL: a bootstrap phase that exchanges endpoint information, then a data-plane
phase that moves buffers. The code is deliberately modular — `Planner`, `CollPlan`,
`MultiThreadExecutor`, `Topology` — so that algorithms and execution strategies can
evolve independently.

We want code that is correct and clear, and that still holds up on 1, 2, and 4 ranks
after moving to another machine. Not code that happens to work in the one configuration
it was first tried in.

## Structure

Two chains, deliberately independent. See the skill for the diagrams.

- **Execution:** `CollTask` → `Planner::Plan` → `CollPlan` → `ChannelPlan` → `PlanTask`
  → `MultiThreadExecutor` → `Topology::AllReduce`
- **Initialization:** `Communicator::Init` → `TopologyRing` → `Bootstrap` (exchange
  `NodeInfo`) → `InitChannels` (per-channel send/recv transports)

These boundaries are **not enforced by the compiler**. Everything ships as one shared
library, and nothing mechanically stops a lower layer from reaching upward. The
separation holds only if you keep it.

## Golden rules (the things agents most often get wrong)

- **Algorithms belong in `Topology`.** `TopologyRing::AllReduce` owns the copy,
  ReduceScatter, AllGather, and AVG stages. `Planner::Plan` only slices the tensor and
  assembles tasks — it must not expand algorithm steps. `MultiThreadExecutor` only
  validates and dispatches on `task.func`.
- **No callbacks in the plan.** `PlanTask` and `CollPlan` carry data, not behavior. Do
  not introduce `std::function`, `execute`, `pre_execute`, or `post_execute`.
- **Executor workers are lazy and persistent.** Channel `i` is always run by worker `i`.
  A newly created worker must be initialized with the *current* `batch_id_`, otherwise
  it re-runs a batch that already completed.
- **Shut down before closing sockets.** `Communicator::Finalize` must call
  `executor.Shutdown()` and join the workers *before* closing channel transports.
  A worker must never touch a socket that is already gone.
- **Transports must outlive the plan.** That is why `PlanTask` holds
  `shared_ptr<Transport>`. Keep it that way.
- **Edge direction is a deadlock invariant.** Each channel needs a prev and a next edge.
  The code decides with `rank < peer`: the lower rank connects, the higher rank accepts.
  Changing this reintroduces startup deadlock.
- **Call-level exclusion is the caller's job.** The executor does not serialize
  concurrent `AllReduce` calls on the same `Communicator`. Do not assume it does.
- **Keep the plan types in sync.** `PlanTask`, `ChannelPlan`, and `CollPlan` live in
  `include/types.h`. Changing a field means updating the planner and the executor both.
- **Confirm before adding a class, especially a base class.** New classes — and above all
  new abstract base classes — add architecture surface and coupling. Never introduce one
  without explicit approval first. Prefer free functions or extending an existing type.
- **Prefer the simplest design and implementation.** Make the smallest change that solves
  the problem. Do not do large-scale rewrites, speculative abstractions, or drive-by
  refactors unless explicitly asked.

## Performance (data path)

- Don't add allocations, locks, or branches to the critical send/recv path without a
  clear, measured justification.
- `Topology` is shared: every channel worker calls `AllReduce` on the same instance
  concurrently. Keep it free of mutable member state — no cached scratch buffers, no
  member counters. Scratch space belongs in locals.

## Do NOT touch

- **`build/`** is generated. Note that `build/third_party/fmt/` is a stale leftover from
  an earlier layout; there is no `third_party/` in the source tree. Do not treat it as a
  source of truth, and do not edit anything under it.
- **Do not re-vendor fmt.** fmt and the MPI runtime are system dependencies, resolved via
  `find_package(fmt REQUIRED)` (fmt ≥ 9 required). They are not part of this tree.
- **Do not weaken a test to make it pass.** When a test fails, the default assumption is
  that the code is wrong. Fix the code, or report the bug.

## Build and verify

**Tier 1 — build.** The build enables `-Wall -Wextra` but *not* `-Werror`, so warnings
will not stop you. Read them, and do not add new ones.

```sh
cmake -S . -B build
cmake --build build -j"$(nproc)"
```

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

Finish with `git diff --check`. After a clean build, `git status` must be clean; an
untracked build product means a missing `.gitignore` entry, and committing the artifact
is never the fix.

**Do not report untested code as verified.** State which tiers you actually ran.

## Tests

The test harness reads `OMPI_COMM_WORLD_RANK` and `OMPI_COMM_WORLD_SIZE`; other launchers
(MPICH, PMI, Slurm) are not supported. Without those variables it falls back to
`argv[1]` = rank and `argv[2]` = world_size, and then the ranks must be started by hand.

Rank `i` fills its buffer with `i + 1`, so SUM is `ws * (ws + 1) / 2` and AVG is
`(ws + 1) / 2`. The planner enables one channel per 64 KiB, so small messages only ever
exercise a single channel.

## Style

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
- Comments: `src/` and `include/` lean toward no comments — prefer names that carry the
  intent. This is a preference, not a ban.
- Prefer modern C++17 idioms where they make intent clearer: `std::optional` for
  values that may be absent, `std::variant` for type-safe alternatives, smart pointers
  for ownership, and `std::promise` / `std::future` / `std::async` for async results.
  The project is C++17, so avoid C++20-only features (`std::span`, `std::jthread`,
  concepts, ranges). The rule above still holds: public entry points return `bool`, so
  use `std::optional` and friends in internals.

## Names

| Name | Meaning |
|------|---------|
| `OriginCCL` | the project |
| `OCCL_` | environment-variable prefix (`OCCL_MASTER_ADDR`, `OCCL_MASTER_PORT`) |
| `originccl` | CMake target and shared library name |
| `ORIGIN_CCL_SOURCES` | source list variable in `CMakeLists.txt` |

`n_channels` defaults to 4 when unset or non-positive.

## Commits

Short, lower-case, plain English — `add fmt`, `modify channels`, `root ip and port from
env`. No Conventional Commits prefixes, and no AI-tool attribution. If you fix an
unrelated bug along the way, land it as its own commit.

## Documentation

If a change alters an architecture boundary, a build command, or the way tests are run,
update `.github/skills/originccl-project/SKILL.md` in the same change. That file is the
authority; this one is the map.

An agent hook (`.github/hooks/skill.json`) enforces the habit: once a session has changed
100+ lines under `src/`, `include/`, `tests/`, `CMakeLists.txt`, or `scripts/` without
touching `.github/skills/`, it injects a reminder naming the reference file for each layer
that changed. It only reminds — it never blocks. Editing any file under `.github/skills/`
resets the counter.

## When in doubt

- Match the style of the file you are editing. The tree is not perfectly uniform — for
  example, `Communicator`'s private members carry no trailing `_` while
  `MultiThreadExecutor`'s do. Don't use that as an excuse for a drive-by refactor.
- Read the skill before inventing a new pattern.

## Agent skills

### Issue tracker

Issues and specs live as markdown files under `.scratch/<feature-slug>/` in this repo.
See `docs/agents/issue-tracker.md`.

### Triage labels

Five labels, default names: `needs-triage`, `needs-info`, `ready-for-agent`,
`ready-for-human`, `wontfix`. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: one `CONTEXT.md` and `docs/adr/` at the repo root. See
`docs/agents/domain.md`.
