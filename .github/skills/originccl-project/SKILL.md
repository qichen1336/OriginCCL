---
name: originccl-project
description: 'OriginCCL 写/改代码的指引与约束入口：改某层代码前先读 reference/ 对应文件，拿到「怎么写、怎么改、哪些不能动」（职责边界、不变式、设计区间、修改原则），辅以架构、文件、排障等背景知识。触发：修改/实现 bootstrap、planner、executor、communicator、topology_ring、transport_tcp 等层代码，或排障。关键词：OriginCCL、AllReduce、Ring、集合通信、执行计划、executor、epoll、channel、fmt、mpirun、OpenMPI、TCP、bootstrap。'
---

# OriginCCL 项目 Skill（写/改代码指引与约束入口）

**本 skill 以「怎么写代码、怎么改代码」的指引与约束为主，以知识库（架构、文件、排障、环境）为辅。** 改某层代码前，先读 `reference/` 对应文件，拿到该层的职责边界、不变式、设计区间、修改原则，再动手。构建/测试/风格/提交等跨层规则见 `AGENTS.md`（常驻上下文，无需触发）。

C++17 集合通信库，单机多进程 TCP AllReduce，参考 NCCL 分 bootstrap / 数据面两步建连。本 skill 随仓库分发，路径均相对仓库根。

## 改哪层代码，读哪个文件

| 要改动的层 / 任务 | 加载文件 |
|------|------|
| Transport 非阻塞收发、GetFd | [reference/transport.md](reference/transport.md) |
| Topology 状态机、算法、CollOpState | [reference/topology.md](reference/topology.md) |
| Executor（多线程/epoll/轮询）、事件驱动 | [reference/executor.md](reference/executor.md) |
| Planner、PlanTask/CollPlan 数据模型 | [reference/planner.md](reference/planner.md) |
| Communicator 初始化、建连、生命周期、Bootstrap | [reference/communicator.md](reference/communicator.md) |

## reference 四段模板

每个 reference 文件固定四段，顺序如下。**核心两段是改代码时必读；辅助两段给背景。**

| 段 | 性质 | 回答 | 何时更新 |
|----|------|------|---------|
| `## 核心职责边界` | 辅助背景 | 这层负责什么 / 不负责什么 | 职责转移时 |
| `## 不变式与设计区间` | **核心** | 什么绝对不能动（不变式）+ 什么可自主决定（设计区间） | 算法/状态机/并发/生命周期变化时 |
| `## 文件介绍` | 辅助背景 | 有哪些文件、各干什么 | 文件增删/改名/职责变化时 |
| `## 修改原则` | **核心** | 改这层守什么、禁止回退什么 | 踩新坑、发现新规则时 |

`## 不变式与设计区间` 内分两个子块：

- `### 不变式`：硬约束，违反 = bug/死锁。写/改代码前逐条核对。
- `### 设计区间`：自由度，实现者可自主决定的范围。改动落在区间内无需特别说明；落在区间外须先确认。

## 自我更新流程

改代码 = 同 commit 更新对应 reference。步骤：

1. 改某层代码前先读该层 reference 的四段。
2. 改动落定后，判断命中了哪几段，同步更新：
   - 职责转移 → 更新「核心职责边界」。
   - 算法/状态机/并发/生命周期变化 → 更新「不变式」；新增自由度 → 补「设计区间」。
   - 文件增删/改名 → 更新「文件介绍」。
   - 踩新坑/发现新规则 → 更新「修改原则」。
3. 只记录源码检查或实测验证过的事实；不记录未实现的设计提议。
4. 删除被新实现取代的旧描述，避免互相矛盾；保持精炼，优先更新既有条目，不追加流水账。

### 自动提醒（`.github/hooks/skill.json`）

`SessionStart` 记录 baseline commit，`PostToolUse` 按 session 累计度量。当 `src/`、`include/`、
`tests/`、`CMakeLists.txt`、`scripts/` 的累计改动（增+删）达到 100 行，且 `.github/skills/` 全程未被
改动时，hook 会向对话注入一段提醒，点名改动所在层应更新的 reference 文件，并提示按四段检查。阈值可用
环境变量 `OCCL_SKILL_SYNC_LINES` 覆盖。

一旦更新了本目录下任一文件，计数即重置；每次提醒后也重置，因此是「每满一批提醒一次」，不会逐次刷屏。
这只是提醒，不阻塞操作——更新了 reference（哪怕是先补一句）它就会安静下来。