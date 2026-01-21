# TiForth Milestone 22: TaskGroup/PipelineTask Model (ara-inspired, zero sync in operators)

- Author(s): TBD
- Last Updated: 2026-01-22
- Status: Planned / In Progress
- Related design: `docs/design/2026-01-14-tiforth.md`
- Reference implementation: `https://github.com/zanmato1984/ara` (notably `src/ara/pipeline/op/op.h`, `src/ara/pipeline/pipeline_task.h`, `src/ara/task/task_group.h`, and the hash agg sketch `src/sketch/sketch_aggregation_test.cpp`)

## Design Goal (Hard Rule)

**No synchronization inside operators.**

- No `mutex` / `atomic` / `condition_variable` inside any operator (`SourceOp`/`TransformOp`/`SinkOp`) implementation, or inside any operator-owned shared state.
- Operators are **single-threaded** and can assume that each operator instance is driven by at most one thread at a time.
- Any required coordination (barrier, merge/finalize, handoff between pipelines) must be expressed via the **scheduler model** (TaskGroups / stage boundaries), not by operator internals.

Rationale:

- This is the key advantage of the pipeline model: fully decouple the parallelizable portion from the synchronization portion, with explicit, testable boundaries.
- Synchronization hidden inside operators makes reasoning/debugging hard and blocks future scheduler features (backpressure, cooperative yielding, priority, cancellation).

## Problem With Current TiForth Shape

TiForth’s current public surface (`PipelineExec`, `Task`, `Plan`) is host-driven and supports breaker-style plans, but it does not yet encode the “frontend parallel + backend merge” split as a first-class scheduling concept.

As a result, it is easy to accidentally implement cross-task “parallel” tests by sharing a breaker state across tasks and adding locks inside the breaker state/operator to make it pass. This violates the design goal above and must be avoided.

## ara Reference Model (What We Mirror)

In `ara`:

- Operators (`SourceOp`/`PipeOp`/`SinkOp`) expose:
  - a per-thread callable (`Source`/`Pipe`/`Sink`/`Drain`) that operates on **thread-local state** keyed by `ThreadId`,
  - **TaskGroups** (`Frontend()` and optional `Backend()`) which the scheduler runs at explicit boundaries.
- A `PipelineTask` owns a pipeline + channels (DOP) and is scheduled via TaskGroups.
- Breakers (e.g. hash agg) follow a clean split:
  - parallel phase: each thread updates its own local aggregator state (no sync),
  - barrier phase: a single task merges thread-local states and finalizes output.

See `src/sketch/sketch_aggregation_test.cpp`:

- `HashAggregateSink::Consume(thread_id, ...)` updates `local_states_[thread_id]`.
- `HashAggregateSink::Frontend()` returns a single-task group which calls `Merge()` and `Finalize()`.

## Proposed TiForth Scheduling Abstractions

### `TaskGroup`

Introduce a TiForth-native equivalent of ara `TaskGroup`:

- `TaskGroup` = (`TaskFn`, `num_tasks`, optional continuation, optional notify-finish hook)
- `TaskGroups` = list of `TaskGroup`

The scheduler (TiFlash) owns:

- mapping of `task_id -> thread`,
- resuming/yielding/backpressure integration,
- stage dependency execution.

TiForth provides:

- a default test runner that can execute TaskGroups with a simple thread pool (for gtests).

### `PipelineTask`

Introduce a TiForth `PipelineTask` which:

- represents one pipeline with DOP channels,
- routes batches through `SourceOp` → `TransformOps` → `SinkOp`,
- uses per-channel **thread-local state** owned by operators (indexed by channel id),
- exposes TaskGroups:
  - a parallel “run” group (DOP tasks), and
  - optional backend groups required by breaker operators.

### Operator evolution (minimal changes)

We do not need ara’s LogicalPipeline/PhysicalPipeline split initially.

But we do need an explicit way for operators to participate in TaskGroups:

- allow `SinkOp` (and some `TransformOp`) to expose **backend task groups** used for merge/finalize.
- allow per-channel state indexing without adding atomics.

## Migration Plan (Incremental)

1. Add the new abstractions (`TaskGroup`, `PipelineTask`) alongside existing `Task`/`Plan` APIs (do not break TiFlash integration).
2. Add a gtest-only runner that executes:
   - frontend DOP tasks in parallel,
   - then backend single-task merge groups,
   - then downstream dependent pipelines.
3. Migrate breaker operators, starting with HashAgg:
   - partial build phase produces thread-local `PartialState` (one per channel),
   - backend merge phase merges all partial states into the convergent breaker state,
   - result phase reads immutable finalized output with partitioned source instances (no shared offsets).
4. Remove any operator-internal synchronization that was introduced for tests.
5. Extend to other breaker-style operators (join build/probe, sort, exchange) as needed.

## Definition of Done

- TiForth enforces “no sync in operators” in code review and tests (no mutex/atomic usage in operator codepaths).
- HashAgg parallel gtests demonstrate:
  - parallel build phase,
  - explicit backend merge stage,
  - parallel result consumption,
  - all without operator locks.

