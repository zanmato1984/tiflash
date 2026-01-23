# TiForth Milestone 26: public API refactor (logical pipeline + task groups only)

- Author(s): TBD
- Last Updated: 2026-01-23
- Status: In Progress
- Related design: `docs/design/2026-01-14-tiforth.md`
- Reference: `https://github.com/zanmato1984/ara` (`DESIGN.md`, `src/ara/pipeline/*`)

## Motivation

MS1-25 delivered a working TiForth + TiFlash integration, but TiForth still exposed (and TiFlash still used) a
`Plan`/`PlanBuilder`, a `Pipeline`/`PipelineBuilder`, and a push/pull `tiforth::Task` wrapper. These concepts overlap
with the ara execution model and make it harder to keep the public API small and ara-like.

This milestone makes TiForth a pure “operator + pipeline runtime” library:

- Hosts build `pipeline::LogicalPipeline` directly using operator objects (`SourceOp`/`PipeOp`/`SinkOp`).
- TiForth compiles a logical pipeline into an *ordered list of* `task::TaskGroup`.
- Hosts schedule/execute task groups with their own threading model and executor.

## Goals

- Remove from TiForth public surface:
  - `tiforth::{Plan,PlanBuilder}`
  - `tiforth::{Pipeline,PipelineBuilder}`
  - the push/pull `tiforth::Task` wrapper and its `RecordBatchReader` adapter
- Expose as the only public orchestration/execution surface:
  - `pipeline::{LogicalPipeline,SourceOp,PipeOp,SinkOp}`
  - `pipeline::CompileToTaskGroups(...)`
  - `task::{Task,TaskGroup,TaskGroups,TaskContext,TaskStatus,Awaiter,Resumer}`
- Keep breaker semantics ara-like, especially HashAgg:
  - streaming consume in a parallel `PipelineTask` group
  - merge/finalize in a single-task barrier group (`SinkOp::Frontend`)
  - results provided by a source operator created by the host (typically via `SinkOp::ImplicitSource`)

## Non-goals

- Designing the final C ABI for this new API (follow-up milestone once MS26 stabilizes).
- Providing a scheduler/executor inside TiForth (hosts own scheduling).

## Design

### 1) Compilation API: `LogicalPipeline -> TaskGroups`

Add a compilation API that returns an ordered task-group list:

- `pipeline::CompileToTaskGroups(PipelineContext, LogicalPipeline, dop) -> task::TaskGroups`
- Output ordering:
  - Per physical stage: SourceOp frontend groups → stage PipelineTask group (`num_tasks=dop`) → SourceOp backend groups
  - After all stages: SinkOp frontend groups → optional SinkOp backend group

This keeps stage barriers explicit at `TaskGroup` boundaries and keeps TiForth scheduler-agnostic.

### 2) Host integration: host-defined source/sink operators

Instead of push/pull tasks:

- Input is represented by a host-defined `SourceOp` (e.g. a `BlockInputStream` reader or an in-memory vector).
- Output is represented by a host-defined `SinkOp` (e.g. collect into a queue or write to a host stream).

The host is responsible for:

- providing `task::TaskContext` factories (`resumer_factory`, `any_awaiter_factory`, etc.)
- executing returned task groups sequentially and tasks within a group in parallel

### 3) Breakers without `Plan`

Without a TiForth `Plan` graph, breaker sequencing is an explicit host responsibility:

- Run the build pipeline task groups.
- Run breaker `SinkOp::Frontend()` groups (merge/finalize barrier).
- Then build and run a downstream logical pipeline that reads from breaker state (often using `ImplicitSource()`).

This is intentionally “dumb” inside TiForth: TiForth compiles one logical pipeline at a time; hosts own the DAG.

## Definition of Done

- TiForth headers install without legacy Plan/Pipeline/Task wrapper surfaces.
- TiForth tests and TiFlash gtests are updated to the new API and remain green.
- HashAgg merge/finalize is expressed as a stage-barrier task group (`Frontend`) and matches ara semantics.
- `.codex/progress/daemon.md` updated; changes committed and pushed in both repos.

