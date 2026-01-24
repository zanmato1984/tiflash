# TiForth Milestone 25: ara-style operator rewrite (remove legacy operator surface)

- Author(s): TBD
- Last Updated: 2026-01-23
- Status: Implemented
- Related design: `docs/design/2026-01-14-tiforth.md`
- Reference: `https://github.com/zanmato1984/ara` (`DESIGN.md`, `src/ara/pipeline/op/*`)

## Motivation

MS23/24 introduced ara’s runtime core (`pipeline::OpOutput`/`PipelineTask`, `task::{Task,TaskGroup}`) and ported
aggregation to ara-style pipeline ops, but TiForth still carries a **legacy operator surface**:

- `tiforth/operators.h`: `OperatorStatus` + `{Source,Transform,Sink}Op` classes
- `tiforth/pipeline_exec.h`: legacy pipeline executor
- `tiforth::PipelineBuilder` builds pipelines out of legacy `TransformOp`s
- `tiforth::Task` wrapper still contains legacy adapters (`Legacy*Adapter`)

This prevents TiForth operators from being “fully ara-like” (operators express streaming semantics only; any
parallelism/barriers are encoded by `TaskGroup` boundaries and/or stage splitting, not by operator-internal protocol
variants like `OperatorStatus`).

## Goals

- **Delete** the legacy operator surface (`tiforth/operators.h`, `tiforth/pipeline_exec.h`) and remove all runtime
  dependencies on it (including legacy adapters in `tiforth::Task`).
- Port all TiForth concrete operators to ara-style pipeline ops:
  - projection, filter, sort, hash join, pass-through, pilot, arrow-compute agg, hash agg.
- Make **HashAgg** match ara’s intended breaker shape as closely as possible:
  - per-thread partial state (indexed by `ThreadId`)
  - merge + finalize runs in a single-thread task group (barrier after build)
  - result production expressed as a source stage (`SinkOp::ImplicitSource` + stage graph)
  - keep TiForth-specific extensions: collation-aware grouping + small-string optimizations.
- Keep TiForth independent from TiFlash; update TiFlash integration only at the API boundary.

## Non-goals

- Implementing ara Meta/Observer features (names/trace hooks) if they create significant surface area churn.
- Changing TiForth batch boundary away from `arrow::RecordBatch` (TiForth remains RecordBatch-based at the public API).
- Reworking TiFlash scheduling strategy (TiFlash still drives TiForth via `Task` API).

## Design

### 1) Operator surface: only `pipeline::{SourceOp,PipeOp,SinkOp}`

- Remove `tiforth/operators.h` and migrate all operators to implement:
  - `pipeline::PipeOp` for streaming transforms (projection/filter/sort/join/etc.)
  - `pipeline::SourceOp` for result sources
  - `pipeline::SinkOp` for sinks / breakers
- Remove `tiforth/pipeline_exec.h` (legacy executor) and any tests/examples that depend on it.

### 2) PipelineBuilder: build a single-stage logical pipeline (no legacy transforms)

Update `tiforth::PipelineBuilder` to accept `pipeline::PipeOp` factories and compile directly into a pipeline plan
consumable by `tiforth::Task`:

- `PipelineBuilder::AppendPipe(Factory)` where Factory returns `std::unique_ptr<pipeline::PipeOp>`
- `Pipeline::CreateTask()` constructs:
  - task input source op
  - the configured pipe ops
  - task output sink op

### 3) HashAgg: ara-like breaker without operator synchronization

HashAgg is a two-stage breaker expressed via the stage graph:

- **Build stage**:
  - `HashAggSinkOp` consumes input batches.
  - `HashAggState` owns `dop`-indexed thread-local partial state:
    - one `arrow::compute::Grouper` per `ThreadId`
    - one grouped hash aggregate kernel state per `(agg, ThreadId)`
- **Merge/finalize**:
  - `HashAggSinkOp::Backend` runs as a single-task `TaskGroup` (barrier after build) and performs:
    - grouper transposition merge across thread-locals
    - kernel state merge and finalize into a single output `arrow::RecordBatch`
- **Result stage**:
  - `HashAggSinkOp::ImplicitSource` returns `HashAggResultSourceOp`.
  - `HashAggResultSourceOp` streams the finalized output batch and supports slice bounds (so the host can parallelize
    output by creating multiple sources).

This mirrors ara’s `hash_aggregate.cpp` (thread-local build + single-thread merge/finalize + result source) while
preserving TiForth’s custom `Grouper` selection (collation + small-string).

### 4) Blocked-state pilot

Re-implement the Pilot operator on pipeline ops by returning `pipeline::OpOutput::Blocked(resumer)` and adapting
`tiforth::Task` host API to preserve `TaskState::{kIOIn,kIOOut,kWaiting,kWaitForNotify}` semantics without the legacy
operator surface.

## Definition of Done

- TiForth builds and all TiForth tests pass.
- TiFlash builds with `TIFLASH_ENABLE_TIFORTH` and TiForth-related gtests pass.
- No remaining includes/references to `tiforth/operators.h` or `tiforth/pipeline_exec.h` in TiForth runtime/operator
  codepaths.
- Design docs updated to mark MS25 implemented and record final file locations.
