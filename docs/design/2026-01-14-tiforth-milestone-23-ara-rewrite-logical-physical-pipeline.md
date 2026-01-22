# TiForth Milestone 23: ara-based runtime rewrite (Logical/PhysicalPipeline + TaskGroup scheduler)

- Author(s): TBD
- Last Updated: 2026-01-23
- Status: Implemented
- Related design: `docs/design/2026-01-14-tiforth.md`
- Reference: `https://github.com/zanmato1984/ara` (`DESIGN.md`, `src/ara/pipeline/*`, `src/ara/task/*`)

## Motivation

Current TiForth has a workable host-driven execution surface (`Pipeline`/`Plan` + `Task::Step()` and `TaskState`),
but it does not fully encode ara’s core abstractions:

- `LogicalPipeline`/`PhysicalPipeline` split + compilation (implicit sources become stage boundaries).
- `OpOutput` as the single flow-control protocol between operators and the pipeline runtime.
- `Task`/`TaskStatus`/`TaskGroup` as the schedulable interface (so schedulers stay operator-agnostic).
- `Resumer`/`Awaiter` as the unified “blocked” mechanism (instead of separate IO/wait/notify states).

This milestone rewrites TiForth runtime around ara’s model (while intentionally **ignoring ara Meta/Observer**
features as over-designed for TiForth right now).

## Hard Rules (carry over from MS22)

- **No synchronization inside operators**: no `mutex`/`atomic`/`condition_variable` in any operator codepaths or
  operator-owned shared state. Parallelism and barriers must be expressed via `TaskGroup` boundaries.
- **Arrow-native code style**: internal APIs use `arrow::Status` / `arrow::Result<T>` and Arrow helper macros.

## Target Core Abstractions (names preserved)

### Common

- `Options`: per-query execution knobs (batch sizing, mini-batch sizing, etc).
- `QueryContext`: holds `Options` and other per-query handles needed by the runtime (e.g. memory pool).

### Task module (`tiforth::task`)

- `Resumer`: “waker” object, can be triggered externally (callbacks / host).
- `Awaiter`: scheduler wait handle (what the scheduler blocks/polls on).
- `TaskStatus`: `Continue` / `Blocked(awaiter)` / `Yield` / `Finished` / `Cancelled`.
- `Task`: callable unit the scheduler invokes repeatedly (small-step protocol).
- `TaskGroup`: N parallel instances of the same `Task` + optional continuation; the explicit barrier primitive.

### Pipeline module (`tiforth::pipeline`)

- `OpOutput`: the finite flow-control protocol between operator streaming functions and the pipeline runtime.
- Operator interfaces:
  - `SourceOp` exposes `Source()` and optional `Frontend()` / `Backend()` task groups.
  - `PipeOp` exposes `Pipe()` / `Drain()` and optional `ImplicitSource()` (stage splitting hook).
  - `SinkOp` exposes `Sink()` and optional `Frontend()` / `Backend()` task groups and optional `ImplicitSource()`.
- `LogicalPipeline`: a pipeline description (channels share a sink).
- `PhysicalPipeline`: compiled stage unit (channels have concrete sink; contains implicit sources owned by the stage).
- `CompilePipeline`: compile `LogicalPipeline -> PhysicalPipelines` by splitting on implicit sources.
- `PipelineTask`: turns one `PhysicalPipeline` into a schedulable `task::Task` by mapping `OpOutput -> TaskStatus`.

## Batch Type

Ara uses `arrow::compute::ExecBatch`. TiForth’s existing boundary with TiFlash is Arrow `RecordBatch`.
For this rewrite:

- Define TiForth “Batch” for the pipeline runtime as `std::shared_ptr<arrow::RecordBatch>`.
- Operators may internally use Arrow compute kernels (often via `ExecBatch`); conversions are operator-local.

## Compatibility / Migration Strategy

This rewrite is allowed to be incremental as long as:

- the new ara-style runtime is the **primary** runtime for new code paths, and
- old APIs (`Engine` / `Pipeline` / `Plan` / `Task`) either:
  - become thin wrappers over the new runtime, or
  - are moved under an explicit `tiforth::stash` namespace for reference and fully replaced.

Concrete migration steps:

1. Add the ara-style headers/sources (task + pipeline modules) and a minimal test scheduler.
2. Introduce adapters to wrap existing TiForth operators into `SourceOp` / `PipeOp` / `SinkOp` without changing their
   semantics; remove any operator-internal sync as required by MS22.
3. Rewrite TiForth pipeline execution on top of `PipelineTask` + `TaskGroup`, then re-implement:
   - `Pipeline::CreateTask()` / `Pipeline::MakeReader()`
   - breaker plans (`PlanBuilder`) as explicit stage graphs of `TaskGroup`s (or via implicit sources once available).
4. Port existing TiForth unit tests to the new runtime (keep parity coverage).
5. Update TiFlash integration to either:
   - keep using the wrapper `Task` API, or
   - drive TiForth via `TaskGroup` boundaries directly (preferred long-term).

## Definition of Done

- All TiForth tests pass with the new runtime as the default execution path.
- TiFlash builds with `TIFLASH_ENABLE_TIFORTH`, and TiFlash TiForth gtests pass.
- Operators contain no synchronization primitives; barriers only exist at `TaskGroup` boundaries.
- Design docs updated:
  - `docs/design/2026-01-14-tiforth.md` (milestone list + architecture notes)
  - this MS23 doc updated to `Implemented` with final file locations and API notes.

## Implementation Notes (Final)

- Core runtime modules (TiForth repo):
  - `include/tiforth/task/*` (`Resumer`/`Awaiter`/`TaskStatus`/`Task`/`TaskGroup`)
  - `include/tiforth/pipeline/*` (`OpOutput`, `SourceOp`/`PipeOp`/`SinkOp`, `LogicalPipeline`, `PhysicalPipeline`, `PipelineTask`)
  - `src/tiforth/pipeline/*` (`CompilePipeline` + `PipelineTask` runtime)
- Compatibility wrappers:
  - `src/tiforth/task.cc`: `tiforth::Task` now drives `pipeline::PipelineTask` and bridges legacy operator blocked states
    (`OperatorStatus::{kIOIn,kIOOut,kWaiting,kWaitForNotify}`) into ara-style `Resumer`/`Awaiter` while preserving the
    host-facing `TaskState` API.
- Key bug fix surfaced by MS23 porting:
  - `HashAggPartialState` now carries an `ExecContext` keepalive so Arrow `Grouper`/`KernelState` objects don’t retain
    dangling `ExecContext*` pointers when partial states outlive the build-task operator instance (previously a flaky
    segfault in parallel hash agg tests).
- TiFlash integration note:
  - `include/tiforth/options.h` avoids including `arrow/compute/util.h` (which defines `ROTL`) to prevent a hard
    `-Werror` macro conflict with TiFlash `SipHash.h`.
