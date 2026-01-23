# TiForth Milestone 24: deprecate legacy agg operators; run agg on ara-style pipeline/task

- Author(s): TBD
- Last Updated: 2026-01-23
- Status: Implemented
- Related design: `docs/design/2026-01-14-tiforth.md`
- Depends on: MS23 (`docs/design/2026-01-14-tiforth-milestone-23-ara-rewrite-logical-physical-pipeline.md`)

## Motivation

MS23 rewrote TiForth runtime around ara’s core abstractions (`pipeline::OpOutput`, `pipeline::PipelineTask`,
`task::TaskGroup`/`TaskStatus`) while keeping the old public “host API” (`Pipeline`/`Plan` + `Task::Step()`)
working via legacy-operator adapters.

Aggregation is still expressed in terms of **legacy operators** (`TransformOp`/`SinkOp`/`SourceOp` returning
`OperatorStatus`), which forces:

- the continued existence of legacy operator surfaces for TiForth aggregation,
- adapter glue (`Legacy*Adapter`) as a required runtime layer,
- “EOS as nullptr batch + kHasOutput” quirks to flow through the new pipeline runtime.

This milestone removes aggregation’s dependency on the legacy operator interfaces by porting TiForth agg operators
to the **ara-style pipeline operator interfaces**:

- `pipeline::PipeOp` (streaming transform / breaker build),
- `pipeline::SinkOp` (breaker backend finalize),
- `pipeline::SourceOp` (breaker result source).

## Goals

- Replace `ArrowComputeAggTransformOp` and `HashAgg*` legacy operators with ara-style pipeline operator
  implementations (no `OperatorStatus`/`TransformOp` usage in agg codepaths).
- Allow TiFlash and TiForth unit tests to build agg plans without using legacy operator adapters for agg.
- Keep aggregation semantics unchanged (schema validation, type/metadata contract, collation behavior, etc.).

## Non-goals

- Porting all non-agg operators (filter/projection/join/sort) to pipeline ops in the same milestone.
- Removing the legacy host wrappers (`tiforth::Pipeline`/`tiforth::Plan`/`tiforth::Task`) immediately; they can
  remain as compatibility layers while non-agg operators migrate.

## Design

### Plan construction: allow pipeline ops in `PlanBuilder`

Extend `tiforth::PlanBuilder` to accept stage operator factories for ara-style pipeline ops:

- stage source: `std::unique_ptr<pipeline::SourceOp>`
- stage pipes: `std::unique_ptr<pipeline::PipeOp>`
- stage sink: `std::unique_ptr<pipeline::SinkOp>`

Legacy operator factories remain supported for non-ported operators and are wrapped via the existing legacy adapters
inside `tiforth::Task`.

### HashAgg breaker on ara pipeline

Keep `HashAggContext` as the shared aggregation state (Grouper + grouped hash_* kernels).

Port the breaker operators to pipeline ops:

- build: a `pipeline::PipeOp` that consumes input batches and seals a per-thread partial state on drain
  (no output batches emitted).
- finalize: a `pipeline::SinkOp` whose backend task merges partials into the shared `HashAggContext` and calls
  `Finalize()`.
- result: a `pipeline::SourceOp` that streams `HashAggContext` output batches (with optional slice bounds).

### ArrowComputeAgg on ara pipeline

Port `ArrowComputeAggTransformOp` into a `pipeline::PipeOp`:

- consume batches in `Pipe()`
- close input and emit output batches in `Drain()` (supports multiple output batches)
- keep the existing stable-dictionary option behavior

## Definition of Done

- TiForth unit tests pass with agg implemented as pipeline ops (no legacy-operator agg codepaths).
- TiFlash builds with `TIFLASH_ENABLE_TIFORTH` and TiForth-related gtests pass.
- Design docs updated to mark MS24 as implemented and record final file locations.

## Implementation Notes (Final)

- TiForth plan building now supports ara-style pipeline ops (in addition to legacy operator factories):
  - `tiforth::PlanBuilder::{SetStageSource,AppendPipe,SetStageSink}` accept `pipeline::SourceOp` / `pipeline::PipeOp` /
    `pipeline::SinkOp` factories.
  - `tiforth::Task` stage wrapper composes legacy transforms (via `LegacyTransformPipeOp`) and native pipeline `PipeOp`s
    in a single stage when needed.
- Aggregation is no longer implemented on the legacy `TransformOp`/`OperatorStatus` surface:
  - `ArrowComputeAggTransformOp` is a `pipeline::PipeOp` (consume in `Pipe()`, produce in `Drain()`).
  - HashAgg breaker operators are `pipeline` ops:
    - `HashAggTransformOp` (`PipeOp`) consumes input, seals partial state on `Drain()` (no output batches emitted).
    - `HashAggMergeSinkOp` (`SinkOp`) finalizes via `Backend()` task (stage sink consumes no batches).
    - `HashAggResultSourceOp` (`SourceOp`) streams output batches (supports slice bounds).
- Primary integration points updated to build agg stages using pipeline factories:
  - TiFlash planner (`PhysicalAggregation`) and TiFlash parity/bench tests now call `PlanBuilder::AppendPipe` /
    `SetStageSource` / `SetStageSink` for aggregation stages.
