# TiForth Milestone 3 (Part A): Pipeline Framework Port (TiFlash Shape, Arrow Batches)

- Author(s): TBD
- Last Updated: 2026-01-16
- Status: Draft
- Related design: `docs/design/2026-01-14-tiforth.md`
- Related milestone: `docs/design/2026-01-14-tiforth-milestone-2-recordbatch-plumbing.md`

## Scope Clarification

This milestone ports the *framework / shape* of TiFlash's pipeline execution model into TiForth:

- `Operator` / `SourceOp` / `TransformOp` / `SinkOp`
- `PipelineExec` (source → transforms → sink push model)
- `PipelineExecBuilder` and (minimal) group support as a future extension point

but implements **only the common synchronous path** (CPU, no IO/await/notify).

All TiForth compute remains Arrow-native:

- inputs/outputs are `std::shared_ptr<arrow::RecordBatch>`
- stream boundary can be `arrow::RecordBatchReader` or push/pull batches (Task API)

This milestone is intentionally about **core abstractions + integration shape**, not about porting real operators yet.

## Goals

- Keep TiForth independent from TiFlash (`dbms/`): no includes, no linking.
- Provide a TiFlash-shaped pipeline execution framework inside TiForth, but using Arrow batches.
- Export key APIs so TiFlash can:
  - translate an in-memory TiFlash pipeline DAG (in a unit test) into a TiForth pipeline using TiForth APIs only,
  - run the translated TiForth pipeline end-to-end on Arrow batches.
- Keep the existing TiForth host-driven `Task` stepping API intact.

## Non-goals

- No fine-grained scheduling/events/reactor port.
- No IO/await/notify operator states (reserved for later; may return `NotImplemented`).
- No multi-source or multi-sink DAG execution (single linear chain only).
- No real TiFlash operator semantics (Projection/Filter/etc deferred to later milestones).

## Public API (TiForth)

### Operator status

Expose an operator-level status enum similar to TiFlash:

- `kNeedInput`: downstream needs an input batch to proceed
- `kHasOutput`: operator produced a batch (via out-parameter)
- `kFinished`: end-of-stream reached

### Operator interfaces

Define Arrow equivalents of TiFlash operator interfaces:

- `class SourceOp` with `Read(std::shared_ptr<arrow::RecordBatch>* out)`
- `class TransformOp` with:
  - `TryOutput(std::shared_ptr<arrow::RecordBatch>* out)` (optional buffered output)
  - `Transform(std::shared_ptr<arrow::RecordBatch>* batch)` (in-place pointer rewrite)
- `class SinkOp` with:
  - `Prepare()`
  - `Write(std::shared_ptr<arrow::RecordBatch> batch)`

For Milestone 3A, only `kNeedInput/kHasOutput/kFinished` are supported.

### Pipeline execution

`PipelineExec` mirrors TiFlash's push model:

- fetch a batch from buffered transform output or from source
- apply transforms in forward order
- write into sink
- return a pipeline-level status so the host can drive stepping

### Integration with Task API

`tiforth::Task` remains the host boundary:

- host pushes batches into the task (or provides an input reader)
- task returns `TaskState` (`kNeedInput`, `kHasOutput`, `kFinished`)
- host pulls output batches

Internally, `Task::Step()` drives `PipelineExec`.

## Supported “Common Path” Execution

The only supported execution path in this milestone:

- single source
- zero or more transforms
- single sink
- synchronous (no blocking IO)
- end-of-stream represented as `Read(out=nullptr)` or `Write(batch=nullptr)` and results in `kFinished`

## TiFlash Unit Test (Translation)

Add a TiFlash unit test guarded by `TIFLASH_ENABLE_TIFORTH`:

- Build a minimal TiFlash pipeline DAG using TiFlash operator framework types (source/transform/sink).
- Translate that DAG into a TiForth pipeline by instantiating TiForth operators via exported TiForth APIs.
- Execute TiForth pipeline on a small Arrow `RecordBatch`.
- Assert pass-through output equivalence.

The test is a shape/interop validation: it does not attempt to validate TiFlash compute semantics.

## Definition of Done

- TiForth builds + tests pass standalone (`TIFORTH_BUILD_TESTS=ON`).
- TiFlash `gtests_dbms` builds with `ENABLE_TIFORTH=ON`.
- New TiFlash translation test passes (guarded by `TIFLASH_ENABLE_TIFORTH`).

