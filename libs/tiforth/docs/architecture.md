# TiForth architecture

TiForth is an **Arrow-native** compute library: it consumes and produces `arrow::RecordBatch` and relies on
Arrow’s compute runtime for a large part of expression evaluation.

This document describes the main moving pieces and how they fit together. For how to *use* the APIs, see
`pipeline_apis.md`.

## Core concepts

### Data model: `arrow::RecordBatch`

- **Unit of exchange**: TiForth operators process `arrow::RecordBatch` objects.
- **Schema stability**: a `tiforth::Task` validates that all input batches have the same schema
  (including field metadata).
- **End-of-stream (EOS)**: EOS is represented as a `nullptr` `std::shared_ptr<arrow::RecordBatch>`
  inside the pipeline execution loop.

### Engine: host integration point

Public API: `tiforth::Engine` (`libs/tiforth/include/tiforth/engine.h`).

An `Engine` is the “host context” for execution. It owns/points to:

- `arrow::MemoryPool*`: all TiForth allocations should flow through the host-provided pool.
- `tiforth::SpillManager`: an abstraction for externalizing intermediate data (default is deny-spill).
- `arrow::compute::FunctionRegistry`: an engine-local registry overlay where TiForth registers its
  semantics-sensitive kernels (collation-aware string comparisons, decimal add parity, packed-MyTime helpers).

### Pipeline + Task: composing and running operators

Public APIs:

- `tiforth::PipelineBuilder` / `tiforth::Pipeline` (`libs/tiforth/include/tiforth/pipeline.h`)
- `tiforth::Task` (`libs/tiforth/include/tiforth/task.h`)

Model:

- A `Pipeline` is an immutable sequence of **transform operators**.
- A `Task` is a single executable instance of that sequence with internal input/output queues.

Execution is **incremental** and driven by `Task::Step()`:

- `kNeedInput`: provide more input (push a batch, or close input).
- `kHasOutput`: consume outputs via `Task::PullOutput()`.
- `kFinished`: no more outputs will be produced.
- `kBlocked`: reserved for future async/blocking operators.

Internally, `Task` runs a `PipelineExec` which repeatedly:

1. asks the sink whether it needs input (`SinkOp::Prepare`)
2. lets transforms flush buffered outputs (`TransformOp::TryOutput`, in reverse order)
3. reads a batch from the source (`SourceOp::Read`)
4. pushes the batch through transforms (`TransformOp::Transform`)
5. writes the batch to the sink (`SinkOp::Write`)

EOS is treated like an “output” batch to allow transforms/sinks to flush and finalize.

### Operator abstraction

Public API: `tiforth::Operator` and derived `SourceOp` / `TransformOp` / `SinkOp`
(`libs/tiforth/include/tiforth/operators.h`).

Key points:

- Operators communicate using `tiforth::OperatorStatus`:
  - `kNeedInput`: operator cannot progress without more input.
  - `kHasOutput`: operator produced a batch (or forwarded EOS).
  - `kFinished`: operator is complete.
- Transform operators must correctly handle `*batch == nullptr` (EOS marker).

TiForth ships a small set of built-in transform operators (filter/projection/join/agg/sort); see
`operators_and_functions.md`.

### Expressions and functions

Public APIs:

- Expression IR: `tiforth::Expr` (`libs/tiforth/include/tiforth/expr.h`)
- “Bind once, execute many”: `tiforth::CompiledExpr` (`libs/tiforth/include/tiforth/compiled_expr.h`)

TiForth expression compilation uses Arrow compute `Expression::Bind` against an input schema and an engine-local
function registry. During compilation TiForth may rewrite certain calls to preserve TiDB/MySQL semantics, driven
by Arrow field metadata (see `type_mapping_tidb_to_arrow.md`).

### Logical types via Arrow field metadata

Public API: `tiforth::LogicalType` helpers (`libs/tiforth/include/tiforth/type_metadata.h`).

TiForth uses Arrow `Field::metadata()` to preserve semantics that cannot be represented by Arrow’s physical type
alone (notably: decimals, packed date/time, and collated strings).

The metadata contract (keys, values, and the TiDB->Arrow mapping rules) is specified in
`type_mapping_tidb_to_arrow.md`.

## Layering and independence

TiForth is designed to be embedded into arbitrary hosts:

- it depends on Arrow (core + compute)
- it does **not** depend on any host’s internal headers/types
- semantic hooks are expressed as:
  - Arrow field metadata
  - an engine-owned `FunctionRegistry` overlay
  - host-provided `MemoryPool` / `SpillManager`
