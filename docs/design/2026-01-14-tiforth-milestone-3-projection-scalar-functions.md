# TiForth Milestone 3 (Part B): Projection + Minimal Scalar Functions (Detailed Steps)

- Author(s): TBD
- Last Updated: 2026-01-16
- Status: Draft
- Related design: `docs/design/2026-01-14-tiforth.md`
- Related milestone: `docs/design/2026-01-14-tiforth-milestone-3-pipeline-framework.md`

## Scope Clarification

This milestone adds the first real compute operator to TiForth:

- `Projection` as a `TransformOp` operating on Arrow `RecordBatch`.
- a minimal scalar expression model and a small initial function set.

The goal is to unlock a realistic “single-input → single-output” DAG path using the newly ported pipeline framework.

## Goals

- Implement a `ProjectionTransformOp` (`tiforth::TransformOp`) that:
  - selects existing columns and/or computes new columns from scalar expressions,
  - produces a new `arrow::RecordBatch` with an output schema derived from the projection list.
- Provide a minimal scalar expression IR owned by TiForth and evaluated via Arrow compute.
- Implement a small initial function set sufficient for early pipelines and tests.
- Keep TiForth independent from TiFlash (`dbms/`).

## Non-goals

- Full TiFlash function parity (collation, decimal/date/time quirks, etc).
- Filter / expression short-circuiting / boolean tri-valued semantics parity (deferred).
- Codegen/JIT or vectorized custom kernels (deferred).
- Multi-input expressions (e.g. join condition evaluation) and multi-stream DAGs.

## Public API Additions (Proposed)

### Expression IR

Introduce a small TiForth-owned scalar expression tree:

- `FieldRef` (by name or index)
- `Literal` (`std::shared_ptr<arrow::Scalar>`)
- `Call` (`function_name` + `args[]`)

Evaluation contract:

- Given an input `arrow::RecordBatch`, an expression evaluates to an `arrow::Datum`.
- Scalars are broadcast to arrays (length = input batch rows) during projection materialization.

### Projection description

Expose a projection list type:

- `struct ProjectionExpr { std::string name; Expr expr; }`
- `ProjectionTransformOp` constructed from `std::vector<ProjectionExpr>`

### Minimal scalar function set

Start with a small set of functions implemented by delegating to Arrow compute:

- numeric: `add` (and potentially `subtract`, `multiply` as follow-ups)
- boolean: (optional) `and`, `not` (if Arrow compute mappings are straightforward)

The function set is intentionally tiny for Milestone 3B; it expands in later milestones.

## Implementation Plan

### 1) Add expression types and evaluation helper

- Add `libs/tiforth/include/tiforth/expr.h` defining the IR.
- Add `libs/tiforth/src/tiforth/expr.cc` implementing:
  - evaluation (`EvalExpr(batch, expr) -> arrow::Result<arrow::Datum>`)
  - broadcasting scalars to arrays when needed (`MakeArrayFromScalar`)
  - Arrow compute invocation (`arrow::compute::CallFunction`)
  - lazy `arrow::compute::Initialize()` to ensure built-in kernels are registered
  - chunked array results: concatenate chunks into a single `arrow::Array` (common scalar kernels produce arrays)

### 2) Implement `ProjectionTransformOp`

- Add `libs/tiforth/include/tiforth/operators/projection.h` and implementation in `libs/tiforth/src/tiforth/operators/projection.cc`.
- `TransformImpl` behavior:
  - if input `batch == nullptr` (end-of-stream marker), return `kHasOutput` with `batch == nullptr`
  - evaluate each projection expression to an array datum
  - build output schema (fields with computed types, names from projection list)
  - replace `*batch` with the new `arrow::RecordBatch`

### 3) Wire into pipeline builder usage

- No new pipeline abstractions required: `ProjectionTransformOp` is appended via `PipelineBuilder::AppendTransform(...)`.
- Add a small helper factory for tests/examples (optional).

### 4) Tests

Add TiForth gtests validating:

- column selection projection (reorder + duplicate fields)
- computed projection using `add` on int32 (and literal broadcast)
- schema stability across multiple batches

## Validation / Definition of Done

- Standalone TiForth:
  - `cmake -S libs/tiforth -B libs/tiforth/build-debug -DTIFORTH_BUILD_TESTS=ON`
  - `ninja -C libs/tiforth/build-debug`
  - `ctest --test-dir libs/tiforth/build-debug`
- Examples updated if needed for a “projection hello” pipeline.
