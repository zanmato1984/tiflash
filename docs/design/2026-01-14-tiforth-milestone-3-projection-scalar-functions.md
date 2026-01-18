# TiForth Milestone 3 (Part B): Projection + Minimal Scalar Functions (Detailed Steps)

- Author(s): TBD
- Last Updated: 2026-01-18
- Status: Implemented (extended arithmetic coverage)
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

Start with a small set of functions implemented by delegating to Arrow compute, plus a small set
of TiForth custom kernels to preserve TiFlash/TiDB semantics for tricky types:

- numeric (common path via Arrow compute): `add`, `subtract`, `multiply`, `divide`
- numeric (TiForth custom kernels + compile-time rewrite when any argument is a decimal logical type):
  - `tiforth.decimal_add` / `tiforth.decimal_subtract`
  - `tiforth.decimal_multiply`
  - `tiforth.decimal_divide` (TiFlash decimal inference + truncation)
  - `tiforth.decimal_tidb_divide` (TiDB rounding + div-by-zero => NULL)
  - `tiforth.decimal_modulo` (div-by-zero => NULL)
- numeric / bitwise (TiForth custom kernels matching TiFlash/TiDB semantics):
  - bitwise: `bitAnd`, `bitOr`, `bitXor`, `bitNot`, `bitShiftLeft`, `bitShiftRight` (returns `uint64`)
  - unary: `abs` (signed int -> unsigned; `abs(INT64_MIN)` errors), `negate` (unsigned -> signed nextSize)
  - integer division: `intDiv` (division-by-zero errors), `intDivOrZero` (division-by-zero returns 0)
  - modulo: `modulo` (division-by-zero yields NULL; decimal cases are rewritten to `tiforth.decimal_modulo`)
  - other: `gcd`, `lcm`
- boolean: (optional) `and`, `not` (if Arrow compute mappings are straightforward)

The function set is intentionally tiny for Milestone 3B; it expands in later milestones.

Implementation note (current code): TiForth does not implement its own function registry. Instead,
each `Engine` owns an **Arrow** `arrow::compute::FunctionRegistry` overlay (parent = Arrow global
registry) with TiForth custom kernels registered:

- internal dispatch-only names (e.g. `tiforth.decimal_*`)
- TiFlash-compatible names (e.g. `abs`, `modulo`, `intDiv`, `bitAnd`, ...)

Some TiFlash-compatible names intentionally overlap Arrow builtins (notably `abs`/`negate`), so TiForth registers
them with `allow_overwrite=true` to preserve TiFlash semantics when evaluated via Arrow compute.

TiForth does **not** override Arrow builtin function names via `MetaFunction` because Arrow compute
`Expression::Bind` cannot bind meta-functions (they have no kernels). Instead, TiForth compiles its
own `tiforth::Expr` IR into an Arrow compute `Expression`, and does **compile-time dispatch**:

- rewrite calls like `add/equal/hour` into `tiforth.decimal_add`, `tiforth.collated_equal`,
  `tiforth.mytime_hour` when inputs require TiFlash/TiDB semantics,
- rewrite calls like `subtract/multiply/divide/tidbDivide/modulo` into the corresponding
  `tiforth.decimal_*` kernels when at least one argument is a decimal logical type,
- attach `FunctionOptions` (collation id / packed-MyTime type) derived from Arrow field metadata,
- bind once (`Expression::Bind`) and execute via `ExecuteScalarExpression`.

Operators cache bound expressions per input schema so per-batch evaluation avoids repeated kernel
lookup/init.

Current organization: custom kernels live under `libs/tiforth/src/tiforth/functions/scalar/` grouped
by Arrow compute categories (e.g. arithmetic/comparison/temporal).

## Implementation Plan

### 1) Add expression types and evaluation helper

- Add `libs/tiforth/include/tiforth/expr.h` defining the IR.
- Add `libs/tiforth/src/tiforth/expr.cc` implementing:
  - evaluation (`EvalExpr(batch, expr) -> arrow::Result<arrow::Datum>`)
  - broadcasting scalars to arrays when needed (`MakeArrayFromScalar`)
  - Arrow compute `Expression` compilation/binding (`detail/expr_compiler.cc`) with the engine registry
  - execution (`arrow::compute::ExecuteScalarExpression`) for the common scalar-expression path
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

Add arithmetic-focused tests:

- TiForth gtests for decimal arithmetic inference + semantics (mixed decimal/int inputs, overflow,
  div-by-zero semantics for `tidbDivide` / `modulo`, rounding parity).
- TiForth gtests for TiFlash-style numeric/bitwise semantics (`abs/negate`, `intDiv/intDivOrZero`, integer `modulo`,
  bitwise and shifts, `gcd/lcm`).
- TiFlash differential gtests: run the same expressions via TiFlash `executeFunction(...)` and via
  TiForth Projection/Expr on Arrow batches and assert identical types + values (including errors vs NULL).

## Validation / Definition of Done

- Standalone TiForth:
  - `cmake -S libs/tiforth -B libs/tiforth/build-debug -DTIFORTH_BUILD_TESTS=ON`
  - `ninja -C libs/tiforth/build-debug`
  - `ctest --test-dir libs/tiforth/build-debug`
- TiFlash parity (ENABLE_TIFORTH=ON):
  - `ninja -C cmake-build-tiflash-tiforth-debug gtests_dbms`
  - `cmake-build-tiflash-tiforth-debug/dbms/gtests_dbms '--gtest_filter=FunctionTest.TiForthArithmeticParity*'`
- Examples updated if needed for a “projection hello” pipeline.
