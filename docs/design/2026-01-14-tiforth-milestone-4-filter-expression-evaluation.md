# TiForth Milestone 4: Filter + Predicate Expression Evaluation (Plan + Tests)

- Author(s): TBD
- Last Updated: 2026-01-18
- Status: Implemented
- Related design: `docs/design/2026-01-14-tiforth.md`
- Depends on: `docs/design/2026-01-14-tiforth-milestone-3-pipeline-framework.md`, `docs/design/2026-01-14-tiforth-milestone-3-projection-scalar-functions.md`

## Scope

This milestone adds the first predicate-driven operator:

- `Filter` as a `tiforth::TransformOp` on Arrow `RecordBatch`
- expand scalar expression evaluation to cover the minimal boolean + comparison set needed by `Filter`
- add tests, including an initial TiFlash↔TiForth equivalence plan

## Goals

- Implement `FilterTransformOp`:
  - input: `arrow::RecordBatch` (or end-of-stream marker `nullptr`)
  - output: filtered `arrow::RecordBatch` with the same schema as input
  - SQL WHERE semantics: keep rows where predicate is `TRUE`; drop rows where predicate is `FALSE` or `NULL`
- Support TiDB "truthy" predicates (TiFlash `convertBool` behavior):
  - if predicate does not evaluate to `bool`, coerce to boolean via MySQL-style truthiness:
    - numeric: `value != 0`
    - string/binary: parse as number (invalid => 0), then `value != 0`
    - `NULL` remains `NULL` (dropped by WHERE)
- Extend expression evaluation enough to build typical TiFlash filter predicates:
  - comparisons (e.g. `equal`, `less`, `less_equal`, `greater`, `greater_equal`)
  - boolean ops with 3-valued logic (`and_kleene`, `or_kleene`, `invert`)
- Add unit tests to validate filter correctness and NULL behavior.
- Define a differential testing strategy to compare TiFlash execution vs TiForth for the same logical pipeline.

## Non-goals

- Full TiFlash function parity (type casts, timezone/date, collations, decimals).
- Complex predicate compilation / CSE / JIT.
- Multi-input predicates (join conditions).

## Proposed Public API

Add a new operator:

- `tiforth::FilterTransformOp`
  - constructor: `explicit FilterTransformOp(ExprPtr predicate)`

No changes to the existing `tiforth::Expr` IR are required in the common path; `Expr::Call` uses Arrow compute function names.

## Semantics

- Input batches use the pipeline end-of-stream marker convention:
  - `nullptr` means EOS; `FilterTransformOp` must forward it and return `OperatorStatus::kHasOutput`.
- Predicate evaluation result:
  - may produce boolean values (scalar or array); scalars broadcast to `batch.num_rows()`.
  - if non-boolean: coerce to boolean using "truthy" rules above.
  - `NULL` in predicate is treated as “row not selected” (SQL WHERE). Implement by using Arrow `FilterOptions::NullSelectionBehavior::DROP`.

## Implementation Plan

### 1) `FilterTransformOp`

- Add `libs/tiforth/include/tiforth/operators/filter.h` and `libs/tiforth/src/tiforth/operators/filter.cc`.
- Implementation outline:
  - On non-null batch:
    - evaluate predicate via `EvalExprAsArray(input, *predicate, &exec_context_)`
    - validate predicate type is `bool`
    - for each input column:
      - `arrow::compute::Filter(column, predicate, FilterOptions::Defaults(), &exec_context_)`
    - cache and reuse output schema pointer (`output_schema_`) for stable output schemas across batches.

### 2) Expression evaluation additions (minimal)

No new IR nodes are needed; we rely on Arrow compute kernels and function names.

- Confirm / document minimal kernel names used in tests:
  - comparisons: `equal`, `less`, `less_equal`, `greater`, `greater_equal`
  - boolean: `and_kleene`, `or_kleene`, `invert`

### 3) Tests

TiForth unit tests (`libs/tiforth/tests/`):

- `tiforth_filter_test`:
  - filter with a comparison predicate, validate output rows and schema stability
  - predicate producing NULLs, validate NULL rows are dropped
  - predicate as scalar true/false/null, validate broadcast behavior

TiFlash integration tests (guarded by `TIFLASH_ENABLE_TIFORTH`):

- Add a small differential test that:
  - constructs a simple TiFlash DAG containing a filter-like predicate
  - translates the DAG into a TiForth pipeline via TiForth APIs only
  - runs TiForth and compares with a TiFlash reference output (initially: hard-coded expected output; later: execute the TiFlash pipeline and compare)

## Definition of Done

- `FilterTransformOp` implemented + exported.
- TiForth tests pass:
  - `cmake -S libs/tiforth -B libs/tiforth/build-debug -DTIFORTH_BUILD_TESTS=ON`
  - `ninja -C libs/tiforth/build-debug`
  - `ctest --test-dir libs/tiforth/build-debug`
- TiFlash test(s) added/updated under `TIFLASH_ENABLE_TIFORTH` and pass in a TiFlash build with `ENABLE_TIFORTH=ON`.
