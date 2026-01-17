# TiForth Milestone 10: Expr Compilation + Arrow `ExecuteScalarExpression`

- Author(s): TBD
- Last Updated: 2026-01-17
- Status: Implemented
- Related design: `docs/design/2026-01-14-tiforth.md`
- Depends on: MS3 (Expr IR) + MS8 (type metadata + custom kernels)

## Problem

TiForth started with a small `tiforth::Expr` IR evaluated by recursively calling Arrow compute
`CallFunction`. To hook TiFlash/TiDB semantics, earlier iterations used Arrow compute `MetaFunction`
overrides for names like `add` / `equal`.

This has two problems:

- Arrow compute `Expression::Bind` cannot bind `MetaFunction` (no kernels), so we cannot leverage
  Arrow's `ExecuteScalarExpression` path.
- Per-batch evaluation repeatedly does function lookup + kernel init, adding avoidable overhead.

## Goals

- Compile `tiforth::Expr` into Arrow compute `Expression`.
- Bind once per input schema (`Expression::Bind`) and execute via `ExecuteScalarExpression`.
- Keep TiFlash/TiDB semantics by compile-time dispatch:
  - decimal `add` -> `tiforth.decimal_add`
  - collated string comparisons `equal/less/...` -> `tiforth.collated_*` + collation options
  - packed MyTime extraction `hour/minute/second` -> `tiforth.mytime_*` + MyTime options
- Avoid overriding Arrow builtin names in the registry (no `MetaFunction` overrides).

## Implementation Notes

- New internal module `tiforth/detail/expr_compiler.{h,cc}`:
  - builds an Arrow `Expression` from `tiforth::Expr`
  - derives `FunctionOptions` from Arrow field metadata (`tiforth.*` metadata keys)
  - binds against the engine-owned registry overlay
- Operators cache bound expressions per input schema:
  - `ProjectionTransformOp`, `FilterTransformOp`, `HashAggTransformOp`.

## Files

- `libs/tiforth/include/tiforth/detail/expr_compiler.h`
- `libs/tiforth/src/tiforth/detail/expr_compiler.cc`
- `libs/tiforth/src/tiforth/expr.cc`
- `libs/tiforth/src/tiforth/operators/{projection,filter,hash_agg}.cc`
- `libs/tiforth/src/tiforth/functions/scalar/{arithmetic/add,comparison/collated_compare,temporal/mytime}.cc`

## Validation

- `ninja -C libs/tiforth/build-debug && ctest --test-dir libs/tiforth/build-debug`
- `ninja -C cmake-build-tiflash-tiforth-debug gtests_dbms && gtests_dbms --gtest_filter=TiForth*`

