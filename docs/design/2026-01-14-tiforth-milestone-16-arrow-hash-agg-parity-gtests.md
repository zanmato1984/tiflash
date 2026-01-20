# TiForth MS16: Arrow HashAgg Parity GTests (vs TiFlash Aggregator, No Collation)

- Author(s): zanmato
- Last Updated: 2026-01-20
- Status: Implemented
- Related design: `docs/design/2026-01-14-tiforth.md`
- Related milestone: `docs/design/2026-01-14-tiforth-milestone-15-arrow-hash-agg-operator.md`
- Discussion PR: TBD
- Tracking Issue: TBD

## Summary

Add **extensive parity gtests in TiFlash** to validate that Arrow-compute native grouped **`hash_*`** aggregation kernels
(as used by TiForth’s `ArrowHashAggTransformOp`) are **semantically compatible** with TiFlash native aggregation for the
supported subset.

Scope explicitly **ignores collation** for now (treat string keys as binary or exclude them).

## Motivation / Problem

TiForth’s long-term direction is to use Arrow-style abstractions (notably `arrow::compute::Grouper`) while remaining
compatible with TiFlash/TiDB semantics.

Even if the grouping infrastructure is sound, the aggregate function semantics must match TiFlash for correctness.
Arrow grouped kernels can differ subtly in:

- NULL handling (`skip_nulls`, `min_count`, empty groups)
- integer overflow / type promotion rules
- floating NaN/Inf handling
- output type selection (e.g. sum type widening)

MS16 creates a test suite that makes these differences explicit and prevents accidental drift as TiForth evolves.
As a direct outcome, TiForth now overrides selected Arrow grouped `hash_*` aggregates (via its function registry) to
match TiFlash semantics where Arrow defaults differ.

## Goals

- Add a parity test suite that compares results between:
  - **TiFlash native**: `DB::Aggregator` (no spill), and
  - **Arrow grouped kernels**: via TiForth `ArrowHashAggTransformOp` (or direct kernel driving) using the same input.
- Cover the aggregate set intended to be supported by `ArrowHashAggTransformOp`:
  - `count_all`, `count`, `sum`, `min`, `max` (and `mean`/`avg` if enabled).
- Cover representative key/value types and common distributions, across multiple batches.
- Compare results without relying on group output ordering (treat results as sets keyed by group-by columns).

## Non-goals

- Collation-sensitive group-by semantics (handled by a future custom grouper / sort-key pipeline).
- Spill/external aggregation.
- Full coverage of every TiFlash aggregate function; focus on the subset implemented by the Arrow-kernel-backed path.

## Design

### Test harness shape

For each test case:

1. Generate a deterministic dataset (in-memory) that can be fed both to:
   - TiFlash native aggregation (as `DB::Block` input), and
   - TiForth/Arrow aggregation (as `arrow::RecordBatch` input).
2. Run aggregation with the same logical query shape:
   - `GROUP BY keys` + a fixed aggregate list.
3. Normalize outputs:
   - Convert both results into a canonical map keyed by serialized group key values (binary compare only).
   - Compare aggregate columns for equality under the chosen semantics (exact for integers; well-defined for floats).

### Suggested coverage matrix

- **Key types**:
  - `Int32`, `Int64`
  - optional: binary string keys (`String`) with binary collation only
  - multi-key combos (e.g. `Int32 + Int64`)
- **Value types**:
  - `Int32`, `Int64`, `UInt64`
  - `Float32`, `Float64`
  - `Decimal(20,2)`
- **Distributions** (reuse `bench_dbms` shapes where possible):
  - single group
  - uniform low cardinality (e.g. 16 groups)
  - uniform high cardinality (unique keys)
  - skewed (Zipf-like hot keys)
- **NULL patterns**:
  - no NULLs
  - NULLs in value columns
  - NULLs in key columns (ensure NULLs group together)
- **Batching**:
  - single batch
  - multiple batches (schema stable across batches)

### Compatibility rules (initial)

To avoid ambiguous comparisons, MS16 should start with constraints that are known to be comparable:

- Use integer ranges that do not overflow the target sum type (unless overflow behavior is being tested explicitly).
- Avoid NaN-heavy inputs unless NaN semantics are being tested explicitly and clearly defined.

As divergences are found, either:

- adjust TiForth operator options to match TiFlash (e.g. `skip_nulls`), or
- document and gate the Arrow-kernel-backed path as “not supported” for that semantic corner.

## Implementation Plan (Checklist)

- [x] Add a new TiFlash gtest file under `dbms/src/Flash/tests/` implementing a parity matrix.
- [x] Add helpers to run native `DB::Aggregator` and TiForth `ArrowHashAggTransformOp`, normalize outputs (unordered map keyed by group key bytes) and compare.
- [x] Integrate into `gtests_dbms` (auto via `gtest*.cpp` glob).

## Validation

- `ninja -C cmake-build-debug gtests_dbms`
- `gtests_dbms '--gtest_filter=TiForthArrowHashAggParityTest.*'`

## Status / Notes

- Implemented parity test: `dbms/src/Flash/tests/gtest_tiforth_arrow_hash_agg_parity.cpp`.
- Coverage includes: int32/int64 keys; int64/float64/decimal values; single/low-card/high-card/zipf distributions; null keys + null values (including an “all-null group” case to validate sum/min/max nullability).
- Coverage includes: output type/nullability checks; int32/int64 keys; int32/int64/uint64/float32/float64/decimal values;
  single/low-card/high-card/zipf distributions; null keys + null values (including an “all-null group” case); empty input;
  multi-key group-by; special float corner cases (NaN/Inf and signed zero); and an explicit Int64 overflow case.
- TiFlash can now enable this path in planner via `enable_tiforth_arrow_hash_agg` (collation-sensitive string GROUP BY still falls back to TiForth `HashAggTransformOp`).
- Key mitigations implemented based on parity failures:
  - `hash_count`/`hash_count_all`: override to UInt64 output (non-nullable) to match TiFlash `count` type contract.
  - `hash_min`/`hash_max` (float): override float kernels to use `<`/`>` comparisons (order-sensitive) to match TiFlash
    NaN and signed-zero semantics.
