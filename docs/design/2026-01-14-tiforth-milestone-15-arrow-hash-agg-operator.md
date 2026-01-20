# TiForth MS15: Arrow HashAgg Operator (Port Acero GroupByNode Into TiForth)

- Author(s): zanmato
- Last Updated: 2026-01-20
- Status: Implemented
- Related design: `docs/design/2026-01-14-tiforth.md`
- Related milestone: `docs/design/2026-01-14-tiforth-milestone-14-arrow-compute-aggregation.md`
- Discussion PR: TBD
- Tracking Issue: TBD

## Summary

Add a new TiForth hash aggregation operator, **`ArrowHashAggTransformOp`**, by porting the core logic of Arrow Acero’s
`GroupByNode` into TiForth’s own operator/pipeline framework (no Acero `ExecPlan` dependency).

This operator:

- Reuses Arrow-compute native grouped **`hash_*`** aggregation kernels (e.g. `hash_sum`, `hash_count`, `hash_min`,
  `hash_max`, …) rather than implementing TiForth-owned aggregate math.
- Makes **grouper creation explicit** so TiForth can provide custom `arrow::compute::Grouper` implementations for:
  - collation-aware grouping (future), and
  - TiFlash-style short-string key optimizations (future).

It is intentionally separate from:

- **`HashAggTransformOp`**: TiFlash-port aggregation core + TiFlash-shaped hash table optimizations (semantics anchor).
- **`ArrowComputeAggTransformOp`**: Arrow Acero-backed baseline (MS14) via `DeclarationToReader`.

## Motivation / Problem

MS14 validated that Arrow’s **`Grouper` + grouped kernels** can perform well, but the current TiForth Arrow-compute path
routes through Acero `GroupByNode`, which:

- hardcodes `arrow::compute::Grouper::Make(...)` (cannot inject collation/short-string specializations),
- brings an Acero `ExecPlan` dependency into TiForth execution, while the long-term goal is “no Acero”.

MS15 keeps the algorithmic structure that works (Arrow grouper + grouped kernels), but moves the orchestration into
TiForth so:

- TiForth can own the “hash agg operator contract” (breaker semantics, blocked-state integration, memory pool),
- TiForth can plug in alternative grouper implementations without being constrained by Acero’s node options.

## Goals

- Provide a TiForth breaker-style operator that:
  - consumes input batches until EOS,
  - assigns group ids via a TiForth-provided `arrow::compute::Grouper`,
  - updates Arrow grouped aggregate kernel states incrementally across batches,
  - emits final grouped results + EOS.
- Use Arrow-compute native grouped `hash_*` kernels for the aggregate math.
- Keep implementation independent of TiFlash (no `dbms/` includes), and route allocations through the task’s
  `arrow::MemoryPool`.
- Define a clear hook point for grouper specialization (factory/strategy object).

## Non-goals

- Replacing TiForth’s TiFlash-semantics `HashAggTransformOp` (MS12 remains the semantics/perf anchor).
- Collation-aware grouping semantics (only the hook point is introduced here).
- Spill / external aggregation.

## Design

### Operator surface + name

Operator: **`ArrowHashAggTransformOp`** (distinct from the Acero-backed `ArrowComputeAggTransformOp`).

Input/Output: Arrow-native (`arrow::RecordBatch` / `arrow::compute::ExecBatch`) consistent with existing TiForth
operators.

### Control flow (ported from Acero `GroupByNode`)

1. **Init (first batch)**:
   - resolve grouping key columns (ideally already materialized by an upstream projection op),
   - construct `grouper_` via a TiForth-level `GrouperFactory`,
   - resolve the requested Arrow grouped `hash_*` kernels and initialize per-aggregate kernel state.
2. **Consume batches**:
   - compute `group_ids = grouper_->Consume(keys)`
   - ensure per-aggregate kernel states are resized to `grouper_->num_groups()`
   - call each grouped kernel’s consume step with `(value_columns..., group_ids)`
3. **Finalize on EOS**:
   - fetch unique keys from `grouper_->GetUniques()`
   - finalize each aggregate kernel state to produce per-group arrays
   - assemble output batches (order must be documented and stable)

### Kernel interface strategy

Arrow’s grouped `hash_*` functions are registered in the public compute registry, but the incremental “kernel state”
driving logic (init/resize/consume/finalize) is currently packaged with Acero’s implementation.

MS15 ports the minimal glue necessary to:

- drive Arrow’s existing grouped kernels without constructing an Acero plan, and
- keep TiForth’s aggregation operator independent from Acero execution.

Follow-up (preferred): upstream a compute-level public API that exposes “grouped aggregation with external group ids”
without needing Acero/internal headers, so TiForth can depend on stable Arrow APIs only.

### Grouper injection hook

Introduce a TiForth-level interface:

- `GrouperFactory` (key types + exec context → `std::unique_ptr<arrow::compute::Grouper>`)

Default implementation returns `arrow::compute::Grouper::Make(...)`.

Future implementations can:

- group by **collation sort keys** (and retain original strings via payload / `first_row`),
- inline/pack **short strings** (and composite keys) before hashing/probing.

## Implementation Plan (Checklist)

TiForth:

- [x] Add `include/tiforth/operators/arrow_hash_agg.h` + `src/tiforth/operators/arrow_hash_agg.cc`.
- [x] Port minimal orchestration logic from Arrow Acero `GroupByNode`:
  - [x] group id production via `arrow::compute::Grouper`
  - [x] grouped kernel state init/resize/consume/finalize wiring
- [x] Keep `HashAggTransformOp` and `ArrowComputeAggTransformOp` unchanged.
- [x] Add unit tests:
  - [x] int key + numeric value (`count_all`, `count`, `sum`, `min`, `max`, `mean`) across multiple batches
  - [x] computed key + computed aggregate arg
  - [x] multi-key group-by (fixed-width + string)
  - [x] string keys treated as binary (no collation), where supported
  - [x] compare results as sets (ignore group ordering)

TiFlash integration (optional in MS15; required for MS16):

- Add a switch to select which TiForth hash agg implementation to use:
  - `HashAggTransformOp` vs `ArrowHashAggTransformOp` vs `ArrowComputeAggTransformOp`.
- [x] Implemented as TiFlash settings:
  - `enable_tiforth_arrow_compute_agg`: use `ArrowComputeAggTransformOp` (Acero, baseline)
  - `enable_tiforth_arrow_hash_agg`: use `ArrowHashAggTransformOp` (no Acero)
  - default: `HashAggTransformOp`
  - precedence: `enable_tiforth_arrow_compute_agg` wins if both are enabled
  - string keys: both Arrow-backed modes are binary semantics only; collation-sensitive GROUP BY falls back to `HashAggTransformOp`

## Validation

- TiForth: `ctest --test-dir build-debug --output-on-failure`
- TiFlash: `ninja -C cmake-build-debug gtests_dbms`, `gtests_dbms '--gtest_filter=TiForth*'`

## Status / Notes

- Implemented in TiForth `include/tiforth/operators/arrow_hash_agg.h` and `src/tiforth/operators/arrow_hash_agg.cc`.
- Key decisions:
  - no Acero `ExecPlan`; drive `HashAggregateKernel` state directly (`resize`/`consume`/`finalize`)
  - keys/agg args are TiForth expressions (compiled via `CompileExpr`), not restricted to field refs
  - output columns: aggregates first, then grouping keys; output is sliced into `kOutputBatchSize` batches
  - `GrouperFactory` hook kept to allow future collation/short-string optimized groupers
- Current limitations:
  - group-by without keys not implemented yet
  - only a small set of grouped functions mapped (`hash_{count_all,count,sum,mean,min,max}`)
  - TiFlash planner switch lives in `dbms/src/Flash/Planner/Plans/PhysicalAggregation.cpp` via `enable_tiforth_arrow_hash_agg`
