# TiForth MS15: Arrow HashAgg Operator (Port Acero GroupByNode Into TiForth)

- Author(s): TBD
- Last Updated: 2026-01-19
- Status: Planned
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

- Add `include/tiforth/operators/arrow_hash_agg.h` + `src/tiforth/operators/arrow_hash_agg.cc`.
- Port minimal orchestration logic from Arrow Acero `GroupByNode`:
  - group id production via `arrow::compute::Grouper`
  - grouped kernel state init/resize/consume/finalize wiring
- Keep `HashAggTransformOp` and `ArrowComputeAggTransformOp` unchanged.
- Add unit tests:
  - int key + numeric value (`count`, `sum`, `min`, `max`) across multiple batches
  - multi-key group-by (fixed-width)
  - string keys treated as binary (no collation), where supported
  - compare results as sets (ignore group ordering)

TiFlash integration (optional in MS15; required for MS16):

- Add a switch to select which TiForth hash agg implementation to use:
  - `HashAggTransformOp` vs `ArrowHashAggTransformOp` vs `ArrowComputeAggTransformOp`.

## Validation

- `ninja -C cmake-build-debug gtests_dbms`
- `ninja -C cmake-build-release gtests_dbms bench_dbms`

## Status / Notes

- MS15 introduces the “ported GroupByNode” TiForth operator and the grouper injection point.
- MS16 adds extensive parity tests to confirm Arrow grouped kernels match TiFlash aggregation semantics for the
  supported subset.
