# TiForth MS20: Rework HashAgg (Arrow-based), Remove Legacy, Rename ArrowHashAgg → HashAgg

- Author(s): zanmato
- Last Updated: 2026-01-21
- Status: Implemented
- Related design: `docs/design/2026-01-14-tiforth.md`
- Related milestone: `docs/design/2026-01-14-tiforth-milestone-17-default-arrow-hash-agg.md`
- Discussion PR: TBD
- Tracking Issue: TBD

## Summary

Complete the MS17 transition and fully “make ArrowHashAgg the HashAgg” by:

1. **Removing** the legacy TiFlash-port hash aggregation operator from TiForth.
2. **Removing** the current legacy HashAgg breaker sink/source operators (`HashAggBuildSinkOp`, `HashAggConvergentSourceOp`).
3. **Reworking** the Arrow-based non-Acero hash aggregation (current `ArrowHashAggTransformOp`, renamed to `HashAgg*`) into
   a proper two-stage pipeline breaker:
   - a per-task `TransformOp` that performs **partial aggregation** in parallel, and
   - a shared-context `SinkOp`/`SourceOp` pair that performs **merge + final aggregation** and emits result batches.
4. **Renaming** the Arrow-based hash aggregation operator family from “ArrowHashAgg” to the canonical “HashAgg”
   across APIs/files/docs/tests/benchmarks/integration.

After this milestone:

- TiForth has exactly one “hash agg” operator family name: `HashAgg*` (Arrow grouper + grouped `hash_*` kernels).
- The old implementation (`LegacyHashAggTransformOp`) is deleted (no alias).
- Hash aggregation follows TiForth breaker semantics (partial → merge/final → emit), enabling parallel partial aggregation.
- All user-facing names in TiForth/TiFlash integration/docs/tests/benchmarks no longer mention “ArrowHashAgg”.

## Motivation / Problem

We currently carry multiple names and implementations:

- “Legacy HashAgg” (TiFlash-port, collation-aware) and
- “ArrowHashAgg” (Arrow-native, the default for most cases).

Even after MS17, the naming remains confusing because:

- `HashAggTransformOp` is a temporary alias to the legacy implementation, while the default is ArrowHashAgg.
- TiFlash integration and benchmarks still carry “ArrowHashAgg” naming, even though it is intended to become “the” hash agg.

This milestone makes the API and documentation coherent and reduces long-term maintenance cost.

## Goals

- Remove `LegacyHashAggTransformOp` and its tests/docs/bench references entirely.
- Remove the current legacy HashAgg breaker operators (`HashAggBuildSinkOp`, `HashAggConvergentSourceOp`) as part of deleting
  the legacy hash-agg implementation.
- Rework ArrowHashAgg into a breaker-style two-phase hash aggregation:
  - per-task partial aggregation (`TransformOp`) for parallelism,
  - merge + final aggregation in a convergent stage (shared `SinkOp`/`SourceOp`), emitting final results.
- Rename ArrowHashAgg symbols and file layout so the Arrow-based implementation is “the” `HashAgg*` operator family.
- Keep TiForth independent (no TiFlash headers/assumptions introduced into TiForth).
- Keep behavioral and type-contract expectations stable for the supported subset (guarded by existing parity tests).

## Non-goals

- Expanding the supported aggregate/kernel matrix beyond the existing ArrowHashAgg coverage.
- Removing `ArrowComputeAggTransformOp` (Acero-backed path) or changing its naming.
- Shipping collation-aware GROUP BY as part of this milestone (handled by MS21; MS20 is expected to depend on MS21 to avoid a semantics regression for collated string keys).

## Design

### Naming plan

Target naming after MS20:

- HashAgg (Arrow-based):
  - `tiforth::HashAggTransformOp`: partial aggregation transform (per task).
  - `tiforth::HashAggMergeSinkOp`: merge + final aggregation sink (shared context).
  - `tiforth::HashAggResultSourceOp`: result source that emits final aggregated batches.
- No `tiforth::ArrowHashAggTransformOp` symbol remains (renamed).
- No `tiforth::LegacyHashAggTransformOp` remains (deleted).
- No legacy breaker types remain (`HashAggBuildSinkOp`, `HashAggConvergentSourceOp` deleted).

### File layout

Before MS20:

- ArrowHashAgg: `include/tiforth/operators/arrow_hash_agg.h` + `src/tiforth/operators/arrow_hash_agg.cc`
- Legacy HashAgg: `include/tiforth/operators/hash_agg.h` + `src/tiforth/operators/hash_agg.cc`

After removing legacy, reuse `hash_agg.{h,cc}` for the Arrow-based implementation:

- `include/tiforth/operators/hash_agg.h` (Arrow-based)
- `src/tiforth/operators/hash_agg.cc` (Arrow-based)

### Dependency on MS21 (collation)

Legacy HashAgg is currently the only collation-aware GROUP BY implementation. Removing it must not silently drop
collation semantics.

Plan:

- MS21 provides a collation-aware grouper path for Arrow-based hash agg (at least single-key).
- MS20 deletes legacy after MS21 parity tests show collation-aware grouping still matches TiFlash for basic cases.

### New HashAgg breaker architecture (partial → merge/final → emit)

#### High-level shape

Represent hash aggregation as a TiForth breaker (two pipelines):

- **Pipeline A (parallel / partial)**:
  - upstream operators
  - `HashAggTransformOp` in **partial mode** (local hash table + grouper + grouped kernels)
  - `HashAggMergeSinkOp` (shared context)

- **Pipeline B (convergent / emit)**:
  - `HashAggResultSourceOp` (shared context)
  - downstream operators

This enables multiple tasks to run Pipeline A concurrently and reduce input volume before the convergent stage.

#### Partial aggregation output contract

`HashAggTransformOp` must output an intermediate representation that is mergeable across tasks. Proposed contract:

- Output columns:
  - group-by keys (original key values, with metadata preserved),
  - aggregate “partial state” columns that are mergeable by the final stage.

Initial scope can restrict aggregates to a subset with obvious 2-phase decomposition:

- `count_all` / `count`: partial output is a `uint64` count; final stage sums counts.
- `sum`: partial output is a sum; final stage sums partial sums.
- `min` / `max`: partial output is min/max; final stage reduces over partial mins/maxs.
- `mean/avg`: partial output is `(sum, count)`; final stage computes `sum/count` with TiFlash-compatible types/nulls.

The final stage (`HashAggMergeSinkOp`) is responsible for:

- merging partial outputs across tasks by grouping on keys (reusing the same grouper selection),
- applying merge reducers and finalization (especially for `mean`),
- materializing the final output schema and batches for `HashAggResultSourceOp`.

#### Removing legacy breaker ops

The existing legacy HashAgg breaker types (`HashAggBuildSinkOp`, `HashAggConvergentSourceOp`) are tied to the legacy
implementation and are removed. The new HashAgg breaker sink/source are implemented specifically for the Arrow-based
HashAgg (shared context stores Arrow-native state / merged outputs).

## Implementation Plan (Checklist)

TiForth:

- [x] Delete `LegacyHashAggTransformOp` implementation + remove any remaining alias types.
- [x] Delete legacy HashAgg breaker types (`HashAggBuildSinkOp`, `HashAggConvergentSourceOp`) and their shared context.
- [x] Rework ArrowHashAgg into a breaker-style implementation:
  - [x] `HashAggTransformOp`: partial aggregation transform used in parallel pipelines/tasks.
  - [x] `HashAggMergeSinkOp`: merge + final aggregation sink with a shared context.
  - [x] `HashAggResultSourceOp`: source that emits final result batches after merge/finalization.
  - [x] define the partial-state schema contract and the merge/finalization mapping for supported aggregates.
- [x] Rename `ArrowHashAggTransformOp` → `HashAggTransformOp` and remove “ArrowHashAgg” naming across headers/paths/docs/tests.
- [x] Ensure the default operator docs clearly describe string semantics:
  - binary semantics by default
  - collation-aware grouping via custom grouper selection (once MS21 lands).

TiFlash integration:

- [x] Update planner selection code to use the renamed `HashAgg*` operators and breaker wiring (partial + convergent stage).
- [x] Update any settings/logging/benchmark names to remove “ArrowHashAgg”.
- [x] Keep the TiFlash→TiForth Arrow type metadata contract unchanged (`tiforth.string.collation_id`, etc).

## Validation

- TiForth: `ctest --test-dir build-debug --output-on-failure`
- TiFlash: `ninja -C cmake-build-debug gtests_dbms`
- TiFlash: `gtests_dbms --gtest_filter='TiForth*'`

## Notes

- This is an API-breaking rename; do it in one coordinated sweep (TiForth + TiFlash integration + docs/tests).
- If a temporary compatibility alias is needed, it should be time-bounded and removed promptly; default plan is **no alias**.
