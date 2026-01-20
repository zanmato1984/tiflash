# TiForth MS17: Make ArrowHashAgg The Default HashAgg (Rename Legacy HashAgg)

- Author(s): zanmato
- Last Updated: 2026-01-21
- Status: Implemented
- Related design: `docs/design/2026-01-14-tiforth.md`
- Related milestone: `docs/design/2026-01-14-tiforth-milestone-15-arrow-hash-agg-operator.md`
- Discussion PR: TBD
- Tracking Issue: TBD

## Summary

Switch TiForth’s **default hash aggregation operator** from the TiFlash-port implementation to the **Arrow-compute-based**
non-Acero implementation (`ArrowHashAggTransformOp`), and rename the current default hash agg operator to **“legacy”**
to reflect its new role (semantics anchor / fallback path).

This is a behavior and naming change only: the legacy operator remains available for:

- collation-sensitive string GROUP BY (until MS18 provides a fast binary-key grouper + a separate collation path),
- semantic escape hatches when Arrow-kernel-backed behavior is not supported,
- cross-checking parity/perf regressions.

## Motivation / Problem

We now have:

- **Legacy** TiFlash-port `HashAggTransformOp`: semantics anchor, but its code and optimizations are tied to TiFlash
  internals and are harder to evolve in a “pure Arrow-native” direction.
- **Arrow** `ArrowHashAggTransformOp`: Arrow-native architecture (Grouper + grouped `hash_*` kernels), no Acero,
  pluggable grouper hook, and now guarded by parity tests + TiForth overrides for known semantic mismatches.

To keep TiForth moving toward “Arrow-native compute library”, the Arrow-based hash agg should become the default.

## Goals

- Make the ArrowHashAgg implementation the **default** hash agg chosen by TiForth pipeline builders (and TiFlash planner
  integration where applicable).
- Rename the current default hash agg operator to **Legacy** for clarity.
- Keep an explicit, low-risk fallback path to legacy for:
  - collation-sensitive grouping,
  - unsupported aggregates/types,
  - debugging/parity experiments.
- Keep TiForth independent: no TiFlash includes or assumptions in TiForth code.

## Non-goals

- Removing the legacy implementation.
- Changing semantics coverage beyond the already supported ArrowHashAgg subset.
- Adding collation semantics (handled separately).

## Design

### Naming / API surface

Implemented naming:

- `ArrowHashAggTransformOp`: stays as-is (it’s the implementation).
- Rename TiFlash-port `HashAggTransformOp` to `LegacyHashAggTransformOp`.
- Keep `HashAggTransformOp` as a temporary alias to `LegacyHashAggTransformOp` for migration/compat.

### Default selection rules (host-independent)

In TiForth (library), the “default” is by convention:

- Default hash agg operator: `ArrowHashAggTransformOp`.
- Fallback to `LegacyHashAggTransformOp` when:
  - grouping key is collated string (metadata indicates collation),
  - or aggregate list contains a function not supported by ArrowHashAgg,
  - or key/value types unsupported by Arrow grouped kernels / TiForth overrides.

In TiFlash integration (planner):

- ArrowHashAgg is selected as the default TiForth hash-agg implementation when TiForth aggregation is enabled.
- Keep collation-sensitive grouping on legacy until MS18+ collation pipeline exists.

### Tests / parity guardrails

- Reuse existing TiFlash parity gtests (MS16) as correctness gate.
- Add a lightweight “selection test” to ensure the default path is ArrowHashAgg for non-collated keys.

## Implementation Plan (Checklist)

TiForth:

- [x] Rename `HashAggTransformOp` -> `LegacyHashAggTransformOp` (headers, impl, tests, build files).
- [x] Keep `HashAggTransformOp` as a temporary alias for migration.
- [x] Ensure ArrowHashAgg output schema and function registry overrides stay intact.

TiFlash:

- [x] Update planner/operator selection logic to treat ArrowHashAgg as the default TiForth hash agg implementation.
- [x] Preserve existing fallback behavior for collation-sensitive GROUP BY.
- [x] Update docs/tests and references to clarify legacy vs default.

## Validation

- TiForth: `ctest --test-dir build-debug --output-on-failure`
- TiFlash: `ninja -C cmake-build-debug gtests_dbms`
- TiFlash: `gtests_dbms --gtest_filter='TiForthArrowHashAggParityTest.*:TiForth*'`

## Notes / Open Questions

- API compatibility: keep `HashAggTransformOp` as an alias to legacy for one migration window.
