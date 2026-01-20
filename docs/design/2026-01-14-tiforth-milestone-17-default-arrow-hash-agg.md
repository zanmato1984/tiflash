# TiForth MS17: Make ArrowHashAgg The Default HashAgg (Rename Legacy HashAgg)

- Author(s): zanmato
- Last Updated: 2026-01-20
- Status: Planned
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

Proposed naming:

- `ArrowHashAggTransformOp`: stays as-is (it’s the implementation).
- Rename current TiFlash-port `HashAggTransformOp` to `LegacyHashAggTransformOp`.
- Add a TiForth-level “default selection” alias/helper:
  - either a type alias `using HashAggTransformOp = ArrowHashAggTransformOp;` (temporary migration), or
  - a factory helper `MakeDefaultHashAggTransformOp(...)` returning an `ArrowHashAggTransformOp` by default.

Preference: **factory helper** over type alias to avoid confusing symbol meaning and to keep both implementations
available by name in the public API.

### Default selection rules (host-independent)

In TiForth (library):

- Default hash agg operator: `ArrowHashAggTransformOp`.
- Fallback to `LegacyHashAggTransformOp` when:
  - grouping key is collated string (metadata indicates collation),
  - or aggregate list contains a function not supported by ArrowHashAgg,
  - or key/value types unsupported by Arrow grouped kernels / TiForth overrides.

In TiFlash integration (planner):

- Keep the existing `enable_tiforth_arrow_hash_agg` / `enable_tiforth_arrow_compute_agg` toggles, but consider flipping
  the default selection to ArrowHashAgg once MS17 lands.
- Keep collation-sensitive grouping on legacy until MS18+ collation pipeline exists.

### Tests / parity guardrails

- Reuse existing TiFlash parity gtests (MS16) as correctness gate.
- Add a lightweight “selection test” to ensure the default path is ArrowHashAgg for non-collated keys.

## Implementation Plan (Checklist)

TiForth:

- [ ] Rename `HashAggTransformOp` -> `LegacyHashAggTransformOp` (headers, impl, tests, build files).
- [ ] Introduce a stable default-selection factory/helper (and avoid breaking external names when possible).
- [ ] Make default pipeline builder helpers choose ArrowHashAgg by default.
- [ ] Ensure ArrowHashAgg output schema and function registry overrides stay intact.

TiFlash:

- [ ] Update planner/operator selection logic to treat ArrowHashAgg as the default TiForth hash agg implementation
  (still respecting explicit settings).
- [ ] Preserve existing fallback behavior for collation-sensitive GROUP BY.
- [ ] Update docs and any references to “HashAggTransformOp” to clarify legacy vs default.

## Validation

- TiForth: `ctest --test-dir build-debug --output-on-failure`
- TiFlash: `ninja -C cmake-build-debug gtests_dbms`
- TiFlash: `gtests_dbms --gtest_filter='TiForthArrowHashAggParityTest.*:TiForth*'`

## Notes / Open Questions

- API compatibility strategy: keep old name as an alias for one release vs immediate rename.
- Whether TiFlash should flip the default immediately or behind a setting rollout.

