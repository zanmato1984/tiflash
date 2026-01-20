# TiForth MS18: Custom Grouper For Small-String Single-Key GROUP BY

- Author(s): zanmato
- Last Updated: 2026-01-21
- Status: Implemented
- Related design: `docs/design/2026-01-14-tiforth.md`
- Related milestone: `docs/design/2026-01-14-tiforth-milestone-15-arrow-hash-agg-operator.md`
- Discussion PR: TBD
- Tracking Issue: TBD

## Summary

Add a TiForth-owned **custom `arrow::compute::Grouper` implementation** optimized for the hot case:

- **single grouping key**
- **binary/string-like keys**
- **small strings**

The new grouper is injected into `ArrowHashAggTransformOp` via its `GrouperFactory` hook and becomes the default
grouper for supported inputs.

The implementation should be based on the proven TiFlash approach (small-string optimized hash table / inline key
storage), but must remain TiForth-independent (no TiFlash headers).

## Motivation / Problem

We already observed that Arrow’s default row grouper can be a hotspot on string keys. For TiFlash-like workloads,
**single-key GROUP BY on short strings** is common, and key hashing/probing dominates.

MS15 intentionally added an explicit grouper factory hook. MS18 fills that hook with a fast, predictable implementation
for this specific case.

## Goals

- Implement a `SmallStringSingleKeyGrouper` that satisfies Arrow’s `arrow::compute::Grouper` contract:
  - `Consume(keys) -> group_id_array`
  - `GetUniques() -> unique key arrays`
  - correct grouping semantics for nulls (nulls form one group)
- Optimize for:
  - small string keys (inline storage + fast hashing),
  - low-to-medium cardinality (common GROUP BY),
  - streaming batches (incremental `Consume`).
- Keep allocations on the provided `arrow::MemoryPool`.
- Keep behavior compatible with TiFlash semantics (binary equality; collation handled separately).

## Non-goals

- Collation-aware grouping (requires sort-key pipeline or specialized key normalization).
- Multi-key optimization in this milestone (can be extended later).
- Spill / external aggregation.

## Design

### Supported input types

TiFlash↔TiForth conversion currently maps string-like Block columns to Arrow **`BinaryArray`**.
TiForth itself may also see `utf8` keys from tests or other callers.

The custom grouper should support:

- `arrow::binary()` and `arrow::utf8()` (Large* variants can be added later if needed).

### Key representation + hashing strategy

Implemented approach:

- For keys with length `<= kInlineBytes`:
  - store bytes inline in a fixed-size key slot (`kInlineBytes` + length tag),
  - hash on the inlined bytes (fast, no heap traffic).
- For longer keys:
  - store an owned copy in an arena buffer and hash/compare on the bytes (still correct, but not the optimized path).

### Hash table

Port the underlying idea from TiFlash’s string-key aggregation method:

- open-addressing table
- store group id + key bytes (inline) + null marker
- predictable probing

TiForth should use `ARROW_DCHECK/ARROW_CHECK` liberally to keep invariants rigid (group id bounds, buffer sizes, etc).

### Integration point

`ArrowHashAggTransformOp` already supports:

- `GrouperFactory` injection

MS18 wires:

- a default grouper selection that chooses `SmallStringSingleKeyGrouper` when:
  - `keys.size() == 1`
  - key type is binary/utf8
  - (inline threshold is handled internally; longer keys are stored in the arena)

### Output uniques

The grouper must be able to emit `GetUniques()` as Arrow arrays. For inline keys, reconstruct `BinaryArray`/`StringArray`
buffers from the stored inline bytes.

Null group should be emitted as a null slot in the output unique array.

## Implementation Plan (Checklist)

- [x] Identify TiFlash “small string key” aggregation method and port the core approach (open addressing + inline bytes).
- [x] Implement `SmallStringSingleKeyGrouper : public arrow::compute::Grouper`.
- [x] Wire as the default grouper for single-key binary/string grouping in `ArrowHashAggTransformOp`.
- [x] Add unit tests (multiple batches, nulls, duplicates, high-cardinality, reset).
- [x] Extend TiFlash parity gtests to include string/binary keys (binary semantics; collation out of scope).

## Validation

- TiForth: `ctest --test-dir build-debug --output-on-failure`
- TiFlash: `ninja -C cmake-build-debug gtests_dbms`
- TiFlash: `gtests_dbms --gtest_filter='TiForthArrowHashAggParityTest.*'`

## Notes

- Collation: long-term plan is “group by sort keys + keep original string via payload/first_row”; MS18 is binary-only.
- This grouper is explicitly a “single-key fast path”; multi-key string grouping can come later.
