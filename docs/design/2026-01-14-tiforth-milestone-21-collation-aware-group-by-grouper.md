# TiForth MS21: Collation-Aware GROUP BY via Custom Arrow Grouper (Exploration + Prototype)

- Author(s): zanmato
- Last Updated: 2026-01-21
- Status: Implemented
- Related design: `docs/design/2026-01-14-tiforth.md`
- Related milestone: `docs/design/2026-01-14-tiforth-milestone-20-remove-legacy-hash-agg-rename-hash-agg.md`
- Discussion PR: TBD
- Tracking Issue: TBD

## Summary

Explore whether HashAgg-style aggregation (Grouper + grouped `hash_*` kernels) can support **collation-aware**
string grouping by providing a **custom `arrow::compute::Grouper`** implementation.

If feasible, land a minimal implementation + parity tests for simple cases so we can remove the legacy hash agg
implementation without losing TiFlash collation semantics.

## Motivation / Problem

- TiFlash GROUP BY on collated strings must group by **collation equality** (via sort-key normalization), not binary bytes.
- Today, TiForth’s collation-aware GROUP BY lives in the legacy TiFlash-port HashAgg implementation.
- The Arrow-based hash agg path currently treats strings with **binary semantics** only.

To remove the legacy hash agg (MS20), we need a replacement for collation-aware grouping.

## Goals

- Support collation-aware grouping for the hot case:
  - **single grouping key**
  - Arrow key physical type: **`binary`** (and optionally `utf8`)
  - key field has TiForth logical metadata:
    - `tiforth.logical_type=string`
    - `tiforth.string.collation_id=<tidb collation id>`
- Group semantics: equality defined by the collation (sort-key bytes).
- Output key semantics: preserve a representative **original** key per group (match TiFlash “first_row key keep” behavior).
- Keep allocations on the provided `arrow::MemoryPool`.
- Add basic parity tests against TiFlash native aggregation for simple collations/cases.

## Non-goals

- Multi-key collation-aware grouping (can be extended later).
- End-to-end collation coverage for all TiDB collations (start with a small supported set).
- Performance tuning beyond “works and is not catastrophically slow”.

## Design

### Input contract and metadata

TiForth identifies collated strings via Arrow field metadata:

- `tiforth.logical_type=string`
- `tiforth.string.collation_id=<int32>`

TiFlash→Arrow conversion already carries this metadata; TiForth must continue to treat any missing value as “binary”
collation (id 63) unless the caller explicitly sets a supported collation id.

### Core idea: custom Grouper normalizes-to-sort-key, but outputs original key

The Arrow `Grouper` API is sufficient if we implement a grouper that:

- hashes/compares using **collation sort-key bytes**, and
- stores **one original key** per group (e.g. the first seen) for `GetUniques()`.

This matches TiFlash’s existing approach for collation grouping:

- grouping key: normalized bytes (sort key)
- output key: preserved original bytes (“first row” value for the group)

### Proposed implementation: `CollationSingleKeyGrouper`

Implement:

- `tiforth::detail::CollationSingleKeyGrouper : public arrow::compute::Grouper`

Key behaviors:

- `Consume()`:
  - For each row:
    - if null → assign the null group id (one null group)
    - else:
      - compute sort-key bytes using the configured collation id
      - probe/insert into an open-addressing table keyed by sort-key bytes
      - on first insert: also store the **original** key bytes for output uniques
- `Lookup()`:
  - Same normalization, probe only; return null for “not found”.
- `GetUniques()`:
  - Return an Arrow `BinaryArray` (or `StringArray` if input is utf8) of **original** keys, one per group.
  - For groups formed by multiple distinct originals that normalize to the same sort key, return the first-seen original.
- `Reset()` clears state.

Notes:

- Sort-key bytes can be longer than input; store them in an arena (pool-backed) and use slices for hash/equality.
- Reuse the existing TiForth collation key encoding logic used by legacy HashAgg / sort / join (no TiFlash headers).

### Integration point

Wire into the default grouper selection inside TiForth hash agg:

- If `keys.size() == 1` and the key field is logical string with `collation_id != 63`:
  - select `CollationSingleKeyGrouper`
- Else:
  - keep existing selection (`SmallStringSingleKeyGrouper` for binary/string single-key; Arrow default otherwise)

### Parity tests (simple matrix)

Add small, deterministic parity tests that compare TiFlash native aggregation to TiForth hash agg for collated keys.

Minimal suggested cases:

- Collation id 33 (`general_ci`):
  - keys: `["A", "a", "a ", "b", NULL]`
  - group-by key: string
  - aggregates: `count_all()`, `count(v)`, `sum(v)`
  - expectations:
    - `"A"`, `"a"`, `"a "` collapse to one group
    - output key equals the first seen original for that group (e.g. `"A"` if it appears first)
    - null key is its own group

Optional additional case:

- Collation id 46 (`utf8mb4_bin`-like / binary-ish) should behave as binary grouping (sanity).

## Implementation Plan (Checklist)

- [x] Validate feasibility: confirm `Grouper` contract allows grouping by normalized keys while outputting first-seen originals.
- [x] Implement `CollationSingleKeyGrouper` (single-key only) using TiForth collation key encoding.
- [x] Wire it into TiForth hash-agg grouper selection when `tiforth.string.collation_id` is present and supported.
- [x] Add TiForth unit tests for the grouper (covered via HashAgg + memory-pool tests).
- [x] Add TiFlash parity gtests for 1–2 simple collations/cases (single-key only).

## Validation

- TiForth: `ctest --test-dir build-debug --output-on-failure`
- TiFlash: `ninja -C cmake-build-debug gtests_dbms`
- TiFlash: `gtests_dbms --gtest_filter='TiForth*'`

## Notes

- If a full custom grouper proves too complex, a fallback exploration is to precompute sort-key arrays as key expressions
  and separately preserve original keys via a “first_row” mechanism; however, this likely requires extending the hash-agg
  operator beyond standard Arrow grouped `hash_*` kernels. The preferred plan is the custom grouper.
