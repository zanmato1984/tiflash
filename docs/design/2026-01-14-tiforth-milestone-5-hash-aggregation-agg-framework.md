# TiForth MS5 Follow-up: Hash Aggregation Aggregate Function Framework (Port TiFlash/ClickHouse-style Aggregate State Layout)

- Author(s): TBD
- Last Updated: 2026-01-18
- Status: Implemented (aggregate function interface + per-group state row)
- Related design: `docs/design/2026-01-14-tiforth.md`, `docs/design/2026-01-14-tiforth-milestone-5-hash-aggregation.md`

## Problem

TiForth MS5 hash aggregation is parity-tested, but the aggregate execution core is still “prototype-grade”:

- aggregate functions are hard-coded in `HashAggContext` via a `switch` over a small `AggState::Kind`
- per-group state is spread across many `std::vector<...>` members (one vector per function and/or per internal field)
- adding new aggregate functions or supporting richer state (e.g. strings, decimals, sketches) requires invasive edits

TiFlash’s production aggregation (ClickHouse-derived) uses a generic aggregate function interface and a compact per-group
state layout:

- each aggregate function defines its state size/alignment + create/destroy/add/finalize hooks
- each group owns one contiguous “aggregate state row” storing all function states, allocated from an arena
- the hash table maps keys to the group’s aggregate state pointer

This document specifies the TiForth equivalent refactor while keeping TiForth Arrow-native and independent from TiFlash.

## Goals

- Introduce an internal aggregate function abstraction mirroring the TiFlash/ClickHouse shape:
  - `state_size` / `state_alignment`
  - `Create(state)` / `Destroy(state)`
  - `Add(state, arg_array, row)` (or no-arg for `count_all`)
  - `Finalize(state, builder)`
- Store per-group aggregate state as a single contiguous blob allocated from an Arrow-pool-backed arena.
- Migrate existing MS5 aggregates (`count_all`, `count`, `sum`, `min`, `max`) onto the new framework without changing
  semantics or tests.
- Keep allocations routed through `arrow::MemoryPool`:
  - state memory: arena
  - variable-sized fields inside state (e.g. strings): `std::pmr::*` backed by an Arrow-pool `memory_resource`

Non-goals (this follow-up):

- partial merge aggregation, two-level tables, or spill/external aggregation
- expanding the aggregate function set beyond MS5 minimal functions

## Design

### Aggregate function interface (internal)

Define an internal interface (names illustrative):

- `class AggregateFunction`
  - `int64_t state_size() const`
  - `int64_t state_alignment() const`
  - `void Create(uint8_t* state) const`
  - `void Destroy(uint8_t* state) const`
  - `arrow::Status Add(uint8_t* state, const arrow::Array* arg, int64_t row) const`
  - `arrow::Status Finalize(const uint8_t* state, arrow::ArrayBuilder* out) const`

Notes:

- `Add` uses `arrow::Array` + `row` to stay Arrow-native; per-row virtual calls match the TiFlash/ClickHouse shape.
- Follow-ups can add `Merge(dst_state, src_state)` for partial aggregation.

### Aggregate state layout

For a configured `HashAggContext` with N aggregates:

- compute an `AggStateLayout` assigning each aggregate a byte `offset` into the per-group blob
- respect each aggregate’s `state_alignment` (pad offsets as needed)
- record the total `row_size` and `row_alignment` (max alignment)

Per new group insertion:

- allocate `row_size` bytes from an Arrow-pool-backed arena using `row_alignment`
- call each aggregate’s `Create(state + offset)`
- keep a `group_id -> uint8_t* row_state` mapping (e.g. a vector of pointers indexed by group id)

### Semantics preservation

Migrate the existing functions with the same observable behavior:

- `count_all`: non-null `UInt64`, increments for every row
- `count`: non-null `UInt64`, increments for non-NULL arg rows
- `sum`: nullable `Int64`/`UInt64` chosen by arg signedness; NULL if no non-NULL inputs
- `min`/`max`: nullable; compare using normalized representation; output is the chosen raw value (collations apply to
  binary string args via sort-key normalization)

### Implementation Plan

1. Add the internal aggregate function abstraction and state layout helper.
2. Refactor `HashAggContext` to:
   - construct aggregate function instances once types are known (first input batch)
   - allocate per-group aggregate state blobs from an arena
   - update states via `Add(...)`
   - finalize via `Finalize(...)` into Arrow builders
3. Update/extend tests as needed; keep all existing parity tests passing.

## Notes / Follow-ups

- Once the framework exists, porting additional TiFlash aggregate functions becomes additive (new function classes) rather
  than invasive edits to `HashAggContext`.
- The same “aggregate state row” layout is the foundation for partial merge + spill in later milestones.

Status (2026-01-18): implemented in tiforth commit `a354622` (add `detail::AggregateFunction` + `detail::ComputeAggStateLayout`; refactor `HashAggContext` to allocate a per-group state row from an Arrow-pool arena and call `Create/Add/Finalize/Destroy` for `count_all/count/sum/min/max`). Files: (tiforth) `include/tiforth/detail/aggregate_function.h`, `include/tiforth/operators/hash_agg.h`, `src/tiforth/operators/hash_agg.cc`; (tiflash) `libs/CMakeLists.txt`, `docs/design/2026-01-14-tiforth-milestone-5-hash-aggregation-agg-framework.md`. Checks: `cmake --build /Users/zanmato/dev/tiforth/build-debug && ctest --test-dir /Users/zanmato/dev/tiforth/build-debug`; `ninja -C cmake-build-debug gtests_dbms && LOG_LEVEL=error cmake-build-debug/dbms/gtests_dbms --gtest_filter='TiForthPipelineTranslateTest.*:TiForthFilterAggParityTestRunner.*'`.
