# TiForth Milestone 12: Port TiFlash “Full” Aggregation Core (Hash Agg Methods + Hash Tables)

- Author(s): TBD
- Last Updated: 2026-01-19
- Status: Partially Implemented (one-key methods)
- Related design: `docs/design/2026-01-14-tiforth.md`
- Discussion PR: TBD
- Tracking Issue: TBD

## Problem

TiForth hash aggregation is correct for the common path and already uses an arena-backed open-addressing hash table, but
it remains a *minimal* aggregation subset compared to TiFlash’s production `Aggregator`:

- Group-by keying uses one generic “serialized normalized bytes” method (good for correctness, not optimal for common
  fixed-width keys).
- Aggregate function coverage and type coverage are intentionally small (e.g. limited `sum`/`min`/`max` kinds).
- There is no shared “aggregation core” layer that mirrors TiFlash’s method selection (one-number / fixed-keys / string /
  serialized), making it easy for TiForth semantics/perf to drift from TiFlash over time.

The request is to port TiFlash’s *full aggregation implementation shape* into TiForth (especially the hash table layer),
without coupling TiForth to TiFlash types/headers and while keeping Arrow-native execution.

## Goals

- Mirror TiFlash/ClickHouse aggregation architecture inside TiForth:
  - aggregation method selection based on GROUP BY key physical shapes
  - optimized hash tables for common key patterns (fixed-size and string)
  - arena-backed aggregate state storage
- Keep TiForth independent:
  - no `dbms/` includes, no ClickHouse column types
  - Arrow arrays/buffers are the physical representation
- Keep Arrow-style APIs and harden with `ARROW_DCHECK` / `ARROW_CHECK`.
- Preserve existing semantics already codified in TiForth:
  - NULL grouping: NULLs group together per key
  - float key canonicalization (`-0 == 0`, NaNs canonicalized for grouping)
  - collated string grouping via sort-key normalization
  - deterministic group ordering by first appearance
- Route all allocations (hash tables, arenas, scratch) through `arrow::MemoryPool`.

## Non-goals (initial MS12)

- Full external aggregation / spill end-to-end (hooks may exist; execution wiring can stay stubbed).
- Multi-threaded build-side aggregation and 2-level hash table conversion (can follow once correctness lands).
- Implementing *every* TiFlash aggregate function on day 1; focus on closing the big functional gaps first.

## Proposed Design

### A) “Aggregation method” selection (TiFlash-shaped)

Implement TiFlash-shaped “method selection” in `HashAggContext`:

- choose a keying method once key types are known (first batch)
- dispatch to method-specific keying logic in `ConsumeBatch` (deterministic insert order)

An explicit `AggKeyTable` interface can be extracted later once multi-key fixed-key methods land; the first implementation
keeps the changes local to reduce risk.

Provide implementations (subset initially, expand as needed):

1. **WithoutKey**: global aggregation (single group id 0).
2. **Dense one-key map**: TiFlash `FixedHashMap`-style for bool/i8/u8/i16/u16 (array indexed by key value), plus a
   separate “null group”.
3. **OneNumber (uint64)**: open-addressing table keyed by a single fixed-width scalar normalized to `uint64_t`:
   - covers `Int32/Int64/UInt32/UInt64` and float/double via canonicalized bit patterns.
4. **FixedKeys128/256**: pack multiple fixed-width keys (+ null bitmap) into a fixed-size blob (`UInt128`/`UInt256`)
   and key an open-addressing table on that blob.
5. **String / Serialized**: fallback for variable-length or collation-normalized keys:
   - reuse/extend the existing `detail::KeyHashTable` (saved hash + arena-owned bytes)

Selection rules mirror TiFlash’s `AggregatedDataVariants` logic but driven by Arrow physical types + TiForth metadata:

- Prefer fixed-key methods only when normalization is fixed-width and equality can be implemented without variable-length
  sort keys (e.g. numeric keys without collation/string normalization).
- Use serialized fallback when any key requires variable-length normalization (collated strings, mixed variable-length).

### B) Hash table layer

Port TiFlash’s hash-table *behavior* (not its ClickHouse dependencies):

- open addressing, power-of-two capacity, linear probing
- saved hash per entry
- Arrow-pool-backed contiguous entry storage (Arrow `Buffer`)
- arena-backed varlen key ownership where needed

This can be implemented as:

- keep existing `detail::KeyHashTable` as the serialized/string table
- add a small fixed-key open-addressing table for `uint64_t`/`UInt128`/`UInt256` keys (initially `uint64_t`)
- add a fixed array map for `UInt8/UInt16`

### C) Aggregate function coverage

Extend the internal aggregate function surface *towards* TiFlash:

- extend `detail::AggregateFunction` with:
  - `Merge(dst, src)` (foundation for partial aggregation and future spill)
  - optional `Serialize/Deserialize` later if/when spill lands

Add missing “core” aggregate functions/types needed for TiFlash parity, prioritized:

1. `avg` (uses `sum` + `count`, with TiDB/TiFlash decimal rules where relevant)
2. `sum`:
   - float/double
   - decimal128/256 (respect precision/scale and overflow semantics as per TiForth type contract)
3. `min`/`max`:
   - float/double with NaN rules matching TiFlash/TiDB
   - decimal128/256
   - packed-MyTime types where applicable (stored as `uint64` with logical-type metadata)

### D) Operator wiring

Keep the existing operator surfaces (`HashAggTransformOp` common path and breaker-form build/convergent ops), but change
their internals to use the new method-selected key table and expanded aggregate function set.

## Implementation Steps

1. Implement one-key method selection in `HashAggContext` and keep multi-key as serialized fallback.
2. Implement dense one-key map (bool/i8/u8/i16/u16) + tests.
3. Implement fixed-width open-addressing tables (`OneNumber`, `FixedKeys128/256`) + packers + tests.
4. Extend `detail::AggregateFunction` with `Merge` and implement it for existing aggregates.
5. Add `avg` + numeric/decimal/float `sum/min/max` coverage (and unit tests).
6. Update TiFlash integration:
   - bump TiForth pin
   - extend parity tests to cover the new aggregates/types and ensure breaker path parity

## Testing / Validation

TiForth:

- gtests for:
  - each key-table method correctness (NULLs, first-appearance ordering)
  - stress tests for new hash tables (insert/find, rehash)
  - aggregate function correctness across supported types

TiFlash:

- gtests parity on representative DAG shapes that exercise:
  - fixed-width group keys (int/uint)
  - collated string group keys (serialized fallback)
- new aggregates/types (avg/decimal/float)

Status (2026-01-19):

- Implemented one-key method selection in TiForth:
  - dense one-key map (bool/i8/u8/i16/u16)
  - one-number `uint64_t` open addressing via `detail::FixedKeyHashTable`
  - single-string-key fast path with collation normalization (binary/pad-binary avoid extra sort-key allocation)
  - multi-key remains serialized (`detail::KeyHashTable`)
- TiForth commit: `0eeb174`

## Notes

- This milestone ports the *shape* of TiFlash aggregation (method selection + hash tables) into TiForth, but keeps
  Arrow arrays as the execution surface.
- When later adding parallel build-side aggregation, two-level tables can be introduced in a TiFlash-compatible way
  (hash prefix partitioning), using the same fixed-key/serialized methods.
