# TiForth MS5 Follow-up: Hash Aggregation Hash Table (Port TiFlash/ClickHouse-style Open Addressing + Arena)

- Author(s): TBD
- Last Updated: 2026-01-19
- Status: Implemented
- Related design: `docs/design/2026-01-14-tiforth.md`, `docs/design/2026-01-14-tiforth-milestone-5-hash-aggregation.md`

## Problem

TiForth MS5 hash aggregation is correct and parity-tested, but the core GROUP BY keying path is still “prototype-grade”:

- uses `std::unordered_map<NormalizedKey, group_id>` (node-based, poor locality)
- stores string keys as owning `std::pmr::string` inside the key object
- builds both `NormalizedKey` and `OutputKey` *per input row*, even when the group already exists, which causes:
  - per-row string copies (raw key + sort key) for collated string group keys
  - unnecessary allocations/constructors and increased CPU/memory overhead

TiFlash’s production aggregation (ClickHouse-derived) uses:

- open-addressing hash tables with saved hash (cache-friendly)
- arenas for variable-length key storage (stable addresses, bulk free)
- “lookup-before-copy”: only materialize key bytes when the group is new

This document specifies the TiForth equivalent refactor.

## Goals

- Replace `std::unordered_map` in `HashAggContext` with a TiFlash-shaped open-addressing hash table.
- Route all allocations through `arrow::MemoryPool` (no `new`/`malloc` for key storage / table buffers).
- Keep existing semantics:
  - deterministic group id assignment based on first appearance
  - NULL grouping behavior (NULLs group together)
  - float key canonicalization (`-0==0`, NaNs canonicalized)
  - collated string grouping via sort-key normalization (PAD SPACE / CI / no-pad)
  - output uses first-seen *raw* key value (not normalized)
- Avoid per-row string allocations for GROUP BY keys:
  - probe using scratch-encoded normalized key bytes
  - allocate/copy only when inserting a new group

Non-goals (this follow-up):

- partial aggregation merge / two-level tables / spill
- extending aggregate function set (handled by other work)

## Design

### Key storage: Arena (Arrow pool-backed)

Introduce an internal arena that:

- allocates big chunks from `arrow::MemoryPool`
- provides `Allocate(size)` returning stable pointers
- frees all chunks on arena destruction (owned by `HashAggContext`)

This mirrors TiFlash/ClickHouse `Arena` but uses Arrow error handling and memory pool integration.

### Key representation: normalized serialized bytes

Instead of storing a typed `NormalizedKey` object (with owning strings), represent the hash table key as a byte slice:

- `ByteSlice = {const uint8_t* data, int32_t size}`
- bytes are the *normalized* representation used for hashing/equality
- stored bytes live in the arena

This makes the hash table key trivially movable and cache-friendly.

### Key encoding (fixed schema per `HashAggContext`)

For each grouping key (in order):

- 1 byte: `is_null` (0 = NULL, 1 = non-NULL)
- if non-NULL:
  - integers/bool: append canonical 8 bytes (little-endian) of widened `int64_t` / `uint64_t`
  - float/double: append canonicalized IEEE bits as 8 bytes
  - decimal128/256: append raw 16/32 bytes
  - binary string: append `uint32_le length` + sort-key-normalized bytes (collation-dependent)

Key count is implicit from the operator configuration (no need to encode for GROUP BY).
Global aggregation (`key_count==0`) bypasses the hash table entirely (single group id 0).

### Hash table: open addressing + saved hash

Implement a minimal open-addressing table specialized for `ByteSlice -> uint32_t group_id`:

- power-of-two capacity, linear probing
- empty slot sentinel: `key_data == nullptr`
- store `hash` per entry to speed probes
- equality check: `hash` + `size` + `memcmp(key_bytes)`
- rehash when load factor exceeds a threshold (e.g. 0.7)

This captures TiFlash’s core advantages (no per-entry heap allocations; locality; saved hash).

### Lookup-before-copy

Per input row:

1. Encode normalized key bytes into a scratch buffer (stack/`std::string`/`BufferBuilder`).
2. Probe the hash table using the scratch pointer/size (no allocations).
3. If found: use the existing `group_id`.
4. If not found:
   - allocate/copy normalized key bytes into the arena
   - insert into hash table with new `group_id`
   - materialize and store the output key values for this group (first-seen raw key)

This removes the biggest current cost for collated string GROUP BY.

## Implementation Checklist

- Add internal `Arena` + `KeyHashTable` to `tiforth/detail/`.
- Refactor `HashAggContext`:
  - replace `std::unordered_map<NormalizedKey,...>` with new table
  - bypass table for global aggregation
  - build output keys only on insert
- Keep/extend tests:
  - existing TiForth hash-agg tests must pass (ordering + collations)
  - TiFlash parity gtests for agg breaker/common path must pass

Status (2026-01-19): implemented in tiforth commit `147d4be` (arena-backed key storage + open-addressing hash table + lookup-before-copy for GROUP BY keys). Files: `include/tiforth/detail/{arena,key_hash_table}.h`, `include/tiforth/operators/hash_agg.h`, `src/tiforth/operators/hash_agg.cc`. Checks: `cmake --build /Users/zanmato/dev/tiforth/build-debug && ctest --test-dir /Users/zanmato/dev/tiforth/build-debug`; `ninja -C cmake-build-debug gtests_dbms && LOG_LEVEL=error cmake-build-debug/dbms/gtests_dbms --gtest_filter='TiForthPipelineTranslateTest.*:TiForthFilterAggParityTestRunner.*'`.

## Implementation Notes (as landed)

- Hashing: `XXH3_64bits` (Arrow vendored), with saved hash per entry and linear probing.
- `KeyHashTable` storage: a single zero-initialized `arrow::Buffer` (no per-entry heap allocations; no deletes/tombstones).
- `Arena`: chunked allocations from `arrow::MemoryPool` with stable pointers; frees all chunks in destructor.
- HashAgg hot path: probe using scratch-encoded normalized key bytes; only on insert allocate/copy normalized bytes + materialize `OutputKey` (first-seen raw key).

## Notes / Follow-ups

- The same arena + key table can be reused by hash join build side (and other operators with keying).
- If/when TiForth adopts a more complete TiFlash aggregate function framework, group key handling can stay as-is and only the per-group aggregate state layout changes.
