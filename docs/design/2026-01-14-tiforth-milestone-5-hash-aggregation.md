# TiForth Milestone 5: Hash Aggregation (Common Path) + Minimal Aggregate Functions

- Author(s): TBD
- Last Updated: 2026-01-18
- Status: Implemented
- Related design: `docs/design/2026-01-14-tiforth.md`
- Depends on: MS2-4 (pipeline/task + projection + filter)

## Scope

Implement the first blocking operator:

- hash aggregation (GROUP BY) as a `tiforth::TransformOp` on Arrow `RecordBatch`
- common path only (single input, no partial merge, no spill)
- a minimal-but-useful aggregate function set:
  - `count_all` (COUNT(*))
  - `count` (COUNT(arg), skip NULLs)
  - `sum` (nullable output; signed/unsigned tracked)
  - `min` / `max` (nullable output; skip NULLs)

## Goals

- `HashAggTransformOp` consumes input batches and produces exactly one output `RecordBatch` at end-of-stream.
- GROUP BY keys:
  - up to 8 keys (nullable)
  - supported key array types:
    - integers (signed/unsigned), bool
    - float/double keys normalized for hashing/equality:
      - `-0.0` and `0.0` treated equal
      - NaNs canonicalized for grouping
    - `Decimal128` / `Decimal256` (raw bytes)
    - binary strings (`arrow::binary`) with TiDB collations via sort-key normalization (padding + CI + no-pad)
- Aggregate functions:
  - `count_all`: always non-null `UInt64`
  - `count`: always non-null `UInt64`
  - `sum`: nullable `Int64` / `UInt64` depending on arg signedness (skip NULLs; emit NULL if no non-null inputs)
  - `min` / `max`: nullable, same logical type as arg (skip NULLs; emit NULL if no non-null inputs)
- Deterministic output order (stable group id assignment based on first appearance).
- Output column order: aggregate outputs first, then group key columns.
- Unit tests covering multi-batch accumulation, multi-key grouping, and collated string grouping.
- TiFlash↔TiForth parity tests for common Filter+Agg pipelines.

## Non-goals

- Partial aggregation / merge / multi-stage aggregation.
- Distinct aggregates.
- Full TiFlash aggregate function set (avg, stddev, bitmap/hll, approx distinct, ...).
- Memory spill / external aggregation.

## Proposed Public API

Add a new operator and simple plan structs:

- `struct AggKey { std::string name; ExprPtr expr; }`
- `struct AggFunc { std::string name; std::string func; ExprPtr arg; }`
  - `func` supports: `count_all`, `count`, `sum`, `min`, `max` (some aliases accepted)
  - `arg` is unused for `count_all`
- `class HashAggTransformOp final : public TransformOp`
  - `HashAggTransformOp(const Engine* engine, std::vector<AggKey> keys, std::vector<AggFunc> aggs, arrow::MemoryPool* pool = nullptr)`

Notes:

- `ExprPtr` reuse keeps TiForth independent from TiFlash expression types.
- MS5 common path assumes keys/args evaluate to arrays (scalar broadcasts are handled in expression execution helpers).

## Operator Semantics / State Machine

`HashAggTransformOp` is blocking:

- On non-null input batch:
  - consume rows into an internal hash table / accumulators
  - produce no output (`OperatorStatus::kNeedInput`)
- On EOS marker (`*batch == nullptr`):
  - first time: finalize and output one aggregated `RecordBatch` (`OperatorStatus::kHasOutput`)
  - second time: forward EOS (`*batch == nullptr`, `OperatorStatus::kHasOutput`)

## Implementation Plan

### 1) Core data structures

Maintain:

- `std::unordered_map<NormalizedKey, uint32_t> key_to_group_id_`
- `std::vector<OutputKey> group_keys_` (index by group id, preserves insertion order)
- per-aggregate state vectors (index by group id):
  - `count_all : std::vector<uint64_t>`
  - `count : std::vector<uint64_t>`
  - `sum_i64 / sum_u64 + sum_has_value`
  - `extreme_out / extreme_norm` for `min`/`max`

Key representation:

- `NormalizedKey` stores a small fixed array of key parts (up to 8) used for hashing/equality:
  - numeric keys: widened to `int64_t` / `uint64_t`
  - float keys: store canonicalized IEEE bits in `uint64_t`
  - decimal keys: store raw bytes (`Decimal128`/`Decimal256`)
  - string keys: store collation sort key in a `std::pmr::string`
- `OutputKey` stores the first-seen raw key values for output materialization (e.g. original strings).

### 2) Batch consumption

- Lazily compile key/arg expressions on the first input batch.
- Evaluate key/arg expressions to arrays per batch.
- Iterate rows:
  - build `NormalizedKey` / `OutputKey`
  - find/insert group id
  - update aggregate state:
    - `count_all`: +1
    - `count`: +1 if arg non-null
    - `sum`: add value if arg non-null (track `sum_has_value`)
    - `min`/`max`: compare normalized value, keep output value

### 3) Finalize output

- Build output arrays using Arrow builders (with nullability rules above).
- Output schema:
  - aggregate columns first, then key columns
  - preserve key/arg field metadata when keys/args are `FieldRef` expressions (collation + logical type)
  - cache `output_schema_` for stable shared schema across outputs

### 4) Tests

TiForth gtests:

- multi-batch input with repeated/new keys, validate `count`/`sum`/`min`/`max`
- multi-key grouping
- collated string keys (padding BIN, general CI, 0900 no-pad)

TiFlash gtests (guarded by `TIFLASH_ENABLE_TIFORTH`):

- parity tests comparing TiFlash execution vs TiForth `HashAggTransformOp` outputs on the same mock tables:
  - `Filter` + `HashAgg` common pipelines
  - multi-key + collated string group keys

## Definition of Done

- TiForth:
  - `ninja -C libs/tiforth/build-debug`
  - `ctest --test-dir libs/tiforth/build-debug`
- TiFlash:
  - `ninja -C cmake-build-tiflash-tiforth-debug gtests_dbms`
  - `gtests_dbms --gtest_filter=TiForthFilterAggParityTestRunner.*`
