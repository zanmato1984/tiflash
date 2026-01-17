# TiForth Milestone 5: Hash Aggregation (Common Path) + Minimal Aggregate Functions

- Author(s): TBD
- Last Updated: 2026-01-17
- Status: Implemented
- Related design: `docs/design/2026-01-14-tiforth.md`
- Depends on: MS2-4 (pipeline/task + projection + filter)

## Scope

Implement the first blocking operator:

- hash aggregation (GROUP BY) as a `tiforth::TransformOp` on Arrow `RecordBatch`
- only the most common/smallest supported path (single input, no partial merge, no spill)
- a minimal aggregate function set (initially `count_all`, `sum_int32`)

## Goals

- `HashAggTransformOp` consumes input batches and produces exactly one output `RecordBatch` at end-of-stream.
- Support:
  - single group key (int32, nullable)
  - aggregate functions:
    - `count_all` (COUNT(*))
    - `sum` over int32 input (skip nulls; emit null if no non-null inputs in a group)
- Deterministic output order (stable group id assignment based on first appearance).
- Unit tests covering multi-batch accumulation.
- One TiFlash translation smoke (guarded by `TIFLASH_ENABLE_TIFORTH`) mapping a TiFlash DAG shape into a TiForth hash-agg pipeline (hardcoded config for now).

## Non-goals

- Partial aggregation / merge / multi-stage aggregation.
- Distinct aggregates.
- Multiple group keys / complex key types.
- Memory spill / external aggregation.
- Full TiFlash semantics parity (type coercions, decimals, collations).

## Proposed Public API

Add a new operator and simple plan structs:

- `struct AggKey { std::string name; ExprPtr expr; }`
- `struct AggFunc { std::string name; std::string func; ExprPtr arg; }`
  - `func` initially supports: `count_all`, `sum_int32`
  - `arg` is unused for `count_all`
- `class HashAggTransformOp final : public TransformOp`
  - `explicit HashAggTransformOp(std::vector<AggKey> keys, std::vector<AggFunc> aggs)`

Notes:

- `ExprPtr` reuse keeps TiForth independent from TiFlash expression types.
- MS5 only requires `FieldRef` expressions (no nested compute) for keys/args.

## Operator Semantics / State Machine

`HashAggTransformOp` is blocking:

- On non-null input batch:
  - consume rows into an internal hash table / accumulators
  - produce no output (`OperatorStatus::kNeedInput`)
- On EOS marker (`*batch == nullptr`):
  - first time: finalize and output one aggregated `RecordBatch` (`OperatorStatus::kHasOutput`)
  - second time: forward EOS (`*batch == nullptr`, `OperatorStatus::kHasOutput`)

## Implementation Plan

### 1) Core data structures (minimal types)

Maintain:

- `std::unordered_map<Key, uint32_t> key_to_group_id_`
- `std::vector<Key> group_keys_` (index by group id, preserves insertion order)
- per-aggregate state vectors (index by group id):
  - `count_all_ : std::vector<uint64_t>`
  - `sum_i64_ : std::vector<int64_t>`
  - `sum_has_value_ : std::vector<bool>` (to emit null when all inputs are null)

Key representation:

- MS5 supports nullable int32:
  - `struct Key { bool is_null; int32_t value; }` with custom hash/eq

### 2) Batch consumption

- Evaluate key/arg expressions to arrays (MS5: require `int32` arrays).
- Iterate rows:
  - find/insert group id for key
  - update `count_all_`
  - update `sum_*` for non-null arg values

### 3) Finalize output

- Build output arrays using Arrow builders:
  - key column(s): `arrow::Int32Builder` (+ nulls)
  - `count_all`: `arrow::UInt64Builder`
  - `sum_int32`: `arrow::Int64Builder` (+ nulls based on `sum_has_value_`)
- Output schema:
  - cache `output_schema_` for stable shared schema across outputs

### 4) Tests

TiForth gtests:

- multi-batch input with repeated/new keys, validate counts/sums
- null key handling (NULL group)
- sum null behavior (all-null group -> output null)

TiFlash gtest (guarded by `TIFLASH_ENABLE_TIFORTH`):

- construct a TiFlash-shaped DAG containing a dummy “agg” transform
- translate into a TiForth pipeline using TiForth APIs only (hardcoded config)
- run TiForth pipeline on Arrow input and validate output

## Definition of Done

- TiForth:
  - `ninja -C libs/tiforth/build-debug`
  - `ctest --test-dir libs/tiforth/build-debug`
- TiFlash:
  - `ninja -C cmake-build-tiflash-tiforth-debug gtests_dbms`
  - `gtests_dbms --gtest_filter=TiForthPipelineTranslateTest.*`
