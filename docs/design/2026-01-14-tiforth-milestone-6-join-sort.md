# TiForth Milestone 6: Hash Join + Sort (Minimal Coverage Plan)

- Author(s): TBD
- Last Updated: 2026-01-17
- Status: Implemented
- Related design: `docs/design/2026-01-14-tiforth.md`
- Depends on: MS2-5 (pipeline/task + expr/projection/filter + hash agg)

## Scope

Port two major operators to unlock more real queries:

- `Sort` as a blocking single-input operator
- `HashJoin` as a minimal “common path” operator

This milestone intentionally prioritizes a small, testable surface over full TiFlash parity.

## Goals

- `SortTransformOp`:
  - consumes all input batches, outputs sorted rows (single key, ASC, nulls last)
  - uses Arrow `RecordBatch` input/output
- `HashJoinTransformOp` (minimal):
  - inner join only
  - single int32 join key
  - probe side is the pipeline input stream
  - build side is provided as a pre-materialized set of Arrow batches at operator construction time (keeps MS6 compatible with the current single-input pipeline framework)
- Add unit tests for both operators.
- Add one TiFlash translation smoke test (guarded by `TIFLASH_ENABLE_TIFORTH`) mapping a TiFlash DAG shape into a TiForth join/sort pipeline (hardcoded config/expected initially).

## Non-goals

- Multi-input pipeline scheduling (real join build/probe pipelines).
- Outer/semi/anti joins.
- Multiple join keys / complex key types / join filters.
- Spill / partitioned hash join.
- Full TiFlash ordering/collation semantics.

## Proposed Public API

### Sort

- `struct SortKey { std::string name; bool ascending; bool nulls_first; }`
- `class SortTransformOp final : public TransformOp`
  - `explicit SortTransformOp(std::vector<SortKey> keys)`

MS6 restriction: exactly one key, `ascending=true`, `nulls_first=false`.

### Hash Join (minimal single-input form)

- `struct JoinKey { std::string left; std::string right; }`
- `class HashJoinTransformOp final : public TransformOp`
  - `HashJoinTransformOp(std::vector<std::shared_ptr<arrow::RecordBatch>> build_batches, JoinKey key)`

MS6 restriction:

- key type must be int32 on both sides
- output schema = left fields + right fields (name conflicts deferred)

## Operator Semantics / State Machines

### SortTransformOp

- On non-null input: buffer (no output) -> `OperatorStatus::kNeedInput`
- On EOS:
  - first time: emit one sorted `RecordBatch` -> `OperatorStatus::kHasOutput`
  - second time: forward EOS (`nullptr`) -> `OperatorStatus::kHasOutput`

### HashJoinTransformOp (probe-side transform)

- Build phase happens in constructor (or lazily on first input):
  - build an in-memory hash table from `build_batches`
- On non-null probe batch:
  - produce joined batch for matching rows -> `OperatorStatus::kHasOutput`
- On EOS: forward EOS

## Implementation Plan

### 1) Sort (Arrow compute)

- Buffer input batches, concatenate into a single `RecordBatch`/`Table` at EOS.
- Use Arrow compute:
  - `SortIndices` on the key column with `SortOptions`
  - `Take` each column with the computed indices
- Output as one `RecordBatch`.

### 2) Hash join (minimal int32 key)

- Build side:
  - materialize a map `key -> vector<row_index>` (or key -> vector of row ids)
  - store build columns for output materialization
- Probe:
  - for each probe row, lookup key and emit joined rows (nested loop over matches)
  - MS6 output can be a single output batch per input batch (may be larger than input)

### 3) Tests

TiForth unit tests:

- sort: verify ordering + null placement for int32
- join: small build side + probe side, verify output rows

TiFlash smoke:

- create TiFlash-shaped DAG with dummy join/sort ops, translate to TiForth join/sort, validate Arrow output (hardcoded expected).

## Definition of Done

- `ninja -C libs/tiforth/build-debug` + `ctest --test-dir libs/tiforth/build-debug`
- `ninja -C cmake-build-tiflash-tiforth-debug gtests_dbms`
- `gtests_dbms --gtest_filter=TiForthPipelineTranslateTest.*`
