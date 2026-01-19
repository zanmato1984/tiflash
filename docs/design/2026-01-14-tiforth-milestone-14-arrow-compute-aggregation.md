# TiForth MS14: Arrow-Compute Group Aggregation + TiFlash Benchmarks

- Author(s): TBD
- Last Updated: 2026-01-19
- Status: In progress

## Summary

Add a **TiForth pipeline operator** that performs **GROUP BY + aggregates** using **Apache Arrow compute primitives**, primarily:

- `arrow::compute::Grouper` for group-id assignment and unique key materialization.
- Arrow hash-aggregate kernels via `arrow::compute::CallFunction("hash_*", ...)` for grouped aggregates.

Then add **Google Benchmark** coverage in TiFlash to compare:

- TiFlash native `Aggregator` (no spill)
- TiForth Arrow-compute aggregation operator (no spill)

across multiple **data types** and **data distributions**.

This milestone is explicitly a **rapid validation / performance baseline** and temporarily ignores parity with TiFlash aggregation semantics (type coercions, NULL/NaN corner cases, collations, decimals, spill, etc.).

## Goals

- Provide a TiForth transform operator that:
  - consumes input batches until EOS,
  - computes grouped results using Arrow compute,
  - emits a single output `arrow::RecordBatch` + EOS.
- Support a minimal aggregate set using Arrow public APIs:
  - `count_all`, `count`, `sum`, `mean`, `min`, `max`
  - multi-key group-by.
- Add TiFlash `bench_dbms` benchmarks comparing native vs TiForth Arrow-compute aggregation:
  - cover multiple key/value types (at least `int32`, `int64`, `double`, `string` keys where feasible)
  - cover multiple key distributions (single group, low-cardinality, high-cardinality, skewed)
  - ignore spill in both implementations.

## Non-goals

- Parity with TiFlash aggregation semantics and all function coverage.
- External / spilled aggregation.
- Incremental / streaming aggregation (this operator may buffer all input).
- Wiring this operator into TiFlash DAG→pipeline translation (benchmark uses manual pipeline construction).

## Design

### TiForth operator: `ArrowComputeAggTransformOp`

High-level behavior:

1. On first input batch: validate schema, bind/compile key & arg expressions, create `arrow::compute::Grouper`.
2. For each input batch:
   - evaluate key expressions into arrays,
   - `grouper->Consume(keys)` to produce a `UInt32` group-id array,
   - evaluate aggregate argument expressions (if any),
   - buffer `{group_ids, agg_args}` by chunk.
3. On EOS:
   - `grouper->GetUniques()` to obtain the grouped key columns,
   - for each aggregate, call Arrow hash-aggregate function:
     - `hash_count_all(group_ids)`
     - `hash_count(values, group_ids)`
     - `hash_sum(values, group_ids)`
     - `hash_mean(values, group_ids)`
     - `hash_min(values, group_ids)`
     - `hash_max(values, group_ids)`
   - build an output schema `{keys..., aggs...}` and emit one output batch.

Notes:

- All compute runs on the task’s `arrow::compute::ExecContext` backed by the engine’s `arrow::MemoryPool`.
- Input schema must remain stable across batches; operator errors on mismatch.
- No spill: all chunk state kept in-memory until EOS.

### TiFlash benchmark

Add a new `bench_*.cpp` under `dbms/src/Flash/tests/` and integrate with existing `bench_dbms` target.

Benchmark cases:

- Multiple key/value types (minimum):
  - key: `Int32`, `Int64`, `String`; value: `Int64` and/or `Double`
- Distributions:
  - single group (all keys identical)
  - low cardinality uniform (e.g. 16 groups)
  - high cardinality uniform (e.g. ~N/2 groups)
  - skewed (Zipf-like hot keys)

Each case runs two benchmarks:

- `NativeAggregator/*`: build `DB::Aggregator` and process `DB::Block` inputs.
- `TiForthArrowCompute/*`: build a TiForth pipeline using `ArrowComputeAggTransformOp` and process Arrow record batches.

Data generation is done once per benchmark instance (outside the timed loop). Spill is disabled.

## Implementation Plan (Checklist)

TiForth:

- Add `include/tiforth/operators/arrow_compute_agg.h` + `src/tiforth/operators/arrow_compute_agg.cc`.
- Implement the operator using `arrow::compute::Grouper` + `CallFunction("hash_*")`.
- Add unit tests:
  - correctness on a few types (int keys + numeric values; include NULLs; multi-batch input)
  - validate EOS behavior (single output batch + EOS).

TiFlash:

- Add `dbms/src/Flash/tests/bench_tiforth_arrow_compute_agg.cpp`.
- Ensure `bench_dbms` links `tiforth::tiforth` and defines `TIFLASH_ENABLE_TIFORTH` when `ENABLE_TIFORTH=ON`.
- Run benchmarks and record summary + ratio in `.codex/progress/daemon.md`.

## Validation

- TiForth: `cmake --build /Users/zanmato/dev/tiforth/build-debug && ctest --test-dir /Users/zanmato/dev/tiforth/build-debug`
- TiFlash: `ninja -C cmake-build-debug gtests_dbms bench_dbms`
- Benchmark: `cmake-build-debug/dbms/bench_dbms --benchmark_filter='.*ArrowComputeAgg.*' --benchmark_min_time=0.2`

## Status / Notes

- Status will be updated with commit SHAs and benchmark summary once implementation lands.

