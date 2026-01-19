# TiForth MS14: Arrow-Compute Group Aggregation + TiFlash Benchmarks

- Author(s): TBD
- Last Updated: 2026-01-19
- Status: Implemented (baseline) + streaming update

## Summary

Add a **TiForth pipeline operator** that performs **GROUP BY + aggregates** using **Apache Arrow compute primitives**, primarily:

- Arrow Acero execution plans (`source` + `aggregate`), because Arrow grouped hash-aggregate kernels are exposed through Acero plan nodes.

Then add **Google Benchmark** coverage in TiFlash to compare:

- TiFlash native `Aggregator` (no spill)
- TiForth Arrow-compute aggregation operator (no spill)

across multiple **data types** and **data distributions**.

This milestone is explicitly a **rapid validation / performance baseline** and temporarily ignores parity with TiFlash aggregation semantics (type coercions, NULL/NaN corner cases, collations, decimals, spill, etc.).

## Goals

- Provide a TiForth transform operator that:
  - consumes input batches until EOS,
  - computes grouped results using Arrow Acero,
  - emits aggregated output batches + EOS.
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
- Full expression support for group keys / aggregate args (follow-up To Do: add a pre-projection stage).
- Host-managed executor integration for Acero (current implementation uses Arrow's CPU thread pool via `QueryOptions.use_threads=true` so the plan can run while the pipeline is feeding input).
- Wiring this operator into TiFlash DAG→pipeline translation (benchmark uses manual pipeline construction).

## Design

### TiForth operator: `ArrowComputeAggTransformOp`

High-level behavior:

1. On first input batch:
   - validate the input schema,
   - bind key / aggregate argument `Expr` to Arrow `FieldRef` (currently only `FieldRef` supported),
   - build an Acero plan:
     - `source` node backed by an `AsyncGenerator<std::optional<arrow::compute::ExecBatch>>`,
     - `aggregate` node using `arrow::acero::AggregateNodeOptions`.
   - materialize a `arrow::RecordBatchReader` via `arrow::acero::DeclarationToReader`.
2. For each input batch:
   - convert `arrow::RecordBatch` → `arrow::compute::ExecBatch`,
   - push the batch into a `arrow::PushGenerator` feeding the Acero `source` node.
3. On EOS:
   - close the generator producer,
   - drain `RecordBatchReader::ReadNext` and forward each aggregated batch downstream,
   - forward EOS once the reader is exhausted.

Notes:

- All compute runs on the task’s `arrow::compute::ExecContext` backed by the engine’s `arrow::MemoryPool`.
- Input schema must remain stable across batches; operator errors on mismatch.
- The operator does not materialize input into an `arrow::Table` (no full input buffering); any state is internal to Acero aggregation.
- Output order is not stable; tests and benchmarks must not assume group ordering.
- No spill: all aggregation state kept in-memory until EOS.

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
- Implement the operator using Arrow Acero (`source` + `aggregate`) and a `RecordBatchReader` result stream.
- Add unit tests:
  - correctness on a few types (int keys + numeric values; include NULLs; multi-batch input)
  - validate EOS behavior (output batches + EOS) without assuming output order.

TiFlash:

- Add `dbms/src/Flash/tests/bench_tiforth_arrow_compute_agg.cpp`.
- Ensure `bench_dbms` links `tiforth::tiforth` and defines `TIFLASH_ENABLE_TIFORTH` when `ENABLE_TIFORTH=ON`.
- Run benchmarks and record summary + ratio in `.codex/progress/daemon.md`.

## Validation

- TiForth: `cmake --preset debug && cmake --build --preset debug && ctest --preset debug`
- TiFlash: `ninja -C cmake-build-debug gtests_dbms bench_dbms`
- Benchmark: `cmake-build-debug/dbms/bench_dbms --benchmark_filter='.*ArrowComputeAgg.*' --benchmark_min_time=0.2`

## Status / Notes

- Status will be updated with commit SHAs and benchmark summary once implementation lands.
