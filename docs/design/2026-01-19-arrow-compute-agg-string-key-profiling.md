# ArrowComputeAgg: Why string keys are slower (profiling, 2026-01-19)

This doc investigates why `ArrowComputeAgg/TiForth` is slower than `ArrowComputeAgg/Native` for **string group keys** in `dbms/bench_dbms`, and why `TiForthDictKey` (shared dictionary) is much faster.

Benchmark context and raw throughput tables are in:

- `docs/design/2026-01-19-arrow-compute-agg-benchmark-report.md`

## TL;DR

- **TiForth (Arrow/Acero) with raw string keys** spends most CPU in Arrow’s **group-by key hashing + varbinary key comparisons**, dominated by:
  - `arrow::compute::Hashing32::HashVarLenImp`
  - `arrow::compute::KeyCompare::CompareVarBinaryColumnToRowHelper`
  - `_platform_memmove`/`memcpy` within the compare path
- **TiFlash Native** spends CPU mostly in its **string hash-map** insertion/find path:
  - `DB::Aggregator::emplaceOrFindKey` + `DB::ColumnsHashing::KeyStringBatchHandlerBase::prepareNextBatchType`
- **TiForthDictKey** removes most varbinary comparison work by turning keys into **int dictionary codes** (stable dictionary across batches), shifting time to:
  - `arrow::compute::SwissTable::*` + grouped aggregate update kernels

Practical conclusion:

- Arrow/Acero grouped aggregation is fast when keys are **fixed-width**, but raw `BinaryArray` keys incur substantial per-row varlen hash/compare overhead.
- A shared-dictionary representation can eliminate the bottleneck, but Arrow currently cannot “unify differing dictionaries” across batches (so per-batch dictionary-encode is not viable for streaming).

## Profiling method

Platform: macOS. Profiler: `sample` + FlameGraph scripts.

1) Build:

```bash
ninja -C cmake-build-release bench_dbms
```

2) Run and sample a single benchmark for ~20s, while sampling stacks for 10s:

```bash
bench=cmake-build-release/dbms/bench_dbms
filter='^ArrowComputeAgg/TiForth/kString_vInt64_uniform_low_rows262144_blk65536_groups16$'

$bench --benchmark_filter=\"$filter\" --benchmark_min_time=20 & pid=$!
sleep 2
sample \"$pid\" 10 -file /tmp/sample.txt
wait \"$pid\"
```

3) Convert to flamegraph (using https://github.com/brendangregg/FlameGraph):

```bash
awk -f /tmp/FlameGraph/stackcollapse-sample.awk /tmp/sample.txt > /tmp/sample.folded
perl /tmp/FlameGraph/flamegraph.pl --title \"profile\" --countname \"samples\" /tmp/sample.folded > out.svg
```

## Collected flamegraphs (string/uniform_low)

All profiles below sample the `kString_vInt64_uniform_low_rows262144_blk65536_groups16` case.

### Native

![](images/2026-01-19-arrowcomputeagg-native-string-uniform_low.svg)

### TiForth (raw string keys)

![](images/2026-01-19-arrowcomputeagg-tiforth-string-uniform_low.svg)

### TiForthDictKey (shared dictionary keys)

![](images/2026-01-19-arrowcomputeagg-tiforthdictkey-string-uniform_low.svg)

## What the profiles show

### TiForth (raw `BinaryArray` keys): dominated by varlen key compare/hash

Hot stack (representative from `sample`):

```
GroupByNode::Consume
  GrouperFastImpl::ConsumeImpl
    SwissTable::find / run_comparisons
      KeyCompare::CompareColumnsToRows
        KeyCompare::CompareVarBinaryColumnToRowHelper
          _platform_memmove
```

Leaf hotspots (10s sample, approximate from folded stacks):

- `_platform_memmove` (inside `CompareVarBinaryColumnToRowHelper`)
- `arrow::compute::Hashing32::HashVarLenImp`
- `arrow::compute::KeyCompare::CompareVarBinaryColumnToRowHelper`
- `arrow::compute::SwissTable::early_filter`

Interpretation:

- Every input row needs (1) hash of the key bytes and (2) equality compare against candidate rows in the group table.
- For varlen keys this becomes a tight loop over offsets+bytes + memmove/memcpy/memcmp-style operations.

### TiFlash Native: dominated by its string hash-map batch hashing + key insertion/find

Native hot path is concentrated in:

- `DB::Aggregator::handleOneBatch<...AggregationMethodString...>`
- `DB::Aggregator::emplaceOrFindKey<...AggregationMethodString...>`
- `DB::ColumnsHashing::KeyStringBatchHandlerBase::prepareNextBatchType<...>`

Interpretation:

- TiFlash does a specialized string-key pipeline: batch hashing into a `PODArray<UInt64>` and a specialized `StringHashMap`/arena key holder.
- The per-row overhead is largely “hash + table probe” without Arrow’s row-table varbinary compare machinery.

### TiForthDictKey: varbinary compare disappears; grouped update becomes visible

With a shared `DictionaryArray` key column, the profile no longer shows `CompareVarBinary*` and heavy memmove.

Instead, time shifts to:

- `arrow::compute::SwissTable::early_filter/find/extract_group_ids`
- grouped aggregate update kernels (`GroupedReducingAggregator`, `GroupedCountImpl`, etc.)

Interpretation:

- Keys behave like fixed-width integers; grouping becomes a fast hash/probe on integer codes.

## Root cause hypothesis

The slowdown is primarily **representation + algorithmic overhead** in Arrow’s grouping for varlen keys:

- `BinaryArray` keys require **varlen hashing** and **varlen equality comparisons** against an internal row table of unique keys.
- The Arrow grouper implementation uses a Swiss-table + row-table layout; for varlen keys this triggers frequent varbinary compare logic (`CompareVarBinaryColumnToRowHelper`) and memmove/memcpy in the hot loop.

Why dict keys help:

- Feeding a `DictionaryArray` with a **stable dictionary** effectively turns the group key into a fixed-width integer index, avoiding the varbinary compare loop.

Why per-batch dictionary-encode doesn’t work today:

- Arrow/Acero currently errors on multi-batch input with differing dictionaries (`NotImplemented: Unifying differing dictionaries`), so streaming inputs where the dictionary grows/changes cannot simply `dictionary_encode` per batch.

## Follow-ups (actionable)

- Short term (bench/prototype): keep `TiForthDictKey` as a proof that “fixed-width group keys are fast” and use it to isolate remaining overheads.
- Medium term (TiForth operator): explore a **two-phase** mode for string keys:
  - buffer input until a stable dictionary is built (or until reaching a threshold), then run Acero aggregate on dict keys; trade off memory/latency for throughput.
- Long term (Arrow): upstream or track support for **dictionary unification** inside Grouper/GroupByNode so streamed `DictionaryArray` batches with differing dictionaries can be accepted efficiently.
