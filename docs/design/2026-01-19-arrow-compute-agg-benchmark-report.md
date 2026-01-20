# ArrowComputeAgg Bench Report (2026-01-20)

This report summarizes `dbms/bench_dbms` `ArrowComputeAgg/*` benchmarks comparing:

- **Native**: TiFlash `DB::Aggregator` (spill disabled, `concurrency=1`)
- **TiForth**: `tiforth::ArrowComputeAggTransformOp` via Arrow Acero (`use_threads=true`)
- **TiForthDictKey**: same as TiForth, but for **string keys** the key column is a `DictionaryArray` with a **shared dictionary** across all input batches
- **TiForthStableDictKey**: same as TiForth, but enable `ArrowComputeAggOptions.stable_dictionary_encode_binary_keys` (varlen keys are encoded into stable `int32` codes via `dictionary_encode` + `DictionaryUnifier`, aggregated as fixed-width, then decoded on output)

## Environment

- Git describe: `v4.1.0-alpha-3798-g603cd4bc9b-dirty` (branch `tiforth`)
- Build dir: `cmake-build-release` (`CMAKE_BUILD_TYPE=RELEASE`, `ENABLE_TIFORTH=ON`, `NO_WERROR=ON`)
- OS: macOS 15.7.3 (Darwin 24.6.0, arm64)
- CPU: Apple M1 Pro (`hw.ncpu=10`)
- Compiler: Apple clang 17.0.0
- Benchmark run time: `2026-01-20T12:57:44+08:00`

## How to reproduce

```bash
ninja -C cmake-build-release bench_dbms
cmake-build-release/dbms/bench_dbms --benchmark_filter='^ArrowComputeAgg/.*' --benchmark_min_time=0.2
```

## Benchmark configuration

- Source: `dbms/src/Flash/tests/bench_tiforth_arrow_compute_agg.cpp`
- Query shape: `GROUP BY k` with aggregates:
  - `count(v)`
  - `sum(v)`
- Input sizes:
  - numeric key/value cases: `rows=1<<20` (1048576), `rows_per_block=1<<16` (65536)
  - string key cases: `rows=1<<18` (262144), `rows_per_block=1<<16` (65536)
- Key distributions:
  - `single`: all keys are `0`
  - `uniform_low`: uniform random key in `[0, groups)` (groups=16)
  - `uniform_high`: unique key per row (`key_id = row`; `groups` shown as 0 in case id)
  - `zipf`: Zipf(n=`groups`, s=1.1) (groups=1024)

Notes:

- Block→Arrow conversion for TiForth (`TiForth::toArrowRecordBatch`) is done once per dataset, outside the timed loop.
- `TiForthDictKey/*` constructs dictionary keys once per dataset, outside the timed loop.
- TiForth uses Acero `QueryOptions.use_threads=true`, so grouped aggregation may use Arrow’s CPU thread pool internally; Native is explicitly `concurrency=1`.

## Results (items/s)

`items/s` counts **input rows processed** per second (Google Benchmark `items_per_second`). Values below are in **M rows/s**.

### Numeric keys

| Case | Native (M/s) | TiForth (M/s) | TiForth/Native |
|---|---:|---:|---:|
| `kInt32_vInt64_single_rows1048576_blk65536_groups1` | 143.2 | 122.9 | 0.86x |
| `kInt32_vInt64_uniform_low_rows1048576_blk65536_groups16` | 177.6 | 188.7 | 1.06x |
| `kInt32_vInt64_uniform_high_rows1048576_blk65536_groups0` | 23.0 | 26.3 | 1.15x |
| `kInt32_vInt64_zipf_rows1048576_blk65536_groups1024` | 162.4 | 193.1 | 1.19x |
| `kInt64_vFloat64_uniform_low_rows1048576_blk65536_groups16` | 182.3 | 197.6 | 1.08x |
| `kInt64_vFloat64_zipf_rows1048576_blk65536_groups1024` | 159.6 | 196.2 | 1.23x |

### String keys

| Case | Native (M/s) | TiForth (M/s) | TiForth/Native | TiForthDictKey (M/s) | DictKey/Native | TiForthStableDictKey (M/s) | StableDictKey/Native |
|---|---:|---:|---:|---:|---:|---:|---:|
| `kString_vInt64_uniform_low_rows262144_blk65536_groups16` | 155.4 | 69.3 | 0.45x | 192.6 | 1.24x | 46.4 | 0.30x |
| `kString_vInt64_zipf_rows262144_blk65536_groups1024` | 119.7 | 56.5 | 0.47x | 186.4 | 1.56x | 41.7 | 0.35x |

## Observations / notes

- For numeric keys, TiForth/Acero is generally faster on higher-cardinality and skewed distributions, but slower on the single-group case (likely overhead/parallelism tradeoff).
- For string keys, TiForth with plain `BinaryArray` keys is ~2-3x slower than Native in these runs.
- Switching string keys to a **shared-dictionary** `DictionaryArray` makes TiForth significantly faster than Native.
- `TiForthStableDictKey` currently does not help: key encoding (`dictionary_encode` + unification + remap) dominates, so end-to-end is slower than both TiForth(raw) and Native for these cases.
