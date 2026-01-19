# Overall Work Objectives

- Deliver TiForth as an independent Arrow-native compute library (standalone repo `zanmato1984/tiforth`, consumed by TiFlash via FetchContent / local `libs/tiforth` checkout) with stable host APIs (Engine/Pipeline/Task), Arrow compute integration, and a minimal C ABI surface.
- Integrate TiForth into TiFlash behind `TIFLASH_ENABLE_TIFORTH` with Block<->Arrow bridging, DAG->pipeline translation, and gtests covering a common TiFlash DAG shape.
- Preserve TiFlash/TiDB semantics via a clear logical-type contract (Arrow field metadata) for tricky types (decimals, packed MyTime temporals, collated strings: compare/hash/sort keys).
- Route TiForth allocations through host-provided `arrow::MemoryPool` to support accounting/limits/spill hooks.

# Work in Progress

# To Do

- Review “Overall Work Objectives” against current implementation; if any objective is not achieved, break it into actionable tasks and add them here; otherwise derive a new To Do list from current shortcomings (simplicity, readability, testability, perf gaps, missing operators).

# Completed

- TiForth baseline delivered (MS1-13, 2026-01-19): standalone `tiforth` library with Engine/Pipeline/Plan/Task + C ABI skeleton; core operators (projection/filter/hash agg/join/sort) and breaker plan; Arrow-native type contract via field metadata (decimals/MyTime/collations) with custom Arrow compute kernels for TiDB/TiFlash semantics; memory pool routed through host-provided `arrow::MemoryPool`. TiFlash integrates behind `TIFLASH_ENABLE_TIFORTH` with Block<->Arrow bridge + DAG→pipeline translation and parity gtests for a common DAG shape.
- TiForth “full aggregation core” progress (MS12/MS12A, 2026-01-19): ported TiFlash-shaped aggregation method selection and optimized hash tables (dense direct-map, fixed-key open addressing, single-string fast path; multi-key via serialized bytes in `detail::KeyHashTable`), plus broader aggregate coverage (decimal sum/avg inference + avg state, float min/max NaN alignment, metadata upsert). TiFlash parity gtests extended for avg/decimal/float and TiFlash pins TiForth accordingly.
- TiForth task blocked-state model (MS13, 2026-01-19): wired `Pipeline::MakeReader()` blocked states (IOIn/IOOut via `ExecuteIO`, Waiting via `Await`, WaitForNotify host-driven) and added `PilotAsyncTransformOp` + gtests for IO/error/notify propagation; docs/design updated.
- MS14 Arrow-compute group aggregation + benchmarks (2026-01-19): implemented TiForth `ArrowComputeAggTransformOp` via Arrow Acero (`table_source` + `aggregate`) because Arrow `hash_*` aggregate kernels cannot be executed via `CallFunction`; enabled ARROW_ACERO for the bundled Arrow build and linked `arrow_acero`. Added TiFlash `bench_dbms` benchmarks comparing TiFlash native aggregation vs TiForth Arrow-compute aggregation across types + key distributions (no spill). Validation: TiForth `ctest` pass; TiFlash `gtests_dbms`/`bench_dbms` build ok. Benchmark (DEBUG): TiForth ~2x on low-cardinality ints + Zipf, ~same on single-group, ~1.2x on strings. Files: `docs/design/2026-01-14-tiforth-milestone-14-arrow-compute-aggregation.md`, `docs/design/2026-01-14-tiforth.md`, `dbms/src/Flash/tests/bench_tiforth_arrow_compute_agg.cpp`, `dbms/CMakeLists.txt`; TiForth commit `0224a45`. Notes: operator currently buffers full input into a Table and only supports `FieldRef` (no computed expressions yet).
