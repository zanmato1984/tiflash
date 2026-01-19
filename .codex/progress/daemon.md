# Overall Work Objectives

- Deliver TiForth as an independent Arrow-native compute library (standalone repo `zanmato1984/tiforth`, consumed by TiFlash via FetchContent / local `libs/tiforth` checkout) with stable host APIs (Engine/Pipeline/Task), Arrow compute integration, and a minimal C ABI surface.
- Integrate TiForth into TiFlash behind `TIFLASH_ENABLE_TIFORTH` with Block<->Arrow bridging, DAG->pipeline translation, and gtests covering a common TiFlash DAG shape.
- Preserve TiFlash/TiDB semantics via a clear logical-type contract (Arrow field metadata) for tricky types (decimals, packed MyTime temporals, collated strings: compare/hash/sort keys).
- Route TiForth allocations through host-provided `arrow::MemoryPool` to support accounting/limits/spill hooks.

# Work in Progress

# To Do

- Wire `enable_tiforth_arrow_compute_agg` into real TiFlash TiForth executor once non-pass-through DAG→TiForth translation is enabled (today only test harness translation).
- Decide next step for string-key perf: keep dict-key as benchmark-only, or add an optional “build stable dictionary then aggregate” mode / fallback-to-HashAgg heuristic for string keys; track Arrow’s lack of multi-batch dictionary unification.

# Completed

- TiForth baseline (MS1-13, 2026-01-19): delivered standalone `tiforth` library (Engine/Pipeline/Plan/Task + C ABI skeleton), core operators (projection/filter/hash agg/join/sort) with breaker plan semantics and blocked-state model, Arrow logical-type metadata contract (decimal/MyTime/collations) and custom kernels for TiDB/TiFlash semantics, and TiFlash integration behind `TIFLASH_ENABLE_TIFORTH` (Block<->Arrow bridge + DAG→pipeline translation + parity gtests). Key decision: keep TiForth independent (no TiFlash assumptions) while matching Arrow coding style + rigid checks. Notes: full aggregation core includes TiFlash-shaped method selection + optimized hash tables. 
- MS14 Arrow-compute agg + benchmarks (2026-01-19): implemented TiForth `ArrowComputeAggTransformOp` via Arrow Acero (streaming input; supports compiled expressions via pre-projection; outputs reordered aggs-then-keys) and added TiFlash `bench_dbms` coverage + markdown report + string-key profiling. Found performance profile: numeric keys often faster than Native; string keys slow on raw varbinary but fast with shared-dictionary keys; Arrow cannot unify differing dictionaries across batches. Flamegraph root cause: Arrow grouper hot path on string keys is `GrouperFastImpl` + `SwissTable` with heavy varlen hashing (`HashVarLenImp`) and varbinary compares (`KeyCompare::CompareVarBinaryColumnToRowHelper` + memmove/memcpy); dict keys remove varbinary compare. Files: `libs/tiforth/*` (operator + tests), `dbms/src/Flash/tests/bench_tiforth_arrow_compute_agg.cpp`, `docs/design/2026-01-19-arrow-compute-agg-benchmark-report.md`, `docs/design/2026-01-19-arrow-compute-agg-string-key-profiling.md`, `docs/design/images/2026-01-19-arrowcomputeagg-*-string-uniform_low.svg`.
- TiFlash Arrow-compute agg A/B switch (2026-01-19): added runtime setting `enable_tiforth_arrow_compute_agg`, wired it into the TiFlash-shaped DAG→TiForth pipeline translation test harness to toggle `HashAggTransformOp` vs `ArrowComputeAggTransformOp`, and added a parity gtest for group-by sum (setting flipped on). Validation: `gtests_dbms` + targeted filters pass; branch pushed to `ruoxi/tiforth`. Files: `dbms/src/Interpreters/Settings.h`, `dbms/src/Flash/tests/gtest_tiforth_filter_agg_parity.cpp`, `dbms/src/Flash/tests/gtest_tiforth_pipeline_translate.cpp`, `.codex/progress/daemon.md`.
