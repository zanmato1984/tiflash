# Overall Work Objectives

- Deliver TiForth as an independent Arrow-native compute library (standalone repo `zanmato1984/tiforth`, consumed by TiFlash via FetchContent / local `libs/tiforth` checkout) with stable host APIs (Engine/Pipeline/Task), Arrow compute integration, and a minimal C ABI surface.
- Integrate TiForth into TiFlash behind `TIFLASH_ENABLE_TIFORTH` with Block<->Arrow bridging, DAG->pipeline translation, and gtests covering a common TiFlash DAG shape.
- Preserve TiFlash/TiDB semantics via a clear logical-type contract (Arrow field metadata) for tricky types (decimals, packed MyTime temporals, collated strings: compare/hash/sort keys).
- Route TiForth allocations through host-provided `arrow::MemoryPool` to support accounting/limits/spill hooks.

# Work in Progress

- Route remaining HashAgg state allocations through `arrow::MemoryPool` (replace `std::vector` w/ Arrow buffers or PMR vectors using pool resource; keep strict accounting)

# To Do

- Reuse `detail::Arena` + `detail::KeyHashTable` for hash join build side keying (shared key encoding; replace any node-based maps)
- Reduce remaining key-path heap allocations (replace scratch `std::string` with pool-backed buffer builder; store output binary keys as arena slices, not `pmr::string`)
- Add stress/perf tests for key hash table (rehash, high cardinality, collision-y keys) and run TiForth+TiFlash test suites

# Completed

- TiForth core + tests (MS1-11): Arrow-native Engine/Pipeline/Task APIs, expression compilation, operators (projection/filter/hash agg/hash join/sort), type mapping + logical-type metadata (decimal/MyTime/collations), memory pool plumbing, and C ABI scaffolding. Plan breaker infrastructure (multi-stage `Plan`, TiFlash-shaped `OperatorStatus`/`TaskState`) and hash-agg breaker split (`HashAggContext` + build sink + convergent source) are implemented and covered by TiForth gtests.
- TiFlash integration + tests: builds/consumes TiForth behind `TIFLASH_ENABLE_TIFORTH`; Block<->Arrow bridge + runner; `enable_tiforth_executor` pass-through execution path; DAG→TiForth translation smoke tests (incl breaker hash agg plan) and TiFlash parity gtests for filter+agg (collations) + arithmetic. Note: end-to-end blocked IO/await states are not wired; production translation is still incremental.
- Function parity: custom Arrow compute scalar kernels + compile-time rewrites to match TiDB/TiFlash semantics (numeric/bitwise + decimal arithmetic), with TiForth unit tests, TiFlash parity gtests, and operator/function docs updates.
- Hash agg key hash table port (TiForth `147d4be`): added Arrow-pool `detail::Arena` + open-addressing `detail::KeyHashTable` (saved hash, linear probing) and refactored `HashAggContext` to encode normalized key bytes into scratch, probe before copying, and only materialize/store first-seen raw keys on insert; global agg bypass. TiFlash pin bumped to `147d4be`. Docs updated. Tests: `ninja -C cmake-build-debug gtests_dbms` + `cmake-build-debug/dbms/gtests_dbms --gtest_filter='TiForthPipelineTranslateTest.*:TiForthFilterAggParityTestRunner.*'`. Files: `libs/CMakeLists.txt`, `docs/design/2026-01-14-tiforth-milestone-5-hash-aggregation.md`, `docs/design/2026-01-14-tiforth-milestone-5-hash-aggregation-hash-table.md`, `.codex/progress/daemon.md`. Notes: scratch key buffers still use `std::string`; output binary keys still store `std::pmr::string` (pool-backed) only on insert.
- Hash agg aggregate-function framework (TiForth `a354622`): added internal `detail::AggregateFunction` + `detail::ComputeAggStateLayout`; `HashAggContext` now allocates a per-group aggregate state row from an Arrow-pool arena and routes `count_all/count/sum/min/max` through `Create/Add/Finalize/Destroy` instead of ad-hoc per-agg vectors. TiFlash pin bumped to `a354622`. Docs updated. Tests: `cmake --build /Users/zanmato/dev/tiforth/build-debug && ctest --test-dir /Users/zanmato/dev/tiforth/build-debug`; `ninja -C cmake-build-debug gtests_dbms && LOG_LEVEL=error cmake-build-debug/dbms/gtests_dbms --gtest_filter='TiForthPipelineTranslateTest.*:TiForthFilterAggParityTestRunner.*'`. Files: (tiforth) `include/tiforth/detail/aggregate_function.h`, `include/tiforth/operators/hash_agg.h`, `src/tiforth/operators/hash_agg.cc`; (tiflash) `libs/CMakeLists.txt`, `docs/design/2026-01-14-tiforth-milestone-5-hash-aggregation-agg-framework.md`, `.codex/progress/daemon.md`. Notes: group key output storage still uses `std::pmr::string` (pool-backed); remaining `std::vector` and scratch buffers still use the default allocator.
