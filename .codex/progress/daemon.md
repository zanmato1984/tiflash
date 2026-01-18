# Overall Work Objectives

- Deliver TiForth as an independent Arrow-native compute library (standalone repo `zanmato1984/tiforth`, consumed by TiFlash via FetchContent / local `libs/tiforth` checkout) with stable host APIs (Engine/Pipeline/Task), Arrow compute integration, and a minimal C ABI surface.
- Integrate TiForth into TiFlash behind `TIFLASH_ENABLE_TIFORTH` with Block<->Arrow bridging, DAG->pipeline translation, and gtests covering a common TiFlash DAG shape.
- Preserve TiFlash/TiDB semantics via a clear logical-type contract (Arrow field metadata) for tricky types (decimals, packed MyTime temporals, collated strings: compare/hash/sort keys).
- Route TiForth allocations through host-provided `arrow::MemoryPool` to support accounting/limits/spill hooks.

# Work in Progress

# To Do

- Based on the current implementation, thoroughly review the “Overall Work Objectives” and confirm all objectives have been achieved. If any objective has not been achieved, break it into actionable tasks and add them to **To Do**. If all objectives have been achieved, analyze shortcomings of the current implementation (e.g. code simplicity, engineering quality, readability, testability, etc.) and derive a new **To Do** list.

# Completed

- TiForth core + TiFlash integration (MS1-11 common path): Engine/Pipeline/Task APIs, expression compilation, operators (projection/filter/hash agg/hash join/sort), breaker `Plan` scaffolding, and C ABI scaffolding. TiFlash consumes TiForth behind `TIFLASH_ENABLE_TIFORTH` with Block<->Arrow bridge, DAG→pipeline translation, and gtests for a common DAG shape. Note: blocked IO/await states are not wired; production translation remains incremental.
- Semantics + type contract: logical-type metadata contract for decimals/MyTime/collations; collation compare/sort-key/hashing; custom Arrow compute scalar kernels + rewrites for TiDB/TiFlash semantics (incl decimal arithmetic), with TiForth unit tests + TiFlash parity gtests.
- Hash keying performance + tests (TiForth pinned `1ae1b8a`): arena-backed open-addressing `detail::KeyHashTable` reused by hash agg + hash join; aggregate state layout framework and pooled containers; scratch key encoding uses Arrow-pool-backed `detail::ScratchBytes` and hash-agg output `binary` keys are arena slices (`detail::ByteSlice`). Added `KeyHashTable` stress tests and hash-agg memory-pool gtest. TiFlash pins TiForth to `1ae1b8a`. Checks: `cmake --build /Users/zanmato/dev/tiforth/build-debug && ctest --test-dir /Users/zanmato/dev/tiforth/build-debug`; `ninja -C cmake-build-debug gtests_dbms && LOG_LEVEL=error cmake-build-debug/dbms/gtests_dbms --gtest_filter='TiForthPipelineTranslateTest.*:TiForthFilterAggParityTestRunner.*'`.
- Docs sync (TiForth `8cefd3a`): updated `tiforth/docs/*` and `examples/README.md` to remove TiFlash `libs/tiforth` path assumptions and refresh hash agg/join operator coverage notes. Tests: `ctest --test-dir /Users/zanmato/dev/tiforth/build-debug`. Files: (tiforth) `docs/{README,architecture,operators_and_functions,pipeline_apis,type_mapping_tidb_to_arrow}.md`, `examples/README.md`.
- Cleanup (TiForth `1ae1b8a`): deduplicated `ArrowMemoryPoolResource` into `include/tiforth/detail/arrow_memory_pool_resource.h` and reused it in hash agg/join/sort. Tests: `cmake --build /Users/zanmato/dev/tiforth/build-debug && ctest --test-dir /Users/zanmato/dev/tiforth/build-debug`; `ninja -C cmake-build-debug gtests_dbms && LOG_LEVEL=error cmake-build-debug/dbms/gtests_dbms --gtest_filter='TiForthPipelineTranslateTest.*:TiForthFilterAggParityTestRunner.*'`.
