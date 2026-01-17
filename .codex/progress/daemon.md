# Overall Work Objectives

- Deliver TiForth as an independent Arrow-native compute library (standalone repo `zanmato1984/tiforth`, consumed by TiFlash via FetchContent / local `libs/tiforth` checkout) with stable host APIs (Engine/Pipeline/Task), Arrow compute integration, and a minimal C ABI surface.
- Integrate TiForth into TiFlash behind `TIFLASH_ENABLE_TIFORTH` with Block<->Arrow bridging, DAG->pipeline translation, and gtests covering a common TiFlash DAG shape.
- Preserve TiFlash/TiDB semantics via a clear logical-type contract (Arrow field metadata) for tricky types (decimals, packed MyTime temporals, collated strings: compare/hash/sort keys).
- Route TiForth allocations through host-provided `arrow::MemoryPool` to support accounting/limits/spill hooks.

# Work in Progress

# (none)

# To Do

# (none)

# Completed
- TiFlash TiForth executor mode (pass-through) + tri-mode gtests: added runtime setting `enable_tiforth_executor`; `queryExecute` gains a TiForth path (guarded) that currently supports only pass-through DAG (single TableScan/PartitionTableScan/ExchangeReceiver) and throws `NOT_IMPLEMENTED` otherwise; added `DB::TiForth::TiForthQueryExecutor` (BlockInputStream -> Blocks, runs a pass-through TiForth pipeline via `RunTiForthPipelineOnBlocks`); extended `ExecutorTest` runner from bool to 3-value state (dag/pipeline/tiforth) and updated existing `{false,true}` wrappers; added a minimal table-scan pass-through gtest. Decisions: keep unsupported DAG shapes failing (visibility for missing operators); keep implementation Block-based and buffered (tests first). Files: `dbms/src/Interpreters/Settings.h`, `dbms/src/Flash/executeQuery.cpp`, `dbms/src/Flash/TiForth/TiForthQueryExecutor.h`, `dbms/src/Flash/TiForth/TiForthQueryExecutor.cpp`, `dbms/src/TestUtils/ExecutorTestUtils.h`, `dbms/src/TestUtils/ExecutorTestUtils.cpp`, `dbms/src/Flash/tests/gtest_tiforth_query_executor_passthrough.cpp`, `docs/design/2026-01-14-tiforth.md`, `docs/design/2026-01-14-tiforth-milestone-9-host-integration.md`, `.codex/progress/daemon.md`. Checks: `cmake -S . -B cmake-build-tiflash-tiforth-debug`, `ninja -C cmake-build-tiflash-tiforth-debug gtests_dbms`, `gtests_dbms --gtest_filter=TiForthQueryExecutorPassThroughTestRunner.TableScanPassThrough`. Notes: many existing gtests will fail in tiforth mode until DAG->TiForth translation/operator coverage is expanded. Commit: `685723ca74`.
- TiForth + TiFlash (MS1-10 common path): Arrow-native Engine/Pipeline/Task + operators + function registry + C ABI; TiDB semantics via Arrow field metadata (collations/decimals/MyTime); memory pool propagation; TiFlash guarded integration behind `ENABLE_TIFORTH` with Block<->Arrow bridge + DAG->pipeline translation + gtests. Notes: objective review done; no remaining To Do.
- Docs + packaging: milestone/design docs reconciled; user-facing docs under `libs/tiforth/docs`; install-tree hygiene (no internal headers) + `verify_install.sh` consumer check.
- Repo split: extracted `libs/tiforth` history to standalone repo `zanmato1984/tiforth` (split tip `3c68077ade7d0c4ee0677b732ff80e7d02daabe6`), cloned to `~/dev/tiforth`; TiFlash updated for external TiForth (ignore `libs/tiforth`, FetchContent fallback to the new repo when in-tree source missing). Files: `.gitignore`, `libs/CMakeLists.txt`, `.codex/progress/daemon.md`. Checks: `cmake --preset debug && cmake --build --preset debug && ctest --preset debug` (tiforth repo), `ninja -C cmake-build-tiflash-tiforth-debug gtests_dbms && cmake-build-tiflash-tiforth-debug/dbms/gtests_dbms --gtest_filter=TiForth*`.
