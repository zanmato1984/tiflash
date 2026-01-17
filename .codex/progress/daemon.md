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
- TiForth + TiFlash (MS1-10 common path): Arrow-native Engine/Pipeline/Task + operators + function registry + C ABI; TiDB semantics via Arrow field metadata (collations/decimals/MyTime); memory pool propagation; TiFlash guarded integration behind `ENABLE_TIFORTH` with Block<->Arrow bridge + DAG->pipeline translation + gtests. Notes: objective review done; no remaining To Do.
- Docs + packaging: milestone/design docs reconciled; user-facing docs under `libs/tiforth/docs`; install-tree hygiene (no internal headers) + `verify_install.sh` consumer check.
- Repo split: extracted `libs/tiforth` history to standalone repo `zanmato1984/tiforth` (split tip `3c68077ade7d0c4ee0677b732ff80e7d02daabe6`), cloned to `~/dev/tiforth`; TiFlash updated for external TiForth (ignore `libs/tiforth`, FetchContent fallback to the new repo when in-tree source missing). Files: `.gitignore`, `libs/CMakeLists.txt`, `.codex/progress/daemon.md`. Checks: `cmake --preset debug && cmake --build --preset debug && ctest --preset debug` (tiforth repo), `ninja -C cmake-build-tiflash-tiforth-debug gtests_dbms && cmake-build-tiflash-tiforth-debug/dbms/gtests_dbms --gtest_filter=TiForth*`.
