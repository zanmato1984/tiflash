# Work in Progress
- (none)

# To Do
- (none)

# Completed
- TiForth: reuse parent Arrow targets; skip Find/Fetch. Simplified parent-target handling to accept just core `arrow`/`arrow_shared` and optionally compute/testing. Files: libs/tiforth/cmake/find/arrow.cmake, libs/tiforth/src/tiforth/CMakeLists.txt
- TiForth tests: link ArrowTesting library when enabled. Bundled Arrow test mode now enables minimal Arrow options needed for ArrowTesting (IPC+JSON) and avoids system gtest mismatch by reusing already-built `gtest` targets. Files: libs/tiforth/cmake/testing/testing.cmake, libs/tiforth/cmake/find/gtest.cmake, libs/tiforth/cmake/find/arrow.cmake
- TiFlash: stop assuming vendored Arrow for TiForth; removed TiFlash-side `FETCHCONTENT_SOURCE_DIR_ARROW` override. Note: offline TiFlash builds may need `-DFETCHCONTENT_SOURCE_DIR_ARROW=...` or parent-provided Arrow targets. Files: libs/CMakeLists.txt
- TiForth: support building as static or shared via `TIFORTH_LIBRARY_LINKAGE`; TiFlash selects via `TIFLASH_TIFORTH_LINKAGE` (auto picks static when `USE_STATIC_LIBRARIES`/`MAKE_STATIC_LIBRARIES` is ON). Files: libs/tiforth/CMakeLists.txt, libs/tiforth/src/tiforth/CMakeLists.txt, libs/tiforth/cmake/tiforthConfig.cmake.in, CMakeLists.txt, libs/CMakeLists.txt
- TiForth: Arrow compute is required for `tiforth` itself (not test-only). Always configure ArrowCompute and link compute target; parent-provided Arrow must also provide/propagate compute. Files: libs/tiforth/cmake/find/arrow.cmake
- TiForth: simplify gtest handling; don't rely on parent-project `GTest::` targets (tests are top-level only). Files: libs/tiforth/cmake/find/gtest.cmake
- TiForth: Arrow CMake polish: remove parent-project assumptions; broaden parent/system ArrowTesting target detection; drop redundant system compute re-selection. Files: libs/tiforth/cmake/find/arrow.cmake
- Validation: `cmake`/`ninja`/`ctest` OK for `libs/tiforth/build-debug`; TiFlash integration config OK and `ninja -C cmake-build-tiflash-tiforth tiforth` OK. Notes: building full `tiflash` blocked by disk-full when rebuilding `tiflash-proxy` via cargo; ccache needs `CCACHE_DIR` under workspace in sandbox. Files: (none)
- Review: no explicit “Overall Work Objectives” section in `/.codex/progress/daemon.md`; treated current CMake integration tasks as objectives; no follow-up To Do derived.
