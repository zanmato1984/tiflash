# Work in Progress
- (none)

# To Do
- (none)

# Completed
- TiForth build + Arrow: always require ArrowCompute; parent Arrow target reuse now requires separate compute target and forbids `TIFORTH_BUILD_TESTS`; tests explicitly link ArrowTesting; package config always depends on ArrowCompute; simplify linkage validation. Files: libs/tiforth/CMakeLists.txt, libs/tiforth/cmake/find/arrow.cmake, libs/tiforth/cmake/find/gtest.cmake, libs/tiforth/cmake/testing/testing.cmake, libs/tiforth/cmake/tiforthConfig.cmake.in, libs/tiforth/src/tiforth/CMakeLists.txt
- TiFlash integration + Arrow removal: replace old `ENABLE_ARROW` plumbing with `ENABLE_TIFORTH`; stop assuming in-tree Arrow; remove Arrow submodule + NOTICE entry. Files: CMakeLists.txt, libs/CMakeLists.txt, dbms/CMakeLists.txt, dbms/src/Server/CMakeLists.txt, dbms/src/Flash/tests/gtest_tiforth_integration.cpp, contrib/CMakeLists.txt, NOTICE, .gitmodules; deleted: cmake/find_arrow.cmake, contrib/arrow, contrib/arrow-cmake/CMakeLists.txt
- Validation: `ctest --test-dir libs/tiforth/build-debug` OK; TiFlash configure OK; `ninja -C cmake-build-tiflash-tiforth tiforth` OK (used `-DFETCHCONTENT_SOURCE_DIR_ARROW=libs/tiforth/build-debug/_deps/arrow-src` after removing contrib/arrow). Notes: full TiFlash build may still be disk-heavy due to `tiflash-proxy`.
- TiForth: simplify googletest discovery for tests: bundled Arrow requires `gtest`/`gtest_main` targets and provides `GTest::gtest` aliases; system Arrow uses `find_package(GTest)`. Decision: no parent/outer gtest assumptions since tests are top-level only. Files: libs/tiforth/cmake/find/gtest.cmake
- Validation + ship: `cmake -S libs/tiforth -B libs/tiforth/build-debug -DTIFORTH_BUILD_TESTS=ON` + `ninja -C libs/tiforth/build-debug` + `ctest --test-dir libs/tiforth/build-debug` OK; commit. Files: .codex/progress/daemon.md
