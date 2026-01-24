# TiForth Milestone 1: C++ Library Skeleton (Detailed Steps)

- Author(s): TBD
- Last Updated: 2026-01-17
- Status: Implemented
- Related design: `docs/design/2026-01-14-tiforth.md`
- Discussion PR: TBD
- Tracking Issue: TBD

## Scope Clarification

This document describes the detailed implementation steps for **Milestone 1** from the related design doc, with one adjustment:

- Milestone 1 is **C++-only** (TiFlash integrates via a C++ interface).
- The stable C ABI / cross-language FFI is deferred to a later milestone.

It also sets the build/link strategy for Arrow and tests.

## Goals (Milestone 1)

- TiForth (`tiforth`) is a **standalone / intact CMake project** that can be built independently of TiFlash.
- TiForth has **no dependency** on `dbms/` headers or libraries.
- TiForth builds as a **shared library by default** (`libtiforth.so` / `libtiforth.dylib`) and optionally supports static builds via `TIFORTH_LIBRARY_LINKAGE`.
- TiForth links **shared Arrow** (dynamic `libarrow`) and exports Arrow as a **public transitive dependency**.
- The host (TiFlash) links **only** TiForth (no direct Arrow dependency) and relies on TiForth's transitive Arrow to keep a single Arrow DSO in-process.
- Provide a minimal C++ API for `Engine` / `Pipeline` / `Task` with **no-op** execution (ABI shape only).
- Provide TiForth-only smoke tests for TiForth development (not wired into TiFlash tests).

## Non-goals (Milestone 1)

- No compute operators/functions are implemented.
- No Arrow `RecordBatch` / `RecordBatchReader` input/output surface yet (planned in a later milestone).
- No TiFlash planner/executor integration (only build + API scaffolding).
- No C ABI / cross-language FFI.
- No performance work.

## Deliverables

1. Repository layout under `libs/tiforth/`.
2. CMake targets:
   - `tiforth` (library; linkage via `TIFORTH_LIBRARY_LINKAGE`).
3. Public headers under `libs/tiforth/include/`.
4. Minimal no-op implementation:
   - `Engine` create/destroy,
   - `PipelineBuilder` finalize,
   - `Pipeline` task creation,
   - `Task::Step()` reaches a deterministic terminal state.
5. Standalone TiForth build works via `cmake -S libs/tiforth -B <build_dir>`.
6. A smoke test binary validating basic lifecycle (enabled only for TiForth development).
7. Smoke tests validating Arrow core + compute are linkable and usable (enabled only for TiForth development).
8. Consumer-facing example projects under `libs/tiforth/examples/` covering:
   - app uses Arrow or not,
   - Arrow system vs bundled (and static/shared when relevant),
   - TiForth system vs bundled (and static/shared).

## Step-by-step

### 0) Preconditions

- TiForth is built and tested standalone (`cmake -S libs/tiforth ...`).
- Arrow is required:
  - Default: build bundled Arrow **22.0.0** via FetchContent (`TIFORTH_ARROW_PROVIDER=bundled`).
  - System option: use system Arrow via `find_package(Arrow)` / `find_package(ArrowCompute)` (`TIFORTH_ARROW_PROVIDER=system`, must be Arrow 22.x).
  - Linkage: `TIFORTH_ARROW_LINKAGE=(shared|static)` selects shared/static Arrow linking.
- Tests are optional and controlled by `TIFORTH_BUILD_TESTS` (default `OFF`, top-level only).

### 1) Create directory skeleton

Create `libs/tiforth/` with a minimal library layout:

```
libs/tiforth/
  CMakeLists.txt
  cmake/
    find/
      arrow.cmake
      gtest.cmake
    testing/
      testing.cmake
  include/
    tiforth/
      tiforth.h
      engine.h
      pipeline.h
      task.h
  src/
    tiforth/
      CMakeLists.txt
      engine.cc
      pipeline.cc
      task.cc
  tests/
    CMakeLists.txt
    gtest_tiforth_smoke.cpp
  .clang-format
```

Notes:

- Keep all exported headers under `include/tiforth/…`.
- Follow Apache Arrow C++ style for naming, errors, and formatting (see related design doc).

### 2) Create a standalone CMake project (ara-style dependency wiring)

In `libs/tiforth/CMakeLists.txt`, define `tiforth` as a normal standalone CMake project and wire dependencies similarly to the `ara` project:

- define project options (`TIFORTH_ARROW_PROVIDER`, `TIFORTH_ARROW_LINKAGE`, `TIFORTH_BUILD_TESTS`)
- include dependency find modules under `cmake/find/`
- build the shared library under `src/tiforth/`
- enable tests only when `TIFORTH_BUILD_TESTS=ON`

### 3) Arrow linkage rule (simplified)

- TiForth is a shared library and links to **shared Arrow**.
- The host process must not load multiple different Arrow DSOs:
  - TiFlash must not link Arrow directly.
  - TiForth exports Arrow as a public transitive dependency, so the final process uses exactly one Arrow DSO.

### 4) Define minimal C++ API surface (Milestone 1)

Milestone 1 uses a C++ interface so TiFlash can integrate without a C ABI yet. Keep this surface intentionally small.

#### 4.1) Error handling

Follow Arrow conventions:

- return `arrow::Status` / `arrow::Result<T>`
- avoid exceptions as control flow

#### 4.2) Header split

Suggested headers:

- `include/tiforth/tiforth.h`: umbrella include + version macros
- `include/tiforth/engine.h`: `Engine` + `EngineOptions`
- `include/tiforth/pipeline.h`: `PipelineBuilder`, `Pipeline`
- `include/tiforth/task.h`: `Task` + `TaskState`

#### 4.3) Minimal types and methods

Recommended “shape” (no-op implementation in Milestone 1):

- `class Engine { public: static arrow::Result<std::unique_ptr<Engine>> Create(EngineOptions); };`
- `class PipelineBuilder { public: arrow::Result<std::unique_ptr<Pipeline>> Finalize(); };`
- `class Pipeline { public: arrow::Result<std::unique_ptr<Task>> CreateTask(); };`
- `enum class TaskState { kNeedInput, kHasOutput, kFinished, kBlocked };`
- `class Task { public: arrow::Result<TaskState> Step(); };`

Milestone 1 behavior:

- `Task::Step()` returns `TaskState::kFinished` immediately (or returns `NotImplemented`; choose one and document it).

### 5) Implement the no-op engine/pipeline/task

Implement minimal logic in `libs/tiforth/src/tiforth/`:

- `Engine` owns configuration and will later own registries/memory pool.
- `PipelineBuilder::Finalize()` returns an empty pipeline.
- `Pipeline::CreateTask()` returns a task whose `Step()` finishes immediately.

Keep implementation free of `dbms/` dependencies.

### 6) Add smoke tests

Add a minimal gtest binary under `libs/tiforth/tests/` that:

- creates/destroys an `Engine`
- creates a `PipelineBuilder`, finalizes it, creates a `Task`, and calls `Step()`
- asserts the no-op task reaches the documented terminal state

Add two additional smoke tests that validate Arrow dependencies are usable through TiForth's CMake wiring:

- Arrow struct smoke: build an `arrow::Array` via `arrow::Int32Builder`, validate values and `ToString()`.
- Arrow compute smoke: call an Arrow compute kernel (e.g. `arrow::compute::Add`) and validate results.

Keep TiForth tests independent from TiFlash tests:

- Gate TiForth tests behind `TIFORTH_BUILD_TESTS`.
- Do not wire TiForth tests into TiFlash’s test targets.

### 7) Definition of done (Milestone 1)

- Build:
  - `cmake -S libs/tiforth -B <build_dir> ...` configures successfully (standalone TiForth)
  - `cmake --build <build_dir> --target tiforth` builds successfully
- Isolation:
  - no `#include <dbms/...>` or `dbms/src/...` dependencies in `libs/tiforth/`
  - `tiforth` does not link against `dbms`
- Arrow dependency:
  - `tiforth` links shared Arrow (`Arrow::arrow_shared`)
- Tests:
  - `cmake -DTIFORTH_ARROW_PROVIDER=bundled -DTIFORTH_BUILD_TESTS=ON ...` configures successfully
  - `ctest --test-dir <build_dir>` passes
