# tiforth Milestone 1: C++ Library Skeleton (Detailed Steps)

- Author(s): TBD
- Last Updated: 2026-01-14
- Status: Draft
- Related design: `docs/design/2026-01-14-apache-arrow-internal-column-representation.md`
- Discussion PR: TBD
- Tracking Issue: TBD

## Scope Clarification

This document describes the detailed implementation steps for **Milestone 1** from the related design doc, with one adjustment:

- Milestone 1 is **C++-only** (TiFlash integrates via a C++ interface).
- The stable C ABI / cross-language FFI is deferred to a later milestone.

It also covers the build/link strategy for avoiding duplicated Arrow dependencies between TiFlash and `tiforth`.

## Goals (Milestone 1)

- `tiforth` builds as a standalone C++ library inside the TiFlash repo, with no dependency on `dbms/` headers.
- Provide a minimal C++ API for `Engine` / `Pipeline` / `Task` with **no-op** execution (ABI shape only).
- Build `tiforth` as both:
  - a **static** library (`tiforth_static`) that TiFlash links,
  - a **shared** library (`tiforth_shared`) for future embedding/testing (C++ ABI not guaranteed stable).
- `tiforth` links Apache Arrow (C++) itself.
- Avoid duplicated Arrow builds/symbols by making TiFlash and `tiforth` depend on the same Arrow target.
- Optionally (for non-TiFlash embedding), allow `tiforth_shared` to link Arrow **dynamically** (shared `libarrow`) so Arrow can be shared across DSOs in the same process.

## Non-goals (Milestone 1)

- No compute operators/functions are implemented.
- No Arrow C Stream import/export yet (planned in a later milestone).
- No TiFlash planner/executor integration (only build + API scaffolding).
- No C ABI / cross-language FFI.
- No performance work.

## Deliverables

1. Repository layout under `libs/tiforth/`.
2. CMake targets:
   - `tiforth_static` (static library; linked by TiFlash),
   - `tiforth_shared` (shared library; optional).
3. Public headers under `libs/tiforth/include/`.
4. Minimal no-op implementation:
   - `Engine` create/destroy,
   - `PipelineBuilder` finalize,
   - `Pipeline` task creation,
   - `Task::Step()` reaches a deterministic terminal state.
5. A smoke test binary validating basic lifecycle.

## Step-by-step

### 0) Preconditions

- Arrow is configured through TiFlash’s existing mechanism:
  - `-DENABLE_ARROW=ON`
  - internal Arrow (`contrib/arrow`) or a system `Arrow::arrow_static` (see `cmake/find_arrow.cmake`).
- `tiforth` must reuse the same Arrow selection used by TiFlash (`${ARROW_LIBRARY}`).
- If building `tiforth_shared` with dynamic Arrow, Arrow must also be available as a **shared** library target (see “3.4) Optional: dynamic Arrow for `tiforth_shared`”).

### 1) Create directory skeleton

Create `libs/tiforth/` with a minimal library layout:

```
libs/tiforth/
  CMakeLists.txt
  include/
    tiforth/
      tiforth.h
      engine.h
      pipeline.h
      task.h
  src/
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

### 2) Add CMake integration

#### 2.1) Add an option

Add a compile-time option (root `CMakeLists.txt`, near `ENABLE_ARROW`):

- `option(ENABLE_TIFORTH "Build tiforth compute library" OFF)`
- If `ENABLE_TIFORTH=ON` and `ENABLE_ARROW=OFF`, fail fast (or force-enable `ENABLE_ARROW`).

#### 2.2) Hook into `libs/`

In `libs/CMakeLists.txt`:

- `if (ENABLE_TIFORTH) add_subdirectory(tiforth) endif()`

This keeps `tiforth` in the “libs” layer and avoids pulling in `dbms/`.

#### 2.3) Define both `tiforth_static` and `tiforth_shared`

In `libs/tiforth/CMakeLists.txt`, build both library types from the same sources.

Recommended pattern (object library):

- `add_library(tiforth_obj OBJECT ...)`
- `set_property(TARGET tiforth_obj PROPERTY POSITION_INDEPENDENT_CODE ON)` (required for shared)
- `add_library(tiforth_static STATIC $<TARGET_OBJECTS:tiforth_obj>)`
- `add_library(tiforth_shared SHARED $<TARGET_OBJECTS:tiforth_obj>)`
- `set_target_properties(tiforth_static PROPERTIES OUTPUT_NAME tiforth)`
- `set_target_properties(tiforth_shared PROPERTIES OUTPUT_NAME tiforth)`

Usage requirements (apply to both `tiforth_static` and `tiforth_shared`):

- `target_include_directories(<each> PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)`
- `target_link_libraries(tiforth_static PUBLIC ${ARROW_LIBRARY})`
- `target_link_libraries(tiforth_shared PUBLIC ${ARROW_LIBRARY})` (default; static Arrow)
- Optional: link `tiforth_shared` against a shared Arrow target instead (see “3.4) Optional: dynamic Arrow for `tiforth_shared`”).

This satisfies “the library itself links Arrow too” and ensures consumers pick up Arrow transitively.

#### 2.4) Make TiFlash link `tiforth_static`

In `dbms/CMakeLists.txt`:

- `target_link_libraries(dbms PRIVATE tiforth_static)` when `ENABLE_TIFORTH=ON`.

Do not link `tiforth_shared` into TiFlash.

### 3) Solve duplicated Arrow dependency (TiFlash + tiforth)

This is the key requirement for Milestone 1: `tiforth` must not introduce a second Arrow build or a second copy of Arrow symbols in the TiFlash process.

#### 3.1) Build-time: single Arrow selection

Rules:

- `tiforth` must not fetch/build Arrow on its own.
- `tiforth` must link Arrow only via the CMake targets resolved by `cmake/find_arrow.cmake`:
  - `${ARROW_LIBRARY}`: the Arrow target used by TiFlash (static, today),
  - `${ARROW_SHARED_LIBRARY}`: optional shared-Arrow target (only when dynamic Arrow is enabled for `tiforth_shared`).
- `${ARROW_LIBRARY}` is resolved by `cmake/find_arrow.cmake` to either:
  - `tiflash_arrow` (bundled Arrow build), or
  - `Arrow::arrow_static` (system Arrow).

Outcome: Arrow selection stays centralized and explicit, and `tiforth` does not accidentally introduce an extra Arrow build.

#### 3.2) Link-time: keep Arrow on the TiFlash link line exactly once

Recommended policy for Milestone 1:

- `tiforth_static` links `${ARROW_LIBRARY}` as `PUBLIC`.
- TiFlash (`dbms`) does **not** link `${ARROW_LIBRARY}` directly when `ENABLE_TIFORTH=ON`; it gets Arrow transitively via `tiforth_static`.

If later some non-`tiforth` TiFlash code needs Arrow directly, it must still link `${ARROW_LIBRARY}` (same target). CMake can de-duplicate target-based dependencies.

#### 3.3) Runtime: shared `tiforth` + static Arrow caveat

Today, the bundled Arrow build is forced to static (`contrib/arrow-cmake` sets `ARROW_BUILD_SHARED=OFF`). If `tiforth_shared` links a static Arrow:

- Arrow symbols are embedded inside `libtiforth.so`/`libtiforth.dylib`.
- Any process that loads `tiforth_shared` must not also link another copy of Arrow C++ (static or shared), or you risk ODR/RTTI/global-singleton issues.

This is why TiFlash must link `tiforth_static` (single final binary ⇒ single Arrow instance).

If we later need “shared tiforth in a process that also links Arrow”, we must add a shared-Arrow build mode and make both depend on the same Arrow shared library.

#### 3.4) Optional: dynamic Arrow for `tiforth_shared`

This is for non-TiFlash embedding scenarios where the host process may also use Arrow and we want **one** Arrow instance shared via a DSO.

Add a build option:

- `option(TIFORTH_LINK_ARROW_SHARED "Link tiforth_shared against shared Arrow" OFF)`

Behavior:

- `tiforth_static` always links `${ARROW_LIBRARY}` (static Arrow target chosen by TiFlash).
- If `TIFORTH_LINK_ARROW_SHARED=ON`, `tiforth_shared` links `${ARROW_SHARED_LIBRARY}` (a shared Arrow target).

How to provide `${ARROW_SHARED_LIBRARY}`:

- **Internal Arrow** (default build): extend `contrib/arrow-cmake` to optionally build `arrow_shared`:
  - set `ARROW_BUILD_SHARED=ON` (and keep `ARROW_BUILD_STATIC=ON` so TiFlash still links static)
  - add an interface target `tiflash_arrow_shared` linking to `arrow_shared` / `Arrow::arrow_shared`
  - in `cmake/find_arrow.cmake`, set `ARROW_SHARED_LIBRARY=tiflash_arrow_shared`
- **System Arrow**: if `find_package(Arrow CONFIG)` provides `Arrow::arrow_shared` (or `Arrow::arrow` as shared), set `ARROW_SHARED_LIBRARY` to that target.
  - To avoid version/config mismatches, require `${ARROW_LIBRARY}` and `${ARROW_SHARED_LIBRARY}` to come from the same Arrow installation (i.e. system Arrow must provide both static + shared targets).

Constraints:

- When `TIFORTH_LINK_ARROW_SHARED=ON`, reject configurations where `${ARROW_SHARED_LIBRARY}` is not available.
- Do not mix `tiforth_shared` (shared Arrow) and `tiforth_static` (static Arrow) in the same process.

#### 3.5) Overall Arrow linkage matrix (what is valid)

- **TiFlash binary**: `dbms` + `tiforth_static` + `${ARROW_LIBRARY}` (static) ⇒ one Arrow instance (static) in the final executable.
- **Embedding via shared lib** (dynamic Arrow): `tiforth_shared` + `${ARROW_SHARED_LIBRARY}` (shared) ⇒ one Arrow instance (shared `libarrow`) in the process; host may also link the same shared Arrow.
- **Embedding via shared lib** (static Arrow): `tiforth_shared` + `${ARROW_LIBRARY}` (static) ⇒ Arrow is embedded in `libtiforth`; host must not link another Arrow.


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

Implement minimal logic in `libs/tiforth/src/`:

- `Engine` owns configuration and will later own registries/memory pool.
- `PipelineBuilder::Finalize()` returns an empty pipeline.
- `Pipeline::CreateTask()` returns a task whose `Step()` finishes immediately.

Keep implementation free of `dbms/` dependencies.

### 6) Add smoke tests

Add a minimal gtest binary under `libs/tiforth/tests/` that:

- creates/destroys an `Engine`
- creates a `PipelineBuilder`, finalizes it, creates a `Task`, and calls `Step()`
- asserts the no-op task reaches the documented terminal state

### 7) Definition of done (Milestone 1)

- Build:
  - `cmake -DENABLE_ARROW=ON -DENABLE_TIFORTH=ON ...` configures successfully
  - `ninja tiforth_static tiforth_shared` builds successfully
- Isolation:
  - no `#include <dbms/...>` or `dbms/src/...` dependencies in `libs/tiforth/`
  - `tiforth_*` targets do not link against `dbms`
- Arrow dependency:
  - `tiforth_*` link `${ARROW_LIBRARY}`
  - TiFlash links `tiforth_static` (not `tiforth_shared`)
  - no second Arrow build is introduced by `tiforth`
- Tests:
  - `ctest -R tiforth` (or equivalent check target) passes
