# TiForth Milestone 2: Arrow RecordBatch Plumbing + Pass-Through Pipeline (Detailed Steps)

- Author(s): TBD
- Last Updated: 2026-01-17
- Status: Implemented
- Related design: `docs/design/2026-01-14-tiforth.md`
- Related milestone: `docs/design/2026-01-14-tiforth-milestone-1-skeleton.md`
- Discussion PR: TBD
- Tracking Issue: TBD

## Scope Clarification

This milestone focuses on making TiForth pipeline execution **Arrow-native** at the boundary by:

- accepting input as `arrow::RecordBatch` and/or `arrow::RecordBatchReader`,
- producing output as `arrow::RecordBatch` and/or `arrow::RecordBatchReader`,
- implementing a single built-in operator: **pass-through** (identity).

This is intentionally a “hello pipeline” milestone to validate the end-to-end plumbing before porting real operators.

## Goals (Milestone 2)

- Keep TiForth (`tiforth`) **independent** from TiFlash DBMS code (`dbms/`): no headers, no linking.
- Extend the public C++ API so a host can:
  - push `arrow::RecordBatch` into a `Task`,
  - pull `arrow::RecordBatch` out of a `Task`,
  - (optionally) drive the same flow via `arrow::RecordBatchReader`.
- Define deterministic, host-friendly task stepping semantics:
  - `TaskState::kNeedInput`, `TaskState::kHasOutput`, `TaskState::kFinished` (reserve `kBlocked` for later).
- Provide a minimal “compiled pipeline” that performs **pass-through** (output batches are identical to input batches).

## Non-goals (Milestone 2)

- No projection/filter/aggregation/join implementations yet (Milestone 3+).
- No DAG builder / multi-input operators yet.
- No TiFlash executor integration beyond a smoke test exercising the API.
- No memory pool / MemoryTracker integration yet.

## Deliverables

1. `Task` supports batch-based IO:
   - `PushInput(std::shared_ptr<arrow::RecordBatch>)`
   - `CloseInput()`
   - `PullOutput()` (returns a batch when available, `nullptr` at end-of-stream)
2. `Task` optionally supports reader-based IO:
   - `SetInputReader(std::shared_ptr<arrow::RecordBatchReader>)` (for non-blocking/in-memory readers)
3. `Pipeline` provides a reader-to-reader convenience wrapper:
   - `MakeReader(std::shared_ptr<arrow::RecordBatchReader>) -> std::shared_ptr<arrow::RecordBatchReader>`
   - the returned reader owns the internal `Task` to avoid lifetime hazards
4. Pass-through pipeline behavior:
   - output schema equals input schema
   - output batches are the same objects as input batches (no copy) for pass-through
5. Tests:
   - push/pull mode: input batches round-trip unchanged
   - reader mode: input reader round-trips unchanged
6. Examples and TiFlash integration smoke test updated to use the new API.

## Step-by-step

### 1) Extend public headers

Update TiForth public headers under `libs/tiforth/include/tiforth/`:

- Add Arrow includes needed for RecordBatch plumbing (`arrow/record_batch.h`, `arrow/record_batch_reader.h`).
- Extend `Task` with batch IO methods and clarify Step semantics.
- Extend `Pipeline` with a `MakeReader(...)` helper.

### 2) Implement pass-through task execution

In `libs/tiforth/src/tiforth/`:

- Implement a minimal, single-operator pipeline inside `Task`:
  - maintain an input queue and output queue of `std::shared_ptr<arrow::RecordBatch>`
  - `Step()` moves at most one batch from input to output per call
  - `Step()` reads one batch from the input reader (if configured) when input is otherwise unavailable
- Validate schema consistency:
  - first observed batch sets the expected schema (or use input reader schema)
  - subsequent batches must match (`Schema::Equals` including metadata)

### 3) Implement `Pipeline::MakeReader`

- Implement a small `arrow::RecordBatchReader` wrapper that:
  - owns the internal `Task`
  - drives `Task::Step()` and `Task::PullOutput()` inside `ReadNext(...)`
  - exposes a stable `schema()` (pass-through: same as input schema)

### 4) Update tests and examples

- Add/adjust gtests under `libs/tiforth/tests/`:
  - push/pull pass-through round-trip
  - reader-to-reader pass-through round-trip
- Update `libs/tiforth/examples/src/{noarrow_main.cc,witharrow_main.cc}` to demonstrate the “hello pipeline”.
- Update `dbms/src/Flash/tests/gtest_tiforth_integration.cpp` to validate the new API shape via a tiny pass-through batch.

## Definition of done (Milestone 2)

- Standalone TiForth:
  - `cmake -S libs/tiforth -B <build_dir> -DTIFORTH_BUILD_TESTS=ON` configures
  - `cmake --build <build_dir>` builds
  - `ctest --test-dir <build_dir>` passes
- TiFlash integration:
  - TiFlash config/build with `ENABLE_TIFORTH=ON` succeeds
  - integration smoke test compiles and passes
