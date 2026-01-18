# TiForth Milestone 7: Memory Pool + Spill Hooks + ABI/Binder Stabilization (Plan)

- Author(s): TBD
- Last Updated: 2026-01-17
- Status: Implemented
- Related design: `docs/design/2026-01-14-tiforth.md`
- Depends on: MS1-6

## Scope

This milestone is about “host integration hardening”:

- memory pool plumbing so all TiForth allocations are host-accounted
- spill/backpressure hook points (skeleton first, no full spill engine required)
- define and stabilize an initial C ABI + binding strategy (Go/Rust)

## Goals

- `Engine` owns (or references) the `arrow::MemoryPool` used by TiForth.
- All Arrow allocations performed by TiForth operators go through the Engine pool:
  - Arrow compute `ExecContext`
  - builders (`Int32Builder`, etc)
  - concatenation/sort/filter intermediate buffers
- Provide an explicit extension point for spill:
  - operator can request spill
  - host can accept/deny and provide a location/handle
  - initial implementation can be “no spill” (always deny) but the plumbing exists
- Define a minimal stable C ABI surface:
  - create/destroy engine with host callbacks
  - build a pipeline (initially: append a small set of operators)
  - run a task via step/push/pull

## Non-goals

- Full spill implementation and external-memory operators.
- Full “plan IR” coverage for all operators.
- Production-grade bindings (MS7 defines the ABI and a plan; implementation can be incremental).

## Memory Pool Design

### API changes

- Extend `EngineOptions` to accept a pool:
  - `arrow::MemoryPool* memory_pool = arrow::default_memory_pool();`
- Add `Engine::memory_pool()` accessor.

### Propagation strategy

Operators that allocate default to the Engine pool, with an explicit escape hatch:

- `ProjectionTransformOp`, `FilterTransformOp`, `HashAggTransformOp`, `HashJoinTransformOp`, `SortTransformOp`:
  - accept `const Engine*` plus optional `arrow::MemoryPool*`
  - when the pool is null, use `engine->memory_pool()` for Arrow builders and the internal `arrow::compute::ExecContext`
- Expression evaluation/compilation:
  - support executing with a caller-provided `ExecContext`
  - `EvalExpr*` / `CompiledExpr` create a local `ExecContext` from the Engine when one is not provided

Tests validate non-default pool usage via `arrow::ProxyMemoryPool`.

## Spill Hooks (Skeleton)

Introduce (design-only in MS7) a host-provided interface:

- `struct SpillOptions { ... }`
- `class SpillManager` (or callback vtable):
  - `RequestSpill(bytes_hint) -> Status/Handle`
  - `WriteSpill(handle, RecordBatch/Table) -> Status`
  - `ReadSpill(handle) -> RecordBatchReader/Status`

Initial MS7 implementation may provide:

- `tiforth::SpillManager` C++ interface + default `DenySpillManager` (`libs/tiforth/include/tiforth/spill.h`)
- plumbed via `EngineOptions.spill_manager` (default installed when absent)
- no operator actually spills yet (just checks the hook for future milestones)

## ABI / Bindings Plan

### C ABI surface

Add `libs/tiforth/include/tiforth_c/tiforth.h` (new namespace) with:

- opaque handles: `tiforth_engine_t`, `tiforth_pipeline_t`, `tiforth_task_t`
- engine/pipeline/task lifecycle + minimal pipeline builder
- task step/push/pull using Arrow C Data Interface (`ArrowSchema` + `ArrowArray`) and Arrow C Stream Interface (`ArrowArrayStream`)

MS7 focuses on:

- versioning (`TIFORTH_C_ABI_VERSION`)
- ownership rules + explicit destroy functions
- error reporting conventions (status code + owned message)

MS7C implementation status:

- Header `libs/tiforth/include/tiforth_c/tiforth.h` defines C ABI v1:
  - opaque handles + lifecycle APIs
  - expression builder subset
  - Arrow C Data/Stream push/pull helpers
  - ownership rules for Arrow C structs (moved-in vs exported)

MS7D implementation status:

- `tiforth_capi` library implements the header surface:
  - engine/pipeline/task + filter/projection operators (common path)
  - Arrow C Data Interface import/export and ArrowArrayStream input/output
  - negative/misuse tests cover error mapping + ownership rules

### Go/Rust bindings

Plan only:

- Go: cgo wrapper around C ABI, with `io.Reader`/`io.Writer` style task stepping
- Rust: bindgen + safe wrapper types, integrate with Arrow Rust via FFI (future milestone)

## Definition of Done

- MS7 design doc updated to match implementation.
- TiForth builds/tests pass (`ninja -C libs/tiforth/build-debug && ctest`).
