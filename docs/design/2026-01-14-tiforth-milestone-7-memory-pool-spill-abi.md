# TiForth Milestone 7: Memory Pool + Spill Hooks + ABI/Binder Stabilization (Plan)

- Author(s): TBD
- Last Updated: 2026-01-16
- Status: Draft
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

Operators that allocate should accept a pool (or exec context) explicitly:

- `ProjectionTransformOp`, `FilterTransformOp`, `SortTransformOp`:
  - add optional constructor param `arrow::MemoryPool*`
  - initialize their internal `arrow::compute::ExecContext` with that pool
- Expression evaluation helpers:
  - ensure `EvalExpr*` uses the caller-provided `ExecContext` when present

Pipeline builders/tests can remain unchanged by default (use Arrow default pool), while TiFlash integration can pass an accounting pool.

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
- engine create/destroy with optional memory pool callbacks
- task push/pull/step (Arrow stays C++-only; C ABI can move to Arrow C Data Interface later)

MS7 focuses on:

- versioning (`TIFORTH_C_ABI_VERSION`)
- ownership rules + explicit destroy functions
- error reporting conventions (status code + owned message)

### Go/Rust bindings

Plan only:

- Go: cgo wrapper around C ABI, with `io.Reader`/`io.Writer` style task stepping
- Rust: bindgen + safe wrapper types, integrate with Arrow Rust via FFI (future milestone)

## Definition of Done

- MS7 design doc committed.
- Follow-up MS7 implementation tasks created in `.codex/progress/daemon.md`.
