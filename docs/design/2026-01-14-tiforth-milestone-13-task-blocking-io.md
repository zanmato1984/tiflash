# TiForth Milestone 13: Complete Task Blocking Model (IO/Await/Notify) + Pilot Operator

- Author(s): TBD
- Last Updated: 2026-01-19
- Status: Implemented
- Related design: `docs/design/2026-01-14-tiforth.md`
- Related milestones:
  - `docs/design/2026-01-14-tiforth-milestone-3-pipeline-framework.md` (common synchronous path)
  - `docs/design/2026-01-14-tiforth-milestone-11-pipeline-breaker-hash-aggregation.md` (adopt full OperatorStatus)

## Problem

TiForth already exposes a TiFlash-shaped stepping API (`Task::Step()` returning `TaskState`) and includes the extended
status surface (`IOIn/IOOut/Waiting/WaitForNotify`). However, end-to-end wiring is incomplete:

- `Pipeline::MakeReader()` / `TaskRecordBatchReader` rejects blocked states with `NotImplemented`.
- There is no dedicated “pilot” operator to validate the full blocked-state state machine (IO progress, await, notify)
  and error propagation in unit tests.

This leaves the blocking model untested and makes it easy for host integrations to drift or accidentally rely on
undefined behavior.

## Goals

- Make TiForth’s pipeline/task model **fully usable** for blocked operators:
  - `Task::Step()` returns the correct blocking `TaskState`
  - host can drive progress via `Task::{ExecuteIO, Await, Notify}`
  - `Pipeline::MakeReader()` drives `IOIn/IOOut` and `Waiting` internally so it can run operators that block on IO/await
- Add a small **pilot operator** that intentionally triggers `IO*` and error paths for tests.
- Keep TiForth independent and Arrow-native (no TiFlash headers/types).

## Non-goals

- Implement a full reactor/event-loop inside TiForth.
- Define a universal “notify source” for `WaitForNotify` inside `RecordBatchReader` wrappers (host-specific).
- Add real storage/RPC operators; pilot operator is purely for validation.

## Proposed Design

### A) RecordBatchReader wiring

Extend `Pipeline::MakeReader()`’s internal task driver to handle blocked states:

- On `TaskState::kIOIn/kIOOut`: call `Task::ExecuteIO()` and continue driving until the task transitions to a runnable
  state (or produces output / finishes).
- On `TaskState::kWaiting`: call `Task::Await()` and continue.
- On `TaskState::kWaitForNotify`: keep returning `NotImplemented` (host must provide a notify mechanism).

This preserves the host-driven model while making the “reader adapter” usable for common blocked cases.

### B) Pilot operator

Add `PilotAsyncTransformOp` (name TBD) that can be configured to:

- block once on `kIOIn` or `kIOOut` for each input batch, requiring an explicit `ExecuteIO()` call before producing
  output (via `TryOutput()` buffering)
- optionally inject an `arrow::Status` error at a configured phase (e.g. on `Transform()` or `ExecuteIO()`)

This provides deterministic coverage for:

- `TaskState` transitions (`Step → IO → ExecuteIO → Step → output`)
- IO progression semantics (`ExecuteIO` must not return `kHasOutput`)
- error propagation out of `Task` and out of `Pipeline::MakeReader()`

## Implementation Steps

1. Implement `PilotAsyncTransformOp` under `tiforth/operators/` and include it in the umbrella header.
2. Wire `Pipeline::MakeReader()` to drive `IO*` and `Waiting` states.
3. Add gtests:
   - task-driven IO path (explicit `Step/ExecuteIO`)
   - reader-driven IO path (`Pipeline::MakeReader`)
   - error injection propagation
4. Run `cmake --build` + `ctest` in TiForth; bump TiFlash pin only if/when TiFlash integration starts using the new
   behaviors.

## Definition of Done

- TiForth unit tests cover `IOIn/IOOut` and error propagation via the pilot operator.
- `Pipeline::MakeReader()` can execute pipelines that block on IO/await without returning `NotImplemented`.
- Docs updated to state that IO/await are wired (notify remains host-driven).

## Status (2026-01-19)

- Implemented in TiForth commit: `ef8546a`
- Key changes:
  - `Pipeline::MakeReader()` drives `TaskState::{kIOIn,kIOOut}` via `Task::ExecuteIO()` and `TaskState::kWaiting` via
    `Task::Await()`; `kWaitForNotify` remains host-driven and returns `NotImplemented` in the reader adapter.
  - Added `PilotAsyncTransformOp` to exercise IO/notify/error paths and gtests validating state transitions.
