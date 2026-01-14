# tiforth: C++ Compute Engine For TiFlash (Draft)

- Author(s): TBD
- Last Updated: 2026-01-14
- Status: Draft / working notes
- Discussion PR: TBD
- Tracking Issue: TBD

## Summary

Build a **dedicated C++ library** named **tiforth** (source located in the TiFlash repo but **independent** from TiFlash DBMS/compute code) that re-implements TiFlash compute:

- Rewrite every compute **operator** and **function** in `tiforth`, porting semantics from the existing TiFlash C++ implementation, but organizing the library cleanly (no one-to-one source mapping requirement).
- Unify compute across hosts, which implies a unified batch-oriented **in-memory representation** for columnar data; `tiforth` adopts **Apache Arrow (C++)** as the physical layout for that representation.
- Define a unified **database type system** inside `tiforth` and map those types to canonical Arrow types (including Arrow extension metadata where needed), so the memory representation is consistent across TiFlash/TiDB/TiKV integrations.
- Expose a stable, minimal public interface for:
  - pipeline orchestration (build/compile a pipeline),
  - executing a pipeline task (host-driven execution),
  - memory pool integration,
  - error/status reporting.

Integrate it into TiFlash by:

- converting storage-read data into Arrow batches,
- letting TiFlash compose pipeline tasks via the `tiforth` API,
- executing `tiforth` pipeline tasks using TiFlash’s existing threading model/pool.

## Goals

- **Isolation**: `tiforth` is self-contained and does not depend on `dbms/` headers, `DB::Block`, `DB::IColumn`, or other TiFlash C++ implementation details.
- **Unified memory representation across the boundary**: since compute is centralized in `tiforth`, data exchange uses a single batch format (Arrow) to avoid re-implementing operators/functions per host memory layout; prefer Arrow C Data / C Stream for zero-copy interop where possible.
- **Host-managed threading**: TiFlash controls scheduling/execution; `tiforth` must not require owning a thread pool.
- **Incremental migration**: allow partial operator/function coverage with a fallback to the existing C++ engine while `tiforth` grows.
- **Unified memory accounting**: allocations in `tiforth` are tracked/limited using a host-provided memory pool (integrated with TiFlash `MemoryTracker` and spill/limits policy).
- **Unified type system**: all operator/function signatures are defined in `tiforth` types and deterministically mapped to Arrow types.
- **Long-term reuse across the stack**: evolve `tiforth` into a shared compute library integrated into TiFlash, TiKV, and TiDB (e.g. TiDB via cgo calling a stable C ABI; TiKV via Rust FFI calling the same C ABI), progressively reducing host-side conversions by aligning batch memory layouts.
- **Language-friendly bindings**: provide official Go/Rust bindings that expose idiomatic APIs and hide the raw C ABI from application code (the C ABI remains the stable “wire” layer, but is not the primary developer interface).

## Non-goals (Initial)

- Rewriting TiFlash storage engine, raft/proxy, or networking as part of `tiforth`.
- Replacing TiDB DAG planning / MPP scheduling logic immediately.
- “Big bang” replacement of the entire execution engine without a fallback path.

## High-Level Architecture

```
     (C++) TiFlash Host
  ┌───────────────────────────┐
  │ Storage Readers (DM, ...)  │
  │   └─ materialize as Arrow  │
  │        (RecordBatch)       │
  │                            │
  │ Pipeline build (host)      │
  │   └─ call `tiforth` orchestration API
  │                            │
  │ ThreadPool / Scheduler     │
  │   └─ run `tiforth` tasks on host threads
  └─────────────┬─────────────┘
                │ Arrow C Data / Stream
                ▼
      (C++) tiforth
  ┌───────────────────────────┐
  │ Pipeline Orchestrator      │
  │ Operators + Functions      │
  │ Type system + Arrow mapping│
  │ Arrow execution runtime    │
  │ Memory pool adapter        │
  │ Status/error API           │
  └───────────────────────────┘
```

### Boundary Choice: Arrow C Data / C Stream

Use the Arrow C interfaces as the interop boundary:

- Inputs: `ArrowArray` + `ArrowSchema` (arrays/struct arrays) or `ArrowArrayStream` (record batch streams).
- Outputs: same.

This allows both sides to use their native Arrow implementations:

- C++ host may use `libarrow` (C++) or custom buffers exported via the C interface.
- `tiforth` uses `libarrow` (C++) and imports/exports via the same C interface.

Long-term, TiDB and TiKV can move their internal compute batch formats toward `tiforth`’s Arrow mapping to reduce conversion overhead at the boundary.

## Repository Layout / Build Integration

### Location

Create a new C++ library under the TiFlash repo, e.g.:

- `libs/tiforth/`
  - Project root is the C++ implementation (no extra `cpp/` subdir); keep C++ sources under `src/` and headers under `include/`.
  - `CMakeLists.txt`
  - `include/` (public headers; include a stable C header for the C ABI)
  - `src/` (implementation)
  - `tests/` (optional; unit tests + differential tests harness)
  - `bindings/` (language-friendly APIs that hide the raw C ABI)
    - `go/` (Go module + cgo wrapper)
    - `rust/` (Rust crate wrapping the C ABI)

### Independence Contract

The library must not:

- link against `dbms` or include any `dbms/src` headers,
- expose C++ ABI types across language boundaries (use a C ABI),
- assume TiFlash internal column layouts (except through Arrow C interfaces).

### Build

Integrate with TiFlash CMake build:

- Build `tiforth` as a standalone CMake target (static or shared, depending on distribution needs).
- Link against `libarrow` using the existing `ENABLE_ARROW` + `${ARROW_LIBRARY}` mechanism.
- Provide a small `tiforth_capi` target exporting the stable C ABI (used by TiFlash, TiDB(cgo), and TiKV(Rust FFI)).

### Coding Style (Required)

`tiforth` is intentionally **not** a “normal TiFlash module”. To reduce friction when using Arrow APIs and to keep upstream Arrow integration idiomatic, `tiforth` code must follow **Apache Arrow C++** conventions rather than TiFlash/ClickHouse style.

Minimum requirements:

- **Style guide**: follow Apache Arrow C++ coding style and conventions (naming, formatting, include order, and error handling idioms).
- **Error handling**: prefer `arrow::Status` / `arrow::Result<T>` and Arrow helper macros (e.g. `ARROW_RETURN_NOT_OK`, `ARROW_ASSIGN_OR_RAISE`) internally; do not use TiFlash `DB::Exception` as the primary internal control flow.
- **Formatting**: keep a dedicated clang-format configuration for `libs/tiforth/` aligned with Arrow’s `.clang-format` (do not apply TiFlash formatting rules to `tiforth`).

## Type System & Arrow Mapping

`tiforth` defines the compute-layer database type system and treats Arrow as the physical in-memory format.

Choosing Arrow is a **consequence** of centralizing compute: once operators/functions are unified across hosts, a single shared, language-neutral batch memory representation is required to avoid N-way re-implementations and conversions.

### Type Ownership

- `tiforth` defines canonical types (examples): `DataType`, `Field`, `Schema`, `Nullability`, collation/timezone/precision metadata, and any TiDB/TiFlash-specific logical types (e.g. vector float32).
- `tiforth` is the **source of truth** for how these logical types map to Arrow schemas/arrays.
- TiFlash (host) must convert storage output into Arrow that conforms to `tiforth`’s mapping, or provide sufficient Arrow extension metadata so `tiforth` can interpret it correctly.

### Arrow Mapping Rules (Conceptual)

- Prefer Arrow standard types when semantics match (integers/floats, `Timestamp`, `Decimal128/Decimal256`, `Utf8/LargeUtf8`, `Binary/LargeBinary`, `List/FixedSizeList`, …).
- For semantics not representable by Arrow alone (e.g. MyDate/MyDateTime fsp, collation, vector dimension constraints, unsigned-vs-signed where needed, etc.), use **Arrow extension type metadata** carried on fields (`ARROW:extension:*`) so the mapping round-trips across the Arrow C Data/Stream boundary.
- Treat this mapping as part of `tiforth`’s public contract: any Arrow data exchanged with TiFlash must follow the mapping (or it is rejected as invalid/unsupported).

This yields a single place to reason about:

- supported types,
- operator/function signatures,
- on-wire / interop schema expectations,
- correctness edge cases.

## Public API Surface (FFI)

`tiforth` exports a stable C ABI (for TiFlash, TiDB via cgo, and TiKV via Rust FFI). TiFlash can also add a thin C++ wrapper for convenience.

### Concepts

- **Engine**: global state/config + function registry + memory pool handle.
- **Pipeline**: a compiled DAG of operators.
- **Task**: an executable unit of a pipeline (e.g., one driver/fragment of the pipeline).
- **RecordBatch stream**: input/output via Arrow C Stream interface.

### Status / Error

Expose a stable “status” type (independent of C++ exceptions):

- error code enum (engine error, invalid argument, not supported, internal, OOM, …)
- message (owned by `tiforth` with explicit free API, or returned via callback)

Goal: map to TiFlash error handling without leaking C++ exceptions across FFI.

### Memory Pool

`tiforth` should route Arrow allocations through an `arrow::MemoryPool` implementation. To keep hosts language-agnostic, expose a C ABI to create a memory pool wrapper backed by host callbacks, and pass that pool into `tiforth` engine/task creation.

Host-defined memory pool vtable (conceptual):

- allocate / deallocate / reallocate callbacks
- optional accounting callbacks (on_alloc/on_free) if allocator is not the accounting source
- per-query pool instances (so TiFlash can enforce query memory limits and trigger spill)

The engine must ensure:

- all Arrow buffers and operator state allocations go through the provided pool (or are fully accounted for).

### Pipeline Orchestration API

Minimal building blocks:

- create/destroy engine
- create/destroy pipeline builder
- add operator nodes with opaque operator configuration blobs (or structured configs)
- connect operator edges (data dependencies)
- finalize/compile pipeline
- instantiate N tasks from compiled pipeline (for parallelism)

### Task Execution API (Host-Driven)

TiFlash runs tasks on its own threads, so task execution must be explicit and re-entrant:

- task accepts input via Arrow stream or “push” batches
- task produces output via Arrow stream or “pull” batches
- task exposes a step/run method that:
  - consumes available input,
  - produces output,
  - returns a state (`NeedInput`, `HasOutput`, `Finished`, `Blocked`, `Error`)

This avoids `tiforth` managing threads and allows TiFlash’s scheduler to control fairness and resource limits.

### Why The Abstraction Is Pipeline-Level

`tiforth` deliberately exposes orchestration at the **pipeline task** level (instead of row-by-row or “whole query”) to balance performance and integration ergonomics:

1. **Right compute granularity**: a pipeline task is neither too big (so a single unit monopolizes a host thread for too long) nor too small (so the FFI/cgo call overhead dominates). It is naturally batch-oriented and can be stepped frequently.
2. **Pure compute (no blocking / no IO)**: a task should not perform blocking operations or IO. The host (TiFlash/TiDB/TiKV) owns IO and backpressure; the compute unit stays deterministic and schedulable.
3. **Threading-agnostic by design**: `tiforth` does not own threads. The host decides concurrency and parallelism (how many tasks, where they run, how to interleave them) and can integrate with its existing thread pools and resource control.
4. **Unify operator/function implementations**: historically, similar operators/functions have been implemented across multiple languages and codebases (TiFlash C++, TiKV Rust, TiDB Go). Centralizing compute in `tiforth` reduces duplicated logic, improves consistency, and significantly lowers long-term maintenance cost.
   - As a consequence, we also need a single shared memory representation for batched columnar data; `tiforth` defines the logical types and uses Arrow as the canonical physical layout so the same operators/functions can run across hosts.

## Operator / Function Rewrite Strategy

### Library-First Organization

Do not mirror TiFlash DBMS file structure. Organize `tiforth` as a clean library with explicit layers:

- Separate stable interfaces (C ABI, type system, plan representation) from execution internals.
- Keep Arrow concerns explicit (schemas, arrays, kernels, `arrow::MemoryPool`) rather than spread across unrelated modules.
- Keep operator/function registration explicit and testable (registry + factories).

### Semantics Compatibility

The engine must match TiFlash behavior for:

- type system (incl. decimals, date/time, enum, nullable),
- collation-aware string comparisons (if applicable),
- overflow/rounding behavior for numeric/decimal,
- NULL semantics and corner cases.

### Correctness Testing Plan

Use differential testing as the primary safety net:

- generate Arrow input batches,
- run the same logical pipeline on the existing C++ engine and `tiforth`,
- compare output Arrow batches for equality (or defined equivalence).

Add targeted golden tests for tricky cases:

- NULL propagation,
- decimal scale/precision,
- timezone/fsp,
- collation,
- vector float32 operations (distance metrics).

## TiFlash Integration Plan

### 1) Storage → Arrow Materialization

Add a C++ adapter to produce Arrow `RecordBatch` (or an Arrow C stream) directly from storage reads.

Possible implementation approaches:

- Use `libarrow` in C++ to build arrays/batches and export via Arrow C Data/Stream interfaces.
- Or reuse existing TiFlash internal buffers and wrap/export them as Arrow C data (only if layout is fully compatible).

### 2) Pipeline Composition (Host Side)

Keep TiFlash query planning and existing DAG interpretation in C++ initially.

Instead of constructing C++ pipeline operators, TiFlash:

- calls the `tiforth` pipeline builder API,
- passes operator configurations (expression trees, schema, join keys, aggregation params, etc.),
- compiles a `tiforth` pipeline and receives task handles.

### 3) Task Scheduling & Execution (Host Side)

TiFlash uses its existing thread pool:

- schedule task handles onto worker threads,
- call `task_run/step` repeatedly,
- exchange input/output batches using Arrow streams.

`tiforth` must be safe under host-controlled concurrency rules:

- tasks are independent units,
- shared state is read-only or internally synchronized,
- no implicit global mutability without synchronization.

### 4) Output Integration

Downstream sinks remain in C++ initially (e.g. TiDB chunk encoding / MPP exchange):

- consume Arrow output batches from `tiforth`,
- encode to existing protocols, or
- convert back into existing `Block/IColumn` if needed as a transitional step.

### 5) Feature Flag + Fallback

Add a build flag and a runtime switch:

- compile-time: `ENABLE_TIFORTH` (name TBD)
- runtime: enable per query / per operator, with fallback to C++ for unsupported operators/functions.

This allows incremental rollout and easy A/B testing.

## Long-Term: TiDB/TiKV Integration & Memory Layout Migration

The long-term goal is for TiFlash, TiKV, and TiDB to share the same operator/function implementations via `tiforth`. Achieving this requires converging on a unified batch memory representation (as defined by `tiforth`’s type mapping).

### TiDB (Go, via cgo)

- Define a stable Go wrapper over the `tiforth` C ABI (lifecycle, error handling, and Arrow stream plumbing).
- Implement Arrow C Data / C Stream import/export in TiDB’s execution layer to convert between TiDB’s native row/column batches and `tiforth`’s Arrow mapping.
- Evaluate TiDB’s existing in-memory batch/column layout (e.g. `chunk`) and implement a zero-copy export path to Arrow C Data where possible, aligned with `tiforth`’s schema/type contract.
- Introduce an Arrow-native “compute batch” representation in TiDB as an intermediate step, so most internal execution can stay Arrow without repeated conversion (switch memory layout for compute paths first, not necessarily storage/network).
- Establish build/distribution and CI support for shipping `tiforth` artifacts to TiDB (platform matrix, reproducible builds, symbol visibility, ABI compatibility).

### TiKV (Rust)

- Call `tiforth` via Rust FFI against the stable C ABI.
- Identify TiKV coprocessor/executor boundaries where batch formats are produced/consumed; add adapters to/from `tiforth`’s Arrow mapping as a transitional step.
- Gradually switch TiKV compute-oriented batch layouts to Arrow-native representations to avoid repeated conversions, aligned with `tiforth`’s type mapping and extension metadata.

### Cross-Component Tasks For “Switching The Memory Layout”

- Specify a “compute batch” contract (schemas, types, nullability, extension metadata) owned by `tiforth`, versioned and validated at boundaries.
- Start with boundary conversions (host-native → Arrow → `tiforth`) and evolve toward native Arrow batches in each host’s compute layer.
- Ensure zero-copy where feasible (buffers, string/varlen, dictionary/encoding decisions), and document when copies are unavoidable.
- If needed for end-to-end performance, add an optional Arrow-based interchange between components (e.g. TiDB↔TiKV result materialization) that preserves the `tiforth` compute batch contract across process/language boundaries.
- Build differential and compatibility test suites that run the same pipelines across TiFlash/TiKV/TiDB hosts and validate result equivalence.

## Risks / Open Challenges

- **Memory pool integration across languages**: for TiDB(cgo) and TiKV(Rust FFI), the host-provided allocator callbacks must be safe, fast, and compatible with Arrow’s allocation patterns (including cross-thread frees).
- **FFI overhead**: batch sizes and API shape must avoid per-row or per-value crossings.
- **Semantic drift**: parity with C++ behavior is non-trivial (especially collation/decimal/time).
- **Debuggability**: cross-language stack traces and error propagation must be first-class.
- **Incremental migration complexity**: mixing two engines requires clear ownership of schemas, function registries, and execution rules.

## Milestones (Suggested)

1. Library skeleton + CMake integration + minimal FFI (`Engine`, `Status`, no-op pipeline).
2. Arrow C stream import/export + pass-through operator (end-to-end “hello pipeline”).
3. Port `Projection` + a small set of scalar functions; integrate into one DAG path.
4. Port `Filter` + expression evaluation; expand test coverage with differential tests.
5. Port `Aggregation` (hash agg) + key functions/types.
6. Port `Join` (hash join) and `Sort`; expand coverage until most queries can run fully in `tiforth`.
7. Optimize memory pool + spill hooks; stabilize ABI.
8. Integrate into TiDB/TiKV and progressively migrate their compute batch memory layouts toward `tiforth`’s Arrow contract (minimize conversions, maximize zero-copy).

## References (TiFlash Semantics / Integration)

- Operators (semantics reference): `dbms/src/Operators/`
- Functions (semantics reference): `dbms/src/Functions/`
- Aggregation (semantics reference): `dbms/src/AggregateFunctions/`
- Pipeline model (current): `dbms/src/Flash/Executor/` and `dbms/src/Flash/Pipeline/` (if present)
- Storage read path: `dbms/src/Storages/DeltaMerge/`
- Existing coprocessor “Arrow chunk” codec (layout inspiration, not `libarrow`): `dbms/src/Flash/Coprocessor/`
