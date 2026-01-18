# TiForth Milestone 11: Pipeline Breaker Hash Aggregation (Build/Convergent Split, TiFlash Parity)

- Author(s): TBD
- Last Updated: 2026-01-18
- Status: Planned
- Related design: `docs/design/2026-01-14-tiforth.md`
- Depends on: MS3 (pipeline framework common path), MS5 (hash agg common path)

## Motivation

In TiFlash pipeline model, the essential abstraction is the **pipeline breaker**: a blocking operator becomes a boundary
that splits a plan into multiple pipelines with dependency ordering.

Hash aggregation (GROUP BY) is a canonical pipeline breaker:

- it must **consume all upstream input** to build a hash table
- only then can it **produce downstream output**

TiFlash addresses this by splitting aggregation into two **non-blocking** operators in **two distinct pipelines**:

- `AggregationBuild`: consumes input and builds the hash table (sink stage)
- `AggregationConvergent`: reads from the built state and produces output (source stage)

TiForth currently implements hash aggregation as a single blocking `TransformOp`. This milestone plans the refactor to
mirror TiFlash’s true pipeline-breaker form.

## TiFlash Reference: How Aggregation Is Split

Plan split point (non-fine-grained-shuffle, non-auto-pass-through):

- `dbms/src/Flash/Planner/Plans/PhysicalAggregation.cpp`:
  - `PhysicalAggregation::buildPipeline(...)` creates:
    - `PhysicalAggregationBuild` and calls `builder.breakPipeline(agg_build)`
    - then adds `PhysicalAggregationConvergent` to the downstream pipeline

Build pipeline (sink stage):

- `dbms/src/Flash/Planner/Plans/PhysicalAggregationBuild.cpp`:
  - `aggregate_context->initBuild(...)`
  - installs `AggregateBuildSinkOp` as the pipeline sink (per concurrency index)
- `dbms/src/Operators/AggregateBuildSinkOp.cpp`:
  - `writeImpl(Block &&)` feeds blocks into `AggregateContext::buildOnBlock(...)`
  - `prepareImpl()` continues draining local buffered data
  - spill hooks exist but are orthogonal to the pipeline break concept

Convergent pipeline (source stage):

- `dbms/src/Flash/Planner/Plans/PhysicalAggregationConvergent.cpp`:
  - `aggregate_context->initConvergent()`
  - installs `AggregateConvergentSourceOp` as the pipeline source (per convergent concurrency index)
- `dbms/src/Operators/AggregateConvergentSourceOp.cpp`:
  - `readImpl(Block &)` returns `agg_context->readForConvergent(index)`
- `dbms/src/Operators/AggregateContext.cpp`:
  - `initBuild(...)`: allocates per-thread aggregated variants (`many_data`) and thread data
  - `initConvergent()`: merges per-thread states and converts to output blocks
  - `readForConvergent(index)`: produces blocks until exhausted

Single-operator (non-breaker) alternatives in TiFlash exist (`LocalAggregateTransform`, `AutoPassThrough...`) but the
pipeline breaker split above is the target shape for TiForth parity.

## Current TiForth State (Common Path Only)

TiForth today:

- pipeline framework supports only a single linear chain per task:
  - `libs/tiforth/include/tiforth/pipeline.h`
  - `libs/tiforth/src/tiforth/task.cc`
  - `libs/tiforth/src/tiforth/pipeline_exec.cc`
- hash aggregation is a single blocking transform:
  - `libs/tiforth/src/tiforth/operators/hash_agg.cc` (`HashAggTransformOp`)
  - consumes until EOS, then emits one output `RecordBatch`

This is sufficient for early parity tests, but does not model the pipeline-breaker split.

## Target TiForth Architecture

### 1) Pipeline breaker support in TiForth pipeline model

Introduce the minimal abstractions needed to express:

- **multiple pipelines** (linear operator chains) in a single compiled plan
- **dependency edges** between pipelines (downstream starts only after upstream finishes)
- a **shared breaker state** object owned by the plan/task, referenced by both sides

Keep the host-facing `Task` API stable (push input / step / pull output). Internally, a task will drive:

1) the **build pipeline** until `Finished`
2) then the **convergent pipeline** until `Finished`

Initial scope can be single-threaded per task (concurrency=1), but keep the API compatible with later multi-concurrency
execution (multiple build sinks feeding one shared context, then multiple convergent sources draining it).

### 2) HashAgg split operators in TiForth

Create TiForth counterparts of TiFlash’s split ops, backed by a shared context:

- `HashAggContext`
  - owns the hash table + aggregate states + key output buffers
  - supports:
    - `InitBuild(...)` / `ConsumeBatch(...)` / `FinishBuild(...)`
    - `InitConvergent(...)`
    - `ReadNextBatch(...)` (chunked output; not necessarily one batch)
- `HashAggBuildSinkOp` (build pipeline sink)
  - `Write(batch)` feeds data into `HashAggContext`
  - `Write(nullptr)` marks build finished (EOS)
- `HashAggConvergentSourceOp` (convergent pipeline source)
  - `Read(out)` returns next output batch from `HashAggContext` until exhausted, then EOS

Semantics to preserve (carry over from MS5 common path):

- global aggregation emits one row even when input is empty-after-filter (matching TiDB/TiFlash behavior)
- deterministic group ordering (stable group id by first appearance)
- output column ordering: aggregate outputs first, then group keys
- key normalization rules (float canonicalization, collated string sort-key normalization, decimals raw bytes)

## Work Items / Tasks (for a dedicated agent)

### A) Pipeline framework: multi-pipeline + breaker dependency

1. Add a plan representation that can hold multiple linear pipelines and their dependencies
   - keep the existing `Pipeline` API for “single-chain” plans
   - add a new type (example naming):
     - `PipelineDAG` / `PipelineGraph` / `Plan` (compiled), with `CreateTask()` producing a task that drives all stages
2. Extend the internal execution engine to run pipeline stages in dependency order
   - minimal: two stages (build then convergent), single concurrency
   - later: N build drivers + M convergent drivers (mirror TiFlash `PipelineExecGroup`/concurrency)
3. Add a generic “breaker state” ownership model
   - state must outlive both stages
   - state must be attached to the task instance (not global/static)
4. Add unit tests in TiForth for stage transitions and EOS semantics
   - build stage consumes all input and produces no output
   - convergent stage produces output after build completes

### B) HashAgg refactor: context + build sink + convergent source

1. Extract MS5 `HashAggTransformOp` internal state into a reusable `HashAggContext`
   - keep MS5 operator working initially by delegating to the context (reduces risk)
2. Implement `HashAggBuildSinkOp` and `HashAggConvergentSourceOp`
   - ensure `HashAggConvergentSourceOp` can emit multiple output batches (configurable max rows)
3. Wire the split operators into the new multi-pipeline plan
4. Add TiForth gtests covering:
   - group-by and global agg in breaker form
   - multi-batch input (build stage) + chunked multi-batch output (convergent stage)

### C) TiFlash integration: translate pipeline-breaker agg into TiForth plan

1. Update TiFlash DAG→TiForth translation to produce a TiForth multi-pipeline plan for aggregation
   - build pipeline: before-agg expressions + build sink
   - convergent pipeline: convergent source + after-agg expressions
2. Keep TiForth independent (no TiFlash types in `tiforth`); translation lives in TiFlash

### D) Parity tests: exercise “true breaker” path

1. Extend `dbms/src/Flash/tests/gtest_tiforth_filter_agg_parity.cpp` to run the new breaker plan
2. Cover both:
   - GROUP BY with collated string keys
   - global aggregation (empty and non-empty)
3. Keep existing MS5 common-path tests until breaker path is stable; then consider switching default path to breaker.

### E) Validation checklist

- TiForth: `cmake --build /Users/zanmato/dev/tiforth/build-debug && ctest --test-dir /Users/zanmato/dev/tiforth/build-debug`
- TiFlash: `ninja -C cmake-build-tiflash-tiforth-debug gtests_dbms && cmake-build-tiflash-tiforth-debug/dbms/gtests_dbms --gtest_filter='TiForthFilterAggParityTestRunner.*'`

