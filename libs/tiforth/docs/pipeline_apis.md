# Pipeline APIs (C++ and C ABI)

TiForth exposes a small set of stable “host APIs” for building and executing pipelines:

- C++ API (`tiforth::Engine`, `tiforth::Pipeline`, `tiforth::Task`)
- C ABI (`tiforth_engine_t`, `tiforth_pipeline_t`, `tiforth_task_t`)

This doc focuses on the execution model and API contracts. For operator details, see
`operators_and_functions.md`.

## C++ API

Include:

- umbrella header: `#include <tiforth/tiforth.h>`
- or individual headers under `libs/tiforth/include/tiforth/`

### Engine

`tiforth::Engine` is created from `tiforth::EngineOptions`:

- `memory_pool`: required (defaults to `arrow::default_memory_pool()` if you pass none)
- `spill_manager`: optional (defaults to `tiforth::DenySpillManager`)
- `function_registry`: optional (defaults to a fresh registry overlay cloned from Arrow’s global registry)

On creation, the engine registers TiForth functions into its registry.

### Building a pipeline

Use `tiforth::PipelineBuilder` to append transform factories, then finalize into an immutable `Pipeline`.

Example (filter + projection):

```cpp
auto engine = tiforth::Engine::Create({}).ValueOrDie();

auto builder = tiforth::PipelineBuilder::Create(engine.get()).ValueOrDie();
builder->AppendTransform([&] {
  return arrow::Result<tiforth::TransformOpPtr>(
      std::make_unique<tiforth::FilterTransformOp>(
          engine.get(),
          tiforth::MakeCall("less", {tiforth::MakeFieldRef("a"),
                                     tiforth::MakeLiteral(std::make_shared<arrow::Int32Scalar>(10))})));
});
builder->AppendTransform([&] {
  std::vector<tiforth::ProjectionExpr> exprs;
  exprs.push_back({"b", tiforth::MakeFieldRef("b")});
  return arrow::Result<tiforth::TransformOpPtr>(
      std::make_unique<tiforth::ProjectionTransformOp>(engine.get(), std::move(exprs)));
});

auto pipeline = builder->Finalize().ValueOrDie();
```

Notes:

- A transform factory is invoked once per task instance (`Pipeline::CreateTask()`), so transforms can keep
  task-local state.
- Operators are expected to handle end-of-stream (`batch == nullptr`).

### Running a task (push/pull)

Create a task and drive it using `Task::Step()`:

- Provide inputs via `Task::PushInput(batch)` and then `Task::CloseInput()`.
- Consume outputs via `Task::PullOutput()` when `Step()` reports `kHasOutput`.

Important contracts:

- Input schema must be stable across batches. `Task` validates schema equality (including metadata).
- End-of-stream is internal: do not push `nullptr` batches. Use `CloseInput()`.
- `PullOutput()` returns:
  - a batch when available
  - `nullptr` after the task has finished and all output has been drained

### Running as an `arrow::RecordBatchReader`

If you already have an input `arrow::RecordBatchReader`, you can get an output reader:

- `Pipeline::MakeReader(input_reader)`

This wraps a task internally and drives it until completion.

## C ABI (tiforth_capi)

C header: `libs/tiforth/include/tiforth_c/tiforth.h`.

The C ABI mirrors the C++ concepts:

- engine: `tiforth_engine_t`
- pipeline: `tiforth_pipeline_t`
- task: `tiforth_task_t`

### Expressions (C)

The C ABI provides a minimal expression builder:

- `tiforth_expr_field_ref_index`
- `tiforth_expr_literal_int32`
- `tiforth_expr_call`

Pipelines/operators keep references to expressions; you can destroy expression handles after appending operators.

### Data interchange (Arrow C interfaces)

Record batches are exchanged through Arrow’s standard C interfaces:

- Arrow C Data interface: `ArrowSchema` + `ArrowArray` (batch is a struct array)
- Arrow C Stream interface: `ArrowArrayStream`

Ownership is explicit in the header comments:

- “push” APIs consume (`move`) the provided schema/array/stream
- “pull/export” APIs produce owned outputs that the caller must release

### Minimal pipeline coverage (C)

The C pipeline builder currently exposes:

- filter: `tiforth_pipeline_append_filter`
- projection: `tiforth_pipeline_append_projection`

Other operators are available through the C++ API.

