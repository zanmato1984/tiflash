# Operator and function fundamentals

This doc describes TiForth’s operator model, built-in operators, and how expressions/functions interact with
Arrow compute.

## 1) Operator model

Public interfaces: `libs/tiforth/include/tiforth/operators.h`.

TiForth defines three operator kinds:

- `SourceOp`: produces batches (`Read`)
- `TransformOp`: transforms batches (`Transform`) and may buffer output (`TryOutput`)
- `SinkOp`: consumes batches (`Write`) and may need pre-work before accepting input (`Prepare`)

### Status protocol

Operators communicate with `tiforth::OperatorStatus`:

- `kNeedInput`: operator cannot proceed without new input.
- `kHasOutput`: operator produced a batch (or forwarded end-of-stream).
- `kFinished`: operator is complete.

### End-of-stream (EOS)

EOS is represented as a `nullptr` `std::shared_ptr<arrow::RecordBatch>` inside the pipeline.

Rules:

- `SourceOp::Read` returns `kFinished` to signal EOS.
- The executor converts that into “an output batch” with `batch == nullptr` so transforms/sinks can flush.
- Transform operators must accept `*batch == nullptr` and should generally return `kHasOutput` to forward EOS.
- `SinkOp::Write(nullptr)` marks completion for the task sink.

## 2) Built-in transform operators

Public headers live under `libs/tiforth/include/tiforth/operators/`.

### Pass-through

- `tiforth::PassThroughTransformOp` (`pass_through.h`)
- Behavior: forwards input batch or EOS as-is.

### Filter

- `tiforth::FilterTransformOp` (`filter.h`)
- Input: any schema
- Output: same schema (rows filtered)
- Predicate: `tiforth::Expr` evaluated via Arrow compute; must produce a boolean array of length `num_rows`.
- Null handling: uses Arrow `FilterOptions::DROP` (rows with null predicate are dropped).
- Compilation: predicate is compiled once on the first batch schema and reused.

### Projection

- `tiforth::ProjectionTransformOp` (`projection.h`)
- Input: any schema
- Output: new schema with one field per `ProjectionExpr {name, expr}`
- Compilation: expressions compiled once on first batch schema and reused.
- Metadata preservation:
  - direct field refs preserve the input field (including metadata) and only rename it
  - computed outputs are new fields; TiForth currently auto-attaches logical metadata only for:
    - Arrow decimal outputs (marks them `tiforth.logical_type=decimal`)
    - `toMyDate(...)` outputs (marks them `tiforth.logical_type=mydate`)

### Hash aggregation (group-by)

- `tiforth::HashAggTransformOp` (`hash_agg.h`)
- Current limitations (common path):
  - supports 1 or 2 group keys
  - supported key physical types:
    - `int32`
    - `uint64`
    - `decimal128` / `decimal256` (compared by raw fixed-size bytes)
    - `binary` (with `tiforth.logical_type=string` and supported `collation_id` for normalization)
  - supported aggregate funcs:
    - `count_all`
    - `sum_int32` (argument must evaluate to `int32`)
- Collated string keys:
  - original key value is preserved for output
  - normalized key (sort-key bytes) is used for hashing/equality to match collation semantics
- Memory:
  - group key storage uses `std::pmr::string` backed by an Arrow `MemoryPool` adapter

### Hash join (inner join)

- `tiforth::HashJoinTransformOp` (`hash_join.h`)
- Build side:
  - provided as a vector of `RecordBatch` at operator construction time
  - concatenated and indexed on first probe batch
- Join keys:
  - 1 or 2 keys
  - supported key physical types match hash-agg’s supported set
  - for string keys, build/probe must agree on `collation_id`
- Output schema:
  - concatenation of probe schema fields followed by build schema fields
- Join type:
  - common path implements an inner join (unmatched probe rows produce no output)

### Sort

- `tiforth::SortTransformOp` (`sort.h`)
- Current limitations (common path):
  - exactly 1 sort key
  - `ascending=true`, `nulls_first=false` only (ASC with NULLS LAST)
- For string keys:
  - if key field has `tiforth.logical_type=string`, TiForth sorts using collation-aware rules
  - current implementation requires the key array type to be `binary`
- Output:
  - buffers all input, emits one sorted output batch, then forwards EOS

## 3) Expressions

Public API: `libs/tiforth/include/tiforth/expr.h`.

TiForth has a minimal expression IR:

- `FieldRef {name|index}`: reference an input column
- `Literal {Scalar}`: scalar constant
- `Call {function_name, args...}`: call into Arrow compute

You can:

- evaluate directly per batch: `EvalExpr`, `EvalExprAsArray`
- compile/bind once per schema and execute repeatedly:
  - `CompileExpr` -> `tiforth::CompiledExpr`
  - `ExecuteExpr`, `ExecuteExprAsArray`

## 4) Function fundamentals (Arrow compute integration)

### Engine-local function registry

`tiforth::Engine` creates (or accepts) a `arrow::compute::FunctionRegistry` and registers TiForth functions into
it. Operators that evaluate expressions create an `arrow::compute::ExecContext` using:

- the engine’s memory pool
- the engine’s function registry

### Logical-type-driven rewrites (during compilation)

During expression compilation, TiForth inspects argument logical types (from Arrow field metadata and some
physical types) and may rewrite calls:

- Collated string comparisons:
  - calls like `equal/less/...` become `tiforth.collated_equal/tiforth.collated_less/...`
  - a `collation_id` option is derived from `tiforth.string.collation_id`
- Decimal add:
  - `add(x,y)` becomes `tiforth.decimal_add(x,y)` when at least one argument is decimal
- Packed MyTime extraction:
  - when an argument is `tiforth.logical_type=mydate|mydatetime`, TiForth attaches MyTime options
  - for `hour/minute/second` it routes to `tiforth.mytime_hour/_minute/_second` to avoid colliding with Arrow’s
    timestamp extractors

### TiForth-defined function names (common path)

Registered by `libs/tiforth/src/tiforth/functions/register.cc`:

- Collated compare:
  - `tiforth.collated_equal`
  - `tiforth.collated_not_equal`
  - `tiforth.collated_less`
  - `tiforth.collated_less_equal`
  - `tiforth.collated_greater`
  - `tiforth.collated_greater_equal`
- Decimal arithmetic:
  - `tiforth.decimal_add`
- Packed MyTime (selected):
  - `toYear`, `toMonth`, `toDayOfMonth`, `toMyDate`
  - `toDayOfWeek`, `toWeek`, `toYearWeek`
  - `tidbDayOfWeek`, `tidbWeekOfYear`, `yearWeek`
  - `tiforth.mytime_hour`, `tiforth.mytime_minute`, `tiforth.mytime_second`
  - `microSecond`

For the mapping/metadata that drives these rewrites, see `type_mapping_tidb_to_arrow.md`.

