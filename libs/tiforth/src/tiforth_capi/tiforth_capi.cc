#include "tiforth_c/tiforth.h"

#include <memory>

#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/scalar.h>
#include <arrow/status.h>

#include "tiforth/engine.h"
#include "tiforth/expr.h"
#include "tiforth/operators/filter.h"
#include "tiforth/operators/projection.h"
#include "tiforth/pipeline.h"
#include "tiforth/task.h"

#include <cstdlib>
#include <cstring>
#include <string>
#include <utility>
#include <vector>

namespace {

tiforth_status_t MakeStatus(tiforth_status_code_t code, const char* message) {
  tiforth_status_t status;
  status.code = code;
  status.message = nullptr;
  if (message == nullptr) {
    return status;
  }

  const size_t len = std::strlen(message) + 1;
  auto* buffer = static_cast<char*>(std::malloc(len));
  if (buffer == nullptr) {
    return status;
  }
  std::memcpy(buffer, message, len);
  status.message = buffer;
  return status;
}

tiforth_status_t MakeOk() { return MakeStatus(TIFORTH_STATUS_OK, nullptr); }

[[nodiscard]] tiforth_status_t MakeStatusFromArrow(const arrow::Status& status) {
  if (status.ok()) {
    return MakeOk();
  }
  tiforth_status_code_t code = TIFORTH_STATUS_INTERNAL_ERROR;
  if (status.IsInvalid()) {
    code = TIFORTH_STATUS_INVALID_ARGUMENT;
  } else if (status.IsNotImplemented()) {
    code = TIFORTH_STATUS_NOT_IMPLEMENTED;
  }
  return MakeStatus(code, status.ToString().c_str());
}

bool IsZeroedOptions(const tiforth_engine_options_t& options) {
  if (options.reserved_u32 != 0 || options.reserved_u64 != 0) {
    return false;
  }
  for (void* ptr : options.reserved_ptrs) {
    if (ptr != nullptr) {
      return false;
    }
  }
  return true;
}

}  // namespace

struct tiforth_engine_t {
  std::unique_ptr<tiforth::Engine> engine;
};

struct tiforth_pipeline_t {
  tiforth_engine_t* engine = nullptr;
  std::unique_ptr<tiforth::PipelineBuilder> builder;
  std::unique_ptr<tiforth::Pipeline> finalized;
};

struct tiforth_task_t {
  std::unique_ptr<tiforth::Task> task;
};

struct tiforth_expr_t {
  tiforth::ExprPtr expr;
};

extern "C" {

void tiforth_free(void* ptr) { std::free(ptr); }

tiforth_status_t tiforth_expr_field_ref_index(int32_t index, tiforth_expr_t** out_expr) {
  if (out_expr == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "out_expr must not be null");
  }
  *out_expr = nullptr;
  if (index < 0) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "field ref index must be non-negative");
  }

  auto handle = std::make_unique<tiforth_expr_t>();
  handle->expr = tiforth::MakeFieldRef(index);
  *out_expr = handle.release();
  return MakeOk();
}

tiforth_status_t tiforth_expr_literal_int32(int32_t value, tiforth_expr_t** out_expr) {
  if (out_expr == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "out_expr must not be null");
  }
  *out_expr = nullptr;

  auto handle = std::make_unique<tiforth_expr_t>();
  handle->expr = tiforth::MakeLiteral(std::make_shared<arrow::Int32Scalar>(value));
  *out_expr = handle.release();
  return MakeOk();
}

tiforth_status_t tiforth_expr_call(const char* function_name,
                                   const tiforth_expr_t* const* args,
                                   size_t num_args,
                                   tiforth_expr_t** out_expr) {
  if (out_expr == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "out_expr must not be null");
  }
  *out_expr = nullptr;
  if (function_name == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "function_name must not be null");
  }
  if ((args == nullptr && num_args != 0) || (args != nullptr && num_args == 0)) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "args/num_args mismatch");
  }

  std::vector<tiforth::ExprPtr> call_args;
  call_args.reserve(num_args);
  for (size_t i = 0; i < num_args; ++i) {
    if (args[i] == nullptr || args[i]->expr == nullptr) {
      return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "call arg must not be null");
    }
    call_args.push_back(args[i]->expr);
  }

  auto handle = std::make_unique<tiforth_expr_t>();
  handle->expr = tiforth::MakeCall(function_name, std::move(call_args));
  *out_expr = handle.release();
  return MakeOk();
}

void tiforth_expr_destroy(tiforth_expr_t* expr) { delete expr; }

tiforth_status_t tiforth_engine_create(const tiforth_engine_options_t* options,
                                       tiforth_engine_t** out_engine) {
  if (out_engine == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "out_engine must not be null");
  }
  *out_engine = nullptr;

  if (options == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "options must not be null");
  }
  if (options->abi_version != TIFORTH_C_ABI_VERSION) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "unsupported tiforth C ABI version");
  }
  if (!IsZeroedOptions(*options)) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "reserved engine options must be zero");
  }

  auto handle = std::make_unique<tiforth_engine_t>();
  auto engine_res = tiforth::Engine::Create(tiforth::EngineOptions{});
  if (!engine_res.ok()) {
    return MakeStatusFromArrow(engine_res.status());
  }
  handle->engine = std::move(*engine_res);
  *out_engine = handle.release();
  return MakeOk();
}

void tiforth_engine_destroy(tiforth_engine_t* engine) { delete engine; }

tiforth_status_t tiforth_pipeline_create(tiforth_engine_t* engine, tiforth_pipeline_t** out_pipeline) {
  if (out_pipeline == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "out_pipeline must not be null");
  }
  *out_pipeline = nullptr;
  if (engine == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "engine must not be null");
  }
  if (engine->engine == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "engine handle is not initialized");
  }

  auto handle = std::make_unique<tiforth_pipeline_t>();
  handle->engine = engine;
  auto builder_res = tiforth::PipelineBuilder::Create(engine->engine.get());
  if (!builder_res.ok()) {
    return MakeStatusFromArrow(builder_res.status());
  }
  handle->builder = std::move(*builder_res);
  *out_pipeline = handle.release();
  return MakeOk();
}

void tiforth_pipeline_destroy(tiforth_pipeline_t* pipeline) { delete pipeline; }

tiforth_status_t tiforth_pipeline_append_filter(tiforth_pipeline_t* pipeline,
                                                const tiforth_expr_t* predicate) {
  if (pipeline == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "pipeline must not be null");
  }
  if (pipeline->builder == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "pipeline is finalized");
  }
  if (pipeline->engine == nullptr || pipeline->engine->engine == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "pipeline engine must not be null");
  }
  if (predicate == nullptr || predicate->expr == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "predicate must not be null");
  }

  auto predicate_expr = predicate->expr;
  const auto* engine = pipeline->engine->engine.get();
  auto st = pipeline->builder->AppendTransform(
      [engine, predicate_expr]() -> arrow::Result<tiforth::TransformOpPtr> {
        return std::make_unique<tiforth::FilterTransformOp>(engine, predicate_expr);
      });
  return MakeStatusFromArrow(st);
}

tiforth_status_t tiforth_pipeline_append_projection(tiforth_pipeline_t* pipeline,
                                                    const tiforth_projection_expr_t* exprs,
                                                    size_t num_exprs) {
  if (pipeline == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "pipeline must not be null");
  }
  if (pipeline->builder == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "pipeline is finalized");
  }
  if (pipeline->engine == nullptr || pipeline->engine->engine == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "pipeline engine must not be null");
  }
  if ((exprs == nullptr && num_exprs != 0) || (exprs != nullptr && num_exprs == 0)) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "exprs/num_exprs mismatch");
  }

  std::vector<tiforth::ProjectionExpr> projection_exprs;
  projection_exprs.reserve(num_exprs);
  for (size_t i = 0; i < num_exprs; ++i) {
    if (exprs[i].name == nullptr) {
      return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "projection expr name must not be null");
    }
    if (exprs[i].expr == nullptr || exprs[i].expr->expr == nullptr) {
      return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "projection expr must not be null");
    }
    tiforth::ProjectionExpr p;
    p.name = exprs[i].name;
    p.expr = exprs[i].expr->expr;
    projection_exprs.push_back(std::move(p));
  }

  const auto* engine = pipeline->engine->engine.get();
  auto st = pipeline->builder->AppendTransform(
      [engine, projection_exprs = std::move(projection_exprs)]() mutable
          -> arrow::Result<tiforth::TransformOpPtr> {
        return std::make_unique<tiforth::ProjectionTransformOp>(engine, std::move(projection_exprs));
      });
  return MakeStatusFromArrow(st);
}

tiforth_status_t tiforth_pipeline_create_task(tiforth_pipeline_t* pipeline,
                                              tiforth_task_t** out_task) {
  if (out_task == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "out_task must not be null");
  }
  *out_task = nullptr;
  if (pipeline == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "pipeline must not be null");
  }
  if (pipeline->engine == nullptr || pipeline->engine->engine == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "pipeline engine must not be null");
  }
  if (pipeline->finalized == nullptr) {
    if (pipeline->builder == nullptr) {
      return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "pipeline is not buildable");
    }
    auto pipeline_res = pipeline->builder->Finalize();
    if (!pipeline_res.ok()) {
      return MakeStatusFromArrow(pipeline_res.status());
    }
    pipeline->finalized = std::move(*pipeline_res);
    pipeline->builder.reset();
  }

  auto task_res = pipeline->finalized->CreateTask();
  if (!task_res.ok()) {
    return MakeStatusFromArrow(task_res.status());
  }
  auto handle = std::make_unique<tiforth_task_t>();
  handle->task = std::move(*task_res);
  *out_task = handle.release();
  return MakeOk();
}

void tiforth_task_destroy(tiforth_task_t* task) { delete task; }

tiforth_status_t tiforth_task_step(tiforth_task_t* task, tiforth_task_state_t* out_state) {
  if (out_state == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "out_state must not be null");
  }
  *out_state = TIFORTH_TASK_BLOCKED;
  if (task == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "task must not be null");
  }
  if (task->task == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "task handle is not initialized");
  }
  auto res = task->task->Step();
  if (!res.ok()) {
    return MakeStatusFromArrow(res.status());
  }
  switch (*res) {
    case tiforth::TaskState::kNeedInput:
      *out_state = TIFORTH_TASK_NEED_INPUT;
      break;
    case tiforth::TaskState::kHasOutput:
      *out_state = TIFORTH_TASK_HAS_OUTPUT;
      break;
    case tiforth::TaskState::kFinished:
      *out_state = TIFORTH_TASK_FINISHED;
      break;
    case tiforth::TaskState::kBlocked:
      *out_state = TIFORTH_TASK_BLOCKED;
      break;
  }
  return MakeOk();
}

tiforth_status_t tiforth_task_close_input(tiforth_task_t* task) {
  if (task == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "task must not be null");
  }
  if (task->task == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "task handle is not initialized");
  }
  return MakeStatusFromArrow(task->task->CloseInput());
}

tiforth_status_t tiforth_task_push_input_batch(tiforth_task_t* task,
                                               struct ArrowSchema* schema,
                                               struct ArrowArray* array) {
  if (task == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "task must not be null");
  }
  if (schema == nullptr || array == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "schema/array must not be null");
  }
  if (task->task == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "task handle is not initialized");
  }

  auto batch_res = arrow::ImportRecordBatch(array, schema);
  if (!batch_res.ok()) {
    return MakeStatusFromArrow(batch_res.status());
  }
  return MakeStatusFromArrow(task->task->PushInput(std::move(*batch_res)));
}

tiforth_status_t tiforth_task_pull_output_batch(tiforth_task_t* task,
                                                struct ArrowSchema* out_schema,
                                                struct ArrowArray* out_array) {
  if (task == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "task must not be null");
  }
  if (out_schema == nullptr || out_array == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "out_schema/out_array must not be null");
  }
  if (task->task == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INVALID_ARGUMENT, "task handle is not initialized");
  }

  auto batch_res = task->task->PullOutput();
  if (!batch_res.ok()) {
    return MakeStatusFromArrow(batch_res.status());
  }
  if (*batch_res == nullptr) {
    return MakeStatus(TIFORTH_STATUS_INTERNAL_ERROR, "expected non-null output batch");
  }
  return MakeStatusFromArrow(arrow::ExportRecordBatch(**batch_res, out_array, out_schema));
}

}  // extern "C"
