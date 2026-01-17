#include "tiforth/operators/filter.h"

#include <utility>
#include <vector>

#include <arrow/compute/api_vector.h>
#include <arrow/memory_pool.h>
#include <arrow/status.h>
#include <arrow/type.h>

#include "tiforth/compiled_expr.h"
#include "tiforth/engine.h"
#include "tiforth/detail/arrow_compute.h"

namespace tiforth {

namespace {

}  // namespace

struct FilterTransformOp::Compiled {
  CompiledExpr predicate;
};

FilterTransformOp::~FilterTransformOp() = default;

FilterTransformOp::FilterTransformOp(const Engine* engine, ExprPtr predicate,
                                     arrow::MemoryPool* memory_pool)
    : engine_(engine),
      predicate_(std::move(predicate)),
      exec_context_(memory_pool != nullptr
                        ? memory_pool
                        : (engine != nullptr ? engine->memory_pool() : arrow::default_memory_pool()),
                    /*executor=*/nullptr,
                    engine != nullptr ? engine->function_registry() : nullptr) {}

arrow::Result<OperatorStatus> FilterTransformOp::TransformImpl(
    std::shared_ptr<arrow::RecordBatch>* batch) {
  if (*batch == nullptr) {
    return OperatorStatus::kHasOutput;
  }
  if (engine_ == nullptr) {
    return arrow::Status::Invalid("filter engine must not be null");
  }
  if (predicate_ == nullptr) {
    return arrow::Status::Invalid("filter predicate must not be null");
  }

  const auto& input = **batch;
  if (output_schema_ == nullptr) {
    output_schema_ = input.schema();
  } else if (!output_schema_->Equals(*input.schema(), /*check_metadata=*/true)) {
    return arrow::Status::Invalid("filter input schema mismatch");
  }

  ARROW_RETURN_NOT_OK(detail::EnsureArrowComputeInitialized());

  if (compiled_ == nullptr) {
    if (predicate_ == nullptr) {
      return arrow::Status::Invalid("filter predicate must not be null");
    }
    ARROW_ASSIGN_OR_RAISE(auto compiled_pred,
                          CompileExpr(output_schema_, *predicate_, engine_, &exec_context_));
    compiled_ = std::make_unique<Compiled>(Compiled{std::move(compiled_pred)});
  }

  ARROW_ASSIGN_OR_RAISE(auto predicate_array, ExecuteExprAsArray(compiled_->predicate, input, &exec_context_));
  if (predicate_array == nullptr) {
    return arrow::Status::Invalid("filter predicate result must not be null");
  }
  if (predicate_array->length() != input.num_rows()) {
    return arrow::Status::Invalid("filter predicate length mismatch");
  }
  if (predicate_array->type_id() != arrow::Type::BOOL) {
    return arrow::Status::Invalid("filter predicate must evaluate to boolean");
  }

  const arrow::compute::FilterOptions options(arrow::compute::FilterOptions::DROP);

  std::vector<std::shared_ptr<arrow::Array>> out_columns;
  out_columns.reserve(input.num_columns());
  for (int i = 0; i < input.num_columns(); ++i) {
    ARROW_ASSIGN_OR_RAISE(auto filtered,
                          arrow::compute::Filter(arrow::Datum(input.column(i)),
                                                 arrow::Datum(predicate_array), options,
                                                 &exec_context_));
    ARROW_ASSIGN_OR_RAISE(auto out_array,
                          detail::DatumToArray(filtered, exec_context_.memory_pool()));
    out_columns.push_back(std::move(out_array));
  }

  int64_t out_rows = 0;
  if (!out_columns.empty()) {
    out_rows = out_columns.front()->length();
  } else {
    ARROW_ASSIGN_OR_RAISE(auto filtered_pred,
                          arrow::compute::Filter(arrow::Datum(predicate_array),
                                                 arrow::Datum(predicate_array), options,
                                                 &exec_context_));
    ARROW_ASSIGN_OR_RAISE(auto filtered_pred_array,
                          detail::DatumToArray(filtered_pred, exec_context_.memory_pool()));
    out_rows = filtered_pred_array->length();
  }

  *batch = arrow::RecordBatch::Make(output_schema_, out_rows, std::move(out_columns));
  return OperatorStatus::kHasOutput;
}

}  // namespace tiforth
