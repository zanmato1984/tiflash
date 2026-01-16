#include "tiforth/operators/projection.h"

#include <utility>

#include <arrow/array.h>
#include <arrow/memory_pool.h>
#include <arrow/status.h>
#include <arrow/type.h>

namespace tiforth {

ProjectionTransformOp::ProjectionTransformOp(std::vector<ProjectionExpr> exprs,
                                             arrow::MemoryPool* memory_pool)
    : exprs_(std::move(exprs)),
      exec_context_(memory_pool != nullptr ? memory_pool : arrow::default_memory_pool()) {}

arrow::Result<std::shared_ptr<arrow::Schema>> ProjectionTransformOp::ComputeOutputSchema(
    const std::vector<std::shared_ptr<arrow::Array>>& arrays) const {
  if (arrays.size() != exprs_.size()) {
    return arrow::Status::Invalid("projection output array count mismatch");
  }

  std::vector<std::shared_ptr<arrow::Field>> fields;
  fields.reserve(exprs_.size());
  for (std::size_t i = 0; i < exprs_.size(); ++i) {
    const auto& name = exprs_[i].name;
    if (name.empty()) {
      return arrow::Status::Invalid("projection field name must not be empty");
    }
    if (arrays[i] == nullptr) {
      return arrow::Status::Invalid("projection array must not be null");
    }
    fields.push_back(arrow::field(name, arrays[i]->type()));
  }
  return arrow::schema(std::move(fields));
}

arrow::Result<OperatorStatus> ProjectionTransformOp::TransformImpl(
    std::shared_ptr<arrow::RecordBatch>* batch) {
  if (*batch == nullptr) {
    return OperatorStatus::kHasOutput;
  }

  const auto& input = **batch;
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.reserve(exprs_.size());

  for (const auto& expr : exprs_) {
    if (expr.expr == nullptr) {
      return arrow::Status::Invalid("projection expr must not be null");
    }
    ARROW_ASSIGN_OR_RAISE(auto array, EvalExprAsArray(input, *expr.expr, &exec_context_));
    if (array->length() != input.num_rows()) {
      return arrow::Status::Invalid("projection array length mismatch");
    }
    arrays.push_back(std::move(array));
  }

  if (output_schema_ == nullptr) {
    ARROW_ASSIGN_OR_RAISE(output_schema_, ComputeOutputSchema(arrays));
  } else {
    if (static_cast<std::size_t>(output_schema_->num_fields()) != arrays.size()) {
      return arrow::Status::Invalid("projection output schema field count mismatch");
    }
    for (std::size_t i = 0; i < arrays.size(); ++i) {
      if (!output_schema_->field(static_cast<int>(i))->type()->Equals(arrays[i]->type())) {
        return arrow::Status::Invalid("projection output schema type mismatch");
      }
    }
  }

  *batch = arrow::RecordBatch::Make(output_schema_, input.num_rows(), std::move(arrays));
  return OperatorStatus::kHasOutput;
}

}  // namespace tiforth
