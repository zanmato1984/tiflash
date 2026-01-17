#include "tiforth/operators/projection.h"

#include <utility>

#include <arrow/array.h>
#include <arrow/memory_pool.h>
#include <arrow/status.h>
#include <arrow/type.h>

#include "tiforth/compiled_expr.h"
#include "tiforth/engine.h"
#include "tiforth/type_metadata.h"

namespace tiforth {

struct ProjectionTransformOp::Compiled {
  std::vector<CompiledExpr> exprs;
};

ProjectionTransformOp::~ProjectionTransformOp() = default;

ProjectionTransformOp::ProjectionTransformOp(const Engine* engine, std::vector<ProjectionExpr> exprs,
                                             arrow::MemoryPool* memory_pool)
    : engine_(engine),
      exprs_(std::move(exprs)),
      exec_context_(memory_pool != nullptr
                        ? memory_pool
                        : (engine != nullptr ? engine->memory_pool() : arrow::default_memory_pool()),
                    /*executor=*/nullptr,
                    engine != nullptr ? engine->function_registry() : nullptr) {}

arrow::Result<std::shared_ptr<arrow::Schema>> ProjectionTransformOp::ComputeOutputSchema(
    const arrow::RecordBatch& input, const std::vector<std::shared_ptr<arrow::Array>>& arrays) const {
  if (arrays.size() != exprs_.size()) {
    return arrow::Status::Invalid("projection output array count mismatch");
  }

  const auto input_schema = input.schema();

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

    // Preserve metadata when the projection is a direct field reference.
    std::shared_ptr<arrow::Field> out_field;
    if (input_schema != nullptr && exprs_[i].expr != nullptr) {
      if (const auto* field_ref = std::get_if<FieldRef>(&exprs_[i].expr->node); field_ref != nullptr) {
        int index = field_ref->index;
        if (index < 0 && !field_ref->name.empty()) {
          index = input_schema->GetFieldIndex(field_ref->name);
        }
        if (index >= 0 && index < input_schema->num_fields()) {
          out_field = input_schema->field(index);
        }
      }
    }

    if (out_field == nullptr) {
      out_field = arrow::field(name, arrays[i]->type());

      // Carry TiForth logical type metadata for common tricky types where Arrow physical
      // type alone is not enough for round-tripping (e.g. host collation side-channel).
      LogicalType logical_type;
      if (arrays[i]->type_id() == arrow::Type::DECIMAL128 || arrays[i]->type_id() == arrow::Type::DECIMAL256) {
        const auto& dec = static_cast<const arrow::DecimalType&>(*arrays[i]->type());
        logical_type.id = LogicalTypeId::kDecimal;
        logical_type.decimal_precision = static_cast<int32_t>(dec.precision());
        logical_type.decimal_scale = static_cast<int32_t>(dec.scale());
      }
      if (logical_type.id == LogicalTypeId::kUnknown && exprs_[i].expr != nullptr) {
        if (const auto* call = std::get_if<Call>(&exprs_[i].expr->node); call != nullptr) {
          if (call->function_name == "toMyDate") {
            logical_type.id = LogicalTypeId::kMyDate;
          }
        }
      }

      if (logical_type.id != LogicalTypeId::kUnknown) {
        ARROW_ASSIGN_OR_RAISE(out_field, WithLogicalTypeMetadata(out_field, logical_type));
      }
    } else {
      out_field = out_field->WithName(name);
    }

    fields.push_back(std::move(out_field));
  }
  return arrow::schema(std::move(fields));
}

arrow::Result<OperatorStatus> ProjectionTransformOp::TransformImpl(
    std::shared_ptr<arrow::RecordBatch>* batch) {
  if (*batch == nullptr) {
    return OperatorStatus::kHasOutput;
  }
  if (engine_ == nullptr) {
    return arrow::Status::Invalid("projection engine must not be null");
  }

  const auto& input = **batch;
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.reserve(exprs_.size());

  if (compiled_ == nullptr) {
    auto compiled = std::make_unique<Compiled>();
    compiled->exprs.reserve(exprs_.size());
    for (const auto& expr : exprs_) {
      if (expr.expr == nullptr) {
        return arrow::Status::Invalid("projection expr must not be null");
      }
      ARROW_ASSIGN_OR_RAISE(auto compiled_expr,
                            CompileExpr(input.schema(), *expr.expr, engine_, &exec_context_));
      compiled->exprs.push_back(std::move(compiled_expr));
    }
    compiled_ = std::move(compiled);
  }

  if (compiled_->exprs.size() != exprs_.size()) {
    return arrow::Status::Invalid("projection compiled expr count mismatch");
  }

  for (std::size_t i = 0; i < exprs_.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(auto array, ExecuteExprAsArray(compiled_->exprs[i], input, &exec_context_));
    if (array == nullptr) {
      return arrow::Status::Invalid("projection result must not be null");
    }
    if (array->length() != input.num_rows()) {
      return arrow::Status::Invalid("projection array length mismatch");
    }
    arrays.push_back(std::move(array));
  }

  if (output_schema_ == nullptr) {
    ARROW_ASSIGN_OR_RAISE(output_schema_, ComputeOutputSchema(input, arrays));
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
