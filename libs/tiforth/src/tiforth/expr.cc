#include "tiforth/expr.h"

#include <optional>
#include <type_traits>
#include <utility>

#include <arrow/array.h>
#include <arrow/array/concatenate.h>
#include <arrow/array/util.h>
#include <arrow/builder.h>
#include <arrow/chunked_array.h>
#include <arrow/compute/api_scalar.h>
#include <arrow/compute/exec.h>
#include <arrow/memory_pool.h>
#include <arrow/scalar.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>

#include "tiforth/collation.h"
#include "tiforth/detail/arrow_compute.h"
#include "tiforth/type_metadata.h"

namespace tiforth {

namespace {

arrow::compute::ExecContext* GetExecContext(arrow::compute::ExecContext* maybe_exec_context,
                                           arrow::compute::ExecContext* local_exec_context) {
  if (maybe_exec_context != nullptr) {
    return maybe_exec_context;
  }
  return local_exec_context;
}

arrow::Result<arrow::Datum> EvalExprImpl(const arrow::RecordBatch& batch, const Expr& expr,
                                        arrow::compute::ExecContext* exec_context) {
  return std::visit(
      [&](const auto& node) -> arrow::Result<arrow::Datum> {
        using T = std::decay_t<decltype(node)>;
        if constexpr (std::is_same_v<T, FieldRef>) {
          if (node.index >= 0) {
            if (node.index >= batch.num_columns()) {
              return arrow::Status::Invalid("field index out of range");
            }
            return arrow::Datum(batch.column(node.index));
          }
          if (node.name.empty()) {
            return arrow::Status::Invalid("field ref must have name or index");
          }
          const int index = batch.schema()->GetFieldIndex(node.name);
          if (index < 0) {
            return arrow::Status::Invalid("unknown field name: ", node.name);
          }
          return arrow::Datum(batch.column(index));
        } else if constexpr (std::is_same_v<T, Literal>) {
          if (node.value == nullptr) {
            return arrow::Status::Invalid("literal value must not be null");
          }
          return arrow::Datum(node.value);
        } else if constexpr (std::is_same_v<T, Call>) {
          ARROW_RETURN_NOT_OK(detail::EnsureArrowComputeInitialized());

          // Collation-aware binary string comparisons (MS8 common path).
          if ((node.function_name == "equal" || node.function_name == "not_equal" ||
               node.function_name == "less" || node.function_name == "less_equal" ||
               node.function_name == "greater" || node.function_name == "greater_equal") &&
              node.args.size() == 2) {
            auto schema = batch.schema();
            if (schema != nullptr) {
              const auto try_get_field_index = [&](const ExprPtr& arg) -> std::optional<int> {
                if (arg == nullptr) {
                  return std::nullopt;
                }
                const auto* field_ref = std::get_if<FieldRef>(&arg->node);
                if (field_ref == nullptr) {
                  return std::nullopt;
                }
                if (field_ref->index >= 0) {
                  if (field_ref->index < schema->num_fields()) {
                    return field_ref->index;
                  }
                  return std::nullopt;
                }
                if (!field_ref->name.empty()) {
                  const int idx = schema->GetFieldIndex(field_ref->name);
                  if (idx >= 0) {
                    return idx;
                  }
                }
                return std::nullopt;
              };

              std::optional<int> field_index = try_get_field_index(node.args[0]);
              if (!field_index.has_value()) {
                field_index = try_get_field_index(node.args[1]);
              }

              if (field_index.has_value()) {
                const auto& field = schema->field(*field_index);
                if (field != nullptr) {
                  ARROW_ASSIGN_OR_RAISE(const auto logical_type, GetLogicalType(*field));
                  if (logical_type.id == LogicalTypeId::kString) {
                    const int32_t collation_id =
                        logical_type.collation_id >= 0 ? logical_type.collation_id : 63;
                    const auto collation = CollationFromId(collation_id);
                    if (collation.kind == CollationKind::kUnsupported) {
                      return arrow::Status::NotImplemented("unsupported collation id: ",
                                                           collation_id);
                    }

                    ARROW_ASSIGN_OR_RAISE(auto lhs, EvalExprImpl(batch, *node.args[0], exec_context));
                    ARROW_ASSIGN_OR_RAISE(auto rhs, EvalExprImpl(batch, *node.args[1], exec_context));

                    const auto to_base_binary_array =
                        [](const arrow::Datum& datum) -> arrow::Result<std::shared_ptr<arrow::Array>> {
                      if (!datum.is_array()) {
                        return arrow::Status::Invalid("expected array datum for string comparison");
                      }
                      return datum.make_array();
                    };

                    const auto to_base_binary_scalar =
                        [](const arrow::Datum& datum) -> const arrow::BaseBinaryScalar* {
                      if (!datum.is_scalar() || datum.scalar() == nullptr) {
                        return nullptr;
                      }
                      return dynamic_cast<const arrow::BaseBinaryScalar*>(datum.scalar().get());
                    };

                    std::shared_ptr<arrow::Array> lhs_array;
                    std::shared_ptr<arrow::Array> rhs_array;
                    const arrow::BaseBinaryScalar* lhs_scalar = to_base_binary_scalar(lhs);
                    const arrow::BaseBinaryScalar* rhs_scalar = to_base_binary_scalar(rhs);

                    if (lhs.is_array()) {
                      ARROW_ASSIGN_OR_RAISE(lhs_array, to_base_binary_array(lhs));
                    }
                    if (rhs.is_array()) {
                      ARROW_ASSIGN_OR_RAISE(rhs_array, to_base_binary_array(rhs));
                    }

                    const auto apply_predicate = [&](int cmp) -> arrow::Result<bool> {
                      if (node.function_name == "equal") {
                        return cmp == 0;
                      }
                      if (node.function_name == "not_equal") {
                        return cmp != 0;
                      }
                      if (node.function_name == "less") {
                        return cmp < 0;
                      }
                      if (node.function_name == "less_equal") {
                        return cmp <= 0;
                      }
                      if (node.function_name == "greater") {
                        return cmp > 0;
                      }
                      if (node.function_name == "greater_equal") {
                        return cmp >= 0;
                      }
                      return arrow::Status::Invalid("unexpected comparison function: ",
                                                   node.function_name);
                    };

                    const auto is_binary_like = [](arrow::Type::type type_id) {
                      return type_id == arrow::Type::BINARY || type_id == arrow::Type::LARGE_BINARY ||
                             type_id == arrow::Type::STRING || type_id == arrow::Type::LARGE_STRING;
                    };

                    const auto get_view_at = [](const arrow::Array& array, int64_t i) -> std::string_view {
                      const auto type_id = array.type_id();
                      if (type_id == arrow::Type::LARGE_BINARY || type_id == arrow::Type::LARGE_STRING) {
                        const auto& bin = static_cast<const arrow::LargeBinaryArray&>(array);
                        return bin.GetView(i);
                      }
                      const auto& bin = static_cast<const arrow::BinaryArray&>(array);
                      return bin.GetView(i);
                    };

                    // Array-scalar, scalar-array, array-array, scalar-scalar.
                    if (lhs_array != nullptr && rhs_scalar != nullptr) {
                      if (!is_binary_like(lhs_array->type_id())) {
                        return arrow::Status::NotImplemented("string comparison requires binary-like arrays");
                      }
                      arrow::BooleanBuilder out_builder(
                          exec_context != nullptr ? exec_context->memory_pool()
                                                  : arrow::default_memory_pool());
                      const bool scalar_valid = rhs_scalar->is_valid;
                      const std::string_view rhs_view = rhs_scalar->view();
                      const int64_t rows = lhs_array->length();
                      for (int64_t i = 0; i < rows; ++i) {
                        if (lhs_array->IsNull(i) || !scalar_valid) {
                          ARROW_RETURN_NOT_OK(out_builder.AppendNull());
                          continue;
                        }
                        const int cmp = CompareString(collation, get_view_at(*lhs_array, i), rhs_view);
                        ARROW_ASSIGN_OR_RAISE(const bool keep, apply_predicate(cmp));
                        ARROW_RETURN_NOT_OK(out_builder.Append(keep));
                      }
                      std::shared_ptr<arrow::Array> out;
                      ARROW_RETURN_NOT_OK(out_builder.Finish(&out));
                      return arrow::Datum(std::move(out));
                    }

                    if (lhs_scalar != nullptr && rhs_array != nullptr) {
                      if (!is_binary_like(rhs_array->type_id())) {
                        return arrow::Status::NotImplemented("string comparison requires binary-like arrays");
                      }
                      arrow::BooleanBuilder out_builder(
                          exec_context != nullptr ? exec_context->memory_pool()
                                                  : arrow::default_memory_pool());
                      const bool scalar_valid = lhs_scalar->is_valid;
                      const std::string_view lhs_view = lhs_scalar->view();
                      const int64_t rows = rhs_array->length();
                      for (int64_t i = 0; i < rows; ++i) {
                        if (rhs_array->IsNull(i) || !scalar_valid) {
                          ARROW_RETURN_NOT_OK(out_builder.AppendNull());
                          continue;
                        }
                        const int cmp = CompareString(collation, lhs_view, get_view_at(*rhs_array, i));
                        ARROW_ASSIGN_OR_RAISE(const bool keep, apply_predicate(cmp));
                        ARROW_RETURN_NOT_OK(out_builder.Append(keep));
                      }
                      std::shared_ptr<arrow::Array> out;
                      ARROW_RETURN_NOT_OK(out_builder.Finish(&out));
                      return arrow::Datum(std::move(out));
                    }

                    if (lhs_array != nullptr && rhs_array != nullptr) {
                      if (lhs_array->length() != rhs_array->length()) {
                        return arrow::Status::Invalid("string comparison array length mismatch");
                      }
                      if (!is_binary_like(lhs_array->type_id()) || !is_binary_like(rhs_array->type_id())) {
                        return arrow::Status::NotImplemented("string comparison requires binary-like arrays");
                      }
                      arrow::BooleanBuilder out_builder(
                          exec_context != nullptr ? exec_context->memory_pool()
                                                  : arrow::default_memory_pool());
                      const int64_t rows = lhs_array->length();
                      for (int64_t i = 0; i < rows; ++i) {
                        if (lhs_array->IsNull(i) || rhs_array->IsNull(i)) {
                          ARROW_RETURN_NOT_OK(out_builder.AppendNull());
                          continue;
                        }
                        const int cmp = CompareString(collation, get_view_at(*lhs_array, i),
                                                      get_view_at(*rhs_array, i));
                        ARROW_ASSIGN_OR_RAISE(const bool keep, apply_predicate(cmp));
                        ARROW_RETURN_NOT_OK(out_builder.Append(keep));
                      }
                      std::shared_ptr<arrow::Array> out;
                      ARROW_RETURN_NOT_OK(out_builder.Finish(&out));
                      return arrow::Datum(std::move(out));
                    }

                    if (lhs_scalar != nullptr && rhs_scalar != nullptr) {
                      if (!lhs_scalar->is_valid || !rhs_scalar->is_valid) {
                        return arrow::Datum(std::make_shared<arrow::BooleanScalar>());
                      }
                      const int cmp = CompareString(collation, lhs_scalar->view(), rhs_scalar->view());
                      ARROW_ASSIGN_OR_RAISE(const bool keep, apply_predicate(cmp));
                      return arrow::Datum(std::make_shared<arrow::BooleanScalar>(keep));
                    }
                  }
                }
              }
            }
          }

          std::vector<arrow::Datum> args;
          args.reserve(node.args.size());
          for (const auto& arg : node.args) {
            if (arg == nullptr) {
              return arrow::Status::Invalid("call arg must not be null");
            }
            ARROW_ASSIGN_OR_RAISE(auto datum, EvalExprImpl(batch, *arg, exec_context));
            args.push_back(std::move(datum));
          }
          return arrow::compute::CallFunction(node.function_name, args,
                                              /*options=*/nullptr, exec_context);
        } else {
          return arrow::Status::Invalid("unknown Expr variant");
        }
      },
      expr.node);
}

}  // namespace

ExprPtr MakeFieldRef(std::string name) {
  auto expr = std::make_shared<Expr>();
  expr->node = FieldRef{std::move(name), -1};
  return expr;
}

ExprPtr MakeFieldRef(int index) {
  auto expr = std::make_shared<Expr>();
  expr->node = FieldRef{"", index};
  return expr;
}

ExprPtr MakeLiteral(std::shared_ptr<arrow::Scalar> value) {
  auto expr = std::make_shared<Expr>();
  expr->node = Literal{std::move(value)};
  return expr;
}

ExprPtr MakeCall(std::string function_name, std::vector<ExprPtr> args) {
  auto expr = std::make_shared<Expr>();
  expr->node = Call{std::move(function_name), std::move(args)};
  return expr;
}

arrow::Result<arrow::Datum> EvalExpr(const arrow::RecordBatch& batch, const Expr& expr,
                                    arrow::compute::ExecContext* exec_context) {
  arrow::compute::ExecContext local_exec_context;
  return EvalExprImpl(batch, expr, GetExecContext(exec_context, &local_exec_context));
}

arrow::Result<std::shared_ptr<arrow::Array>> EvalExprAsArray(
    const arrow::RecordBatch& batch, const Expr& expr, arrow::compute::ExecContext* exec_context) {
  arrow::compute::ExecContext local_exec_context;
  auto* ctx = GetExecContext(exec_context, &local_exec_context);

  ARROW_ASSIGN_OR_RAISE(auto datum, EvalExprImpl(batch, expr, ctx));
  if (datum.is_array()) {
    return datum.make_array();
  }
  if (datum.is_chunked_array()) {
    auto chunked = datum.chunked_array();
    if (chunked == nullptr) {
      return arrow::Status::Invalid("expected non-null chunked array datum");
    }
    if (chunked->num_chunks() == 1) {
      return chunked->chunk(0);
    }
    return arrow::Concatenate(chunked->chunks(), ctx->memory_pool());
  }
  if (datum.is_scalar()) {
    return arrow::MakeArrayFromScalar(*datum.scalar(), batch.num_rows(), ctx->memory_pool());
  }
  return arrow::Status::Invalid("unsupported datum kind for scalar expression result");
}

}  // namespace tiforth
