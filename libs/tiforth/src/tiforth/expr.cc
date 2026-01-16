#include "tiforth/expr.h"

#include <type_traits>
#include <utility>

#include <arrow/array.h>
#include <arrow/array/concatenate.h>
#include <arrow/array/util.h>
#include <arrow/chunked_array.h>
#include <arrow/compute/exec.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/scalar.h>
#include <arrow/status.h>
#include <arrow/type.h>

#include "tiforth/engine.h"
#include "tiforth/function_registry.h"
#include "tiforth/type_metadata.h"

namespace tiforth {

namespace {

const FunctionRegistry& GetRegistry(const Engine* engine) {
  if (engine != nullptr) {
    return engine->function_registry();
  }

  static std::shared_ptr<FunctionRegistry> default_registry = FunctionRegistry::MakeDefault();
  return *default_registry;
}

arrow::compute::ExecContext* GetExecContext(arrow::compute::ExecContext* maybe_exec_context,
                                           arrow::compute::ExecContext* local_exec_context) {
  if (maybe_exec_context != nullptr) {
    return maybe_exec_context;
  }
  return local_exec_context;
}

arrow::Result<LogicalType> InferLogicalType(const arrow::Datum& datum,
                                           const std::shared_ptr<arrow::Field>& field) {
  LogicalType out;
  if (field != nullptr) {
    ARROW_ASSIGN_OR_RAISE(out, GetLogicalType(*field));
  }

  if (out.id == LogicalTypeId::kUnknown) {
    const auto type = datum.type();
    if (type != nullptr &&
        (type->id() == arrow::Type::DECIMAL128 || type->id() == arrow::Type::DECIMAL256)) {
      const auto& dec = static_cast<const arrow::DecimalType&>(*type);
      out.id = LogicalTypeId::kDecimal;
      out.decimal_precision = static_cast<int32_t>(dec.precision());
      out.decimal_scale = static_cast<int32_t>(dec.scale());
    }
  }

  return out;
}

arrow::Result<TypedDatum> EvalExprTypedImpl(const arrow::RecordBatch& batch, const Expr& expr,
                                           const Engine* engine,
                                           arrow::compute::ExecContext* exec_context) {
  return std::visit(
      [&](const auto& node) -> arrow::Result<TypedDatum> {
        using T = std::decay_t<decltype(node)>;
        if constexpr (std::is_same_v<T, FieldRef>) {
          auto schema = batch.schema();

          int index = node.index;
          if (index < 0) {
            if (node.name.empty()) {
              return arrow::Status::Invalid("field ref must have name or index");
            }
            if (schema == nullptr) {
              return arrow::Status::Invalid("input schema must not be null");
            }
            index = schema->GetFieldIndex(node.name);
          }

          if (index < 0 || index >= batch.num_columns()) {
            return arrow::Status::Invalid("unknown field: ", node.name);
          }

          std::shared_ptr<arrow::Field> field;
          if (schema != nullptr && index < schema->num_fields()) {
            field = schema->field(index);
          }
          auto datum = arrow::Datum(batch.column(index));
          ARROW_ASSIGN_OR_RAISE(auto logical_type, InferLogicalType(datum, field));
          return TypedDatum{std::move(datum), std::move(field), logical_type};
        } else if constexpr (std::is_same_v<T, Literal>) {
          if (node.value == nullptr) {
            return arrow::Status::Invalid("literal value must not be null");
          }
          auto datum = arrow::Datum(node.value);
          ARROW_ASSIGN_OR_RAISE(auto logical_type, InferLogicalType(datum, /*field=*/nullptr));
          return TypedDatum{std::move(datum), /*field=*/nullptr, logical_type};
        } else if constexpr (std::is_same_v<T, Call>) {
          std::vector<TypedDatum> args;
          args.reserve(node.args.size());
          for (const auto& arg : node.args) {
            if (arg == nullptr) {
              return arrow::Status::Invalid("call arg must not be null");
            }
            ARROW_ASSIGN_OR_RAISE(auto datum, EvalExprTypedImpl(batch, *arg, engine, exec_context));
            args.push_back(std::move(datum));
          }
          return GetRegistry(engine).Call(node.function_name, args, exec_context);
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
                                    const Engine* engine,
                                    arrow::compute::ExecContext* exec_context) {
  arrow::compute::ExecContext local_exec_context(
      engine != nullptr ? engine->memory_pool() : arrow::default_memory_pool());
  auto* ctx = GetExecContext(exec_context, &local_exec_context);
  ARROW_ASSIGN_OR_RAISE(auto typed, EvalExprTypedImpl(batch, expr, engine, ctx));
  return std::move(typed.datum);
}

arrow::Result<std::shared_ptr<arrow::Array>> EvalExprAsArray(
    const arrow::RecordBatch& batch, const Expr& expr, const Engine* engine,
    arrow::compute::ExecContext* exec_context) {
  arrow::compute::ExecContext local_exec_context(
      engine != nullptr ? engine->memory_pool() : arrow::default_memory_pool());
  auto* ctx = GetExecContext(exec_context, &local_exec_context);

  ARROW_ASSIGN_OR_RAISE(auto typed, EvalExprTypedImpl(batch, expr, engine, ctx));
  const auto& datum = typed.datum;

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

