#include "tiforth/expr.h"

#include <optional>
#include <string_view>
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

#include "tiforth/detail/arrow_compute.h"
#include "tiforth/engine.h"
#include "tiforth/functions/scalar/comparison/collated_compare.h"
#include "tiforth/functions/scalar/temporal/mytime.h"
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

struct TypedValue {
  arrow::Datum datum;
  std::shared_ptr<arrow::Field> field;
  LogicalType logical_type;
};

arrow::Result<int32_t> ResolveStringCollationId(const std::vector<TypedValue>& args) {
  std::optional<int32_t> collation_id;
  for (const auto& arg : args) {
    if (arg.logical_type.id != LogicalTypeId::kString) {
      continue;
    }
    const int32_t id = arg.logical_type.collation_id >= 0 ? arg.logical_type.collation_id : 63;
    if (collation_id.has_value() && *collation_id != id) {
      return arrow::Status::NotImplemented("collation mismatch: ", *collation_id, " vs ", id);
    }
    collation_id = id;
  }
  return collation_id.value_or(63);
}

bool IsCollatedCompareFunction(std::string_view name) {
  return name == "equal" || name == "not_equal" || name == "less" || name == "less_equal" ||
         name == "greater" || name == "greater_equal";
}

bool IsMyTimeFunction(std::string_view name) {
  return name == "toYear" || name == "toMonth" || name == "toDayOfMonth" || name == "toMyDate" ||
         name == "hour" || name == "minute" || name == "second" || name == "microSecond";
}

arrow::Result<std::unique_ptr<arrow::compute::FunctionOptions>> MaybeMakeCallOptions(
    std::string_view function_name, const std::vector<TypedValue>& args) {
  if (!IsCollatedCompareFunction(function_name)) {
    if (!IsMyTimeFunction(function_name)) {
      return nullptr;
    }

    std::optional<LogicalType> mytime_type;
    for (const auto& arg : args) {
      if (arg.logical_type.id != LogicalTypeId::kMyDate &&
          arg.logical_type.id != LogicalTypeId::kMyDateTime) {
        continue;
      }

      if (mytime_type.has_value() && mytime_type->id != arg.logical_type.id) {
        return arrow::Status::NotImplemented("mixed MyTime logical types in call");
      }
      mytime_type = arg.logical_type;
    }

    if (!mytime_type.has_value()) {
      return nullptr;
    }
    return functions::MakeMyTimeOptions(mytime_type->id, mytime_type->datetime_fsp);
  }

  bool has_string = false;
  for (const auto& arg : args) {
    if (arg.logical_type.id == LogicalTypeId::kString) {
      has_string = true;
      break;
    }
  }
  if (!has_string) {
    return nullptr;
  }

  ARROW_ASSIGN_OR_RAISE(const auto collation_id, ResolveStringCollationId(args));
  return functions::MakeCollatedCompareOptions(collation_id);
}

arrow::Result<TypedValue> EvalExprTypedImpl(const arrow::RecordBatch& batch, const Expr& expr,
                                           const Engine* engine,
                                           arrow::compute::ExecContext* exec_context) {
  return std::visit(
      [&](const auto& node) -> arrow::Result<TypedValue> {
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
          return TypedValue{std::move(datum), std::move(field), logical_type};
        } else if constexpr (std::is_same_v<T, Literal>) {
          if (node.value == nullptr) {
            return arrow::Status::Invalid("literal value must not be null");
          }
          auto datum = arrow::Datum(node.value);
          ARROW_ASSIGN_OR_RAISE(auto logical_type, InferLogicalType(datum, /*field=*/nullptr));
          return TypedValue{std::move(datum), /*field=*/nullptr, logical_type};
        } else if constexpr (std::is_same_v<T, Call>) {
          std::vector<TypedValue> args;
          args.reserve(node.args.size());
          for (const auto& arg : node.args) {
            if (arg == nullptr) {
              return arrow::Status::Invalid("call arg must not be null");
            }
            ARROW_ASSIGN_OR_RAISE(auto value, EvalExprTypedImpl(batch, *arg, engine, exec_context));
            args.push_back(std::move(value));
          }

          if (exec_context == nullptr) {
            return arrow::Status::Invalid("exec_context must not be null");
          }
          ARROW_RETURN_NOT_OK(detail::EnsureArrowComputeInitialized());

          std::vector<arrow::Datum> arrow_args;
          arrow_args.reserve(args.size());
          for (const auto& arg : args) {
            arrow_args.push_back(arg.datum);
          }

          ARROW_ASSIGN_OR_RAISE(auto options, MaybeMakeCallOptions(node.function_name, args));
          ARROW_ASSIGN_OR_RAISE(auto out,
                                arrow::compute::CallFunction(node.function_name, arrow_args,
                                                             options.get(), exec_context));
          ARROW_ASSIGN_OR_RAISE(auto logical_type, InferLogicalType(out, /*field=*/nullptr));
          if (node.function_name == "toMyDate") {
            logical_type.id = LogicalTypeId::kMyDate;
          }
          return TypedValue{std::move(out), /*field=*/nullptr, logical_type};
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
      engine != nullptr ? engine->memory_pool() : arrow::default_memory_pool(), /*executor=*/nullptr,
      engine != nullptr ? engine->function_registry() : nullptr);
  auto* ctx = GetExecContext(exec_context, &local_exec_context);
  ARROW_ASSIGN_OR_RAISE(auto typed, EvalExprTypedImpl(batch, expr, engine, ctx));
  return std::move(typed.datum);
}

arrow::Result<std::shared_ptr<arrow::Array>> EvalExprAsArray(
    const arrow::RecordBatch& batch, const Expr& expr, const Engine* engine,
    arrow::compute::ExecContext* exec_context) {
  arrow::compute::ExecContext local_exec_context(
      engine != nullptr ? engine->memory_pool() : arrow::default_memory_pool(), /*executor=*/nullptr,
      engine != nullptr ? engine->function_registry() : nullptr);
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
