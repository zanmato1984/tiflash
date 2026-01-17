#include "tiforth/detail/expr_compiler.h"

#include <optional>
#include <string_view>
#include <type_traits>
#include <utility>

#include <arrow/array.h>
#include <arrow/compute/exec.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/scalar.h>
#include <arrow/status.h>
#include <arrow/type.h>

#include "tiforth/collation.h"
#include "tiforth/detail/arrow_compute.h"
#include "tiforth/engine.h"
#include "tiforth/expr.h"
#include "tiforth/functions/scalar/comparison/collated_compare.h"
#include "tiforth/functions/scalar/temporal/mytime.h"
#include "tiforth/type_metadata.h"

namespace tiforth::detail {

namespace {

struct TypedExpr {
  arrow::compute::Expression expr;
  LogicalType logical_type;
};

arrow::Result<LogicalType> InferLogicalType(const std::shared_ptr<arrow::Field>& field,
                                           const std::shared_ptr<arrow::DataType>& physical_type) {
  LogicalType out;
  if (field != nullptr) {
    ARROW_ASSIGN_OR_RAISE(out, GetLogicalType(*field));
  }

  if (out.id == LogicalTypeId::kUnknown && physical_type != nullptr &&
      (physical_type->id() == arrow::Type::DECIMAL128 || physical_type->id() == arrow::Type::DECIMAL256)) {
    const auto& dec = static_cast<const arrow::DecimalType&>(*physical_type);
    out.id = LogicalTypeId::kDecimal;
    out.decimal_precision = static_cast<int32_t>(dec.precision());
    out.decimal_scale = static_cast<int32_t>(dec.scale());
  }

  return out;
}

arrow::Result<int32_t> ResolveStringCollationId(const std::vector<TypedExpr>& args) {
  const auto canonical_id_for_kind = [](CollationKind kind) -> int32_t {
    switch (kind) {
      case CollationKind::kBinary:
        return 63;
      case CollationKind::kPaddingBinary:
        return 46;
      case CollationKind::kGeneralCi:
        return 33;
      case CollationKind::kUnicodeCi0400:
        return 192;
      case CollationKind::kUnicodeCi0900:
        return 255;
      case CollationKind::kUnsupported:
        break;
    }
    return 63;
  };

  std::optional<CollationKind> resolved_kind;
  for (const auto& arg : args) {
    if (arg.logical_type.id != LogicalTypeId::kString) {
      continue;
    }
    const int32_t id = arg.logical_type.collation_id >= 0 ? arg.logical_type.collation_id : 63;
    const auto collation = CollationFromId(id);
    if (collation.kind == CollationKind::kUnsupported) {
      return arrow::Status::NotImplemented("unsupported collation id: ", id);
    }

    if (!resolved_kind.has_value()) {
      resolved_kind = collation.kind;
      continue;
    }

    if (collation.kind == *resolved_kind) {
      continue;
    }

    // Minimal coercion (common path): mixing BINARY and padding-BIN should fall back to BINARY
    // semantics (no trailing-space trimming).
    const bool binary_mix =
        (collation.kind == CollationKind::kBinary && *resolved_kind == CollationKind::kPaddingBinary) ||
        (collation.kind == CollationKind::kPaddingBinary && *resolved_kind == CollationKind::kBinary);
    if (binary_mix) {
      resolved_kind = CollationKind::kBinary;
      continue;
    }

    return arrow::Status::NotImplemented("collation mismatch in call");
  }

  return canonical_id_for_kind(resolved_kind.value_or(CollationKind::kBinary));
}

bool IsCollatedCompareFunction(std::string_view name) {
  return name == "equal" || name == "not_equal" || name == "less" || name == "less_equal" ||
         name == "greater" || name == "greater_equal";
}

bool IsMyTimeFunction(std::string_view name) {
  return name == "toYear" || name == "toMonth" || name == "toDayOfMonth" || name == "toMyDate" ||
         name == "toDayOfWeek" || name == "toWeek" || name == "toYearWeek" ||
         name == "tidbDayOfWeek" || name == "tidbWeekOfYear" || name == "yearWeek" ||
         name == "hour" || name == "minute" || name == "second" || name == "microSecond";
}

bool IsPackedMyTimeExtractFunction(std::string_view name) {
  return name == "hour" || name == "minute" || name == "second";
}

bool HasMyTimeArgs(const std::vector<TypedExpr>& args) {
  for (const auto& arg : args) {
    if (arg.logical_type.id == LogicalTypeId::kMyDate || arg.logical_type.id == LogicalTypeId::kMyDateTime) {
      return true;
    }
  }
  return false;
}

arrow::Result<std::optional<LogicalType>> ResolveMyTimeType(const std::vector<TypedExpr>& args) {
  std::optional<LogicalType> mytime;
  for (const auto& arg : args) {
    if (arg.logical_type.id != LogicalTypeId::kMyDate && arg.logical_type.id != LogicalTypeId::kMyDateTime) {
      continue;
    }
    if (mytime.has_value() && mytime->id != arg.logical_type.id) {
      return arrow::Status::NotImplemented("mixed MyTime logical types in call");
    }
    mytime = arg.logical_type;
  }
  return mytime;
}

std::string CollatedCompareName(std::string_view op) {
  return std::string("tiforth.collated_") + std::string(op);
}

bool IsDecimalLogical(const LogicalType& t) { return t.id == LogicalTypeId::kDecimal; }

bool IsDecimalAddCall(std::string_view name, const std::vector<TypedExpr>& args) {
  if (name != "add") {
    return false;
  }
  if (args.size() != 2) {
    return false;
  }

  const bool lhs_decimal = IsDecimalLogical(args[0].logical_type);
  const bool rhs_decimal = IsDecimalLogical(args[1].logical_type);
  return lhs_decimal || rhs_decimal;
}

arrow::Result<TypedExpr> CompileTypedExprImpl(const arrow::Schema& schema, const Expr& expr,
                                              const Engine* engine);

arrow::Result<TypedExpr> CompileCall(const arrow::Schema& schema, const Call& call,
                                     const Engine* engine) {
  std::vector<TypedExpr> args;
  args.reserve(call.args.size());
  std::vector<arrow::compute::Expression> arrow_args;
  arrow_args.reserve(call.args.size());

  for (const auto& arg : call.args) {
    if (arg == nullptr) {
      return arrow::Status::Invalid("call arg must not be null");
    }
    ARROW_ASSIGN_OR_RAISE(auto compiled, CompileTypedExprImpl(schema, *arg, engine));
    arrow_args.push_back(compiled.expr);
    args.push_back(std::move(compiled));
  }

  const bool enable_tiforth = engine != nullptr && engine->function_registry() != nullptr;

  std::string function_name = call.function_name;
  std::shared_ptr<arrow::compute::FunctionOptions> options;

  if (enable_tiforth) {
    if (IsDecimalAddCall(function_name, args)) {
      function_name = "tiforth.decimal_add";
    } else if (IsCollatedCompareFunction(function_name)) {
      bool has_string = false;
      for (const auto& arg : args) {
        if (arg.logical_type.id == LogicalTypeId::kString) {
          has_string = true;
          break;
        }
      }
      if (has_string) {
        ARROW_ASSIGN_OR_RAISE(const auto collation_id, ResolveStringCollationId(args));
        auto unique_options = functions::MakeCollatedCompareOptions(collation_id);
        options = std::shared_ptr<arrow::compute::FunctionOptions>(std::move(unique_options));
        function_name = CollatedCompareName(function_name);
      }
    } else if (IsMyTimeFunction(function_name) && HasMyTimeArgs(args)) {
      ARROW_ASSIGN_OR_RAISE(const auto mytime, ResolveMyTimeType(args));
      if (mytime.has_value()) {
        auto unique_options = functions::MakeMyTimeOptions(mytime->id, mytime->datetime_fsp);
        options = std::shared_ptr<arrow::compute::FunctionOptions>(std::move(unique_options));
      }

      if (options != nullptr && IsPackedMyTimeExtractFunction(function_name)) {
        if (function_name == "hour") {
          function_name = "tiforth.mytime_hour";
        } else if (function_name == "minute") {
          function_name = "tiforth.mytime_minute";
        } else if (function_name == "second") {
          function_name = "tiforth.mytime_second";
        }
      }
    }
  }

  auto out_expr = arrow::compute::call(function_name, std::move(arrow_args), std::move(options));

  LogicalType out_logical;
  if (enable_tiforth) {
    if (function_name == "tiforth.decimal_add") {
      out_logical.id = LogicalTypeId::kDecimal;
    } else if (function_name == "toMyDate") {
      out_logical.id = LogicalTypeId::kMyDate;
    }
  }

  return TypedExpr{std::move(out_expr), out_logical};
}

arrow::Result<TypedExpr> CompileTypedExprImpl(const arrow::Schema& schema, const Expr& expr,
                                              const Engine* engine) {
  return std::visit(
      [&](const auto& node) -> arrow::Result<TypedExpr> {
        using T = std::decay_t<decltype(node)>;
        if constexpr (std::is_same_v<T, FieldRef>) {
          int index = node.index;
          std::shared_ptr<arrow::Field> field;

          if (index < 0) {
            if (node.name.empty()) {
              return arrow::Status::Invalid("field ref must have name or index");
            }
            index = schema.GetFieldIndex(node.name);
          }

          if (index < 0 || index >= schema.num_fields()) {
            return arrow::Status::Invalid("unknown field: ", node.name);
          }

          field = schema.field(index);
          auto out_expr = arrow::compute::field_ref(arrow::FieldRef(index));
          ARROW_ASSIGN_OR_RAISE(auto logical_type, InferLogicalType(field, field != nullptr ? field->type() : nullptr));
          return TypedExpr{std::move(out_expr), logical_type};
        } else if constexpr (std::is_same_v<T, Literal>) {
          if (node.value == nullptr) {
            return arrow::Status::Invalid("literal value must not be null");
          }
          auto scalar_type = node.value->type;
          ARROW_ASSIGN_OR_RAISE(auto logical_type,
                                InferLogicalType(/*field=*/nullptr, scalar_type));
          return TypedExpr{arrow::compute::literal(arrow::Datum(node.value)), logical_type};
        } else if constexpr (std::is_same_v<T, Call>) {
          return CompileCall(schema, node, engine);
        } else {
          return arrow::Status::Invalid("unknown Expr variant");
        }
      },
      expr.node);
}

}  // namespace

arrow::Result<CompiledExpr> CompileExpr(const std::shared_ptr<arrow::Schema>& schema, const Expr& expr,
                                       const Engine* engine,
                                       arrow::compute::ExecContext* exec_context) {
  if (schema == nullptr) {
    return arrow::Status::Invalid("input schema must not be null");
  }
  if (exec_context == nullptr) {
    return arrow::Status::Invalid("exec_context must not be null");
  }
  ARROW_RETURN_NOT_OK(EnsureArrowComputeInitialized());

  ARROW_ASSIGN_OR_RAISE(auto typed, CompileTypedExprImpl(*schema, expr, engine));

  ARROW_ASSIGN_OR_RAISE(auto bound, typed.expr.Bind(*schema, exec_context));
  return CompiledExpr{schema, std::move(bound)};
}

arrow::Result<arrow::Datum> ExecuteExpr(const CompiledExpr& compiled, const arrow::RecordBatch& batch,
                                       arrow::compute::ExecContext* exec_context) {
  if (compiled.schema == nullptr) {
    return arrow::Status::Invalid("compiled schema must not be null");
  }
  if (exec_context == nullptr) {
    return arrow::Status::Invalid("exec_context must not be null");
  }
  if (batch.schema() == nullptr || !compiled.schema->Equals(*batch.schema(), /*check_metadata=*/true)) {
    return arrow::Status::Invalid("input batch schema mismatch for compiled expression");
  }

  return arrow::compute::ExecuteScalarExpression(compiled.bound, arrow::compute::ExecBatch(batch),
                                                 exec_context);
}

arrow::Result<std::shared_ptr<arrow::Array>> ExecuteExprAsArray(const CompiledExpr& compiled,
                                                                const arrow::RecordBatch& batch,
                                                                arrow::compute::ExecContext* exec_context) {
  ARROW_ASSIGN_OR_RAISE(auto out, ExecuteExpr(compiled, batch, exec_context));
  return DatumToArray(out, batch.num_rows(), exec_context != nullptr ? exec_context->memory_pool()
                                                                     : arrow::default_memory_pool());
}

}  // namespace tiforth::detail
