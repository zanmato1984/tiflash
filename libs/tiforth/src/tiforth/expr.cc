#include "tiforth/expr.h"

#include <mutex>
#include <type_traits>
#include <utility>

#include <arrow/array/concatenate.h>
#include <arrow/array/util.h>
#include <arrow/chunked_array.h>
#include <arrow/compute/api_scalar.h>
#include <arrow/compute/exec.h>
#include <arrow/compute/initialize.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>

namespace tiforth {

namespace {

std::once_flag arrow_compute_init_once;
arrow::Status arrow_compute_init_status = arrow::Status::OK();

arrow::Status EnsureArrowComputeInitialized() {
  std::call_once(arrow_compute_init_once,
                 []() { arrow_compute_init_status = arrow::compute::Initialize(); });
  return arrow_compute_init_status;
}

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
          ARROW_RETURN_NOT_OK(EnsureArrowComputeInitialized());

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
