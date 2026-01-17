#include "tiforth/compiled_expr.h"

#include <arrow/compute/exec.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>

#include "tiforth/detail/arrow_compute.h"
#include "tiforth/detail/expr_compiler.h"

namespace tiforth {

arrow::Result<CompiledExpr> CompileExpr(const std::shared_ptr<arrow::Schema>& schema, const Expr& expr,
                                       const Engine* engine, arrow::compute::ExecContext* exec_context) {
  ARROW_ASSIGN_OR_RAISE(auto compiled, detail::CompileExpr(schema, expr, engine, exec_context));
  return CompiledExpr{std::move(compiled.schema), std::move(compiled.bound)};
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
  if (exec_context == nullptr) {
    return arrow::Status::Invalid("exec_context must not be null");
  }
  ARROW_ASSIGN_OR_RAISE(auto out, ExecuteExpr(compiled, batch, exec_context));
  return detail::DatumToArray(out, batch.num_rows(), exec_context->memory_pool());
}

}  // namespace tiforth
