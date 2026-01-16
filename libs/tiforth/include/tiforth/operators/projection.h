#pragma once

#include <memory>
#include <string>
#include <vector>

#include <arrow/compute/exec.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>

#include "tiforth/expr.h"
#include "tiforth/operators.h"

namespace arrow {
class Array;
class MemoryPool;
class Schema;
}  // namespace arrow

namespace tiforth {

struct ProjectionExpr {
  std::string name;
  ExprPtr expr;
};

class ProjectionTransformOp final : public TransformOp {
 public:
  ProjectionTransformOp(std::vector<ProjectionExpr> exprs, arrow::MemoryPool* memory_pool = nullptr);

 protected:
  arrow::Result<OperatorStatus> TransformImpl(
      std::shared_ptr<arrow::RecordBatch>* batch) override;

 private:
  arrow::Result<std::shared_ptr<arrow::Schema>> ComputeOutputSchema(
      const std::vector<std::shared_ptr<arrow::Array>>& arrays) const;

  std::vector<ProjectionExpr> exprs_;
  std::shared_ptr<arrow::Schema> output_schema_;
  arrow::compute::ExecContext exec_context_;
};

}  // namespace tiforth
