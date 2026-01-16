#pragma once

#include <memory>

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

class FilterTransformOp final : public TransformOp {
 public:
  explicit FilterTransformOp(ExprPtr predicate, arrow::MemoryPool* memory_pool = nullptr);

 protected:
  arrow::Result<OperatorStatus> TransformImpl(
      std::shared_ptr<arrow::RecordBatch>* batch) override;

 private:
  ExprPtr predicate_;
  std::shared_ptr<arrow::Schema> output_schema_;
  arrow::compute::ExecContext exec_context_;
};

}  // namespace tiforth
