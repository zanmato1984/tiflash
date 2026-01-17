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
  FilterTransformOp(const Engine* engine, ExprPtr predicate, arrow::MemoryPool* memory_pool = nullptr);
  ~FilterTransformOp() override;

 protected:
  arrow::Result<OperatorStatus> TransformImpl(
      std::shared_ptr<arrow::RecordBatch>* batch) override;

 private:
  struct Compiled;

  const Engine* engine_ = nullptr;
  ExprPtr predicate_;
  std::shared_ptr<arrow::Schema> output_schema_;
  std::unique_ptr<Compiled> compiled_;
  arrow::compute::ExecContext exec_context_;
};

}  // namespace tiforth
