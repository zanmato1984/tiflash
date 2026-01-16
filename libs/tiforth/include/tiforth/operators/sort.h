#pragma once

#include <memory>
#include <string>
#include <vector>

#include <arrow/compute/exec.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>

#include "tiforth/operators.h"

namespace arrow {
class Array;
class MemoryPool;
class Schema;
}  // namespace arrow

namespace tiforth {

struct SortKey {
  std::string name;
  bool ascending = true;
  bool nulls_first = false;
};

class SortTransformOp final : public TransformOp {
 public:
  explicit SortTransformOp(std::vector<SortKey> keys, arrow::MemoryPool* memory_pool = nullptr);

 protected:
  arrow::Result<OperatorStatus> TransformImpl(
      std::shared_ptr<arrow::RecordBatch>* batch) override;

 private:
  arrow::Result<std::shared_ptr<arrow::RecordBatch>> SortAll();

  std::vector<SortKey> keys_;
  std::shared_ptr<arrow::Schema> output_schema_;
  std::vector<std::shared_ptr<arrow::RecordBatch>> buffered_;

  bool output_emitted_ = false;
  bool eos_forwarded_ = false;

  arrow::compute::ExecContext exec_context_;
};

}  // namespace tiforth
