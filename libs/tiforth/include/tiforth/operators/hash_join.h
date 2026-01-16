#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <arrow/record_batch.h>
#include <arrow/result.h>

#include "tiforth/operators.h"

namespace arrow {
class Array;
class MemoryPool;
class Schema;
}  // namespace arrow

namespace tiforth {

struct JoinKey {
  std::string left;
  std::string right;
};

class HashJoinTransformOp final : public TransformOp {
 public:
  HashJoinTransformOp(std::vector<std::shared_ptr<arrow::RecordBatch>> build_batches, JoinKey key,
                      arrow::MemoryPool* memory_pool = nullptr);

 protected:
  arrow::Result<OperatorStatus> TransformImpl(
      std::shared_ptr<arrow::RecordBatch>* batch) override;

 private:
  struct BuildRowRef {
    std::size_t batch_index = 0;
    int64_t row = 0;
  };

  arrow::Status BuildIndex();
  arrow::Result<std::shared_ptr<arrow::Schema>> BuildOutputSchema(
      const std::shared_ptr<arrow::Schema>& left_schema) const;

  std::vector<std::shared_ptr<arrow::RecordBatch>> build_batches_;
  JoinKey key_;

  std::shared_ptr<arrow::Schema> build_schema_;
  std::shared_ptr<arrow::Schema> output_schema_;

  int build_key_index_ = -1;
  int probe_key_index_ = -1;

  std::unordered_map<int32_t, std::vector<BuildRowRef>> build_index_;
  bool index_built_ = false;

  arrow::MemoryPool* memory_pool_ = nullptr;
};

}  // namespace tiforth
