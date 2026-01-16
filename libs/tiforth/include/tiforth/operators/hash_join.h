#pragma once

#include <array>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
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
  using Decimal128Bytes = std::array<uint8_t, 16>;
  using Decimal256Bytes = std::array<uint8_t, 32>;
  using KeyValue = std::variant<int32_t, uint64_t, Decimal128Bytes, Decimal256Bytes, std::string>;
  struct KeyHash {
    std::size_t operator()(const KeyValue& key) const noexcept;
  };

  arrow::Status BuildIndex();
  arrow::Result<std::shared_ptr<arrow::Schema>> BuildOutputSchema(
      const std::shared_ptr<arrow::Schema>& left_schema) const;

  std::vector<std::shared_ptr<arrow::RecordBatch>> build_batches_;
  JoinKey key_;

  std::shared_ptr<arrow::Schema> build_schema_;
  std::shared_ptr<arrow::RecordBatch> build_combined_;
  std::shared_ptr<arrow::Schema> output_schema_;

  int build_key_index_ = -1;
  int probe_key_index_ = -1;

  int32_t key_collation_id_ = -1;

  std::unordered_map<KeyValue, std::vector<int64_t>, KeyHash> build_index_;
  bool index_built_ = false;

  arrow::MemoryPool* memory_pool_ = nullptr;
};

}  // namespace tiforth
