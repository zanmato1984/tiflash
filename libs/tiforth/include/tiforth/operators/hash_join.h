#pragma once

#include <array>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
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

class Engine;

struct JoinKey {
  std::vector<std::string> left;
  std::vector<std::string> right;
};

class HashJoinTransformOp final : public TransformOp {
 public:
  HashJoinTransformOp(const Engine* engine, std::vector<std::shared_ptr<arrow::RecordBatch>> build_batches,
                      JoinKey key, arrow::MemoryPool* memory_pool = nullptr);

 protected:
  arrow::Result<OperatorStatus> TransformImpl(
      std::shared_ptr<arrow::RecordBatch>* batch) override;

 private:
  using Decimal128Bytes = std::array<uint8_t, 16>;
  using Decimal256Bytes = std::array<uint8_t, 32>;
  using KeyValue = std::variant<int32_t, uint64_t, Decimal128Bytes, Decimal256Bytes, std::string>;

  struct CompositeKey {
    uint8_t key_count = 0;
    std::array<KeyValue, 2> parts;

    bool operator==(const CompositeKey& rhs) const noexcept {
      if (key_count != rhs.key_count) {
        return false;
      }
      for (uint8_t i = 0; i < key_count && i < parts.size(); ++i) {
        if (parts[i] != rhs.parts[i]) {
          return false;
        }
      }
      return true;
    }
  };

  struct KeyHash {
    std::size_t operator()(const CompositeKey& key) const noexcept;
  };

  arrow::Status BuildIndex();
  arrow::Result<std::shared_ptr<arrow::Schema>> BuildOutputSchema(
      const std::shared_ptr<arrow::Schema>& left_schema) const;

  std::vector<std::shared_ptr<arrow::RecordBatch>> build_batches_;
  JoinKey key_;

  std::shared_ptr<arrow::Schema> build_schema_;
  std::shared_ptr<arrow::RecordBatch> build_combined_;
  std::shared_ptr<arrow::Schema> output_schema_;

  uint8_t key_count_ = 0;
  std::array<int, 2> build_key_indices_ = {-1, -1};
  std::array<int, 2> probe_key_indices_ = {-1, -1};

  std::array<int32_t, 2> key_collation_ids_ = {-1, -1};

  std::unordered_map<CompositeKey, std::vector<int64_t>, KeyHash> build_index_;
  bool index_built_ = false;

  arrow::MemoryPool* memory_pool_ = nullptr;
  arrow::compute::ExecContext exec_context_;
};

}  // namespace tiforth
