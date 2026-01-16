#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

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

struct AggKey {
  std::string name;
  ExprPtr expr;
};

struct AggFunc {
  std::string name;
  std::string func;  // MS5: "count_all", "sum_int32"
  ExprPtr arg;       // unused for "count_all"
};

class HashAggTransformOp final : public TransformOp {
 public:
  HashAggTransformOp(std::vector<AggKey> keys, std::vector<AggFunc> aggs,
                     arrow::MemoryPool* memory_pool = nullptr);

 protected:
  arrow::Result<OperatorStatus> TransformImpl(
      std::shared_ptr<arrow::RecordBatch>* batch) override;

 private:
  struct Key {
    bool is_null = false;
    int32_t value = 0;
  };
  struct KeyHash {
    std::size_t operator()(const Key& key) const noexcept;
  };
  struct KeyEq {
    bool operator()(const Key& lhs, const Key& rhs) const noexcept;
  };

  struct AggState {
    enum class Kind {
      kUnsupported,
      kCountAll,
      kSumInt32,
    };

    std::string name;
    std::string func;
    Kind kind;
    ExprPtr arg;

    std::vector<uint64_t> count_all;
    std::vector<int64_t> sum_i64;
    std::vector<uint8_t> sum_has_value;
  };

  arrow::Status ConsumeBatch(const arrow::RecordBatch& input);
  arrow::Result<std::shared_ptr<arrow::RecordBatch>> FinalizeOutput();
  arrow::Result<std::shared_ptr<arrow::Schema>> BuildOutputSchema() const;

  uint32_t GetOrAddGroup(const Key& key);

  std::vector<AggKey> keys_;
  std::vector<AggState> aggs_;

  std::shared_ptr<arrow::Schema> output_schema_;

  arrow::MemoryPool* memory_pool_ = nullptr;

  bool finalized_ = false;
  bool eos_forwarded_ = false;

  std::unordered_map<Key, uint32_t, KeyHash, KeyEq> key_to_group_id_;
  std::vector<Key> group_keys_;
};

}  // namespace tiforth
