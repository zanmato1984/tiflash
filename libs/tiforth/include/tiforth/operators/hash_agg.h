#pragma once

#include <array>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <variant>
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
  HashAggTransformOp(const Engine* engine, std::vector<AggKey> keys, std::vector<AggFunc> aggs,
                     arrow::MemoryPool* memory_pool = nullptr);
  ~HashAggTransformOp() override;

 protected:
  arrow::Result<OperatorStatus> TransformImpl(
      std::shared_ptr<arrow::RecordBatch>* batch) override;

 private:
  using Decimal128Bytes = std::array<uint8_t, 16>;
  using Decimal256Bytes = std::array<uint8_t, 32>;
  using KeyValue = std::variant<int32_t, uint64_t, Decimal128Bytes, Decimal256Bytes, std::string>;

  struct KeyPart {
    bool is_null = false;
    KeyValue value;
  };

  struct NormalizedKey {
    uint8_t key_count = 0;
    std::array<KeyPart, 2> parts;
  };
  struct OutputKey {
    uint8_t key_count = 0;
    std::array<KeyPart, 2> parts;
  };
  struct KeyHash {
    std::size_t operator()(const NormalizedKey& key) const noexcept;
  };
  struct KeyEq {
    bool operator()(const NormalizedKey& lhs, const NormalizedKey& rhs) const noexcept;
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

  uint32_t GetOrAddGroup(const NormalizedKey& key, OutputKey output_key);

  struct Compiled;

  const Engine* engine_ = nullptr;
  std::vector<AggKey> keys_;
  std::vector<AggState> aggs_;

  std::shared_ptr<arrow::Schema> output_schema_;
  std::vector<std::shared_ptr<arrow::Field>> output_key_fields_;

  arrow::MemoryPool* memory_pool_ = nullptr;
  std::unique_ptr<Compiled> compiled_;
  arrow::compute::ExecContext exec_context_;

  bool finalized_ = false;
  bool eos_forwarded_ = false;

  std::unordered_map<NormalizedKey, uint32_t, KeyHash, KeyEq> key_to_group_id_;
  std::vector<OutputKey> group_keys_;
};

}  // namespace tiforth
