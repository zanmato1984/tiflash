#include "tiforth/operators/hash_agg.h"

#include <functional>
#include <utility>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/status.h>
#include <arrow/type.h>

namespace tiforth {

std::size_t HashAggTransformOp::KeyHash::operator()(const Key& key) const noexcept {
  if (key.is_null) {
    return 0x9e3779b97f4a7c15ULL;
  }
  return std::hash<int32_t>{}(key.value);
}

bool HashAggTransformOp::KeyEq::operator()(const Key& lhs, const Key& rhs) const noexcept {
  if (lhs.is_null != rhs.is_null) {
    return false;
  }
  if (lhs.is_null) {
    return true;
  }
  return lhs.value == rhs.value;
}

HashAggTransformOp::HashAggTransformOp(std::vector<AggKey> keys, std::vector<AggFunc> aggs)
    : keys_(std::move(keys)) {
  aggs_.reserve(aggs.size());
  for (auto& agg : aggs) {
    if (agg.func == "count_all") {
      aggs_.push_back(AggState{.name = std::move(agg.name),
                               .func = "count_all",
                               .kind = AggState::Kind::kCountAll,
                               .arg = nullptr});
    } else if (agg.func == "sum_int32") {
      aggs_.push_back(AggState{.name = std::move(agg.name),
                               .func = "sum_int32",
                               .kind = AggState::Kind::kSumInt32,
                               .arg = std::move(agg.arg)});
    } else {
      aggs_.push_back(AggState{.name = std::move(agg.name),
                               .func = std::move(agg.func),
                               .kind = AggState::Kind::kUnsupported,
                               .arg = std::move(agg.arg)});
    }
  }
}

uint32_t HashAggTransformOp::GetOrAddGroup(const Key& key) {
  auto it = key_to_group_id_.find(key);
  if (it != key_to_group_id_.end()) {
    return it->second;
  }

  const uint32_t group_id = static_cast<uint32_t>(group_keys_.size());
  key_to_group_id_.emplace(key, group_id);
  group_keys_.push_back(key);
  for (auto& agg : aggs_) {
    switch (agg.kind) {
      case AggState::Kind::kUnsupported:
        break;
      case AggState::Kind::kCountAll:
        agg.count_all.push_back(0);
        break;
      case AggState::Kind::kSumInt32:
        agg.sum_i64.push_back(0);
        agg.sum_has_value.push_back(0);
        break;
    }
  }
  return group_id;
}

arrow::Status HashAggTransformOp::ConsumeBatch(const arrow::RecordBatch& input) {
  if (keys_.size() != 1) {
    return arrow::Status::NotImplemented("MS5 supports exactly one group key");
  }
  if (keys_[0].expr == nullptr) {
    return arrow::Status::Invalid("group key expr must not be null");
  }

  ARROW_ASSIGN_OR_RAISE(auto key_array_any, EvalExprAsArray(input, *keys_[0].expr, nullptr));
  if (key_array_any == nullptr) {
    return arrow::Status::Invalid("group key must not evaluate to null array");
  }
  if (key_array_any->length() != input.num_rows()) {
    return arrow::Status::Invalid("group key length mismatch");
  }
  if (key_array_any->type_id() != arrow::Type::INT32) {
    return arrow::Status::NotImplemented("MS5 group key type must be int32");
  }
  const auto key_array = std::static_pointer_cast<arrow::Int32Array>(key_array_any);

  std::vector<std::shared_ptr<arrow::Int32Array>> sum_args(aggs_.size());
  for (std::size_t i = 0; i < aggs_.size(); ++i) {
    auto& agg = aggs_[i];
    if (agg.kind != AggState::Kind::kSumInt32) {
      if (agg.kind == AggState::Kind::kUnsupported) {
        return arrow::Status::NotImplemented("unsupported aggregate func: ", agg.func);
      }
      continue;
    }
    if (agg.arg == nullptr) {
      return arrow::Status::Invalid("sum_int32 arg must not be null");
    }

    ARROW_ASSIGN_OR_RAISE(auto arg_any, EvalExprAsArray(input, *agg.arg, nullptr));
    if (arg_any == nullptr) {
      return arrow::Status::Invalid("sum_int32 arg must not evaluate to null array");
    }
    if (arg_any->length() != input.num_rows()) {
      return arrow::Status::Invalid("sum_int32 arg length mismatch");
    }
    if (arg_any->type_id() != arrow::Type::INT32) {
      return arrow::Status::NotImplemented("MS5 sum_int32 arg type must be int32");
    }
    sum_args[i] = std::static_pointer_cast<arrow::Int32Array>(arg_any);
  }

  const int64_t rows = input.num_rows();
  for (int64_t row = 0; row < rows; ++row) {
    Key key;
    if (key_array->IsNull(row)) {
      key.is_null = true;
    } else {
      key.is_null = false;
      key.value = key_array->Value(row);
    }
    const uint32_t group_id = GetOrAddGroup(key);

    for (std::size_t i = 0; i < aggs_.size(); ++i) {
      auto& agg = aggs_[i];
      switch (agg.kind) {
        case AggState::Kind::kUnsupported:
          return arrow::Status::NotImplemented("unsupported aggregate func: ", agg.func);
        case AggState::Kind::kCountAll:
          ++agg.count_all[group_id];
          break;
        case AggState::Kind::kSumInt32: {
          const auto& arg = sum_args[i];
          if (arg == nullptr) {
            return arrow::Status::Invalid("internal error: missing sum_int32 arg array");
          }
          if (!arg->IsNull(row)) {
            agg.sum_i64[group_id] += static_cast<int64_t>(arg->Value(row));
            agg.sum_has_value[group_id] = 1;
          }
          break;
        }
      }
    }
  }

  return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::Schema>> HashAggTransformOp::BuildOutputSchema() const {
  if (keys_.size() != 1) {
    return arrow::Status::NotImplemented("MS5 supports exactly one group key");
  }
  if (keys_[0].name.empty()) {
    return arrow::Status::Invalid("group key name must not be empty");
  }

  std::vector<std::shared_ptr<arrow::Field>> fields;
  fields.reserve(1 + aggs_.size());
  fields.push_back(arrow::field(keys_[0].name, arrow::int32(), /*nullable=*/true));

  for (const auto& agg : aggs_) {
    if (agg.name.empty()) {
      return arrow::Status::Invalid("agg output name must not be empty");
    }
    switch (agg.kind) {
      case AggState::Kind::kUnsupported:
        return arrow::Status::NotImplemented("unsupported aggregate func: ", agg.func);
      case AggState::Kind::kCountAll:
        fields.push_back(arrow::field(agg.name, arrow::uint64(), /*nullable=*/false));
        break;
      case AggState::Kind::kSumInt32:
        fields.push_back(arrow::field(agg.name, arrow::int64(), /*nullable=*/true));
        break;
    }
  }

  return arrow::schema(std::move(fields));
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> HashAggTransformOp::FinalizeOutput() {
  if (output_schema_ == nullptr) {
    ARROW_ASSIGN_OR_RAISE(output_schema_, BuildOutputSchema());
  }

  arrow::Int32Builder key_builder;
  for (const auto& key : group_keys_) {
    if (key.is_null) {
      ARROW_RETURN_NOT_OK(key_builder.AppendNull());
    } else {
      ARROW_RETURN_NOT_OK(key_builder.Append(key.value));
    }
  }
  std::shared_ptr<arrow::Array> out_key;
  ARROW_RETURN_NOT_OK(key_builder.Finish(&out_key));

  std::vector<std::shared_ptr<arrow::Array>> columns;
  columns.reserve(1 + aggs_.size());
  columns.push_back(std::move(out_key));

  for (const auto& agg : aggs_) {
    switch (agg.kind) {
      case AggState::Kind::kUnsupported:
        return arrow::Status::NotImplemented("unsupported aggregate func: ", agg.func);
      case AggState::Kind::kCountAll: {
        arrow::UInt64Builder builder;
        ARROW_RETURN_NOT_OK(builder.AppendValues(agg.count_all));
        std::shared_ptr<arrow::Array> out;
        ARROW_RETURN_NOT_OK(builder.Finish(&out));
        columns.push_back(std::move(out));
        break;
      }
      case AggState::Kind::kSumInt32: {
        arrow::Int64Builder builder;
        for (std::size_t i = 0; i < agg.sum_i64.size(); ++i) {
          if (i >= agg.sum_has_value.size() || agg.sum_has_value[i] == 0) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
          } else {
            ARROW_RETURN_NOT_OK(builder.Append(agg.sum_i64[i]));
          }
        }
        std::shared_ptr<arrow::Array> out;
        ARROW_RETURN_NOT_OK(builder.Finish(&out));
        columns.push_back(std::move(out));
        break;
      }
    }
  }

  return arrow::RecordBatch::Make(output_schema_, static_cast<int64_t>(group_keys_.size()),
                                 std::move(columns));
}

arrow::Result<OperatorStatus> HashAggTransformOp::TransformImpl(
    std::shared_ptr<arrow::RecordBatch>* batch) {
  if (*batch == nullptr) {
    if (!finalized_) {
      ARROW_ASSIGN_OR_RAISE(*batch, FinalizeOutput());
      finalized_ = true;
      return OperatorStatus::kHasOutput;
    }
    if (!eos_forwarded_) {
      eos_forwarded_ = true;
      batch->reset();
      return OperatorStatus::kHasOutput;
    }
    batch->reset();
    return OperatorStatus::kHasOutput;
  }

  if (finalized_) {
    return arrow::Status::Invalid("hash agg received input after finalization");
  }

  const auto& input = **batch;
  ARROW_RETURN_NOT_OK(ConsumeBatch(input));
  batch->reset();
  return OperatorStatus::kNeedInput;
}

}  // namespace tiforth
