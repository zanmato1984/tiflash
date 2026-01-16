#include "tiforth/operators/hash_agg.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <string_view>
#include <type_traits>
#include <utility>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/compute/exec.h>
#include <arrow/memory_pool.h>
#include <arrow/status.h>
#include <arrow/type.h>

#include "tiforth/collation.h"
#include "tiforth/type_metadata.h"

namespace tiforth {

namespace {

std::size_t HashBytes(const uint8_t* data, std::size_t size) noexcept {
  uint64_t h = 14695981039346656037ULL;
  for (std::size_t i = 0; i < size; ++i) {
    h ^= static_cast<uint64_t>(data[i]);
    h *= 1099511628211ULL;
  }
  return static_cast<std::size_t>(h);
}

template <std::size_t N>
std::size_t HashBytes(const std::array<uint8_t, N>& bytes) noexcept {
  return HashBytes(bytes.data(), bytes.size());
}

}  // namespace

std::size_t HashAggTransformOp::KeyHash::operator()(const NormalizedKey& key) const noexcept {
  if (key.is_null) {
    return 0x9e3779b97f4a7c15ULL;
  }
  return std::visit(
      [&](const auto& v) -> std::size_t {
        using V = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<V, int32_t>) {
          return std::hash<int32_t>{}(v);
        } else if constexpr (std::is_same_v<V, uint64_t>) {
          return std::hash<uint64_t>{}(v);
        } else if constexpr (std::is_same_v<V, Decimal128Bytes> ||
                             std::is_same_v<V, Decimal256Bytes>) {
          return HashBytes(v);
        } else if constexpr (std::is_same_v<V, std::string>) {
          return std::hash<std::string>{}(v);
        } else {
          return 0;
        }
      },
      key.value);
}

bool HashAggTransformOp::KeyEq::operator()(const NormalizedKey& lhs,
                                           const NormalizedKey& rhs) const noexcept {
  if (lhs.is_null != rhs.is_null) {
    return false;
  }
  if (lhs.is_null) {
    return true;
  }
  return lhs.value == rhs.value;
}

HashAggTransformOp::HashAggTransformOp(std::vector<AggKey> keys, std::vector<AggFunc> aggs,
                                       arrow::MemoryPool* memory_pool)
    : keys_(std::move(keys)),
      memory_pool_(memory_pool != nullptr ? memory_pool : arrow::default_memory_pool()) {
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

uint32_t HashAggTransformOp::GetOrAddGroup(const NormalizedKey& key, OutputKey output_key) {
  auto it = key_to_group_id_.find(key);
  if (it != key_to_group_id_.end()) {
    return it->second;
  }

  const uint32_t group_id = static_cast<uint32_t>(group_keys_.size());
  key_to_group_id_.emplace(key, group_id);
  group_keys_.push_back(std::move(output_key));
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

  arrow::compute::ExecContext exec_context(memory_pool_);
  ARROW_ASSIGN_OR_RAISE(auto key_array_any,
                        EvalExprAsArray(input, *keys_[0].expr, &exec_context));
  if (key_array_any == nullptr) {
    return arrow::Status::Invalid("group key must not evaluate to null array");
  }
  if (key_array_any->length() != input.num_rows()) {
    return arrow::Status::Invalid("group key length mismatch");
  }

  if (output_key_field_ == nullptr) {
    auto schema = input.schema();
    const auto* field_ref = std::get_if<FieldRef>(&keys_[0].expr->node);
    if (schema != nullptr && field_ref != nullptr) {
      int field_index = field_ref->index;
      if (field_index < 0 && !field_ref->name.empty()) {
        field_index = schema->GetFieldIndex(field_ref->name);
      }
      if (field_index >= 0 && field_index < schema->num_fields()) {
        if (const auto& field = schema->field(field_index); field != nullptr) {
          output_key_field_ = field->WithName(keys_[0].name);
        }
      }
    }
    if (output_key_field_ == nullptr) {
      // Fallback: use evaluated key type (metadata may be missing).
      output_key_field_ = arrow::field(keys_[0].name, key_array_any->type(), /*nullable=*/true);
    }
  }

  int32_t collation_id = -1;
  Collation collation{.id = -1, .kind = CollationKind::kUnsupported};
  if (key_array_any->type_id() == arrow::Type::BINARY) {
    if (output_key_field_ != nullptr) {
      ARROW_ASSIGN_OR_RAISE(const auto logical_type, GetLogicalType(*output_key_field_));
      if (logical_type.id == LogicalTypeId::kString) {
        collation_id = logical_type.collation_id >= 0 ? logical_type.collation_id : 63;
      }
    }
    // Default to BINARY semantics even if metadata is missing.
    if (collation_id < 0) {
      collation_id = 63;
    }
    collation = CollationFromId(collation_id);
    if (collation.kind == CollationKind::kUnsupported) {
      return arrow::Status::NotImplemented("unsupported collation id: ", collation_id);
    }
  }

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

    ARROW_ASSIGN_OR_RAISE(auto arg_any, EvalExprAsArray(input, *agg.arg, &exec_context));
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
    NormalizedKey norm_key;
    OutputKey out_key;
    if (key_array_any->IsNull(row)) {
      norm_key.is_null = true;
      out_key.is_null = true;
    } else {
      norm_key.is_null = false;
      out_key.is_null = false;

      switch (key_array_any->type_id()) {
        case arrow::Type::INT32: {
          const auto& arr = static_cast<const arrow::Int32Array&>(*key_array_any);
          const int32_t v = arr.Value(row);
          norm_key.value = v;
          out_key.value = v;
          break;
        }
        case arrow::Type::UINT64: {
          const auto& arr = static_cast<const arrow::UInt64Array&>(*key_array_any);
          const uint64_t v = arr.Value(row);
          norm_key.value = v;
          out_key.value = v;
          break;
        }
        case arrow::Type::DECIMAL128: {
          const auto& arr = static_cast<const arrow::FixedSizeBinaryArray&>(*key_array_any);
          Decimal128Bytes bytes{};
          std::memcpy(bytes.data(), arr.GetValue(row), bytes.size());
          norm_key.value = bytes;
          out_key.value = bytes;
          break;
        }
        case arrow::Type::DECIMAL256: {
          const auto& arr = static_cast<const arrow::FixedSizeBinaryArray&>(*key_array_any);
          Decimal256Bytes bytes{};
          std::memcpy(bytes.data(), arr.GetValue(row), bytes.size());
          norm_key.value = bytes;
          out_key.value = bytes;
          break;
        }
        case arrow::Type::BINARY: {
          const auto& arr = static_cast<const arrow::BinaryArray&>(*key_array_any);
          std::string_view view = arr.GetView(row);
          out_key.value = std::string(view.data(), view.size());
          if (collation.kind == CollationKind::kPaddingBinary) {
            view = RightTrimAsciiSpace(view);
          }
          norm_key.value = std::string(view.data(), view.size());
          break;
        }
        default:
          return arrow::Status::NotImplemented("unsupported group key type: ",
                                               key_array_any->type()->ToString());
      }
    }
    const uint32_t group_id = GetOrAddGroup(norm_key, std::move(out_key));

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
  if (output_key_field_ == nullptr) {
    return arrow::Status::Invalid("group key field must not be null");
  }

  std::vector<std::shared_ptr<arrow::Field>> fields;
  fields.reserve(1 + aggs_.size());
  fields.push_back(output_key_field_->WithName(keys_[0].name));

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

  if (output_key_field_ == nullptr) {
    return arrow::Status::Invalid("group key field must not be null");
  }

  std::shared_ptr<arrow::Array> out_key;
  switch (output_key_field_->type()->id()) {
    case arrow::Type::INT32: {
      arrow::Int32Builder builder(memory_pool_);
      for (const auto& key : group_keys_) {
        if (key.is_null) {
          ARROW_RETURN_NOT_OK(builder.AppendNull());
        } else {
          ARROW_RETURN_NOT_OK(builder.Append(std::get<int32_t>(key.value)));
        }
      }
      ARROW_RETURN_NOT_OK(builder.Finish(&out_key));
      break;
    }
    case arrow::Type::UINT64: {
      arrow::UInt64Builder builder(memory_pool_);
      for (const auto& key : group_keys_) {
        if (key.is_null) {
          ARROW_RETURN_NOT_OK(builder.AppendNull());
        } else {
          ARROW_RETURN_NOT_OK(builder.Append(std::get<uint64_t>(key.value)));
        }
      }
      ARROW_RETURN_NOT_OK(builder.Finish(&out_key));
      break;
    }
    case arrow::Type::DECIMAL128: {
      arrow::Decimal128Builder builder(output_key_field_->type(), memory_pool_);
      for (const auto& key : group_keys_) {
        if (key.is_null) {
          ARROW_RETURN_NOT_OK(builder.AppendNull());
        } else {
          const auto& bytes = std::get<Decimal128Bytes>(key.value);
          ARROW_RETURN_NOT_OK(builder.Append(std::string_view(
              reinterpret_cast<const char*>(bytes.data()), bytes.size())));
        }
      }
      ARROW_RETURN_NOT_OK(builder.Finish(&out_key));
      break;
    }
    case arrow::Type::DECIMAL256: {
      arrow::Decimal256Builder builder(output_key_field_->type(), memory_pool_);
      for (const auto& key : group_keys_) {
        if (key.is_null) {
          ARROW_RETURN_NOT_OK(builder.AppendNull());
        } else {
          const auto& bytes = std::get<Decimal256Bytes>(key.value);
          ARROW_RETURN_NOT_OK(builder.Append(std::string_view(
              reinterpret_cast<const char*>(bytes.data()), bytes.size())));
        }
      }
      ARROW_RETURN_NOT_OK(builder.Finish(&out_key));
      break;
    }
    case arrow::Type::BINARY: {
      arrow::BinaryBuilder builder(memory_pool_);
      for (const auto& key : group_keys_) {
        if (key.is_null) {
          ARROW_RETURN_NOT_OK(builder.AppendNull());
        } else {
          const auto& bytes = std::get<std::string>(key.value);
          ARROW_RETURN_NOT_OK(
              builder.Append(reinterpret_cast<const uint8_t*>(bytes.data()),
                             static_cast<int32_t>(bytes.size())));
        }
      }
      ARROW_RETURN_NOT_OK(builder.Finish(&out_key));
      break;
    }
    default:
      return arrow::Status::NotImplemented("unsupported group key output type: ",
                                           output_key_field_->type()->ToString());
  }

  std::vector<std::shared_ptr<arrow::Array>> columns;
  columns.reserve(1 + aggs_.size());
  columns.push_back(std::move(out_key));

  for (const auto& agg : aggs_) {
    switch (agg.kind) {
      case AggState::Kind::kUnsupported:
        return arrow::Status::NotImplemented("unsupported aggregate func: ", agg.func);
      case AggState::Kind::kCountAll: {
        arrow::UInt64Builder builder(memory_pool_);
        ARROW_RETURN_NOT_OK(builder.AppendValues(agg.count_all));
        std::shared_ptr<arrow::Array> out;
        ARROW_RETURN_NOT_OK(builder.Finish(&out));
        columns.push_back(std::move(out));
        break;
      }
      case AggState::Kind::kSumInt32: {
        arrow::Int64Builder builder(memory_pool_);
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
