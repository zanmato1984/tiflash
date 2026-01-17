#include "tiforth/operators/hash_agg.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <optional>
#include <string_view>
#include <type_traits>
#include <utility>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/compute/exec.h>
#include <arrow/memory_pool.h>
#include <arrow/status.h>
#include <arrow/type.h>

#include "tiforth/compiled_expr.h"
#include "tiforth/engine.h"
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
  // FNV-1a style mixing over key parts (supports 1 or 2 keys).
  uint64_t h = 14695981039346656037ULL;
  h ^= static_cast<uint64_t>(key.key_count);
  h *= 1099511628211ULL;

  for (uint8_t i = 0; i < key.key_count && i < key.parts.size(); ++i) {
    const auto& part = key.parts[i];
    if (part.is_null) {
      h ^= 0x9e3779b97f4a7c15ULL;
      h *= 1099511628211ULL;
      continue;
    }
    const std::size_t part_hash = std::visit(
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
            return HashBytes(reinterpret_cast<const uint8_t*>(v.data()), v.size());
          } else {
            return 0;
          }
        },
        part.value);
    h ^= static_cast<uint64_t>(part_hash);
    h *= 1099511628211ULL;
  }

  return static_cast<std::size_t>(h);
}

bool HashAggTransformOp::KeyEq::operator()(const NormalizedKey& lhs,
                                           const NormalizedKey& rhs) const noexcept {
  if (lhs.key_count != rhs.key_count) {
    return false;
  }
  for (uint8_t i = 0; i < lhs.key_count && i < lhs.parts.size(); ++i) {
    const auto& l = lhs.parts[i];
    const auto& r = rhs.parts[i];
    if (l.is_null != r.is_null) {
      return false;
    }
    if (l.is_null) {
      continue;
    }
    if (l.value != r.value) {
      return false;
    }
  }
  return true;
}

struct HashAggTransformOp::Compiled {
  std::vector<CompiledExpr> key_exprs;
  std::vector<std::optional<CompiledExpr>> sum_arg_exprs;
};

HashAggTransformOp::~HashAggTransformOp() = default;

HashAggTransformOp::HashAggTransformOp(const Engine* engine, std::vector<AggKey> keys,
                                       std::vector<AggFunc> aggs, arrow::MemoryPool* memory_pool)
    : engine_(engine),
      keys_(std::move(keys)),
      memory_pool_(memory_pool != nullptr
                       ? memory_pool
                       : (engine != nullptr ? engine->memory_pool() : arrow::default_memory_pool())),
      exec_context_(memory_pool_, /*executor=*/nullptr,
                   engine != nullptr ? engine->function_registry() : nullptr) {
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
  if (engine_ == nullptr) {
    return arrow::Status::Invalid("hash agg engine must not be null");
  }
  const std::size_t key_count = keys_.size();
  if (key_count == 0 || key_count > 2) {
    return arrow::Status::NotImplemented("MS9 supports 1 or 2 group keys");
  }
  for (std::size_t i = 0; i < key_count; ++i) {
    if (keys_[i].name.empty()) {
      return arrow::Status::Invalid("group key name must not be empty");
    }
    if (keys_[i].expr == nullptr) {
      return arrow::Status::Invalid("group key expr must not be null");
    }
  }

  if (compiled_ == nullptr) {
    auto compiled = std::make_unique<Compiled>();
    compiled->key_exprs.reserve(key_count);
    for (std::size_t i = 0; i < key_count; ++i) {
      ARROW_ASSIGN_OR_RAISE(auto compiled_key,
                            CompileExpr(input.schema(), *keys_[i].expr, engine_, &exec_context_));
      compiled->key_exprs.push_back(std::move(compiled_key));
    }

    compiled->sum_arg_exprs.resize(aggs_.size());
    for (std::size_t i = 0; i < aggs_.size(); ++i) {
      const auto& agg = aggs_[i];
      if (agg.kind != AggState::Kind::kSumInt32) {
        continue;
      }
      if (agg.arg == nullptr) {
        return arrow::Status::Invalid("sum_int32 arg must not be null");
      }
      ARROW_ASSIGN_OR_RAISE(auto compiled_arg,
                            CompileExpr(input.schema(), *agg.arg, engine_, &exec_context_));
      compiled->sum_arg_exprs[i] = std::move(compiled_arg);
    }

    compiled_ = std::move(compiled);
  }
  if (compiled_ == nullptr || compiled_->key_exprs.size() != key_count) {
    return arrow::Status::Invalid("compiled hash agg key expr count mismatch");
  }

  std::array<std::shared_ptr<arrow::Array>, 2> key_arrays{};
  for (std::size_t i = 0; i < key_count; ++i) {
    ARROW_ASSIGN_OR_RAISE(key_arrays[i],
                          ExecuteExprAsArray(compiled_->key_exprs[i], input, &exec_context_));
    if (key_arrays[i] == nullptr) {
      return arrow::Status::Invalid("group key must not evaluate to null array");
    }
    if (key_arrays[i]->length() != input.num_rows()) {
      return arrow::Status::Invalid("group key length mismatch");
    }
  }

  if (output_key_fields_.empty()) {
    auto schema = input.schema();
    output_key_fields_.reserve(key_count);

    for (std::size_t i = 0; i < key_count; ++i) {
      const auto* field_ref = std::get_if<FieldRef>(&keys_[i].expr->node);
      if (schema != nullptr && field_ref != nullptr) {
        int field_index = field_ref->index;
        if (field_index < 0 && !field_ref->name.empty()) {
          field_index = schema->GetFieldIndex(field_ref->name);
        }
        if (field_index >= 0 && field_index < schema->num_fields()) {
          if (const auto& field = schema->field(field_index); field != nullptr) {
            output_key_fields_.push_back(field);
            continue;
          }
        }
      }
      // Fallback: use evaluated key type (metadata may be missing).
      output_key_fields_.push_back(
          arrow::field(keys_[i].name, key_arrays[i]->type(), /*nullable=*/true));
    }
  }
  if (output_key_fields_.size() != key_count) {
    return arrow::Status::Invalid("group key field count mismatch");
  }

  std::array<Collation, 2> collations{CollationFromId(63), CollationFromId(63)};
  for (std::size_t i = 0; i < key_count; ++i) {
    if (key_arrays[i]->type_id() != arrow::Type::BINARY) {
      continue;
    }
    int32_t collation_id = 63;
    if (output_key_fields_[i] != nullptr) {
      ARROW_ASSIGN_OR_RAISE(const auto logical_type, GetLogicalType(*output_key_fields_[i]));
      if (logical_type.id == LogicalTypeId::kString) {
        collation_id = logical_type.collation_id >= 0 ? logical_type.collation_id : 63;
      }
    }
    const auto collation = CollationFromId(collation_id);
    if (collation.kind == CollationKind::kUnsupported) {
      return arrow::Status::NotImplemented("unsupported collation id: ", collation_id);
    }
    collations[i] = collation;
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
    if (compiled_ == nullptr || i >= compiled_->sum_arg_exprs.size() ||
        !compiled_->sum_arg_exprs[i].has_value()) {
      return arrow::Status::Invalid("sum_int32 arg must not be null");
    }

    ARROW_ASSIGN_OR_RAISE(
        auto arg_any,
        ExecuteExprAsArray(*compiled_->sum_arg_exprs[i], input, &exec_context_));
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
    norm_key.key_count = static_cast<uint8_t>(key_count);
    OutputKey out_key;
    out_key.key_count = static_cast<uint8_t>(key_count);

    for (std::size_t ki = 0; ki < key_count; ++ki) {
      auto& norm_part = norm_key.parts[ki];
      auto& out_part = out_key.parts[ki];

      const auto& arr_any = key_arrays[ki];
      if (arr_any == nullptr) {
        return arrow::Status::Invalid("internal error: missing key array");
      }

      if (arr_any->IsNull(row)) {
        norm_part.is_null = true;
        out_part.is_null = true;
        continue;
      }

      norm_part.is_null = false;
      out_part.is_null = false;

      switch (arr_any->type_id()) {
        case arrow::Type::INT32: {
          const auto& arr = static_cast<const arrow::Int32Array&>(*arr_any);
          const int32_t v = arr.Value(row);
          norm_part.value = v;
          out_part.value = v;
          break;
        }
        case arrow::Type::UINT64: {
          const auto& arr = static_cast<const arrow::UInt64Array&>(*arr_any);
          const uint64_t v = arr.Value(row);
          norm_part.value = v;
          out_part.value = v;
          break;
        }
        case arrow::Type::DECIMAL128: {
          const auto& arr = static_cast<const arrow::FixedSizeBinaryArray&>(*arr_any);
          if (arr.byte_width() != static_cast<int>(Decimal128Bytes{}.size())) {
            return arrow::Status::Invalid("unexpected decimal128 byte width");
          }
          Decimal128Bytes bytes{};
          std::memcpy(bytes.data(), arr.GetValue(row), bytes.size());
          norm_part.value = bytes;
          out_part.value = bytes;
          break;
        }
        case arrow::Type::DECIMAL256: {
          const auto& arr = static_cast<const arrow::FixedSizeBinaryArray&>(*arr_any);
          if (arr.byte_width() != static_cast<int>(Decimal256Bytes{}.size())) {
            return arrow::Status::Invalid("unexpected decimal256 byte width");
          }
          Decimal256Bytes bytes{};
          std::memcpy(bytes.data(), arr.GetValue(row), bytes.size());
          norm_part.value = bytes;
          out_part.value = bytes;
          break;
        }
        case arrow::Type::BINARY: {
          const auto& arr = static_cast<const arrow::BinaryArray&>(*arr_any);
          std::string_view view = arr.GetView(row);
          out_part.value = std::string(view.data(), view.size());
          norm_part.value = SortKeyString(collations[ki], view);
          break;
        }
        default:
          return arrow::Status::NotImplemented("unsupported group key type: ",
                                               arr_any->type()->ToString());
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
  const std::size_t key_count = keys_.size();
  if (key_count == 0 || key_count > 2) {
    return arrow::Status::NotImplemented("MS9 supports 1 or 2 group keys");
  }
  if (output_key_fields_.size() != key_count) {
    return arrow::Status::Invalid("group key field count mismatch");
  }

  std::vector<std::shared_ptr<arrow::Field>> fields;
  fields.reserve(key_count + aggs_.size());
  for (std::size_t i = 0; i < key_count; ++i) {
    if (keys_[i].name.empty()) {
      return arrow::Status::Invalid("group key name must not be empty");
    }
    if (output_key_fields_[i] == nullptr) {
      return arrow::Status::Invalid("group key field must not be null");
    }
    fields.push_back(output_key_fields_[i]->WithName(keys_[i].name));
  }

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

  std::vector<std::shared_ptr<arrow::Array>> columns;
  const std::size_t key_count = keys_.size();
  columns.reserve(key_count + aggs_.size());

  if (output_key_fields_.size() != key_count) {
    return arrow::Status::Invalid("group key field count mismatch");
  }

  for (std::size_t ki = 0; ki < key_count; ++ki) {
    const auto& field = output_key_fields_[ki];
    if (field == nullptr) {
      return arrow::Status::Invalid("group key field must not be null");
    }

    std::shared_ptr<arrow::Array> out_key;
    switch (field->type()->id()) {
      case arrow::Type::INT32: {
        arrow::Int32Builder builder(memory_pool_);
        for (const auto& key : group_keys_) {
          if (key.key_count != key_count) {
            return arrow::Status::Invalid("group key arity mismatch");
          }
          const auto& part = key.parts[ki];
          if (part.is_null) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
          } else {
            ARROW_RETURN_NOT_OK(builder.Append(std::get<int32_t>(part.value)));
          }
        }
        ARROW_RETURN_NOT_OK(builder.Finish(&out_key));
        break;
      }
      case arrow::Type::UINT64: {
        arrow::UInt64Builder builder(memory_pool_);
        for (const auto& key : group_keys_) {
          if (key.key_count != key_count) {
            return arrow::Status::Invalid("group key arity mismatch");
          }
          const auto& part = key.parts[ki];
          if (part.is_null) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
          } else {
            ARROW_RETURN_NOT_OK(builder.Append(std::get<uint64_t>(part.value)));
          }
        }
        ARROW_RETURN_NOT_OK(builder.Finish(&out_key));
        break;
      }
      case arrow::Type::DECIMAL128: {
        arrow::Decimal128Builder builder(field->type(), memory_pool_);
        for (const auto& key : group_keys_) {
          if (key.key_count != key_count) {
            return arrow::Status::Invalid("group key arity mismatch");
          }
          const auto& part = key.parts[ki];
          if (part.is_null) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
          } else {
            const auto& bytes = std::get<Decimal128Bytes>(part.value);
            ARROW_RETURN_NOT_OK(builder.Append(std::string_view(
                reinterpret_cast<const char*>(bytes.data()), bytes.size())));
          }
        }
        ARROW_RETURN_NOT_OK(builder.Finish(&out_key));
        break;
      }
      case arrow::Type::DECIMAL256: {
        arrow::Decimal256Builder builder(field->type(), memory_pool_);
        for (const auto& key : group_keys_) {
          if (key.key_count != key_count) {
            return arrow::Status::Invalid("group key arity mismatch");
          }
          const auto& part = key.parts[ki];
          if (part.is_null) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
          } else {
            const auto& bytes = std::get<Decimal256Bytes>(part.value);
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
          if (key.key_count != key_count) {
            return arrow::Status::Invalid("group key arity mismatch");
          }
          const auto& part = key.parts[ki];
          if (part.is_null) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
          } else {
            const auto& bytes = std::get<std::string>(part.value);
            ARROW_RETURN_NOT_OK(builder.Append(reinterpret_cast<const uint8_t*>(bytes.data()),
                                               static_cast<int32_t>(bytes.size())));
          }
        }
        ARROW_RETURN_NOT_OK(builder.Finish(&out_key));
        break;
      }
      default:
        return arrow::Status::NotImplemented("unsupported group key output type: ",
                                             field->type()->ToString());
    }

    columns.push_back(std::move(out_key));
  }

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
