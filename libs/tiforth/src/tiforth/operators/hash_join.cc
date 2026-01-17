#include "tiforth/operators/hash_join.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <arrow/array.h>
#include <arrow/array/concatenate.h>
#include <arrow/builder.h>
#include <arrow/compute/exec.h>
#include <arrow/memory_pool.h>
#include <arrow/compute/api_vector.h>
#include <arrow/status.h>
#include <arrow/type.h>

#include "tiforth/collation.h"
#include "tiforth/detail/arrow_compute.h"
#include "tiforth/type_metadata.h"

namespace tiforth {

HashJoinTransformOp::HashJoinTransformOp(std::vector<std::shared_ptr<arrow::RecordBatch>> build_batches,
                                         JoinKey key, arrow::MemoryPool* memory_pool)
    : build_batches_(std::move(build_batches)),
      key_(std::move(key)),
      memory_pool_(memory_pool != nullptr ? memory_pool : arrow::default_memory_pool()) {}

namespace {

std::size_t HashBytes(const uint8_t* data, std::size_t size) noexcept {
  // FNV-1a (64-bit); good enough for MS8 common path.
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

arrow::Result<std::shared_ptr<arrow::Array>> DatumToArray(const arrow::Datum& datum,
                                                         arrow::MemoryPool* pool) {
  if (datum.is_array()) {
    return datum.make_array();
  }
  if (datum.is_chunked_array()) {
    auto chunked = datum.chunked_array();
    if (chunked == nullptr) {
      return arrow::Status::Invalid("expected non-null chunked array datum");
    }
    if (chunked->num_chunks() == 1) {
      return chunked->chunk(0);
    }
    return arrow::Concatenate(chunked->chunks(), pool);
  }
  return arrow::Status::Invalid("expected array or chunked array result");
}

}  // namespace

std::size_t HashJoinTransformOp::KeyHash::operator()(
    const HashJoinTransformOp::CompositeKey& key) const noexcept {
  uint64_t h = 14695981039346656037ULL;
  h ^= static_cast<uint64_t>(key.key_count);
  h *= 1099511628211ULL;

  for (uint8_t i = 0; i < key.key_count && i < key.parts.size(); ++i) {
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
        key.parts[i]);
    h ^= static_cast<uint64_t>(part_hash);
    h *= 1099511628211ULL;
  }

  return static_cast<std::size_t>(h);
}

arrow::Status HashJoinTransformOp::BuildIndex() {
  if (index_built_) {
    return arrow::Status::OK();
  }

  if (build_batches_.empty()) {
    return arrow::Status::NotImplemented("MS6A requires non-empty build side batches");
  }

  build_schema_ = build_batches_[0]->schema();
  if (build_schema_ == nullptr) {
    return arrow::Status::Invalid("build schema must not be null");
  }

  if (key_.left.size() != key_.right.size()) {
    return arrow::Status::Invalid("join key arity mismatch between probe and build");
  }
  key_count_ = static_cast<uint8_t>(key_.right.size());
  if (key_count_ == 0 || key_count_ > 2) {
    return arrow::Status::NotImplemented("MS9 supports 1 or 2 join keys");
  }

  key_collation_ids_.fill(-1);

  for (uint8_t i = 0; i < key_count_; ++i) {
    if (key_.right[i].empty()) {
      return arrow::Status::Invalid("build join key name must not be empty");
    }
    const int idx = build_schema_->GetFieldIndex(key_.right[i]);
    if (idx < 0 || idx >= build_schema_->num_fields()) {
      return arrow::Status::Invalid("unknown build join key: ", key_.right[i]);
    }
    build_key_indices_[i] = idx;

    // Decode collation metadata if present; missing metadata defaults to BINARY behavior.
    if (const auto& field = build_schema_->field(idx); field != nullptr) {
      ARROW_ASSIGN_OR_RAISE(const auto logical_type, GetLogicalType(*field));
      if (logical_type.id == LogicalTypeId::kString) {
        const int32_t collation_id = logical_type.collation_id >= 0 ? logical_type.collation_id : 63;
        const auto collation = CollationFromId(collation_id);
        if (collation.kind == CollationKind::kUnsupported) {
          return arrow::Status::NotImplemented("unsupported collation id: ", collation_id);
        }
        key_collation_ids_[i] = collation_id;
      }
    }
  }

  int64_t total_rows = 0;
  for (std::size_t batch_index = 0; batch_index < build_batches_.size(); ++batch_index) {
    const auto& batch = build_batches_[batch_index];
    if (batch == nullptr) {
      return arrow::Status::Invalid("build batch must not be null");
    }
    if (!build_schema_->Equals(*batch->schema(), /*check_metadata=*/true)) {
      return arrow::Status::Invalid("build schema mismatch");
    }
    total_rows += batch->num_rows();
  }

  // Concatenate build side batches once (preserves batch order) so join output can use Take.
  std::vector<std::shared_ptr<arrow::Array>> combined_columns;
  combined_columns.reserve(static_cast<std::size_t>(build_schema_->num_fields()));
  for (int col = 0; col < build_schema_->num_fields(); ++col) {
    std::vector<std::shared_ptr<arrow::Array>> chunks;
    chunks.reserve(build_batches_.size());
    for (const auto& batch : build_batches_) {
      chunks.push_back(batch->column(col));
    }
    ARROW_ASSIGN_OR_RAISE(auto combined, arrow::Concatenate(chunks, memory_pool_));
    combined_columns.push_back(std::move(combined));
  }
  build_combined_ =
      arrow::RecordBatch::Make(build_schema_, total_rows, std::move(combined_columns));

  std::array<std::shared_ptr<arrow::Array>, 2> key_arrays{};
  for (uint8_t i = 0; i < key_count_; ++i) {
    key_arrays[i] = build_combined_->column(build_key_indices_[i]);
    if (key_arrays[i] == nullptr) {
      return arrow::Status::Invalid("build join key column must not be null");
    }
  }

  std::array<Collation, 2> collations{CollationFromId(63), CollationFromId(63)};
  for (uint8_t i = 0; i < key_count_; ++i) {
    if (key_collation_ids_[i] >= 0) {
      collations[i] = CollationFromId(key_collation_ids_[i]);
    }
  }

  const auto make_key_part = [&](const arrow::Array& array, int64_t row,
                                 Collation collation) -> arrow::Result<KeyValue> {
    switch (array.type_id()) {
      case arrow::Type::INT32:
        return static_cast<const arrow::Int32Array&>(array).Value(row);
      case arrow::Type::UINT64:
        return static_cast<const arrow::UInt64Array&>(array).Value(row);
      case arrow::Type::DECIMAL128: {
        const auto& fixed = static_cast<const arrow::FixedSizeBinaryArray&>(array);
        if (fixed.byte_width() != static_cast<int>(Decimal128Bytes{}.size())) {
          return arrow::Status::Invalid("unexpected decimal128 byte width");
        }
        Decimal128Bytes bytes{};
        std::memcpy(bytes.data(), fixed.GetValue(row), bytes.size());
        return bytes;
      }
      case arrow::Type::DECIMAL256: {
        const auto& fixed = static_cast<const arrow::FixedSizeBinaryArray&>(array);
        if (fixed.byte_width() != static_cast<int>(Decimal256Bytes{}.size())) {
          return arrow::Status::Invalid("unexpected decimal256 byte width");
        }
        Decimal256Bytes bytes{};
        std::memcpy(bytes.data(), fixed.GetValue(row), bytes.size());
        return bytes;
      }
      case arrow::Type::BINARY: {
        const auto& bin = static_cast<const arrow::BinaryArray&>(array);
        return SortKeyString(collation, bin.GetView(row));
      }
      default:
        break;
    }
    return arrow::Status::NotImplemented("unsupported join key type: ",
                                         array.type()->ToString());
  };

  // Build the hash index on the concatenated build side.
  const int64_t rows = build_combined_->num_rows();
  for (int64_t row = 0; row < rows; ++row) {
    bool any_null = false;
    for (uint8_t i = 0; i < key_count_; ++i) {
      if (key_arrays[i] != nullptr && key_arrays[i]->IsNull(row)) {
        any_null = true;
        break;
      }
    }
    if (any_null) {
      continue;
    }

    CompositeKey key;
    key.key_count = key_count_;
    for (uint8_t i = 0; i < key_count_; ++i) {
      ARROW_ASSIGN_OR_RAISE(key.parts[i], make_key_part(*key_arrays[i], row, collations[i]));
    }
    build_index_[key].push_back(row);
  }

  index_built_ = true;
  return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::Schema>> HashJoinTransformOp::BuildOutputSchema(
    const std::shared_ptr<arrow::Schema>& left_schema) const {
  if (left_schema == nullptr) {
    return arrow::Status::Invalid("left schema must not be null");
  }
  if (build_schema_ == nullptr) {
    return arrow::Status::Invalid("build schema must not be null");
  }

  std::vector<std::shared_ptr<arrow::Field>> fields;
  fields.reserve(static_cast<std::size_t>(left_schema->num_fields() + build_schema_->num_fields()));
  for (const auto& field : left_schema->fields()) {
    fields.push_back(field);
  }
  for (const auto& field : build_schema_->fields()) {
    fields.push_back(field);
  }
  return arrow::schema(std::move(fields));
}

arrow::Result<OperatorStatus> HashJoinTransformOp::TransformImpl(
    std::shared_ptr<arrow::RecordBatch>* batch) {
  if (*batch == nullptr) {
    return OperatorStatus::kHasOutput;
  }

  ARROW_RETURN_NOT_OK(BuildIndex());
  if (build_schema_ == nullptr || build_combined_ == nullptr) {
    return arrow::Status::Invalid("build side must be initialized before probing");
  }

  const auto& probe = **batch;
  const auto probe_schema = probe.schema();
  if (probe_schema == nullptr) {
    return arrow::Status::Invalid("probe schema must not be null");
  }

  if (output_schema_ == nullptr) {
    if (key_.left.size() != key_.right.size()) {
      return arrow::Status::Invalid("join key arity mismatch between probe and build");
    }
    if (key_count_ == 0) {
      key_count_ = static_cast<uint8_t>(key_.left.size());
    }
    if (key_count_ == 0 || key_count_ > 2) {
      return arrow::Status::NotImplemented("MS9 supports 1 or 2 join keys");
    }

    for (uint8_t i = 0; i < key_count_; ++i) {
      if (key_.left[i].empty()) {
        return arrow::Status::Invalid("probe join key name must not be empty");
      }
      const int idx = probe_schema->GetFieldIndex(key_.left[i]);
      if (idx < 0 || idx >= probe_schema->num_fields()) {
        return arrow::Status::Invalid("unknown probe join key: ", key_.left[i]);
      }
      probe_key_indices_[i] = idx;

      if (key_collation_ids_[i] >= 0) {
        if (const auto& field = probe_schema->field(idx); field != nullptr) {
          ARROW_ASSIGN_OR_RAISE(const auto logical_type, GetLogicalType(*field));
          if (logical_type.id != LogicalTypeId::kString) {
            return arrow::Status::NotImplemented("join key collation mismatch between build and probe");
          }
          const int32_t collation_id =
              logical_type.collation_id >= 0 ? logical_type.collation_id : 63;
          if (collation_id != key_collation_ids_[i]) {
            return arrow::Status::NotImplemented("join key collation mismatch between build and probe");
          }
        }
      }
    }
    ARROW_ASSIGN_OR_RAISE(output_schema_, BuildOutputSchema(probe_schema));
  } else {
    // MS6A assumes a stable probe schema across batches.
    if (probe_schema->num_fields() + build_schema_->num_fields() != output_schema_->num_fields()) {
      return arrow::Status::Invalid("hash join output schema field count mismatch");
    }
  }

  std::array<std::shared_ptr<arrow::Array>, 2> probe_key_arrays{};
  std::array<std::shared_ptr<arrow::Array>, 2> build_key_arrays{};
  for (uint8_t i = 0; i < key_count_; ++i) {
    probe_key_arrays[i] = probe.column(probe_key_indices_[i]);
    if (probe_key_arrays[i] == nullptr) {
      return arrow::Status::Invalid("probe join key column must not be null");
    }
    build_key_arrays[i] = build_combined_->column(build_key_indices_[i]);
    if (build_key_arrays[i] == nullptr) {
      return arrow::Status::Invalid("build join key column must not be null");
    }
    if (probe_key_arrays[i]->type_id() != build_key_arrays[i]->type_id()) {
      return arrow::Status::NotImplemented("join key type mismatch between build and probe");
    }
  }

  std::array<Collation, 2> collations{CollationFromId(63), CollationFromId(63)};
  for (uint8_t i = 0; i < key_count_; ++i) {
    if (key_collation_ids_[i] >= 0) {
      collations[i] = CollationFromId(key_collation_ids_[i]);
    }
  }

  const auto make_key_part = [&](const arrow::Array& array, int64_t row,
                                 Collation collation) -> arrow::Result<KeyValue> {
    switch (array.type_id()) {
      case arrow::Type::INT32:
        return static_cast<const arrow::Int32Array&>(array).Value(row);
      case arrow::Type::UINT64:
        return static_cast<const arrow::UInt64Array&>(array).Value(row);
      case arrow::Type::DECIMAL128: {
        const auto& fixed = static_cast<const arrow::FixedSizeBinaryArray&>(array);
        Decimal128Bytes bytes{};
        std::memcpy(bytes.data(), fixed.GetValue(row), bytes.size());
        return bytes;
      }
      case arrow::Type::DECIMAL256: {
        const auto& fixed = static_cast<const arrow::FixedSizeBinaryArray&>(array);
        Decimal256Bytes bytes{};
        std::memcpy(bytes.data(), fixed.GetValue(row), bytes.size());
        return bytes;
      }
      case arrow::Type::BINARY: {
        const auto& bin = static_cast<const arrow::BinaryArray&>(array);
        return SortKeyString(collation, bin.GetView(row));
      }
      default:
        break;
    }
    return arrow::Status::NotImplemented("unsupported join key type: ",
                                         array.type()->ToString());
  };

  arrow::UInt64Builder probe_indices(memory_pool_);
  arrow::UInt64Builder build_indices(memory_pool_);

  const int64_t probe_rows = probe.num_rows();
  for (int64_t row = 0; row < probe_rows; ++row) {
    bool any_null = false;
    for (uint8_t i = 0; i < key_count_; ++i) {
      if (probe_key_arrays[i] != nullptr && probe_key_arrays[i]->IsNull(row)) {
        any_null = true;
        break;
      }
    }
    if (any_null) {
      continue;
    }

    CompositeKey key;
    key.key_count = key_count_;
    for (uint8_t i = 0; i < key_count_; ++i) {
      ARROW_ASSIGN_OR_RAISE(key.parts[i],
                            make_key_part(*probe_key_arrays[i], row, collations[i]));
    }
    auto it = build_index_.find(key);
    if (it == build_index_.end()) {
      continue;
    }
    for (const auto build_row : it->second) {
      ARROW_RETURN_NOT_OK(probe_indices.Append(static_cast<uint64_t>(row)));
      ARROW_RETURN_NOT_OK(build_indices.Append(static_cast<uint64_t>(build_row)));
    }
  }

  std::shared_ptr<arrow::Array> probe_indices_array;
  std::shared_ptr<arrow::Array> build_indices_array;
  ARROW_RETURN_NOT_OK(probe_indices.Finish(&probe_indices_array));
  ARROW_RETURN_NOT_OK(build_indices.Finish(&build_indices_array));

  ARROW_RETURN_NOT_OK(detail::EnsureArrowComputeInitialized());
  arrow::compute::ExecContext exec_context(memory_pool_);
  const auto take_options = arrow::compute::TakeOptions::NoBoundsCheck();

  std::vector<std::shared_ptr<arrow::Array>> out_arrays;
  out_arrays.reserve(static_cast<std::size_t>(probe.num_columns() + build_schema_->num_fields()));

  for (int ci = 0; ci < probe.num_columns(); ++ci) {
    ARROW_ASSIGN_OR_RAISE(
        auto taken,
        arrow::compute::Take(arrow::Datum(probe.column(ci)), arrow::Datum(probe_indices_array),
                             take_options, &exec_context));
    ARROW_ASSIGN_OR_RAISE(auto out_array, DatumToArray(taken, exec_context.memory_pool()));
    out_arrays.push_back(std::move(out_array));
  }

  for (int ci = 0; ci < build_schema_->num_fields(); ++ci) {
    ARROW_ASSIGN_OR_RAISE(
        auto taken,
        arrow::compute::Take(arrow::Datum(build_combined_->column(ci)),
                             arrow::Datum(build_indices_array), take_options, &exec_context));
    ARROW_ASSIGN_OR_RAISE(auto out_array, DatumToArray(taken, exec_context.memory_pool()));
    out_arrays.push_back(std::move(out_array));
  }

  const int64_t out_rows = probe_indices_array->length();
  *batch = arrow::RecordBatch::Make(output_schema_, out_rows, std::move(out_arrays));
  return OperatorStatus::kHasOutput;
}

}  // namespace tiforth
