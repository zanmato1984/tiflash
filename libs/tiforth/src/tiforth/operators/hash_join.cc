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
    const HashJoinTransformOp::KeyValue& key) const noexcept {
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
      key);
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

  build_key_index_ = build_schema_->GetFieldIndex(key_.right);
  if (build_key_index_ < 0 || build_key_index_ >= build_schema_->num_fields()) {
    return arrow::Status::Invalid("unknown build join key: ", key_.right);
  }

  // Decode collation metadata if present; missing metadata defaults to BINARY behavior.
  key_collation_id_ = -1;
  if (const auto& field = build_schema_->field(build_key_index_); field != nullptr) {
    ARROW_ASSIGN_OR_RAISE(const auto logical_type, GetLogicalType(*field));
    if (logical_type.id == LogicalTypeId::kString) {
      key_collation_id_ = logical_type.collation_id >= 0 ? logical_type.collation_id : 63;
      const auto collation = CollationFromId(key_collation_id_);
      if (collation.kind == CollationKind::kUnsupported) {
        return arrow::Status::NotImplemented("unsupported collation id: ", key_collation_id_);
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

  const auto key_array_any = build_combined_->column(build_key_index_);
  if (key_array_any == nullptr) {
    return arrow::Status::Invalid("build join key column must not be null");
  }

  const auto make_key = [&](const arrow::Array& array, int64_t row) -> arrow::Result<KeyValue> {
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
        std::string_view view = bin.GetView(row);
        if (key_collation_id_ >= 0) {
          const auto collation = CollationFromId(key_collation_id_);
          if (collation.kind == CollationKind::kPaddingBinary) {
            view = RightTrimAsciiSpace(view);
          }
        }
        return std::string(view.data(), view.size());
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
    if (key_array_any->IsNull(row)) {
      continue;
    }
    ARROW_ASSIGN_OR_RAISE(const auto key, make_key(*key_array_any, row));
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
    probe_key_index_ = probe_schema->GetFieldIndex(key_.left);
    if (probe_key_index_ < 0 || probe_key_index_ >= probe_schema->num_fields()) {
      return arrow::Status::Invalid("unknown probe join key: ", key_.left);
    }
    ARROW_ASSIGN_OR_RAISE(output_schema_, BuildOutputSchema(probe_schema));
  } else {
    // MS6A assumes a stable probe schema across batches.
    if (probe_schema->num_fields() + build_schema_->num_fields() != output_schema_->num_fields()) {
      return arrow::Status::Invalid("hash join output schema field count mismatch");
    }
  }

  const auto probe_key_any = probe.column(probe_key_index_);
  if (probe_key_any == nullptr) {
    return arrow::Status::Invalid("probe join key column must not be null");
  }

  if (build_combined_->column(build_key_index_) == nullptr) {
    return arrow::Status::Invalid("build join key column must not be null");
  }
  if (probe_key_any->type_id() != build_combined_->column(build_key_index_)->type_id()) {
    return arrow::Status::NotImplemented("join key type mismatch between build and probe");
  }

  const auto make_key = [&](const arrow::Array& array, int64_t row) -> arrow::Result<KeyValue> {
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
        std::string_view view = bin.GetView(row);
        if (key_collation_id_ >= 0) {
          const auto collation = CollationFromId(key_collation_id_);
          if (collation.kind == CollationKind::kPaddingBinary) {
            view = RightTrimAsciiSpace(view);
          }
        }
        return std::string(view.data(), view.size());
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
    if (probe_key_any->IsNull(row)) {
      continue;
    }
    ARROW_ASSIGN_OR_RAISE(const auto key, make_key(*probe_key_any, row));
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
