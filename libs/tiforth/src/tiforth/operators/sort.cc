#include "tiforth/operators/sort.h"

#include <algorithm>
#include <cstdint>
#include <string_view>
#include <utility>
#include <vector>

#include <arrow/array/concatenate.h>
#include <arrow/builder.h>
#include <arrow/chunked_array.h>
#include <arrow/compute/api_vector.h>
#include <arrow/memory_pool.h>
#include <arrow/status.h>

#include "tiforth/collation.h"
#include "tiforth/detail/arrow_compute.h"
#include "tiforth/type_metadata.h"

namespace tiforth {

namespace {

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

SortTransformOp::SortTransformOp(std::vector<SortKey> keys, arrow::MemoryPool* memory_pool)
    : keys_(std::move(keys)),
      exec_context_(memory_pool != nullptr ? memory_pool : arrow::default_memory_pool()) {}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> SortTransformOp::SortAll() {
  if (keys_.size() != 1) {
    return arrow::Status::NotImplemented("MS6B supports exactly one sort key");
  }
  if (!keys_[0].ascending || keys_[0].nulls_first) {
    return arrow::Status::NotImplemented("MS6B supports ASC with nulls last only");
  }
  if (output_schema_ == nullptr) {
    return arrow::Status::Invalid("sort output schema must not be null");
  }
  if (buffered_.empty()) {
    return arrow::Status::Invalid("SortAll requires at least one buffered batch");
  }

  const int num_columns = output_schema_->num_fields();
  int64_t total_rows = 0;
  for (const auto& batch : buffered_) {
    if (batch == nullptr) {
      return arrow::Status::Invalid("buffered batch must not be null");
    }
    if (!output_schema_->Equals(*batch->schema(), /*check_metadata=*/true)) {
      return arrow::Status::Invalid("sort input schema mismatch");
    }
    if (batch->num_columns() != num_columns) {
      return arrow::Status::Invalid("sort input column count mismatch");
    }
    total_rows += batch->num_rows();
  }

  std::vector<std::shared_ptr<arrow::Array>> combined_columns;
  combined_columns.reserve(num_columns);
  for (int col = 0; col < num_columns; ++col) {
    std::vector<std::shared_ptr<arrow::Array>> chunks;
    chunks.reserve(buffered_.size());
    for (const auto& batch : buffered_) {
      chunks.push_back(batch->column(col));
    }
    ARROW_ASSIGN_OR_RAISE(auto combined, arrow::Concatenate(chunks, exec_context_.memory_pool()));
    combined_columns.push_back(std::move(combined));
  }

  const int key_index = output_schema_->GetFieldIndex(keys_[0].name);
  if (key_index < 0 || key_index >= num_columns) {
    return arrow::Status::Invalid("unknown sort key: ", keys_[0].name);
  }
  const auto& key_array = combined_columns[static_cast<std::size_t>(key_index)];
  if (key_array == nullptr) {
    return arrow::Status::Invalid("sort key array must not be null");
  }

  ARROW_RETURN_NOT_OK(detail::EnsureArrowComputeInitialized());
  arrow::Datum indices;
  if (const auto& key_field = output_schema_->field(key_index); key_field != nullptr) {
    ARROW_ASSIGN_OR_RAISE(const auto logical_type, GetLogicalType(*key_field));
    if (logical_type.id == LogicalTypeId::kString) {
      const int32_t collation_id =
          logical_type.collation_id >= 0 ? logical_type.collation_id : 63;
      const auto collation = CollationFromId(collation_id);
      if (collation.kind == CollationKind::kUnsupported) {
        return arrow::Status::NotImplemented("unsupported collation id: ", collation_id);
      }
      if (key_array->type_id() != arrow::Type::BINARY) {
        return arrow::Status::NotImplemented("string sort requires binary key array");
      }
      const auto bin = std::static_pointer_cast<arrow::BinaryArray>(key_array);

      std::vector<uint64_t> idx;
      idx.reserve(static_cast<std::size_t>(total_rows));
      for (uint64_t i = 0; i < static_cast<uint64_t>(total_rows); ++i) {
        idx.push_back(i);
      }

      std::stable_sort(idx.begin(), idx.end(), [&](uint64_t lhs, uint64_t rhs) -> bool {
        const bool lhs_null = bin->IsNull(static_cast<int64_t>(lhs));
        const bool rhs_null = bin->IsNull(static_cast<int64_t>(rhs));
        if (lhs_null != rhs_null) {
          return rhs_null;  // nulls last
        }
        if (lhs_null) {
          return false;
        }
        const std::string_view lhs_view = bin->GetView(static_cast<int64_t>(lhs));
        const std::string_view rhs_view = bin->GetView(static_cast<int64_t>(rhs));
        return CompareString(collation, lhs_view, rhs_view) < 0;
      });

      arrow::UInt64Builder idx_builder(exec_context_.memory_pool());
      ARROW_RETURN_NOT_OK(idx_builder.AppendValues(idx));
      std::shared_ptr<arrow::Array> idx_array;
      ARROW_RETURN_NOT_OK(idx_builder.Finish(&idx_array));
      indices = arrow::Datum(std::move(idx_array));
    }
  }
  if (!indices.is_value()) {
    ARROW_ASSIGN_OR_RAISE(indices,
                          arrow::compute::SortIndices(*key_array,
                                                      arrow::compute::SortOrder::Ascending,
                                                      &exec_context_));
  }

  std::vector<std::shared_ptr<arrow::Array>> sorted_columns;
  sorted_columns.reserve(combined_columns.size());
  const auto take_options = arrow::compute::TakeOptions::NoBoundsCheck();
  for (const auto& col : combined_columns) {
    ARROW_ASSIGN_OR_RAISE(
        auto taken,
        arrow::compute::Take(arrow::Datum(col), arrow::Datum(indices), take_options, &exec_context_));
    ARROW_ASSIGN_OR_RAISE(auto out_array, DatumToArray(taken, exec_context_.memory_pool()));
    sorted_columns.push_back(std::move(out_array));
  }

  buffered_.clear();
  buffered_.shrink_to_fit();

  return arrow::RecordBatch::Make(output_schema_, total_rows, std::move(sorted_columns));
}

arrow::Result<OperatorStatus> SortTransformOp::TransformImpl(std::shared_ptr<arrow::RecordBatch>* batch) {
  if (*batch == nullptr) {
    if (!output_emitted_) {
      if (!buffered_.empty()) {
        ARROW_ASSIGN_OR_RAISE(*batch, SortAll());
      } else {
        batch->reset();
      }
      output_emitted_ = true;
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

  if (output_emitted_) {
    return arrow::Status::Invalid("sort received input after end-of-stream");
  }

  const auto& input = **batch;
  if (output_schema_ == nullptr) {
    output_schema_ = input.schema();
  } else if (!output_schema_->Equals(*input.schema(), /*check_metadata=*/true)) {
    return arrow::Status::Invalid("sort input schema mismatch");
  }

  buffered_.push_back(std::move(*batch));
  batch->reset();
  return OperatorStatus::kNeedInput;
}

}  // namespace tiforth
