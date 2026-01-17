#include "tiforth/operators/sort.h"

#include <algorithm>
#include <cstdint>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <arrow/array/concatenate.h>
#include <arrow/builder.h>
#include <arrow/compute/api_vector.h>
#include <arrow/memory_pool.h>
#include <arrow/status.h>

#include "tiforth/engine.h"
#include "tiforth/collation.h"
#include "tiforth/detail/arrow_compute.h"
#include "tiforth/type_metadata.h"

namespace tiforth {

namespace {

}  // namespace

SortTransformOp::SortTransformOp(const Engine* engine, std::vector<SortKey> keys,
                                 arrow::MemoryPool* memory_pool)
    : keys_(std::move(keys)),
      exec_context_(memory_pool != nullptr
                        ? memory_pool
                        : (engine != nullptr ? engine->memory_pool() : arrow::default_memory_pool()),
                    /*executor=*/nullptr,
                    engine != nullptr ? engine->function_registry() : nullptr) {}

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

      const auto sort_with = [&](auto tag) {
        constexpr CollationKind kind = decltype(tag)::value;
        // For collations with non-trivial semantics, precompute per-row sort keys ("weight strings")
        // and then sort by normalized bytes. This avoids repeated UTF-8 decode/weighting in the
        // comparator (std::stable_sort calls it O(N log N) times).
        const auto row_count = static_cast<std::size_t>(total_rows);
        std::vector<uint8_t> is_null(row_count, 0);
        std::vector<std::string_view> key_views(row_count);

        if constexpr (kind == CollationKind::kGeneralCi || kind == CollationKind::kUnicodeCi0400 ||
                      kind == CollationKind::kUnicodeCi0900) {
          std::vector<std::string> key_strings(row_count);
          for (std::size_t i = 0; i < row_count; ++i) {
            const bool null = bin->IsNull(static_cast<int64_t>(i));
            is_null[i] = static_cast<uint8_t>(null);
            if (null) {
              continue;
            }
            const std::string_view view = bin->GetView(static_cast<int64_t>(i));
            key_strings[i] = SortKeyString<kind>(view);
            key_views[i] = std::string_view(key_strings[i]);
          }

          std::stable_sort(idx.begin(), idx.end(), [&](uint64_t lhs, uint64_t rhs) -> bool {
            const bool lhs_null = is_null[static_cast<std::size_t>(lhs)] != 0;
            const bool rhs_null = is_null[static_cast<std::size_t>(rhs)] != 0;
            if (lhs_null != rhs_null) {
              return rhs_null;  // nulls last
            }
            if (lhs_null) {
              return false;
            }
            const std::string_view lhs_key = key_views[static_cast<std::size_t>(lhs)];
            const std::string_view rhs_key = key_views[static_cast<std::size_t>(rhs)];
            return CompareBinary(lhs_key, rhs_key) < 0;
          });
          return;
        }

        for (std::size_t i = 0; i < row_count; ++i) {
          const bool null = bin->IsNull(static_cast<int64_t>(i));
          is_null[i] = static_cast<uint8_t>(null);
          if (null) {
            continue;
          }
          std::string_view view = bin->GetView(static_cast<int64_t>(i));
          if constexpr (kind == CollationKind::kPaddingBinary) {
            view = RightTrimAsciiSpace(view);
          }
          key_views[i] = view;
        }

        std::stable_sort(idx.begin(), idx.end(), [&](uint64_t lhs, uint64_t rhs) -> bool {
          const bool lhs_null = is_null[static_cast<std::size_t>(lhs)] != 0;
          const bool rhs_null = is_null[static_cast<std::size_t>(rhs)] != 0;
          if (lhs_null != rhs_null) {
            return rhs_null;  // nulls last
          }
          if (lhs_null) {
            return false;
          }
          const std::string_view lhs_key = key_views[static_cast<std::size_t>(lhs)];
          const std::string_view rhs_key = key_views[static_cast<std::size_t>(rhs)];
          return CompareBinary(lhs_key, rhs_key) < 0;
        });
      };

      switch (collation.kind) {
        case CollationKind::kBinary:
          sort_with(std::integral_constant<CollationKind, CollationKind::kBinary>{});
          break;
        case CollationKind::kPaddingBinary:
          sort_with(std::integral_constant<CollationKind, CollationKind::kPaddingBinary>{});
          break;
        case CollationKind::kGeneralCi:
          sort_with(std::integral_constant<CollationKind, CollationKind::kGeneralCi>{});
          break;
        case CollationKind::kUnicodeCi0400:
          sort_with(std::integral_constant<CollationKind, CollationKind::kUnicodeCi0400>{});
          break;
        case CollationKind::kUnicodeCi0900:
          sort_with(std::integral_constant<CollationKind, CollationKind::kUnicodeCi0900>{});
          break;
        case CollationKind::kUnsupported:
          return arrow::Status::NotImplemented("unsupported collation id: ", collation_id);
      }

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
    ARROW_ASSIGN_OR_RAISE(auto out_array, detail::DatumToArray(taken, exec_context_.memory_pool()));
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
