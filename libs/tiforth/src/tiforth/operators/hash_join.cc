#include "tiforth/operators/hash_join.h"

#include <memory>
#include <utility>
#include <vector>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/memory_pool.h>
#include <arrow/status.h>
#include <arrow/type.h>

namespace tiforth {

HashJoinTransformOp::HashJoinTransformOp(std::vector<std::shared_ptr<arrow::RecordBatch>> build_batches,
                                         JoinKey key, arrow::MemoryPool* memory_pool)
    : build_batches_(std::move(build_batches)),
      key_(std::move(key)),
      memory_pool_(memory_pool != nullptr ? memory_pool : arrow::default_memory_pool()) {}

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

  for (int i = 0; i < build_schema_->num_fields(); ++i) {
    if (build_schema_->field(i)->type()->id() != arrow::Type::INT32) {
      return arrow::Status::NotImplemented("MS6A supports int32 build columns only");
    }
  }

  for (std::size_t batch_index = 0; batch_index < build_batches_.size(); ++batch_index) {
    const auto& batch = build_batches_[batch_index];
    if (batch == nullptr) {
      return arrow::Status::Invalid("build batch must not be null");
    }
    if (!build_schema_->Equals(*batch->schema(), /*check_metadata=*/true)) {
      return arrow::Status::Invalid("build schema mismatch");
    }

    const auto key_array_any = batch->column(build_key_index_);
    if (key_array_any == nullptr) {
      return arrow::Status::Invalid("build join key column must not be null");
    }
    if (key_array_any->type_id() != arrow::Type::INT32) {
      return arrow::Status::NotImplemented("MS6A build join key must be int32");
    }
    const auto key_array = std::static_pointer_cast<arrow::Int32Array>(key_array_any);

    const int64_t rows = batch->num_rows();
    for (int64_t row = 0; row < rows; ++row) {
      if (key_array->IsNull(row)) {
        continue;
      }
      const int32_t key = key_array->Value(row);
      build_index_[key].push_back(BuildRowRef{.batch_index = batch_index, .row = row});
    }
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
    for (int i = 0; i < probe_schema->num_fields(); ++i) {
      if (probe_schema->field(i)->type()->id() != arrow::Type::INT32) {
        return arrow::Status::NotImplemented("MS6A supports int32 probe columns only");
      }
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
  if (probe_key_any->type_id() != arrow::Type::INT32) {
    return arrow::Status::NotImplemented("MS6A probe join key must be int32");
  }
  const auto probe_key = std::static_pointer_cast<arrow::Int32Array>(probe_key_any);

  const int probe_cols = probe.num_columns();
  const int build_cols = build_schema_->num_fields();
  const int out_cols = probe_cols + build_cols;

  std::vector<std::shared_ptr<arrow::Int32Array>> probe_arrays;
  probe_arrays.reserve(probe_cols);
  for (int i = 0; i < probe_cols; ++i) {
    probe_arrays.push_back(std::static_pointer_cast<arrow::Int32Array>(probe.column(i)));
  }

  std::vector<std::vector<std::shared_ptr<arrow::Int32Array>>> build_arrays;
  build_arrays.resize(build_batches_.size());
  for (std::size_t bi = 0; bi < build_batches_.size(); ++bi) {
    const auto& b = build_batches_[bi];
    build_arrays[bi].reserve(build_cols);
    for (int ci = 0; ci < build_cols; ++ci) {
      build_arrays[bi].push_back(std::static_pointer_cast<arrow::Int32Array>(b->column(ci)));
    }
  }

  std::vector<std::unique_ptr<arrow::Int32Builder>> builders;
  builders.reserve(out_cols);
  for (int i = 0; i < out_cols; ++i) {
    builders.push_back(std::make_unique<arrow::Int32Builder>(memory_pool_));
  }

  int64_t out_rows = 0;
  const int64_t probe_rows = probe.num_rows();
  for (int64_t row = 0; row < probe_rows; ++row) {
    if (probe_key->IsNull(row)) {
      continue;
    }

    const int32_t key = probe_key->Value(row);
    auto it = build_index_.find(key);
    if (it == build_index_.end()) {
      continue;
    }

    for (const auto& ref : it->second) {
      // Append probe columns.
      for (int ci = 0; ci < probe_cols; ++ci) {
        const auto& arr = probe_arrays[ci];
        if (arr->IsNull(row)) {
          ARROW_RETURN_NOT_OK(builders[static_cast<std::size_t>(ci)]->AppendNull());
        } else {
          ARROW_RETURN_NOT_OK(builders[static_cast<std::size_t>(ci)]->Append(arr->Value(row)));
        }
      }

      // Append build columns.
      const auto& b_cols = build_arrays[ref.batch_index];
      for (int ci = 0; ci < build_cols; ++ci) {
        const auto& arr = b_cols[ci];
        const int out_ci = probe_cols + ci;
        if (arr->IsNull(ref.row)) {
          ARROW_RETURN_NOT_OK(builders[static_cast<std::size_t>(out_ci)]->AppendNull());
        } else {
          ARROW_RETURN_NOT_OK(builders[static_cast<std::size_t>(out_ci)]->Append(arr->Value(ref.row)));
        }
      }
      ++out_rows;
    }
  }

  std::vector<std::shared_ptr<arrow::Array>> out_arrays;
  out_arrays.reserve(out_cols);
  for (auto& builder : builders) {
    std::shared_ptr<arrow::Array> array;
    ARROW_RETURN_NOT_OK(builder->Finish(&array));
    out_arrays.push_back(std::move(array));
  }

  *batch = arrow::RecordBatch::Make(output_schema_, out_rows, std::move(out_arrays));
  return OperatorStatus::kHasOutput;
}

}  // namespace tiforth
