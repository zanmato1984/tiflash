#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>

#include <cstdint>
#include <optional>
#include <vector>

#include <gtest/gtest.h>

#include "tiforth/engine.h"
#include "tiforth/expr.h"
#include "tiforth/operators/hash_agg.h"
#include "tiforth/pipeline.h"
#include "tiforth/task.h"

namespace tiforth {

namespace {

arrow::Result<std::shared_ptr<arrow::Array>> MakeInt32Array(
    const std::vector<std::optional<int32_t>>& values) {
  arrow::Int32Builder builder;
  for (const auto& value : values) {
    if (value.has_value()) {
      ARROW_RETURN_NOT_OK(builder.Append(*value));
    } else {
      ARROW_RETURN_NOT_OK(builder.AppendNull());
    }
  }
  std::shared_ptr<arrow::Array> array;
  ARROW_RETURN_NOT_OK(builder.Finish(&array));
  return array;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeBatch(
    const std::vector<std::optional<int32_t>>& keys, const std::vector<std::optional<int32_t>>& values) {
  if (keys.size() != values.size()) {
    return arrow::Status::Invalid("keys and values must have the same length");
  }

  auto schema =
      arrow::schema({arrow::field("k", arrow::int32()), arrow::field("v", arrow::int32())});
  ARROW_ASSIGN_OR_RAISE(auto k_array, MakeInt32Array(keys));
  ARROW_ASSIGN_OR_RAISE(auto v_array, MakeInt32Array(values));
  return arrow::RecordBatch::Make(schema, static_cast<int64_t>(keys.size()), {k_array, v_array});
}

arrow::Status RunHashAggSmoke() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, PipelineBuilder::Create(engine.get()));

  std::vector<AggKey> keys = {{"k", MakeFieldRef("k")}};
  std::vector<AggFunc> aggs;
  aggs.push_back({"cnt", "count_all", nullptr});
  aggs.push_back({"sum_v", "sum_int32", MakeFieldRef("v")});

  ARROW_RETURN_NOT_OK(builder->AppendTransform([keys, aggs]() -> arrow::Result<TransformOpPtr> {
    return std::make_unique<HashAggTransformOp>(keys, aggs);
  }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, pipeline->CreateTask());

  ARROW_ASSIGN_OR_RAISE(auto initial_state, task->Step());
  if (initial_state != TaskState::kNeedInput) {
    return arrow::Status::Invalid("expected TaskState::kNeedInput");
  }

  ARROW_ASSIGN_OR_RAISE(auto batch0, MakeBatch({1, 2, 1, std::nullopt}, {10, 20, std::nullopt, 7}));
  ARROW_RETURN_NOT_OK(task->PushInput(batch0));
  ARROW_ASSIGN_OR_RAISE(auto batch1, MakeBatch({2, 3, std::nullopt, 4}, {1, 5, std::nullopt, std::nullopt}));
  ARROW_RETURN_NOT_OK(task->PushInput(batch1));
  ARROW_RETURN_NOT_OK(task->CloseInput());

  std::vector<std::shared_ptr<arrow::RecordBatch>> outputs;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto state, task->Step());
    if (state == TaskState::kFinished) {
      break;
    }
    if (state == TaskState::kNeedInput) {
      // Hash agg is blocking; it consumes input without producing output.
      continue;
    }
    if (state != TaskState::kHasOutput) {
      return arrow::Status::Invalid("expected TaskState::kHasOutput/kNeedInput/kFinished");
    }

    ARROW_ASSIGN_OR_RAISE(auto out, task->PullOutput());
    if (out == nullptr) {
      return arrow::Status::Invalid("expected non-null output batch");
    }
    outputs.push_back(std::move(out));
  }

  if (outputs.size() != 1) {
    return arrow::Status::Invalid("expected exactly 1 output batch");
  }

  const auto& out = outputs[0];
  if (out->num_columns() != 3 || out->num_rows() != 5) {
    return arrow::Status::Invalid("unexpected output shape");
  }
  if (out->schema()->field(0)->name() != "k" || out->schema()->field(1)->name() != "cnt" ||
      out->schema()->field(2)->name() != "sum_v") {
    return arrow::Status::Invalid("unexpected output schema");
  }

  arrow::Int32Builder key_builder;
  ARROW_RETURN_NOT_OK(key_builder.Append(1));
  ARROW_RETURN_NOT_OK(key_builder.Append(2));
  ARROW_RETURN_NOT_OK(key_builder.AppendNull());
  ARROW_RETURN_NOT_OK(key_builder.Append(3));
  ARROW_RETURN_NOT_OK(key_builder.Append(4));
  std::shared_ptr<arrow::Array> expect_k;
  ARROW_RETURN_NOT_OK(key_builder.Finish(&expect_k));

  arrow::UInt64Builder cnt_builder;
  ARROW_RETURN_NOT_OK(cnt_builder.AppendValues({2, 2, 2, 1, 1}));
  std::shared_ptr<arrow::Array> expect_cnt;
  ARROW_RETURN_NOT_OK(cnt_builder.Finish(&expect_cnt));

  arrow::Int64Builder sum_builder;
  ARROW_RETURN_NOT_OK(sum_builder.Append(10));
  ARROW_RETURN_NOT_OK(sum_builder.Append(21));
  ARROW_RETURN_NOT_OK(sum_builder.Append(7));
  ARROW_RETURN_NOT_OK(sum_builder.Append(5));
  ARROW_RETURN_NOT_OK(sum_builder.AppendNull());
  std::shared_ptr<arrow::Array> expect_sum;
  ARROW_RETURN_NOT_OK(sum_builder.Finish(&expect_sum));

  if (!expect_k->Equals(*out->column(0)) || !expect_cnt->Equals(*out->column(1)) ||
      !expect_sum->Equals(*out->column(2))) {
    return arrow::Status::Invalid("unexpected output values");
  }

  return arrow::Status::OK();
}

}  // namespace

TEST(TiForthHashAggTest, CountAllSumInt32) {
  auto status = RunHashAggSmoke();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

}  // namespace tiforth

