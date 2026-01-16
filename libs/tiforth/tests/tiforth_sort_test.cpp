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
#include "tiforth/operators/sort.h"
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
    const std::vector<std::optional<int32_t>>& xs, const std::vector<std::optional<int32_t>>& ys) {
  if (xs.size() != ys.size()) {
    return arrow::Status::Invalid("xs and ys must have the same length");
  }

  auto schema =
      arrow::schema({arrow::field("x", arrow::int32()), arrow::field("y", arrow::int32())});
  ARROW_ASSIGN_OR_RAISE(auto x_array, MakeInt32Array(xs));
  ARROW_ASSIGN_OR_RAISE(auto y_array, MakeInt32Array(ys));
  return arrow::RecordBatch::Make(schema, static_cast<int64_t>(xs.size()), {x_array, y_array});
}

arrow::Status RunSortSmoke() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, PipelineBuilder::Create(engine.get()));

  std::vector<SortKey> keys = {SortKey{.name = "x", .ascending = true, .nulls_first = false}};
  ARROW_RETURN_NOT_OK(builder->AppendTransform([keys]() -> arrow::Result<TransformOpPtr> {
    return std::make_unique<SortTransformOp>(keys);
  }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, pipeline->CreateTask());

  ARROW_ASSIGN_OR_RAISE(auto initial_state, task->Step());
  if (initial_state != TaskState::kNeedInput) {
    return arrow::Status::Invalid("expected TaskState::kNeedInput");
  }

  // Concatenated input: x=[3,1,null,2], y=[30,10,99,20]
  // Sorted (ASC, nulls last): x=[1,2,3,null], y=[10,20,30,99]
  ARROW_ASSIGN_OR_RAISE(auto batch0, MakeBatch({3, 1}, {30, 10}));
  ARROW_RETURN_NOT_OK(task->PushInput(batch0));
  ARROW_ASSIGN_OR_RAISE(auto batch1, MakeBatch({std::nullopt, 2}, {99, 20}));
  ARROW_RETURN_NOT_OK(task->PushInput(batch1));
  ARROW_RETURN_NOT_OK(task->CloseInput());

  std::vector<std::shared_ptr<arrow::RecordBatch>> outputs;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto state, task->Step());
    if (state == TaskState::kFinished) {
      break;
    }
    if (state == TaskState::kNeedInput) {
      continue;
    }
    if (state != TaskState::kHasOutput) {
      return arrow::Status::Invalid("unexpected task state");
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
  if (out->num_columns() != 2 || out->num_rows() != 4) {
    return arrow::Status::Invalid("unexpected output shape");
  }

  ARROW_ASSIGN_OR_RAISE(auto expect_x, MakeInt32Array({1, 2, 3, std::nullopt}));
  ARROW_ASSIGN_OR_RAISE(auto expect_y, MakeInt32Array({10, 20, 30, 99}));
  if (!expect_x->Equals(*out->column(0)) || !expect_y->Equals(*out->column(1))) {
    return arrow::Status::Invalid("unexpected output values");
  }

  return arrow::Status::OK();
}

}  // namespace

TEST(TiForthSortTest, SortInt32AscNullsLast) {
  auto status = RunSortSmoke();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

}  // namespace tiforth

