#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "tiforth/engine.h"
#include "tiforth/operators/sort.h"
#include "tiforth/pipeline.h"
#include "tiforth/task.h"
#include "tiforth/type_metadata.h"

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
  const auto* eng = engine.get();

  std::vector<SortKey> keys = {SortKey{.name = "x", .ascending = true, .nulls_first = false}};
  ARROW_RETURN_NOT_OK(builder->AppendTransform([keys, eng]() -> arrow::Result<TransformOpPtr> {
    return std::make_unique<SortTransformOp>(eng, keys);
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

arrow::Result<std::shared_ptr<arrow::Array>> MakeBinaryArray(
    const std::vector<std::optional<std::string>>& values) {
  arrow::BinaryBuilder builder;
  for (const auto& v : values) {
    if (v.has_value()) {
      ARROW_RETURN_NOT_OK(builder.Append(*v));
    } else {
      ARROW_RETURN_NOT_OK(builder.AppendNull());
    }
  }
  std::shared_ptr<arrow::Array> array;
  ARROW_RETURN_NOT_OK(builder.Finish(&array));
  return array;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeStringSortBatch(
    int32_t collation_id, const std::vector<std::optional<std::string>>& ss,
    const std::vector<int32_t>& vs) {
  if (ss.size() != vs.size()) {
    return arrow::Status::Invalid("ss and vs must have the same length");
  }

  auto s_field = arrow::field("s", arrow::binary());
  LogicalType lt;
  lt.id = LogicalTypeId::kString;
  lt.collation_id = collation_id;
  ARROW_ASSIGN_OR_RAISE(s_field, WithLogicalTypeMetadata(s_field, lt));

  auto schema = arrow::schema({s_field, arrow::field("v", arrow::int32())});
  ARROW_ASSIGN_OR_RAISE(auto s_array, MakeBinaryArray(ss));

  arrow::Int32Builder v_builder;
  ARROW_RETURN_NOT_OK(v_builder.AppendValues(vs));
  std::shared_ptr<arrow::Array> v_array;
  ARROW_RETURN_NOT_OK(v_builder.Finish(&v_array));

  return arrow::RecordBatch::Make(schema, static_cast<int64_t>(ss.size()), {s_array, v_array});
}

arrow::Status RunStringSort(int32_t collation_id, const std::vector<int32_t>& expected_vs) {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, PipelineBuilder::Create(engine.get()));
  const auto* eng = engine.get();

  std::vector<SortKey> keys = {SortKey{.name = "s", .ascending = true, .nulls_first = false}};
  ARROW_RETURN_NOT_OK(builder->AppendTransform([keys, eng]() -> arrow::Result<TransformOpPtr> {
    return std::make_unique<SortTransformOp>(eng, keys);
  }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, pipeline->CreateTask());

  ARROW_ASSIGN_OR_RAISE(auto initial_state, task->Step());
  if (initial_state != TaskState::kNeedInput) {
    return arrow::Status::Invalid("expected TaskState::kNeedInput");
  }

  // Input (2 batches): [("a ",1), ("b",3)], [("a",2), (null,4)].
  ARROW_ASSIGN_OR_RAISE(
      auto batch0,
      MakeStringSortBatch(collation_id, {std::string("a "), std::string("b")}, {1, 3}));
  ARROW_RETURN_NOT_OK(task->PushInput(batch0));
  ARROW_ASSIGN_OR_RAISE(
      auto batch1,
      MakeStringSortBatch(collation_id, {std::string("a"), std::nullopt}, {2, 4}));
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

  const auto v_out = std::static_pointer_cast<arrow::Int32Array>(out->column(1));
  if (v_out == nullptr) {
    return arrow::Status::Invalid("expected int32 v output column");
  }

  std::vector<int32_t> actual_vs;
  actual_vs.reserve(static_cast<std::size_t>(v_out->length()));
  for (int64_t i = 0; i < v_out->length(); ++i) {
    if (v_out->IsNull(i)) {
      return arrow::Status::Invalid("unexpected null in v output");
    }
    actual_vs.push_back(v_out->Value(i));
  }

  if (actual_vs != expected_vs) {
    return arrow::Status::Invalid("unexpected sorted v values");
  }
  return arrow::Status::OK();
}

}  // namespace

TEST(TiForthSortTest, SortInt32AscNullsLast) {
  auto status = RunSortSmoke();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthSortTest, SortStringBinaryCollation) {
  // Binary collation: "a" < "a " < "b", nulls last.
  auto status = RunStringSort(/*collation_id=*/63, /*expected_vs=*/{2, 1, 3, 4});
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthSortTest, SortStringPaddingBinaryCollation) {
  // Padding BIN: "a " == "a", stable (keeps input order 1 then 2).
  auto status = RunStringSort(/*collation_id=*/46, /*expected_vs=*/{1, 2, 3, 4});
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthSortTest, SortStringGeneralCiCollation) {
  // General CI uses weight strings with padding semantics (trailing ASCII spaces trimmed).
  auto status = RunStringSort(/*collation_id=*/45, /*expected_vs=*/{1, 2, 3, 4});
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthSortTest, SortStringUnicode0900Collation) {
  // Unicode 0900 AI CI is NO PAD: "a" < "a " (space is significant).
  auto status = RunStringSort(/*collation_id=*/255, /*expected_vs=*/{2, 1, 3, 4});
  ASSERT_TRUE(status.ok()) << status.ToString();
}

}  // namespace tiforth
