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
#include "tiforth/operators/filter.h"
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

arrow::Status RunFilterGreaterThan() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, PipelineBuilder::Create(engine.get()));

  auto predicate = MakeCall("greater",
                            {MakeFieldRef("x"),
                             MakeLiteral(std::make_shared<arrow::Int32Scalar>(2))});
  ARROW_RETURN_NOT_OK(builder->AppendTransform(
      [engine_ptr = engine.get(), predicate]() -> arrow::Result<TransformOpPtr> {
        return std::make_unique<FilterTransformOp>(engine_ptr, predicate);
      }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, pipeline->CreateTask());

  ARROW_ASSIGN_OR_RAISE(auto initial_state, task->Step());
  if (initial_state != TaskState::kNeedInput) {
    return arrow::Status::Invalid("expected TaskState::kNeedInput");
  }

  ARROW_ASSIGN_OR_RAISE(auto batch0, MakeBatch({1, 2, 3}, {10, 20, 30}));
  ARROW_RETURN_NOT_OK(task->PushInput(batch0));
  ARROW_ASSIGN_OR_RAISE(auto batch1, MakeBatch({4}, {40}));
  ARROW_RETURN_NOT_OK(task->PushInput(batch1));
  ARROW_RETURN_NOT_OK(task->CloseInput());

  std::shared_ptr<arrow::Schema> seen_schema;
  std::vector<std::shared_ptr<arrow::RecordBatch>> outputs;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto state, task->Step());
    if (state == TaskState::kFinished) {
      break;
    }
    if (state != TaskState::kHasOutput) {
      return arrow::Status::Invalid("expected TaskState::kHasOutput or kFinished");
    }

    ARROW_ASSIGN_OR_RAISE(auto out, task->PullOutput());
    if (out == nullptr) {
      return arrow::Status::Invalid("expected non-null output batch");
    }
    if (seen_schema == nullptr) {
      seen_schema = out->schema();
    } else if (out->schema().get() != seen_schema.get()) {
      return arrow::Status::Invalid("expected stable shared output schema");
    }
    outputs.push_back(std::move(out));
  }

  if (outputs.size() != 2) {
    return arrow::Status::Invalid("expected 2 output batches");
  }

  ARROW_ASSIGN_OR_RAISE(auto expect_x0, MakeInt32Array({3}));
  ARROW_ASSIGN_OR_RAISE(auto expect_y0, MakeInt32Array({30}));
  if (outputs[0]->num_rows() != 1 || outputs[0]->num_columns() != 2) {
    return arrow::Status::Invalid("unexpected filtered batch0 shape");
  }
  if (!expect_x0->Equals(*outputs[0]->column(0)) || !expect_y0->Equals(*outputs[0]->column(1))) {
    return arrow::Status::Invalid("unexpected filtered batch0 values");
  }

  ARROW_ASSIGN_OR_RAISE(auto expect_x1, MakeInt32Array({4}));
  ARROW_ASSIGN_OR_RAISE(auto expect_y1, MakeInt32Array({40}));
  if (outputs[1]->num_rows() != 1 || outputs[1]->num_columns() != 2) {
    return arrow::Status::Invalid("unexpected filtered batch1 shape");
  }
  if (!expect_x1->Equals(*outputs[1]->column(0)) || !expect_y1->Equals(*outputs[1]->column(1))) {
    return arrow::Status::Invalid("unexpected filtered batch1 values");
  }

  return arrow::Status::OK();
}

arrow::Status RunFilterDropsNullPredicate() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, PipelineBuilder::Create(engine.get()));

  auto predicate = MakeCall("greater",
                            {MakeFieldRef("x"),
                             MakeLiteral(std::make_shared<arrow::Int32Scalar>(1))});
  ARROW_RETURN_NOT_OK(builder->AppendTransform(
      [engine_ptr = engine.get(), predicate]() -> arrow::Result<TransformOpPtr> {
        return std::make_unique<FilterTransformOp>(engine_ptr, predicate);
      }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, pipeline->CreateTask());

  ARROW_ASSIGN_OR_RAISE(auto initial_state, task->Step());
  if (initial_state != TaskState::kNeedInput) {
    return arrow::Status::Invalid("expected TaskState::kNeedInput");
  }

  // x = [1, null, 3], predicate: x > 1 => [false, null, true]
  // SQL WHERE semantics => keep only the last row (true).
  ARROW_ASSIGN_OR_RAISE(auto batch0, MakeBatch({1, std::nullopt, 3}, {10, 20, 30}));
  ARROW_RETURN_NOT_OK(task->PushInput(batch0));
  ARROW_RETURN_NOT_OK(task->CloseInput());

  ARROW_ASSIGN_OR_RAISE(auto state, task->Step());
  if (state != TaskState::kHasOutput) {
    return arrow::Status::Invalid("expected TaskState::kHasOutput");
  }

  ARROW_ASSIGN_OR_RAISE(auto out, task->PullOutput());
  if (out == nullptr) {
    return arrow::Status::Invalid("expected non-null output batch");
  }

  ARROW_ASSIGN_OR_RAISE(auto expect_x, MakeInt32Array({3}));
  ARROW_ASSIGN_OR_RAISE(auto expect_y, MakeInt32Array({30}));
  if (out->num_rows() != 1 || out->num_columns() != 2) {
    return arrow::Status::Invalid("unexpected filtered output shape");
  }
  if (!expect_x->Equals(*out->column(0)) || !expect_y->Equals(*out->column(1))) {
    return arrow::Status::Invalid("unexpected filtered output values");
  }

  ARROW_ASSIGN_OR_RAISE(auto final_state, task->Step());
  if (final_state != TaskState::kFinished) {
    return arrow::Status::Invalid("expected TaskState::kFinished");
  }
  return arrow::Status::OK();
}

}  // namespace

TEST(TiForthFilterTest, GreaterThan) {
  auto status = RunFilterGreaterThan();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthFilterTest, DropsNullPredicate) {
  auto status = RunFilterDropsNullPredicate();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

}  // namespace tiforth
