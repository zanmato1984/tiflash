#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/scalar.h>
#include <arrow/status.h>
#include <gtest/gtest.h>

#include "tiforth/engine.h"
#include "tiforth/expr.h"
#include "tiforth/operators/projection.h"
#include "tiforth/pipeline.h"
#include "tiforth/task.h"

namespace tiforth {

namespace {

arrow::Result<std::shared_ptr<arrow::Array>> MakeInt32Array(const std::vector<int32_t>& values) {
  arrow::Int32Builder builder;
  ARROW_RETURN_NOT_OK(builder.AppendValues(values));
  std::shared_ptr<arrow::Array> array;
  ARROW_RETURN_NOT_OK(builder.Finish(&array));
  return array;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeBatch(const std::vector<int32_t>& xs,
                                                            const std::vector<int32_t>& ys) {
  if (xs.size() != ys.size()) {
    return arrow::Status::Invalid("xs and ys must have the same length");
  }

  auto schema = arrow::schema({arrow::field("x", arrow::int32()), arrow::field("y", arrow::int32())});
  ARROW_ASSIGN_OR_RAISE(auto x_array, MakeInt32Array(xs));
  ARROW_ASSIGN_OR_RAISE(auto y_array, MakeInt32Array(ys));
  return arrow::RecordBatch::Make(schema, static_cast<int64_t>(xs.size()), {x_array, y_array});
}

arrow::Status RunProjectionSmoke() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, PipelineBuilder::Create(engine.get()));

  std::vector<ProjectionExpr> exprs;
  exprs.push_back({"x", MakeFieldRef("x")});
  exprs.push_back({"x_plus_y", MakeCall("add", {MakeFieldRef("x"), MakeFieldRef("y")})});
  exprs.push_back({"x_plus_10",
                   MakeCall("add",
                            {MakeFieldRef("x"), MakeLiteral(std::make_shared<arrow::Int32Scalar>(10))})});

  ARROW_RETURN_NOT_OK(builder->AppendTransform(
      [engine_ptr = engine.get(), exprs]() -> arrow::Result<TransformOpPtr> {
        return std::make_unique<ProjectionTransformOp>(engine_ptr, exprs);
      }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, pipeline->CreateTask());

  ARROW_ASSIGN_OR_RAISE(auto initial_state, task->Step());
  if (initial_state != TaskState::kNeedInput) {
    return arrow::Status::Invalid("expected TaskState::kNeedInput");
  }

  ARROW_ASSIGN_OR_RAISE(auto batch0, MakeBatch({1, 2, 3}, {10, 20, 30}));
  ARROW_RETURN_NOT_OK(task->PushInput(batch0));
  ARROW_ASSIGN_OR_RAISE(auto batch1, MakeBatch({4}, {100}));
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

  // Validate batch0 output.
  if (outputs[0]->num_columns() != 3 || outputs[0]->num_rows() != 3) {
    return arrow::Status::Invalid("unexpected output batch0 shape");
  }
  if (outputs[0]->schema()->field(0)->name() != "x" ||
      outputs[0]->schema()->field(1)->name() != "x_plus_y" ||
      outputs[0]->schema()->field(2)->name() != "x_plus_10") {
    return arrow::Status::Invalid("unexpected output batch0 schema");
  }

  ARROW_ASSIGN_OR_RAISE(auto expect_x, MakeInt32Array({1, 2, 3}));
  ARROW_ASSIGN_OR_RAISE(auto expect_x_plus_y, MakeInt32Array({11, 22, 33}));
  ARROW_ASSIGN_OR_RAISE(auto expect_x_plus_10, MakeInt32Array({11, 12, 13}));

  if (!expect_x->Equals(*outputs[0]->column(0)) ||
      !expect_x_plus_y->Equals(*outputs[0]->column(1)) ||
      !expect_x_plus_10->Equals(*outputs[0]->column(2))) {
    return arrow::Status::Invalid("unexpected output batch0 values");
  }

  // Validate batch1 output.
  if (outputs[1]->num_rows() != 1 || outputs[1]->num_columns() != 3) {
    return arrow::Status::Invalid("unexpected output batch1 shape");
  }
  ARROW_ASSIGN_OR_RAISE(auto expect_x1, MakeInt32Array({4}));
  ARROW_ASSIGN_OR_RAISE(auto expect_x1_plus_y, MakeInt32Array({104}));
  ARROW_ASSIGN_OR_RAISE(auto expect_x1_plus_10, MakeInt32Array({14}));
  if (!expect_x1->Equals(*outputs[1]->column(0)) ||
      !expect_x1_plus_y->Equals(*outputs[1]->column(1)) ||
      !expect_x1_plus_10->Equals(*outputs[1]->column(2))) {
    return arrow::Status::Invalid("unexpected output batch1 values");
  }

  return arrow::Status::OK();
}

}  // namespace

TEST(TiForthProjectionTest, ProjectionAdd) {
  auto status = RunProjectionSmoke();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

}  // namespace tiforth
