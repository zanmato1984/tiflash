#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/compute/api_scalar.h>
#include <arrow/compute/initialize.h>
#include <arrow/result.h>
#include <arrow/status.h>

#include <cstdint>
#include <iostream>
#include <vector>

#include "tiforth/tiforth.h"

namespace {

arrow::Result<std::shared_ptr<arrow::Array>> MakeInt32Array(
    const std::vector<int32_t>& values);
arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeXYBatch(const std::vector<int32_t>& xs,
                                                               const std::vector<int32_t>& ys);

arrow::Status RunTiForthSmoke() {
  ARROW_ASSIGN_OR_RAISE(auto engine, tiforth::Engine::Create(tiforth::EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, tiforth::PipelineBuilder::Create(engine.get()));
  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, pipeline->CreateTask());

  ARROW_ASSIGN_OR_RAISE(auto initial_state, task->Step());
  if (initial_state != tiforth::TaskState::kNeedInput) {
    return arrow::Status::Invalid("expected TaskState::kNeedInput");
  }

  auto schema = arrow::schema({arrow::field("x", arrow::int32())});
  ARROW_ASSIGN_OR_RAISE(auto array, MakeInt32Array({1, 2, 3}));
  auto batch = arrow::RecordBatch::Make(schema, /*num_rows=*/3, {array});

  ARROW_RETURN_NOT_OK(task->PushInput(batch));
  ARROW_RETURN_NOT_OK(task->CloseInput());

  ARROW_ASSIGN_OR_RAISE(auto state, task->Step());
  if (state != tiforth::TaskState::kHasOutput) {
    return arrow::Status::Invalid("expected TaskState::kHasOutput");
  }

  ARROW_ASSIGN_OR_RAISE(auto out, task->PullOutput());
  if (out.get() != batch.get()) {
    return arrow::Status::Invalid("expected pass-through output batch");
  }

  ARROW_ASSIGN_OR_RAISE(auto final_state, task->Step());
  if (final_state != tiforth::TaskState::kFinished) {
    return arrow::Status::Invalid("expected TaskState::kFinished");
  }
  return arrow::Status::OK();
}

arrow::Status RunTiForthProjectionSmoke() {
  ARROW_ASSIGN_OR_RAISE(auto engine, tiforth::Engine::Create(tiforth::EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, tiforth::PipelineBuilder::Create(engine.get()));

  std::vector<tiforth::ProjectionExpr> exprs;
  exprs.push_back({"x", tiforth::MakeFieldRef("x")});
  exprs.push_back({"x_plus_y",
                   tiforth::MakeCall("add", {tiforth::MakeFieldRef("x"), tiforth::MakeFieldRef("y")})});
  exprs.push_back(
      {"x_plus_10",
       tiforth::MakeCall("add",
                         {tiforth::MakeFieldRef("x"),
                          tiforth::MakeLiteral(std::make_shared<arrow::Int32Scalar>(10))})});

  ARROW_RETURN_NOT_OK(builder->AppendTransform([exprs]() -> arrow::Result<tiforth::TransformOpPtr> {
    return std::make_unique<tiforth::ProjectionTransformOp>(exprs);
  }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, pipeline->CreateTask());

  ARROW_ASSIGN_OR_RAISE(auto initial_state, task->Step());
  if (initial_state != tiforth::TaskState::kNeedInput) {
    return arrow::Status::Invalid("expected TaskState::kNeedInput");
  }

  ARROW_ASSIGN_OR_RAISE(auto batch0, MakeXYBatch({1, 2, 3}, {10, 20, 30}));
  ARROW_RETURN_NOT_OK(task->PushInput(batch0));
  ARROW_ASSIGN_OR_RAISE(auto batch1, MakeXYBatch({4}, {100}));
  ARROW_RETURN_NOT_OK(task->PushInput(batch1));
  ARROW_RETURN_NOT_OK(task->CloseInput());

  std::size_t seen_batches = 0;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto state, task->Step());
    if (state == tiforth::TaskState::kFinished) {
      break;
    }
    if (state != tiforth::TaskState::kHasOutput) {
      return arrow::Status::Invalid("expected TaskState::kHasOutput or kFinished");
    }

    ARROW_ASSIGN_OR_RAISE(auto out, task->PullOutput());
    if (out == nullptr) {
      return arrow::Status::Invalid("expected non-null output batch");
    }

    if (out->num_columns() != 3) {
      return arrow::Status::Invalid("unexpected projection output column count");
    }
    if (out->schema()->field(0)->name() != "x" || out->schema()->field(1)->name() != "x_plus_y" ||
        out->schema()->field(2)->name() != "x_plus_10") {
      return arrow::Status::Invalid("unexpected projection output schema");
    }

    if (seen_batches == 0) {
      ARROW_ASSIGN_OR_RAISE(auto expect_x, MakeInt32Array({1, 2, 3}));
      ARROW_ASSIGN_OR_RAISE(auto expect_x_plus_y, MakeInt32Array({11, 22, 33}));
      ARROW_ASSIGN_OR_RAISE(auto expect_x_plus_10, MakeInt32Array({11, 12, 13}));
      if (!expect_x->Equals(*out->column(0)) || !expect_x_plus_y->Equals(*out->column(1)) ||
          !expect_x_plus_10->Equals(*out->column(2))) {
        return arrow::Status::Invalid("unexpected projection output values (batch0)");
      }
    } else if (seen_batches == 1) {
      ARROW_ASSIGN_OR_RAISE(auto expect_x, MakeInt32Array({4}));
      ARROW_ASSIGN_OR_RAISE(auto expect_x_plus_y, MakeInt32Array({104}));
      ARROW_ASSIGN_OR_RAISE(auto expect_x_plus_10, MakeInt32Array({14}));
      if (!expect_x->Equals(*out->column(0)) || !expect_x_plus_y->Equals(*out->column(1)) ||
          !expect_x_plus_10->Equals(*out->column(2))) {
        return arrow::Status::Invalid("unexpected projection output values (batch1)");
      }
    } else {
      return arrow::Status::Invalid("unexpected extra output batch");
    }

    ++seen_batches;
  }

  if (seen_batches != 2) {
    return arrow::Status::Invalid("expected exactly 2 output batches");
  }

  return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::Array>> MakeInt32Array(
    const std::vector<int32_t>& values) {
  arrow::Int32Builder builder;
  for (const auto value : values) {
    ARROW_RETURN_NOT_OK(builder.Append(value));
  }

  std::shared_ptr<arrow::Array> array;
  ARROW_RETURN_NOT_OK(builder.Finish(&array));
  return array;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeXYBatch(const std::vector<int32_t>& xs,
                                                               const std::vector<int32_t>& ys) {
  if (xs.size() != ys.size()) {
    return arrow::Status::Invalid("xs and ys must have the same length");
  }

  auto schema =
      arrow::schema({arrow::field("x", arrow::int32()), arrow::field("y", arrow::int32())});
  ARROW_ASSIGN_OR_RAISE(auto x_array, MakeInt32Array(xs));
  ARROW_ASSIGN_OR_RAISE(auto y_array, MakeInt32Array(ys));
  return arrow::RecordBatch::Make(schema, static_cast<int64_t>(xs.size()), {x_array, y_array});
}

arrow::Status RunArrowComputeSmoke() {
  ARROW_RETURN_NOT_OK(arrow::compute::Initialize());

  ARROW_ASSIGN_OR_RAISE(auto lhs, MakeInt32Array({1, 2, 3}));
  ARROW_ASSIGN_OR_RAISE(auto rhs, MakeInt32Array({10, 20, 30}));

  ARROW_ASSIGN_OR_RAISE(auto sum,
                        arrow::compute::Add(arrow::Datum(lhs), arrow::Datum(rhs)));

  if (!sum.is_array()) {
    return arrow::Status::Invalid("expected array result");
  }

  auto sum_array = sum.make_array();
  if (sum_array == nullptr) {
    return arrow::Status::Invalid("expected non-null result array");
  }
  if (sum_array->type_id() != arrow::Type::INT32) {
    return arrow::Status::Invalid("expected int32 result array");
  }

  auto sum_i32 = std::static_pointer_cast<arrow::Int32Array>(sum_array);
  if (sum_i32->length() != 3) {
    return arrow::Status::Invalid("expected length=3 result array");
  }
  if (sum_i32->Value(0) != 11 || sum_i32->Value(1) != 22 || sum_i32->Value(2) != 33) {
    return arrow::Status::Invalid("unexpected compute result values");
  }

  return arrow::Status::OK();
}

arrow::Status RunAll() {
  ARROW_RETURN_NOT_OK(RunTiForthSmoke());
  ARROW_RETURN_NOT_OK(RunTiForthProjectionSmoke());
  ARROW_RETURN_NOT_OK(RunArrowComputeSmoke());
  return arrow::Status::OK();
}

}  // namespace

int main() {
  auto status = RunAll();
  if (!status.ok()) {
    std::cerr << status.ToString() << "\n";
    return 1;
  }
  return 0;
}
