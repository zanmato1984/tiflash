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
#include "tiforth/operators/hash_join.h"
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
    std::string k_name, std::string v_name, const std::vector<std::optional<int32_t>>& ks,
    const std::vector<std::optional<int32_t>>& vs) {
  if (ks.size() != vs.size()) {
    return arrow::Status::Invalid("ks and vs must have the same length");
  }

  auto schema =
      arrow::schema({arrow::field(k_name, arrow::int32()), arrow::field(v_name, arrow::int32())});
  ARROW_ASSIGN_OR_RAISE(auto k_array, MakeInt32Array(ks));
  ARROW_ASSIGN_OR_RAISE(auto v_array, MakeInt32Array(vs));
  return arrow::RecordBatch::Make(schema, static_cast<int64_t>(ks.size()), {k_array, v_array});
}

arrow::Status RunHashJoinSmoke() {
  ARROW_ASSIGN_OR_RAISE(auto build_batch,
                        MakeBatch("k", "bv", {1, 2, 2, std::nullopt}, {100, 200, 201, 999}));
  std::vector<std::shared_ptr<arrow::RecordBatch>> build_batches = {build_batch};

  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, PipelineBuilder::Create(engine.get()));

  JoinKey key{.left = "k", .right = "k"};
  ARROW_RETURN_NOT_OK(builder->AppendTransform([build_batches, key]() -> arrow::Result<TransformOpPtr> {
    return std::make_unique<HashJoinTransformOp>(build_batches, key);
  }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, pipeline->CreateTask());

  ARROW_ASSIGN_OR_RAISE(auto initial_state, task->Step());
  if (initial_state != TaskState::kNeedInput) {
    return arrow::Status::Invalid("expected TaskState::kNeedInput");
  }

  ARROW_ASSIGN_OR_RAISE(auto probe_batch,
                        MakeBatch("k", "pv", {2, 1, 3, std::nullopt}, {20, 10, 30, 0}));
  ARROW_RETURN_NOT_OK(task->PushInput(probe_batch));
  ARROW_RETURN_NOT_OK(task->CloseInput());

  ARROW_ASSIGN_OR_RAISE(auto state, task->Step());
  if (state != TaskState::kHasOutput) {
    return arrow::Status::Invalid("expected TaskState::kHasOutput");
  }

  ARROW_ASSIGN_OR_RAISE(auto out, task->PullOutput());
  if (out == nullptr) {
    return arrow::Status::Invalid("expected non-null output batch");
  }

  if (out->num_columns() != 4 || out->num_rows() != 3) {
    return arrow::Status::Invalid("unexpected output shape");
  }

  ARROW_ASSIGN_OR_RAISE(auto expect_probe_k, MakeInt32Array({2, 2, 1}));
  ARROW_ASSIGN_OR_RAISE(auto expect_probe_pv, MakeInt32Array({20, 20, 10}));
  ARROW_ASSIGN_OR_RAISE(auto expect_build_k, MakeInt32Array({2, 2, 1}));
  ARROW_ASSIGN_OR_RAISE(auto expect_build_bv, MakeInt32Array({200, 201, 100}));

  if (!expect_probe_k->Equals(*out->column(0)) || !expect_probe_pv->Equals(*out->column(1)) ||
      !expect_build_k->Equals(*out->column(2)) || !expect_build_bv->Equals(*out->column(3))) {
    return arrow::Status::Invalid("unexpected join output values");
  }

  ARROW_ASSIGN_OR_RAISE(auto final_state, task->Step());
  if (final_state != TaskState::kFinished) {
    return arrow::Status::Invalid("expected TaskState::kFinished");
  }
  return arrow::Status::OK();
}

}  // namespace

TEST(TiForthHashJoinTest, InnerJoinInt32Key) {
  auto status = RunHashJoinSmoke();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

}  // namespace tiforth

