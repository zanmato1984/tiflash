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
#include "tiforth/operators/hash_join.h"
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

arrow::Result<std::shared_ptr<arrow::Array>> MakeBinaryArray(
    const std::vector<std::optional<std::string>>& values) {
  arrow::BinaryBuilder builder;
  for (const auto& value : values) {
    if (value.has_value()) {
      ARROW_RETURN_NOT_OK(builder.Append(reinterpret_cast<const uint8_t*>(value->data()),
                                         static_cast<int32_t>(value->size())));
    } else {
      ARROW_RETURN_NOT_OK(builder.AppendNull());
    }
  }
  std::shared_ptr<arrow::Array> array;
  ARROW_RETURN_NOT_OK(builder.Finish(&array));
  return array;
}

arrow::Result<std::shared_ptr<arrow::Field>> MakeBinaryFieldWithCollation(std::string name,
                                                                          int32_t collation_id) {
  auto field = arrow::field(std::move(name), arrow::binary(), /*nullable=*/true);
  LogicalType type;
  type.id = LogicalTypeId::kString;
  type.collation_id = collation_id;
  return WithLogicalTypeMetadata(field, type);
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

  JoinKey key{.left = {"k"}, .right = {"k"}};
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

arrow::Status RunHashJoinTwoKeySmoke() {
  ARROW_ASSIGN_OR_RAISE(auto build_s_field, MakeBinaryFieldWithCollation("s", /*collation_id=*/46));
  auto build_schema =
      arrow::schema({build_s_field, arrow::field("k2", arrow::int32()),
                     arrow::field("bv", arrow::int32())});

  std::vector<std::optional<std::string>> build_s = {std::string("a"), std::string("a "),
                                                     std::string("a"), std::nullopt,
                                                     std::string("b")};
  ARROW_ASSIGN_OR_RAISE(auto build_s_array, MakeBinaryArray(build_s));
  ARROW_ASSIGN_OR_RAISE(auto build_k2_array, MakeInt32Array({1, 1, 2, 1, std::nullopt}));
  ARROW_ASSIGN_OR_RAISE(auto build_bv_array, MakeInt32Array({100, 101, 200, 999, 998}));
  auto build_batch = arrow::RecordBatch::Make(build_schema, static_cast<int64_t>(build_s.size()),
                                              {build_s_array, build_k2_array, build_bv_array});
  std::vector<std::shared_ptr<arrow::RecordBatch>> build_batches = {build_batch};

  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, PipelineBuilder::Create(engine.get()));

  JoinKey key{.left = {"s", "k2"}, .right = {"s", "k2"}};
  ARROW_RETURN_NOT_OK(builder->AppendTransform([build_batches, key]() -> arrow::Result<TransformOpPtr> {
    return std::make_unique<HashJoinTransformOp>(build_batches, key);
  }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, pipeline->CreateTask());

  ARROW_ASSIGN_OR_RAISE(auto initial_state, task->Step());
  if (initial_state != TaskState::kNeedInput) {
    return arrow::Status::Invalid("expected TaskState::kNeedInput");
  }

  ARROW_ASSIGN_OR_RAISE(auto probe_s_field, MakeBinaryFieldWithCollation("s", /*collation_id=*/46));
  auto probe_schema =
      arrow::schema({probe_s_field, arrow::field("k2", arrow::int32()),
                     arrow::field("pv", arrow::int32())});
  std::vector<std::optional<std::string>> probe_s = {std::string("a "), std::string("a"),
                                                     std::string("b "), std::nullopt,
                                                     std::string("a ")};
  ARROW_ASSIGN_OR_RAISE(auto probe_s_array, MakeBinaryArray(probe_s));
  ARROW_ASSIGN_OR_RAISE(auto probe_k2_array, MakeInt32Array({1, 2, 1, 1, std::nullopt}));
  ARROW_ASSIGN_OR_RAISE(auto probe_pv_array, MakeInt32Array({10, 20, 30, 40, 50}));
  auto probe_batch = arrow::RecordBatch::Make(probe_schema, static_cast<int64_t>(probe_s.size()),
                                              {probe_s_array, probe_k2_array, probe_pv_array});

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

  if (out->num_columns() != 6 || out->num_rows() != 3) {
    return arrow::Status::Invalid("unexpected output shape");
  }

  arrow::BinaryBuilder probe_s_builder;
  ARROW_RETURN_NOT_OK(probe_s_builder.Append(reinterpret_cast<const uint8_t*>("a "), 2));
  ARROW_RETURN_NOT_OK(probe_s_builder.Append(reinterpret_cast<const uint8_t*>("a "), 2));
  ARROW_RETURN_NOT_OK(probe_s_builder.Append(reinterpret_cast<const uint8_t*>("a"), 1));
  std::shared_ptr<arrow::Array> expect_probe_s;
  ARROW_RETURN_NOT_OK(probe_s_builder.Finish(&expect_probe_s));
  ARROW_ASSIGN_OR_RAISE(auto expect_probe_k2, MakeInt32Array({1, 1, 2}));
  ARROW_ASSIGN_OR_RAISE(auto expect_probe_pv, MakeInt32Array({10, 10, 20}));

  arrow::BinaryBuilder build_s_builder;
  ARROW_RETURN_NOT_OK(build_s_builder.Append(reinterpret_cast<const uint8_t*>("a"), 1));
  ARROW_RETURN_NOT_OK(build_s_builder.Append(reinterpret_cast<const uint8_t*>("a "), 2));
  ARROW_RETURN_NOT_OK(build_s_builder.Append(reinterpret_cast<const uint8_t*>("a"), 1));
  std::shared_ptr<arrow::Array> expect_build_s;
  ARROW_RETURN_NOT_OK(build_s_builder.Finish(&expect_build_s));
  ARROW_ASSIGN_OR_RAISE(auto expect_build_k2, MakeInt32Array({1, 1, 2}));
  ARROW_ASSIGN_OR_RAISE(auto expect_build_bv, MakeInt32Array({100, 101, 200}));

  if (!expect_probe_s->Equals(*out->column(0)) || !expect_probe_k2->Equals(*out->column(1)) ||
      !expect_probe_pv->Equals(*out->column(2)) || !expect_build_s->Equals(*out->column(3)) ||
      !expect_build_k2->Equals(*out->column(4)) || !expect_build_bv->Equals(*out->column(5))) {
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

TEST(TiForthHashJoinTest, InnerJoinTwoKeyBinaryAndInt32) {
  auto status = RunHashJoinTwoKeySmoke();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

}  // namespace tiforth
