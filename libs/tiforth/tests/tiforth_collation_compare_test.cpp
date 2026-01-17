#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>

#include <gtest/gtest.h>

#include "tiforth/engine.h"
#include "tiforth/expr.h"
#include "tiforth/operators/filter.h"
#include "tiforth/pipeline.h"
#include "tiforth/task.h"
#include "tiforth/type_metadata.h"

namespace tiforth {

namespace {

arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeBinaryBatch(
    int32_t collation_id, const std::vector<std::string>& values) {
  auto field = arrow::field("s", arrow::binary());
  LogicalType lt;
  lt.id = LogicalTypeId::kString;
  lt.collation_id = collation_id;
  ARROW_ASSIGN_OR_RAISE(field, WithLogicalTypeMetadata(field, lt));

  auto schema = arrow::schema({field});

  arrow::BinaryBuilder builder;
  for (const auto& v : values) {
    ARROW_RETURN_NOT_OK(builder.Append(v));
  }
  std::shared_ptr<arrow::Array> array;
  ARROW_RETURN_NOT_OK(builder.Finish(&array));

  return arrow::RecordBatch::Make(schema, static_cast<int64_t>(values.size()), {array});
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeLargeBinaryBatch(
    int32_t collation_id, const std::vector<std::string>& values) {
  auto field = arrow::field("s", arrow::large_binary());
  LogicalType lt;
  lt.id = LogicalTypeId::kString;
  lt.collation_id = collation_id;
  ARROW_ASSIGN_OR_RAISE(field, WithLogicalTypeMetadata(field, lt));

  auto schema = arrow::schema({field});

  arrow::LargeBinaryBuilder builder;
  for (const auto& v : values) {
    ARROW_RETURN_NOT_OK(builder.Append(v));
  }
  std::shared_ptr<arrow::Array> array;
  ARROW_RETURN_NOT_OK(builder.Finish(&array));

  return arrow::RecordBatch::Make(schema, static_cast<int64_t>(values.size()), {array});
}

arrow::Status RunFilterEquals(int32_t collation_id, const std::vector<std::string>& values,
                              const std::string& literal, int64_t expected_rows) {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, PipelineBuilder::Create(engine.get()));

  auto predicate = MakeCall(
      "equal",
      {MakeFieldRef("s"), MakeLiteral(std::make_shared<arrow::BinaryScalar>(literal))});
  ARROW_RETURN_NOT_OK(builder->AppendTransform(
      [engine_ptr = engine.get(), predicate]() -> arrow::Result<TransformOpPtr> {
        return std::make_unique<FilterTransformOp>(engine_ptr, predicate);
      }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, pipeline->CreateTask());

  ARROW_ASSIGN_OR_RAISE(auto input, MakeBinaryBatch(collation_id, values));
  ARROW_RETURN_NOT_OK(task->PushInput(input));
  ARROW_RETURN_NOT_OK(task->CloseInput());

  ARROW_ASSIGN_OR_RAISE(auto state, task->Step());
  if (state != TaskState::kHasOutput) {
    return arrow::Status::Invalid("expected output");
  }

  ARROW_ASSIGN_OR_RAISE(auto out, task->PullOutput());
  if (out == nullptr) {
    return arrow::Status::Invalid("expected non-null output batch");
  }
  if (out->num_rows() != expected_rows) {
    return arrow::Status::Invalid("unexpected output rows");
  }
  return arrow::Status::OK();
}

arrow::Status RunFilterEqualsLargeBinary(int32_t collation_id, const std::vector<std::string>& values,
                                        const std::string& literal, int64_t expected_rows) {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, PipelineBuilder::Create(engine.get()));

  auto predicate = MakeCall(
      "equal", {MakeFieldRef("s"),
                MakeLiteral(std::make_shared<arrow::LargeBinaryScalar>(literal))});
  ARROW_RETURN_NOT_OK(builder->AppendTransform(
      [engine_ptr = engine.get(), predicate]() -> arrow::Result<TransformOpPtr> {
        return std::make_unique<FilterTransformOp>(engine_ptr, predicate);
      }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, pipeline->CreateTask());

  ARROW_ASSIGN_OR_RAISE(auto input, MakeLargeBinaryBatch(collation_id, values));
  ARROW_RETURN_NOT_OK(task->PushInput(input));
  ARROW_RETURN_NOT_OK(task->CloseInput());

  ARROW_ASSIGN_OR_RAISE(auto state, task->Step());
  if (state != TaskState::kHasOutput) {
    return arrow::Status::Invalid("expected output");
  }

  ARROW_ASSIGN_OR_RAISE(auto out, task->PullOutput());
  if (out == nullptr) {
    return arrow::Status::Invalid("expected non-null output batch");
  }
  if (out->num_rows() != expected_rows) {
    return arrow::Status::Invalid("unexpected output rows");
  }
  return arrow::Status::OK();
}

}  // namespace

TEST(TiForthCollationCompareTest, BinaryNoPadding) {
  // BINARY: "a " != "a".
  auto status = RunFilterEquals(/*collation_id=*/63, {"a ", "a"}, "a", /*expected_rows=*/1);
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthCollationCompareTest, PaddingBinaryTrimsSpaces) {
  // UTF8MB4_BIN (PAD SPACE): "a " == "a".
  auto status = RunFilterEquals(/*collation_id=*/46, {"a ", "a"}, "a", /*expected_rows=*/2);
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthCollationCompareTest, LargeBinaryPaddingBinaryTrimsSpaces) {
  auto status = RunFilterEqualsLargeBinary(/*collation_id=*/46, {"a ", "a"}, "a", /*expected_rows=*/2);
  ASSERT_TRUE(status.ok()) << status.ToString();
}

}  // namespace tiforth
