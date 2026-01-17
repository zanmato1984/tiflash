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

arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeBinaryBatch2(
    std::pair<std::string, int32_t> lhs_col,
    std::pair<std::string, int32_t> rhs_col,
    const std::vector<std::string>& lhs_values,
    const std::vector<std::string>& rhs_values) {
  if (lhs_values.size() != rhs_values.size()) {
    return arrow::Status::Invalid("lhs/rhs values size mismatch");
  }

  auto lhs_field = arrow::field(lhs_col.first, arrow::binary());
  auto rhs_field = arrow::field(rhs_col.first, arrow::binary());
  {
    LogicalType lt;
    lt.id = LogicalTypeId::kString;
    lt.collation_id = lhs_col.second;
    ARROW_ASSIGN_OR_RAISE(lhs_field, WithLogicalTypeMetadata(lhs_field, lt));
  }
  {
    LogicalType lt;
    lt.id = LogicalTypeId::kString;
    lt.collation_id = rhs_col.second;
    ARROW_ASSIGN_OR_RAISE(rhs_field, WithLogicalTypeMetadata(rhs_field, lt));
  }

  auto schema = arrow::schema({lhs_field, rhs_field});

  arrow::BinaryBuilder lhs_builder;
  arrow::BinaryBuilder rhs_builder;
  for (std::size_t i = 0; i < lhs_values.size(); ++i) {
    ARROW_RETURN_NOT_OK(lhs_builder.Append(lhs_values[i]));
    ARROW_RETURN_NOT_OK(rhs_builder.Append(rhs_values[i]));
  }
  std::shared_ptr<arrow::Array> lhs_array;
  std::shared_ptr<arrow::Array> rhs_array;
  ARROW_RETURN_NOT_OK(lhs_builder.Finish(&lhs_array));
  ARROW_RETURN_NOT_OK(rhs_builder.Finish(&rhs_array));

  return arrow::RecordBatch::Make(schema, static_cast<int64_t>(lhs_values.size()), {lhs_array, rhs_array});
}

arrow::Status RunFilterEqualsFields(std::pair<std::string, int32_t> lhs_col,
                                   std::pair<std::string, int32_t> rhs_col,
                                   const std::vector<std::string>& lhs_values,
                                   const std::vector<std::string>& rhs_values,
                                   int64_t expected_rows) {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, PipelineBuilder::Create(engine.get()));

  auto predicate = MakeCall("equal", {MakeFieldRef(lhs_col.first), MakeFieldRef(rhs_col.first)});
  ARROW_RETURN_NOT_OK(builder->AppendTransform(
      [engine_ptr = engine.get(), predicate]() -> arrow::Result<TransformOpPtr> {
        return std::make_unique<FilterTransformOp>(engine_ptr, predicate);
      }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, pipeline->CreateTask());

  ARROW_ASSIGN_OR_RAISE(auto input, MakeBinaryBatch2(lhs_col, rhs_col, lhs_values, rhs_values));
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

TEST(TiForthCollationCompareTest, GeneralCiCaseInsensitive) {
  // UTF8_GENERAL_CI: "A" == "a", trims trailing spaces.
  auto status = RunFilterEquals(/*collation_id=*/33, {"A", "a", "a ", "b"}, "a", /*expected_rows=*/3);
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthCollationCompareTest, UnicodeCi0400CaseInsensitive) {
  // UTF8_UNICODE_CI: "A" == "a", trims trailing spaces.
  auto status = RunFilterEquals(/*collation_id=*/192, {"A", "a", "a ", "b"}, "a", /*expected_rows=*/3);
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthCollationCompareTest, UnicodeCi0900NoPadding) {
  // UTF8MB4_0900_AI_CI: "A" == "a", but NO PAD so "a " != "a".
  auto status = RunFilterEquals(/*collation_id=*/255, {"A", "a ", "b"}, "a", /*expected_rows=*/1);
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthCollationCompareTest, CoerceBinaryAndPaddingBinary) {
  // Mixed collation compare should be accepted on the common path; BINARY wins over PAD SPACE BIN.
  auto status = RunFilterEqualsFields(
      /*lhs_col=*/{"s_bin", 63},
      /*rhs_col=*/{"s_pad", 46},
      /*lhs_values=*/{"a ", "a", "a ", "b"},
      /*rhs_values=*/{"a", "a", "a ", "b "},
      /*expected_rows=*/2);
  ASSERT_TRUE(status.ok()) << status.ToString();
}

}  // namespace tiforth
