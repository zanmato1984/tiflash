#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/testing/gtest_util.h>
#include <arrow/type.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "tiforth_c/tiforth.h"

namespace {

void FreeStatus(tiforth_status_t* status) {
  if (status == nullptr) {
    return;
  }
  if (status->message != nullptr) {
    tiforth_free(status->message);
    status->message = nullptr;
  }
}

void AssertOkAndFree(tiforth_status_t status) {
  const std::string message = status.message != nullptr ? status.message : "";
  ASSERT_EQ(status.code, TIFORTH_STATUS_OK) << message;
  FreeStatus(&status);
}

}  // namespace

TEST(TiForthCapiSmokeTest, EngineCreateDestroy) {
  tiforth_engine_options_t options{};
  options.abi_version = TIFORTH_C_ABI_VERSION;

  tiforth_engine_t* engine = nullptr;
  AssertOkAndFree(tiforth_engine_create(&options, &engine));
  ASSERT_NE(engine, nullptr);
  tiforth_engine_destroy(engine);
}

TEST(TiForthCapiSmokeTest, EngineCreateRejectsBadAbiVersion) {
  tiforth_engine_options_t options{};
  options.abi_version = TIFORTH_C_ABI_VERSION + 1;
  tiforth_engine_t* engine = nullptr;

  tiforth_status_t status = tiforth_engine_create(&options, &engine);
  EXPECT_EQ(status.code, TIFORTH_STATUS_INVALID_ARGUMENT);
  EXPECT_EQ(engine, nullptr);
  FreeStatus(&status);
}

TEST(TiForthCapiSmokeTest, ProjectionFilterPipeline) {
  tiforth_engine_options_t options{};
  options.abi_version = TIFORTH_C_ABI_VERSION;

  tiforth_engine_t* engine = nullptr;
  AssertOkAndFree(tiforth_engine_create(&options, &engine));
  ASSERT_NE(engine, nullptr);

  tiforth_pipeline_t* pipeline = nullptr;
  AssertOkAndFree(tiforth_pipeline_create(engine, &pipeline));
  ASSERT_NE(pipeline, nullptr);

  tiforth_expr_t* x = nullptr;
  tiforth_expr_t* y = nullptr;
  tiforth_expr_t* lit1 = nullptr;
  tiforth_expr_t* predicate = nullptr;
  tiforth_expr_t* sum = nullptr;
  AssertOkAndFree(tiforth_expr_field_ref_index(/*index=*/0, &x));
  AssertOkAndFree(tiforth_expr_field_ref_index(/*index=*/1, &y));
  AssertOkAndFree(tiforth_expr_literal_int32(1, &lit1));
  {
    const tiforth_expr_t* args[] = {x, lit1};
    AssertOkAndFree(tiforth_expr_call("greater", args, /*num_args=*/2, &predicate));
  }
  {
    const tiforth_expr_t* args[] = {x, y};
    AssertOkAndFree(tiforth_expr_call("add", args, /*num_args=*/2, &sum));
  }

  AssertOkAndFree(tiforth_pipeline_append_filter(pipeline, predicate));
  const tiforth_projection_expr_t proj_exprs[] = {
      {.name = "x", .expr = x},
      {.name = "sum", .expr = sum},
  };
  AssertOkAndFree(tiforth_pipeline_append_projection(pipeline, proj_exprs, /*num_exprs=*/2));

  tiforth_task_t* task = nullptr;
  AssertOkAndFree(tiforth_pipeline_create_task(pipeline, &task));
  ASSERT_NE(task, nullptr);

  tiforth_task_state_t state = TIFORTH_TASK_BLOCKED;
  AssertOkAndFree(tiforth_task_step(task, &state));
  ASSERT_EQ(state, TIFORTH_TASK_NEED_INPUT);

  auto schema = arrow::schema({arrow::field("x", arrow::int32()), arrow::field("y", arrow::int32())});
  arrow::Int32Builder x_builder;
  ASSERT_OK(x_builder.AppendValues({1, 2, 3}));
  std::shared_ptr<arrow::Array> x_array;
  ASSERT_OK(x_builder.Finish(&x_array));

  arrow::Int32Builder y_builder;
  ASSERT_OK(y_builder.AppendValues({10, 20, 30}));
  std::shared_ptr<arrow::Array> y_array;
  ASSERT_OK(y_builder.Finish(&y_array));

  auto batch = arrow::RecordBatch::Make(schema, /*num_rows=*/3, {x_array, y_array});
  ASSERT_NE(batch, nullptr);

  ArrowArray c_array{};
  ArrowSchema c_schema{};
  ASSERT_OK(arrow::ExportRecordBatch(*batch, &c_array, &c_schema));

  AssertOkAndFree(tiforth_task_push_input_batch(task, &c_schema, &c_array));
  AssertOkAndFree(tiforth_task_close_input(task));

  AssertOkAndFree(tiforth_task_step(task, &state));
  ASSERT_EQ(state, TIFORTH_TASK_HAS_OUTPUT);

  ArrowArray out_array{};
  ArrowSchema out_schema{};
  AssertOkAndFree(tiforth_task_pull_output_batch(task, &out_schema, &out_array));
  ASSERT_OK_AND_ASSIGN(auto out_batch, arrow::ImportRecordBatch(&out_array, &out_schema));
  ASSERT_NE(out_batch, nullptr);

  ASSERT_EQ(out_batch->num_columns(), 2);
  ASSERT_EQ(out_batch->num_rows(), 2);
  ASSERT_EQ(out_batch->schema()->field(0)->name(), "x");
  ASSERT_EQ(out_batch->schema()->field(1)->name(), "sum");

  arrow::Int32Builder expect_x_builder;
  ASSERT_OK(expect_x_builder.AppendValues({2, 3}));
  std::shared_ptr<arrow::Array> expect_x;
  ASSERT_OK(expect_x_builder.Finish(&expect_x));

  arrow::Int32Builder expect_sum_builder;
  ASSERT_OK(expect_sum_builder.AppendValues({22, 33}));
  std::shared_ptr<arrow::Array> expect_sum;
  ASSERT_OK(expect_sum_builder.Finish(&expect_sum));

  ASSERT_TRUE(expect_x->Equals(*out_batch->column(0)));
  ASSERT_TRUE(expect_sum->Equals(*out_batch->column(1)));

  AssertOkAndFree(tiforth_task_step(task, &state));
  ASSERT_EQ(state, TIFORTH_TASK_FINISHED);

  tiforth_task_destroy(task);
  tiforth_pipeline_destroy(pipeline);
  tiforth_engine_destroy(engine);

  tiforth_expr_destroy(x);
  tiforth_expr_destroy(y);
  tiforth_expr_destroy(lit1);
  tiforth_expr_destroy(predicate);
  tiforth_expr_destroy(sum);
}

TEST(TiForthCapiSmokeTest, PushInputBatchConsumesOnInvalidTask) {
  auto schema = arrow::schema({arrow::field("x", arrow::int32()), arrow::field("y", arrow::int32())});
  arrow::Int32Builder x_builder;
  ASSERT_OK(x_builder.Append(1));
  std::shared_ptr<arrow::Array> x_array;
  ASSERT_OK(x_builder.Finish(&x_array));

  arrow::Int32Builder y_builder;
  ASSERT_OK(y_builder.Append(2));
  std::shared_ptr<arrow::Array> y_array;
  ASSERT_OK(y_builder.Finish(&y_array));

  auto batch = arrow::RecordBatch::Make(schema, /*num_rows=*/1, {x_array, y_array});
  ASSERT_NE(batch, nullptr);

  ArrowArray c_array{};
  ArrowSchema c_schema{};
  ASSERT_OK(arrow::ExportRecordBatch(*batch, &c_array, &c_schema));
  ASSERT_NE(c_array.release, nullptr);
  ASSERT_NE(c_schema.release, nullptr);

  tiforth_status_t status = tiforth_task_push_input_batch(/*task=*/nullptr, &c_schema, &c_array);
  EXPECT_EQ(status.code, TIFORTH_STATUS_INVALID_ARGUMENT);
  FreeStatus(&status);

  // Deterministic ownership: tiforth consumes Arrow C structs even on error.
  EXPECT_EQ(c_array.release, nullptr);
  EXPECT_EQ(c_schema.release, nullptr);
}

TEST(TiForthCapiSmokeTest, ProjectionFilterPipelineFromInputStream) {
  tiforth_engine_options_t options{};
  options.abi_version = TIFORTH_C_ABI_VERSION;

  tiforth_engine_t* engine = nullptr;
  AssertOkAndFree(tiforth_engine_create(&options, &engine));
  ASSERT_NE(engine, nullptr);

  tiforth_pipeline_t* pipeline = nullptr;
  AssertOkAndFree(tiforth_pipeline_create(engine, &pipeline));
  ASSERT_NE(pipeline, nullptr);

  tiforth_expr_t* x = nullptr;
  tiforth_expr_t* y = nullptr;
  tiforth_expr_t* lit1 = nullptr;
  tiforth_expr_t* predicate = nullptr;
  tiforth_expr_t* sum = nullptr;
  AssertOkAndFree(tiforth_expr_field_ref_index(/*index=*/0, &x));
  AssertOkAndFree(tiforth_expr_field_ref_index(/*index=*/1, &y));
  AssertOkAndFree(tiforth_expr_literal_int32(1, &lit1));
  {
    const tiforth_expr_t* args[] = {x, lit1};
    AssertOkAndFree(tiforth_expr_call("greater", args, /*num_args=*/2, &predicate));
  }
  {
    const tiforth_expr_t* args[] = {x, y};
    AssertOkAndFree(tiforth_expr_call("add", args, /*num_args=*/2, &sum));
  }

  AssertOkAndFree(tiforth_pipeline_append_filter(pipeline, predicate));
  const tiforth_projection_expr_t proj_exprs[] = {
      {.name = "x", .expr = x},
      {.name = "sum", .expr = sum},
  };
  AssertOkAndFree(tiforth_pipeline_append_projection(pipeline, proj_exprs, /*num_exprs=*/2));

  tiforth_task_t* task = nullptr;
  AssertOkAndFree(tiforth_pipeline_create_task(pipeline, &task));
  ASSERT_NE(task, nullptr);

  auto schema = arrow::schema({arrow::field("x", arrow::int32()), arrow::field("y", arrow::int32())});
  arrow::Int32Builder x_builder;
  ASSERT_OK(x_builder.AppendValues({1, 2, 3}));
  std::shared_ptr<arrow::Array> x_array;
  ASSERT_OK(x_builder.Finish(&x_array));

  arrow::Int32Builder y_builder;
  ASSERT_OK(y_builder.AppendValues({10, 20, 30}));
  std::shared_ptr<arrow::Array> y_array;
  ASSERT_OK(y_builder.Finish(&y_array));

  auto batch = arrow::RecordBatch::Make(schema, /*num_rows=*/3, {x_array, y_array});
  ASSERT_NE(batch, nullptr);

  ASSERT_OK_AND_ASSIGN(auto reader, arrow::RecordBatchReader::Make({batch}, schema));
  ArrowArrayStream in_stream{};
  ASSERT_OK(arrow::ExportRecordBatchReader(reader, &in_stream));

  AssertOkAndFree(tiforth_task_set_input_stream(task, &in_stream));
  EXPECT_EQ(in_stream.release, nullptr);

  tiforth_task_state_t state = TIFORTH_TASK_BLOCKED;
  while (true) {
    AssertOkAndFree(tiforth_task_step(task, &state));
    if (state == TIFORTH_TASK_HAS_OUTPUT) {
      break;
    }
    ASSERT_NE(state, TIFORTH_TASK_NEED_INPUT);
    ASSERT_NE(state, TIFORTH_TASK_FINISHED);
  }

  ArrowArray out_array{};
  ArrowSchema out_schema{};
  AssertOkAndFree(tiforth_task_pull_output_batch(task, &out_schema, &out_array));
  ASSERT_OK_AND_ASSIGN(auto out_batch, arrow::ImportRecordBatch(&out_array, &out_schema));
  ASSERT_NE(out_batch, nullptr);
  ASSERT_EQ(out_batch->num_columns(), 2);
  ASSERT_EQ(out_batch->num_rows(), 2);

  AssertOkAndFree(tiforth_task_step(task, &state));
  ASSERT_EQ(state, TIFORTH_TASK_FINISHED);

  tiforth_task_destroy(task);
  tiforth_pipeline_destroy(pipeline);
  tiforth_engine_destroy(engine);

  tiforth_expr_destroy(x);
  tiforth_expr_destroy(y);
  tiforth_expr_destroy(lit1);
  tiforth_expr_destroy(predicate);
  tiforth_expr_destroy(sum);
}

TEST(TiForthCapiSmokeTest, ExportOutputStreamDrainsTask) {
  tiforth_engine_options_t options{};
  options.abi_version = TIFORTH_C_ABI_VERSION;

  tiforth_engine_t* engine = nullptr;
  AssertOkAndFree(tiforth_engine_create(&options, &engine));
  ASSERT_NE(engine, nullptr);

  tiforth_pipeline_t* pipeline = nullptr;
  AssertOkAndFree(tiforth_pipeline_create(engine, &pipeline));
  ASSERT_NE(pipeline, nullptr);

  tiforth_expr_t* x = nullptr;
  AssertOkAndFree(tiforth_expr_field_ref_index(/*index=*/0, &x));

  const tiforth_projection_expr_t proj_exprs[] = {
      {.name = "x", .expr = x},
  };
  AssertOkAndFree(tiforth_pipeline_append_projection(pipeline, proj_exprs, /*num_exprs=*/1));

  tiforth_task_t* task = nullptr;
  AssertOkAndFree(tiforth_pipeline_create_task(pipeline, &task));
  ASSERT_NE(task, nullptr);

  auto schema = arrow::schema({arrow::field("x", arrow::int32())});
  arrow::Int32Builder x_builder;
  ASSERT_OK(x_builder.AppendValues({1, 2, 3}));
  std::shared_ptr<arrow::Array> x_array;
  ASSERT_OK(x_builder.Finish(&x_array));
  auto batch = arrow::RecordBatch::Make(schema, /*num_rows=*/3, {x_array});
  ASSERT_NE(batch, nullptr);

  ASSERT_OK_AND_ASSIGN(auto reader, arrow::RecordBatchReader::Make({batch}, schema));
  ArrowArrayStream in_stream{};
  ASSERT_OK(arrow::ExportRecordBatchReader(reader, &in_stream));
  AssertOkAndFree(tiforth_task_set_input_stream(task, &in_stream));

  ArrowArrayStream out_stream{};
  AssertOkAndFree(tiforth_task_export_output_stream(task, &out_stream));
  ASSERT_NE(out_stream.release, nullptr);

  ASSERT_OK_AND_ASSIGN(auto out_reader, arrow::ImportRecordBatchReader(&out_stream));
  std::shared_ptr<arrow::RecordBatch> out_batch;
  ASSERT_OK(out_reader->ReadNext(&out_batch));
  ASSERT_NE(out_batch, nullptr);
  ASSERT_EQ(out_batch->num_columns(), 1);
  ASSERT_EQ(out_batch->num_rows(), 3);
  ASSERT_OK(out_reader->ReadNext(&out_batch));
  ASSERT_EQ(out_batch, nullptr);

  tiforth_task_state_t state = TIFORTH_TASK_BLOCKED;
  AssertOkAndFree(tiforth_task_step(task, &state));
  ASSERT_EQ(state, TIFORTH_TASK_FINISHED);

  tiforth_task_destroy(task);
  tiforth_pipeline_destroy(pipeline);
  tiforth_engine_destroy(engine);
  tiforth_expr_destroy(x);
}
