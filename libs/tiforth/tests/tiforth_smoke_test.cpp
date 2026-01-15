#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/record_batch.h>
#include <arrow/testing/gtest_util.h>

#include <arrow/array.h>
#include <arrow/builder.h>

#include <gtest/gtest.h>

#include "tiforth/engine.h"
#include "tiforth/pipeline.h"
#include "tiforth/task.h"

namespace tiforth {

namespace {

arrow::Status RunSmoke() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, PipelineBuilder::Create(engine.get()));
  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, pipeline->CreateTask());

  ARROW_ASSIGN_OR_RAISE(auto initial_state, task->Step());
  if (initial_state != TaskState::kNeedInput) {
    return arrow::Status::Invalid("expected TaskState::kNeedInput");
  }

  auto schema = arrow::schema({arrow::field("x", arrow::int32())});
  arrow::Int32Builder values;
  ARROW_RETURN_NOT_OK(values.AppendValues({1, 2, 3}));
  std::shared_ptr<arrow::Array> array;
  ARROW_RETURN_NOT_OK(values.Finish(&array));

  auto batch = arrow::RecordBatch::Make(schema, /*num_rows=*/3, {array});
  ARROW_RETURN_NOT_OK(task->PushInput(batch));
  ARROW_RETURN_NOT_OK(task->CloseInput());

  ARROW_ASSIGN_OR_RAISE(auto state, task->Step());
  if (state != TaskState::kHasOutput) {
    return arrow::Status::Invalid("expected TaskState::kHasOutput");
  }

  ARROW_ASSIGN_OR_RAISE(auto out, task->PullOutput());
  if (out.get() != batch.get()) {
    return arrow::Status::Invalid("expected pass-through output batch");
  }

  ARROW_ASSIGN_OR_RAISE(auto final_state, task->Step());
  if (final_state != TaskState::kFinished) {
    return arrow::Status::Invalid("expected TaskState::kFinished after draining output");
  }

  return arrow::Status::OK();
}

}  // namespace

TEST(TiForthSmokeTest, Lifecycle) {
  auto status = RunSmoke();
  ASSERT_OK(status);
}

}  // namespace tiforth
