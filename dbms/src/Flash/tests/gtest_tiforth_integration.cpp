#include <gtest/gtest.h>

#if defined(TIFLASH_ENABLE_TIFORTH)

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/result.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>

#include "tiforth/engine.h"
#include "tiforth/pipeline.h"
#include "tiforth/task.h"

namespace {

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
  arrow::Int32Builder values;
  ARROW_RETURN_NOT_OK(values.AppendValues({1, 2, 3}));
  std::shared_ptr<arrow::Array> array;
  ARROW_RETURN_NOT_OK(values.Finish(&array));
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

}  // namespace

TEST(TiForthIntegrationSmokeTest, Lifecycle) {
  auto status = RunTiForthSmoke();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

#else

TEST(TiForthIntegrationSmokeTest, Disabled) {
  GTEST_SKIP() << "TiForth integration is disabled (ENABLE_TIFORTH=OFF)";
}

#endif
