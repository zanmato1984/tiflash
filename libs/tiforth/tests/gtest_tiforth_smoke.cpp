#include <arrow/result.h>
#include <arrow/status.h>

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

  ARROW_ASSIGN_OR_RAISE(auto state, task->Step());
  if (state != TaskState::kFinished) {
    return arrow::Status::Invalid("expected TaskState::kFinished");
  }

  return arrow::Status::OK();
}

}  // namespace

TEST(TiForthSmokeTest, Lifecycle) {
  auto status = RunSmoke();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

}  // namespace tiforth
