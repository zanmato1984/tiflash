#include <gtest/gtest.h>

#if defined(TIFLASH_ENABLE_TIFORTH)

#include <arrow/result.h>
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

  ARROW_ASSIGN_OR_RAISE(auto state, task->Step());
  if (state != tiforth::TaskState::kFinished) {
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

