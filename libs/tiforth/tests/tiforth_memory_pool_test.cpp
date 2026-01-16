#include <arrow/memory_pool.h>
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
#include "tiforth/operators/sort.h"
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
    const std::vector<std::optional<int32_t>>& xs) {
  auto schema = arrow::schema({arrow::field("x", arrow::int32())});
  ARROW_ASSIGN_OR_RAISE(auto x_array, MakeInt32Array(xs));
  return arrow::RecordBatch::Make(schema, static_cast<int64_t>(xs.size()), {x_array});
}

arrow::Status RunMemoryPoolSmoke() {
  arrow::ProxyMemoryPool pool(arrow::default_memory_pool());

  EngineOptions options;
  options.memory_pool = &pool;
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(options));
  ARROW_ASSIGN_OR_RAISE(auto builder, PipelineBuilder::Create(engine.get()));

  std::vector<SortKey> keys = {SortKey{.name = "x", .ascending = true, .nulls_first = false}};
  auto* mp = engine->memory_pool();
  ARROW_RETURN_NOT_OK(builder->AppendTransform([keys, mp]() -> arrow::Result<TransformOpPtr> {
    return std::make_unique<SortTransformOp>(keys, mp);
  }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, pipeline->CreateTask());

  ARROW_ASSIGN_OR_RAISE(auto initial_state, task->Step());
  if (initial_state != TaskState::kNeedInput) {
    return arrow::Status::Invalid("expected TaskState::kNeedInput");
  }

  ARROW_ASSIGN_OR_RAISE(auto batch0, MakeBatch({3, 1}));
  ARROW_RETURN_NOT_OK(task->PushInput(batch0));
  ARROW_ASSIGN_OR_RAISE(auto batch1, MakeBatch({std::nullopt, 2}));
  ARROW_RETURN_NOT_OK(task->PushInput(batch1));
  ARROW_RETURN_NOT_OK(task->CloseInput());

  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto state, task->Step());
    if (state == TaskState::kFinished) {
      break;
    }
    if (state == TaskState::kNeedInput) {
      continue;
    }
    if (state != TaskState::kHasOutput) {
      return arrow::Status::Invalid("unexpected task state");
    }
    ARROW_ASSIGN_OR_RAISE(auto out, task->PullOutput());
    (void)out;
  }

  if (pool.num_allocations() <= 0 || pool.total_bytes_allocated() <= 0) {
    return arrow::Status::Invalid("expected allocations to go through the provided memory pool");
  }
  return arrow::Status::OK();
}

}  // namespace

TEST(TiForthMemoryPoolTest, SortUsesEnginePool) {
  auto status = RunMemoryPoolSmoke();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

}  // namespace tiforth
