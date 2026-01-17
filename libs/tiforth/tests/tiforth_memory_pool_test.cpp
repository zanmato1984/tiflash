#include <arrow/memory_pool.h>
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
#include "tiforth/operators/sort.h"
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

arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeBatch(
    const std::vector<std::optional<int32_t>>& xs) {
  auto schema = arrow::schema({arrow::field("x", arrow::int32())});
  ARROW_ASSIGN_OR_RAISE(auto x_array, MakeInt32Array(xs));
  return arrow::RecordBatch::Make(schema, static_cast<int64_t>(xs.size()), {x_array});
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeBatch2(std::string a_name,
                                                              std::string b_name,
                                                              const std::vector<std::optional<int32_t>>& a,
                                                              const std::vector<std::optional<int32_t>>& b) {
  if (a.size() != b.size()) {
    return arrow::Status::Invalid("batch column size mismatch");
  }
  auto schema = arrow::schema({arrow::field(std::move(a_name), arrow::int32()),
                               arrow::field(std::move(b_name), arrow::int32())});
  ARROW_ASSIGN_OR_RAISE(auto a_array, MakeInt32Array(a));
  ARROW_ASSIGN_OR_RAISE(auto b_array, MakeInt32Array(b));
  return arrow::RecordBatch::Make(schema, static_cast<int64_t>(a.size()), {a_array, b_array});
}

arrow::Result<std::shared_ptr<arrow::Array>> MakeBinaryArray(
    const std::vector<std::optional<std::string>>& values) {
  arrow::BinaryBuilder builder;
  for (const auto& value : values) {
    if (value.has_value()) {
      ARROW_RETURN_NOT_OK(builder.Append(*value));
    } else {
      ARROW_RETURN_NOT_OK(builder.AppendNull());
    }
  }
  std::shared_ptr<arrow::Array> out;
  ARROW_RETURN_NOT_OK(builder.Finish(&out));
  return out;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeBinaryBatch(
    int32_t collation_id, const std::vector<std::optional<std::string>>& xs) {
  auto field = arrow::field("x", arrow::binary());
  LogicalType logical;
  logical.id = LogicalTypeId::kString;
  logical.collation_id = collation_id;
  ARROW_ASSIGN_OR_RAISE(field, WithLogicalTypeMetadata(field, logical));
  auto schema = arrow::schema({field});

  ARROW_ASSIGN_OR_RAISE(auto x_array, MakeBinaryArray(xs));
  return arrow::RecordBatch::Make(schema, static_cast<int64_t>(xs.size()), {x_array});
}

arrow::Status RunMemoryPoolSmoke() {
  arrow::ProxyMemoryPool pool(arrow::default_memory_pool());

  EngineOptions options;
  options.memory_pool = &pool;
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(options));
  ARROW_ASSIGN_OR_RAISE(auto builder, PipelineBuilder::Create(engine.get()));
  const auto* eng = engine.get();

  std::vector<SortKey> keys = {SortKey{.name = "x", .ascending = true, .nulls_first = false}};
  ARROW_RETURN_NOT_OK(builder->AppendTransform([keys, eng]() -> arrow::Result<TransformOpPtr> {
    return std::make_unique<SortTransformOp>(eng, keys);
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

arrow::Result<int64_t> RunCollatedSortBytesAllocated(int32_t collation_id) {
  arrow::ProxyMemoryPool pool(arrow::default_memory_pool());

  EngineOptions options;
  options.memory_pool = &pool;
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(options));
  ARROW_ASSIGN_OR_RAISE(auto builder, PipelineBuilder::Create(engine.get()));
  const auto* eng = engine.get();

  std::vector<SortKey> keys = {SortKey{.name = "x", .ascending = true, .nulls_first = false}};
  ARROW_RETURN_NOT_OK(builder->AppendTransform([keys, eng]() -> arrow::Result<TransformOpPtr> {
    return std::make_unique<SortTransformOp>(eng, keys);
  }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, pipeline->CreateTask());

  ARROW_ASSIGN_OR_RAISE(auto initial_state, task->Step());
  if (initial_state != TaskState::kNeedInput) {
    return arrow::Status::Invalid("expected TaskState::kNeedInput");
  }

  std::vector<std::optional<std::string>> values;
  values.reserve(128);
  for (int i = 0; i < 128; ++i) {
    values.push_back(std::string(64, static_cast<char>('a' + (i % 26))));
  }
  ARROW_ASSIGN_OR_RAISE(auto batch, MakeBinaryBatch(collation_id, values));
  ARROW_RETURN_NOT_OK(task->PushInput(batch));
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

  return pool.total_bytes_allocated();
}

arrow::Status RunHashJoinMemoryPoolSmoke() {
  arrow::ProxyMemoryPool pool(arrow::default_memory_pool());

  EngineOptions options;
  options.memory_pool = &pool;
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(options));
  ARROW_ASSIGN_OR_RAISE(auto builder, PipelineBuilder::Create(engine.get()));
  const auto* eng = engine.get();

  ARROW_ASSIGN_OR_RAISE(auto build,
                        MakeBatch2("k", "bv", {1, 2, 2, std::nullopt}, {100, 200, 201, 999}));
  std::vector<std::shared_ptr<arrow::RecordBatch>> build_batches = {std::move(build)};
  JoinKey key{.left = {"k"}, .right = {"k"}};
  ARROW_RETURN_NOT_OK(builder->AppendTransform(
      [eng, build_batches, key]() -> arrow::Result<TransformOpPtr> {
        return std::make_unique<HashJoinTransformOp>(eng, build_batches, key);
      }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, pipeline->CreateTask());

  ARROW_ASSIGN_OR_RAISE(auto initial_state, task->Step());
  if (initial_state != TaskState::kNeedInput) {
    return arrow::Status::Invalid("expected TaskState::kNeedInput");
  }

  ARROW_ASSIGN_OR_RAISE(auto probe, MakeBatch2("k", "pv", {2, 1, 3, std::nullopt}, {20, 10, 30, 0}));
  ARROW_RETURN_NOT_OK(task->PushInput(probe));
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

TEST(TiForthMemoryPoolTest, CollatedSortUsesEnginePoolForSortKeys) {
  auto bytes_binary = RunCollatedSortBytesAllocated(/*collation_id=*/63);
  ASSERT_TRUE(bytes_binary.ok()) << bytes_binary.status().ToString();

  auto bytes_general_ci = RunCollatedSortBytesAllocated(/*collation_id=*/33);
  ASSERT_TRUE(bytes_general_ci.ok()) << bytes_general_ci.status().ToString();

  ASSERT_GT(*bytes_general_ci, *bytes_binary + 8192)
      << "expected collated sort to allocate additional sort-key memory through the engine pool";
}

TEST(TiForthMemoryPoolTest, HashJoinUsesEnginePool) {
  auto status = RunHashJoinMemoryPoolSmoke();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

}  // namespace tiforth
