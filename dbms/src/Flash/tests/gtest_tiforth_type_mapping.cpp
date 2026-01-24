#include <gtest/gtest.h>

#if defined(TIFLASH_ENABLE_TIFORTH)

#include <Common/MyTime.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeMyDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Flash/TiForth/ArrowTypeMapping.h>
#include <Flash/TiForth/TaskGroupRunner.h>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/util/decimal.h>

#include <cassert>
#include <functional>
#include <optional>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "tiforth/engine.h"
#include "tiforth/expr.h"
#include "tiforth/operators/hash_agg.h"
#include "tiforth/operators/hash_join.h"
#include "tiforth/pipeline/logical_pipeline.h"
#include "tiforth/pipeline/op/op.h"
#include "tiforth/pipeline/task_groups.h"

namespace DB::tests {

namespace {

Block MakeInputBlockForAgg() {
  // s: Nullable(String) with padding BIN collation; d: Decimal256(40,2); t: MyDateTime(6)
  auto s_nested = ColumnString::create();
  auto s_null = ColumnUInt8::create();
  auto append_s = [&](std::optional<std::string_view> v) {
    if (v.has_value()) {
      s_nested->insertData(v->data(), v->size());
      s_null->insert(Field(static_cast<UInt64>(0)));
    } else {
      s_nested->insertDefault();
      s_null->insert(Field(static_cast<UInt64>(1)));
    }
  };
  append_s("a");
  append_s("a ");
  append_s("b");
  append_s(std::nullopt);
  auto s_col = ColumnNullable::create(std::move(s_nested), std::move(s_null));
  const auto s_type = makeNullable(std::make_shared<DataTypeString>());

  const auto d_type = createDecimal(/*prec=*/40, /*scale=*/2);
  auto d_mut = d_type->createColumn();
  auto * d_col = typeid_cast<ColumnDecimal<Decimal256> *>(d_mut.get());
  assert(d_col != nullptr);
  d_col->insert(Decimal256(Int256(123)));
  d_col->insert(Decimal256(Int256(123)));
  d_col->insert(Decimal256(Int256(456)));
  d_col->insert(Decimal256(Int256(456)));

  const auto t_type = std::make_shared<DataTypeMyDateTime>(6);
  auto t_col = ColumnUInt64::create();
  // Use raw packed representation (no timezone); duplicates to validate hash key handling.
  const UInt64 dt1 = MyDateTime(2024, 1, 1, 0, 0, 0, 0).toPackedUInt();
  const UInt64 dt2 = MyDateTime(2024, 1, 2, 3, 4, 5, 0).toPackedUInt();
  const UInt64 dt3 = MyDateTime(2024, 1, 3, 0, 0, 0, 0).toPackedUInt();
  t_col->insert(Field(dt1));
  t_col->insert(Field(dt1));
  t_col->insert(Field(dt2));
  t_col->insert(Field(dt3));

  ColumnsWithTypeAndName cols;
  cols.emplace_back(std::move(s_col), s_type, "s");
  cols.emplace_back(std::move(d_mut), d_type, "d");
  cols.emplace_back(std::move(t_col), t_type, "t");
  return Block(std::move(cols));
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> ToArrow(const Block & block) {
  std::unordered_map<String, TiForth::ColumnOptions> options;
  // UTF8MB4_BIN (PAD SPACE) collation.
  options.emplace("s", TiForth::ColumnOptions{.collation_id = 46});
  return TiForth::toArrowRecordBatch(block, options, arrow::default_memory_pool());
}

class VectorSourceOp final : public tiforth::pipeline::SourceOp {
 public:
  explicit VectorSourceOp(std::vector<std::shared_ptr<arrow::RecordBatch>> batches_)
      : batches(std::move(batches_)) {}

  tiforth::pipeline::PipelineSource Source(const tiforth::pipeline::PipelineContext &) override {
    return [this](const tiforth::pipeline::PipelineContext &, const tiforth::task::TaskContext &,
                  tiforth::pipeline::ThreadId thread_id) -> tiforth::pipeline::OpResult {
      if (thread_id != 0) {
        return arrow::Status::Invalid("VectorSourceOp only supports thread_id=0");
      }
      if (next >= batches.size()) {
        return tiforth::pipeline::OpOutput::Finished();
      }
      auto batch = batches[next++];
      if (batch == nullptr) {
        return arrow::Status::Invalid("source batch must not be null");
      }
      return tiforth::pipeline::OpOutput::SourcePipeHasMore(std::move(batch));
    };
  }

 private:
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  std::size_t next = 0;
};

class CollectSinkOp final : public tiforth::pipeline::SinkOp {
 public:
  explicit CollectSinkOp(std::vector<std::shared_ptr<arrow::RecordBatch>> * outputs_)
      : outputs(outputs_) {}

  tiforth::pipeline::PipelineSink Sink(const tiforth::pipeline::PipelineContext &) override {
    return [this](const tiforth::pipeline::PipelineContext &, const tiforth::task::TaskContext &,
                  tiforth::pipeline::ThreadId thread_id,
                  std::optional<tiforth::pipeline::Batch> input) -> tiforth::pipeline::OpResult {
      if (thread_id != 0) {
        return arrow::Status::Invalid("CollectSinkOp only supports thread_id=0");
      }
      if (outputs == nullptr) {
        return arrow::Status::Invalid("outputs must not be null");
      }
      if (!input.has_value()) {
        return tiforth::pipeline::OpOutput::PipeSinkNeedsMore();
      }
      auto batch = std::move(*input);
      if (batch != nullptr) {
        outputs->push_back(std::move(batch));
      }
      return tiforth::pipeline::OpOutput::PipeSinkNeedsMore();
    };
  }

 private:
  std::vector<std::shared_ptr<arrow::RecordBatch>> * outputs = nullptr;
};

arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>> RunPipeline(
    std::shared_ptr<arrow::RecordBatch> input,
    std::vector<std::function<arrow::Result<std::unique_ptr<tiforth::pipeline::PipeOp>>(const tiforth::Engine*)>>
        transforms) {
  if (input == nullptr) {
    return arrow::Status::Invalid("input batch must not be null");
  }
  ARROW_ASSIGN_OR_RAISE(auto engine, tiforth::Engine::Create(tiforth::EngineOptions{}));

  std::vector<std::unique_ptr<tiforth::pipeline::PipeOp>> pipe_ops;
  pipe_ops.reserve(transforms.size());
  for (auto & factory : transforms) {
    if (factory == nullptr) {
      return arrow::Status::Invalid("pipeline factory must not be null");
    }
    ARROW_ASSIGN_OR_RAISE(auto op, factory(engine.get()));
    if (op == nullptr) {
      return arrow::Status::Invalid("pipeline factory returned null op");
    }
    pipe_ops.push_back(std::move(op));
  }

  auto source_op = std::make_unique<VectorSourceOp>(std::vector<std::shared_ptr<arrow::RecordBatch>>{input});

  std::vector<std::shared_ptr<arrow::RecordBatch>> outputs;
  auto sink_op = std::make_unique<CollectSinkOp>(&outputs);

  tiforth::pipeline::LogicalPipeline::Channel channel;
  channel.source_op = source_op.get();
  channel.pipe_ops.reserve(pipe_ops.size());
  for (const auto & op : pipe_ops) {
    if (op == nullptr) {
      return arrow::Status::Invalid("pipe op must not be null");
    }
    channel.pipe_ops.push_back(op.get());
  }

  tiforth::pipeline::LogicalPipeline logical_pipeline{"TypeMappingPipeline", {std::move(channel)},
                                                      sink_op.get()};
  ARROW_ASSIGN_OR_RAISE(
      auto task_groups,
      tiforth::pipeline::CompileToTaskGroups(tiforth::pipeline::PipelineContext{}, logical_pipeline, /*dop=*/1));
  const auto task_ctx = TiForth::MakeTaskContext();
  ARROW_RETURN_NOT_OK(TiForth::RunTaskGroupsToCompletion(task_groups, task_ctx));

  return outputs;
}

arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>> RunHashAgg(
    std::shared_ptr<arrow::RecordBatch> input,
    std::vector<tiforth::AggKey> keys,
    std::vector<tiforth::AggFunc> aggs) {
  if (input == nullptr) {
    return arrow::Status::Invalid("input batch must not be null");
  }
  ARROW_ASSIGN_OR_RAISE(auto engine, tiforth::Engine::Create(tiforth::EngineOptions{}));
  auto agg_state = std::make_shared<tiforth::HashAggState>(
      engine.get(),
      std::move(keys),
      std::move(aggs),
      /*grouper_factory=*/tiforth::HashAggState::GrouperFactory{},
      arrow::default_memory_pool(),
      /*dop=*/1);

  // Build stage.
  {
    auto source_op = std::make_unique<VectorSourceOp>(std::vector<std::shared_ptr<arrow::RecordBatch>>{input});
    auto sink_op = std::make_unique<tiforth::HashAggSinkOp>(agg_state);

    tiforth::pipeline::LogicalPipeline::Channel channel;
    channel.source_op = source_op.get();
    channel.pipe_ops = {};

    tiforth::pipeline::LogicalPipeline logical_pipeline{"TypeMappingHashAggBuild", {std::move(channel)},
                                                        sink_op.get()};
    ARROW_ASSIGN_OR_RAISE(
        auto task_groups,
        tiforth::pipeline::CompileToTaskGroups(tiforth::pipeline::PipelineContext{}, logical_pipeline, /*dop=*/1));
    const auto task_ctx = TiForth::MakeTaskContext();
    ARROW_RETURN_NOT_OK(TiForth::RunTaskGroupsToCompletion(task_groups, task_ctx));
  }

  // Result stage.
  std::vector<std::shared_ptr<arrow::RecordBatch>> outputs;
  {
    auto source_op =
        std::make_unique<tiforth::HashAggResultSourceOp>(agg_state, /*max_output_rows=*/1 << 30);
    auto sink_op = std::make_unique<CollectSinkOp>(&outputs);

    tiforth::pipeline::LogicalPipeline::Channel channel;
    channel.source_op = source_op.get();
    channel.pipe_ops = {};

    tiforth::pipeline::LogicalPipeline logical_pipeline{"TypeMappingHashAggResult", {std::move(channel)},
                                                        sink_op.get()};
    ARROW_ASSIGN_OR_RAISE(
        auto task_groups,
        tiforth::pipeline::CompileToTaskGroups(tiforth::pipeline::PipelineContext{}, logical_pipeline, /*dop=*/1));
    const auto task_ctx = TiForth::MakeTaskContext();
    ARROW_RETURN_NOT_OK(TiForth::RunTaskGroupsToCompletion(task_groups, task_ctx));
  }

  return outputs;
}

arrow::Status RunTypeMappingAndOps() {
  ARROW_ASSIGN_OR_RAISE(auto input, ToArrow(MakeInputBlockForAgg()));

  // Validate schema contract (physical types + metadata).
  if (input->schema()->field(0)->type()->id() != arrow::Type::BINARY) {
    return arrow::Status::Invalid("expected s to map to Arrow binary");
  }
  if (input->schema()->field(1)->type()->id() != arrow::Type::DECIMAL256) {
    return arrow::Status::Invalid("expected d to map to Arrow decimal256");
  }
  if (input->schema()->field(2)->type()->id() != arrow::Type::UINT64) {
    return arrow::Status::Invalid("expected t to map to Arrow uint64");
  }

  // HashAgg: group by collated string (padding BIN): "a" == "a ".
  {
    std::vector<tiforth::AggKey> keys = {{"s", tiforth::MakeFieldRef("s")}};
    std::vector<tiforth::AggFunc> aggs;
    aggs.push_back({"cnt", "count_all", nullptr});
    ARROW_ASSIGN_OR_RAISE(auto outputs, RunHashAgg(input, keys, aggs));
    if (outputs.size() != 1) {
      return arrow::Status::Invalid("expected exactly 1 hash agg output batch");
    }
    const auto & out = outputs[0];
    if (out->num_columns() != 2 || out->num_rows() != 3) {
      return arrow::Status::Invalid("unexpected string hash agg output shape");
    }

    arrow::BinaryBuilder key_builder;
    ARROW_RETURN_NOT_OK(key_builder.Append(reinterpret_cast<const uint8_t *>("a"), 1));
    ARROW_RETURN_NOT_OK(key_builder.Append(reinterpret_cast<const uint8_t *>("b"), 1));
    ARROW_RETURN_NOT_OK(key_builder.AppendNull());
    std::shared_ptr<arrow::Array> key_expect;
    ARROW_RETURN_NOT_OK(key_builder.Finish(&key_expect));

    arrow::UInt64Builder cnt_builder;
    ARROW_RETURN_NOT_OK(cnt_builder.AppendValues({2, 1, 1}));
    std::shared_ptr<arrow::Array> cnt_expect;
    ARROW_RETURN_NOT_OK(cnt_builder.Finish(&cnt_expect));

    if (!cnt_expect->Equals(*out->column(0)) || !key_expect->Equals(*out->column(1))) {
      return arrow::Status::Invalid("unexpected string hash agg values");
    }
  }

  // HashAgg: group by Decimal256(40,2).
  {
    std::vector<tiforth::AggKey> keys = {{"d", tiforth::MakeFieldRef("d")}};
    std::vector<tiforth::AggFunc> aggs;
    aggs.push_back({"cnt", "count_all", nullptr});
    ARROW_ASSIGN_OR_RAISE(auto outputs, RunHashAgg(input, keys, aggs));
    if (outputs.size() != 1) {
      return arrow::Status::Invalid("expected exactly 1 decimal hash agg output batch");
    }
    const auto & out = outputs[0];
    if (out->num_columns() != 2 || out->num_rows() != 2) {
      return arrow::Status::Invalid("unexpected decimal hash agg output shape");
    }

    auto dec_type = arrow::decimal256(/*precision=*/40, /*scale=*/2);
    arrow::Decimal256Builder key_builder(dec_type, arrow::default_memory_pool());
    ARROW_RETURN_NOT_OK(key_builder.Append(arrow::Decimal256(123)));
    ARROW_RETURN_NOT_OK(key_builder.Append(arrow::Decimal256(456)));
    std::shared_ptr<arrow::Array> key_expect;
    ARROW_RETURN_NOT_OK(key_builder.Finish(&key_expect));

    arrow::UInt64Builder cnt_builder;
    ARROW_RETURN_NOT_OK(cnt_builder.AppendValues({2, 2}));
    std::shared_ptr<arrow::Array> cnt_expect;
    ARROW_RETURN_NOT_OK(cnt_builder.Finish(&cnt_expect));

    if (!cnt_expect->Equals(*out->column(0)) || !key_expect->Equals(*out->column(1))) {
      return arrow::Status::Invalid("unexpected decimal hash agg values");
    }
  }

  // HashAgg: group by packed MyDateTime (uint64).
  {
    std::vector<tiforth::AggKey> keys = {{"t", tiforth::MakeFieldRef("t")}};
    std::vector<tiforth::AggFunc> aggs;
    aggs.push_back({"cnt", "count_all", nullptr});
    ARROW_ASSIGN_OR_RAISE(auto outputs, RunHashAgg(input, keys, aggs));
    if (outputs.size() != 1) {
      return arrow::Status::Invalid("expected exactly 1 datetime hash agg output batch");
    }
    const auto & out = outputs[0];
    if (out->num_columns() != 2 || out->num_rows() != 3) {
      return arrow::Status::Invalid("unexpected datetime hash agg output shape");
    }

    const auto t_array_any = input->column(2);
    const auto t_array = std::static_pointer_cast<arrow::UInt64Array>(t_array_any);
    arrow::UInt64Builder key_builder;
    ARROW_RETURN_NOT_OK(key_builder.Append(t_array->Value(0)));
    ARROW_RETURN_NOT_OK(key_builder.Append(t_array->Value(2)));
    ARROW_RETURN_NOT_OK(key_builder.Append(t_array->Value(3)));
    std::shared_ptr<arrow::Array> key_expect;
    ARROW_RETURN_NOT_OK(key_builder.Finish(&key_expect));

    arrow::UInt64Builder cnt_builder;
    ARROW_RETURN_NOT_OK(cnt_builder.AppendValues({2, 1, 1}));
    std::shared_ptr<arrow::Array> cnt_expect;
    ARROW_RETURN_NOT_OK(cnt_builder.Finish(&cnt_expect));

    if (!cnt_expect->Equals(*out->column(0)) || !key_expect->Equals(*out->column(1))) {
      return arrow::Status::Invalid("unexpected datetime hash agg values");
    }
  }

  // HashJoin: collated string key (padding BIN) + propagate decimal/datetime columns.
  {
    const auto build_block = []() -> Block {
      const auto s_type = std::make_shared<DataTypeString>();
      auto s_col = ColumnString::create();
      s_col->insertData("a", 1);
      s_col->insertData("b ", 2);

      const auto d_type = createDecimal(/*prec=*/40, /*scale=*/2);
      auto d_mut = d_type->createColumn();
      auto * d_col = typeid_cast<ColumnDecimal<Decimal256> *>(d_mut.get());
      assert(d_col != nullptr);
      d_col->insert(Decimal256(Int256(123)));
      d_col->insert(Decimal256(Int256(456)));

      const auto t_type = std::make_shared<DataTypeMyDateTime>(6);
      auto t_col = ColumnUInt64::create();
      const UInt64 dt1 = MyDateTime(2024, 1, 1, 0, 0, 0, 0).toPackedUInt();
      const UInt64 dt2 = MyDateTime(2024, 1, 2, 3, 4, 5, 0).toPackedUInt();
      t_col->insert(Field(dt1));
      t_col->insert(Field(dt2));

      const auto bv_type = std::make_shared<DataTypeInt32>();
      auto bv_col = ColumnInt32::create();
      bv_col->insert(Field(static_cast<Int64>(100)));
      bv_col->insert(Field(static_cast<Int64>(200)));

      ColumnsWithTypeAndName cols;
      cols.emplace_back(std::move(s_col), s_type, "s");
      cols.emplace_back(std::move(d_mut), d_type, "d");
      cols.emplace_back(std::move(t_col), t_type, "t");
      cols.emplace_back(std::move(bv_col), bv_type, "bv");
      return Block(std::move(cols));
    }();

    const auto probe_block = []() -> Block {
      const auto s_type = makeNullable(std::make_shared<DataTypeString>());
      auto s_nested = ColumnString::create();
      auto s_null = ColumnUInt8::create();
      s_nested->insertData("a ", 2);
      s_null->insert(Field(static_cast<UInt64>(0)));
      s_nested->insertData("b", 1);
      s_null->insert(Field(static_cast<UInt64>(0)));
      s_nested->insertData("c", 1);
      s_null->insert(Field(static_cast<UInt64>(0)));
      s_nested->insertDefault();
      s_null->insert(Field(static_cast<UInt64>(1)));
      auto s_col = ColumnNullable::create(std::move(s_nested), std::move(s_null));

      const auto pv_type = std::make_shared<DataTypeInt32>();
      auto pv_col = ColumnInt32::create();
      pv_col->insert(Field(static_cast<Int64>(10)));
      pv_col->insert(Field(static_cast<Int64>(20)));
      pv_col->insert(Field(static_cast<Int64>(30)));
      pv_col->insert(Field(static_cast<Int64>(40)));

      ColumnsWithTypeAndName cols;
      cols.emplace_back(std::move(s_col), s_type, "s");
      cols.emplace_back(std::move(pv_col), pv_type, "pv");
      return Block(std::move(cols));
    }();

    ARROW_ASSIGN_OR_RAISE(auto build_batch, ToArrow(build_block));
    ARROW_ASSIGN_OR_RAISE(auto probe_batch, ToArrow(probe_block));

    tiforth::JoinKey key{.left = {"s"}, .right = {"s"}};
    std::vector<std::shared_ptr<arrow::RecordBatch>> build_batches;
    build_batches.push_back(build_batch);

    std::vector<std::function<arrow::Result<std::unique_ptr<tiforth::pipeline::PipeOp>>(const tiforth::Engine*)>>
        transforms;
    transforms.push_back(
        [build_batches, key](const tiforth::Engine* engine)
            -> arrow::Result<std::unique_ptr<tiforth::pipeline::PipeOp>> {
          return std::make_unique<tiforth::HashJoinPipeOp>(engine, build_batches, key);
        });

    ARROW_ASSIGN_OR_RAISE(auto outputs, RunPipeline(probe_batch, std::move(transforms)));
    if (outputs.size() != 1) {
      return arrow::Status::Invalid("expected exactly 1 join output batch");
    }
    const auto & out = outputs[0];
    if (out->num_columns() != 6 || out->num_rows() != 2) {
      return arrow::Status::Invalid("unexpected join output shape");
    }

    arrow::BinaryBuilder probe_s_builder;
    ARROW_RETURN_NOT_OK(probe_s_builder.Append(reinterpret_cast<const uint8_t *>("a "), 2));
    ARROW_RETURN_NOT_OK(probe_s_builder.Append(reinterpret_cast<const uint8_t *>("b"), 1));
    std::shared_ptr<arrow::Array> probe_s_expect;
    ARROW_RETURN_NOT_OK(probe_s_builder.Finish(&probe_s_expect));

    arrow::Int32Builder pv_builder;
    ARROW_RETURN_NOT_OK(pv_builder.AppendValues({10, 20}));
    std::shared_ptr<arrow::Array> pv_expect;
    ARROW_RETURN_NOT_OK(pv_builder.Finish(&pv_expect));

    arrow::BinaryBuilder build_s_builder;
    ARROW_RETURN_NOT_OK(build_s_builder.Append(reinterpret_cast<const uint8_t *>("a"), 1));
    ARROW_RETURN_NOT_OK(build_s_builder.Append(reinterpret_cast<const uint8_t *>("b "), 2));
    std::shared_ptr<arrow::Array> build_s_expect;
    ARROW_RETURN_NOT_OK(build_s_builder.Finish(&build_s_expect));

    auto dec_type = arrow::decimal256(/*precision=*/40, /*scale=*/2);
    arrow::Decimal256Builder build_d_builder(dec_type, arrow::default_memory_pool());
    ARROW_RETURN_NOT_OK(build_d_builder.Append(arrow::Decimal256(123)));
    ARROW_RETURN_NOT_OK(build_d_builder.Append(arrow::Decimal256(456)));
    std::shared_ptr<arrow::Array> build_d_expect;
    ARROW_RETURN_NOT_OK(build_d_builder.Finish(&build_d_expect));

    const UInt64 dt1 = MyDateTime(2024, 1, 1, 0, 0, 0, 0).toPackedUInt();
    const UInt64 dt2 = MyDateTime(2024, 1, 2, 3, 4, 5, 0).toPackedUInt();
    arrow::UInt64Builder build_t_builder;
    ARROW_RETURN_NOT_OK(build_t_builder.AppendValues({dt1, dt2}));
    std::shared_ptr<arrow::Array> build_t_expect;
    ARROW_RETURN_NOT_OK(build_t_builder.Finish(&build_t_expect));

    arrow::Int32Builder bv_builder;
    ARROW_RETURN_NOT_OK(bv_builder.AppendValues({100, 200}));
    std::shared_ptr<arrow::Array> bv_expect;
    ARROW_RETURN_NOT_OK(bv_builder.Finish(&bv_expect));

    if (!probe_s_expect->Equals(*out->column(0)) || !pv_expect->Equals(*out->column(1)) ||
        !build_s_expect->Equals(*out->column(2)) || !build_d_expect->Equals(*out->column(3)) ||
        !build_t_expect->Equals(*out->column(4)) || !bv_expect->Equals(*out->column(5))) {
      return arrow::Status::Invalid("unexpected join output values");
    }
  }

  return arrow::Status::OK();
}

}  // namespace

TEST(TiForthTypeMappingTest, BlockToArrowAndOperators) {
  auto status = RunTypeMappingAndOps();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

}  // namespace DB::tests

#else

TEST(TiForthTypeMappingTest, Disabled) {
  GTEST_SKIP() << "TiForth integration is disabled (ENABLE_TIFORTH=OFF)";
}

#endif
