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

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/util/decimal.h>

#include <cassert>
#include <optional>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "tiforth/engine.h"
#include "tiforth/expr.h"
#include "tiforth/operators/hash_agg.h"
#include "tiforth/operators/hash_join.h"
#include "tiforth/pipeline.h"
#include "tiforth/task.h"

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

arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>> RunPipeline(
    std::shared_ptr<arrow::RecordBatch> input,
    std::vector<tiforth::PipelineBuilder::TransformFactory> transforms) {
  ARROW_ASSIGN_OR_RAISE(auto engine, tiforth::Engine::Create(tiforth::EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, tiforth::PipelineBuilder::Create(engine.get()));
  for (auto & factory : transforms) {
    ARROW_RETURN_NOT_OK(builder->AppendTransform(std::move(factory)));
  }
  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto task, pipeline->CreateTask());

  ARROW_ASSIGN_OR_RAISE(auto initial_state, task->Step());
  if (initial_state != tiforth::TaskState::kNeedInput) {
    return arrow::Status::Invalid("expected TaskState::kNeedInput");
  }

  ARROW_RETURN_NOT_OK(task->PushInput(std::move(input)));
  ARROW_RETURN_NOT_OK(task->CloseInput());

  std::vector<std::shared_ptr<arrow::RecordBatch>> outputs;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto state, task->Step());
    if (state == tiforth::TaskState::kFinished) {
      break;
    }
    if (state == tiforth::TaskState::kNeedInput) {
      continue;
    }
    if (state != tiforth::TaskState::kHasOutput) {
      return arrow::Status::Invalid("unexpected task state");
    }

    ARROW_ASSIGN_OR_RAISE(auto out, task->PullOutput());
    if (out == nullptr) {
      return arrow::Status::Invalid("expected non-null output batch");
    }
    outputs.push_back(std::move(out));
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
    std::vector<tiforth::PipelineBuilder::TransformFactory> transforms;
    transforms.push_back([keys, aggs]() -> arrow::Result<tiforth::TransformOpPtr> {
      return std::make_unique<tiforth::HashAggTransformOp>(keys, aggs);
    });
    ARROW_ASSIGN_OR_RAISE(auto outputs, RunPipeline(input, std::move(transforms)));
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

    if (!key_expect->Equals(*out->column(0)) || !cnt_expect->Equals(*out->column(1))) {
      return arrow::Status::Invalid("unexpected string hash agg values");
    }
  }

  // HashAgg: group by Decimal256(40,2).
  {
    std::vector<tiforth::AggKey> keys = {{"d", tiforth::MakeFieldRef("d")}};
    std::vector<tiforth::AggFunc> aggs;
    aggs.push_back({"cnt", "count_all", nullptr});
    std::vector<tiforth::PipelineBuilder::TransformFactory> transforms;
    transforms.push_back([keys, aggs]() -> arrow::Result<tiforth::TransformOpPtr> {
      return std::make_unique<tiforth::HashAggTransformOp>(keys, aggs);
    });
    ARROW_ASSIGN_OR_RAISE(auto outputs, RunPipeline(input, std::move(transforms)));
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

    if (!key_expect->Equals(*out->column(0)) || !cnt_expect->Equals(*out->column(1))) {
      return arrow::Status::Invalid("unexpected decimal hash agg values");
    }
  }

  // HashAgg: group by packed MyDateTime (uint64).
  {
    std::vector<tiforth::AggKey> keys = {{"t", tiforth::MakeFieldRef("t")}};
    std::vector<tiforth::AggFunc> aggs;
    aggs.push_back({"cnt", "count_all", nullptr});
    std::vector<tiforth::PipelineBuilder::TransformFactory> transforms;
    transforms.push_back([keys, aggs]() -> arrow::Result<tiforth::TransformOpPtr> {
      return std::make_unique<tiforth::HashAggTransformOp>(keys, aggs);
    });
    ARROW_ASSIGN_OR_RAISE(auto outputs, RunPipeline(input, std::move(transforms)));
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

    if (!key_expect->Equals(*out->column(0)) || !cnt_expect->Equals(*out->column(1))) {
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

    tiforth::JoinKey key{.left = "s", .right = "s"};
    std::vector<std::shared_ptr<arrow::RecordBatch>> build_batches;
    build_batches.push_back(build_batch);

    std::vector<tiforth::PipelineBuilder::TransformFactory> transforms;
    transforms.push_back([build_batches, key]() -> arrow::Result<tiforth::TransformOpPtr> {
      return std::make_unique<tiforth::HashJoinTransformOp>(build_batches, key);
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
