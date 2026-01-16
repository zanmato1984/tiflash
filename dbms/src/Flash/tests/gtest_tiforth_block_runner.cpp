#include <gtest/gtest.h>

#if defined(TIFLASH_ENABLE_TIFORTH)

#include <Common/MyTime.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/typeid_cast.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeMyDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Flash/TiForth/BlockPipelineRunner.h>

#include <arrow/result.h>
#include <arrow/scalar.h>
#include <arrow/status.h>

#include <optional>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "tiforth/engine.h"
#include "tiforth/expr.h"
#include "tiforth/operators/hash_agg.h"
#include "tiforth/operators/hash_join.h"
#include "tiforth/operators/filter.h"
#include "tiforth/pipeline.h"

namespace DB::tests {

namespace {

arrow::Status RunFilterOnBlockWithCollation() {
  // Input: s is Nullable(String) with padding BIN collation.
  auto s_nested = ColumnString::create();
  auto s_null = ColumnUInt8::create();
  const auto append_s = [&](std::optional<std::string_view> v) {
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
  auto s_type = makeNullable(std::make_shared<DataTypeString>());

  ColumnsWithTypeAndName cols;
  cols.emplace_back(std::move(s_col), s_type, "s");
  Block input(std::move(cols));

  std::unordered_map<String, TiForth::ColumnOptions> options_by_name;
  options_by_name.emplace("s", TiForth::ColumnOptions{.collation_id = 46});  // UTF8MB4_BIN (PAD SPACE)

  ARROW_ASSIGN_OR_RAISE(auto engine, tiforth::Engine::Create(tiforth::EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, tiforth::PipelineBuilder::Create(engine.get()));

  auto predicate = tiforth::MakeCall(
      "equal", {tiforth::MakeFieldRef("s"),
                tiforth::MakeLiteral(std::make_shared<arrow::StringScalar>("a"))});

  ARROW_RETURN_NOT_OK(builder->AppendTransform(
      [predicate]() -> arrow::Result<tiforth::TransformOpPtr> {
        return std::make_unique<tiforth::FilterTransformOp>(predicate);
      }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());

  ARROW_ASSIGN_OR_RAISE(
      auto outputs,
      TiForth::RunTiForthPipelineOnBlocks(*pipeline, {input}, options_by_name,
                                          arrow::default_memory_pool()));

  if (outputs.size() != 1) {
    return arrow::Status::Invalid("expected exactly 1 output block");
  }

  const auto & out = outputs[0];
  const auto & block = out.block;
  if (block.columns() != 1 || block.rows() != 2) {
    return arrow::Status::Invalid("unexpected output block shape");
  }

  const auto opt_it = out.options_by_name.find("s");
  if (opt_it == out.options_by_name.end() || !opt_it->second.collation_id.has_value() ||
      *opt_it->second.collation_id != 46) {
    return arrow::Status::Invalid("output collation id mismatch");
  }

  const auto & elem = block.getByName("s");
  const auto * nullable = typeid_cast<const ColumnNullable *>(elem.column.get());
  if (nullable == nullptr) {
    return arrow::Status::Invalid("expected nullable string column");
  }
  const auto & nested = nullable->getNestedColumn();
  const auto * str = typeid_cast<const ColumnString *>(&nested);
  if (str == nullptr) {
    return arrow::Status::Invalid("expected ColumnString nested column");
  }

  if (nullable->isNullAt(0) || nullable->isNullAt(1)) {
    return arrow::Status::Invalid("unexpected nulls in output");
  }
  const auto v0 = str->getDataAt(0);
  const auto v1 = str->getDataAt(1);
  if (std::string_view(v0.data, v0.size) != "a" || std::string_view(v1.data, v1.size) != "a ") {
    return arrow::Status::Invalid("unexpected filtered values");
  }

  return arrow::Status::OK();
}

arrow::Status RunTwoKeyHashAggOnBlocks() {
  // Input: group by (s, k2) where s uses padding BIN collation ("a" == "a ").
  auto s_col = ColumnString::create();
  s_col->insertData("a", 1);
  s_col->insertData("a ", 2);
  s_col->insertData("a ", 2);
  s_col->insertData("b", 1);
  auto s_type = std::make_shared<DataTypeString>();

  auto k2_col = ColumnInt32::create();
  k2_col->insert(Field(static_cast<Int64>(1)));
  k2_col->insert(Field(static_cast<Int64>(1)));
  k2_col->insert(Field(static_cast<Int64>(2)));
  k2_col->insert(Field(static_cast<Int64>(1)));
  auto k2_type = std::make_shared<DataTypeInt32>();

  auto v_col = ColumnInt32::create();
  v_col->insert(Field(static_cast<Int64>(10)));
  v_col->insert(Field(static_cast<Int64>(20)));
  v_col->insert(Field(static_cast<Int64>(1)));
  v_col->insert(Field(static_cast<Int64>(5)));
  auto v_type = std::make_shared<DataTypeInt32>();

  ColumnsWithTypeAndName cols;
  cols.emplace_back(std::move(s_col), s_type, "s");
  cols.emplace_back(std::move(k2_col), k2_type, "k2");
  cols.emplace_back(std::move(v_col), v_type, "v");
  Block input(std::move(cols));

  std::unordered_map<String, TiForth::ColumnOptions> options_by_name;
  options_by_name.emplace("s", TiForth::ColumnOptions{.collation_id = 46});  // UTF8MB4_BIN (PAD SPACE)

  ARROW_ASSIGN_OR_RAISE(auto engine, tiforth::Engine::Create(tiforth::EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, tiforth::PipelineBuilder::Create(engine.get()));

  std::vector<tiforth::AggKey> keys = {{"s", tiforth::MakeFieldRef("s")},
                                       {"k2", tiforth::MakeFieldRef("k2")}};
  std::vector<tiforth::AggFunc> aggs;
  aggs.push_back({"cnt", "count_all", nullptr});
  aggs.push_back({"sum_v", "sum_int32", tiforth::MakeFieldRef("v")});

  ARROW_RETURN_NOT_OK(builder->AppendTransform([keys, aggs]() -> arrow::Result<tiforth::TransformOpPtr> {
    return std::make_unique<tiforth::HashAggTransformOp>(keys, aggs);
  }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(auto outputs,
                        TiForth::RunTiForthPipelineOnBlocks(*pipeline, {input}, options_by_name,
                                                            arrow::default_memory_pool()));
  if (outputs.size() != 1) {
    return arrow::Status::Invalid("expected exactly 1 hash agg output block");
  }

  const auto& out = outputs[0];
  const auto& block = out.block;
  if (block.columns() != 4 || block.rows() != 3) {
    return arrow::Status::Invalid("unexpected hash agg output shape");
  }

  // Collation metadata should survive on the string key.
  const auto opt_it = out.options_by_name.find("s");
  if (opt_it == out.options_by_name.end() || !opt_it->second.collation_id.has_value() ||
      *opt_it->second.collation_id != 46) {
    return arrow::Status::Invalid("hash agg output collation id mismatch");
  }

  const auto& s_out = block.getByPosition(0).column;
  const auto* s_str = typeid_cast<const ColumnString*>(s_out.get());
  if (s_str == nullptr) {
    return arrow::Status::Invalid("expected ColumnString for group key s");
  }
  const auto& k2_out = block.getByPosition(1).column;
  const auto* k2_i32 = typeid_cast<const ColumnInt32*>(k2_out.get());
  if (k2_i32 == nullptr) {
    return arrow::Status::Invalid("expected ColumnInt32 for group key k2");
  }
  const auto& cnt_out = block.getByPosition(2).column;
  const auto* cnt_u64 = typeid_cast<const ColumnUInt64*>(cnt_out.get());
  if (cnt_u64 == nullptr) {
    return arrow::Status::Invalid("expected ColumnUInt64 for count");
  }
  const auto& sum_out = block.getByPosition(3).column;
  const auto* sum_nullable = typeid_cast<const ColumnNullable*>(sum_out.get());
  if (sum_nullable == nullptr) {
    return arrow::Status::Invalid("expected nullable sum column");
  }
  const auto* sum_i64 = typeid_cast<const ColumnInt64*>(&sum_nullable->getNestedColumn());
  if (sum_i64 == nullptr) {
    return arrow::Status::Invalid("expected nested ColumnInt64 for sum");
  }

  // Output order is first-seen groups: ("a",1), ("a ",2), ("b",1).
  const auto s0 = s_str->getDataAt(0);
  const auto s1 = s_str->getDataAt(1);
  const auto s2 = s_str->getDataAt(2);
  if (std::string_view(s0.data, s0.size) != "a" || std::string_view(s1.data, s1.size) != "a " ||
      std::string_view(s2.data, s2.size) != "b") {
    return arrow::Status::Invalid("unexpected hash agg s values");
  }
  const auto& k2_data = k2_i32->getData();
  const auto& cnt_data = cnt_u64->getData();
  if (k2_data.size() != 3 || cnt_data.size() != 3) {
    return arrow::Status::Invalid("unexpected hash agg key/cnt sizes");
  }
  if (k2_data[0] != 1 || k2_data[1] != 2 || k2_data[2] != 1) {
    return arrow::Status::Invalid("unexpected hash agg k2 values");
  }
  if (cnt_data[0] != 2 || cnt_data[1] != 1 || cnt_data[2] != 1) {
    return arrow::Status::Invalid("unexpected hash agg cnt values");
  }

  if (sum_nullable->isNullAt(0) || sum_nullable->isNullAt(1) || sum_nullable->isNullAt(2)) {
    return arrow::Status::Invalid("unexpected null sums");
  }
  const auto& sum_data = sum_i64->getData();
  if (sum_data.size() != 3 || sum_data[0] != 30 || sum_data[1] != 1 || sum_data[2] != 5) {
    return arrow::Status::Invalid("unexpected hash agg sum values");
  }

  return arrow::Status::OK();
}

arrow::Status RunTwoKeyHashJoinOnBlocks() {
  // Build side (bs, bd, bt, bv) joined with probe side (s, d, pv) on (s == bs, d == bd).
  const auto d_type = createDecimal(/*prec=*/40, /*scale=*/2);
  const auto t_type = std::make_shared<DataTypeMyDateTime>(6);

  ColumnsWithTypeAndName build_cols;
  {
    auto bs = ColumnString::create();
    bs->insertData("a", 1);
    bs->insertData("b ", 2);
    build_cols.emplace_back(std::move(bs), std::make_shared<DataTypeString>(), "bs");
  }
  {
    auto d_mut = d_type->createColumn();
    auto* d_col = typeid_cast<ColumnDecimal<Decimal256>*>(d_mut.get());
    if (d_col == nullptr) {
      return arrow::Status::Invalid("expected Decimal256 column for build bd");
    }
    d_col->insert(Decimal256(Int256(123)));
    d_col->insert(Decimal256(Int256(456)));
    build_cols.emplace_back(std::move(d_mut), d_type, "bd");
  }
  UInt64 dt1 = MyDateTime(2024, 1, 1, 0, 0, 0, 0).toPackedUInt();
  UInt64 dt2 = MyDateTime(2024, 1, 2, 3, 4, 5, 0).toPackedUInt();
  {
    auto bt = ColumnUInt64::create();
    bt->insert(Field(dt1));
    bt->insert(Field(dt2));
    build_cols.emplace_back(std::move(bt), t_type, "bt");
  }
  {
    auto bv = ColumnInt32::create();
    bv->insert(Field(static_cast<Int64>(100)));
    bv->insert(Field(static_cast<Int64>(200)));
    build_cols.emplace_back(std::move(bv), std::make_shared<DataTypeInt32>(), "bv");
  }
  Block build_block(std::move(build_cols));

  ColumnsWithTypeAndName probe_cols;
  {
    auto s = ColumnString::create();
    s->insertData("a ", 2);
    s->insertData("b", 1);
    s->insertData("c", 1);
    s->insertData("a", 1);
    probe_cols.emplace_back(std::move(s), std::make_shared<DataTypeString>(), "s");
  }
  {
    auto d_mut = d_type->createColumn();
    auto* d_col = typeid_cast<ColumnDecimal<Decimal256>*>(d_mut.get());
    if (d_col == nullptr) {
      return arrow::Status::Invalid("expected Decimal256 column for probe d");
    }
    d_col->insert(Decimal256(Int256(123)));
    d_col->insert(Decimal256(Int256(456)));
    d_col->insert(Decimal256(Int256(456)));
    d_col->insert(Decimal256(Int256(999)));
    probe_cols.emplace_back(std::move(d_mut), d_type, "d");
  }
  {
    auto pv = ColumnInt32::create();
    pv->insert(Field(static_cast<Int64>(10)));
    pv->insert(Field(static_cast<Int64>(20)));
    pv->insert(Field(static_cast<Int64>(30)));
    pv->insert(Field(static_cast<Int64>(40)));
    probe_cols.emplace_back(std::move(pv), std::make_shared<DataTypeInt32>(), "pv");
  }
  Block probe_block(std::move(probe_cols));

  std::unordered_map<String, TiForth::ColumnOptions> build_options;
  build_options.emplace("bs", TiForth::ColumnOptions{.collation_id = 46});  // UTF8MB4_BIN (PAD SPACE)
  std::unordered_map<String, TiForth::ColumnOptions> probe_options;
  probe_options.emplace("s", TiForth::ColumnOptions{.collation_id = 46});

  ARROW_ASSIGN_OR_RAISE(auto build_batch,
                        TiForth::toArrowRecordBatch(build_block, build_options,
                                                    arrow::default_memory_pool()));
  std::vector<std::shared_ptr<arrow::RecordBatch>> build_batches;
  build_batches.push_back(std::move(build_batch));

  ARROW_ASSIGN_OR_RAISE(auto engine, tiforth::Engine::Create(tiforth::EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, tiforth::PipelineBuilder::Create(engine.get()));

  tiforth::JoinKey key{.left = {"s", "d"}, .right = {"bs", "bd"}};
  ARROW_RETURN_NOT_OK(builder->AppendTransform([build_batches, key]() -> arrow::Result<tiforth::TransformOpPtr> {
    return std::make_unique<tiforth::HashJoinTransformOp>(build_batches, key);
  }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(
      auto outputs,
      TiForth::RunTiForthPipelineOnBlocks(*pipeline, {probe_block}, probe_options,
                                          arrow::default_memory_pool()));
  if (outputs.size() != 1) {
    return arrow::Status::Invalid("expected exactly 1 join output block");
  }

  const auto& out = outputs[0];
  const auto& block = out.block;
  if (block.columns() != 7 || block.rows() != 2) {
    return arrow::Status::Invalid("unexpected join output shape");
  }

  // Collation should be preserved for both string columns (unique names: s, bs).
  const auto s_opt = out.options_by_name.find("s");
  const auto bs_opt = out.options_by_name.find("bs");
  if (s_opt == out.options_by_name.end() || bs_opt == out.options_by_name.end()) {
    return arrow::Status::Invalid("missing string collation options in join output");
  }
  if (!s_opt->second.collation_id.has_value() || !bs_opt->second.collation_id.has_value() ||
      *s_opt->second.collation_id != 46 || *bs_opt->second.collation_id != 46) {
    return arrow::Status::Invalid("join output collation id mismatch");
  }

  const auto* out_s = typeid_cast<const ColumnString*>(block.getByPosition(0).column.get());
  const auto* out_d = typeid_cast<const ColumnDecimal<Decimal256>*>(block.getByPosition(1).column.get());
  const auto* out_pv = typeid_cast<const ColumnInt32*>(block.getByPosition(2).column.get());
  const auto* out_bs = typeid_cast<const ColumnString*>(block.getByPosition(3).column.get());
  const auto* out_bd = typeid_cast<const ColumnDecimal<Decimal256>*>(block.getByPosition(4).column.get());
  const auto* out_bt = typeid_cast<const ColumnUInt64*>(block.getByPosition(5).column.get());
  const auto* out_bv = typeid_cast<const ColumnInt32*>(block.getByPosition(6).column.get());
  if (out_s == nullptr || out_d == nullptr || out_pv == nullptr || out_bs == nullptr || out_bd == nullptr ||
      out_bt == nullptr || out_bv == nullptr) {
    return arrow::Status::Invalid("unexpected join output column types");
  }

  const auto s0 = out_s->getDataAt(0);
  const auto s1 = out_s->getDataAt(1);
  if (std::string_view(s0.data, s0.size) != "a " || std::string_view(s1.data, s1.size) != "b") {
    return arrow::Status::Invalid("unexpected probe s values");
  }
  const auto bs0 = out_bs->getDataAt(0);
  const auto bs1 = out_bs->getDataAt(1);
  if (std::string_view(bs0.data, bs0.size) != "a" || std::string_view(bs1.data, bs1.size) != "b ") {
    return arrow::Status::Invalid("unexpected build bs values");
  }

  const auto& pv_data = out_pv->getData();
  const auto& bv_data = out_bv->getData();
  const auto& bt_data = out_bt->getData();
  if (pv_data.size() != 2 || bv_data.size() != 2 || bt_data.size() != 2) {
    return arrow::Status::Invalid("unexpected join output sizes");
  }
  if (pv_data[0] != 10 || pv_data[1] != 20 || bv_data[0] != 100 || bv_data[1] != 200 ||
      bt_data[0] != dt1 || bt_data[1] != dt2) {
    return arrow::Status::Invalid("unexpected join pv/bv/bt values");
  }

  const auto& d_data = out_d->getData();
  const auto& bd_data = out_bd->getData();
  if (d_data.size() != 2 || bd_data.size() != 2) {
    return arrow::Status::Invalid("unexpected join decimal output sizes");
  }
  if (d_data[0] != Decimal256(Int256(123)) || d_data[1] != Decimal256(Int256(456)) ||
      bd_data[0] != Decimal256(Int256(123)) || bd_data[1] != Decimal256(Int256(456))) {
    return arrow::Status::Invalid("unexpected join decimal values");
  }

  return arrow::Status::OK();
}

}  // namespace

TEST(TiForthBlockRunnerTest, FilterOnBlockWithCollation) {
  auto status = RunFilterOnBlockWithCollation();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthBlockRunnerTest, TwoKeyHashAgg) {
  auto status = RunTwoKeyHashAggOnBlocks();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthBlockRunnerTest, TwoKeyHashJoin) {
  auto status = RunTwoKeyHashJoinOnBlocks();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

}  // namespace DB::tests

#else

TEST(TiForthBlockRunnerTest, Disabled) {
  GTEST_SKIP() << "TiForth integration is disabled (ENABLE_TIFORTH=OFF)";
}

#endif
