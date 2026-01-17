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
#include "tiforth/operators/projection.h"
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
      [engine_ptr = engine.get(), predicate]() -> arrow::Result<tiforth::TransformOpPtr> {
        return std::make_unique<tiforth::FilterTransformOp>(engine_ptr, predicate);
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

arrow::Status RunFilterOnBlockWithMixedCollation() {
  // Input: s_bin uses BINARY collation, s_pad uses padding BIN collation.
  auto s_bin_col = ColumnString::create();
  auto s_pad_col = ColumnString::create();

  const auto append = [&](ColumnString & col, std::string_view v) {
    col.insertData(v.data(), v.size());
  };

  append(*s_bin_col, "a ");
  append(*s_pad_col, "a");
  append(*s_bin_col, "a");
  append(*s_pad_col, "a");
  append(*s_bin_col, "a ");
  append(*s_pad_col, "a ");
  append(*s_bin_col, "b");
  append(*s_pad_col, "b ");

  auto s_type = std::make_shared<DataTypeString>();
  ColumnsWithTypeAndName cols;
  cols.emplace_back(std::move(s_bin_col), s_type, "s_bin");
  cols.emplace_back(std::move(s_pad_col), s_type, "s_pad");
  Block input(std::move(cols));

  std::unordered_map<String, TiForth::ColumnOptions> options_by_name;
  options_by_name.emplace("s_bin", TiForth::ColumnOptions{.collation_id = 63});  // BINARY
  options_by_name.emplace("s_pad", TiForth::ColumnOptions{.collation_id = 46});  // UTF8MB4_BIN (PAD SPACE)

  ARROW_ASSIGN_OR_RAISE(auto engine, tiforth::Engine::Create(tiforth::EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, tiforth::PipelineBuilder::Create(engine.get()));

  auto predicate = tiforth::MakeCall("equal", {tiforth::MakeFieldRef("s_bin"), tiforth::MakeFieldRef("s_pad")});
  ARROW_RETURN_NOT_OK(builder->AppendTransform(
      [engine_ptr = engine.get(), predicate]() -> arrow::Result<tiforth::TransformOpPtr> {
        return std::make_unique<tiforth::FilterTransformOp>(engine_ptr, predicate);
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
  if (block.columns() != 2 || block.rows() != 2) {
    return arrow::Status::Invalid("unexpected output block shape");
  }

  const auto s_bin_it = out.options_by_name.find("s_bin");
  const auto s_pad_it = out.options_by_name.find("s_pad");
  if (s_bin_it == out.options_by_name.end() || s_pad_it == out.options_by_name.end()) {
    return arrow::Status::Invalid("missing string collation options in output");
  }
  if (!s_bin_it->second.collation_id.has_value() || !s_pad_it->second.collation_id.has_value()
      || *s_bin_it->second.collation_id != 63 || *s_pad_it->second.collation_id != 46) {
    return arrow::Status::Invalid("output collation id mismatch");
  }

  const auto & s_bin_elem = block.getByName("s_bin");
  const auto & s_pad_elem = block.getByName("s_pad");
  const auto * s_bin = typeid_cast<const ColumnString *>(s_bin_elem.column.get());
  const auto * s_pad = typeid_cast<const ColumnString *>(s_pad_elem.column.get());
  if (s_bin == nullptr || s_pad == nullptr) {
    return arrow::Status::Invalid("expected ColumnString output columns");
  }

  const auto v0_bin = s_bin->getDataAt(0);
  const auto v0_pad = s_pad->getDataAt(0);
  const auto v1_bin = s_bin->getDataAt(1);
  const auto v1_pad = s_pad->getDataAt(1);
  if (std::string_view(v0_bin.data, v0_bin.size) != "a" || std::string_view(v0_pad.data, v0_pad.size) != "a") {
    return arrow::Status::Invalid("unexpected row0 output values");
  }
  if (std::string_view(v1_bin.data, v1_bin.size) != "a " || std::string_view(v1_pad.data, v1_pad.size) != "a ") {
    return arrow::Status::Invalid("unexpected row1 output values");
  }

  return arrow::Status::OK();
}

arrow::Status RunProjectionDecimalAddOnBlock() {
  // Input: a is Nullable(Decimal(38,2)), b is Nullable(Decimal(38,3)).
  const auto a_nested_type = createDecimal(/*prec=*/38, /*scale=*/2);
  const auto b_nested_type = createDecimal(/*prec=*/38, /*scale=*/3);

  auto a_nested = a_nested_type->createColumn();
  auto b_nested = b_nested_type->createColumn();
  auto* a_dec = typeid_cast<ColumnDecimal<Decimal128>*>(a_nested.get());
  auto* b_dec = typeid_cast<ColumnDecimal<Decimal128>*>(b_nested.get());
  if (a_dec == nullptr || b_dec == nullptr) {
    return arrow::Status::Invalid("expected Decimal128 columns for inputs");
  }

  auto a_null = ColumnUInt8::create();
  auto b_null = ColumnUInt8::create();

  const auto append_a = [&](std::optional<Int128> raw) {
    if (raw.has_value()) {
      a_dec->insert(Decimal128(*raw));
      a_null->insert(Field(static_cast<UInt64>(0)));
    } else {
      a_dec->insert(Decimal128(Int128(0)));
      a_null->insert(Field(static_cast<UInt64>(1)));
    }
  };
  const auto append_b = [&](std::optional<Int128> raw) {
    if (raw.has_value()) {
      b_dec->insert(Decimal128(*raw));
      b_null->insert(Field(static_cast<UInt64>(0)));
    } else {
      b_dec->insert(Decimal128(Int128(0)));
      b_null->insert(Field(static_cast<UInt64>(1)));
    }
  };

  // Values are stored as scaled integers:
  // - a: scale=2 => 1.23 is 123
  // - b: scale=3 => 0.100 is 100
  append_a(Int128(123));
  append_b(Int128(100));

  append_a(Int128(456));
  append_b(Int128(2345));

  append_a(std::nullopt);
  append_b(Int128(2345));

  auto a_col = ColumnNullable::create(std::move(a_nested), std::move(a_null));
  auto b_col = ColumnNullable::create(std::move(b_nested), std::move(b_null));

  const auto a_type = makeNullable(a_nested_type);
  const auto b_type = makeNullable(b_nested_type);

  ColumnsWithTypeAndName cols;
  cols.emplace_back(std::move(a_col), a_type, "a");
  cols.emplace_back(std::move(b_col), b_type, "b");
  Block input(std::move(cols));

  ARROW_ASSIGN_OR_RAISE(auto engine, tiforth::Engine::Create(tiforth::EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, tiforth::PipelineBuilder::Create(engine.get()));

  std::vector<tiforth::ProjectionExpr> exprs;
  exprs.push_back({"sum", tiforth::MakeCall("add", {tiforth::MakeFieldRef("a"), tiforth::MakeFieldRef("b")})});

  ARROW_RETURN_NOT_OK(builder->AppendTransform(
      [engine_ptr = engine.get(), exprs]() -> arrow::Result<tiforth::TransformOpPtr> {
        return std::make_unique<tiforth::ProjectionTransformOp>(engine_ptr, exprs);
      }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());

  ARROW_ASSIGN_OR_RAISE(
      auto outputs,
      TiForth::RunTiForthPipelineOnBlocks(*pipeline, {input},
                                          /*input_options_by_name=*/{},
                                          arrow::default_memory_pool()));
  if (outputs.size() != 1) {
    return arrow::Status::Invalid("expected exactly 1 output block");
  }

  const auto& out = outputs[0].block;
  if (out.columns() != 1 || out.rows() != 3) {
    return arrow::Status::Invalid("unexpected projection output shape");
  }

  const auto& elem = out.getByName("sum");
  const auto* out_nullable = typeid_cast<const ColumnNullable*>(elem.column.get());
  if (out_nullable == nullptr) {
    return arrow::Status::Invalid("expected nullable decimal output column");
  }
  const auto* out_dec =
      typeid_cast<const ColumnDecimal<Decimal256>*>(&out_nullable->getNestedColumn());
  if (out_dec == nullptr) {
    return arrow::Status::Invalid("expected Decimal256 nested column for output");
  }

  const auto nested_type = removeNullable(elem.type);
  if (nested_type == nullptr || !nested_type->isDecimal()) {
    return arrow::Status::Invalid("expected decimal output type");
  }
  const auto out_prec = getDecimalPrecision(*nested_type, /*default_value=*/0);
  const auto out_scale = getDecimalScale(*nested_type, /*default_value=*/0);
  if (out_prec != 40 || out_scale != 3) {
    return arrow::Status::Invalid("unexpected decimal add output precision/scale");
  }

  if (out_nullable->isNullAt(0) || out_nullable->isNullAt(1) || !out_nullable->isNullAt(2)) {
    return arrow::Status::Invalid("unexpected null map in decimal add output");
  }

  const auto& data = out_dec->getData();
  if (data.size() != 3) {
    return arrow::Status::Invalid("unexpected decimal output size");
  }
  // Expected sums (scaled by 10^3):
  // 1.23 + 0.100 = 1.330  -> 1330
  // 4.56 + 2.345 = 6.905  -> 6905
  if (data[0] != Decimal256(Int256(1330)) || data[1] != Decimal256(Int256(6905))) {
    return arrow::Status::Invalid("unexpected decimal add output values");
  }

  return arrow::Status::OK();
}

arrow::Status RunProjectionDecimalAddIntOnBlock() {
  // Input: a is Nullable(Decimal(20,2)), i is Int64.
  const auto a_nested_type = createDecimal(/*prec=*/20, /*scale=*/2);
  auto a_nested = a_nested_type->createColumn();
  auto* a_dec = typeid_cast<ColumnDecimal<Decimal128>*>(a_nested.get());
  if (a_dec == nullptr) {
    return arrow::Status::Invalid("expected Decimal128 column for input decimal");
  }

  auto a_null = ColumnUInt8::create();
  const auto append_a = [&](std::optional<Int128> raw) {
    if (raw.has_value()) {
      a_dec->insert(Decimal128(*raw));
      a_null->insert(Field(static_cast<UInt64>(0)));
    } else {
      a_dec->insert(Decimal128(Int128(0)));
      a_null->insert(Field(static_cast<UInt64>(1)));
    }
  };

  // Values stored as scaled integers (scale=2).
  append_a(Int128(123));       // 1.23
  append_a(Int128(456));       // 4.56
  append_a(std::nullopt);      // NULL

  auto a_col = ColumnNullable::create(std::move(a_nested), std::move(a_null));
  auto a_type = makeNullable(a_nested_type);

  auto i_col = ColumnInt64::create();
  i_col->insert(Field(static_cast<Int64>(5)));
  i_col->insert(Field(static_cast<Int64>(-7)));
  i_col->insert(Field(static_cast<Int64>(1)));
  auto i_type = std::make_shared<DataTypeInt64>();

  ColumnsWithTypeAndName cols;
  cols.emplace_back(std::move(a_col), a_type, "a");
  cols.emplace_back(std::move(i_col), i_type, "i");
  Block input(std::move(cols));

  ARROW_ASSIGN_OR_RAISE(auto engine, tiforth::Engine::Create(tiforth::EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, tiforth::PipelineBuilder::Create(engine.get()));

  std::vector<tiforth::ProjectionExpr> exprs;
  exprs.push_back({"sum", tiforth::MakeCall("add", {tiforth::MakeFieldRef("a"), tiforth::MakeFieldRef("i")})});

  ARROW_RETURN_NOT_OK(builder->AppendTransform(
      [engine_ptr = engine.get(), exprs]() -> arrow::Result<tiforth::TransformOpPtr> {
        return std::make_unique<tiforth::ProjectionTransformOp>(engine_ptr, exprs);
      }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(
      auto outputs,
      TiForth::RunTiForthPipelineOnBlocks(*pipeline, {input},
                                          /*input_options_by_name=*/{},
                                          arrow::default_memory_pool()));
  if (outputs.size() != 1) {
    return arrow::Status::Invalid("expected exactly 1 output block");
  }

  const auto& out = outputs[0].block;
  if (out.columns() != 1 || out.rows() != 3) {
    return arrow::Status::Invalid("unexpected projection output shape");
  }

  const auto& elem = out.getByName("sum");
  const auto* out_nullable = typeid_cast<const ColumnNullable*>(elem.column.get());
  if (out_nullable == nullptr) {
    return arrow::Status::Invalid("expected nullable decimal output column");
  }
  const auto* out_dec =
      typeid_cast<const ColumnDecimal<Decimal128>*>(&out_nullable->getNestedColumn());
  if (out_dec == nullptr) {
    return arrow::Status::Invalid("expected Decimal128 nested column for output");
  }

  const auto nested_type = removeNullable(elem.type);
  if (nested_type == nullptr || !nested_type->isDecimal()) {
    return arrow::Status::Invalid("expected decimal output type");
  }
  const auto out_prec = getDecimalPrecision(*nested_type, /*default_value=*/0);
  const auto out_scale = getDecimalScale(*nested_type, /*default_value=*/0);
  if (out_prec != 22 || out_scale != 2) {
    return arrow::Status::Invalid("unexpected decimal+int add output precision/scale");
  }

  if (out_nullable->isNullAt(0) || out_nullable->isNullAt(1) || !out_nullable->isNullAt(2)) {
    return arrow::Status::Invalid("unexpected null map in decimal+int output");
  }

  const auto& data = out_dec->getData();
  if (data.size() != 3) {
    return arrow::Status::Invalid("unexpected output size");
  }
  if (data[0] != Decimal128(Int128(623)) || data[1] != Decimal128(Int128(-244))) {
    return arrow::Status::Invalid("unexpected decimal+int output values");
  }
  return arrow::Status::OK();
}

arrow::Status RunProjectionTiDBPackedMyTimeScalarsOnBlock() {
  // Input: t is MyDateTime(6) stored as packed UInt64.
  const auto t_type = std::make_shared<DataTypeMyDateTime>(6);
  auto t_col = ColumnUInt64::create();
  const UInt64 dt0 = MyDateTime(2024, 1, 2, 3, 4, 5, 0).toPackedUInt();
  const UInt64 dt1 = MyDateTime(1999, 12, 31, 23, 59, 58, 0).toPackedUInt();
  const UInt64 dt2 = 0;  // invalid (month/day zero)
  t_col->insert(Field(dt0));
  t_col->insert(Field(dt1));
  t_col->insert(Field(dt2));

  ColumnsWithTypeAndName cols;
  cols.emplace_back(std::move(t_col), t_type, "t");
  Block input(std::move(cols));

  ARROW_ASSIGN_OR_RAISE(auto engine, tiforth::Engine::Create(tiforth::EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, tiforth::PipelineBuilder::Create(engine.get()));

  std::vector<tiforth::ProjectionExpr> exprs;
  exprs.push_back({"dow", tiforth::MakeCall("tidbDayOfWeek", {tiforth::MakeFieldRef("t")})});
  exprs.push_back({"woy", tiforth::MakeCall("tidbWeekOfYear", {tiforth::MakeFieldRef("t")})});
  exprs.push_back({"yw", tiforth::MakeCall("yearWeek", {tiforth::MakeFieldRef("t")})});

  ARROW_RETURN_NOT_OK(builder->AppendTransform(
      [engine_ptr = engine.get(), exprs]() -> arrow::Result<tiforth::TransformOpPtr> {
        return std::make_unique<tiforth::ProjectionTransformOp>(engine_ptr, exprs);
      }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());
  ARROW_ASSIGN_OR_RAISE(
      auto outputs,
      TiForth::RunTiForthPipelineOnBlocks(*pipeline, {input},
                                          /*input_options_by_name=*/{},
                                          arrow::default_memory_pool()));
  if (outputs.size() != 1) {
    return arrow::Status::Invalid("expected exactly 1 output block");
  }

  const auto& out = outputs[0].block;
  if (out.columns() != 3 || out.rows() != 3) {
    return arrow::Status::Invalid("unexpected projection output shape");
  }

  const auto* dow_nullable = typeid_cast<const ColumnNullable*>(out.getByName("dow").column.get());
  const auto* woy_nullable = typeid_cast<const ColumnNullable*>(out.getByName("woy").column.get());
  const auto* yw_nullable = typeid_cast<const ColumnNullable*>(out.getByName("yw").column.get());
  if (dow_nullable == nullptr || woy_nullable == nullptr || yw_nullable == nullptr) {
    return arrow::Status::Invalid("expected nullable output columns for TiDB packed time scalars");
  }
  const auto* dow = typeid_cast<const ColumnUInt16*>(&dow_nullable->getNestedColumn());
  const auto* woy = typeid_cast<const ColumnUInt16*>(&woy_nullable->getNestedColumn());
  const auto* yw = typeid_cast<const ColumnUInt32*>(&yw_nullable->getNestedColumn());
  if (dow == nullptr || woy == nullptr || yw == nullptr) {
    return arrow::Status::Invalid("unexpected nested numeric column types");
  }

  if (dow_nullable->isNullAt(0) || dow_nullable->isNullAt(1) || !dow_nullable->isNullAt(2) ||
      woy_nullable->isNullAt(0) || woy_nullable->isNullAt(1) || !woy_nullable->isNullAt(2) ||
      yw_nullable->isNullAt(0) || yw_nullable->isNullAt(1) || !yw_nullable->isNullAt(2)) {
    return arrow::Status::Invalid("unexpected null map for packed time scalar outputs");
  }

  const auto& dow_data = dow->getData();
  const auto& woy_data = woy->getData();
  const auto& yw_data = yw->getData();
  if (dow_data.size() != 3 || woy_data.size() != 3 || yw_data.size() != 3) {
    return arrow::Status::Invalid("unexpected packed time scalar output sizes");
  }
  if (dow_data[0] != 3 || dow_data[1] != 6) {
    return arrow::Status::Invalid("unexpected tidbDayOfWeek values");
  }
  if (woy_data[0] != 1 || woy_data[1] != 52) {
    return arrow::Status::Invalid("unexpected tidbWeekOfYear values");
  }
  if (yw_data[0] != 202353 || yw_data[1] != 199952) {
    return arrow::Status::Invalid("unexpected yearWeek values");
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

  ARROW_RETURN_NOT_OK(builder->AppendTransform([engine_ptr = engine.get(), keys,
                                                aggs]() -> arrow::Result<tiforth::TransformOpPtr> {
    return std::make_unique<tiforth::HashAggTransformOp>(engine_ptr, keys, aggs);
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

arrow::Status RunGeneralCiHashAggOnBlocks() {
  // Input: group by s where s uses UTF8MB4_GENERAL_CI (PAD SPACE, case-insensitive).
  auto s_col = ColumnString::create();
  s_col->insertData("a", 1);
  s_col->insertData("A", 1);
  s_col->insertData("a ", 2);
  s_col->insertData("b", 1);
  auto s_type = std::make_shared<DataTypeString>();

  auto v_col = ColumnInt32::create();
  v_col->insert(Field(static_cast<Int64>(10)));
  v_col->insert(Field(static_cast<Int64>(20)));
  v_col->insert(Field(static_cast<Int64>(1)));
  v_col->insert(Field(static_cast<Int64>(5)));
  auto v_type = std::make_shared<DataTypeInt32>();

  ColumnsWithTypeAndName cols;
  cols.emplace_back(std::move(s_col), s_type, "s");
  cols.emplace_back(std::move(v_col), v_type, "v");
  Block input(std::move(cols));

  std::unordered_map<String, TiForth::ColumnOptions> options_by_name;
  options_by_name.emplace("s", TiForth::ColumnOptions{.collation_id = 45});  // UTF8MB4_GENERAL_CI (PAD SPACE)

  ARROW_ASSIGN_OR_RAISE(auto engine, tiforth::Engine::Create(tiforth::EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, tiforth::PipelineBuilder::Create(engine.get()));

  std::vector<tiforth::AggKey> keys = {{"s", tiforth::MakeFieldRef("s")}};
  std::vector<tiforth::AggFunc> aggs;
  aggs.push_back({"cnt", "count_all", nullptr});
  aggs.push_back({"sum_v", "sum_int32", tiforth::MakeFieldRef("v")});

  ARROW_RETURN_NOT_OK(builder->AppendTransform([engine_ptr = engine.get(), keys,
                                                aggs]() -> arrow::Result<tiforth::TransformOpPtr> {
    return std::make_unique<tiforth::HashAggTransformOp>(engine_ptr, keys, aggs);
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
  if (block.columns() != 3 || block.rows() != 2) {
    return arrow::Status::Invalid("unexpected hash agg output shape");
  }

  const auto opt_it = out.options_by_name.find("s");
  if (opt_it == out.options_by_name.end() || !opt_it->second.collation_id.has_value() ||
      *opt_it->second.collation_id != 45) {
    return arrow::Status::Invalid("hash agg output collation id mismatch");
  }

  const auto* s_out = typeid_cast<const ColumnString*>(block.getByPosition(0).column.get());
  const auto* cnt_out = typeid_cast<const ColumnUInt64*>(block.getByPosition(1).column.get());
  const auto* sum_nullable = typeid_cast<const ColumnNullable*>(block.getByPosition(2).column.get());
  if (s_out == nullptr || cnt_out == nullptr || sum_nullable == nullptr) {
    return arrow::Status::Invalid("unexpected hash agg output column types");
  }
  const auto* sum_i64 = typeid_cast<const ColumnInt64*>(&sum_nullable->getNestedColumn());
  if (sum_i64 == nullptr) {
    return arrow::Status::Invalid("expected nested ColumnInt64 for sum");
  }

  // Output order is first-seen groups: "a", "b".
  const auto s0 = s_out->getDataAt(0);
  const auto s1 = s_out->getDataAt(1);
  if (std::string_view(s0.data, s0.size) != "a" || std::string_view(s1.data, s1.size) != "b") {
    return arrow::Status::Invalid("unexpected hash agg s values");
  }

  const auto& cnt_data = cnt_out->getData();
  const auto& sum_data = sum_i64->getData();
  if (cnt_data.size() != 2 || sum_data.size() != 2) {
    return arrow::Status::Invalid("unexpected hash agg output sizes");
  }
  if (cnt_data[0] != 3 || cnt_data[1] != 1 || sum_data[0] != 31 || sum_data[1] != 5) {
    return arrow::Status::Invalid("unexpected hash agg cnt/sum values");
  }

  if (sum_nullable->isNullAt(0) || sum_nullable->isNullAt(1)) {
    return arrow::Status::Invalid("unexpected null sums");
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
  const auto* eng = engine.get();

  tiforth::JoinKey key{.left = {"s", "d"}, .right = {"bs", "bd"}};
  ARROW_RETURN_NOT_OK(builder->AppendTransform([eng, build_batches, key]() -> arrow::Result<tiforth::TransformOpPtr> {
    return std::make_unique<tiforth::HashJoinTransformOp>(eng, build_batches, key);
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

arrow::Status RunUnicode0900HashJoinOnBlocks() {
  // Join build (bs, bv) with probe (s, pv) on s == bs (UTF8MB4_0900_AI_CI, no padding).
  ColumnsWithTypeAndName build_cols;
  {
    auto bs = ColumnString::create();
    bs->insertData("a", 1);
    bs->insertData("a ", 2);
    bs->insertData("b", 1);
    build_cols.emplace_back(std::move(bs), std::make_shared<DataTypeString>(), "bs");
  }
  {
    auto bv = ColumnInt32::create();
    bv->insert(Field(static_cast<Int64>(100)));
    bv->insert(Field(static_cast<Int64>(101)));
    bv->insert(Field(static_cast<Int64>(200)));
    build_cols.emplace_back(std::move(bv), std::make_shared<DataTypeInt32>(), "bv");
  }
  Block build_block(std::move(build_cols));

  ColumnsWithTypeAndName probe_cols;
  {
    auto s = ColumnString::create();
    s->insertData("A", 1);
    s->insertData("a ", 2);
    s->insertData("A ", 2);
    s->insertData("B", 1);
    probe_cols.emplace_back(std::move(s), std::make_shared<DataTypeString>(), "s");
  }
  {
    auto pv = ColumnInt32::create();
    pv->insert(Field(static_cast<Int64>(10)));
    pv->insert(Field(static_cast<Int64>(11)));
    pv->insert(Field(static_cast<Int64>(12)));
    pv->insert(Field(static_cast<Int64>(20)));
    probe_cols.emplace_back(std::move(pv), std::make_shared<DataTypeInt32>(), "pv");
  }
  Block probe_block(std::move(probe_cols));

  std::unordered_map<String, TiForth::ColumnOptions> build_options;
  build_options.emplace("bs", TiForth::ColumnOptions{.collation_id = 255});  // UTF8MB4_0900_AI_CI (NO PAD)
  std::unordered_map<String, TiForth::ColumnOptions> probe_options;
  probe_options.emplace("s", TiForth::ColumnOptions{.collation_id = 255});

  ARROW_ASSIGN_OR_RAISE(auto build_batch,
                        TiForth::toArrowRecordBatch(build_block, build_options,
                                                    arrow::default_memory_pool()));
  std::vector<std::shared_ptr<arrow::RecordBatch>> build_batches;
  build_batches.push_back(std::move(build_batch));

  ARROW_ASSIGN_OR_RAISE(auto engine, tiforth::Engine::Create(tiforth::EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, tiforth::PipelineBuilder::Create(engine.get()));
  const auto* eng = engine.get();

  tiforth::JoinKey key{.left = {"s"}, .right = {"bs"}};
  ARROW_RETURN_NOT_OK(builder->AppendTransform([eng, build_batches, key]() -> arrow::Result<tiforth::TransformOpPtr> {
    return std::make_unique<tiforth::HashJoinTransformOp>(eng, build_batches, key);
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
  if (block.columns() != 4 || block.rows() != 4) {
    return arrow::Status::Invalid("unexpected join output shape");
  }

  const auto s_opt = out.options_by_name.find("s");
  const auto bs_opt = out.options_by_name.find("bs");
  if (s_opt == out.options_by_name.end() || bs_opt == out.options_by_name.end()) {
    return arrow::Status::Invalid("missing string collation options in join output");
  }
  if (!s_opt->second.collation_id.has_value() || !bs_opt->second.collation_id.has_value() ||
      *s_opt->second.collation_id != 255 || *bs_opt->second.collation_id != 255) {
    return arrow::Status::Invalid("join output collation id mismatch");
  }

  const auto* out_s = typeid_cast<const ColumnString*>(block.getByPosition(0).column.get());
  const auto* out_pv = typeid_cast<const ColumnInt32*>(block.getByPosition(1).column.get());
  const auto* out_bs = typeid_cast<const ColumnString*>(block.getByPosition(2).column.get());
  const auto* out_bv = typeid_cast<const ColumnInt32*>(block.getByPosition(3).column.get());
  if (out_s == nullptr || out_pv == nullptr || out_bs == nullptr || out_bv == nullptr) {
    return arrow::Status::Invalid("unexpected join output column types");
  }

  const auto s0 = out_s->getDataAt(0);
  const auto s1 = out_s->getDataAt(1);
  const auto s2 = out_s->getDataAt(2);
  const auto s3 = out_s->getDataAt(3);
  if (std::string_view(s0.data, s0.size) != "A" || std::string_view(s1.data, s1.size) != "a " ||
      std::string_view(s2.data, s2.size) != "A " || std::string_view(s3.data, s3.size) != "B") {
    return arrow::Status::Invalid("unexpected probe s values");
  }

  const auto bs0 = out_bs->getDataAt(0);
  const auto bs1 = out_bs->getDataAt(1);
  const auto bs2 = out_bs->getDataAt(2);
  const auto bs3 = out_bs->getDataAt(3);
  if (std::string_view(bs0.data, bs0.size) != "a" || std::string_view(bs1.data, bs1.size) != "a " ||
      std::string_view(bs2.data, bs2.size) != "a " || std::string_view(bs3.data, bs3.size) != "b") {
    return arrow::Status::Invalid("unexpected build bs values");
  }

  const auto& pv_data = out_pv->getData();
  const auto& bv_data = out_bv->getData();
  if (pv_data.size() != 4 || bv_data.size() != 4) {
    return arrow::Status::Invalid("unexpected join output sizes");
  }
  if (pv_data[0] != 10 || pv_data[1] != 11 || pv_data[2] != 12 || pv_data[3] != 20 ||
      bv_data[0] != 100 || bv_data[1] != 101 || bv_data[2] != 101 || bv_data[3] != 200) {
    return arrow::Status::Invalid("unexpected join pv/bv values");
  }

  return arrow::Status::OK();
}

}  // namespace

TEST(TiForthBlockRunnerTest, FilterOnBlockWithCollation) {
  auto status = RunFilterOnBlockWithCollation();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthBlockRunnerTest, FilterMixedStringCollationCoercibility) {
  auto status = RunFilterOnBlockWithMixedCollation();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthBlockRunnerTest, ProjectionDecimalAdd) {
  auto status = RunProjectionDecimalAddOnBlock();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthBlockRunnerTest, ProjectionDecimalAddInt) {
  auto status = RunProjectionDecimalAddIntOnBlock();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthBlockRunnerTest, ProjectionTiDBPackedMyTimeScalars) {
  auto status = RunProjectionTiDBPackedMyTimeScalarsOnBlock();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthBlockRunnerTest, TwoKeyHashAgg) {
  auto status = RunTwoKeyHashAggOnBlocks();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthBlockRunnerTest, GeneralCiHashAgg) {
  auto status = RunGeneralCiHashAggOnBlocks();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthBlockRunnerTest, TwoKeyHashJoin) {
  auto status = RunTwoKeyHashJoinOnBlocks();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthBlockRunnerTest, Unicode0900HashJoin) {
  auto status = RunUnicode0900HashJoinOnBlocks();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

}  // namespace DB::tests

#else

TEST(TiForthBlockRunnerTest, Disabled) {
  GTEST_SKIP() << "TiForth integration is disabled (ENABLE_TIFORTH=OFF)";
}

#endif
