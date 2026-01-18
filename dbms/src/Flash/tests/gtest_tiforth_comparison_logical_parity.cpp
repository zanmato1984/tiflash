#include <gtest/gtest.h>

#if defined(TIFLASH_ENABLE_TIFORTH)

#include <Core/Block.h>
#include <Flash/TiForth/BlockPipelineRunner.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TiDB/Collation/Collator.h>

#include <arrow/result.h>
#include <arrow/status.h>

#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "tiforth/engine.h"
#include "tiforth/expr.h"
#include "tiforth/operators/projection.h"
#include "tiforth/pipeline.h"

namespace DB::tests {

namespace {

using ColumnOptionsByName = std::unordered_map<String, TiForth::ColumnOptions>;

arrow::Result<ColumnWithTypeAndName> EvalTiForthProjection(
    const Block& input, std::string_view out_name, const std::shared_ptr<tiforth::Expr>& expr,
    const ColumnOptionsByName& input_options_by_name) {
  if (expr == nullptr) {
    return arrow::Status::Invalid("expr must not be null");
  }

  ARROW_ASSIGN_OR_RAISE(auto engine, tiforth::Engine::Create(tiforth::EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, tiforth::PipelineBuilder::Create(engine.get()));

  std::vector<tiforth::ProjectionExpr> exprs;
  exprs.push_back({std::string(out_name), expr});

  ARROW_RETURN_NOT_OK(builder->AppendTransform(
      [engine_ptr = engine.get(), exprs]() -> arrow::Result<tiforth::TransformOpPtr> {
        return std::make_unique<tiforth::ProjectionTransformOp>(engine_ptr, exprs);
      }));

  ARROW_ASSIGN_OR_RAISE(auto pipeline, builder->Finalize());

  ARROW_ASSIGN_OR_RAISE(auto outputs,
                        TiForth::RunTiForthPipelineOnBlocks(*pipeline, {input},
                                                            input_options_by_name,
                                                            arrow::default_memory_pool()));
  if (outputs.size() != 1) {
    return arrow::Status::Invalid("expected exactly 1 output block");
  }

  const auto& out = outputs[0].block;
  if (!out.has(std::string(out_name))) {
    return arrow::Status::Invalid("missing output column: ", out_name);
  }
  return out.getByName(std::string(out_name));
}

arrow::Status CheckArgsParity(FunctionTest& test, const String& tiflash_func,
                              std::string_view tiforth_func,
                              const ColumnsWithTypeAndName& args,
                              const ColumnOptionsByName& input_options_by_name,
                              TiDB::TiDBCollatorPtr collator = nullptr,
                              bool raw_function_test = false) {
  ColumnWithTypeAndName expected;
  try {
    expected = test.executeFunction(tiflash_func, args, collator, raw_function_test);
  } catch (const Exception& e) {
    return arrow::Status::Invalid("TiFlash function threw: ", e.message());
  }

  Block input(args);
  std::vector<tiforth::ExprPtr> call_args;
  call_args.reserve(args.size());
  for (const auto& arg : args) {
    call_args.push_back(tiforth::MakeFieldRef(arg.name));
  }
  auto expr = tiforth::MakeCall(std::string(tiforth_func), std::move(call_args));
  ARROW_ASSIGN_OR_RAISE(auto actual, EvalTiForthProjection(input, /*out_name=*/"out", expr,
                                                           input_options_by_name));

  if (auto cmp = DB::tests::columnEqual(expected, actual); !cmp) {
    return arrow::Status::Invalid(cmp.message());
  }
  return arrow::Status::OK();
}

}  // namespace

TEST_F(FunctionTest, TiForthComparisonParityInt32) {
  const auto a = createColumn<Nullable<Int32>>({Int32(1), Int32(2), {}, Int32(2)}, "a");
  const auto b = createColumn<Nullable<Int32>>({Int32(1), Int32(1), Int32(1), {}}, "b");
  ColumnOptionsByName no_options;

  {
    auto status = CheckArgsParity(*this, /*tiflash_func=*/"equals", /*tiforth_func=*/"equal", {a, b},
                                  no_options);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }
  {
    auto status = CheckArgsParity(*this, /*tiflash_func=*/"notEquals", /*tiforth_func=*/"not_equal",
                                  {a, b}, no_options);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }
  {
    auto status = CheckArgsParity(*this, /*tiflash_func=*/"less", /*tiforth_func=*/"less", {a, b},
                                  no_options);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }
  {
    auto status = CheckArgsParity(*this, /*tiflash_func=*/"lessOrEquals", /*tiforth_func=*/"less_equal",
                                  {a, b}, no_options);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }
  {
    auto status = CheckArgsParity(*this, /*tiflash_func=*/"greater", /*tiforth_func=*/"greater", {a, b},
                                  no_options);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }
  {
    auto status = CheckArgsParity(*this, /*tiflash_func=*/"greaterOrEquals",
                                  /*tiforth_func=*/"greater_equal", {a, b}, no_options);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }
}

TEST_F(FunctionTest, TiForthComparisonParityDecimal128) {
  using DecimalField128 = DecimalField<Decimal128>;

  const auto a = createColumn<Nullable<Decimal128>>(std::make_tuple(10, 2),
                                                    {DecimalField128(Int128(123), 2),
                                                     DecimalField128(Int128(-500), 2),
                                                     {},
                                                     DecimalField128(Int128(0), 2)},
                                                    "a");
  const auto b = createColumn<Nullable<Decimal128>>(std::make_tuple(10, 2),
                                                    {DecimalField128(Int128(123), 2),
                                                     DecimalField128(Int128(100), 2),
                                                     DecimalField128(Int128(1), 2),
                                                     {}},
                                                    "b");
  ColumnOptionsByName no_options;

  {
    auto status = CheckArgsParity(*this, /*tiflash_func=*/"equals", /*tiforth_func=*/"equal", {a, b},
                                  no_options,
                                  /*collator=*/nullptr,
                                  /*raw_function_test=*/true);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }
  {
    auto status =
        CheckArgsParity(*this, /*tiflash_func=*/"less", /*tiforth_func=*/"less", {a, b}, no_options,
                        /*collator=*/nullptr,
                        /*raw_function_test=*/true);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }
}

TEST_F(FunctionTest, TiForthComparisonParityCollatedStringPaddingBinary) {
  const auto lhs =
      createColumn<Nullable<String>>({String("a "), String("a"), String("A"), {}, String("b ")}, "lhs");
  const auto rhs =
      createColumn<Nullable<String>>({String("a"), String("a "), String("a"), String("a"), {}}, "rhs");

  ColumnOptionsByName options;
  options.emplace("lhs", TiForth::ColumnOptions{.collation_id = 46});  // UTF8MB4_BIN (PAD SPACE)
  options.emplace("rhs", TiForth::ColumnOptions{.collation_id = 46});

  const auto* collator = TiDB::ITiDBCollator::getCollator(46);
  ASSERT_TRUE(collator != nullptr);

  auto status = CheckArgsParity(*this, /*tiflash_func=*/"equals", /*tiforth_func=*/"equal", {lhs, rhs},
                                options, collator,
                                /*raw_function_test=*/true);
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST_F(FunctionTest, TiForthComparisonParityCollatedStringGeneralCi) {
  const auto lhs =
      createColumn<Nullable<String>>({String("a "), String("A"), String("b"), {}, String("c")}, "lhs");
  const auto rhs =
      createColumn<Nullable<String>>({String("a"), String("a"), String("b "), String("x"), {}}, "rhs");

  ColumnOptionsByName options;
  options.emplace("lhs", TiForth::ColumnOptions{.collation_id = 33});  // UTF8_GENERAL_CI
  options.emplace("rhs", TiForth::ColumnOptions{.collation_id = 33});

  const auto* collator = TiDB::ITiDBCollator::getCollator(33);
  ASSERT_TRUE(collator != nullptr);

  auto status = CheckArgsParity(*this, /*tiflash_func=*/"equals", /*tiforth_func=*/"equal", {lhs, rhs},
                                options, collator,
                                /*raw_function_test=*/true);
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST_F(FunctionTest, TiForthComparisonParityCollatedStringBinaryWinsOverPaddingBinary) {
  const auto lhs = createColumn<Nullable<String>>({String("a "), String("a"), String("b")}, "lhs");
  const auto rhs = createColumn<Nullable<String>>({String("a"), String("a "), String("b ")}, "rhs");

  ColumnOptionsByName options;
  options.emplace("lhs", TiForth::ColumnOptions{.collation_id = 63});  // BINARY
  options.emplace("rhs", TiForth::ColumnOptions{.collation_id = 46});  // UTF8MB4_BIN (PAD SPACE)

  // TiFlash resolves mixed BINARY vs padding BIN to BINARY; passing nullptr uses the binary comparator.
  auto status = CheckArgsParity(*this, /*tiflash_func=*/"equals", /*tiforth_func=*/"equal", {lhs, rhs},
                                options,
                                /*collator=*/nullptr,
                                /*raw_function_test=*/true);
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST_F(FunctionTest, TiForthLogicalParityAndOrXorNot) {
  const auto a = createColumn<Nullable<Int32>>({Int32(1), Int32(0), {}, Int32(2)}, "a");
  const auto b = createColumn<Nullable<Int32>>({Int32(1), Int32(1), Int32(0), {}}, "b");
  const auto c = createColumn<Nullable<Int32>>({Int32(1), Int32(0), Int32(1), Int32(1)}, "c");
  ColumnOptionsByName no_options;

  {
    auto status = CheckArgsParity(*this, /*tiflash_func=*/"and", /*tiforth_func=*/"and", {a, b}, no_options);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }
  {
    auto status = CheckArgsParity(*this, /*tiflash_func=*/"or", /*tiforth_func=*/"or", {a, b}, no_options);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }
  {
    auto status = CheckArgsParity(*this, /*tiflash_func=*/"xor", /*tiforth_func=*/"xor", {a, b}, no_options);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }
  {
    auto status = CheckArgsParity(*this, /*tiflash_func=*/"not", /*tiforth_func=*/"not", {a}, no_options);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }
  {
    auto status = CheckArgsParity(*this, /*tiflash_func=*/"and", /*tiforth_func=*/"and", {a, b, c}, no_options);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }
  {
    auto status = CheckArgsParity(*this, /*tiflash_func=*/"or", /*tiforth_func=*/"or", {a, b, c}, no_options);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }
  {
    auto status = CheckArgsParity(*this, /*tiflash_func=*/"xor", /*tiforth_func=*/"xor", {a, b, c}, no_options);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }
}

}  // namespace DB::tests

#endif  // defined(TIFLASH_ENABLE_TIFORTH)

