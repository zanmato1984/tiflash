#include <gtest/gtest.h>

#if defined(TIFLASH_ENABLE_TIFORTH)

#include <Core/Block.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Flash/TiForth/BlockPipelineRunner.h>
#include <TestUtils/FunctionTestUtils.h>

#include <arrow/result.h>
#include <arrow/status.h>

#include <limits>
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

using DecimalField32 = DecimalField<Decimal32>;
using DecimalField64 = DecimalField<Decimal64>;
using DecimalField128 = DecimalField<Decimal128>;
using DecimalField256 = DecimalField<Decimal256>;

arrow::Result<ColumnWithTypeAndName> EvalTiForthProjection(const Block& input,
                                                           std::string_view out_name,
                                                           const std::shared_ptr<tiforth::Expr>& expr) {
  if (expr == nullptr) {
    return arrow::Status::Invalid("expr must not be null");
  }

  ARROW_ASSIGN_OR_RAISE(auto engine, tiforth::Engine::Create(tiforth::EngineOptions{}));
  ARROW_ASSIGN_OR_RAISE(auto builder, tiforth::PipelineBuilder::Create(engine.get()));

  std::vector<tiforth::ProjectionExpr> exprs;
  exprs.push_back({std::string(out_name), expr});

  ARROW_RETURN_NOT_OK(builder->AppendPipe(
      [engine_ptr = engine.get(), exprs]()
          -> arrow::Result<std::unique_ptr<tiforth::pipeline::PipeOp>> {
        return std::make_unique<tiforth::ProjectionPipeOp>(engine_ptr, exprs);
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
  if (!out.has(std::string(out_name))) {
    return arrow::Status::Invalid("missing output column: ", out_name);
  }
  return out.getByName(std::string(out_name));
}

arrow::Status CheckBinaryParity(FunctionTest& test, const String& tiflash_func,
                                std::string_view tiforth_func, const ColumnWithTypeAndName& lhs,
                                const ColumnWithTypeAndName& rhs, bool raw_function_test = false) {
  ColumnWithTypeAndName expected;
  try {
    expected = test.executeFunction(
        tiflash_func, ColumnsWithTypeAndName{lhs, rhs}, /*collator=*/nullptr, raw_function_test);
  } catch (const Exception& e) {
    return arrow::Status::Invalid("TiFlash function threw: ", e.message());
  }

  Block input({lhs, rhs});
  auto expr = tiforth::MakeCall(std::string(tiforth_func),
                                {tiforth::MakeFieldRef(lhs.name), tiforth::MakeFieldRef(rhs.name)});
  ARROW_ASSIGN_OR_RAISE(auto actual, EvalTiForthProjection(input, /*out_name=*/"out", expr));

  if (auto cmp = DB::tests::columnEqual(expected, actual); !cmp) {
    return arrow::Status::Invalid(cmp.message());
  }
  return arrow::Status::OK();
}

arrow::Status CheckBinaryFailureParity(FunctionTest& test, const String& tiflash_func,
                                       std::string_view tiforth_func, const ColumnWithTypeAndName& lhs,
                                       const ColumnWithTypeAndName& rhs, bool raw_function_test = false) {
  bool tiflash_failed = false;
  try {
    (void)test.executeFunction(
        tiflash_func, ColumnsWithTypeAndName{lhs, rhs}, /*collator=*/nullptr, raw_function_test);
  } catch (...) {
    tiflash_failed = true;
  }

  Block input({lhs, rhs});
  auto expr = tiforth::MakeCall(std::string(tiforth_func),
                                {tiforth::MakeFieldRef(lhs.name), tiforth::MakeFieldRef(rhs.name)});
  const auto tiforth_res = EvalTiForthProjection(input, /*out_name=*/"out", expr);

  if (!tiflash_failed) {
    return arrow::Status::Invalid("expected TiFlash failure, got success");
  }
  if (tiforth_res.ok()) {
    return arrow::Status::Invalid("expected TiForth failure, got success");
  }
  return arrow::Status::OK();
}

arrow::Status CheckUnaryParity(FunctionTest& test, const String& tiflash_func, std::string_view tiforth_func,
                               const ColumnWithTypeAndName& arg, bool raw_function_test = false) {
  ColumnWithTypeAndName expected;
  try {
    expected = test.executeFunction(
        tiflash_func, ColumnsWithTypeAndName{arg}, /*collator=*/nullptr, raw_function_test);
  } catch (const Exception& e) {
    return arrow::Status::Invalid("TiFlash function threw: ", e.message());
  }

  Block input({arg});
  auto expr = tiforth::MakeCall(std::string(tiforth_func), {tiforth::MakeFieldRef(arg.name)});
  ARROW_ASSIGN_OR_RAISE(auto actual, EvalTiForthProjection(input, /*out_name=*/"out", expr));

  if (auto cmp = DB::tests::columnEqual(expected, actual); !cmp) {
    return arrow::Status::Invalid(cmp.message());
  }
  return arrow::Status::OK();
}

arrow::Status CheckUnaryFailureParity(FunctionTest& test, const String& tiflash_func, std::string_view tiforth_func,
                                      const ColumnWithTypeAndName& arg, bool raw_function_test = false) {
  bool tiflash_failed = false;
  try {
    (void)test.executeFunction(
        tiflash_func, ColumnsWithTypeAndName{arg}, /*collator=*/nullptr, raw_function_test);
  } catch (...) {
    tiflash_failed = true;
  }

  Block input({arg});
  auto expr = tiforth::MakeCall(std::string(tiforth_func), {tiforth::MakeFieldRef(arg.name)});
  const auto tiforth_res = EvalTiForthProjection(input, /*out_name=*/"out", expr);

  if (!tiflash_failed) {
    return arrow::Status::Invalid("expected TiFlash failure, got success");
  }
  if (tiforth_res.ok()) {
    return arrow::Status::Invalid("expected TiForth failure, got success");
  }
  return arrow::Status::OK();
}

}  // namespace

TEST_F(FunctionTest, TiForthArithmeticParityDecimalAddSubtract) {
  // Use nullable inputs so both TiFlash and TiForth results are Nullable(...) types.
  const auto a = createColumn<Nullable<Decimal128>>(std::make_tuple(10, 2),
                                                    {DecimalField128(Int128(123), 2),
                                                     {},
                                                     DecimalField128(Int128(1000), 2)},
                                                    "a");
  const auto b = createColumn<Nullable<Decimal128>>(std::make_tuple(10, 4),
                                                    {DecimalField128(Int128(100), 4),
                                                     DecimalField128(Int128(200), 4),
                                                     DecimalField128(Int128(-500), 4)},
                                                    "b");

  {
    auto status = CheckBinaryParity(*this, /*tiflash_func=*/"plus", /*tiforth_func=*/"add", a, b);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }
  {
    auto status =
        CheckBinaryParity(*this, /*tiflash_func=*/"minus", /*tiforth_func=*/"subtract", a, b);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }
}

TEST_F(FunctionTest, TiForthArithmeticParityDecimalAddMixedInt) {
  const auto a = createColumn<Nullable<Decimal128>>(std::make_tuple(10, 2),
                                                    {DecimalField128(Int128(123), 2),
                                                     {},
                                                     DecimalField128(Int128(1000), 2)},
                                                    "a");
  const auto i = createColumn<Nullable<Int32>>({Int32(2), Int32(3), Int32(-5)}, "i");

  auto status = CheckBinaryParity(*this, /*tiflash_func=*/"plus", /*tiforth_func=*/"add", a, i);
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST_F(FunctionTest, TiForthArithmeticParityDecimalMultiply) {
  // scale(20)+scale(20) is clamped to 30; TiFlash truncates toward zero for scale reduction.
  const auto a = createColumn<Nullable<Decimal128>>(std::make_tuple(30, 20),
                                                    {std::string("-0.01234567890123456789")},
                                                    "a");
  const auto b = createColumn<Nullable<Decimal128>>(std::make_tuple(30, 20),
                                                    {std::string("0.00000000000000000001")},
                                                    "b");

  auto status = CheckBinaryParity(*this, /*tiflash_func=*/"multiply", /*tiforth_func=*/"multiply", a, b);
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST_F(FunctionTest, TiForthArithmeticParityDecimalDivide) {
  const auto a = createColumn<Nullable<Decimal128>>(std::make_tuple(10, 2),
                                                    {DecimalField128(Int128(123), 2),
                                                     DecimalField128(Int128(-123), 2),
                                                     {}},
                                                    "a");
  const auto b = createColumn<Nullable<Decimal128>>(std::make_tuple(10, 2),
                                                    {DecimalField128(Int128(200), 2),
                                                     DecimalField128(Int128(200), 2),
                                                     DecimalField128(Int128(200), 2)},
                                                    "b");

  auto status = CheckBinaryParity(*this, /*tiflash_func=*/"divide", /*tiforth_func=*/"divide", a, b,
                                  /*raw_function_test=*/true);
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST_F(FunctionTest, TiForthArithmeticParityDecimalTiDBDivideAndModulo) {
  // Borrow the rounding cases from TiFlash gtest_arithmetic_functions.cpp.
  const auto lhs_i = createColumn<Nullable<Int32>>({Int32(1), Int32(1), Int32(1), Int32(1), Int32(1)}, "lhs_i");
  const auto rhs_dec = createColumn<Nullable<Decimal32>>(std::make_tuple(20, 4),
                                                         {DecimalField32(Int32(100000000), 4),
                                                          DecimalField32(Int32(100010000), 4),
                                                          DecimalField32(Int32(199990000), 4),
                                                          DecimalField32(Int32(200000000), 4),
                                                          DecimalField32(Int32(200010000), 4)},
                                                         "rhs_dec");

  {
    auto status = CheckBinaryParity(*this, /*tiflash_func=*/"tidbDivide", /*tiforth_func=*/"tidbDivide", lhs_i, rhs_dec);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }
  {
    // Division-by-zero yields NULL under TiDB semantics.
    const auto lhs0 = createColumn<Nullable<Decimal32>>(std::make_tuple(10, 0),
                                                       {DecimalField32(Int32(1), 0),
                                                        DecimalField32(Int32(2), 0)},
                                                       "lhs0");
    const auto rhs0 = createColumn<Nullable<Decimal32>>(std::make_tuple(10, 0),
                                                       {DecimalField32(Int32(0), 0),
                                                        DecimalField32(Int32(1), 0)},
                                                       "rhs0");
    auto status = CheckBinaryParity(*this, /*tiflash_func=*/"tidbDivide", /*tiforth_func=*/"tidbDivide", lhs0, rhs0);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }

  // modulo: include scaling + divisor=0 -> NULL.
  const auto m_lhs = createColumn<Nullable<Decimal128>>(std::make_tuple(18, 2),
                                                        {DecimalField128(Int128(12345), 2),
                                                         DecimalField128(Int128(-12345), 2),
                                                         DecimalField128(Int128(12345), 2)},
                                                        "m_lhs");
  const auto m_rhs = createColumn<Nullable<Decimal128>>(std::make_tuple(18, 0),
                                                        {DecimalField128(Int128(100), 0),
                                                         DecimalField128(Int128(100), 0),
                                                         DecimalField128(Int128(0), 0)},
                                                        "m_rhs");

  {
    auto status = CheckBinaryParity(*this, /*tiflash_func=*/"modulo", /*tiforth_func=*/"modulo", m_lhs, m_rhs);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }
}

TEST_F(FunctionTest, TiForthArithmeticParityDecimalOverflowIsError) {
  const std::string max_65_digits(65, '9');
  const auto lhs = createColumn<Nullable<Decimal256>>(std::make_tuple(65, 0), {max_65_digits}, "lhs");
  const auto rhs = createColumn<Nullable<Decimal256>>(std::make_tuple(65, 0), {std::string("1")}, "rhs");

  auto status = CheckBinaryFailureParity(*this, /*tiflash_func=*/"plus", /*tiforth_func=*/"add", lhs, rhs);
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST_F(FunctionTest, TiForthArithmeticParityBitwise) {
  {
    const auto lhs = createColumn<Nullable<Int64>>({Int64(-1), Int64(1), Int64(5), {}, std::numeric_limits<Int64>::max()},
                                                  "lhs");
    const auto rhs = createColumn<Nullable<Int64>>({Int64(0), Int64(3), Int64(1), Int64(4), Int64(-1)}, "rhs");
    auto status = CheckBinaryParity(*this, /*tiflash_func=*/"bitAnd", /*tiforth_func=*/"bitAnd", lhs, rhs);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }
  {
    const auto lhs = createColumn<Nullable<Int16>>({Int16(-1), Int16(0), Int16(1), {}}, "lhs");
    const auto rhs = createColumn<Nullable<UInt32>>({UInt32(0), UInt32(1), UInt32(0), UInt32(1)}, "rhs");
    auto status = CheckBinaryParity(*this, /*tiflash_func=*/"bitOr", /*tiforth_func=*/"bitOr", lhs, rhs);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }
  {
    const auto lhs = createColumn<Nullable<UInt64>>(
        {UInt64(0), std::numeric_limits<UInt64>::max(), UInt64(1), {}}, "lhs");
    const auto rhs = createColumn<Nullable<Int8>>({Int8(0), Int8(1), Int8(-1), Int8(0)}, "rhs");
    auto status = CheckBinaryParity(*this, /*tiflash_func=*/"bitXor", /*tiforth_func=*/"bitXor", lhs, rhs);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }
  {
    const auto arg = createColumn<Nullable<Int64>>(
        {Int64(-1), Int64(0), Int64(1), std::numeric_limits<Int64>::min(), {}}, "arg");
    auto status = CheckUnaryParity(*this, /*tiflash_func=*/"bitNot", /*tiforth_func=*/"bitNot", arg);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }
  {
    const auto lhs = createColumn<Nullable<Int64>>({Int64(0),
                                                   Int64(1),
                                                   Int64(-1),
                                                   std::numeric_limits<Int64>::max(),
                                                   std::numeric_limits<Int64>::min(),
                                                   {}},
                                                  "lhs");
    const auto rhs = createColumn<Nullable<UInt64>>({UInt64(0),
                                                     UInt64(63),
                                                     UInt64(1),
                                                     UInt64(64),
                                                     std::numeric_limits<UInt64>::max(),
                                                     UInt64(1)},
                                                    "rhs");
    {
      auto status = CheckBinaryParity(*this, /*tiflash_func=*/"bitShiftLeft", /*tiforth_func=*/"bitShiftLeft", lhs, rhs);
      ASSERT_TRUE(status.ok()) << status.ToString();
    }
    {
      auto status = CheckBinaryParity(*this, /*tiflash_func=*/"bitShiftRight", /*tiforth_func=*/"bitShiftRight", lhs, rhs);
      ASSERT_TRUE(status.ok()) << status.ToString();
    }
  }
}

TEST_F(FunctionTest, TiForthArithmeticParityAbsNegate) {
  {
    const auto arg = createColumn<Nullable<Int64>>({Int64(-123), Int64(0), Int64(123), {}}, "arg");
    auto status = CheckUnaryParity(*this, /*tiflash_func=*/"abs", /*tiforth_func=*/"abs", arg);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }
  {
    const auto arg = createColumn<Nullable<Int64>>({std::numeric_limits<Int64>::min()}, "arg");
    auto status = CheckUnaryFailureParity(*this, /*tiflash_func=*/"abs", /*tiforth_func=*/"abs", arg);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }
  {
    const auto arg = createColumn<Nullable<UInt32>>({UInt32(0), UInt32(1), UInt32(0xffffffffu), {}}, "arg");
    auto status = CheckUnaryParity(*this, /*tiflash_func=*/"abs", /*tiforth_func=*/"abs", arg);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }

  {
    const auto arg = createColumn<Nullable<Int64>>(
        {Int64(123), Int64(-123), Int64(0), std::numeric_limits<Int64>::min(), {}}, "arg");
    auto status = CheckUnaryParity(*this, /*tiflash_func=*/"negate", /*tiforth_func=*/"negate", arg);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }
  {
    const auto arg = createColumn<Nullable<UInt8>>({UInt8(0), UInt8(1), UInt8(255), {}}, "arg");
    auto status = CheckUnaryParity(*this, /*tiflash_func=*/"negate", /*tiforth_func=*/"negate", arg);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }
  {
    const auto arg = createColumn<Nullable<UInt64>>(
        {UInt64(0), UInt64(1), std::numeric_limits<UInt64>::max(), UInt64(9223372036854775808ull), {}}, "arg");
    auto status = CheckUnaryParity(*this, /*tiflash_func=*/"negate", /*tiforth_func=*/"negate", arg);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }
}

TEST_F(FunctionTest, TiForthArithmeticParityIntDivAndModulo) {
  {
    const auto lhs = createColumn<Nullable<Int64>>({Int64(10), Int64(-10), std::numeric_limits<Int64>::min(), {}}, "lhs");
    const auto rhs = createColumn<Nullable<Int64>>({Int64(3), Int64(3), Int64(2), Int64(1)}, "rhs");
    auto status = CheckBinaryParity(*this, /*tiflash_func=*/"intDiv", /*tiforth_func=*/"intDiv", lhs, rhs);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }
  {
    const auto lhs = createColumn<Nullable<UInt32>>({UInt32(10), UInt32(10), UInt32(0xffffffffu), {}}, "lhs");
    const auto rhs = createColumn<Nullable<Int8>>({Int8(3), Int8(-3), Int8(1), Int8(1)}, "rhs");
    auto status = CheckBinaryParity(*this, /*tiflash_func=*/"intDiv", /*tiforth_func=*/"intDiv", lhs, rhs);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }

  {
    const auto lhs = createColumn<Nullable<Int64>>({Int64(1)}, "lhs");
    const auto rhs = createColumn<Nullable<Int64>>({Int64(0)}, "rhs");
    auto status = CheckBinaryFailureParity(*this, /*tiflash_func=*/"intDiv", /*tiforth_func=*/"intDiv", lhs, rhs);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }
  {
    const auto lhs = createColumn<Nullable<Int64>>({std::numeric_limits<Int64>::min()}, "lhs");
    const auto rhs = createColumn<Nullable<Int64>>({Int64(-1)}, "rhs");
    auto status = CheckBinaryFailureParity(*this, /*tiflash_func=*/"intDiv", /*tiforth_func=*/"intDiv", lhs, rhs);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }
  {
    const auto lhs = createColumn<Nullable<Int64>>({Int64(1), std::numeric_limits<Int64>::min()}, "lhs");
    const auto rhs = createColumn<Nullable<Int64>>({Int64(0), Int64(-1)}, "rhs");
    auto status = CheckBinaryParity(*this, /*tiflash_func=*/"intDivOrZero", /*tiforth_func=*/"intDivOrZero", lhs, rhs,
                                    /*raw_function_test=*/true);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }

  {
    const auto lhs = createColumn<Nullable<Int64>>({Int64(5), Int64(-5), Int64(5), {}}, "lhs");
    const auto rhs = createColumn<Nullable<Int64>>({Int64(3), Int64(3), Int64(0), Int64(1)}, "rhs");
    auto status = CheckBinaryParity(*this, /*tiflash_func=*/"modulo", /*tiforth_func=*/"modulo", lhs, rhs);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }
  {
    const auto lhs = createColumn<Nullable<UInt64>>({UInt64(5), UInt64(5), UInt64(0), {}}, "lhs");
    const auto rhs = createColumn<Nullable<Int64>>({Int64(-3), Int64(3), Int64(7), Int64(1)}, "rhs");
    auto status = CheckBinaryParity(*this, /*tiflash_func=*/"modulo", /*tiforth_func=*/"modulo", lhs, rhs);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }
  {
    const auto lhs = createColumn<Nullable<Int64>>({std::numeric_limits<Int64>::min()}, "lhs");
    const auto rhs = createColumn<Nullable<Int64>>({Int64(-1)}, "rhs");
    auto status = CheckBinaryParity(*this, /*tiflash_func=*/"modulo", /*tiforth_func=*/"modulo", lhs, rhs);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }
}

TEST_F(FunctionTest, TiForthArithmeticParityGcdLcm) {
  {
    const auto lhs = createColumn<Nullable<Int32>>({Int32(12), Int32(-12), Int32(18)}, "lhs");
    const auto rhs = createColumn<Nullable<Int32>>({Int32(18), Int32(18), Int32(12)}, "rhs");
    {
      auto status = CheckBinaryParity(*this, /*tiflash_func=*/"gcd", /*tiforth_func=*/"gcd", lhs, rhs,
                                      /*raw_function_test=*/true);
      ASSERT_TRUE(status.ok()) << status.ToString();
    }
    {
      auto status = CheckBinaryParity(*this, /*tiflash_func=*/"lcm", /*tiforth_func=*/"lcm", lhs, rhs,
                                      /*raw_function_test=*/true);
      ASSERT_TRUE(status.ok()) << status.ToString();
    }
  }
  {
    const auto lhs = createColumn<Nullable<Int32>>({Int32(0)}, "lhs");
    const auto rhs = createColumn<Nullable<Int32>>({Int32(5)}, "rhs");
    {
      auto status = CheckBinaryFailureParity(*this, /*tiflash_func=*/"gcd", /*tiforth_func=*/"gcd", lhs, rhs,
                                             /*raw_function_test=*/true);
      ASSERT_TRUE(status.ok()) << status.ToString();
    }
    {
      auto status = CheckBinaryFailureParity(*this, /*tiflash_func=*/"lcm", /*tiforth_func=*/"lcm", lhs, rhs,
                                             /*raw_function_test=*/true);
      ASSERT_TRUE(status.ok()) << status.ToString();
    }
  }
  {
    const auto lhs = createColumn<Nullable<Int64>>({std::numeric_limits<Int64>::min()}, "lhs");
    const auto rhs = createColumn<Nullable<Int64>>({Int64(-1)}, "rhs");
    {
      auto status = CheckBinaryFailureParity(*this, /*tiflash_func=*/"gcd", /*tiforth_func=*/"gcd", lhs, rhs,
                                             /*raw_function_test=*/true);
      ASSERT_TRUE(status.ok()) << status.ToString();
    }
    {
      auto status = CheckBinaryFailureParity(*this, /*tiflash_func=*/"lcm", /*tiforth_func=*/"lcm", lhs, rhs,
                                             /*raw_function_test=*/true);
      ASSERT_TRUE(status.ok()) << status.ToString();
    }
  }
}

#else

TEST(TiForthArithmeticParityTest, Disabled) {
  GTEST_SKIP() << "TiForth integration is disabled (ENABLE_TIFORTH=OFF)";
}

#endif // defined(TIFLASH_ENABLE_TIFORTH)

}  // namespace DB::tests
