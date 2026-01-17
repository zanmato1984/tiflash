#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/compute/exec.h>
#include <arrow/compute/function.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/util/decimal.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "tiforth/engine.h"

namespace tiforth {

namespace {

arrow::Result<std::shared_ptr<arrow::Array>> MakeInt32Array(
    const std::vector<std::optional<int32_t>>& values) {
  arrow::Int32Builder builder;
  for (const auto value : values) {
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

arrow::Result<std::shared_ptr<arrow::Array>> MakeInt64Array(
    const std::vector<std::optional<int64_t>>& values) {
  arrow::Int64Builder builder;
  for (const auto value : values) {
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

arrow::Result<std::shared_ptr<arrow::Array>> MakeDecimal128Array(
    int32_t precision, int32_t scale, const std::vector<std::optional<int64_t>>& unscaled_values) {
  auto type = arrow::decimal128(precision, scale);
  arrow::Decimal128Builder builder(type);
  for (const auto raw : unscaled_values) {
    if (raw.has_value()) {
      ARROW_RETURN_NOT_OK(builder.Append(arrow::Decimal128(*raw)));
    } else {
      ARROW_RETURN_NOT_OK(builder.AppendNull());
    }
  }
  std::shared_ptr<arrow::Array> out;
  ARROW_RETURN_NOT_OK(builder.Finish(&out));
  return out;
}

arrow::Result<std::shared_ptr<arrow::Array>> MakeDecimal256Array(
    int32_t precision, int32_t scale, const std::vector<std::optional<arrow::Decimal256>>& unscaled_values) {
  auto type = arrow::decimal256(precision, scale);
  arrow::Decimal256Builder builder(type);
  for (const auto& raw : unscaled_values) {
    if (raw.has_value()) {
      ARROW_RETURN_NOT_OK(builder.Append(*raw));
    } else {
      ARROW_RETURN_NOT_OK(builder.AppendNull());
    }
  }
  std::shared_ptr<arrow::Array> out;
  ARROW_RETURN_NOT_OK(builder.Finish(&out));
  return out;
}

arrow::Result<arrow::Datum> CallAdd(const Engine& engine, const arrow::Datum& lhs,
                                   const arrow::Datum& rhs) {
  arrow::compute::ExecContext ctx(engine.memory_pool(), /*executor=*/nullptr, engine.function_registry());
  return arrow::compute::CallFunction("add", {lhs, rhs}, /*options=*/nullptr, &ctx);
}

arrow::Status RunDecimal128PlusInt32() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));

  ARROW_ASSIGN_OR_RAISE(auto d,
                        MakeDecimal128Array(/*precision=*/10, /*scale=*/2,
                                            {int64_t{123}, std::nullopt, int64_t{1000}}));
  ARROW_ASSIGN_OR_RAISE(auto i, MakeInt32Array({int32_t{2}, int32_t{3}, int32_t{-5}}));

  ARROW_ASSIGN_OR_RAISE(auto out, CallAdd(*engine, arrow::Datum(d), arrow::Datum(i)));
  if (!out.is_array()) {
    return arrow::Status::Invalid("expected array output");
  }
  if (!out.type()->Equals(*arrow::decimal128(/*precision=*/13, /*scale=*/2))) {
    return arrow::Status::Invalid("unexpected decimal+int32 output type: ", out.type()->ToString());
  }

  ARROW_ASSIGN_OR_RAISE(
      auto expect,
      MakeDecimal128Array(/*precision=*/13, /*scale=*/2, {int64_t{323}, std::nullopt, int64_t{500}}));
  if (!expect->Equals(*out.make_array())) {
    return arrow::Status::Invalid("unexpected decimal+int32 output values");
  }

  // Validate commutativity (int32 + decimal128).
  ARROW_ASSIGN_OR_RAISE(auto out2, CallAdd(*engine, arrow::Datum(i), arrow::Datum(d)));
  if (!out2.is_array()) {
    return arrow::Status::Invalid("expected array output (swapped)");
  }
  if (!out2.type()->Equals(*arrow::decimal128(/*precision=*/13, /*scale=*/2))) {
    return arrow::Status::Invalid("unexpected int32+decimal output type: ", out2.type()->ToString());
  }
  if (!expect->Equals(*out2.make_array())) {
    return arrow::Status::Invalid("unexpected int32+decimal output values");
  }

  return arrow::Status::OK();
}

arrow::Status RunDecimal128PlusInt64PromotesToDecimal256() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));

  // 38-digit decimal + int64 should promote to Decimal256(39,0) per TiFlash PlusDecimalInferer.
  ARROW_ASSIGN_OR_RAISE(auto d, MakeDecimal128Array(/*precision=*/38, /*scale=*/0, {int64_t{1}}));
  ARROW_ASSIGN_OR_RAISE(auto i, MakeInt64Array({int64_t{2}}));

  ARROW_ASSIGN_OR_RAISE(auto out, CallAdd(*engine, arrow::Datum(d), arrow::Datum(i)));
  if (!out.is_array()) {
    return arrow::Status::Invalid("expected array output");
  }
  if (!out.type()->Equals(*arrow::decimal256(/*precision=*/39, /*scale=*/0))) {
    return arrow::Status::Invalid("unexpected decimal256 output type: ", out.type()->ToString());
  }

  ARROW_ASSIGN_OR_RAISE(auto expect, MakeDecimal256Array(/*precision=*/39, /*scale=*/0, {arrow::Decimal256(3)}));
  if (!expect->Equals(*out.make_array())) {
    return arrow::Status::Invalid("unexpected promoted decimal256 output values");
  }
  return arrow::Status::OK();
}

arrow::Status RunDecimalOverflowIsError() {
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Create(EngineOptions{}));

  // Decimal(65,0) max value + 1 should overflow (result precision clamped to 65).
  const std::string max_65_digits(65, '9');
  ARROW_ASSIGN_OR_RAISE(const auto lhs_val, arrow::Decimal256::FromString(max_65_digits));
  const arrow::Decimal256 rhs_val(1);

  ARROW_ASSIGN_OR_RAISE(auto lhs, MakeDecimal256Array(/*precision=*/65, /*scale=*/0, {lhs_val}));
  ARROW_ASSIGN_OR_RAISE(auto rhs, MakeDecimal256Array(/*precision=*/65, /*scale=*/0, {rhs_val}));

  auto out_res = CallAdd(*engine, arrow::Datum(lhs), arrow::Datum(rhs));
  if (out_res.ok()) {
    return arrow::Status::Invalid("expected overflow error, got OK");
  }
  const auto message = out_res.status().ToString();
  if (message.find("decimal math overflow") == std::string::npos) {
    return arrow::Status::Invalid("unexpected overflow error message: ", message);
  }
  return arrow::Status::OK();
}

}  // namespace

TEST(TiForthDecimalAddTest, Decimal128PlusInt32) {
  auto status = RunDecimal128PlusInt32();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthDecimalAddTest, Decimal128PlusInt64PromotesToDecimal256) {
  auto status = RunDecimal128PlusInt64PromotesToDecimal256();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

TEST(TiForthDecimalAddTest, DecimalOverflowIsError) {
  auto status = RunDecimalOverflowIsError();
  ASSERT_TRUE(status.ok()) << status.ToString();
}

}  // namespace tiforth

