#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/compute/api_scalar.h>
#include <arrow/compute/initialize.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/testing/gtest_util.h>

#include <cstdint>
#include <vector>

#include <gtest/gtest.h>

namespace tiforth {

namespace {

arrow::Result<std::shared_ptr<arrow::Array>> MakeInt32Array(
    const std::vector<int32_t>& values) {
  arrow::Int32Builder builder;
  for (const auto value : values) {
    ARROW_RETURN_NOT_OK(builder.Append(value));
  }

  std::shared_ptr<arrow::Array> array;
  ARROW_RETURN_NOT_OK(builder.Finish(&array));
  return array;
}

arrow::Status RunArrowComputeSmoke() {
  ARROW_RETURN_NOT_OK(arrow::compute::Initialize());

  ARROW_ASSIGN_OR_RAISE(auto lhs, MakeInt32Array({1, 2, 3}));
  ARROW_ASSIGN_OR_RAISE(auto rhs, MakeInt32Array({10, 20, 30}));

  ARROW_ASSIGN_OR_RAISE(auto sum,
                        arrow::compute::Add(arrow::Datum(lhs), arrow::Datum(rhs)));

  if (!sum.is_array()) {
    return arrow::Status::Invalid("expected array result");
  }

  auto sum_array = sum.make_array();
  if (sum_array == nullptr) {
    return arrow::Status::Invalid("expected non-null result array");
  }
  if (sum_array->type_id() != arrow::Type::INT32) {
    return arrow::Status::Invalid("expected int32 result array");
  }

  auto sum_i32 = std::static_pointer_cast<arrow::Int32Array>(sum_array);
  if (sum_i32->length() != 3) {
    return arrow::Status::Invalid("expected length=3 result array");
  }

  if (sum_i32->Value(0) != 11 || sum_i32->Value(1) != 22 || sum_i32->Value(2) != 33) {
    return arrow::Status::Invalid("unexpected compute result values");
  }

  return arrow::Status::OK();
}

}  // namespace

TEST(TiForthArrowSmokeTest, ArrowCompute) {
  auto status = RunArrowComputeSmoke();
  ASSERT_OK(status);
}

}  // namespace tiforth
